import discord
from discord.ext import commands
from discord import app_commands, ui
import json
import os
import asyncio
import logging
import copy
import time
import random
from typing import Dict, Any

# --- Constants ---
CONFIG_FILE = "welcome_config.json"
# How many seconds after first game role to wait for further role additions before sending welcome
ROLE_WAIT_SECONDS = 30
# Max seconds to wait for onboarding signal before giving up and sending a generic welcome
MAX_ONBOARDING_WAIT_SECONDS = 300

logger = logging.getLogger(__name__)

WELCOME_COLORS = [
    discord.Color.from_rgb(255, 107, 107),  # Coral red
    discord.Color.from_rgb(255, 159, 67),   # Tangerine
    discord.Color.from_rgb(255, 214, 0),    # Golden yellow
    discord.Color.from_rgb(46, 213, 115),   # Emerald green
    discord.Color.from_rgb(0, 210, 211),    # Teal
    discord.Color.from_rgb(30, 144, 255),   # Dodger blue
    discord.Color.from_rgb(116, 94, 255),   # Soft purple
    discord.Color.from_rgb(209, 72, 255),   # Orchid purple
    discord.Color.from_rgb(255, 71, 181),   # Hot pink
    discord.Color.from_rgb(255, 135, 178),  # Rose pink
    discord.Color.from_rgb(0, 184, 148),    # Mint
    discord.Color.from_rgb(52, 152, 219),   # Sky blue
    discord.Color.from_rgb(241, 196, 15),   # Sunflower
    discord.Color.from_rgb(231, 76, 60),    # Vermilion
    discord.Color.from_rgb(155, 89, 182),   # Amethyst
    discord.Color.from_rgb(26, 188, 156),   # Turquoise
    discord.Color.from_rgb(230, 126, 34),   # Carrot orange
    discord.Color.from_rgb(52, 73, 94),     # Midnight blue
    discord.Color.from_rgb(253, 121, 168),  # Flamingo
    discord.Color.from_rgb(99, 205, 218),   # Aquamarine
]


# --- Config I/O ---
def _load_config_sync(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error loading config {file_path}: {e}")
        return {}


def _save_config_sync(file_path: str, data: Dict[str, Any]):
    try:
        temp = f"{file_path}.tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp, file_path)
    except IOError as e:
        logger.error(f"Error saving config {file_path}: {e}")


class ConfigManager:
    def __init__(self, file_path: str):
        self.file_path = file_path

    async def load(self) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _load_config_sync, self.file_path)

    async def save(self, data: Dict[str, Any]):
        data_to_save = copy.deepcopy(data)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _save_config_sync, self.file_path, data_to_save)


def _default_guild_config() -> Dict[str, Any]:
    return {
        "welcome_channel_id": None,
        "exit_channel_id": None,
        "mod_channel_id": None,
        "intro_channel_id": None,
        "lfg_forum_id": None,
        "game_mappings": {},  # role_id (str) -> {"thread_id": int}
    }


class Alerts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.config_manager = ConfigManager(CONFIG_FILE)
        self.config: Dict[str, Any] = {}
        # Track pending new joiners: {user_id: {"joined_at": time, "task": asyncio.Task, "triggered": bool}}
        self._pending_welcomes: Dict[int, Dict] = {}
        self.bot.loop.create_task(self._load_config())

    async def _load_config(self):
        self.config = await self.config_manager.load()

    async def _save_config(self):
        await self.config_manager.save(self.config)

    def _guild_config(self, guild_id: int) -> Dict[str, Any]:
        key = str(guild_id)
        if key not in self.config:
            self.config[key] = _default_guild_config()
        return self.config[key]

    # ------------------------------------------------------------------
    # WELCOME LOGIC
    # ------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot:
            return
        gc = self._guild_config(member.guild.id)
        if not gc.get("welcome_channel_id"):
            return

        # Register as pending — wait for a game role before starting the welcome timer.
        # Fallback timer ensures welcome still fires if onboarding is never completed.
        task = asyncio.create_task(self._delayed_welcome(member, MAX_ONBOARDING_WAIT_SECONDS))
        self._pending_welcomes[member.id] = {
            "joined_at": time.time(),
            "task": task,
            "triggered": False,
        }

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if after.bot:
            return

        # --- Welcome: pending role tracking ---
        pending = self._pending_welcomes.get(after.id)
        if pending and before.roles != after.roles:
            gc = self._guild_config(after.guild.id)
            game_role_ids = {int(rid) for rid in gc.get("game_mappings", {})}
            added_role_ids = {r.id for r in after.roles} - {r.id for r in before.roles}

            if not pending["triggered"]:
                # Waiting for first game role to signal onboarding is done
                if added_role_ids & game_role_ids:
                    pending["triggered"] = True
                    task = pending.get("task")
                    if task and not task.done():
                        task.cancel()
                    task = asyncio.create_task(self._delayed_welcome(after))
                    pending["task"] = task
            else:
                # Already triggered — restart timer to batch further game role adds
                if added_role_ids & game_role_ids:
                    task = pending.get("task")
                    if task and not task.done():
                        task.cancel()
                    task = asyncio.create_task(self._delayed_welcome(after))
                    pending["task"] = task

        # --- Mod alerts: timeout detection ---
        if not before.timed_out_until and after.timed_out_until:
            asyncio.create_task(self._handle_timeout(after))

    async def _delayed_welcome(self, member: discord.Member, delay: int = ROLE_WAIT_SECONDS):
        """Wait for role additions to settle, then send a tailored welcome."""
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        # Clean up pending tracker
        self._pending_welcomes.pop(member.id, None)

        gc = self._guild_config(member.guild.id)
        welcome_ch_id = gc.get("welcome_channel_id")
        if not welcome_ch_id:
            return

        channel = member.guild.get_channel(welcome_ch_id)
        if not channel:
            try:
                channel = await member.guild.fetch_channel(welcome_ch_id)
            except Exception:
                return

        # Re-fetch member to get current roles
        try:
            member = await member.guild.fetch_member(member.id)
        except Exception:
            return

        # Determine which trigger game roles the user picked up
        game_mappings = gc.get("game_mappings", {})
        matched_roles = []
        for role_id_str, mapping in game_mappings.items():
            try:
                role_id = int(role_id_str)
            except ValueError:
                continue
            if any(r.id == role_id for r in member.roles):
                role = member.guild.get_role(role_id)
                matched_roles.append((role, mapping))

        # Build the embed
        lfg_forum_id = gc.get("lfg_forum_id")
        intro_ch_id = gc.get("intro_channel_id")

        embed = discord.Embed(
            title=f"Welcome to {member.guild.name}!",
            color=random.choice(WELCOME_COLORS),
        )
        embed.set_thumbnail(url=member.display_avatar.url)

        desc_lines = []

        if len(matched_roles) == 1:
            role, mapping = matched_roles[0]
            thread_id = mapping.get("thread_id")
            role_name = role.name if role else "LFG"
            desc_lines.append(
                f"If you're looking for teammates right now, head over to <#{thread_id}> "
                f"and add `@{role_name}` to your message to alert other players."
            )
        elif len(matched_roles) > 1:
            if lfg_forum_id:
                desc_lines.append(
                    f"If you're looking for teammates, head over to <#{lfg_forum_id}> "
                    f"and find the thread for the game you want to play. "
                    f"Add the game's LFG ping (e.g. `@GameLFG`) to your message to alert other players."
                )
            else:
                desc_lines.append(
                    "If you're looking for teammates, check out our LFG channels "
                    "and add the game's LFG ping to your message to alert other players."
                )
        else:
            # No game roles detected
            if lfg_forum_id:
                desc_lines.append(
                    f"If you're looking for teammates, head over to <#{lfg_forum_id}> "
                    f"and find the thread for your game. "
                    f"Add the game's LFG ping (e.g. `@GameLFG`) to your message to alert other players."
                )

        desc_lines.append(
            "*When responding to someone, remember to reply to their message so they get notified.*"
        )

        if intro_ch_id:
            desc_lines.append(
                f"Feel free to introduce yourself in <#{intro_ch_id}> -- we'd love to get to know you."
            )

        embed.description = "\n\n".join(desc_lines)

        await channel.send(content=member.mention, embed=embed)

    # ------------------------------------------------------------------
    # EXIT LOGIC
    # ------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        if member.bot:
            return

        # Clean up any pending welcome
        pending = self._pending_welcomes.pop(member.id, None)
        if pending:
            task = pending.get("task")
            if task and not task.done():
                task.cancel()

        gc = self._guild_config(member.guild.id)
        exit_ch_id = gc.get("exit_channel_id")
        mod_ch_id = gc.get("mod_channel_id")
        if not exit_ch_id and not mod_ch_id:
            return

        exit_channel = None
        if exit_ch_id:
            exit_channel = member.guild.get_channel(exit_ch_id)
            if not exit_channel:
                try:
                    exit_channel = await member.guild.fetch_channel(exit_ch_id)
                except Exception:
                    exit_channel = None

        mod_channel = None
        if mod_ch_id:
            mod_channel = member.guild.get_channel(mod_ch_id)
            if not mod_channel:
                try:
                    mod_channel = await member.guild.fetch_channel(mod_ch_id)
                except Exception:
                    mod_channel = None

        if not exit_channel and not mod_channel:
            return

        # Small delay to let audit log populate for kicks/bans
        await asyncio.sleep(2)

        # Check audit log for kick or ban
        action_type = "left"
        moderator = None
        reason = None
        is_inactivity_kick = False

        try:
            # Check for ban first
            async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
                if entry.target and entry.target.id == member.id:
                    if (discord.utils.utcnow() - entry.created_at).total_seconds() < 15:
                        action_type = "banned"
                        moderator = entry.user
                        reason = entry.reason
                        break

            # If not banned, check for kick
            if action_type == "left":
                async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.kick):
                    if entry.target and entry.target.id == member.id:
                        if (discord.utils.utcnow() - entry.created_at).total_seconds() < 15:
                            action_type = "kicked"
                            moderator = entry.user
                            reason = entry.reason
                            # Parse inactivity kicks to attribute to the real admin
                            if reason and reason.startswith("Inactivity | admin:"):
                                is_inactivity_kick = True
                                try:
                                    admin_id = int(reason.split("admin:")[1])
                                    admin = member.guild.get_member(admin_id) or await member.guild.fetch_member(admin_id)
                                    if admin:
                                        moderator = admin
                                except (ValueError, IndexError, discord.NotFound):
                                    pass
                                reason = "Inactivity"
                            break
        except discord.Forbidden:
            pass  # No audit log permission

        if action_type == "kicked":
            color = discord.Color.orange()
            footer = f"Kicked by {moderator}"
            if reason:
                footer += f" — {reason}"
        elif action_type == "banned":
            color = discord.Color.red()
            footer = f"Banned by {moderator}"
            if reason:
                footer += f" — {reason}"
        else:
            color = discord.Color.yellow()
            footer = "Left"

        embed = discord.Embed(
            description=f"{member.name}\n<@{member.id}>",
            color=color,
        )
        embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.set_footer(text=footer)

        # Send to exit channel (all events)
        if exit_channel:
            await exit_channel.send(embed=embed)

        # Send to mod channel (kicks and bans only, skip inactivity kicks)
        if mod_channel and action_type in ("kicked", "banned") and not is_inactivity_kick:
            await mod_channel.send(embed=embed)

    # ------------------------------------------------------------------
    # TIMEOUT DETECTION
    # ------------------------------------------------------------------
    async def _handle_timeout(self, member: discord.Member):
        gc = self._guild_config(member.guild.id)
        mod_ch_id = gc.get("mod_channel_id")
        if not mod_ch_id:
            return

        channel = member.guild.get_channel(mod_ch_id)
        if not channel:
            try:
                channel = await member.guild.fetch_channel(mod_ch_id)
            except Exception:
                return

        # Small delay for audit log to populate
        await asyncio.sleep(2)

        moderator = None
        reason = None
        try:
            async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.member_update):
                if entry.target and entry.target.id == member.id:
                    if (discord.utils.utcnow() - entry.created_at).total_seconds() < 15:
                        moderator = entry.user
                        reason = entry.reason
                        break
        except discord.Forbidden:
            pass

        # Calculate duration
        duration_str = "Unknown duration"
        if member.timed_out_until:
            delta = member.timed_out_until - discord.utils.utcnow()
            total_seconds = int(delta.total_seconds())
            if total_seconds > 0:
                days, remainder = divmod(total_seconds, 86400)
                hours, remainder = divmod(remainder, 3600)
                minutes, _ = divmod(remainder, 60)
                parts = []
                if days:
                    parts.append(f"{days}d")
                if hours:
                    parts.append(f"{hours}h")
                if minutes:
                    parts.append(f"{minutes}m")
                duration_str = " ".join(parts) if parts else "<1m"

        footer = f"Timed out by {moderator}" if moderator else "Timed out"
        footer += f" — {duration_str}"
        if reason:
            footer += f" — {reason}"

        embed = discord.Embed(
            description=f"{member.name}\n<@{member.id}>",
            color=discord.Color.dark_gold(),
        )
        embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.set_footer(text=footer)

        await channel.send(embed=embed)

    # ------------------------------------------------------------------
    # ADMIN PANEL
    # ------------------------------------------------------------------
    @app_commands.command(name="alerts_panel", description="Admin: Configure the Alerts system (Welcome, Exit & Mod)")
    async def alerts_panel(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("Admin access only.", ephemeral=True)
        view = AlertsPanelView(self)
        await interaction.response.send_message(
            "**Alerts Panel**\nSelect a module to configure:", view=view, ephemeral=True
        )


# ======================================================================
# ADMIN PANEL VIEWS
# ======================================================================

class AlertsPanelView(ui.View):
    def __init__(self, cog: Alerts):
        super().__init__(timeout=120)
        self.cog = cog

    @ui.button(label="Channels", style=discord.ButtonStyle.primary, row=0)
    async def channels_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "Select channels for welcome, exit, and mod alert messages:",
            view=ChannelConfigView(self.cog, interaction.guild_id),
            ephemeral=True,
        )

    @ui.button(label="Game Mappings", style=discord.ButtonStyle.primary, row=0)
    async def mappings_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "Manage game role to LFG thread mappings:",
            view=GameMappingMenuView(self.cog, interaction.guild_id),
            ephemeral=True,
        )

    @ui.button(label="View Config", style=discord.ButtonStyle.secondary, row=0)
    async def view_btn(self, interaction: discord.Interaction, button: ui.Button):
        gc = self.cog._guild_config(interaction.guild_id)
        guild = interaction.guild

        welcome_ch = f"<#{gc['welcome_channel_id']}>" if gc.get("welcome_channel_id") else "Not set"
        exit_ch = f"<#{gc['exit_channel_id']}>" if gc.get("exit_channel_id") else "Not set"
        mod_ch = f"<#{gc['mod_channel_id']}>" if gc.get("mod_channel_id") else "Not set"
        intro_ch = f"<#{gc['intro_channel_id']}>" if gc.get("intro_channel_id") else "Not set"
        lfg_forum = f"<#{gc['lfg_forum_id']}>" if gc.get("lfg_forum_id") else "Not set"

        lines = [
            "**Current Configuration**",
            f"Welcome Channel: {welcome_ch}",
            f"Exit Channel: {exit_ch}",
            f"Mod Channel: {mod_ch}",
            f"Intro Channel: {intro_ch}",
            f"LFG Forum: {lfg_forum}",
            "",
            "**Game Mappings**",
        ]

        mappings = gc.get("game_mappings", {})
        if not mappings:
            lines.append("None configured.")
        else:
            for role_id_str, mapping in mappings.items():
                role = guild.get_role(int(role_id_str))
                role_name = role.name if role else f"Unknown ({role_id_str})"
                thread_id = mapping.get("thread_id")
                lines.append(f"- **{role_name}** -> <#{thread_id}>")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)


class ChannelConfigView(ui.View):
    def __init__(self, cog: Alerts, guild_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id

    @ui.select(
        cls=ui.ChannelSelect,
        placeholder="Welcome Channel",
        min_values=1, max_values=1,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
        ],
    )
    async def welcome_ch(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        gc = self.cog._guild_config(self.guild_id)
        gc["welcome_channel_id"] = select.values[0].id
        await self.cog._save_config()
        await interaction.response.send_message(
            f"Welcome channel set to {select.values[0].mention}", ephemeral=True
        )

    @ui.select(
        cls=ui.ChannelSelect,
        placeholder="Exit Channel",
        min_values=1, max_values=1,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
        ],
    )
    async def exit_ch(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        gc = self.cog._guild_config(self.guild_id)
        gc["exit_channel_id"] = select.values[0].id
        await self.cog._save_config()
        await interaction.response.send_message(
            f"Exit channel set to {select.values[0].mention}", ephemeral=True
        )

    @ui.select(
        cls=ui.ChannelSelect,
        placeholder="Mod Channel (kick/ban/timeout alerts)",
        min_values=1, max_values=1,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
        ],
    )
    async def mod_ch(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        gc = self.cog._guild_config(self.guild_id)
        gc["mod_channel_id"] = select.values[0].id
        await self.cog._save_config()
        await interaction.response.send_message(
            f"Mod channel set to {select.values[0].mention}", ephemeral=True
        )

    @ui.select(
        cls=ui.ChannelSelect,
        placeholder="Introduction Channel (for invite at end of welcome)",
        min_values=1, max_values=1,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
        ],
    )
    async def intro_ch(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        gc = self.cog._guild_config(self.guild_id)
        gc["intro_channel_id"] = select.values[0].id
        await self.cog._save_config()
        await interaction.response.send_message(
            f"Introduction channel set to {select.values[0].mention}", ephemeral=True
        )

    @ui.select(
        cls=ui.ChannelSelect,
        placeholder="LFG Forum (main forum for fallback redirect)",
        min_values=1, max_values=1,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.forum,
            discord.ChannelType.public_thread,
        ],
    )
    async def lfg_forum(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        gc = self.cog._guild_config(self.guild_id)
        gc["lfg_forum_id"] = select.values[0].id
        await self.cog._save_config()
        await interaction.response.send_message(
            f"LFG Forum set to {select.values[0].mention}", ephemeral=True
        )


class GameMappingMenuView(ui.View):
    def __init__(self, cog: Alerts, guild_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id

    @ui.button(label="Add Mapping", style=discord.ButtonStyle.success)
    async def add_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "Step 1: Select the game role that triggers this mapping.",
            view=AddMappingRoleView(self.cog, self.guild_id),
            ephemeral=True,
        )

    @ui.button(label="Remove Mapping", style=discord.ButtonStyle.danger)
    async def remove_btn(self, interaction: discord.Interaction, button: ui.Button):
        gc = self.cog._guild_config(self.guild_id)
        mappings = gc.get("game_mappings", {})
        if not mappings:
            return await interaction.response.send_message("No mappings to remove.", ephemeral=True)

        options = []
        for role_id_str, mapping in mappings.items():
            role = interaction.guild.get_role(int(role_id_str))
            label = role.name if role else f"Unknown ({role_id_str})"
            options.append(discord.SelectOption(label=label[:100], value=role_id_str))

        view = RemoveMappingView(self.cog, self.guild_id, options)
        await interaction.response.send_message("Select mapping to remove:", view=view, ephemeral=True)


class AddMappingRoleView(ui.View):
    def __init__(self, cog: Alerts, guild_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id

    @ui.select(cls=ui.RoleSelect, placeholder="Select the game role", min_values=1, max_values=1)
    async def role_select(self, interaction: discord.Interaction, select: ui.RoleSelect):
        role = select.values[0]
        await interaction.response.send_message(
            f"Selected role: **{role.name}**\nStep 2: Select the LFG thread/channel for this game.",
            view=AddMappingThreadView(self.cog, self.guild_id, role),
            ephemeral=True,
        )


class AddMappingThreadView(ui.View):
    def __init__(self, cog: Alerts, guild_id: int, role: discord.Role):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id
        self.role = role

    @ui.select(
        cls=ui.ChannelSelect,
        placeholder="Select the LFG thread/channel",
        min_values=1, max_values=1,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
            discord.ChannelType.forum,
        ],
    )
    async def thread_select(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        thread = select.values[0]
        gc = self.cog._guild_config(self.guild_id)
        gc["game_mappings"][str(self.role.id)] = {
            "thread_id": thread.id,
        }
        await self.cog._save_config()
        await interaction.response.send_message(
            f"Mapping added: **{self.role.name}** -> <#{thread.id}>",
            ephemeral=True,
        )


class RemoveMappingView(ui.View):
    def __init__(self, cog: Alerts, guild_id: int, options):
        super().__init__(timeout=60)
        self.cog = cog
        self.guild_id = guild_id
        self.select_menu = ui.Select(placeholder="Select mapping to remove", options=options)
        self.select_menu.callback = self.on_select
        self.add_item(self.select_menu)

    async def on_select(self, interaction: discord.Interaction):
        role_id_str = self.select_menu.values[0]
        gc = self.cog._guild_config(self.guild_id)
        removed = gc.get("game_mappings", {}).pop(role_id_str, None)
        await self.cog._save_config()
        if removed:
            await interaction.response.send_message("Mapping removed.", ephemeral=True)
        else:
            await interaction.response.send_message("Mapping not found.", ephemeral=True)


async def setup(bot):
    await bot.add_cog(Alerts(bot))
