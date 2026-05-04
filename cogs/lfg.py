import discord
from discord.ext import commands
from discord import app_commands
import json
import os
import asyncio
import logging
import copy
import random
import time
from typing import Dict, Any, List, Optional

# --- Constants ---
CONFIG_FILE = "lfg_config.json"
EMBED_COLOR = 0xE91E63

# 12 distinct colors for trigger role embeds, randomly assigned on creation
TRIGGER_COLORS = [
    0xE91E63,  # Pink
    0x3498DB,  # Blue
    0x2ECC71,  # Green
    0xE67E22,  # Orange
    0x9B59B6,  # Purple
    0x1ABC9C,  # Teal
    0xE74C3C,  # Red
    0xF1C40F,  # Yellow
    0x11806A,  # Dark Teal
    0x7289DA,  # Blurple
    0xFD7E14,  # Amber
    0x607D8B,  # Blue Grey
]

logger = logging.getLogger(__name__)


# --- Synchronous Helper Functions for File I/O ---
def _load_config_sync(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error loading config file {file_path}: {e}")
        return {}


def _save_config_sync(file_path: str, data: Dict[str, Any]):
    try:
        temp_file = f"{file_path}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, file_path)
    except IOError as e:
        logger.error(f"Error saving config file {file_path}: {e}")


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


# --- Default guild config ---
def _default_guild_config() -> Dict[str, Any]:
    return {
        "lfg_channel_id": None,
        "triggers": {},
        "active_triggers": [],
        "ignore_list": {},
        "subscriptions": {},
        "vip_role_id": None,
        "vip_stats": [],
    }


# ============================================================
# Persistent Bell View (survives restart)
# ============================================================
class LFGBellView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="\U0001f514", style=discord.ButtonStyle.secondary, custom_id="lfg_bell_notify")
    async def bell_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return
        guild_id = str(guild.id)
        user_id = str(interaction.user.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            user_subs = gc["subscriptions"].get(user_id, [])
            triggers = gc["triggers"]

        embed = self._build_sub_embed(guild, user_subs, triggers)
        view = LFGNotifyPanelView(self.cog, guild_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @staticmethod
    def _build_sub_embed(guild: discord.Guild, user_subs: List[str], triggers: Dict) -> discord.Embed:
        embed = discord.Embed(title="Your LFG Alert Subscriptions", color=EMBED_COLOR)
        if not user_subs:
            embed.description = "You are not subscribed to any alerts."
        else:
            lines = []
            for role_id_str in user_subs:
                role = guild.get_role(int(role_id_str))
                name = role.name if role else f"Unknown Role ({role_id_str})"
                lines.append(f"- {name}")
            embed.description = "\n".join(lines)
        return embed


# ============================================================
# Notification Panel Views (ephemeral, edit same message)
# ============================================================
class LFGNotifyPanelView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="Add", style=discord.ButtonStyle.green)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        user_id = str(interaction.user.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            user_subs = gc["subscriptions"].get(user_id, [])
            triggers = gc["triggers"]

        available = [rid for rid in triggers if rid not in user_subs]
        if not available:
            await interaction.response.edit_message(
                embed=discord.Embed(title="No Triggers Available", description="You are already subscribed to all available triggers.", color=EMBED_COLOR),
                view=LFGNotifyPanelView(self.cog, self.guild_id),
            )
            return

        options = []
        for rid in available:
            role = guild.get_role(int(rid))
            name = role.name if role else f"Unknown ({rid})"
            options.append(discord.SelectOption(label=name, value=rid))

        view = LFGNotifyAddView(self.cog, self.guild_id, options)
        await interaction.response.edit_message(
            embed=discord.Embed(title="Add Alert Subscription", description="Select the roles you want to be alerted for.", color=EMBED_COLOR),
            view=view,
        )

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.red)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        user_id = str(interaction.user.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            user_subs = gc["subscriptions"].get(user_id, [])

        if not user_subs:
            await interaction.response.edit_message(
                embed=discord.Embed(title="No Subscriptions", description="You have no subscriptions to remove.", color=EMBED_COLOR),
                view=LFGNotifyPanelView(self.cog, self.guild_id),
            )
            return

        options = []
        for rid in user_subs:
            role = guild.get_role(int(rid))
            name = role.name if role else f"Unknown ({rid})"
            options.append(discord.SelectOption(label=name, value=rid))

        view = LFGNotifyRemoveView(self.cog, self.guild_id, options)
        await interaction.response.edit_message(
            embed=discord.Embed(title="Remove Alert Subscription", description="Select the roles you want to unsubscribe from.", color=EMBED_COLOR),
            view=view,
        )


class LFGNotifyAddView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id
        self.add_item(LFGNotifyAddSelect(cog, guild_id, options))


class LFGNotifyAddSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        self.cog = cog
        self.guild_id = guild_id
        super().__init__(placeholder="Select roles to subscribe to...", options=options, min_values=1, max_values=len(options))

    async def callback(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            subs = gc["subscriptions"].setdefault(user_id, [])
            for val in self.values:
                if val not in subs:
                    subs.append(val)
            await self.cog._save()

        async with self.cog.config_lock:
            user_subs = gc["subscriptions"].get(user_id, [])
            triggers = gc["triggers"]

        embed = LFGBellView._build_sub_embed(interaction.guild, user_subs, triggers)
        embed.set_footer(text="Subscriptions updated.")
        await interaction.response.edit_message(embed=embed, view=LFGNotifyPanelView(self.cog, self.guild_id))


class LFGNotifyRemoveView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id
        self.add_item(LFGNotifyRemoveSelect(cog, guild_id, options))


class LFGNotifyRemoveSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        self.cog = cog
        self.guild_id = guild_id
        super().__init__(placeholder="Select roles to unsubscribe from...", options=options, min_values=1, max_values=len(options))

    async def callback(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            subs = gc["subscriptions"].get(user_id, [])
            for val in self.values:
                if val in subs:
                    subs.remove(val)
            if not subs:
                gc["subscriptions"].pop(user_id, None)
            await self.cog._save()

        async with self.cog.config_lock:
            user_subs = gc["subscriptions"].get(user_id, [])
            triggers = gc["triggers"]

        embed = LFGBellView._build_sub_embed(interaction.guild, user_subs, triggers)
        embed.set_footer(text="Subscriptions updated.")
        await interaction.response.edit_message(embed=embed, view=LFGNotifyPanelView(self.cog, self.guild_id))


# ============================================================
# Admin Panel Views
# ============================================================
class LFGAdminPanelView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to use this.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Channel", style=discord.ButtonStyle.primary, row=0)
    async def channel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Set LFG Channel", description="Select the channel where LFG alerts will be sent.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGChannelSelectView(self.cog))

    @discord.ui.button(label="Triggers", style=discord.ButtonStyle.primary, row=0)
    async def triggers_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Trigger Management", description="Add, edit, delete, or view trigger roles.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))

    @discord.ui.button(label="Ignore", style=discord.ButtonStyle.primary, row=0)
    async def ignore_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Ignore List", description="Manage users and roles the bot will ignore.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))

    @discord.ui.button(label="Rotation", style=discord.ButtonStyle.primary, row=1)
    async def rotation_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            triggers = gc["triggers"]
            active = gc["active_triggers"]

        if not triggers:
            embed = discord.Embed(title="Rotation", description="No triggers configured yet. Add triggers first.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGBackOnlyView(self.cog))
            return

        options = []
        for rid, data in triggers.items():
            role = guild.get_role(int(rid))
            name = role.name if role else f"Unknown ({rid})"
            options.append(discord.SelectOption(label=name, value=rid, default=(rid in active)))

        active_names = []
        for rid in active:
            role = guild.get_role(int(rid))
            active_names.append(role.name if role else f"Unknown ({rid})")

        desc = "Select which triggers are currently active.\n\n"
        if active_names:
            desc += f"**Currently active:** {', '.join(active_names)}"
        else:
            desc += "**Currently active:** None"

        embed = discord.Embed(title="Rotation", description=desc, color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGRotationView(self.cog, guild_id, options))

    @discord.ui.button(label="VIP Role", style=discord.ButtonStyle.primary, row=1)
    async def vip_role_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        vip_id = gc.get("vip_role_id")
        if vip_id:
            role = interaction.guild.get_role(vip_id)
            current = role.mention if role else f"Unknown ({vip_id})"
        else:
            current = "Not set"
        embed = discord.Embed(title="VIP Role", description=f"**Current:** {current}\n\nSelect a role to set as the VIP role. VIP members' replies to trigger messages will be tracked in stats.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGVIPRoleSelectView(self.cog))

    @discord.ui.button(label="Stats", style=discord.ButtonStyle.primary, row=1)
    async def stats_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        if not gc.get("vip_role_id"):
            embed = discord.Embed(title="VIP Stats", description="Set a VIP role first.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGBackOnlyView(self.cog))
            return
        embed = discord.Embed(title="VIP Stats", description="Choose an option below.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGStatsMenuView(self.cog, guild_id))

    @discord.ui.button(label="Alert List", style=discord.ButtonStyle.primary, row=2)
    async def alert_list_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            triggers = gc["triggers"]
            subscriptions = gc["subscriptions"]

        if not triggers:
            embed = discord.Embed(title="Alert List", description="No triggers configured yet.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGBackOnlyView(self.cog))
            return

        # Build a map: trigger_role_id -> list of user display names
        trigger_subs: Dict[str, List[str]] = {rid: [] for rid in triggers}
        for user_id_str, sub_roles in subscriptions.items():
            member = guild.get_member(int(user_id_str))
            name = member.display_name if member else f"User ({user_id_str})"
            for rid in sub_roles:
                if rid in trigger_subs:
                    trigger_subs[rid].append(name)

        for rid in trigger_subs:
            trigger_subs[rid].sort(key=str.casefold)

        embed = discord.Embed(title="Alert Subscribers", color=EMBED_COLOR)
        for rid, names in trigger_subs.items():
            role = guild.get_role(int(rid))
            role_name = role.name if role else f"Unknown ({rid})"
            if names:
                embed.add_field(name=role_name, value="\n".join(names), inline=False)
            else:
                embed.add_field(name=role_name, value="*No subscribers*", inline=False)

        total = len(subscriptions)
        embed.set_footer(text=f"{total} user{'s' if total != 1 else ''} subscribed")
        await interaction.response.edit_message(embed=embed, view=LFGBackOnlyView(self.cog))

    @discord.ui.button(label="User Alerts", style=discord.ButtonStyle.primary, row=2)
    async def user_alerts_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)

        if not gc.get("lfg_channel_id"):
            embed = discord.Embed(title="User Alerts", description="Set an LFG channel first.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGBackOnlyView(self.cog))
            return

        if not gc.get("triggers"):
            embed = discord.Embed(title="User Alerts", description="Add triggers first.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGBackOnlyView(self.cog))
            return

        embed = discord.Embed(
            title="Send User Alerts Prompt",
            description=f"This will send a public embed with a notification button to <#{gc['lfg_channel_id']}> so users can manage their alert subscriptions.\n\nAre you sure?",
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=LFGUserAlertsConfirmView(self.cog, guild_id))


class LFGUserAlertsConfirmView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            lfg_channel_id = gc.get("lfg_channel_id")
            triggers = gc.get("triggers", {})

        lfg_channel = guild.get_channel(lfg_channel_id) if lfg_channel_id else None
        if not lfg_channel:
            embed = _build_panel_embed(self.cog, guild, gc)
            embed.set_footer(text="LFG channel not found.")
            await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))
            return

        # Build the public prompt embed
        lines = []
        for rid in triggers:
            role = guild.get_role(int(rid))
            name = role.name if role else f"Unknown ({rid})"
            lines.append(f"- {name}")

        prompt_embed = discord.Embed(
            title="LFG Alert Notifications",
            description="Click the button below to choose which LFG alerts you want to be pinged for.\n\n**Available alerts:**\n" + "\n".join(lines),
            color=EMBED_COLOR,
        )

        try:
            await lfg_channel.send(embed=prompt_embed, view=LFGBellView(self.cog))
        except discord.HTTPException as e:
            logger.error(f"Failed to send user alerts prompt: {e}")
            embed = _build_panel_embed(self.cog, guild, gc)
            embed.set_footer(text="Failed to send prompt.")
            await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))
            return

        embed = _build_panel_embed(self.cog, guild, gc)
        embed.set_footer(text="User alerts prompt sent.")
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


def _build_panel_embed(cog: "LFG", guild: discord.Guild, gc: Dict) -> discord.Embed:
    embed = discord.Embed(title="LFG Admin Panel", color=EMBED_COLOR)

    # Channel
    ch_id = gc.get("lfg_channel_id")
    ch_text = f"<#{ch_id}>" if ch_id else "Not set"
    embed.add_field(name="LFG Channel", value=ch_text, inline=True)

    # Ignore
    ignore_count = len(gc.get("ignore_list", {}))
    embed.add_field(name="Ignored", value=str(ignore_count), inline=True)

    # VIP Role
    vip_id = gc.get("vip_role_id")
    if vip_id:
        vip_role = guild.get_role(vip_id)
        vip_text = vip_role.name if vip_role else f"Unknown ({vip_id})"
    else:
        vip_text = "Not set"
    embed.add_field(name="VIP Role", value=vip_text, inline=True)

    # Triggers list
    triggers = gc.get("triggers", {})
    active = gc.get("active_triggers", [])
    if not triggers:
        embed.add_field(name="Triggers", value="None configured", inline=False)
    else:
        lines = []
        for rid, data in triggers.items():
            role = guild.get_role(int(rid))
            role_name = role.name if role else f"Unknown ({rid})"
            ch = guild.get_channel(data["channel_id"]) or guild.get_thread(data["channel_id"])
            ch_text = f"<#{data['channel_id']}>" if ch else "Unknown channel"
            status = "Active" if rid in active else "Inactive"
            lines.append(f"**{role_name}** -> {ch_text} [{status}]")
        embed.add_field(name="Triggers", value="\n".join(lines), inline=False)

    return embed


def _build_stats_embed(guild: discord.Guild, gc: Dict, trigger_filter: str = None, days_filter: int = None) -> discord.Embed:
    stats = gc.get("vip_stats", [])
    triggers = gc.get("triggers", {})

    # Filter by trigger role
    if trigger_filter:
        stats = [s for s in stats if s["trigger_role_id"] == trigger_filter]

    # Filter by time
    if days_filter:
        cutoff = time.time() - days_filter * 86400
        stats = [s for s in stats if s["timestamp"] >= cutoff]

    # Count by user
    counts: Dict[str, int] = {}
    for s in stats:
        uid = s["user_id"]
        counts[uid] = counts.get(uid, 0) + 1

    # Sort by count descending
    sorted_users = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    # Build filter description
    filter_parts = []
    if trigger_filter:
        role = guild.get_role(int(trigger_filter))
        filter_parts.append(f"**Trigger:** {role.name if role else trigger_filter}")
    else:
        filter_parts.append("**Trigger:** All")
    if days_filter:
        filter_parts.append(f"**Period:** Last {days_filter} days")
    else:
        filter_parts.append("**Period:** All time")

    embed = discord.Embed(title="VIP Response Stats", color=EMBED_COLOR)
    embed.description = " | ".join(filter_parts)

    if not sorted_users:
        embed.add_field(name="Leaderboard", value="No responses recorded.", inline=False)
    else:
        lines = []
        for i, (uid, count) in enumerate(sorted_users[:25], 1):
            member = guild.get_member(int(uid))
            name = member.display_name if member else f"User ({uid})"
            lines.append(f"**{i}.** {name} — {count} response{'s' if count != 1 else ''}")
        embed.add_field(name="Leaderboard", value="\n".join(lines), inline=False)

    embed.set_footer(text=f"Total responses: {sum(counts.values())}")
    return embed


# --- Channel Select ---
class LFGChannelSelectView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select LFG channel...", channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        guild_id = str(interaction.guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["lfg_channel_id"] = channel.id
            await self.cog._save()

        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        embed.set_footer(text=f"LFG channel set to #{channel}.")
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


# --- Back-only helper ---
class LFGBackOnlyView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


# ============================================================
# Trigger Management Views
# ============================================================
class LFGTriggerMenuView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.button(label="Add", style=discord.ButtonStyle.green, row=0)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Trigger", description="Select a role to use as a trigger.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerAddRoleView(self.cog))

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary, row=0)
    async def edit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            triggers = gc["triggers"]

        if not triggers:
            embed = discord.Embed(title="Edit Trigger", description="No triggers configured.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))
            return

        options = []
        for rid, data in triggers.items():
            role = guild.get_role(int(rid))
            ch = guild.get_channel(data["channel_id"]) or guild.get_thread(data["channel_id"])
            role_name = role.name if role else f"Unknown ({rid})"
            ch_name = f"#{ch.name}" if ch else "Unknown channel"
            options.append(discord.SelectOption(label=role_name, description=ch_name, value=rid))

        embed = discord.Embed(title="Edit Trigger", description="Select a trigger to change its watched channel.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerEditSelectView(self.cog, options))

    @discord.ui.button(label="Delete", style=discord.ButtonStyle.red, row=0)
    async def delete_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            triggers = gc["triggers"]

        if not triggers:
            embed = discord.Embed(title="Delete Trigger", description="No triggers configured.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))
            return

        options = []
        for rid, data in triggers.items():
            role = guild.get_role(int(rid))
            ch = guild.get_channel(data["channel_id"]) or guild.get_thread(data["channel_id"])
            role_name = role.name if role else f"Unknown ({rid})"
            ch_name = f"#{ch.name}" if ch else "Unknown channel"
            options.append(discord.SelectOption(label=role_name, description=ch_name, value=rid))

        embed = discord.Embed(title="Delete Trigger", description="Select triggers to delete.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerDeleteSelectView(self.cog, guild_id, options))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


class LFGTriggerAddRoleView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        embed = discord.Embed(title="Add Trigger", description=f"Now select the channel to watch for **{role.name}**.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerAddChannelView(self.cog, str(role.id)))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Trigger Management", description="Add, edit, delete, or view trigger roles.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))


class LFGTriggerAddChannelView(discord.ui.View):
    def __init__(self, cog: "LFG", role_id: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.role_id = role_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        placeholder="Select a channel/thread...",
        channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread, discord.ChannelType.private_thread, discord.ChannelType.forum],
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        guild_id = str(interaction.guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            # Assign a random color not already used by other triggers
            used_colors = [t.get("color", 0) for t in gc["triggers"].values()]
            available_colors = [c for c in TRIGGER_COLORS if c not in used_colors]
            color = random.choice(available_colors) if available_colors else random.choice(TRIGGER_COLORS)
            gc["triggers"][self.role_id] = {"channel_id": channel.id, "color": color}
            # Auto-activate new triggers
            if self.role_id not in gc["active_triggers"]:
                gc["active_triggers"].append(self.role_id)
            await self.cog._save()

        role = interaction.guild.get_role(int(self.role_id))
        role_name = role.name if role else self.role_id
        embed = discord.Embed(title="Trigger Management", description=f"Trigger **{role_name}** added, watching <#{channel.id}>.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Trigger", description="Select a role to use as a trigger.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerAddRoleView(self.cog))


class LFGTriggerEditSelectView(discord.ui.View):
    def __init__(self, cog: "LFG", options: List[discord.SelectOption]):
        super().__init__(timeout=180)
        self.cog = cog
        self.add_item(LFGTriggerEditSelect(cog, options))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Trigger Management", description="Add, edit, delete, or view trigger roles.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))


class LFGTriggerEditSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", options: List[discord.SelectOption]):
        self.cog = cog
        super().__init__(placeholder="Select a trigger to edit...", options=options)

    async def callback(self, interaction: discord.Interaction):
        role_id = self.values[0]
        guild_id = str(interaction.guild.id)
        role = interaction.guild.get_role(int(role_id))
        role_name = role.name if role else role_id

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            trigger_color = gc["triggers"].get(role_id, {}).get("color", EMBED_COLOR)

        embed = discord.Embed(title="Edit Trigger", description=f"Select a new channel for **{role_name}**.", color=trigger_color)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerAddChannelView(self.cog, role_id))


class LFGTriggerDeleteSelectView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.add_item(LFGTriggerDeleteSelect(cog, guild_id, options))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Trigger Management", description="Add, edit, delete, or view trigger roles.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))


class LFGTriggerDeleteSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        self.cog = cog
        self.guild_id = guild_id
        super().__init__(placeholder="Select triggers to delete...", options=options, min_values=1, max_values=len(options))

    async def callback(self, interaction: discord.Interaction):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            for rid in self.values:
                gc["triggers"].pop(rid, None)
                if rid in gc["active_triggers"]:
                    gc["active_triggers"].remove(rid)
                # Clean up subscriptions referencing deleted triggers
                for uid, subs in list(gc["subscriptions"].items()):
                    if rid in subs:
                        subs.remove(rid)
                    if not subs:
                        gc["subscriptions"].pop(uid, None)
            await self.cog._save()

        deleted_count = len(self.values)
        embed = discord.Embed(title="Trigger Management", description=f"Deleted {deleted_count} trigger(s).", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGTriggerMenuView(self.cog))


# ============================================================
# Rotation View
# ============================================================
class LFGRotationView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.add_item(LFGRotationSelect(cog, guild_id, options))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


class LFGRotationSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        self.cog = cog
        self.guild_id = guild_id
        super().__init__(placeholder="Select active triggers...", options=options, min_values=0, max_values=len(options))

    async def callback(self, interaction: discord.Interaction):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            gc["active_triggers"] = list(self.values)
            await self.cog._save()

        count = len(self.values)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        embed.set_footer(text=f"Rotation updated: {count} trigger(s) active.")
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


# ============================================================
# Ignore Management Views
# ============================================================
class LFGIgnoreMenuView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.button(label="Add", style=discord.ButtonStyle.green, row=0)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Ignore", description="Choose whether to ignore a user or a role.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreTypeView(self.cog))

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.red, row=0)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            ignore_list = gc["ignore_list"]

        if not ignore_list:
            embed = discord.Embed(title="Remove Ignore", description="No entries in the ignore list.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))
            return

        options = []
        for eid, entry in ignore_list.items():
            if entry["type"] == "user":
                member = guild.get_member(int(eid))
                name = member.display_name if member else f"User ({eid})"
                label = f"User: {name}"
            else:
                role = guild.get_role(int(eid))
                name = role.name if role else f"Role ({eid})"
                label = f"Role: {name}"
            scope_text = "All" if entry["scope"] == "all" else f"{len(entry['scope'])} trigger(s)"
            options.append(discord.SelectOption(label=label, description=f"Scope: {scope_text}", value=eid))

        embed = discord.Embed(title="Remove Ignore", description="Select entries to remove from the ignore list.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreRemoveView(self.cog, guild_id, options))

    @discord.ui.button(label="View", style=discord.ButtonStyle.secondary, row=0)
    async def view_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            ignore_list = gc["ignore_list"]

        if not ignore_list:
            embed = discord.Embed(title="Ignore List", description="No entries.", color=EMBED_COLOR)
        else:
            lines = []
            for eid, entry in ignore_list.items():
                if entry["type"] == "user":
                    member = guild.get_member(int(eid))
                    name = member.display_name if member else f"User ({eid})"
                    prefix = "User"
                else:
                    role = guild.get_role(int(eid))
                    name = role.name if role else f"Role ({eid})"
                    prefix = "Role"
                scope_text = "All triggers" if entry["scope"] == "all" else f"{len(entry['scope'])} specific trigger(s)"
                lines.append(f"**{prefix}: {name}** - {scope_text}")
            embed = discord.Embed(title="Ignore List", description="\n".join(lines), color=EMBED_COLOR)

        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


class LFGIgnoreTypeView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.button(label="User", style=discord.ButtonStyle.primary, row=0)
    async def user_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Ignore User", description="Select a user to ignore.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreUserSelectView(self.cog))

    @discord.ui.button(label="Role", style=discord.ButtonStyle.primary, row=0)
    async def role_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Ignore Role", description="Select a role to ignore.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreRoleSelectView(self.cog))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Ignore List", description="Manage users and roles the bot will ignore.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))


class LFGIgnoreUserSelectView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        embed = discord.Embed(
            title="Ignore Scope",
            description=f"Ignore **{user.display_name}** for all triggers or specific ones?",
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreScopeView(self.cog, str(user.id), "user"))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Ignore", description="Choose whether to ignore a user or a role.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreTypeView(self.cog))


class LFGIgnoreRoleSelectView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        embed = discord.Embed(
            title="Ignore Scope",
            description=f"Ignore **{role.name}** for all triggers or specific ones?",
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreScopeView(self.cog, str(role.id), "role"))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Ignore", description="Choose whether to ignore a user or a role.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreTypeView(self.cog))


class LFGIgnoreScopeView(discord.ui.View):
    def __init__(self, cog: "LFG", entity_id: str, entity_type: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.entity_id = entity_id
        self.entity_type = entity_type

    @discord.ui.button(label="All Triggers", style=discord.ButtonStyle.primary, row=0)
    async def all_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["ignore_list"][self.entity_id] = {"type": self.entity_type, "scope": "all"}
            await self.cog._save()

        embed = discord.Embed(title="Ignore List", description="Entry added (all triggers).", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))

    @discord.ui.button(label="Specific Triggers", style=discord.ButtonStyle.primary, row=0)
    async def specific_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        guild_id = str(guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            triggers = gc["triggers"]

        if not triggers:
            embed = discord.Embed(title="Ignore Scope", description="No triggers configured. Add triggers first.", color=EMBED_COLOR)
            await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))
            return

        options = []
        for rid in triggers:
            role = guild.get_role(int(rid))
            name = role.name if role else f"Unknown ({rid})"
            options.append(discord.SelectOption(label=name, value=rid))

        embed = discord.Embed(title="Ignore Scope", description="Select which triggers to ignore for this entry.", color=EMBED_COLOR)
        await interaction.response.edit_message(
            embed=embed,
            view=LFGIgnoreScopeSelectView(self.cog, self.entity_id, self.entity_type, guild_id, options),
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Ignore", description="Choose whether to ignore a user or a role.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreTypeView(self.cog))


class LFGIgnoreScopeSelectView(discord.ui.View):
    def __init__(self, cog: "LFG", entity_id: str, entity_type: str, guild_id: str, options: List[discord.SelectOption]):
        super().__init__(timeout=180)
        self.cog = cog
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.guild_id = guild_id
        self.add_item(LFGIgnoreScopeSelect(cog, entity_id, entity_type, guild_id, options))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Ignore", description="Choose whether to ignore a user or a role.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreTypeView(self.cog))


class LFGIgnoreScopeSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", entity_id: str, entity_type: str, guild_id: str, options: List[discord.SelectOption]):
        self.cog = cog
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.guild_id = guild_id
        super().__init__(placeholder="Select triggers to ignore...", options=options, min_values=1, max_values=len(options))

    async def callback(self, interaction: discord.Interaction):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            gc["ignore_list"][self.entity_id] = {"type": self.entity_type, "scope": list(self.values)}
            await self.cog._save()

        embed = discord.Embed(title="Ignore List", description=f"Entry added ({len(self.values)} trigger(s)).", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))


class LFGIgnoreRemoveView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.add_item(LFGIgnoreRemoveSelect(cog, guild_id, options))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Ignore List", description="Manage users and roles the bot will ignore.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))


class LFGIgnoreRemoveSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption]):
        self.cog = cog
        self.guild_id = guild_id
        super().__init__(placeholder="Select entries to remove...", options=options, min_values=1, max_values=len(options))

    async def callback(self, interaction: discord.Interaction):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            for eid in self.values:
                gc["ignore_list"].pop(eid, None)
            await self.cog._save()

        removed = len(self.values)
        embed = discord.Embed(title="Ignore List", description=f"Removed {removed} entry(ies).", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGIgnoreMenuView(self.cog))


# ============================================================
# VIP Role View
# ============================================================
class LFGVIPRoleSelectView(discord.ui.View):
    def __init__(self, cog: "LFG"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select VIP role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        guild_id = str(interaction.guild.id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["vip_role_id"] = role.id
            await self.cog._save()

        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        embed.set_footer(text=f"VIP role set to {role.name}.")
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))

    @discord.ui.button(label="Clear", style=discord.ButtonStyle.red, row=1)
    async def clear_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["vip_role_id"] = None
            await self.cog._save()

        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        embed.set_footer(text="VIP role cleared.")
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


# ============================================================
# Stats Views
# ============================================================
class LFGStatsMenuView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="View Stats", style=discord.ButtonStyle.primary, row=0)
    async def view_stats_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
        embed = _build_stats_embed(interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGStatsView(self.cog, self.guild_id))

    @discord.ui.button(label="Manage Values", style=discord.ButtonStyle.primary, row=0)
    async def manage_values_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Manage Values", description="Add or remove points from a user.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGManageValuesView(self.cog, self.guild_id))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
        embed = _build_panel_embed(self.cog, interaction.guild, gc)
        await interaction.response.edit_message(embed=embed, view=LFGAdminPanelView(self.cog))


class LFGManageValuesView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="Add Points", style=discord.ButtonStyle.green, row=0)
    async def add_points_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Add Points", description="Select a user to add points to.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGManagePointsUserView(self.cog, self.guild_id, mode="add"))

    @discord.ui.button(label="Remove Points", style=discord.ButtonStyle.red, row=0)
    async def remove_points_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Remove Points", description="Select a user to remove points from.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGManagePointsUserView(self.cog, self.guild_id, mode="remove"))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="VIP Stats", description="Choose an option below.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGStatsMenuView(self.cog, self.guild_id))


class LFGManagePointsUserView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, mode: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.mode = mode

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        modal = LFGManagePointsModal(self.cog, self.guild_id, str(user.id), user.display_name, self.mode)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="Manage Values", description="Add or remove points from a user.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGManageValuesView(self.cog, self.guild_id))


class LFGManagePointsModal(discord.ui.Modal):
    amount = discord.ui.TextInput(label="Points", placeholder="Enter a number...", min_length=1, max_length=6)

    def __init__(self, cog: "LFG", guild_id: str, user_id: str, display_name: str, mode: str):
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.mode = mode
        title = f"{'Add' if mode == 'add' else 'Remove'} Points — {display_name}"
        super().__init__(title=title[:45])

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(self.amount.value)
            if value <= 0:
                raise ValueError
        except ValueError:
            await interaction.response.send_message("Please enter a positive whole number.", ephemeral=True)
            return

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            if self.mode == "add":
                for _ in range(value):
                    gc["vip_stats"].append({
                        "user_id": self.user_id,
                        "trigger_role_id": "admin_adjustment",
                        "timestamp": time.time(),
                    })
            else:
                # Remove up to `value` entries for this user (oldest first)
                remaining = value
                new_stats = []
                for entry in gc["vip_stats"]:
                    if remaining > 0 and entry["user_id"] == self.user_id:
                        remaining -= 1
                        continue
                    new_stats.append(entry)
                gc["vip_stats"] = new_stats
            await self.cog._save()

        action = "Added" if self.mode == "add" else "Removed"
        member = interaction.guild.get_member(int(self.user_id))
        name = member.display_name if member else f"User ({self.user_id})"
        embed = discord.Embed(
            title="Manage Values",
            description=f"{action} **{value}** point{'s' if value != 1 else ''} {'to' if self.mode == 'add' else 'from'} **{name}**.",
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=LFGManageValuesView(self.cog, self.guild_id))


class LFGStatsView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, trigger_filter: str = None, days_filter: int = None):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.trigger_filter = trigger_filter
        self.days_filter = days_filter

    @discord.ui.button(label="Filter by Trigger", style=discord.ButtonStyle.primary, row=0)
    async def trigger_filter_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
            triggers = gc["triggers"]

        if not triggers:
            await interaction.response.send_message("No triggers configured.", ephemeral=True)
            return

        options = [discord.SelectOption(label="All Triggers", value="all", default=self.trigger_filter is None)]
        for rid in triggers:
            role = guild.get_role(int(rid))
            name = role.name if role else f"Unknown ({rid})"
            options.append(discord.SelectOption(label=name, value=rid, default=self.trigger_filter == rid))

        embed = discord.Embed(title="Filter by Trigger", description="Select a trigger role to filter stats.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGStatsTriggerSelectView(self.cog, self.guild_id, options, self.days_filter))

    @discord.ui.button(label="All Time", style=discord.ButtonStyle.secondary, row=1)
    async def all_time_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_time(interaction, None)

    @discord.ui.button(label="7 Days", style=discord.ButtonStyle.secondary, row=1)
    async def seven_days_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_time(interaction, 7)

    @discord.ui.button(label="14 Days", style=discord.ButtonStyle.secondary, row=1)
    async def fourteen_days_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_time(interaction, 14)

    @discord.ui.button(label="30 Days", style=discord.ButtonStyle.secondary, row=1)
    async def thirty_days_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._update_time(interaction, 30)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(title="VIP Stats", description="Choose an option below.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=embed, view=LFGStatsMenuView(self.cog, self.guild_id))

    async def _update_time(self, interaction: discord.Interaction, days: Optional[int]):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
        embed = _build_stats_embed(interaction.guild, gc, self.trigger_filter, days)
        await interaction.response.edit_message(embed=embed, view=LFGStatsView(self.cog, self.guild_id, self.trigger_filter, days))


class LFGStatsTriggerSelectView(discord.ui.View):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption], days_filter: int = None):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.days_filter = days_filter
        self.add_item(LFGStatsTriggerSelect(cog, guild_id, options, days_filter))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
        embed = _build_stats_embed(interaction.guild, gc, None, self.days_filter)
        await interaction.response.edit_message(embed=embed, view=LFGStatsView(self.cog, self.guild_id, None, self.days_filter))


class LFGStatsTriggerSelect(discord.ui.Select):
    def __init__(self, cog: "LFG", guild_id: str, options: List[discord.SelectOption], days_filter: int = None):
        self.cog = cog
        self.guild_id = guild_id
        self.days_filter = days_filter
        super().__init__(placeholder="Select a trigger...", options=options)

    async def callback(self, interaction: discord.Interaction):
        selected = self.values[0]
        trigger_filter = None if selected == "all" else selected

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(self.guild_id)
        embed = _build_stats_embed(interaction.guild, gc, trigger_filter, self.days_filter)
        await interaction.response.edit_message(embed=embed, view=LFGStatsView(self.cog, self.guild_id, trigger_filter, self.days_filter))


# ============================================================
# Main Cog
# ============================================================
class LFG(commands.Cog, name="lfg"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config_manager = ConfigManager(CONFIG_FILE)
        self.config: Dict[str, Any] = {}
        self.config_lock = asyncio.Lock()

    async def cog_load(self):
        async with self.config_lock:
            self.config = await self.config_manager.load()
        logger.info("LFG cog loaded")

    def _get_guild_config(self, guild_id: str) -> Dict[str, Any]:
        """Get or create guild config. Must be called within config_lock."""
        if guild_id not in self.config:
            self.config[guild_id] = _default_guild_config()
        gc = self.config[guild_id]
        # Ensure all keys exist for older configs
        for key, val in _default_guild_config().items():
            gc.setdefault(key, val)
        return gc

    async def _save(self):
        """Save config. Must be called within config_lock."""
        await self.config_manager.save(self.config)

    # --- Slash Command ---
    lfg_group = app_commands.Group(name="lfg", description="LFG alert management")

    @lfg_group.command(name="panel", description="Open the LFG admin panel")
    @app_commands.default_permissions(manage_guild=True)
    async def panel(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to use this.", ephemeral=True)
            return

        guild_id = str(interaction.guild.id)
        async with self.config_lock:
            gc = self._get_guild_config(guild_id)

        embed = _build_panel_embed(self, interaction.guild, gc)
        await interaction.response.send_message(embed=embed, view=LFGAdminPanelView(self), ephemeral=True)

    # --- on_message Listener ---
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        # Skip forwarded messages — they carry the original's role mentions
        # and set message.reference, which would false-trigger alerts and VIP tracking
        if getattr(message, "message_snapshots", None):
            return

        guild_id = str(message.guild.id)

        async with self.config_lock:
            gc = self.config.get(guild_id)
            if not gc:
                return
            lfg_channel_id = gc.get("lfg_channel_id")
            triggers = gc.get("triggers", {})
            active_triggers = set(gc.get("active_triggers", []))
            ignore_list = gc.get("ignore_list", {})
            subscriptions = gc.get("subscriptions", {})
            vip_role_id = gc.get("vip_role_id")

        if not lfg_channel_id or not triggers:
            return

        # --- VIP reply tracking ---
        if message.reference and vip_role_id:
            author_role_ids_int = {r.id for r in message.author.roles}
            if vip_role_id in author_role_ids_int:
                try:
                    ref_msg = message.reference.resolved
                    if ref_msg is None or isinstance(ref_msg, discord.DeletedReferencedMessage):
                        ref_msg = await message.channel.fetch_message(message.reference.message_id)

                    if ref_msg and ref_msg.raw_role_mentions:
                        # Skip if the referenced message author also has the VIP role
                        ref_author = ref_msg.author
                        if ref_author and not ref_author.bot:
                            ref_author_role_ids = {r.id for r in getattr(ref_author, "roles", [])}
                            if vip_role_id in ref_author_role_ids:
                                return

                        ref_mentioned = {str(r) for r in ref_msg.raw_role_mentions}
                        ref_channel_id = ref_msg.channel.id
                        ref_parent_id = getattr(ref_msg.channel, "parent_id", None)

                        for role_id_str, trigger_data in triggers.items():
                            if role_id_str not in ref_mentioned:
                                continue
                            if role_id_str not in active_triggers:
                                continue
                            watched = trigger_data["channel_id"]
                            if ref_channel_id != watched and ref_parent_id != watched:
                                continue
                            # Record the VIP response
                            async with self.config_lock:
                                gc = self._get_guild_config(guild_id)
                                gc["vip_stats"].append({
                                    "user_id": str(message.author.id),
                                    "trigger_role_id": role_id_str,
                                    "timestamp": time.time(),
                                })
                                await self._save()
                            break
                except (discord.HTTPException, discord.NotFound):
                    pass

        # --- Trigger alert logic ---
        if not message.raw_role_mentions:
            return

        mentioned_role_ids = {str(r) for r in message.raw_role_mentions}

        # Find matching active triggers in the correct channel
        matched_triggers = []
        msg_channel_id = message.channel.id
        msg_parent_id = getattr(message.channel, "parent_id", None)

        for role_id_str, trigger_data in triggers.items():
            if role_id_str not in mentioned_role_ids:
                continue
            if role_id_str not in active_triggers:
                continue
            watched = trigger_data["channel_id"]
            if msg_channel_id != watched and msg_parent_id != watched:
                continue
            matched_triggers.append(role_id_str)

        if not matched_triggers:
            return

        # Check ignore list
        author_id_str = str(message.author.id)
        author_role_ids = [str(r.id) for r in message.author.roles]

        for trigger_role_id in list(matched_triggers):
            # Check user ignore
            if author_id_str in ignore_list:
                entry = ignore_list[author_id_str]
                if entry["scope"] == "all" or trigger_role_id in entry["scope"]:
                    matched_triggers.remove(trigger_role_id)
                    continue
            # Check role ignores
            for author_role_id in author_role_ids:
                if author_role_id in ignore_list:
                    entry = ignore_list[author_role_id]
                    if entry["scope"] == "all" or trigger_role_id in entry["scope"]:
                        if trigger_role_id in matched_triggers:
                            matched_triggers.remove(trigger_role_id)
                        break

        if not matched_triggers:
            return

        # Build embed - use the first matched trigger's color
        first_trigger = matched_triggers[0]
        alert_color = triggers.get(first_trigger, {}).get("color", EMBED_COLOR)

        embed = discord.Embed(description=message.content, color=alert_color, timestamp=message.created_at)
        embed.set_author(name=message.author.display_name, icon_url=message.author.display_avatar.url)
        embed.add_field(name="Source", value=f"[Jump to message]({message.jump_url})", inline=False)

        # Gather role names for the embed title
        role_names = []
        for rid in matched_triggers:
            role = message.guild.get_role(int(rid))
            role_names.append(role.name if role else rid)
        embed.title = f"LFG Alert - {', '.join(role_names)}"

        # Gather subscribers to mention
        mention_user_ids = set()
        for user_id_str, sub_roles in subscriptions.items():
            for trigger_role_id in matched_triggers:
                if trigger_role_id in sub_roles:
                    mention_user_ids.add(user_id_str)
                    break

        # Don't ping the original author
        mention_user_ids.discard(author_id_str)

        mention_text = " ".join(f"<@{uid}>" for uid in mention_user_ids) if mention_user_ids else None

        # Send to LFG channel
        lfg_channel = message.guild.get_channel(lfg_channel_id)
        if not lfg_channel:
            return

        try:
            await lfg_channel.send(
                content=mention_text,
                embed=embed,
                view=LFGBellView(self),
                allowed_mentions=discord.AllowedMentions(users=True, roles=False),
            )
        except discord.HTTPException as e:
            logger.error(f"Failed to send LFG alert: {e}")


async def setup(bot: commands.Bot):
    cog = LFG(bot)
    bot.add_view(LFGBellView(cog))
    await bot.add_cog(cog)
