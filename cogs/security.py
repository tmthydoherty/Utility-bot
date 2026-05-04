import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import json
import os
import re
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# --- Basic Setup ---
log = logging.getLogger(__name__)

CONFIG_FILE = "security_config.json"
INFRACTIONS_FILE = "security_infractions.json"
EMBED_COLOR = 0xE74C3C  # Red for security alerts
EMBED_COLOR_INFO = 0x3498DB  # Blue for info/panel
EMBED_COLOR_SUCCESS = 0x2ECC71  # Green for success

# --- Detection Thresholds ---
FLOOD_MSG_COUNT = 5          # messages in window to trigger
FLOOD_WINDOW_SECS = 4        # seconds
DUPLICATE_COUNT = 4           # duplicate messages to trigger
DUPLICATE_WINDOW_SECS = 10   # seconds
MASS_MENTION_THRESHOLD = 6   # unique mentions in one message
NUKE_CHANNEL_DELETE = 3       # channel deletes in window
NUKE_ROLE_DELETE = 3          # role deletes in window
NUKE_MASS_BAN = 5             # bans in window
NUKE_WINDOW_SECS = 60        # audit log action window
INVITE_STRIKE_COUNT = 3       # invite links before quarantine
INVITE_STRIKE_WINDOW = 60    # seconds
INVITE_WARN_DELETE_AFTER = 8  # auto-delete warning after N seconds

# --- Invite Link Regex ---
INVITE_PATTERN = re.compile(
    r"(?:https?://)?(?:www\.)?"
    r"(?:discord\.gg|discord(?:app)?\.com/invite)"
    r"/[a-zA-Z0-9\-]+",
    re.IGNORECASE
)

# --- Config Helpers ---

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_config(config):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

def load_infractions():
    if os.path.exists(INFRACTIONS_FILE):
        try:
            with open(INFRACTIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_infractions(data):
    with open(INFRACTIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


# ============================================================
#  Persistent Views — survive bot restarts via custom_id
# ============================================================

class InfractionActionSelect(ui.Select):
    """Dropdown on alert embeds: Ban / Kick / Start Discussion / Ignore Infraction."""

    def __init__(self, cog: "Security", infraction_id: str):
        self.cog = cog
        self.infraction_id = infraction_id
        options = [
            discord.SelectOption(label="Ban User", value="ban", description="Permanently ban the user from the server"),
            discord.SelectOption(label="Kick User", value="kick", description="Kick the user from the server"),
            discord.SelectOption(label="Start Discussion", value="discuss", description="Open a mod discussion thread"),
            discord.SelectOption(label="Ignore Infraction", value="ignore", description="Remove quarantine — false positive"),
        ]
        super().__init__(
            placeholder="Choose an action...",
            options=options,
            custom_id=f"security_action:{infraction_id}",
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message(
                embed=discord.Embed(description="You don't have permission to do this.", color=discord.Color.red()),
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True, thinking=True)
        choice = self.values[0]
        infractions = load_infractions()
        guild_key = str(interaction.guild_id)
        infraction = infractions.get(guild_key, {}).get(self.infraction_id)

        if not infraction:
            return await interaction.followup.send(
                embed=discord.Embed(description="Infraction record not found.", color=discord.Color.red()),
                ephemeral=True,
            )

        if infraction.get("resolved"):
            return await interaction.followup.send(
                embed=discord.Embed(description="This infraction has already been resolved.", color=discord.Color.yellow()),
                ephemeral=True,
            )

        user_id = infraction["user_id"]
        member = interaction.guild.get_member(user_id)
        cfg = self.cog.get_guild_config(interaction.guild_id)
        quarantine_role_id = cfg.get("quarantine_role_id")

        result_text = ""

        if choice == "ban":
            try:
                user_obj = member or await interaction.guild.fetch_member(user_id)
                await interaction.guild.ban(user_obj, reason=f"Security infraction: {infraction['rule']} — actioned by {interaction.user}")
                result_text = f"**{user_obj.display_name}** has been banned."
            except discord.NotFound:
                try:
                    await interaction.guild.ban(discord.Object(id=user_id), reason=f"Security infraction: {infraction['rule']} — actioned by {interaction.user}")
                    result_text = f"User `{user_id}` has been banned."
                except Exception as e:
                    result_text = f"Failed to ban: {e}"
            except Exception as e:
                result_text = f"Failed to ban: {e}"

        elif choice == "kick":
            if member:
                try:
                    await member.kick(reason=f"Security infraction: {infraction['rule']} — actioned by {interaction.user}")
                    result_text = f"**{member.display_name}** has been kicked."
                except Exception as e:
                    result_text = f"Failed to kick: {e}"
            else:
                result_text = "User is no longer in the server. Consider banning instead."

        elif choice == "discuss":
            result_text = await self.cog.start_modtools_discussion(interaction, user_id)

        elif choice == "ignore":
            if member and quarantine_role_id:
                quarantine_role = interaction.guild.get_role(quarantine_role_id)
                if quarantine_role and quarantine_role in member.roles:
                    try:
                        await member.remove_roles(quarantine_role, reason="Infraction ignored — false positive")
                    except discord.Forbidden:
                        pass

            # Restore roles if they were stripped (anti-nuke)
            stored_roles = infraction.get("stored_role_ids", [])
            if member and stored_roles:
                roles_to_restore = [r for r in (interaction.guild.get_role(rid) for rid in stored_roles)
                                    if r and not r.is_default() and r.is_assignable()]
                if roles_to_restore:
                    try:
                        await member.add_roles(*roles_to_restore, reason="Infraction ignored — restoring roles")
                    except discord.Forbidden:
                        pass

            # Set ignore cooldown so user doesn't re-trigger immediately
            self.cog._ignore_cooldown[interaction.guild_id][user_id] = datetime.now(timezone.utc).timestamp()

            result_text = f"Infraction ignored. Quarantine removed for <@{user_id}>."

        # Mark resolved
        infraction["resolved"] = True
        infraction["resolved_by"] = interaction.user.id
        infraction["resolved_action"] = choice
        infraction["resolved_at"] = datetime.now(timezone.utc).isoformat()
        save_infractions(infractions)

        # Update the original alert embed
        try:
            original_embed = interaction.message.embeds[0] if interaction.message.embeds else None
            if original_embed:
                action_labels = {"ban": "Banned", "kick": "Kicked", "discuss": "Discussion Started", "ignore": "Ignored (False Positive)"}
                original_embed.set_footer(text=f"Resolved: {action_labels.get(choice, choice)} by {interaction.user.display_name}")
                original_embed.color = discord.Color.dark_grey()
                await interaction.message.edit(embed=original_embed, view=None)
        except Exception:
            pass

        await interaction.followup.send(
            embed=discord.Embed(description=result_text, color=EMBED_COLOR_SUCCESS),
            ephemeral=True,
        )


class InfractionActionView(ui.View):
    """Wraps InfractionActionSelect in a persistent view."""

    def __init__(self, cog: "Security", infraction_id: str):
        super().__init__(timeout=None)
        self.add_item(InfractionActionSelect(cog, infraction_id))


# ============================================================
#  Admin Panel Views
# ============================================================

class ToggleModuleSelect(ui.Select):
    """Dropdown to enable/disable detection modules."""

    MODULES = [
        ("invite_links", "Invite Links", "Detect Discord server invite links"),
        ("message_flood", "Message Flood", "Detect rapid message spam"),
        ("duplicate_spam", "Duplicate Spam", "Detect repeated identical messages"),
        ("mass_mentions", "Mass Mentions", "Detect messages with 6+ mentions"),
        ("nuke_channel_delete", "Anti-Nuke: Channel Delete", "Detect mass channel deletion"),
        ("nuke_role_delete", "Anti-Nuke: Role Delete", "Detect mass role deletion"),
        ("nuke_mass_ban", "Anti-Nuke: Mass Ban", "Detect mass banning"),
    ]

    def __init__(self, cog: "Security"):
        self.cog = cog
        # Options are built dynamically when the panel is opened
        options = [
            discord.SelectOption(label=label, value=key, description=desc)
            for key, label, desc in self.MODULES
        ]
        super().__init__(
            placeholder="Select modules to toggle on/off...",
            options=options,
            min_values=1,
            max_values=len(options),
            custom_id="security_toggle_modules",
        )

    async def callback(self, interaction: discord.Interaction):
        cfg = self.cog.get_guild_config(interaction.guild_id)
        enabled = cfg.setdefault("enabled_modules", {m[0]: True for m in self.MODULES})
        toggled = []
        for key in self.values:
            enabled[key] = not enabled.get(key, True)
            status = "enabled" if enabled[key] else "disabled"
            label = next((l for k, l, _ in self.MODULES if k == key), key)
            icon = "ON" if enabled[key] else "OFF"
            toggled.append(f"{icon} **{label}** — {status}")
        self.cog.save()
        embed = discord.Embed(
            title="Modules Updated",
            description="\n".join(toggled),
            color=EMBED_COLOR_INFO,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


class AdminPanelView(ui.View):
    """Main /security admin panel with buttons."""

    def __init__(self, cog: "Security"):
        super().__init__(timeout=180)
        self.cog = cog

    @ui.button(label="Dashboard", style=discord.ButtonStyle.primary, row=0)
    async def dashboard_btn(self, interaction: discord.Interaction, button: ui.Button):
        cfg = self.cog.get_guild_config(interaction.guild_id)
        enabled = cfg.get("enabled_modules", {})
        infractions = load_infractions()
        guild_infractions = infractions.get(str(interaction.guild_id), {})

        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_count = sum(
            1 for inf in guild_infractions.values()
            if datetime.fromisoformat(inf["timestamp"]) >= today_start
        )
        unresolved = sum(1 for inf in guild_infractions.values() if not inf.get("resolved"))

        module_status = []
        for key, label, _ in ToggleModuleSelect.MODULES:
            on = enabled.get(key, True)
            icon = "ON" if on else "OFF"
            module_status.append(f"{icon} {label}")

        quarantine_role = f"<@&{cfg['quarantine_role_id']}>" if cfg.get("quarantine_role_id") else "*Not set*"
        alert_channel = f"<#{cfg['alert_channel_id']}>" if cfg.get("alert_channel_id") else "*Not set*"
        exempt_channels = ", ".join(f"<#{c}>" for c in cfg.get("exempt_channels", [])) or "*None*"
        mod_roles = ", ".join(f"<@&{r}>" for r in cfg.get("mod_roles", [])) or "*None*"
        mention_exempt = ", ".join(f"<@&{r}>" for r in cfg.get("mention_exempt_roles", [])) or "*None*"

        embed = discord.Embed(title="Security Dashboard", color=EMBED_COLOR_INFO)
        embed.add_field(name="Infractions Today", value=str(today_count), inline=True)
        embed.add_field(name="Unresolved", value=str(unresolved), inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)
        embed.add_field(name="Alert Channel", value=alert_channel, inline=True)
        embed.add_field(name="Quarantine Role", value=quarantine_role, inline=True)
        embed.add_field(name="Exempt Channels", value=exempt_channels, inline=False)
        embed.add_field(name="Mod Roles (Exempt)", value=mod_roles, inline=False)
        embed.add_field(name="Mention-Exempt Roles", value=mention_exempt, inline=False)
        embed.add_field(name="Modules", value="\n".join(module_status), inline=False)
        embed.set_footer(text=self.cog.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @ui.button(label="Toggle Modules", style=discord.ButtonStyle.secondary, row=0)
    async def toggle_modules_btn(self, interaction: discord.Interaction, button: ui.Button):
        embed = discord.Embed(
            title="Toggle Detection Modules",
            description="Select which modules to flip on or off.",
            color=EMBED_COLOR_INFO,
        )
        view = ui.View(timeout=60)
        view.add_item(ToggleModuleSelect(self.cog))
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @ui.button(label="Set Alert Channel", style=discord.ButtonStyle.secondary, row=1)
    async def set_alert_channel_btn(self, interaction: discord.Interaction, button: ui.Button):
        embed = discord.Embed(description="Select the channel where security alerts will be sent.", color=EMBED_COLOR_INFO)
        view = ui.View(timeout=60)
        ch_select = ui.ChannelSelect(
            placeholder="Select alert channel...",
            channel_types=[discord.ChannelType.text],
            custom_id="security_set_alert_channel",
        )

        async def ch_callback(i: discord.Interaction):
            channel_id = int(i.data["values"][0])
            cfg = self.cog.get_guild_config(i.guild_id)
            cfg["alert_channel_id"] = channel_id
            self.cog.save()
            await i.response.send_message(
                embed=discord.Embed(description=f"Alert channel set to <#{channel_id}>.", color=EMBED_COLOR_SUCCESS),
                ephemeral=True,
            )

        ch_select.callback = ch_callback
        view.add_item(ch_select)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @ui.button(label="Set Quarantine Role", style=discord.ButtonStyle.secondary, row=1)
    async def set_quarantine_role_btn(self, interaction: discord.Interaction, button: ui.Button):
        embed = discord.Embed(description="Select the role to assign when a user is quarantined.", color=EMBED_COLOR_INFO)
        view = ui.View(timeout=60)
        role_select = ui.RoleSelect(
            placeholder="Select quarantine role...",
            custom_id="security_set_quarantine_role",
        )

        async def role_callback(i: discord.Interaction):
            role_id = int(i.data["values"][0])
            cfg = self.cog.get_guild_config(i.guild_id)
            cfg["quarantine_role_id"] = role_id
            self.cog.save()
            await i.response.send_message(
                embed=discord.Embed(description=f"Quarantine role set to <@&{role_id}>.", color=EMBED_COLOR_SUCCESS),
                ephemeral=True,
            )

        role_select.callback = role_callback
        view.add_item(role_select)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @ui.button(label="Exempt Channels", style=discord.ButtonStyle.secondary, row=2)
    async def exempt_channels_btn(self, interaction: discord.Interaction, button: ui.Button):
        embed = discord.Embed(
            description="Select a channel to add/remove from the exempt list.\nExempt channels bypass all spam detection.",
            color=EMBED_COLOR_INFO,
        )
        view = ui.View(timeout=60)
        ch_select = ui.ChannelSelect(
            placeholder="Toggle channel exemption...",
            channel_types=[discord.ChannelType.text],
            custom_id="security_exempt_channel",
        )

        async def ch_callback(i: discord.Interaction):
            channel_id = int(i.data["values"][0])
            cfg = self.cog.get_guild_config(i.guild_id)
            exempt = cfg.setdefault("exempt_channels", [])
            if channel_id in exempt:
                exempt.remove(channel_id)
                msg = f"<#{channel_id}> removed from exempt list."
            else:
                exempt.append(channel_id)
                msg = f"<#{channel_id}> added to exempt list."
            self.cog.save()
            await i.response.send_message(
                embed=discord.Embed(description=msg, color=EMBED_COLOR_SUCCESS),
                ephemeral=True,
            )

        ch_select.callback = ch_callback
        view.add_item(ch_select)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @ui.button(label="Mod Roles (Exempt)", style=discord.ButtonStyle.secondary, row=2)
    async def mod_roles_btn(self, interaction: discord.Interaction, button: ui.Button):
        embed = discord.Embed(
            description="Select a role to add/remove from the exempt mod roles list.\nUsers with these roles bypass all spam detection.",
            color=EMBED_COLOR_INFO,
        )
        view = ui.View(timeout=60)
        role_select = ui.RoleSelect(
            placeholder="Toggle mod role exemption...",
            custom_id="security_mod_role",
        )

        async def role_callback(i: discord.Interaction):
            role_id = int(i.data["values"][0])
            cfg = self.cog.get_guild_config(i.guild_id)
            mod_roles = cfg.setdefault("mod_roles", [])
            if role_id in mod_roles:
                mod_roles.remove(role_id)
                msg = f"<@&{role_id}> removed from exempt roles."
            else:
                mod_roles.append(role_id)
                msg = f"<@&{role_id}> added to exempt roles."
            self.cog.save()
            await i.response.send_message(
                embed=discord.Embed(description=msg, color=EMBED_COLOR_SUCCESS),
                ephemeral=True,
            )

        role_select.callback = role_callback
        view.add_item(role_select)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @ui.button(label="Mention-Exempt Roles", style=discord.ButtonStyle.secondary, row=3)
    async def mention_exempt_roles_btn(self, interaction: discord.Interaction, button: ui.Button):
        embed = discord.Embed(
            description="Select a role to add/remove from the mention-exempt list.\nUsers with these roles can mass-mention without triggering detection.",
            color=EMBED_COLOR_INFO,
        )
        view = ui.View(timeout=60)
        role_select = ui.RoleSelect(
            placeholder="Toggle mention-exempt role...",
            custom_id="security_mention_exempt_role",
        )

        async def role_callback(i: discord.Interaction):
            role_id = int(i.data["values"][0])
            cfg = self.cog.get_guild_config(i.guild_id)
            mention_roles = cfg.setdefault("mention_exempt_roles", [])
            if role_id in mention_roles:
                mention_roles.remove(role_id)
                msg = f"<@&{role_id}> removed from mention-exempt roles."
            else:
                mention_roles.append(role_id)
                msg = f"<@&{role_id}> added to mention-exempt roles."
            self.cog.save()
            await i.response.send_message(
                embed=discord.Embed(description=msg, color=EMBED_COLOR_SUCCESS),
                ephemeral=True,
            )

        role_select.callback = role_callback
        view.add_item(role_select)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @ui.button(label="Infraction Log", style=discord.ButtonStyle.danger, row=3)
    async def infraction_log_btn(self, interaction: discord.Interaction, button: ui.Button):
        infractions = load_infractions()
        guild_infractions = infractions.get(str(interaction.guild_id), {})

        if not guild_infractions:
            return await interaction.response.send_message(
                embed=discord.Embed(description="No infractions recorded.", color=EMBED_COLOR_INFO),
                ephemeral=True,
            )

        # Show last 10
        sorted_infs = sorted(guild_infractions.values(), key=lambda x: x["timestamp"], reverse=True)[:10]
        lines = []
        for inf in sorted_infs:
            ts = int(datetime.fromisoformat(inf["timestamp"]).timestamp())
            status = "Resolved" if inf.get("resolved") else "**OPEN**"
            action = inf.get("resolved_action", "pending")
            lines.append(f"{status} <@{inf['user_id']}> — **{inf['rule']}** — {action} — <t:{ts}:R>")

        embed = discord.Embed(
            title="Recent Infractions",
            description="\n".join(lines),
            color=EMBED_COLOR,
        )
        embed.set_footer(text="Showing last 10 infractions")
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ============================================================
#  Main Cog
# ============================================================

class Security(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config()

        # Per-user message tracking for flood & duplicate detection
        # guild_id -> user_id -> list of (timestamp, content)
        self._message_history: dict[int, dict[int, list[tuple[float, str]]]] = defaultdict(lambda: defaultdict(list))

        # Per-user audit log action tracking for anti-nuke
        # guild_id -> user_id -> list of (timestamp, action_type)
        self._audit_actions: dict[int, dict[int, list[tuple[float, str]]]] = defaultdict(lambda: defaultdict(list))

        # Per-user invite link strike tracker: guild_id -> user_id -> list of timestamps
        self._invite_strikes: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))

        # Cooldown: users recently ignored (guild_id -> set of user_ids)
        self._ignore_cooldown: dict[int, dict[int, float]] = defaultdict(dict)

        self.cleanup_loop.start()

    def cog_unload(self):
        self.cleanup_loop.cancel()

    def get_footer_text(self):
        return f"{self.bot.user.name} \u2022 Security"

    # --- Config ---

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {
                "alert_channel_id": None,
                "quarantine_role_id": None,
                "mod_roles": [],
                "mention_exempt_roles": [],
                "exempt_channels": [],
                "enabled_modules": {
                    "invite_links": True,
                    "message_flood": True,
                    "duplicate_spam": True,
                    "mass_mentions": True,
                    "nuke_channel_delete": True,
                    "nuke_role_delete": True,
                    "nuke_mass_ban": True,
                },
            }
        return self.config[gid]

    def save(self):
        save_config(self.config)

    def is_module_enabled(self, guild_id: int, module: str) -> bool:
        cfg = self.get_guild_config(guild_id)
        return cfg.get("enabled_modules", {}).get(module, True)

    def is_exempt(self, member: discord.Member, channel_id: int | None = None) -> bool:
        """Check if a member is exempt from spam detection."""
        if member.bot:
            return True
        if member.guild_permissions.manage_messages:
            return True
        cfg = self.get_guild_config(member.guild.id)
        mod_roles = cfg.get("mod_roles", [])
        if any(role.id in mod_roles for role in member.roles):
            return True
        if channel_id and channel_id in cfg.get("exempt_channels", []):
            return True
        # Ignore cooldown: if a mod recently hit "Ignore" on this user, give 5 min grace
        cooldowns = self._ignore_cooldown.get(member.guild.id, {})
        if member.id in cooldowns:
            if datetime.now(timezone.utc).timestamp() - cooldowns[member.id] < 300:
                return True
            else:
                del cooldowns[member.id]
        return False

    # --- Quarantine ---

    async def quarantine_user(self, member: discord.Member, strip_roles: bool = False) -> list[int]:
        """Apply quarantine role to a user. If strip_roles=True, remove all their roles first and return the IDs."""
        cfg = self.get_guild_config(member.guild.id)
        quarantine_role_id = cfg.get("quarantine_role_id")
        stored_role_ids = []

        if not quarantine_role_id:
            return stored_role_ids

        quarantine_role = member.guild.get_role(quarantine_role_id)
        if not quarantine_role:
            return stored_role_ids

        if strip_roles:
            stored_role_ids = [r.id for r in member.roles if not r.is_default() and r.id != quarantine_role_id]
            removable = [r for r in member.roles if not r.is_default() and r.is_assignable() and r.id != quarantine_role_id]
            if removable:
                try:
                    await member.remove_roles(*removable, reason="Security: Anti-nuke quarantine — stripping all roles")
                except discord.Forbidden:
                    log.warning(f"Could not strip roles from {member} in {member.guild}")

        if quarantine_role not in member.roles:
            try:
                await member.add_roles(quarantine_role, reason="Security: Quarantined for policy violation")
            except discord.Forbidden:
                log.warning(f"Could not add quarantine role to {member} in {member.guild}")

        return stored_role_ids

    # --- Alert ---

    async def send_alert(self, guild: discord.Guild, member: discord.Member, rule: str,
                         evidence: str, stored_role_ids: list[int] | None = None):
        """Send an alert embed to the mod channel and log the infraction."""
        cfg = self.get_guild_config(guild.id)
        alert_channel_id = cfg.get("alert_channel_id")
        if not alert_channel_id:
            return

        alert_channel = guild.get_channel(alert_channel_id)
        if not alert_channel:
            return

        # Create infraction record
        infractions = load_infractions()
        guild_key = str(guild.id)
        infractions.setdefault(guild_key, {})
        infraction_id = f"{member.id}_{int(datetime.now(timezone.utc).timestamp())}"

        infractions[guild_key][infraction_id] = {
            "user_id": member.id,
            "rule": rule,
            "evidence": evidence[:1500],  # cap evidence length
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resolved": False,
            "resolved_by": None,
            "resolved_action": None,
            "resolved_at": None,
            "stored_role_ids": stored_role_ids or [],
        }
        save_infractions(infractions)

        # Build embed
        account_age = discord.utils.format_dt(member.created_at, style="R")
        joined_at = discord.utils.format_dt(member.joined_at, style="R") if member.joined_at else "Unknown"

        embed = discord.Embed(
            title="Security Alert",
            description=f"**User:** {member.mention} (`{member.id}`)\n"
                        f"**Rule Violated:** {rule}\n"
                        f"**Account Created:** {account_age}\n"
                        f"**Joined Server:** {joined_at}",
            color=EMBED_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Evidence", value=f"```\n{evidence[:1000]}\n```", inline=False)

        if stored_role_ids:
            embed.add_field(
                name="Anti-Nuke: Roles Stripped",
                value="All roles were removed to prevent further damage. Select **Ignore** to restore them.",
                inline=False,
            )

        embed.set_thumbnail(url=member.display_avatar.url)
        embed.set_footer(text=self.get_footer_text())

        view = InfractionActionView(self, infraction_id)

        try:
            await alert_channel.send(embed=embed, view=view)
        except discord.Forbidden:
            log.warning(f"Cannot send security alert in guild {guild.id} — missing permissions")

    # --- Modtools Discussion Integration ---

    async def start_modtools_discussion(self, interaction: discord.Interaction, user_id: int) -> str:
        """Create a modtools discussion thread for the given user."""
        modtools_cog = self.bot.get_cog("ModTools")
        if not modtools_cog:
            return "ModTools cog is not loaded. Cannot start discussion."

        cfg = modtools_cog.get_guild_config(interaction.guild_id)
        thread_channel_id = cfg.get("thread_channel_id")

        if not thread_channel_id:
            return "ModTools discussion channel not configured. Use `/modtools config` to set it."

        thread_channel = self.bot.get_channel(thread_channel_id)
        if not thread_channel or not isinstance(thread_channel, discord.TextChannel):
            return "ModTools discussion channel not found."

        user = interaction.guild.get_member(user_id)
        display = user.display_name if user else f"User {user_id}"

        try:
            thread = await thread_channel.create_thread(
                name=f"Security Review - {display}",
                type=discord.ChannelType.private_thread,
                invitable=False,
            )
        except discord.Forbidden:
            return "Missing permission to create private threads in the discussion channel."
        except Exception as e:
            log.error(f"Failed to create security discussion thread: {e}")
            return f"Failed to create thread: {e}"

        await thread.add_user(interaction.user)

        thread_embed = discord.Embed(
            title=f"Security Review \u2014 {display}",
            description=f"This thread was created by {interaction.user.mention} to review a security infraction for <@{user_id}>.",
            color=EMBED_COLOR,
        ).set_footer(text=self.get_footer_text())
        initial_message = await thread.send(embed=thread_embed)

        # Silently invite mod roles (same pattern as modtools)
        mod_roles_ids = cfg.get("mod_roles", [])
        if mod_roles_ids:
            await asyncio.sleep(2.5)
            role_mentions = " ".join(f"<@&{role_id}>" for role_id in mod_roles_ids)
            await initial_message.edit(content=role_mentions, allowed_mentions=discord.AllowedMentions(roles=True))

        # Track in modtools
        cfg.setdefault("threads", {})[str(thread.id)] = {
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "reminded_24h": False,
            "reminded_36h": False,
        }
        modtools_cog.save()

        return f"Discussion thread created: {thread.mention}"

    # --- Delete & Quarantine Helper ---

    async def handle_violation(self, message: discord.Message, rule: str, evidence: str):
        """Delete the message, quarantine the user, send alert."""
        # Delete the offending message
        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

        # Quarantine (no role stripping for message-based violations)
        stored_roles = await self.quarantine_user(message.author)

        # Send alert
        await self.send_alert(message.guild, message.author, rule, evidence, stored_roles)

    async def handle_flood_violation(self, guild: discord.Guild, member: discord.Member,
                                     channel: discord.TextChannel, messages_content: list[str]):
        """Handle flood/duplicate by deleting recent messages, quarantining, alerting."""
        # Bulk delete recent messages from this user in the channel
        now = datetime.now(timezone.utc)

        def is_recent_by_user(m):
            return m.author.id == member.id and (now - m.created_at).total_seconds() < 30

        try:
            deleted = await channel.purge(limit=50, check=is_recent_by_user)
            log.info(f"Purged {len(deleted)} messages from {member} in #{channel.name}")
        except discord.Forbidden:
            log.warning(f"Cannot purge messages in #{channel.name} — missing permissions")
        except Exception as e:
            log.warning(f"Purge failed in #{channel.name}: {e}")

        stored_roles = await self.quarantine_user(member)
        evidence = "\n".join(messages_content[:10])
        await self.send_alert(guild, member, "Message Flood / Duplicate Spam", evidence, stored_roles)

    # ============================================================
    #  Event Listeners — Spam Detection
    # ============================================================

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        if not isinstance(message.author, discord.Member):
            return

        member = message.author
        guild = message.guild
        channel_id = message.channel.parent_id if isinstance(message.channel, discord.Thread) else message.channel.id

        if self.is_exempt(member, channel_id):
            return

        now = datetime.now(timezone.utc).timestamp()
        history = self._message_history[guild.id][member.id]
        content = message.content or ""
        history.append((now, content))

        # Prune old entries (keep last 30 seconds)
        history[:] = [(t, c) for t, c in history if now - t < 30]

        # --- Invite Link Detection ---
        if self.is_module_enabled(guild.id, "invite_links") and INVITE_PATTERN.search(content):
            # Always delete the message
            try:
                await message.delete()
            except (discord.Forbidden, discord.NotFound):
                pass

            # Track strikes
            strikes = self._invite_strikes[guild.id][member.id]
            strikes.append(now)
            strikes[:] = [t for t in strikes if now - t < INVITE_STRIKE_WINDOW]

            if len(strikes) >= INVITE_STRIKE_COUNT:
                # 3rd strike in 60s — quarantine + full alert
                strike_count = len(strikes)
                strikes.clear()
                stored_roles = await self.quarantine_user(member)
                await self.send_alert(guild, member, f"Discord Invite Links ({strike_count})", content, stored_roles)
                history.clear()
            else:
                # Warning — auto-deleting message
                try:
                    warn_embed = discord.Embed(
                        description=f"{member.mention}, Discord invite links are not allowed here.",
                        color=discord.Color.orange(),
                    )
                    await message.channel.send(embed=warn_embed, delete_after=INVITE_WARN_DELETE_AFTER)
                except discord.Forbidden:
                    pass
            return

        # --- Mass Mention Detection ---
        if self.is_module_enabled(guild.id, "mass_mentions"):
            # Check if user has a mention-exempt role
            mention_exempt = self.get_guild_config(guild.id).get("mention_exempt_roles", [])
            is_mention_exempt = any(role.id in mention_exempt for role in member.roles)
            if not is_mention_exempt:
                unique_mentions = set()
                unique_mentions.update(m.id for m in message.mentions)
                unique_mentions.update(r.id for r in message.role_mentions)
                if message.mention_everyone:
                    unique_mentions.add("everyone")
                if len(unique_mentions) >= MASS_MENTION_THRESHOLD:
                    await self.handle_violation(message, "Mass Mentions", content)
                    history.clear()
                    return

        # --- Message Flood Detection ---
        if self.is_module_enabled(guild.id, "message_flood"):
            recent = [(t, c) for t, c in history if now - t <= FLOOD_WINDOW_SECS]
            if len(recent) >= FLOOD_MSG_COUNT:
                contents = [c for _, c in recent]
                history.clear()
                await self.handle_flood_violation(guild, member, message.channel, contents)
                return

        # --- Duplicate Spam Detection ---
        if self.is_module_enabled(guild.id, "duplicate_spam") and content.strip():
            recent = [(t, c) for t, c in history if now - t <= DUPLICATE_WINDOW_SECS]
            normalized = content.strip().lower()
            dupe_count = sum(1 for _, c in recent if c.strip().lower() == normalized)
            if dupe_count >= DUPLICATE_COUNT:
                contents = [c for _, c in recent if c.strip().lower() == normalized]
                history.clear()
                await self.handle_flood_violation(guild, member, message.channel, contents)
                return

    # ============================================================
    #  Event Listeners — Anti-Nuke (Audit Log)
    # ============================================================

    @commands.Cog.listener()
    async def on_audit_log_entry_create(self, entry: discord.AuditLogEntry):
        if not entry.guild or not entry.user:
            return
        # Don't track bot's own actions
        if entry.user.id == self.bot.user.id:
            return

        guild = entry.guild
        user = entry.user

        action_map = {
            discord.AuditLogAction.channel_delete: ("nuke_channel_delete", NUKE_CHANNEL_DELETE, "Mass Channel Deletion"),
            discord.AuditLogAction.role_delete: ("nuke_role_delete", NUKE_ROLE_DELETE, "Mass Role Deletion"),
            discord.AuditLogAction.ban: ("nuke_mass_ban", NUKE_MASS_BAN, "Mass Ban"),
        }

        action_type = action_map.get(entry.action)
        if not action_type:
            return

        module_key, threshold, rule_name = action_type
        if not self.is_module_enabled(guild.id, module_key):
            return

        now = datetime.now(timezone.utc).timestamp()
        actions = self._audit_actions[guild.id][user.id]
        actions.append((now, module_key))

        # Prune old entries
        actions[:] = [(t, a) for t, a in actions if now - t < NUKE_WINDOW_SECS]

        # Count actions of this type
        count = sum(1 for _, a in actions if a == module_key)
        if count < threshold:
            return

        # Triggered — clear to prevent re-triggering
        actions[:] = [(t, a) for t, a in actions if a != module_key]

        member = guild.get_member(user.id)
        if not member:
            return

        # Anti-nuke: strip ALL roles
        stored_role_ids = await self.quarantine_user(member, strip_roles=True)

        evidence = f"{rule_name}: {count} actions in {NUKE_WINDOW_SECS}s by {user} ({user.id})"
        await self.send_alert(guild, member, rule_name, evidence, stored_role_ids)

        log.warning(f"ANTI-NUKE: {rule_name} triggered for {user} in {guild.name}")

    # ============================================================
    #  Slash Command — Admin Panel
    # ============================================================

    @app_commands.command(name="security", description="Open the security admin panel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def security_panel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Security Admin Panel",
            description="Manage anti-spam and anti-nuke settings for this server.",
            color=EMBED_COLOR_INFO,
        )
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, view=AdminPanelView(self), ephemeral=True)

    # ============================================================
    #  Background Tasks
    # ============================================================

    @tasks.loop(minutes=5)
    async def cleanup_loop(self):
        """Periodically prune stale tracking data to prevent memory bloat."""
        now = datetime.now(timezone.utc).timestamp()
        for guild_id in list(self._message_history.keys()):
            users = self._message_history[guild_id]
            for user_id in list(users.keys()):
                users[user_id] = [(t, c) for t, c in users[user_id] if now - t < 30]
                if not users[user_id]:
                    del users[user_id]
            if not users:
                del self._message_history[guild_id]

        for guild_id in list(self._audit_actions.keys()):
            users = self._audit_actions[guild_id]
            for user_id in list(users.keys()):
                users[user_id] = [(t, a) for t, a in users[user_id] if now - t < NUKE_WINDOW_SECS]
                if not users[user_id]:
                    del users[user_id]
            if not users:
                del self._audit_actions[guild_id]

        # Prune invite strikes
        for guild_id in list(self._invite_strikes.keys()):
            users = self._invite_strikes[guild_id]
            for user_id in list(users.keys()):
                users[user_id] = [t for t in users[user_id] if now - t < INVITE_STRIKE_WINDOW]
                if not users[user_id]:
                    del users[user_id]
            if not users:
                del self._invite_strikes[guild_id]

        # Prune ignore cooldowns
        for guild_id in list(self._ignore_cooldown.keys()):
            users = self._ignore_cooldown[guild_id]
            for uid in list(users.keys()):
                if now - users[uid] >= 300:
                    del users[uid]
            if not users:
                del self._ignore_cooldown[guild_id]

    @cleanup_loop.before_loop
    async def before_cleanup(self):
        await self.bot.wait_until_ready()

    # ============================================================
    #  Persistent View Registration
    # ============================================================

    async def cog_load(self):
        """Re-register persistent views for existing infraction alerts on bot restart."""
        infractions = load_infractions()
        for guild_key, guild_infs in infractions.items():
            for inf_id, inf_data in guild_infs.items():
                if not inf_data.get("resolved"):
                    self.bot.add_view(InfractionActionView(self, inf_id))


async def setup(bot: commands.Bot):
    await bot.add_cog(Security(bot))
