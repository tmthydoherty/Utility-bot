"""
Role Alerts Cog
---------------
When a user gains a tracked role, sends an alert to a configured channel.
Admins can claim the alert and open a private thread with the user.

Features:
- Multiple tracked roles with per-role settings (channel, admin role, ping, etc.)
- Ticket-style claim system (one admin claims, others see it's taken)
- Private thread creation with only claimer + user
- Close button to delete threads with confirmation
- Admin panel for all settings
- Auto-archive after 3 days, auto-delete after 7 days
- Cooldown protection
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
import asyncio

logger = logging.getLogger('role_alerts')
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

DB_PATH = "data/role_alerts.db"

# Constants
AUTO_ARCHIVE_DURATION = 4320  # 3 days in minutes
AUTO_DELETE_DAYS = 7


# ============================================================================
# DATABASE SETUP
# ============================================================================

async def init_db():
    """Initialize the database tables."""
    import os
    os.makedirs("data", exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        # Guild settings - minimal now, just for global toggle and thread format
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                alert_channel_id INTEGER,
                log_channel_id INTEGER,
                admin_role_id INTEGER,
                welcome_message TEXT DEFAULT 'Hello {user}! An admin will be with you shortly.',
                thread_name_format TEXT DEFAULT '{user}-{role}',
                enabled INTEGER DEFAULT 1
            )
        """)

        # Tracked roles - each has its own complete settings
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tracked_roles (
                guild_id INTEGER,
                role_id INTEGER,
                ping_enabled INTEGER DEFAULT 0,
                ping_role_id INTEGER,
                alert_channel_id INTEGER,
                thread_channel_id INTEGER,
                admin_role_id INTEGER,
                welcome_message TEXT,
                thread_name_format TEXT,
                PRIMARY KEY (guild_id, role_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS active_alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                channel_id INTEGER,
                message_id INTEGER,
                user_id INTEGER,
                role_id INTEGER,
                claimed_by INTEGER,
                thread_id INTEGER,
                created_at TEXT,
                claimed_at TEXT,
                status TEXT DEFAULT 'pending'
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS role_cooldowns (
                guild_id INTEGER,
                user_id INTEGER,
                role_id INTEGER,
                last_alert TEXT,
                PRIMARY KEY (guild_id, user_id, role_id)
            )
        """)

        # Migration: add new columns to tracked_roles if they don't exist
        cursor = await db.execute("PRAGMA table_info(tracked_roles)")
        columns = {row[1] for row in await cursor.fetchall()}
        if 'alert_channel_id' not in columns:
            await db.execute("ALTER TABLE tracked_roles ADD COLUMN alert_channel_id INTEGER")
        if 'thread_channel_id' not in columns:
            await db.execute("ALTER TABLE tracked_roles ADD COLUMN thread_channel_id INTEGER")
        if 'welcome_message' not in columns:
            await db.execute("ALTER TABLE tracked_roles ADD COLUMN welcome_message TEXT")
        if 'admin_role_id' not in columns:
            await db.execute("ALTER TABLE tracked_roles ADD COLUMN admin_role_id INTEGER")
        if 'thread_name_format' not in columns:
            await db.execute("ALTER TABLE tracked_roles ADD COLUMN thread_name_format TEXT")
        if 'bypass_role_id' not in columns:
            await db.execute("ALTER TABLE tracked_roles ADD COLUMN bypass_role_id INTEGER")

        await db.commit()


# ============================================================================
# ALERT VIEWS (for the actual alerts, not config)
# ============================================================================

class CloseConfirmView(discord.ui.View):
    """Confirmation view for closing/deleting a thread."""

    def __init__(self, cog: 'RoleAlerts', alert_id: int, thread_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.alert_id = alert_id
        self.thread_id = thread_id

    @discord.ui.button(label="Yes, Delete Thread", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Confirm thread deletion."""
        try:
            thread = interaction.guild.get_channel_or_thread(self.thread_id)
            if thread:
                await thread.delete()

            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE active_alerts SET status = 'closed', thread_id = NULL WHERE alert_id = ?",
                    (self.alert_id,)
                )
                await db.commit()

            alert_data = await self.cog.get_alert_data(self.alert_id)
            if alert_data:
                try:
                    channel = interaction.guild.get_channel(alert_data['channel_id'])
                    if channel:
                        message = await channel.fetch_message(alert_data['message_id'])
                        embed = message.embeds[0] if message.embeds else None
                        if embed:
                            embed.color = discord.Color.dark_gray()
                            embed.set_footer(text=f"Closed by {interaction.user.display_name}")
                        await message.edit(embed=embed, view=None)
                except Exception as e:
                    logger.error(f"Error updating alert message after close: {e}")

            await interaction.response.edit_message(content="Thread deleted.", view=None)

        except discord.NotFound:
            await interaction.response.edit_message(content="Thread was already deleted.", view=None)
        except Exception as e:
            await interaction.response.edit_message(content=f"Error: {e}", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel thread deletion."""
        await interaction.response.edit_message(content="Cancelled.", view=None)


class ClaimedAlertView(discord.ui.View):
    """View shown after an alert is claimed. Uses dynamic custom_ids for persistence."""

    def __init__(self, cog: 'RoleAlerts', alert_id: int, thread_id: int, role_id: int = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.alert_id = alert_id
        self.thread_id = thread_id
        self.role_id = role_id

        # Create buttons with dynamic custom_ids that encode the alert data
        join_btn = discord.ui.Button(
            label="Join",
            style=discord.ButtonStyle.primary,
            custom_id=f"ralert_join:{alert_id}:{thread_id}:{role_id or 0}"
        )
        join_btn.callback = self.join_callback
        self.add_item(join_btn)

        close_btn = discord.ui.Button(
            label="Close Thread",
            style=discord.ButtonStyle.danger,
            custom_id=f"ralert_close:{alert_id}:{thread_id}:{role_id or 0}"
        )
        close_btn.callback = self.close_callback
        self.add_item(close_btn)

    async def join_callback(self, interaction: discord.Interaction):
        """Allow admins to join the thread."""
        role_settings = None
        if self.role_id:
            role_settings = await self.cog.get_tracked_role_settings(interaction.guild_id, self.role_id)

        is_admin = interaction.user.guild_permissions.administrator
        if role_settings and role_settings.get('admin_role_id'):
            admin_role = interaction.guild.get_role(role_settings['admin_role_id'])
            if admin_role and admin_role in interaction.user.roles:
                is_admin = True
        if hasattr(self.cog.bot, 'is_bot_admin'):
            is_admin = is_admin or self.cog.bot.is_bot_admin(interaction.user)

        if not is_admin:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        thread = interaction.guild.get_channel_or_thread(self.thread_id)
        if not thread:
            await interaction.response.send_message("Thread no longer exists.", ephemeral=True)
            return

        try:
            await thread.add_user(interaction.user)
            await interaction.response.send_message(f"You've been added to {thread.mention}", ephemeral=True)
        except discord.HTTPException as e:
            await interaction.response.send_message(f"Failed to join: {e}", ephemeral=True)

    async def close_callback(self, interaction: discord.Interaction):
        """Handle close button click."""
        role_settings = None
        if self.role_id:
            role_settings = await self.cog.get_tracked_role_settings(interaction.guild_id, self.role_id)

        is_admin = interaction.user.guild_permissions.administrator
        if role_settings and role_settings.get('admin_role_id'):
            admin_role = interaction.guild.get_role(role_settings['admin_role_id'])
            if admin_role and admin_role in interaction.user.roles:
                is_admin = True
        if hasattr(self.cog.bot, 'is_bot_admin'):
            is_admin = is_admin or self.cog.bot.is_bot_admin(interaction.user)

        if not is_admin:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        confirm_view = CloseConfirmView(self.cog, self.alert_id, self.thread_id)
        await interaction.response.send_message(
            "Delete this thread? This cannot be undone.",
            view=confirm_view,
            ephemeral=True
        )


class ClaimButton(discord.ui.View):
    """View with the claim button for role alerts. Uses dynamic custom_ids for persistence."""

    def __init__(self, cog: 'RoleAlerts', alert_id: int, user_id: int, role_name: str, role_id: int = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.alert_id = alert_id
        self.user_id = user_id
        self.role_name = role_name
        self.role_id = role_id

        # Create buttons with dynamic custom_ids that encode the alert data
        claim_btn = discord.ui.Button(
            label="Claim",
            style=discord.ButtonStyle.primary,
            custom_id=f"ralert_claim:{alert_id}:{user_id}:{role_id or 0}"
        )
        claim_btn.callback = self.claim_callback
        self.add_item(claim_btn)

        dismiss_btn = discord.ui.Button(
            label="Dismiss",
            style=discord.ButtonStyle.secondary,
            custom_id=f"ralert_dismiss:{alert_id}:{user_id}:{role_id or 0}"
        )
        dismiss_btn.callback = self.dismiss_callback
        self.add_item(dismiss_btn)

    async def claim_callback(self, interaction: discord.Interaction):
        """Handle the claim button click."""
        role_settings = None
        if self.role_id:
            role_settings = await self.cog.get_tracked_role_settings(interaction.guild_id, self.role_id)

        is_admin = False
        if interaction.user.guild_permissions.administrator:
            is_admin = True
        elif role_settings and role_settings.get('admin_role_id'):
            admin_role = interaction.guild.get_role(role_settings['admin_role_id'])
            if admin_role and admin_role in interaction.user.roles:
                is_admin = True

        if hasattr(self.cog.bot, 'is_bot_admin'):
            is_admin = is_admin or self.cog.bot.is_bot_admin(interaction.user)

        if not is_admin:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT claimed_by, status FROM active_alerts WHERE alert_id = ?",
                (self.alert_id,)
            )
            row = await cursor.fetchone()

            if not row:
                await interaction.response.send_message("Alert no longer exists.", ephemeral=True)
                return

            if row[0] is not None or row[1] == 'claimed':
                claimer = interaction.guild.get_member(row[0])
                claimer_name = claimer.display_name if claimer else "Unknown"
                await interaction.response.send_message(f"Already claimed by {claimer_name}.", ephemeral=True)
                return

            # Check for existing active thread for this user/role
            cursor = await db.execute(
                """SELECT thread_id FROM active_alerts
                   WHERE guild_id = ? AND user_id = ? AND role_id = ?
                   AND status = 'claimed' AND thread_id IS NOT NULL""",
                (interaction.guild_id, self.user_id, self.role_id)
            )
            existing = await cursor.fetchone()
            if existing:
                thread = interaction.guild.get_channel_or_thread(existing[0])
                if thread:
                    await interaction.response.send_message(
                        f"There's already an active thread for this user: {thread.mention}",
                        ephemeral=True
                    )
                    return

            # Mark as claimed immediately to prevent race condition
            await db.execute(
                "UPDATE active_alerts SET claimed_by = ?, status = 'claimed' WHERE alert_id = ? AND claimed_by IS NULL",
                (interaction.user.id, self.alert_id)
            )
            await db.commit()

        await interaction.response.defer()

        target_user = interaction.guild.get_member(self.user_id)
        if not target_user:
            await interaction.followup.send("User is no longer in the server.", ephemeral=True)
            return

        try:
            # Use per-role thread name format or default
            thread_format = (role_settings.get('thread_name_format') if role_settings else None) or '{user}-{role}'
            thread_name = thread_format.format(
                user=target_user.display_name[:20],
                role=self.role_name[:20],
                date=datetime.now(timezone.utc).strftime("%m-%d")
            )[:100]

            # Use parent channel (thread_channel_id) if set, otherwise alert channel
            thread_channel = interaction.channel
            if role_settings and role_settings.get('thread_channel_id'):
                thread_channel = interaction.guild.get_channel(role_settings['thread_channel_id'])
                if not thread_channel:
                    thread_channel = interaction.channel

            thread = await thread_channel.create_thread(
                name=thread_name,
                type=discord.ChannelType.private_thread,
                auto_archive_duration=AUTO_ARCHIVE_DURATION,
                reason=f"Role alert claimed by {interaction.user}"
            )

            await thread.add_user(target_user)

            # Use per-role welcome message
            welcome_msg_template = role_settings.get('welcome_message') if role_settings else None

            # Only send welcome message if set
            if welcome_msg_template:
                welcome_msg = welcome_msg_template.format(
                    user=target_user.mention,
                    role=self.role_name,
                    admin=interaction.user.mention
                )
                await thread.send(welcome_msg)

            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("""
                    UPDATE active_alerts
                    SET thread_id = ?, claimed_at = ?
                    WHERE alert_id = ?
                """, (thread.id, datetime.now(timezone.utc).isoformat(), self.alert_id))
                await db.commit()

            embed = interaction.message.embeds[0] if interaction.message.embeds else None
            if embed:
                embed.color = discord.Color.green()
                embed.description = f"{embed.description}\n{thread.mention}"
                embed.set_footer(text=f"Claimed by {interaction.user.display_name}")

            new_view = ClaimedAlertView(self.cog, self.alert_id, thread.id, self.role_id)

            await interaction.message.edit(embed=embed, view=new_view)
            await interaction.followup.send(f"Thread created: {thread.mention}", ephemeral=True)

        except discord.Forbidden:
            await interaction.followup.send("Missing permissions to create threads.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error creating thread: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    async def dismiss_callback(self, interaction: discord.Interaction):
        """Dismiss the alert without creating a thread."""
        role_settings = None
        if self.role_id:
            role_settings = await self.cog.get_tracked_role_settings(interaction.guild_id, self.role_id)

        is_admin = interaction.user.guild_permissions.administrator
        if role_settings and role_settings.get('admin_role_id'):
            admin_role = interaction.guild.get_role(role_settings['admin_role_id'])
            if admin_role and admin_role in interaction.user.roles:
                is_admin = True
        if hasattr(self.cog.bot, 'is_bot_admin'):
            is_admin = is_admin or self.cog.bot.is_bot_admin(interaction.user)

        if not is_admin:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_alerts SET status = 'dismissed', claimed_by = ? WHERE alert_id = ?",
                (interaction.user.id, self.alert_id)
            )
            await db.commit()

        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = discord.Color.dark_gray()
            embed.set_footer(text=f"Dismissed by {interaction.user.display_name}")

        await interaction.response.edit_message(embed=embed, view=None)


# ============================================================================
# CONFIG PANEL - New/Manage flow
# ============================================================================

class ConfigPanel(discord.ui.View):
    """
    Main configuration panel. Starts with New/Manage buttons.
    """

    def __init__(self, cog: 'RoleAlerts', guild: discord.Guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild
        self.mode = "home"  # home, new_select_role, new_settings, manage_list, edit_role
        self.selected_role_id: Optional[int] = None
        self.new_role_data: dict = {}  # Temporary storage for new role wizard
        self.message: Optional[discord.Message] = None

    async def build_embed(self) -> discord.Embed:
        """Build the embed based on current mode."""
        tracked = await self.cog.get_tracked_roles_with_settings(self.guild.id)

        if self.mode == "home":
            return self._build_home_embed(tracked)
        elif self.mode == "new_select_role":
            return self._build_new_select_role_embed()
        elif self.mode == "new_settings":
            return await self._build_new_settings_embed()
        elif self.mode == "manage_list":
            return self._build_manage_list_embed(tracked)
        elif self.mode == "edit_role":
            return await self._build_edit_role_embed(tracked)
        return discord.Embed(title="Error", color=discord.Color.red())

    def _build_home_embed(self, tracked: list) -> discord.Embed:
        """Build the home screen embed."""
        embed = discord.Embed(
            title="Role Alerts",
            description="Create alerts when users gain specific roles. Admins can claim alerts and open private threads.",
            color=discord.Color.blue()
        )

        if tracked:
            roles_text = []
            for rd in tracked[:10]:
                role = self.guild.get_role(rd['role_id'])
                if role:
                    channel = self.guild.get_channel(rd['alert_channel_id']) if rd.get('alert_channel_id') else None
                    channel_text = f" → {channel.mention}" if channel else " → No channel set"
                    roles_text.append(f"{role.mention}{channel_text}")
            if len(tracked) > 10:
                roles_text.append(f"...and {len(tracked) - 10} more")
            embed.add_field(name=f"Active Role Alerts ({len(tracked)})", value="\n".join(roles_text), inline=False)
        else:
            embed.add_field(name="No Role Alerts", value="Click **New** to create your first role alert.", inline=False)

        return embed

    def _build_new_select_role_embed(self) -> discord.Embed:
        """Build the embed for selecting a role in new wizard."""
        embed = discord.Embed(
            title="New Role Alert - Step 1",
            description="Select the role to track. When a user gains this role, an alert will be sent.",
            color=discord.Color.green()
        )
        return embed

    async def _build_new_settings_embed(self) -> discord.Embed:
        """Build the embed for configuring new role alert settings."""
        role = self.guild.get_role(self.selected_role_id)
        embed = discord.Embed(
            title=f"New Role Alert - Configure: {role.name if role else 'Unknown'}",
            description="Configure the settings for this role alert. All fields are specific to this role.",
            color=role.color if role else discord.Color.green()
        )

        data = self.new_role_data
        alert_ch = f"<#{data['alert_channel_id']}>" if data.get('alert_channel_id') else "**Not set (required)**"
        parent_ch = f"<#{data['parent_channel_id']}>" if data.get('parent_channel_id') else "Same as alert channel"
        admin_role = f"<@&{data['admin_role_id']}>" if data.get('admin_role_id') else "Server administrators only"
        ping_role = f"<@&{data['ping_role_id']}>" if data.get('ping_role_id') else "None"
        bypass_role = f"<@&{data['bypass_role_id']}>" if data.get('bypass_role_id') else "None"
        welcome = data.get('welcome_message') or "None"
        if len(welcome) > 50:
            welcome = welcome[:47] + "..."

        embed.add_field(name="Alert Channel", value=alert_ch, inline=True)
        embed.add_field(name="Parent Channel", value=parent_ch, inline=True)
        embed.add_field(name="Admin Role", value=admin_role, inline=True)
        embed.add_field(name="Ping Role", value=ping_role, inline=True)
        embed.add_field(name="Bypass Role", value=bypass_role, inline=True)
        embed.add_field(name="Welcome Message", value=welcome, inline=False)

        if not data.get('alert_channel_id'):
            embed.set_footer(text="⚠️ Alert channel is required before saving")
        else:
            embed.set_footer(text="Click Save when ready")

        return embed

    def _build_manage_list_embed(self, tracked: list) -> discord.Embed:
        """Build the embed for managing existing role alerts."""
        embed = discord.Embed(
            title="Manage Role Alerts",
            description="Select a role alert to view, edit, or delete.",
            color=discord.Color.blue()
        )

        if tracked:
            for rd in tracked[:25]:
                role = self.guild.get_role(rd['role_id'])
                if role:
                    alert_ch = f"<#{rd['alert_channel_id']}>" if rd.get('alert_channel_id') else "Not set"
                    parent_ch = f"<#{rd['thread_channel_id']}>" if rd.get('thread_channel_id') else "Same as alert"
                    admin = f"<@&{rd['admin_role_id']}>" if rd.get('admin_role_id') else "Admins only"
                    bypass = f"<@&{rd['bypass_role_id']}>" if rd.get('bypass_role_id') else "None"
                    embed.add_field(
                        name=role.name,
                        value=f"Alert: {alert_ch}\nParent: {parent_ch}\nAdmin: {admin}\nBypass: {bypass}",
                        inline=True
                    )
        else:
            embed.add_field(name="No Role Alerts", value="No role alerts configured yet.", inline=False)

        return embed

    async def _build_edit_role_embed(self, tracked: list) -> discord.Embed:
        """Build the embed for editing a role alert."""
        role = self.guild.get_role(self.selected_role_id)
        role_data = None
        for rd in tracked:
            if rd['role_id'] == self.selected_role_id:
                role_data = rd
                break

        embed = discord.Embed(
            title=f"Edit Role Alert: {role.name if role else 'Unknown'}",
            color=role.color if role else discord.Color.blue()
        )

        if role_data:
            alert_ch = f"<#{role_data['alert_channel_id']}>" if role_data.get('alert_channel_id') else "Not set"
            parent_ch = f"<#{role_data['thread_channel_id']}>" if role_data.get('thread_channel_id') else "Same as alert"
            admin_role = f"<@&{role_data['admin_role_id']}>" if role_data.get('admin_role_id') else "Server admins only"
            ping_status = "Enabled" if role_data['ping_enabled'] else "Disabled"
            ping_target = f"<@&{role_data['ping_role_id']}>" if role_data.get('ping_role_id') else "None"
            welcome = role_data.get('welcome_message')
            if welcome is None:
                welcome_display = "None"
            elif welcome == "":
                welcome_display = "*Empty (no message sent)*"
            else:
                welcome_display = welcome[:50] + "..." if len(welcome) > 50 else welcome
            thread_format = role_data.get('thread_name_format') or "{user}-{role}"

            bypass = f"<@&{role_data['bypass_role_id']}>" if role_data.get('bypass_role_id') else "None"

            embed.add_field(name="Alert Channel", value=alert_ch, inline=True)
            embed.add_field(name="Parent Channel", value=parent_ch, inline=True)
            embed.add_field(name="Admin Role", value=admin_role, inline=True)
            embed.add_field(name="Ping Status", value=ping_status, inline=True)
            embed.add_field(name="Ping Role", value=ping_target, inline=True)
            embed.add_field(name="Bypass Role", value=bypass, inline=True)
            embed.add_field(name="Thread Format", value=f"`{thread_format}`", inline=True)
            embed.add_field(name="Welcome Message", value=welcome_display, inline=False)
            embed.description = "Use the dropdowns and buttons below to edit settings."
        else:
            embed.description = "Role alert not found."

        return embed

    async def rebuild_items(self):
        """Rebuild view items based on current mode."""
        self.clear_items()

        if self.mode == "home":
            self._add_home_items()
        elif self.mode == "new_select_role":
            self._add_new_select_role_items()
        elif self.mode == "new_settings":
            self._add_new_settings_items()
        elif self.mode == "manage_list":
            await self._add_manage_list_items()
        elif self.mode == "edit_role":
            self._add_edit_role_items()

    def _add_home_items(self):
        """Add items for home view - New and Manage buttons."""
        new_btn = discord.ui.Button(label="New", style=discord.ButtonStyle.success, row=0)
        new_btn.callback = self.on_new
        self.add_item(new_btn)

        manage_btn = discord.ui.Button(label="Manage", style=discord.ButtonStyle.primary, row=0)
        manage_btn.callback = self.on_manage
        self.add_item(manage_btn)

    def _add_new_select_role_items(self):
        """Add items for selecting role in new wizard."""
        role_select = discord.ui.RoleSelect(
            placeholder="Select a role to track",
            min_values=1,
            max_values=1,
            row=0
        )
        role_select.callback = self.on_new_role_selected
        self.add_item(role_select)

        back_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.on_back_home
        self.add_item(back_btn)

    def _add_new_settings_items(self):
        """Add items for configuring new role alert."""
        # Alert channel select (required)
        alert_select = discord.ui.ChannelSelect(
            placeholder="Alert Channel (required)",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            row=0
        )
        alert_select.callback = self.on_new_alert_channel
        self.add_item(alert_select)

        # Parent channel select (optional)
        parent_select = discord.ui.ChannelSelect(
            placeholder="Parent Channel for threads (optional)",
            channel_types=[discord.ChannelType.text],
            min_values=0,
            max_values=1,
            row=1
        )
        parent_select.callback = self.on_new_parent_channel
        self.add_item(parent_select)

        # Admin role select (optional)
        admin_select = discord.ui.RoleSelect(
            placeholder="Admin Role (optional, default: server admins)",
            min_values=0,
            max_values=1,
            row=2
        )
        admin_select.callback = self.on_new_admin_role
        self.add_item(admin_select)

        # Ping role select (optional)
        ping_select = discord.ui.RoleSelect(
            placeholder="Ping Role on alert (optional)",
            min_values=0,
            max_values=1,
            row=3
        )
        ping_select.callback = self.on_new_ping_role
        self.add_item(ping_select)

        # Buttons
        bypass_btn = discord.ui.Button(label="Bypass Role", style=discord.ButtonStyle.secondary, row=4)
        bypass_btn.callback = self.on_new_bypass_role
        self.add_item(bypass_btn)

        welcome_btn = discord.ui.Button(label="Set Welcome Message", style=discord.ButtonStyle.secondary, row=4)
        welcome_btn.callback = self.on_new_welcome_message
        self.add_item(welcome_btn)

        save_btn = discord.ui.Button(label="Save", style=discord.ButtonStyle.success, row=4)
        save_btn.callback = self.on_new_save
        self.add_item(save_btn)

        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.danger, row=4)
        cancel_btn.callback = self.on_back_home
        self.add_item(cancel_btn)

    async def _add_manage_list_items(self):
        """Add items for manage list view."""
        await self._add_role_alert_select()

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self.on_back_home
        self.add_item(back_btn)

    async def _add_role_alert_select(self):
        """Add select for choosing a role alert to edit."""
        tracked = await self.cog.get_tracked_roles_with_settings(self.guild.id)
        if not tracked:
            return

        options = []
        for rd in tracked[:25]:
            role = self.guild.get_role(rd['role_id'])
            if role:
                channel = self.guild.get_channel(rd['alert_channel_id']) if rd.get('alert_channel_id') else None
                desc = f"→ #{channel.name}" if channel else "No channel set"
                options.append(discord.SelectOption(
                    label=role.name[:100],
                    value=str(rd['role_id']),
                    description=desc[:100]
                ))

        if options:
            select = discord.ui.Select(
                placeholder="Select a role alert to edit",
                options=options,
                row=0
            )
            select.callback = self.on_select_role_alert
            self.add_item(select)

    def _add_edit_role_items(self):
        """Add items for editing a role alert."""
        # Alert channel select
        alert_select = discord.ui.ChannelSelect(
            placeholder="Change Alert Channel",
            channel_types=[discord.ChannelType.text],
            min_values=0,
            max_values=1,
            row=0
        )
        alert_select.callback = self.on_edit_alert_channel
        self.add_item(alert_select)

        # Parent channel select
        parent_select = discord.ui.ChannelSelect(
            placeholder="Change Parent Channel (clear = same as alert)",
            channel_types=[discord.ChannelType.text],
            min_values=0,
            max_values=1,
            row=1
        )
        parent_select.callback = self.on_edit_parent_channel
        self.add_item(parent_select)

        # Admin role select
        admin_select = discord.ui.RoleSelect(
            placeholder="Change Admin Role (clear = server admins)",
            min_values=0,
            max_values=1,
            row=2
        )
        admin_select.callback = self.on_edit_admin_role
        self.add_item(admin_select)

        # Ping role select
        ping_select = discord.ui.RoleSelect(
            placeholder="Change Ping Role (clear = no ping)",
            min_values=0,
            max_values=1,
            row=3
        )
        ping_select.callback = self.on_edit_ping_role
        self.add_item(ping_select)

        # Buttons row 4
        bypass_btn = discord.ui.Button(label="Bypass", style=discord.ButtonStyle.secondary, row=4)
        bypass_btn.callback = self.on_edit_bypass_role
        self.add_item(bypass_btn)

        welcome_btn = discord.ui.Button(label="Welcome", style=discord.ButtonStyle.secondary, row=4)
        welcome_btn.callback = self.on_edit_welcome
        self.add_item(welcome_btn)

        test_btn = discord.ui.Button(label="Test", style=discord.ButtonStyle.secondary, row=4)
        test_btn.callback = self.on_test_alert
        self.add_item(test_btn)

        delete_btn = discord.ui.Button(label="Delete", style=discord.ButtonStyle.danger, row=4)
        delete_btn.callback = self.on_delete_role_alert
        self.add_item(delete_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
        back_btn.callback = self.on_back_manage
        self.add_item(back_btn)

    async def refresh(self, interaction: discord.Interaction):
        """Refresh the embed and view."""
        await self.rebuild_items()
        embed = await self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    # ---- Home callbacks ----

    async def on_new(self, interaction: discord.Interaction):
        self.mode = "new_select_role"
        self.new_role_data = {}
        await self.refresh(interaction)

    async def on_manage(self, interaction: discord.Interaction):
        tracked = await self.cog.get_tracked_roles_with_settings(self.guild.id)
        if not tracked:
            await interaction.response.send_message("No role alerts configured yet. Use **New** to create one.", ephemeral=True)
            return
        self.mode = "manage_list"
        await self.refresh(interaction)

    async def on_back_home(self, interaction: discord.Interaction):
        self.mode = "home"
        self.selected_role_id = None
        self.new_role_data = {}
        await self.refresh(interaction)

    # ---- New wizard callbacks ----

    async def on_new_role_selected(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0])

        # Check if already tracked
        tracked = await self.cog.get_tracked_roles(self.guild.id)
        if role_id in tracked:
            await interaction.response.send_message("This role is already being tracked. Use Manage to edit it.", ephemeral=True)
            return

        self.selected_role_id = role_id
        self.new_role_data = {'role_id': role_id}
        self.mode = "new_settings"
        await self.refresh(interaction)

    async def on_new_alert_channel(self, interaction: discord.Interaction):
        if interaction.data['values']:
            self.new_role_data['alert_channel_id'] = int(interaction.data['values'][0])
        await self.refresh(interaction)

    async def on_new_parent_channel(self, interaction: discord.Interaction):
        if interaction.data['values']:
            self.new_role_data['parent_channel_id'] = int(interaction.data['values'][0])
        else:
            self.new_role_data.pop('parent_channel_id', None)
        await self.refresh(interaction)

    async def on_new_admin_role(self, interaction: discord.Interaction):
        if interaction.data['values']:
            self.new_role_data['admin_role_id'] = int(interaction.data['values'][0])
        else:
            self.new_role_data.pop('admin_role_id', None)
        await self.refresh(interaction)

    async def on_new_ping_role(self, interaction: discord.Interaction):
        if interaction.data['values']:
            self.new_role_data['ping_role_id'] = int(interaction.data['values'][0])
        else:
            self.new_role_data.pop('ping_role_id', None)
        await self.refresh(interaction)

    async def on_new_bypass_role(self, interaction: discord.Interaction):
        view = BypassRoleSelectView(self, mode="new")
        current = self.new_role_data.get('bypass_role_id')
        label = f"Current: <@&{current}>" if current else "No bypass role set."
        await interaction.response.send_message(label, view=view, ephemeral=True)

    async def on_new_welcome_message(self, interaction: discord.Interaction):
        modal = NewWelcomeModal(self)
        await interaction.response.send_modal(modal)

    async def on_new_save(self, interaction: discord.Interaction):
        if not self.new_role_data.get('alert_channel_id'):
            await interaction.response.send_message("Alert channel is required.", ephemeral=True)
            return

        data = self.new_role_data
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO tracked_roles (
                    guild_id, role_id, alert_channel_id, thread_channel_id,
                    admin_role_id, ping_role_id, ping_enabled, welcome_message, bypass_role_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, role_id) DO UPDATE SET
                    alert_channel_id = ?, thread_channel_id = ?,
                    admin_role_id = ?, ping_role_id = ?, ping_enabled = ?, welcome_message = ?,
                    bypass_role_id = ?
            """, (
                self.guild.id, data['role_id'],
                data.get('alert_channel_id'), data.get('parent_channel_id'),
                data.get('admin_role_id'), data.get('ping_role_id'),
                1 if data.get('ping_role_id') else 0, data.get('welcome_message'),
                data.get('bypass_role_id'),
                data.get('alert_channel_id'), data.get('parent_channel_id'),
                data.get('admin_role_id'), data.get('ping_role_id'),
                1 if data.get('ping_role_id') else 0, data.get('welcome_message'),
                data.get('bypass_role_id')
            ))
            await db.commit()

        self.mode = "home"
        self.selected_role_id = None
        self.new_role_data = {}
        await self.refresh(interaction)

    # ---- Manage callbacks ----

    async def on_select_role_alert(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0])
        self.selected_role_id = role_id
        self.mode = "edit_role"
        await self.refresh(interaction)

    async def on_back_manage(self, interaction: discord.Interaction):
        self.mode = "manage_list"
        self.selected_role_id = None
        await self.refresh(interaction)

    # ---- Edit role callbacks ----

    async def on_edit_alert_channel(self, interaction: discord.Interaction):
        channel_id = int(interaction.data['values'][0]) if interaction.data['values'] else None
        if channel_id:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE tracked_roles SET alert_channel_id = ? WHERE guild_id = ? AND role_id = ?",
                    (channel_id, self.guild.id, self.selected_role_id)
                )
                await db.commit()
        await self.refresh(interaction)

    async def on_edit_parent_channel(self, interaction: discord.Interaction):
        channel_id = int(interaction.data['values'][0]) if interaction.data['values'] else None
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tracked_roles SET thread_channel_id = ? WHERE guild_id = ? AND role_id = ?",
                (channel_id, self.guild.id, self.selected_role_id)
            )
            await db.commit()
        await self.refresh(interaction)

    async def on_edit_admin_role(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0]) if interaction.data['values'] else None
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tracked_roles SET admin_role_id = ? WHERE guild_id = ? AND role_id = ?",
                (role_id, self.guild.id, self.selected_role_id)
            )
            await db.commit()
        await self.refresh(interaction)

    async def on_edit_ping_role(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0]) if interaction.data['values'] else None
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tracked_roles SET ping_role_id = ?, ping_enabled = ? WHERE guild_id = ? AND role_id = ?",
                (role_id, 1 if role_id else 0, self.guild.id, self.selected_role_id)
            )
            await db.commit()
        await self.refresh(interaction)

    async def on_clear_ping(self, interaction: discord.Interaction):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tracked_roles SET ping_role_id = NULL, ping_enabled = 0 WHERE guild_id = ? AND role_id = ?",
                (self.guild.id, self.selected_role_id)
            )
            await db.commit()
        await self.refresh(interaction)

    async def on_edit_bypass_role(self, interaction: discord.Interaction):
        role_settings = await self.cog.get_tracked_role_settings(self.guild.id, self.selected_role_id)
        current = role_settings.get('bypass_role_id') if role_settings else None
        view = BypassRoleSelectView(self, mode="edit")
        label = f"Current: <@&{current}>" if current else "No bypass role set."
        await interaction.response.send_message(label, view=view, ephemeral=True)

    async def on_edit_welcome(self, interaction: discord.Interaction):
        role_settings = await self.cog.get_tracked_role_settings(self.guild.id, self.selected_role_id)
        modal = EditWelcomeModal(self, role_settings)
        await interaction.response.send_modal(modal)

    async def on_edit_thread_format(self, interaction: discord.Interaction):
        role_settings = await self.cog.get_tracked_role_settings(self.guild.id, self.selected_role_id)
        modal = EditThreadFormatModal(self, role_settings)
        await interaction.response.send_modal(modal)

    async def on_test_alert(self, interaction: discord.Interaction):
        role_settings = await self.cog.get_tracked_role_settings(self.guild.id, self.selected_role_id)
        if not role_settings or not role_settings.get('alert_channel_id'):
            await interaction.response.send_message("Set an alert channel first.", ephemeral=True)
            return

        role = self.guild.get_role(self.selected_role_id)
        if not role:
            await interaction.response.send_message("Role not found.", ephemeral=True)
            return

        await interaction.response.send_message(
            f"Sending test alert to <#{role_settings['alert_channel_id']}>",
            ephemeral=True
        )
        await self.cog.send_role_alert(interaction.user, role, role_settings)

    async def on_delete_role_alert(self, interaction: discord.Interaction):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM tracked_roles WHERE guild_id = ? AND role_id = ?",
                (self.guild.id, self.selected_role_id)
            )
            await db.commit()

        self.mode = "manage_list"
        self.selected_role_id = None

        # Check if there are any remaining role alerts
        tracked = await self.cog.get_tracked_roles_with_settings(self.guild.id)
        if not tracked:
            self.mode = "home"

        await self.refresh(interaction)


class BypassRoleSelectView(discord.ui.View):
    """Ephemeral view for selecting a bypass role."""

    def __init__(self, panel: ConfigPanel, mode: str):
        super().__init__(timeout=60)
        self.panel = panel
        self.mode = mode  # "new" or "edit"

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a bypass role", min_values=0, max_values=1, row=0)
    async def bypass_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role_id = int(interaction.data['values'][0]) if interaction.data['values'] else None

        if self.mode == "new":
            if role_id:
                self.panel.new_role_data['bypass_role_id'] = role_id
            else:
                self.panel.new_role_data.pop('bypass_role_id', None)
        elif self.mode == "edit":
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE tracked_roles SET bypass_role_id = ? WHERE guild_id = ? AND role_id = ?",
                    (role_id, self.panel.guild.id, self.panel.selected_role_id)
                )
                await db.commit()

        label = f"Bypass role set to <@&{role_id}>." if role_id else "Bypass role cleared."
        await interaction.response.edit_message(content=label, view=None)

        # Refresh the main panel
        if self.panel.message:
            await self.panel.rebuild_items()
            embed = await self.panel.build_embed()
            await self.panel.message.edit(embed=embed, view=self.panel)

    @discord.ui.button(label="Clear Bypass Role", style=discord.ButtonStyle.secondary, row=1)
    async def clear_bypass(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.mode == "new":
            self.panel.new_role_data.pop('bypass_role_id', None)
        elif self.mode == "edit":
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE tracked_roles SET bypass_role_id = NULL WHERE guild_id = ? AND role_id = ?",
                    (self.panel.guild.id, self.panel.selected_role_id)
                )
                await db.commit()

        await interaction.response.edit_message(content="Bypass role cleared.", view=None)

        if self.panel.message:
            await self.panel.rebuild_items()
            embed = await self.panel.build_embed()
            await self.panel.message.edit(embed=embed, view=self.panel)


class NewWelcomeModal(discord.ui.Modal, title="Set Welcome Message"):
    """Modal for setting welcome message in new wizard."""

    def __init__(self, panel: ConfigPanel):
        super().__init__()
        self.panel = panel

        current = panel.new_role_data.get('welcome_message', '')

        self.welcome_msg = discord.ui.TextInput(
            label="Welcome Message",
            placeholder="Hello {user}! An admin will be with you shortly.",
            default=current,
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=False
        )
        self.add_item(self.welcome_msg)

    async def on_submit(self, interaction: discord.Interaction):
        self.panel.new_role_data['welcome_message'] = self.welcome_msg.value or None
        await self.panel.rebuild_items()
        embed = await self.panel.build_embed()
        await interaction.response.edit_message(embed=embed, view=self.panel)


class EditWelcomeModal(discord.ui.Modal, title="Edit Welcome Message"):
    """Modal for editing per-role welcome message."""

    def __init__(self, panel: ConfigPanel, role_settings: dict):
        super().__init__()
        self.panel = panel

        current = role_settings.get('welcome_message') if role_settings else ''

        self.welcome_msg = discord.ui.TextInput(
            label="Welcome Message (empty = no message)",
            placeholder="Hello {user}! An admin will be with you shortly.",
            default=current or '',
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=False
        )
        self.add_item(self.welcome_msg)

    async def on_submit(self, interaction: discord.Interaction):
        value = self.welcome_msg.value if self.welcome_msg.value else None

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tracked_roles SET welcome_message = ? WHERE guild_id = ? AND role_id = ?",
                (value, interaction.guild_id, self.panel.selected_role_id)
            )
            await db.commit()

        await self.panel.rebuild_items()
        embed = await self.panel.build_embed()
        await interaction.response.edit_message(embed=embed, view=self.panel)


class EditThreadFormatModal(discord.ui.Modal, title="Edit Thread Name Format"):
    """Modal for editing per-role thread name format."""

    def __init__(self, panel: ConfigPanel, role_settings: dict):
        super().__init__()
        self.panel = panel

        current = role_settings.get('thread_name_format') if role_settings else '{user}-{role}'

        self.format_input = discord.ui.TextInput(
            label="Thread Name Format",
            placeholder="{user}-{role}",
            default=current or '{user}-{role}',
            max_length=100,
            required=False
        )
        self.add_item(self.format_input)

    async def on_submit(self, interaction: discord.Interaction):
        value = self.format_input.value if self.format_input.value else '{user}-{role}'

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tracked_roles SET thread_name_format = ? WHERE guild_id = ? AND role_id = ?",
                (value, interaction.guild_id, self.panel.selected_role_id)
            )
            await db.commit()

        await self.panel.rebuild_items()
        embed = await self.panel.build_embed()
        await interaction.response.edit_message(embed=embed, view=self.panel)


# ============================================================================
# MAIN COG
# ============================================================================

class RoleAlerts(commands.Cog):
    """Cog for role-based alerts with ticket claiming system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cooldowns: dict[tuple[int, int, int], datetime] = {}
        self.COOLDOWN_SECONDS = 60

    async def cog_load(self):
        """Called when the cog is loaded."""
        await init_db()
        self.check_expired_threads.start()
        # No need to restore individual views - we use the interaction listener
        logger.info("RoleAlerts cog loaded")

    async def cog_unload(self):
        """Called when the cog is unloaded."""
        self.check_expired_threads.cancel()

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle persistent button interactions by parsing custom_ids."""
        if interaction.type != discord.InteractionType.component:
            return

        custom_id = interaction.data.get('custom_id', '')
        if not custom_id.startswith('ralert_'):
            return

        try:
            parts = custom_id.split(':')
            action = parts[0]

            if action == 'ralert_claim' and len(parts) >= 4:
                alert_id = int(parts[1])
                user_id = int(parts[2])
                role_id = int(parts[3]) if parts[3] != '0' else None
                await self._handle_claim(interaction, alert_id, user_id, role_id)

            elif action == 'ralert_dismiss' and len(parts) >= 4:
                alert_id = int(parts[1])
                user_id = int(parts[2])
                role_id = int(parts[3]) if parts[3] != '0' else None
                await self._handle_dismiss(interaction, alert_id, role_id)

            elif action == 'ralert_join' and len(parts) >= 4:
                alert_id = int(parts[1])
                thread_id = int(parts[2])
                role_id = int(parts[3]) if parts[3] != '0' else None
                await self._handle_join(interaction, alert_id, thread_id, role_id)

            elif action == 'ralert_close' and len(parts) >= 4:
                alert_id = int(parts[1])
                thread_id = int(parts[2])
                role_id = int(parts[3]) if parts[3] != '0' else None
                await self._handle_close(interaction, alert_id, thread_id, role_id)

            else:
                # Unknown action format - respond to prevent "interaction failed"
                logger.warning(f"Unknown role alert action format: {custom_id}")
                if not interaction.response.is_done():
                    await interaction.response.send_message("This button is no longer valid.", ephemeral=True)

        except Exception as e:
            logger.error(f"Error handling role alert interaction {custom_id}: {e}", exc_info=True)
            # Always respond to prevent "interaction failed" message
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred processing this button. Please try again.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "An error occurred processing this button. Please try again.",
                        ephemeral=True
                    )
            except Exception:
                pass  # Best effort - interaction may have already timed out

    async def _check_admin_permission(self, interaction: discord.Interaction, role_id: Optional[int]) -> bool:
        """Check if user has admin permission for this alert."""
        role_settings = None
        if role_id:
            role_settings = await self.get_tracked_role_settings(interaction.guild_id, role_id)

        is_admin = interaction.user.guild_permissions.administrator
        if role_settings and role_settings.get('admin_role_id'):
            admin_role = interaction.guild.get_role(role_settings['admin_role_id'])
            if admin_role and admin_role in interaction.user.roles:
                is_admin = True
        if hasattr(self.bot, 'is_bot_admin'):
            is_admin = is_admin or self.bot.is_bot_admin(interaction.user)

        return is_admin

    async def _handle_claim(self, interaction: discord.Interaction, alert_id: int, user_id: int, role_id: Optional[int]):
        """Handle claim button interaction."""
        if not await self._check_admin_permission(interaction, role_id):
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        role_settings = await self.get_tracked_role_settings(interaction.guild_id, role_id) if role_id else None

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT claimed_by, status, role_id FROM active_alerts WHERE alert_id = ?",
                (alert_id,)
            )
            row = await cursor.fetchone()

            if not row:
                await interaction.response.send_message("Alert no longer exists.", ephemeral=True)
                return

            if row[0] is not None or row[1] == 'claimed':
                claimer = interaction.guild.get_member(row[0])
                claimer_name = claimer.display_name if claimer else "Unknown"
                await interaction.response.send_message(f"Already claimed by {claimer_name}.", ephemeral=True)
                return

            # Check for existing active thread for this user/role
            cursor = await db.execute(
                """SELECT thread_id FROM active_alerts
                   WHERE guild_id = ? AND user_id = ? AND role_id = ?
                   AND status = 'claimed' AND thread_id IS NOT NULL""",
                (interaction.guild_id, user_id, role_id)
            )
            existing = await cursor.fetchone()
            if existing:
                thread = interaction.guild.get_channel_or_thread(existing[0])
                if thread:
                    await interaction.response.send_message(
                        f"There's already an active thread for this user: {thread.mention}",
                        ephemeral=True
                    )
                    return

            # Mark as claimed immediately
            await db.execute(
                "UPDATE active_alerts SET claimed_by = ?, status = 'claimed' WHERE alert_id = ? AND claimed_by IS NULL",
                (interaction.user.id, alert_id)
            )
            await db.commit()

        await interaction.response.defer()

        target_user = interaction.guild.get_member(user_id)
        if not target_user:
            await interaction.followup.send("User is no longer in the server.", ephemeral=True)
            return

        # Get role name
        role = interaction.guild.get_role(role_id) if role_id else None
        role_name = role.name if role else "Unknown Role"

        try:
            thread_format = (role_settings.get('thread_name_format') if role_settings else None) or '{user}-{role}'
            thread_name = thread_format.format(
                user=target_user.display_name[:20],
                role=role_name[:20],
                date=datetime.now(timezone.utc).strftime("%m-%d")
            )[:100]

            thread_channel = interaction.channel
            if role_settings and role_settings.get('thread_channel_id'):
                thread_channel = interaction.guild.get_channel(role_settings['thread_channel_id'])
                if not thread_channel:
                    thread_channel = interaction.channel

            thread = await thread_channel.create_thread(
                name=thread_name,
                type=discord.ChannelType.private_thread,
                auto_archive_duration=AUTO_ARCHIVE_DURATION,
                reason=f"Role alert claimed by {interaction.user}"
            )

            await thread.add_user(target_user)

            welcome_msg_template = role_settings.get('welcome_message') if role_settings else None
            if welcome_msg_template:
                welcome_msg = welcome_msg_template.format(
                    user=target_user.mention,
                    role=role_name,
                    admin=interaction.user.mention
                )
                await thread.send(welcome_msg)

            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("""
                    UPDATE active_alerts
                    SET thread_id = ?, claimed_at = ?
                    WHERE alert_id = ?
                """, (thread.id, datetime.now(timezone.utc).isoformat(), alert_id))
                await db.commit()

            embed = interaction.message.embeds[0] if interaction.message.embeds else None
            if embed:
                embed.color = discord.Color.green()
                embed.description = f"{embed.description}\n{thread.mention}"
                embed.set_footer(text=f"Claimed by {interaction.user.display_name}")

            new_view = ClaimedAlertView(self, alert_id, thread.id, role_id)
            await interaction.message.edit(embed=embed, view=new_view)
            await interaction.followup.send(f"Thread created: {thread.mention}", ephemeral=True)

        except discord.Forbidden:
            await interaction.followup.send("Missing permissions to create threads.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error creating thread: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    async def _handle_dismiss(self, interaction: discord.Interaction, alert_id: int, role_id: Optional[int]):
        """Handle dismiss button interaction."""
        if not await self._check_admin_permission(interaction, role_id):
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_alerts SET status = 'dismissed', claimed_by = ? WHERE alert_id = ?",
                (interaction.user.id, alert_id)
            )
            await db.commit()

        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = discord.Color.dark_gray()
            embed.set_footer(text=f"Dismissed by {interaction.user.display_name}")

        await interaction.response.edit_message(embed=embed, view=None)

    async def _handle_join(self, interaction: discord.Interaction, alert_id: int, thread_id: int, role_id: Optional[int]):
        """Handle join button interaction."""
        if not await self._check_admin_permission(interaction, role_id):
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        thread = interaction.guild.get_channel_or_thread(thread_id)
        if not thread:
            await interaction.response.send_message("Thread no longer exists.", ephemeral=True)
            return

        try:
            await thread.add_user(interaction.user)
            await interaction.response.send_message(f"You've been added to {thread.mention}", ephemeral=True)
        except discord.HTTPException as e:
            await interaction.response.send_message(f"Failed to join: {e}", ephemeral=True)

    async def _handle_close(self, interaction: discord.Interaction, alert_id: int, thread_id: int, role_id: Optional[int]):
        """Handle close button interaction."""
        if not await self._check_admin_permission(interaction, role_id):
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        confirm_view = CloseConfirmView(self, alert_id, thread_id)
        await interaction.response.send_message(
            "Delete this thread? This cannot be undone.",
            view=confirm_view,
            ephemeral=True
        )

    async def get_guild_settings(self, guild_id: int) -> Optional[dict]:
        """Get settings for a guild (legacy, mostly unused now)."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM guild_settings WHERE guild_id = ?",
                (guild_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_tracked_roles(self, guild_id: int) -> list[int]:
        """Get list of tracked role IDs for a guild."""
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT role_id FROM tracked_roles WHERE guild_id = ?",
                (guild_id,)
            )
            rows = await cursor.fetchall()
            return [r[0] for r in rows]

    async def get_tracked_roles_with_settings(self, guild_id: int) -> list[dict]:
        """Get tracked roles with their settings."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM tracked_roles WHERE guild_id = ?",
                (guild_id,)
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_tracked_role_settings(self, guild_id: int, role_id: int) -> Optional[dict]:
        """Get settings for a specific tracked role."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM tracked_roles WHERE guild_id = ? AND role_id = ?",
                (guild_id, role_id)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_alert_data(self, alert_id: int) -> Optional[dict]:
        """Get data for a specific alert."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM active_alerts WHERE alert_id = ?",
                (alert_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    def is_on_cooldown(self, guild_id: int, user_id: int, role_id: int) -> bool:
        """Check if an alert is on cooldown."""
        key = (guild_id, user_id, role_id)
        if key in self.cooldowns:
            elapsed = (datetime.now(timezone.utc) - self.cooldowns[key]).total_seconds()
            if elapsed < self.COOLDOWN_SECONDS:
                return True
        return False

    def set_cooldown(self, guild_id: int, user_id: int, role_id: int):
        """Set cooldown for an alert."""
        self.cooldowns[(guild_id, user_id, role_id)] = datetime.now(timezone.utc)

    @tasks.loop(hours=6)
    async def check_expired_threads(self):
        """Check for threads that should be auto-deleted (7 days old)."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            cursor = await db.execute(
                "SELECT * FROM active_alerts WHERE status = 'claimed' AND thread_id IS NOT NULL"
            )
            alerts = await cursor.fetchall()

            for alert in alerts:
                claimed_at = datetime.fromisoformat(alert['claimed_at']) if alert['claimed_at'] else None
                if not claimed_at:
                    continue

                if claimed_at.tzinfo is None:
                    claimed_at = claimed_at.replace(tzinfo=timezone.utc)

                if datetime.now(timezone.utc) - claimed_at > timedelta(days=AUTO_DELETE_DAYS):
                    try:
                        guild = self.bot.get_guild(alert['guild_id'])
                        if guild:
                            thread = guild.get_channel_or_thread(alert['thread_id'])
                            if thread:
                                await thread.delete()
                                logger.info(f"Auto-deleted thread {alert['thread_id']} after 7 days")

                            await db.execute(
                                "UPDATE active_alerts SET status = 'auto_deleted', thread_id = NULL WHERE alert_id = ?",
                                (alert['alert_id'],)
                            )

                            channel = guild.get_channel(alert['channel_id'])
                            if channel:
                                try:
                                    message = await channel.fetch_message(alert['message_id'])
                                    embed = message.embeds[0] if message.embeds else None
                                    if embed:
                                        embed.color = discord.Color.dark_gray()
                                        embed.set_footer(text="Auto-deleted (7 days expired)")
                                        await message.edit(embed=embed, view=None)
                                except discord.NotFound:
                                    pass

                    except Exception as e:
                        logger.error(f"Error auto-deleting thread: {e}")

            await db.commit()

    @check_expired_threads.before_loop
    async def before_check_expired(self):
        await self.bot.wait_until_ready()

    # ========================================================================
    # EVENT LISTENERS
    # ========================================================================

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Detect when a member gains a tracked role."""
        added_roles = set(after.roles) - set(before.roles)
        if not added_roles:
            return

        tracked_roles = await self.get_tracked_roles(after.guild.id)
        if not tracked_roles:
            return

        for role in added_roles:
            if role.id not in tracked_roles:
                continue

            if self.is_on_cooldown(after.guild.id, after.id, role.id):
                continue

            role_settings = await self.get_tracked_role_settings(after.guild.id, role.id)
            if not role_settings or not role_settings.get('alert_channel_id'):
                continue

            # Check bypass role - skip alert if user has the bypass role
            bypass_role_id = role_settings.get('bypass_role_id')
            if bypass_role_id:
                bypass_role = after.guild.get_role(bypass_role_id)
                if bypass_role and bypass_role in after.roles:
                    logger.info(f"Skipping alert for {after} gaining {role.name} — has bypass role {bypass_role.name}")
                    continue

            self.set_cooldown(after.guild.id, after.id, role.id)
            await self.send_role_alert(after, role, role_settings)

    async def send_role_alert(self, member: discord.Member, role: discord.Role, role_settings: dict):
        """Send an alert when a user gains a tracked role."""
        alert_channel_id = role_settings.get('alert_channel_id')
        if not alert_channel_id:
            return

        channel = member.guild.get_channel(alert_channel_id)
        if not channel:
            logger.warning(f"Alert channel not found for guild {member.guild.id}")
            return

        embed = discord.Embed(
            description=f"{member.mention} added {role.mention}",
            color=discord.Color.gold()
        )
        embed.set_footer(text="Unclaimed")

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("""
                INSERT INTO active_alerts (
                    guild_id, channel_id, user_id, role_id,
                    created_at, status
                ) VALUES (?, ?, ?, ?, ?, 'pending')
            """, (
                member.guild.id, channel.id, member.id, role.id,
                datetime.now(timezone.utc).isoformat()
            ))
            alert_id = cursor.lastrowid
            await db.commit()

        view = ClaimButton(self, alert_id, member.id, role.name, role.id)

        content = None
        if role_settings.get('ping_enabled') and role_settings.get('ping_role_id'):
            content = f"<@&{role_settings['ping_role_id']}>"

        try:
            message = await channel.send(content=content, embed=embed, view=view)

            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE active_alerts SET message_id = ? WHERE alert_id = ?",
                    (message.id, alert_id)
                )
                await db.commit()

            self.bot.add_view(view, message_id=message.id)

        except discord.Forbidden:
            logger.error(f"Cannot send to alert channel in {member.guild.name}")
        except Exception as e:
            logger.error(f"Error sending role alert: {e}", exc_info=True)

    # ========================================================================
    # COMMANDS
    # ========================================================================

    @app_commands.command(name="rolealerts", description="Configure role alerts system")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def role_alerts_config(self, interaction: discord.Interaction):
        """Open the admin panel for role alerts."""
        panel = ConfigPanel(self, interaction.guild)
        await panel.rebuild_items()
        embed = await panel.build_embed()
        await interaction.response.send_message(embed=embed, view=panel, ephemeral=True)
        panel.message = await interaction.original_response()


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleAlerts(bot))
