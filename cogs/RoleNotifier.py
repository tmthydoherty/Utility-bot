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

DB_PATH = "data/role_notifier_config/role_alerts.db"

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
        await db.execute("PRAGMA journal_mode=WAL")
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

    def __init__(self, cog: 'RoleNotifier', alert_id: int, thread_id: int):
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

    def __init__(self, cog: 'RoleNotifier', alert_id: int, thread_id: int, role_id: int = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.alert_id = alert_id
        self.thread_id = thread_id
        self.role_id = role_id

        # Buttons deliberately carry NO callback: every ralert_* click is handled
        # by RoleNotifier.on_interaction, which parses these custom_ids. That listener
        # is the only path that survives a restart (nothing re-registers these views
        # on startup), so it owns all four actions. Attaching callbacks here too
        # would double-handle the click and the loser gets 40060 "already acknowledged".
        self.add_item(discord.ui.Button(
            label="Join",
            style=discord.ButtonStyle.primary,
            custom_id=f"ralert_join:{alert_id}:{thread_id}:{role_id or 0}"
        ))
        self.add_item(discord.ui.Button(
            label="Close Thread",
            style=discord.ButtonStyle.danger,
            custom_id=f"ralert_close:{alert_id}:{thread_id}:{role_id or 0}"
        ))


class ClaimButton(discord.ui.View):
    """View with the claim button for role alerts. Uses dynamic custom_ids for persistence."""

    def __init__(self, cog: 'RoleNotifier', alert_id: int, user_id: int, role_name: str, role_id: int = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.alert_id = alert_id
        self.user_id = user_id
        self.role_name = role_name
        self.role_id = role_id

        # Buttons deliberately carry NO callback — see ClaimedAlertView above.
        # RoleNotifier.on_interaction parses these custom_ids and is the single
        # handler for every ralert_* click.
        self.add_item(discord.ui.Button(
            label="Claim",
            style=discord.ButtonStyle.primary,
            custom_id=f"ralert_claim:{alert_id}:{user_id}:{role_id or 0}"
        ))
        self.add_item(discord.ui.Button(
            label="Dismiss",
            style=discord.ButtonStyle.secondary,
            custom_id=f"ralert_dismiss:{alert_id}:{user_id}:{role_id or 0}"
        ))


# ============================================================================
# MAIN COG
# ============================================================================

class RoleNotifier(commands.Cog):
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
        logger.info("RoleNotifier cog loaded")

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
        try:
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
        except Exception as e:
            await self.bot.error_reporter.report("RoleNotifier", f"check_expired_threads: {e}")

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




async def setup(bot: commands.Bot):
    await bot.add_cog(RoleNotifier(bot))
