import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
import random
import copy
import os
import json
import logging
from pathlib import Path

# =====================================================================================
# UTILS & CONSTANTS
# =====================================================================================

log_quote = logging.getLogger("discord.daily_quote")

_cog_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_QUOTE = os.path.join(_cog_dir, '..', 'daily_quote_config.json')
DB_PATH_QUOTE = os.path.join(_cog_dir, '..', 'quotes.db')
DISTRACTORS_DB_PATH = os.path.join(_cog_dir, '..', 'distractors.db')

EMBED_COLOR_QUOTE = 0xE67E22  # Orange, distinct from trivia's teal
POST_TIME = time(19, 0)  # 7 PM CT
QUOTE_TIMEZONE = ZoneInfo("America/Chicago")
DEFAULT_LOW_QUOTE_ALERT_DAYS = 30

def load_config_quote():
    if os.path.exists(CONFIG_FILE_QUOTE):
        try:
            with open(CONFIG_FILE_QUOTE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log_quote.error(f"Error loading daily quote config: {e}")
    return {"global_data": {}, "guild_settings": {}}

def save_config_quote(config):
    try:
        with open(CONFIG_FILE_QUOTE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
    except IOError as e:
        log_quote.error(f"Error saving daily quote config: {e}")

async def is_quote_admin_check(interaction: discord.Interaction) -> bool:
    cog = interaction.client.get_cog("DailyQuote")
    if not cog:
        return await interaction.client.is_owner(interaction.user)
    return await cog.is_user_admin(interaction)

# =====================================================================================
# UI VIEWS (PLAYER-FACING)
# =====================================================================================

class QuoteAnswerView(discord.ui.View):
    """Persistent view with answer buttons on the daily quote gateway message."""

    def __init__(self, cog: "DailyQuote", answers: list[str] | None = None):
        super().__init__(timeout=None)
        self.cog = cog
        labels = answers if answers else ["A", "B", "C", "D"]
        for i, label in enumerate(labels):
            btn = discord.ui.Button(
                label=label[:80],
                style=discord.ButtonStyle.secondary,
                custom_id=f"daily_quote_answer_{i}",
            )
            btn.callback = self._make_callback(i)
            self.add_item(btn)

    def _make_callback(self, index: int):
        async def callback(interaction: discord.Interaction):
            await self.cog._handle_answer_button(interaction, index)
        return callback


# =====================================================================================
# ADMIN VIEWS
# =====================================================================================

class ChannelSelectView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread, discord.ChannelType.private_thread],
        placeholder="Select a channel or thread...",
        min_values=1, max_values=1
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["channel_id"] = channel.id
            cfg["enabled"] = True
            self.cog.config_is_dirty = True

        if self.cog.quote_loop.is_running():
            self.cog.quote_loop.restart()

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"Channel set to {channel.mention} and **enabled**.",
            view=self
        )

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class QuotePreviewView(discord.ui.View):
    """Admin approval view sent via DM. First click shows confirmation."""

    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="daily_quote_preview_approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.is_user_admin(interaction):
            return await interaction.response.send_message("Only admins can approve quotes.", ephemeral=True)
        # Swap to confirmation view
        await interaction.response.edit_message(view=QuotePreviewConfirmApproveView(self.cog))

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, custom_id="daily_quote_preview_deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.is_user_admin(interaction):
            return await interaction.response.send_message("Only admins can deny quotes.", ephemeral=True)
        # Swap to confirmation view
        await interaction.response.edit_message(view=QuotePreviewConfirmDenyView(self.cog))


class QuotePreviewConfirmApproveView(discord.ui.View):
    """Confirmation step for approving a quote."""

    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=30)
        self.cog = cog

    @discord.ui.button(label="Confirm Approve", style=discord.ButtonStyle.success, custom_id="daily_quote_confirm_approve")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            pending = self.cog.get_global_data().get("pending_question_data")
            if not pending:
                return await interaction.response.edit_message(content="No pending question to approve.", embed=None, view=None)
            pending["approved"] = True
            self.cog.config_is_dirty = True

        await interaction.response.edit_message(
            content="Quote **approved!** It will go live with tomorrow's daily post.",
            embed=None, view=None
        )

    @discord.ui.button(label="Go Back", style=discord.ButtonStyle.secondary, custom_id="daily_quote_confirm_approve_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(view=QuotePreviewView(self.cog))

    async def on_timeout(self):
        pass  # View expires silently; original message keeps its embed


class QuotePreviewConfirmDenyView(discord.ui.View):
    """Confirmation step for denying a quote."""

    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=30)
        self.cog = cog

    @discord.ui.button(label="Confirm Deny", style=discord.ButtonStyle.danger, custom_id="daily_quote_confirm_deny")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        async with self.cog.config_lock:
            pending = self.cog.get_global_data().get("pending_question_data")
        if not pending:
            return await interaction.edit_original_response(content="No pending question to deny.", embed=None, view=None)

        denied_id = pending.get("quote_id")
        guild_id = pending.get("guild_id")
        await self.cog._delete_quote_from_db(denied_id)

        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            used = global_data.get("used_quote_ids", [])
            if denied_id and denied_id in used:
                used.remove(denied_id)
            global_data["pending_question_data"] = None
            self.cog.config_is_dirty = True

        await interaction.edit_original_response(
            content="Quote **denied**. Generating a new preview...",
            embed=None, view=None
        )

        # Generate a new question and DM it for approval
        guild = self.cog.bot.get_guild(guild_id) if guild_id else None
        if guild:
            await self.cog._send_preview_question(guild)

    @discord.ui.button(label="Go Back", style=discord.ButtonStyle.secondary, custom_id="daily_quote_confirm_deny_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(view=QuotePreviewView(self.cog))

    async def on_timeout(self):
        pass


class AlertSettingsModal(discord.ui.Modal, title="Low Quote Alert Settings"):
    days = discord.ui.TextInput(
        label="Alert when quotes remaining ≤ X days",
        placeholder=f"{DEFAULT_LOW_QUOTE_ALERT_DAYS}",
        required=True,
        max_length=4,
        default=str(DEFAULT_LOW_QUOTE_ALERT_DAYS)
    )

    def __init__(self, cog: "DailyQuote"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(self.days.value.strip())
            if value < 1 or value > 999:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Please enter a number between 1 and 999.", ephemeral=True)

        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["low_quote_alert_days"] = value
            self.cog.config_is_dirty = True

        await interaction.response.send_message(f"Low quote alert set to **{value} days**.", ephemeral=True)


class ResetAttemptSelectView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user to reset...", min_values=1, max_values=1)
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            interactions = global_data.get("daily_interactions", [])
            original_len = len(interactions)
            global_data["daily_interactions"] = [d for d in interactions if d.get("user_id") != str(user.id)]
            removed = original_len - len(global_data["daily_interactions"])
            self.cog.config_is_dirty = True

        for item in self.children:
            item.disabled = True

        if removed:
            await interaction.response.edit_message(content=f"Reset daily attempt for **{user.display_name}**.", view=self)
        else:
            await interaction.response.edit_message(content=f"**{user.display_name}** hasn't attempted today's quote.", view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class ResetQuestionsConfirmView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Confirm Reset", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            count = len(global_data.get("used_quote_ids", []))
            global_data["used_quote_ids"] = []
            self.cog.config_is_dirty = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"Reset **{count}** used questions back to the pool.", view=self
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Reset cancelled.", view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class VoidTodayConfirmView(discord.ui.View):
    """Confirmation view for voiding today's question and reverting all scores/streaks."""

    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Confirm Void", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await self.cog._void_today()
        for item in self.children:
            item.disabled = True
        await interaction.edit_original_response(content=result, view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Void cancelled.", view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class ScoreManageUserSelect(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user to adjust score...", min_values=1, max_values=1)
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        async with self.cog.config_lock:
            scores = self.cog.get_global_data().get("scores", {})
            current = scores.get(str(user.id), {}).get("score", 0) if isinstance(scores.get(str(user.id)), dict) else scores.get(str(user.id), 0)
        await interaction.response.send_modal(ScoreAdjustModal(self.cog, user, current))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class ScoreAdjustModal(discord.ui.Modal, title="Adjust Score"):
    amount = discord.ui.TextInput(
        label="Points to add (negative to remove)",
        placeholder="e.g. 3 or -2",
        required=True,
        max_length=6,
    )

    def __init__(self, cog: "DailyQuote", user: discord.Member, current_score: int):
        super().__init__()
        self.cog = cog
        self.user = user
        self.current_score = current_score
        self.amount.label = f"Adjust points (current: {current_score})"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(self.amount.value.strip())
        except ValueError:
            return await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

        uid = str(self.user.id)
        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            score_data = global_data.setdefault("scores", {}).setdefault(uid, {"score": 0, "timestamp": None})
            if isinstance(score_data, int):
                score_data = {"score": score_data, "timestamp": None}
                global_data["scores"][uid] = score_data
            score_data["score"] = max(0, score_data.get("score", 0) + value)
            new_score = score_data["score"]

            stats = self.cog.get_user_stats(self.user.id)
            stats["all_time_score"] = max(0, stats.get("all_time_score", 0) + value)

            self.cog.config_is_dirty = True

        sign = "+" if value >= 0 else ""
        await interaction.response.send_message(
            f"Adjusted **{self.user.display_name}**'s score by **{sign}{value}** → now **{new_score}** (monthly).",
            ephemeral=True
        )


class WipeScoresConfirmView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Confirm Wipe", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            count = len(global_data.get("scores", {}))
            global_data["scores"] = {}
            self.cog.config_is_dirty = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"Wiped all monthly scores (**{count}** users cleared).", view=self
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Wipe cancelled.", view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class ScoreManagePanelView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Adjust User Score", style=discord.ButtonStyle.primary)
    async def adjust_score(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Select a user to adjust their score:", view=ScoreManageUserSelect(self.cog), ephemeral=True
        )

    @discord.ui.button(label="Wipe All Scores", style=discord.ButtonStyle.danger)
    async def wipe_scores(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            count = len(self.cog.get_global_data().get("scores", {}))
        await interaction.response.send_message(
            f"This will wipe **all** monthly scores for **{count}** users.\nAre you sure?",
            view=WipeScoresConfirmView(self.cog),
            ephemeral=True
        )

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class BlockUserSelectView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user to block/unblock...", min_values=1, max_values=1)
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            blocked = global_data.setdefault("blocked_users", [])
            if user.id in blocked:
                blocked.remove(user.id)
                action = "unblocked"
            else:
                blocked.append(user.id)
                action = "blocked"
            self.cog.config_is_dirty = True

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"**{user.display_name}** has been **{action}** from Daily Quote.", view=self
        )

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class QuoteAdminPanelView(discord.ui.View):
    def __init__(self, cog: "DailyQuote"):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(label="Set Channel", style=discord.ButtonStyle.primary, row=0)
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Select the channel for daily quotes:", view=ChannelSelectView(self.cog), ephemeral=True)

    @discord.ui.button(label="Enable / Disable", style=discord.ButtonStyle.secondary, row=0)
    async def toggle_enabled(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["enabled"] = not cfg.get("enabled", False)
            new_state = cfg["enabled"]
            self.cog.config_is_dirty = True

        if self.cog.quote_loop.is_running():
            self.cog.quote_loop.restart()

        await interaction.response.send_message(f"Daily Quote is now **{'enabled' if new_state else 'disabled'}**.", ephemeral=True)

    @discord.ui.button(label="Toggle Screening", style=discord.ButtonStyle.secondary, row=0)
    async def toggle_screening(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["screening_enabled"] = not cfg.get("screening_enabled", True)
            new_state = cfg["screening_enabled"]
            self.cog.config_is_dirty = True
        state_text = "**enabled** — previews will be DM'd for approval" if new_state else "**disabled** — quotes post directly"
        await interaction.response.send_message(f"Screening is now {state_text}.", ephemeral=True)

    @discord.ui.button(label="Alert Settings", style=discord.ButtonStyle.secondary, row=0)
    async def alert_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AlertSettingsModal(self.cog))

    @discord.ui.button(label="Force Post", style=discord.ButtonStyle.success, row=1)
    async def force_post(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.cog._trigger_daily_post(interaction.guild)
            await interaction.followup.send("Daily quote posted!", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Failed to post: {e}", ephemeral=True)

    @discord.ui.button(label="Force Preview", style=discord.ButtonStyle.success, row=1)
    async def force_preview(self, interaction: discord.Interaction, button: discord.ui.Button):
        cfg = self.cog.get_guild_settings(interaction.guild.id)
        if not cfg.get("screening_enabled", True):
            return await interaction.response.send_message("Screening is disabled. Enable it first.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.cog._send_preview_question(interaction.guild)
            await interaction.followup.send("Preview question DM'd for approval!", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Failed to send preview: {e}", ephemeral=True)

    @discord.ui.button(label="Reset User Attempt", style=discord.ButtonStyle.secondary, row=1)
    async def reset_attempt(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Select the user to reset:", view=ResetAttemptSelectView(self.cog), ephemeral=True)

    @discord.ui.button(label="Bump", style=discord.ButtonStyle.secondary, row=1)
    async def bump(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.cog._post_gateway_message(interaction.guild)
            await interaction.followup.send("Quote bumped!", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Failed to bump: {e}", ephemeral=True)

    @discord.ui.button(label="Score Management", style=discord.ButtonStyle.primary, row=2)
    async def score_management(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "**Score Management**", view=ScoreManagePanelView(self.cog), ephemeral=True
        )

    @discord.ui.button(label="Block User", style=discord.ButtonStyle.danger, row=2)
    async def block_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            blocked = self.cog.get_global_data().get("blocked_users", [])
        blocked_mentions = ", ".join(f"<@{uid}>" for uid in blocked) if blocked else "None"
        await interaction.response.send_message(
            f"**Currently blocked:** {blocked_mentions}\n\nSelect a user to block or unblock:",
            view=BlockUserSelectView(self.cog),
            ephemeral=True
        )

    @discord.ui.button(label="Reset Questions", style=discord.ButtonStyle.danger, row=2)
    async def reset_questions(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            count = len(self.cog.get_global_data().get("used_quote_ids", []))
        total = await self.cog._get_total_quotes()
        await interaction.response.send_message(
            f"This will reset **{count}** used questions back to the unused pool ({total} total).\nAre you sure?",
            view=ResetQuestionsConfirmView(self.cog),
            ephemeral=True
        )

    @discord.ui.button(label="Void Today", style=discord.ButtonStyle.danger, row=3)
    async def void_today(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            interactions = self.cog.get_global_data().get("daily_interactions", [])
            q_data = self.cog.get_global_data().get("daily_question_data")
        if not interactions and not q_data:
            return await interaction.response.send_message("No active question or interactions to void.", ephemeral=True)
        attempts = len(interactions)
        await interaction.response.send_message(
            f"This will **void** today's question, revert all **{attempts}** answers (scores, streaks, correct/incorrect counts), "
            f"and remove the question from used pool.\n\n**Are you sure?**",
            view=VoidTodayConfirmView(self.cog),
            ephemeral=True
        )

    @discord.ui.button(label="Info & Status", style=discord.ButtonStyle.secondary, row=3)
    async def info_status(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        cfg = self.cog.get_guild_settings(interaction.guild.id)
        global_data = self.cog.get_global_data()

        db_total = 0
        db_titles = 0
        if os.path.exists(self.cog.db_path):
            try:
                async with aiosqlite.connect(self.cog.db_path) as db:
                    async with db.execute("SELECT COUNT(*) FROM quotes") as c:
                        db_total = (await c.fetchone())[0]
                    async with db.execute("SELECT COUNT(DISTINCT movie_title) FROM quotes") as c:
                        db_titles = (await c.fetchone())[0]
            except Exception as e:
                log_quote.error(f"Error reading DB stats: {e}")

        used_count = len(global_data.get("used_quote_ids", []))
        remaining = db_total - used_count
        today_attempts = len(global_data.get("daily_interactions", []))
        alert_days = cfg.get("low_quote_alert_days", DEFAULT_LOW_QUOTE_ALERT_DAYS)

        embed = discord.Embed(title="🎬 Daily Quote Info", color=EMBED_COLOR_QUOTE)
        embed.add_field(
            name="Guild Settings",
            value=(
                f"**Channel:** {'<#' + str(cfg['channel_id']) + '>' if cfg.get('channel_id') else 'Not set'}\n"
                f"**Enabled:** {cfg.get('enabled', False)}\n"
                f"**Screening:** {'DM (enabled)' if cfg.get('screening_enabled', True) else 'Disabled'}\n"
                f"**Last Posted:** {cfg.get('last_posted_date', 'Never')}\n"
                f"**Low Quote Alert:** {alert_days} days"
            ),
            inline=False
        )
        # Distractors pool stats
        dist_info = "Not loaded"
        if os.path.exists(self.cog.distractors_db_path):
            try:
                async with aiosqlite.connect(self.cog.distractors_db_path) as ddb:
                    async with ddb.execute("SELECT COUNT(*) FROM titles") as c:
                        dist_titles = (await c.fetchone())[0]
                    async with ddb.execute("SELECT COUNT(DISTINCT character_name) FROM characters") as c:
                        dist_chars = (await c.fetchone())[0]
                dist_info = f"{dist_titles:,} titles, {dist_chars:,} characters"
            except Exception:
                dist_info = "Error reading"

        embed.add_field(
            name="Database",
            value=(
                f"**Total Quotes:** {db_total:,}\n"
                f"**Movies/Shows:** {db_titles:,}\n"
                f"**Used:** {used_count:,}\n"
                f"**Remaining:** {remaining:,} (~{remaining} days)\n"
                f"**Distractors Pool:** {dist_info}"
            ),
            inline=False
        )
        embed.add_field(
            name="Today",
            value=f"**Attempts:** {today_attempts}\n**Question:** {'Loaded' if global_data.get('daily_question_data') else 'None'}",
            inline=False
        )

        if remaining <= alert_days:
            embed.add_field(
                name="⚠️ Low Quotes Warning",
                value=f"Only **{remaining}** quotes remaining! Consider adding more to the database.",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


# =====================================================================================
# MAIN COG
# =====================================================================================

class DailyQuote(commands.Cog, name="DailyQuote"):
    dailyquote = app_commands.Group(name="dailyquote", description="Commands for the daily movie/TV quote trivia.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_quote()
        self.config_lock = asyncio.Lock()
        self.config_is_dirty = False
        self.db_path = DB_PATH_QUOTE
        self.distractors_db_path = DISTRACTORS_DB_PATH
        self.bot.loop.create_task(self.setup_hook())

    async def setup_hook(self):
        await self.bot.wait_until_ready()
        # Register persistent views
        self.bot.add_view(QuoteAnswerView(self))
        self.bot.add_view(QuotePreviewView(self))
        # Detect new schema columns for graceful fallback
        await self._check_db_schema()

    async def _check_db_schema(self):
        """Detect which columns exist so we can fall back gracefully on old DBs."""
        self._has_difficulty = False
        self._has_year = False
        self._has_is_tv = False
        self._has_curated = False
        if not os.path.exists(self.db_path):
            return
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("PRAGMA table_info(quotes)") as cursor:
                    columns = {row[1] async for row in cursor}
            self._has_difficulty = "difficulty" in columns
            self._has_year = "year" in columns
            self._has_is_tv = "is_tv" in columns
            self._has_curated = "curated_distractors" in columns
            log_quote.info(f"DB schema: difficulty={self._has_difficulty}, year={self._has_year}, is_tv={self._has_is_tv}, curated={self._has_curated}")
        except Exception as e:
            log_quote.error(f"Failed to check DB schema: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.quote_loop.is_running():
            self.quote_loop.start()
        if not self.backup_save_loop.is_running():
            self.backup_save_loop.start()
        if not self.auto_approve_loop.is_running():
            self.auto_approve_loop.start()
        log_quote.info(f"DailyQuote cog is ready. Config: {CONFIG_FILE_QUOTE} | DB: {self.db_path}")

    async def cog_unload(self):
        if self.config_is_dirty:
            save_config_quote(self.config)
        self.quote_loop.cancel()
        self.backup_save_loop.cancel()
        self.auto_approve_loop.cancel()

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        log_quote.error(f"Error in command '{interaction.command.name}': {error}", exc_info=True)
        msg = "You don't have permission for this." if isinstance(error, app_commands.CheckFailure) else "An unexpected error occurred."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass

    # === Config & Data Management ===

    def get_default_guild_settings(self) -> dict:
        return {
            "channel_id": None,
            "enabled": False,
            "gateway_message_id": None,
            "last_posted_date": None,
            "low_quote_alert_days": DEFAULT_LOW_QUOTE_ALERT_DAYS,
            "last_low_quote_alert_date": None,
            "screening_enabled": True,
        }

    def get_global_data(self) -> dict:
        global_data = self.config.setdefault("global_data", {})
        default_global = {
            "scores": {},
            "user_stats": {},
            "daily_question_data": None,
            "daily_interactions": [],
            "used_quote_ids": [],
            "blocked_users": [],
            "pending_question_data": None,
        }
        updated = False
        for key, value in default_global.items():
            if key not in global_data:
                global_data[key] = copy.deepcopy(value)
                updated = True
        if updated:
            self.config_is_dirty = True
        return global_data

    def get_guild_settings(self, guild_id: int) -> dict:
        gid = str(guild_id)
        guild_settings_pool = self.config.setdefault("guild_settings", {})
        if gid not in guild_settings_pool:
            guild_settings_pool[gid] = self.get_default_guild_settings()
            self.config_is_dirty = True
        guild_cfg = guild_settings_pool[gid]
        default_cfg = self.get_default_guild_settings()
        updated = False
        for key, value in default_cfg.items():
            if key not in guild_cfg:
                guild_cfg[key] = copy.deepcopy(value)
                updated = True
        if updated:
            self.config_is_dirty = True
        return guild_cfg

    def get_user_stats(self, user_id: int) -> dict:
        uid = str(user_id)
        global_data = self.get_global_data()
        default_stats = {
            "correct": 0,
            "incorrect": 0,
            "current_streak": 0,
            "longest_streak": 0,
            "all_time_score": 0,
            "all_time_timestamp": None,
            "streak_achieved_at": None,
        }
        user_stats_pool = global_data.setdefault("user_stats", {})
        if uid not in user_stats_pool:
            user_stats_pool[uid] = default_stats.copy()
            self.config_is_dirty = True
        user_stats_ref = user_stats_pool[uid]
        updated = False
        for key, value in default_stats.items():
            if key not in user_stats_ref:
                user_stats_ref[key] = copy.deepcopy(value)
                updated = True
        if updated:
            self.config_is_dirty = True
        return user_stats_ref

    async def save_config_now(self):
        json_str = None
        async with self.config_lock:
            if self.config_is_dirty:
                json_str = await self.bot.loop.run_in_executor(
                    None, lambda: json.dumps(self.config, indent=4)
                )
                self.config_is_dirty = False
        if json_str:
            await self.bot.loop.run_in_executor(
                None, lambda: Path(CONFIG_FILE_QUOTE).write_text(json_str, encoding="utf-8")
            )
            log_quote.debug("Daily quote config saved to disk.")

    async def is_user_admin(self, interaction: discord.Interaction) -> bool:
        if await self.bot.is_owner(interaction.user):
            return True
        if interaction.guild and isinstance(interaction.user, discord.Member):
            return self.bot.is_bot_admin(interaction.user)
        return False

    async def _is_user_blocked(self, user_id: int) -> bool:
        async with self.config_lock:
            return user_id in self.get_global_data().get("blocked_users", [])

    async def _void_today(self) -> str:
        """Void today's question: restore all user stats from pre-answer snapshots."""
        async with self.config_lock:
            global_data = self.get_global_data()
            interactions = global_data.get("daily_interactions", [])
            q_data = global_data.get("daily_question_data")
            scores = global_data.get("scores", {})
            user_stats = global_data.get("user_stats", {})

            if not interactions and not q_data:
                return "Nothing to void."

            reverted = 0

            for entry in interactions:
                uid = entry.get("user_id")
                snapshot = entry.get("snapshot")
                stats = user_stats.get(uid, {})

                if snapshot:
                    # Perfect restoration from snapshot
                    stats["current_streak"] = snapshot["streak"]
                    stats["longest_streak"] = snapshot["longest_streak"]
                    stats["streak_achieved_at"] = snapshot["streak_achieved_at"]
                    stats["correct"] = snapshot["correct"]
                    stats["incorrect"] = snapshot["incorrect"]
                    stats["all_time_score"] = snapshot["all_time_score"]
                    stats["all_time_timestamp"] = snapshot["all_time_timestamp"]

                    score_data = scores.get(uid, {})
                    if isinstance(score_data, dict):
                        score_data["score"] = snapshot["monthly_score"]
                        score_data["timestamp"] = snapshot["monthly_timestamp"]
                else:
                    # Fallback for interactions recorded before snapshot tracking
                    was_correct = entry.get("correct", False)
                    if was_correct:
                        stats["correct"] = max(0, stats.get("correct", 0) - 1)
                        stats["all_time_score"] = max(0, stats.get("all_time_score", 0) - 1)
                        stats["current_streak"] = max(0, stats.get("current_streak", 0) - 1)
                        if stats["current_streak"] == 0:
                            stats["streak_achieved_at"] = None
                        score_data = scores.get(uid, {})
                        if isinstance(score_data, dict):
                            score_data["score"] = max(0, score_data.get("score", 0) - 1)
                    else:
                        stats["incorrect"] = max(0, stats.get("incorrect", 0) - 1)

                reverted += 1

            # Remove the quote from used pool so it can be reused correctly
            quote_id = q_data.get("quote_id") if q_data else None
            used = global_data.get("used_quote_ids", [])
            if quote_id and quote_id in used:
                used.remove(quote_id)

            global_data["daily_interactions"] = []
            global_data["daily_question_data"] = None
            self.config_is_dirty = True

        await self.save_config_now()

        return (
            f"Today's question has been **voided**.\n"
            f"- Reverted **{reverted}** user answers (scores, streaks, stats fully restored)\n"
            f"- Question removed from used pool"
        )

    # === Leaderboard & Formatting ===

    async def _get_formatted_name(self, guild: discord.Guild, user_id_str: str) -> str:
        try:
            member = guild.get_member(int(user_id_str)) or await guild.fetch_member(int(user_id_str))
            return member.display_name
        except (discord.NotFound, discord.Forbidden):
            try:
                user = self.bot.get_user(int(user_id_str)) or await self.bot.fetch_user(int(user_id_str))
                return user.name
            except (discord.NotFound, discord.Forbidden):
                return f"User ({user_id_str[-4:]})"

    def _get_score_sort_key(self, item):
        _, data = item
        score = data.get("score", 0) if isinstance(data, dict) else data
        timestamp = datetime.fromisoformat(data["timestamp"]).timestamp() if isinstance(data, dict) and data.get("timestamp") else float('inf')
        return (-score, timestamp)

    async def _get_longest_streak_text(self, guild: discord.Guild) -> str:
        global_data = self.get_global_data()
        all_stats = global_data.get("user_stats", {})

        candidates = [
            (uid, stats.get("current_streak", 0), stats.get("streak_achieved_at"))
            for uid, stats in all_stats.items()
            if stats.get("current_streak", 0) > 0
        ]
        if not candidates:
            return "> *No active streaks*"

        # Highest streak wins; ties go to whoever reached it first (earliest timestamp)
        candidates.sort(key=lambda x: (-x[1], x[2] or "9999"))
        best_uid, best_streak, _ = candidates[0]

        name = await self._get_formatted_name(guild, best_uid)
        return f"> **{name}** (x{best_streak})"

    async def _get_gateway_leaderboard_text(self, guild: discord.Guild) -> str:
        global_data = self.get_global_data()
        scores = global_data.get("scores", {})
        if not scores:
            return "> *No scores yet this month!*"
        sorted_scores = sorted(scores.items(), key=self._get_score_sort_key)
        top = sorted_scores[:3]
        if not top:
            return "> *No scores yet this month!*"
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, (uid, dat) in enumerate(top):
            name = await self._get_formatted_name(guild, uid)
            name_formatted = f"**{name[:15]}**"
            lines.append(f"> {medals[i]} {name_formatted} - {dat.get('score', 0)} pts")
        return "\n".join(lines)

    # === SQLite Question Generation ===

    async def _get_total_quotes(self) -> int:
        if not os.path.exists(self.db_path):
            return 0
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM quotes") as c:
                    return (await c.fetchone())[0]
        except Exception:
            return 0

    async def _get_remaining_quotes(self) -> int:
        total = await self._get_total_quotes()
        async with self.config_lock:
            used = len(self.get_global_data().get("used_quote_ids", []))
        return total - used

    def _pick_difficulty(self) -> str:
        """Weighted random: ~75% easy, ~20% medium, ~5% hard."""
        roll = random.random()
        if roll < 0.75:
            return "easy"
        elif roll < 0.95:
            return "medium"
        else:
            return "hard"

    async def _get_daily_question(self) -> dict | None:
        if not os.path.exists(self.db_path):
            log_quote.error(f"Database not found at {self.db_path}")
            return None

        async with self.config_lock:
            used_ids = self.get_global_data().get("used_quote_ids", [])

        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row

                async with db.execute("SELECT COUNT(*) FROM quotes") as cursor:
                    total_available = (await cursor.fetchone())[0]

                if total_available == 0:
                    log_quote.error("No quotes found in database.")
                    return None

                if len(used_ids) >= total_available:
                    log_quote.info("All quotes have been used. Resetting used_quote_ids.")
                    used_ids = []
                    async with self.config_lock:
                        self.get_global_data()["used_quote_ids"] = []
                        self.config_is_dirty = True

                # Weighted difficulty selection (falls back if difficulty column missing)
                row = None
                difficulty = "easy"
                if self._has_difficulty:
                    difficulty = self._pick_difficulty()
                    if used_ids:
                        placeholders = ",".join("?" for _ in used_ids)
                        query = f"SELECT * FROM quotes WHERE id NOT IN ({placeholders}) AND difficulty = ? ORDER BY RANDOM() LIMIT 1"
                        params = used_ids + [difficulty]
                    else:
                        query = "SELECT * FROM quotes WHERE difficulty = ? ORDER BY RANDOM() LIMIT 1"
                        params = [difficulty]
                    async with db.execute(query, params) as cursor:
                        row = await cursor.fetchone()

                # Fallback: any difficulty
                if not row:
                    if used_ids:
                        placeholders = ",".join("?" for _ in used_ids)
                        query = f"SELECT * FROM quotes WHERE id NOT IN ({placeholders}) ORDER BY RANDOM() LIMIT 1"
                        params = used_ids
                    else:
                        query = "SELECT * FROM quotes ORDER BY RANDOM() LIMIT 1"
                        params = []
                    async with db.execute(query, params) as cursor:
                        row = await cursor.fetchone()
                    if row and self._has_difficulty:
                        difficulty = row["difficulty"]

                if not row:
                    log_quote.error("Failed to fetch a quote from the database.")
                    return None

                # --- Retry loop: skip ambiguous quotes (max 10 attempts) ---
                skipped_ids = []
                for _attempt in range(10):
                    quote_id = row["id"]
                    quote_text = row["quote"]
                    character = row["character"]
                    movie_title = row["movie_title"]
                    genre = row["genre"]
                    if self._has_difficulty:
                        difficulty = row["difficulty"]
                    year = row["year"] if self._has_year else None
                    is_tv = row["is_tv"] if self._has_is_tv else 0
                    curated_json = row["curated_distractors"] if self._has_curated else None

                    # --- Ambiguity detection ---
                    # Check if this character appears in multiple movies (franchise risk)
                    async with db.execute(
                        "SELECT COUNT(DISTINCT movie_title) FROM quotes WHERE character = ?",
                        (character,)
                    ) as cursor:
                        char_movie_count = (await cursor.fetchone())[0]
                    is_franchise_char = char_movie_count > 1

                    # Check if character name leaks the answer (appears in quote text)
                    quote_lower = quote_text.lower()
                    char_parts = [p.strip().lower() for p in character.replace("-", " ").split() if len(p.strip()) > 2]
                    char_leaked = any(part in quote_lower for part in char_parts) if char_parts else False

                    # Determine which question types are safe
                    movie_ok = not is_franchise_char   # Franchise chars make "which movie" ambiguous
                    char_ok = not char_leaked           # Leaked name makes "who said it" trivial

                    if not movie_ok and not char_ok:
                        # Both types are problematic — skip this quote and try another
                        log_quote.info(f"Skipping ambiguous quote {quote_id} (franchise char + name leaked)")
                        skipped_ids.append(quote_id)
                        skip_exclude = used_ids + skipped_ids
                        placeholders = ",".join("?" for _ in skip_exclude)
                        async with db.execute(
                            f"SELECT * FROM quotes WHERE id NOT IN ({placeholders}) ORDER BY RANDOM() LIMIT 1",
                            skip_exclude
                        ) as cursor:
                            row = await cursor.fetchone()
                        if not row:
                            log_quote.error("No non-ambiguous quotes remaining.")
                            return None
                        continue

                    # Decide question type — respect ambiguity constraints
                    if not movie_ok:
                        question_type = "character"
                    elif not char_ok:
                        question_type = "movie"
                    else:
                        # Both safe — use existing heuristic
                        async with db.execute(
                            "SELECT COUNT(DISTINCT character) FROM quotes WHERE movie_title = ?",
                            (movie_title,)
                        ) as cursor:
                            char_count = (await cursor.fetchone())[0]

                        if char_count >= 3:
                            question_type = random.choice(["movie", "character"])
                        elif char_count >= 2:
                            question_type = random.choices(["movie", "character"], weights=[0.7, 0.3])[0]
                        else:
                            question_type = "movie"

                    break  # Found a usable quote
                else:
                    log_quote.error("Exhausted retry attempts finding non-ambiguous quote.")
                    return None

                if question_type == "movie":
                    correct = movie_title
                    distractors = await self._get_movie_distractors(db, movie_title, genre, year, is_tv, curated_json, character)
                else:
                    correct = character
                    distractors = await self._get_character_distractors(db, character, movie_title, genre, year, is_tv, curated_json)

                if len(distractors) < 3:
                    if question_type == "movie":
                        question_type = "character"
                        correct = character
                        distractors = await self._get_character_distractors(db, character, movie_title, genre, year, is_tv, curated_json)
                    else:
                        question_type = "movie"
                        correct = movie_title
                        distractors = await self._get_movie_distractors(db, movie_title, genre, year, is_tv, curated_json, character)

                if len(distractors) < 3:
                    distractors = await self._pad_distractors(db, correct, question_type, distractors)

                answers = [correct] + distractors[:3]
                random.shuffle(answers)
                correct_index = answers.index(correct)

                async with self.config_lock:
                    self.get_global_data().setdefault("used_quote_ids", []).append(quote_id)
                    self.config_is_dirty = True

                return {
                    "quote": quote_text,
                    "character": character,
                    "movie_title": movie_title,
                    "genre": genre,
                    "difficulty": difficulty,
                    "correct": correct,
                    "answers": answers,
                    "correct_index": correct_index,
                    "question_type": question_type,
                    "quote_id": quote_id,
                }

        except Exception as e:
            log_quote.error(f"Database error generating daily question: {e}", exc_info=True)
            return None

    async def _get_movie_distractors(self, db: aiosqlite.Connection, correct_title: str,
                                      genre: str, year: int | None = None,
                                      is_tv: int = 0, curated_json: str | None = None,
                                      character: str | None = None) -> list:
        # Step 1: Use curated distractors if available
        if curated_json:
            try:
                curated = json.loads(curated_json)
                movie_dists = curated.get("movie", [])
                if len(movie_dists) >= 3:
                    return movie_dists[:3]
            except (json.JSONDecodeError, TypeError):
                pass

        # Step 2: TMDb distractors pool (primary source)
        if os.path.exists(self.distractors_db_path):
            distractors = await self._tmdb_title_distractors(correct_title, genre, year, is_tv, character)
            if len(distractors) >= 3:
                return distractors[:3]
        else:
            distractors = []

        # Step 3: Quotes table fallback (only if TMDb pool insufficient)
        if len(distractors) < 3:
            needed = 3 - len(distractors)
            exclude = [correct_title] + distractors
            placeholders = ",".join("?" for _ in exclude)
            query = f"""
                SELECT DISTINCT movie_title FROM quotes
                WHERE movie_title NOT IN ({placeholders})
                ORDER BY RANDOM() LIMIT ?
            """
            async with db.execute(query, exclude + [needed]) as cursor:
                distractors.extend(r[0] for r in await cursor.fetchall())

        return distractors[:3]

    async def _tmdb_title_distractors(self, correct_title: str, genre: str,
                                       year: int | None, is_tv: int,
                                       character: str | None = None) -> list:
        """Query distractors.db for believable movie/show title distractors."""
        genres = [g.strip() for g in genre.split(",")]
        primary_genre = genres[0] if genres else ""
        distractors = []

        # Build franchise-sibling exclusion from quotes DB
        franchise_exclude = set()
        if character:
            try:
                async with aiosqlite.connect(self.db_path) as qdb:
                    async with qdb.execute(
                        "SELECT DISTINCT movie_title FROM quotes WHERE character = ? AND movie_title != ?",
                        (character, correct_title)
                    ) as cursor:
                        franchise_exclude = {r[0] for r in await cursor.fetchall()}
            except Exception:
                pass

        async with aiosqlite.connect(self.distractors_db_path) as ddb:
            base_exclude = [correct_title] + list(franchise_exclude)

            # Tier A: Same type + primary genre + same decade
            if year:
                decade_start = (year // 10) * 10
                decade_end = decade_start + 9
                excl_ph = ",".join("?" for _ in base_exclude)
                query = f"""
                    SELECT DISTINCT title FROM titles
                    WHERE is_tv = ? AND title NOT IN ({excl_ph})
                    AND genres LIKE '%' || ? || '%'
                    AND year BETWEEN ? AND ?
                    ORDER BY RANDOM() LIMIT 3
                """
                async with ddb.execute(query, [is_tv] + base_exclude + [primary_genre, decade_start, decade_end]) as c:
                    distractors = [r[0] for r in await c.fetchall()]

            # Tier B: Same type + primary genre (any year)
            if len(distractors) < 3:
                needed = 3 - len(distractors)
                excl = base_exclude + distractors
                excl_ph = ",".join("?" for _ in excl)
                query = f"""
                    SELECT DISTINCT title FROM titles
                    WHERE is_tv = ? AND title NOT IN ({excl_ph})
                    AND genres LIKE '%' || ? || '%'
                    ORDER BY RANDOM() LIMIT ?
                """
                async with ddb.execute(query, [is_tv] + excl + [primary_genre, needed]) as c:
                    distractors.extend(r[0] for r in await c.fetchall())

            # Tier C: Same type, any genre
            if len(distractors) < 3:
                needed = 3 - len(distractors)
                excl = base_exclude + distractors
                excl_ph = ",".join("?" for _ in excl)
                query = f"""
                    SELECT DISTINCT title FROM titles
                    WHERE is_tv = ? AND title NOT IN ({excl_ph})
                    ORDER BY RANDOM() LIMIT ?
                """
                async with ddb.execute(query, [is_tv] + excl + [needed]) as c:
                    distractors.extend(r[0] for r in await c.fetchall())

        return distractors[:3]

    async def _get_character_distractors(self, db: aiosqlite.Connection, correct_character: str,
                                          movie_title: str, genre: str,
                                          year: int | None = None, is_tv: int = 0,
                                          curated_json: str | None = None) -> list:
        # Step 1: Use curated distractors if available
        if curated_json:
            try:
                curated = json.loads(curated_json)
                char_dists = curated.get("character", [])
                if len(char_dists) >= 3:
                    return char_dists[:3]
            except (json.JSONDecodeError, TypeError):
                pass

        # Step 2: TMDb distractors pool (primary source)
        if os.path.exists(self.distractors_db_path):
            distractors = await self._tmdb_character_distractors(
                correct_character, movie_title, genre, year, is_tv
            )
            if len(distractors) >= 3:
                return distractors[:3]
        else:
            distractors = []

        # Step 3: Quotes table fallback
        if len(distractors) < 3:
            needed = 3 - len(distractors)
            exclude = [correct_character] + distractors
            placeholders = ",".join("?" for _ in exclude)
            query = f"""
                SELECT DISTINCT character FROM quotes
                WHERE character NOT IN ({placeholders})
                ORDER BY RANDOM() LIMIT ?
            """
            async with db.execute(query, exclude + [needed]) as cursor:
                distractors.extend(r[0] for r in await cursor.fetchall())

        return distractors[:3]

    async def _tmdb_character_distractors(self, correct_character: str, movie_title: str,
                                           genre: str, year: int | None, is_tv: int) -> list:
        """Query distractors.db for believable character name distractors."""
        genres = [g.strip() for g in genre.split(",")]
        primary_genre = genres[0] if genres else ""
        distractors = []

        async with aiosqlite.connect(self.distractors_db_path) as ddb:
            exclude = [correct_character]

            # Tier A: Characters from the SAME movie in TMDb (best possible distractors)
            excl_ph = ",".join("?" for _ in exclude)
            query = f"""
                SELECT DISTINCT c.character_name FROM characters c
                JOIN titles t ON c.title_id = t.id
                WHERE t.title = ? AND c.character_name NOT IN ({excl_ph})
                ORDER BY c.cast_order LIMIT 3
            """
            async with ddb.execute(query, [movie_title] + exclude) as c:
                distractors = [r[0] for r in await c.fetchall()]

            if len(distractors) >= 3:
                return distractors[:3]

            # Tier B: Characters from same-genre titles, lead cast only
            if len(distractors) < 3:
                needed = 3 - len(distractors)
                excl = exclude + distractors
                excl_ph = ",".join("?" for _ in excl)
                query = f"""
                    SELECT DISTINCT c.character_name FROM characters c
                    JOIN titles t ON c.title_id = t.id
                    WHERE t.is_tv = ? AND c.character_name NOT IN ({excl_ph})
                    AND t.genres LIKE '%' || ? || '%'
                    AND c.cast_order <= 2
                    ORDER BY RANDOM() LIMIT ?
                """
                async with ddb.execute(query, [is_tv] + excl + [primary_genre, needed]) as c:
                    distractors.extend(r[0] for r in await c.fetchall())

            # Tier C: Characters from same type, any genre, lead cast
            if len(distractors) < 3:
                needed = 3 - len(distractors)
                excl = exclude + distractors
                excl_ph = ",".join("?" for _ in excl)
                query = f"""
                    SELECT DISTINCT c.character_name FROM characters c
                    JOIN titles t ON c.title_id = t.id
                    WHERE t.is_tv = ? AND c.character_name NOT IN ({excl_ph})
                    AND c.cast_order <= 3
                    ORDER BY RANDOM() LIMIT ?
                """
                async with ddb.execute(query, [is_tv] + excl + [needed]) as c:
                    distractors.extend(r[0] for r in await c.fetchall())

        return distractors[:3]

    async def _pad_distractors(self, db: aiosqlite.Connection, correct: str, question_type: str, existing: list) -> list:
        needed = 3 - len(existing)
        if needed <= 0:
            return existing

        # Try TMDb pool first
        if os.path.exists(self.distractors_db_path):
            async with aiosqlite.connect(self.distractors_db_path) as ddb:
                exclude = [correct] + existing
                excl_ph = ",".join("?" for _ in exclude)
                if question_type == "movie":
                    query = f"SELECT DISTINCT title FROM titles WHERE title NOT IN ({excl_ph}) ORDER BY RANDOM() LIMIT ?"
                else:
                    query = f"SELECT DISTINCT character_name FROM characters WHERE character_name NOT IN ({excl_ph}) ORDER BY RANDOM() LIMIT ?"
                async with ddb.execute(query, exclude + [needed]) as cursor:
                    existing.extend(r[0] for r in await cursor.fetchall())

        # Quotes table fallback
        if len(existing) < 3:
            needed = 3 - len(existing)
            column = "movie_title" if question_type == "movie" else "character"
            exclude = [correct] + existing
            placeholders = ",".join("?" for _ in exclude)
            async with db.execute(
                f"SELECT DISTINCT {column} FROM quotes WHERE {column} NOT IN ({placeholders}) ORDER BY RANDOM() LIMIT ?",
                exclude + [needed]
            ) as cursor:
                existing.extend(r[0] for r in await cursor.fetchall())

        return existing[:3]

    # === Answer Handling ===

    async def _handle_answer_button(self, interaction: discord.Interaction, answer_index: int):
        try:
            if await self._is_user_blocked(interaction.user.id):
                return await interaction.response.send_message("You are currently blocked from participating.", ephemeral=True)

            user_id_str = str(interaction.user.id)

            async with self.config_lock:
                global_data = self.get_global_data()
                if any(d.get("user_id") == user_id_str for d in global_data.get("daily_interactions", [])):
                    return await interaction.response.send_message("You have already answered today's quote!", ephemeral=True)
                q_data = global_data.get("daily_question_data")

            if not q_data:
                return await interaction.response.send_message("Today's quote hasn't been posted yet.", ephemeral=True)

            # Support old data without correct_index by computing from answers/correct
            correct_index = q_data.get("correct_index")
            if correct_index is None:
                try:
                    correct_index = q_data["answers"].index(q_data["correct"])
                except (ValueError, KeyError):
                    return await interaction.response.send_message("Question data is corrupted. Please contact an admin.", ephemeral=True)

            # Check primary correct index and any alt correct indices
            alt_correct = q_data.get("alt_correct_indices", [])
            is_correct = (answer_index == correct_index) or (answer_index in alt_correct)
            points = 0
            final_score = 0

            async with self.config_lock:
                global_data = self.get_global_data()
                stats = self.get_user_stats(interaction.user.id)

                # Snapshot pre-answer state so Void Today can restore perfectly
                snapshot = {
                    "streak": stats.get("current_streak", 0),
                    "longest_streak": stats.get("longest_streak", 0),
                    "streak_achieved_at": stats.get("streak_achieved_at"),
                    "correct": stats.get("correct", 0),
                    "incorrect": stats.get("incorrect", 0),
                    "all_time_score": stats.get("all_time_score", 0),
                    "all_time_timestamp": stats.get("all_time_timestamp"),
                }

                score_data = global_data.setdefault("scores", {}).setdefault(user_id_str, {"score": 0, "timestamp": None})
                if isinstance(score_data, int):
                    score_data = {"score": score_data, "timestamp": None}
                    global_data["scores"][user_id_str] = score_data

                snapshot["monthly_score"] = score_data.get("score", 0)
                snapshot["monthly_timestamp"] = score_data.get("timestamp")

                if is_correct:
                    stats["correct"] += 1
                    stats["current_streak"] += 1
                    stats["longest_streak"] = max(stats["current_streak"], stats["longest_streak"])
                    stats["streak_achieved_at"] = datetime.now(timezone.utc).isoformat()
                    points = 1
                else:
                    stats["incorrect"] += 1
                    stats["current_streak"] = 0
                    stats["streak_achieved_at"] = None

                score_data["score"] = score_data.get("score", 0) + points
                stats["all_time_score"] = stats.get("all_time_score", 0) + points

                if points > 0:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    score_data["timestamp"] = now_iso
                    stats["all_time_timestamp"] = now_iso

                final_score = score_data["score"]

                global_data.setdefault("daily_interactions", []).append({
                    "user_id": user_id_str,
                    "answer_index": answer_index,
                    "correct": is_correct,
                    "snapshot": snapshot,
                })
                self.config_is_dirty = True

            correct_answer = q_data.get("correct", "Unknown")
            if is_correct:
                # Show the answer they picked, not always the primary correct
                chosen_answer = q_data.get("answers", [])[answer_index] if answer_index < len(q_data.get("answers", [])) else correct_answer
                pts_text = f" (+{points} pts)" if points > 1 else ""
                response = f"✅ **Correct!** The answer was **{chosen_answer}**.{pts_text}"
            else:
                response = f"❌ Incorrect. The answer was **{correct_answer}**."

            streak_text = ""
            if is_correct and stats["current_streak"] > 1:
                streak_text = f"\n🔥 Streak: **{stats['current_streak']}**"

            await interaction.response.send_message(
                f"{response}\nYour monthly score is now **{final_score}**.{streak_text}",
                ephemeral=True
            )
            await self.update_gateway_message(interaction.guild.id)

        except Exception as e:
            log_quote.error(f"Critical error in _handle_answer_button for {interaction.user}: {e}", exc_info=True)
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("A critical error occurred. Please contact an admin.", ephemeral=True)
                else:
                    await interaction.response.send_message("A critical error occurred. Please contact an admin.", ephemeral=True)
            except discord.HTTPException:
                pass

    # === Gateway Message ===

    async def _build_daily_embed(self, guild: discord.Guild) -> discord.Embed:
        global_data = self.get_global_data()
        q_data = global_data.get("daily_question_data", {})
        now_ct = datetime.now(QUOTE_TIMEZONE)

        quote_text = q_data.get("quote", "")
        question_type = q_data.get("question_type", "movie")
        prompt = "Which movie or show is this quote from?" if question_type == "movie" else "Who said this quote?"

        attempts = len(global_data.get('daily_interactions', []))

        embed = discord.Embed(
            description=f'## *"{quote_text}"*\n\n{prompt}',
            color=EMBED_COLOR_QUOTE
        )

        all_stats = global_data.get("user_stats", {})
        max_streak = max(
            (s.get("current_streak", 0) for s in all_stats.values()),
            default=0
        )
        if max_streak >= 3:
            streak_text = await self._get_longest_streak_text(guild)
            embed.add_field(name="🔥 Longest Streak", value=streak_text, inline=False)

        monthly_text = await self._get_gateway_leaderboard_text(guild)
        embed.add_field(name="🏆 Monthly Top 3", value=monthly_text, inline=False)

        embed.set_footer(text=f"🎬 {now_ct.strftime('%A')}'s Daily Quote | {attempts} attempt{'s' if attempts != 1 else ''}")
        return embed

    async def update_gateway_message(self, guild_id: int):
        cfg_settings = self.get_guild_settings(guild_id)
        msg_id = cfg_settings.get("gateway_message_id")
        chan_id = cfg_settings.get("channel_id")
        if not msg_id or not chan_id:
            return
        try:
            channel = self.bot.get_channel(chan_id) or await self.bot.fetch_channel(chan_id)
            message = await channel.fetch_message(msg_id)
            q_data = self.get_global_data().get("daily_question_data", {})
            answers = q_data.get("answers", [])
            view = QuoteAnswerView(self, answers) if answers else QuoteAnswerView(self)
            await message.edit(embed=await self._build_daily_embed(message.guild), view=view)
        except discord.HTTPException:
            pass

    # === Daily Post Logic ===

    async def _delete_quote_from_db(self, quote_id: int | None):
        """Permanently delete a used or denied quote from the database."""
        if not quote_id or not os.path.exists(self.db_path):
            return
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM quotes WHERE id = ?", (quote_id,))
                await db.commit()
            log_quote.info(f"Deleted used quote ID {quote_id} from database")
        except Exception as e:
            log_quote.error(f"Failed to delete quote ID {quote_id}: {e}")

    async def _retire_old_question(self):
        """Delete the outgoing daily question from DB and clean used_quote_ids."""
        async with self.config_lock:
            global_data = self.get_global_data()
            old_q = global_data.get("daily_question_data")
            if not old_q:
                return
            old_id = old_q.get("quote_id")
            used = global_data.get("used_quote_ids", [])
            if old_id and old_id in used:
                used.remove(old_id)
                self.config_is_dirty = True
        await self._delete_quote_from_db(old_id)

    async def _trigger_daily_post(self, guild: discord.Guild):
        q_data = await self._get_daily_question()
        if not q_data:
            log_quote.error(f"Failed to generate daily question for guild {guild.id}")
            return

        # Delete the outgoing question from DB before replacing it
        await self._retire_old_question()

        async with self.config_lock:
            global_data = self.get_global_data()
            cfg_settings = self.get_guild_settings(guild.id)

            # Break streaks for users who missed the previous day's quote
            if global_data.get("daily_question_data") is not None:
                answered_ids = {d.get("user_id") for d in global_data.get("daily_interactions", [])}
                for uid, stats in global_data.get("user_stats", {}).items():
                    if stats.get("current_streak", 0) > 0 and uid not in answered_ids:
                        stats["current_streak"] = 0
                        stats["streak_achieved_at"] = None

            global_data["daily_question_data"] = q_data
            global_data["daily_interactions"] = []

            now_ct = datetime.now(QUOTE_TIMEZONE)
            cfg_settings["last_posted_date"] = now_ct.date().isoformat()
            self.config_is_dirty = True

        await self._post_gateway_message(guild)
        await self._check_low_quote_alert(guild)

    async def _post_gateway_message(self, guild: discord.Guild):
        cfg_settings = self.get_guild_settings(guild.id)
        channel_id = cfg_settings.get("channel_id")
        if not channel_id:
            log_quote.warning(f"Quote channel not set for guild {guild.id}")
            return

        try:
            channel = await self.bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden) as e:
            log_quote.error(f"Cannot access quote channel {channel_id}: {e}")
            return

        perms = channel.permissions_for(guild.me)
        if not perms.view_channel or not perms.send_messages or not perms.embed_links:
            log_quote.error(f"Missing permissions in quote channel {channel_id}")
            return

        if isinstance(channel, discord.Thread) and (channel.archived or channel.locked):
            try:
                await channel.edit(archived=False, locked=False)
            except discord.Forbidden:
                log_quote.error(f"Need Manage Threads permission to unarchive {channel.mention}")
                return

        if gateway_id := cfg_settings.get("gateway_message_id"):
            try:
                old_msg = channel.get_partial_message(gateway_id)
                try:
                    await old_msg.unpin()
                except discord.HTTPException:
                    pass
                await old_msg.delete()
            except discord.HTTPException:
                pass

        q_data = self.get_global_data().get("daily_question_data", {})
        answers = q_data.get("answers", [])

        try:
            view = QuoteAnswerView(self, answers) if answers else QuoteAnswerView(self)
            msg = await channel.send(embed=await self._build_daily_embed(guild), view=view)
            async with self.config_lock:
                self.get_guild_settings(guild.id)["gateway_message_id"] = msg.id
                self.config_is_dirty = True
            # Pin silently: pin the message, then delete the system notification
            try:
                await msg.pin()
                async for m in channel.history(limit=5, after=msg):
                    if m.type == discord.MessageType.pins_add:
                        await m.delete()
                        break
            except discord.HTTPException:
                pass
        except discord.HTTPException as e:
            log_quote.error(f"Failed to send quote gateway message: {e}")

    async def _check_low_quote_alert(self, guild: discord.Guild):
        cfg = self.get_guild_settings(guild.id)
        alert_days = cfg.get("low_quote_alert_days", DEFAULT_LOW_QUOTE_ALERT_DAYS)
        today = datetime.now(QUOTE_TIMEZONE).date().isoformat()

        if cfg.get("last_low_quote_alert_date") == today:
            return

        remaining = await self._get_remaining_quotes()
        if remaining > alert_days:
            return

        channel_id = cfg.get("channel_id")
        if not channel_id:
            return

        try:
            channel = await self.bot.fetch_channel(channel_id)
            embed = discord.Embed(
                title="⚠️ Low Quotes Alert",
                description=(
                    f"Only **{remaining}** quotes remaining in the database!\n"
                    f"At 1 per day, quotes will run out in **{remaining} days**.\n\n"
                    f"Consider adding more quotes to `quotes.db`."
                ),
                color=0xFF6B6B
            )
            await channel.send(embed=embed)

            async with self.config_lock:
                cfg["last_low_quote_alert_date"] = today
                self.config_is_dirty = True

            log_quote.warning(f"Low quote alert sent for guild {guild.id}: {remaining} quotes remaining")
        except discord.HTTPException as e:
            log_quote.error(f"Failed to send low quote alert: {e}")

    # === Preview / Approval System ===

    async def _send_preview_question(self, guild: discord.Guild):
        """Generate a question and DM it to the bot owner for approval."""
        q_data = await self._get_daily_question()
        if not q_data:
            log_quote.error(f"Failed to generate preview question for guild {guild.id}")
            return

        q_data["guild_id"] = guild.id  # Store guild context for DM approve/deny

        async with self.config_lock:
            self.get_global_data()["pending_question_data"] = q_data
            self.config_is_dirty = True

        # DM the bot owner
        try:
            app_info = await self.bot.application_info()
            owner = app_info.owner
            dm = await owner.create_dm()
        except Exception as e:
            log_quote.error(f"Cannot DM bot owner for preview: {e}")
            return

        question_type = q_data.get("question_type", "movie")
        prompt = "Which movie or show is this quote from?" if question_type == "movie" else "Who said this quote?"

        answer_labels = ["A", "B", "C", "D"]
        correct_idx = q_data.get("correct_index", 0)
        embed = discord.Embed(
            title="📋 Daily Quote — Pending Approval",
            description=(
                f'## *"{q_data["quote"]}"*\n\n'
                f"**{prompt}**\n\n"
                + "\n".join(
                    f"**{answer_labels[i]}.** {a}"
                    for i, a in enumerate(q_data["answers"])
                )
            ),
            color=0xFFA500,
        )
        embed.add_field(
            name="Details",
            value=(
                f"**Difficulty:** {q_data.get('difficulty', '?')}\n"
                f"**Quote ID:** {q_data['quote_id']}"
            ),
            inline=False,
        )

        try:
            await dm.send(embed=embed, view=QuotePreviewView(self))
            log_quote.info(f"Preview question DM'd to owner for guild {guild.id}")
        except discord.HTTPException as e:
            log_quote.error(f"Failed to DM preview question: {e}")

    async def _approve_pending_quote(self, guild: discord.Guild):
        """Move pending question to live and post to the main channel."""
        # Delete the outgoing question from DB before replacing it
        await self._retire_old_question()

        async with self.config_lock:
            global_data = self.get_global_data()
            cfg_settings = self.get_guild_settings(guild.id)
            pending = global_data.get("pending_question_data")
            if not pending:
                return

            # Break streaks for users who missed the previous day's quote
            if global_data.get("daily_question_data") is not None:
                answered_ids = {d.get("user_id") for d in global_data.get("daily_interactions", [])}
                for uid, stats in global_data.get("user_stats", {}).items():
                    if stats.get("current_streak", 0) > 0 and uid not in answered_ids:
                        stats["current_streak"] = 0
                        stats["streak_achieved_at"] = None

            global_data["daily_question_data"] = pending
            global_data["pending_question_data"] = None
            global_data["daily_interactions"] = []

            now_ct = datetime.now(QUOTE_TIMEZONE)
            cfg_settings["last_posted_date"] = now_ct.date().isoformat()
            self.config_is_dirty = True

        await self._post_gateway_message(guild)
        await self._check_low_quote_alert(guild)

    # === Background Tasks ===

    @tasks.loop(time=time(19, 0, tzinfo=QUOTE_TIMEZONE))
    async def quote_loop(self):
        try:
            now_ct = datetime.now(QUOTE_TIMEZONE)

            guild_settings_pool = self.config.get("guild_settings", {})
            ran_monthly_reset = False

            for gid_str, cfg_settings in list(guild_settings_pool.items()):
                if not cfg_settings.get("enabled") or not cfg_settings.get("channel_id"):
                    continue

                try:
                    guild = self.bot.get_guild(int(gid_str))
                    if not guild or not guild.me:
                        log_quote.warning(f"Could not find or access guild {gid_str}, skipping quote loop.")
                        continue

                    if cfg_settings.get("last_posted_date") == now_ct.date().isoformat():
                        continue

                    log_quote.info(f"Posting daily quote for guild {guild.id}")

                    if now_ct.day == 1:
                        await self._post_monthly_leaderboard(guild)
                        if not ran_monthly_reset:
                            async with self.config_lock:
                                if self.get_global_data().get("scores"):
                                    self.get_global_data()["scores"] = {}
                                    self.config_is_dirty = True
                                    log_quote.info("Monthly quote scores have been reset.")
                            ran_monthly_reset = True

                    # Post today's question: use pending if available, otherwise generate fresh
                    pending = self.get_global_data().get("pending_question_data")
                    if pending and cfg_settings.get("screening_enabled", True):
                        await self._approve_pending_quote(guild)
                    else:
                        await self._trigger_daily_post(guild)

                    # Generate tomorrow's preview if screening is enabled
                    if cfg_settings.get("screening_enabled", True):
                        await self._send_preview_question(guild)

                except Exception as e:
                    log_quote.error(f"Error in quote loop for guild {gid_str}: {e}", exc_info=True)

            # Monthly reset already handled inside the guild loop above
        except Exception as e:
            await self.bot.error_reporter.report("DailyQuote", f"quote_loop: {e}")

    @tasks.loop(seconds=60)
    async def backup_save_loop(self):
        try:
            if self.config_is_dirty:
                await self.save_config_now()
        except Exception as e:
            await self.bot.error_reporter.report("DailyQuote", f"backup_save_loop: {e}")

    @tasks.loop(time=time(18, 30, tzinfo=QUOTE_TIMEZONE))
    async def auto_approve_loop(self):
        """Auto-approve pending questions 30 minutes before post time if admins haven't acted."""
        try:
            pending = self.get_global_data().get("pending_question_data")
            if not pending or pending.get("approved"):
                return

            async with self.config_lock:
                pending = self.get_global_data().get("pending_question_data")
                if pending and not pending.get("approved"):
                    pending["approved"] = True
                    self.config_is_dirty = True
                    log_quote.info("Pending question auto-approved (30 min before post time)")
        except Exception as e:
            await self.bot.error_reporter.report("DailyQuote", f"auto_approve_loop: {e}")

    @quote_loop.before_loop
    async def before_quote_loop(self):
        await self.bot.wait_until_ready()

    @backup_save_loop.before_loop
    async def before_backup_save_loop(self):
        await self.bot.wait_until_ready()

    @auto_approve_loop.before_loop
    async def before_auto_approve_loop(self):
        await self.bot.wait_until_ready()

    # === Monthly Leaderboard ===

    async def _post_monthly_leaderboard(self, guild: discord.Guild):
        global_data = self.get_global_data()
        scores = global_data.get("scores", {})
        if not scores:
            return

        cfg_settings = self.get_guild_settings(guild.id)
        channel_id = cfg_settings.get("channel_id")
        if not channel_id:
            return

        sorted_scores = sorted(scores.items(), key=self._get_score_sort_key)
        if not sorted_scores:
            return

        try:
            channel = await self.bot.fetch_channel(channel_id)
            last_month = (datetime.now(QUOTE_TIMEZONE) - timedelta(days=1)).strftime("%B %Y")

            lines = []
            medals = ["🥇", "🥈", "🥉"]
            for i, (uid, dat) in enumerate(sorted_scores[:20]):
                name = await self._get_formatted_name(guild, uid)
                s = dat.get("score", 0) if isinstance(dat, dict) else dat
                prefix = medals[i] if i < 3 else f"**{i+1}.**"
                lines.append(f"{prefix} **{name}** - {s} pts")

            embed = discord.Embed(
                title=f"🏆 {last_month} — Final Standings",
                description="\n".join(lines),
                color=EMBED_COLOR_QUOTE
            )

            await channel.send(embed=embed)
            log_quote.info(f"Monthly leaderboard posted for guild {guild.id}")

        except discord.HTTPException as e:
            log_quote.error(f"Failed to post monthly leaderboard: {e}")

    # === Slash Commands ===

    @dailyquote.command(name="panel", description="Open the Daily Quote admin panel.")
    @app_commands.check(is_quote_admin_check)
    async def panel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="🎬 Daily Quote Admin Panel",
            description="Use the buttons below to manage the Daily Quote cog.",
            color=EMBED_COLOR_QUOTE
        )
        await interaction.response.send_message(embed=embed, view=QuoteAdminPanelView(self), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(DailyQuote(bot))
