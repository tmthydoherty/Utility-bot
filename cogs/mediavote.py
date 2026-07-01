import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import asyncio
import logging
import copy
import io
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional

# ─── Constants ───────────────────────────────────────────────────────────────
CONFIG_FILE = "mediavote_config.json"
EMBED_COLOR = 0xE91E63
RESULTS_COLOR = 0xF1C40F
RANK_VALUES = {"1": 15, "2": 10, "3": 5}
RANK_LABELS = {"1": "1st", "2": "2nd", "3": "3rd"}
EST = ZoneInfo("America/New_York")

# ─── Logger ──────────────────────────────────────────────────────────────────
logger = logging.getLogger('mediavote')
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ─── Config I/O ──────────────────────────────────────────────────────────────
def _load_config_sync(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error loading config: {e}")
        return {}


def _save_config_sync(file_path: str, data: Dict[str, Any]):
    try:
        temp = f"{file_path}.tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp, file_path)
    except IOError as e:
        logger.error(f"Error saving config: {e}")


class ConfigManager:
    def __init__(self, file_path: str):
        self.file_path = file_path

    async def load(self) -> Dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None, _load_config_sync, self.file_path
        )

    async def save(self, data: Dict[str, Any]):
        await asyncio.get_running_loop().run_in_executor(
            None, _save_config_sync, self.file_path, copy.deepcopy(data)
        )


# ═══════════════════════════════════════════════════════════════════════════
#  UI COMPONENTS
# ═══════════════════════════════════════════════════════════════════════════

# ─── Event Name Modal ────────────────────────────────────────────────────────
class EventNameModal(discord.ui.Modal, title="Create Event"):
    event_name = discord.ui.TextInput(
        label="Event Name",
        placeholder="e.g. June Art Contest",
        min_length=3,
        max_length=80,
        required=True
    )

    def __init__(self, cog: 'MediaVote'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        name = self.event_name.value.strip()
        thread_name = re.sub(r'[^a-z0-9\s-]', '', name.lower()).strip()
        thread_name = re.sub(r'\s+', '-', thread_name)

        if not thread_name:
            await interaction.response.send_message(
                "Event name must contain at least one letter or number.", ephemeral=True
            )
            return

        wizard_data = {"display_name": name, "name": thread_name}

        embed = discord.Embed(
            title="Create Event — Step 2/5",
            description=(
                f"**Event:** {name}\n"
                f"**Thread:** #{thread_name}\n\n"
                "Select the channel where users will submit their entries."
            ),
            color=EMBED_COLOR
        )
        await interaction.response.send_message(
            embed=embed,
            view=WizardChannelSelectView(self.cog, wizard_data),
            ephemeral=True
        )


# ─── Wizard: Channel Select ─────────────────────────────────────────────────
class WizardChannelSelectView(discord.ui.View):
    def __init__(self, cog: 'MediaVote', wizard_data: dict):
        super().__init__(timeout=180)
        self.cog = cog
        self.wizard_data = wizard_data

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="Select submission channel...",
        row=0
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        self.wizard_data["channel_id"] = channel.id

        embed = discord.Embed(
            title="Create Event — Step 3/5",
            description=(
                f"**Event:** {self.wizard_data['display_name']}\n"
                f"**Channel:** <#{channel.id}>\n\n"
                "Select when **submissions close** (times are in EST)."
            ),
            color=EMBED_COLOR
        )
        await interaction.response.edit_message(
            embed=embed,
            view=DateTimePickerView(self.cog, self.wizard_data, "submission")
        )


# ─── Wizard: DateTime Picker Components ─────────────────────────────────────
class _DateSelect(discord.ui.Select):
    def __init__(self, parent_view: 'DateTimePickerView'):
        self.parent_view = parent_view
        now = datetime.now(EST)
        options = []
        for i in range(14):
            day = now + timedelta(days=i)
            if i == 0:
                label = f"Today ({day.strftime('%b %d')})"
            elif i == 1:
                label = f"Tomorrow ({day.strftime('%b %d')})"
            else:
                label = day.strftime("%a %b %d")
            options.append(discord.SelectOption(label=label, value=day.strftime("%Y-%m-%d")))
        super().__init__(placeholder="Select date...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.selected_date = self.values[0]
        await interaction.response.defer()


class _HourSelect(discord.ui.Select):
    def __init__(self, parent_view: 'DateTimePickerView'):
        self.parent_view = parent_view
        options = []
        for h in range(24):
            if h == 0:
                label = "12 AM"
            elif h < 12:
                label = f"{h} AM"
            elif h == 12:
                label = "12 PM"
            else:
                label = f"{h - 12} PM"
            options.append(discord.SelectOption(label=label, value=str(h)))
        super().__init__(placeholder="Select hour...", options=options, row=1)

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.selected_hour = self.values[0]
        await interaction.response.defer()


class _MinuteSelect(discord.ui.Select):
    def __init__(self, parent_view: 'DateTimePickerView'):
        self.parent_view = parent_view
        options = [
            discord.SelectOption(label=":00", value="0"),
            discord.SelectOption(label=":15", value="15"),
            discord.SelectOption(label=":30", value="30"),
            discord.SelectOption(label=":45", value="45"),
        ]
        super().__init__(placeholder="Select minutes...", options=options, row=2)

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.selected_minute = self.values[0]
        await interaction.response.defer()


class DateTimePickerView(discord.ui.View):
    def __init__(self, cog: 'MediaVote', wizard_data: dict, purpose: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.wizard_data = wizard_data
        self.purpose = purpose  # "submission" or "voting"
        self.selected_date: Optional[str] = None
        self.selected_hour: Optional[str] = None
        self.selected_minute: Optional[str] = None

        self.add_item(_DateSelect(self))
        self.add_item(_HourSelect(self))
        self.add_item(_MinuteSelect(self))

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, row=3)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.selected_date is None or self.selected_hour is None or self.selected_minute is None:
            await interaction.response.send_message(
                "Please select a date, hour, and minutes before confirming.",
                ephemeral=True
            )
            return

        year, month, day = self.selected_date.split("-")
        dt_est = datetime(
            int(year), int(month), int(day),
            int(self.selected_hour), int(self.selected_minute),
            tzinfo=EST
        )
        dt = dt_est.astimezone(timezone.utc)

        if dt <= datetime.now(timezone.utc):
            await interaction.response.send_message(
                "The selected time must be in the future.", ephemeral=True
            )
            return

        if self.purpose == "submission":
            self.wizard_data["submission_deadline"] = dt

            embed = discord.Embed(
                title="Create Event — Step 4/5",
                description=(
                    f"**Event:** {self.wizard_data['display_name']}\n"
                    f"**Channel:** <#{self.wizard_data['channel_id']}>\n"
                    f"**Submissions close:** {discord.utils.format_dt(dt, 'F')} ({discord.utils.format_dt(dt, 'R')})\n\n"
                    "Now select when **voting ends** (times are in EST)."
                ),
                color=EMBED_COLOR
            )
            await interaction.response.edit_message(
                embed=embed,
                view=DateTimePickerView(self.cog, self.wizard_data, "voting")
            )
        else:  # voting
            if dt <= self.wizard_data["submission_deadline"]:
                await interaction.response.send_message(
                    "Voting deadline must be after the submission deadline.",
                    ephemeral=True
                )
                return

            self.wizard_data["voting_deadline"] = dt
            sub_dt = self.wizard_data["submission_deadline"]

            embed = discord.Embed(
                title="Create Event — Review",
                description=(
                    f"**Event:** {self.wizard_data['display_name']}\n"
                    f"**Thread:** #{self.wizard_data['name']}\n"
                    f"**Channel:** <#{self.wizard_data['channel_id']}>\n"
                    f"**Submissions close:** {discord.utils.format_dt(sub_dt, 'F')} ({discord.utils.format_dt(sub_dt, 'R')})\n"
                    f"**Voting ends:** {discord.utils.format_dt(dt, 'F')} ({discord.utils.format_dt(dt, 'R')})\n\n"
                    "Ready to launch?"
                ),
                color=EMBED_COLOR
            )
            await interaction.response.edit_message(
                embed=embed,
                view=ConfirmLaunchView(self.cog, self.wizard_data)
            )


# ─── Wizard: Confirm Launch ──────────────────────────────────────────────────
class ConfirmLaunchView(discord.ui.View):
    def __init__(self, cog: 'MediaVote', wizard_data: dict):
        super().__init__(timeout=180)
        self.cog = cog
        self.wizard_data = wizard_data

    @discord.ui.button(label="Launch Event", style=discord.ButtonStyle.success)
    async def launch_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        guild_id = str(interaction.guild_id)

        # Guard: no active event
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            if gc.get("active_event"):
                await interaction.edit_original_response(
                    embed=discord.Embed(description="❌ An event is already active. End it first.", color=discord.Color.red()),
                    view=None
                )
                return

        channel = interaction.guild.get_channel(self.wizard_data["channel_id"])
        if not channel:
            await interaction.edit_original_response(
                embed=discord.Embed(description="❌ The selected channel was not found.", color=discord.Color.red()),
                view=None
            )
            return

        # Create thread
        try:
            thread = await channel.create_thread(
                name=self.wizard_data["name"],
                type=discord.ChannelType.public_thread,
                auto_archive_duration=4320
            )
        except discord.Forbidden:
            await interaction.edit_original_response(
                embed=discord.Embed(description="❌ I don't have permission to create threads in that channel.", color=discord.Color.red()),
                view=None
            )
            return
        except discord.HTTPException as e:
            logger.error(f"Thread creation failed: {e}")
            await interaction.edit_original_response(
                embed=discord.Embed(description=f"❌ Failed to create thread: {e}", color=discord.Color.red()),
                view=None
            )
            return

        # Delete the "started a thread" message in the parent channel to keep it hidden
        try:
            starter_msg = await channel.fetch_message(thread.id)
            await starter_msg.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

        # Send sticky in submission channel
        sub_dl = self.wizard_data["submission_deadline"]
        sticky_embed = discord.Embed(
            title=self.wizard_data['display_name'],
            description=(
                "Submit your entry by attaching an image to a message saying **Entry**.\n\n"
                f"Submissions close {discord.utils.format_dt(sub_dl, 'R')} ({discord.utils.format_dt(sub_dl, 'F')})"
            ),
            color=EMBED_COLOR
        )
        sticky_msg = await channel.send(embed=sticky_embed)

        # Save config
        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["active_event"] = {
                "name": self.wizard_data["name"],
                "display_name": self.wizard_data["display_name"],
                "channel_id": self.wizard_data["channel_id"],
                "thread_id": thread.id,
                "sticky_message_id": sticky_msg.id,
                "sticky_text": None,
                "submission_deadline": self.wizard_data["submission_deadline"].isoformat(),
                "voting_deadline": self.wizard_data["voting_deadline"].isoformat(),
                "phase": "submissions",
                "entries": {},
                "votes": {}
            }
            await self.cog._save()

        vote_dl = self.wizard_data["voting_deadline"]
        embed = discord.Embed(
            title="Event Launched",
            description=(
                f"**{self.wizard_data['display_name']}** is now live.\n\n"
                f"**Channel:** <#{self.wizard_data['channel_id']}>\n"
                f"**Thread:** <#{thread.id}>\n"
                f"**Submissions close:** {discord.utils.format_dt(sub_dl, 'R')}\n"
                f"**Voting ends:** {discord.utils.format_dt(vote_dl, 'R')}"
            ),
            color=0x57F287
        )
        await interaction.edit_original_response(embed=embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(description="Event creation cancelled.", color=discord.Color.greyple()),
            view=None
        )


# ─── Edit Sticky Modal ──────────────────────────────────────────────────────
class EditStickyModal(discord.ui.Modal, title="Edit Sticky Message"):
    sticky_text = discord.ui.TextInput(
        label="Sticky Message Text",
        style=discord.TextStyle.paragraph,
        placeholder="Custom instructions for submitters...",
        max_length=1000,
        required=True
    )

    def __init__(self, cog: 'MediaVote', current_text: Optional[str] = None):
        super().__init__()
        self.cog = cog
        if current_text:
            self.sticky_text.default = current_text

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild_id = str(interaction.guild_id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            event = gc.get("active_event")
            if not event:
                await interaction.followup.send("No active event.", ephemeral=True)
                return
            event["sticky_text"] = self.sticky_text.value.strip()
            await self.cog._save()

        channel = interaction.guild.get_channel(event["channel_id"])
        if channel:
            await self.cog._send_sticky(channel, guild_id)

        await interaction.followup.send("✅ Sticky message updated.", ephemeral=True)


# ─── Confirm Action View ────────────────────────────────────────────────────
class ConfirmActionView(discord.ui.View):
    def __init__(self, action_label: str, callback):
        super().__init__(timeout=60)
        self.callback_fn = callback

        confirm_btn = discord.ui.Button(
            label=f"Confirm: {action_label}",
            style=discord.ButtonStyle.danger
        )
        confirm_btn.callback = self._confirm
        self.add_item(confirm_btn)

        cancel_btn = discord.ui.Button(
            label="Go Back",
            style=discord.ButtonStyle.secondary
        )
        cancel_btn.callback = self._cancel
        self.add_item(cancel_btn)

    async def _confirm(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            embed=discord.Embed(description="⏳ Processing...", color=discord.Color.greyple()),
            view=None
        )
        try:
            await self.callback_fn()
            await interaction.edit_original_response(
                embed=discord.Embed(description="✅ Done!", color=0x57F287)
            )
        except Exception as e:
            logger.error(f"Confirm action error: {e}", exc_info=True)
            await interaction.edit_original_response(
                embed=discord.Embed(description="❌ An error occurred.", color=discord.Color.red())
            )
        self.stop()

    async def _cancel(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            embed=discord.Embed(description="Action cancelled.", color=discord.Color.greyple()),
            view=None
        )
        self.stop()


# ─── Admin Channel Select ───────────────────────────────────────────────────
class AdminChannelSelectView(discord.ui.View):
    def __init__(self, cog: 'MediaVote'):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="Select admin notification channel...",
        row=0
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        await interaction.response.defer(ephemeral=True)
        channel = select.values[0]
        guild_id = str(interaction.guild_id)

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["admin_channel_id"] = channel.id
            await self.cog._save()

        await interaction.edit_original_response(
            embed=discord.Embed(
                description=f"✅ Admin channel set to <#{channel.id}>.",
                color=0x57F287
            ),
            view=None
        )


# ─── Admin Role Select ──────────────────────────────────────────────────────
class AdminRoleSelectView(discord.ui.View):
    def __init__(self, cog: 'MediaVote'):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select admin roles...",
        min_values=1,
        max_values=10,
        row=0
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        await interaction.response.defer(ephemeral=True)
        guild_id = str(interaction.guild_id)
        role_ids = [r.id for r in select.values]

        async with self.cog.config_lock:
            gc = self.cog._get_guild_config(guild_id)
            gc["admin_roles"] = role_ids
            await self.cog._save()

        role_mentions = ", ".join(f"<@&{r}>" for r in role_ids)
        await interaction.edit_original_response(
            embed=discord.Embed(
                description=f"Admin roles set to {role_mentions}.",
                color=0x57F287
            ),
            view=None
        )


# ─── Submission Vote View (persistent buttons on each thread entry) ──────────
class SubmissionVoteView(discord.ui.View):
    """Attached to each submission in the showcase thread. Buttons handled via on_interaction."""
    def __init__(self, submitter_id: str):
        super().__init__(timeout=None)
        for rank in [1, 2, 3]:
            btn = discord.ui.Button(
                label=f"#{rank}",
                custom_id=f"mv:{rank}:{submitter_id}",
                style=discord.ButtonStyle.secondary
            )
            self.add_item(btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return False  # All handling done in on_interaction


# ─── Admin Panel View ───────────────────────────────────────────────────────
class MediaVotePanelView(discord.ui.View):
    def __init__(self, cog: 'MediaVote', guild_id: str, event: Optional[dict]):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id

        if not event:
            create_btn = discord.ui.Button(label="Create Event", style=discord.ButtonStyle.success, row=0)
            create_btn.callback = self._create_event
            self.add_item(create_btn)

            admin_btn = discord.ui.Button(label="Set Admin Channel", style=discord.ButtonStyle.secondary, row=0)
            admin_btn.callback = self._set_admin_channel
            self.add_item(admin_btn)

            roles_btn = discord.ui.Button(label="Admin Roles", style=discord.ButtonStyle.secondary, row=0)
            roles_btn.callback = self._set_admin_roles
            self.add_item(roles_btn)

        elif event["phase"] == "submissions":
            edit_btn = discord.ui.Button(label="Edit Sticky", style=discord.ButtonStyle.primary, row=0)
            edit_btn.callback = self._edit_sticky
            self.add_item(edit_btn)

            end_sub_btn = discord.ui.Button(label="End Submissions Early", style=discord.ButtonStyle.danger, row=0)
            end_sub_btn.callback = self._end_submissions
            self.add_item(end_sub_btn)

            cancel_btn = discord.ui.Button(label="Cancel Event", style=discord.ButtonStyle.danger, row=1)
            cancel_btn.callback = self._cancel_event
            self.add_item(cancel_btn)

            admin_btn = discord.ui.Button(label="Set Admin Channel", style=discord.ButtonStyle.secondary, row=1)
            admin_btn.callback = self._set_admin_channel
            self.add_item(admin_btn)

            roles_btn = discord.ui.Button(label="Admin Roles", style=discord.ButtonStyle.secondary, row=1)
            roles_btn.callback = self._set_admin_roles
            self.add_item(roles_btn)

        elif event["phase"] == "voting":
            end_vote_btn = discord.ui.Button(label="End Voting Early", style=discord.ButtonStyle.danger, row=0)
            end_vote_btn.callback = self._end_voting
            self.add_item(end_vote_btn)

            cancel_btn = discord.ui.Button(label="Cancel Event", style=discord.ButtonStyle.danger, row=0)
            cancel_btn.callback = self._cancel_event
            self.add_item(cancel_btn)

            admin_btn = discord.ui.Button(label="Set Admin Channel", style=discord.ButtonStyle.secondary, row=0)
            admin_btn.callback = self._set_admin_channel
            self.add_item(admin_btn)

            roles_btn = discord.ui.Button(label="Admin Roles", style=discord.ButtonStyle.secondary, row=0)
            roles_btn.callback = self._set_admin_roles
            self.add_item(roles_btn)

    async def _create_event(self, interaction: discord.Interaction):
        gc = self.cog.config.get(self.guild_id, {})
        if gc.get("active_event"):
            await interaction.response.send_message("An event is already active.", ephemeral=True)
            return
        await interaction.response.send_modal(EventNameModal(self.cog))

    async def _set_admin_channel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Set Admin Channel",
            description="Select the channel where event results and notifications will be sent.",
            color=EMBED_COLOR
        )
        await interaction.response.send_message(
            embed=embed, view=AdminChannelSelectView(self.cog), ephemeral=True
        )

    async def _set_admin_roles(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Admin Roles",
            description="Select the roles that can use the media vote panel. This replaces any previously set roles.",
            color=EMBED_COLOR
        )
        await interaction.response.send_message(
            embed=embed, view=AdminRoleSelectView(self.cog), ephemeral=True
        )

    async def _edit_sticky(self, interaction: discord.Interaction):
        gc = self.cog.config.get(self.guild_id, {})
        event = gc.get("active_event")
        current_text = event.get("sticky_text") if event else None
        await interaction.response.send_modal(EditStickyModal(self.cog, current_text))

    async def _end_submissions(self, interaction: discord.Interaction):
        async def do_end():
            await self.cog._transition_to_voting(self.guild_id)

        embed = discord.Embed(
            title="End Submissions Early?",
            description="This will immediately start the voting phase.",
            color=discord.Color.orange()
        )
        await interaction.response.send_message(
            embed=embed, view=ConfirmActionView("End Submissions", do_end), ephemeral=True
        )

    async def _end_voting(self, interaction: discord.Interaction):
        async def do_end():
            await self.cog._end_event(self.guild_id)

        embed = discord.Embed(
            title="End Voting Early?",
            description="This will immediately close voting and reveal results.",
            color=discord.Color.orange()
        )
        await interaction.response.send_message(
            embed=embed, view=ConfirmActionView("End Voting", do_end), ephemeral=True
        )

    async def _cancel_event(self, interaction: discord.Interaction):
        async def do_cancel():
            await self.cog._cancel_event(self.guild_id)

        embed = discord.Embed(
            title="Cancel Event?",
            description="This will delete the sticky and archive the showcase thread. **This cannot be undone.**",
            color=discord.Color.red()
        )
        await interaction.response.send_message(
            embed=embed, view=ConfirmActionView("Cancel Event", do_cancel), ephemeral=True
        )


# ═══════════════════════════════════════════════════════════════════════════
#  COG
# ═══════════════════════════════════════════════════════════════════════════

class MediaVote(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config_manager = ConfigManager(CONFIG_FILE)
        self.config: Dict[str, Any] = {}
        self.config_lock = asyncio.Lock()
        self.sticky_locks: Dict[str, asyncio.Lock] = {}
        self.sticky_timers: Dict[tuple, asyncio.Task] = {}
        self._processing_entries: set = set()  # per-user guard against double-submit

    async def cog_load(self):
        async with self.config_lock:
            self.config = await self.config_manager.load()
        self.phase_check_loop.start()

    def cog_unload(self):
        self.phase_check_loop.cancel()
        for task in self.sticky_timers.values():
            task.cancel()
        self.sticky_timers.clear()

    def _get_guild_config(self, guild_id: str) -> Dict[str, Any]:
        if guild_id not in self.config:
            self.config[guild_id] = {"admin_channel_id": None, "admin_roles": [], "active_event": None}
        return self.config[guild_id]

    async def _save(self):
        await self.config_manager.save(self.config)

    # ─── Panel Embed ─────────────────────────────────────────────────────────
    def _build_panel_embed(self, guild_id: str) -> discord.Embed:
        gc = self.config.get(guild_id, {})
        event = gc.get("active_event")
        admin_ch = gc.get("admin_channel_id")
        admin_text = f"<#{admin_ch}>" if admin_ch else "Not set"
        admin_roles = gc.get("admin_roles", [])
        roles_text = ", ".join(f"<@&{r}>" for r in admin_roles) if admin_roles else "Not set"

        if not event:
            embed = discord.Embed(
                title="Media Vote Panel",
                description="No active event.\n\nUse the buttons below to create a new event.",
                color=EMBED_COLOR
            )
            embed.add_field(name="Admin Channel", value=admin_text, inline=True)
            embed.add_field(name="Admin Roles", value=roles_text, inline=True)
            return embed

        sub_dl = datetime.fromisoformat(event["submission_deadline"])
        vote_dl = datetime.fromisoformat(event["voting_deadline"])
        entry_count = len(event.get("entries", {}))

        if event["phase"] == "submissions":
            desc = (
                f"**Phase:** Submissions\n"
                f"**Channel:** <#{event['channel_id']}>\n"
                f"**Thread:** <#{event['thread_id']}>\n"
                f"**Entries:** {entry_count}\n\n"
                f"**Submissions close:** {discord.utils.format_dt(sub_dl, 'R')} ({discord.utils.format_dt(sub_dl, 'F')})\n"
                f"**Voting ends:** {discord.utils.format_dt(vote_dl, 'R')}"
            )
        else:
            desc = (
                f"**Phase:** Voting\n"
                f"**Channel:** <#{event['channel_id']}>\n"
                f"**Thread:** <#{event['thread_id']}>\n"
                f"**Entries:** {entry_count}\n\n"
                f"**Voting ends:** {discord.utils.format_dt(vote_dl, 'R')} ({discord.utils.format_dt(vote_dl, 'F')})"
            )

        embed = discord.Embed(title=event['display_name'], description=desc, color=EMBED_COLOR)
        embed.add_field(name="Admin Channel", value=admin_text, inline=True)
        embed.add_field(name="Admin Roles", value=roles_text, inline=True)
        return embed

    # ─── Sticky Embed ────────────────────────────────────────────────────────
    def _build_sticky_embed(self, guild_id: str) -> Optional[discord.Embed]:
        gc = self.config.get(guild_id, {})
        event = gc.get("active_event")
        if not event:
            return None

        if event["phase"] == "submissions":
            sub_dl = datetime.fromisoformat(event["submission_deadline"])
            custom_text = event.get("sticky_text")
            body = custom_text or (
                "Submit your entry by attaching an image to a message saying **Entry**."
            )
            return discord.Embed(
                title=event['display_name'],
                description=(
                    f"{body}\n\n"
                    f"Submissions close {discord.utils.format_dt(sub_dl, 'R')} ({discord.utils.format_dt(sub_dl, 'F')})"
                ),
                color=EMBED_COLOR
            )

        # Voting phase
        vote_dl = datetime.fromisoformat(event["voting_deadline"])
        return discord.Embed(
            title=f"{event['display_name']} — Voting Open!",
            description=(
                "Head over to the showcase thread and vote for your favorites!\n"
                "Use the **#1**, **#2**, and **#3** buttons to pick your top 3.\n\n"
                f"<#{event['thread_id']}>\n\n"
                f"Voting closes {discord.utils.format_dt(vote_dl, 'R')} ({discord.utils.format_dt(vote_dl, 'F')})"
            ),
            color=EMBED_COLOR
        )

    # ─── Sticky Send / Delete ────────────────────────────────────────────────
    async def _send_sticky(self, channel: discord.TextChannel, guild_id: str):
        if guild_id not in self.sticky_locks:
            self.sticky_locks[guild_id] = asyncio.Lock()

        try:
            perms = channel.permissions_for(channel.guild.me)
        except AttributeError:
            return
        if not perms.send_messages or not perms.embed_links:
            logger.warning(f"Missing permissions in {channel.name}")
            return

        async with self.sticky_locks[guild_id]:
            gc = self.config.get(guild_id, {})
            event = gc.get("active_event")
            if not event:
                return

            old_id = event.get("sticky_message_id")

            # Delete old sticky
            if old_id:
                try:
                    old_msg = await asyncio.wait_for(channel.fetch_message(old_id), timeout=5.0)
                    await old_msg.delete()
                except discord.NotFound:
                    pass
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout fetching old sticky in {channel.name}")
                except (discord.Forbidden, discord.HTTPException) as e:
                    logger.debug(f"Could not delete old sticky: {e}")

            # Send new sticky
            embed = self._build_sticky_embed(guild_id)
            if not embed:
                return

            try:
                msg = await asyncio.wait_for(channel.send(embed=embed), timeout=10.0)
                async with self.config_lock:
                    event["sticky_message_id"] = msg.id
                    await self._save()
            except asyncio.TimeoutError:
                logger.error(f"Timeout sending sticky to {channel.name}")
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.error(f"Error sending sticky: {e}")

    async def _delayed_sticky_send(self, channel: discord.TextChannel, guild_id: str, timer_key: tuple):
        try:
            await asyncio.sleep(1.0)
            if timer_key not in self.sticky_timers:
                return
            if self.sticky_timers[timer_key] != asyncio.current_task():
                return
            await self._send_sticky(channel, guild_id)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in sticky delay: {e}")
        finally:
            if timer_key in self.sticky_timers and self.sticky_timers[timer_key] == asyncio.current_task():
                del self.sticky_timers[timer_key]

    # ─── Entry Handler ───────────────────────────────────────────────────────
    async def _handle_entry(self, message: discord.Message, guild_id: str, event: dict):
        images = [a for a in message.attachments if a.content_type and a.content_type.startswith("image/")]
        channel = message.channel

        if not images:
            try:
                await message.delete()
            except (discord.NotFound, discord.Forbidden):
                pass
            try:
                await channel.send("Please attach an image with your entry.", delete_after=5)
            except (discord.Forbidden, discord.HTTPException):
                pass
            return

        attachment = images[0]
        user_id = str(message.author.id)
        display_name = message.author.display_name

        # Bug 3: Per-user guard against concurrent double-submission
        if user_id in self._processing_entries:
            try:
                await message.delete()
            except (discord.NotFound, discord.Forbidden):
                pass
            return
        self._processing_entries.add(user_id)

        try:
            # Download image BEFORE deleting message
            try:
                image_data = await attachment.read()
            except (discord.HTTPException, discord.NotFound):
                try:
                    await message.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
                try:
                    await channel.send("Failed to process your entry. Please try again.", delete_after=5)
                except (discord.Forbidden, discord.HTTPException):
                    pass
                return

            # Bug 8: Re-check phase after download (could have changed during await)
            gc = self.config.get(guild_id, {})
            current_event = gc.get("active_event")
            if not current_event or current_event["phase"] != "submissions":
                try:
                    await message.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
                return

            # Delete original for anonymity
            try:
                await message.delete()
                logger.info(f"Deleted entry message {message.id} from #{channel.name}")
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                logger.warning(f"Could not delete entry message {message.id} in #{channel.name}: {e}")

            # Get thread
            thread = self.bot.get_channel(event["thread_id"])
            if not thread:
                logger.warning(f"Showcase thread not found for guild {guild_id}")
                try:
                    await channel.send("Something went wrong. Please contact an admin.", delete_after=5)
                except (discord.Forbidden, discord.HTTPException):
                    pass
                return

            # Bug 7: Unarchive thread if auto-archived
            if hasattr(thread, 'archived') and thread.archived:
                try:
                    await thread.edit(archived=False)
                except (discord.Forbidden, discord.HTTPException):
                    logger.warning(f"Could not unarchive thread for entry in guild {guild_id}")
                    try:
                        await channel.send("Something went wrong. Please contact an admin.", delete_after=5)
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                    return

            # Handle re-submission: delete old entry
            is_resubmission = False
            old_entry = event.get("entries", {}).get(user_id)
            if old_entry:
                is_resubmission = True
                try:
                    old_msg = await thread.fetch_message(old_entry["thread_message_id"])
                    await old_msg.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass

            # Bug 2: Wrap thread.send in try/except — image data would be lost on failure
            filename = attachment.filename or "entry.png"
            file = discord.File(io.BytesIO(image_data), filename=filename)
            view = SubmissionVoteView(user_id)
            try:
                thread_msg = await thread.send(file=file, view=view)
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.error(f"Failed to post entry to thread: {e}")
                try:
                    await channel.send("Failed to post your entry. Please try again.", delete_after=5)
                except (discord.Forbidden, discord.HTTPException):
                    pass
                return

            # Save entry
            async with self.config_lock:
                event.setdefault("entries", {})[user_id] = {
                    "thread_message_id": thread_msg.id,
                    "username": display_name,
                    "submitted_at": datetime.now(timezone.utc).isoformat()
                }
                await self._save()

            # Bug 6: Wrap confirmation send in try/except
            try:
                confirm_text = "Entry received — your previous submission has been replaced." if is_resubmission else "Entry received."
                await channel.send(confirm_text, delete_after=5)
            except (discord.Forbidden, discord.HTTPException):
                pass
        finally:
            self._processing_entries.discard(user_id)

    # ─── Phase Transitions ───────────────────────────────────────────────────
    async def _transition_to_voting(self, guild_id: str):
        async with self.config_lock:
            gc = self._get_guild_config(guild_id)
            event = gc.get("active_event")
            if not event or event["phase"] != "submissions":
                return
            event["phase"] = "voting"
            await self._save()

        channel = self.bot.get_channel(event["channel_id"])
        if channel:
            await self._send_sticky(channel, guild_id)

        logger.info(f"Event '{event['display_name']}' transitioned to voting in guild {guild_id}")

    async def _end_event(self, guild_id: str):
        # Atomically claim the event to prevent double-processing
        async with self.config_lock:
            gc = self.config.get(guild_id, {})
            event = gc.get("active_event")
            if not event:
                return
            event_data = copy.deepcopy(event)
            gc["active_event"] = None
            await self._save()

        thread = self.bot.get_channel(event_data["thread_id"])
        entries = event_data.get("entries", {})
        votes = event_data.get("votes", {})

        # Tally votes per submission
        vote_counts = {uid: {"1": 0, "2": 0, "3": 0, "score": 0} for uid in entries}
        for voter_votes in votes.values():
            for rank_str, submitter_id in voter_votes.items():
                if submitter_id in vote_counts:
                    vote_counts[submitter_id][rank_str] += 1
                    vote_counts[submitter_id]["score"] += RANK_VALUES.get(rank_str, 0)

        if thread:
            # Ensure thread is not archived
            if hasattr(thread, 'archived') and thread.archived:
                try:
                    await thread.edit(archived=False)
                except (discord.Forbidden, discord.HTTPException):
                    pass

            # Reveal authors and remove vote buttons
            for user_id, entry in entries.items():
                try:
                    msg = await thread.fetch_message(entry["thread_message_id"])
                    await msg.edit(content=f"Entry by **{entry['username']}**", view=None)
                    await asyncio.sleep(0.5)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    logger.warning(f"Could not reveal entry for {entry.get('username', user_id)}: {e}")

            # Lock and archive thread
            try:
                await thread.edit(locked=True, archived=True)
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.warning(f"Could not lock thread: {e}")

        # Sort by score descending
        sorted_entries = sorted(
            entries.items(),
            key=lambda x: vote_counts.get(x[0], {}).get("score", 0),
            reverse=True
        )

        # Build results embed with hyperlinks
        thread_id = event_data["thread_id"]
        lines = []
        for i, (uid, entry) in enumerate(sorted_entries):
            vc = vote_counts.get(uid, {"1": 0, "2": 0, "3": 0})
            jump_url = f"https://discord.com/channels/{guild_id}/{thread_id}/{entry['thread_message_id']}"
            lines.append(
                f"{i + 1}. [{entry['username']}]({jump_url}) — "
                f"🥇 ×{vc['1']}  🥈 ×{vc['2']}  🥉 ×{vc['3']}"
            )

        results_embed = discord.Embed(
            title=f"🏆 Results — {event_data['display_name']}",
            description="\n".join(lines) if lines else "No entries were submitted.",
            color=RESULTS_COLOR
        )
        if sorted_entries:
            total_voters = len(votes)
            results_embed.set_footer(text=f"Total entries: {len(sorted_entries)} · Total voters: {total_voters}")

        # Send to admin channel
        admin_ch_id = gc.get("admin_channel_id")
        if admin_ch_id:
            admin_ch = self.bot.get_channel(admin_ch_id)
            if admin_ch:
                try:
                    await admin_ch.send(embed=results_embed)
                except (discord.Forbidden, discord.HTTPException) as e:
                    logger.warning(f"Could not send results to admin channel: {e}")
            else:
                logger.warning(f"Admin channel {admin_ch_id} not found in guild {guild_id}")
        else:
            logger.warning(f"No admin channel set for guild {guild_id}")

        # Replace sticky with reveal message (auto-deletes after 24h)
        channel = self.bot.get_channel(event_data["channel_id"])
        if channel:
            old_sticky = event_data.get("sticky_message_id")
            if old_sticky:
                try:
                    msg = await channel.fetch_message(old_sticky)
                    await msg.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

            reveal_embed = discord.Embed(
                title=f"{event_data['display_name']} — Authors Revealed",
                description=(
                    "Submission authors have been revealed, check out who made what!\n\n"
                    f"<#{event_data['thread_id']}>"
                ),
                color=EMBED_COLOR
            )
            try:
                await channel.send(embed=reveal_embed, delete_after=86400)
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.warning(f"Could not send reveal message: {e}")

        logger.info(f"Event '{event_data['display_name']}' ended in guild {guild_id}")

    async def _cancel_event(self, guild_id: str):
        # Atomically claim the event
        async with self.config_lock:
            gc = self.config.get(guild_id, {})
            event = gc.get("active_event")
            if not event:
                return
            event_data = copy.deepcopy(event)
            gc["active_event"] = None
            await self._save()

        # Delete sticky
        channel = self.bot.get_channel(event_data["channel_id"])
        if channel:
            old_sticky = event_data.get("sticky_message_id")
            if old_sticky:
                try:
                    msg = await channel.fetch_message(old_sticky)
                    await msg.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

        # Archive thread
        thread = self.bot.get_channel(event_data["thread_id"])
        if thread:
            try:
                await thread.edit(locked=True, archived=True)
            except (discord.Forbidden, discord.HTTPException):
                pass

        logger.info(f"Event '{event_data['display_name']}' cancelled in guild {guild_id}")

    # ─── Slash Command ───────────────────────────────────────────────────────
    def _has_admin_access(self, member: discord.Member, guild_id: str) -> bool:
        if member.guild_permissions.manage_guild:
            return True
        gc = self.config.get(guild_id, {})
        admin_roles = gc.get("admin_roles", [])
        if not admin_roles:
            return False
        member_role_ids = {r.id for r in member.roles}
        return bool(member_role_ids & set(admin_roles))

    @app_commands.command(name="mediavote_panel", description="Open the media vote admin panel")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.guild_only()
    async def mediavote_panel(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild_id)

        if not self._has_admin_access(interaction.user, guild_id):
            await interaction.response.send_message("You don't have permission to use this.", ephemeral=True)
            return

        gc = self.config.get(guild_id, {})
        event = gc.get("active_event")

        embed = self._build_panel_embed(guild_id)
        view = MediaVotePanelView(self, guild_id, event)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    # ─── Message Listener ────────────────────────────────────────────────────
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.id == self.bot.user.id or not message.guild:
            return

        guild_id = str(message.guild.id)
        gc = self.config.get(guild_id)
        if not gc:
            return
        event = gc.get("active_event")
        if not event or message.channel.id != event.get("channel_id"):
            return

        # Don't re-stick on the sticky itself
        if message.id == event.get("sticky_message_id"):
            return

        # Handle entry submission
        if (event["phase"] == "submissions"
                and message.content
                and re.search(r'\bentry\b', message.content, re.IGNORECASE)):
            await self._handle_entry(message, guild_id, event)

        # Re-stick (debounced)
        timer_key = (message.channel.id, guild_id)
        if timer_key in self.sticky_timers:
            self.sticky_timers[timer_key].cancel()
        task = asyncio.create_task(self._delayed_sticky_send(message.channel, guild_id, timer_key))
        self.sticky_timers[timer_key] = task

    # ─── Vote Interaction Handler ────────────────────────────────────────────
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get('custom_id', '')
        if not custom_id.startswith('mv:'):
            return

        parts = custom_id.split(':')
        if len(parts) != 3:
            return

        _, rank_str, submitter_id = parts
        if rank_str not in ('1', '2', '3'):
            return

        guild_id = str(interaction.guild_id) if interaction.guild_id else None
        if not guild_id:
            return

        gc = self.config.get(guild_id, {})
        event = gc.get("active_event")
        if not event or event["phase"] != "voting":
            await interaction.response.send_message("Voting is not currently active.", ephemeral=True)
            return

        # Verify the submitter is a valid entry
        if submitter_id not in event.get("entries", {}):
            await interaction.response.send_message("This submission is no longer valid.", ephemeral=True)
            return

        voter_id = str(interaction.user.id)
        submitter_name = event["entries"][submitter_id]["username"]

        already_voted = False
        old_rank = None

        async with self.config_lock:
            votes = event.setdefault("votes", {})
            voter_votes = votes.setdefault(voter_id, {})

            # Already voted this rank for this submission?
            if voter_votes.get(rank_str) == submitter_id:
                already_voted = True
            else:
                # If this submission already has a different rank from this voter, remove it
                for r, sid in list(voter_votes.items()):
                    if sid == submitter_id:
                        old_rank = r
                        del voter_votes[r]
                        break

                # Set the new vote (overwrites any previous submission at this rank)
                voter_votes[rank_str] = submitter_id
                await self._save()

        if already_voted:
            await interaction.response.send_message(
                f"You've already placed your **#{rank_str}** vote here.",
                ephemeral=True
            )
            return

        # Build response
        rank_label = RANK_LABELS[rank_str]
        if old_rank:
            old_label = RANK_LABELS[old_rank]
            msg = f"Your vote for **{submitter_name}** moved from **{old_label}** to **{rank_label}**."
        else:
            msg = f"Your **{rank_label}** vote has been recorded."

        await interaction.response.send_message(msg, ephemeral=True)

    # ─── Background Task ─────────────────────────────────────────────────────
    @tasks.loop(seconds=30)
    async def phase_check_loop(self):
        try:
            now = datetime.now(timezone.utc)

            for guild_id, gc in list(self.config.items()):
                event = gc.get("active_event")
                if not event:
                    continue

                # Keep thread from auto-archiving
                thread = self.bot.get_channel(event.get("thread_id", 0))
                if thread and hasattr(thread, 'archived') and thread.archived:
                    try:
                        await thread.edit(archived=False)
                    except (discord.Forbidden, discord.HTTPException, aiohttp.ClientError):
                        pass

                if event["phase"] == "submissions":
                    deadline = datetime.fromisoformat(event["submission_deadline"])
                    if now >= deadline:
                        await self._transition_to_voting(guild_id)

                elif event["phase"] == "voting":
                    deadline = datetime.fromisoformat(event["voting_deadline"])
                    if now >= deadline:
                        await self._end_event(guild_id)
        except Exception as e:
            await self.bot.error_reporter.report("MediaVote", f"phase_check_loop: {e}")

    @phase_check_loop.before_loop
    async def before_phase_check(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(MediaVote(bot))
