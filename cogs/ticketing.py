import discord
from discord.ext import commands
from discord import app_commands
import json, os, asyncio, re, logging
from typing import Dict, Any, List, Optional, TYPE_CHECKING, Union, Tuple
from datetime import datetime, timezone, timedelta
import io

logger = logging.getLogger("ticketing_cog")

# The user must install this library for survey exports: pip install openpyxl
try:
    import openpyxl
except ImportError:
    openpyxl = None


if TYPE_CHECKING:
    from discord.ext.commands import Bot

# Use separate files for better organization
DATA_DIR = "data"
TOPICS_FILE = os.path.join(DATA_DIR, "topics.json")
PANELS_FILE = os.path.join(DATA_DIR, "panels.json")
SURVEY_DATA_FILE = os.path.join(DATA_DIR, "survey_data.json")
SURVEY_SESSIONS_FILE = os.path.join(DATA_DIR, "survey_sessions.json")

# Default cooldowns (in minutes)
DEFAULT_SURVEY_COOLDOWN_MINUTES = 5  # 5 minutes between submissions of the same survey

# ---------- Data Sanitization ----------
def _sanitize_for_json(data: Union[Dict, List]) -> Union[Dict, List]:
    if isinstance(data, dict):
        return {key: _sanitize_for_json(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_sanitize_for_json(item) for item in data]
    elif hasattr(data, 'id'):
        return data.id
    else:
        return data

# ---------- Storage Layer ----------
async def _load_json(bot: "Bot", file_path: str, lock: asyncio.Lock) -> Dict[str, Any]:
    def sync_load():
        if not os.path.exists(file_path):
            return {}
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                return json.loads(content) if content else {}
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    async with lock:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, sync_load)

async def _save_json(bot: "Bot", file_path: str, data: Dict[str, Any], lock: asyncio.Lock):
    """Save JSON with atomic write to prevent corruption."""
    def sync_save():
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        sanitized_data = _sanitize_for_json(data)
        # Atomic write: write to temp file, then rename
        temp_path = file_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(sanitized_data, f, indent=4)
        os.replace(temp_path, file_path)  # Atomic on most systems

    async with lock:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, sync_save)

# ---------- Default Data Helpers ----------
def _ensure_topic_defaults(t: Dict[str, Any]) -> Dict[str, Any]:
    t.setdefault("name", "new-topic")
    t.setdefault("label", "New Topic")
    t.setdefault("emoji", None)
    t.setdefault("type", "ticket")
    t.setdefault("mode", "thread")
    t.setdefault("parent_id", None)
    t.setdefault("staff_role_ids", [])
    t.setdefault("log_channel_id", None)
    t.setdefault("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
    t.setdefault("questions", [])
    t.setdefault("approval_mode", False)
    t.setdefault("discussion_mode", False)
    t.setdefault("application_channel_mode", "dm")
    t.setdefault("button_color", "secondary")
    # Pre-modal question feature
    t.setdefault("pre_modal_enabled", False)
    t.setdefault("pre_modal_question", "Do you have your profile link ready?")
    t.setdefault("pre_modal_redirect_url", None)
    t.setdefault("pre_modal_redirect_channel_id", None)  # Channel redirect as alternative to URL
    # Pre-modal button labels
    t.setdefault("pre_modal_yes_label", "Yes, I have it")
    t.setdefault("pre_modal_no_label", "No, I need to get it")
    # Pre-modal no-click behavior
    t.setdefault("pre_modal_no_message", "Please get what you need ready, then click the button below to continue.")
    t.setdefault("pre_modal_ready_button_enabled", True)
    t.setdefault("pre_modal_ready_button_label", "I'm ready now")
    # Pre-modal required answer feature
    t.setdefault("pre_modal_answer_enabled", False)
    t.setdefault("pre_modal_answer_question", "Please provide your information:")
    # Second pre-modal question
    t.setdefault("pre_modal_2_enabled", False)
    t.setdefault("pre_modal_2_question", "Do you have your second requirement ready?")
    t.setdefault("pre_modal_2_redirect_url", None)
    t.setdefault("pre_modal_2_redirect_channel_id", None)
    t.setdefault("pre_modal_2_yes_label", "Yes")
    t.setdefault("pre_modal_2_no_label", "No")
    t.setdefault("pre_modal_2_no_message", "Please get what you need ready, then click the button below to continue.")
    t.setdefault("pre_modal_2_ready_button_enabled", True)
    t.setdefault("pre_modal_2_ready_button_label", "I'm ready now")
    # Survey-specific settings
    t.setdefault("survey_cooldown_minutes", DEFAULT_SURVEY_COOLDOWN_MINUTES)
    # Staff notification settings
    t.setdefault("ping_staff_on_create", False)  # False = silent add, True = ping roles
    # Ticket close settings
    t.setdefault("delete_on_close", True)  # True = delete, False = archive
    t.setdefault("member_can_close", True)  # True = opener can close, False = only staff
    # Claim system settings
    t.setdefault("claim_enabled", False)  # Enable claim alerts for tickets/applications
    t.setdefault("claim_alerts_channel_id", None)  # Channel to send claim alerts
    t.setdefault("claim_role_id", None)  # Role that can join via claim button
    return t

def _ensure_panel_defaults(p: Dict[str, Any]) -> Dict[str, Any]:
    p.setdefault("name", "new-panel")
    p.setdefault("channel_id", None)
    p.setdefault("title", "Support Panel")
    p.setdefault("description", "Please select an option below.")
    p.setdefault("display_mode", "buttons")
    p.setdefault("message_id", None)
    p.setdefault("topic_names", [])
    p.setdefault("image_url", None)
    p.setdefault("image_type", "banner")
    return p

# ---------- Pre-Modal Question Views ----------
class PreModalAnswerModal(discord.ui.Modal):
    """Modal for collecting required answer before creating ticket or starting application."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any], question: str):
        super().__init__(title="Required Information", timeout=300)
        self.cog = cog
        self.topic = topic
        self.answer_input = discord.ui.TextInput(
            label=question[:45],  # Discord limit for label
            placeholder="Enter your answer here...",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )
        self.add_item(self.answer_input)

    async def on_submit(self, interaction: discord.Interaction):
        topic_type = self.topic.get('type', 'ticket')
        user_answer = self.answer_input.value

        if topic_type == 'application':
            channel_mode = self.topic.get('application_channel_mode', 'dm') == 'channel'
            if not channel_mode:
                await interaction.response.send_message("The application is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            self.topic['_pre_modal_user_answer'] = user_answer
            asyncio.create_task(self.cog.conduct_survey_flow(interaction, self.topic))
        else:
            # For tickets, create the discussion channel
            await interaction.response.defer(ephemeral=True, thinking=True)
            ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True, user_answer=user_answer)
            if ch:
                self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
                await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)
            else:
                await interaction.followup.send("Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)

class PreModalCheckView(discord.ui.View):
    """View shown before opening a modal to ask if user has required info ready."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any], interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.cog = cog
        self.topic = topic
        self.original_interaction = interaction

        # Create buttons with custom labels from topic data
        yes_label = topic.get('pre_modal_yes_label', 'Yes, I have it')
        no_label = topic.get('pre_modal_no_label', 'No, I need to get it')

        yes_button = discord.ui.Button(label=yes_label, style=discord.ButtonStyle.success)
        yes_button.callback = self.yes_button_callback
        self.add_item(yes_button)

        no_button = discord.ui.Button(label=no_label, style=discord.ButtonStyle.secondary)
        no_button.callback = self.no_button_callback
        self.add_item(no_button)

    async def yes_button_callback(self, interaction: discord.Interaction):
        # Check if second pre-question is enabled
        if self.topic.get('pre_modal_2_enabled', False):
            pre_question_2 = self.topic.get('pre_modal_2_question', 'Do you have your second requirement ready?')
            await interaction.response.edit_message(content=pre_question_2, view=PreModalCheckView2(self.cog, self.topic, interaction))
            self.stop()
            return

        await self._proceed_after_prequestion(interaction)
        self.stop()

    async def _proceed_after_prequestion(self, interaction: discord.Interaction):
        """Common logic to proceed after all pre-questions are answered Yes."""
        topic_type = self.topic.get('type', 'ticket')

        # Check if required answer is enabled
        if self.topic.get('pre_modal_answer_enabled', False):
            question = self.topic.get('pre_modal_answer_question', 'Please provide your information:')
            await interaction.response.send_modal(PreModalAnswerModal(self.cog, self.topic, question))
        elif topic_type == 'application':
            channel_mode = self.topic.get('application_channel_mode', 'dm') == 'channel'
            if not channel_mode:
                await interaction.response.send_message("The application is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            asyncio.create_task(self.cog.conduct_survey_flow(interaction, self.topic))
        else:
            # User has the info ready, proceed to create ticket
            await interaction.response.defer(ephemeral=True, thinking=True)
            ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
            if ch:
                self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
                await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)
            else:
                await interaction.followup.send("Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)

    async def no_button_callback(self, interaction: discord.Interaction):
        redirect_url = self.topic.get("pre_modal_redirect_url")
        redirect_channel_id = self.topic.get("pre_modal_redirect_channel_id")
        no_message = self.topic.get("pre_modal_no_message", "Please get what you need ready, then click the button below to continue.")
        ready_button_enabled = self.topic.get("pre_modal_ready_button_enabled", True)

        # Build the message description
        if redirect_channel_id:
            description = f"Please check out <#{redirect_channel_id}> for more information.\n\n{no_message}"
        elif redirect_url:
            description = f"[Click here to get what you need]({redirect_url})\n\n{no_message}"
        else:
            description = no_message

        embed = discord.Embed(description=description, color=discord.Color.blue())

        # Only show ready button view if enabled
        if ready_button_enabled:
            await interaction.response.edit_message(embed=embed, view=PreModalReadyView(self.cog, self.topic))
        else:
            await interaction.response.edit_message(embed=embed, view=None)
        self.stop()

class PreModalReadyView(discord.ui.View):
    """View shown after user says they need to get the info, with a 'Ready now' button."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any]):
        super().__init__(timeout=300)
        self.cog = cog
        self.topic = topic

        # Create button with custom label from topic data
        ready_label = topic.get('pre_modal_ready_button_label', "I'm ready now")
        ready_button = discord.ui.Button(label=ready_label, style=discord.ButtonStyle.success)
        ready_button.callback = self.ready_button_callback
        self.add_item(ready_button)

    async def ready_button_callback(self, interaction: discord.Interaction):
        # Check if second pre-question is enabled
        if self.topic.get('pre_modal_2_enabled', False):
            pre_question_2 = self.topic.get('pre_modal_2_question', 'Do you have your second requirement ready?')
            await interaction.response.edit_message(content=pre_question_2, embed=None, view=PreModalCheckView2(self.cog, self.topic, interaction))
            self.stop()
            return

        await self._proceed_after_prequestion(interaction)
        self.stop()

    async def _proceed_after_prequestion(self, interaction: discord.Interaction):
        """Common logic to proceed after all pre-questions are answered Yes."""
        topic_type = self.topic.get('type', 'ticket')

        # Check if required answer is enabled
        if self.topic.get('pre_modal_answer_enabled', False):
            question = self.topic.get('pre_modal_answer_question', 'Please provide your information:')
            await interaction.response.send_modal(PreModalAnswerModal(self.cog, self.topic, question))
        elif topic_type == 'application':
            channel_mode = self.topic.get('application_channel_mode', 'dm') == 'channel'
            if not channel_mode:
                await interaction.response.send_message("The application is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            asyncio.create_task(self.cog.conduct_survey_flow(interaction, self.topic))
        else:
            await interaction.response.defer(ephemeral=True, thinking=True)
            ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
            if ch:
                self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
                await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)
            else:
                await interaction.followup.send("Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)

class PreModalCheckView2(discord.ui.View):
    """View for the second pre-modal question."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any], interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.cog = cog
        self.topic = topic
        self.original_interaction = interaction

        yes_label = topic.get('pre_modal_2_yes_label', 'Yes')
        no_label = topic.get('pre_modal_2_no_label', 'No')

        yes_button = discord.ui.Button(label=yes_label, style=discord.ButtonStyle.success)
        yes_button.callback = self.yes_button_callback
        self.add_item(yes_button)

        no_button = discord.ui.Button(label=no_label, style=discord.ButtonStyle.secondary)
        no_button.callback = self.no_button_callback
        self.add_item(no_button)

    async def yes_button_callback(self, interaction: discord.Interaction):
        await self._proceed_after_prequestion(interaction)
        self.stop()

    async def _proceed_after_prequestion(self, interaction: discord.Interaction):
        """Common logic to proceed after all pre-questions are answered Yes."""
        topic_type = self.topic.get('type', 'ticket')

        if self.topic.get('pre_modal_answer_enabled', False):
            question = self.topic.get('pre_modal_answer_question', 'Please provide your information:')
            await interaction.response.send_modal(PreModalAnswerModal(self.cog, self.topic, question))
        elif topic_type == 'application':
            channel_mode = self.topic.get('application_channel_mode', 'dm') == 'channel'
            if not channel_mode:
                await interaction.response.send_message("The application is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            asyncio.create_task(self.cog.conduct_survey_flow(interaction, self.topic))
        else:
            await interaction.response.defer(ephemeral=True, thinking=True)
            ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
            if ch:
                self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
                await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)
            else:
                await interaction.followup.send("Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)

    async def no_button_callback(self, interaction: discord.Interaction):
        redirect_url = self.topic.get("pre_modal_2_redirect_url")
        redirect_channel_id = self.topic.get("pre_modal_2_redirect_channel_id")
        no_message = self.topic.get("pre_modal_2_no_message", "Please get what you need ready, then click the button below to continue.")

        if redirect_channel_id:
            description = f"Please check out <#{redirect_channel_id}> for more information.\n\n{no_message}"
        elif redirect_url:
            description = f"[Click here to get what you need]({redirect_url})\n\n{no_message}"
        else:
            description = no_message

        embed = discord.Embed(description=description, color=discord.Color.blue())
        await interaction.response.edit_message(content=None, embed=embed, view=PreModalReadyView2(self.cog, self.topic))
        self.stop()

class PreModalReadyView2(discord.ui.View):
    """View shown after user says No on second pre-question."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any]):
        super().__init__(timeout=300)
        self.cog = cog
        self.topic = topic

        # Only add ready button if enabled
        ready_button_enabled = topic.get('pre_modal_2_ready_button_enabled', True)
        if ready_button_enabled:
            ready_label = topic.get('pre_modal_2_ready_button_label', "I'm ready now")
            ready_button = discord.ui.Button(label=ready_label, style=discord.ButtonStyle.success)
            ready_button.callback = self.ready_button_callback
            self.add_item(ready_button)

    async def ready_button_callback(self, interaction: discord.Interaction):
        await self._proceed_after_prequestion(interaction)
        self.stop()

    async def _proceed_after_prequestion(self, interaction: discord.Interaction):
        """Common logic to proceed after all pre-questions are answered Yes."""
        topic_type = self.topic.get('type', 'ticket')

        if self.topic.get('pre_modal_answer_enabled', False):
            question = self.topic.get('pre_modal_answer_question', 'Please provide your information:')
            await interaction.response.send_modal(PreModalAnswerModal(self.cog, self.topic, question))
        elif topic_type == 'application':
            channel_mode = self.topic.get('application_channel_mode', 'dm') == 'channel'
            if not channel_mode:
                await interaction.response.send_message("The application is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            asyncio.create_task(self.cog.conduct_survey_flow(interaction, self.topic))
        else:
            await interaction.response.defer(ephemeral=True, thinking=True)
            ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
            if ch:
                self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
                await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)
            else:
                await interaction.followup.send("Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)

# ---------- Pre-Modal Configuration Modals ----------
class PreModalConfigModal(discord.ui.Modal, title="Configure Pre-Question"):
    question = discord.ui.TextInput(
        label="Question to ask",
        placeholder="e.g., Do you have your profile link ready?",
        max_length=200,
        required=True
    )
    redirect_url = discord.ui.TextInput(
        label="Redirect URL (if they say No)",
        placeholder="https://example.com/get-your-link",
        required=False,
        max_length=500
    )
    yes_label = discord.ui.TextInput(
        label="Yes button label",
        placeholder="e.g., Yes, I have it",
        max_length=80,
        required=True
    )
    no_label = discord.ui.TextInput(
        label="No button label",
        placeholder="e.g., No, I need to get it",
        max_length=80,
        required=True
    )

    def __init__(self, view: "TopicWizardView"):
        super().__init__()
        self.view = view
        self.question.default = view.topic_data.get("pre_modal_question", "Do you have your profile link ready?")
        self.redirect_url.default = view.topic_data.get("pre_modal_redirect_url") or ""
        self.yes_label.default = view.topic_data.get("pre_modal_yes_label", "Yes, I have it")
        self.no_label.default = view.topic_data.get("pre_modal_no_label", "No, I need to get it")

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data["pre_modal_question"] = self.question.value
        self.view.topic_data["pre_modal_redirect_url"] = self.redirect_url.value.strip() or None
        self.view.topic_data["pre_modal_yes_label"] = self.yes_label.value.strip()
        self.view.topic_data["pre_modal_no_label"] = self.no_label.value.strip()
        await self.view.update_message_state(itx, "✅ Pre-question configured.")


class PreModalNoResponseConfigModal(discord.ui.Modal, title="Configure No Response"):
    no_message = discord.ui.TextInput(
        label="Message when user clicks No",
        placeholder="e.g., Please get what you need ready...",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True
    )
    ready_button_label = discord.ui.TextInput(
        label="Ready button label (leave blank to disable)",
        placeholder="e.g., I'm ready now (leave empty to hide button)",
        max_length=80,
        required=False
    )

    def __init__(self, view: "TopicWizardView"):
        super().__init__()
        self.view = view
        self.no_message.default = view.topic_data.get("pre_modal_no_message", "Please get what you need ready, then click the button below to continue.")
        # If ready button is disabled, show empty; otherwise show the label
        if view.topic_data.get("pre_modal_ready_button_enabled", True):
            self.ready_button_label.default = view.topic_data.get("pre_modal_ready_button_label", "I'm ready now")
        else:
            self.ready_button_label.default = ""

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data["pre_modal_no_message"] = self.no_message.value.strip()
        ready_label = self.ready_button_label.value.strip()
        if ready_label:
            self.view.topic_data["pre_modal_ready_button_enabled"] = True
            self.view.topic_data["pre_modal_ready_button_label"] = ready_label
        else:
            self.view.topic_data["pre_modal_ready_button_enabled"] = False
        await self.view.update_message_state(itx, "✅ No response configured.")


class PreModal2NoResponseConfigModal(discord.ui.Modal, title="Configure Q2 No Response"):
    no_message = discord.ui.TextInput(
        label="Message when user clicks No",
        placeholder="e.g., Please get what you need ready...",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True
    )
    ready_button_label = discord.ui.TextInput(
        label="Ready button label (leave blank to disable)",
        placeholder="e.g., I'm ready now (leave empty to hide button)",
        max_length=80,
        required=False
    )

    def __init__(self, view: "TopicWizardView"):
        super().__init__()
        self.view = view
        self.no_message.default = view.topic_data.get("pre_modal_2_no_message", "Please get what you need ready, then click the button below to continue.")
        # If ready button is disabled, show empty; otherwise show the label
        if view.topic_data.get("pre_modal_2_ready_button_enabled", True):
            self.ready_button_label.default = view.topic_data.get("pre_modal_2_ready_button_label", "I'm ready now")
        else:
            self.ready_button_label.default = ""

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data["pre_modal_2_no_message"] = self.no_message.value.strip()
        ready_label = self.ready_button_label.value.strip()
        if ready_label:
            self.view.topic_data["pre_modal_2_ready_button_enabled"] = True
            self.view.topic_data["pre_modal_2_ready_button_label"] = ready_label
        else:
            self.view.topic_data["pre_modal_2_ready_button_enabled"] = False
        await self.view.update_message_state(itx, "✅ Q2 no response configured.")


class PreModalAnswerConfigModal(discord.ui.Modal, title="Configure Required Answer"):
    question = discord.ui.TextInput(
        label="Question for required answer",
        placeholder="e.g., What is your in-game username?",
        max_length=200,
        required=True
    )

    def __init__(self, view: "TopicWizardView"):
        super().__init__()
        self.view = view
        self.question.default = view.topic_data.get("pre_modal_answer_question", "Please provide your information:")

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data["pre_modal_answer_question"] = self.question.value
        await self.view.update_message_state(itx, "✅ Required answer question configured.")

class PreModalConfig2Modal(discord.ui.Modal, title="Configure Pre-Question 2"):
    question = discord.ui.TextInput(
        label="Second question to ask",
        placeholder="e.g., Do you have your second requirement ready?",
        max_length=200,
        required=True
    )
    redirect_url = discord.ui.TextInput(
        label="Redirect URL (if they say No)",
        placeholder="https://example.com/get-your-link",
        required=False,
        max_length=500
    )
    yes_label = discord.ui.TextInput(
        label="Yes button label",
        placeholder="e.g., Yes",
        max_length=80,
        required=True
    )
    no_label = discord.ui.TextInput(
        label="No button label",
        placeholder="e.g., No",
        max_length=80,
        required=True
    )

    def __init__(self, view: "TopicWizardView"):
        super().__init__()
        self.view = view
        self.question.default = view.topic_data.get("pre_modal_2_question", "Do you have your second requirement ready?")
        self.redirect_url.default = view.topic_data.get("pre_modal_2_redirect_url") or ""
        self.yes_label.default = view.topic_data.get("pre_modal_2_yes_label", "Yes")
        self.no_label.default = view.topic_data.get("pre_modal_2_no_label", "No")

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data["pre_modal_2_question"] = self.question.value
        self.view.topic_data["pre_modal_2_redirect_url"] = self.redirect_url.value.strip() or None
        self.view.topic_data["pre_modal_2_yes_label"] = self.yes_label.value.strip()
        self.view.topic_data["pre_modal_2_no_label"] = self.no_label.value.strip()
        await self.view.update_message_state(itx, "✅ Pre-question 2 configured.")

# ---------- WIZARD: Topic Creation/Editing ----------
class TopicWizardView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", interaction: discord.Interaction, topic_data: Dict[str, Any], is_new: bool):
        super().__init__(timeout=600)
        self.cog = cog
        self.original_interaction = interaction
        self.topic_data = topic_data
        self.is_new = is_new
        self.update_components()

    async def update_message_state(self, interaction: discord.Interaction, ephemeral_content: Optional[str] = None):
        """Centralized method to update the wizard's state. Prevents 'Interaction Failed' errors."""
        if interaction.response.is_done():
            if ephemeral_content:
                await interaction.followup.send(content=ephemeral_content, ephemeral=True, delete_after=5)
        else:
            if ephemeral_content:
                await interaction.response.send_message(content=ephemeral_content, ephemeral=True, delete_after=5)
            else:
                await interaction.response.defer()

        self.update_components()
        new_embed = self.generate_embed()
        try:
            await self.original_interaction.edit_original_response(embed=new_embed, view=self)
        except discord.NotFound:
            pass

    def update_components(self):
        self.clear_items()
        topic_type = self.topic_data.get('type', 'ticket')

        self.add_item(self.create_button("Edit Label/Emoji", discord.ButtonStyle.secondary, self.edit_label_callback, 0))
        self.add_item(self.create_button("Set Button Color", discord.ButtonStyle.secondary, self.set_button_color_callback, 0))
        
        if topic_type in ['ticket', 'application']:
            self.add_item(self.create_button("Set Welcome Message", discord.ButtonStyle.secondary, self.set_welcome_message_callback, 0))

        self.add_item(self.create_button(f"Type: {topic_type.capitalize()}", discord.ButtonStyle.primary, self.toggle_type_callback, 1))
        
        if topic_type != 'survey':
            mode_label = "Discussion Mode" if topic_type == 'application' else "Ticket Mode"
            topic_mode = self.topic_data.get('mode', 'thread')
            self.add_item(self.create_button(f"{mode_label}: {topic_mode.capitalize()}", discord.ButtonStyle.primary, self.toggle_mode_callback, 1))
            
            parent_label = "Discussion Parent" if topic_type == 'application' else "Ticket Parent"
            parent_button_label = f"Set {parent_label} Channel" if topic_mode == 'thread' else f"Set {parent_label} Category"
            self.add_item(self.create_button(parent_button_label, discord.ButtonStyle.secondary, self.set_parent_callback, 1))
        
        self.add_item(self.create_button("Manage Staff Roles", discord.ButtonStyle.blurple, self.manage_staff_callback, 2))
        self.add_item(self.create_button("Set Log Channel", discord.ButtonStyle.secondary, self.set_log_callback, 2))

        # Staff ping toggle (only for tickets)
        if topic_type == 'ticket':
            ping_enabled = self.topic_data.get('ping_staff_on_create', False)
            ping_label = "Staff Notify: Ping" if ping_enabled else "Staff Notify: Silent"
            self.add_item(self.create_button(ping_label, discord.ButtonStyle.primary, self.toggle_staff_ping_callback, 2))

        if topic_type in ['application', 'survey']:
            self.add_item(self.create_button("Manage Questions", discord.ButtonStyle.blurple, self.manage_questions_callback, 2))

        if topic_type == 'application':
            approval_label = "Approval Buttons: On" if self.topic_data.get('approval_mode') else "Approval Buttons: Off"
            self.add_item(self.create_button(approval_label, discord.ButtonStyle.primary, self.toggle_approval_callback, 2))
            discussion_label = "Auto-Discussion: On" if self.topic_data.get('discussion_mode') else "Auto-Discussion: Off"
            self.add_item(self.create_button(discussion_label, discord.ButtonStyle.primary, self.toggle_discussion_callback, 2))

            chan_mode = self.topic_data.get('application_channel_mode', 'dm')
            chan_label = "App Q&A: Channel" if chan_mode == 'channel' else "App Q&A: DMs"
            self.add_item(self.create_button(chan_label, discord.ButtonStyle.primary, self.toggle_application_channel_mode_callback, 3))

        # Pre-modal question button (for tickets and applications)
        if topic_type in ['ticket', 'application']:
            pre_modal_enabled = self.topic_data.get('pre_modal_enabled', False)
            pre_modal_label = "Pre-Q 1: On" if pre_modal_enabled else "Pre-Q 1: Off"
            self.add_item(self.create_button(pre_modal_label, discord.ButtonStyle.primary, self.toggle_pre_modal_callback, 3))
            if pre_modal_enabled:
                self.add_item(self.create_button("Config Q1", discord.ButtonStyle.secondary, self.configure_pre_modal_callback, 3))
                self.add_item(self.create_button("Config Q1 No", discord.ButtonStyle.secondary, self.configure_pre_modal_no_response_callback, 3))
                # Second pre-question toggle
                pre_modal_2_enabled = self.topic_data.get('pre_modal_2_enabled', False)
                pre_modal_2_label = "Pre-Q 2: On" if pre_modal_2_enabled else "Pre-Q 2: Off"
                self.add_item(self.create_button(pre_modal_2_label, discord.ButtonStyle.primary, self.toggle_pre_modal_2_callback, 3))
                if pre_modal_2_enabled:
                    # Q2 config buttons on row 1 to avoid overflow
                    self.add_item(self.create_button("Config Q2", discord.ButtonStyle.secondary, self.configure_pre_modal_2_callback, 4))
                    self.add_item(self.create_button("Config Q2 No", discord.ButtonStyle.secondary, self.configure_pre_modal_2_no_response_callback, 4))

        # Ticket close settings (only for tickets) - put on row 3 to save space
        if topic_type == 'ticket':
            delete_on_close = self.topic_data.get('delete_on_close', True)
            delete_label = "On Close: Delete" if delete_on_close else "On Close: Archive"
            self.add_item(self.create_button(delete_label, discord.ButtonStyle.secondary, self.toggle_delete_on_close_callback, 2))

            member_can_close = self.topic_data.get('member_can_close', True)
            member_label = "Member Close: Yes" if member_can_close else "Member Close: No"
            self.add_item(self.create_button(member_label, discord.ButtonStyle.secondary, self.toggle_member_can_close_callback, 2))

        # Claim system settings (for tickets and applications) - on row 0 to avoid overflow
        if topic_type in ['ticket', 'application']:
            claim_enabled = self.topic_data.get('claim_enabled', False)
            claim_label = "Claim: On" if claim_enabled else "Claim: Off"
            self.add_item(self.create_button(claim_label, discord.ButtonStyle.primary, self.toggle_claim_callback, 0))
            if claim_enabled:
                self.add_item(self.create_button("Set Alerts Channel", discord.ButtonStyle.secondary, self.set_claim_alerts_channel_callback, 1))
                self.add_item(self.create_button("Set Join Role", discord.ButtonStyle.secondary, self.set_claim_role_callback, 1))

        # Row 4 for finish/cancel (Discord max is row 0-4)
        self.add_item(self.create_button("Finish & Save", discord.ButtonStyle.success, self.finish_callback, 4))
        self.add_item(self.create_button("Cancel", discord.ButtonStyle.danger, self.cancel_callback, 4))

    def create_button(self, label, style, callback, row):
        btn = discord.ui.Button(label=label, style=style, row=row)
        btn.callback = callback
        return btn

    def generate_embed(self) -> discord.Embed:
        action = "Creating" if self.is_new else "Editing"
        topic_type = self.topic_data.get('type', 'ticket')
        embed = discord.Embed(title=f"{action} {topic_type.capitalize()}: `{self.topic_data['name']}`", color=discord.Color.blue())
        embed.add_field(name="Label", value=f"{self.topic_data.get('emoji') or ''} {self.topic_data.get('label')}", inline=True)
        embed.add_field(name="Type", value=topic_type.capitalize(), inline=True)
        button_color = self.topic_data.get('button_color', 'secondary').capitalize()
        embed.add_field(name="Button Color", value=f"`{button_color}`", inline=True)
        
        log_id = self.topic_data.get('log_channel_id')
        log_text = f"<#{log_id}>" if log_id else "`Required`"
        embed.add_field(name="Log Channel", value=log_text, inline=True)
        
        if topic_type != 'survey':
            parent_id = self.topic_data.get('parent_id')
            parent_text = f"<#{parent_id}>" if parent_id else "`Required`"
            parent_context_label = "Ticket/Discussion Parent"
            embed.add_field(name=parent_context_label, value=parent_text, inline=True)
            
            mode_context_label = "Ticket/Discussion Mode"
            embed.add_field(name=mode_context_label, value=self.topic_data.get('mode', 'thread').capitalize(), inline=True)
        
        staff_roles = ", ".join([f"<@&{rid}>" for rid in self.topic_data.get('staff_role_ids', [])]) or "`None`"
        embed.add_field(name="Staff Roles", value=staff_roles, inline=False)
        
        questions = self.topic_data.get('questions', [])
        q_text = "\n".join([f"• {q}" for q in questions]) or "`No questions set.`"
        embed.add_field(name=f"Questions ({len(questions)})", value=q_text[:1024], inline=False)

        if topic_type == 'application':
            embed.add_field(name="Approval Buttons", value="Enabled" if self.topic_data.get('approval_mode') else "Disabled", inline=True)
            embed.add_field(name="Auto-Discussion", value="Enabled" if self.topic_data.get('discussion_mode') else "Disabled", inline=True)
            chan_mode = self.topic_data.get('application_channel_mode', 'dm')
            embed.add_field(name="Q&A Mode", value="In Channel" if chan_mode == 'channel' else "In DMs", inline=True)

        # Show pre-modal question status (for tickets and applications)
        if topic_type in ['ticket', 'application']:
            welcome_msg = self.topic_data.get('welcome_message', 'Default Message')
            # Truncate if too long
            if len(welcome_msg) > 1000:
                welcome_msg = welcome_msg[:997] + "..."
            embed.add_field(name="Welcome Message", value=f"```{welcome_msg}```", inline=False)

            pre_modal_enabled = self.topic_data.get('pre_modal_enabled', False)
            if pre_modal_enabled:
                pre_q = self.topic_data.get('pre_modal_question', 'Not set')
                pre_url = self.topic_data.get('pre_modal_redirect_url') or 'None'
                pre_channel = self.topic_data.get('pre_modal_redirect_channel_id')
                redirect_text = f"<#{pre_channel}>" if pre_channel else pre_url
                embed.add_field(name="Pre-Question 1", value=f"**Q:** {pre_q}\n**Redirect:** {redirect_text}", inline=False)

                # Show second pre-question if enabled
                pre_modal_2_enabled = self.topic_data.get('pre_modal_2_enabled', False)
                if pre_modal_2_enabled:
                    pre_q2 = self.topic_data.get('pre_modal_2_question', 'Not set')
                    pre_url2 = self.topic_data.get('pre_modal_2_redirect_url') or 'None'
                    pre_channel2 = self.topic_data.get('pre_modal_2_redirect_channel_id')
                    redirect_text2 = f"<#{pre_channel2}>" if pre_channel2 else pre_url2
                    embed.add_field(name="Pre-Question 2", value=f"**Q:** {pre_q2}\n**Redirect:** {redirect_text2}", inline=False)
            else:
                embed.add_field(name="Pre-Question", value="`Disabled`", inline=True)

            if topic_type == 'ticket':
                # Show staff notification mode
                ping_staff = self.topic_data.get('ping_staff_on_create', False)
                embed.add_field(name="Staff Notification", value="Ping roles" if ping_staff else "Silent add", inline=True)

                # Show close settings
                delete_on_close = self.topic_data.get('delete_on_close', True)
                member_can_close = self.topic_data.get('member_can_close', True)
                embed.add_field(name="On Close", value="Delete" if delete_on_close else "Archive", inline=True)
                embed.add_field(name="Member Can Close", value="Yes" if member_can_close else "No", inline=True)

            # Show claim settings for tickets and applications
            claim_enabled = self.topic_data.get('claim_enabled', False)
            if claim_enabled:
                alerts_channel_id = self.topic_data.get('claim_alerts_channel_id')
                alerts_text = f"<#{alerts_channel_id}>" if alerts_channel_id else "`Not set`"
                claim_role_id = self.topic_data.get('claim_role_id')
                role_text = f"<@&{claim_role_id}>" if claim_role_id else "`Staff roles`"
                embed.add_field(name="Claim System", value=f"**Alerts:** {alerts_text}\n**Join Role:** {role_text}", inline=False)
            else:
                embed.add_field(name="Claim System", value="`Disabled`", inline=True)

        return embed

    async def toggle_pre_modal_callback(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_enabled'] = not self.topic_data.get('pre_modal_enabled', False)
        await self.update_message_state(interaction)

    async def toggle_staff_ping_callback(self, interaction: discord.Interaction):
        self.topic_data['ping_staff_on_create'] = not self.topic_data.get('ping_staff_on_create', False)
        await self.update_message_state(interaction)

    async def configure_pre_modal_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalConfigModal(self))

    async def toggle_pre_modal_2_callback(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_2_enabled'] = not self.topic_data.get('pre_modal_2_enabled', False)
        await self.update_message_state(interaction)

    async def configure_pre_modal_2_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalConfig2Modal(self))

    async def configure_pre_modal_no_response_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalNoResponseConfigModal(self))

    async def configure_pre_modal_2_no_response_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModal2NoResponseConfigModal(self))

    async def toggle_pre_modal_answer_callback(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_answer_enabled'] = not self.topic_data.get('pre_modal_answer_enabled', False)
        await self.update_message_state(interaction)

    async def configure_pre_modal_answer_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalAnswerConfigModal(self))

    async def set_redirect_channel_callback(self, interaction: discord.Interaction):
        picker_view = discord.ui.View(timeout=180)

        # Add a "Clear" option via a regular select first
        clear_view = discord.ui.View(timeout=180)
        clear_select = discord.ui.Select(
            placeholder="Choose an action...",
            options=[
                discord.SelectOption(label="Set a channel", value="set", description="Choose a channel to redirect users to"),
                discord.SelectOption(label="Clear channel redirect", value="clear", description="Remove the channel redirect (URL will be used if set)")
            ]
        )

        original_msg_interaction = interaction

        async def clear_callback(itx: discord.Interaction):
            action = itx.data['values'][0]
            if action == "clear":
                self.topic_data['pre_modal_redirect_channel_id'] = None
                await self.update_message_state(itx, "Channel redirect cleared.")
                try:
                    await original_msg_interaction.delete_original_response()
                except discord.NotFound:
                    pass
            else:
                # Show channel picker
                channel_picker_view = discord.ui.View(timeout=180)
                channel_select = discord.ui.ChannelSelect(placeholder="Select a channel...", channel_types=[discord.ChannelType.text])

                async def channel_callback(ch_itx: discord.Interaction):
                    self.topic_data['pre_modal_redirect_channel_id'] = int(ch_itx.data['values'][0])
                    await self.update_message_state(ch_itx, "Redirect channel set.")
                    try:
                        await original_msg_interaction.delete_original_response()
                    except discord.NotFound:
                        pass

                channel_select.callback = channel_callback
                channel_picker_view.add_item(channel_select)
                await itx.response.edit_message(content="Select a channel to redirect users to:", view=channel_picker_view)

        clear_select.callback = clear_callback
        clear_view.add_item(clear_select)

        current_channel = self.topic_data.get('pre_modal_redirect_channel_id')
        current_text = f"\nCurrent: <#{current_channel}>" if current_channel else "\nCurrent: None"
        await interaction.response.send_message(f"Configure redirect channel (takes priority over URL).{current_text}", view=clear_view, ephemeral=True)

    async def toggle_delete_on_close_callback(self, interaction: discord.Interaction):
        self.topic_data['delete_on_close'] = not self.topic_data.get('delete_on_close', True)
        await self.update_message_state(interaction)

    async def toggle_member_can_close_callback(self, interaction: discord.Interaction):
        self.topic_data['member_can_close'] = not self.topic_data.get('member_can_close', True)
        await self.update_message_state(interaction)

    async def toggle_claim_callback(self, interaction: discord.Interaction):
        self.topic_data['claim_enabled'] = not self.topic_data.get('claim_enabled', False)
        await self.update_message_state(interaction)

    async def set_claim_alerts_channel_callback(self, interaction: discord.Interaction):
        picker_view = discord.ui.View(timeout=180)
        select = discord.ui.ChannelSelect(placeholder="Select alerts channel...", channel_types=[discord.ChannelType.text])
        original_msg_interaction = interaction

        async def pick_callback(itx: discord.Interaction):
            self.topic_data['claim_alerts_channel_id'] = int(itx.data['values'][0])
            await self.update_message_state(itx, "✅ Alerts channel set.")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        select.callback = pick_callback
        picker_view.add_item(select)
        current_channel = self.topic_data.get('claim_alerts_channel_id')
        current_text = f"\nCurrent: <#{current_channel}>" if current_channel else ""
        await interaction.response.send_message(f"Select a channel for claim alerts.{current_text}", view=picker_view, ephemeral=True)

    async def set_claim_role_callback(self, interaction: discord.Interaction):
        picker_view = discord.ui.View(timeout=180)
        role_select = discord.ui.RoleSelect(placeholder="Select join role...", min_values=0, max_values=1)
        original_msg_interaction = interaction

        async def pick_callback(itx: discord.Interaction):
            if role_select.values:
                self.topic_data['claim_role_id'] = role_select.values[0].id
                await self.update_message_state(itx, "✅ Join role set.")
            else:
                self.topic_data['claim_role_id'] = None
                await self.update_message_state(itx, "✅ Join role cleared (staff roles will be used).")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        role_select.callback = pick_callback
        picker_view.add_item(role_select)
        current_role = self.topic_data.get('claim_role_id')
        current_text = f"\nCurrent: <@&{current_role}>" if current_role else "\nCurrent: Staff roles (default)"
        await interaction.response.send_message(f"Select which role can use the Join button on claimed tickets.{current_text}\n\n*Leave empty to allow staff roles.*", view=picker_view, ephemeral=True)

    async def set_button_color_callback(self, interaction: discord.Interaction):
        view = discord.ui.View(timeout=180)
        options = [
            discord.SelectOption(label="Grey (Default)", value="secondary", emoji="🔘"),
            discord.SelectOption(label="Blue", value="primary", emoji="🔵"),
            discord.SelectOption(label="Green", value="success", emoji="🟢"),
            discord.SelectOption(label="Red", value="danger", emoji="🔴"),
        ]
        select = discord.ui.Select(placeholder="Choose a button color...", options=options)

        original_msg_interaction = interaction

        async def select_callback(itx: discord.Interaction):
            self.topic_data['button_color'] = itx.data['values'][0]
            await self.update_message_state(itx, "✅ Color updated.")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a color for this topic's button:", view=view, ephemeral=True)

    async def toggle_discussion_callback(self, interaction: discord.Interaction):
        self.topic_data['discussion_mode'] = not self.topic_data.get('discussion_mode', False)
        await self.update_message_state(interaction)

    async def toggle_application_channel_mode_callback(self, interaction: discord.Interaction):
        current = self.topic_data.get('application_channel_mode', 'dm')
        self.topic_data['application_channel_mode'] = 'channel' if current == 'dm' else 'dm'
        await self.update_message_state(interaction)

    async def edit_label_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(LabelModal(self))

    async def set_welcome_message_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(WelcomeMessageModal(self))

    async def set_parent_callback(self, interaction: discord.Interaction):
        key = 'parent_id'
        channel_types = [discord.ChannelType.text] if self.topic_data.get('mode') == 'thread' else [discord.ChannelType.category]
        await self.send_channel_picker(interaction, key, channel_types)

    async def set_log_callback(self, interaction: discord.Interaction):
        key = 'log_channel_id'
        channel_types = [discord.ChannelType.text]
        await self.send_channel_picker(interaction, key, channel_types)

    async def manage_staff_callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(view=StaffRoleManagerView(self), ephemeral=True)

    async def manage_questions_callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(view=QuestionManagerView(self), ephemeral=True)

    async def toggle_type_callback(self, interaction: discord.Interaction):
        if self.topic_data.get('type') != 'survey':
            self.topic_data['type'] = 'application' if self.topic_data.get('type', 'ticket') == 'ticket' else 'ticket'
            await self.update_message_state(interaction)
        else:
            await interaction.response.send_message("Cannot change the type of a survey.", ephemeral=True, delete_after=5)

    async def toggle_mode_callback(self, interaction: discord.Interaction):
        self.topic_data['mode'] = 'channel' if self.topic_data.get('mode', 'thread') == 'thread' else 'thread'
        await self.update_message_state(interaction)

    async def toggle_approval_callback(self, interaction: discord.Interaction):
        self.topic_data['approval_mode'] = not self.topic_data.get('approval_mode', False)
        await self.update_message_state(interaction)

    async def finish_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        topic_type = self.topic_data.get('type')
        claim_enabled = self.topic_data.get('claim_enabled', False)

        # For surveys, log channel is always required
        # For applications, log channel is required unless claim is enabled (Q&A goes to claim alert)
        if topic_type == 'survey' and not self.topic_data.get('log_channel_id'):
            await interaction.followup.send("❌ A survey requires a Log Channel to be set.", ephemeral=True)
            return
        if topic_type == 'application' and not self.topic_data.get('log_channel_id') and not claim_enabled:
            await interaction.followup.send("❌ An application requires either a Log Channel or Claim mode enabled.", ephemeral=True)
            return
        # If claim is enabled for application, require alerts channel
        if topic_type == 'application' and claim_enabled and not self.topic_data.get('claim_alerts_channel_id'):
            await interaction.followup.send("❌ Claim mode requires an Alerts Channel to be set.", ephemeral=True)
            return
        # For tickets with claim enabled, also require alerts channel
        if topic_type == 'ticket' and claim_enabled and not self.topic_data.get('claim_alerts_channel_id'):
            await interaction.followup.send("❌ Claim mode requires an Alerts Channel to be set.", ephemeral=True)
            return
        if topic_type != 'survey' and not self.topic_data.get('parent_id'):
            await interaction.followup.send("❌ Please set a parent channel/category before saving.", ephemeral=True)
            return
        
        try:
            topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
            topics[self.topic_data['name']] = self.topic_data
            await _save_json(self.cog.bot, TOPICS_FILE, topics, self.cog.topics_lock)
        except Exception as e:
            logger.error(f"Failed to save topic: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred: `{e.__class__.__name__}`. Check logs.", ephemeral=True)
            return

        # Refresh any live panel messages that include this topic
        asyncio.create_task(self.cog._refresh_panels_for_topic(interaction.guild, self.topic_data['name']))

        await self.original_interaction.edit_original_response(content=f"✅ {topic_type.capitalize()} `{self.topic_data['name']}` saved.", embed=None, view=None)
        self.stop()

    async def cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Configuration cancelled.", embed=None, view=None)
        self.stop()

    async def send_channel_picker(self, interaction: discord.Interaction, key: str, channel_types: List[discord.ChannelType]):
        picker_view = discord.ui.View(timeout=180)
        select = discord.ui.ChannelSelect(placeholder="Select a channel...", channel_types=channel_types)
        
        original_msg_interaction = interaction

        async def pick_callback(itx: discord.Interaction):
            self.topic_data[key] = int(itx.data['values'][0])
            await self.update_message_state(itx, f"✅ {key.replace('_id','').replace('_',' ').title()} set.")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass
        
        select.callback = pick_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Select a channel:", view=picker_view, ephemeral=True)

class LabelModal(discord.ui.Modal, title="Edit Label & Emoji"):
    label = discord.ui.TextInput(label="Button/Option Label")
    emoji = discord.ui.TextInput(label="Emoji (Optional)", required=False, max_length=5)

    def __init__(self, view: TopicWizardView):
        super().__init__()
        self.view = view
        self.label.default = view.topic_data.get('label')
        self.emoji.default = view.topic_data.get('emoji')

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data['label'] = self.label.value
        self.view.topic_data['emoji'] = self.emoji.value or None
        await self.view.update_message_state(itx)

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"LabelModal error: {error}", exc_info=True)

class WelcomeMessageModal(discord.ui.Modal, title="Set Welcome Message"):
    message = discord.ui.TextInput(label="Message", style=discord.TextStyle.long, placeholder="Use {user} for user mention and {topic} for topic label.")

    def __init__(self, view: TopicWizardView):
        super().__init__()
        self.view = view
        self.message.default = view.topic_data.get('welcome_message')

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data['welcome_message'] = self.message.value
        await self.view.update_message_state(itx)

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"WelcomeMessageModal error: {error}", exc_info=True)

class AddQuestionModal(discord.ui.Modal, title="Add a Question"):
    question = discord.ui.TextInput(label="Question Text", style=discord.TextStyle.long, placeholder="Enter the question you want to ask the user.", required=True)

    def __init__(self, parent_wizard: TopicWizardView):
        super().__init__()
        self.parent_wizard = parent_wizard

    async def on_submit(self, itx: discord.Interaction):
        self.parent_wizard.topic_data.setdefault('questions', []).append(self.question.value)
        await self.parent_wizard.update_message_state(itx, "✅ Question added.")

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"AddQuestionModal error: {error}", exc_info=True)

class EditQuestionModal(discord.ui.Modal, title="Edit Question"):
    question = discord.ui.TextInput(label="Question Text", style=discord.TextStyle.long, required=True)

    def __init__(self, parent_wizard: "TopicWizardView", index: int, current_text: str):
        super().__init__()
        self.parent_wizard = parent_wizard
        self.index = index
        self.question.default = current_text

    async def on_submit(self, itx: discord.Interaction):
        questions = self.parent_wizard.topic_data.get('questions', [])
        if 0 <= self.index < len(questions):
            questions[self.index] = self.question.value
        await self.parent_wizard.update_message_state(itx, "✅ Question updated.")

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"EditQuestionModal error: {error}", exc_info=True)

class QuestionManagerView(discord.ui.View):
    def __init__(self, parent_wizard: "TopicWizardView"):
        super().__init__(timeout=180)
        self.parent_wizard = parent_wizard
        self._update_remove_button_state()

    def _update_remove_button_state(self):
        """Safely update the remove/edit buttons' disabled state."""
        has_questions = bool(self.parent_wizard.topic_data.get('questions', []))
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.label in ("Remove Question", "Edit Question"):
                item.disabled = not has_questions

    @discord.ui.button(label="Add Question", style=discord.ButtonStyle.success, row=0)
    async def add_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddQuestionModal(self.parent_wizard))

    @discord.ui.button(label="Edit Question", style=discord.ButtonStyle.primary, row=0)
    async def edit_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        questions = self.parent_wizard.topic_data.get('questions', [])
        if not questions:
            return await interaction.response.send_message("No questions to edit.", ephemeral=True, delete_after=5)

        options = [discord.SelectOption(label=q[:100], value=str(i)) for i, q in enumerate(questions)]
        picker_view = discord.ui.View(timeout=120)
        select = discord.ui.Select(placeholder="Select a question to edit...", options=options, min_values=1, max_values=1)

        original_msg_interaction = interaction

        async def select_callback(itx: discord.Interaction):
            index = int(itx.data['values'][0])
            current_questions = self.parent_wizard.topic_data.get('questions', [])
            try:
                await original_msg_interaction.delete_original_response()
            except (discord.NotFound, discord.HTTPException):
                pass
            if 0 <= index < len(current_questions):
                await itx.response.send_modal(EditQuestionModal(self.parent_wizard, index, current_questions[index]))

        select.callback = select_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Select a question to edit:", view=picker_view, ephemeral=True)

    @discord.ui.button(label="Remove Question", style=discord.ButtonStyle.danger, row=0)
    async def remove_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        questions = self.parent_wizard.topic_data.get('questions', [])
        if not questions:
            return await interaction.response.send_message("No questions to remove.", ephemeral=True, delete_after=5)
        
        options = [discord.SelectOption(label=q[:100], value=str(i)) for i, q in enumerate(questions)]
        picker_view = discord.ui.View(timeout=120)
        select = discord.ui.Select(placeholder="Select questions to remove...", options=options, min_values=1, max_values=len(options))
        
        original_msg_interaction = interaction

        async def select_callback(itx: discord.Interaction):
            indices_to_remove = sorted([int(v) for v in itx.data['values']], reverse=True)
            for index in indices_to_remove:
                if 0 <= index < len(self.parent_wizard.topic_data['questions']):
                    self.parent_wizard.topic_data['questions'].pop(index)
            await self.parent_wizard.update_message_state(itx, "✅ Questions removed.")
            try:
                await original_msg_interaction.delete_original_response()
            except (discord.NotFound, discord.HTTPException):
                pass

        select.callback = select_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Select questions to remove:", view=picker_view, ephemeral=True)

    @discord.ui.button(label="Done", style=discord.ButtonStyle.secondary, row=1)
    async def done(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        try:
            await interaction.response.edit_message(content="Done managing questions.", view=None)
        except discord.HTTPException:
            pass

class StaffRoleManagerView(discord.ui.View):
    def __init__(self, parent_wizard: TopicWizardView):
        super().__init__(timeout=180)
        self.parent_wizard = parent_wizard

    @discord.ui.button(label="Add Roles", style=discord.ButtonStyle.success)
    async def add_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        picker = discord.ui.View(timeout=120)
        role_select = discord.ui.RoleSelect(placeholder="Select roles to add...", min_values=1, max_values=25)
        
        original_msg_interaction = interaction

        async def cb(itx: discord.Interaction):
            current_roles = set(self.parent_wizard.topic_data.get("staff_role_ids", []))
            new_role_ids = {role.id for role in role_select.values}
            current_roles.update(new_role_ids)
            self.parent_wizard.topic_data["staff_role_ids"] = list(current_roles)
            await self.parent_wizard.update_message_state(itx, "✅ Roles added.")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass
            
        role_select.callback = cb
        picker.add_item(role_select)
        await interaction.response.send_message("Select roles:", view=picker, ephemeral=True)

    @discord.ui.button(label="Remove Roles", style=discord.ButtonStyle.danger)
    async def remove_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        current_roles_ids = self.parent_wizard.topic_data.get("staff_role_ids", [])
        if not current_roles_ids:
            return await interaction.response.send_message("No roles to remove.", ephemeral=True, delete_after=10)
        
        options = []
        for r_id in current_roles_ids:
            role = interaction.guild.get_role(r_id)
            if role:
                options.append(discord.SelectOption(label=f"@{role.name}"[:100], value=str(r_id)))
        
        if not options:
            return await interaction.response.send_message("None of the configured roles could be found.", ephemeral=True, delete_after=10)

        picker = discord.ui.View(timeout=120)
        role_select = discord.ui.Select(placeholder="Select roles to remove...", options=options, max_values=len(options))

        original_msg_interaction = interaction

        async def cb(itx: discord.Interaction):
            to_remove_ids = {int(v) for v in itx.data['values']}
            current_ids = set(self.parent_wizard.topic_data.get("staff_role_ids", []))
            current_ids.difference_update(to_remove_ids)
            self.parent_wizard.topic_data["staff_role_ids"] = list(current_ids)
            await self.parent_wizard.update_message_state(itx, "✅ Roles removed.")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        role_select.callback = cb
        picker.add_item(role_select)
        await interaction.response.send_message("Select roles:", view=picker, ephemeral=True)

class PanelWizardView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", interaction: discord.Interaction, panel_data: Dict[str, Any], is_new: bool, all_topics: Dict[str, Any]):
        super().__init__(timeout=600)
        self.cog = cog
        self.original_interaction = interaction
        self.panel_data = panel_data
        self.is_new = is_new
        self.all_topics = all_topics
        self.update_components()

    async def update_message_state(self, interaction: discord.Interaction, ephemeral_content: Optional[str] = None):
        if not interaction.response.is_done():
            if ephemeral_content:
                await interaction.response.send_message(ephemeral_content, ephemeral=True, delete_after=5)
            else:
                await interaction.response.defer()
        elif ephemeral_content:
            await interaction.followup.send(ephemeral_content, ephemeral=True, delete_after=5)

        self.update_components()
        try:
            await self.original_interaction.edit_original_response(embed=self.generate_embed(), view=self)
        except discord.NotFound:
            pass

    def update_components(self):
        self.clear_items()
        self.add_item(self.create_button("Edit Title/Desc", discord.ButtonStyle.secondary, self.edit_text_callback, 0))
        self.add_item(self.create_button("Set Post Channel", discord.ButtonStyle.secondary, self.set_channel_callback, 0))
        self.add_item(self.create_button("Set Image", discord.ButtonStyle.secondary, self.set_image_callback, 0))
        mode = self.panel_data.get('display_mode', 'buttons')
        self.add_item(self.create_button(f"Display: {mode.capitalize()}", discord.ButtonStyle.primary, self.toggle_display_callback, 1))
        self.add_item(self.create_button("Manage Topics", discord.ButtonStyle.blurple, self.manage_topics_callback, 1))
        self.add_item(self.create_button("Finish & Save", discord.ButtonStyle.success, self.finish_callback, 4))
        self.add_item(self.create_button("Cancel", discord.ButtonStyle.danger, self.cancel_callback, 4))

    def create_button(self, label, style, callback, row):
        btn = discord.ui.Button(label=label, style=style, row=row)
        btn.callback = callback
        return btn

    def generate_embed(self) -> discord.Embed:
        action = "Creating" if self.is_new else "Editing"
        embed = discord.Embed(title=f"{action} Panel: `{self.panel_data['name']}`", color=discord.Color.purple())
        embed.add_field(name="Title", value=self.panel_data.get('title'), inline=False)
        embed.add_field(name="Description", value=self.panel_data.get('description')[:1024] if self.panel_data.get('description') else "`Not set`", inline=False)
        channel_id = self.panel_data.get('channel_id')
        channel_text = f"<#{channel_id}>" if channel_id else "`Not set`"
        embed.add_field(name="Post Channel", value=channel_text, inline=True)
        embed.add_field(name="Display Mode", value=self.panel_data.get('display_mode', 'buttons').capitalize(), inline=True)
        image_url = self.panel_data.get('image_url')
        image_type = self.panel_data.get('image_type', 'banner')
        image_text = f"`{image_type.capitalize()}`" if image_url else "`Not set`"
        embed.add_field(name="Image", value=image_text, inline=True)
        attached_topics = self.panel_data.get('topic_names', [])
        topic_text = "\n".join([f"• `{name}`" for name in attached_topics]) or "`None`"
        embed.add_field(name=f"Attached Topics ({len(attached_topics)})", value=topic_text[:1024], inline=False)
        return embed
    
    async def edit_text_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PanelTextModal(self))

    async def set_image_callback(self, interaction: discord.Interaction):
        picker_view = discord.ui.View(timeout=180)
        select = discord.ui.Select(
            placeholder="Choose image type or remove image...",
            options=[
                discord.SelectOption(label="Banner (e.g., 1200x400)", value="banner"),
                discord.SelectOption(label="Thumbnail (e.g., 256x256)", value="thumbnail"),
                discord.SelectOption(label="Remove Image", value="remove", emoji="🗑️")
            ]
        )
        original_msg_interaction = interaction

        async def select_callback(itx: discord.Interaction):
            image_type = itx.data['values'][0]
            if image_type == "remove":
                self.panel_data['image_url'] = None
                await self.update_message_state(itx, "✅ Image removed.")
                try:
                    await original_msg_interaction.delete_original_response()
                except discord.NotFound:
                    pass
                return
            await itx.response.send_modal(ImageUrlModal(self, image_type))
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        select.callback = select_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Select an image type:", view=picker_view, ephemeral=True)

    async def set_channel_callback(self, interaction: discord.Interaction):
        picker_view = discord.ui.View(timeout=180)
        select = discord.ui.ChannelSelect(placeholder="Select a channel...", channel_types=[discord.ChannelType.text])
        original_msg_interaction = interaction

        async def pick_callback(itx: discord.Interaction):
            self.panel_data['channel_id'] = int(itx.data['values'][0])
            await self.update_message_state(itx, "✅ Post channel updated.")
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        select.callback = pick_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Select a channel:", view=picker_view, ephemeral=True)

    async def manage_topics_callback(self, interaction: discord.Interaction):
        all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if not all_topics:
            return await interaction.response.send_message("❌ No topics created yet.", ephemeral=True, delete_after=10)
        options = [
            discord.SelectOption(
                label=f"{t.get('emoji') or '🔘'} {t.get('label')}"[:100], 
                value=name[:100], 
                description=f"Type: {t.get('type', 'N/A').capitalize()}"[:100],
                default=name in self.panel_data.get('topic_names', [])
            ) for name, t in all_topics.items()
        ][:25]  # Discord limit
        picker_view = discord.ui.View(timeout=120)
        select = discord.ui.Select(placeholder="Select topics...", options=options, min_values=0, max_values=len(options))

        async def pick_callback(itx: discord.Interaction):
            self.panel_data['topic_names'] = itx.data['values']
            await self.update_message_state(itx, "✅ Topics updated.")

        select.callback = pick_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Manage attached topics:", view=picker_view, ephemeral=True)

    async def toggle_display_callback(self, interaction: discord.Interaction):
        self.panel_data['display_mode'] = 'dropdown' if self.panel_data.get('display_mode', 'buttons') == 'buttons' else 'buttons'
        await self.update_message_state(interaction)

    async def finish_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self.panel_data.get('channel_id'):
            await interaction.followup.send("❌ Please set a post channel.", ephemeral=True)
            return
        if not self.panel_data.get('topic_names'):
            await interaction.followup.send("❌ Please attach at least one topic.", ephemeral=True)
            return
        try:
            panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
            panels[self.panel_data['name']] = self.panel_data
            await _save_json(self.cog.bot, PANELS_FILE, panels, self.cog.panels_lock)
        except Exception as e:
            logger.error(f"Failed to save panel: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred: `{e.__class__.__name__}`. Check console.", ephemeral=True)
            return
        await self.original_interaction.edit_original_response(content=f"✅ Panel `{self.panel_data['name']}` saved.", embed=None, view=None)
        self.stop()

    async def cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Panel configuration cancelled.", embed=None, view=None)
        self.stop()

class PanelTextModal(discord.ui.Modal, title="Edit Panel Text"):
    title_input = discord.ui.TextInput(label="Embed Title")
    description_input = discord.ui.TextInput(label="Embed Description", style=discord.TextStyle.long)

    def __init__(self, view: PanelWizardView):
        super().__init__()
        self.view = view
        self.title_input.default = view.panel_data.get('title')
        self.description_input.default = view.panel_data.get('description')
    
    async def on_submit(self, itx: discord.Interaction):
        self.view.panel_data['title'] = self.title_input.value
        self.view.panel_data['description'] = self.description_input.value
        await self.view.update_message_state(itx)

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"PanelTextModal error: {error}", exc_info=True)

class ImageUrlModal(discord.ui.Modal, title="Set Image URL"):
    image_url = discord.ui.TextInput(label="Image URL", placeholder="https://example.com/image.png", required=True)

    def __init__(self, view: PanelWizardView, image_type: str):
        super().__init__()
        self.view = view
        self.image_type = image_type
        self.image_url.default = view.panel_data.get('image_url')

    async def on_submit(self, itx: discord.Interaction):
        self.view.panel_data['image_url'] = self.image_url.value
        self.view.panel_data['image_type'] = self.image_type
        await self.view.update_message_state(itx, "✅ Image updated.")

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"ImageUrlModal error: {error}", exc_info=True)

class ReasonModal(discord.ui.Modal):
    reason = discord.ui.TextInput(label="Reason", style=discord.TextStyle.long, placeholder="Provide a reason for this decision...", required=True, max_length=512)

    def __init__(self, parent_view: "ApprovalView", approved: bool):
        super().__init__(title="Provide a Reason")
        self.parent_view = parent_view
        self.approved = approved

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.parent_view.finalize_decision(interaction, self.approved, self.reason.value)

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"ReasonModal error: {error}", exc_info=True)

class ApprovalView(discord.ui.View):
    def __init__(self, bot: "Bot", topic: Dict[str, Any]):
        super().__init__(timeout=None)
        self.bot = bot
        self.topic = topic
        
        if self.topic.get("approval_mode"):
            approve_btn = discord.ui.Button(label="Approve", style=discord.ButtonStyle.success, custom_id="approve_app")
            approve_btn.callback = self.approve_callback
            self.add_item(approve_btn)
            
            deny_btn = discord.ui.Button(label="Deny", style=discord.ButtonStyle.danger, custom_id="deny_app")
            deny_btn.callback = self.deny_callback
            self.add_item(deny_btn)
            
            approve_reason_btn = discord.ui.Button(label="Approve with Reason", style=discord.ButtonStyle.success, custom_id="approve_reason_app", row=1)
            approve_reason_btn.callback = self.approve_with_reason_callback
            self.add_item(approve_reason_btn)
            
            deny_reason_btn = discord.ui.Button(label="Deny with Reason", style=discord.ButtonStyle.danger, custom_id="deny_reason_app", row=1)
            deny_reason_btn.callback = self.deny_with_reason_callback
            self.add_item(deny_reason_btn)

        if not self.topic.get("discussion_mode"):
            row = 2 if self.topic.get("approval_mode") else 0
            discussion_btn = discord.ui.Button(label="Start Discussion", style=discord.ButtonStyle.secondary, custom_id="start_discussion_app", row=row)
            discussion_btn.callback = self.start_discussion_callback
            self.add_item(discussion_btn)

    async def _get_context(self, interaction: discord.Interaction):
        embed = interaction.message.embeds[0]
        applicant_id_match = re.search(r'\((\d{17,19})\)', embed.description or "")
        applicant_id = int(applicant_id_match.group(1)) if applicant_id_match else None
        return applicant_id, self.topic

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        staff_role_ids = set(self.topic.get("staff_role_ids", []))
        if not staff_role_ids:
             await interaction.response.send_message("❌ No staff roles are configured for this topic.", ephemeral=True)
             return False
        user_role_ids = {role.id for role in interaction.user.roles}
        if not staff_role_ids.intersection(user_role_ids):
            await interaction.response.send_message("❌ You do not have permission to use these buttons.", ephemeral=True)
            return False
        return True

    async def finalize_decision(self, interaction: discord.Interaction, approved: bool, reason: Optional[str] = None):
        applicant_id, topic = await self._get_context(interaction)
        if not applicant_id or not topic:
            return await interaction.followup.send("❌ Could not process the decision. Context missing.", ephemeral=True)

        disabled_view = self.__class__(self.bot, self.topic)
        for item in disabled_view.children:
            item.disabled = True
        
        original_embed = interaction.message.embeds[0]
        status = "Approved" if approved else "Denied"
        color = discord.Color.green() if approved else discord.Color.red()
        
        footer_text = f"{status} by {interaction.user.display_name} | {discord.utils.format_dt(discord.utils.utcnow())}"
        if reason:
            original_embed.add_field(name="Reason", value=reason, inline=False)
        
        original_embed.set_footer(text=footer_text)
        original_embed.color = color
        
        await interaction.message.edit(embed=original_embed, view=disabled_view)

        if approved and topic.get("discussion_mode", False):
            try:
                applicant = self.bot.get_user(applicant_id) or await self.bot.fetch_user(applicant_id)
                cog = self.bot.get_cog("TicketSystem")
                discussion_channel = await cog._create_discussion_channel(interaction, topic, applicant)
                if discussion_channel:
                    await discussion_channel.send(embed=original_embed)
                    original_embed.add_field(name="Discussion", value=f"Started in {discussion_channel.mention}", inline=False)
                    await interaction.message.edit(embed=original_embed)
            except Exception as e:
                logger.warning(f"Failed to create post-approval discussion channel: {e}")

        try:
            applicant = self.bot.get_user(applicant_id) or await self.bot.fetch_user(applicant_id)
            if applicant:
                dm_embed = discord.Embed(title=f"Application for '{topic.get('label')}' {status}", description=f"Your application in **{interaction.guild.name}** has been reviewed.", color=color)
                if reason: 
                    dm_embed.add_field(name="Reason", value=reason, inline=False)
                await applicant.send(embed=dm_embed)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            pass
    
    async def approve_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.finalize_decision(interaction, approved=True)

    async def deny_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.finalize_decision(interaction, approved=False)

    async def approve_with_reason_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ReasonModal(self, approved=True))

    async def deny_with_reason_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ReasonModal(self, approved=False))

    async def start_discussion_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        applicant_id, topic = await self._get_context(interaction)
        if not applicant_id or not topic:
            return await interaction.followup.send("❌ Could not start discussion. Context missing.", ephemeral=True)

        try:
            applicant = self.bot.get_user(applicant_id) or await self.bot.fetch_user(applicant_id)
        except discord.NotFound:
            return await interaction.followup.send("❌ Could not find the applicant.", ephemeral=True)

        cog = self.bot.get_cog("TicketSystem")
        discussion_channel = await cog._create_discussion_channel(interaction, topic, applicant)

        if discussion_channel:
            await discussion_channel.send(embed=interaction.message.embeds[0])
            original_embed = interaction.message.embeds[0]
            original_embed.add_field(name="Discussion", value=f"Started in {discussion_channel.mention}", inline=False)
            
            new_view = self.__class__(self.bot, self.topic)
            for child in new_view.children:
                if child.custom_id == "start_discussion_app":
                    child.disabled = True

            await interaction.message.edit(embed=original_embed, view=new_view)
            await interaction.followup.send(f"✅ Discussion started in {discussion_channel.mention}", ephemeral=True)
        else:
            await interaction.followup.send("❌ Failed to start discussion channel.", ephemeral=True)

class ResponseModal(discord.ui.Modal):
    answer = discord.ui.TextInput(label="Your Response", style=discord.TextStyle.long, max_length=1024)

    def __init__(self, title: str):
        super().__init__(title=title, timeout=300)

    async def on_submit(self, interaction: discord.Interaction):
        self.modal_interaction = interaction
        await interaction.response.defer()

class CloseTicketView(discord.ui.View):
    def __init__(self, topic_name: Optional[str] = None, opener_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.topic_name = topic_name
        self.opener_id = opener_id
        # Update button custom_id to include context if provided
        if topic_name and opener_id:
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.label == "Close Ticket":
                    item.custom_id = f"close_ticket_button::{topic_name}::{opener_id}"
                    break

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="close_ticket_button")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        # This callback is mainly for legacy tickets without context in custom_id
        # New tickets are handled via on_interaction listener
        await interaction.response.defer(ephemeral=True)

        cog = interaction.client.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An internal error occurred (Cog not found).", ephemeral=True)

        topic_name, opener_id = await cog._get_ticket_context(interaction.channel)

        if opener_id is None or topic_name is None:
            return await interaction.followup.send("Could not identify the ticket context. This ticket may be corrupted.", ephemeral=True)

        await cog._handle_close_ticket(interaction, topic_name, opener_id)

class ClaimAlertView(discord.ui.View):
    """View attached to claim alerts with a Claim button."""
    def __init__(self, bot: "Bot", topic_name: str, channel_id: int, opener_id: int, qa_embed: Optional[discord.Embed] = None):
        super().__init__(timeout=None)
        self.bot = bot
        self.topic_name = topic_name
        self.channel_id = channel_id
        self.opener_id = opener_id
        self.qa_embed = qa_embed  # For applications with no log channel
        # Set custom_id for persistence
        self.children[0].custom_id = f"claim_ticket::{topic_name}::{channel_id}::{opener_id}"

    async def claim_callback(self, interaction: discord.Interaction):
        """Handle claim button press."""
        await interaction.response.defer()

        # Load topic to check claim_role
        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An error occurred.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic_name)
        if not topic:
            return await interaction.followup.send("Topic no longer exists.", ephemeral=True)

        # Check if user has permission (staff role or claim role)
        staff_role_ids = set(topic.get("staff_role_ids", []))
        claim_role_id = topic.get("claim_role_id")
        user_role_ids = {role.id for role in interaction.user.roles}

        has_permission = bool(staff_role_ids.intersection(user_role_ids))
        if claim_role_id and claim_role_id in user_role_ids:
            has_permission = True

        if not has_permission:
            return await interaction.followup.send("You don't have permission to claim this.", ephemeral=True)

        # Get the channel
        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            return await interaction.followup.send("The ticket channel no longer exists.", ephemeral=True)

        # Add claimer to the channel/thread
        try:
            if isinstance(channel, discord.Thread):
                await channel.add_user(interaction.user)
            elif isinstance(channel, discord.TextChannel):
                await channel.set_permissions(interaction.user, view_channel=True, send_messages=True)
        except discord.Forbidden:
            return await interaction.followup.send("I don't have permission to add you to the channel.", ephemeral=True)

        # Update the embed footer to show who claimed it
        original_embed = interaction.message.embeds[0]
        original_embed.set_footer(text=f"Claimed by {interaction.user.display_name}")
        original_embed.color = discord.Color.green()

        # Replace with ClaimedTicketView
        new_view = ClaimedTicketView(self.bot, self.topic_name, self.channel_id, self.opener_id)

        # If there's a Q&A embed, keep it
        embeds_to_send = [original_embed]
        if self.qa_embed:
            embeds_to_send.append(self.qa_embed)

        await interaction.message.edit(embeds=embeds_to_send, view=new_view)
        await interaction.followup.send(f"You've claimed this ticket: {channel.mention}", ephemeral=True)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success, emoji="✋")
    async def claim_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.claim_callback(interaction)


class ClaimedTicketView(discord.ui.View):
    """View shown after a ticket is claimed - Join and Close buttons."""
    def __init__(self, bot: "Bot", topic_name: str, channel_id: int, opener_id: int):
        super().__init__(timeout=None)
        self.bot = bot
        self.topic_name = topic_name
        self.channel_id = channel_id
        self.opener_id = opener_id
        # Set custom_ids for persistence
        for child in self.children:
            if child.label == "Join":
                child.custom_id = f"claim_join::{topic_name}::{channel_id}::{opener_id}"
            elif child.label == "Close":
                child.custom_id = f"claim_close::{topic_name}::{channel_id}::{opener_id}"

    async def join_callback(self, interaction: discord.Interaction):
        """Handle join button press."""
        await interaction.response.defer(ephemeral=True)

        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An error occurred.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic_name)
        if not topic:
            return await interaction.followup.send("Topic no longer exists.", ephemeral=True)

        # Check if user has claim_role or staff role
        staff_role_ids = set(topic.get("staff_role_ids", []))
        claim_role_id = topic.get("claim_role_id")
        user_role_ids = {role.id for role in interaction.user.roles}

        has_permission = bool(staff_role_ids.intersection(user_role_ids))
        if claim_role_id and claim_role_id in user_role_ids:
            has_permission = True

        if not has_permission:
            return await interaction.followup.send("You don't have the required role to join.", ephemeral=True)

        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            return await interaction.followup.send("The ticket channel no longer exists.", ephemeral=True)

        try:
            if isinstance(channel, discord.Thread):
                await channel.add_user(interaction.user)
            elif isinstance(channel, discord.TextChannel):
                await channel.set_permissions(interaction.user, view_channel=True, send_messages=True)
            await interaction.followup.send(f"You've been added to {channel.mention}", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to add you.", ephemeral=True)

    async def close_callback(self, interaction: discord.Interaction):
        """Handle close button press."""
        await interaction.response.defer(ephemeral=True)

        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An error occurred.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic_name)
        if not topic:
            return await interaction.followup.send("Topic no longer exists.", ephemeral=True)

        # Check permissions
        staff_role_ids = set(topic.get("staff_role_ids", []))
        claim_role_id = topic.get("claim_role_id")
        user_role_ids = {role.id for role in interaction.user.roles}

        has_permission = bool(staff_role_ids.intersection(user_role_ids))
        if claim_role_id and claim_role_id in user_role_ids:
            has_permission = True

        if not has_permission:
            return await interaction.followup.send("You don't have permission to close this.", ephemeral=True)

        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            # Channel already deleted, just update the message
            original_embed = interaction.message.embeds[0]
            original_embed.color = discord.Color.dark_grey()
            footer_text = original_embed.footer.text if original_embed.footer else ""
            original_embed.set_footer(text=f"{footer_text} | Closed by {interaction.user.display_name}")
            for item in self.children:
                item.disabled = True
            await interaction.message.edit(embed=original_embed, view=self)
            return await interaction.followup.send("The ticket channel was already deleted.", ephemeral=True)

        delete_on_close = topic.get("delete_on_close", True)

        try:
            if delete_on_close:
                await channel.delete(reason=f"Ticket closed via claim by {interaction.user} ({interaction.user.id})")
            else:
                if isinstance(channel, discord.Thread):
                    await channel.edit(archived=True, locked=True, reason=f"Ticket closed via claim by {interaction.user}")
                else:
                    await channel.edit(
                        name=f"closed-{channel.name}"[:100],
                        overwrites={interaction.guild.default_role: discord.PermissionOverwrite(send_messages=False)},
                        reason=f"Ticket closed via claim by {interaction.user}"
                    )
        except discord.Forbidden:
            return await interaction.followup.send("I don't have permission to close the channel.", ephemeral=True)

        # Notify opener
        try:
            opener = await self.bot.fetch_user(self.opener_id)
            if opener:
                await opener.send(f"Your ticket has been closed by {interaction.user.display_name}.")
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            pass

        # Update the claim embed
        original_embed = interaction.message.embeds[0]
        original_embed.color = discord.Color.dark_grey()
        footer_text = original_embed.footer.text if original_embed.footer else ""
        original_embed.set_footer(text=f"{footer_text} | Closed by {interaction.user.display_name}")
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(embed=original_embed, view=self)

        close_action = "deleted" if delete_on_close else "archived"
        await interaction.followup.send(f"Ticket has been {close_action}.", ephemeral=True)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.primary, emoji="➡️")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.join_callback(interaction)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, emoji="🔒")
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.close_callback(interaction)


class StartSurveyView(discord.ui.View):
    def __init__(self, topic: Dict, bot: "Bot"):
        super().__init__(timeout=None)
        self.topic = topic
        self.bot = bot
        # Set custom_id after calling super().__init__
        self._set_custom_id()

    def _set_custom_id(self):
        """Safely set the custom_id for the start survey button."""
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.label == "Start Survey":
                item.custom_id = f"start_survey_dm::{self.topic['name']}"
                break

    @discord.ui.button(label="Start Survey", style=discord.ButtonStyle.success)
    async def start_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.response.send_message("An error occurred. Please try again later.", ephemeral=True, delete_after=10)
        
        is_dm = isinstance(interaction.channel, discord.DMChannel)
        
        if not is_dm:
            await interaction.response.send_message("The survey is starting in your DMs. Please make sure you have DMs enabled.", ephemeral=True)
        else:
            await interaction.response.defer()

        asyncio.create_task(cog.conduct_survey_flow(interaction, self.topic))

class ResumeOrRestartView(discord.ui.View):
    """View shown when a user has an active survey session."""
    def __init__(self, cog: "TicketSystem", topic: Dict, session: Dict, interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.cog = cog
        self.topic = topic
        self.session = session
        self.original_interaction = interaction
        self.choice: Optional[str] = None

    @discord.ui.button(label="Resume", style=discord.ButtonStyle.success, emoji="▶️")
    async def resume_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "resume"
        await interaction.response.defer()
        self.stop()

    @discord.ui.button(label="Start Over", style=discord.ButtonStyle.danger, emoji="🔄")
    async def restart_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "restart"
        await interaction.response.defer()
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "cancel"
        await interaction.response.defer()
        self.stop()

class PanelAction(discord.ui.Button):
    def __init__(self, bot: "Bot", topic: Dict[str, Any], panel_data: Dict[str, Any]):
        color_name = topic.get("button_color", "secondary")
        style = getattr(discord.ButtonStyle, color_name, discord.ButtonStyle.secondary)

        super().__init__(
            label=topic.get('label', 'Topic')[:80],
            emoji=topic.get('emoji'),
            style=style,
            custom_id=f"panel_action::{topic.get('name', 'unknown')[:80]}"
        )
        self.bot = bot
        self.topic = topic

    async def callback(self, interaction: discord.Interaction):
        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.response.send_message("An error occurred. Please try again later.", ephemeral=True)

        # Load fresh topic data from file so changes take effect without resending panel
        topic_name = self.topic.get('name')
        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(topic_name)
        if not topic:
            return await interaction.response.send_message("This topic no longer exists.", ephemeral=True)
        topic = _ensure_topic_defaults(topic)

        topic_type = topic.get('type')

        if topic_type == 'ticket':
            user_id = interaction.user.id
            last_ticket_time = cog.cooldowns.get(user_id)
            if last_ticket_time and (datetime.now(timezone.utc) - last_ticket_time.replace(tzinfo=timezone.utc) < timedelta(minutes=5)):
                remaining = last_ticket_time.replace(tzinfo=timezone.utc) + timedelta(minutes=5)
                return await interaction.response.send_message(f"You are on cooldown. Please try again {discord.utils.format_dt(remaining, style='R')}.", ephemeral=True)
            
            # Check if pre-modal question is enabled
            if topic.get('pre_modal_enabled', False):
                pre_question = topic.get('pre_modal_question', 'Do you have your profile link ready?')
                await interaction.response.send_message(pre_question, view=PreModalCheckView(cog, topic, interaction), ephemeral=True)
            else:
                await interaction.response.defer(ephemeral=True, thinking=True)
                ch = await cog._create_discussion_channel(interaction, topic, interaction.user, is_ticket=True)
                if ch:
                    cog.cooldowns[user_id] = datetime.now(timezone.utc)
                    await interaction.followup.send(f"✅ Your ticket has been created: {ch.mention}", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)

        elif topic_type == 'application':
            if not topic.get('questions'):
                return await interaction.response.send_message("❌ This application has no questions configured.", ephemeral=True)

            # Check if pre-modal question is enabled for applications
            # Store guild_id for claim system to create discussion channels
            topic['_guild_id'] = interaction.guild.id
            if topic.get('pre_modal_enabled', False):
                pre_question = topic.get('pre_modal_question', 'Do you have your profile link ready?')
                await interaction.response.send_message(pre_question, view=PreModalCheckView(cog, topic, interaction), ephemeral=True)
            else:
                channel_mode = topic.get('application_channel_mode', 'dm') == 'channel'
                if not channel_mode:
                    await interaction.response.send_message("The application is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
                asyncio.create_task(cog.conduct_survey_flow(interaction, topic))

        elif topic_type == 'survey':
            if not topic.get('questions'):
                return await interaction.response.send_message("❌ This survey has no questions configured.", ephemeral=True)

            await interaction.response.send_message("The survey is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            asyncio.create_task(cog.conduct_survey_flow(interaction, topic))

# ---------- SURVEY ADMIN PANEL AND RELATED CLASSES ----------
class CreateSurveyNameModal(discord.ui.Modal, title="Create New Survey"):
    name_input = discord.ui.TextInput(label="Survey Name", placeholder="e.g., q3-feedback (no spaces)", required=True)

    def __init__(self, cog: "TicketSystem"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        name = self.name_input.value.lower().strip().replace(" ", "-")
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if name in topics:
            return await interaction.followup.send("❌ A topic or survey with this name already exists.", ephemeral=True)
        
        survey_data = _ensure_topic_defaults({"name": name, "label": name.replace("-", " ").title(), "type": "survey"})
        survey_data["welcome_message"] = "" 

        view = TopicWizardView(self.cog, interaction, survey_data, is_new=True)
        await interaction.followup.send(embed=view.generate_embed(), view=view, ephemeral=True)

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred. Please try again.", ephemeral=True)
        logger.error(f"CreateSurveyNameModal error: {error}", exc_info=True)

class SurveyTargetView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", topic_data: Dict):
        super().__init__(timeout=180)
        self.cog = cog
        self.topic_data = topic_data
        self.selected_roles: List[discord.Role] = []
        self.selected_users: List[Union[discord.Member, discord.User]] = []

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select roles to send to...", max_values=25, row=0)
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        self.selected_roles = list(select.values)
        await interaction.response.defer()

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select members to send to...", max_values=25, row=1)
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_users = list(select.values)
        await interaction.response.defer()

    @discord.ui.button(label="Send Survey", style=discord.ButtonStyle.success, row=2)
    async def send(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        targets = set()
        for role in self.selected_roles:
            targets.update(role.members)
        targets.update(self.selected_users)
        
        if not targets:
            return await interaction.followup.send("You must select at least one role or member.", ephemeral=True)

        embed = discord.Embed(
            title=f"Survey Invitation: {self.topic_data.get('label')}",
            description=f"You have been invited to participate in a survey from **{interaction.guild.name}**. Please click the button below to begin.",
            color=discord.Color.blue()
        )
        view = StartSurveyView(self.topic_data, self.cog.bot)

        success_count = 0
        fail_count = 0
        for target in targets:
            if target.bot:
                continue
            try:
                await target.send(embed=embed, view=view)
                success_count += 1
                await asyncio.sleep(0.1)
            except (discord.Forbidden, discord.HTTPException):
                fail_count += 1
        
        await interaction.followup.send(f"✅ Survey sent!\n- **Successful DMs:** {success_count}\n- **Failed DMs (privacy settings):** {fail_count}", ephemeral=True)
        try:
            await interaction.edit_original_response(content="Survey sent.", view=None)
        except discord.NotFound:
            pass

class SurveySelectView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", action: str, options: List[discord.SelectOption]):
        super().__init__(timeout=180)
        self.cog = cog
        self.action = action
        self.add_item(self.SurveySelect(cog, action, options))

    class SurveySelect(discord.ui.Select):
        def __init__(self, cog: "TicketSystem", action: str, options: List[discord.SelectOption]):
            self.cog = cog
            self.action = action
            super().__init__(placeholder=f"Select a survey to {action}...", options=options)

        async def callback(self, interaction: discord.Interaction):
            survey_name = self.values[0]
            
            if self.action == "edit":
                await interaction.response.defer(ephemeral=True)
                topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
                if survey_name not in topics:
                    return await interaction.followup.send("❌ Survey not found.", ephemeral=True)
                view = TopicWizardView(self.cog, interaction, topics[survey_name], is_new=False)
                await interaction.followup.send(embed=view.generate_embed(), view=view, ephemeral=True)
                try:
                    await interaction.edit_original_response(view=None, content=f"Opening editor for '{survey_name}'...")
                except discord.NotFound:
                    pass
            
            elif self.action == "delete":
                confirm_view = discord.ui.View(timeout=60)
                confirm_btn = discord.ui.Button(label="Confirm Delete", style=discord.ButtonStyle.danger)
                cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

                async def confirm_callback(itx: discord.Interaction):
                    await itx.response.defer(ephemeral=True)
                    topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
                    if survey_name in topics:
                        del topics[survey_name]
                        await _save_json(self.cog.bot, TOPICS_FILE, topics, self.cog.topics_lock)
                    panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
                    updated = False
                    for p_data in panels.values():
                        if survey_name in p_data.get('topic_names', []):
                            p_data['topic_names'].remove(survey_name)
                            updated = True
                    if updated:
                        await _save_json(self.cog.bot, PANELS_FILE, panels, self.cog.panels_lock)
                    await itx.followup.send(f"🗑️ Survey `{survey_name}` deleted.", ephemeral=True)
                    try:
                        await interaction.edit_original_response(view=None, content=f"Deleted '{survey_name}'.")
                    except discord.NotFound:
                        pass
                
                async def cancel_callback(itx: discord.Interaction):
                    await itx.response.edit_message(content="Deletion cancelled.", view=None)

                confirm_btn.callback = confirm_callback
                cancel_btn.callback = cancel_callback
                confirm_view.add_item(confirm_btn)
                confirm_view.add_item(cancel_btn)
                await interaction.response.edit_message(content=f"**Are you sure you want to delete '{survey_name}'?** This cannot be undone.", view=confirm_view)

            elif self.action == "export":
                await interaction.response.defer(ephemeral=True, thinking=True)
                all_survey_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
                responses = all_survey_data.get(survey_name)
                if not responses:
                    await interaction.followup.send("No responses found for this survey.", ephemeral=True)
                    try:
                        await interaction.edit_original_response(view=None, content="No responses to export.")
                    except discord.NotFound:
                        pass
                    return

                wb = openpyxl.Workbook()
                ws = wb.active
                ws.title = survey_name[:30]
                headers = ["Timestamp", "User ID", "User Name"]
                all_questions = set()
                for response in responses:
                    all_questions.update(response.get("answers", {}).keys())
                
                sorted_questions = sorted(list(all_questions))
                headers.extend(sorted_questions)
                ws.append(headers)

                for response in responses:
                    row = [response.get("timestamp"), response.get("user_id"), response.get("user_name")]
                    for question in sorted_questions:
                        row.append(response.get("answers", {}).get(question, ""))
                    ws.append(row)

                virtual_file = io.BytesIO()
                wb.save(virtual_file)
                virtual_file.seek(0)
                
                file_name = f"survey_{survey_name}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.xlsx"
                await interaction.followup.send("Here is your survey data export:", file=discord.File(virtual_file, filename=file_name), ephemeral=True)
                try:
                    await interaction.edit_original_response(view=None, content="Export sent.")
                except discord.NotFound:
                    pass

            elif self.action == "send":
                topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
                topic_data = topics.get(survey_name)
                if not topic_data:
                    return await interaction.response.send_message("❌ Survey not found.", ephemeral=True)
                await interaction.response.edit_message(content=f"Select who to send the **'{topic_data.get('label')}'** survey to:", view=SurveyTargetView(self.cog, topic_data))

class SurveyAdminPanelView(discord.ui.View):
    def __init__(self, cog: "TicketSystem"):
        super().__init__(timeout=None)
        self.cog = cog

    async def get_survey_options(self) -> List[discord.SelectOption]:
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        survey_topics = {name: data for name, data in topics.items() if data.get('type') == 'survey'}
        if not survey_topics:
            return []
        return [discord.SelectOption(label=data.get('label', name)[:100], value=name[:100]) for name, data in survey_topics.items()][:25]

    @discord.ui.button(label="Create Survey", style=discord.ButtonStyle.success, row=0)
    async def create_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CreateSurveyNameModal(self.cog))

    @discord.ui.button(label="Edit Survey", style=discord.ButtonStyle.primary, row=0)
    async def edit_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        options = await self.get_survey_options()
        if not options:
            return await interaction.response.send_message("No surveys exist to be edited.", ephemeral=True, delete_after=10)
        await interaction.response.send_message(view=SurveySelectView(self.cog, action="edit", options=options), ephemeral=True)

    @discord.ui.button(label="Delete Survey", style=discord.ButtonStyle.danger, row=0)
    async def delete_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        options = await self.get_survey_options()
        if not options:
            return await interaction.response.send_message("No surveys exist to be deleted.", ephemeral=True, delete_after=10)
        await interaction.response.send_message(view=SurveySelectView(self.cog, action="delete", options=options), ephemeral=True)

    @discord.ui.button(label="Send Survey", style=discord.ButtonStyle.secondary, row=1)
    async def send_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        options = await self.get_survey_options()
        if not options:
            return await interaction.response.send_message("No surveys exist to be sent.", ephemeral=True, delete_after=10)
        await interaction.response.send_message(view=SurveySelectView(self.cog, action="send", options=options), ephemeral=True)

    @discord.ui.button(label="Export Results", style=discord.ButtonStyle.secondary, row=1)
    async def export_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        if openpyxl is None:
            await interaction.response.send_message("❌ The `openpyxl` library is not installed on the bot. Please contact the bot owner.", ephemeral=True)
            return
        options = await self.get_survey_options()
        if not options:
            return await interaction.response.send_message("No surveys exist to be exported.", ephemeral=True, delete_after=10)
        await interaction.response.send_message(view=SurveySelectView(self.cog, action="export", options=options), ephemeral=True)

@app_commands.default_permissions(administrator=True)
class TicketSystem(commands.Cog):
    ticket_group = app_commands.Group(name="ticket", description="Main command for the ticketing system.")
    survey_group = app_commands.Group(name="survey", description="Commands to manage surveys.")

    def __init__(self, bot: "Bot"):
        self.bot = bot
        self.persistent_views_added = False
        self.topics_lock = asyncio.Lock()
        self.panels_lock = asyncio.Lock()
        self.survey_data_lock = asyncio.Lock()
        self.survey_sessions_lock = asyncio.Lock()
        self.cooldowns: Dict[int, datetime] = {}
        # Survey cooldowns: {user_id: {survey_name: last_submission_time}}
        self.survey_cooldowns: Dict[int, Dict[str, datetime]] = {}
        # Active survey sessions: {user_id: session_data}
        self.active_survey_sessions: Dict[int, Dict[str, Any]] = {}
        self._cooldown_cleanup_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        """Called when the cog is loaded."""
        self._cooldown_cleanup_task = asyncio.create_task(self._cleanup_cooldowns_loop())
        # Restore survey sessions from file
        await self._restore_survey_sessions()

    async def cog_unload(self):
        """Called when the cog is unloaded."""
        if self._cooldown_cleanup_task:
            self._cooldown_cleanup_task.cancel()
            try:
                await self._cooldown_cleanup_task
            except asyncio.CancelledError:
                pass
        # Save active survey sessions to file for persistence across restarts
        await self._save_survey_sessions()

    async def _restore_survey_sessions(self):
        """Restore survey sessions from file on cog load."""
        try:
            sessions = await _load_json(self.bot, SURVEY_SESSIONS_FILE, self.survey_sessions_lock)
            now = datetime.now(timezone.utc)
            restored_count = 0
            for user_id_str, session in list(sessions.items()):
                try:
                    user_id = int(user_id_str)
                    # Check if session is not too old (24 hours max)
                    session_time = datetime.fromisoformat(session.get("started_at", ""))
                    if (now - session_time) < timedelta(hours=24):
                        self.active_survey_sessions[user_id] = session
                        restored_count += 1
                except (ValueError, KeyError):
                    continue
            if restored_count > 0:
                logger.info(f"Restored {restored_count} active survey sessions from file.")
        except Exception as e:
            logger.error(f"Failed to restore survey sessions: {e}", exc_info=True)

    async def _save_survey_sessions(self):
        """Save active survey sessions to file."""
        try:
            # Convert keys to strings for JSON
            sessions_to_save = {str(k): v for k, v in self.active_survey_sessions.items()}
            await _save_json(self.bot, SURVEY_SESSIONS_FILE, sessions_to_save, self.survey_sessions_lock)
            if sessions_to_save:
                logger.info(f"Saved {len(sessions_to_save)} active survey sessions to file.")
        except Exception as e:
            logger.error(f"Failed to save survey sessions: {e}", exc_info=True)

    async def _cleanup_cooldowns_loop(self):
        """Background task to clean up expired cooldowns every 10 minutes."""
        await self.bot.wait_until_ready()
        while True:
            try:
                await asyncio.sleep(600)  # 10 minutes
                now = datetime.now(timezone.utc)

                # Cleanup ticket cooldowns
                expired = [
                    uid for uid, ts in self.cooldowns.items()
                    if (now - ts.replace(tzinfo=timezone.utc)) > timedelta(minutes=10)
                ]
                for uid in expired:
                    del self.cooldowns[uid]
                if expired:
                    logger.debug(f"Cleaned up {len(expired)} expired ticket cooldowns.")

                # Cleanup survey cooldowns (remove entries older than 24 hours)
                survey_expired_users = []
                for uid, surveys in self.survey_cooldowns.items():
                    expired_surveys = [
                        sname for sname, ts in surveys.items()
                        if (now - ts.replace(tzinfo=timezone.utc)) > timedelta(hours=24)
                    ]
                    for sname in expired_surveys:
                        del surveys[sname]
                    if not surveys:
                        survey_expired_users.append(uid)
                for uid in survey_expired_users:
                    del self.survey_cooldowns[uid]

                # Cleanup expired survey sessions (older than 24 hours)
                expired_sessions = [
                    uid for uid, session in self.active_survey_sessions.items()
                    if (now - datetime.fromisoformat(session.get("started_at", now.isoformat())).replace(tzinfo=timezone.utc)) > timedelta(hours=24)
                ]
                for uid in expired_sessions:
                    del self.active_survey_sessions[uid]
                if expired_sessions:
                    logger.debug(f"Cleaned up {len(expired_sessions)} expired survey sessions.")
                    await self._save_survey_sessions()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cooldown cleanup error: {e}", exc_info=True)

    async def _create_discussion_channel(self, interaction: discord.Interaction, topic: Dict[str, Any], member: Union[discord.Member, discord.User], is_ticket: bool = False, user_answer: Optional[str] = None):
        try:
            guild = interaction.guild
            channel_name = f"{topic.get('name', 'ticket')}-{member.name}".replace(" ", "-").lower()[:100]
            parent_id = topic.get('parent_id')
            if not parent_id:
                raise ValueError("Parent not set.")
            parent = guild.get_channel(parent_id)
            if not parent:
                raise ValueError(f"Parent {parent_id} not found.")

            topic_name = topic.get('name', 'unknown')
            channel_topic_str = f"Ticket Topic: {topic_name} | Opener: {member.id}"
            welcome_template = topic.get("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
            welcome_message = welcome_template.format(user=member.mention, topic=topic.get('label', 'N/A'))

            # Create CloseTicketView with context
            close_view = CloseTicketView(topic_name=topic_name, opener_id=member.id)

            if topic.get('mode') == 'channel':
                if not isinstance(parent, discord.CategoryChannel):
                    raise ValueError("Parent must be a category.")
                if is_ticket:
                    existing = discord.utils.get(parent.text_channels, name=channel_name)
                    if existing:
                        if interaction.response.is_done():
                            await interaction.followup.send(f"You already have a ticket: {existing.mention}", ephemeral=True)
                        else:
                            await interaction.response.send_message(f"You already have a ticket: {existing.mention}", ephemeral=True)
                        return None
                overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False)}
                if is_ticket:
                    overwrites[member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                else:
                    overwrites[member] = discord.PermissionOverwrite(view_channel=False)
                overwrites[guild.me] = discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True)
                for rid in topic.get("staff_role_ids", []):
                    role = guild.get_role(rid)
                    if role:
                        overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

                new_channel = await parent.create_text_channel(name=channel_name, overwrites=overwrites, topic=channel_topic_str)

                # For channel mode, send as embed with optional answer field
                embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                if user_answer:
                    answer_question = topic.get('pre_modal_answer_question', 'Response')
                    embed.add_field(name=answer_question, value=user_answer, inline=False)
                await new_channel.send(embed=embed, view=close_view)

                # Ping staff roles if enabled (only for tickets)
                if is_ticket and topic.get('ping_staff_on_create', False):
                    staff_mentions = []
                    for rid in topic.get("staff_role_ids", []):
                        role = guild.get_role(rid)
                        if role:
                            staff_mentions.append(role.mention)
                    if staff_mentions:
                        await new_channel.send(f"{' '.join(staff_mentions)} - New ticket opened!", delete_after=5)

                # Send claim alert if enabled (for tickets)
                if is_ticket and topic.get('claim_enabled', False):
                    await self._send_claim_alert(topic, new_channel, member)

                return new_channel

            else:  # Thread mode
                if not isinstance(parent, discord.TextChannel):
                    raise ValueError("Parent must be a text channel.")
                if is_ticket:
                    # Properly gather all threads
                    all_threads = list(parent.threads)
                    try:
                        async for archived_thread in parent.archived_threads(limit=100):
                            all_threads.append(archived_thread)
                    except discord.Forbidden:
                        pass

                    existing = discord.utils.get(all_threads, name=channel_name)
                    if existing:
                        if interaction.response.is_done():
                            await interaction.followup.send(f"You already have a ticket: {existing.mention}", ephemeral=True)
                        else:
                            await interaction.response.send_message(f"You already have a ticket: {existing.mention}", ephemeral=True)
                        return None

                ch = await parent.create_thread(name=channel_name, type=discord.ChannelType.private_thread)
                if is_ticket:
                    await ch.add_user(member)
                    embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                    # Add user answer field if provided
                    if user_answer:
                        answer_question = topic.get('pre_modal_answer_question', 'Response')
                        embed.add_field(name=answer_question, value=user_answer, inline=False)
                    # No footer - context is stored in the close button custom_id
                    await ch.send(embed=embed, view=close_view)

                # Add staff to thread by mentioning their roles
                staff_mentions = []
                for rid in topic.get("staff_role_ids", []):
                    role = guild.get_role(rid)
                    if role:
                        staff_mentions.append(role.mention)
                if staff_mentions:
                    mention_text = ' '.join(staff_mentions)
                    if is_ticket and topic.get('ping_staff_on_create', False):
                        await ch.send(f"{mention_text} - New ticket opened!", delete_after=5)
                    else:
                        # Mention roles to add staff, then delete to keep it clean
                        await ch.send(mention_text, delete_after=2)

                # Send claim alert if enabled (for tickets)
                if is_ticket and topic.get('claim_enabled', False):
                    await self._send_claim_alert(topic, ch, member)

                return ch
        except Exception as e:
            error_msg = f"Error in _create_discussion_channel: {e}"
            logger.error(error_msg)
            return None

    async def _send_claim_alert(self, topic: Dict[str, Any], channel: Union[discord.TextChannel, discord.Thread], opener: Union[discord.Member, discord.User], qa_embed: Optional[discord.Embed] = None):
        """Send a claim alert to the configured alerts channel."""
        alerts_channel_id = topic.get('claim_alerts_channel_id')
        if not alerts_channel_id:
            return

        alerts_channel = self.bot.get_channel(alerts_channel_id)
        if not alerts_channel:
            logger.warning(f"Claim alerts channel {alerts_channel_id} not found.")
            return

        topic_type = topic.get('type', 'ticket')
        topic_name = topic.get('name', 'unknown')
        topic_label = topic.get('label', 'Unknown Topic')

        # Create a brief claim alert embed
        alert_embed = discord.Embed(
            title=f"New {topic_type.capitalize()} Awaiting Claim",
            color=discord.Color.orange()
        )
        alert_embed.add_field(name="From", value=f"{opener.mention} ({opener.name})", inline=True)
        alert_embed.add_field(name="Topic", value=topic_label, inline=True)
        alert_embed.add_field(name="Link", value=channel.mention, inline=True)
        alert_embed.set_footer(text="Click Claim to handle this ticket")
        alert_embed.timestamp = discord.utils.utcnow()

        # Create the claim view
        view = ClaimAlertView(self.bot, topic_name, channel.id, opener.id, qa_embed=qa_embed)

        try:
            embeds_to_send = [alert_embed]
            if qa_embed:
                embeds_to_send.append(qa_embed)
            await alerts_channel.send(embeds=embeds_to_send, view=view)
        except discord.Forbidden:
            logger.warning(f"Missing permissions to send claim alert to {alerts_channel_id}")
        except discord.HTTPException as e:
            logger.error(f"Failed to send claim alert: {e}")

    async def _create_application_discussion(self, topic: Dict[str, Any], member: discord.Member, guild: discord.Guild) -> Optional[Union[discord.TextChannel, discord.Thread]]:
        """Create a discussion channel for an application (used when claim is enabled)."""
        try:
            channel_name = f"{topic.get('name', 'app')}-{member.name}".replace(" ", "-").lower()[:100]
            parent_id = topic.get('parent_id')
            if not parent_id:
                return None

            parent = guild.get_channel(parent_id)
            if not parent:
                return None

            topic_name = topic.get('name', 'unknown')
            channel_topic_str = f"Application Topic: {topic_name} | Applicant: {member.id}"
            welcome_template = topic.get("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
            welcome_message = welcome_template.format(user=member.mention, topic=topic.get('label', 'N/A'))

            if topic.get('mode') == 'channel':
                if not isinstance(parent, discord.CategoryChannel):
                    return None

                overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False)}
                overwrites[member] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
                overwrites[guild.me] = discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True)
                for rid in topic.get("staff_role_ids", []):
                    role = guild.get_role(rid)
                    if role:
                        overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

                new_channel = await parent.create_text_channel(name=channel_name, overwrites=overwrites, topic=channel_topic_str)

                welcome_embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                close_view = CloseTicketView(topic_name=topic_name, opener_id=member.id)
                await new_channel.send(content=member.mention, embed=welcome_embed, view=close_view)

                return new_channel

            else:  # Thread mode
                if not isinstance(parent, discord.TextChannel):
                    return None

                ch = await parent.create_thread(name=channel_name, type=discord.ChannelType.private_thread)

                # Add applicant to thread
                try:
                    await ch.add_user(member)
                except discord.HTTPException:
                    pass

                # Add staff to thread by mentioning their roles
                staff_mentions = []
                for rid in topic.get("staff_role_ids", []):
                    role = guild.get_role(rid)
                    if role:
                        staff_mentions.append(role.mention)
                if staff_mentions:
                    await ch.send(' '.join(staff_mentions), delete_after=2)

                welcome_embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                close_view = CloseTicketView(topic_name=topic_name, opener_id=member.id)
                await ch.send(content=member.mention, embed=welcome_embed, view=close_view)

                return ch

        except Exception as e:
            logger.error(f"Error in _create_application_discussion: {e}")
            return None

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.persistent_views_added:
            asyncio.create_task(self.load_persistent_views())

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if not interaction.data or "custom_id" not in interaction.data:
            return
        custom_id = interaction.data["custom_id"]

        # Handle close ticket buttons with context in custom_id
        if custom_id.startswith("close_ticket_button::"):
            await interaction.response.defer(ephemeral=True)
            parts = custom_id.split("::")
            if len(parts) >= 3:
                topic_name = parts[1]
                try:
                    opener_id = int(parts[2])
                except ValueError:
                    return await interaction.followup.send("Invalid ticket context.", ephemeral=True)
                await self._handle_close_ticket(interaction, topic_name, opener_id)
            return

        # Handle claim buttons
        if custom_id.startswith("claim_ticket::") or custom_id.startswith("claim_join::") or custom_id.startswith("claim_close::"):
            parts = custom_id.split("::")
            if len(parts) >= 4:
                topic_name = parts[1]
                try:
                    channel_id = int(parts[2])
                    opener_id = int(parts[3])
                except ValueError:
                    return await interaction.response.send_message("Invalid claim context.", ephemeral=True)

                # Get Q&A embed if present (for applications)
                qa_embed = None
                if interaction.message and len(interaction.message.embeds) > 1:
                    qa_embed = interaction.message.embeds[1]

                if custom_id.startswith("claim_ticket::"):
                    view = ClaimAlertView(self.bot, topic_name, channel_id, opener_id, qa_embed=qa_embed)
                    await view.claim_callback(interaction)
                elif custom_id.startswith("claim_join::"):
                    view = ClaimedTicketView(self.bot, topic_name, channel_id, opener_id)
                    await view.join_callback(interaction)
                elif custom_id.startswith("claim_close::"):
                    view = ClaimedTicketView(self.bot, topic_name, channel_id, opener_id)
                    await view.close_callback(interaction)
            return

        if custom_id.startswith("approve_") or custom_id.startswith("deny_") or custom_id.startswith("start_discussion_"):
            if not interaction.message or not interaction.message.embeds:
                return
            embed = interaction.message.embeds[0]
            if not embed.footer or not embed.footer.text:
                return

            topic_name_match = re.search(r'Topic: (\S+)', embed.footer.text)
            if not topic_name_match:
                return
            
            topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
            topic = topics.get(topic_name_match.group(1))
            if not topic:
                return

            view = ApprovalView(self.bot, topic)
            if await view.interaction_check(interaction):
                if custom_id == "approve_app":
                    await view.approve_callback(interaction)
                elif custom_id == "deny_app":
                    await view.deny_callback(interaction)
                elif custom_id == "approve_reason_app":
                    await view.approve_with_reason_callback(interaction)
                elif custom_id == "deny_reason_app":
                    await view.deny_with_reason_callback(interaction)
                elif custom_id == "start_discussion_app":
                    await view.start_discussion_callback(interaction)
    
    async def load_persistent_views(self):
        await self.bot.wait_until_ready()
        panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
        for name, p_data in panels.items():
            view = self.create_panel_view(p_data, topics)
            if view and p_data.get("message_id"):
                self.bot.add_view(view, message_id=p_data["message_id"])
        
        self.bot.add_view(CloseTicketView())
        
        for t_data in topics.values():
            if t_data.get("type") == "application":
                if t_data.get("approval_mode") or not t_data.get("discussion_mode"):
                     self.bot.add_view(ApprovalView(self.bot, t_data))
            elif t_data.get("type") == "survey":
                self.bot.add_view(StartSurveyView(t_data, self.bot))

        self.persistent_views_added = True
        logger.info("Persistent ticket, survey, and action views have been loaded.")

    async def _refresh_panels_for_topic(self, guild: discord.Guild, topic_name: str):
        """Edit live panel messages for every panel that includes the given topic."""
        try:
            panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
            topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
            for panel_data in panels.values():
                if topic_name not in panel_data.get('topic_names', []):
                    continue
                channel_id = panel_data.get('channel_id')
                message_id = panel_data.get('message_id')
                if not channel_id or not message_id:
                    continue
                channel = guild.get_channel(channel_id)
                if not channel or not isinstance(channel, discord.TextChannel):
                    continue
                try:
                    msg = await channel.fetch_message(message_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    continue
                view = self.create_panel_view(panel_data, topics)
                if not view:
                    continue
                try:
                    await msg.edit(view=view)
                except (discord.Forbidden, discord.HTTPException) as e:
                    logger.warning(f"Failed to refresh panel '{panel_data.get('name')}' message: {e}")
        except Exception as e:
            logger.error(f"Error refreshing panels for topic '{topic_name}': {e}", exc_info=True)

    def create_panel_view(self, panel_data: Dict[str, Any], all_topics: Dict[str, Any]) -> Optional[discord.ui.View]:
        view = discord.ui.View(timeout=None)
        attached_topic_names = panel_data.get('topic_names', [])
        if not attached_topic_names:
            return None
        valid_topics = {name: all_topics[name] for name in attached_topic_names if name in all_topics}
        
        if panel_data.get('display_mode') == 'dropdown':
            options = [
                discord.SelectOption(
                    label=t.get('label', name)[:100],
                    value=name[:100],
                    emoji=t.get('emoji')
                ) for name, t in valid_topics.items()
            ][:25]
            if not options:
                return None
            select = discord.ui.Select(placeholder="Select an option...", options=options, custom_id=f"panel_select::{panel_data['name'][:80]}")

            async def select_cb(itx: discord.Interaction):
                topic_name = itx.data['values'][0]
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                topic_data = topics.get(topic_name)
                if topic_data:
                    await PanelAction(self.bot, topic_data, panel_data).callback(itx)
                else:
                    await itx.response.send_message("❌ Topic not found.", ephemeral=True)

            select.callback = select_cb
            view.add_item(select)
        else:
            for name, t_data in valid_topics.items():
                view.add_item(PanelAction(self.bot, t_data, panel_data))
        return view if view.children else None

    async def topic_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
        ticket_topics = {k: v for k, v in topics.items() if v.get('type') != 'survey'}
        return [app_commands.Choice(name=name, value=name) for name in ticket_topics if current.lower() in name.lower()][:25]
    
    async def panel_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
        return [app_commands.Choice(name=name, value=name) for name in panels if current.lower() in name.lower()][:25]
        
    @ticket_group.command(name="topic", description="Manage ticket/application topics.")
    @app_commands.describe(action="The action to perform on a topic.", name="The unique name for the topic (e.g., 'ban-appeal').")
    @app_commands.choices(action=[
        app_commands.Choice(name="Create", value="create"), app_commands.Choice(name="Edit", value="edit"),
        app_commands.Choice(name="Delete", value="delete"), app_commands.Choice(name="List", value="list"),
    ])
    @app_commands.autocomplete(name=topic_autocomplete)
    async def topic(self, interaction: discord.Interaction, action: str, name: Optional[str] = None):
        if action in ["create", "edit", "delete"] and not name:
            return await interaction.response.send_message(f"❌ You must provide a name to {action} a topic.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            if action == "create":
                name = name.lower().strip().replace(" ", "-")
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                if name in topics:
                    return await interaction.followup.send("❌ A topic or survey with this name already exists.", ephemeral=True)
                topic_data = _ensure_topic_defaults({"name": name, "label": name.replace("-", " ").title()})
                view = TopicWizardView(self, interaction, topic_data, is_new=True)
                await interaction.followup.send(embed=view.generate_embed(), view=view, ephemeral=True)
            elif action == "edit":
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                if name not in topics or topics[name].get('type') == 'survey':
                    return await interaction.followup.send("❌ Topic not found.", ephemeral=True)
                # Ensure topic has all default fields before editing
                topic_data = _ensure_topic_defaults(topics[name])
                view = TopicWizardView(self, interaction, topic_data, is_new=False)
                await interaction.followup.send(embed=view.generate_embed(), view=view, ephemeral=True)
            elif action == "delete":
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                if name not in topics or topics[name].get('type') == 'survey':
                    return await interaction.followup.send("❌ Topic not found.", ephemeral=True)
                del topics[name]
                await _save_json(self.bot, TOPICS_FILE, topics, self.topics_lock)
                panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
                updated = False
                for p_data in panels.values():
                    if name in p_data.get('topic_names', []):
                        p_data['topic_names'].remove(name)
                        updated = True
                if updated:
                    await _save_json(self.bot, PANELS_FILE, panels, self.panels_lock)
                await interaction.followup.send(f"🗑️ Topic `{name}` deleted.", ephemeral=True)
            elif action == "list":
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                ticket_topics = {k: v for k, v in topics.items() if v.get('type') != 'survey'}
                if not ticket_topics:
                    return await interaction.followup.send("No topics have been created yet.", ephemeral=True)
                lines = [f"• `{name}` - {t.get('emoji') or ''} **{t.get('label')}** ({t.get('type')})" for name, t in ticket_topics.items()]
                await interaction.followup.send("**Available Topics:**\n" + "\n".join(lines), ephemeral=True)
        except Exception as e:
            logger.error(f"Error in topic command: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred: {e}", ephemeral=True)

    @ticket_group.command(name="panel", description="Manage ticket panels.")
    @app_commands.describe(action="The action to perform on a panel.", name="The unique name for the panel (e.g., 'main-support').")
    @app_commands.choices(action=[
        app_commands.Choice(name="Create", value="create"), app_commands.Choice(name="Edit", value="edit"),
        app_commands.Choice(name="Delete", value="delete"), app_commands.Choice(name="Send", value="send"),
    ])
    @app_commands.autocomplete(name=panel_autocomplete)
    async def panel(self, interaction: discord.Interaction, action: str, name: Optional[str] = None):
        if not name:
            return await interaction.response.send_message(f"❌ You must provide a name to {action} a panel.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        if action == "create":
            name = name.lower().strip().replace(" ", "-")
            panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
            if name in panels:
                return await interaction.followup.send("❌ A panel with this name already exists.", ephemeral=True)
            topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
            panel_data = _ensure_panel_defaults({"name": name})
            view = PanelWizardView(self, interaction, panel_data, is_new=True, all_topics=topics)
            await interaction.followup.send(embed=view.generate_embed(), view=view, ephemeral=True)
        elif action == "edit":
            panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
            if name not in panels:
                return await interaction.followup.send("❌ Panel not found.", ephemeral=True)
            topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
            view = PanelWizardView(self, interaction, panels[name], is_new=False, all_topics=topics)
            await interaction.followup.send(embed=view.generate_embed(), view=view, ephemeral=True)
        elif action == "delete":
            panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
            if name not in panels:
                return await interaction.followup.send("❌ Panel not found.", ephemeral=True)
            del panels[name]
            await _save_json(self.bot, PANELS_FILE, panels, self.panels_lock)
            await interaction.followup.send(f"🗑️ Panel `{name}` deleted.", ephemeral=True)
        elif action == "send":
            panels = await _load_json(self.bot, PANELS_FILE, self.panels_lock)
            topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
            panel_data = panels.get(name)
            if not panel_data:
                return await interaction.followup.send("❌ Panel not found.", ephemeral=True)
            channel_id = panel_data.get('channel_id')
            if not channel_id:
                return await interaction.followup.send("❌ Panel has no post channel set.", ephemeral=True)
            channel = interaction.guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                return await interaction.followup.send("❌ Post channel not found.", ephemeral=True)
            view = self.create_panel_view(panel_data, topics)
            if not view:
                return await interaction.followup.send("❌ Panel has no valid topics attached.", ephemeral=True)
            embed = discord.Embed(title=panel_data.get('title'), description=panel_data.get('description'), color=discord.Color.purple())
            if self.bot.user.avatar:
                embed.set_footer(text="Powered by Better Vibes", icon_url=self.bot.user.avatar.url)
            image_url = panel_data.get("image_url")
            if image_url:
                if panel_data.get("image_type") == "thumbnail":
                    embed.set_thumbnail(url=image_url)
                else:
                    embed.set_image(url=image_url)
            msg_id = panel_data.get("message_id")
            if msg_id:
                try:
                    old_message = await channel.fetch_message(msg_id)
                    await old_message.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
            try:
                msg = await channel.send(embed=embed, view=view)
                panels[name]['message_id'] = msg.id
                await _save_json(self.bot, PANELS_FILE, panels, self.panels_lock)
                await interaction.followup.send(f"✅ Panel `{name}` sent to {channel.mention}.", ephemeral=True)
            except Exception as e:
                logger.error(f"Failed to send panel: {e}", exc_info=True)
                await interaction.followup.send(f"❌ An error occurred: {e}", ephemeral=True)

    @survey_group.command(name="admin", description="Opens the survey administration panel.")
    async def survey_admin(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Survey Admin Panel",
            description="Use the buttons below to manage surveys.",
            color=discord.Color.teal()
        )
        await interaction.response.send_message(embed=embed, view=SurveyAdminPanelView(self), ephemeral=True)

    @ticket_group.command(name="export", description="Export application responses to Excel.")
    async def ticket_export(self, interaction: discord.Interaction):
        if openpyxl is None:
            await interaction.response.send_message("The `openpyxl` library is not installed on the bot. Please contact the bot owner.", ephemeral=True)
            return

        # Get all application topics
        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
        application_topics = {name: data for name, data in topics.items() if data.get('type') == 'application'}

        if not application_topics:
            return await interaction.response.send_message("No application topics exist.", ephemeral=True, delete_after=10)

        options = [discord.SelectOption(label=data.get('label', name)[:100], value=name[:100]) for name, data in application_topics.items()][:25]

        view = discord.ui.View(timeout=180)
        select = discord.ui.Select(placeholder="Select an application to export...", options=options)

        async def select_callback(itx: discord.Interaction):
            app_name = select.values[0]
            await itx.response.defer(ephemeral=True, thinking=True)

            all_data = await _load_json(self.bot, SURVEY_DATA_FILE, self.survey_data_lock)
            responses = all_data.get(app_name)

            if not responses:
                await itx.followup.send("No responses found for this application.", ephemeral=True)
                return

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = app_name[:30]
            headers = ["Timestamp", "User ID", "User Name"]

            all_questions = set()
            for response in responses:
                all_questions.update(response.get("answers", {}).keys())

            sorted_questions = sorted(list(all_questions))
            headers.extend(sorted_questions)
            ws.append(headers)

            for response in responses:
                row = [response.get("timestamp"), response.get("user_id"), response.get("user_name")]
                for question in sorted_questions:
                    row.append(response.get("answers", {}).get(question, ""))
                ws.append(row)

            virtual_file = io.BytesIO()
            wb.save(virtual_file)
            virtual_file.seek(0)

            file_name = f"application_{app_name}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.xlsx"
            await itx.followup.send("Here is your application data export:", file=discord.File(virtual_file, filename=file_name), ephemeral=True)

        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select an application to export responses:", view=view, ephemeral=True)

    @ticket_group.command(name="responses", description="View and delete application/survey responses.")
    async def ticket_responses(self, interaction: discord.Interaction):
        # Get all application and survey topics
        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
        app_survey_topics = {name: data for name, data in topics.items() if data.get('type') in ['application', 'survey']}

        if not app_survey_topics:
            return await interaction.response.send_message("No application or survey topics exist.", ephemeral=True)

        options = [discord.SelectOption(label=f"{data.get('label', name)[:90]} ({data.get('type')})", value=name[:100]) for name, data in app_survey_topics.items()][:25]

        view = discord.ui.View(timeout=300)
        select = discord.ui.Select(placeholder="Select a topic to manage responses...", options=options)

        async def topic_select_callback(itx: discord.Interaction):
            topic_name = select.values[0]
            await itx.response.defer(ephemeral=True)

            all_data = await _load_json(self.bot, SURVEY_DATA_FILE, self.survey_data_lock)
            responses = all_data.get(topic_name, [])

            if not responses:
                await itx.followup.send("No responses found for this topic.", ephemeral=True)
                return

            # Show responses with delete options
            embed = discord.Embed(title=f"Responses for {topic_name}", description=f"Total: {len(responses)} responses", color=discord.Color.blue())

            # Create options for each response (max 25)
            response_options = []
            for i, resp in enumerate(responses[:25]):
                user_name = resp.get('user_name', 'Unknown')
                timestamp = resp.get('timestamp', 'Unknown')[:10]  # Just date
                response_options.append(discord.SelectOption(
                    label=f"{user_name[:50]} - {timestamp}",
                    value=str(i),
                    description=f"User ID: {resp.get('user_id', 'N/A')}"
                ))

            if not response_options:
                await itx.followup.send("No responses to display.", ephemeral=True)
                return

            delete_view = discord.ui.View(timeout=300)
            delete_select = discord.ui.Select(
                placeholder="Select responses to delete...",
                options=response_options,
                min_values=1,
                max_values=len(response_options)
            )

            async def delete_callback(del_itx: discord.Interaction):
                await del_itx.response.defer(ephemeral=True)
                indices_to_remove = sorted([int(v) for v in delete_select.values], reverse=True)

                # Reload data to ensure we have latest
                current_data = await _load_json(self.bot, SURVEY_DATA_FILE, self.survey_data_lock)
                current_responses = current_data.get(topic_name, [])

                removed_count = 0
                for idx in indices_to_remove:
                    if 0 <= idx < len(current_responses):
                        current_responses.pop(idx)
                        removed_count += 1

                current_data[topic_name] = current_responses
                await _save_json(self.bot, SURVEY_DATA_FILE, current_data, self.survey_data_lock)

                await del_itx.followup.send(f"Deleted {removed_count} response(s). {len(current_responses)} remaining.", ephemeral=True)

            delete_select.callback = delete_callback
            delete_view.add_item(delete_select)

            # Add a "Delete All" button
            delete_all_btn = discord.ui.Button(label="Delete All Responses", style=discord.ButtonStyle.danger, row=1)

            async def delete_all_callback(del_all_itx: discord.Interaction):
                # Confirmation
                confirm_view = discord.ui.View(timeout=60)
                confirm_btn = discord.ui.Button(label="Confirm Delete All", style=discord.ButtonStyle.danger)
                cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

                async def confirm_del_all(c_itx: discord.Interaction):
                    await c_itx.response.defer(ephemeral=True)
                    current_data = await _load_json(self.bot, SURVEY_DATA_FILE, self.survey_data_lock)
                    current_data[topic_name] = []
                    await _save_json(self.bot, SURVEY_DATA_FILE, current_data, self.survey_data_lock)
                    await c_itx.followup.send(f"All responses for {topic_name} have been deleted.", ephemeral=True)

                async def cancel_del_all(c_itx: discord.Interaction):
                    await c_itx.response.edit_message(content="Deletion cancelled.", view=None)

                confirm_btn.callback = confirm_del_all
                cancel_btn.callback = cancel_del_all
                confirm_view.add_item(confirm_btn)
                confirm_view.add_item(cancel_btn)

                await del_all_itx.response.send_message(f"Are you sure you want to delete ALL {len(responses)} responses?", view=confirm_view, ephemeral=True)

            delete_all_btn.callback = delete_all_callback
            delete_view.add_item(delete_all_btn)

            await itx.followup.send(f"**{topic_name}** - {len(responses)} responses\nSelect responses to delete:", view=delete_view, ephemeral=True)

        select.callback = topic_select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a topic to manage responses:", view=view, ephemeral=True)

    async def _get_ticket_context(self, channel: Union[discord.TextChannel, discord.Thread]) -> Tuple[Optional[str], Optional[int]]:
        topic_str = ""
        if isinstance(channel, discord.TextChannel):
            topic_str = channel.topic or ""
        elif isinstance(channel, discord.Thread):
            try:
                async for msg in channel.history(limit=1, oldest_first=True):
                    if msg.embeds and msg.embeds[0].footer and msg.embeds[0].footer.text and "Ticket Topic:" in msg.embeds[0].footer.text:
                        topic_str = msg.embeds[0].footer.text
                        break
            except (discord.Forbidden, discord.HTTPException):
                return None, None
        
        topic_name_match = re.search(r'Ticket Topic: (\S+)', topic_str)
        opener_id_match = re.search(r'Opener: (\d+)', topic_str)

        topic_name = topic_name_match.group(1) if topic_name_match else None
        opener_id = int(opener_id_match.group(1)) if opener_id_match else None
        
        return topic_name, opener_id

    async def _handle_close_ticket(self, interaction: discord.Interaction, topic_name: str, opener_id: int):
        """Handle the close ticket logic with permission checks and close behavior."""
        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
        topic_data = topics.get(topic_name)

        user_is_opener = (interaction.user.id == opener_id)
        user_is_staff = False

        if topic_data:
            staff_role_ids = set(topic_data.get("staff_role_ids", []))
            user_role_ids = {role.id for role in interaction.user.roles}
            if staff_role_ids.intersection(user_role_ids):
                user_is_staff = True

        # Check member_can_close permission
        member_can_close = topic_data.get("member_can_close", True) if topic_data else True

        if user_is_opener and not member_can_close and not user_is_staff:
            return await interaction.followup.send("Members cannot close their own tickets for this topic. Please wait for staff.", ephemeral=True)

        if not user_is_opener and not user_is_staff:
            return await interaction.followup.send("You do not have permission to close this ticket.", ephemeral=True)

        delete_on_close = topic_data.get("delete_on_close", True) if topic_data else True

        confirm_view = discord.ui.View(timeout=60)
        confirm_btn = discord.ui.Button(label="Confirm Close", style=discord.ButtonStyle.danger)
        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

        async def confirm_callback(itx: discord.Interaction):
            await itx.response.defer()
            try:
                opener = await self.bot.fetch_user(opener_id)
                if opener and opener.id != itx.user.id:
                    await opener.send(f"Your ticket `{itx.channel.name}` in **{itx.guild.name}** has been closed by {itx.user.display_name}.")
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                pass

            try:
                if delete_on_close:
                    await itx.channel.delete(reason=f"Ticket closed by {itx.user} ({itx.user.id})")
                else:
                    # Archive the thread/channel instead of deleting
                    if isinstance(itx.channel, discord.Thread):
                        await itx.channel.edit(archived=True, locked=True, reason=f"Ticket closed by {itx.user} ({itx.user.id})")
                        await itx.followup.send("Ticket has been closed and archived.", ephemeral=True)
                    else:
                        # For text channels, we can't archive, so rename and lock
                        await itx.channel.edit(
                            name=f"closed-{itx.channel.name}"[:100],
                            overwrites={itx.guild.default_role: discord.PermissionOverwrite(send_messages=False)},
                            reason=f"Ticket closed by {itx.user} ({itx.user.id})"
                        )
                        await itx.followup.send("Ticket has been closed.", ephemeral=True)
            except discord.Forbidden:
                await itx.followup.send("I don't have permission to close this channel.", ephemeral=True)
            except discord.HTTPException as e:
                await itx.followup.send(f"Failed to close channel: {e}", ephemeral=True)

        async def cancel_callback(itx: discord.Interaction):
            await itx.response.edit_message(content="Ticket closure cancelled.", view=None)

        confirm_btn.callback = confirm_callback
        cancel_btn.callback = cancel_callback
        confirm_view.add_item(confirm_btn)
        confirm_view.add_item(cancel_btn)

        close_action = "deleted" if delete_on_close else "archived"
        await interaction.followup.send(f"Are you sure you want to close this ticket? The ticket will be {close_action}.", view=confirm_view, ephemeral=True)

    async def conduct_survey_flow(self, interaction: discord.Interaction, topic: Dict):
        user = interaction.user
        survey_name = topic.get('name', 'unknown')
        topic_type = topic.get('type', 'survey')

        # --- Rate Limit Check ---
        cooldown_minutes = topic.get('survey_cooldown_minutes', DEFAULT_SURVEY_COOLDOWN_MINUTES)
        user_survey_cooldowns = self.survey_cooldowns.get(user.id, {})
        last_submission = user_survey_cooldowns.get(survey_name)

        if last_submission:
            now = datetime.now(timezone.utc)
            time_since = now - last_submission.replace(tzinfo=timezone.utc)
            if time_since < timedelta(minutes=cooldown_minutes):
                remaining = last_submission.replace(tzinfo=timezone.utc) + timedelta(minutes=cooldown_minutes)
                cooldown_msg = f"You recently submitted this {topic_type}. Please try again {discord.utils.format_dt(remaining, style='R')}."
                try:
                    if interaction.response.is_done():
                        await interaction.followup.send(cooldown_msg, ephemeral=True)
                    else:
                        await interaction.response.send_message(cooldown_msg, ephemeral=True)
                except discord.HTTPException:
                    pass
                return

        # --- Check for existing session ---
        existing_session = self.active_survey_sessions.get(user.id)
        start_index = 0
        answers = {}

        # Include pre-modal answer if provided (from PreModalAnswerModal)
        pre_modal_answer = topic.pop('_pre_modal_user_answer', None)
        if pre_modal_answer:
            pre_modal_question = topic.get('pre_modal_answer_question', 'Pre-Question Response')
            answers[pre_modal_question] = pre_modal_answer

        # --- Channel mode: ephemeral Q&A in the same channel ---
        is_channel_mode = topic_type == 'application' and topic.get('application_channel_mode', 'dm') == 'channel'

        if is_channel_mode:
            # Clear any existing session (no resume for ephemeral)
            if user.id in self.active_survey_sessions:
                del self.active_survey_sessions[user.id]
                await self._save_survey_sessions()

            questions = topic.get('questions', [])[:25]
            if not questions:
                return

            # Q1: open modal directly from the interaction (no Answer button needed)
            first_modal = ResponseModal(title=f"Question 1/{len(questions)}")
            first_modal.answer.label = questions[0][:45]
            try:
                await interaction.response.send_modal(first_modal)
            except discord.HTTPException:
                return

            timed_out = await first_modal.wait()
            if timed_out or first_modal.answer.value is None:
                return

            answers[questions[0]] = first_modal.answer.value
            latest_interaction = first_modal.modal_interaction

            # Q2+: ephemeral followup with Answer button → modal
            for i in range(1, len(questions)):
                question_text = questions[i]
                answer_view = discord.ui.View(timeout=300)
                future = asyncio.get_running_loop().create_future()

                async def channel_answer_callback(button_interaction: discord.Interaction, idx=i, total=len(questions), fut=future):
                    modal = ResponseModal(title=f"Question {idx+1}/{total}")
                    await button_interaction.response.send_modal(modal)
                    await modal.wait()
                    if not fut.done():
                        fut.set_result(modal)

                answer_button = discord.ui.Button(label="Answer", style=discord.ButtonStyle.primary)
                answer_button.callback = channel_answer_callback
                answer_view.add_item(answer_button)

                embed = discord.Embed(
                    title=f"{topic.get('label')} ({i+1}/{len(questions)})",
                    description=question_text,
                    color=discord.Color.blue()
                )

                try:
                    await latest_interaction.followup.send(embed=embed, view=answer_view, ephemeral=True)
                except discord.HTTPException:
                    return

                try:
                    modal = await asyncio.wait_for(future, timeout=300)
                except asyncio.TimeoutError:
                    try:
                        await latest_interaction.followup.send("Application timed out. Please try again.", ephemeral=True)
                    except discord.HTTPException:
                        pass
                    return

                answers[question_text] = modal.answer.value
                latest_interaction = modal.modal_interaction

            # Send success as ephemeral followup
            success_embed = discord.Embed(
                title="Application Completed",
                description="Thank you for completing the application! Your responses have been submitted.",
                color=discord.Color.green()
            )
            try:
                await latest_interaction.followup.send(embed=success_embed, ephemeral=True)
            except discord.HTTPException:
                pass

        else:
            # --- DM mode (existing logic) ---
            if existing_session:
                existing_survey_name = existing_session.get('survey_name')

                # If user has a session for a DIFFERENT survey, warn them
                if existing_survey_name != survey_name:
                    try:
                        target_channel = user.dm_channel or await user.create_dm()
                        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                        old_topic = topics.get(existing_survey_name, {})
                        old_label = old_topic.get('label', existing_survey_name)

                        warn_embed = discord.Embed(
                            title="Abandon Previous Session?",
                            description=f"You have an incomplete **{old_label}** session.\n\nStarting **{topic.get('label')}** will abandon that session. Continue?",
                            color=discord.Color.orange()
                        )
                        warn_view = discord.ui.View(timeout=60)
                        continue_btn = discord.ui.Button(label="Continue", style=discord.ButtonStyle.danger)
                        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

                        user_choice = {"choice": None}

                        async def continue_cb(itx: discord.Interaction):
                            user_choice["choice"] = "continue"
                            await itx.response.defer()
                            warn_view.stop()

                        async def cancel_cb(itx: discord.Interaction):
                            user_choice["choice"] = "cancel"
                            await itx.response.defer()
                            warn_view.stop()

                        continue_btn.callback = continue_cb
                        cancel_btn.callback = cancel_cb
                        warn_view.add_item(continue_btn)
                        warn_view.add_item(cancel_btn)

                        warn_msg = await target_channel.send(embed=warn_embed, view=warn_view)
                        await warn_view.wait()

                        if user_choice["choice"] != "continue":
                            await warn_msg.edit(
                                embed=discord.Embed(title="Cancelled", description="No changes made to your sessions.", color=discord.Color.red()),
                                view=None
                            )
                            return

                        # Clear old session
                        del self.active_survey_sessions[user.id]
                        await self._save_survey_sessions()
                        await warn_msg.delete()
                        existing_session = None  # Treat as no session

                    except (discord.Forbidden, discord.HTTPException):
                        pass

            if existing_session and existing_session.get('survey_name') == survey_name:
                # User has an active session for this survey
                try:
                    target_channel = user.dm_channel or await user.create_dm()
                    progress = existing_session.get('current_question', 0)
                    total = len(topic.get('questions', []))

                    resume_embed = discord.Embed(
                        title=f"Resume {topic.get('label')}?",
                        description=f"You have an incomplete session (Question {progress + 1}/{total}).\n\nWould you like to **resume** where you left off, or **start over**?",
                        color=discord.Color.gold()
                    )
                    resume_view = ResumeOrRestartView(self, topic, existing_session, interaction)
                    resume_msg = await target_channel.send(embed=resume_embed, view=resume_view)

                    await resume_view.wait()

                    if resume_view.choice == "resume":
                        start_index = existing_session.get('current_question', 0)
                        answers = existing_session.get('answers', {})
                    elif resume_view.choice == "restart":
                        start_index = 0
                        answers = {}
                    else:  # cancel or timeout
                        await resume_msg.edit(
                            embed=discord.Embed(title="Cancelled", description="Survey cancelled.", color=discord.Color.red()),
                            view=None
                        )
                        return

                    await resume_msg.delete()
                except (discord.Forbidden, discord.HTTPException):
                    pass

            # --- Setup DM channel ---
            questions = topic.get('questions', [])[:25]

            try:
                target_channel = user.dm_channel or await user.create_dm()
            except (discord.Forbidden, discord.HTTPException):
                if not isinstance(interaction.channel, discord.DMChannel):
                    try:
                        await interaction.followup.send("I couldn't send you a DM. Please check your privacy settings.", ephemeral=True)
                    except discord.HTTPException:
                        pass
                return

            embed = discord.Embed(title=f"Starting: {topic.get('label')}", description="Please wait...", color=discord.Color.light_grey())
            try:
                flow_message = await target_channel.send(embed=embed)
            except (discord.Forbidden, discord.HTTPException):
                return

            # --- Create/update session ---
            session_data = {
                "survey_name": survey_name,
                "started_at": existing_session.get('started_at') if existing_session else datetime.now(timezone.utc).isoformat(),
                "current_question": start_index,
                "answers": answers,
                "flow_message_id": flow_message.id,
                "channel_id": target_channel.id
            }
            self.active_survey_sessions[user.id] = session_data
            await self._save_survey_sessions()

            # --- Question loop ---
            for i in range(start_index, len(questions)):
                question_text = questions[i]
                answer_view = discord.ui.View(timeout=300)
                future = asyncio.get_running_loop().create_future()

                async def answer_callback(button_interaction: discord.Interaction, idx=i, total=len(questions)):
                    modal = ResponseModal(title=f"Question {idx+1}/{total}")
                    await button_interaction.response.send_modal(modal)
                    await modal.wait()
                    if not future.done():
                        future.set_result(modal.answer.value)

                answer_button = discord.ui.Button(label="Answer", style=discord.ButtonStyle.primary)
                answer_button.callback = answer_callback
                answer_view.add_item(answer_button)

                embed = discord.Embed(
                    title=f"{topic.get('label')} ({i+1}/{len(questions)})",
                    description=question_text,
                    color=discord.Color.blue()
                )
                embed.set_footer(text="Your progress is saved. You can resume later if needed.")

                try:
                    await flow_message.edit(embed=embed, view=answer_view)
                except discord.HTTPException:
                    return

                try:
                    answer = await asyncio.wait_for(future, timeout=300)
                except asyncio.TimeoutError:
                    answer = None

                if answer is None:
                    # Session is preserved for resume
                    embed.color = discord.Color.orange()
                    embed.set_footer(text="Session paused. You can resume this survey later.")
                    for item in answer_view.children:
                        item.disabled = True
                    try:
                        await flow_message.edit(embed=embed, view=answer_view)
                    except discord.HTTPException:
                        pass
                    return

                answers[question_text] = answer

                # Update session after each answer
                self.active_survey_sessions[user.id] = {
                    "survey_name": survey_name,
                    "started_at": session_data["started_at"],
                    "current_question": i + 1,
                    "answers": answers,
                    "flow_message_id": flow_message.id,
                    "channel_id": target_channel.id
                }
                await self._save_survey_sessions()

        # --- Finalize and save ---
        results_embed = discord.Embed(
            title=f"New Response: {topic.get('label')}",
            description=f"Submitted by {user.mention} ({user.id})",
            color=discord.Color.teal(),
            timestamp=discord.utils.utcnow()
        )
        for question, answer in answers.items():
            results_embed.add_field(name=question[:256], value=answer[:1024], inline=False)

        # Add footer with topic name for ApprovalView to identify
        results_embed.set_footer(text=f"Topic: {survey_name}")

        all_survey_data = await _load_json(self.bot, SURVEY_DATA_FILE, self.survey_data_lock)
        survey_responses = all_survey_data.get(survey_name, [])
        survey_responses.append({
            "user_id": user.id, "user_name": str(user),
            "timestamp": datetime.now(timezone.utc).isoformat(), "answers": answers
        })
        all_survey_data[survey_name] = survey_responses
        await _save_json(self.bot, SURVEY_DATA_FILE, all_survey_data, self.survey_data_lock)

        log_channel_id = topic.get('log_channel_id')
        claim_enabled = topic.get('claim_enabled', False)

        if topic_type == 'application':
            # --- Application finalization: create discussion thread if enabled ---
            discussion_channel = None

            if topic.get('discussion_mode') or topic.get('claim_enabled', False):
                guild_id = topic.get('_guild_id')
                guild = self.bot.get_guild(guild_id) if guild_id else None
                member = guild.get_member(user.id) if guild else None

                if member and guild:
                    discussion_channel = await self._create_application_discussion(topic, member, guild)

            # Add thread link to the results embed
            if discussion_channel:
                results_embed.add_field(name="Discussion", value=discussion_channel.mention, inline=True)

            # Determine where to send and which view to attach
            alerts_channel_id = topic.get('claim_alerts_channel_id') if claim_enabled else None
            target_channel_id = alerts_channel_id or log_channel_id

            if claim_enabled and discussion_channel and member:
                topic_name = topic.get('name', 'unknown')
                view = ClaimAlertView(self.bot, topic_name, discussion_channel.id, member.id)
            elif topic.get('approval_mode'):
                view = ApprovalView(self.bot, topic)
            else:
                view = None

            if target_channel_id:
                target_channel = self.bot.get_channel(target_channel_id)
                if target_channel:
                    try:
                        await target_channel.send(embed=results_embed, view=view)
                    except discord.Forbidden:
                        logger.warning(f"Missing permissions for channel {target_channel_id}")

        elif log_channel_id:
            log_channel = self.bot.get_channel(log_channel_id)
            if log_channel:
                try:
                    await log_channel.send(embed=results_embed)
                except discord.Forbidden:
                    logger.warning(f"Missing permissions for survey log channel {log_channel_id}")
            else:
                logger.warning(f"Survey log channel {log_channel_id} not found.")

        # --- Clear session and set cooldown ---
        if user.id in self.active_survey_sessions:
            del self.active_survey_sessions[user.id]
            await self._save_survey_sessions()

        # Set cooldown for this survey
        if user.id not in self.survey_cooldowns:
            self.survey_cooldowns[user.id] = {}
        self.survey_cooldowns[user.id][survey_name] = datetime.now(timezone.utc)

        if not is_channel_mode:
            success_embed = discord.Embed(
                title=f"✅ {topic.get('label')} Completed",
                description="Thank you for completing the form! Your responses have been submitted.",
                color=discord.Color.green()
            )
            try:
                await flow_message.edit(embed=success_embed, view=None)
            except discord.HTTPException:
                pass

            if isinstance(interaction.channel, discord.DMChannel):
                try:
                    view = StartSurveyView(topic, self.bot)
                    for item in view.children:
                        item.disabled = True
                    await interaction.message.edit(view=view)
                except (discord.HTTPException, AttributeError):
                    pass


async def setup(bot: commands.Bot):
    cog = TicketSystem(bot)
    await bot.add_cog(cog)
