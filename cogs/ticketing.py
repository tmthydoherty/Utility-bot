import discord
from discord.ext import commands
from discord import app_commands
import json, os, asyncio, re
from typing import Dict, Any, List, Optional, TYPE_CHECKING, Union, Tuple
from datetime import datetime, timezone
import io

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
        return await asyncio.get_event_loop().run_in_executor(None, sync_load)

async def _save_json(bot: "Bot", file_path: str, data: Dict[str, Any], lock: asyncio.Lock):
    def sync_save():
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        sanitized_data = _sanitize_for_json(data)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(sanitized_data, f, indent=4)

    async with lock:
        await asyncio.get_event_loop().run_in_executor(None, sync_save)

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
    t.setdefault("button_color", "secondary")
    # New pre-modal question feature
    t.setdefault("pre_modal_enabled", False)
    t.setdefault("pre_modal_question", "Do you have your profile link ready?")
    t.setdefault("pre_modal_redirect_url", None)
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
class PreModalCheckView(discord.ui.View):
    """View shown before opening a modal to ask if user has required info ready."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any], interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.cog = cog
        self.topic = topic
        self.original_interaction = interaction
    
    @discord.ui.button(label="Yes, I have it", style=discord.ButtonStyle.success)
    async def yes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # User has the info ready, proceed to create ticket
        await interaction.response.defer(ephemeral=True, thinking=True)
        ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
        if ch:
            self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
            await interaction.followup.send(f"✅ Your ticket has been created: {ch.mention}", ephemeral=True)
        else:
            await interaction.followup.send("❌ Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)
        self.stop()
    
    @discord.ui.button(label="No, I need to get it", style=discord.ButtonStyle.secondary)
    async def no_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        redirect_url = self.topic.get("pre_modal_redirect_url")
        if redirect_url:
            embed = discord.Embed(
                description=f"[Click here to get what you need]({redirect_url})\n\nOnce you have it ready, click the button below.",
                color=discord.Color.blue()
            )
        else:
            embed = discord.Embed(
                description="Please get what you need ready, then click the button below to continue.",
                color=discord.Color.blue()
            )
        await interaction.response.edit_message(embed=embed, view=PreModalReadyView(self.cog, self.topic))
        self.stop()

class PreModalReadyView(discord.ui.View):
    """View shown after user says they need to get the info, with a 'Ready now' button."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any]):
        super().__init__(timeout=300)
        self.cog = cog
        self.topic = topic
    
    @discord.ui.button(label="I'm ready now", style=discord.ButtonStyle.success)
    async def ready_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        ch = await self.cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
        if ch:
            self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
            await interaction.followup.send(f"✅ Your ticket has been created: {ch.mention}", ephemeral=True)
        else:
            await interaction.followup.send("❌ Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)
        self.stop()

# ---------- Pre-Modal Configuration Modal ----------
class PreModalConfigModal(discord.ui.Modal, title="Configure Pre-Modal Question"):
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

    def __init__(self, view: "TopicWizardView"):
        super().__init__()
        self.view = view
        self.question.default = view.topic_data.get("pre_modal_question", "Do you have your profile link ready?")
        self.redirect_url.default = view.topic_data.get("pre_modal_redirect_url") or ""

    async def on_submit(self, itx: discord.Interaction):
        self.view.topic_data["pre_modal_question"] = self.question.value
        self.view.topic_data["pre_modal_redirect_url"] = self.redirect_url.value.strip() or None
        await self.view.update_message_state(itx, "✅ Pre-modal question configured.")

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
        
        if topic_type == 'ticket':
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
        
        if topic_type in ['application', 'survey']:
            self.add_item(self.create_button("Manage Questions", discord.ButtonStyle.blurple, self.manage_questions_callback, 3))
        
        if topic_type == 'application':
            approval_label = "Approval Buttons: On" if self.topic_data.get('approval_mode') else "Approval Buttons: Off"
            self.add_item(self.create_button(approval_label, discord.ButtonStyle.primary, self.toggle_approval_callback, 3))
            discussion_label = "Auto-Discussion: On" if self.topic_data.get('discussion_mode') else "Auto-Discussion: Off"
            self.add_item(self.create_button(discussion_label, discord.ButtonStyle.primary, self.toggle_discussion_callback, 3))

        # Pre-modal question button (only for tickets)
        if topic_type == 'ticket':
            pre_modal_enabled = self.topic_data.get('pre_modal_enabled', False)
            pre_modal_label = "Pre-Question: On" if pre_modal_enabled else "Pre-Question: Off"
            self.add_item(self.create_button(pre_modal_label, discord.ButtonStyle.primary, self.toggle_pre_modal_callback, 3))
            if pre_modal_enabled:
                self.add_item(self.create_button("Configure Pre-Question", discord.ButtonStyle.secondary, self.configure_pre_modal_callback, 3))

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
        elif topic_type == 'ticket':
            welcome_msg = self.topic_data.get('welcome_message', 'Default Message')
            # Truncate if too long
            if len(welcome_msg) > 1000:
                welcome_msg = welcome_msg[:997] + "..."
            embed.add_field(name="Welcome Message", value=f"```{welcome_msg}```", inline=False)
            
            # Show pre-modal question status
            pre_modal_enabled = self.topic_data.get('pre_modal_enabled', False)
            if pre_modal_enabled:
                pre_q = self.topic_data.get('pre_modal_question', 'Not set')
                pre_url = self.topic_data.get('pre_modal_redirect_url') or 'None'
                embed.add_field(name="Pre-Question", value=f"**Question:** {pre_q}\n**Redirect URL:** {pre_url}", inline=False)
            else:
                embed.add_field(name="Pre-Question", value="`Disabled`", inline=True)

        return embed

    async def toggle_pre_modal_callback(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_enabled'] = not self.topic_data.get('pre_modal_enabled', False)
        await self.update_message_state(interaction)

    async def configure_pre_modal_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalConfigModal(self))

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
        if topic_type in ['application', 'survey'] and not self.topic_data.get('log_channel_id'):
            await interaction.followup.send(f"❌ An {topic_type} requires a Log Channel to be set.", ephemeral=True)
            return
        if topic_type != 'survey' and not self.topic_data.get('parent_id'):
            await interaction.followup.send(f"❌ Please set a parent channel/category before saving.", ephemeral=True)
            return
        
        try:
            topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
            topics[self.topic_data['name']] = self.topic_data
            await _save_json(self.cog.bot, TOPICS_FILE, topics, self.cog.topics_lock)
        except Exception as e:
            print(f"[TICKETING ERROR] Failed to save topic: {e}")
            await interaction.followup.send(f"❌ An error occurred: `{e.__class__.__name__}`. Check console.", ephemeral=True)
            return

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
        print(f"[TICKETING ERROR] LabelModal error: {error}")

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
        print(f"[TICKETING ERROR] WelcomeMessageModal error: {error}")

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
        print(f"[TICKETING ERROR] AddQuestionModal error: {error}")

class QuestionManagerView(discord.ui.View):
    def __init__(self, parent_wizard: "TopicWizardView"):
        super().__init__(timeout=180)
        self.parent_wizard = parent_wizard
        self._update_remove_button_state()

    def _update_remove_button_state(self):
        """Safely update the remove button's disabled state."""
        has_questions = bool(self.parent_wizard.topic_data.get('questions', []))
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.label == "Remove Question":
                item.disabled = not has_questions
                break

    @discord.ui.button(label="Add Question", style=discord.ButtonStyle.success, row=0)
    async def add_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddQuestionModal(self.parent_wizard))

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
            self.stop()
            try:
                await original_msg_interaction.delete_original_response()
            except discord.NotFound:
                pass

        select.callback = select_callback
        picker_view.add_item(select)
        await interaction.response.send_message("Select questions to remove:", view=picker_view, ephemeral=True)

    @discord.ui.button(label="Done", style=discord.ButtonStyle.secondary, row=1)
    async def done(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        self.stop()
        try:
            await interaction.delete_original_response()
        except discord.NotFound:
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
            print(f"[TICKETING ERROR] Failed to save panel: {e}")
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
        print(f"[TICKETING ERROR] PanelTextModal error: {error}")

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
        print(f"[TICKETING ERROR] ImageUrlModal error: {error}")

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
        print(f"[TICKETING ERROR] ReasonModal error: {error}")

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
                print(f"[TICKETING] Failed to create post-approval discussion channel: {e}")

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
        await interaction.response.defer()

class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="close_ticket_button")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        cog = interaction.client.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An internal error occurred (Cog not found).", ephemeral=True)
        
        topic_name, opener_id = await cog._get_ticket_context(interaction.channel)

        if opener_id is None or topic_name is None:
            return await interaction.followup.send("Could not identify the ticket context. This ticket may be corrupted.", ephemeral=True)

        user_is_opener = (interaction.user.id == opener_id)
        user_is_staff = False

        topics = await _load_json(cog.bot, TOPICS_FILE, cog.topics_lock)
        topic_data = topics.get(topic_name)
        if topic_data:
            staff_role_ids = set(topic_data.get("staff_role_ids", []))
            user_role_ids = {role.id for role in interaction.user.roles}
            if staff_role_ids.intersection(user_role_ids):
                user_is_staff = True
        
        if not user_is_opener and not user_is_staff:
            return await interaction.followup.send("You do not have permission to close this ticket.", ephemeral=True)

        confirm_view = discord.ui.View(timeout=60)
        confirm_btn = discord.ui.Button(label="Confirm Close", style=discord.ButtonStyle.danger)
        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

        async def confirm_callback(itx: discord.Interaction):
            await itx.response.defer()
            try:
                opener = await cog.bot.fetch_user(opener_id)
                if opener and opener.id != itx.user.id:
                    await opener.send(f"Your ticket `{itx.channel.name}` in **{itx.guild.name}** has been closed by {itx.user.display_name}.")
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                pass

            try:
                await itx.channel.delete(reason=f"Ticket closed by {itx.user} ({itx.user.id})")
            except discord.Forbidden:
                await itx.followup.send("❌ I don't have permission to delete this channel.", ephemeral=True)
            except discord.HTTPException as e:
                await itx.followup.send(f"❌ Failed to delete channel: {e}", ephemeral=True)

        async def cancel_callback(itx: discord.Interaction):
            await itx.response.edit_message(content="Ticket closure cancelled.", view=None)

        confirm_btn.callback = confirm_callback
        cancel_btn.callback = cancel_callback
        confirm_view.add_item(confirm_btn)
        confirm_view.add_item(cancel_btn)

        await interaction.followup.send("Are you sure you want to close this ticket? This action cannot be undone.", view=confirm_view, ephemeral=True)

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

        topic_type = self.topic.get('type')

        if topic_type == 'ticket':
            user_id = interaction.user.id
            last_ticket_time = cog.cooldowns.get(user_id)
            if last_ticket_time and (datetime.now(timezone.utc) - last_ticket_time.replace(tzinfo=timezone.utc) < timedelta(minutes=5)):
                remaining = last_ticket_time.replace(tzinfo=timezone.utc) + timedelta(minutes=5)
                return await interaction.response.send_message(f"You are on cooldown. Please try again {discord.utils.format_dt(remaining, style='R')}.", ephemeral=True)
            
            # Check if pre-modal question is enabled
            if self.topic.get('pre_modal_enabled', False):
                pre_question = self.topic.get('pre_modal_question', 'Do you have your profile link ready?')
                await interaction.response.send_message(pre_question, view=PreModalCheckView(cog, self.topic, interaction), ephemeral=True)
            else:
                await interaction.response.defer(ephemeral=True, thinking=True)
                ch = await cog._create_discussion_channel(interaction, self.topic, interaction.user, is_ticket=True)
                if ch:
                    cog.cooldowns[user_id] = datetime.now(timezone.utc)
                    await interaction.followup.send(f"✅ Your ticket has been created: {ch.mention}", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Failed to create your ticket. The bot may be missing permissions or the parent category is misconfigured.", ephemeral=True)
        
        elif topic_type in ['application', 'survey']:
            if not self.topic.get('questions'):
                return await interaction.response.send_message(f"❌ This {topic_type} has no questions configured.", ephemeral=True)
            
            await interaction.response.send_message(f"The {topic_type} is starting in your DMs. Please check your direct messages.", ephemeral=True, delete_after=10)
            asyncio.create_task(cog.conduct_survey_flow(interaction, self.topic))

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
        print(f"[TICKETING ERROR] CreateSurveyNameModal error: {error}")

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
        self.cooldowns: Dict[int, datetime] = {}

    async def _create_discussion_channel(self, interaction: discord.Interaction, topic: Dict[str, Any], member: Union[discord.Member, discord.User], is_ticket: bool = False):
        try:
            guild = interaction.guild
            channel_name = f"{topic.get('name', 'ticket')}-{member.name}".replace(" ", "-").lower()[:100]
            parent_id = topic.get('parent_id')
            if not parent_id:
                raise ValueError("Parent not set.")
            parent = guild.get_channel(parent_id)
            if not parent:
                raise ValueError(f"Parent {parent_id} not found.")

            channel_topic_str = f"Ticket Topic: {topic.get('name', 'unknown')} | Opener: {member.id}"
            welcome_template = topic.get("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
            welcome_message = welcome_template.format(user=member.mention, topic=topic.get('label', 'N/A'))

            if topic.get('mode') == 'channel':
                if not isinstance(parent, discord.CategoryChannel):
                    raise ValueError("Parent must be a category.")
                if is_ticket:
                    existing = discord.utils.get(parent.text_channels, name=channel_name)
                    if existing:
                        if interaction.response.is_done():
                            await interaction.followup.send(f"⚠️ You already have a ticket: {existing.mention}", ephemeral=True)
                        else:
                            await interaction.response.send_message(f"⚠️ You already have a ticket: {existing.mention}", ephemeral=True)
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
                await new_channel.send(welcome_message, view=CloseTicketView())
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
                            await interaction.followup.send(f"⚠️ You already have a ticket: {existing.mention}", ephemeral=True)
                        else:
                            await interaction.response.send_message(f"⚠️ You already have a ticket: {existing.mention}", ephemeral=True)
                        return None

                ch = await parent.create_thread(name=channel_name, type=discord.ChannelType.private_thread)
                if is_ticket: 
                    await ch.add_user(member)
                    embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                    embed.set_footer(text=channel_topic_str)
                    await ch.send(embed=embed, view=CloseTicketView())

                for rid in topic.get("staff_role_ids", []):
                    role = guild.get_role(rid)
                    if role:
                        for m in role.members:
                            try:
                                await ch.add_user(m)
                            except discord.HTTPException:
                                pass
                return ch
        except Exception as e:
            error_msg = f"Error in _create_discussion_channel: {e}"
            print(f"[TICKETING ERROR] {error_msg}")
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
        print("✅ Persistent ticket, survey, and action views have been loaded.")

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
            view = TopicWizardView(self, interaction, topics[name], is_new=False)
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
                print(f"[TICKETING ERROR] Failed to send panel: {e}")
                await interaction.followup.send(f"❌ An error occurred: {e}", ephemeral=True)

    @survey_group.command(name="admin", description="Opens the survey administration panel.")
    async def survey_admin(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Survey Admin Panel",
            description="Use the buttons below to manage surveys.",
            color=discord.Color.teal()
        )
        await interaction.response.send_message(embed=embed, view=SurveyAdminPanelView(self), ephemeral=True)

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

    async def conduct_survey_flow(self, interaction: discord.Interaction, topic: Dict):
        user = interaction.user
        # Increase question limit to 25
        questions = topic.get('questions', [])[:25]
        answers = {}

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

        for i, question_text in enumerate(questions):
            answer_view = discord.ui.View(timeout=300)
            future = asyncio.get_event_loop().create_future()

            async def answer_callback(button_interaction: discord.Interaction, idx=i, total=len(questions)):
                modal = ResponseModal(title=f"Question {idx+1}/{total}")
                await button_interaction.response.send_modal(modal)
                await modal.wait()
                if not future.done():
                    future.set_result(modal.answer.value)

            answer_button = discord.ui.Button(label="Answer", style=discord.ButtonStyle.primary)
            answer_button.callback = answer_callback
            answer_view.add_item(answer_button)
            
            embed = discord.Embed(title=f"{topic.get('label')} ({i+1}/{len(questions)})", description=question_text, color=discord.Color.blue())
            try:
                await flow_message.edit(embed=embed, view=answer_view)
            except discord.HTTPException:
                return

            try:
                answer = await asyncio.wait_for(future, timeout=300)
            except asyncio.TimeoutError:
                answer = None

            if answer is None:
                embed.color = discord.Color.red()
                embed.set_footer(text="Form cancelled due to inactivity.")
                for item in answer_view.children:
                    item.disabled = True
                try:
                    await flow_message.edit(embed=embed, view=answer_view)
                except discord.HTTPException:
                    pass
                return

            answers[question_text] = answer

        # Finalize and save
        results_embed = discord.Embed(
            title=f"New Response: {topic.get('label')}",
            description=f"Submitted by {user.mention} ({user.id})",
            color=discord.Color.teal(),
            timestamp=discord.utils.utcnow()
        )
        for question, answer in answers.items():
            results_embed.add_field(name=question[:256], value=answer[:1024], inline=False)
        
        all_survey_data = await _load_json(self.bot, SURVEY_DATA_FILE, self.survey_data_lock)
        survey_responses = all_survey_data.get(topic['name'], [])
        survey_responses.append({
            "user_id": user.id, "user_name": str(user),
            "timestamp": datetime.now(timezone.utc).isoformat(), "answers": answers
        })
        all_survey_data[topic['name']] = survey_responses
        await _save_json(self.bot, SURVEY_DATA_FILE, all_survey_data, self.survey_data_lock)

        log_channel_id = topic.get('log_channel_id')
        if log_channel_id:
            log_channel = self.bot.get_channel(log_channel_id)
            if log_channel:
                try:
                    await log_channel.send(embed=results_embed)
                except discord.Forbidden:
                    print(f"[SURVEY ERROR] Missing permissions for log channel {log_channel_id}")
            else:
                print(f"[SURVEY ERROR] Log channel {log_channel_id} not found.")

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
