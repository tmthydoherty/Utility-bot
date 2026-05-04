import discord
import asyncio
import logging
from typing import Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..cog import TicketSystem

logger = logging.getLogger("ticketing_cog")


async def proceed_after_prequestions(cog: "TicketSystem", interaction: discord.Interaction, topic: Dict[str, Any]):
    """Unified logic to proceed after all pre-questions are answered Yes.
    Uses feature flags instead of type branching."""
    # If required answer is enabled, show that modal first
    if topic.get('pre_modal_answer_enabled', False):
        question = topic.get('pre_modal_answer_question', 'Please provide your information:')
        await interaction.response.send_modal(PreModalAnswerModal(cog, topic, question))
        return

    # If questions exist, start Q&A flow
    if topic.get('questions'):
        channel_mode = topic.get('application_channel_mode', 'dm') == 'channel'
        if channel_mode:
            # Channel mode sends a modal as its first response, so await directly
            await cog.conduct_survey_flow(interaction, topic)
        else:
            await interaction.response.send_message(
                "The form is starting in your DMs. Please check your direct messages.",
                ephemeral=True, delete_after=10
            )
            asyncio.create_task(cog.conduct_survey_flow(interaction, topic))
        return

    # Otherwise create a discussion channel (ticket-style)
    await interaction.response.defer(ephemeral=True, thinking=True)
    ch = await cog._create_discussion_channel(interaction, topic, interaction.user, is_ticket=True)
    if ch:
        cog.cooldowns[interaction.user.id] = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
        await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)


class PreModalAnswerModal(discord.ui.Modal):
    """Modal for collecting required answer before proceeding."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any], question: str):
        super().__init__(title="Required Information", timeout=300)
        self.cog = cog
        self.topic = topic
        self.answer_input = discord.ui.TextInput(
            label=question[:45],
            placeholder="Enter your answer here...",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500
        )
        self.add_item(self.answer_input)

    async def on_submit(self, interaction: discord.Interaction):
        from datetime import datetime, timezone
        user_answer = self.answer_input.value

        # If questions exist, pass the answer along and start Q&A
        if self.topic.get('questions'):
            channel_mode = self.topic.get('application_channel_mode', 'dm') == 'channel'
            if not channel_mode:
                await interaction.response.send_message(
                    "The form is starting in your DMs. Please check your direct messages.",
                    ephemeral=True, delete_after=10
                )
            self.topic['_pre_modal_user_answer'] = user_answer
            asyncio.create_task(self.cog.conduct_survey_flow(interaction, self.topic))
        else:
            # Create discussion channel with the answer
            await interaction.response.defer(ephemeral=True, thinking=True)
            ch = await self.cog._create_discussion_channel(
                interaction, self.topic, interaction.user, is_ticket=True, user_answer=user_answer
            )
            if ch:
                self.cog.cooldowns[interaction.user.id] = datetime.now(timezone.utc)
                await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)


class PreModalCheckView(discord.ui.View):
    """View shown before opening a modal to ask if user has required info ready."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any], interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.cog = cog
        self.topic = topic
        self.original_interaction = interaction

        yes_label = topic.get('pre_modal_yes_label', 'Yes, I have it')
        no_label = topic.get('pre_modal_no_label', 'No, I need to get it')

        yes_button = discord.ui.Button(label=yes_label, style=discord.ButtonStyle.success)
        yes_button.callback = self.yes_button_callback
        self.add_item(yes_button)

        no_button = discord.ui.Button(label=no_label, style=discord.ButtonStyle.secondary)
        no_button.callback = self.no_button_callback
        self.add_item(no_button)

    async def yes_button_callback(self, interaction: discord.Interaction):
        if self.topic.get('pre_modal_2_enabled', False):
            pre_question_2 = self.topic.get('pre_modal_2_question', 'Do you have your second requirement ready?')
            await interaction.response.edit_message(
                content=pre_question_2,
                view=PreModalCheckView2(self.cog, self.topic, interaction)
            )
            self.stop()
            return
        await proceed_after_prequestions(self.cog, interaction, self.topic)
        self.stop()

    async def no_button_callback(self, interaction: discord.Interaction):
        redirect_url = self.topic.get("pre_modal_redirect_url")
        redirect_channel_id = self.topic.get("pre_modal_redirect_channel_id")
        no_message = self.topic.get("pre_modal_no_message",
                                     "Please get what you need ready, then click the button below to continue.")
        ready_button_enabled = self.topic.get("pre_modal_ready_button_enabled", True)

        if redirect_channel_id:
            description = f"Please check out <#{redirect_channel_id}> for more information.\n\n{no_message}"
        elif redirect_url:
            description = f"[Click here to get what you need]({redirect_url})\n\n{no_message}"
        else:
            description = no_message

        embed = discord.Embed(description=description, color=discord.Color.blue())
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

        ready_label = topic.get('pre_modal_ready_button_label', "I'm ready now")
        ready_button = discord.ui.Button(label=ready_label, style=discord.ButtonStyle.success)
        ready_button.callback = self.ready_button_callback
        self.add_item(ready_button)

    async def ready_button_callback(self, interaction: discord.Interaction):
        if self.topic.get('pre_modal_2_enabled', False):
            pre_question_2 = self.topic.get('pre_modal_2_question', 'Do you have your second requirement ready?')
            await interaction.response.edit_message(
                content=pre_question_2, embed=None,
                view=PreModalCheckView2(self.cog, self.topic, interaction)
            )
            self.stop()
            return
        await proceed_after_prequestions(self.cog, interaction, self.topic)
        self.stop()


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
        await proceed_after_prequestions(self.cog, interaction, self.topic)
        self.stop()

    async def no_button_callback(self, interaction: discord.Interaction):
        redirect_url = self.topic.get("pre_modal_2_redirect_url")
        redirect_channel_id = self.topic.get("pre_modal_2_redirect_channel_id")
        no_message = self.topic.get("pre_modal_2_no_message",
                                     "Please get what you need ready, then click the button below to continue.")

        if redirect_channel_id:
            description = f"Please check out <#{redirect_channel_id}> for more information.\n\n{no_message}"
        elif redirect_url:
            description = f"[Click here to get what you need]({redirect_url})\n\n{no_message}"
        else:
            description = no_message

        embed = discord.Embed(description=description, color=discord.Color.blue())
        await interaction.response.edit_message(content=None, embed=embed,
                                                 view=PreModalReadyView2(self.cog, self.topic))
        self.stop()


class PreModalReadyView2(discord.ui.View):
    """View shown after user says No on second pre-question."""
    def __init__(self, cog: "TicketSystem", topic: Dict[str, Any]):
        super().__init__(timeout=300)
        self.cog = cog
        self.topic = topic

        ready_button_enabled = topic.get('pre_modal_2_ready_button_enabled', True)
        if ready_button_enabled:
            ready_label = topic.get('pre_modal_2_ready_button_label', "I'm ready now")
            ready_button = discord.ui.Button(label=ready_label, style=discord.ButtonStyle.success)
            ready_button.callback = self.ready_button_callback
            self.add_item(ready_button)

    async def ready_button_callback(self, interaction: discord.Interaction):
        await proceed_after_prequestions(self.cog, interaction, self.topic)
        self.stop()
