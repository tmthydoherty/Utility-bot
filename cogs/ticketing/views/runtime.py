import discord
import asyncio
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from discord.ext.commands import Bot
    from ..cog import TicketSystem

from ..storage import _load_json, TOPICS_FILE
from ..defaults import _ensure_topic_defaults
from .pre_modal import PreModalCheckView, PreModalAnswerModal

logger = logging.getLogger("ticketing_cog")


class ResponseModal(discord.ui.Modal):
    answer = discord.ui.TextInput(label="Your Response", style=discord.TextStyle.long, max_length=1024)

    def __init__(self, title: str):
        super().__init__(title=title, timeout=300)

    async def on_submit(self, interaction: discord.Interaction):
        self.modal_interaction = interaction
        await interaction.response.defer()


class PanelAction(discord.ui.Button):
    """Unified button/select handler for panel topic actions. Feature-flag based, not type-branched."""
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
        cog: "TicketSystem" = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.response.send_message("An error occurred. Please try again later.", ephemeral=True)

        # Load fresh topic data
        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic.get('name'))
        if not topic:
            return await interaction.response.send_message("This topic no longer exists.", ephemeral=True)
        topic = _ensure_topic_defaults(topic)

        user_id = interaction.user.id

        # --- Blacklist check ---
        if user_id in topic.get('blacklisted_user_ids', []):
            return await interaction.response.send_message("You are not allowed to use this.", ephemeral=True)

        # --- Unified cooldown check (admins bypass) ---
        is_admin = interaction.user.guild_permissions.administrator
        cooldown_minutes = topic.get('cooldown_minutes', 5)
        if not is_admin:
            last_time = cog.cooldowns.get(user_id)
            if last_time and (datetime.now(timezone.utc) - last_time.replace(tzinfo=timezone.utc) < timedelta(minutes=cooldown_minutes)):
                remaining = last_time.replace(tzinfo=timezone.utc) + timedelta(minutes=cooldown_minutes)
                return await interaction.response.send_message(
                    f"You are on cooldown. Please try again {discord.utils.format_dt(remaining, style='R')}.",
                    ephemeral=True
                )

            # Also check survey-style cooldowns for Q&A topics
            if topic.get('questions'):
                survey_name = topic.get('name')
                user_survey_cooldowns = cog.survey_cooldowns.get(user_id, {})
                last_submission = user_survey_cooldowns.get(survey_name)
                if last_submission:
                    time_since = datetime.now(timezone.utc) - last_submission.replace(tzinfo=timezone.utc)
                    if time_since < timedelta(minutes=cooldown_minutes):
                        remaining = last_submission.replace(tzinfo=timezone.utc) + timedelta(minutes=cooldown_minutes)
                        return await interaction.response.send_message(
                            f"You recently submitted this. Please try again {discord.utils.format_dt(remaining, style='R')}.",
                            ephemeral=True
                        )

        # --- Store guild_id for claim/discussion system ---
        topic['_guild_id'] = interaction.guild.id

        # --- Unified flow: feature flags, not type ---

        # Step 1: Pre-modal questions
        if topic.get('pre_modal_enabled', False):
            pre_question = topic.get('pre_modal_question', 'Do you have your profile link ready?')
            await interaction.response.send_message(
                pre_question,
                view=PreModalCheckView(cog, topic, interaction),
                ephemeral=True
            )
            return

        # Step 2: Standalone required question (no pre-modal flow)
        if topic.get('pre_modal_answer_enabled', False):
            question = topic.get('pre_modal_answer_question', 'Please provide your information:')
            await interaction.response.send_modal(PreModalAnswerModal(cog, topic, question))
            return

        # Step 3: If questions exist, start Q&A flow
        if topic.get('questions'):
            channel_mode = topic.get('application_channel_mode', 'dm') == 'channel'
            if channel_mode:
                await cog.conduct_survey_flow(interaction, topic)
            else:
                await interaction.response.send_message(
                    "The form is starting in your DMs. Please check your direct messages.",
                    ephemeral=True, delete_after=10
                )
                asyncio.create_task(cog.conduct_survey_flow(interaction, topic))
            return

        # Step 4: No questions, no pre-modal -> create ticket/discussion channel directly
        await interaction.response.defer(ephemeral=True, thinking=True)
        ch = await cog._create_discussion_channel(interaction, topic, interaction.user, is_ticket=True)
        if ch:
            cog.cooldowns[user_id] = datetime.now(timezone.utc)
            await interaction.followup.send(f"Your ticket has been created: {ch.mention}", ephemeral=True)


class CategoryAction(discord.ui.Button):
    """Button on a panel that opens an ephemeral with the category's topics."""
    def __init__(self, bot: "Bot", category_data: Dict[str, Any], panel_data: Dict[str, Any]):
        color_name = category_data.get("button_color", "primary")
        style = getattr(discord.ButtonStyle, color_name, discord.ButtonStyle.primary)
        cat_slug = category_data["_slug"]
        super().__init__(
            label=category_data.get("label", "Category")[:80],
            emoji=category_data.get("emoji"),
            style=style,
            custom_id=f"panel_category::{cat_slug[:40]}::{panel_data['name'][:40]}"
        )
        self.bot = bot
        self.category_data = category_data
        self.panel_data = panel_data

    async def callback(self, interaction: discord.Interaction):
        cog: "TicketSystem" = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.response.send_message("An error occurred. Please try again later.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        cat_topic_names = self.category_data.get("topic_names", [])
        cat_topics = [(n, _ensure_topic_defaults(topics[n])) for n in cat_topic_names if n in topics]

        if not cat_topics:
            return await interaction.response.send_message("No topics available in this category.", ephemeral=True)

        view = discord.ui.View(timeout=300)
        cat_display = self.category_data.get("display_mode", "buttons")

        if cat_display == "dropdown":
            options = [
                discord.SelectOption(
                    label=t.get("label", name)[:100],
                    value=name[:100],
                    emoji=t.get("emoji")
                ) for name, t in cat_topics
            ][:25]
            select = discord.ui.Select(placeholder="Select a topic...", options=options)
            select.callback = cog._make_select_callback(self.panel_data)
            view.add_item(select)
        else:
            for name, t_data in cat_topics:
                view.add_item(PanelAction(self.bot, t_data, self.panel_data))

        label = self.category_data.get("label", "Category")
        emoji = self.category_data.get("emoji") or ""
        prefix = f"{emoji} " if emoji else ""
        await interaction.response.send_message(
            f"{prefix}**{label}**\nSelect a topic below:",
            view=view,
            ephemeral=True
        )


class CloseTicketView(discord.ui.View):
    def __init__(self, topic_name: Optional[str] = None, opener_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.topic_name = topic_name
        self.opener_id = opener_id
        if topic_name and opener_id:
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.label == "Close Ticket":
                    item.custom_id = f"close_ticket_button::{topic_name}::{opener_id}"
                    break

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="close_ticket_button")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        cog = interaction.client.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An internal error occurred (Cog not found).", ephemeral=True)
        topic_name, opener_id = await cog._get_ticket_context(interaction.channel)
        if opener_id is None or topic_name is None:
            return await interaction.followup.send("Could not identify the ticket context.", ephemeral=True)
        await cog._handle_close_ticket(interaction, topic_name, opener_id)


class ReasonModal(discord.ui.Modal):
    reason = discord.ui.TextInput(
        label="Reason", style=discord.TextStyle.long,
        placeholder="Provide a reason for this decision...", required=True, max_length=512
    )

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
            await interaction.response.send_message("No staff roles are configured for this topic.", ephemeral=True)
            return False
        user_role_ids = {role.id for role in interaction.user.roles}
        if not staff_role_ids.intersection(user_role_ids):
            await interaction.response.send_message("You do not have permission to use these buttons.", ephemeral=True)
            return False
        return True

    async def finalize_decision(self, interaction: discord.Interaction, approved: bool, reason: Optional[str] = None):
        applicant_id, topic = await self._get_context(interaction)
        if not applicant_id or not topic:
            return await interaction.followup.send("Could not process the decision. Context missing.", ephemeral=True)

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
                dm_embed = discord.Embed(
                    title=f"Response for '{topic.get('label')}' {status}",
                    description=f"Your submission in **{interaction.guild.name}** has been reviewed.",
                    color=color
                )
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
            return await interaction.followup.send("Could not start discussion. Context missing.", ephemeral=True)

        try:
            applicant = self.bot.get_user(applicant_id) or await self.bot.fetch_user(applicant_id)
        except discord.NotFound:
            return await interaction.followup.send("Could not find the applicant.", ephemeral=True)

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
            await interaction.followup.send(f"Discussion started in {discussion_channel.mention}", ephemeral=True)
        else:
            await interaction.followup.send("Failed to start discussion channel.", ephemeral=True)


class ClaimAlertView(discord.ui.View):
    """View attached to claim alerts with a Claim button."""
    def __init__(self, bot: "Bot", topic_name: str, channel_id: int, opener_id: int,
                 qa_embed: Optional[discord.Embed] = None):
        super().__init__(timeout=None)
        self.bot = bot
        self.topic_name = topic_name
        self.channel_id = channel_id
        self.opener_id = opener_id
        self.qa_embed = qa_embed
        self.children[0].custom_id = f"claim_ticket::{topic_name}::{channel_id}::{opener_id}"

    async def claim_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An error occurred.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic_name)
        if not topic:
            return await interaction.followup.send("Topic no longer exists.", ephemeral=True)

        staff_role_ids = set(topic.get("staff_role_ids", []))
        claim_role_id = topic.get("claim_role_id")
        user_role_ids = {role.id for role in interaction.user.roles}

        has_permission = bool(staff_role_ids.intersection(user_role_ids))
        if claim_role_id and claim_role_id in user_role_ids:
            has_permission = True

        if not has_permission:
            return await interaction.followup.send("You don't have permission to claim this.", ephemeral=True)

        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            return await interaction.followup.send("The ticket channel no longer exists.", ephemeral=True)

        try:
            if isinstance(channel, discord.Thread):
                await channel.add_user(interaction.user)
            elif isinstance(channel, discord.TextChannel):
                await channel.set_permissions(interaction.user, view_channel=True, send_messages=True)
        except discord.Forbidden:
            return await interaction.followup.send("I don't have permission to add you to the channel.", ephemeral=True)

        original_embed = interaction.message.embeds[0]
        original_embed.set_footer(text=f"Claimed by {interaction.user.display_name}")
        original_embed.color = discord.Color.green()

        new_view = ClaimedTicketView(self.bot, self.topic_name, self.channel_id, self.opener_id)
        embeds_to_send = [original_embed]
        if self.qa_embed:
            embeds_to_send.append(self.qa_embed)

        await interaction.message.edit(embeds=embeds_to_send, view=new_view)
        await interaction.followup.send(f"You've claimed this ticket: {channel.mention}", ephemeral=True)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success, emoji="\u270b")
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
        for child in self.children:
            if child.label == "Join":
                child.custom_id = f"claim_join::{topic_name}::{channel_id}::{opener_id}"
            elif child.label == "Close":
                child.custom_id = f"claim_close::{topic_name}::{channel_id}::{opener_id}"

    async def join_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An error occurred.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic_name)
        if not topic:
            return await interaction.followup.send("Topic no longer exists.", ephemeral=True)

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
        await interaction.response.defer(ephemeral=True)
        cog = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.followup.send("An error occurred.", ephemeral=True)

        topics = await _load_json(self.bot, TOPICS_FILE, cog.topics_lock)
        topic = topics.get(self.topic_name)
        if not topic:
            return await interaction.followup.send("Topic no longer exists.", ephemeral=True)

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
            original_embed = interaction.message.embeds[0]
            original_embed.color = discord.Color.dark_grey()
            footer_text = original_embed.footer.text if original_embed.footer else ""
            original_embed.set_footer(text=f"{footer_text} | Closed by {interaction.user.display_name}")
            for item in self.children:
                item.disabled = True
            await interaction.message.edit(embed=original_embed, view=self)
            return await interaction.followup.send("The ticket channel was already deleted.", ephemeral=True)

        delete_on_close = topic.get("delete_on_close", True)

        # Send custom close message to opener
        close_msg_template = topic.get("close_message", "Your ticket `{channel}` in **{server}** has been closed by {closer}.")
        try:
            opener = await self.bot.fetch_user(self.opener_id)
            if opener:
                close_msg = close_msg_template.format(
                    channel=channel.name, server=interaction.guild.name,
                    closer=interaction.user.display_name, user=opener.display_name
                )
                await opener.send(close_msg)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException, KeyError):
            pass

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

        original_embed = interaction.message.embeds[0]
        original_embed.color = discord.Color.dark_grey()
        footer_text = original_embed.footer.text if original_embed.footer else ""
        original_embed.set_footer(text=f"{footer_text} | Closed by {interaction.user.display_name}")
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(embed=original_embed, view=self)

        close_action = "deleted" if delete_on_close else "archived"
        await interaction.followup.send(f"Ticket has been {close_action}.", ephemeral=True)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.primary, emoji="\u27a1\ufe0f")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.join_callback(interaction)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, emoji="\U0001f512")
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.close_callback(interaction)
