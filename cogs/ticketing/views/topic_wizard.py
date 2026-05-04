import discord
import asyncio
import logging
from typing import Dict, Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..cog import TicketSystem

from ..storage import _load_json, _save_json, TOPICS_FILE
from ..defaults import _ensure_topic_defaults

logger = logging.getLogger("ticketing_cog")

PAGE_NAMES = ["Identity", "Channels", "Staff & Claims", "Flow", "Behavior"]


class PaginatedTopicWizardView(discord.ui.View):
    """5-page wizard for editing topic settings. All features available to all types."""

    def __init__(self, cog: "TicketSystem", interaction: discord.Interaction,
                 topic_data: Dict[str, Any], is_new: bool):
        super().__init__(timeout=600)
        self.cog = cog
        self.original_interaction = interaction
        self.topic_data = topic_data
        self.is_new = is_new
        self.page = 0
        self.wizard_message = None
        self.subpage = None
        self._guild = None
        self.update_components()

    # --- Helpers ---

    def _btn(self, label: str, style: discord.ButtonStyle, callback, row: int,
             disabled: bool = False) -> discord.ui.Button:
        btn = discord.ui.Button(label=label, style=style, row=row, disabled=disabled)
        btn.callback = callback
        return btn

    async def _refresh(self, interaction: discord.Interaction, ephemeral_msg: Optional[str] = None):
        """Called from modal on_submit callbacks. Defers the interaction and updates the wizard."""
        if not interaction.response.is_done():
            await interaction.response.defer()
        self.update_components()
        try:
            if self.wizard_message:
                await self.wizard_message.edit(embed=self._embed(), view=self)
            else:
                await self.original_interaction.edit_original_response(embed=self._embed(), view=self)
        except discord.NotFound:
            pass

    async def _edit(self, interaction: discord.Interaction):
        """Update the wizard message via a component interaction (button/select)."""
        self.update_components()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    # --- Component Builder ---

    def update_components(self):
        self.clear_items()
        if self.subpage:
            self._build_subpage()
            return
        builders = [
            self._page_identity,
            self._page_channels,
            self._page_staff,
            self._page_flow,
            self._page_behavior,
        ]
        builders[self.page]()
        self._nav_row()
        self._action_row()

    # Page 0: Identity
    def _page_identity(self):
        self.add_item(self._btn("Edit Label/Emoji", discord.ButtonStyle.secondary, self.edit_label_cb, 0))
        self.add_item(self._btn("Set Button Color", discord.ButtonStyle.secondary, self.set_button_color_cb, 0))

        topic_type = self.topic_data.get('type', 'ticket')
        self.add_item(self._btn(f"Type: {topic_type.capitalize()}", discord.ButtonStyle.primary, self.toggle_type_cb, 1))

        cooldown = self.topic_data.get('cooldown_minutes', 5)
        self.add_item(self._btn(f"Cooldown: {cooldown}min", discord.ButtonStyle.secondary, self.set_cooldown_cb, 1))

        numbering = self.topic_data.get('use_numbering', False)
        self.add_item(self._btn(
            f"Numbering: {'On' if numbering else 'Off'}",
            discord.ButtonStyle.primary, self.toggle_numbering_cb, 2
        ))

        fmt = self.topic_data.get('channel_name_format')
        fmt_label = "Name Format: Custom" if fmt else "Name Format: Default"
        self.add_item(self._btn(fmt_label, discord.ButtonStyle.secondary, self.set_name_format_cb, 2))

    # Page 1: Channels
    def _page_channels(self):
        mode = self.topic_data.get('mode', 'thread')
        self.add_item(self._btn(f"Mode: {mode.capitalize()}", discord.ButtonStyle.primary, self.toggle_mode_cb, 0))

        parent_label = "Set Parent Category" if mode == 'channel' else "Set Parent Channel"
        self.add_item(self._btn(parent_label, discord.ButtonStyle.secondary, self.set_parent_cb, 0))

        self.add_item(self._btn("Set Log Channel", discord.ButtonStyle.secondary, self.set_log_cb, 1))

        chan_mode = self.topic_data.get('application_channel_mode', 'dm')
        self.add_item(self._btn(
            f"Q&A Mode: {'Channel' if chan_mode == 'channel' else 'DMs'}",
            discord.ButtonStyle.primary, self.toggle_qa_mode_cb, 1
        ))

    # Page 2: Staff & Claims
    def _page_staff(self):
        self.add_item(self._btn("Manage Staff Roles", discord.ButtonStyle.blurple, self.manage_staff_cb, 0))

        ping = self.topic_data.get('ping_staff_on_create', False)
        self.add_item(self._btn(
            f"Staff Notify: {'Ping' if ping else 'Silent'}",
            discord.ButtonStyle.primary, self.toggle_ping_cb, 0
        ))

        claim = self.topic_data.get('claim_enabled', False)
        self.add_item(self._btn(f"Claim: {'On' if claim else 'Off'}", discord.ButtonStyle.primary, self.toggle_claim_cb, 1))

        if claim:
            self.add_item(self._btn("Set Alerts Channel", discord.ButtonStyle.secondary, self.set_claim_alerts_cb, 1))
            self.add_item(self._btn("Set Join Role", discord.ButtonStyle.secondary, self.set_claim_role_cb, 2))

        bl_count = len(self.topic_data.get('blacklisted_user_ids', []))
        self.add_item(self._btn(f"Blacklist ({bl_count})", discord.ButtonStyle.secondary, self.manage_blacklist_cb, 2))

    # Page 3: Flow
    def _page_flow(self):
        self.add_item(self._btn("Set Welcome Message", discord.ButtonStyle.secondary, self.set_welcome_cb, 0))
        self.add_item(self._btn("Manage Questions", discord.ButtonStyle.blurple, self.manage_questions_cb, 0))

        pre1 = self.topic_data.get('pre_modal_enabled', False)
        self.add_item(self._btn(f"Pre-Q 1: {'On' if pre1 else 'Off'}", discord.ButtonStyle.primary, self.toggle_preq1_cb, 1))
        if pre1:
            self.add_item(self._btn("Config Q1", discord.ButtonStyle.secondary, self.config_preq1_cb, 1))
            self.add_item(self._btn("Config Q1 No", discord.ButtonStyle.secondary, self.config_preq1_no_cb, 1))

            pre2 = self.topic_data.get('pre_modal_2_enabled', False)
            self.add_item(self._btn(f"Pre-Q 2: {'On' if pre2 else 'Off'}", discord.ButtonStyle.primary, self.toggle_preq2_cb, 2))
            if pre2:
                self.add_item(self._btn("Config Q2", discord.ButtonStyle.secondary, self.config_preq2_cb, 2))
                self.add_item(self._btn("Config Q2 No", discord.ButtonStyle.secondary, self.config_preq2_no_cb, 2))

    # Page 4: Behavior
    def _page_behavior(self):
        approval = self.topic_data.get('approval_mode', False)
        self.add_item(self._btn(
            f"Approval: {'On' if approval else 'Off'}",
            discord.ButtonStyle.primary, self.toggle_approval_cb, 0
        ))

        discussion = self.topic_data.get('discussion_mode', False)
        self.add_item(self._btn(
            f"Auto-Discussion: {'On' if discussion else 'Off'}",
            discord.ButtonStyle.primary, self.toggle_discussion_cb, 0
        ))

        delete_on_close = self.topic_data.get('delete_on_close', True)
        self.add_item(self._btn(
            f"On Close: {'Delete' if delete_on_close else 'Archive'}",
            discord.ButtonStyle.secondary, self.toggle_delete_close_cb, 1
        ))

        member_close = self.topic_data.get('member_can_close', True)
        self.add_item(self._btn(
            f"Member Close: {'Yes' if member_close else 'No'}",
            discord.ButtonStyle.secondary, self.toggle_member_close_cb, 1
        ))

        answer = self.topic_data.get('pre_modal_answer_enabled', False)
        self.add_item(self._btn(
            f"Opener Question: {'On' if answer else 'Off'}",
            discord.ButtonStyle.primary, self.toggle_answer_cb, 2
        ))
        if answer:
            self.add_item(self._btn("Edit Question", discord.ButtonStyle.secondary, self.config_answer_cb, 2))

        self.add_item(self._btn("Set Close Message", discord.ButtonStyle.secondary, self.set_close_message_cb, 2))

    # Navigation
    def _nav_row(self):
        self.add_item(self._btn("\u25c0 Prev", discord.ButtonStyle.secondary, self.prev_page_cb, 3,
                                disabled=(self.page == 0)))
        self.add_item(self._btn(f"[{self.page + 1}/5: {PAGE_NAMES[self.page]}]",
                                discord.ButtonStyle.secondary, self._noop, 3, disabled=True))
        self.add_item(self._btn("Next \u25b6", discord.ButtonStyle.secondary, self.next_page_cb, 3,
                                disabled=(self.page == 4)))

    def _action_row(self):
        self.add_item(self._btn("Finish & Save", discord.ButtonStyle.success, self.finish_cb, 4))
        self.add_item(self._btn("Cancel", discord.ButtonStyle.danger, self.cancel_cb, 4))

    async def _noop(self, interaction: discord.Interaction):
        await interaction.response.defer()

    # --- Subpage Builder ---

    def _build_subpage(self):
        sp = self.subpage
        sp_type = sp["type"]

        if sp_type == "channel_picker":
            select = discord.ui.ChannelSelect(
                placeholder="Select a channel...",
                channel_types=sp["channel_types"],
                row=0
            )
            key = sp["key"]

            async def _pick(itx: discord.Interaction):
                self.topic_data[key] = int(itx.data['values'][0])
                self.subpage = None
                await self._edit(itx)

            select.callback = _pick
            self.add_item(select)

        elif sp_type == "color_picker":
            select = discord.ui.Select(
                placeholder="Choose a button color...",
                options=[
                    discord.SelectOption(label="Blurple", value="primary"),
                    discord.SelectOption(label="Grey", value="secondary"),
                    discord.SelectOption(label="Green", value="success"),
                    discord.SelectOption(label="Red", value="danger"),
                ],
                row=0
            )

            async def _pick(itx: discord.Interaction):
                self.topic_data['button_color'] = itx.data['values'][0]
                self.subpage = None
                await self._edit(itx)

            select.callback = _pick
            self.add_item(select)

        elif sp_type == "role_picker":
            select = discord.ui.RoleSelect(placeholder="Select a role...", max_values=1, row=0)
            key = sp["key"]

            async def _pick(itx: discord.Interaction):
                self.topic_data[key] = int(itx.data['values'][0])
                self.subpage = None
                await self._edit(itx)

            select.callback = _pick
            self.add_item(select)

        elif sp_type == "staff_manager":
            add_select = discord.ui.RoleSelect(placeholder="Select roles to add...", max_values=25, row=0)

            async def _add(itx: discord.Interaction):
                current = set(self.topic_data.get('staff_role_ids', []))
                for role in add_select.values:
                    current.add(role.id)
                self.topic_data['staff_role_ids'] = list(current)
                self._guild = itx.guild
                await self._edit(itx)

            add_select.callback = _add
            self.add_item(add_select)

            current_ids = self.topic_data.get('staff_role_ids', [])
            if current_ids and self._guild:
                options = []
                for rid in current_ids[:25]:
                    role = self._guild.get_role(rid)
                    label = role.name if role else f"Unknown ({rid})"
                    options.append(discord.SelectOption(label=label[:100], value=str(rid)))
                if options:
                    remove_select = discord.ui.Select(
                        placeholder="Select roles to remove...",
                        options=options,
                        min_values=1, max_values=len(options),
                        row=1
                    )

                    async def _remove(itx: discord.Interaction):
                        to_remove = {int(v) for v in itx.data['values']}
                        self.topic_data['staff_role_ids'] = [
                            rid for rid in self.topic_data.get('staff_role_ids', []) if rid not in to_remove
                        ]
                        self._guild = itx.guild
                        await self._edit(itx)

                    remove_select.callback = _remove
                    self.add_item(remove_select)

        elif sp_type == "question_manager":
            self.add_item(self._btn("Add Question", discord.ButtonStyle.success, self._qm_add_cb, 0))
            questions = self.topic_data.get('questions', [])
            if questions:
                edit_options = [discord.SelectOption(label=q[:100], value=str(i))
                                for i, q in enumerate(questions)][:25]
                edit_select = discord.ui.Select(
                    placeholder="Select a question to edit...",
                    options=edit_options,
                    row=1
                )

                async def _edit_q(itx: discord.Interaction):
                    idx = int(itx.data['values'][0])
                    await itx.response.send_modal(EditQuestionModal(self, idx))

                edit_select.callback = _edit_q
                self.add_item(edit_select)

                remove_options = [discord.SelectOption(label=q[:100], value=str(i))
                                  for i, q in enumerate(questions)][:25]
                remove_select = discord.ui.Select(
                    placeholder="Select questions to remove...",
                    options=remove_options,
                    min_values=1, max_values=len(remove_options),
                    row=2
                )

                async def _remove_q(itx: discord.Interaction):
                    indices = sorted([int(v) for v in itx.data['values']], reverse=True)
                    for idx in indices:
                        if 0 <= idx < len(self.topic_data['questions']):
                            self.topic_data['questions'].pop(idx)
                    await self._edit(itx)

                remove_select.callback = _remove_q
                self.add_item(remove_select)

        elif sp_type == "blacklist_manager":
            add_select = discord.ui.UserSelect(placeholder="Select users to blacklist...", max_values=25, row=0)

            async def _add_bl(itx: discord.Interaction):
                current = set(self.topic_data.get('blacklisted_user_ids', []))
                for user in add_select.values:
                    current.add(user.id)
                self.topic_data['blacklisted_user_ids'] = list(current)
                await self._edit(itx)

            add_select.callback = _add_bl
            self.add_item(add_select)

            if self.topic_data.get('blacklisted_user_ids'):
                async def _clear_bl(itx: discord.Interaction):
                    self.topic_data['blacklisted_user_ids'] = []
                    await self._edit(itx)

                self.add_item(self._btn("Clear Blacklist", discord.ButtonStyle.danger, _clear_bl, 1))

        # Back button for all subpages
        self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._subpage_back_cb, 4))

    async def _subpage_back_cb(self, interaction: discord.Interaction):
        self.subpage = None
        await self._edit(interaction)

    async def _qm_add_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(AddQuestionModal(self))

    # --- Embed ---

    def _embed(self) -> discord.Embed:
        if self.subpage:
            return self._subpage_embed()
        return self._wizard_embed()

    def _subpage_embed(self) -> discord.Embed:
        sp = self.subpage
        sp_type = sp["type"]
        topic_name = self.topic_data.get('name', '')
        base = f"`{topic_name}` \u2014 "

        if sp_type == "channel_picker":
            return discord.Embed(title=base + sp.get("label", "Select Channel"), color=discord.Color.blue())
        elif sp_type == "color_picker":
            current = self.topic_data.get('button_color', 'secondary').capitalize()
            embed = discord.Embed(title=base + "Button Color", color=discord.Color.blue())
            embed.description = f"Current: **{current}**"
            return embed
        elif sp_type == "role_picker":
            return discord.Embed(title=base + sp.get("label", "Select Role"), color=discord.Color.blue())
        elif sp_type == "staff_manager":
            roles = ", ".join([f"<@&{rid}>" for rid in self.topic_data.get('staff_role_ids', [])]) or "*None*"
            embed = discord.Embed(title=base + "Staff Roles", color=discord.Color.blue())
            embed.description = f"**Current:** {roles}\n\nUse the dropdowns to add or remove roles."
            return embed
        elif sp_type == "question_manager":
            questions = self.topic_data.get('questions', [])
            q_text = "\n".join([f"`{i+1}.` {q}" for i, q in enumerate(questions)]) or "*No questions yet.*"
            embed = discord.Embed(title=base + "Questions", color=discord.Color.blue())
            embed.description = q_text[:4000]
            return embed
        elif sp_type == "blacklist_manager":
            bl = self.topic_data.get('blacklisted_user_ids', [])
            bl_text = ", ".join([f"<@{uid}>" for uid in bl]) or "*None*"
            embed = discord.Embed(title=base + "Blacklist", color=discord.Color.blue())
            embed.description = f"**Current:** {bl_text}\n\nUse the dropdown to add users."
            return embed
        return discord.Embed(title="Configuration", color=discord.Color.blue())

    def _wizard_embed(self) -> discord.Embed:
        action = "Creating" if self.is_new else "Editing"
        topic_type = self.topic_data.get('type', 'ticket')
        embed = discord.Embed(
            title=f"{action} {topic_type.capitalize()}: `{self.topic_data['name']}`",
            color=discord.Color.blue()
        )

        # Identity
        embed.add_field(name="Label",
                        value=f"{self.topic_data.get('emoji') or ''} {self.topic_data.get('label')}", inline=True)
        embed.add_field(name="Type", value=topic_type.capitalize(), inline=True)
        embed.add_field(name="Button Color",
                        value=f"`{self.topic_data.get('button_color', 'secondary').capitalize()}`", inline=True)
        embed.add_field(name="Cooldown",
                        value=f"{self.topic_data.get('cooldown_minutes', 5)} min", inline=True)
        embed.add_field(name="Numbering",
                        value="On" if self.topic_data.get('use_numbering') else "Off", inline=True)
        fmt = self.topic_data.get('channel_name_format')
        embed.add_field(name="Name Format",
                        value=f"`{fmt}`" if fmt else "`Default`", inline=True)

        # Channels
        log_id = self.topic_data.get('log_channel_id')
        embed.add_field(name="Log Channel", value=f"<#{log_id}>" if log_id else "`Not set`", inline=True)

        parent_id = self.topic_data.get('parent_id')
        embed.add_field(name="Parent", value=f"<#{parent_id}>" if parent_id else "`Not set`", inline=True)
        embed.add_field(name="Mode", value=self.topic_data.get('mode', 'thread').capitalize(), inline=True)

        chan_mode = self.topic_data.get('application_channel_mode', 'dm')
        embed.add_field(name="Q&A Mode", value="In Channel" if chan_mode == 'channel' else "In DMs", inline=True)

        # Staff
        staff_roles = ", ".join([f"<@&{rid}>" for rid in self.topic_data.get('staff_role_ids', [])]) or "`None`"
        embed.add_field(name="Staff Roles", value=staff_roles, inline=False)

        ping = self.topic_data.get('ping_staff_on_create', False)
        embed.add_field(name="Staff Notify", value="Ping" if ping else "Silent", inline=True)

        claim = self.topic_data.get('claim_enabled', False)
        if claim:
            alerts_id = self.topic_data.get('claim_alerts_channel_id')
            alerts_text = f"<#{alerts_id}>" if alerts_id else "`Not set`"
            role_id = self.topic_data.get('claim_role_id')
            role_text = f"<@&{role_id}>" if role_id else "`Staff roles`"
            embed.add_field(name="Claim", value=f"On | Alerts: {alerts_text} | Role: {role_text}", inline=False)
        else:
            embed.add_field(name="Claim", value="Off", inline=True)

        # Questions
        questions = self.topic_data.get('questions', [])
        q_text = "\n".join([f"\u2022 {q}" for q in questions]) or "`No questions set.`"
        embed.add_field(name=f"Questions ({len(questions)})", value=q_text[:1024], inline=False)

        # Welcome message
        welcome_msg = self.topic_data.get('welcome_message', 'Default Message')
        if len(welcome_msg) > 200:
            welcome_msg = welcome_msg[:197] + "..."
        embed.add_field(name="Welcome Message", value=f"```{welcome_msg}```", inline=False)

        # Pre-questions
        pre1 = self.topic_data.get('pre_modal_enabled', False)
        if pre1:
            pre_q = self.topic_data.get('pre_modal_question', 'Not set')
            pre_ch = self.topic_data.get('pre_modal_redirect_channel_id')
            redirect = f"<#{pre_ch}>" if pre_ch else (self.topic_data.get('pre_modal_redirect_url') or 'None')
            embed.add_field(name="Pre-Q 1", value=f"{pre_q}\nRedirect: {redirect}", inline=False)

            pre2 = self.topic_data.get('pre_modal_2_enabled', False)
            if pre2:
                pre_q2 = self.topic_data.get('pre_modal_2_question', 'Not set')
                pre_ch2 = self.topic_data.get('pre_modal_2_redirect_channel_id')
                redirect2 = f"<#{pre_ch2}>" if pre_ch2 else (self.topic_data.get('pre_modal_2_redirect_url') or 'None')
                embed.add_field(name="Pre-Q 2", value=f"{pre_q2}\nRedirect: {redirect2}", inline=False)
        else:
            embed.add_field(name="Pre-Questions", value="`Disabled`", inline=True)

        # Behavior
        embed.add_field(name="Approval", value="On" if self.topic_data.get('approval_mode') else "Off", inline=True)
        embed.add_field(name="Auto-Discussion", value="On" if self.topic_data.get('discussion_mode') else "Off", inline=True)
        embed.add_field(name="On Close", value="Delete" if self.topic_data.get('delete_on_close', True) else "Archive", inline=True)
        embed.add_field(name="Member Close", value="Yes" if self.topic_data.get('member_can_close', True) else "No", inline=True)

        answer = self.topic_data.get('pre_modal_answer_enabled', False)
        embed.add_field(name="Opener Question", value="On" if answer else "Off", inline=True)

        bl_count = len(self.topic_data.get('blacklisted_user_ids', []))
        embed.add_field(name="Blacklist", value=f"{bl_count} users", inline=True)

        return embed

    # --- Page Navigation ---

    async def prev_page_cb(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        await self._edit(interaction)

    async def next_page_cb(self, interaction: discord.Interaction):
        self.page = min(4, self.page + 1)
        await self._edit(interaction)

    # --- Page 0: Identity callbacks ---

    async def edit_label_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(LabelModal(self))

    async def set_button_color_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "color_picker"}
        await self._edit(interaction)

    async def toggle_type_cb(self, interaction: discord.Interaction):
        cycle = {'ticket': 'application', 'application': 'survey', 'survey': 'ticket'}
        self.topic_data['type'] = cycle.get(self.topic_data.get('type', 'ticket'), 'ticket')
        await self._edit(interaction)

    async def set_cooldown_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CooldownModal(self))

    async def toggle_numbering_cb(self, interaction: discord.Interaction):
        self.topic_data['use_numbering'] = not self.topic_data.get('use_numbering', False)
        await self._edit(interaction)

    async def set_name_format_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ChannelNameFormatModal(self))

    # --- Page 1: Channels callbacks ---

    async def toggle_mode_cb(self, interaction: discord.Interaction):
        self.topic_data['mode'] = 'channel' if self.topic_data.get('mode') == 'thread' else 'thread'
        await self._edit(interaction)

    async def set_parent_cb(self, interaction: discord.Interaction):
        mode = self.topic_data.get('mode', 'thread')
        if mode == 'channel':
            channel_types = [discord.ChannelType.category]
        else:
            channel_types = [discord.ChannelType.text]
        self.subpage = {"type": "channel_picker", "key": "parent_id",
                        "channel_types": channel_types, "label": "Select Parent"}
        await self._edit(interaction)

    async def set_log_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "channel_picker", "key": "log_channel_id",
                        "channel_types": [discord.ChannelType.text], "label": "Select Log Channel"}
        await self._edit(interaction)

    async def toggle_qa_mode_cb(self, interaction: discord.Interaction):
        current = self.topic_data.get('application_channel_mode', 'dm')
        self.topic_data['application_channel_mode'] = 'channel' if current == 'dm' else 'dm'
        await self._edit(interaction)

    # --- Page 2: Staff & Claims callbacks ---

    async def manage_staff_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "staff_manager"}
        self._guild = interaction.guild
        await self._edit(interaction)

    async def toggle_ping_cb(self, interaction: discord.Interaction):
        self.topic_data['ping_staff_on_create'] = not self.topic_data.get('ping_staff_on_create', False)
        await self._edit(interaction)

    async def toggle_claim_cb(self, interaction: discord.Interaction):
        self.topic_data['claim_enabled'] = not self.topic_data.get('claim_enabled', False)
        await self._edit(interaction)

    async def set_claim_alerts_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "channel_picker", "key": "claim_alerts_channel_id",
                        "channel_types": [discord.ChannelType.text], "label": "Select Alerts Channel"}
        await self._edit(interaction)

    async def set_claim_role_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "role_picker", "key": "claim_role_id", "label": "Select Claim Role"}
        await self._edit(interaction)

    async def manage_blacklist_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "blacklist_manager"}
        await self._edit(interaction)

    # --- Page 3: Flow callbacks ---

    async def set_welcome_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(WelcomeMessageModal(self))

    async def manage_questions_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "question_manager"}
        await self._edit(interaction)

    async def toggle_preq1_cb(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_enabled'] = not self.topic_data.get('pre_modal_enabled', False)
        await self._edit(interaction)

    async def config_preq1_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalConfigModal(self, question_num=1))

    async def config_preq1_no_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalNoResponseConfigModal(self, question_num=1))

    async def toggle_preq2_cb(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_2_enabled'] = not self.topic_data.get('pre_modal_2_enabled', False)
        await self._edit(interaction)

    async def config_preq2_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalConfigModal(self, question_num=2))

    async def config_preq2_no_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalNoResponseConfigModal(self, question_num=2))

    # --- Page 4: Behavior callbacks ---

    async def toggle_approval_cb(self, interaction: discord.Interaction):
        self.topic_data['approval_mode'] = not self.topic_data.get('approval_mode', False)
        await self._edit(interaction)

    async def toggle_discussion_cb(self, interaction: discord.Interaction):
        self.topic_data['discussion_mode'] = not self.topic_data.get('discussion_mode', False)
        await self._edit(interaction)

    async def toggle_delete_close_cb(self, interaction: discord.Interaction):
        self.topic_data['delete_on_close'] = not self.topic_data.get('delete_on_close', True)
        await self._edit(interaction)

    async def toggle_member_close_cb(self, interaction: discord.Interaction):
        self.topic_data['member_can_close'] = not self.topic_data.get('member_can_close', True)
        await self._edit(interaction)

    async def toggle_answer_cb(self, interaction: discord.Interaction):
        self.topic_data['pre_modal_answer_enabled'] = not self.topic_data.get('pre_modal_answer_enabled', False)
        await self._edit(interaction)

    async def config_answer_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PreModalAnswerConfigModal(self))

    async def set_close_message_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CloseMessageModal(self))

    # --- Finish / Cancel ---

    async def finish_cb(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
            topics[self.topic_data['name']] = self.topic_data
            await _save_json(self.cog.bot, TOPICS_FILE, topics, self.cog.topics_lock)
        except Exception as e:
            logger.error(f"Failed to save topic: {e}", exc_info=True)
            await interaction.followup.send(f"An error occurred: `{e.__class__.__name__}`. Check console.", ephemeral=True)
            return

        # Refresh any live panels that include this topic
        if interaction.guild:
            await self.cog._refresh_panels_for_topic(interaction.guild, self.topic_data['name'])

        try:
            if self.wizard_message:
                await self.wizard_message.edit(content=f"Topic `{self.topic_data['name']}` saved.", embed=None, view=None)
            else:
                await self.original_interaction.edit_original_response(
                    content=f"Topic `{self.topic_data['name']}` saved.", embed=None, view=None
                )
        except discord.NotFound:
            pass
        self.stop()

    async def cancel_cb(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Topic configuration cancelled.", embed=None, view=None)
        self.stop()


# --- Modals ---

class LabelModal(discord.ui.Modal, title="Edit Label & Emoji"):
    label_input = discord.ui.TextInput(label="Button Label", max_length=80)
    emoji_input = discord.ui.TextInput(label="Emoji (optional)", max_length=50, required=False)

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard
        self.label_input.default = wizard.topic_data.get('label')
        self.emoji_input.default = wizard.topic_data.get('emoji') or ""

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.topic_data['label'] = self.label_input.value
        self.wizard.topic_data['emoji'] = self.emoji_input.value.strip() or None
        await self.wizard._refresh(itx)

    async def on_error(self, itx: discord.Interaction, error: Exception):
        await itx.response.send_message("An error occurred.", ephemeral=True)


class CooldownModal(discord.ui.Modal, title="Set Cooldown"):
    minutes_input = discord.ui.TextInput(
        label="Cooldown in minutes",
        placeholder="5",
        max_length=5,
        required=True
    )

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard
        self.minutes_input.default = str(wizard.topic_data.get('cooldown_minutes', 5))

    async def on_submit(self, itx: discord.Interaction):
        try:
            minutes = int(self.minutes_input.value)
            if minutes < 0:
                minutes = 0
            self.wizard.topic_data['cooldown_minutes'] = minutes
            await self.wizard._refresh(itx)
        except ValueError:
            await itx.response.send_message("Please enter a valid number.", ephemeral=True)


class WelcomeMessageModal(discord.ui.Modal, title="Set Welcome Message"):
    message_input = discord.ui.TextInput(
        label="Welcome Message", style=discord.TextStyle.long,
        placeholder="Use {user} and {topic} as placeholders", max_length=2000
    )

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard
        self.message_input.default = wizard.topic_data.get('welcome_message')

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.topic_data['welcome_message'] = self.message_input.value
        await self.wizard._refresh(itx)


class CloseMessageModal(discord.ui.Modal, title="Set Close Message"):
    message_input = discord.ui.TextInput(
        label="Close DM Message", style=discord.TextStyle.long,
        placeholder="{channel}, {server}, {closer}, {user}",
        max_length=1000
    )

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard
        self.message_input.default = wizard.topic_data.get(
            'close_message',
            "Your ticket `{channel}` in **{server}** has been closed by {closer}."
        )

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.topic_data['close_message'] = self.message_input.value
        await self.wizard._refresh(itx)


class PreModalConfigModal(discord.ui.Modal, title="Configure Pre-Question"):
    question_input = discord.ui.TextInput(label="Question Text", max_length=200)
    yes_label_input = discord.ui.TextInput(label="Yes Button Label", max_length=80, required=False)
    no_label_input = discord.ui.TextInput(label="No Button Label", max_length=80, required=False)
    redirect_url_input = discord.ui.TextInput(
        label="Redirect URL (optional)", max_length=500, required=False
    )

    def __init__(self, wizard: PaginatedTopicWizardView, question_num: int):
        super().__init__()
        self.wizard = wizard
        self.num = question_num
        prefix = "pre_modal" if question_num == 1 else "pre_modal_2"
        self.prefix = prefix
        self.question_input.default = wizard.topic_data.get(f'{prefix}_question', '')
        self.yes_label_input.default = wizard.topic_data.get(f'{prefix}_yes_label', '')
        self.no_label_input.default = wizard.topic_data.get(f'{prefix}_no_label', '')
        self.redirect_url_input.default = wizard.topic_data.get(f'{prefix}_redirect_url', '') or ''

    async def on_submit(self, itx: discord.Interaction):
        p = self.prefix
        self.wizard.topic_data[f'{p}_question'] = self.question_input.value
        if self.yes_label_input.value:
            self.wizard.topic_data[f'{p}_yes_label'] = self.yes_label_input.value
        if self.no_label_input.value:
            self.wizard.topic_data[f'{p}_no_label'] = self.no_label_input.value
        url = self.redirect_url_input.value.strip()
        self.wizard.topic_data[f'{p}_redirect_url'] = url if url else None
        await self.wizard._refresh(itx)


class PreModalNoResponseConfigModal(discord.ui.Modal, title="Configure No Response"):
    no_message_input = discord.ui.TextInput(
        label="Message when user clicks No", style=discord.TextStyle.long, max_length=500
    )
    ready_enabled_input = discord.ui.TextInput(
        label="Show Ready Button? (yes/no)", max_length=5, required=False,
        placeholder="yes"
    )
    ready_label_input = discord.ui.TextInput(
        label="Ready Button Label", max_length=80, required=False
    )

    def __init__(self, wizard: PaginatedTopicWizardView, question_num: int):
        super().__init__()
        self.wizard = wizard
        self.num = question_num
        prefix = "pre_modal" if question_num == 1 else "pre_modal_2"
        self.prefix = prefix
        self.no_message_input.default = wizard.topic_data.get(f'{prefix}_no_message', '')
        enabled = wizard.topic_data.get(f'{prefix}_ready_button_enabled', True)
        self.ready_enabled_input.default = "yes" if enabled else "no"
        self.ready_label_input.default = wizard.topic_data.get(f'{prefix}_ready_button_label', '')

    async def on_submit(self, itx: discord.Interaction):
        p = self.prefix
        self.wizard.topic_data[f'{p}_no_message'] = self.no_message_input.value
        val = self.ready_enabled_input.value.strip().lower()
        if val in ("no", "n", "false", "off", "0"):
            self.wizard.topic_data[f'{p}_ready_button_enabled'] = False
        elif val in ("yes", "y", "true", "on", "1", ""):
            self.wizard.topic_data[f'{p}_ready_button_enabled'] = True
        if self.ready_label_input.value:
            self.wizard.topic_data[f'{p}_ready_button_label'] = self.ready_label_input.value
        await self.wizard._refresh(itx)


class PreModalAnswerConfigModal(discord.ui.Modal, title="Set Opener Question"):
    question_input = discord.ui.TextInput(
        label="Answer Question", max_length=200,
        placeholder="What information do you need from the user?"
    )

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard
        self.question_input.default = wizard.topic_data.get('pre_modal_answer_question', '')

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.topic_data['pre_modal_answer_question'] = self.question_input.value
        await self.wizard._refresh(itx)


class AddQuestionModal(discord.ui.Modal, title="Add a Question"):
    question_input = discord.ui.TextInput(label="Question Text", style=discord.TextStyle.long, max_length=200)

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.topic_data.setdefault('questions', []).append(self.question_input.value)
        await self.wizard._refresh(itx)


class EditQuestionModal(discord.ui.Modal, title="Edit Question"):
    question_input = discord.ui.TextInput(label="Question Text", style=discord.TextStyle.long, max_length=200)

    def __init__(self, wizard: PaginatedTopicWizardView, index: int):
        super().__init__()
        self.wizard = wizard
        self.index = index
        questions = wizard.topic_data.get('questions', [])
        if 0 <= index < len(questions):
            self.question_input.default = questions[index]

    async def on_submit(self, itx: discord.Interaction):
        questions = self.wizard.topic_data.get('questions', [])
        if 0 <= self.index < len(questions):
            questions[self.index] = self.question_input.value
        await self.wizard._refresh(itx)


class ChannelNameFormatModal(discord.ui.Modal, title="Channel Name Format"):
    format_input = discord.ui.TextInput(
        label="Name format (leave blank for default)",
        placeholder="{topic}-{user}  or  {topic}-{number}",
        max_length=100,
        required=False
    )

    def __init__(self, wizard: PaginatedTopicWizardView):
        super().__init__()
        self.wizard = wizard
        current = wizard.topic_data.get('channel_name_format')
        if current:
            self.format_input.default = current

    async def on_submit(self, itx: discord.Interaction):
        value = self.format_input.value.strip()
        self.wizard.topic_data['channel_name_format'] = value or None
        await self.wizard._refresh(itx)
