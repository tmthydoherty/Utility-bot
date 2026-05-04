import discord
import asyncio
import copy
import io
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..cog import TicketSystem

from ..storage import _load_json, _save_json, TOPICS_FILE, PANELS_FILE, SURVEY_DATA_FILE
from ..defaults import _ensure_topic_defaults, _ensure_panel_defaults
from .topic_wizard import PaginatedTopicWizardView
from .panel_wizard import PanelWizardView
from .survey import SurveyTargetView

logger = logging.getLogger("ticketing_cog")

try:
    import openpyxl
except ImportError:
    openpyxl = None


class AdminDashboardView(discord.ui.View):
    """Master admin dashboard accessed via /ticketing."""

    def __init__(self, cog: "TicketSystem"):
        super().__init__(timeout=600)
        self.cog = cog
        self.section = "home"
        self.sub_action = None  # {"type": "pick_edit_topic", ...} etc.
        self.update_components()

    def update_components(self):
        self.clear_items()
        if self.sub_action:
            self._build_sub_action()
            return
        if self.section == "home":
            self._home_components()
        elif self.section == "topics":
            self._topics_components()
        elif self.section == "panels":
            self._panels_components()
        elif self.section == "responses":
            self._responses_components()
        elif self.section == "send_survey":
            self._send_survey_components()

    def _btn(self, label, style, callback, row, disabled=False):
        btn = discord.ui.Button(label=label, style=style, row=row, disabled=disabled)
        btn.callback = callback
        return btn

    async def _edit(self, interaction: discord.Interaction):
        """Update the dashboard via a component interaction."""
        self.update_components()
        await interaction.response.edit_message(embed=await self._embed(), view=self)

    # --- Home ---
    def _home_components(self):
        select = discord.ui.Select(
            placeholder="Navigate to a section...",
            options=[
                discord.SelectOption(label="Topics", value="topics", description="Manage topics (tickets, applications, surveys)"),
                discord.SelectOption(label="Panels", value="panels", description="Manage support panels"),
                discord.SelectOption(label="Responses", value="responses", description="View/export/delete responses"),
                discord.SelectOption(label="Send Survey", value="send_survey", description="DM a survey to roles/users"),
            ],
            row=0
        )
        select.callback = self._nav_select_cb
        self.add_item(select)

    async def _nav_select_cb(self, interaction: discord.Interaction):
        self.section = interaction.data['values'][0]
        self.sub_action = None
        await self._edit(interaction)

    async def _back_cb(self, interaction: discord.Interaction):
        self.section = "home"
        self.sub_action = None
        await self._edit(interaction)

    async def _sub_back_cb(self, interaction: discord.Interaction):
        self.sub_action = None
        await self._edit(interaction)

    # --- Embed ---
    async def _embed(self) -> discord.Embed:
        if self.sub_action:
            return await self._sub_action_embed()
        if self.section == "home":
            return await self._home_embed()
        elif self.section == "topics":
            return await self._topics_embed()
        elif self.section == "panels":
            return await self._panels_embed()
        elif self.section == "responses":
            return await self._responses_embed()
        elif self.section == "send_survey":
            return await self._send_survey_embed()
        return discord.Embed(title="Ticketing Admin", color=discord.Color.blue())

    async def _sub_action_embed(self) -> discord.Embed:
        sa = self.sub_action
        sa_type = sa["type"]

        if sa_type in ("pick_edit_topic", "pick_delete_topic", "pick_clone_topic"):
            action_label = {"pick_edit_topic": "Edit", "pick_delete_topic": "Delete", "pick_clone_topic": "Clone"}[sa_type]
            embed = discord.Embed(title=f"{action_label} Topic", color=0x2b2d31)
            embed.description = f"Select a topic to {action_label.lower()}."
            return embed

        if sa_type == "confirm_delete_topic":
            return discord.Embed(
                title="Confirm Delete",
                description=f"Are you sure you want to delete topic `{sa['name']}`?\nThis cannot be undone.",
                color=discord.Color.red()
            )

        if sa_type in ("pick_edit_panel", "pick_delete_panel", "pick_send_panel"):
            action_label = {"pick_edit_panel": "Edit", "pick_delete_panel": "Delete", "pick_send_panel": "Send"}[sa_type]
            embed = discord.Embed(title=f"{action_label} Panel", color=0x2b2d31)
            embed.description = f"Select a panel to {action_label.lower()}."
            return embed

        if sa_type in ("pick_view_responses", "pick_export_responses", "pick_delete_responses"):
            action_label = {"pick_view_responses": "View", "pick_export_responses": "Export", "pick_delete_responses": "Delete"}[sa_type]
            embed = discord.Embed(title=f"{action_label} Responses", color=0x2b2d31)
            embed.description = f"Select a topic to {action_label.lower()} responses."
            return embed

        if sa_type == "confirm_delete_responses":
            return discord.Embed(
                title="Confirm Delete Responses",
                description=f"Delete all **{sa['count']}** responses for `{sa['name']}`?",
                color=discord.Color.red()
            )

        if sa_type == "pick_survey_topic":
            embed = discord.Embed(title="Send Survey", color=0x2b2d31)
            embed.description = "Select a topic to send as a survey."
            return embed

        if sa_type == "survey_targets":
            topic = sa.get("topic", {})
            embed = discord.Embed(title=f"Send: {topic.get('label', 'Survey')}", color=0x2b2d31)
            embed.description = "Select roles and/or members, then click **Send Survey**."
            return embed

        return discord.Embed(title="Configuration", color=0x2b2d31)

    # --- Sub-action Builder ---

    def _build_sub_action(self):
        sa = self.sub_action
        sa_type = sa["type"]

        if sa_type in ("pick_edit_topic", "pick_delete_topic", "pick_clone_topic"):
            options = sa.get("options", [])
            if options:
                callbacks = {
                    "pick_edit_topic": self._sa_edit_topic_selected,
                    "pick_delete_topic": self._sa_delete_topic_selected,
                    "pick_clone_topic": self._sa_clone_topic_selected,
                }
                select = discord.ui.Select(placeholder="Select a topic...", options=options, row=0)
                select.callback = callbacks[sa_type]
                self.add_item(select)
            self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._sub_back_cb, 4))

        elif sa_type == "confirm_delete_topic":
            self.add_item(self._btn("Confirm Delete", discord.ButtonStyle.danger, self._sa_confirm_delete_topic, 0))
            self.add_item(self._btn("Cancel", discord.ButtonStyle.secondary, self._sub_back_cb, 0))

        elif sa_type in ("pick_edit_panel", "pick_delete_panel", "pick_send_panel"):
            options = sa.get("options", [])
            if options:
                callbacks = {
                    "pick_edit_panel": self._sa_edit_panel_selected,
                    "pick_delete_panel": self._sa_delete_panel_selected,
                    "pick_send_panel": self._sa_send_panel_selected,
                }
                select = discord.ui.Select(placeholder="Select a panel...", options=options, row=0)
                select.callback = callbacks[sa_type]
                self.add_item(select)
            self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._sub_back_cb, 4))

        elif sa_type in ("pick_view_responses", "pick_export_responses", "pick_delete_responses"):
            options = sa.get("options", [])
            if options:
                callbacks = {
                    "pick_view_responses": self._sa_view_responses_selected,
                    "pick_export_responses": self._sa_export_responses_selected,
                    "pick_delete_responses": self._sa_delete_responses_selected,
                }
                select = discord.ui.Select(placeholder="Select a topic...", options=options, row=0)
                select.callback = callbacks[sa_type]
                self.add_item(select)
            self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._sub_back_cb, 4))

        elif sa_type == "confirm_delete_responses":
            self.add_item(self._btn(f"Delete All {sa['count']} Responses", discord.ButtonStyle.danger, self._sa_confirm_delete_responses, 0))
            self.add_item(self._btn("Cancel", discord.ButtonStyle.secondary, self._sub_back_cb, 0))

        elif sa_type == "pick_survey_topic":
            options = sa.get("options", [])
            if options:
                select = discord.ui.Select(placeholder="Select a topic...", options=options, row=0)
                select.callback = self._sa_survey_topic_selected
                self.add_item(select)
            self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._sub_back_cb, 4))

        elif sa_type == "survey_targets":
            role_select = discord.ui.RoleSelect(placeholder="Select roles to send to...", max_values=25, row=0)
            role_select.callback = self._sa_survey_role_cb
            self.add_item(role_select)

            user_select = discord.ui.UserSelect(placeholder="Select members to send to...", max_values=25, row=1)
            user_select.callback = self._sa_survey_user_cb
            self.add_item(user_select)

            self.add_item(self._btn("Send Survey", discord.ButtonStyle.success, self._sa_survey_send_cb, 2))
            self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._sub_back_cb, 4))

    async def _home_embed(self) -> discord.Embed:
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        survey_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)

        total_responses = sum(len(v) for v in survey_data.values())

        embed = discord.Embed(title="Ticketing Dashboard", color=0x2b2d31)
        embed.add_field(
            name="Overview",
            value=(
                f"```\n"
                f"Topics      {len(topics):>3}\n"
                f"Panels      {len(panels):>3}\n"
                f"Responses   {total_responses:>3}\n"
                f"```"
            ),
            inline=False
        )
        return embed

    # ===== TOPICS SECTION =====

    async def _topics_embed(self) -> discord.Embed:
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        embed = discord.Embed(title="Topics", color=0x2b2d31)

        if not topics:
            embed.description = "*No topics created yet.*"
            return embed

        type_icons = {"ticket": "\U0001f3ab", "application": "\U0001f4cb", "survey": "\U0001f4ca"}
        type_labels = {"ticket": "Tickets", "application": "Applications", "survey": "Surveys"}
        grouped: Dict[str, list] = {"ticket": [], "application": [], "survey": []}

        for name, t in topics.items():
            topic_type = t.get('type', 'ticket')
            emoji = t.get('emoji') or ''
            label = t.get('label', name)
            line = f"- {emoji} **{label}**" if emoji else f"- **{label}**"
            grouped.setdefault(topic_type, []).append(line)

        for ttype in ["ticket", "application", "survey"]:
            items = grouped.get(ttype, [])
            if items:
                icon = type_icons.get(ttype, "")
                header = type_labels.get(ttype, ttype.capitalize())
                embed.add_field(
                    name=f"{icon}  {header}",
                    value="\n".join(items)[:1024],
                    inline=False
                )

        return embed

    def _topics_components(self):
        self.add_item(self._btn("Create Topic", discord.ButtonStyle.success, self._create_topic_cb, 0))
        self.add_item(self._btn("Edit Topic", discord.ButtonStyle.primary, self._edit_topic_cb, 0))
        self.add_item(self._btn("Delete Topic", discord.ButtonStyle.danger, self._delete_topic_cb, 0))
        self.add_item(self._btn("Clone Topic", discord.ButtonStyle.secondary, self._clone_topic_cb, 1))
        self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._back_cb, 4))

    async def _create_topic_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateTopicNameModal(self.cog, self))

    async def _get_topic_options(self) -> List[discord.SelectOption]:
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        return [discord.SelectOption(label=t.get('label', name)[:100], value=name[:100])
                for name, t in topics.items()][:25]

    async def _edit_topic_cb(self, interaction: discord.Interaction):
        options = await self._get_topic_options()
        if not options:
            return await interaction.response.send_message("No topics to edit.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_edit_topic", "options": options}
        await self._edit(interaction)

    async def _sa_edit_topic_selected(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        name = interaction.data['values'][0]
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if name not in topics:
            return await interaction.followup.send("Topic not found.", ephemeral=True, delete_after=5)
        topic_data = _ensure_topic_defaults(topics[name])
        # Restore dashboard before opening wizard
        self.sub_action = None
        self.update_components()
        try:
            await interaction.message.edit(embed=await self._embed(), view=self)
        except discord.NotFound:
            pass
        wizard = PaginatedTopicWizardView(self.cog, interaction, topic_data, is_new=False)
        wizard.wizard_message = await interaction.followup.send(embed=wizard._embed(), view=wizard, ephemeral=True, wait=True)

    async def _delete_topic_cb(self, interaction: discord.Interaction):
        options = await self._get_topic_options()
        if not options:
            return await interaction.response.send_message("No topics to delete.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_delete_topic", "options": options}
        await self._edit(interaction)

    async def _sa_delete_topic_selected(self, interaction: discord.Interaction):
        name = interaction.data['values'][0]
        self.sub_action = {"type": "confirm_delete_topic", "name": name}
        await self._edit(interaction)

    async def _sa_confirm_delete_topic(self, interaction: discord.Interaction):
        name = self.sub_action['name']
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if name in topics:
            del topics[name]
            await _save_json(self.cog.bot, TOPICS_FILE, topics, self.cog.topics_lock)
        # Remove from panels
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        updated = False
        for p_data in panels.values():
            if name in p_data.get('topic_names', []):
                p_data['topic_names'].remove(name)
                updated = True
        if updated:
            await _save_json(self.cog.bot, PANELS_FILE, panels, self.cog.panels_lock)
        self.sub_action = None
        self.section = "topics"
        self.update_components()
        await interaction.response.edit_message(embed=await self._embed(), view=self)
        await interaction.followup.send(f"Topic `{name}` deleted.", ephemeral=True, delete_after=5)

    async def _clone_topic_cb(self, interaction: discord.Interaction):
        options = await self._get_topic_options()
        if not options:
            return await interaction.response.send_message("No topics to clone.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_clone_topic", "options": options}
        await self._edit(interaction)

    async def _sa_clone_topic_selected(self, interaction: discord.Interaction):
        name = interaction.data['values'][0]
        await interaction.response.send_modal(CloneTopicNameModal(self.cog, self, name))

    # ===== PANELS SECTION =====

    async def _panels_embed(self) -> discord.Embed:
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        embed = discord.Embed(title="Panels", color=0x2b2d31)

        if not panels:
            embed.description = "*No panels created yet.*"
            return embed

        lines = []
        for name, p in panels.items():
            ch_id = p.get('channel_id')
            ch_text = f"<#{ch_id}>" if ch_id else "No channel"
            msg_id = p.get('message_id')
            status = "\u2705" if msg_id else "\U0001f4dd"
            mode = p.get('display_mode', 'buttons').capitalize()
            attached = p.get('topic_names', [])
            lines.append(f"- {status} **{p.get('title', name)}** `{name}`")
            lines.append(f"  {ch_text} \u00b7 {mode} \u00b7 {len(attached)} topics")

        embed.description = "\n".join(lines)[:4096]
        return embed

    def _panels_components(self):
        self.add_item(self._btn("Create Panel", discord.ButtonStyle.success, self._create_panel_cb, 0))
        self.add_item(self._btn("Edit Panel", discord.ButtonStyle.primary, self._edit_panel_cb, 0))
        self.add_item(self._btn("Delete Panel", discord.ButtonStyle.danger, self._delete_panel_cb, 0))
        self.add_item(self._btn("Send Panel", discord.ButtonStyle.blurple, self._send_panel_cb, 1))
        self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._back_cb, 4))

    async def _create_panel_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreatePanelNameModal(self.cog, self))

    async def _get_panel_options(self) -> List[discord.SelectOption]:
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        return [discord.SelectOption(label=name[:100], value=name[:100]) for name in panels][:25]

    async def _edit_panel_cb(self, interaction: discord.Interaction):
        options = await self._get_panel_options()
        if not options:
            return await interaction.response.send_message("No panels to edit.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_edit_panel", "options": options}
        await self._edit(interaction)

    async def _sa_edit_panel_selected(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        name = interaction.data['values'][0]
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        if name not in panels:
            return await interaction.followup.send("Panel not found.", ephemeral=True, delete_after=5)
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        # Restore dashboard before opening wizard
        self.sub_action = None
        self.update_components()
        try:
            await interaction.message.edit(embed=await self._embed(), view=self)
        except discord.NotFound:
            pass
        wizard = PanelWizardView(self.cog, interaction, panels[name], is_new=False, all_topics=topics)
        wizard.wizard_message = await interaction.followup.send(embed=wizard._embed(), view=wizard, ephemeral=True, wait=True)

    async def _delete_panel_cb(self, interaction: discord.Interaction):
        options = await self._get_panel_options()
        if not options:
            return await interaction.response.send_message("No panels to delete.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_delete_panel", "options": options}
        await self._edit(interaction)

    async def _sa_delete_panel_selected(self, interaction: discord.Interaction):
        await interaction.response.defer()
        name = interaction.data['values'][0]
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        if name in panels:
            del panels[name]
            await _save_json(self.cog.bot, PANELS_FILE, panels, self.cog.panels_lock)
        self.sub_action = None
        self.section = "panels"
        self.update_components()
        try:
            await interaction.message.edit(embed=await self._embed(), view=self)
        except discord.NotFound:
            pass
        await interaction.followup.send(f"Panel `{name}` deleted.", ephemeral=True, delete_after=5)

    async def _send_panel_cb(self, interaction: discord.Interaction):
        options = await self._get_panel_options()
        if not options:
            return await interaction.response.send_message("No panels to send.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_send_panel", "options": options}
        await self._edit(interaction)

    async def _sa_send_panel_selected(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        name = interaction.data['values'][0]
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        panel_data = panels.get(name)
        if not panel_data:
            return await interaction.followup.send("Panel not found.", ephemeral=True)
        channel_id = panel_data.get('channel_id')
        if not channel_id:
            return await interaction.followup.send("Panel has no post channel set.", ephemeral=True)
        channel = interaction.guild.get_channel(channel_id)
        if not channel or not isinstance(channel, discord.TextChannel):
            return await interaction.followup.send("Post channel not found.", ephemeral=True)

        view = self.cog.create_panel_view(panel_data, topics)
        if not view:
            return await interaction.followup.send("Panel has no valid topics attached.", ephemeral=True)

        embed = discord.Embed(
            title=panel_data.get('title') or None,
            description=panel_data.get('description') or None,
            color=discord.Color.purple()
        )
        image_url = panel_data.get("image_url")
        if image_url:
            if panel_data.get("image_type") == "thumbnail":
                embed.set_thumbnail(url=image_url)
            else:
                embed.set_image(url=image_url)

        # Delete old message if exists
        msg_id = panel_data.get("message_id")
        if msg_id:
            try:
                old_msg = await channel.fetch_message(msg_id)
                await old_msg.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        try:
            msg = await channel.send(embed=embed, view=view)
            panels[name]['message_id'] = msg.id
            await _save_json(self.cog.bot, PANELS_FILE, panels, self.cog.panels_lock)
        except Exception as e:
            logger.error(f"Failed to send panel: {e}", exc_info=True)
            return await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)

        # Restore dashboard
        self.sub_action = None
        self.section = "panels"
        self.update_components()
        try:
            await interaction.message.edit(embed=await self._embed(), view=self)
        except discord.NotFound:
            pass
        await interaction.followup.send(f"Panel `{name}` sent to {channel.mention}.", ephemeral=True, delete_after=10)

    # ===== RESPONSES SECTION =====

    async def _responses_embed(self) -> discord.Embed:
        survey_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        embed = discord.Embed(title="Responses", color=0x2b2d31)

        type_icons = {"ticket": "\U0001f3ab", "application": "\U0001f4cb", "survey": "\U0001f4ca"}
        lines = []
        for name, responses in survey_data.items():
            if not responses:
                continue
            t = topics.get(name, {})
            label = t.get('label', name)
            icon = type_icons.get(t.get('type', 'unknown'), "")
            lines.append(f"- {icon} **{label}** `{name}` \u2014 **{len(responses)}** responses")

        if lines:
            embed.description = "\n".join(lines)[:4096]
        else:
            embed.description = "*No responses collected yet.*"

        return embed

    def _responses_components(self):
        self.add_item(self._btn("View Responses", discord.ButtonStyle.primary, self._view_responses_cb, 0))
        self.add_item(self._btn("Export to Excel", discord.ButtonStyle.success, self._export_responses_cb, 0))
        self.add_item(self._btn("Delete Responses", discord.ButtonStyle.danger, self._delete_responses_cb, 1))
        self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._back_cb, 4))

    async def _get_response_topic_options(self) -> List[discord.SelectOption]:
        survey_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
        options = []
        for name, responses in survey_data.items():
            if responses:
                options.append(discord.SelectOption(
                    label=f"{name} ({len(responses)} responses)"[:100],
                    value=name[:100]
                ))
        return options[:25]

    async def _view_responses_cb(self, interaction: discord.Interaction):
        options = await self._get_response_topic_options()
        if not options:
            return await interaction.response.send_message("No responses to view.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_view_responses", "options": options}
        await self._edit(interaction)

    async def _sa_view_responses_selected(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        name = interaction.data['values'][0]
        all_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
        responses = all_data.get(name, [])
        if not responses:
            return await interaction.followup.send("No responses found.", ephemeral=True, delete_after=5)

        embeds = []
        for resp in responses[-5:]:
            embed = discord.Embed(
                title=f"Response from {resp.get('user_name', 'Unknown')}",
                color=discord.Color.teal(),
                timestamp=datetime.fromisoformat(resp['timestamp']) if resp.get('timestamp') else None
            )
            for q, a in resp.get('answers', {}).items():
                embed.add_field(name=q[:256], value=a[:1024], inline=False)
            embeds.append(embed)

        # View responses must go as followup (embeds can't fit in edit_message easily)
        await interaction.followup.send(
            f"**{name}** - Showing latest {len(embeds)} of {len(responses)} responses:",
            embeds=embeds[:10], ephemeral=True
        )

    async def _export_responses_cb(self, interaction: discord.Interaction):
        if openpyxl is None:
            return await interaction.response.send_message(
                "The `openpyxl` library is not installed.", ephemeral=True
            )
        options = await self._get_response_topic_options()
        if not options:
            return await interaction.response.send_message("No responses to export.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_export_responses", "options": options}
        await self._edit(interaction)

    async def _sa_export_responses_selected(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        name = interaction.data['values'][0]
        all_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
        responses = all_data.get(name, [])
        if not responses:
            return await interaction.followup.send("No responses found.", ephemeral=True)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = name[:30]
        headers = ["Timestamp", "User ID", "User Name"]
        all_questions = set()
        for resp in responses:
            all_questions.update(resp.get("answers", {}).keys())
        sorted_questions = sorted(list(all_questions))
        headers.extend(sorted_questions)
        ws.append(headers)

        for resp in responses:
            row = [resp.get("timestamp"), resp.get("user_id"), resp.get("user_name")]
            for q in sorted_questions:
                row.append(resp.get("answers", {}).get(q, ""))
            ws.append(row)

        virtual_file = io.BytesIO()
        wb.save(virtual_file)
        virtual_file.seek(0)
        file_name = f"export_{name}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.xlsx"
        await interaction.followup.send("Here is your export:", file=discord.File(virtual_file, filename=file_name), ephemeral=True)

    async def _delete_responses_cb(self, interaction: discord.Interaction):
        options = await self._get_response_topic_options()
        if not options:
            return await interaction.response.send_message("No responses to delete.", ephemeral=True, delete_after=10)
        self.sub_action = {"type": "pick_delete_responses", "options": options}
        await self._edit(interaction)

    async def _sa_delete_responses_selected(self, interaction: discord.Interaction):
        name = interaction.data['values'][0]
        all_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
        count = len(all_data.get(name, []))
        self.sub_action = {"type": "confirm_delete_responses", "name": name, "count": count}
        await self._edit(interaction)

    async def _sa_confirm_delete_responses(self, interaction: discord.Interaction):
        await interaction.response.defer()
        name = self.sub_action['name']
        current_data = await _load_json(self.cog.bot, SURVEY_DATA_FILE, self.cog.survey_data_lock)
        current_data[name] = []
        await _save_json(self.cog.bot, SURVEY_DATA_FILE, current_data, self.cog.survey_data_lock)
        self.sub_action = None
        self.section = "responses"
        self.update_components()
        try:
            await interaction.message.edit(embed=await self._embed(), view=self)
        except discord.NotFound:
            pass
        await interaction.followup.send(f"All responses for `{name}` deleted.", ephemeral=True, delete_after=5)

    # ===== SEND SURVEY SECTION =====

    async def _send_survey_embed(self) -> discord.Embed:
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        survey_topics = {n: t for n, t in topics.items() if t.get('questions')}
        embed = discord.Embed(title="Send Survey", color=0x2b2d31)

        if not survey_topics:
            embed.description = "*No topics with questions to send.*"
            return embed

        type_icons = {"ticket": "\U0001f3ab", "application": "\U0001f4cb", "survey": "\U0001f4ca"}
        lines = ["Select a topic below to DM it to roles or users.\n"]
        for name, t in survey_topics.items():
            q_count = len(t.get('questions', []))
            icon = type_icons.get(t.get('type', 'unknown'), "")
            lines.append(f"- {icon} **{t.get('label', name)}** `{name}` \u2014 {q_count} questions")

        embed.description = "\n".join(lines)[:4096]
        return embed

    def _send_survey_components(self):
        self.add_item(self._btn("Select & Send", discord.ButtonStyle.success, self._pick_survey_to_send_cb, 0))
        self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._back_cb, 4))

    async def _pick_survey_to_send_cb(self, interaction: discord.Interaction):
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        survey_topics = {n: t for n, t in topics.items() if t.get('questions')}
        if not survey_topics:
            return await interaction.response.send_message("No topics with questions.", ephemeral=True, delete_after=10)
        options = [discord.SelectOption(label=f"{t.get('label', n)[:90]} ({t.get('type')})", value=n[:100])
                   for n, t in survey_topics.items()][:25]
        self.sub_action = {"type": "pick_survey_topic", "options": options}
        await self._edit(interaction)

    async def _sa_survey_topic_selected(self, interaction: discord.Interaction):
        name = interaction.data['values'][0]
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        topic = topics.get(name)
        if not topic:
            return await interaction.response.send_message("Topic not found.", ephemeral=True, delete_after=5)
        topic = _ensure_topic_defaults(topic)
        self.sub_action = {
            "type": "survey_targets",
            "topic": topic,
            "selected_roles": [],
            "selected_users": [],
        }
        await self._edit(interaction)

    async def _sa_survey_role_cb(self, interaction: discord.Interaction):
        self.sub_action["selected_roles"] = [r.id for r in interaction.data.get('resolved', {}).get('roles', {}).values()] if hasattr(interaction.data, 'get') else []
        # Store raw values for later resolution
        self.sub_action["_role_values"] = interaction.data.get('values', [])
        self.sub_action["_guild"] = interaction.guild
        await interaction.response.defer()

    async def _sa_survey_user_cb(self, interaction: discord.Interaction):
        self.sub_action["_user_values"] = interaction.data.get('values', [])
        self.sub_action["_guild"] = interaction.guild
        await interaction.response.defer()

    async def _sa_survey_send_cb(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        sa = self.sub_action
        guild = sa.get("_guild") or interaction.guild
        topic = sa["topic"]

        targets = set()
        for role_id_str in sa.get("_role_values", []):
            role = guild.get_role(int(role_id_str))
            if role:
                targets.update(role.members)
        for user_id_str in sa.get("_user_values", []):
            member = guild.get_member(int(user_id_str))
            if member:
                targets.add(member)

        if not targets:
            return await interaction.followup.send("You must select at least one role or member.", ephemeral=True)

        from .survey import StartSurveyView
        embed = discord.Embed(
            title=f"Survey Invitation: {topic.get('label')}",
            description=f"You have been invited to participate in a survey from **{guild.name}**. Please click the button below to begin.",
            color=discord.Color.blue()
        )
        view = StartSurveyView(topic, self.cog.bot)

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

        # Restore dashboard
        self.sub_action = None
        self.section = "send_survey"
        self.update_components()
        try:
            await interaction.message.edit(embed=await self._embed(), view=self)
        except discord.NotFound:
            pass
        await interaction.followup.send(
            f"Survey sent!\n- **Successful DMs:** {success_count}\n- **Failed DMs (privacy settings):** {fail_count}",
            ephemeral=True
        )


# --- Helper Modals ---

class CreateTopicNameModal(discord.ui.Modal, title="Create New Topic"):
    name_input = discord.ui.TextInput(label="Topic Name", placeholder="e.g., ban-appeal (no spaces)", required=True)

    def __init__(self, cog: "TicketSystem", dashboard: AdminDashboardView):
        super().__init__()
        self.cog = cog
        self.dashboard = dashboard

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        name = self.name_input.value.lower().strip().replace(" ", "-")
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if name in topics:
            return await interaction.followup.send("A topic with this name already exists.", ephemeral=True)
        topic_data = _ensure_topic_defaults({"name": name, "label": name.replace("-", " ").title()})
        wizard = PaginatedTopicWizardView(self.cog, interaction, topic_data, is_new=True)
        wizard.wizard_message = await interaction.followup.send(embed=wizard._embed(), view=wizard, ephemeral=True, wait=True)


class CloneTopicNameModal(discord.ui.Modal, title="Clone Topic"):
    name_input = discord.ui.TextInput(label="New Topic Name", placeholder="e.g., ban-appeal-v2", required=True)

    def __init__(self, cog: "TicketSystem", dashboard: AdminDashboardView, source_name: str):
        super().__init__()
        self.cog = cog
        self.dashboard = dashboard
        self.source_name = source_name

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        new_name = self.name_input.value.lower().strip().replace(" ", "-")
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if new_name in topics:
            return await interaction.followup.send("A topic with this name already exists.", ephemeral=True)
        source = topics.get(self.source_name)
        if not source:
            return await interaction.followup.send("Source topic not found.", ephemeral=True)

        cloned = copy.deepcopy(source)
        cloned['name'] = new_name
        cloned['label'] = new_name.replace("-", " ").title()
        cloned['ticket_counter'] = 0
        cloned = _ensure_topic_defaults(cloned)

        # Restore dashboard from sub_action
        self.dashboard.sub_action = None
        self.dashboard.update_components()

        wizard = PaginatedTopicWizardView(self.cog, interaction, cloned, is_new=True)
        wizard.wizard_message = await interaction.followup.send(embed=wizard._embed(), view=wizard, ephemeral=True, wait=True)


class CreatePanelNameModal(discord.ui.Modal, title="Create New Panel"):
    name_input = discord.ui.TextInput(label="Panel Name", placeholder="e.g., main-support", required=True)

    def __init__(self, cog: "TicketSystem", dashboard: AdminDashboardView):
        super().__init__()
        self.cog = cog
        self.dashboard = dashboard

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        name = self.name_input.value.lower().strip().replace(" ", "-")
        panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
        if name in panels:
            return await interaction.followup.send("A panel with this name already exists.", ephemeral=True)
        topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        panel_data = _ensure_panel_defaults({"name": name})
        wizard = PanelWizardView(self.cog, interaction, panel_data, is_new=True, all_topics=topics)
        wizard.wizard_message = await interaction.followup.send(embed=wizard._embed(), view=wizard, ephemeral=True, wait=True)
