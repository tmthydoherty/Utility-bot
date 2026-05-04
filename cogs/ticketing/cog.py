import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from discord.ext.commands import Bot

from .storage import _load_json, _save_json, TOPICS_FILE, PANELS_FILE, SURVEY_DATA_FILE, SURVEY_SESSIONS_FILE
from .defaults import _ensure_topic_defaults, _ensure_panel_defaults, DEFAULT_COOLDOWN_MINUTES, format_channel_name
from .views.runtime import (
    PanelAction, CategoryAction, CloseTicketView, ApprovalView, ClaimAlertView, ClaimedTicketView, ResponseModal
)
from .views.survey import StartSurveyView, ResumeOrRestartView
from .views.admin_dashboard import AdminDashboardView

logger = logging.getLogger("ticketing_cog")


@app_commands.default_permissions(administrator=True)
class TicketSystem(commands.Cog):

    def __init__(self, bot: "Bot"):
        self.bot = bot
        self.persistent_views_added = False
        self.topics_lock = asyncio.Lock()
        self.panels_lock = asyncio.Lock()
        self.survey_data_lock = asyncio.Lock()
        self.survey_sessions_lock = asyncio.Lock()
        self.cooldowns: Dict[int, datetime] = {}
        self.survey_cooldowns: Dict[int, Dict[str, datetime]] = {}
        self.active_survey_sessions: Dict[int, Dict[str, Any]] = {}
        self._cooldown_cleanup_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self._cooldown_cleanup_task = asyncio.create_task(self._cleanup_cooldowns_loop())
        await self._restore_survey_sessions()

    async def cog_unload(self):
        if self._cooldown_cleanup_task:
            self._cooldown_cleanup_task.cancel()
            try:
                await self._cooldown_cleanup_task
            except asyncio.CancelledError:
                pass
        await self._save_survey_sessions()

    # --- Session persistence ---

    async def _restore_survey_sessions(self):
        try:
            sessions = await _load_json(self.bot, SURVEY_SESSIONS_FILE, self.survey_sessions_lock)
            now = datetime.now(timezone.utc)
            restored = 0
            for uid_str, session in list(sessions.items()):
                try:
                    uid = int(uid_str)
                    session_time = datetime.fromisoformat(session.get("started_at", ""))
                    if (now - session_time) < timedelta(hours=24):
                        self.active_survey_sessions[uid] = session
                        restored += 1
                except (ValueError, KeyError):
                    continue
            if restored:
                logger.info(f"Restored {restored} active survey sessions.")
        except Exception as e:
            logger.error(f"Failed to restore survey sessions: {e}", exc_info=True)

    async def _save_survey_sessions(self):
        try:
            to_save = {str(k): v for k, v in self.active_survey_sessions.items()}
            await _save_json(self.bot, SURVEY_SESSIONS_FILE, to_save, self.survey_sessions_lock)
            if to_save:
                logger.info(f"Saved {len(to_save)} active survey sessions.")
        except Exception as e:
            logger.error(f"Failed to save survey sessions: {e}", exc_info=True)

    # --- Background cleanup ---

    async def _cleanup_cooldowns_loop(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                await asyncio.sleep(600)
                now = datetime.now(timezone.utc)

                expired = [uid for uid, ts in self.cooldowns.items()
                           if (now - ts.replace(tzinfo=timezone.utc)) > timedelta(minutes=30)]
                for uid in expired:
                    del self.cooldowns[uid]

                survey_expired_users = []
                for uid, surveys in self.survey_cooldowns.items():
                    exp = [s for s, ts in surveys.items()
                           if (now - ts.replace(tzinfo=timezone.utc)) > timedelta(hours=24)]
                    for s in exp:
                        del surveys[s]
                    if not surveys:
                        survey_expired_users.append(uid)
                for uid in survey_expired_users:
                    del self.survey_cooldowns[uid]

                exp_sessions = [uid for uid, s in self.active_survey_sessions.items()
                                if (now - datetime.fromisoformat(s.get("started_at", now.isoformat())).replace(tzinfo=timezone.utc)) > timedelta(hours=24)]
                for uid in exp_sessions:
                    del self.active_survey_sessions[uid]
                if exp_sessions:
                    await self._save_survey_sessions()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cooldown cleanup error: {e}", exc_info=True)

    # --- Channel creation ---

    async def _create_discussion_channel(self, interaction: discord.Interaction,
                                          topic: Dict[str, Any],
                                          member: Union[discord.Member, discord.User],
                                          is_ticket: bool = False,
                                          user_answer: Optional[str] = None,
                                          qa_answers: Optional[Dict[str, str]] = None):
        try:
            guild = interaction.guild
            topic_name = topic.get('name', 'ticket')

            # Increment counter if numbering or custom format uses {number}
            fmt = topic.get('channel_name_format')
            if topic.get('use_numbering', False) or (fmt and '{number}' in fmt):
                counter = topic.get('ticket_counter', 0) + 1
                topic['ticket_counter'] = counter
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                if topic_name in topics:
                    topics[topic_name]['ticket_counter'] = counter
                    await _save_json(self.bot, TOPICS_FILE, topics, self.topics_lock)

            channel_name = format_channel_name(topic, member.name)

            parent_id = topic.get('parent_id')
            if not parent_id:
                raise ValueError("Parent not set.")
            parent = guild.get_channel(parent_id)
            if not parent:
                raise ValueError(f"Parent {parent_id} not found.")

            channel_topic_str = f"Ticket Topic: {topic_name} | Opener: {member.id}"
            welcome_template = topic.get("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
            welcome_message = welcome_template.format(user=member.mention, topic=topic.get('label', 'N/A'))

            close_view = CloseTicketView(topic_name=topic_name, opener_id=member.id)
            has_unique_names = topic.get('use_numbering', False) or (fmt and '{number}' in fmt)

            if topic.get('mode') == 'channel':
                if not isinstance(parent, discord.CategoryChannel):
                    raise ValueError("Parent must be a category.")
                if is_ticket and not has_unique_names:
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

                try:
                    embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                    if user_answer:
                        answer_q = topic.get('pre_modal_answer_question', 'Response')
                        embed.add_field(name=answer_q, value=user_answer, inline=False)
                    if qa_answers:
                        for q, a in qa_answers.items():
                            embed.add_field(name=q[:256], value=a[:1024], inline=False)
                    await new_channel.send(embed=embed, view=close_view)

                    if is_ticket and topic.get('ping_staff_on_create', False):
                        mentions = [guild.get_role(rid).mention for rid in topic.get("staff_role_ids", []) if guild.get_role(rid)]
                        if mentions:
                            await new_channel.send(f"{' '.join(mentions)} - New ticket opened!", delete_after=5)

                    if is_ticket and topic.get('claim_enabled', False):
                        await self._send_claim_alert(topic, new_channel, member)
                except Exception as e:
                    logger.error(f"Error sending welcome content to channel {new_channel.id}: {e}")

                return new_channel

            else:  # Thread mode
                if not isinstance(parent, discord.TextChannel):
                    raise ValueError("Parent must be a text channel.")
                if is_ticket and not has_unique_names:
                    all_threads = list(parent.threads)
                    try:
                        async for t in parent.archived_threads(limit=100):
                            all_threads.append(t)
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

                try:
                    if is_ticket:
                        await ch.add_user(member)
                        embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                        if user_answer:
                            answer_q = topic.get('pre_modal_answer_question', 'Response')
                            embed.add_field(name=answer_q, value=user_answer, inline=False)
                        if qa_answers:
                            for q, a in qa_answers.items():
                                embed.add_field(name=q[:256], value=a[:1024], inline=False)
                        embed.set_footer(text=f"Ticket Topic: {topic_name} | Opener: {member.id}")
                        await ch.send(embed=embed, view=close_view)

                    mentions = [guild.get_role(rid).mention for rid in topic.get("staff_role_ids", []) if guild.get_role(rid)]
                    if mentions:
                        mention_text = ' '.join(mentions)
                        if is_ticket and topic.get('ping_staff_on_create', False):
                            await ch.send(f"{mention_text} - New ticket opened!", delete_after=5)
                        else:
                            await ch.send(mention_text, delete_after=2)

                    if is_ticket and topic.get('claim_enabled', False):
                        await self._send_claim_alert(topic, ch, member)
                except Exception as e:
                    logger.error(f"Error sending welcome content to thread {ch.id}: {e}")

                return ch
        except Exception as e:
            logger.error(f"Error in _create_discussion_channel: {e}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(
                        "Failed to create your ticket. The bot may be missing permissions or the parent is misconfigured.",
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        "Failed to create your ticket. The bot may be missing permissions or the parent is misconfigured.",
                        ephemeral=True
                    )
            except discord.HTTPException:
                pass
            return None

    async def _send_claim_alert(self, topic: Dict[str, Any],
                                 channel: Union[discord.TextChannel, discord.Thread],
                                 opener: Union[discord.Member, discord.User],
                                 qa_embed: Optional[discord.Embed] = None):
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

        alert_embed = discord.Embed(title=f"New {topic_type.capitalize()} Awaiting Claim", color=discord.Color.orange())
        alert_embed.add_field(name="From", value=f"{opener.mention} ({opener.name})", inline=True)
        alert_embed.add_field(name="Topic", value=topic_label, inline=True)
        alert_embed.add_field(name="Link", value=channel.mention, inline=True)
        alert_embed.timestamp = discord.utils.utcnow()

        view = ClaimAlertView(self.bot, topic_name, channel.id, opener.id, qa_embed=qa_embed)
        try:
            embeds = [alert_embed]
            if qa_embed:
                embeds.append(qa_embed)
            await alerts_channel.send(embeds=embeds, view=view)
        except discord.Forbidden:
            logger.warning(f"Missing permissions to send claim alert to {alerts_channel_id}")
        except discord.HTTPException as e:
            logger.error(f"Failed to send claim alert: {e}")

    async def _create_application_discussion(self, topic: Dict[str, Any],
                                              member: discord.Member,
                                              guild: discord.Guild) -> Optional[Union[discord.TextChannel, discord.Thread]]:
        try:
            topic_name = topic.get('name', 'app')
            fmt = topic.get('channel_name_format')
            if topic.get('use_numbering', False) or (fmt and '{number}' in fmt):
                counter = topic.get('ticket_counter', 0) + 1
                topic['ticket_counter'] = counter
                topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                if topic_name in topics:
                    topics[topic_name]['ticket_counter'] = counter
                    await _save_json(self.bot, TOPICS_FILE, topics, self.topics_lock)

            channel_name = format_channel_name(topic, member.name)

            parent_id = topic.get('parent_id')
            if not parent_id:
                return None
            parent = guild.get_channel(parent_id)
            if not parent:
                return None

            channel_topic_str = f"Ticket Topic: {topic_name} | Opener: {member.id}"
            welcome_template = topic.get("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
            welcome_message = welcome_template.format(user=member.mention, topic=topic.get('label', 'N/A'))
            close_view = CloseTicketView(topic_name=topic_name, opener_id=member.id)

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
                try:
                    embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                    await new_channel.send(content=member.mention, embed=embed, view=close_view)
                except Exception as e:
                    logger.error(f"Error sending welcome content to channel {new_channel.id}: {e}")
                return new_channel
            else:
                if not isinstance(parent, discord.TextChannel):
                    return None
                ch = await parent.create_thread(name=channel_name, type=discord.ChannelType.private_thread)
                try:
                    await ch.add_user(member)
                except discord.HTTPException:
                    pass
                try:
                    mentions = [guild.get_role(rid).mention for rid in topic.get("staff_role_ids", []) if guild.get_role(rid)]
                    if mentions:
                        await ch.send(' '.join(mentions), delete_after=2)
                    embed = discord.Embed(description=welcome_message, color=discord.Color.dark_grey())
                    embed.set_footer(text=f"Ticket Topic: {topic_name} | Opener: {member.id}")
                    await ch.send(content=member.mention, embed=embed, view=close_view)
                except Exception as e:
                    logger.error(f"Error sending welcome content to thread {ch.id}: {e}")
                return ch
        except Exception as e:
            logger.error(f"Error in _create_application_discussion: {e}")
            return None

    # --- Listeners ---

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.persistent_views_added:
            asyncio.create_task(self.load_persistent_views())

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if not interaction.data or "custom_id" not in interaction.data:
            return
        custom_id = interaction.data["custom_id"]

        # Close buttons are handled by the persistent CloseTicketView — skip here
        if custom_id.startswith("close_ticket_button::"):
            return

        if custom_id.startswith(("claim_ticket::", "claim_join::", "claim_close::")):
            parts = custom_id.split("::")
            if len(parts) >= 4:
                topic_name = parts[1]
                try:
                    channel_id = int(parts[2])
                    opener_id = int(parts[3])
                except ValueError:
                    return await interaction.response.send_message("Invalid claim context.", ephemeral=True)

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
            t_data = _ensure_topic_defaults(t_data)
            if t_data.get("approval_mode") or not t_data.get("discussion_mode"):
                self.bot.add_view(ApprovalView(self.bot, t_data))
            if t_data.get("questions"):
                self.bot.add_view(StartSurveyView(t_data, self.bot))

        self.persistent_views_added = True
        logger.info("Persistent ticket, survey, and action views have been loaded.")

    def _ordered_panel_items(self, panel_data: Dict[str, Any], all_topics: Dict[str, Any]) -> List[tuple]:
        """Return ordered list of ('topic', name, data) or ('category', slug, data) tuples."""
        attached = set(panel_data.get('topic_names', []))
        categories = panel_data.get('categories', {})
        topic_order = panel_data.get('topic_order', [])

        # Determine which topics are in categories
        categorized: set = set()
        for cat_data in categories.values():
            for tn in cat_data.get('topic_names', []):
                if tn in attached:
                    categorized.add(tn)

        result: List[tuple] = []
        seen_cats: set = set()
        seen_topics: set = set()

        for item_name in topic_order:
            if item_name.startswith("cat:"):
                cat_slug = item_name[4:]
                if cat_slug in categories and cat_slug not in seen_cats:
                    cat_data = categories[cat_slug]
                    valid = [n for n in cat_data.get('topic_names', []) if n in attached and n in all_topics]
                    if valid:
                        enriched = dict(cat_data)
                        enriched["_slug"] = cat_slug
                        enriched["_valid_topic_names"] = valid
                        result.append(("category", cat_slug, enriched))
                        seen_cats.add(cat_slug)
            elif item_name in attached and item_name in all_topics and item_name not in categorized and item_name not in seen_topics:
                result.append(("topic", item_name, all_topics[item_name]))
                seen_topics.add(item_name)

        # Append categories not in topic_order
        for cat_slug, cat_data in categories.items():
            if cat_slug not in seen_cats:
                valid = [n for n in cat_data.get('topic_names', []) if n in attached and n in all_topics]
                if valid:
                    enriched = dict(cat_data)
                    enriched["_slug"] = cat_slug
                    enriched["_valid_topic_names"] = valid
                    result.append(("category", cat_slug, enriched))
                    seen_cats.add(cat_slug)

        # Append uncategorized topics not in topic_order
        for n in panel_data.get('topic_names', []):
            if n not in categorized and n not in seen_topics and n in all_topics:
                result.append(("topic", n, all_topics[n]))
                seen_topics.add(n)

        return result

    def _make_select_callback(self, panel_data: Dict[str, Any]):
        async def select_cb(itx: discord.Interaction):
            topic_name = itx.data['values'][0]
            topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
            topic_data = topics.get(topic_name)
            if topic_data:
                await PanelAction(self.bot, topic_data, panel_data).callback(itx)
            else:
                await itx.response.send_message("Topic not found.", ephemeral=True)
        return select_cb

    def create_panel_view(self, panel_data: Dict[str, Any], all_topics: Dict[str, Any]) -> Optional[discord.ui.View]:
        view = discord.ui.View(timeout=None)
        attached = panel_data.get('topic_names', [])
        if not attached:
            return None

        items = self._ordered_panel_items(panel_data, all_topics)
        if not items:
            return None

        display_mode = panel_data.get('display_mode', 'buttons')
        display_map = panel_data.get('topic_display_map', {})

        # Separate categories and uncategorized topics
        category_items = [(slug, data) for kind, slug, data in items if kind == "category"]
        topic_items = [(name, data) for kind, name, data in items if kind == "topic"]

        if display_mode == 'dropdown':
            # Categories always as buttons (can't nest dropdowns)
            for slug, cat_data in category_items:
                view.add_item(CategoryAction(self.bot, cat_data, panel_data))
            if topic_items:
                options = [
                    discord.SelectOption(
                        label=t.get('label', name)[:100],
                        value=name[:100],
                        emoji=t.get('emoji')
                    ) for name, t in topic_items
                ][:25]
                select = discord.ui.Select(
                    placeholder="Select an option...",
                    options=options,
                    custom_id=f"panel_select::{panel_data['name'][:80]}"
                )
                select.callback = self._make_select_callback(panel_data)
                view.add_item(select)

        elif display_mode == 'mixed':
            # Categories always as buttons
            for slug, cat_data in category_items:
                view.add_item(CategoryAction(self.bot, cat_data, panel_data))

            button_topics = [(n, t) for n, t in topic_items if display_map.get(n, 'button') == 'button']
            dropdown_topics = [(n, t) for n, t in topic_items if display_map.get(n) == 'dropdown']

            for name, t_data in button_topics:
                view.add_item(PanelAction(self.bot, t_data, panel_data))

            if dropdown_topics:
                options = [
                    discord.SelectOption(
                        label=t.get('label', name)[:100],
                        value=name[:100],
                        emoji=t.get('emoji')
                    ) for name, t in dropdown_topics
                ][:25]
                select = discord.ui.Select(
                    placeholder="Anything else...",
                    options=options,
                    custom_id=f"panel_select::{panel_data['name'][:80]}"
                )
                select.callback = self._make_select_callback(panel_data)
                view.add_item(select)

        else:  # buttons — preserve interleaved order
            for kind, key, data in items:
                if kind == "category":
                    view.add_item(CategoryAction(self.bot, data, panel_data))
                else:
                    view.add_item(PanelAction(self.bot, data, panel_data))

        return view if view.children else None

    async def _refresh_panels_for_topic(self, guild: discord.Guild, topic_name: str):
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
                    logger.warning(f"Failed to refresh panel '{panel_data.get('name')}': {e}")
        except Exception as e:
            logger.error(f"Error refreshing panels for topic '{topic_name}': {e}", exc_info=True)

    # --- Ticket close ---

    async def _get_ticket_context(self, channel: Union[discord.TextChannel, discord.Thread]) -> Tuple[Optional[str], Optional[int]]:
        topic_str = ""
        if isinstance(channel, discord.TextChannel):
            topic_str = channel.topic or ""
        elif isinstance(channel, discord.Thread):
            try:
                async for msg in channel.history(limit=5, oldest_first=True):
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
        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
        topic_data = topics.get(topic_name)

        user_is_opener = (interaction.user.id == opener_id)
        user_is_staff = False

        if topic_data:
            staff_role_ids = set(topic_data.get("staff_role_ids", []))
            user_role_ids = {role.id for role in interaction.user.roles}
            if staff_role_ids.intersection(user_role_ids):
                user_is_staff = True

        member_can_close = topic_data.get("member_can_close", True) if topic_data else True
        if user_is_opener and not member_can_close and not user_is_staff:
            return await interaction.followup.send("Members cannot close their own tickets for this topic.", ephemeral=True)
        if not user_is_opener and not user_is_staff:
            return await interaction.followup.send("You do not have permission to close this ticket.", ephemeral=True)

        delete_on_close = topic_data.get("delete_on_close", True) if topic_data else True
        close_msg_template = (topic_data.get("close_message", "Your ticket `{channel}` in **{server}** has been closed by {closer}.")
                              if topic_data else "Your ticket `{channel}` in **{server}** has been closed by {closer}.")

        confirm_view = discord.ui.View(timeout=60)
        confirm_btn = discord.ui.Button(label="Confirm Close", style=discord.ButtonStyle.danger)
        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

        async def confirm_callback(itx: discord.Interaction):
            await itx.response.defer()
            # Send custom close message to opener
            try:
                opener = await self.bot.fetch_user(opener_id)
                if opener and opener.id != itx.user.id:
                    close_msg = close_msg_template.format(
                        channel=itx.channel.name, server=itx.guild.name,
                        closer=itx.user.display_name, user=opener.display_name
                    )
                    await opener.send(close_msg)
            except (discord.Forbidden, discord.NotFound, discord.HTTPException, KeyError):
                pass

            try:
                if delete_on_close:
                    await itx.channel.delete(reason=f"Ticket closed by {itx.user} ({itx.user.id})")
                else:
                    if isinstance(itx.channel, discord.Thread):
                        await itx.channel.edit(archived=True, locked=True, reason=f"Ticket closed by {itx.user}")
                        await itx.followup.send("Ticket has been closed and archived.", ephemeral=True)
                    else:
                        await itx.channel.edit(
                            name=f"closed-{itx.channel.name}"[:100],
                            overwrites={itx.guild.default_role: discord.PermissionOverwrite(send_messages=False)},
                            reason=f"Ticket closed by {itx.user}"
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
        await interaction.followup.send(
            f"Are you sure you want to close this ticket? The ticket will be {close_action}.",
            view=confirm_view, ephemeral=True
        )

    # --- Survey/Application Q&A Flow ---

    async def conduct_survey_flow(self, interaction: discord.Interaction, topic: Dict):
        user = interaction.user
        survey_name = topic.get('name', 'unknown')
        topic_type = topic.get('type', 'survey')

        # Rate limit check (admins bypass)
        is_admin = hasattr(user, 'guild_permissions') and user.guild_permissions.administrator
        cooldown_minutes = topic.get('cooldown_minutes', DEFAULT_COOLDOWN_MINUTES)
        if not is_admin:
            user_survey_cooldowns = self.survey_cooldowns.get(user.id, {})
            last_submission = user_survey_cooldowns.get(survey_name)

            if last_submission:
                now = datetime.now(timezone.utc)
                time_since = now - last_submission.replace(tzinfo=timezone.utc)
                if time_since < timedelta(minutes=cooldown_minutes):
                    remaining = last_submission.replace(tzinfo=timezone.utc) + timedelta(minutes=cooldown_minutes)
                    msg = f"You recently submitted this. Please try again {discord.utils.format_dt(remaining, style='R')}."
                    try:
                        if interaction.response.is_done():
                            await interaction.followup.send(msg, ephemeral=True)
                        else:
                            await interaction.response.send_message(msg, ephemeral=True)
                    except discord.HTTPException:
                        pass
                    return

        existing_session = self.active_survey_sessions.get(user.id)
        start_index = 0
        answers = {}

        pre_modal_answer = topic.pop('_pre_modal_user_answer', None)
        if pre_modal_answer:
            pre_modal_question = topic.get('pre_modal_answer_question', 'Pre-Question Response')
            answers[pre_modal_question] = pre_modal_answer

        is_channel_mode = topic.get('application_channel_mode', 'dm') == 'channel'

        if is_channel_mode:
            if user.id in self.active_survey_sessions:
                del self.active_survey_sessions[user.id]
                await self._save_survey_sessions()

            questions = topic.get('questions', [])[:25]
            if not questions:
                return

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

            # Send one persistent message for questions 2+, then edit it each time
            flow_msg = None

            for i in range(1, len(questions)):
                question_text = questions[i]
                answer_view = discord.ui.View(timeout=300)
                future = asyncio.get_running_loop().create_future()

                async def channel_answer_cb(btn_itx: discord.Interaction, idx=i, total=len(questions), fut=future):
                    modal = ResponseModal(title=f"Question {idx+1}/{total}")
                    await btn_itx.response.send_modal(modal)
                    await modal.wait()
                    if not fut.done():
                        fut.set_result(modal)

                answer_btn = discord.ui.Button(label="Answer", style=discord.ButtonStyle.primary)
                answer_btn.callback = channel_answer_cb
                answer_view.add_item(answer_btn)

                embed = discord.Embed(
                    title=f"{topic.get('label')} ({i+1}/{len(questions)})",
                    description=question_text,
                    color=discord.Color.blue()
                )

                try:
                    if flow_msg is None:
                        flow_msg = await latest_interaction.followup.send(
                            embed=embed, view=answer_view, ephemeral=True, wait=True
                        )
                    else:
                        await flow_msg.edit(embed=embed, view=answer_view)
                except discord.HTTPException:
                    return

                try:
                    modal = await asyncio.wait_for(future, timeout=300)
                except asyncio.TimeoutError:
                    try:
                        if flow_msg:
                            await flow_msg.edit(content="Timed out. Please try again.", embed=None, view=None)
                        else:
                            await latest_interaction.followup.send("Timed out. Please try again.", ephemeral=True)
                    except discord.HTTPException:
                        pass
                    return

                answers[question_text] = modal.answer.value
                latest_interaction = modal.modal_interaction

            flow_message = flow_msg
            channel_mode_latest_interaction = latest_interaction

        else:
            channel_mode_latest_interaction = None
            # DM mode
            if existing_session:
                existing_survey = existing_session.get('survey_name')
                if existing_survey != survey_name:
                    try:
                        target_channel = user.dm_channel or await user.create_dm()
                        topics = await _load_json(self.bot, TOPICS_FILE, self.topics_lock)
                        old_topic = topics.get(existing_survey, {})
                        old_label = old_topic.get('label', existing_survey)

                        warn_embed = discord.Embed(
                            title="Abandon Previous Session?",
                            description=f"You have an incomplete **{old_label}** session.\nStarting **{topic.get('label')}** will abandon it. Continue?",
                            color=discord.Color.orange()
                        )
                        warn_view = discord.ui.View(timeout=60)
                        continue_btn = discord.ui.Button(label="Continue", style=discord.ButtonStyle.danger)
                        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
                        user_choice = {"choice": None}

                        async def continue_cb(itx):
                            user_choice["choice"] = "continue"
                            await itx.response.defer()
                            warn_view.stop()

                        async def cancel_cb(itx):
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
                                embed=discord.Embed(title="Cancelled", description="No changes made.", color=discord.Color.red()),
                                view=None
                            )
                            return

                        del self.active_survey_sessions[user.id]
                        await self._save_survey_sessions()
                        await warn_msg.delete()
                        existing_session = None
                    except (discord.Forbidden, discord.HTTPException):
                        pass

            if existing_session and existing_session.get('survey_name') == survey_name:
                try:
                    target_channel = user.dm_channel or await user.create_dm()
                    progress = existing_session.get('current_question', 0)
                    total = len(topic.get('questions', []))

                    resume_embed = discord.Embed(
                        title=f"Resume {topic.get('label')}?",
                        description=f"You have an incomplete session (Question {progress + 1}/{total}).\nResume or start over?",
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
                    else:
                        await resume_msg.edit(
                            embed=discord.Embed(title="Cancelled", color=discord.Color.red()), view=None
                        )
                        return
                    await resume_msg.delete()
                except (discord.Forbidden, discord.HTTPException):
                    pass

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

            for i in range(start_index, len(questions)):
                question_text = questions[i]
                answer_view = discord.ui.View(timeout=300)
                future = asyncio.get_running_loop().create_future()

                async def answer_cb(btn_itx: discord.Interaction, idx=i, total=len(questions)):
                    modal = ResponseModal(title=f"Question {idx+1}/{total}")
                    await btn_itx.response.send_modal(modal)
                    await modal.wait()
                    if not future.done():
                        future.set_result(modal.answer.value)

                answer_btn = discord.ui.Button(label="Answer", style=discord.ButtonStyle.primary)
                answer_btn.callback = answer_cb
                answer_view.add_item(answer_btn)

                embed = discord.Embed(
                    title=f"{topic.get('label')} ({i+1}/{len(questions)})",
                    description=question_text,
                    color=discord.Color.blue()
                )

                try:
                    await flow_message.edit(embed=embed, view=answer_view)
                except discord.HTTPException:
                    return

                try:
                    answer = await asyncio.wait_for(future, timeout=300)
                except asyncio.TimeoutError:
                    answer = None

                if answer is None:
                    embed.color = discord.Color.orange()
                    for item in answer_view.children:
                        item.disabled = True
                    try:
                        await flow_message.edit(embed=embed, view=answer_view)
                    except discord.HTTPException:
                        pass
                    return

                answers[question_text] = answer
                self.active_survey_sessions[user.id] = {
                    "survey_name": survey_name,
                    "started_at": session_data["started_at"],
                    "current_question": i + 1,
                    "answers": answers,
                    "flow_message_id": flow_message.id,
                    "channel_id": target_channel.id
                }
                await self._save_survey_sessions()

        # --- Finalize: feature-flag based, not type-branched ---
        results_embed = discord.Embed(
            title=f"New Response: {topic.get('label')}",
            description=f"Submitted by {user.mention} ({user.id})",
            color=discord.Color.teal(),
            timestamp=discord.utils.utcnow()
        )
        for question, answer in answers.items():
            results_embed.add_field(name=question[:256], value=answer[:1024], inline=False)

        # Save response data
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

        # Discussion channel creation (feature flag, not type check)
        discussion_channel = None
        if topic.get('discussion_mode') or claim_enabled:
            guild_id = topic.get('_guild_id')
            guild = self.bot.get_guild(guild_id) if guild_id else None
            member = guild.get_member(user.id) if guild else None
            if member and guild:
                discussion_channel = await self._create_application_discussion(topic, member, guild)

        if discussion_channel:
            results_embed.add_field(name="Discussion", value=discussion_channel.mention, inline=True)

        # Determine view and target channel (feature flag based)
        alerts_channel_id = topic.get('claim_alerts_channel_id') if claim_enabled else None
        target_channel_id = alerts_channel_id or log_channel_id

        if claim_enabled and discussion_channel:
            member = self.bot.get_guild(topic.get('_guild_id')).get_member(user.id) if topic.get('_guild_id') else None
            view = ClaimAlertView(self.bot, survey_name, discussion_channel.id, user.id) if member else None
        elif topic.get('approval_mode'):
            view = ApprovalView(self.bot, topic)
        else:
            view = None

        created_ticket_ch = None
        if target_channel_id:
            target_ch = self.bot.get_channel(target_channel_id)
            if target_ch:
                try:
                    await target_ch.send(embed=results_embed, view=view)
                except discord.Forbidden:
                    logger.warning(f"Missing permissions for channel {target_channel_id}")
        else:
            # No log/alerts channel — create a ticket channel with Q&A in the welcome embed
            guild_id = topic.get('_guild_id')
            guild = self.bot.get_guild(guild_id) if guild_id else None
            member = guild.get_member(user.id) if guild else None
            if member and guild and not discussion_channel:
                created_ticket_ch = await self._create_discussion_channel(
                    interaction, topic, member, is_ticket=True, qa_answers=answers
                )
            elif discussion_channel:
                try:
                    await discussion_channel.send(embed=results_embed, view=view)
                except discord.Forbidden:
                    logger.warning(f"Missing permissions for discussion channel {discussion_channel.id}")

        # Clear session and set cooldown
        if user.id in self.active_survey_sessions:
            del self.active_survey_sessions[user.id]
            await self._save_survey_sessions()

        if user.id not in self.survey_cooldowns:
            self.survey_cooldowns[user.id] = {}
        self.survey_cooldowns[user.id][survey_name] = datetime.now(timezone.utc)

        # Build success message with link to ticket if one was created
        link_ch = created_ticket_ch or discussion_channel
        desc = "Thank you! Your responses have been submitted."
        if link_ch:
            desc += f"\n\nYour ticket: {link_ch.mention}"
        success_embed = discord.Embed(
            title=f"{topic.get('label')} Completed",
            description=desc,
            color=discord.Color.green()
        )

        if is_channel_mode:
            try:
                if flow_message:
                    await flow_message.edit(embed=success_embed, view=None)
                elif channel_mode_latest_interaction:
                    await channel_mode_latest_interaction.followup.send(embed=success_embed, ephemeral=True)
            except discord.HTTPException:
                pass
        else:
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

    # --- Commands ---

    @app_commands.command(name="ticketing", description="Open the ticketing admin dashboard.")
    @app_commands.default_permissions(administrator=True)
    async def ticketing_cmd(self, interaction: discord.Interaction):
        dashboard = AdminDashboardView(self)
        embed = await dashboard._embed()
        await interaction.response.send_message(embed=embed, view=dashboard, ephemeral=True)
