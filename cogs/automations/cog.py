"""The Automations cog — triggers, the engine, and everything that runs on time.

Automations began as a section inside the Utility cog and outgrew it: the
engine, its registry, its scheduler and its builder are together larger than
every other utility feature combined, and they share nothing with media
channels or sticky messages beyond the panel framework in `utils/`. Being its
own cog means it can be reloaded on its own, its listeners live next to the
engine they feed, and the website has one obvious thing to talk to.

The listeners here are on hot paths — `on_message` runs for every message in
the server — so the trigger lookup goes through an in-memory dict keyed by
trigger type, rebuilt only when something writes. A message that no automation
is listening for costs one dict lookup and nothing else.

Edits made on the dashboard land straight in `automations.db` from another
process, so `revision_tick` watches a counter in that file and reloads the
cache when it moves. That is what lets someone build an automation on their
phone and have it running seconds later without touching the bot.
"""

from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands, tasks

from utils.events import EventContext
from utils.perms import is_bot_admin

from . import engine, scheduler
from .buttons import AutomationButton
from .models import Automation, walk_steps
from .storage import (KILL_SWITCH_KEY, AutomationsDB, migrate_from_utility)

logger = logging.getLogger('cogs.automations')


class Automations(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = AutomationsDB()
        self._ready = False
        # trigger_type -> automations, priority-ordered. Rebuilt on save, so
        # the listeners never touch the database to decide there is nothing
        # to do.
        self._automations: dict[str, list] = {}
        self._automations_on = True
        # The revision this cache was built from, so the watcher can tell an
        # outside edit from its own writes.
        self._revision = 0

    # ------------------------------------------------------------ lifecycle

    async def cog_load(self):
        await self.db.connect()
        await migrate_from_utility(self.db)
        await self.refresh_cache()

        # Teach the bot to recognise our message buttons after any restart.
        # They carry the click handler for the "button that opens a private
        # reply" step, and a message posted days ago must keep working.
        try:
            self.bot.add_dynamic_items(AutomationButton)
        except ValueError:
            # Already registered — a cog reload rather than a cold start.
            pass

        # Catch up on anything that came due while the bot was down *before*
        # the loop starts, so a restart never strands a temporary role or a
        # half-finished automation.
        await scheduler.sweep(self)

        self.scheduler_tick.start()
        self.revision_tick.start()
        self.maintenance_tick.start()
        self._ready = True
        logger.info(
            f"Automations ready — {sum(len(v) for v in self._automations.values())} "
            f"live automation(s) across {len(self._automations)} trigger(s)."
        )

    async def cog_unload(self):
        self.scheduler_tick.cancel()
        self.revision_tick.cancel()
        self.maintenance_tick.cancel()
        await self.db.close()

    # ---------------------------------------------------------------- cache

    async def refresh_cache(self):
        """Rebuild the trigger-keyed automation cache from the database."""
        automations: dict[str, list] = {}
        for row in await self.db.fetchall(
            "SELECT * FROM automations WHERE enabled = 1 ORDER BY priority, rowid"
        ):
            try:
                parsed = Automation.from_row(row)
            except Exception as e:
                # A single malformed graph must not take every automation with
                # it — skip it and leave the rest working.
                logger.error(f"Skipping unreadable automation {row['id']}: {e}")
                continue
            automations.setdefault(parsed.trigger_type, []).append(parsed)

        self._automations = automations
        self._automations_on = await self.db.get_bool(KILL_SWITCH_KEY, True)
        self._revision = await self.db.revision()

    async def save_made(self):
        """Called after any write from inside the bot.

        Bumps the shared revision counter as well as reloading, so a dashboard
        open in a browser can tell that what it is showing is now stale.
        """
        await self.db.bump_revision()
        await self.refresh_cache()

    def is_admin(self, user) -> bool:
        return is_bot_admin(user, self.bot)

    # ---------------------------------------------------- engine interface

    async def automations_enabled(self) -> bool:
        return self._automations_on

    def automations_for(self, trigger_type: str) -> list:
        return self._automations.get(trigger_type, [])

    async def find_button_step(self, button_id: str):
        """The send step a clicked button belongs to, or None.

        Read straight from the database rather than the trigger cache, so a
        button still answers even if its automation has since been switched off,
        and so an edit to the reply on the dashboard takes effect immediately.
        Matched by the stable id stamped into the step, not its position, so
        reordering steps never breaks a button already posted.
        """
        for row in await self.db.fetchall("SELECT * FROM automations"):
            try:
                automation = Automation.from_row(row)
            except Exception:
                continue
            for _path, step, _depth in walk_steps(automation.steps):
                if (step.type == "send_message"
                        and str(step.config.get("button_id") or "") == button_id):
                    return step
        return None

    async def fire(self, trigger_type: str, ctx: EventContext):
        """Hand an event to the engine, if anything is listening for it.

        Wrapped so a bug in one automation can never propagate into the
        listener that produced the event — an exception escaping here would
        stop discord.py dispatching that event to other cogs too.
        """
        if not self._ready or not self._automations_on:
            return
        if not self._automations.get(trigger_type):
            return
        try:
            await engine.handle(self, trigger_type, ctx)
        except Exception as e:
            logger.error(f"Automation dispatch for {trigger_type} failed: {e}",
                         exc_info=True)

    # ----------------------------------------------------------- listeners

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not self._ready or message.guild is None:
            return
        # Automations see bot messages too — the engine drops them per
        # automation unless one has opted in, which is a decision the admin
        # should get to make rather than the listener.
        await self.fire("message_sent", EventContext.from_message(self.bot, message))

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if after.guild is None:
            return
        await self.fire("message_edited",
                        EventContext.from_message(self.bot, after, "message_edited"))

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if message.guild is None:
            return
        await self.fire("message_deleted",
                        EventContext.from_message(self.bot, message, "message_deleted"))

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await self.fire("member_joined",
                        EventContext.from_member(self.bot, member, "member_joined"))

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        await self.fire("member_left",
                        EventContext.from_member(self.bot, member, "member_left"))

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        gained = set(after.roles) - set(before.roles)
        lost = set(before.roles) - set(after.roles)
        for role in gained:
            await self.fire("role_added", EventContext.from_member(
                self.bot, after, "role_added", role_id=role.id, role_name=role.name))
        for role in lost:
            await self.fire("role_removed", EventContext.from_member(
                self.bot, after, "role_removed", role_id=role.id, role_name=role.name))

        if before.nick != after.nick:
            await self.fire("nickname_changed", EventContext.from_member(
                self.bot, after, "nickname_changed",
                old_nick=before.nick, new_nick=after.nick))

        # premium_since going from None to a timestamp is the boost starting;
        # the reverse is it lapsing, which is not what this trigger means.
        if before.premium_since is None and after.premium_since is not None:
            await self.fire("member_boosted", EventContext.from_member(
                self.bot, after, "member_boosted"))

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before, after):
        if before.channel is None and after.channel is not None:
            ctx = EventContext.from_member(self.bot, member, "voice_joined")
            ctx.channel = after.channel
            await self.fire("voice_joined", ctx)
        elif before.channel is not None and after.channel is None:
            ctx = EventContext.from_member(self.bot, member, "voice_left")
            ctx.channel = before.channel
            await self.fire("voice_left", ctx)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if not self._ready or not payload.guild_id:
            return
        if self.bot.user and payload.user_id == self.bot.user.id:
            return
        if not self._automations.get("reaction_added"):
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        member = payload.member or guild.get_member(payload.user_id)
        channel = guild.get_channel_or_thread(payload.channel_id)
        ctx = EventContext(
            event="reaction_added", bot=self.bot, guild=guild,
            member=member, user=member, channel=channel,
            extra={"emoji": str(payload.emoji),
                   "message_id": payload.message_id},
        )
        # Fetched only because an automation is actually listening — the
        # reaction-rule path in the Utility cog still avoids it.
        if channel is not None:
            try:
                ctx.message = await channel.fetch_message(payload.message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        await self.fire("reaction_added", ctx)

    # --- cross-cog events, dispatched by the cogs that own them ---

    @commands.Cog.listener()
    async def on_member_level_up(self, user_id, old_level, new_level):
        # Economy XP is global rather than per-guild, so this event carries no
        # guild id — the member has to be found the same way newcomer.py does.
        for guild in self.bot.guilds:
            member = guild.get_member(user_id)
            if member is None:
                continue
            await self.fire("member_level_up", EventContext.from_member(
                self.bot, member, "member_level_up",
                old_level=old_level, new_level=new_level))
            return

    @commands.Cog.listener()
    async def on_ticket_opened(self, guild_id, channel_id, user_id, topic):
        await self._fire_guild_event("ticket_opened", guild_id, user_id,
                                     channel_id, topic=topic)

    @commands.Cog.listener()
    async def on_ticket_closed(self, guild_id, channel_id, opener_id, closer_id, topic):
        await self._fire_guild_event("ticket_closed", guild_id, opener_id,
                                     channel_id, topic=topic, closer_id=closer_id)

    @commands.Cog.listener()
    async def on_newcomer_intro_posted(self, guild_id, user_id, thread_id, ign):
        await self._fire_guild_event("newcomer_intro_posted", guild_id, user_id,
                                     thread_id, ign=ign)

    @commands.Cog.listener()
    async def on_newcomer_graduated(self, guild_id, user_id, reason):
        await self._fire_guild_event("newcomer_graduated", guild_id, user_id,
                                     None, reason=reason)

    @commands.Cog.listener()
    async def on_vc_lobby_created(self, guild_id, channel_id, owner_id, is_basic):
        await self._fire_guild_event("vc_lobby_created", guild_id, owner_id,
                                     channel_id, is_basic=is_basic)

    @commands.Cog.listener()
    async def on_vc_lobby_closed(self, guild_id, channel_id, owner_id):
        await self._fire_guild_event("vc_lobby_closed", guild_id, owner_id,
                                     None, closed_channel_id=channel_id)

    async def _fire_guild_event(self, trigger: str, guild_id, user_id,
                                channel_id, **extra):
        """Rebuild a context from the scalar ids the dispatch convention uses."""
        if not self._automations.get(trigger):
            return
        guild = self.bot.get_guild(int(guild_id)) if guild_id else None
        if guild is None:
            return
        member = guild.get_member(int(user_id)) if user_id else None
        ctx = EventContext(event=trigger, bot=self.bot, guild=guild,
                           member=member, user=member, extra=extra)
        if channel_id:
            ctx.channel = guild.get_channel_or_thread(int(channel_id))
        await self.fire(trigger, ctx)

    # ------------------------------------------------------------ upkeep

    @tasks.loop(seconds=30)
    async def scheduler_tick(self):
        """Deferred steps, expiring roles and scheduled triggers.

        30 seconds is the resolution of a `wait` step and a temporary role.
        Sub-minute precision is not worth a second mechanism, and the schedule
        trigger only needs the minute it fires in.
        """
        await scheduler.sweep(self)

    @scheduler_tick.before_loop
    async def _before_scheduler(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=10)
    async def revision_tick(self):
        """Pick up edits made on the dashboard.

        The website writes to automations.db directly, in another process, so
        nothing here is notified. Reading one integer out of a WAL-mode SQLite
        file every ten seconds costs nothing measurable, and it means an
        automation switched on in a browser is live before the person who did
        it has put their phone down.
        """
        try:
            revision = await self.db.revision()
        except Exception as e:
            logger.debug(f"Could not read the automations revision: {e}")
            return
        if revision == self._revision:
            return
        logger.info(f"Automations changed outside the bot "
                    f"(revision {self._revision} → {revision}); reloading.")
        await self.refresh_cache()

    @revision_tick.before_loop
    async def _before_revision(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=30)
    async def maintenance_tick(self):
        try:
            await self.db.prune_runs()
        except Exception as e:
            logger.error(f"Automations maintenance failed: {e}", exc_info=True)

    @maintenance_tick.before_loop
    async def _before_maintenance(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------------ command

    @app_commands.command(name="automations",
                          description="Build and manage automations.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    async def automations_panel(self, interaction: discord.Interaction):
        if not self.is_admin(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to use this command.", ephemeral=True
            )
            return
        from .panel.home import AutomationsHomePage
        page = AutomationsHomePage(self, owner_id=interaction.user.id)
        await page.open(interaction)
