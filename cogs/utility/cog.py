"""The Utility cog — reminders, one-off messages and always-on channel rules.

Two kinds of thing live here. *Reminders* (and the one-off messages and sticky
notes built the same way) are content the bot posts: on a schedule, on a repeat
interval, or once. *Channel rules* — media-only channels and reaction rules —
are things that are simply true of a channel all the time. Anything
event-driven lives in the Automations cog next door, which used to be a section
of this one. This cog absorbed the old Reminders and Send-Message cogs; the
mass-DM cog was removed outright.

Everything is configured from the web dashboard, which writes this cog's
database directly and bumps `settings['revision']`; `dashboard_sync` watches
that integer and reloads. There is no Discord admin panel any more.

The listeners in this file are on hot paths — `on_message` runs for every
message in the server — so rule lookup goes through an in-memory cache keyed by
channel id, refreshed only when the dashboard writes. The reminder loop reads
the database directly, since it ticks only once a minute.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import discord
from discord.ext import commands, tasks

from .features import media as media_feature
from .features import reactions as reactions_feature
from .features import reminders as reminders_feature
from .features import sticky as sticky_feature
from .storage import (
    UtilityDB,
    migrate_legacy_json,
    migrate_reminders_json,
    repair_reminder_ids,
)

logger = logging.getLogger('cogs.utility')


class Utility(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = UtilityDB()
        self._ready = False
        # channel_id -> list of rules. Threads resolve to their parent, so a
        # rule on a forum channel covers the posts inside it.
        self._media_cache: dict[int, list] = {}
        self._reaction_cache: dict[int, list] = {}
        self._sticky_cache: dict[int, Any] = {}
        # The dashboard bumps settings['revision'] on every write; we watch it
        # and reload the channel-keyed caches when it moves. Reminders are read
        # straight from the database in their loop, so they need no cache.
        self._revision = 0

    # ------------------------------------------------------------ lifecycle

    async def cog_load(self):
        await self.db.connect()
        await migrate_legacy_json(self.db)
        await migrate_reminders_json(self.db)
        await repair_reminder_ids(self.db)
        await self.refresh_cache()
        self._revision = await self.db.get_revision()

        # Without this the thread-delete buttons stop working on restart.
        try:
            self.bot.add_dynamic_items(media_feature.ThreadDeleteButton)
        except Exception as e:
            logger.warning(f"Could not register dynamic items: {e}")

        self.maintenance_tick.start()
        self.reminder_tick.start()
        self.dashboard_sync.start()
        self._ready = True
        logger.info(
            f"Utility ready — {sum(len(v) for v in self._media_cache.values())} media "
            f"rule(s), {sum(len(v) for v in self._reaction_cache.values())} reaction rule(s)."
        )

    async def cog_unload(self):
        self.maintenance_tick.cancel()
        self.reminder_tick.cancel()
        self.dashboard_sync.cancel()
        await self.db.close()

    # ---------------------------------------------------------------- cache

    async def refresh_cache(self):
        """Rebuild the channel-keyed rule caches from the database."""
        media: dict[int, list] = {}
        for row in await self.db.fetchall(
            "SELECT * FROM media_channels WHERE enabled = 1"
        ):
            media.setdefault(row["channel_id"], []).append(row)

        reactions: dict[int, list] = {}
        for row in await self.db.fetchall(
            "SELECT * FROM reaction_rules WHERE enabled = 1"
        ):
            reactions.setdefault(row["channel_id"], []).append(row)

        stickies: dict[int, Any] = {}
        for row in await self.db.fetchall(
            "SELECT * FROM sticky_messages WHERE enabled = 1"
        ):
            stickies[row["channel_id"]] = row

        self._media_cache = media
        self._reaction_cache = reactions
        self._sticky_cache = stickies

    async def save_made(self):
        """Called after any write from the panel.

        The shared panel scaffolding in `utils/panel_rules.py` calls this
        rather than `refresh_cache` directly, because a cog whose data is also
        read by another process has more to do here. This one owns its data
        alone, so a reload is the whole job.
        """
        await self.refresh_cache()

    # ------------------------------------------------------------- helpers

    async def _resolve_channel(self, channel_id: int):
        channel = self.bot.get_channel(channel_id)
        # A cached thread can be missing parent_id; treat that as a cache miss
        # rather than concluding it has no parent.
        if isinstance(channel, discord.Thread) and not channel.parent_id:
            channel = None
        if channel is not None:
            return channel
        try:
            return await self.bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
            logger.debug(f"Could not resolve channel {channel_id}: {e}")
            return None

    @staticmethod
    def _parent_id(channel) -> Optional[int]:
        return getattr(channel, "parent_id", None)

    # ----------------------------------------------------------- listeners

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not self._ready or message.guild is None:
            return

        sticky = self._sticky_cache.get(message.channel.id)
        if sticky is not None:
            try:
                await sticky_feature.on_message(self, message, sticky)
            except Exception as e:
                logger.error(f"Sticky handling failed in {message.channel.id}: {e}")

        if message.author.bot or not self._media_cache:
            return

        rules = self._media_cache.get(message.channel.id)
        if rules is None:
            # A post inside a thread belongs to the parent channel's rule, but
            # only for enforcement of the *parent* — comments in the thread
            # under a media post are meant to be plain text, so a thread whose
            # parent has a rule is explicitly left alone.
            if isinstance(message.channel, discord.Thread):
                return
            return

        for rule in rules:
            removed = await media_feature.handle_message(self, message, rule)
            if removed:
                return

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if not self._ready or not payload.guild_id:
            return
        if self.bot.user and payload.user_id == self.bot.user.id:
            return
        if not self._reaction_cache:
            return

        rules = self._reaction_cache.get(payload.channel_id)
        channel = None
        if rules is None:
            # A thread carries its own channel id, so a rule set on the parent
            # channel has to be found through it.
            channel = await self._resolve_channel(payload.channel_id)
            parent_id = self._parent_id(channel)
            if not parent_id:
                return
            candidates = self._reaction_cache.get(parent_id)
            if not candidates:
                return
            rules = [r for r in candidates if r["include_threads"]]
            if not rules:
                return

        if channel is None:
            channel = await self._resolve_channel(payload.channel_id)
        if channel is None:
            return

        member = payload.member
        if member is None and payload.guild_id:
            guild = self.bot.get_guild(payload.guild_id)
            member = guild.get_member(payload.user_id) if guild else None

        for rule in rules:
            if await reactions_feature.apply(self, payload, rule, channel, member):
                return

    # ------------------------------------------------------------ upkeep

    @tasks.loop(hours=6)
    async def maintenance_tick(self):
        try:
            await self.db.prune_audit()
        except Exception as e:
            logger.error(f"Utility maintenance failed: {e}", exc_info=True)

    @maintenance_tick.before_loop
    async def _before_maintenance(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------------ reminders

    @tasks.loop(minutes=1)
    async def reminder_tick(self):
        await reminders_feature.process_tick(self)

    @reminder_tick.before_loop
    async def _before_reminders(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------ dashboard bridge

    @tasks.loop(seconds=10)
    async def dashboard_sync(self):
        """Reload the caches when the website has written something.

        Reminders are read from the database in their own loop, so this only
        has to refresh the channel-keyed caches (media, reactions, stickies)
        that the hot-path listeners read from memory.
        """
        try:
            revision = await self.db.get_revision()
            if revision != self._revision:
                self._revision = revision
                await self.refresh_cache()
                logger.info(f"Utility caches reloaded for dashboard revision {revision}.")
        except Exception as e:
            logger.error(f"Utility dashboard sync failed: {e}", exc_info=True)

    @dashboard_sync.before_loop
    async def _before_sync(self):
        await self.bot.wait_until_ready()

    # ---------------------------------------------------- role-button clicks

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Toggle a role when someone clicks a reminder/sticky role button.

        Handled here rather than through a persistent view so a button posted
        by any past reminder keeps working across restarts without the cog
        having to re-register a view per message.
        """
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = str((interaction.data or {}).get("custom_id", ""))
        if not custom_id.startswith("remind_role:"):
            return
        if not interaction.guild:
            await interaction.response.send_message(
                "This button only works in servers.", ephemeral=True)
            return
        try:
            role = interaction.guild.get_role(int(custom_id.split(":")[1]))
        except (ValueError, IndexError):
            await interaction.response.send_message("Invalid role.", ephemeral=True)
            return
        if role is None:
            await interaction.response.send_message("Role not found.", ephemeral=True)
            return
        if role.managed:
            await interaction.response.send_message(
                f"{role.mention} is managed by an integration and cannot be assigned by hand.",
                ephemeral=True)
            return
        if not interaction.guild.me.guild_permissions.manage_roles:
            await interaction.response.send_message(
                "I don't have the Manage Roles permission here.", ephemeral=True)
            return
        if role >= interaction.guild.me.top_role:
            await interaction.response.send_message(
                f"I can't manage {role.mention} — it's at or above my highest role. "
                f"Move my role higher in Server Settings → Roles.", ephemeral=True)
            return
        try:
            if role in interaction.user.roles:
                await interaction.user.remove_roles(role, reason="Reminder role button")
                await interaction.response.send_message(f"Removed {role.mention}", ephemeral=True)
            else:
                await interaction.user.add_roles(role, reason="Reminder role button")
                await interaction.response.send_message(f"Added {role.mention}", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(
                "Permission denied managing that role.", ephemeral=True)
        except discord.HTTPException as e:
            logger.error(f"Role toggle failed: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message("Error managing role.", ephemeral=True)
