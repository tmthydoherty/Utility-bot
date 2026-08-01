"""Economy — leveling and Points economy.

Leveling is Mee6-style and deliberately silent (no level-up announcements).
Points are earned from the same activity plus participation in the other cogs,
and spent in the shop. Everything tunable lives in /economy_panel.
"""

import asyncio
import contextlib
import json
import logging
import sqlite3
import time
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, tasks

from .cards import EconomyCardGenerator
from .config import Config
from .database import EconomyDB
from .earning import EarningEngine, today_key
from .effects import EffectsManager
from .leveling import LevelingEngine
from .render import build_level_card_data
from .stats import ActivityStats
from .views_level import LeaderboardView, LevelCardView

logger = logging.getLogger('cogs.economy')

QOTD_DB = str(Path(__file__).resolve().parent.parent.parent / "qotd_database.db")

# Retention. The ledger keeps a row per event for full auditability; these
# windows stop it growing without bound over years.
LEDGER_RETENTION_DAYS = 365
DAILY_RETENTION_DAYS = 90


class Economy(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.db = EconomyDB()
        self.config = Config(self.db)
        self.leveling = LevelingEngine(self)
        self.earning = EarningEngine(self)
        self.effects = EffectsManager(self)
        self.stats = ActivityStats()
        self.cards = EconomyCardGenerator()
        # Populated in cog_load; guarded everywhere in case an event lands first.
        self.items = None

        # Message ids of live QOTD posts, so replies to them can be paid.
        self._qotd_anchors: set = set()
        self._qotd_channels: set = set()
        self._qotd_refresh_ts: float = 0.0

        self._ready = asyncio.Event()

    # ------------------------------------------------------------ lifecycle

    async def cog_load(self):
        await self.db.connect()
        await self.config.load()
        await self.stats.connect()
        await self.cards.initialize()
        await self._refresh_qotd_channels()

        # Items register their own persistent views and background hooks.
        from .items import register_all
        self.items = register_all(self)

        self.voice_tick.start()
        self.maintenance_tick.start()
        self.retention_tick.start()
        self._ready.set()
        logger.info("Economy cog loaded.")

    async def cog_unload(self):
        self.voice_tick.cancel()
        self.maintenance_tick.cancel()
        self.retention_tick.cancel()
        await self.cards.close()
        await self.stats.close()
        await self.db.close()

    async def _report(self, where: str, error: Exception):
        """Log and surface a failure instead of swallowing it."""
        logger.error(f"{where}: {error}", exc_info=True)
        reporter = getattr(self.bot, "error_reporter", None)
        if reporter is not None:
            with contextlib.suppress(Exception):
                await reporter.report("Economy", f"{where}: {error}")

    @commands.Cog.listener()
    async def on_ready(self):
        if not await self._wait_ready(timeout=120):
            return
        try:
            await self.effects.restore_on_ready()
        except Exception as e:
            await self._report("effects.restore_on_ready", e)
        if self.items is not None:
            try:
                await self.items.restore(self)
            except Exception as e:
                await self._report("items.restore", e)

    # -------------------------------------------------------- public award

    async def _wait_ready(self, timeout: float = 30.0) -> bool:
        """Wait for load to finish, but never indefinitely.

        Every message handler passes through here; an unbounded wait would let
        handlers pile up without limit if loading ever stalled.
        """
        if self._ready.is_set():
            return True
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning("Economy is not ready — dropping this event.")
            return False

    async def award(self, user_id: int, source: str, *, amount: int = None,
                    meta: dict = None) -> int:
        """Entry point used by utils/economy_award.py from the other cogs."""
        if not await self._wait_ready():
            return 0
        try:
            return await self.earning.award(user_id, source, amount=amount, meta=meta)
        except Exception as e:
            logger.error(f"award({user_id}, {source}) failed: {e}", exc_info=True)
            return 0

    def is_admin(self, user) -> bool:
        checker = getattr(self.bot, "is_bot_admin", None)
        if checker:
            return checker(user)
        return getattr(user, "guild_permissions", None) is not None \
            and user.guild_permissions.administrator

    # ----------------------------------------------------------- listeners

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            # The bot's own QOTD post becomes the anchor replies are paid for.
            if message.guild and message.author.id == self.bot.user.id:
                await self._maybe_track_qotd(message)
            return
        if not message.guild:
            return

        if not await self._wait_ready():
            return

        try:
            # Curses and reaction items run first; a deleted message earns nothing.
            if await self.effects.handle_message(message):
                return
        except Exception as e:
            logger.error(f"effects.handle_message failed: {e}", exc_info=True)

        try:
            await self._handle_activity(message)
        except Exception as e:
            logger.error(f"activity handling failed: {e}", exc_info=True)

        if self.items is not None:
            try:
                await self.items.on_message(self, message)
            except Exception as e:
                await self._report("items.on_message", e)

    async def _handle_activity(self, message: discord.Message):
        user_id = message.author.id

        if self.leveling.channel_excluded(message.channel):
            return

        await self.leveling.grant_message_xp(message)
        await self.earning.award(user_id, "message")

        streak = await self.earning.touch_streak(user_id)
        if streak["first_today"]:
            await self.earning.award(user_id, "first_message")

        await self._check_newcomer_reply(message)
        await self._check_qotd_answer(message)

    async def _check_newcomer_reply(self, message: discord.Message):
        """Pay for replying to someone holding the newcomer role.

        Ordered cheapest-check-first on purpose. Resolving the replied-to
        message can cost an HTTP call, and this runs on every reply in the
        server, so the daily cap is checked from the database first — once a
        user is capped out for the day their replies cost nothing at all.
        """
        role_id = self.config.get_int("role_newcomer", 0)
        if not role_id or not message.reference:
            return

        cap = self.config.earn_cap("newcomer_reply")
        if cap > 0:
            used = await self.db.daily_used(
                message.author.id, today_key(), "newcomer_points"
            )
            if used >= cap:
                return

        # Prefer anything already in memory; only pay for an HTTP round trip
        # when the reply target genuinely is not cached.
        target = message.reference.resolved
        if not isinstance(target, discord.Message):
            target = message.reference.cached_message
        if target is None:
            ref_id = message.reference.message_id
            if not ref_id:
                return
            try:
                target = await message.channel.fetch_message(ref_id)
            except discord.HTTPException:
                return

        author = target.author
        if author.bot or author.id == message.author.id:
            return

        member = message.guild.get_member(author.id)
        if member and any(r.id == role_id for r in member.roles):
            await self.earning.award(
                message.author.id, "newcomer_reply", meta={"target": author.id}
            )

    async def _check_qotd_answer(self, message: discord.Message):
        """Pay for answering the QOTD, whether by reply or in its thread."""
        if not self._qotd_anchors:
            return

        anchor_hit = False
        if message.reference and message.reference.message_id in self._qotd_anchors:
            anchor_hit = True
        else:
            # A thread created on the QOTD post shares the post's message id.
            channel = message.channel
            if isinstance(channel, discord.Thread) and channel.id in self._qotd_anchors:
                anchor_hit = True

        if anchor_hit:
            await self.earning.award(message.author.id, "qotd")

    async def _maybe_track_qotd(self, message: discord.Message):
        await self._refresh_qotd_channels()
        channel_id = getattr(message.channel, "id", None)
        parent_id = getattr(message.channel, "parent_id", None)
        if channel_id in self._qotd_channels or parent_id in self._qotd_channels:
            if message.embeds or message.content:
                # Only the handful of most recent posts stay payable.
                self._qotd_anchors.add(message.id)
                self._prune_qotd_anchors()

    @staticmethod
    def _read_qotd_channels_blocking() -> set:
        conn = sqlite3.connect(f"file:{QOTD_DB}?mode=ro", uri=True)
        try:
            rows = conn.execute("SELECT post_channel_ids FROM guild_settings").fetchall()
        finally:
            conn.close()
        channels = set()
        for (raw,) in rows:
            with contextlib.suppress(TypeError, ValueError):
                channels.update(int(c) for c in json.loads(raw or "[]"))
        return channels

    async def _refresh_qotd_channels(self):
        """Read QOTD's configured post channels. Cached for 10 minutes.

        Off-thread: this uses the synchronous sqlite driver against another
        cog's database, and doing that inline would stall the event loop.
        """
        if time.time() - self._qotd_refresh_ts < 600:
            return
        self._qotd_refresh_ts = time.time()
        try:
            self._qotd_channels = await asyncio.to_thread(
                self._read_qotd_channels_blocking
            )
        except Exception as e:
            logger.debug(f"Could not read QOTD channels: {e}")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if not self._ready.is_set():
            return
        try:
            await self.effects.handle_member_update(before, after)
        except Exception as e:
            await self._report("effects.handle_member_update", e)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if not self._ready.is_set():
            return
        try:
            await self.effects.handle_member_join(member)
        except Exception as e:
            await self._report("effects.handle_member_join", e)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if self.items is None:
            return
        try:
            await self.items.on_reaction(self, payload)
        except Exception as e:
            await self._report("items.on_reaction", e)

    # --------------------------------------------------------------- loops

    @tasks.loop(minutes=1)
    async def voice_tick(self):
        """Credit a minute of voice time to everyone eligibly in a channel.

        Sweeping the channel list each minute means no join-time bookkeeping
        to lose across a restart.
        """
        try:
            per_min = self.config.get_int("lvl_voice_xp_per_min", 5)
            if per_min <= 0:
                return
            for guild in self.bot.guilds:
                for channel in guild.voice_channels:
                    if guild.afk_channel and channel.id == guild.afk_channel.id:
                        continue
                    humans = [m for m in channel.members if not m.bot]
                    # Sitting alone in a channel doesn't count as activity.
                    if len(humans) < 2:
                        continue
                    for member in humans:
                        state = member.voice
                        if not state or state.self_deaf or state.deaf:
                            continue
                        await self.leveling.grant_voice_xp(member.id, 60)
        except Exception as e:
            await self._report("voice_tick", e)

    @voice_tick.before_loop
    async def before_voice_tick(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=5)
    async def maintenance_tick(self):
        """Expire effects and let each item run its own upkeep."""
        try:
            await self.effects.expire_tick()
            if self.items is not None:
                await self.items.maintenance(self)
            self.leveling.prune_memory()
            self._prune_qotd_anchors()
        except Exception as e:
            await self._report("maintenance_tick", e)

    def _prune_qotd_anchors(self):
        if len(self._qotd_anchors) > 8:
            self._qotd_anchors = set(sorted(self._qotd_anchors)[-8:])

    @tasks.loop(hours=24)
    async def retention_tick(self):
        """Keep the ledger and daily tables bounded over years of uptime."""
        try:
            ledger = await self.db.prune_ledger(LEDGER_RETENTION_DAYS)
            daily = await self.db.prune_daily(DAILY_RETENTION_DAYS)
            if ledger or daily:
                logger.info(
                    f"Retention: pruned {ledger} ledger row(s), {daily} daily row(s)."
                )
        except Exception as e:
            await self._report("retention_tick", e)

    @retention_tick.before_loop
    async def before_retention_tick(self):
        await self.bot.wait_until_ready()

    @maintenance_tick.before_loop
    async def before_maintenance_tick(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------------ commands

    @app_commands.command(
        name="user_level",
        description="View a member's level, rank and activity profile.",
    )
    @app_commands.describe(member="Whose profile to show. Defaults to you.")
    @app_commands.guild_only()
    async def user_level(self, interaction: discord.Interaction,
                         member: discord.Member = None):
        await interaction.response.defer()
        target = member or interaction.user

        if not self.cards.ready:
            await interaction.followup.send(
                "Card rendering is unavailable right now.", ephemeral=True
            )
            return

        try:
            buffer = await self.build_level_card(target)
        except Exception as e:
            logger.error(f"user_level render failed: {e}", exc_info=True)
            with contextlib.suppress(Exception):
                await self.bot.error_reporter.report("Economy", f"user_level: {e}")
            await interaction.followup.send(
                "Something went wrong building that card.", ephemeral=True
            )
            return

        if buffer is None:
            await interaction.followup.send(
                "Could not render that card.", ephemeral=True
            )
            return

        file = discord.File(buffer, filename=f"level_{target.id}.png")
        view = LevelCardView(self, interaction.user.id)
        await interaction.followup.send(file=file, view=view)

    async def build_level_card(self, member: discord.Member):
        user_row = await self.db.get_user(member.id)
        rank, total_ranked = await self.leveling.get_rank(member.id)
        streak_row = await self.earning.get_streak(member.id)

        profile = {}
        if self.stats.available:
            profile = await self.stats.full_profile(member.id)

        channel_name = None
        if profile.get("top_channel_id"):
            channel = member.guild.get_channel(profile["top_channel_id"])
            if channel:
                channel_name = channel.name

        data = build_level_card_data(
            member, user_row, profile, rank, total_ranked, streak_row, channel_name
        )
        return await self.cards.render("eco_level_card", data)

    @app_commands.command(
        name="eco_leaderboard",
        description="Server level leaderboard.",
    )
    @app_commands.guild_only()
    async def eco_leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = LeaderboardView(self, interaction.guild, interaction.user.id)
        await view.jump_to_viewer()
        embed = await view.build_embed()
        view.update_buttons()
        await interaction.followup.send(embed=embed, view=view)

    @app_commands.command(
        name="eco_shop",
        description="Open the shop and spend your Points.",
    )
    @app_commands.guild_only()
    async def eco_shop(self, interaction: discord.Interaction):
        from .shop import ShopHomeView, build_home_embed
        await interaction.response.defer(ephemeral=True)
        view = ShopHomeView(self, interaction.user)
        embed = await build_home_embed(self, interaction.user)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @app_commands.command(
        name="economy_panel",
        description="Admin: configure the Economy and leveling systems.",
    )
    @app_commands.guild_only()
    async def economy_panel(self, interaction: discord.Interaction):
        from .panel import PanelView, build_panel_embed
        if not self.is_admin(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to use this.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        view = PanelView(self)
        embed = await build_panel_embed(self)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @app_commands.command(
        name="points",
        description="Check your Points balance and streak.",
    )
    @app_commands.describe(member="Whose balance to check. Defaults to you.")
    @app_commands.guild_only()
    async def points(self, interaction: discord.Interaction, member: discord.Member = None):
        await interaction.response.defer(ephemeral=True)
        target = member or interaction.user
        row = await self.db.get_user(target.id)
        streak = await self.earning.get_streak(target.id)

        embed = discord.Embed(
            title=f"{target.display_name} — Points",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Balance", value=f"**{row['points']:,}**")
        embed.add_field(name="Lifetime Earned", value=f"{row['lifetime_points']:,}")
        embed.add_field(
            name="Daily Streak",
            value=f"{(streak['current_streak'] if streak else 0):,} days",
        )
        embed.set_thumbnail(url=target.display_avatar.url)
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Economy(bot))
