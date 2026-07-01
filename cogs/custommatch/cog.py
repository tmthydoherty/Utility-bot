import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import logging
import os
import io
import json
import re
import time as _time
import types
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple
from pathlib import Path
import random
import itertools
from zoneinfo import ZoneInfo

from .models import (
    QueueType, CaptainSelection, Team,
    GameConfig, PlayerIGN, ReadyPenalty, Suspension, PlayerStats, QueueState, MatchState,
    K_FACTOR_PLACEMENT, K_FACTOR_LEARNING, K_FACTOR_STABLE,
    PLACEMENT_GAMES, LEARNING_GAMES, RIVALRY_MIN_GAMES,
    ROLE_MMR_TOLERANCE, SHAKE_MMR_TOLERANCE, SHAKE_LOOKBACK_MATCHES, SHAKE_OVERLAP_THRESHOLD,
    COLOR_WHITE, COLOR_RED, COLOR_BLUE, COLOR_NEUTRAL, COLOR_SUCCESS, COLOR_WARNING,
    RIVALS_ROLES, RIVALS_ROSTER, FONTS_PATH,
    is_valorant_game, is_rivals_game, _parse_tracker_url, _streak_bonus_multiplier,
    _role_diversity_penalty, generate_short_id, parse_duration_to_minutes,
    normalize_ign, find_best_ign_match, resolve_ocr_ign, normalize_rivals_role,
    safe_display_name, sanitize_for_codeblock,
    display_width, pad_to_width, truncate_to_width,
)
from .database import DatabaseHelper, DB_PATH, init_db, migrate_db
from .api_clients import HenrikDevAPI, MarvelRivalsAPI, RivalsVisionClient, RivalsScoreboardResult
from .stats_generator import StatsCardGenerator, PLAYWRIGHT_AVAILABLE
from .views_settings import (
    BaseMatchView, ConfirmView, GameSelectDropdown, SettingsView,
    RivalsSettingsView, RivalsIGNResolverView, RivalsCorrectStatsModal,
    _ConfirmLinkResolverView, _resolve_role_emojis,
)
from .views_gameplay import (
    AdminPanelView, QueueView, QueueMenuView, ReadyCheckView,
    WinVoteView, AbandonVoteView, IGNModal, IGNRequiredModal,
    PersistentIGNView, RoleSelectModal, RoleRequiredView,
    PersistentRoleDropdownView, PersistentRoleView, CaptainDraftView,
    VerificationTicketView, ServerStatsToggleView, PersistentLeaderboardView,
    MatchHistorySelectView, PlayerStatsView, StatsSelectDropdown, StatsImageView,
    SimpleStatsSelectDropdown, SimpleStatsImageView,
    FetchStatsModal, RefetchModeSelectView,
    DiscussionNotificationView, IGNSuggestionView,
    ShuffleMatchSelectView, ShuffleStartedCheckView, ShuffleVoteView,
)

EST = ZoneInfo("America/New_York")
logger = logging.getLogger('custommatch')


# =============================================================================
# MAIN COG
# =============================================================================

class CustomMatch(commands.Cog):
    """Custom match management system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.queues: Dict[int, QueueState] = {}  # queue_id -> QueueState
        self.ready_check_tasks: Dict[int, asyncio.Task] = {}  # queue_id -> task
        self.match_timeout_tasks: Dict[int, asyncio.Task] = {}  # match_id -> task
        self.arcade_end_votes: Dict[int, set] = {}  # match_id -> set of user_ids
        self.queue_timeout_task: Optional[asyncio.Task] = None
        self.penalty_decay_task: Optional[asyncio.Task] = None
        self.queue_schedule_task: Optional[asyncio.Task] = None
        self.orphan_cleanup_task: Optional[asyncio.Task] = None
        self.stats_retry_poll_task: Optional[asyncio.Task] = None
        self.monthly_reset_task: Optional[asyncio.Task] = None
        self.queue_embed_refresh_task: Optional[asyncio.Task] = None
        self.channel_cleanup_task: Optional[asyncio.Task] = None
        self.vacuum_task: Optional[asyncio.Task] = None
        self.pending_upload_cleanup_task: Optional[asyncio.Task] = None
        self.henrik_api = HenrikDevAPI(bot)
        self.rivals_api = MarvelRivalsAPI()  # marvelrivalsapi.com player lookup
        self.rivals_vision = RivalsVisionClient()  # Gemini 2.5 Flash scoreboard OCR
        # channel_id -> {match_id, game_id, guild_id, expires_at}
        self.rivals_pending_uploads: Dict[int, dict] = {}
        self.rivals_reminder_tasks: Dict[int, asyncio.Task] = {}  # keyed by match_id
        self.stats_generator = StatsCardGenerator()
        self.ign_update_cache: Dict[int, datetime] = {}  # player_id -> last checked timestamp
        self.ready_check_lock: asyncio.Lock = asyncio.Lock()  # Prevents race in ready check
        self.queue_locks: Dict[int, asyncio.Lock] = {}  # Per-queue locks to prevent join/leave races
        self.match_finalize_locks: Dict[int, asyncio.Lock] = {}  # match_id -> lock to prevent double finalization
        self._last_retry_cleanup_at: float = 0.0  # monotonic timestamp of last old-retry purge
        self.not_ready_cooldowns: Dict[int, datetime] = {}  # player_id -> cooldown expiry (UTC)
        self.lf1_messages: Dict[int, discord.Message] = {}  # game_id -> LF1 message (for deletion)
        self.lf1_tasks: Dict[int, asyncio.Task] = {}  # game_id -> auto-delete task
        self.lf1_cooldowns: Dict[int, datetime] = {}  # game_id -> last LF1 send time
        self.secondary_alert_sent: set = set()  # legacy, kept for attribute safety

    async def cog_load(self):
        await init_db()
        await DatabaseHelper.connect()
        # Reconcile player stats from match data (fixes out-of-sync wins/losses)
        try:
            updated = await DatabaseHelper.reconcile_player_stats()
            if updated:
                logger.info(f"Reconciled {updated} player stats entries on startup")
        except Exception as e:
            logger.error(f"Error reconciling player stats: {e}")
        # Restore active queues from database
        await self.restore_queues()
        # Restore persistent leaderboard views
        await self.restore_leaderboard_views()
        # Register persistent IGN and Role views
        self.bot.add_view(PersistentIGNView(self))
        self.bot.add_view(PersistentRoleView(self))
        # Register persistent server-stats views for every eligible configured game,
        # so toggle/matches buttons survive bot restarts.
        try:
            all_games = await DatabaseHelper.get_all_games()
            for g in (all_games or []):
                if is_valorant_game(g) or is_rivals_game(g):
                    self.bot.add_view(ServerStatsToggleView(self, g.game_id, is_monthly=True))
                    self.bot.add_view(ServerStatsToggleView(self, g.game_id, is_monthly=False))
        except Exception as e:
            logger.error(f"Failed to register persistent ServerStatsToggleView: {e}")
        # Restore match timeout tasks for active matches
        await self.restore_match_timeout_tasks()
        # Start background tasks
        self.queue_timeout_task = asyncio.create_task(self.queue_timeout_check())
        self.penalty_decay_task = asyncio.create_task(self.penalty_decay_check())
        self.queue_schedule_task = asyncio.create_task(self.queue_schedule_check())
        self.orphan_cleanup_task = asyncio.create_task(self.orphan_match_cleanup())
        # Start persistent stats retry poll (replaces in-memory retry tasks)
        self.stats_retry_poll_task = asyncio.create_task(self.stats_retry_poll())
        # Start monthly leaderboard reset check
        self.monthly_reset_task = asyncio.create_task(self.monthly_leaderboard_check())
        # Start queue embed periodic refresh
        self.queue_embed_refresh_task = asyncio.create_task(self.queue_embed_refresh())
        # Start match channel auto-cleanup (12h after match ends)
        self.channel_cleanup_task = asyncio.create_task(self.match_channel_cleanup())
        # Start weekly database VACUUM
        self.vacuum_task = asyncio.create_task(self.weekly_vacuum())
        # Start periodic cleanup of expired pending uploads
        self.pending_upload_cleanup_task = asyncio.create_task(self._pending_upload_cleanup())
        # Initialize stats card generator
        await self.stats_generator.initialize()
        logger.info("CustomMatch cog loaded, database initialized.")

    async def restore_queues(self):
        """Restore active queues from database after restart."""
        try:
            async with DatabaseHelper._get_db() as db:
                # First, clean up orphaned queue_players (where queue doesn't exist)
                await db.execute("""
                    DELETE FROM queue_players
                    WHERE queue_id NOT IN (SELECT queue_id FROM active_queues)
                """)

                # Clean up stale ready_check queues (older than 1 hour)
                await db.execute("""
                    DELETE FROM queue_players WHERE queue_id IN (
                        SELECT queue_id FROM active_queues
                        WHERE state = 'ready_check'
                        AND datetime(ready_check_started) < datetime('now', '-1 hour')
                    )
                """)
                await db.execute("""
                    DELETE FROM active_queues
                    WHERE state = 'ready_check'
                    AND datetime(ready_check_started) < datetime('now', '-1 hour')
                """)

                # Reset any remaining ready_check or starting_match queues to waiting state
                # We can't properly resume the timeout task after restart, so reset them
                await db.execute("""
                    UPDATE active_queues
                    SET state = 'waiting', ready_check_started = NULL
                    WHERE state IN ('ready_check', 'starting_match')
                """)
                # Also reset players' ready status in those queues
                await db.execute("""
                    UPDATE queue_players SET is_ready = 0
                    WHERE queue_id IN (SELECT queue_id FROM active_queues WHERE state = 'waiting')
                """)
                logger.info("Reset any ready_check queues to waiting state after restart")

                await db.commit()

                async with db.execute("SELECT * FROM active_queues") as cursor:
                    rows = await cursor.fetchall()

                for row in rows:
                    queue_id = row["queue_id"]
                    game_id = row["game_id"]
                    channel_id = row["channel_id"]
                    message_id = row["message_id"]
                    state = row["state"]
                    short_id = row["short_id"] if "short_id" in row.keys() else None
                    is_secondary = bool(row["is_secondary"]) if "is_secondary" in row.keys() else False

                    # Get players in this queue
                    async with db.execute(
                        "SELECT player_id FROM queue_players WHERE queue_id = ?",
                        (queue_id,)
                    ) as pcursor:
                        player_rows = await pcursor.fetchall()
                        players = {r[0]: False for r in player_rows}  # All not ready initially

                    # Drop any players who are no longer in the guild — they can't
                    # ready up and would permanently stall the queue after a restart.
                    guild = self.bot.guilds[0] if self.bot.guilds else None
                    if guild and players:
                        stale = [pid for pid in list(players) if guild.get_member(pid) is None]
                        if stale:
                            logger.warning(
                                f"restore_queues: removing {len(stale)} stale player(s) "
                                f"from queue {queue_id} (no longer in guild): {stale}"
                            )
                            for pid in stale:
                                del players[pid]
                            # Clean up stale entries from DB
                            for pid in stale:
                                await db.execute(
                                    "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                                    (queue_id, pid)
                                )
                            await db.commit()

                    # Create queue state
                    queue_state = QueueState(
                        queue_id=queue_id,
                        game_id=game_id,
                        channel_id=channel_id,
                        message_id=message_id,
                        players=players,
                        state=state,
                        short_id=short_id,
                        is_secondary=is_secondary,
                    )
                    self.queues[queue_id] = queue_state

                    # Re-register the view with the bot based on state
                    game = await DatabaseHelper.get_game(game_id)
                    if game and message_id:
                        # Verify the message still exists in Discord
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            try:
                                await channel.fetch_message(message_id)
                            except discord.NotFound:
                                # Message deleted — recreate the embed
                                logger.warning(f"restore_queues: message {message_id} not found for queue {queue_id}, recreating")
                                try:
                                    embed = await self.create_queue_embed(game, queue_state, channel.guild)
                                    view = QueueView(self, game_id, queue_id)
                                    new_msg = await channel.send(embed=embed, view=view)
                                    queue_state.message_id = new_msg.id
                                    message_id = new_msg.id
                                    await db.execute(
                                        "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                                        (new_msg.id, queue_id)
                                    )
                                    await db.commit()
                                except Exception as e2:
                                    logger.error(f"restore_queues: failed to recreate embed for queue {queue_id}: {e2}")
                            except Exception:
                                pass  # Other errors (permissions, etc.) — try to register view anyway

                        # state was already reset to 'waiting' above for ready_check/starting_match
                        view = QueueView(self, game_id, queue_id)
                        self.bot.add_view(view, message_id=message_id)

            logger.info(f"Restored {len(self.queues)} active queues from database.")
        except Exception as e:
            logger.error(f"Error restoring queues: {e}")

    async def restore_leaderboard_views(self):
        """Re-register persistent leaderboard views after restart."""
        try:
            games = await DatabaseHelper.get_all_games()
            count = 0
            for game in games:
                if game.leaderboard_channel_id and game.leaderboard_message_id:
                    is_valorant = 'valorant' in game.name.lower()
                    view = PersistentLeaderboardView(self, game.game_id, is_valorant=is_valorant)
                    self.bot.add_view(view, message_id=game.leaderboard_message_id)
                    count += 1
            logger.info(f"Restored {count} persistent leaderboard views.")
        except Exception as e:
            logger.error(f"Error restoring leaderboard views: {e}")

    async def restore_match_timeout_tasks(self):
        """Restore match timeout tasks for active matches after restart."""
        try:
            async with DatabaseHelper._get_db() as db:
                async with db.execute(
                    "SELECT match_id, channel_id, created_at FROM matches "
                    "WHERE winning_team IS NULL AND cancelled = 0 AND channel_id IS NOT NULL"
                ) as cursor:
                    rows = await cursor.fetchall()

            count = 0
            max_timeout = 3 * 60 * 60
            for row in rows:
                match_id = row["match_id"]
                channel_id = row["channel_id"]
                created_at_str = row["created_at"]

                if not created_at_str or not channel_id:
                    continue

                try:
                    created_at = datetime.fromisoformat(created_at_str)
                except (ValueError, TypeError):
                    continue

                # Calculate remaining timeout
                now = datetime.now(timezone.utc)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                elapsed = (now - created_at).total_seconds()
                remaining = max(0, max_timeout - elapsed)

                if remaining <= 0:
                    remaining = 60  # Fire soon for already-expired matches

                # Find the channel in any guild
                channel = None
                for guild in self.bot.guilds:
                    channel = guild.get_channel(channel_id)
                    if channel:
                        break

                if not channel:
                    continue

                task = asyncio.create_task(
                    self.match_timeout(channel.guild, match_id, channel, delay_seconds=int(remaining))
                )
                self.match_timeout_tasks[match_id] = task
                count += 1

            logger.info(f"Restored {count} match timeout tasks.")
        except Exception as e:
            logger.error(f"Error restoring match timeout tasks: {e}")

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle orphaned queue interactions that weren't caught by registered views."""
        if interaction.type != discord.InteractionType.component:
            return

        # Skip if already handled by a registered view
        if interaction.response.is_done():
            return

        # Give registered views a moment to claim the interaction first.
        # Without this, on_interaction races with the view callback and both
        # try to respond, causing "Interaction has already been acknowledged".
        await asyncio.sleep(0.25)
        if interaction.response.is_done():
            return

        custom_id = interaction.data.get("custom_id", "")

        # Handle queue join/leave/menu buttons
        if custom_id.startswith("cm_queue_join:") or custom_id.startswith("cm_queue_leave:") or custom_id.startswith("cm_queue_menu:"):
            try:
                queue_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            # Check if queue exists in memory
            if queue_id not in self.queues:
                await interaction.response.send_message(
                    "This queue is no longer active. Please use a new queue.",
                    ephemeral=True
                )
                return

            queue_state = self.queues[queue_id]
            game = await DatabaseHelper.get_game(queue_state.game_id)
            if not game:
                return

            # Route to appropriate handler
            if custom_id.startswith("cm_queue_join:"):
                await self.handle_queue_join(interaction, game.game_id, queue_id)
            elif custom_id.startswith("cm_queue_leave:"):
                await self.handle_queue_leave(interaction, game.game_id, queue_id)
            else:
                # Menu button — open ephemeral menu
                current_sub = await DatabaseHelper.get_player_subscription(queue_id, interaction.user.id)
                player_count = len(queue_state.players)
                view = QueueMenuView(self, game.game_id, queue_id, current_sub, player_count)
                if current_sub is not None:
                    msg = f"**Queue Menu**\nYou have a DM request active for when **{current_sub}** more needed."
                else:
                    msg = "**Queue Menu**\nRequest a DM when the queue is almost full."
                await interaction.response.send_message(msg, view=view, ephemeral=True)

        # Handle ready check buttons
        elif custom_id.startswith("cm_ready_yes:") or custom_id.startswith("cm_ready_no:"):
            try:
                queue_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            if queue_id not in self.queues:
                await interaction.response.send_message(
                    "This ready check has expired.",
                    ephemeral=True
                )
                return

            is_ready = custom_id.startswith("cm_ready_yes:")
            await self.handle_ready(interaction, queue_id, is_ready)

        # Handle win vote buttons (persistent across restart)
        elif custom_id.startswith("cm_vote_red:") or custom_id.startswith("cm_vote_blue:"):
            try:
                match_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            team = Team.RED if custom_id.startswith("cm_vote_red:") else Team.BLUE
            await self.handle_win_vote(interaction, match_id, team)

        # Handle abandon vote button (persistent across restart)
        elif custom_id.startswith("cm_abandon_vote:"):
            try:
                match_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            # Get match to determine needed votes
            match = await DatabaseHelper.get_match(match_id)
            if match:
                players = await DatabaseHelper.get_match_players(match_id)
                total_players = len(players)
                needed_votes = max(2, (total_players // 2) + 1)  # Majority vote
                await self.handle_abandon_vote(interaction, match_id, needed_votes)
            else:
                await interaction.response.send_message("Match not found.", ephemeral=True)

        # Handle shuffle vote button (persistent across restart)
        elif custom_id.startswith("cm_shuffle_vote:"):
            try:
                match_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            match = await DatabaseHelper.get_match(match_id)
            if match:
                players = await DatabaseHelper.get_match_players(match_id)
                needed_votes = max(2, (len(players) // 2) + 1)
                await self.handle_shuffle_vote(interaction, match_id, needed_votes)
            else:
                await interaction.response.send_message("Match not found.", ephemeral=True)

        # Handle DM request select/cancel (fallback when QueueMenuView times out)
        elif custom_id.startswith("cm_queue_sub_select:") or custom_id.startswith("cm_queue_sub_cancel:"):
            try:
                queue_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            if queue_id not in self.queues:
                await interaction.response.send_message(
                    "This queue is no longer active.",
                    ephemeral=True
                )
                return

            queue_state = self.queues[queue_id]

            if custom_id.startswith("cm_queue_sub_select:"):
                threshold = int(interaction.data["values"][0])
                player_count = len(queue_state.players)
                if player_count < 4:
                    await interaction.response.edit_message(
                        content="The lobby needs at least **4 players** before you can request a DM.",
                        view=None
                    )
                    return
                await DatabaseHelper.subscribe_to_queue(queue_id, interaction.user.id, threshold)
                await interaction.response.edit_message(
                    content=f"You'll be DM'd when **{threshold}** more player{'s are' if threshold > 1 else ' is'} needed. This request expires in **60 minutes**.",
                    view=None
                )
            else:
                await DatabaseHelper.unsubscribe_from_queue(queue_id, interaction.user.id)
                await interaction.response.edit_message(
                    content="Your DM request has been cancelled.",
                    view=None
                )

        # Handle persistent leaderboard buttons (fallback if view wasn't registered)
        elif custom_id.startswith("cm_lb_alltime:") or custom_id.startswith("cm_lb_matches:"):
            try:
                game_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            game = await DatabaseHelper.get_game(game_id)
            if not game:
                await interaction.response.send_message("Game not found.", ephemeral=True)
                return

            if custom_id.startswith("cm_lb_alltime:"):
                embed = await self._build_leaderboard_text_embed(interaction.guild, game_id, monthly=False)
                await interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                # Matches button
                await interaction.response.defer(ephemeral=True)
                recent = await DatabaseHelper.get_recent_completed_matches(
                    game_id, limit=5, require_rivals_stats=is_rivals_game(game)
                )
                if not recent:
                    await interaction.followup.send("No completed matches found.", ephemeral=True)
                    return
                latest = recent[0]
                embed, file = await self._generate_match_scoreboard(interaction.guild, latest["match_id"])
                view = MatchHistorySelectView(self, recent) if len(recent) > 1 else None
                kwargs = {"embed": embed, "ephemeral": True}
                if file:
                    kwargs["file"] = file
                if view:
                    kwargs["view"] = view
                await interaction.followup.send(**kwargs)

        # Handle discussion thread buttons (persistent across restart)
        elif custom_id.startswith("cm_discussion_join:") or custom_id.startswith("cm_discussion_close:"):
            if not await self.is_cm_admin(interaction.user):
                await interaction.response.send_message("You need the CM Admin role.", ephemeral=True)
                return

            try:
                thread_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            view = DiscussionNotificationView(thread_id)
            if custom_id.startswith("cm_discussion_join:"):
                await view.join_callback(interaction)
            else:
                await view.close_callback(interaction)

    async def cog_unload(self):
        # Cancel all tasks
        for task in self.ready_check_tasks.values():
            task.cancel()
        for task in self.match_timeout_tasks.values():
            task.cancel()
        if self.queue_timeout_task:
            self.queue_timeout_task.cancel()
        if self.penalty_decay_task:
            self.penalty_decay_task.cancel()
        if self.queue_schedule_task:
            self.queue_schedule_task.cancel()
        if self.orphan_cleanup_task:
            self.orphan_cleanup_task.cancel()
        if self.stats_retry_poll_task:
            self.stats_retry_poll_task.cancel()
        if self.monthly_reset_task:
            self.monthly_reset_task.cancel()
        if self.queue_embed_refresh_task:
            self.queue_embed_refresh_task.cancel()
        if self.channel_cleanup_task:
            self.channel_cleanup_task.cancel()
        if self.vacuum_task:
            self.vacuum_task.cancel()
        if self.pending_upload_cleanup_task:
            self.pending_upload_cleanup_task.cancel()
        for task in self.rivals_reminder_tasks.values():
            task.cancel()
        # Close API session
        await self.henrik_api.close()
        await self.rivals_api.close()
        # Close stats generator
        await self.stats_generator.close()
        # Close persistent DB connection
        await DatabaseHelper.close()

    # -------------------------------------------------------------------------
    # BACKGROUND TASKS
    # -------------------------------------------------------------------------

    async def queue_timeout_check(self):
        """Background task to remove players who have been in queue too long."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                games = await DatabaseHelper.get_all_games()
                games_with_timeout = {g.game_id: g for g in games if g.queue_timeout_minutes > 0}

                for queue_id, queue_state in list(self.queues.items()):
                    if queue_state.state != "waiting":
                        continue

                    game = games_with_timeout.get(queue_state.game_id)
                    if not game:
                        continue

                    # Get join times
                    join_times = await DatabaseHelper.get_queue_join_times(queue_id)
                    now = datetime.now(timezone.utc)
                    timeout_delta = timedelta(minutes=game.queue_timeout_minutes)

                    removed = []
                    for pid, joined_at in join_times.items():
                        if queue_state.state != "waiting":
                            break
                        if pid in queue_state.players and (now - joined_at) > timeout_delta:
                            # Acquire queue lock to prevent racing with join/leave handlers
                            async with self.queue_locks.setdefault(queue_id, asyncio.Lock()):
                                # Re-check inside lock — state or membership may have changed
                                if queue_state.state != "waiting" or pid not in queue_state.players:
                                    continue
                                del queue_state.players[pid]
                                queue_state.grace_timers.pop(pid, None)
                            await DatabaseHelper.remove_player_from_queue(queue_id, pid)
                            removed.append(pid)

                    if removed:
                        # If queue dropped below LF1 threshold, clean up the alert
                        epc = self._get_effective_player_count(game, queue_state)
                        if len(queue_state.players) < epc - 1:
                            await self._delete_lf1_message(game.game_id)

                        # Update embed
                        channel = self.bot.get_channel(queue_state.channel_id)
                        if channel and queue_state.message_id:
                            try:
                                msg = await channel.fetch_message(queue_state.message_id)
                                embed = await self.create_queue_embed(game, queue_state, channel.guild)
                                view = QueueView(self, game.game_id, queue_id)
                                await msg.edit(embed=embed, view=view)
                                # DM removed players with a link to rejoin
                                queue_url = msg.jump_url
                                for pid in removed:
                                    try:
                                        user = self.bot.get_user(pid) or await self.bot.fetch_user(pid)
                                        if user:
                                            await user.send(
                                                f"You were removed from the **{game.name}** queue after being in it for "
                                                f"{game.queue_timeout_minutes // 60}h. Click below to rejoin if you'd like!\n"
                                                f"{queue_url}"
                                            )
                                    except Exception:
                                        pass
                            except Exception:
                                pass

            except Exception as e:
                logger.error(f"Queue timeout check error: {e}")

            await asyncio.sleep(60)  # Check every minute

    async def queue_embed_refresh(self):
        """Background task to periodically re-edit active queue embeds (every 30s).

        Prevents stale embeds when msg.edit() silently fails on user action.
        Skips edits when the queue fingerprint (player count + state) hasn't changed.
        """
        fingerprints: Dict[int, tuple] = {}  # queue_id -> (player_count, state)
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                game_cache: Dict[int, GameConfig] = {}
                for queue_id, qs in list(self.queues.items()):
                    if qs.state != "waiting" or not qs.message_id or not qs.channel_id:
                        fingerprints.pop(queue_id, None)
                        continue
                    try:
                        fp = (len(qs.players), qs.state)
                        if fingerprints.get(queue_id) == fp:
                            continue
                        if qs.game_id not in game_cache:
                            game = await DatabaseHelper.get_game(qs.game_id)
                            if not game:
                                continue
                            game_cache[qs.game_id] = game
                        game = game_cache[qs.game_id]

                        channel = self.bot.get_channel(qs.channel_id)
                        if not channel:
                            continue
                        msg = await channel.fetch_message(qs.message_id)
                        embed = await self.create_queue_embed(game, qs, channel.guild)
                        view = QueueView(self, game.game_id, queue_id)
                        await msg.edit(embed=embed, view=view)
                        fingerprints[queue_id] = fp
                    except Exception as e:
                        logger.debug(f"queue_embed_refresh: queue {queue_id} skipped: {e}")
            except Exception as e:
                logger.error(f"queue_embed_refresh error: {e}")

            await asyncio.sleep(30)

    async def penalty_decay_check(self):
        """Background task to decay old penalty offenses."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                # This runs daily - actual decay logic is in add_ready_penalty_offense
                # based on penalty_decay_days setting
                # We just clean up expired penalties from the database here
                async with DatabaseHelper._get_db() as db:
                    now = datetime.now(timezone.utc).isoformat()
                    # Clear expired penalties (set to no active penalty)
                    await db.execute(
                        "UPDATE ready_penalties SET penalty_expires = NULL WHERE penalty_expires < ?",
                        (now,)
                    )
                    await db.commit()

            except Exception as e:
                logger.error(f"Penalty decay check error: {e}")

            await asyncio.sleep(3600 * 24)  # Check daily

    async def queue_schedule_check(self):
        """Background task to manage queue open/close schedules."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                games = await DatabaseHelper.get_all_games()
                scheduled_games = [g for g in games if g.schedule_enabled and g.queue_channel_id]

                for game in scheduled_games:
                    desired = self._get_desired_state(game)
                    if desired is None:
                        continue  # Unconfigured day, don't touch

                    channel = self.bot.get_channel(game.queue_channel_id)
                    if not channel:
                        continue
                    guild = channel.guild

                    queue_exists = any(qs.game_id == game.game_id and qs.state in ("waiting", "ready_check", "starting_match", "in_match") for qs in self.queues.values())
                    has_down_message = game.schedule_down_message_id is not None

                    if desired == "open" and not queue_exists:
                        # Should be open but no queue — open it
                        if has_down_message:
                            try:
                                old_msg = await channel.fetch_message(game.schedule_down_message_id)
                                await old_msg.delete()
                            except discord.NotFound:
                                pass
                            await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=None)

                        # Count queues before to detect if start_queue truly created a new one
                        queues_before = sum(1 for qs in self.queues.values() if qs.game_id == game.game_id)
                        await self.start_queue(channel, game)
                        queues_after = sum(1 for qs in self.queues.values() if qs.game_id == game.game_id)
                        if queues_after > queues_before:
                            await self.log_action(guild, f"Queue opened for **{game.name}** (scheduled)")

                    elif desired == "closed":
                        # Should be closed — always verify/create the down embed
                        await self._close_queue_for_schedule(game, guild, channel)


            except Exception as e:
                logger.error(f"Queue schedule check error: {e}")

            await asyncio.sleep(60)  # Check every minute

    def _get_desired_state(self, game: GameConfig) -> Optional[str]:
        """Return 'open', 'closed', or None (unconfigured day) based on schedule."""
        now = datetime.now()
        current_day = now.weekday()
        current_time = now.strftime("%H:%M")

        if game.schedule_times and str(current_day) in game.schedule_times:
            config = game.schedule_times[str(current_day)]
            mode = config.get("mode")
            if mode == "open":
                return "open"
            if mode == "closed":
                return "closed"
            # Timed schedule
            open_time = config.get("open")
            close_time = config.get("close")
            if open_time and close_time:
                if close_time < open_time:
                    return "open" if (current_time >= open_time or current_time < close_time) else "closed"
                else:
                    return "open" if (open_time <= current_time < close_time) else "closed"
            elif open_time and not close_time:
                # Open-only: opens at this time, never closes today
                return "open" if current_time >= open_time else self._get_previous_end_state(game, current_day)
            elif close_time and not open_time:
                # Close-only: was open from previous day, closes at this time
                return "open" if current_time < close_time else "closed"

        # Unconfigured day with schedule_times — inherit from previous day's end state
        if game.schedule_times:
            return self._get_previous_end_state(game, current_day)

        # Legacy fallback
        if game.schedule_open_days and game.schedule_open_time and game.schedule_close_time:
            open_days = [int(d) for d in game.schedule_open_days.split(",") if d.isdigit()]
            if current_day in open_days:
                open_time = game.schedule_open_time
                close_time = game.schedule_close_time
                if close_time < open_time:
                    return "open" if (current_time >= open_time or current_time < close_time) else "closed"
                else:
                    return "open" if (open_time <= current_time < close_time) else "closed"

        return None

    def _get_previous_end_state(self, game: GameConfig, from_day: int) -> Optional[str]:
        """Walk backwards through configured days to determine rollover state."""
        for offset in range(1, 8):
            prev_day = (from_day - offset) % 7
            config = game.schedule_times.get(str(prev_day))
            if not config:
                continue
            mode = config.get("mode")
            if mode == "open":
                return "open"
            if mode == "closed":
                return "closed"
            open_t = config.get("open")
            close_t = config.get("close")
            if open_t and close_t:
                # If close < open (midnight rollover), day ends in "open" state
                return "open" if close_t < open_t else "closed"
            elif open_t and not close_t:
                return "open"   # opened, never closed
            elif close_t and not open_t:
                return "closed"  # closed, never reopened
        return None

    def _is_currently_open(self, game: GameConfig) -> bool:
        """Check if queue should currently be open based on schedule."""
        return self._get_desired_state(game) == "open"

    async def apply_schedule_state(self, game: GameConfig):
        """Apply the current schedule state immediately (called when schedule is toggled on)."""
        try:
            logger.debug(f"apply_schedule_state called for {game.name}")
            if not game.queue_channel_id:
                logger.debug(f"apply_schedule_state: No queue channel for {game.name}")
                return

            channel = self.bot.get_channel(game.queue_channel_id)
            if not channel:
                logger.debug(f"apply_schedule_state: Channel {game.queue_channel_id} not found")
                return

            guild = channel.guild
            logger.debug(f"apply_schedule_state: Got channel {channel.name} in guild {guild.name}")

            queue_exists = any(qs.game_id == game.game_id and qs.state in ("waiting", "ready_check", "starting_match") for qs in self.queues.values())
            desired = self._get_desired_state(game)
            logger.debug(f"apply_schedule_state: desired={desired}, queue_exists={queue_exists}")

            if desired == "open":
                # Should be open
                logger.debug(f"apply_schedule_state: Queue SHOULD be open")
                if not queue_exists:
                    # Delete down message if exists
                    if game.schedule_down_message_id:
                        try:
                            old_msg = await channel.fetch_message(game.schedule_down_message_id)
                            await old_msg.delete()
                        except discord.NotFound:
                            pass
                        await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=None)

                    # Start queue
                    await self.start_queue(channel, game)
                    await self.log_action(guild, f"Queue opened for **{game.name}** (schedule enabled)")
            elif desired == "closed":
                # Should be closed
                logger.debug(f"apply_schedule_state: Queue SHOULD be closed, calling _close_queue_for_schedule")
                await self._close_queue_for_schedule(game, guild, channel)
            else:
                # Unconfigured day — close queue as safe default when schedule is first enabled
                logger.debug(f"apply_schedule_state: Unconfigured day, closing as default")
                await self._close_queue_for_schedule(game, guild, channel)
        except Exception as e:
            logger.error(f"apply_schedule_state ERROR: {e}")
            import traceback
            traceback.print_exc()

    async def _close_queue_for_schedule(self, game: GameConfig, guild: discord.Guild, channel: discord.TextChannel):
        """Close queue and show countdown embed."""
        has_down_message = game.schedule_down_message_id is not None
        logger.debug(f"_close_queue_for_schedule: {game.name} - has_down_message={has_down_message}")

        # If down message already exists, verify it's still there and bail out early.
        # This prevents the message scan below from deleting the closed embed and triggering a resend.
        if has_down_message:
            try:
                await channel.fetch_message(game.schedule_down_message_id)
                logger.debug(f"_close_queue_for_schedule: Down message {game.schedule_down_message_id} still exists, nothing to do")
                return
            except discord.NotFound:
                logger.debug(f"_close_queue_for_schedule: Down message {game.schedule_down_message_id} not found, will create new one")
                await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=None)

        # Close any existing queue from memory
        logger.debug(f"_close_queue_for_schedule: Checking {len(self.queues)} queues in memory")
        for qid, qs in list(self.queues.items()):
            if qs.game_id == game.game_id:
                # Skip queues in ready_check/starting_match/in_match — let them
                # finish naturally rather than yanking state out from under them.
                if qs.state not in ("waiting",):
                    logger.info(
                        f"_close_queue_for_schedule: skipping queue {qid} in state "
                        f"{qs.state} — will close after it resolves"
                    )
                    continue
                logger.debug(f"_close_queue_for_schedule: Found queue {qid} in memory, deleting msg {qs.message_id}")
                if qs.message_id:
                    try:
                        old_msg = await channel.fetch_message(qs.message_id)
                        await old_msg.delete()
                        logger.debug(f"_close_queue_for_schedule: Deleted queue message {qs.message_id}")
                    except discord.NotFound:
                        logger.debug(f"_close_queue_for_schedule: Queue message {qs.message_id} not found")
                # Remove from memory
                if qid in self.queues:
                    del self.queues[qid]
                    self.queue_locks.pop(qid, None)
                # Remove from database
                async with DatabaseHelper._get_db() as db:
                    await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (qid,))
                    await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (qid,))
                    await db.commit()
                logger.debug(f"_close_queue_for_schedule: Queue {qid} removed from memory and DB")

        # Fallback: Check database for orphan queues not in memory
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT queue_id, message_id FROM active_queues WHERE game_id = ?",
                (game.game_id,)
            ) as cursor:
                orphan_queues = await cursor.fetchall()
            logger.debug(f"_close_queue_for_schedule: Found {len(orphan_queues)} orphan queues in DB")

            for row in orphan_queues:
                qid = row["queue_id"]
                msg_id = row["message_id"]
                logger.debug(f"_close_queue_for_schedule: Orphan queue {qid}, msg {msg_id}")
                if msg_id:
                    try:
                        old_msg = await channel.fetch_message(msg_id)
                        await old_msg.delete()
                        logger.debug(f"_close_queue_for_schedule: Deleted orphan message {msg_id}")
                    except discord.NotFound:
                        logger.debug(f"_close_queue_for_schedule: Orphan message {msg_id} not found")
                await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (qid,))
                await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (qid,))
            await db.commit()

        # Fallback: Scan recent messages for any queue embeds from this bot
        logger.debug(f"_close_queue_for_schedule: Scanning recent messages for queue embeds")
        try:
            async for msg in channel.history(limit=20):
                if msg.author.id == self.bot.user.id and msg.embeds:
                    embed = msg.embeds[0]
                    # Check if it's a queue embed for this game (has "Queue" in title and game name)
                    if embed.title and game.name in embed.title and "Queue" in embed.title and "Closed" not in embed.title:
                        await msg.delete()
                        logger.debug(f"_close_queue_for_schedule: Deleted stale queue embed {msg.id}")
        except Exception as e:
            logger.debug(f"_close_queue_for_schedule: Error scanning messages: {e}")

        now = datetime.now()
        next_open_ts = self._calculate_next_open_time(game, now)

        # Build the closed embed with only the next open time
        embed = discord.Embed(
            title=f"{game.name} Queue",
            color=COLOR_NEUTRAL
        )
        embed.add_field(
            name="Queue Opens",
            value=f"<t:{next_open_ts}:F>\n(<t:{next_open_ts}:R>)",
            inline=False
        )
        if game.banner_url:
            embed.set_image(url=game.banner_url)

        # Send new down message
        logger.debug(f"_close_queue_for_schedule: Sending closed embed")
        down_msg = await channel.send(embed=embed)
        await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=down_msg.id)
        logger.info(f"_close_queue_for_schedule: Sent closed embed for {game.name}, msg_id={down_msg.id}")
        await self.log_action(guild, f"Queue closed for **{game.name}** (scheduled)")

    def _calculate_next_open_time(self, game: GameConfig, now: datetime) -> int:
        """Calculate the Unix timestamp of the next queue open time."""
        if game.schedule_times:
            # Check today first — if today has an open time we haven't reached yet
            day_num = str(now.weekday())
            if day_num in game.schedule_times:
                config = game.schedule_times[day_num]
                mode = config.get("mode")
                if mode != "closed" and mode != "open":
                    open_time = config.get("open", "00:00")
                    if now.strftime("%H:%M") < open_time:
                        open_parts = open_time.split(":")
                        today_open = now.replace(hour=int(open_parts[0]), minute=int(open_parts[1]), second=0, microsecond=0)
                        return int(today_open.timestamp())

            # Check future days
            for days_ahead in range(1, 8):
                check_date = now + timedelta(days=days_ahead)
                future_day = str(check_date.weekday())
                if future_day in game.schedule_times:
                    config = game.schedule_times[future_day]
                    mode = config.get("mode")
                    if mode == "closed":
                        continue  # Skip closed-all-day days
                    if mode == "open":
                        # Open all day — next open is midnight of that day
                        check_date = check_date.replace(hour=0, minute=0, second=0, microsecond=0)
                        return int(check_date.timestamp())
                    # Timed schedule
                    open_time = config.get("open", "00:00")
                    open_parts = open_time.split(":")
                    check_date = check_date.replace(hour=int(open_parts[0]), minute=int(open_parts[1]), second=0, microsecond=0)
                    return int(check_date.timestamp())

        # Fall back to legacy format
        if game.schedule_open_days and game.schedule_open_time:
            open_days = [int(d) for d in game.schedule_open_days.split(",") if d.isdigit()]
            open_parts = game.schedule_open_time.split(":")
            open_hour = int(open_parts[0])
            open_minute = int(open_parts[1])

            check_date = now + timedelta(days=1)
            check_date = check_date.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)

            for _ in range(7):
                if check_date.weekday() in open_days:
                    return int(check_date.timestamp())
                check_date += timedelta(days=1)

        # Fallback to a week from now
        return int((now + timedelta(days=7)).timestamp())

    # ---- Secondary queue schedule helpers ----


    async def weekly_vacuum(self):
        """Background task to VACUUM the SQLite database weekly for defragmentation."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(3600)  # Wait 1 hour after startup before first VACUUM
        while not self.bot.is_closed():
            try:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("VACUUM")
                logger.info("Weekly VACUUM completed successfully")
            except Exception as e:
                logger.error(f"Weekly VACUUM failed: {e}")
            await asyncio.sleep(604800)  # 7 days in seconds

    async def orphan_match_cleanup(self):
        """Background task to clean up orphaned matches - matches that are stuck without channels/roles."""
        await self.bot.wait_until_ready()
        # Run cleanup on startup
        await asyncio.sleep(30)  # Wait for bot to fully initialize
        await self._do_orphan_cleanup()

        while not self.bot.is_closed():
            try:
                await asyncio.sleep(3600)  # Check every hour
                await self._do_orphan_cleanup()
                await self._do_orphan_queue_cleanup()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Orphan cleanup error: {e}")
                await asyncio.sleep(300)  # Wait 5 min on error before retry

    async def _do_orphan_cleanup(self):
        """Perform the actual orphan match cleanup."""
        try:
            async with DatabaseHelper._get_db() as db:
                # Find active matches (not cancelled, no winner)
                async with db.execute(
                    """SELECT match_id, game_id, channel_id, red_role_id, blue_role_id,
                              red_vc_id, blue_vc_id, created_at, short_id
                       FROM matches
                       WHERE winning_team IS NULL AND cancelled = 0"""
                ) as cursor:
                    active_matches = await cursor.fetchall()

                orphaned_count = 0
                for match in active_matches:
                    # Convert Row to dict for easier access
                    match_dict = dict(match)
                    match_id = match_dict["match_id"]
                    game_id = match_dict["game_id"]
                    channel_id = match_dict["channel_id"]
                    red_role_id = match_dict["red_role_id"]
                    blue_role_id = match_dict["blue_role_id"]
                    created_at_str = match_dict["created_at"]
                    short_id = match_dict.get("short_id") or str(match_id)
                    red_vc_id = match_dict.get("red_vc_id")
                    blue_vc_id = match_dict.get("blue_vc_id")

                    # Get the game to find the guild
                    game = await DatabaseHelper.get_game(game_id)
                    if not game or not game.queue_channel_id:
                        continue

                    guild = None
                    for g in self.bot.guilds:
                        if g.get_channel(game.queue_channel_id):
                            guild = g
                            break

                    if not guild:
                        continue

                    is_orphaned = False
                    reasons = []

                    # Check 1: Match channel doesn't exist or was never created
                    if channel_id:
                        channel = guild.get_channel(channel_id)
                        if not channel:
                            # Cache miss — confirm via API before marking orphaned to avoid
                            # killing live matches right after a bot restart
                            try:
                                channel = await guild.fetch_channel(channel_id)
                            except discord.NotFound:
                                is_orphaned = True
                                reasons.append("channel deleted")
                            except (discord.Forbidden, discord.HTTPException):
                                pass  # Can't verify — skip to avoid false positive
                    else:
                        # No channel ID at all - definitely orphaned if older than 5 minutes
                        if created_at_str:
                            try:
                                created_at = datetime.fromisoformat(created_at_str)
                                if datetime.now(timezone.utc) - created_at > timedelta(minutes=5):
                                    is_orphaned = True
                                    reasons.append("no channel created")
                            except Exception:
                                is_orphaned = True
                                reasons.append("no channel, invalid date")

                    # Check 2: Both team roles don't exist
                    red_role = guild.get_role(red_role_id) if red_role_id else None
                    blue_role = guild.get_role(blue_role_id) if blue_role_id else None
                    if not red_role and not blue_role and (red_role_id or blue_role_id):
                        is_orphaned = True
                        if "channel" not in str(reasons):
                            reasons.append("roles deleted")

                    # Check 3: Match is older than 6 hours without activity (very stale)
                    if created_at_str and not is_orphaned:
                        try:
                            created_at = datetime.fromisoformat(created_at_str)
                            if datetime.now(timezone.utc) - created_at > timedelta(hours=6):
                                ch_exists = bool(guild.get_channel(channel_id)) if channel_id else False
                                if not ch_exists and channel_id:
                                    try:
                                        await guild.fetch_channel(channel_id)
                                        ch_exists = True
                                    except discord.NotFound:
                                        pass
                                    except (discord.Forbidden, discord.HTTPException):
                                        ch_exists = True  # Can't confirm deletion — assume alive
                                if not channel_id or not ch_exists:
                                    is_orphaned = True
                                    reasons.append("stale (6h+)")
                        except Exception:
                            pass

                    if is_orphaned:
                        logger.info(f"Cleaning up orphaned match #{match_id} ({short_id}): {', '.join(reasons)}")

                        # Mark as cancelled
                        await db.execute(
                            "UPDATE matches SET cancelled = 1 WHERE match_id = ?",
                            (match_id,)
                        )

                        # Clean up roles if they still exist
                        if red_role:
                            try:
                                await red_role.delete()
                            except Exception:
                                pass
                        if blue_role:
                            try:
                                await blue_role.delete()
                            except Exception:
                                pass

                        # Clean up VCs if they exist
                        if red_vc_id:
                            vc = guild.get_channel(red_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception:
                                    pass
                        if blue_vc_id:
                            vc = guild.get_channel(blue_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception:
                                    pass

                        orphaned_count += 1

                await db.commit()

                if orphaned_count > 0:
                    logger.info(f"Cleaned up {orphaned_count} orphaned matches")

        except Exception as e:
            logger.error(f"Error in _do_orphan_cleanup: {e}", exc_info=True)

    async def _do_orphan_queue_cleanup(self):
        """Force-restore queues stuck in transient states for too long."""
        try:
            now = datetime.now(timezone.utc)
            for qid, qs in list(self.queues.items()):
                game = await DatabaseHelper.get_game(qs.game_id)
                if not game:
                    continue
                channel = self.bot.get_channel(qs.channel_id)
                if not channel:
                    continue

                if qs.state == "ready_check" and qs.ready_check_started:
                    max_age = game.ready_timer_seconds + 60
                    elapsed = (now - qs.ready_check_started).total_seconds()
                    if elapsed > max_age:
                        logger.warning(
                            f"Orphan queue cleanup: queue {qid} stuck in ready_check "
                            f"for {elapsed:.0f}s (max {max_age}s) — restoring to waiting"
                        )
                        await self._restore_queue_to_waiting(channel, game, qs)

                elif qs.state == "starting_match":
                    # starting_match should resolve in seconds; 120s means something is stuck
                    if qs.ready_check_started:
                        elapsed = (now - qs.ready_check_started).total_seconds()
                    else:
                        elapsed = 999  # No timestamp — assume stuck
                    if elapsed > 120:
                        logger.warning(
                            f"Orphan queue cleanup: queue {qid} stuck in starting_match "
                            f"for {elapsed:.0f}s — restoring to waiting"
                        )
                        await self._restore_queue_to_waiting(channel, game, qs)
        except Exception as e:
            logger.error(f"Error in _do_orphan_queue_cleanup: {e}", exc_info=True)

    async def manual_orphan_cleanup(self, guild: discord.Guild) -> Tuple[int, List[str]]:
        """Manually trigger orphan cleanup. Returns (cleaned_count, details)."""
        details = []
        cleaned_count = 0

        try:
            async with DatabaseHelper._get_db() as db:
                # Find active matches
                async with db.execute(
                    """SELECT match_id, game_id, channel_id, red_role_id, blue_role_id,
                              red_vc_id, blue_vc_id, created_at, short_id
                       FROM matches
                       WHERE winning_team IS NULL AND cancelled = 0"""
                ) as cursor:
                    active_matches = await cursor.fetchall()

                for match in active_matches:
                    match_id = match["match_id"]
                    channel_id = match["channel_id"]
                    red_role_id = match["red_role_id"]
                    blue_role_id = match["blue_role_id"]
                    # Use dict() to convert Row, then .get() for optional fields
                    match_dict = dict(match)
                    short_id = match_dict.get("short_id") or str(match_id)
                    red_vc_id = match_dict.get("red_vc_id")
                    blue_vc_id = match_dict.get("blue_vc_id")

                    is_orphaned = False
                    reasons = []

                    # Check channel - if no channel, definitely stale
                    if channel_id:
                        channel = guild.get_channel(channel_id)
                        if not channel:
                            is_orphaned = True
                            reasons.append("channel deleted")
                    else:
                        is_orphaned = True
                        reasons.append("no channel")

                    # Check roles - if both roles are missing, also stale
                    red_role = guild.get_role(red_role_id) if red_role_id else None
                    blue_role = guild.get_role(blue_role_id) if blue_role_id else None
                    if not red_role and not blue_role and (red_role_id or blue_role_id):
                        is_orphaned = True
                        if "no channel" not in reasons and "channel deleted" not in reasons:
                            reasons.append("roles missing")

                    if is_orphaned:
                        details.append(f"Match {short_id}: {', '.join(reasons)}")

                        # Mark as cancelled
                        await db.execute(
                            "UPDATE matches SET cancelled = 1 WHERE match_id = ?",
                            (match_id,)
                        )

                        # Delete remaining roles
                        if red_role:
                            try:
                                await red_role.delete()
                            except Exception as e:
                                logger.warning(f"Failed to delete red role for match {match_id}: {e}")
                        if blue_role:
                            try:
                                await blue_role.delete()
                            except Exception as e:
                                logger.warning(f"Failed to delete blue role for match {match_id}: {e}")

                        # Delete VCs
                        if red_vc_id:
                            vc = guild.get_channel(red_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception as e:
                                    logger.warning(f"Failed to delete red VC for match {match_id}: {e}")
                        if blue_vc_id:
                            vc = guild.get_channel(blue_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception as e:
                                    logger.warning(f"Failed to delete blue VC for match {match_id}: {e}")

                        cleaned_count += 1

                await db.commit()

        except Exception as e:
            logger.error(f"Error in manual_orphan_cleanup: {e}", exc_info=True)
            details.append(f"Error: {str(e)}")

        return cleaned_count, details

    # -------------------------------------------------------------------------
    # MATCH CHANNEL AUTO-CLEANUP
    # -------------------------------------------------------------------------

    async def match_channel_cleanup(self):
        """Background task: delete match channels/roles/VCs 12 hours after match ends."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self._do_channel_cleanup()
            except Exception as e:
                logger.error(f"Error in match_channel_cleanup: {e}", exc_info=True)
            await asyncio.sleep(3600)  # Run every hour

    async def _do_channel_cleanup(self):
        """Find ended matches older than 12 hours and clean up their Discord resources."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
        async with DatabaseHelper._get_db() as db:
            async with db.execute("""
                SELECT match_id, channel_id, draft_channel_id, red_role_id, blue_role_id,
                       red_vc_id, blue_vc_id, game_id
                FROM matches
                WHERE ended_at IS NOT NULL AND ended_at < ?
                AND (channel_id IS NOT NULL OR red_role_id IS NOT NULL)
            """, (cutoff,)) as cursor:
                stale_matches = await cursor.fetchall()

        if not stale_matches:
            return

        for guild in self.bot.guilds:
            for match in stale_matches:
                cleaned_something = False
                # Delete text channels
                for col in ('channel_id', 'draft_channel_id'):
                    ch_id = match[col]
                    if ch_id:
                        channel = guild.get_channel(ch_id)
                        if channel:
                            try:
                                await channel.delete(reason="Auto-cleanup: match ended 12+ hours ago")
                                cleaned_something = True
                            except (discord.NotFound, discord.Forbidden):
                                pass
                            except Exception as e:
                                logger.error(f"Channel cleanup error for {ch_id}: {e}")

                # Delete voice channels
                for col in ('red_vc_id', 'blue_vc_id'):
                    vc_id = match[col]
                    if vc_id:
                        vc = guild.get_channel(vc_id)
                        if vc:
                            try:
                                await vc.delete(reason="Auto-cleanup: match ended 12+ hours ago")
                                cleaned_something = True
                            except (discord.NotFound, discord.Forbidden):
                                pass
                            except Exception as e:
                                logger.error(f"VC cleanup error for {vc_id}: {e}")

                # Delete roles
                for col in ('red_role_id', 'blue_role_id'):
                    role_id = match[col]
                    if role_id:
                        role = guild.get_role(role_id)
                        if role:
                            try:
                                await role.delete(reason="Auto-cleanup: match ended 12+ hours ago")
                                cleaned_something = True
                            except (discord.NotFound, discord.Forbidden):
                                pass
                            except Exception as e:
                                logger.error(f"Role cleanup error for {role_id}: {e}")

                # Null out the IDs so we don't try again
                if cleaned_something or True:
                    async with DatabaseHelper._get_db() as db:
                        await db.execute("""
                            UPDATE matches SET channel_id = NULL, draft_channel_id = NULL,
                                red_role_id = NULL, blue_role_id = NULL,
                                red_vc_id = NULL, blue_vc_id = NULL
                            WHERE match_id = ?
                        """, (match['match_id'],))
                        await db.commit()

        if stale_matches:
            logger.info(f"Channel cleanup: processed {len(stale_matches)} ended matches")

    # -------------------------------------------------------------------------
    # HELPER METHODS
    # -------------------------------------------------------------------------

    async def is_cm_admin(self, member: discord.Member) -> bool:
        """Check if member is a CM admin."""
        if member.guild_permissions.administrator:
            return True
        
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        if admin_role_id:
            return any(r.id == int(admin_role_id) for r in member.roles)
        return False
    
    async def _check_and_update_ign(self, user_id: int, game_id: int):
        """Check if a Valorant player's Riot ID has changed via PUUID lookup.

        Silently updates the database if a name change is detected.
        Uses a 1-hour per-user cooldown to avoid excessive API calls.

        The HenrikDev PUUID-to-account endpoint can return stale cached data,
        so we verify the result by also looking up the returned name. If that
        lookup fails or returns a different PUUID, we fall back to checking
        recent match history for the player's current IGN.
        """
        try:
            # Prune stale cache entries (older than 2 hours) when cache gets large
            if len(self.ign_update_cache) > 500:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
                self.ign_update_cache = {k: v for k, v in self.ign_update_cache.items() if v > cutoff}

            # Check cooldown (1 hour)
            last_check = self.ign_update_cache.get(user_id)
            if last_check and (datetime.now(timezone.utc) - last_check).total_seconds() < 3600:
                return

            puuid = await DatabaseHelper.get_player_puuid(user_id, game_id)
            if not puuid:
                return

            account = await self.henrik_api.get_account_by_puuid(puuid)
            if not account:
                # API failed — update cache to avoid hammering
                self.ign_update_cache[user_id] = datetime.now(timezone.utc)
                return

            api_name = account.get('name', '')
            api_tag = account.get('tag', '')
            if not api_name or not api_tag:
                self.ign_update_cache[user_id] = datetime.now(timezone.utc)
                return

            new_ign = f"{api_name}#{api_tag}"
            current_ign = await DatabaseHelper.get_player_ign(user_id, game_id)

            # Verify the PUUID result isn't stale: look up the returned name
            # and check it resolves to the same PUUID. If not, the name is outdated.
            if current_ign and new_ign.lower() == current_ign.lower():
                # PUUID endpoint returned the same name we have stored.
                # Verify this name still exists — if it doesn't, a rename happened
                # but the PUUID cache is stale (returning the old name).
                verify = await self.henrik_api.get_account(api_name, api_tag)
                if not verify or verify.get('puuid') != puuid:
                    # Name no longer valid — find current IGN from match history
                    logger.info(
                        f"IGN check for user {user_id}: PUUID endpoint returned stale name "
                        f"'{new_ign}', verifying via match history..."
                    )
                    fresh_ign = await self._resolve_current_ign_from_matches(puuid)
                    if fresh_ign and fresh_ign.lower() != current_ign.lower():
                        new_ign = fresh_ign
                    else:
                        # Couldn't determine current name — skip update
                        self.ign_update_cache[user_id] = datetime.now(timezone.utc)
                        return

            if current_ign and current_ign.lower() != new_ign.lower():
                await DatabaseHelper.set_player_ign(user_id, game_id, new_ign, puuid=puuid)
                logger.info(f"Auto-updated Riot ID for user {user_id}: '{current_ign}' -> '{new_ign}'")

            self.ign_update_cache[user_id] = datetime.now(timezone.utc)
        except Exception as e:
            logger.debug(f"IGN update check failed for user {user_id}: {e}")

    async def _resolve_current_ign_from_matches(self, puuid: str) -> Optional[str]:
        """Find a player's current IGN by checking their recent match history.

        Match data always contains the player's current IGN at the time of the match,
        so the most recent match gives us the freshest name.
        """
        try:
            matches = await self.henrik_api.get_match_history_v4_by_puuid(
                puuid, 'na', 'console', mode=None
            )
            if not matches:
                matches = await self.henrik_api.get_match_history_by_puuid(
                    puuid, 'na', mode=None
                )
            if not matches:
                return None

            # Check the most recent match for this player's current name
            for match in matches[:3]:
                players_data = match.get('players', {})
                player_lists = []
                if isinstance(players_data, dict):
                    if 'all_players' in players_data:
                        player_lists.append(players_data['all_players'])
                    else:
                        for team in ['red', 'blue', 'Red', 'Blue']:
                            if team in players_data:
                                player_lists.append(players_data[team])
                elif isinstance(players_data, list):
                    for item in players_data:
                        if isinstance(item, list):
                            player_lists.append(item)
                        elif isinstance(item, dict):
                            player_lists.append([item])

                for plist in player_lists:
                    for vp in plist:
                        if vp.get('puuid') == puuid:
                            vp_name = vp.get('name', '')
                            vp_tag = vp.get('tag', '')
                            if vp_name and vp_tag:
                                return f"{vp_name}#{vp_tag}"
            return None
        except Exception as e:
            logger.debug(f"Failed to resolve IGN from match history for puuid {puuid[:8]}...: {e}")
            return None

    async def update_mmr_roles(self, guild: discord.Guild, player_id: int, game_id: int, new_mmr: int):
        """Update a player's MMR role based on their current MMR.

        Finds the highest MMR threshold <= player's MMR and assigns that role,
        removing any other MMR roles for the game.
        """
        try:
            mmr_roles = await DatabaseHelper.get_mmr_roles(game_id)
            if not mmr_roles:
                return

            member = guild.get_member(player_id)
            if not member:
                return

            # Sort thresholds ascending to find highest threshold <= player MMR
            sorted_roles = sorted(mmr_roles.items(), key=lambda x: x[1])
            all_mmr_role_ids = set(mmr_roles.keys())

            # Find the correct role (highest threshold <= player's MMR)
            target_role_id = None
            for role_id, threshold in sorted_roles:
                if new_mmr >= threshold:
                    target_role_id = role_id

            # Determine roles to remove and add
            roles_to_remove = [r for r in member.roles if r.id in all_mmr_role_ids and r.id != target_role_id]
            has_target = any(r.id == target_role_id for r in member.roles)

            if roles_to_remove:
                await member.remove_roles(*roles_to_remove, reason="MMR role update")
            if target_role_id and not has_target:
                role = guild.get_role(target_role_id)
                if role:
                    await member.add_roles(role, reason="MMR role update")
        except Exception as e:
            logger.error(f"Error updating MMR roles for player {player_id}: {e}")

    async def log_action(self, guild: discord.Guild, message: str, prefix: str = ""):
        """Log an action to the log channel. Optional prefix (e.g. emoji) before timestamp."""
        channel_id = await DatabaseHelper.get_config("log_channel_id")
        if channel_id:
            channel = guild.get_channel(int(channel_id))
            if channel:
                pfx = f"{prefix} " if prefix else ""
                await channel.send(f"{pfx}`[{datetime.now().strftime('%H:%M:%S')}]` {message}")

    async def _get_match_short_id(self, match_id: int) -> str:
        """Get the short_id for a match, or return match_id as string."""
        match = await DatabaseHelper.get_match(match_id)
        if match and match.get("short_id"):
            return match["short_id"]
        return str(match_id)

    async def _get_map_image_url(self, game_name: str, map_name: str) -> Optional[str]:
        """Get map image URL from map_voter_config.json for use as embed thumbnail."""
        try:
            def _load_config():
                with open("map_voter_config.json", "r") as f:
                    return json.load(f)
            config = await asyncio.to_thread(_load_config)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

        universal = config.get("universal_games", {})
        # Case-insensitive game name lookup
        game_data = None
        for key, value in universal.items():
            if key.lower() == game_name.lower():
                game_data = value
                break
        if not game_data:
            return None

        maps = game_data.get("maps", {})
        map_data = maps.get(map_name)
        if not map_data:
            return None

        return map_data.get("url")  # May be null (e.g. Pearl)

    def _build_mmr_log_line(
        self, guild: discord.Guild, pid: int, mmr: int,
        role_prefs: Dict[int, Tuple[str, Optional[str]]],
        role_emojis: dict, is_rivals: bool,
        change: Optional[int] = None
    ) -> str:
        """Build a single player line for the MMR log embed."""
        member = guild.get_member(pid)
        name = member.display_name if member else str(pid)
        name = sanitize_for_codeblock(name, fallback=member.name if member else None)
        name = truncate_to_width(name, 13)
        padded_name = pad_to_width(name, 13)

        if change is not None:
            sign = "+" if change >= 0 else "-"
            code_part = f"`{padded_name} {mmr:>4} {sign}{abs(change)}`"
        else:
            code_part = f"`{padded_name} {mmr:>4}`"

        if is_rivals:
            prefs = role_prefs.get(pid)
            none_emoji = role_emojis.get("none", "")
            if prefs:
                p_emoji = role_emojis.get(prefs[0], none_emoji)
                s_emoji = role_emojis.get(prefs[1], none_emoji) if prefs[1] else none_emoji
            else:
                p_emoji = none_emoji
                s_emoji = none_emoji
            return f"{p_emoji}{s_emoji}{code_part}"
        else:
            return code_part

    async def _send_mmr_embed_to_log(
        self, guild: discord.Guild, game: GameConfig, match_id: int,
        red_team: List[int], blue_team: List[int], igns: Dict[int, str],
        red_role: discord.Role, blue_role: discord.Role,
        reshuffled: bool = False
    ):
        """Send team embed with MMR values to log channel (admin only)."""
        channel_id = await DatabaseHelper.get_config("log_channel_id")
        if not channel_id:
            return

        channel = guild.get_channel(int(channel_id))
        if not channel:
            return

        short_id = await self._get_match_short_id(match_id)
        is_rivals = 'rivals' in game.name.lower()

        # Fetch role data for Rivals
        role_emojis = {}
        role_prefs = {}
        if is_rivals:
            role_emojis = _resolve_role_emojis(await DatabaseHelper.get_role_emojis(), self.bot)
            all_pids = red_team + blue_team
            role_prefs = await DatabaseHelper.get_bulk_role_prefs(all_pids, game.game_id)

        # Build red team lines with MMR
        red_lines = []
        red_total_mmr = 0
        for pid in red_team:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            mmr = stats.effective_mmr
            red_total_mmr += mmr
            red_lines.append(self._build_mmr_log_line(
                guild, pid, mmr, role_prefs, role_emojis, is_rivals
            ))

        # Build blue team lines with MMR
        blue_lines = []
        blue_total_mmr = 0
        for pid in blue_team:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            mmr = stats.effective_mmr
            blue_total_mmr += mmr
            blue_lines.append(self._build_mmr_log_line(
                guild, pid, mmr, role_prefs, role_emojis, is_rivals
            ))

        red_avg = red_total_mmr // len(red_team) if red_team else 0
        blue_avg = blue_total_mmr // len(blue_team) if blue_team else 0

        title = f"Match {short_id} — {game.name}"
        if reshuffled:
            title += " (Reshuffled)"
        embed = discord.Embed(
            title=title,
            color=COLOR_NEUTRAL
        )
        embed.add_field(
            name=f"{red_role.name if red_role else 'Red Team'} (Avg: {red_avg})",
            value="\n".join(red_lines) or "—",
            inline=False
        )
        embed.add_field(
            name=f"{blue_role.name if blue_role else 'Blue Team'} (Avg: {blue_avg})",
            value="\n".join(blue_lines) or "—",
            inline=False
        )

        msg = await channel.send(embed=embed)
        # Store message ID for post-match editing
        await DatabaseHelper.update_match(match_id, log_msg_id=msg.id)

    async def _edit_prematch_log_embed(
        self, guild: discord.Guild, log_channel: discord.TextChannel,
        game: GameConfig, match_id: int, players: List[dict],
        winner_results: List[tuple], loser_results: List[tuple]
    ):
        """Edit the pre-match log embed to add +/- MMR changes after match result."""
        match = await DatabaseHelper.get_match(match_id)
        if not match or not match.get("log_msg_id"):
            return

        try:
            log_msg = await log_channel.fetch_message(int(match["log_msg_id"]))
        except (discord.NotFound, discord.HTTPException):
            return

        # Build pid → (old_mmr, change) mapping
        mmr_changes = {}
        for pid, old_mmr, change in winner_results:
            mmr_changes[pid] = (old_mmr, change)
        for pid, old_mmr, change in loser_results:
            mmr_changes[pid] = (old_mmr, change)

        # Separate players by team
        red_pids = [p["player_id"] for p in players if p["team"] == "red"]
        blue_pids = [p["player_id"] for p in players if p["team"] == "blue"]

        is_rivals = 'rivals' in game.name.lower()
        role_emojis = {}
        role_prefs = {}
        if is_rivals:
            role_emojis = _resolve_role_emojis(await DatabaseHelper.get_role_emojis(), self.bot)
            all_pids = red_pids + blue_pids
            role_prefs = await DatabaseHelper.get_bulk_role_prefs(all_pids, game.game_id)

        # Rebuild lines with +/- changes
        red_lines = []
        red_total_mmr = 0
        for pid in red_pids:
            old_mmr, change = mmr_changes.get(pid, (0, 0))
            red_total_mmr += old_mmr
            red_lines.append(self._build_mmr_log_line(
                guild, pid, old_mmr, role_prefs, role_emojis, is_rivals, change=change
            ))

        blue_lines = []
        blue_total_mmr = 0
        for pid in blue_pids:
            old_mmr, change = mmr_changes.get(pid, (0, 0))
            blue_total_mmr += old_mmr
            blue_lines.append(self._build_mmr_log_line(
                guild, pid, old_mmr, role_prefs, role_emojis, is_rivals, change=change
            ))

        red_avg = red_total_mmr // len(red_pids) if red_pids else 0
        blue_avg = blue_total_mmr // len(blue_pids) if blue_pids else 0

        # Edit the existing embed
        embed = log_msg.embeds[0] if log_msg.embeds else discord.Embed(color=COLOR_NEUTRAL)
        embed.clear_fields()

        # Show the map name under the header (applies to all games)
        map_name = match.get("map_name")
        if map_name:
            embed.description = f"({map_name})"

        # Reconstruct with original role names from embed field names if possible
        red_role = guild.get_role(int(match.get("red_role_id", 0)))
        blue_role = guild.get_role(int(match.get("blue_role_id", 0)))
        red_name = red_role.name if red_role else "Red Team"
        blue_name = blue_role.name if blue_role else "Blue Team"

        embed.add_field(
            name=f"{red_name} (Avg: {red_avg})",
            value="\n".join(red_lines) or "—",
            inline=False
        )
        embed.add_field(
            name=f"{blue_name} (Avg: {blue_avg})",
            value="\n".join(blue_lines) or "—",
            inline=False
        )

        await log_msg.edit(embed=embed)

    async def refresh_match_embeds(self, guild: discord.Guild, match_id: int, reshuffled: bool = False):
        """Refresh the match channel embed and queue teams embed after a sub/swap."""
        try:
            match = await DatabaseHelper.get_match(match_id)
            if not match:
                return

            game = await DatabaseHelper.get_game(match["game_id"])
            if not game:
                return

            players = await DatabaseHelper.get_match_players(match_id)
            igns = await DatabaseHelper.get_match_igns(match_id)
            short_id = match.get("short_id") or str(match_id)

            red_players = [p for p in players if p["team"] == "red"]
            blue_players = [p for p in players if p["team"] == "blue"]
            red_team = [p["player_id"] for p in red_players]
            blue_team = [p["player_id"] for p in blue_players]
            red_captain = next((p["player_id"] for p in red_players if p["was_captain"]), None)
            blue_captain = next((p["player_id"] for p in blue_players if p["was_captain"]), None)

            # --- Update match channel embed ---
            match_channel = guild.get_channel(match["channel_id"]) if match["channel_id"] else None
            if match_channel:
                embed = discord.Embed(
                    title=f"{game.name} Match {short_id}",
                    color=COLOR_NEUTRAL
                )

                red_lines = []
                for pid in red_team:
                    if pid in igns:
                        line = f"`{igns[pid]}`"
                    else:
                        m = guild.get_member(pid)
                        line = m.display_name if m else f"<@{pid}>"
                    if pid == red_captain:
                        line += " (C)"
                    red_lines.append(line)

                blue_lines = []
                for pid in blue_team:
                    if pid in igns:
                        line = f"`{igns[pid]}`"
                    else:
                        m = guild.get_member(pid)
                        line = m.display_name if m else f"<@{pid}>"
                    if pid == blue_captain:
                        line += " (C)"
                    blue_lines.append(line)

                embed.add_field(name="Red Team", value="\n".join(red_lines) or "—", inline=True)
                embed.add_field(name="Blue Team", value="\n".join(blue_lines) or "—", inline=True)

                # Rebuild VC info if present
                if match.get("red_vc_id") and match.get("blue_vc_id"):
                    embed.add_field(
                        name="Voice Channels",
                        value=f"Red: <#{match['red_vc_id']}>\nBlue: <#{match['blue_vc_id']}>",
                        inline=False
                    )

                # Map name if set
                if match.get("map_name"):
                    map_image_url = await self._get_map_image_url(game.name, match["map_name"])
                    embed.add_field(name="Map", value=match["map_name"], inline=False)
                    if map_image_url:
                        embed.set_thumbnail(url=map_image_url)

                embed.add_field(
                    name="Report Winner",
                    value="Use `/cm_win` when the match is over, or `/cm_abandon` to cancel.",
                    inline=False
                )

                is_sec = bool(match.get("is_secondary"))
                banner = game.secondary_banner_url if is_sec and game.secondary_banner_url else game.banner_url
                if banner:
                    embed.set_image(url=banner)

                # Try to edit the stored message, fall back to searching channel
                msg = None
                if match.get("match_msg_id"):
                    try:
                        msg = await match_channel.fetch_message(match["match_msg_id"])
                    except Exception:
                        pass

                if not msg:
                    # Fall back: find the first bot embed message in the channel
                    async for m in match_channel.history(oldest_first=True, limit=10):
                        if m.author == guild.me and m.embeds:
                            msg = m
                            break

                if msg:
                    await msg.edit(embed=embed)

            # --- Update queue channel teams embed ---
            if match.get("queue_teams_msg_id") and game.queue_channel_id:
                queue_channel = guild.get_channel(game.queue_channel_id)
                if queue_channel:
                    try:
                        teams_msg = await queue_channel.fetch_message(match["queue_teams_msg_id"])
                        teams_embed = discord.Embed(
                            title="Ongoing Match",
                            description="⚠️ Lineups updated" if reshuffled else None,
                            url=match_channel.jump_url if match_channel else None,
                            color=COLOR_NEUTRAL
                        )
                        teams_embed.set_footer(text=f"Match {short_id}")

                        red_names = []
                        for pid in red_team:
                            if pid in igns:
                                red_names.append(f"`{igns[pid]}`")
                            else:
                                member = guild.get_member(pid)
                                red_names.append(member.display_name if member else f"<@{pid}>")
                        blue_names = []
                        for pid in blue_team:
                            if pid in igns:
                                blue_names.append(f"`{igns[pid]}`")
                            else:
                                member = guild.get_member(pid)
                                blue_names.append(member.display_name if member else f"<@{pid}>")

                        teams_embed.add_field(name="Red Team", value="\n".join(red_names) or "—", inline=True)
                        teams_embed.add_field(name="Blue Team", value="\n".join(blue_names) or "—", inline=True)
                        await teams_msg.edit(embed=teams_embed)
                    except Exception as e:
                        logger.warning(f"Failed to update queue teams embed for match {match_id}: {e}")

        except Exception as e:
            logger.error(f"Error refreshing match embeds for match {match_id}: {e}")

    async def get_next_role_number(self, guild: discord.Guild, prefix: str) -> int:
        """Get the next available role number (e.g., Blue1, Blue2)."""
        existing = [r for r in guild.roles if r.name.startswith(prefix)]
        numbers = []
        for r in existing:
            try:
                num = int(r.name[len(prefix):])
                numbers.append(num)
            except ValueError:
                pass
        
        for i in range(1, 100):
            if i not in numbers:
                return i
        return 1
    
    async def get_next_channel_suffix(self, guild: discord.Guild, category: discord.CategoryChannel, 
                                       base_name: str) -> str:
        """Get the next available channel name."""
        existing = [c.name for c in category.channels if c.name.startswith(base_name)]
        if base_name not in existing:
            return base_name
        
        for i in range(2, 100):
            name = f"{base_name}-{i}"
            if name not in existing:
                return name
        return f"{base_name}-new"
    
    # -------------------------------------------------------------------------
    # QUEUE MANAGEMENT
    # -------------------------------------------------------------------------

    def _get_effective_player_count(self, game: GameConfig, queue_state: QueueState) -> int:
        """Return the correct player_count for this queue (secondary overrides main)."""
        if queue_state.is_secondary and game.secondary_queue_player_count:
            return game.secondary_queue_player_count
        return game.player_count

    def _get_effective_queue_type(self, game: GameConfig, queue_state: QueueState) -> QueueType:
        """Return the correct queue_type for this queue (secondary overrides main)."""
        if queue_state.is_secondary and game.secondary_queue_type:
            return game.secondary_queue_type
        return game.queue_type

    async def create_queue_embed(self, game: GameConfig, queue_state: QueueState, guild: discord.Guild = None) -> discord.Embed:
        """Create the queue embed."""
        player_count = len(queue_state.players)
        effective_pc = self._get_effective_player_count(game, queue_state)
        effective_qt = self._get_effective_queue_type(game, queue_state)

        description = f"Players: {player_count}/{effective_pc}"
        lurker_count = await DatabaseHelper.get_subscriber_count(queue_state.queue_id)
        if lurker_count >= 1 and player_count < effective_pc - 1:
            description += f"\n-# Lurkers: {lurker_count}"

        if queue_state.is_secondary and game.secondary_queue_name:
            title = f"{game.name}: {game.secondary_queue_name} ({effective_qt.value.upper()})"
        else:
            title = f"{game.name} Queue ({effective_qt.value.upper()})"

        embed = discord.Embed(
            title=title,
            description=description,
            color=COLOR_NEUTRAL
        )

        if queue_state.players:
            player_ids = list(queue_state.players.keys())
            ign_set = await DatabaseHelper.get_players_with_ign(player_ids, game.game_id)

            # Fetch role emojis for games with role_required
            role_prefs_map = {}
            role_emojis = {}
            if game.role_required:
                role_prefs_map = await DatabaseHelper.get_bulk_role_prefs(player_ids, game.game_id)
                role_emojis = _resolve_role_emojis(await DatabaseHelper.get_role_emojis(), self.bot)

            # Fetch win streaks for badge display
            win_streaks = await DatabaseHelper.get_current_win_streaks_batch(player_ids, game.game_id)

            lines = []
            for pid in queue_state.players.keys():
                member = guild.get_member(pid) if guild else None
                name = member.display_name if member else f"<@{pid}>"
                prefix = ""
                if role_emojis and game.role_required:
                    none_emoji = role_emojis.get("none", "➖")
                    if pid in role_prefs_map:
                        prefs = role_prefs_map[pid]
                        p_emoji = role_emojis.get(prefs[0], none_emoji)
                        s_emoji = role_emojis.get(prefs[1], none_emoji) if prefs[1] else none_emoji
                    else:
                        p_emoji = none_emoji
                        s_emoji = none_emoji
                    prefix = f"{p_emoji}{s_emoji} | "
                if prefix:
                    name = f"{prefix}{name}"
                if pid in ign_set:
                    name += " Ⓘ"
                # Win streak badge
                streak = win_streaks.get(pid, 0)
                if streak >= 5:
                    name += f" 🔥×{streak}"
                elif streak >= 3:
                    name += " 🔥"
                lines.append(f"- {name}")
            player_list = "\n".join(lines)
            embed.add_field(name="Joined", value=player_list, inline=False)

        # Add banner image if configured (secondary queue can override)
        banner = game.secondary_banner_url if queue_state.is_secondary and game.secondary_banner_url else game.banner_url
        if banner:
            embed.set_image(url=banner)

        # Add short_id to footer
        if queue_state.short_id:
            embed.set_footer(text=queue_state.short_id)

        return embed
    
    async def create_ready_check_embed(self, game: GameConfig, queue_state: QueueState,
                                        time_remaining: int, guild: discord.Guild = None) -> discord.Embed:
        """Create the ready check embed with emoji indicators."""
        ready_count = sum(1 for is_ready in queue_state.players.values() if is_ready)
        total_count = len(queue_state.players)

        if queue_state.is_secondary and game.secondary_queue_name:
            rc_title = f"{game.secondary_queue_name} - Ready Check!"
        else:
            rc_title = "Queue Full - Ready Check!"

        embed = discord.Embed(
            title=rc_title,
            description=f"Time remaining: {time_remaining}s\n\nReady: {ready_count}/{total_count}",
            color=COLOR_WARNING
        )

        # Build player list with emoji indicators
        player_lines = []
        for pid, is_ready in queue_state.players.items():
            emoji = game.ready_done_emoji if is_ready else game.ready_loading_emoji
            member = guild.get_member(pid) if guild else None
            name = member.display_name if member else f"<@{pid}>"
            # Show "(auto)" only for players who were actually auto-readied via grace period
            auto_tag = " (auto)" if pid in queue_state.auto_readied else ""
            player_lines.append(f"{emoji} {name}{auto_tag}")

        embed.add_field(
            name="Players",
            value="\n".join(player_lines) if player_lines else "No players",
            inline=False
        )

        # Add banner image if configured (secondary queue can override)
        banner = game.secondary_banner_url if queue_state.is_secondary and game.secondary_banner_url else game.banner_url
        if banner:
            embed.set_image(url=banner)

        return embed

    async def start_queue(self, channel: discord.TextChannel, game: GameConfig,
                         is_secondary: bool = False) -> int:
        """Start a new queue for a game. Only one queue type (standard/arcade) per game at a time."""
        # Remove any existing queue for this game (mutual exclusivity)
        for qid, qs in list(self.queues.items()):
            if qs.game_id == game.game_id and qs.state in ("waiting", "ready_check", "starting_match"):
                if qs.is_secondary == is_secondary and qs.channel_id == channel.id:
                    # Same type, same channel — return existing queue
                    logger.warning(
                        f"start_queue: active queue {qid} already exists for game {game.game_id} "
                        f"in channel {channel.id} (secondary={is_secondary}, state={qs.state}) — returning existing queue"
                    )
                    return qid
                # Different type or channel — delete the old queue
                logger.info(
                    f"start_queue: removing existing queue {qid} (secondary={qs.is_secondary}) "
                    f"for game {game.game_id} to start new queue (secondary={is_secondary})"
                )
                if qs.message_id:
                    try:
                        old_ch = self.bot.get_channel(qs.channel_id)
                        if old_ch:
                            old_msg = await old_ch.fetch_message(qs.message_id)
                            await old_msg.delete()
                    except (discord.NotFound, Exception):
                        pass
                if qid in self.ready_check_tasks:
                    self.ready_check_tasks[qid].cancel()
                    del self.ready_check_tasks[qid]
                del self.queues[qid]
                self.queue_locks.pop(qid, None)
                async with DatabaseHelper._get_db() as db:
                    await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (qid,))
                    await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (qid,))
                    await db.commit()

        # Generate a short ID for this queue
        short_id = generate_short_id()

        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                "INSERT INTO active_queues (game_id, channel_id, state, short_id, is_secondary) VALUES (?, ?, 'waiting', ?, ?)",
                (game.game_id, channel.id, short_id, int(is_secondary))
            )
            queue_id = cursor.lastrowid
            await db.commit()

        queue_state = QueueState(
            queue_id=queue_id,
            game_id=game.game_id,
            channel_id=channel.id,
            short_id=short_id,
            is_secondary=is_secondary,
        )
        self.queues[queue_id] = queue_state

        embed = await self.create_queue_embed(game, queue_state, channel.guild)
        view = QueueView(self, game.game_id, queue_id)
        try:
            msg = await channel.send(embed=embed, view=view)
        except Exception as e:
            logger.error(f"start_queue: failed to send queue embed for queue {queue_id}: {e}")
            del self.queues[queue_id]
            self.queue_locks.pop(queue_id, None)
            async with DatabaseHelper._get_db() as db:
                await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (queue_id,))
                await db.commit()
            raise

        queue_state.message_id = msg.id
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                (msg.id, queue_id)
            )
            await db.commit()

        return queue_id

    def _player_has_grace(self, queue_state: QueueState, player_id: int, game: GameConfig) -> bool:
        """Check if a player has an active per-player grace period."""
        join_time = queue_state.grace_timers.get(player_id)
        if not join_time:
            return False
        elapsed = (datetime.now(timezone.utc) - join_time).total_seconds()
        return elapsed < (game.grace_period_minutes * 60)

    async def _notify_queue_subscribers(
        self, guild: discord.Guild, queue_id: int, game: GameConfig,
        remaining: int, queue_state: QueueState
    ):
        """DM subscribers whose threshold >= remaining slots."""
        try:
            subscribers = await DatabaseHelper.get_queue_subscribers(queue_id, remaining)
            if not subscribers:
                return

            queue_player_ids = set(queue_state.players.keys())
            for player_id, threshold in subscribers:
                # Skip players already in the queue
                if player_id in queue_player_ids:
                    continue
                member = guild.get_member(player_id)
                if not member:
                    continue
                try:
                    await member.send(
                        f"**{game.name}** queue needs **{remaining}** more player{'s' if remaining > 1 else ''}! "
                        f"({len(queue_state.players)}/{self._get_effective_player_count(game, queue_state)})"
                    )
                    # Remove after notifying so they don't get spammed
                    await DatabaseHelper.unsubscribe_from_queue(queue_id, player_id)
                except discord.Forbidden:
                    # DMs disabled — silently remove subscription
                    await DatabaseHelper.unsubscribe_from_queue(queue_id, player_id)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error notifying queue subscribers: {e}")

    async def _send_lf1_notification(self, game: GameConfig, queue_state: QueueState, guild: discord.Guild):
        """Send a 'Looking for 1' notification to the game's LF1 channel/thread.

        - Skipped if no lf1_channel_id is configured for the game.
        - Subject to a 30-minute cooldown per game.
        - The message auto-deletes after 15 minutes if not cleaned up sooner.
        """
        if not game.lf1_channel_id:
            return

        game_id = game.game_id
        now = datetime.now(timezone.utc)

        # Check 30-minute cooldown
        last_sent = self.lf1_cooldowns.get(game_id)
        if last_sent and (now - last_sent).total_seconds() < 1800:
            logger.info(f"LF1: Skipping for game {game.name} — cooldown active (last sent {last_sent.isoformat()})")
            return

        channel = guild.get_channel(game.lf1_channel_id)
        if not channel:
            # Try as a thread
            channel = guild.get_thread(game.lf1_channel_id)
        if not channel:
            logger.warning(f"LF1: Channel/thread {game.lf1_channel_id} not found for game {game.name}")
            return

        try:
            current_count = len(queue_state.players)
            needed = self._get_effective_player_count(game, queue_state)
            queue_channel = guild.get_channel(game.queue_channel_id) if game.queue_channel_id else None
            queue_mention = queue_channel.mention if queue_channel else "the queue channel"
            msg = await channel.send(
                f"**{game.name}** custom match queue is at **{current_count}/{needed}** — need one more! "
                f"If you'd like to join head on over to {queue_mention} and click join."
            )
            self.lf1_messages[game_id] = msg
            self.lf1_cooldowns[game_id] = now
            logger.info(f"LF1: Sent notification for game {game.name} in channel {channel.id}")

            # Schedule auto-delete after 20 minutes
            if game_id in self.lf1_tasks:
                self.lf1_tasks[game_id].cancel()
            self.lf1_tasks[game_id] = asyncio.create_task(self._lf1_auto_delete(game_id, 900))
        except Exception as e:
            logger.warning(f"LF1: Failed to send notification for game {game.name}: {e}")

    async def _lf1_auto_delete(self, game_id: int, delay: float):
        """Auto-delete the LF1 message after a delay."""
        await asyncio.sleep(delay)
        await self._delete_lf1_message(game_id)

    async def _delete_lf1_message(self, game_id: int):
        """Delete the LF1 message for a game if it exists."""
        # Cancel the auto-delete task if running
        task = self.lf1_tasks.pop(game_id, None)
        if task and not task.done():
            task.cancel()

        msg = self.lf1_messages.pop(game_id, None)
        if msg:
            try:
                await msg.delete()
                logger.info(f"LF1: Deleted notification for game_id {game_id}")
            except discord.NotFound:
                pass  # Already deleted
            except Exception as e:
                logger.warning(f"LF1: Failed to delete notification for game_id {game_id}: {e}")

    async def handle_queue_join(self, interaction: discord.Interaction, game_id: int, queue_id: int):
        """Handle a player joining the queue."""
        user = interaction.user
        game = await DatabaseHelper.get_game(game_id)

        if not game:
            await interaction.response.send_message("Game no longer exists.", ephemeral=True)
            return

        # Auto-update Riot ID in the background (non-blocking)
        if 'valorant' in game.name.lower():
            asyncio.create_task(self._check_and_update_ign(user.id, game_id))

        # Quick in-memory checks first (no DB calls)
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return

        queue_state = self.queues[queue_id]

        if queue_state.state != "waiting":
            await interaction.response.send_message("Queue is no longer accepting players.", ephemeral=True)
            return

        if user.id in queue_state.players:
            # Reset grace timer if queue is in waiting state
            if queue_state.state == "waiting":
                queue_state.grace_timers[user.id] = datetime.now(timezone.utc)
                await interaction.response.send_message(
                    f"You're already in this queue. Your {game.grace_period_minutes}min grace period timer has been reset.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message("You're already in this queue.", ephemeral=True)
            return

        # Block join if the player is already in an active ready-check for ANY game
        for qid, qs in self.queues.items():
            if qid != queue_id and user.id in qs.players \
                    and qs.state in ("ready_check", "starting_match"):
                await interaction.response.send_message(
                    "You are currently in an active ready check. Please ready up or wait for it to conclude.",
                    ephemeral=True
                )
                return

        # Remove from any other waiting queue for the SAME game (one queue per game)
        for qid, qs in list(self.queues.items()):
            if qid != queue_id and qs.game_id == game_id \
                    and user.id in qs.players and qs.state == "waiting":
                del qs.players[user.id]
                qs.grace_timers.pop(user.id, None)

        # Check Not Ready cooldown (in-memory, no DB call)
        cooldown_expiry = self.not_ready_cooldowns.get(user.id)
        if cooldown_expiry and cooldown_expiry > datetime.now(timezone.utc):
            remaining = int((cooldown_expiry - datetime.now(timezone.utc)).total_seconds())
            mins, secs = divmod(remaining, 60)
            time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
            await interaction.response.send_message(
                f"You are on cooldown for declining a ready check. Try again in **{time_str}**.",
                ephemeral=True
            )
            return
        # Clean up expired cooldown entry
        self.not_ready_cooldowns.pop(user.id, None)

        # Check verified role (if required) - no DB call, just role check
        if game.queue_role_required and game.verified_role_id:
            if not any(r.id == game.verified_role_id for r in user.roles):
                if game.verification_topic:
                    view = VerificationTicketView(self, game)
                    await interaction.response.send_message(
                        "You need the verified role to join this queue. Click the button below to get set up.",
                        view=view,
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        "You need the verified role to queue for this game.",
                        ephemeral=True
                    )
                return

        # Check IGN requirement before deferring (may need to show modal)
        if game.ign_required:
            existing_ign = await DatabaseHelper.get_player_ign(user.id, game.game_id)
            if not existing_ign:
                modal = IGNRequiredModal(self, game.game_id, game.name)
                await interaction.response.send_modal(modal)
                return

        # Check role requirement before deferring
        if game.role_required:
            existing_prefs = await DatabaseHelper.get_player_role_prefs(user.id, game.game_id)
            if not existing_prefs:
                view = RoleRequiredView(self, game.game_id, game.name)
                await interaction.response.send_message(
                    f"**You need to set your role to join {game.name} queue.**\n\n"
                    "Select your **primary role** below:",
                    view=view,
                    ephemeral=True
                )
                return

        # Defer early to prevent timeout - we'll do DB operations next
        await interaction.response.defer()

        try:
            # Check if schedule says queue should be closed
            if game.schedule_enabled and not self._is_currently_open(game):
                next_open_ts = self._calculate_next_open_time(game, datetime.now())
                await interaction.followup.send(
                    f"Queue is currently closed.\n\n**Queue opens:**\n<t:{next_open_ts}:F>\n\n<t:{next_open_ts}:R>",
                    ephemeral=True
                )
                return

            # Check MMR role alignment and auto-correct for new players
            role_mmr_map = await DatabaseHelper.get_mmr_roles(game_id)
            if role_mmr_map:
                stats = await DatabaseHelper.get_player_stats(user.id, game_id)
                # Find highest MMR role the user has
                expected_mmr = None
                for role in user.roles:
                    if role.id in role_mmr_map:
                        role_val = role_mmr_map[role.id]
                        if expected_mmr is None or role_val > expected_mmr:
                            expected_mmr = role_val

                # Only auto-correct for truly new players (no existing stats row).
                # stats.is_new=True means get_player_stats found no row at all.
                # This correctly ignores veterans after a season wipe whose
                # games_played was reset to 0 but whose stats row still exists.
                if expected_mmr is not None and stats.mmr != expected_mmr and stats.is_new:
                    old_mmr = stats.mmr
                    stats.mmr = expected_mmr
                    await DatabaseHelper.update_player_stats(stats)
                    log_channel_id = await DatabaseHelper.get_config("log_channel_id")
                    if log_channel_id:
                        log_ch = interaction.guild.get_channel(int(log_channel_id))
                        if log_ch:
                            try:
                                await log_ch.send(
                                    f"MMR auto-corrected for <@{user.id}>: {old_mmr} → {expected_mmr} "
                                    f"(role mismatch detected on queue join)"
                                )
                            except Exception as e:
                                logger.warning(f"Failed to send MMR correction log: {e}")

            # Batch all DB checks in a single connection
            async with DatabaseHelper._get_db() as db:
                # Check blacklist
                async with db.execute(
                    "SELECT blacklisted_until FROM players WHERE player_id = ?",
                    (user.id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        until = datetime.fromisoformat(row[0])
                        if until > datetime.now(timezone.utc):
                            await interaction.followup.send("You are blacklisted from queues.", ephemeral=True)
                            return

                # Check suspension
                async with db.execute(
                    """SELECT suspended_until, reason FROM suspensions
                       WHERE player_id = ? AND (game_id = ? OR game_id IS NULL)
                       AND datetime(suspended_until) > datetime('now')
                       ORDER BY suspended_until DESC LIMIT 1""",
                    (user.id, game_id)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        until = datetime.fromisoformat(row[0]).strftime("%Y-%m-%d %H:%M UTC")
                        reason = row[1] or "No reason provided"
                        await interaction.followup.send(
                            f"You are suspended from this game until {until}.\nReason: {reason}",
                            ephemeral=True
                        )
                        return

                # Check ready penalty (timeout)
                async with db.execute(
                    "SELECT penalty_expires FROM ready_penalties WHERE player_id = ?",
                    (user.id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        penalty_expires = datetime.fromisoformat(row[0])
                        if penalty_expires > datetime.now(timezone.utc):
                            until = penalty_expires.strftime("%Y-%m-%d %H:%M UTC")
                            await interaction.followup.send(
                                f"You are penalized for missing a ready check.\nPenalty expires: {until}",
                                ephemeral=True
                            )
                            return

                # Check decline penalty (Not Ready click)
                async with db.execute(
                    "SELECT penalty_expires FROM decline_penalties WHERE player_id = ?",
                    (user.id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        penalty_expires = datetime.fromisoformat(row[0])
                        if penalty_expires > datetime.now(timezone.utc):
                            until = penalty_expires.strftime("%Y-%m-%d %H:%M UTC")
                            await interaction.followup.send(
                                f"You are penalized for declining a ready check.\nPenalty expires: {until}",
                                ephemeral=True
                            )
                            return

                # Check if in active match
                async with db.execute(
                    """SELECT m.match_id FROM matches m
                       JOIN match_players mp ON m.match_id = mp.match_id
                       WHERE mp.player_id = ? AND m.game_id = ?
                       AND m.winning_team IS NULL AND m.cancelled = 0""",
                    (user.id, game_id)
                ) as cursor:
                    if await cursor.fetchone():
                        await interaction.followup.send(
                            "You're already in an active match for this game.",
                            ephemeral=True
                        )
                        return

                # Clean up orphaned queue entries for this user (same game only)
                await db.execute(
                    """DELETE FROM queue_players WHERE player_id = ? AND queue_id != ?
                       AND queue_id IN (SELECT queue_id FROM active_queues WHERE game_id = ?)""",
                    (user.id, queue_id, game_id)
                )

                # Add to queue in DB
                await db.execute(
                    "INSERT OR IGNORE INTO queue_players (queue_id, player_id) VALUES (?, ?)",
                    (queue_id, user.id)
                )
                # Remove subscription if they join the queue themselves
                await db.execute(
                    "DELETE FROM queue_subscribers WHERE queue_id = ? AND player_id = ?",
                    (queue_id, user.id)
                )
                await db.commit()

            # Lock to prevent race condition when multiple players join simultaneously.
            # CRITICAL: Re-check state inside the lock. start_ready_check() can set
            # state to "ready_check" between the fast-path check above (line 10937)
            # and here, because the defer + DB operations above yield to the event loop.
            # Without this re-check a 10th-and-11th concurrent joiner both pass the
            # outer guard then add themselves, producing 11 players in a 10-player queue
            # and triggering _restore_queue_to_waiting which wipes the whole ready check.
            queue_full = False
            join_rejected_in_lock = False
            join_rejected_msg = "Queue is no longer accepting players."
            async with self.queue_locks.setdefault(queue_id, asyncio.Lock()):
                if queue_state.state != "waiting":
                    join_rejected_in_lock = True
                else:
                    # Check if player is already in a waiting queue for a different game (A2)
                    for qid, qs in self.queues.items():
                        if qid != queue_id and qs.game_id != game_id \
                                and user.id in qs.players and qs.state == "waiting":
                            join_rejected_in_lock = True
                            join_rejected_msg = "You're already in a queue for another game."
                            break

                if not join_rejected_in_lock:
                    # Remove from other waiting queues for different games.
                    # Allow coexistence in main + secondary queue for the same game.
                    for qid, qs in list(self.queues.items()):
                        if qid != queue_id and user.id in qs.players and qs.state == "waiting":
                            # Skip same-game queues where one is secondary and the other isn't
                            if qs.game_id == game_id and qs.is_secondary != queue_state.is_secondary:
                                continue
                            del qs.players[user.id]
                            qs.grace_timers.pop(user.id, None)

                    # Add to in-memory queue
                    queue_state.players[user.id] = False  # Not ready yet
                    queue_state.grace_timers[user.id] = datetime.now(timezone.utc)

                    # Check if queue is full (must be checked atomically with add)
                    effective_pc = self._get_effective_player_count(game, queue_state)
                    queue_full = len(queue_state.players) >= effective_pc

            if join_rejected_in_lock:
                # Clean up the DB entry we inserted before acquiring the lock —
                # the player was rejected so they should not persist in the DB.
                async with DatabaseHelper._get_db() as db:
                    await db.execute(
                        "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                        (queue_id, user.id)
                    )
                    await db.commit()
                await interaction.followup.send(join_rejected_msg, ephemeral=True)
                return

            # Update embed using the message directly (since we deferred)
            try:
                embed = await self.create_queue_embed(game, queue_state, interaction.guild)
                msg = await interaction.channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating queue embed: {e}")

            # Notify queue subscribers when slots remaining <= their threshold
            if not queue_full:
                remaining = effective_pc - len(queue_state.players)
                await self._notify_queue_subscribers(
                    interaction.guild, queue_id, game, remaining, queue_state
                )

            # LF1 notification: send when queue is 1 player away from filling
            if not queue_full and len(queue_state.players) == effective_pc - 1:
                await self._send_lf1_notification(game, queue_state, interaction.guild)

            # Start ready check when queue fills
            if queue_full:
                # Clean up LF1 notification since queue is now full
                await self._delete_lf1_message(game.game_id)
                await self.start_ready_check(interaction.channel, game, queue_state)

        except Exception as e:
            logger.error(f"Error in handle_queue_join: {e}", exc_info=True)
            try:
                await interaction.followup.send(
                    "An error occurred while joining the queue. Please try again.",
                    ephemeral=True
                )
            except Exception:
                pass
    
    async def handle_queue_leave(self, interaction: discord.Interaction, game_id: int, queue_id: int):
        """Handle a player leaving the queue."""
        user = interaction.user

        # Quick in-memory checks first
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return

        queue_state = self.queues[queue_id]

        if user.id not in queue_state.players:
            await interaction.response.send_message("You're not in this queue.", ephemeral=True)
            return

        if queue_state.state != "waiting":
            await interaction.response.send_message(
                "Use the Not Ready button during ready check.",
                ephemeral=True
            )
            return

        # Defer before DB operations
        await interaction.response.defer()

        try:
            game = await DatabaseHelper.get_game(game_id)

            # Acquire lock first, then re-check state before removing the player.
            # The initial state check above is a fast-path guard, but start_ready_check
            # can change state to "ready_check" during the defer without holding this lock.
            # Re-checking inside the lock closes that race window (the EK16A bug).
            async with self.queue_locks.setdefault(queue_id, asyncio.Lock()):
                if queue_state.state != "waiting":
                    # State changed while we were deferring — queue is in ready check.
                    return
                if user.id not in queue_state.players:
                    return
                del queue_state.players[user.id]
                queue_state.grace_timers.pop(user.id, None)

            # DB cleanup after in-memory update
            async with DatabaseHelper._get_db() as db:
                await db.execute(
                    "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                    (queue_id, user.id)
                )
                await db.commit()

            # Also remove from DM subscriber list
            await DatabaseHelper.unsubscribe_from_queue(queue_id, user.id)

            # If queue dropped below LF1 threshold, delete the "need one more" message
            effective_pc = self._get_effective_player_count(game, queue_state) if game else 0
            if game and len(queue_state.players) < effective_pc - 1:
                await self._delete_lf1_message(game.game_id)

            # Update embed using direct message edit (since we deferred)
            embed = await self.create_queue_embed(game, queue_state, interaction.guild)
            try:
                msg = await interaction.channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating queue embed on leave: {e}")

        except Exception as e:
            logger.error(f"Error in handle_queue_leave: {e}", exc_info=True)
            try:
                await interaction.followup.send("An error occurred. Please try again.", ephemeral=True)
            except Exception:
                pass
    
    async def start_ready_check(self, channel: discord.TextChannel, game: GameConfig, 
                                 queue_state: QueueState):
        """Start the ready check phase."""
        # Guard: verify player count is still correct before committing to ready-check.
        # A concurrent start_ready_check from another game's queue may have removed
        # one of our players in the window between queue_full being set and this call.
        effective_pc = self._get_effective_player_count(game, queue_state)
        if len(queue_state.players) != effective_pc:
            logger.warning(
                f"start_ready_check: player count mismatch "
                f"({len(queue_state.players)} vs expected {effective_pc}) "
                f"for queue {queue_state.queue_id} — aborting, restoring to waiting"
            )
            await self._restore_queue_to_waiting(channel, game, queue_state)
            return

        # Acquire ready_check_lock for the state transition to prevent races
        # with concurrent handle_ready or timeout tasks.
        async with self.ready_check_lock:
            if queue_state.state != "waiting":
                # Another coroutine already transitioned this queue
                logger.warning(f"start_ready_check: state is {queue_state.state}, expected waiting — aborting")
                return
            queue_state.state = "ready_check"
            queue_state.ready_check_started = datetime.now(timezone.utc)

        # Clear queue subscribers since queue is now full
        await DatabaseHelper.clear_queue_subscribers(queue_state.queue_id)

        # Remove players from ALL other queues (waiting or ready_check).
        # If the other queue is in ready_check, revert it to waiting state.
        for pid in list(queue_state.players.keys()):
            for qid, qs in list(self.queues.items()):
                if qid != queue_state.queue_id and pid in qs.players:
                    async with self.queue_locks.setdefault(qid, asyncio.Lock()):
                        if pid not in qs.players:
                            continue
                        if qs.state == "waiting":
                            del qs.players[pid]
                            qs.grace_timers.pop(pid, None)
                        elif qs.state == "ready_check":
                            # Remove player and revert queue to waiting
                            del qs.players[pid]
                            qs.grace_timers.pop(pid, None)
                            qs.state = "waiting"
                            qs.ready_check_started = None
                            qs.auto_readied.discard(pid)
                            # Reset all remaining players' ready status
                            for remaining_pid in qs.players:
                                qs.players[remaining_pid] = False
                            # Cancel the ready check timeout task
                            if qid in self.ready_check_tasks:
                                self.ready_check_tasks[qid].cancel()
                                del self.ready_check_tasks[qid]
                            # Update DB state
                            async with DatabaseHelper._get_db() as db:
                                await db.execute(
                                    "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                                    (qid, pid)
                                )
                                await db.execute(
                                    "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                                    (qid,)
                                )
                                await db.commit()
                        else:
                            continue  # Don't touch starting_match or in_match

                    # Clean up LF1 message if the other queue dropped below threshold
                    other_game = await DatabaseHelper.get_game(qs.game_id)
                    if other_game and other_game.game_id in self.lf1_messages:
                        other_epc = self._get_effective_player_count(other_game, qs)
                        if len(qs.players) < other_epc - 1:
                            await self._delete_lf1_message(other_game.game_id)

                    # Update the other queue's embed
                    if other_game and qs.message_id:
                        try:
                            other_channel = channel.guild.get_channel(qs.channel_id)
                            if other_channel:
                                msg = await other_channel.fetch_message(qs.message_id)
                                if qs.state == "waiting":
                                    embed = await self.create_queue_embed(other_game, qs, channel.guild)
                                    view = QueueView(self, other_game.game_id, qid)
                                    await msg.edit(embed=embed, view=view)
                                    # Notify the channel
                                    member = channel.guild.get_member(pid)
                                    name = member.display_name if member else f"User {pid}"
                                    await other_channel.send(
                                        embed=discord.Embed(
                                            description=f"**{name}** was pulled into another match. Ready check cancelled — queue reset.",
                                            color=COLOR_WARNING
                                        ),
                                        delete_after=60
                                    )
                        except Exception:
                            pass
        
        # Update database
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE active_queues SET state = 'ready_check', ready_check_started = ? WHERE queue_id = ?",
                (queue_state.ready_check_started.isoformat(), queue_state.queue_id)
            )
            await db.commit()
        
        # Auto-ready players within their grace period (BEFORE creating embed
        # so the embed reflects the correct ready state from the start)
        auto_readied = []
        queue_state.auto_readied = set()
        for pid in list(queue_state.players.keys()):
            if self._player_has_grace(queue_state, pid, game):
                queue_state.players[pid] = True
                queue_state.auto_readied.add(pid)
                auto_readied.append(pid)

        if auto_readied:
            logger.info(
                f"Queue {queue_state.queue_id}: auto-readied {len(auto_readied)} player(s) "
                f"via grace period: {auto_readied}"
            )

        # If ALL players are auto-readied, skip ready check entirely.
        # This check must be inside ready_check_lock to prevent a concurrent
        # handle_ready (Not Ready) from racing between state="ready_check" above
        # and this state="starting_match" transition.
        if all(queue_state.players.values()):
            async with self.ready_check_lock:
                if queue_state.state != "ready_check":
                    # Another coroutine already transitioned state
                    logger.warning(f"start_ready_check: all-auto-ready race — state is {queue_state.state}")
                    return
                queue_state.state = "starting_match"
            async with DatabaseHelper._get_db() as db:
                await db.execute(
                    "UPDATE active_queues SET state = 'starting_match' WHERE queue_id = ?",
                    (queue_state.queue_id,)
                )
                await db.commit()
            logger.info(f"All players auto-readied via grace period for queue {queue_state.queue_id}")
            try:
                await channel.send(
                    "All players have an active grace period — skipping ready check. Starting match...",
                    delete_after=30
                )
            except Exception:
                pass
            await self.proceed_to_match(channel, game, queue_state)
            return

        # Create embed AFTER auto-ready so it reflects correct ready states
        embed = await self.create_ready_check_embed(game, queue_state, game.ready_timer_seconds, channel.guild)
        view = ReadyCheckView(self, game.game_id, queue_state.queue_id)

        # Edit message with ready check view (with retry + fallback)
        view_updated = False
        for attempt in range(2):
            try:
                msg = await channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed, view=view)
                view_updated = True
                break
            except Exception as e:
                logger.error(f"Error updating ready check embed (attempt {attempt + 1}): {e}")
                if attempt == 0:
                    await asyncio.sleep(1)

        if not view_updated:
            # Fallback: send a new message with the ReadyCheckView
            try:
                new_msg = await channel.send(embed=embed, view=view)
                queue_state.message_id = new_msg.id
                async with DatabaseHelper._get_db() as db:
                    await db.execute(
                        "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                        (new_msg.id, queue_state.queue_id)
                    )
                    await db.commit()
                logger.info(f"Sent new ready check message for queue {queue_state.queue_id}")
            except Exception as e:
                logger.error(f"Failed to send fallback ready check message: {e}")

        # Ping only players who are NOT auto-readied
        non_grace_players = [pid for pid in queue_state.players if not queue_state.players[pid]]
        if non_grace_players:
            mentions = " ".join([f"<@{pid}>" for pid in non_grace_players])
            try:
                ping_msg = await channel.send(f"Ready check! {mentions}")
                await asyncio.sleep(3)
                await ping_msg.delete()
            except Exception as e:
                logger.warning(f"Error with ready check ping: {e}")

        # DM only non-auto-readied players if enabled (with rate limiting)
        if game.dm_ready_up:
            for pid in non_grace_players:
                try:
                    user_obj = self.bot.get_user(pid) or await self.bot.fetch_user(pid)
                    await user_obj.send(
                        f"**{game.name}** queue is ready!\n"
                        f"Click the Ready button in {channel.mention} within {game.ready_timer_seconds} seconds."
                    )
                    await asyncio.sleep(0.5)  # Rate limit DMs
                except Exception as e:
                    logger.debug(f"Could not DM player {pid} for ready check: {e}")

        # Start timeout task
        task = asyncio.create_task(
            self.ready_check_timeout(channel, game, queue_state)
        )
        self.ready_check_tasks[queue_state.queue_id] = task
    
    async def ready_check_timeout(self, channel: discord.TextChannel, game: GameConfig,
                                   queue_state: QueueState, initial_timer: int = None):
        """Handle ready check timeout.

        initial_timer can be supplied when restarting after a failed proceed_to_match
        so the countdown continues from where it left off rather than resetting.
        """
        timer = initial_timer if initial_timer is not None else game.ready_timer_seconds

        while timer > 0:
            await asyncio.sleep(5)
            timer -= 5

            # Check if all ready — read players dict inside lock to avoid stale reads
            if queue_state.state != "ready_check":
                return

            async with self.ready_check_lock:
                if queue_state.state != "ready_check":
                    return  # handle_ready beat us to it
                if all(queue_state.players.values()):
                    queue_state.state = "starting_match"
                    # Persist starting_match to DB (Bug 7)
                    async with DatabaseHelper._get_db() as db:
                        await db.execute(
                            "UPDATE active_queues SET state = 'starting_match' WHERE queue_id = ?",
                            (queue_state.queue_id,)
                        )
                        await db.commit()
                    all_ready = True
                else:
                    all_ready = False

            if all_ready:
                await self.proceed_to_match(channel, game, queue_state)
                return

            # Update embed with new time
            try:
                msg = await channel.fetch_message(queue_state.message_id)
                embed = await self.create_ready_check_embed(game, queue_state, timer, channel.guild)
                await msg.edit(embed=embed)
            except Exception:
                pass

        # Time's up — atomically claim the "waiting" transition so we don't race
        # with a concurrent handle_ready that may have just seen all-ready.
        async with self.ready_check_lock:
            if queue_state.state != "ready_check":
                return  # handle_ready or timeout from another path already handled it
            queue_state.state = "waiting"  # block any concurrent handle_ready

        # Remove unready players and apply penalties
        unready = [pid for pid, ready in queue_state.players.items() if not ready]

        # Apply penalties to unready players
        penalty_messages = []
        for pid in unready:
            del queue_state.players[pid]
            queue_state.grace_timers.pop(pid, None)
            # Apply penalty
            offense_count, penalty_expires = await DatabaseHelper.add_ready_penalty_offense(pid, game)
            penalty_messages.append(f"<@{pid}> (Offense #{offense_count})")
            member = channel.guild.get_member(pid)
            member_name = member.display_name if member else str(pid)
            expires_str = f"<t:{int(penalty_expires.timestamp())}:F>" if penalty_expires else "N/A"
            await self.log_action(
                channel.guild,
                f"Ready-up penalty applied to **{member_name}** ({game.name}): Offense #{offense_count}, expires {expires_str}"
            )

            # DM the player if DM is enabled
            if game.dm_ready_up:
                try:
                    user_obj = self.bot.get_user(pid) or await self.bot.fetch_user(pid)
                    await user_obj.send(
                        f"You missed the ready check for **{game.name}**.\n"
                        f"This is offense #{offense_count}. You are penalized until "
                        f"{penalty_expires.strftime('%Y-%m-%d %H:%M UTC')}."
                    )
                except Exception as e:
                    logger.debug(f"Could not DM player {pid} about penalty: {e}")

        # Reset all remaining players' ready status
        for pid in queue_state.players:
            queue_state.players[pid] = False

        embed = await self.create_queue_embed(game, queue_state, channel.guild)
        view = QueueView(self, game.game_id, queue_state.queue_id)

        try:
            msg = await channel.fetch_message(queue_state.message_id)
            await msg.edit(embed=embed, view=view)

            if penalty_messages:
                not_ready_embed = discord.Embed(
                    title="Players Not Ready",
                    description="The following players did not ready up in time:\n" + "\n".join(penalty_messages),
                    color=COLOR_WARNING
                )
                await channel.send(embed=not_ready_embed, delete_after=60)
        except Exception as e:
            logger.error(f"Error updating queue after ready timeout: {e}")

    async def handle_ready(self, interaction: discord.Interaction, queue_id: int, is_ready: bool):
        """Handle ready/not ready button."""
        user = interaction.user
        
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return
        
        queue_state = self.queues[queue_id]
        
        if user.id not in queue_state.players:
            await interaction.response.send_message("You're not in this queue.", ephemeral=True)
            return
        
        if queue_state.state != "ready_check":
            await interaction.response.send_message("Ready check not active.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(queue_state.game_id)
        if not game:
            await interaction.response.send_message("Game no longer exists.", ephemeral=True)
            return

        if is_ready:
            # Mark ready and check for full consensus inside the lock so concurrent
            # clicks cannot both see all-ready and both call proceed_to_match.
            async with self.ready_check_lock:
                if queue_state.state != "ready_check":
                    # State changed between the outer check and acquiring the lock
                    await interaction.response.send_message("Ready check not active.", ephemeral=True)
                    return

                queue_state.players[user.id] = True
                # Player manually readied — remove from auto_readied set
                queue_state.auto_readied.discard(user.id)
                # Reset grace timer so it's fresh for subsequent queues
                queue_state.grace_timers[user.id] = datetime.now(timezone.utc)

                if all(queue_state.players.values()):
                    # Immediately set state to prevent concurrent triggers
                    queue_state.state = "starting_match"

                    # Cancel timeout task
                    if queue_id in self.ready_check_tasks:
                        self.ready_check_tasks[queue_id].cancel()
                        del self.ready_check_tasks[queue_id]

                    # Persist starting_match to DB so crash recovery knows about it
                    async with DatabaseHelper._get_db() as db:
                        await db.execute(
                            "UPDATE active_queues SET state = 'starting_match' WHERE queue_id = ?",
                            (queue_id,)
                        )
                        await db.commit()

                    await interaction.response.send_message("All players ready! Starting match...", ephemeral=True)
                    try:
                        await self.proceed_to_match(interaction.channel, game, queue_state)
                    except Exception as e:
                        logger.error(f"Error in proceed_to_match: {e}", exc_info=True)
                        # Restore queue state on failure and restart the countdown so
                        # players aren't stuck staring at a frozen ready-check embed.
                        queue_state.state = "ready_check"
                        elapsed = int((datetime.now(timezone.utc) - queue_state.ready_check_started).total_seconds())
                        remaining = max(10, game.ready_timer_seconds - elapsed)
                        restart_task = asyncio.create_task(
                            self.ready_check_timeout(interaction.channel, game, queue_state, initial_timer=remaining)
                        )
                        self.ready_check_tasks[queue_id] = restart_task
                        try:
                            await interaction.followup.send(
                                f"Error starting match: {str(e)[:100]}. Please try again.",
                                ephemeral=True
                            )
                        except Exception:
                            pass
                    return

            # Send ephemeral confirmation and update embed
            ready_count = sum(1 for r in queue_state.players.values() if r)
            total_count = len(queue_state.players)
            await interaction.response.send_message(
                f"You're ready! ({ready_count}/{total_count})",
                ephemeral=True
            )

            # Update embed in background
            elapsed = (datetime.now(timezone.utc) - queue_state.ready_check_started).seconds
            remaining = max(0, game.ready_timer_seconds - elapsed)
            embed = await self.create_ready_check_embed(game, queue_state, remaining, interaction.guild)
            try:
                msg = await interaction.channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating ready check embed: {e}")
        else:
            # Not ready - remove from queue, revert to waiting
            # Defer IMMEDIATELY to avoid the 3-second interaction token expiry
            # (root cause of the duplicate-queue bug).
            await interaction.response.defer()

            try:
                # Acquire lock so we don't race with timeout task or concurrent handle_ready
                all_others_ready = False
                async with self.ready_check_lock:
                    if queue_state.state != "ready_check":
                        # Another path already transitioned state
                        return

                    # Cancel timeout task INSIDE lock before state change
                    if queue_id in self.ready_check_tasks:
                        self.ready_check_tasks[queue_id].cancel()
                        del self.ready_check_tasks[queue_id]

                    # In-memory changes FIRST (while lock is held)
                    if user.id in queue_state.players:
                        del queue_state.players[user.id]
                    queue_state.grace_timers.pop(user.id, None)

                    queue_state.state = "waiting"
                    queue_state.ready_check_started = None
                    for pid in queue_state.players:
                        queue_state.players[pid] = False

                # DB update AFTER in-memory is consistent (outside lock to avoid holding it during I/O)
                async with DatabaseHelper._get_db() as db:
                    await db.execute(
                        "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                        (queue_id, user.id)
                    )
                    await db.execute(
                        "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                        (queue_id,)
                    )
                    await db.commit()

                # Apply decline penalty (scaling with offenses)
                offense_count, penalty_expires = await DatabaseHelper.add_decline_penalty_offense(user.id, game)
                self.not_ready_cooldowns[user.id] = penalty_expires
                expires_str = f"<t:{int(penalty_expires.timestamp())}:R>"
                cooldown_msg = f" Decline penalty #{offense_count} applied — expires {expires_str}."
                member_name = user.display_name
                log_expires = f"<t:{int(penalty_expires.timestamp())}:F>"
                await self.log_action(
                    interaction.guild,
                    f"Decline penalty applied to **{member_name}** ({game.name}): "
                    f"Offense #{offense_count}, expires {log_expires}"
                )
                if game.dm_ready_up:
                    try:
                        await user.send(
                            f"You declined the ready check for **{game.name}**.\n"
                            f"This is offense #{offense_count}. You are penalized until "
                            f"{penalty_expires.strftime('%Y-%m-%d %H:%M UTC')}."
                        )
                    except Exception:
                        pass

                # Update the embed via fetch+edit (interaction was deferred, can't use response.edit_message)
                embed = await self.create_queue_embed(game, queue_state, interaction.guild)
                view = QueueView(self, game.game_id, queue_state.queue_id)
                try:
                    msg = await interaction.channel.fetch_message(queue_state.message_id)
                    await msg.edit(embed=embed, view=view)
                except discord.NotFound:
                    # Message gone — send a fresh embed and update message_id
                    new_msg = await interaction.channel.send(embed=embed, view=view)
                    queue_state.message_id = new_msg.id
                    async with DatabaseHelper._get_db() as db:
                        await db.execute(
                            "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                            (new_msg.id, queue_state.queue_id)
                        )
                        await db.commit()

                # Send notification embed about who wasn't ready (auto-deletes after 60s)
                not_ready_embed = discord.Embed(
                    description=f"<@{user.id}> was not ready. Queue reset.{cooldown_msg}",
                    color=COLOR_WARNING
                )
                await interaction.channel.send(embed=not_ready_embed, delete_after=60)

            except Exception as e:
                logger.error(f"Error handling not ready: {e}", exc_info=True)
                try:
                    await interaction.followup.send(
                        "An error occurred. Please try again.",
                        ephemeral=True
                    )
                except Exception:
                    pass
    
    # -------------------------------------------------------------------------
    # MATCH CREATION
    # -------------------------------------------------------------------------
    
    async def proceed_to_match(self, channel: discord.TextChannel, game: GameConfig,
                                queue_state: QueueState):
        """Proceed from ready check to match creation."""
        # Verify we have the lock on match creation (state must be "starting_match")
        if queue_state.state != "starting_match":
            logger.warning(f"proceed_to_match called but state is {queue_state.state}, aborting")
            return

        # Immediately set to in_match to prevent any other calls
        queue_state.state = "in_match"

        guild = channel.guild
        player_ids = list(queue_state.players.keys())
        effective_pc = self._get_effective_player_count(game, queue_state)
        effective_qt = self._get_effective_queue_type(game, queue_state)

        if len(player_ids) != effective_pc:
            logger.error(
                f"proceed_to_match: player count mismatch "
                f"({len(player_ids)} vs expected {effective_pc}) for queue {queue_state.queue_id}"
            )
            await channel.send(
                f"⚠️ Match cancelled: player count mismatch "
                f"({len(player_ids)}/{effective_pc}). Queue restored.",
                delete_after=30
            )
            await self._restore_queue_to_waiting(channel, game, queue_state)
            return

        # Get category — use per-game override if set, fall back to global config
        if game.category_id:
            category_id = str(game.category_id)
        else:
            category_id = await DatabaseHelper.get_config("category_id")
        category = guild.get_channel(int(category_id)) if category_id else None

        if not category:
            await channel.send("Error: Category not configured. Contact an admin.")
            # Restore queue to waiting state instead of leaving it broken
            await self._restore_queue_to_waiting(channel, game, queue_state)
            return

        # Generate a short ID for this match
        match_short_id = generate_short_id()
        match_id = None
        red_role = None
        blue_role = None

        try:
            # Create match in database
            match_id = await DatabaseHelper.create_match(
                game.game_id,
                effective_qt.value,
                queue_state.message_id,
                short_id=match_short_id,
                is_secondary=queue_state.is_secondary,
            )

            # Create team roles using the match short_id (mentionable so players can ping their team)
            red_role = await guild.create_role(name=f"Red {match_short_id}", color=discord.Color.red(), mentionable=True)
            blue_role = await guild.create_role(name=f"Blue {match_short_id}", color=discord.Color.blue(), mentionable=True)

            await DatabaseHelper.update_match(
                match_id,
                red_role_id=red_role.id,
                blue_role_id=blue_role.id
            )

            # Route based on queue type - this creates the match channel
            if effective_qt == QueueType.CAPTAINS:
                await self.start_captain_draft(guild, category, game, match_id, player_ids, red_role, blue_role)
            else:
                # MMR or Random - balance teams and create match channel
                if effective_qt == QueueType.MMR:
                    red_team, blue_team = await self.balance_teams_mmr(player_ids, game.game_id)
                else:
                    random.shuffle(player_ids)
                    mid = len(player_ids) // 2
                    red_team = player_ids[:mid]
                    blue_team = player_ids[mid:]

                await self.create_match_channel(guild, category, game, match_id,
                                                 red_team, blue_team, red_role, blue_role)

            # Match creation succeeded - clean up old queue and start new one.
            # Start new queue FIRST so there's always a queue available, then clean up old.
            old_queue_id = queue_state.queue_id
            old_message_id = queue_state.message_id

            # Remove old queue from memory immediately to prevent duplicate state
            if old_queue_id in self.queues:
                del self.queues[old_queue_id]
                self.queue_locks.pop(old_queue_id, None)

            # Start a new queue (same type as the one that just started a match)
            try:
                await self.start_queue(channel, game, is_secondary=queue_state.is_secondary)
            except Exception as e:
                logger.error(f"proceed_to_match: failed to start new queue: {e}", exc_info=True)
                # Match was already created, so just log — don't crash

            # Delete the old queue message (or tombstone it if delete fails)
            try:
                msg = await channel.fetch_message(old_message_id)
                await msg.delete()
            except discord.NotFound:
                pass  # Already gone
            except Exception:
                # Can't delete — disable buttons with a tombstone edit
                try:
                    msg = await channel.fetch_message(old_message_id)
                    tombstone = discord.Embed(
                        description="This queue has ended. A new queue has been started.",
                        color=discord.Color.greyple()
                    )
                    await msg.edit(embed=tombstone, view=None)
                except Exception:
                    pass

            # Clean up DB
            async with DatabaseHelper._get_db() as db:
                await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (old_queue_id,))
                await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (old_queue_id,))
                await db.commit()

        except Exception as e:
            logger.error(f"Error creating match: {e}", exc_info=True)

            # Clean up partial match creation
            if match_id:
                await DatabaseHelper.update_match(
                    match_id, cancelled=1, ended_at=datetime.now(timezone.utc).isoformat()
                )
            if red_role:
                try:
                    await red_role.delete()
                except Exception:
                    pass
            if blue_role:
                try:
                    await blue_role.delete()
                except Exception:
                    pass

            # Notify users and restore queue
            await channel.send(
                f"Error creating match: {str(e)[:100]}. Queue has been restored.",
                delete_after=15
            )
            await self._restore_queue_to_waiting(channel, game, queue_state)

    async def _restore_queue_to_waiting(self, channel: discord.TextChannel, game: GameConfig,
                                         queue_state: QueueState):
        """Restore a queue to waiting state after a failed match creation."""
        qid = queue_state.queue_id

        # Cancel any lingering tasks that might interfere after recovery
        if qid in self.ready_check_tasks:
            self.ready_check_tasks[qid].cancel()
            del self.ready_check_tasks[qid]
        queue_state.state = "waiting"
        queue_state.ready_check_started = None
        for pid in queue_state.players:
            queue_state.players[pid] = False

        # Update database
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                (qid,)
            )
            await db.commit()

        # Update the embed — if message is gone, send a new one
        try:
            if queue_state.message_id:
                msg = await channel.fetch_message(queue_state.message_id)
                embed = await self.create_queue_embed(game, queue_state, channel.guild)
                view = QueueView(self, game.game_id, qid)
                await msg.edit(embed=embed, view=view)
        except discord.NotFound:
            # Message was deleted — send a fresh embed
            try:
                embed = await self.create_queue_embed(game, queue_state, channel.guild)
                view = QueueView(self, game.game_id, qid)
                new_msg = await channel.send(embed=embed, view=view)
                queue_state.message_id = new_msg.id
                async with DatabaseHelper._get_db() as db:
                    await db.execute(
                        "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                        (new_msg.id, qid)
                    )
                    await db.commit()
            except Exception as e:
                logger.error(f"Error sending replacement queue embed: {e}")
        except Exception as e:
            logger.error(f"Error restoring queue embed: {e}")
    
    async def balance_teams_mmr(
        self, player_ids: List[int], game_id: int,
        force_shuffle_from: Optional[Dict[int, str]] = None,
        min_swap_pct: float = 0.0,
    ) -> Tuple[List[int], List[int]]:
        """Balance teams by enumerating all splits and picking among the
        best-balanced candidates with role-diversity + anti-repeat tiebreakers,
        then randomizing within the remaining set so back-to-back queues of
        the same roster don't replay the identical match.

        Priority order (hard tiers):
          1. Minimum MMR diff (within SHAKE_MMR_TOLERANCE of the best).
          2. Minimum role-diversity penalty.
          3. Minimum "repeat" penalty (players on the same team as last time).
             When force_shuffle_from is set, this is *maximized* instead.
          4. Random choice among survivors.

        When force_shuffle_from is provided (a dict of player_id -> 'red'/'blue'),
        candidates with fewer than min_swap_pct fraction of players changing
        teams are filtered out, and Tier 3 maximizes change instead of
        minimizing repeats.

        For odd-sized or very small rosters, falls back to the legacy
        snake-draft balancer.
        """
        # Get all player stats
        players_with_mmr = []
        for pid in player_ids:
            stats = await DatabaseHelper.get_player_stats(pid, game_id)
            players_with_mmr.append((pid, stats.effective_mmr))

        n = len(players_with_mmr)
        if n < 2:
            return ([pid for pid, _ in players_with_mmr], [])

        # Fetch role preferences for role-aware balancing
        role_prefs = await DatabaseHelper.get_bulk_role_prefs(player_ids, game_id)

        # Odd rosters or very small queues fall back to legacy snake draft
        if n % 2 != 0 or n < 4:
            return self._balance_teams_legacy(players_with_mmr, role_prefs)

        team_size = n // 2
        pid_list = [pid for pid, _ in players_with_mmr]
        mmr_map = {pid: mmr for pid, mmr in players_with_mmr}

        # Look up the previous team assignment for a similar roster
        prev_map = await DatabaseHelper.get_previous_team_assignment(
            pid_list,
            game_id,
            overlap_threshold=SHAKE_OVERLAP_THRESHOLD,
            lookback=SHAKE_LOOKBACK_MATCHES,
        )

        # Step 1: Identify top 2 MMR players — they must ALWAYS be separated
        sorted_by_mmr = sorted(pid_list, key=lambda p: mmr_map[p], reverse=True)
        top1, top2 = sorted_by_mmr[0], sorted_by_mmr[1]

        # Bottom-2 separation: same constraint for lowest-MMR pair (when n >= 6
        # and they don't overlap with top-2)
        bottom1, bottom2 = sorted_by_mmr[-1], sorted_by_mmr[-2]
        apply_bottom_sep = (
            n >= 6
            and bottom1 != top1 and bottom1 != top2
            and bottom2 != top1 and bottom2 != top2
        )

        pid_set = set(pid_list)
        candidates = []  # (mmr_diff, role_penalty, repeat_penalty, red_frozen)
        for red_group in itertools.combinations(pid_list, team_size):
            # Enforce top-2 separation: skip any split where both are on the same team
            red_set_quick = set(red_group)
            if top1 in red_set_quick and top2 in red_set_quick:
                continue
            if top1 not in red_set_quick and top2 not in red_set_quick:
                continue
            # Enforce bottom-2 separation
            if apply_bottom_sep:
                if bottom1 in red_set_quick and bottom2 in red_set_quick:
                    continue
                if bottom1 not in red_set_quick and bottom2 not in red_set_quick:
                    continue
            blue_list = [p for p in pid_list if p not in red_set_quick]
            red_sum = sum(mmr_map[p] for p in red_group)
            blue_sum = sum(mmr_map[p] for p in blue_list)
            mmr_diff = abs(red_sum - blue_sum)
            role_penalty = (
                _role_diversity_penalty(list(red_group), role_prefs)
                + _role_diversity_penalty(blue_list, role_prefs)
            )
            if prev_map:
                repeat_penalty = (
                    sum(1 for p in red_group if prev_map.get(p) == 'red')
                    + sum(1 for p in blue_list if prev_map.get(p) == 'blue')
                )
            else:
                repeat_penalty = 0
            candidates.append((mmr_diff, role_penalty, repeat_penalty, frozenset(red_group)))

        # Fallback: if bottom-2 constraint left no candidates, retry without it
        if not candidates and apply_bottom_sep:
            logger.warning(
                f"balance_teams_mmr: bottom-2 separation over-constrained for game {game_id}, "
                f"retrying without bottom-2 constraint"
            )
            for red_group in itertools.combinations(pid_list, team_size):
                red_set_quick = set(red_group)
                if top1 in red_set_quick and top2 in red_set_quick:
                    continue
                if top1 not in red_set_quick and top2 not in red_set_quick:
                    continue
                blue_list = [p for p in pid_list if p not in red_set_quick]
                red_sum = sum(mmr_map[p] for p in red_group)
                blue_sum = sum(mmr_map[p] for p in blue_list)
                mmr_diff = abs(red_sum - blue_sum)
                role_penalty = (
                    _role_diversity_penalty(list(red_group), role_prefs)
                    + _role_diversity_penalty(blue_list, role_prefs)
                )
                if prev_map:
                    repeat_penalty = (
                        sum(1 for p in red_group if prev_map.get(p) == 'red')
                        + sum(1 for p in blue_list if prev_map.get(p) == 'blue')
                    )
                else:
                    repeat_penalty = 0
                candidates.append((mmr_diff, role_penalty, repeat_penalty, frozenset(red_group)))

        # Filter by minimum swap percentage when force-shuffling
        if force_shuffle_from and min_swap_pct > 0:
            def _swap_pct(cand):
                red_frozen = cand[3]
                changed = sum(
                    1 for p in pid_list
                    if (p in red_frozen) != (force_shuffle_from.get(p) == 'red')
                )
                return changed / len(pid_list)
            filtered = [c for c in candidates if _swap_pct(c) >= min_swap_pct]
            if filtered:
                candidates = filtered

        # Tier 1: within SHAKE_MMR_TOLERANCE of best MMR diff
        best_mmr = min(c[0] for c in candidates)
        tier1 = [c for c in candidates if c[0] <= best_mmr + SHAKE_MMR_TOLERANCE]
        # Tier 2: minimum role penalty among tier1
        best_role = min(c[1] for c in tier1)
        tier2 = [c for c in tier1 if c[1] == best_role]
        # Tier 3: repeat penalty — maximize change when force-shuffling, minimize otherwise
        if force_shuffle_from:
            best_repeat = max(c[2] for c in tier2)
        else:
            best_repeat = min(c[2] for c in tier2)
        tier3 = [c for c in tier2 if c[2] == best_repeat]

        logger.debug(
            f"balance_teams_mmr: {len(candidates)} total, "
            f"{len(tier1)} tier1, {len(tier2)} tier2, {len(tier3)} tier3, "
            f"best_mmr_diff={best_mmr}, best_repeat={best_repeat}"
        )

        chosen = random.choice(tier3)
        red_frozen = chosen[3]
        red_team = [p for p in pid_list if p in red_frozen]
        blue_team = [p for p in pid_list if p not in red_frozen]
        return red_team, blue_team

    def _balance_teams_legacy(
        self,
        players_with_mmr: List[Tuple[int, int]],
        role_prefs: Dict[int, Tuple[str, Optional[str]]],
    ) -> Tuple[List[int], List[int]]:
        """Snake-draft top 4 then combinatorial assignment of the rest.

        Preserved as the fallback for odd-sized rosters and queues with fewer
        than 4 players, which the full-enumeration path doesn't handle.
        """
        # Sort by MMR descending
        players_with_mmr = sorted(players_with_mmr, key=lambda x: x[1], reverse=True)

        top = players_with_mmr[:4]
        red_team = [top[0][0], top[3][0]] if len(top) > 3 else [top[0][0]]
        blue_team = [top[1][0], top[2][0]] if len(top) > 2 else [top[1][0]] if len(top) > 1 else []

        red_mmr = sum(mmr for pid, mmr in top if pid in red_team)
        blue_mmr = sum(mmr for pid, mmr in top if pid in blue_team)

        remaining = players_with_mmr[min(4, len(players_with_mmr)):]

        if remaining:
            team_size = len(players_with_mmr) // 2
            red_needed = max(0, team_size - len(red_team))

            best_score = (float('inf'), float('inf'))
            best_red_group = set()

            remaining_total_mmr = sum(mmr for _, mmr in remaining)
            remaining_pids = [pid for pid, _ in remaining]

            for red_group in itertools.combinations(remaining, red_needed):
                red_group_mmr = sum(mmr for _, mmr in red_group)
                red_total = red_mmr + red_group_mmr
                blue_total = blue_mmr + (remaining_total_mmr - red_group_mmr)
                mmr_diff = abs(red_total - blue_total)

                if mmr_diff <= best_score[0] + ROLE_MMR_TOLERANCE:
                    red_group_pids = set(pid for pid, _ in red_group)
                    full_red = red_team + [pid for pid, _ in red_group]
                    full_blue = blue_team + [pid for pid in remaining_pids if pid not in red_group_pids]
                    role_penalty = (
                        _role_diversity_penalty(full_red, role_prefs)
                        + _role_diversity_penalty(full_blue, role_prefs)
                    )
                    score = (mmr_diff, role_penalty)
                else:
                    score = (mmr_diff, 0)

                if score < best_score:
                    best_score = score
                    best_red_group = set(pid for pid, _ in red_group)

            for pid, _ in remaining:
                if pid in best_red_group:
                    red_team.append(pid)
                else:
                    blue_team.append(pid)

        return red_team, blue_team
    
    async def start_captain_draft(self, guild: discord.Guild, category: discord.CategoryChannel,
                                   game: GameConfig, match_id: int, player_ids: List[int],
                                   red_role: discord.Role, blue_role: discord.Role):
        """Start the captain draft phase."""
        # Create draft channel
        channel_name = await self.get_next_channel_suffix(guild, category, "draft-lobby")
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }
        
        # All players can view
        for pid in player_ids:
            member = guild.get_member(pid)
            if member:
                overwrites[member] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=False
                )
        
        draft_channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
        await DatabaseHelper.update_match(match_id, draft_channel_id=draft_channel.id)
        
        # Select captains based on method
        if game.captain_selection == CaptainSelection.HIGHEST_MMR:
            # Get two highest MMR players
            players_with_mmr = []
            for pid in player_ids:
                stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
                players_with_mmr.append((pid, stats.effective_mmr))
            players_with_mmr.sort(key=lambda x: x[1], reverse=True)
            red_captain = players_with_mmr[0][0]
            blue_captain = players_with_mmr[1][0]
            
            await self.proceed_with_draft(
                guild, draft_channel, category, game, match_id, player_ids,
                red_role, blue_role, red_captain, blue_captain
            )
        
        elif game.captain_selection == CaptainSelection.RANDOM:
            captains = random.sample(player_ids, 2)
            red_captain, blue_captain = captains
            
            await self.proceed_with_draft(
                guild, draft_channel, category, game, match_id, player_ids,
                red_role, blue_role, red_captain, blue_captain
            )
        
        else:  # ADMIN selection
            # Show player list and wait for admin
            embed = discord.Embed(
                title=f"{game.name} - Captain Selection",
                description="An admin must select the captains.",
                color=COLOR_NEUTRAL
            )
            
            player_list = "\n".join([f"• <@{pid}>" for pid in player_ids])
            embed.add_field(name="Available Players", value=player_list, inline=False)
            embed.add_field(
                name="Instructions",
                value="Admin: Use `/cm admin` and select captains, or @ two players here.",
                inline=False
            )
            
            await draft_channel.send(embed=embed)
            
            # Store state for admin selection
            await DatabaseHelper.update_match(match_id, queue_type="captains_awaiting")
    
    async def proceed_with_draft(self, guild: discord.Guild, draft_channel: discord.TextChannel,
                                  category: discord.CategoryChannel, game: GameConfig,
                                  match_id: int, player_ids: List[int],
                                  red_role: discord.Role, blue_role: discord.Role,
                                  red_captain: int, blue_captain: int):
        """Proceed with the captain draft."""
        # Give captains send message permission
        red_member = guild.get_member(red_captain)
        blue_member = guild.get_member(blue_captain)
        
        if red_member:
            await draft_channel.set_permissions(red_member, view_channel=True, send_messages=True)
        if blue_member:
            await draft_channel.set_permissions(blue_member, view_channel=True, send_messages=True)
        
        # Initialize teams
        red_team = [red_captain]
        blue_team = [blue_captain]
        available = [pid for pid in player_ids if pid not in [red_captain, blue_captain]]
        
        # Snake draft order: Red, Blue, Blue, Red, Red, Blue, Blue, Red...
        # Total picks = len(available)
        picks_needed = len(available)
        current_picker = "red"  # Red picks first
        pick_count = {"red": 0, "blue": 0}

        # Fetch IGNs for display (show only IGN when available)
        igns = await DatabaseHelper.get_match_igns(match_id)

        def format_player(pid: int, is_captain: bool = False) -> str:
            """Format player display - show only IGN when available."""
            if pid in igns:
                line = f"`{igns[pid]}`"
            else:
                m = guild.get_member(pid)
                line = m.display_name if m else f"<@{pid}>"
            if is_captain:
                line += " (C)"
            return line

        async def update_draft_embed():
            embed = discord.Embed(
                title=f"{game.name} - Captain Draft",
                color=COLOR_NEUTRAL
            )

            red_list = "\n".join([f"• {format_player(pid, pid == red_captain)}"
                                  for pid in red_team])
            blue_list = "\n".join([f"• {format_player(pid, pid == blue_captain)}"
                                   for pid in blue_team])

            embed.add_field(name=f"Red Team ({red_role.mention})", value=red_list or "None", inline=True)
            embed.add_field(name=f"Blue Team ({blue_role.mention})", value=blue_list or "None", inline=True)

            if available:
                avail_list = "\n".join([f"• {format_player(pid)}" for pid in available])
                embed.add_field(name="Available", value=avail_list, inline=False)

                if current_picker == "red":
                    embed.add_field(name="Now Picking", value=f"{format_player(red_captain)} (Red)", inline=False)
                else:
                    embed.add_field(name="Now Picking", value=f"{format_player(blue_captain)} (Blue)", inline=False)
            else:
                embed.add_field(name="Status", value="Draft complete!", inline=False)

            return embed
        
        embed = await update_draft_embed()
        draft_msg = await draft_channel.send(embed=embed)
        
        # Wait for picks
        def check(m):
            if m.channel != draft_channel:
                return False
            if current_picker == "red" and m.author.id != red_captain:
                return False
            if current_picker == "blue" and m.author.id != blue_captain:
                return False
            # Check for mention
            if not m.mentions:
                return False
            return m.mentions[0].id in available
        
        while available:
            try:
                msg = await self.bot.wait_for("message", check=check, timeout=300)
                picked_player = msg.mentions[0].id
                
                if current_picker == "red":
                    red_team.append(picked_player)
                    pick_count["red"] += 1
                else:
                    blue_team.append(picked_player)
                    pick_count["blue"] += 1
                
                available.remove(picked_player)
                await msg.delete()
                
                # Determine next picker (snake draft)
                total_picks = pick_count["red"] + pick_count["blue"]
                # Pattern: R, B, B, R, R, B, B, R...
                # Position 0: R, 1: B, 2: B, 3: R, 4: R, 5: B...
                position_in_cycle = total_picks % 4
                if position_in_cycle in [0, 3]:
                    current_picker = "red"
                else:
                    current_picker = "blue"
                
                embed = await update_draft_embed()
                await draft_msg.edit(embed=embed)
                
            except asyncio.TimeoutError:
                # Auto-pick randomly
                picked_player = random.choice(available)
                if current_picker == "red":
                    red_team.append(picked_player)
                    pick_count["red"] += 1
                else:
                    blue_team.append(picked_player)
                    pick_count["blue"] += 1
                
                available.remove(picked_player)
                
                total_picks = pick_count["red"] + pick_count["blue"]
                position_in_cycle = total_picks % 4
                if position_in_cycle in [0, 3]:
                    current_picker = "red"
                else:
                    current_picker = "blue"
                
                embed = await update_draft_embed()
                await draft_msg.edit(embed=embed)
        
        # Draft complete - add captains flag
        await DatabaseHelper.add_match_player(match_id, red_captain, "red", was_captain=True)
        await DatabaseHelper.add_match_player(match_id, blue_captain, "blue", was_captain=True)
        
        # Create match channel
        await self.create_match_channel(
            guild, category, game, match_id,
            red_team, blue_team, red_role, blue_role,
            draft_channel=draft_channel
        )
    
    async def _generate_storyline(
        self, guild: discord.Guild, game: GameConfig,
        red_team: List[int], blue_team: List[int]
    ) -> Optional[str]:
        """Pick the single most interesting storyline for this match.

        Candidates are scored by priority; the highest-scoring one wins.
        Returns a formatted string or None if nothing interesting is found.
        """
        all_players = red_team + blue_team
        candidates: List[Tuple[float, str]] = []  # (priority_score, text)

        try:
            async with DatabaseHelper._get_db() as db:
                # --- Win / Loss streaks ---
                for pid in all_players:
                    rows = await (await db.execute("""
                        SELECT CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END as won
                        FROM matches m JOIN match_players mp ON m.match_id = mp.match_id
                        WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                        ORDER BY m.decided_at DESC
                    """, (pid, game.game_id))).fetchall()

                    if not rows:
                        continue
                    # Count consecutive wins from most recent
                    win_streak = 0
                    for r in rows:
                        if r['won']:
                            win_streak += 1
                        else:
                            break
                    # Count consecutive losses from most recent
                    loss_streak = 0
                    for r in rows:
                        if not r['won']:
                            loss_streak += 1
                        else:
                            break

                    if win_streak >= 5:
                        score = win_streak * 10
                        candidates.append((score, f"<@{pid}> is on a **{win_streak}-game win streak** 🔥"))
                    if loss_streak >= 5:
                        score = loss_streak * 10
                        candidates.append((score, f"<@{pid}> is on a **{loss_streak}-game loss streak** 💀"))

                # --- Teammate duo win% / loss% ---
                for team in (red_team, blue_team):
                    for i, p1 in enumerate(team):
                        for p2 in team[i + 1:]:
                            row = await (await db.execute("""
                                SELECT COUNT(*) as games,
                                       SUM(CASE WHEN mp1.team = m.winning_team THEN 1 ELSE 0 END) as wins
                                FROM match_players mp1
                                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team = mp2.team
                                JOIN matches m ON mp1.match_id = m.match_id
                                WHERE mp1.player_id = ? AND mp2.player_id = ? AND m.game_id = ?
                                  AND m.winning_team IS NOT NULL
                            """, (p1, p2, game.game_id))).fetchone()

                            if not row or row['games'] < 5:
                                continue
                            games = row['games']
                            wins = row['wins']
                            wr = (wins / games) * 100

                            if wr >= 70:
                                score = (wr - 50) * games / 5
                                candidates.append((score, f"<@{p1}> & <@{p2}> are **{wr:.0f}% WR** together ({wins}-{games - wins})"))
                            elif wr <= 30:
                                score = (50 - wr) * games / 5
                                candidates.append((score, f"<@{p1}> & <@{p2}> are **{wr:.0f}% WR** together ({wins}-{games - wins}) 😬"))

                # --- Cross-team rivalry ---
                for red_pid in red_team:
                    for blue_pid in blue_team:
                        rivalry = await DatabaseHelper.get_rivalry(red_pid, blue_pid, game.game_id)
                        if not rivalry:
                            continue
                        total = rivalry[0] + rivalry[1]
                        if total < 5:
                            continue
                        dom = max(rivalry[0], rivalry[1]) / total * 100
                        if dom >= 70:
                            score = (dom - 50) * total / 5
                            dominant_pid = red_pid if rivalry[0] > rivalry[1] else blue_pid
                            other_pid = blue_pid if dominant_pid == red_pid else red_pid
                            candidates.append((score, f"<@{dominant_pid}> has a **{rivalry[0]}-{rivalry[1]}** record vs <@{other_pid}>"))

                # --- Comeback (30+ days away) ---
                for pid in all_players:
                    row = await (await db.execute("""
                        SELECT MAX(m.decided_at) as last_played
                        FROM matches m JOIN match_players mp ON m.match_id = mp.match_id
                        WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                    """, (pid, game.game_id))).fetchone()

                    if row and row['last_played']:
                        try:
                            last = datetime.fromisoformat(row['last_played'])
                            if last.tzinfo is None:
                                last = last.replace(tzinfo=timezone.utc)
                            days_away = (datetime.now(timezone.utc) - last).days
                            if days_away >= 30:
                                candidates.append((25, f"<@{pid}> returns after **{days_away} days** away 👋"))
                        except (ValueError, TypeError):
                            pass

        except Exception as e:
            logger.error(f"_generate_storyline error: {e}")
            return None

        if not candidates:
            return None

        # Highest priority score wins
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    async def create_match_channel(self, guild: discord.Guild, category: discord.CategoryChannel,
                                    game: GameConfig, match_id: int,
                                    red_team: List[int], blue_team: List[int],
                                    red_role: discord.Role, blue_role: discord.Role,
                                    draft_channel: discord.TextChannel = None):
        """Create the match channel and assign roles."""
        # Get match short_id for naming
        match_data = await DatabaseHelper.get_match(match_id)
        short_id = match_data.get("short_id", str(match_id)) if match_data else str(match_id)

        # Create channel named "lobby-{short_id}"
        channel_name = f"lobby-{short_id}"

        # Get mod roles and admin role for permissions
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")

        # Check if this is a secondary match with mode voting
        is_secondary_match = bool(match_data.get("is_secondary")) if match_data else False
        secondary_modes = []
        if is_secondary_match:
            secondary_modes = await DatabaseHelper.get_secondary_modes(game.game_id)
        has_mode_vote = is_secondary_match and len(secondary_modes) >= 2

        # Channel viewable by everyone but only players, admins, and mods can type
        # For mode vote matches: teams start locked (can't send) until voting concludes
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True),
            red_role: discord.PermissionOverwrite(view_channel=True, send_messages=not has_mode_vote),
            blue_role: discord.PermissionOverwrite(view_channel=True, send_messages=not has_mode_vote)
        }

        # Add CM admin role permissions
        if admin_role_id:
            admin_role = guild.get_role(int(admin_role_id))
            if admin_role:
                overwrites[admin_role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, manage_messages=True
                )

        # Add mod role permissions
        for mod_role_id in mod_role_ids:
            mod_role = guild.get_role(mod_role_id)
            if mod_role:
                overwrites[mod_role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, manage_messages=True
                )

        match_channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
        await DatabaseHelper.update_match(match_id, channel_id=match_channel.id)

        # Create team VCs if enabled
        red_vc_id = None
        blue_vc_id = None
        vc_error = None
        if game.vc_creation_enabled:
            try:
                # VC permissions: only team members can connect/speak, others can't even view
                red_vc_overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False),
                    guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    red_role: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    blue_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False)
                }
                blue_vc_overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False),
                    guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    blue_role: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    red_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False)
                }

                # Name VCs after the team role (e.g., "Red 72KW9" or "Blue 72KW9")
                red_vc = None
                blue_vc = None
                try:
                    red_vc = await category.create_voice_channel(
                        name=red_role.name,
                        overwrites=red_vc_overwrites
                    )
                    blue_vc = await category.create_voice_channel(
                        name=blue_role.name,
                        overwrites=blue_vc_overwrites
                    )
                    red_vc_id = red_vc.id
                    blue_vc_id = blue_vc.id
                    await DatabaseHelper.update_match(match_id, red_vc_id=red_vc_id, blue_vc_id=blue_vc_id)
                except Exception as e:
                    # Cleanup any partially created VCs
                    if red_vc:
                        try:
                            await red_vc.delete(reason="Cleanup after blue VC creation failed")
                        except Exception:
                            pass
                    raise  # Re-raise to be caught by outer handler
            except Exception as e:
                logger.error(f"Failed to create VCs for match #{match_id}: {e}")
                vc_error = str(e)

        # Assign roles and add to database
        try:
            existing_players = await DatabaseHelper.get_match_players(match_id)
            existing_player_ids = {p["player_id"] for p in existing_players}

            for pid in red_team:
                member = guild.get_member(pid)
                if member:
                    await member.add_roles(red_role)
                # Check if already added (captains)
                if pid not in existing_player_ids:
                    await DatabaseHelper.add_match_player(match_id, pid, "red")
                    existing_player_ids.add(pid)

            for pid in blue_team:
                member = guild.get_member(pid)
                if member:
                    await member.add_roles(blue_role)
                if pid not in existing_player_ids:
                    await DatabaseHelper.add_match_player(match_id, pid, "blue")
                    existing_player_ids.add(pid)
        except Exception as e:
            logger.error(f"create_match_channel: role assignment failed for match #{match_id}: {e}")
            # Clean up created channels to avoid ghost channels
            try:
                await match_channel.delete(reason="Cleanup after role assignment failure")
            except Exception:
                pass
            if red_vc_id:
                vc = guild.get_channel(red_vc_id)
                if vc:
                    try:
                        await vc.delete(reason="Cleanup after role assignment failure")
                    except Exception:
                        pass
            if blue_vc_id:
                vc = guild.get_channel(blue_vc_id)
                if vc:
                    try:
                        await vc.delete(reason="Cleanup after role assignment failure")
                    except Exception:
                        pass
            raise

        if has_mode_vote:
            # Secondary match with 2+ modes: run two-round mode vote
            asyncio.create_task(self._run_secondary_mode_vote(
                guild, match_channel, game, match_id,
                secondary_modes, red_team, blue_team,
                red_role, blue_role,
                red_vc_id=red_vc_id, blue_vc_id=blue_vc_id, vc_error=vc_error
            ))
        elif is_secondary_match and len(secondary_modes) == 1:
            # Only 1 mode: skip voting, use it directly
            mode = secondary_modes[0]
            map_name = await self._resolve_mode_map(game, mode)
            await DatabaseHelper.set_match_mode(match_id, mode["mode_name"], map_name)
            mirror_comp = self._generate_mirror_comp() if mode.get("is_mirror") else None
            await self._post_match_embeds(
                guild, match_channel, game, match_id,
                red_team, blue_team, red_role, blue_role,
                red_vc_id=red_vc_id, blue_vc_id=blue_vc_id, vc_error=vc_error,
                mode_name=mode["mode_name"], map_name=map_name,
                mode_description=mode.get("description"),
                is_ffa=mode.get("is_ffa", False),
                mirror_comp=mirror_comp
            )
        else:
            # Normal flow: post team embeds, then start regular map vote
            await self._post_match_embeds(
                guild, match_channel, game, match_id,
                red_team, blue_team, red_role, blue_role,
                red_vc_id=red_vc_id, blue_vc_id=blue_vc_id, vc_error=vc_error
            )

            # Start map vote if mapvote cog is loaded and game is configured
            mapvote_cog = self.bot.get_cog("mapvote")
            if mapvote_cog:
                try:
                    game_configured = False
                    async with mapvote_cog.config_lock:
                        games_cfg = mapvote_cog._get_games_config_sync()
                        game_configured = game.name in games_cfg

                    if game_configured:
                        all_players = red_team + blue_team
                        await mapvote_cog.start_programmatic_vote(
                            guild_id=guild.id,
                            channel=match_channel,
                            game_name=game.name,
                            duration=3,
                            max_votes=len(all_players),
                            allowed_voters=all_players,
                            red_role_id=red_role.id,
                            blue_role_id=blue_role.id,
                            match_id=match_id
                        )
                except Exception as e:
                    logger.error(f"Error starting map vote for match {match_id}: {e}")

        # Delete draft channel if exists
        if draft_channel:
            await draft_channel.delete()

        # Start 3-hour timeout
        task = asyncio.create_task(self.match_timeout(guild, match_id, match_channel))
        self.match_timeout_tasks[match_id] = task

    async def _run_secondary_mode_vote(self, guild, channel, game, match_id,
                                        modes, red_team, blue_team,
                                        red_role, blue_role, **kwargs):
        """Run the full two-round hidden mode vote for secondary matches."""
        all_players = red_team + blue_team
        try:
            if len(modes) == 2:
                # Skip Round 1, go directly to final round
                winner, vote_tally = await self._run_vote_round(
                    channel, modes, match_id, all_players, round_num=2
                )
            else:
                # Round 1: all modes
                top_modes, _ = await self._run_vote_round(
                    channel, modes, match_id, all_players, round_num=1
                )
                if len(top_modes) == 1:
                    winner = top_modes[0]
                    vote_tally = None
                else:
                    winner, vote_tally = await self._run_vote_round(
                        channel, top_modes, match_id, all_players, round_num=2
                    )

            map_name = await self._resolve_mode_map(game, winner)
            await DatabaseHelper.set_match_mode(match_id, winner["mode_name"], map_name)

            mirror_comp = self._generate_mirror_comp() if winner.get("is_mirror") else None

            await self._post_match_embeds(
                guild, channel, game, match_id,
                red_team, blue_team, red_role, blue_role,
                mode_name=winner["mode_name"], map_name=map_name,
                mode_description=winner.get("description"),
                vote_tally=vote_tally,
                is_ffa=winner.get("is_ffa", False),
                mirror_comp=mirror_comp, **kwargs
            )
        except Exception as e:
            logger.error(f"Mode vote error for match {match_id}: {e}", exc_info=True)
            # Fallback: post embeds without mode info
            try:
                await self._post_match_embeds(
                    guild, channel, game, match_id,
                    red_team, blue_team, red_role, blue_role, **kwargs
                )
            except Exception:
                pass
        finally:
            # Always unlock channel
            try:
                await channel.set_permissions(red_role, view_channel=True, send_messages=True)
                await channel.set_permissions(blue_role, view_channel=True, send_messages=True)
            except Exception:
                pass

    async def _run_vote_round(self, channel, modes, match_id, players, round_num):
        """Run a single voting round.

        For round_num=1: returns (top_2_modes, None)
        For round_num=2: returns (winning_mode, {mode_name: count})
        """
        result_future = asyncio.get_running_loop().create_future()

        from .views_gameplay import SecondaryModeVoteView
        view = SecondaryModeVoteView(
            modes=modes,
            match_id=match_id,
            allowed_voters=players,
            round_num=round_num,
            result_future=result_future,
        )

        round_label = "Round 1 — Vote for your mode!" if round_num == 1 else "Final Round — Vote!"
        mode_list = " / ".join(f"**{m['mode_name']}**" for m in modes)
        embed = discord.Embed(
            title=f"Mode Vote — {round_label}",
            description=f"Modes: {mode_list}\n\n"
                        "Votes are hidden. Timer starts when the first vote is cast (30s).",
            color=COLOR_NEUTRAL
        )
        msg = await channel.send(embed=embed, view=view)

        # Timer task
        async def timer():
            # Wait up to 30s for first vote (idle timeout)
            for _ in range(30):
                if view.concluded or view.first_vote_time:
                    break
                await asyncio.sleep(1)
            if view.concluded:
                return
            if view.first_vote_time is None:
                # No votes after 30s idle — force conclude
                if not view.concluded:
                    view.concluded = True
                    if not result_future.done():
                        result_future.set_result("timeout")
                return
            # Wait 30s from first vote
            elapsed = (datetime.now(timezone.utc) - view.first_vote_time).total_seconds()
            remaining = max(0, 30 - elapsed)
            # Update embed with countdown
            countdown_embed = discord.Embed(
                title=f"Mode Vote — {round_label}",
                description=f"Modes: {mode_list}\n\n"
                            f"Voting ends <t:{int((view.first_vote_time + timedelta(seconds=30)).timestamp())}:R>",
                color=COLOR_NEUTRAL
            )
            try:
                await msg.edit(embed=countdown_embed)
            except Exception:
                pass
            if remaining > 0:
                await asyncio.sleep(remaining)
            if not view.concluded:
                view.concluded = True
                if not result_future.done():
                    result_future.set_result("timeout")

        timer_task = asyncio.create_task(timer())

        try:
            await result_future
        finally:
            timer_task.cancel()
            try:
                await timer_task
            except (asyncio.CancelledError, Exception):
                pass

        # Disable buttons
        for item in view.children:
            item.disabled = True
        try:
            await msg.edit(view=view)
        except Exception:
            pass

        # Tally votes
        vote_counts = {name: len(voters) for name, voters in view.votes.items()}

        if round_num == 1:
            # Find top 2 modes
            sorted_modes = sorted(modes, key=lambda m: vote_counts.get(m["mode_name"], 0), reverse=True)
            voted_modes = [m for m in sorted_modes if vote_counts.get(m["mode_name"], 0) > 0]

            if len(voted_modes) <= 1:
                # 0 or 1 modes got votes
                if len(voted_modes) == 1:
                    return [voted_modes[0]], None
                # No votes at all — random pick, return as single winner
                return [random.choice(modes)], None

            # Top 2 by vote count (ties broken by position in sorted list, which is random for ties)
            top_count = vote_counts.get(sorted_modes[0]["mode_name"], 0)
            second_count = vote_counts.get(sorted_modes[1]["mode_name"], 0)

            # Get all modes tied for 1st
            first_place = [m for m in modes if vote_counts.get(m["mode_name"], 0) == top_count]
            if len(first_place) >= 2:
                # Multiple tied for 1st — pick 2 randomly
                top_2 = random.sample(first_place, 2)
            else:
                # Get all modes tied for 2nd
                second_place = [m for m in modes if vote_counts.get(m["mode_name"], 0) == second_count and m["mode_name"] != first_place[0]["mode_name"]]
                runner_up = random.choice(second_place) if second_place else sorted_modes[1]
                top_2 = [first_place[0], runner_up]

            return top_2, None
        else:
            # Round 2: determine winner
            sorted_modes = sorted(modes, key=lambda m: vote_counts.get(m["mode_name"], 0), reverse=True)
            top_count = vote_counts.get(sorted_modes[0]["mode_name"], 0)

            if top_count == 0:
                # No votes — random
                return random.choice(modes), {m["mode_name"]: 0 for m in modes}

            # Check for tie
            tied = [m for m in modes if vote_counts.get(m["mode_name"], 0) == top_count]
            winner = random.choice(tied) if len(tied) > 1 else sorted_modes[0]

            tally = {m["mode_name"]: vote_counts.get(m["mode_name"], 0) for m in modes}
            return winner, tally

    async def _resolve_mode_map(self, game: GameConfig, mode: dict) -> Optional[str]:
        """Resolve the map for a winning mode based on its map_pool_type."""
        pool_type = mode.get("map_pool_type", "none")
        if pool_type == "none":
            return None
        elif pool_type == "standard":
            mapvote_cog = self.bot.get_cog("mapvote")
            if mapvote_cog:
                try:
                    games_cfg = mapvote_cog._get_games_config_sync()
                    game_data = games_cfg.get(game.name, {})
                    maps = list(game_data.get("maps", {}).keys())
                    if maps:
                        return random.choice(maps)
                except Exception as e:
                    logger.error(f"Error resolving standard maps for {game.name}: {e}")
            return None
        elif pool_type == "custom":
            custom_maps = mode.get("custom_maps")
            if isinstance(custom_maps, str):
                custom_maps = json.loads(custom_maps)
            if custom_maps:
                return random.choice(custom_maps)
            return None
        return None

    def _generate_mirror_comp(self) -> dict:
        """Generate a mirror match comp: 2 Vanguards, 2 Strategists, 2 Duelists."""
        comp = {}
        for role, pool in RIVALS_ROSTER.items():
            comp[role] = random.sample(pool, 2)
        return comp

    async def _post_match_embeds(self, guild: discord.Guild, match_channel: discord.TextChannel,
                                game: GameConfig, match_id: int,
                                red_team: list, blue_team: list,
                                red_role: discord.Role, blue_role: discord.Role,
                                red_vc_id: int = None, blue_vc_id: int = None,
                                vc_error: str = None,
                                mode_name: str = None, map_name: str = None,
                                mode_description: str = None, vote_tally: dict = None,
                                is_ffa: bool = False, mirror_comp: dict = None):
        """Post team embeds to match channel and queue channel. Reused by normal flow and mode vote."""
        match_data = await DatabaseHelper.get_match(match_id)
        short_id = match_data.get("short_id", str(match_id)) if match_data else str(match_id)
        igns = await DatabaseHelper.get_match_igns(match_id)

        embed = discord.Embed(
            title=f"{game.name} Match {short_id}",
            color=COLOR_NEUTRAL
        )

        players = await DatabaseHelper.get_match_players(match_id)
        is_secondary = bool(match_data.get("is_secondary")) if match_data else False

        if is_ffa:
            # FFA: show all players in one field
            all_pids = red_team + blue_team
            player_lines = []
            for pid in all_pids:
                if pid in igns:
                    player_lines.append(f"`{igns[pid]}`")
                else:
                    m = guild.get_member(pid)
                    player_lines.append(m.display_name if m else f"<@{pid}>")
            embed.add_field(name="Players", value="\n".join(player_lines), inline=False)
        else:
            red_captain = next((p["player_id"] for p in players if p["team"] == "red" and p["was_captain"]), None)
            blue_captain = next((p["player_id"] for p in players if p["team"] == "blue" and p["was_captain"]), None)

            red_lines = []
            for pid in red_team:
                if pid in igns:
                    line = f"`{igns[pid]}`"
                else:
                    m = guild.get_member(pid)
                    line = m.display_name if m else f"<@{pid}>"
                if pid == red_captain:
                    line += " (C)"
                red_lines.append(line)

            blue_lines = []
            for pid in blue_team:
                if pid in igns:
                    line = f"`{igns[pid]}`"
                else:
                    m = guild.get_member(pid)
                    line = m.display_name if m else f"<@{pid}>"
                if pid == blue_captain:
                    line += " (C)"
                blue_lines.append(line)

            embed.add_field(name="Red Team", value="\n".join(red_lines), inline=True)
            embed.add_field(name="Blue Team", value="\n".join(blue_lines), inline=True)

        # Add mode/map info if provided
        if mode_name:
            mode_str = f"**{mode_name}**"
            if map_name:
                mode_str += f" on **{map_name}**"
            if mode_description:
                mode_str += f"\n{mode_description}"
            embed.add_field(name="Mode", value=mode_str, inline=False)

        # Add mirror match comp if provided
        if mirror_comp:
            comp_lines = []
            for role, chars in mirror_comp.items():
                comp_lines.append(f"**{role}:** {', '.join(chars)}")
            embed.add_field(name="Team Comp", value="\n".join(comp_lines), inline=False)

        # Add vote tally if provided
        if vote_tally:
            tally_lines = [f"{name}: **{count}** vote{'s' if count != 1 else ''}" for name, count in sorted(vote_tally.items(), key=lambda x: x[1], reverse=True)]
            embed.add_field(name="Vote Results", value="\n".join(tally_lines), inline=False)

        if not is_ffa:
            storyline = await self._generate_storyline(guild, game, red_team, blue_team)
            if storyline:
                embed.add_field(name="Storylines", value=storyline, inline=False)

        if red_vc_id and blue_vc_id:
            embed.add_field(
                name="Voice Channels",
                value=f"Red: <#{red_vc_id}>\nBlue: <#{blue_vc_id}>",
                inline=False
            )
        elif vc_error:
            embed.add_field(
                name="Voice Channels",
                value=f"Failed to create VCs: {vc_error[:50]}...",
                inline=False
            )

        if is_secondary:
            embed.add_field(
                name="End Match",
                value="Use `/cm_arcade_end` when the game is over (3 votes to end).",
                inline=False
            )
        else:
            embed.add_field(
                name="Report Winner",
                value="Use `/cm_win` when the match is over, or `/cm_abandon` to cancel.",
                inline=False
            )
        banner = game.secondary_banner_url if is_secondary and game.secondary_banner_url else game.banner_url
        if banner:
            embed.set_image(url=banner)

        if is_ffa:
            red_mention = red_role.mention if red_role else ""
            blue_mention = blue_role.mention if blue_role else ""
            match_msg = await match_channel.send(f"{red_mention} {blue_mention} — Match is live!", embed=embed)
        else:
            red_mention = red_role.mention if red_role else "Red Team"
            blue_mention = blue_role.mention if blue_role else "Blue Team"
            match_msg = await match_channel.send(f"{red_mention} vs {blue_mention}", embed=embed)
        await DatabaseHelper.update_match(match_id, match_msg_id=match_msg.id)

        # Send MMR embed to log channel
        await self._send_mmr_embed_to_log(guild, game, match_id, red_team, blue_team, igns, red_role, blue_role)

        # Send teams embed to queue channel (use secondary channel if applicable)
        queue_ch_id = game.secondary_queue_channel_id if is_secondary and game.secondary_queue_channel_id else game.queue_channel_id
        if queue_ch_id:
            queue_channel = guild.get_channel(queue_ch_id)
            if queue_channel:
                try:
                    teams_embed = discord.Embed(
                        title="Ongoing Match",
                        url=match_channel.jump_url,
                        color=COLOR_NEUTRAL
                    )
                    teams_embed.set_footer(text=f"Match {short_id}")

                    if is_ffa:
                        all_names = []
                        for pid in red_team + blue_team:
                            if pid in igns:
                                all_names.append(f"`{igns[pid]}`")
                            else:
                                member = guild.get_member(pid)
                                all_names.append(member.display_name if member else f"<@{pid}>")
                        teams_embed.add_field(name="Players", value="\n".join(all_names) or "—", inline=False)
                    else:
                        red_names = []
                        for pid in red_team:
                            if pid in igns:
                                red_names.append(f"`{igns[pid]}`")
                            else:
                                member = guild.get_member(pid)
                                red_names.append(member.display_name if member else f"<@{pid}>")
                        blue_names = []
                        for pid in blue_team:
                            if pid in igns:
                                blue_names.append(f"`{igns[pid]}`")
                            else:
                                member = guild.get_member(pid)
                                blue_names.append(member.display_name if member else f"<@{pid}>")
                        teams_embed.add_field(name="Red Team", value="\n".join(red_names) or "—", inline=True)
                        teams_embed.add_field(name="Blue Team", value="\n".join(blue_names) or "—", inline=True)
                    teams_msg = await queue_channel.send(embed=teams_embed)
                    await DatabaseHelper.update_match(match_id, queue_teams_msg_id=teams_msg.id)

                    # Bump the active queue message so it's always the newest in the channel
                    for qs in self.queues.values():
                        if qs.game_id == game.game_id and qs.channel_id == queue_channel.id \
                                and qs.state == "waiting" and qs.message_id:
                            try:
                                old_msg = await queue_channel.fetch_message(qs.message_id)
                                queue_embed = await self.create_queue_embed(game, qs, guild)
                                view = QueueView(self, game.game_id, qs.queue_id)
                                new_msg = await queue_channel.send(embed=queue_embed, view=view)
                                qs.message_id = new_msg.id
                                async with DatabaseHelper._get_db() as db:
                                    await db.execute(
                                        "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                                        (new_msg.id, qs.queue_id)
                                    )
                                    await db.commit()
                                await old_msg.delete()
                            except Exception as e:
                                logger.warning(f"Failed to bump queue message after teams embed: {e}")
                            break

                except Exception as e:
                    logger.warning(f"Failed to send teams embed to queue channel: {e}")

    async def match_timeout(self, guild: discord.Guild, match_id: int, channel: discord.TextChannel,
                            delay_seconds: int = 3 * 60 * 60):
        """Handle match timeout after delay_seconds (default 3 hours)."""
        await asyncio.sleep(delay_seconds)
        
        # Check if match still active
        match = await DatabaseHelper.get_match(match_id)
        if match and not match["winning_team"] and not match["cancelled"]:
            is_secondary = bool(match.get("is_secondary"))
            if is_secondary:
                # Auto-end arcade matches on timeout
                self.arcade_end_votes.pop(match_id, None)
                await channel.send("This arcade match has been open for 3 hours — ending automatically.")
                await asyncio.sleep(3)
                await self._end_arcade_match(guild, match_id)
            else:
                admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
                admin_mention = f"<@&{admin_role_id}>" if admin_role_id else "Admins"
                await channel.send(
                    f"{admin_mention} - This match has been ongoing for 3 hours without a winner. "
                    "Please use `/cm admin` to force a winner or cancel the match."
                )
    
    # -------------------------------------------------------------------------
    # WIN VOTING & FINALIZATION
    # -------------------------------------------------------------------------
    
    async def handle_win_vote(self, interaction: discord.Interaction, match_id: int, team: Team):
        """Handle a win vote."""
        user = interaction.user

        # Guard: reject votes on matches that are already decided or cancelled
        match_state = await DatabaseHelper.get_match(match_id)
        if not match_state:
            await interaction.response.send_message("This match no longer exists.", ephemeral=True)
            return
        if match_state.get("winning_team") or match_state.get("cancelled"):
            await interaction.response.send_message("This match has already ended.", ephemeral=True)
            return

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match_id)
        if not any(p["player_id"] == user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        # Prevent double-voting
        existing_vote = await DatabaseHelper.get_player_win_vote(match_id, user.id)
        if existing_vote is not None:
            if existing_vote == team.value:
                await interaction.response.send_message("Your vote has already been counted.", ephemeral=True)
                return
            else:
                await interaction.response.send_message("You've already voted.", ephemeral=True)
                return

        # Record vote
        await DatabaseHelper.add_win_vote(match_id, user.id, team.value)
        
        # Get vote counts
        votes = await DatabaseHelper.get_win_votes(match_id)
        red_votes = votes.get("red", 0)
        blue_votes = votes.get("blue", 0)
        
        # Check for majority
        match = await DatabaseHelper.get_match(match_id)
        game = await DatabaseHelper.get_game(match["game_id"])
        match_players = await DatabaseHelper.get_match_players(match_id)
        total_match_players = len(match_players) if match_players else game.player_count
        needed = (total_match_players // 2) + 1

        if red_votes >= needed:
            await interaction.response.defer()
            await self.finalize_match(interaction.guild, match_id, Team.RED)
        elif blue_votes >= needed:
            await interaction.response.defer()
            await self.finalize_match(interaction.guild, match_id, Team.BLUE)
        else:
            # Get IGNs, voter IDs, and player lists to keep team names visible
            igns = await DatabaseHelper.get_match_igns(match_id)
            voter_ids = await DatabaseHelper.get_win_voter_ids(match_id)
            red_players = [p for p in players if p["team"] == "red"]
            blue_players = [p for p in players if p["team"] == "blue"]

            def format_player(p: dict) -> str:
                pid = p["player_id"]
                check = "✓ " if pid in voter_ids else "⠀ "
                if pid in igns:
                    return f"{check}`{igns[pid]}`"
                m = interaction.guild.get_member(pid)
                name = m.display_name if m else f"<@{pid}>"
                return f"{check}{name}"

            red_lines = [format_player(p) for p in red_players]
            blue_lines = [format_player(p) for p in blue_players]

            # Update embed with team names still visible
            total_votes = red_votes + blue_votes
            embed = discord.Embed(
                title="Who Won?",
                description=f"Votes: {total_votes}/{needed}\nCast your vote!",
                color=COLOR_NEUTRAL
            )
            embed.add_field(
                name=f"Red Team ({red_votes} votes)",
                value="\n".join(red_lines) or "None",
                inline=True
            )
            embed.add_field(
                name=f"Blue Team ({blue_votes} votes)",
                value="\n".join(blue_lines) or "None",
                inline=True
            )

            await interaction.response.edit_message(embed=embed)

    async def handle_abandon_vote(self, interaction: discord.Interaction, match_id: int, needed_votes: int):
        """Handle an abandon vote."""
        user = interaction.user

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match_id)
        if not any(p["player_id"] == user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        # Check if already voted
        if await DatabaseHelper.has_voted_abandon(match_id, user.id):
            await interaction.response.send_message("You've already voted to abandon.", ephemeral=True)
            return

        # Record vote
        await DatabaseHelper.add_abandon_vote(match_id, user.id)

        # Get vote count
        current_votes = await DatabaseHelper.get_abandon_votes(match_id)

        # Check for majority
        if current_votes >= needed_votes:
            await interaction.response.defer()
            await self.cancel_match(
                interaction.guild, match_id,
                reason=f"Vote to abandon ({current_votes}/{needed_votes} votes)"
            )
            await interaction.followup.send("Match abandoned by vote.", ephemeral=False)
        else:
            # Update embed
            embed = discord.Embed(
                title="Abandon Match?",
                description=f"Votes: {current_votes}/{needed_votes}",
                color=COLOR_WARNING
            )
            embed.add_field(
                name="Warning",
                value="If the match is abandoned, no stats will be recorded.",
                inline=False
            )
            await interaction.response.edit_message(embed=embed)

    async def finalize_match(self, guild: discord.Guild, match_id: int, winning_team: Team):
        """Finalize a match and update stats (acquires per-match lock)."""
        if match_id not in self.match_finalize_locks:
            self.match_finalize_locks[match_id] = asyncio.Lock()

        async with self.match_finalize_locks[match_id]:
            await self._finalize_match_inner(guild, match_id, winning_team)

    async def _finalize_match_inner(self, guild: discord.Guild, match_id: int, winning_team: Team):
        """Core finalize logic — caller MUST hold self.match_finalize_locks[match_id]."""
        match = await DatabaseHelper.get_match(match_id)
        if not match or match["winning_team"]:
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        players = await DatabaseHelper.get_match_players(match_id)
        is_secondary = bool(match.get("is_secondary"))

        # Determine winners and losers
        winners = [p["player_id"] for p in players if p["team"] == winning_team.value]
        losers = [p["player_id"] for p in players if p["team"] != winning_team.value]

        # Single-pass: fetch all player stats once, compute averages + changes from that
        all_stats = {}
        for pid in winners + losers:
            all_stats[pid] = await DatabaseHelper.get_player_stats(pid, game.game_id)

        now = datetime.now(timezone.utc)

        if is_secondary:
            # Arcade/secondary: no stat tracking at all — just mark ended and cleanup
            await DatabaseHelper.update_match(
                match_id,
                winning_team=winning_team.value,
                ended_at=now.isoformat(),
                decided_at=now.isoformat()
            )

            # Cancel timeout task
            if match_id in self.match_timeout_tasks:
                self.match_timeout_tasks[match_id].cancel()
                del self.match_timeout_tasks[match_id]

            # Skip everything: no stats, no MMR, no rivalries, no leaderboard
            await self.cleanup_match(guild, match)
            return
        else:
            # Standard queue: full MMR calculation

            # Detect returning players (42+ days inactive) and grant boosted K-factor games
            for pid in winners + losers:
                stats = all_stats[pid]
                if stats.last_played and stats.returning_games_remaining == 0:
                    days_inactive = (now - stats.last_played).days
                    if days_inactive >= 42:
                        stats.returning_games_remaining = 5
                        logger.info(
                            f"Returning player detected: {pid} (game {game.game_id}), "
                            f"{days_inactive} days inactive, granting 5 boosted games"
                        )

            avg_winner_mmr = sum(all_stats[pid].effective_mmr for pid in winners) / len(winners) if winners else 1000
            avg_loser_mmr = sum(all_stats[pid].effective_mmr for pid in losers) / len(losers) if losers else 1000

            # Calculate expected scores (ELO formula — divisor 600 for 500-6000 MMR range)
            expected_winner = 1 / (1 + 10 ** ((avg_loser_mmr - avg_winner_mmr) / 600))
            expected_loser = 1 - expected_winner

            # Fetch current win streaks for bonus calculation
            win_streaks = await DatabaseHelper.get_current_win_streaks_batch(winners, game.game_id)

            # Pre-compute all MMR changes
            winner_results = []  # (player_id, old_mmr, mmr_change)
            winner_stats_list = []
            for pid in winners:
                stats = all_stats[pid]
                k = stats.get_k_factor()
                mmr_change = int(k * (1 - expected_winner))
                # Win streak bonus: current streak from DB is pre-this-game, so +1 for this win
                current_streak = win_streaks.get(pid, 0) + 1
                mmr_change = int(mmr_change * _streak_bonus_multiplier(current_streak))
                mmr_change = max(1, mmr_change)
                old_mmr = stats.mmr
                stats.mmr += mmr_change
                stats.wins += 1
                stats.games_played += 1
                stats.last_played = now
                winner_results.append((pid, old_mmr, mmr_change))
                winner_stats_list.append((pid, old_mmr, stats))

            loser_results = []  # (player_id, old_mmr, mmr_change)
            loser_stats_list = []
            for pid in losers:
                stats = all_stats[pid]
                k = stats.get_k_factor()
                mmr_change = int(k * (0 - expected_loser))
                old_mmr = stats.mmr
                stats.mmr = max(500, stats.mmr + mmr_change)  # Floor at 500 — minimum MMR tier
                actual_change = stats.mmr - old_mmr
                stats.losses += 1
                stats.games_played += 1
                stats.last_played = now
                loser_results.append((pid, old_mmr, actual_change))
                loser_stats_list.append((pid, old_mmr, stats))

            # Decrement returning player counters after MMR is computed
            for pid in winners + losers:
                stats = all_stats[pid]
                if stats.returning_games_remaining > 0:
                    stats.returning_games_remaining -= 1

            # Single atomic transaction for all MMR writes + match winner (all-or-nothing)
            async with DatabaseHelper._get_db() as _mmr_db:
                # Mark match as decided within the same transaction as MMR writes
                await _mmr_db.execute(
                    "UPDATE matches SET winning_team = ?, ended_at = ? WHERE match_id = ?",
                    (winning_team.value, now.isoformat(), match_id)
                )
                for pid, old_mmr, stats in winner_stats_list:
                    await _mmr_db.execute(
                        """INSERT OR REPLACE INTO player_game_stats
                           (player_id, game_id, mmr, games_played, wins, losses, admin_offset, last_played, returning_games_remaining)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (stats.player_id, stats.game_id, stats.mmr, stats.games_played,
                         stats.wins, stats.losses, stats.admin_offset,
                         stats.last_played.isoformat() if stats.last_played else None,
                         stats.returning_games_remaining)
                    )
                    await _mmr_db.execute(
                        """INSERT INTO mmr_history (player_id, game_id, match_id, mmr_before, mmr_after, change)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (pid, game.game_id, match_id, old_mmr, stats.mmr, stats.mmr - old_mmr)
                    )
                for pid, old_mmr, stats in loser_stats_list:
                    await _mmr_db.execute(
                        """INSERT OR REPLACE INTO player_game_stats
                           (player_id, game_id, mmr, games_played, wins, losses, admin_offset, last_played, returning_games_remaining)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (stats.player_id, stats.game_id, stats.mmr, stats.games_played,
                         stats.wins, stats.losses, stats.admin_offset,
                         stats.last_played.isoformat() if stats.last_played else None,
                         stats.returning_games_remaining)
                    )
                    await _mmr_db.execute(
                        """INSERT INTO mmr_history (player_id, game_id, match_id, mmr_before, mmr_after, change)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (pid, game.game_id, match_id, old_mmr, stats.mmr, stats.mmr - old_mmr)
                    )
                await _mmr_db.commit()

            # Update rivalries
            for winner_id in winners:
                for loser_id in losers:
                    await DatabaseHelper.update_rivalry(winner_id, loser_id, game.game_id)

            # Stamp decided_at now that all DB writes are complete
            await DatabaseHelper.update_match(
                match_id,
                decided_at=datetime.now(timezone.utc).isoformat()
            )

            # Update MMR roles for all players (Discord API, safe outside lock)
            for pid in winners + losers:
                stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
                await self.update_mmr_roles(guild, pid, game.game_id, stats.effective_mmr)

            # Cancel timeout task
            if match_id in self.match_timeout_tasks:
                self.match_timeout_tasks[match_id].cancel()
                del self.match_timeout_tasks[match_id]

            # Schedule persistent Valorant stats retry (first attempt in 30s)
            if is_valorant_game(game):
                next_attempt = datetime.now(timezone.utc) + timedelta(seconds=30)
                await DatabaseHelper.create_stats_retry(match_id, game.game_id, next_attempt)

        # Rivals: request a scoreboard screenshot in the match channel
        rivals_prompt_posted = False
        if is_rivals_game(game) and match.get("channel_id") and not is_secondary:
            try:
                match_channel = guild.get_channel(match["channel_id"])
                if match_channel:
                    await self._post_rivals_upload_prompt(match_channel, match_id, game)
                    self.rivals_pending_uploads[match_channel.id] = {
                        "match_id": match_id,
                        "game_id": game.game_id,
                        "guild_id": guild.id,
                        "expires_at": datetime.now(timezone.utc) + timedelta(hours=3),
                    }
                    # Start the 30-min reminder + 3h timeout task
                    task = asyncio.create_task(
                        self._rivals_upload_reminder_task(
                            match_id=match_id,
                            channel_id=match_channel.id,
                            guild_id=guild.id,
                        )
                    )
                    self.rivals_reminder_tasks[match_id] = task
                    rivals_prompt_posted = True
            except Exception as e:
                logger.error(f"Error posting Rivals upload prompt for match {match_id}: {e}")

        # Send winner/loser embed to game channel if configured
        if game.game_channel_id:
            game_channel = guild.get_channel(game.game_channel_id)
            if game_channel:
                await self._send_winner_loser_embed(game_channel, game, match_id, players, winning_team)

        # Send results embed to log channel and edit pre-match embed with +/-
        log_channel_id = await DatabaseHelper.get_config("log_channel_id")
        if log_channel_id:
            log_ch = guild.get_channel(int(log_channel_id))
            if log_ch:
                try:
                    await self._send_match_results_to_log(
                        guild, log_ch, game, match_id, winning_team,
                        winner_results, loser_results
                    )
                except Exception as e:
                    logger.error(f"Error sending match results to log: {e}")

                # Edit the pre-match log embed to add +/- changes
                try:
                    await self._edit_prematch_log_embed(
                        guild, log_ch, game, match_id, players,
                        winner_results, loser_results
                    )
                except Exception as e:
                    logger.error(f"Error editing pre-match log embed: {e}")

        # Update persistent leaderboard
        if game.leaderboard_channel_id:
            try:
                await self._update_persistent_leaderboard(guild, game)
            except Exception as e:
                logger.error(f"Error updating persistent leaderboard: {e}")

        # Clean up. For Rivals, keep the match text channel alive so players can
        # still upload the scoreboard screenshot — but tear down VCs, roles,
        # draft channel, and the queue-channel teams embed immediately.
        # The background match_channel_cleanup task will delete the match
        # channel itself after its 12h grace window.
        if rivals_prompt_posted:
            await self.cleanup_match(guild, match, skip_match_channel=True)
        else:
            await self.cleanup_match(guild, match)

    async def _refresh_stale_leaderboards(self):
        """Check each game's leaderboard message; refresh if it's showing a past month."""
        try:
            now = datetime.now(timezone.utc)
            current_month = now.strftime('%B')
            games = await DatabaseHelper.get_all_games()
            for game in games:
                if not game.leaderboard_channel_id:
                    continue
                channel = self.bot.get_channel(game.leaderboard_channel_id)
                if not channel:
                    continue
                needs_refresh = False
                if game.leaderboard_message_id:
                    try:
                        msg = await channel.fetch_message(game.leaderboard_message_id)
                        if msg.embeds and current_month not in (msg.embeds[0].title or ''):
                            needs_refresh = True
                    except discord.NotFound:
                        needs_refresh = True  # Message deleted; self-heal
                    except Exception as e:
                        logger.error(f"Error fetching leaderboard message for {game.name}: {e}")
                else:
                    needs_refresh = True
                if needs_refresh:
                    logger.info(f"Stale leaderboard detected for {game.name}, refreshing for {current_month}.")
                    try:
                        await self._update_persistent_leaderboard(channel.guild, game)
                    except Exception as e:
                        logger.error(f"Error refreshing stale leaderboard for {game.name}: {e}")
        except Exception as e:
            logger.error(f"Error in _refresh_stale_leaderboards: {e}")

    async def monthly_leaderboard_check(self):
        """Background task: at the start of each new month, refresh all leaderboard embeds."""
        await self.bot.wait_until_ready()
        # Immediately check for stale leaderboards (handles bot restarts after month change)
        await self._refresh_stale_leaderboards()
        last_month = datetime.now(EST).month
        while not self.bot.is_closed():
            try:
                await asyncio.sleep(3600)  # Check every hour
                now = datetime.now(EST)
                if now.month != last_month:
                    last_month = now.month
                    month_name = now.strftime('%B')
                    logger.info(f"New month detected ({month_name}), refreshing all leaderboard embeds.")
                    games = await DatabaseHelper.get_all_games()
                    for game in games:
                        if not game.leaderboard_channel_id:
                            continue
                        channel = self.bot.get_channel(game.leaderboard_channel_id)
                        if not channel:
                            continue
                        try:
                            await self._update_persistent_leaderboard(channel.guild, game)
                            logger.info(f"Leaderboard refreshed for {game.name} ({month_name})")
                        except Exception as e:
                            logger.error(f"Error refreshing leaderboard for {game.name}: {e}")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Error in monthly_leaderboard_check: {e}")

    async def stats_retry_poll(self):
        """Background poll loop: fetch Valorant stats for matches with pending retries."""
        # Delays between attempts (seconds): cumulative from match end ~0.5, 2.5, 5.5, 9.5, 14.5, 19.5, 24.5, 34.5, 49.5, 79.5, 139.5 min
        retry_delays = [30, 120, 180, 240, 300, 300, 300, 600, 900, 1800, 3600]
        total_attempts = len(retry_delays)

        while True:
            await asyncio.sleep(30)
            try:
                # Once per day, purge old exhausted/success rows (keeps the table lean)
                import time as _time_cleanup
                if _time_cleanup.monotonic() - self._last_retry_cleanup_at > 86400:
                    async with DatabaseHelper._get_db() as _db:
                        await _db.execute(
                            "DELETE FROM valorant_stats_retry "
                            "WHERE status IN ('success', 'exhausted') "
                            "AND datetime(created_at) < datetime('now', '-30 days')"
                        )
                        await _db.commit()
                    self._last_retry_cleanup_at = _time_cleanup.monotonic()

                pending = await DatabaseHelper.get_pending_stats_retries()
                for retry in pending:
                    match_id = retry["match_id"]
                    game_id = retry["game_id"]
                    attempt = retry["attempt_count"] + 1

                    match = await DatabaseHelper.get_match(match_id)
                    if not match:
                        await DatabaseHelper.update_stats_retry(match_id, status='exhausted',
                                                                last_reason='match not found')
                        continue

                    players = await DatabaseHelper.get_match_players(match_id)
                    player_ids = [p["player_id"] for p in players]
                    if not player_ids:
                        await DatabaseHelper.update_stats_retry(match_id, status='exhausted',
                                                                last_reason='no players')
                        continue

                    # Parse timestamps
                    match_end_time = None
                    if match.get("decided_at"):
                        try:
                            match_end_time = datetime.fromisoformat(match["decided_at"])
                            if match_end_time.tzinfo is None:
                                match_end_time = match_end_time.replace(tzinfo=timezone.utc)
                        except (ValueError, TypeError):
                            pass
                    if not match_end_time:
                        match_end_time = datetime.now(timezone.utc)

                    match_created_at = None
                    if match.get("created_at"):
                        try:
                            match_created_at = datetime.fromisoformat(match["created_at"])
                            if match_created_at.tzinfo is None:
                                match_created_at = match_created_at.replace(tzinfo=timezone.utc)
                        except (ValueError, TypeError):
                            pass

                    # Resolve guild by finding which guild contains the game channel
                    game_obj = await DatabaseHelper.get_game(game_id)
                    guild = None
                    if game_obj and game_obj.game_channel_id:
                        for g in self.bot.guilds:
                            if g.get_channel(game_obj.game_channel_id):
                                guild = g
                                break
                    if not guild and self.bot.guilds:
                        guild = self.bot.guilds[0]

                    short_id = await self._get_match_short_id(match_id)

                    logger.info(f"Match #{match_id}: Stats retry attempt {attempt}/{total_attempts} (guild={'found' if guild else 'none'})")

                    success, reason = await self.fetch_valorant_match_stats(
                        match_id, game_id, player_ids, match_end_time,
                        match_created_at=match_created_at, guild=guild,
                        attempt=attempt
                    )

                    if success:
                        await DatabaseHelper.update_stats_retry(
                            match_id, status='success',
                            attempt_count=attempt, last_reason=reason
                        )
                        logger.info(f"Match #{match_id}: Stats fetched on attempt {attempt}")
                        # Log and notify — isolated try/except so a notification
                        # failure can't swallow the success or crash the poll loop
                        stats = None
                        if guild:
                            try:
                                stats = await DatabaseHelper.get_valorant_match_stats(match_id)
                                missing_count = len(player_ids) - len(stats) if stats else len(player_ids)
                                msg = f"Match {short_id}: Valorant stats auto-fetched ({len(stats) if stats else 0} players, attempt {attempt}/{total_attempts})"
                                if missing_count > 0:
                                    msg += f" — {missing_count} player(s) not matched"
                                await self.log_action(guild, msg)
                            except Exception as e:
                                logger.error(f"Match #{match_id}: Failed to send stats log message: {e}", exc_info=True)

                            # Send stats scoreboard to game channel and log channel
                            try:
                                if stats is None:
                                    stats = await DatabaseHelper.get_valorant_match_stats(match_id)
                                game = game_obj or await DatabaseHelper.get_game(game_id)
                                if game and stats:
                                    logger.info(f"Match #{match_id}: Generating scoreboard (game_channel={game.game_channel_id})")
                                    scoreboard_embed, scoreboard_file = await self._generate_match_scoreboard(guild, match_id)
                                    logger.info(f"Match #{match_id}: Scoreboard generated (has_file={scoreboard_file is not None})")

                                    # Send to game channel
                                    game_channel = guild.get_channel(game.game_channel_id) if game.game_channel_id else None
                                    if game_channel:
                                        if scoreboard_file:
                                            await game_channel.send(
                                                content=f"📊 **Match {short_id}** stats are in!",
                                                embed=scoreboard_embed,
                                                file=scoreboard_file
                                            )
                                        else:
                                            await game_channel.send(
                                                content=f"📊 **Match {short_id}** stats are in!",
                                                embed=scoreboard_embed
                                            )
                                        logger.info(f"Match #{match_id}: Scoreboard sent to game channel")

                                    # Scoreboard is only sent to the game channel, not the log channel
                            except Exception as e:
                                logger.error(f"Match #{match_id}: Failed to send stats scoreboard: {e}", exc_info=True)
                    else:
                        # Check if this failure was due to a transient API outage (5xx).
                        # If so, don't burn a retry attempt — reschedule with the same count.
                        import time as _time
                        is_transient = (
                            'transient' in reason
                            or (_time.monotonic() - self.henrik_api._last_transient_error_at) < 120
                        )

                        if attempt >= total_attempts and not is_transient:
                            # All retries exhausted
                            await DatabaseHelper.update_stats_retry(
                                match_id, status='exhausted',
                                attempt_count=attempt, last_reason=reason
                            )
                            logger.warning(f"Match #{match_id}: Stats fetch exhausted after {attempt} attempts ({reason})")
                            if guild:
                                await self.log_action(
                                    guild,
                                    f"Match {short_id}: Failed to fetch Valorant stats after {attempt} attempts ({reason})"
                                )
                                await self._send_stats_failure_notification(guild, match_id, game_id, reason)
                        else:
                            # Schedule next attempt; preserve attempt count on transient errors
                            saved_attempt = attempt - 1 if is_transient else attempt
                            next_delay = retry_delays[min(saved_attempt, len(retry_delays) - 1)]
                            next_at = datetime.now(timezone.utc) + timedelta(seconds=next_delay)
                            await DatabaseHelper.update_stats_retry(
                                match_id, attempt_count=saved_attempt,
                                next_attempt_at=next_at, last_reason=reason
                            )
                            if is_transient:
                                logger.info(f"Match #{match_id}: Transient API error — not counting against retry budget, next in {next_delay}s")
                            else:
                                logger.info(f"Match #{match_id}: Attempt {attempt}/{total_attempts} failed ({reason}), next in {next_delay}s")
                            # After ~15 min of non-transient failures, alert admins
                            if attempt == 5 and not is_transient and guild:
                                await self._send_early_stats_alert(guild, match_id, game_id, reason)

            except asyncio.CancelledError:
                logger.info("Stats retry poll task cancelled")
                return
            except Exception as e:
                logger.error(f"Stats retry poll error: {e}")

    async def _send_early_stats_alert(self, guild: discord.Guild, match_id: int,
                                      game_id: int, reason: str):
        """Send early admin alert after a few failed attempts — still retrying in background."""
        short_id = await self._get_match_short_id(match_id)

        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")
        if not admin_channel_id or not guild:
            return

        channel = guild.get_channel(int(admin_channel_id))
        if not channel:
            return

        match = await DatabaseHelper.get_match(match_id)
        game = await DatabaseHelper.get_game(game_id)
        map_name = match.get("map_name") or "Unknown" if match else "Unknown"

        embed = discord.Embed(
            title=f"Stats Fetch Struggling \u2014 Match {short_id}",
            description=(
                "Auto-fetch hasn't found this match yet after a few attempts.\n"
                "**Still retrying in the background** — if you have a tracker.gg link for this match, "
                "paste it via the admin panel now to speed things up."
            ),
            color=0xfaa61a
        )
        embed.add_field(name="Map", value=map_name, inline=True)
        embed.add_field(name="Game", value=game.name if game else "?", inline=True)
        embed.add_field(name="Last Error", value=reason[:200], inline=False)
        embed.set_footer(text=f"Match ID: {match_id} \u00b7 Use /custommatch admin \u2192 Fetch Stats")

        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.warning(f"Match #{match_id}: Failed to send early stats alert: {e}")

    async def _send_stats_failure_notification(self, guild: discord.Guild, match_id: int,
                                                game_id: int, reason: str):
        """Send failure notification to cm_admin channel when all retries are exhausted."""
        short_id = await self._get_match_short_id(match_id)

        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")
        if not admin_channel_id or not guild:
            await self.log_action(guild, f"Match {short_id}: Stats fetch failed after all retries ({reason})")
            return

        channel = guild.get_channel(int(admin_channel_id))
        if not channel:
            await self.log_action(guild, f"Match {short_id}: Stats fetch failed after all retries ({reason})")
            return

        match = await DatabaseHelper.get_match(match_id)
        game = await DatabaseHelper.get_game(game_id)
        players = await DatabaseHelper.get_match_players(match_id)
        igns = await DatabaseHelper.get_match_igns(match_id)
        map_name = match.get("map_name") or "Unknown" if match else "Unknown"

        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        def format_player(p):
            pid = p["player_id"]
            if pid in igns:
                return f"`{igns[pid]}`"
            member = guild.get_member(pid)
            return member.display_name if member else f"<@{pid}>"

        red_list = "\n".join(format_player(p) for p in red_players) or "\u2014"
        blue_list = "\n".join(format_player(p) for p in blue_players) or "\u2014"

        embed = discord.Embed(
            title=f"Stats Fetch Failed \u2014 Match {short_id}",
            description="Auto-fetch exhausted all attempts.\nPlease link this match manually with a tracker.gg URL.",
            color=0xf04747
        )
        embed.add_field(name="Map", value=map_name, inline=True)
        embed.add_field(name="Game", value=game.name if game else "?", inline=True)
        embed.add_field(name="Last Error", value=reason[:200], inline=False)
        embed.add_field(name="Red Team", value=red_list, inline=True)
        embed.add_field(name="Blue Team", value=blue_list, inline=True)
        embed.set_footer(text=f"Match ID: {match_id}")

        # Add map thumbnail if available
        if map_name != "Unknown" and game:
            image_url = await self._get_map_image_url(game.name, map_name)
            if image_url:
                embed.set_thumbnail(url=image_url)

        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.warning(f"Match #{match_id}: Failed to send failure notification: {e}")

    async def fetch_valorant_match_stats(
        self, match_id: int, game_id: int, player_ids: List[int], match_end_time: datetime,
        match_created_at: Optional[datetime] = None,
        valorant_match_id: Optional[str] = None,
        guild: discord.Guild = None,
        attempt: int = 0,
        force_refetch: bool = False
    ) -> Tuple[bool, str]:
        """Fetch Valorant stats from HenrikDev API for a completed match. Returns (success, reason)."""
        try:
            sub_count_log = len(await DatabaseHelper.get_match_sub_mappings(match_id) or {})
            logger.info(
                f"Match #{match_id}: fetch_valorant_match_stats called — "
                f"attempt={attempt}, players={len(player_ids)}, subs={sub_count_log}, "
                f"val_match_id={'yes' if valorant_match_id else 'no'}, "
                f"force_refetch={force_refetch}"
            )

            # Skip if stats already exist for this match (prevents duplicates on retry)
            # force_refetch bypasses this to allow correcting a specific player's missing stats
            existing_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
            if existing_stats and not force_refetch:
                logger.info(f"Match #{match_id}: Stats already exist ({len(existing_stats)} players), skipping fetch")
                return True, f"already fetched ({len(existing_stats)} players)"

            # Always use fresh player list from DB (reflects swaps/subs)
            current_players = await DatabaseHelper.get_match_players(match_id)
            player_ids = [p['player_id'] for p in current_players]

            igns = await DatabaseHelper.get_match_igns(match_id)
            if not igns:
                reason = "no IGNs linked for any players"
                logger.info(f"Match #{match_id}: {reason}")
                return False, reason

            # Handle substituted players: keep using the sub player's own IGN since they
            # are the ones who actually played in Valorant. sub_mappings is kept for
            # logging/debugging only — do NOT overwrite the sub's IGN/PUUID.
            sub_mappings = await DatabaseHelper.get_match_sub_mappings(match_id)
            effective_igns = dict(igns)  # copy: player_id -> IGN used for API matching
            if sub_mappings:
                for sub_pid, orig_pid in sub_mappings.items():
                    sub_ign = igns.get(sub_pid, "N/A")
                    logger.info(
                        f"Match #{match_id}: Sub detected — player {sub_pid} replaced {orig_pid}; "
                        f"using sub's own IGN '{sub_ign}' for API matching"
                    )

            players_with_igns = [f"{pid}={ign}" for pid, ign in igns.items()]
            logger.info(f"Match #{match_id}: Found {len(igns)} linked IGNs: {players_with_igns}")

            valorant_match_data = None
            match_record = None  # loaded lazily; used for expected_map and val_match_id recovery

            # If no Valorant match ID provided, try two fallback sources:
            # 1. matches.valorant_match_id column (set on successful fetch)
            # 2. valorant_match_stats rows (set at the same time — survives if the
            #    matches column was never backfilled for older matches)
            if not valorant_match_id:
                match_record = await DatabaseHelper.get_match(match_id)
                if match_record and match_record.get('valorant_match_id'):
                    valorant_match_id = match_record['valorant_match_id']
                    logger.info(f"Match #{match_id}: Using stored valorant_match_id from match record: {valorant_match_id}")

            if not valorant_match_id:
                existing_rows = await DatabaseHelper.get_valorant_match_stats(match_id)
                if existing_rows:
                    uid = next((r['valorant_match_id'] for r in existing_rows if r.get('valorant_match_id')), None)
                    if uid:
                        valorant_match_id = uid
                        logger.info(f"Match #{match_id}: Recovered valorant_match_id from existing stats rows: {valorant_match_id}")
                        # Back-fill match record so this lookup isn't needed again
                        try:
                            tracker_url = f"https://tracker.gg/valorant/match/{valorant_match_id}"
                            await DatabaseHelper.update_match(match_id, valorant_match_id=valorant_match_id, tracker_url=tracker_url)
                        except Exception:
                            pass

            # If a Valorant match ID was provided or found, try a direct details fetch.
            # If that fails (API down, match pruned, stale UUID), fall through to the
            # player-search path rather than giving up — the match may still be findable.
            if valorant_match_id:
                logger.info(f"Match #{match_id}: Using provided Valorant match ID: {valorant_match_id}")
                details = await self.henrik_api.get_match_details(valorant_match_id)
                if details:
                    map_data = details.get('metadata', {}).get('map')
                    if isinstance(map_data, dict):
                        map_name = map_data.get('name')
                    else:
                        map_name = map_data
                    valorant_match_data = {
                        'valorant_match_id': valorant_match_id,
                        'details': details,
                        'map': map_name
                    }
                    logger.info(f"Match #{match_id}: Fetched match details directly (map={map_name})")
                else:
                    logger.warning(
                        f"Match #{match_id}: get_match_details failed for stored UUID "
                        f"'{valorant_match_id}' — falling back to player search"
                    )
                    valorant_match_id = None  # clear so the else-branch runs below

            if not valorant_match_data:
                # Get expected map from the match record (from map vote) for scoring
                if not match_record:
                    match_record = await DatabaseHelper.get_match(match_id)
                expected_map = match_record.get('map_name') if match_record else None

                # Collect all player IGNs for overlap verification
                # Use effective_igns so subs are represented by the original player's IGN
                our_player_igns = {ign for ign in effective_igns.values() if '#' in ign}

                # Get stored PUUIDs for overlap verification and to skip redundant API calls
                stored_puuids = await DatabaseHelper.get_match_puuids(match_id)
                # Use the sub player's own PUUID (do NOT overwrite with original player's PUUID)
                effective_puuids = dict(stored_puuids)

                our_player_puuids = {p for p in effective_puuids.values() if p}

                # Get list of regulars to prioritize
                regulars = await DatabaseHelper.get_valorant_regulars(game_id)
                regular_pids = {r['player_id'] for r in regulars}

                # Collect already-linked Valorant match IDs to exclude from search
                # (exclude current match's own rows so refetches don't self-block)
                exclude_val_ids = set()
                async with DatabaseHelper._get_db() as db:
                    async with db.execute(
                        "SELECT DISTINCT valorant_match_id FROM valorant_match_stats "
                        "WHERE valorant_match_id IS NOT NULL AND match_id != ?",
                        (match_id,)
                    ) as cursor:
                        exclude_val_ids = {row[0] for row in await cursor.fetchall()}

                # Sort players - regulars first, then build eligible list
                sorted_player_ids = sorted(player_ids, key=lambda pid: pid not in regular_pids)
                eligible_pids = [
                    pid for pid in sorted_player_ids
                    if pid in effective_igns and '#' in effective_igns.get(pid, '')
                ]

                if not eligible_pids:
                    return False, "no eligible players with valid IGNs"

                # Pre-resolve PUUIDs for any eligible player that doesn't have one stored.
                # PUUID-based lookups use the faster v3 endpoint and survive name changes.
                # Only do this on the first attempt to avoid wasting API calls on retries.
                if attempt == 1:
                    for pid in eligible_pids:
                        if effective_puuids.get(pid):
                            continue
                        ign = effective_igns[pid]
                        if '#' not in ign:
                            continue
                        name_part, tag_part = ign.rsplit('#', 1)
                        try:
                            acct = await self.henrik_api.get_account(name_part, tag_part)
                            if acct and acct.get('puuid'):
                                resolved = acct['puuid']
                                effective_puuids[pid] = resolved
                                our_player_puuids.add(resolved)
                                await DatabaseHelper.mark_valorant_regular(pid, game_id, ign, puuid=resolved)
                                logger.info(f"Match #{match_id}: Pre-resolved PUUID for '{ign}'")
                        except Exception as e:
                            logger.warning(f"Match #{match_id}: Failed to pre-resolve PUUID for '{ign}': {e}")

                # Tracker ping: only during background auto-fetch (attempt >= 1).
                # For manual/interactive fetches (attempt=0) the match is either old
                # (ping won't help) or just played (already indexed). Pinging adds
                # 8+ seconds per match and would make bulk refetch unusably slow.
                if attempt >= 1 and attempt <= 2:
                    ping_targets = eligible_pids[:2]
                    ping_igns = [effective_igns[pid] for pid in ping_targets if '#' in effective_igns.get(pid, '')]
                    if ping_igns:
                        logger.info(
                            f"Match #{match_id}: Pinging Tracker.gg for "
                            f"{[i for i in ping_igns]} to trigger indexing..."
                        )
                        ping_tasks = []
                        for ign in ping_igns:
                            name_p, tag_p = ign.rsplit('#', 1)
                            ping_tasks.append(self.henrik_api.ping_tracker_for_refresh(name_p, tag_p))
                        ping_results = await asyncio.gather(*ping_tasks, return_exceptions=True)
                        logger.info(f"Match #{match_id}: Tracker ping results: {ping_results}")
                        logger.info(f"Match #{match_id}: Waiting 8s for Tracker/Riot indexing...")
                        await asyncio.sleep(8)

                # Try up to 3 eligible players. During background retry (attempt >= 1)
                # we pause 15s between failures to let the API index the match.
                # For interactive paths (attempt=0) we skip the delays so bulk refetch
                # doesn't time out the Discord interaction.
                logger.info(
                    f"Match #{match_id}: Attempt {attempt} - searching via "
                    f"up to 3 of {len(eligible_pids)} eligible player(s)"
                )

                found_pid = None
                search_pids = eligible_pids[:3]
                for idx, pid in enumerate(search_pids):
                    player_ign = effective_igns[pid]
                    pid_puuid = effective_puuids.get(pid)

                    logger.info(
                        f"Match #{match_id}: Attempt {attempt} [{idx + 1}/{len(search_pids)}] "
                        f"- trying '{player_ign}' (puuid={'yes' if pid_puuid else 'no'})"
                    )

                    match_data = await self.henrik_api.find_and_fetch_match_stats(
                        player_ign, match_id, game_id, match_end_time,
                        match_created_at=match_created_at,
                        our_player_igns=our_player_igns,
                        our_player_puuids=our_player_puuids,
                        regulars=regulars,
                        exclude_val_ids=exclude_val_ids,
                        player_puuid=pid_puuid,
                        expected_map=expected_map
                    )

                    if match_data:
                        valorant_match_data = match_data
                        resolved_puuid = match_data.get('puuid')
                        await DatabaseHelper.mark_valorant_regular(pid, game_id, player_ign, puuid=resolved_puuid)
                        logger.info(f"Match #{match_id}: Found match via '{player_ign}' on attempt {attempt}")
                        found_pid = pid
                        break

                    # In background retries only: wait between players to allow indexing
                    if attempt >= 1 and idx < len(search_pids) - 1:
                        logger.info(f"Match #{match_id}: No result for '{player_ign}', waiting 15s before next player...")
                        await asyncio.sleep(15)

                if not valorant_match_data:
                    reason = f"no match found via {len(search_pids)} player(s) on attempt {attempt}"
                    logger.info(f"Match #{match_id}: {reason}")
                    return False, reason

            # Extract stats for all players in the match
            details = valorant_match_data['details']
            valorant_match_id = valorant_match_data['valorant_match_id']
            map_name = valorant_match_data.get('map')

            # Build lookups of Valorant player data by normalized IGN and by PUUID
            # v2 API structure: details.players.all_players[] or details.players.red/blue[]
            val_players = {}       # {ign_key: player_data}
            val_players_by_puuid = {}  # {puuid: player_data}
            players_data = details.get('players', {})

            def _index_val_player(vp):
                if not isinstance(vp, dict):
                    return
                vp_name = vp.get('name', '')
                vp_tag = vp.get('tag', '')
                if vp_name and vp_tag:
                    key = f"{vp_name}#{vp_tag}".lower()
                    val_players[key] = vp
                vp_puuid = vp.get('puuid')
                if vp_puuid:
                    val_players_by_puuid[vp_puuid] = vp

            # Check for all_players (v2 format)
            if isinstance(players_data, dict) and 'all_players' in players_data:
                for vp in players_data['all_players']:
                    _index_val_player(vp)
            # Check for team-based structure (red/blue)
            elif isinstance(players_data, dict):
                for team_name in ['red', 'blue', 'Red', 'Blue']:
                    if team_name in players_data:
                        for vp in players_data[team_name]:
                            _index_val_player(vp)
            # Check for list format (v4 format)
            elif isinstance(players_data, list):
                for team_data in players_data:
                    for vp in (team_data if isinstance(team_data, list) else [team_data]):
                        _index_val_player(vp)

            # Parse round data for plants, defuses, multi-kills, and economy
            rounds_data = details.get('rounds', [])

            # Build per-player round stats: {ign_key: {plants, defuses, kills_per_round[], econ_spent, econ_loadout}}
            player_round_stats = {}
            for ign_key in val_players:
                player_round_stats[ign_key] = {
                    'plants': 0,
                    'defuses': 0,
                    'first_bloods': 0,
                    'kills_per_round': [],
                    'econ_spent': 0,
                    'econ_loadout': 0
                }

            for rnd in rounds_data:
                # Track kills per round for multi-kill calculation
                round_kills = {}  # ign_key -> kills this round
                # Track all kill events this round to find first blood
                # Each entry: (kill_time_in_round_ms, killer_ign_key)
                round_kill_events = []

                # Get plant/defuse events
                plant_events = rnd.get('plant_events', {})
                defuse_events = rnd.get('defuse_events', {})

                # Plant - v2 format has planted_by with display_name already in "Name#Tag" format
                planted_by = plant_events.get('planted_by', {})
                if planted_by:
                    planter_display = planted_by.get('display_name', '')
                    if planter_display and '#' in planter_display:
                        planter_key = planter_display.lower()
                        if planter_key in player_round_stats:
                            player_round_stats[planter_key]['plants'] += 1

                # Defuse - same format
                defused_by = defuse_events.get('defused_by', {})
                if defused_by:
                    defuser_display = defused_by.get('display_name', '')
                    if defuser_display and '#' in defuser_display:
                        defuser_key = defuser_display.lower()
                        if defuser_key in player_round_stats:
                            player_round_stats[defuser_key]['defuses'] += 1

                # Player stats per round (for economy, kills, and first blood)
                player_stats_list = rnd.get('player_stats', [])
                for ps in player_stats_list:
                    # player_display_name is already in "Name#Tag" format
                    ps_display = ps.get('player_display_name', '')
                    if ps_display and '#' in ps_display:
                        ps_key = ps_display.lower()
                        if ps_key in player_round_stats:
                            # Economy
                            economy = ps.get('economy', {})
                            spent = economy.get('spent', 0)
                            # Handle if spent is a dict with 'overall' key or just an int
                            if isinstance(spent, dict):
                                spent = spent.get('overall', 0) or 0
                            player_round_stats[ps_key]['econ_spent'] += spent or 0
                            player_round_stats[ps_key]['econ_loadout'] += economy.get('loadout_value', 0) or 0

                            # Count kills this round (use 'kills' field if available, otherwise count kill_events)
                            kill_events_list = ps.get('kill_events', [])
                            kills_this_round = ps.get('kills', 0) or len(kill_events_list)
                            round_kills[ps_key] = kills_this_round

                            # Collect kill timings for first blood calculation
                            # kill_events are the kills made BY this player
                            for ke in kill_events_list:
                                t = ke.get('kill_time_in_round', ke.get('kill_time_in_round_in_ms'))
                                if t is not None:
                                    round_kill_events.append((int(t), ps_key))

                # First blood: the kill with the earliest time in the round
                if round_kill_events:
                    round_kill_events.sort(key=lambda x: x[0])
                    fb_key = round_kill_events[0][1]
                    if fb_key in player_round_stats:
                        player_round_stats[fb_key]['first_bloods'] += 1

                # Record kills per round for multi-kill calculation
                for ign_key, kills in round_kills.items():
                    if ign_key in player_round_stats:
                        player_round_stats[ign_key]['kills_per_round'].append(kills)

            # Calculate multi-kills from kills_per_round
            def count_multikills(kills_per_round):
                c2k = c3k = c4k = c5k = 0
                for k in kills_per_round:
                    if k == 2:
                        c2k += 1
                    elif k == 3:
                        c3k += 1
                    elif k == 4:
                        c4k += 1
                    elif k >= 5:
                        c5k += 1
                return c2k, c3k, c4k, c5k

            # Get stored PUUIDs for PUUID-first player matching
            # (effective_puuids may already be built during match search above)
            try:
                effective_puuids
            except NameError:
                stored_puuids = await DatabaseHelper.get_match_puuids(match_id)
                # Use each player's own stored PUUID (do NOT overwrite subs with original's PUUID)
                effective_puuids = dict(stored_puuids)

            async def _save_player_stats(pid, display_ign, vp, matched_key):
                """Extract and save stats for one player. Returns True on success."""
                stats = vp.get('stats', {})
                agent = vp.get('character') or (
                    vp.get('agent', {}).get('name') if isinstance(vp.get('agent'), dict) else vp.get('agent')
                )
                damage = vp.get('damage_made', 0) or stats.get('damage', 0) or stats.get('damage_made', 0) or 0
                round_stats = player_round_stats.get(matched_key, {})
                # First bloods are calculated from kill_events timing in the round loop;
                # the v2 API does not expose first_bloods as a per-player field.
                first_bloods = round_stats.get('first_bloods', 0)
                plants = round_stats.get('plants', 0)
                defuses = round_stats.get('defuses', 0)
                econ_spent = round_stats.get('econ_spent', 0)
                econ_loadout = round_stats.get('econ_loadout', 0)
                c2k, c3k, c4k, c5k = count_multikills(round_stats.get('kills_per_round', []))

                await DatabaseHelper.save_valorant_match_stats(
                    match_id=match_id,
                    valorant_match_id=valorant_match_id,
                    player_id=pid,
                    ign=display_ign,
                    agent=agent,
                    kills=stats.get('kills', 0),
                    deaths=stats.get('deaths', 0),
                    assists=stats.get('assists', 0),
                    headshots=stats.get('headshots', 0),
                    bodyshots=stats.get('bodyshots', 0),
                    legshots=stats.get('legshots', 0),
                    score=stats.get('score', 0),
                    map_name=map_name,
                    damage_dealt=damage,
                    first_bloods=first_bloods,
                    plants=plants,
                    defuses=defuses,
                    c2k=c2k,
                    c3k=c3k,
                    c4k=c4k,
                    c5k=c5k,
                    econ_spent=econ_spent,
                    econ_loadout=econ_loadout
                )
                return True

            # Save stats for each player in our match
            stats_saved = 0
            stats_not_found = []  # list of (pid, lookup_ign, display_ign) tuples
            matched_val_keys = set()  # Track which API players have been claimed
            for pid in player_ids:
                if pid not in igns and pid not in effective_igns:
                    continue

                # Use effective_igns for API matching (original player's IGN for subs)
                lookup_ign = effective_igns.get(pid) or igns.get(pid)
                display_ign = igns.get(pid, lookup_ign)

                # Try PUUID match first (immune to name changes)
                vp = None
                matched_key = None
                pid_puuid = effective_puuids.get(pid)
                if pid_puuid and pid_puuid in val_players_by_puuid:
                    vp = val_players_by_puuid[pid_puuid]
                    # Find the IGN key for round stats lookup
                    vp_name = vp.get('name', '')
                    vp_tag = vp.get('tag', '')
                    if vp_name and vp_tag:
                        matched_key = f"{vp_name}#{vp_tag}".lower()
                        # Detect IGN change: match data is always fresh, stored IGN may be stale
                        api_ign = f"{vp_name}#{vp_tag}"
                        if lookup_ign and api_ign.lower() != lookup_ign.lower():
                            logger.info(
                                f"Match #{match_id}: IGN change detected for pid {pid}: "
                                f"'{lookup_ign}' -> '{api_ign}' (via PUUID match)"
                            )
                            await DatabaseHelper.set_player_ign(pid, game_id, api_ign, puuid=pid_puuid)
                            display_ign = api_ign
                    logger.info(f"Match #{match_id}: PUUID match for pid {pid} -> '{matched_key}'")

                # Fall back to IGN matching
                if not vp:
                    matched_key = find_best_ign_match(lookup_ign, val_players)
                    if matched_key:
                        vp = val_players[matched_key]
                        # Opportunistic PUUID backfill: if we matched by IGN and the Valorant data has a PUUID, store it
                        vp_puuid = vp.get('puuid')
                        if vp_puuid and not pid_puuid:
                            await DatabaseHelper.update_player_puuid(pid, game_id, vp_puuid)
                            logger.info(f"Match #{match_id}: Backfilled PUUID for pid {pid} from IGN match")

                if not vp:
                    stats_not_found.append((pid, lookup_ign, display_ign))
                    logger.info(f"Match #{match_id}: No match found for IGN '{lookup_ign}'")
                    continue

                if matched_key:
                    matched_val_keys.add(matched_key)
                await _save_player_stats(pid, display_ign, vp, matched_key)
                stats_saved += 1

            # --- REMAINDER MATCHING ---
            # If some players weren't matched by PUUID or IGN, try pairing them
            # with unmatched API players by process of elimination
            if stats_not_found:
                unmatched_val_keys = [k for k in val_players if k not in matched_val_keys]

                if unmatched_val_keys and 0 < len(unmatched_val_keys) <= 5 and len(stats_not_found) == len(unmatched_val_keys):
                    logger.info(
                        f"Match #{match_id}: Remainder matching {len(stats_not_found)} unmatched players "
                        f"to {len(unmatched_val_keys)} unmatched API players"
                    )
                    remainder_matched = []
                    for (pid, lookup_ign, display_ign), val_key in zip(stats_not_found, unmatched_val_keys):
                        vp = val_players[val_key]
                        logger.info(f"Match #{match_id}: Remainder match pid {pid} ('{lookup_ign}') -> '{val_key}'")

                        await _save_player_stats(pid, display_ign, vp, val_key)
                        stats_saved += 1

                        # Backfill PUUID
                        vp_puuid = vp.get('puuid')
                        pid_puuid = effective_puuids.get(pid)
                        if vp_puuid and not pid_puuid:
                            await DatabaseHelper.update_player_puuid(pid, game_id, vp_puuid)
                            logger.info(f"Match #{match_id}: Backfilled PUUID for pid {pid} from remainder match")

                        remainder_matched.append((pid, lookup_ign))

                    # Remove successfully matched from stats_not_found
                    stats_not_found = [x for x in stats_not_found if (x[0], x[1]) not in remainder_matched]

            if stats_not_found:
                failed_igns = [x[1] for x in stats_not_found]
                logger.warning(f"Match #{match_id}: Could not find stats for IGNs: {failed_igns}")
                # Log failed IGNs to Discord log channel
                if guild:
                    short_id = await self._get_match_short_id(match_id)
                    failed_list = ", ".join(f"`{ign}`" for ign in failed_igns)
                    await self.log_action(
                        guild,
                        f"Match {short_id}: Could not match IGNs to Valorant data: {failed_list}"
                    )

                    # IGN suggestion: if exactly one Discord player is unmatched and exactly
                    # one Valorant player is unmatched, ask admin if the player changed their IGN
                    unmatched_val_keys = [k for k in val_players if k not in matched_val_keys]
                    if len(stats_not_found) == 1 and len(unmatched_val_keys) == 1:
                        pid, lookup_ign, display_ign = stats_not_found[0]
                        unmatched_vp = val_players[unmatched_val_keys[0]]
                        val_name = unmatched_vp.get('name', '')
                        val_tag = unmatched_vp.get('tag', '')
                        val_ign_display = f"{val_name}#{val_tag}" if val_name and val_tag else unmatched_val_keys[0]
                        val_puuid = unmatched_vp.get('puuid')

                        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")
                        if admin_channel_id:
                            admin_ch = guild.get_channel(int(admin_channel_id))
                            if admin_ch:
                                member = guild.get_member(pid)
                                member_name = member.display_name if member else str(pid)
                                embed = discord.Embed(
                                    title="IGN Mismatch — Stats Not Linked",
                                    description=(
                                        f"**{member_name}** did not receive stats from match **{short_id}**.\n\n"
                                        f"One Valorant player was unlinked in the API data: `{val_ign_display}`\n\n"
                                        f"Did **{member_name}** change their IGN to `{val_ign_display}`?"
                                    ),
                                    color=COLOR_WARNING
                                )
                                view = IGNSuggestionView(
                                    self, match_id, game_id, pid, val_ign_display, val_puuid
                                )
                                try:
                                    await admin_ch.send(embed=embed, view=view)
                                except Exception as e:
                                    logger.warning(f"Match #{match_id}: Failed to send IGN suggestion: {e}")

            logger.info(f"Saved Valorant stats for {stats_saved} players in match #{match_id}")

            # Store valorant_match_id and tracker URL on the match record
            if stats_saved > 0 and valorant_match_id:
                try:
                    tracker_url = f"https://tracker.gg/valorant/match/{valorant_match_id}"
                    async with DatabaseHelper._get_db() as db:
                        await db.execute(
                            "UPDATE matches SET valorant_match_id = ?, tracker_url = ? WHERE match_id = ?",
                            (valorant_match_id, tracker_url, match_id)
                        )
                        await db.commit()
                    logger.info(f"Match #{match_id}: Stored valorant_match_id '{valorant_match_id}' and tracker URL on match record")
                except Exception as e:
                    logger.warning(f"Match #{match_id}: Failed to store valorant_match_id on match record: {e}")

            if stats_saved == 0:
                failed_igns = [x[1] for x in stats_not_found]
                return False, f"no players matched (tried: {failed_igns})"

            # Cross-validate winner against Valorant API data and store round scores
            if guild and stats_saved >= 2:
                try:
                    match_record = await DatabaseHelper.get_match(match_id)
                    voted_winner = match_record.get("winning_team") if match_record else None
                    if voted_winner:
                        # Determine which Valorant team (Red/Blue) won from the API
                        teams_data = details.get('teams', {})
                        val_red_won = None
                        val_red_rounds_count = 0
                        val_blue_rounds_count = 0
                        if isinstance(teams_data, dict):
                            red_rounds = 0
                            blue_rounds = 0
                            for team_key, team_info in teams_data.items():
                                if isinstance(team_info, dict):
                                    if team_key.lower() == 'red':
                                        red_rounds = team_info.get('rounds_won', 0) or team_info.get('rounds', {}).get('won', 0)
                                    elif team_key.lower() == 'blue':
                                        blue_rounds = team_info.get('rounds_won', 0) or team_info.get('rounds', {}).get('won', 0)
                            if red_rounds > 0 or blue_rounds > 0:
                                val_red_won = red_rounds > blue_rounds
                                val_red_rounds_count = red_rounds
                                val_blue_rounds_count = blue_rounds
                        elif isinstance(teams_data, list):
                            for t in teams_data:
                                if isinstance(t, dict) and t.get('team_id', '').lower() == 'red':
                                    val_red_won = t.get('has_won', False)
                                    break

                        # Map bot players to Valorant teams FIRST so we can store
                        # round scores in bot-team order (not raw Valorant order)
                        match_players = await DatabaseHelper.get_match_players(match_id)
                        bot_red_pids = {p["player_id"] for p in match_players if p["team"] == "red"}

                        # Check which Valorant team our bot-red players are on
                        red_on_val_red = 0
                        red_on_val_blue = 0
                        for pid in bot_red_pids:
                            pid_puuid = effective_puuids.get(pid)
                            vp = None
                            if pid_puuid:
                                vp = val_players_by_puuid.get(pid_puuid)
                            if not vp and pid in effective_igns:
                                ign_key = effective_igns[pid].lower()
                                vp = val_players.get(ign_key)
                            if vp:
                                val_team = vp.get('team', '').lower()
                                if val_team == 'red':
                                    red_on_val_red += 1
                                elif val_team == 'blue':
                                    red_on_val_blue += 1

                        bot_red_is_val_red = red_on_val_red >= red_on_val_blue if (red_on_val_red + red_on_val_blue) > 0 else True

                        # Store round scores mapped to BOT team order so the scoreboard
                        # displays them correctly regardless of Valorant side assignments
                        if val_red_rounds_count > 0 or val_blue_rounds_count > 0:
                            if bot_red_is_val_red:
                                stored_red_rounds = val_red_rounds_count
                                stored_blue_rounds = val_blue_rounds_count
                            else:
                                stored_red_rounds = val_blue_rounds_count
                                stored_blue_rounds = val_red_rounds_count
                            try:
                                async with DatabaseHelper._get_db() as db:
                                    await db.execute(
                                        "UPDATE matches SET val_red_rounds = ?, val_blue_rounds = ?, bot_red_is_val_red = ? WHERE match_id = ?",
                                        (stored_red_rounds, stored_blue_rounds, int(bot_red_is_val_red), match_id)
                                    )
                                    await db.commit()
                                logger.info(
                                    f"Match #{match_id}: Stored round scores: bot-red={stored_red_rounds}, bot-blue={stored_blue_rounds} "
                                    f"(bot_red_is_val_red={bot_red_is_val_red})"
                                )
                            except Exception as e:
                                logger.warning(f"Match #{match_id}: Failed to store round scores: {e}")

                        if val_red_won is not None and (red_on_val_red > 0 or red_on_val_blue > 0):
                            # If bot-red = val-red, then val_red_won means bot-red won
                            # If bot-red = val-blue, then val_red_won means bot-blue won
                            val_suggests_red_won = val_red_won if bot_red_is_val_red else not val_red_won
                            val_suggested_winner = "red" if val_suggests_red_won else "blue"

                            if val_suggested_winner != voted_winner:
                                short_id = await self._get_match_short_id(match_id)
                                await self.log_action(
                                    guild,
                                    f"**Match {short_id}: Possible incorrect winner** — "
                                    f"Valorant data suggests **{val_suggested_winner}** won but "
                                    f"**{voted_winner}** was recorded as winner."
                                )
                                logger.warning(
                                    f"Match #{match_id}: Winner mismatch — Valorant suggests "
                                    f"{val_suggested_winner} but {voted_winner} was voted"
                                )
                except Exception as e:
                    logger.warning(f"Match #{match_id}: Error during winner cross-validation: {e}")

            if stats_not_found:
                failed_names = ", ".join(x[1] for x in stats_not_found)
                return True, f"success ({stats_saved} players, {len(stats_not_found)} not found: {failed_names})"
            return True, f"success ({stats_saved} players)"

        except Exception as e:
            logger.error(f"Error fetching Valorant stats for match #{match_id}: {e}", exc_info=True)
            return False, f"exception: {e}"

    async def cancel_match(self, guild: discord.Guild, match_id: int, reason: str = "Admin action",
                           cancelled_by: Optional[int] = None):
        """Cancel a match without updating stats."""
        match = await DatabaseHelper.get_match(match_id)
        if not match:
            return

        await DatabaseHelper.update_match(
            match_id, cancelled=1, ended_at=datetime.now(timezone.utc).isoformat()
        )

        # Stop stats retry so we don't fetch/store stats for a cancelled match
        await DatabaseHelper.update_stats_retry(
            match_id, status='exhausted', last_reason='match cancelled'
        )

        # Cancel timeout
        if match_id in self.match_timeout_tasks:
            self.match_timeout_tasks[match_id].cancel()
            del self.match_timeout_tasks[match_id]

        await self.cleanup_match(guild, match)

        game = await DatabaseHelper.get_game(match["game_id"])
        short_id = match.get("short_id") or str(match_id)

        # Build detailed log message
        log_msg = f"Match {short_id} cancelled. Reason: {reason}"
        if cancelled_by:
            log_msg += f" (by <@{cancelled_by}>)"
        await self.log_action(guild, log_msg)
    
    # -------------------------------------------------------------------------
    # Marvel Rivals: scoreboard screenshot upload pipeline
    # -------------------------------------------------------------------------

    RIVALS_SCREENSHOT_DIR = Path("data/rivals_screenshots")
    RIVALS_MIN_WIDTH = 1000
    RIVALS_MIN_HEIGHT = 500
    RIVALS_CONFIDENCE_THRESHOLD = 0.80

    async def _post_rivals_upload_prompt(self, channel: discord.TextChannel,
                                          match_id: int, game: GameConfig):
        """Post the 'upload your scoreboard' prompt after a Rivals match finalizes."""
        embed = discord.Embed(
            title="Upload the match scoreboard",
            description=(
                "Upload a screenshot of the **post-match scoreboard** (from the menu, "
                "**not during the match**) so we can record everyone's stats.\n\n"
                "**Requirements:**\n"
                "• Must be taken from the end-of-match scoreboard screen\n"
                "• Must be **high quality** — no phone photos of a screen, no cropping, "
                "no filters. Blurry/low-res screenshots will be rejected.\n"
                "• Anyone on either team can upload.\n\n"
                "You have **3 hours** to upload. We'll ping you every 30 minutes "
                "until we get it. If no screenshot is uploaded in time, the channel "
                "will be deleted."
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text=f"Match #{match_id} • Rivals stats")
        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to post Rivals upload prompt in channel {channel.id}: {e}")

    async def _pending_upload_cleanup(self):
        """Periodically remove expired entries from rivals_pending_uploads."""
        while True:
            await asyncio.sleep(60)
            now = datetime.now(timezone.utc)
            expired_keys = [
                k for k, v in self.rivals_pending_uploads.items()
                if v["expires_at"] < now
            ]
            for k in expired_keys:
                self.rivals_pending_uploads.pop(k, None)
                logger.info(f"Cleaned up expired pending upload for channel {k}")

    async def _rivals_upload_reminder_task(self, match_id: int, channel_id: int, guild_id: int):
        """Per-match reminder loop for Rivals scoreboard uploads.

        Pings match players after 1 hour, then every 30 minutes until the
        90-minute deadline. On successful upload, the caller cancels this task.
        On timeout, deletes the match channel and alerts the configured Rivals
        admin channel.
        """
        FIRST_REMINDER_SEC = 60 * 60      # 1 hour before first ping
        SUBSEQUENT_INTERVAL_SEC = 30 * 60  # 30 minutes after that
        TOTAL_WINDOW = timedelta(minutes=90)
        deadline = datetime.now(timezone.utc) + TOTAL_WINDOW

        first_reminder_sent = False

        try:
            while True:
                # Sleep first so we don't immediately ping right after the prompt
                interval = FIRST_REMINDER_SEC if not first_reminder_sent else SUBSEQUENT_INTERVAL_SEC
                sleep_for = min(
                    interval,
                    max(1, int((deadline - datetime.now(timezone.utc)).total_seconds()))
                )
                await asyncio.sleep(sleep_for)

                # Upload already committed? Pending entry was cleared.
                if channel_id not in self.rivals_pending_uploads:
                    return

                now = datetime.now(timezone.utc)
                if now >= deadline:
                    break  # fall through to timeout handling

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    return
                channel = guild.get_channel(channel_id)
                if not channel:
                    # Channel was deleted externally — drop pending state
                    self.rivals_pending_uploads.pop(channel_id, None)
                    return

                # Ping everyone still on the match roster
                try:
                    match_players = await DatabaseHelper.get_match_players(match_id)
                    mentions = " ".join(f"<@{mp['player_id']}>" for mp in match_players)
                    mins_left = max(0, int((deadline - now).total_seconds() // 60))
                    await channel.send(
                        f"{mentions}\nStill waiting on a post-match scoreboard screenshot. "
                        f"**~{mins_left} min left** before this channel is deleted."
                    )
                except Exception as e:
                    logger.error(f"Rivals reminder ping failed for match {match_id}: {e}")
                first_reminder_sent = True

            # --- Timeout path ---
            if channel_id not in self.rivals_pending_uploads:
                return  # upload landed at the very last moment

            guild = self.bot.get_guild(guild_id)
            if not guild:
                self.rivals_pending_uploads.pop(channel_id, None)
                return

            # Grab match + player info before deleting the channel
            match = await DatabaseHelper.get_match(match_id)
            match_players = await DatabaseHelper.get_match_players(match_id)
            short_id = (match or {}).get("short_id") or str(match_id)
            player_mentions = ", ".join(f"<@{mp['player_id']}>" for mp in match_players)

            # Delete the match channel
            channel = guild.get_channel(channel_id)
            if channel:
                try:
                    await channel.delete(reason="Rivals scoreboard upload timed out")
                except Exception as e:
                    logger.error(f"Failed to delete Rivals match channel on timeout: {e}")

            # Also clean up roles / VCs that the usual cleanup_match handles
            if match:
                try:
                    await self.cleanup_match(guild, match)
                except Exception as e:
                    logger.error(f"cleanup_match after Rivals timeout failed: {e}")

            # Alert admin channel
            admin_channel_id = await DatabaseHelper.get_config("rivals_admin_channel_id")
            if admin_channel_id:
                admin_channel = guild.get_channel(int(admin_channel_id))
                if admin_channel:
                    try:
                        embed = discord.Embed(
                            title="Rivals scoreboard upload timed out",
                            description=(
                                f"Match `{short_id}` had no scoreboard uploaded within 90 minutes.\n"
                                f"The match channel has been deleted.\n\n"
                                f"**Players:** {player_mentions or '—'}\n\n"
                                f"You can still add stats for this match via "
                                f"**/cm_settings → Rivals Stats → Correct a match's stats**."
                            ),
                            color=discord.Color.orange(),
                        )
                        await admin_channel.send(embed=embed)
                    except Exception as e:
                        logger.error(f"Failed to alert admin channel of Rivals timeout: {e}")

            # Clear pending state
            self.rivals_pending_uploads.pop(channel_id, None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Rivals reminder task error for match {match_id}: {e}", exc_info=True)
        finally:
            self.rivals_reminder_tasks.pop(match_id, None)

    async def _process_rivals_scoreboard_upload(
        self,
        message: discord.Message,
        match_id: int,
        game_id: int,
        pending_key: Optional[int] = None,
        is_correction: bool = False,
    ) -> bool:
        """Handle a Rivals scoreboard screenshot upload.

        Returns True if the upload produced committed stats, False otherwise
        (rejected, pending review, or errored).
        """
        # Pick first image attachment
        attach = None
        for a in message.attachments:
            ct = (a.content_type or "").lower()
            fn = (a.filename or "").lower()
            if ct.startswith("image/") or fn.endswith((".png", ".jpg", ".jpeg", ".webp")):
                attach = a
                break
        if not attach:
            return False

        if attach.width and attach.height:
            if attach.width < self.RIVALS_MIN_WIDTH or attach.height < self.RIVALS_MIN_HEIGHT:
                try:
                    await message.reply(
                        f"That screenshot is too low-resolution "
                        f"({attach.width}x{attach.height}). We need at least "
                        f"{self.RIVALS_MIN_WIDTH}x{self.RIVALS_MIN_HEIGHT}. "
                        f"Please upload a higher-quality screenshot.",
                        mention_author=False,
                    )
                except Exception:
                    pass
                try:
                    await self._post_rivals_rejection_to_admin(
                        guild=message.guild,
                        match_id=match_id,
                        uploader=message.author,
                        reason=(
                            f"Too low-resolution "
                            f"({attach.width}x{attach.height}, "
                            f"min {self.RIVALS_MIN_WIDTH}x{self.RIVALS_MIN_HEIGHT})"
                        ),
                        message=message,
                        attachment=attach,
                    )
                except Exception as e:
                    logger.error(f"Error posting low-res rejection alert: {e}")
                return False

        if not self.rivals_vision.available:
            try:
                await message.reply(
                    "Rivals stats extraction isn't configured on this bot "
                    "(missing GEMINI_API_KEY). Ping an admin.",
                    mention_author=False,
                )
            except Exception:
                pass
            return False

        # Download the image bytes
        try:
            image_bytes = await attach.read()
        except Exception as e:
            logger.error(f"Failed to read Rivals attachment: {e}")
            return False

        # Save to disk for audit
        self.RIVALS_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ext = ".png" if (attach.content_type or "").endswith("png") else ".jpg"
        saved_path = self.RIVALS_SCREENSHOT_DIR / f"match_{match_id}_{ts}{ext}"
        try:
            saved_path.write_bytes(image_bytes)
        except Exception as e:
            logger.error(f"Failed to save Rivals screenshot to {saved_path}: {e}")

        # Feedback message while Gemini runs.
        # For corrections, reuse the ephemeral message from the correction
        # modal so the whole flow lives in a single edited message.
        ephemeral_msg = None
        if is_correction and pending_key is not None:
            pending_entry = self.rivals_pending_uploads.get(pending_key) or {}
            ephemeral_msg = pending_entry.get("ephemeral_msg")

        progress_msg = None
        queued = self.rivals_vision.queue_depth > 0
        initial_status = (
            "Another scoreboard is being processed — yours is queued..."
            if queued
            else "Reading the scoreboard... (this takes a few seconds)"
        )
        if ephemeral_msg is not None:
            try:
                await ephemeral_msg.edit(
                    content=initial_status,
                    embed=None,
                    view=None,
                )
                progress_msg = ephemeral_msg
            except Exception as e:
                logger.error(f"Failed to edit correction ephemeral: {e}")
                ephemeral_msg = None

        if progress_msg is None:
            try:
                progress_msg = await message.reply(
                    initial_status,
                    mention_author=False,
                )
            except Exception:
                progress_msg = None

        # Status callback so the vision client can relay rate-limit
        # waits / queue progress back to the user in real time.
        _progress = progress_msg  # capture for closure
        async def _on_vision_status(status_text: str):
            if _progress:
                try:
                    await _progress.edit(content=status_text)
                except Exception:
                    pass

        mime = "image/png" if ext == ".png" else "image/jpeg"
        result = await self.rivals_vision.extract_scoreboard(
            image_bytes, mime_type=mime, on_status=_on_vision_status,
        )

        if result is None:
            if progress_msg:
                try:
                    await progress_msg.edit(
                        content="Failed to read the scoreboard (Gemini error). "
                                "Please try uploading a different screenshot."
                    )
                except Exception:
                    pass
            try:
                await self._post_rivals_rejection_to_admin(
                    guild=message.guild,
                    match_id=match_id,
                    uploader=message.author,
                    reason="Gemini failed to parse the scoreboard",
                    message=message,
                    attachment=attach,
                )
            except Exception as e:
                logger.error(f"Error posting Gemini-parse rejection alert: {e}")
            # Refresh expiry so the user has time to retry with a different screenshot
            if pending_key is not None and pending_key in self.rivals_pending_uploads:
                self.rivals_pending_uploads[pending_key]["expires_at"] = (
                    datetime.now(timezone.utc) + timedelta(minutes=15)
                )
            return False

        # Backfill matches.map_name from the scoreboard OCR if it's still
        # NULL at this point — e.g. because the map vote was skipped, ended
        # early, or otherwise never called set_match_map. The Rivals
        # scoreboard header shows the map label, and Gemini extracts it.
        if result.map_name:
            try:
                existing_match = await DatabaseHelper.get_match(match_id)
                if existing_match and not (existing_match.get("map_name") or "").strip():
                    await DatabaseHelper.set_match_map(match_id, result.map_name)
                    logger.info(
                        f"Backfilled matches.map_name='{result.map_name}' "
                        f"for match #{match_id} from scoreboard OCR"
                    )
            except Exception as e:
                logger.error(f"Failed to backfill map_name for match #{match_id}: {e}")

        # Map IGNs → player_ids. Uses the global player_igns table (not just
        # the match roster) so that IGNs manually linked via "Link anyway" on
        # a prior correction resolve on subsequent uploads/corrections instead
        # of getting re-prompted forever.
        ign_to_player = await DatabaseHelper.build_ign_lookup(match_id, game_id)

        rows_out: List[dict] = []
        unmapped: List[str] = []
        for p in result.players:
            pid = resolve_ocr_ign(p.ign or "", ign_to_player)
            if pid is None:
                unmapped.append(p.ign)
                continue
            rows_out.append({
                "player_id": pid,
                "ign": p.ign,
                "role": p.role,
                "team": p.team,
                "kills": p.kills,
                "deaths": p.deaths,
                "assists": p.assists,
                "final_hits": p.final_hits,
                "damage": p.damage,
                "damage_blocked": p.damage_blocked,
                "healing": p.healing,
                "accuracy_pct": p.accuracy_pct,
                "mvp_svp": p.mvp_svp,
                "medals": RivalsVisionClient.medals_to_counts(p.medals),
            })

        mapping_ok = not unmapped and len(rows_out) == len(result.players) and len(rows_out) > 0

        if mapping_ok:
            # Auto-commit
            await DatabaseHelper.save_rivals_match_stats(match_id, rows_out)
            # Self-heal player_igns: only overwrite a stored IGN when the
            # OCR name matched *exactly* (case-insensitive). If the match was
            # fuzzy (unicode normalisation, Levenshtein, etc.) the stored IGN
            # is the authoritative form — e.g. "ãnkh_štatus" should not be
            # replaced by OCR's "ankh_status".
            for row in rows_out:
                try:
                    row_ign = (row.get("ign") or "").strip()
                    pid = row.get("player_id")
                    if row_ign and pid:
                        stored_ign = await DatabaseHelper.get_player_ign(pid, game_id)
                        if not stored_ign or stored_ign.strip().lower() == row_ign.lower():
                            # Exact match or no stored IGN — safe to refresh
                            await DatabaseHelper.set_player_ign(pid, game_id, row_ign)
                        else:
                            logger.debug(
                                f"Skipping IGN self-heal for player {pid}: "
                                f"stored='{stored_ign}' != ocr='{row_ign}' (fuzzy match)"
                            )
                except Exception as ign_err:
                    logger.error(
                        f"Failed to refresh player_igns for {row.get('player_id')}: {ign_err}"
                    )
            upload_id = await DatabaseHelper.record_rivals_upload(
                match_id=match_id,
                uploader_id=message.author.id,
                image_path=str(saved_path),
                gemini_raw_json=result.raw_json,
                confidence=result.confidence,
                status="committed",
            )
            if is_correction:
                await DatabaseHelper.supersede_prior_rivals_uploads(match_id, keep_upload_id=upload_id)
            # Clear pending state + cancel the reminder loop
            if pending_key is not None:
                self.rivals_pending_uploads.pop(pending_key, None)
            if not is_correction:
                reminder_task = self.rivals_reminder_tasks.pop(match_id, None)
                if reminder_task:
                    reminder_task.cancel()

            # Always tear down lobby/roles now that stats are committed —
            # regardless of whether this was a normal upload or a correction.
            # cleanup_match no-ops on already-deleted channels/roles.
            try:
                match_row = await DatabaseHelper.get_match(match_id)
                if match_row and message.guild:
                    await self.cleanup_match(message.guild, match_row)
            except Exception as e:
                logger.error(f"cleanup_match after Rivals upload failed: {e}")

            # Post a brief stats-saved message to the log channel.
            if message.guild:
                await self._send_rivals_stats_log(
                    guild=message.guild,
                    match_id=match_id,
                    rows_out=rows_out,
                    winning_team=result.winning_team,
                    source="correction" if is_correction else "auto-commit",
                    uploader=message.author,
                )

            if is_correction:
                # Terse success for corrections — no results card, no summary.
                if progress_msg:
                    try:
                        await progress_msg.edit(
                            content=f"Corrected stats for match #{match_id}: "
                                    f"{len(rows_out)} rows saved. No issues."
                        )
                    except Exception:
                        pass
                return True

            summary = self._format_rivals_summary(rows_out, result.winning_team)
            if progress_msg:
                try:
                    await progress_msg.edit(
                        content=f"Saved stats for {len(rows_out)} players "
                                f"(confidence {int(result.confidence * 100)}%).\n{summary}"
                    )
                except Exception:
                    pass

            # Render + post the results card
            try:
                red_players = [
                    {**r, "medal_count": sum((r.get("medals") or {}).values())}
                    for r in rows_out if (r.get("team") or "").upper() == "RED"
                ]
                blue_players = [
                    {**r, "medal_count": sum((r.get("medals") or {}).values())}
                    for r in rows_out if (r.get("team") or "").upper() == "BLUE"
                ]
                image_buf = await self.stats_generator.generate_rivals_results_image(
                    red_players=red_players,
                    blue_players=blue_players,
                    winning_team=result.winning_team or "",
                )
                if image_buf:
                    await message.channel.send(
                        file=discord.File(image_buf, filename=f"rivals_match_{match_id}.png")
                    )
            except Exception as e:
                logger.error(f"Failed to render/post Rivals results card: {e}")

            return True

        # Pending review path — unmapped IGNs need admin resolution
        upload_id = await DatabaseHelper.record_rivals_upload(
            match_id=match_id,
            uploader_id=message.author.id,
            image_path=str(saved_path),
            gemini_raw_json=result.raw_json,
            confidence=result.confidence,
            status="pending_review",
        )

        # Fetch match roster for process-of-elimination suggestions
        match_players = await DatabaseHelper.get_match_players(match_id)

        reason_bits = []
        if unmapped:
            reason_bits.append(f"{len(unmapped)} unmapped name(s)")
        if len(rows_out) != len(result.players):
            reason_bits.append(
                f"extracted {len(result.players)} / mapped {len(rows_out)}"
            )
        reason = ", ".join(reason_bits) or "needs review"

        if is_correction:
            # Corrections: edit the ephemeral in place with the resolver view
            try:
                await self._post_rivals_correction_discrepancy(
                    message=message,
                    match_id=match_id,
                    game_id=game_id,
                    result=result,
                    rows_out=rows_out,
                    unmapped=unmapped,
                    upload_id=upload_id,
                    match_players=match_players,
                    winning_team=result.winning_team,
                    initiator_id=(
                        self.rivals_pending_uploads.get(pending_key, {}).get("initiator_id")
                        if pending_key is not None else message.author.id
                    ) or message.author.id,
                    ephemeral_msg=ephemeral_msg,
                )
            except Exception as e:
                logger.error(f"Error posting Rivals correction discrepancy: {e}")
            if ephemeral_msg is None and progress_msg:
                try:
                    await progress_msg.edit(
                        content=f"Correction needs review for match #{match_id}: {reason}."
                    )
                except Exception:
                    pass
            if pending_key is not None:
                self.rivals_pending_uploads.pop(pending_key, None)
            return False

        # Standard upload: close lobby immediately, send resolver to admin channel
        if pending_key is not None:
            self.rivals_pending_uploads.pop(pending_key, None)
        reminder_task = self.rivals_reminder_tasks.pop(match_id, None)
        if reminder_task:
            reminder_task.cancel()

        if progress_msg:
            try:
                await progress_msg.edit(
                    content=f"Scoreboard received. {len(unmapped)} IGN(s) need admin mapping. "
                            f"Lobby closing."
                )
            except Exception:
                pass

        # Close the lobby — we have all the data we need
        try:
            match_row = await DatabaseHelper.get_match(match_id)
            if match_row and message.guild:
                await self.cleanup_match(message.guild, match_row)
        except Exception as e:
            logger.error(f"cleanup_match after Rivals unmapped-IGN upload failed: {e}")

        # Post resolver to admin channel
        try:
            await self._post_rivals_ign_resolver_to_admin(
                guild=message.guild,
                match_id=match_id,
                game_id=game_id,
                upload_id=upload_id,
                result=result,
                rows_out=rows_out,
                unmapped=unmapped,
                match_players=match_players,
                winning_team=result.winning_team,
                image_path=str(saved_path),
            )
        except Exception as e:
            logger.error(f"Error posting Rivals IGN resolver to admin channel: {e}")

        return False

    @staticmethod
    def _format_rivals_summary(rows: List[dict], winning_team: Optional[str]) -> str:
        """Short inline summary for the match channel."""
        if not rows:
            return ""
        lines = []
        winner_line = ""
        if winning_team:
            winner_line = f"**Winner:** {winning_team.upper()}\n"
        # Top K player
        top = max(rows, key=lambda r: (r.get("kills") or 0))
        lines.append(winner_line + f"Top kills: **{top.get('ign') or '?'}** "
                     f"({top.get('kills')}/{top.get('deaths')}/{top.get('assists')})")
        return "\n".join(lines)

    async def _post_rivals_rejection_to_admin(
        self,
        guild: Optional[discord.Guild],
        match_id: int,
        uploader: discord.abc.User,
        reason: str,
        message: Optional[discord.Message] = None,
        attachment: Optional[discord.Attachment] = None,
    ):
        """Post a short alert to the Rivals admin channel when a scoreboard
        upload is silently rejected (low-res, Gemini parse failure, etc.).

        Unlike the pending-review path, these rejections previously died in
        the match channel with no admin visibility — this gives admins a
        chance to follow up if the user needs help."""
        if guild is None:
            return
        admin_channel_id = await DatabaseHelper.get_config("rivals_admin_channel_id")
        if not admin_channel_id:
            return
        channel = guild.get_channel(int(admin_channel_id))
        if channel is None:
            return

        description_bits = [
            f"**Match:** #{match_id}",
            f"**Uploader:** {uploader.mention}",
            f"**Reason:** {reason}",
        ]
        if message is not None:
            try:
                description_bits.append(f"[Jump to message]({message.jump_url})")
            except Exception:
                pass

        embed = discord.Embed(
            title="Rivals scoreboard rejected",
            description="\n".join(description_bits),
            color=discord.Color.red(),
        )

        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to post Rivals rejection alert: {e}")

    async def _post_rivals_ign_resolver_to_admin(
        self,
        guild: discord.Guild,
        match_id: int,
        game_id: int,
        upload_id: int,
        result: "RivalsScoreboardResult",
        rows_out: List[dict],
        unmapped: List[str],
        match_players: List[dict],
        winning_team: Optional[str],
        image_path: Optional[str] = None,
    ):
        """Post a minimal IGN resolver embed + view to the admin channel.

        If image_path is provided and the file exists, the saved screenshot
        is attached as a thumbnail so admins can reference it.
        """
        admin_channel_id = await DatabaseHelper.get_config("rivals_admin_channel_id")
        if not admin_channel_id:
            logger.warning(
                f"Rivals upload for match {match_id} needs review but no "
                f"rivals_admin_channel_id is configured."
            )
            return
        channel = guild.get_channel(int(admin_channel_id))
        if channel is None:
            return

        view = RivalsIGNResolverView(
            cog=self,
            guild=guild,
            match_id=match_id,
            game_id=game_id,
            upload_id=upload_id,
            result=result,
            rows_out=rows_out,
            unmapped=unmapped,
            match_players=match_players,
            winning_team=winning_team,
        )
        embed = view.build_embed()

        # Attach the saved screenshot if available
        screenshot_file = None
        if image_path:
            p = Path(image_path)
            if p.is_file():
                filename = f"scoreboard_{match_id}{p.suffix}"
                screenshot_file = discord.File(str(p), filename=filename)
                embed.set_image(url=f"attachment://{filename}")

        try:
            kwargs = {"embed": embed, "view": view}
            if screenshot_file:
                kwargs["file"] = screenshot_file
            await channel.send(**kwargs)
        except Exception as e:
            logger.error(f"Failed to send Rivals IGN resolver embed: {e}")

    async def _post_rivals_correction_discrepancy(
        self,
        message: discord.Message,
        match_id: int,
        game_id: int,
        result: "RivalsScoreboardResult",
        rows_out: List[dict],
        unmapped: List[str],
        upload_id: int,
        match_players: List[dict],
        winning_team: Optional[str],
        initiator_id: int,
        ephemeral_msg=None,
    ):
        """Discrepancy embed + IGN resolver view for the correction flow.

        If `ephemeral_msg` is provided, edits it in place so the whole
        correction stays in one ephemeral message; otherwise falls back to
        posting in-channel.
        """
        view = None
        if unmapped:
            view = RivalsIGNResolverView(
                cog=self,
                guild=message.guild,
                match_id=match_id,
                game_id=game_id,
                upload_id=upload_id,
                result=result,
                rows_out=rows_out,
                unmapped=unmapped,
                match_players=match_players,
                winning_team=winning_team,
            )
            embed = view.build_embed()
            embed.title = f"Match #{match_id} — correction needs review"
        else:
            embed = discord.Embed(
                title=f"Match #{match_id} — correction needs review",
                description="No automatic issues detected.",
                color=discord.Color.orange(),
            )

        if ephemeral_msg is not None:
            try:
                await ephemeral_msg.edit(
                    content=None,
                    embed=embed,
                    view=view,
                )
                return
            except Exception as e:
                logger.error(
                    f"Failed to edit correction ephemeral with discrepancy: {e}"
                )

        try:
            if view:
                await message.channel.send(embed=embed, view=view)
            else:
                await message.channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to send Rivals correction discrepancy: {e}")

    async def cleanup_match(self, guild: discord.Guild, match: dict, skip_match_channel: bool = False):
        """Clean up match channels, VCs, and roles.

        skip_match_channel=True leaves the match text channel intact.
        Used by the Rivals flow on winner-call so players can still upload
        the scoreboard screenshot after VCs/roles/queue embeds are gone.
        """
        # Delete match channel
        if match["channel_id"] and not skip_match_channel:
            try:
                channel = guild.get_channel(match["channel_id"])
                if channel:
                    await channel.delete()
            except Exception as e:
                logger.warning(f"cleanup_match: failed to delete match channel {match['channel_id']}: {e}")

        # Delete draft channel
        if match["draft_channel_id"]:
            try:
                channel = guild.get_channel(match["draft_channel_id"])
                if channel:
                    await channel.delete()
            except Exception as e:
                logger.warning(f"cleanup_match: failed to delete draft channel {match['draft_channel_id']}: {e}")

        # Delete team VCs if they exist
        if match.get("red_vc_id"):
            try:
                vc = guild.get_channel(match["red_vc_id"])
                if vc:
                    await vc.delete()
            except Exception as e:
                logger.warning(f"cleanup_match: failed to delete red VC {match['red_vc_id']}: {e}")

        if match.get("blue_vc_id"):
            try:
                vc = guild.get_channel(match["blue_vc_id"])
                if vc:
                    await vc.delete()
            except Exception as e:
                logger.warning(f"cleanup_match: failed to delete blue VC {match['blue_vc_id']}: {e}")

        # Delete roles
        if match["red_role_id"]:
            try:
                role = guild.get_role(match["red_role_id"])
                if role:
                    await role.delete()
            except Exception as e:
                logger.warning(f"cleanup_match: failed to delete red role {match['red_role_id']}: {e}")

        if match["blue_role_id"]:
            try:
                role = guild.get_role(match["blue_role_id"])
                if role:
                    await role.delete()
            except Exception as e:
                logger.warning(f"cleanup_match: failed to delete blue role {match['blue_role_id']}: {e}")

        # Delete queue message and teams embed from queue channel
        if match["queue_message_id"] or match.get("queue_teams_msg_id"):
            game = await DatabaseHelper.get_game(match["game_id"])
            if game and game.queue_channel_id:
                channel = guild.get_channel(game.queue_channel_id)
                if channel:
                    if match["queue_message_id"]:
                        try:
                            msg = await channel.fetch_message(match["queue_message_id"])
                            await msg.delete()
                        except Exception:
                            pass
                    if match.get("queue_teams_msg_id"):
                        try:
                            msg = await channel.fetch_message(match["queue_teams_msg_id"])
                            await msg.delete()
                        except Exception:
                            pass

    async def _send_match_results_to_log(
        self, guild: discord.Guild, channel: discord.TextChannel,
        game: GameConfig, match_id: int, winning_team: Team,
        winner_results: List[tuple], loser_results: List[tuple]
    ):
        """Send a concise match results embed to the log channel.

        Each player line: emoji prefix + code span with Name, MMR, change.
        Players who voted get 🔴/🔵 circles; non-voters get a space indent.
        """
        short_id = await self._get_match_short_id(match_id)

        winner_team_name = winning_team.value.title() + " Team"
        loser_team = "blue" if winning_team.value == "red" else "red"
        loser_team_name = loser_team.title() + " Team"
        embed_color = 0xFF4444 if winning_team.value == "red" else 0x4488FF

        # Get vote data for colored circle prefixes
        voter_teams = await DatabaseHelper.get_win_voter_teams(match_id)

        def fmt_line(pid: int, old_mmr: int, change: int) -> str:
            member = guild.get_member(pid)
            name = member.display_name if member else str(pid)
            name = sanitize_for_codeblock(name, fallback=member.name if member else None)
            name = truncate_to_width(name, 13)
            sign = "+" if change >= 0 else "-"
            stat_str = f"{pad_to_width(name, 13)} {old_mmr:>4} {sign}{abs(change)}"

            voted = voter_teams.get(pid)
            if voted == "red":
                return f"🔴 `{stat_str}`"
            elif voted == "blue":
                return f"🔵 `{stat_str}`"
            else:
                return f"⚫ `{stat_str}`"

        winner_lines = [fmt_line(pid, mmr, chg) for pid, mmr, chg in winner_results]
        loser_lines  = [fmt_line(pid, mmr, chg) for pid, mmr, chg in loser_results]

        embed = discord.Embed(
            title=f"Match {short_id} — {game.name}",
            color=embed_color
        )
        embed.add_field(
            name=f"🏆 {winner_team_name}",
            value="\n".join(winner_lines) or "—",
            inline=False
        )
        embed.add_field(
            name=loser_team_name,
            value="\n".join(loser_lines) or "—",
            inline=False
        )
        await channel.send(embed=embed)

    async def _send_rivals_stats_log(
        self,
        guild: discord.Guild,
        match_id: int,
        rows_out: List[dict],
        winning_team: Optional[str],
        source: str,
        uploader: Optional[discord.Member] = None,
    ):
        """Post a brief Rivals stats-saved embed to the configured log channel.

        Fires whenever a Rivals scoreboard successfully commits (auto-commit,
        admin review confirm, or unmapped-IGN resolver final save).
        `source` describes which path committed it (e.g. "auto-commit",
        "admin review", "correction").
        """
        try:
            log_channel_id = await DatabaseHelper.get_config("log_channel_id")
            if not log_channel_id:
                return
            channel = guild.get_channel(int(log_channel_id))
            if channel is None:
                return

            short_id = await self._get_match_short_id(match_id)
            winner_str = (winning_team or "").upper() if winning_team else ""
            if winner_str == "RED":
                color = 0xFF4444
                winner_label = "🔴 Red"
            elif winner_str == "BLUE":
                color = 0x4488FF
                winner_label = "🔵 Blue"
            else:
                color = COLOR_SUCCESS
                winner_label = "—"

            # Top kills
            top_line = ""
            if rows_out:
                top = max(rows_out, key=lambda r: (r.get("kills") or 0))
                top_pid = top.get("player_id")
                top_member = guild.get_member(top_pid) if top_pid else None
                top_name = (
                    safe_display_name(top_member) if top_member
                    else (top.get("ign") or str(top_pid or "?"))
                )
                top_line = (
                    f"**Top kills:** {top_name} "
                    f"({top.get('kills')}/{top.get('deaths')}/{top.get('assists')})"
                )

            description_bits = [
                f"**Players recorded:** {len(rows_out)}",
                f"**Winner:** {winner_label}",
            ]
            if top_line:
                description_bits.append(top_line)
            if uploader:
                description_bits.append(f"**Uploaded by:** {uploader.display_name}")

            embed = discord.Embed(
                title=f"Rivals stats saved — Match {short_id}",
                description="\n".join(description_bits),
                color=color,
            )
            embed.set_footer(text=f"Source: {source}")
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to send Rivals stats log message: {e}")

    async def _send_winner_loser_embed(
        self, channel: discord.TextChannel, game: GameConfig,
        match_id: int, players: List[dict], winning_team: Team
    ):
        """Send match result embed to configured game channel."""
        igns = await DatabaseHelper.get_match_igns(match_id)

        # Separate by team
        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        def format_player(p: dict) -> str:
            pid = p["player_id"]
            if pid in igns:
                return f"`{igns[pid]}`"
            member = channel.guild.get_member(pid)
            return member.display_name if member else f"<@{pid}>"

        red_lines = [format_player(p) for p in red_players]
        blue_lines = [format_player(p) for p in blue_players]

        red_label = "Red Team"
        blue_label = "Blue Team"
        if winning_team == Team.RED:
            red_label += " \u2605 WINNER"
        else:
            blue_label += " \u2605 WINNER"

        red_players_str = "\n".join(red_lines) or "None"
        blue_players_str = "\n".join(blue_lines) or "None"

        # Get map/mode name from match record
        match = await DatabaseHelper.get_match(match_id)
        map_name = match.get('map_name') if match else None
        mode_name = match.get('mode_name') if match else None

        # Fallback to Valorant stats if no map name in match record
        if not map_name:
            val_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
            map_name = val_stats[0].get('map_name') if val_stats and val_stats[0].get('map_name') else None

        # Build title with mode/map name if available
        if mode_name and map_name:
            title = f"{mode_name} on {map_name} - Results"
        elif mode_name:
            title = f"{mode_name} - Results"
        elif map_name:
            title = f"{map_name} - Results"
        else:
            short_id = await self._get_match_short_id(match_id)
            title = f"Match {short_id} - Results"

        embed = discord.Embed(
            title=title,
            color=COLOR_SUCCESS
        )

        # Set map thumbnail if available
        if map_name:
            image_url = await self._get_map_image_url(game.name, map_name)
            if image_url:
                embed.set_thumbnail(url=image_url)

        embed.add_field(
            name=red_label,
            value=red_players_str,
            inline=True
        )
        embed.add_field(
            name=blue_label,
            value=blue_players_str,
            inline=True
        )

        await channel.send(embed=embed)

    # -------------------------------------------------------------------------
    # LEADERBOARD & STATS
    # -------------------------------------------------------------------------

    async def _gather_stats_data(
        self, member: discord.Member, game: GameConfig, monthly: bool,
        guild: discord.Guild = None
    ) -> dict:
        """Gather stats data for image generation."""
        stats = await DatabaseHelper.get_player_stats(member.id, game.game_id)
        valorant_stats = await DatabaseHelper.get_valorant_player_stats(
            member.id, game.game_id, monthly
        )
        recent_matches = await DatabaseHelper.get_player_recent_matches(
            member.id, game.game_id, limit=5, monthly=monthly
        )
        streak_stats = await DatabaseHelper.get_player_streak_stats(
            member.id, game.game_id, monthly
        )
        teammate_stats = await DatabaseHelper.get_all_teammate_stats(
            member.id, game.game_id, monthly
        )

        now = datetime.now(timezone.utc)
        if monthly:
            period_title = f"{now.strftime('%B')}"
            monthly_wins, monthly_losses = await DatabaseHelper.get_player_monthly_wins_losses(
                member.id, game.game_id
            )
            wins = monthly_wins
            losses = monthly_losses
            games_played = wins + losses
        else:
            period_title = "Lifetime Stats"
            wins = stats.wins
            losses = stats.losses
            games_played = stats.games_played

        # Get leaderboard rank
        leaderboard_rank = await DatabaseHelper.get_player_leaderboard_rank(
            member.id, game.game_id, monthly=monthly
        )

        # Build recent matches list
        formatted_matches = []
        for match in recent_matches:
            won = match.get('team') == match.get('winning_team')
            formatted_matches.append({
                'won': won,
                'kills': match.get('kills'),
                'deaths': match.get('deaths'),
                'assists': match.get('assists'),
                'agent': match.get('agent'),
                'map_name': match.get('map_name') or match.get('match_map_name')
            })

        # Resolve teammate names
        def get_teammate_name(player_id: int) -> str:
            if guild:
                m = guild.get_member(player_id)
                if m:
                    name = m.display_name
                    return name[:12] + '...' if len(name) > 15 else name
            return f"User {player_id}"

        best_teammates = []
        for t in teammate_stats.get('best_teammates', []):
            best_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        worst_teammates = []
        for t in teammate_stats.get('worst_teammates', []):
            worst_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        return {
            'player_name': safe_display_name(member),
            'avatar_url': member.display_avatar.url,
            'period_title': period_title,
            'leaderboard_rank': leaderboard_rank,
            'games_played': games_played,
            'wins': wins,
            'losses': losses,
            'total_kills': valorant_stats.get('total_kills', 0),
            'total_deaths': valorant_stats.get('total_deaths', 0),
            'total_assists': valorant_stats.get('total_assists', 0),
            'total_score': valorant_stats.get('total_score', 0),
            'total_damage': valorant_stats.get('total_damage', 0),
            'total_first_bloods': valorant_stats.get('total_first_bloods', 0),
            'hs_percent': valorant_stats.get('hs_percent', 0),
            'longest_win_streak': streak_stats.get('longest_win_streak', 0),
            'longest_loss_streak': streak_stats.get('longest_loss_streak', 0),
            'recent_matches': formatted_matches,
            'agent_stats': valorant_stats.get('agent_stats', {}),
            'map_stats': valorant_stats.get('map_stats', []),
            'best_teammates': best_teammates,
            'worst_teammates': worst_teammates
        }

    async def _gather_simple_stats_data(
        self, member: discord.Member, game: GameConfig, monthly: bool,
        guild: discord.Guild = None
    ) -> dict:
        """Gather simplified stats data for non-Valorant games."""
        stats = await DatabaseHelper.get_player_stats(member.id, game.game_id)
        map_stats = await DatabaseHelper.get_player_map_stats(member.id, game.game_id, monthly)
        recent_matches = await DatabaseHelper.get_player_recent_matches(member.id, game.game_id, limit=10)
        teammate_stats = await DatabaseHelper.get_all_teammate_stats(member.id, game.game_id, monthly)

        if monthly:
            monthly_wins, monthly_losses = await DatabaseHelper.get_player_monthly_wins_losses(
                member.id, game.game_id
            )
        else:
            monthly_wins, monthly_losses = stats.wins, stats.losses

        now = datetime.now(timezone.utc)
        period_title = now.strftime('%B') if monthly else "Lifetime Stats"

        def get_name(player_id: int) -> str:
            if guild:
                m = guild.get_member(player_id)
                if m:
                    n = m.display_name
                    return n[:12] + '...' if len(n) > 15 else n
            return f"User {player_id}"

        best_teammates = [
            {'name': get_name(t['player_id']), 'wins': t['wins'], 'losses': t['losses']}
            for t in teammate_stats.get('best_teammates', [])
        ]
        worst_teammates = [
            {'name': get_name(t['player_id']), 'wins': t['wins'], 'losses': t['losses']}
            for t in teammate_stats.get('worst_teammates', [])
        ]

        formatted_matches = []
        for match in recent_matches:
            won = match.get('team') == match.get('winning_team')
            formatted_matches.append({
                'won': won,
                'map_name': match.get('match_map_name') or match.get('map_name') or 'Unknown',
            })

        return {
            'player_name': safe_display_name(member),
            'avatar_url': member.display_avatar.url,
            'game_name': game.name,
            'period_title': period_title,
            'wins': monthly_wins,
            'losses': monthly_losses,
            'map_stats': map_stats,
            'recent_matches': formatted_matches,
            'best_teammates': best_teammates,
            'worst_teammates': worst_teammates,
        }

    async def _gather_serverstats_data(self, guild: discord.Guild, game: GameConfig, monthly: bool) -> dict:
        """Gather server-wide Valorant stats for the serverstats card (~18 categories)."""
        now = datetime.now(timezone.utc)
        if monthly:
            period_title = now.strftime('%B %Y')
            date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')"
        else:
            period_title = "All-Time"
            date_filter = ""

        def _name(pid):
            m = guild.get_member(pid)
            if m:
                n = sanitize_for_codeblock(m.display_name, fallback=m.name)
                return n[:14] + '..' if len(n) > 16 else n
            return str(pid)

        async with DatabaseHelper._get_db() as db:

            # === OVERVIEW ===
            row = await (await db.execute(f"""
                SELECT COUNT(DISTINCT m.match_id) as total_matches,
                       COUNT(DISTINCT mp.player_id) as total_players
                FROM matches m JOIN match_players mp ON m.match_id = mp.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
            """, (game.game_id,))).fetchone()
            total_matches = row['total_matches'] if row else 0
            total_players = row['total_players'] if row else 0

            row = await (await db.execute(f"""
                SELECT COALESCE(SUM(kills),0) as k, COALESCE(SUM(deaths),0) as d,
                       COALESCE(SUM(assists),0) as a, COALESCE(SUM(headshots),0) as hs,
                       COALESCE(SUM(bodyshots),0) as bs, COALESCE(SUM(legshots),0) as ls,
                       COALESCE(SUM(damage_dealt),0) as dmg, COALESCE(SUM(first_bloods),0) as fb,
                       COALESCE(SUM(c5k),0) as aces, COALESCE(SUM(c4k),0) as c4k,
                       COALESCE(SUM(c3k),0) as c3k, COALESCE(SUM(c2k),0) as c2k,
                       COALESCE(SUM(plants),0) as plants, COALESCE(SUM(defuses),0) as defuses
                FROM valorant_match_stats vms JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (
                        SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id
                        AND v2.player_id = vms.player_id
                    )
            """, (game.game_id,))).fetchone()
            totals = dict(row) if row else {}

            total_shots = totals.get('hs',0) + totals.get('bs',0) + totals.get('ls',0)
            hs_pct = round(totals.get('hs',0) / total_shots * 100) if total_shots > 0 else 0

            # === MAPS (text only, with pick %) ===
            map_rows = await (await db.execute(f"""
                SELECT vms.map_name, COUNT(DISTINCT vms.match_id) as cnt
                FROM valorant_match_stats vms JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL AND vms.map_name IS NOT NULL {date_filter}
                    AND vms.id = (
                        SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id
                        AND v2.player_id = vms.player_id
                    )
                GROUP BY vms.map_name ORDER BY cnt DESC
            """, (game.game_id,))).fetchall()
            maps = [{'name': r['map_name'], 'count': r['cnt'],
                     'pct': round(r['cnt'] / total_matches * 100) if total_matches else 0} for r in map_rows]

            # === AGENTS (text only, top 10) ===
            agent_rows = await (await db.execute(f"""
                SELECT vms.agent, COUNT(*) as cnt
                FROM valorant_match_stats vms JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL AND vms.agent IS NOT NULL {date_filter}
                    AND vms.id = (
                        SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id
                        AND v2.player_id = vms.player_id
                    )
                GROUP BY vms.agent ORDER BY cnt DESC LIMIT 10
            """, (game.game_id,))).fetchall()
            agents = [{'name': r['agent'], 'count': r['cnt']} for r in agent_rows]

            # === LEADERBOARD CATEGORIES (18 tiles) ===
            leaders = []

            # Helper: single-value leaderboard query
            async def _top1(label, query, val_key, fmt=None):
                r = await (await db.execute(query, (game.game_id,))).fetchone()
                if r and r[val_key]:
                    v = r[val_key]
                    leaders.append({'label': label, 'player': _name(r['player_id']),
                                    'value': fmt(v) if fmt else v})

            # Row 1: Wins & Win Rate, Kills, K/D
            # 1. Most Wins
            await _top1("Most Wins", f"""
                SELECT mp.player_id, COUNT(*) as v FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND mp.team = m.winning_team {date_filter}
                GROUP BY mp.player_id ORDER BY v DESC LIMIT 1""", 'v')

            # 2. Best Win Rate (min 5 games)
            r = await (await db.execute(f"""
                SELECT mp.player_id,
                       SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as w,
                       COUNT(*) as g
                FROM match_players mp JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp.player_id HAVING g >= 5
                ORDER BY CAST(w AS FLOAT)/g DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                wr = round(r['w'] / r['g'] * 100)
                leaders.append({'label': 'Best Win Rate', 'player': _name(r['player_id']),
                                'value': f"{wr}% ({r['w']}-{r['g']-r['w']})", 'min_games': 5})

            # 3. Most Kills
            await _top1("Most Kills", f"""
                SELECT player_id, SUM(kills) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v')

            # 4. Best K/D (min 5 games)
            r = await (await db.execute(f"""
                SELECT player_id, SUM(kills) as k, SUM(deaths) as d, COUNT(*) as g
                FROM valorant_match_stats vms JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING g >= 5 AND d > 0
                ORDER BY CAST(k AS FLOAT)/d DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                kd = round(r['k'] / r['d'], 2)
                leaders.append({'label': 'Best K/D', 'player': _name(r['player_id']), 'value': str(kd), 'min_games': 5})

            # Row 2: Combat stats
            # 5. Best KDA (min 5 games, deaths > 0)
            r = await (await db.execute(f"""
                SELECT player_id, SUM(kills) as k, SUM(assists) as a, SUM(deaths) as d, COUNT(*) as g
                FROM valorant_match_stats vms JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING g >= 5 AND d > 0
                ORDER BY CAST(k + a AS FLOAT)/d DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                kda = round((r['k'] + r['a']) / r['d'], 2)
                leaders.append({'label': 'Best KDA', 'player': _name(r['player_id']),
                                'value': str(kda), 'min_games': 5})

            # 6. Most First Bloods
            await _top1("Most First Bloods", f"""
                SELECT player_id, SUM(first_bloods) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # 7. Best Headshot % (min 5 games)
            r = await (await db.execute(f"""
                SELECT player_id, SUM(headshots) as hs,
                       SUM(headshots)+SUM(bodyshots)+SUM(legshots) as total
                FROM valorant_match_stats vms JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING COUNT(*) >= 5 AND total > 0
                ORDER BY CAST(hs AS FLOAT)/total DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                pct = round(r['hs'] / r['total'] * 100)
                leaders.append({'label': 'Best Headshot %', 'player': _name(r['player_id']),
                                'value': f"{pct}%", 'min_games': 5})

            # 8. Most Multi-Kills
            await _top1("Most Multi-Kills", f"""
                SELECT player_id, SUM(c2k + c3k + c4k + c5k) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # Row 3: Assists, Damage, etc.
            # 9. Most Assists
            await _top1("Most Assists", f"""
                SELECT player_id, SUM(assists) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v')

            # 12. Most Damage Dealt
            await _top1("Most Damage", f"""
                SELECT player_id, SUM(damage_dealt) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v', lambda v: f"{v:,}")

            # Row 4: Plants, Defuses, Games Played, Streaks
            # 13. Most Plants
            await _top1("Most Plants", f"""
                SELECT player_id, SUM(plants) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # 14. Most Defuses
            await _top1("Most Defuses", f"""
                SELECT player_id, SUM(defuses) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # 15. Most Games Played
            await _top1("Most Games Played", f"""
                SELECT mp.player_id, COUNT(*) as v FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp.player_id ORDER BY v DESC LIMIT 1""", 'v')

            # 16. Most 3Ks
            await _top1("Most 3Ks", f"""
                SELECT player_id, SUM(c3k) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # 17. Most 4Ks
            await _top1("Most 4Ks", f"""
                SELECT player_id, SUM(c4k) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # 18. Most Aces
            await _top1("Most Aces", f"""
                SELECT player_id, SUM(c5k) as v FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND vms.id = (SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id AND v2.player_id = vms.player_id)
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # 19. Longest Win Streak + Active Win Streak
            streak_query = f"""
                SELECT DISTINCT mp.player_id FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
            """
            all_pids = [r['player_id'] for r in await (await db.execute(streak_query, (game.game_id,))).fetchall()]

            best_streak = 0; best_streak_pid = None
            best_current = 0; best_current_pid = None

            for pid in all_pids:
                rows = await (await db.execute(f"""
                    SELECT CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END as won
                    FROM matches m JOIN match_players mp ON m.match_id = mp.match_id
                    WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    ORDER BY m.decided_at ASC
                """, (pid, game.game_id))).fetchall()

                cur = 0; longest = 0
                for r in rows:
                    if r['won']:
                        cur += 1; longest = max(longest, cur)
                    else:
                        cur = 0
                if longest > best_streak:
                    best_streak = longest; best_streak_pid = pid
                if cur > best_current:
                    best_current = cur; best_current_pid = pid

            if best_streak_pid:
                leaders.append({'label': 'Longest Win Streak', 'player': _name(best_streak_pid),
                                'value': f'{best_streak}W'})
            if best_current_pid and best_current > 1:
                leaders.append({'label': 'Active Win Streak', 'player': _name(best_current_pid),
                                'value': f'{best_current}W'})

        return {
            'period_title': period_title,
            'game_name': game.name,
            'total_matches': total_matches,
            'total_players': total_players,
            'total_kills': totals.get('k', 0),
            'total_deaths': totals.get('d', 0),
            'total_assists': totals.get('a', 0),
            'total_damage': totals.get('dmg', 0),
            'total_aces': totals.get('aces', 0),
            'hs_pct': hs_pct,
            'maps': maps,
            'agents': agents,
            'leaders': leaders,
        }

    async def _gather_rivals_player_extras(
        self, player_id: int, game: GameConfig, monthly: bool
    ) -> dict:
        """Aggregate Rivals-specific per-player stats from rivals_match_stats."""
        date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')" if monthly else ""

        async with DatabaseHelper._get_db() as db:
            row = await (await db.execute(f"""
                SELECT COALESCE(SUM(kills),0) as k,
                       COALESCE(SUM(deaths),0) as d,
                       COALESCE(SUM(assists),0) as a,
                       COALESCE(SUM(final_hits),0) as fh,
                       COALESCE(SUM(damage),0) as dmg,
                       COALESCE(SUM(damage_blocked),0) as blk,
                       COALESCE(SUM(healing),0) as heal,
                       COUNT(*) as g,
                       SUM(CASE WHEN mvp_svp = 'MVP' THEN 1 ELSE 0 END) as mvps,
                       SUM(CASE WHEN mvp_svp = 'SVP' THEN 1 ELSE 0 END) as svps
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE rms.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
            """, (player_id, game.game_id))).fetchone()
            agg = dict(row) if row else {}

            # Favorite role
            fav_row = await (await db.execute(f"""
                SELECT rms.role, COUNT(*) as c FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE rms.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                    AND rms.role IS NOT NULL {date_filter}
                GROUP BY rms.role ORDER BY c DESC LIMIT 1
            """, (player_id, game.game_id))).fetchone()
            favorite_role = fav_row['role'] if fav_row else None

            # Best / last accuracy (single-match)
            acc_best_row = await (await db.execute(f"""
                SELECT MAX(rms.accuracy_pct) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE rms.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                    AND rms.accuracy_pct IS NOT NULL {date_filter}
            """, (player_id, game.game_id))).fetchone()
            best_accuracy = acc_best_row['v'] if acc_best_row and acc_best_row['v'] is not None else None

            last_acc_row = await (await db.execute(f"""
                SELECT rms.accuracy_pct FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE rms.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                    AND rms.accuracy_pct IS NOT NULL {date_filter}
                ORDER BY m.decided_at DESC LIMIT 1
            """, (player_id, game.game_id))).fetchone()
            last_accuracy = last_acc_row['accuracy_pct'] if last_acc_row else None

            # Medals total from medals_json
            medal_rows = await (await db.execute(f"""
                SELECT rms.medals_json FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE rms.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                    AND rms.medals_json IS NOT NULL {date_filter}
            """, (player_id, game.game_id))).fetchall()
            total_medals = 0
            medal_type_totals: Dict[str, int] = {}
            for mr in medal_rows:
                try:
                    d = json.loads(mr['medals_json']) or {}
                    if isinstance(d, dict):
                        for k, v in d.items():
                            try:
                                iv = int(v)
                            except (TypeError, ValueError):
                                continue
                            total_medals += iv
                            medal_type_totals[k] = medal_type_totals.get(k, 0) + iv
                except Exception:
                    pass

        top_medals = sorted(medal_type_totals.items(), key=lambda x: x[1], reverse=True)[:3]

        return {
            'games': agg.get('g', 0) or 0,
            'total_kills': agg.get('k', 0) or 0,
            'total_deaths': agg.get('d', 0) or 0,
            'total_assists': agg.get('a', 0) or 0,
            'total_final_hits': agg.get('fh', 0) or 0,
            'total_damage': agg.get('dmg', 0) or 0,
            'total_blocked': agg.get('blk', 0) or 0,
            'total_healing': agg.get('heal', 0) or 0,
            'total_medals': total_medals,
            'top_medal_types': top_medals,
            'mvps': agg.get('mvps', 0) or 0,
            'svps': agg.get('svps', 0) or 0,
            'favorite_role': favorite_role,
            'best_accuracy': best_accuracy,
            'last_accuracy': last_accuracy,
        }

    def _build_rivals_player_embed_field(self, extras: dict) -> str:
        """Format a Rivals player-stats summary for embed display."""
        g = extras.get('games', 0)
        if g <= 0:
            return "No Marvel Rivals matches tracked yet."
        k = extras['total_kills']; d = extras['total_deaths']; a = extras['total_assists']
        kd = round(k / d, 2) if d > 0 else k
        lines = [
            f"**Games:** {g}  •  **K/D/A:** {k}/{d}/{a}  (K/D {kd})",
            f"**Final Hits:** {extras['total_final_hits']:,}  •  **Damage:** {extras['total_damage']:,}",
            f"**Dmg Blocked:** {extras['total_blocked']:,}  •  **Healing:** {extras['total_healing']:,}",
            f"**Medals:** {extras['total_medals']}  •  **MVPs:** {extras['mvps']}  •  **SVPs:** {extras['svps']}",
        ]
        if extras.get('favorite_role'):
            lines.append(f"**Favorite Role:** {extras['favorite_role']}")
        if extras.get('top_medal_types'):
            top_str = ", ".join(f"{name} ×{cnt}" for name, cnt in extras['top_medal_types'])
            lines.append(f"**Top Medals:** {top_str}")
        if extras.get('best_accuracy') is not None or extras.get('last_accuracy') is not None:
            ba = extras.get('best_accuracy')
            la = extras.get('last_accuracy')
            bits = []
            if ba is not None:
                bits.append(f"best {round(ba)}%")
            if la is not None:
                bits.append(f"last {round(la)}%")
            lines.append(f"**Accuracy:** {'  •  '.join(bits)}")
        return "\n".join(lines)

    async def _gather_rivals_serverstats_data(self, guild: discord.Guild, game: GameConfig, monthly: bool) -> dict:
        """Gather server-wide Marvel Rivals stats from rivals_match_stats."""
        now = datetime.now(timezone.utc)
        if monthly:
            period_title = now.strftime('Marvel Rivals — %B %Y')
            date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')"
        else:
            period_title = "Marvel Rivals — All-Time"
            date_filter = ""

        def _name(pid):
            m = guild.get_member(pid)
            if m:
                n = sanitize_for_codeblock(m.display_name, fallback=m.name)
                return n[:14] + '..' if len(n) > 16 else n
            return str(pid)

        async with DatabaseHelper._get_db() as db:
            # Overview totals
            row = await (await db.execute(f"""
                SELECT COUNT(DISTINCT m.match_id) as total_matches,
                       COUNT(DISTINCT mp.player_id) as total_players
                FROM matches m JOIN match_players mp ON m.match_id = mp.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
            """, (game.game_id,))).fetchone()
            total_matches = row['total_matches'] if row else 0
            total_players = row['total_players'] if row else 0

            row = await (await db.execute(f"""
                SELECT COALESCE(SUM(rms.kills),0) as k,
                       COALESCE(SUM(rms.deaths),0) as d,
                       COALESCE(SUM(rms.assists),0) as a,
                       COALESCE(SUM(rms.final_hits),0) as fh,
                       COALESCE(SUM(rms.damage),0) as dmg,
                       COALESCE(SUM(rms.damage_blocked),0) as blk,
                       COALESCE(SUM(rms.healing),0) as heal
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
            """, (game.game_id,))).fetchone()
            totals = dict(row) if row else {}

            # Sum medals across every medals_json blob
            medal_rows = await (await db.execute(f"""
                SELECT rms.medals_json FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND rms.medals_json IS NOT NULL
            """, (game.game_id,))).fetchall()
            total_medals = 0
            medal_type_totals: Dict[str, int] = {}
            for mr in medal_rows:
                try:
                    d = json.loads(mr['medals_json']) or {}
                    if isinstance(d, dict):
                        for k, v in d.items():
                            try:
                                iv = int(v)
                            except (TypeError, ValueError):
                                continue
                            total_medals += iv
                            medal_type_totals[k] = medal_type_totals.get(k, 0) + iv
                except Exception:
                    pass

            leaders: List[dict] = []

            async def _top1(label, query, val_key, fmt=None):
                r = await (await db.execute(query, (game.game_id,))).fetchone()
                if r and r[val_key]:
                    v = r[val_key]
                    leaders.append({'label': label, 'player': _name(r['player_id']),
                                    'value': fmt(v) if fmt else str(v)})

            # --- Core KDA / Combat ---
            await _top1("Most Kills", f"""
                SELECT player_id, SUM(kills) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v')

            await _top1("Most Deaths", f"""
                SELECT player_id, SUM(deaths) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v')

            await _top1("Most Assists", f"""
                SELECT player_id, SUM(assists) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v')

            await _top1("Most Final Hits", f"""
                SELECT player_id, SUM(final_hits) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v',
                lambda v: f"{v:,}")

            # Best K/D (min 3 games)
            r = await (await db.execute(f"""
                SELECT player_id, SUM(kills) as k, SUM(deaths) as d, COUNT(*) as g
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id HAVING g >= 3 AND d > 0
                ORDER BY CAST(k AS FLOAT)/d DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                kd = round(r['k'] / r['d'], 2)
                leaders.append({'label': 'Best K/D', 'player': _name(r['player_id']), 'value': str(kd)})

            # Best KDA (min 3 games)
            r = await (await db.execute(f"""
                SELECT player_id, SUM(kills) as k, SUM(assists) as a, SUM(deaths) as d, COUNT(*) as g
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id HAVING g >= 3 AND d > 0
                ORDER BY CAST(k + a AS FLOAT)/d DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                kda = round((r['k'] + r['a']) / r['d'], 2)
                leaders.append({'label': 'Best KDA', 'player': _name(r['player_id']), 'value': str(kda)})

            # --- Damage / Tank / Support ---
            await _top1("Most Damage", f"""
                SELECT player_id, SUM(damage) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id ORDER BY v DESC LIMIT 1""", 'v',
                lambda v: f"{v:,}")

            await _top1("Most Dmg Blocked", f"""
                SELECT player_id, SUM(damage_blocked) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v',
                lambda v: f"{v:,}")

            await _top1("Most Healing", f"""
                SELECT player_id, SUM(healing) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v',
                lambda v: f"{v:,}")

            # Top Vanguard (avg dmg blocked, min 2 games as Vanguard)
            r = await (await db.execute(f"""
                SELECT player_id, AVG(damage_blocked) as v, COUNT(*) as g
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND LOWER(rms.role) = 'vanguard'
                GROUP BY player_id HAVING g >= 2 ORDER BY v DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r and r['v']:
                leaders.append({'label': 'Top Vanguard',
                                'player': _name(r['player_id']),
                                'value': f"{int(r['v']):,} avg blk"})

            # Top Duelist (avg damage, min 2 games as Duelist)
            r = await (await db.execute(f"""
                SELECT player_id, AVG(damage) as v, COUNT(*) as g
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND LOWER(rms.role) = 'duelist'
                GROUP BY player_id HAVING g >= 2 ORDER BY v DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r and r['v']:
                leaders.append({'label': 'Top Duelist',
                                'player': _name(r['player_id']),
                                'value': f"{int(r['v']):,} avg dmg"})

            # Top Strategist (avg healing, min 2 games as Strategist)
            r = await (await db.execute(f"""
                SELECT player_id, AVG(healing) as v, COUNT(*) as g
                FROM rivals_match_stats rms JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND LOWER(rms.role) = 'strategist'
                GROUP BY player_id HAVING g >= 2 ORDER BY v DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r and r['v']:
                leaders.append({'label': 'Top Strategist',
                                'player': _name(r['player_id']),
                                'value': f"{int(r['v']):,} avg heal"})

            # --- Medals & MVP ---
            await _top1("Most MVPs", f"""
                SELECT player_id, COUNT(*) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND rms.mvp_svp = 'MVP'
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            await _top1("Most SVPs", f"""
                SELECT player_id, COUNT(*) as v FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND rms.mvp_svp = 'SVP'
                GROUP BY player_id HAVING v > 0 ORDER BY v DESC LIMIT 1""", 'v')

            # Most Total Medals — aggregate in Python from medals_json
            pid_medal_totals: Dict[int, int] = {}
            medal_player_rows = await (await db.execute(f"""
                SELECT rms.player_id, rms.medals_json FROM rivals_match_stats rms
                JOIN matches m ON rms.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    AND rms.medals_json IS NOT NULL
            """, (game.game_id,))).fetchall()
            for mr in medal_player_rows:
                try:
                    d = json.loads(mr['medals_json']) or {}
                    if isinstance(d, dict):
                        tot = sum(int(v) for v in d.values() if isinstance(v, (int, float)))
                        pid_medal_totals[mr['player_id']] = pid_medal_totals.get(mr['player_id'], 0) + tot
                except Exception:
                    pass
            if pid_medal_totals:
                top_pid, top_val = max(pid_medal_totals.items(), key=lambda kv: kv[1])
                if top_val > 0:
                    leaders.append({'label': 'Most Medals',
                                    'player': _name(top_pid),
                                    'value': str(top_val)})

            # Top medal types (up to 3)
            for medal_name, count in sorted(medal_type_totals.items(), key=lambda x: x[1], reverse=True)[:3]:
                leaders.append({'label': f'Most {medal_name}',
                                'player': '—',
                                'value': str(count)})

            # --- Win / Streak ---
            await _top1("Most Wins", f"""
                SELECT mp.player_id, COUNT(*) as v FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND mp.team = m.winning_team {date_filter}
                GROUP BY mp.player_id ORDER BY v DESC LIMIT 1""", 'v')

            r = await (await db.execute(f"""
                SELECT mp.player_id,
                       SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as w,
                       COUNT(*) as g
                FROM match_players mp JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp.player_id HAVING g >= 3
                ORDER BY CAST(w AS FLOAT)/g DESC LIMIT 1
            """, (game.game_id,))).fetchone()
            if r:
                wr = round(r['w'] / r['g'] * 100)
                leaders.append({'label': 'Best Win Rate', 'player': _name(r['player_id']),
                                'value': f"{wr}% ({r['w']}-{r['g']-r['w']})"})

            await _top1("Most Games Played", f"""
                SELECT mp.player_id, COUNT(*) as v FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp.player_id ORDER BY v DESC LIMIT 1""", 'v')

            # Longest win streak
            streak_pids = [r['player_id'] for r in await (await db.execute(f"""
                SELECT DISTINCT mp.player_id FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
            """, (game.game_id,))).fetchall()]
            best_streak = 0; best_streak_pid = None
            for pid in streak_pids:
                rows = await (await db.execute(f"""
                    SELECT CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END as won
                    FROM matches m JOIN match_players mp ON m.match_id = mp.match_id
                    WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                    ORDER BY m.decided_at ASC
                """, (pid, game.game_id))).fetchall()
                cur = 0; longest = 0
                for r in rows:
                    if r['won']:
                        cur += 1; longest = max(longest, cur)
                    else:
                        cur = 0
                if longest > best_streak:
                    best_streak = longest; best_streak_pid = pid
            if best_streak_pid and best_streak > 0:
                leaders.append({'label': 'Longest Win Streak',
                                'player': _name(best_streak_pid),
                                'value': f'{best_streak}W'})

        return {
            'period_title': period_title,
            'game_name': game.name,
            'total_matches': total_matches,
            'total_players': total_players,
            'total_kills': totals.get('k', 0),
            'total_final_hits': totals.get('fh', 0),
            'total_damage': totals.get('dmg', 0),
            'total_blocked': totals.get('blk', 0),
            'total_healing': totals.get('heal', 0),
            'total_medals': total_medals,
            'leaders': leaders,
            'generated_at': now.strftime('%Y-%m-%d %H:%M UTC'),
        }

    async def _build_leaderboard_text_embed(self, guild: discord.Guild, game_id: int,
                                             monthly: bool = True) -> discord.Embed:
        """Build a text-based leaderboard embed (top 20)."""
        game = await DatabaseHelper.get_game(game_id)
        leaderboard = await DatabaseHelper.get_leaderboard(game_id, monthly=monthly, limit=20)
        now = datetime.now(timezone.utc)

        if monthly:
            title = f"{game.name} Leaderboard — {now.strftime('%B')}"
        else:
            title = f"{game.name} Leaderboard — All-time"

        embed = discord.Embed(title=title, color=COLOR_NEUTRAL)

        if not leaderboard:
            embed.description = "No matches played yet."
        else:
            lines = []
            for i, entry in enumerate(leaderboard, 1):
                player_id = entry["player_id"]
                member = guild.get_member(player_id)
                name = member.display_name if member else str(player_id)
                wins = entry["wins"]
                losses = entry["losses"]
                total = wins + losses
                winrate = round((wins / total * 100)) if total > 0 else 0
                lines.append(f"**{i}) {name}**\n> -# {wins}W - {losses}L {winrate}% W/L")
            embed.description = "\n".join(lines)

        return embed

    async def _update_persistent_leaderboard(self, guild: discord.Guild, game: 'GameConfig'):
        """Edit the persistent leaderboard message with fresh data. Self-healing if deleted."""
        channel = guild.get_channel(game.leaderboard_channel_id)
        if not channel:
            return

        embed = await self._build_leaderboard_text_embed(guild, game.game_id, monthly=True)
        is_valorant = 'valorant' in game.name.lower()
        view = PersistentLeaderboardView(self, game.game_id, is_valorant=is_valorant)

        msg = None
        if game.leaderboard_message_id:
            try:
                msg = await channel.fetch_message(game.leaderboard_message_id)
                await msg.edit(embed=embed, view=view)
                return
            except discord.NotFound:
                pass  # Message was deleted, send a new one

        # Self-heal: send new message and save its ID
        try:
            msg = await channel.send(embed=embed, view=view)
            await DatabaseHelper.update_game(game.game_id, leaderboard_message_id=msg.id)
            self.bot.add_view(view, message_id=msg.id)
            # Update the in-memory game config too
            game.leaderboard_message_id = msg.id
        except discord.Forbidden:
            logger.warning(f"No permission to send leaderboard in channel {channel.id}")

    async def _generate_match_scoreboard(self, guild: discord.Guild, match_id: int) -> Tuple[discord.Embed, Optional[discord.File]]:
        """Generate a scoreboard image for a match. Returns (embed, file_or_none)."""
        match = await DatabaseHelper.get_match(match_id)
        if not match:
            return discord.Embed(description="Match not found.", color=COLOR_NEUTRAL), None

        # Branch on game type — Rivals uses the rivals results card
        game = await DatabaseHelper.get_game(match.get('game_id'))
        if game and is_rivals_game(game):
            return await self._generate_rivals_match_scoreboard(guild, match_id, match)

        val_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
        if not val_stats:
            return discord.Embed(description="No stats available for this match.", color=COLOR_NEUTRAL), None

        players = await DatabaseHelper.get_match_players(match_id)
        player_teams = {p['player_id']: p['team'] for p in players}

        winning_team = match.get('winning_team')
        map_name = match.get('map_name') or (val_stats[0].get('map_name') if val_stats else 'Unknown')

        # Retrieve stored round scores
        val_red_rounds = match.get('val_red_rounds') or 0
        val_blue_rounds = match.get('val_blue_rounds') or 0

        red_players_list = []
        blue_players_list = []

        for stat in val_stats:
            player_id = stat['player_id']
            team = player_teams.get(player_id, 'red')

            member = guild.get_member(player_id)
            name = member.display_name if member else stat.get('ign', f'User {player_id}')

            player_data = {
                'name': name,
                'agent': stat.get('agent', '?'),
                'kills': stat.get('kills', 0),
                'deaths': stat.get('deaths', 0),
                'assists': stat.get('assists', 0),
                'score': stat.get('score', 0),
                'damage': stat.get('damage_dealt', 0),
                'first_bloods': stat.get('first_bloods', 0),
                'headshots': stat.get('headshots', 0),
                'bodyshots': stat.get('bodyshots', 0),
                'legshots': stat.get('legshots', 0)
            }

            if team == 'red':
                red_players_list.append(player_data)
            else:
                blue_players_list.append(player_data)

        scoreboard_data = {
            'map_name': map_name,
            'red_score': val_red_rounds,
            'blue_score': val_blue_rounds,
            'red_is_winner': winning_team == 'red',
            'red_players': red_players_list,
            'blue_players': blue_players_list
        }

        image = await self.stats_generator.generate_scoreboard_image(scoreboard_data)
        if image:
            image.seek(0)
            file = discord.File(image, filename='scoreboard.png')
            tracker_url = match.get('tracker_url')
            embed = discord.Embed(title=f"{map_name} - Scoreboard", color=COLOR_WHITE, url=tracker_url if tracker_url else None)
            embed.set_image(url="attachment://scoreboard.png")
            return embed, file
        else:
            return discord.Embed(description="Failed to generate scoreboard.", color=COLOR_NEUTRAL), None

    async def _generate_rivals_match_scoreboard(
        self, guild: discord.Guild, match_id: int, match: dict
    ) -> Tuple[discord.Embed, Optional[discord.File]]:
        """Generate a Marvel Rivals scoreboard image for a match."""
        rivals_stats = await DatabaseHelper.get_rivals_match_stats(match_id)
        if not rivals_stats:
            return discord.Embed(description="No Rivals stats available for this match.", color=COLOR_NEUTRAL), None

        players = await DatabaseHelper.get_match_players(match_id)
        player_teams = {p['player_id']: p['team'] for p in players}
        winning_team = match.get('winning_team') or 'red'

        red_players_list: List[dict] = []
        blue_players_list: List[dict] = []

        for stat in rivals_stats:
            player_id = stat.get('player_id')
            team = player_teams.get(player_id, 'red')
            member = guild.get_member(player_id) if player_id else None
            display = member.display_name if member else (stat.get('ign') or f'User {player_id}')

            medal_count = 0
            medals = stat.get('medals') or {}
            if isinstance(medals, dict):
                for v in medals.values():
                    try:
                        medal_count += int(v)
                    except (TypeError, ValueError):
                        continue

            pdata = {
                'ign': display,
                'role': stat.get('role') or '',
                'mvp_svp': stat.get('mvp_svp'),
                'kills': stat.get('kills', 0) or 0,
                'deaths': stat.get('deaths', 0) or 0,
                'assists': stat.get('assists', 0) or 0,
                'final_hits': stat.get('final_hits', 0) or 0,
                'damage': stat.get('damage', 0) or 0,
                'damage_blocked': stat.get('damage_blocked', 0) or 0,
                'healing': stat.get('healing', 0) or 0,
                'medal_count': medal_count,
            }
            if team == 'red':
                red_players_list.append(pdata)
            else:
                blue_players_list.append(pdata)

        image = await self.stats_generator.generate_rivals_results_image(
            red_players_list, blue_players_list, winning_team
        )
        if image:
            image.seek(0)
            file = discord.File(image, filename='rivals_scoreboard.png')
            map_name = match.get('map_name') or 'Marvel Rivals'
            embed = discord.Embed(title=f"{map_name} - Scoreboard", color=COLOR_WHITE)
            embed.set_image(url="attachment://rivals_scoreboard.png")
            return embed, file
        return discord.Embed(description="Failed to generate Rivals scoreboard.", color=COLOR_NEUTRAL), None

    async def build_stats_embed(self, guild: discord.Guild, user: discord.Member) -> discord.Embed:
        """Build a stats embed for a user."""
        embed = discord.Embed(
            title=f"Stats for {user.display_name}",
            color=COLOR_NEUTRAL
        )
        
        games = await DatabaseHelper.get_all_games()
        
        for game in games:
            stats = await DatabaseHelper.get_player_stats(user.id, game.game_id)
            
            if stats.games_played > 0:
                winrate = (stats.wins / stats.games_played * 100)
                embed.add_field(
                    name=game.name,
                    value=f"Games: {stats.games_played} | Wins: {stats.wins} | Losses: {stats.losses}\nWin Rate: {winrate:.1f}%",
                    inline=False
                )
                
                # Top rivals
                rivals = await DatabaseHelper.get_player_rivalries(user.id, game.game_id)
                if rivals:
                    rival_lines = []
                    for r in rivals[:3]:
                        opponent = guild.get_member(r["opponent_id"])
                        opp_name = opponent.display_name if opponent else str(r["opponent_id"])
                        total = r["wins"] + r["losses"]
                        wr = (r["wins"] / total * 100) if total > 0 else 0
                        rival_lines.append(f"vs {opp_name}: {r['wins']}-{r['losses']} ({wr:.0f}%)")
                    
                    embed.add_field(
                        name=f"Top Rivals ({game.name})",
                        value="\n".join(rival_lines),
                        inline=False
                    )
        
        # Recent matches
        recent = await DatabaseHelper.get_player_recent_matches(user.id)
        if recent:
            match_lines = []
            for m in recent:
                result = "W" if m["winning_team"] == m["team"] else "L"
                date = datetime.fromisoformat(m["decided_at"]).strftime("%b %d")
                match_lines.append(f"{m['game_name']} - {result} - {date}")
            
            embed.add_field(
                name="Recent Matches",
                value="\n".join(match_lines),
                inline=False
            )
        
        if not games or all(
            (await DatabaseHelper.get_player_stats(user.id, g.game_id)).games_played == 0 
            for g in games
        ):
            embed.description = "No matches played yet."
        
        return embed
    
    # -------------------------------------------------------------------------
    # COMMANDS
    # -------------------------------------------------------------------------

    @app_commands.command(name="cm_settings", description="Open the settings panel (Server Admin)")
    async def settings_cmd(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You need Administrator permission.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="Custom Matches Settings",
            description="Configure your custom match system.",
            color=COLOR_NEUTRAL
        )
        
        # Show current config
        log_id = await DatabaseHelper.get_config("log_channel_id")
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")

        config_lines = []
        if log_id:
            ch = interaction.guild.get_channel(int(log_id))
            config_lines.append(f"Log Channel: {ch.mention if ch else 'Not found'}")
        else:
            config_lines.append("Log Channel: Not set")

        if admin_channel_id:
            ch = interaction.guild.get_channel(int(admin_channel_id))
            config_lines.append(f"Admin Channel: {ch.mention if ch else 'Not found'}")
        else:
            config_lines.append("Admin Channel: Not set")

        if admin_role_id:
            role = interaction.guild.get_role(int(admin_role_id))
            config_lines.append(f"CM Admin Role: {role.name if role else 'Not found'}")
        else:
            config_lines.append("CM Admin Role: Not set")
        
        embed.add_field(name="Current Config", value="\n".join(config_lines), inline=False)
        
        # Show games
        games = await DatabaseHelper.get_all_games()
        if games:
            game_lines = []
            for g in games:
                ch = interaction.guild.get_channel(g.queue_channel_id) if g.queue_channel_id else None
                ch_str = ch.mention if ch else "No channel"
                cat = interaction.guild.get_channel(g.category_id) if g.category_id else None
                cat_str = f" | cat: **{cat.name}**" if cat else ""
                game_lines.append(f"**{g.name}** — {g.player_count}p, {g.queue_type.value}, {ch_str}{cat_str}")
            embed.add_field(name="Games (set per-game category via Games → Set Category)", value="\n".join(game_lines), inline=False)
        else:
            embed.add_field(name="Games", value="No games configured.", inline=False)
        
        view = SettingsView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @app_commands.command(name="cm_panel", description="Open the admin panel (CM Admins)")
    async def admin_cmd(self, interaction: discord.Interaction):
        if not await self.is_cm_admin(interaction.user):
            await interaction.response.send_message("You need the CM Admin role.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="Custom Matches Admin Panel",
            description="Manage active matches.",
            color=COLOR_NEUTRAL
        )
        
        matches = await DatabaseHelper.get_active_matches()
        if matches:
            match_lines = []
            for m in matches:
                game = await DatabaseHelper.get_game(m["game_id"])
                game_name = game.name if game else "Unknown"
                short_id = m.get("short_id") or str(m["match_id"])
                match_lines.append(f"Match {short_id} - {game_name}")
            embed.add_field(name="Active Matches", value="\n".join(match_lines), inline=False)
        else:
            embed.add_field(name="Active Matches", value="No active matches.", inline=False)

        # Show queues in ready-check state so admins know Force Start applies to them
        ready_check_lines = []
        for qid, qs in self.queues.items():
            if qs.state == "ready_check":
                g = await DatabaseHelper.get_game(qs.game_id)
                g_name = g.name if g else f"Game {qs.game_id}"
                ready_check_lines.append(f"{g_name} — {len(qs.players)} players readying up")
        if ready_check_lines:
            embed.add_field(name="Active Ready Checks", value="\n".join(ready_check_lines), inline=False)

        view = AdminPanelView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(
        name="cm_verify_rivals_igns",
        description="Verify every stored Marvel Rivals IGN against the API; fix casing + remove invalid ones"
    )
    async def verify_rivals_igns_cmd(self, interaction: discord.Interaction):
        if not await self.is_cm_admin(interaction.user):
            await interaction.response.send_message("You need the CM Admin role.", ephemeral=True)
            return

        if not self.rivals_api.available:
            await interaction.response.send_message(
                "Rivals API key is not configured (`MARVEL_RIVALS_API_KEY`). Cannot verify.",
                ephemeral=True,
            )
            return

        # Find every Rivals game configured
        all_games = await DatabaseHelper.get_all_games()
        rivals_games = [g for g in all_games if is_rivals_game(g)]
        if not rivals_games:
            await interaction.response.send_message(
                "No Marvel Rivals games configured.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        total_checked = 0
        corrected: List[str] = []   # "<display> — old → new"
        removed: List[str] = []     # "<display> — <ign>"
        unchanged = 0
        api_errors = 0

        for game in rivals_games:
            rows = await DatabaseHelper.get_all_player_igns_for_game(game.game_id)
            for player_id, ign in rows:
                total_checked += 1
                try:
                    player = await self.rivals_api.find_player(ign)
                except Exception as e:
                    logger.warning(f"verify_rivals_igns: exception for '{ign}': {e}")
                    player = None
                    api_errors += 1

                member = interaction.guild.get_member(player_id) if interaction.guild else None
                display = member.display_name if member else f"user {player_id}"

                if player is None:
                    # Not found — remove the bad IGN
                    await DatabaseHelper.delete_player_ign(player_id, game.game_id)
                    removed.append(f"{display} — `{ign}`")
                else:
                    canonical = player["name"]
                    if canonical != ign:
                        await DatabaseHelper.set_player_ign(player_id, game.game_id, canonical)
                        corrected.append(f"{display} — `{ign}` → `{canonical}`")
                    else:
                        unchanged += 1

                # Be polite to the API
                await asyncio.sleep(0.2)

        # Build report
        embed = discord.Embed(
            title="Rivals IGN verification — complete",
            color=COLOR_NEUTRAL,
        )
        embed.add_field(name="Checked", value=str(total_checked), inline=True)
        embed.add_field(name="Unchanged", value=str(unchanged), inline=True)
        embed.add_field(name="Corrected", value=str(len(corrected)), inline=True)
        embed.add_field(name="Removed", value=str(len(removed)), inline=True)
        if api_errors:
            embed.add_field(name="API errors", value=str(api_errors), inline=True)

        def _truncate_list(items: List[str], limit: int = 1000) -> str:
            if not items:
                return "None"
            out = []
            running = 0
            for line in items:
                if running + len(line) + 1 > limit:
                    out.append(f"… (+{len(items) - len(out)} more)")
                    break
                out.append(line)
                running += len(line) + 1
            return "\n".join(out)

        embed.add_field(
            name="Corrected IGNs",
            value=_truncate_list(corrected),
            inline=False,
        )
        embed.add_field(
            name="Removed IGNs",
            value=_truncate_list(removed),
            inline=False,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="cm_retry_rivals",
        description="Re-process a Rivals scoreboard from saved data (no re-upload needed)"
    )
    @app_commands.describe(match_id="Match ID or short ID to retry")
    async def retry_rivals_cmd(self, interaction: discord.Interaction, match_id: str):
        if not await self.is_cm_admin(interaction.user):
            await interaction.response.send_message("You need the CM Admin role.", ephemeral=True)
            return

        # Resolve match
        raw = match_id.strip()
        match = None
        if raw.isdigit():
            match = await DatabaseHelper.get_match(int(raw))
        if match is None:
            async with DatabaseHelper._get_db() as db:
                async with db.execute(
                    "SELECT * FROM matches WHERE UPPER(short_id) = UPPER(?) LIMIT 1", (raw,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        match = dict(row)
        if not match:
            await interaction.response.send_message(f"No match found for `{raw}`.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        if not game or not is_rivals_game(game):
            await interaction.response.send_message(f"Match `{raw}` is not a Rivals match.", ephemeral=True)
            return

        # Find the most recent retryable upload
        upload = await DatabaseHelper.get_latest_rivals_upload(match["match_id"])
        if not upload:
            await interaction.response.send_message(
                f"No retryable upload found for match `{raw}`. "
                f"(Only pending_review / timed_out / rejected uploads can be retried.)",
                ephemeral=True,
            )
            return

        if not upload.get("gemini_raw_json"):
            await interaction.response.send_message(
                f"Upload #{upload['upload_id']} has no saved Gemini data to re-process.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Reconstruct the scoreboard result from saved JSON
        result = RivalsScoreboardResult.from_raw_json(upload["gemini_raw_json"])
        if result is None:
            await interaction.followup.send(
                f"Failed to parse saved Gemini data for upload #{upload['upload_id']}.",
                ephemeral=True,
            )
            return

        # Run IGN mapping
        mid = match["match_id"]
        gid = game.game_id
        ign_to_player = await DatabaseHelper.build_ign_lookup(mid, gid)

        rows_out: List[dict] = []
        unmapped: List[str] = []
        for p in result.players:
            pid = resolve_ocr_ign(p.ign or "", ign_to_player)
            if pid is None:
                unmapped.append(p.ign)
                continue
            rows_out.append({
                "player_id": pid,
                "ign": p.ign,
                "role": p.role,
                "team": p.team,
                "kills": p.kills,
                "deaths": p.deaths,
                "assists": p.assists,
                "final_hits": p.final_hits,
                "damage": p.damage,
                "damage_blocked": p.damage_blocked,
                "healing": p.healing,
                "accuracy_pct": p.accuracy_pct,
                "mvp_svp": p.mvp_svp,
                "medals": RivalsVisionClient.medals_to_counts(p.medals),
            })

        match_players = await DatabaseHelper.get_match_players(mid)

        if not unmapped and len(rows_out) == len(result.players) and rows_out:
            # All mapped — auto-commit
            upload_id = upload["upload_id"]
            await DatabaseHelper.save_rivals_match_stats(mid, rows_out)
            for row in rows_out:
                try:
                    row_ign = (row.get("ign") or "").strip()
                    pid = row.get("player_id")
                    if row_ign and pid:
                        stored_ign = await DatabaseHelper.get_player_ign(pid, gid)
                        if not stored_ign or stored_ign.strip().lower() == row_ign.lower():
                            await DatabaseHelper.set_player_ign(pid, gid, row_ign)
                except Exception as e:
                    logger.error(f"IGN refresh failed during retry: {e}")
            await DatabaseHelper.mark_rivals_upload_status(upload_id, "committed")
            await DatabaseHelper.supersede_prior_rivals_uploads(mid, keep_upload_id=upload_id)

            await interaction.followup.send(
                f"All IGNs resolved on retry. **{len(rows_out)}** player rows committed "
                f"for match #{mid}.",
                ephemeral=True,
            )

            # Post results card + log
            try:
                red_players = [
                    {**r, "medal_count": sum((r.get("medals") or {}).values())}
                    for r in rows_out if (r.get("team") or "").upper() == "RED"
                ]
                blue_players = [
                    {**r, "medal_count": sum((r.get("medals") or {}).values())}
                    for r in rows_out if (r.get("team") or "").upper() == "BLUE"
                ]
                image_buf = await self.stats_generator.generate_rivals_results_image(
                    red_players=red_players,
                    blue_players=blue_players,
                    winning_team=result.winning_team or "",
                )
                if image_buf:
                    log_channel_id = await DatabaseHelper.get_config("log_channel_id")
                    if log_channel_id:
                        log_ch = interaction.guild.get_channel(int(log_channel_id))
                        if log_ch:
                            await log_ch.send(
                                file=discord.File(image_buf, filename=f"rivals_match_{mid}.png")
                            )
            except Exception as e:
                logger.error(f"Failed to render results card from retry: {e}")

            await self._send_rivals_stats_log(
                guild=interaction.guild,
                match_id=mid,
                rows_out=rows_out,
                winning_team=result.winning_team,
                source="retry",
            )
            return

        # Still unmapped — post resolver to admin channel
        await self._post_rivals_ign_resolver_to_admin(
            guild=interaction.guild,
            match_id=mid,
            game_id=gid,
            upload_id=upload["upload_id"],
            result=result,
            rows_out=rows_out,
            unmapped=unmapped,
            match_players=match_players,
            winning_team=result.winning_team,
            image_path=upload.get("image_path"),
        )
        await interaction.followup.send(
            f"Retry for match #{mid}: {len(unmapped)} IGN(s) still need mapping. "
            f"Resolver posted to the admin channel.",
            ephemeral=True,
        )

    @app_commands.command(name="cm_stats", description="View player stats")
    @app_commands.describe(
        game="The game to view stats for",
        user="The user to view stats for (defaults to yourself)"
    )
    async def stats_cmd(self, interaction: discord.Interaction, game: str, user: discord.Member = None):
        target = user or interaction.user

        game_config = await DatabaseHelper.get_game(int(game))
        if not game_config:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        is_valorant = game_config.name.lower() == 'valorant'

        # Auto-update Riot ID if needed (non-blocking)
        if is_valorant:
            await self._check_and_update_ign(target.id, game_config.game_id)

        # Try to use image generation if available
        if self.stats_generator.browser:
            await interaction.response.defer()

            if is_valorant:
                # Valorant: full stats card with dropdown (monthly + lifetime + recent matches)
                recent_matches = await DatabaseHelper.get_player_recent_matches(
                    target.id, game_config.game_id, limit=5, monthly=True
                )
                seasonal_data = await self._gather_stats_data(target, game_config, monthly=True, guild=interaction.guild)
                lifetime_data = await self._gather_stats_data(target, game_config, monthly=False, guild=interaction.guild)

                seasonal_image = await self.stats_generator.generate_stats_image(seasonal_data)
                lifetime_image = await self.stats_generator.generate_stats_image(lifetime_data)

                if seasonal_image and lifetime_image:
                    images = {'seasonal': seasonal_image, 'lifetime': lifetime_image}
                    view = StatsImageView(
                        self, target, game_config, images, recent_matches,
                        invoker_id=interaction.user.id, guild=interaction.guild
                    )
                    images['lifetime'].seek(0)
                    file = discord.File(images['lifetime'], filename='stats_lifetime.png')
                    embed = discord.Embed(
                        title=f"{target.display_name} - {game_config.name} Stats",
                        color=COLOR_WHITE
                    )
                    embed.set_image(url="attachment://stats_lifetime.png")
                    await interaction.edit_original_response(embed=embed, attachments=[file], view=view)
                    return
            else:
                # Non-Valorant: simplified stats card starting on all-time
                monthly_data = await self._gather_simple_stats_data(target, game_config, monthly=True, guild=interaction.guild)
                lifetime_data = await self._gather_simple_stats_data(target, game_config, monthly=False, guild=interaction.guild)

                recent_matches = await DatabaseHelper.get_player_recent_matches(
                    target.id, game_config.game_id, limit=5, monthly=False
                )

                if is_rivals_game(game_config):
                    # Rivals: dedicated high-res stats card with all combat stats baked in
                    try:
                        monthly_extras = await self._gather_rivals_player_extras(
                            target.id, game_config, monthly=True
                        )
                        lifetime_extras = await self._gather_rivals_player_extras(
                            target.id, game_config, monthly=False
                        )
                    except Exception as e:
                        logger.error(f"Failed to gather Rivals player extras: {e}")
                        monthly_extras = lifetime_extras = {}

                    monthly_merged = {**monthly_data, **monthly_extras}
                    lifetime_merged = {**lifetime_data, **lifetime_extras}

                    monthly_image = await self.stats_generator.generate_rivals_stats_image(monthly_merged)
                    lifetime_image = await self.stats_generator.generate_rivals_stats_image(lifetime_merged)
                else:
                    monthly_image = await self.stats_generator.generate_simple_stats_image(monthly_data)
                    lifetime_image = await self.stats_generator.generate_simple_stats_image(lifetime_data)

                if monthly_image and lifetime_image:
                    images = {'monthly': monthly_image, 'lifetime': lifetime_image}
                    view = SimpleStatsImageView(
                        self, target, game_config, images,
                        invoker_id=interaction.user.id, guild=interaction.guild,
                        rivals_extras=None,
                        recent_matches=recent_matches,
                    )
                    lifetime_image.seek(0)
                    file = discord.File(lifetime_image, filename='stats_lifetime.png')
                    embed = view._build_embed('lifetime')

                    await interaction.edit_original_response(embed=embed, attachments=[file], view=view)
                    return

        # Fallback to embed-based stats
        stats = await DatabaseHelper.get_player_stats(target.id, game_config.game_id)
        view = PlayerStatsView(self, interaction.guild, target.id, game_config, stats, monthly=True)
        await view.load_data()
        embed = await view.build_embed()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view)
        else:
            await interaction.response.send_message(embed=embed, view=view)

    @stats_cmd.autocomplete('game')
    async def stats_game_autocomplete(self, interaction: discord.Interaction, current: str):
        games = await DatabaseHelper.get_all_games()
        return [
            app_commands.Choice(name=g.name, value=str(g.game_id))
            for g in (games or [])
            if current.lower() in g.name.lower()
        ][:25]

    # -------------------------------------------------------------------------
    # HEAD-TO-HEAD COMMAND
    # -------------------------------------------------------------------------

    async def _gather_h2h_data(
        self, guild: discord.Guild, player_a: int, player_b: int, game: GameConfig
    ) -> dict:
        """Gather all data needed for the H2H card."""
        # H2H record from rivalries table
        rivalry = await DatabaseHelper.get_rivalry(player_a, player_b, game.game_id)
        h2h_a_wins = rivalry[0] if rivalry else 0
        h2h_b_wins = rivalry[1] if rivalry else 0

        # All H2H matches (opposite teams) — includes round data
        h2h_matches = await DatabaseHelper.get_h2h_matches(player_a, player_b, game.game_id)
        match_ids = [m['match_id'] for m in h2h_matches]

        # Teammate matches (same team) — includes round data + match IDs
        teammate_matches = await DatabaseHelper.get_h2h_teammate_match_list(player_a, player_b, game.game_id)
        teammate_match_ids = [m['match_id'] for m in teammate_matches]

        # Teammate win/loss
        tm_games = len(teammate_matches)
        tm_wins = sum(1 for m in teammate_matches if m['a_team'] == m['winning_team'])
        teammate = {'games': tm_games, 'wins': tm_wins}

        # Player display info
        member_a = guild.get_member(player_a)
        member_b = guild.get_member(player_b)
        a_name = member_a.display_name if member_a else f"Player {player_a}"
        b_name = member_b.display_name if member_b else f"Player {player_b}"
        a_avatar = str(member_a.display_avatar.url) if member_a else ''
        b_avatar = str(member_b.display_avatar.url) if member_b else ''

        # Truncate long names
        a_name_display = a_name[:15] + '...' if len(a_name) > 18 else a_name
        b_name_display = b_name[:15] + '...' if len(b_name) > 18 else b_name
        a_name_short = a_name[:8] if len(a_name) > 8 else a_name
        b_name_short = b_name[:8] if len(b_name) > 8 else b_name

        # H2H record per player
        h2h_total = h2h_a_wins + h2h_b_wins
        a_wr = round(h2h_a_wins / h2h_total * 100) if h2h_total > 0 else 0
        b_wr = round(h2h_b_wins / h2h_total * 100) if h2h_total > 0 else 0

        # H2H streak (consecutive wins by one player in H2H matches)
        h2h_streak_text = ''
        if h2h_matches:
            streak_player = None
            streak_count = 0
            for m in h2h_matches:  # already desc by decided_at
                a_won = m['a_team'] == m['winning_team']
                if streak_player is None:
                    streak_player = 'a' if a_won else 'b'
                    streak_count = 1
                elif (streak_player == 'a' and a_won) or (streak_player == 'b' and not a_won):
                    streak_count += 1
                else:
                    break
            if streak_count >= 2:
                streak_name = a_name_short if streak_player == 'a' else b_name_short
                h2h_streak_text = f"{streak_name} on a {streak_count}-game H2H streak"

        # Form: last 5 H2H results (from invoker's perspective)
        form = []
        for m in h2h_matches[:5]:
            a_won = m['a_team'] == m['winning_team']
            form.append('W' if a_won else 'L')

        # Build round count mapping for proper ADR/ACS
        def _round_map(matches):
            rm = {}
            for m in matches:
                red = m.get('val_red_rounds') or 0
                blue = m.get('val_blue_rounds') or 0
                rm[m['match_id']] = (red + blue) if (red + blue) > 0 else 24
            return rm

        h2h_round_map = _round_map(h2h_matches)
        tm_round_map = _round_map(teammate_matches)

        # Helper to build a stat row with highlighting
        def _make_row(label, a_val, b_val, higher_better=True):
            try:
                a_f = float(str(a_val).rstrip('%')) if a_val not in (None, '', '—') else None
                b_f = float(str(b_val).rstrip('%')) if b_val not in (None, '', '—') else None
            except (TypeError, ValueError):
                a_f = b_f = None
            a_better = b_better = False
            if a_f is not None and b_f is not None:
                if higher_better:
                    a_better = a_f > b_f
                    b_better = b_f > a_f
                else:
                    a_better = a_f < b_f
                    b_better = b_f < a_f
            return {
                'label': label,
                'a_val': a_val,
                'b_val': b_val,
                'a_better': a_better,
                'b_better': b_better,
            }

        def _build_valorant_stat_rows(a_stats_list, b_stats_list, round_map):
            """Build stat rows from Valorant match stats with proper per-round calcs."""
            rows = []
            if not a_stats_list and not b_stats_list:
                return rows

            def _avg(stats, key):
                vals = [s.get(key, 0) or 0 for s in stats]
                return round(sum(vals) / len(vals), 1) if vals else 0

            def _sum(stats, key):
                return sum(s.get(key, 0) or 0 for s in stats)

            def _total_rounds(stats):
                return sum(round_map.get(s['match_id'], 24) for s in stats)

            # K/D/A combined as "18.4/12.3/17.1"
            a_kda = f"{_avg(a_stats_list, 'kills')}/{_avg(a_stats_list, 'deaths')}/{_avg(a_stats_list, 'assists')}"
            b_kda = f"{_avg(b_stats_list, 'kills')}/{_avg(b_stats_list, 'deaths')}/{_avg(b_stats_list, 'assists')}"
            # Highlight based on K/D ratio
            a_k, a_d = _sum(a_stats_list, 'kills'), _sum(a_stats_list, 'deaths')
            b_k, b_d = _sum(b_stats_list, 'kills'), _sum(b_stats_list, 'deaths')
            a_kd = a_k / a_d if a_d > 0 else float(a_k)
            b_kd = b_k / b_d if b_d > 0 else float(b_k)
            rows.append({
                'label': 'K / D / A',
                'a_val': a_kda,
                'b_val': b_kda,
                'a_better': a_kd > b_kd,
                'b_better': b_kd > a_kd,
            })

            # K/D Ratio
            rows.append(_make_row('K/D Ratio', round(a_kd, 2), round(b_kd, 2)))

            # HS%
            def _hs_pct(stats):
                hs = sum(s.get('headshots', 0) or 0 for s in stats)
                total = hs + sum(s.get('bodyshots', 0) or 0 for s in stats) + sum(s.get('legshots', 0) or 0 for s in stats)
                return f"{round(hs / total * 100)}%" if total > 0 else "—"
            rows.append(_make_row('HS%', _hs_pct(a_stats_list), _hs_pct(b_stats_list)))

            # ADR (total damage / total rounds)
            a_rounds = _total_rounds(a_stats_list)
            b_rounds = _total_rounds(b_stats_list)
            a_adr = round(_sum(a_stats_list, 'damage_dealt') / a_rounds) if a_rounds > 0 else 0
            b_adr = round(_sum(b_stats_list, 'damage_dealt') / b_rounds) if b_rounds > 0 else 0
            rows.append(_make_row('ADR', a_adr, b_adr))

            # ACS (total score / total rounds)
            a_acs = round(_sum(a_stats_list, 'score') / a_rounds) if a_rounds > 0 else 0
            b_acs = round(_sum(b_stats_list, 'score') / b_rounds) if b_rounds > 0 else 0
            rows.append(_make_row('ACS', a_acs, b_acs))

            # First Bloods
            rows.append(_make_row('First Bloods', _sum(a_stats_list, 'first_bloods'), _sum(b_stats_list, 'first_bloods')))

            # Multi-Kills
            a_multi = _sum(a_stats_list, 'c2k') + _sum(a_stats_list, 'c3k') + _sum(a_stats_list, 'c4k') + _sum(a_stats_list, 'c5k')
            b_multi = _sum(b_stats_list, 'c2k') + _sum(b_stats_list, 'c3k') + _sum(b_stats_list, 'c4k') + _sum(b_stats_list, 'c5k')
            rows.append(_make_row('Multi-Kills', a_multi, b_multi))

            return rows

        def _build_rivals_stat_rows(a_stats_list, b_stats_list):
            """Build stat rows from Rivals match stats."""
            rows = []
            if not a_stats_list and not b_stats_list:
                return rows

            def _avg(stats, key):
                vals = [s.get(key, 0) or 0 for s in stats]
                return round(sum(vals) / len(vals), 1) if vals else 0

            def _sum(stats, key):
                return sum(s.get(key, 0) or 0 for s in stats)

            # K/D/A combined
            a_kda = f"{_avg(a_stats_list, 'kills')}/{_avg(a_stats_list, 'deaths')}/{_avg(a_stats_list, 'assists')}"
            b_kda = f"{_avg(b_stats_list, 'kills')}/{_avg(b_stats_list, 'deaths')}/{_avg(b_stats_list, 'assists')}"
            a_k, a_d = _sum(a_stats_list, 'kills'), _sum(a_stats_list, 'deaths')
            b_k, b_d = _sum(b_stats_list, 'kills'), _sum(b_stats_list, 'deaths')
            a_kd = a_k / a_d if a_d > 0 else float(a_k)
            b_kd = b_k / b_d if b_d > 0 else float(b_k)
            rows.append({
                'label': 'K / D / A',
                'a_val': a_kda,
                'b_val': b_kda,
                'a_better': a_kd > b_kd,
                'b_better': b_kd > a_kd,
            })

            rows.append(_make_row('K/D Ratio', round(a_kd, 2), round(b_kd, 2)))
            rows.append(_make_row('Avg Damage', round(_avg(a_stats_list, 'damage')), round(_avg(b_stats_list, 'damage'))))
            rows.append(_make_row('Avg Healing', round(_avg(a_stats_list, 'healing')), round(_avg(b_stats_list, 'healing'))))
            rows.append(_make_row('Avg Blocked', round(_avg(a_stats_list, 'damage_blocked')), round(_avg(b_stats_list, 'damage_blocked'))))
            rows.append(_make_row('Final Hits', _sum(a_stats_list, 'final_hits'), _sum(b_stats_list, 'final_hits')))

            a_mvps = sum(1 for s in a_stats_list if s.get('mvp_svp') == 'MVP')
            b_mvps = sum(1 for s in b_stats_list if s.get('mvp_svp') == 'MVP')
            rows.append(_make_row('MVPs', a_mvps, b_mvps))

            def _avg_acc(stats):
                vals = [s.get('accuracy_pct') for s in stats if s.get('accuracy_pct') is not None]
                return f"{round(sum(vals) / len(vals))}%" if vals else "—"
            rows.append(_make_row('Accuracy', _avg_acc(a_stats_list), _avg_acc(b_stats_list)))

            return rows

        # Build stat rows from game-specific H2H match stats
        stat_rows = []
        teammate_stat_rows = []

        if is_valorant_game(game):
            if match_ids:
                val_stats = await DatabaseHelper.get_h2h_valorant_stats(player_a, player_b, match_ids)
                a_stats_list = val_stats.get(player_a, [])
                b_stats_list = val_stats.get(player_b, [])
                stat_rows = _build_valorant_stat_rows(a_stats_list, b_stats_list, h2h_round_map)

            if teammate_match_ids:
                tm_val_stats = await DatabaseHelper.get_h2h_valorant_stats(player_a, player_b, teammate_match_ids)
                tm_a_stats = tm_val_stats.get(player_a, [])
                tm_b_stats = tm_val_stats.get(player_b, [])
                teammate_stat_rows = _build_valorant_stat_rows(tm_a_stats, tm_b_stats, tm_round_map)

        elif is_rivals_game(game):
            if match_ids:
                riv_stats = await DatabaseHelper.get_h2h_rivals_stats(player_a, player_b, match_ids)
                a_stats_list = riv_stats.get(player_a, [])
                b_stats_list = riv_stats.get(player_b, [])
                stat_rows = _build_rivals_stat_rows(a_stats_list, b_stats_list)

            if teammate_match_ids:
                tm_riv_stats = await DatabaseHelper.get_h2h_rivals_stats(player_a, player_b, teammate_match_ids)
                tm_a_stats = tm_riv_stats.get(player_a, [])
                tm_b_stats = tm_riv_stats.get(player_b, [])
                teammate_stat_rows = _build_rivals_stat_rows(tm_a_stats, tm_b_stats)

        return {
            'game_name': game.name,
            'a_avatar': a_avatar,
            'b_avatar': b_avatar,
            'a_name': a_name_display,
            'b_name': b_name_display,
            'a_name_short': a_name_short,
            'b_name_short': b_name_short,
            'a_record': f"{h2h_a_wins}W-{h2h_b_wins}L",
            'b_record': f"{h2h_b_wins}W-{h2h_a_wins}L",
            'a_wr': a_wr,
            'b_wr': b_wr,
            'h2h_a_wins': h2h_a_wins,
            'h2h_b_wins': h2h_b_wins,
            'h2h_streak_text': h2h_streak_text,
            'form': form,
            'stat_rows': stat_rows,
            'teammate': teammate,
            'teammate_stat_rows': teammate_stat_rows,
        }

    @app_commands.command(name="cm_h2h", description="Head-to-head comparison between two players")
    @app_commands.describe(
        game="The game to compare stats for",
        user="The player to compare against"
    )
    async def h2h_cmd(self, interaction: discord.Interaction, game: str, user: discord.Member):
        if user.id == interaction.user.id:
            await interaction.response.send_message("You can't compare yourself against yourself.", ephemeral=True)
            return

        game_config = await DatabaseHelper.get_game(int(game))
        if not game_config:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        await interaction.response.defer()

        try:
            data = await self._gather_h2h_data(
                interaction.guild, interaction.user.id, user.id, game_config
            )

            # Check if there are any H2H matches at all
            if data['h2h_a_wins'] + data['h2h_b_wins'] == 0 and data['teammate']['games'] == 0:
                await interaction.followup.send(
                    f"No head-to-head matches found between you and {user.display_name} in **{game_config.name}**.",
                    ephemeral=True
                )
                return

            # Try image generation
            if self.stats_generator.browser:
                image = await self.stats_generator.generate_h2h_image(data)
                if image:
                    image.seek(0)
                    file = discord.File(image, filename='h2h.png')
                    embed = discord.Embed(color=COLOR_WHITE)
                    embed.set_image(url="attachment://h2h.png")
                    await interaction.followup.send(embed=embed, file=file)
                    return

            # Fallback: text embed
            embed = discord.Embed(
                title=f"Head to Head — {game_config.name}",
                description=(
                    f"**{data['a_name']}** vs **{data['b_name']}**\n"
                    f"H2H Record: {data['h2h_a_wins']} — {data['h2h_b_wins']}"
                ),
                color=COLOR_NEUTRAL
            )
            if data.get('h2h_streak_text'):
                embed.add_field(name="H2H Streak", value=data['h2h_streak_text'], inline=False)
            if data.get('teammate', {}).get('games', 0) > 0:
                tm = data['teammate']
                tm_wr = round(tm['wins'] / tm['games'] * 100) if tm['games'] > 0 else 0
                embed.add_field(
                    name="As Teammates",
                    value=f"{tm['games']} games · {tm['wins']}W · {tm_wr}% WR",
                    inline=False
                )
            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Error in cm_h2h: {e}", exc_info=True)
            await interaction.followup.send(
                "An error occurred generating the H2H comparison.", ephemeral=True
            )

    @h2h_cmd.autocomplete('game')
    async def h2h_game_autocomplete(self, interaction: discord.Interaction, current: str):
        games = await DatabaseHelper.get_all_games()
        return [
            app_commands.Choice(name=g.name, value=str(g.game_id))
            for g in (games or [])
            if current.lower() in g.name.lower()
        ][:25]

    @app_commands.command(name="cm_win", description="Report the match winner")
    async def win_cmd(self, interaction: discord.Interaction):
        # Check if in a match channel
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return

        # Block for secondary/arcade matches
        if match.get("is_secondary"):
            await interaction.response.send_message(
                "Arcade matches don't track wins. Use `/cm_arcade_end` to end the match.",
                ephemeral=True
            )
            return

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        total_match_players = len(players)
        needed = (total_match_players // 2) + 1

        # Get existing votes
        votes = await DatabaseHelper.get_win_votes(match["match_id"])
        red_votes = votes.get("red", 0)
        blue_votes = votes.get("blue", 0)

        red_role = interaction.guild.get_role(match["red_role_id"])
        blue_role = interaction.guild.get_role(match["blue_role_id"])

        # Get IGNs and voter IDs for display
        igns = await DatabaseHelper.get_match_igns(match["match_id"])
        voter_ids = await DatabaseHelper.get_win_voter_ids(match["match_id"])

        # Build team lists
        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        def format_player(p: dict) -> str:
            pid = p["player_id"]
            check = "✓ " if pid in voter_ids else "⠀ "
            if pid in igns:
                return f"{check}`{igns[pid]}`"
            member = interaction.guild.get_member(pid)
            name = member.display_name if member else f"<@{pid}>"
            return f"{check}{name}"

        red_lines = [format_player(p) for p in red_players]
        blue_lines = [format_player(p) for p in blue_players]

        total_votes = red_votes + blue_votes
        embed = discord.Embed(
            title="Who Won?",
            description=f"Votes: {total_votes}/{needed}\nCast your vote!",
            color=COLOR_NEUTRAL
        )
        embed.add_field(
            name=f"Red Team ({red_votes} votes)",
            value="\n".join(red_lines) or "None",
            inline=True
        )
        embed.add_field(
            name=f"Blue Team ({blue_votes} votes)",
            value="\n".join(blue_lines) or "None",
            inline=True
        )

        view = WinVoteView(self, match["match_id"])
        # Ping both team roles
        await interaction.response.send_message(
            f"{red_role.mention} {blue_role.mention} - Vote for the winner!",
            embed=embed,
            view=view
        )

    @app_commands.command(name="cm_arcade_end", description="Vote to end this arcade match (3 votes needed)")
    async def arcade_end_cmd(self, interaction: discord.Interaction):
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return

        if not match.get("is_secondary"):
            await interaction.response.send_message(
                "This command is only for arcade matches. Use `/cm_win` instead.",
                ephemeral=True
            )
            return

        if match.get("winning_team") or match.get("cancelled"):
            await interaction.response.send_message("This match has already ended.", ephemeral=True)
            return

        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        match_id = match["match_id"]
        if match_id not in self.arcade_end_votes:
            self.arcade_end_votes[match_id] = set()

        self.arcade_end_votes[match_id].add(interaction.user.id)
        vote_count = len(self.arcade_end_votes[match_id])

        if vote_count >= 3:
            await interaction.response.send_message("Match ended! GG!")
            await self._end_arcade_match(interaction.guild, match_id)
        else:
            await interaction.response.send_message(
                f"Vote to end recorded ({vote_count}/3). Need {3 - vote_count} more.",
                ephemeral=True
            )

    async def _end_arcade_match(self, guild: discord.Guild, match_id: int):
        """End an arcade match — no stats, just cleanup."""
        match = await DatabaseHelper.get_match(match_id)
        if not match or match.get("winning_team") or match.get("cancelled"):
            return

        now = datetime.now(timezone.utc)
        await DatabaseHelper.update_match(
            match_id,
            winning_team="ended",
            ended_at=now.isoformat(),
            decided_at=now.isoformat()
        )

        if match_id in self.match_timeout_tasks:
            self.match_timeout_tasks[match_id].cancel()
            del self.match_timeout_tasks[match_id]

        self.arcade_end_votes.pop(match_id, None)

        # Short delay so the "GG" message is visible before channel deletion
        await asyncio.sleep(3)
        await self.cleanup_match(guild, match)

    @app_commands.command(name="cm_abandon", description="Vote to abandon the current match")
    async def abandon_cmd(self, interaction: discord.Interaction):
        # Check if in a match channel
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return

        # Block for secondary/arcade matches
        if match.get("is_secondary"):
            await interaction.response.send_message(
                "Use `/cm_arcade_end` to end arcade matches.",
                ephemeral=True
            )
            return

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        total_match_players = len(players)
        needed = (total_match_players // 2) + 1  # Majority needed

        # Get existing votes
        current_votes = await DatabaseHelper.get_abandon_votes(match["match_id"])

        embed = discord.Embed(
            title="Abandon Match?",
            description=f"Vote to abandon this match.\nVotes: {current_votes}/{needed}",
            color=COLOR_WARNING
        )
        embed.add_field(
            name="Warning",
            value="If the match is abandoned, no stats will be recorded.",
            inline=False
        )

        view = AbandonVoteView(self, match["match_id"], needed)
        await interaction.response.send_message(embed=embed, view=view)


    @app_commands.command(name="cm_shuffle", description="Vote to reshuffle teams before a match starts")
    async def shuffle_cmd(self, interaction: discord.Interaction):
        user = interaction.user
        is_admin = await self.is_cm_admin(user)

        # Fetch all active matches
        active_matches = await DatabaseHelper.get_active_matches()
        if not active_matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return

        # Filter: admins see all, players see only matches they're in
        visible_matches = []
        for m in active_matches:
            if is_admin:
                game = await DatabaseHelper.get_game(m["game_id"])
                m["_game_name"] = game.name if game else "Unknown"
                visible_matches.append(m)
            else:
                players = await DatabaseHelper.get_match_players(m["match_id"])
                if any(p["player_id"] == user.id for p in players):
                    game = await DatabaseHelper.get_game(m["game_id"])
                    m["_game_name"] = game.name if game else "Unknown"
                    visible_matches.append(m)

        if not visible_matches:
            await interaction.response.send_message(
                "No active matches found that you're a part of.", ephemeral=True
            )
            return

        if len(visible_matches) == 1:
            # Skip dropdown, go straight to started-check
            view = ShuffleStartedCheckView(self, visible_matches[0]["match_id"])
            await interaction.response.send_message(
                "Has the match started yet?", view=view, ephemeral=True
            )
        else:
            view = ShuffleMatchSelectView(self, visible_matches)
            await interaction.response.send_message(
                "Select a match to initiate a team shuffle vote:",
                view=view, ephemeral=True
            )

    async def _initiate_shuffle_vote(self, interaction: discord.Interaction, match_id: int):
        """Open a shuffle vote in the match's lobby channel."""
        match = await DatabaseHelper.get_match(match_id)
        if not match:
            await interaction.response.edit_message(content="Match not found.", view=None)
            return

        if match.get("winning_team") or match.get("cancelled"):
            await interaction.response.edit_message(
                content="This match has already concluded.", view=None
            )
            return

        if match.get("shuffled"):
            await interaction.response.edit_message(
                content="Teams have already been shuffled once this match.", view=None
            )
            return

        players = await DatabaseHelper.get_match_players(match_id)
        needed = max(2, (len(players) // 2) + 1)
        short_id = match.get("short_id") or str(match_id)

        lobby_channel = interaction.guild.get_channel(match["channel_id"]) if match.get("channel_id") else None
        if not lobby_channel:
            await interaction.response.edit_message(
                content="Could not find the lobby channel for this match.", view=None
            )
            return

        # Build vote embed
        embed = discord.Embed(
            title="Team Shuffle Vote",
            description=(
                f"**{interaction.user.display_name}** wants to reshuffle teams.\n\n"
                f"Votes: 0/{needed}\n"
                "Vote below if you want the teams shuffled."
            ),
            color=COLOR_NEUTRAL,
        )
        embed.set_footer(text=f"Match {short_id} • Majority required")

        view = ShuffleVoteView(self, match_id, needed)

        await interaction.response.edit_message(
            content=f"Shuffle vote started in <#{lobby_channel.id}>.", view=None
        )

        await lobby_channel.send(embed=embed, view=view)

        await self.log_action(
            interaction.guild,
            f"Shuffle vote initiated by **{interaction.user.display_name}** for match {short_id}",
        )

    async def handle_shuffle_vote(self, interaction: discord.Interaction,
                                   match_id: int, needed_votes: int):
        """Handle a shuffle vote button press."""
        user = interaction.user

        # Check voter is in the match
        players = await DatabaseHelper.get_match_players(match_id)
        player_ids = [p["player_id"] for p in players]
        if user.id not in player_ids:
            await interaction.response.send_message(
                "You're not in this match.", ephemeral=True
            )
            return

        # Check match state
        match = await DatabaseHelper.get_match(match_id)
        if not match or match.get("winning_team") or match.get("cancelled"):
            await interaction.response.send_message(
                "This match has already concluded.", ephemeral=True
            )
            return
        if match.get("shuffled"):
            await interaction.response.send_message(
                "Teams have already been shuffled.", ephemeral=True
            )
            return

        # Check if already voted
        if await DatabaseHelper.has_voted_shuffle(match_id, user.id):
            await interaction.response.send_message(
                "You've already voted.", ephemeral=True
            )
            return

        # Record vote
        await DatabaseHelper.add_shuffle_vote(match_id, user.id)
        current_votes = await DatabaseHelper.get_shuffle_vote_count(match_id)

        if current_votes >= needed_votes:
            # Majority reached — execute shuffle
            await interaction.response.defer()
            try:
                await interaction.message.edit(view=None)
            except Exception:
                pass
            await self._execute_shuffle(interaction.guild, match_id, interaction.user)
        else:
            # Update embed with new vote count
            short_id = match.get("short_id") or str(match_id)
            voters = await DatabaseHelper.get_shuffle_voters(match_id)
            voter_mentions = []
            for vid in voters:
                m = interaction.guild.get_member(vid)
                voter_mentions.append(m.display_name if m else str(vid))

            embed = discord.Embed(
                title="Team Shuffle Vote",
                description=(
                    f"Votes: {current_votes}/{needed_votes}\n\n"
                    f"Voted: {', '.join(voter_mentions)}"
                ),
                color=COLOR_NEUTRAL,
            )
            embed.set_footer(text=f"Match {short_id} • Majority required")

            view = ShuffleVoteView(self, match_id, needed_votes)
            await interaction.response.edit_message(embed=embed, view=view)

    async def _execute_shuffle(self, guild: discord.Guild, match_id: int,
                                initiator: discord.Member):
        """Execute team shuffle after a successful vote."""
        match = await DatabaseHelper.get_match(match_id)
        if not match or match.get("shuffled") or match.get("winning_team") or match.get("cancelled"):
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        if not game:
            return

        players = await DatabaseHelper.get_match_players(match_id)
        player_ids = [p["player_id"] for p in players]
        current_map = {p["player_id"]: p["team"] for p in players}
        igns = await DatabaseHelper.get_match_igns(match_id)
        short_id = match.get("short_id") or str(match_id)

        # Get new teams with forced shuffle (min 40% swap)
        new_red, new_blue = await self.balance_teams_mmr(
            player_ids, game.game_id,
            force_shuffle_from=current_map,
            min_swap_pct=0.4,
        )
        new_red_set = set(new_red)

        # Update DB team assignments
        for pid in new_red:
            await DatabaseHelper.update_match_player_team(match_id, pid, "red")
        for pid in new_blue:
            await DatabaseHelper.update_match_player_team(match_id, pid, "blue")

        # Swap Discord roles
        red_role = guild.get_role(match["red_role_id"]) if match.get("red_role_id") else None
        blue_role = guild.get_role(match["blue_role_id"]) if match.get("blue_role_id") else None
        if red_role and blue_role:
            for pid in player_ids:
                member = guild.get_member(pid)
                if not member:
                    continue
                try:
                    if pid in new_red_set:
                        await member.remove_roles(blue_role)
                        await member.add_roles(red_role)
                    else:
                        await member.remove_roles(red_role)
                        await member.add_roles(blue_role)
                except Exception as e:
                    logger.warning(f"Failed to update roles for {pid} during shuffle: {e}")

        # Mark as shuffled
        await DatabaseHelper.update_match(match_id, shuffled=1)

        # Refresh embeds (lobby + queue channel)
        await self.refresh_match_embeds(guild, match_id, reshuffled=True)

        # Resend MMR embed to log
        await self._send_mmr_embed_to_log(
            guild, game, match_id, new_red, new_blue, igns,
            red_role, blue_role, reshuffled=True,
        )

        # Send log embed with initiator and voters
        voters = await DatabaseHelper.get_shuffle_voters(match_id)
        voter_names = []
        for vid in voters:
            m = guild.get_member(vid)
            voter_names.append(m.display_name if m else str(vid))

        log_embed = discord.Embed(
            title=f"Teams Shuffled — Match {short_id}",
            color=COLOR_NEUTRAL,
        )
        log_embed.add_field(
            name="Initiated by",
            value=initiator.display_name,
            inline=True,
        )
        log_embed.add_field(
            name="Voted to shuffle",
            value=", ".join(voter_names) or "—",
            inline=True,
        )

        log_channel_id = await DatabaseHelper.get_config("log_channel_id")
        if log_channel_id:
            log_ch = guild.get_channel(int(log_channel_id))
            if log_ch:
                await log_ch.send(embed=log_embed)

        # Notify lobby channel
        lobby_channel = guild.get_channel(match["channel_id"]) if match.get("channel_id") else None
        if lobby_channel:
            await lobby_channel.send(
                embed=discord.Embed(
                    description="Teams have been reshuffled! Check the updated lineups above.",
                    color=COLOR_SUCCESS,
                )
            )

    @app_commands.command(name="cm_serverstats", description="View server-wide match stats (Valorant or Marvel Rivals)")
    @app_commands.describe(game="Which game to show stats for")
    async def serverstats_cmd(self, interaction: discord.Interaction, game: str):
        game_config = await DatabaseHelper.get_game(int(game))
        if not game_config:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        if not (is_valorant_game(game_config) or is_rivals_game(game_config)):
            await interaction.response.send_message(
                "Server stats are only available for Valorant or Marvel Rivals.",
                ephemeral=True,
            )
            return

        await interaction.response.defer()

        if is_rivals_game(game_config):
            data = await self._gather_rivals_serverstats_data(interaction.guild, game_config, monthly=True)
            image = await self.stats_generator.generate_rivals_serverstats_image(data)
        else:
            data = await self._gather_serverstats_data(interaction.guild, game_config, monthly=True)
            image = await self.stats_generator.generate_serverstats_image(data)

        if image:
            image.seek(0)
            filename = 'serverstats.png'
            file = discord.File(image, filename=filename)
            embed = discord.Embed(color=COLOR_NEUTRAL)
            embed.set_image(url=f"attachment://{filename}")
            view = ServerStatsToggleView(self, game_config.game_id)
            await interaction.followup.send(embed=embed, file=file, view=view)
        else:
            # Fallback to text embed
            lines = [f"**{data['period_title']}** — {data.get('game_name', '')}"]
            lines.append(
                f"Matches: {data.get('total_matches', 0)} | "
                f"Players: {data.get('total_players', 0)} | "
                f"Kills: {data.get('total_kills', 0)}"
            )
            for leader in data.get('leaders', []):
                lines.append(f"**{leader['label']}:** {leader['player']} ({leader['value']})")
            embed = discord.Embed(title="Server Stats", description="\n".join(lines), color=COLOR_NEUTRAL)
            view = ServerStatsToggleView(self, game_config.game_id)
            await interaction.followup.send(embed=embed, view=view)

    @serverstats_cmd.autocomplete('game')
    async def serverstats_game_autocomplete(self, interaction: discord.Interaction, current: str):
        games = await DatabaseHelper.get_all_games()
        return [
            app_commands.Choice(name=g.name, value=str(g.game_id))
            for g in (games or [])
            if (is_valorant_game(g) or is_rivals_game(g))
            and current.lower() in g.name.lower()
        ][:25]

    @commands.command(name="ign")
    async def ign_button_cmd(self, ctx: commands.Context):
        """Post a persistent Set IGN button in the current channel."""
        embed = discord.Embed(
            title="Set Your In-Game Name",
            description="Click the button below to set or update your IGN.",
            color=COLOR_NEUTRAL
        )
        view = PersistentIGNView(self)
        await ctx.send(embed=embed, view=view)

    @commands.command(name="role")
    async def role_button_cmd(self, ctx: commands.Context):
        """Post a persistent Set Role button in the current channel."""
        embed = discord.Embed(
            title="Set Your Role Preferences",
            description="Click the button below to set or update your role preferences.",
            color=COLOR_NEUTRAL
        )
        view = PersistentRoleView(self)
        await ctx.send(embed=embed, view=view)

    # -------------------------------------------------------------------------
    # EVENT LISTENERS
    # -------------------------------------------------------------------------
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Handle member leaving - remove from queues and delete all stats."""
        try:
            # Remove from any active queues
            for queue_id, queue_state in list(self.queues.items()):
                if member.id in queue_state.players:
                    # Skip queues already in ready_check or beyond — the member may
                    # have only briefly disappeared due to a Discord API glitch
                    # (same root cause as the phantom MMR reset). If they genuinely
                    # left, the ready_check_timeout will handle them as unready.
                    if queue_state.state in ("ready_check", "starting_match", "in_match"):
                        logger.warning(
                            f"on_member_remove: {member.id} is in a {queue_state.state} queue "
                            f"({queue_state.queue_id}) — skipping removal to avoid false positive"
                        )
                        # Notify match channel so admins know to sub them out
                        active_match = await DatabaseHelper.get_active_match_for_player(member.id)
                        if active_match:
                            match_channel = member.guild.get_channel(active_match.get("channel_id"))
                            if match_channel:
                                try:
                                    await match_channel.send(
                                        f"⚠️ <@{member.id}> has left the server mid-match. "
                                        f"An admin may need to sub them out."
                                    )
                                except Exception:
                                    pass
                        continue

                    # Acquire queue lock to prevent racing with join/leave handlers
                    async with self.queue_locks.setdefault(queue_id, asyncio.Lock()):
                        # Re-check inside lock — state may have changed
                        if queue_state.state != "waiting" or member.id not in queue_state.players:
                            continue
                        del queue_state.players[member.id]
                        queue_state.grace_timers.pop(member.id, None)

                    # Update embed if possible (outside lock — just I/O)
                    game = await DatabaseHelper.get_game(queue_state.game_id)
                    if game:
                        channel = member.guild.get_channel(queue_state.channel_id)
                        if channel and queue_state.message_id:
                            try:
                                msg = await channel.fetch_message(queue_state.message_id)
                                embed = await self.create_queue_embed(game, queue_state, member.guild)
                                await msg.edit(embed=embed)
                            except Exception:
                                pass

            # Delete all player stats (permanently removed from leaderboards)
            await DatabaseHelper.delete_player_stats(member.id)
        except Exception as e:
            logger.error(f"Error in on_member_remove: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle queue embed deletion - clean up the queue state."""
        try:
            # Check if this message was a queue embed
            for queue_id, queue_state in list(self.queues.items()):
                if queue_state.message_id == payload.message_id:
                    # Cancel any running ready check task before removing the queue
                    if queue_id in self.ready_check_tasks:
                        self.ready_check_tasks[queue_id].cancel()
                        del self.ready_check_tasks[queue_id]
                    # Clean up the queue from memory and database
                    del self.queues[queue_id]
                    self.queue_locks.pop(queue_id, None)
                    async with DatabaseHelper._get_db() as db:
                        await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (queue_id,))
                        await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (queue_id,))
                        await db.commit()
                    logger.info(f"Queue {queue_id} cleaned up due to message deletion")
                    break
        except Exception as e:
            logger.error(f"Error in on_raw_message_delete: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Handle captain selection via @mentions in draft channels AND
        Rivals scoreboard screenshot uploads in finalized match channels."""
        try:
            if message.author.bot:
                return
            if not message.guild:
                return

            # Rivals scoreboard upload path
            pending = self.rivals_pending_uploads.get(message.channel.id)
            if pending and message.attachments:
                # Expiry check
                if pending["expires_at"] < datetime.now(timezone.utc):
                    self.rivals_pending_uploads.pop(message.channel.id, None)
                else:
                    is_correction = bool(pending.get("is_correction"))
                    if is_correction:
                        # Only the initiating admin can upload corrections
                        if message.author.id != pending.get("initiator_id"):
                            return
                        if not await self.is_cm_admin(message.author):
                            return
                    else:
                        # Blacklist check for regular post-match uploads
                        if await DatabaseHelper.is_rivals_upload_blacklisted(
                            message.guild.id, message.author.id
                        ):
                            try:
                                await message.reply(
                                    "You are blacklisted from uploading Rivals "
                                    "scoreboard screenshots.",
                                    mention_author=False,
                                    delete_after=10,
                                )
                            except Exception:
                                pass
                            return
                    await self._process_rivals_scoreboard_upload(
                        message=message,
                        match_id=pending["match_id"],
                        game_id=pending["game_id"],
                        pending_key=message.channel.id,
                        is_correction=is_correction,
                    )
                    return  # Don't fall through to captain-draft handling

            # Check if this is a draft channel awaiting admin captain selection
            match = await DatabaseHelper.get_match_by_channel(message.channel.id)
            if not match:
                # Check draft channels
                async with DatabaseHelper._get_db() as db:
                    async with db.execute(
                        """SELECT * FROM matches WHERE draft_channel_id = ?
                           AND queue_type = 'captains_awaiting' AND winning_team IS NULL""",
                        (message.channel.id,)
                    ) as cursor:
                        row = await cursor.fetchone()
                        if not row:
                            return
                        match = dict(zip([d[0] for d in cursor.description], row))

            if match.get("queue_type") != "captains_awaiting":
                return

            # Check if admin
            if not await self.is_cm_admin(message.author):
                return

            # Check for two mentions
            if len(message.mentions) != 2:
                return

            # Get players in this match
            players = await DatabaseHelper.get_match_players(match["match_id"])
            player_ids = [p["player_id"] for p in players]

            # Verify both mentioned users are in the match
            captain1, captain2 = message.mentions
            if captain1.id not in player_ids or captain2.id not in player_ids:
                await message.channel.send("Both captains must be players in this match.", delete_after=5)
                return

            # Proceed with draft
            game = await DatabaseHelper.get_game(match["game_id"])
            guild = message.guild
            category = message.channel.category

            red_role = guild.get_role(match["red_role_id"])
            blue_role = guild.get_role(match["blue_role_id"])

            await DatabaseHelper.update_match(match["match_id"], queue_type="captains")

            await self.proceed_with_draft(
                guild, message.channel, category, game, match["match_id"],
                player_ids, red_role, blue_role, captain1.id, captain2.id
            )
        except Exception as e:
            logger.error(f"Error in on_message: {e}", exc_info=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CustomMatch(bot))
