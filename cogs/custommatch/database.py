import aiosqlite
import asyncio
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple
from pathlib import Path
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from .models import (
    GameConfig, PlayerIGN, ReadyPenalty, Suspension, PlayerStats,
    QueueState, MatchState, QueueType, CaptainSelection, Team,
    K_FACTOR_PLACEMENT, K_FACTOR_LEARNING, K_FACTOR_STABLE,
    PLACEMENT_GAMES, LEARNING_GAMES, RIVALRY_MIN_GAMES,
)

logger = logging.getLogger('custommatch')

# =============================================================================
# DATABASE SETUP
# =============================================================================

DB_PATH = Path("data/custommatch.db")

SCHEMA = """
-- Configuration
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Games
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    player_count INTEGER NOT NULL,
    queue_type TEXT NOT NULL DEFAULT 'mmr',
    captain_selection TEXT NOT NULL DEFAULT 'random',
    queue_channel_id INTEGER,
    verified_role_id INTEGER,
    ready_timer_seconds INTEGER DEFAULT 60,
    schedule_enabled INTEGER DEFAULT 0,
    schedule_open_days TEXT,
    schedule_open_time TEXT,
    schedule_close_time TEXT,
    schedule_down_message_id INTEGER
);

-- MMR roles per game
CREATE TABLE IF NOT EXISTS game_mmr_roles (
    game_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    mmr_value INTEGER NOT NULL,
    PRIMARY KEY (game_id, role_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Players
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    blacklisted_until TIMESTAMP
);

-- Player stats per game
CREATE TABLE IF NOT EXISTS player_game_stats (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    mmr INTEGER DEFAULT 1000,
    games_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    admin_offset INTEGER DEFAULT 0,
    last_played TIMESTAMP,
    returning_games_remaining INTEGER DEFAULT 0,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Matches
CREATE TABLE IF NOT EXISTS matches (
    match_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    channel_id INTEGER,
    draft_channel_id INTEGER,
    red_role_id INTEGER,
    blue_role_id INTEGER,
    queue_type TEXT NOT NULL,
    winning_team TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_at TIMESTAMP,
    cancelled INTEGER DEFAULT 0,
    queue_message_id INTEGER,
    map_name TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- Match players
CREATE TABLE IF NOT EXISTS match_players (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    team TEXT NOT NULL,
    was_captain INTEGER DEFAULT 0,
    was_sub INTEGER DEFAULT 0,
    original_player_id INTEGER,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Rivalries
CREATE TABLE IF NOT EXISTS rivalries (
    player_a_id INTEGER NOT NULL,
    player_b_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    player_a_wins INTEGER DEFAULT 0,
    player_b_wins INTEGER DEFAULT 0,
    PRIMARY KEY (player_a_id, player_b_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- MMR History
CREATE TABLE IF NOT EXISTS mmr_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    match_id INTEGER NOT NULL,
    mmr_before INTEGER NOT NULL,
    mmr_after INTEGER NOT NULL,
    change INTEGER NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

-- Win votes
CREATE TABLE IF NOT EXISTS win_votes (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    voted_team TEXT NOT NULL,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Active queues (in-memory tracking, but persist for recovery)
CREATE TABLE IF NOT EXISTS active_queues (
    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    message_id INTEGER,
    channel_id INTEGER,
    state TEXT DEFAULT 'waiting',
    ready_check_started TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Queue players
CREATE TABLE IF NOT EXISTS queue_players (
    queue_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_ready INTEGER DEFAULT 0,
    PRIMARY KEY (queue_id, player_id),
    FOREIGN KEY (queue_id) REFERENCES active_queues(queue_id) ON DELETE CASCADE
);

-- Store per-game IGN for players
CREATE TABLE IF NOT EXISTS player_igns (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    ign TEXT NOT NULL,
    puuid TEXT,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Track ready-up penalty offenses with decay
CREATE TABLE IF NOT EXISTS ready_penalties (
    player_id INTEGER PRIMARY KEY,
    offense_count INTEGER DEFAULT 0,
    penalty_expires TIMESTAMP,
    last_offense TIMESTAMP
);

-- Track decline (Not Ready click) penalties — separate scaling from timeout penalties
CREATE TABLE IF NOT EXISTS decline_penalties (
    player_id INTEGER PRIMARY KEY,
    offense_count INTEGER DEFAULT 0,
    penalty_expires TIMESTAMP,
    last_offense TIMESTAMP
);

-- Track player suspensions (separate from blacklist)
CREATE TABLE IF NOT EXISTS suspensions (
    suspension_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    game_id INTEGER,
    suspended_until TIMESTAMP NOT NULL,
    reason TEXT,
    suspended_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track match abandon votes
CREATE TABLE IF NOT EXISTS abandon_votes (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    voted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Track team shuffle votes
CREATE TABLE IF NOT EXISTS shuffle_votes (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Valorant match stats from HenrikDev API
CREATE TABLE IF NOT EXISTS valorant_match_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    valorant_match_id TEXT,
    player_id INTEGER NOT NULL,
    ign TEXT NOT NULL,
    agent TEXT,
    kills INTEGER DEFAULT 0,
    deaths INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    headshots INTEGER DEFAULT 0,
    bodyshots INTEGER DEFAULT 0,
    legshots INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    damage_dealt INTEGER DEFAULT 0,
    first_bloods INTEGER DEFAULT 0,
    plants INTEGER DEFAULT 0,
    defuses INTEGER DEFAULT 0,
    c2k INTEGER DEFAULT 0,
    c3k INTEGER DEFAULT 0,
    c4k INTEGER DEFAULT 0,
    c5k INTEGER DEFAULT 0,
    econ_spent INTEGER DEFAULT 0,
    econ_loadout INTEGER DEFAULT 0,
    map_name TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Track players whose IGN works reliably for API lookups
CREATE TABLE IF NOT EXISTS valorant_player_regulars (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    ign TEXT NOT NULL,
    puuid TEXT,
    region TEXT DEFAULT 'na',
    verified_at TIMESTAMP,
    PRIMARY KEY (player_id, game_id)
);

-- Mod roles that get access to all match channels
CREATE TABLE IF NOT EXISTS mod_roles (
    role_id INTEGER PRIMARY KEY
);

-- Admin stat adjustments (tracks manual W/L changes for monthly leaderboards)
CREATE TABLE IF NOT EXISTS admin_stat_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    wins_delta INTEGER DEFAULT 0,
    losses_delta INTEGER DEFAULT 0,
    adjusted_by INTEGER,
    adjusted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Persistent stats retry tracking (survives bot restarts)
CREATE TABLE IF NOT EXISTS valorant_stats_retry (
    match_id INTEGER PRIMARY KEY,
    game_id INTEGER NOT NULL,
    attempt_count INTEGER DEFAULT 0,
    next_attempt_at TIMESTAMP,
    last_attempt_at TIMESTAMP,
    last_reason TEXT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stats_retry_status ON valorant_stats_retry(status, next_attempt_at);

-- Marvel Rivals per-match stats (extracted from scoreboard screenshots via Gemini)
CREATE TABLE IF NOT EXISTS rivals_match_stats (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    ign TEXT,
    role TEXT,              -- Vanguard / Duelist / Strategist
    team TEXT,              -- 'red' / 'blue'
    kills INTEGER DEFAULT 0,
    deaths INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    final_hits INTEGER DEFAULT 0,
    damage INTEGER DEFAULT 0,
    damage_blocked INTEGER DEFAULT 0,
    healing INTEGER DEFAULT 0,
    accuracy_pct REAL,      -- per-match only, not aggregated
    mvp_svp TEXT,           -- NULL / 'MVP' / 'SVP'
    medals_json TEXT,       -- JSON dict: {"<medal_type>": count}
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rivals_stats_player ON rivals_match_stats(player_id);

-- Users blacklisted from uploading Rivals scoreboard screenshots (per guild)
CREATE TABLE IF NOT EXISTS rivals_upload_blacklist (
    guild_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    added_by INTEGER,
    reason TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (guild_id, player_id)
);

-- Audit log for Rivals scoreboard uploads (one row per upload attempt)
CREATE TABLE IF NOT EXISTS rivals_scoreboard_uploads (
    upload_id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    uploader_id INTEGER,
    image_path TEXT,
    gemini_raw_json TEXT,
    confidence REAL,
    status TEXT,            -- 'committed' | 'pending_review' | 'rejected' | 'superseded'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rivals_uploads_match ON rivals_scoreboard_uploads(match_id, status);

-- Queue subscribers (ping when queue needs X more)
CREATE TABLE IF NOT EXISTS queue_subscribers (
    queue_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    threshold INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (queue_id, player_id)
);

-- Player role preferences (e.g. Rivals: vanguard/duelist/strategist)
CREATE TABLE IF NOT EXISTS player_role_prefs (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    primary_role TEXT NOT NULL,
    secondary_role TEXT,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Secondary queue game modes
CREATE TABLE IF NOT EXISTS secondary_modes (
    mode_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    mode_name TEXT NOT NULL,
    map_pool_type TEXT NOT NULL DEFAULT 'none',
    custom_maps TEXT,
    description TEXT,
    is_ffa INTEGER DEFAULT 0,
    is_mirror INTEGER DEFAULT 0,
    display_order INTEGER DEFAULT 0,
    UNIQUE(game_id, mode_name),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_matches_winning_cancelled ON matches(winning_team, cancelled);
CREATE INDEX IF NOT EXISTS idx_match_players_player_id ON match_players(player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_stats_game_id ON player_game_stats(game_id);
CREATE INDEX IF NOT EXISTS idx_mmr_history_player_game ON mmr_history(player_id, game_id);
"""

async def init_db():
    """Initialize the database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with DatabaseHelper._get_db() as db:
        await db.executescript(SCHEMA)
        await db.commit()
    await migrate_db()

async def migrate_db():
    """Run database migrations to add new columns to existing tables."""
    async with DatabaseHelper._get_db() as db:
        # Get existing columns for games table
        async with db.execute("PRAGMA table_info(games)") as cursor:
            game_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to games table if they don't exist
        game_migrations = [
            ("vc_creation_enabled", "INTEGER DEFAULT 0"),
            ("queue_role_required", "INTEGER DEFAULT 1"),
            ("dm_ready_up", "INTEGER DEFAULT 0"),
            # match_history_channel_id removed - using game_channel_id for results
            ("queue_timeout_minutes", "INTEGER DEFAULT 180"),
            ("penalty_1st_minutes", "INTEGER DEFAULT 60"),
            ("penalty_2nd_minutes", "INTEGER DEFAULT 1440"),
            ("penalty_3rd_minutes", "INTEGER DEFAULT 10080"),
            ("penalty_decay_days", "INTEGER DEFAULT 30"),
            ("banner_url", "TEXT"),
            ("verification_topic", "TEXT"),
            ("game_channel_id", "INTEGER"),
            ("ready_loading_emoji", "TEXT DEFAULT '<a:loading:1234567890>'"),
            ("ready_done_emoji", "TEXT DEFAULT '<:check:1234567890>'"),
            ("schedule_enabled", "INTEGER DEFAULT 0"),
            ("schedule_open_days", "TEXT"),
            ("schedule_open_time", "TEXT"),
            ("schedule_close_time", "TEXT"),
            ("schedule_down_message_id", "INTEGER"),
            ("schedule_times", "TEXT"),  # JSON: {"0": {"open": "16:00", "close": "23:00"}, ...}
            ("leaderboard_channel_id", "INTEGER"),
            ("leaderboard_message_id", "INTEGER"),
            ("ign_required", "INTEGER DEFAULT 0"),
            ("role_required", "INTEGER DEFAULT 0"),
            ("category_id", "INTEGER"),
            ("lf1_channel_id", "INTEGER"),
            ("grace_period_minutes", "INTEGER DEFAULT 10"),
            ("not_ready_cooldown_minutes", "INTEGER DEFAULT 5"),
            ("decline_1st_minutes", "INTEGER DEFAULT 15"),
            ("decline_2nd_minutes", "INTEGER DEFAULT 60"),
            ("decline_3rd_minutes", "INTEGER DEFAULT 1440"),
            # Secondary queue fields
            ("secondary_queue_enabled", "INTEGER DEFAULT 0"),
            ("secondary_queue_name", "TEXT"),
            ("secondary_queue_player_count", "INTEGER"),
            ("secondary_queue_type", "TEXT"),
            ("secondary_queue_channel_id", "INTEGER"),
            ("secondary_schedule_times", "TEXT"),
            ("secondary_queue_match_limit", "INTEGER"),
            ("secondary_mapvote_game", "TEXT"),
            ("secondary_banner_url", "TEXT"),
        ]

        for col_name, col_def in game_migrations:
            if col_name not in game_columns:
                await db.execute(f"ALTER TABLE games ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to games table")

        # Get existing columns for matches table
        async with db.execute("PRAGMA table_info(matches)") as cursor:
            match_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to matches table if they don't exist
        match_migrations = [
            ("red_vc_id", "INTEGER"),
            ("blue_vc_id", "INTEGER"),
            ("short_id", "TEXT"),
            ("valorant_match_id", "TEXT"),
            ("map_name", "TEXT"),
            ("queue_teams_msg_id", "INTEGER"),
            ("match_msg_id", "INTEGER"),
            ("val_red_rounds", "INTEGER"),
            ("val_blue_rounds", "INTEGER"),
            ("bot_red_is_val_red", "INTEGER"),
            ("ended_at", "TIMESTAMP"),
            ("tracker_url", "TEXT"),
            ("log_msg_id", "INTEGER"),
            ("shuffled", "INTEGER DEFAULT 0"),
            ("is_secondary", "INTEGER DEFAULT 0"),
            ("mode_name", "TEXT"),
        ]

        for col_name, col_def in match_migrations:
            if col_name not in match_columns:
                await db.execute(f"ALTER TABLE matches ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to matches table")

        # Backfill ended_at for existing completed/cancelled matches
        if "ended_at" not in match_columns:
            await db.execute("""
                UPDATE matches SET ended_at = COALESCE(decided_at, created_at)
                WHERE ended_at IS NULL AND (winning_team IS NOT NULL OR cancelled = 1)
            """)
            await db.commit()
            logger.info("Backfilled ended_at for existing completed/cancelled matches")

        # Get existing columns for active_queues table
        async with db.execute("PRAGMA table_info(active_queues)") as cursor:
            queue_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to active_queues table if they don't exist
        queue_migrations = [
            ("short_id", "TEXT"),
            ("is_secondary", "INTEGER DEFAULT 0"),
        ]

        for col_name, col_def in queue_migrations:
            if col_name not in queue_columns:
                await db.execute(f"ALTER TABLE active_queues ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to active_queues table")

        # Get existing columns for player_game_stats table
        async with db.execute("PRAGMA table_info(player_game_stats)") as cursor:
            pgs_columns = {row[1] for row in await cursor.fetchall()}

        pgs_migrations = [
            ("returning_games_remaining", "INTEGER DEFAULT 0"),
        ]

        for col_name, col_def in pgs_migrations:
            if col_name not in pgs_columns:
                await db.execute(f"ALTER TABLE player_game_stats ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to player_game_stats table")

        # Get existing columns for valorant_match_stats table
        async with db.execute("PRAGMA table_info(valorant_match_stats)") as cursor:
            val_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to valorant_match_stats table if they don't exist
        val_migrations = [
            ("damage_dealt", "INTEGER DEFAULT 0"),
            ("first_bloods", "INTEGER DEFAULT 0"),
            ("plants", "INTEGER DEFAULT 0"),
            ("defuses", "INTEGER DEFAULT 0"),
            ("c2k", "INTEGER DEFAULT 0"),
            ("c3k", "INTEGER DEFAULT 0"),
            ("c4k", "INTEGER DEFAULT 0"),
            ("c5k", "INTEGER DEFAULT 0"),
            ("econ_spent", "INTEGER DEFAULT 0"),
            ("econ_loadout", "INTEGER DEFAULT 0"),
        ]

        for col_name, col_def in val_migrations:
            if col_name not in val_columns:
                await db.execute(f"ALTER TABLE valorant_match_stats ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to valorant_match_stats table")

        # Migrate valorant_match_stats to add UNIQUE(match_id, player_id) constraint
        # Check if the unique index already exists
        async with db.execute("PRAGMA index_list(valorant_match_stats)") as cursor:
            indexes = {row[1] for row in await cursor.fetchall()}

        if 'uq_valorant_stats' not in indexes:
            logger.info("Migrating valorant_match_stats to add UNIQUE(match_id, player_id) constraint...")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS valorant_match_stats_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id INTEGER NOT NULL,
                    valorant_match_id TEXT,
                    player_id INTEGER NOT NULL,
                    ign TEXT NOT NULL,
                    agent TEXT,
                    kills INTEGER DEFAULT 0,
                    deaths INTEGER DEFAULT 0,
                    assists INTEGER DEFAULT 0,
                    headshots INTEGER DEFAULT 0,
                    bodyshots INTEGER DEFAULT 0,
                    legshots INTEGER DEFAULT 0,
                    score INTEGER DEFAULT 0,
                    damage_dealt INTEGER DEFAULT 0,
                    first_bloods INTEGER DEFAULT 0,
                    plants INTEGER DEFAULT 0,
                    defuses INTEGER DEFAULT 0,
                    c2k INTEGER DEFAULT 0,
                    c3k INTEGER DEFAULT 0,
                    c4k INTEGER DEFAULT 0,
                    c5k INTEGER DEFAULT 0,
                    econ_spent INTEGER DEFAULT 0,
                    econ_loadout INTEGER DEFAULT 0,
                    map_name TEXT,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(match_id, player_id),
                    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
                )
            """)
            # Copy deduplicated data (keep highest id per match_id+player_id pair)
            await db.execute("""
                INSERT INTO valorant_match_stats_new
                SELECT * FROM valorant_match_stats
                WHERE id IN (
                    SELECT MAX(id) FROM valorant_match_stats
                    GROUP BY match_id, player_id
                )
            """)
            await db.execute("DROP TABLE valorant_match_stats")
            await db.execute("ALTER TABLE valorant_match_stats_new RENAME TO valorant_match_stats")
            await db.execute("CREATE UNIQUE INDEX uq_valorant_stats ON valorant_match_stats(match_id, player_id)")
            logger.info("Successfully migrated valorant_match_stats with UNIQUE constraint")

        # Clean up cross-match duplicates: same valorant_match_id used for multiple match_ids
        # Keep only the entry with the lowest match_id for each (valorant_match_id, player_id) pair
        async with db.execute("""
            SELECT valorant_match_id, COUNT(DISTINCT match_id) as match_count
            FROM valorant_match_stats
            WHERE valorant_match_id IS NOT NULL
            GROUP BY valorant_match_id
            HAVING match_count > 1
        """) as cursor:
            cross_dupes = await cursor.fetchall()

        if cross_dupes:
            for row in cross_dupes:
                val_id = row[0]
                # Keep the lowest match_id, delete the rest
                await db.execute("""
                    DELETE FROM valorant_match_stats
                    WHERE valorant_match_id = ?
                    AND match_id != (
                        SELECT MIN(match_id) FROM valorant_match_stats
                        WHERE valorant_match_id = ?
                    )
                """, (val_id, val_id))
                logger.info(f"Cleaned up cross-match duplicate for valorant_match_id={val_id}")

        # Add puuid column to player_igns table
        async with db.execute("PRAGMA table_info(player_igns)") as cursor:
            ign_columns = {row[1] for row in await cursor.fetchall()}

        if 'puuid' not in ign_columns:
            await db.execute("ALTER TABLE player_igns ADD COLUMN puuid TEXT")
            logger.info("Added column puuid to player_igns table")

            # Backfill PUUIDs from valorant_player_regulars (which caches PUUIDs for players with past stats fetches)
            await db.execute("""
                UPDATE player_igns SET puuid = (
                    SELECT vpr.puuid FROM valorant_player_regulars vpr
                    WHERE vpr.player_id = player_igns.player_id
                    AND vpr.game_id = player_igns.game_id
                    AND vpr.puuid IS NOT NULL
                )
                WHERE puuid IS NULL
            """)
            backfilled = db.total_changes
            if backfilled:
                logger.info(f"Backfilled {backfilled} PUUIDs from valorant_player_regulars into player_igns")

        # Add label column to game_mmr_roles if it doesn't exist
        async with db.execute("PRAGMA table_info(game_mmr_roles)") as cursor:
            mmr_role_columns = {row[1] for row in await cursor.fetchall()}
        if 'label' not in mmr_role_columns:
            await db.execute("ALTER TABLE game_mmr_roles ADD COLUMN label TEXT")
            logger.info("Added column label to game_mmr_roles table")

        # Add description column to secondary_modes if it doesn't exist
        async with db.execute("PRAGMA table_info(secondary_modes)") as cursor:
            sm_columns = {row[1] for row in await cursor.fetchall()}
        if 'description' not in sm_columns:
            await db.execute("ALTER TABLE secondary_modes ADD COLUMN description TEXT")
            logger.info("Added column description to secondary_modes table")
        if 'is_ffa' not in sm_columns:
            await db.execute("ALTER TABLE secondary_modes ADD COLUMN is_ffa INTEGER DEFAULT 0")
            logger.info("Added column is_ffa to secondary_modes table")
        if 'is_mirror' not in sm_columns:
            await db.execute("ALTER TABLE secondary_modes ADD COLUMN is_mirror INTEGER DEFAULT 0")
            logger.info("Added column is_mirror to secondary_modes table")

        await db.commit()

# =============================================================================
# DATABASE HELPERS
# =============================================================================

class DatabaseHelper:
    """Helper class for database operations."""
    _db: Optional[aiosqlite.Connection] = None

    @classmethod
    async def connect(cls):
        """Open a persistent database connection with WAL mode."""
        cls._db = await aiosqlite.connect(DB_PATH)
        cls._db.row_factory = aiosqlite.Row
        await cls._db.execute("PRAGMA journal_mode=WAL")
        await cls._db.execute("PRAGMA busy_timeout=5000")
        logger.info("DatabaseHelper: persistent connection opened (WAL mode)")

    @classmethod
    async def close(cls):
        """Close the persistent database connection."""
        if cls._db:
            await cls._db.close()
            cls._db = None
            logger.info("DatabaseHelper: persistent connection closed")

    @classmethod
    @asynccontextmanager
    async def _get_db(cls):
        """Return the shared connection, or a temporary one if not yet connected."""
        if cls._db is not None:
            yield cls._db
        else:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                yield db

    @staticmethod
    async def get_config(key: str) -> Optional[str]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def set_config(key: str, value: str):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, value)
            )
            await db.commit()

    @staticmethod
    async def get_role_emojis() -> dict:
        """Get Rivals role emojis from config. Returns dict with keys: vanguard, duelist, strategist, none."""
        raw = await DatabaseHelper.get_config("role_emojis")
        if raw:
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    @staticmethod
    async def set_role_emojis(emojis: dict):
        """Save Rivals role emojis to config as JSON."""
        await DatabaseHelper.set_config("role_emojis", json.dumps(emojis))

    @staticmethod
    async def get_game(game_id: int) -> Optional[GameConfig]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM games WHERE game_id = ?", (game_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)

    @staticmethod
    async def get_game_by_name(name: str) -> Optional[GameConfig]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM games WHERE name = ?", (name,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)

    @staticmethod
    async def get_game_by_channel(channel_id: int) -> Optional[GameConfig]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM games WHERE queue_channel_id = ?", (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)

    @staticmethod
    def _row_to_game_config(row) -> GameConfig:
        """Helper to convert a database row to GameConfig."""
        return GameConfig(
            game_id=row["game_id"],
            name=row["name"],
            player_count=row["player_count"],
            queue_type=QueueType(row["queue_type"]),
            captain_selection=CaptainSelection(row["captain_selection"]),
            queue_channel_id=row["queue_channel_id"],
            verified_role_id=row["verified_role_id"],
            ready_timer_seconds=row["ready_timer_seconds"] or 60,
            vc_creation_enabled=bool(row["vc_creation_enabled"]) if "vc_creation_enabled" in row.keys() else False,
            queue_role_required=bool(row["queue_role_required"]) if "queue_role_required" in row.keys() else True,
            dm_ready_up=bool(row["dm_ready_up"]) if "dm_ready_up" in row.keys() else False,

            queue_timeout_minutes=row["queue_timeout_minutes"] if ("queue_timeout_minutes" in row.keys() and row["queue_timeout_minutes"]) else 180,
            penalty_1st_minutes=row["penalty_1st_minutes"] if "penalty_1st_minutes" in row.keys() else 60,
            penalty_2nd_minutes=row["penalty_2nd_minutes"] if "penalty_2nd_minutes" in row.keys() else 1440,
            penalty_3rd_minutes=row["penalty_3rd_minutes"] if "penalty_3rd_minutes" in row.keys() else 10080,
            penalty_decay_days=row["penalty_decay_days"] if "penalty_decay_days" in row.keys() else 30,
            banner_url=row["banner_url"] if "banner_url" in row.keys() else None,
            verification_topic=row["verification_topic"] if "verification_topic" in row.keys() else None,
            game_channel_id=row["game_channel_id"] if "game_channel_id" in row.keys() else None,
            leaderboard_channel_id=row["leaderboard_channel_id"] if "leaderboard_channel_id" in row.keys() else None,
            leaderboard_message_id=row["leaderboard_message_id"] if "leaderboard_message_id" in row.keys() else None,
            ready_loading_emoji=row["ready_loading_emoji"] if "ready_loading_emoji" in row.keys() and row["ready_loading_emoji"] else "<a:loading:1234567890>",
            ready_done_emoji=row["ready_done_emoji"] if "ready_done_emoji" in row.keys() and row["ready_done_emoji"] else "<:check:1234567890>",
            schedule_enabled=bool(row["schedule_enabled"]) if "schedule_enabled" in row.keys() else False,
            schedule_open_days=row["schedule_open_days"] if "schedule_open_days" in row.keys() else None,
            schedule_open_time=row["schedule_open_time"] if "schedule_open_time" in row.keys() else None,
            schedule_close_time=row["schedule_close_time"] if "schedule_close_time" in row.keys() else None,
            schedule_down_message_id=row["schedule_down_message_id"] if "schedule_down_message_id" in row.keys() else None,
            schedule_times=json.loads(row["schedule_times"]) if "schedule_times" in row.keys() and row["schedule_times"] else None,
            ign_required=bool(row["ign_required"]) if "ign_required" in row.keys() else False,
            role_required=bool(row["role_required"]) if "role_required" in row.keys() else False,
            category_id=row["category_id"] if "category_id" in row.keys() and row["category_id"] else None,
            lf1_channel_id=row["lf1_channel_id"] if "lf1_channel_id" in row.keys() and row["lf1_channel_id"] else None,
            grace_period_minutes=row["grace_period_minutes"] if "grace_period_minutes" in row.keys() and row["grace_period_minutes"] else 10,
            not_ready_cooldown_minutes=row["not_ready_cooldown_minutes"] if "not_ready_cooldown_minutes" in row.keys() and row["not_ready_cooldown_minutes"] is not None else 5,
            decline_1st_minutes=row["decline_1st_minutes"] if "decline_1st_minutes" in row.keys() and row["decline_1st_minutes"] is not None else 15,
            decline_2nd_minutes=row["decline_2nd_minutes"] if "decline_2nd_minutes" in row.keys() and row["decline_2nd_minutes"] is not None else 60,
            decline_3rd_minutes=row["decline_3rd_minutes"] if "decline_3rd_minutes" in row.keys() and row["decline_3rd_minutes"] is not None else 1440,
            # Secondary queue fields
            secondary_queue_enabled=bool(row["secondary_queue_enabled"]) if "secondary_queue_enabled" in row.keys() else False,
            secondary_queue_name=row["secondary_queue_name"] if "secondary_queue_name" in row.keys() else None,
            secondary_queue_player_count=row["secondary_queue_player_count"] if "secondary_queue_player_count" in row.keys() and row["secondary_queue_player_count"] else None,
            secondary_queue_type=QueueType(row["secondary_queue_type"]) if "secondary_queue_type" in row.keys() and row["secondary_queue_type"] else None,
            secondary_queue_channel_id=row["secondary_queue_channel_id"] if "secondary_queue_channel_id" in row.keys() and row["secondary_queue_channel_id"] else None,
            secondary_schedule_times=json.loads(row["secondary_schedule_times"]) if "secondary_schedule_times" in row.keys() and row["secondary_schedule_times"] else None,
            secondary_queue_match_limit=row["secondary_queue_match_limit"] if "secondary_queue_match_limit" in row.keys() and row["secondary_queue_match_limit"] else None,
            secondary_banner_url=row["secondary_banner_url"] if "secondary_banner_url" in row.keys() else None,
        )

    @staticmethod
    async def get_all_games() -> List[GameConfig]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute("SELECT * FROM games") as cursor:
                rows = await cursor.fetchall()
                return [DatabaseHelper._row_to_game_config(row) for row in rows]

    @staticmethod
    async def add_game(name: str, player_count: int, queue_type: str = "mmr",
                       captain_selection: str = "random") -> int:
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                """INSERT INTO games (name, player_count, queue_type, captain_selection)
                   VALUES (?, ?, ?, ?)""",
                (name, player_count, queue_type, captain_selection)
            )
            await db.commit()
            return cursor.lastrowid

    # Valid column names for games table - prevents SQL injection
    VALID_GAME_COLUMNS = {
        'name', 'player_count', 'queue_type', 'captain_selection',
        'queue_channel_id', 'verified_role_id', 'ready_timer_seconds',
        'schedule_enabled', 'schedule_open_days', 'schedule_open_time',
        'schedule_close_time', 'schedule_down_message_id', 'vc_creation_enabled',
        'queue_role_required', 'dm_ready_up',
        'banner_url', 'verification_topic', 'game_channel_id',
        'leaderboard_channel_id', 'leaderboard_message_id',
        'ign_required', 'role_required', 'schedule_times',
        'penalty_1st_minutes', 'penalty_2nd_minutes', 'penalty_3rd_minutes',
        'penalty_decay_days', 'queue_timeout_minutes', 'category_id',
        'ready_loading_emoji', 'ready_done_emoji', 'lf1_channel_id',
        'grace_period_minutes',
        'not_ready_cooldown_minutes',
        'decline_1st_minutes', 'decline_2nd_minutes', 'decline_3rd_minutes',
        'secondary_queue_enabled', 'secondary_queue_name', 'secondary_queue_player_count',
        'secondary_queue_type', 'secondary_queue_channel_id', 'secondary_schedule_times',
        'secondary_queue_match_limit',
        'secondary_banner_url',
    }

    VALID_MATCH_COLUMNS = {
        'channel_id', 'draft_channel_id', 'red_role_id', 'blue_role_id',
        'winning_team', 'decided_at', 'cancelled', 'queue_message_id',
        'map_name', 'red_vc_id', 'blue_vc_id', 'short_id',
        'valorant_match_id', 'queue_type', 'queue_teams_msg_id',
        'match_msg_id', 'val_red_rounds', 'val_blue_rounds', 'ended_at',
        'bot_red_is_val_red', 'tracker_url', 'log_msg_id', 'shuffled',
        'is_secondary',
        'mode_name',
    }

    @staticmethod
    async def update_game(game_id: int, **kwargs):
        async with DatabaseHelper._get_db() as db:
            set_clauses = []
            params = []
            for key, value in kwargs.items():
                if key not in DatabaseHelper.VALID_GAME_COLUMNS:
                    raise ValueError(f"Invalid column name: {key}")
                set_clauses.append(f"{key} = ?")
                params.append(value)
            if set_clauses:
                params.append(game_id)
                await db.execute(
                    f"UPDATE games SET {', '.join(set_clauses)} WHERE game_id = ?",
                    params
                )
                await db.commit()

    @staticmethod
    async def delete_game(game_id: int):
        async with DatabaseHelper._get_db() as db:
            await db.execute("DELETE FROM games WHERE game_id = ?", (game_id,))
            await db.commit()

    @staticmethod
    async def get_player_stats(player_id: int, game_id: int) -> PlayerStats:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM player_game_stats WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return PlayerStats(player_id=player_id, game_id=game_id, is_new=True)
                return PlayerStats(
                    player_id=row[0],
                    game_id=row[1],
                    mmr=row[2],
                    games_played=row[3],
                    wins=row[4],
                    losses=row[5],
                    admin_offset=row[6],
                    last_played=datetime.fromisoformat(row[7]) if row[7] else None,
                    returning_games_remaining=row[8] if len(row) > 8 and row[8] else 0
                )

    @staticmethod
    async def update_player_stats(stats: PlayerStats):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO player_game_stats
                   (player_id, game_id, mmr, games_played, wins, losses, admin_offset, last_played, returning_games_remaining)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (stats.player_id, stats.game_id, stats.mmr, stats.games_played,
                 stats.wins, stats.losses, stats.admin_offset,
                 stats.last_played.isoformat() if stats.last_played else None,
                 stats.returning_games_remaining)
            )
            await db.commit()

    @staticmethod
    async def get_mmr_roles(game_id: int) -> Dict[int, int]:
        """Returns {role_id: mmr_value}"""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT role_id, mmr_value FROM game_mmr_roles WHERE game_id = ?",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    @staticmethod
    async def set_mmr_role(game_id: int, role_id: int, mmr_value: int, label: str = None):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO game_mmr_roles (game_id, role_id, mmr_value, label)
                   VALUES (?, ?, ?, ?)""",
                (game_id, role_id, mmr_value, label)
            )
            await db.commit()

    @staticmethod
    async def get_mmr_roles_with_labels(game_id: int) -> Dict[int, dict]:
        """Returns {role_id: {'mmr': mmr_value, 'label': label}}"""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT role_id, mmr_value, label FROM game_mmr_roles WHERE game_id = ?",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: {'mmr': row[1], 'label': row[2]} for row in rows}

    @staticmethod
    async def remove_mmr_role(game_id: int, role_id: int):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM game_mmr_roles WHERE game_id = ? AND role_id = ?",
                (game_id, role_id)
            )
            await db.commit()

    @staticmethod
    async def is_blacklisted(player_id: int) -> bool:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT blacklisted_until FROM players WHERE player_id = ?",
                (player_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row or not row[0]:
                    return False
                until = datetime.fromisoformat(row[0])
                return until > datetime.now(timezone.utc)

    @staticmethod
    async def blacklist_player(player_id: int, until: Optional[datetime] = None):
        """Blacklist a player. If until is None, permanent."""
        if until is None:
            until = datetime(2099, 12, 31, tzinfo=timezone.utc)
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT INTO players (player_id, blacklisted_until) VALUES (?, ?)
                   ON CONFLICT(player_id) DO UPDATE SET blacklisted_until = ?""",
                (player_id, until.isoformat(), until.isoformat())
            )
            await db.commit()

    @staticmethod
    async def unblacklist_player(player_id: int):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE players SET blacklisted_until = NULL WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()

    @staticmethod
    async def get_blacklisted_players() -> List[Tuple[int, datetime]]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id, blacklisted_until FROM players WHERE blacklisted_until IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
                return [(row[0], datetime.fromisoformat(row[1])) for row in rows if row[1]]

    @staticmethod
    async def create_match(game_id: int, queue_type: str, queue_message_id: Optional[int] = None,
                           short_id: Optional[str] = None, is_secondary: bool = False) -> int:
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                """INSERT INTO matches (game_id, queue_type, queue_message_id, created_at, short_id, is_secondary)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (game_id, queue_type, queue_message_id, datetime.now(timezone.utc).isoformat(), short_id,
                 int(is_secondary))
            )
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def count_secondary_matches_in_window(game_id: int, window_start: datetime) -> int:
        """Count non-cancelled secondary matches created since window_start."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT COUNT(*) FROM matches
                   WHERE game_id = ? AND is_secondary = 1 AND cancelled = 0
                   AND datetime(created_at) >= datetime(?)""",
                (game_id, window_start.isoformat())
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    @staticmethod
    async def update_match(match_id: int, **kwargs):
        async with DatabaseHelper._get_db() as db:
            for key, value in kwargs.items():
                if key not in DatabaseHelper.VALID_MATCH_COLUMNS:
                    raise ValueError(f"Invalid column name: {key}")
                await db.execute(
                    f"UPDATE matches SET {key} = ? WHERE match_id = ?",
                    (value, match_id)
                )
            await db.commit()

    @staticmethod
    async def get_match(match_id: int) -> Optional[dict]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_match_by_channel(channel_id: int) -> Optional[dict]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM matches WHERE channel_id = ? AND winning_team IS NULL AND cancelled = 0",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_match_by_short_id(short_id: str) -> Optional[dict]:
        """Get a match by its short ID (case-insensitive)."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM matches WHERE UPPER(short_id) = UPPER(?)",
                (short_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_active_matches() -> List[dict]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM matches WHERE winning_team IS NULL AND cancelled = 0"
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_active_match_for_player(player_id: int) -> Optional[dict]:
        """Return the active match a player is currently in, or None."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT m.* FROM matches m
                   JOIN match_players mp ON m.match_id = mp.match_id
                   WHERE mp.player_id = ? AND m.winning_team IS NULL AND m.cancelled = 0
                   LIMIT 1""",
                (player_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def add_match_player(match_id: int, player_id: int, team: str,
                               was_captain: bool = False, was_sub: bool = False,
                               original_player_id: Optional[int] = None):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO match_players
                   (match_id, player_id, team, was_captain, was_sub, original_player_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (match_id, player_id, team, int(was_captain), int(was_sub), original_player_id)
            )
            await db.commit()

    @staticmethod
    async def get_match_players(match_id: int) -> List[dict]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM match_players WHERE match_id = ?", (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_previous_team_assignment(
        player_ids: List[int],
        game_id: int,
        overlap_threshold: float = 0.5,
        lookback: int = 20,
    ) -> Dict[int, str]:
        """Return the most recent team assignment for a roster similar to the
        given `player_ids`, restricted to players present in the current roster.

        Scans up to `lookback` recent decided, non-cancelled matches for the
        given game. For each candidate (newest first), computes the fraction of
        the current roster that was on that match; returns the first match
        whose overlap meets `overlap_threshold`. Returns `{}` if none qualify.
        """
        if not player_ids:
            return {}
        current_set = set(player_ids)
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT match_id FROM matches
                   WHERE game_id = ?
                     AND winning_team IS NOT NULL
                     AND cancelled = 0
                   ORDER BY decided_at DESC
                   LIMIT ?""",
                (game_id, lookback),
            ) as cursor:
                rows = await cursor.fetchall()
        for row in rows:
            match_id = row["match_id"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
            prev_players = await DatabaseHelper.get_match_players(match_id)
            prev_pids = {p["player_id"] for p in prev_players}
            overlap = len(prev_pids & current_set) / len(current_set)
            if overlap >= overlap_threshold:
                return {
                    p["player_id"]: p["team"]
                    for p in prev_players
                    if p["player_id"] in current_set and p["team"] in ("red", "blue")
                }
        return {}

    @staticmethod
    async def remove_match_player(match_id: int, player_id: int):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM match_players WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def update_match_player_team(match_id: int, player_id: int, team: str):
        """Update a player's team assignment in a match."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE match_players SET team = ? WHERE match_id = ? AND player_id = ?",
                (team, match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def set_match_map(match_id: int, map_name: str):
        """Set the map name for a match (from map vote)."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE matches SET map_name = ? WHERE match_id = ?",
                (map_name, match_id)
            )
            await db.commit()

    @staticmethod
    async def set_match_mode(match_id: int, mode_name: str, map_name: Optional[str] = None):
        """Set the mode and optionally the map for a match (from mode vote)."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE matches SET mode_name = ?, map_name = ? WHERE match_id = ?",
                (mode_name, map_name, match_id)
            )
            await db.commit()

    @staticmethod
    async def get_match_by_channel_id(channel_id: int) -> Optional[dict]:
        """Get match by channel ID (includes completed/cancelled matches)."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM matches WHERE channel_id = ? ORDER BY created_at DESC LIMIT 1",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_rivalry(player_a: int, player_b: int, game_id: int) -> Optional[Tuple[int, int]]:
        """Returns (player_a_wins, player_b_wins) or None."""
        # Ensure consistent ordering
        if player_a > player_b:
            player_a, player_b = player_b, player_a
            swapped = True
        else:
            swapped = False

        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT player_a_wins, player_b_wins FROM rivalries
                   WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                (player_a, player_b, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                if swapped:
                    return (row[1], row[0])
                return (row[0], row[1])

    @staticmethod
    async def update_rivalry(winner_id: int, loser_id: int, game_id: int):
        """Update rivalry stats after a match."""
        # Ensure consistent ordering
        if winner_id > loser_id:
            player_a, player_b = loser_id, winner_id
            winner_is_b = True
        else:
            player_a, player_b = winner_id, loser_id
            winner_is_b = False

        async with DatabaseHelper._get_db() as db:
            # Check if exists
            async with db.execute(
                """SELECT player_a_wins, player_b_wins FROM rivalries
                   WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                (player_a, player_b, game_id)
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                if winner_is_b:
                    await db.execute(
                        """UPDATE rivalries SET player_b_wins = player_b_wins + 1
                           WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                        (player_a, player_b, game_id)
                    )
                else:
                    await db.execute(
                        """UPDATE rivalries SET player_a_wins = player_a_wins + 1
                           WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                        (player_a, player_b, game_id)
                    )
            else:
                a_wins = 0 if winner_is_b else 1
                b_wins = 1 if winner_is_b else 0
                await db.execute(
                    """INSERT INTO rivalries (player_a_id, player_b_id, game_id, player_a_wins, player_b_wins)
                       VALUES (?, ?, ?, ?, ?)""",
                    (player_a, player_b, game_id, a_wins, b_wins)
                )
            await db.commit()

    @staticmethod
    async def reverse_rivalry(old_winner_id: int, old_loser_id: int, game_id: int):
        """Reverse a rivalry record - swap a win from old_winner to old_loser."""
        # Ensure consistent ordering
        if old_winner_id > old_loser_id:
            player_a, player_b = old_loser_id, old_winner_id
            old_winner_is_b = True
        else:
            player_a, player_b = old_winner_id, old_loser_id
            old_winner_is_b = False

        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT player_a_wins, player_b_wins FROM rivalries
                   WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                (player_a, player_b, game_id)
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                # Decrement old winner's wins, increment old loser's wins
                if old_winner_is_b:
                    new_a_wins = row[0] + 1  # old loser now has one more win
                    new_b_wins = max(0, row[1] - 1)  # old winner loses a win
                else:
                    new_a_wins = max(0, row[0] - 1)  # old winner loses a win
                    new_b_wins = row[1] + 1  # old loser now has one more win

                await db.execute(
                    """UPDATE rivalries SET player_a_wins = ?, player_b_wins = ?
                       WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                    (new_a_wins, new_b_wins, player_a, player_b, game_id)
                )
                await db.commit()

    @staticmethod
    async def get_player_rivalries(player_id: int, game_id: int, limit: int = 3) -> List[dict]:
        """Get top rivalries for a player."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT player_a_id, player_b_id, player_a_wins, player_b_wins
                   FROM rivalries WHERE (player_a_id = ? OR player_b_id = ?) AND game_id = ?
                   ORDER BY (player_a_wins + player_b_wins) DESC LIMIT ?""",
                (player_id, player_id, game_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    if row[0] == player_id:
                        results.append({
                            "opponent_id": row[1],
                            "wins": row[2],
                            "losses": row[3]
                        })
                    else:
                        results.append({
                            "opponent_id": row[0],
                            "wins": row[3],
                            "losses": row[2]
                        })
                return results

    # -------------------------------------------------------------------------
    # HEAD-TO-HEAD METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_h2h_matches(player_a: int, player_b: int, game_id: int) -> List[dict]:
        """Get all matches where player_a and player_b were on OPPOSITE teams.

        Returns list of dicts with match info + each player's team, ordered
        by decided_at DESC (most recent first).
        """
        async with DatabaseHelper._get_db() as db:
            query = """
                SELECT m.match_id, m.winning_team, m.decided_at, m.map_name,
                       mp_a.team AS a_team, mp_b.team AS b_team,
                       m.val_red_rounds, m.val_blue_rounds
                FROM matches m
                JOIN match_players mp_a ON m.match_id = mp_a.match_id AND mp_a.player_id = ?
                JOIN match_players mp_b ON m.match_id = mp_b.match_id AND mp_b.player_id = ?
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL
                  AND mp_a.team != mp_b.team
                ORDER BY m.decided_at DESC
            """
            async with db.execute(query, (player_a, player_b, game_id)) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    @staticmethod
    async def get_h2h_teammate_matches(player_a: int, player_b: int, game_id: int) -> dict:
        """Get record when player_a and player_b are on the SAME team.

        Returns {games: int, wins: int}.
        """
        async with DatabaseHelper._get_db() as db:
            query = """
                SELECT
                    COUNT(*) as games,
                    SUM(CASE WHEN mp_a.team = m.winning_team THEN 1 ELSE 0 END) as wins
                FROM matches m
                JOIN match_players mp_a ON m.match_id = mp_a.match_id AND mp_a.player_id = ?
                JOIN match_players mp_b ON m.match_id = mp_b.match_id AND mp_b.player_id = ?
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL
                  AND mp_a.team = mp_b.team
            """
            async with db.execute(query, (player_a, player_b, game_id)) as cursor:
                row = await cursor.fetchone()
                return {'games': row[0] or 0, 'wins': row[1] or 0}

    @staticmethod
    async def get_h2h_teammate_match_list(player_a: int, player_b: int, game_id: int) -> List[dict]:
        """Get all matches where player_a and player_b were on the SAME team.

        Returns list of dicts with match info including round data,
        ordered by decided_at DESC.
        """
        async with DatabaseHelper._get_db() as db:
            query = """
                SELECT m.match_id, m.winning_team, m.decided_at, m.map_name,
                       mp_a.team AS a_team,
                       m.val_red_rounds, m.val_blue_rounds
                FROM matches m
                JOIN match_players mp_a ON m.match_id = mp_a.match_id AND mp_a.player_id = ?
                JOIN match_players mp_b ON m.match_id = mp_b.match_id AND mp_b.player_id = ?
                WHERE m.game_id = ? AND m.winning_team IS NOT NULL
                  AND mp_a.team = mp_b.team
                ORDER BY m.decided_at DESC
            """
            async with db.execute(query, (player_a, player_b, game_id)) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    @staticmethod
    async def get_h2h_valorant_stats(player_a: int, player_b: int, match_ids: List[int]) -> Dict[int, List[dict]]:
        """Get Valorant stats for both players across specified matches.

        Returns {player_id: [stats_dict_per_match]}.
        """
        if not match_ids:
            return {}
        async with DatabaseHelper._get_db() as db:
            placeholders = ','.join('?' for _ in match_ids)
            query = f"""
                SELECT match_id, player_id, kills, deaths, assists, headshots,
                       bodyshots, legshots, score, damage_dealt, first_bloods,
                       plants, defuses, c2k, c3k, c4k, c5k, map_name
                FROM valorant_match_stats
                WHERE player_id IN (?, ?) AND match_id IN ({placeholders})
            """
            params = [player_a, player_b] + match_ids
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
            result: Dict[int, List[dict]] = {player_a: [], player_b: []}
            for row in rows:
                d = dict(row)
                pid = d['player_id']
                if pid in result:
                    result[pid].append(d)
            return result

    @staticmethod
    async def get_h2h_rivals_stats(player_a: int, player_b: int, match_ids: List[int]) -> Dict[int, List[dict]]:
        """Get Rivals stats for both players across specified matches.

        Returns {player_id: [stats_dict_per_match]}.
        """
        if not match_ids:
            return {}
        async with DatabaseHelper._get_db() as db:
            placeholders = ','.join('?' for _ in match_ids)
            query = f"""
                SELECT match_id, player_id, kills, deaths, assists, final_hits,
                       damage, damage_blocked, healing, accuracy_pct, mvp_svp
                FROM rivals_match_stats
                WHERE player_id IN (?, ?) AND match_id IN ({placeholders})
            """
            params = [player_a, player_b] + match_ids
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
            result: Dict[int, List[dict]] = {player_a: [], player_b: []}
            for row in rows:
                d = dict(row)
                pid = d['player_id']
                if pid in result:
                    result[pid].append(d)
            return result

    @staticmethod
    async def add_win_vote(match_id: int, player_id: int, team: str):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO win_votes (match_id, player_id, voted_team)
                   VALUES (?, ?, ?)""",
                (match_id, player_id, team)
            )
            await db.commit()

    @staticmethod
    async def get_win_votes(match_id: int) -> Dict[str, int]:
        """Returns {team: vote_count}"""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT voted_team, COUNT(*) FROM win_votes WHERE match_id = ? GROUP BY voted_team",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    @staticmethod
    async def get_win_voter_ids(match_id: int) -> set:
        """Returns the set of player_ids who have cast a win vote."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id FROM win_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}

    @staticmethod
    async def get_win_voter_teams(match_id: int) -> Dict[int, str]:
        """Returns {player_id: voted_team} for all voters in a match."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id, voted_team FROM win_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    @staticmethod
    async def get_player_win_vote(match_id: int, player_id: int) -> Optional[str]:
        """Returns the team the player has already voted for, or None."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT voted_team FROM win_votes WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def record_mmr_change(player_id: int, game_id: int, match_id: int,
                                mmr_before: int, mmr_after: int):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT INTO mmr_history (player_id, game_id, match_id, mmr_before, mmr_after, change)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (player_id, game_id, match_id, mmr_before, mmr_after, mmr_after - mmr_before)
            )
            await db.commit()

    @staticmethod
    async def get_leaderboard(game_id: int, monthly: bool = True, limit: int = 20) -> List[dict]:
        """Get leaderboard for a game."""
        async with DatabaseHelper._get_db() as db:
            if monthly:
                # Get current month start
                now = datetime.now(timezone.utc)
                month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                # Get wins/losses from matches this month + admin adjustments
                query = """
                    WITH match_stats AS (
                        SELECT mp.player_id,
                               SUM(CASE WHEN m.winning_team = mp.team THEN 1 ELSE 0 END) as wins,
                               SUM(CASE WHEN m.winning_team != mp.team THEN 1 ELSE 0 END) as losses
                        FROM match_players mp
                        JOIN matches m ON mp.match_id = m.match_id
                        WHERE m.game_id = ? AND m.winning_team IS NOT NULL
                              AND m.decided_at >= ? AND m.cancelled = 0
                        GROUP BY mp.player_id
                    ),
                    admin_stats AS (
                        SELECT player_id,
                               SUM(wins_delta) as wins,
                               SUM(losses_delta) as losses
                        FROM admin_stat_adjustments
                        WHERE game_id = ? AND adjusted_at >= ?
                        GROUP BY player_id
                    ),
                    combined AS (
                        SELECT player_id, wins, losses FROM match_stats
                        UNION ALL
                        SELECT player_id, wins, losses FROM admin_stats
                    )
                    SELECT player_id, SUM(wins) as wins, SUM(losses) as losses
                    FROM combined
                    GROUP BY player_id
                    ORDER BY wins DESC, losses ASC
                    LIMIT ?
                """
                async with db.execute(query, (game_id, month_start.isoformat(),
                                              game_id, month_start.isoformat(), limit)) as cursor:
                    rows = await cursor.fetchall()
            else:
                # All-time from player_game_stats
                query = """
                    SELECT player_id, wins, losses FROM player_game_stats
                    WHERE game_id = ?
                    ORDER BY wins DESC, losses ASC
                    LIMIT ?
                """
                async with db.execute(query, (game_id, limit)) as cursor:
                    rows = await cursor.fetchall()

            return [{"player_id": row[0], "wins": row[1], "losses": row[2]} for row in rows]

    @staticmethod
    async def get_recent_completed_matches(
        game_id: int, limit: int = 5, require_rivals_stats: bool = False
    ) -> List[dict]:
        """Get the most recent completed matches for a game.

        When ``require_rivals_stats`` is True, only matches that have at least
        one row in ``rivals_match_stats`` are returned. This filters out
        abandoned or never-uploaded Rivals matches that would otherwise show
        up as empty/unknown entries in the scoreboard dropdown.
        """
        async with DatabaseHelper._get_db() as db:
            if require_rivals_stats:
                query = """
                    SELECT m.match_id, m.map_name, m.decided_at
                    FROM matches m
                    WHERE m.game_id = ?
                      AND m.winning_team IS NOT NULL
                      AND m.cancelled = 0
                      AND EXISTS (
                          SELECT 1 FROM rivals_match_stats rs
                          WHERE rs.match_id = m.match_id
                      )
                    ORDER BY m.decided_at DESC
                    LIMIT ?
                """
            else:
                query = """
                    SELECT match_id, map_name, decided_at
                    FROM matches
                    WHERE game_id = ? AND winning_team IS NOT NULL AND cancelled = 0
                    ORDER BY decided_at DESC
                    LIMIT ?
                """
            async with db.execute(query, (game_id, limit)) as cursor:
                rows = await cursor.fetchall()
                return [{"match_id": row["match_id"], "map_name": row["map_name"],
                         "decided_at": row["decided_at"]} for row in rows]

    @staticmethod
    async def get_player_leaderboard_rank(player_id: int, game_id: int, monthly: bool = True) -> Optional[int]:
        """Get a player's rank on the leaderboard. Returns None if not on leaderboard."""
        leaderboard = await DatabaseHelper.get_leaderboard(game_id, monthly=monthly, limit=100)
        for i, entry in enumerate(leaderboard, 1):
            if entry["player_id"] == player_id:
                return i
        return None

    @staticmethod
    async def get_player_recent_matches(player_id: int, limit: int = 5) -> List[dict]:
        """Get recent match history for a player."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT m.*, mp.team, g.name as game_name
                   FROM matches m
                   JOIN match_players mp ON m.match_id = mp.match_id
                   JOIN games g ON m.game_id = g.game_id
                   WHERE mp.player_id = ? AND m.winning_team IS NOT NULL
                   ORDER BY m.decided_at DESC
                   LIMIT ?""",
                (player_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_player_in_active_match(player_id: int, game_id: int) -> bool:
        """Check if player is in an active match for a specific game."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT 1 FROM match_players mp
                   JOIN matches m ON mp.match_id = m.match_id
                   WHERE mp.player_id = ? AND m.game_id = ?
                         AND m.winning_team IS NULL AND m.cancelled = 0
                   LIMIT 1""",
                (player_id, game_id)
            ) as cursor:
                return await cursor.fetchone() is not None

    # -------------------------------------------------------------------------
    # IGN METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_player_ign(player_id: int, game_id: int) -> Optional[str]:
        """Get a player's IGN for a specific game."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT ign FROM player_igns WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def set_player_ign(player_id: int, game_id: int, ign: str, puuid: str = None):
        """Set a player's IGN for a specific game, optionally storing the PUUID.
        When no PUUID is provided the existing PUUID is preserved so stats remain
        linked to the player even after an IGN change."""
        async with DatabaseHelper._get_db() as db:
            if puuid is not None:
                await db.execute(
                    """INSERT OR REPLACE INTO player_igns (player_id, game_id, ign, puuid)
                       VALUES (?, ?, ?, ?)""",
                    (player_id, game_id, ign, puuid)
                )
            else:
                # Preserve existing PUUID — only update the IGN field
                await db.execute(
                    """INSERT INTO player_igns (player_id, game_id, ign, puuid)
                       VALUES (?, ?, ?, NULL)
                       ON CONFLICT(player_id, game_id) DO UPDATE SET ign = excluded.ign""",
                    (player_id, game_id, ign)
                )
            await db.commit()

    @staticmethod
    async def get_all_player_igns_for_game(game_id: int) -> List[Tuple[int, str]]:
        """Return [(player_id, ign), ...] for every player with an IGN set for this game."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id, ign FROM player_igns WHERE game_id = ? AND ign IS NOT NULL AND ign != ''",
                (game_id,)
            ) as cursor:
                return [(int(r[0]), r[1]) for r in await cursor.fetchall()]

    @staticmethod
    async def build_ign_lookup(match_id: int, game_id: int) -> Dict[str, int]:
        """Return {lowercase_ign: player_id} for Rivals IGN → player mapping.

        Includes every player with an IGN linked for this game, not just the
        match roster. This is critical so that IGNs manually linked via the
        "Link anyway" path (for players not in match_players) are still
        resolvable on subsequent corrections. Roster entries take priority on
        conflicts so that match-local ownership wins over stale global rows.
        """
        lookup: Dict[str, int] = {}
        # Global pass first — roster entries will overwrite any conflicts.
        try:
            for pid, ign in await DatabaseHelper.get_all_player_igns_for_game(game_id):
                if ign:
                    lookup[ign.strip().lower()] = pid
        except Exception as e:
            logger.error(f"build_ign_lookup: global pass failed for game={game_id}: {e}")
        # Roster pass — ensures current match roster wins on any collision.
        try:
            match_players = await DatabaseHelper.get_match_players(match_id)
            for mp in match_players:
                pid = mp["player_id"]
                ign = await DatabaseHelper.get_player_ign(pid, game_id)
                if ign:
                    lookup[ign.strip().lower()] = pid
        except Exception as e:
            logger.error(f"build_ign_lookup: roster pass failed for match={match_id}: {e}")
        return lookup

    @staticmethod
    async def delete_player_ign(player_id: int, game_id: int):
        """Delete a single player's IGN row for a given game."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM player_igns WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            )
            await db.commit()

    @staticmethod
    async def set_player_role_prefs(player_id: int, game_id: int, primary_role: str, secondary_role: str = None):
        """Set a player's role preferences for a specific game."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO player_role_prefs (player_id, game_id, primary_role, secondary_role)
                   VALUES (?, ?, ?, ?)""",
                (player_id, game_id, primary_role, secondary_role)
            )
            await db.commit()

    @staticmethod
    async def get_player_role_prefs(player_id: int, game_id: int) -> Optional[Tuple[str, Optional[str]]]:
        """Get a player's role preferences for a specific game."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT primary_role, secondary_role FROM player_role_prefs WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                return (row[0], row[1]) if row else None

    @staticmethod
    async def get_bulk_role_prefs(player_ids: List[int], game_id: int) -> Dict[int, Tuple[str, Optional[str]]]:
        """Batch fetch role preferences for multiple players."""
        if not player_ids:
            return {}
        async with DatabaseHelper._get_db() as db:
            placeholders = ",".join("?" for _ in player_ids)
            async with db.execute(
                f"SELECT player_id, primary_role, secondary_role FROM player_role_prefs WHERE game_id = ? AND player_id IN ({placeholders})",
                [game_id] + list(player_ids)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: (row[1], row[2]) for row in rows}

    @staticmethod
    async def get_player_puuid(player_id: int, game_id: int) -> Optional[str]:
        """Get a player's stored PUUID for a specific game."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT puuid FROM player_igns WHERE player_id = ? AND game_id = ? AND puuid IS NOT NULL",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def get_match_puuids(match_id: int) -> Dict[int, str]:
        """Get all PUUIDs for players in a match. Returns {player_id: puuid} for players that have one."""
        async with DatabaseHelper._get_db() as db:
            # First get match game_id
            async with db.execute(
                "SELECT game_id FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {}
                game_id = row[0]

            # Get all player PUUIDs for this game
            async with db.execute(
                """SELECT pi.player_id, pi.puuid FROM player_igns pi
                   JOIN match_players mp ON pi.player_id = mp.player_id
                   WHERE mp.match_id = ? AND pi.game_id = ? AND pi.puuid IS NOT NULL""",
                (match_id, game_id)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    @staticmethod
    async def update_player_puuid(player_id: int, game_id: int, puuid: str):
        """Backfill a player's PUUID in player_igns (only if not already set)."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE player_igns SET puuid = ? WHERE player_id = ? AND game_id = ? AND puuid IS NULL",
                (puuid, player_id, game_id)
            )
            await db.commit()

    @staticmethod
    async def get_match_igns(match_id: int) -> Dict[int, str]:
        """Get all IGNs for players in a match. Returns {player_id: ign}."""
        async with DatabaseHelper._get_db() as db:
            # First get match game_id
            async with db.execute(
                "SELECT game_id FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {}
                game_id = row[0]

            # Get all player IGNs for this game
            async with db.execute(
                """SELECT pi.player_id, pi.ign FROM player_igns pi
                   JOIN match_players mp ON pi.player_id = mp.player_id
                   WHERE mp.match_id = ? AND pi.game_id = ?""",
                (match_id, game_id)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    @staticmethod
    async def get_player_all_igns(player_id: int) -> List[Tuple[int, str, str]]:
        """Get all IGNs for a player. Returns [(game_id, game_name, ign)]."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT pi.game_id, g.name, pi.ign FROM player_igns pi
                   JOIN games g ON pi.game_id = g.game_id
                   WHERE pi.player_id = ?""",
                (player_id,)
            ) as cursor:
                return await cursor.fetchall()

    @staticmethod
    async def get_players_with_ign(player_ids: List[int], game_id: int) -> set:
        """Return the set of player_ids that have an IGN set for the given game."""
        if not player_ids:
            return set()
        async with DatabaseHelper._get_db() as db:
            placeholders = ",".join("?" for _ in player_ids)
            async with db.execute(
                f"SELECT player_id FROM player_igns WHERE game_id = ? AND player_id IN ({placeholders})",
                (game_id, *player_ids)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}

    @staticmethod
    async def get_players_with_roles(player_ids: List[int], game_id: int) -> set:
        """Return the set of player_ids that have role preferences set for the given game."""
        if not player_ids:
            return set()
        async with DatabaseHelper._get_db() as db:
            placeholders = ",".join("?" for _ in player_ids)
            async with db.execute(
                f"SELECT player_id FROM player_role_prefs WHERE game_id = ? AND player_id IN ({placeholders})",
                (game_id, *player_ids)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}

    @staticmethod
    async def get_match_sub_mappings(match_id: int) -> Dict[int, int]:
        """Get substitute mappings for a match. Returns {sub_player_id: original_player_id}."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id, original_player_id FROM match_players "
                "WHERE match_id = ? AND was_sub = 1 AND original_player_id IS NOT NULL",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    # -------------------------------------------------------------------------
    # READY PENALTY METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_ready_penalty(player_id: int) -> ReadyPenalty:
        """Get a player's ready penalty status."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM ready_penalties WHERE player_id = ?",
                (player_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return ReadyPenalty(player_id=player_id)
                return ReadyPenalty(
                    player_id=row[0],
                    offense_count=row[1] or 0,
                    penalty_expires=datetime.fromisoformat(row[2]) if row[2] else None,
                    last_offense=datetime.fromisoformat(row[3]) if row[3] else None
                )

    @staticmethod
    async def add_ready_penalty_offense(player_id: int, game: GameConfig) -> Tuple[int, Optional[datetime]]:
        """Add a ready penalty offense. Returns (new_offense_count, penalty_expires)."""
        penalty = await DatabaseHelper.get_ready_penalty(player_id)
        now = datetime.now(timezone.utc)

        # Check for decay
        if penalty.last_offense:
            days_since = (now - penalty.last_offense).days
            if days_since >= game.penalty_decay_days:
                penalty.offense_count = 0

        penalty.offense_count += 1
        penalty.last_offense = now

        # Determine penalty duration based on offense count
        if penalty.offense_count == 1:
            duration_minutes = game.penalty_1st_minutes
        elif penalty.offense_count == 2:
            duration_minutes = game.penalty_2nd_minutes
        else:
            duration_minutes = game.penalty_3rd_minutes

        penalty.penalty_expires = now + timedelta(minutes=duration_minutes)

        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO ready_penalties
                   (player_id, offense_count, penalty_expires, last_offense)
                   VALUES (?, ?, ?, ?)""",
                (player_id, penalty.offense_count,
                 penalty.penalty_expires.isoformat(),
                 penalty.last_offense.isoformat())
            )
            await db.commit()

        return penalty.offense_count, penalty.penalty_expires

    @staticmethod
    async def clear_ready_penalty(player_id: int):
        """Clear a player's ready penalty (admin action)."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM ready_penalties WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()

    @staticmethod
    async def get_all_penalties() -> List[ReadyPenalty]:
        """Get all active penalties."""
        now = datetime.now(timezone.utc).isoformat()
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM ready_penalties WHERE penalty_expires > ?",
                (now,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    ReadyPenalty(
                        player_id=row[0],
                        offense_count=row[1] or 0,
                        penalty_expires=datetime.fromisoformat(row[2]) if row[2] else None,
                        last_offense=datetime.fromisoformat(row[3]) if row[3] else None
                    )
                    for row in rows
                ]

    @staticmethod
    async def is_penalized(player_id: int) -> Optional[datetime]:
        """Check if a player is currently penalized. Returns expiry time or None."""
        penalty = await DatabaseHelper.get_ready_penalty(player_id)
        if penalty.penalty_expires and penalty.penalty_expires > datetime.now(timezone.utc):
            return penalty.penalty_expires
        return None

    # -------------------------------------------------------------------------
    # DECLINE PENALTY METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_decline_penalty(player_id: int) -> ReadyPenalty:
        """Get a player's decline penalty status."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM decline_penalties WHERE player_id = ?",
                (player_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return ReadyPenalty(player_id=player_id)
                return ReadyPenalty(
                    player_id=row[0],
                    offense_count=row[1] or 0,
                    penalty_expires=datetime.fromisoformat(row[2]) if row[2] else None,
                    last_offense=datetime.fromisoformat(row[3]) if row[3] else None
                )

    @staticmethod
    async def add_decline_penalty_offense(player_id: int, game: GameConfig) -> Tuple[int, Optional[datetime]]:
        """Add a decline penalty offense. Returns (new_offense_count, penalty_expires)."""
        penalty = await DatabaseHelper.get_decline_penalty(player_id)
        now = datetime.now(timezone.utc)

        # Check for decay (uses same decay period as ready penalties)
        if penalty.last_offense:
            days_since = (now - penalty.last_offense).days
            if days_since >= game.penalty_decay_days:
                penalty.offense_count = 0

        penalty.offense_count += 1
        penalty.last_offense = now

        # Determine penalty duration based on offense count
        if penalty.offense_count == 1:
            duration_minutes = game.decline_1st_minutes
        elif penalty.offense_count == 2:
            duration_minutes = game.decline_2nd_minutes
        else:
            duration_minutes = game.decline_3rd_minutes

        penalty.penalty_expires = now + timedelta(minutes=duration_minutes)

        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO decline_penalties
                   (player_id, offense_count, penalty_expires, last_offense)
                   VALUES (?, ?, ?, ?)""",
                (player_id, penalty.offense_count,
                 penalty.penalty_expires.isoformat(),
                 penalty.last_offense.isoformat())
            )
            await db.commit()

        return penalty.offense_count, penalty.penalty_expires

    @staticmethod
    async def clear_decline_penalty(player_id: int):
        """Clear a player's decline penalty (admin action)."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM decline_penalties WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()

    @staticmethod
    async def get_all_decline_penalties() -> List[ReadyPenalty]:
        """Get all active decline penalties."""
        now = datetime.now(timezone.utc).isoformat()
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM decline_penalties WHERE penalty_expires > ?",
                (now,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    ReadyPenalty(
                        player_id=row[0],
                        offense_count=row[1] or 0,
                        penalty_expires=datetime.fromisoformat(row[2]) if row[2] else None,
                        last_offense=datetime.fromisoformat(row[3]) if row[3] else None
                    )
                    for row in rows
                ]

    # -------------------------------------------------------------------------
    # SUSPENSION METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def add_suspension(player_id: int, game_id: Optional[int], until: datetime,
                             reason: Optional[str], suspended_by: Optional[int]) -> int:
        """Add a suspension. Returns suspension_id."""
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                """INSERT INTO suspensions (player_id, game_id, suspended_until, reason, suspended_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (player_id, game_id, until.isoformat(), reason, suspended_by)
            )
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def remove_suspension(suspension_id: int):
        """Remove a suspension."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM suspensions WHERE suspension_id = ?",
                (suspension_id,)
            )
            await db.commit()

    @staticmethod
    async def is_suspended(player_id: int, game_id: int) -> Optional[Suspension]:
        """Check if a player is suspended for a game. Returns Suspension or None."""
        now = datetime.now(timezone.utc).isoformat()
        async with DatabaseHelper._get_db() as db:
            # Check for game-specific or all-game suspension
            async with db.execute(
                """SELECT * FROM suspensions
                   WHERE player_id = ? AND suspended_until > ?
                         AND (game_id = ? OR game_id IS NULL)
                   ORDER BY suspended_until DESC LIMIT 1""",
                (player_id, now, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return Suspension(
                    suspension_id=row[0],
                    player_id=row[1],
                    game_id=row[2],
                    suspended_until=datetime.fromisoformat(row[3]),
                    reason=row[4],
                    suspended_by=row[5],
                    created_at=datetime.fromisoformat(row[6]) if row[6] else datetime.now(timezone.utc)
                )

    @staticmethod
    async def get_all_suspensions() -> List[Suspension]:
        """Get all active suspensions."""
        now = datetime.now(timezone.utc).isoformat()
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM suspensions WHERE suspended_until > ?",
                (now,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Suspension(
                        suspension_id=row[0],
                        player_id=row[1],
                        game_id=row[2],
                        suspended_until=datetime.fromisoformat(row[3]),
                        reason=row[4],
                        suspended_by=row[5],
                        created_at=datetime.fromisoformat(row[6]) if row[6] else datetime.now(timezone.utc)
                    )
                    for row in rows
                ]

    # -------------------------------------------------------------------------
    # ABANDON VOTE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def add_abandon_vote(match_id: int, player_id: int):
        """Add an abandon vote."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO abandon_votes (match_id, player_id)
                   VALUES (?, ?)""",
                (match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_abandon_votes(match_id: int) -> int:
        """Get number of abandon votes for a match."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM abandon_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    @staticmethod
    async def has_voted_abandon(match_id: int, player_id: int) -> bool:
        """Check if a player has already voted to abandon."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT 1 FROM abandon_votes WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            ) as cursor:
                return await cursor.fetchone() is not None

    # -------------------------------------------------------------------------
    # SHUFFLE VOTE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def add_shuffle_vote(match_id: int, player_id: int):
        """Add a shuffle vote."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "INSERT OR IGNORE INTO shuffle_votes (match_id, player_id) VALUES (?, ?)",
                (match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_shuffle_vote_count(match_id: int) -> int:
        """Get number of shuffle votes for a match."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM shuffle_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    @staticmethod
    async def get_shuffle_voters(match_id: int) -> List[int]:
        """Get player IDs who voted to shuffle."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id FROM shuffle_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    @staticmethod
    async def has_voted_shuffle(match_id: int, player_id: int) -> bool:
        """Check if a player has already voted to shuffle."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT 1 FROM shuffle_votes WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            ) as cursor:
                return await cursor.fetchone() is not None

    # -------------------------------------------------------------------------
    # STATS METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def delete_player_stats(player_id: int):
        """Remove a player's active presence when they leave the server.

        Intentionally preserves player_game_stats and mmr_history so that MMR
        is intact if the player rejoins. Only clears leaderboard visibility,
        IGNs, and rivalry records.
        """
        async with DatabaseHelper._get_db() as db:
            # Delete from rivalries (both sides)
            await db.execute(
                "DELETE FROM rivalries WHERE player_a_id = ? OR player_b_id = ?",
                (player_id, player_id)
            )
            # Delete IGNs
            await db.execute("DELETE FROM player_igns WHERE player_id = ?", (player_id,))
            # Remove from leaderboard-visible players table
            await db.execute("DELETE FROM players WHERE player_id = ?", (player_id,))
            await db.commit()

    @staticmethod
    async def adjust_player_stats(player_id: int, game_id: int, wins_delta: int, losses_delta: int,
                                   adjusted_by: Optional[int] = None):
        """Adjust a player's wins/losses by specified amounts. Records for monthly leaderboard."""
        stats = await DatabaseHelper.get_player_stats(player_id, game_id)
        stats.wins = max(0, stats.wins + wins_delta)
        stats.losses = max(0, stats.losses + losses_delta)
        stats.games_played = stats.wins + stats.losses
        await DatabaseHelper.update_player_stats(stats)

        # Also record in admin_stat_adjustments for monthly leaderboard tracking
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT INTO admin_stat_adjustments
                   (player_id, game_id, wins_delta, losses_delta, adjusted_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (player_id, game_id, wins_delta, losses_delta, adjusted_by)
            )
            await db.commit()

    @staticmethod
    async def reconcile_player_stats() -> int:
        """Recalculate player wins/losses/games_played from match data + admin adjustments.
        Returns the number of players updated."""
        async with DatabaseHelper._get_db() as db:
            # Get correct match-based stats
            async with db.execute("""
                SELECT mp.player_id, m.game_id,
                       SUM(CASE WHEN m.winning_team = mp.team THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN m.winning_team != mp.team THEN 1 ELSE 0 END) as losses
                FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.winning_team IS NOT NULL AND m.cancelled = 0
                GROUP BY mp.player_id, m.game_id
            """) as cursor:
                match_stats = await cursor.fetchall()

            # Get admin adjustments
            admin_adj = {}
            async with db.execute("""
                SELECT player_id, game_id,
                       SUM(wins_delta) as wins_adj,
                       SUM(losses_delta) as losses_adj
                FROM admin_stat_adjustments
                GROUP BY player_id, game_id
            """) as cursor:
                for row in await cursor.fetchall():
                    admin_adj[(row[0], row[1])] = (row[2] or 0, row[3] or 0)

            try:
                # Build batch update parameters
                update_params = []
                for row in match_stats:
                    player_id, game_id, match_wins, match_losses = row
                    adj = admin_adj.get((player_id, game_id), (0, 0))
                    total_wins = max(0, match_wins + adj[0])
                    total_losses = max(0, match_losses + adj[1])
                    games_played = total_wins + total_losses
                    update_params.append((
                        total_wins, total_losses, games_played,
                        player_id, game_id,
                        total_wins, total_losses, games_played
                    ))

                if update_params:
                    await db.executemany("""
                        UPDATE player_game_stats
                        SET wins = ?, losses = ?, games_played = ?
                        WHERE player_id = ? AND game_id = ?
                        AND (wins != ? OR losses != ? OR games_played != ?)
                    """, update_params)

                await db.commit()
                return db.total_changes
            except Exception as e:
                logger.error(f"reconcile_player_stats failed mid-update, rolling back: {e}")
                await db.rollback()
                return 0

    @staticmethod
    async def get_completed_match(match_id: int) -> Optional[dict]:
        """Get a match even if it's already decided."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def reverse_match_result(match_id: int) -> bool:
        """Reverse all MMR changes from a completed match. Returns True if successful."""
        async with DatabaseHelper._get_db() as db:
            # Get MMR history for this match
            async with db.execute(
                "SELECT player_id, game_id, mmr_before, mmr_after FROM mmr_history WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                return False

            # Reverse all changes atomically within this single connection
            for player_id, game_id, mmr_before, mmr_after in rows:
                change = mmr_after - mmr_before
                if change > 0:  # Was a winner
                    await db.execute(
                        """UPDATE player_game_stats
                           SET mmr = mmr - ?, wins = MAX(0, wins - 1),
                               games_played = MAX(0, wins - 1) + losses
                           WHERE player_id = ? AND game_id = ?""",
                        (change, player_id, game_id)
                    )
                else:  # Was a loser
                    await db.execute(
                        """UPDATE player_game_stats
                           SET mmr = mmr - ?, losses = MAX(0, losses - 1),
                               games_played = wins + MAX(0, losses - 1)
                           WHERE player_id = ? AND game_id = ?""",
                        (change, player_id, game_id)
                    )

            # Delete the MMR history entries
            await db.execute("DELETE FROM mmr_history WHERE match_id = ?", (match_id,))

            # Clear the winning_team so the match can be re-decided
            await db.execute(
                "UPDATE matches SET winning_team = NULL, decided_at = NULL, ended_at = NULL WHERE match_id = ?",
                (match_id,)
            )
            await db.commit()

        return True

    # -------------------------------------------------------------------------
    # SECONDARY MODE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_secondary_modes(game_id: int) -> list:
        """Get all modes for a game's secondary queue, ordered by display_order."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT mode_id, game_id, mode_name, map_pool_type, custom_maps, description, is_ffa, is_mirror, display_order "
                "FROM secondary_modes WHERE game_id = ? ORDER BY display_order, mode_id",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    custom_maps = json.loads(row[4]) if row[4] else None
                    results.append({
                        "mode_id": row[0],
                        "game_id": row[1],
                        "mode_name": row[2],
                        "map_pool_type": row[3],
                        "custom_maps": custom_maps,
                        "description": row[5],
                        "is_ffa": bool(row[6]),
                        "is_mirror": bool(row[7]),
                        "display_order": row[8],
                    })
                return results

    @staticmethod
    async def add_secondary_mode(game_id: int, mode_name: str,
                                  map_pool_type: str = "none",
                                  custom_maps: list = None) -> int:
        """Add a mode. Returns mode_id. Raises ValueError if 5 modes already exist."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM secondary_modes WHERE game_id = ?", (game_id,)
            ) as cursor:
                count = (await cursor.fetchone())[0]
                if count >= 5:
                    raise ValueError("Maximum of 5 modes per game.")
            custom_json = json.dumps(custom_maps) if custom_maps else None
            cursor = await db.execute(
                "INSERT INTO secondary_modes (game_id, mode_name, map_pool_type, custom_maps, display_order) "
                "VALUES (?, ?, ?, ?, ?)",
                (game_id, mode_name, map_pool_type, custom_json, count)
            )
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def remove_secondary_mode(mode_id: int) -> bool:
        """Remove a mode by ID. Returns True if deleted."""
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                "DELETE FROM secondary_modes WHERE mode_id = ?", (mode_id,)
            )
            await db.commit()
            return cursor.rowcount > 0

    @staticmethod
    async def update_secondary_mode(mode_id: int, **kwargs):
        """Update mode fields (mode_name, map_pool_type, custom_maps, display_order)."""
        valid = {'mode_name', 'map_pool_type', 'custom_maps', 'description', 'is_ffa', 'is_mirror', 'display_order'}
        async with DatabaseHelper._get_db() as db:
            for key, value in kwargs.items():
                if key not in valid:
                    raise ValueError(f"Invalid column: {key}")
                if key == 'custom_maps' and isinstance(value, list):
                    value = json.dumps(value)
                await db.execute(
                    f"UPDATE secondary_modes SET {key} = ? WHERE mode_id = ?",
                    (value, mode_id)
                )
            await db.commit()

    # -------------------------------------------------------------------------
    # QUEUE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def clear_queue(queue_id: int):
        """Clear all players from a queue."""
        async with DatabaseHelper._get_db() as db:
            await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (queue_id,))
            await db.commit()

    @staticmethod
    async def remove_player_from_queue(queue_id: int, player_id: int):
        """Remove a specific player from a queue."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                (queue_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_queue_join_times(queue_id: int) -> Dict[int, datetime]:
        """Get join times for all players in a queue. Returns {player_id: joined_at}."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id, joined_at FROM queue_players WHERE queue_id = ?",
                (queue_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {
                    row[0]: datetime.fromisoformat(row[1]).replace(tzinfo=timezone.utc) if row[1] else datetime.now(timezone.utc)
                    for row in rows
                }

    # -------------------------------------------------------------------------
    # QUEUE SUBSCRIBER METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def subscribe_to_queue(queue_id: int, player_id: int, threshold: int):
        """Subscribe a player to get DM'd when a queue needs <= threshold more players."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "INSERT OR REPLACE INTO queue_subscribers (queue_id, player_id, threshold) VALUES (?, ?, ?)",
                (queue_id, player_id, threshold)
            )
            await db.commit()

    @staticmethod
    async def unsubscribe_from_queue(queue_id: int, player_id: int):
        """Unsubscribe a player from queue notifications."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM queue_subscribers WHERE queue_id = ? AND player_id = ?",
                (queue_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_queue_subscribers(queue_id: int, max_threshold: int) -> List[Tuple[int, int]]:
        """Get subscribers whose threshold >= remaining slots. Returns [(player_id, threshold)]."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT player_id, threshold FROM queue_subscribers WHERE queue_id = ? AND threshold >= ? AND created_at > datetime('now', '-60 minutes')",
                (queue_id, max_threshold)
            ) as cursor:
                return [(row[0], row[1]) for row in await cursor.fetchall()]

    @staticmethod
    async def get_subscriber_count(queue_id: int) -> int:
        """Count non-expired subscribers (lurkers) for a queue."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM queue_subscribers WHERE queue_id = ? AND created_at > datetime('now', '-60 minutes')",
                (queue_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    @staticmethod
    async def clear_queue_subscribers(queue_id: int):
        """Clear all subscribers for a queue."""
        async with DatabaseHelper._get_db() as db:
            await db.execute("DELETE FROM queue_subscribers WHERE queue_id = ?", (queue_id,))
            await db.commit()

    @staticmethod
    async def get_player_subscription(queue_id: int, player_id: int) -> Optional[int]:
        """Get a player's subscription threshold for a queue. Returns threshold or None if expired/missing."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT threshold FROM queue_subscribers WHERE queue_id = ? AND player_id = ? AND created_at > datetime('now', '-60 minutes')",
                (queue_id, player_id)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    # -------------------------------------------------------------------------
    # VALORANT STATS METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def save_valorant_match_stats(
        match_id: int, valorant_match_id: str, player_id: int, ign: str,
        agent: str, kills: int, deaths: int, assists: int,
        headshots: int, bodyshots: int, legshots: int, score: int, map_name: str,
        damage_dealt: int = 0, first_bloods: int = 0, plants: int = 0, defuses: int = 0,
        c2k: int = 0, c3k: int = 0, c4k: int = 0, c5k: int = 0,
        econ_spent: int = 0, econ_loadout: int = 0
    ):
        """Save Valorant match stats for a player."""
        async with DatabaseHelper._get_db() as db:
            # Guard: if this Valorant match UUID is already stored for a *different* match,
            # skip the insert to avoid corrupting the existing match's stats.
            if valorant_match_id:
                async with db.execute(
                    "SELECT match_id FROM valorant_match_stats WHERE valorant_match_id = ? AND match_id != ? LIMIT 1",
                    (valorant_match_id, match_id)
                ) as cursor:
                    conflict_row = await cursor.fetchone()
                if conflict_row:
                    logger.warning(
                        f"save_valorant_match_stats: valorant_match_id={valorant_match_id} already "
                        f"linked to match #{conflict_row[0]}, refusing to overwrite for match #{match_id}"
                    )
                    return
            await db.execute("""
                INSERT OR REPLACE INTO valorant_match_stats
                (match_id, valorant_match_id, player_id, ign, agent, kills, deaths, assists,
                 headshots, bodyshots, legshots, score, map_name, damage_dealt, first_bloods,
                 plants, defuses, c2k, c3k, c4k, c5k, econ_spent, econ_loadout)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (match_id, valorant_match_id, player_id, ign, agent, kills, deaths, assists,
                  headshots, bodyshots, legshots, score, map_name, damage_dealt, first_bloods,
                  plants, defuses, c2k, c3k, c4k, c5k, econ_spent, econ_loadout))
            await db.commit()

    @staticmethod
    async def get_valorant_match_stats(match_id: int) -> List[dict]:
        """Get all Valorant stats for a match."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM valorant_match_stats WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def check_valorant_match_id_used(valorant_match_id: str) -> Optional[int]:
        """Check if a Valorant match ID is already stored. Returns the match_id if found."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT DISTINCT match_id FROM valorant_match_stats WHERE valorant_match_id = ? LIMIT 1",
                (valorant_match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def clear_valorant_match_stats(match_id: int) -> int:
        """Clear all Valorant stats for a match. Returns number of rows deleted."""
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                "DELETE FROM valorant_match_stats WHERE match_id = ?",
                (match_id,)
            )
            await db.commit()
            return cursor.rowcount

    @staticmethod
    async def get_valorant_player_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get aggregated Valorant stats for a player."""
        async with DatabaseHelper._get_db() as db:

            # Build date filter for monthly stats
            date_filter = ""
            if monthly:
                date_filter = "AND vms.fetched_at >= strftime('%Y-%m-01', 'now')"

            # Get aggregate stats (deduplicate by valorant_match_id to prevent cross-match dupes)
            query = f"""
                SELECT
                    COUNT(*) as total_games,
                    COALESCE(SUM(vms.kills), 0) as total_kills,
                    COALESCE(SUM(vms.deaths), 0) as total_deaths,
                    COALESCE(SUM(vms.assists), 0) as total_assists,
                    COALESCE(SUM(vms.headshots), 0) as total_headshots,
                    COALESCE(SUM(vms.bodyshots), 0) as total_bodyshots,
                    COALESCE(SUM(vms.legshots), 0) as total_legshots,
                    COALESCE(SUM(vms.score), 0) as total_score,
                    COALESCE(SUM(vms.damage_dealt), 0) as total_damage,
                    COALESCE(SUM(vms.first_bloods), 0) as total_first_bloods
                FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE vms.player_id = ? AND m.game_id = ? {date_filter}
                    AND vms.id = (
                        SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id
                        AND v2.player_id = vms.player_id
                    )
            """
            async with db.execute(query, (player_id, game_id)) as cursor:
                row = await cursor.fetchone()
                stats = dict(row) if row else {}

            # Calculate HS%
            total_shots = stats.get('total_headshots', 0) + stats.get('total_bodyshots', 0) + stats.get('total_legshots', 0)
            stats['hs_percent'] = round((stats.get('total_headshots', 0) / total_shots * 100), 1) if total_shots > 0 else 0

            # Get map stats with wins/losses (deduplicate by valorant_match_id)
            map_query = f"""
                SELECT
                    vms.map_name,
                    COUNT(*) as games,
                    SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as wins
                FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                JOIN match_players mp ON m.match_id = mp.match_id AND mp.player_id = vms.player_id
                WHERE vms.player_id = ? AND m.game_id = ? AND vms.map_name IS NOT NULL {date_filter}
                    AND vms.id = (
                        SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id
                        AND v2.player_id = vms.player_id
                    )
                GROUP BY vms.map_name
                ORDER BY CAST(wins AS FLOAT) / games DESC
            """
            async with db.execute(map_query, (player_id, game_id)) as cursor:
                map_rows = await cursor.fetchall()
                stats['map_stats'] = []
                for row in map_rows:
                    wins = row['wins']
                    losses = row['games'] - wins
                    winrate = round(wins / row['games'] * 100, 1) if row['games'] > 0 else 0
                    stats['map_stats'].append({
                        'name': row['map_name'],
                        'games': row['games'],
                        'wins': wins,
                        'losses': losses,
                        'winrate': winrate
                    })
                if map_rows:
                    best = map_rows[0]
                    worst = map_rows[-1]
                    stats['best_map'] = {
                        'name': best['map_name'],
                        'games': best['games'],
                        'winrate': round(best['wins'] / best['games'] * 100, 1)
                    }
                    stats['worst_map'] = {
                        'name': worst['map_name'],
                        'games': worst['games'],
                        'winrate': round(worst['wins'] / worst['games'] * 100, 1)
                    }

            # Get agent stats with wins/losses (deduplicate by valorant_match_id)
            agent_query = f"""
                SELECT
                    vms.agent,
                    COUNT(*) as games,
                    SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as wins
                FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                JOIN match_players mp ON m.match_id = mp.match_id AND mp.player_id = vms.player_id
                WHERE vms.player_id = ? AND m.game_id = ? AND vms.agent IS NOT NULL {date_filter}
                    AND vms.id = (
                        SELECT MIN(v2.id) FROM valorant_match_stats v2
                        WHERE v2.valorant_match_id = vms.valorant_match_id
                        AND v2.player_id = vms.player_id
                    )
                GROUP BY vms.agent
                ORDER BY games DESC
            """
            async with db.execute(agent_query, (player_id, game_id)) as cursor:
                agent_rows = await cursor.fetchall()
                stats['agent_stats'] = {}
                for row in agent_rows:
                    wins = row['wins']
                    losses = row['games'] - wins
                    stats['agent_stats'][row['agent']] = {
                        'games': row['games'],
                        'wins': wins,
                        'losses': losses
                    }

            return stats

    @staticmethod
    async def get_player_teammate_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get teammate win rates for a player."""
        async with DatabaseHelper._get_db() as db:

            date_filter = ""
            if monthly:
                date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')"

            query = f"""
                SELECT
                    mp2.player_id as teammate_id,
                    COUNT(*) as games_together,
                    SUM(CASE WHEN mp1.team = m.winning_team THEN 1 ELSE 0 END) as wins_together
                FROM match_players mp1
                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team = mp2.team
                JOIN matches m ON mp1.match_id = m.match_id
                WHERE mp1.player_id = ? AND mp2.player_id != ? AND m.game_id = ?
                      AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp2.player_id
                HAVING games_together >= 3
                ORDER BY CAST(wins_together AS FLOAT) / games_together DESC
            """
            async with db.execute(query, (player_id, player_id, game_id)) as cursor:
                rows = await cursor.fetchall()

            result = {}
            if rows:
                best = rows[0]
                worst = rows[-1]
                result['best_teammate'] = {
                    'player_id': best['teammate_id'],
                    'games': best['games_together'],
                    'winrate': round(best['wins_together'] / best['games_together'] * 100, 1)
                }
                result['cursed_teammate'] = {
                    'player_id': worst['teammate_id'],
                    'games': worst['games_together'],
                    'winrate': round(worst['wins_together'] / worst['games_together'] * 100, 1)
                }
            return result

    @staticmethod
    async def get_player_recent_matches(player_id: int, game_id: int, limit: int = 5, monthly: bool = False) -> List[dict]:
        """Get recent matches with Valorant stats for a player. Deduplicates by valorant_match_id."""
        async with DatabaseHelper._get_db() as db:
            date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')" if monthly else ""
            query = f"""
                SELECT
                    m.match_id, m.short_id, m.winning_team, m.decided_at,
                    m.map_name as match_map_name,
                    mp.team,
                    vms.kills, vms.deaths, vms.assists, vms.agent, vms.map_name,
                    vms.valorant_match_id
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                LEFT JOIN valorant_match_stats vms ON m.match_id = vms.match_id AND vms.player_id = mp.player_id
                WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                ORDER BY m.decided_at DESC
                LIMIT ?
            """
            async with db.execute(query, (player_id, game_id, limit)) as cursor:
                rows = await cursor.fetchall()
                # Deduplicate: if same valorant_match_id appears for multiple matches, keep first
                seen_val_ids = set()
                results = []
                for row in rows:
                    row_dict = dict(row)
                    val_id = row_dict.get('valorant_match_id')
                    if val_id and val_id in seen_val_ids:
                        continue
                    if val_id:
                        seen_val_ids.add(val_id)
                    results.append(row_dict)
                return results

    @staticmethod
    async def get_player_monthly_wins_losses(player_id: int, game_id: int) -> tuple:
        """Get wins and losses for a player from match history this month."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT
                    SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN mp.team != m.winning_team THEN 1 ELSE 0 END) as losses
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                WHERE mp.player_id = ? AND m.game_id = ?
                      AND m.winning_team IS NOT NULL AND m.cancelled = 0
                      AND m.decided_at >= strftime('%Y-%m-01', 'now')""",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return (row[0] or 0, row[1] or 0)
                return (0, 0)

    @staticmethod
    async def get_player_map_stats(player_id: int, game_id: int, monthly: bool = False) -> list:
        """Get per-map W/L stats for a player (non-Valorant games)."""
        async with DatabaseHelper._get_db() as db:
            date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')" if monthly else ""
            query = f"""
                SELECT
                    m.map_name,
                    COUNT(*) as games_played,
                    SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN mp.team != m.winning_team THEN 1 ELSE 0 END) as losses
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                WHERE mp.player_id = ? AND m.game_id = ?
                      AND m.winning_team IS NOT NULL AND m.map_name IS NOT NULL {date_filter}
                GROUP BY m.map_name
                ORDER BY games_played DESC
            """
            async with db.execute(query, (player_id, game_id)) as cursor:
                rows = await cursor.fetchall()
                return [{'name': r['map_name'], 'wins': r['wins'], 'losses': r['losses']} for r in rows]

    @staticmethod
    async def get_player_match_stats(player_id: int, match_id: int) -> dict:
        """Get full Valorant stats for a specific match."""
        async with DatabaseHelper._get_db() as db:
            query = """
                SELECT
                    vms.agent, vms.kills, vms.deaths, vms.assists,
                    vms.headshots, vms.bodyshots, vms.legshots,
                    vms.score, vms.damage_dealt, vms.first_bloods, vms.map_name,
                    vms.plants, vms.defuses, vms.c2k, vms.c3k, vms.c4k, vms.c5k,
                    vms.econ_spent, vms.econ_loadout
                FROM valorant_match_stats vms
                WHERE vms.player_id = ? AND vms.match_id = ?
            """
            async with db.execute(query, (player_id, match_id)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else {}

    @staticmethod
    async def get_player_streak_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get longest winning and losing streaks for a player."""
        async with DatabaseHelper._get_db() as db:

            date_filter = ""
            if monthly:
                date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')"

            query = f"""
                SELECT
                    m.match_id,
                    CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END as won
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                ORDER BY m.decided_at ASC
            """
            async with db.execute(query, (player_id, game_id)) as cursor:
                rows = await cursor.fetchall()

            # Calculate streaks
            longest_win_streak = 0
            longest_loss_streak = 0
            current_win_streak = 0
            current_loss_streak = 0

            for row in rows:
                if row['won']:
                    current_win_streak += 1
                    current_loss_streak = 0
                    longest_win_streak = max(longest_win_streak, current_win_streak)
                else:
                    current_loss_streak += 1
                    current_win_streak = 0
                    longest_loss_streak = max(longest_loss_streak, current_loss_streak)

            return {
                'longest_win_streak': longest_win_streak,
                'longest_loss_streak': longest_loss_streak
            }

    @staticmethod
    async def get_current_win_streaks_batch(player_ids: List[int], game_id: int) -> Dict[int, int]:
        """Get current win streak for multiple players in a single query.

        Returns {player_id: streak_count} for players with streaks >= 3.
        """
        if not player_ids:
            return {}
        async with DatabaseHelper._get_db() as db:
            placeholders = ','.join('?' for _ in player_ids)
            query = f"""
                SELECT mp.player_id, m.match_id,
                       CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END as won,
                       m.decided_at
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                WHERE mp.player_id IN ({placeholders})
                  AND m.game_id = ? AND m.winning_team IS NOT NULL
                ORDER BY mp.player_id, m.decided_at DESC
            """
            async with db.execute(query, (*player_ids, game_id)) as cursor:
                rows = await cursor.fetchall()

        # Group by player and count consecutive wins from most recent
        streaks: Dict[int, int] = {}
        current_pid = None
        streak = 0
        for row in rows:
            pid = row['player_id']
            if pid != current_pid:
                if current_pid is not None and streak is not None and streak >= 3:
                    streaks[current_pid] = streak
                current_pid = pid
                streak = 0
            if streak is not None:
                if row['won']:
                    streak += 1
                else:
                    if streak >= 3:
                        streaks[pid] = streak
                    streak = None  # Stop counting for this player
        # Handle last player
        if current_pid is not None and streak is not None and streak >= 3:
            streaks[current_pid] = streak

        return streaks

    @staticmethod
    async def get_all_teammate_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get all teammate stats for display (top 3 best, top 3 worst)."""
        async with DatabaseHelper._get_db() as db:

            date_filter = ""
            if monthly:
                date_filter = "AND m.decided_at >= strftime('%Y-%m-01', 'now')"

            query = f"""
                SELECT
                    mp2.player_id as teammate_id,
                    COUNT(*) as games_together,
                    SUM(CASE WHEN mp1.team = m.winning_team THEN 1 ELSE 0 END) as wins_together
                FROM match_players mp1
                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team = mp2.team
                JOIN matches m ON mp1.match_id = m.match_id
                WHERE mp1.player_id = ? AND mp2.player_id != ? AND m.game_id = ?
                      AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp2.player_id
                HAVING games_together >= 3
                ORDER BY CAST(wins_together AS FLOAT) / games_together DESC
            """
            async with db.execute(query, (player_id, player_id, game_id)) as cursor:
                rows = await cursor.fetchall()

            result = {'best_teammates': [], 'worst_teammates': []}
            if rows:
                best_ids = set()
                # Best teammates: winning record (wins > losses), sorted by winrate DESC
                for row in rows:
                    wins = row['wins_together']
                    losses = row['games_together'] - wins
                    if wins > losses:
                        result['best_teammates'].append({
                            'player_id': row['teammate_id'],
                            'games': row['games_together'],
                            'wins': wins,
                            'losses': losses,
                            'winrate': round(wins / row['games_together'] * 100, 1)
                        })
                        best_ids.add(row['teammate_id'])
                    if len(result['best_teammates']) >= 3:
                        break
                # Cursed teammates: losing record (losses > wins), sorted by winrate ASC
                for row in reversed(rows):
                    wins = row['wins_together']
                    losses = row['games_together'] - wins
                    if losses > wins and row['teammate_id'] not in best_ids:
                        result['worst_teammates'].append({
                            'player_id': row['teammate_id'],
                            'games': row['games_together'],
                            'wins': wins,
                            'losses': losses,
                            'winrate': round(wins / row['games_together'] * 100, 1)
                        })
                    if len(result['worst_teammates']) >= 3:
                        break
            return result

    @staticmethod
    async def mark_valorant_regular(player_id: int, game_id: int, ign: str, puuid: str = None, region: str = 'na'):
        """Mark a player as a verified Valorant regular for API lookups."""
        async with DatabaseHelper._get_db() as db:
            await db.execute("""
                INSERT OR REPLACE INTO valorant_player_regulars
                (player_id, game_id, ign, puuid, region, verified_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (player_id, game_id, ign, puuid, region))
            await db.commit()

    @staticmethod
    async def get_valorant_regulars(game_id: int) -> List[dict]:
        """Get all verified Valorant regulars for a game."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM valorant_player_regulars WHERE game_id = ?",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_mod_roles() -> List[int]:
        """Get all mod role IDs."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute("SELECT role_id FROM mod_roles") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    @staticmethod
    async def add_mod_role(role_id: int):
        """Add a mod role."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "INSERT OR IGNORE INTO mod_roles (role_id) VALUES (?)",
                (role_id,)
            )
            await db.commit()

    @staticmethod
    async def remove_mod_role(role_id: int):
        """Remove a mod role."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM mod_roles WHERE role_id = ?",
                (role_id,)
            )
            await db.commit()

    @staticmethod
    async def get_match_valorant_id(match_id: int) -> Optional[str]:
        """Get the Valorant match ID for a custom match (if available)."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT valorant_match_id FROM valorant_match_stats WHERE match_id = ? LIMIT 1",
                (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    # --- Stats retry persistence ---

    @staticmethod
    async def create_stats_retry(match_id: int, game_id: int, next_attempt_at: datetime):
        """Insert a new stats retry record for a match."""
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                """INSERT OR REPLACE INTO valorant_stats_retry
                   (match_id, game_id, next_attempt_at, status)
                   VALUES (?, ?, ?, 'pending')""",
                (match_id, game_id, next_attempt_at.isoformat())
            )
            await db.commit()

    @staticmethod
    async def get_pending_stats_retries() -> List[dict]:
        """Get all pending retry records whose next_attempt_at has passed."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                """SELECT * FROM valorant_stats_retry
                   WHERE status = 'pending'
                   AND datetime(next_attempt_at) <= datetime('now')"""
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    VALID_RETRY_STATUSES = {'pending', 'success', 'failed', 'abandoned'}

    @staticmethod
    async def update_stats_retry(match_id: int, status: str = None,
                                  attempt_count: int = None,
                                  next_attempt_at: datetime = None,
                                  last_reason: str = None):
        """Update fields on a stats retry record."""
        async with DatabaseHelper._get_db() as db:
            updates = []
            params = []
            if status is not None:
                if status not in DatabaseHelper.VALID_RETRY_STATUSES:
                    raise ValueError(f"Invalid retry status: {status}")
                updates.append("status = ?")
                params.append(status)
            if attempt_count is not None:
                updates.append("attempt_count = ?")
                params.append(attempt_count)
            if next_attempt_at is not None:
                updates.append("next_attempt_at = ?")
                params.append(next_attempt_at.isoformat())
            if last_reason is not None:
                updates.append("last_reason = ?")
                params.append(last_reason[:500])
            updates.append("last_attempt_at = datetime('now')")
            params.append(match_id)

            await db.execute(
                f"UPDATE valorant_stats_retry SET {', '.join(updates)} WHERE match_id = ?",
                params
            )
            await db.commit()

    # -------------------------------------------------------------------------
    # Marvel Rivals helpers (scoreboard OCR stats)
    # -------------------------------------------------------------------------

    @staticmethod
    async def save_rivals_match_stats(match_id: int, rows: List[dict]):
        """Replace all Rivals stats rows for a match.

        Each row dict should contain: player_id, ign, role, team, kills, deaths,
        assists, final_hits, damage, damage_blocked, healing, accuracy_pct,
        mvp_svp, medals (dict[str,int]).
        """
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "DELETE FROM rivals_match_stats WHERE match_id = ?",
                (match_id,)
            )
            for r in rows:
                medals_json = json.dumps(r.get("medals") or {})
                await db.execute("""
                    INSERT INTO rivals_match_stats
                    (match_id, player_id, ign, role, team, kills, deaths, assists,
                     final_hits, damage, damage_blocked, healing, accuracy_pct,
                     mvp_svp, medals_json, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """, (
                    match_id,
                    r["player_id"],
                    r.get("ign"),
                    r.get("role"),
                    r.get("team"),
                    int(r.get("kills") or 0),
                    int(r.get("deaths") or 0),
                    int(r.get("assists") or 0),
                    int(r.get("final_hits") or 0),
                    int(r.get("damage") or 0),
                    int(r.get("damage_blocked") or 0),
                    int(r.get("healing") or 0),
                    float(r["accuracy_pct"]) if r.get("accuracy_pct") is not None else None,
                    r.get("mvp_svp"),
                    medals_json,
                ))
            await db.commit()

    @staticmethod
    async def get_rivals_match_stats(match_id: int) -> List[dict]:
        """Get all Rivals stats for a match."""
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM rivals_match_stats WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
        out = []
        for row in rows:
            d = dict(row)
            try:
                d["medals"] = json.loads(d.get("medals_json") or "{}")
            except (json.JSONDecodeError, TypeError):
                d["medals"] = {}
            out.append(d)
        return out

    @staticmethod
    async def clear_rivals_match_stats(match_id: int) -> int:
        """Delete all Rivals stats rows for a match. Returns rows deleted."""
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                "DELETE FROM rivals_match_stats WHERE match_id = ?",
                (match_id,)
            )
            await db.commit()
            return cursor.rowcount

    @staticmethod
    async def is_rivals_upload_blacklisted(guild_id: int, player_id: int) -> bool:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT 1 FROM rivals_upload_blacklist WHERE guild_id = ? AND player_id = ?",
                (guild_id, player_id)
            ) as cursor:
                return (await cursor.fetchone()) is not None

    @staticmethod
    async def add_rivals_blacklist(guild_id: int, player_id: int, added_by: int, reason: str = None):
        async with DatabaseHelper._get_db() as db:
            await db.execute("""
                INSERT OR REPLACE INTO rivals_upload_blacklist
                (guild_id, player_id, added_by, reason, added_at)
                VALUES (?, ?, ?, ?, datetime('now'))
            """, (guild_id, player_id, added_by, reason))
            await db.commit()

    @staticmethod
    async def remove_rivals_blacklist(guild_id: int, player_id: int) -> int:
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute(
                "DELETE FROM rivals_upload_blacklist WHERE guild_id = ? AND player_id = ?",
                (guild_id, player_id)
            )
            await db.commit()
            return cursor.rowcount

    @staticmethod
    async def list_rivals_blacklist(guild_id: int) -> List[dict]:
        async with DatabaseHelper._get_db() as db:
            async with db.execute(
                "SELECT * FROM rivals_upload_blacklist WHERE guild_id = ? ORDER BY added_at DESC",
                (guild_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def record_rivals_upload(match_id: int, uploader_id: int, image_path: str,
                                    gemini_raw_json: str, confidence: float, status: str) -> int:
        """Insert a rivals_scoreboard_uploads audit row. Returns the upload_id."""
        async with DatabaseHelper._get_db() as db:
            cursor = await db.execute("""
                INSERT INTO rivals_scoreboard_uploads
                (match_id, uploader_id, image_path, gemini_raw_json, confidence, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """, (match_id, uploader_id, image_path, gemini_raw_json, confidence, status))
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def mark_rivals_upload_status(upload_id: int, status: str):
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE rivals_scoreboard_uploads SET status = ? WHERE upload_id = ?",
                (status, upload_id)
            )
            await db.commit()

    @staticmethod
    async def supersede_prior_rivals_uploads(match_id: int, keep_upload_id: int = None):
        """Mark all prior committed/pending uploads for a match as 'superseded'."""
        async with DatabaseHelper._get_db() as db:
            if keep_upload_id is None:
                await db.execute("""
                    UPDATE rivals_scoreboard_uploads SET status = 'superseded'
                    WHERE match_id = ? AND status IN ('committed', 'pending_review')
                """, (match_id,))
            else:
                await db.execute("""
                    UPDATE rivals_scoreboard_uploads SET status = 'superseded'
                    WHERE match_id = ? AND upload_id != ?
                    AND status IN ('committed', 'pending_review')
                """, (match_id, keep_upload_id))
            await db.commit()

    @staticmethod
    async def get_latest_rivals_upload(match_id: int, statuses: tuple = ("pending_review", "timed_out", "rejected")) -> Optional[dict]:
        """Fetch the most recent rivals upload row for a match, filtered by status."""
        placeholders = ",".join("?" for _ in statuses)
        async with DatabaseHelper._get_db() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"SELECT * FROM rivals_scoreboard_uploads "
                f"WHERE match_id = ? AND status IN ({placeholders}) "
                f"ORDER BY upload_id DESC LIMIT 1",
                (match_id, *statuses),
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
