"""
Custom Matches Cog
A comprehensive queue and match management system for custom games.
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple
from pathlib import Path
import random
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('custommatch')

# =============================================================================
# CONSTANTS & ENUMS
# =============================================================================

class QueueType(Enum):
    MMR = "mmr"
    CAPTAINS = "captains"
    RANDOM = "random"

class CaptainSelection(Enum):
    RANDOM = "random"
    ADMIN = "admin"
    HIGHEST_MMR = "highest_mmr"

class Team(Enum):
    RED = "red"
    BLUE = "blue"

# K-Factor settings
K_FACTOR_NEWBIE = 50      # Games 1-5
K_FACTOR_LEARNING = 35    # Games 6-15
K_FACTOR_STABLE = 20      # Games 16+

# Thresholds
NEWBIE_GAMES = 5
LEARNING_GAMES = 15
RIVALRY_MIN_GAMES = 5

# Colors
COLOR_RED = 0xFF0000
COLOR_BLUE = 0x0000FF
COLOR_NEUTRAL = 0x2F3136
COLOR_SUCCESS = 0x00FF00
COLOR_WARNING = 0xFFAA00

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
    ready_timer_seconds INTEGER DEFAULT 60
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
"""

async def init_db():
    """Initialize the database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()
    await migrate_db()

async def migrate_db():
    """Run database migrations to add new columns to existing tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Get existing columns for games table
        async with db.execute("PRAGMA table_info(games)") as cursor:
            game_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to games table if they don't exist
        game_migrations = [
            ("vc_creation_enabled", "INTEGER DEFAULT 0"),
            ("queue_role_required", "INTEGER DEFAULT 1"),
            ("dm_ready_up", "INTEGER DEFAULT 0"),
            ("match_history_channel_id", "INTEGER"),
            ("queue_timeout_minutes", "INTEGER DEFAULT 0"),
            ("penalty_1st_minutes", "INTEGER DEFAULT 60"),
            ("penalty_2nd_minutes", "INTEGER DEFAULT 1440"),
            ("penalty_3rd_minutes", "INTEGER DEFAULT 10080"),
            ("penalty_decay_days", "INTEGER DEFAULT 30"),
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
        ]

        for col_name, col_def in match_migrations:
            if col_name not in match_columns:
                await db.execute(f"ALTER TABLE matches ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to matches table")

        await db.commit()

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class GameConfig:
    game_id: int
    name: str
    player_count: int
    queue_type: QueueType
    captain_selection: CaptainSelection
    queue_channel_id: Optional[int]
    verified_role_id: Optional[int]
    ready_timer_seconds: int = 60
    vc_creation_enabled: bool = False
    queue_role_required: bool = True
    dm_ready_up: bool = False
    match_history_channel_id: Optional[int] = None
    queue_timeout_minutes: int = 0
    penalty_1st_minutes: int = 60
    penalty_2nd_minutes: int = 1440
    penalty_3rd_minutes: int = 10080
    penalty_decay_days: int = 30

@dataclass
class PlayerIGN:
    player_id: int
    game_id: int
    ign: str

@dataclass
class ReadyPenalty:
    player_id: int
    offense_count: int = 0
    penalty_expires: Optional[datetime] = None
    last_offense: Optional[datetime] = None

@dataclass
class Suspension:
    suspension_id: int
    player_id: int
    game_id: Optional[int]
    suspended_until: datetime
    reason: Optional[str]
    suspended_by: Optional[int]
    created_at: datetime

@dataclass
class PlayerStats:
    player_id: int
    game_id: int
    mmr: int = 1000
    games_played: int = 0
    wins: int = 0
    losses: int = 0
    admin_offset: int = 0
    last_played: Optional[datetime] = None
    
    @property
    def effective_mmr(self) -> int:
        return self.mmr + self.admin_offset
    
    def get_k_factor(self) -> int:
        """Get K-factor based on games played and inactivity."""
        base_k = K_FACTOR_STABLE
        if self.games_played <= NEWBIE_GAMES:
            base_k = K_FACTOR_NEWBIE
        elif self.games_played <= LEARNING_GAMES:
            base_k = K_FACTOR_LEARNING
        
        # Inactivity boost
        if self.last_played:
            days_inactive = (datetime.now(timezone.utc) - self.last_played).days
            if days_inactive >= 60:  # 2+ months
                return K_FACTOR_NEWBIE
            elif days_inactive >= 30:  # 1 month
                # Bump up one tier
                if base_k == K_FACTOR_STABLE:
                    return K_FACTOR_LEARNING
        
        return base_k

@dataclass
class QueueState:
    queue_id: int
    game_id: int
    message_id: Optional[int] = None
    channel_id: Optional[int] = None
    state: str = "waiting"  # waiting, ready_check, drafting, in_match
    players: Dict[int, bool] = field(default_factory=dict)  # player_id -> is_ready
    ready_check_started: Optional[datetime] = None

@dataclass
class MatchState:
    match_id: int
    game_id: int
    channel_id: Optional[int] = None
    draft_channel_id: Optional[int] = None
    red_role_id: Optional[int] = None
    blue_role_id: Optional[int] = None
    red_team: List[int] = field(default_factory=list)
    blue_team: List[int] = field(default_factory=list)
    captains: Dict[str, int] = field(default_factory=dict)  # team -> player_id
    queue_type: QueueType = QueueType.MMR

# =============================================================================
# DATABASE HELPERS
# =============================================================================

class DatabaseHelper:
    """Helper class for database operations."""
    
    @staticmethod
    async def get_config(key: str) -> Optional[str]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    
    @staticmethod
    async def set_config(key: str, value: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, value)
            )
            await db.commit()
    
    @staticmethod
    async def get_game(game_id: int) -> Optional[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM games WHERE game_id = ?", (game_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)
    
    @staticmethod
    async def get_game_by_name(name: str) -> Optional[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM games WHERE name = ?", (name,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)
    
    @staticmethod
    async def get_game_by_channel(channel_id: int) -> Optional[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
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
            match_history_channel_id=row["match_history_channel_id"] if "match_history_channel_id" in row.keys() else None,
            queue_timeout_minutes=row["queue_timeout_minutes"] if "queue_timeout_minutes" in row.keys() else 0,
            penalty_1st_minutes=row["penalty_1st_minutes"] if "penalty_1st_minutes" in row.keys() else 60,
            penalty_2nd_minutes=row["penalty_2nd_minutes"] if "penalty_2nd_minutes" in row.keys() else 1440,
            penalty_3rd_minutes=row["penalty_3rd_minutes"] if "penalty_3rd_minutes" in row.keys() else 10080,
            penalty_decay_days=row["penalty_decay_days"] if "penalty_decay_days" in row.keys() else 30,
        )

    @staticmethod
    async def get_all_games() -> List[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM games") as cursor:
                rows = await cursor.fetchall()
                return [DatabaseHelper._row_to_game_config(row) for row in rows]
    
    @staticmethod
    async def add_game(name: str, player_count: int, queue_type: str = "mmr",
                       captain_selection: str = "random") -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """INSERT INTO games (name, player_count, queue_type, captain_selection)
                   VALUES (?, ?, ?, ?)""",
                (name, player_count, queue_type, captain_selection)
            )
            await db.commit()
            return cursor.lastrowid
    
    @staticmethod
    async def update_game(game_id: int, **kwargs):
        async with aiosqlite.connect(DB_PATH) as db:
            for key, value in kwargs.items():
                await db.execute(
                    f"UPDATE games SET {key} = ? WHERE game_id = ?",
                    (value, game_id)
                )
            await db.commit()
    
    @staticmethod
    async def delete_game(game_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM games WHERE game_id = ?", (game_id,))
            await db.commit()
    
    @staticmethod
    async def get_player_stats(player_id: int, game_id: int) -> PlayerStats:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM player_game_stats WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return PlayerStats(player_id=player_id, game_id=game_id)
                return PlayerStats(
                    player_id=row[0],
                    game_id=row[1],
                    mmr=row[2],
                    games_played=row[3],
                    wins=row[4],
                    losses=row[5],
                    admin_offset=row[6],
                    last_played=datetime.fromisoformat(row[7]) if row[7] else None
                )
    
    @staticmethod
    async def update_player_stats(stats: PlayerStats):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO player_game_stats 
                   (player_id, game_id, mmr, games_played, wins, losses, admin_offset, last_played)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (stats.player_id, stats.game_id, stats.mmr, stats.games_played,
                 stats.wins, stats.losses, stats.admin_offset,
                 stats.last_played.isoformat() if stats.last_played else None)
            )
            await db.commit()
    
    @staticmethod
    async def get_mmr_roles(game_id: int) -> Dict[int, int]:
        """Returns {role_id: mmr_value}"""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT role_id, mmr_value FROM game_mmr_roles WHERE game_id = ?",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}
    
    @staticmethod
    async def set_mmr_role(game_id: int, role_id: int, mmr_value: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO game_mmr_roles (game_id, role_id, mmr_value)
                   VALUES (?, ?, ?)""",
                (game_id, role_id, mmr_value)
            )
            await db.commit()
    
    @staticmethod
    async def remove_mmr_role(game_id: int, role_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM game_mmr_roles WHERE game_id = ? AND role_id = ?",
                (game_id, role_id)
            )
            await db.commit()
    
    @staticmethod
    async def is_blacklisted(player_id: int) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO players (player_id, blacklisted_until) VALUES (?, ?)
                   ON CONFLICT(player_id) DO UPDATE SET blacklisted_until = ?""",
                (player_id, until.isoformat(), until.isoformat())
            )
            await db.commit()
    
    @staticmethod
    async def unblacklist_player(player_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE players SET blacklisted_until = NULL WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()
    
    @staticmethod
    async def get_blacklisted_players() -> List[Tuple[int, datetime]]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT player_id, blacklisted_until FROM players WHERE blacklisted_until IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
                return [(row[0], datetime.fromisoformat(row[1])) for row in rows if row[1]]
    
    @staticmethod
    async def create_match(game_id: int, queue_type: str, queue_message_id: Optional[int] = None) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """INSERT INTO matches (game_id, queue_type, queue_message_id, created_at)
                   VALUES (?, ?, ?, ?)""",
                (game_id, queue_type, queue_message_id, datetime.now(timezone.utc).isoformat())
            )
            await db.commit()
            return cursor.lastrowid
    
    @staticmethod
    async def update_match(match_id: int, **kwargs):
        async with aiosqlite.connect(DB_PATH) as db:
            for key, value in kwargs.items():
                await db.execute(
                    f"UPDATE matches SET {key} = ? WHERE match_id = ?",
                    (value, match_id)
                )
            await db.commit()
    
    @staticmethod
    async def get_match(match_id: int) -> Optional[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    
    @staticmethod
    async def get_match_by_channel(channel_id: int) -> Optional[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE channel_id = ? AND winning_team IS NULL AND cancelled = 0",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    
    @staticmethod
    async def get_active_matches() -> List[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE winning_team IS NULL AND cancelled = 0"
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    @staticmethod
    async def add_match_player(match_id: int, player_id: int, team: str,
                               was_captain: bool = False, was_sub: bool = False,
                               original_player_id: Optional[int] = None):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO match_players 
                   (match_id, player_id, team, was_captain, was_sub, original_player_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (match_id, player_id, team, int(was_captain), int(was_sub), original_player_id)
            )
            await db.commit()
    
    @staticmethod
    async def get_match_players(match_id: int) -> List[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM match_players WHERE match_id = ?", (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    @staticmethod
    async def remove_match_player(match_id: int, player_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM match_players WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            )
            await db.commit()
    
    @staticmethod
    async def get_rivalry(player_a: int, player_b: int, game_id: int) -> Optional[Tuple[int, int]]:
        """Returns (player_a_wins, player_b_wins) or None."""
        # Ensure consistent ordering
        if player_a > player_b:
            player_a, player_b = player_b, player_a
            swapped = True
        else:
            swapped = False
        
        async with aiosqlite.connect(DB_PATH) as db:
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
        
        async with aiosqlite.connect(DB_PATH) as db:
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
    async def get_player_rivalries(player_id: int, game_id: int, limit: int = 3) -> List[dict]:
        """Get top rivalries for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
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
    
    @staticmethod
    async def add_win_vote(match_id: int, player_id: int, team: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO win_votes (match_id, player_id, voted_team)
                   VALUES (?, ?, ?)""",
                (match_id, player_id, team)
            )
            await db.commit()
    
    @staticmethod
    async def get_win_votes(match_id: int) -> Dict[str, int]:
        """Returns {team: vote_count}"""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT voted_team, COUNT(*) FROM win_votes WHERE match_id = ? GROUP BY voted_team",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}
    
    @staticmethod
    async def record_mmr_change(player_id: int, game_id: int, match_id: int,
                                mmr_before: int, mmr_after: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO mmr_history (player_id, game_id, match_id, mmr_before, mmr_after, change)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (player_id, game_id, match_id, mmr_before, mmr_after, mmr_after - mmr_before)
            )
            await db.commit()
    
    @staticmethod
    async def get_leaderboard(game_id: int, monthly: bool = True, limit: int = 20) -> List[dict]:
        """Get leaderboard for a game."""
        async with aiosqlite.connect(DB_PATH) as db:
            if monthly:
                # Get current month start
                now = datetime.now(timezone.utc)
                month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                
                # Get wins/losses from matches this month
                query = """
                    SELECT mp.player_id, 
                           SUM(CASE WHEN m.winning_team = mp.team THEN 1 ELSE 0 END) as wins,
                           SUM(CASE WHEN m.winning_team != mp.team THEN 1 ELSE 0 END) as losses
                    FROM match_players mp
                    JOIN matches m ON mp.match_id = m.match_id
                    WHERE m.game_id = ? AND m.winning_team IS NOT NULL 
                          AND m.decided_at >= ? AND m.cancelled = 0
                    GROUP BY mp.player_id
                    ORDER BY wins DESC, losses ASC
                    LIMIT ?
                """
                async with db.execute(query, (game_id, month_start.isoformat(), limit)) as cursor:
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
    async def get_player_recent_matches(player_id: int, limit: int = 5) -> List[dict]:
        """Get recent match history for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT ign FROM player_igns WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def set_player_ign(player_id: int, game_id: int, ign: str):
        """Set a player's IGN for a specific game."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO player_igns (player_id, game_id, ign)
                   VALUES (?, ?, ?)""",
                (player_id, game_id, ign)
            )
            await db.commit()

    @staticmethod
    async def get_match_igns(match_id: int) -> Dict[int, str]:
        """Get all IGNs for players in a match. Returns {player_id: ign}."""
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """SELECT pi.game_id, g.name, pi.ign FROM player_igns pi
                   JOIN games g ON pi.game_id = g.game_id
                   WHERE pi.player_id = ?""",
                (player_id,)
            ) as cursor:
                return await cursor.fetchall()

    # -------------------------------------------------------------------------
    # READY PENALTY METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_ready_penalty(player_id: int) -> ReadyPenalty:
        """Get a player's ready penalty status."""
        async with aiosqlite.connect(DB_PATH) as db:
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

        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM ready_penalties WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()

    @staticmethod
    async def get_all_penalties() -> List[ReadyPenalty]:
        """Get all active penalties."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
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
    # SUSPENSION METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def add_suspension(player_id: int, game_id: Optional[int], until: datetime,
                             reason: Optional[str], suspended_by: Optional[int]) -> int:
        """Add a suspension. Returns suspension_id."""
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM suspensions WHERE suspension_id = ?",
                (suspension_id,)
            )
            await db.commit()

    @staticmethod
    async def is_suspended(player_id: int, game_id: int) -> Optional[Suspension]:
        """Check if a player is suspended for a game. Returns Suspension or None."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO abandon_votes (match_id, player_id)
                   VALUES (?, ?)""",
                (match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_abandon_votes(match_id: int) -> int:
        """Get number of abandon votes for a match."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM abandon_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    @staticmethod
    async def has_voted_abandon(match_id: int, player_id: int) -> bool:
        """Check if a player has already voted to abandon."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM abandon_votes WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            ) as cursor:
                return await cursor.fetchone() is not None

    # -------------------------------------------------------------------------
    # STATS METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def delete_player_stats(player_id: int):
        """Delete all stats for a player (when they leave the server)."""
        async with aiosqlite.connect(DB_PATH) as db:
            # Delete from player_game_stats
            await db.execute("DELETE FROM player_game_stats WHERE player_id = ?", (player_id,))
            # Delete from mmr_history
            await db.execute("DELETE FROM mmr_history WHERE player_id = ?", (player_id,))
            # Delete from rivalries (both sides)
            await db.execute(
                "DELETE FROM rivalries WHERE player_a_id = ? OR player_b_id = ?",
                (player_id, player_id)
            )
            # Delete IGNs
            await db.execute("DELETE FROM player_igns WHERE player_id = ?", (player_id,))
            # Delete from players table
            await db.execute("DELETE FROM players WHERE player_id = ?", (player_id,))
            await db.commit()

    @staticmethod
    async def adjust_player_stats(player_id: int, game_id: int, wins_delta: int, losses_delta: int):
        """Adjust a player's wins/losses by specified amounts."""
        stats = await DatabaseHelper.get_player_stats(player_id, game_id)
        stats.wins = max(0, stats.wins + wins_delta)
        stats.losses = max(0, stats.losses + losses_delta)
        stats.games_played = stats.wins + stats.losses
        await DatabaseHelper.update_player_stats(stats)

    @staticmethod
    async def get_completed_match(match_id: int) -> Optional[dict]:
        """Get a match even if it's already decided."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def reverse_match_result(match_id: int) -> bool:
        """Reverse all MMR changes from a completed match. Returns True if successful."""
        async with aiosqlite.connect(DB_PATH) as db:
            # Get MMR history for this match
            async with db.execute(
                "SELECT player_id, game_id, mmr_before, mmr_after FROM mmr_history WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                return False

            # Reverse the MMR changes
            for player_id, game_id, mmr_before, mmr_after in rows:
                change = mmr_after - mmr_before
                # Get current stats
                stats = await DatabaseHelper.get_player_stats(player_id, game_id)
                stats.mmr -= change  # Reverse the change

                # Also reverse W/L
                if change > 0:  # Was a winner
                    stats.wins = max(0, stats.wins - 1)
                else:  # Was a loser
                    stats.losses = max(0, stats.losses - 1)
                stats.games_played = stats.wins + stats.losses

                await DatabaseHelper.update_player_stats(stats)

            # Delete the MMR history entries
            await db.execute("DELETE FROM mmr_history WHERE match_id = ?", (match_id,))

            # Clear the winning_team so the match can be re-decided
            await db.execute(
                "UPDATE matches SET winning_team = NULL, decided_at = NULL WHERE match_id = ?",
                (match_id,)
            )
            await db.commit()

        return True

    # -------------------------------------------------------------------------
    # QUEUE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def clear_queue(queue_id: int):
        """Clear all players from a queue."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (queue_id,))
            await db.commit()

    @staticmethod
    async def remove_player_from_queue(queue_id: int, player_id: int):
        """Remove a specific player from a queue."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                (queue_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_queue_join_times(queue_id: int) -> Dict[int, datetime]:
        """Get join times for all players in a queue. Returns {player_id: joined_at}."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT player_id, joined_at FROM queue_players WHERE queue_id = ?",
                (queue_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {
                    row[0]: datetime.fromisoformat(row[1]) if row[1] else datetime.now(timezone.utc)
                    for row in rows
                }

# =============================================================================
# VIEWS & MODALS
# =============================================================================

class GameSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a game."""
    
    def __init__(self, games: List[GameConfig], callback_func):
        options = [
            discord.SelectOption(label=g.name, value=str(g.game_id))
            for g in games
        ]
        if not options:
            options = [discord.SelectOption(label="No games configured", value="none")]
        super().__init__(placeholder="Select a game...", options=options)
        self.callback_func = callback_func
    
    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            await interaction.response.send_message("No games configured yet.", ephemeral=True)
            return
        await self.callback_func(interaction, int(self.values[0]))


class ConfirmView(discord.ui.View):
    """Simple confirmation view."""
    
    def __init__(self, timeout: float = 60.0):
        super().__init__(timeout=timeout)
        self.value = None
    
    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        await interaction.response.defer()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        await interaction.response.defer()


# =============================================================================
# SETTINGS PANEL
# =============================================================================

class SettingsView(discord.ui.View):
    """Main settings panel for server admins."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(label="Select Category", style=discord.ButtonStyle.secondary, row=0)
    async def select_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = CategorySelectView(self.cog)
        await interaction.response.send_message("Select a category for match channels:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Log Channel", style=discord.ButtonStyle.secondary, row=0)
    async def set_log_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = LogChannelSelectView(self.cog)
        await interaction.response.send_message("Select a log channel:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.secondary, row=0)
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AdminRoleSelectView(self.cog)
        await interaction.response.send_message("Select the CM Admin role:", view=view, ephemeral=True)
    
    @discord.ui.button(label="Add Game", style=discord.ButtonStyle.success, row=1)
    async def add_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AddGameModal(self.cog)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Edit Game", style=discord.ButtonStyle.primary, row=1)
    async def edit_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_edit_game_modal))
        await interaction.response.send_message("Select a game to edit:", view=view, ephemeral=True)
    
    async def show_edit_game_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = EditGameModal(self.cog, game)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Remove Game", style=discord.ButtonStyle.danger, row=1)
    async def remove_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.confirm_remove_game))
        await interaction.response.send_message("Select a game to remove:", view=view, ephemeral=True)
    
    async def confirm_remove_game(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to remove **{game.name}**? This cannot be undone.",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            await DatabaseHelper.delete_game(game_id)
            await interaction.followup.send(f"Removed **{game.name}**.", ephemeral=True)
    
    @discord.ui.button(label="Configure MMR Roles", style=discord.ButtonStyle.primary, row=2)
    async def config_mmr_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_mmr_roles_panel))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)
    
    async def show_mmr_roles_panel(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        mmr_roles = await DatabaseHelper.get_mmr_roles(game_id)
        
        lines = [f"**MMR Roles for {game.name}**\n"]
        if mmr_roles:
            for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
                role = interaction.guild.get_role(role_id)
                role_name = role.name if role else f"Unknown ({role_id})"
                lines.append(f"• {role_name}: {mmr} MMR")
        else:
            lines.append("No MMR roles configured.")
        
        view = MMRRolesView(self.cog, game_id)
        await interaction.response.send_message("\n".join(lines), view=view, ephemeral=True)
    
    @discord.ui.button(label="Set Player MMR", style=discord.ButtonStyle.secondary, row=2)
    async def set_player_mmr(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_set_mmr_modal))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)
    
    async def show_set_mmr_modal(self, interaction: discord.Interaction, game_id: int):
        modal = SetPlayerMMRModal(self.cog, game_id)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Set Admin Offset", style=discord.ButtonStyle.secondary, row=2)
    async def set_admin_offset(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_offset_modal))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)
    
    async def show_offset_modal(self, interaction: discord.Interaction, game_id: int):
        modal = SetAdminOffsetModal(self.cog, game_id)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Blacklist Player", style=discord.ButtonStyle.danger, row=3)
    async def blacklist_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BlacklistUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to blacklist:", view=view, ephemeral=True)

    @discord.ui.button(label="Unblacklist Player", style=discord.ButtonStyle.secondary, row=3)
    async def unblacklist_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        if not blacklisted:
            await interaction.response.send_message("No blacklisted players.", ephemeral=True)
            return
        view = UnblacklistSelectView(self.cog, blacklisted, interaction.guild)
        await interaction.response.send_message("Select a player to unblacklist:", view=view, ephemeral=True)

    @discord.ui.button(label="View Blacklist", style=discord.ButtonStyle.secondary, row=3)
    async def view_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        if not blacklisted:
            await interaction.response.send_message("No blacklisted players.", ephemeral=True)
            return

        lines = ["**Blacklisted Players**\n"]
        now = datetime.now(timezone.utc)
        for player_id, until in blacklisted:
            if until > now:
                user = interaction.guild.get_member(player_id)
                name = user.display_name if user else str(player_id)
                if until.year == 2099:
                    lines.append(f"• {name}: Permanent")
                else:
                    lines.append(f"• {name}: Until {until.strftime('%Y-%m-%d %H:%M')} UTC")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    # Row 4: Game settings
    @discord.ui.button(label="Game Toggles", style=discord.ButtonStyle.primary, row=4)
    async def game_toggles(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_game_toggles))
        await interaction.response.send_message("Select a game to configure toggles:", view=view, ephemeral=True)

    async def show_game_toggles(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = GameTogglesView(self.cog, game)
        embed = discord.Embed(title=f"{game.name} Toggles", color=COLOR_NEUTRAL)
        embed.add_field(name="VC Creation", value="Enabled" if game.vc_creation_enabled else "Disabled", inline=True)
        embed.add_field(name="Queue Role Required", value="Yes" if game.queue_role_required else "No", inline=True)
        embed.add_field(name="DM Ready-Up", value="Enabled" if game.dm_ready_up else "Disabled", inline=True)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Penalty Settings", style=discord.ButtonStyle.primary, row=4)
    async def penalty_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_penalty_settings))
        await interaction.response.send_message("Select a game to configure penalties:", view=view, ephemeral=True)

    async def show_penalty_settings(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = PenaltySettingsView(self.cog, game)
        embed = discord.Embed(title=f"{game.name} Penalty Settings", color=COLOR_NEUTRAL)
        embed.add_field(name="1st Offense", value=f"{game.penalty_1st_minutes} min", inline=True)
        embed.add_field(name="2nd Offense", value=f"{game.penalty_2nd_minutes} min", inline=True)
        embed.add_field(name="3rd+ Offense", value=f"{game.penalty_3rd_minutes} min", inline=True)
        embed.add_field(name="Decay Period", value=f"{game.penalty_decay_days} days", inline=True)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Match History Ch", style=discord.ButtonStyle.primary, row=4)
    async def match_history_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_match_history_select))
        await interaction.response.send_message("Select a game to set match history channel:", view=view, ephemeral=True)

    async def show_match_history_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = MatchHistoryChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.match_history_channel_id) if game.match_history_channel_id else None
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current match history channel for **{game.name}**: {current_str}\n\nSelect a new channel:",
            view=view, ephemeral=True
        )


class MMRRolesView(discord.ui.View):
    """View for managing MMR roles."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.button(label="Add/Update Role", style=discord.ButtonStyle.success)
    async def add_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddMMRRoleSelectView(self.cog, self.game_id)
        await interaction.response.send_message("Select a role:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Role", style=discord.ButtonStyle.danger)
    async def remove_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        if not mmr_roles:
            await interaction.response.send_message("No MMR roles configured.", ephemeral=True)
            return
        view = RemoveMMRRoleSelectView(self.cog, self.game_id, mmr_roles, interaction.guild)
        await interaction.response.send_message("Select a role to remove:", view=view, ephemeral=True)


class AddMMRRoleSelectView(discord.ui.View):
    """View for selecting a role to add as MMR role."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        modal = MMRValueModal(self.cog, self.game_id, role.id, role.name)
        await interaction.response.send_modal(modal)


class MMRValueModal(discord.ui.Modal, title="Set MMR Value"):
    mmr_value = discord.ui.TextInput(
        label="MMR Value",
        placeholder="e.g., 1500",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, role_id: int, role_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.role_id = role_id
        self.title = f"Set MMR for {role_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
            await DatabaseHelper.set_mmr_role(self.game_id, self.role_id, mmr)
            await interaction.response.send_message(f"Set role MMR to {mmr}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class RemoveMMRRoleSelectView(discord.ui.View):
    """View for selecting an MMR role to remove."""

    def __init__(self, cog: 'CustomMatch', game_id: int, mmr_roles: Dict[int, int], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=f"{name} ({mmr} MMR)", value=str(role_id)))

        select = discord.ui.Select(placeholder="Select role to remove...", options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_mmr_role(self.game_id, role_id)
        await interaction.response.send_message("Removed MMR role.", ephemeral=True)


class CategorySelectView(discord.ui.View):
    """View for selecting a category channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a category...",
                       channel_types=[discord.ChannelType.category])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        category = select.values[0]
        await DatabaseHelper.set_config("category_id", str(category.id))
        await interaction.response.send_message(f"Category set to **{category.name}**.", ephemeral=True)


class LogChannelSelectView(discord.ui.View):
    """View for selecting a log channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.set_config("log_channel_id", str(channel.id))
        await interaction.response.send_message(f"Log channel set to {channel.mention}.", ephemeral=True)


class AdminRoleSelectView(discord.ui.View):
    """View for selecting the admin role."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await DatabaseHelper.set_config("cm_admin_role_id", str(role.id))
        await interaction.response.send_message(f"CM Admin role set to **{role.name}**.", ephemeral=True)


class BlacklistUserSelectView(discord.ui.View):
    """View for selecting a user to blacklist."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        modal = BlacklistDurationModal(self.cog, user.id, user.display_name)
        await interaction.response.send_modal(modal)


class BlacklistDurationModal(discord.ui.Modal, title="Blacklist Duration"):
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, user_name: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.title = f"Blacklist {user_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.duration.value)
            if days <= 0:
                await DatabaseHelper.blacklist_player(self.user_id)
                await interaction.response.send_message(f"Permanently blacklisted user.", ephemeral=True)
            else:
                until = datetime.now(timezone.utc) + timedelta(days=days)
                await DatabaseHelper.blacklist_player(self.user_id, until)
                await interaction.response.send_message(f"Blacklisted user for {days} days.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)


class UnblacklistSelectView(discord.ui.View):
    """View for selecting a user to unblacklist."""

    def __init__(self, cog: 'CustomMatch', blacklisted: List[Tuple[int, datetime]], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        now = datetime.now(timezone.utc)
        for player_id, until in blacklisted:
            if until > now:
                user = guild.get_member(player_id)
                name = user.display_name if user else str(player_id)
                duration = "Permanent" if until.year == 2099 else f"Until {until.strftime('%Y-%m-%d')}"
                options.append(discord.SelectOption(label=name, value=str(player_id), description=duration))

        if options:
            select = discord.ui.Select(placeholder="Select player...", options=options[:25])
            select.callback = self.on_select
            self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        player_id = int(interaction.data["values"][0])
        await DatabaseHelper.unblacklist_player(player_id)
        await interaction.response.send_message("Player unblacklisted.", ephemeral=True)


class GameTogglesView(discord.ui.View):
    """View for toggling game settings."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()

        vc_btn = discord.ui.Button(
            label=f"VC Creation: {'ON' if self.game.vc_creation_enabled else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.vc_creation_enabled else discord.ButtonStyle.secondary
        )
        vc_btn.callback = self.toggle_vc
        self.add_item(vc_btn)

        role_btn = discord.ui.Button(
            label=f"Role Required: {'ON' if self.game.queue_role_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.queue_role_required else discord.ButtonStyle.secondary
        )
        role_btn.callback = self.toggle_role
        self.add_item(role_btn)

        dm_btn = discord.ui.Button(
            label=f"DM Ready: {'ON' if self.game.dm_ready_up else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.dm_ready_up else discord.ButtonStyle.secondary
        )
        dm_btn.callback = self.toggle_dm
        self.add_item(dm_btn)

    async def toggle_vc(self, interaction: discord.Interaction):
        new_val = not self.game.vc_creation_enabled
        await DatabaseHelper.update_game(self.game.game_id, vc_creation_enabled=int(new_val))
        self.game.vc_creation_enabled = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)

    async def toggle_role(self, interaction: discord.Interaction):
        new_val = not self.game.queue_role_required
        await DatabaseHelper.update_game(self.game.game_id, queue_role_required=int(new_val))
        self.game.queue_role_required = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)

    async def toggle_dm(self, interaction: discord.Interaction):
        new_val = not self.game.dm_ready_up
        await DatabaseHelper.update_game(self.game.game_id, dm_ready_up=int(new_val))
        self.game.dm_ready_up = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)


class PenaltySettingsView(discord.ui.View):
    """View for configuring penalty settings."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game

    @discord.ui.button(label="Edit Penalty Durations", style=discord.ButtonStyle.primary)
    async def edit_durations(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = PenaltyDurationsModal(self.cog, self.game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="View Active Penalties", style=discord.ButtonStyle.secondary)
    async def view_penalties(self, interaction: discord.Interaction, button: discord.ui.Button):
        penalties = await DatabaseHelper.get_all_penalties()
        if not penalties:
            await interaction.response.send_message("No active penalties.", ephemeral=True)
            return

        lines = ["**Active Ready Penalties**\n"]
        for p in penalties:
            user = interaction.guild.get_member(p.player_id)
            name = user.display_name if user else str(p.player_id)
            expires = p.penalty_expires.strftime("%Y-%m-%d %H:%M UTC") if p.penalty_expires else "Unknown"
            lines.append(f"• {name}: Offense #{p.offense_count}, expires {expires}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Clear Player Penalty", style=discord.ButtonStyle.danger)
    async def clear_penalty(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ClearPenaltyUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to clear penalty:", view=view, ephemeral=True)


class PenaltyDurationsModal(discord.ui.Modal, title="Penalty Durations"):
    first_offense = discord.ui.TextInput(label="1st Offense (minutes)", required=True)
    second_offense = discord.ui.TextInput(label="2nd Offense (minutes)", required=True)
    third_offense = discord.ui.TextInput(label="3rd+ Offense (minutes)", required=True)
    decay_days = discord.ui.TextInput(label="Decay Period (days)", required=True)

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        self.first_offense.default = str(game.penalty_1st_minutes)
        self.second_offense.default = str(game.penalty_2nd_minutes)
        self.third_offense.default = str(game.penalty_3rd_minutes)
        self.decay_days.default = str(game.penalty_decay_days)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await DatabaseHelper.update_game(
                self.game.game_id,
                penalty_1st_minutes=int(self.first_offense.value),
                penalty_2nd_minutes=int(self.second_offense.value),
                penalty_3rd_minutes=int(self.third_offense.value),
                penalty_decay_days=int(self.decay_days.value)
            )
            await interaction.response.send_message("Penalty settings updated.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid values.", ephemeral=True)


class ClearPenaltyUserSelectView(discord.ui.View):
    """View for selecting a user to clear penalty."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        await DatabaseHelper.clear_ready_penalty(user.id)
        await interaction.response.send_message(f"Cleared penalty for **{user.display_name}**.", ephemeral=True)


class MatchHistoryChannelSelectView(discord.ui.View):
    """View for selecting match history channel."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.update_game(self.game_id, match_history_channel_id=channel.id)
        await interaction.response.send_message(f"Match history channel set to {channel.mention}.", ephemeral=True)

    @discord.ui.button(label="Clear (No History)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, match_history_channel_id=None)
        await interaction.response.send_message("Match history channel cleared.", ephemeral=True)


# =============================================================================
# SETTINGS MODALS (Legacy - kept for backwards compatibility)
# =============================================================================

class CategoryModal(discord.ui.Modal, title="Set Category"):
    category_id = discord.ui.TextInput(
        label="Category ID",
        placeholder="Right-click category > Copy ID",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            cat_id = int(self.category_id.value)
            category = interaction.guild.get_channel(cat_id)
            if not category or not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message("Invalid category ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("category_id", str(cat_id))
            await interaction.response.send_message(f"Category set to **{category.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class LogChannelModal(discord.ui.Modal, title="Set Log Channel"):
    channel_id = discord.ui.TextInput(
        label="Channel ID",
        placeholder="Right-click channel > Copy ID",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            ch_id = int(self.channel_id.value)
            channel = interaction.guild.get_channel(ch_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                await interaction.response.send_message("Invalid channel ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("log_channel_id", str(ch_id))
            await interaction.response.send_message(f"Log channel set to {channel.mention}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class AdminRoleModal(discord.ui.Modal, title="Set Custom Match Admin Role"):
    role_id = discord.ui.TextInput(
        label="Role ID",
        placeholder="Right-click role > Copy ID",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            role = interaction.guild.get_role(r_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("cm_admin_role_id", str(r_id))
            await interaction.response.send_message(f"CM Admin role set to **{role.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class AddGameModal(discord.ui.Modal, title="Add Game"):
    name = discord.ui.TextInput(label="Game Name", placeholder="e.g., Valorant", required=True)
    player_count = discord.ui.TextInput(label="Players per Queue", placeholder="e.g., 10", required=True)
    queue_type = discord.ui.TextInput(
        label="Queue Type (mmr/captains/random)",
        placeholder="mmr",
        default="mmr",
        required=True
    )
    captain_selection = discord.ui.TextInput(
        label="Captain Selection (random/admin/highest_mmr)",
        placeholder="random",
        default="random",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
            if count < 2 or count > 20:
                await interaction.response.send_message("Player count must be 2-20.", ephemeral=True)
                return
            
            qt = self.queue_type.value.lower()
            if qt not in ["mmr", "captains", "random"]:
                await interaction.response.send_message("Invalid queue type.", ephemeral=True)
                return
            
            cs = self.captain_selection.value.lower()
            if cs not in ["random", "admin", "highest_mmr"]:
                await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
                return
            
            game_id = await DatabaseHelper.add_game(self.name.value, count, qt, cs)
            await interaction.response.send_message(
                f"Added **{self.name.value}** ({count} players, {qt} queue).",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid player count.", ephemeral=True)
        except Exception as e:
            if "UNIQUE" in str(e):
                await interaction.response.send_message("A game with that name already exists.", ephemeral=True)
            else:
                raise


class EditGameModal(discord.ui.Modal, title="Edit Game"):
    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        
        self.player_count = discord.ui.TextInput(
            label="Players per Queue",
            default=str(game.player_count),
            required=True
        )
        self.queue_type = discord.ui.TextInput(
            label="Queue Type (mmr/captains/random)",
            default=game.queue_type.value,
            required=True
        )
        self.captain_selection = discord.ui.TextInput(
            label="Captain Selection (random/admin/highest_mmr)",
            default=game.captain_selection.value,
            required=True
        )
        self.queue_channel = discord.ui.TextInput(
            label="Queue Channel ID (blank to clear)",
            default=str(game.queue_channel_id) if game.queue_channel_id else "",
            required=False
        )
        self.verified_role = discord.ui.TextInput(
            label="Verified Role ID (blank to clear)",
            default=str(game.verified_role_id) if game.verified_role_id else "",
            required=False
        )
        
        self.add_item(self.player_count)
        self.add_item(self.queue_type)
        self.add_item(self.captain_selection)
        self.add_item(self.queue_channel)
        self.add_item(self.verified_role)
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
            qt = self.queue_type.value.lower()
            cs = self.captain_selection.value.lower()
            
            if qt not in ["mmr", "captains", "random"]:
                await interaction.response.send_message("Invalid queue type.", ephemeral=True)
                return
            if cs not in ["random", "admin", "highest_mmr"]:
                await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
                return
            
            updates = {
                "player_count": count,
                "queue_type": qt,
                "captain_selection": cs
            }
            
            if self.queue_channel.value:
                updates["queue_channel_id"] = int(self.queue_channel.value)
            else:
                updates["queue_channel_id"] = None
            
            if self.verified_role.value:
                updates["verified_role_id"] = int(self.verified_role.value)
            else:
                updates["verified_role_id"] = None
            
            await DatabaseHelper.update_game(self.game.game_id, **updates)
            await interaction.response.send_message(f"Updated **{self.game.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid number format.", ephemeral=True)


class AddMMRRoleModal(discord.ui.Modal, title="Add/Update MMR Role"):
    role_id = discord.ui.TextInput(label="Role ID", required=True)
    mmr_value = discord.ui.TextInput(label="MMR Value", placeholder="e.g., 1500", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            mmr = int(self.mmr_value.value)
            role = interaction.guild.get_role(r_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            await DatabaseHelper.set_mmr_role(self.game_id, r_id, mmr)
            await interaction.response.send_message(f"Set **{role.name}** to {mmr} MMR.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class RemoveMMRRoleModal(discord.ui.Modal, title="Remove MMR Role"):
    role_id = discord.ui.TextInput(label="Role ID", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            await DatabaseHelper.remove_mmr_role(self.game_id, r_id)
            await interaction.response.send_message("Removed MMR role.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class SetPlayerMMRModal(discord.ui.Modal, title="Set Player MMR"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse user ID from mention or raw ID
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            member = interaction.guild.get_member(user_id)
            if not member:
                await interaction.response.send_message("User not found in server.", ephemeral=True)
                return
            
            # Check for MMR roles
            mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
            detected_mmr = None
            detected_role = None
            
            for role in member.roles:
                if role.id in mmr_roles:
                    detected_mmr = mmr_roles[role.id]
                    detected_role = role
                    break
            
            if detected_mmr:
                # Set MMR from role
                stats = await DatabaseHelper.get_player_stats(user_id, self.game_id)
                stats.mmr = detected_mmr
                await DatabaseHelper.update_player_stats(stats)
                await interaction.response.send_message(
                    f"Set **{member.display_name}**'s MMR to {detected_mmr} (from {detected_role.name}).",
                    ephemeral=True
                )
            else:
                # Show role selection
                if not mmr_roles:
                    await interaction.response.send_message(
                        "No MMR roles configured for this game.",
                        ephemeral=True
                    )
                    return
                
                view = MMRRoleSelectView(self.cog, self.game_id, user_id, interaction.guild)
                await interaction.response.send_message(
                    f"No MMR role detected on {member.display_name}. Select one:",
                    view=view,
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


class MMRRoleSelectView(discord.ui.View):
    """View for selecting an MMR role when none is detected."""
    
    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int, guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id
        self.guild = guild
        
        # We'll add the select in setup
        asyncio.create_task(self.setup_select())
    
    async def setup_select(self):
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = self.guild.get_role(role_id)
            if role:
                options.append(discord.SelectOption(
                    label=f"{role.name} ({mmr} MMR)",
                    value=str(role_id)
                ))
        
        if options:
            select = discord.ui.Select(placeholder="Select MMR role...", options=options)
            select.callback = self.on_select
            self.add_item(select)
    
    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        mmr = mmr_roles.get(role_id, 1000)
        
        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        stats.mmr = mmr
        await DatabaseHelper.update_player_stats(stats)
        
        member = self.guild.get_member(self.user_id)
        role = self.guild.get_role(role_id)
        await interaction.response.send_message(
            f"Set **{member.display_name if member else self.user_id}**'s MMR to {mmr} (from {role.name if role else role_id}).",
            ephemeral=True
        )


class SetAdminOffsetModal(discord.ui.Modal, title="Set Admin Offset"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    offset = discord.ui.TextInput(label="Offset Value", placeholder="e.g., 100 or -50", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            offset = int(self.offset.value)
            
            member = interaction.guild.get_member(user_id)
            if not member:
                await interaction.response.send_message("User not found.", ephemeral=True)
                return
            
            stats = await DatabaseHelper.get_player_stats(user_id, self.game_id)
            stats.admin_offset = offset
            await DatabaseHelper.update_player_stats(stats)
            
            await interaction.response.send_message(
                f"Set **{member.display_name}**'s admin offset to {offset:+d}.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class BlacklistModal(discord.ui.Modal, title="Blacklist Player"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            days = int(self.duration.value)
            
            member = interaction.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)
            
            if days <= 0:
                await DatabaseHelper.blacklist_player(user_id)
                await interaction.response.send_message(f"Permanently blacklisted **{name}**.", ephemeral=True)
            else:
                until = datetime.now(timezone.utc) + timedelta(days=days)
                await DatabaseHelper.blacklist_player(user_id, until)
                await interaction.response.send_message(
                    f"Blacklisted **{name}** for {days} days.",
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class UnblacklistModal(discord.ui.Modal, title="Unblacklist Player"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            
            await DatabaseHelper.unblacklist_player(user_id)
            member = interaction.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)
            await interaction.response.send_message(f"Unblacklisted **{name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


# =============================================================================
# ADMIN PANEL
# =============================================================================

class AdminPanelView(discord.ui.View):
    """Admin panel for custom match admins."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=300)
        self.cog = cog

    # Row 0: Match management
    @discord.ui.button(label="Substitute Player", style=discord.ButtonStyle.primary, row=0)
    async def substitute(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = MatchSelectView(self.cog, matches, self.show_sub_modal)
        await interaction.response.send_message("Select a match:", view=view, ephemeral=True)

    async def show_sub_modal(self, interaction: discord.Interaction, match_id: int):
        modal = SubstituteModal(self.cog, match_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Swap Players", style=discord.ButtonStyle.primary, row=0)
    async def swap(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = MatchSelectView(self.cog, matches, self.show_swap_modal)
        await interaction.response.send_message("Select a match:", view=view, ephemeral=True)

    async def show_swap_modal(self, interaction: discord.Interaction, match_id: int):
        modal = SwapModal(self.cog, match_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Force Winner", style=discord.ButtonStyle.danger, row=0)
    async def force_winner(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = MatchSelectView(self.cog, matches, self.show_force_winner)
        await interaction.response.send_message("Select a match:", view=view, ephemeral=True)

    async def show_force_winner(self, interaction: discord.Interaction, match_id: int):
        view = ForceWinnerView(self.cog, match_id)
        await interaction.response.send_message("Select the winning team:", view=view, ephemeral=True)

    @discord.ui.button(label="Cancel Match", style=discord.ButtonStyle.danger, row=0)
    async def cancel_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = MatchSelectView(self.cog, matches, self.confirm_cancel)
        await interaction.response.send_message("Select a match to cancel:", view=view, ephemeral=True)

    async def confirm_cancel(self, interaction: discord.Interaction, match_id: int):
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to cancel Match #{match_id}?",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            await self.cog.cancel_match(interaction.guild, match_id)
            await interaction.followup.send(f"Match #{match_id} cancelled.", ephemeral=True)

    # Row 1: Advanced match/queue controls
    @discord.ui.button(label="Change Winner", style=discord.ButtonStyle.secondary, row=1)
    async def change_winner(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ChangeWinnerModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Force Start", style=discord.ButtonStyle.secondary, row=1)
    async def force_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_force_start))
        await interaction.response.send_message("Select a game to force start:", view=view, ephemeral=True)

    async def do_force_start(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        # Find active queue for this game
        queue_state = None
        for qid, qs in self.cog.queues.items():
            if qs.game_id == game_id and qs.state in ("waiting", "ready_check"):
                queue_state = qs
                break

        if not queue_state:
            await interaction.response.send_message("No active queue for this game.", ephemeral=True)
            return

        if len(queue_state.players) < game.player_count:
            await interaction.response.send_message(
                f"Queue is not full ({len(queue_state.players)}/{game.player_count}).",
                ephemeral=True
            )
            return

        # Cancel ready check if active
        if queue_state.queue_id in self.cog.ready_check_tasks:
            self.cog.ready_check_tasks[queue_state.queue_id].cancel()
            del self.cog.ready_check_tasks[queue_state.queue_id]

        # Mark all as ready
        for pid in queue_state.players:
            queue_state.players[pid] = True

        await interaction.response.defer()
        channel = interaction.guild.get_channel(queue_state.channel_id)
        if channel:
            await self.cog.proceed_to_match(channel, game, queue_state)
        await interaction.followup.send("Match force started.", ephemeral=True)

    @discord.ui.button(label="Clear Queue", style=discord.ButtonStyle.secondary, row=1)
    async def clear_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.confirm_clear_queue))
        await interaction.response.send_message("Select a game to clear its queue:", view=view, ephemeral=True)

    async def confirm_clear_queue(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to clear the queue for **{game.name}**?",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            # Find and clear queue
            for qid, qs in list(self.cog.queues.items()):
                if qs.game_id == game_id:
                    qs.players.clear()
                    await DatabaseHelper.clear_queue(qid)
                    # Update embed
                    channel = interaction.guild.get_channel(qs.channel_id)
                    if channel and qs.message_id:
                        try:
                            msg = await channel.fetch_message(qs.message_id)
                            embed = await self.cog.create_queue_embed(game, qs)
                            view = QueueView(self.cog, game_id, qid)
                            await msg.edit(embed=embed, view=view)
                        except:
                            pass
            await interaction.followup.send(f"Queue cleared for **{game.name}**.", ephemeral=True)

    @discord.ui.button(label="Gen Queue Embed", style=discord.ButtonStyle.secondary, row=1)
    async def generate_queue_embed(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_generate_embed))
        await interaction.response.send_message("Select a game to generate queue embed:", view=view, ephemeral=True)

    async def do_generate_embed(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        await interaction.response.defer()
        await self.cog.start_queue(interaction.channel, game)
        await interaction.followup.send("Queue embed generated.", ephemeral=True)

    # Row 2: Player management
    @discord.ui.button(label="Suspensions", style=discord.ButtonStyle.secondary, row=2)
    async def manage_suspensions(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SuspensionManagementView(self.cog)
        await interaction.response.send_message("Manage Suspensions:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove from Queue", style=discord.ButtonStyle.secondary, row=2)
    async def remove_from_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_queue_players))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    async def show_queue_players(self, interaction: discord.Interaction, game_id: int):
        # Find queue for this game
        queue_state = None
        for qid, qs in self.cog.queues.items():
            if qs.game_id == game_id and qs.state in ("waiting", "ready_check"):
                queue_state = qs
                break

        if not queue_state or not queue_state.players:
            await interaction.response.send_message("No players in queue.", ephemeral=True)
            return

        view = QueuePlayerRemoveView(self.cog, game_id, queue_state)
        await interaction.response.send_message("Select a player to remove:", view=view, ephemeral=True)

    @discord.ui.button(label="Adjust W/L", style=discord.ButtonStyle.secondary, row=2)
    async def adjust_wl(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AdjustWLUserSelectView(self.cog)
        await interaction.response.send_message("Select a user:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Player MMR", style=discord.ButtonStyle.secondary, row=2)
    async def set_player_mmr_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = SetMMRUserSelectView(self.cog, games)
        await interaction.response.send_message("Select a user and game:", view=view, ephemeral=True)

    # Row 3: Setup and utilities
    @discord.ui.button(label="Setup New User", style=discord.ButtonStyle.success, row=3)
    async def setup_new_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = SetupUserGameSelectView(self.cog, games)
        await interaction.response.send_message("Step 1: Select a game for the new user:", view=view, ephemeral=True)


class ChangeWinnerModal(discord.ui.Modal, title="Change Match Winner"):
    match_id_input = discord.ui.TextInput(
        label="Match ID",
        placeholder="Enter the match ID",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            match_id = int(self.match_id_input.value)
            match = await DatabaseHelper.get_completed_match(match_id)
            if not match:
                await interaction.response.send_message("Match not found.", ephemeral=True)
                return

            view = ChangeWinnerSelectView(self.cog, match_id, match.get("winning_team"))
            game = await DatabaseHelper.get_game(match["game_id"])
            current = match.get("winning_team", "None")
            await interaction.response.send_message(
                f"Match #{match_id} ({game.name})\nCurrent winner: **{current}**\n\nSelect the new winner:",
                view=view, ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid match ID.", ephemeral=True)


class ChangeWinnerSelectView(discord.ui.View):
    """View for selecting a new winner for a match."""

    def __init__(self, cog: 'CustomMatch', match_id: int, current_winner: Optional[str]):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.current_winner = current_winner

    @discord.ui.button(label="Red Team Wins", style=discord.ButtonStyle.danger)
    async def red_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, Team.RED)

    @discord.ui.button(label="Blue Team Wins", style=discord.ButtonStyle.primary)
    async def blue_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, Team.BLUE)

    async def change_winner(self, interaction: discord.Interaction, new_winner: Team):
        # Reverse existing result if there was one
        if self.current_winner:
            await DatabaseHelper.reverse_match_result(self.match_id)

        # Apply new result
        await self.cog.finalize_match(interaction.guild, self.match_id, new_winner)
        await interaction.response.send_message(
            f"Match #{self.match_id} winner changed to **{new_winner.value}** team.",
            ephemeral=True
        )
        await self.cog.log_action(
            interaction.guild,
            f"Match #{self.match_id} winner changed to {new_winner.value} by {interaction.user.mention}"
        )


class SuspensionManagementView(discord.ui.View):
    """View for managing suspensions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add Suspension", style=discord.ButtonStyle.danger)
    async def add_suspension(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddSuspensionUserSelectView(self.cog)
        await interaction.response.send_message("Select a user to suspend:", view=view, ephemeral=True)

    @discord.ui.button(label="View Suspensions", style=discord.ButtonStyle.secondary)
    async def view_suspensions(self, interaction: discord.Interaction, button: discord.ui.Button):
        suspensions = await DatabaseHelper.get_all_suspensions()
        if not suspensions:
            await interaction.response.send_message("No active suspensions.", ephemeral=True)
            return

        lines = ["**Active Suspensions**\n"]
        for s in suspensions:
            user = interaction.guild.get_member(s.player_id)
            name = user.display_name if user else str(s.player_id)
            game = await DatabaseHelper.get_game(s.game_id) if s.game_id else None
            game_str = game.name if game else "All Games"
            until = s.suspended_until.strftime("%Y-%m-%d %H:%M UTC")
            reason = s.reason or "No reason"
            lines.append(f"**#{s.suspension_id}** - {name} ({game_str})")
            lines.append(f"  Until: {until} | Reason: {reason}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Remove Suspension", style=discord.ButtonStyle.success)
    async def remove_suspension(self, interaction: discord.Interaction, button: discord.ui.Button):
        suspensions = await DatabaseHelper.get_all_suspensions()
        if not suspensions:
            await interaction.response.send_message("No active suspensions.", ephemeral=True)
            return

        view = RemoveSuspensionSelectView(self.cog, suspensions, interaction.guild)
        await interaction.response.send_message("Select a suspension to remove:", view=view, ephemeral=True)


class AddSuspensionUserSelectView(discord.ui.View):
    """View for selecting a user to suspend."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        games = await DatabaseHelper.get_all_games()
        view = AddSuspensionGameSelectView(self.cog, user.id, games)
        await interaction.response.send_message(
            f"Suspending **{user.display_name}**\nSelect a game (or 'All Games'):",
            view=view, ephemeral=True
        )


class AddSuspensionGameSelectView(discord.ui.View):
    """View for selecting game for suspension."""

    def __init__(self, cog: 'CustomMatch', user_id: int, games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id

        options = [discord.SelectOption(label="All Games", value="all")]
        for g in games:
            options.append(discord.SelectOption(label=g.name, value=str(g.game_id)))

        select = discord.ui.Select(placeholder="Select game...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        game_id = None if value == "all" else int(value)
        modal = AddSuspensionModal(self.cog, self.user_id, game_id)
        await interaction.response.send_modal(modal)


class AddSuspensionModal(discord.ui.Modal, title="Add Suspension"):
    duration = discord.ui.TextInput(
        label="Duration (hours)",
        placeholder="e.g., 24 for 1 day, 168 for 1 week",
        required=True
    )
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="Reason for suspension",
        required=False,
        style=discord.TextStyle.paragraph
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: Optional[int]):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            hours = int(self.duration.value)
            until = datetime.now(timezone.utc) + timedelta(hours=hours)
            suspension_id = await DatabaseHelper.add_suspension(
                self.user_id, self.game_id, until,
                self.reason.value or None, interaction.user.id
            )

            user = interaction.guild.get_member(self.user_id)
            name = user.display_name if user else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id) if self.game_id else None
            game_str = game.name if game else "All Games"

            await interaction.response.send_message(
                f"Suspended **{name}** from **{game_str}** for {hours} hours.\nSuspension ID: #{suspension_id}",
                ephemeral=True
            )

            await self.cog.log_action(
                interaction.guild,
                f"Suspended {name} from {game_str} for {hours}h by {interaction.user.mention}"
            )
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)


class RemoveSuspensionSelectView(discord.ui.View):
    """View for selecting a suspension to remove."""

    def __init__(self, cog: 'CustomMatch', suspensions: List[Suspension], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        for s in suspensions[:25]:
            user = guild.get_member(s.player_id)
            name = user.display_name if user else str(s.player_id)
            options.append(discord.SelectOption(
                label=f"#{s.suspension_id} - {name}",
                value=str(s.suspension_id),
                description=s.reason[:50] if s.reason else "No reason"
            ))

        select = discord.ui.Select(placeholder="Select suspension...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        suspension_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_suspension(suspension_id)
        await interaction.response.send_message(
            f"Removed suspension #{suspension_id}.",
            ephemeral=True
        )


class QueuePlayerRemoveView(discord.ui.View):
    """View for removing a player from queue."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_state: 'QueueState'):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.queue_state = queue_state

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select player to remove...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        if user.id not in self.queue_state.players:
            await interaction.response.send_message("That player is not in the queue.", ephemeral=True)
            return

        del self.queue_state.players[user.id]
        await DatabaseHelper.remove_player_from_queue(self.queue_state.queue_id, user.id)

        game = await DatabaseHelper.get_game(self.game_id)
        channel = interaction.guild.get_channel(self.queue_state.channel_id)
        if channel and self.queue_state.message_id:
            try:
                msg = await channel.fetch_message(self.queue_state.message_id)
                embed = await self.cog.create_queue_embed(game, self.queue_state)
                view = QueueView(self.cog, self.game_id, self.queue_state.queue_id)
                await msg.edit(embed=embed, view=view)
            except:
                pass

        await interaction.response.send_message(
            f"Removed **{user.display_name}** from the queue.",
            ephemeral=True
        )


class AdjustWLUserSelectView(discord.ui.View):
    """View for selecting a user to adjust W/L."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return

        async def show_adjust_modal(inter: discord.Interaction, game_id: int):
            modal = AdjustWLModal(self.cog, user.id, game_id)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, show_adjust_modal))
        await interaction.response.send_message(
            f"Adjusting stats for **{user.display_name}**\nSelect a game:",
            view=view, ephemeral=True
        )


class AdjustWLModal(discord.ui.Modal, title="Adjust Wins/Losses"):
    wins_delta = discord.ui.TextInput(
        label="Wins adjustment",
        placeholder="e.g., 1 or -2",
        default="0",
        required=True
    )
    losses_delta = discord.ui.TextInput(
        label="Losses adjustment",
        placeholder="e.g., 1 or -2",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            wins = int(self.wins_delta.value)
            losses = int(self.losses_delta.value)

            await DatabaseHelper.adjust_player_stats(self.user_id, self.game_id, wins, losses)

            member = interaction.guild.get_member(self.user_id)
            name = member.display_name if member else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id)

            await interaction.response.send_message(
                f"Adjusted **{name}**'s stats for {game.name}: {wins:+d}W, {losses:+d}L",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid numbers.", ephemeral=True)


class SetMMRUserSelectView(discord.ui.View):
    """View for setting player MMR with user and game selection."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.games = games
        self.selected_user = None

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_user = select.values[0]

        async def show_mmr_modal(inter: discord.Interaction, game_id: int):
            modal = SetMMRModal(self.cog, self.selected_user.id, game_id)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(self.games, show_mmr_modal))
        await interaction.response.send_message(
            f"Setting MMR for **{self.selected_user.display_name}**\nSelect a game:",
            view=view, ephemeral=True
        )


class SetMMRModal(discord.ui.Modal, title="Set Player MMR"):
    mmr_value = discord.ui.TextInput(
        label="MMR Value",
        placeholder="e.g., 1500",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
            stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
            stats.mmr = mmr
            await DatabaseHelper.update_player_stats(stats)

            member = interaction.guild.get_member(self.user_id)
            name = member.display_name if member else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id)

            await interaction.response.send_message(
                f"Set **{name}**'s MMR for {game.name} to {mmr}.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class SetupUserGameSelectView(discord.ui.View):
    """Step 1: Select game for new user setup."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=120)
        self.cog = cog
        self.games = games

        options = [discord.SelectOption(label=g.name, value=str(g.game_id)) for g in games]
        select = discord.ui.Select(placeholder="Select game...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        game_id = int(interaction.data["values"][0])
        view = SetupUserSelectView(self.cog, game_id)
        await interaction.response.send_message(
            "Step 2: Select the user to set up:",
            view=view, ephemeral=True
        )


class SetupUserSelectView(discord.ui.View):
    """Step 2: Select user for setup."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        modal = SetupUserModal(self.cog, self.game_id, user.id)
        await interaction.response.send_modal(modal)


class SetupUserModal(discord.ui.Modal, title="Setup New User"):
    ign = discord.ui.TextInput(
        label="IGN (leave blank to skip)",
        placeholder="In-game name",
        required=False
    )
    mmr = discord.ui.TextInput(
        label="Starting MMR",
        placeholder="e.g., 1000",
        default="1000",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr_val = int(self.mmr.value)

            # Set IGN if provided
            if self.ign.value:
                await DatabaseHelper.set_player_ign(self.user_id, self.game_id, self.ign.value)

            # Set MMR
            stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
            stats.mmr = mmr_val
            await DatabaseHelper.update_player_stats(stats)

            # Give verified role if configured
            game = await DatabaseHelper.get_game(self.game_id)
            member = interaction.guild.get_member(self.user_id)

            if game.verified_role_id and member:
                role = interaction.guild.get_role(game.verified_role_id)
                if role and role not in member.roles:
                    await member.add_roles(role)

            name = member.display_name if member else str(self.user_id)
            lines = [f"Setup complete for **{name}** ({game.name}):"]
            if self.ign.value:
                lines.append(f"- IGN: `{self.ign.value}`")
            lines.append(f"- MMR: {mmr_val}")
            if game.verified_role_id:
                lines.append(f"- Verified role assigned")

            await interaction.response.send_message("\n".join(lines), ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class MatchSelectView(discord.ui.View):
    """View for selecting an active match."""
    
    def __init__(self, cog: 'CustomMatch', matches: List[dict], callback):
        super().__init__(timeout=60)
        self.cog = cog
        self.callback = callback
        
        options = []
        for m in matches[:25]:  # Discord limit
            game = asyncio.get_event_loop().run_until_complete(
                DatabaseHelper.get_game(m["game_id"])
            )
            game_name = game.name if game else "Unknown"
            options.append(discord.SelectOption(
                label=f"Match #{m['match_id']} - {game_name}",
                value=str(m["match_id"])
            ))
        
        if options:
            select = discord.ui.Select(placeholder="Select match...", options=options)
            select.callback = self.on_select
            self.add_item(select)
    
    async def on_select(self, interaction: discord.Interaction):
        match_id = int(interaction.data["values"][0])
        await self.callback(interaction, match_id)


class ForceWinnerView(discord.ui.View):
    """View for forcing a winner."""
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
    
    @discord.ui.button(label="Red Team Wins", style=discord.ButtonStyle.danger)
    async def red_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.finalize_match(interaction.guild, self.match_id, Team.RED)
        await interaction.response.send_message("Red team declared winner.", ephemeral=True)
    
    @discord.ui.button(label="Blue Team Wins", style=discord.ButtonStyle.primary)
    async def blue_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.finalize_match(interaction.guild, self.match_id, Team.BLUE)
        await interaction.response.send_message("Blue team declared winner.", ephemeral=True)


class SubstituteModal(discord.ui.Modal, title="Substitute Player"):
    out_player = discord.ui.TextInput(label="Player leaving (ID or @mention)", required=True)
    in_player = discord.ui.TextInput(label="Substitute player (ID or @mention)", required=True)
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__()
        self.cog = cog
        self.match_id = match_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            out_str = self.out_player.value.replace("<@", "").replace(">", "").replace("!", "")
            in_str = self.in_player.value.replace("<@", "").replace(">", "").replace("!", "")
            out_id = int(out_str)
            in_id = int(in_str)
            
            match = await DatabaseHelper.get_match(self.match_id)
            if not match:
                await interaction.response.send_message("Match not found.", ephemeral=True)
                return
            
            game = await DatabaseHelper.get_game(match["game_id"])
            
            # Check sub has verified role
            if game.verified_role_id:
                in_member = interaction.guild.get_member(in_id)
                if not in_member or not any(r.id == game.verified_role_id for r in in_member.roles):
                    await interaction.response.send_message(
                        "Substitute doesn't have the verified role.",
                        ephemeral=True
                    )
                    return
            
            # Get the outgoing player's team
            players = await DatabaseHelper.get_match_players(self.match_id)
            out_player_data = next((p for p in players if p["player_id"] == out_id), None)
            if not out_player_data:
                await interaction.response.send_message("Outgoing player not in this match.", ephemeral=True)
                return
            
            team = out_player_data["team"]
            
            # Remove old player, add new
            await DatabaseHelper.remove_match_player(self.match_id, out_id)
            await DatabaseHelper.add_match_player(
                self.match_id, in_id, team,
                was_sub=True, original_player_id=out_id
            )
            
            # Update roles
            out_member = interaction.guild.get_member(out_id)
            in_member = interaction.guild.get_member(in_id)
            
            role_id = match["red_role_id"] if team == "red" else match["blue_role_id"]
            role = interaction.guild.get_role(role_id)
            
            if role:
                if out_member:
                    await out_member.remove_roles(role)
                if in_member:
                    await in_member.add_roles(role)
            
            await interaction.response.send_message(
                f"Substituted <@{out_id}> with <@{in_id}> on {team} team.",
                ephemeral=True
            )
            
            # Log
            await self.cog.log_action(
                interaction.guild,
                f"Match #{self.match_id}: <@{out_id}> substituted by <@{in_id}>"
            )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


class SwapModal(discord.ui.Modal, title="Swap Players"):
    player1 = discord.ui.TextInput(label="Player 1 (ID or @mention)", required=True)
    player2 = discord.ui.TextInput(label="Player 2 (ID or @mention)", required=True)
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__()
        self.cog = cog
        self.match_id = match_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            p1_str = self.player1.value.replace("<@", "").replace(">", "").replace("!", "")
            p2_str = self.player2.value.replace("<@", "").replace(">", "").replace("!", "")
            p1_id = int(p1_str)
            p2_id = int(p2_str)
            
            match = await DatabaseHelper.get_match(self.match_id)
            players = await DatabaseHelper.get_match_players(self.match_id)
            
            p1_data = next((p for p in players if p["player_id"] == p1_id), None)
            p2_data = next((p for p in players if p["player_id"] == p2_id), None)
            
            if not p1_data or not p2_data:
                await interaction.response.send_message("Both players must be in the match.", ephemeral=True)
                return
            
            if p1_data["team"] == p2_data["team"]:
                await interaction.response.send_message("Players must be on different teams.", ephemeral=True)
                return
            
            # Swap teams in database
            await DatabaseHelper.remove_match_player(self.match_id, p1_id)
            await DatabaseHelper.remove_match_player(self.match_id, p2_id)
            await DatabaseHelper.add_match_player(
                self.match_id, p1_id, p2_data["team"],
                was_captain=p1_data["was_captain"]
            )
            await DatabaseHelper.add_match_player(
                self.match_id, p2_id, p1_data["team"],
                was_captain=p2_data["was_captain"]
            )
            
            # Swap roles
            p1_member = interaction.guild.get_member(p1_id)
            p2_member = interaction.guild.get_member(p2_id)
            
            red_role = interaction.guild.get_role(match["red_role_id"])
            blue_role = interaction.guild.get_role(match["blue_role_id"])
            
            if red_role and blue_role and p1_member and p2_member:
                if p1_data["team"] == "red":
                    await p1_member.remove_roles(red_role)
                    await p1_member.add_roles(blue_role)
                    await p2_member.remove_roles(blue_role)
                    await p2_member.add_roles(red_role)
                else:
                    await p1_member.remove_roles(blue_role)
                    await p1_member.add_roles(red_role)
                    await p2_member.remove_roles(red_role)
                    await p2_member.add_roles(blue_role)
            
            await interaction.response.send_message(
                f"Swapped <@{p1_id}> and <@{p2_id}>.",
                ephemeral=True
            )
            
            await self.cog.log_action(
                interaction.guild,
                f"Match #{self.match_id}: Swapped <@{p1_id}> and <@{p2_id}>"
            )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


# =============================================================================
# QUEUE VIEWS
# =============================================================================

class QueueView(discord.ui.View):
    """Main queue view with Join/Leave buttons."""
    
    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id
    
    @discord.ui.button(label="Join", style=discord.ButtonStyle.success, custom_id="queue_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_queue_join(interaction, self.game_id, self.queue_id)
    
    @discord.ui.button(label="Leave", style=discord.ButtonStyle.secondary, custom_id="queue_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_queue_leave(interaction, self.game_id, self.queue_id)


class ReadyCheckView(discord.ui.View):
    """Ready check view."""
    
    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id
    
    @discord.ui.button(label="Ready", style=discord.ButtonStyle.success, custom_id="ready_yes")
    async def ready(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_ready(interaction, self.queue_id, True)
    
    @discord.ui.button(label="Not Ready", style=discord.ButtonStyle.danger, custom_id="ready_no")
    async def not_ready(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_ready(interaction, self.queue_id, False)


class WinVoteView(discord.ui.View):
    """Win vote view."""
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id
    
    @discord.ui.button(label="Red Team", style=discord.ButtonStyle.danger, custom_id="vote_red")
    async def vote_red(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_win_vote(interaction, self.match_id, Team.RED)
    
    @discord.ui.button(label="Blue Team", style=discord.ButtonStyle.primary, custom_id="vote_blue")
    async def vote_blue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_win_vote(interaction, self.match_id, Team.BLUE)


class AbandonVoteView(discord.ui.View):
    """Abandon vote view."""

    def __init__(self, cog: 'CustomMatch', match_id: int, needed_votes: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.match_id = match_id
        self.needed_votes = needed_votes

    @discord.ui.button(label="Vote to Abandon", style=discord.ButtonStyle.danger, custom_id="abandon_vote")
    async def vote_abandon(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_abandon_vote(interaction, self.match_id, self.needed_votes)


class IGNModal(discord.ui.Modal, title="Set Your IGN"):
    ign = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter your in-game name...",
        required=True,
        max_length=100
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.title = f"Set Your {game_name} IGN"

    async def on_submit(self, interaction: discord.Interaction):
        await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, self.ign.value)
        await interaction.response.send_message(
            f"Your IGN for this game has been set to: `{self.ign.value}`",
            ephemeral=True
        )


class CaptainDraftView(discord.ui.View):
    """View for captain drafting."""
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id


# =============================================================================
# LEADERBOARD VIEW
# =============================================================================

class LeaderboardView(discord.ui.View):
    """Leaderboard toggle view."""
    
    def __init__(self, cog: 'CustomMatch', game_id: int, monthly: bool = True):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id
        self.monthly = monthly
        self.update_button()
    
    def update_button(self):
        self.clear_items()
        if self.monthly:
            btn = discord.ui.Button(label="Switch to All-Time", style=discord.ButtonStyle.secondary)
        else:
            btn = discord.ui.Button(label="Switch to Monthly", style=discord.ButtonStyle.secondary)
        btn.callback = self.toggle
        self.add_item(btn)
    
    async def toggle(self, interaction: discord.Interaction):
        self.monthly = not self.monthly
        self.update_button()
        embed = await self.cog.build_leaderboard_embed(interaction.guild, self.game_id, self.monthly)
        await interaction.response.edit_message(embed=embed, view=self)


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
        self.queue_timeout_task: Optional[asyncio.Task] = None
        self.penalty_decay_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await init_db()
        # Start background tasks
        self.queue_timeout_task = asyncio.create_task(self.queue_timeout_check())
        self.penalty_decay_task = asyncio.create_task(self.penalty_decay_check())
        logger.info("CustomMatch cog loaded, database initialized.")

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
                        if pid in queue_state.players and (now - joined_at) > timeout_delta:
                            del queue_state.players[pid]
                            await DatabaseHelper.remove_player_from_queue(queue_id, pid)
                            removed.append(pid)

                    if removed:
                        # Update embed
                        guild = self.bot.get_guild(int(await DatabaseHelper.get_config("guild_id") or 0))
                        if guild:
                            channel = guild.get_channel(queue_state.channel_id)
                            if channel and queue_state.message_id:
                                try:
                                    msg = await channel.fetch_message(queue_state.message_id)
                                    embed = await self.create_queue_embed(game, queue_state)
                                    view = QueueView(self, game.game_id, queue_id)
                                    await msg.edit(embed=embed, view=view)
                                    mentions = ", ".join([f"<@{pid}>" for pid in removed])
                                    await channel.send(
                                        f"Removed from queue (timeout): {mentions}",
                                        delete_after=10
                                    )
                                except:
                                    pass

            except Exception as e:
                logger.error(f"Queue timeout check error: {e}")

            await asyncio.sleep(60)  # Check every minute

    async def penalty_decay_check(self):
        """Background task to decay old penalty offenses."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                # This runs daily - actual decay logic is in add_ready_penalty_offense
                # based on penalty_decay_days setting
                # We just clean up expired penalties from the database here
                async with aiosqlite.connect(DB_PATH) as db:
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
    
    async def log_action(self, guild: discord.Guild, message: str):
        """Log an action to the log channel."""
        channel_id = await DatabaseHelper.get_config("log_channel_id")
        if channel_id:
            channel = guild.get_channel(int(channel_id))
            if channel:
                await channel.send(f"`[{datetime.now().strftime('%H:%M:%S')}]` {message}")
    
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
    
    async def create_queue_embed(self, game: GameConfig, queue_state: QueueState) -> discord.Embed:
        """Create the queue embed."""
        player_count = len(queue_state.players)
        
        embed = discord.Embed(
            title=f"{game.name} Queue ({game.queue_type.value.upper()})",
            description=f"Players: {player_count}/{game.player_count}",
            color=COLOR_NEUTRAL
        )
        
        if queue_state.players:
            player_list = "\n".join([f"• <@{pid}>" for pid in queue_state.players.keys()])
            embed.add_field(name="Joined", value=player_list, inline=False)
        
        return embed
    
    async def create_ready_check_embed(self, game: GameConfig, queue_state: QueueState,
                                        time_remaining: int) -> discord.Embed:
        """Create the ready check embed."""
        ready = [pid for pid, is_ready in queue_state.players.items() if is_ready]
        waiting = [pid for pid, is_ready in queue_state.players.items() if not is_ready]
        
        embed = discord.Embed(
            title="Queue Full - Ready Check!",
            description=f"Time remaining: {time_remaining}s",
            color=COLOR_WARNING
        )
        
        if ready:
            embed.add_field(
                name=f"Ready ({len(ready)})",
                value="\n".join([f"• <@{pid}>" for pid in ready]),
                inline=True
            )
        
        if waiting:
            embed.add_field(
                name=f"Waiting ({len(waiting)})",
                value="\n".join([f"• <@{pid}>" for pid in waiting]),
                inline=True
            )
        
        return embed
    
    async def start_queue(self, channel: discord.TextChannel, game: GameConfig) -> int:
        """Start a new queue for a game."""
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "INSERT INTO active_queues (game_id, channel_id, state) VALUES (?, ?, 'waiting')",
                (game.game_id, channel.id)
            )
            queue_id = cursor.lastrowid
            await db.commit()
        
        queue_state = QueueState(
            queue_id=queue_id,
            game_id=game.game_id,
            channel_id=channel.id
        )
        self.queues[queue_id] = queue_state
        
        embed = await self.create_queue_embed(game, queue_state)
        view = QueueView(self, game.game_id, queue_id)
        msg = await channel.send(embed=embed, view=view)
        
        queue_state.message_id = msg.id
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                (msg.id, queue_id)
            )
            await db.commit()
        
        return queue_id
    
    async def handle_queue_join(self, interaction: discord.Interaction, game_id: int, queue_id: int):
        """Handle a player joining the queue."""
        user = interaction.user
        game = await DatabaseHelper.get_game(game_id)
        
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return
        
        queue_state = self.queues[queue_id]
        
        # Check if queue is still in waiting state
        if queue_state.state != "waiting":
            await interaction.response.send_message("Queue is no longer accepting players.", ephemeral=True)
            return
        
        # Check blacklist
        if await DatabaseHelper.is_blacklisted(user.id):
            await interaction.response.send_message("You are blacklisted from queues.", ephemeral=True)
            return

        # Check suspension
        suspension = await DatabaseHelper.is_suspended(user.id, game_id)
        if suspension:
            until = suspension.suspended_until.strftime("%Y-%m-%d %H:%M UTC")
            reason = suspension.reason or "No reason provided"
            await interaction.response.send_message(
                f"You are suspended from this game until {until}.\nReason: {reason}",
                ephemeral=True
            )
            return

        # Check penalty
        penalty_expires = await DatabaseHelper.is_penalized(user.id)
        if penalty_expires:
            until = penalty_expires.strftime("%Y-%m-%d %H:%M UTC")
            await interaction.response.send_message(
                f"You are penalized for missing a ready check.\nPenalty expires: {until}",
                ephemeral=True
            )
            return

        # Check verified role (if required)
        if game.queue_role_required and game.verified_role_id:
            if not any(r.id == game.verified_role_id for r in user.roles):
                await interaction.response.send_message(
                    "You need the verified role to queue for this game.",
                    ephemeral=True
                )
                return
        
        # Check if already in this queue
        if user.id in queue_state.players:
            await interaction.response.send_message("You're already in this queue.", ephemeral=True)
            return
        
        # Check if in active match for this game
        if await DatabaseHelper.get_player_in_active_match(user.id, game_id):
            await interaction.response.send_message(
                "You're already in an active match for this game.",
                ephemeral=True
            )
            return
        
        # Check if in another queue for this game
        for qid, qs in self.queues.items():
            if qid != queue_id and qs.game_id == game_id and user.id in qs.players:
                await interaction.response.send_message(
                    "You're already in another queue for this game.",
                    ephemeral=True
                )
                return
        
        # Add to queue
        queue_state.players[user.id] = False  # Not ready yet
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO queue_players (queue_id, player_id) VALUES (?, ?)",
                (queue_id, user.id)
            )
            await db.commit()
        
        # Update embed
        embed = await self.create_queue_embed(game, queue_state)
        await interaction.response.edit_message(embed=embed)
        
        # Check if queue is full
        if len(queue_state.players) >= game.player_count:
            await self.start_ready_check(interaction.channel, game, queue_state)
    
    async def handle_queue_leave(self, interaction: discord.Interaction, game_id: int, queue_id: int):
        """Handle a player leaving the queue."""
        user = interaction.user
        game = await DatabaseHelper.get_game(game_id)
        
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
        
        # Remove from queue
        del queue_state.players[user.id]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                (queue_id, user.id)
            )
            await db.commit()
        
        # Update embed
        embed = await self.create_queue_embed(game, queue_state)
        await interaction.response.edit_message(embed=embed)
    
    async def start_ready_check(self, channel: discord.TextChannel, game: GameConfig, 
                                 queue_state: QueueState):
        """Start the ready check phase."""
        queue_state.state = "ready_check"
        queue_state.ready_check_started = datetime.now(timezone.utc)
        
        # Remove players from other game queues
        for pid in list(queue_state.players.keys()):
            for qid, qs in list(self.queues.items()):
                if qid != queue_state.queue_id and pid in qs.players:
                    del qs.players[pid]
                    # Update that queue's embed
                    other_game = await DatabaseHelper.get_game(qs.game_id)
                    if other_game and qs.message_id:
                        try:
                            other_channel = channel.guild.get_channel(qs.channel_id)
                            if other_channel:
                                msg = await other_channel.fetch_message(qs.message_id)
                                embed = await self.create_queue_embed(other_game, qs)
                                await msg.edit(embed=embed)
                        except:
                            pass
        
        # Update database
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_queues SET state = 'ready_check', ready_check_started = ? WHERE queue_id = ?",
                (queue_state.ready_check_started.isoformat(), queue_state.queue_id)
            )
            await db.commit()
        
        # Edit message with ready check
        embed = await self.create_ready_check_embed(game, queue_state, game.ready_timer_seconds)
        view = ReadyCheckView(self, game.game_id, queue_state.queue_id)
        
        try:
            msg = await channel.fetch_message(queue_state.message_id)
            await msg.edit(embed=embed, view=view)
        except:
            pass
        
        # Ping players
        mentions = " ".join([f"<@{pid}>" for pid in queue_state.players.keys()])
        ping_msg = await channel.send(f"Ready check! {mentions}")
        await asyncio.sleep(3)
        await ping_msg.delete()

        # DM players if enabled
        if game.dm_ready_up:
            for pid in queue_state.players.keys():
                member = channel.guild.get_member(pid)
                if member:
                    try:
                        await member.send(
                            f"**{game.name}** queue is ready!\n"
                            f"Click the Ready button in {channel.mention} within {game.ready_timer_seconds} seconds."
                        )
                    except:
                        pass

        # Start timeout task
        task = asyncio.create_task(
            self.ready_check_timeout(channel, game, queue_state)
        )
        self.ready_check_tasks[queue_state.queue_id] = task
    
    async def ready_check_timeout(self, channel: discord.TextChannel, game: GameConfig,
                                   queue_state: QueueState):
        """Handle ready check timeout."""
        timer = game.ready_timer_seconds
        
        while timer > 0:
            await asyncio.sleep(5)
            timer -= 5
            
            # Check if all ready
            if queue_state.state != "ready_check":
                return
            
            if all(queue_state.players.values()):
                # All ready
                await self.proceed_to_match(channel, game, queue_state)
                return
            
            # Update embed with new time
            try:
                msg = await channel.fetch_message(queue_state.message_id)
                embed = await self.create_ready_check_embed(game, queue_state, timer)
                await msg.edit(embed=embed)
            except:
                pass
        
        # Time's up - remove unready players and apply penalties
        unready = [pid for pid, ready in queue_state.players.items() if not ready]

        # Apply penalties to unready players
        penalty_messages = []
        for pid in unready:
            del queue_state.players[pid]
            # Apply penalty
            offense_count, penalty_expires = await DatabaseHelper.add_ready_penalty_offense(pid, game)
            penalty_messages.append(f"<@{pid}> (Offense #{offense_count})")

            # DM the player if DM is enabled
            if game.dm_ready_up:
                member = channel.guild.get_member(pid)
                if member:
                    try:
                        await member.send(
                            f"You missed the ready check for **{game.name}**.\n"
                            f"This is offense #{offense_count}. You are penalized until "
                            f"{penalty_expires.strftime('%Y-%m-%d %H:%M UTC')}."
                        )
                    except:
                        pass

        # Revert to waiting state
        queue_state.state = "waiting"
        for pid in queue_state.players:
            queue_state.players[pid] = False

        embed = await self.create_queue_embed(game, queue_state)
        view = QueueView(self, game.game_id, queue_state.queue_id)

        try:
            msg = await channel.fetch_message(queue_state.message_id)
            await msg.edit(embed=embed, view=view)

            if penalty_messages:
                await channel.send(
                    f"Removed and penalized for not readying: {', '.join(penalty_messages)}",
                    delete_after=15
                )
        except:
            pass
    
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
        
        if is_ready:
            queue_state.players[user.id] = True
            
            # Check if all ready
            if all(queue_state.players.values()):
                # Cancel timeout task
                if queue_id in self.ready_check_tasks:
                    self.ready_check_tasks[queue_id].cancel()
                    del self.ready_check_tasks[queue_id]
                
                await interaction.response.defer()
                await self.proceed_to_match(interaction.channel, game, queue_state)
                return
            
            # Update embed
            elapsed = (datetime.now(timezone.utc) - queue_state.ready_check_started).seconds
            remaining = max(0, game.ready_timer_seconds - elapsed)
            embed = await self.create_ready_check_embed(game, queue_state, remaining)
            await interaction.response.edit_message(embed=embed)
        else:
            # Not ready - remove from queue, revert to waiting
            del queue_state.players[user.id]
            
            # Cancel timeout
            if queue_id in self.ready_check_tasks:
                self.ready_check_tasks[queue_id].cancel()
                del self.ready_check_tasks[queue_id]
            
            queue_state.state = "waiting"
            for pid in queue_state.players:
                queue_state.players[pid] = False
            
            embed = await self.create_queue_embed(game, queue_state)
            view = QueueView(self, game.game_id, queue_state.queue_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    # -------------------------------------------------------------------------
    # MATCH CREATION
    # -------------------------------------------------------------------------
    
    async def proceed_to_match(self, channel: discord.TextChannel, game: GameConfig,
                                queue_state: QueueState):
        """Proceed from ready check to match creation."""
        guild = channel.guild
        player_ids = list(queue_state.players.keys())
        
        # Get category
        category_id = await DatabaseHelper.get_config("category_id")
        category = guild.get_channel(int(category_id)) if category_id else None
        
        if not category:
            await channel.send("Error: Category not configured. Contact an admin.")
            return
        
        # Create match in database
        match_id = await DatabaseHelper.create_match(
            game.game_id, 
            game.queue_type.value,
            queue_state.message_id
        )
        
        # Create team roles
        red_num = await self.get_next_role_number(guild, "Red")
        blue_num = await self.get_next_role_number(guild, "Blue")
        
        red_role = await guild.create_role(name=f"Red{red_num}", color=discord.Color.red())
        blue_role = await guild.create_role(name=f"Blue{blue_num}", color=discord.Color.blue())
        
        await DatabaseHelper.update_match(
            match_id,
            red_role_id=red_role.id,
            blue_role_id=blue_role.id
        )
        
        # Update queue embed to "ongoing"
        embed = discord.Embed(
            title=f"{game.name} - Match in Progress",
            description=f"Match #{match_id}",
            color=COLOR_NEUTRAL
        )
        embed.add_field(
            name="Teams",
            value=f"{red_role.mention} vs {blue_role.mention}",
            inline=False
        )
        
        try:
            msg = await channel.fetch_message(queue_state.message_id)
            await msg.edit(embed=embed, view=None)
        except:
            pass
        
        # Remove queue from active
        del self.queues[queue_state.queue_id]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (queue_state.queue_id,))
            await db.commit()
        
        # Start new queue
        await self.start_queue(channel, game)
        
        # Route based on queue type
        if game.queue_type == QueueType.CAPTAINS:
            await self.start_captain_draft(guild, category, game, match_id, player_ids, red_role, blue_role)
        else:
            # MMR or Random - balance teams and create match channel
            if game.queue_type == QueueType.MMR:
                red_team, blue_team = await self.balance_teams_mmr(player_ids, game.game_id)
            else:
                random.shuffle(player_ids)
                mid = len(player_ids) // 2
                red_team = player_ids[:mid]
                blue_team = player_ids[mid:]
            
            await self.create_match_channel(guild, category, game, match_id, 
                                             red_team, blue_team, red_role, blue_role)
    
    async def balance_teams_mmr(self, player_ids: List[int], game_id: int) -> Tuple[List[int], List[int]]:
        """Balance teams by MMR using snake draft."""
        # Get all player stats
        players_with_mmr = []
        for pid in player_ids:
            stats = await DatabaseHelper.get_player_stats(pid, game_id)
            players_with_mmr.append((pid, stats.effective_mmr))
        
        # Sort by MMR descending
        players_with_mmr.sort(key=lambda x: x[1], reverse=True)
        
        # Snake draft
        red_team = []
        blue_team = []
        
        for i, (pid, mmr) in enumerate(players_with_mmr):
            # Pattern: 0->A, 1->B, 2->B, 3->A, 4->A, 5->B, 6->B, 7->A...
            position_in_pair = i % 4
            if position_in_pair == 0 or position_in_pair == 3:
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
                stats = await DatabaseHelper.get_player_stats(pid, game_id)
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
        
        async def update_draft_embed():
            embed = discord.Embed(
                title=f"{game.name} - Captain Draft",
                color=COLOR_NEUTRAL
            )
            
            red_list = "\n".join([f"• <@{pid}>" + (" (C)" if pid == red_captain else "") 
                                  for pid in red_team])
            blue_list = "\n".join([f"• <@{pid}>" + (" (C)" if pid == blue_captain else "") 
                                   for pid in blue_team])
            
            embed.add_field(name=f"Red Team ({red_role.mention})", value=red_list or "None", inline=True)
            embed.add_field(name=f"Blue Team ({blue_role.mention})", value=blue_list or "None", inline=True)
            
            if available:
                avail_list = "\n".join([f"• <@{pid}>" for pid in available])
                embed.add_field(name="Available", value=avail_list, inline=False)
                
                if current_picker == "red":
                    embed.add_field(name="Now Picking", value=f"<@{red_captain}> (Red)", inline=False)
                else:
                    embed.add_field(name="Now Picking", value=f"<@{blue_captain}> (Blue)", inline=False)
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
    
    async def create_match_channel(self, guild: discord.Guild, category: discord.CategoryChannel,
                                    game: GameConfig, match_id: int,
                                    red_team: List[int], blue_team: List[int],
                                    red_role: discord.Role, blue_role: discord.Role,
                                    draft_channel: discord.TextChannel = None):
        """Create the match channel and assign roles."""
        # Create channel
        channel_name = await self.get_next_channel_suffix(guild, category, "match-lobby")

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            red_role: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            blue_role: discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }

        match_channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
        await DatabaseHelper.update_match(match_id, channel_id=match_channel.id)

        # Create team VCs if enabled
        red_vc_id = None
        blue_vc_id = None
        if game.vc_creation_enabled:
            red_vc_overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.me: discord.PermissionOverwrite(view_channel=True, connect=True),
                red_role: discord.PermissionOverwrite(view_channel=True, connect=True),
                blue_role: discord.PermissionOverwrite(view_channel=False, connect=False)
            }
            blue_vc_overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.me: discord.PermissionOverwrite(view_channel=True, connect=True),
                blue_role: discord.PermissionOverwrite(view_channel=True, connect=True),
                red_role: discord.PermissionOverwrite(view_channel=False, connect=False)
            }

            red_vc = await category.create_voice_channel(
                name=f"Red Team #{match_id}",
                overwrites=red_vc_overwrites
            )
            blue_vc = await category.create_voice_channel(
                name=f"Blue Team #{match_id}",
                overwrites=blue_vc_overwrites
            )
            red_vc_id = red_vc.id
            blue_vc_id = blue_vc.id
            await DatabaseHelper.update_match(match_id, red_vc_id=red_vc_id, blue_vc_id=blue_vc_id)

        # Assign roles and add to database
        for pid in red_team:
            member = guild.get_member(pid)
            if member:
                await member.add_roles(red_role)
            # Check if already added (captains)
            players = await DatabaseHelper.get_match_players(match_id)
            if not any(p["player_id"] == pid for p in players):
                await DatabaseHelper.add_match_player(match_id, pid, "red")

        for pid in blue_team:
            member = guild.get_member(pid)
            if member:
                await member.add_roles(blue_role)
            players = await DatabaseHelper.get_match_players(match_id)
            if not any(p["player_id"] == pid for p in players):
                await DatabaseHelper.add_match_player(match_id, pid, "blue")

        # Get IGNs for all players
        igns = await DatabaseHelper.get_match_igns(match_id)

        # Build match embed with rivalries and IGNs
        embed = discord.Embed(
            title=f"{game.name} Match #{match_id}",
            color=COLOR_NEUTRAL
        )

        # Get captain info
        players = await DatabaseHelper.get_match_players(match_id)
        red_captain = next((p["player_id"] for p in players if p["team"] == "red" and p["was_captain"]), None)
        blue_captain = next((p["player_id"] for p in players if p["team"] == "blue" and p["was_captain"]), None)

        # Build team lists with IGNs
        red_lines = []
        for pid in red_team:
            line = f"<@{pid}>"
            if pid == red_captain:
                line += " (C)"
            if pid in igns:
                line += f" - `{igns[pid]}`"
            red_lines.append(line)

        blue_lines = []
        for pid in blue_team:
            line = f"<@{pid}>"
            if pid == blue_captain:
                line += " (C)"
            if pid in igns:
                line += f" - `{igns[pid]}`"
            blue_lines.append(line)

        embed.add_field(name="Red Team", value="\n".join(red_lines), inline=True)
        embed.add_field(name="Blue Team", value="\n".join(blue_lines), inline=True)

        # Find rivalries
        rivalries = []
        for red_pid in red_team:
            for blue_pid in blue_team:
                rivalry = await DatabaseHelper.get_rivalry(red_pid, blue_pid, game.game_id)
                if rivalry and (rivalry[0] + rivalry[1]) >= RIVALRY_MIN_GAMES:
                    total = rivalry[0] + rivalry[1]
                    red_wins = rivalry[0]
                    win_rate = (red_wins / total) * 100
                    rivalries.append((red_pid, blue_pid, red_wins, rivalry[1], win_rate))

        if rivalries:
            rivalry_lines = []
            for red_pid, blue_pid, r_wins, b_wins, win_rate in rivalries[:3]:
                if win_rate < 50:
                    rivalry_lines.append(
                        f"<@{red_pid}> has a {win_rate:.0f}% win rate vs <@{blue_pid}> ({r_wins}-{b_wins})"
                    )
                else:
                    rivalry_lines.append(
                        f"<@{red_pid}> vs <@{blue_pid}>: {r_wins}-{b_wins}"
                    )
            embed.add_field(name="Storylines", value="\n".join(rivalry_lines), inline=False)

        # Add VC info if created
        if red_vc_id and blue_vc_id:
            embed.add_field(
                name="Voice Channels",
                value=f"Red: <#{red_vc_id}>\nBlue: <#{blue_vc_id}>",
                inline=False
            )

        embed.add_field(
            name="Report Winner",
            value="Use `/win` when the match is over, or `/abandon` to cancel.",
            inline=False
        )

        await match_channel.send(f"{red_role.mention} vs {blue_role.mention}", embed=embed)

        # Delete draft channel if exists
        if draft_channel:
            await draft_channel.delete()

        # Start 3-hour timeout
        task = asyncio.create_task(self.match_timeout(guild, match_id, match_channel))
        self.match_timeout_tasks[match_id] = task
        
        # Log
        await self.log_action(guild, f"Match #{match_id} started in {match_channel.mention}")
    
    async def match_timeout(self, guild: discord.Guild, match_id: int, channel: discord.TextChannel):
        """Handle 3-hour match timeout."""
        await asyncio.sleep(3 * 60 * 60)  # 3 hours
        
        # Check if match still active
        match = await DatabaseHelper.get_match(match_id)
        if match and not match["winning_team"] and not match["cancelled"]:
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
        
        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match_id)
        if not any(p["player_id"] == user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
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
        needed = (game.player_count // 2) + 1
        
        if red_votes >= needed:
            await interaction.response.defer()
            await self.finalize_match(interaction.guild, match_id, Team.RED)
        elif blue_votes >= needed:
            await interaction.response.defer()
            await self.finalize_match(interaction.guild, match_id, Team.BLUE)
        else:
            # Update embed
            embed = discord.Embed(
                title="Who Won?",
                description=f"Cast your vote! ({needed} votes needed)",
                color=COLOR_NEUTRAL
            )
            embed.add_field(name="Red Team", value=f"{red_votes} votes", inline=True)
            embed.add_field(name="Blue Team", value=f"{blue_votes} votes", inline=True)
            
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
            await self.cancel_match(interaction.guild, match_id)
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
        """Finalize a match and update stats."""
        match = await DatabaseHelper.get_match(match_id)
        if not match or match["winning_team"]:
            return
        
        game = await DatabaseHelper.get_game(match["game_id"])
        players = await DatabaseHelper.get_match_players(match_id)
        
        # Determine winners and losers
        winners = [p["player_id"] for p in players if p["team"] == winning_team.value]
        losers = [p["player_id"] for p in players if p["team"] != winning_team.value]
        
        # Calculate average MMR for each team
        winner_mmr = []
        loser_mmr = []
        
        for pid in winners:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            winner_mmr.append(stats.effective_mmr)
        
        for pid in losers:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            loser_mmr.append(stats.effective_mmr)
        
        avg_winner_mmr = sum(winner_mmr) / len(winner_mmr) if winner_mmr else 1000
        avg_loser_mmr = sum(loser_mmr) / len(loser_mmr) if loser_mmr else 1000
        
        # Calculate expected scores (ELO formula)
        expected_winner = 1 / (1 + 10 ** ((avg_loser_mmr - avg_winner_mmr) / 400))
        expected_loser = 1 - expected_winner
        
        # Update winner stats
        for pid in winners:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            k = stats.get_k_factor()
            mmr_change = int(k * (1 - expected_winner))
            
            old_mmr = stats.mmr
            stats.mmr += mmr_change
            stats.wins += 1
            stats.games_played += 1
            stats.last_played = datetime.now(timezone.utc)
            
            await DatabaseHelper.update_player_stats(stats)
            await DatabaseHelper.record_mmr_change(pid, game.game_id, match_id, old_mmr, stats.mmr)
        
        # Update loser stats
        for pid in losers:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            k = stats.get_k_factor()
            mmr_change = int(k * (0 - expected_loser))
            
            old_mmr = stats.mmr
            stats.mmr += mmr_change  # This will be negative
            stats.losses += 1
            stats.games_played += 1
            stats.last_played = datetime.now(timezone.utc)
            
            await DatabaseHelper.update_player_stats(stats)
            await DatabaseHelper.record_mmr_change(pid, game.game_id, match_id, old_mmr, stats.mmr)
        
        # Update rivalries
        for winner_id in winners:
            for loser_id in losers:
                await DatabaseHelper.update_rivalry(winner_id, loser_id, game.game_id)
        
        # Mark match complete
        await DatabaseHelper.update_match(
            match_id,
            winning_team=winning_team.value,
            decided_at=datetime.now(timezone.utc).isoformat()
        )

        # Cancel timeout task
        if match_id in self.match_timeout_tasks:
            self.match_timeout_tasks[match_id].cancel()
            del self.match_timeout_tasks[match_id]

        # Send match history embed if configured
        if game.match_history_channel_id:
            history_channel = guild.get_channel(game.match_history_channel_id)
            if history_channel:
                await self.send_match_history_embed(history_channel, game, match_id, players, winning_team)

        # Clean up
        await self.cleanup_match(guild, match)

        # Log
        await self.log_action(
            guild,
            f"Match #{match_id} ({game.name}): {winning_team.value.capitalize()} team wins!"
        )
    
    async def cancel_match(self, guild: discord.Guild, match_id: int):
        """Cancel a match without updating stats."""
        match = await DatabaseHelper.get_match(match_id)
        if not match:
            return
        
        await DatabaseHelper.update_match(match_id, cancelled=1)
        
        # Cancel timeout
        if match_id in self.match_timeout_tasks:
            self.match_timeout_tasks[match_id].cancel()
            del self.match_timeout_tasks[match_id]
        
        await self.cleanup_match(guild, match)
        
        game = await DatabaseHelper.get_game(match["game_id"])
        await self.log_action(guild, f"Match #{match_id} ({game.name}) cancelled.")
    
    async def cleanup_match(self, guild: discord.Guild, match: dict):
        """Clean up match channels, VCs, and roles."""
        # Delete match channel
        if match["channel_id"]:
            channel = guild.get_channel(match["channel_id"])
            if channel:
                await channel.delete()

        # Delete draft channel
        if match["draft_channel_id"]:
            channel = guild.get_channel(match["draft_channel_id"])
            if channel:
                await channel.delete()

        # Delete team VCs if they exist
        if match.get("red_vc_id"):
            vc = guild.get_channel(match["red_vc_id"])
            if vc:
                await vc.delete()

        if match.get("blue_vc_id"):
            vc = guild.get_channel(match["blue_vc_id"])
            if vc:
                await vc.delete()

        # Delete roles
        if match["red_role_id"]:
            role = guild.get_role(match["red_role_id"])
            if role:
                await role.delete()

        if match["blue_role_id"]:
            role = guild.get_role(match["blue_role_id"])
            if role:
                await role.delete()

        # Delete queue message
        if match["queue_message_id"]:
            game = await DatabaseHelper.get_game(match["game_id"])
            if game and game.queue_channel_id:
                channel = guild.get_channel(game.queue_channel_id)
                if channel:
                    try:
                        msg = await channel.fetch_message(match["queue_message_id"])
                        await msg.delete()
                    except:
                        pass

    async def send_match_history_embed(self, channel: discord.TextChannel, game: GameConfig,
                                       match_id: int, players: List[dict], winning_team: Team):
        """Send match history embed to configured channel."""
        igns = await DatabaseHelper.get_match_igns(match_id)

        # Get team members
        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        # Build team lists with IGNs
        red_lines = []
        for p in red_players:
            pid = p["player_id"]
            line = f"<@{pid}>"
            if p.get("was_captain"):
                line += " (C)"
            if pid in igns:
                line += f" - `{igns[pid]}`"
            red_lines.append(line)

        blue_lines = []
        for p in blue_players:
            pid = p["player_id"]
            line = f"<@{pid}>"
            if p.get("was_captain"):
                line += " (C)"
            if pid in igns:
                line += f" - `{igns[pid]}`"
            blue_lines.append(line)

        # Create embed
        winner_color = COLOR_RED if winning_team == Team.RED else COLOR_BLUE
        embed = discord.Embed(
            title=f"{game.name} Match #{match_id} - Complete",
            color=winner_color,
            timestamp=datetime.now(timezone.utc)
        )

        # Add winner indicator
        red_header = "Red Team" + (" ★ WINNER" if winning_team == Team.RED else "")
        blue_header = "Blue Team" + (" ★ WINNER" if winning_team == Team.BLUE else "")

        embed.add_field(name=red_header, value="\n".join(red_lines) or "None", inline=True)
        embed.add_field(name=blue_header, value="\n".join(blue_lines) or "None", inline=True)

        embed.set_footer(text=f"Match ID: {match_id}")

        await channel.send(embed=embed)
    
    # -------------------------------------------------------------------------
    # LEADERBOARD & STATS
    # -------------------------------------------------------------------------
    
    async def build_leaderboard_embed(self, guild: discord.Guild, game_id: int, 
                                       monthly: bool = True) -> discord.Embed:
        """Build a leaderboard embed."""
        game = await DatabaseHelper.get_game(game_id)
        leaderboard = await DatabaseHelper.get_leaderboard(game_id, monthly=monthly)
        
        if monthly:
            now = datetime.now(timezone.utc)
            title = f"{game.name} Leaderboard - {now.strftime('%B %Y')}"
        else:
            title = f"{game.name} Leaderboard - All Time"
        
        embed = discord.Embed(title=title, color=COLOR_NEUTRAL)
        
        if not leaderboard:
            embed.description = "No matches played yet."
            return embed
        
        lines = []
        for i, entry in enumerate(leaderboard, 1):
            member = guild.get_member(entry["player_id"])
            name = member.display_name if member else str(entry["player_id"])
            wins = entry["wins"]
            losses = entry["losses"]
            total = wins + losses
            winrate = (wins / total * 100) if total > 0 else 0
            
            lines.append(f"`{i:>2}.` **{name}** — {wins}W {losses}L ({winrate:.0f}%)")
        
        embed.description = "\n".join(lines)
        return embed
    
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
    
    cm_group = app_commands.Group(name="cm", description="Custom match commands")
    
    @cm_group.command(name="settings", description="Open the settings panel (Server Admin)")
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
        category_id = await DatabaseHelper.get_config("category_id")
        log_id = await DatabaseHelper.get_config("log_channel_id")
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        
        config_lines = []
        if category_id:
            cat = interaction.guild.get_channel(int(category_id))
            config_lines.append(f"Category: {cat.name if cat else 'Not found'}")
        else:
            config_lines.append("Category: Not set")
        
        if log_id:
            ch = interaction.guild.get_channel(int(log_id))
            config_lines.append(f"Log Channel: {ch.mention if ch else 'Not found'}")
        else:
            config_lines.append("Log Channel: Not set")
        
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
                game_lines.append(f"**{g.name}** — {g.player_count}p, {g.queue_type.value}, {ch_str}")
            embed.add_field(name="Games", value="\n".join(game_lines), inline=False)
        else:
            embed.add_field(name="Games", value="No games configured.", inline=False)
        
        view = SettingsView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @cm_group.command(name="admin", description="Open the admin panel (CM Admins)")
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
                match_lines.append(f"Match #{m['match_id']} - {game_name}")
            embed.add_field(name="Active Matches", value="\n".join(match_lines), inline=False)
        else:
            embed.add_field(name="Active Matches", value="No active matches.", inline=False)
        
        view = AdminPanelView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @cm_group.command(name="leaderboard", description="View the leaderboard")
    async def leaderboard_cmd(self, interaction: discord.Interaction):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        
        async def show_leaderboard(inter: discord.Interaction, game_id: int):
            embed = await self.build_leaderboard_embed(inter.guild, game_id, monthly=True)
            view = LeaderboardView(self, game_id, monthly=True)
            await inter.response.send_message(embed=embed, view=view)
        
        if len(games) == 1:
            await show_leaderboard(interaction, games[0].game_id)
        else:
            view = discord.ui.View(timeout=60)
            view.add_item(GameSelectDropdown(games, show_leaderboard))
            await interaction.response.send_message("Select a game:", view=view, ephemeral=True)
    
    @cm_group.command(name="stats", description="View player stats")
    @app_commands.describe(user="The user to view stats for (defaults to yourself)")
    async def stats_cmd(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user
        embed = await self.build_stats_embed(interaction.guild, target)
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="win", description="Report the match winner")
    async def win_cmd(self, interaction: discord.Interaction):
        # Check if in a match channel
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return
        
        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return
        
        game = await DatabaseHelper.get_game(match["game_id"])
        needed = (game.player_count // 2) + 1
        
        # Get existing votes
        votes = await DatabaseHelper.get_win_votes(match["match_id"])
        red_votes = votes.get("red", 0)
        blue_votes = votes.get("blue", 0)
        
        red_role = interaction.guild.get_role(match["red_role_id"])
        blue_role = interaction.guild.get_role(match["blue_role_id"])
        
        embed = discord.Embed(
            title="Who Won?",
            description=f"Cast your vote! ({needed} votes needed)\n\n{red_role.mention} vs {blue_role.mention}",
            color=COLOR_NEUTRAL
        )
        embed.add_field(name="Red Team", value=f"{red_votes} votes", inline=True)
        embed.add_field(name="Blue Team", value=f"{blue_votes} votes", inline=True)
        
        view = WinVoteView(self, match["match_id"])
        await interaction.response.send_message(embed=embed, view=view)

    @app_commands.command(name="abandon", description="Vote to abandon the current match")
    async def abandon_cmd(self, interaction: discord.Interaction):
        # Check if in a match channel
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        needed = (game.player_count // 2) + 1  # Majority needed

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

    ign_group = app_commands.Group(name="ign", description="In-game name commands")

    @ign_group.command(name="set", description="Set your in-game name for a game")
    async def ign_set_cmd(self, interaction: discord.Interaction):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return

        async def show_ign_modal(inter: discord.Interaction, game_id: int):
            game = await DatabaseHelper.get_game(game_id)
            modal = IGNModal(self, game_id, game.name)
            await inter.response.send_modal(modal)

        if len(games) == 1:
            await show_ign_modal(interaction, games[0].game_id)
        else:
            view = discord.ui.View(timeout=60)
            view.add_item(GameSelectDropdown(games, show_ign_modal))
            await interaction.response.send_message("Select a game to set your IGN:", view=view, ephemeral=True)

    @ign_group.command(name="view", description="View your saved in-game names")
    @app_commands.describe(user="The user to view IGNs for (defaults to yourself)")
    async def ign_view_cmd(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user
        igns = await DatabaseHelper.get_player_all_igns(target.id)

        if not igns:
            await interaction.response.send_message(
                f"{'You have' if target == interaction.user else f'{target.display_name} has'} no saved IGNs.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"IGNs for {target.display_name}",
            color=COLOR_NEUTRAL
        )

        for game_id, game_name, ign in igns:
            embed.add_field(name=game_name, value=f"`{ign}`", inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # -------------------------------------------------------------------------
    # EVENT LISTENERS
    # -------------------------------------------------------------------------
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Handle member leaving - remove from queues and delete all stats."""
        # Remove from any active queues
        for queue_id, queue_state in list(self.queues.items()):
            if member.id in queue_state.players:
                del queue_state.players[member.id]

                # Update embed if possible
                game = await DatabaseHelper.get_game(queue_state.game_id)
                if game:
                    channel = member.guild.get_channel(queue_state.channel_id)
                    if channel and queue_state.message_id:
                        try:
                            msg = await channel.fetch_message(queue_state.message_id)
                            embed = await self.create_queue_embed(game, queue_state)
                            await msg.edit(embed=embed)
                        except:
                            pass

        # Delete all player stats (permanently removed from leaderboards)
        await DatabaseHelper.delete_player_stats(member.id)

        # Log the removal
        await self.log_action(
            member.guild,
            f"{member.display_name} ({member.id}) left the server - all stats deleted"
        )
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Handle captain selection via @mentions in draft channels."""
        if message.author.bot:
            return
        
        # Check if this is a draft channel awaiting admin captain selection
        match = await DatabaseHelper.get_match_by_channel(message.channel.id)
        if not match:
            # Check draft channels
            async with aiosqlite.connect(DB_PATH) as db:
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


async def setup(bot: commands.Bot):
    await bot.add_cog(CustomMatch(bot))
