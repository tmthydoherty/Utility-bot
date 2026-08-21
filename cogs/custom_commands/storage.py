"""Custom Commands database — schema, connection and accessors.

Backed by ``data/custom_commands/custom_commands.db``, in a directory of its
own for exactly the reason ``cogs/utility/storage.py`` explains: the dashboard
writes to this file from another process, under ``ProtectSystem=strict`` with a
read-only home, so it needs a ``ReadWritePaths`` grant — and a WAL database is
three files, so the grant covers the *directory*. Nothing else lives there.

The dashboard writes commands (and GIF-moderation intents) and bumps
``settings['revision']``; the cog polls that integer and reloads its caches when
it moves. No socket, no endpoint — the same bridge the Utility and Automations
builders use.

Two tables carry real weight:

* **commands** — the admin-authored ``!`` commands and keyword responders. A row
  is addressed by a stable id, never a list index, so two admins editing at once
  never clobber each other's row.
* **gif_moderation** — a small queue. The economy-purchased GIF commands live in
  ``economy.db``, which the dashboard sandbox cannot write. So when a moderator
  disables or deletes one from the website, the dashboard records the *intent*
  here and the cog — which can write ``economy.db`` through the Economy cog —
  applies it and clears the row.

Connection handling mirrors ``cogs/utility/storage.py`` (WAL, one shared
aiosqlite connection, re-entrant per-task transactions).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import typing
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

logger = logging.getLogger('cogs.custom_commands.storage')

ROOT = Path(__file__).resolve().parent.parent.parent
DB_DIR = ROOT / "data" / "custom_commands"
DB_PATH = str(DB_DIR / "custom_commands.db")

# Bumped by every write the dashboard makes and watched by the cog so a command
# edited on the website takes effect without a restart. A monotonic counter
# rather than a timestamp: two edits in the same second must be two distinct
# values, or the second is never noticed.
REVISION_KEY = "revision"


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Admin-authored custom commands.
--
-- `name` is the bare trigger word, lowercased, with no `!` — the display list
-- puts the `!` back for exact commands. `match_type` decides how it fires:
--   exact      — `!name` (the GIF-command style; the everyday case).
--   startswith — a message that begins with `name` (no prefix needed).
--   contains   — a message that contains `name` anywhere.
--
-- `responses_json` is a list of strings; one is chosen at random each time, so a
-- command can have several answers. `embed_json` is an optional full embed
-- (same shape as the Utility builder). `delivery` is where the reply lands.
CREATE TABLE IF NOT EXISTS commands (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    match_type TEXT NOT NULL DEFAULT 'exact',     -- exact | startswith | contains
    responses_json TEXT NOT NULL DEFAULT '[]',
    plain_text INTEGER NOT NULL DEFAULT 1,        -- 0 = send the embed instead
    embed_json TEXT,
    delivery TEXT NOT NULL DEFAULT 'channel',     -- channel | reply | dm
    delete_trigger INTEGER NOT NULL DEFAULT 0,
    react_emoji TEXT NOT NULL DEFAULT '',
    allowed_role_ids TEXT NOT NULL DEFAULT '[]',
    denied_role_ids TEXT NOT NULL DEFAULT '[]',
    allowed_channel_ids TEXT NOT NULL DEFAULT '[]',
    denied_channel_ids TEXT NOT NULL DEFAULT '[]',
    cooldown_s INTEGER NOT NULL DEFAULT 0,
    cooldown_scope TEXT NOT NULL DEFAULT 'user',  -- user | channel | guild
    use_count INTEGER NOT NULL DEFAULT 0,
    last_used_ts INTEGER NOT NULL DEFAULT 0,
    created_ts INTEGER NOT NULL DEFAULT 0,
    updated_ts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cc_name ON commands(guild_id, name);
CREATE INDEX IF NOT EXISTS idx_cc_guild ON commands(guild_id, enabled);

-- Moderation intents for economy-purchased GIF commands. The dashboard cannot
-- write economy.db (it is outside the sandbox's writable paths), so it records
-- what a moderator wants done here and the cog applies it, then clears the row.
CREATE TABLE IF NOT EXISTS gif_moderation (
    name TEXT PRIMARY KEY,
    action TEXT NOT NULL,                         -- disable | enable | delete
    updated_ts INTEGER NOT NULL DEFAULT 0,
    updated_by INTEGER NOT NULL DEFAULT 0
);

-- Who changed what. A command that starts misbehaving is a much shorter
-- conversation when the panel can say who last touched it.
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    entity TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_cc_audit_ts ON audit(ts DESC);
"""


# Columns added after the first release. `CREATE TABLE IF NOT EXISTS` does
# nothing to a table that already exists, so a column added to SCHEMA later
# never reaches a running server unless it is also listed here — and the symptom
# is an insert failing in production, not an error at boot.
ADDED_COLUMNS: dict[str, list[tuple[str, str]]] = {}


def new_id() -> str:
    return uuid.uuid4().hex[:12]


def now() -> int:
    return int(time.time())


def loads(raw: typing.Optional[str], default):
    """JSON columns, tolerant of a hand-edited database."""
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return default


class CustomCommandsDB:
    """Single shared aiosqlite connection, mirroring UtilityDB / EconomyDB."""

    def __init__(self, path: str = None):
        # Resolved at call time, not bound as a default: a default argument is
        # captured when the function is defined, so a test that repoints DB_PATH
        # would silently keep writing to the real database.
        self.db_path = path or DB_PATH
        self._db: typing.Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._tx_owner = None
        self._tx_depth = 0

    # ---------------------------------------------------------------- setup

    async def connect(self):
        async with self._lock:
            if self._db:
                return
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(self.db_path)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL;")
            # NORMAL is the right pairing with WAL on a Pi: commits stop fsyncing
            # and only checkpoints do. A power cut can cost the last transaction
            # but can never corrupt the file.
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            # The dashboard writes this same file from another process, so a
            # write there must not fail this one instantly.
            await self._db.execute("PRAGMA busy_timeout=5000;")
            await self._db.executescript(SCHEMA)
            await self._migrate()
            await self._db.commit()
            logger.info("Custom Commands database connected (WAL, synchronous=NORMAL).")

    async def _migrate(self):
        """Additive column migrations, safe to re-run."""
        for table, columns in ADDED_COLUMNS.items():
            cursor = await self._db.execute(f"PRAGMA table_info({table})")
            existing = {row["name"] for row in await cursor.fetchall()}
            if not existing:
                continue  # table not created yet; SCHEMA will have it right
            for name, spec in columns:
                if name not in existing:
                    await self._db.execute(
                        f"ALTER TABLE {table} ADD COLUMN {name} {spec}")
                    logger.info(f"Migrated: added {table}.{name}")

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("CustomCommandsDB used before connect()")
        return self._db

    def _in_transaction(self) -> bool:
        return self._tx_owner is asyncio.current_task() and self._tx_depth > 0

    @asynccontextmanager
    async def transaction(self):
        """Serialise a multi-statement operation and commit it once.

        Re-entrant per task: nested uses join the outer transaction and only
        the outermost exit commits.
        """
        task = asyncio.current_task()
        if self._tx_owner is task:
            self._tx_depth += 1
            try:
                yield
            finally:
                self._tx_depth -= 1
            return

        await self._write_lock.acquire()
        self._tx_owner = task
        self._tx_depth = 1
        try:
            yield
            await self.db.commit()
        except BaseException:
            try:
                await self.db.rollback()
            except Exception as e:
                logger.error(f"rollback failed: {e}")
            raise
        finally:
            self._tx_depth = 0
            self._tx_owner = None
            self._write_lock.release()

    # ------------------------------------------------------------ raw query

    async def fetchone(self, query: str, params: tuple = ()) -> typing.Optional[aiosqlite.Row]:
        async with self.db.execute(query, params) as cursor:
            return await cursor.fetchone()

    async def fetchall(self, query: str, params: tuple = ()) -> list:
        async with self.db.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def execute(self, query: str, params: tuple = (), *, commit: bool = True):
        cursor = await self.db.execute(query, params)
        if commit and not self._in_transaction():
            await self.db.commit()
        return cursor

    # ------------------------------------------------------------- settings

    async def get_setting(self, key: str, default: str = "") -> str:
        row = await self.fetchone("SELECT value FROM settings WHERE key = ?", (key,))
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str):
        await self.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    async def get_revision(self) -> int:
        raw = await self.get_setting(REVISION_KEY, "0")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    # ------------------------------------------------------------- commands

    async def all_commands(self) -> list:
        return await self.fetchall("SELECT * FROM commands ORDER BY name ASC")

    async def bump_use(self, command_id: str):
        """Record one use. Written straight out — this never bumps the revision,
        so it does not trip the dashboard-sync reload."""
        await self.execute(
            "UPDATE commands SET use_count = use_count + 1, last_used_ts = ? "
            "WHERE id = ?",
            (now(), command_id),
        )

    # ---------------------------------------------------- gif moderation

    async def pending_gif_moderation(self) -> list:
        return await self.fetchall("SELECT * FROM gif_moderation")

    async def clear_gif_moderation(self, name: str):
        await self.execute("DELETE FROM gif_moderation WHERE name = ?", (name,))
