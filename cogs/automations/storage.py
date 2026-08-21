"""Automations database — schema, connection and accessors.

Backed by automations.db in the project root, alongside every other cog
database. Everything the Automations cog persists lives here: the automations
themselves and the durable state the engine needs — counters, delayed actions,
temporary roles, cooldowns and run history.

These tables used to live in utility.db, because automations started life as a
section of the Utility cog. `migrate_from_utility` moves them across once, so
an existing server keeps its automations, its tallies and its half-finished
`wait` steps through the split rather than starting over.

Two rules the rest of the cog depends on:

* **Everything is addressed by a stable id, never a list index.** Two admins
  with a panel open must not edit and delete different rows than the ones they
  picked.

* **Every expiry table is swept by the scheduler but filtered on read.**
  Accessors add `expires_ts > now` themselves, so an expired row the sweeper
  has not reached yet is never observed as live and correctness never depends
  on sweeper latency. Same rule as `cogs/economy/effects.py`.

Connection handling mirrors `cogs/economy/database.py` (WAL, one shared
aiosqlite connection, re-entrant transactions).
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

logger = logging.getLogger('cogs.automations.storage')

ROOT = Path(__file__).resolve().parent.parent.parent

# In a directory of its own rather than alongside the other cog databases, and
# that is a security decision rather than tidiness. The dashboard writes to this
# file directly, and it runs behind `ProtectSystem=strict` with a read-only
# home — so it needs a `ReadWritePaths` grant, and SQLite in WAL mode creates
# two sibling files (`-wal`, `-shm`) which means the grant has to cover the
# *directory*, not just the file. Anything else in that directory would be
# writable by the one process on this machine that is reachable from the
# internet. So the directory holds exactly this database and nothing else.
DB_DIR = ROOT / "data" / "automations"
DB_PATH = str(DB_DIR / "automations.db")

# Where these tables lived before automations became its own cog.
LEGACY_DB_PATH = str(ROOT / "utility.db")
# And where this database itself briefly lived, before it was moved somewhere
# the dashboard sandbox could be pointed at safely.
PREVIOUS_DB_PATH = str(ROOT / "automations.db")

# The master pause. Lives here rather than in cog.py so the panel and the
# dashboard can read it without importing the cog.
KILL_SWITCH_KEY = "automations_enabled"

# Bumped by every write the dashboard or the panel makes, and watched by the
# cog so an automation edited on the website takes effect without a restart.
# A monotonic counter rather than a timestamp: two edits in the same second
# have to be two distinct values, or the second one is never noticed.
REVISION_KEY = "revision"


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- One automation. `graph` is the JSON step tree; see models.py.
CREATE TABLE IF NOT EXISTS automations (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 0,
    dry_run INTEGER NOT NULL DEFAULT 1,     -- new automations start safe
    priority INTEGER NOT NULL DEFAULT 100,
    stop_after INTEGER NOT NULL DEFAULT 0,
    trigger_type TEXT NOT NULL,
    trigger_config TEXT NOT NULL DEFAULT '{}',
    -- The top-level gate: conditions checked before any step runs. Branches
    -- inside the graph have their own, but the common shape is
    -- trigger -> conditions -> actions, and that should not require building
    -- an if/else to express.
    conditions TEXT NOT NULL DEFAULT '{}',
    graph TEXT NOT NULL DEFAULT '{"steps": []}',
    cooldown_s INTEGER NOT NULL DEFAULT 0,
    cooldown_scope TEXT NOT NULL DEFAULT 'user',   -- user | channel | guild
    allow_bots INTEGER NOT NULL DEFAULT 0,
    last_fired_ts INTEGER NOT NULL DEFAULT 0,
    run_count INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER,
    updated_by INTEGER,
    created_ts INTEGER NOT NULL DEFAULT 0,
    updated_ts INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    -- Which ready-made automation this started from, so the setup flow can
    -- still find that template's own prompts on a later visit. Empty for one
    -- built from scratch, and never read by the engine.
    template_key TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_auto_trigger ON automations(trigger_type, enabled);

-- Run history. Pruned on a retention window; the trace is what powers the
-- "why didn't it fire?" screen, so it is kept even for runs that matched
-- nothing.
CREATE TABLE IF NOT EXISTS automation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    automation_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    outcome TEXT NOT NULL,             -- fired | skipped | error | dry_run
    summary TEXT NOT NULL DEFAULT '',
    duration_ms INTEGER NOT NULL DEFAULT 0,
    trace TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_runs_auto ON automation_runs(automation_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_runs_ts ON automation_runs(ts);

-- Persistent counters, the thing a stateless rule engine cannot express:
-- "three strikes within 24 hours". expires_ts of 0 means it never decays.
CREATE TABLE IF NOT EXISTS counters (
    scope TEXT NOT NULL,               -- user | channel | guild
    scope_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    expires_ts INTEGER NOT NULL DEFAULT 0,
    updated_ts INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (scope, scope_id, key)
);

-- The `wait` action, persisted so a restart mid-delay still finishes the job.
CREATE TABLE IF NOT EXISTS pending_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    automation_id TEXT NOT NULL,
    execute_ts INTEGER NOT NULL,
    steps TEXT NOT NULL,               -- the remaining step list
    context TEXT NOT NULL,             -- serialised EventContext ids
    depth INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_pending_due ON pending_actions(execute_ts);

CREATE TABLE IF NOT EXISTS temp_roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL,
    automation_id TEXT,
    granted_ts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_temp_roles_exp ON temp_roles(expires_ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_temp_roles_key ON temp_roles(guild_id, user_id, role_id);

CREATE TABLE IF NOT EXISTS cooldowns (
    automation_id TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    expires_ts INTEGER NOT NULL,
    PRIMARY KEY (automation_id, scope_key)
);
CREATE INDEX IF NOT EXISTS idx_cooldowns_exp ON cooldowns(expires_ts);

-- Who changed what. An automation that starts misbehaving at 3am is a much
-- shorter conversation when the panel can say who last touched it. `source`
-- distinguishes an edit made in Discord from one made on the website.
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    entity TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'discord'
);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit(ts DESC);
"""


# Columns added after the first release. `CREATE TABLE IF NOT EXISTS` does
# nothing to a table that already exists, so anything added to SCHEMA later
# never reaches a running server unless it is also listed here — and the symptom
# is an insert failing in production, not an error at boot.
#
# Module level rather than inside `_migrate` so the two lists can be compared:
# `check_migration` asserts every column here also exists in SCHEMA, which is
# the half of the mistake an existing database cannot reveal.
ADDED_COLUMNS = {
    "automations": [
        ("created_ts", "INTEGER NOT NULL DEFAULT 0"),
        ("conditions", "TEXT NOT NULL DEFAULT '{}'"),
        ("template_key", "TEXT NOT NULL DEFAULT ''"),
    ],
    "audit": [
        ("source", "TEXT NOT NULL DEFAULT 'discord'"),
    ],
}

# Moved wholesale out of utility.db when automations became its own cog. Order
# matters only in that it is the order they are copied in; nothing here has a
# foreign key.
MIGRATED_TABLES = ("automations", "automation_runs", "counters",
                   "pending_actions", "temp_roles", "cooldowns")


def _relocate_from_root(destination: str) -> None:
    """Move a database left in the project root by an earlier build.

    The first version of this cog put automations.db beside every other cog
    database. That turned out to be a place the dashboard could not be granted
    write access to without also granting it the whole tree, so the file moved
    into a directory of its own. Anyone who ran that build for even one boot
    has automations in the old file.

    Renamed rather than copied, and only when the new location does not exist
    yet, so this cannot run twice or silently prefer an empty new file over a
    populated old one. The `-wal` and `-shm` siblings move too: leaving a WAL
    behind loses every transaction that had not been checkpointed.
    """
    target = Path(destination)
    previous = Path(PREVIOUS_DB_PATH)
    if target.exists() or not previous.exists():
        return
    try:
        previous.rename(target)
        for suffix in ("-wal", "-shm"):
            sibling = Path(str(previous) + suffix)
            if sibling.exists():
                sibling.rename(Path(str(target) + suffix))
        logger.info(f"Moved {previous.name} into {target.parent}.")
    except OSError as e:
        logger.error(f"Could not move {previous} to {target}: {e}. "
                     f"Automations will start empty until it is moved by hand.")


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


class AutomationsDB:
    """Single shared aiosqlite connection, mirroring EconomyDB in economy/database.py."""

    def __init__(self, path: str = None):
        # Resolved at call time, not bound as a default: a default argument is
        # captured when the function is defined, so a test that repoints
        # DB_PATH would silently keep writing to the real database.
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
            _relocate_from_root(self.db_path)
            self._db = await aiosqlite.connect(self.db_path)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL;")
            # NORMAL is the right pairing with WAL: commits stop fsyncing and
            # only checkpoints do. A power cut can cost the last transaction
            # but can never corrupt the file — which matters on a Pi.
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            # The dashboard writes to this same file from another process, so
            # a write there must not fail this one instantly. Five seconds is
            # far longer than any write here takes and far shorter than a user
            # would notice.
            await self._db.execute("PRAGMA busy_timeout=5000;")
            await self._db.executescript(SCHEMA)
            await self._migrate()
            await self._db.commit()
            logger.info("Automations database connected (WAL, synchronous=NORMAL).")

    async def _migrate(self):
        """Additive column migrations, safe to re-run.

        `CREATE TABLE IF NOT EXISTS` does nothing to a table that already
        exists, so a column added to SCHEMA after the first boot never reaches
        an existing database — the table simply stays one version behind until
        something inserts into it and fails. Every future column belongs here
        as well as in SCHEMA.
        """
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
            raise RuntimeError("AutomationsDB used before connect()")
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

    async def get_bool(self, key: str, default: bool = False) -> bool:
        raw = await self.get_setting(key, "")
        return default if raw == "" else raw == "1"

    async def set_bool(self, key: str, value: bool):
        await self.set_setting(key, "1" if value else "0")

    # ------------------------------------------------------------- revision

    async def revision(self) -> int:
        try:
            return int(await self.get_setting(REVISION_KEY, "0") or 0)
        except (TypeError, ValueError):
            return 0

    async def bump_revision(self) -> int:
        """Announce that something changed, for anyone watching this file.

        Done as one SQL statement rather than read-then-write so a dashboard
        write landing between the two cannot be lost.
        """
        await self.execute(
            "INSERT INTO settings (key, value) VALUES (?, '1') "
            "ON CONFLICT(key) DO UPDATE SET "
            "value = CAST(CAST(settings.value AS INTEGER) + 1 AS TEXT)",
            (REVISION_KEY,),
        )
        return await self.revision()

    # ------------------------------------------------------- generic rows

    async def get_row(self, table: str, row_id: str) -> typing.Optional[aiosqlite.Row]:
        return await self.fetchone(f"SELECT * FROM {table} WHERE id = ?", (row_id,))

    async def list_rows(self, table: str, guild_id: int) -> list:
        return await self.fetchall(
            f"SELECT * FROM {table} WHERE guild_id = ? ORDER BY rowid", (guild_id,)
        )

    async def delete_row(self, table: str, row_id: str):
        await self.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))

    async def update_row(self, table: str, row_id: str, **fields):
        """Update named columns on one row. Column names are code-supplied."""
        if not fields:
            return
        assignments = ", ".join(f"{k} = ?" for k in fields)
        await self.execute(
            f"UPDATE {table} SET {assignments} WHERE id = ?",
            (*fields.values(), row_id),
        )

    async def insert_row(self, table: str, **fields) -> str:
        row_id = fields.pop("id", None) or new_id()
        fields.setdefault("created_ts", now())
        columns = ", ".join(["id", *fields])
        markers = ", ".join("?" * (len(fields) + 1))
        await self.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({markers})",
            (row_id, *fields.values()),
        )
        return row_id

    # ------------------------------------------------------------- counters

    async def counter_get(self, scope: str, scope_id: int, key: str) -> int:
        row = await self.fetchone(
            "SELECT value FROM counters WHERE scope = ? AND scope_id = ? AND key = ? "
            "AND (expires_ts = 0 OR expires_ts > ?)",
            (scope, scope_id, key, now()),
        )
        return row["value"] if row else 0

    async def counter_add(self, scope: str, scope_id: int, key: str,
                          delta: int, ttl_s: int = 0) -> int:
        """Add to a counter, resetting it first if its window already lapsed.

        Returns the new value. The read-then-write is done in one transaction
        so two events in the same tick can't both read the pre-increment value.
        """
        async with self.transaction():
            row = await self.fetchone(
                "SELECT value, expires_ts FROM counters "
                "WHERE scope = ? AND scope_id = ? AND key = ?",
                (scope, scope_id, key),
            )
            ts = now()
            lapsed = row is not None and row["expires_ts"] != 0 and row["expires_ts"] <= ts
            base = 0 if (row is None or lapsed) else row["value"]
            value = base + delta
            # A fresh window starts from this write, not from the lapsed one.
            expires = (ts + ttl_s) if ttl_s > 0 else (0 if row is None or lapsed else row["expires_ts"])
            await self.execute(
                "INSERT INTO counters (scope, scope_id, key, value, expires_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(scope, scope_id, key) DO UPDATE SET "
                "value = excluded.value, expires_ts = excluded.expires_ts, "
                "updated_ts = excluded.updated_ts",
                (scope, scope_id, key, value, expires, ts),
                commit=False,
            )
        return value

    async def counter_reset(self, scope: str, scope_id: int, key: str):
        await self.execute(
            "DELETE FROM counters WHERE scope = ? AND scope_id = ? AND key = ?",
            (scope, scope_id, key),
        )

    # ------------------------------------------------------------ cooldowns

    async def cooldown_active(self, automation_id: str, scope_key: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM cooldowns WHERE automation_id = ? AND scope_key = ? "
            "AND expires_ts > ?",
            (automation_id, scope_key, now()),
        )
        return row is not None

    async def cooldown_set(self, automation_id: str, scope_key: str, seconds: int):
        await self.execute(
            "INSERT INTO cooldowns (automation_id, scope_key, expires_ts) VALUES (?, ?, ?) "
            "ON CONFLICT(automation_id, scope_key) DO UPDATE SET expires_ts = excluded.expires_ts",
            (automation_id, scope_key, now() + seconds),
        )

    # ----------------------------------------------------------- temp roles

    async def temp_role_add(self, guild_id: int, user_id: int, role_id: int,
                            expires_ts: int, automation_id: str = None):
        await self.execute(
            "INSERT INTO temp_roles (guild_id, user_id, role_id, expires_ts, "
            "automation_id, granted_ts) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(guild_id, user_id, role_id) DO UPDATE SET "
            "expires_ts = excluded.expires_ts",
            (guild_id, user_id, role_id, expires_ts, automation_id, now()),
        )

    async def temp_roles_due(self) -> list:
        return await self.fetchall(
            "SELECT * FROM temp_roles WHERE expires_ts <= ?", (now(),)
        )

    async def temp_role_clear(self, row_id: int):
        await self.execute("DELETE FROM temp_roles WHERE id = ?", (row_id,))

    # -------------------------------------------------------------- audit

    async def audit_log(self, user_id: int, entity: str, entity_id: str,
                        action: str, detail: str = "", source: str = "discord"):
        await self.execute(
            "INSERT INTO audit (ts, user_id, entity, entity_id, action, detail, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (now(), user_id, entity, entity_id, action, detail[:500], source),
        )

    # --------------------------------------------------------------- runs

    async def record_run(self, automation_id: str, outcome: str, summary: str,
                         duration_ms: int, trace: list):
        await self.execute(
            "INSERT INTO automation_runs (automation_id, ts, outcome, summary, "
            "duration_ms, trace) VALUES (?, ?, ?, ?, ?, ?)",
            (automation_id, now(), outcome, summary[:400], duration_ms,
             json.dumps(trace)[:20000]),
        )

    async def prune_runs(self, retain_days: int = 14, keep_per_automation: int = 200):
        """Trim history two ways: an age cap, and a per-automation depth cap.

        The age cap alone lets one chatty automation bury every other one's
        history; the depth cap alone lets a quiet automation keep rows forever.
        """
        cutoff = now() - retain_days * 86400
        await self.execute("DELETE FROM automation_runs WHERE ts < ?", (cutoff,))
        await self.execute(
            "DELETE FROM automation_runs WHERE id IN ("
            "  SELECT id FROM ("
            "    SELECT id, ROW_NUMBER() OVER ("
            "      PARTITION BY automation_id ORDER BY ts DESC) AS rn"
            "    FROM automation_runs"
            "  ) WHERE rn > ?"
            ")",
            (keep_per_automation,),
        )


# --------------------------------------------------------------- migration

async def migrate_from_utility(db: AutomationsDB, legacy_path: str = None) -> int:
    """Move the automation tables out of utility.db, once.

    Automations were a section of the Utility cog before they were a cog, so
    an existing server has its automations, tallies, cooldowns and pending
    `wait` steps in the wrong file. Copying rather than re-creating is the
    point: a half-finished `wait` and a running strike count are live state,
    and losing them at upgrade time would time nobody out and reset every
    tally to zero silently.

    Once copied, each source table is renamed to `moved_<name>` rather than
    dropped. That is what makes this run exactly once — the next boot finds no
    `automations` table to read — and it leaves the old rows in place for as
    long as anyone might want to look at them.
    """
    legacy_path = legacy_path or LEGACY_DB_PATH
    if not Path(legacy_path).exists():
        return 0

    moved = 0
    async with aiosqlite.connect(legacy_path) as legacy:
        legacy.row_factory = aiosqlite.Row

        async def table_exists(name: str) -> bool:
            async with legacy.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ) as cursor:
                return await cursor.fetchone() is not None

        present = [t for t in MIGRATED_TABLES if await table_exists(t)]
        if not present:
            return 0

        for table in present:
            async with legacy.execute(f"SELECT * FROM {table}") as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                data = dict(row)
                # AUTOINCREMENT ids are local to their file; letting SQLite
                # assign fresh ones avoids colliding with anything already
                # written here.
                if table in ("automation_runs", "pending_actions", "temp_roles"):
                    data.pop("id", None)
                columns = ", ".join(data)
                markers = ", ".join("?" * len(data))
                try:
                    await db.execute(
                        f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({markers})",
                        tuple(data.values()), commit=False,
                    )
                    moved += 1
                except Exception as e:
                    logger.error(f"Could not move a {table} row across: {e}")

        # The audit trail is shared with the Utility cog's own rules, so only
        # the automation entries come across.
        if await table_exists("audit"):
            async with legacy.execute(
                "SELECT ts, user_id, entity, entity_id, action, detail FROM audit "
                "WHERE entity IN ('automations', 'automation')"
            ) as cursor:
                for row in await cursor.fetchall():
                    await db.execute(
                        "INSERT INTO audit (ts, user_id, entity, entity_id, action, "
                        "detail, source) VALUES (?, ?, ?, ?, ?, ?, 'discord')",
                        tuple(row), commit=False,
                    )
                    moved += 1

        # The kill switch is a setting rather than a table, and losing it would
        # silently re-arm every automation on a server that had paused them.
        async with legacy.execute(
            "SELECT value FROM settings WHERE key = ?", (KILL_SWITCH_KEY,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is not None:
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (KILL_SWITCH_KEY, row["value"]), commit=False,
            )

        await db.db.commit()

        # Only now that everything is committed here. Renaming first would put
        # a crash between the two steps and lose the lot.
        for table in present:
            await legacy.execute(f"ALTER TABLE {table} RENAME TO moved_{table}")
        await legacy.execute(
            "DELETE FROM audit WHERE entity IN ('automations', 'automation')")
        await legacy.commit()

    logger.info(f"Moved {moved} row(s) from utility.db into automations.db. "
                f"The old tables are still there, renamed to moved_*.")
    return moved
