"""Economy database layer — schema, connection management and accessors.

Backed by economy.db in the project root. Everything the Economy cog persists
lives here: levels, Points balances, the ledger, inventory, timed effects and
the approval queue.
"""

import aiosqlite
import asyncio
import json
import logging
import time
import typing
from contextlib import asynccontextmanager
from pathlib import Path

logger = logging.getLogger('cogs.economy.database')

# economy.db sits in the project root, alongside every other cog database.
DB_PATH = str(Path(__file__).resolve().parent.parent.parent / "economy.db")

# tracker.py's database — read-only here. It already logs every message and
# voice session, so the level card reads history from it rather than
# duplicating the tracking.
TRACKING_DB_PATH = str(Path(__file__).resolve().parent.parent.parent / "tracking_data.db")


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    xp INTEGER NOT NULL DEFAULT 0,
    level INTEGER NOT NULL DEFAULT 0,
    points INTEGER NOT NULL DEFAULT 0,
    lifetime_points INTEGER NOT NULL DEFAULT 0,
    messages INTEGER NOT NULL DEFAULT 0,
    voice_seconds INTEGER NOT NULL DEFAULT 0,
    last_msg_xp_ts INTEGER NOT NULL DEFAULT 0,
    first_seen_ts INTEGER NOT NULL DEFAULT 0
);

-- One row per user per day. Every daily cap is enforced against this table,
-- which makes caps survive restarts for free.
CREATE TABLE IF NOT EXISTS daily_activity (
    user_id INTEGER NOT NULL,
    day TEXT NOT NULL,
    msg_points INTEGER NOT NULL DEFAULT 0,
    newcomer_points INTEGER NOT NULL DEFAULT 0,
    claimed TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (user_id, day)
);

-- Once-per-day claims. A composite primary key makes "claim this flag" a single
-- INSERT OR IGNORE whose rowcount is the answer — the JSON blob this replaced
-- was a read-modify-write and double-paid under concurrency.
CREATE TABLE IF NOT EXISTS daily_claims (
    user_id INTEGER NOT NULL,
    day TEXT NOT NULL,
    flag TEXT NOT NULL,
    ts INTEGER NOT NULL,
    PRIMARY KEY (user_id, day, flag)
);

CREATE TABLE IF NOT EXISTS streaks (
    user_id INTEGER PRIMARY KEY,
    current_streak INTEGER NOT NULL DEFAULT 0,
    longest_streak INTEGER NOT NULL DEFAULT 0,
    last_active_day TEXT,
    last_bonus_day TEXT
);

CREATE TABLE IF NOT EXISTS voice_accum (
    user_id INTEGER PRIMARY KEY,
    pending_seconds INTEGER NOT NULL DEFAULT 0,
    last_tick_ts INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS points_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    delta INTEGER NOT NULL,
    source TEXT NOT NULL,
    meta TEXT,
    ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_user ON points_ledger(user_id, ts);

-- state: owned | active | consumed | expired | refunded
CREATE TABLE IF NOT EXISTS inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    item_key TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'owned',
    price_paid INTEGER NOT NULL DEFAULT 0,
    purchased_ts INTEGER NOT NULL,
    expires_ts INTEGER,
    activated_ts INTEGER,
    payload TEXT NOT NULL DEFAULT '{}',
    reminded TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_inventory_user ON inventory(user_id, state);

CREATE TABLE IF NOT EXISTS active_effects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    effect_key TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}',
    started_ts INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL,
    inventory_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_effects_target ON active_effects(target_id, effect_key);

-- kind: gif | emoji | emoji_permanent    status: pending | approved | denied
CREATE TABLE IF NOT EXISTS approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    channel_id INTEGER,
    message_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    price_paid INTEGER NOT NULL DEFAULT 0,
    inventory_id INTEGER,
    decided_by INTEGER,
    decided_ts INTEGER,
    created_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS gif_commands (
    name TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    approved_ts INTEGER NOT NULL
);

-- status: pending | trial | permanent | removed
CREATE TABLE IF NOT EXISTS shop_emojis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    emoji_id INTEGER,
    name TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    added_ts INTEGER NOT NULL,
    expires_ts INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS emoji_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    emoji_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    usage_type TEXT NOT NULL,
    ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_emoji_usage ON emoji_usage(emoji_id, ts);

CREATE TABLE IF NOT EXISTS throne_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    price_paid INTEGER NOT NULL,
    ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS vibes_role (
    user_id INTEGER PRIMARY KEY,
    claimed_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS proxy_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    message_id INTEGER,
    content TEXT,
    ts INTEGER NOT NULL
);

-- Temporary permission grants (HoF Post). Persisted so a restart mid-grant
-- still revokes the overwrite instead of leaving it in place forever.
CREATE TABLE IF NOT EXISTS perm_grants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    granted_ts INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL,
    inventory_id INTEGER,
    prior_state TEXT
);

-- Custom match queues opened by a shop purchase, tracked so they can be
-- closed once the match starts or the 3h window elapses.
CREATE TABLE IF NOT EXISTS match_unlocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    queue_id INTEGER,
    game_id INTEGER,
    channel_id INTEGER,
    opened_ts INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL,
    closed INTEGER NOT NULL DEFAULT 0
);
"""


class EconomyDB:
    """Single shared aiosqlite connection, mirroring TrackingDB in tracker.py."""

    def __init__(self):
        self.db_path = DB_PATH
        self._db: typing.Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        # Serialises multi-statement logical operations. Without it, one
        # coroutine's commit() lands in the middle of another's sequence.
        self._write_lock = asyncio.Lock()
        self._tx_owner = None
        self._tx_depth = 0

    # ---------------------------------------------------------------- setup

    async def connect(self):
        async with self._lock:
            if self._db:
                return
            self._db = await aiosqlite.connect(self.db_path)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL;")
            # NORMAL is the correct pairing with WAL: commits stop fsyncing and
            # only checkpoints do. A power cut can cost the last transaction but
            # can never corrupt the file. This matters here because the database
            # lives on a USB-attached SSD, where flushes are expensive.
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            await self._db.execute("PRAGMA foreign_keys=ON;")
            await self._db.executescript(SCHEMA)
            await self._migrate()
            await self._db.commit()
            logger.info("Economy database connected (WAL, synchronous=NORMAL).")

    # ---------------------------------------------------------- transactions

    @asynccontextmanager
    async def transaction(self):
        """Serialise a multi-statement operation and commit it once.

        Re-entrant per task: nested uses join the outer transaction and only
        the outermost exit commits, so callers can compose freely without
        either deadlocking or committing half a change.
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

    async def _stmt(self, query: str, params: tuple = ()):
        """Execute inside an open transaction — never commits on its own."""
        return await self.db.execute(query, params)

    async def _migrate(self):
        """Additive column migrations, safe to re-run."""
        additions = {
            "users": [
                ("messages", "INTEGER NOT NULL DEFAULT 0"),
                ("voice_seconds", "INTEGER NOT NULL DEFAULT 0"),
            ],
            "inventory": [
                ("reminded", "TEXT NOT NULL DEFAULT '[]'"),
                ("activated_ts", "INTEGER"),
            ],
            "perm_grants": [
                ("prior_state", "TEXT"),
            ],
        }
        for table, columns in additions.items():
            cursor = await self._db.execute(f"PRAGMA table_info({table})")
            existing = {row["name"] for row in await cursor.fetchall()}
            for name, spec in columns:
                if name not in existing:
                    await self._db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {spec}")
                    logger.info(f"Migrated: added {table}.{name}")

        await self._backfill_daily_claims()

    async def _backfill_daily_claims(self):
        """Move the old daily_activity.claimed JSON blobs into daily_claims.

        Runs once; afterwards the blob is left alone as dead weight rather than
        dropped, so a rollback to the previous build still works.
        """
        cursor = await self._db.execute(
            "SELECT COUNT(*) AS c FROM daily_claims"
        )
        if (await cursor.fetchone())["c"]:
            return

        cursor = await self._db.execute(
            "SELECT user_id, day, claimed FROM daily_activity "
            "WHERE claimed IS NOT NULL AND claimed NOT IN ('', '[]')"
        )
        moved = 0
        for row in await cursor.fetchall():
            try:
                flags = json.loads(row["claimed"] or "[]")
            except (TypeError, ValueError):
                continue
            for flag in flags:
                await self._db.execute(
                    "INSERT OR IGNORE INTO daily_claims (user_id, day, flag, ts) "
                    "VALUES (?, ?, ?, ?)",
                    (row["user_id"], row["day"], str(flag), int(time.time())),
                )
                moved += 1
        if moved:
            logger.info(f"Migrated {moved} daily claim(s) into daily_claims.")

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("EconomyDB used before connect()")
        return self._db

    async def commit(self):
        await self.db.commit()

    # ------------------------------------------------------------ raw query

    async def fetchone(self, query: str, params: tuple = ()) -> typing.Optional[aiosqlite.Row]:
        async with self.db.execute(query, params) as cursor:
            return await cursor.fetchone()

    async def fetchall(self, query: str, params: tuple = ()) -> list:
        async with self.db.execute(query, params) as cursor:
            return await cursor.fetchall()

    async def execute(self, query: str, params: tuple = (), *, commit: bool = True):
        """Execute a statement, committing unless a transaction is open.

        Callers inside `transaction()` do not need to know they are — the
        commit is deferred to the transaction's exit automatically, so a
        helper can be reused both standalone and composed.
        """
        cursor = await self.db.execute(query, params)
        if commit and not self._in_transaction():
            await self.db.commit()
        return cursor

    def _in_transaction(self) -> bool:
        return self._tx_owner is asyncio.current_task() and self._tx_depth > 0

    # ------------------------------------------------------------- settings

    async def get_setting(self, key: str) -> typing.Optional[str]:
        row = await self.fetchone("SELECT value FROM settings WHERE key = ?", (key,))
        return row["value"] if row else None

    async def set_setting(self, key: str, value):
        await self.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    async def all_settings(self) -> dict:
        rows = await self.fetchall("SELECT key, value FROM settings")
        return {row["key"]: row["value"] for row in rows}

    # ---------------------------------------------------------------- users

    async def ensure_user(self, user_id: int):
        await self.execute(
            "INSERT OR IGNORE INTO users (user_id, first_seen_ts) VALUES (?, ?)",
            (user_id, int(time.time())),
        )

    async def get_user(self, user_id: int) -> aiosqlite.Row:
        await self.ensure_user(user_id)
        return await self.fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))

    async def get_balance(self, user_id: int) -> int:
        row = await self.fetchone("SELECT points FROM users WHERE user_id = ?", (user_id,))
        return row["points"] if row else 0

    async def _ledger(self, user_id: int, delta: int, source: str, meta: dict = None):
        await self._stmt(
            "INSERT INTO points_ledger (user_id, delta, source, meta, ts) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, delta, source, json.dumps(meta or {}), int(time.time())),
        )

    async def credit(self, user_id: int, amount: int, source: str,
                     meta: dict = None) -> bool:
        """Add Points and record the ledger entry. Two statements, one commit.

        The UPSERT removes the separate ensure_user round trip.
        """
        if amount <= 0:
            return False
        async with self.transaction():
            await self._stmt(
                "INSERT INTO users (user_id, points, lifetime_points, first_seen_ts) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "points = points + excluded.points, "
                "lifetime_points = lifetime_points + excluded.lifetime_points",
                (user_id, amount, amount, int(time.time())),
            )
            await self._ledger(user_id, amount, source, meta)
        return True

    async def try_debit(self, user_id: int, cost: int, source: str,
                        meta: dict = None) -> bool:
        """Atomically spend Points. Returns False if unaffordable.

        The balance check lives *inside* the UPDATE, so there is no window
        between deciding and deducting — the previous read-then-write let a
        double-click buy several items for the price of one.
        """
        if cost <= 0:
            return True
        async with self.transaction():
            cursor = await self._stmt(
                "UPDATE users SET points = points - ? "
                "WHERE user_id = ? AND points >= ?",
                (cost, user_id, cost),
            )
            if cursor.rowcount != 1:
                return False
            await self._ledger(user_id, -cost, source, meta)
        return True

    async def adjust_points(self, user_id: int, delta: int, source: str,
                            meta: dict = None) -> int:
        """Compatibility wrapper for admin adjustments and payouts.

        Positive deltas credit. Negative deltas debit as far as the balance
        allows (an admin removing more than someone holds zeroes them out
        rather than failing).
        """
        if delta >= 0:
            await self.credit(user_id, delta, source, meta)
            return await self.get_balance(user_id)

        await self.ensure_user(user_id)
        async with self.transaction():
            cursor = await self._stmt(
                "UPDATE users SET points = MAX(0, points + ?) WHERE user_id = ? "
                "RETURNING points",
                (delta, user_id),
            )
            row = await cursor.fetchone()
            await self._ledger(user_id, delta, source, meta)
        return row["points"] if row else 0

    # ------------------------------------------------------------ inventory

    async def add_inventory(self, user_id: int, item_key: str, price_paid: int,
                            *, expires_ts: int = None, payload: dict = None) -> int:
        cursor = await self.execute(
            "INSERT INTO inventory (user_id, item_key, state, price_paid, purchased_ts, "
            "expires_ts, payload) VALUES (?, ?, 'owned', ?, ?, ?, ?)",
            (user_id, item_key, price_paid, int(time.time()), expires_ts,
             json.dumps(payload or {})),
        )
        return cursor.lastrowid

    async def get_inventory(self, user_id: int, states: tuple = ('owned', 'active')) -> list:
        placeholders = ",".join("?" for _ in states)
        return await self.fetchall(
            f"SELECT * FROM inventory WHERE user_id = ? AND state IN ({placeholders}) "
            "ORDER BY purchased_ts ASC",
            (user_id, *states),
        )

    async def get_inventory_item(self, inv_id: int) -> typing.Optional[aiosqlite.Row]:
        return await self.fetchone("SELECT * FROM inventory WHERE id = ?", (inv_id,))

    async def set_inventory_state(self, inv_id: int, state: str, *, payload: dict = None,
                                  expires_ts: int = None):
        sets, params = ["state = ?"], [state]
        if state == 'active':
            sets.append("activated_ts = ?")
            params.append(int(time.time()))
        if payload is not None:
            sets.append("payload = ?")
            params.append(json.dumps(payload))
        if expires_ts is not None:
            sets.append("expires_ts = ?")
            params.append(expires_ts)
        params.append(inv_id)
        await self.execute(f"UPDATE inventory SET {', '.join(sets)} WHERE id = ?", tuple(params))

    # -------------------------------------------------------------- effects

    async def add_effect(self, owner_id: int, target_id: int, effect_key: str,
                         expires_ts: int, *, data: dict = None,
                         inventory_id: int = None) -> int:
        cursor = await self.execute(
            "INSERT INTO active_effects (owner_id, target_id, effect_key, data, started_ts, "
            "expires_ts, inventory_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (owner_id, target_id, effect_key, json.dumps(data or {}), int(time.time()),
             expires_ts, inventory_id),
        )
        return cursor.lastrowid

    async def get_effects(self, target_id: int = None, effect_key: str = None) -> list:
        clauses, params = ["expires_ts > ?"], [int(time.time())]
        if target_id is not None:
            clauses.append("target_id = ?")
            params.append(target_id)
        if effect_key is not None:
            clauses.append("effect_key = ?")
            params.append(effect_key)
        return await self.fetchall(
            f"SELECT * FROM active_effects WHERE {' AND '.join(clauses)}", tuple(params)
        )

    async def remove_effect(self, effect_id: int):
        await self.execute("DELETE FROM active_effects WHERE id = ?", (effect_id,))

    # ---------------------------------------------------------------- daily

    async def get_daily(self, user_id: int, day: str) -> aiosqlite.Row:
        await self.execute(
            "INSERT OR IGNORE INTO daily_activity (user_id, day) VALUES (?, ?)",
            (user_id, day),
        )
        return await self.fetchone(
            "SELECT * FROM daily_activity WHERE user_id = ? AND day = ?", (user_id, day)
        )

    async def claim_daily_flag(self, user_id: int, day: str, flag: str) -> bool:
        """Claim a once-per-day flag. Returns False if it was already claimed.

        A single INSERT OR IGNORE against a composite primary key — the whole
        decision is the statement, so concurrent callers cannot both win.
        """
        cursor = await self.execute(
            "INSERT OR IGNORE INTO daily_claims (user_id, day, flag, ts) "
            "VALUES (?, ?, ?, ?)",
            (user_id, day, flag, int(time.time())),
        )
        return cursor.rowcount == 1

    async def grant_capped(self, user_id: int, day: str, column: str,
                           amount: int, cap: int) -> int:
        """Add to a capped daily counter, returning what was actually granted.

        Held under the write lock so the read and the write cannot interleave;
        a cap of 0 means uncapped.
        """
        if amount <= 0:
            return 0
        async with self.transaction():
            cursor = await self._stmt(
                f"SELECT {column} AS used FROM daily_activity "
                "WHERE user_id = ? AND day = ?",
                (user_id, day),
            )
            row = await cursor.fetchone()
            used = row["used"] if row else 0

            granted = amount if cap <= 0 else max(0, min(amount, cap - used))
            if granted <= 0:
                return 0

            if row is None:
                await self._stmt(
                    f"INSERT INTO daily_activity (user_id, day, {column}) "
                    "VALUES (?, ?, ?) "
                    f"ON CONFLICT(user_id, day) DO UPDATE SET {column} = {column} + ?",
                    (user_id, day, granted, granted),
                )
            else:
                await self._stmt(
                    f"UPDATE daily_activity SET {column} = {column} + ? "
                    "WHERE user_id = ? AND day = ?",
                    (granted, user_id, day),
                )
        return granted

    async def daily_used(self, user_id: int, day: str, column: str) -> int:
        """Cheap read of a daily counter — used to skip expensive work early."""
        row = await self.fetchone(
            f"SELECT {column} AS used FROM daily_activity WHERE user_id = ? AND day = ?",
            (user_id, day),
        )
        return row["used"] if row else 0

    # ------------------------------------------------------------ inventory

    async def claim_inventory(self, inv_id: int) -> bool:
        """Reserve an owned item so only one activation flow can hold it.

        Returns False if someone (or a second click) already took it.
        """
        cursor = await self.execute(
            "UPDATE inventory SET state = 'active', activated_ts = ? "
            "WHERE id = ? AND state = 'owned'",
            (int(time.time()), inv_id),
        )
        return cursor.rowcount == 1

    async def release_inventory(self, inv_id: int) -> bool:
        """Hand a reserved item back when its flow is cancelled or times out."""
        cursor = await self.execute(
            "UPDATE inventory SET state = 'owned', activated_ts = NULL "
            "WHERE id = ? AND state = 'active'",
            (inv_id,),
        )
        return cursor.rowcount == 1

    # --------------------------------------------------------------- upkeep

    async def prune_ledger(self, older_than_days: int = 365) -> int:
        """Drop ledger rows past the retention window."""
        cutoff = int(time.time()) - older_than_days * 86400
        cursor = await self.execute(
            "DELETE FROM points_ledger WHERE ts < ?", (cutoff,)
        )
        return cursor.rowcount or 0

    async def prune_daily(self, older_than_days: int = 90) -> int:
        """Drop daily counters and claims well past any cap window."""
        import datetime
        cutoff = (datetime.date.today()
                  - datetime.timedelta(days=older_than_days)).isoformat()
        removed = 0
        for table in ("daily_activity", "daily_claims"):
            cursor = await self.execute(f"DELETE FROM {table} WHERE day < ?", (cutoff,))
            removed += cursor.rowcount or 0
        return removed
