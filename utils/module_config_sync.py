"""Dashboard ↔ bot bridge for per-guild module settings.

The web dashboard is sandboxed and cannot reach into a cog's own config files
(welcome_config.json, security_config.json, guild_settings.json, …). This is the
same problem the economy cog solved with cogs/economy/config_sync.py, generalised
so any cog can join in: one small SQLite file in a directory of its own that both
sides may touch, keyed by ``(module, guild, key)`` because — unlike the economy
config, which is global — these settings are per-guild.

The protocol, per module:

* The dashboard writes desired values into ``overrides`` and bumps
  ``meta['revision:<module>']``. It never writes the cog's real config file.
* The owning cog polls its revision every ten seconds. When it moves, it reads
  the pending overrides, applies each through the cog's normal write path — which
  updates the real file and the in-memory state, so the change is live at once —
  records the revision in ``meta['applied:<module>']``, and (when nothing raced
  it) deletes the overrides it consumed.
* After any change, from here or from the cog's own Discord panel, the cog
  republishes its full current settings into ``config``.

Deleting consumed overrides is the one deliberate difference from the economy
bridge, and it is what makes a change made *in Discord* show up on the website:
once an override is applied and cleared, ``config`` is the single source of truth,
so the next publish after a Discord-side edit is what the dashboard reads. An
override only ever shadows ``config`` for the few seconds between a website save
and the cog applying it.

Two processes on one SQLite file is supported as long as neither holds a
transaction open across an await and both set ``busy_timeout``. Every call here
opens, works synchronously off the event loop, and closes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict

from discord.ext import tasks

logger = logging.getLogger('utils.module_config_sync')

# A directory of its own, holding this bridge and nothing else — the same rule
# the automations and economy_config databases follow. The dashboard's sandbox
# grant is to the directory (WAL writes three files alongside the database).
SYNC_DIR = Path(__file__).resolve().parent.parent / "data" / "module_config"
DB_PATH = str(SYNC_DIR / "module_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS config (
    module TEXT,
    guild_id TEXT,
    key TEXT,
    value TEXT,
    PRIMARY KEY (module, guild_id, key)
);
CREATE TABLE IF NOT EXISTS overrides (
    module TEXT,
    guild_id TEXT,
    key TEXT,
    value TEXT,
    updated_ts INTEGER,
    updated_by TEXT,
    PRIMARY KEY (module, guild_id, key)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

# {guild_id: {key: value}} — the shape both snapshots and pending overrides take.
GuildValues = Dict[str, Dict[str, Any]]


class ModuleConfigStore:
    """Synchronous SQLite access to the shared bridge file, run off-thread.

    One store instance is safe to share between every cog that syncs; each call
    is scoped by ``module`` so two cogs never see each other's rows.
    """

    def __init__(self, path: str = DB_PATH):
        self.path = path

    # ---------------------------------------------------------------- helpers

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    # ------------------------------------------------------------------ setup

    async def ensure(self) -> None:
        await asyncio.to_thread(self._ensure_blocking)

    def _ensure_blocking(self) -> None:
        # Derived from the actual path, not the module default, so a test that
        # repoints the store creates its own directory rather than the real one.
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------- read

    async def read_pending(self, module: str) -> tuple[int, GuildValues]:
        """Return ``(revision, {guild_id: {key: value}})`` not yet applied.

        ``{}`` means nothing new since the last :meth:`mark_applied`. Values are
        decoded from the dashboard's JSON into native Python.
        """
        return await asyncio.to_thread(self._read_pending_blocking, module)

    def _read_pending_blocking(self, module: str) -> tuple[int, GuildValues]:
        conn = self._connect()
        try:
            revision = self._meta_int(conn, f"revision:{module}")
            applied = self._meta_int(conn, f"applied:{module}")
            if revision <= applied:
                return applied, {}

            pending: GuildValues = defaultdict(dict)
            for row in conn.execute(
                "SELECT guild_id, key, value FROM overrides WHERE module = ?",
                (module,),
            ):
                try:
                    pending[row["guild_id"]][row["key"]] = json.loads(row["value"])
                except (TypeError, ValueError):
                    logger.warning(
                        "Skipping un-decodable override %s/%s/%s",
                        module, row["guild_id"], row["key"],
                    )
            return revision, dict(pending)
        finally:
            conn.close()

    async def mark_applied(self, module: str, revision: int) -> None:
        await asyncio.to_thread(self._mark_applied_blocking, module, revision)

    def _mark_applied_blocking(self, module: str, revision: int) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            current = self._meta_int(conn, f"revision:{module}")
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (f"applied:{module}", str(revision)),
            )
            # Only clear the overrides if nothing bumped the revision while we
            # were applying. If it moved, the extra rows stay and next tick —
            # which sees revision > applied again — reprocesses them idempotently.
            if current == revision:
                conn.execute("DELETE FROM overrides WHERE module = ?", (module,))
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------ write

    async def publish_snapshot(self, module: str, snapshot: GuildValues) -> None:
        """Mirror a module's full, current settings into ``config``.

        A complete replace of this module's rows in one transaction, so a reader
        sees the whole previous snapshot or the whole next one, never a mix.
        """
        await asyncio.to_thread(self._publish_snapshot_blocking, module, snapshot)

    def _publish_snapshot_blocking(self, module: str, snapshot: GuildValues) -> None:
        rows = [
            (module, str(guild_id), key, json.dumps(value))
            for guild_id, values in snapshot.items()
            for key, value in values.items()
        ]
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM config WHERE module = ?", (module,))
            conn.executemany(
                "INSERT INTO config (module, guild_id, key, value) VALUES (?, ?, ?, ?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _meta_int(conn: sqlite3.Connection, key: str) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        try:
            return int(row["value"]) if row else 0
        except (TypeError, ValueError):
            return 0


class ConfigSyncAgent:
    """Runs one module's sync loop.

    A cog builds an agent with two callbacks and starts it. Everything else — the
    ten-second poll, applying pending overrides per guild, marking the revision,
    and republishing the snapshot — is handled here.

    * ``snapshot()`` returns ``{guild_id: {key: value}}`` for the cog's current
      settings, flattened to the dashboard's field keys. May be sync or async.
    * ``apply(guild_id, values)`` writes one guild's incoming values back into the
      cog's own store. May be sync or async. Raising for one guild doesn't stop
      the others.
    """

    def __init__(
        self,
        module: str,
        snapshot: Callable[[], GuildValues | Awaitable[GuildValues]],
        apply: Callable[[str, Dict[str, Any]], None | Awaitable[None]],
        *,
        bot=None,
        store: ModuleConfigStore | None = None,
    ):
        self.module = module
        self._snapshot = snapshot
        self._apply = apply
        self._bot = bot
        self.store = store or ModuleConfigStore()
        # Publish once on the first tick so the dashboard has real values to read
        # even if nothing has changed since startup.
        self._dirty = True
        self._loop = tasks.loop(seconds=10)(self._tick)
        self._loop.before_loop(self._before_loop)
        self._loop.error(self._on_error)

    async def start(self) -> None:
        await self.store.ensure()
        self._loop.start()

    def stop(self) -> None:
        self._loop.cancel()

    def mark_dirty(self) -> None:
        """Call after a Discord-side settings change so the next tick republishes."""
        self._dirty = True

    async def _before_loop(self) -> None:
        if self._bot is not None:
            await self._bot.wait_until_ready()

    async def _on_error(self, *args) -> None:
        # tasks.Loop.error passes the exception (and, for method loops, self).
        error = args[-1]
        logger.error("config sync loop for %s failed: %r", self.module, error, exc_info=error)

    async def _tick(self) -> None:
        revision, pending = await self.store.read_pending(self.module)
        if pending:
            applied = 0
            for guild_id, values in pending.items():
                try:
                    result = self._apply(guild_id, values)
                    if asyncio.iscoroutine(result):
                        await result
                    applied += 1
                except Exception as e:
                    logger.error(
                        "Applying %s settings for guild %s failed: %r",
                        self.module, guild_id, e, exc_info=True,
                    )
            await self.store.mark_applied(self.module, revision)
            self._dirty = True
            if applied:
                logger.info("Applied %s setting(s) from the dashboard for %d guild(s).", self.module, applied)

        if self._dirty:
            snapshot = self._snapshot()
            if asyncio.iscoroutine(snapshot):
                snapshot = await snapshot
            await self.store.publish_snapshot(self.module, snapshot)
            self._dirty = False
