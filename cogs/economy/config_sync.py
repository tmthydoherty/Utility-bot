"""Dashboard ↔ bot bridge for the Economy and Leveling settings.

The web dashboard cannot reach into economy.db. It sits in the project root
beside fourteen other databases, and the dashboard is the one process here
exposed to the internet, so its systemd sandbox deliberately cannot write
there. This is the same problem the automations cog solved, solved the same
way: a small SQLite file in a directory of its own that both sides may touch.

The protocol mirrors dashboard/lib/automations/store.ts exactly:

* The dashboard writes desired setting values into ``overrides`` and bumps
  ``meta.revision``. It never writes economy.db.
* This bot polls ``meta.revision`` every ten seconds. When it moves it reads the
  pending overrides, applies each through the normal ``Config.set`` path — which
  writes economy.db and refreshes the in-memory cache, so the change is live at
  once — and records the revision it has caught up to in ``meta.applied``.
* After any settings change, from here or from ``/economy_panel``, the bot
  republishes the full settings snapshot into ``config`` so the dashboard always
  reads back the real, current values rather than its own last guess.

Two processes on one SQLite file is a supported configuration as long as neither
holds a transaction open across an await and both set ``busy_timeout``. Every
call here opens, does its work synchronously off the event loop, and closes, so
nothing is ever held open; the dashboard sets the same timeout on its side.
"""

import asyncio
import json
import logging
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger('cogs.economy.config_sync')

# A directory of its own, holding this file and nothing else — the same rule the
# automations database follows. The dashboard's sandbox grant is to the
# directory (WAL writes three files), and anything else living here would become
# writable by the internet-facing process.
SYNC_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "economy_config"
DB_PATH = str(SYNC_DIR / "economy_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS overrides (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_ts INTEGER,
    updated_by TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class EconomyConfigSync:
    """Synchronous SQLite access to the shared config file, run off-thread.

    Kept separate from EconomyDB on purpose: this file is the seam with another
    process and lives outside economy.db, so coupling its lifecycle to the main
    database — or sharing a connection across the two — would only invite the
    kind of cross-database surprise the isolation is meant to prevent.
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

    async def ensure(self):
        await asyncio.to_thread(self._ensure_blocking)

    def _ensure_blocking(self):
        SYNC_DIR.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------- read

    async def read_pending(self) -> tuple:
        """Return ``(revision, {key: value})`` for overrides not yet applied.

        ``revision`` is the dashboard's counter; ``{}`` means nothing new since
        the last :meth:`mark_applied`. Values are decoded from the dashboard's
        JSON into native Python, ready to hand straight to ``Config.set``.
        """
        return await asyncio.to_thread(self._read_pending_blocking)

    def _read_pending_blocking(self) -> tuple:
        conn = self._connect()
        try:
            meta = {row["key"]: row["value"] for row in conn.execute(
                "SELECT key, value FROM meta"
            )}
            revision = int(meta.get("revision") or 0)
            applied = int(meta.get("applied") or 0)
            if revision <= applied:
                return applied, {}

            overrides = {}
            for row in conn.execute("SELECT key, value FROM overrides"):
                try:
                    overrides[row["key"]] = json.loads(row["value"])
                except (TypeError, ValueError):
                    logger.warning("Skipping un-decodable override for %r", row["key"])
            return revision, overrides
        finally:
            conn.close()

    async def mark_applied(self, revision: int):
        await asyncio.to_thread(self._mark_applied_blocking, revision)

    def _mark_applied_blocking(self, revision: int):
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('applied', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(revision),),
            )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------ write

    async def publish_snapshot(self, settings: dict):
        """Mirror the cog's full, current settings into ``config``.

        Called after every change so the dashboard reads real values. Written in
        one transaction; a reader either sees the whole previous snapshot or the
        whole next one, never a half-updated mix.
        """
        await asyncio.to_thread(self._publish_snapshot_blocking, settings)

    def _publish_snapshot_blocking(self, settings: dict):
        conn = self._connect()
        try:
            conn.executemany(
                "INSERT INTO config (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [(key, value) for key, value in settings.items()],
            )
            conn.commit()
        finally:
            conn.close()
