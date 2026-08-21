"""Dashboard ↔ bot bridge for the NHIE pool.

NHIE keeps two kinds of state. Its *settings* (post channels, log channel, trigger role, cooldown)
are flat per-guild values, so they ride the shared ``utils/module_config_sync.py`` bridge.

Its *question pool*, though, is a list of records with add / edit / delete
semantics that a flat key/value bridge can't express, so it gets its own small
SQLite file in a directory of its own — the same shape as QOTD.

* **snapshot** — the bot republishes the whole pool here every tick under a
  handful of keys, so the website always reads reality.
* **commands** — imperative one-shots the website appends and the bot drains.
"""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger("nhie_cog")

SYNC_DIR = Path(__file__).resolve().parent.parent / "data" / "nhie_config"
DB_PATH = str(SYNC_DIR / "nhie_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    created_ts INTEGER,
    actor TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class NhieConfigSync:
    """Synchronous SQLite access to the shared NHIE question bridge, off-thread."""

    def __init__(self, path: str = DB_PATH):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    @staticmethod
    def _meta_int(conn: sqlite3.Connection, key: str) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        try:
            return int(row["value"]) if row else 0
        except (TypeError, ValueError):
            return 0

    async def ensure(self):
        await asyncio.to_thread(self._ensure_blocking)

    def _ensure_blocking(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    async def read_commands(self) -> Tuple[int, List[Dict[str, Any]]]:
        return await asyncio.to_thread(self._read_commands_blocking)

    def _read_commands_blocking(self):
        conn = self._connect()
        try:
            revision = self._meta_int(conn, "revision")
            applied = self._meta_int(conn, "applied")
            if revision <= applied:
                return applied, []

            commands: List[Dict[str, Any]] = []
            for row in conn.execute("SELECT id, type, payload FROM commands ORDER BY id"):
                try:
                    payload = json.loads(row["payload"]) if row["payload"] else {}
                except (TypeError, ValueError):
                    payload = {}
                commands.append({"id": row["id"], "type": row["type"], "payload": payload})
            return revision, commands
        finally:
            conn.close()

    async def mark_command_done(self, command_id: int):
        await asyncio.to_thread(self._mark_command_done_blocking, command_id)

    def _mark_command_done_blocking(self, command_id: int):
        conn = self._connect()
        try:
            conn.execute("DELETE FROM commands WHERE id = ?", (command_id,))
            conn.commit()
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

    async def publish_snapshot(self, questions: Dict[str, Any]):
        """Mirror the whole pool into ``snapshot`` in one transaction."""
        await asyncio.to_thread(self._publish_snapshot_blocking, questions)

    def _publish_snapshot_blocking(self, questions):
        # We can store the questions snapshot per guild, or just as a whole object.
        # Given NHIE stores it per-guild, we just serialize the whole dict for "questions" key.
        rows = [
            ("questions", json.dumps(questions)),
        ]
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM snapshot")
            conn.executemany("INSERT INTO snapshot (key, value) VALUES (?, ?)", rows)
            conn.commit()
        finally:
            conn.close()
