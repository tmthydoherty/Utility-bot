"""Dashboard ↔ bot bridge for the Question of the Day pool.

QOTD keeps two kinds of state. Its *settings* (schedule, timezone, post
channels, ping role) are flat per-guild values, so they ride the shared
``utils/module_config_sync.py`` bridge like welcome/security/suggestions do.

Its *question pool*, though, is a list of records with add / edit / delete
semantics that a flat key/value bridge can't express, so it gets its own small
SQLite file in a directory of its own — the same shape as
``cogs/ticketing/config_sync.py``:

* **snapshot** — the bot republishes the whole pool here every tick under a
  handful of keys (``questions``, ``tomorrow``, ``counts``), so the website
  always reads reality, including questions added from Discord and whichever
  question is queued to post next in each guild.
* **commands** — imperative one-shots the website appends (add questions, edit
  one, delete one, reset the pool, clear seen, re-roll tomorrow's question) and
  the bot drains. Each is deleted the moment it succeeds, so a retry never
  re-fires it.

Two processes on one SQLite file is supported as long as neither holds a
transaction open across an await and both set ``busy_timeout``. Every call here
opens, works synchronously off the event loop, and closes.
"""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger("qotd_cog")

# A directory of its own, holding this file and nothing else — the same rule the
# ticketing, automations and economy-config databases follow. The dashboard's
# sandbox grant is to the directory (WAL writes three files), and anything else
# living here would become writable by the internet-facing process.
SYNC_DIR = Path(__file__).resolve().parent.parent / "data" / "qotd_config"
DB_PATH = str(SYNC_DIR / "qotd_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    key TEXT PRIMARY KEY,      -- 'questions' | 'tomorrow' | 'counts'
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,        -- add_questions | edit_question | delete_question
                               -- | reset_pool | clear_seen | reroll_tomorrow
    payload TEXT NOT NULL DEFAULT '{}',
    created_ts INTEGER,
    actor TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class QotdConfigSync:
    """Synchronous SQLite access to the shared QOTD question bridge, off-thread.

    Pure I/O: this class only reads and writes the bridge file. Applying a
    command needs the question database (and, for a re-roll, the guild's
    settings), so that logic stays in the cog — the same division the ticketing
    and economy bridges keep with their owning cogs.
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

    @staticmethod
    def _meta_int(conn: sqlite3.Connection, key: str) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        try:
            return int(row["value"]) if row else 0
        except (TypeError, ValueError):
            return 0

    # ------------------------------------------------------------------ setup

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

    # ------------------------------------------------------------------- read

    async def read_commands(self) -> Tuple[int, List[Dict[str, Any]]]:
        """Return ``(revision, command_rows)`` for commands not yet applied.

        The list is empty when nothing has moved since the last
        :meth:`mark_applied`. A command is ``{id, type, payload}`` with
        ``payload`` decoded to native Python.
        """
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

    # ------------------------------------------------------------------ write

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

    async def publish_snapshot(self, questions: List[Dict[str, Any]],
                               tomorrow: Dict[str, Any],
                               counts: Dict[str, int]):
        """Mirror the whole pool into ``snapshot`` in one transaction.

        Called every tick so the website reads real, current values — including
        questions added or removed from Discord and the queued next question —
        rather than its own last guess. A reader sees either the whole previous
        snapshot or the whole next one, never a mix.
        """
        await asyncio.to_thread(self._publish_snapshot_blocking, questions, tomorrow, counts)

    def _publish_snapshot_blocking(self, questions, tomorrow, counts):
        rows = [
            ("questions", json.dumps(questions)),
            ("tomorrow", json.dumps(tomorrow)),
            ("counts", json.dumps(counts)),
        ]
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM snapshot")
            conn.executemany("INSERT INTO snapshot (key, value) VALUES (?, ?)", rows)
            conn.commit()
        finally:
            conn.close()
