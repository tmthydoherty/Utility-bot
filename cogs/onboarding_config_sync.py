"""Dashboard ↔ bot bridge for the Welcome & Onboarding module's rich state.

The onboarding cog keeps two kinds of state on the dashboard.

Its flat *settings* (channels, roles, point thresholds, tier/VIP roles) ride the
shared ``utils/module_config_sync.py`` bridge like welcome/security/qotd do.

Everything else — the intro question pool, the blacklist, per-member point totals
and their decay grants, and the ranked point lists — is a list of records with
add / edit / delete semantics that a flat key/value bridge can't express, and the
VIP point floors can only be computed bot-side (they come from Discord roles). So
it gets its own small SQLite file in a directory of its own, the same shape as
``cogs/qotd_config_sync.py``:

* **snapshot** — the bot republishes the whole read model here every tick under a
  handful of keys (``questions``, ``blacklist``, ``members``, ``lists``,
  ``mappings``, ``meta``), so the website always reads reality, including changes
  made in Discord and the VIP floors it can't compute itself.
* **commands** — imperative one-shots the website appends (add/edit/delete a
  question, reorder, blacklist add/remove, adjust/set/wipe a member's points,
  wipe everything, add/remove a game mapping) and the bot drains. Each is deleted
  the moment it succeeds, so a retry never re-fires it.

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

logger = logging.getLogger("onboarding_cog")

# A directory of its own, holding this file and nothing else — the same rule the
# qotd, ticketing and automations databases follow. The dashboard's sandbox grant
# is to the directory (WAL writes three files), and anything else living here
# would become writable by the internet-facing process.
SYNC_DIR = Path(__file__).resolve().parent.parent / "data" / "onboarding_config"
DB_PATH = str(SYNC_DIR / "onboarding_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    key TEXT PRIMARY KEY,      -- 'questions' | 'blacklist' | 'members' | 'lists'
                               -- | 'mappings' | 'meta'
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,        -- add_question | edit_question | delete_question
                               -- | reorder_questions | blacklist_add
                               -- | blacklist_remove | points_adjust | points_wipe
                               -- | wipe_all | mapping_add | mapping_remove
    payload TEXT NOT NULL DEFAULT '{}',
    created_ts INTEGER,
    actor TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class OnboardingConfigSync:
    """Synchronous SQLite access to the shared onboarding bridge, run off-thread.

    Pure I/O: this class only reads and writes the bridge file. Applying a command
    needs the intro database (and, for point/tier maths, the Alerts & Colors cog),
    so that logic stays in the cog — the same division qotd and ticketing keep with
    their owning cogs.
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
        :meth:`mark_applied`. A command is ``{id, type, payload}`` with ``payload``
        decoded to native Python.
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

    async def publish_snapshot(self, sections: Dict[str, Any]):
        """Mirror the whole read model into ``snapshot`` in one transaction.

        Called every tick so the website reads real, current values — including
        questions/blacklist changes made in Discord and the VIP point floors only
        the bot can compute. A reader sees either the whole previous snapshot or the
        whole next one, never a mix. ``sections`` maps a snapshot key to any
        JSON-serialisable value.
        """
        await asyncio.to_thread(self._publish_snapshot_blocking, sections)

    def _publish_snapshot_blocking(self, sections: Dict[str, Any]):
        rows = [(key, json.dumps(value)) for key, value in sections.items()]
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM snapshot")
            conn.executemany("INSERT INTO snapshot (key, value) VALUES (?, ?)", rows)
            conn.commit()
        finally:
            conn.close()
