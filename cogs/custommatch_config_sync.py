"""Dashboard → bot bridge for the Custom Matches module.

Unlike onboarding/qotd, this bridge is *commands-only*. Custom Matches keeps a
large, always-current read model in its own database (``data/custommatch.db`` —
games, players, MMR history, matches, rivalries, live queues), and the dashboard
reads that file **read-only and directly**, the same way the Overview reads
``tracking_data.db`` and the economy inspector reads ``economy.db``. There is no
value in republishing a snapshot the reader can already see, so this file carries
only the *write* half:

* **commands** — imperative one-shots the website appends (edit a game, add/clone/
  delete one, set a global channel or role, edit the rank ladder, adjust a
  player's MMR, clear a penalty …) and the bot drains. Each is deleted the moment
  it succeeds, so a retry never re-fires it. Applying a command is what lets the
  bot do the Discord-side work a raw DB write can't — re-render the live queue
  embed, refresh the leaderboard, seed Overwatch role weights.

* **meta** — a ``revision`` this bridge bumps on every append and an ``applied``
  the cog advances once it has drained up to that point. That counter is the
  whole protocol; between the two the loop has nothing to do and stays idle.

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

logger = logging.getLogger("cogs.custommatch")

# A directory of its own, holding this file and nothing else — the same rule the
# onboarding, qotd, ticketing and automations bridges follow. The dashboard's
# sandbox grant is to the directory (WAL writes three sibling files), and
# anything else living here would become writable by the internet-facing process.
SYNC_DIR = Path(__file__).resolve().parent.parent / "data" / "custommatch_config"
DB_PATH = str(SYNC_DIR / "custommatch_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,        -- game_update | game_add | game_clone | game_delete
                               -- | global_set | mod_role_add | mod_role_remove
                               -- | blacklist_add | blacklist_remove
                               -- | rank_set | rank_remove
                               -- | player_mmr_set | player_offset_set | player_ign_set
                               -- | penalty_clear | suspension_remove
    payload TEXT NOT NULL DEFAULT '{}',
    created_ts INTEGER,
    actor TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class CustomMatchConfigSync:
    """Synchronous SQLite access to the Custom Matches command bridge, run off-thread.

    Pure I/O: this class only reads and writes the bridge file. Applying a command
    needs the custom-match database and the running cog (for the Discord-side
    effects), so that logic stays in the cog — the same division onboarding and
    qotd keep with their owning cogs.
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
