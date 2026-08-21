"""Dashboard ↔ bot bridge for the Ticketing module.

Ticketing keeps its state in JSON files the bot owns — ``data/topics.json``,
``data/panels.json``, ``data/survey_data.json``. Those files are safe for one
writer, not two: the dashboard runs in a separate, internet-facing process whose
systemd sandbox deliberately cannot write into the project data tree, and a
half-written JSON file has no equivalent of SQLite's WAL to protect it. So the
dashboard never touches them. It talks to this small SQLite file instead, in a
directory of its own, exactly as ``cogs/economy/config_sync.py`` and the
automations cog do.

The protocol, mirroring ``dashboard/lib/ticketing/store.ts``:

* **snapshot** — the bot republishes the full contents of the JSON files here
  every tick (``kind`` is ``topic`` / ``panel`` / ``responses``), so the
  dashboard always reads reality, including message ids the bot assigned and any
  edit made from the ``/ticketing`` Discord dashboard.
* **desired** — the dashboard writes whole topic/panel objects (or a tombstone)
  here and bumps ``meta.revision``. The bot applies them into the JSON files and
  records the revision in ``meta.applied``. The dashboard reads
  ``desired ?? snapshot`` so an edit reads back at once, then falls through to
  the snapshot once the bot has applied and cleared the desired row.
* **commands** — imperative one-shots the bot must perform (post a panel to
  Discord, DM a survey, delete responses). Each is deleted the moment it
  succeeds, so a race that re-runs the declarative apply never re-fires one.

Two processes on one SQLite file is a supported configuration as long as neither
holds a transaction open across an await and both set ``busy_timeout``. Every
call here opens, does its work synchronously off the event loop, and closes, so
nothing is ever held open; the dashboard sets the same timeout on its side.
"""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("ticketing_cog")

# A directory of its own, holding this file and nothing else — the same rule the
# automations and economy-config databases follow. The dashboard's sandbox grant
# is to the directory (WAL writes three files), and anything else living here
# would become writable by the internet-facing process.
SYNC_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "ticketing_config"
DB_PATH = str(SYNC_DIR / "ticketing_config.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    kind TEXT NOT NULL,        -- 'topic' | 'panel' | 'responses'
    name TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (kind, name)
);
CREATE TABLE IF NOT EXISTS desired (
    kind TEXT NOT NULL,        -- 'topic' | 'panel'
    name TEXT NOT NULL,
    data TEXT,                 -- JSON object, or NULL when this is a tombstone
    deleted INTEGER NOT NULL DEFAULT 0,
    updated_ts INTEGER,
    updated_by TEXT,
    PRIMARY KEY (kind, name)
);
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,        -- publish_panel | unpublish_panel | send_survey
                               -- | delete_responses | delete_response
    payload TEXT NOT NULL DEFAULT '{}',
    created_ts INTEGER,
    actor TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class TicketingConfigSync:
    """Synchronous SQLite access to the shared ticketing config file, off-thread.

    Pure I/O: this class only reads and writes the bridge. Applying a desired
    change or a command needs Discord objects, so that logic lives in the cog,
    the same division economy/config_sync keeps with EconomyDB.
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
        SYNC_DIR.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------- read

    async def read_pending(self) -> Tuple[int, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Return ``(revision, desired_rows, command_rows)`` not yet applied.

        Both lists are empty when nothing has moved since the last
        :meth:`mark_applied`. A ``desired`` row is ``{kind, name, data, deleted}``
        with ``data`` decoded to native Python; a command is ``{id, type, payload}``.
        """
        return await asyncio.to_thread(self._read_pending_blocking)

    def _read_pending_blocking(self):
        conn = self._connect()
        try:
            revision = self._meta_int(conn, "revision")
            applied = self._meta_int(conn, "applied")
            if revision <= applied:
                return applied, [], []

            desired: List[Dict[str, Any]] = []
            for row in conn.execute("SELECT kind, name, data, deleted FROM desired"):
                data = None
                if row["data"] is not None:
                    try:
                        data = json.loads(row["data"])
                    except (TypeError, ValueError):
                        logger.warning("Skipping un-decodable desired %s/%s",
                                       row["kind"], row["name"])
                        continue
                desired.append({
                    "kind": row["kind"],
                    "name": row["name"],
                    "data": data,
                    "deleted": bool(row["deleted"]),
                })

            commands: List[Dict[str, Any]] = []
            for row in conn.execute(
                "SELECT id, type, payload FROM commands ORDER BY id"
            ):
                try:
                    payload = json.loads(row["payload"]) if row["payload"] else {}
                except (TypeError, ValueError):
                    payload = {}
                commands.append({"id": row["id"], "type": row["type"], "payload": payload})

            return revision, desired, commands
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
            # Only clear the desired rows if nothing bumped the revision while we
            # were applying. If something did, leave them: the next tick sees
            # revision > applied again and re-applies them, which is idempotent.
            if self._meta_int(conn, "revision") == revision:
                conn.execute("DELETE FROM desired")
            conn.commit()
        finally:
            conn.close()

    async def publish_snapshot(self, topics: Dict[str, Any],
                               panels: Dict[str, Any],
                               responses: Dict[str, Any]):
        """Mirror the current JSON files into ``snapshot`` in one transaction.

        Called every tick so the dashboard reads real, current values — including
        edits made from Discord — rather than its own last guess. A reader sees
        either the whole previous snapshot or the whole next one, never a mix.
        """
        await asyncio.to_thread(self._publish_snapshot_blocking, topics, panels, responses)

    def _publish_snapshot_blocking(self, topics, panels, responses):
        rows = []
        for name, data in topics.items():
            rows.append(("topic", name, json.dumps(data)))
        for name, data in panels.items():
            rows.append(("panel", name, json.dumps(data)))
        for name, data in responses.items():
            rows.append(("responses", name, json.dumps(data)))

        conn = self._connect()
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM snapshot")
            conn.executemany(
                "INSERT INTO snapshot (kind, name, data) VALUES (?, ?, ?)", rows
            )
            conn.commit()
        finally:
            conn.close()
