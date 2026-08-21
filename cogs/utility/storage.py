"""Utility database — schema, connection and accessors.

Backed by data/utility/utility.db, in a directory of its own. Everything the
Utility cog persists lives here: reminders and one-off messages, the media,
reaction and sticky rules, and who changed them.

The file lives in its own directory rather than beside the other cog databases
for the same reason `cogs/automations/storage.py` moved out: the dashboard
writes to it directly from another process, and it runs behind
`ProtectSystem=strict` with a read-only home, so it needs a `ReadWritePaths`
grant — and SQLite in WAL mode creates two sibling files (`-wal`, `-shm`),
which means the grant has to cover the *directory*, not just the file. Anything
else in that directory would be writable by the one process on this machine
that is reachable from the internet, so the directory holds exactly this
database and nothing else. `_relocate_from_root` moves an older build's
`utility.db` in from the project root on first boot.

The dashboard writes reminders/messages/rules and bumps `settings['revision']`;
the cog polls that integer and reloads when it moves. No socket, no endpoint —
the same bridge the Automations builder uses.

Automations used to live here too. They are their own cog now, with their own
file — `cogs/automations/storage.py` moves those tables across on first boot
and renames the originals to `moved_*` rather than dropping them, so the rows
are still on disk if anyone wants to look.

Two rules that the rest of the cog depends on:

* **Everything is addressed by a stable id, never a list index.** The version
  this replaced kept rules in a JSON list and had the panel refer to them by
  position, so two admins with the panel open would edit and delete different
  rules than the ones they picked.

* **Every expiry table is swept by the scheduler but filtered on read.**
  Accessors add `expires_ts > now` themselves, so an expired row that the
  sweeper has not reached yet is never observed as live and correctness never
  depends on sweeper latency. Same rule as `cogs/economy/effects.py`.

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

logger = logging.getLogger('cogs.utility.storage')

ROOT = Path(__file__).resolve().parent.parent.parent
DB_DIR = ROOT / "data" / "utility"
DB_PATH = str(DB_DIR / "utility.db")
# Where this database sat before it moved into a directory the dashboard sandbox
# could be pointed at. `_relocate_from_root` brings it across once.
PREVIOUS_DB_PATH = str(ROOT / "utility.db")
LEGACY_CONFIG = ROOT / "utility_config.json"
# The reminder cog this one absorbed kept its data in these two JSON files.
LEGACY_REMINDERS = ROOT / "reminders.json"
LEGACY_REMINDERS_CONFIG = ROOT / "reminders_config.json"

# Bumped by every write the dashboard makes and watched by the cog so a reminder
# or rule edited on the website takes effect without a restart. A monotonic
# counter rather than a timestamp: two edits in the same second have to be two
# distinct values, or the second is never noticed.
REVISION_KEY = "revision"


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Media-only channels. The old config stored a bare channel id; everything
-- past `channel_id` here is a setting that version had no way to express.
CREATE TABLE IF NOT EXISTS media_channels (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    allow_attachments INTEGER NOT NULL DEFAULT 1,
    allow_links INTEGER NOT NULL DEFAULT 1,
    allow_embeds INTEGER NOT NULL DEFAULT 1,
    allow_stickers INTEGER NOT NULL DEFAULT 1,
    bypass_role_ids TEXT NOT NULL DEFAULT '[]',
    thread_enabled INTEGER NOT NULL DEFAULT 1,
    thread_name_template TEXT NOT NULL DEFAULT '{user} - {date}',
    thread_archive_minutes INTEGER NOT NULL DEFAULT 60,
    auto_react TEXT NOT NULL DEFAULT '[]',
    post_cooldown_s INTEGER NOT NULL DEFAULT 0,
    dm_on_delete INTEGER NOT NULL DEFAULT 0,
    created_ts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_channel ON media_channels(channel_id);

-- Reaction rules. `mode` is remove_matching (the original behaviour) or
-- allowlist / blocklist, which the old version could not do at all.
CREATE TABLE IF NOT EXISTS reaction_rules (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    scope TEXT NOT NULL DEFAULT 'all',          -- all | role_mention | from_user
    mode TEXT NOT NULL DEFAULT 'remove',        -- remove | allowlist | blocklist
    role_ids TEXT NOT NULL DEFAULT '[]',
    user_ids TEXT NOT NULL DEFAULT '[]',
    emoji TEXT NOT NULL DEFAULT '[]',           -- for allowlist / blocklist
    bypass_role_ids TEXT NOT NULL DEFAULT '[]',
    remove_after_s INTEGER NOT NULL DEFAULT 0,  -- 0 = immediately
    max_reactions INTEGER NOT NULL DEFAULT 0,   -- 0 = uncapped
    include_threads INTEGER NOT NULL DEFAULT 1,
    created_ts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_reaction_channel ON reaction_rules(channel_id);

CREATE TABLE IF NOT EXISTS sticky_messages (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    embed_json TEXT,
    plain_text INTEGER NOT NULL DEFAULT 0,
    ping_role_id INTEGER,
    buttons_json TEXT NOT NULL DEFAULT '[]',
    message_id INTEGER,
    min_interval_s INTEGER NOT NULL DEFAULT 30,
    last_posted_ts INTEGER NOT NULL DEFAULT 0,
    created_ts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sticky_channel ON sticky_messages(channel_id);

-- Reminders and one-off messages. `kind` decides how it fires:
--   scheduled — a recurring post on a weekly/monthly/etc. schedule.
--   interval  — a repeating post every N seconds (interval_s).
--   oneoff    — sent once, then editable in place from the dashboard.
-- The embed is a full JSON document (title, description, colour, image,
-- thumbnail, author, footer, fields) so the dashboard can build anything
-- Discord allows. `content` is the text above the embed — the only place a
-- ping actually notifies, since a mention inside an embed never does.
CREATE TABLE IF NOT EXISTS reminders (
    id TEXT PRIMARY KEY,
    guild_id INTEGER NOT NULL,
    kind TEXT NOT NULL DEFAULT 'scheduled',     -- scheduled | interval | oneoff
    name TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    plain_text INTEGER NOT NULL DEFAULT 0,
    embed_json TEXT NOT NULL DEFAULT '{}',
    content TEXT NOT NULL DEFAULT '',
    ping_role_id INTEGER,
    channel_ids_json TEXT NOT NULL DEFAULT '[]',
    buttons_json TEXT NOT NULL DEFAULT '[]',
    reaction_role_json TEXT,
    use_timestamp INTEGER NOT NULL DEFAULT 0,
    delete_previous INTEGER NOT NULL DEFAULT 0,
    interval_s INTEGER NOT NULL DEFAULT 0,
    schedule_json TEXT,                          -- {frequency,time_utc,timezone,days_of_week,day_of_month}
    event_schedule_json TEXT,                    -- recurring countdown shown in the body
    event_timestamp_utc INTEGER,                 -- legacy single-event countdown
    skip_next INTEGER NOT NULL DEFAULT 0,
    skipped_dates_json TEXT NOT NULL DEFAULT '[]',
    last_sent_ts INTEGER NOT NULL DEFAULT 0,
    last_message_ids_json TEXT NOT NULL DEFAULT '{}',
    pending_edit INTEGER NOT NULL DEFAULT 0,     -- oneoff: dashboard edited a sent message
    created_ts INTEGER NOT NULL DEFAULT 0,
    updated_ts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_reminders_guild ON reminders(guild_id, enabled);

-- Who changed what. An automation that starts misbehaving at 3am is a much
-- shorter conversation when the panel can say who last touched it.
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    entity TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT ''
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
    "sticky_messages": [
        ("name", "TEXT NOT NULL DEFAULT ''"),
        ("plain_text", "INTEGER NOT NULL DEFAULT 0"),
        ("ping_role_id", "INTEGER"),
        ("buttons_json", "TEXT NOT NULL DEFAULT '[]'"),
    ],
}


def _relocate_from_root(destination: str) -> None:
    """Move a utility.db left in the project root by an earlier build.

    The first version of this cog put utility.db beside every other cog
    database. That is a place the dashboard cannot be granted write access to
    without also granting it the whole tree, so the file moved into a directory
    of its own. Anyone who ran that build has their media/reaction/sticky rules
    in the old file.

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
                     f"Utility will start empty until it is moved by hand.")


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


class UtilityDB:
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
            # The dashboard writes to this same file from another process, so a
            # write there must not fail this one instantly. Five seconds is far
            # longer than any write here takes and far shorter than a user would
            # notice.
            await self._db.execute("PRAGMA busy_timeout=5000;")
            await self._db.executescript(SCHEMA)
            await self._migrate()
            await self._db.commit()
            logger.info("Utility database connected (WAL, synchronous=NORMAL).")

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
            raise RuntimeError("UtilityDB used before connect()")
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

    async def get_revision(self) -> int:
        raw = await self.get_setting(REVISION_KEY, "0")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    async def bump_revision(self):
        """Called after any write from the cog side, so the dashboard reads the
        current state rather than a stale ``config``."""
        await self.set_setting(REVISION_KEY, str(await self.get_revision() + 1))

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

    # -------------------------------------------------------------- audit

    async def audit_log(self, user_id: int, entity: str, entity_id: str,
                        action: str, detail: str = ""):
        await self.execute(
            "INSERT INTO audit (ts, user_id, entity, entity_id, action, detail) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (now(), user_id, entity, entity_id, action, detail[:500]),
        )

    async def prune_audit(self, retain_days: int = 180):
        """Keep the trail long enough to answer "who changed this?" and no longer.

        Six months is well past the point anyone is still asking, and these
        rows are never read in bulk — the panel shows the last handful.
        """
        await self.execute("DELETE FROM audit WHERE ts < ?",
                           (now() - retain_days * 86400,))


# --------------------------------------------------------------- migration

async def migrate_legacy_json(db: UtilityDB, path: Path = None) -> int:
    """Import utility_config.json, then take it out of the way.

    The old file was keyed by guild id, with `media_channels` as a list of
    channel ids and `reaction_rules` as a list of dicts. Renaming rather than
    deleting keeps a rollback possible; the rename is also what makes this
    run exactly once.
    """
    # Resolved here rather than as a default argument, for the same reason as
    # DB_PATH above.
    path = Path(path) if path is not None else LEGACY_CONFIG
    if not path.exists():
        return 0

    def _read():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        data = await asyncio.to_thread(_read)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Could not read {path} for migration: {e}")
        return 0

    imported = 0
    ts = now()
    for guild_raw, payload in (data or {}).items():
        try:
            guild_id = int(guild_raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue

        for channel_id in payload.get("media_channels", []) or []:
            try:
                channel_id = int(channel_id)
            except (TypeError, ValueError):
                continue
            existing = await db.fetchone(
                "SELECT id FROM media_channels WHERE channel_id = ?", (channel_id,)
            )
            if existing:
                continue
            # The old cog deleted anything without an image/video attachment,
            # so links and embeds are off here to preserve current behaviour —
            # the panel can turn them on.
            await db.insert_row(
                "media_channels", guild_id=guild_id, channel_id=channel_id,
                allow_links=0, allow_embeds=0, allow_stickers=0, created_ts=ts,
            )
            imported += 1

        for rule in payload.get("reaction_rules", []) or []:
            if not isinstance(rule, dict) or "channel_id" not in rule:
                continue
            try:
                channel_id = int(rule["channel_id"])
            except (TypeError, ValueError):
                continue
            scope = rule.get("type", "all")
            if scope not in ("all", "role_mention", "from_user"):
                scope = "all"
            await db.insert_row(
                "reaction_rules", guild_id=guild_id, channel_id=channel_id,
                scope=scope, mode="remove",
                role_ids=json.dumps([int(r) for r in rule.get("role_ids", [])]),
                user_ids=json.dumps([int(u) for u in rule.get("user_ids", [])]),
                created_ts=ts,
            )
            imported += 1

    try:
        await asyncio.to_thread(path.rename, path.with_suffix(".json.migrated"))
    except OSError as e:
        # Without the rename this would re-run every boot. The media insert is
        # guarded by a uniqueness check but the reaction insert is not, so say
        # so loudly rather than quietly duplicating rules.
        logger.error(f"Migrated {imported} rows but could not rename {path}: {e}. "
                     f"Move it aside manually or rules will be duplicated on next start.")
        return imported

    logger.info(f"Migrated {imported} rows from {path.name} into utility.db.")
    return imported


def _url_safe_id(value: str) -> bool:
    """A reminder id is used as a URL path segment on the dashboard, so it must
    be free of spaces and punctuation. New ids are 12 hex chars; the only ids
    that fail this are legacy keys an earlier migration carried across."""
    return bool(value) and all(c.isalnum() or c in "_-" for c in value)


async def repair_reminder_ids(db: "UtilityDB") -> int:
    """Rename any reminder whose id isn't URL-safe.

    An earlier build's migration reused the old cog's `name_guildid` key as the
    reminder id, and those keys contain spaces and brackets — so the dashboard's
    /utility/reminders/<id> route 404s on them. Nothing references a reminder id
    but the row itself, so a straight rename is safe. Idempotent: clean ids are
    left alone, so this does nothing on every boot after the first.
    """
    rows = await db.fetchall("SELECT id FROM reminders")
    fixed = 0
    for row in rows:
        old = row["id"]
        if _url_safe_id(str(old)):
            continue
        await db.execute("UPDATE reminders SET id = ? WHERE id = ?", (new_id(), old))
        fixed += 1
    if fixed:
        logger.info(f"Repaired {fixed} reminder id(s) that were not URL-safe.")
    return fixed


def _embed_from_legacy(data: dict) -> dict:
    """The old reminder embed was title + message + colour + one image. Map it
    into the full-embed document the new table stores, leaving everything the
    old format could not express unset so the dashboard shows it empty."""
    embed: dict = {}
    if data.get("title_text"):
        embed["title"] = data["title_text"]
    if data.get("message"):
        embed["description"] = data["message"]
    if data.get("color") is not None:
        embed["color"] = data["color"]
    if data.get("image_url"):
        embed["image_url"] = data["image_url"]
    return embed


async def migrate_reminders_json(
    db: "UtilityDB",
    reminders_path: Path = None,
    config_path: Path = None,
) -> int:
    """Import the absorbed reminder cog's reminders.json and reminders_config.json.

    Scheduled/interval reminders become rows in `reminders`; each sticky becomes
    one row per channel in `sticky_messages` (that table is keyed per channel,
    which is cleaner than the old one-record-many-channels shape). Firing state —
    last_sent, skip_next, last_message_ids — carries across so nothing double-fires
    or re-fires after the move. The files are renamed to `.migrated` rather than
    deleted, which both keeps a rollback possible and makes this run exactly once.
    """
    reminders_path = Path(reminders_path) if reminders_path is not None else LEGACY_REMINDERS
    config_path = Path(config_path) if config_path is not None else LEGACY_REMINDERS_CONFIG

    def _read(p: Path):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    # --- per-guild config: timezone + admin roles ---
    if config_path.exists():
        try:
            cfg = await asyncio.to_thread(_read, config_path)
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Could not read {config_path} for migration: {e}")
            cfg = {}
        for guild_raw, payload in (cfg or {}).items():
            if not isinstance(payload, dict):
                continue
            tz = payload.get("timezone")
            if tz:
                await db.set_setting(f"timezone:{guild_raw}", str(tz))
            roles = payload.get("admin_role_ids") or []
            await db.set_setting(f"admin_roles:{guild_raw}", json.dumps(list(roles)))
        try:
            await asyncio.to_thread(config_path.rename, config_path.with_suffix(".json.migrated"))
        except OSError as e:
            logger.error(f"Could not rename {config_path}: {e}")

    if not reminders_path.exists():
        return 0
    try:
        data = await asyncio.to_thread(_read, reminders_path)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Could not read {reminders_path} for migration: {e}")
        return 0

    imported = 0
    ts = now()
    for rid, rem in (data or {}).items():
        if not isinstance(rem, dict):
            continue
        try:
            guild_id = int(rem.get("guild_id"))
        except (TypeError, ValueError):
            logger.warning(f"Reminder {rid} has no usable guild_id; skipping.")
            continue

        channel_ids = [int(c) for c in rem.get("channel_ids", []) if str(c).lstrip("-").isdigit()]
        embed_json = json.dumps(_embed_from_legacy(rem))
        reaction_role = rem.get("reaction_role")
        ping_role_id = rem.get("ping_role_id")
        plain_text = 1 if rem.get("plain_text") else 0

        if rem.get("type") == "sticky":
            sticky_ids = rem.get("last_sticky_ids", {}) or {}
            for cid in channel_ids:
                existing = await db.fetchone(
                    "SELECT id FROM sticky_messages WHERE channel_id = ?", (cid,)
                )
                if existing:
                    continue
                await db.insert_row(
                    "sticky_messages", guild_id=guild_id, channel_id=cid,
                    enabled=1 if rem.get("enabled", True) else 0,
                    name=rem.get("name", ""),
                    content="", embed_json=embed_json, plain_text=plain_text,
                    ping_role_id=ping_role_id,
                    buttons_json=json.dumps([reaction_role] if reaction_role else []),
                    message_id=sticky_ids.get(str(cid)),
                    created_ts=ts,
                )
                imported += 1
            continue

        # A fresh id, never the legacy key: those keys are `name_guildid` and
        # can contain spaces and brackets, which are hostile in a URL path (the
        # dashboard addresses a reminder by id) and as a database key.
        await db.insert_row(
            "reminders", id=new_id(),
            guild_id=guild_id, kind="scheduled",
            name=rem.get("name", ""),
            enabled=1 if rem.get("enabled", True) else 0,
            plain_text=plain_text,
            embed_json=embed_json,
            content="",
            ping_role_id=ping_role_id,
            channel_ids_json=json.dumps(channel_ids),
            buttons_json="[]",
            reaction_role_json=json.dumps(reaction_role) if reaction_role else None,
            use_timestamp=1 if rem.get("use_timestamp") else 0,
            delete_previous=1 if rem.get("delete_previous") else 0,
            schedule_json=json.dumps(rem["schedule_data"]) if rem.get("schedule_data") else None,
            event_schedule_json=json.dumps(rem["event_schedule"]) if rem.get("event_schedule") else None,
            event_timestamp_utc=rem.get("event_timestamp_utc"),
            skip_next=int(rem.get("skip_next", 0) or 0),
            skipped_dates_json=json.dumps(rem.get("skipped_dates", []) or []),
            last_sent_ts=int(rem.get("last_sent_timestamp", 0) or 0),
            last_message_ids_json=json.dumps(rem.get("last_message_ids", {}) or {}),
            created_ts=ts, updated_ts=ts,
        )
        imported += 1

    try:
        await asyncio.to_thread(reminders_path.rename, reminders_path.with_suffix(".json.migrated"))
    except OSError as e:
        logger.error(f"Migrated {imported} reminders but could not rename {reminders_path}: {e}. "
                     f"Move it aside manually or reminders will be duplicated on next start.")
        return imported

    logger.info(f"Migrated {imported} reminder(s)/sticky row(s) from {reminders_path.name}.")
    return imported
