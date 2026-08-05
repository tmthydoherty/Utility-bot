"""Server audit logging.

Every listener funnels into a single batcher rather than posting on the spot.
Discord emits one gateway event per atomic change, so a moderator stripping ten
roles off someone, or Vibey handing a team role to a full custom-match lobby,
would otherwise produce ten or twelve near-identical embeds. Events are keyed by
(guild, event, actor) and held on a debounce timer, then rendered together.

Attribution comes from the audit log. `on_audit_log_entry_create` fills a
short-lived cache, and anything that misses the cache falls back to polling
`guild.audit_logs()` the way cogs/welcome.py always has. When the actor turns out
to be Vibey acting on a single member, the member is credited first and the bot
noted in parentheses — that reads correctly for the common case of a user
clicking a button and the bot carrying out the change on their behalf.

Configured entirely through /audit_panel.
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

logger = logging.getLogger('cogs.audit_log')

DB_PATH = os.path.join("data", "audit_log.db")

# Seeded into guild_settings on first run so exit logging doesn't gap while the
# panel is being configured. This is the file cogs/welcome.py still owns.
LEGACY_CONFIG_FILE = "welcome_config.json"

# How long a bucket waits for more of the same kind of event before posting.
# Overridable per guild in the panel.
DEFAULT_BATCH_SECONDS = 15
# Ceiling on the debounce, so a steady trickle of events can't defer a bucket
# forever.
MAX_BATCH_HOLD_SECONDS = 60

# Audit log entries older than this are assumed to belong to some earlier
# action, not the event we're currently attributing.
AUDIT_FRESHNESS_SECONDS = 15
# The gateway usually delivers the member/channel update before the matching
# audit log entry. Give the entry a moment to land before giving up on it.
AUDIT_SETTLE_SECONDS = 2

# Deleted message content has to come from our own mirror; Discord sends only an
# ID. Two days covers effectively every delete anyone asks about.
MESSAGE_RETENTION_SECONDS = 48 * 60 * 60

# A member's onboarding picks and any role a cog hands them on arrival belong in
# their join embed, not in a separate role-change entry moments later. Role
# changes are held back until the join embed has taken its snapshot, then
# reported normally — the handoff is on the snapshot itself, not a second timer,
# so no change can fall between the two and go unlogged.
JOIN_SETTLE_SECONDS = 30
# Safety net only: drops a tracked join whose snapshot never ran, so a failed
# _log_join can't suppress that member's role changes forever.
JOIN_TRACK_TTL_SECONDS = 90

# Kicks issued by cogs/inactivity.py, which get their own muted colour so they
# don't read as moderator action.
INACTIVITY_KICK_COLOR = discord.Color.from_rgb(199, 218, 232)

COLOR_JOIN = discord.Color.from_rgb(87, 181, 120)
COLOR_LEAVE = discord.Color.yellow()
COLOR_KICK = discord.Color.orange()
COLOR_BAN = discord.Color.red()
COLOR_UNBAN = discord.Color.from_rgb(120, 170, 220)
COLOR_TIMEOUT = discord.Color.dark_gold()
COLOR_DELETE = discord.Color.from_rgb(214, 88, 88)
COLOR_NEUTRAL = discord.Color.from_rgb(88, 101, 242)
# Role gains and losses are told apart at a glance by colour as well as by the
# field labels. Kept clear of the exit palette above, which these never share a
# channel with.
COLOR_ROLE_ADD = discord.Color.from_rgb(87, 181, 120)
COLOR_ROLE_REMOVE = discord.Color.from_rgb(191, 120, 148)

CATEGORIES: Dict[str, str] = {
    "messages": "Messages",
    "members": "Members",
    "roles": "Roles",
    "server": "Server",
}

# key -> (category, label, default_enabled)
EVENTS: Dict[str, Tuple[str, str, bool]] = {
    "message_delete":       ("messages", "Message deleted", True),
    "message_edit":         ("messages", "Message edited", True),
    "message_bulk_delete":  ("messages", "Bulk delete", True),

    "member_join":          ("members", "Member joined", True),
    "member_leave":         ("members", "Member left", True),
    "member_kick":          ("members", "Member kicked", True),
    "member_ban":           ("members", "Member banned", True),
    "member_unban":         ("members", "Member unbanned", True),
    "member_timeout":       ("members", "Member timed out", True),
    "nickname_change":      ("members", "Nickname changed", True),
    "username_change":      ("members", "Username changed", True),
    "avatar_change":        ("members", "Avatar changed", True),

    "member_roles":         ("roles", "Member roles changed", True),
    "role_update":          ("roles", "Role renamed / recoloured", True),
    "role_permissions":     ("roles", "Role permissions changed", True),

    "channel_name":         ("server", "Channel renamed", True),
    "channel_permissions":  ("server", "Channel permissions changed", True),
}

# These carry moderation weight and go to the exit / mod channels the server
# already uses, bypassing per-category routing entirely.
EXIT_EVENTS = {"member_leave", "member_kick", "member_ban"}
MOD_ONLY_EVENTS = {"member_timeout", "member_unban"}

# Events whose footer already carries the timing that matters — how long they
# were in the server, when the account was made — so Discord's own "posted at"
# stamp beside it is just noise.
NO_TIMESTAMP_EVENTS = {"member_join"} | EXIT_EVENTS

# Columns /audit_panel is allowed to write, so the settings updater can build a
# column name into SQL without opening an injection hole.
ALLOWED_SETTING_KEYS = {
    "default_channel_id", "exit_channel_id", "mod_channel_id",
    "batch_seconds", "log_bot_messages",
}

# Channel slots the panel can assign, in display order. A slot names either a
# settings column, a whole category, or a single event — an event slot overrides
# its category, so profile churn can be kept out of the members channel without
# turning it off entirely.
CHANNEL_TARGETS: Dict[str, Dict[str, Any]] = {
    "default":  {"label": "Default", "column": "default_channel_id",
                 "help": "Fallback for any category without its own channel."},
    "messages": {"label": "Messages", "category": "messages",
                 "help": "Deletes, edits, bulk deletes."},
    "members":  {"label": "Members", "category": "members",
                 "help": "Joins, nicknames, and anything not split out below."},
    "roles":    {"label": "Roles", "category": "roles",
                 "help": "Member role changes, role edits."},
    "server":   {"label": "Server", "category": "server",
                 "help": "Channel renames and permission changes."},
    "usernames": {"label": "Usernames", "event": "username_change",
                  "help": "Username changes only. Falls back to Members."},
    "avatars":  {"label": "Avatars", "event": "avatar_change",
                 "help": "Avatar changes only. Falls back to Members."},
    "exit":     {"label": "Exit", "column": "exit_channel_id",
                 "help": "All leaves, kicks and bans."},
    "mod":      {"label": "Mod", "column": "mod_channel_id",
                 "help": "Kicks and bans (not inactivity), timeouts, unbans."},
}


# ----------------------------------------------------------------------
# Formatting helpers
# ----------------------------------------------------------------------

def fmt_duration(seconds: int) -> str:
    """Render a span as `3d 4h 20m`, the format the exit embeds already use."""
    seconds = int(seconds)
    if seconds < 60:
        return "<1m"
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    return " ".join(parts) if parts else "<1m"


def fmt_account_age(created_at) -> str:
    """Creation date plus how old it is, as plain text.

    Footers render no markdown, so Discord's `<t:…>` timestamps come out as
    literal gibberish there and the date has to be formatted here instead.
    """
    days = max(0, (discord.utils.utcnow() - created_at).days)
    stamp = created_at.strftime("%d %b %Y")
    if days < 1:
        return f"{stamp} (today)"
    if days < 30:
        return f"{stamp} ({days}d old)"
    if days < 365:
        return f"{stamp} ({days // 30}mo old)"
    years, months = days // 365, (days % 365) // 30
    age = f"{years}y" + (f" {months}mo" if months else "")
    return f"{stamp} ({age} old)"


def truncate(text: str, limit: int) -> str:
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def channel_label(guild: discord.Guild, channel_id) -> str:
    """Render a stored channel id as a mention, or a clear unset/missing marker."""
    if not channel_id:
        return "`not set`"
    cid = int(channel_id)
    channel = guild.get_channel(cid) or guild.get_thread(cid)
    if channel is None:
        return f"`missing ({cid})`"
    return channel.mention


def perm_name(flag: str) -> str:
    return flag.replace("_", " ").replace("guild", "server").title()


def diff_permissions(before: discord.Permissions,
                     after: discord.Permissions) -> Tuple[List[str], List[str]]:
    """Return (granted, revoked) permission names between two permission sets."""
    granted, revoked = [], []
    for flag, value in after:
        if getattr(before, flag) == value:
            continue
        (granted if value else revoked).append(perm_name(flag))
    return granted, revoked


def diff_overwrite(before: Optional[discord.PermissionOverwrite],
                   after: Optional[discord.PermissionOverwrite]) -> Tuple[List[str], List[str], List[str]]:
    """Return (allowed, denied, cleared) between two channel permission overwrites."""
    b = dict(before) if before is not None else {}
    a = dict(after) if after is not None else {}
    allowed, denied, cleared = [], [], []
    for flag in set(b) | set(a):
        old, new = b.get(flag), a.get(flag)
        if old == new:
            continue
        if new is True:
            allowed.append(perm_name(flag))
        elif new is False:
            denied.append(perm_name(flag))
        else:
            cleared.append(perm_name(flag))
    return allowed, denied, cleared


# ----------------------------------------------------------------------
# Database
# ----------------------------------------------------------------------

class AuditDB:
    """Settings plus the short-lived message mirror.

    Writes go through a single connection in WAL mode. Message rows are buffered
    in memory and flushed with executemany rather than committed one at a time —
    the bot runs on a Raspberry Pi and a commit per message would hammer the card.
    """

    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self._message_buffer: List[Tuple] = []
        # Config is read on the hot path — every message checks the ignore list
        # and every event checks its toggle — so keep it in memory and drop the
        # guild's entry whenever the panel writes. Reads outnumber writes by
        # several orders of magnitude.
        self._cache: Dict[str, Dict[int, Any]] = {
            "settings": {}, "categories": {}, "events": {},
            "toggles": {}, "ignored": {},
        }

    def _invalidate(self, guild_id: int, *kinds: str):
        for kind in kinds:
            self._cache[kind].pop(guild_id, None)

    async def connect(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._create_tables()
        await self._conn.commit()

    async def close(self):
        await self.flush_messages()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _create_tables(self):
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id           INTEGER PRIMARY KEY,
                default_channel_id INTEGER,
                exit_channel_id    INTEGER,
                mod_channel_id     INTEGER,
                batch_seconds      INTEGER DEFAULT 15,
                log_bot_messages   INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS category_channels (
                guild_id   INTEGER NOT NULL,
                category   TEXT    NOT NULL,
                channel_id INTEGER,
                PRIMARY KEY (guild_id, category)
            );
            CREATE TABLE IF NOT EXISTS event_channels (
                guild_id   INTEGER NOT NULL,
                event_key  TEXT    NOT NULL,
                channel_id INTEGER,
                PRIMARY KEY (guild_id, event_key)
            );
            CREATE TABLE IF NOT EXISTS event_toggles (
                guild_id  INTEGER NOT NULL,
                event_key TEXT    NOT NULL,
                enabled   INTEGER NOT NULL,
                PRIMARY KEY (guild_id, event_key)
            );
            CREATE TABLE IF NOT EXISTS ignored_channels (
                guild_id   INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                PRIMARY KEY (guild_id, channel_id)
            );
            CREATE TABLE IF NOT EXISTS ignored_users (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS message_cache (
                message_id  INTEGER PRIMARY KEY,
                guild_id    INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                author_id   INTEGER NOT NULL,
                content     TEXT,
                attachments TEXT,
                created_at  INTEGER NOT NULL,
                edited_at   INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_message_created
                ON message_cache (created_at);
            """
        )

    # -- settings ------------------------------------------------------

    async def get_settings(self, guild_id: int) -> Dict[str, Any]:
        cached = self._cache["settings"].get(guild_id)
        if cached is not None:
            return cached
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT * FROM guild_settings WHERE guild_id = ?", (guild_id,)
            )
            row = await cur.fetchone()
            if row is None:
                await self._conn.execute(
                    "INSERT INTO guild_settings (guild_id) VALUES (?)", (guild_id,)
                )
                await self._conn.commit()
                cur = await self._conn.execute(
                    "SELECT * FROM guild_settings WHERE guild_id = ?", (guild_id,)
                )
                row = await cur.fetchone()
            settings = dict(row)
        self._cache["settings"][guild_id] = settings
        return settings

    async def set_setting(self, guild_id: int, key: str, value):
        if key not in ALLOWED_SETTING_KEYS:
            logger.error("Rejected write to unknown setting key: %s", key)
            return
        await self.get_settings(guild_id)  # ensure the row exists
        async with self._lock:
            await self._conn.execute(
                f"UPDATE guild_settings SET {key} = ? WHERE guild_id = ?",
                (value, guild_id),
            )
            await self._conn.commit()
        self._invalidate(guild_id, "settings")

    async def get_category_channels(self, guild_id: int) -> Dict[str, Optional[int]]:
        cached = self._cache["categories"].get(guild_id)
        if cached is not None:
            return cached
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT category, channel_id FROM category_channels WHERE guild_id = ?",
                (guild_id,),
            )
            channels = {r["category"]: r["channel_id"] for r in await cur.fetchall()}
        self._cache["categories"][guild_id] = channels
        return channels

    async def set_category_channel(self, guild_id: int, category: str,
                                   channel_id: Optional[int]):
        async with self._lock:
            if channel_id is None:
                await self._conn.execute(
                    "DELETE FROM category_channels WHERE guild_id = ? AND category = ?",
                    (guild_id, category),
                )
            else:
                await self._conn.execute(
                    "INSERT INTO category_channels (guild_id, category, channel_id) "
                    "VALUES (?, ?, ?) ON CONFLICT(guild_id, category) "
                    "DO UPDATE SET channel_id = excluded.channel_id",
                    (guild_id, category, channel_id),
                )
            await self._conn.commit()
        self._invalidate(guild_id, "categories")

    async def get_event_channels(self, guild_id: int) -> Dict[str, Optional[int]]:
        """Per-event channel overrides, which win over the category channel."""
        cached = self._cache["events"].get(guild_id)
        if cached is not None:
            return cached
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT event_key, channel_id FROM event_channels WHERE guild_id = ?",
                (guild_id,),
            )
            channels = {r["event_key"]: r["channel_id"] for r in await cur.fetchall()}
        self._cache["events"][guild_id] = channels
        return channels

    async def set_event_channel(self, guild_id: int, event_key: str,
                                channel_id: Optional[int]):
        if event_key not in EVENTS:
            logger.error("Rejected channel override for unknown event: %s", event_key)
            return
        async with self._lock:
            if channel_id is None:
                await self._conn.execute(
                    "DELETE FROM event_channels WHERE guild_id = ? AND event_key = ?",
                    (guild_id, event_key),
                )
            else:
                await self._conn.execute(
                    "INSERT INTO event_channels (guild_id, event_key, channel_id) "
                    "VALUES (?, ?, ?) ON CONFLICT(guild_id, event_key) "
                    "DO UPDATE SET channel_id = excluded.channel_id",
                    (guild_id, event_key, channel_id),
                )
            await self._conn.commit()
        self._invalidate(guild_id, "events")

    async def get_toggles(self, guild_id: int) -> Dict[str, bool]:
        """Stored overrides layered over each event's default."""
        cached = self._cache["toggles"].get(guild_id)
        if cached is not None:
            return cached
        toggles = {key: spec[2] for key, spec in EVENTS.items()}
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT event_key, enabled FROM event_toggles WHERE guild_id = ?",
                (guild_id,),
            )
            for row in await cur.fetchall():
                if row["event_key"] in toggles:
                    toggles[row["event_key"]] = bool(row["enabled"])
        self._cache["toggles"][guild_id] = toggles
        return toggles

    async def set_toggle(self, guild_id: int, event_key: str, enabled: bool):
        if event_key not in EVENTS:
            return
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO event_toggles (guild_id, event_key, enabled) "
                "VALUES (?, ?, ?) ON CONFLICT(guild_id, event_key) "
                "DO UPDATE SET enabled = excluded.enabled",
                (guild_id, event_key, int(enabled)),
            )
            await self._conn.commit()
        self._invalidate(guild_id, "toggles")

    async def get_ignored(self, guild_id: int) -> Tuple[set, set]:
        cached = self._cache["ignored"].get(guild_id)
        if cached is not None:
            return cached
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT channel_id FROM ignored_channels WHERE guild_id = ?", (guild_id,)
            )
            channels = {r["channel_id"] for r in await cur.fetchall()}
            cur = await self._conn.execute(
                "SELECT user_id FROM ignored_users WHERE guild_id = ?", (guild_id,)
            )
            users = {r["user_id"] for r in await cur.fetchall()}
        self._cache["ignored"][guild_id] = (channels, users)
        return channels, users

    async def add_ignored(self, guild_id: int, kind: str, target_id: int):
        table = "ignored_channels" if kind == "channel" else "ignored_users"
        column = "channel_id" if kind == "channel" else "user_id"
        async with self._lock:
            await self._conn.execute(
                f"INSERT OR IGNORE INTO {table} (guild_id, {column}) VALUES (?, ?)",
                (guild_id, target_id),
            )
            await self._conn.commit()
        self._invalidate(guild_id, "ignored")

    async def remove_ignored(self, guild_id: int, kind: str, target_id: int):
        table = "ignored_channels" if kind == "channel" else "ignored_users"
        column = "channel_id" if kind == "channel" else "user_id"
        async with self._lock:
            await self._conn.execute(
                f"DELETE FROM {table} WHERE guild_id = ? AND {column} = ?",
                (guild_id, target_id),
            )
            await self._conn.commit()
        self._invalidate(guild_id, "ignored")

    # -- message mirror ------------------------------------------------

    def buffer_message(self, message: discord.Message):
        attachments = json.dumps([a.filename for a in message.attachments])
        self._message_buffer.append((
            message.id, message.guild.id, message.channel.id, message.author.id,
            message.content or "", attachments,
            int(message.created_at.timestamp()), None,
        ))

    async def flush_messages(self):
        if not self._message_buffer or self._conn is None:
            return
        rows, self._message_buffer = self._message_buffer, []
        async with self._lock:
            await self._conn.executemany(
                "INSERT OR REPLACE INTO message_cache "
                "(message_id, guild_id, channel_id, author_id, content, "
                " attachments, created_at, edited_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            await self._conn.commit()

    async def update_message(self, message_id: int, content: str):
        # The edited message may still be sitting in the buffer unflushed, so
        # push that through first or the update targets a row that isn't there.
        await self.flush_messages()
        async with self._lock:
            await self._conn.execute(
                "UPDATE message_cache SET content = ?, edited_at = ? WHERE message_id = ?",
                (content, int(time.time()), message_id),
            )
            await self._conn.commit()

    async def get_message(self, message_id: int) -> Optional[Dict[str, Any]]:
        await self.flush_messages()
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT * FROM message_cache WHERE message_id = ?", (message_id,)
            )
            row = await cur.fetchone()
        return dict(row) if row else None

    async def get_messages(self, message_ids) -> Dict[int, Dict[str, Any]]:
        ids = list(message_ids)
        if not ids:
            return {}
        await self.flush_messages()
        placeholders = ",".join("?" * len(ids))
        async with self._lock:
            cur = await self._conn.execute(
                f"SELECT * FROM message_cache WHERE message_id IN ({placeholders})", ids
            )
            return {r["message_id"]: dict(r) for r in await cur.fetchall()}

    async def purge_messages(self) -> int:
        cutoff = int(time.time()) - MESSAGE_RETENTION_SECONDS
        async with self._lock:
            cur = await self._conn.execute(
                "DELETE FROM message_cache WHERE created_at < ?", (cutoff,)
            )
            await self._conn.commit()
            return cur.rowcount or 0

    async def message_count(self) -> int:
        async with self._lock:
            cur = await self._conn.execute("SELECT COUNT(*) AS n FROM message_cache")
            row = await cur.fetchone()
            return row["n"] if row else 0

    # -- migration -----------------------------------------------------

    async def migrate_from_legacy(self):
        """Seed exit/mod channels from welcome_config.json on first run.

        cogs/welcome.py owned leave/kick/ban logging before this cog existed. Its
        config is the only place those channel IDs live, and re-picking them by
        hand after deploy would mean a window with no exit logging at all.
        """
        async with self._lock:
            cur = await self._conn.execute("SELECT COUNT(*) AS n FROM guild_settings")
            row = await cur.fetchone()
            if row and row["n"]:
                return  # already configured; never overwrite

        if not os.path.exists(LEGACY_CONFIG_FILE):
            return
        try:
            with open(LEGACY_CONFIG_FILE, "r", encoding="utf-8") as f:
                legacy = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error("Could not read %s for migration: %s", LEGACY_CONFIG_FILE, e)
            return

        for guild_id_str, gc in legacy.items():
            try:
                guild_id = int(guild_id_str)
            except ValueError:
                continue
            async with self._lock:
                await self._conn.execute(
                    "INSERT OR REPLACE INTO guild_settings "
                    "(guild_id, exit_channel_id, mod_channel_id) VALUES (?, ?, ?)",
                    (guild_id, gc.get("exit_channel_id"), gc.get("mod_channel_id")),
                )
                await self._conn.commit()
            self._invalidate(guild_id, "settings")
            logger.info("Seeded audit settings for guild %s from %s",
                        guild_id, LEGACY_CONFIG_FILE)


# ----------------------------------------------------------------------
# Batching
# ----------------------------------------------------------------------

@dataclass
class Actor:
    """Who caused a change, and whether the bot carried it out."""
    user: Optional[discord.abc.User] = None
    reason: Optional[str] = None
    via_bot: bool = False

    @property
    def id(self) -> Optional[int]:
        return self.user.id if self.user else None


@dataclass
class LogEvent:
    event_key: str
    guild_id: int
    target: Optional[discord.abc.User] = None
    actor: Actor = field(default_factory=Actor)
    data: Dict[str, Any] = field(default_factory=dict)

    @property
    def category(self) -> str:
        return EVENTS[self.event_key][0]


@dataclass
class Bucket:
    key: Tuple
    events: List[LogEvent] = field(default_factory=list)
    first_at: float = field(default_factory=time.monotonic)
    last_at: float = field(default_factory=time.monotonic)

    def add(self, event: LogEvent):
        self.events.append(event)
        self.last_at = time.monotonic()

    def ready(self, window: float) -> bool:
        now = time.monotonic()
        return (now - self.last_at >= window
                or now - self.first_at >= MAX_BATCH_HOLD_SECONDS)


# ----------------------------------------------------------------------
# The cog
# ----------------------------------------------------------------------

class AuditLog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = AuditDB()
        self._ready = asyncio.Event()

        self._buckets: Dict[Tuple, Bucket] = {}
        self._buckets_lock = asyncio.Lock()

        # (guild_id, action_value, target_id) -> (entry, monotonic_ts), filled by
        # on_audit_log_entry_create so most attributions never need to poll.
        self._actor_cache: Dict[Tuple, Tuple[discord.AuditLogEntry, float]] = {}

        # (guild, channel, author) -> (audit entry id, its delete count). Discord
        # coalesces repeated message deletions into one entry, so the count is
        # the only thing that moves on the second and later deletion.
        self._delete_counts: Dict[Tuple[int, int, int], Tuple[int, int]] = {}

        # Members who joined moments ago; their onboarding roles belong to the
        # join embed, not a separate role-change entry.
        self._recent_joins: Dict[int, float] = {}

        self.bot.loop.create_task(self._startup())

    async def _startup(self):
        try:
            await self.db.connect()
            await self.db.migrate_from_legacy()
        except Exception as e:
            logger.error("Audit log startup failed: %s", e, exc_info=True)
            await self._report(f"startup: {e}")
            return
        self._ready.set()
        self.flush_loop.start()
        self.purge_loop.start()
        logger.info("Audit log ready (db=%s)", DB_PATH)

    async def cog_unload(self):
        self.flush_loop.cancel()
        self.purge_loop.cancel()
        try:
            await self._flush_all()
        finally:
            await self.db.close()

    async def _report(self, message: str):
        reporter = getattr(self.bot, "error_reporter", None)
        if reporter is not None:
            try:
                await reporter.report("AuditLog", message)
            except Exception:
                pass

    def is_admin(self, member: discord.Member) -> bool:
        return bool(self.bot.is_bot_admin(member))

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    async def _resolve_channel(self, guild: discord.Guild,
                               channel_id: Optional[int]) -> Optional[discord.abc.Messageable]:
        if not channel_id:
            return None
        channel = guild.get_channel(channel_id)
        if channel is None:
            try:
                channel = await guild.fetch_channel(channel_id)
            except Exception:
                return None
        return channel

    async def _destinations(self, guild: discord.Guild,
                            event_key: str, data: Dict[str, Any]) -> List[discord.abc.Messageable]:
        """Which channels an event should be posted to.

        Leaves, kicks and bans keep the routing the server already had: every
        one of them to the exit channel, and kicks and bans additionally to the
        mod channel — except inactivity kicks, which are automated housekeeping
        rather than something a moderator needs to see twice.

        Everything else resolves event override → category → default, so a slot
        left unset simply inherits rather than dropping the event.
        """
        settings = await self.db.get_settings(guild.id)

        if event_key in EXIT_EVENTS or event_key in MOD_ONLY_EVENTS:
            targets = []
            if event_key in EXIT_EVENTS:
                exit_ch = await self._resolve_channel(guild, settings["exit_channel_id"])
                if exit_ch:
                    targets.append(exit_ch)
            wants_mod = (
                event_key in MOD_ONLY_EVENTS
                or (event_key in ("member_kick", "member_ban")
                    and not data.get("is_inactivity_kick"))
            )
            if wants_mod:
                mod_ch = await self._resolve_channel(guild, settings["mod_channel_id"])
                if mod_ch and mod_ch not in targets:
                    targets.append(mod_ch)
            return targets

        category = EVENTS[event_key][0]
        event_channels = await self.db.get_event_channels(guild.id)
        cat_channels = await self.db.get_category_channels(guild.id)
        channel_id = (event_channels.get(event_key)
                      or cat_channels.get(category)
                      or settings["default_channel_id"])
        channel = await self._resolve_channel(guild, channel_id)
        return [channel] if channel else []

    # ------------------------------------------------------------------
    # Event intake
    # ------------------------------------------------------------------

    async def _enabled(self, guild_id: int, event_key: str) -> bool:
        toggles = await self.db.get_toggles(guild_id)
        return toggles.get(event_key, False)

    def _ignore_scope(self, guild_id: int,
                      channel: Union[int, discord.abc.GuildChannel, discord.Thread, None]
                      ) -> Set[int]:
        """Every channel id an event in `channel` should be matched against.

        A thread carries its parent along: ignoring #general is meant to cover
        the threads hanging off it, not just the channel body. `channel` may be
        an object or a bare id out of a raw payload — ids resolve through the
        guild cache, which holds the threads the bot can see, and a miss simply
        leaves no parent to add.
        """
        if channel is None:
            return set()
        if isinstance(channel, int):
            channel_id = channel
            guild = self.bot.get_guild(guild_id)
            resolved = guild.get_channel_or_thread(channel_id) if guild else None
        else:
            channel_id = channel.id
            resolved = channel
        ids = {channel_id}
        if isinstance(resolved, discord.Thread) and resolved.parent_id:
            ids.add(resolved.parent_id)
        return ids

    async def _is_ignored(self, guild_id: int, *,
                          channel: Union[int, discord.abc.GuildChannel,
                                         discord.Thread, None] = None,
                          user_id: Optional[int] = None) -> bool:
        channels, users = await self.db.get_ignored(guild_id)
        if channels and channels & self._ignore_scope(guild_id, channel):
            return True
        if user_id is not None and user_id in users:
            return True
        return False

    async def submit(self, event: LogEvent):
        """Queue an event, merging it into any bucket already collecting.

        The bucket key deliberately includes the actor but not the target: a
        moderator stripping roles from one member and Vibey assigning a team
        role to a whole lobby are both single actions from the server's point of
        view, and both should come out as one embed.
        """
        if not self._ready.is_set():
            return
        if not await self._enabled(event.guild_id, event.event_key):
            return

        key = (event.guild_id, event.event_key, event.actor.id)
        async with self._buckets_lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = Bucket(key=key)
                self._buckets[key] = bucket
            bucket.add(event)

    @tasks.loop(seconds=2.0)
    async def flush_loop(self):
        try:
            await self.db.flush_messages()
        except Exception as e:
            logger.error("Message buffer flush failed: %s", e, exc_info=True)

        try:
            await self._flush_ready()
        except Exception as e:
            logger.error("Bucket flush failed: %s", e, exc_info=True)
            await self._report(f"flush_loop: {e}")

    @flush_loop.before_loop
    async def _before_flush(self):
        await self.bot.wait_until_ready()

    async def _flush_ready(self):
        # Work out the windows before taking the lock; submit() should never
        # wait behind a database read.
        async with self._buckets_lock:
            snapshot = list(self._buckets.items())
        windows: Dict[int, int] = {}
        for (guild_id, _, _), _bucket in snapshot:
            if guild_id not in windows:
                settings = await self.db.get_settings(guild_id)
                windows[guild_id] = settings["batch_seconds"] or DEFAULT_BATCH_SECONDS

        due: List[Bucket] = []
        async with self._buckets_lock:
            for key, bucket in snapshot:
                if bucket.ready(windows[key[0]]) and key in self._buckets:
                    due.append(self._buckets.pop(key))
        for bucket in due:
            await self._post_bucket(bucket)

    async def _flush_all(self):
        async with self._buckets_lock:
            due = list(self._buckets.values())
            self._buckets.clear()
        for bucket in due:
            try:
                await self._post_bucket(bucket)
            except Exception:
                pass

    async def _post_bucket(self, bucket: Bucket):
        if not bucket.events:
            return
        guild = self.bot.get_guild(bucket.key[0])
        if guild is None:
            return
        event_key = bucket.key[1]
        try:
            embed = self._render(guild, event_key, bucket.events)
            if embed is None:
                return
            destinations = await self._destinations(
                guild, event_key, bucket.events[0].data
            )
            for channel in destinations:
                try:
                    await channel.send(embed=embed)
                except discord.HTTPException as e:
                    logger.warning("Could not post %s to #%s: %s",
                                   event_key, getattr(channel, "name", "?"), e)
        except Exception as e:
            logger.error("Rendering %s failed: %s", event_key, e, exc_info=True)
            await self._report(f"render {event_key}: {e}")

    # ------------------------------------------------------------------
    # Attribution
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_audit_log_entry_create(self, entry: discord.AuditLogEntry):
        """Cache entries so attribution rarely has to poll the audit log."""
        target_id = getattr(entry.target, "id", None)
        key = (entry.guild.id, entry.action.value, target_id)
        self._actor_cache[key] = (entry, time.monotonic())

        # Cheap opportunistic sweep; the cache only needs to cover the seconds
        # between a gateway event and its audit entry.
        if len(self._actor_cache) > 256:
            cutoff = time.monotonic() - 60
            for k, (_, ts) in list(self._actor_cache.items()):
                if ts < cutoff:
                    self._actor_cache.pop(k, None)

    def _actor_from_entry(self, entry: discord.AuditLogEntry) -> Actor:
        user = entry.user
        via_bot = bool(user and self.bot.user and user.id == self.bot.user.id)
        return Actor(user=user, reason=entry.reason, via_bot=via_bot)

    async def _find_entry(self, guild: discord.Guild, action: discord.AuditLogAction,
                          target_id: Optional[int]) -> Optional[discord.AuditLogEntry]:
        key = (guild.id, action.value, target_id)
        cached = self._actor_cache.get(key)
        if cached and time.monotonic() - cached[1] < AUDIT_FRESHNESS_SECONDS:
            return cached[0]

        try:
            async for entry in guild.audit_logs(limit=5, action=action):
                entry_target = getattr(entry.target, "id", None)
                if target_id is not None and entry_target != target_id:
                    continue
                age = (discord.utils.utcnow() - entry.created_at).total_seconds()
                if age < AUDIT_FRESHNESS_SECONDS:
                    return entry
        except discord.Forbidden:
            pass  # no audit log permission; fall through unattributed
        except discord.HTTPException as e:
            logger.warning("Audit log lookup failed in %s: %s", guild.name, e)
        return None

    async def resolve_actor(self, guild: discord.Guild, action: discord.AuditLogAction,
                            target_id: Optional[int], *, settle: bool = True) -> Actor:
        """Work out who is responsible for a change.

        `settle` waits for the audit log entry to catch up with the gateway
        event, which is nearly always necessary — the two arrive out of order.
        """
        if settle:
            await asyncio.sleep(AUDIT_SETTLE_SECONDS)
        entry = await self._find_entry(guild, action, target_id)
        if entry is None:
            return Actor()
        return self._actor_from_entry(entry)

    async def resolve_message_deleter(self, guild: discord.Guild, channel_id: int,
                                      author_id: Optional[int]) -> Actor:
        """Decide whether a moderator deleted this message, or its author did.

        Discord writes no audit entry when someone deletes their own message, so
        an absent entry is meaningful rather than a failure. But it also
        *coalesces*: deleting several messages from the same author in the same
        channel bumps `extra.count` on the existing entry instead of writing a
        new one, and no gateway event fires for that bump.

        So neither the entry's existence nor its age can identify the second
        deletion in a spam clear-out — only the count moving can. Tracking it
        per (channel, author) is what keeps a moderator's fourth deletion from
        being reported as the author deleting their own message.
        """
        if author_id is None:
            return Actor()

        await asyncio.sleep(AUDIT_SETTLE_SECONDS)
        key = (guild.id, channel_id, author_id)
        seen_id, seen_count = self._delete_counts.get(key, (None, 0))

        try:
            async for entry in guild.audit_logs(
                limit=5, action=discord.AuditLogAction.message_delete
            ):
                if getattr(entry.target, "id", None) != author_id:
                    continue
                extra_channel = getattr(entry.extra, "channel", None)
                if extra_channel is not None and extra_channel.id != channel_id:
                    continue

                count = getattr(entry.extra, "count", 0) or 0
                if entry.id != seen_id:
                    # First sighting. Only credit it if it is actually recent —
                    # otherwise it belongs to some earlier clear-out and we are
                    # just recording the baseline count.
                    self._delete_counts[key] = (entry.id, count)
                    age = (discord.utils.utcnow() - entry.created_at).total_seconds()
                    return (self._actor_from_entry(entry)
                            if age < AUDIT_FRESHNESS_SECONDS else Actor())
                if count > seen_count:
                    self._delete_counts[key] = (entry.id, count)
                    return self._actor_from_entry(entry)
                return Actor()  # entry unchanged, so this was a self-delete
        except discord.Forbidden:
            pass
        except discord.HTTPException as e:
            logger.warning("Message delete lookup failed in %s: %s", guild.name, e)
        return Actor()

    def _footer_text(self, events: List[LogEvent]) -> Optional[str]:
        """Attribution line.

        A bot-driven change to a single member is credited to that member with
        Vibey noted after it — the member clicked something, Vibey executed it.
        A bot-driven change spanning several members has no single initiator, so
        it is simply Vibey.
        """
        actor = events[0].actor
        targets = {e.target.id: e.target for e in events if e.target is not None}

        if actor.user is None:
            base = None
        elif actor.via_bot:
            if len(targets) == 1:
                only = next(iter(targets.values()))
                base = f"{only} (Vibey)"
            else:
                base = "Vibey"
        else:
            base = str(actor.user)

        reasons = [e.actor.reason for e in events if e.actor.reason]
        if base and reasons:
            return truncate(f"{base} — {reasons[0]}", 2048)
        return base

    def _apply_identity(self, embed: discord.Embed, events: List[LogEvent],
                        header: Optional[str]):
        """Put the subject's avatar in the header and top right.

        Only meaningful when the batch is about one person; a bundle covering
        several members gets a plain header instead. Pass ``header=None`` where
        the action is stated in the footer and the header is just the name.
        """
        targets = {e.target.id: e.target for e in events if e.target is not None}
        if len(targets) == 1:
            user = next(iter(targets.values()))
            avatar = getattr(user, "display_avatar", None)
            name = getattr(user, "display_name", None) or str(user)
            if avatar is not None:
                embed.set_author(name=f"{name} — {header}" if header else name,
                                 icon_url=avatar.url)
                embed.set_thumbnail(url=avatar.url)
                return
        embed.set_author(name=header or "")

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render(self, guild: discord.Guild, event_key: str,
                events: List[LogEvent]) -> Optional[discord.Embed]:
        renderer = getattr(self, f"_render_{event_key}", None)
        if renderer is None:
            logger.warning("No renderer for %s", event_key)
            return None
        embed = renderer(guild, events)
        if embed is None:
            return None
        # A renderer that built its own footer has said everything it needs to;
        # the generic attribution line would only duplicate it.
        if not embed.footer.text:
            footer = self._footer_text(events)
            if footer:
                embed.set_footer(text=footer)
        if event_key not in NO_TIMESTAMP_EVENTS:
            embed.timestamp = discord.utils.utcnow()
        return embed

    # -- messages ------------------------------------------------------

    def _render_message_delete(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_DELETE)
        self._apply_identity(embed, events, "Message deleted")

        lines = []
        for event in events[:10]:
            d = event.data
            where = f"<#{d['channel_id']}>"
            author = f"<@{d['author_id']}>" if d.get("author_id") else "unknown author"
            content = d.get("content") or "*no text content*"
            lines.append(f"{author} in {where}\n{truncate(content, 900)}")
            if d.get("attachments"):
                lines[-1] += f"\n*Attachments: {', '.join(d['attachments'])}*"
        if len(events) > 10:
            lines.append(f"*…and {len(events) - 10} more*")
        embed.description = truncate("\n\n".join(lines), 4096)
        return embed

    def _render_message_edit(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        self._apply_identity(embed, events, "Message edited")

        # Only the newest edit of a given message matters; intermediate states
        # in a rapid edit burst are noise.
        latest: Dict[int, LogEvent] = {}
        for event in events:
            latest[event.data["message_id"]] = event

        # An embed is capped at 6000 characters in total, not just per field, so
        # several long edits in one batch have to share a budget or Discord
        # rejects the whole thing.
        budget = 5000
        header = []
        shown = 0
        for event in latest.values():
            share = min(1024, (budget - 40) // 2)
            if share < 120:
                break
            d = event.data
            header.append(
                f"<@{d['author_id']}> in <#{d['channel_id']}> — [jump]({d['jump_url']})"
            )
            before = truncate(d["before"] or "*empty*", share)
            after = truncate(d["after"] or "*empty*", share)
            embed.add_field(name="Before", value=before, inline=False)
            embed.add_field(name="After", value=after, inline=False)
            budget -= len(before) + len(after) + len(header[-1]) + 20
            shown += 1
        if len(latest) > shown:
            header.append(f"*…and {len(latest) - shown} more edits*")
        embed.description = truncate("\n".join(header), max(0, budget))
        return embed

    def _render_message_bulk_delete(self, guild, events) -> Optional[discord.Embed]:
        total = sum(e.data["count"] for e in events)
        embed = discord.Embed(color=COLOR_DELETE)
        self._apply_identity(embed, events, "Bulk delete")

        lines = [f"**{total}** messages deleted in <#{events[0].data['channel_id']}>"]
        shown = 0
        for event in events:
            for msg in event.data.get("messages", []):
                if shown >= 15:
                    break
                lines.append(f"<@{msg['author_id']}>: {truncate(msg['content'] or '*no text*', 200)}")
                shown += 1
        recovered = sum(len(e.data.get("messages", [])) for e in events)
        if recovered > shown:
            lines.append(f"*…and {recovered - shown} more*")
        if total > recovered:
            lines.append(f"*{total - recovered} were not in the message cache*")
        embed.description = truncate("\n".join(lines), 4096)
        return embed

    # -- members -------------------------------------------------------

    def _render_member_join(self, guild, events) -> Optional[discord.Embed]:
        event = events[0]
        member = event.target
        d = event.data
        embed = discord.Embed(
            description=f"{member.name}\n<@{member.id}>",
            color=COLOR_JOIN,
        )
        self._apply_identity(embed, events, "Joined")
        embed.set_footer(text=f"Account created {fmt_account_age(member.created_at)}")

        # Every role they hold once onboarding and any on-join grant have
        # settled — onboarding picks, the newcomer role, an invite role — rather
        # than onboarding alone, since they all arrive in the same moment.
        roles = d.get("roles") or []
        if roles:
            embed.add_field(
                name=f"Roles on join ({len(roles)})",
                value=truncate(", ".join(roles), 1024),
                inline=False,
            )
        return embed

    def _render_member_leave(self, guild, events) -> Optional[discord.Embed]:
        return self._render_exit(events, COLOR_LEAVE, "Left")

    def _render_member_kick(self, guild, events) -> Optional[discord.Embed]:
        d = events[0].data
        color = INACTIVITY_KICK_COLOR if d.get("is_inactivity_kick") else COLOR_KICK
        return self._render_exit(events, color, "Kicked")

    def _render_member_ban(self, guild, events) -> Optional[discord.Embed]:
        return self._render_exit(events, COLOR_BAN, "Banned")

    def _render_exit(self, events, color, action) -> discord.Embed:
        """Name, avatar and colour in the body; everything else in the footer.

        How long they were here and who removed them are the details a
        moderator scans past, not the headline, so they sit on one footer line.
        Account age belongs to joining, not leaving.
        """
        event = events[0]
        member = event.target
        embed = discord.Embed(
            description=f"{member.name}\n<@{member.id}>",
            color=color,
        )
        self._apply_identity(embed, events, None)

        actor = event.actor
        if actor.user is not None:
            line = f"{action} by {actor.user}"
            if actor.reason:
                line += f" — {actor.reason}"
        else:
            line = action

        tenure = event.data.get("tenure_seconds")
        if tenure is not None:
            line += f" — {fmt_duration(tenure)} in server"
        embed.set_footer(text=truncate(line, 2048))
        return embed

    def _render_member_unban(self, guild, events) -> Optional[discord.Embed]:
        user = events[0].target
        embed = discord.Embed(description=f"{user.name}\n<@{user.id}>", color=COLOR_UNBAN)
        self._apply_identity(embed, events, "Unbanned")
        return embed

    def _render_member_timeout(self, guild, events) -> Optional[discord.Embed]:
        event = events[-1]
        member = event.target
        embed = discord.Embed(description=f"{member.name}\n<@{member.id}>",
                              color=COLOR_TIMEOUT)
        self._apply_identity(embed, events, "Timed out")
        until = event.data.get("until")
        if until is not None:
            embed.add_field(name="Duration",
                            value=fmt_duration(event.data.get("duration", 0)), inline=True)
            embed.add_field(name="Expires",
                            value=discord.utils.format_dt(until, "R"), inline=True)
        return embed

    def _render_nickname_change(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        self._apply_identity(embed, events, "Nickname changed")
        lines = []
        for event in events[:10]:
            before = event.data["before"] or "*none*"
            after = event.data["after"] or "*none*"
            lines.append(f"<@{event.target.id}>\n{before} → **{after}**")
        embed.description = truncate("\n\n".join(lines), 4096)
        return embed

    def _render_username_change(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        self._apply_identity(embed, events, "Username changed")
        event = events[-1]
        embed.description = (f"<@{event.target.id}>\n"
                             f"{event.data['before']} → **{event.data['after']}**")
        return embed

    def _render_avatar_change(self, guild, events) -> Optional[discord.Embed]:
        event = events[-1]
        embed = discord.Embed(description=f"<@{event.target.id}>", color=COLOR_NEUTRAL)
        self._apply_identity(embed, events, "Avatar changed")
        # The thumbnail is the whole point of this entry, so pin it to the new
        # avatar explicitly rather than trusting whatever the cached member
        # object happens to be carrying by the time the batch renders.
        embed.set_thumbnail(url=event.data["after"])
        if event.data.get("before"):
            embed.add_field(name="Previous", value=f"[old avatar]({event.data['before']})")
        return embed

    # -- roles ---------------------------------------------------------

    def _render_member_roles(self, guild, events) -> Optional[discord.Embed]:
        """One entry per member, all members in one embed.

        This is the case the whole batching design exists for: a custom match
        hands a team role to every player in the lobby and takes it back at the
        end, which is one action, not twenty.
        """
        per_member: Dict[int, Dict[str, List[str]]] = {}
        order: List[discord.abc.User] = []
        for event in events:
            uid = event.target.id
            if uid not in per_member:
                per_member[uid] = {"gained": [], "lost": []}
                order.append(event.target)
            entry = per_member[uid]
            for name in event.data["gained"]:
                if name in entry["lost"]:
                    entry["lost"].remove(name)
                elif name not in entry["gained"]:
                    entry["gained"].append(name)
            for name in event.data["lost"]:
                if name in entry["gained"]:
                    entry["gained"].remove(name)
                elif name not in entry["lost"]:
                    entry["lost"].append(name)

        # A role added and removed inside the same window nets out to nothing.
        order = [u for u in order
                 if per_member[u.id]["gained"] or per_member[u.id]["lost"]]
        if not order:
            return None

        # Group members by the change they saw. A team role handed to a whole
        # lobby is then one line naming the role once and everyone who got it,
        # instead of the same role name repeated down the embed.
        groups: Dict[Tuple[Tuple[str, ...], Tuple[str, ...]], List] = {}
        for user in order:
            entry = per_member[user.id]
            signature = (tuple(entry["gained"]), tuple(entry["lost"]))
            groups.setdefault(signature, []).append(user)

        any_gained = any(gained for gained, _ in groups)
        any_lost = any(lost for _, lost in groups)
        if any_gained and any_lost:
            header, color = "Roles changed", COLOR_NEUTRAL
        elif any_gained:
            header, color = "Roles added", COLOR_ROLE_ADD
        else:
            header, color = "Roles removed", COLOR_ROLE_REMOVE

        embed = discord.Embed(color=color)

        if len(order) == 1:
            # One member: name them in the header and let the fields carry the
            # two directions separately.
            gained, lost = next(iter(groups))
            self._apply_identity(embed, [LogEvent("member_roles", guild.id, order[0])],
                                 header)
            embed.description = f"<@{order[0].id}>"
            if gained:
                embed.add_field(name=f"Added ({len(gained)})",
                                value=truncate("\n".join(gained), 1024), inline=True)
            if lost:
                embed.add_field(name=f"Removed ({len(lost)})",
                                value=truncate("\n".join(lost), 1024), inline=True)
            return embed

        embed.set_author(name=f"{header} ({len(order)} members)")

        # Field values share a budget — 25 fields at the 1024 limit would be
        # four times what an embed can carry in total.
        budget = 5000
        shown_groups = 0
        for (gained, lost), members in groups.items():
            if shown_groups >= 20 or budget < 200:
                break
            parts = []
            if gained:
                parts.append(f"Added: {', '.join(gained)}")
            if lost:
                parts.append(f"Removed: {', '.join(lost)}")
            name = truncate(" · ".join(parts), 256)

            mentions = [f"<@{u.id}>" for u in members]
            value = ", ".join(mentions)
            if len(value) > min(1024, budget):
                keep = []
                for mention in mentions:
                    if len(", ".join(keep + [mention])) > min(1024, budget) - 20:
                        break
                    keep.append(mention)
                value = ", ".join(keep) + f" *…and {len(mentions) - len(keep)} more*"
            embed.add_field(name=name, value=value or "*none*", inline=False)
            budget -= len(value) + len(name)
            shown_groups += 1

        if shown_groups < len(groups):
            embed.add_field(name="​",
                            value=f"*…and {len(groups) - shown_groups} more changes*",
                            inline=False)
        return embed

    def _render_role_update(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        embed.set_author(name="Role updated")
        lines = []
        for event in events[:10]:
            d = event.data
            changes = []
            if d.get("name_before") != d.get("name_after"):
                changes.append(f"name: {d['name_before']} → **{d['name_after']}**")
            if d.get("color_before") != d.get("color_after"):
                changes.append(f"colour: {d['color_before']} → **{d['color_after']}**")
            if d.get("hoist_changed"):
                changes.append(f"displayed separately: **{d['hoist_after']}**")
            if d.get("mentionable_changed"):
                changes.append(f"mentionable: **{d['mentionable_after']}**")
            if changes:
                lines.append(f"<@&{d['role_id']}>\n" + "\n".join(changes))
        if not lines:
            return None
        embed.description = truncate("\n\n".join(lines), 4096)
        return embed

    def _render_role_permissions(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        embed.set_author(name="Role permissions changed")

        # Collapse repeated edits to the same role into a single net diff.
        merged: Dict[int, Dict[str, List[str]]] = {}
        for event in events:
            d = event.data
            entry = merged.setdefault(d["role_id"], {"granted": [], "revoked": []})
            for name in d["granted"]:
                if name in entry["revoked"]:
                    entry["revoked"].remove(name)
                elif name not in entry["granted"]:
                    entry["granted"].append(name)
            for name in d["revoked"]:
                if name in entry["granted"]:
                    entry["granted"].remove(name)
                elif name not in entry["revoked"]:
                    entry["revoked"].append(name)

        lines = []
        for role_id, entry in merged.items():
            if not entry["granted"] and not entry["revoked"]:
                continue
            parts = [f"<@&{role_id}>"]
            if entry["granted"]:
                parts.append("+ " + ", ".join(entry["granted"]))
            if entry["revoked"]:
                parts.append("- " + ", ".join(entry["revoked"]))
            lines.append("\n".join(parts))
        if not lines:
            return None
        embed.description = truncate("\n\n".join(lines), 4096)
        return embed

    # -- server --------------------------------------------------------

    def _render_channel_name(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        embed.set_author(name="Channel renamed")
        lines = []
        for event in events[:10]:
            d = event.data
            lines.append(f"<#{d['channel_id']}>\n{d['before']} → **{d['after']}**")
        embed.description = truncate("\n\n".join(lines), 4096)
        return embed

    def _render_channel_permissions(self, guild, events) -> Optional[discord.Embed]:
        embed = discord.Embed(color=COLOR_NEUTRAL)
        embed.set_author(name="Channel permissions changed")
        lines = []
        for event in events[:8]:
            d = event.data
            parts = [f"<#{d['channel_id']}> — {d['target_label']}"]
            if d["allowed"]:
                parts.append("+ " + ", ".join(d["allowed"]))
            if d["denied"]:
                parts.append("- " + ", ".join(d["denied"]))
            if d["cleared"]:
                parts.append("= " + ", ".join(d["cleared"]) + " (inherit)")
            if len(parts) > 1:
                lines.append("\n".join(parts))
        if not lines:
            return None
        if len(events) > 8:
            lines.append(f"*…and {len(events) - 8} more*")
        embed.description = truncate("\n\n".join(lines), 4096)
        return embed

    # ------------------------------------------------------------------
    # Listeners — messages
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Mirror messages so deletes and edits can show real content later."""
        if not self._ready.is_set() or message.guild is None:
            return
        if await self._is_ignored(message.guild.id, channel=message.channel,
                                  user_id=message.author.id):
            return
        if message.author.bot:
            settings = await self.db.get_settings(message.guild.id)
            if not settings["log_bot_messages"]:
                return
        self.db.buffer_message(message)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        """Raw, so edits to messages older than the library cache still log.

        on_message_edit only fires while the message sits in discord.py's
        in-memory cache, which holds a fixed number of recent messages — nothing
        like the 48h the mirror keeps. Editing anything older would log nothing
        at all, even though the original text is in our own table.
        """
        if not self._ready.is_set() or payload.guild_id is None:
            return
        # MESSAGE_UPDATE also fires for link previews resolving, pins and
        # attachment changes. Only a payload carrying content is a real edit.
        if "content" not in payload.data:
            return

        after = payload.message
        if await self._is_ignored(payload.guild_id, channel=payload.channel_id,
                                  user_id=after.author.id):
            return
        if after.author.bot:
            settings = await self.db.get_settings(payload.guild_id)
            if not settings["log_bot_messages"]:
                return

        stored = await self.db.get_message(payload.message_id)
        if stored is not None:
            before_content = stored["content"]
        elif payload.cached_message is not None:
            before_content = payload.cached_message.content
        else:
            # Past the retention window, or never mirrored. Showing an "after"
            # with no "before" is not worth an entry.
            return

        if before_content == after.content:
            return

        await self.db.update_message(payload.message_id, after.content or "")
        await self.submit(LogEvent(
            event_key="message_edit",
            guild_id=payload.guild_id,
            target=after.author,
            data={
                "message_id": payload.message_id,
                "channel_id": payload.channel_id,
                "author_id": after.author.id,
                "before": before_content,
                "after": after.content,
                "jump_url": after.jump_url,
            },
        ))

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Raw, so deletes of messages the bot never cached still resolve."""
        if not self._ready.is_set() or payload.guild_id is None:
            return
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        if await self._is_ignored(payload.guild_id, channel=payload.channel_id):
            return

        stored = await self.db.get_message(payload.message_id)
        cached = payload.cached_message

        if stored is None and cached is None:
            # Neither our mirror nor discord.py's cache has it. In practice that
            # means a bot's own panel message — several cogs delete and repost
            # those constantly — or something past the retention window. An
            # "unknown message deleted" entry for that is noise, not signal.
            return

        author = cached.author if cached is not None else None
        author_id = (stored["author_id"] if stored else None) or \
                    (author.id if author is not None else None)

        if author is not None and author.bot:
            settings = await self.db.get_settings(payload.guild_id)
            if not settings["log_bot_messages"]:
                return
        if author_id and await self._is_ignored(payload.guild_id, user_id=author_id):
            return

        target = None
        if author_id:
            target = guild.get_member(author_id) or self.bot.get_user(author_id) or author

        actor = await self.resolve_message_deleter(
            guild, payload.channel_id, author_id
        )
        if actor.user is None and target is not None:
            # No moderator entry, so the author took it down themselves.
            actor = Actor(user=target)

        if stored is not None:
            content = stored["content"]
            attachments = json.loads(stored["attachments"]) if stored["attachments"] else []
        else:
            content = cached.content
            attachments = [a.filename for a in cached.attachments]

        await self.submit(LogEvent(
            event_key="message_delete",
            guild_id=payload.guild_id,
            target=target,
            actor=actor,
            data={
                "channel_id": payload.channel_id,
                "author_id": author_id,
                "content": content,
                "attachments": attachments,
            },
        ))

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        if not self._ready.is_set() or payload.guild_id is None:
            return
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        if await self._is_ignored(payload.guild_id, channel=payload.channel_id):
            return

        stored = await self.db.get_messages(payload.message_ids)
        actor = await self.resolve_actor(
            guild, discord.AuditLogAction.message_bulk_delete, payload.channel_id
        )
        await self.submit(LogEvent(
            event_key="message_bulk_delete",
            guild_id=payload.guild_id,
            actor=actor,
            data={
                "channel_id": payload.channel_id,
                "count": len(payload.message_ids),
                "messages": [
                    {"author_id": row["author_id"], "content": row["content"]}
                    for row in stored.values()
                ],
            },
        ))

    # ------------------------------------------------------------------
    # Listeners — members
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if not self._ready.is_set() or member.bot:
            return
        if await self._is_ignored(member.guild.id, user_id=member.id):
            return
        self._recent_joins[member.id] = time.monotonic()
        asyncio.create_task(self._log_join(member))

    async def _log_join(self, member: discord.Member):
        """Wait for onboarding and any on-join cog grants to settle so every role
        the member ends up with lands in the join embed."""
        await asyncio.sleep(JOIN_SETTLE_SECONDS)
        try:
            try:
                fresh = member.guild.get_member(member.id)
                if fresh is None:
                    fresh = await member.guild.fetch_member(member.id)
            except discord.HTTPException:
                fresh = member  # left again already, or unfetchable

            roles = [r.name for r in getattr(fresh, "roles", []) if not r.is_default()]
        finally:
            # Release the member before submitting: from here on their role
            # changes are ordinary edits and get their own embed. In a finally
            # so a failure above can't mute them until the TTL sweep.
            self._recent_joins.pop(member.id, None)

        await self.submit(LogEvent(
            event_key="member_join",
            guild_id=member.guild.id,
            target=fresh,
            data={"roles": roles},
        ))

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Leave, kick or ban — the audit log is the only way to tell them apart."""
        if not self._ready.is_set() or member.bot:
            return
        self._recent_joins.pop(member.id, None)
        if await self._is_ignored(member.guild.id, user_id=member.id):
            return

        tenure = None
        if member.joined_at is not None:
            tenure = int((discord.utils.utcnow() - member.joined_at).total_seconds())

        # Ban is checked first: a ban also fires on_member_remove, and a kick
        # lookup would otherwise mislabel it.
        actor = await self.resolve_actor(
            member.guild, discord.AuditLogAction.ban, member.id
        )
        if actor.user is not None:
            await self.submit(LogEvent(
                event_key="member_ban", guild_id=member.guild.id,
                target=member, actor=actor, data={"tenure_seconds": tenure},
            ))
            return

        actor = await self.resolve_actor(
            member.guild, discord.AuditLogAction.kick, member.id, settle=False
        )
        if actor.user is not None:
            data = {"tenure_seconds": tenure}
            # cogs/inactivity.py kicks with "Inactivity | admin:<id>" so the
            # real admin can be credited instead of the bot.
            if actor.reason and actor.reason.startswith("Inactivity | admin:"):
                data["is_inactivity_kick"] = True
                try:
                    admin_id = int(actor.reason.split("admin:")[1])
                    admin = (member.guild.get_member(admin_id)
                             or await member.guild.fetch_member(admin_id))
                    if admin:
                        actor.user = admin
                        actor.via_bot = False
                except (ValueError, IndexError, discord.HTTPException):
                    pass
                actor.reason = "Inactivity"
            await self.submit(LogEvent(
                event_key="member_kick", guild_id=member.guild.id,
                target=member, actor=actor, data=data,
            ))
            return

        await self.submit(LogEvent(
            event_key="member_leave", guild_id=member.guild.id,
            target=member, data={"tenure_seconds": tenure},
        ))

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        if not self._ready.is_set():
            return
        if await self._is_ignored(guild.id, user_id=user.id):
            return
        actor = await self.resolve_actor(guild, discord.AuditLogAction.unban, user.id)
        await self.submit(LogEvent(
            event_key="member_unban", guild_id=guild.id, target=user, actor=actor,
        ))

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if not self._ready.is_set() or after.bot:
            return
        if await self._is_ignored(after.guild.id, user_id=after.id):
            return

        if before.nick != after.nick:
            actor = await self.resolve_actor(
                after.guild, discord.AuditLogAction.member_update, after.id
            )
            await self.submit(LogEvent(
                event_key="nickname_change", guild_id=after.guild.id,
                target=after, actor=actor,
                data={"before": before.nick, "after": after.nick},
            ))

        if not before.timed_out_until and after.timed_out_until:
            actor = await self.resolve_actor(
                after.guild, discord.AuditLogAction.member_update, after.id
            )
            remaining = (after.timed_out_until - discord.utils.utcnow()).total_seconds()
            await self.submit(LogEvent(
                event_key="member_timeout", guild_id=after.guild.id,
                target=after, actor=actor,
                data={"until": after.timed_out_until,
                      "duration": max(0, int(remaining))},
            ))

        if before.roles != after.roles:
            await self._log_role_change(before, after)

    async def _log_role_change(self, before: discord.Member, after: discord.Member):
        # Still tracked means their join embed hasn't snapshotted yet, so these
        # roles — onboarding picks, the newcomer role, an invite role — will be
        # listed there. _log_join stops tracking them the moment it snapshots,
        # so anything later is a real edit and falls through to its own embed.
        if after.id in self._recent_joins:
            return

        before_ids = {r.id for r in before.roles}
        after_ids = {r.id for r in after.roles}
        gained = [r.name for r in after.roles if r.id not in before_ids]
        lost = [r.name for r in before.roles if r.id not in after_ids]
        if not gained and not lost:
            return

        actor = await self.resolve_actor(
            after.guild, discord.AuditLogAction.member_role_update, after.id
        )
        await self.submit(LogEvent(
            event_key="member_roles", guild_id=after.guild.id,
            target=after, actor=actor,
            data={"gained": gained, "lost": lost},
        ))

    @commands.Cog.listener()
    async def on_user_update(self, before: discord.User, after: discord.User):
        """Global profile changes — fired once, for every guild the user is in."""
        if not self._ready.is_set() or after.bot:
            return
        for guild in self.bot.guilds:
            member = guild.get_member(after.id)
            if member is None:
                continue
            if await self._is_ignored(guild.id, user_id=after.id):
                continue

            if before.name != after.name:
                await self.submit(LogEvent(
                    event_key="username_change", guild_id=guild.id, target=member,
                    data={"before": before.name, "after": after.name},
                ))
            if before.display_avatar.url != after.display_avatar.url:
                await self.submit(LogEvent(
                    event_key="avatar_change", guild_id=guild.id, target=member,
                    data={"before": before.display_avatar.url,
                          "after": after.display_avatar.url},
                ))

    # ------------------------------------------------------------------
    # Listeners — roles and channels
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        if not self._ready.is_set():
            return

        granted, revoked = diff_permissions(before.permissions, after.permissions)
        if granted or revoked:
            actor = await self.resolve_actor(
                after.guild, discord.AuditLogAction.role_update, after.id
            )
            await self.submit(LogEvent(
                event_key="role_permissions", guild_id=after.guild.id, actor=actor,
                data={"role_id": after.id, "granted": granted, "revoked": revoked},
            ))

        cosmetic = (before.name != after.name or before.color != after.color
                    or before.hoist != after.hoist
                    or before.mentionable != after.mentionable)
        if cosmetic:
            actor = await self.resolve_actor(
                after.guild, discord.AuditLogAction.role_update, after.id, settle=False
            )
            await self.submit(LogEvent(
                event_key="role_update", guild_id=after.guild.id, actor=actor,
                data={
                    "role_id": after.id,
                    "name_before": before.name, "name_after": after.name,
                    "color_before": str(before.color), "color_after": str(after.color),
                    "hoist_changed": before.hoist != after.hoist,
                    "hoist_after": after.hoist,
                    "mentionable_changed": before.mentionable != after.mentionable,
                    "mentionable_after": after.mentionable,
                },
            ))

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel,
                                      after: discord.abc.GuildChannel):
        if not self._ready.is_set():
            return
        if await self._is_ignored(after.guild.id, channel=after):
            return

        if before.name != after.name:
            actor = await self.resolve_actor(
                after.guild, discord.AuditLogAction.channel_update, after.id
            )
            await self.submit(LogEvent(
                event_key="channel_name", guild_id=after.guild.id, actor=actor,
                data={"channel_id": after.id,
                      "before": before.name, "after": after.name},
            ))

        if before.overwrites != after.overwrites:
            await self._log_overwrites(before, after)

    async def _log_overwrites(self, before, after):
        """One event per affected role/member, batched back together on render."""
        actor = await self.resolve_actor(
            after.guild, discord.AuditLogAction.overwrite_update, after.id
        )
        targets = set(before.overwrites) | set(after.overwrites)
        for target in targets:
            allowed, denied, cleared = diff_overwrite(
                before.overwrites.get(target), after.overwrites.get(target)
            )
            if not (allowed or denied or cleared):
                continue
            if isinstance(target, discord.Role):
                label = f"<@&{target.id}>"
            else:
                label = f"<@{target.id}>"
            await self.submit(LogEvent(
                event_key="channel_permissions", guild_id=after.guild.id, actor=actor,
                data={"channel_id": after.id, "target_label": label,
                      "allowed": allowed, "denied": denied, "cleared": cleared},
            ))

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    @tasks.loop(hours=1)
    async def purge_loop(self):
        try:
            removed = await self.db.purge_messages()
            if removed:
                logger.info("Purged %s expired cached messages", removed)
        except Exception as e:
            logger.error("Message purge failed: %s", e, exc_info=True)

        cutoff = time.monotonic() - JOIN_TRACK_TTL_SECONDS
        for uid, ts in list(self._recent_joins.items()):
            if ts < cutoff:
                self._recent_joins.pop(uid, None)

        # Coalescing only groups deletions made close together, so hourly is far
        # longer than any run this needs to span.
        self._delete_counts.clear()

    @purge_loop.before_loop
    async def _before_purge(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------------------
    # Command
    # ------------------------------------------------------------------

    @app_commands.command(name="audit_panel",
                          description="Admin: configure server audit logging")
    @app_commands.guild_only()
    async def audit_panel(self, interaction: discord.Interaction):
        if not self.is_admin(interaction.user):
            await interaction.response.send_message(
                "You need to be a bot admin to use this.", ephemeral=True
            )
            return
        if not self._ready.is_set():
            await interaction.response.send_message(
                "The audit log is still starting up — try again in a moment.",
                ephemeral=True,
            )
            return
        await AuditPanelHome(self).open(interaction)


# ----------------------------------------------------------------------
# Panel
# ----------------------------------------------------------------------

class ExpiringView(discord.ui.View):
    """A view that dies visibly instead of silently going dead.

    discord.py stops dispatching once the timeout elapses but the components on
    screen still look live, so every later click lands on nothing and the user
    sees "Vibey didn't respond in time" — which reads as an outage rather than
    an expired panel. Grey the components out instead, and keep the clock
    restarted while the panel is actually being used.
    """

    expiry_note = "\n\n*This panel expired — re-open it with /audit_panel.*"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message: Optional[discord.Message] = None

    async def track(self, interaction: discord.Interaction):
        try:
            self.message = await interaction.original_response()
        except discord.HTTPException:
            self.message = None

    async def on_timeout(self):
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True
        if self.message is None:
            return
        try:
            await self.message.edit(
                content=(self.message.content or "") + self.expiry_note, view=self
            )
        except discord.HTTPException:
            pass

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        logger.error("Audit panel error on %s: %s", item, error, exc_info=True)
        cog = getattr(self, "cog", None)
        if cog is not None:
            await cog._report(f"panel {type(item).__name__}: {error}")
        try:
            msg = "Something went wrong — it has been logged."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


class BackButton(discord.ui.Button):
    def __init__(self, parent: 'AuditPage', row: int = 4):
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.parent_page = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_page.render(interaction)


class AuditPage(ExpiringView):
    """One screen of /audit_panel.

    Subclasses declare their components and override build_embed. Passing a
    parent adds a Back button and keeps the whole ancestor chain's timeout alive,
    so a parent can't quietly expire underneath a long editing session.
    """

    back_row = 4

    def __init__(self, cog: AuditLog, parent: Optional['AuditPage'] = None,
                 timeout: float = 300):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.parent_page = parent
        if parent is not None:
            self.add_item(BackButton(parent, row=self.back_row))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        page = self
        while page is not None:
            page._refresh_timeout()
            page = getattr(page, "parent_page", None)
        if self.cog.is_admin(interaction.user):
            return True
        await interaction.response.send_message(
            "You no longer have permission to use this panel.", ephemeral=True
        )
        return False

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        raise NotImplementedError

    async def rebuild(self, guild: discord.Guild):
        """Rebuild dynamic components. Override where the layout is data-driven."""
        return

    async def render(self, interaction: discord.Interaction, *,
                     flash: Optional[str] = None):
        await self.rebuild(interaction.guild)
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = False
        embed = await self.build_embed(interaction.guild)
        if flash:
            embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        await interaction.response.edit_message(embed=embed, view=self)
        self.message = interaction.message

    async def open(self, interaction: discord.Interaction):
        await self.rebuild(interaction.guild)
        embed = await self.build_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, view=self, ephemeral=True)
        await self.track(interaction)

    def _restore_back(self):
        if self.parent_page is not None:
            self.add_item(BackButton(self.parent_page, row=self.back_row))


class AuditPanelHome(AuditPage):

    def __init__(self, cog: AuditLog):
        super().__init__(cog, parent=None, timeout=600)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        settings = await self.cog.db.get_settings(guild.id)
        toggles = await self.cog.db.get_toggles(guild.id)
        ignored_channels, ignored_users = await self.cog.db.get_ignored(guild.id)
        on = sum(1 for v in toggles.values() if v)

        embed = discord.Embed(
            title="Audit Log",
            description="Server logging with related events bundled into one entry.",
            color=COLOR_NEUTRAL,
        )
        embed.add_field(
            name="Channels",
            value=(f"Default: {channel_label(guild, settings['default_channel_id'])}\n"
                   f"Exit: {channel_label(guild, settings['exit_channel_id'])}\n"
                   f"Mod: {channel_label(guild, settings['mod_channel_id'])}"),
            inline=True,
        )
        embed.add_field(
            name="Events",
            value=f"{on} of {len(EVENTS)} enabled",
            inline=True,
        )
        embed.add_field(
            name="Ignoring",
            value=f"{len(ignored_channels)} channels, {len(ignored_users)} users",
            inline=True,
        )
        embed.add_field(
            name="Bundling",
            value=(f"Events are collected for "
                   f"**{settings['batch_seconds'] or DEFAULT_BATCH_SECONDS}s** "
                   f"before posting, so a burst of related changes arrives as "
                   f"one entry."),
            inline=False,
        )
        return embed

    @discord.ui.button(label="Channels", style=discord.ButtonStyle.primary, row=0)
    async def channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ChannelsPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Events", style=discord.ButtonStyle.primary, row=0)
    async def events(self, interaction: discord.Interaction, button: discord.ui.Button):
        await EventsPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Ignore list", style=discord.ButtonStyle.primary, row=0)
    async def ignore(self, interaction: discord.Interaction, button: discord.ui.Button):
        await IgnorePage(self.cog, self).render(interaction)

    @discord.ui.button(label="Message log", style=discord.ButtonStyle.primary, row=0)
    async def message_log(self, interaction: discord.Interaction, button: discord.ui.Button):
        await MessageLogPage(self.cog, self).render(interaction)


class ChannelsPage(AuditPage):

    def __init__(self, cog: AuditLog, parent: AuditPage):
        super().__init__(cog, parent)
        self.picker = discord.ui.Select(
            placeholder="Pick a channel slot to set…",
            options=[
                discord.SelectOption(label=spec["label"], value=key,
                                     description=truncate(spec["help"], 100))
                for key, spec in CHANNEL_TARGETS.items()
            ],
            row=0,
        )
        self.picker.callback = self._on_pick
        self.add_item(self.picker)

    async def _on_pick(self, interaction: discord.Interaction):
        await ChannelTargetPage(self.cog, self, self.picker.values[0]).render(interaction)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        settings = await self.cog.db.get_settings(guild.id)
        cats = await self.cog.db.get_category_channels(guild.id)
        events = await self.cog.db.get_event_channels(guild.id)

        embed = discord.Embed(
            title="Channels",
            description=("Each category falls back to the default channel when it "
                         "has none of its own, and **Usernames** and **Avatars** "
                         "fall back to Members.\n"
                         "Leaves, kicks and bans always go to **Exit**; kicks and "
                         "bans also go to **Mod**, except inactivity kicks."),
            color=COLOR_NEUTRAL,
        )
        for key, spec in CHANNEL_TARGETS.items():
            if "column" in spec:
                value = channel_label(guild, settings[spec["column"]])
            elif "event" in spec:
                cid = events.get(spec["event"])
                value = channel_label(guild, cid)
                if not cid:
                    category = EVENTS[spec["event"]][0]
                    inherited = cats.get(category) or settings["default_channel_id"]
                    value += f" → {channel_label(guild, inherited)}"
            else:
                cid = cats.get(spec["category"])
                value = channel_label(guild, cid)
                if not cid:
                    value += f" → {channel_label(guild, settings['default_channel_id'])}"
            embed.add_field(name=spec["label"], value=value, inline=True)
        return embed


class ChannelTargetPage(AuditPage):

    def __init__(self, cog: AuditLog, parent: AuditPage, target_key: str):
        super().__init__(cog, parent)
        self.target_key = target_key
        self.spec = CHANNEL_TARGETS[target_key]
        self.select = discord.ui.ChannelSelect(
            placeholder=f"Pick a channel for {self.spec['label'].lower()}…",
            channel_types=[discord.ChannelType.text, discord.ChannelType.news],
            row=0,
        )
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def _save(self, guild_id: int, channel_id: Optional[int]):
        if "column" in self.spec:
            await self.cog.db.set_setting(guild_id, self.spec["column"], channel_id)
        elif "event" in self.spec:
            await self.cog.db.set_event_channel(
                guild_id, self.spec["event"], channel_id
            )
        else:
            await self.cog.db.set_category_channel(
                guild_id, self.spec["category"], channel_id
            )

    async def _on_pick(self, interaction: discord.Interaction):
        channel = self.select.values[0]
        await self._save(interaction.guild.id, channel.id)
        await self.render(interaction,
                          flash=f"{self.spec['label']} set to {channel.mention}.")

    @discord.ui.button(label="Clear", style=discord.ButtonStyle.danger, row=1)
    async def clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._save(interaction.guild.id, None)
        await self.render(interaction, flash=f"{self.spec['label']} cleared.")

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        settings = await self.cog.db.get_settings(guild.id)
        note = ""
        if "column" in self.spec:
            current = settings[self.spec["column"]]
        elif "event" in self.spec:
            event_key = self.spec["event"]
            events = await self.cog.db.get_event_channels(guild.id)
            current = events.get(event_key)
            if not current:
                cats = await self.cog.db.get_category_channels(guild.id)
                inherited = (cats.get(EVENTS[event_key][0])
                             or settings["default_channel_id"])
                note = f"\nInheriting: {channel_label(guild, inherited)}"
            toggles = await self.cog.db.get_toggles(guild.id)
            if not toggles.get(event_key):
                note += (f"\n\n*{EVENTS[event_key][1]} is currently turned off, so "
                         f"nothing will be posted here until you enable it under "
                         f"Events.*")
        else:
            cats = await self.cog.db.get_category_channels(guild.id)
            current = cats.get(self.spec["category"])
        return discord.Embed(
            title=self.spec["label"],
            description=(f"{self.spec['help']}\n\n"
                         f"Currently: {channel_label(guild, current)}{note}"),
            color=COLOR_NEUTRAL,
        )


class EventsPage(AuditPage):

    def __init__(self, cog: AuditLog, parent: AuditPage):
        super().__init__(cog, parent)
        self.picker = discord.ui.Select(
            placeholder="Pick a category to configure…",
            options=[
                discord.SelectOption(label=label, value=key)
                for key, label in CATEGORIES.items()
            ],
            row=0,
        )
        self.picker.callback = self._on_pick
        self.add_item(self.picker)

    async def _on_pick(self, interaction: discord.Interaction):
        await EventTogglesPage(self.cog, self, self.picker.values[0]).render(interaction)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        toggles = await self.cog.db.get_toggles(guild.id)
        embed = discord.Embed(title="Events", color=COLOR_NEUTRAL)
        for category, label in CATEGORIES.items():
            keys = [k for k, spec in EVENTS.items() if spec[0] == category]
            lines = [f"{'on ' if toggles[k] else 'off'} — {EVENTS[k][1]}" for k in keys]
            embed.add_field(name=label, value="\n".join(lines) or "*none*", inline=True)
        return embed


class EventTogglesPage(AuditPage):
    """Toggle buttons are rebuilt wholesale, since the set is data-driven."""

    def __init__(self, cog: AuditLog, parent: AuditPage, category: str):
        super().__init__(cog, parent)
        self.category = category
        self.keys = [k for k, spec in EVENTS.items() if spec[0] == category]

    async def rebuild(self, guild: discord.Guild):
        self.clear_items()
        toggles = await self.cog.db.get_toggles(guild.id)
        for index, key in enumerate(self.keys):
            button = discord.ui.Button(
                label=f"{EVENTS[key][1]}: {'ON' if toggles[key] else 'OFF'}",
                style=(discord.ButtonStyle.success if toggles[key]
                       else discord.ButtonStyle.secondary),
                row=min(index // 2, 3),
            )
            button.callback = self._toggler(key)
            self.add_item(button)
        self._restore_back()

    def _toggler(self, key: str):
        async def callback(interaction: discord.Interaction):
            toggles = await self.cog.db.get_toggles(interaction.guild.id)
            new_value = not toggles[key]
            await self.cog.db.set_toggle(interaction.guild.id, key, new_value)
            await self.render(
                interaction,
                flash=f"{EVENTS[key][1]} turned {'on' if new_value else 'off'}.",
            )
        return callback

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=CATEGORIES[self.category],
            description="Turn individual events on or off.",
            color=COLOR_NEUTRAL,
        )


class IgnorePage(AuditPage):
    """Channels and users excluded from every kind of logging."""

    def __init__(self, cog: AuditLog, parent: AuditPage):
        super().__init__(cog, parent)
        self.remove_channel_select: Optional[discord.ui.Select] = None
        self.remove_user_select: Optional[discord.ui.Select] = None

    async def rebuild(self, guild: discord.Guild):
        self.clear_items()

        channel_select = discord.ui.ChannelSelect(
            placeholder="Ignore a channel…",
            channel_types=[discord.ChannelType.text, discord.ChannelType.news,
                           discord.ChannelType.voice, discord.ChannelType.forum],
            row=0,
        )
        channel_select.callback = self._add_channel
        self.add_item(channel_select)

        user_select = discord.ui.UserSelect(placeholder="Ignore a user…", row=1)
        user_select.callback = self._add_user
        self.add_item(user_select)

        ignored_channels, ignored_users = await self.cog.db.get_ignored(guild.id)

        if ignored_channels:
            options = []
            for cid in list(ignored_channels)[:25]:
                channel = guild.get_channel(cid)
                options.append(discord.SelectOption(
                    label=truncate(f"#{channel.name}" if channel else str(cid), 100),
                    value=str(cid),
                ))
            self.remove_channel_select = discord.ui.Select(
                placeholder="Stop ignoring a channel…", options=options, row=2
            )
            self.remove_channel_select.callback = self._remove_channel
            self.add_item(self.remove_channel_select)

        if ignored_users:
            options = []
            for uid in list(ignored_users)[:25]:
                member = guild.get_member(uid)
                options.append(discord.SelectOption(
                    label=truncate(member.display_name if member else str(uid), 100),
                    value=str(uid),
                ))
            self.remove_user_select = discord.ui.Select(
                placeholder="Stop ignoring a user…", options=options, row=3
            )
            self.remove_user_select.callback = self._remove_user
            self.add_item(self.remove_user_select)

        self._restore_back()

    async def _add_channel(self, interaction: discord.Interaction):
        channel = interaction.data["values"][0]
        await self.cog.db.add_ignored(interaction.guild.id, "channel", int(channel))
        await self.render(interaction, flash=f"Now ignoring <#{channel}>.")

    async def _add_user(self, interaction: discord.Interaction):
        user_id = int(interaction.data["values"][0])
        await self.cog.db.add_ignored(interaction.guild.id, "user", user_id)
        await self.render(interaction, flash=f"Now ignoring <@{user_id}>.")

    async def _remove_channel(self, interaction: discord.Interaction):
        cid = int(self.remove_channel_select.values[0])
        await self.cog.db.remove_ignored(interaction.guild.id, "channel", cid)
        await self.render(interaction, flash=f"No longer ignoring <#{cid}>.")

    async def _remove_user(self, interaction: discord.Interaction):
        uid = int(self.remove_user_select.values[0])
        await self.cog.db.remove_ignored(interaction.guild.id, "user", uid)
        await self.render(interaction, flash=f"No longer ignoring <@{uid}>.")

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        channels, users = await self.cog.db.get_ignored(guild.id)
        embed = discord.Embed(
            title="Ignore list",
            description="Nothing happening in these channels, or to these users, "
                        "is logged.",
            color=COLOR_NEUTRAL,
        )
        embed.add_field(
            name=f"Channels ({len(channels)})",
            value=truncate(", ".join(f"<#{c}>" for c in channels) or "*none*", 1024),
            inline=False,
        )
        embed.add_field(
            name=f"Users ({len(users)})",
            value=truncate(", ".join(f"<@{u}>" for u in users) or "*none*", 1024),
            inline=False,
        )
        return embed


class BatchWindowModal(discord.ui.Modal, title="Bundling window"):
    seconds = discord.ui.TextInput(
        label="Seconds to collect before posting",
        placeholder="15",
        max_length=3,
        required=True,
    )

    def __init__(self, page: 'MessageLogPage'):
        super().__init__()
        self.page = page

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(str(self.seconds.value).strip())
        except ValueError:
            await interaction.response.send_message(
                "That isn't a number.", ephemeral=True
            )
            return
        if not 1 <= value <= MAX_BATCH_HOLD_SECONDS:
            await interaction.response.send_message(
                f"Pick something between 1 and {MAX_BATCH_HOLD_SECONDS} seconds.",
                ephemeral=True,
            )
            return
        await self.page.cog.db.set_setting(interaction.guild.id, "batch_seconds", value)
        await self.page.render(interaction, flash=f"Bundling window set to {value}s.")


class MessageLogPage(AuditPage):

    async def rebuild(self, guild: discord.Guild):
        self.clear_items()
        settings = await self.cog.db.get_settings(guild.id)

        bots_on = bool(settings["log_bot_messages"])
        bot_button = discord.ui.Button(
            label=f"Log bot messages: {'ON' if bots_on else 'OFF'}",
            style=(discord.ButtonStyle.success if bots_on
                   else discord.ButtonStyle.secondary),
            row=0,
        )
        bot_button.callback = self._toggle_bots
        self.add_item(bot_button)

        window_button = discord.ui.Button(
            label="Set bundling window", style=discord.ButtonStyle.primary, row=0
        )
        window_button.callback = self._set_window
        self.add_item(window_button)

        purge_button = discord.ui.Button(
            label="Purge expired now", style=discord.ButtonStyle.danger, row=1
        )
        purge_button.callback = self._purge
        self.add_item(purge_button)

        self._restore_back()

    async def _toggle_bots(self, interaction: discord.Interaction):
        settings = await self.cog.db.get_settings(interaction.guild.id)
        new_value = 0 if settings["log_bot_messages"] else 1
        await self.cog.db.set_setting(
            interaction.guild.id, "log_bot_messages", new_value
        )
        await self.render(
            interaction,
            flash=f"Bot messages {'will' if new_value else 'will not'} be recorded.",
        )

    async def _set_window(self, interaction: discord.Interaction):
        await interaction.response.send_modal(BatchWindowModal(self))

    async def _purge(self, interaction: discord.Interaction):
        removed = await self.cog.db.purge_messages()
        await self.render(interaction, flash=f"Purged {removed} expired messages.")

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        settings = await self.cog.db.get_settings(guild.id)
        count = await self.cog.db.message_count()
        hours = MESSAGE_RETENTION_SECONDS // 3600
        return discord.Embed(
            title="Message log",
            description=(
                f"Discord sends no content when a message is deleted, only an ID, "
                f"so messages are mirrored locally for **{hours}h** to make "
                f"before/after possible.\n\n"
                f"Cached messages: **{count}**\n"
                f"Bundling window: **{settings['batch_seconds'] or DEFAULT_BATCH_SECONDS}s**\n"
                f"Bot messages: **{'recorded' if settings['log_bot_messages'] else 'skipped'}**"
            ),
            color=COLOR_NEUTRAL,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AuditLog(bot))
