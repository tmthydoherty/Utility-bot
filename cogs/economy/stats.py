"""Activity stats read from tracker.py's database.

cogs/tracker.py already logs every message, voice session, reaction and emoji
use into tracking_data.db. The level card reads from it directly rather than
duplicating that tracking. This module is strictly read-only — it never
writes to another cog's database.
"""

import aiosqlite
import asyncio
import logging
import typing
from datetime import datetime, timedelta

from .database import TRACKING_DB_PATH

logger = logging.getLogger('cogs.economy.stats')

WEEKDAYS = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")


class ActivityStats:
    def __init__(self):
        self._db: typing.Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def connect(self):
        async with self._lock:
            if self._db:
                return
            try:
                self._db = await aiosqlite.connect(TRACKING_DB_PATH)
                self._db.row_factory = aiosqlite.Row
                logger.info("Activity stats connected to tracking_data.db.")
            except Exception as e:
                logger.warning(f"Could not open tracking_data.db: {e}")
                self._db = None

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def available(self) -> bool:
        return self._db is not None

    async def _one(self, query: str, params: tuple = ()):
        if not self._db:
            return None
        try:
            async with self._db.execute(query, params) as cursor:
                return await cursor.fetchone()
        except Exception as e:
            logger.warning(f"stats query failed: {e}")
            return None

    async def _all(self, query: str, params: tuple = ()) -> list:
        if not self._db:
            return []
        try:
            async with self._db.execute(query, params) as cursor:
                return await cursor.fetchall()
        except Exception as e:
            logger.warning(f"stats query failed: {e}")
            return []

    # ------------------------------------------------------------ lifetime

    async def lifetime(self, user_id: int) -> dict:
        """Totals across the whole retained history."""
        out = {
            "messages": 0, "voice_seconds": 0, "days_active": 0,
            "avg_length": 0, "attachments": 0, "replies": 0,
            "reactions_given": 0, "reactions_received": 0,
            "first_seen": None,
        }

        row = await self._one(
            "SELECT COUNT(*) AS c, AVG(length) AS avg_len, "
            "SUM(has_attachment) AS atts, SUM(is_reply) AS replies, "
            "MIN(timestamp) AS first_ts, "
            "COUNT(DISTINCT date(timestamp, 'unixepoch', 'localtime')) AS days "
            "FROM message_logs WHERE user_id = ?",
            (user_id,),
        )
        if row and row["c"]:
            out["messages"] = row["c"]
            out["avg_length"] = round(row["avg_len"] or 0)
            out["attachments"] = row["atts"] or 0
            out["replies"] = row["replies"] or 0
            out["days_active"] = row["days"] or 0
            out["first_seen"] = row["first_ts"]

        row = await self._one(
            "SELECT SUM(duration) AS total FROM voice_sessions WHERE user_id = ?", (user_id,)
        )
        if row and row["total"]:
            out["voice_seconds"] = int(row["total"])

        row = await self._one(
            "SELECT COUNT(*) AS c FROM reaction_logs WHERE user_id = ?", (user_id,)
        )
        out["reactions_given"] = row["c"] if row else 0

        row = await self._one(
            "SELECT COUNT(*) AS c FROM reaction_logs WHERE target_message_author_id = ?",
            (user_id,),
        )
        out["reactions_received"] = row["c"] if row else 0

        return out

    # ----------------------------------------------------------- behaviour

    async def top_channel(self, user_id: int) -> typing.Optional[int]:
        row = await self._one(
            "SELECT channel_id, COUNT(*) AS c FROM message_logs WHERE user_id = ? "
            "GROUP BY channel_id ORDER BY c DESC LIMIT 1",
            (user_id,),
        )
        return row["channel_id"] if row else None

    async def peak_hour(self, user_id: int) -> typing.Optional[int]:
        row = await self._one(
            "SELECT CAST(strftime('%H', timestamp, 'unixepoch', 'localtime') AS INTEGER) AS hr, "
            "COUNT(*) AS c FROM message_logs WHERE user_id = ? "
            "GROUP BY hr ORDER BY c DESC LIMIT 1",
            (user_id,),
        )
        return row["hr"] if row else None

    async def peak_weekday(self, user_id: int) -> typing.Optional[str]:
        row = await self._one(
            "SELECT CAST(strftime('%w', timestamp, 'unixepoch', 'localtime') AS INTEGER) AS wd, "
            "COUNT(*) AS c FROM message_logs WHERE user_id = ? "
            "GROUP BY wd ORDER BY c DESC LIMIT 1",
            (user_id,),
        )
        if not row or row["wd"] is None:
            return None
        return WEEKDAYS[row["wd"] % 7]

    async def top_emoji(self, user_id: int) -> typing.Optional[dict]:
        row = await self._one(
            "SELECT emoji_id, emoji_name, COUNT(*) AS c FROM emoji_logs "
            "WHERE user_id = ? AND emoji_name IS NOT NULL "
            "GROUP BY emoji_name ORDER BY c DESC LIMIT 1",
            (user_id,),
        )
        if not row:
            return None
        return {"id": row["emoji_id"], "name": row["emoji_name"], "count": row["c"]}

    # -------------------------------------------------------- daily series

    async def daily_series(self, user_id: int, days: int = 30) -> dict:
        """Per-day message counts and voice minutes for the last `days` days.

        Returns aligned lists so the card can draw one chart with two series.
        Days with no activity are present as zeroes.
        """
        today = datetime.now().date()
        start = today - timedelta(days=days - 1)
        start_ts = int(datetime.combine(start, datetime.min.time()).timestamp())

        buckets = {(start + timedelta(days=i)).isoformat(): [0, 0] for i in range(days)}

        for row in await self._all(
            "SELECT date(timestamp, 'unixepoch', 'localtime') AS d, COUNT(*) AS c "
            "FROM message_logs WHERE user_id = ? AND timestamp >= ? GROUP BY d",
            (user_id, start_ts),
        ):
            if row["d"] in buckets:
                buckets[row["d"]][0] = row["c"]

        for row in await self._all(
            "SELECT date(end_time, 'unixepoch', 'localtime') AS d, SUM(duration) AS total "
            "FROM voice_sessions WHERE user_id = ? AND end_time >= ? GROUP BY d",
            (user_id, start_ts),
        ):
            if row["d"] in buckets and row["total"]:
                buckets[row["d"]][1] = round(row["total"] / 60)

        ordered = sorted(buckets.items())
        return {
            "labels": [d for d, _ in ordered],
            "messages": [v[0] for _, v in ordered],
            "voice_minutes": [v[1] for _, v in ordered],
        }

    # --------------------------------------------------------------- bundle

    async def full_profile(self, user_id: int, days: int = 30) -> dict:
        """Everything the level card needs, in one call."""
        profile = await self.lifetime(user_id)
        profile["top_channel_id"] = await self.top_channel(user_id)
        profile["peak_hour"] = await self.peak_hour(user_id)
        profile["peak_weekday"] = await self.peak_weekday(user_id)
        profile["top_emoji"] = await self.top_emoji(user_id)
        profile["series"] = await self.daily_series(user_id, days)
        return profile
