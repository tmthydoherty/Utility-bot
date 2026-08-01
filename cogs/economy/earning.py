"""Points earning — rules, daily caps, streaks and the single award entry point.

Every Points grant in the whole cog funnels through `EarningEngine.award`, so
caps, the ledger and the balance can never drift apart.
"""

import logging
from datetime import date, timedelta

from .config import EARNING_SOURCES

logger = logging.getLogger('cogs.economy.earning')

# Which daily_activity column tracks the running total for a capped source.
CAP_COLUMNS = {
    "message": "msg_points",
    "newcomer_reply": "newcomer_points",
}


def today_key() -> str:
    return date.today().isoformat()


class EarningEngine:
    def __init__(self, cog):
        self.cog = cog
        self.db = cog.db
        self.config = cog.config

    # ---------------------------------------------------------------- award

    async def award(self, user_id: int, source: str, *, amount: int = None,
                    meta: dict = None) -> int:
        """Grant Points for `source`, honouring its configured value and cap.

        Returns the number of Points actually awarded (0 if capped out, the
        source is unknown, or the configured value is zero).
        """
        spec = EARNING_SOURCES.get(source)
        if spec is None and amount is None:
            logger.warning(f"award() called with unknown source '{source}'")
            return 0

        value = amount if amount is not None else self.config.earn_value(source)
        if value <= 0:
            return 0

        day = today_key()

        # Everything below happens in one transaction, so the whole award is a
        # single commit rather than one per statement.
        async with self.db.transaction():
            # Once-per-day sources: the claim is a single INSERT OR IGNORE, so
            # only one concurrent caller can win it.
            if spec and spec.get("once_daily"):
                if not await self.db.claim_daily_flag(user_id, day, source):
                    return 0
                await self.db.credit(user_id, value, source, meta)
                return value

            # Capped sources: the read and the increment are held together, and
            # the counter itself tells us how much we were actually allowed.
            column = CAP_COLUMNS.get(source)
            cap = self.config.earn_cap(source) if spec else 0
            if column:
                value = await self.db.grant_capped(user_id, day, column, value, cap)
                if value <= 0:
                    return 0

            await self.db.credit(user_id, value, source, meta)
        return value

    async def spend(self, user_id: int, cost: int, reason: str, meta: dict = None) -> bool:
        """Deduct Points if affordable, atomically. False means nothing changed."""
        return await self.db.try_debit(user_id, cost, reason, meta)

    async def refund(self, user_id: int, amount: int, reason: str, meta: dict = None):
        if amount > 0:
            await self.db.adjust_points(user_id, amount, f"refund:{reason}", meta)

    # -------------------------------------------------------------- streaks

    async def touch_streak(self, user_id: int) -> dict:
        """Record activity for today and advance the streak.

        Returns {"first_today": bool, "streak": int, "bonus": int}. The caller
        uses first_today to decide whether the first-message bonus applies.
        """
        today = date.today()
        key = today.isoformat()
        yesterday = (today - timedelta(days=1)).isoformat()

        async with self.db.transaction():
            row = await self.db.fetchone(
                "SELECT * FROM streaks WHERE user_id = ?", (user_id,)
            )

            # Fast path: already counted today. One read, no writes — this is
            # what almost every message after the first hits.
            if row is not None and row["last_active_day"] == key:
                return {"first_today": False, "streak": row["current_streak"], "bonus": 0}

            if row is None:
                current, longest, last_bonus = 1, 1, None
            else:
                current = (row["current_streak"] + 1
                           if row["last_active_day"] == yesterday else 1)
                longest = max(current, row["longest_streak"] or 0)
                last_bonus = row["last_bonus_day"]

            # UPSERT so the insert and update paths are one statement.
            await self.db.execute(
                "INSERT INTO streaks (user_id, current_streak, longest_streak, "
                "last_active_day) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "current_streak = excluded.current_streak, "
                "longest_streak = excluded.longest_streak, "
                "last_active_day = excluded.last_active_day",
                (user_id, current, longest, key),
            )

            bonus = 0
            # Every completed week of consecutive days pays out once.
            if current % 7 == 0 and last_bonus != key:
                bonus = await self.award(user_id, "streak7", meta={"streak": current})
                if bonus:
                    await self.db.execute(
                        "UPDATE streaks SET last_bonus_day = ? WHERE user_id = ?",
                        (key, user_id),
                    )

        return {"first_today": True, "streak": current, "bonus": bonus}

    async def get_streak(self, user_id: int):
        return await self.db.fetchone("SELECT * FROM streaks WHERE user_id = ?", (user_id,))

    # ---------------------------------------------------------------- audit

    async def recent_ledger(self, user_id: int, limit: int = 15) -> list:
        return await self.db.fetchall(
            "SELECT * FROM points_ledger WHERE user_id = ? ORDER BY ts DESC LIMIT ?",
            (user_id, limit),
        )

    async def top_balances(self, limit: int = 20, offset: int = 0) -> list:
        return await self.db.fetchall(
            "SELECT user_id, points, lifetime_points FROM users WHERE points > 0 "
            "ORDER BY points DESC, user_id ASC LIMIT ? OFFSET ?",
            (limit, offset),
        )
