"""Leveling engine — XP curve, message XP and voice XP accrual.

Standard Mee6-style progression: random XP per message behind a per-user
cooldown, plus XP per minute spent in voice. Deliberately silent — nothing
announces a level-up anywhere. Crossing a level does dispatch the internal
`member_level_up` event, which other cogs listen for; it posts nothing itself.
"""

import logging
import random
import time

logger = logging.getLogger('cogs.economy.leveling')


# --------------------------------------------------------------------------
# XP curve (Mee6 parity)
# --------------------------------------------------------------------------

def xp_for_next_level(level: int) -> int:
    """XP required to advance from `level` to `level + 1`."""
    return 5 * (level ** 2) + 50 * level + 100


def total_xp_for_level(level: int) -> int:
    """Cumulative XP needed to reach `level` from zero.

    Closed form of sum(5n^2 + 50n + 100) for n in [0, level).
    """
    if level <= 0:
        return 0
    n = level - 1
    return (5 * n * level * (2 * level - 1)) // 6 + 25 * n * level + 100 * level


def level_from_xp(xp: int) -> int:
    """Highest level fully covered by `xp`."""
    level = 0
    while xp >= total_xp_for_level(level + 1):
        level += 1
        if level > 1000:  # hard stop; nobody reaches this
            break
    return level


def level_progress(xp: int) -> tuple:
    """(level, xp_into_level, xp_needed_for_level, percent_complete)."""
    level = level_from_xp(xp)
    base = total_xp_for_level(level)
    needed = xp_for_next_level(level)
    into = xp - base
    percent = 0 if needed <= 0 else min(100, max(0, round(into / needed * 100)))
    return level, into, needed, percent


# --------------------------------------------------------------------------
# Engine
# --------------------------------------------------------------------------

class LevelingEngine:
    """Owns XP accrual. Reads its rates from the shared Config on every call
    so panel edits take effect immediately without a reload."""

    def __init__(self, cog):
        self.cog = cog
        self.db = cog.db
        self.config = cog.config
        # user_id -> monotonic timestamp of last message XP grant. In-memory
        # only; a restart just means one free message, which is harmless.
        self._msg_cooldowns: dict = {}

    # ------------------------------------------------------------- messages

    def channel_excluded(self, channel) -> bool:
        excluded = {int(c) for c in self.config.get_list("lvl_excluded_channels")}
        if not excluded:
            return False
        # Thread messages carry the thread's id, not the parent channel's, so
        # an exclusion on the parent has to cover its threads too.
        if channel.id in excluded:
            return True
        parent_id = getattr(channel, "parent_id", None)
        return parent_id is not None and parent_id in excluded

    async def grant_message_xp(self, message) -> bool:
        """Award message XP if the user is off cooldown. Returns True if granted."""
        user_id = message.author.id
        cooldown = self.config.get_int("lvl_xp_cooldown", 60)
        now = time.time()

        last = self._msg_cooldowns.get(user_id, 0)
        if now - last < cooldown:
            # Still count the message for stats even when XP is on cooldown.
            await self.db.execute(
                "UPDATE users SET messages = messages + 1 WHERE user_id = ?", (user_id,)
            )
            return False

        self._msg_cooldowns[user_id] = now
        lo = self.config.get_int("lvl_msg_xp_min", 15)
        hi = self.config.get_int("lvl_msg_xp_max", 25)
        if hi < lo:
            lo, hi = hi, lo
        amount = random.randint(lo, hi)

        await self.add_xp(user_id, amount, count_message=True)
        return True

    # ---------------------------------------------------------------- voice

    async def grant_voice_xp(self, user_id: int, seconds: int):
        """Bank voice time and convert whole minutes into XP.

        Leftover seconds are carried in voice_accum so a member who drifts in
        and out of voice still gets credit for the partial minutes.
        """
        if seconds <= 0:
            return
        await self.db.execute(
            "INSERT INTO voice_accum (user_id, pending_seconds, last_tick_ts) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET pending_seconds = pending_seconds + ?, "
            "last_tick_ts = ?",
            (user_id, seconds, int(time.time()), seconds, int(time.time())),
        )
        await self.db.execute(
            "UPDATE users SET voice_seconds = voice_seconds + ? WHERE user_id = ?",
            (seconds, user_id),
        )

        row = await self.db.fetchone(
            "SELECT pending_seconds FROM voice_accum WHERE user_id = ?", (user_id,)
        )
        pending = row["pending_seconds"] if row else 0
        minutes = pending // 60
        if minutes <= 0:
            return

        per_min = self.config.get_int("lvl_voice_xp_per_min", 5)
        await self.db.execute(
            "UPDATE voice_accum SET pending_seconds = ? WHERE user_id = ?",
            (pending - minutes * 60, user_id),
        )
        if per_min > 0:
            await self.add_xp(user_id, minutes * per_min)

    # ------------------------------------------------------------------ xp

    async def add_xp(self, user_id: int, amount: int, *, count_message: bool = False):
        if amount == 0:
            return
        await self.db.ensure_user(user_id)
        extra = ", messages = messages + 1" if count_message else ""
        await self.db.execute(
            f"UPDATE users SET xp = MAX(0, xp + ?){extra} WHERE user_id = ?",
            (amount, user_id),
            commit=False,
        )
        row = await self.db.fetchone(
            "SELECT xp, level FROM users WHERE user_id = ?", (user_id,)
        )
        old_level = row["level"] if row else 0
        new_level = level_from_xp(row["xp"] if row else 0)
        await self.db.execute(
            "UPDATE users SET level = ? WHERE user_id = ?", (new_level, user_id)
        )

        # Still no announcement anywhere — this is a bare signal other cogs can
        # act on (the newcomer cog swaps its role on it). Only fired on a real
        # gain, so recalculate_levels and XP removals stay silent.
        if new_level > old_level:
            self.cog.bot.dispatch("member_level_up", user_id, old_level, new_level)

    def prune_memory(self, max_age: float = 3600.0):
        """Shed cooldown entries for users who have gone quiet.

        One entry per user accumulates otherwise, and this bot has months of
        uptime between restarts.
        """
        cutoff = time.time() - max_age
        for user_id, stamp in list(self._msg_cooldowns.items()):
            if stamp < cutoff:
                del self._msg_cooldowns[user_id]

    async def recalculate_levels(self) -> int:
        """Re-derive every stored level from XP. Used after a curve change."""
        rows = await self.db.fetchall("SELECT user_id, xp, level FROM users")
        changed = 0
        for row in rows:
            correct = level_from_xp(row["xp"])
            if correct != row["level"]:
                await self.db.execute(
                    "UPDATE users SET level = ? WHERE user_id = ?",
                    (correct, row["user_id"]), commit=False,
                )
                changed += 1
        await self.db.commit()
        return changed

    # ----------------------------------------------------------- leaderboard

    async def get_rank(self, user_id: int) -> tuple:
        """(rank, total_ranked). Rank is 1-indexed; 0 means unranked."""
        row = await self.db.fetchone("SELECT xp FROM users WHERE user_id = ?", (user_id,))
        total_row = await self.db.fetchone("SELECT COUNT(*) AS c FROM users WHERE xp > 0")
        total = total_row["c"] if total_row else 0
        if not row or row["xp"] <= 0:
            return 0, total
        ahead = await self.db.fetchone(
            "SELECT COUNT(*) AS c FROM users WHERE xp > ?", (row["xp"],)
        )
        return (ahead["c"] if ahead else 0) + 1, total

    async def get_leaderboard(self, limit: int = 20, offset: int = 0) -> list:
        return await self.db.fetchall(
            "SELECT user_id, xp, level, messages, voice_seconds FROM users "
            "WHERE xp > 0 ORDER BY xp DESC, user_id ASC LIMIT ? OFFSET ?",
            (limit, offset),
        )

    async def leaderboard_size(self) -> int:
        row = await self.db.fetchone("SELECT COUNT(*) AS c FROM users WHERE xp > 0")
        return row["c"] if row else 0
