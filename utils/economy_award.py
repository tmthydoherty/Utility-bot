"""Bridge for awarding Economy Points from other cogs.

Deliberately defensive: if the Economy cog is not loaded, or anything at all
goes wrong, this is a no-op. A cog calling it must never break because of the
economy.

Usage:
    from utils.economy_award import award_points
    award_points(self.bot, user_id, "trivia")
"""

import asyncio
import logging

logger = logging.getLogger('utils.economy_award')

# asyncio only holds weak references to tasks, so a fire-and-forget task can be
# garbage collected mid-flight. Keep a strong reference until it finishes.
_pending: set = set()


def award_points(bot, user_id: int, source: str, **meta):
    """Fire-and-forget Points award. Never raises, never blocks."""
    try:
        cog = bot.get_cog("Economy")
        if cog is None:
            return
        # Deliberately not bot.loop — that is unset until the bot logs in, and
        # every call site is already inside a running loop.
        loop = asyncio.get_running_loop()
        task = loop.create_task(_award(cog, user_id, source, meta))
        _pending.add(task)
        task.add_done_callback(_pending.discard)
    except RuntimeError:
        # No running loop — a caller outside async context. Nothing to do, and
        # not worth a warning.
        logger.debug(f"award_points({user_id}, {source}): no running loop")
    except Exception as e:
        # Warning, not debug: a silently broken bridge would mean nobody ever
        # gets paid for trivia, quotes, matches or game nights again.
        logger.warning(f"award_points({user_id}, {source}) failed: {e}")


async def _award(cog, user_id: int, source: str, meta: dict):
    try:
        await cog.award(user_id, source, meta=meta or None)
    except Exception as e:
        logger.warning(f"Economy award failed for {user_id}/{source}: {e}")


def award_many(bot, user_ids, source: str, **meta):
    """Award the same source to several users at once."""
    for user_id in set(user_ids or ()):
        award_points(bot, user_id, source, **meta)
