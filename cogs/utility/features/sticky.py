"""Sticky messages — a note that stays at the bottom of a channel.

The mechanic is simple: when someone posts, delete the old copy and send a new
one. The two things that make a naive version unpleasant are both handled
here.

*Rate.* Re-posting on every message turns a busy channel into a strobe and
burns the channel's message-send ratelimit. Each sticky has a minimum interval
and, below it, the repost is deferred rather than dropped — so the sticky
always ends up at the bottom once the burst passes, instead of being stranded
halfway up.

*Ordering.* Two messages arriving together could both try to repost, and the
loser deletes the winner's copy, leaving no sticky at all. A per-channel lock
makes the delete-then-send one atomic step.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict

import discord

from ..storage import now
from . import render

logger = logging.getLogger('cogs.utility.sticky')

# channel_id -> lock, so a repost is never interleaved with another repost.
_locks: Dict[int, asyncio.Lock] = {}
# channel_id -> monotonic time of the last repost, for the interval.
_last_post: Dict[int, float] = {}
# channel_id -> True when a repost is already queued behind the interval.
_pending: Dict[int, bool] = {}


def _lock(channel_id: int) -> asyncio.Lock:
    lock = _locks.get(channel_id)
    if lock is None:
        lock = _locks[channel_id] = asyncio.Lock()
    return lock


async def on_message(cog, message: discord.Message, rule) -> None:
    """Called for every message in a channel that has a sticky."""
    if message.author.id == (cog.bot.user.id if cog.bot.user else 0):
        return  # our own sticky landing would otherwise retrigger this

    channel_id = message.channel.id
    interval = max(0, rule["min_interval_s"] or 0)
    elapsed = time.monotonic() - _last_post.get(channel_id, 0.0)

    if interval and elapsed < interval:
        # Already waiting — one deferred repost is enough, however many
        # messages arrive during the window.
        if _pending.get(channel_id):
            return
        _pending[channel_id] = True
        asyncio.create_task(_repost_after(cog, message.channel, rule,
                                          interval - elapsed))
        return

    await repost(cog, message.channel, rule)


async def _repost_after(cog, channel, rule, delay: float):
    try:
        await asyncio.sleep(delay)
        # Re-read: the sticky may have been edited or deleted while waiting.
        fresh = await cog.db.get_row("sticky_messages", rule["id"])
        if fresh is not None and fresh["enabled"]:
            await repost(cog, channel, fresh)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Deferred sticky repost in {channel.id} failed: {e}")
    finally:
        _pending.pop(channel.id, None)


async def repost(cog, channel, rule) -> bool:
    """Move the sticky to the bottom. True if a new copy was posted."""
    async with _lock(channel.id):
        # A sticky is now a full message like a reminder: content, embed, ping
        # and buttons all come from the same renderer, so a sticky and a
        # reminder built from the same fields look identical.
        rendered = render.render(dict(rule))
        content, embed, view = rendered["content"], rendered["embed"], rendered["view"]
        if not (content and content.strip()) and embed is None:
            return False

        old_id = rule["message_id"]
        try:
            new_message = await channel.send(
                content=(content[:2000] if content else None),
                embed=embed, view=view,
                allowed_mentions=render.ALLOWED_MENTIONS,
            )
        except discord.Forbidden:
            logger.warning(f"Cannot post the sticky in {channel.id} — missing perms.")
            return False
        except discord.HTTPException as e:
            logger.warning(f"Sticky send failed in {channel.id}: {e}")
            return False

        # Sent before the delete, so a failure leaves two copies rather than
        # none. A duplicate is visible and self-corrects on the next repost;
        # a missing sticky is silent.
        if old_id:
            try:
                await channel.get_partial_message(old_id).delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        _last_post[channel.id] = time.monotonic()
        await cog.db.update_row("sticky_messages", rule["id"],
                                message_id=new_message.id, last_posted_ts=now())
        return True


async def clear(cog, channel, rule):
    """Remove the posted copy, e.g. when the sticky is disabled or deleted."""
    if not rule["message_id"] or channel is None:
        return
    try:
        await channel.get_partial_message(rule["message_id"]).delete()
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        pass
    await cog.db.update_row("sticky_messages", rule["id"], message_id=None)
