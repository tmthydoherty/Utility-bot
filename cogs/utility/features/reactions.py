"""Reaction rules.

Three modes, where the old cog had one:

* `remove`    — strip reactions from matching messages (the original behaviour)
* `allowlist` — only the listed emoji may stay; everything else is removed
* `blocklist` — the listed emoji are removed; everything else stays

plus a per-rule reaction cap and bypass roles.

The scope (all messages / messages mentioning a role / messages from a user)
is orthogonal to the mode, so "only ⭐ and 🔥 on posts from the announcements
bot" is expressible where before it was not.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import discord

from ..storage import loads

logger = logging.getLogger('cogs.utility.reactions')


def emoji_key(emoji) -> str:
    """A stable string for an emoji, custom or unicode.

    Custom emoji are keyed by id because they can be renamed; unicode emoji
    are the character itself.
    """
    emoji_id = getattr(emoji, "id", None)
    if emoji_id:
        return str(emoji_id)
    return str(getattr(emoji, "name", emoji))


def _matches_emoji(emoji, listed: list) -> bool:
    if not listed:
        return False
    key = emoji_key(emoji)
    name = str(getattr(emoji, "name", ""))
    raw = str(emoji)
    for entry in listed:
        entry = str(entry)
        if entry in (key, name, raw):
            return True
        # Stored as a full <:name:id> form — compare on the id inside it.
        if ":" in entry and entry.rstrip(">").rsplit(":", 1)[-1] == key:
            return True
    return False


def _has_bypass(member: Optional[discord.Member], role_ids: list) -> bool:
    if not role_ids or member is None:
        return False
    wanted = {int(r) for r in role_ids}
    return any(r.id in wanted for r in getattr(member, "roles", []))


def needs_message(rule) -> bool:
    """Can this rule be decided from the reaction event alone?

    Scope `all` with no cap is decidable without the message, which lets the
    common case skip a `fetch_message` per reaction — the old cog fetched
    unconditionally, spending an API call on every reaction in a ruled
    channel even when the rule could not possibly match.
    """
    if rule["max_reactions"]:
        return True
    return rule["scope"] in ("role_mention", "from_user")


def scope_matches(rule, message: Optional[discord.Message]) -> bool:
    scope = rule["scope"]
    if scope == "all":
        return True
    if message is None:
        return False
    if scope == "role_mention":
        wanted = {int(r) for r in loads(rule["role_ids"], [])}
        return bool(wanted & set(message.raw_role_mentions))
    if scope == "from_user":
        wanted = {int(u) for u in loads(rule["user_ids"], [])}
        return message.author.id in wanted
    return False


def should_remove(rule, emoji) -> bool:
    """Given the scope already matched, does the mode remove this emoji?"""
    mode = rule["mode"]
    listed = loads(rule["emoji"], [])
    if mode == "allowlist":
        # An empty allowlist means nothing is permitted, which is the literal
        # reading and also the only one that isn't a silent no-op.
        return not _matches_emoji(emoji, listed)
    if mode == "blocklist":
        return _matches_emoji(emoji, listed)
    return True  # plain remove


async def apply(cog, payload: discord.RawReactionActionEvent, rule,
                channel, member: Optional[discord.Member]) -> bool:
    """Enforce one reaction rule. True if the reaction was removed."""
    if _has_bypass(member, loads(rule["bypass_role_ids"], [])):
        return False

    message = None
    if needs_message(rule):
        try:
            message = await channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
            logger.debug(f"Could not fetch {payload.message_id} for reaction rule: {e}")
            return False

    if not scope_matches(rule, message):
        return False

    remove = should_remove(rule, payload.emoji)

    # The cap is a separate reason to remove: the emoji itself may be fine,
    # but this message already carries as many distinct reactions as allowed.
    if not remove and rule["max_reactions"] and message is not None:
        existing = {emoji_key(r.emoji) for r in message.reactions}
        if len(existing) > rule["max_reactions"]:
            remove = True

    if not remove:
        return False

    delay = rule["remove_after_s"] or 0
    if delay > 0:
        # Short enough that losing it to a restart doesn't matter, so it stays
        # a plain sleep rather than earning a row in pending_actions.
        await asyncio.sleep(min(delay, 60))

    target = message or channel.get_partial_message(payload.message_id)
    try:
        await target.remove_reaction(payload.emoji, discord.Object(id=payload.user_id))
        return True
    except (discord.Forbidden, discord.NotFound, discord.HTTPException) as e:
        logger.debug(f"Could not remove reaction in {payload.channel_id}: {e}")
        return False
