"""Condition predicates.

Each takes `(cog, ctx, config)` and returns `(matched, reason)`. The reason is
what the "why didn't it fire?" screen shows, so it should read as an
explanation rather than a restatement — "author has none of the 2 roles"
rather than "role_has failed".

A predicate that cannot evaluate — a message condition on a member-join
trigger, say — returns False with a reason saying so, rather than raising.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Tuple

import discord

from utils.fieldspec import as_list
from utils import placeholders as variables

logger = logging.getLogger('cogs.automations.conditions')

Result = Tuple[bool, str]

INVITE_RE = re.compile(r"(discord\.(gg|io|me|li)|discordapp\.com/invite)/\S+", re.I)
LINK_RE = re.compile(r"https?://\S+", re.I)
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:\d+>")


def _ids(config, key) -> set:
    return {int(v) for v in as_list(config.get(key)) if str(v).strip().isdigit()}


def _member(ctx):
    return ctx.member or ctx.user


# ------------------------------------------------------------------ channel

async def channel_is(cog, ctx, config) -> Result:
    wanted = _ids(config, "channels")
    if not wanted:
        return True, "no channels configured, so this matches anything"
    if not ctx.channel:
        return False, "this trigger has no channel"

    parent_id = getattr(ctx.channel, "parent_id", None)
    is_thread = (isinstance(ctx.channel, discord.Thread) or parent_id is not None)
    # Threads and forum posts only count when the requirement opts in. A thread
    # is its own channel with its own id, so listing #clips used to quietly
    # match every thread and forum post under it — the surprise that let a rule
    # fire in a thread of a channel it was never pointed at. The exact thread
    # can always be listed on its own if that is really what is wanted.
    include_threads = bool(config.get("include_threads"))

    candidates = {ctx.channel.id}
    if is_thread and include_threads and parent_id:
        candidates.add(parent_id)
    category = getattr(ctx.channel, "category_id", None)
    # A category stands in for the channels inside it; extend that to a thread's
    # category only when threads are being counted at all.
    if category and (not is_thread or include_threads):
        candidates.add(category)

    if candidates & wanted:
        return True, f"in <#{ctx.channel.id}>"
    if is_thread and not include_threads and parent_id in wanted:
        return (False,
                f"it's a thread of <#{parent_id}>, and this requirement isn't "
                "set to include threads")
    return False, f"<#{ctx.channel.id}> is not one of the {len(wanted)} listed"


# --------------------------------------------------------------------- roles

async def role_has(cog, ctx, config) -> Result:
    member = _member(ctx)
    wanted = _ids(config, "roles")
    if not wanted:
        return True, "no roles configured, so this matches anyone"
    if member is None or not hasattr(member, "roles"):
        return False, "no member on this event"
    held = {r.id for r in member.roles}
    mode = config.get("match", "any")
    if mode == "all":
        missing = wanted - held
        if missing:
            return False, f"missing {len(missing)} of the {len(wanted)} required roles"
        return True, "has every listed role"
    if mode == "none":
        overlap = wanted & held
        if overlap:
            return False, f"has {len(overlap)} of the excluded roles"
        return True, "has none of the listed roles"
    if wanted & held:
        return True, "has a listed role"
    return False, f"has none of the {len(wanted)} listed roles"


# ------------------------------------------------------------------ content

async def content_contains(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    needles = [n.strip() for n in str(config.get("text", "")).split(",") if n.strip()]
    if not needles:
        return True, "no text configured"
    content = (ctx.message.content or "")
    if not config.get("case_sensitive"):
        content = content.lower()
        needles = [n.lower() for n in needles]
    hit = next((n for n in needles if n in content), None)
    if hit:
        return True, f"contains {hit!r}"
    return False, "contains none of the listed phrases"


async def content_regex(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    pattern = str(config.get("pattern", ""))
    if not pattern:
        return True, "no pattern configured"
    match = variables.search(pattern, ctx.message.content or "")
    if match is None:
        return False, "the pattern did not match"
    variables.store_captures(ctx, match)
    return True, f"matched {match.group(0)[:60]!r}"


async def content_starts_with(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    prefixes = [p.strip() for p in str(config.get("text", "")).split(",") if p.strip()]
    if not prefixes:
        return True, "no text configured"
    content = (ctx.message.content or "").lstrip()
    if not config.get("case_sensitive"):
        content = content.lower()
        prefixes = [p.lower() for p in prefixes]
    hit = next((p for p in prefixes if content.startswith(p)), None)
    if hit:
        return True, f"starts with {hit!r}"
    return False, "starts with none of the listed phrases"


async def content_ends_with(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    suffixes = [s.strip() for s in str(config.get("text", "")).split(",") if s.strip()]
    if not suffixes:
        return True, "no text configured"
    content = (ctx.message.content or "").rstrip()
    if not config.get("case_sensitive"):
        content = content.lower()
        suffixes = [s.lower() for s in suffixes]
    hit = next((s for s in suffixes if content.endswith(s)), None)
    if hit:
        return True, f"ends with {hit!r}"
    return False, "ends with none of the listed phrases"


async def content_exactly(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    wanted = [w.strip() for w in str(config.get("text", "")).split(",") if w.strip()]
    if not wanted:
        return True, "no text configured"
    content = (ctx.message.content or "").strip()
    if not config.get("case_sensitive"):
        content = content.lower()
        wanted = [w.lower() for w in wanted]
    if content in wanted:
        return True, f"is exactly {content[:60]!r}"
    return False, "does not exactly match any listed phrase"


async def is_reply(cog, ctx, config) -> Result:
    """Whether the message replies to another one.

    A forwarded message also carries a `reference`, so the reference *type*
    has to be checked — otherwise every forward would count as a reply.
    """
    if ctx.message is None:
        return False, "this trigger has no message"
    reference = getattr(ctx.message, "reference", None)
    replying = reference is not None and getattr(reference, "message_id", None) is not None
    if replying:
        reference_type = getattr(reference, "type", None)
        forward = getattr(discord, "MessageReferenceType", None)
        if forward is not None and reference_type == forward.forward:
            replying = False

    # `state` was how this used to be inverted, before negation moved into
    # every check's own settings. Automations saved back then still carry it,
    # so it is still honoured; new ones leave it unset and use the shared
    # "This check should — Not match" instead.
    want_reply = config.get("state", "yes") != "no"
    if replying == want_reply:
        return True, "is a reply" if replying else "is not a reply"
    return False, ("is a reply" if replying else "is not a reply")


async def content_length(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    length = len(ctx.message.content or "")
    minimum = int(config.get("min") or 0)
    maximum = int(config.get("max") or 0)
    if minimum and length < minimum:
        return False, f"{length} characters, under the {minimum} minimum"
    if maximum and length > maximum:
        return False, f"{length} characters, over the {maximum} maximum"
    return True, f"{length} characters"


async def has_attachment(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    if ctx.message.attachments:
        return True, f"{len(ctx.message.attachments)} attachment(s)"
    return False, "no attachments"


async def has_link(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    if LINK_RE.search(ctx.message.content or ""):
        return True, "contains a link"
    return False, "no links"


async def has_invite(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    if INVITE_RE.search(ctx.message.content or ""):
        return True, "contains a Discord invite"
    return False, "no invite links"


async def has_image(cog, ctx, config) -> Result:
    """Image or video specifically, rather than any attached file."""
    if ctx.message is None:
        return False, "this trigger has no message"
    for attachment in ctx.message.attachments:
        content_type = attachment.content_type or ""
        if content_type.startswith(("image/", "video/")):
            return True, f"has {attachment.filename}"
        # Discord does not always resolve a content type; fall back to the
        # extension rather than calling a screenshot "not an image".
        if attachment.filename.lower().endswith(
                (".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".mov", ".webm")):
            return True, f"has {attachment.filename}"
    if any(e.type in ("image", "video", "gifv") for e in ctx.message.embeds):
        return True, "has an image or video embed"
    return False, "no image or video"


async def has_emoji(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    content = ctx.message.content or ""
    if CUSTOM_EMOJI_RE.search(content):
        return True, "contains a custom emoji"
    # Unicode emoji live in these ranges; enough to catch real usage without
    # pulling in an emoji dependency.
    for char in content:
        code = ord(char)
        if (0x1F300 <= code <= 0x1FAFF or 0x2600 <= code <= 0x27BF
                or 0x1F000 <= code <= 0x1F2FF):
            return True, "contains an emoji"
    return False, "no emoji"


async def caps_ratio(cog, ctx, config) -> Result:
    """Proportion of letters that are uppercase — the SHOUTING check."""
    if ctx.message is None:
        return False, "this trigger has no message"
    content = ctx.message.content or ""
    letters = [c for c in content if c.isalpha()]
    minimum_length = int(config.get("min_length") or 8)
    if len(letters) < minimum_length:
        return False, f"only {len(letters)} letters, under the {minimum_length} minimum"
    ratio = sum(1 for c in letters if c.isupper()) / len(letters) * 100
    threshold = int(config.get("percent") or 70)
    if ratio >= threshold:
        return True, f"{ratio:.0f}% caps (threshold {threshold}%)"
    return False, f"{ratio:.0f}% caps, under the {threshold}% threshold"


async def user_is(cog, ctx, config) -> Result:
    member = _member(ctx)
    wanted = _ids(config, "users")
    if not wanted:
        return True, "no users configured, so this matches anyone"
    if member is None:
        return False, "no user on this event"
    if member.id in wanted:
        return True, f"is {member}"
    return False, f"{member} is not one of the {len(wanted)} listed"


async def is_boosting(cog, ctx, config) -> Result:
    member = _member(ctx)
    if member is None:
        return False, "no member on this event"
    if getattr(member, "premium_since", None) is not None:
        return True, "is boosting the server"
    return False, "is not boosting"


async def has_permission(cog, ctx, config) -> Result:
    member = _member(ctx)
    if member is None or not hasattr(member, "guild_permissions"):
        return False, "no member on this event"
    wanted = str(config.get("permission") or "").strip()
    if not wanted:
        return True, "no permission configured"
    held = getattr(member.guild_permissions, wanted, None)
    if held is None:
        return False, f"`{wanted}` is not a permission I know"
    if held:
        return True, f"has `{wanted}`"
    return False, f"does not have `{wanted}`"


async def in_thread(cog, ctx, config) -> Result:
    if ctx.channel is None:
        return False, "this trigger has no channel"
    is_thread = isinstance(ctx.channel, discord.Thread)
    # See is_reply: `state` is kept for automations saved before negation
    # became a per-check setting.
    want = config.get("state", "yes") != "no"
    if is_thread == want:
        return True, "is in a thread" if is_thread else "is not in a thread"
    return False, ("is in a thread" if is_thread else "is not in a thread")


async def day_of_week(cog, ctx, config) -> Result:
    from datetime import datetime, timezone
    wanted = {str(d) for d in as_list(config.get("days"))}
    if not wanted:
        return True, "no days configured"
    names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
             "Saturday", "Sunday"]
    today = datetime.now(timezone.utc).weekday()
    if str(today) in wanted:
        return True, f"it is {names[today]}"
    return False, f"it is {names[today]}, not one of the listed days"


async def variable_compare(cog, ctx, config) -> Result:
    """Compare a `set variable` value from earlier in the same run."""
    key = str(config.get("key", "")).strip().lower()
    if not key:
        return True, "no variable configured"
    value = str(ctx.variables.get(key, ""))
    expected = str(config.get("value", ""))
    operator = config.get("op", "eq")
    outcomes = {
        "eq": (value == expected, "equals"),
        "neq": (value != expected, "does not equal"),
        "contains": (expected in value, "contains"),
        "set": (value != "", "is set"),
    }
    matched, word = outcomes.get(operator, outcomes["eq"])
    if matched:
        return True, f"{{var.{key}}} ({value[:40]!r}) {word} the expected value"
    return False, f"{{var.{key}}} is {value[:40]!r}, which does not {word} {expected[:40]!r}"


async def mention_count(cog, ctx, config) -> Result:
    if ctx.message is None:
        return False, "this trigger has no message"
    total = len(set(ctx.message.raw_mentions)) + len(set(ctx.message.raw_role_mentions))
    minimum = int(config.get("min") or 1)
    if total >= minimum:
        return True, f"{total} mentions"
    return False, f"{total} mentions, under the {minimum} minimum"


# ------------------------------------------------------------------ account

async def account_age(cog, ctx, config) -> Result:
    member = _member(ctx)
    if member is None:
        return False, "no user on this event"
    days = (discord.utils.utcnow() - member.created_at).days
    minimum = int(config.get("min_days") or 0)
    maximum = int(config.get("max_days") or 0)
    if minimum and days < minimum:
        return False, f"account is {days} days old, under the {minimum} minimum"
    if maximum and days > maximum:
        return False, f"account is {days} days old, over the {maximum} maximum"
    return True, f"account is {days} days old"


async def member_age(cog, ctx, config) -> Result:
    member = _member(ctx)
    joined = getattr(member, "joined_at", None)
    if joined is None:
        return False, "not a member of this server"
    days = (discord.utils.utcnow() - joined).days
    minimum = int(config.get("min_days") or 0)
    maximum = int(config.get("max_days") or 0)
    if minimum and days < minimum:
        return False, f"joined {days} days ago, under the {minimum} minimum"
    if maximum and days > maximum:
        return False, f"joined {days} days ago, over the {maximum} maximum"
    return True, f"joined {days} days ago"


async def is_bot(cog, ctx, config) -> Result:
    member = _member(ctx)
    if member is None:
        return False, "no user on this event"
    if getattr(member, "bot", False):
        return True, "is a bot"
    return False, "is not a bot"


# ----------------------------------------------------------------- counters

async def counter_compare(cog, ctx, config) -> Result:
    key = str(config.get("key", "")).strip()
    if not key:
        return True, "no counter configured"
    scope = config.get("scope", "user")
    scope_id = {"user": ctx.user_id, "channel": ctx.channel_id}.get(scope, ctx.guild_id)
    value = await cog.db.counter_get(scope, scope_id, key)
    # Make it available to message templates without a second lookup.
    ctx.variables.setdefault("_counters", {})[key] = value
    threshold = int(config.get("value") or 0)
    operator = config.get("op", "gte")
    outcomes = {
        "gte": (value >= threshold, "at least"),
        "lte": (value <= threshold, "at most"),
        "eq": (value == threshold, "exactly"),
        "lt": (value < threshold, "under"),
        "gt": (value > threshold, "over"),
    }
    matched, word = outcomes.get(operator, outcomes["gte"])
    if matched:
        return True, f"{key} is {value} ({word} {threshold})"
    return False, f"{key} is {value}, not {word} {threshold}"


# --------------------------------------------------------------------- misc

async def chance(cog, ctx, config) -> Result:
    import random
    percent = max(0, min(100, int(config.get("percent") or 50)))
    if random.randint(1, 100) <= percent:
        return True, f"{percent}% roll succeeded"
    return False, f"{percent}% roll failed"


async def time_window(cog, ctx, config) -> Result:
    """Between two `HH:MM` times UTC, wrapping over midnight if needed."""
    from datetime import datetime, timezone
    start = str(config.get("start", "")) or "00:00"
    end = str(config.get("end", "")) or "23:59"

    def _minutes(text: str) -> int:
        try:
            hours, _, mins = text.partition(":")
            return int(hours) * 60 + int(mins or 0)
        except ValueError:
            return 0

    now_minutes = (lambda n: n.hour * 60 + n.minute)(datetime.now(timezone.utc))
    start_m, end_m = _minutes(start), _minutes(end)
    inside = (start_m <= now_minutes <= end_m if start_m <= end_m
              else now_minutes >= start_m or now_minutes <= end_m)
    if inside:
        return True, f"inside {start}–{end} UTC"
    return False, f"outside {start}–{end} UTC"


async def level_at_least(cog, ctx, config) -> Result:
    """Reads the economy cog's level, which is where levels actually live."""
    minimum = int(config.get("level") or 0)
    economy = ctx.bot.get_cog("Economy") if ctx.bot else None
    db = getattr(economy, "db", None)
    if db is None:
        return False, "the economy cog is not loaded, so levels are unavailable"
    try:
        row = await db.fetchone("SELECT level FROM users WHERE user_id = ?", (ctx.user_id,))
    except Exception as e:
        logger.warning(f"level lookup failed: {e}")
        return False, "could not read the level"
    level = row["level"] if row else 0
    if level >= minimum:
        return True, f"level {level} (needs {minimum})"
    return False, f"level {level}, under the {minimum} required"
