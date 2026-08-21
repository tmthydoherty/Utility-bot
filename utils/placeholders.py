"""Placeholders, and the regex safety rules.

Every text an action sends goes through `render`, so `{user.mention}` works in
a message body, a thread name, a log line or a nickname without each action
having to know about substitution.

Regex is the sharp edge here. Python's `re` does not release the GIL while
matching, so a catastrophically backtracking pattern would freeze the *whole*
bot, not just one automation — and it cannot be interrupted by a timeout,
because there is no thread to interrupt it from. Running it in a worker thread
does not help for the same reason. The honest mitigations, all applied at save
time rather than match time, are:

* reject patterns that fail to compile,
* cap the pattern length,
* cap the subject length, and
* refuse the nested-quantifier shapes (`(a+)+`, `(.*)*`) that cause almost
  every accidental blow-up.

That catches the realistic mistakes. It is not a sandbox: an admin determined
to write a pathological pattern can still write one. Admins can also ban
everyone, so the trust boundary is the same — but the limitation is real and
worth knowing rather than papering over.
"""

from __future__ import annotations

import logging
import random
import re
from typing import Any, Optional

logger = logging.getLogger('utils.placeholders')

MAX_PATTERN_LENGTH = 200
MAX_SUBJECT_LENGTH = 2000        # a Discord message cannot exceed this anyway
_PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\.([a-z0-9_\-]+)\}", re.IGNORECASE)

# `{#general}` and `{@Moderators}` — linking a channel or role by name.
# Discord's own syntax is `<#1431565437224751245>`, which means digging a
# 19-digit id out of the client before you can mention a channel in a message.
# Nobody should have to do that to write "check out {#rules}".
_CHANNEL_RE = re.compile(r"\{#([^{}]{1,100})\}")
_ROLE_RE = re.compile(r"\{@([^{}]{1,100})\}")


def _normalise(name: str) -> str:
    """Loose matching, so `{#rules}` finds "📜-rules" and `{@mods}` finds "Mods"."""
    return "".join(c for c in name.lower() if c.isalnum())


def _link_channel(guild, name: str) -> Optional[str]:
    if guild is None:
        return None
    wanted = _normalise(name)
    if not wanted:
        return None
    channels = list(getattr(guild, "channels", []) or [])
    channels += list(getattr(guild, "threads", []) or [])
    # An exact normalised match wins; otherwise fall back to a unique
    # containing match, so `{#rules}` still finds "server-rules".
    for channel in channels:
        if _normalise(getattr(channel, "name", "")) == wanted:
            return f"<#{channel.id}>"
    partial = [c for c in channels if wanted in _normalise(getattr(c, "name", ""))]
    if len(partial) == 1:
        return f"<#{partial[0].id}>"
    return None


def _link_role(guild, name: str) -> Optional[str]:
    if guild is None:
        return None
    if name.lower() in ("everyone", "here"):
        return f"@{name.lower()}"
    wanted = _normalise(name)
    if not wanted:
        return None
    roles = list(getattr(guild, "roles", []) or [])
    for role in roles:
        if _normalise(getattr(role, "name", "")) == wanted:
            return f"<@&{role.id}>"
    partial = [r for r in roles if wanted in _normalise(getattr(r, "name", ""))]
    if len(partial) == 1:
        return f"<@&{partial[0].id}>"
    return None

# `(x+)+`, `(x*)*`, `(x+)*` and friends — a quantified group whose body is
# itself quantified is the classic exponential-backtracking shape.
_NESTED_QUANTIFIER_RE = re.compile(r"\([^)]*[+*][^)]*\)\s*[+*{]")

_compiled: dict[str, re.Pattern] = {}


class RegexRejected(ValueError):
    """Raised at save time so a bad pattern never reaches the hot path."""


def compile_pattern(pattern: str, *, ignore_case: bool = True) -> re.Pattern:
    """Validate and cache a user-supplied pattern. Raises `RegexRejected`."""
    if not pattern:
        raise RegexRejected("The pattern is empty.")
    if len(pattern) > MAX_PATTERN_LENGTH:
        raise RegexRejected(
            f"The pattern is {len(pattern)} characters; the limit is {MAX_PATTERN_LENGTH}.")
    if _NESTED_QUANTIFIER_RE.search(pattern):
        raise RegexRejected(
            "That pattern nests one repeat inside another (like `(a+)+`), which can "
            "hang on some inputs. Rewrite it without the inner repeat.")

    key = f"{int(ignore_case)}:{pattern}"
    cached = _compiled.get(key)
    if cached is not None:
        return cached
    try:
        compiled = re.compile(pattern, re.IGNORECASE if ignore_case else 0)
    except re.error as e:
        raise RegexRejected(f"Not a valid pattern: {e}") from e
    if len(_compiled) > 500:
        _compiled.clear()
    _compiled[key] = compiled
    return compiled


def search(pattern: str, subject: str, *, ignore_case: bool = True) -> Optional[re.Match]:
    """Match against a length-capped subject. Returns None on a bad pattern."""
    try:
        compiled = compile_pattern(pattern, ignore_case=ignore_case)
    except RegexRejected as e:
        logger.warning(f"Skipping rejected pattern {pattern!r}: {e}")
        return None
    return compiled.search((subject or "")[:MAX_SUBJECT_LENGTH])


# ------------------------------------------------------------- placeholders

def _user_value(ctx, attribute: str) -> Optional[str]:
    user = ctx.member or ctx.user
    if user is None:
        return None
    if attribute == "mention":
        return user.mention
    if attribute in ("name", "username"):
        return user.name
    if attribute in ("display", "displayname", "nick"):
        return getattr(user, "display_name", user.name)
    if attribute == "id":
        return str(user.id)
    if attribute == "tag":
        return str(user)
    if attribute == "avatar":
        avatar = getattr(user, "display_avatar", None)
        return avatar.url if avatar else None
    if attribute in ("joined_days", "member_days"):
        joined = getattr(user, "joined_at", None)
        if joined is None:
            return None
        import discord
        return str((discord.utils.utcnow() - joined).days)
    if attribute in ("account_days", "created_days"):
        created = getattr(user, "created_at", None)
        if created is None:
            return None
        import discord
        return str((discord.utils.utcnow() - created).days)
    return None


def _message_value(ctx, attribute: str) -> Optional[str]:
    message = ctx.message
    if message is None:
        return None
    if attribute == "content":
        return message.content or ""
    if attribute == "link":
        return message.jump_url
    if attribute == "id":
        return str(message.id)
    return None


def _channel_value(ctx, attribute: str) -> Optional[str]:
    channel = ctx.channel
    if channel is None:
        return None
    if attribute == "mention":
        return getattr(channel, "mention", "")
    if attribute == "name":
        return getattr(channel, "name", "")
    if attribute == "id":
        return str(channel.id)
    return None


def _guild_value(ctx, attribute: str) -> Optional[str]:
    guild = ctx.guild
    if guild is None:
        return None
    if attribute == "name":
        return guild.name
    if attribute == "id":
        return str(guild.id)
    if attribute in ("members", "member_count"):
        return str(guild.member_count or 0)
    return None


def _random_value(_ctx, attribute: str) -> Optional[str]:
    # `{random.1-100}` — the range is in the attribute so it needs no field.
    if "-" in attribute:
        low, _, high = attribute.partition("-")
        try:
            return str(random.randint(int(low), int(high)))
        except ValueError:
            return None
    try:
        return str(random.randint(1, int(attribute)))
    except ValueError:
        return None


_RESOLVERS = {
    "user": _user_value,
    "member": _user_value,
    "message": _message_value,
    "channel": _channel_value,
    "guild": _guild_value,
    "server": _guild_value,
    "random": _random_value,
}


def render(template: str, ctx) -> str:
    """Substitute every `{namespace.attribute}` this context can resolve.

    An unresolvable placeholder is left as written rather than blanked, so a
    typo shows up in the output instead of silently producing an empty
    message.
    """
    if not template or "{" not in template:
        return template or ""

    guild = getattr(ctx, "guild", None)

    def _channel_link(match: re.Match) -> str:
        return _link_channel(guild, match.group(1)) or match.group(0)

    def _role_link(match: re.Match) -> str:
        return _link_role(guild, match.group(1)) or match.group(0)

    # Names first: these can't collide with the dotted form, and resolving
    # them here keeps the main resolver dealing only with `a.b`.
    template = _CHANNEL_RE.sub(_channel_link, template)
    template = _ROLE_RE.sub(_role_link, template)

    def _replace(match: re.Match) -> str:
        namespace, attribute = match.group(1).lower(), match.group(2).lower()

        if namespace == "match":
            # Regex capture groups from the condition that matched.
            captures = ctx.variables.get("_captures") or {}
            return str(captures.get(attribute, match.group(0)))
        if namespace == "counter":
            counters = ctx.variables.get("_counters") or {}
            return str(counters.get(attribute, match.group(0)))
        if namespace == "var":
            value = ctx.variables.get(attribute)
            return match.group(0) if value is None else str(value)

        resolver = _RESOLVERS.get(namespace)
        if resolver is None:
            return match.group(0)
        try:
            value = resolver(ctx, attribute)
        except Exception as e:
            logger.debug(f"Placeholder {match.group(0)} failed: {e}")
            return match.group(0)
        return match.group(0) if value is None else str(value)

    return _PLACEHOLDER_RE.sub(_replace, template)


def store_captures(ctx, match: re.Match):
    """Expose a matched pattern's groups as `{match.1}` / `{match.name}`."""
    captures: dict[str, Any] = {"0": match.group(0)}
    for index, value in enumerate(match.groups(), start=1):
        captures[str(index)] = value if value is not None else ""
    for name, value in (match.groupdict() or {}).items():
        captures[name.lower()] = value if value is not None else ""
    ctx.variables["_captures"] = captures


# Shown on the "What can I write?" screen, grouped the way someone would look
# for them rather than by how they are implemented.
PLACEHOLDER_HELP = [
    ("Links", [
        ("{#channel-name}", "A clickable link to that channel, e.g. `{#rules}`"),
        ("{@role-name}", "Pings that role, e.g. `{@Moderators}`"),
        ("{user.mention}", "Pings the person who set this off"),
        ("{channel.mention}", "The channel it happened in"),
        ("{message.link}", "A jump link to their message"),
    ]),
    ("About them", [
        ("{user.name}", "Their username"),
        ("{user.display}", "Their nickname here"),
        ("{user.avatar}", "Their profile picture — for embed images"),
        ("{user.joined_days}", "Days since they joined the server"),
        ("{user.account_days}", "How old their Discord account is, in days"),
        ("{user.id}", "Their user ID"),
    ]),
    ("About the server", [
        ("{guild.name}", "The server's name"),
        ("{guild.members}", "How many members it has"),
        ("{channel.name}", "The channel name, without a link"),
    ]),
    ("About the message", [
        ("{message.content}", "What they actually wrote"),
        ("{match.1}", "Text captured by a search-pattern check"),
    ]),
    ("Other", [
        ("{counter.name}", "The current value of a tally"),
        ("{var.name}", "Something saved earlier by 'Save a note'"),
        ("{random.1-100}", "A random number in that range"),
    ]),
]
