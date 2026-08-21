"""Turning a stored reminder/sticky row into a Discord message.

One place builds the message so a reminder, a one-off post and a sticky all
look the same and honour the same rules. Two of those rules are Discord's, not
ours, and the reminder cog this replaced learned them the hard way:

* **A ping inside an embed never notifies.** `<@&role>` in an embed renders as
  a link and silences. So the role mention goes in the *content above* the
  embed, and `send()` must pass `allowed_mentions` that permit roles or even
  that is swallowed.

* **A bad image URL rejects the whole message**, not just the image — Discord
  returns 400 and nothing sends. The dashboard validates URLs before saving;
  here we simply avoid setting empty ones.

`data` is a plain dict (callers pass `dict(row)`), with the JSON columns still
as strings — `_load` parses them tolerantly.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import discord

from ..storage import loads
from . import schedule as schedule_math

logger = logging.getLogger('cogs.utility.render')

DEFAULT_COLOR = 0x57F287

# Permit role pings from a reminder's content line, and nothing else — a stray
# @everyone in an admin's text should not go out to the whole server.
ALLOWED_MENTIONS = discord.AllowedMentions(everyone=False, roles=True, users=False)

_STYLES = {
    "primary": discord.ButtonStyle.primary,
    "secondary": discord.ButtonStyle.secondary,
    "success": discord.ButtonStyle.success,
    "danger": discord.ButtonStyle.danger,
    "link": discord.ButtonStyle.link,
}


def _load(data: dict, key: str, default):
    val = data.get(key)
    if isinstance(val, (dict, list)):
        return val
    return loads(val, default)


def _event_suffix(data: dict) -> str:
    """The countdown appended to the body, if this reminder tracks an event."""
    try:
        event = _load(data, "event_schedule_json", None)
        if event:
            ts = schedule_math.get_next_event_time(event)
            if ts:
                return f"\n\n<t:{ts}:F>\n(<t:{ts}:R>)"
        legacy = data.get("event_timestamp_utc")
        if legacy:
            return f"\n\n<t:{int(legacy)}:F>\n(<t:{int(legacy)}:R>)"
    except Exception as e:  # a bad schedule must not stop the post
        logger.warning(f"Event countdown failed: {e}")
    return ""


def build_embed(data: dict) -> Optional[discord.Embed]:
    ej = _load(data, "embed_json", {}) or {}
    desc = ej.get("description") or ""
    desc += _event_suffix(data)
    if data.get("use_timestamp"):
        desc += f"\n\n*Posted:* <t:{int(time.time())}:F>"

    title = ej.get("title") or None
    color = ej.get("color")
    embed = discord.Embed(
        title=title,
        description=desc or None,
        color=discord.Color(color) if color is not None else discord.Color(DEFAULT_COLOR),
        url=ej.get("url") or None,
    )
    if ej.get("image_url"):
        embed.set_image(url=ej["image_url"])
    if ej.get("thumbnail_url"):
        embed.set_thumbnail(url=ej["thumbnail_url"])
    author = ej.get("author") or {}
    if isinstance(author, dict) and author.get("name"):
        embed.set_author(
            name=author["name"],
            url=author.get("url") or None,
            icon_url=author.get("icon_url") or None,
        )
    footer = ej.get("footer") or {}
    if isinstance(footer, dict) and footer.get("text"):
        embed.set_footer(text=footer["text"], icon_url=footer.get("icon_url") or None)
    for field in ej.get("fields") or []:
        if not isinstance(field, dict):
            continue
        name = (field.get("name") or "​")[:256]
        value = (field.get("value") or "​")[:1024]
        embed.add_field(name=name, value=value, inline=bool(field.get("inline")))
    return embed


def build_content(data: dict) -> str:
    """Text above the embed — the ping, plus any content the admin typed."""
    parts = []
    if data.get("ping_role_id"):
        parts.append(f"<@&{int(data['ping_role_id'])}>")
    content = (data.get("content") or "").strip()
    if content:
        parts.append(content)
    return "\n".join(parts)


def build_plain_content(data: dict) -> str:
    """Everything as plain text, for reminders with the embed turned off."""
    ej = _load(data, "embed_json", {}) or {}
    parts = []
    if data.get("ping_role_id"):
        parts.append(f"<@&{int(data['ping_role_id'])}>")
    if (data.get("content") or "").strip():
        parts.append(data["content"].strip())
    if ej.get("title"):
        parts.append(f"**{ej['title']}**")
    body = (ej.get("description") or "") + _event_suffix(data)
    if data.get("use_timestamp"):
        body += f"\n\n*Posted:* <t:{int(time.time())}:F>"
    if body.strip():
        parts.append(body)
    return "\n".join(parts)[:2000]


class ReactionRoleButton(discord.ui.Button):
    """A button that toggles one role on the clicker.

    The custom_id encodes the role id (`remind_role:<id>`), so the cog's
    `on_interaction` can handle a click on any message from any past post
    without the view having to survive a restart.
    """

    def __init__(self, spec: dict):
        style = _STYLES.get(spec.get("button_style", "secondary"), discord.ButtonStyle.secondary)
        if style is discord.ButtonStyle.link:
            style = discord.ButtonStyle.secondary
        super().__init__(
            style=style,
            label=spec.get("button_label") or spec.get("label") or "Get Role",
            custom_id=f"remind_role:{int(spec['role_id'])}",
            emoji=spec.get("button_emoji") or spec.get("emoji") or None,
        )


def build_view(data: dict) -> Optional[discord.ui.View]:
    view = discord.ui.View(timeout=None)
    added = False

    reaction_role = _load(data, "reaction_role_json", None)
    if isinstance(reaction_role, dict) and reaction_role.get("role_id"):
        try:
            view.add_item(ReactionRoleButton(reaction_role))
            added = True
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Bad reaction_role button skipped: {e}")

    for btn in _load(data, "buttons_json", []) or []:
        if not isinstance(btn, dict):
            continue
        try:
            if btn.get("type") == "link" and btn.get("url"):
                view.add_item(discord.ui.Button(
                    style=discord.ButtonStyle.link,
                    label=btn.get("label") or "Open",
                    url=btn["url"],
                    emoji=btn.get("emoji") or None,
                ))
                added = True
            elif btn.get("role_id"):
                view.add_item(ReactionRoleButton(btn))
                added = True
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Bad button skipped: {e}")
    return view if added else None


def render(data: dict) -> dict:
    """(content, embed, view) for a send, honouring plain-text mode."""
    if data.get("plain_text"):
        return {"content": build_plain_content(data), "embed": None, "view": build_view(data)}
    return {"content": build_content(data), "embed": build_embed(data), "view": build_view(data)}
