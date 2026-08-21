"""Building an embed from panel settings.

Shared by every action that sends something, so "send it as an embed" works
the same whether the message is going to a channel, a reply, a DM or a log.

Two things here exist because of how Discord actually behaves rather than
because anyone asked for them:

* **Pings do not work inside an embed.** A `{user.mention}` in an embed's text
  renders as a link but never notifies anyone. That is a genuinely surprising
  rule, and someone building a "welcome them and ping them" automation will
  hit it and conclude the bot is broken. So there is a separate *text above
  the embed* field, its help says exactly this, and the panel warns when an
  embed contains a mention but the text above it does not.

* **A bad image URL rejects the whole embed**, not just the image. Discord
  returns a 400 and nothing is sent at all. So URLs are checked here, and a
  bad one is dropped with a note rather than being allowed to take the entire
  message down with it.
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

import discord

from utils import placeholders as variables
from utils.fieldspec import Field, FieldType

logger = logging.getLogger('utils.embed_builder')

URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)
MENTION_RE = re.compile(r"<@[!&]?\d+>|@everyone|@here|\{user\.mention\}")

COLOUR_CHOICES = [
    ("blurple", "Discord blurple"),
    ("green", "Green"),
    ("red", "Red"),
    ("orange", "Orange"),
    ("yellow", "Yellow"),
    ("blue", "Blue"),
    ("purple", "Purple"),
    ("pink", "Pink"),
    ("teal", "Teal"),
    ("white", "White"),
    ("black", "Black"),
    ("user", "Match their top role colour"),
]

_COLOURS = {
    "blurple": discord.Color.blurple,
    "green": discord.Color.green,
    "red": discord.Color.red,
    "orange": discord.Color.orange,
    "yellow": discord.Color.gold,
    "blue": discord.Color.blue,
    "purple": discord.Color.purple,
    "pink": lambda: discord.Color.from_rgb(255, 105, 180),
    "teal": discord.Color.teal,
    "white": lambda: discord.Color.from_rgb(255, 255, 255),
    "black": lambda: discord.Color.from_rgb(43, 45, 49),
}


def _visible(config: dict) -> bool:
    return bool(config.get("use_embed"))


def embed_fields(*, include_toggle: bool = True, prefix: str = "",
                 gate=None) -> List[Field]:
    """The settings that describe an embed, for any action that sends one.

    `prefix` namespaces every key (e.g. ``reply_``) so a second embed can live
    in the same config without clashing with the message's own — the button
    reply reuses this. `gate` is an extra visibility test all the fields share,
    used to hide the whole block behind a toggle like "add a button".
    """
    p = prefix

    def visible(config: dict) -> bool:
        if gate is not None and not gate(config):
            return False
        return bool(config.get(f"{p}use_embed"))

    fields: List[Field] = []
    if include_toggle:
        fields.append(Field(
            f"{p}use_embed", "Send it as an embed box", FieldType.BOOL,
            "An embed is the tidy coloured box, with room for a title and "
            "pictures.",
            visible_when=(gate if gate is not None else None)))
    fields += [
        Field(f"{p}text_above", "Text above the embed", FieldType.TEXT,
              "Pings only work out here — a ping inside an embed looks right "
              "but never notifies anyone.",
              placeholder="{user.mention}",
              visible_when=visible),
        Field(f"{p}embed_title", "Title", FieldType.TEXT,
              "The bold heading at the top.",
              visible_when=visible),
        Field(f"{p}embed_colour", "Colour", FieldType.CHOICE,
              "The stripe down the left-hand side.",
              choices=COLOUR_CHOICES, default="blurple",
              visible_when=visible),
        Field(f"{p}embed_thumbnail", "Small picture (top right)", FieldType.TEXT,
              "A link ending in .png, .jpg or .gif. Use {user.avatar} for "
              "their profile picture.",
              placeholder="{user.avatar}",
              visible_when=visible),
        Field(f"{p}embed_image", "Big picture (across the bottom)", FieldType.TEXT,
              "A link to an image or GIF. Tenor page links don't work — use "
              "the direct .gif link.",
              placeholder="https://example.com/welcome.gif",
              visible_when=visible),
        Field(f"{p}embed_footer", "Small text at the very bottom", FieldType.TEXT,
              visible_when=visible),
        Field(f"{p}embed_author", "Small text at the very top", FieldType.TEXT,
              "Sits above the title, next to a small icon.",
              visible_when=visible),
        Field(f"{p}embed_author_icon", "Icon next to that top text", FieldType.TEXT,
              "A link to an image. Use {user.avatar} for their profile picture.",
              visible_when=lambda c: visible(c) and bool(c.get(f"{p}embed_author"))),
    ]
    return fields


def _resolve_colour(config: dict, ctx) -> discord.Color:
    choice = str(config.get("embed_colour") or "blurple")
    if choice == "user":
        member = ctx.member or ctx.user
        colour = getattr(member, "colour", None)
        # A member with no coloured role reports the default, which renders as
        # no stripe at all — fall back so the embed still looks deliberate.
        if colour is not None and colour.value:
            return colour
        return discord.Color.blurple()
    factory = _COLOURS.get(choice, discord.Color.blurple)
    return factory()


def _clean_url(raw: str, ctx) -> Optional[str]:
    """Render placeholders, then keep it only if it is actually a URL."""
    if not raw:
        return None
    url = variables.render(str(raw), ctx).strip()
    if not url:
        return None
    if not URL_RE.match(url):
        logger.info(f"Dropping an image that isn't a usable link: {url[:80]!r}")
        return None
    return url


def build(config: dict, ctx) -> Tuple[Optional[str], Optional[discord.Embed]]:
    """Turn the settings into (plain text, embed).

    When embeds are off this is just the rendered message with no embed, so
    every caller can use the same two-value result.
    """
    body = variables.render(str(config.get("content", "")), ctx)

    if not config.get("use_embed"):
        return (body or None), None

    embed = discord.Embed(colour=_resolve_colour(config, ctx))

    title = variables.render(str(config.get("embed_title", "")), ctx).strip()
    if title:
        embed.title = title[:256]
    if body.strip():
        embed.description = body[:4000]

    thumbnail = _clean_url(config.get("embed_thumbnail"), ctx)
    if thumbnail:
        embed.set_thumbnail(url=thumbnail)
    image = _clean_url(config.get("embed_image"), ctx)
    if image:
        embed.set_image(url=image)

    footer = variables.render(str(config.get("embed_footer", "")), ctx).strip()
    if footer:
        embed.set_footer(text=footer[:2048])

    author = variables.render(str(config.get("embed_author", "")), ctx).strip()
    if author:
        icon = _clean_url(config.get("embed_author_icon"), ctx)
        embed.set_author(name=author[:256], icon_url=icon)

    above = variables.render(str(config.get("text_above", "")), ctx).strip()

    # An embed with nothing in it renders as a bare coloured bar, which looks
    # like a bug. Fall back to sending the text plainly.
    if not (embed.title or embed.description or embed.image.url
            or embed.thumbnail.url or embed.footer.text or embed.author.name):
        return (above or body or None), None

    return (above or None), embed


def warnings(config: dict) -> List[str]:
    """Things worth telling the admin at build time, not at 3am."""
    notes = []
    if not config.get("use_embed"):
        return notes

    inside = f"{config.get('content', '')} {config.get('embed_title', '')}"
    above = str(config.get("text_above") or "")
    if MENTION_RE.search(inside) and not MENTION_RE.search(above):
        notes.append(
            "There's a ping inside the embed. Pings don't notify anyone from "
            "in there — put it in **Text above the embed** instead.")

    for key, name in (("embed_image", "Big picture"),
                      ("embed_thumbnail", "Small picture"),
                      ("embed_author_icon", "Icon")):
        raw = str(config.get(key) or "").strip()
        if raw and "{" not in raw and not URL_RE.match(raw):
            notes.append(f"**{name}** doesn't look like a link, so it'll be skipped.")
        elif "tenor.com/view" in raw:
            notes.append(
                f"**{name}** is a Tenor page link, which won't show. Open the "
                f"GIF, right-click it and copy the image address instead.")
    return notes
