"""The click-a-button-for-a-private-reply action helper.

A `send_message` step can carry a button. When someone clicks it, the bot
sends them an ephemeral message only they can see — optionally a full embed,
built from the same settings any other embed uses. Nothing else happens: no
roles, no state, no side effects. It is a "read more" / "get the rules" button.

The hard part is that the message is posted at run time, long before anyone
clicks, and the bot may restart in between. So the button can't rely on a view
kept in memory. Instead its `custom_id` carries a short id, and the click is
matched back to its configuration by a `DynamicItem` template that Discord
re-attaches after every restart. At click time the automation is re-read from
the database, so editing the reply on the dashboard updates buttons already
sitting in the channel.
"""

from __future__ import annotations

import logging

import discord

from utils.events import EventContext
from utils import embed_builder as embeds

logger = logging.getLogger('cogs.automations.buttons')

# How the reply's embed settings are namespaced inside the step config, so they
# never collide with the message's own embed. `reply_embed_title` is the button
# reply's title; `embed_title` is the message's.
REPLY_PREFIX = "reply_"

# Discord only offers four styles for a clickable (non-link) button.
BUTTON_STYLES = {
    "blurple": discord.ButtonStyle.primary,
    "green": discord.ButtonStyle.success,
    "grey": discord.ButtonStyle.secondary,
    "gray": discord.ButtonStyle.secondary,
    "red": discord.ButtonStyle.danger,
}


def reply_config(step_config: dict) -> dict:
    """The button reply's settings, un-prefixed back to the embed builder's keys.

    `reply_embed_title` → `embed_title`, `reply_content` → `content`, and so on,
    so the shared `embed_builder.build()` can render it exactly like any other
    message without knowing a button was involved.
    """
    out: dict = {}
    for key, value in step_config.items():
        if key.startswith(REPLY_PREFIX):
            out[key[len(REPLY_PREFIX):]] = value
    return out


def _emoji(raw):
    """A button emoji, parsed from a token or unicode, or None.

    Tolerates the value arriving as a one-item list, the way the picker stores
    a multi-emoji field, as well as a plain string.
    """
    if isinstance(raw, (list, tuple)):
        raw = raw[0] if raw else ""
    token = str(raw or "").strip()
    if not token:
        return None
    try:
        return discord.PartialEmoji.from_str(token)
    except (ValueError, TypeError):
        return None


def build_button(button_id: str, config: dict) -> "AutomationButton":
    """The button to attach when a `send_message` step with one is posted."""
    style = BUTTON_STYLES.get(str(config.get("button_colour") or "blurple").lower(),
                              discord.ButtonStyle.primary)
    emoji = _emoji(config.get("button_emoji"))
    label = str(config.get("button_label") or "").strip()[:80]
    # Discord needs a label or an emoji. An empty label with an emoji set means
    # "the emoji on its own" — so it's left off. Only when there's no emoji
    # either does it fall back to a word, since a button with neither is invalid.
    if not label and emoji is None:
        label = "Click me"
    return AutomationButton(
        button_id,
        label=label or None,
        style=style,
        emoji=emoji,
    )


class AutomationButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"vibey:autobtn:(?P<bid>[A-Za-z0-9_-]{1,40})",
):
    """A persistent button that opens a private reply.

    The `bid` in the custom id is the step's `button_id`. On a click Discord
    hands us that id; we find the step it belongs to and render its reply. The
    button's own look (label, colour, emoji) is already baked into the message
    Discord stored, so the copy rebuilt here for the callback needs only the id.
    """

    def __init__(self, button_id: str, *, label: str = "Click me",
                 style: discord.ButtonStyle = discord.ButtonStyle.primary,
                 emoji=None):
        self.button_id = button_id
        super().__init__(
            discord.ui.Button(
                label=label,
                style=style,
                emoji=emoji,
                custom_id=f"vibey:autobtn:{button_id}",
            )
        )

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(match["bid"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Automations")
        step = await cog.find_button_step(self.button_id) if cog else None
        if step is None:
            # The automation was deleted or the button removed since it was
            # posted — say so quietly rather than failing silently.
            await interaction.response.send_message(
                "This button isn't set up any more.", ephemeral=True)
            return

        # The reply is addressed to whoever clicked, so its placeholders
        # ({user.mention} and friends) resolve to them, not the person the
        # automation originally ran for.
        ctx = EventContext(
            event="button_click", bot=interaction.client,
            guild=interaction.guild,
            member=interaction.user if hasattr(interaction.user, "roles") else None,
            user=interaction.user,
            channel=interaction.channel,
        )
        content, embed = embeds.build(reply_config(step.config), ctx)
        if not (content or embed):
            content = "…"  # An empty reply would be an error; show something.
        try:
            await interaction.response.send_message(
                content=content, embed=embed, ephemeral=True)
        except discord.HTTPException as e:
            logger.warning(f"Button {self.button_id} reply failed: {e}")
