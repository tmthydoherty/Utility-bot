"""Media-only channels.

A media channel deletes anything that isn't media and opens a comment thread
under everything that is.

The version this replaces counted a message as media only if it carried an
attachment whose content type began with `image/` or `video/`. That deleted
Tenor and YouTube links, GIF embeds, stickers, audio files, and anything
Discord had not finished processing a content type for — in a channel whose
whole purpose is people posting clips, most of which arrive as links. What
counts as media is now per-channel and defaults to permissive.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from typing import Optional

import discord

from ..storage import loads

logger = logging.getLogger('cogs.utility.media')

# Deliberately loose: this decides whether a message is *allowed*, so a false
# positive leaves a message up and a false negative deletes someone's post.
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

# Last-post time per (channel, user) for the optional cooldown. In memory
# only — a cooldown that survives a restart would punish people for the bot
# restarting, and the worst case of losing it is one extra post.
_last_post: dict[tuple[int, int], float] = {}


class ThreadDeleteButton(discord.ui.DynamicItem[discord.ui.Button],
                         template=r"util:mediathread:(?P<op>\d+)"):
    """Deletes a media comment thread.

    The op's id is carried *in the custom_id*. The previous version stored it
    on the view instance behind a static custom_id and never registered the
    view, so every one of these buttons stopped working the moment the bot
    restarted — which, on a thread that auto-archives in an hour, meant most
    of them.
    """

    def __init__(self, op_id: int):
        self.op_id = op_id
        super().__init__(
            discord.ui.Button(
                label="Delete thread",
                style=discord.ButtonStyle.danger,
                custom_id=f"util:mediathread:{op_id}",
            )
        )

    @classmethod
    async def from_custom_id(cls, interaction, item, match: re.Match):
        return cls(int(match["op"]))

    async def callback(self, interaction: discord.Interaction):
        user = interaction.user
        allowed = user.id == self.op_id
        if not allowed:
            perms = getattr(user, "guild_permissions", None)
            allowed = bool(perms and (perms.manage_threads or perms.administrator))
        if not allowed:
            checker = getattr(interaction.client, "is_bot_admin", None)
            if checker is not None:
                try:
                    allowed = bool(checker(user))
                except (AttributeError, TypeError):
                    allowed = False
        if not allowed:
            await interaction.response.send_message(
                "Only the original poster or a moderator can delete this thread.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message("Deleting thread…", ephemeral=True)
        try:
            await interaction.channel.delete()
        except discord.HTTPException as e:
            logger.warning(f"Could not delete media thread {interaction.channel_id}: {e}")


def _has_bypass(member: discord.Member, role_ids: list) -> bool:
    if not role_ids:
        return False
    wanted = {int(r) for r in role_ids}
    return any(r.id in wanted for r in getattr(member, "roles", []))


def qualifies_as_media(message: discord.Message, rule) -> bool:
    """Does this message carry something the channel accepts?"""
    if rule["allow_attachments"] and message.attachments:
        return True
    if rule["allow_stickers"] and message.stickers:
        return True
    if rule["allow_links"] and URL_RE.search(message.content or ""):
        return True
    if rule["allow_embeds"] and message.embeds:
        # Link embeds are resolved by Discord *after* the message arrives, so
        # at on_message time this is usually empty even for a link post. It is
        # the link check above that actually saves those; this catches
        # rich embeds posted by apps and forwarded messages.
        return True
    return False


def cooldown_remaining(rule, channel_id: int, user_id: int) -> float:
    seconds = rule["post_cooldown_s"] or 0
    if seconds <= 0:
        return 0.0
    last = _last_post.get((channel_id, user_id))
    if last is None:
        return 0.0
    return max(0.0, seconds - (time.time() - last))


def mark_posted(channel_id: int, user_id: int):
    _last_post[(channel_id, user_id)] = time.time()
    # The dict is only ever read for channels that still have a rule, but it
    # would otherwise grow for the life of the process.
    if len(_last_post) > 5000:
        cutoff = time.time() - 3600
        for key, ts in list(_last_post.items()):
            if ts < cutoff:
                _last_post.pop(key, None)


def render_thread_name(template: str, message: discord.Message) -> str:
    name = (template or "{user} - {date}")
    replacements = {
        "{user}": message.author.display_name,
        "{username}": message.author.name,
        "{date}": datetime.now().strftime("%Y-%m-%d"),
        "{time}": datetime.now().strftime("%H:%M"),
        "{channel}": getattr(message.channel, "name", ""),
    }
    for token, value in replacements.items():
        name = name.replace(token, str(value))
    return (name.strip() or message.author.display_name)[:100]


async def handle_message(cog, message: discord.Message, rule) -> Optional[str]:
    """Enforce one media rule against one message.

    Returns a short reason string when the message was removed, else None.
    """
    member = message.author
    if _has_bypass(member, loads(rule["bypass_role_ids"], [])):
        return None

    if not qualifies_as_media(message, rule):
        reason = "no media"
        try:
            if rule["dm_on_delete"] and (message.content or message.attachments):
                await _dm_copy(message, "Your message was removed because "
                                        f"{message.channel.mention} only accepts media.")
            await message.delete()
        except discord.Forbidden:
            logger.warning(
                f"Cannot delete in #{getattr(message.channel, 'name', message.channel.id)} — "
                f"missing Manage Messages."
            )
            return None
        except discord.NotFound:
            pass
        return reason

    remaining = cooldown_remaining(rule, message.channel.id, member.id)
    if remaining > 0:
        try:
            if rule["dm_on_delete"]:
                await _dm_copy(message, f"You can post again in "
                                        f"{message.channel.mention} in {int(remaining)}s.")
            await message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass
        return "cooldown"

    mark_posted(message.channel.id, member.id)

    for emoji in loads(rule["auto_react"], []):
        try:
            await message.add_reaction(emoji)
        except (discord.HTTPException, TypeError) as e:
            logger.debug(f"Auto-react {emoji!r} failed: {e}")

    if rule["thread_enabled"]:
        await _open_thread(message, rule)
    return None


async def _dm_copy(message: discord.Message, note: str):
    """Hand back what was deleted, so a long post isn't just lost."""
    body = (message.content or "").strip()
    embed = discord.Embed(
        description=body[:4000] or "*(no text)*",
        color=discord.Color.orange(),
    )
    embed.set_footer(text=note[:2000])
    try:
        await message.author.send(embed=embed)
    except (discord.Forbidden, discord.HTTPException):
        pass  # DMs closed — not worth failing the deletion over


async def _open_thread(message: discord.Message, rule):
    try:
        thread = await message.create_thread(
            name=render_thread_name(rule["thread_name_template"], message),
            auto_archive_duration=rule["thread_archive_minutes"] or 60,
        )
    except discord.HTTPException as e:
        logger.error(f"Failed to create media thread in {message.channel.id}: {e}")
        return

    view = discord.ui.View(timeout=None)
    view.add_item(ThreadDeleteButton(message.author.id))
    try:
        await thread.send(
            content=(f"{message.author.mention} This thread is the comment section "
                     f"for your post. You can delete it with the button below."),
            view=view,
        )
    except discord.HTTPException as e:
        logger.warning(f"Created thread {thread.id} but could not post into it: {e}")
