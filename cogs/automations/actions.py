"""Action implementations.

Each takes `(cog, ctx, config)` and returns a short line describing what it
did, which becomes the run-log entry. Raising is fine — the engine catches,
records the failure against the step, and stops that automation's run without
touching the others.

`ctx.simulate` is the dry-run flag. Every action that touches Discord must
check it and return the sentence it *would* have logged, because dry-run is
how a new automation gets watched for a day before being armed, and an action
that ignores the flag makes that guarantee a lie.

Punitive moderation stops at timeout. Kick and ban live in security.py and
modtools.py, which have the infraction history and appeal flow that make those
decisions reviewable; an automation firing them from a mistyped condition
would be both unreviewable and irreversible.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import discord

from utils.fieldspec import as_list
from .storage import now
from . import buttons
from utils import embed_builder as embeds
from utils import placeholders as variables

logger = logging.getLogger('cogs.automations.actions')


class ActionError(RuntimeError):
    """A failure worth showing in the run log rather than a stack trace."""


def _reaction_emoji(raw: str):
    """An emoji Discord will accept as a reaction.

    A custom emoji arrives as its token — ``<:name:id>`` or the animated
    ``<a:name:id>``. Handed straight to ``add_reaction`` as a string, discord.py
    only ``strip('<>')``s it, leaving ``:name:id`` with a leading colon the
    reaction endpoint rejects, so custom-emoji reactions silently fail. Parsing
    the token into a PartialEmoji first hands the API the ``name:id`` form it
    wants. Unicode emoji are left as the plain string they already are.
    """
    token = raw.strip()
    if token.startswith("<") and token.endswith(">"):
        try:
            return discord.PartialEmoji.from_str(token)
        except (ValueError, TypeError):
            return token
    return token


def _member(ctx) -> Optional[discord.Member]:
    """The guild member this event is about, if there is one.

    Tested for the capability rather than the class: a message author in a DM
    or from a webhook has no `roles`, which is exactly what "not a member"
    means here, and the check keeps working for anything Discord-shaped.
    """
    member = ctx.member
    return member if member is not None and hasattr(member, "roles") else None


def _target_message(ctx, config, what: str):
    """Which message an action should act on.

    `trigger` is the message that set the automation off. `sent` is the last
    message the bot posted during this run, which is what makes "post an
    announcement, then react to it" possible — and is the only way to touch
    something the bot itself created, since no event fires for it.
    """
    if config.get("target") == "sent":
        message = getattr(ctx, "last_sent", None)
        if message is None:
            raise ActionError(
                f"there's no message from me to {what} yet — put a "
                f"'Send a message' step before this one")
        return message
    message = ctx.message
    if message is None:
        raise ActionError(
            f"there's no message to {what} on this event. If you meant the one "
            f"I just sent, change 'Which message' to that")
    return message


def _first_id(config, key) -> int:
    values = as_list(config.get(key))
    if values:
        try:
            return int(values[0])
        except (TypeError, ValueError):
            return 0
    raw = config.get(key)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


async def _resolve_channel(ctx, channel_id: int):
    if not channel_id:
        return ctx.channel
    channel = ctx.guild.get_channel_or_thread(channel_id) if ctx.guild else None
    if channel is None and ctx.bot is not None:
        try:
            channel = await ctx.bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            channel = None
    return channel


# ----------------------------------------------------------------- messaging

def _button_view(config) -> Optional[discord.ui.View]:
    """A one-button view when the step is set to attach one, else None.

    The button's id is stamped in by the dashboard when it's turned on; without
    one there is nothing to match a click back to, so the button is skipped
    rather than posted as a dead control.
    """
    if not config.get("add_button"):
        return None
    button_id = str(config.get("button_id") or "").strip()
    if not button_id:
        return None
    view = discord.ui.View(timeout=None)
    view.add_item(buttons.build_button(button_id, config))
    return view


async def send_message(cog, ctx, config) -> str:
    content, embed = embeds.build(config, ctx)
    view = _button_view(config)
    if not (content or embed):
        raise ActionError("there's nothing to send — the message is empty")

    preview = (content or (embed.title or embed.description or "an embed"))[:80]
    destination = config.get("destination", "channel")
    # discord.py rejects view=None with a type error on some paths, so a message
    # with no button passes an empty kwargs dict instead.
    extra = {"view": view} if view is not None else {}

    if destination == "dm":
        target = _member(ctx) or ctx.user
        if target is None:
            raise ActionError("there's nobody to DM on this event")
        if ctx.simulate:
            return f"would DM {target}: {preview}"
        try:
            sent = await target.send(content=content, embed=embed, **extra)
        except discord.Forbidden:
            return f"couldn't DM {target} — they have DMs turned off"
        ctx.last_sent = sent
        return f"DMed {target}"

    if destination == "reply":
        if ctx.message is None:
            raise ActionError("there's no message to reply to on this event")
        if ctx.simulate:
            return f"would reply: {preview}"
        sent = await ctx.message.reply(content=content, embed=embed,
                                       mention_author=bool(config.get("ping")),
                                       **extra)
        ctx.last_sent = sent
        return "replied to their message"

    channel = await _resolve_channel(ctx, _first_id(config, "channel"))
    if channel is None:
        raise ActionError("that channel doesn't exist any more")
    name = getattr(channel, "name", channel.id)
    if ctx.simulate:
        return f"would post in #{name}: {preview}"
    sent = await channel.send(content=content, embed=embed, **extra)
    # Remembered so a later step can react to, pin or thread this message —
    # which is the only way to act on something the bot itself just posted.
    ctx.last_sent = sent
    return f"posted in #{name}"


async def delete_message(cog, ctx, config) -> str:
    if ctx.message is None:
        raise ActionError("no message to delete on this event")
    if ctx.simulate:
        return "would delete the message"
    try:
        await ctx.message.delete()
    except discord.NotFound:
        return "the message was already gone"
    except discord.Forbidden:
        raise ActionError("missing Manage Messages here")
    return "deleted the message"


async def add_reaction(cog, ctx, config) -> str:
    # Discord's own cap is 20 distinct reactions on a message.
    emojis = [str(e) for e in as_list(config.get("emoji"))][:20]
    if not emojis:
        raise ActionError("no emoji picked yet")
    where = ("the message I sent" if config.get("target") == "sent"
             else "their message")
    if ctx.simulate:
        return f"would react to {where} with {' '.join(emojis)}"

    message = _target_message(ctx, config, "react to")
    added, failed = [], []
    for emoji in emojis:
        try:
            await message.add_reaction(_reaction_emoji(emoji))
            added.append(emoji)
        except (discord.HTTPException, TypeError) as e:
            # One unusable emoji (from another server, say) must not cost the
            # rest of them.
            failed.append(emoji)
            logger.debug(f"Reaction {emoji!r} failed: {e}")
    if not added:
        raise ActionError(
            f"none of those emoji worked — {' '.join(failed)}. I can only use "
            f"standard emoji and ones from servers I'm in")
    note = f"reacted to {where} with {' '.join(added)}"
    if failed:
        note += f" (couldn't use {' '.join(failed)})"
    return note


async def remove_reaction(cog, ctx, config) -> str:
    if ctx.message is None:
        raise ActionError("no message to remove a reaction from")
    emojis = [str(e) for e in as_list(config.get("emoji"))][:5]
    if not emojis:
        raise ActionError("no emoji configured")
    if ctx.simulate:
        return f"would remove {' '.join(emojis)}"
    removed = []
    for emoji in emojis:
        try:
            # Removes the bot's own reaction; clearing everyone's is the
            # separate `clear_reactions` action, which needs Manage Messages.
            await ctx.message.remove_reaction(_reaction_emoji(emoji), ctx.guild.me)
            removed.append(emoji)
        except (discord.HTTPException, TypeError) as e:
            logger.debug(f"remove_reaction {emoji!r} failed: {e}")
    if not removed:
        return "there was nothing to remove"
    return f"removed {' '.join(removed)}"


async def clear_reactions(cog, ctx, config) -> str:
    if ctx.message is None:
        raise ActionError("no message to clear reactions from")
    if ctx.simulate:
        return "would clear every reaction"
    try:
        await ctx.message.clear_reactions()
    except discord.Forbidden:
        raise ActionError("missing Manage Messages")
    except discord.HTTPException as e:
        raise ActionError(f"could not clear reactions: {e}")
    return "cleared every reaction"


async def publish_message(cog, ctx, config) -> str:
    """Crosspost an announcement-channel message to following servers."""
    if ctx.message is None:
        raise ActionError("no message to publish")
    if not isinstance(ctx.channel, discord.TextChannel) or not ctx.channel.is_news():
        raise ActionError("this only works in an announcement channel")
    if ctx.simulate:
        return "would publish the message"
    try:
        await ctx.message.publish()
    except discord.Forbidden:
        raise ActionError("missing Manage Messages")
    except discord.HTTPException as e:
        # Already published is a 400, not worth failing the whole run over.
        return f"could not publish: {e}"
    return "published the message"


async def unpin_message(cog, ctx, config) -> str:
    if ctx.message is None:
        raise ActionError("no message to unpin")
    if ctx.simulate:
        return "would unpin the message"
    try:
        await ctx.message.unpin()
    except discord.HTTPException as e:
        raise ActionError(f"could not unpin: {e}")
    return "unpinned the message"


async def create_thread(cog, ctx, config) -> str:
    name = variables.render(str(config.get("name") or "{user} - discussion"), ctx)[:100]
    if ctx.simulate:
        return f"would start a thread called {name!r}"
    message = _target_message(ctx, config, "start a thread on")
    thread = await message.create_thread(
        name=name or "discussion",
        auto_archive_duration=int(config.get("archive_minutes") or 1440),
    )
    starter = variables.render(str(config.get("message", "")), ctx)
    if starter.strip():
        try:
            await thread.send(starter[:2000])
        except discord.HTTPException:
            pass
    return f"opened thread {name!r}"


async def pin_message(cog, ctx, config) -> str:
    where = ("the message I sent" if config.get("target") == "sent"
             else "their message")
    if ctx.simulate:
        return f"would pin {where}"
    message = _target_message(ctx, config, "pin")
    try:
        await message.pin()
    except discord.HTTPException as e:
        raise ActionError(f"couldn't pin it: {e}")
    return f"pinned {where}"


# --------------------------------------------------------------------- roles

async def _role_from(ctx, config, key="role") -> discord.Role:
    role_id = _first_id(config, key)
    role = ctx.guild.get_role(role_id) if ctx.guild else None
    if role is None:
        raise ActionError("that role no longer exists")
    me = ctx.guild.me
    if me is not None and role >= me.top_role:
        raise ActionError(
            f"@{role.name} sits above my highest role, so I cannot manage it")
    return role


async def add_role(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    role = await _role_from(ctx, config)
    duration = int(config.get("duration") or 0)

    if ctx.simulate:
        window = f" for {duration}s" if duration else ""
        return f"would add @{role.name}{window}"

    if role in member.roles:
        note = f"@{role.name} was already held"
    else:
        try:
            await member.add_roles(role, reason="Utility automation")
        except discord.Forbidden:
            raise ActionError("missing Manage Roles")
        note = f"added @{role.name}"

    if duration > 0:
        # Persisted rather than slept on, so a restart still removes it.
        await cog.db.temp_role_add(ctx.guild_id, member.id, role.id,
                                   now() + duration, config.get("_automation_id"))
        note += f", expiring in {duration}s"
    return note


async def remove_role(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    role = await _role_from(ctx, config)
    if ctx.simulate:
        return f"would remove @{role.name}"
    if role not in member.roles:
        return f"@{role.name} was not held"
    try:
        await member.remove_roles(role, reason="Utility automation")
    except discord.Forbidden:
        raise ActionError("missing Manage Roles")
    return f"removed @{role.name}"


async def toggle_role(cog, ctx, config) -> str:
    """Add the role if they lack it, remove it if they have it."""
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    role = await _role_from(ctx, config)
    held = role in member.roles
    if ctx.simulate:
        return f"would {'remove' if held else 'add'} @{role.name}"
    try:
        if held:
            await member.remove_roles(role, reason="Utility automation")
            return f"removed @{role.name}"
        await member.add_roles(role, reason="Utility automation")
        return f"added @{role.name}"
    except discord.Forbidden:
        raise ActionError("missing Manage Roles")


async def set_nickname(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    raw = str(config.get("nickname", ""))
    nickname = variables.render(raw, ctx)[:32] if raw.strip() else None
    if ctx.simulate:
        return f"would set their nickname to {nickname!r}" if nickname else \
               "would reset their nickname"
    try:
        await member.edit(nick=nickname, reason="Utility automation")
    except discord.Forbidden:
        raise ActionError("missing Manage Nicknames, or they outrank me")
    return f"set their nickname to {nickname!r}" if nickname else "reset their nickname"


# ---------------------------------------------------------------- moderation

async def timeout_member(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    seconds = int(config.get("duration") or 600)
    # Discord's own ceiling; asking for more is rejected outright.
    seconds = max(60, min(seconds, 28 * 86400))
    reason = variables.render(str(config.get("reason") or "Utility automation"), ctx)

    if ctx.simulate:
        return f"would time out {member} for {seconds}s"
    try:
        await member.timeout(discord.utils.utcnow() + __import__("datetime").timedelta(seconds=seconds),
                             reason=reason[:400])
    except discord.Forbidden:
        raise ActionError("missing Timeout Members, or they outrank me")
    return f"timed out {member} for {seconds}s"


async def remove_timeout(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    if ctx.simulate:
        return f"would lift the timeout on {member}"
    try:
        await member.timeout(None, reason="Utility automation")
    except discord.Forbidden:
        raise ActionError("missing Timeout Members, or they outrank me")
    return f"lifted the timeout on {member}"


# --------------------------------------------------------------------- voice

async def voice_disconnect(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    if getattr(member, "voice", None) is None:
        return "they are not in a voice channel"
    if ctx.simulate:
        return f"would disconnect {member} from voice"
    try:
        await member.move_to(None, reason="Utility automation")
    except discord.Forbidden:
        raise ActionError("missing Move Members")
    return f"disconnected {member} from voice"


async def voice_move(cog, ctx, config) -> str:
    member = _member(ctx)
    if member is None:
        raise ActionError("no member on this event")
    if getattr(member, "voice", None) is None:
        return "they are not in a voice channel"
    channel = await _resolve_channel(ctx, _first_id(config, "channel"))
    if not isinstance(channel, discord.VoiceChannel):
        raise ActionError("that is not a voice channel")
    if ctx.simulate:
        return f"would move {member} to {channel.name}"
    try:
        await member.move_to(channel, reason="Utility automation")
    except discord.Forbidden:
        raise ActionError("missing Move Members")
    return f"moved {member} to {channel.name}"


# ------------------------------------------------------------------ channel

async def set_slowmode(cog, ctx, config) -> str:
    channel = await _resolve_channel(ctx, _first_id(config, "channel"))
    if channel is None or not hasattr(channel, "edit"):
        raise ActionError("no channel to set slowmode on")
    seconds = max(0, min(int(config.get("seconds") or 0), 21600))
    if ctx.simulate:
        return f"would set slowmode to {seconds}s in #{getattr(channel, 'name', '')}"
    try:
        await channel.edit(slowmode_delay=seconds)
    except discord.Forbidden:
        raise ActionError("missing Manage Channels")
    return f"slowmode set to {seconds}s"


# ------------------------------------------------------------------ counters

async def counter_change(cog, ctx, config) -> str:
    key = str(config.get("key", "")).strip()
    if not key:
        raise ActionError("no counter name configured")
    scope = config.get("scope", "user")
    scope_id = {"user": ctx.user_id, "channel": ctx.channel_id}.get(scope, ctx.guild_id)
    delta = int(config.get("amount") or 1)
    ttl = int(config.get("window") or 0)

    if ctx.simulate:
        current = await cog.db.counter_get(scope, scope_id, key)
        return f"would change {key} from {current} to {current + delta}"

    value = await cog.db.counter_add(scope, scope_id, key, delta, ttl_s=ttl)
    ctx.variables.setdefault("_counters", {})[key] = value
    return f"{key} is now {value}"


async def counter_reset(cog, ctx, config) -> str:
    key = str(config.get("key", "")).strip()
    if not key:
        raise ActionError("no counter name configured")
    scope = config.get("scope", "user")
    scope_id = {"user": ctx.user_id, "channel": ctx.channel_id}.get(scope, ctx.guild_id)
    if ctx.simulate:
        return f"would reset {key}"
    await cog.db.counter_reset(scope, scope_id, key)
    ctx.variables.setdefault("_counters", {})[key] = 0
    return f"reset {key}"


async def set_variable(cog, ctx, config) -> str:
    key = str(config.get("key", "")).strip().lower()
    if not key:
        raise ActionError("no variable name configured")
    value = variables.render(str(config.get("value", "")), ctx)
    ctx.variables[key] = value
    return f"{{var.{key}}} = {value[:60]}"


# -------------------------------------------------------------------- points

async def award_points(cog, ctx, config) -> str:
    """Hands off to the economy cog rather than writing to its tables.

    `Economy.award` accepts an explicit amount alongside an unknown source
    (see cogs/economy/earning.py:41), which is what lets an automation grant
    an arbitrary number of Points without a catalogue entry. It returns the
    amount actually credited — 0 when the economy declined — so the run log
    reports what happened rather than what was asked for.
    """
    amount = int(config.get("amount") or 0)
    if not amount:
        raise ActionError("no amount configured")
    if ctx.simulate:
        return f"would award {amount} points"

    economy = ctx.bot.get_cog("Economy") if ctx.bot else None
    if economy is None:
        raise ActionError("the economy cog is not loaded")
    reason = variables.render(str(config.get("reason") or "Utility automation"), ctx)
    try:
        granted = await economy.award(
            ctx.user_id, "utility_automation", amount=amount,
            meta={"reason": reason[:200], "automation": config.get("_automation_id")},
        )
    except Exception as e:
        raise ActionError(f"the economy cog refused the award: {e}")
    if not granted:
        return f"the economy declined the {amount} point award"
    return f"awarded {granted} points"


# ------------------------------------------------------------------- control

async def wait(cog, ctx, config) -> str:
    """Handled by the engine, which persists the rest of the run.

    Reaching this function means the engine did not intercept the step, which
    would be a bug — so it fails loudly rather than silently sleeping.
    """
    raise ActionError("internal: wait should be handled by the engine")


async def stop(cog, ctx, config) -> str:
    return "stopped"


async def log_line(cog, ctx, config) -> str:
    channel = await _resolve_channel(ctx, _first_id(config, "channel"))
    if channel is None:
        raise ActionError("no log channel picked yet")
    name = getattr(channel, "name", "")
    if ctx.simulate:
        return f"would write to #{name}"

    if config.get("use_embed"):
        content, embed = embeds.build(config, ctx)
    else:
        # The default shape for a log line: a tidy box with the context
        # underneath, which is what makes a log skimmable.
        body = variables.render(
            str(config.get("content") or "{user.mention} set this off."), ctx)
        content, embed = None, discord.Embed(description=body[:4000],
                                             color=discord.Color.blurple())
        embed.set_footer(text=ctx.describe())
    try:
        sent = await channel.send(content=content, embed=embed)
    except discord.HTTPException as e:
        raise ActionError(f"couldn't post the log line: {e}")
    ctx.last_sent = sent
    return f"wrote to #{name}"


async def run_automation(cog, ctx, config) -> str:
    """Composition. The engine enforces the depth cap before we get here."""
    target_id = str(config.get("automation") or "").strip()
    if not target_id:
        raise ActionError("no automation selected")
    from .engine import run_by_id
    return await run_by_id(cog, target_id, ctx)
