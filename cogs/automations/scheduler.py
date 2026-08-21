"""Durable timers: deferred steps, temporary roles and scheduled triggers.

Everything here follows the pattern the economy cog settled on (see
`cogs/economy/effects.py` and `cogs/economy/items/hof.py`): write an absolute
expiry to the database, sweep it from a `tasks.loop`, and run the same sweep
once on startup so anything that lapsed while the bot was down is caught up
rather than stranded.

No `asyncio.sleep` holds any of this. A sleep is lost on restart, and the
whole promise of "wait 10 minutes, then give them the role" is that it still
happens if the Pi reboots at minute three.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import discord

from utils.events import EventContext
from .storage import now
from .models import Automation, parse_steps

logger = logging.getLogger('cogs.automations.scheduler')


async def sweep(cog):
    """One pass over everything that might be due. Never raises."""
    for name, handler in (("pending actions", _run_pending),
                          ("temporary roles", _expire_temp_roles),
                          ("scheduled triggers", _run_scheduled),
                          ("cooldowns", _prune_cooldowns)):
        try:
            await handler(cog)
        except Exception as e:
            logger.error(f"Sweeping {name} failed: {e}", exc_info=True)


# ------------------------------------------------------------ deferred steps

async def _run_pending(cog):
    rows = await cog.db.fetchall(
        "SELECT * FROM pending_actions WHERE execute_ts <= ? ORDER BY execute_ts",
        (now(),),
    )
    for row in rows:
        # Delete first: a step that crashes must not be retried forever on
        # every sweep. One attempt is the contract.
        await cog.db.execute("DELETE FROM pending_actions WHERE id = ?", (row["id"],))
        try:
            await _resume(cog, row)
        except Exception as e:
            logger.error(f"Resuming automation {row['automation_id']} failed: {e}",
                         exc_info=True)


async def _resume(cog, row):
    from .engine import Trace, run_steps, MAX_ACTIONS_PER_RUN, Stop, Deferred

    automation_row = await cog.db.get_row("automations", row["automation_id"])
    if automation_row is None:
        return
    automation = Automation.from_row(automation_row)
    if not automation.enabled:
        logger.info(f"Dropping deferred steps for disabled {automation.name}.")
        return

    try:
        payload = json.loads(row["context"])
    except (json.JSONDecodeError, TypeError):
        return

    ctx = await _rebuild_context(cog, payload, row["depth"])
    if ctx is None:
        return
    ctx.variables["_automation_id"] = automation.id
    ctx.simulate = automation.dry_run

    steps = parse_steps(json.loads(row["steps"]))
    trace = Trace()
    trace.add("info", "Resumed", None, "continuing after a wait")
    outcome = "fired"
    try:
        await run_steps(cog, ctx, steps, trace, [MAX_ACTIONS_PER_RUN])
    except Stop:
        trace.add("info", "Stop", None, "the automation stopped here")
    except Deferred as deferred:
        # A second wait in the same run just queues again.
        from .engine import _defer
        await _defer(cog, automation, ctx, deferred)
        outcome = "deferred"
    except Exception as e:
        trace.add("error", "Automation", False, str(e))
        outcome = "error"

    if outcome == "fired" and ctx.simulate:
        outcome = "dry_run"
    await cog.db.record_run(automation.id, outcome,
                            f"resumed — {trace.summary()}", 0, trace.to_list())


async def _rebuild_context(cog, payload: dict, depth: int):
    """Re-resolve Discord objects from the ids stored at defer time.

    The member or channel may be gone by now — someone left, a channel was
    deleted. That is not an error worth logging loudly; the run simply cannot
    continue.
    """
    guild = cog.bot.get_guild(int(payload.get("guild_id") or 0))
    if guild is None:
        return None
    member = guild.get_member(int(payload.get("user_id") or 0))
    channel = guild.get_channel_or_thread(int(payload.get("channel_id") or 0))
    ctx = EventContext(
        event=payload.get("event", "resumed"), bot=cog.bot, guild=guild,
        member=member, user=member, channel=channel, depth=depth,
        variables=payload.get("variables") or {},
    )
    message_id = int(payload.get("message_id") or 0)
    if message_id and channel is not None:
        try:
            ctx.message = await channel.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    return ctx


# ---------------------------------------------------------- temporary roles

async def _expire_temp_roles(cog):
    for row in await cog.db.temp_roles_due():
        # Cleared first for the same reason as pending actions: a role that
        # cannot be removed (gone, or now above the bot) must not be retried
        # on every sweep forever.
        await cog.db.temp_role_clear(row["id"])
        guild = cog.bot.get_guild(row["guild_id"])
        if guild is None:
            continue
        member = guild.get_member(row["user_id"])
        role = guild.get_role(row["role_id"])
        if member is None or role is None:
            continue
        if role not in member.roles:
            continue
        try:
            await member.remove_roles(role, reason="Temporary role expired")
            logger.info(f"Removed temporary @{role.name} from {member}.")
        except discord.HTTPException as e:
            logger.warning(f"Could not remove temporary @{role.name} from {member}: {e}")


# ------------------------------------------------------- scheduled triggers

async def _run_scheduled(cog):
    """Fire `schedule` automations whose next due time has passed.

    Idempotency comes from `last_fired_ts`, the same approach the Utility cog's
    reminders use (cogs/utility/features/schedule.py) — there is no queue to get
    out of sync with.
    """
    automations = cog.automations_for("schedule")
    if not automations:
        return
    moment = datetime.now(timezone.utc)
    for automation in automations:
        row = await cog.db.get_row("automations", automation.id)
        if row is None:
            continue
        if not _schedule_due(automation.trigger_config, row["last_fired_ts"], moment):
            continue
        guild = cog.bot.get_guild(automation.guild_id)
        if guild is None:
            continue
        from .engine import run_automation
        ctx = EventContext(event="schedule", bot=cog.bot, guild=guild)
        await run_automation(cog, automation, ctx)


def _schedule_due(config: dict, last_fired_ts: int, moment: datetime) -> bool:
    frequency = (config or {}).get("frequency", "daily")

    if frequency == "interval":
        hours = max(1, int(config.get("interval_hours") or 24))
        return (moment.timestamp() - (last_fired_ts or 0)) >= hours * 3600

    target = str(config.get("time_utc") or "").strip()
    if ":" not in target:
        return False
    try:
        hour, minute = (int(p) for p in target.split(":", 1))
    except ValueError:
        return False
    # Minute granularity, matched against the sweep's clock rather than a
    # window, so a slow sweep skips rather than double-firing.
    if moment.hour != hour or moment.minute != minute:
        return False
    if frequency == "weekly" and moment.weekday() != int(config.get("weekday") or 0):
        return False
    # Guards against two sweeps inside the same minute.
    return (moment.timestamp() - (last_fired_ts or 0)) > 90


async def _prune_cooldowns(cog):
    await cog.db.execute("DELETE FROM cooldowns WHERE expires_ts <= ?", (now(),))
