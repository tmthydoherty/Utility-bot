"""The reminder engine — deciding what to post and posting it.

Absorbed from the standalone reminder cog. Three kinds of entry share this
path, chosen by the row's ``kind``:

* **scheduled** — a recurring post on a weekly/monthly/daily/etc. schedule
  (`schedule.check_schedule`). Fires at most once per calendar day.
* **interval** — a repeating post every ``interval_s`` seconds.
* **oneoff** — a message sent once from the dashboard, then editable in place:
  a later edit on the website flips ``pending_edit`` and the next tick edits the
  live message rather than posting a new one.

The loop ticks once a minute. Times are stored in UTC (the dashboard converts
the admin's local time before saving), so "fire now?" is a minute-granular
string compare and the tick cost is one indexed query.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import aiohttp
import discord

from ..storage import loads
from . import render
from . import schedule as schedule_math

logger = logging.getLogger('cogs.utility.reminders')


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


async def _resolve_channel(cog, cid: int):
    ch = cog.bot.get_channel(cid)
    if ch is not None:
        return ch
    try:
        return await cog.bot.fetch_channel(cid)
    except (discord.NotFound, discord.Forbidden, aiohttp.ClientError, discord.HTTPException):
        return None


async def _send(cog, data: dict) -> dict:
    """Post the reminder to each of its channels. Returns the map of channel id
    (str) -> new message id for the copies that landed, empty if none did."""
    channel_ids = loads(data.get("channel_ids_json"), []) or []
    if not channel_ids:
        return {}

    rendered = render.render(data)
    content, embed, view = rendered["content"], rendered["embed"], rendered["view"]
    delete_previous = bool(data.get("delete_previous"))
    old_ids = loads(data.get("last_message_ids_json"), {}) or {}

    new_ids: dict = {}
    for cid in channel_ids:
        ch = await _resolve_channel(cog, int(cid))
        if ch is None:
            logger.warning(f"Reminder {data.get('id')}: channel {cid} not found, skipping.")
            continue
        try:
            perms = ch.permissions_for(ch.guild.me)
        except AttributeError:
            continue
        if not perms.send_messages:
            logger.warning(f"Reminder {data.get('id')}: no send perms in {cid}.")
            continue
        if embed is not None and not perms.embed_links:
            logger.warning(f"Reminder {data.get('id')}: no embed perms in {cid}.")
            continue

        if delete_previous:
            old_mid = old_ids.get(str(cid))
            if old_mid:
                try:
                    await ch.get_partial_message(int(old_mid)).delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

        try:
            msg = await ch.send(content=content or None, embed=embed, view=view,
                                allowed_mentions=render.ALLOWED_MENTIONS)
            new_ids[str(cid)] = msg.id
        except discord.Forbidden:
            logger.error(f"Reminder {data.get('id')}: forbidden to send in {cid}.")
        except discord.HTTPException as e:
            logger.error(f"Reminder {data.get('id')}: send failed in {cid}: {e}")
    return new_ids


async def _edit_live(cog, data: dict) -> None:
    """Apply a dashboard edit to the messages a one-off already posted."""
    old_ids = loads(data.get("last_message_ids_json"), {}) or {}
    if not old_ids:
        return
    rendered = render.render(data)
    for cid, mid in old_ids.items():
        ch = await _resolve_channel(cog, int(cid))
        if ch is None:
            continue
        try:
            msg = await ch.fetch_message(int(mid))
            await msg.edit(content=rendered["content"] or None, embed=rendered["embed"],
                           view=rendered["view"], allowed_mentions=render.ALLOWED_MENTIONS)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
            logger.warning(f"Reminder {data.get('id')}: could not edit message in {cid}: {e}")


async def _handle_scheduled(cog, data: dict) -> None:
    schedule_data = loads(data.get("schedule_json"), {}) or {}
    if not schedule_data or not data.get("guild_id"):
        return
    if not schedule_math.check_schedule(schedule_data, data.get("last_sent_ts")):
        return

    # A specific date the admin chose to skip.
    skipped = loads(data.get("skipped_dates_json"), []) or []
    today = _today_utc()
    if today in skipped:
        skipped = [d for d in skipped if d != today]
        await cog.db.update_row("reminders", data["id"],
                                skipped_dates_json=json.dumps(skipped))
        logger.info(f"Reminder {data.get('name')}: skipped {today} (specific date).")
        return

    # The next N occurrences, skipped in a run.
    skip_next = int(data.get("skip_next") or 0)
    if skip_next > 0:
        await cog.db.update_row("reminders", data["id"], skip_next=skip_next - 1)
        logger.info(f"Reminder {data.get('name')}: skipped one (skip_next now {skip_next - 1}).")
        return

    new_ids = await _send(cog, data)
    if new_ids:
        now_utc = datetime.now(timezone.utc)
        fields = {
            "last_sent_ts": int(now_utc.timestamp()),
            "last_message_ids_json": json.dumps(new_ids),
        }
        if schedule_data.get("frequency") == "biweekly":
            schedule_data["last_biweekly_fire"] = now_utc.date().isoformat()
            fields["schedule_json"] = json.dumps(schedule_data)
        await cog.db.update_row("reminders", data["id"], **fields)


async def _handle_interval(cog, data: dict) -> None:
    interval_s = int(data.get("interval_s") or 0)
    if interval_s <= 0:
        return
    now_ts = int(datetime.now(timezone.utc).timestamp())
    last = int(data.get("last_sent_ts") or 0)
    if last and now_ts - last < interval_s:
        return
    new_ids = await _send(cog, data)
    if new_ids:
        await cog.db.update_row("reminders", data["id"],
                                last_sent_ts=now_ts,
                                last_message_ids_json=json.dumps(new_ids))


async def _handle_oneoff(cog, data: dict) -> None:
    if data.get("pending_edit"):
        await _edit_live(cog, data)
        await cog.db.update_row("reminders", data["id"], pending_edit=0)
        return
    if int(data.get("last_sent_ts") or 0) > 0:
        return  # already sent; it lives on only to be edited
    new_ids = await _send(cog, data)
    if new_ids:
        await cog.db.update_row("reminders", data["id"],
                                last_sent_ts=int(datetime.now(timezone.utc).timestamp()),
                                last_message_ids_json=json.dumps(new_ids))


async def process_tick(cog) -> None:
    """One pass over every enabled reminder. Called once a minute."""
    try:
        rows = await cog.db.fetchall("SELECT * FROM reminders WHERE enabled = 1")
    except Exception as e:
        logger.error(f"Reminder tick query failed: {e}", exc_info=True)
        return
    for row in rows:
        data = dict(row)
        kind = data.get("kind", "scheduled")
        try:
            if kind == "oneoff":
                await _handle_oneoff(cog, data)
            elif kind == "interval":
                await _handle_interval(cog, data)
            else:
                await _handle_scheduled(cog, data)
        except Exception as e:
            logger.error(f"Reminder {data.get('id')} tick failed: {e}", exc_info=True)


async def send_now(cog, reminder_id: str) -> bool:
    """Force a reminder to post immediately (dashboard 'send test'/'send now')."""
    row = await cog.db.get_row("reminders", reminder_id)
    if row is None:
        return False
    new_ids = await _send(cog, dict(row))
    if new_ids:
        await cog.db.update_row("reminders", reminder_id,
                                last_sent_ts=int(datetime.now(timezone.utc).timestamp()),
                                last_message_ids_json=json.dumps(new_ids))
    return bool(new_ids)
