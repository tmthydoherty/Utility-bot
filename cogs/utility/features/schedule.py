"""Schedule maths for reminders — pure functions, no Discord, no database.

Lifted from the reminder cog this package absorbed. Times are stored in UTC
(the dashboard converts the admin's local time before saving), so firing is a
straight ``current HH:MM == schedule time`` comparison and the loop only has to
run once a minute. `check_schedule` answers "fire now?"; the two `get_next_*`
helpers produce the countdown shown in a reminder's body and the panel.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


def _hm(time_utc_str) -> Optional[tuple]:
    if not time_utc_str or ":" not in str(time_utc_str):
        return None
    try:
        hour, minute = map(int, str(time_utc_str).split(":"))
        return hour, minute
    except (ValueError, AttributeError):
        return None


def check_schedule(schedule_data: dict, last_sent_ts) -> bool:
    """True when a scheduled reminder is due this minute."""
    if not schedule_data:
        return False
    now_utc = datetime.now(timezone.utc)
    current_time_utc = now_utc.strftime("%H:%M")

    # Fired already today? The loop ticks every minute; without this a reminder
    # would fire sixty times during the minute its time matches.
    if last_sent_ts:
        try:
            ts = int(last_sent_ts)
            last_sent = datetime.fromtimestamp(ts, tz=timezone.utc)
            if last_sent.date() == now_utc.date():
                return False
        except (ValueError, TypeError, OSError):
            pass

    frequency = schedule_data.get("frequency")
    time_utc = schedule_data.get("time_utc")
    if not frequency or not time_utc or current_time_utc != time_utc:
        return False

    if frequency == "daily":
        return True
    if frequency == "monthly":
        try:
            return now_utc.day == int(schedule_data.get("day_of_month", 0))
        except (ValueError, TypeError):
            return False
    if frequency in ("weekly", "biweekly"):
        try:
            days_of_week = [int(d) for d in schedule_data.get("days_of_week", [])]
        except (ValueError, TypeError):
            return False
        if now_utc.weekday() not in days_of_week:
            return False
        if frequency == "weekly":
            return True
        last_fire = schedule_data.get("last_biweekly_fire")
        if not last_fire:
            return True
        try:
            last_date = datetime.fromisoformat(last_fire).date()
        except (ValueError, TypeError):
            return True
        return (now_utc.date() - last_date).days >= 14
    if frequency == "every_x_days":
        try:
            interval = int(schedule_data.get("interval_days", 1))
        except (ValueError, TypeError):
            return False
        if not last_sent_ts:
            return True
        try:
            ts = int(last_sent_ts)
            last_sent = datetime.fromtimestamp(ts, tz=timezone.utc)
            return (now_utc.date() - last_sent.date()).days >= interval
        except (ValueError, TypeError, OSError):
            return True
    return False


def get_next_event_time(event_schedule: dict) -> Optional[int]:
    """Next occurrence of a recurring *event* (the countdown in the body).

    Distinct from the reminder's own schedule: an event countdown says "next
    match is in 3 days" regardless of when the reminder itself posts.
    """
    if not event_schedule:
        return None
    hm = _hm(event_schedule.get("time_utc"))
    if hm is None:
        return None
    hour, minute = hm
    now_utc = datetime.now(timezone.utc)
    frequency = event_schedule.get("frequency")

    if frequency == "monthly":
        try:
            dom = int(event_schedule.get("day_of_month", 1))
        except (ValueError, TypeError):
            return None
        try:
            next_event = now_utc.replace(day=dom, hour=hour, minute=minute, second=0, microsecond=0)
            if next_event <= now_utc:
                raise ValueError
        except ValueError:
            year = now_utc.year if now_utc.month < 12 else now_utc.year + 1
            month = now_utc.month + 1 if now_utc.month < 12 else 1
            next_event = None
            for _ in range(12):
                try:
                    next_event = datetime(year, month, dom, hour, minute, tzinfo=timezone.utc)
                    break
                except ValueError:
                    month += 1
                    if month > 12:
                        month = 1
                        year += 1
            if next_event is None:
                return None
    elif frequency in ("weekly", "biweekly"):
        try:
            days_of_week = [int(d) for d in event_schedule.get("days_of_week", [])]
        except (ValueError, TypeError):
            return None
        if not days_of_week:
            return None
        candidates = []
        for target_dow in days_of_week:
            days_ahead = (target_dow - now_utc.weekday()) % 7
            if days_ahead == 0:
                candidate = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now_utc:
                    days_ahead = 7
                else:
                    candidates.append(candidate)
                    continue
            candidate = (now_utc + timedelta(days=days_ahead)).replace(
                hour=hour, minute=minute, second=0, microsecond=0)
            candidates.append(candidate)
        if not candidates:
            return None
        next_event = min(candidates)
        if frequency == "biweekly":
            last_fire = event_schedule.get("last_biweekly_fire")
            if last_fire:
                try:
                    last_date = datetime.fromisoformat(last_fire).date()
                    if (next_event.date() - last_date).days < 14:
                        next_event += timedelta(weeks=1)
                except (ValueError, TypeError):
                    pass
    else:
        return None
    return int(next_event.timestamp())


def get_next_fire_time(schedule_data: dict, last_sent_ts=None) -> Optional[int]:
    """Unix timestamp of a scheduled reminder's next post, or None if unknown."""
    if not schedule_data:
        return None
    hm = _hm(schedule_data.get("time_utc"))
    if hm is None:
        return None
    hour, minute = hm
    now_utc = datetime.now(timezone.utc)
    frequency = schedule_data.get("frequency")

    if frequency == "daily":
        next_fire = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_fire <= now_utc:
            next_fire += timedelta(days=1)
    elif frequency == "monthly":
        try:
            dom = int(schedule_data.get("day_of_month", 1))
        except (ValueError, TypeError):
            return None
        try:
            next_fire = now_utc.replace(day=dom, hour=hour, minute=minute, second=0, microsecond=0)
            if next_fire <= now_utc:
                raise ValueError
        except ValueError:
            year = now_utc.year if now_utc.month < 12 else now_utc.year + 1
            month = now_utc.month + 1 if now_utc.month < 12 else 1
            next_fire = None
            for _ in range(12):
                try:
                    next_fire = datetime(year, month, dom, hour, minute, tzinfo=timezone.utc)
                    break
                except ValueError:
                    month += 1
                    if month > 12:
                        month = 1
                        year += 1
            if next_fire is None:
                return None
    elif frequency in ("weekly", "biweekly"):
        try:
            days_of_week = [int(d) for d in schedule_data.get("days_of_week", [])]
        except (ValueError, TypeError):
            return None
        if not days_of_week:
            return None
        candidates = []
        for target_dow in days_of_week:
            days_ahead = (target_dow - now_utc.weekday()) % 7
            if days_ahead == 0:
                candidate = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if candidate <= now_utc:
                    days_ahead = 7
                else:
                    candidates.append(candidate)
                    continue
            candidate = (now_utc + timedelta(days=days_ahead)).replace(
                hour=hour, minute=minute, second=0, microsecond=0)
            candidates.append(candidate)
        if not candidates:
            return None
        next_fire = min(candidates)
        if frequency == "biweekly":
            last_fire = schedule_data.get("last_biweekly_fire")
            if last_fire:
                try:
                    last_date = datetime.fromisoformat(last_fire).date()
                    if (next_fire.date() - last_date).days < 14:
                        next_fire += timedelta(weeks=1)
                except (ValueError, TypeError):
                    pass
    elif frequency == "every_x_days":
        try:
            interval = int(schedule_data.get("interval_days", 1))
        except (ValueError, TypeError):
            return None
        base = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if last_sent_ts:
            try:
                last_sent = datetime.fromtimestamp(int(last_sent_ts), tz=timezone.utc)
                candidate = last_sent.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=interval)
                next_fire = candidate if candidate > now_utc else base
            except (ValueError, TypeError, OSError):
                next_fire = base
        else:
            next_fire = base
        if next_fire <= now_utc:
            next_fire += timedelta(days=1)
    else:
        return None
    return int(next_fire.timestamp())
