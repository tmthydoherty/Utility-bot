"""What a setting *is*, said once and reused everywhere.

A `Field` describes a setting — its type, its help text, when it applies —
without saying anything about how it is edited. `utils/panel_fields.py` turns
these into Discord components; the Utility cog describes its media, reaction
and sticky rules with them; `cogs/automations/registry.py` uses the same type
for a trigger's, condition's or action's configuration, which is what lets a
new action ship without a line of UI code. The web dashboard mirrors this file
in TypeScript for the same reason.

It lives in `utils/` rather than inside a cog because two cogs now depend on
it, and the automations engine must be importable without pulling in Discord
views — which is what makes it testable without a bot.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field as dc_field
from enum import Enum
from typing import Any, Callable, List, Optional


class FieldType(str, Enum):
    BOOL = "bool"
    TEXT = "text"
    MULTILINE = "multiline"
    NUMBER = "number"
    DURATION = "duration"
    CHANNEL = "channel"
    ROLE = "role"
    USER = "user"
    EMOJI = "emoji"
    CHOICE = "choice"


@dataclass
class Field:
    key: str
    label: str
    type: FieldType
    help: str = ""
    choices: List[tuple] = dc_field(default_factory=list)  # (value, label[, description])
    multi: bool = False          # CHANNEL / ROLE / USER: a list rather than one
    minimum: Optional[int] = None
    maximum: Optional[int] = None
    placeholder: str = ""
    default: Any = None
    required: bool = False
    # Hide a field that only makes sense given another field's value.
    visible_when: Optional[Callable[[dict], bool]] = None
    # What the event supplies if this is left blank, e.g. "channel" for a
    # destination that falls back to wherever the trigger happened. Blank is
    # then only a problem on a trigger that has no such thing — which is what
    # lets the setup checklist stay silent about fields that are fine empty.
    needs_context: str = ""


def is_unset(field: "Field", config: dict) -> bool:
    """Whether a field has no usable value.

    Zero counts as empty for the ID types because that is what an unfilled
    channel or role placeholder looks like, but it is a legitimate value for a
    number — `set_slowmode` to 0 seconds turns slowmode off.
    """
    value = config.get(field.key, None)
    if value is None or value == "" or value == []:
        return True
    if field.type in (FieldType.CHANNEL, FieldType.ROLE, FieldType.USER):
        # A snowflake of 0 is what an unfilled template placeholder looks like,
        # and it arrives as either the number or the string.
        kept = []
        for item in as_list(value):
            try:
                if int(item):
                    kept.append(item)
            except (TypeError, ValueError):
                pass
        return not kept
    if field.multi:
        return not as_list(value)
    if isinstance(value, str) and value.strip() in ("", "[]"):
        return True
    return False


# ----------------------------------------------------------------- durations

def parse_duration(text: str) -> Optional[int]:
    """`10m`, `2h30m`, `1d`, or a bare number of seconds. None if unparseable."""
    text = (text or "").strip().lower().replace(" ", "")
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    total, number = 0, ""
    for char in text:
        if char.isdigit():
            number += char
        elif char in units and number:
            total += int(number) * units[char]
            number = ""
        else:
            return None
    if number:  # trailing digits with no unit
        return None
    return total


def format_duration(seconds: int) -> str:
    seconds = int(seconds or 0)
    if seconds <= 0:
        return "off"
    parts = []
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        if seconds >= size:
            parts.append(f"{seconds // size}{unit}")
            seconds %= size
    return "".join(parts)


# -------------------------------------------------------------------- values

def as_list(value) -> list:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            return [value]
    return [value]


def display_value(field: Field, value: Any) -> str:
    """One line describing a field's current value, for a summary embed."""
    if field.type is FieldType.BOOL:
        return "✅ On" if value else "⬜ Off"
    if field.type is FieldType.DURATION:
        return f"`{format_duration(value)}`"
    if field.type is FieldType.CHOICE:
        labels = {str(v): label for v, label, *_ in field.choices}
        if field.multi:
            chosen = [str(v) for v in as_list(value)]
            if not chosen:
                return "*none*"
            return "`" + "`, `".join(labels.get(c, c) for c in chosen) + "`"
        label = labels.get(str(value))
        if label is not None:
            return f"`{label}`"
        return f"`{value}`" if value not in (None, "") else "*not set*"
    if field.type in (FieldType.CHANNEL, FieldType.ROLE, FieldType.USER):
        items = as_list(value) if field.multi else ([value] if value else [])
        if not items:
            return "*none*"
        token = {FieldType.CHANNEL: "#", FieldType.ROLE: "@&", FieldType.USER: "@"}[field.type]
        rendered = [f"<{token}{i}>" for i in items[:8]]
        if len(items) > 8:
            rendered.append(f"+{len(items) - 8} more")
        return " ".join(rendered)
    if field.type is FieldType.EMOJI:
        items = as_list(value)
        return " ".join(str(i) for i in items[:15]) if items else "*none*"
    if field.type is FieldType.NUMBER:
        return f"`{value}`" if value not in (None, "") else "`0`"
    text = str(value or "")
    if not text:
        return "*not set*"
    return f"`{text[:80]}`" if len(text) <= 80 else f"`{text[:77]}…`"


def describe_config(fields: List[Field], config: dict, limit: int = 3) -> str:
    """The *values* of a node's settings, without repeating their labels.

    For a diagram, "As a reply · Hey {user.name}!" reads; the label-prefixed
    form ("where should it go `As a reply`, what should it say `Hey…`") is
    twice as long and says nothing extra, because the action's own name
    already established what the settings are.
    """
    parts = []
    for field in fields:
        if field.visible_when is not None and not field.visible_when(config):
            continue
        value = config.get(field.key, field.default)
        if field.type is FieldType.BOOL:
            if value:
                parts.append(field.label.lower())
            continue
        if value in (None, "", 0, [], "[]"):
            continue
        rendered = display_value(field, value).strip("`")
        if field.type in (FieldType.MULTILINE, FieldType.TEXT) and len(rendered) > 45:
            rendered = rendered[:44] + "…"
        parts.append(rendered)
        if len(parts) >= limit:
            break
    return " · ".join(parts)


def summarise_config(fields: List[Field], config: dict, limit: int = 3) -> str:
    """A short one-line description of a node's configuration.

    Used in the automation builder's step list, where there is no room for the
    full field table but "add role @Muted for 10m" has to be readable.
    """
    parts = []
    for field in fields:
        if field.visible_when is not None and not field.visible_when(config):
            continue
        value = config.get(field.key, field.default)
        if field.type is FieldType.BOOL:
            # A switched-off toggle says nothing useful in a one-line summary,
            # and crowds out the settings that were actually changed.
            if value:
                parts.append(field.label.lower())
            continue
        if value in (None, "", 0, [], "[]"):
            continue
        parts.append(f"{field.label.lower()} {display_value(field, value)}")
        if len(parts) >= limit:
            break
    return ", ".join(parts)
