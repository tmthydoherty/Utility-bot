"""Drawing an automation as a picture.

The builder used to be three screens you clicked between, which meant holding
the shape of the automation in your head — what sets it off, what it checks,
what it does — and rebuilding that picture every time you moved. People got
lost, and no amount of renaming the buttons fixed it, because the problem was
that you could never see the whole thing at once.

So every screen now shows the same diagram, with the part you are editing
highlighted:

    ⚡ WHEN
      Someone sends a message

    🔍 ONLY IF
      • The message contains "hello"
      • It's in #general

    ▶️ THEN
      1. Reply to their message
      2. React with 🎉

You always know what the automation currently does, and where you are in it.
Nothing here changes behaviour — it just makes the behaviour legible.
"""

from __future__ import annotations

from typing import List, Optional

from utils.fieldspec import as_list, describe_config, display_value

from ..models import Automation, ConditionGroup, ConditionLeaf, Step
from ..registry import ACTIONS, CONDITIONS, TRIGGERS


def _quote(text: str, limit: int = 40) -> str:
    text = str(text or "").replace("\n", " ").strip()
    if not text:
        return ""
    return f'"{text[:limit]}…"' if len(text) > limit else f'"{text}"'


def _channel(config, key="channel") -> str:
    values = as_list(config.get(key))
    ident = values[0] if values else config.get(key)
    return f"<#{ident}>" if ident else "a channel"


def _role(config, key="role") -> str:
    values = as_list(config.get(key))
    ident = values[0] if values else config.get(key)
    return f"<@&{ident}>" if ident else "a role"


# Hand-written one-liners for the entries whose generic summary reads badly.
# Everything else falls back to `describe_config`, which is fine for simple
# settings — this is only for the ones where word order carries meaning.
def _send_message(c) -> str:
    body = _quote(c.get("content") or c.get("embed_title") or "")
    kind = "an embed" if c.get("use_embed") else "a message"
    where = c.get("destination", "channel")
    if where == "dm":
        return f"DM them {kind}: {body}" if body else f"DM them {kind}"
    if where == "reply":
        return f"Reply with {kind}: {body}" if body else f"Reply with {kind}"
    return f"Post {kind} in {_channel(c)}: {body}" if body else \
           f"Post {kind} in {_channel(c)}"


def _add_reaction(c) -> str:
    emoji = " ".join(str(e) for e in as_list(c.get("emoji"))) or "an emoji"
    where = ("the message I sent" if c.get("target") == "sent"
             else "their message")
    return f"React to {where} with {emoji}"


def _add_role(c) -> str:
    duration = c.get("duration")
    if duration:
        from utils.fieldspec import format_duration
        return f"Give them {_role(c)} for {format_duration(duration)}"
    return f"Give them {_role(c)}"


def _counter_change(c) -> str:
    amount = int(c.get("amount") or 1)
    name = c.get("key") or "a tally"
    verb = "Add" if amount >= 0 else "Take"
    return f"{verb} {abs(amount)} {'to' if amount >= 0 else 'from'} the \"{name}\" tally"


def _counter_compare(c) -> str:
    words = {"gte": "is at least", "lte": "is at most", "eq": "is exactly",
             "gt": "is more than", "lt": "is less than"}
    return (f'the "{c.get("key") or "?"}" tally '
            f'{words.get(c.get("op"), "is at least")} {c.get("value", 1)}')


ACTION_SUMMARIES = {
    "send_message": _send_message,
    "add_reaction": _add_reaction,
    "add_role": _add_role,
    "remove_role": lambda c: f"Take {_role(c)} away",
    "toggle_role": lambda c: f"Give or take {_role(c)}",
    "counter_change": _counter_change,
    "counter_reset": lambda c: f'Reset the "{c.get("key") or "?"}" tally',
    "timeout_member": lambda c: (
        f"Time them out for {display_value(ACTIONS['timeout_member'].fields[0], c.get('duration'))}"
        .replace("`", "")),
    "log_line": lambda c: f"Write to {_channel(c)}",
    "set_slowmode": lambda c: f"Set {_channel(c)} slowmode to {c.get('seconds', 0)}s",
    "award_points": lambda c: f"Give them {c.get('amount', 0)} Points",
    "set_nickname": lambda c: (f"Rename them to {_quote(c.get('nickname'))}"
                               if c.get("nickname") else "Reset their nickname"),
    "wait": lambda c: (
        f"Wait {display_value(ACTIONS['wait'].fields[0], c.get('duration'))}"
        .replace("`", "")),
    "create_thread": lambda c: (f"Start a thread called {_quote(c.get('name'))}"
                                if c.get("name") else "Start a thread"),
}

def _channel_is(c) -> str:
    values = as_list(c.get("channels"))
    if not values:
        return "it's in certain channels"
    shown = ", ".join(f"<#{v}>" for v in values[:3])
    if len(values) > 3:
        shown += f" +{len(values) - 3} more"
    tail = " (threads & posts too)" if c.get("include_threads") else ""
    return f"it's in {shown}{tail}"


CONDITION_SUMMARIES = {
    "channel_is": _channel_is,
    "content_contains": lambda c: f"the message contains {_quote(c.get('text'))}",
    "content_starts_with": lambda c: f"the message starts with {_quote(c.get('text'))}",
    "content_ends_with": lambda c: f"the message ends with {_quote(c.get('text'))}",
    "content_exactly": lambda c: f"the message is exactly {_quote(c.get('text'))}",
    "content_regex": lambda c: f"the message matches {_quote(c.get('pattern'))}",
    "counter_compare": _counter_compare,
    "chance": lambda c: f"a {c.get('percent', 50)}% chance says so",
    "level_at_least": lambda c: f"they're level {c.get('level', 0)} or above",
    "caps_ratio": lambda c: f"the message is {c.get('percent', 70)}%+ capitals",
}


def _summarise(spec, config: dict, custom: dict) -> str:
    """The registry's own wording where it exists, else the generic one."""
    maker = custom.get(getattr(spec, "key", None))
    if maker is not None:
        try:
            return maker(config or {})
        except Exception:
            pass  # a half-configured node must never break the diagram
    return describe_config(spec.fields, config, limit=2)

# Discord caps an embed description at 4096; leave room for headers and flash
# messages that get prepended.
MAX_LENGTH = 2600

WHEN, ONLY_IF, THEN = "when", "only_if", "then"


def _heading(icon: str, text: str, focused: bool) -> str:
    """Bold always, underlined when it is the section being edited."""
    label = f"__{text}__" if focused else text
    arrow = "  ⬅︎ *you're here*" if focused else ""
    return f"{icon} **{label}**{arrow}"


def _check_line(node, depth: int = 0) -> List[str]:
    indent = "　" * (depth + 1)
    if isinstance(node, ConditionGroup):
        if not node.items:
            return []
        word = "any one of these" if node.op == "or" else "all of these"
        if node.negate:
            word = f"NOT {word}"
        lines = [f"{indent}┌ {word}:"]
        for child in node.items:
            lines.extend(_check_line(child, depth + 1))
        return lines

    spec = CONDITIONS.get(node.type)
    if spec is None:
        return [f"{indent}• {node.type or 'Unrecognised requirement'}"]
    custom = CONDITION_SUMMARIES.get(node.type)
    # A hand-written summary already reads as a sentence, so it replaces the
    # label rather than being tacked onto it.
    text = (custom(node.config or {}) if custom
            else (f"{spec.label} ({describe_config(spec.fields, node.config, 2)})"
                  if describe_config(spec.fields, node.config, 2) else spec.label))
    if not text:
        return [f"{indent}•"]
    text = f"{text[0].upper()}{text[1:]}"
    # "Not: the message is a reply" reads; "the message is a reply — must NOT
    # be true" makes you parse a double negative to work out what it means.
    if node.negate:
        text = f"**Not:** {text[0].lower()}{text[1:]}"
    return [f"{indent}• {text}"]


def _action_line(step: Step, number: str, depth: int = 0) -> List[str]:
    indent = "　" * (depth + 1)
    if step.is_branch:
        items = step.conditions.items if step.conditions else []
        # Naming the requirement beats counting it: "1 requirement" tells you
        # nothing about what the split actually decides.
        if len(items) == 1:
            asked = "".join(_check_line(items[0])).strip().lstrip("• ")
            question = f"If {asked[0].lower()}{asked[1:]}" if asked else "If"
        elif items:
            joiner = "any one of" if step.conditions.op == "or" else "all of"
            question = f"If {joiner} {len(items)} requirements pass"
        else:
            question = "If *(no requirements set yet)*"
        lines = [f"{indent}**{number}.** ⑂ {question}:"]
        lines.append(f"{indent}　**If yes →**")
        if step.then:
            for i, child in enumerate(step.then, 1):
                lines.extend(_action_line(child, str(i), depth + 2))
        else:
            lines.append(f"{indent}　　*nothing yet*")
        lines.append(f"{indent}　**If no →**")
        if step.otherwise:
            for i, child in enumerate(step.otherwise, 1):
                lines.extend(_action_line(child, str(i), depth + 2))
        else:
            lines.append(f"{indent}　　*nothing yet*")
        return lines

    spec = ACTIONS.get(step.type)
    if spec is None:
        return [f"{indent}**{number}.** {step.type} *(no longer available)*"]
    custom = ACTION_SUMMARIES.get(step.type)
    if custom:
        text = _summarise(spec, step.config, ACTION_SUMMARIES)
    else:
        detail = describe_config(spec.fields, step.config, limit=2)
        text = f"{spec.label} ({detail})" if detail else spec.label
    return [f"{indent}**{number}.** {text}"]


def render(model: Automation, *, focus: Optional[str] = None) -> str:
    """The whole automation as one readable block."""
    lines: List[str] = []

    # --- when
    trigger = TRIGGERS.get(model.trigger_type)
    lines.append(_heading("⚡", "WHEN", focus == WHEN))
    if trigger is None:
        lines.append("　*Not chosen yet*")
    else:
        lines.append(f"　{trigger.label}")
    lines.append("")

    # --- only if
    lines.append(_heading("🔍", "ONLY IF", focus == ONLY_IF))
    checks = model.conditions.items if model.conditions else []
    if not checks:
        lines.append("　*No requirements — this runs every time*")
    else:
        # The join word goes *above* the list, because "any one is enough" read
        # after the bullets is the whole reason people mistake an OR for an AND.
        # OR is the surprising one, so it is flagged rather than stated flat.
        if len(checks) > 1:
            if model.conditions.op == "or":
                lines.append("　⚠️ ***any one*** *of these is enough on its own:*")
            else:
                lines.append("　*all of these must be true:*")
        for node in checks:
            lines.extend(_check_line(node))
    lines.append("")

    # --- then
    lines.append(_heading("▶️", "THEN", focus == THEN))
    if not model.steps:
        lines.append("　⚠️ *Nothing yet — this automation won't do anything*")
    else:
        for i, step in enumerate(model.steps, 1):
            lines.extend(_action_line(step, str(i)))

    text = "\n".join(lines)
    if len(text) > MAX_LENGTH:
        text = text[:MAX_LENGTH].rsplit("\n", 1)[0] + "\n　*…too long to show it all*"
    return text


# ------------------------------------------------------- teaching diagrams

GROUPS_EXAMPLE = (
    "**Normally**, every requirement has to be true:\n"
    "```\n"
    "• has a link\n"
    "• joined less than a day ago\n"
    "→ both must be true\n"
    "```\n"
    "**With a group**, you can mix the two kinds:\n"
    "```\n"
    "• has a link\n"
    "┌ any one of these:\n"
    "  • joined less than a day ago\n"
    "  • has no roles\n"
    "```\n"
    "That means: *has a link* **and** *(is new **or** has no roles)*."
)

SPLIT_EXAMPLE = (
    "**Normally**, actions just run one after another:\n"
    "```\n"
    "1. Delete their message\n"
    "2. Send them a warning\n"
    "```\n"
    "**With a split**, it does one thing or another:\n"
    "```\n"
    "1. Delete their message\n"
    "2. ⑂ Is this their 3rd strike?\n"
    "     If yes → time them out\n"
    "     If no  → just warn them\n"
    "```"
)
