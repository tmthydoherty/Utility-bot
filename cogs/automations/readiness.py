"""What is still blank in an automation, and where to go and fill it.

Templates land deliberately unfinished — a welcome message with no channel
picked, a "give them a role" step with no role. That is the right trade: a
concrete automation you adjust beats an empty one you have to design. But it
only works if something knows which parts are still blank, because otherwise
the gap is carried entirely by prose the reader has to translate into
navigation, and `Turn on` happily arms an automation that cannot work.

So the blanks are **derived from the automation itself** rather than listed by
hand on each template. That keeps them true for automations built from
scratch, and means a template's steps and its checklist can never drift apart.

A blank is only reported when it is genuinely a problem, because a checklist
that cries wolf is one people learn to dismiss. Two things earn a mention:

    it cannot work    — no role picked, so the step raises every single time.
    it matches all    — an empty "it's in certain channels" check matches
                        *every* channel, so a delete-messages automation
                        carrying one would sweep the whole server.

Everything else stays quiet. `Send a message` with no channel falls back to
wherever the trigger happened, which is right on a message trigger and
impossible on `member_joined` — so the trigger's own `provides` decides
whether that counts, and nobody is nagged about a field that is fine empty.

What this deliberately does **not** claim to know is intent. A ticket alert
with no channel picked posts into the ticket itself: working, and probably not
what you meant. That belongs to the template's own `needs` tips, which the
setup flow shows at the end.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import List

from utils.fieldspec import Field, is_unset
from .models import (Automation, ConditionGroup, find_condition, find_step_in,
                     walk_conditions, walk_steps)
from .registry import ACTIONS, CONDITIONS, TRIGGERS

# Where a blank lives, which is also how the setup flow navigates to it.
TRIGGER, CONDITION, ACTION = "trigger", "condition", "action"


@dataclass
class Blank:
    """One unfilled setting, with enough context to jump straight to it."""

    where: str                          # TRIGGER | CONDITION | ACTION
    path: List[int] = dc_field(default_factory=list)  # find_condition / find_step_in
    field: Field = None
    owner: str = ""                     # the entry it belongs to
    note: str = ""                      # why leaving it blank is a problem
    # A template's own wording for the question, when it has one. Asked instead
    # of the field's label, because a template knows what the setting is *for*
    # here — "Which staff channel should the alert go to?" rather than
    # "Which channel".
    prompt: str = ""
    # True for a template's own asks: worth a decision, but the automation
    # works without one, so these never stop `Turn on`.
    optional: bool = False

    @property
    def ref(self) -> str:
        """Stable identity, so a skipped blank stays skipped across redraws."""
        return f"{self.where}:{'.'.join(str(p) for p in self.path)}:{self.field.key}"

    @property
    def section(self) -> str:
        """Which of the three screens this sits behind."""
        return {TRIGGER: "① When this happens",
                CONDITION: "② Only if…",
                ACTION: "③ Then do this"}.get(self.where, "")


# Why an empty check matters, per condition. Every one of these falls back to
# "matches anything" when its main setting is blank (see `conditions.py`), so
# leaving one empty does not merely fail to narrow the automation — it widens
# it, which is the opposite of what adding a check was for.
MATCHES_EVERYTHING = {
    "channel_is": "left blank this matches **every channel**, not just the ones "
                  "you meant",
    "role_has": "left blank this matches **everyone**, whatever roles they have",
    "user_is": "left blank this matches **anyone**",
    "has_permission": "left blank this matches **everyone**",
    "content_contains": "left blank this matches **every message**",
    "content_starts_with": "left blank this matches **every message**",
    "content_ends_with": "left blank this matches **every message**",
    "content_exactly": "left blank this matches **every message**",
    "content_regex": "left blank this matches **every message**",
    "day_of_week": "left blank this matches **every day**",
    "counter_compare": "without a tally name there is nothing to compare",
    "variable_compare": "without a note name there is nothing to check",
}


def _nothing_to_send(config: dict) -> bool:
    """True when `Send a message` would have no content at all.

    Cross-field by nature: an embed with only a title and no body is perfectly
    valid, and so is plain text with no embed, so neither field can be marked
    required on its own. The action raises "there's nothing to send" at
    runtime, which is a failure buried in the run log.
    """
    if str(config.get("content") or "").strip():
        return False
    if str(config.get("text_above") or "").strip():
        return False
    if not config.get("use_embed"):
        return True
    return not any(str(config.get(key) or "").strip() for key in (
        "embed_title", "embed_image", "embed_thumbnail", "embed_footer",
        "embed_author"))


# Rules no single field can express, because they span several of them.
ACTION_CHECKS = {
    "send_message": (_nothing_to_send, "content",
                     "there's nothing to send yet, so this step would fail "
                     "every time"),
}


def _fields_of(spec, config: dict, *, provides: tuple):
    """The blanks in one entry's config, as (field, note) pairs."""
    for field in getattr(spec, "fields", ()) or ():
        if field.visible_when is not None and not field.visible_when(config):
            continue
        if not is_unset(field, config):
            continue
        if field.needs_context:
            # Blank means "use the one from the event", so this is only a
            # problem when the event hasn't got one.
            if field.needs_context not in provides:
                yield field, (f"this trigger has no {field.needs_context}, so "
                              f"one has to be picked here")
            continue
        if field.required:
            yield field, ""


def blanks(model: Automation) -> List[Blank]:
    """Every setting that has to be filled in before this can work."""
    found: List[Blank] = []
    trigger = TRIGGERS.get(model.trigger_type)
    provides = tuple(getattr(trigger, "provides", ()) or ()) if trigger else ()

    if trigger is not None:
        for field, note in _fields_of(trigger, model.trigger_config,
                                      provides=provides):
            found.append(Blank(TRIGGER, [], field, trigger.label, note))

    if model.conditions is not None:
        for path, node, _ in walk_conditions(model.conditions):
            if not path or isinstance(node, ConditionGroup):
                continue
            spec = CONDITIONS.get(node.type)
            if spec is None:
                continue
            why = MATCHES_EVERYTHING.get(node.type, "")
            for field, note in _fields_of(spec, node.config or {},
                                          provides=provides):
                found.append(Blank(CONDITION, list(path), field, spec.label,
                                   note or why))

    for path, step, _ in walk_steps(model.steps):
        if step.is_branch:
            continue
        spec = ACTIONS.get(step.type)
        if spec is None:
            continue
        config = step.config or {}
        seen = set()
        for field, note in _fields_of(spec, config, provides=provides):
            seen.add(field.key)
            found.append(Blank(ACTION, list(path), field, spec.label, note))
        check = ACTION_CHECKS.get(step.type)
        if check is not None:
            predicate, key, note = check
            if key not in seen and predicate(config):
                field = next((f for f in spec.fields if f.key == key), None)
                if field is not None:
                    found.append(Blank(ACTION, list(path), field, spec.label, note))

    return found


# --------------------------------------------------------------- template asks

def _spec_and_config(model: Automation, where: str, path: List[int]):
    """The (spec, config) a path addresses, or (None, None) if it does not.

    Hand-written paths are the one part of this module that can rot: a template
    gains a step and every path after it shifts by one. So resolution is a
    lookup that can fail rather than an assumption, and `check_readiness`
    asserts every declared path still lands on a node that has the named field.
    """
    if where == TRIGGER:
        spec = TRIGGERS.get(model.trigger_type)
        return spec, (model.trigger_config if spec else None)
    if where == CONDITION:
        node = find_condition(model.conditions, path) if model.conditions else None
        if node is None or isinstance(node, ConditionGroup):
            return None, None
        spec = CONDITIONS.get(node.type)
        if node.config is None:
            node.config = {}
        return spec, (node.config if spec else None)
    step = find_step_in(model.steps, path)
    if step is None or step.is_branch:
        return None, None
    spec = ACTIONS.get(step.type)
    if step.config is None:
        step.config = {}
    return spec, (step.config if spec else None)


def asks(model: Automation, template) -> tuple:
    """A template's own prompts, split by whether they still need answering.

    Returns `(unanswered, answered)`. The first are asked during setup and can
    be skipped; the second are offered at the end as worth revisiting. Which
    list an ask lands in is decided by the value that is actually there, so
    there is no "done" flag to keep in step with the config.

    Anything `blanks` already reports is dropped: being asked the same question
    twice, once as a requirement and once as a suggestion, is worse than not
    being asked at all.
    """
    if template is None:
        return [], []
    already = {(b.where, tuple(b.path), b.field.key) for b in blanks(model)}
    unanswered, answered = [], []
    for ask in getattr(template, "asks", ()) or ():
        spec, config = _spec_and_config(model, ask.where, ask.path)
        if spec is None or config is None:
            continue
        field = next((f for f in spec.fields if f.key == ask.field), None)
        if field is None:
            continue
        if field.visible_when is not None and not field.visible_when(config):
            continue
        if (ask.where, tuple(ask.path), ask.field) in already:
            continue
        entry = Blank(ask.where, list(ask.path), field,
                      getattr(spec, "label", ""), prompt=ask.prompt,
                      optional=True)
        (unanswered if is_unset(field, config) else answered).append(entry)
    return unanswered, answered


def is_ready(model: Automation) -> bool:
    """Whether this could actually run: a trigger, something to do, no blanks."""
    if TRIGGERS.get(model.trigger_type) is None:
        return False
    if not model.steps:
        return False
    return not blanks(model)


def summary(model: Automation) -> str:
    """One line for a list row — what is left, or "" when nothing is."""
    if TRIGGERS.get(model.trigger_type) is None:
        return "no trigger chosen yet"
    if not model.steps:
        return "nothing for it to do yet"
    count = len(blanks(model))
    if count:
        return f"{count} thing{'s' if count != 1 else ''} still to fill in"
    return ""
