"""Verify the setup checklist: what counts as blank, and that blanks are reachable.

`readiness` decides whether an automation can be turned on, so two kinds of bug
matter here and neither shows up as an exception:

  a blank it misses    — `Turn on` arms something that throws on every run, or
                         worse, a check left empty that matches *everything*
                         and sweeps the whole server.
  a blank it invents    — the checklist nags about a field that is fine empty,
                         which teaches people to ignore it, and `Turn on`
                         refuses for no reason.

The template expectations below are deliberately spelled out one by one rather
than derived, because "every template needs exactly these blanks" is the claim
the setup flow rests on, and a derived version would agree with whatever the
code currently does.

Run: .venv/bin/python tests/automations/check_readiness.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cogs.automations import readiness
from cogs.automations.models import (Automation, ConditionGroup,
                                             ConditionLeaf, Step,
                                             parse_condition, parse_steps)
from cogs.automations.registry import ACTIONS, CONDITIONS, TRIGGERS
from cogs.automations.templates import TEMPLATES
from cogs.automations.panel.setup import _config_for
from utils.fieldspec import Field, FieldType, is_unset

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


def automation(**kwargs) -> Automation:
    kwargs.setdefault("id", "test")
    kwargs.setdefault("name", "Test")
    kwargs.setdefault("trigger_type", "message_sent")
    return Automation(**kwargs)


def from_template(template) -> Automation:
    conditions = parse_condition(template.conditions)
    if not isinstance(conditions, ConditionGroup):
        conditions = ConditionGroup(items=[conditions])
    return automation(
        name=template.name, trigger_type=template.trigger_type,
        trigger_config=dict(template.trigger_config), conditions=conditions,
        steps=parse_steps(template.steps))


def refs(model) -> set:
    """(where, path, field key) for each blank, as comparable tuples."""
    return {(b.where, tuple(b.path), b.field.key) for b in readiness.blanks(model)}


# ------------------------------------------------------------------ is_unset

def check_is_unset():
    print("\n-- what counts as blank")

    channel = Field("c", "Channel", FieldType.CHANNEL)
    for value, expected, label in [
        (None, True, "missing"),
        (0, True, "zero"),
        ("0", True, "zero as a string"),
        ("", True, "empty string"),
        ([], True, "empty list"),
        ("[]", True, "empty JSON list"),
        ([0], True, "list holding only zero"),
        (123, False, "an id"),
        ("123", False, "an id as a string"),
        ([123], False, "a list holding an id"),
    ]:
        check(is_unset(channel, {"c": value}) is expected,
              f"channel {label} is {'blank' if expected else 'set'}")

    number = Field("n", "How many", FieldType.NUMBER)
    check(is_unset(number, {"n": 0}) is False,
          "number zero counts as set (slowmode 0 turns slowmode off)")
    check(is_unset(number, {}) is True, "missing number is blank")

    text = Field("t", "Text", FieldType.TEXT)
    # Whitespace-only is blank rather than set: `content_contains` splits on
    # commas and strips, so "   " yields no terms at all and the condition
    # falls back to matching anything — the exact case worth catching.
    check(is_unset(text, {"t": "  "}) is True,
          "whitespace-only text is blank, since it matches anything")
    check(is_unset(text, {"t": "hi"}) is False, "real text is set")
    check(is_unset(text, {"t": "0"}) is False, "the text '0' is set")

    multi = Field("m", "Days", FieldType.CHOICE, multi=True,
                  choices=[("0", "Monday")])
    check(is_unset(multi, {"m": []}) is True, "empty multi-choice is blank")
    check(is_unset(multi, {"m": ["0"]}) is False, "chosen multi-choice is set")


# ------------------------------------------------------ context-aware blanks

def check_needs_context():
    print("\n-- fields that fall back to the event")

    # Send a message with no channel picked posts wherever the trigger
    # happened, so it is fine on a message trigger and impossible on a join.
    step = Step(type="send_message",
                config={"destination": "channel", "content": "hi"})
    on_message = automation(trigger_type="message_sent", steps=[step])
    check(refs(on_message) == set(),
          "blank channel is not flagged when the trigger provides one")

    on_join = automation(trigger_type="member_joined", steps=[step])
    check(refs(on_join) == {("action", (0,), "channel")},
          "blank channel is flagged when the trigger has no channel")
    note = readiness.blanks(on_join)[0].note
    check("no channel" in note,
          f"the reason says why, not just that it is blank ({note!r})")

    # A DM or reply hides the channel field entirely, so it cannot be blank.
    for destination in ("dm", "reply"):
        model = automation(trigger_type="member_joined", steps=[Step(
            type="send_message",
            config={"destination": destination, "content": "hi"})])
        check(refs(model) == set(),
              f"a {destination} needs no channel even on a join")

    # Every needs_context value has to name something a trigger really
    # supplies, or the field is flagged on every trigger forever.
    supplied = set()
    for spec in TRIGGERS.values():
        supplied |= set(getattr(spec, "provides", ()) or ())
    for catalogue in (ACTIONS, CONDITIONS, TRIGGERS):
        for spec in catalogue.values():
            for field in getattr(spec, "fields", ()) or ():
                if field.needs_context:
                    check(field.needs_context in supplied,
                          f"{spec.key}.{field.key} falls back to "
                          f"'{field.needs_context}', which some trigger provides")


# --------------------------------------------------------- cross-field rules

def check_nothing_to_send():
    print("\n-- send a message with nothing in it")

    def blanks_for(config):
        return refs(automation(trigger_type="message_sent",
                               steps=[Step(type="send_message", config=config)]))

    empty = ("action", (0,), "content")
    check(blanks_for({"destination": "reply"}) == {empty},
          "no text and no embed is flagged")
    check(blanks_for({"destination": "reply", "content": "hi"}) == set(),
          "plain text is enough")
    check(blanks_for({"destination": "reply", "use_embed": True,
                      "embed_title": "Hello"}) == set(),
          "an embed with only a title is enough")
    check(blanks_for({"destination": "reply", "use_embed": True,
                      "embed_image": "https://x/y.png"}) == set(),
          "an embed with only a picture is enough")
    check(blanks_for({"destination": "reply", "text_above": "hi"}) == set(),
          "text above the embed is enough")
    check(blanks_for({"destination": "reply", "use_embed": True}) == {empty},
          "an embed switched on but left empty is flagged")
    check(blanks_for({"destination": "reply", "content": "   "}) == {empty},
          "whitespace-only content is nothing to send")


# ------------------------------------------------------- checks that match all

def check_matches_everything():
    print("\n-- checks that match everything when empty")

    # This is the dangerous case: `conditions.py` returns "no channels
    # configured, so this matches anything", so an empty channel check on a
    # delete-messages automation covers the whole server.
    model = automation(
        conditions=ConditionGroup(items=[
            ConditionLeaf(type="channel_is", config={"channels": []})]),
        steps=[Step(type="delete_message", config={})])
    found = readiness.blanks(model)
    check(refs(model) == {("condition", (0,), "channels")},
          "an empty channel check is flagged")
    check("every channel" in found[0].note,
          "the reason says it would match every channel")
    check(readiness.is_ready(model) is False,
          "an automation with an empty channel check is not ready")

    filled = automation(
        conditions=ConditionGroup(items=[
            ConditionLeaf(type="channel_is", config={"channels": [42]})]),
        steps=[Step(type="delete_message", config={})])
    check(readiness.is_ready(filled) is True,
          "the same automation is ready once a channel is picked")

    # Every condition whose implementation falls back to matching anything
    # should have its main field marked required, or the checklist stays quiet
    # about exactly the case that widens the rule.
    for key in readiness.MATCHES_EVERYTHING:
        spec = CONDITIONS.get(key)
        check(spec is not None, f"{key} is a real condition")
        if spec is not None:
            check(any(f.required for f in spec.fields),
                  f"{key} has a required field, so an empty one is caught")


# ----------------------------------------------------------------- readiness

def check_is_ready():
    print("\n-- overall readiness")

    check(readiness.is_ready(automation(trigger_type="nonsense",
                                       steps=[Step(type="delete_message")])) is False,
          "an unrecognised trigger is not ready")
    check(readiness.summary(automation(trigger_type="nonsense")) ==
          "no trigger chosen yet", "and says so")
    check(readiness.is_ready(automation(steps=[])) is False,
          "no actions is not ready")
    check(readiness.summary(automation(steps=[])) == "nothing for it to do yet",
          "and says so")

    ready = automation(steps=[Step(type="delete_message", config={})])
    check(readiness.is_ready(ready) is True, "a complete automation is ready")
    check(readiness.summary(ready) == "", "a ready automation has nothing to say")

    two = automation(trigger_type="member_joined", steps=[
        Step(type="send_message", config={"destination": "channel", "content": "a"}),
        Step(type="add_role", config={"role": 0})])
    check(readiness.summary(two) == "2 things still to fill in",
          "the count is pluralised")
    one = automation(trigger_type="member_joined", steps=[
        Step(type="add_role", config={"role": 0})])
    check(readiness.summary(one) == "1 thing still to fill in",
          "and reads correctly for one")

    # A branch holds its own steps, and a blank inside one still counts.
    branch = automation(steps=[Step(
        type="if", conditions=ConditionGroup(),
        then=[Step(type="add_role", config={"role": 0})], otherwise=[])])
    check(refs(branch) == {("action", (0, 0, 0), "role")},
          "a blank inside a split is found, at a path that addresses it")


# ----------------------------------------------------------------- templates

# What each ready-made automation still needs, as (where, path, field). These
# are the blanks the setup flow walks someone through, so they are asserted
# exactly: an unexpected one means the flow asks a question it should not, and
# a missing one means `Turn on` arms something broken.
EXPECTED = {
    "welcome": {("action", (0,), "channel")},
    "keyword_reply": set(),
    "react_to_images": {("condition", (0,), "channels")},
    "auto_thread": {("condition", (0,), "channels")},
    "no_links": {("condition", (0,), "channels")},
    "calm_the_caps": set(),
    "three_strikes": set(),
    "role_on_keyword": {("action", (0,), "role")},
    "temporary_role": {("action", (0,), "role")},
    # The channel post, not the DM before it — the DM hides its channel field.
    "role_welcome_pack": {("action", (1,), "channel")},
    "goodbye": {("action", (0,), "channel")},
    "level_up": {("action", (0,), "channel")},
    "thank_booster": {("action", (0,), "channel"), ("action", (1,), "role")},
    # A ticket supplies its own channel, so nothing is technically blank; the
    # template's `needs` tip covers picking a staff channel instead.
    "ticket_alert": set(),
    "quiet_hours": {("action", (0,), "channel")},
}


def check_templates():
    print("\n-- ready-made automations")

    check(set(EXPECTED) == {t.key for t in TEMPLATES},
          "every template is accounted for here")

    for template in TEMPLATES:
        model = from_template(template)
        expected = EXPECTED.get(template.key)
        if expected is None:
            continue
        found = refs(model)
        check(found == expected,
              f"{template.key} needs exactly {sorted(expected)}"
              + (f" (found {sorted(found)})" if found != expected else ""))

        # Whatever the checklist reports, the setup flow has to be able to
        # navigate to it — a path that does not resolve is a dead question.
        for blank in readiness.blanks(model):
            config = _config_for(model, blank)
            check(isinstance(config, dict),
                  f"{template.key}: the {blank.field.key} blank resolves to a "
                  f"real config dict")
            if isinstance(config, dict):
                check(is_unset(blank.field, config),
                      f"{template.key}: {blank.field.key} really is blank there")

        # A blank's owner and section are shown to the reader, so neither may
        # come out empty.
        for blank in readiness.blanks(model):
            check(bool(blank.owner) and bool(blank.section),
                  f"{template.key}: the {blank.field.key} blank says where it lives")

    # Templates promising a ready-to-run automation must actually be one.
    for template in TEMPLATES:
        model = from_template(template)
        if not EXPECTED.get(template.key):
            check(readiness.is_ready(model) is True,
                  f"{template.key} is ready the moment it is created")


def check_asks():
    print("\n-- a template's own questions")

    from cogs.automations.templates import Ask

    # Every declared path must resolve to a field that really exists on that
    # node. This is the one part of readiness that can rot: insert a step into
    # a template and every path after it shifts by one, silently.
    for template in TEMPLATES:
        model = from_template(template)
        unanswered, answered = readiness.asks(model, template)
        check(len(template.asks) == len(unanswered) + len(answered),
              f"{template.key}: all {len(template.asks)} declared ask(s) resolve "
              f"(got {len(unanswered)} + {len(answered)})")
        for entry in unanswered + answered:
            check(entry.optional is True,
                  f"{template.key}: {entry.field.key} is marked optional")
            check(bool(entry.prompt),
                  f"{template.key}: {entry.field.key} carries its own wording")
            check(isinstance(_config_for(model, entry), dict),
                  f"{template.key}: {entry.field.key} is reachable")
        # Unanswered means unset and vice versa — that split is what decides
        # whether something is asked during setup or merely offered at the end.
        for entry in unanswered:
            check(is_unset(entry.field, _config_for(model, entry)),
                  f"{template.key}: {entry.field.key} is asked because it is unset")
        for entry in answered:
            check(not is_unset(entry.field, _config_for(model, entry)),
                  f"{template.key}: {entry.field.key} is offered because it is set")

    # An ask must never duplicate a derived blank, or the same question gets
    # asked twice — once as a requirement, once as a suggestion.
    for template in TEMPLATES:
        model = from_template(template)
        derived = {(b.where, tuple(b.path), b.field.key)
                   for b in readiness.blanks(model)}
        unanswered, answered = readiness.asks(model, template)
        for entry in unanswered + answered:
            key = (entry.where, tuple(entry.path), entry.field.key)
            check(key not in derived,
                  f"{template.key}: {entry.field.key} is not also a derived blank")

    # A bad path is dropped rather than crashing or inventing a question.
    model = from_template(TEMPLATES[0])

    class Bogus:
        asks = [Ask("action", [99], "channel", "Nowhere"),
                Ask("condition", [42], "channels", "Nowhere"),
                Ask("action", [0], "no_such_field", "Nothing"),
                Ask("trigger", [], "no_such_field", "Nothing")]
        needs = []

    unanswered, answered = readiness.asks(model, Bogus())
    check(unanswered == [] and answered == [],
          "paths and field names that don't resolve are dropped, not guessed")
    check(readiness.asks(model, None) == ([], []),
          "no template means no questions, without a crash")

    # Optional questions must not affect whether it can be turned on.
    ticket = next(t for t in TEMPLATES if t.key == "ticket_alert")
    model = from_template(ticket)
    unanswered, _ = readiness.asks(model, ticket)
    check(len(unanswered) == 1,
          "ticket_alert asks which staff channel, since a ticket supplies its own")
    check(readiness.is_ready(model) is True,
          "and it is still ready to turn on regardless")
    check(readiness.summary(model) == "",
          "an unanswered optional question is not 'still to fill in'")


def check_editable():
    print("\n-- every blank can actually be edited")

    # The setup flow opens a picker based on the field's type. A type with no
    # picker would leave a question on screen with no way to answer it.
    editable = {FieldType.BOOL, FieldType.TEXT, FieldType.MULTILINE,
                FieldType.NUMBER, FieldType.DURATION, FieldType.CHANNEL,
                FieldType.ROLE, FieldType.USER, FieldType.EMOJI,
                FieldType.CHOICE}
    for catalogue, kind in ((TRIGGERS, "trigger"), (CONDITIONS, "condition"),
                            (ACTIONS, "action")):
        for spec in catalogue.values():
            for field in getattr(spec, "fields", ()) or ():
                if not (field.required or field.needs_context):
                    continue
                check(field.type in editable,
                      f"{kind} {spec.key}.{field.key} has an editor for its type")
                check(bool(field.label),
                      f"{kind} {spec.key}.{field.key} has a label to show")
                if field.type is FieldType.CHOICE:
                    check(bool(field.choices),
                          f"{kind} {spec.key}.{field.key} has choices to pick from")


def main():
    check_is_unset()
    check_needs_context()
    check_nothing_to_send()
    check_matches_everything()
    check_is_ready()
    check_templates()
    check_asks()
    check_editable()

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1
    print("All readiness checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
