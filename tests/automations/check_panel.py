"""Verify the /automations page tree: the builder, the diagram and its wording.

Structural checks come from tests/panel_harness.py, shared with the utility
suite. What is specific here is the language: the builder is the one screen a
person meets before they know what any of this is called, so a check that no
engine jargon reaches the screen is as much a real test as one that counts
components in a row.

Run: .venv/bin/python tests/automations/check_panel.py
"""
import asyncio
import inspect
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import discord

from cogs.automations.storage import AutomationsDB
from utils.panel import MAX_SELECT_OPTIONS, PanelPage, page_count, page_slice
from utils.panel_fields import (Field, FieldType, RecordEditor, display_value,
                                format_duration, parse_duration)

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


import panel_harness as harness  # noqa: E402  (needs sys.path set above)

harness.bind(check)
FakeCog = harness.FakeCog
FakeInteraction = harness.FakeInteraction
FakeGuild = harness.FakeGuild
walk_layout = harness.walk_layout
has_back = harness.has_back


async def main():
    tmp = Path(tempfile.mkdtemp())
    db = AutomationsDB(str(tmp / "t.db"))
    await db.connect()
    try:
        await run_checks(db)
    finally:
        # aiosqlite runs a background thread; leaving it open hangs interpreter
        # shutdown and turns a plain failure into a mystery timeout.
        await db.close()
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for failure in FAILURES:
            print(f"  - {failure}")
        sys.exit(1)
    print("All automations panel checks passed.")


async def run_checks(db):
    cog = FakeCog(db)
    from cogs.automations.panel.home import AutomationsHomePage

    print("\nHome")
    home = AutomationsHomePage(cog, owner_id=42)
    embed = await home._compose(FakeInteraction())
    walk_layout(home, "AutomationsHomePage")
    check(embed.author.name == "Automations", "breadcrumb on the root page")
    check(not has_back(home), "root page has no Back button")

    print("\nAutomations tree")
    from cogs.automations.models import Automation, ConditionGroup, Step
    from cogs.automations.panel.builder import (ActionPickerPage, AutomationDraft,
                                                AutomationEditor,
                                                AutomationListPage, BranchPage,
                                                ConditionPickerPage,
                                                ConditionsPage, RunsPage,
                                                SettingsPage, StepsPage,
                                                TriggerPage)

    auto_id = await db.insert_row(
        "automations", guild_id=1, name="Test automation",
        trigger_type="message_sent", enabled=0, dry_run=1, updated_ts=0)
    draft = await AutomationDraft.load(cog, auto_id)
    check(draft is not None, "an automation loads into a draft")
    check(draft.model.enabled is False and draft.model.dry_run is True,
          "a new automation is created off and in dry run")

    auto_list = AutomationListPage(cog, home)
    await auto_list._compose(FakeInteraction())
    walk_layout(auto_list, "AutomationListPage")
    check(has_back(auto_list), "AutomationListPage has Back")

    editor = AutomationEditor(cog, auto_list, draft)
    embed = await editor._compose(FakeInteraction())
    walk_layout(editor, "AutomationEditor")
    check(has_back(editor), "AutomationEditor has Back")
    # Off outranks test mode in the wording: an automation that cannot run at
    # all should not be described by how it would behave if it could.
    check("Off" in str(embed.to_dict()),
          "a switched-off automation reads as Off, not by its test-mode setting")
    await db.update_row("automations", auto_id, enabled=1)
    embed = await editor._compose(FakeInteraction())
    check("Test mode" in str(embed.to_dict()),
          "once switched on, it says it is in test mode")
    await db.update_row("automations", auto_id, dry_run=0)
    embed = await editor._compose(FakeInteraction())
    check("really doing" in str(embed.to_dict()),
          "and with test mode off, it says it is really doing things")
    await db.update_row("automations", auto_id, enabled=0, dry_run=1)
    draft = await AutomationDraft.load(cog, auto_id)

    for page_cls, name in ((TriggerPage, "TriggerPage"), (StepsPage, "StepsPage")):
        page = page_cls(cog, editor, draft)
        await page._compose(FakeInteraction())
        walk_layout(page, name)
        check(has_back(page), f"{name} has Back")

    settings = SettingsPage(cog, editor, draft)
    await settings._compose(FakeInteraction())
    walk_layout(settings, "SettingsPage")

    runs = RunsPage(cog, editor, auto_id)
    await runs._compose(FakeInteraction())
    walk_layout(runs, "RunsPage")

    print("\nBuilding a branch through the UI")
    from cogs.automations.panel.builder import (ConditionsAdvancedPage,
                                                StepsAdvancedPage)
    steps_page = StepsPage(cog, editor, draft)
    await steps_page._compose(FakeInteraction())
    interaction = FakeInteraction()
    # Splits live behind Advanced now — they're the one power feature on this
    # screen and were making it look complicated.
    steps_advanced = StepsAdvancedPage(cog, steps_page, draft)
    await steps_advanced._compose(FakeInteraction())
    walk_layout(steps_advanced, "StepsAdvancedPage")
    await steps_advanced._add_branch(interaction)
    check(len(draft.model.steps) == 1 and draft.model.steps[0].is_branch,
          "the branch was added to the model")
    reloaded = await AutomationDraft.load(cog, auto_id)
    check(len(reloaded.model.steps) == 1,
          "and persisted — a panel timeout would not lose it")
    check(len(interaction.sends) == 0, "adding a branch spawned no extra message")

    branch_page = BranchPage(cog, steps_page, draft, [0])
    await branch_page._compose(FakeInteraction())
    walk_layout(branch_page, "BranchPage")

    conditions = ConditionsPage(cog, branch_page, draft, [0])
    await conditions._compose(FakeInteraction())
    walk_layout(conditions, "ConditionsPage (a branch's)")
    check(has_back(conditions), "ConditionsPage has Back")

    print("\nThe top-level Only if screen, on a brand-new automation")
    # This is the path that broke in production: an automation with no
    # conditions stores '{}', which used to parse into a nameless leaf and made
    # Discord reject the whole select with "the bot didn't respond".
    fresh_id = await db.insert_row(
        "automations", guild_id=1, name="Fresh", trigger_type="message_sent")
    fresh = await AutomationDraft.load(cog, fresh_id)
    check(isinstance(fresh.model.conditions, ConditionGroup),
          "no conditions parses to an empty group, not a nameless check")
    check(fresh.model.conditions.is_empty(),
          f"and it really is empty ({fresh.model.conditions.items})")

    top = ConditionsPage(cog, home, fresh)
    await top._compose(FakeInteraction())
    walk_layout(top, "ConditionsPage (top level, empty)")
    check(top.top_level, "it knows it is the automation's own checks")

    # And once it holds something real.
    from cogs.automations.models import ConditionLeaf as CL
    fresh.model.conditions.items.append(CL(type="content_contains",
                                           config={"text": "hi"}))
    await fresh.save_graph(1)
    reloaded_fresh = await AutomationDraft.load(cog, fresh_id)
    check(len(reloaded_fresh.model.conditions.items) == 1,
          "a saved check survives the round trip")
    top2 = ConditionsPage(cog, home, reloaded_fresh)
    await top2._compose(FakeInteraction())
    walk_layout(top2, "ConditionsPage (top level, one check)")

    print("\nEvery template's Only if screen renders")
    from cogs.automations.templates import TEMPLATES as ALL_TEMPLATES
    for template in ALL_TEMPLATES:
        tid = await db.insert_row(
            "automations", guild_id=1, name=f"t-{template.key}",
            trigger_type=template.trigger_type,
            conditions=json.dumps(template.conditions),
            graph=json.dumps({"steps": template.steps}))
        tdraft = await AutomationDraft.load(cog, tid)
        page = ConditionsPage(cog, home, tdraft)
        await page._compose(FakeInteraction())
        walk_layout(page, f"Only if — {template.key}")
        spage = StepsPage(cog, home, tdraft)
        await spage._compose(FakeInteraction())
        walk_layout(spage, f"Then do this — {template.key}")

    print("\nUnfinished automations, and the flow that finishes them")
    # An automation with blanks in it gains a whole extra row for **Finish
    # setting up**, which puts the editor at exactly Discord's five-row limit —
    # so the layout is checked in that state and not only in the tidy one.
    from cogs.automations import readiness
    from cogs.automations.panel.builder import TemplateReadyPage
    from cogs.automations.panel.setup import SetupFlowPage, _config_for, _verb

    unfinished_seen = False
    for template in ALL_TEMPLATES:
        tid = await db.insert_row(
            "automations", guild_id=1, name=f"r-{template.key}",
            trigger_type=template.trigger_type,
            trigger_config=json.dumps(template.trigger_config),
            conditions=json.dumps(template.conditions),
            graph=json.dumps({"steps": template.steps}),
            enabled=0, dry_run=1, updated_ts=0)
        tdraft = await AutomationDraft.load(cog, tid)
        blanks = readiness.blanks(tdraft.model)

        editor = AutomationEditor(cog, auto_list, tdraft)
        await editor._compose(FakeInteraction())
        walk_layout(editor, f"AutomationEditor — {template.key}")
        check(has_back(editor), f"AutomationEditor keeps Back — {template.key}")
        labels = [c.label for c in editor.children if getattr(c, "label", None)]
        offers_setup = any("Finish setting up" in str(label) for label in labels)
        check(offers_setup is bool(blanks),
              f"{template.key}: offers Finish setting up exactly when blank "
              f"({len(blanks)} blank(s))")

        ready_page = TemplateReadyPage(cog, auto_list, tdraft, template)
        await ready_page._compose(FakeInteraction())
        walk_layout(ready_page, f"TemplateReadyPage — {template.key}")

        flow = SetupFlowPage(cog, editor, tdraft, template=template)
        await flow._compose(FakeInteraction())
        walk_layout(flow, f"SetupFlowPage — {template.key}")
        check(has_back(flow), f"SetupFlowPage keeps Back — {template.key}")

        # The flow asks about derived blanks and the template's own unanswered
        # questions, so it has something to ask whenever either exists.
        optional, answered = readiness.asks(tdraft.model, template)
        current = flow.current()
        check((current is not None) is bool(blanks or optional),
              f"{template.key}: the flow has a question exactly when one is due")
        check(len(template.asks) == len(optional) + len(answered),
              f"{template.key}: all {len(template.asks)} declared ask(s) resolve")
        if blanks and current is not None:
            check(current.optional is False,
                  f"{template.key}: requirements are asked before suggestions")

        # The last screen lists the already-answered ones, so walk that state
        # too — it carries a select, which is the layout most likely to break.
        for question in list(flow.remaining()):
            flow.skipped.add(question.ref)
        await flow._compose(FakeInteraction())
        walk_layout(flow, f"SetupFlowPage (finished) — {template.key}")
        flow.skipped.clear()
        await flow._compose(FakeInteraction())
        if current is not None:
            unfinished_seen = True
            # A question with no reachable setting behind it is a dead end.
            check(isinstance(_config_for(tdraft.model, current), dict),
                  f"{template.key}: the flow can reach {current.field.key}")
            check(bool(_verb(current.field)),
                  f"{template.key}: the fill button has a label")
            # Skipping has to move on rather than re-ask the same thing.
            first = current.ref
            flow.skipped.add(first)
            check(flow.current() is None or flow.current().ref != first,
                  f"{template.key}: skipping advances past the question")

    check(unfinished_seen,
          "at least one template exercises the unfinished layout")

    picker = ConditionPickerPage(cog, conditions, draft,
                                 draft.model.steps[0].conditions, [0])
    await picker._compose(FakeInteraction())
    walk_layout(picker, "ConditionPickerPage")

    action_picker = ActionPickerPage(cog, steps_page, draft, draft.model.steps)
    await action_picker._compose(FakeInteraction())
    walk_layout(action_picker, "ActionPickerPage")

    print("\nEvery registry entry renders as a config page")
    from cogs.automations.registry import ACTIONS, CONDITIONS, TRIGGERS
    from cogs.automations.panel.builder import ConfigEditor

    async def _noop(_config):
        pass

    for catalogue, kind in ((ACTIONS, "action"), (CONDITIONS, "condition"),
                            (TRIGGERS, "trigger")):
        for key, spec in catalogue.items():
            if not spec.fields:
                continue
            page = ConfigEditor(cog, editor, title=spec.label, fields=spec.fields,
                                config={}, on_save=_noop)
            await page._compose(FakeInteraction())
            walk_layout(page, f"{kind} {key}")

    print("\nTemplates all build a valid automation")
    from cogs.automations.models import parse_condition, parse_steps
    from cogs.automations.registry import ACTIONS as A, CONDITIONS as C
    from cogs.automations.templates import TEMPLATES

    def _walk_condition(node, seen):
        if "op" in node:
            for item in node.get("items", []):
                _walk_condition(item, seen)
        elif node.get("type"):
            seen.add(node["type"])

    def _walk_steps(steps, actions, conditions):
        for step in steps:
            if step["type"] == "if":
                _walk_condition(step.get("conditions", {}), conditions)
                _walk_steps(step.get("then", []), actions, conditions)
                _walk_steps(step.get("else", []), actions, conditions)
            else:
                actions.add(step["type"])

    for template in TEMPLATES:
        used_actions, used_conditions = set(), set()
        _walk_steps(template.steps, used_actions, used_conditions)
        _walk_condition(template.conditions or {}, used_conditions)

        check(template.trigger_type in TRIGGERS,
              f"{template.key}: trigger '{template.trigger_type}' exists")
        unknown_actions = used_actions - set(A)
        check(not unknown_actions,
              f"{template.key}: every action exists ({unknown_actions or 'ok'})")
        unknown_conditions = used_conditions - set(C)
        check(not unknown_conditions,
              f"{template.key}: every check exists ({unknown_conditions or 'ok'})")
        check(bool(template.steps), f"{template.key}: actually does something")

        # Guidance used to have to be prose in `needs`, so every template
        # carried some. It now comes from three places, and which one is right
        # depends on the template: a derived blank the flow already asks for,
        # a declared `asks` entry pointing at a setting, or prose for advice
        # that points at no setting at all. What matters is that a reader is
        # told *something* — and that the same thing is not said twice, which
        # is what the duplicate check below is for.
        from cogs.automations import readiness as _readiness
        tmodel = Automation(
            id=f"chk-{template.key}", name=template.name,
            trigger_type=template.trigger_type,
            trigger_config=dict(template.trigger_config),
            conditions=(parse_condition(template.conditions)
                        if isinstance(parse_condition(template.conditions),
                                      ConditionGroup)
                        else ConditionGroup(
                            items=[parse_condition(template.conditions)])),
            steps=parse_steps(template.steps))
        derived = _readiness.blanks(tmodel)
        declared_ok, declared_pending = _readiness.asks(tmodel, template)
        check(bool(derived or declared_ok or declared_pending or template.needs),
              f"{template.key}: tells the reader something about finishing it")
        check(len(template.asks) == len(declared_ok) + len(declared_pending),
              f"{template.key}: every declared ask path resolves to a real field")

        # A prose tip that repeats a question the flow already asks is the
        # thing this design set out to remove: read the checklist, do the
        # steps, then be told to do one of them again.
        asked_labels = {q.field.label.lower()
                        for q in list(derived) + list(declared_ok)}
        for tip in template.needs:
            overlap = [lab for lab in asked_labels
                       if lab in tip.lower()]
            check(not overlap,
                  f"{template.key}: prose tip does not repeat an asked setting "
                  f"({overlap or 'ok'}): {tip[:60]}")
        # The graph must survive the same parsing the engine will do.
        check(len(parse_steps(template.steps)) == len(template.steps),
              f"{template.key}: steps parse cleanly")
        parse_condition(template.conditions or {})

        # Every option a template presets must be a real option on that entry.
        for step in template.steps:
            if step["type"] == "if":
                continue
            valid = {f.key for f in A[step["type"]].fields}
            unknown = set(step.get("config", {})) - valid
            check(not unknown,
                  f"{template.key}: '{step['type']}' options are real ({unknown or 'ok'})")

    print("\nCreating from a template lands a working automation")
    from cogs.automations.panel.builder import (TemplatePickerPage,
                                                TemplateReadyPage)
    picker = TemplatePickerPage(cog, auto_list)
    await picker._compose(FakeInteraction())
    walk_layout(picker, "TemplatePickerPage")

    template = TEMPLATES[0]
    interaction = FakeInteraction()
    await picker._create(interaction, template)
    created = await db.fetchall(
        "SELECT * FROM automations WHERE name = ?", (template.name,))
    check(len(created) == 1, f"the template created one automation")
    row = created[0]
    check(row["enabled"] == 0 and row["dry_run"] == 1,
          "and it arrives switched off, so nothing can happen by surprise")
    rebuilt = Automation.from_row(row)
    check(rebuilt.trigger_type == template.trigger_type,
          "with its trigger set")
    check(rebuilt.step_count() == len(template.steps),
          f"and its actions ({rebuilt.step_count()})")
    check(len(interaction.sends) == 0, "no stray message was sent")

    ready = TemplateReadyPage(cog, auto_list, AutomationDraft(cog, rebuilt), template)
    await ready._compose(FakeInteraction())
    walk_layout(ready, "TemplateReadyPage")

    print("\nNo engine jargon reaches the screen")
    # These read as implementation vocabulary rather than instructions. If one
    # turns up in a label a user sees, the wording has drifted back.
    BANNED = ["dry run", "dry_run", "trigger type", "predicate", "boolean",
              "regex condition", "condition node", "negate", "snowflake",
              "graph", "enum", "traceback", "null", "config dict"]
    from cogs.automations.registry import TRIGGERS as T
    surfaces = []
    for catalogue in (T, C, A):
        for spec in catalogue.values():
            surfaces.append((spec.key, spec.label))
            surfaces.append((spec.key, spec.description))
            for f in spec.fields:
                surfaces.append((spec.key, f.label))
                surfaces.append((spec.key, f.help))
                for choice in f.choices:
                    surfaces.append((spec.key, choice[1]))
    offenders = [(k, t) for k, t in surfaces
                 if any(b in (t or "").lower() for b in BANNED)]
    check(not offenders, f"catalogue wording is plain ({offenders[:3] or 'clean'})")

    print("\nEvery label reads as a phrase, not an identifier")
    bad_case = [(k, t) for k, t in surfaces
                if t and ("_" in t or t.isupper() and len(t) > 4)]
    check(not bad_case, f"no snake_case or SHOUTING labels ({bad_case[:3] or 'clean'})")

    print("\nLive preview")
    from cogs.automations.panel.builder import ConfigEditor
    from cogs.automations.registry import ACTIONS as ACT

    async def _noop2(_config):
        pass

    send_spec = ACT["send_message"]
    cfg = {"destination": "channel", "content": "Hi {user.mention}!",
           "use_embed": True, "embed_title": "Welcome",
           "embed_image": "https://example.com/a.gif",
           "text_above": "{user.mention}"}
    editor = ConfigEditor(cog, home, title="Send a message",
                          fields=send_spec.fields, config=cfg, on_save=_noop2)
    check(editor.previewable, "a message editor offers a preview")

    interaction = FakeInteraction()
    await editor._compose(interaction)
    content, extras = await editor.companion(interaction)
    check(content is None and extras == [],
          "nothing extra is shown until the preview is switched on")

    await editor._toggle_preview(interaction)
    check(editor._preview, "the toggle turns it on")
    await editor._compose(interaction)
    content, extras = await editor.companion(interaction)
    check(len(extras) == 1, f"the preview renders one embed ({len(extras)})")
    preview = extras[0]
    check(preview.title == "Welcome", "with the title as configured")
    check(preview.image.url == "https://example.com/a.gif", "and the picture")
    check(content and "<@42>" in content,
          f"and the text above it, with placeholders filled in ({content!r})")
    walk_layout(editor, "ConfigEditor with preview")

    print("\nThe preview reflects edits")
    cfg["embed_title"] = "Changed"
    await editor._compose(interaction)
    _, extras = await editor.companion(interaction)
    check(extras[0].title == "Changed",
          "editing a setting updates the preview, without a stale second message")

    print("\nA plain message previews too")
    plain = ConfigEditor(cog, home, title="Send a message",
                         fields=send_spec.fields,
                         config={"content": "just text"}, on_save=_noop2)
    plain._preview = True
    await plain._compose(interaction)
    content, extras = await plain.companion(interaction)
    check(len(extras) == 1 and "just text" in (extras[0].description or ""),
          "a message with no embed is still shown")
    check("no embed box" in (extras[0].author.name or "").lower(),
          "and is labelled as a plain message")

    print("\nA broken preview doesn't take the panel down")
    class Exploding(dict):
        def get(self, *a, **k):
            raise RuntimeError("boom")

    broken = ConfigEditor(cog, home, title="x", fields=send_spec.fields,
                          config={}, on_save=_noop2)
    broken._preview = True
    broken._values = Exploding()
    content, extras = await broken.companion(interaction)
    check(len(extras) == 1 and extras[0].title == "Preview unavailable",
          "a preview that fails to build says so instead of erroring out")

    print("\nNo button asks you to understand boolean logic")
    # These were real buttons and every one of them made someone stop and
    # think about the model rather than the task.
    RETIRED = ["flip to the opposite", "all / any one", "add a sub-group",
               "negate", "and / or", "toggle", "invert", "operator", "boolean"]

    def buttons_on(view):
        return [b.label or "" for b in view.children
                if isinstance(b, discord.ui.Button)]

    fresh2 = await AutomationDraft.load(cog, fresh_id)
    screens = [
        ("Only if", ConditionsPage(cog, home, fresh2)),
        ("Then do this", StepsPage(cog, home, fresh2)),
        ("When this happens", TriggerPage(cog, home, fresh2)),
        ("Automation home", AutomationEditor(cog, auto_list, fresh2)),
        ("Advanced (checks)", ConditionsAdvancedPage(cog, home, fresh2, None)),
        ("Advanced (actions)", StepsAdvancedPage(cog, home, fresh2)),
    ]
    for name, screen in screens:
        await screen._compose(FakeInteraction())
        walk_layout(screen, name)
        for label in buttons_on(screen):
            hit = next((r for r in RETIRED if r in label.lower()), None)
            check(hit is None, f"{name}: button {label!r} is plain language")

    print("\nThe checks screen only shows the match rule when it matters")
    empty_checks = ConditionsPage(cog, home, fresh2)
    await empty_checks._compose(FakeInteraction())
    labels = " ".join(buttons_on(empty_checks)).lower()
    check("needs:" not in labels,
          f"with one check there's no all/any button to puzzle over ({labels})")

    two = await AutomationDraft.load(cog, fresh_id)
    two.model.conditions.items.append(CL(type="has_link"))
    await two.save_graph(1)
    two = await AutomationDraft.load(cog, fresh_id)
    many_checks = ConditionsPage(cog, home, two)
    await many_checks._compose(FakeInteraction())
    labels = buttons_on(many_checks)
    match_button = next((b for b in labels if b.lower().startswith("needs:")), None)
    check(match_button is not None,
          f"with two checks it appears, stating the current rule ({labels})")
    check("all of these" in (match_button or "").lower(),
          f"and reads as a sentence, not an operator ({match_button!r})")

    print("\nNegation is a setting on the check, not a mode")
    from cogs.automations.panel.builder import MATCH_FIELD, NEGATE_KEY
    target = two.model.conditions.items[0]
    check(not target.negate, "the check starts off matching normally")

    interaction = FakeInteraction()
    page = ConditionsPage(cog, home, two)
    await page._compose(interaction)
    page.selected = [0]
    await page._edit(interaction)
    opened = interaction.edits[-1] if interaction.edits else None
    check(opened is not None, "opening a check shows its settings")

    # Drive the setting the way the panel does.
    cfg = dict(target.config)
    cfg[NEGATE_KEY] = "yes"

    async def _apply(new_config):
        target.negate = new_config.pop(NEGATE_KEY, "no") == "yes"
        target.config = new_config
        await two.save_graph(1)

    await _apply(cfg)
    check(target.negate, "choosing 'Not match' flips the check")
    check(NEGATE_KEY not in target.config,
          "and the setting is stripped back out, not saved as a fake option")

    reloaded_two = await AutomationDraft.load(cog, fresh_id)
    check(reloaded_two.model.conditions.items[0].negate,
          "the flip survives a save and reload")

    listing = ConditionsPage(cog, home, reloaded_two)
    await listing._compose(FakeInteraction())
    text = listing._label([0], reloaded_two.model.conditions.items[0], 1)
    check("must NOT match" in text,
          f"and the list says so, since there's no button showing it ({text!r})")

    print("\nEvery check can be opened, even ones with no options")
    from cogs.automations.registry import CONDITIONS as ALL_CHECKS
    for key, spec in ALL_CHECKS.items():
        editor_page = ConfigEditor(
            cog, home, title=spec.label,
            fields=[MATCH_FIELD] + list(spec.fields),
            config={NEGATE_KEY: "no"}, on_save=_noop)
        await editor_page._compose(FakeInteraction())
        walk_layout(editor_page, f"check settings — {key}")

    print("\nThe diagram")
    from cogs.automations.panel import diagram
    from cogs.automations.models import parse_condition as pc
    from cogs.automations.templates import BY_KEY

    blank = Automation(id="d0", name="Blank", trigger_type="message_sent")
    text = diagram.render(blank)
    check("WHEN" in text and "ONLY IF" in text and "THEN" in text,
          "all three parts are always shown, so the shape is never a mystery")
    check("runs every time" in text,
          "no checks is stated as what it means, not left blank")
    check("won't do anything" in text,
          "an automation with no actions warns instead of showing an empty list")

    focused = diagram.render(blank, focus=diagram.ONLY_IF)
    check("you're here" in focused, "the section being edited is marked")
    check(focused.count("you're here") == 1, "and only that one")

    print("\nIt reads as English, not as settings")
    reply = BY_KEY["keyword_reply"]
    model = Automation(id="d1", name=reply.name, trigger_type=reply.trigger_type,
                       conditions=pc(reply.conditions),
                       steps=parse_steps(reply.steps))
    text = diagram.render(model)
    check('The message contains "hello, hi, hey"' in text,
          f"a check states its value, not its field name")
    check("where should it go" not in text and "what should it say" not in text,
          "field labels are not repeated back at you")
    check('Reply with a message: "Hey {user.name}! 👋"' in text,
          "an action says what it does in one line")
    check("**Not:** the message is a reply" in text,
          "a flipped check reads as 'Not:' rather than a double negative")

    print("\nSplits name their question")
    strikes = BY_KEY["three_strikes"]
    model = Automation(id="d2", name=strikes.name,
                       trigger_type=strikes.trigger_type,
                       conditions=pc(strikes.conditions),
                       steps=parse_steps(strikes.steps))
    text = diagram.render(model)
    check('If the "strikes" tally is at least 3' in text,
          "a split shows what it decides, not how many checks it has")
    check("If yes →" in text and "If no →" in text,
          "and both outcomes are visible")
    check("Time them out for 10m" in text, "including the nested actions")

    print("\nEvery template draws without error")
    for template in ALL_TEMPLATES:
        model = Automation(id=f"d-{template.key}", name=template.name,
                           trigger_type=template.trigger_type,
                           conditions=pc(template.conditions),
                           steps=parse_steps(template.steps))
        for focus in (None, diagram.WHEN, diagram.ONLY_IF, diagram.THEN):
            drawn = diagram.render(model, focus=focus)
            check(len(drawn) <= 4096,
                  f"{template.key}: fits Discord's description limit "
                  f"({len(drawn)})")
            check(bool(drawn.strip()), f"{template.key}: draws something")

    print("\nA half-configured automation still draws")
    messy = Automation(
        id="d3", name="Messy", trigger_type="nonexistent_trigger",
        conditions=pc({"op": "and", "items": [{"type": "no_such_check"}]}),
        steps=parse_steps([{"type": "no_such_action", "config": {}},
                           {"type": "send_message", "config": {}}]))
    drawn = diagram.render(messy)
    check("Not chosen yet" in drawn, "an unknown trigger says so")
    check("no_such_check" in drawn, "an unknown check is still listed")
    check("no longer available" in drawn, "an unknown action is flagged")

    print("\nThe editor screens all carry the diagram")
    diagram_draft = await AutomationDraft.load(cog, auto_id)
    for page_cls, name in ((TriggerPage, "When"), (StepsPage, "Then")):
        page = page_cls(cog, home, diagram_draft)
        embed = await page._compose(FakeInteraction())
        body = embed.description or ""
        check("WHEN" in body and "THEN" in body,
              f"{name} screen shows the whole automation")
        check("you're here" in body, f"{name} screen marks where you are")
    checks_page = ConditionsPage(cog, home, diagram_draft)
    embed = await checks_page._compose(FakeInteraction())
    check("you're here" in (embed.description or ""),
          "Only if screen marks where you are")

    print("\nAdvanced pages teach with an example")
    for page, name in ((ConditionsAdvancedPage(cog, home, diagram_draft, None), "groups"),
                       (StepsAdvancedPage(cog, home, diagram_draft), "splits")):
        embed = await page._compose(FakeInteraction())
        body = embed.description or ""
        check("```" in body, f"{name}: shows a worked example, not just prose")
        check("never need this" in body,
              f"{name}: says up front that most people can skip it")

    print("\nSource audit")
    harness.audit_send_message([Path("cogs/automations/panel"),
                                Path("utils/panel.py"),
                                Path("utils/panel_fields.py"),
                                Path("utils/panel_rules.py")])


if __name__ == "__main__":
    asyncio.run(main())
