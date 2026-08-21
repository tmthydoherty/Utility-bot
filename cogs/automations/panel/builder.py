"""Building an automation, in plain language.

An automation is three questions, and the screens are named after them:

    ① When this happens   — what sets it off
    ② Only if             — extra checks before it runs (optional)
    ③ Then do this        — what the bot actually does

Everything the user reads is written to complete one of those sentences, so
the automation can be read back as English. No screen says "trigger",
"predicate", "node" or "dry run"; the concepts are the same underneath, but a
person setting up their first automation should not have to learn a vocabulary
first.

New automations start from a ready-made template wherever possible — reading
something that already works and changing it is a far shorter path than
assembling one from an empty list.
"""

from __future__ import annotations

import json
import logging
from typing import Any, List, Optional

import discord

from utils import embed_builder as embeds
from utils.events import EventContext
from utils.fieldspec import Field, FieldType, is_unset, summarise_config
from utils.panel import (BackButton, PanelPage, add_pager, page_count,
                         page_placeholder, page_slice, respond_to_modal)
from utils.panel_fields import RecordEditor
from utils.panel_rules import AdminPage, DeleteRulePage, RuleListPage

from .. import engine, readiness
from ..models import (Automation, ConditionGroup, ConditionLeaf, Step,
                      container_for, find_condition, find_step_in,
                      parse_condition, parse_steps, remove_condition,
                      remove_step, walk_conditions, walk_steps)
from ..registry import (ACTIONS, CONDITIONS, TRIGGERS, categories,
                        missing_permissions, permission_name)
from ..storage import now
from ..templates import BY_KEY as TEMPLATES_BY_KEY
from ..templates import TEMPLATES
from . import diagram

logger = logging.getLogger('cogs.automations.panel.builder')

MAX_AUTOMATION_STEPS = 50

# Negation, presented as a setting on the check itself. The key is prefixed so
# it can never collide with a real option, and it is stripped back out before
# the config is saved.
NEGATE_KEY = "_match"
MATCH_FIELD = Field(
    NEGATE_KEY, "This requirement should", FieldType.CHOICE,
    "Switch to 'Not match' for the opposite — for example, messages that do "
    "NOT contain a word.",
    choices=[("no", "Match"), ("yes", "Not match")], default="no")

# How each run gets described in the activity list.
OUTCOMES = {
    "fired": ("✅", "Ran"),
    "dry_run": ("🧪", "Test only"),
    "skipped": ("⬜", "Didn't match"),
    "deferred": ("⏳", "Waiting"),
    "error": ("⚠️", "Problem"),
}


def _state_of(model: Automation) -> tuple:
    """(icon, short label, colour) for an automation's on/off/test state."""
    if not model.enabled:
        return "⬜", "Off", discord.Color.dark_grey()
    if model.dry_run:
        return "🧪", "Test mode", discord.Color.orange()
    return "✅", "On", discord.Color.green()


def _fit(text: str, limit: int = 100) -> str:
    """Trim to Discord's option-label limit, visibly rather than mid-word.

    Never returns an empty string: Discord rejects the whole select menu over
    one blank label, which reaches the user as "the bot didn't respond".
    """
    text = (text or "").strip()
    if not text:
        return "(unnamed)"
    if len(text) <= limit:
        return text
    return text[:limit - 1].rstrip() + "…"


def _path_key(path: List[int]) -> str:
    return ".".join(str(p) for p in path)


def _parse_path(raw: str) -> List[int]:
    return [int(p) for p in raw.split(".") if p.strip().lstrip("-").isdigit()]


# --------------------------------------------------------------- persistence

class AutomationDraft:
    """The automation being edited, saved as you go.

    Changes are written immediately rather than held until a Save button. A
    half-finished automation is harmless because a new one is created switched
    off, and being off survives the panel timing out — which an unsaved draft
    would not.
    """

    def __init__(self, cog, automation: Automation):
        self.cog = cog
        self.model = automation

    @classmethod
    async def load(cls, cog, automation_id: str) -> Optional["AutomationDraft"]:
        row = await cog.db.get_row("automations", automation_id)
        if row is None:
            return None
        return cls(cog, Automation.from_row(row))

    async def save_graph(self, user_id: int = 0):
        await self.cog.db.update_row(
            "automations", self.model.id,
            graph=self.model.graph_json(),
            conditions=self.model.conditions_json(),
            updated_ts=now(), updated_by=user_id,
        )
        await self.cog.save_made()

    async def save_field(self, user_id: int, **fields):
        await self.cog.db.update_row(
            "automations", self.model.id,
            updated_ts=now(), updated_by=user_id, **fields)
        await self.cog.save_made()

    async def save_trigger(self, user_id: int):
        await self.cog.db.update_row(
            "automations", self.model.id,
            trigger_type=self.model.trigger_type,
            trigger_config=json.dumps(self.model.trigger_config),
            updated_ts=now(), updated_by=user_id,
        )
        await self.cog.save_made()


# ------------------------------------------------------------ generic config

class ConfigEditor(RecordEditor):
    """A settings screen built from a list of `Field`s over a plain dict.

    Every option screen in the builder is this one class pointed at a
    different entry's fields, which is why adding an action needs no new UI.
    """

    def __init__(self, cog, parent: PanelPage, *, title: str, fields: List[Field],
                 config: dict, on_save, intro: str = ""):
        self.fields = fields
        self.title = title
        self.intro = intro
        self._config = config
        self._on_save = on_save
        super().__init__(cog, parent, record_id="config")

    async def is_allowed(self, interaction):
        return self.cog.is_admin(interaction.user)

    def header(self, values) -> str:
        return f"{self.intro}\n\nPick a setting below to change it.".strip()

    def notices(self, values: dict) -> List[str]:
        # Someone who edits a step directly rather than through the setup flow
        # should learn about a blank required setting here, not by pressing
        # Turn on later and being refused.
        notes = []
        for field in self.fields:
            if not field.required:
                continue
            if field.visible_when is not None and not field.visible_when(values):
                continue
            if is_unset(field, values):
                notes.append(f"**{field.label}** still needs a value — this "
                             f"can't be turned on until it has one.")
        # Harmless for anything without embed settings — it returns nothing
        # unless the embed toggle is actually on.
        return notes + embeds.warnings(values)

    async def load(self) -> dict:
        return dict(self._config)

    async def save(self, key: str, value: Any):
        # Snowflake and multi-choice selects hand back JSON; store real lists
        # so the engine never has to guess how a value was written.
        if isinstance(value, str) and value.startswith("["):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        self._config[key] = value
        await self._on_save(self._config)


# --------------------------------------------------- ① when this happens

class TriggerPage(AdminPage):
    """Pick what sets the automation off."""

    title = "When this happens"

    def __init__(self, cog, parent: PanelPage, draft: AutomationDraft):
        super().__init__(cog, parent)
        self.draft = draft
        options = []
        for category, specs in categories(TRIGGERS).items():
            for spec in specs:
                options.append(discord.SelectOption(
                    label=spec.label[:100],
                    value=spec.key,
                    description=category[:100],
                    default=spec.key == draft.model.trigger_type,
                ))
        self._options = options
        self.page = 0
        self._build()

    def _build(self):
        self.clear_items()
        select = discord.ui.Select(
            placeholder=page_placeholder("Choose what sets this off…",
                                         len(self._options), self.page),
            options=page_slice(self._options, self.page), row=0)
        select.callback = self._choose
        self.add_item(select)
        add_pager(self, len(self._options), self.page, row=1)

        spec = TRIGGERS.get(self.draft.model.trigger_type)
        if spec and spec.fields:
            configure = discord.ui.Button(label="Set its options",
                                          style=discord.ButtonStyle.primary, row=2)
            configure.callback = self._configure
            self.add_item(configure)
        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def show_page(self, interaction, page: int):
        self.page = max(0, min(page, page_count(len(self._options)) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        spec = TRIGGERS.get(self.draft.model.trigger_type)
        embed = discord.Embed(
            title="① When this happens",
            description=diagram.render(self.draft.model, focus=diagram.WHEN),
            color=discord.Color.blurple())
        body = ["Pick the thing that should set this automation off."]
        if spec:
            body.append(f"\n**{spec.label}** — {spec.description}")
            if spec.fields:
                from utils.fieldspec import display_value
                for field in spec.fields:
                    if field.visible_when and not field.visible_when(
                            self.draft.model.trigger_config):
                        continue
                    value = self.draft.model.trigger_config.get(field.key)
                    body.append(f"**{field.label}:** {display_value(field, value)}")
        embed.add_field(name="Choosing", value="\n".join(body)[:1024], inline=False)
        return embed

    async def _choose(self, interaction: discord.Interaction):
        key = interaction.data["values"][0]
        if key != self.draft.model.trigger_type:
            self.draft.model.trigger_type = key
            # Options belonging to the old choice mean nothing to the new one.
            self.draft.model.trigger_config = {}
            await self.draft.save_trigger(interaction.user.id)
        for option in self._options:
            option.default = option.value == key
        self._build()
        await self.render(interaction,
                          flash=f"✅ Set to **{TRIGGERS[key].label}**.")

    async def _configure(self, interaction: discord.Interaction):
        spec = TRIGGERS.get(self.draft.model.trigger_type)
        if spec is None or not spec.fields:
            await self.render(interaction, flash="This one has nothing to narrow down.")
            return

        async def _save(config):
            self.draft.model.trigger_config = config
            await self.draft.save_trigger(interaction.user.id)

        page = ConfigEditor(self.cog, self, title=spec.label,
                            fields=spec.fields,
                            config=dict(self.draft.model.trigger_config),
                            on_save=_save, intro=spec.description)
        await page.render(interaction)


# ------------------------------------------------------------- ② only if…

class ConditionsPage(AdminPage):
    """The checks that must pass before anything happens.

    `step_path` of None means the automation's own checks, which is the shape
    most automations want. A path means the checks belonging to one split
    inside the actions.
    """

    title = "Only if"

    def __init__(self, cog, parent: PanelPage, draft: AutomationDraft,
                 step_path: Optional[List[int]] = None):
        super().__init__(cog, parent)
        self.draft = draft
        self.step_path = step_path
        self.selected: Optional[List[int]] = None
        self.page = 0
        self._build()

    @property
    def top_level(self) -> bool:
        return self.step_path is None

    @property
    def root(self) -> ConditionGroup:
        if self.top_level:
            if self.draft.model.conditions is None:
                self.draft.model.conditions = ConditionGroup()
            return self.draft.model.conditions
        step = find_step_in(self.draft.model.steps, self.step_path)
        if step is None:
            return ConditionGroup()
        if step.conditions is None:
            step.conditions = ConditionGroup()
        return step.conditions

    def _nodes(self) -> list:
        """The conditions themselves — never the container holding them.

        The outermost group used to be listed as a row. On a new automation it
        was the *only* row, so the obvious thing to do was select it and press
        Change it, which could never work: it isn't a condition, it's the box
        they sit in.
        """
        return [(path, node, depth - 1)
                for path, node, depth in walk_conditions(self.root) if path]

    def _label(self, path, node, depth) -> str:
        indent = "     " * depth
        if isinstance(node, ConditionGroup):
            word = ("Any one of these" if node.op == "or"
                    else "All of these")
            if node.negate:
                word = f"NOT ({word.lower()})"
            return f"{indent}{word}:"
        spec = CONDITIONS.get(node.type)
        # Never empty: Discord rejects a select option with a blank label, and
        # it rejects the entire menu rather than that one entry.
        label = spec.label if spec else (node.type or "Unrecognised requirement")
        # Spelled out, because negation is no longer a button you can see the
        # state of — the list itself has to say which way round a check is.
        if node.negate:
            label = f"{label} — must NOT match"
        detail = summarise_config(spec.fields, node.config, limit=2) if spec else ""
        return f"{indent}{label}{f' ({detail})' if detail else ''}"

    def _build(self):
        self.clear_items()
        nodes = self._nodes()
        options = []
        for path, node, depth in page_slice(nodes, self.page):
            options.append(discord.SelectOption(
                label=_fit(self._label(path, node, depth)),
                value=_path_key(path) or "root",
                default=self.selected is not None and path == self.selected,
            ))
        select = discord.ui.Select(
            placeholder=page_placeholder("Pick one to change it…",
                                         len(nodes), self.page),
            options=options or [discord.SelectOption(label="(nothing yet)",
                                                     value="root")],
            row=0)
        select.callback = self._select
        self.add_item(select)
        add_pager(self, len(nodes), self.page, row=1)

        for label, style, callback in (
            ("Add a requirement", discord.ButtonStyle.success, self._add_condition),
            ("Change it", discord.ButtonStyle.primary, self._edit),
            ("Remove it", discord.ButtonStyle.danger, self._remove),
        ):
            button = discord.ui.Button(label=label, style=style, row=2)
            button.callback = callback
            self.add_item(button)

        # Reads as the sentence it controls rather than as an operator, and
        # only appears once there is more than one check for it to describe.
        if len(self.root.items) > 1:
            match = discord.ui.Button(
                label=("Needs: all of these" if self.root.op == "and"
                       else "Needs: any one of these"),
                style=discord.ButtonStyle.secondary, row=3)
            match.callback = self._toggle_op
            self.add_item(match)

        advanced = discord.ui.Button(label="Advanced",
                                     style=discord.ButtonStyle.secondary, row=3)
        advanced.callback = self._advanced
        self.add_item(advanced)

        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def _advanced(self, interaction: discord.Interaction):
        await ConditionsAdvancedPage(self.cog, self, self.draft,
                                     self.step_path).render(interaction)

    async def show_page(self, interaction, page: int):
        self.page = max(0, min(page, page_count(len(self._nodes())) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        if self.top_level:
            embed = discord.Embed(
                title="② Only if…",
                description=diagram.render(self.draft.model, focus=diagram.ONLY_IF),
                color=discord.Color.blurple())
        else:
            # A split's checks aren't part of the main flow, so the diagram
            # would be misleading — show just this split's own list.
            lines = [f"　{self._label(p, n, d)}" for p, n, d in self._nodes()]
            embed = discord.Embed(
                title="Requirements for this split",
                description="\n".join(lines) or "　*Nothing yet*",
                color=discord.Color.blurple())

        if self.root.is_empty():
            hint = ("**No requirements yet**, so this runs every single time.\n\n"
                    "Press **Add a requirement** to narrow it down — for example only "
                    "when the message contains a certain word, or only in one "
                    "channel.")
        else:
            hint = ("Press **Add a requirement** to add another, or pick one above "
                    "and press **Change it**.")
        embed.add_field(name="What to do here", value=hint, inline=False)
        return embed

    async def _select(self, interaction: discord.Interaction):
        raw = interaction.data["values"][0]
        self.selected = [] if raw == "root" else _parse_path(raw)
        self._build()
        await self.render(interaction)

    def _target_group(self) -> tuple:
        """Where a new check goes: the selection if it is a group, else its parent."""
        path = self.selected if self.selected is not None else []
        node = find_condition(self.root, path)
        if isinstance(node, ConditionGroup):
            return node, path
        parent_path = path[:-1] if path else []
        parent = find_condition(self.root, parent_path)
        if isinstance(parent, ConditionGroup):
            return parent, parent_path
        return self.root, []

    async def _add_condition(self, interaction: discord.Interaction):
        group, _ = self._target_group()
        await ConditionPickerPage(self.cog, self, self.draft, group,
                                  self.step_path).render(interaction)

    async def _edit(self, interaction: discord.Interaction):
        if not self.selected:
            hint = ("Pick one from the list above first."
                    if self._nodes() else
                    "There's nothing to change yet — press **Add a requirement**.")
            await self.render(interaction, flash=f"⚠️ {hint}")
            return
        node = find_condition(self.root, self.selected)
        if node is None:
            await self.render(interaction, flash="⚠️ That one is gone.")
            return
        if isinstance(node, ConditionGroup):
            await self.render(
                interaction,
                flash="⚠️ That's a group, not a requirement. Select a requirement "
                      "inside it, or use **Needs:** to change how it matches.")
            return
        spec = CONDITIONS.get(node.type)
        if spec is None:
            await self.render(interaction, flash="⚠️ That condition no longer exists.")
            return

        # "Should this match, or not match?" belongs with the check it applies
        # to. As a separate button it was a mode you had to notice, select
        # something for, and press — three steps to express "does not contain".
        config = dict(node.config)
        config[NEGATE_KEY] = "yes" if node.negate else "no"

        async def _save(new_config):
            node.negate = new_config.pop(NEGATE_KEY, "no") == "yes"
            node.config = new_config
            await self.draft.save_graph(interaction.user.id)

        page = ConfigEditor(self.cog, self, title=spec.label,
                            fields=[MATCH_FIELD] + list(spec.fields),
                            config=config, on_save=_save,
                            intro=spec.description)
        await page.render(interaction)

    async def _toggle_op(self, interaction: discord.Interaction):
        path = self.selected if self.selected is not None else []
        node = find_condition(self.root, path)
        if not isinstance(node, ConditionGroup):
            node = self.root
        node.op = "or" if node.op == "and" else "and"
        await self.draft.save_graph(interaction.user.id)
        self._build()
        await self.render(interaction, flash=(
            "✅ Now runs only when **every** requirement matches."
            if node.op == "and" else
            "✅ Now runs when **any one** of the requirements matches."))

    async def _remove(self, interaction: discord.Interaction):
        if not self.selected:
            await self.render(interaction, flash="⚠️ Pick one from the list first.")
            return
        if remove_condition(self.root, self.selected):
            self.selected = None
            await self.draft.save_graph(interaction.user.id)
            self._build()
            await self.render(interaction, flash="✅ Removed.")
        else:
            await self.render(interaction, flash="⚠️ Couldn't remove that one.")


class ConditionsAdvancedPage(AdminPage):
    """Grouping — powerful, rarely needed, and off the main screen.

    A group lets you mix "all of these" with "any one of these" in the same
    rule. It is the only genuinely advanced idea in the checks screen, and
    having it sitting next to Add and Remove made the whole screen look
    complicated to someone who only wanted two simple checks.
    """

    title = "Advanced"

    def __init__(self, cog, parent: "ConditionsPage", draft: AutomationDraft,
                 step_path: Optional[List[int]]):
        super().__init__(cog, parent)
        self.draft = draft
        self.step_path = step_path

        add = discord.ui.Button(label="Add a group",
                                style=discord.ButtonStyle.primary, row=0)
        add.callback = self._add_group
        self.add_item(add)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="Groups",
            description=("*Most automations never need this — you can safely "
                         "go back.*\n\n" + diagram.GROUPS_EXAMPLE),
            color=discord.Color.blurple())
        embed.add_field(
            name="If you add one",
            value=("An empty **any one of these** group appears in your list "
                   "of requirements. Select it, then press **Add a requirement** "
                   "to put requirements inside it."),
            inline=False)
        return embed

    async def _add_group(self, interaction: discord.Interaction):
        parent: ConditionsPage = self.parent_page
        group, _ = parent._target_group()
        # Starts as "any one of": nesting an all-of inside an all-of changes
        # nothing, so mixing the two is the only reason to make a group.
        group.items.append(ConditionGroup(op="or"))
        await self.draft.save_graph(interaction.user.id)
        parent.selected = None
        parent._build()
        await parent.render(
            interaction,
            flash="✅ Added an **any one of these** group. Select it and press "
                  "**Add a requirement** to put requirements inside it.")


class ConditionPickerPage(AdminPage):
    """Choose which requirement to add."""

    title = "Add a requirement"

    def __init__(self, cog, parent: ConditionsPage, draft: AutomationDraft,
                 group: ConditionGroup, step_path: Optional[List[int]]):
        super().__init__(cog, parent)
        self.draft = draft
        self.group = group
        self.step_path = step_path
        self._specs = list(CONDITIONS.values())
        self.page = 0
        self._build()

    def _build(self):
        self.clear_items()
        options = [
            discord.SelectOption(label=spec.label[:100], value=spec.key,
                                 description=spec.category[:100])
            for spec in page_slice(self._specs, self.page)
        ]
        select = discord.ui.Select(
            placeholder=page_placeholder("Choose a condition…", len(self._specs), self.page),
            options=options, row=0)
        select.callback = self._choose
        self.add_item(select)
        add_pager(self, len(self._specs), self.page, row=1)
        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def show_page(self, interaction, page: int):
        self.page = max(0, min(page, page_count(len(self._specs)) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="Add a requirement",
            description=("Pick something that has to be true for this automation "
                         "to run.\n\nThe list is grouped below; the dropdown has "
                         "all of them."),
            color=discord.Color.blurple())
        for category, specs in categories(CONDITIONS).items():
            embed.add_field(name=category,
                            value="\n".join(f"• {s.label}" for s in specs)[:1024],
                            inline=False)
        return embed

    async def _choose(self, interaction: discord.Interaction):
        key = interaction.data["values"][0]
        spec = CONDITIONS[key]
        leaf = ConditionLeaf(type=key, config={
            f.key: f.default for f in spec.fields if f.default is not None})
        self.group.items.append(leaf)
        await self.draft.save_graph(interaction.user.id)

        parent: ConditionsPage = self.parent_page
        parent.selected = None
        parent._build()
        if spec.fields:
            async def _save(config):
                leaf.config = config
                await self.draft.save_graph(interaction.user.id)

            page = ConfigEditor(self.cog, parent, title=spec.label,
                                fields=spec.fields, config=dict(leaf.config),
                                on_save=_save, intro=spec.description)
            await page.render(interaction)
        else:
            await parent.render(interaction, flash=f"✅ Added **{spec.label}**.")


# -------------------------------------------------------- ③ then do this

class StepsPage(AdminPage):
    """The list of things the bot does, in order."""

    title = "Then do this"

    def __init__(self, cog, parent: PanelPage, draft: AutomationDraft):
        super().__init__(cog, parent)
        self.draft = draft
        self.selected: Optional[List[int]] = None
        self.page = 0
        self._build()

    def _steps(self) -> list:
        return list(walk_steps(self.draft.model.steps))

    def _label(self, path, step: Step, depth: int) -> str:
        indent = "     " * depth
        if step.is_branch:
            total = len(step.conditions.items) if step.conditions else 0
            return (f"{indent}Split — {total} requirement"
                    f"{'s' if total != 1 else ''}, then do one of two sets "
                    f"({len(step.then)} or {len(step.otherwise)})")
        spec = ACTIONS.get(step.type)
        label = spec.label if spec else f"{step.type} (no longer available)"
        detail = summarise_config(spec.fields, step.config, limit=2) if spec else ""
        return f"{indent}{label}{f' ({detail})' if detail else ''}"

    def _build(self):
        self.clear_items()
        steps = self._steps()
        options = []
        for path, step, depth in page_slice(steps, self.page):
            options.append(discord.SelectOption(
                label=_fit(self._label(path, step, depth)),
                value=_path_key(path),
                default=self.selected == path,
            ))
        select = discord.ui.Select(
            placeholder=page_placeholder("Pick a step to change it…",
                                         len(steps), self.page),
            options=options or [discord.SelectOption(label="(nothing yet)",
                                                     value="none")],
            row=0)
        select.callback = self._select
        self.add_item(select)
        add_pager(self, len(steps), self.page, row=1)

        for label, style, callback in (
            ("Add an action", discord.ButtonStyle.success, self._add_action),
            ("Change it", discord.ButtonStyle.primary, self._edit),
            ("Remove it", discord.ButtonStyle.danger, self._remove),
        ):
            button = discord.ui.Button(label=label, style=style, row=2)
            button.callback = callback
            self.add_item(button)

        for label, callback in (
            ("Move up", self._move_up),
            ("Move down", self._move_down),
            ("Advanced", self._advanced),
        ):
            button = discord.ui.Button(label=label,
                                       style=discord.ButtonStyle.secondary, row=3)
            button.callback = callback
            self.add_item(button)

        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def _advanced(self, interaction: discord.Interaction):
        await StepsAdvancedPage(self.cog, self, self.draft).render(interaction)

    async def show_page(self, interaction, page: int):
        self.page = max(0, min(page, page_count(len(self._steps())) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="③ Then do this",
            description=diagram.render(self.draft.model, focus=diagram.THEN),
            color=discord.Color.blurple())

        if not self._steps():
            hint = ("**Nothing here yet**, so this automation won't do "
                    "anything.\n\nPress **Add an action** to say what the bot "
                    "should do. Most automations only need one.")
        else:
            hint = ("They happen in order, top to bottom. Pick one above and "
                    "press **Change it**, or use **Move up** / **Move down** "
                    "to reorder them.")
        embed.add_field(name="What to do here", value=hint, inline=False)

        missing = missing_permissions(interaction.guild, self.draft.model.steps)
        if missing:
            embed.add_field(
                name="⚠️ I'm missing a permission",
                value=("Give me " + ", ".join(f"**{permission_name(p)}**" for p in missing) +
                       " in Server Settings, or those steps will fail when this runs."),
                inline=False)
        return embed

    async def _select(self, interaction: discord.Interaction):
        raw = interaction.data["values"][0]
        self.selected = None if raw == "none" else _parse_path(raw)
        self._build()
        await self.render(interaction)

    def _target_list(self) -> List[Step]:
        """New steps go inside the selected split, else next to the selection."""
        if self.selected:
            step = find_step_in(self.draft.model.steps, self.selected)
            if step is not None and step.is_branch:
                return step.then
            container = container_for(self.draft.model.steps, self.selected)
            if container is not None:
                return container
        return self.draft.model.steps

    async def _add_action(self, interaction: discord.Interaction):
        if self.draft.model.step_count() >= MAX_AUTOMATION_STEPS:
            await self.render(
                interaction,
                flash=f"⚠️ One automation can hold {MAX_AUTOMATION_STEPS} steps at most.")
            return
        await ActionPickerPage(self.cog, self, self.draft,
                               self._target_list()).render(interaction)

    async def _edit(self, interaction: discord.Interaction):
        if not self.selected:
            await self.render(interaction, flash="⚠️ Pick a step from the list first.")
            return
        step = find_step_in(self.draft.model.steps, self.selected)
        if step is None:
            await self.render(interaction, flash="⚠️ That step is gone.")
            return

        if step.is_branch:
            await BranchPage(self.cog, self, self.draft, self.selected).render(interaction)
            return

        spec = ACTIONS.get(step.type)
        if spec is None:
            await self.render(interaction, flash="⚠️ That action no longer exists.")
            return
        if not spec.fields:
            await self.render(interaction, flash=f"**{spec.label}** has nothing to set.")
            return

        async def _save(config):
            step.config = config
            await self.draft.save_graph(interaction.user.id)

        page = ConfigEditor(self.cog, self, title=spec.label, fields=spec.fields,
                            config=dict(step.config), on_save=_save,
                            intro=spec.description)
        await page.render(interaction)

    async def _move(self, interaction: discord.Interaction, delta: int):
        if not self.selected:
            await self.render(interaction, flash="⚠️ Pick a step from the list first.")
            return
        container = container_for(self.draft.model.steps, self.selected)
        if container is None:
            await self.render(interaction, flash="⚠️ Couldn't move that step.")
            return
        index = self.selected[-1]
        target = index + delta
        if target < 0 or target >= len(container):
            await self.render(interaction, flash="⚠️ It's already at the end.")
            return
        container[index], container[target] = container[target], container[index]
        self.selected = self.selected[:-1] + [target]
        await self.draft.save_graph(interaction.user.id)
        self._build()
        await self.render(interaction, flash="✅ Moved.")

    async def _move_up(self, interaction):
        await self._move(interaction, -1)

    async def _move_down(self, interaction):
        await self._move(interaction, 1)

    async def _remove(self, interaction: discord.Interaction):
        if not self.selected:
            await self.render(interaction, flash="⚠️ Pick a step from the list first.")
            return
        if remove_step(self.draft.model.steps, self.selected):
            self.selected = None
            await self.draft.save_graph(interaction.user.id)
            self._build()
            await self.render(interaction, flash="✅ Removed.")
        else:
            await self.render(interaction, flash="⚠️ Couldn't remove that step.")


class StepsAdvancedPage(AdminPage):
    """Splits — the one advanced idea in the actions screen."""

    title = "Advanced"

    def __init__(self, cog, parent: "StepsPage", draft: AutomationDraft):
        super().__init__(cog, parent)
        self.draft = draft

        add = discord.ui.Button(label="Add a split",
                                style=discord.ButtonStyle.primary, row=0)
        add.callback = self._add_branch
        self.add_item(add)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="Splits",
            description=("*Most automations never need this — you can safely "
                         "go back.*\n\n" + diagram.SPLIT_EXAMPLE),
            color=discord.Color.blurple())
        embed.add_field(
            name="If you add one",
            value=("A split appears in your list of actions. Select it, then "
                   "press **Change it** to set up its requirements and the two "
                   "sets of actions."),
            inline=False)
        return embed

    async def _add_branch(self, interaction: discord.Interaction):
        parent: StepsPage = self.parent_page
        if self.draft.model.step_count() >= MAX_AUTOMATION_STEPS:
            await parent.render(interaction, flash="⚠️ Step limit reached.")
            return
        parent._target_list().append(
            Step(type="if", conditions=ConditionGroup(), then=[], otherwise=[]))
        await self.draft.save_graph(interaction.user.id)
        parent.selected = None
        parent._build()
        await parent.render(
            interaction,
            flash="✅ Split added. Select it and press **Change it** to set up "
                  "its requirements and its two sets of actions.")


class BranchPage(AdminPage):
    """A split: checks, then one of two sets of actions."""

    title = "Split"

    def __init__(self, cog, parent: StepsPage, draft: AutomationDraft,
                 step_path: List[int]):
        super().__init__(cog, parent)
        self.draft = draft
        self.step_path = step_path

        conditions = discord.ui.Button(label="Set its requirements",
                                       style=discord.ButtonStyle.primary, row=0)
        conditions.callback = self._conditions
        self.add_item(conditions)

    async def build_embed(self, interaction) -> discord.Embed:
        step = find_step_in(self.draft.model.steps, self.step_path)
        if step is None:
            return discord.Embed(title="Split",
                                 description="*This split is gone.*",
                                 color=discord.Color.red())
        total = len(step.conditions.items) if step.conditions else 0
        embed = discord.Embed(
            title="Split",
            description=(
                "A split lets the automation do two different things.\n\n"
                f"It has **{total}** requirement{'s' if total != 1 else ''}. If "
                f"they pass it runs the first set of actions, and if not it runs "
                f"the second set.\n\n"
                "To add actions to either set, go back to **Then do this**, "
                "select this split, and use **Add an action**."),
            color=discord.Color.blurple())
        embed.add_field(name="If the requirements pass", value=f"{len(step.then)} action(s)")
        embed.add_field(name="If they don't",
                        value=f"{len(step.otherwise)} action(s)")
        return embed

    async def _conditions(self, interaction: discord.Interaction):
        await ConditionsPage(self.cog, self, self.draft,
                             self.step_path).render(interaction)


class ActionPickerPage(AdminPage):
    """Choose what the bot should do."""

    title = "Add an action"

    def __init__(self, cog, parent: StepsPage, draft: AutomationDraft,
                 target: List[Step]):
        super().__init__(cog, parent)
        self.draft = draft
        self.target = target
        self._specs = list(ACTIONS.values())
        self.page = 0
        self._build()

    def _build(self):
        self.clear_items()
        options = []
        for spec in page_slice(self._specs, self.page):
            label = f"{spec.label}{' ⚠' if spec.destructive else ''}"
            options.append(discord.SelectOption(
                label=label[:100], value=spec.key,
                description=spec.category[:100]))
        select = discord.ui.Select(
            placeholder=page_placeholder("Choose an action…", len(self._specs), self.page),
            options=options, row=0)
        select.callback = self._choose
        self.add_item(select)
        add_pager(self, len(self._specs), self.page, row=1)
        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def show_page(self, interaction, page: int):
        self.page = max(0, min(page, page_count(len(self._specs)) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="Add an action",
            description="Pick what the bot should do.",
            color=discord.Color.blurple())
        for category, specs in categories(ACTIONS).items():
            embed.add_field(name=category,
                            value="\n".join(f"• {s.label}" for s in specs)[:1024],
                            inline=False)
        embed.set_footer(text="⚠ marks something that removes or changes things.")
        return embed

    async def _choose(self, interaction: discord.Interaction):
        key = interaction.data["values"][0]
        spec = ACTIONS[key]
        step = Step(type=key, config={
            f.key: f.default for f in spec.fields if f.default is not None})
        self.target.append(step)
        await self.draft.save_graph(interaction.user.id)

        parent: StepsPage = self.parent_page
        parent.selected = None
        parent._build()
        if spec.fields:
            async def _save(config):
                step.config = config
                await self.draft.save_graph(interaction.user.id)

            page = ConfigEditor(self.cog, parent, title=spec.label,
                                fields=spec.fields, config=dict(step.config),
                                on_save=_save, intro=spec.description)
            await page.render(interaction)
        else:
            await parent.render(interaction, flash=f"✅ Added **{spec.label}**.")


# ------------------------------------------------------------------ settings

SETTINGS_FIELDS = [
    Field("priority", "Run order", FieldType.NUMBER,
          "If several automations match the same thing, lower numbers go first.",
          minimum=0, maximum=1000),
    Field("stop_after", "Stop other automations running", FieldType.BOOL,
          "When this one runs, no other automation runs for the same event."),
    Field("cooldown_s", "Wait between runs", FieldType.DURATION,
          "Stops it firing again straight away. Leave blank for no wait."),
    Field("cooldown_scope", "That wait applies to", FieldType.CHOICE,
          choices=[("user", "Each person separately"),
                   ("channel", "Each channel separately"),
                   ("guild", "The whole server")],
          visible_when=lambda v: bool(v.get("cooldown_s"))),
    Field("allow_bots", "Also run for bots", FieldType.BOOL,
          "Off by default. Leaving it off is what stops two automations "
          "replying to each other forever."),
]


class SettingsPage(RecordEditor):
    fields = SETTINGS_FIELDS
    title = "Extra settings"

    def __init__(self, cog, parent: PanelPage, draft: AutomationDraft):
        self.draft = draft
        super().__init__(cog, parent, draft.model.id)

    async def is_allowed(self, interaction):
        return self.cog.is_admin(interaction.user)

    def header(self, values) -> str:
        return ("These are optional. Most automations work fine without "
                "touching anything here.")

    async def load(self) -> dict:
        row = await self.cog.db.get_row("automations", self.record_id)
        return dict(row) if row else {}

    async def save(self, key: str, value: Any):
        await self.cog.db.update_row("automations", self.record_id, **{key: value})
        await self.cog.save_made()


# ------------------------------------------------------------------ activity

class RunsPage(AdminPage):
    """Every time this automation was set off, and what it decided."""

    title = "Activity"

    def __init__(self, cog, parent: PanelPage, automation_id: str):
        super().__init__(cog, parent)
        self.automation_id = automation_id
        self._runs: list = []
        self.selected: Optional[int] = None

    async def build_embed(self, interaction) -> discord.Embed:
        self._runs = await self.cog.db.fetchall(
            "SELECT * FROM automation_runs WHERE automation_id = ? "
            "ORDER BY ts DESC LIMIT 25", (self.automation_id,))
        self._build()

        if self.selected is not None:
            run = next((r for r in self._runs if r["id"] == self.selected), None)
            if run is not None:
                return self._trace_embed(run)

        if not self._runs:
            return discord.Embed(
                title="Activity",
                description=(
                    "**Nothing yet.**\n\n"
                    "A line appears here every time this automation is set off — "
                    "including when it decides not to do anything. So an empty "
                    "list means the thing that sets it off hasn't happened yet, "
                    "or the automation is switched off."),
                color=discord.Color.blurple())

        lines = []
        for run in self._runs[:12]:
            icon, word = OUTCOMES.get(run["outcome"], ("•", run["outcome"]))
            lines.append(f"{icon} **{word}** · <t:{run['ts']}:R> — {run['summary'][:100]}")
        embed = discord.Embed(title="Activity", description="\n".join(lines),
                              color=discord.Color.blurple())
        embed.set_footer(text="Pick one to see exactly what it checked and why.")
        return embed

    def _trace_embed(self, run) -> discord.Embed:
        try:
            trace = json.loads(run["trace"])
        except (json.JSONDecodeError, TypeError):
            trace = []
        icons = {True: "✅", False: "❌", None: "•"}
        lines = []
        for entry in trace:
            icon = icons.get(entry.get("result"), "•")
            lines.append(f"{icon} {entry.get('label')} — {entry.get('detail')}")
        icon, word = OUTCOMES.get(run["outcome"], ("•", run["outcome"]))
        embed = discord.Embed(
            title=f"{icon} {word}",
            description=("Step by step, this is what happened:\n\n"
                         + "\n".join(lines))[:4000] or "*Nothing recorded.*",
            color=discord.Color.green() if run["outcome"] in ("fired", "dry_run")
            else discord.Color.orange())
        embed.set_footer(text=f"Took {run['duration_ms']}ms · {run['summary'][:150]}")
        return embed

    def _build(self):
        self.clear_items()
        if self._runs:
            options = []
            for run in self._runs[:25]:
                icon, word = OUTCOMES.get(run["outcome"], ("•", run["outcome"]))
                options.append(discord.SelectOption(
                    label=f"{icon} {run['summary'][:90]}"[:100],
                    value=str(run["id"]),
                    description=word[:100],
                    default=self.selected == run["id"]))
            select = discord.ui.Select(
                placeholder="See what happened in one of these…",
                options=options, row=0)
            select.callback = self._select
            self.add_item(select)
        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def _select(self, interaction: discord.Interaction):
        self.selected = int(interaction.data["values"][0])
        await self.render(interaction)


# ---------------------------------------------------------------- automation

class AutomationEditor(AdminPage):
    """One automation's home screen — reads back as a sentence."""

    title = "Automation"

    def __init__(self, cog, parent: PanelPage, draft: AutomationDraft):
        super().__init__(cog, parent)
        self.draft = draft
        self._blanks: List = []
        self._optional: List = []
        self._template_key: str = ""
        self._build()

    def _build(self):
        """Rebuilt per render, because what to offer depends on the state.

        An unfinished automation leads with **Finish setting up** and has no
        Turn on to press; a finished one has no reason to mention setup. A
        disabled button that is always there teaches people to ignore a whole
        row, so the row changes shape instead.
        """
        self.clear_items()

        # The one thing to do next, when there is one, ahead of the three
        # sections so it reads before the navigation rather than after it.
        # Counted separately: a blank stops it running, an optional question
        # does not, and a single number would imply both are obligations.
        setup_row = bool(self._blanks or self._optional)
        if setup_row:
            if self._blanks:
                label = f"✨ Finish setting up ({len(self._blanks)} left)"
                style = discord.ButtonStyle.success
            else:
                count = len(self._optional)
                label = (f"✨ {count} optional setting"
                         f"{'s' if count != 1 else ''}")
                style = discord.ButtonStyle.secondary
            finish = discord.ui.Button(label=label, style=style, row=0)
            finish.callback = self._finish_setup
            self.add_item(finish)

        for label, callback in (
            ("① When this happens", self._trigger),
            ("② Only if…", self._conditions),
            ("③ Then do this", self._steps),
        ):
            button = discord.ui.Button(label=label,
                                       style=discord.ButtonStyle.primary,
                                       row=1 if setup_row else 0)
            button.callback = callback
            self.add_item(button)

        controls = 2 if setup_row else 1
        self._power = discord.ui.Button(label="Turn on", row=controls)
        self._power.callback = self._toggle_power
        self.add_item(self._power)

        self._mode = discord.ui.Button(label="Test mode",
                                       style=discord.ButtonStyle.secondary,
                                       row=controls)
        self._mode.callback = self._toggle_mode
        self.add_item(self._mode)

        test = discord.ui.Button(label="Try it now",
                                 style=discord.ButtonStyle.success, row=controls)
        test.callback = self._test
        self.add_item(test)

        for label, style, callback in (
            ("Activity", discord.ButtonStyle.secondary, self._runs),
            ("Extra settings", discord.ButtonStyle.secondary, self._settings),
            ("Rename", discord.ButtonStyle.secondary, self._rename),
            ("Delete", discord.ButtonStyle.danger, self._delete),
        ):
            button = discord.ui.Button(label=label, style=style, row=controls + 1)
            button.callback = callback
            self.add_item(button)

        if self.parent_page is not None:
            self.add_item(BackButton(self.parent_page, row=self.back_row))

    def _template(self):
        """The template this started from, if it still exists.

        Read from the row rather than remembered in the view, so finishing the
        setup a week later asks the same questions as finishing it immediately.
        A key from a template since removed simply resolves to None, and the
        derived blanks carry on regardless.
        """
        return TEMPLATES_BY_KEY.get(self._template_key or "")

    async def _finish_setup(self, interaction: discord.Interaction):
        from .setup import SetupFlowPage
        page = SetupFlowPage(self.cog, self, self.draft,
                             template=self._template())
        await page.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        reloaded = await AutomationDraft.load(self.cog, self.draft.model.id)
        if reloaded is None:
            return discord.Embed(title="Automation",
                                 description="*This automation was deleted.*",
                                 color=discord.Color.red())
        self.draft = reloaded
        model = self.draft.model
        icon, _, colour = _state_of(model)

        trigger = TRIGGERS.get(model.trigger_type)
        actions = model.step_count()

        # Recomputed before the components are laid out, since whether there is
        # a Finish setting up button depends on it.
        self._template_key = model.template_key
        self._blanks = readiness.blanks(model)
        # The button is offered for a template's own unanswered questions too,
        # since those are exactly what someone would otherwise never find.
        self._optional, _ = readiness.asks(model, self._template())
        self._build()

        # Buttons reflect the current state rather than describing an action
        # in the abstract.
        self._power.label = "Turn off" if model.enabled else "Turn on"
        self._power.style = (discord.ButtonStyle.secondary if model.enabled
                             else discord.ButtonStyle.success)
        self._mode.label = ("Test mode: ON" if model.dry_run else "Test mode: OFF")
        self._mode.style = (discord.ButtonStyle.primary if model.dry_run
                            else discord.ButtonStyle.secondary)

        # The whole automation as one picture. Everything else on this screen
        # is navigation; this is the thing you actually came to read.
        embed = discord.Embed(
            title=f"{icon} {model.name}",
            description=diagram.render(model),
            color=colour)
        if model.description:
            embed.description = f"*{model.description}*\n\n{embed.description}"

        # What this state actually means, in a sentence.
        if not model.enabled:
            meaning = "**Off** — nothing happens, whatever anyone does."
        elif model.dry_run:
            meaning = ("**Test mode** — it watches and writes to Activity, but "
                       "doesn't really do anything yet. Turn test mode off when "
                       "you're happy with it.")
        else:
            meaning = "**On** — it's really doing these things."
        embed.add_field(name="Right now", value=meaning, inline=False)

        # A checklist beats an error: it says what's left rather than what's
        # wrong. Every entry names the setting and the screen it lives behind,
        # so it can be acted on without first working out where to go.
        todo = []
        if not trigger:
            todo.append("Choose what sets it off — **① When this happens**")
        if not actions:
            todo.append("Add something for it to do — **③ Then do this**")
        for blank in self._blanks[:6]:
            line = f"Set **{blank.field.label}** for {blank.owner} — {blank.section}"
            if blank.note:
                line += f"\n　*{blank.note}*"
            todo.append(line)
        if len(self._blanks) > 6:
            todo.append(f"…and {len(self._blanks) - 6} more")
        missing = missing_permissions(interaction.guild, model.steps)
        if missing:
            todo.append("Give me " + ", ".join(f"**{permission_name(p)}**" for p in missing)
                        + " in Server Settings")
        if not todo and not model.enabled:
            todo.append("Looks ready — press **Turn on** when you want it live")
        if todo:
            name = ("Still to do" if not self._blanks else
                    "Still to do — press ✨ Finish setting up to work through these")
            embed.add_field(name=name,
                            value="\n".join(f"• {t}" for t in todo)[:1024],
                            inline=False)

        # Kept apart from "Still to do" so the two never disagree with each
        # other or with the button: these change what it does, but nothing here
        # stops it running.
        if self._optional:
            embed.add_field(
                name="Optional — this one's ready without them",
                value="\n".join(f"• {q.prompt or q.field.label}"
                                for q in self._optional[:5])[:1024],
                inline=False)

        row = await self.cog.db.get_row("automations", model.id)
        if row is not None and row["run_count"]:
            # Discord's timestamp markup only renders in a description or a
            # field, not in a footer, so the count goes here and the "when"
            # goes in Activity where it can be shown properly.
            embed.set_footer(
                text=f"Set off {row['run_count']} time(s) — see Activity for details")
        return embed

    async def _trigger(self, interaction):
        await TriggerPage(self.cog, self, self.draft).render(interaction)

    async def _conditions(self, interaction):
        await ConditionsPage(self.cog, self, self.draft).render(interaction)

    async def _steps(self, interaction):
        await StepsPage(self.cog, self, self.draft).render(interaction)

    async def _settings(self, interaction):
        await SettingsPage(self.cog, self, self.draft).render(interaction)

    async def _runs(self, interaction):
        await RunsPage(self.cog, self, self.draft.model.id).render(interaction)

    async def _toggle_power(self, interaction: discord.Interaction):
        model = self.draft.model
        turning_on = not model.enabled
        if turning_on and not model.steps:
            await self.render(
                interaction,
                flash="⚠️ Add an action in **③ Then do this** first — right now "
                      "it wouldn't do anything.")
            return
        # Arming something with blanks in it is the failure mode this whole
        # checklist exists to prevent: an unfilled channel makes every run
        # throw, and an unfilled check silently matches *everything*, which on
        # a delete-messages automation means the whole server.
        if turning_on:
            blanks = readiness.blanks(model)
            if blanks:
                first = blanks[0]
                await self.render(
                    interaction,
                    flash=(f"⚠️ Not ready yet — **{first.field.label}** for "
                           f"{first.owner} is still blank"
                           + (f" and {len(blanks) - 1} other thing"
                              f"{'s' if len(blanks) != 2 else ''}"
                              if len(blanks) > 1 else "")
                           + ". Press **✨ Finish setting up**."))
                return
        await self.draft.save_field(interaction.user.id, enabled=1 if turning_on else 0)
        model.enabled = turning_on
        if turning_on:
            note = ("✅ Turned on **in test mode** — it'll record what it would do "
                    "without doing it." if model.dry_run
                    else "✅ Turned on. It's live.")
        else:
            note = "⬜ Turned off."
        await self.render(interaction, flash=note)

    async def _toggle_mode(self, interaction: discord.Interaction):
        model = self.draft.model
        going_live = model.dry_run
        await self.draft.save_field(interaction.user.id,
                                    dry_run=0 if going_live else 1)
        model.dry_run = not going_live
        await self.render(
            interaction,
            flash=("✅ Test mode off — this will really do things now."
                   if going_live else
                   "🧪 Test mode on — it'll record what it would do, without "
                   "doing it."))

    async def _rename(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RenameModal(self, self.draft))

    async def _test(self, interaction: discord.Interaction):
        """Run it against you, here, now — with nothing actually happening."""
        model = self.draft.model
        ctx = EventContext(
            event=model.trigger_type, bot=self.cog.bot, guild=interaction.guild,
            member=interaction.user, user=interaction.user,
            channel=interaction.channel, simulate=True,
        )
        trace = await engine.explain(self.cog, model, ctx)
        icons = {True: "✅", False: "❌", None: "•"}
        lines = [f"{icons.get(e.get('result'), '•')} {e.get('label')} — {e.get('detail')}"
                 for e in trace.to_list()]
        body = "\n".join(lines) or "*Nothing ran — there are no actions yet.*"
        await TestResultPage(self.cog, self, body).render(interaction)

    async def _delete(self, interaction: discord.Interaction):
        page = DeleteRulePage(self.cog, self, "automations", self.draft.model.id,
                              f"the automation **{self.draft.model.name}**")
        await page.render(interaction)


class TestResultPage(AdminPage):
    title = "Try it now"

    def __init__(self, cog, parent: PanelPage, body: str):
        super().__init__(cog, parent)
        self.body = body

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="Try it now",
            description=("Pretending it just happened to you, in this channel:\n\n"
                         + self.body)[:4000],
            color=discord.Color.blurple())
        embed.set_footer(
            text="Nothing actually happened — this was only a rehearsal. "
                 "Requirements about messages may fail here, since there's no message.")
        return embed


class RenameModal(discord.ui.Modal, title="Rename this automation"):
    def __init__(self, page: AutomationEditor, draft: AutomationDraft):
        super().__init__()
        self.page = page
        self.draft = draft
        self.name = discord.ui.TextInput(
            label="Name", default=draft.model.name[:100], max_length=100)
        self.note = discord.ui.TextInput(
            label="Notes (optional)", required=False,
            style=discord.TextStyle.long,
            placeholder="A reminder to yourself about what this is for.",
            default=draft.model.description[:500] or None, max_length=500)
        self.add_item(self.name)
        self.add_item(self.note)

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name.value.strip() or self.draft.model.name
        await self.draft.save_field(interaction.user.id, name=name,
                                    description=self.note.value.strip())
        self.draft.model.name = name
        self.draft.model.description = self.note.value.strip()
        await respond_to_modal(interaction, self.page, "Renamed.")


# ------------------------------------------------------------------ new / list

class TemplatePickerPage(AdminPage):
    """Start from something that already works."""

    title = "New automation"

    def __init__(self, cog, parent: PanelPage):
        super().__init__(cog, parent)
        self.page = 0
        self._build()

    def _build(self):
        self.clear_items()
        options = [
            discord.SelectOption(label=t.name[:100], value=t.key,
                                 description=t.blurb[:100])
            for t in page_slice(TEMPLATES, self.page)
        ]
        select = discord.ui.Select(
            placeholder=page_placeholder("Pick a ready-made one…",
                                         len(TEMPLATES), self.page),
            options=options, row=0)
        select.callback = self._choose
        self.add_item(select)
        add_pager(self, len(TEMPLATES), self.page, row=1)

        scratch = discord.ui.Button(label="Start from scratch instead",
                                    style=discord.ButtonStyle.secondary, row=2)
        scratch.callback = self._scratch
        self.add_item(scratch)
        self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def show_page(self, interaction, page: int):
        self.page = max(0, min(page, page_count(len(TEMPLATES)) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title="New automation",
            description=("Pick one of these to get a working automation you can "
                         "then change. Everything arrives switched **off**, so "
                         "nothing happens until you're ready."),
            color=discord.Color.blurple())
        from ..templates import categories as template_categories
        for category, templates in template_categories().items():
            embed.add_field(
                name=category,
                value="\n".join(f"**{t.name}** — {t.blurb}" for t in templates)[:1024],
                inline=False)
        return embed

    async def _create(self, interaction: discord.Interaction, template):
        record_id = await self.cog.db.insert_row(
            "automations", guild_id=interaction.guild_id, name=template.name,
            description=template.blurb,
            trigger_type=template.trigger_type,
            trigger_config=json.dumps(template.trigger_config),
            conditions=json.dumps(template.conditions),
            graph=json.dumps({"steps": template.steps}),
            enabled=0, dry_run=1,
            # Remembered so someone finishing the setup days later still gets
            # this template's own questions, not just the derived blanks.
            template_key=template.key,
            created_by=interaction.user.id, updated_by=interaction.user.id,
            updated_ts=now(),
        )
        await self.cog.db.audit_log(interaction.user.id, "automations",
                                    record_id, "create", template.name)
        await self.cog.save_made()
        draft = await AutomationDraft.load(self.cog, record_id)
        page = TemplateReadyPage(self.cog, self.parent_page, draft, template)
        await page.render(interaction)

    async def _choose(self, interaction: discord.Interaction):
        template = TEMPLATES_BY_KEY[interaction.data["values"][0]]
        await self._create(interaction, template)

    async def _scratch(self, interaction: discord.Interaction):
        await interaction.response.send_modal(NewAutomationModal(self.parent_page))


class TemplateReadyPage(AdminPage):
    """What a freshly created template still needs.

    Deliberately one screen and one button: the reader has just made a choice
    and the useful next move is to fill in the blanks, not to study a summary.
    Where those blanks are comes from `readiness`, so this page cannot promise
    a checklist that the builder then disagrees with.
    """

    title = "Almost there"

    def __init__(self, cog, parent: PanelPage, draft: AutomationDraft, template):
        super().__init__(cog, parent)
        self.draft = draft
        self.template = template
        self._blanks = readiness.blanks(draft.model)
        self._optional, _ = readiness.asks(draft.model, template)

        if self._blanks or self._optional:
            total = len(self._blanks) + len(self._optional)
            setup = discord.ui.Button(
                label=f"✨ Set it up ({total} question"
                      f"{'s' if total != 1 else ''})",
                style=discord.ButtonStyle.success, row=0)
            setup.callback = self._setup
        else:
            setup = discord.ui.Button(label="Open it",
                                      style=discord.ButtonStyle.success, row=0)
            setup.callback = self._open
        self.add_item(setup)

    async def build_embed(self, interaction) -> discord.Embed:
        embed = discord.Embed(
            title=f"Created: {self.template.name}",
            description=(f"{self.template.blurb}\n\n"
                         + diagram.render(self.draft.model)),
            color=discord.Color.green())

        if self._blanks:
            embed.add_field(
                name=f"{len(self._blanks)} thing"
                     f"{'s' if len(self._blanks) != 1 else ''} to fill in",
                value=("\n".join(f"• **{b.field.label}** — {b.owner}"
                                 for b in self._blanks[:6])
                       + "\n\nThe next screen asks for them one at a time."),
                inline=False)
        elif not self._optional:
            embed.add_field(
                name="Ready to go",
                value=("Nothing needs filling in. Press **Try it now** on the "
                       "next screen to rehearse it, then **Turn on**."),
                inline=False)

        # Listed even with nothing blocking, because this is the only place
        # someone would find out that the alert channel is worth choosing.
        if self._optional:
            embed.add_field(
                name="Worth deciding" + (" too" if self._blanks else ""),
                value=("\n".join(f"• {q.prompt or q.field.label}"
                                 for q in self._optional[:6])
                       + "\n\nOptional — it works without them."),
                inline=False)

        if self.template.needs:
            embed.add_field(
                name="Worth changing too",
                value="\n".join(f"• {n}" for n in self.template.needs)[:1024],
                inline=False)
        embed.set_footer(text="Switched off until you turn it on, so nothing "
                              "happens yet.")
        return embed

    async def _setup(self, interaction: discord.Interaction):
        from .setup import SetupFlowPage
        page = SetupFlowPage(self.cog, self.parent_page, self.draft,
                             template=self.template)
        await page.render(interaction)

    async def _open(self, interaction: discord.Interaction):
        await AutomationEditor(self.cog, self.parent_page,
                               self.draft).render(interaction)


class AutomationListPage(RuleListPage):
    table = "automations"
    title = "Automations"
    add_label = "New automation"
    empty_hint = ("Nothing here yet. An automation makes the bot react to "
                  "things by itself — press **New automation** and pick a "
                  "ready-made one to see how they work.")

    def __init__(self, cog, parent: Optional[PanelPage] = None, **kwargs):
        # Per instance rather than per class: a shared dict would serve one
        # admin's panel from another's last render.
        self._readiness: dict = {}
        super().__init__(cog, parent, **kwargs)

    def _left_to_do(self, row) -> str:
        """What this row still needs, parsed once per render.

        Both the icon and the two summary lines want this, and working it out
        means parsing the row's stored JSON — so it is memoised rather than
        recomputed four times for every row in the list.
        """
        key = row["id"]
        if key not in self._readiness:
            self._readiness[key] = readiness.summary(Automation.from_row(row))
        return self._readiness[key]

    def _icon(self, row) -> str:
        """Unfinished outranks off, because it is the state you can act on.

        An automation that is merely off was switched off on purpose. One with
        blanks in it cannot be turned on at all, and saying so in the list
        saves opening each one to find out which is which.
        """
        if self._left_to_do(row):
            return "🛠️"
        if not row["enabled"]:
            return "⬜"
        return "🧪" if row["dry_run"] else "✅"

    def summarise(self, row, guild) -> tuple:
        trigger = TRIGGERS.get(row["trigger_type"])
        when = (f"when {trigger.label.lower()}" if trigger
                else row["trigger_type"])
        left = self._left_to_do(row)
        return (f"{self._icon(row)} {row['name']}",
                f"{left} · {when}" if left else when)

    def describe(self, row, guild) -> str:
        trigger = TRIGGERS.get(row["trigger_type"])
        when = trigger.label.lower() if trigger else row["trigger_type"]
        left = self._left_to_do(row)
        tail = f" — *{left}*" if left else ""
        return f"{self._icon(row)} **{row['name']}** — when {when}{tail}"

    async def build_embed(self, interaction) -> discord.Embed:
        # Cleared before the rows are read, so an edit made elsewhere in the
        # panel shows up rather than being served from the last render.
        self._readiness = {}
        embed = await super().build_embed(interaction)
        if self._rows:
            embed.set_footer(
                text="✅ on  ·  🧪 test mode, not really doing it  ·  "
                     "⬜ off  ·  🛠️ needs finishing")
        if not await self.cog.automations_enabled():
            embed.add_field(
                name="⚠️ Everything is paused",
                value=("All automations are switched off by the pause button on "
                       "the main panel."),
                inline=False)
        return embed

    async def open_editor(self, interaction, row):
        draft = AutomationDraft(self.cog, Automation.from_row(row))
        await AutomationEditor(self.cog, self, draft).render(interaction)

    async def start_add(self, interaction: discord.Interaction):
        await TemplatePickerPage(self.cog, self).render(interaction)


class NewAutomationModal(discord.ui.Modal, title="New automation"):
    def __init__(self, page: AutomationListPage):
        super().__init__()
        self.page = page
        self.name = discord.ui.TextInput(
            label="Give it a name", max_length=100,
            placeholder="Welcome new members")
        self.add_item(self.name)

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name.value.strip() or "Untitled automation"
        # Created switched off: a half-built automation must not be able to
        # act, and being off survives the panel timing out.
        record_id = await self.page.cog.db.insert_row(
            "automations", guild_id=interaction.guild_id, name=name,
            trigger_type="message_sent", enabled=0, dry_run=1,
            created_by=interaction.user.id, updated_by=interaction.user.id,
            updated_ts=now(),
        )
        await self.page.cog.db.audit_log(interaction.user.id, "automations",
                                         record_id, "create", name)
        await self.page.cog.save_made()

        draft = await AutomationDraft.load(self.page.cog, record_id)
        editor = AutomationEditor(self.page.cog, self.page, draft)
        await respond_to_modal(
            interaction, editor,
            f"Created **{name}**. Start with **① When this happens**.")
