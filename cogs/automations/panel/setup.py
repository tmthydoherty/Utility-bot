"""Filling in the blanks, one question at a time.

A template arrives working but unfinished, and the old flow handed the reader
a prose checklist — *"Pick the channel the welcome should go to"* — then left
them to translate it into six clicks of navigation they had to invent. The
list could not tell them whether they had done it, and `Turn on` did not care
either way.

This screen asks for one thing at a time instead, in the order the automation
reads, and goes straight to the picker for it. It knows what is still blank
because `readiness` derives that from the automation itself, so finishing the
flow and being genuinely ready are the same condition rather than two things
that can disagree.

Nothing here is a wizard you are trapped in: every blank can be skipped, and
the automation stays switched off regardless until someone arms it.
"""

from __future__ import annotations

import json
import logging
from typing import Any, List, Optional

import discord

from utils.fieldspec import display_value
from utils.panel import PanelPage
from utils.panel_fields import RecordEditor
from utils.panel_rules import AdminPage

from .. import readiness
from ..models import find_condition, find_step_in
from . import diagram


def _fit(text: str, limit: int = 100) -> str:
    """Trim to Discord's option-label limit; a blank label rejects the menu."""
    text = (text or "").strip()
    if not text:
        return "(unnamed)"
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"

logger = logging.getLogger('cogs.automations.panel.setup')

# Which diagram section to highlight for a blank, so the picture on screen
# points at the part being filled in.
_FOCUS = {
    readiness.TRIGGER: diagram.WHEN,
    readiness.CONDITION: diagram.ONLY_IF,
    readiness.ACTION: diagram.THEN,
}


def _verb(field) -> str:
    """What the button says.

    Named for the kind of thing being picked rather than built from the field's
    own label, because those labels are written to head a settings row — "Set
    what should it say" is what happens when you splice one into a sentence.
    """
    from utils.fieldspec import FieldType
    if field.type is FieldType.CHANNEL:
        return "Pick the channels" if field.multi else "Pick a channel"
    if field.type is FieldType.ROLE:
        return "Pick the roles" if field.multi else "Pick a role"
    if field.type is FieldType.USER:
        return "Pick the people" if field.multi else "Pick a person"
    if field.type is FieldType.CHOICE:
        return "Choose an option"
    if field.type is FieldType.BOOL:
        return "Turn it on or off"
    if field.type is FieldType.EMOJI:
        return "Pick the emoji"
    if field.type is FieldType.DURATION:
        return "Set how long"
    return "Write it now"


def _config_for(model, blank: readiness.Blank) -> Optional[dict]:
    """The live config dict a blank belongs to, or None if it has moved.

    A live reference rather than a copy: the editor writes into it and the
    draft then persists the whole graph, which is how every other builder
    screen already works.
    """
    if blank.where == readiness.TRIGGER:
        return model.trigger_config
    if blank.where == readiness.CONDITION:
        node = find_condition(model.conditions, blank.path)
        if node is None or not hasattr(node, "config"):
            return None
        if node.config is None:
            node.config = {}
        return node.config
    step = find_step_in(model.steps, blank.path)
    if step is None:
        return None
    if step.config is None:
        step.config = {}
    return step.config


class BlankEditor(RecordEditor):
    """A one-setting editor that hands control straight back to the flow.

    It is never rendered itself — `edit_field` opens the right picker for the
    field's type and this object only exists to receive the value. Overriding
    `render` is what makes every path back land on the flow: the bool toggle,
    the choice list, the channel picker and the text modal all finish by
    rendering their editor, and here that means "next question".
    """

    def __init__(self, cog, flow: "SetupFlowPage", blank: readiness.Blank,
                 config: dict):
        self.flow = flow
        self.blank = blank
        self.fields = [blank.field]
        self._config = config
        super().__init__(cog, flow, record_id="blank")
        # Populated up front because the choice and snowflake pickers read it
        # for their current selection, and nothing renders this page first.
        self._values = dict(config)

    async def is_allowed(self, interaction):
        return self.cog.is_admin(interaction.user)

    async def load(self) -> dict:
        return dict(self._config)

    async def save(self, key: str, value: Any):
        # Snowflake and multi-choice pickers hand back JSON; store real lists so
        # the engine never has to guess how a value was written.
        if isinstance(value, str) and value.startswith("["):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        self._config[key] = value
        self._values[key] = value
        await self.flow.persist(self.blank)

    async def render(self, interaction, *, flash: Optional[str] = None):
        """Finishing a value means moving on, not returning to a field list.

        A filled blank needs nothing recorded — it simply stops being one, so
        the flow recomputes and asks the next question.
        """
        await self.flow.render(interaction, flash=flash)


class SetupFlowPage(AdminPage):
    """One blank at a time, in the order the automation reads."""

    title = "Finish setting up"

    def __init__(self, cog, parent: PanelPage, draft, *, template=None):
        super().__init__(cog, parent)
        self.draft = draft
        self.template = template
        # Skipped questions stay skipped for this visit, so "Skip" advances
        # rather than looping back onto the same one forever.
        self.skipped: set = set()
        self._total = max(1, len(self.remaining()))
        # Who is filling these in, for the row's `updated_by` audit column.
        self._actor = 0
        self._build()

    # ---- state

    @property
    def tips(self) -> List[str]:
        """Advice that points at no single setting, so it stays prose."""
        return list(getattr(self.template, "needs", ()) or [])

    def questions(self) -> tuple:
        """(to ask now, already answered).

        Requirements first, then the template's own suggestions: being asked to
        choose a staff channel before picking the role that makes the thing run
        at all would be the wrong way round.
        """
        model = self.draft.model
        unanswered, answered = readiness.asks(model, self.template)
        return readiness.blanks(model) + unanswered, answered

    def remaining(self) -> List[readiness.Blank]:
        pending, _ = self.questions()
        return [q for q in pending if q.ref not in self.skipped]

    def current(self) -> Optional[readiness.Blank]:
        pending = self.remaining()
        return pending[0] if pending else None

    async def persist(self, blank: readiness.Blank):
        if blank.where == readiness.TRIGGER:
            await self.draft.save_trigger(self._actor)
        else:
            await self.draft.save_graph(self._actor)

    # ---- components

    def _build(self):
        self.clear_items()
        blank = self.current()
        if blank is not None:
            fill = discord.ui.Button(
                label=_verb(blank.field),
                style=discord.ButtonStyle.success, row=0)
            fill.callback = self._fill
            self.add_item(fill)

            skip = discord.ui.Button(label="Skip for now",
                                     style=discord.ButtonStyle.secondary, row=0)
            skip.callback = self._skip
            self.add_item(skip)
        else:
            done = discord.ui.Button(label="Done — open it",
                                     style=discord.ButtonStyle.success, row=0)
            done.callback = self._open_editor
            self.add_item(done)

            # The template's own defaults, openable rather than merely listed —
            # "change the words to watch for" is useless advice if acting on it
            # means going and finding them yourself.
            _, answered = self.questions()
            if answered:
                options = [
                    discord.SelectOption(
                        label=_fit(b.prompt or b.field.label),
                        value=str(index),
                        description=_fit(b.owner, 100) or None)
                    for index, b in enumerate(answered[:25])
                ]
                select = discord.ui.Select(
                    placeholder="Change something it came with…",
                    options=options, row=1)
                select.callback = self._revisit_pick
                self.add_item(select)

        if self.parent_page is not None:
            from utils.panel import BackButton
            self.add_item(BackButton(self.parent_page, row=self.back_row))

    # ---- rendering

    async def build_embed(self, interaction) -> discord.Embed:
        # Reloaded so the picture reflects edits made through the pickers.
        # Through the draft's own class rather than importing AutomationDraft,
        # which would be a circular import: the builder opens this screen.
        reloaded = await type(self.draft).load(self.cog, self.draft.model.id)
        if reloaded is not None:
            self.draft = reloaded
        self._build()

        model = self.draft.model
        blank = self.current()
        pending = self.remaining()

        if blank is None:
            return self._finished_embed(model)

        position = max(1, self._total - len(pending) + 1)
        total = max(self._total, position)
        embed = discord.Embed(
            title=f"Finish setting up — {position} of {total}",
            description=diagram.render(model, focus=_FOCUS.get(blank.where)),
            color=discord.Color.blurple())

        # A template's own question is better wording than the field's label,
        # because it says what the setting is *for* in this automation.
        ask = [f"**{blank.prompt or blank.field.label}**"]
        if blank.field.help and not blank.prompt:
            ask.append(blank.field.help)
        ask.append(f"Belongs to **{blank.owner}**, under {blank.section}.")
        if blank.optional:
            ask.append("This one's optional — it already works without it.")
        elif blank.note:
            ask.append(f"⚠️ {blank.note[0].upper()}{blank.note[1:]}.")
        embed.add_field(
            name="Optional — worth a moment" if blank.optional else "This one next",
            value="\n\n".join(ask)[:1024], inline=False)

        if len(pending) > 1:
            later = [f"• {b.prompt or b.field.label}"
                     + (" *(optional)*" if b.optional else "")
                     for b in pending[1:6]]
            if len(pending) > 6:
                later.append(f"• …and {len(pending) - 6} more")
            embed.add_field(name="Then", value="\n".join(later)[:1024],
                            inline=False)
        embed.set_footer(text="It stays switched off until you turn it on.")
        return embed

    def _finished_embed(self, model) -> discord.Embed:
        # Only a skipped *requirement* is a problem. A skipped suggestion is a
        # decision, and reporting it as unfinished business would make the
        # optional questions feel like obligations after all.
        blocked = [b for b in readiness.blanks(model) if b.ref in self.skipped]
        embed = discord.Embed(
            title=("Nothing left to fill in" if not blocked
                   else "Done, apart from what you skipped"),
            description=diagram.render(model),
            color=discord.Color.green() if not blocked
            else discord.Color.orange())

        if blocked:
            embed.add_field(
                name="⚠️ You skipped these",
                value=("\n".join(f"• **{b.field.label}** — {b.owner}"
                                 for b in blocked[:6])
                       + "\n\nIt can't be turned on until they're filled in. "
                         "Come back through **✨ Finish setting up** when you "
                         "want to."),
                inline=False)
        else:
            embed.add_field(
                name="What now",
                value=("Press **Done — open it** to see the whole automation, "
                       "where you can rehearse it with **Try it now** and then "
                       "**Turn on** when you're happy.\n\n"
                       "It starts in test mode, so it records what it *would* "
                       "do until you switch that off."),
                inline=False)

        # The template's defaults, each openable from the select below. These
        # already have sensible values, so they are a menu rather than a
        # checklist — nothing here is outstanding.
        _, answered = self.questions()
        if answered:
            embed.add_field(
                name="Worth a look — the wording and numbers it came with",
                value=("\n".join(
                    f"• {b.prompt or b.field.label} — "
                    f"currently {display_value(b.field, self._value_of(b))}"
                    for b in answered[:8])
                    + "\n\nPick one below to change it.")[:1024],
                inline=False)

        if self.tips:
            embed.add_field(
                name="Also worth doing",
                value="\n".join(f"• {t}" for t in self.tips)[:1024],
                inline=False)
        return embed

    def _value_of(self, question: readiness.Blank):
        config = _config_for(self.draft.model, question) or {}
        return config.get(question.field.key)

    # ---- actions

    async def _fill(self, interaction: discord.Interaction):
        blank = self.current()
        if blank is None:
            await self.render(interaction, flash="✅ Nothing left to fill in.")
            return
        self._actor = interaction.user.id
        config = _config_for(self.draft.model, blank)
        if config is None:
            # The step or check it belonged to was removed from another screen.
            self.skipped.add(blank.ref)
            await self.render(interaction,
                              flash="⚠️ That part is gone now — skipping it.")
            return
        editor = BlankEditor(self.cog, self, blank, config)
        await editor.edit_field(interaction, blank.field)

    async def _revisit_pick(self, interaction: discord.Interaction):
        """Open one of the already-answered questions from the last screen."""
        _, answered = self.questions()
        try:
            question = answered[int(interaction.data["values"][0])]
        except (KeyError, IndexError, ValueError):
            await self.render(interaction, flash="⚠️ That one has moved.")
            return
        self._actor = interaction.user.id
        config = _config_for(self.draft.model, question)
        if config is None:
            await self.render(interaction, flash="⚠️ That part is gone now.")
            return
        editor = BlankEditor(self.cog, self, question, config)
        await editor.edit_field(interaction, question.field)

    async def _skip(self, interaction: discord.Interaction):
        blank = self.current()
        if blank is not None:
            self.skipped.add(blank.ref)
        self._build()
        await self.render(interaction, flash="Skipped — you can come back to it.")

    async def _open_editor(self, interaction: discord.Interaction):
        from .automations import AutomationEditor
        # Reached from the editor itself, going "back" is the whole move —
        # opening a second editor on top would breadcrumb as
        # "Automation ▸ Automation" and leave a dead screen behind it.
        if isinstance(self.parent_page, AutomationEditor):
            await self.parent_page.render(interaction)
            return
        page = AutomationEditor(self.cog, self.parent_page, self.draft)
        await page.render(interaction)
