"""Declarative settings editing.

A screen per setting does not scale: the media rule alone has thirteen of
them, and Discord allows five components a row. So a record declares what its
fields *are* and this module renders them — one summary embed, one select to
pick a field, and the right editor for that field's type.

The same machinery drives the automations builder, where the fields come from
the trigger/condition/action registry instead of a database table. Anything
that can describe itself as a list of `Field` gets an editor for free — which
is why this lives in `utils/` and not inside either cog.

**Changes commit immediately** rather than accumulating into a draft with a
Save button. A draft that lives in a view is lost when the panel times out,
and it invites two admins to overwrite each other. Records that should not
act until they are finished protect themselves with an `enabled` flag that
starts off — inert until armed, and durable, which a draft is not.
"""

from __future__ import annotations

import json
import logging
from typing import Any, List, Optional

import discord

from utils import embed_builder, placeholders
from utils.events import EventContext
from utils.panel import (MAX_SELECT_OPTIONS, PanelPage, add_pager, page_slice,
                         page_placeholder, respond_to_modal)

# Re-exported: callers edit fields through this module and shouldn't have to
# know the descriptions themselves live a layer down.
from utils.fieldspec import (Field, FieldType, as_list as _as_list,
                             display_value, format_duration, parse_duration)

logger = logging.getLogger('utils.panel_fields')

__all__ = [
    "Field", "FieldType", "display_value", "format_duration", "parse_duration",
    "RecordEditor", "ValueModal", "ChoicePage", "SnowflakePage",
]


# ------------------------------------------------------------ record access

class RecordEditor(PanelPage):
    """Edits one record described by a list of `Field`s.

    Subclasses supply `fields`, a `load()` returning the current values as a
    dict, and a `save(key, value)` that persists one field.
    """

    fields: List[Field] = []
    title = "Edit"
    intro = ""

    def __init__(self, cog, parent: PanelPage, record_id: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.cog = cog
        self.record_id = record_id
        self.page = 0
        self._values: dict = {}
        self._preview = False
        self._build()

    # ---- preview

    @property
    def previewable(self) -> bool:
        """True when these fields describe something that gets sent."""
        keys = {f.key for f in self.fields}
        return bool(keys & {"content", "use_embed"})

    async def companion(self, interaction):
        """Render the message being edited, underneath the settings.

        Shown on the panel itself rather than as a second ephemeral: the
        preview has to stay in step with the settings above it, and a separate
        message would go stale the moment anything changed.
        """
        if not self._preview or not self.previewable:
            return None, []

        # Placeholders are rendered against whoever is looking, here and now,
        # so {user.mention} shows a real name instead of the literal token.
        ctx = EventContext(
            event="preview", bot=self.cog.bot, guild=interaction.guild,
            member=interaction.user, user=interaction.user,
            channel=interaction.channel, simulate=True,
        )
        try:
            content, embed = embed_builder.build(self._values, ctx)
        except Exception as e:
            logger.warning(f"Preview failed to build: {e}")
            broken = discord.Embed(
                title="Preview unavailable",
                description=f"Something in these settings can't be rendered: {e}",
                color=discord.Color.red())
            return None, [broken]

        extras = []
        if embed is not None:
            extras.append(embed)
        elif content:
            # A plain message has no embed to show, so it is quoted instead —
            # otherwise there would be nothing under the divider.
            extras.append(discord.Embed(
                description=content[:4000],
                color=discord.Color.dark_grey()
            ).set_author(name="Plain message — no embed box"))
            content = None
        if content:
            content = f"**Preview** ↓\n{content}"
        return content, extras

    # ---- subclass hooks

    async def load(self) -> dict:
        raise NotImplementedError

    async def save(self, key: str, value: Any):
        raise NotImplementedError

    def header(self, values: dict) -> str:
        return self.intro

    def notices(self, values: dict) -> List[str]:
        """Warnings to show alongside the settings.

        For things that are configured wrongly but not *invalid* — they would
        save happily and then quietly misbehave at runtime.
        """
        return []

    # ---- rendering

    def visible_fields(self) -> List[Field]:
        return [f for f in self.fields
                if f.visible_when is None or f.visible_when(self._values)]

    def _build(self):
        self.clear_items()
        fields = self.visible_fields()
        options = []
        for f in page_slice(fields, self.page):
            options.append(discord.SelectOption(
                label=f.label[:100],
                value=f.key,
                description=(f.help or "")[:100] or None,
            ))
        if options:
            select = discord.ui.Select(
                placeholder=page_placeholder("Edit a setting…", len(fields), self.page),
                options=options,
                row=0,
            )
            select.callback = self._on_pick
            self.add_item(select)
            add_pager(self, len(fields), self.page, row=1)

        if self.previewable:
            preview = discord.ui.Button(
                label="Hide preview" if self._preview else "Show preview",
                style=(discord.ButtonStyle.primary if self._preview
                       else discord.ButtonStyle.secondary),
                row=2)
            preview.callback = self._toggle_preview
            self.add_item(preview)

            # Without this the shortcuts existed but nothing on screen ever
            # mentioned them, so the only way to find one was to be told.
            helper = discord.ui.Button(label="What can I write?",
                                       style=discord.ButtonStyle.secondary,
                                       row=2)
            helper.callback = self._open_help
            self.add_item(helper)

        self._add_extra_items()
        if self.parent_page is not None:
            from utils.panel import BackButton
            self.add_item(BackButton(self.parent_page, row=self.back_row))

    def _add_extra_items(self):
        """Hook for subclass buttons (enable toggle, delete, test…)."""

    async def _open_help(self, interaction: discord.Interaction):
        await PlaceholderHelpPage(self.cog, self).render(interaction)

    async def _toggle_preview(self, interaction: discord.Interaction):
        self._preview = not self._preview
        self._build()
        await self.render(
            interaction,
            flash=None if self._preview else "Preview hidden.")

    async def show_page(self, interaction: discord.Interaction, page: int):
        from utils.panel import page_count
        self.page = max(0, min(page, page_count(len(self.visible_fields())) - 1))
        self._build()
        await self.render(interaction)

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        self._values = await self.load()
        # Visibility can depend on values that just changed, so the component
        # list is rebuilt after loading rather than only on navigation.
        self._build()
        embed = discord.Embed(
            title=self.title,
            description=self.header(self._values) or None,
            color=discord.Color.blurple(),
        )
        lines = []
        for f in self.visible_fields():
            lines.append(f"**{f.label}** — {display_value(f, self._values.get(f.key))}")
        # Embed fields cap at 1024 characters; a long list becomes several.
        chunk, size = [], 0
        for line in lines:
            if size + len(line) > 900 and chunk:
                embed.add_field(name="​", value="\n".join(chunk), inline=False)
                chunk, size = [], 0
            chunk.append(line)
            size += len(line) + 1
        if chunk:
            embed.add_field(name="​", value="\n".join(chunk), inline=False)

        notices = self.notices(self._values)
        if notices:
            embed.add_field(name="⚠️ Worth knowing",
                            value="\n\n".join(f"• {n}" for n in notices)[:1024],
                            inline=False)
        return embed

    # ---- editing

    async def _on_pick(self, interaction: discord.Interaction):
        key = interaction.data["values"][0]
        field = next((f for f in self.fields if f.key == key), None)
        if field is None:
            await self.render(interaction, flash="⚠️ That setting no longer exists.")
            return
        await self.edit_field(interaction, field)

    async def edit_field(self, interaction: discord.Interaction, field: Field):
        current = self._values.get(field.key)

        if field.type is FieldType.BOOL:
            new = not bool(current)
            await self.save(field.key, 1 if new else 0)
            await self.render(
                interaction,
                flash=f"✅ **{field.label}** turned {'on' if new else 'off'}.",
            )
            return

        if field.type is FieldType.CHOICE:
            await ChoicePage(self.cog, self, field, self).render(interaction)
            return

        if field.type in (FieldType.CHANNEL, FieldType.ROLE, FieldType.USER):
            await SnowflakePage(self.cog, self, field, self).render(interaction)
            return

        await interaction.response.send_modal(
            ValueModal(self, field, current)
        )

    async def apply_value(self, interaction: discord.Interaction,
                          field: Field, value: Any, note: str = None):
        await self.save(field.key, value)
        await self.render(interaction, flash=f"✅ {note or f'{field.label} updated.'}")


class PlaceholderHelpPage(PanelPage):
    """Everything you can drop into a message, with worked examples."""

    title = "What can I write?"

    def __init__(self, cog, parent: PanelPage):
        super().__init__(parent)
        self.cog = cog

    async def is_allowed(self, interaction):
        return self.cog.is_admin(interaction.user)

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        embed = discord.Embed(
            title="What can I write?",
            description=(
                "Type these anywhere in a message and the bot fills them in "
                "when it sends it.\n\n"
                "**Example**\n"
                "> Welcome {user.mention}! Please read {#rules} "
                "and say hi in {#general}.\n\n"
                "…sends as *Welcome @Ash! Please read #rules and say hi in "
                "#general.*"),
            color=discord.Color.blurple())
        for heading, entries in placeholders.PLACEHOLDER_HELP:
            embed.add_field(
                name=heading,
                value="\n".join(f"`{token}` — {what}" for token, what in entries)[:1024],
                inline=False)
        embed.set_footer(
            text="Channel and role names don't need to be exact — {#rules} "
                 "finds 📜-server-rules. Use Show preview to check.")
        return embed


class ValueModal(discord.ui.Modal):
    """Text, number, duration and emoji-list editing."""

    def __init__(self, page: RecordEditor, field: Field, current: Any):
        super().__init__(title=field.label[:45])
        self.page = page
        self.field = field

        if field.type is FieldType.EMOJI:
            initial = " ".join(str(e) for e in _as_list(current))
        elif field.type is FieldType.DURATION:
            initial = format_duration(current) if current else ""
            if initial == "off":
                initial = ""
        else:
            initial = "" if current is None else str(current)

        style = (discord.TextStyle.long
                 if field.type is FieldType.MULTILINE else discord.TextStyle.short)
        placeholder = field.placeholder
        if not placeholder:
            if field.type is FieldType.DURATION:
                placeholder = "e.g. 30s, 10m, 2h30m, 1d — blank for off"
            elif field.type is FieldType.EMOJI:
                placeholder = "Emoji separated by spaces"
            elif field.type is FieldType.NUMBER:
                placeholder = "A whole number"

        self.input = discord.ui.TextInput(
            label=field.label[:45],
            style=style,
            required=False,
            default=initial[:4000] or None,
            placeholder=placeholder[:100] or None,
            max_length=4000 if style is discord.TextStyle.long else 200,
        )
        self.add_item(self.input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.input.value.strip()
        field = self.field

        if field.type is FieldType.NUMBER:
            try:
                value = int(raw) if raw else 0
            except ValueError:
                await respond_to_modal(interaction, self.page,
                                       f"⚠️ **{field.label}** needs a whole number.")
                return
            if field.minimum is not None and value < field.minimum:
                await respond_to_modal(interaction, self.page,
                                       f"⚠️ **{field.label}** must be at least {field.minimum}.")
                return
            if field.maximum is not None and value > field.maximum:
                await respond_to_modal(interaction, self.page,
                                       f"⚠️ **{field.label}** must be at most {field.maximum}.")
                return

        elif field.type is FieldType.DURATION:
            value = parse_duration(raw)
            if value is None:
                await respond_to_modal(
                    interaction, self.page,
                    f"⚠️ Couldn't read **{raw}** as a duration. Try `30s`, `10m` or `2h`.")
                return

        elif field.type is FieldType.EMOJI:
            value = json.dumps([e for e in raw.split() if e][:25])

        else:
            value = raw

        await self.page.save(field.key, value)
        await respond_to_modal(interaction, self.page, f"**{field.label}** updated.")


class ChoicePage(PanelPage):
    """Picks one value from a fixed set."""

    def __init__(self, cog, parent: PanelPage, field: Field, editor: RecordEditor):
        super().__init__(parent)
        self.cog = cog
        self.field = field
        self.editor = editor
        self.title = field.label
        current = {str(v) for v in _as_list(editor._values.get(field.key))}
        options = [
            discord.SelectOption(label=label[:100], value=str(value),
                                 description=(desc[0][:100] if desc else None),
                                 default=str(value) in current)
            for value, label, *desc in field.choices[:MAX_SELECT_OPTIONS]
        ]
        select = discord.ui.Select(
            placeholder="Choose…", options=options, row=0,
            min_values=0 if field.multi else 1,
            max_values=len(options) if field.multi else 1,
        )
        select.callback = self._on_choose
        self.add_item(select)

    async def is_allowed(self, interaction):
        return self.cog.is_admin(interaction.user)

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        return discord.Embed(
            title=self.field.label,
            description=self.field.help or "Choose a value.",
            color=discord.Color.blurple(),
        )

    async def _on_choose(self, interaction: discord.Interaction):
        chosen = interaction.data.get("values", [])
        if self.field.multi:
            value = json.dumps(chosen)
        else:
            value = chosen[0] if chosen else ""
        await self.editor.save(self.field.key, value)
        await self.editor.render(
            interaction, flash=f"✅ **{self.field.label}** set.")


class SnowflakePage(PanelPage):
    """Picks channels, roles or users with the native selects."""

    def __init__(self, cog, parent: PanelPage, field: Field, editor: RecordEditor):
        super().__init__(parent)
        self.cog = cog
        self.field = field
        self.editor = editor
        self.title = field.label

        maximum = 25 if field.multi else 1
        if field.type is FieldType.CHANNEL:
            select = discord.ui.ChannelSelect(
                placeholder=f"Select {'channels' if field.multi else 'a channel'}…",
                channel_types=[
                    discord.ChannelType.text, discord.ChannelType.news,
                    discord.ChannelType.forum, discord.ChannelType.voice,
                ],
                min_values=0, max_values=maximum, row=0,
            )
        elif field.type is FieldType.ROLE:
            select = discord.ui.RoleSelect(
                placeholder=f"Select {'roles' if field.multi else 'a role'}…",
                min_values=0, max_values=maximum, row=0,
            )
        else:
            select = discord.ui.UserSelect(
                placeholder=f"Select {'users' if field.multi else 'a user'}…",
                min_values=0, max_values=maximum, row=0,
            )
        select.callback = self._on_select
        self.add_item(select)

    async def is_allowed(self, interaction):
        return self.cog.is_admin(interaction.user)

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        note = "Selecting nothing clears this setting."
        return discord.Embed(
            title=self.field.label,
            description=f"{self.field.help}\n\n{note}".strip(),
            color=discord.Color.blurple(),
        )

    async def _on_select(self, interaction: discord.Interaction):
        chosen = [int(v) for v in interaction.data.get("values", [])]
        if self.field.multi:
            value = json.dumps(chosen)
        else:
            value = chosen[0] if chosen else 0
        await self.editor.save(self.field.key, value)
        await self.editor.render(
            interaction, flash=f"✅ **{self.field.label}** updated.")
