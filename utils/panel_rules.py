"""The list-with-add-edit-delete shape, shared by every rule-based panel.

`AdminPage` is the admin-gated base. `RuleListPage` is the screen that the
media, reaction, sticky and automation sections all are: a paginated list, an
Add button, and an editor behind each row. The only differences between them
are which table they read, how a row is summarised, and which editor opens.

Everything here talks to its cog through three things — `cog.is_admin`,
`cog.db` and `cog.save_made()` — so it works for any cog that has them and
belongs to none of them.

`save_made()` rather than `refresh_cache()` because a write is not always only
a cache concern: the Automations cog's data is also read and written by the
website, so its `save_made` bumps a revision counter as well as reloading. A
cog that owns its data alone implements it as a plain reload.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import discord

from utils.panel import (BackButton, ConfirmPage, PanelPage, add_pager,
                         page_count, page_placeholder, page_slice)

logger = logging.getLogger('utils.panel_rules')


class AdminPage(PanelPage):
    """Admin-gated page. The check runs per interaction, so an admin role
    removed mid-session loses access immediately."""

    title = "Settings"

    def __init__(self, cog, parent: Optional[PanelPage] = None, **kwargs):
        super().__init__(parent, **kwargs)
        self.cog = cog

    async def is_allowed(self, interaction: discord.Interaction) -> bool:
        return self.cog.is_admin(interaction.user)


class DeleteRulePage(ConfirmPage):
    """Confirms deleting one row, in place."""

    def __init__(self, cog, parent: PanelPage, table: str, record_id: str,
                 label: str):
        super().__init__(parent, prompt=f"Delete {label}?",
                         detail=("This happens straight away and can't be undone.\n\n"
                                 "If you only want to stop it for now, go back and "
                                 "switch it off instead."))
        self.cog = cog
        self.table = table
        self.record_id = record_id
        self.label = label

    async def is_allowed(self, interaction: discord.Interaction) -> bool:
        return self.cog.is_admin(interaction.user)

    async def on_confirm(self, interaction: discord.Interaction) -> str:
        await self.cog.db.delete_row(self.table, self.record_id)
        await self.cog.db.audit_log(interaction.user.id, self.table,
                                    self.record_id, "delete", self.label)
        await self.cog.save_made()
        return f"Deleted {self.label}."

    async def _confirm(self, interaction: discord.Interaction):
        note = await self.on_confirm(interaction)
        # Back past the editor of a row that no longer exists, to the list.
        target = getattr(self.parent_page, "parent_page", None) or self.parent_page
        await target.render(interaction, flash=f"✅ {note}")


class RuleListPage(AdminPage):
    """A paginated list of records with add / edit / delete.

    Subclasses set `table`, `empty_hint`, and implement `summarise`,
    `open_editor` and `start_add`.
    """

    table = ""
    empty_hint = "Nothing configured yet."
    add_label = "Add"

    def __init__(self, cog, parent: Optional[PanelPage] = None, **kwargs):
        super().__init__(cog, parent, **kwargs)
        self.page = 0
        self._rows: List = []
        self._build()

    # ---- subclass hooks

    def summarise(self, row, guild: discord.Guild) -> tuple:
        """(label, description) for the select option."""
        raise NotImplementedError

    def describe(self, row, guild: discord.Guild) -> str:
        """One line for the summary embed."""
        label, description = self.summarise(row, guild)
        state = "" if row["enabled"] else " *(off)*"
        return f"**{label}**{state} — {description}"

    async def open_editor(self, interaction: discord.Interaction, row):
        raise NotImplementedError

    async def start_add(self, interaction: discord.Interaction):
        raise NotImplementedError

    # ---- rendering

    async def load_rows(self) -> List:
        return await self.cog.db.list_rows(self.table, self._guild_id)

    def _build(self):
        self.clear_items()
        options = []
        for row in page_slice(self._rows, self.page):
            label, description = self._cached_summary.get(row["id"], (row["id"], ""))
            options.append(discord.SelectOption(
                label=label[:100], value=row["id"],
                description=description[:100] or None,
            ))
        if options:
            select = discord.ui.Select(
                placeholder=page_placeholder("Select to edit…", len(self._rows), self.page),
                options=options, row=0,
            )
            select.callback = self._on_pick
            self.add_item(select)
            add_pager(self, len(self._rows), self.page, row=1)

        add_button = discord.ui.Button(label=self.add_label,
                                       style=discord.ButtonStyle.success, row=2)
        add_button.callback = self._on_add
        self.add_item(add_button)

        if self.parent_page is not None:
            self.add_item(BackButton(self.parent_page, row=self.back_row))

    _guild_id: int = 0
    _cached_summary: dict = {}

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        self._guild_id = interaction.guild_id
        self._rows = await self.load_rows()
        self._cached_summary = {
            row["id"]: self.summarise(row, interaction.guild) for row in self._rows
        }
        self.page = max(0, min(self.page, page_count(len(self._rows)) - 1))
        self._build()

        if self._rows:
            body = "\n".join(self.describe(r, interaction.guild) for r in self._rows[:20])
            if len(self._rows) > 20:
                body += f"\n*…and {len(self._rows) - 20} more*"
        else:
            body = f"*{self.empty_hint}*"
        return discord.Embed(title=self.title, description=body,
                             color=discord.Color.blurple())

    async def show_page(self, interaction: discord.Interaction, page: int):
        self.page = max(0, min(page, page_count(len(self._rows)) - 1))
        self._build()
        await self.render(interaction)

    async def _on_pick(self, interaction: discord.Interaction):
        record_id = interaction.data["values"][0]
        row = next((r for r in self._rows if r["id"] == record_id), None)
        if row is None:
            await self.render(interaction, flash="⚠️ That rule was just removed.")
            return
        await self.open_editor(interaction, row)

    async def _on_add(self, interaction: discord.Interaction):
        await self.start_add(interaction)


class ChannelPickerPage(AdminPage):
    """Step one of adding a channel-scoped rule."""

    title = "Pick a channel"

    def __init__(self, cog, parent: PanelPage, *, prompt: str,
                 on_choose, channel_types=None):
        super().__init__(cog, parent)
        self.prompt = prompt
        self._on_choose = on_choose
        select = discord.ui.ChannelSelect(
            placeholder="Select a channel…",
            channel_types=channel_types or [
                discord.ChannelType.text, discord.ChannelType.news,
                discord.ChannelType.forum,
            ],
            min_values=1, max_values=1, row=0,
        )
        select.callback = self._picked
        self.add_item(select)

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        return discord.Embed(title=self.title, description=self.prompt,
                             color=discord.Color.blurple())

    async def _picked(self, interaction: discord.Interaction):
        channel_id = int(interaction.data["values"][0])
        await self._on_choose(interaction, channel_id)
