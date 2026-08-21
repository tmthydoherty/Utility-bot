"""One ephemeral message, edited forever.

A panel that spawns a new ephemeral on every button leaves the admin scrolling
a column of half-stale screens, each with its own now-wrong state. The pattern
here keeps a panel to exactly one message: every navigation is an
`edit_message` on the message the interaction arrived from, and the only
`send_message` in a whole page tree is the entry point.

The pieces:

* `ExpiringView` — a view that dies *visibly*. discord.py stops dispatching to
  a view once its timeout elapses but the components on screen still look
  live, so every later click lands on nothing and the user sees "the bot
  didn't respond in time", which reads as an outage rather than an expired
  panel. This greys the components out and says so.

* `PanelPage` — one screen. Subclasses override `build_embed`; passing
  `parent` gets a Back button for free and keeps the *whole ancestor chain's*
  timeout alive, so a grandparent can't quietly expire underneath a long edit.

* `respond_to_modal` — closes a modal by repainting the page behind it rather
  than stacking a confirmation the admin then has to dismiss.

* the pagination helpers — Discord caps a select at 25 options, and a silent
  truncation past that is the usual way a config UI starts lying.

Adapted from the /cm_settings tree in cogs/custommatch/views_settings.py,
which is where this pattern was first worked out.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence

import discord

log = logging.getLogger(__name__)

# Discord's hard cap on options in a single select menu.
MAX_SELECT_OPTIONS = 25


# ---------------------------------------------------------------- pagination

def page_count(total: int) -> int:
    return max(1, -(-total // MAX_SELECT_OPTIONS))


def page_slice(items: Sequence[Any], page: int) -> List[Any]:
    return list(items[page * MAX_SELECT_OPTIONS:(page + 1) * MAX_SELECT_OPTIONS])


def page_placeholder(base: str, total: int, page: int) -> str:
    """Page number goes in the placeholder — it costs no component slot."""
    pages = page_count(total)
    return base if pages <= 1 else f"{base} (page {page + 1}/{pages})"


class SelectPageButton(discord.ui.Button):
    """Steps a paginated select. Its view must implement `show_page`."""

    def __init__(self, delta: int, label: str, disabled: bool, row: int = 1):
        super().__init__(label=label, style=discord.ButtonStyle.secondary,
                         disabled=disabled, row=row)
        self.delta = delta

    async def callback(self, interaction: discord.Interaction):
        await self.view.show_page(interaction, self.view.page + self.delta)


def add_pager(view: discord.ui.View, total: int, page: int, row: int = 1):
    """Adds prev/next buttons, but only once the list actually overflows."""
    pages = page_count(total)
    if pages <= 1:
        return
    view.add_item(SelectPageButton(-1, "◀ Prev", page <= 0, row=row))
    view.add_item(SelectPageButton(1, "Next ▶", page >= pages - 1, row=row))


# ------------------------------------------------------------------- views

class ExpiringView(discord.ui.View):
    """A view that dies visibly instead of silently going dead.

    Call `await view.track(interaction)` right after sending. Ephemeral
    messages are editable for 15 minutes via the interaction token, so keep
    timeouts comfortably under that.
    """

    expiry_note = "\n\n*This panel expired — re-open it to continue.*"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message: Optional[discord.Message] = None

    async def track(self, interaction: discord.Interaction):
        """Remember the message this view was sent on, so on_timeout can edit it."""
        try:
            self.message = await interaction.original_response()
        except discord.HTTPException:
            self.message = None  # nothing to grey out later; timeout still fires

    def attach_back(self, parent: "PanelPage", row: int = 4):
        """Wire a standalone editor view into a page tree.

        Lets a view that was written to stand alone be rendered onto the panel
        message itself, instead of spawning a loose ephemeral that dead-ends.
        """
        self._back_button = BackButton(parent, row=row)
        self.add_item(self._back_button)
        return self

    def _restore_back(self):
        """Re-add the Back button after a rebuild that cleared the view."""
        button = getattr(self, "_back_button", None)
        if button is not None and button not in self.children:
            self.add_item(button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        self._refresh_timeout()
        return True

    async def on_timeout(self):
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True
        if self.message is None:
            return
        try:
            content = (self.message.content or "") + self.expiry_note
            await self.message.edit(content=content, view=self)
        except discord.HTTPException:
            pass  # message already gone, or the token expired first


class BackButton(discord.ui.Button):
    """Returns to the page that opened this one, redrawn with fresh data."""

    def __init__(self, parent: "PanelPage", row: int = 4, label: str = "◀ Back"):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=row)
        self.parent_page = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_page.render(interaction)


class CloseButton(discord.ui.Button):
    """Ends the session, leaving a tombstone rather than a dead panel."""

    def __init__(self, row: int = 4, label: str = "✕ Close"):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=row)

    async def callback(self, interaction: discord.Interaction):
        view: PanelPage = self.view  # type: ignore[assignment]
        view.stop()
        embed = discord.Embed(
            description="*Panel closed.*",
            color=discord.Color.dark_grey(),
        )
        await interaction.response.edit_message(embed=embed, view=None)


class PanelPage(ExpiringView):
    """One screen of a panel.

    Subclasses declare their components as usual and override `build_embed`.
    `title` is used to build the breadcrumb, so give every page one.
    """

    back_row = 4
    title = "Panel"

    def __init__(self, parent: Optional["PanelPage"] = None, *,
                 timeout: float = 600, owner_id: Optional[int] = None):
        super().__init__(timeout=timeout)
        self.parent_page = parent
        # Inherited so a nested page doesn't have to be told who opened it.
        self.owner_id = owner_id if owner_id is not None else getattr(parent, "owner_id", None)
        if parent is not None:
            self.add_item(BackButton(parent, row=self.back_row))

    # ------------------------------------------------------------ breadcrumb

    def breadcrumb(self) -> str:
        """`Utility ▸ Automations ▸ Actions` — built by walking to the root."""
        parts, page, seen = [], self, set()
        while page is not None and id(page) not in seen:
            seen.add(id(page))  # a mis-wired parent cycle must not hang the panel
            parts.append(page.title)
            page = getattr(page, "parent_page", None)
        return " ▸ ".join(reversed(parts))

    # ---------------------------------------------------------------- gating

    async def is_allowed(self, interaction: discord.Interaction) -> bool:
        """Override for a permission check. Runs on every interaction, so a
        permission revoked mid-session takes effect immediately."""
        return True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Refresh the whole chain: a parent must not expire while its child is
        # in use, or Back would land on a dead view.
        page = self
        seen = set()
        while page is not None and id(page) not in seen:
            seen.add(id(page))
            page._refresh_timeout()
            page = getattr(page, "parent_page", None)

        if self.owner_id is not None and interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This panel belongs to someone else. Open your own with the command.",
                ephemeral=True,
            )
            return False

        if not await self.is_allowed(interaction):
            await interaction.response.send_message(
                "You no longer have permission to use this panel.", ephemeral=True
            )
            return False
        return True

    # --------------------------------------------------------------- drawing

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        raise NotImplementedError

    async def companion(self, interaction: discord.Interaction):
        """Extra content and embeds to show below this page's own embed.

        Discord allows up to ten embeds on one message, which is what lets a
        live preview sit under the settings that produce it instead of being
        sent as a second message the user then has to dismiss. Returns
        `(content, [embeds])`.
        """
        return None, []

    async def _compose(self, interaction: discord.Interaction,
                       flash: Optional[str] = None) -> discord.Embed:
        embed = await self.build_embed(interaction)
        if flash:
            embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        crumb = self.breadcrumb()
        if crumb and not embed.author:
            embed.set_author(name=crumb)
        return embed

    async def render(self, interaction: discord.Interaction, *,
                     flash: Optional[str] = None):
        """Draw this page onto the message the interaction came from.

        This is the only way a page should ever reach the screen after the
        first one — never `send_message`.
        """
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = False
        embed = await self._compose(interaction, flash)
        content, extras = await self.companion(interaction)
        # `content` is passed unconditionally so that turning a preview off
        # clears the text it left behind.
        embeds = [embed, *extras]
        if interaction.response.is_done():
            # A page that deferred (slow work) still has to land somewhere.
            await interaction.edit_original_response(content=content, embeds=embeds,
                                                     view=self)
            try:
                self.message = await interaction.original_response()
            except discord.HTTPException:
                pass
        else:
            await interaction.response.edit_message(content=content, embeds=embeds,
                                                    view=self)
            self.message = interaction.message

    async def open(self, interaction: discord.Interaction):
        """Send this page as a fresh ephemeral — the one entry point."""
        embed = await self._compose(interaction)
        content, extras = await self.companion(interaction)
        embeds = [embed, *extras]
        if interaction.response.is_done():
            await interaction.followup.send(content=content, embeds=embeds,
                                            view=self, ephemeral=True)
        else:
            await interaction.response.send_message(content=content, embeds=embeds,
                                                    view=self, ephemeral=True)
        await self.track(interaction)

    async def show_view(self, interaction: discord.Interaction, view: ExpiringView,
                        embed: discord.Embed, *, back_row: int = 4):
        """Render a standalone editor view onto the panel, with a way back here."""
        view.attach_back(self, row=back_row)
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = interaction.message


# ------------------------------------------------------------------- modals

async def respond_to_modal(interaction: discord.Interaction,
                           page: Optional[PanelPage], note: str):
    """Close a modal by repainting the page behind it.

    A modal opened from a component carries that component's message, so the
    normal path redraws the panel with the change already visible. The
    ephemeral reply is the fallback for a modal reached any other way — from a
    slash command, say, where there is no message to edit.
    """
    if page is not None and interaction.message is not None:
        try:
            await page.render(interaction, flash=f"✅ {note}")
            return
        except discord.HTTPException as e:
            log.debug(f"Modal repaint failed, falling back to ephemeral: {e}")
    if not interaction.response.is_done():
        await interaction.response.send_message(note, ephemeral=True)


# ------------------------------------------------------------ confirmations

class ConfirmPage(PanelPage):
    """A destructive-action confirmation, rendered in place.

    `on_confirm` runs the action and returns the flash note to show on the
    page underneath; the panel then returns there. Nothing is spawned.
    """

    title = "Confirm"

    def __init__(self, parent: PanelPage, *, prompt: str, detail: str = "",
                 confirm_label: str = "Delete", danger: bool = True):
        super().__init__(parent)
        self.prompt = prompt
        self.detail = detail
        button = discord.ui.Button(
            label=confirm_label,
            style=discord.ButtonStyle.danger if danger else discord.ButtonStyle.success,
            row=0,
        )
        button.callback = self._confirm
        self.add_item(button)

    async def on_confirm(self, interaction: discord.Interaction) -> str:
        raise NotImplementedError

    async def _confirm(self, interaction: discord.Interaction):
        note = await self.on_confirm(interaction)
        await self.parent_page.render(interaction, flash=f"✅ {note}")

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        return discord.Embed(
            title=self.prompt,
            description=self.detail or "This cannot be undone.",
            color=discord.Color.red(),
        )
