"""Views for the level card and the server leaderboard."""

import logging
import unicodedata

import discord
from discord import ui

from .leveling import level_progress
from .render import commas

logger = logging.getLogger('cogs.economy.views_level')

PAGE_SIZE = 20

# Shared with VibeyMusic's queue view so paging looks the same across both
# bots. They live in the Emojiland guild, which both bots are in.
EMOJI_FIRST = "<:firstpage:1527940889446715525>"
EMOJI_PREV = "<:previouspage:1527940716393922590>"
EMOJI_NEXT = "<:nextpage:1527940602837078078>"
EMOJI_LAST = "<:lastpage:1527940822778122299>"

# Longest display name kept before it is clipped. The whole row has to fit one
# line of a mobile code block (~40 monospace columns), and everything else on
# the row costs about 20 of them.
NAME_WIDTH = 16


# ZWJ and the variation selectors join glyphs rather than adding columns.
_ZWJ = "\u200D"
_ZERO_WIDTH = (_ZWJ, "\uFE0F", "\uFE0E")


def display_width(text: str) -> int:
    """Columns `text` occupies in a monospace block, not characters.

    CJK and emoji are drawn double width, so measuring names with len() lets a
    name like ｍｏｏｎ push the columns to its right out of line.
    """
    width = 0
    joined = False
    for ch in text:
        if joined:
            # Whatever follows a ZWJ merges into the glyph before it, so 👨‍👩‍👧
            # occupies one emoji's worth of columns rather than three.
            joined = False
            continue
        if ch == _ZWJ:
            joined = True
            continue
        # Skin tone modifiers are East_Asian_Width W but add no columns.
        if "\U0001F3FB" <= ch <= "\U0001F3FF":
            continue
        if ch in _ZERO_WIDTH or unicodedata.combining(ch):
            continue
        width += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return width


def pad(text: str, width: int, right: bool = False) -> str:
    """Pad to `width` columns. str.ljust would count characters instead."""
    spaces = " " * max(0, width - display_width(text))
    return spaces + text if right else text + spaces


def clip_name(name: str, width: int = NAME_WIDTH) -> str:
    """Trim a display name to `width` columns for the monospace table.

    NFKC folds the styled-alphabet tricks people put in display names — 𝒜𝓁𝑒𝓍,
    𝕭𝖔𝖑𝖉, ｍｏｏｎ — down to plain letters. Those characters are missing from the
    client's monospace font, so it silently falls back to a proportional one
    and the glyphs come out wider than the single column Unicode claims,
    dragging every column after them out of line.

    Backticks are swapped out because a name containing ``` would otherwise
    close the code block and spill the rest of the table into the message.
    """
    name = unicodedata.normalize("NFKC", name).replace("`", "'")
    if display_width(name) <= width:
        return name
    out = ""
    for ch in name:
        if display_width(out + ch) > width - 1:
            break
        out += ch
    return out + "…"


class LevelCardView(ui.View):
    """Sits under the rendered level card."""

    def __init__(self, cog, viewer_id: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.viewer_id = viewer_id

    @ui.button(label="Leaderboard", style=discord.ButtonStyle.secondary)
    async def leaderboard(self, interaction: discord.Interaction, button: ui.Button):
        # Ephemeral: the card itself is public, so a non-ephemeral reply here
        # would let anyone flood the channel by clicking someone else's card.
        # It also means each viewer gets their own page position.
        await interaction.response.defer(thinking=True, ephemeral=True)
        view = LeaderboardView(self.cog, interaction.guild, interaction.user.id)
        await view.jump_to_viewer()
        embed = await view.build_embed()
        view.update_buttons()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class LeaderboardView(ui.View):
    """Paginated XP leaderboard, 20 members per page."""

    def __init__(self, cog, guild: discord.Guild, viewer_id: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild
        self.viewer_id = viewer_id
        self.page = 0
        self.total_pages = 1

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only the person who opened it can page it."""
        if interaction.user.id != self.viewer_id:
            await interaction.response.send_message(
                "Run `/eco_leaderboard` to get your own copy.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    async def jump_to_viewer(self):
        """Open on the page containing whoever pressed the button."""
        total = await self.cog.leveling.leaderboard_size()
        self.total_pages = max(1, -(-total // PAGE_SIZE))
        rank, _ = await self.cog.leveling.get_rank(self.viewer_id)
        if rank:
            self.page = min(self.total_pages - 1, (rank - 1) // PAGE_SIZE)

    async def build_embed(self) -> discord.Embed:
        total = await self.cog.leveling.leaderboard_size()
        self.total_pages = max(1, -(-total // PAGE_SIZE))
        self.page = max(0, min(self.page, self.total_pages - 1))

        rows = await self.cog.leveling.get_leaderboard(
            limit=PAGE_SIZE, offset=self.page * PAGE_SIZE
        )

        embed = discord.Embed(
            title=f"{self.guild.name} — Level Leaderboard",
            color=discord.Color.blurple(),
        )

        if not rows:
            embed.description = "*No one has earned XP yet.*"
            embed.set_footer(text="Page 1 of 1")
            return embed

        entries = []
        for index, row in enumerate(rows):
            rank = self.page * PAGE_SIZE + index + 1
            member = self.guild.get_member(row["user_id"])
            name = member.display_name if member else f"Unknown ({row['user_id']})"
            level, _, _, _ = level_progress(row["xp"])
            entries.append((rank, clip_name(name), level, commas(row["xp"]),
                            row["user_id"] == self.viewer_id))

        # Sized to the widest entry on this page rather than to fixed maxima,
        # so a page of short names does not carry a gutter of dead space. The
        # header labels set the floor for each column.
        rank_w = max(len(str(e[0])) for e in entries)
        name_w = max(6, max(display_width(e[1]) for e in entries))
        lvl_w = max(3, max(len(str(e[2])) for e in entries))
        xp_w = max(2, max(len(e[3]) for e in entries))

        lines = [
            f"{'':{rank_w + 3}}{pad('MEMBER', name_w)}  "
            f"{pad('LVL', lvl_w, right=True)}  {pad('XP', xp_w, right=True)}"
        ]
        for rank, name, level, xp, is_viewer in entries:
            # No bold inside a code block, so the viewer's own row is flagged
            # with a marker in the gutter instead.
            marker = "›" if is_viewer else " "
            lines.append(
                f"{marker}{rank:>{rank_w}}. {pad(name, name_w)}  "
                f"{pad(str(level), lvl_w, right=True)}  {pad(xp, xp_w, right=True)}"
            )

        # A code block is the only way to hold the columns in line: embed
        # descriptions render proportionally, where padding with spaces drifts.
        embed.description = "```\n" + "\n".join(lines) + "\n```"

        rank, total_ranked = await self.cog.leveling.get_rank(self.viewer_id)
        footer = f"Page {self.page + 1} of {self.total_pages} · {commas(total_ranked)} ranked"
        if rank:
            footer += f" · You are #{rank}"
        embed.set_footer(text=footer)
        return embed

    def update_buttons(self):
        self.first.disabled = self.page == 0
        self.prev.disabled = self.page == 0
        self.next.disabled = self.page >= self.total_pages - 1
        self.last.disabled = self.page >= self.total_pages - 1

    async def _refresh(self, interaction: discord.Interaction):
        embed = await self.build_embed()
        self.update_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    @ui.button(emoji=EMOJI_FIRST, style=discord.ButtonStyle.secondary)
    async def first(self, interaction: discord.Interaction, button: ui.Button):
        self.page = 0
        await self._refresh(interaction)

    @ui.button(emoji=EMOJI_PREV, style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, button: ui.Button):
        self.page -= 1
        await self._refresh(interaction)

    @ui.button(emoji=EMOJI_NEXT, style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: ui.Button):
        self.page += 1
        await self._refresh(interaction)

    @ui.button(emoji=EMOJI_LAST, style=discord.ButtonStyle.secondary)
    async def last(self, interaction: discord.Interaction, button: ui.Button):
        self.page = self.total_pages - 1
        await self._refresh(interaction)
