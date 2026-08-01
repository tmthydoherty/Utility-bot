"""Views for the level card and the server leaderboard."""

import logging

import discord
from discord import ui

from .leveling import level_progress
from .render import commas

logger = logging.getLogger('cogs.economy.views_level')

PAGE_SIZE = 20


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

        lines = []
        for index, row in enumerate(rows):
            rank = self.page * PAGE_SIZE + index + 1
            member = self.guild.get_member(row["user_id"])
            name = member.display_name if member else f"Unknown ({row['user_id']})"
            if len(name) > 24:
                name = name[:23] + "…"

            level, _, _, _ = level_progress(row["xp"])
            marker = "**" if row["user_id"] == self.viewer_id else ""
            lines.append(
                f"`{rank:>3}.` {marker}{discord.utils.escape_markdown(name)}{marker}"
                f" — Level **{level}** · {commas(row['xp'])} XP"
            )

        embed.description = "\n".join(lines)

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

    @ui.button(label="First", style=discord.ButtonStyle.secondary)
    async def first(self, interaction: discord.Interaction, button: ui.Button):
        self.page = 0
        await self._refresh(interaction)

    @ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, button: ui.Button):
        self.page -= 1
        await self._refresh(interaction)

    @ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: ui.Button):
        self.page += 1
        await self._refresh(interaction)

    @ui.button(label="Last", style=discord.ButtonStyle.secondary)
    async def last(self, interaction: discord.Interaction, button: ui.Button):
        self.page = self.total_pages - 1
        await self._refresh(interaction)
