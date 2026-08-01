"""Customs Match — unlock the custom match queue for a single match.

Calls CustomMatch.start_queue directly; the custommatch cog is not modified.
Its own queue_timeout_check already closes idle queues after the configured
window, so this item only has to open one and observe it.
"""

import logging

import discord
from discord import ui

from .base import ClaimedFlowView, ShopItem

logger = logging.getLogger('cogs.economy.items.customs_match')


class CustomsMatchItem(ShopItem):
    key = "customs_match"
    name = "Customs Match"

    def _custom_match_cog(self):
        return self.cog.bot.get_cog("CustomMatch")

    async def can_buy(self, user: discord.Member) -> tuple:
        allowed, reason = await super().can_buy(user)
        if not allowed:
            return allowed, reason
        if self._custom_match_cog() is None:
            return False, "The custom match system is not available right now."
        return True, ""

    async def _queueable_games(self) -> list:
        """Games configured with a queue channel — the only ones we can open."""
        try:
            from cogs.custommatch.database import DatabaseHelper
            games = await DatabaseHelper.get_all_games()
        except Exception as e:
            logger.warning(f"Could not list custom match games: {e}")
            return []
        return [g for g in games if getattr(g, "queue_channel_id", None)]

    async def activate(self, interaction: discord.Interaction, inv_row):
        games = await self._queueable_games()
        if not games:
            await self.release(inv_row)
            await interaction.response.send_message(
                "No custom match games currently have a queue channel set up. "
                "Your item has not been spent.",
                ephemeral=True,
            )
            return

        view = MatchUnlockView(self, inv_row, games)
        await interaction.response.send_message(
            embed=view.build_embed(), view=view, ephemeral=True
        )

    async def open_queue(self, interaction: discord.Interaction, view, game):
        inv_row = view.inv_row
        match_cog = self._custom_match_cog()
        if match_cog is None:
            await view.release()
            await interaction.response.edit_message(
                content="The custom match system went away. Nothing was spent.",
                embed=None, view=None,
            )
            return

        channel = self.cog.bot.get_channel(
            self.cog.config.get_int("ch_match_queue", 0) or game.queue_channel_id
        )
        if channel is None:
            await view.release()
            await interaction.response.edit_message(
                content="The queue channel could not be found. Nothing was spent.",
                embed=None, view=None,
            )
            return

        try:
            queue_id = await match_cog.start_queue(channel, game)
        except Exception as e:
            logger.error(f"start_queue failed: {e}", exc_info=True)
            await view.release()
            await interaction.response.edit_message(
                content=f"The queue could not be opened (`{e}`). Nothing was spent.",
                embed=None, view=None,
            )
            return

        hours = self.cog.config.get_int("match_queue_hours", 3)
        await self.cog.db.execute(
            "INSERT INTO match_unlocks (user_id, queue_id, game_id, channel_id, "
            "opened_ts, expires_ts) VALUES (?, ?, ?, ?, ?, ?)",
            (interaction.user.id, queue_id, game.game_id, channel.id,
             self.now(), self.now() + hours * 3600),
        )
        await self.consume(inv_row)
        view.settle()

        await interaction.response.edit_message(
            content=f"The **{game.name}** queue is open in {channel.mention}. "
                    f"It closes when the match starts, or in {hours} hours.",
            embed=None, view=None,
        )

    async def maintenance(self, cog):
        """Mark unlocks closed once their queue is gone or the window lapsed."""
        rows = await cog.db.fetchall(
            "SELECT * FROM match_unlocks WHERE closed = 0"
        )
        if not rows:
            return
        match_cog = self._custom_match_cog()
        live = set(getattr(match_cog, "queues", {}) or {}) if match_cog else set()

        for row in rows:
            gone = row["queue_id"] not in live
            lapsed = row["expires_ts"] <= self.now()
            if gone or lapsed:
                await cog.db.execute(
                    "UPDATE match_unlocks SET closed = 1 WHERE id = ?", (row["id"],)
                )


class MatchUnlockView(ClaimedFlowView):
    def __init__(self, item: CustomsMatchItem, inv_row, games):
        super().__init__(item, inv_row)
        self.games = {g.game_id: g for g in games}
        self.selected = None

        options = [
            discord.SelectOption(
                label=g.name[:100],
                value=str(g.game_id),
                description=f"{g.player_count} players",
            )
            for g in games[:25]
        ]
        self.picker.options = options
        self._sync()

    def build_embed(self) -> discord.Embed:
        hours = self.item.cog.config.get_int("match_queue_hours", 3)
        embed = discord.Embed(
            title="Customs Match",
            description=f"Choose a game. The queue opens immediately and closes "
                        f"once the match starts, or after {hours} hours.",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Game",
            value=self.selected.name if self.selected else "*Not chosen*",
        )
        embed.set_footer(
            text="Non-refundable — if the queue never fills, the item is still spent."
        )
        return embed

    def _sync(self):
        self.open_button.disabled = self.selected is None

    @ui.select(placeholder="Choose a game...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        self.selected = self.games.get(int(select.values[0]))
        self._sync()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @ui.button(label="Open Queue", style=discord.ButtonStyle.success, row=1)
    async def open_button(self, interaction: discord.Interaction, button: ui.Button):
        if self.selected is None:
            await interaction.response.send_message("Pick a game first.", ephemeral=True)
            return
        await self.item.open_queue(interaction, self, self.selected)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await self.cancel_flow(interaction)
