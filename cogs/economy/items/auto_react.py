"""Auto React — for 24h the bot reacts to every message a target sends."""

import logging
import re

import discord
from discord import ui

from .base import ClaimedFlowView, ShopItem

logger = logging.getLogger('cogs.economy.items.auto_react')

CUSTOM_EMOJI = re.compile(r"<(a?):([A-Za-z0-9_]+):(\d+)>")


class AutoReactItem(ShopItem):
    key = "auto_react"
    name = "Auto React"

    async def activate(self, interaction: discord.Interaction, inv_row):
        view = AutoReactSetupView(self, inv_row, interaction.user)
        await interaction.response.send_message(
            embed=view.build_embed(), view=view, ephemeral=True
        )

    async def existing_on(self, target_id: int):
        """The live Auto React on a target, if any.

        Only one can apply at a time — the message handler dedupes by effect
        key — so a second purchase aimed at the same person would be burned
        for nothing.
        """
        rows = await self.cog.db.get_effects(
            target_id=target_id, effect_key="auto_react"
        )
        return rows[0] if rows else None

    async def start(self, interaction: discord.Interaction, view,
                    target: discord.Member, emoji: str):
        inv_row = view.inv_row

        clash = await self.existing_on(target.id)
        if clash is not None:
            await view.release()
            await interaction.response.edit_message(
                content=f"{target.mention} already has an Auto React running "
                        f"until <t:{clash['expires_ts']}:R>. Only one can apply "
                        f"at a time, so your item has **not** been spent — pick "
                        f"someone else or come back later.",
                embed=None, view=None,
            )
            view.stop()
            return

        hours = self.cog.config.get_int("auto_react_hours", 24)
        await self.cog.effects.apply(
            "auto_react", interaction.user.id, target.id, hours * 3600,
            data={"emoji": emoji}, inventory_id=inv_row["id"],
        )
        await self.consume(inv_row)
        view.settle()

        await interaction.response.edit_message(
            content=f"{emoji} is now landing on every message "
                    f"{target.mention} sends for the next {hours} hours.",
            embed=None, view=None,
        )


class AutoReactSetupView(ClaimedFlowView):
    def __init__(self, item: AutoReactItem, inv_row, buyer):
        super().__init__(item, inv_row)
        self.buyer = buyer
        self.target = None
        self.emoji_value = None
        self._sync()

    def build_embed(self) -> discord.Embed:
        hours = self.item.cog.config.get_int("auto_react_hours", 24)
        embed = discord.Embed(
            title="Auto React",
            description=f"Pick who gets reacted to and which emoji lands on "
                        f"their messages for the next {hours} hours.",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Target",
            value=self.target.mention if self.target else "*Not chosen*",
            inline=False,
        )
        embed.add_field(
            name="Emoji",
            value=self.emoji_value or "*Not chosen*",
            inline=False,
        )
        embed.set_footer(text="This is spent the moment you press Start.")
        return embed

    def _sync(self):
        self.start_button.disabled = not (self.target and self.emoji_value)

    @ui.select(cls=ui.UserSelect, placeholder="Choose a target...", row=0)
    async def pick_target(self, interaction: discord.Interaction, select: ui.UserSelect):
        chosen = select.values[0]
        if chosen.bot:
            await interaction.response.send_message(
                "You cannot target a bot.", ephemeral=True
            )
            return
        self.target = chosen
        self._sync()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @ui.button(label="Set Emoji", style=discord.ButtonStyle.secondary, row=1)
    async def set_emoji(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(EmojiModal(self))

    @ui.button(label="Start", style=discord.ButtonStyle.success, row=1)
    async def start_button(self, interaction: discord.Interaction, button: ui.Button):
        if not (self.target and self.emoji_value):
            await interaction.response.send_message(
                "Choose a target and an emoji first.", ephemeral=True
            )
            return
        member = interaction.guild.get_member(self.target.id)
        if member is None:
            await interaction.response.send_message(
                "That member is no longer in the server.", ephemeral=True
            )
            return
        await self.item.start(interaction, self, member, self.emoji_value)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await self.cancel_flow(interaction)


class EmojiModal(ui.Modal, title="Choose an Emoji"):
    emoji_input = ui.TextInput(
        label="Emoji",
        placeholder="Paste any emoji the bot can use",
        max_length=64,
    )

    def __init__(self, view: AutoReactSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.emoji_input.value).strip()

        match = CUSTOM_EMOJI.fullmatch(raw)
        if match:
            emoji_id = int(match.group(3))
            # The bot can only react with custom emoji from servers it is in.
            if self.view.item.cog.bot.get_emoji(emoji_id) is None:
                await interaction.response.send_message(
                    "The bot cannot use that custom emoji — it is not in a "
                    "server the bot shares. Try a standard emoji or one from "
                    "this server.",
                    ephemeral=True,
                )
                return
        elif len(raw) > 8 or not raw:
            await interaction.response.send_message(
                "That does not look like a single emoji.", ephemeral=True
            )
            return

        self.view.emoji_value = raw
        self.view._sync()
        await interaction.response.edit_message(embed=self.view.build_embed(), view=self.view)
