"""Mystery Box prizes that live in inventory: Nickname Hijack and Curse Wipe.

Both carry a 90-day shelf life with DM reminders at 7 days and 1 day, handled
generically by the item registry.
"""

import logging

import discord
from discord import ui

from .base import ClaimedFlowView, ShopItem

logger = logging.getLogger('cogs.economy.items.curses')

CURSE_LABELS = {
    "clown_mode": "Clown Mode",
    "slowmo": "Slowmo",
    "spongebob": "SpongeBob Case",
    "nickname_hijack": "Nickname Hijack",
}


class NicknameHijackItem(ShopItem):
    key = "nickname_hijack"
    name = "Nickname Hijack"

    async def activate(self, interaction: discord.Interaction, inv_row):
        view = HijackSetupView(self, inv_row)
        await interaction.response.send_message(
            embed=view.build_embed(), view=view, ephemeral=True
        )

    async def apply_to(self, interaction: discord.Interaction, view,
                       target: discord.Member, nickname: str):
        inv_row = view.inv_row

        if target.id == self.cog.bot.user.id:
            await interaction.response.send_message(
                "Not the bot.", ephemeral=True
            )
            return

        me = interaction.guild.me
        # Discord refuses nickname edits on anyone at or above the bot's top role.
        if target.top_role >= me.top_role or target.id == interaction.guild.owner_id:
            await interaction.response.send_message(
                "The bot cannot rename that member — they outrank it. "
                "Nothing was spent; pick someone else.",
                ephemeral=True,
            )
            return

        # Only one hijack can hold a nickname at a time; a second would fight
        # the first and burn the item for nothing.
        existing = await self.cog.db.get_effects(
            target_id=target.id, effect_key="nickname_hijack"
        )
        if existing:
            await interaction.response.send_message(
                f"{target.mention} is already hijacked until "
                f"<t:{existing[0]['expires_ts']}:R>. Your item has not been spent.",
                ephemeral=True,
            )
            return

        hours = self.cog.config.get_int("nickname_hijack_hours", 24)
        await self.cog.effects.apply_nickname_hijack(
            interaction.guild, interaction.user.id, target, nickname,
            hours * 3600, inventory_id=inv_row["id"],
        )
        await self.consume(inv_row)
        view.settle()

        await self.dm(
            target.id,
            f"Someone spent a **Nickname Hijack** on you. You are "
            f"**{nickname}** for the next {hours} hours, and the bot will put it "
            f"back if you change it.",
        )
        await interaction.response.edit_message(
            content=f"{target.mention} is now **{nickname}** for {hours} hours.",
            embed=None, view=None,
        )


class HijackSetupView(ClaimedFlowView):
    def __init__(self, item: NicknameHijackItem, inv_row):
        super().__init__(item, inv_row)
        self.target = None
        self.nickname = None
        self._sync()

    def build_embed(self) -> discord.Embed:
        hours = self.item.cog.config.get_int("nickname_hijack_hours", 24)
        embed = discord.Embed(
            title="Nickname Hijack",
            description=f"Rename one person for {hours} hours. If they change it "
                        f"back, the bot changes it right back again.",
            color=discord.Color.dark_purple(),
        )
        embed.add_field(
            name="Target",
            value=self.target.mention if self.target else "*Not chosen*",
            inline=False,
        )
        embed.add_field(
            name="Nickname",
            value=f"**{self.nickname}**" if self.nickname else "*Not chosen*",
            inline=False,
        )
        return embed

    def _sync(self):
        self.apply_button.disabled = not (self.target and self.nickname)

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

    @ui.button(label="Set Nickname", style=discord.ButtonStyle.secondary, row=1)
    async def set_nick(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(NicknameModal(self))

    @ui.button(label="Apply", style=discord.ButtonStyle.danger, row=1)
    async def apply_button(self, interaction: discord.Interaction, button: ui.Button):
        member = interaction.guild.get_member(self.target.id)
        if member is None:
            await interaction.response.send_message(
                "That member is no longer in the server.", ephemeral=True
            )
            return
        await self.item.apply_to(interaction, self, member, self.nickname)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=2)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await self.cancel_flow(interaction)


class NicknameModal(ui.Modal, title="Choose a Nickname"):
    nickname = ui.TextInput(label="Nickname", max_length=32)

    def __init__(self, view: HijackSetupView):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        value = str(self.nickname.value).strip()
        if not value:
            await interaction.response.send_message(
                "Give them an actual nickname.", ephemeral=True
            )
            return
        self.view.nickname = value
        self.view._sync()
        await interaction.response.edit_message(
            embed=self.view.build_embed(), view=self.view
        )


class CurseWipeItem(ShopItem):
    key = "curse_wipe"
    name = "Curse Wipe"

    async def activate(self, interaction: discord.Interaction, inv_row):
        curses = await self.cog.effects.active_curses()
        if not curses:
            await self.release(inv_row)
            await interaction.response.send_message(
                "There are no active curses to wipe. Hold onto this one.",
                ephemeral=True,
            )
            return

        view = CurseWipeView(self, inv_row, curses, interaction.guild)
        await interaction.response.send_message(
            embed=view.build_embed(), view=view, ephemeral=True
        )

    async def wipe(self, interaction: discord.Interaction, view, effect_row):
        label = CURSE_LABELS.get(effect_row["effect_key"], effect_row["effect_key"])
        await self.cog.effects.clear_effect(effect_row)
        await self.consume(view.inv_row)
        view.settle()

        victim_id = effect_row["target_id"]
        if victim_id != interaction.user.id:
            await self.dm(
                victim_id,
                f"**{interaction.user.display_name}** spent a Curse Wipe to lift "
                f"your **{label}**.",
            )

        await interaction.response.edit_message(
            content=f"**{label}** has been lifted from <@{victim_id}>.",
            embed=None, view=None,
        )


class CurseWipeView(ClaimedFlowView):
    def __init__(self, item: CurseWipeItem, inv_row, curses, guild):
        super().__init__(item, inv_row)
        self.curses = {str(c["id"]): c for c in curses}
        self.selected = None

        options = []
        for row in curses[:25]:
            member = guild.get_member(row["target_id"])
            name = member.display_name if member else f"User {row['target_id']}"
            label = CURSE_LABELS.get(row["effect_key"], row["effect_key"])
            options.append(
                discord.SelectOption(
                    label=f"{label} — {name}"[:100],
                    value=str(row["id"]),
                    description=f"Ends in {max(0, (row['expires_ts'] - self.item.now()) // 3600)}h",
                )
            )
        self.picker.options = options
        self._sync()

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Curse Wipe",
            description="Lift one active curse. Yours, or anyone else's.",
            color=discord.Color.green(),
        )
        if self.selected:
            label = CURSE_LABELS.get(
                self.selected["effect_key"], self.selected["effect_key"]
            )
            embed.add_field(
                name="Selected",
                value=f"**{label}** on <@{self.selected['target_id']}>",
            )
        return embed

    def _sync(self):
        self.wipe_button.disabled = self.selected is None

    @ui.select(placeholder="Choose a curse to lift...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        self.selected = self.curses.get(select.values[0])
        self._sync()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @ui.button(label="Wipe It", style=discord.ButtonStyle.success, row=1)
    async def wipe_button(self, interaction: discord.Interaction, button: ui.Button):
        if self.selected is None:
            await interaction.response.send_message("Pick a curse first.", ephemeral=True)
            return
        # The curse may have expired while this view sat open.
        still_live = await self.item.cog.db.fetchone(
            "SELECT * FROM active_effects WHERE id = ?", (self.selected["id"],)
        )
        if still_live is None:
            await self.cancel_flow(
                interaction,
                "That curse already ended. Your Curse Wipe is untouched.",
            )
            return
        await self.item.wipe(interaction, self, still_live)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await self.cancel_flow(interaction)
