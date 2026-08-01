"""Mystery Box — one weighted roll. Some outcomes are prizes, some are curses.

Weights come entirely from the panel, so the box can be retuned without a code
change and new outcomes can be switched off rather than removed.
"""

import logging
import random

import discord
from discord import ui

from .base import ShopItem
from ..config import BOX_OUTCOMES
from ..render import commas

logger = logging.getLogger('cogs.economy.items.mystery_box')

FLAVOUR = {
    "vibes_role": "The rarest pull in the box. It is yours until someone else rolls it.",
    "curse_wipe": "Bank it. Play it on yourself, or be a hero.",
    "nickname_hijack": "Someone is about to have a bad day.",
    "points_small": "Not nothing.",
    "points_big": "The box paid out.",
    "clown_mode": "The box has judged you.",
    "slowmo": "Take your time. You have no choice.",
    "spongebob": "yOu BrOuGhT ThIs On YoUrSeLf.",
}


class MysteryBoxItem(ShopItem):
    key = "mystery_box"
    name = "Mystery Box"

    async def activate(self, interaction: discord.Interaction, inv_row):
        weights = self.cog.config.box_weights()
        if not weights:
            await self.release(inv_row)
            await interaction.response.send_message(
                "The Mystery Box has no outcomes enabled. Tell an admin. "
                "Your box has not been opened.",
                ephemeral=True,
            )
            return

        outcome = random.choices(
            list(weights.keys()), weights=list(weights.values()), k=1
        )[0]

        await self.consume(inv_row)
        embed = await self._resolve(interaction, outcome)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ------------------------------------------------------------- outcomes

    async def _resolve(self, interaction: discord.Interaction, outcome: str) -> discord.Embed:
        meta = BOX_OUTCOMES.get(outcome, {})
        is_curse = meta.get("kind") == "curse"

        embed = discord.Embed(
            title=f"Mystery Box — {meta.get('name', outcome)}",
            description=FLAVOUR.get(outcome, ""),
            color=discord.Color.red() if is_curse else discord.Color.green(),
        )

        handler = getattr(self, f"_give_{outcome}", None)
        if handler is None:
            embed.add_field(name="Result", value="Nothing happened. Consider it a refund of luck.")
            return embed

        detail = await handler(interaction)
        if detail:
            embed.add_field(name="Result", value=detail, inline=False)
        return embed

    async def _give_points_small(self, interaction) -> str:
        amount = self.cog.config.get_int("box_points_small", 250)
        await self.cog.db.adjust_points(
            interaction.user.id, amount, "mystery_box", {"outcome": "points_small"}
        )
        return f"**+{commas(amount)}** Points."

    async def _give_points_big(self, interaction) -> str:
        amount = self.cog.config.get_int("box_points_big", 1500)
        await self.cog.db.adjust_points(
            interaction.user.id, amount, "mystery_box", {"outcome": "points_big"}
        )
        return f"**+{commas(amount)}** Points."

    async def _give_curse_wipe(self, interaction) -> str:
        return await self._grant_inventory(interaction, "curse_wipe")

    async def _give_nickname_hijack(self, interaction) -> str:
        return await self._grant_inventory(interaction, "nickname_hijack")

    async def _grant_inventory(self, interaction, item_key: str) -> str:
        from ..config import BOX_ITEMS
        days = (BOX_ITEMS.get(item_key) or {}).get("shelf_days")
        expires = self.now() + int(days) * 86400 if days else None
        await self.cog.db.add_inventory(
            interaction.user.id, item_key, 0, expires_ts=expires,
            payload={"source": "mystery_box"},
        )
        if expires:
            return (f"Added to your items. Use it within **{days} days** "
                    f"(expires <t:{expires}:R>).")
        return "Added to your items."

    async def _give_vibes_role(self, interaction) -> str:
        role_id = self.cog.config.get_int("role_vibes", 0)
        role = interaction.guild.get_role(role_id) if role_id else None
        if role is None:
            # Nobody should lose a 2% pull to a misconfiguration.
            amount = self.cog.config.get_int("box_points_big", 1500)
            await self.cog.db.adjust_points(
                interaction.user.id, amount, "mystery_box",
                {"outcome": "vibes_role_unconfigured"},
            )
            return (f"The Vibes Role is not set up, so the box paid out "
                    f"**{commas(amount)}** Points instead. Tell an admin.")

        previous = await self.cog.db.fetchone(
            "SELECT user_id FROM vibes_role ORDER BY claimed_ts DESC LIMIT 1"
        )

        try:
            await interaction.user.add_roles(role, reason="Economy: Vibes Role pulled")
        except discord.HTTPException as e:
            logger.warning(f"Could not grant Vibes Role: {e}")
            return f"The role could not be assigned (`{e}`). Tell an admin."

        if previous and previous["user_id"] != interaction.user.id:
            old = interaction.guild.get_member(previous["user_id"])
            if old and role in old.roles:
                try:
                    await old.remove_roles(role, reason="Economy: Vibes Role stolen")
                except discord.HTTPException:
                    pass
            await self.dm(
                previous["user_id"],
                f"**{interaction.user.display_name}** just pulled the Vibes Role "
                f"out of a Mystery Box. It is no longer yours.",
            )

        await self.cog.db.execute("DELETE FROM vibes_role")
        await self.cog.db.execute(
            "INSERT INTO vibes_role (user_id, claimed_ts) VALUES (?, ?)",
            (interaction.user.id, self.now()),
        )
        stolen = f" Taken from <@{previous['user_id']}>." if previous and \
            previous["user_id"] != interaction.user.id else ""
        return f"{role.mention} is yours.{stolen}"

    async def _give_clown_mode(self, interaction) -> str:
        hours = self.cog.config.get_int("clown_hours", 24)
        await self.cog.effects.apply(
            "clown_mode", interaction.user.id, interaction.user.id, hours * 3600
        )
        return f"Every message you send gets a clown for the next **{hours} hours**."

    async def _give_slowmo(self, interaction) -> str:
        minutes = self.cog.config.get_int("slowmo_minutes", 60)
        interval = self.cog.config.get_int("slowmo_interval_minutes", 5)
        await self.cog.effects.apply(
            "slowmo", interaction.user.id, interaction.user.id, minutes * 60
        )
        return (f"One message every **{interval} minutes** for the next "
                f"**{minutes} minutes**. Anything faster gets deleted.")

    async def _give_spongebob(self, interaction) -> str:
        days = self.cog.config.get_int("spongebob_days", 7)
        chance = self.cog.config.get_int("spongebob_chance", 5)
        await self.cog.effects.apply(
            "spongebob", interaction.user.id, interaction.user.id, days * 86400
        )
        return (f"For **{days} days**, {chance}% of your messages get echoed back "
                f"at you in aLtErNaTiNg CaPs.")
