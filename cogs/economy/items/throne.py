"""The Throne — a 1-of-1 role seized by outspending whoever holds it.

Resolved at purchase rather than sitting in inventory: the price *is* the bid,
so holding a Throne voucher would make no sense.
"""

import asyncio
import logging

import discord
from discord import ui

from .base import ShopItem
from ..render import commas

logger = logging.getLogger('cogs.economy.items.throne')


class ThroneItem(ShopItem):
    key = "throne"
    name = "The Throne"
    immediate = True

    def __init__(self, cog):
        super().__init__(cog)
        # Serialises seizures. Without it, concurrent bids all validate against
        # the same minimum, all pay, and all end up holding the role.
        self._lock = asyncio.Lock()

    # ---------------------------------------------------------------- state

    async def current_holder(self):
        return await self.cog.db.fetchone(
            "SELECT * FROM throne_history ORDER BY price_paid DESC, ts DESC LIMIT 1"
        )

    async def minimum_bid(self) -> int:
        holder = await self.current_holder()
        base = self.cog.config.price(self.key)
        if holder is None:
            return base
        return holder["price_paid"] + 1

    async def price(self, user_id: int) -> int:
        return await self.minimum_bid()

    async def can_buy(self, user: discord.Member) -> tuple:
        allowed, reason = await super().can_buy(user)
        if not allowed:
            return allowed, reason
        if not self.cog.config.get_int("role_throne", 0):
            return False, "The Throne role has not been configured yet."
        holder = await self.current_holder()
        if holder and holder["user_id"] == user.id:
            return False, "You already sit on the Throne."
        return True, ""

    # ---------------------------------------------------------------- flow

    async def open_bid(self, interaction: discord.Interaction):
        # Courtesy check only, so nobody types a bid they cannot afford. The
        # binding validation happens inside the lock in _seize_locked.
        minimum = await self.minimum_bid()
        balance = await self.cog.db.get_balance(interaction.user.id)
        if balance < minimum:
            await interaction.response.send_message(
                f"You need at least **{commas(minimum)}** Points to seize the "
                f"Throne. You have {commas(balance)}.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(ThroneBidModal(self, minimum, balance))

    async def seize(self, interaction: discord.Interaction, bid: int):
        # Everything from re-reading the minimum to recording the new holder
        # runs under one lock, so two simultaneous bids cannot both win.
        async with self._lock:
            await self._seize_locked(interaction, bid)

    async def _seize_locked(self, interaction: discord.Interaction, bid: int):
        minimum = await self.minimum_bid()
        if bid < minimum:
            await interaction.response.send_message(
                f"Someone outbid you while you were typing — the Throne now "
                f"costs at least **{commas(minimum)}** Points. Nothing was spent.",
                ephemeral=True,
            )
            return

        role_id = self.cog.config.get_int("role_throne", 0)
        role = interaction.guild.get_role(role_id)
        if role is None:
            await interaction.response.send_message(
                "The Throne role no longer exists. Tell an admin.", ephemeral=True
            )
            return

        if not await self.cog.earning.spend(
            interaction.user.id, bid, "shop:throne", {"bid": bid}
        ):
            await interaction.response.send_message(
                "You cannot afford that bid.", ephemeral=True
            )
            return

        previous = await self.current_holder()

        # Grant before revoking so the role is never briefly unheld.
        try:
            await interaction.user.add_roles(role, reason="Economy: seized the Throne")
        except discord.HTTPException as e:
            await self.cog.earning.refund(interaction.user.id, bid, "throne")
            await interaction.response.send_message(
                f"Could not assign the Throne role ({e}). You were refunded.",
                ephemeral=True,
            )
            return

        if previous and previous["user_id"] != interaction.user.id:
            old = interaction.guild.get_member(previous["user_id"])
            if old and role in old.roles:
                try:
                    await old.remove_roles(role, reason="Economy: lost the Throne")
                except discord.HTTPException as e:
                    logger.warning(f"Could not strip Throne from {old.id}: {e}")
            await self.dm(
                previous["user_id"],
                f"**{interaction.user.display_name}** just took the Throne from you "
                f"for **{commas(bid)}** Points. You paid {commas(previous['price_paid'])}.",
            )

        await self.cog.db.execute(
            "INSERT INTO throne_history (user_id, price_paid, ts) VALUES (?, ?, ?)",
            (interaction.user.id, bid, self.now()),
        )

        embed = discord.Embed(
            title="The Throne Has Changed Hands",
            description=f"{interaction.user.mention} seized the Throne for "
                        f"**{commas(bid)}** Points.",
            color=role.color if role.color.value else discord.Color.gold(),
        )
        if previous:
            embed.add_field(
                name="Previous Holder",
                value=f"<@{previous['user_id']}> — {commas(previous['price_paid'])} Points",
            )
        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------- history

    async def history(self, limit: int = 25) -> list:
        return await self.cog.db.fetchall(
            "SELECT * FROM throne_history ORDER BY ts DESC LIMIT ?", (limit,)
        )

    async def build_history_embed(self, guild: discord.Guild) -> discord.Embed:
        rows = await self.history()
        embed = discord.Embed(
            title="The Throne — Price History",
            color=discord.Color.gold(),
        )
        if not rows:
            embed.description = "*The Throne has never been claimed.*"
            return embed

        holder = await self.current_holder()
        if holder:
            member = guild.get_member(holder["user_id"])
            name = member.display_name if member else f"<@{holder['user_id']}>"
            embed.description = (
                f"**Current holder:** {name}\n"
                f"**Paid:** {commas(holder['price_paid'])} Points\n"
                f"**To seize it:** {commas(holder['price_paid'] + 1)} Points"
            )

        lines = []
        for row in rows:
            member = guild.get_member(row["user_id"])
            name = member.display_name if member else f"User {row['user_id']}"
            lines.append(
                f"`{commas(row['price_paid']):>9}` — "
                f"{discord.utils.escape_markdown(name)} · <t:{row['ts']}:d>"
            )
        embed.add_field(name="Every Claim", value="\n".join(lines)[:1024], inline=False)
        return embed


class ThroneBidModal(ui.Modal, title="Seize the Throne"):
    def __init__(self, item: ThroneItem, minimum: int, balance: int):
        super().__init__()
        self.item = item
        self.minimum = minimum
        self.bid = ui.TextInput(
            label=f"Bid (minimum {minimum:,})",
            placeholder=f"You have {balance:,} Points",
            max_length=12,
        )
        self.add_item(self.bid)

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.bid.value).replace(",", "").replace(" ", "").strip()
        if not raw.isdigit():
            await interaction.response.send_message(
                "Enter a whole number of Points.", ephemeral=True
            )
            return
        await self.item.seize(interaction, int(raw))
