"""The shop — /eco_shop.

Landing screen splits into Shop (browse and buy) and My Items (activate what
you own). Buying puts an item in your inventory; activating opens that item's
own configuration flow.
"""

import logging

import discord
from discord import ui

from .config import ALL_ITEMS, SHOP_ITEMS
from .render import commas

logger = logging.getLogger('cogs.economy.shop')

INVENTORY_PAGE = 20


async def build_home_embed(cog, user) -> discord.Embed:
    row = await cog.db.get_user(user.id)
    owned = await cog.db.get_inventory(user.id)

    embed = discord.Embed(
        title="The Shop",
        description="Spend Points on things that mostly should not exist.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Your Balance", value=f"**{commas(row['points'])}** Points")
    embed.add_field(name="Lifetime Earned", value=commas(row["lifetime_points"]))
    embed.add_field(name="Items Owned", value=str(len(owned)))
    embed.set_footer(text="Browse the shop, or open My Items to use what you have.")
    return embed


class ShopViewBase(ui.View):
    """Shared owner check and timeout behaviour for the shop screens."""

    def __init__(self, cog, user, *, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.user = user

    async def on_timeout(self):
        # Leaving live-looking buttons behind produces "interaction failed"
        # rather than an honest "this expired".
        for child in self.children:
            child.disabled = True


class ShopHomeView(ShopViewBase):
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "This isn't your shop session — run `/eco_shop` yourself.",
                ephemeral=True,
            )
            return False
        return True

    @ui.button(label="Shop", style=discord.ButtonStyle.primary)
    async def shop(self, interaction: discord.Interaction, button: ui.Button):
        view = ShopBrowseView(self.cog, self.user)
        await view.load()
        await interaction.response.edit_message(
            embed=await view.build_embed(), view=view
        )

    @ui.button(label="My Items", style=discord.ButtonStyle.secondary)
    async def inventory(self, interaction: discord.Interaction, button: ui.Button):
        view = InventoryView(self.cog, self.user)
        await view.load()
        await interaction.response.edit_message(
            embed=await view.build_embed(), view=view
        )

    @ui.button(label="Throne History", style=discord.ButtonStyle.secondary)
    async def throne_history(self, interaction: discord.Interaction, button: ui.Button):
        item = self.cog.items.get("throne")
        embed = await item.build_history_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class BackButton(ui.Button):
    def __init__(self, cog, user, row: int = 4):
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.cog = cog
        self.user = user

    async def callback(self, interaction: discord.Interaction):
        view = ShopHomeView(self.cog, self.user)
        embed = await build_home_embed(self.cog, self.user)
        await interaction.response.edit_message(embed=embed, view=view)


# --------------------------------------------------------------------------
# Browsing and buying
# --------------------------------------------------------------------------

class ShopBrowseView(ShopViewBase):
    def __init__(self, cog, user):
        super().__init__(cog, user)
        self.selected_key = None
        self.prices = {}
        self.add_item(BackButton(cog, user))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "This isn't your shop session.", ephemeral=True
            )
            return False
        return True

    async def load(self):
        options = []
        for item in self.cog.items.shop_items():
            if not self.cog.config.item_enabled(item.key):
                continue
            price = await item.price(self.user.id)
            self.prices[item.key] = price
            meta = SHOP_ITEMS.get(item.key, {})
            options.append(
                discord.SelectOption(
                    label=meta.get("name", item.key)[:100],
                    value=item.key,
                    description=f"{commas(price)} Points"[:100],
                    default=item.key == self.selected_key,
                )
            )
        if options:
            self.picker.options = options
        else:
            self.picker.options = [
                discord.SelectOption(label="Nothing is for sale", value="none")
            ]
            self.picker.disabled = True
        await self._sync()

    async def _sync(self):
        if self.selected_key is None:
            self.buy.disabled = True
            self.buy.label = "Buy"
            return
        balance = await self.cog.db.get_balance(self.user.id)
        price = self.prices.get(self.selected_key, 0)
        item = self.cog.items.get(self.selected_key)
        allowed, _ = await item.can_buy(self.user)
        if balance < price:
            self.buy.disabled = True
            self.buy.label = f"Need {commas(price - balance)} more"
        else:
            self.buy.disabled = not allowed
            self.buy.label = f"Buy — {commas(price)} Points"

    async def build_embed(self) -> discord.Embed:
        balance = await self.cog.db.get_balance(self.user.id)
        embed = discord.Embed(title="The Shop", color=discord.Color.blurple())

        if self.selected_key is None:
            lines = []
            for item in self.cog.items.shop_items():
                if not self.cog.config.item_enabled(item.key):
                    continue
                meta = SHOP_ITEMS.get(item.key, {})
                price = self.prices.get(item.key, 0)
                lines.append(f"**{meta.get('name', item.key)}** — {commas(price)} Points")
            embed.description = "\n".join(lines) or "*Nothing is for sale right now.*"
            embed.set_footer(text=f"Balance: {commas(balance)} Points")
            return embed

        meta = SHOP_ITEMS.get(self.selected_key, {})
        item = self.cog.items.get(self.selected_key)
        price = self.prices.get(self.selected_key, 0)

        embed.title = meta.get("name", self.selected_key)
        embed.description = meta.get("blurb", "")
        embed.add_field(name="Price", value=f"**{commas(price)}** Points")
        embed.add_field(name="Your Balance", value=commas(balance))

        allowed, reason = await item.can_buy(self.user)
        if not allowed:
            embed.add_field(name="Unavailable", value=reason, inline=False)
        elif item.immediate:
            embed.add_field(
                name="Note",
                value="This resolves immediately — it does not go into My Items.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Note",
                value="Buying adds this to **My Items**. You choose how to use it later.",
                inline=False,
            )
        return embed

    @ui.select(placeholder="Choose an item...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        if select.values[0] == "none":
            await interaction.response.defer()
            return
        self.selected_key = select.values[0]
        await self.load()
        await interaction.response.edit_message(embed=await self.build_embed(), view=self)

    @ui.button(label="Buy", style=discord.ButtonStyle.success, row=1, disabled=True)
    async def buy(self, interaction: discord.Interaction, button: ui.Button):
        item = self.cog.items.get(self.selected_key)
        if item is None:
            await interaction.response.send_message("Pick an item first.", ephemeral=True)
            return

        allowed, reason = await item.can_buy(self.user)
        if not allowed:
            await interaction.response.send_message(reason, ephemeral=True)
            return

        # The Throne is a bid, not a fixed price — it runs its own flow.
        if item.immediate:
            await item.open_bid(interaction)
            return

        price = await item.price(self.user.id)
        meta = SHOP_ITEMS.get(item.key, {})
        embed = discord.Embed(
            title=f"Buy {meta.get('name', item.key)}?",
            description=f"{meta.get('blurb', '')}\n\n"
                        f"This costs **{commas(price)}** Points.",
            color=discord.Color.orange(),
        )
        if item.key == "customs_match":
            embed.add_field(
                name="Non-refundable",
                value="If the queue never fills, the item is still spent.",
                inline=False,
            )
        await interaction.response.send_message(
            embed=embed, view=ConfirmPurchaseView(self.cog, self.user, item, price),
            ephemeral=True,
        )


class ConfirmPurchaseView(ui.View):
    def __init__(self, cog, user, item, price: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.user = user
        self.item = item
        self.price = price

    @ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        # Re-check price and availability — both can move while this sits open.
        current = await self.item.price(self.user.id)
        allowed, reason = await self.item.can_buy(self.user)
        if not allowed:
            await interaction.response.edit_message(
                content=reason, embed=None, view=None
            )
            self.stop()
            return
        if current != self.price:
            await interaction.response.edit_message(
                content=f"The price changed to **{commas(current)}** Points. "
                        f"Nothing was spent — try again.",
                embed=None, view=None,
            )
            self.stop()
            return

        if not await self.cog.earning.spend(
            self.user.id, current, f"shop:{self.item.key}"
        ):
            await interaction.response.edit_message(
                content="You cannot afford that any more.", embed=None, view=None
            )
            self.stop()
            return

        shelf = self.item.shelf_seconds()
        inv_id = await self.cog.db.add_inventory(
            self.user.id, self.item.key, current,
            expires_ts=(self.item.now() + shelf) if shelf else None,
        )

        meta = ALL_ITEMS.get(self.item.key, {})
        note = ""
        if shelf:
            note = f"\n\nUse it within **{shelf // 86400} days** or it expires."
        await interaction.response.edit_message(
            content=f"**{meta.get('name', self.item.key)}** is in your items. "
                    f"Open **My Items** to use it.{note}",
            embed=None, view=None,
        )
        await self.item.on_purchase(interaction, inv_id)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(
            content="Cancelled.", embed=None, view=None
        )
        self.stop()


# --------------------------------------------------------------------------
# Inventory
# --------------------------------------------------------------------------

class InventoryView(ShopViewBase):
    def __init__(self, cog, user):
        super().__init__(cog, user)
        self.rows = []
        self.selected_id = None
        self.add_item(BackButton(cog, user))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "These aren't your items.", ephemeral=True
            )
            return False
        return True

    async def load(self):
        self.rows = await self.cog.db.get_inventory(self.user.id, states=('owned',))
        options = []
        for row in self.rows[:INVENTORY_PAGE]:
            meta = ALL_ITEMS.get(row["item_key"], {})
            item = self.cog.items.get(row["item_key"])
            description = item.describe_owned(row) if item else "Ready to use"
            options.append(
                discord.SelectOption(
                    label=meta.get("name", row["item_key"])[:100],
                    value=str(row["id"]),
                    description=description[:100],
                    default=str(row["id"]) == str(self.selected_id),
                )
            )

        if options:
            self.picker.options = options
            self.picker.disabled = False
        else:
            self.picker.options = [
                discord.SelectOption(label="You own nothing", value="none")
            ]
            self.picker.disabled = True
        self.activate.disabled = self.selected_id is None

    async def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="My Items", color=discord.Color.blurple())

        if not self.rows:
            embed.description = "*You do not own anything yet.*"
            return embed

        lines = []
        for row in self.rows[:INVENTORY_PAGE]:
            meta = ALL_ITEMS.get(row["item_key"], {})
            name = meta.get("name", row["item_key"])
            if row["expires_ts"]:
                lines.append(f"**{name}** — expires <t:{row['expires_ts']}:R>")
            else:
                lines.append(f"**{name}**")
        embed.description = "\n".join(lines)

        active = await self.cog.effects.effects_for(self.user.id)
        if active:
            from .items.curses import CURSE_LABELS
            embed.add_field(
                name="Active On You",
                value="\n".join(
                    f"{CURSE_LABELS.get(e['effect_key'], e['effect_key'])} — "
                    f"ends <t:{e['expires_ts']}:R>"
                    for e in active
                )[:1024],
                inline=False,
            )

        if len(self.rows) > INVENTORY_PAGE:
            embed.set_footer(
                text=f"Showing {INVENTORY_PAGE} of {len(self.rows)} items."
            )
        return embed

    @ui.select(placeholder="Choose an item to use...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        if select.values[0] == "none":
            await interaction.response.defer()
            return
        self.selected_id = int(select.values[0])
        await self.load()
        await interaction.response.edit_message(embed=await self.build_embed(), view=self)

    @ui.button(label="Use", style=discord.ButtonStyle.success, row=1, disabled=True)
    async def activate(self, interaction: discord.Interaction, button: ui.Button):
        row = await self.cog.db.get_inventory_item(self.selected_id)
        if row is None or row["user_id"] != self.user.id:
            await interaction.response.send_message(
                "That item is no longer available.", ephemeral=True
            )
            return

        item = self.cog.items.get(row["item_key"])
        if item is None:
            await interaction.response.send_message(
                "That item type no longer exists. Tell an admin.", ephemeral=True
            )
            return

        # Reserve the row before opening any flow. Activation flows span several
        # interactions, so without this claim the same item can be driven
        # through two flows at once and used twice.
        if not await self.cog.db.claim_inventory(row["id"]):
            await interaction.response.send_message(
                "You are already using that item somewhere else — finish or "
                "close that window first.",
                ephemeral=True,
            )
            return

        try:
            await item.activate(interaction, row)
        except Exception:
            # The flow never opened, so hand the item straight back.
            await self.cog.db.release_inventory(row["id"])
            raise
