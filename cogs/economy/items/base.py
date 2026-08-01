"""Shop item interface.

Each item subclasses ShopItem and overrides only what it needs. The registry
calls the optional hooks (on_message, on_reaction, maintenance, restore) on
every item that defines them, so an item's whole behaviour lives in one file.
"""

import logging
import time

import discord
from discord import ui

logger = logging.getLogger('cogs.economy.items')


class ClaimedFlowView(ui.View):
    """A setup view holding an inventory row that was claimed before it opened.

    The claim must be given back if the flow is abandoned, otherwise the item
    is stranded in 'active' and the owner can never use it. Call `settle()`
    once the item has genuinely been spent.
    """

    def __init__(self, item, inv_row, *, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.item = item
        self.inv_row = inv_row
        self._settled = False

    def settle(self):
        """Mark the item as legitimately consumed — nothing to give back."""
        self._settled = True

    async def release(self):
        if self._settled:
            return
        self._settled = True
        await self.item.cog.db.release_inventory(self.inv_row["id"])

    async def on_timeout(self):
        await self.release()

    async def cancel_flow(self, interaction: discord.Interaction,
                          message: str = "Cancelled — your item is untouched."):
        await self.release()
        await interaction.response.edit_message(content=message, embed=None, view=None)
        self.stop()


class ShopItem:
    #: Settings key suffix and inventory item_key.
    key: str = ""
    #: Falls back to the catalogue entry in config.SHOP_ITEMS.
    name: str = ""
    #: Set on items that resolve at purchase rather than sitting in inventory.
    immediate: bool = False

    def __init__(self, cog):
        self.cog = cog

    # ----------------------------------------------------------- purchasing

    async def price(self, user_id: int) -> int:
        return self.cog.config.price(self.key)

    async def can_buy(self, user: discord.Member) -> tuple:
        """(allowed, reason). Reason is shown to the buyer when blocked."""
        if not self.cog.config.item_enabled(self.key):
            return False, "This item is not available right now."
        return True, ""

    def shelf_seconds(self) -> int:
        """Seconds before an unused copy expires. 0 means it never expires."""
        from ..config import ALL_ITEMS
        days = (ALL_ITEMS.get(self.key) or {}).get("shelf_days")
        return int(days) * 86400 if days else 0

    async def on_purchase(self, interaction: discord.Interaction, inv_id: int):
        """Called after Points are deducted and the item is in inventory.

        Immediate items override this to run their flow straight away.
        """
        return None

    # ----------------------------------------------------------- activation

    async def activate(self, interaction: discord.Interaction, inv_row):
        """Open this item's configuration flow from My Items."""
        await interaction.response.send_message(
            "This item has nothing to configure.", ephemeral=True
        )

    def describe_owned(self, inv_row) -> str:
        """One line shown in My Items beneath the item name."""
        if inv_row["expires_ts"]:
            return f"Expires <t:{inv_row['expires_ts']}:R>"
        return "Ready to use"

    # -------------------------------------------------------------- helpers

    async def consume(self, inv_row, *, state: str = "consumed"):
        await self.cog.db.set_inventory_state(inv_row["id"], state)

    async def release(self, inv_row):
        """Hand a claimed item back — for early returns out of activate()."""
        await self.cog.db.release_inventory(inv_row["id"])

    async def refund(self, user_id: int, amount: int, note: str = ""):
        await self.cog.earning.refund(user_id, amount, self.key, {"note": note})

    async def dm(self, user_id: int, content: str = None, embed: discord.Embed = None):
        """Best-effort DM. A closed DM or a dead user must never break a
        maintenance loop, so every failure mode is swallowed."""
        try:
            user = self.cog.bot.get_user(user_id)
            if user is None:
                user = await self.cog.bot.fetch_user(user_id)
            await user.send(content=content, embed=embed)
            return True
        except Exception as e:
            logger.debug(f"DM to {user_id} failed: {e}")
            return False

    @staticmethod
    def now() -> int:
        return int(time.time())
