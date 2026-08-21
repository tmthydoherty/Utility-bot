"""Add GIF Command — buy your own !command that posts a GIF.

This item sells and approves the command and writes its `gif_commands` row.
Serving the live `!command` belongs to the Custom Commands cog, which owns the
whole `!` namespace (see cogs/custom_commands); it reads the row this writes.
"""

import logging
import re

import discord
from discord import ui

from .base import ShopItem
from .. import approvals

logger = logging.getLogger('cogs.economy.items.gif_command')

NAME_PATTERN = re.compile(r"^[a-z0-9_]{2,20}$")
URL_PATTERN = re.compile(r"^https?://\S+$", re.IGNORECASE)


class GifCommandItem(ShopItem):
    key = "gif_command"
    name = "Add GIF Command"

    # ---------------------------------------------------------------- price

    async def slot_for(self, user_id: int) -> int:
        row = await self.cog.db.fetchone(
            "SELECT COUNT(*) AS c FROM gif_commands WHERE user_id = ?", (user_id,)
        )
        owned = row["c"] if row else 0
        pending = await self.cog.db.fetchone(
            "SELECT COUNT(*) AS c FROM approvals WHERE kind = 'gif' AND user_id = ? "
            "AND status = 'pending'",
            (user_id,),
        )
        return owned + (pending["c"] if pending else 0) + 1

    async def price(self, user_id: int) -> int:
        return self.cog.config.gif_price(await self.slot_for(user_id))

    async def can_buy(self, user: discord.Member) -> tuple:
        allowed, reason = await super().can_buy(user)
        if not allowed:
            return allowed, reason
        limit = self.cog.config.get_int("gif_max_per_user", 3)
        if await self.slot_for(user.id) > limit:
            return False, f"You already have the maximum of {limit} GIF commands."
        if not self.cog.config.get_int("ch_approval", 0):
            return False, "The approval channel has not been configured yet."
        return True, ""

    def _name_reserved(self, name: str) -> bool:
        """True if a real bot command already owns this name or alias."""
        taken = {c.name for c in self.cog.bot.commands}
        taken |= {a for c in self.cog.bot.commands for a in c.aliases}
        return name in taken

    # ----------------------------------------------------------- activation

    async def activate(self, interaction: discord.Interaction, inv_row):
        await interaction.response.send_modal(GifSubmitModal(self, inv_row))

    async def submit(self, interaction: discord.Interaction, inv_row,
                     name: str, url: str):
        name = name.strip().lstrip("!").lower()

        if not NAME_PATTERN.match(name):
            await interaction.response.send_message(
                "Command names must be 2–20 characters, lowercase letters, "
                "numbers or underscores.",
                ephemeral=True,
            )
            return
        if not URL_PATTERN.match(url.strip()):
            await interaction.response.send_message(
                "That does not look like a valid URL.", ephemeral=True
            )
            return
        if self._name_reserved(name):
            await interaction.response.send_message(
                f"`!{name}` is already a built-in command. Pick another name.",
                ephemeral=True,
            )
            return

        existing = await self.cog.db.fetchone(
            "SELECT user_id FROM gif_commands WHERE name = ?", (name,)
        )
        if existing:
            await interaction.response.send_message(
                f"`!{name}` is already taken.", ephemeral=True
            )
            return

        embed = discord.Embed(
            title="GIF Command Submission",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Command", value=f"`!{name}`", inline=False)
        embed.add_field(name="URL", value=url[:1000], inline=False)
        embed.add_field(name="Submitted by", value=interaction.user.mention)
        embed.set_image(url=url)

        approval_id = await approvals.submit(
            self.cog, "gif", interaction.user.id,
            {"name": name, "url": url.strip()},
            price_paid=inv_row["price_paid"], inventory_id=inv_row["id"],
            embed=embed,
        )
        await self.cog.db.set_inventory_state(inv_row["id"], "active")

        await interaction.response.send_message(
            f"`!{name}` has been sent for admin approval (submission "
            f"#{approval_id}). You will be DM'd either way — if it is denied "
            f"you get your Points back.",
            ephemeral=True,
        )

    # ------------------------------------------------------------ approvals

    async def on_decision(self, cog, row, status: str, admin) -> str:
        import json
        try:
            payload = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError):
            payload = {}
        name, url = payload.get("name"), payload.get("url")

        if status == "approved":
            # The name was free when this was submitted, but another submission
            # may have been approved first. INSERT (not INSERT OR REPLACE) so a
            # collision fails loudly instead of silently destroying the command
            # the other person paid for.
            taken = await cog.db.fetchone(
                "SELECT user_id FROM gif_commands WHERE name = ?", (name,)
            )
            if taken or self._name_reserved(name):
                owner = f"<@{taken['user_id']}>" if taken else "a built-in command"
                await self.refund(
                    row["user_id"], row["price_paid"], f"gif name taken: {name}"
                )
                if row["inventory_id"]:
                    await cog.db.set_inventory_state(row["inventory_id"], "refunded")
                await self.dm(
                    row["user_id"],
                    f"Your GIF command **`!{name}`** could not be created — the "
                    f"name was claimed before yours was approved. Your "
                    f"**{row['price_paid']:,}** Points have been returned.",
                )
                return (f" `!{name}` was already claimed by {owner}; the "
                        f"submitter was refunded instead.")

            await cog.db.execute(
                "INSERT INTO gif_commands (name, user_id, url, approved_ts) "
                "VALUES (?, ?, ?, ?)",
                (name, row["user_id"], url, self.now()),
            )
            if row["inventory_id"]:
                await cog.db.set_inventory_state(row["inventory_id"], "consumed")
            await self.dm(
                row["user_id"],
                f"Your GIF command **`!{name}`** was approved and is live now.",
            )
            return f" `!{name}` is live."

        await self.refund(row["user_id"], row["price_paid"], f"gif denied: {name}")
        if row["inventory_id"]:
            await cog.db.set_inventory_state(row["inventory_id"], "refunded")
        await self.dm(
            row["user_id"],
            f"Your GIF command **`!{name}`** was denied. Your "
            f"**{row['price_paid']:,}** Points have been returned to your balance.",
        )
        return f" {row['price_paid']:,} Points refunded."

    # -------------------------------------------------------------- runtime

    # Serving the live `!command` is deliberately *not* here any more. The
    # Custom Commands cog (cogs/custom_commands) now owns the entire `!`
    # namespace — its own admin commands and these purchased GIFs — so there is
    # one matcher, one precedence order and no chance of two cogs both answering
    # the same `!word`. This item still sells and approves GIF commands and
    # writes the `gif_commands` row; Custom Commands reads that row and serves
    # it, skipping any a moderator has flagged `disabled`.


class GifSubmitModal(ui.Modal, title="Add a GIF Command"):
    command_name = ui.TextInput(
        label="Command name (without the !)",
        placeholder="e.g. dance",
        max_length=20,
    )
    gif_url = ui.TextInput(
        label="GIF URL",
        placeholder="https://...",
        max_length=500,
    )

    def __init__(self, item: GifCommandItem, inv_row):
        super().__init__()
        self.item = item
        self.inv_row = inv_row

    async def on_submit(self, interaction: discord.Interaction):
        await self.item.submit(
            interaction, self.inv_row,
            str(self.command_name.value), str(self.gif_url.value),
        )
