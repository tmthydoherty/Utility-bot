"""HoF Post — a one-message window into the Hall of Fame channel.

The buyer is granted a real send_messages overwrite, which is revoked the
instant they post (or when the grant window lapses). Grants are persisted, so
a restart mid-window still cleans up instead of leaving access behind.
"""

import json
import logging

import discord
from discord import ui

from .base import ClaimedFlowView, ShopItem

logger = logging.getLogger('cogs.economy.items.hof')


class HofPostItem(ShopItem):
    key = "hof_post"
    name = "HoF Post"

    def __init__(self, cog):
        super().__init__(cog)
        # (user_id, channel_id) pairs with a live grant. on_message runs for
        # every message in the server, so this has to be answerable without
        # touching the database.
        self._live: set = set()

    async def can_buy(self, user: discord.Member) -> tuple:
        allowed, reason = await super().can_buy(user)
        if not allowed:
            return allowed, reason
        if not self.cog.config.get_int("ch_hof", 0):
            return False, "The Hall of Fame channel has not been configured yet."
        return True, ""

    async def activate(self, interaction: discord.Interaction, inv_row):
        channel = self.cog.bot.get_channel(self.cog.config.get_int("ch_hof", 0))
        if channel is None:
            await self.release(inv_row)
            await interaction.response.send_message(
                "The Hall of Fame channel is not set up. Tell an admin.", ephemeral=True
            )
            return

        minutes = self.cog.config.get_int("hof_grant_minutes", 15)
        embed = discord.Embed(
            title="HoF Post",
            description=(
                f"You will be given permission to post in {channel.mention} for "
                f"**{minutes} minutes**.\n\n"
                f"Your access is removed the moment you send **one** message, "
                f"or when the window closes — whichever comes first.\n\n"
                f"This item is spent as soon as you confirm."
            ),
            color=discord.Color.gold(),
        )
        await interaction.response.send_message(
            embed=embed, view=HofConfirmView(self, inv_row, channel), ephemeral=True
        )

    async def grant(self, interaction: discord.Interaction, view,
                    channel: discord.TextChannel):
        inv_row = view.inv_row
        member = interaction.user
        existing = channel.overwrites_for(member)
        prior = json.dumps({"send_messages": existing.send_messages})

        overwrite = existing
        overwrite.send_messages = True
        overwrite.view_channel = True
        try:
            await channel.set_permissions(
                member, overwrite=overwrite, reason="Economy: HoF Post purchased"
            )
        except discord.HTTPException as e:
            # Nothing was granted, so give the item back rather than stranding it.
            logger.warning(f"HoF permission grant failed for {member.id}: {e}")
            await view.release()
            await interaction.response.edit_message(
                content="The bot could not edit permissions on that channel. "
                        "Nothing was spent — tell an admin.",
                embed=None, view=None,
            )
            return

        # From here the perm_grants row owns the item until the window closes.
        view.settle()

        minutes = self.cog.config.get_int("hof_grant_minutes", 15)
        expires = self.now() + minutes * 60
        await self.cog.db.execute(
            "INSERT INTO perm_grants (user_id, guild_id, channel_id, kind, granted_ts, "
            "expires_ts, inventory_id, prior_state) VALUES (?, ?, ?, 'hof', ?, ?, ?, ?)",
            (member.id, channel.guild.id, channel.id, self.now(), expires,
             inv_row["id"], prior),
        )
        self._live.add((member.id, channel.id))

        await interaction.response.edit_message(
            content=f"You can now post in {channel.mention}. Your access closes "
                    f"after one message, or <t:{expires}:R>.",
            embed=None, view=None,
        )

    # ------------------------------------------------------------- runtime

    async def on_message(self, cog, message: discord.Message):
        """Close the window as soon as the buyer posts."""
        if (message.author.id, message.channel.id) not in self._live:
            return
        row = await cog.db.fetchone(
            "SELECT * FROM perm_grants WHERE user_id = ? AND channel_id = ? "
            "AND kind = 'hof'",
            (message.author.id, message.channel.id),
        )
        if row:
            await self._revoke(row, consumed=True)
        else:
            self._live.discard((message.author.id, message.channel.id))

    async def maintenance(self, cog):
        """Revoke grants whose window has lapsed unused."""
        rows = await cog.db.fetchall(
            "SELECT * FROM perm_grants WHERE kind = 'hof' AND expires_ts <= ?",
            (self.now(),),
        )
        for row in rows:
            await self._revoke(row, consumed=False)

    async def restore(self, cog):
        """Rebuild the in-memory grant set, then sweep anything already lapsed."""
        rows = await cog.db.fetchall(
            "SELECT user_id, channel_id FROM perm_grants WHERE kind = 'hof'"
        )
        self._live = {(r["user_id"], r["channel_id"]) for r in rows}
        await self.maintenance(cog)

    async def _revoke(self, row, *, consumed: bool):
        channel = self.cog.bot.get_channel(row["channel_id"])
        guild = self.cog.bot.get_guild(row["guild_id"])
        member = guild.get_member(row["user_id"]) if guild else None

        if channel and member:
            try:
                prior = json.loads(row["prior_state"] or "{}")
            except (TypeError, ValueError):
                prior = {}
            overwrite = channel.overwrites_for(member)
            overwrite.send_messages = prior.get("send_messages")
            overwrite.view_channel = None
            try:
                if overwrite.is_empty():
                    await channel.set_permissions(
                        member, overwrite=None, reason="Economy: HoF Post window closed"
                    )
                else:
                    await channel.set_permissions(
                        member, overwrite=overwrite,
                        reason="Economy: HoF Post window closed",
                    )
            except discord.HTTPException as e:
                logger.warning(f"Could not revoke HoF access for {row['user_id']}: {e}")

        self._live.discard((row["user_id"], row["channel_id"]))
        await self.cog.db.execute("DELETE FROM perm_grants WHERE id = ?", (row["id"],))

        # Spent either way — the buyer had their window whether they used it or not.
        if row["inventory_id"]:
            await self.cog.db.set_inventory_state(row["inventory_id"], "consumed")

        if not consumed:
            await self.dm(
                row["user_id"],
                "Your **HoF Post** window closed before you used it. The item has "
                "been spent — talk to an admin if that was not your doing.",
            )


class HofConfirmView(ClaimedFlowView):
    def __init__(self, item: HofPostItem, inv_row, channel):
        super().__init__(item, inv_row, timeout=180)
        self.channel = channel

    @ui.button(label="Grant Access", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        await self.item.grant(interaction, self, self.channel)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await self.cancel_flow(interaction, "Cancelled — nothing was spent.")
