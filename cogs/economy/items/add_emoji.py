"""Add Emoji — put an emoji on the server for a month, permanent if it earns it.

Usage is tracked for the whole trial: total uses, how many distinct people used
it, and how many of those were the buyer — so an inflated count is obvious in
the review embed.
"""

import json
import logging
import re

import aiohttp
import discord
from discord import ui

from .base import ShopItem
from .. import approvals

logger = logging.getLogger('cogs.economy.items.add_emoji')

CUSTOM_EMOJI = re.compile(r"<(a?):([A-Za-z0-9_]+):(\d+)>")
NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{2,32}$")
URL_PATTERN = re.compile(r"^https?://\S+$", re.IGNORECASE)


class AddEmojiItem(ShopItem):
    key = "add_emoji"
    name = "Add Emoji"

    #: Discord's own emoji ceiling is 256 KB; anything larger is rejected
    #: before it is read into memory.
    MAX_EMOJI_BYTES = 256 * 1024

    async def can_buy(self, user: discord.Member) -> tuple:
        allowed, reason = await super().can_buy(user)
        if not allowed:
            return allowed, reason
        if not self.cog.config.get_int("ch_approval", 0):
            return False, "The approval channel has not been configured yet."
        return True, ""

    async def activate(self, interaction: discord.Interaction, inv_row):
        await interaction.response.send_modal(EmojiSubmitModal(self, inv_row))

    # ------------------------------------------------------------- submission

    async def submit(self, interaction: discord.Interaction, inv_row,
                     name: str, source: str):
        name = name.strip().strip(":")
        source = source.strip()

        if not NAME_PATTERN.match(name):
            await interaction.response.send_message(
                "Emoji names must be 2–32 characters: letters, numbers or "
                "underscores only.",
                ephemeral=True,
            )
            return

        image_url = self._resolve_source(source)
        if image_url is None:
            await interaction.response.send_message(
                "Paste either a direct image URL or a custom emoji from any "
                "server you are in (type it so it renders, e.g. `:theirEmoji:`).",
                ephemeral=True,
            )
            return

        if discord.utils.get(interaction.guild.emojis, name=name):
            await interaction.response.send_message(
                f"An emoji named `:{name}:` already exists here.", ephemeral=True
            )
            return

        embed = discord.Embed(
            title="Emoji Submission",
            description=f"Requesting `:{name}:` for a "
                        f"{self.cog.config.get_int('emoji_trial_days', 30)}-day trial.",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Submitted by", value=interaction.user.mention)
        embed.set_thumbnail(url=image_url)

        approval_id = await approvals.submit(
            self.cog, "emoji", interaction.user.id,
            {"name": name, "image_url": image_url},
            price_paid=inv_row["price_paid"], inventory_id=inv_row["id"],
            embed=embed,
        )
        await self.cog.db.set_inventory_state(inv_row["id"], "active")

        await interaction.response.send_message(
            f"`:{name}:` has been sent for admin approval (submission "
            f"#{approval_id}). You will be DM'd either way — if it is denied "
            f"you get your Points back.",
            ephemeral=True,
        )

    def _resolve_source(self, source: str):
        """Accept a raw image URL or any server's custom emoji."""
        match = CUSTOM_EMOJI.search(source)
        if match:
            animated, _, emoji_id = match.groups()
            ext = "gif" if animated else "png"
            return f"https://cdn.discordapp.com/emojis/{emoji_id}.{ext}"
        if URL_PATTERN.match(source):
            return source
        return None

    # ------------------------------------------------------------ approvals

    async def on_decision(self, cog, row, status: str, admin) -> str:
        try:
            payload = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError):
            payload = {}
        name = payload.get("name")

        if status != "approved":
            await self.refund(row["user_id"], row["price_paid"], f"emoji denied: {name}")
            if row["inventory_id"]:
                await cog.db.set_inventory_state(row["inventory_id"], "refunded")
            await self.dm(
                row["user_id"],
                f"Your emoji **`:{name}:`** was denied. Your "
                f"**{row['price_paid']:,}** Points have been returned to your balance.",
            )
            return f" {row['price_paid']:,} Points refunded."

        guild = admin.guild
        try:
            image = await self._fetch(payload.get("image_url"))
            emoji = await guild.create_custom_emoji(
                name=name, image=image, reason=f"Economy: purchased by {row['user_id']}"
            )
        except Exception as e:
            logger.error(f"Could not create emoji {name}: {e}")
            await self.refund(row["user_id"], row["price_paid"], f"emoji failed: {name}")
            if row["inventory_id"]:
                await cog.db.set_inventory_state(row["inventory_id"], "refunded")
            await self.dm(
                row["user_id"],
                f"Your emoji **`:{name}:`** was approved but could not be added "
                f"(`{e}`). Your **{row['price_paid']:,}** Points have been returned.",
            )
            return f" Creation failed ({e}); the buyer was refunded."

        trial_days = cog.config.get_int("emoji_trial_days", 30)
        expires = self.now() + trial_days * 86400
        await cog.db.execute(
            "INSERT INTO shop_emojis (emoji_id, name, user_id, added_ts, expires_ts, "
            "status) VALUES (?, ?, ?, ?, ?, 'trial')",
            (emoji.id, name, row["user_id"], self.now(), expires),
        )
        if row["inventory_id"]:
            await cog.db.set_inventory_state(row["inventory_id"], "consumed")

        await self.dm(
            row["user_id"],
            f"Your emoji {emoji} **`:{name}:`** was approved and is live for the "
            f"next {trial_days} days. If it gets enough use, admins can make it "
            f"permanent.",
        )
        return f" {emoji} added for {trial_days} days."

    async def on_permanent_decision(self, cog, row, status: str, admin) -> str:
        try:
            payload = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError):
            payload = {}
        emoji_id = payload.get("emoji_id")
        name = payload.get("name")

        if status == "approved":
            await cog.db.execute(
                "UPDATE shop_emojis SET status = 'permanent', expires_ts = NULL "
                "WHERE emoji_id = ?", (emoji_id,)
            )
            await self.dm(
                row["user_id"],
                f"Your emoji **`:{name}:`** earned a permanent spot on the server.",
            )
            return f" `:{name}:` is now permanent."

        emoji = cog.bot.get_emoji(emoji_id) if emoji_id else None
        if emoji:
            try:
                await emoji.delete(reason="Economy: trial ended without permanent status")
            except discord.HTTPException as e:
                logger.warning(f"Could not delete emoji {emoji_id}: {e}")
        await cog.db.execute(
            "UPDATE shop_emojis SET status = 'removed' WHERE emoji_id = ?", (emoji_id,)
        )
        await self.dm(
            row["user_id"],
            f"Your emoji **`:{name}:`** finished its trial and was not kept. "
            f"No refund applies — it had its month.",
        )
        return f" `:{name}:` removed."

    @classmethod
    async def _fetch(cls, url: str) -> bytes:
        """Download an emoji image with a hard size ceiling.

        The URL is user-supplied, so an unbounded read is a remote OOM: a
        multi-gigabyte target would be pulled entirely into memory. Discord
        caps emoji at 256 KB anyway, so the ceiling costs nothing legitimate.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                resp.raise_for_status()

                declared = resp.content_length
                if declared is not None and declared > cls.MAX_EMOJI_BYTES:
                    raise ValueError(
                        f"image is {declared // 1024} KB — the limit is "
                        f"{cls.MAX_EMOJI_BYTES // 1024} KB"
                    )

                # Content-Length can be absent or a lie, so cap the read itself.
                chunks, total = [], 0
                async for chunk in resp.content.iter_chunked(32 * 1024):
                    total += len(chunk)
                    if total > cls.MAX_EMOJI_BYTES:
                        raise ValueError(
                            f"image exceeds the "
                            f"{cls.MAX_EMOJI_BYTES // 1024} KB limit"
                        )
                    chunks.append(chunk)
                return b"".join(chunks)

    # -------------------------------------------------------------- runtime

    async def on_message(self, cog, message: discord.Message):
        """Log every use of a tracked emoji in message content."""
        if not message.content:
            return
        ids = {int(m.group(3)) for m in CUSTOM_EMOJI.finditer(message.content)}
        if not ids:
            return
        for emoji_id in ids:
            await self._log_usage(cog, emoji_id, message.author.id, "message")

    async def on_reaction(self, cog, payload: discord.RawReactionActionEvent):
        emoji_id = getattr(payload.emoji, "id", None)
        if emoji_id:
            await self._log_usage(cog, emoji_id, payload.user_id, "reaction")

    async def _log_usage(self, cog, emoji_id: int, user_id: int, usage_type: str):
        tracked = await cog.db.fetchone(
            "SELECT 1 FROM shop_emojis WHERE emoji_id = ? AND status = 'trial'",
            (emoji_id,),
        )
        if not tracked:
            return
        await cog.db.execute(
            "INSERT INTO emoji_usage (emoji_id, user_id, usage_type, ts) VALUES (?, ?, ?, ?)",
            (emoji_id, user_id, usage_type, self.now()),
        )

    async def maintenance(self, cog):
        """Send trials that have run their month to the approval channel."""
        rows = await cog.db.fetchall(
            "SELECT * FROM shop_emojis WHERE status = 'trial' AND reviewed = 0 "
            "AND expires_ts IS NOT NULL AND expires_ts <= ?",
            (self.now(),),
        )
        for row in rows:
            try:
                await self._post_review(cog, row)
            except Exception as e:
                logger.error(f"Emoji review failed for {row['emoji_id']}: {e}")

    async def _post_review(self, cog, row):
        totals = await cog.db.fetchone(
            "SELECT COUNT(*) AS uses, COUNT(DISTINCT user_id) AS users "
            "FROM emoji_usage WHERE emoji_id = ?",
            (row["emoji_id"],),
        )
        by_buyer = await cog.db.fetchone(
            "SELECT COUNT(*) AS c FROM emoji_usage WHERE emoji_id = ? AND user_id = ?",
            (row["emoji_id"], row["user_id"]),
        )
        uses = totals["uses"] if totals else 0
        users = totals["users"] if totals else 0
        buyer_uses = by_buyer["c"] if by_buyer else 0
        buyer_share = round(buyer_uses / uses * 100) if uses else 0

        emoji = cog.bot.get_emoji(row["emoji_id"])
        embed = discord.Embed(
            title="Emoji Permanent Status Review",
            description=f"{emoji if emoji else ''} `:{row['name']}:` has finished its "
                        f"{cog.config.get_int('emoji_trial_days', 30)}-day trial.\n\n"
                        f"**Approve** to keep it permanently, **Deny** to remove it.",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Total Uses", value=f"{uses:,}")
        embed.add_field(name="Distinct Users", value=f"{users:,}")
        embed.add_field(name="Buyer's Share", value=f"{buyer_uses:,} ({buyer_share}%)")
        embed.add_field(name="Bought by", value=f"<@{row['user_id']}>", inline=False)
        if emoji:
            embed.set_thumbnail(url=emoji.url)
        if buyer_share >= 60 and uses > 0:
            embed.add_field(
                name="Note",
                value="Most of this emoji's use came from the buyer.",
                inline=False,
            )

        await approvals.submit(
            cog, "emoji_permanent", row["user_id"],
            {"emoji_id": row["emoji_id"], "name": row["name"]},
            embed=embed,
        )
        await cog.db.execute(
            "UPDATE shop_emojis SET reviewed = 1 WHERE id = ?", (row["id"],)
        )


class EmojiSubmitModal(ui.Modal, title="Add an Emoji"):
    emoji_name = ui.TextInput(
        label="Emoji name",
        placeholder="letters, numbers and underscores",
        max_length=32,
    )
    source = ui.TextInput(
        label="Image URL or an existing custom emoji",
        placeholder="https://... or paste an emoji from any server",
        style=discord.TextStyle.paragraph,
        max_length=500,
    )

    def __init__(self, item: AddEmojiItem, inv_row):
        super().__init__()
        self.item = item
        self.inv_row = inv_row

    async def on_submit(self, interaction: discord.Interaction):
        await self.item.submit(
            interaction, self.inv_row,
            str(self.emoji_name.value), str(self.source.value),
        )
