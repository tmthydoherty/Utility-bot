"""Item registry — instantiates every shop item and fans hooks out to them."""

import json
import logging
import time

import discord

from .add_emoji import AddEmojiItem
from .auto_react import AutoReactItem
from .curses import CurseWipeItem, NicknameHijackItem
from .customs_match import CustomsMatchItem
from .gif_command import GifCommandItem
from .hof import HofPostItem
from .mystery_box import MysteryBoxItem
from .proxy import ProxyItem
from .throne import ThroneItem

logger = logging.getLogger('cogs.economy.items')

ITEM_CLASSES = (
    AutoReactItem,
    HofPostItem,
    GifCommandItem,
    AddEmojiItem,
    ThroneItem,
    CustomsMatchItem,
    ProxyItem,
    MysteryBoxItem,
    NicknameHijackItem,
    CurseWipeItem,
)

# Days remaining at which a shelf-life item earns a reminder DM.
REMINDER_DAYS = (7, 1)

# How long an item may sit reserved by an activation flow before we assume the
# flow died (dismissed modal, crash, restart) and hand it back to its owner.
STRANDED_AFTER = 30 * 60


class ItemRegistry:
    def __init__(self, cog):
        self.cog = cog
        self.items = {}
        for cls in ITEM_CLASSES:
            item = cls(cog)
            self.items[item.key] = item

    def get(self, key: str):
        return self.items.get(key)

    def shop_items(self) -> list:
        """Items sold in the shop, in catalogue order."""
        from ..config import SHOP_ITEMS
        return [self.items[k] for k in SHOP_ITEMS if k in self.items]

    # ------------------------------------------------------------ approvals

    def approval_handler(self, kind: str):
        if kind == "gif":
            return self.items["gif_command"].on_decision
        if kind == "emoji":
            return self.items["add_emoji"].on_decision
        if kind == "emoji_permanent":
            return self.items["add_emoji"].on_permanent_decision
        return None

    # ---------------------------------------------------------------- hooks

    async def on_message(self, cog, message: discord.Message):
        for item in self.items.values():
            handler = getattr(item, "on_message", None)
            if handler is None:
                continue
            try:
                await handler(cog, message)
            except Exception as e:
                logger.error(f"{item.key}.on_message failed: {e}", exc_info=True)

    async def on_reaction(self, cog, payload: discord.RawReactionActionEvent):
        for item in self.items.values():
            handler = getattr(item, "on_reaction", None)
            if handler is None:
                continue
            try:
                await handler(cog, payload)
            except Exception as e:
                logger.error(f"{item.key}.on_reaction failed: {e}", exc_info=True)

    async def maintenance(self, cog):
        for item in self.items.values():
            handler = getattr(item, "maintenance", None)
            if handler is None:
                continue
            try:
                await handler(cog)
            except Exception as e:
                logger.error(f"{item.key}.maintenance failed: {e}", exc_info=True)
        for item in self.items.values():
            pruner = getattr(item, "prune_memory", None)
            if pruner is not None:
                try:
                    pruner()
                except Exception as e:
                    logger.warning(f"{item.key}.prune_memory failed: {e}")

        await self._shelf_life_reminders(cog)
        await self._reclaim_stranded(cog)

    async def restore(self, cog):
        for item in self.items.values():
            handler = getattr(item, "restore", None)
            if handler is None:
                continue
            try:
                await handler(cog)
            except Exception as e:
                logger.error(f"{item.key}.restore failed: {e}", exc_info=True)

    # ------------------------------------------------------------- janitor

    async def _reclaim_stranded(self, cog):
        """Return items reserved by an activation flow that never finished.

        A dismissed modal fires no callback at all, so a view's on_timeout
        cannot cover every case. Items legitimately parked in 'active' — a live
        HoF permission window, or a submission awaiting admin approval — are
        left alone by the NOT EXISTS guards.
        """
        cutoff = int(time.time()) - STRANDED_AFTER
        cursor = await cog.db.execute(
            "UPDATE inventory SET state = 'owned', activated_ts = NULL "
            "WHERE state = 'active' "
            "  AND COALESCE(activated_ts, purchased_ts) < ? "
            "  AND NOT EXISTS (SELECT 1 FROM approvals a "
            "                   WHERE a.inventory_id = inventory.id "
            "                     AND a.status = 'pending') "
            "  AND NOT EXISTS (SELECT 1 FROM perm_grants g "
            "                   WHERE g.inventory_id = inventory.id)",
            (cutoff,),
        )
        if cursor.rowcount:
            logger.info(
                f"Reclaimed {cursor.rowcount} item(s) stranded by an abandoned flow."
            )

    # --------------------------------------------------------- shelf life

    async def _shelf_life_reminders(self, cog):
        """DM owners of unused shelf-life items at 7 and 1 days remaining."""
        now = int(time.time())
        horizon = now + REMINDER_DAYS[0] * 86400
        rows = await cog.db.fetchall(
            "SELECT * FROM inventory WHERE state = 'owned' AND expires_ts IS NOT NULL "
            "AND expires_ts > ? AND expires_ts <= ?",
            (now, horizon),
        )
        for row in rows:
            remaining_days = (row["expires_ts"] - now) / 86400
            try:
                sent = set(json.loads(row["reminded"] or "[]"))
            except (TypeError, ValueError):
                sent = set()

            for threshold in REMINDER_DAYS:
                if remaining_days > threshold or threshold in sent:
                    continue
                item = self.get(row["item_key"])
                if item is not None:
                    label = getattr(item, "name", None) or row["item_key"]
                    when = "tomorrow" if threshold == 1 else f"in {threshold} days"
                    await item.dm(
                        row["user_id"],
                        f"Your **{label}** expires {when} "
                        f"(<t:{row['expires_ts']}:R>). Open `/eco_shop` and use it "
                        f"before it is gone.",
                    )
                sent.add(threshold)
                await cog.db.execute(
                    "UPDATE inventory SET reminded = ? WHERE id = ?",
                    (json.dumps(sorted(sent)), row["id"]),
                )
                break


def register_all(cog) -> ItemRegistry:
    """Build the registry and register the persistent approval buttons."""
    from ..approvals import ApprovalButton
    try:
        cog.bot.add_dynamic_items(ApprovalButton)
    except Exception as e:
        # Re-registering on a cog reload is harmless.
        logger.debug(f"add_dynamic_items: {e}")
    return ItemRegistry(cog)
