"""Timed effects — the curses and the reaction items.

Every effect lives in the active_effects table with an absolute expiry, so a
restart never loses one and never leaves one running forever. The expiry loop
in the cog calls `expire_tick`, which also performs each effect's cleanup
(restoring a hijacked nickname, for instance).
"""

import asyncio
import contextlib
import json
import logging
import random
import time

import discord

logger = logging.getLogger('cogs.economy.effects')

CLOWN_EMOJI = "\U0001F921"  # clown face

# Reaction rate limiting. Reactions sit in a tight per-channel bucket, and a
# chatty cursed user would otherwise make the bot 429 itself — which stalls the
# shared HTTP queue for every other request the bot makes, not just this cog.
REACTION_BURST = 5          # reactions allowed back-to-back per target
REACTION_REFILL = 12.0      # seconds to earn one more


class TokenBucket:
    """Small per-key rate limiter. Refuses rather than queues."""

    def __init__(self, burst: int, refill_seconds: float):
        self.burst = burst
        self.refill = refill_seconds
        self._state: dict = {}      # key -> (tokens, last_refill_ts)

    def take(self, key) -> bool:
        now = time.monotonic()
        tokens, last = self._state.get(key, (float(self.burst), now))
        tokens = min(self.burst, tokens + (now - last) / self.refill)
        if tokens < 1.0:
            self._state[key] = (tokens, now)
            return False
        self._state[key] = (tokens - 1.0, now)
        return True

    def prune(self, max_age: float = 3600.0):
        now = time.monotonic()
        for key, (_, last) in list(self._state.items()):
            if now - last > max_age:
                del self._state[key]


def alternating_caps(text: str) -> str:
    """sPoNgEbOb cAsE — flips on every letter, leaving other characters alone."""
    out, upper = [], False
    for char in text:
        if char.isalpha():
            out.append(char.upper() if upper else char.lower())
            upper = not upper
        else:
            out.append(char)
    return "".join(out)


class EffectsManager:
    def __init__(self, cog):
        self.cog = cog
        self.bot = cog.bot
        self.db = cog.db
        self.config = cog.config
        # target_id -> unix ts of last allowed message, for Slowmo.
        self._slowmo_last: dict = {}
        # target_id -> unix ts of last "you're slowed" DM, to avoid DM spam.
        self._slowmo_notified: dict = {}
        # Nicknames we are enforcing, so our own edit doesn't trigger a revert.
        self._nick_enforcing: set = set()
        self._reactions = TokenBucket(REACTION_BURST, REACTION_REFILL)
        # Cache of "this user has no active effects", so the common case costs
        # no query at all. Invalidated whenever an effect is added or removed.
        self._clean: dict = {}

    # ------------------------------------------------------------- queries

    async def effects_for(self, user_id: int, key: str = None) -> list:
        return await self.db.get_effects(target_id=user_id, effect_key=key)

    async def active_curses(self, user_id: int = None) -> list:
        """Every live curse, optionally narrowed to one victim."""
        from .config import CURSE_KEYS
        rows = await self.db.get_effects(target_id=user_id)
        return [r for r in rows if r["effect_key"] in CURSE_KEYS]

    # --------------------------------------------------------- application

    async def apply(self, effect_key: str, owner_id: int, target_id: int,
                    duration_seconds: int, *, data: dict = None,
                    inventory_id: int = None) -> int:
        expires = int(time.time()) + duration_seconds
        effect_id = await self.db.add_effect(
            owner_id, target_id, effect_key, expires,
            data=data or {}, inventory_id=inventory_id,
        )
        self.invalidate(target_id)
        logger.info(f"Applied {effect_key} to {target_id} for {duration_seconds}s "
                    f"(owner {owner_id}).")
        return effect_id

    async def apply_nickname_hijack(self, guild: discord.Guild, owner_id: int,
                                    target: discord.Member, new_nick: str,
                                    duration_seconds: int, inventory_id: int = None):
        original = target.nick
        effect_id = await self.apply(
            "nickname_hijack", owner_id, target.id, duration_seconds,
            data={"new_nick": new_nick, "original_nick": original},
            inventory_id=inventory_id,
        )
        await self._set_nick(target, new_nick)
        return effect_id

    async def _set_nick(self, member: discord.Member, nick):
        """Change a nickname without tripping our own revert listener."""
        self._nick_enforcing.add(member.id)
        try:
            await member.edit(nick=nick, reason="Economy: Nickname Hijack")
        except discord.Forbidden:
            logger.warning(f"Missing permission to rename {member.id}.")
        except discord.HTTPException as e:
            logger.warning(f"Failed to rename {member.id}: {e}")
        finally:
            # Give the gateway a moment to deliver our own update before we
            # police changes again — but do not hold the caller for it, or a
            # batch of expiries would stall the maintenance loop 1.5s each.
            asyncio.create_task(self._clear_enforcing_later(member.id))

    async def _clear_enforcing_later(self, member_id: int, delay: float = 1.5):
        try:
            await asyncio.sleep(delay)
        finally:
            self._nick_enforcing.discard(member_id)

    async def clear_effect(self, effect_row) -> str:
        """Remove one effect and run its cleanup. Returns a human label."""
        key = effect_row["effect_key"]
        if key == "nickname_hijack":
            await self._restore_nickname(effect_row)
        await self.db.remove_effect(effect_row["id"])
        self._slowmo_last.pop(effect_row["target_id"], None)
        self.invalidate(effect_row["target_id"])
        return key

    async def _restore_nickname(self, effect_row):
        try:
            data = json.loads(effect_row["data"] or "{}")
        except (TypeError, ValueError):
            data = {}
        for guild in self.bot.guilds:
            member = guild.get_member(effect_row["target_id"])
            if member:
                await self._set_nick(member, data.get("original_nick"))
                return

    # ------------------------------------------------------------- expiry

    async def expire_tick(self):
        """Drop everything past its expiry, running each effect's cleanup."""
        now = int(time.time())
        rows = await self.db.fetchall(
            "SELECT * FROM active_effects WHERE expires_ts <= ?", (now,)
        )
        for row in rows:
            try:
                await self.clear_effect(row)
            except Exception as e:
                logger.warning(f"Failed to expire effect {row['id']}: {e}")

        # Inventory items past their 90-day shelf life.
        await self.db.execute(
            "UPDATE inventory SET state = 'expired' WHERE state = 'owned' "
            "AND expires_ts IS NOT NULL AND expires_ts <= ?", (now,)
        )

        self.prune_memory()

    def prune_memory(self):
        """Shed per-user bookkeeping so a long-uptime bot does not grow forever."""
        wall = time.time()
        mono = time.monotonic()

        for cache, cutoff in (
            (self._slowmo_last, wall - 24 * 3600),
            (self._slowmo_notified, wall - 3600),
        ):
            for key, stamp in list(cache.items()):
                if stamp < cutoff:
                    del cache[key]

        for key, until in list(self._clean.items()):
            if until < mono:
                del self._clean[key]

        self._reactions.prune()

    async def restore_on_ready(self):
        """Re-assert enforced nicknames after a restart."""
        rows = await self.db.get_effects(effect_key="nickname_hijack")
        for row in rows:
            try:
                data = json.loads(row["data"] or "{}")
            except (TypeError, ValueError):
                continue
            for guild in self.bot.guilds:
                member = guild.get_member(row["target_id"])
                if member and member.nick != data.get("new_nick"):
                    await self._set_nick(member, data.get("new_nick"))
                    break

    # ------------------------------------------------------ message hooks

    def _mark_clean(self, user_id: int, until: float):
        self._clean[user_id] = until

    def invalidate(self, user_id: int = None):
        """Drop the no-effects cache after an effect is applied or cleared."""
        if user_id is None:
            self._clean.clear()
        else:
            self._clean.pop(user_id, None)

    async def handle_message(self, message: discord.Message) -> bool:
        """Run every message-driven effect. Returns True if the message was deleted."""
        user_id = message.author.id

        # Almost nobody is cursed at any given moment, so remember who is not
        # and skip the query entirely. Short TTL keeps it self-correcting even
        # if an invalidation is ever missed.
        clean_until = self._clean.get(user_id)
        if clean_until is not None and time.monotonic() < clean_until:
            return False

        effects = await self.db.get_effects(target_id=user_id)
        if not effects:
            self._mark_clean(user_id, time.monotonic() + 300)
            return False
        self._clean.pop(user_id, None)

        by_key = {}
        for row in effects:
            by_key.setdefault(row["effect_key"], row)

        # Slowmo runs first — if it deletes the message, nothing else applies.
        if "slowmo" in by_key:
            if await self._handle_slowmo(message, by_key["slowmo"]):
                return True

        if "auto_react" in by_key:
            await self._handle_auto_react(message, by_key["auto_react"])

        if "clown_mode" in by_key:
            await self._react(message, CLOWN_EMOJI, "clown_mode")

        if "spongebob" in by_key:
            await self._handle_spongebob(message)

        return False

    async def _handle_slowmo(self, message: discord.Message, row) -> bool:
        interval = self.config.get_int("slowmo_interval_minutes", 5) * 60
        now = time.time()
        last = self._slowmo_last.get(message.author.id)

        if last is not None and now - last < interval:
            remaining = int(interval - (now - last))
            try:
                await message.delete()
            except discord.HTTPException:
                return False
            await self._notify_slowmo(message.author, remaining)
            return True

        self._slowmo_last[message.author.id] = now
        return False

    async def _notify_slowmo(self, user, remaining: int):
        # One DM per interval at most, so the curse annoys without spamming.
        now = time.time()
        if now - self._slowmo_notified.get(user.id, 0) < 45:
            return
        self._slowmo_notified[user.id] = now
        minutes, seconds = divmod(max(0, remaining), 60)
        with contextlib.suppress(discord.HTTPException):
            await user.send(
                f"You're under a **Slowmo** curse — one message every "
                f"{self.config.get_int('slowmo_interval_minutes', 5)} minutes. "
                f"Your message was removed. Try again in **{minutes}m {seconds}s**."
            )

    async def _react(self, message: discord.Message, emoji: str, label: str) -> bool:
        """Add a reaction, subject to the per-target budget."""
        if not self._reactions.take(message.author.id):
            logger.debug(
                f"{label}: reaction budget spent for {message.author.id}, skipping"
            )
            return False
        try:
            await message.add_reaction(emoji)
            return True
        except discord.HTTPException as e:
            # Emoji unavailable (deleted, or from a server the bot has left),
            # or the bot cannot react here. Logged rather than swallowed —
            # otherwise a broken emoji silently wastes the whole 24h.
            logger.warning(f"{label}: could not react with {emoji!r}: {e}")
            return False

    async def _handle_auto_react(self, message: discord.Message, row):
        try:
            data = json.loads(row["data"] or "{}")
        except (TypeError, ValueError):
            return
        emoji = data.get("emoji")
        if emoji:
            await self._react(message, emoji, "auto_react")

    async def _handle_spongebob(self, message: discord.Message):
        chance = self.config.get_int("spongebob_chance", 5)
        if chance <= 0 or random.randint(1, 100) > chance:
            return
        content = (message.content or "").strip()
        if not content or len(content) > 300:
            return
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send(
                alternating_caps(content),
                allowed_mentions=discord.AllowedMentions.none(),
            )

    # ------------------------------------------------------- member hooks

    async def handle_member_update(self, before: discord.Member, after: discord.Member):
        """Revert nickname changes while a hijack is live."""
        if before.nick == after.nick or after.id in self._nick_enforcing:
            return
        rows = await self.db.get_effects(target_id=after.id, effect_key="nickname_hijack")
        if not rows:
            return
        try:
            data = json.loads(rows[0]["data"] or "{}")
        except (TypeError, ValueError):
            return
        enforced = data.get("new_nick")
        if enforced and after.nick != enforced:
            await self._set_nick(after, enforced)

    async def handle_member_join(self, member: discord.Member):
        """Re-apply a hijack to someone who left and came back."""
        rows = await self.db.get_effects(target_id=member.id, effect_key="nickname_hijack")
        if not rows:
            return
        try:
            data = json.loads(rows[0]["data"] or "{}")
        except (TypeError, ValueError):
            return
        if data.get("new_nick"):
            await self._set_nick(member, data["new_nick"])
