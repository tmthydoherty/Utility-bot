"""The Custom Commands cog — MEE6-style ``!`` commands, plus the economy's
purchased GIF commands, served from one place.

Why one place: both a member-bought GIF command and an admin-authored custom
command are just ``!word`` triggers matched in ``on_message``. Two cogs each
matching ``!word`` would double-post on a name they both knew and could never
agree on precedence. So this cog owns the whole ``!`` namespace: it serves the
admin commands from its own database, and it reads the economy's approved GIF
commands out of ``economy.db`` and serves those too. The economy cog still sells
and approves GIF commands — it just no longer serves them (see
``cogs/economy/items/gif_command.py``).

Everything is configured from the web dashboard, which writes this cog's
database directly and bumps ``settings['revision']``; ``dashboard_sync`` watches
that integer and reloads. There is no Discord admin panel.

``on_message`` runs for every message in the server, so the hot path is all
in-memory: a dict of exact commands keyed by name, a short list of keyword
responders, and a dict of GIF names — rebuilt only when the dashboard writes or
the economy approves a new GIF, never per message.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

import discord
from discord.ext import commands, tasks

from . import engine
from .storage import CustomCommandsDB, loads

logger = logging.getLogger('cogs.custom_commands')

# A leading ``!word``. Names are 2–32 of the same characters the economy's GIF
# commands allow, so the two namespaces line up. Anything after the word (args)
# is ignored — a GIF command is `!apple`, full stop.
EXACT_RE = re.compile(r"^!([a-zA-Z0-9_]{2,32})(?:\s|$)")

# Seconds between GIF posts in one channel, so spamming `!dance` can't flood the
# channel or burn the bot's rate limit. Carried over from the economy cog, which
# used to serve these.
GIF_COOLDOWN = 8.0

# Custom-command responses are admin-authored, so a role ping in one is
# intentional — but @everyone never is, whoever typed the trigger.
ALLOWED_MENTIONS = discord.AllowedMentions(everyone=False, roles=True, users=True)


class CustomCommands(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = CustomCommandsDB()
        self._ready = False
        # guild_id -> {name: row} for exact `!name` commands.
        self._exact: dict[int, dict[str, dict]] = {}
        # guild_id -> [row, ...] for startswith / contains responders.
        self._keyword: dict[int, list[dict]] = {}
        # name -> url for enabled economy GIF commands (global, like the shop).
        self._gif: dict[str, str] = {}
        # Per-command cooldown gate: "command_id:scope_key" -> monotonic stamp.
        self._cooldowns: dict[str, float] = {}
        # Per-channel GIF gate, mirroring the economy cog's old behaviour.
        self._gif_cooldowns: dict[int, float] = {}
        self._revision = 0

    # ------------------------------------------------------------ lifecycle

    async def cog_load(self):
        await self.db.connect()
        await self.refresh_commands()
        self._revision = await self.db.get_revision()
        self.dashboard_sync.start()
        self._ready = True
        total = sum(len(v) for v in self._exact.values()) + sum(
            len(v) for v in self._keyword.values())
        logger.info(f"Custom Commands ready — {total} command(s) loaded.")

    async def cog_unload(self):
        self.dashboard_sync.cancel()
        await self.db.close()

    # ---------------------------------------------------------------- cache

    async def refresh_commands(self):
        """Rebuild the in-memory command tables from the database."""
        exact: dict[int, dict[str, dict]] = {}
        keyword: dict[int, list[dict]] = {}
        for row in await self.db.all_commands():
            if not row["enabled"]:
                continue
            gid = row["guild_id"]
            if row["match_type"] == "exact":
                exact.setdefault(gid, {})[row["name"]] = dict(row)
            else:
                keyword.setdefault(gid, []).append(dict(row))
        self._exact = exact
        self._keyword = keyword

    async def _refresh_gifs(self):
        """Reload the economy's approved GIF commands.

        Read every tick, not only on a dashboard revision, because the economy
        cog approves new GIFs on its own schedule — a revision bump would never
        fire for one. Disabled GIFs (moderated off from the dashboard) are left
        out so they simply stop responding.
        """
        econ = self.bot.get_cog("Economy")
        if econ is None or getattr(econ, "db", None) is None:
            return
        try:
            rows = await econ.db.fetchall(
                "SELECT name, url, COALESCE(disabled, 0) AS disabled FROM gif_commands")
            self._gif = {r["name"]: r["url"] for r in rows if not r["disabled"]}
        except Exception as e:
            # The database may not be connected yet, or predate the `disabled`
            # column. Either way, try again next tick rather than crash the loop.
            logger.debug(f"Could not refresh GIF commands: {e}")

    async def _apply_gif_moderation(self):
        """Carry out disable/enable/delete intents the dashboard recorded.

        The website cannot write economy.db (it sits outside the dashboard
        sandbox's writable paths), so it leaves intents in `gif_moderation` and
        we apply them here through the Economy cog's own connection, then clear
        each row. An intent that can't be applied yet — economy not loaded —
        is simply left for the next tick.
        """
        pending = await self.db.pending_gif_moderation()
        if not pending:
            return
        econ = self.bot.get_cog("Economy")
        if econ is None or getattr(econ, "db", None) is None:
            return
        for row in pending:
            name, action = row["name"], row["action"]
            try:
                if action == "disable":
                    await econ.db.execute(
                        "UPDATE gif_commands SET disabled = 1 WHERE name = ?", (name,))
                elif action == "enable":
                    await econ.db.execute(
                        "UPDATE gif_commands SET disabled = 0 WHERE name = ?", (name,))
                elif action == "delete":
                    await econ.db.execute(
                        "DELETE FROM gif_commands WHERE name = ?", (name,))
                else:
                    logger.warning(f"Unknown GIF moderation action {action!r} for {name!r}")
                await self.db.clear_gif_moderation(name)
            except Exception as e:
                logger.error(f"GIF moderation {action} for {name!r} failed: {e}")

    # ------------------------------------------------------ dashboard bridge

    @tasks.loop(seconds=10)
    async def dashboard_sync(self):
        try:
            await self._apply_gif_moderation()
            await self._refresh_gifs()
            revision = await self.db.get_revision()
            if revision != self._revision:
                self._revision = revision
                await self.refresh_commands()
                logger.info(f"Custom Commands reloaded for dashboard revision {revision}.")
        except Exception as e:
            logger.error(f"Custom Commands dashboard sync failed: {e}", exc_info=True)

    @dashboard_sync.before_loop
    async def _before_sync(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------------- helpers

    def _is_reserved(self, name: str) -> bool:
        """True if a real bot command already owns this name or alias — so a
        custom `!help` never shadows the built-in one and double-fires."""
        taken = {c.name for c in self.bot.commands}
        taken |= {a for c in self.bot.commands for a in c.aliases}
        return name in taken

    @staticmethod
    def _channel_ids(message: discord.Message) -> set[int]:
        """The message's channel and, for a thread, its parent — so a channel
        gate set on a forum or text channel also covers threads under it."""
        ids = {message.channel.id}
        parent_id = getattr(message.channel, "parent_id", None)
        if parent_id:
            ids.add(parent_id)
        return ids

    def _passes_gates(self, message: discord.Message, row: dict) -> bool:
        member = message.author
        role_ids = {r.id for r in getattr(member, "roles", [])}
        chan_ids = self._channel_ids(message)

        allowed_ch = set(loads(row["allowed_channel_ids"], []))
        if allowed_ch and not (chan_ids & {int(c) for c in allowed_ch}):
            return False
        denied_ch = set(loads(row["denied_channel_ids"], []))
        if denied_ch and (chan_ids & {int(c) for c in denied_ch}):
            return False

        allowed_roles = set(loads(row["allowed_role_ids"], []))
        if allowed_roles and not (role_ids & {int(r) for r in allowed_roles}):
            return False
        denied_roles = set(loads(row["denied_role_ids"], []))
        if denied_roles and (role_ids & {int(r) for r in denied_roles}):
            return False
        return True

    def _cooldown_ok(self, message: discord.Message, row: dict) -> bool:
        """Check-and-set the per-command cooldown. Returns False while cooling."""
        seconds = row["cooldown_s"] or 0
        if seconds <= 0:
            return True
        scope = row["cooldown_scope"]
        if scope == "channel":
            scope_key = message.channel.id
        elif scope == "guild":
            scope_key = message.guild.id
        else:
            scope_key = message.author.id
        key = f"{row['id']}:{scope_key}"
        now = time.monotonic()
        last = self._cooldowns.get(key, 0.0)
        if now - last < seconds:
            return False
        self._cooldowns[key] = now
        return True

    def _build_embed(self, row: dict, message: discord.Message,
                     use_count: int) -> Optional[discord.Embed]:
        data = loads(row["embed_json"], None)
        if not data:
            return None

        def sub(text):
            return engine.substitute(text, message, use_count) if text else text

        embed = discord.Embed(
            title=sub(data.get("title")) or None,
            description=sub(data.get("description")) or None,
            url=data.get("url") or None,
        )
        color = data.get("color")
        if isinstance(color, int):
            embed.color = discord.Color(color)
        if data.get("image_url"):
            embed.set_image(url=data["image_url"])
        if data.get("thumbnail_url"):
            embed.set_thumbnail(url=data["thumbnail_url"])
        author = data.get("author") or {}
        if author.get("name"):
            embed.set_author(
                name=author["name"],
                url=author.get("url") or None,
                icon_url=author.get("icon_url") or None,
            )
        footer = data.get("footer") or {}
        if footer.get("text"):
            embed.set_footer(text=footer["text"], icon_url=footer.get("icon_url") or None)
        for field in data.get("fields") or []:
            name = field.get("name")
            value = sub(field.get("value"))
            if name and value:
                embed.add_field(name=name, value=value, inline=bool(field.get("inline")))
        return embed

    # ----------------------------------------------------------- listeners

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not self._ready or message.author.bot or message.guild is None:
            return
        stripped = (message.content or "").strip()
        if not stripped:
            return
        gid = message.guild.id

        # 1. Exact `!word` — an admin command first, then an economy GIF.
        match = EXACT_RE.match(stripped)
        if match:
            name = match.group(1).lower()
            if not self._is_reserved(name):
                row = self._exact.get(gid, {}).get(name)
                if row is not None:
                    await self._run_command(message, row)
                    return
                url = self._gif.get(name)
                if url is not None:
                    await self._serve_gif(message, name, url)
                    return

        # 2. Keyword responders — first one that matches wins.
        keyword_rows = self._keyword.get(gid)
        if keyword_rows:
            lowered = stripped.lower()
            for row in keyword_rows:
                kw = row["name"]
                hit = (
                    (row["match_type"] == "startswith" and lowered.startswith(kw))
                    or (row["match_type"] == "contains" and kw in lowered)
                )
                if hit and await self._run_command(message, row):
                    return

    # ------------------------------------------------------------ serving

    async def _run_command(self, message: discord.Message, row: dict) -> bool:
        """Serve one custom command. Returns True only if it actually replied,
        so the keyword loop keeps trying other rules when this one is gated
        off or cooling down."""
        if not self._passes_gates(message, row):
            return False
        if not self._cooldown_ok(message, row):
            return False

        use_count = row["use_count"]
        content = ""
        if row["plain_text"] or not row["embed_json"]:
            responses = loads(row["responses_json"], [])
            content = engine.substitute(engine.pick_response(responses), message, use_count)
        embed = None if row["plain_text"] else self._build_embed(row, message, use_count)
        # A command with no text and no embed has nothing to say; treat it as a
        # miss rather than sending an empty message.
        if not content and embed is None:
            return False

        try:
            await self._deliver(message, row, content, embed)
        except discord.Forbidden:
            return False
        except discord.HTTPException as e:
            logger.warning(f"Custom command {row['name']!r} failed to send: {e}")
            return False

        await self._after(message, row)
        await self.db.bump_use(row["id"])
        # Keep the cached count in step so {count} advances between dashboard
        # reloads, not only after one. The dashboard reads the true total from
        # the database, so its analytics stay exact regardless.
        row["use_count"] = use_count + 1
        return True

    async def _deliver(self, message: discord.Message, row: dict,
                       content: str, embed: Optional[discord.Embed]):
        delivery = row["delivery"]
        if delivery == "dm":
            try:
                await message.author.send(content or None, embed=embed)
            except discord.Forbidden:
                # DMs closed — nothing to do, and not worth cluttering the
                # channel with an error the member didn't ask for.
                pass
            return
        if delivery == "reply":
            await message.reply(
                content or None, embed=embed,
                allowed_mentions=ALLOWED_MENTIONS, mention_author=False)
            return
        await message.channel.send(
            content or None, embed=embed, allowed_mentions=ALLOWED_MENTIONS)

    async def _after(self, message: discord.Message, row: dict):
        """React to the trigger and delete it, if the command asks for either."""
        if row["react_emoji"]:
            try:
                await message.add_reaction(row["react_emoji"])
            except (discord.HTTPException, discord.Forbidden) as e:
                logger.debug(f"Could not react for {row['name']!r}: {e}")
        if row["delete_trigger"]:
            try:
                await message.delete()
            except (discord.HTTPException, discord.Forbidden) as e:
                logger.debug(f"Could not delete trigger for {row['name']!r}: {e}")

    async def _serve_gif(self, message: discord.Message, name: str, url: str):
        """Post an economy-purchased GIF, subject to a per-channel cooldown."""
        now = time.monotonic()
        last = self._gif_cooldowns.get(message.channel.id, 0.0)
        if now - last < GIF_COOLDOWN:
            return
        self._gif_cooldowns[message.channel.id] = now
        try:
            await message.channel.send(url, allowed_mentions=discord.AllowedMentions.none())
        except discord.HTTPException as e:
            logger.warning(f"Could not serve GIF !{name}: {e}")


async def setup(bot: commands.Bot):
    await bot.add_cog(CustomCommands(bot))
