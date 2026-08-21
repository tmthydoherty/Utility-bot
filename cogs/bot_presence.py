"""Bot Presence — the status dot and activity line Vibey shows on its profile.

Discord gives a bot a single presence across every server it is in, so this is
the one setting that is genuinely bot-wide rather than per-guild. It is edited
from the web dashboard's Bot Presence module and persisted here so it survives a
restart.

Wiring to the dashboard is the shared config bridge (utils/module_config_sync.py),
the same mechanism welcome/security/suggestions use. Because the setting is
global, both sides key it under a single fixed scope ("0", never a real guild id):
the dashboard writes overrides there, this cog applies them and republishes its
current values under the same scope so the website reads back reality.
"""

import discord
from discord.ext import commands
import logging
from typing import Any, Dict, Optional

from utils.config_store import get_store
from utils.module_config_sync import ConfigSyncAgent

logger = logging.getLogger('cogs.bot_presence')

CONFIG_FILE = "bot_presence_config.json"
MODULE = "bot-presence"
# The bridge stores this module's one row under a single scope, since the
# presence is bot-wide. Matches GLOBAL_SCOPE in dashboard/lib/bot/module-config.ts.
GLOBAL_SCOPE = "0"

# The field keys, verbatim, from the dashboard schema (lib/schema/modules.ts).
DEFAULTS: Dict[str, Any] = {
    "status": "online",
    "activity_type": "watching",
    "activity_text": "Better Vibes",
    "stream_url": "",
}
SYNC_KEYS = tuple(DEFAULTS.keys())

_STATUS = {
    "online": discord.Status.online,
    "idle": discord.Status.idle,
    "dnd": discord.Status.dnd,
    "invisible": discord.Status.invisible,
}
_ACTIVITY_TYPES = {
    "playing": discord.ActivityType.playing,
    "listening": discord.ActivityType.listening,
    "watching": discord.ActivityType.watching,
    "competing": discord.ActivityType.competing,
}


def _resolve_status(value: Any) -> discord.Status:
    return _STATUS.get(str(value), discord.Status.online)


def _resolve_activity(cfg: Dict[str, Any]) -> Optional[discord.BaseActivity]:
    """Turn the stored fields into a discord activity, or None for no line."""
    kind = str(cfg.get("activity_type") or "none")
    text = (cfg.get("activity_text") or "").strip()

    if kind == "none":
        return None
    if kind == "custom":
        # A custom status is only its text; an empty one shows nothing.
        return discord.CustomActivity(name=text) if text else None
    if kind == "streaming":
        url = (cfg.get("stream_url") or "").strip() or None
        return discord.Streaming(name=text or "Streaming", url=url)

    activity_type = _ACTIVITY_TYPES.get(kind)
    if activity_type is None or not text:
        return None
    return discord.Activity(type=activity_type, name=text)


class BotPresence(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._sync = ConfigSyncAgent(MODULE, self._sync_snapshot, self._sync_apply, bot=bot)

    async def cog_load(self):
        await self._sync.start()

    def cog_unload(self):
        self._sync.stop()

    # ------------------------------------------------------------------ config

    async def _current(self) -> Dict[str, Any]:
        """The stored presence config, with any missing key filled from defaults."""
        store = await get_store(CONFIG_FILE)
        saved = await store.read()
        return {key: saved.get(key, default) for key, default in DEFAULTS.items()}

    async def _apply_presence(self, cfg: Dict[str, Any]) -> None:
        """Push the given config to Discord as the bot's live presence."""
        status = _resolve_status(cfg.get("status"))
        activity = _resolve_activity(cfg)
        await self.bot.change_presence(status=status, activity=activity)
        logger.info(
            "Bot presence set to status=%s activity=%s",
            cfg.get("status"), cfg.get("activity_type"),
        )

    @commands.Cog.listener()
    async def on_ready(self):
        """Restore the saved presence once the bot has connected."""
        try:
            await self._apply_presence(await self._current())
        except Exception as e:
            logger.error("Failed to apply bot presence on ready: %r", e, exc_info=True)

    # --- Dashboard sync (see utils/module_config_sync.py) ---

    async def _sync_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Publish the one global row so the website reads back what's live."""
        return {GLOBAL_SCOPE: await self._current()}

    async def _sync_apply(self, guild_id: str, values: Dict[str, Any]):
        """Adopt values saved on the dashboard and apply them at once.

        The setting is bot-wide, so ``guild_id`` is always the global scope and is
        not used to key anything — a save from any server changes the one presence.
        """
        store = await get_store(CONFIG_FILE)
        cfg = await store.read()
        for key in SYNC_KEYS:
            if key in values:
                cfg[key] = values[key]
        await store.write(cfg)
        await self._apply_presence({key: cfg.get(key, default) for key, default in DEFAULTS.items()})


async def setup(bot: commands.Bot):
    await bot.add_cog(BotPresence(bot))
