"""Economy configuration — every tunable value the admin panel edits.

Settings live in economy.db's `settings` table as strings and are cached in
memory. Nothing here is hardcoded at the call site: the panel writes a key,
`Config.set` refreshes the cache, and the next read sees the new value.
"""

import json
import logging
import typing

logger = logging.getLogger('cogs.economy.config')


# --------------------------------------------------------------------------
# Item + source catalogues
# --------------------------------------------------------------------------

# Shop items, in display order. `shelf_days` is the 90-day use-it-or-lose-it
# window; None means the item never expires while unused.
SHOP_ITEMS = {
    "auto_react": {
        "name": "Auto React",
        "blurb": "For 24h the bot adds an emoji of your choice to every message a target sends.",
        "default_price": 500,
        "shelf_days": None,
    },
    "hof_post": {
        "name": "HoF Post",
        "blurb": "Post one message of your choosing into the Hall of Fame channel.",
        "default_price": 1200,
        "shelf_days": None,
    },
    "gif_command": {
        "name": "Add GIF Command",
        "blurb": "Create your own !command that posts a GIF. Admin approval required.",
        "default_price": 800,
        "shelf_days": None,
    },
    "add_emoji": {
        "name": "Add Emoji",
        "blurb": "Add an emoji to the server for a month. Earns a permanent slot if it catches on.",
        "default_price": 1500,
        "shelf_days": None,
    },
    "throne": {
        "name": "The Throne",
        "blurb": "Seize the 1-of-1 Throne role by outbidding whoever holds it.",
        "default_price": 2000,
        "shelf_days": None,
    },
    "customs_match": {
        "name": "Customs Match",
        "blurb": "Open a custom match queue for a single match. Non-refundable if it never fills.",
        "default_price": 1000,
        "shelf_days": None,
    },
    "proxy": {
        "name": "Proxy",
        "blurb": "Send one message anonymously through Vibey.",
        "default_price": 600,
        "shelf_days": None,
    },
    "mystery_box": {
        "name": "Mystery Box",
        "blurb": "One random outcome. Some are rewards. Some very much are not.",
        "default_price": 750,
        "shelf_days": None,
    },
}

# Items that only ever come out of a Mystery Box — never sold directly.
BOX_ITEMS = {
    "nickname_hijack": {
        "name": "Nickname Hijack",
        "blurb": "Rename one person for 24h. They cannot change it back.",
        "shelf_days": 90,
    },
    "curse_wipe": {
        "name": "Curse Wipe",
        "blurb": "Clear one active curse, from yourself or anyone else.",
        "shelf_days": 90,
    },
}

ALL_ITEMS = {**SHOP_ITEMS, **BOX_ITEMS}

# Mystery Box outcomes with their default weights. Weights are relative; the
# panel shows each as a normalised percentage.
BOX_OUTCOMES = {
    "vibes_role": {"name": "Vibes Role", "weight": 2, "kind": "reward"},
    "curse_wipe": {"name": "Curse Wipe", "weight": 14, "kind": "reward"},
    "nickname_hijack": {"name": "Nickname Hijack", "weight": 14, "kind": "reward"},
    "points_small": {"name": "Points Payout (small)", "weight": 20, "kind": "reward"},
    "points_big": {"name": "Points Payout (big)", "weight": 5, "kind": "reward"},
    "clown_mode": {"name": "Clown Mode", "weight": 17, "kind": "curse"},
    "slowmo": {"name": "Slowmo", "weight": 17, "kind": "curse"},
    "spongebob": {"name": "SpongeBob Case", "weight": 11, "kind": "curse"},
}

# Curses that Curse Wipe can clear.
CURSE_KEYS = ("clown_mode", "slowmo", "spongebob", "nickname_hijack")

# Points earning sources. `cap` of 0 means uncapped; `once_daily` sources are
# gated by a per-day claim flag instead of a running total.
EARNING_SOURCES = {
    "message": {"name": "Message sent", "value": 1, "cap": 50, "once_daily": False},
    "first_message": {"name": "First message of the day", "value": 15, "cap": 0, "once_daily": True},
    "streak7": {"name": "7-day streak bonus", "value": 100, "cap": 0, "once_daily": True},
    "newcomer_reply": {"name": "Reply to a newcomer", "value": 5, "cap": 25, "once_daily": False},
    "trivia": {"name": "Daily trivia played", "value": 25, "cap": 0, "once_daily": True},
    "daily_quote": {"name": "Daily quote played", "value": 25, "cap": 0, "once_daily": True},
    "qotd": {"name": "QOTD answered", "value": 25, "cap": 0, "once_daily": True},
    "custom_match": {"name": "Custom match played", "value": 50, "cap": 0, "once_daily": False},
    "game_night": {"name": "Game night attended", "value": 75, "cap": 0, "once_daily": True},
}


# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------

def _build_defaults() -> dict:
    d = {
        # Leveling
        "lvl_msg_xp_min": 15,
        "lvl_msg_xp_max": 25,
        "lvl_xp_cooldown": 60,
        "lvl_voice_xp_per_min": 5,
        "lvl_excluded_channels": [],

        # Roles
        "role_throne": 0,
        "role_vibes": 0,
        "role_newcomer": 0,

        # Channels
        "ch_approval": 0,
        "ch_hof": 0,
        "ch_proxy_log": 0,
        "ch_match_queue": 0,
        "proxy_blocklist": [],

        # Item behaviour
        "hof_grant_minutes": 15,
        "proxy_max_chars": 500,
        "emoji_trial_days": 30,
        "auto_react_hours": 24,
        "clown_hours": 24,
        "slowmo_minutes": 60,
        "slowmo_interval_minutes": 5,
        "spongebob_days": 7,
        "spongebob_chance": 5,
        "nickname_hijack_hours": 24,
        "match_queue_hours": 3,
        "box_points_small": 250,
        "box_points_big": 1500,

        # GIF command tiers — each slot costs more than the last.
        "price_gif_1": 800,
        "price_gif_2": 1600,
        "price_gif_3": 3200,
        "gif_max_per_user": 3,
    }

    for key, meta in SHOP_ITEMS.items():
        d[f"price_{key}"] = meta["default_price"]
        d[f"enabled_{key}"] = 1

    for key, meta in EARNING_SOURCES.items():
        d[f"earn_{key}_value"] = meta["value"]
        d[f"earn_{key}_cap"] = meta["cap"]

    for key, meta in BOX_OUTCOMES.items():
        d[f"mbox_{key}_weight"] = meta["weight"]
        d[f"mbox_{key}_enabled"] = 1

    return d


DEFAULTS = _build_defaults()


class Config:
    """Cached settings accessor. Reads never hit the database after load."""

    def __init__(self, db):
        self.db = db
        self._cache: dict = {}

    async def load(self):
        self._cache = await self.db.all_settings()
        logger.info(f"Economy config loaded ({len(self._cache)} stored overrides).")

    def _raw(self, key: str):
        if key in self._cache:
            return self._cache[key]
        return DEFAULTS.get(key)

    def get_int(self, key: str, fallback: int = 0) -> int:
        value = self._raw(key)
        if value is None:
            return fallback
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    def get_str(self, key: str, fallback: str = "") -> str:
        value = self._raw(key)
        return fallback if value is None else str(value)

    def get_bool(self, key: str, fallback: bool = False) -> bool:
        value = self._raw(key)
        if value is None:
            return fallback
        return str(value) not in ("0", "False", "false", "")

    def get_list(self, key: str) -> list:
        """JSON list setting. Stored as a JSON string, defaulted as a real list."""
        value = self._raw(key)
        if isinstance(value, list):
            return list(value)
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError):
            return []

    async def set(self, key: str, value):
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        elif isinstance(value, bool):
            value = int(value)
        await self.db.set_setting(key, value)
        self._cache[key] = str(value)

    # ------------------------------------------------------------ shortcuts

    def price(self, item_key: str) -> int:
        return self.get_int(f"price_{item_key}", 0)

    def item_enabled(self, item_key: str) -> bool:
        return self.get_bool(f"enabled_{item_key}", True)

    def earn_value(self, source: str) -> int:
        return self.get_int(f"earn_{source}_value", 0)

    def earn_cap(self, source: str) -> int:
        return self.get_int(f"earn_{source}_cap", 0)

    def gif_price(self, slot: int) -> int:
        """Price for a user's Nth GIF command slot (1-indexed)."""
        slot = max(1, min(3, slot))
        return self.get_int(f"price_gif_{slot}", 0)

    def box_weights(self) -> typing.Dict[str, int]:
        """Enabled Mystery Box outcomes and their weights."""
        return {
            key: self.get_int(f"mbox_{key}_weight", meta["weight"])
            for key, meta in BOX_OUTCOMES.items()
            if self.get_bool(f"mbox_{key}_enabled", True)
            and self.get_int(f"mbox_{key}_weight", meta["weight"]) > 0
        }
