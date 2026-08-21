"""One definition of "is this person a bot admin".

The bot treats two things as admin: the Discord `administrator` permission, and
the configured admin role (see `Vibey.is_bot_admin` in main.py). Several cogs
only ever checked the raw permission, so the admin role silently did not work
in those places — an admin-role holder was an admin in the economy panel but a
stranger to the ticketing cooldown bypass.

Route checks through here so that stays consistent. The bot's own method is
still the source of truth; these helpers just find it and fall back safely
when it is unavailable (a detached view, a unit test, a cog loaded standalone).
"""

from __future__ import annotations

from typing import Any, Optional


def _has_admin_permission(user: Any) -> bool:
    perms = getattr(user, "guild_permissions", None)
    return bool(perms is not None and perms.administrator)


def is_bot_admin(user: Any, client: Optional[Any] = None) -> bool:
    """True if `user` is a Discord administrator or holds the bot admin role.

    `client` is the bot instance. When it is missing or predates
    `is_bot_admin`, this degrades to the plain permission check rather than
    locking everyone out.
    """
    if user is None:
        return False
    checker = getattr(client, "is_bot_admin", None) if client is not None else None
    if checker is not None:
        try:
            return bool(checker(user))
        except (AttributeError, TypeError):
            # Not a guild member (DM context), or an unexpected shape.
            return _has_admin_permission(user)
    return _has_admin_permission(user)


def is_admin_interaction(interaction: Any) -> bool:
    """`is_bot_admin` for an interaction, using its own client."""
    if interaction is None:
        return False
    return is_bot_admin(
        getattr(interaction, "user", None),
        getattr(interaction, "client", None),
    )
