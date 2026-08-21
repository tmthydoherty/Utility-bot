"""One normalised shape for every event an automation can fire on.

`on_message` hands you a Message, `on_member_join` a Member, `member_level_up`
three bare integers. Conditions and actions should not each have to know which
of those they are looking at, so every listener builds an `EventContext` and
the engine only ever sees this.

`extra` carries whatever is specific to the trigger — the old and new level,
the ticket's topic, the role that was added — and conditions read it by name.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, Optional

import discord


@dataclass
class EventContext:
    event: str
    bot: Any = None
    guild: Optional[discord.Guild] = None
    member: Optional[discord.Member] = None
    user: Optional[discord.abc.User] = None
    channel: Optional[Any] = None
    message: Optional[discord.Message] = None
    extra: Dict[str, Any] = dc_field(default_factory=dict)

    # Per-run scratch space: regex captures, counter reads, `set variable`.
    variables: Dict[str, Any] = dc_field(default_factory=dict)
    # The last message the bot sent during this run, so a later step can react
    # to it, pin it or thread it. Nothing else can reach a message the bot
    # itself created — no event fires for the bot's own posts.
    last_sent: Optional[discord.Message] = None
    # Composition depth, so an automation chain cannot recurse forever.
    depth: int = 0
    # True when the engine must not touch Discord — the dry-run and test paths.
    simulate: bool = False

    @classmethod
    def from_message(cls, bot, message: discord.Message, event: str = "message_sent"):
        # `roles` is what distinguishes a guild member from a plain user or a
        # webhook author, and is the thing every role action actually needs.
        author = message.author
        return cls(
            event=event, bot=bot, guild=message.guild,
            member=author if hasattr(author, "roles") else None,
            user=author, channel=message.channel, message=message,
        )

    @classmethod
    def from_member(cls, bot, member: discord.Member, event: str, **extra):
        return cls(event=event, bot=bot, guild=member.guild, member=member,
                   user=member, extra=extra)

    @property
    def user_id(self) -> int:
        target = self.member or self.user
        return target.id if target else 0

    @property
    def channel_id(self) -> int:
        return self.channel.id if self.channel else 0

    @property
    def guild_id(self) -> int:
        return self.guild.id if self.guild else 0

    def is_bot_actor(self) -> bool:
        target = self.member or self.user
        return bool(target and getattr(target, "bot", False))

    def cooldown_key(self, scope: str) -> str:
        if scope == "channel":
            return f"c:{self.channel_id}"
        if scope == "guild":
            return f"g:{self.guild_id}"
        return f"u:{self.user_id}"

    def child(self, depth_increment: int = 1) -> "EventContext":
        """A copy for a nested automation, carrying variables but its own depth."""
        return EventContext(
            event=self.event, bot=self.bot, guild=self.guild, member=self.member,
            user=self.user, channel=self.channel, message=self.message,
            extra=dict(self.extra), variables=dict(self.variables),
            depth=self.depth + depth_increment, simulate=self.simulate,
        )

    def describe(self) -> str:
        """A short line for the run log, so history is readable at a glance."""
        bits = [self.event]
        target = self.member or self.user
        if target is not None:
            bits.append(f"by {target}")
        if self.channel is not None:
            name = getattr(self.channel, "name", None)
            if name:
                bits.append(f"in #{name}")
        return " ".join(bits)[:200]
