"""Verify media qualification and reaction rule decisions.

These are the two hot-path decisions in the cog and both were wrong before:
a media channel deleted link posts, and every reaction cost a fetch_message
whether or not the rule could match.

Run: .venv/bin/python tests/utility/check_features.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cogs.utility.features import media, reactions

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


class Rule(dict):
    """A stand-in for an sqlite3.Row — same subscript access."""

    DEFAULTS = {
        "allow_attachments": 1, "allow_links": 1, "allow_embeds": 1,
        "allow_stickers": 1, "bypass_role_ids": "[]", "thread_enabled": 1,
        "thread_name_template": "{user} - {date}", "thread_archive_minutes": 60,
        "auto_react": "[]", "post_cooldown_s": 0, "dm_on_delete": 0,
        "scope": "all", "mode": "remove", "role_ids": "[]", "user_ids": "[]",
        "emoji": "[]", "remove_after_s": 0, "max_reactions": 0,
        "include_threads": 1, "enabled": 1,
    }

    def __init__(self, **overrides):
        super().__init__({**self.DEFAULTS, **overrides})


class Msg:
    def __init__(self, content="", attachments=(), embeds=(), stickers=(),
                 role_mentions=(), author_id=1):
        self.content = content
        self.attachments = list(attachments)
        self.embeds = list(embeds)
        self.stickers = list(stickers)
        self.raw_role_mentions = list(role_mentions)
        self.author = type("A", (), {"id": author_id, "display_name": "Ash",
                                     "name": "ash"})()
        self.channel = type("C", (), {"name": "clips", "id": 5})()
        self.reactions = []


class Emoji:
    def __init__(self, name, eid=None):
        self.name, self.id = name, eid

    def __str__(self):
        return f"<:{self.name}:{self.id}>" if self.id else self.name


print("\nMedia qualification")
permissive = Rule()
check(media.qualifies_as_media(Msg(attachments=[object()]), permissive),
      "an attachment qualifies")
check(media.qualifies_as_media(Msg(content="https://tenor.com/view/abc"), permissive),
      "a Tenor link qualifies — the old cog deleted these")
check(media.qualifies_as_media(Msg(content="check https://youtu.be/x out"), permissive),
      "a link mid-sentence qualifies")
check(media.qualifies_as_media(Msg(stickers=[object()]), permissive),
      "a sticker qualifies")
check(media.qualifies_as_media(Msg(embeds=[object()]), permissive),
      "an embed qualifies")
check(not media.qualifies_as_media(Msg(content="nice clip"), permissive),
      "plain text does not qualify")
check(not media.qualifies_as_media(Msg(content=""), permissive),
      "an empty message does not qualify")

strict = Rule(allow_links=0, allow_embeds=0, allow_stickers=0)
check(not media.qualifies_as_media(Msg(content="https://tenor.com/x"), strict),
      "links rejected when the channel disallows them")
check(media.qualifies_as_media(Msg(attachments=[object()]), strict),
      "attachments still fine when links are off")

print("\nBypass roles")


class Member:
    def __init__(self, *role_ids):
        self.roles = [type("R", (), {"id": r})() for r in role_ids]


check(media._has_bypass(Member(7), [7]), "a matching bypass role matches")
check(media._has_bypass(Member(1, 7), ["7"]), "bypass ids compare across types")
check(not media._has_bypass(Member(1), [7]), "a non-matching role does not")
check(not media._has_bypass(Member(7), []), "no bypass roles configured means no bypass")

print("\nThread naming")
name = media.render_thread_name("{user} - {date}", Msg())
check(name.startswith("Ash - 20"), f"template renders: {name!r}")
check(media.render_thread_name("", Msg()).startswith("Ash"),
      "an empty template falls back to a usable name")
check(len(media.render_thread_name("{user}" * 60, Msg())) <= 100,
      "an over-long name is truncated to Discord's limit")

print("\nCooldown")
cooled = Rule(post_cooldown_s=60)
check(media.cooldown_remaining(cooled, 5, 1) == 0, "no cooldown before a first post")
media.mark_posted(5, 1)
check(media.cooldown_remaining(cooled, 5, 1) > 55, "cooldown applies after posting")
check(media.cooldown_remaining(cooled, 5, 2) == 0, "cooldown is per user")
check(media.cooldown_remaining(Rule(), 5, 1) == 0, "no cooldown when disabled")

print("\nReaction: does the rule need the message?")
check(not reactions.needs_message(Rule(scope="all")),
      "scope 'all' decides without a fetch — this is the API saving")
check(reactions.needs_message(Rule(scope="role_mention")),
      "role_mention needs the message")
check(reactions.needs_message(Rule(scope="from_user")),
      "from_user needs the message")
check(reactions.needs_message(Rule(scope="all", max_reactions=3)),
      "a reaction cap needs the message even at scope 'all'")

print("\nReaction: scope matching")
check(reactions.scope_matches(Rule(scope="all"), None),
      "scope 'all' matches with no message")
check(reactions.scope_matches(Rule(scope="role_mention", role_ids="[9]"),
                              Msg(role_mentions=[9])),
      "role_mention matches a pinged role")
check(not reactions.scope_matches(Rule(scope="role_mention", role_ids="[9]"),
                                  Msg(role_mentions=[3])),
      "role_mention ignores other pings")
check(reactions.scope_matches(Rule(scope="from_user", user_ids="[4]"),
                              Msg(author_id=4)),
      "from_user matches the author")
check(not reactions.scope_matches(Rule(scope="from_user", user_ids="[4]"),
                                  Msg(author_id=5)),
      "from_user ignores other authors")

print("\nReaction: modes")
star, fire = Emoji("⭐"), Emoji("🔥")
custom = Emoji("pog", 12345)

check(reactions.should_remove(Rule(mode="remove"), star),
      "'remove' strips everything")

allow = Rule(mode="allowlist", emoji='["⭐"]')
check(not reactions.should_remove(allow, star), "allowlist keeps a listed emoji")
check(reactions.should_remove(allow, fire), "allowlist removes an unlisted emoji")
check(reactions.should_remove(Rule(mode="allowlist", emoji="[]"), star),
      "an empty allowlist permits nothing")

block = Rule(mode="blocklist", emoji='["🔥"]')
check(reactions.should_remove(block, fire), "blocklist removes a listed emoji")
check(not reactions.should_remove(block, star), "blocklist keeps everything else")

by_id = Rule(mode="blocklist", emoji='["12345"]')
check(reactions.should_remove(by_id, custom),
      "a custom emoji matches by id, so a rename doesn't break the rule")
by_full = Rule(mode="blocklist", emoji='["<:pog:12345>"]')
check(reactions.should_remove(by_full, custom),
      "a custom emoji also matches its full <:name:id> form")

print("\nReaction: emoji keys")
check(reactions.emoji_key(custom) == "12345", "custom emoji key is its id")
check(reactions.emoji_key(star) == "⭐", "unicode emoji key is the character")

print("\n" + "=" * 60)
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S):")
    for failure in FAILURES:
        print(f"  - {failure}")
    sys.exit(1)
print("All feature checks passed.")
