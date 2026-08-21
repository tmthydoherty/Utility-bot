"""Verify the Custom Commands engine, storage and dispatch decisions.

Three things carry the feature and none of them touch Discord to be checked:
the template engine (placeholders, random, safe math), the database bridge
(commands round-trip, use counts, the GIF-moderation queue), and the routing in
``on_message`` (exact vs keyword vs purchased GIF, gating, cooldown). All are
exercised here with fakes standing in for a live gateway.

Run: .venv/bin/python tests/custom_commands/check_commands.py
"""
import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cogs.custom_commands import engine
from cogs.custom_commands.cog import CustomCommands, EXACT_RE  # noqa: F401
from cogs.custom_commands.storage import CustomCommandsDB, new_id, now

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


# --------------------------------------------------------------------- fakes

class Role:
    def __init__(self, rid):
        self.id = rid


class Author:
    def __init__(self, aid=1, role_ids=(), bot=False):
        self.id = aid
        self.bot = bot
        self.roles = [Role(r) for r in role_ids]
        self.display_name = "Ash"
        self.mention = f"<@{aid}>"


class Channel:
    def __init__(self, cid=10, parent_id=None):
        self.id = cid
        self.parent_id = parent_id


class Guild:
    def __init__(self, gid=1, member_count=42):
        self.id = gid
        self.name = "Vibe Zone"
        self.member_count = member_count


class Message:
    def __init__(self, content="", author=None, channel=None, guild=None):
        self.content = content
        self.author = author or Author()
        self.channel = channel or Channel()
        self.guild = guild or Guild()


class Cmd:
    def __init__(self, name, aliases=()):
        self.name = name
        self.aliases = list(aliases)


class Bot:
    def __init__(self, commands=()):
        self.commands = list(commands)


def row(**over):
    base = {
        "id": new_id(), "guild_id": 1, "name": "apple", "enabled": 1,
        "match_type": "exact", "responses_json": '["hi"]', "plain_text": 1,
        "embed_json": None, "delivery": "channel", "delete_trigger": 0,
        "react_emoji": "", "allowed_role_ids": "[]", "denied_role_ids": "[]",
        "allowed_channel_ids": "[]", "denied_channel_ids": "[]",
        "cooldown_s": 0, "cooldown_scope": "user", "use_count": 0,
        "last_used_ts": 0, "created_ts": 0, "updated_ts": 0,
    }
    base.update(over)
    return base


# ------------------------------------------------------------------- engine

def test_engine():
    m = Message(guild=Guild(member_count=42), author=Author(aid=7))
    check(engine.substitute("{user}", m, 0) == "<@7>", "{user} expands to a mention")
    check(engine.substitute("{username}", m, 0) == "Ash", "{username} expands to the display name")
    check(engine.substitute("{server}", m, 0) == "Vibe Zone", "{server} expands to the guild name")
    check(engine.substitute("{membercount}", m, 0) == "42", "{membercount} expands to the count")
    check(engine.substitute("{count}", m, 4) == "5", "{count} is the current use number")
    check(engine.substitute("{math:2*21}", m, 0) == "42", "{math} evaluates arithmetic")
    check(engine.substitute("{math:__import__('os')}", m, 0) == "{math:__import__('os')}",
          "{math} refuses anything but arithmetic")
    check(engine.substitute("{unknown}", m, 0) == "{unknown}", "an unknown token is left untouched")
    picks = {engine.substitute("{random:a|b|c}", m, 0) for _ in range(50)}
    check(picks <= {"a", "b", "c"} and len(picks) > 1, "{random} varies across a|b|c")
    check(engine.pick_response(["", "  ", "only"]) == "only", "pick_response skips blank responses")
    check(engine.pick_response([]) == "", "pick_response is empty when there are none")


# ------------------------------------------------------------------ storage

async def _storage():
    with tempfile.TemporaryDirectory() as tmp:
        db = CustomCommandsDB(path=str(Path(tmp) / "cc.db"))
        await db.connect()
        try:
            cols = row(name="banana")
            await db.execute(
                "INSERT INTO commands (id, guild_id, name, enabled, match_type, "
                "responses_json, plain_text, created_ts, updated_ts) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (cols["id"], 1, "banana", 1, "exact", '["b"]', 1, now(), now()),
            )
            await db.execute(
                "INSERT INTO commands (id, guild_id, name, enabled, match_type, "
                "responses_json, plain_text, created_ts, updated_ts) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (new_id(), 1, "apple", 1, "exact", '["a"]', 1, now(), now()),
            )
            rows = await db.all_commands()
            check([r["name"] for r in rows] == ["apple", "banana"],
                  "all_commands returns rows sorted by name")

            await db.bump_use(cols["id"])
            await db.bump_use(cols["id"])
            got = await db.fetchone("SELECT use_count FROM commands WHERE id = ?", (cols["id"],))
            check(got["use_count"] == 2, "bump_use increments the use count")

            # GIF moderation queue
            await db.execute(
                "INSERT INTO gif_moderation (name, action, updated_ts) VALUES (?,?,?)",
                ("dance", "delete", now()),
            )
            pending = await db.pending_gif_moderation()
            check(len(pending) == 1 and pending[0]["action"] == "delete",
                  "a queued GIF-moderation intent is readable")
            await db.clear_gif_moderation("dance")
            check(await db.pending_gif_moderation() == [], "clearing removes the intent")
        finally:
            await db.close()


def test_storage():
    asyncio.run(_storage())


# ------------------------------------------------------------------ dispatch

def _cog():
    cog = CustomCommands(Bot(commands=[Cmd("help"), Cmd("ping", aliases=["p"])]))
    cog._ready = True
    return cog


def test_gates():
    cog = _cog()

    r = row(allowed_role_ids="[100]")
    check(cog._passes_gates(Message(author=Author(role_ids=[100])), r) is True,
          "allowed role present -> passes")
    check(cog._passes_gates(Message(author=Author(role_ids=[999])), r) is False,
          "allowed role absent -> blocked")

    r = row(denied_role_ids="[7]")
    check(cog._passes_gates(Message(author=Author(role_ids=[7])), r) is False,
          "denied role present -> blocked")

    r = row(allowed_channel_ids="[55]")
    check(cog._passes_gates(Message(channel=Channel(cid=55)), r) is True,
          "allowed channel -> passes")
    check(cog._passes_gates(Message(channel=Channel(cid=10)), r) is False,
          "wrong channel -> blocked")
    # A thread resolves to its parent for channel gates.
    check(cog._passes_gates(Message(channel=Channel(cid=999, parent_id=55)), r) is True,
          "thread under an allowed parent -> passes")


def test_cooldown():
    cog = _cog()
    r = row(cooldown_s=60, cooldown_scope="user")
    m = Message(author=Author(aid=1))
    check(cog._cooldown_ok(m, r) is True, "first use passes the cooldown")
    check(cog._cooldown_ok(m, r) is False, "second use inside the window is blocked")
    check(cog._cooldown_ok(Message(author=Author(aid=2)), r) is True,
          "a different person is on their own cooldown")


def test_routing():
    cog = _cog()
    cog._exact = {1: {"apple": row(name="apple")}}
    cog._keyword = {1: [row(name="lol", match_type="contains")]}
    cog._gif = {"dance": "http://gif"}

    served = {"cmd": 0, "gif": None}

    async def fake_run(message, r):
        served["cmd"] += 1
        return True

    async def fake_gif(message, name, url):
        served["gif"] = (name, url)

    cog._run_command = fake_run
    cog._serve_gif = fake_gif

    async def run():
        await cog.on_message(Message(content="!apple"))
        check(served["cmd"] == 1, "!apple routes to the custom command")

        await cog.on_message(Message(content="!dance"))
        check(served["gif"] == ("dance", "http://gif"), "!dance routes to the purchased GIF")

        served["cmd"] = 0
        await cog.on_message(Message(content="that was lol honestly"))
        check(served["cmd"] == 1, "a 'contains' keyword responder fires")

        served["cmd"] = 0
        await cog.on_message(Message(content="!help"))
        check(served["cmd"] == 0, "a reserved name (!help) is never served")

        served["cmd"] = 0
        await cog.on_message(Message(content="hi", author=Author(bot=True)))
        check(served["cmd"] == 0, "a bot's own message is ignored")

    asyncio.run(run())


if __name__ == "__main__":
    test_engine()
    test_storage()
    test_gates()
    test_cooldown()
    test_routing()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) failed.")
        sys.exit(1)
    print("All Custom Commands checks passed.")
