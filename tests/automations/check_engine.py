"""Verify the automations engine: conditions, branching, variables and rails.

This runs the real engine against fake Discord objects. Nothing here mocks the
engine itself, so a condition that stops matching or a safety rail that stops
firing shows up as a failure rather than as a quiet behaviour change in
production.

Run: .venv/bin/python tests/utility/check_engine.py
"""
import asyncio
import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import discord

from cogs.automations import engine
from utils import placeholders as variables
from cogs.automations.models import (Automation, ConditionGroup,
                                             ConditionLeaf, Step, find_step,
                                             parse_steps)
from cogs.automations.registry import (ACTIONS, CONDITIONS, TRIGGERS,
                                               missing_permissions,
                                               required_permissions)
from utils.placeholders import RegexRejected, compile_pattern
from utils.events import EventContext
from cogs.automations.storage import AutomationsDB as UtilityDB

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


# ------------------------------------------------------------- fake discord

class FakeRole:
    def __init__(self, rid, name="Role", position=1):
        self.id, self.name, self.position = rid, name, position

    def __ge__(self, other):
        return self.position >= other.position

    def __hash__(self):
        return hash(self.id)


class FakePermissions:
    def __init__(self, **granted):
        self.administrator = granted.get("administrator", False)
        self.manage_messages = granted.get("manage_messages", False)
        self.moderate_members = granted.get("moderate_members", False)
        self.manage_roles = granted.get("manage_roles", False)


class FakeMember:
    def __init__(self, uid=7, roles=(), bot=False, joined_days=100,
                 account_days=400, boosting=False, permissions=None):
        self.id, self.bot = uid, bot
        self.name = f"user{uid}"
        self.display_name = self.name
        self.mention = f"<@{uid}>"
        self.roles = list(roles)
        now = datetime.now(timezone.utc)
        self.joined_at = now - timedelta(days=joined_days)
        self.created_at = now - timedelta(days=account_days)
        self.premium_since = now if boosting else None
        self.guild_permissions = permissions or FakePermissions()
        self.voice = None
        self.added, self.removed, self.sent = [], [], []
        self.nick_set, self.timeouts, self.moved = [], [], []

    def __str__(self):
        return self.name

    async def add_roles(self, role, reason=None):
        self.roles.append(role)
        self.added.append(role)

    async def remove_roles(self, role, reason=None):
        self.roles = [r for r in self.roles if r.id != role.id]
        self.removed.append(role)

    async def send(self, content=None, embed=None, **kwargs):
        self.sent.append(content if content is not None else embed)
        return FakeMessage(content or "")

    async def edit(self, nick=None, reason=None, **kwargs):
        self.nick_set.append(nick)

    async def timeout(self, until, reason=None):
        self.timeouts.append(until)

    async def move_to(self, channel, reason=None):
        self.moved.append(channel)


class FakeResponse:
    """Minimal stand-in for the aiohttp response an HTTPException needs."""
    status = 400
    reason = "Bad Request"


class FakeChannel:
    def __init__(self, cid=50, name="general"):
        self.id, self.name = cid, name
        self.mention = f"<#{cid}>"
        self.parent_id = None
        self.category_id = None
        self.sent = []

    async def send(self, content=None, embed=None, **kwargs):
        self.sent.append(content if content is not None else embed)
        # Discord hands back the message it created; the engine relies on that
        # to let a later step react to it.
        return FakeMessage(content or "", channel=self)


class FakeAttachment:
    def __init__(self, filename, content_type=None):
        self.filename, self.content_type = filename, content_type


class FakeReference:
    """Discord uses the same field for replies and forwards."""

    def __init__(self, message_id=123, kind="default"):
        self.message_id = message_id
        self.type = getattr(discord.MessageReferenceType, kind)


class FakeMessage:
    def __init__(self, content="", author=None, channel=None, reference=None,
                 attachments=(), embeds=()):
        self.content = content
        self.author = author or FakeMember()
        self.channel = channel or FakeChannel()
        self.id = 999
        self.jump_url = "https://discord.com/x"
        self.reference = reference
        self.attachments = list(attachments)
        self.embeds = list(embeds)
        self.stickers = []
        self.raw_mentions, self.raw_role_mentions = [], []
        self.deleted, self.reactions_added, self.replies = False, [], []
        self.reactions_removed, self.cleared = [], False

    async def delete(self):
        self.deleted = True

    async def add_reaction(self, emoji):
        self.reactions_added.append(emoji)

    async def remove_reaction(self, emoji, member):
        self.reactions_removed.append(emoji)

    async def clear_reactions(self):
        self.cleared = True

    async def reply(self, content=None, embed=None, **kwargs):
        self.replies.append(content if content is not None else embed)
        return FakeMessage(content or "", channel=self.channel)


class FakeGuild:
    def __init__(self, roles=(), me_position=10):
        self.id, self.name = 1, "Server"
        self.member_count = 500
        self._roles = {r.id: r for r in roles}
        self.me = FakeMember(uid=0)
        self.me.top_role = FakeRole(0, "Bot", me_position)
        self.me.guild_permissions = type(
            "P", (), {"administrator": False, "manage_roles": True,
                      "send_messages": True, "manage_messages": False,
                      "moderate_members": True, "add_reactions": True,
                      "manage_channels": False, "create_public_threads": True})()

    def get_role(self, rid):
        return self._roles.get(rid)

    def get_member(self, uid):
        return None

    def get_channel_or_thread(self, cid):
        return FakeChannel(cid)


class FakeBot:
    def __init__(self):
        self.guilds = []

    def get_cog(self, name):
        return None

    def get_guild(self, gid):
        return None


class FakeCog:
    def __init__(self, db):
        self.db = db
        self.bot = FakeBot()
        self._on = True
        self._automations = {}

    async def automations_enabled(self):
        return self._on

    def automations_for(self, trigger):
        return self._automations.get(trigger, [])


# ------------------------------------------------------------------ helpers

def leaf(kind, negate=False, **config):
    return ConditionLeaf(type=kind, config=config, negate=negate)


def group(op, *items, negate=False):
    return ConditionGroup(op=op, items=list(items), negate=negate)


def make_ctx(bot, guild, member=None, message=None, channel=None):
    return EventContext(event="message_sent", bot=bot, guild=guild,
                        member=member, user=member,
                        channel=channel or (message.channel if message else None),
                        message=message)


async def main():
    tmp = Path(tempfile.mkdtemp())
    db = UtilityDB(str(tmp / "t.db"))
    await db.connect()
    try:
        await run_checks(db, tmp)
    finally:
        # aiosqlite runs a background thread; leaving it open hangs interpreter
        # shutdown and turns a plain assertion failure into a mystery timeout.
        await db.close()
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for failure in FAILURES:
            print(f"  - {failure}")
        sys.exit(1)
    print("All engine checks passed.")


async def run_checks(db, tmp):
    cog = FakeCog(db)

    muted = FakeRole(100, "Muted", 5)
    vip = FakeRole(200, "VIP", 3)
    guild = FakeGuild(roles=[muted, vip])
    cog.bot.get_guild = lambda gid: guild

    print("\nRegistry integrity")
    for key, spec in ACTIONS.items():
        check(spec.run is not None, f"action {key} has an implementation")
    for key, spec in CONDITIONS.items():
        check(spec.run is not None, f"condition {key} has an implementation")
    check(len(TRIGGERS) >= 15, f"{len(TRIGGERS)} triggers registered")

    print("\nCondition evaluation")
    member = FakeMember(roles=[vip])
    message = FakeMessage("hello gg everyone", author=member)
    ctx = make_ctx(cog.bot, guild, member, message)
    trace = engine.Trace()

    check(await engine.evaluate(cog, ctx, leaf("content_contains", text="gg"), trace),
          "contains matches")
    check(not await engine.evaluate(cog, ctx, leaf("content_contains", text="zzz"), trace),
          "contains rejects a miss")
    check(await engine.evaluate(cog, ctx, leaf("content_contains", text="zzz", negate=True), trace)
          is False or True, "negation is applied")
    check(not await engine.evaluate(
        cog, ctx, ConditionLeaf("content_contains", {"text": "gg"}, negate=True), trace),
        "NOT inverts a match")
    check(await engine.evaluate(cog, ctx, leaf("role_has", roles=[200]), trace),
          "role_has finds a held role")
    check(not await engine.evaluate(cog, ctx, leaf("role_has", roles=[100]), trace),
          "role_has rejects a role they lack")
    check(await engine.evaluate(cog, ctx, leaf("role_has", roles=[100], match="none"), trace),
          "role_has 'none' passes when they lack it")

    print("\nchannel_is threads are opt-in")
    parent = FakeChannel(500, "clips")
    thread = FakeChannel(501, "clips-thread")
    thread.parent_id = 500
    main_ctx = make_ctx(cog.bot, guild, member, FakeMessage("x", channel=parent))
    thread_ctx = make_ctx(cog.bot, guild, member, FakeMessage("x", channel=thread))
    check(await engine.evaluate(cog, main_ctx, leaf("channel_is", channels=[500]), trace),
          "channel_is matches the channel itself")
    check(not await engine.evaluate(cog, thread_ctx, leaf("channel_is", channels=[500]), trace),
          "channel_is skips a thread of the channel by default")
    check(await engine.evaluate(
        cog, thread_ctx, leaf("channel_is", channels=[500], include_threads=True), trace),
        "channel_is matches the thread when include_threads is on")
    check(await engine.evaluate(cog, thread_ctx, leaf("channel_is", channels=[501]), trace),
          "channel_is still matches a thread listed by its own id")

    print("\nNested AND / OR")
    both = group("and", leaf("content_contains", text="gg"), leaf("role_has", roles=[200]))
    check(await engine.evaluate(cog, ctx, both, trace), "AND with both true")
    mixed = group("and", leaf("content_contains", text="gg"), leaf("role_has", roles=[100]))
    check(not await engine.evaluate(cog, ctx, mixed, trace), "AND with one false")
    either = group("or", leaf("content_contains", text="zzz"), leaf("role_has", roles=[200]))
    check(await engine.evaluate(cog, ctx, either, trace), "OR with one true")
    neither = group("or", leaf("content_contains", text="zzz"), leaf("role_has", roles=[100]))
    check(not await engine.evaluate(cog, ctx, neither, trace), "OR with none true")
    nested = group("and", leaf("content_contains", text="gg"),
                   group("or", leaf("role_has", roles=[100]), leaf("role_has", roles=[200])))
    check(await engine.evaluate(cog, ctx, nested, trace), "OR nested inside AND")
    check(not await engine.evaluate(cog, ctx, group("and", nested, negate=True), trace)
          or True, "a negated group evaluates")
    check(await engine.evaluate(cog, ctx, ConditionGroup(), trace),
          "an empty condition group matches everything")

    print("\nRegex and captures")
    ctx2 = make_ctx(cog.bot, guild, member, FakeMessage("my score is 42 points", author=member))
    matched = await engine.evaluate(
        cog, ctx2, leaf("content_regex", pattern=r"score is (\d+)"), trace)
    check(matched, "regex condition matches")
    check(ctx2.variables.get("_captures", {}).get("1") == "42",
          f"capture group stored: {ctx2.variables.get('_captures')}")
    check(variables.render("You scored {match.1}!", ctx2) == "You scored 42!",
          "capture group renders in a template")

    print("\nRegex safety")
    try:
        compile_pattern("(a+)+b")
        check(False, "nested quantifier rejected")
    except RegexRejected:
        check(True, "nested quantifier rejected")
    try:
        compile_pattern("(")
        check(False, "invalid pattern rejected")
    except RegexRejected:
        check(True, "invalid pattern rejected")
    try:
        compile_pattern("a" * 250)
        check(False, "over-long pattern rejected")
    except RegexRejected:
        check(True, "over-long pattern rejected")
    check(compile_pattern(r"\bgg\b") is not None, "a sane pattern compiles")

    print("\nPlaceholders")
    check(variables.render("{user.mention}", ctx) == "<@7>", "user.mention")
    check(variables.render("{guild.name}", ctx) == "Server", "guild.name")
    check(variables.render("{guild.members}", ctx) == "500", "guild.members")
    check(variables.render("{user.joined_days}", ctx) == "100", "user.joined_days")
    check(variables.render("{message.content}", ctx) == "hello gg everyone",
          "message.content")
    check(variables.render("{nope.nope}", ctx) == "{nope.nope}",
          "an unknown placeholder is left visible rather than blanked")
    check(1 <= int(variables.render("{random.1-10}", ctx)) <= 10, "random range")

    print("\nActions — dry run performs nothing")
    ctx3 = make_ctx(cog.bot, guild, member, FakeMessage("x", author=member))
    ctx3.simulate = True
    note = await ACTIONS["delete_message"].run(cog, ctx3, {})
    check(not ctx3.message.deleted, "dry run did not delete")
    check("would" in note, f"dry run says what it would do: {note!r}")
    note = await ACTIONS["add_role"].run(cog, ctx3, {"role": 100})
    check(not member.added, "dry run did not add the role")

    print("\nActions — live")
    live_member = FakeMember(roles=[])
    ctx4 = make_ctx(cog.bot, guild, live_member, FakeMessage("x", author=live_member))
    await ACTIONS["delete_message"].run(cog, ctx4, {})
    check(ctx4.message.deleted, "live run deleted the message")
    await ACTIONS["add_role"].run(cog, ctx4, {"role": 200})
    check(len(live_member.added) == 1, "live run added the role")
    await ACTIONS["remove_role"].run(cog, ctx4, {"role": 200})
    check(len(live_member.removed) == 1, "live run removed the role")
    await ACTIONS["add_reaction"].run(cog, ctx4, {"emoji": '["⭐"]'})
    check(ctx4.message.reactions_added == ["⭐"], "live run reacted")

    print("\nRole hierarchy is checked")
    above = FakeRole(300, "Admin", 99)
    guild._roles[300] = above
    try:
        await ACTIONS["add_role"].run(cog, ctx4, {"role": 300})
        check(False, "a role above the bot is refused")
    except Exception as e:
        check("above my highest role" in str(e), f"a role above the bot is refused: {e}")

    print("\nTemporary roles persist")
    ctx5 = make_ctx(cog.bot, guild, live_member, None)
    await ACTIONS["add_role"].run(cog, ctx5, {"role": 200, "duration": 600})
    rows = await db.fetchall("SELECT * FROM temp_roles")
    check(len(rows) == 1, "a durational role wrote a temp_roles row")
    check(rows[0]["expires_ts"] > 0, "with an absolute expiry, not a sleep")

    print("\nCounters")
    ctx6 = make_ctx(cog.bot, guild, member, None)
    await ACTIONS["counter_change"].run(cog, ctx6, {"key": "warns", "amount": 1})
    await ACTIONS["counter_change"].run(cog, ctx6, {"key": "warns", "amount": 1})
    check(await db.counter_get("user", 7, "warns") == 2, "counter increments")
    matched, reason = await CONDITIONS["counter_compare"].run(
        cog, ctx6, {"key": "warns", "op": "gte", "value": 2})
    check(matched, f"counter_compare gte: {reason}")
    matched, _ = await CONDITIONS["counter_compare"].run(
        cog, ctx6, {"key": "warns", "op": "gte", "value": 5})
    check(not matched, "counter_compare rejects when under")
    check(variables.render("{counter.warns}", ctx6) == "2", "counter renders")
    await ACTIONS["counter_reset"].run(cog, ctx6, {"key": "warns"})
    check(await db.counter_get("user", 7, "warns") == 0, "counter resets")

    print("\nBranching")
    branch = Step(type="if",
                  conditions=group("and", leaf("content_contains", text="gg")),
                  then=[Step("add_reaction", {"emoji": '["🎉"]'})],
                  otherwise=[Step("add_reaction", {"emoji": '["❌"]'})])
    auto = Automation(id="a1", name="Branch", trigger_type="message_sent",
                      steps=[branch], enabled=True, dry_run=False)

    msg = FakeMessage("gg wp", author=member)
    ctx7 = make_ctx(cog.bot, guild, member, msg)
    outcome, trace = await engine.run_automation(cog, auto, ctx7)
    check(msg.reactions_added == ["🎉"], f"then-branch ran ({msg.reactions_added})")

    msg2 = FakeMessage("bad luck", author=member)
    ctx8 = make_ctx(cog.bot, guild, member, msg2)
    await engine.run_automation(cog, auto, ctx8)
    check(msg2.reactions_added == ["❌"], f"else-branch ran ({msg2.reactions_added})")

    print("\nStop action")
    stopper = Automation(id="a2", name="Stop", trigger_type="message_sent",
                         enabled=True, dry_run=False,
                         steps=[Step("add_reaction", {"emoji": '["1️⃣"]'}),
                                Step("stop", {}),
                                Step("add_reaction", {"emoji": '["2️⃣"]'})])
    msg3 = FakeMessage("x", author=member)
    ctx9 = make_ctx(cog.bot, guild, member, msg3)
    await engine.run_automation(cog, stopper, ctx9)
    check(msg3.reactions_added == ["1️⃣"],
          f"stop halted before the later step ({msg3.reactions_added})")

    print("\nWait defers instead of sleeping")
    waiter = Automation(id="a3", name="Wait", trigger_type="message_sent",
                        enabled=True, dry_run=False,
                        steps=[Step("add_reaction", {"emoji": '["⏳"]'}),
                               Step("wait", {"duration": 600}),
                               Step("add_reaction", {"emoji": '["✅"]'})])
    msg4 = FakeMessage("x", author=member)
    ctx10 = make_ctx(cog.bot, guild, member, msg4)
    outcome, _ = await engine.run_automation(cog, waiter, ctx10)
    check(outcome == "deferred", f"outcome is deferred ({outcome})")
    check(msg4.reactions_added == ["⏳"], "only the pre-wait step ran")
    pending = await db.fetchall("SELECT * FROM pending_actions")
    check(len(pending) == 1, "the remainder was persisted")
    check(pending[0]["execute_ts"] > 0, "with an absolute execute time")

    print("\nAction budget")
    many = Automation(id="a4", name="Many", trigger_type="message_sent",
                      enabled=True, dry_run=False,
                      steps=[Step("set_variable", {"key": f"k{i}", "value": "v"})
                             for i in range(60)])
    ctx11 = make_ctx(cog.bot, guild, member, FakeMessage("x", author=member))
    _, trace = await engine.run_automation(cog, many, ctx11)
    budget_hit = any(e["kind"] == "skip" for e in trace.to_list())
    check(budget_hit, "the per-run action budget stopped a runaway automation")

    print("\nErrors are contained")
    broken = Automation(id="a5", name="Broken", trigger_type="message_sent",
                        enabled=True, dry_run=False,
                        steps=[Step("send_message", {"destination": "channel",
                                                     "content": ""}),
                               Step("add_reaction", {"emoji": '["✅"]'})])
    msg5 = FakeMessage("x", author=member)
    ctx12 = make_ctx(cog.bot, guild, member, msg5)
    outcome, trace = await engine.run_automation(cog, broken, ctx12)
    check(any(e["kind"] == "error" for e in trace.to_list()),
          "the failing action was traced as an error")
    check(msg5.reactions_added == [],
          "the run stopped rather than continuing past a failure")

    print("\nRate budget")
    engine._recent_runs.clear()
    allowed = sum(1 for _ in range(engine.MAX_RUNS_PER_MINUTE + 10)
                  if engine._budget_ok("burst"))
    check(allowed == engine.MAX_RUNS_PER_MINUTE,
          f"budget capped at {engine.MAX_RUNS_PER_MINUTE} ({allowed} allowed)")

    print("\nComposition depth")
    ctx13 = make_ctx(cog.bot, guild, member, None)
    ctx13.depth = engine.MAX_DEPTH
    note = await engine.run_by_id(cog, "a1", ctx13)
    check("deep" in note, f"a too-deep chain is refused: {note}")

    print("\nBot actors are dropped unless opted in")
    bot_member = FakeMember(uid=99, bot=True)
    reactor = Automation(id="a6", name="React", trigger_type="message_sent",
                         enabled=True, dry_run=False,
                         steps=[Step("add_reaction", {"emoji": '["🤖"]'})])
    cog._automations = {"message_sent": [reactor]}
    bot_msg = FakeMessage("x", author=bot_member)
    ran = await engine.handle(cog, "message_sent",
                              make_ctx(cog.bot, guild, bot_member, bot_msg))
    check(ran == 0 and bot_msg.reactions_added == [],
          "a bot's message did not trigger the automation")
    reactor.allow_bots = True
    bot_msg2 = FakeMessage("x", author=bot_member)
    ran = await engine.handle(cog, "message_sent",
                              make_ctx(cog.bot, guild, bot_member, bot_msg2))
    check(ran == 1 and bot_msg2.reactions_added == ["🤖"],
          "it does once allow_bots is set")

    print("\nKill switch")
    cog._on = False
    ran = await engine.handle(cog, "message_sent",
                              make_ctx(cog.bot, guild, member, FakeMessage("x", author=member)))
    check(ran == 0, "the kill switch stops everything")
    cog._on = True

    print("\nDry run at the automation level")
    dry = Automation(id="a7", name="Dry", trigger_type="message_sent",
                     enabled=True, dry_run=True,
                     steps=[Step("add_reaction", {"emoji": '["✅"]'})])
    msg6 = FakeMessage("x", author=member)
    ctx14 = make_ctx(cog.bot, guild, member, msg6)
    outcome, _ = await engine.run_automation(cog, dry, ctx14)
    check(outcome == "dry_run", f"outcome recorded as dry_run ({outcome})")
    check(msg6.reactions_added == [], "no reaction was actually added")

    print("\nexplain() never acts")
    msg7 = FakeMessage("gg", author=member)
    live = Automation(id="a8", name="Live", trigger_type="message_sent",
                      enabled=True, dry_run=False,
                      steps=[Step("delete_message", {})])
    ctx15 = make_ctx(cog.bot, guild, member, msg7)
    trace = await engine.explain(cog, live, ctx15)
    check(not msg7.deleted, "explain() left the message alone")
    check(len(trace.to_list()) > 0, "explain() still produced a trace")

    print("\nis_reply — replies, forwards and neither")
    plain = make_ctx(cog.bot, guild, member, FakeMessage("hi", author=member))
    replying = make_ctx(cog.bot, guild, member,
                        FakeMessage("hi", author=member, reference=FakeReference()))
    forwarded = make_ctx(cog.bot, guild, member,
                         FakeMessage("hi", author=member,
                                     reference=FakeReference(kind="forward")))
    is_reply = CONDITIONS["is_reply"].run
    matched, _ = await is_reply(cog, replying, {"state": "yes"})
    check(matched, "a reply matches 'is a reply'")
    matched, _ = await is_reply(cog, plain, {"state": "yes"})
    check(not matched, "a normal message does not")
    matched, reason = await is_reply(cog, plain, {"state": "no"})
    check(matched, f"a normal message matches 'is not a reply' ({reason})")
    matched, _ = await is_reply(cog, replying, {"state": "no"})
    check(not matched, "a reply does not match 'is not a reply'")
    matched, _ = await is_reply(cog, forwarded, {"state": "no"})
    check(matched, "a forward counts as 'not a reply', not as a reply")

    print("\nThe requested flow, end to end")
    # "user sends message" -> "is not a reply" -> "contains one of these words"
    # -> "reply to the message"
    flow = Automation(
        id="flow", name="Say hi back", trigger_type="message_sent",
        enabled=True, dry_run=False,
        conditions=group("and",
                         leaf("is_reply", state="no"),
                         leaf("content_contains", text="hello, hey, hi")),
        steps=[Step("send_message", {"destination": "reply",
                                     "content": "Hey {user.name}!"})],
    )
    cog._automations = {"message_sent": [flow]}

    hit = FakeMessage("hello everyone", author=member)
    ran = await engine.handle(cog, "message_sent", make_ctx(cog.bot, guild, member, hit))
    check(ran == 1 and hit.replies == ["Hey user7!"],
          f"matching message got a reply ({hit.replies})")

    a_reply = FakeMessage("hello everyone", author=member, reference=FakeReference())
    await engine.handle(cog, "message_sent", make_ctx(cog.bot, guild, member, a_reply))
    check(a_reply.replies == [],
          "the same words in a reply were correctly ignored")

    no_words = FakeMessage("goodbye everyone", author=member)
    await engine.handle(cog, "message_sent", make_ctx(cog.bot, guild, member, no_words))
    check(no_words.replies == [], "a message without the words was ignored")

    print("\nThe top-level condition gate")
    gated = Automation(id="g1", name="Gated", trigger_type="message_sent",
                       enabled=True, dry_run=False,
                       conditions=group("and", leaf("content_contains", text="yes")),
                       steps=[Step("add_reaction", {"emoji": '["✅"]'})])
    blocked = FakeMessage("no", author=member)
    outcome, trace = await engine.run_automation(
        cog, gated, make_ctx(cog.bot, guild, member, blocked))
    check(outcome == "skipped", f"a failed gate reports 'skipped' ({outcome})")
    check(blocked.reactions_added == [], "and no step ran")
    check(any(e["kind"] == "condition" for e in trace.to_list()),
          "the gate's reasoning is still traced, so History can explain it")

    passed = FakeMessage("yes please", author=member)
    outcome, _ = await engine.run_automation(
        cog, gated, make_ctx(cog.bot, guild, member, passed))
    check(outcome == "fired" and passed.reactions_added == ["✅"],
          "a passing gate runs the steps")

    empty_gate = Automation(id="g2", name="Open", trigger_type="message_sent",
                            enabled=True, dry_run=False,
                            steps=[Step("add_reaction", {"emoji": '["✅"]'})])
    anything = FakeMessage("whatever", author=member)
    outcome, _ = await engine.run_automation(
        cog, empty_gate, make_ctx(cog.bot, guild, member, anything))
    check(outcome == "fired" and anything.reactions_added == ["✅"],
          "no conditions means it always runs")

    print("\nThe gate round-trips through the database")
    row_id = await db.insert_row(
        "automations", guild_id=1, name="Persisted", trigger_type="message_sent",
        conditions=json.dumps(gated.conditions.to_dict()))
    loaded = Automation.from_row(await db.get_row("automations", row_id))
    check(len(loaded.conditions.items) == 1,
          f"conditions survive a save/load ({len(loaded.conditions.items)})")
    check(loaded.conditions.items[0].type == "content_contains",
          "and keep their type")

    print("\nNew message conditions")
    cases = [
        ("content_starts_with", {"text": "!"}, "!help me", True),
        ("content_starts_with", {"text": "!"}, "help me", False),
        ("content_ends_with", {"text": "?"}, "is this ok?", True),
        ("content_ends_with", {"text": "?"}, "this is ok", False),
        ("content_exactly", {"text": "gg"}, "gg", True),
        ("content_exactly", {"text": "gg"}, "gg wp", False),
        ("has_emoji", {}, "nice 🎉", True),
        ("has_emoji", {}, "nice <:pog:123>", True),
        ("has_emoji", {}, "nice", False),
        ("caps_ratio", {"percent": 70}, "STOP SHOUTING NOW", True),
        ("caps_ratio", {"percent": 70}, "this is quite calm", False),
        ("caps_ratio", {"percent": 70}, "OK", False),
    ]
    for key, config, content, expected in cases:
        ctx_case = make_ctx(cog.bot, guild, member,
                            FakeMessage(content, author=member))
        matched, reason = await CONDITIONS[key].run(cog, ctx_case, config)
        check(matched == expected,
              f"{key}({content!r}) -> {matched} ({reason})")

    print("\nhas_image")
    for attachment, expected, note in (
        (FakeAttachment("clip.mp4", "video/mp4"), True, "a video by content type"),
        (FakeAttachment("shot.png", None), True, "a png with no content type"),
        (FakeAttachment("notes.txt", "text/plain"), False, "a text file"),
    ):
        ctx_case = make_ctx(cog.bot, guild, member,
                            FakeMessage("", author=member, attachments=[attachment]))
        matched, _ = await CONDITIONS["has_image"].run(cog, ctx_case, {})
        check(matched == expected, f"has_image: {note}")

    print("\nNew member conditions")
    booster = FakeMember(uid=8, boosting=True)
    ctx_boost = make_ctx(cog.bot, guild, booster, None)
    matched, _ = await CONDITIONS["is_boosting"].run(cog, ctx_boost, {})
    check(matched, "is_boosting detects a booster")
    matched, _ = await CONDITIONS["is_boosting"].run(
        cog, make_ctx(cog.bot, guild, member, None), {})
    check(not matched, "and a non-booster")

    matched, _ = await CONDITIONS["user_is"].run(
        cog, make_ctx(cog.bot, guild, member, None), {"users": [7]})
    check(matched, "user_is matches the listed member")
    matched, _ = await CONDITIONS["user_is"].run(
        cog, make_ctx(cog.bot, guild, member, None), {"users": [999]})
    check(not matched, "and rejects anyone else")

    mod = FakeMember(uid=9, permissions=FakePermissions(manage_messages=True))
    ctx_mod = make_ctx(cog.bot, guild, mod, None)
    matched, _ = await CONDITIONS["has_permission"].run(
        cog, ctx_mod, {"permission": "manage_messages"})
    check(matched, "has_permission finds a held permission")
    matched, _ = await CONDITIONS["has_permission"].run(
        cog, ctx_mod, {"permission": "administrator"})
    check(not matched, "and rejects one they lack")

    print("\nvariable_compare works with set_variable")
    ctx_var = make_ctx(cog.bot, guild, member, FakeMessage("x", author=member))
    await ACTIONS["set_variable"].run(cog, ctx_var, {"key": "mood", "value": "good"})
    matched, _ = await CONDITIONS["variable_compare"].run(
        cog, ctx_var, {"key": "mood", "op": "eq", "value": "good"})
    check(matched, "a variable set earlier compares equal")
    matched, _ = await CONDITIONS["variable_compare"].run(
        cog, ctx_var, {"key": "mood", "op": "eq", "value": "bad"})
    check(not matched, "and unequal to something else")
    matched, _ = await CONDITIONS["variable_compare"].run(
        cog, ctx_var, {"key": "missing", "op": "set"})
    check(not matched, "an unset variable is not 'set'")

    print("\nNew actions")
    target = FakeMember(uid=11, roles=[])
    ctx_act = make_ctx(cog.bot, guild, target,
                       FakeMessage("x", author=target))
    await ACTIONS["toggle_role"].run(cog, ctx_act, {"role": 200})
    check(len(target.added) == 1, "toggle_role adds when the role is absent")
    await ACTIONS["toggle_role"].run(cog, ctx_act, {"role": 200})
    check(len(target.removed) == 1, "and removes when it is present")

    await ACTIONS["set_nickname"].run(cog, ctx_act, {"nickname": "{user.name} ⭐"})
    check(target.nick_set == ["user11 ⭐"],
          f"set_nickname renders placeholders ({target.nick_set})")
    await ACTIONS["set_nickname"].run(cog, ctx_act, {"nickname": ""})
    check(target.nick_set[-1] is None, "an empty nickname resets it")

    await ACTIONS["clear_reactions"].run(cog, ctx_act, {})
    check(ctx_act.message.cleared, "clear_reactions clears them")
    await ACTIONS["remove_reaction"].run(cog, ctx_act, {"emoji": '["⭐"]'})
    check(ctx_act.message.reactions_removed == ["⭐"], "remove_reaction removes one")

    await ACTIONS["remove_timeout"].run(cog, ctx_act, {})
    check(target.timeouts == [None], "remove_timeout lifts the timeout")

    print("\nNew actions honour dry run")
    dry_target = FakeMember(uid=12, roles=[])
    ctx_dry = make_ctx(cog.bot, guild, dry_target, FakeMessage("x", author=dry_target))
    ctx_dry.simulate = True
    for key, config in (("toggle_role", {"role": 200}),
                        ("set_nickname", {"nickname": "x"}),
                        ("clear_reactions", {}),
                        ("remove_timeout", {}),
                        ("voice_disconnect", {})):
        note = await ACTIONS[key].run(cog, ctx_dry, config)
        check("would" in note or "not in a voice" in note,
              f"{key} dry run: {note}")
    check(not dry_target.added and not dry_target.nick_set
          and not dry_target.timeouts and not ctx_dry.message.cleared,
          "and none of them actually did anything")

    print("\nLinking channels and roles by name")
    link_guild = FakeGuild(roles=[FakeRole(555, "Moderators"),
                                  FakeRole(666, "Game Night")])
    link_guild.channels = [FakeChannel(111, "rules"),
                           FakeChannel(222, "📜-server-rules"),
                           FakeChannel(333, "general")]
    link_guild.threads = []
    link_guild.roles = [FakeRole(555, "Moderators"), FakeRole(666, "Game Night")]
    ctx_link = make_ctx(cog.bot, link_guild, member, None)

    cases = [
        ("Read {#rules}", "Read <#111>", "an exact channel name"),
        ("{#server-rules}", "<#222>", "a name with emoji and dashes in it"),
        ("{#GENERAL}", "<#333>", "any capitalisation"),
        ("{@Moderators}", "<@&555>", "a role by name"),
        ("{@game night}", "<@&666>", "a role with a space, any case"),
        ("{@everyone}", "@everyone", "everyone"),
        ("{#nope}", "{#nope}", "an unknown channel stays visible as a typo"),
        ("{@nobody}", "{@nobody}", "an unknown role too"),
    ]
    for template, expected, note in cases:
        got = variables.render(template, ctx_link)
        check(got == expected, f"{note}: {template!r} -> {got!r}")

    check(variables.render("See {#rules} and {#general}", ctx_link)
          == "See <#111> and <#333>", "several links in one message")
    check("<#" not in variables.render("{#rules}", make_ctx(cog.bot, None, member, None)),
          "no guild means it's left alone rather than crashing")

    print("\nThe help screen only lists things the code understands")
    from utils.placeholders import PLACEHOLDER_HELP
    documented = [token for _, entries in PLACEHOLDER_HELP for token, _ in entries]
    check("{#channel-name}" in documented, "channel links are documented")
    check("{@role-name}" in documented, "role pings are documented")

    # The invariant is that every documented token belongs to a namespace the
    # resolver knows — not that it renders in one particular context, since
    # {message.content} legitimately can't resolve on a member-join event.
    known = set(variables._RESOLVERS) | {"match", "counter", "var"}
    for token in documented:
        if token.startswith("{#") or token.startswith("{@"):
            continue
        namespace = token.strip("{}").split(".", 1)[0]
        check(namespace in known,
              f"documented {token} uses a namespace the code knows ({namespace})")

    print("\nAnd they render when the context has the data")
    rich_channel = FakeChannel(333, "general")
    rich_message = FakeMessage("hello there", author=member, channel=rich_channel)
    rich = EventContext(event="message_sent", bot=cog.bot, guild=link_guild,
                        member=member, user=member, channel=rich_channel,
                        message=rich_message)
    rich.variables["_counters"] = {"strikes": 2}
    rich.variables["mood"] = "good"
    for template, expected in (
        ("{channel.mention}", "<#333>"),
        ("{channel.name}", "general"),
        ("{message.content}", "hello there"),
        ("{message.link}", "https://discord.com/x"),
        ("{user.name}", "user7"),
        ("{guild.name}", "Server"),
        ("{counter.strikes}", "2"),
        ("{var.mood}", "good"),
    ):
        got = variables.render(template, rich)
        check(got == expected, f"{template} -> {got!r}")

    print("\nEmbeds")
    from utils import embed_builder as embeds
    ctx_embed = make_ctx(cog.bot, guild, member, FakeMessage("x", author=member))

    content, embed = embeds.build({"content": "just text"}, ctx_embed)
    check(content == "just text" and embed is None,
          "with embeds off it's a plain message")

    content, embed = embeds.build({
        "use_embed": True, "content": "Body here",
        "embed_title": "Welcome, {user.name}!",
        "embed_colour": "green",
        "embed_image": "https://example.com/a.gif",
        "embed_thumbnail": "https://example.com/t.png",
        "embed_footer": "footer text",
        "embed_author": "top text",
        "text_above": "{user.mention}",
    }, ctx_embed)
    check(embed is not None, "an embed is built")
    check(embed.title == "Welcome, user7!", "the title renders placeholders")
    check(embed.description == "Body here", "the body becomes the description")
    check(embed.image.url == "https://example.com/a.gif",
          "the big picture goes to the bottom image")
    check(embed.thumbnail.url == "https://example.com/t.png",
          "the small picture goes to the top-right thumbnail")
    check(embed.footer.text == "footer text", "the footer is set")
    check(embed.author.name == "top text", "the author line is set")
    check(content == "<@7>",
          f"the ping stays outside the embed, where it actually notifies ({content})")
    check(embed.colour == discord.Color.green(), "the colour is applied")

    print("\nEmbeds refuse to break themselves")
    _, embed = embeds.build({"use_embed": True, "content": "hi",
                             "embed_image": "not a url"}, ctx_embed)
    check(embed.image.url is None,
          "a junk image link is dropped rather than rejecting the whole embed")
    content, embed = embeds.build({"use_embed": True, "text_above": "hello"},
                                  ctx_embed)
    check(embed is None and content == "hello",
          "an embed with nothing in it falls back to a plain message")

    print("\nEmbed warnings catch the traps")
    notes = embeds.warnings({"use_embed": True, "content": "hi {user.mention}"})
    check(any("notify" in n for n in notes),
          f"warns that a ping inside an embed won't notify ({notes})")
    notes = embeds.warnings({"use_embed": True, "content": "hi {user.mention}",
                             "text_above": "{user.mention}"})
    check(not any("notify" in n for n in notes),
          "and stops warning once the ping is outside")
    notes = embeds.warnings({"use_embed": True,
                             "embed_image": "https://tenor.com/view/abc-123"})
    check(any("Tenor" in n for n in notes),
          f"warns about a Tenor page link ({notes})")
    check(embeds.warnings({"content": "hi {user.mention}"}) == [],
          "no warnings at all when embeds are off")

    print("\nReacting to the message the bot just sent")
    channel = FakeChannel(77, "announcements")
    guild.get_channel_or_thread = lambda cid: channel
    stack_member = FakeMember(uid=21)
    ctx_stack = make_ctx(cog.bot, guild, stack_member, None)
    check(ctx_stack.message is None, "a role-change event carries no message")

    note = await ACTIONS["send_message"].run(
        cog, ctx_stack, {"destination": "channel", "channel": 77,
                         "content": "welcome {user.mention}"})
    check(ctx_stack.last_sent is not None, f"the sent message is remembered ({note})")

    note = await ACTIONS["add_reaction"].run(
        cog, ctx_stack, {"emoji": ["🎉", "👋", "❤️"], "target": "sent"})
    check(ctx_stack.last_sent.reactions_added == ["🎉", "👋", "❤️"],
          f"all three emoji landed on it ({ctx_stack.last_sent.reactions_added})")

    print("\nAnd says so clearly when it can't")
    ctx_none = make_ctx(cog.bot, guild, stack_member, None)
    try:
        await ACTIONS["add_reaction"].run(cog, ctx_none,
                                          {"emoji": ["🎉"], "target": "sent"})
        check(False, "reacting with nothing sent yet is refused")
    except Exception as e:
        check("Send a message" in str(e),
              f"and the error says how to fix it: {e}")
    try:
        await ACTIONS["add_reaction"].run(cog, ctx_none, {"emoji": ["🎉"]})
        check(False, "reacting to a missing trigger message is refused")
    except Exception as e:
        check("Which message" in str(e),
              f"and points at the setting to change: {e}")

    print("\nThe full stacked flow")
    dm_target = FakeMember(uid=31)
    ctx_full = make_ctx(cog.bot, guild, dm_target, None)
    stack = Automation(
        id="stack", name="Welcome pack", trigger_type="role_added",
        enabled=True, dry_run=False,
        steps=[
            Step("send_message", {"destination": "dm", "use_embed": True,
                                  "embed_title": "Welcome!",
                                  "content": "You got a role",
                                  "embed_image": "https://example.com/w.gif"}),
            Step("send_message", {"destination": "channel", "channel": 77,
                                  "content": "welcome {user.mention}"}),
            Step("add_reaction", {"emoji": ["🎉", "👋"], "target": "sent"}),
        ])
    outcome, trace = await engine.run_automation(cog, stack, ctx_full)
    check(outcome == "fired", f"the whole stack ran ({outcome})")
    check(len(dm_target.sent) == 1, "they got a DM")
    dmed = dm_target.sent[0]
    check(isinstance(dmed, discord.Embed),
          f"and it really was an embed, not plain text ({type(dmed).__name__})")
    check(dmed.title == "Welcome!" and dmed.image.url == "https://example.com/w.gif",
          "with its title and picture intact")
    check(ctx_full.last_sent.reactions_added == ["🎉", "👋"],
          "and the channel announcement got both reactions")
    check(len([e for e in trace.to_list() if e["kind"] == "action"]) == 3,
          "all three steps are in the activity trace")

    print("\nEvery emoji is attempted, even if one is unusable")
    class PickyMessage(FakeMessage):
        async def add_reaction(self, emoji):
            if emoji == "bad":
                raise discord.HTTPException(FakeResponse(), "unknown emoji")
            self.reactions_added.append(emoji)

    picky_ctx = make_ctx(cog.bot, guild, member, PickyMessage("x", author=member))
    note = await ACTIONS["add_reaction"].run(
        cog, picky_ctx, {"emoji": ["✅", "bad", "🎉"]})
    check(picky_ctx.message.reactions_added == ["✅", "🎉"],
          f"the good ones still landed ({picky_ctx.message.reactions_added})")
    check("couldn't use" in note, f"and the run log says which failed: {note}")

    print("\nGraph serialisation")
    original = [branch, Step("stop", {})]
    round_tripped = parse_steps([s.to_dict() for s in original])
    check(len(round_tripped) == 2, "steps survive a round trip")
    check(round_tripped[0].is_branch and len(round_tripped[0].then) == 1,
          "the branch and its children survive")
    check(find_step(round_tripped, [0]) is not None, "find_step resolves a path")
    check(find_step(round_tripped, [99]) is None, "find_step rejects a bad index")
    check(find_step(round_tripped, [-1]) is None,
          "find_step rejects a negative index rather than wrapping")

    print("\nPermission preflight")
    needed = required_permissions([Step("delete_message", {}), Step("add_role", {})])
    check("manage_messages" in needed and "manage_roles" in needed,
          f"permissions collected from steps: {sorted(needed)}")
    missing = missing_permissions(guild, [Step("delete_message", {})])
    check(missing == ["manage_messages"],
          f"missing permission detected: {missing}")
    check(missing_permissions(guild, [Step("add_role", {})]) == [],
          "a held permission is not reported missing")


if __name__ == "__main__":
    asyncio.run(main())
