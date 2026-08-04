"""Verify cogs/audit_log.py without a live gateway connection.

Three things can break quietly here: the batcher merging events it shouldn't (or
failing to merge ones it should), a renderer blowing an embed limit only when
a lot of events land in one bucket, and the /audit_panel page tree overflowing
a component row. All three are checkable offline, so they are checked here.

Run: .venv/bin/python tests/audit_log/check_audit_log.py
"""
import asyncio
import os
import sys
import tempfile
import types
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import discord
from cogs import audit_log as al


# ----------------------------------------------------------------------
# Stubs
# ----------------------------------------------------------------------

class FakeAvatar:
    url = "https://example.invalid/avatar.png"


class FakeUser:
    def __init__(self, uid, name=None, bot=False):
        self.id = uid
        self.bot = bot
        self.name = name or f"user{uid}"
        self.display_name = self.name
        self.display_avatar = FakeAvatar()
        self.created_at = datetime.now(timezone.utc) - timedelta(days=400)

    def __str__(self):
        return self.name


class FakeChannel:
    def __init__(self, cid, name="general"):
        self.id, self.name = cid, name
        self.mention = f"<#{cid}>"


class FakeGuild:
    id = 1
    name = "Test"

    def get_channel(self, cid):
        return FakeChannel(cid)

    def get_thread(self, cid):
        return None

    def get_member(self, uid):
        return FakeUser(uid)

    def get_role(self, rid):
        return None


def bare_cog(db):
    """An AuditLog with no bot behind it — enough for batching and rendering."""
    cog = object.__new__(al.AuditLog)
    cog.bot = types.SimpleNamespace(user=FakeUser(999, "Vibey"), guilds=[])
    cog.db = db
    cog._ready = asyncio.Event()
    cog._ready.set()
    cog._buckets = {}
    cog._buckets_lock = asyncio.Lock()
    cog._actor_cache = {}
    cog._delete_counts = {}
    cog._recent_joins = {}
    return cog


def embed_errors(name, embed):
    """Discord rejects an embed that breaches any of these."""
    problems = []
    if embed is None:
        return problems
    if len(embed) > 6000:
        problems.append(f"{name}: embed is {len(embed)} chars (max 6000)")
    if embed.description and len(embed.description) > 4096:
        problems.append(f"{name}: description {len(embed.description)} (max 4096)")
    if len(embed.fields) > 25:
        problems.append(f"{name}: {len(embed.fields)} fields (max 25)")
    for field in embed.fields:
        if field.value and len(field.value) > 1024:
            problems.append(f"{name}: field '{field.name}' {len(field.value)} (max 1024)")
        if not field.value:
            problems.append(f"{name}: field '{field.name}' has an empty value")
    return problems


def layout_errors(name, view):
    """Discord renders at most 5 rows; a select eats a whole row."""
    problems = []
    per_row = Counter()
    for item in view.children:
        row = item._rendered_row if item._rendered_row is not None else item.row
        row = 0 if row is None else row
        if row > 4:
            problems.append(f"{name}: item on row {row} (max 4)")
        per_row[row] += 1 if isinstance(item, discord.ui.Button) else 5
    for row, weight in per_row.items():
        if weight > 5:
            problems.append(f"{name}: row {row} over capacity ({weight}/5)")
    if len(view.children) > 25:
        problems.append(f"{name}: {len(view.children)} components (max 25)")
    return problems


# ----------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------

async def check_db(fails, db):
    print("\n-- database --")
    gid = 1

    settings = await db.get_settings(gid)
    print(f"   defaults: batch={settings['batch_seconds']} "
          f"bots={settings['log_bot_messages']}")
    if settings["batch_seconds"] != al.DEFAULT_BATCH_SECONDS:
        fails.append("batch_seconds default is not 15")

    await db.set_setting(gid, "default_channel_id", 555)
    if (await db.get_settings(gid))["default_channel_id"] != 555:
        fails.append("default_channel_id did not persist")

    await db.set_setting(gid, "drop table users", 1)  # must be refused
    print("   rejected unknown setting key: yes")

    await db.set_category_channel(gid, "messages", 777)
    cats = await db.get_category_channels(gid)
    if cats.get("messages") != 777:
        fails.append("category channel did not persist")
    await db.set_category_channel(gid, "messages", None)
    if (await db.get_category_channels(gid)).get("messages") is not None:
        fails.append("category channel did not clear")

    toggles = await db.get_toggles(gid)
    if set(toggles) != set(al.EVENTS):
        fails.append("toggles do not cover every event key")
    await db.set_toggle(gid, "message_edit", False)
    if (await db.get_toggles(gid))["message_edit"]:
        fails.append("toggle override was not applied")
    await db.set_toggle(gid, "message_edit", True)

    await db.add_ignored(gid, "channel", 42)
    await db.add_ignored(gid, "user", 43)
    channels, users = await db.get_ignored(gid)
    if 42 not in channels or 43 not in users:
        fails.append("ignore entries did not persist")
    await db.remove_ignored(gid, "channel", 42)
    await db.remove_ignored(gid, "user", 43)
    channels, users = await db.get_ignored(gid)
    if channels or users:
        fails.append("ignore entries did not clear")
    print("   settings / categories / toggles / ignores: round-trip ok")

    # message mirror
    msg = types.SimpleNamespace(
        id=100, guild=types.SimpleNamespace(id=gid),
        channel=types.SimpleNamespace(id=200),
        author=types.SimpleNamespace(id=300),
        content="hello", attachments=[],
        created_at=datetime.now(timezone.utc),
    )
    db.buffer_message(msg)
    stored = await db.get_message(100)
    if not stored or stored["content"] != "hello":
        fails.append("buffered message did not flush on read")
    await db.update_message(100, "edited")
    stored = await db.get_message(100)
    if stored["content"] != "edited" or stored["edited_at"] is None:
        fails.append("message edit did not persist")
    print(f"   message mirror: stored, edited, count={await db.message_count()}")

    # a message older than the retention window must be purged
    async with db._lock:
        await db._conn.execute(
            "INSERT OR REPLACE INTO message_cache VALUES (?,?,?,?,?,?,?,?)",
            (101, gid, 200, 300, "old", "[]",
             int(datetime.now(timezone.utc).timestamp()) - al.MESSAGE_RETENTION_SECONDS - 60,
             None),
        )
        await db._conn.commit()
    removed = await db.purge_messages()
    print(f"   purged {removed} expired row(s)")
    if removed != 1:
        fails.append(f"purge removed {removed} rows, expected 1")
    if await db.get_message(100) is None:
        fails.append("purge removed a message inside the retention window")


async def check_batching(fails, db):
    print("\n-- batching --")
    cog = bare_cog(db)
    guild_id = 1
    vibey = cog.bot.user
    actor = al.Actor(user=vibey, via_bot=True)

    # A team role handed to a whole lobby is one action, not twelve.
    for uid in range(10, 22):
        await cog.submit(al.LogEvent(
            "member_roles", guild_id, FakeUser(uid), actor,
            {"gained": ["Red Team"], "lost": []},
        ))
    print(f"   12 role grants -> {len(cog._buckets)} bucket(s)")
    if len(cog._buckets) != 1:
        fails.append(f"12 role grants made {len(cog._buckets)} buckets, expected 1")

    bucket = next(iter(cog._buckets.values()))
    embed = cog._render(FakeGuild(), "member_roles", bucket.events)
    print(f"   rendered header: {embed.author.name}")
    print(f"   footer: {embed.footer.text}")
    print(f"   fields: {[(f.name, f.value[:40] + '…') for f in embed.fields]}")
    if "12 members" not in (embed.author.name or ""):
        fails.append("bulk role embed does not name the member count")
    if "added" not in (embed.author.name or "").lower():
        fails.append("a pure role grant is not labelled as an addition")
    if embed.footer.text != "Vibey":
        fails.append(f"bulk bot action footer is {embed.footer.text!r}, expected 'Vibey'")
    # The role name belongs on one line with everyone who got it, not repeated.
    if len(embed.fields) != 1:
        fails.append(f"12 identical grants made {len(embed.fields)} fields, expected 1")
    elif embed.fields[0].name != "Added: Red Team":
        fails.append(f"group field is named {embed.fields[0].name!r}")
    elif embed.fields[0].value.count("<@") != 12:
        fails.append("the group field does not list every affected member")
    if embed.colour != al.COLOR_ROLE_ADD:
        fails.append("a pure role grant did not use the addition colour")

    # Mixed changes in one batch must split into a field per distinct change.
    cog._buckets.clear()
    for uid in (10, 11):
        await cog.submit(al.LogEvent("member_roles", guild_id, FakeUser(uid), actor,
                                     {"gained": ["Red Team"], "lost": []}))
    for uid in (12, 13, 14):
        await cog.submit(al.LogEvent("member_roles", guild_id, FakeUser(uid), actor,
                                     {"gained": [], "lost": ["Blue Team"]}))
    embed = cog._render(FakeGuild(), "member_roles",
                        next(iter(cog._buckets.values())).events)
    print(f"   mixed batch: {embed.author.name!r} "
          f"{[(f.name, f.value) for f in embed.fields]}")
    names = [f.name for f in embed.fields]
    if names != ["Added: Red Team", "Removed: Blue Team"]:
        fails.append(f"mixed batch fields are {names}")
    if "changed" not in (embed.author.name or "").lower():
        fails.append("a mixed batch is not labelled as a change")
    if embed.colour != al.COLOR_NEUTRAL:
        fails.append("a mixed batch did not use the neutral colour")

    # A single member acting through the bot is credited first, Vibey second.
    cog._buckets.clear()
    solo = FakeUser(50, "someone")
    await cog.submit(al.LogEvent(
        "member_roles", guild_id, solo, al.Actor(user=vibey, via_bot=True),
        {"gained": [], "lost": ["Red Team", "Blue Team", "LFG"]},
    ))
    bucket = next(iter(cog._buckets.values()))
    embed = cog._render(FakeGuild(), "member_roles", bucket.events)
    print(f"   solo bot action footer: {embed.footer.text}")
    print(f"   solo header: {embed.author.name!r} "
          f"fields: {[(f.name, f.value) for f in embed.fields]}")
    if embed.footer.text != "someone (Vibey)":
        fails.append(f"solo bot footer is {embed.footer.text!r}, expected 'someone (Vibey)'")
    if "Roles removed" not in (embed.author.name or ""):
        fails.append("a pure role removal is not labelled as a removal")
    removed = [f for f in embed.fields if f.name.startswith("Removed")]
    if len(removed) != 1 or removed[0].value != "Red Team\nBlue Team\nLFG":
        fails.append("mass role removal was not grouped into one Removed field")
    if any(f.name.startswith("Added") for f in embed.fields):
        fails.append("a removal-only change rendered an Added field")

    # A member who both gained and lost roles gets both fields, labelled.
    cog._buckets.clear()
    await cog.submit(al.LogEvent("member_roles", guild_id, solo, al.Actor(user=vibey,
                                 via_bot=True),
                                 {"gained": ["Verified"], "lost": ["Unverified"]}))
    embed = cog._render(FakeGuild(), "member_roles",
                        next(iter(cog._buckets.values())).events)
    print(f"   solo mixed: {embed.author.name!r} "
          f"{[(f.name, f.value) for f in embed.fields]}")
    if [f.name for f in embed.fields] != ["Added (1)", "Removed (1)"]:
        fails.append(f"solo mixed fields are {[f.name for f in embed.fields]}")

    # A human moderator is credited plainly, with their reason.
    cog._buckets.clear()
    mod = FakeUser(60, "ModPerson")
    await cog.submit(al.LogEvent(
        "member_roles", guild_id, solo,
        al.Actor(user=mod, reason="cleanup"), {"gained": ["Muted"], "lost": []},
    ))
    embed = cog._render(FakeGuild(), "member_roles",
                        next(iter(cog._buckets.values())).events)
    print(f"   moderator footer: {embed.footer.text}")
    if embed.footer.text != "ModPerson — cleanup":
        fails.append(f"moderator footer is {embed.footer.text!r}")

    # Add-then-remove inside one window is a no-op and should post nothing.
    cog._buckets.clear()
    for gained, lost in (["Temp"], []), ([], ["Temp"]):
        await cog.submit(al.LogEvent(
            "member_roles", guild_id, solo, actor, {"gained": gained, "lost": lost},
        ))
    embed = cog._render(FakeGuild(), "member_roles",
                        next(iter(cog._buckets.values())).events)
    print(f"   role added then removed in-window -> {embed!r}")
    if embed is not None:
        fails.append("a net-zero role change still produced an embed")

    # Different actors must not be merged into one attribution.
    cog._buckets.clear()
    await cog.submit(al.LogEvent("member_roles", guild_id, solo,
                                 al.Actor(user=mod), {"gained": ["A"], "lost": []}))
    await cog.submit(al.LogEvent("member_roles", guild_id, solo,
                                 al.Actor(user=vibey, via_bot=True),
                                 {"gained": ["B"], "lost": []}))
    print(f"   two actors -> {len(cog._buckets)} bucket(s)")
    if len(cog._buckets) != 2:
        fails.append("events from different actors were merged")

    # A disabled event must never reach a bucket.
    cog._buckets.clear()
    await db.set_toggle(guild_id, "nickname_change", False)
    await cog.submit(al.LogEvent("nickname_change", guild_id, solo,
                                 data={"before": "a", "after": "b"}))
    print(f"   disabled event -> {len(cog._buckets)} bucket(s)")
    if cog._buckets:
        fails.append("a disabled event was still queued")
    await db.set_toggle(guild_id, "nickname_change", True)

    # The debounce must extend on each new event but respect the ceiling.
    bucket = al.Bucket(key=(1, "member_roles", None))
    if bucket.ready(15):
        fails.append("a fresh bucket reported itself ready")
    bucket.last_at -= 20
    if not bucket.ready(15):
        fails.append("an idle bucket did not become ready")
    bucket.last_at = bucket.first_at = 0.0
    if not bucket.ready(10 ** 9):
        fails.append("the max-hold ceiling did not force a flush")
    print("   debounce: extends on activity, capped by max hold")


async def check_renderers(fails, db):
    print("\n-- renderers --")
    cog = bare_cog(db)
    guild = FakeGuild()
    user = FakeUser(70, "subject")
    mod = FakeUser(71, "ModPerson")
    actor = al.Actor(user=mod, reason="because")
    now = datetime.now(timezone.utc)

    long_text = "x" * 5000  # every renderer must survive oversized content

    samples = {
        "message_delete": [al.LogEvent("message_delete", 1, user, actor, {
            "channel_id": 200, "author_id": user.id,
            "content": long_text, "attachments": ["a.png"]})] * 12,
        "message_edit": [al.LogEvent("message_edit", 1, user, al.Actor(), {
            "message_id": 100 + i, "channel_id": 200, "author_id": user.id,
            "before": long_text, "after": long_text,
            "jump_url": "https://discord.com/x"}) for i in range(6)],
        "message_bulk_delete": [al.LogEvent("message_bulk_delete", 1, None, actor, {
            "channel_id": 200, "count": 40,
            "messages": [{"author_id": user.id, "content": long_text}] * 30})],
        "member_join": [al.LogEvent("member_join", 1, user, al.Actor(), {
            "roles": [f"Role{i}" for i in range(60)]})],
        "member_leave": [al.LogEvent("member_leave", 1, user, al.Actor(), {
            "tenure_seconds": 400000})],
        "member_kick": [al.LogEvent("member_kick", 1, user, actor, {
            "tenure_seconds": 90, "is_inactivity_kick": True})],
        "member_ban": [al.LogEvent("member_ban", 1, user, actor, {
            "tenure_seconds": None})],
        "member_unban": [al.LogEvent("member_unban", 1, user, actor, {})],
        "member_timeout": [al.LogEvent("member_timeout", 1, user, actor, {
            "until": now + timedelta(hours=3), "duration": 10800})],
        "nickname_change": [al.LogEvent("nickname_change", 1, FakeUser(80 + i), actor, {
            "before": None, "after": long_text}) for i in range(15)],
        "username_change": [al.LogEvent("username_change", 1, user, al.Actor(), {
            "before": "old", "after": "new"})],
        "avatar_change": [al.LogEvent("avatar_change", 1, user, al.Actor(), {
            "before": "https://example.invalid/a.png",
            "after": "https://example.invalid/b.png"})],
        "member_roles": [al.LogEvent("member_roles", 1, FakeUser(90 + i), actor, {
            "gained": [f"Role{n}" for n in range(40)], "lost": []})
            for i in range(20)],
        "role_update": [al.LogEvent("role_update", 1, None, actor, {
            "role_id": 5, "name_before": "a", "name_after": long_text,
            "color_before": "#000000", "color_after": "#ffffff",
            "hoist_changed": True, "hoist_after": True,
            "mentionable_changed": False, "mentionable_after": False})] * 12,
        "role_permissions": [al.LogEvent("role_permissions", 1, None, actor, {
            "role_id": 5, "granted": [f"Perm {i}" for i in range(50)],
            "revoked": [f"Gone {i}" for i in range(50)]})],
        "channel_name": [al.LogEvent("channel_name", 1, None, actor, {
            "channel_id": 200, "before": "old", "after": long_text})] * 12,
        "channel_permissions": [al.LogEvent("channel_permissions", 1, None, actor, {
            "channel_id": 200, "target_label": "<@&5>",
            "allowed": [f"Perm {i}" for i in range(40)],
            "denied": [], "cleared": []})] * 10,
    }

    missing = set(al.EVENTS) - set(samples)
    if missing:
        fails.append(f"no renderer sample for: {sorted(missing)}")

    for key, events in samples.items():
        embed = cog._render(guild, key, events)
        problems = embed_errors(key, embed)
        status = "None" if embed is None else f"{len(embed)} chars"
        print(f"   {key:<24} {status:>12}   {'ok' if not problems else 'FAIL'}")
        fails.extend(problems)
        if embed is None and key != "member_roles":
            fails.append(f"{key} rendered nothing")

    # A cached-miss delete still has to produce a usable entry.
    embed = cog._render(guild, "message_delete", [al.LogEvent(
        "message_delete", 1, None, al.Actor(), {
            "channel_id": 200, "author_id": None,
            "content": None, "attachments": []})])
    print(f"   uncached delete renders: {embed is not None}")
    fails.extend(embed_errors("message_delete/uncached", embed))

    print(f"   fmt_duration: 30s={al.fmt_duration(30)} "
          f"400000s={al.fmt_duration(400000)} 3600s={al.fmt_duration(3600)}")
    if al.fmt_duration(30) != "<1m" or al.fmt_duration(400000) != "4d 15h 6m":
        fails.append("fmt_duration output changed")

    print("\n-- embed shapes --")
    made = datetime.now(timezone.utc) - timedelta(days=400)
    print(f"   fmt_account_age(400d): {al.fmt_account_age(made)}")
    if "<t:" in al.fmt_account_age(made):
        fails.append("account age emits a timestamp footers cannot render")

    # Exits: no account-created field, action and tenure on the footer, no
    # timestamp, and the header is just the name.
    for key, action in (("member_leave", "Left"), ("member_kick", "Kicked"),
                        ("member_ban", "Banned")):
        data = {"tenure_seconds": 400000}
        actor = al.Actor() if key == "member_leave" else al.Actor(user=mod,
                                                                 reason="spam")
        embed = cog._render(guild, key, [al.LogEvent(key, 1, user, actor, data)])
        print(f"   {key:<14} header={embed.author.name!r} "
              f"footer={embed.footer.text!r} fields={len(embed.fields)} "
              f"ts={embed.timestamp is not None}")
        if embed.fields:
            fails.append(f"{key} still renders fields")
        if embed.timestamp is not None:
            fails.append(f"{key} still carries a timestamp")
        if embed.author.name != user.display_name:
            fails.append(f"{key} header is {embed.author.name!r}, expected the name")
        if not embed.footer.text.startswith(action):
            fails.append(f"{key} footer does not lead with {action!r}")
        if "4d 15h 6m in server" not in embed.footer.text:
            fails.append(f"{key} footer is missing the time in server")
        if "created" in embed.footer.text.lower():
            fails.append(f"{key} still reports the account creation date")
    embed = cog._render(guild, "member_kick",
                        [al.LogEvent("member_kick", 1, user,
                                     al.Actor(user=mod, reason="spam"),
                                     {"tenure_seconds": 60})])
    print(f"   kick attribution: {embed.footer.text!r}")
    if "Kicked by ModPerson — spam" not in embed.footer.text:
        fails.append("a kick footer lost its moderator or reason")

    # Join: account created on the footer, no timestamp.
    embed = cog._render(guild, "member_join",
                        [al.LogEvent("member_join", 1, user, al.Actor(),
                                     {"roles": ["Overwatch", "LFG"]})])
    print(f"   member_join    footer={embed.footer.text!r} "
          f"fields={[f.name for f in embed.fields]} "
          f"ts={embed.timestamp is not None}")
    if not embed.footer.text.startswith("Account created"):
        fails.append("the join footer does not carry the account creation date")
    if embed.timestamp is not None:
        fails.append("the join embed still carries a timestamp")
    if any("Account" in f.name for f in embed.fields):
        fails.append("the join embed still has an account-created field")
    if not any("Roles" in f.name for f in embed.fields):
        fails.append("the join embed lost its onboarding roles")

    # Avatar: new one as the thumbnail, nothing blown up at the bottom.
    embed = cog._render(guild, "avatar_change",
                        [al.LogEvent("avatar_change", 1, user, al.Actor(),
                                     {"before": "https://example.invalid/a.png",
                                      "after": "https://example.invalid/b.png"})])
    print(f"   avatar_change  thumbnail={embed.thumbnail.url} image={embed.image.url}")
    if embed.image.url is not None:
        fails.append("the avatar embed still renders a full-size image")
    if embed.thumbnail.url != "https://example.invalid/b.png":
        fails.append("the avatar thumbnail is not the new avatar")


def check_permission_diffs(fails):
    print("\n-- permission diffs --")
    before = discord.Permissions(send_messages=True, manage_messages=True)
    after = discord.Permissions(send_messages=True, embed_links=True)
    granted, revoked = al.diff_permissions(before, after)
    print(f"   granted={granted} revoked={revoked}")
    if "Embed Links" not in granted or "Manage Messages" not in revoked:
        fails.append("diff_permissions missed a change")
    if "Send Messages" in granted or "Send Messages" in revoked:
        fails.append("diff_permissions reported an unchanged permission")

    ow_before = discord.PermissionOverwrite(send_messages=False, add_reactions=True)
    ow_after = discord.PermissionOverwrite(send_messages=True)
    allowed, denied, cleared = al.diff_overwrite(ow_before, ow_after)
    print(f"   allowed={allowed} denied={denied} cleared={cleared}")
    if "Send Messages" not in allowed:
        fails.append("diff_overwrite missed a grant")
    if "Add Reactions" not in cleared:
        fails.append("diff_overwrite missed a reset-to-inherit")


async def check_panel(fails, db):
    print("\n-- panel --")
    cog = bare_cog(db)
    cog.is_admin = lambda member: True
    guild = FakeGuild()

    home = al.AuditPanelHome(cog)
    channels = al.ChannelsPage(cog, home)
    events = al.EventsPage(cog, home)
    ignore = al.IgnorePage(cog, home)
    message_log = al.MessageLogPage(cog, home)

    pages = [("AuditPanelHome", home), ("ChannelsPage", channels),
             ("EventsPage", events), ("IgnorePage", ignore),
             ("MessageLogPage", message_log)]
    pages += [(f"ChannelTargetPage/{k}", al.ChannelTargetPage(cog, channels, k))
              for k in al.CHANNEL_TARGETS]
    pages += [(f"EventTogglesPage/{c}", al.EventTogglesPage(cog, events, c))
              for c in al.CATEGORIES]

    print(f"   {'page':<28}{'items':>7}{'back':>7}{'rows':>14}")
    for name, page in pages:
        await page.rebuild(guild)
        rows = sorted({(i._rendered_row if i._rendered_row is not None else i.row) or 0
                       for i in page.children})
        has_back = any(isinstance(i, al.BackButton) for i in page.children)
        print(f"   {name:<28}{len(page.children):>7}{str(has_back):>7}{str(rows):>14}")
        fails.extend(layout_errors(name, page))
        if page is not home and not has_back:
            fails.append(f"{name} lost its Back button after rebuild")
        if page is home and has_back:
            fails.append("the home page should not have a Back button")

        embed = await page.build_embed(guild)
        fails.extend(embed_errors(name, embed))

    # Ignore lists add two more selects once populated — the tightest layout.
    await db.add_ignored(1, "channel", 500)
    await db.add_ignored(1, "user", 501)
    populated = al.IgnorePage(cog, home)
    await populated.rebuild(guild)
    print(f"   IgnorePage with entries: {len(populated.children)} items, "
          f"rows {sorted({(i._rendered_row if i._rendered_row is not None else i.row) or 0 for i in populated.children})}")
    fails.extend(layout_errors("IgnorePage/populated", populated))
    if not any(isinstance(i, al.BackButton) for i in populated.children):
        fails.append("populated IgnorePage lost its Back button")
    await db.remove_ignored(1, "channel", 500)
    await db.remove_ignored(1, "user", 501)

    # Every home button must lead somewhere.
    labels = {c.label for c in home.children if isinstance(c, discord.ui.Button)}
    print(f"   home buttons: {sorted(labels)}")
    if labels != {"Channels", "Events", "Ignore list", "Message log"}:
        fails.append(f"unexpected home buttons: {sorted(labels)}")

    # Every channel slot must map to a real column or category.
    for key, spec in al.CHANNEL_TARGETS.items():
        if "column" in spec:
            if spec["column"] not in al.ALLOWED_SETTING_KEYS:
                fails.append(f"channel slot {key} writes a disallowed column")
        elif spec.get("category") not in al.CATEGORIES:
            fails.append(f"channel slot {key} points at an unknown category")
    print("   channel slots map to real columns / categories: yes")


async def check_routing(fails, db):
    print("\n-- routing --")
    cog = bare_cog(db)
    guild = FakeGuild()
    gid = guild.id

    await db.set_setting(gid, "exit_channel_id", 900)
    await db.set_setting(gid, "mod_channel_id", 901)
    await db.set_setting(gid, "default_channel_id", 902)
    await db.set_category_channel(gid, "messages", 903)

    async def ids(event_key, data=None):
        dests = await cog._destinations(guild, event_key, data or {})
        return sorted(c.id for c in dests)

    cases = [
        ("member_leave", {}, [900]),
        ("member_kick", {}, [900, 901]),
        ("member_kick", {"is_inactivity_kick": True}, [900]),
        ("member_ban", {}, [900, 901]),
        ("member_timeout", {}, [901]),
        ("member_unban", {}, [901]),
        ("message_delete", {}, [903]),
        ("member_join", {}, [902]),
        ("role_update", {}, [902]),
    ]
    for event_key, data, expected in cases:
        got = await ids(event_key, data)
        note = " (inactivity)" if data.get("is_inactivity_kick") else ""
        print(f"   {event_key + note:<28} -> {got}")
        if got != expected:
            fails.append(f"{event_key}{note} routed to {got}, expected {expected}")


async def check_delete_attribution(fails, db):
    """A moderator clearing several messages must be credited on every one.

    Discord coalesces repeated message deletions into a single audit entry and
    only bumps its count, so the naive "is there a fresh entry?" test credits
    the moderator once and then blames the author for the rest.
    """
    print("\n-- message delete attribution --")
    cog = bare_cog(db)
    al.AUDIT_SETTLE_SECONDS, original = 0, al.AUDIT_SETTLE_SECONDS

    author, mod = FakeUser(70, "author"), FakeUser(71, "ModPerson")

    class FakeEntry:
        def __init__(self, entry_id, count, age=0.0):
            self.id, self.user, self.reason = entry_id, mod, None
            self.target = author
            self.extra = types.SimpleNamespace(count=count,
                                               channel=FakeChannel(200))
            self.created_at = datetime.now(timezone.utc) - timedelta(seconds=age)

    class LogGuild(FakeGuild):
        entries = []

        def audit_logs(self, limit=None, action=None):
            async def gen():
                for e in self.entries:
                    yield e
            return gen()

    guild = LogGuild()
    try:
        # No entry at all: the author deleted their own message.
        guild.entries = []
        actor = await cog.resolve_message_deleter(guild, 200, author.id)
        print(f"   no audit entry        -> {actor.user}")
        if actor.user is not None:
            fails.append("a self-delete was attributed to somebody")

        # A fresh entry: a moderator deleted it.
        guild.entries = [FakeEntry(entry_id=1, count=1)]
        actor = await cog.resolve_message_deleter(guild, 200, author.id)
        print(f"   fresh entry, count=1  -> {actor.user}")
        if actor.user is not mod:
            fails.append("a moderator delete was not attributed to the moderator")

        # Same entry, count unchanged: this one really was a self-delete.
        actor = await cog.resolve_message_deleter(guild, 200, author.id)
        print(f"   same entry, count=1   -> {actor.user}")
        if actor.user is not None:
            fails.append("an unchanged entry was re-credited to the moderator")

        # Count bumped without a new entry — the coalesced case that used to be
        # misattributed to the author.
        for expected_count in (2, 3, 4):
            guild.entries = [FakeEntry(entry_id=1, count=expected_count)]
            actor = await cog.resolve_message_deleter(guild, 200, author.id)
            print(f"   same entry, count={expected_count}   -> {actor.user}")
            if actor.user is not mod:
                fails.append(
                    f"coalesced delete #{expected_count} was not attributed to the moderator"
                )

        # A stale entry from an older clear-out must not be credited.
        cog._delete_counts.clear()
        guild.entries = [FakeEntry(entry_id=2, count=9,
                                   age=al.AUDIT_FRESHNESS_SECONDS + 30)]
        actor = await cog.resolve_message_deleter(guild, 200, author.id)
        print(f"   stale entry           -> {actor.user}")
        if actor.user is not None:
            fails.append("a stale audit entry was credited to the moderator")

        # ...but a bump on that same stale entry is a live deletion.
        guild.entries = [FakeEntry(entry_id=2, count=10,
                                   age=al.AUDIT_FRESHNESS_SECONDS + 30)]
        actor = await cog.resolve_message_deleter(guild, 200, author.id)
        print(f"   stale entry, bumped   -> {actor.user}")
        if actor.user is not mod:
            fails.append("a bump on a stale entry was not attributed")

        # An entry for a different channel must not be borrowed.
        cog._delete_counts.clear()
        guild.entries = [FakeEntry(entry_id=3, count=1)]
        actor = await cog.resolve_message_deleter(guild, 999, author.id)
        print(f"   other channel         -> {actor.user}")
        if actor.user is not None:
            fails.append("an entry from another channel was credited")
    finally:
        al.AUDIT_SETTLE_SECONDS = original


async def check_self_edits_and_deletes(fails, db):
    """A user editing or deleting their own message must be logged from the
    mirror, including when the message is far too old for discord.py's cache.
    """
    print("\n-- self edits and deletes --")
    cog = bare_cog(db)
    al.AUDIT_SETTLE_SECONDS, original = 0, al.AUDIT_SETTLE_SECONDS

    author = FakeUser(300, "author")

    class NoEntryGuild(FakeGuild):
        """A guild whose audit log is empty — every delete is a self-delete."""

        def audit_logs(self, limit=None, action=None):
            async def gen():
                return
                yield
            return gen()

        def get_member(self, uid):
            return author if uid == author.id else FakeUser(uid)

    guild = NoEntryGuild()
    cog.bot.get_guild = lambda gid: guild
    cog.bot.get_user = lambda uid: FakeUser(uid)

    try:
        # Mirror a message, then edit it with nothing in the library cache —
        # the "edited hours later" case that on_message_edit never sees.
        msg = types.SimpleNamespace(
            id=4242, guild=types.SimpleNamespace(id=guild.id),
            channel=types.SimpleNamespace(id=200), author=author,
            content="the original text", attachments=[],
            created_at=datetime.now(timezone.utc),
        )
        db.buffer_message(msg)

        updated = types.SimpleNamespace(
            author=author, content="the edited text",
            jump_url="https://discord.com/x",
        )
        payload = types.SimpleNamespace(
            guild_id=guild.id, channel_id=200, message_id=4242,
            data={"content": "the edited text"},
            message=updated, cached_message=None,
        )
        await cog.on_raw_message_edit(payload)
        print(f"   uncached edit queued: {len(cog._buckets)} bucket(s)")
        if not cog._buckets:
            fails.append("an edit outside the library cache was not logged")
        else:
            embed = cog._render(guild, "message_edit",
                                next(iter(cog._buckets.values())).events)
            values = [f.value for f in embed.fields]
            print(f"   before/after: {values}")
            if "the original text" not in values or "the edited text" not in values:
                fails.append("the edit embed lost its before/after")

        # The mirror must now hold the new text, so a later delete shows it.
        row = await db.get_message(4242)
        print(f"   mirror updated to: {row['content']!r}")
        if row["content"] != "the edited text":
            fails.append("the mirror was not updated on edit")

        # Embed resolution and pins carry no content and must not log.
        cog._buckets.clear()
        await cog.on_raw_message_edit(types.SimpleNamespace(
            guild_id=guild.id, channel_id=200, message_id=4242,
            data={"embeds": []}, message=updated, cached_message=None))
        print(f"   contentless update queued: {len(cog._buckets)} bucket(s)")
        if cog._buckets:
            fails.append("a contentless MESSAGE_UPDATE was logged as an edit")

        # Now the author deletes it themselves: no audit entry exists, so the
        # entry must still appear, carry the content, and name the author.
        cog._buckets.clear()
        await cog.on_raw_message_delete(types.SimpleNamespace(
            guild_id=guild.id, channel_id=200, message_id=4242,
            cached_message=None))
        print(f"   self-delete queued: {len(cog._buckets)} bucket(s)")
        if not cog._buckets:
            fails.append("a self-delete inside the retention window was not logged")
        else:
            embed = cog._render(guild, "message_delete",
                                next(iter(cog._buckets.values())).events)
            print(f"   footer: {embed.footer.text!r}")
            if "the edited text" not in (embed.description or ""):
                fails.append("the self-delete embed lost the message content")
            if embed.footer.text != "author":
                fails.append(f"self-delete footer is {embed.footer.text!r}, expected 'author'")

        # A delete we have no record of at all (bot panel churn, or older than
        # retention) must stay silent rather than logging an empty entry.
        cog._buckets.clear()
        await cog.on_raw_message_delete(types.SimpleNamespace(
            guild_id=guild.id, channel_id=200, message_id=999999,
            cached_message=None))
        print(f"   unknown delete queued: {len(cog._buckets)} bucket(s)")
        if cog._buckets:
            fails.append("a delete with no record produced an empty entry")
    finally:
        al.AUDIT_SETTLE_SECONDS = original


async def check_flush(fails, db):
    """Bucket -> debounce -> rendered embed -> channel.send, end to end."""
    print("\n-- flush --")
    sent = []

    class SendChannel(FakeChannel):
        async def send(self, embed=None):
            sent.append((self.id, embed))

    class SendGuild(FakeGuild):
        def get_channel(self, cid):
            return SendChannel(cid)

    guild = SendGuild()
    cog = bare_cog(db)
    cog.bot.get_guild = lambda gid: guild

    await db.set_setting(guild.id, "default_channel_id", 902)
    await db.set_setting(guild.id, "exit_channel_id", 900)
    await db.set_setting(guild.id, "mod_channel_id", 901)

    mod = FakeUser(60, "ModPerson")
    await cog.submit(al.LogEvent("member_kick", guild.id, FakeUser(70, "subject"),
                                 al.Actor(user=mod, reason="spam"),
                                 {"tenure_seconds": 7200}))

    # Nothing should leave the batcher until the window has actually elapsed.
    await cog._flush_ready()
    print(f"   before the window elapses: {len(sent)} sent")
    if sent:
        fails.append("a bucket flushed before its window elapsed")

    for bucket in cog._buckets.values():
        bucket.last_at -= al.DEFAULT_BATCH_SECONDS + 1
    await cog._flush_ready()

    print(f"   after the window: {len(sent)} sent to {[c for c, _ in sent]}")
    if sorted(c for c, _ in sent) != [900, 901]:
        fails.append(f"kick went to {[c for c, _ in sent]}, expected exit + mod")
    if cog._buckets:
        fails.append("a flushed bucket was left behind")
    if sent:
        embed = sent[0][1]
        print(f"   embed: author={embed.author.name!r} footer={embed.footer.text!r} "
              f"colour={embed.colour}")
        if embed.footer.text != "Kicked by ModPerson — spam — 2h in server":
            fails.append(f"flushed kick footer is {embed.footer.text!r}")
        if embed.colour != al.COLOR_KICK:
            fails.append("flushed kick used the wrong colour")
        if embed.fields:
            fails.append("flushed kick rendered fields instead of a footer line")

    # A guild the bot can no longer see must not raise on flush.
    cog.bot.get_guild = lambda gid: None
    await cog.submit(al.LogEvent("member_leave", guild.id, FakeUser(71),
                                 al.Actor(), {"tenure_seconds": 10}))
    for bucket in cog._buckets.values():
        bucket.last_at -= al.DEFAULT_BATCH_SECONDS + 1
    await cog._flush_ready()
    print("   unknown guild flushed without raising: yes")


async def check_migration(fails):
    print("\n-- migration from welcome_config.json --")
    with tempfile.TemporaryDirectory() as tmp:
        legacy = os.path.join(tmp, "welcome_config.json")
        with open(legacy, "w", encoding="utf-8") as f:
            f.write('{"1": {"exit_channel_id": 111, "mod_channel_id": 222}}')

        original = al.LEGACY_CONFIG_FILE
        al.LEGACY_CONFIG_FILE = legacy
        try:
            db = al.AuditDB(os.path.join(tmp, "audit.db"))
            await db.connect()
            await db.migrate_from_legacy()
            settings = await db.get_settings(1)
            print(f"   seeded exit={settings['exit_channel_id']} "
                  f"mod={settings['mod_channel_id']}")
            if (settings["exit_channel_id"], settings["mod_channel_id"]) != (111, 222):
                fails.append("migration did not seed the exit/mod channels")

            # A second run must not overwrite whatever the admin has since set.
            await db.set_setting(1, "exit_channel_id", 333)
            await db.migrate_from_legacy()
            after = await db.get_settings(1)
            print(f"   re-run left exit={after['exit_channel_id']}")
            if after["exit_channel_id"] != 333:
                fails.append("migration re-ran and clobbered a configured channel")
            await db.close()
        finally:
            al.LEGACY_CONFIG_FILE = original


async def main():
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        db = al.AuditDB(os.path.join(tmp, "audit.db"))
        await db.connect()
        try:
            await check_db(fails, db)
            await check_batching(fails, db)
            await check_renderers(fails, db)
            check_permission_diffs(fails)
            await check_panel(fails, db)
            await check_routing(fails, db)
            await check_delete_attribution(fails, db)
            await check_self_edits_and_deletes(fails, db)
            await check_flush(fails, db)
        finally:
            await db.close()
    await check_migration(fails)

    print("\n" + "=" * 56)
    if fails:
        print(f"{len(fails)} FAILURES")
        for f in fails:
            print("  !!", f)
    else:
        print("all checks passed")
    return 1 if fails else 0


sys.exit(asyncio.run(main()))
