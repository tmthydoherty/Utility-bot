"""Verify automations storage: schema upgrades, and the move out of utility.db.

Three failure modes are covered, all of which were real or are one restart
away from being real:

* `CREATE TABLE IF NOT EXISTS` does nothing to a table that already exists, so
  a column added to SCHEMA after the first boot never reaches a live database.
  It stays one version behind silently until an insert fails.
* `def f(path=DEFAULT)` captures the default when the function is *defined*,
  so a test that repoints the module global keeps writing to the real file.
* automations used to live in utility.db. The move has to bring live state
  across — a pending `wait`, a running strike count, a paused kill switch — or
  the split silently un-times-out everyone and resets every tally to zero.

Run: .venv/bin/python tests/automations/check_migration.py
"""
import asyncio
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import aiosqlite

from cogs.automations import storage as st
from cogs.automations.storage import (KILL_SWITCH_KEY, AutomationsDB,
                                      migrate_from_utility)
from cogs.utility import storage as utility_storage

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


async def main():
    tmp = Path(tempfile.mkdtemp())
    try:
        await run_checks(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for failure in FAILURES:
            print(f"  - {failure}")
        sys.exit(1)
    print("All automations migration checks passed.")


async def run_checks(tmp: Path):
    print("\nDefaults are resolved at call time, not bound at import")
    check(AutomationsDB(str(tmp / "explicit.db")).db_path == str(tmp / "explicit.db"),
          "an explicit path is honoured")
    original = st.DB_PATH
    st.DB_PATH = str(tmp / "patched.db")
    try:
        check(AutomationsDB().db_path == str(tmp / "patched.db"),
              "repointing DB_PATH takes effect — a test cannot hit production")
    finally:
        st.DB_PATH = original

    await check_schema_upgrade(tmp)
    await check_move_from_utility(tmp)
    await check_revision(tmp)


async def check_schema_upgrade(tmp: Path):
    print("\nSchema upgrade reaches an existing database")
    # Build a database the way an older build would have: every table, but
    # with a column missing from one of them.
    old_path = tmp / "old.db"
    async with aiosqlite.connect(str(old_path)) as conn:
        await conn.executescript(st.SCHEMA)
        await conn.commit()
    async with aiosqlite.connect(str(old_path)) as conn:
        await conn.execute("ALTER TABLE automations DROP COLUMN created_ts")
        await conn.commit()

    async with aiosqlite.connect(str(old_path)) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("PRAGMA table_info(automations)")
        columns = {r["name"] for r in await cursor.fetchall()}
    check("created_ts" not in columns, "set-up: the column really is missing")

    db3 = AutomationsDB(str(old_path))
    await db3.connect()
    columns = {r["name"] for r in await db3.fetchall("PRAGMA table_info(automations)")}
    check("created_ts" in columns,
          "connect() added the missing column to the existing table")

    # The failure this guards against is not a broken ALTER — it is a column
    # added to SCHEMA and forgotten in the migration list, or the reverse. Only
    # the first is visible on an existing database, and only ever as an insert
    # failing in production, so the two lists are compared directly. Checked
    # generically so a future column is covered without touching this test.
    fresh = AutomationsDB(str(tmp / "fresh.db"))
    await fresh.connect()
    for table, names in st.ADDED_COLUMNS.items():
        live = {r["name"] for r in
                await fresh.fetchall(f"PRAGMA table_info({table})")}
        for name, _spec in names:
            check(name in live,
                  f"{table}.{name} is in the migration list and in SCHEMA")
    declared = {n for n, _ in st.ADDED_COLUMNS.get("automations", ())}
    check("template_key" in declared,
          "template_key is migrated, so existing servers gain it")
    await fresh.close()

    # The symptom the missing column actually produced.
    record_id = await db3.insert_row("automations", guild_id=1, name="probe",
                                     trigger_type="manual")
    check(bool(record_id), "and an insert that used to fail now succeeds")

    # An automation written before `template_key` existed must still load. The
    # panel reads it to find a template's own setup questions, so a row without
    # one has to mean "built from scratch" rather than raising.
    from cogs.automations.models import Automation
    row = await db3.get_row("automations", record_id)
    model = Automation.from_row(row)
    check(model.template_key == "",
          "a row with no template records itself as built from scratch")

    class RowWithoutColumn(dict):
        """sqlite3.Row raises rather than returning None for an unknown key."""

        def __getitem__(self, key):
            if key == "template_key":
                raise IndexError(key)
            return dict.__getitem__(self, key)

    legacy = RowWithoutColumn({k: row[k] for k in row.keys()})
    check(Automation.from_row(legacy).template_key == "",
          "and a row predating the column loads without raising")
    await db3.close()


# The schema utility.db actually had before automations became their own cog,
# reduced to the columns this test writes. Written out rather than imported,
# because the point is to reproduce a database the current code can no longer
# create.
OLD_UTILITY_SCHEMA = """
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE automations (
    id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 0,
    dry_run INTEGER NOT NULL DEFAULT 1, priority INTEGER NOT NULL DEFAULT 100,
    stop_after INTEGER NOT NULL DEFAULT 0, trigger_type TEXT NOT NULL,
    trigger_config TEXT NOT NULL DEFAULT '{}',
    conditions TEXT NOT NULL DEFAULT '{}',
    graph TEXT NOT NULL DEFAULT '{"steps": []}',
    cooldown_s INTEGER NOT NULL DEFAULT 0,
    cooldown_scope TEXT NOT NULL DEFAULT 'user',
    allow_bots INTEGER NOT NULL DEFAULT 0,
    last_fired_ts INTEGER NOT NULL DEFAULT 0,
    run_count INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER, updated_by INTEGER,
    created_ts INTEGER NOT NULL DEFAULT 0, updated_ts INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1, template_key TEXT NOT NULL DEFAULT ''
);
CREATE TABLE automation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, automation_id TEXT NOT NULL,
    ts INTEGER NOT NULL, outcome TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '', duration_ms INTEGER NOT NULL DEFAULT 0,
    trace TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE counters (
    scope TEXT NOT NULL, scope_id INTEGER NOT NULL, key TEXT NOT NULL,
    value INTEGER NOT NULL DEFAULT 0, expires_ts INTEGER NOT NULL DEFAULT 0,
    updated_ts INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (scope, scope_id, key)
);
CREATE TABLE pending_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, automation_id TEXT NOT NULL,
    execute_ts INTEGER NOT NULL, steps TEXT NOT NULL, context TEXT NOT NULL,
    depth INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE temp_roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL, role_id INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL, automation_id TEXT,
    granted_ts INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE cooldowns (
    automation_id TEXT NOT NULL, scope_key TEXT NOT NULL,
    expires_ts INTEGER NOT NULL, PRIMARY KEY (automation_id, scope_key)
);
CREATE TABLE audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
    user_id INTEGER NOT NULL, entity TEXT NOT NULL, entity_id TEXT NOT NULL,
    action TEXT NOT NULL, detail TEXT NOT NULL DEFAULT ''
);
CREATE TABLE media_channels (
    id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1
);
"""


async def check_move_from_utility(tmp: Path):
    print("\nThe move out of utility.db")
    legacy_path = tmp / "utility.db"
    async with aiosqlite.connect(str(legacy_path)) as conn:
        await conn.executescript(OLD_UTILITY_SCHEMA)
        await conn.execute(
            "INSERT INTO automations (id, guild_id, name, trigger_type, enabled, "
            "graph, template_key) VALUES ('a1', 7, 'Greeter', 'member_joined', 1, ?, 'welcome')",
            (json.dumps({"steps": [{"type": "send_message", "config": {}}]}),))
        await conn.execute(
            "INSERT INTO automation_runs (automation_id, ts, outcome) "
            "VALUES ('a1', 1000, 'fired')")
        await conn.execute(
            "INSERT INTO counters (scope, scope_id, key, value) "
            "VALUES ('user', 42, 'strikes', 2)")
        await conn.execute(
            "INSERT INTO pending_actions (automation_id, execute_ts, steps, context) "
            "VALUES ('a1', 5000, '[]', '{}')")
        # Absolute unix timestamps, an hour out. A small literal here would be
        # a moment in 1970, and both rows would read as already expired — which
        # is exactly the bug this pair of checks is looking for.
        soon = st.now() + 3600
        await conn.execute(
            "INSERT INTO temp_roles (guild_id, user_id, role_id, expires_ts) "
            "VALUES (7, 42, 99, ?)", (soon,))
        await conn.execute(
            "INSERT INTO cooldowns (automation_id, scope_key, expires_ts) "
            "VALUES ('a1', 'u:42', ?)", (soon,))
        await conn.execute(
            "INSERT INTO audit (ts, user_id, entity, entity_id, action, detail) "
            "VALUES (1, 5, 'automations', 'a1', 'create', 'Greeter')")
        await conn.execute(
            "INSERT INTO audit (ts, user_id, entity, entity_id, action, detail) "
            "VALUES (2, 5, 'media_channels', 'm1', 'create', 'a media rule')")
        await conn.execute("INSERT INTO settings (key, value) VALUES (?, '0')",
                           (KILL_SWITCH_KEY,))
        await conn.execute(
            "INSERT INTO media_channels (id, guild_id, channel_id) VALUES ('m1', 7, 5)")
        await conn.commit()

    db = AutomationsDB(str(tmp / "moved.db"))
    await db.connect()
    moved = await migrate_from_utility(db, str(legacy_path))
    check(moved >= 7, f"rows came across ({moved})")

    row = await db.fetchone("SELECT * FROM automations WHERE id = 'a1'")
    check(row is not None and row["name"] == "Greeter",
          "the automation itself moved, name and all")
    check(row is not None and row["template_key"] == "welcome",
          "…keeping which template it started from")
    check(row is not None and row["enabled"] == 1,
          "…and whether it was switched on")

    # These four are the live state. Losing any of them would look like the
    # bot quietly forgetting what it was in the middle of.
    check((await db.counter_get("user", 42, "strikes")) == 2,
          "a running tally survived, so nobody's strikes reset to zero")
    pending = await db.fetchall("SELECT * FROM pending_actions")
    check(len(pending) == 1, "a half-finished `wait` step survived")
    check(len(await db.temp_roles_due()) == 0,
          "a temporary role that is not due yet is not swept immediately")
    check(len(await db.fetchall("SELECT * FROM temp_roles")) == 1,
          "…but it did move across")
    check(await db.cooldown_active("a1", "u:42"),
          "a live cooldown survived, so nothing double-fires after the split")
    check(len(await db.fetchall("SELECT * FROM automation_runs")) == 1,
          "run history moved")

    audit = await db.fetchall("SELECT * FROM audit")
    check(len(audit) == 1 and audit[0]["entity"] == "automations",
          "only the automation audit entries came across")
    check(audit[0]["source"] == "discord",
          "…and they are marked as having been made in Discord")

    check(await db.get_bool(KILL_SWITCH_KEY, True) is False,
          "a server that had paused automations stays paused")

    print("\nRunning it twice does not duplicate anything")
    check(await migrate_from_utility(db, str(legacy_path)) == 0,
          "a second run moves nothing")
    check(len(await db.fetchall("SELECT * FROM automations")) == 1,
          "and there is still exactly one automation")

    async with aiosqlite.connect(str(legacy_path)) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r["name"] for r in await cursor.fetchall()}
        cursor = await conn.execute("SELECT COUNT(*) AS c FROM audit")
        remaining = (await cursor.fetchone())["c"]
    check("moved_automations" in tables,
          "the source table was renamed rather than dropped")
    check("automations" not in tables,
          "…so nothing re-reads it on the next boot")
    check("media_channels" in tables,
          "the Utility cog's own tables were left alone")
    check(remaining == 1,
          "the media audit entry stayed in utility.db, the automation one did not")

    await db.close()


async def check_revision(tmp: Path):
    print("\nThe revision counter the website and the bot share")
    db = AutomationsDB(str(tmp / "rev.db"))
    await db.connect()
    check(await db.revision() == 0, "a fresh database starts at zero")
    first = await db.bump_revision()
    second = await db.bump_revision()
    check(first == 1 and second == 2, f"each write moves it on ({first}, {second})")
    # A timestamp would collide here; two edits inside one second have to be
    # two distinct values or the bot never notices the second one.
    check(second > first, "two writes in the same second are still distinguishable")
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
