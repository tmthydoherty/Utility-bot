"""Verify the Utility cog's storage migration — the legacy JSON import.

Two failure modes are covered, both of which were real:

* `def f(path=DEFAULT)` captures the default when the function is *defined*,
  so a test that repoints the module global keeps writing to the real file.
* the old cog deleted anything without an image attachment, so a migrated
  media rule that quietly turned links back on would change what the server
  actually does at upgrade time.

The automation half of this file moved to tests/automations/check_migration.py
along with the tables it covers.

Run: .venv/bin/python tests/utility/check_migration.py
"""
import asyncio
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from cogs.utility import storage as st
from cogs.utility.storage import UtilityDB, loads, migrate_legacy_json

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


LEGACY = {
    "1431565435010289778": {
        "reaction_rules": [
            {"channel_id": 111, "type": "role_mention", "role_ids": [222]},
            {"channel_id": 333, "type": "all"},
            {"channel_id": 444, "type": "from_user", "user_ids": [555]},
        ],
        "media_channels": [666],
    }
}


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
    print("All utility migration checks passed.")


async def run_checks(tmp: Path):
    print("\nDefaults are resolved at call time, not bound at import")
    check(UtilityDB(str(tmp / "explicit.db")).db_path == str(tmp / "explicit.db"),
          "an explicit path is honoured")
    original = st.DB_PATH
    st.DB_PATH = str(tmp / "patched.db")
    try:
        check(UtilityDB().db_path == str(tmp / "patched.db"),
              "repointing DB_PATH takes effect — a test cannot hit production")
    finally:
        st.DB_PATH = original

    print("\nThe automation tables are gone from this schema")
    check("CREATE TABLE IF NOT EXISTS automations" not in st.SCHEMA,
          "utility.db no longer declares an automations table")
    check("CREATE TABLE IF NOT EXISTS counters" not in st.SCHEMA,
          "…nor the engine's counters")
    for table in ("media_channels", "reaction_rules", "sticky_messages", "audit"):
        check(f"CREATE TABLE IF NOT EXISTS {table}" in st.SCHEMA,
              f"…and still declares {table}")

    print("\nLegacy JSON import")
    config = tmp / "utility_config.json"
    config.write_text(json.dumps(LEGACY))
    db = UtilityDB(str(tmp / "m.db"))
    await db.connect()

    imported = await migrate_legacy_json(db, config)
    check(imported == 4, f"imported every record ({imported} of 4)")
    check(not config.exists(), "the legacy file was moved aside")
    check((tmp / "utility_config.json.migrated").exists(),
          "renamed rather than deleted, so a rollback is possible")

    media = await db.fetchall("SELECT * FROM media_channels")
    check(len(media) == 1 and media[0]["channel_id"] == 666, "the media channel landed")
    check(media[0]["allow_links"] == 0,
          "links stay off, preserving what the old cog actually did")
    check(media[0]["thread_enabled"] == 1, "comment threads stay on")

    rules = await db.fetchall("SELECT * FROM reaction_rules ORDER BY channel_id")
    check(len(rules) == 3, f"every reaction rule landed ({len(rules)})")
    by_channel = {r["channel_id"]: r for r in rules}
    check(by_channel[111]["scope"] == "role_mention"
          and loads(by_channel[111]["role_ids"], []) == [222],
          "a role_mention rule kept its roles")
    check(by_channel[333]["scope"] == "all", "an 'all' rule kept its scope")
    check(loads(by_channel[444]["user_ids"], []) == [555],
          "a from_user rule kept its users")
    check(all(r["mode"] == "remove" for r in rules),
          "every migrated rule defaults to the original remove behaviour")

    print("\nRe-running is a no-op")
    check(await migrate_legacy_json(db, config) == 0,
          "a second run imports nothing")
    check(len(await db.fetchall("SELECT * FROM reaction_rules")) == 3,
          "and does not duplicate the rules")
    await db.close()

    print("\nA malformed legacy file degrades rather than crashing")
    broken = tmp / "broken.json"
    broken.write_text("{not json")
    db2 = UtilityDB(str(tmp / "b.db"))
    await db2.connect()
    check(await migrate_legacy_json(db2, broken) == 0, "unparseable JSON imports nothing")
    check(broken.exists(), "and is left in place to be looked at")
    await db2.close()


if __name__ == "__main__":
    asyncio.run(main())
