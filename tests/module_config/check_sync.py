"""Verify the dashboard ↔ bot module-config bridge (utils/module_config_sync.py).

The bridge is a two-process protocol on one SQLite file, so what can silently
break is protocol, not syntax: a revision that never clears, an override that
outlives the change it described (and so pins the dashboard to a stale value
after a Discord-side edit), a snapshot replace that leaves orphan rows, or a
revision bump racing an apply. Each of those is checked here against a real
temp database, driving the dashboard side with raw SQL exactly as
dashboard/lib/bot/module-config.ts does.

Run: .venv/bin/python tests/module_config/check_sync.py
"""
import asyncio
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from utils.module_config_sync import ConfigSyncAgent, ModuleConfigStore

_failures = 0


def check(cond: bool, label: str) -> None:
    global _failures
    if cond:
        print(f"  ok   {label}")
    else:
        _failures += 1
        print(f"  FAIL {label}")


def dashboard_write(path: str, module: str, guild_id: str, values: dict, actor: str = "u1") -> None:
    """Mimic module-config.ts: upsert overrides and bump the module revision."""
    conn = sqlite3.connect(path, timeout=5.0)
    try:
        conn.execute("BEGIN")
        for key, value in values.items():
            conn.execute(
                "INSERT INTO overrides (module, guild_id, key, value, updated_ts, updated_by) "
                "VALUES (?, ?, ?, ?, 0, ?) "
                "ON CONFLICT(module, guild_id, key) DO UPDATE SET value = excluded.value",
                (module, guild_id, key, json.dumps(value), actor),
            )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, '1') "
            "ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT)",
            (f"revision:{module}",),
        )
        conn.commit()
    finally:
        conn.close()


def read_config(path: str, module: str, guild_id: str) -> dict:
    conn = sqlite3.connect(path, timeout=5.0)
    try:
        rows = conn.execute(
            "SELECT key, value FROM config WHERE module = ? AND guild_id = ?",
            (module, guild_id),
        ).fetchall()
        return {k: json.loads(v) for k, v in rows}
    finally:
        conn.close()


def count_overrides(path: str, module: str) -> int:
    conn = sqlite3.connect(path, timeout=5.0)
    try:
        return conn.execute("SELECT count(*) FROM overrides WHERE module = ?", (module,)).fetchone()[0]
    finally:
        conn.close()


async def main() -> None:
    tmp = tempfile.mkdtemp()
    path = str(Path(tmp) / "module_config.db")
    store = ModuleConfigStore(path)
    await store.ensure()

    # A stand-in cog: its "real config" is this dict, keyed by guild then key.
    cog_state: dict[str, dict] = {"100": {"welcome_channel_id": "5", "intro_channel_id": None}}

    def snapshot():
        return {gid: dict(vals) for gid, vals in cog_state.items()}

    def apply(guild_id: str, values: dict):
        cog_state.setdefault(guild_id, {}).update(values)

    agent = ConfigSyncAgent("welcome", snapshot, apply, store=store)

    # --- first tick publishes the current snapshot even with no changes -------
    await agent._tick()
    check(read_config(path, "welcome", "100") == {"welcome_channel_id": "5", "intro_channel_id": None},
          "first tick publishes the cog's current snapshot")

    # --- a dashboard save is read, applied, and reflected in the snapshot -----
    big_id = "1213141516171819202"  # bigger than 2**53, must survive as-is
    dashboard_write(path, "welcome", "100", {"welcome_channel_id": big_id})
    revision, pending = await store.read_pending("welcome")
    check(pending == {"100": {"welcome_channel_id": big_id}},
          "read_pending groups overrides by guild and preserves large IDs")

    await agent._tick()
    check(cog_state["100"]["welcome_channel_id"] == big_id,
          "apply writes the dashboard value into the cog")
    check(read_config(path, "welcome", "100")["welcome_channel_id"] == big_id,
          "the applied value is republished to config")

    # --- consumed overrides are cleared, so config becomes the truth ----------
    check(count_overrides(path, "welcome") == 0,
          "overrides are cleared once cleanly applied")
    _, pending_after = await store.read_pending("welcome")
    check(pending_after == {},
          "nothing pending after apply (revision caught up to applied)")

    # --- a Discord-side edit shows up without any override lingering ----------
    cog_state["100"]["intro_channel_id"] = "77"
    agent.mark_dirty()
    await agent._tick()
    check(read_config(path, "welcome", "100")["intro_channel_id"] == "77",
          "a change made in Discord is published to the dashboard")

    # --- snapshot replace drops keys that no longer exist ---------------------
    del cog_state["100"]["intro_channel_id"]
    agent.mark_dirty()
    await agent._tick()
    check("intro_channel_id" not in read_config(path, "welcome", "100"),
          "publish replaces the module's rows rather than merging")

    # --- a revision bump racing an apply is not lost --------------------------
    dashboard_write(path, "welcome", "100", {"welcome_channel_id": "8"})
    revision, pending = await store.read_pending("welcome")
    # Simulate a second save landing after read_pending but before mark_applied.
    dashboard_write(path, "welcome", "100", {"welcome_channel_id": "9"})
    await store.mark_applied("welcome", revision)
    check(count_overrides(path, "welcome") == 1,
          "overrides are kept when a newer revision raced the apply")
    revision2, pending2 = await store.read_pending("welcome")
    check(pending2 == {"100": {"welcome_channel_id": "9"}},
          "the raced-in change is still pending for the next tick")

    # --- modules don't see each other's rows ----------------------------------
    dashboard_write(path, "security", "100", {"alert_channel_id": "42"})
    _, welcome_pending = await store.read_pending("welcome")
    check("alert_channel_id" not in welcome_pending.get("100", {}),
          "read_pending is scoped to its own module")

    print()
    print("all passed" if _failures == 0 else f"{_failures} failed")
    sys.exit(1 if _failures else 0)


if __name__ == "__main__":
    asyncio.run(main())
