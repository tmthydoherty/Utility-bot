"""How long does the 2-2-2 balancer take? It runs on the queue-pop path."""
import asyncio, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch import database as db_mod
db_mod.DB_PATH = HERE / "test_custommatch.db"
from cogs.custommatch import cog as cog_mod
from cogs.custommatch.database import DatabaseHelper as DB

GAME_ID = 3
PIDS = [1000301 + i for i in range(12)]
T, D, S = "Tank", "DPS", "Support"

async def main():
    cog = cog_mod.CustomMatch.__new__(cog_mod.CustomMatch)
    async with DB._get_db() as db:
        for p in PIDS:
            await db.execute("INSERT OR IGNORE INTO players (player_id) VALUES (?)", (p,))
        await db.commit()
    # worst case for the enumerator: everyone flexes all three roles
    for pid in PIDS:
        await DB.set_ow_role_selection(pid, GAME_ID, [T, D, S])
        await DB.sync_ow_role_seed(pid, GAME_ID, 3200)

    for label in ("all-flex (worst case)", "one-tricks (typical)"):
        if label.startswith("one"):
            roles = [[T]]*4 + [[D]]*4 + [[S]]*4
            for pid, r in zip(PIDS, roles):
                await DB.set_ow_role_selection(pid, GAME_ID, r)
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            await cog.balance_teams_overwatch(list(PIDS), GAME_ID)
            times.append(time.perf_counter() - t0)
        print(f"   {label:<24} min={min(times)*1000:7.0f}ms  "
              f"max={max(times)*1000:7.0f}ms  avg={sum(times)/len(times)*1000:7.0f}ms")

    async with DB._get_db() as db:
        ids = ",".join(str(p) for p in PIDS)
        await db.execute(f"DELETE FROM ow_role_selection WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM ow_role_stats WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM players WHERE player_id IN ({ids})")
        await db.commit()

print("balance_teams_overwatch timing (12 players, 924 splits enumerated)")
asyncio.run(main())
