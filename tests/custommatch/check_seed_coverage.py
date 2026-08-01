"""Every path that seeds MMR from a rank must mirror it into ow_role_stats."""
import asyncio, re, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
SRC = Path(__file__).resolve().parents[2] / "cogs" / "custommatch"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch import database as db_mod
db_mod.DB_PATH = HERE / "test_custommatch.db"
from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import OW_ROLES

WINDOW = 22  # lines after the assignment in which the seed call must appear
# Elo updates and the per-role admin modal are not rank-seeding paths.
#
# The views_gameplay entries run the mirror in reverse: the Overwatch admin
# flows write ow_role_stats *directly*, per role, and then derive the aggregate
# column from the roles. Calling sync_ow_role_seed on those paths would be
# actively wrong -- it blanket-seeds every role that has no row yet, which is
# exactly the set those flows deliberately leave unranked so get_ow_role_stats
# can seed them from the player's best role later.
EXEMPT = {
    "cog.py": ["stats.mmr = max(mmr_floor"],
    "views_settings.py": ["stats.mmr = new_mmr - stats.admin_offset"],
    "views_gameplay.py": [
        "stats.mmr = peak - stats.admin_offset",     # SetMMRModal._submit_role
        "stats.mmr = mmr_val - stats.admin_offset",  # _ow_finalize_setup, per role
        "stats.mmr = best - stats.admin_offset",     # _ow_finalize_setup, aggregate
    ],
}

def coverage():
    rows, missing = [], []
    for fn in ("cog.py", "views_settings.py", "views_gameplay.py"):
        lines = (SRC / fn).read_text().splitlines()
        for i, line in enumerate(lines):
            if not re.search(r"^\s*stats\.mmr = ", line):
                continue
            if any(e in line for e in EXEMPT.get(fn, [])):
                rows.append((fn, i + 1, line.strip()[:44], "exempt"))
                continue
            window = "\n".join(lines[i:i + WINDOW])
            ok = "sync_ow_role_seed" in window
            rows.append((fn, i + 1, line.strip()[:44], "OK" if ok else "MISSING"))
            if not ok:
                missing.append(f"{fn}:{i+1}  {line.strip()}")
    return rows, missing

async def behaviour():
    fails = []
    PID = 1000098
    print("\n-- sync_ow_role_seed per game --")
    for gid, name in ((1, "Valorant"), (2, "Rivals"), (3, "Overwatch")):
        async with DB._get_db() as db:
            await db.execute("DELETE FROM ow_role_stats WHERE player_id=?", (PID,))
            await db.commit()
        got = await DB.sync_ow_role_seed(PID, gid, 4000)
        want = list(OW_ROLES) if gid == 3 else []
        ok = got == want
        shown = ', '.join(got) if got else 'nothing'
        print(f"   {name:<10} seeded {shown:<26} {'OK' if ok else 'FAIL'}")
        if not ok:
            fails.append(f"{name}: seeded {got}, expected {want}")
    async with DB._get_db() as db:
        await db.execute("DELETE FROM ow_role_stats WHERE player_id=?", (PID,))
        await db.commit()
    return fails

async def main():
    rows, missing = coverage()
    print("MMR-seeding call sites")
    print("-" * 74)
    for fn, ln, code, status in rows:
        print(f"  {fn:<20}:{ln:<6}{code:<46}{status}")
    fails = await behaviour()
    fails += missing
    print("\n" + "=" * 74)
    if fails:
        print(f"{len(fails)} FAILURES")
        for f in fails: print("  !!", f)
    else:
        print("all seeding paths covered")
    print("=" * 74)
    return 1 if fails else 0

sys.exit(asyncio.run(main()))
