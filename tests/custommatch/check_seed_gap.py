"""Verify the Overwatch fixes against real code paths.

1. rank setup now reaches ow_role_stats (the balancer's actual input)
2. losses floor at the game's lowest band, not a hardcoded 500
3. a new role seeds one band down, recoverable inside placements
4. climb rates land where we tuned them
"""
import asyncio
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch import database as db_mod

db_mod.DB_PATH = HERE / "test_custommatch.db"
from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import (
    OW_ROLES, OW_NEW_ROLE_RANK_DROP, ow_seed_mmr_for_new_role,
    K_FACTOR_PLACEMENT, K_FACTOR_LEARNING, K_FACTOR_STABLE, PlayerStats,
)

GAME_ID, PID = 3, 1000099


async def cleanup():
    async with DB._get_db() as db:
        await db.execute("DELETE FROM ow_role_stats WHERE player_id=?", (PID,))
        await db.execute("DELETE FROM player_game_stats WHERE player_id=?", (PID,))
        await db.execute("DELETE FROM players WHERE player_id=?", (PID,))
        await db.commit()


async def main():
    fails = []
    await cleanup()
    async with DB._get_db() as db:
        await db.execute("INSERT OR IGNORE INTO players (player_id) VALUES (?)", (PID,))
        await db.commit()

    roles = await DB.get_mmr_roles(GAME_ID)
    gm_role_id = max(roles, key=lambda r: roles[r])
    mmr = roles[gm_role_id]

    # --- what the fixed MMRRoleSelectView.on_select now does ---
    stats = await DB.get_player_stats(PID, GAME_ID)
    stats.mmr = mmr
    await DB.update_player_stats(stats)
    seeded = await DB.seed_ow_role_stats_from_rank(PID, GAME_ID, mmr)
    # -----------------------------------------------------------

    print("=" * 70)
    print("1. RANK SETUP -> PER-ROLE MMR")
    print("=" * 70)
    print(f"  admin picks Grandmaster ({mmr}); seeded roles: {', '.join(seeded)}\n")
    for r in OW_ROLES:
        rs = await DB.get_ow_role_stats(PID, GAME_ID, r)
        ok = rs.effective_mmr == mmr
        print(f"   {r:<8} balancer reads {rs.effective_mmr:<6} "
              f"games_played={rs.games_played}  {'OK' if ok else 'FAIL'}")
        if not ok:
            fails.append(f"{r} seeded at {rs.effective_mmr}, expected {mmr}")

    peak = await DB.get_ow_player_peak_mmr(PID, GAME_ID)
    print(f"\n   peak MMR (drives rank role after matches) = {peak}"
          f"  {'OK' if peak == mmr else 'FAIL'}")
    if peak != mmr:
        fails.append(f"peak mmr {peak}, expected {mmr}")

    # Re-running must not clobber an earned rating.
    rs = await DB.get_ow_role_stats(PID, GAME_ID, "Tank")
    rs.mmr, rs.games_played = 5500, 40
    await DB.upsert_ow_role_stats(rs)
    again = await DB.seed_ow_role_stats_from_rank(PID, GAME_ID, mmr)
    after = await DB.get_ow_role_stats(PID, GAME_ID, "Tank")
    ok = after.mmr == 5500 and not again
    print(f"   re-run is safe: Tank still {after.mmr}, re-seeded {again or 'nothing'}"
          f"  {'OK' if ok else 'FAIL'}")
    if not ok:
        fails.append("re-seeding clobbered an earned rating")

    print("\n" + "=" * 70)
    print("2. LOSS FLOOR")
    print("=" * 70)
    for gid, name in ((1, "Valorant"), (2, "Rivals"), (3, "Overwatch")):
        floor = await DB.get_mmr_floor(gid)
        bands = sorted((await DB.get_mmr_roles(gid)).values())
        lowest = bands[0] if bands else None
        ok = floor == lowest
        print(f"   {name:<10} floor={floor:<5} lowest band={lowest:<5} "
              f"{'OK' if ok else 'FAIL'}")
        if not ok:
            fails.append(f"{name} floor {floor} != lowest band {lowest}")

    print("\n" + "=" * 70)
    print("3. NEW-ROLE SEEDING (drop = %d band)" % OW_NEW_ROLE_RANK_DROP)
    print("=" * 70)
    th = sorted((await DB.get_mmr_roles(GAME_ID)).values())
    lbl = {800: "Bronze", 1200: "Silver", 1800: "Gold", 2500: "Plat",
           3200: "Diamond", 4000: "Master", 4900: "GM", 6000: "Champion"}
    placement_reach = int(K_FACTOR_PLACEMENT * 0.5) * 10
    print(f"   placement covers {placement_reach} pts over 10 games\n")
    for m in th:
        s = ow_seed_mmr_for_new_role(m, th)
        gap = m - s
        ok = gap <= placement_reach
        print(f"   {lbl[m]:>9} {m:>5} -> {s:>5} ({lbl.get(s,'?'):<8}) "
              f"gap {gap:>4}  {'recovers in placements' if ok else 'FAIL: too deep'}")
        if not ok:
            fails.append(f"new-role gap {gap} from {m} exceeds placement reach")

    print("\n" + "=" * 70)
    print("4. CLIMB RATES (K-factors read from models.py)")
    print("=" * 70)
    print(f"   placement K={K_FACTOR_PLACEMENT} learning K={K_FACTOR_LEARNING} "
          f"stable K={K_FACTOR_STABLE}")
    per_game = K_FACTOR_STABLE * 0.5
    print(f"   an even match moves +/-{per_game:.0f}\n")
    print(f"   {'tier':<24}{'width':>7}{'@55%':>8}{'@60%':>8}{'@70%':>8}")
    for i in range(len(th) - 1):
        w = th[i + 1] - th[i]
        row = f"   {lbl[th[i]] + ' -> ' + lbl[th[i+1]]:<24}{w:>7}"
        for wr in (0.55, 0.60, 0.70):
            row += f"{w / ((2 * wr - 1) * per_game):>8.0f}"
        print(row)
    total = th[-1] - th[0]
    print(f"\n   full ladder ({total} pts) @60%: "
          f"{total / (0.2 * per_game):.0f} games")

    # K-factor schedule sanity via the real dataclass
    print()
    for gp, expect, name in ((0, K_FACTOR_PLACEMENT, "game 1"),
                             (15, K_FACTOR_LEARNING, "game 15"),
                             (40, K_FACTOR_STABLE, "game 40")):
        got = PlayerStats(player_id=1, game_id=1, games_played=gp).get_k_factor()
        ok = got == expect
        print(f"   {name:<10} K={got:<5} {'OK' if ok else 'FAIL'}")
        if not ok:
            fails.append(f"{name} K={got}, expected {expect}")

    await cleanup()

    print("\n" + "=" * 70)
    if fails:
        print(f"{len(fails)} FAILURES")
        for f in fails:
            print(f"  !! {f}")
    else:
        print("all checks passed")
    print("=" * 70)
    return 1 if fails else 0


sys.exit(asyncio.run(main()))
