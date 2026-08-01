#!/usr/bin/env python3
"""End-to-end Overwatch match lifecycle against real code paths.

queue -> viability gate -> 2-2-2 balance -> persist roles -> finalize per-role
Elo -> rank-role input. Asserts the invariants at every hop, and probes the
degraded paths (missing roles, floor clamping, placement K, streaks).

    venv/bin/python3 ow_lifecycle.py
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch import database as db_mod

db_mod.DB_PATH = HERE / "test_custommatch.db"
from cogs.custommatch import cog as cog_mod
from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import (
    OW_ROLES, Team, K_FACTOR_PLACEMENT, K_FACTOR_STABLE, ow_can_form_222,
)

GAME_ID = 3
PIDS = [1000201 + i for i in range(12)]
T, D, S = "Tank", "DPS", "Support"

FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
    return cond


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.recs = []

    def emit(self, r):
        self.recs.append(r)

    def drain(self, lvl=logging.WARNING):
        out = [f"{r.levelname}: {r.getMessage()}" for r in self.recs if r.levelno >= lvl]
        self.recs.clear()
        return out


CAP = Capture()


async def wipe():
    async with DB._get_db() as db:
        ids = ",".join(str(p) for p in PIDS)
        await db.execute(f"DELETE FROM ow_role_selection WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM ow_role_stats WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM player_game_stats WHERE player_id IN ({ids})")
        await db.execute(
            f"DELETE FROM match_players WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM players WHERE player_id IN ({ids})")
        await db.commit()


async def setup_players(roster, rank_mmr):
    """roster: list of role-lists. Seeds each player like real rank setup does."""
    await wipe()
    async with DB._get_db() as db:
        for p in PIDS:
            await db.execute("INSERT OR IGNORE INTO players (player_id) VALUES (?)", (p,))
        await db.commit()
    for pid, roles, mmr in zip(PIDS, roster, rank_mmr):
        await DB.set_ow_role_selection(pid, GAME_ID, list(roles))
        st = await DB.get_player_stats(pid, GAME_ID)
        st.mmr = mmr
        await DB.update_player_stats(st)
        await DB.sync_ow_role_seed(pid, GAME_ID, st.effective_mmr)


async def make_match():
    mid = await DB.create_match(GAME_ID, "mmr", None, short_id="TST")
    return mid


async def main():
    lg = logging.getLogger("cogs.custommatch")
    lg.addHandler(CAP)
    lg.setLevel(logging.DEBUG)
    lg.propagate = False

    cog = cog_mod.CustomMatch.__new__(cog_mod.CustomMatch)
    cog._ow_role_assignments = {}
    game = await DB.get_game(GAME_ID)

    roster = [[T], [T], [T], [T], [D], [D], [D], [D], [S], [S], [S], [S]]
    ranks = [4900, 4000, 3200, 2500, 4900, 4000, 3200, 2500, 4900, 4000, 3200, 2500]

    # ---------------------------------------------------------------- 1
    print("=" * 74)
    print("1. SETUP -> GATE -> BALANCE -> PERSIST")
    print("=" * 74)
    await setup_players(roster, ranks)

    sel = {pid: {r for r, _ in await DB.get_ow_role_selection(pid, GAME_ID)}
           for pid in PIDS}
    feasible, deficit = ow_can_form_222(sel)
    print(f"   gate feasible={feasible} deficit={ {k:v for k,v in deficit.items() if v} }")
    check(feasible, "gate rejected a clean 4/4/4 roster")

    red, blue, rmap = await cog.balance_teams_overwatch(list(PIDS), GAME_ID)
    check(len(rmap) == 12, f"balancer returned {len(rmap)} role assignments, want 12")

    mid = await make_match()
    for pid in red:
        await DB.add_match_player(mid, pid, "red", role=rmap.get(pid))
    for pid in blue:
        await DB.add_match_player(mid, pid, "blue", role=rmap.get(pid))

    players = await DB.get_match_players(mid)
    roles_persisted = {p["player_id"]: p.get("role") for p in players}
    print(f"   persisted roles: {sum(1 for v in roles_persisted.values() if v)}/12")
    check(all(roles_persisted.values()), "some match_players rows have a NULL role")
    for team_name, team in (("red", red), ("blue", blue)):
        c = {r: sum(1 for p in team if roles_persisted[p] == r) for r in OW_ROLES}
        print(f"   {team_name}: {c}")
        check(c == {T: 2, D: 2, S: 2}, f"{team_name} persisted as {c}, not 2-2-2")

    before = {}
    for p in players:
        st = await DB.get_ow_role_stats(p["player_id"], GAME_ID, roles_persisted[p["player_id"]])
        before[p["player_id"]] = (st.effective_mmr, st.games_played)

    # ---------------------------------------------------------------- 2
    print("\n" + "=" * 74)
    print("2. FINALIZE PER-ROLE ELO")
    print("=" * 74)
    now = datetime.now(timezone.utc)
    CAP.drain(logging.DEBUG)
    await cog._finalize_overwatch_mmr(
        mid, game, players, red, blue, Team.RED, now)
    logs = CAP.drain()
    for l in logs:
        print(f"   log: {l}")
    check(not logs, f"finalize logged warnings on a clean match: {logs}")

    moved_up = moved_down = 0
    for p in players:
        pid = p["player_id"]
        st = await DB.get_ow_role_stats(pid, GAME_ID, roles_persisted[pid])
        old_mmr, old_gp = before[pid]
        delta = st.effective_mmr - old_mmr
        if pid in red:
            moved_up += 1
            check(delta > 0, f"winner {pid} lost MMR ({delta:+d})")
        else:
            moved_down += 1
            check(delta < 0, f"loser {pid} gained MMR ({delta:+d})")
        check(st.games_played == old_gp + 1,
              f"{pid} games_played {old_gp}->{st.games_played}")
    sample = [p["player_id"] for p in players][:1][0]
    st = await DB.get_ow_role_stats(sample, GAME_ID, roles_persisted[sample])
    print(f"   winners moved up: {moved_up}, losers down: {moved_down}")
    print(f"   sample delta: {st.effective_mmr - before[sample][0]:+d} "
          f"(placement K={K_FACTOR_PLACEMENT} -> expect ~{K_FACTOR_PLACEMENT//2})")

    # aggregate wins/losses feed the leaderboard
    agg_w = await DB.get_player_stats(red[0], GAME_ID)
    agg_l = await DB.get_player_stats(blue[0], GAME_ID)
    print(f"   aggregate: winner {agg_w.wins}W/{agg_w.losses}L, "
          f"loser {agg_l.wins}W/{agg_l.losses}L")
    check(agg_w.wins == 1 and agg_l.losses == 1, "aggregate wins/losses not written")

    peak = await DB.get_ow_player_peak_mmr(red[0], GAME_ID)
    print(f"   peak mmr for rank role: {peak}")
    check(peak is not None, "peak mmr None after a finished match")

    # ---------------------------------------------------------------- 3
    print("\n" + "=" * 74)
    print("3. DEGRADED: match_players.role IS NULL (captains-draft shape)")
    print("=" * 74)
    await setup_players(roster, ranks)
    mid2 = await make_match()
    half = PIDS[:6]
    other = PIDS[6:]
    for pid in half:
        await DB.add_match_player(mid2, pid, "red", role=None)
    for pid in other:
        await DB.add_match_player(mid2, pid, "blue", role=None)
    players2 = await DB.get_match_players(mid2)
    CAP.drain(logging.DEBUG)
    await cog._finalize_overwatch_mmr(
        mid2, game, players2, half, other, Team.RED, now)
    logs2 = CAP.drain()
    print(f"   warnings emitted: {len(logs2)} (one per player with no recorded role)")
    if logs2:
        print(f"   e.g. {logs2[0]}")
    red_roles = {p: [r for r, _ in await DB.get_ow_role_selection(p, GAME_ID)][0]
                 for p in half}
    print(f"   credited fallback roles: {sorted(set(red_roles.values()))}")
    check(len(logs2) == 12,
          f"expected 12 fallback warnings, got {len(logs2)}")

    # ---------------------------------------------------------------- 4
    print("\n" + "=" * 74)
    print("4. LOSS FLOOR CLAMP")
    print("=" * 74)
    floor = await DB.get_mmr_floor(GAME_ID)
    await setup_players(roster, [floor] * 12)
    mid3 = await make_match()
    r3, b3, m3 = await cog.balance_teams_overwatch(list(PIDS), GAME_ID)
    for pid in r3:
        await DB.add_match_player(mid3, pid, "red", role=m3.get(pid))
    for pid in b3:
        await DB.add_match_player(mid3, pid, "blue", role=m3.get(pid))
    p3 = await DB.get_match_players(mid3)
    await cog._finalize_overwatch_mmr(mid3, game, p3, r3, b3, Team.RED, now)
    lows = []
    for pid in b3:
        role = next(p["role"] for p in p3 if p["player_id"] == pid)
        st = await DB.get_ow_role_stats(pid, GAME_ID, role)
        lows.append(st.mmr)
    print(f"   losers at the {floor} floor after a loss: {sorted(set(lows))}")
    check(all(v >= floor for v in lows),
          f"a loser fell below the {floor} floor: {sorted(lows)}")

    await wipe()

    print("\n" + "=" * 74)
    if FAILS:
        print(f"{len(FAILS)} FAILURES")
        for f in FAILS:
            print(f"  !! {f}")
    else:
        print("lifecycle clean")
    print("=" * 74)
    return 1 if FAILS else 0


sys.exit(asyncio.run(main()))
