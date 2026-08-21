#!/usr/bin/env python3
"""Synthetic 12-player harness for the Overwatch strict-2-2-2 path.

Runs entirely against a COPY of the live database (see DB_COPY below) with 12
fake player ids that are far too small to be Discord snowflakes. Nothing here
touches the live db, Discord, or any real player row.

What it exercises:
  * models.ow_can_form_222        -- the queue viability gate (bipartite match)
  * cog.balance_teams_overwatch   -- the strict 2-2-2 weighted balancer
  * database.get_ow_role_stats    -- new-role MMR seeding

Usage:  venv/bin/python3 ow_harness.py [-v]
"""

import asyncio
import logging
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = Path(__file__).resolve().parents[2]
DB_COPY = HERE / "test_custommatch.db"

sys.path.insert(0, str(REPO))

# Point every db access at the scratch copy BEFORE anything opens a connection.
from cogs.custommatch import database as db_mod

db_mod.DB_PATH = DB_COPY

from cogs.custommatch import cog as cog_mod
from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import OW_ROLES, ow_can_form_222

cog_mod.DB_PATH = DB_COPY

GAME_NAME = "Overwatch"
# Synthetic player ids: 7 digits, so they can never collide with a snowflake.
BASE_ID = 1000001
PLAYER_IDS = [BASE_ID + i for i in range(12)]

T, D, S = "Tank", "DPS", "Support"
ALL = [T, D, S]

VERBOSE = "-v" in sys.argv


# ---------------------------------------------------------------------------
# Log capture -- the balancer reports every fallback through the module logger,
# so a scenario that silently degrades still shows up in the report.
# ---------------------------------------------------------------------------
class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def drain(self, min_level=logging.WARNING):
        out = [f"{r.levelname}: {r.getMessage()}" for r in self.records
               if r.levelno >= min_level]
        self.records.clear()
        return out


CAPTURE = _Capture()


# ---------------------------------------------------------------------------
# Scenario definition
# ---------------------------------------------------------------------------
class Scenario:
    """roster: list of 12 (roles, {role: mmr}) tuples, one per PLAYER_IDS slot."""

    def __init__(self, name, roster, expect_feasible=True, note=""):
        self.name = name
        self.roster = roster
        self.expect_feasible = expect_feasible
        self.note = note


def flat(roles, mmr):
    """Same mmr on every selected role."""
    return (roles, {r: mmr for r in roles})


def build_scenarios():
    sc = []

    # A. Clean one-tricks: exactly 4 of each role, wide mmr spread.
    sc.append(Scenario(
        "A. one-tricks 4/4/4",
        [flat([T], 2600), flat([T], 2400), flat([T], 1800), flat([T], 1200),
         flat([D], 2800), flat([D], 2300), flat([D], 1900), flat([D], 1100),
         flat([S], 2500), flat([S], 2200), flat([S], 1700), flat([S], 1300)],
        note="the textbook case -- only one legal role per player",
    ))

    # B. Everyone full-flex, distinct mmr per role.
    sc.append(Scenario(
        "B. all 12 full-flex",
        [(ALL, {T: 2000 + i * 40, D: 2200 - i * 30, S: 1800 + i * 50})
         for i in range(12)],
        note="maximum freedom -- balancer picks roles as well as teams",
    ))

    # C. Nobody plays Tank.
    sc.append(Scenario(
        "C. zero tank coverage",
        [flat([D], 2000) for _ in range(6)] + [flat([S], 2000) for _ in range(6)],
        expect_feasible=False,
        note="gate must hold the queue -- deficit Tank=4",
    ))

    # D. One tank short (3 tank-capable, need 4).
    sc.append(Scenario(
        "D. one tank short",
        [flat([T], 2000), flat([T], 2000), flat([T], 2000),
         flat([D], 2000), flat([D], 2000), flat([D], 2000), flat([D], 2000),
         flat([S], 2000), flat([S], 2000), flat([S], 2000), flat([S], 2000),
         flat([D, S], 2000)],
        expect_feasible=False,
        note="off-by-one at the gate -- deficit Tank=1",
    ))

    # E. Tank-stacked: 8 tank-only, 2 dps, 2 support.
    sc.append(Scenario(
        "E. tank-stacked queue",
        [flat([T], 2000) for _ in range(8)]
        + [flat([D], 2000), flat([D], 2000), flat([S], 2000), flat([S], 2000)],
        expect_feasible=False,
        note="4 surplus tanks cannot cover dps/support",
    ))

    # F. Greedy trap: the 4 flex players must ALL be pushed to Support.
    #    A greedy fill would seat them on Tank (already covered) and report a
    #    false Support deficit; the bipartite matching has to back out.
    sc.append(Scenario(
        "F. greedy trap (flex must all go Support)",
        [flat([T], 2400), flat([T], 2300), flat([T], 2100), flat([T], 1900),
         flat([D], 2500), flat([D], 2200), flat([D], 2000), flat([D], 1800),
         flat([T, S], 2300), flat([T, S], 2150), flat([T, S], 1950), flat([T, S], 1750)],
        note="tests that the gate re-matches instead of filling first-come",
    ))

    # G. Extreme spread -- top-2 separation under pressure.
    sc.append(Scenario(
        "G. extreme mmr spread",
        [flat([T], 4000), flat([T], 3900), flat([T], 1050), flat([T], 1000),
         flat([D], 3800), flat([D], 1100), flat([D], 1050), flat([D], 1000),
         flat([S], 3700), flat([S], 1100), flat([S], 1050), flat([S], 1000)],
        note="two smurfs must land on opposite teams",
    ))

    # H. Two players never picked a role -- code treats them as fill and warns.
    sc.append(Scenario(
        "H. missing role selections",
        [([], {}), ([], {}),
         flat([T], 2200), flat([T], 2000),
         flat([D], 2400), flat([D], 2100), flat([D], 1900),
         flat([S], 2300), flat([S], 2000), flat([S], 1800),
         flat([D, S], 2100), flat([T, S], 2050)],
        note="expect 2 'no OW role selection' warnings, still a valid 2-2-2",
    ))

    # J. Role-stacking pressure -- two clearly-best players in EVERY role, with a
    #    compensating elite pair on the other axis, so a balance-only split could
    #    legally stack the two best tanks (or dps/support) on one team. The role
    #    top-2 rules must break them apart on every axis they can.
    sc.append(Scenario(
        "J. role-stacking pressure",
        [flat([T], 4000), flat([T], 3950), flat([T], 2000), flat([T], 1950),
         flat([D], 4000), flat([D], 3950), flat([D], 2000), flat([D], 1950),
         flat([S], 4000), flat([S], 3950), flat([S], 2000), flat([S], 1950)],
        note="the two best of each role must not share a team",
    ))

    # I. Narrow flex on top of a bare-minimum roster.
    sc.append(Scenario(
        "I. minimum viable coverage",
        [flat([T], 2000), flat([T], 1900), flat([T, D], 2100), flat([T, S], 1800),
         flat([D], 2200), flat([D], 2000), flat([D, S], 1950),
         flat([S], 2150), flat([S], 1850), flat([S, T], 2050),
         flat([D], 1700), flat([S], 1600)],
        note="exactly-4 coverage per role once flex is resolved",
    ))

    return sc


# ---------------------------------------------------------------------------
# Fixture management
# ---------------------------------------------------------------------------
async def clear_fixtures(game_id):
    async with DB._get_db() as db:
        ids = ",".join(str(p) for p in PLAYER_IDS)
        await db.execute(f"DELETE FROM ow_role_selection WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM ow_role_stats WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM player_game_stats WHERE player_id IN ({ids})")
        await db.execute(f"DELETE FROM players WHERE player_id IN ({ids})")
        await db.commit()


async def load_scenario(sc, game_id):
    await clear_fixtures(game_id)
    async with DB._get_db() as db:
        for pid in PLAYER_IDS:
            await db.execute(
                "INSERT OR IGNORE INTO players (player_id) VALUES (?)", (pid,))
        await db.commit()

    for pid, (roles, mmrs) in zip(PLAYER_IDS, sc.roster):
        if roles:
            await DB.set_ow_role_selection(pid, game_id, list(roles))
        for role, mmr in mmrs.items():
            stats = await DB.get_ow_role_stats(pid, game_id, role)
            stats.mmr = mmr
            stats.games_played = 25          # past placement, stable K
            stats.wins, stats.losses = 13, 12
            await DB.upsert_ow_role_stats(stats)


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------
def check_split(red, blue, role_map, sel_map, weights, role_mmr, skipped_seps=()):
    """Returns (list_of_failures, weighted_diff or None).

    skipped_seps holds the separation labels the balancer logged as
    over-constrained on this run; a pair it deliberately gave up on is not
    counted against it here.
    """
    fails = []

    if sorted(red + blue) != sorted(PLAYER_IDS):
        fails.append(f"teams are not a partition of the 12 players "
                     f"(red={len(red)} blue={len(blue)})")
        return fails, None
    if len(red) != 6 or len(blue) != 6:
        fails.append(f"not a 6v6 split: red={len(red)} blue={len(blue)}")
    if set(red) & set(blue):
        fails.append(f"player on both teams: {sorted(set(red) & set(blue))}")

    if not role_map:
        fails.append("EMPTY role map -- fell back to the single-MMR balancer, "
                     "so no 2-2-2 was enforced")
        return fails, None

    missing = [p for p in PLAYER_IDS if p not in role_map]
    if missing:
        fails.append(f"players with no assigned role: {missing}")

    for label, team in (("red", red), ("blue", blue)):
        counts = {r: sum(1 for p in team if role_map.get(p) == r) for r in OW_ROLES}
        if counts != {T: 2, D: 2, S: 2}:
            fails.append(f"{label} is not 2-2-2: {counts}")

    for pid, role in role_map.items():
        allowed = sel_map.get(pid) or set(OW_ROLES)
        if role not in allowed:
            fails.append(f"player {pid} assigned {role} but only selected "
                         f"{sorted(allowed)}")

    # Top-2 separation, computed the same way the balancer does.
    peak = {pid: max(weights[r] * role_mmr[(pid, r)]
                     for r in (sel_map.get(pid) or set(OW_ROLES)))
            for pid in PLAYER_IDS}
    ranked = sorted(PLAYER_IDS, key=lambda p: peak[p], reverse=True)
    top1, top2 = ranked[0], ranked[1]
    if (top1 in set(red)) == (top2 in set(red)):
        fails.append(f"top-2 players {top1} and {top2} landed on the same team")

    # Per-role top-2 separation: the two best of each role (by that role's own
    # MMR, among players who selected it) should sit on opposite teams. Tank is a
    # hard rule; DPS/Support yield if over-constrained, so a run the balancer
    # logged as skipped is exempt.
    red_set = set(red)
    for role in OW_ROLES:
        eligible = sorted(
            (p for p in PLAYER_IDS if role in (sel_map.get(p) or set(OW_ROLES))),
            key=lambda p: role_mmr[(p, role)], reverse=True,
        )
        if len(eligible) < 2:
            continue
        label = f"top-2 {role}"
        if label in skipped_seps:
            continue
        r1, r2 = eligible[0], eligible[1]
        if (r1 in red_set) == (r2 in red_set):
            fails.append(f"{label} players {r1} and {r2} landed on the same team")

    def strength(team):
        return sum(weights[role_map[p]] * role_mmr[(p, role_map[p])] for p in team)

    return fails, abs(strength(red) - strength(blue))


async def gather_inputs(game_id):
    """Re-derive sel_map / role_mmr / weights exactly as the balancer does."""
    raw = await DB.get_bulk_ow_role_selection(PLAYER_IDS, game_id)
    sel_map = {}
    for pid in PLAYER_IDS:
        roles = {r for r, _rank in raw.get(pid, [])}
        sel_map[pid] = roles or set(OW_ROLES)
    role_mmr = {}
    for pid in PLAYER_IDS:
        for role in sel_map[pid]:
            st = await DB.get_ow_role_stats(pid, game_id, role)
            role_mmr[(pid, role)] = st.effective_mmr
    weights = await DB.get_ow_role_weights(game_id)
    return sel_map, role_mmr, weights


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
RUNS_PER_SCENARIO = 25  # the balancer picks randomly among tied candidates


async def run_scenario(sc, cog, game_id):
    await load_scenario(sc, game_id)
    sel_map, role_mmr, weights = await gather_inputs(game_id)

    feasible, deficit = ow_can_form_222(sel_map)
    logs = CAPTURE.drain()

    gate_ok = feasible == sc.expect_feasible
    result = {
        "name": sc.name, "note": sc.note, "gate_ok": gate_ok,
        "feasible": feasible, "expected": sc.expect_feasible,
        "deficit": {r: d for r, d in deficit.items() if d},
        "fails": [], "diffs": [], "logs": [],
    }

    if not gate_ok:
        result["fails"].append(
            f"viability gate said feasible={feasible}, expected {sc.expect_feasible}")

    # Only roster the balancer on rosters the gate passes -- that mirrors the
    # cog, which refuses to pop the queue until a 2-2-2 can form.
    if feasible:
        for _ in range(RUNS_PER_SCENARIO):
            CAPTURE.drain(logging.DEBUG)
            red, blue, role_map = await cog.balance_teams_overwatch(
                list(PLAYER_IDS), game_id)
            run_logs = CAPTURE.drain()
            skipped_seps = {
                lab for lab in ("top-2 MMR", "top-2 Tank", "bottom-2 MMR",
                                "top-2 DPS", "top-2 Support")
                if any(f"{lab} separation over-constrained" in line
                       for line in run_logs)
            }
            fails, diff = check_split(red, blue, role_map, sel_map, weights,
                                      role_mmr, skipped_seps)
            if fails:
                result["fails"].extend(fails)
            if diff is not None:
                result["diffs"].append(diff)
            for line in run_logs:
                if line not in result["logs"]:
                    result["logs"].append(line)
    else:
        result["logs"].extend(logs)

    # Dedupe while preserving order.
    seen = set()
    result["fails"] = [f for f in result["fails"]
                       if not (f in seen or seen.add(f))]
    return result


async def check_seeding(cog, game_id):
    """A player with only a Tank rating should get a sane seed on a new role."""
    await clear_fixtures(game_id)
    pid = PLAYER_IDS[0]
    async with DB._get_db() as db:
        await db.execute("INSERT OR IGNORE INTO players (player_id) VALUES (?)", (pid,))
        await db.commit()
    st = await DB.get_ow_role_stats(pid, game_id, T)
    st.mmr, st.games_played = 3000, 30
    await DB.upsert_ow_role_stats(st)

    seeded = await DB.get_ow_role_stats(pid, game_id, D)
    thresholds = list((await DB.get_mmr_roles(game_id)).values())
    await clear_fixtures(game_id)
    return {
        "tank_mmr": 3000,
        "seeded_dps_mmr": seeded.mmr,
        "is_new": seeded.is_new,
        "thresholds_configured": len(thresholds),
    }


async def main():
    logging.getLogger("cogs.custommatch").addHandler(CAPTURE)
    logging.getLogger("cogs.custommatch").setLevel(logging.DEBUG)
    logging.getLogger("cogs.custommatch").propagate = False

    if not DB_COPY.exists():
        print(f"FATAL: {DB_COPY} missing. Re-copy the live db first.")
        return 1
    print(f"db under test : {DB_COPY}")
    print(f"live db        : untouched\n")

    games = await DB.get_all_games()
    game = next((g for g in games if g.name.lower() == GAME_NAME.lower()), None)
    if game is None:
        print(f"FATAL: no game named {GAME_NAME!r} in the db copy.")
        return 1

    print(f"game           : {game.name} (id={game.game_id})")
    print(f"player_count   : {game.player_count}"
          f"{'' if game.player_count == 12 else '   <-- NOT 12; 2-2-2 gates are skipped'}")
    print(f"queue_type     : {game.queue_type}")
    print(f"role_required  : {game.role_required}")
    weights = await DB.get_ow_role_weights(game.game_id)
    print(f"role weights   : " + ", ".join(f"{r}={weights[r]}" for r in OW_ROLES))
    print()

    cog = cog_mod.CustomMatch.__new__(cog_mod.CustomMatch)

    results = []
    for sc in build_scenarios():
        results.append(await run_scenario(sc, cog, game.game_id))

    seed_info = await check_seeding(cog, game.game_id)

    # ---- report ----
    print("=" * 78)
    print(f"SCENARIOS ({RUNS_PER_SCENARIO} balancer runs each, to catch bad random picks)")
    print("=" * 78)
    n_fail = 0
    for r in results:
        ok = r["gate_ok"] and not r["fails"]
        n_fail += 0 if ok else 1
        print(f"\n[{'PASS' if ok else 'FAIL'}] {r['name']}")
        print(f"       {r['note']}")
        print(f"       gate: feasible={r['feasible']} (expected {r['expected']})"
              + (f", deficit={r['deficit']}" if r["deficit"] else ""))
        if r["diffs"]:
            print(f"       weighted team-strength diff: "
                  f"min={min(r['diffs']):.0f} max={max(r['diffs']):.0f} "
                  f"avg={sum(r['diffs']) / len(r['diffs']):.0f}")
        for f in r["fails"]:
            print(f"       !! {f}")
        for line in r["logs"]:
            print(f"       log: {line}")

    print("\n" + "=" * 78)
    print("NEW-ROLE MMR SEEDING")
    print("=" * 78)
    print(f"  tank mmr 3000 -> new DPS role seeds at {seed_info['seeded_dps_mmr']} "
          f"(is_new={seed_info['is_new']})")
    if seed_info["thresholds_configured"] == 0:
        print("  !! game_mmr_roles has no rank bands for this game, so the")
        print("     rank-band seeding in ow_seed_mmr_for_new_role cannot run and")
        print("     it falls back to the flat 85%-of-distance-above-1000 drop.")
    else:
        print(f"  {seed_info['thresholds_configured']} rank bands configured")

    print("\n" + "=" * 78)
    print(f"{len(results) - n_fail}/{len(results)} scenarios passed")
    print("=" * 78)
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
