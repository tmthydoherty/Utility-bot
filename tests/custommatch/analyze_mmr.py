#!/usr/bin/env python3
"""Read-only audit of the live MMR system for Valorant and Rivals.

Asks the only question that matters: does MMR predict who wins? Then looks at
whether the ladder is actually being used, how fast ratings move, and whether
the ratings still line up with the rank roles they were seeded from.
"""

import sqlite3
import statistics
from collections import defaultdict

DB = "file:/home/tmthy/Vibey/data/custommatch.db?mode=ro"
GAMES = [(1, "Valorant"), (2, "Rivals")]

db = sqlite3.connect(DB, uri=True)
db.row_factory = sqlite3.Row


def bands(gid):
    return sorted(r[0] for r in db.execute(
        "SELECT mmr_value FROM game_mmr_roles WHERE game_id=?", (gid,)))


def band_of(mmr, bs, labels):
    cur = None
    for b in bs:
        if mmr >= b:
            cur = b
    return labels.get(cur, f"<{bs[0]}" if bs else "?")


for gid, name in GAMES:
    bs = bands(gid)
    lab = {r["mmr_value"]: (r["label"] or str(r["mmr_value"]))
           for r in db.execute("SELECT mmr_value,label FROM game_mmr_roles WHERE game_id=?", (gid,))}

    print("=" * 78)
    print(f"{name}  (game_id={gid})")
    print("=" * 78)

    m = db.execute(
        """SELECT COUNT(*) n, MIN(decided_at) lo, MAX(decided_at) hi
           FROM matches WHERE game_id=? AND winning_team IS NOT NULL AND cancelled=0""",
        (gid,)).fetchone()
    print(f"  decided matches : {m['n']}")
    print(f"  date range      : {(m['lo'] or '?')[:10]} -> {(m['hi'] or '?')[:10]}")

    hist = db.execute(
        "SELECT COUNT(*) n, AVG(ABS(change)) a, MAX(ABS(change)) mx FROM mmr_history WHERE game_id=?",
        (gid,)).fetchone()
    print(f"  mmr_history rows: {hist['n']}"
          + (f"   avg |change| {hist['a']:.1f}   max {hist['mx']}" if hist['n'] else ""))

    # ---- ladder occupancy -------------------------------------------------
    rows = list(db.execute(
        """SELECT player_id, mmr+admin_offset AS eff, games_played, wins, losses
           FROM player_game_stats WHERE game_id=? AND games_played > 0""", (gid,)))
    if not rows:
        print("  no players with games played\n")
        continue

    effs = [r["eff"] for r in rows]
    print(f"\n  active players  : {len(rows)}")
    print(f"  mmr spread      : {min(effs)} - {max(effs)}  "
          f"(median {int(statistics.median(effs))}, stdev {statistics.pstdev(effs):.0f})")
    if bs:
        print(f"  ladder spans    : {bs[0]} - {bs[-1]}")
        used = max(effs) - min(effs)
        print(f"  ladder in use   : {used} of {bs[-1]-bs[0]} points "
              f"({used/(bs[-1]-bs[0])*100:.0f}%)")

    occ = defaultdict(int)
    for r in rows:
        occ[band_of(r["eff"], bs, lab)] += 1
    order = [lab[b] for b in bs] if bs else []
    print("\n  rank distribution:")
    for l in order:
        if occ.get(l):
            print(f"    {l:<14}{'#' * min(occ[l], 40)} {occ[l]}")
    stray = {k: v for k, v in occ.items() if k not in order}
    if stray:
        print(f"    below lowest band: {stray}")

    at_floor = sum(1 for e in effs if bs and e <= bs[0])
    print(f"\n  players at/below the {bs[0] if bs else '?'} floor: {at_floor}")

    # ---- is MMR predictive? ----------------------------------------------
    # For each decided match, compare team-average MMR *before* the match
    # (reconstructed from mmr_history) and see if the stronger team won.
    correct = total = ties = 0
    diffs = []
    for mt in db.execute(
            """SELECT match_id, winning_team FROM matches
               WHERE game_id=? AND winning_team IS NOT NULL AND cancelled=0""", (gid,)):
        pre = {r["player_id"]: r["mmr_before"] for r in db.execute(
            "SELECT player_id, mmr_before FROM mmr_history WHERE match_id=?",
            (mt["match_id"],))}
        if not pre:
            continue
        teams = defaultdict(list)
        for p in db.execute(
                "SELECT player_id, team FROM match_players WHERE match_id=?",
                (mt["match_id"],)):
            if p["player_id"] in pre:
                teams[p["team"]].append(pre[p["player_id"]])
        if len(teams) != 2 or not all(teams.values()):
            continue
        avg = {t: sum(v) / len(v) for t, v in teams.items()}
        (ta, va), (tb, vb) = avg.items()
        diffs.append(abs(va - vb))
        total += 1
        if va == vb:
            ties += 1
        elif (va > vb and mt["winning_team"] == ta) or (vb > va and mt["winning_team"] == tb):
            correct += 1

    print("\n  --- does MMR predict the winner? ---")
    if total:
        print(f"  matches with pre-match ratings : {total}")
        print(f"  favourite won                  : {correct}/{total} "
              f"({correct/total*100:.1f}%)   [50% = no signal]")
        print(f"  team-avg MMR gap at match time : median {int(statistics.median(diffs))}, "
              f"mean {sum(diffs)/len(diffs):.0f}, max {int(max(diffs))}")
    else:
        print("  not enough history to evaluate")

    # ---- movement from seed ----------------------------------------------
    moved = []
    for r in rows:
        first = db.execute(
            """SELECT mmr_before FROM mmr_history WHERE game_id=? AND player_id=?
               ORDER BY id LIMIT 1""", (gid, r["player_id"])).fetchone()
        if first:
            moved.append((r["eff"] - first["mmr_before"], r["games_played"], r["player_id"]))
    if moved:
        drift = [abs(d) for d, _, _ in moved]
        print(f"\n  drift from first recorded rating (n={len(moved)}):")
        print(f"    median |drift| {int(statistics.median(drift))}, max {max(drift)}")
        top = sorted(moved, key=lambda x: abs(x[0]), reverse=True)[:5]
        for d, gp, pid in top:
            print(f"    {pid}  {d:+6d} over {gp:>3} games")
    print()
