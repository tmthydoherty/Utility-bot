#!/usr/bin/env python3
"""One-off repair: lift players stranded below their game's lowest rank band.

The Elo loss floor was added on 2026-06-30; matches decided before then could
drive a rating under the bottom of the ladder. update_mmr_roles finds no band
at or below such a rating, so it strips the player's rank role and grants
nothing back — they sit rankless and can only climb out.

The floor now derives from each game's own ladder (get_mmr_floor), so this
cannot recur. This repairs the players already stranded.

Overwatch is handled too: its ratings live per-role in ow_role_stats, so each
role is checked separately.

Rank roles are NOT touched here — this has no Discord connection. The hourly
rank_role_audit in the cog will grant the correct role on its next pass.

    # look, change nothing
    venv/bin/python3 tests/custommatch/repair_below_floor.py
    # actually write
    venv/bin/python3 tests/custommatch/repair_below_floor.py --apply
"""

import argparse
import asyncio
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))

parser = argparse.ArgumentParser()
parser.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
parser.add_argument("--db", default=str(REPO / "data" / "custommatch.db"),
                    help="database to repair (defaults to the live one)")
args = parser.parse_args()

from cogs.custommatch import database as db_mod

db_mod.DB_PATH = Path(args.db)

from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import is_overwatch_game


async def main():
    print(f"database : {db_mod.DB_PATH}")
    print(f"mode     : {'APPLY (writing)' if args.apply else 'dry run (no writes)'}\n")

    total = 0
    for game in await DB.get_all_games():
        floor = await DB.get_mmr_floor(game.game_id, default=None)
        if floor is None:
            print(f"  {game.name}: no rank ladder configured, skipping")
            continue

        print(f"=== {game.name} (floor {floor}) ===")
        found = 0

        # Aggregate column -- used directly by every game except Overwatch, and
        # still the seed source there, so repair it regardless.
        async with DB._get_db() as db:
            async with db.execute(
                """SELECT player_id, mmr, admin_offset FROM player_game_stats
                   WHERE game_id = ? AND mmr + admin_offset < ?""",
                (game.game_id, floor)
            ) as cursor:
                rows = await cursor.fetchall()
        for player_id, mmr, offset in rows:
            eff = mmr + (offset or 0)
            new_mmr = floor - (offset or 0)   # keep effective_mmr == floor
            print(f"  aggregate  {player_id}  {eff} -> {floor}"
                  f"{f' (mmr {mmr}->{new_mmr}, offset {offset})' if offset else ''}")
            found += 1
            if args.apply:
                async with DB._get_db() as db:
                    await db.execute(
                        "UPDATE player_game_stats SET mmr = ? WHERE player_id = ? AND game_id = ?",
                        (new_mmr, player_id, game.game_id))
                    await db.commit()

        # Overwatch: per-role ratings are what the balancer actually reads.
        if is_overwatch_game(game):
            async with DB._get_db() as db:
                async with db.execute(
                    """SELECT player_id, role, mmr, admin_offset FROM ow_role_stats
                       WHERE game_id = ? AND mmr + admin_offset < ?""",
                    (game.game_id, floor)
                ) as cursor:
                    rrows = await cursor.fetchall()
            for player_id, role, mmr, offset in rrows:
                new_mmr = floor - (offset or 0)
                print(f"  {role:<9}  {player_id}  {mmr + (offset or 0)} -> {floor}")
                found += 1
                if args.apply:
                    async with DB._get_db() as db:
                        await db.execute(
                            """UPDATE ow_role_stats SET mmr = ?
                               WHERE player_id = ? AND game_id = ? AND role = ?""",
                            (new_mmr, player_id, game.game_id, role))
                        await db.commit()

        print(f"  {found} stranded rating(s)\n")
        total += found

    print("=" * 60)
    if not total:
        print("nothing below any floor — nothing to repair")
    elif args.apply:
        print(f"repaired {total} rating(s).")
        print("Rank roles will be granted by rank_role_audit within the hour.")
    else:
        print(f"{total} rating(s) would be repaired — re-run with --apply")
    print("=" * 60)
    return 0


sys.exit(asyncio.run(main()))
