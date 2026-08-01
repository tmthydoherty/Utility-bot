#!/usr/bin/env python3
"""One-off backfill: give Overwatch players their per-role MMR.

The rank-role setup flow used to write only player_game_stats.mmr, which the
2-2-2 balancer never reads, so everyone set up before that fix has empty
ow_role_stats and enters the balancer at the 1000 default. This copies each
player's setup MMR into their three per-role ratings.

Safe to re-run: seed_ow_role_stats_from_rank only fills roles with no row, so
ratings earned from real matches are never touched.

    # look, change nothing (defaults to the scratch copy)
    python3 backfill_ow_role_mmr.py
    # same, against the real database
    python3 backfill_ow_role_mmr.py --live
    # actually write
    python3 backfill_ow_role_mmr.py --live --apply
"""

import argparse
import asyncio
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

parser = argparse.ArgumentParser()
parser.add_argument("--live", action="store_true",
                    help="use /home/tmthy/Vibey/data/custommatch.db")
parser.add_argument("--apply", action="store_true",
                    help="write changes (otherwise dry run)")
args = parser.parse_args()

DB_FILE = (Path("/home/tmthy/Vibey/data/custommatch.db") if args.live
           else HERE / "test_custommatch.db")

from cogs.custommatch import database as db_mod

db_mod.DB_PATH = DB_FILE

from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import OW_ROLES, is_overwatch_game

DEFAULT_MMR = 1000  # PlayerStats.mmr default -- means "never went through setup"


async def main():
    print(f"database : {DB_FILE}")
    print(f"mode     : {'APPLY (writing)' if args.apply else 'dry run (no writes)'}\n")

    games = [g for g in await DB.get_all_games() if is_overwatch_game(g)]
    if not games:
        print("No Overwatch game configured; nothing to do.")
        return 0

    for game in games:
        floor = await DB.get_mmr_floor(game.game_id)
        bands = sorted((await DB.get_mmr_roles(game.game_id)).values())
        print(f"=== {game.name} (id={game.game_id}) ===")
        print(f"    ladder floor {floor}, bands {bands}\n")

        async with DB._get_db() as db:
            async with db.execute(
                """SELECT player_id, mmr, admin_offset FROM player_game_stats
                   WHERE game_id = ? ORDER BY mmr DESC""",
                (game.game_id,)
            ) as cursor:
                rows = await cursor.fetchall()

        seeded_n = needs_setup = already = 0
        for player_id, mmr, admin_offset in rows:
            async with DB._get_db() as db:
                async with db.execute(
                    "SELECT COUNT(*) FROM ow_role_stats WHERE player_id = ? AND game_id = ?",
                    (player_id, game.game_id)
                ) as cursor:
                    existing = (await cursor.fetchone())[0]

            if existing >= len(OW_ROLES):
                already += 1
                continue

            effective = mmr + (admin_offset or 0)
            if effective == DEFAULT_MMR:
                # Never actually set up -- seeding 1000 would be no better than
                # the default it already gets. Flag it for a real rank setup.
                needs_setup += 1
                print(f"  SKIP  {player_id}  mmr={effective} (default -- needs rank setup)")
                continue

            target = max(floor, effective)
            if args.apply:
                got = await DB.seed_ow_role_stats_from_rank(
                    player_id, game.game_id, target)
            else:
                got = [r for r in OW_ROLES]  # what would be written
            seeded_n += 1
            print(f"  SEED  {player_id}  {', '.join(got)} @ {target}")

        print(f"\n  seeded         : {seeded_n}")
        print(f"  already had it : {already}")
        print(f"  need setup     : {needs_setup}")
        if not args.apply and seeded_n:
            print(f"\n  dry run -- re-run with --apply to write these.")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
