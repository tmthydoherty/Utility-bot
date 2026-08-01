#!/usr/bin/env python3
"""Run the custommatch checks against a throwaway copy of the live database.

    venv/bin/python3 tests/custommatch/run_all.py

Nothing here writes to data/custommatch.db. The copy is taken with sqlite's
backup API so it is safe to run while the bot is live, and every script points
at the copy via database.DB_PATH.

Scripts, and what each protects:

  ow_harness.py          strict 2-2-2 balancer + viability gate, 9 rosters
                         including the flex/greedy-trap case, 25 runs each
                         (the balancer picks randomly among tied splits)
  ow_lifecycle.py        full match: gate -> balance -> persist roles ->
                         per-role Elo -> rank-role input, plus the degraded
                         no-recorded-role path and the loss floor
  check_seed_gap.py      rank setup reaches ow_role_stats; floors match each
                         game's lowest band; new-role seeding; climb rates
  check_seed_coverage.py every `stats.mmr =` write mirrors into per-role MMR.
                         Run this after touching any MMR-seeding path.
  check_view_timeout.py  settings panels refresh while used and grey out on expiry
  check_ow_fixes.py      2-2-2 gates stay scoped; role-map plumbing; modal
                         bounds; settings warnings
  check_perf.py          balancer stays inside Discord's 3s ack window

Not run here (they read the live database or write to it):
  analyze_mmr.py             read-only MMR health report for all games
  backfill_ow_role_mmr.py    one-off per-role seeding repair (--live --apply)
"""

import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
LIVE = REPO / "data" / "custommatch.db"
COPY = HERE / "test_custommatch.db"

SCRIPTS = [
    "check_seed_gap.py",
    "check_seed_coverage.py",
    "check_ow_fixes.py",
    "check_view_timeout.py",
    "ow_lifecycle.py",
    "ow_harness.py",
    "check_perf.py",
]


def refresh_copy():
    if not LIVE.exists():
        sys.exit(f"live database not found at {LIVE}")
    for stale in (COPY, Path(str(COPY) + "-wal"), Path(str(COPY) + "-shm")):
        stale.unlink(missing_ok=True)
    src = sqlite3.connect(f"file:{LIVE}?mode=ro", uri=True)
    dst = sqlite3.connect(COPY)
    src.backup(dst)          # consistent snapshot even with the bot running
    dst.close()
    src.close()


def main():
    refresh_copy()
    print(f"database under test : {COPY}")
    print(f"live database       : untouched\n")

    failed = []
    for name in SCRIPTS:
        print(f"{'=' * 70}\n{name}\n{'=' * 70}")
        r = subprocess.run([sys.executable, str(HERE / name)],
                           capture_output=True, text=True)
        sys.stdout.write(r.stdout)
        if r.returncode != 0:
            sys.stdout.write(r.stderr)
            failed.append(name)
        print()

    print("=" * 70)
    if failed:
        print(f"FAILED: {', '.join(failed)}")
    else:
        print(f"all {len(SCRIPTS)} checks passed")
    print("=" * 70)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
