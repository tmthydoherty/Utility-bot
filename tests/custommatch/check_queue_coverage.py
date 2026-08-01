"""Verify the Overwatch queue's role-coverage panel.

Pure string checks over synthetic willing-sets -- no database, no Discord. What
each group protects:

  sections      the pair/trio headers only appear once someone is in that
                bucket, and the locked rows are always present
  needs         "Roles needed" stays quiet while any role can still join, and
                names the right roles once the open slots are spoken for
  consistency   the panel never contradicts itself: a role listed as needed
                must also show up in the can't-form-2-2-2 line, and vice versa
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch.models import (
    OW_ROLES, OW_ROLE_EMOJI, ow_can_form_222, ow_coverage_panel,
)

T, D, S = "Tank", "DPS", "Support"
FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
    return cond


def mk(*groups):
    """mk(([Tank], 4), ([Tank, DPS], 2)) -> {pid: role_set} for 6 players."""
    sel, pid = {}, 0
    for roles, n in groups:
        for _ in range(n):
            sel[pid] = set(roles)
            pid += 1
    return sel


def row(role, n):
    return f"-# {OW_ROLE_EMOJI[role]}: {n}"


def case(label, selections, *, needed=None, hidden_need=False,
         headers=(), no_headers=(), contains=(), missing=()):
    panel = ow_coverage_panel(selections)
    ok = True

    if hidden_need:
        ok &= check("Roles needed:" not in panel,
                    f"{label}: 'Roles needed' shown but should be hidden")
    if needed is not None:
        ok &= check("Roles needed:" in panel, f"{label}: 'Roles needed' missing")
        # Everything after the header, so the locked rows can't satisfy a match
        tail = panel.split("Roles needed:", 1)[-1]
        for role, n in needed.items():
            ok &= check(row(role, n) in tail, f"{label}: expected need {role}={n}")
        for role in OW_ROLES:
            if role not in needed:
                ok &= check(f"{OW_ROLE_EMOJI[role]}:" not in tail,
                            f"{label}: {role} listed as needed but should not be")
    for h in headers:
        ok &= check(h in panel, f"{label}: missing header {h!r}")
    for h in no_headers:
        ok &= check(h not in panel, f"{label}: header {h!r} should not appear")
    for frag in contains:
        ok &= check(frag in panel, f"{label}: missing {frag!r}")
    for frag in missing:
        ok &= check(frag not in panel, f"{label}: should not contain {frag!r}")

    print(f"   {'ok  ' if ok else 'FAIL'} {label}")
    return panel


def sections():
    print("panel sections appear only when their bucket has players")
    case("empty queue: locked rows only, no need",
         {},
         hidden_need=True,
         headers=["**Locked In Roles:**"],
         no_headers=["**Multi-role Queueing:**", "**All-role Queuing:**"],
         contains=[row(T, "0/4"), row(S, "0/4"), row(D, "0/4")])

    case("one of each bucket",
         mk(([T], 1), ([S], 2), ([D], 1), ([T, S], 2), ([T, D], 1), ([T, D, S], 3)),
         headers=["**Locked In Roles:**", "**Multi-role Queueing:**",
                  "**All-role Queuing:**"],
         contains=[row(T, "1/4"), row(S, "2/4"), row(D, "1/4"),
                   f"-# {OW_ROLE_EMOJI[T]}{OW_ROLE_EMOJI[S]}: 2",
                   f"-# {OW_ROLE_EMOJI[T]}{OW_ROLE_EMOJI[D]}: 1",
                   f"-# {OW_ROLE_EMOJI[T]}{OW_ROLE_EMOJI[S]}{OW_ROLE_EMOJI[D]}: 3"],
         # Support+DPS pair has nobody in it, so that row must not be printed
         missing=[f"-# {OW_ROLE_EMOJI[S]}{OW_ROLE_EMOJI[D]}:"])

    case("locked only: no multi headers",
         mk(([T], 4), ([D], 4)),
         no_headers=["**Multi-role Queueing:**", "**All-role Queuing:**"])

    panel = ow_coverage_panel(mk(([T], 2)))
    check(all(l.startswith("-# ") for l in panel.split("\n") if l),
          "every non-blank panel line must carry the -# subtext prefix")
    print("   ok   every line is -# subtext")


def needs():
    print("\n'Roles needed' threshold and contents")
    case("3 locked Tank, 9 open: still slack, stay quiet",
         mk(([T], 3)), hidden_need=True)

    case("1 locked Tank: far too early",
         mk(([T], 1)), hidden_need=True)

    case("4 Tank + 2 DPS locked, 6 open: every seat spoken for",
         mk(([T], 4), ([D], 2)),
         needed={S: 4, D: 2},
         # 4 + 2 needed lands exactly on the 6 open seats -- tight, not over
         missing=["⚠️ Too many"])

    case("11 queued, only 3 support-capable, 1 open",
         mk(([T], 4), ([D], 4), ([T, S], 3)),
         needed={S: 1})

    case("12 all-flex: nothing needed, 2-2-2 ready",
         mk(([T, D, S], 12)),
         hidden_need=True,
         contains=["✅ 2-2-2 ready"])

    case("6 all-flex: nothing needed",
         mk(([T, D, S], 6)), hidden_need=True)

    print("\nover-subscription warning")
    case("9 locked Tank: more needed than seats left",
         mk(([T], 9)),
         needed={S: 4, D: 4},
         contains=[f"⚠️ Too many {OW_ROLE_EMOJI[T]}", "only 3 spots left"])

    # 6 Tank-only with 6 seats left still needs 8 more players, so two of those
    # tanks can never be slotted -- over-subscribed even though nobody is full.
    case("6 locked Tank: 8 needed into 6 seats",
         mk(([T], 6)),
         needed={S: 4, D: 4},
         contains=[f"⚠️ Too many {OW_ROLE_EMOJI[T]}", "only 6 spots left"])

    # Full queue: no seats left to warn about, the 2-2-2 line carries it instead
    case("full queue short on Support: no seats-left warning",
         mk(([T], 4), ([D], 4), ([T, D], 4)),
         missing=["⚠️ Too many"],
         contains=["⚠️ Can't form 2-2-2"])


def consistency():
    print("\nthe panel never contradicts itself")
    # No single role is short (Tank has 12 coverers, DPS and Support 4 each) yet
    # a 2-2-2 cannot be built: the shortfall lives across the DPS+Support pair.
    # Naming one of them would mean reading it off an arbitrary maximum matching,
    # so the line must report a headcount instead.
    sel = mk(([T], 8), ([T, D, S], 4))
    feasible, _ = ow_can_form_222(sel)
    check(not feasible, "fixture should be infeasible: 8 Tank-only + 4 flex")
    case("pair-only shortfall reports a headcount, not a role",
         sel,
         hidden_need=True,
         contains=["⚠️ Can't form 2-2-2 — 4 queued players need to add roles"])

    # Wherever both are shown, the needed roles and the 2-2-2 line must agree.
    sel = mk(([T], 4), ([D], 4), ([T, D], 4))
    panel = case("full queue: needed roles match the 2-2-2 line",
                 sel, needed={S: 4})
    line = [l for l in panel.split("\n") if "Can't form 2-2-2" in l][0]
    check(OW_ROLE_EMOJI[S] in line and OW_ROLE_EMOJI[T] not in line,
          "2-2-2 failure line disagrees with the 'Roles needed' rows")
    print("   ok   failure line names the same role as 'Roles needed'")

    # A full, formable queue must never ask for anything.
    sel = mk(([T], 4), ([D], 4), ([S], 4))
    panel = ow_coverage_panel(sel)
    check("✅ 2-2-2 ready" in panel and "Roles needed:" not in panel,
          "a perfectly filled 4/4/4 queue must read as ready with no needs")
    print("   ok   perfect 4/4/4 reads ready with no needs")

    # The panel has to fit in an embed field.
    worst = mk(([T], 1), ([S], 1), ([D], 1), ([T, S], 1), ([T, D], 1),
               ([S, D], 1), ([T, D, S], 6))
    size = len(ow_coverage_panel(worst))
    check(size <= 1024, f"panel is {size} chars, over Discord's 1024 field cap")
    print(f"   ok   busiest panel is {size} chars (cap 1024)")


def main():
    sections()
    needs()
    consistency()
    print()
    if FAILS:
        for f in FAILS:
            print(f"FAIL: {f}")
        return 1
    print("queue coverage panel: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
