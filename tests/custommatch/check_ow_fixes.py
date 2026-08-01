"""Verify the audit fixes: gate scope, modal bounds, settings warnings."""
import asyncio
import re
import sys
import types
from dataclasses import replace
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = Path(__file__).resolve().parents[2] / "cogs" / "custommatch"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch import database as db_mod

db_mod.DB_PATH = HERE / "test_custommatch.db"
from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch import views_settings as vs

FAILS = []


def check(c, m):
    if not c:
        FAILS.append(m)
    return c


def gate_scope():
    """All four 2-2-2 gates must be scoped to the non-secondary 12-player queue.

    Every consumer of the role selections -- the three feasibility gates and the
    queue-embed coverage panel -- goes through _ow_selections, so tracking that
    one name catches any new gate somebody adds later."""
    src = (SRC / "cog.py").read_text()
    # Exclude the definition explicitly -- it is NOT first in the file.
    call_sites = [
        m.start() for m in re.finditer(r"_ow_selections\(", src)
        if "async def" not in src[src.rfind("\n", 0, m.start()):m.start()]
    ]
    print("2-2-2 gate call sites (each must be guarded)")
    ok_all = True
    for pos in call_sites:
        line_no = src[:pos].count("\n") + 1
        window = src[max(0, pos - 700):pos]
        guarded = "is_secondary" in window and "== 12" in window
        print(f"   cog.py:{line_no:<6} guarded={guarded}")
        ok_all &= guarded
    check(ok_all, "a _ow_selections call site is missing the is_secondary/==12 guard")
    check(len(call_sites) >= 3, f"expected >=3 gate call sites, found {len(call_sites)}")


def role_map_plumbing():
    src = (SRC / "cog.py").read_text()
    check("ow_roles: Optional[Dict[int, str]] = None" in src,
          "create_match_channel lost its ow_roles parameter")
    check("ow_roles=ow_roles_for_match" in src,
          "the balancer's role map is not passed to create_match_channel")
    check("if ow_roles is None:" in src,
          "create_match_channel no longer falls back to the in-memory stash")
    print("\nrole-map plumbing: parameter + pass-through + fallback all present")


async def modal_bounds():
    print("\nOWRoleMMRModal bounds")
    game = await DB.get_game(3)
    parent = types.SimpleNamespace(game=game)
    bands = sorted((await DB.get_mmr_roles(3)).values())
    m1 = vs.OWRoleMMRModal(parent, bands[0], bands[-1])
    print(f"   overwatch  floor={m1.floor} ceiling={m1.ceiling} label={m1.mmr_input.label!r}")
    check(m1.floor == 800, f"OW floor {m1.floor}, expected 800")

    # A second modal with different bounds must not corrupt the first --
    # discord.ui class-level TextInputs are shared unless copied per instance.
    m2 = vs.OWRoleMMRModal(parent, 500, 6000)
    print(f"   valorant   floor={m2.floor} ceiling={m2.ceiling} label={m2.mmr_input.label!r}")
    same_obj = m1.mmr_input is m2.mmr_input
    print(f"   share the same TextInput object: {same_obj}")
    check(m1.mmr_input.label != m2.mmr_input.label or not same_obj,
          "modal label is shared across instances — bounds shown will be wrong")
    check(m1.floor == 800 and m2.floor == 500,
          "modal floor leaked between instances")


async def settings_warnings():
    print("\nOverwatch settings panel warnings")
    game = await DB.get_game(3)
    view = vs.OverwatchWeightsView(None, game)
    embed = await view.build_embed()
    clean = "player_count is" not in (embed.description or "")
    print(f"   healthy config -> no player_count warning: {clean}")
    check(clean, "warned about player_count on a correct 12-player config")

    bad = replace(game, player_count=10, role_required=False)
    view2 = vs.OverwatchWeightsView(None, bad)
    embed2 = await view2.build_embed()
    d = embed2.description or ""
    has_pc = "player_count is 10" in d
    has_role = "Role selection is off" in d
    print(f"   player_count=10   -> warned: {has_pc}")
    print(f"   role_required off -> warned: {has_role}")
    check(has_pc, "no warning for a non-12 Overwatch player_count")
    check(has_role, "no warning when role selection is disabled")


async def main():
    gate_scope()
    role_map_plumbing()
    await modal_bounds()
    await settings_warnings()
    print("\n" + "=" * 62)
    if FAILS:
        print(f"{len(FAILS)} FAILURES")
        for f in FAILS:
            print("  !!", f)
    else:
        print("all fixes verified")
    print("=" * 62)
    return 1 if FAILS else 0


sys.exit(asyncio.run(main()))
