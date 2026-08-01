"""Verify Overwatch per-role setup and the /cm_panel page tree.

Overwatch rates every role separately, so the admin flows have to be careful in
two directions at once: rank the roles the admin asked for, and leave every
other role exactly as it was. What each group protects:

  seeding    which ow_role_stats rows a setup actually writes -- an unranked
             role must stay absent so get_ow_role_stats can seed it from the
             player's best role, and a player with no rank anywhere lands on a
             real band floor instead of between two bands
  aggregate  player_game_stats.mmr tracks the player's best role, and re-ranking
             one role never drags it below a rating they earned on another
  panel      every /cm_panel page fits Discord's row limits, carries a Back
             button, and keeps its handlers reachable
"""
import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cogs.custommatch import database as db_mod

db_mod.DB_PATH = HERE / "test_custommatch.db"
from cogs.custommatch.database import DatabaseHelper as DB
from cogs.custommatch.models import OW_ROLES, OW_ROLE_DISPLAY_ORDER

import discord
from cogs.custommatch import views_gameplay as vg

OW_GAME = 3
PID = 1000097
FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
    return cond


async def wipe():
    async with DB._get_db() as db:
        await db.execute("DELETE FROM ow_role_stats WHERE player_id=?", (PID,))
        await db.execute("DELETE FROM player_game_stats WHERE player_id=?", (PID,))
        await db.commit()


async def write_role(role, mmr):
    stats = await DB.get_ow_role_stats(PID, OW_GAME, role)
    stats.mmr = mmr - stats.admin_offset
    stats.is_new = False
    stats.games_played = 0
    await DB.upsert_ow_role_stats(stats)


async def seeding():
    print("which rows a setup writes")
    bands = sorted((await DB.get_mmr_roles(OW_GAME)).values())
    check(vg.OW_ALL_UNRANKED_MMR in bands,
          f"the all-unranked default {vg.OW_ALL_UNRANKED_MMR} is not a rank band "
          f"floor ({bands}) -- update_mmr_roles would grant the band below it")
    print(f"   ok   all-unranked default {vg.OW_ALL_UNRANKED_MMR} sits on a band floor")

    # One role ranked: the other two must stay absent, not be blanket-seeded.
    await wipe()
    await write_role("Tank", 3200)
    stored = await DB.get_all_ow_role_stats(PID, OW_GAME)
    check(set(stored) == {"Tank"},
          f"ranking one role wrote rows for {sorted(stored)}; only Tank should exist")
    print("   ok   ranking Tank alone leaves DPS/Support with no row")

    # ...and an unranked role still resolves, one band below the best role.
    for role in ("DPS", "Support"):
        lazy = await DB.get_ow_role_stats(PID, OW_GAME, role)
        check(lazy.is_new, f"{role} should report as newly seeded, not stored")
        check(lazy.effective_mmr < 3200,
              f"{role} seeded at {lazy.effective_mmr}, expected below the 3200 Tank")
        check(lazy.effective_mmr in bands,
              f"{role} seeded at {lazy.effective_mmr}, which is not a band floor")
    still = await DB.get_all_ow_role_stats(PID, OW_GAME)
    check(set(still) == {"Tank"}, "reading an unranked role must not persist it")
    print("   ok   unranked roles seed a band below Tank without being persisted")

    # A second role ranked later must not disturb the first.
    await write_role("Support", 1800)
    stored = await DB.get_all_ow_role_stats(PID, OW_GAME)
    check(stored["Tank"].effective_mmr == 3200,
          f"ranking Support moved Tank to {stored['Tank'].effective_mmr}")
    check(set(stored) == {"Tank", "Support"},
          f"ranking Support also wrote {sorted(set(stored) - {'Tank', 'Support'})}")
    print("   ok   ranking Support later leaves Tank untouched")


async def aggregate():
    print("\naggregate MMR follows the best role")
    await wipe()
    await write_role("Tank", 3200)
    await write_role("Support", 1800)
    peak = await DB.get_ow_player_peak_mmr(PID, OW_GAME)
    check(peak == 3200, f"peak was {peak}, expected the 3200 Tank")
    print("   ok   peak reads the highest role")

    # Re-ranking the *lower* role must not drag the aggregate down: the high
    # role is still live and is what the player's rank role should reflect.
    await write_role("Support", 2500)
    peak = await DB.get_ow_player_peak_mmr(PID, OW_GAME)
    check(peak == 3200, f"peak dropped to {peak} after editing the lower role")
    print("   ok   editing a lower role leaves the peak on Tank")

    await write_role("DPS", 4000)
    peak = await DB.get_ow_player_peak_mmr(PID, OW_GAME)
    check(peak == 4000, f"peak was {peak}, expected the new 4000 DPS")
    print("   ok   a new highest role raises the peak")
    await wipe()


class FakeMember:
    display_name = "Test"
    roles = []

    async def add_roles(self, *a, **k):
        pass


class FakeGuild:
    def get_member(self, i):
        return FakeMember()

    def get_role(self, i):
        return None


class FakeResponse:
    def __init__(self):
        self.content = None

    async def edit_message(self, **kw):
        self.content = kw.get("content")


class FakeInteraction:
    def __init__(self):
        self.guild = FakeGuild()
        self.user = FakeMember()
        self.response = FakeResponse()


class FakeCog:
    async def update_mmr_roles(self, *a, **k):
        pass

    async def log_action(self, *a, **k):
        pass


async def all_unranked():
    """Marking every role Unranked during setup.

    Regression: this used to be a silent no-op for anyone who already had rows,
    while still stamping the 1200 default onto the aggregate column -- so the
    player ended up reading 1200 overall with three 800 roles underneath.
    """
    print("\nall three roles marked Unranked")
    mmr_roles = await DB.get_mmr_roles_with_labels(OW_GAME)

    # Stale rows with no games behind them: the reset should take.
    await wipe()
    for role in OW_ROLES:
        await write_role(role, 800)
    inter = FakeInteraction()
    await vg._ow_finalize_setup(inter, FakeCog(), OW_GAME, PID, "console", {}, {}, mmr_roles)
    rows = await DB.get_all_ow_role_stats(PID, OW_GAME)
    check(all(s.effective_mmr == vg.OW_ALL_UNRANKED_MMR for s in rows.values()),
          f"unplayed roles should reset to {vg.OW_ALL_UNRANKED_MMR}, got "
          f"{ {k: v.effective_mmr for k, v in rows.items()} }")
    agg = await DB.get_player_stats(PID, OW_GAME)
    check(agg.effective_mmr == vg.OW_ALL_UNRANKED_MMR,
          f"aggregate was {agg.effective_mmr}, expected {vg.OW_ALL_UNRANKED_MMR}")
    check("unchanged" not in (inter.response.content or ""),
          "a reset must not report roles as unchanged")
    print("   ok   unplayed roles reset, aggregate agrees with them")

    # A role with real games behind it must survive the same action.
    stats = await DB.get_ow_role_stats(PID, OW_GAME, "Tank")
    stats.mmr, stats.games_played, stats.is_new = 3200, 20, False
    await DB.upsert_ow_role_stats(stats)
    inter = FakeInteraction()
    await vg._ow_finalize_setup(inter, FakeCog(), OW_GAME, PID, "console", {}, {}, mmr_roles)
    rows = await DB.get_all_ow_role_stats(PID, OW_GAME)
    check(rows["Tank"].effective_mmr == 3200,
          f"a played role was reset to {rows['Tank'].effective_mmr}; 20 games of "
          "rating must not be wiped by clicking through setup")
    check(rows["Tank"].games_played == 20, "a protected role lost its game count")
    check(rows["DPS"].effective_mmr == vg.OW_ALL_UNRANKED_MMR,
          "unplayed roles should still reset alongside a protected one")
    check("not reset" in (inter.response.content or ""),
          "the admin must be told which role was protected and why")
    print("   ok   a played role is protected and reported")

    # The aggregate always reflects what is actually stored, never a leftover
    # snapshot or the all-unranked sentinel.
    agg = await DB.get_player_stats(PID, OW_GAME)
    peak = await DB.get_ow_player_peak_mmr(PID, OW_GAME)
    check(agg.effective_mmr == peak == 3200,
          f"aggregate {agg.effective_mmr} disagrees with stored peak {peak}")
    print("   ok   aggregate re-derived from the stored rows")
    await wipe()


def dropdowns():
    print("\nrank dropdowns")
    mmr_roles = {1: {"mmr": 800, "label": "Bronze"}, 2: {"mmr": 3200, "label": "Diamond"}}

    setup = vg.OWSetupRoleRankView(None, OW_GAME, PID, mmr_roles, "console")
    values = [o.value for o in setup.children[0].options]
    check(values[0] == vg.UNRANKED_VALUE,
          f"first-time setup must offer Unranked first, got {values[:1]}")
    check(setup.remaining == list(OW_ROLE_DISPLAY_ORDER),
          "setup must ask about every role")
    print("   ok   setup offers Unranked and walks all three roles")

    rerank = vg.OWSetupRoleRankView(None, OW_GAME, PID, mmr_roles, "console",
                                    ["DPS"], existing_only=True)
    values = [o.value for o in rerank.children[0].options]
    check(vg.UNRANKED_VALUE not in values,
          "the re-rank flow must not offer Unranked -- the admin picked the role "
          "precisely to rank it, so it would only discard their own selection")
    print("   ok   re-ranking drops Unranked")

    # Ranks read high-to-low: an admin reaches for Diamond far more often than Bronze.
    ranked = [o.label for o in setup.children[0].options if o.value != vg.UNRANKED_VALUE]
    check(ranked == ["Diamond", "Bronze"], f"ranks listed {ranked}, expected highest first")
    print("   ok   ranks are listed highest first")


def interaction_claim():
    """A queue button reaching two handlers must only be acted on once.

    Regression: the registered view and the on_interaction fallback both route
    the same press. The fallback waits 0.25s for the view to ack first, but the
    join handler does several DB reads before its first response, so under load
    both dispatched and the loser raised 40060 part-way through the join.
    """
    print("\ndouble-dispatch guard")
    import cogs.custommatch.cog as cogmod
    cog = object.__new__(cogmod.CustomMatch)
    cog._claimed_interactions = {}

    class FakeInteraction:
        def __init__(self, iid):
            self.id = iid

    press = FakeInteraction(111)
    check(cog._claim_interaction(press), "the first handler must win the claim")
    check(not cog._claim_interaction(press),
          "the second handler claimed the same interaction — both would respond")
    check(cog._claim_interaction(FakeInteraction(222)),
          "a different press must claim independently")
    print("   ok   one press is claimed exactly once")

    # The bookkeeping must not grow without bound on a busy server. A burst
    # outruns the age-based prune, so the size cap is what actually holds here.
    cap = cogmod.CustomMatch.CLAIM_CACHE_MAX
    for i in range(cap * 3):
        cog._claim_interaction(FakeInteraction(1000 + i))
    check(len(cog._claimed_interactions) <= cap + 1,
          f"claim table grew to {len(cog._claimed_interactions)} entries, cap is {cap}")
    print(f"   ok   claim table stays bounded ({len(cog._claimed_interactions)} entries)")


def panel():
    print("\n/cm_panel page tree")

    class FakeCog:
        queues = {}

        async def is_cm_admin(self, user):
            return True

    cog = FakeCog()
    top = vg.AdminPanelView(cog)
    pages = [("AdminPanelView", top)]
    for cls in (vg.AdminQueuePage, vg.AdminUserPage, vg.AdminMatchPage):
        pages.append((cls.__name__, cls(cog, top)))

    for name, page in pages:
        rows = {}
        for item in page.children:
            rows[item.row] = rows.get(item.row, 0) + 1
        over = {r: n for r, n in rows.items() if n > 5}
        check(not over, f"{name} row(s) over Discord's 5-component limit: {over}")
        check(len(page.children) <= 25, f"{name} has {len(page.children)} components (max 25)")

        if page is not top:
            backs = [c for c in page.children if isinstance(c, vg.BackButton)]
            check(len(backs) == 1, f"{name} needs exactly one Back button, has {len(backs)}")
    print(f"   ok   {len(pages)} pages inside row limits, Back on every child")

    # Every child-page button must reach a handler on the shared mixin, or the
    # button is wired to nothing and dies at click time.
    for name, page in pages:
        for item in page.children:
            if not isinstance(item, discord.ui.Button) or isinstance(item, vg.BackButton):
                continue
            check(item.callback is not None, f"{name}: {item.label} has no callback")
    print("   ok   every button resolves to a handler")

    labels = {c.label for c in top.children}
    check("OW Role Ranks" in labels, "the OW Role Ranks entry point is missing")
    for moved in ("Sub Player", "New Queue", "Set Platform"):
        check(moved not in labels, f"{moved} should live on a child page, not the top level")
    print("   ok   top level holds the parents and leftovers only")


async def rating_audit():
    """The hourly self-heal, repointed from Discord badges onto the ratings.

    Removing rank roles removed the thing the old audit repaired, so it now
    guards the two invariants that actually matter to the balancer.
    """
    print("\nrating audit")
    import cogs.custommatch.cog as cogmod
    cog = object.__new__(cogmod.CustomMatch)   # no bot needed for the helper
    game = next(g for g in await DB.get_all_games() if g.game_id == OW_GAME)
    floor = await DB.get_mmr_floor(OW_GAME, default=None)
    check(floor is not None, "Overwatch has no ladder floor configured")

    # Drift the aggregate away from the per-role peak -- the exact shape of the
    # bug that shipped: three 800 roles reading as 1200 overall.
    await wipe()
    for role in OW_ROLES:
        await write_role(role, 1200)
    # The audit joins the two tables, so the aggregate row has to exist -- which
    # it always does in practice, since every setup path writes it.
    agg = await DB.get_player_stats(PID, OW_GAME)
    agg.mmr = 3200
    await DB.update_player_stats(agg)
    check((await DB.get_player_stats(PID, OW_GAME)).effective_mmr == 3200,
          "fixture failed to drift the aggregate")
    fixed = await cog._audit_ratings_for_game(game)
    agg = await DB.get_player_stats(PID, OW_GAME)
    check(agg.effective_mmr == 1200,
          f"aggregate stayed at {agg.effective_mmr}; the audit must pull it back "
          "to the 1200 per-role peak")
    check(fixed >= 1, "the audit reported no repair for a drifted aggregate")
    print("   ok   aggregate drift pulled back to the per-role peak")

    # A rating stranded under the bottom band can never climb out on its own,
    # and previously only a hand-run script fixed it.
    async with DB._get_db() as db:
        await db.execute(
            "UPDATE ow_role_stats SET mmr = ? WHERE player_id = ? AND game_id = ? AND role = 'Tank'",
            (floor - 500, PID, OW_GAME))
        await db.commit()
    await cog._audit_ratings_for_game(game)
    rows = await DB.get_all_ow_role_stats(PID, OW_GAME)
    check(rows["Tank"].effective_mmr == floor,
          f"stranded Tank sat at {rows['Tank'].effective_mmr}, expected the {floor} floor")
    print(f"   ok   rating stranded below the floor lifted to {floor}")

    # A healthy server must cost nothing, or the hourly pass becomes a write storm.
    check(await cog._audit_ratings_for_game(game) == 0,
          "the audit reported repairs on an already-healthy game")
    print("   ok   healthy pass makes no writes")
    await wipe()


async def rank_role_switch():
    """Rank-role granting is off by default, and the ladder survives it."""
    print("\nrank role switch")
    prior = await DB.get_rank_roles_enabled()
    try:
        await DB.set_rank_roles_enabled(False)
        check(not await DB.get_rank_roles_enabled(), "the switch did not read back as off")

        # The ladder must keep working with granting off -- it is the setup
        # vocabulary, the band drop for a new role, the PC bump step and the floor.
        bands = await DB.get_mmr_roles_with_labels(OW_GAME)
        check(bands, "the rank ladder disappeared when granting was switched off")
        check(await DB.get_mmr_floor(OW_GAME, default=None) is not None,
              "the loss floor is derived from the ladder and must survive")
        await wipe()
        await write_role("Tank", 3200)
        seeded = await DB.get_ow_role_stats(PID, OW_GAME, "DPS")
        check(seeded.effective_mmr in [d['mmr'] for d in bands.values()],
              "new-role seeding stopped landing on a band when granting went off")
        print("   ok   ladder still drives labels, seeding and the floor")

        await DB.set_rank_roles_enabled(True)
        check(await DB.get_rank_roles_enabled(), "the switch did not read back as on")
        print("   ok   switch flips both ways")
    finally:
        await DB.set_rank_roles_enabled(prior)
        await wipe()


async def main():
    await seeding()
    await aggregate()
    await all_unranked()
    await rating_audit()
    await rank_role_switch()
    dropdowns()
    interaction_claim()
    panel()
    print()
    if FAILS:
        for f in FAILS:
            print(f"FAIL: {f}")
        return 1
    print("OW role setup + panel tree: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
