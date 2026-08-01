"""Verify the /cm_settings page tree: layout, navigation and expiry.

The panel is now a tree of pages that all draw onto one ephemeral message, so
the things that can silently break are structural: a row overflowing Discord's
5-component limit, a page losing its Back button after a rebuild, a parent
expiring while you work inside a child, or a page that never got wired up.
"""
import asyncio
import sys
import types
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import discord
from cogs.custommatch import views_settings as vs
from cogs.custommatch.models import GameConfig, QueueType, CaptainSelection


class FakeMsg:
    def __init__(self, content):
        self.content, self.edited = content, None

    async def edit(self, content=None, view=None):
        self.edited = (content, view)


class FakeRole:
    def __init__(self, rid):
        self.id, self.name = rid, f"Role{rid}"


class FakeGuild:
    id = 1

    def get_role(self, rid):
        return FakeRole(rid)

    def get_channel(self, cid):
        return None

    def get_thread(self, cid):
        return None


def make_game(name="Overwatch", game_id=3) -> GameConfig:
    return GameConfig(
        game_id=game_id,
        name=name,
        player_count=12,
        queue_type=QueueType.MMR,
        captain_selection=CaptainSelection.RANDOM,
        queue_channel_id=None,
        verified_role_id=None,
    )


def layout_errors(name, view):
    """Discord renders at most 5 rows; a select eats a whole row."""
    problems = []
    per_row = Counter()
    for item in view.children:
        row = item._rendered_row if item._rendered_row is not None else item.row
        row = 0 if row is None else row
        if row > 4:
            problems.append(f"{name}: item on row {row} (max 4)")
        per_row[row] += 1 if isinstance(item, discord.ui.Button) else 5
    for row, weight in per_row.items():
        if weight > 5:
            problems.append(f"{name}: row {row} over capacity ({weight}/5)")
    if len(view.children) > 25:
        problems.append(f"{name}: {len(view.children)} components (max 25)")
    return problems


async def main():
    fails = []
    cog = types.SimpleNamespace(bot=types.SimpleNamespace(guilds=[]))
    guild = FakeGuild()
    game = make_game()

    hub = vs.SettingsView(cog)
    games_page = vs.GamesPage(cog, hub)
    game_page = vs.GameSettingsPage(cog, games_page, game)
    channels_page = vs.ChannelsPage(cog, hub)
    access_page = vs.AccessPage(cog, hub)
    data_page = vs.DataPage(cog, hub)
    maint_page = vs.MaintenancePage(cog, hub)
    ladder = vs.RankLadderPage(cog, game_page, game)

    pages = [
        ("SettingsView", hub),
        ("GamesPage", games_page),
        ("GameSettingsPage", game_page),
        ("ChannelsPage", channels_page),
        ("AccessPage", access_page),
        ("DataPage", data_page),
        ("MaintenancePage", maint_page),
        ("EmojisPage", vs.EmojisPage(cog, games_page)),
        ("QueueRolePage", vs.QueueRolePage(cog, game_page, game)),
        ("RankLadderPage", ladder),
        ("PlayerMMRPage", vs.PlayerMMRPage(cog, game_page, game)),
        ("AdminOffsetPage", vs.AdminOffsetPage(cog, game_page, game)),
        ("AddRankPage", vs.AddRankPage(cog, ladder, game)),
        ("RemoveRankPage", vs.RemoveRankPage(cog, ladder, game, {1: 800, 2: 1200}, guild)),
        ("DeleteGamePage", vs.DeleteGamePage(cog, game_page, game)),
        ("AdminRolePage", vs.AdminRolePage(cog, access_page)),
        ("ModRolesPage", vs.ModRolesPage(cog, access_page)),
        ("BlacklistPage", vs.BlacklistPage(cog, access_page)),
        ("WipeStatsPage", vs.WipeStatsPage(cog, data_page)),
        ("ChannelTargetPage/log", vs.ChannelTargetPage(cog, channels_page, "log")),
        ("ChannelTargetPage/queue", vs.ChannelTargetPage(cog, channels_page, "queue", game)),
        ("GamePickerPage", vs.GamePickerPage(cog, hub, [game], None, title="t", blurb="b")),
    ]

    print(f"{'page':<26}{'items':>7}{'back':>7}{'rows':>16}")
    for name, page in pages:
        rows = sorted({(i._rendered_row if i._rendered_row is not None else i.row) or 0
                       for i in page.children})
        has_back = any(isinstance(i, vs.BackButton) for i in page.children)
        print(f"{name:<26}{len(page.children):>7}{str(has_back):>7}{str(rows):>16}")
        fails.extend(layout_errors(name, page))
        if page is not hub and not has_back:
            fails.append(f"{name} has no Back button")
        if page is hub and has_back:
            fails.append("hub should not have a Back button")

    print("\n-- every hub category is reachable --")
    expected = {"Games", "Channels", "Access", "Stats & Data", "Maintenance", "Refresh"}
    labels = {c.label for c in hub.children}
    print(f"   hub buttons: {sorted(labels)}")
    if labels != expected:
        fails.append(f"hub buttons {sorted(labels)} != {sorted(expected)}")

    print("\n-- editor views keep their Back button across rebuilds --")
    for name, view, rebuild in [
        ("GameTogglesView", vs.GameTogglesView(cog, game), "update_buttons"),
        ("SecondaryQueueSettingsView", vs.SecondaryQueueSettingsView(cog, game), "_rebuild_buttons"),
    ]:
        view.attach_back(game_page, row=3)
        before = sum(isinstance(i, vs.BackButton) for i in view.children)
        getattr(view, rebuild)()
        after = sum(isinstance(i, vs.BackButton) for i in view.children)
        print(f"   {name:<28} back buttons {before} -> {after}")
        if after != 1:
            fails.append(f"{name} has {after} back buttons after {rebuild}()")
        fails.extend(layout_errors(name, view))

    print("\n-- Overwatch weights only appear for an Overwatch game --")
    val_page = vs.GameSettingsPage(cog, games_page, make_game("Valorant", 1))
    ow_labels = {c.label for c in game_page.children}
    val_labels = {c.label for c in val_page.children}
    print(f"   Overwatch: {'Overwatch Weights' in ow_labels} / Valorant: {'Overwatch Weights' in val_labels}")
    if "Overwatch Weights" not in ow_labels:
        fails.append("Overwatch game is missing the weights button")
    if "Overwatch Weights" in val_labels:
        fails.append("non-Overwatch game shows the weights button")

    print("\n-- every channel target is settable --")
    for key, spec in vs.CHANNEL_TARGETS.items():
        scoped = "field" in spec
        page = vs.ChannelTargetPage(cog, channels_page, key, game if scoped else None)
        has_select = any(isinstance(i, discord.ui.ChannelSelect) for i in page.children)
        print(f"   {key:<11} {'per-game' if scoped else 'server':<9} select={has_select}")
        if not has_select:
            fails.append(f"channel target {key} has no channel select")
        if scoped == ("config_key" in spec):
            fails.append(f"channel target {key} must have exactly one of field/config_key")
    select = next(i for i in channels_page.children if isinstance(i, vs.ChannelTargetSelect))
    if {o.value for o in select.options} != set(vs.CHANNEL_TARGETS):
        fails.append("Channels dropdown does not offer every target")
    for opt in select.options:
        if len(opt.description or "") > 100:
            fails.append(f"channel option {opt.value} description over 100 chars")

    print("\n-- a child keeps its whole ancestor chain alive --")
    loop = asyncio.get_running_loop()
    for page in (hub, games_page, game_page, ladder):
        page._View__timeout_expiry = loop.time() + 5
    cog.is_cm_admin = lambda user: asyncio.sleep(0, result=True)
    await ladder.interaction_check(types.SimpleNamespace(user=None))
    gained = [(n, p._View__timeout_expiry - (loop.time() + 5))
              for n, p in [("hub", hub), ("games", games_page),
                           ("game", game_page), ("ladder", ladder)]]
    print("   " + ", ".join(f"{n} +{g:.0f}s" for n, g in gained))
    for n, g in gained:
        if g < 200:
            fails.append(f"{n} timeout not refreshed by nested interaction (+{g:.0f}s)")

    print("\n-- on_timeout greys the panel --")
    page = vs.GamesPage(cog, hub)
    page.message = FakeMsg("")
    await page.on_timeout()
    content, sent_view = page.message.edited
    print(f"   children disabled: {all(c.disabled for c in page.children)}")
    print(f"   note appended    : {vs.ExpiringView.expiry_note.strip() in content}")
    if not all(c.disabled for c in page.children):
        fails.append("children not disabled on timeout")
    if vs.ExpiringView.expiry_note.strip() not in content:
        fails.append("expiry note missing")
    if sent_view is not page:
        fails.append("timed-out view not re-sent")

    print("\n-- render() revives a page that had already greyed out --")
    revived = vs.GamesPage(cog, hub)
    for c in revived.children:
        c.disabled = True

    class FakeResponse:
        def __init__(self):
            self.embed = None

        async def edit_message(self, embed=None, view=None):
            self.embed, self.view = embed, view

    class FakeInteraction:
        def __init__(self):
            self.response = FakeResponse()
            self.guild = FakeGuild()
            self.message = FakeMsg("")

    # build_embed hits the database, so stand in for it: render()'s job here is
    # re-enabling the components it is about to send back.
    revived.build_embed = lambda g: asyncio.sleep(0, result=discord.Embed(title="x"))
    it = FakeInteraction()
    await revived.render(it, flash="✅ done")
    print(f"   re-enabled: {not any(c.disabled for c in revived.children)}")
    print(f"   flash kept: {'✅ done' in (it.response.embed.description or '')}")
    if any(c.disabled for c in revived.children):
        fails.append("render() left components disabled")
    if "✅ done" not in (it.response.embed.description or ""):
        fails.append("render() dropped the flash message")

    print("\n" + "=" * 50)
    if fails:
        print(f"{len(fails)} FAILURES")
        for f in fails:
            print("  !!", f)
    else:
        print("all checks passed")
    return 1 if fails else 0


sys.exit(asyncio.run(main()))
