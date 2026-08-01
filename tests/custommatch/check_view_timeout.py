"""Verify the MMR-role panels expire visibly and refresh while in use."""
import asyncio, sys, types
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from cogs.custommatch import views_settings as vs

class FakeMsg:
    def __init__(self, content): self.content, self.edited = content, None
    async def edit(self, content=None, view=None):
        self.edited = (content, view)

class FakeRole:
    def __init__(self, rid): self.id, self.name = rid, f"Role{rid}"

class FakeGuild:
    def get_role(self, rid): return FakeRole(rid)

async def main():
    fails = []
    cog = types.SimpleNamespace()

    views = [
        ("MMRRolesView",           vs.MMRRolesView(cog, 3),                         600),
        ("AddMMRRoleSelectView",   vs.AddMMRRoleSelectView(cog, 3),                 300),
        ("RemoveMMRRoleSelectView",vs.RemoveMMRRoleSelectView(cog, 3, {1:800,2:1200}, FakeGuild()), 300),
        ("MMRRoleSelectView",      vs.MMRRoleSelectView(cog, 3, 42, FakeGuild()),   300),
    ]

    print(f"{'view':<26}{'base ok':>9}{'timeout':>9}{'children':>10}")
    for name, v, want in views:
        base_ok = isinstance(v, vs.ExpiringView)
        t_ok = v.timeout == want
        print(f"{name:<26}{str(base_ok):>9}{v.timeout:>9.0f}{len(v.children):>10}")
        if not base_ok: fails.append(f"{name} is not an ExpiringView")
        if not t_ok:    fails.append(f"{name} timeout {v.timeout} != {want}")

    print("\n-- on_timeout greys the panel --")
    v = vs.MMRRolesView(cog, 3)
    v.message = FakeMsg("**MMR Roles for Overwatch**\n• Bronze: 800 MMR")
    before = [c.disabled for c in v.children]
    await v.on_timeout()
    after = [c.disabled for c in v.children]
    content, edited_view = v.message.edited
    print(f"   children disabled: {before} -> {after}")
    print(f"   note appended    : {vs.ExpiringView.expiry_note.strip() in content}")
    print(f"   view re-sent     : {edited_view is v}")
    if any(before) or not all(after): fails.append("children not disabled on timeout")
    if vs.ExpiringView.expiry_note.strip() not in content: fails.append("expiry note missing")

    print("\n-- on_timeout survives a missing/deleted message --")
    v2 = vs.MMRRolesView(cog, 3)
    v2.message = None
    await v2.on_timeout()          # must not raise
    class Boom(FakeMsg):
        async def edit(self, **kw):
            import discord
            resp = types.SimpleNamespace(status=404, reason='Not Found')
            raise discord.HTTPException(resp, 'gone')
    v3 = vs.MMRRolesView(cog, 3); v3.message = Boom("x")
    await v3.on_timeout()          # must not raise
    print("   no message: ok / deleted message: ok")

    print("\n-- interaction_check restarts the clock --")
    v4 = vs.MMRRolesView(cog, 3)
    loop = asyncio.get_running_loop()
    v4._View__timeout_expiry = loop.time() + 5      # pretend it's nearly dead
    nearly = v4._View__timeout_expiry
    ok = await v4.interaction_check(None)
    after_expiry = v4._View__timeout_expiry
    gained = after_expiry - nearly
    print(f"   check returned {ok}, expiry pushed out by {gained:.0f}s")
    if not ok: fails.append("interaction_check returned False")
    if gained < 500: fails.append(f"timeout not refreshed (only +{gained:.0f}s)")

    print("\n" + "="*50)
    if fails:
        print(f"{len(fails)} FAILURES")
        for f in fails: print("  !!", f)
    else:
        print("all checks passed")
    return 1 if fails else 0

sys.exit(asyncio.run(main()))
