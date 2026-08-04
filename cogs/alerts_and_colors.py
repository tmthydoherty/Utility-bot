"""
Alerts & Colors Cog

Manages a single channel with four self-serve embeds, posted in order:
  1. Alert Roles  — toggleable ping roles for any member
  2. Tier 1 Colors — free name colors, open to everyone
  3. Tier 2 Colors — name colors reserved for Tier 2 (and above)
  4. Tier 3 Colors — name colors reserved for Tier 3

Reward tier model (points come from cogs/newcomer.py):
  - A member holds exactly ONE reward gate role: Tier 3, or Tier 2, or none
    (Tier 1 is the baseline everyone has, never managed here).
  - VIP roles are permanent point floors: holding the Tier 2 VIP role is worth
    exactly the current Tier 2 threshold in points, the Tier 3 VIP role the
    Tier 3 threshold. The floor is virtual (never written to the points DB, so
    it never decays) and is ADDED to points the member earns, meaning a Tier 2
    VIP climbs to Tier 3 by earning the difference. Thresholds are owned by
    newcomer.py, which is where effective points are computed.
  - A member wears at most ONE color role at a time. The last color they chose
    for each tier is recorded in config, so on demotion the bot removes the
    higher color and automatically restores the color they last wore at the
    tier they land on. Promotions get a DM invite; demotions are silent.
  - This cog is the single owner of ALL gate/color role mutations
    (set_member_tier). The newcomer cog only computes the target tier from
    effective points and delegates, so there is one serialized, idempotent
    code path.

Self-healing: an hourly audit reconciles every member wearing a tracked color
(or having a saved record) against their current gate roles, so missed gateway
events, API failures, or offline periods converge to the correct state. The
newcomer cog runs the matching hourly gate-role audit (nobody keeps a Tier 2/3
role without the effective points for it); "Run Tier Audit" on the panel
triggers that same pass on demand.

All roles are pre-created by admins and mapped via /alerts_colors_panel.
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import aiohttp
import asyncio
import io
import json
import logging
import os
import time
from pathlib import Path
from urllib.parse import urlparse

log = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "alerts_colors_config.json"
# Pre-rename config file; read once on first boot, then superseded by CONFIG_PATH.
LEGACY_CONFIG_PATH = Path(__file__).resolve().parent.parent / "color_roles_config.json"

# A string select holds max 25 options; every list reserves one option for
# "Remove" ("Remove all" on alerts, "Remove my color" on tiers).
MAX_ALERT_ROLES = 24
MAX_COLORS_PER_TIER = 24

REMOVE_VALUE = "remove"

# How long (seconds) role changes made by this cog suppress the manual-change
# listener, and how long a promotion DM is deduplicated per member/tier.
MUTATION_GUARD_SECONDS = 15
DM_DEDUP_SECONDS = 600

# Pause between role mutations during bulk audits (rate-limit kindness)
AUDIT_SLEEP = 0.3

# Admin config views stay alive this long. Multi-role selects take a while to
# fill in — a short timeout makes submissions land on a dead view, which
# Discord surfaces as an unexplained "This interaction failed".
CONFIG_VIEW_TIMEOUT = 900

SECTION_META = {
    "alerts": {"title": "🔔 Alert Roles", "color": 0x5865F2},
    "t1": {"title": "🎨 Name Colors", "color": 0x2ECC71},
    "t2": {"title": "✨ Name Colors — Tier 2", "color": 0xF1C40F},
    "t3": {"title": "🌟 Name Colors — Tier 3", "color": 0x9B59B6},
}

# Posting order in the channel (Alert Roles first, then tiers 1 → 3).
# Section banners (plain image messages acting as headers) slot in before
# the alerts embed and before the first color embed when configured.
SECTION_ORDER = ("alerts", "t1", "t2", "t3")
BANNER_IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".gif", ".webp")
BANNER_MAX_BYTES = 8 * 1024 * 1024

CUSTOM_IDS = {
    "alerts": "alertscolors:alerts",
    "t1": "alertscolors:t1",
    "t2": "alertscolors:t2",
    "t3": "alertscolors:t3",
}
# Embeds posted before the rename carry the old custom_ids. Keep routing them
# so live panels keep working until they're reposted.
LEGACY_CUSTOM_IDS = {
    "colorroles:alerts": "alerts",
    "colorroles:t1": "t1",
    "colorroles:t2": "t2",
    "colorroles:t3": "t3",
}
CUSTOM_ID_TO_SECTION = {v: k for k, v in CUSTOM_IDS.items()}
CUSTOM_ID_TO_SECTION.update(LEGACY_CUSTOM_IDS)


def _load_config() -> dict:
    try:
        return json.loads(CONFIG_PATH.read_text())
    except json.JSONDecodeError:
        log.error(f"{CONFIG_PATH.name} is corrupt — starting from an empty config.")
        return {}
    except FileNotFoundError:
        pass
    # First boot after the rename: adopt the old file (left in place as a backup).
    try:
        data = json.loads(LEGACY_CONFIG_PATH.read_text())
        log.info(f"Migrated config from {LEGACY_CONFIG_PATH.name} to {CONFIG_PATH.name}")
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_config(data: dict):
    tmp = f"{CONFIG_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp, CONFIG_PATH)


class AlertsAndColors(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._config = _load_config()
        # If we just adopted the pre-rename config, write it out under the new
        # name straight away rather than waiting for the next admin change —
        # otherwise the migration only lives in memory until then.
        if self._config and not CONFIG_PATH.exists():
            self.save()
        # Per-member serialization so concurrent point awards can't interleave
        self._locks: dict[int, asyncio.Lock] = {}
        # member_id -> monotonic expiry; suppresses on_member_update for our own ops
        self._mutating: dict[int, float] = {}
        # (member_id, tier_key) -> monotonic timestamp of last invite DM
        self._recent_dm: dict[tuple, float] = {}
        self.audit_task.start()

    def cog_unload(self):
        self.audit_task.cancel()

    # ─── Config helpers ───

    def _gcfg(self, guild_id: int) -> dict:
        g = self._config.setdefault(str(guild_id), {})
        g.setdefault("channel_id", None)
        g.setdefault("alert_roles", [])
        g.setdefault("gates", {"t1": None, "t2": None, "t3": None})
        # VIP roles: permanent point floors, worth their tier's threshold.
        # {"t2": [role_id, ...], "t3": [role_id, ...]}
        vip = g.setdefault("vip_roles", {"t2": [], "t3": []})
        vip.setdefault("t2", [])
        vip.setdefault("t3", [])
        g.setdefault("colors", {"t1": [], "t2": [], "t3": []})
        g.setdefault("messages", {})
        # last color chosen per member, per tier: {user_id: {"t1": role_id, ...}}
        g.setdefault("last_colors", {})
        # section header banners: image URLs, re-uploaded as plain messages
        g.setdefault("banners", {"alerts": None, "colors": None})
        # URL each posted banner message currently shows (to detect changes)
        g.setdefault("posted_banners", {})
        return g

    def save(self):
        _save_config(self._config)

    def set_gate(self, guild_id: int, tier_key: str, role_id: int):
        """Set a tier gate role. Also called by the newcomer cog's admin panel."""
        cfg = self._gcfg(guild_id)
        cfg["gates"][tier_key] = role_id
        # A gate role must never double as a VIP role — it would grant the very
        # points that justify it, so nobody could ever lose it.
        for key in ("t2", "t3"):
            ids = cfg["vip_roles"].get(key) or []
            if role_id in ids:
                cfg["vip_roles"][key] = [rid for rid in ids if rid != role_id]
        self.save()

    def get_gate(self, guild_id: int, tier_key: str):
        return self._gcfg(guild_id)["gates"].get(tier_key)

    def make_gate_view(self) -> "GateRolesView":
        """Build the gate/VIP role selection view (shared with the newcomer panel)."""
        return GateRolesView(self)

    # ─── VIP roles (public API — newcomer.py turns these into point floors) ───

    def get_vip_role_ids(self, guild_id: int, tier_key: str) -> list:
        return list(self._gcfg(guild_id)["vip_roles"].get(tier_key) or [])

    def get_vip_tier(self, member: discord.Member) -> int:
        """Highest VIP tier the member holds (3, 2, or 0 for none).

        The VIP role itself grants no colors — it grants points, and points
        grant the gate role, so there stays exactly one source of truth for
        color access.
        """
        cfg = self._gcfg(member.guild.id)
        for tier in (3, 2):
            for rid in cfg["vip_roles"].get(f"t{tier}") or []:
                if member.get_role(rid):
                    return tier
        return 0

    def get_member_tier(self, member: discord.Member) -> int:
        """The reward tier the member currently holds (1 = baseline)."""
        return self._reward_tier(member, self._gcfg(member.guild.id))

    def iter_vip_members(self, guild: discord.Guild):
        """Every member holding any VIP role, deduplicated."""
        cfg = self._gcfg(guild.id)
        seen = set()
        for tier_key in ("t3", "t2"):
            for rid in cfg["vip_roles"].get(tier_key) or []:
                role = guild.get_role(rid)
                if not role:
                    continue
                for member in role.members:
                    if member.bot or member.id in seen:
                        continue
                    seen.add(member.id)
                    yield member

    # ─── Color record helpers ───

    def _records(self, cfg: dict, user_id: int) -> dict:
        return cfg["last_colors"].get(str(user_id), {})

    def _set_record(self, cfg: dict, user_id: int, tier_key: str, role_id: int):
        cfg["last_colors"].setdefault(str(user_id), {})[tier_key] = role_id
        self.save()

    def _drop_record(self, cfg: dict, user_id: int, tier_key: str):
        recs = cfg["last_colors"].get(str(user_id))
        if recs and tier_key in recs:
            del recs[tier_key]
            if not recs:
                del cfg["last_colors"][str(user_id)]
            self.save()

    def _clear_records(self, cfg: dict, user_id: int):
        if cfg["last_colors"].pop(str(user_id), None) is not None:
            self.save()

    # ─── Tier / guard helpers ───

    def _reward_tier(self, member: discord.Member, cfg: dict) -> int:
        """Member's reward tier from the gate roles they hold (1 = baseline)."""
        for tier in (3, 2):
            rid = cfg["gates"].get(f"t{tier}")
            if rid and member.get_role(rid):
                return tier
        return 1

    def _color_access_tier(self, member: discord.Member, cfg: dict) -> int:
        """Highest color tier the member may pick from (Tier 1 is open to everyone)."""
        for tier in (3, 2):
            rid = cfg["gates"].get(f"t{tier}")
            if rid and member.get_role(rid):
                return tier
        return 1

    def _all_color_ids(self, cfg: dict) -> set:
        ids = set()
        for t in ("t1", "t2", "t3"):
            ids.update(cfg["colors"][t])
        return ids

    def _guard(self, member_id: int):
        """Mark a member as being mutated by us, so on_member_update ignores the echo."""
        self._mutating[member_id] = time.monotonic() + MUTATION_GUARD_SECONDS

    def _is_guarded(self, member_id: int) -> bool:
        expiry = self._mutating.get(member_id)
        if expiry is None:
            return False
        if expiry < time.monotonic():
            del self._mutating[member_id]
            return False
        return True

    # ============================================================
    #  Tier enforcement — the ONLY place gate roles are changed
    # ============================================================

    async def set_member_tier(self, member: discord.Member, target: int,
                              *, allow_dm: bool = True) -> bool:
        """Enforce a member's reward tier (1, 2 or 3). Idempotent and
        serialized per member. Returns True if any role was changed.

        Promotion: grant new gate first, remove the old one, auto-restore
        their last recorded color for the new tier (if any), then DM.
        Demotion: swap gates, strip too-high colors, silently restore the
        color they last chose at the tier they land on. No DM.
        """
        guild = member.guild
        cfg = self._gcfg(guild.id)

        desired = None
        if target >= 2:
            rid = cfg["gates"].get(f"t{min(target, 3)}")
            desired = guild.get_role(rid) if rid else None
            if not desired:
                # Target tier's role isn't configured/available — do nothing
                # rather than half-apply (never strip without granting).
                return False

        lock = self._locks.setdefault(member.id, asyncio.Lock())
        async with lock:
            fresh = guild.get_member(member.id)
            if fresh:
                member = fresh

            current = self._reward_tier(member, cfg)
            gate_roles = [
                guild.get_role(cfg["gates"][k]) for k in ("t2", "t3")
                if cfg["gates"].get(k)
            ]
            changed = False
            self._guard(member.id)

            # 1. Grant the new gate first — never leave a member without the
            #    tier they've earned because a later step failed.
            if desired and not member.get_role(desired.id):
                try:
                    await member.add_roles(desired, reason=f"Newcomer tier: reached Tier {target}")
                    log.info(f"Granted {desired.name} to {member.name}")
                    changed = True
                except discord.HTTPException as e:
                    log.error(f"Failed to grant {desired.name} to {member.name}: {e}")
                    await self._report(f"set_member_tier: grant {desired.name} -> {member.name}: {e}")
                    return changed  # abort: keep their old gate/colors intact

            # 2. Remove any other gate roles they hold (exclusive model).
            stale_gates = [
                r for r in gate_roles
                if r and r != desired and member.get_role(r.id)
            ]
            if stale_gates:
                try:
                    await member.remove_roles(*stale_gates, reason="Newcomer tier changed")
                    log.info(f"Removed {', '.join(r.name for r in stale_gates)} from {member.name}")
                    changed = True
                except discord.HTTPException as e:
                    # Non-fatal: they briefly hold two gates; hourly audit retries.
                    log.error(f"Failed to remove stale gate(s) from {member.name}: {e}")
                    await self._report(f"set_member_tier: remove stale gates from {member.name}: {e}")

            # 3. Colors.
            if target < current:
                if await self._reconcile_colors(member, tier=target):
                    changed = True
            elif target > current:
                if await self._restore_recorded_color(member, cfg, f"t{target}"):
                    changed = True
                if allow_dm:
                    await self._send_tier_dm_once(member, f"t{target}")

            return changed

    async def _restore_recorded_color(self, member: discord.Member, cfg: dict,
                                      tier_key: str) -> bool:
        """Re-apply the member's last recorded color for tier_key, if valid."""
        rid = self._records(cfg, member.id).get(tier_key)
        if not rid:
            return False
        role = member.guild.get_role(rid)
        if not role or rid not in cfg["colors"][tier_key]:
            self._drop_record(cfg, member.id, tier_key)  # stale record
            return False

        stale = [r for r in member.roles if r.id in self._all_color_ids(cfg) and r.id != rid]
        try:
            if stale:
                await member.remove_roles(*stale, reason="Swapping to restored tier color")
            if not member.get_role(rid):
                await member.add_roles(role, reason=f"Restored last chosen Tier {tier_key[1]} color")
                log.info(f"Restored color {role.name} for {member.name}")
            return True
        except discord.HTTPException as e:
            log.error(f"Failed restoring color {role.name} for {member.name}: {e}")
            await self._report(f"restore color {role.name} -> {member.name}: {e}")
            return False

    async def _reconcile_colors(self, member: discord.Member, tier: int = None) -> bool:
        """Make a member's color roles consistent with a tier. Idempotent.

        - Strips tracked colors of a HIGHER tier than they hold.
        - Enforces a single color role (keeps the highest-tier one).
        - If they end up with no color, restores their last recorded color for
          their tier, falling back down tier by tier.
        - Syncs the record to whatever they legitimately wear (reality wins).
        """
        guild = member.guild
        cfg = self._gcfg(guild.id)
        if tier is None:
            tier = self._color_access_tier(member, cfg)

        worn = []
        for t in (1, 2, 3):
            for rid in cfg["colors"][f"t{t}"]:
                role = member.get_role(rid)
                if role:
                    worn.append((t, role))

        to_remove = [role for t, role in worn if t > tier]
        keep = sorted(
            [(t, role) for t, role in worn if t <= tier],
            key=lambda x: x[0], reverse=True,
        )
        if len(keep) > 1:  # single-color rule: keep highest tier only
            to_remove += [role for _, role in keep[1:]]
            keep = keep[:1]

        changed = False
        try:
            if to_remove:
                await member.remove_roles(*to_remove, reason="Color tier reconciliation")
                log.info(
                    f"Reconcile: removed {', '.join(r.name for r in to_remove)} from {member.name}"
                )
                changed = True

            if keep:
                t, role = keep[0]
                if self._records(cfg, member.id).get(f"t{t}") != role.id:
                    self._set_record(cfg, member.id, f"t{t}", role.id)
            else:
                recs = self._records(cfg, member.id)
                for t in range(min(tier, 3), 0, -1):
                    rid = recs.get(f"t{t}")
                    if not rid:
                        continue
                    role = guild.get_role(rid)
                    if not role or rid not in cfg["colors"][f"t{t}"]:
                        self._drop_record(cfg, member.id, f"t{t}")
                        continue
                    await member.add_roles(role, reason=f"Restored last chosen Tier {t} color")
                    log.info(f"Reconcile: restored {role.name} for {member.name}")
                    changed = True
                    break
        except discord.HTTPException as e:
            log.error(f"Color reconciliation failed for {member.name}: {e}")
            await self._report(f"reconcile colors for {member.name}: {e}")
        return changed

    async def _report(self, msg: str):
        try:
            await self.bot.error_reporter.report("AlertsAndColors", msg)
        except Exception:
            pass

    # ─── Promotion DM (deduplicated) ───

    async def _send_tier_dm_once(self, member: discord.Member, tier_key: str):
        now = time.monotonic()
        key = (member.id, tier_key)
        last = self._recent_dm.get(key)
        if last and now - last < DM_DEDUP_SECONDS:
            return
        self._recent_dm[key] = now
        # opportunistic prune
        if len(self._recent_dm) > 500:
            self._recent_dm = {
                k: v for k, v in self._recent_dm.items() if now - v < DM_DEDUP_SECONDS
            }
        await self._send_tier_dm(member, tier_key)

    async def _send_tier_dm(self, member: discord.Member, tier_key: str):
        cfg = self._gcfg(member.guild.id)
        gate_id = cfg["gates"].get(tier_key)
        gate = member.guild.get_role(gate_id) if gate_id else None
        role_name = gate.name if gate else f"Tier {tier_key[1]}"

        channel_id = cfg["channel_id"]
        msg_id = cfg["messages"].get(tier_key)
        if channel_id and msg_id:
            link = f"https://discord.com/channels/{member.guild.id}/{channel_id}/{msg_id}"
        elif channel_id:
            link = f"https://discord.com/channels/{member.guild.id}/{channel_id}"
        else:
            link = None

        # A VIP's floor at this tier never decays, so don't warn them about
        # something that can't happen to them.
        vip_secured = self.get_vip_tier(member) >= int(tier_key[1])

        if tier_key == "t2":
            outro = (
                "💎 **Your VIP perk:** this one's yours to keep — your Tier 2 access doesn't "
                "expire. Any points you earn from here stack on top and count toward Tier 3."
                if vip_secured else
                "⏳ **One heads-up:** tier colors aren't forever — they run on your recent "
                "welcoming activity. Keep being friendly with the new folks and your color "
                "will stick around for the long haul. 💛"
            )
            text = (
                f"Hey {member.display_name}! 👋\n\n"
                f"You've been making our newcomers feel right at home, and it hasn't gone "
                f"unnoticed — you've just earned the **{role_name}** role! 🎉\n\n"
                "As a thank-you, you now have access to the exclusive **Tier 2 name colors**. "
                "Head over and pick whichever one you like:"
                + (f"\n{link}\n\n" if link else "\n\n")
                + outro
            )
        else:
            outro = (
                "💎 **Your VIP perk:** this one's yours to keep — your Tier 3 access doesn't "
                "expire. Enjoy the rarest colors on the server."
                if vip_secured else
                "⏳ **One heads-up:** tier colors aren't forever — they run on your recent "
                "welcoming activity. Keep making newcomers feel welcome and your color will "
                "stick around. 💛"
            )
            text = (
                f"Hey {member.display_name}! 🌟\n\n"
                f"Incredible — you've earned the **{role_name}** role, our highest welcoming "
                "honor! You're a big part of what makes this server feel like home. 🏡\n\n"
                "You now have access to the exclusive **Tier 3 name colors** — the rarest of "
                "the bunch. Pick your favorite here:"
                + (f"\n{link}\n\n" if link else "\n\n")
                + outro
            )

        try:
            await member.send(text)
            log.info(f"Sent Tier {tier_key[1]} color invite DM to {member.name}")
        except (discord.Forbidden, discord.HTTPException) as e:
            log.info(f"Could not DM {member.name} their tier color invite: {e}")

    # ─── Embed / view builders ───

    def role_list_summary(self, guild: discord.Guild, list_key: str) -> str:
        """Live summary shown above the add/remove editor for a role list."""
        cfg = self._gcfg(guild.id)
        if list_key == "alerts":
            ids, label, cap = cfg["alert_roles"], "Alert roles", MAX_ALERT_ROLES
        else:
            ids = cfg["colors"][list_key]
            label, cap = f"Tier {list_key[1]} colors", MAX_COLORS_PER_TIER
        lines = self._role_lines(guild, ids)
        return (
            f"**{label}** ({len(ids)}/{cap}) — use the menus to add or remove, "
            "then **Post / Update Embeds** on the panel:\n"
            + ("\n".join(lines) if lines else "*none yet*")
        )

    def _role_lines(self, guild: discord.Guild, role_ids: list) -> list:
        lines = []
        for i, rid in enumerate(role_ids, start=1):
            role = guild.get_role(rid)
            if role:
                lines.append(f"`{i}.` {role.mention}")
        return lines

    def _build_embed(self, guild: discord.Guild, cfg: dict, section: str) -> discord.Embed:
        meta = SECTION_META[section]
        embed = discord.Embed(title=meta["title"], color=meta["color"])

        if section == "alerts":
            embed.description = (
                "Choose which alerts/pings you'd like to receive or remove. "
                "Use the menu below to **toggle** an alert role on or off.\n\n"
                "You can also manage your roles anytime in <id:customize>."
            )
            return embed

        lines = self._role_lines(guild, cfg["colors"][section])
        listing = "\n".join(lines) if lines else "*No colors configured yet.*"

        if section == "t1":
            embed.description = (
                "If you'd like to change your name color choose one from the menu "
                "below — you can switch or remove it anytime.\n\n"
                + listing
            )
        elif section == "t2":
            embed.description = (
                "Exclusive colors reserved for members who help make our "
                "newcomers feel welcome.\n\n"
                + listing
            )
            embed.set_footer(text="Tier colors run on recent welcoming activity — stay friendly to keep yours!")
        else:  # t3
            embed.description = (
                "Our rarest colors, reserved for anyone who regularly chats with "
                "and helps welcome newcomers.\n\n"
                + listing
            )
            embed.set_footer(text="Tier colors run on recent welcoming activity — stay friendly to keep yours!")
        return embed

    def _build_view(self, guild: discord.Guild, cfg: dict, section: str):
        """Build the dropdown view for a section. Returns None if no roles are configured."""
        if section == "alerts":
            role_ids = cfg["alert_roles"][:MAX_ALERT_ROLES]
            roles = [r for r in (guild.get_role(rid) for rid in role_ids) if r]
            roles.sort(key=lambda r: r.name.casefold())
            # Remove-all stays pinned at the top; the rest is always A-Z.
            options = [discord.SelectOption(label="❌ Remove all alerts", value=REMOVE_VALUE)]
            options += [
                discord.SelectOption(label=role.name[:100], value=str(role.id))
                for role in roles
            ]
            if len(options) <= 1:
                return None
            select = ui.Select(
                custom_id=CUSTOM_IDS["alerts"],
                placeholder="Toggle your alert roles...",
                min_values=1,
                max_values=len(options),
                options=options,
            )
        else:
            role_ids = cfg["colors"][section]
            options = [discord.SelectOption(label="❌ Remove my color", value=REMOVE_VALUE)]
            options += [
                discord.SelectOption(label=guild.get_role(rid).name[:100], value=str(rid))
                for rid in role_ids if guild.get_role(rid)
            ]
            if len(options) <= 1:
                return None
            tier_num = section[1]
            placeholder = (
                "Choose your color" if section == "t1"
                else f"Pick your Tier {tier_num} color..."
            )
            select = ui.Select(
                custom_id=CUSTOM_IDS[section],
                placeholder=placeholder,
                min_values=1,
                max_values=1,
                options=options,
            )
        view = ui.View(timeout=None)
        view.add_item(select)
        return view

    # ─── Posting / updating the embeds ───

    def _post_order(self, cfg: dict) -> list:
        """Message keys in channel order, including configured banners."""
        order = []
        if cfg["banners"].get("alerts"):
            order.append("banner_alerts")
        order.append("alerts")
        if cfg["banners"].get("colors"):
            order.append("banner_colors")
        order += ["t1", "t2", "t3"]
        return order

    async def _fetch_banner_file(self, url: str):
        """Download a banner image so it posts as a clean full-width upload."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.read()
            if not data or len(data) > BANNER_MAX_BYTES:
                return None
            ext = os.path.splitext(urlparse(url).path)[1].lower()
            if ext not in BANNER_IMAGE_EXTS:
                ext = ".png"
            return discord.File(io.BytesIO(data), filename=f"banner{ext}")
        except Exception as e:
            log.warning(f"Banner download failed for {url}: {e}")
            return None

    async def _send_banner(self, channel, url: str):
        """Send a banner message; re-upload preferred, embed-image fallback."""
        file = await self._fetch_banner_file(url)
        if file:
            return await channel.send(file=file)
        embed = discord.Embed(color=0x2B2D31)
        embed.set_image(url=url)
        return await channel.send(embed=embed)

    async def post_embeds(self, interaction: discord.Interaction):
        guild = interaction.guild
        cfg = self._gcfg(guild.id)

        if not cfg["channel_id"]:
            return await interaction.followup.send("Set the channel first.", ephemeral=True)

        channel = guild.get_channel(cfg["channel_id"])
        if not channel:
            try:
                channel = await guild.fetch_channel(cfg["channel_id"])
            except discord.HTTPException:
                return await interaction.followup.send("Configured channel no longer exists.", ephemeral=True)

        order = self._post_order(cfg)

        # Edit in place only if the posted messages match the expected layout
        # exactly (same keys, all still present); otherwise repost fresh so the
        # order (banners included) stays correct.
        existing = {}
        can_edit = set(cfg["messages"].keys()) == set(order)
        if can_edit:
            for key in order:
                try:
                    existing[key] = await channel.fetch_message(cfg["messages"][key])
                except discord.HTTPException:
                    can_edit = False
                    break

        try:
            if can_edit:
                for key in order:
                    if key.startswith("banner_"):
                        bkey = key.split("_", 1)[1]
                        url = cfg["banners"][bkey]
                        if cfg["posted_banners"].get(bkey) == url:
                            continue  # banner unchanged
                        file = await self._fetch_banner_file(url)
                        if file:
                            await existing[key].edit(content=None, embed=None, attachments=[file])
                        else:
                            embed = discord.Embed(color=0x2B2D31)
                            embed.set_image(url=url)
                            await existing[key].edit(content=None, embed=embed, attachments=[])
                        cfg["posted_banners"][bkey] = url
                        self.save()
                    else:
                        await existing[key].edit(
                            embed=self._build_embed(guild, cfg, key),
                            view=self._build_view(guild, cfg, key),
                        )
                return await interaction.followup.send(
                    "Updated all messages in place.", ephemeral=True
                )

            # Repost: clear out everything we previously posted first
            for mid in list(cfg["messages"].values()):
                try:
                    old = await channel.fetch_message(mid)
                    await old.delete()
                except discord.HTTPException:
                    pass
            cfg["messages"] = {}
            cfg["posted_banners"] = {}

            for key in order:
                if key.startswith("banner_"):
                    bkey = key.split("_", 1)[1]
                    url = cfg["banners"][bkey]
                    msg = await self._send_banner(channel, url)
                    cfg["posted_banners"][bkey] = url
                else:
                    msg = await channel.send(
                        embed=self._build_embed(guild, cfg, key),
                        view=self._build_view(guild, cfg, key),
                    )
                cfg["messages"][key] = msg.id
            self.save()
            await interaction.followup.send(
                f"Posted {len(order)} messages to {channel.mention} in order "
                "(banners → Alert Roles → Tier 1 → Tier 2 → Tier 3).",
                ephemeral=True,
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "I'm missing permission to post/edit in that channel "
                "(need Send Messages + Attach Files).", ephemeral=True
            )

    # ─── Component interaction handling (survives restarts, same pattern as newcomer.py) ───

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get("custom_id", "")
        section = CUSTOM_ID_TO_SECTION.get(custom_id)
        if not section or not interaction.guild:
            return

        values = interaction.data.get("values") or []
        if not values:
            return

        try:
            await interaction.response.defer(ephemeral=True)
        except (discord.NotFound, discord.InteractionResponded) as e:
            # 10062 Unknown interaction: Discord's 3s ack window closed before our
            # defer landed (gateway/HTTP lag). The token is dead, so there's nothing
            # to reply to and nothing actionable to report — just note it and drop it.
            log.warning(f"AlertsAndColors: dropped stale interaction {custom_id}: {e}")
            return

        try:
            if section == "alerts":
                await self._handle_alert_toggle(interaction, values)
            else:
                await self._handle_color_pick(interaction, section, values[0])
        except discord.NotFound as e:
            log.warning(f"AlertsAndColors: interaction token expired mid-handler ({custom_id}): {e}")
        except Exception as e:
            log.error(f"AlertsAndColors interaction failed: {e}", exc_info=True)
            await self._report(f"on_interaction: {e}")
            try:
                await interaction.followup.send("Something went wrong — try again.", ephemeral=True)
            except Exception:
                pass

    async def _handle_alert_toggle(self, interaction: discord.Interaction, values: list):
        guild = interaction.guild
        member = interaction.user
        cfg = self._gcfg(guild.id)
        alert_ids = set(cfg["alert_roles"])

        # "Remove all" takes precedence over anything else selected with it
        if REMOVE_VALUE in values:
            held = [r for r in member.roles if r.id in alert_ids]
            if not held:
                return await interaction.followup.send(
                    "You don't have any alert roles to remove.", ephemeral=True
                )
            try:
                await member.remove_roles(*held, reason="Alert roles: remove all")
            except discord.Forbidden:
                return await interaction.followup.send(
                    "I can't manage those roles — my role may be positioned too low.",
                    ephemeral=True,
                )
            return await interaction.followup.send(
                "🔕 Removed all your alert roles: " + ", ".join(r.mention for r in held),
                ephemeral=True,
            )

        to_add, to_remove = [], []
        for v in values:
            try:
                rid = int(v)
            except ValueError:
                continue
            if rid not in alert_ids:
                continue
            role = guild.get_role(rid)
            if not role:
                continue
            if member.get_role(rid):
                to_remove.append(role)
            else:
                to_add.append(role)

        if not to_add and not to_remove:
            return await interaction.followup.send("Those roles no longer exist.", ephemeral=True)

        try:
            if to_remove:
                await member.remove_roles(*to_remove, reason="Alert roles toggle")
            if to_add:
                await member.add_roles(*to_add, reason="Alert roles toggle")
        except discord.Forbidden:
            return await interaction.followup.send(
                "I can't manage those roles — my role may be positioned too low.", ephemeral=True
            )

        parts = []
        if to_add:
            parts.append("➕ Added: " + ", ".join(r.mention for r in to_add))
        if to_remove:
            parts.append("➖ Removed: " + ", ".join(r.mention for r in to_remove))
        await interaction.followup.send("🔔 Alerts updated!\n" + "\n".join(parts), ephemeral=True)

    async def _handle_color_pick(self, interaction: discord.Interaction, tier_key: str, value: str):
        guild = interaction.guild
        member = interaction.user
        cfg = self._gcfg(guild.id)
        tier_num = int(tier_key[1])

        access = self._color_access_tier(member, cfg)
        if access < tier_num:
            gate_id = cfg["gates"].get(tier_key)
            gate = guild.get_role(gate_id) if gate_id else None
            if gate:
                return await interaction.followup.send(
                    f"You need the {gate.mention} role to use Tier {tier_num} colors.",
                    ephemeral=True,
                )
            return await interaction.followup.send(
                f"Tier {tier_num} isn't fully configured yet — ask an admin.", ephemeral=True
            )

        all_color_ids = self._all_color_ids(cfg)
        self._guard(member.id)

        try:
            if value == REMOVE_VALUE:
                worn = [r for r in member.roles if r.id in all_color_ids]
                # Clear all records too, so audits don't quietly re-apply a color
                # the member explicitly removed.
                self._clear_records(cfg, member.id)
                if worn:
                    await member.remove_roles(*worn, reason="Color removed by user")
                return await interaction.followup.send(
                    "Your name color has been removed.", ephemeral=True
                )

            try:
                rid = int(value)
            except ValueError:
                return await interaction.followup.send("Invalid selection.", ephemeral=True)
            if rid not in cfg["colors"][tier_key]:
                return await interaction.followup.send(
                    "That color is no longer available — the embed may be outdated.", ephemeral=True
                )
            role = guild.get_role(rid)
            if not role:
                return await interaction.followup.send("That color role no longer exists.", ephemeral=True)

            stale = [r for r in member.roles if r.id in all_color_ids and r.id != rid]
            if stale:
                await member.remove_roles(*stale, reason=f"Swapped to Tier {tier_num} color")
            if not member.get_role(rid):
                await member.add_roles(role, reason=f"Tier {tier_num} color picked")
            # Record only after the role ops succeeded
            self._set_record(cfg, member.id, tier_key, rid)
            await interaction.followup.send(
                f"Your Tier {tier_num} color is now {role.mention} — looking good! 🎨", ephemeral=True
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "I can't manage that role — my role may be positioned too low.", ephemeral=True
            )

    # ─── Manual role changes by admins (our own ops are guarded out) ───

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if before.roles == after.roles:
            return
        gid = str(after.guild.id)
        if gid not in self._config:
            return
        cfg = self._gcfg(after.guild.id)
        t2_gate = cfg["gates"].get("t2")
        t3_gate = cfg["gates"].get("t3")
        vip_ids = {
            rid for key in ("t2", "t3")
            for rid in (cfg["vip_roles"].get(key) or [])
        }
        if not t2_gate and not t3_gate and not vip_ids:
            return

        before_ids = {r.id for r in before.roles}
        after_ids = {r.id for r in after.roles}
        gained = after_ids - before_ids
        lost = before_ids - after_ids
        watched = {rid for rid in (t2_gate, t3_gate) if rid}
        touched_vip = bool(vip_ids & (gained | lost))
        if not (watched & (gained | lost)) and not touched_vip:
            return

        if self._is_guarded(after.id):
            return  # this change was made by set_member_tier / a color handler

        try:
            # A VIP role coming or going changes the member's effective points,
            # so recompute their tier straight away instead of waiting an hour.
            # newcomer.py owns the points DB, so it does the math.
            if touched_vip:
                newcomer = self.bot.get_cog("IntroCog")
                if newcomer:
                    await newcomer.resync_member(after)
                return  # resync_member already reconciled gates and colors

            # Manual promotion by an admin → still send the invite DM
            if t3_gate and t3_gate in gained:
                await self._send_tier_dm_once(after, "t3")
            elif t2_gate and t2_gate in gained:
                await self._send_tier_dm_once(after, "t2")

            # Manual gate removal → strip too-high colors and restore their
            # last recorded color for the tier they now sit at.
            if watched & lost:
                await self._reconcile_colors(after)
        except Exception as e:
            log.error(f"AlertsAndColors member update failed: {e}", exc_info=True)
            await self._report(f"on_member_update: {e}")

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Forget color records for members who leave."""
        cfg = self._config.get(str(member.guild.id))
        if cfg and str(member.id) in cfg.get("last_colors", {}):
            del cfg["last_colors"][str(member.id)]
            self.save()

    # ─── Hourly self-heal: converge colors with gates even after missed events ───

    @tasks.loop(hours=1)
    async def audit_task(self):
        try:
            for gid_str in list(self._config.keys()):
                guild = self.bot.get_guild(int(gid_str))
                if not guild:
                    continue
                cfg = self._gcfg(guild.id)
                seen = set()

                # 1) Everyone currently wearing a tracked color
                for t in ("t1", "t2", "t3"):
                    for rid in list(cfg["colors"][t]):
                        role = guild.get_role(rid)
                        if not role:
                            continue
                        for member in list(role.members):
                            if member.bot or member.id in seen:
                                continue
                            seen.add(member.id)
                            self._guard(member.id)
                            if await self._reconcile_colors(member):
                                await asyncio.sleep(AUDIT_SLEEP)

                # 2) Members with a saved record but no color (missed restore)
                for uid_str in list(cfg["last_colors"].keys()):
                    uid = int(uid_str)
                    if uid in seen:
                        continue
                    member = guild.get_member(uid)
                    if not member or member.bot:
                        continue
                    self._guard(member.id)
                    if await self._reconcile_colors(member):
                        await asyncio.sleep(AUDIT_SLEEP)
        except Exception as e:
            log.error(f"AlertsAndColors color audit failed: {e}", exc_info=True)
            await self._report(f"audit_task: {e}")

    @audit_task.before_loop
    async def before_audit_task(self):
        await self.bot.wait_until_ready()

    # ─── Admin panel ───

    @app_commands.command(
        name="alerts_colors_panel",
        description="Admin: Configure the Alert Roles + Tier Color embeds",
    )
    async def alerts_colors_panel(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("Admin access only.", ephemeral=True)

        guild = interaction.guild
        cfg = self._gcfg(guild.id)

        def role_txt(rid):
            role = guild.get_role(rid) if rid else None
            return role.mention if role else "*not set*"

        def vip_txt(tier_key):
            ids = cfg["vip_roles"].get(tier_key) or []
            mentions = [guild.get_role(rid).mention for rid in ids if guild.get_role(rid)]
            return ", ".join(mentions) if mentions else "*none*"

        channel = guild.get_channel(cfg["channel_id"]) if cfg["channel_id"] else None
        posted = all(cfg["messages"].get(s) for s in SECTION_ORDER)
        embed = discord.Embed(
            title="Alerts & Colors — Admin Panel",
            color=0x5865F2,
            description=(
                f"**Channel:** {channel.mention if channel else '*not set*'}\n"
                f"**Embeds posted:** {'yes' if posted else 'no'}\n\n"
                f"**Tier 1:** open to everyone (no role required)\n"
                f"**Tier 2 role:** {role_txt(cfg['gates'].get('t2'))}\n"
                f"**Tier 3 role:** {role_txt(cfg['gates'].get('t3'))}\n\n"
                f"**Tier 2 VIP:** {vip_txt('t2')}\n"
                f"**Tier 3 VIP:** {vip_txt('t3')}\n"
                "*VIP roles are worth their tier's point threshold, permanently.*\n\n"
                f"**Alerts banner:** {'set' if cfg['banners'].get('alerts') else '*none*'}\n"
                f"**Colors banner:** {'set' if cfg['banners'].get('colors') else '*none*'}\n\n"
                f"**Alert roles:** {len(cfg['alert_roles'])}\n"
                f"**Tier 1 colors:** {len(cfg['colors']['t1'])}\n"
                f"**Tier 2 colors:** {len(cfg['colors']['t2'])}\n"
                f"**Tier 3 colors:** {len(cfg['colors']['t3'])}\n\n"
                "After changing roles, hit **Post / Update Embeds** to refresh the channel."
            ),
        )
        await interaction.response.send_message(embed=embed, view=PanelView(self), ephemeral=True)


# ============================================================
#  Admin panel views
# ============================================================

class ConfigView(ui.View):
    """Base for admin config views: long timeout + loud, visible errors."""

    def __init__(self, cog: AlertsAndColors):
        super().__init__(timeout=CONFIG_VIEW_TIMEOUT)
        self.cog = cog

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        log.error(f"Panel error in {type(self).__name__}/{item}: {error}", exc_info=error)
        await self.cog._report(f"panel {type(self).__name__}: {error}")
        msg = f"Panel error: {error}"
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass


class PanelView(ConfigView):

    @ui.button(label="Set Channel", style=discord.ButtonStyle.primary, row=0)
    async def channel_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "Select the channel for the 4 embeds:", view=SetChannelView(self.cog), ephemeral=True
        )

    @ui.button(label="Set Gate / VIP Roles", style=discord.ButtonStyle.primary, row=0)
    async def gates_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "**Tier roles** are awarded by points. **VIP roles** are permanent point "
            "floors — holding one is worth that tier's threshold, and any points the "
            "member earns stack on top toward the next tier.\n"
            "Selecting nothing in a VIP menu clears it.",
            view=GateRolesView(self.cog),
            ephemeral=True,
        )

    async def _open_list_editor(self, interaction: discord.Interaction, list_key: str):
        await interaction.response.send_message(
            self.cog.role_list_summary(interaction.guild, list_key),
            view=RoleListView(self.cog, list_key),
            ephemeral=True,
        )

    @ui.button(label="Edit Alert Roles", style=discord.ButtonStyle.primary, row=0)
    async def alerts_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self._open_list_editor(interaction, "alerts")

    @ui.button(label="Edit Tier 1 Colors", style=discord.ButtonStyle.secondary, row=1)
    async def t1_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self._open_list_editor(interaction, "t1")

    @ui.button(label="Edit Tier 2 Colors", style=discord.ButtonStyle.secondary, row=1)
    async def t2_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self._open_list_editor(interaction, "t2")

    @ui.button(label="Edit Tier 3 Colors", style=discord.ButtonStyle.secondary, row=1)
    async def t3_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self._open_list_editor(interaction, "t3")

    @ui.button(label="Set Banners", style=discord.ButtonStyle.secondary, row=2)
    async def banners_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(BannerModal(self.cog, interaction.guild_id))

    @ui.button(label="Post / Update Embeds", style=discord.ButtonStyle.success, row=2)
    async def post_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.post_embeds(interaction)

    @ui.button(label="Run Tier Audit", style=discord.ButtonStyle.secondary, row=3)
    async def audit_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        newcomer = self.cog.bot.get_cog("IntroCog")
        if not newcomer:
            return await interaction.followup.send(
                "The newcomer cog isn't loaded — it owns the points the audit needs.",
                ephemeral=True,
            )
        try:
            report = await newcomer.run_tier_audit(interaction.guild, dry_run=True)
        except Exception as e:
            log.error(f"Tier audit dry run failed: {e}", exc_info=True)
            await self.cog._report(f"tier audit dry run: {e}")
            return await interaction.followup.send(f"Audit failed: {e}", ephemeral=True)

        if report is None:
            return await interaction.followup.send(
                "Point thresholds aren't configured yet (newcomer panel → Points/Tiers).",
                ephemeral=True,
            )
        if not report.changes:
            return await interaction.followup.send(
                f"✅ Nothing to fix — all **{report.tracked}** tracked member(s) hold the "
                "right tier role.",
                ephemeral=True,
            )
        await interaction.followup.send(
            report.summary(), view=TierAuditApplyView(self.cog), ephemeral=True
        )


class TierAuditApplyView(ConfigView):
    """Confirmation step for the tier audit's dry run."""

    @ui.button(label="Apply Changes", style=discord.ButtonStyle.danger)
    async def apply_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Running the audit…", view=None)
        newcomer = self.cog.bot.get_cog("IntroCog")
        if not newcomer:
            return await interaction.followup.send("The newcomer cog isn't loaded.", ephemeral=True)
        report = await newcomer.run_tier_audit(interaction.guild, dry_run=False)
        if report is None:
            return await interaction.followup.send(
                "Point thresholds aren't configured yet.", ephemeral=True
            )
        await interaction.followup.send(
            f"✅ Audit complete — updated **{report.applied}** of "
            f"**{len(report.changes)}** member(s) needing a change "
            f"(**{report.tracked}** tracked)." +
            (f"\n⚠️ {report.failed} failed — check the logs." if report.failed else ""),
            ephemeral=True,
        )

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Audit cancelled — nothing changed.", view=None)


class BannerModal(ui.Modal, title="Section Banners"):
    """Header images posted above the Alerts section and the Colors section."""

    alerts_url = ui.TextInput(
        label="Alerts banner image URL",
        required=False,
        placeholder="https://... (leave empty for no banner)",
        max_length=500,
    )
    colors_url = ui.TextInput(
        label="Colors banner image URL (all 3 tiers)",
        required=False,
        placeholder="https://... (leave empty for no banner)",
        max_length=500,
    )

    def __init__(self, cog: AlertsAndColors, guild_id: int):
        super().__init__()
        self.cog = cog
        banners = cog._gcfg(guild_id)["banners"]
        self.alerts_url.default = banners.get("alerts") or ""
        self.colors_url.default = banners.get("colors") or ""

    async def on_submit(self, interaction: discord.Interaction):
        urls = {}
        for key, field in (("alerts", self.alerts_url), ("colors", self.colors_url)):
            value = field.value.strip()
            if value and not value.lower().startswith(("http://", "https://")):
                return await interaction.response.send_message(
                    f"The {key} banner must be a direct http(s) image link.", ephemeral=True
                )
            urls[key] = value or None

        cfg = self.cog._gcfg(interaction.guild_id)
        cfg["banners"] = urls
        self.cog.save()
        await interaction.response.send_message(
            "Banners saved — hit **Post / Update Embeds** to apply them.\n"
            f"Alerts: {urls['alerts'] or '*none*'}\nColors: {urls['colors'] or '*none*'}",
            ephemeral=True,
        )


class SetChannelView(ConfigView):

    @ui.select(cls=ui.ChannelSelect, placeholder="Select channel...",
               channel_types=[discord.ChannelType.text], min_values=1, max_values=1)
    async def sel(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        cfg = self.cog._gcfg(interaction.guild_id)
        cfg["channel_id"] = select.values[0].id
        # Channel changed — old message IDs no longer apply
        cfg["messages"] = {}
        self.cog.save()
        await interaction.response.send_message(
            f"Channel set to <#{select.values[0].id}>. Use **Post / Update Embeds** to publish.",
            ephemeral=True,
        )


class GateRolesView(ConfigView):
    """Tier gate roles + VIP roles (shared with the newcomer panel)."""

    async def _set(self, interaction, tier_key, role, label):
        self.cog.set_gate(interaction.guild_id, tier_key, role.id)
        await interaction.response.send_message(f"{label} set to {role.mention}.", ephemeral=True)

    async def _set_vip(self, interaction, tier_key, roles, label):
        cfg = self.cog._gcfg(interaction.guild_id)
        gates = {cfg["gates"].get(k) for k in ("t2", "t3")}
        chosen = [r for r in roles if r.id not in gates]
        rejected = [r for r in roles if r.id in gates]

        cfg["vip_roles"][tier_key] = [r.id for r in chosen]
        self.cog.save()

        if chosen:
            msg = f"{label} set to " + ", ".join(r.mention for r in chosen) + "."
        else:
            msg = f"{label} cleared."
        if rejected:
            # A gate role can't also be a VIP role: it would grant the points
            # that justify itself, so nobody could ever lose it.
            msg += ("\n⚠️ Skipped " + ", ".join(r.mention for r in rejected)
                    + " — a tier role can't also be its own VIP role.")
        msg += "\nUse **Run Tier Audit** on the panel to apply this to current holders."
        await interaction.response.send_message(msg, ephemeral=True)

    @ui.select(cls=ui.RoleSelect, placeholder="Tier 2 role (awarded by newcomer points)",
               min_values=1, max_values=1, row=0)
    async def g2(self, interaction, select):
        await self._set(interaction, "t2", select.values[0], "Tier 2 role")

    @ui.select(cls=ui.RoleSelect, placeholder="Tier 3 role (awarded by newcomer points)",
               min_values=1, max_values=1, row=1)
    async def g3(self, interaction, select):
        await self._set(interaction, "t3", select.values[0], "Tier 3 role")

    @ui.select(cls=ui.RoleSelect, placeholder="Tier 2 VIP role(s) — worth the Tier 2 threshold",
               min_values=0, max_values=10, row=2)
    async def vip2(self, interaction, select):
        await self._set_vip(interaction, "t2", select.values, "Tier 2 VIP role(s)")

    @ui.select(cls=ui.RoleSelect, placeholder="Tier 3 VIP role(s) — worth the Tier 3 threshold",
               min_values=0, max_values=10, row=3)
    async def vip3(self, interaction, select):
        await self._set_vip(interaction, "t3", select.values, "Tier 3 VIP role(s)")


class RoleListView(ConfigView):
    """Additive add/remove editor for a configured role list.

    list_key is "alerts" or a color tier key ("t1"/"t2"/"t3"). Additions merge
    into the existing list (capped), removals only take out what's picked —
    the list is never replaced wholesale, so admins can configure in passes.
    """

    def __init__(self, cog: AlertsAndColors, list_key: str):
        super().__init__(cog)
        self.list_key = list_key
        self.cap = MAX_ALERT_ROLES if list_key == "alerts" else MAX_COLORS_PER_TIER
        label = "alert roles" if list_key == "alerts" else f"Tier {list_key[1]} color roles"

        add_sel = ui.RoleSelect(placeholder=f"➕ Add {label}...",
                                min_values=1, max_values=25, row=0)
        add_sel.callback = self._on_add
        rem_sel = ui.RoleSelect(placeholder=f"➖ Remove {label}...",
                                min_values=1, max_values=25, row=1)
        rem_sel.callback = self._on_remove
        self.add_sel = add_sel
        self.rem_sel = rem_sel
        self.add_item(add_sel)
        self.add_item(rem_sel)

    def _get_list(self, guild_id: int) -> list:
        cfg = self.cog._gcfg(guild_id)
        return cfg["alert_roles"] if self.list_key == "alerts" else cfg["colors"][self.list_key]

    def _set_list(self, guild_id: int, ids: list):
        cfg = self.cog._gcfg(guild_id)
        if self.list_key == "alerts":
            cfg["alert_roles"] = ids
        else:
            cfg["colors"][self.list_key] = ids
        self.cog.save()

    async def _on_add(self, interaction: discord.Interaction):
        guild = interaction.guild
        merged_ids = set(self._get_list(guild.id))
        skipped = []
        for role in self.add_sel.values:
            if role.id in merged_ids:
                continue
            if len(merged_ids) >= self.cap:
                skipped.append(role)
                continue
            merged_ids.add(role.id)

        # Keep the list ordered like the guild role list (highest first)
        roles = sorted(
            (guild.get_role(rid) for rid in merged_ids if guild.get_role(rid)),
            key=lambda r: r.position, reverse=True,
        )
        self._set_list(guild.id, [r.id for r in roles])

        await interaction.response.edit_message(
            content=self.cog.role_list_summary(guild, self.list_key), view=self
        )
        if skipped:
            await interaction.followup.send(
                f"⚠️ List is capped at {self.cap} — couldn't add: "
                + ", ".join(r.mention for r in skipped),
                ephemeral=True,
            )

    async def _on_remove(self, interaction: discord.Interaction):
        guild = interaction.guild
        current = self._get_list(guild.id)
        remove_ids = {r.id for r in self.rem_sel.values if r.id in current}
        if not remove_ids:
            return await interaction.response.send_message(
                "None of those roles are on the list.", ephemeral=True
            )
        self._set_list(guild.id, [rid for rid in current if rid not in remove_ids])

        await interaction.response.edit_message(
            content=self.cog.role_list_summary(guild, self.list_key), view=self
        )

        # For color tiers, also take a delisted color off anyone wearing it so
        # nobody keeps an untracked, unselectable color.
        if self.list_key != "alerts":
            stripped = 0
            for rid in remove_ids:
                role = guild.get_role(rid)
                if not role:
                    continue
                for m in list(role.members):
                    self.cog._guard(m.id)
                    try:
                        await m.remove_roles(role, reason="Color removed from tier list")
                        stripped += 1
                        await asyncio.sleep(AUDIT_SLEEP)
                    except discord.HTTPException as e:
                        log.error(f"Failed stripping delisted color {role.name} from {m.name}: {e}")
                        await self.cog._report(f"delist strip {role.name} -> {m.name}: {e}")
            if stripped:
                await interaction.followup.send(
                    f"Also removed the delisted color(s) from **{stripped}** member(s).",
                    ephemeral=True,
                )


async def setup(bot: commands.Bot):
    await bot.add_cog(AlertsAndColors(bot))
