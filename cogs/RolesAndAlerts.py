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
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

log = logging.getLogger(__name__)

# Paths for the new unified configuration
BASE_DIR = Path.cwd() # Or however the bot root is defined; typically cwd when running main.py
CONFIG_DIR = BASE_DIR / "data" / "roles_and_alerts_config"
CONFIG_PATH = CONFIG_DIR / "config.json"
LEGACY_CONFIG_PATH = BASE_DIR / "alerts_colors_config.json"
DB_PATH = CONFIG_DIR / "commands.db"

# Limits & Settings
MAX_ALERT_ROLES = 24
MAX_COLORS_PER_TIER = 24
REMOVE_VALUE = "remove"
MUTATION_GUARD_SECONDS = 15
DM_DEDUP_SECONDS = 600
AUDIT_SLEEP = 0.3
ROLE_SHUFFLE_SLEEP = 0.5 # rate limit safety for role shuffle

# Section configurations
SECTION_META = {
    "alerts": {"title": "🔔 Alert Roles", "color": 0x5865F2},
    "t1": {"title": "🎨 Name Colors", "color": 0x2ECC71},
    "t2": {"title": "✨ Name Colors — Tier 2", "color": 0xF1C40F},
    "t3": {"title": "🌟 Name Colors — Tier 3", "color": 0x9B59B6},
}

SECTION_ORDER = ("alerts", "t1", "t2", "t3")
BANNER_IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".gif", ".webp")
BANNER_MAX_BYTES = 8 * 1024 * 1024

CUSTOM_IDS = {
    "alerts": "alertscolors:alerts",
    "t1": "alertscolors:t1",
    "t2": "alertscolors:t2",
    "t3": "alertscolors:t3",
}

LEGACY_CUSTOM_IDS = {
    "colorroles:alerts": "alerts",
    "colorroles:t1": "t1",
    "colorroles:t2": "t2",
    "colorroles:t3": "t3",
}
CUSTOM_ID_TO_SECTION = {v: k for k, v in CUSTOM_IDS.items()}
CUSTOM_ID_TO_SECTION.update(LEGACY_CUSTOM_IDS)


def _load_config() -> dict:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Auto-migrate legacy config if present and new config doesn't exist
    if not CONFIG_PATH.exists() and LEGACY_CONFIG_PATH.exists():
        try:
            import shutil
            shutil.copy2(LEGACY_CONFIG_PATH, CONFIG_PATH)
            log.info(f"Migrated config from {LEGACY_CONFIG_PATH.name} to {CONFIG_PATH}")
        except Exception as e:
            log.error(f"Failed to migrate legacy config: {e}")

    try:
        if CONFIG_PATH.exists():
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        log.error(f"{CONFIG_PATH.name} is corrupt — starting from an empty config.")
    except Exception as e:
        log.error(f"Error reading config: {e}")
    return {}


def _save_config(data: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = f"{CONFIG_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp, CONFIG_PATH)


class RolesAndAlerts(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._config = _load_config()
        self._locks: dict[int, asyncio.Lock] = {}
        self._mutating: dict[int, float] = {}
        self._recent_dm: dict[tuple, float] = {}
        
        self._init_db()
        self.audit_task.start()
        self.role_shuffle_loop.start()

    def cog_unload(self):
        self.audit_task.cancel()
        self.role_shuffle_loop.cancel()

    def _init_db(self):
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS role_shuffle_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT NOT NULL,
                trigger_roles TEXT NOT NULL,
                gain_role TEXT NOT NULL,
                lose_role TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                logs TEXT
            )
        ''')
        conn.commit()
        conn.close()

    # ─── Role Shuffle Task Polling ───

    @tasks.loop(seconds=5)
    async def role_shuffle_loop(self):
        """Polls the SQLite database for pending role shuffle tasks and executes them."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT id, guild_id, trigger_roles, gain_role, lose_role FROM role_shuffle_tasks WHERE status = 'pending' LIMIT 1")
            row = c.fetchone()
            
            if not row:
                conn.close()
                return

            task_id, guild_id_str, trigger_roles_str, gain_role_str, lose_role_str = row
            
            # Mark as running
            c.execute("UPDATE role_shuffle_tasks SET status = 'running' WHERE id = ?", (task_id,))
            conn.commit()
            
            guild_id = int(guild_id_str)
            try:
                trigger_roles = [int(rid) for rid in json.loads(trigger_roles_str)]
            except (json.JSONDecodeError, TypeError, ValueError):
                trigger_roles = []
                
            gain_role_id = int(gain_role_str) if gain_role_str else None
            lose_role_id = int(lose_role_str) if lose_role_str else None

            guild = self.bot.get_guild(guild_id)
            if not guild:
                self._update_task_status(c, task_id, "error", "Guild not found.")
                conn.commit()
                conn.close()
                return

            if not guild.chunked:
                await guild.chunk()

            gain_role = guild.get_role(gain_role_id) if gain_role_id else None
            lose_role = guild.get_role(lose_role_id) if lose_role_id else None

            if not gain_role:
                self._update_task_status(c, task_id, "error", "Gain role not found.")
                conn.commit()
                conn.close()
                return

            affected = 0
            errors = []
            trigger_ids = set(trigger_roles)

            for member in guild.members:
                member_role_ids = {r.id for r in member.roles}
                if not trigger_ids.issubset(member_role_ids):
                    continue

                try:
                    # Execute the shuffle
                    await member.add_roles(gain_role, reason="Role Management shuffle (Dashboard)")
                    if lose_role:
                        await member.remove_roles(lose_role, reason="Role Management shuffle (Dashboard)")
                    affected += 1
                except discord.Forbidden:
                    errors.append(f"Missing perms for {member.id}")
                except discord.HTTPException as e:
                    errors.append(f"Error for {member.id}: {e}")
                
                # Rate limit safety pause
                await asyncio.sleep(ROLE_SHUFFLE_SLEEP)

            # Build logs summary
            logs = f"Affected {affected} members."
            if errors:
                logs += f"\\nErrors ({len(errors)}):\\n" + "\\n".join(errors[:10])

            self._update_task_status(c, task_id, "complete", logs)
            conn.commit()
            conn.close()
            
        except Exception as e:
            log.error(f"Error in role_shuffle_loop: {e}")
            try:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("UPDATE role_shuffle_tasks SET status = 'error', logs = ? WHERE status = 'running'", (str(e),))
                conn.commit()
                conn.close()
            except Exception as inner_e:
                log.error(f"Failed to update task to error status: {inner_e}")

    def _update_task_status(self, cursor, task_id: int, status: str, logs: str):
        cursor.execute("UPDATE role_shuffle_tasks SET status = ?, logs = ? WHERE id = ?", (status, logs, task_id))

    @role_shuffle_loop.before_loop
    async def before_role_shuffle_loop(self):
        await self.bot.wait_until_ready()

    # ─── Alerts & Colors Config Helpers ───

    def _gcfg(self, guild_id: int) -> dict:
        g = self._config.setdefault(str(guild_id), {})
        g.setdefault("channel_id", None)
        g.setdefault("alert_roles", [])
        g.setdefault("gates", {"t1": None, "t2": None, "t3": None})
        vip = g.setdefault("vip_roles", {"t2": [], "t3": []})
        vip.setdefault("t2", [])
        vip.setdefault("t3", [])
        g.setdefault("colors", {"t1": [], "t2": [], "t3": []})
        g.setdefault("messages", {})
        g.setdefault("last_colors", {})
        g.setdefault("banners", {"alerts": None, "colors": None})
        g.setdefault("posted_banners", {})
        return g

    def save(self):
        _save_config(self._config)

    def set_gate(self, guild_id: int, tier_key: str, role_id: int):
        cfg = self._gcfg(guild_id)
        cfg["gates"][tier_key] = role_id
        for key in ("t2", "t3"):
            ids = cfg["vip_roles"].get(key) or []
            if role_id in ids:
                cfg["vip_roles"][key] = [rid for rid in ids if rid != role_id]
        self.save()

    def get_gate(self, guild_id: int, tier_key: str):
        return self._gcfg(guild_id)["gates"].get(tier_key)

    def get_vip_role_ids(self, guild_id: int, tier_key: str) -> list:
        return list(self._gcfg(guild_id)["vip_roles"].get(tier_key) or [])

    def get_vip_tier(self, member: discord.Member) -> int:
        cfg = self._gcfg(member.guild.id)
        for tier in (3, 2):
            for rid in cfg["vip_roles"].get(f"t{tier}") or []:
                if member.get_role(rid):
                    return tier
        return 0

    def get_member_tier(self, member: discord.Member) -> int:
        return self._reward_tier(member, self._gcfg(member.guild.id))

    def iter_vip_members(self, guild: discord.Guild):
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
        for tier in (3, 2):
            rid = cfg["gates"].get(f"t{tier}")
            if rid and member.get_role(rid):
                return tier
        return 1

    def _color_access_tier(self, member: discord.Member, cfg: dict) -> int:
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
        self._mutating[member_id] = time.monotonic() + MUTATION_GUARD_SECONDS

    def _is_guarded(self, member_id: int) -> bool:
        expiry = self._mutating.get(member_id)
        if expiry is None:
            return False
        if expiry < time.monotonic():
            del self._mutating[member_id]
            return False
        return True

    # ─── Tier Enforcement ───

    async def set_member_tier(self, member: discord.Member, target: int, *, allow_dm: bool = True) -> bool:
        guild = member.guild
        cfg = self._gcfg(guild.id)

        desired = None
        if target >= 2:
            rid = cfg["gates"].get(f"t{min(target, 3)}")
            desired = guild.get_role(rid) if rid else None
            if not desired:
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

            if desired and not member.get_role(desired.id):
                try:
                    await member.add_roles(desired, reason=f"Newcomer tier: reached Tier {target}")
                    log.info(f"Granted {desired.name} to {member.name}")
                    changed = True
                except discord.HTTPException as e:
                    log.error(f"Failed to grant {desired.name} to {member.name}: {e}")
                    await self._report(f"set_member_tier: grant {desired.name} -> {member.name}: {e}")
                    return changed

            stale_gates = [r for r in gate_roles if r and r != desired and member.get_role(r.id)]
            if stale_gates:
                try:
                    await member.remove_roles(*stale_gates, reason="Newcomer tier changed")
                    log.info(f"Removed {', '.join(r.name for r in stale_gates)} from {member.name}")
                    changed = True
                except discord.HTTPException as e:
                    log.error(f"Failed to remove stale gate(s) from {member.name}: {e}")
                    await self._report(f"set_member_tier: remove stale gates from {member.name}: {e}")

            if target < current:
                if await self._reconcile_colors(member, tier=target):
                    changed = True
            elif target > current:
                if await self._restore_recorded_color(member, cfg, f"t{target}"):
                    changed = True
                if allow_dm:
                    await self._send_tier_dm_once(member, f"t{target}")

            return changed

    async def _restore_recorded_color(self, member: discord.Member, cfg: dict, tier_key: str) -> bool:
        rid = self._records(cfg, member.id).get(tier_key)
        if not rid:
            return False
        role = member.guild.get_role(rid)
        if not role or rid not in cfg["colors"][tier_key]:
            self._drop_record(cfg, member.id, tier_key)
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
        if len(keep) > 1:
            to_remove += [role for _, role in keep[1:]]
            keep = keep[:1]

        changed = False
        try:
            if to_remove:
                await member.remove_roles(*to_remove, reason="Color tier reconciliation")
                log.info(f"Reconcile: removed {', '.join(r.name for r in to_remove)} from {member.name}")
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
            if hasattr(self.bot, "error_reporter"):
                await self.bot.error_reporter.report("RolesAndAlerts", msg)
        except Exception:
            pass

    # ─── Promotion DM ───

    async def _send_tier_dm_once(self, member: discord.Member, tier_key: str):
        now = time.monotonic()
        key = (member.id, tier_key)
        last = self._recent_dm.get(key)
        if last and now - last < DM_DEDUP_SECONDS:
            return
        self._recent_dm[key] = now
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
                f"Hey {member.display_name}! 👋\\n\\n"
                f"You've been making our newcomers feel right at home, and it hasn't gone "
                f"unnoticed — you've just earned the **{role_name}** role! 🎉\\n\\n"
                "As a thank-you, you now have access to the exclusive **Tier 2 name colors**. "
                "Head over and pick whichever one you like:"
                + (f"\\n{link}\\n\\n" if link else "\\n\\n")
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
                f"Hey {member.display_name}! 🌟\\n\\n"
                f"Incredible — you've earned the **{role_name}** role, our highest welcoming "
                "honor! You're a big part of what makes this server feel like home. 🏡\\n\\n"
                "You now have access to the exclusive **Tier 3 name colors** — the rarest of "
                "the bunch. Pick your favorite here:"
                + (f"\\n{link}\\n\\n" if link else "\\n\\n")
                + outro
            )

        try:
            await member.send(text)
            log.info(f"Sent Tier {tier_key[1]} color invite DM to {member.name}")
        except (discord.Forbidden, discord.HTTPException) as e:
            log.info(f"Could not DM {member.name} their tier color invite: {e}")

    # ─── Component interaction handling (Self-serve user interface) ───

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
            log.warning(f"RolesAndAlerts: dropped stale interaction {custom_id}: {e}")
            return

        val = values[0]
        member = interaction.guild.get_member(interaction.user.id)
        if not member:
            return

        cfg = self._gcfg(interaction.guild.id)

        # ─── ALERTS ───
        if section == "alerts":
            roles_to_add = []
            roles_to_remove = []
            if val == REMOVE_VALUE:
                for rid in cfg["alert_roles"]:
                    role = interaction.guild.get_role(rid)
                    if role and member.get_role(rid):
                        roles_to_remove.append(role)
                msg = "Removed all your alert roles." if roles_to_remove else "You don't have any alert roles."
            else:
                role = interaction.guild.get_role(int(val))
                if role:
                    if member.get_role(role.id):
                        roles_to_remove.append(role)
                        msg = f"Removed the **{role.name}** alert."
                    else:
                        roles_to_add.append(role)
                        msg = f"Added the **{role.name}** alert."
                else:
                    msg = "That role no longer exists."

            try:
                if roles_to_add:
                    await member.add_roles(*roles_to_add, reason="Self-serve alert roles")
                if roles_to_remove:
                    await member.remove_roles(*roles_to_remove, reason="Self-serve alert roles")
                await interaction.followup.send(msg, ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send("I'm missing permission to manage roles.", ephemeral=True)
            except discord.HTTPException:
                await interaction.followup.send("An error occurred while updating your roles.", ephemeral=True)
            return

        # ─── COLORS ───
        tier_num = int(section[1])
        if tier_num > self._color_access_tier(member, cfg):
            await interaction.followup.send(
                "You don't have access to this tier's colors yet.", ephemeral=True
            )
            return

        if val == REMOVE_VALUE:
            worn = [interaction.guild.get_role(r) for r in cfg["colors"][section] if member.get_role(r)]
            if worn:
                try:
                    await member.remove_roles(*worn, reason=f"Removed Tier {tier_num} color")
                    self._drop_record(cfg, member.id, section)
                    await interaction.followup.send(f"Removed your Tier {tier_num} color.", ephemeral=True)
                except discord.HTTPException:
                    await interaction.followup.send("An error occurred while removing your color.", ephemeral=True)
            else:
                await interaction.followup.send("You don't have a color from this tier to remove.", ephemeral=True)
            return

        role = interaction.guild.get_role(int(val))
        if not role:
            await interaction.followup.send("That color no longer exists.", ephemeral=True)
            return

        stale = []
        for tier in ("t1", "t2", "t3"):
            for r in cfg["colors"][tier]:
                if r != role.id and member.get_role(r):
                    stale.append(interaction.guild.get_role(r))

        try:
            if stale:
                await member.remove_roles(*stale, reason="Switched tier colors")
            await member.add_roles(role, reason="Chose tier color")
            self._set_record(cfg, member.id, section, role.id)
            await interaction.followup.send(f"Your color is now **{role.name}**.", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("I'm missing permission to manage roles.", ephemeral=True)
        except discord.HTTPException:
            await interaction.followup.send("An error occurred while updating your colors.", ephemeral=True)

    @tasks.loop(hours=1)
    async def audit_task(self):
        for guild in self.bot.guilds:
            cfg = self._gcfg(guild.id)
            tracked_color_ids = self._all_color_ids(cfg)
            if not tracked_color_ids:
                continue

            try:
                for member in guild.members:
                    if member.bot:
                        continue
                        
                    has_tracked_color = any(r.id in tracked_color_ids for r in member.roles)
                    has_record = str(member.id) in cfg["last_colors"]
                    
                    if has_tracked_color or has_record:
                        if self._is_guarded(member.id):
                            continue
                        
                        lock = self._locks.setdefault(member.id, asyncio.Lock())
                        if lock.locked():
                            continue
                            
                        await self._reconcile_colors(member)
                        await asyncio.sleep(AUDIT_SLEEP)
            except Exception as e:
                log.error(f"Audit task failed for guild {guild.id}: {e}")

    @audit_task.before_loop
    async def before_audit(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(RolesAndAlerts(bot))
