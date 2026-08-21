import discord
from discord import ui
from discord.ext import commands, tasks
import aiosqlite
import asyncio
import copy
import json
import logging
import io
import os
import random
import textwrap
import time
import datetime
from dataclasses import dataclass
from typing import Any, Dict
from PIL import Image, ImageDraw, ImageFont

from utils.module_config_sync import ConfigSyncAgent
from cogs.onboarding_config_sync import OnboardingConfigSync

# --- CONFIGURATION ---
DB_NAME = "intro_system.db"
# Per-guild greeting config (the old welcome cog's store). The intro/points
# system above is global; only the join greeting and its game→LFG mappings are
# per-guild, so they keep their own small JSON file.
WELCOME_CONFIG_FILE = "welcome_config.json"
# How long after the first game role to wait for more before sending the welcome.
ROLE_WAIT_SECONDS = 30
# Longest to wait for an onboarding signal before sending a generic welcome.
MAX_ONBOARDING_WAIT_SECONDS = 300
# How long an intro discussion thread lives before the bot deletes it (with its
# Lore Drop banner). The Q&A post in the intro channel is kept as the record.
INTRO_THREAD_LIFETIME_DAYS = 7
# Level a newcomer must reach before the newcomer role is swapped for the member
# role. Levels come from the economy cog, whose 60s per-user XP cooldown is what
# makes this un-spammable — a raw message count is not. Overridable in the panel
# via the member_role_level setting; the swap is skipped until both roles are
# set. Voice time counts toward it, which is deliberate: voice-only members
# would otherwise stay newcomers forever and keep drawing inactivity nudges.
DEFAULT_MEMBER_ROLE_LEVEL = 2
# Font paths - Noto Sans for broad Unicode coverage
FONT_PATH_BOLD = "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf"
FONT_PATH_REG = "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"

# Fallback
if not os.path.exists(FONT_PATH_BOLD):
    FONT_PATH_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
if not os.path.exists(FONT_PATH_REG):
    FONT_PATH_REG = FONT_PATH_BOLD

# Soft pastel accent colors for intro images (rotates sequentially)
ACCENT_COLORS = [
    (180, 230, 150),  # #B4E696 - Lime green (warmer, more yellow-green)
    (150, 220, 220),  # #96DCDC - Teal/Cyan (cooler, more distinct from green)
    (150, 190, 240),  # #96BEF0 - Sky blue (more saturated blue)
    (203, 192, 236),  # #CBC0EC - Soft purple
    (236, 192, 231),  # #ECC0E7 - Soft pink
    (236, 192, 193),  # #ECC0C1 - Soft coral
    (236, 228, 192),  # #ECE4C0 - Soft cream
]

logger = logging.getLogger('bot_main')

# Random accent for the join-greeting embed (the old welcome cog's palette).
WELCOME_COLORS = [
    discord.Color.from_rgb(255, 107, 107), discord.Color.from_rgb(255, 159, 67),
    discord.Color.from_rgb(255, 214, 0), discord.Color.from_rgb(46, 213, 115),
    discord.Color.from_rgb(0, 210, 211), discord.Color.from_rgb(30, 144, 255),
    discord.Color.from_rgb(116, 94, 255), discord.Color.from_rgb(209, 72, 255),
    discord.Color.from_rgb(255, 71, 181), discord.Color.from_rgb(255, 135, 178),
    discord.Color.from_rgb(0, 184, 148), discord.Color.from_rgb(52, 152, 219),
    discord.Color.from_rgb(241, 196, 15), discord.Color.from_rgb(231, 76, 60),
    discord.Color.from_rgb(155, 89, 182), discord.Color.from_rgb(26, 188, 156),
    discord.Color.from_rgb(230, 126, 34), discord.Color.from_rgb(52, 73, 94),
    discord.Color.from_rgb(253, 121, 168), discord.Color.from_rgb(99, 205, 218),
]

# Longest display name printed in a list row. Anything past this is clipped so
# every entry stays on a single line on mobile, where the viewport is narrow.
MAX_NAME_LEN = 23


def shorten_name(name):
    """Clip a display name to one line's worth of characters."""
    name = " ".join((name or "").split())
    return name if len(name) <= MAX_NAME_LEN else name[:MAX_NAME_LEN].rstrip() + "..."


def fmt_points(points):
    """Points as a compact string: 3 not 3.0, 0.5 kept as 0.5."""
    text = f"{points:.1f}" if isinstance(points, float) else str(points)
    return text[:-2] if text.endswith(".0") else text


# --- WELCOME (per-guild) CONFIG I/O ---
def _load_welcome_sync(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error loading welcome config {file_path}: {e}")
        return {}


def _save_welcome_sync(file_path: str, data: Dict[str, Any]):
    try:
        temp = f"{file_path}.tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp, file_path)
    except IOError as e:
        logger.error(f"Error saving welcome config {file_path}: {e}")


class WelcomeConfigManager:
    """Async wrapper around the per-guild welcome_config.json (blocking I/O off
    the event loop). Carried over verbatim from the old welcome cog."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    async def load(self) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _load_welcome_sync, self.file_path)

    async def save(self, data: Dict[str, Any]):
        data_to_save = copy.deepcopy(data)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _save_welcome_sync, self.file_path, data_to_save)


def _default_welcome_guild_config() -> Dict[str, Any]:
    return {
        "welcome_channel_id": None,
        "intro_channel_id": None,
        "suggest_introduction": False,
        "lfg_forum_id": None,
        "game_mappings": {},  # role_id (str) -> {"thread_id": int}
    }


@dataclass
class TierAuditRow:
    """One member's place in the tier audit."""
    member: discord.Member
    earned: int        # points actually earned (these decay)
    vip_base: int      # permanent floor from a VIP role (never decays)
    points: int        # earned + vip_base
    current: int       # tier role they hold right now
    target: int        # tier their points entitle them to


@dataclass
class TierAuditReport:
    tracked: int
    changes: list
    applied: int = 0
    failed: int = 0

    # Lines to list in a dry-run summary before it gets too long for a message
    MAX_LISTED = 20

    def summary(self):
        promos = [r for r in self.changes if r.target > r.current]
        demos = [r for r in self.changes if r.target < r.current]
        lines = [
            "**Tier audit — dry run**",
            f"Tracked members: **{self.tracked}**",
            f"Promotions: **{len(promos)}** · Demotions: **{len(demos)}**",
            "",
        ]
        for row in (demos + promos)[:self.MAX_LISTED]:
            pts = f"{row.points} pts"
            if row.vip_base:
                pts += f" ({row.earned} earned + {row.vip_base} VIP)"
            lines.append(
                f"• {row.member.mention} — Tier {row.current} → Tier {row.target} · {pts}"
            )
        remaining = len(self.changes) - self.MAX_LISTED
        if remaining > 0:
            lines.append(f"…and **{remaining}** more.")
        lines.append("\nApply these changes?")
        return "\n".join(lines)


class Onboarding(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = DB_NAME
        # In-memory cache for hot settings read on every message.
        # Populated lazily by _get_setting and invalidated by _set_setting.
        # Sentinel distinguishes "cached as missing" from "not yet cached".
        self._settings_cache = {}
        self._settings_missing = object()
        # Gates anything that reads the DB before init_db has created the tables.
        self._db_ready = asyncio.Event()
        # Members mid-swap from newcomer to member role. Two messages landing
        # together would otherwise both cross the threshold and both swap.
        self._graduating = set()

        # --- Welcome half (per-guild greeting, folded in from the welcome cog) ---
        self.welcome_config_manager = WelcomeConfigManager(WELCOME_CONFIG_FILE)
        self.welcome_config: Dict[str, Any] = {}
        # {user_id: {"joined_at": time, "task": asyncio.Task, "triggered": bool}}
        self._pending_welcomes: Dict[int, Dict] = {}

        # --- Dashboard sync ---
        # Flat settings (channels, roles, thresholds, tier/VIP roles) ride the
        # shared per-guild bridge; the rich state (questions, blacklist, per-member
        # points, ranked lists) has its own snapshot+commands bridge.
        self._settings_sync = ConfigSyncAgent(
            "welcome", self._settings_snapshot, self._settings_apply, bot=bot
        )
        self._obridge = OnboardingConfigSync()

        self.bot.loop.create_task(self.init_db())
        self._backfill_task = self.bot.loop.create_task(self._run_backfill())
        self.bot.loop.create_task(self._startup())
        self.decay_task.start()
        self.thread_cleanup_task.start()
        self.onboarding_sync_task.start()

    async def _startup(self):
        self.welcome_config = await self.welcome_config_manager.load()
        await self._obridge.ensure()
        await self._settings_sync.start()

    def cog_unload(self):
        self.decay_task.cancel()
        self.thread_cleanup_task.cancel()
        self.onboarding_sync_task.cancel()
        self._backfill_task.cancel()
        self._settings_sync.stop()

    async def _run_backfill(self):
        """backfill_newcomer_role with its own error handling — it runs as a
        bare task, so an escaping exception would be swallowed by the loop.

        The config check rides along: it wants the same "tables built, gateway
        up" moment, and neither job should stop the other from running.
        """
        try:
            await self.backfill_newcomer_role()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Newcomer role backfill failed: {e}", exc_info=True)

        try:
            await self._warn_if_unconfigured()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Newcomer config check failed: {e}", exc_info=True)

    async def _config_warnings(self):
        """Settings whose absence silently switches a feature off.

        An unset newcomer_role makes the reply-points branch a no-op that is
        indistinguishable from a working feature nobody happens to trigger —
        that state went unnoticed for three months. Shared by the boot check
        and the panel so the two can never disagree.
        """
        problems = []
        newcomer_id, member_id = await self._role_setting_ids()

        if not newcomer_id:
            problems.append(
                "No newcomer role is set — reply points are DISABLED. "
                "Set one in /newcomer_panel → Newcomer Role."
            )
        elif self.bot.guilds and all(not g.get_role(newcomer_id) for g in self.bot.guilds):
            problems.append(
                f"The configured newcomer role ({newcomer_id}) no longer exists in any "
                "guild — reply points are DISABLED until it is re-selected."
            )

        if not member_id:
            problems.append(
                "No member role is set — newcomers never graduate off the newcomer "
                "role. Set one in /newcomer_panel → Member Role."
            )
        return problems

    async def _warn_if_unconfigured(self):
        """Log the config warnings once per boot."""
        await self._db_ready.wait()
        await self.bot.wait_until_ready()
        for problem in await self._config_warnings():
            logger.warning(f"Newcomer: {problem}")

    async def _get_setting(self, db, key):
        """Cached read of a settings row. Returns the string value or None."""
        cached = self._settings_cache.get(key, self._settings_missing)
        if cached is not self._settings_missing:
            return cached
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        value = row[0] if row else None
        self._settings_cache[key] = value
        return value

    async def _setting(self, key):
        """Read a setting without the caller having to hold a connection.

        _get_setting() already serves from cache, but every caller opened a
        connection before calling it — so a cache hit still paid for a
        connection it never used, once per message on the on_message path.
        This opens one only on an actual miss.
        """
        cached = self._settings_cache.get(key, self._settings_missing)
        if cached is not self._settings_missing:
            return cached
        async with aiosqlite.connect(self.db_path) as db:
            return await self._get_setting(db, key)

    def _invalidate_setting(self, key):
        self._settings_cache.pop(key, None)

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
            
            # Updated: Added is_optional column logic
            await db.execute('''
                CREATE TABLE IF NOT EXISTS questions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text TEXT,
                    style TEXT,
                    order_num INTEGER,
                    is_optional INTEGER DEFAULT 0
                )
            ''')
            # Check if column exists, if not add it (Migration for existing DBs)
            try:
                await db.execute("ALTER TABLE questions ADD COLUMN is_optional INTEGER DEFAULT 0")
            except Exception:
                pass

            await db.execute('CREATE TABLE IF NOT EXISTS point_config (tier INTEGER PRIMARY KEY, points_required INTEGER)')

            # One-time migration from the old 5-base-role x 3-tier system:
            # the old Tier 1/2 thresholds become the new global Tier 2/3 thresholds
            # (the old 3rd upgrade is removed). Reward roles now live in the
            # Alerts & Colors cog config (alerts_colors_config.json).
            cursor = await db.execute("SELECT value FROM settings WHERE key='tier_rework_v2'")
            if not await cursor.fetchone():
                cursor = await db.execute("SELECT tier, points_required FROM point_config")
                old = dict(await cursor.fetchall())
                if 1 in old:
                    await db.execute("INSERT OR REPLACE INTO point_config (tier, points_required) VALUES (2, ?)", (old[1],))
                    if 2 in old:
                        await db.execute("INSERT OR REPLACE INTO point_config (tier, points_required) VALUES (3, ?)", (old[2],))
                    await db.execute("DELETE FROM point_config WHERE tier = 1")
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('tier_rework_v2', '1')")
            await db.execute('CREATE TABLE IF NOT EXISTS user_points (user_id INTEGER PRIMARY KEY, points INTEGER DEFAULT 0)')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS thread_logs (
                    user_id INTEGER,
                    thread_id INTEGER,
                    msg_count INTEGER DEFAULT 0,
                    points_earned INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, thread_id)
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS point_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    month_str TEXT,
                    points INTEGER
                )
            ''')
            await db.execute('CREATE TABLE IF NOT EXISTS blacklist (user_id INTEGER PRIMARY KEY)')

            # Point decay ledger: each row is a point grant that expires after 30 days
            await db.execute('''
                CREATE TABLE IF NOT EXISTS point_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    points INTEGER,
                    earned_at TEXT,
                    expires_at TEXT,
                    decayed INTEGER DEFAULT 0
                )
            ''')
            
            # New Table: Metadata for threads to handle deletion
            await db.execute('''
                CREATE TABLE IF NOT EXISTS intro_metadata (
                    thread_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    lore_msg_id INTEGER,
                    parent_channel_id INTEGER,
                    qa_msg_id INTEGER,
                    intro_channel_id INTEGER
                )
            ''')
            # Migration for existing DBs - add qa_msg_id column
            try:
                await db.execute("ALTER TABLE intro_metadata ADD COLUMN qa_msg_id INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE intro_metadata ADD COLUMN intro_channel_id INTEGER")
            except Exception:
                pass
            # Marks a row whose thread has been auto-deleted. The row itself stays
            # so the one-intro-per-user check still holds after cleanup.
            try:
                await db.execute("ALTER TABLE intro_metadata ADD COLUMN cleaned_up INTEGER DEFAULT 0")
            except Exception:
                pass

            # Hourly point tracking for rate limiting
            await db.execute('''
                CREATE TABLE IF NOT EXISTS hourly_points (
                    user_id INTEGER,
                    hour_key TEXT,
                    points_earned INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, hour_key)
                )
            ''')

            # Superseded by the economy cog's levels as the graduation trigger.
            await db.execute("DROP TABLE IF EXISTS message_counts")

            # Left behind by the 5-base-role x 3-tier system the tier_rework_v2
            # migration replaced. Nothing has read either since, but they stayed
            # populated, so the database looked correctly configured while
            # settings.newcomer_role — the key the reply-points branch actually
            # reads — was empty. Not migrated into it on purpose: base_rank 1 is
            # the *member* role, so seeding from it would set the wrong role.
            await db.execute("DROP TABLE IF EXISTS base_roles")
            await db.execute("DROP TABLE IF EXISTS role_config")

            await db.commit()
        self._db_ready.set()

    # --- IMAGE GENERATION ---
    def generate_lore_banner(self, username, color_index=0):
        """Generates the RPG Style Banner - high resolution for sharp text"""
        # Render at 4x scale for crisp text
        SCALE = 4
        W, H = 400 * SCALE, 70 * SCALE  # 1600x280
        bg_color = (43, 45, 49)
        accent_color = ACCENT_COLORS[color_index % len(ACCENT_COLORS)]

        img = Image.new('RGB', (W, H), color=bg_color)
        draw = ImageDraw.Draw(img)
        draw.rectangle([(0, 0), (6 * SCALE, H)], fill=accent_color)

        try:
            font_lg = ImageFont.truetype(FONT_PATH_BOLD, 20 * SCALE)  # 60pt
            font_sm = ImageFont.truetype(FONT_PATH_BOLD, 12 * SCALE)  # 36pt
        except Exception:
            font_lg = ImageFont.load_default()
            font_sm = ImageFont.load_default()

        # Center vertically (scaled positions)
        draw.text((20 * SCALE, 8 * SCALE), f"{username.upper()}'S", font=font_sm, fill="white")
        draw.text((20 * SCALE, 26 * SCALE), "LORE DROP", font=font_lg, fill=accent_color)

        return img

    def generate_qa_image(self, qa_list, avatar_bytes=None, username="User", color_index=0):
        """Generates High Quality Infographic List with user header - rendered at 3x for sharp text"""
        # qa_list = [(Question, Answer), ...]
        SCALE = 3  # Render at 3x for crisp text
        W = 700 * SCALE
        padding = 20 * SCALE
        row_padding = 15 * SCALE

        # Get accent color based on rotating index
        accent_color = ACCENT_COLORS[color_index % len(ACCENT_COLORS)]

        # Header dimensions for avatar + username
        avatar_size = 40 * SCALE
        header_height = avatar_size + (20 * SCALE)

        try:
            font_q = ImageFont.truetype(FONT_PATH_BOLD, 18 * SCALE)
            font_a = ImageFont.truetype(FONT_PATH_BOLD, 14 * SCALE)
            font_username = ImageFont.truetype(FONT_PATH_BOLD, 20 * SCALE)
        except Exception:
            font_q = ImageFont.load_default()
            font_a = ImageFont.load_default()
            font_username = ImageFont.load_default()

        rows = []
        total_h = padding + header_height  # Start after header
        draw_temp = ImageDraw.Draw(Image.new('RGB', (1, 1)))

        for q, a in qa_list:
            # Skip if empty answer (double check)
            if not a or not a.strip():
                continue

            char_width = 10 * SCALE
            wrap_width = int((W - (padding * 2)) / char_width) + 10
            wrapped_a = textwrap.fill(a, width=wrap_width)

            bbox_q = draw_temp.textbbox((0, 0), q, font=font_q)
            h_q = bbox_q[3] - bbox_q[1]

            bbox_a = draw_temp.textbbox((0, 0), wrapped_a, font=font_a)
            h_a = bbox_a[3] - bbox_a[1]

            row_h = h_q + h_a + (row_padding * 2) + (10 * SCALE)
            rows.append({'q': q, 'a': wrapped_a, 'h': row_h, 'h_q': h_q})
            total_h += row_h

        total_h += padding

        if not rows:
            # Fallback if somehow empty
            total_h = (100 * SCALE) + header_height

        img = Image.new('RGB', (W, total_h), color=(30, 31, 34))
        draw = ImageDraw.Draw(img)

        # Draw user header (avatar + username)
        avatar_x = padding
        avatar_y = padding

        if avatar_bytes:
            try:
                avatar_img = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
                avatar_img = avatar_img.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)

                # Create circular mask
                mask = Image.new("L", (avatar_size, avatar_size), 0)
                mask_draw = ImageDraw.Draw(mask)
                mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)

                # Create circular avatar
                circular_avatar = Image.new("RGBA", (avatar_size, avatar_size), (0, 0, 0, 0))
                circular_avatar.paste(avatar_img, (0, 0), mask)

                # Paste onto main image
                img.paste(circular_avatar, (avatar_x, avatar_y), circular_avatar)
            except Exception:
                # Draw placeholder circle if avatar fails
                draw.ellipse((avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size),
                            fill=accent_color)
        else:
            # Draw placeholder circle
            draw.ellipse((avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size),
                        fill=accent_color)

        # Draw username next to avatar
        username_x = avatar_x + avatar_size + (12 * SCALE)
        username_y = avatar_y + (avatar_size - (20 * SCALE)) // 2
        draw.text((username_x, username_y), username, font=font_username, fill="white")

        # Draw Q&A rows (starting after header)
        y = padding + header_height
        for i, row in enumerate(rows):
            if i % 2 == 0:
                draw.rectangle([(0, y), (W, y + row['h'])], fill=(43, 45, 49))

            draw.text((padding, y + row_padding), row['q'], font=font_q, fill=accent_color)
            draw.text((padding, y + row_padding + row['h_q'] + (8 * SCALE)), row['a'], font=font_a, fill="white")
            y += row['h']

        # Border outline with buffer for Discord corner rounding
        border_margin = 9 * SCALE
        draw.rectangle([(border_margin, border_margin), (W - 1 - border_margin, total_h - 1 - border_margin)], outline=accent_color, width=3 * SCALE)

        return img

    # --- EVENTS ---
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get('custom_id')
            
            # INTRO BUTTON CLICK
            if custom_id == "start_intro_modal":
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute("SELECT 1 FROM blacklist WHERE user_id = ?", (interaction.user.id,))
                    if await cursor.fetchone():
                        return await interaction.response.send_message("You are blocked from using this.", ephemeral=True)

                    # Check for duplicate intro (admins can bypass)
                    if not self.bot.is_bot_admin(interaction.user):
                        cursor = await db.execute("SELECT 1 FROM intro_metadata WHERE user_id = ?", (interaction.user.id,))
                        if await cursor.fetchone():
                            return await interaction.response.send_message("You've already created an intro. Only one intro per user is allowed.", ephemeral=True)

                    # Fetch questions
                    cursor = await db.execute("SELECT text, style, is_optional FROM questions ORDER BY order_num ASC")
                    questions = await cursor.fetchall()
                
                if not questions:
                    return await interaction.response.send_message("No questions configured yet.", ephemeral=True)
                
                if len(questions) > 5:
                    questions = questions[:5] 

                modal = DynamicIntroModal(self, questions)
                await interaction.response.send_modal(modal)

            # CLOSE THREAD BUTTON CLICK
            elif custom_id and custom_id.startswith("close_thread_btn"):
                # Check DB for ownership
                thread_id = interaction.channel_id
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute("SELECT user_id, lore_msg_id, parent_channel_id FROM intro_metadata WHERE thread_id = ?", (thread_id,))
                    row = await cursor.fetchone()
                
                if not row:
                    # Fallback check if DB entry missing
                    # Fix: Added strict type check to prevent AttributeError on owner_id
                    is_owner = False
                    if isinstance(interaction.channel, discord.Thread) and interaction.channel.owner_id == interaction.user.id:
                        is_owner = True

                    if is_owner or self.bot.is_bot_admin(interaction.user):
                         # Allow confirm without deleting lore msg (cant find it)
                         return await interaction.response.send_message("Are you sure you want to close this thread?", view=CloseThreadConfirmView(self, None, None), ephemeral=True)
                    return await interaction.response.send_message("❌ Cannot verify thread ownership.", ephemeral=True)

                owner_id, lore_msg_id, parent_channel_id = row
                
                if interaction.user.id != owner_id and not self.bot.is_bot_admin(interaction.user):
                    return await interaction.response.send_message("❌ Only the thread owner or Admins can close this.", ephemeral=True)
                
                # Show confirmation
                view = CloseThreadConfirmView(self, lore_msg_id, parent_channel_id)
                await interaction.response.send_message("⚠️ **Are you sure?**\nThis will delete this thread AND the Lore Drop banner in the main channel.", view=view, ephemeral=True)


    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot or not message.guild:
            return

        # Shared low-effort filter (also applied to replies now)
        content = message.content or ""
        is_low_effort = len(content) < 3 or len(content.split()) < 2

        # Determine if this message is inside an intro thread so we can
        # avoid double-dipping reply points + thread points on the same message.
        in_intro_thread = False
        if isinstance(message.channel, discord.Thread):
            thread_parent_setting = await self._setting('thread_channel_id')
            if thread_parent_setting:
                try:
                    in_intro_thread = message.channel.parent_id == int(thread_parent_setting)
                except ValueError:
                    in_intro_thread = False

        # VIP Role Reply Logic
        # Only run when we have a reply reference, and skip low-effort content
        # and any reply inside an intro thread (thread-points branch handles it).
        if (
            message.reference
            and message.reference.message_id
            and not is_low_effort
            and not in_intro_thread
        ):
            # Resolve the replied-to message. `resolved` is often None when the
            # target wasn't in the gateway payload cache, so fall back to fetch.
            replied_to = message.reference.resolved
            if not isinstance(replied_to, discord.Message):
                try:
                    replied_to = await message.channel.fetch_message(message.reference.message_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    replied_to = None

            # Who the reply is really aimed at. Normally the author of the
            # message replied to — but the intro posts are written by the bot,
            # so a reply to one is a welcome aimed at the newcomer it is about.
            target_id = None
            if replied_to:
                if replied_to.author.bot:
                    target_id = await self._intro_owner_for_message(
                        message.channel.id, replied_to.id
                    )
                else:
                    target_id = replied_to.author.id

            if target_id and target_id != message.author.id:
                # Resolve the target as a Member via the guild so we can read .roles.
                # replied_to.author may be a User, not a Member.
                target_member = message.guild.get_member(target_id)

                # Settle the role gate from cached settings before touching the
                # database. Most replies in the server are not aimed at a
                # newcomer, and those used to open a connection they never
                # wrote a row through.
                vip_role_id = None
                role_value = await self._setting('newcomer_role')
                if role_value and target_member:
                    try:
                        vip_role_id = int(role_value)
                    except ValueError:
                        vip_role_id = None

                if vip_role_id and any(r.id == vip_role_id for r in target_member.roles):
                    pts_value = await self._setting('reply_points')
                    try:
                        reply_pts = float(pts_value) if pts_value else 0.5
                    except ValueError:
                        reply_pts = 0.5

                    # Apply hourly cap to reply points too
                    cap_value = await self._setting('hourly_point_cap')
                    try:
                        hourly_cap = int(cap_value) if cap_value else 0
                    except ValueError:
                        hourly_cap = 0

                    hour_key = datetime.datetime.now().strftime("%Y-%m-%d-%H")

                    async with aiosqlite.connect(self.db_path) as db:
                        award = reply_pts
                        if hourly_cap > 0:
                            cursor = await db.execute(
                                "SELECT points_earned FROM hourly_points WHERE user_id = ? AND hour_key = ?",
                                (message.author.id, hour_key),
                            )
                            hrow = await cursor.fetchone()
                            hourly_earned = hrow[0] if hrow else 0
                            remaining = hourly_cap - hourly_earned
                            if remaining <= 0:
                                award = 0
                            elif award > remaining:
                                award = remaining

                        if award > 0:
                            today = datetime.date.today().isoformat()
                            expires_at = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()

                            await db.execute(
                                "INSERT INTO user_points (user_id, points) VALUES (?, ?) "
                                "ON CONFLICT(user_id) DO UPDATE SET points = points + ?",
                                (message.author.id, award, award),
                            )
                            cursor = await db.execute(
                                "SELECT id FROM point_ledger WHERE user_id = ? AND earned_at = ? AND decayed = 0",
                                (message.author.id, today),
                            )
                            ledger_row = await cursor.fetchone()
                            if ledger_row:
                                await db.execute(
                                    "UPDATE point_ledger SET points = points + ? WHERE id = ?",
                                    (award, ledger_row[0]),
                                )
                            else:
                                await db.execute(
                                    "INSERT INTO point_ledger (user_id, points, earned_at, expires_at) VALUES (?, ?, ?, ?)",
                                    (message.author.id, award, today, expires_at),
                                )
                            await db.execute(
                                "INSERT INTO hourly_points (user_id, hour_key, points_earned) VALUES (?, ?, ?) "
                                "ON CONFLICT(user_id, hour_key) DO UPDATE SET points_earned = points_earned + ?",
                                (message.author.id, hour_key, award, award),
                            )
                            await db.commit()
                            # INFO, not debug: the root logger runs at INFO, so
                            # a debug line here writes nowhere and the whole
                            # reply-points path looks identical whether it is
                            # working or silently disabled.
                            logger.info(
                                f"Reply points: awarded {fmt_points(award)} to {message.author} "
                                f"({message.author.id}) for replying to newcomer {target_member} "
                                f"({target_member.id})"
                            )
                            await self.check_role_upgrade(message.author, db)

        # Only process messages in threads for thread-points branch
        if not isinstance(message.channel, discord.Thread):
            return

        # Intro Thread Points Logic
        # in_intro_thread was already resolved from 'thread_channel_id' at the
        # top of this handler, so every gate below is answerable without the
        # database. Only open a connection once we know points are in play.
        if not in_intro_thread:
            return

        if not message.content or len(message.content) < 3:
            return

        # Filter out 1-word messages (low effort)
        if len(message.content.split()) < 2:
            return

        user_id = message.author.id
        thread_id = message.channel.id

        async with aiosqlite.connect(self.db_path) as db:
            # Check if user is the thread owner (intro creator) - they can't earn points in their own thread
            cursor = await db.execute("SELECT user_id FROM intro_metadata WHERE thread_id = ?", (thread_id,))
            owner_row = await cursor.fetchone()
            if owner_row and owner_row[0] == user_id:
                return  # Thread owner cannot earn points in their own thread

            # Check hourly cap
            hour_key = datetime.datetime.now().strftime("%Y-%m-%d-%H")
            cap_value = await self._get_setting(db, 'hourly_point_cap')
            try:
                hourly_cap = int(cap_value) if cap_value else 0  # 0 = no cap
            except ValueError:
                hourly_cap = 0

            if hourly_cap > 0:
                cursor = await db.execute("SELECT points_earned FROM hourly_points WHERE user_id = ? AND hour_key = ?", (user_id, hour_key))
                hourly_row = await cursor.fetchone()
                hourly_earned = hourly_row[0] if hourly_row else 0
                if hourly_earned >= hourly_cap:
                    return  # User has hit the hourly cap

            cursor = await db.execute("SELECT msg_count, points_earned FROM thread_logs WHERE user_id = ? AND thread_id = ?", (user_id, thread_id))
            log = await cursor.fetchone()

            points_to_add = 0
            if not log:
                await db.execute("INSERT INTO thread_logs (user_id, thread_id, msg_count, points_earned) VALUES (?, ?, 1, 3)", (user_id, thread_id))
                points_to_add = 3
            else:
                count, earned = log
                if earned < 5:
                    new_points = 1
                    if earned + new_points > 5:
                        new_points = 5 - earned
                    if new_points > 0:
                        await db.execute("UPDATE thread_logs SET msg_count = msg_count + 1, points_earned = points_earned + ? WHERE user_id = ? AND thread_id = ?", (new_points, user_id, thread_id))
                        points_to_add = new_points

            # Apply hourly cap limit to points_to_add if needed
            if hourly_cap > 0 and points_to_add > 0:
                cursor = await db.execute("SELECT points_earned FROM hourly_points WHERE user_id = ? AND hour_key = ?", (user_id, hour_key))
                hourly_row = await cursor.fetchone()
                hourly_earned = hourly_row[0] if hourly_row else 0
                remaining = hourly_cap - hourly_earned
                if points_to_add > remaining:
                    points_to_add = remaining

            if points_to_add > 0:
                await db.execute("INSERT INTO user_points (user_id, points) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET points = points + ?", (user_id, points_to_add, points_to_add))
                # Record in decay ledger
                earned_at = datetime.date.today().isoformat()
                expires_at = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
                await db.execute(
                    "INSERT INTO point_ledger (user_id, points, earned_at, expires_at) VALUES (?, ?, ?, ?)",
                    (user_id, points_to_add, earned_at, expires_at)
                )
                # Track hourly points
                await db.execute("INSERT INTO hourly_points (user_id, hour_key, points_earned) VALUES (?, ?, ?) ON CONFLICT(user_id, hour_key) DO UPDATE SET points_earned = points_earned + ?", (user_id, hour_key, points_to_add, points_to_add))
                await db.commit()
                await self.check_role_upgrade(message.author, db)

    async def _newcomer_role(self, guild, db):
        """The role set in the panel, resolved against `guild`. None if unset,
        malformed, or deleted from the server."""
        value = await self._get_setting(db, 'newcomer_role')
        if not value:
            return None
        try:
            return guild.get_role(int(value))
        except ValueError:
            return None

    async def _role_setting_ids(self):
        """(newcomer_role_id, member_role_id), either possibly None.

        Served from the settings cache. on_message runs for every message in the
        server, so this only opens the DB on the first read after startup or a
        panel change — a missing key caches as None and doesn't re-query.
        """
        keys = ('newcomer_role', 'member_role')
        if any(self._settings_cache.get(k, self._settings_missing) is self._settings_missing
               for k in keys):
            async with aiosqlite.connect(self.db_path) as db:
                for key in keys:
                    await self._get_setting(db, key)

        ids = []
        for key in keys:
            value = self._settings_cache.get(key)
            try:
                ids.append(int(value) if value else None)
            except ValueError:
                ids.append(None)
        return ids[0], ids[1]

    async def _intro_channel_ids(self):
        """(intro_channel_id, thread_parent_id), either possibly None.

        Served from the settings cache for the same reason as
        _role_setting_ids: this is on the path of every reply in the server.
        """
        keys = ('intro_channel_id', 'thread_channel_id')
        if any(self._settings_cache.get(k, self._settings_missing) is self._settings_missing
               for k in keys):
            async with aiosqlite.connect(self.db_path) as db:
                for key in keys:
                    await self._get_setting(db, key)

        ids = []
        for key in keys:
            value = self._settings_cache.get(key)
            try:
                ids.append(int(value) if value else None)
            except ValueError:
                ids.append(None)
        return ids[0], ids[1]

    async def _intro_owner_for_message(self, channel_id, message_id):
        """The newcomer an intro post is about, or None if it isn't one.

        Both halves of an intro are posted by the bot — the Q&A image in the
        intro channel and the Lore Drop banner in the thread parent — so
        without this, welcoming someone by replying to their own intro is the
        one reply that earns nothing.

        The channel check comes first so an ordinary reply to any other bot
        (trivia, polls, music) costs no database round trip at all.
        """
        intro_id, parent_id = await self._intro_channel_ids()
        if channel_id not in (intro_id, parent_id):
            return None

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT user_id FROM intro_metadata WHERE qa_msg_id = ? OR lore_msg_id = ?",
                (message_id, message_id),
            )
            row = await cursor.fetchone()
        return row[0] if row else None

    async def _member_role_level(self):
        """Level at which a newcomer becomes a member."""
        async with aiosqlite.connect(self.db_path) as db:
            value = await self._get_setting(db, 'member_role_level')
        try:
            return int(value) if value else DEFAULT_MEMBER_ROLE_LEVEL
        except ValueError:
            return DEFAULT_MEMBER_ROLE_LEVEL

    @commands.Cog.listener()
    async def on_member_level_up(self, user_id, old_level, new_level):
        """Dispatched by the economy cog's leveling engine on a real level gain."""
        newcomer_id, member_id = await self._role_setting_ids()
        if not newcomer_id or not member_id:
            return
        if new_level < await self._member_role_level():
            return

        # XP is tracked per user, not per guild, so act only where both roles
        # actually exist — that scopes this to the real server on its own.
        for guild in self.bot.guilds:
            if not guild.get_role(newcomer_id) or not guild.get_role(member_id):
                continue
            member = guild.get_member(user_id)
            if member and not member.bot:
                await self._swap_to_member_role(member, newcomer_id, member_id)

    async def _swap_to_member_role(self, member, newcomer_id, member_id, reason="Reached member level"):
        """Give the member role if they lack it, then take the newcomer role away
        if they have it. Doing nothing is a valid outcome.

        Add-before-remove on purpose: if the second call fails the member is left
        holding both roles, which the hourly audit will finish. The reverse order
        could strip their only role and leave them with nothing.
        """
        if member.id in self._graduating:
            return
        self._graduating.add(member.id)
        try:
            guild = member.guild
            member_role = guild.get_role(member_id)
            if not member_role:
                logger.warning(f"Member role {member_id} no longer exists in {guild.name}.")
                return

            if not any(r.id == member_id for r in member.roles):
                try:
                    await member.add_roles(member_role, reason=reason)
                except discord.Forbidden:
                    logger.warning(
                        f"Missing permissions to give {member_role.name} to {member.name} "
                        f"({member.id}) — the bot needs Manage Roles and a role above it."
                    )
                    return
                except discord.HTTPException as e:
                    logger.warning(f"Could not give {member_role.name} to {member.name}: {e}")
                    return

            # Only now is it safe to drop the newcomer role.
            newcomer_role = guild.get_role(newcomer_id)
            if newcomer_role and any(r.id == newcomer_id for r in member.roles):
                try:
                    await member.remove_roles(newcomer_role, reason=reason)
                except discord.HTTPException as e:
                    logger.warning(
                        f"Gave {member_role.name} to {member.name} but could not remove "
                        f"{newcomer_role.name}: {e}"
                    )
                    return

            logger.info(f"{member.name} ({member.id}) — {reason}, swapped to {member_role.name}.")
            # Only dispatched on a swap that actually completed — every failure
            # path above returns, so this never claims a graduation that
            # half-happened.
            self.bot.dispatch("newcomer_graduated", guild.id, member.id, reason)
        finally:
            self._graduating.discard(member.id)

    async def _levelled_user_ids(self, level):
        """Every user id at or above `level`, straight from the economy cog.
        None (not an empty set) if that cog isn't available to ask."""
        economy = self.bot.get_cog("Economy")
        if not economy or not getattr(economy, "db", None):
            return None
        rows = await economy.db.fetchall(
            "SELECT user_id FROM users WHERE level >= ?", (level,)
        )
        return {row["user_id"] for row in rows}

    async def audit_member_role_swaps(self, dry_run=False):
        """Self-heal: anyone holding the newcomer role who is already at the
        member level gets swapped over.

        Covers level-ups missed while the bot was down, half-applied swaps
        (both roles held because the remove failed), and members who levelled
        before the roles were configured. Returns the members it did (or would)
        swap, so this doubles as the dry run.
        """
        newcomer_id, member_id = await self._role_setting_ids()
        if not newcomer_id or not member_id:
            return []

        level = await self._member_role_level()
        due = await self._levelled_user_ids(level)
        if due is None:
            logger.warning("Economy cog unavailable — skipping member role audit.")
            return []
        if not due:
            return []

        swapped = []
        for guild in self.bot.guilds:
            if not guild.get_role(newcomer_id) or not guild.get_role(member_id):
                continue
            for user_id in due:
                member = guild.get_member(user_id)
                if not member or member.bot:
                    continue
                if not any(r.id == newcomer_id for r in member.roles):
                    continue
                swapped.append(member)
                if not dry_run:
                    await self._swap_to_member_role(
                        member, newcomer_id, member_id, reason=f"Reached level {level}"
                    )
                    await asyncio.sleep(1)
        return swapped

    async def _grant_newcomer_role(self, member, role, reason):
        """The one place the newcomer role is added. True if it was granted."""
        if member.bot or any(r.id == role.id for r in member.roles):
            return False
        try:
            await member.add_roles(role, reason=reason)
            return True
        except discord.Forbidden:
            logger.warning(
                f"Missing permissions to give {role.name} to {member.name} ({member.id}) — "
                "the bot needs Manage Roles and a role above it."
            )
        except discord.HTTPException as e:
            logger.warning(f"Could not give {role.name} to {member.name} ({member.id}): {e}")
        return False

    @commands.Cog.listener()
    async def on_member_join(self, member):
        """Give every new arrival the newcomer role, then start the greeting timer."""
        if member.bot:
            return
        async with aiosqlite.connect(self.db_path) as db:
            role = await self._newcomer_role(member.guild, db)
        if role:
            await self._grant_newcomer_role(member, role, "Newcomer role on join")

        # --- Welcome greeting (folded in from the welcome cog) ---
        gc = self._welcome_guild_config(member.guild.id)
        if not gc.get("welcome_channel_id"):
            return
        # Wait for a game role before greeting; a fallback timer greets anyway if
        # onboarding is never completed.
        task = asyncio.create_task(self._delayed_welcome(member, MAX_ONBOARDING_WAIT_SECONDS))
        self._pending_welcomes[member.id] = {
            "joined_at": time.time(), "task": task, "triggered": False,
        }

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Watch a pending joiner's roles so the greeting can name their games."""
        if after.bot:
            return
        pending = self._pending_welcomes.get(after.id)
        if not pending or before.roles == after.roles:
            return
        gc = self._welcome_guild_config(after.guild.id)
        game_role_ids = {int(rid) for rid in gc.get("game_mappings", {})}
        added_role_ids = {r.id for r in after.roles} - {r.id for r in before.roles}
        if not (added_role_ids & game_role_ids):
            return
        # First game role signals onboarding is done; further adds re-batch. Either
        # way, restart the settle timer to bundle roles arriving together.
        pending["triggered"] = True
        task = pending.get("task")
        if task and not task.done():
            task.cancel()
        pending["task"] = asyncio.create_task(self._delayed_welcome(after))

    async def backfill_newcomer_role(self):
        """Catch joins the bot missed while it was offline.

        A join is only ever missed while the gateway is disconnected, so this
        runs once per boot instead of on a loop. The watermark is seeded on the
        first run and only ever moves forward, which means members who joined
        before this feature existed are never touched, and a mod stripping the
        role by hand is permanent — the backfill can't resurrect it.
        """
        await self._db_ready.wait()
        await self.bot.wait_until_ready()

        # Taken before the scan so anyone joining mid-scan is still covered on
        # the next boot if their on_member_join lands in the gap.
        started_at = datetime.datetime.now(datetime.timezone.utc)

        async with aiosqlite.connect(self.db_path) as db:
            since_value = await self._get_setting(db, 'newcomer_backfill_since')

            since = None
            if since_value:
                try:
                    since = datetime.datetime.fromisoformat(since_value)
                    # joined_at is always tz-aware; a naive watermark would raise
                    # on comparison rather than just being wrong.
                    if since.tzinfo is None:
                        since = since.replace(tzinfo=datetime.timezone.utc)
                except ValueError:
                    logger.warning(f"Unreadable newcomer_backfill_since ({since_value!r}); reseeding.")

            if since is not None:
                for guild in self.bot.guilds:
                    role = await self._newcomer_role(guild, db)
                    if not role:
                        continue
                    missed = [
                        m for m in guild.members
                        if not m.bot
                        and m.joined_at is not None
                        and m.joined_at > since
                        and not any(r.id == role.id for r in m.roles)
                    ]
                    granted = 0
                    for member in missed:
                        if await self._grant_newcomer_role(member, role, "Newcomer role (missed while offline)"):
                            granted += 1
                        await asyncio.sleep(1)
                    if granted:
                        logger.info(
                            f"Newcomer backfill ({guild.name}): gave {role.name} to {granted} "
                            f"member(s) who joined while the bot was offline."
                        )

            # Advance unconditionally: with no role configured, "the future"
            # starts now rather than retroactively once one is chosen.
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('newcomer_backfill_since', ?)",
                (started_at.isoformat(),),
            )
            await db.commit()
        self._invalidate_setting('newcomer_backfill_since')

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        """Clean up all intro data when a user leaves the server"""
        # Drop any greeting still waiting on this member.
        pending = self._pending_welcomes.pop(member.id, None)
        if pending:
            task = pending.get("task")
            if task and not task.done():
                task.cancel()

        async with aiosqlite.connect(self.db_path) as db:
            # Get intro metadata for this user
            cursor = await db.execute(
                "SELECT thread_id, lore_msg_id, parent_channel_id, qa_msg_id, intro_channel_id FROM intro_metadata WHERE user_id = ?",
                (member.id,)
            )
            row = await cursor.fetchone()

            if row:
                thread_id, lore_msg_id, parent_channel_id, qa_msg_id, intro_channel_id = row

                # Delete the lore banner message from parent channel
                if parent_channel_id and lore_msg_id:
                    try:
                        parent_ch = member.guild.get_channel(parent_channel_id)
                        if not parent_ch:
                            parent_ch = await member.guild.fetch_channel(parent_channel_id)
                        msg = await parent_ch.fetch_message(lore_msg_id)
                        await msg.delete()
                    except Exception as e:
                        logger.warning(f"Could not delete lore message for {member.id}: {e}")

                # Delete the Q&A image from intro channel
                if intro_channel_id and qa_msg_id:
                    try:
                        intro_ch = member.guild.get_channel(intro_channel_id)
                        if not intro_ch:
                            intro_ch = await member.guild.fetch_channel(intro_channel_id)
                        qa_msg = await intro_ch.fetch_message(qa_msg_id)
                        await qa_msg.delete()
                    except Exception as e:
                        logger.warning(f"Could not delete Q&A message for {member.id}: {e}")

                # Delete the thread
                if thread_id:
                    try:
                        thread = member.guild.get_channel(thread_id)
                        if not thread:
                            thread = await member.guild.fetch_channel(thread_id)
                        await thread.delete()
                    except Exception as e:
                        logger.warning(f"Could not delete thread for {member.id}: {e}")

                # Clean up database entries
                await db.execute("DELETE FROM intro_metadata WHERE user_id = ?", (member.id,))

            # Clean up other user data from this cog
            await db.execute("DELETE FROM thread_logs WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM user_points WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM point_ledger WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM hourly_points WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM blacklist WHERE user_id = ?", (member.id,))
            await db.commit()

            logger.info(f"Cleaned up intro data for departed member {member.name} ({member.id})")

    # ------------------------------------------------------------------
    # WELCOME GREETING (per-guild; folded in from the welcome cog)
    # ------------------------------------------------------------------
    def _welcome_guild_config(self, guild_id: int) -> Dict[str, Any]:
        key = str(guild_id)
        if key not in self.welcome_config:
            self.welcome_config[key] = _default_welcome_guild_config()
        return self.welcome_config[key]

    async def _save_welcome_config(self):
        await self.welcome_config_manager.save(self.welcome_config)
        # Any save should reach the dashboard on the next sync tick.
        self._settings_sync.mark_dirty()

    async def _delayed_welcome(self, member: discord.Member, delay: int = ROLE_WAIT_SECONDS):
        """Wait for role additions to settle, then send a tailored greeting."""
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        self._pending_welcomes.pop(member.id, None)

        gc = self._welcome_guild_config(member.guild.id)
        welcome_ch_id = gc.get("welcome_channel_id")
        if not welcome_ch_id:
            return
        channel = member.guild.get_channel(welcome_ch_id)
        if not channel:
            try:
                channel = await member.guild.fetch_channel(welcome_ch_id)
            except Exception:
                return

        try:
            member = await member.guild.fetch_member(member.id)
        except Exception:
            return

        game_mappings = gc.get("game_mappings", {})
        matched_roles = []
        for role_id_str, mapping in game_mappings.items():
            try:
                role_id = int(role_id_str)
            except ValueError:
                continue
            if any(r.id == role_id for r in member.roles):
                matched_roles.append((member.guild.get_role(role_id), mapping))

        lfg_forum_id = gc.get("lfg_forum_id")
        intro_ch_id = gc.get("intro_channel_id")
        suggest_intro = gc.get("suggest_introduction")

        embed = discord.Embed(
            title=f"Welcome to {member.guild.name}!",
            color=random.choice(WELCOME_COLORS),
        )
        embed.set_thumbnail(url=member.display_avatar.url)

        desc_lines = []
        if len(matched_roles) == 1:
            role, mapping = matched_roles[0]
            thread_id = mapping.get("thread_id")
            role_name = role.name if role else "LFG"
            desc_lines.append(
                f"If you're looking for teammates right now, head over to <#{thread_id}> "
                f"and add `@{role_name}` to your message to alert other players."
            )
        elif len(matched_roles) > 1:
            if lfg_forum_id:
                desc_lines.append(
                    f"If you're looking for teammates, head over to <#{lfg_forum_id}> "
                    f"and find the thread for the game you want to play. "
                    f"Add the game's LFG ping (e.g. `@GameLFG`) to your message to alert other players."
                )
            else:
                desc_lines.append(
                    "If you're looking for teammates, check out our LFG channels "
                    "and add the game's LFG ping to your message to alert other players."
                )
        elif lfg_forum_id:
            desc_lines.append(
                f"If you're looking for teammates, head over to <#{lfg_forum_id}> "
                f"and find the thread for your game. "
                f"Add the game's LFG ping (e.g. `@GameLFG`) to your message to alert other players."
            )

        desc_lines.append(
            "*When responding to someone, remember to reply to their message so they get notified.*"
        )
        if suggest_intro and intro_ch_id:
            desc_lines.append(
                f"Feel free to introduce yourself in <#{intro_ch_id}> -- we'd love to get to know you."
            )
        embed.description = "\n\n".join(desc_lines)
        await channel.send(content=member.mention, embed=embed)

    @staticmethod
    def _target_tier(points, t2_req, t3_req):
        """Map a point total to a reward tier (1 = baseline, no reward role)."""
        target = 1
        if t2_req is not None and points >= t2_req:
            target = 2
        if t3_req is not None and points >= t3_req:
            target = 3
        return target

    def _alerts_cog(self):
        """The Alerts & Colors cog, which owns every gate/color role mutation."""
        return self.bot.get_cog("AlertsAndColors")

    async def _thresholds(self, db):
        """(t2_req, t3_req) from point_config; either may be None."""
        thresholds = {}
        cursor = await db.execute("SELECT tier, points_required FROM point_config")
        async for row in cursor:
            thresholds[row[0]] = row[1]
        return thresholds.get(2), thresholds.get(3)

    @staticmethod
    def vip_base_points(alerts_cog, member, t2_req, t3_req):
        """Permanent point floor a member's VIP role grants them.

        A Tier 2 VIP role is always worth exactly the current Tier 2 threshold
        and a Tier 3 VIP role the Tier 3 threshold, so the floor follows the
        thresholds if an admin changes them. It's virtual: never written to
        user_points/point_ledger, so it can't decay. Earned points stack on
        top, which is how a Tier 2 VIP climbs to Tier 3.
        """
        if not alerts_cog:
            return 0
        vip_tier = alerts_cog.get_vip_tier(member)
        req = t3_req if vip_tier == 3 else t2_req if vip_tier == 2 else None
        return req or 0

    async def effective_points(self, member, db, t2_req=None, t3_req=None):
        """(earned, vip_base, effective) for a member."""
        if t2_req is None and t3_req is None:
            t2_req, t3_req = await self._thresholds(db)
        cursor = await db.execute("SELECT points FROM user_points WHERE user_id = ?", (member.id,))
        row = await cursor.fetchone()
        earned = row[0] if row else 0
        vip_base = self.vip_base_points(self._alerts_cog(), member, t2_req, t3_req)
        return earned, vip_base, earned + vip_base

    async def sync_tier_role(self, member, db):
        """Compute the member's reward tier from their effective points (earned
        + any VIP floor) and hand enforcement to the Alerts & Colors cog.

        That cog owns ALL gate/color role mutations (single serialized,
        idempotent code path): it grants the new gate before removing the old,
        DMs on promotion only, and on demotion silently restores the color the
        member last chose at the tier they land on.
        """
        alerts_cog = self._alerts_cog()
        if not alerts_cog:
            return

        t2_req, t3_req = await self._thresholds(db)
        if t2_req is None and t3_req is None:
            return

        _, _, points = await self.effective_points(member, db, t2_req, t3_req)

        try:
            await alerts_cog.set_member_tier(member, self._target_tier(points, t2_req, t3_req))
        except Exception as e:
            logger.error(f"Failed to sync tier for {member.name}: {e}", exc_info=True)

    async def resync_member(self, member):
        """Recompute one member's tier now. Called by the Alerts & Colors cog
        when a VIP role is added or removed."""
        async with aiosqlite.connect(self.db_path) as db:
            await self.sync_tier_role(member, db)

    async def resync_member_id(self, guild, user_id):
        """resync_member by id; no-op if they aren't in the guild."""
        member = guild.get_member(user_id) if guild else None
        if member and not member.bot:
            await self.resync_member(member)

    async def check_role_upgrade(self, member, db):
        await self.sync_tier_role(member, db)

    # Grant lines to print before collapsing the rest into a "…and N more".
    MAX_GRANT_LINES = 25

    async def build_user_point_view(self, guild, user):
        """One member's point breakdown: totals, tier standing, and every live
        grant with the date it decays."""
        member = guild.get_member(user.id)
        today = datetime.date.today()

        async with aiosqlite.connect(self.db_path) as db:
            t2_req, t3_req = await self._thresholds(db)
            if member:
                earned, vip_base, effective = await self.effective_points(member, db, t2_req, t3_req)
            else:
                cursor = await db.execute("SELECT points FROM user_points WHERE user_id = ?", (user.id,))
                row = await cursor.fetchone()
                earned = row[0] if row else 0
                vip_base, effective = 0, earned
            cursor = await db.execute(
                "SELECT expires_at, SUM(points) FROM point_ledger "
                "WHERE user_id = ? AND decayed = 0 GROUP BY expires_at ORDER BY expires_at",
                (user.id,),
            )
            grants = await cursor.fetchall()

        name = member.display_name if member else user.display_name
        lines = [f"**Point Breakdown — {name}**"]

        totals = f"Earned **{fmt_points(earned)}**"
        if vip_base:
            totals += f" · VIP floor **+{fmt_points(vip_base)}** · Effective **{fmt_points(effective)}**"
        lines.append(totals)

        tier = self._target_tier(effective, t2_req, t3_req)
        if t3_req is not None and effective < t3_req and (t2_req is None or effective >= t2_req):
            lines.append(f"Tier **{tier}** — **{fmt_points(t3_req - effective)}** more for Tier 3.")
        elif t2_req is not None and effective < t2_req:
            lines.append(f"Tier **{tier}** — **{fmt_points(t2_req - effective)}** more for Tier 2.")
        else:
            lines.append(f"Tier **{tier}** — top tier reached.")

        active_total = sum(g[1] for g in grants)
        lines.append(f"\n**Active grants ({fmt_points(active_total)} pts)**")

        if not grants:
            lines.append("No active points — nothing on the clock.")

        for expires_at, points in grants[:self.MAX_GRANT_LINES]:
            try:
                exp = datetime.date.fromisoformat(expires_at)
            except (TypeError, ValueError):
                lines.append(f"`{fmt_points(points)} pts` — expiry unknown")
                continue
            days = (exp - today).days
            when = "expires today" if days <= 0 else f"in {days}d"
            lines.append(f"`{fmt_points(points)} pts` — {exp.strftime('%b %d')} · {when}")

        remaining = len(grants) - self.MAX_GRANT_LINES
        if remaining > 0:
            lines.append(f"…and **{remaining}** more grant date(s).")

        # Admin point removals don't touch the ledger, so the two can drift.
        # Say so rather than leaving the mismatch looking like a bug.
        if active_total != earned:
            lines.append(
                f"\n*Note: earned total (**{fmt_points(earned)}**) differs from the ledger "
                f"(**{fmt_points(active_total)}**) — usually a manual admin adjustment.*"
            )

        # VIP floor is virtual: it never lands in the ledger and never decays.
        if vip_base:
            lines.append(f"*The **+{fmt_points(vip_base)}** VIP floor never decays — it lasts as long as the role.*")

        return "\n".join(lines)

    async def repost_sticky_button(self, channel, db=None):
        """Delete old button message and repost at bottom"""
        close_db = False
        if db is None:
            db = await aiosqlite.connect(self.db_path)
            close_db = True

        try:
            cursor = await db.execute("SELECT value FROM settings WHERE key='intro_button_msg_id'")
            res = await cursor.fetchone()

            # Delete old button message
            if res:
                try:
                    old_msg = await channel.fetch_message(int(res[0]))
                    await old_msg.delete()
                except Exception:
                    pass  # Message already deleted or not found

            # Send new button
            new_msg = await channel.send("Click below to introduce yourself!", view=IntroButtonView())
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('intro_button_msg_id', ?)", (str(new_msg.id),))
            await db.commit()
        finally:
            if close_db:
                await db.close()

    @tasks.loop(hours=1)
    async def decay_task(self):
        """Runs hourly. Expires point grants whose 30-day window has passed."""
        try:
            today = datetime.date.today().isoformat()

            async with aiosqlite.connect(self.db_path) as db:
                # Find all expired, undecayed ledger entries grouped by user
                cursor = await db.execute(
                    "SELECT user_id, SUM(points) FROM point_ledger WHERE expires_at <= ? AND decayed = 0 GROUP BY user_id",
                    (today,)
                )
                decay_rows = await cursor.fetchall()

                if decay_rows:
                    # Mark those entries as decayed
                    await db.execute(
                        "UPDATE point_ledger SET decayed = 1 WHERE expires_at <= ? AND decayed = 0",
                        (today,)
                    )
                    # Subtract expired points from user totals (floor at 0)
                    for user_id, pts_to_remove in decay_rows:
                        await db.execute(
                            "UPDATE user_points SET points = MAX(0, points - ?) WHERE user_id = ?",
                            (pts_to_remove, user_id)
                        )

                # Clean up hourly_points rows older than 2 days
                cutoff_key = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d-%H")
                await db.execute("DELETE FROM hourly_points WHERE hour_key < ?", (cutoff_key,))

                await db.commit()

            if decay_rows:
                logger.info(f"Decay task: expired points for {len(decay_rows)} user(s).")

            # Sync tier roles for every affected user (may downgrade)
            if decay_rows:
                for guild in self.bot.guilds:
                    for user_id, _ in decay_rows:
                        member = guild.get_member(user_id)
                        if member:
                            async with aiosqlite.connect(self.db_path) as db:
                                await self.sync_tier_role(member, db)

            # Audit: nobody keeps a tier role without the points for it
            try:
                await self._audit_stale_tier_roles()
            except Exception as e:
                logger.error(f"Audit failed: {e}", exc_info=True)

            # Audit: nobody who reached the member level is still a newcomer
            try:
                swapped = await self.audit_member_role_swaps()
                if swapped:
                    logger.info(f"Member role audit: swapped {len(swapped)} newcomer(s).")
            except Exception as e:
                logger.error(f"Member role audit failed: {e}", exc_info=True)
        except Exception as e:
            await self.bot.error_reporter.report("Newcomer", f"decay_task: {e}")

    async def _collect_tier_rows(self, guild, t2_req, t3_req, points_map):
        """Every member the tier system tracks, with their computed target tier.

        Tracked = has a points row, holds a Tier 2/3 gate role, or holds a VIP
        role. Gate holders are included precisely so a member who no longer has
        the points (or never did) gets the role taken back; VIP holders are
        included so their permanent floor still lands even with no points row.
        """
        alerts_cog = self._alerts_cog()
        rows, seen = [], set()

        def add(member):
            if member.bot or member.id in seen:
                return
            seen.add(member.id)
            earned = points_map.get(member.id, 0)
            vip_base = self.vip_base_points(alerts_cog, member, t2_req, t3_req)
            points = earned + vip_base
            rows.append(TierAuditRow(
                member=member,
                earned=earned,
                vip_base=vip_base,
                points=points,
                current=alerts_cog.get_member_tier(member),
                target=self._target_tier(points, t2_req, t3_req),
            ))

        for user_id in points_map:
            member = guild.get_member(user_id)
            if member:
                add(member)

        for tier in (2, 3):
            role_id = alerts_cog.get_gate(guild.id, f"t{tier}")
            role = guild.get_role(role_id) if role_id else None
            if role:
                for member in list(role.members):
                    add(member)

        for member in list(alerts_cog.iter_vip_members(guild)):
            add(member)

        return rows

    async def run_tier_audit(self, guild, dry_run=False):
        """Reconcile every tracked member's tier role in one guild.

        Returns a TierAuditReport, or None if the cog/thresholds aren't set up.
        With dry_run the report lists what would change and nothing is touched.
        """
        alerts_cog = self._alerts_cog()
        if not alerts_cog:
            return None

        async with aiosqlite.connect(self.db_path) as db:
            t2_req, t3_req = await self._thresholds(db)
            if t2_req is None and t3_req is None:
                return None
            cursor = await db.execute("SELECT user_id, points FROM user_points")
            points_map = dict(await cursor.fetchall())

        rows = await self._collect_tier_rows(guild, t2_req, t3_req, points_map)
        changes = [r for r in rows if r.current != r.target]
        report = TierAuditReport(tracked=len(rows), changes=changes)
        if dry_run:
            return report

        # Enforce for every tracked member, not just the changes: set_member_tier
        # is idempotent and also repairs color drift for members already correct.
        for row in rows:
            try:
                if await alerts_cog.set_member_tier(row.member, row.target):
                    if row.current != row.target:
                        report.applied += 1
                    await asyncio.sleep(0.3)
                elif row.current != row.target:
                    report.failed += 1
            except Exception as e:
                logger.error(f"Audit: tier sync failed for {row.member.name}: {e}")
                if row.current != row.target:
                    report.failed += 1
        return report

    async def _audit_stale_tier_roles(self):
        """Hourly self-heal: recompute every tracked member's reward tier from
        their effective points (earned + VIP floor) and enforce it via the
        Alerts & Colors cog — adds missed promotions, strips tier roles whose
        points have expired, restores the right colors."""
        for guild in self.bot.guilds:
            report = await self.run_tier_audit(guild, dry_run=False)
            if report and report.changes:
                logger.info(
                    f"Tier audit ({guild.name}): {report.applied}/{len(report.changes)} "
                    f"member(s) corrected out of {report.tracked} tracked."
                )

    @decay_task.before_loop
    async def before_decay_task(self):
        await self.bot.wait_until_ready()

    async def _delete_intro_thread(self, thread_id, lore_msg_id, parent_channel_id):
        """Delete one intro thread and its Lore Drop banner.

        Deleting the banner also takes the thread with it (a thread dies with the
        message it was started from), so the thread delete below is a fallback for
        rows with no banner recorded. Anything already gone is treated as done.
        """
        if parent_channel_id and lore_msg_id:
            try:
                parent_ch = self.bot.get_channel(parent_channel_id) or await self.bot.fetch_channel(parent_channel_id)
                msg = await parent_ch.fetch_message(lore_msg_id)
                await msg.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(f"Cleanup: could not delete lore message {lore_msg_id}: {e}")

        try:
            thread = self.bot.get_channel(thread_id) or await self.bot.fetch_channel(thread_id)
            await thread.delete()
        except discord.NotFound:
            pass
        except Exception as e:
            logger.warning(f"Cleanup: could not delete intro thread {thread_id}: {e}")

    @tasks.loop(hours=1)
    async def thread_cleanup_task(self):
        """Runs hourly. Deletes intro threads older than INTRO_THREAD_LIFETIME_DAYS.

        The first run after startup also clears any backlog left from before this
        existed. The Q&A image in the intro channel is deliberately left alone.
        """
        try:
            cutoff = discord.utils.utcnow() - datetime.timedelta(days=INTRO_THREAD_LIFETIME_DAYS)

            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    "SELECT thread_id, lore_msg_id, parent_channel_id FROM intro_metadata WHERE cleaned_up = 0"
                )
                rows = await cursor.fetchall()

            # A thread's id is the id of the Lore Drop message it was created from,
            # so the snowflake gives us its creation time — no stored timestamp
            # needed, and it works for rows written before this task existed.
            expired = [r for r in rows if discord.utils.snowflake_time(r[0]) <= cutoff]
            if not expired:
                return

            for thread_id, lore_msg_id, parent_channel_id in expired:
                await self._delete_intro_thread(thread_id, lore_msg_id, parent_channel_id)
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("UPDATE intro_metadata SET cleaned_up = 1 WHERE thread_id = ?", (thread_id,))
                    # Per-thread point caps are meaningless once the thread is gone.
                    await db.execute("DELETE FROM thread_logs WHERE thread_id = ?", (thread_id,))
                    await db.commit()
                await asyncio.sleep(1)

            logger.info(
                f"Intro cleanup: deleted {len(expired)} thread(s) older than {INTRO_THREAD_LIFETIME_DAYS} days."
            )
        except Exception as e:
            await self.bot.error_reporter.report("Newcomer", f"thread_cleanup_task: {e}")

    @thread_cleanup_task.before_loop
    async def before_thread_cleanup_task(self):
        await self.bot.wait_until_ready()

    # ==================================================================
    # DASHBOARD SYNC
    # ==================================================================
    # Flat welcome keys live in the per-guild welcome_config.json; every other
    # flat key is global intro state echoed under each guild. The intro button's
    # channel is exposed as `intro_button_channel_id` so it doesn't collide with
    # welcome's own `intro_channel_id` (a greeting nudge target).
    _WELCOME_ID_KEYS = ("welcome_channel_id", "intro_channel_id", "lfg_forum_id")
    _WELCOME_BOOL_KEYS = ("suggest_introduction",)
    # schema field key -> intro-DB settings key
    _INTRO_SETTING_KEYS = {
        "intro_button_channel_id": "intro_channel_id",
        "thread_channel_id": "thread_channel_id",
        "newcomer_role": "newcomer_role",
        "member_role": "member_role",
        "member_role_level": "member_role_level",
        "reply_points": "reply_points",
        "hourly_point_cap": "hourly_point_cap",
        "welcome_msg": "welcome_msg",
    }
    _INTRO_ROLE_KEYS = ("newcomer_role", "member_role")
    # schema field key -> alerts gate tier key
    _GATE_KEYS = {"tier1_role": "t1", "tier2_role": "t2", "tier3_role": "t3"}
    # schema field key -> alerts vip tier key
    _VIP_KEYS = {"tier2_vip_roles": "t2", "tier3_vip_roles": "t3"}

    def _primary_guild(self):
        """The one server the global intro/points system is for: whichever has the
        newcomer role, else the first guild the bot is in."""
        newcomer_id = None
        raw = self._settings_cache.get("newcomer_role")
        if raw and raw is not self._settings_missing:
            try:
                newcomer_id = int(raw)
            except (TypeError, ValueError):
                newcomer_id = None
        if newcomer_id:
            for guild in self.bot.guilds:
                if guild.get_role(newcomer_id):
                    return guild
        return self.bot.guilds[0] if self.bot.guilds else None

    async def _settings_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Every guild's flat onboarding settings, flattened to the field keys.

        Welcome keys come from that guild's welcome_config; the intro/points keys
        are global and echoed under each guild; the tier/VIP roles come from the
        Alerts & Colors cog for that guild.
        """
        # Global intro settings (read once, echoed per guild).
        intro_vals: Dict[str, Any] = {}
        async with aiosqlite.connect(self.db_path) as db:
            for field_key, setting_key in self._INTRO_SETTING_KEYS.items():
                value = await self._get_setting(db, setting_key)
                if field_key in self._INTRO_ROLE_KEYS or field_key in (
                    "intro_button_channel_id", "thread_channel_id"
                ):
                    intro_vals[field_key] = str(value) if value else None
                elif field_key == "member_role_level":
                    try:
                        intro_vals[field_key] = int(value) if value else DEFAULT_MEMBER_ROLE_LEVEL
                    except (TypeError, ValueError):
                        intro_vals[field_key] = DEFAULT_MEMBER_ROLE_LEVEL
                elif field_key == "reply_points":
                    try:
                        intro_vals[field_key] = float(value) if value else 0.5
                    except (TypeError, ValueError):
                        intro_vals[field_key] = 0.5
                elif field_key == "hourly_point_cap":
                    try:
                        intro_vals[field_key] = int(value) if value else 0
                    except (TypeError, ValueError):
                        intro_vals[field_key] = 0
                else:  # welcome_msg
                    intro_vals[field_key] = value or ""
            t2_req, t3_req = await self._thresholds(db)
        intro_vals["tier2_points"] = t2_req if t2_req is not None else 0
        intro_vals["tier3_points"] = t3_req if t3_req is not None else 0

        alerts_cog = self._alerts_cog()
        out: Dict[str, Dict[str, Any]] = {}
        for guild in self.bot.guilds:
            gid = str(guild.id)
            gc = self._welcome_guild_config(guild.id)
            row: Dict[str, Any] = {
                key: (str(gc[key]) if gc.get(key) else None) for key in self._WELCOME_ID_KEYS
            }
            for key in self._WELCOME_BOOL_KEYS:
                row[key] = bool(gc.get(key))
            row.update(intro_vals)
            if alerts_cog:
                for field_key, tier_key in self._GATE_KEYS.items():
                    rid = alerts_cog.get_gate(guild.id, tier_key)
                    row[field_key] = str(rid) if rid else None
                for field_key, tier_key in self._VIP_KEYS.items():
                    row[field_key] = [str(r) for r in alerts_cog.get_vip_role_ids(guild.id, tier_key)]
            out[gid] = row
        return out

    async def _settings_apply(self, guild_id: str, values: Dict[str, Any]):
        """Apply flat settings saved on the dashboard for one guild."""
        gid = int(guild_id)

        # --- Welcome (per-guild JSON) ---
        gc = self._welcome_guild_config(gid)
        welcome_touched = False
        for key in self._WELCOME_ID_KEYS:
            if key in values:
                raw = values[key]
                gc[key] = int(raw) if raw else None
                welcome_touched = True
        for key in self._WELCOME_BOOL_KEYS:
            if key in values:
                gc[key] = bool(values[key])
                welcome_touched = True
        if welcome_touched:
            await self._save_welcome_config()

        # --- Intro/points (global settings + point_config) ---
        intro_button_changed = False
        async with aiosqlite.connect(self.db_path) as db:
            for field_key, setting_key in self._INTRO_SETTING_KEYS.items():
                if field_key not in values:
                    continue
                raw = values[field_key]
                if field_key in self._INTRO_ROLE_KEYS or field_key in (
                    "intro_button_channel_id", "thread_channel_id"
                ):
                    stored = str(int(raw)) if raw else None
                elif field_key == "member_role_level":
                    stored = str(int(raw)) if raw not in (None, "") else None
                elif field_key == "reply_points":
                    stored = str(float(raw)) if raw not in (None, "") else None
                elif field_key == "hourly_point_cap":
                    stored = str(int(raw)) if raw not in (None, "") else "0"
                else:  # welcome_msg
                    stored = str(raw) if raw else None
                if stored is None:
                    await db.execute("DELETE FROM settings WHERE key = ?", (setting_key,))
                else:
                    await db.execute(
                        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                        (setting_key, stored),
                    )
                self._invalidate_setting(setting_key)
                if field_key == "intro_button_channel_id":
                    intro_button_changed = True
            for field_key, tier in (("tier2_points", 2), ("tier3_points", 3)):
                if field_key in values:
                    try:
                        pts = int(values[field_key])
                    except (TypeError, ValueError):
                        pts = 0
                    await db.execute(
                        "INSERT OR REPLACE INTO point_config (tier, points_required) VALUES (?, ?)",
                        (tier, pts),
                    )
            await db.commit()

        # --- Tier / VIP roles (Alerts & Colors cog) ---
        alerts_cog = self._alerts_cog()
        if alerts_cog:
            for field_key, tier_key in self._GATE_KEYS.items():
                if field_key in values:
                    raw = values[field_key]
                    alerts_cog.set_gate(gid, tier_key, int(raw) if raw else None)
            for field_key, tier_key in self._VIP_KEYS.items():
                if field_key in values and hasattr(alerts_cog, "set_vip_roles"):
                    ids = [int(r) for r in (values[field_key] or []) if str(r).isdigit()]
                    alerts_cog.set_vip_roles(gid, tier_key, ids)
            guild = self.bot.get_guild(gid)
            if guild:
                try:
                    await self.run_tier_audit(guild)
                except Exception as e:
                    logger.error(f"Onboarding: tier audit after save failed: {e}", exc_info=True)

        # Re-post the intro button when its channel is (re)assigned.
        if intro_button_changed:
            await self._repost_intro_button_from_settings()

    async def _repost_intro_button_from_settings(self):
        """Post the Introduce Yourself button into the configured intro channel."""
        channel_id = await self._setting("intro_channel_id")
        if not channel_id:
            return
        try:
            cid = int(channel_id)
        except (TypeError, ValueError):
            return
        channel = self.bot.get_channel(cid) or await self.bot.fetch_channel(cid)
        if channel:
            try:
                await self.repost_sticky_button(channel)
            except Exception as e:
                logger.error(f"Onboarding: could not repost intro button: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Imperative bridge (questions / blacklist / points / mappings)
    # ------------------------------------------------------------------
    @tasks.loop(seconds=10)
    async def onboarding_sync_task(self):
        try:
            revision, commands = await self._obridge.read_commands()
            if commands:
                for cmd in commands:
                    try:
                        await self._apply_command(cmd)
                    except Exception as e:
                        logger.error(
                            f"Onboarding: failed to apply command {cmd.get('type')}: {e}",
                            exc_info=True,
                        )
                    await self._obridge.mark_command_done(cmd["id"])
                await self._obridge.mark_applied(revision)

            await self._obridge.publish_snapshot(await self._build_bridge_snapshot())

            # A settings change made from a Discord role edit or the like should
            # also show on the website within a tick.
            self._settings_sync.mark_dirty()
        except Exception as e:
            try:
                await self.bot.error_reporter.report("Onboarding", f"onboarding_sync_task: {e}")
            except Exception:
                pass

    @onboarding_sync_task.before_loop
    async def before_onboarding_sync_task(self):
        await self._db_ready.wait()
        await self.bot.wait_until_ready()

    async def _apply_command(self, cmd: Dict[str, Any]):
        ctype = cmd["type"]
        payload = cmd.get("payload", {})

        if ctype == "add_question":
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT COALESCE(MAX(order_num), 0) FROM questions")
                max_order = (await cursor.fetchone())[0]
                await db.execute(
                    "INSERT INTO questions (text, style, order_num, is_optional) VALUES (?, ?, ?, ?)",
                    (
                        str(payload.get("text", "")).strip(),
                        "long" if payload.get("style") == "long" else "short",
                        max_order + 1,
                        1 if payload.get("is_optional") else 0,
                    ),
                )
                await db.commit()

        elif ctype == "edit_question":
            qid = int(payload["id"])
            sets, args = [], []
            if "text" in payload:
                sets.append("text = ?"); args.append(str(payload["text"]).strip())
            if "style" in payload:
                sets.append("style = ?"); args.append("long" if payload["style"] == "long" else "short")
            if "is_optional" in payload:
                sets.append("is_optional = ?"); args.append(1 if payload["is_optional"] else 0)
            if sets:
                args.append(qid)
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(f"UPDATE questions SET {', '.join(sets)} WHERE id = ?", args)
                    await db.commit()

        elif ctype == "delete_question":
            qid = int(payload["id"])
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT order_num FROM questions WHERE id = ?", (qid,))
                row = await cursor.fetchone()
                await db.execute("DELETE FROM questions WHERE id = ?", (qid,))
                if row and payload.get("shift"):
                    await db.execute(
                        "UPDATE questions SET order_num = order_num - 1 WHERE order_num > ?",
                        (row[0],),
                    )
                await db.commit()

        elif ctype == "reorder_questions":
            order = [int(i) for i in payload.get("order", [])]
            async with aiosqlite.connect(self.db_path) as db:
                for idx, qid in enumerate(order, start=1):
                    await db.execute("UPDATE questions SET order_num = ? WHERE id = ?", (idx, qid))
                await db.commit()

        elif ctype == "blacklist_add":
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO blacklist (user_id) VALUES (?)", (int(payload["user_id"]),)
                )
                await db.commit()

        elif ctype == "blacklist_remove":
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM blacklist WHERE user_id = ?", (int(payload["user_id"]),))
                await db.commit()

        elif ctype == "points_adjust":
            await self._apply_points_adjust(payload)

        elif ctype == "points_wipe":
            uid = int(payload["user_id"])
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM user_points WHERE user_id = ?", (uid,))
                await db.execute("DELETE FROM point_ledger WHERE user_id = ?", (uid,))
                await db.execute("DELETE FROM thread_logs WHERE user_id = ?", (uid,))
                await db.commit()
            guild = self._primary_guild()
            if guild:
                await self.resync_member_id(guild, uid)

        elif ctype == "wipe_all":
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("DELETE FROM user_points")
                await db.execute("DELETE FROM point_ledger")
                await db.execute("DELETE FROM thread_logs")
                await db.commit()

        elif ctype == "mapping_add":
            gc = self._welcome_guild_config(int(payload["guild_id"]))
            gc.setdefault("game_mappings", {})[str(int(payload["role_id"]))] = {
                "thread_id": int(payload["thread_id"]),
            }
            await self._save_welcome_config()

        elif ctype == "mapping_remove":
            gc = self._welcome_guild_config(int(payload["guild_id"]))
            gc.get("game_mappings", {}).pop(str(int(payload["role_id"])), None)
            await self._save_welcome_config()

        else:
            logger.warning(f"Onboarding: unknown command type {ctype!r}")

    async def _apply_points_adjust(self, payload: Dict[str, Any]):
        """Add / remove / set a member's earned points, mirroring the old panel's
        ledger handling exactly, then re-sync their tier role."""
        uid = int(payload["user_id"])
        action = payload.get("action", "add")
        try:
            pts = int(payload.get("amount", 0))
        except (TypeError, ValueError):
            pts = 0
        if pts < 0:
            pts = 0

        earned_at = datetime.date.today().isoformat()
        expires_at = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            if action == "add":
                await db.execute(
                    "INSERT INTO user_points (user_id, points) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET points = points + ?",
                    (uid, pts, pts),
                )
                await db.execute(
                    "INSERT INTO point_ledger (user_id, points, earned_at, expires_at) VALUES (?, ?, ?, ?)",
                    (uid, pts, earned_at, expires_at),
                )
            elif action == "remove":
                await db.execute(
                    "UPDATE user_points SET points = MAX(0, points - ?) WHERE user_id = ?",
                    (pts, uid),
                )
            else:  # set
                await db.execute("DELETE FROM point_ledger WHERE user_id = ?", (uid,))
                await db.execute(
                    "INSERT INTO user_points (user_id, points) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET points = ?",
                    (uid, pts, pts),
                )
                if pts > 0:
                    await db.execute(
                        "INSERT INTO point_ledger (user_id, points, earned_at, expires_at) VALUES (?, ?, ?, ?)",
                        (uid, pts, earned_at, expires_at),
                    )
            await db.commit()
        guild = self._primary_guild()
        if guild:
            await self.resync_member_id(guild, uid)

    # ------------------------------------------------------------------
    # Read model published for the dashboard (the bot is the only side that can
    # compute VIP floors from Discord roles, so it renders every view here).
    # ------------------------------------------------------------------
    async def _build_bridge_snapshot(self) -> Dict[str, Any]:
        guild = self._primary_guild()
        alerts_cog = self._alerts_cog()

        def name_for(uid: int) -> str:
            member = guild.get_member(uid) if guild else None
            return member.display_name if member else f"User {uid}"

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, text, style, order_num, is_optional FROM questions ORDER BY order_num"
            )
            questions = [dict(r) for r in await cursor.fetchall()]

            cursor = await db.execute("SELECT user_id FROM blacklist")
            blacklist = [
                {"user_id": str(r[0]), "name": name_for(r[0])} for r in await cursor.fetchall()
            ]

            db.row_factory = None
            t2_req, t3_req = await self._thresholds(db)
            reply_pts = await self._get_setting(db, "reply_points")
            hourly_cap = await self._get_setting(db, "hourly_point_cap")

            cursor = await db.execute("SELECT user_id, points FROM user_points")
            earned_map = {r[0]: r[1] for r in await cursor.fetchall()}

            # Time-windowed earned totals from the decay ledger.
            today = datetime.date.today()
            d30 = (today - datetime.timedelta(days=30)).isoformat()
            d60 = (today - datetime.timedelta(days=60)).isoformat()
            cursor = await db.execute(
                "SELECT user_id, SUM(points) FROM point_ledger WHERE earned_at >= ? "
                "GROUP BY user_id HAVING SUM(points) > 0 ORDER BY SUM(points) DESC", (d30,)
            )
            list_30 = await cursor.fetchall()
            cursor = await db.execute(
                "SELECT user_id, SUM(points) FROM point_ledger WHERE earned_at >= ? AND earned_at < ? "
                "GROUP BY user_id HAVING SUM(points) > 0 ORDER BY SUM(points) DESC", (d60, d30)
            )
            list_60 = await cursor.fetchall()
            cursor = await db.execute(
                "SELECT user_id, SUM(points) FROM point_ledger "
                "GROUP BY user_id HAVING SUM(points) > 0 ORDER BY SUM(points) DESC"
            )
            list_all = await cursor.fetchall()

            # Live grants per user, for the per-member view.
            cursor = await db.execute(
                "SELECT user_id, expires_at, SUM(points) FROM point_ledger WHERE decayed = 0 "
                "GROUP BY user_id, expires_at ORDER BY expires_at"
            )
            grants_rows = await cursor.fetchall()

        grants_by_user: Dict[int, list] = {}
        for uid, expires_at, pts in grants_rows:
            grants_by_user.setdefault(uid, []).append({"expires_at": expires_at, "points": pts})

        def window_list(rows):
            return [{"user_id": str(uid), "name": name_for(uid), "points": pts} for uid, pts in rows]

        # Members with a points row or a VIP role, with their effective standing.
        member_ids = set(earned_map)
        if alerts_cog and guild:
            for member in list(alerts_cog.iter_vip_members(guild)):
                member_ids.add(member.id)

        members = {}
        upgrade_rows = []
        for uid in member_ids:
            member = guild.get_member(uid) if guild else None
            earned = earned_map.get(uid, 0)
            vip_base = self.vip_base_points(alerts_cog, member, t2_req, t3_req) if member else 0
            effective = earned + vip_base
            if earned <= 0 and vip_base <= 0:
                continue
            tier = self._target_tier(effective, t2_req, t3_req)
            members[str(uid)] = {
                "name": name_for(uid),
                "earned": earned,
                "vip_base": vip_base,
                "effective": effective,
                "tier": tier,
                "grants": grants_by_user.get(uid, []),
            }
            upgrade_rows.append({
                "user_id": str(uid), "name": name_for(uid),
                "earned": earned, "vip_base": vip_base, "effective": effective,
            })
        upgrade_rows.sort(key=lambda r: r["effective"], reverse=True)

        # Per-guild game→LFG mappings.
        mappings: Dict[str, list] = {}
        for gid_str, gc in self.welcome_config.items():
            rows = []
            for role_id_str, mapping in (gc.get("game_mappings") or {}).items():
                role = None
                g = self.bot.get_guild(int(gid_str)) if gid_str.isdigit() else None
                if g:
                    role = g.get_role(int(role_id_str))
                rows.append({
                    "role_id": role_id_str,
                    "role_name": role.name if role else f"Role {role_id_str}",
                    "thread_id": str(mapping.get("thread_id")),
                })
            if rows:
                mappings[gid_str] = rows

        return {
            "questions": questions,
            "blacklist": blacklist,
            "members": members,
            "lists": {
                "30days": window_list(list_30),
                "60days": window_list(list_60),
                "alltime": window_list(list_all),
                "upgrade": upgrade_rows,
            },
            "mappings": mappings,
            "meta": {
                "tier2_points": t2_req,
                "tier3_points": t3_req,
                "reply_points": reply_pts,
                "hourly_point_cap": hourly_cap,
                "primary_guild_id": str(guild.id) if guild else None,
            },
        }

# --- UI CLASSES ---

class DynamicIntroModal(ui.Modal):
    def __init__(self, cog, questions):
        super().__init__(title="Introduce Yourself")
        self.cog = cog
        self.questions = questions # List of (text, style, is_optional)
        
        for q_text, q_style, is_opt in questions:
            style = discord.TextStyle.paragraph if q_style == 'long' else discord.TextStyle.short
            required = True
            label = q_text
            placeholder = None
            
            if is_opt:
                required = False
                placeholder = "(Optional)"
            
            self.add_item(ui.TextInput(label=label[:45], style=style, required=required, placeholder=placeholder, max_length=1000))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # 1. FIX: Grab IGN from first field explicitly, before filtering empty optionals
        # The list self.children corresponds exactly to questions order
        ign = "Newcomer"
        if self.children:
            ign = self.children[0].value.strip() or "Newcomer"

        # 2. Filter Answers (Skip empty optionals) for the image
        answers = []
        for i, item in enumerate(self.children):
            if item.value and item.value.strip():
                answers.append((item.label, item.value))
        
        # If absolutely everything is empty (shouldn't happen if name is req)
        if not answers:
             return await interaction.followup.send("You didn't fill anything out!", ephemeral=True)

        try:
            # Fetch user's avatar
            avatar_bytes = None
            try:
                avatar_asset = interaction.user.display_avatar
                avatar_bytes = await avatar_asset.read()
            except Exception:
                pass  # Will use placeholder if avatar fetch fails

            username = interaction.user.display_name

            # Get and increment the rotating color index
            async with aiosqlite.connect(self.cog.db_path) as db:
                cursor = await db.execute("SELECT value FROM settings WHERE key='color_index'")
                res = await cursor.fetchone()
                color_index = int(res[0]) if res else 0
                # Increment for next user
                next_index = (color_index + 1) % len(ACCENT_COLORS)
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('color_index', ?)", (str(next_index),))
                await db.commit()

            lore_img = await self.cog.bot.loop.run_in_executor(None, self.cog.generate_lore_banner, ign, color_index)
            qa_img = await self.cog.bot.loop.run_in_executor(
                None, self.cog.generate_qa_image, answers, avatar_bytes, username, color_index
            )
            
            # 3. FIX: BytesIO Context Manager Bug
            # Create buffers independently first (No nesting context managers for the send)
            bin_lore = io.BytesIO()
            lore_img.save(bin_lore, "PNG")
            bin_lore.seek(0)
            
            bin_qa = io.BytesIO()
            qa_img.save(bin_qa, "PNG")
            bin_qa.seek(0)
            
            # Create discord Files from the open buffers
            file_lore = discord.File(bin_lore, filename="lore.png")
            file_qa = discord.File(bin_qa, filename="qa.png")

            async with aiosqlite.connect(self.cog.db_path) as db:
                # Get Parent Channel
                cursor = await db.execute("SELECT value FROM settings WHERE key='thread_channel_id'")
                res = await cursor.fetchone()
                if not res: return await interaction.followup.send("Thread Parent Channel not configured!")
                
                parent_channel_id = int(res[0])
                parent_channel = interaction.guild.get_channel(parent_channel_id)
                if not parent_channel:
                    # Try fetch
                    try: parent_channel = await interaction.guild.fetch_channel(parent_channel_id)
                    except Exception: return await interaction.followup.send("Thread Parent Channel invalid!")

                # Send Hook (Lore Drop)
                hook_msg = await parent_channel.send(file=file_lore)
                
                # Create Thread (ID = hook_msg.id)
                thread = await hook_msg.create_thread(name=f"Welcome {ign}!", auto_archive_duration=60) # 1h
                
                # Send Welcome in Thread
                cursor = await db.execute("SELECT value FROM settings WHERE key='welcome_msg'")
                w_res = await cursor.fetchone()
                
                # Default Message
                w_default = (
                    "Thanks for the Lore Drop, {username}!\n\n"
                    "We’ve pinned your intro here so the welcome wagon can say hello without it getting lost in the main chat scroll.\n\n"
                    "**This space is totally optional.** Feel free to chat here, or if you prefer to just jump into the main channels, "
                    "you can delete this thread instantly using the **Close Thread** button below.\n\n"
                    f"*This thread is automatically deleted after {INTRO_THREAD_LIFETIME_DAYS} days — your intro post stays up.*"
                )
                
                w_msg = w_res[0] if w_res else w_default
                w_msg = w_msg.replace("{username}", interaction.user.mention)

                close_view = CloseThreadView()
                await thread.send(content=w_msg, file=file_qa, view=close_view)

                # Send Q&A to intro channel as well
                cursor = await db.execute("SELECT value FROM settings WHERE key='intro_channel_id'")
                intro_res = await cursor.fetchone()
                intro_channel = None
                if intro_res:
                    intro_channel = interaction.guild.get_channel(int(intro_res[0]))
                    if not intro_channel:
                        try: intro_channel = await interaction.guild.fetch_channel(int(intro_res[0]))
                        except Exception: intro_channel = None

                    if intro_channel:
                        # Re-create file since discord.File can only be used once
                        bin_qa2 = io.BytesIO()
                        qa_img.save(bin_qa2, "PNG")
                        bin_qa2.seek(0)
                        file_qa2 = discord.File(bin_qa2, filename="qa.png")
                        qa_intro_msg = await intro_channel.send(f"**{ign}** just introduced themselves!", file=file_qa2)

                        # Save Metadata for Deletion (includes Q&A message in intro channel)
                        await db.execute('''
                            INSERT OR REPLACE INTO intro_metadata (thread_id, user_id, lore_msg_id, parent_channel_id, qa_msg_id, intro_channel_id)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (thread.id, interaction.user.id, hook_msg.id, parent_channel.id, qa_intro_msg.id, intro_channel.id))
                        await db.commit()

                        # Repost sticky button
                        await self.cog.repost_sticky_button(intro_channel, db)
                    else:
                        # Intro channel found but not accessible, save metadata without Q&A message ID
                        await db.execute('''
                            INSERT OR REPLACE INTO intro_metadata (thread_id, user_id, lore_msg_id, parent_channel_id, qa_msg_id, intro_channel_id)
                            VALUES (?, ?, ?, ?, NULL, NULL)
                        ''', (thread.id, interaction.user.id, hook_msg.id, parent_channel.id))
                        await db.commit()
                else:
                    # No intro channel configured at all, save metadata without Q&A message ID
                    await db.execute('''
                        INSERT OR REPLACE INTO intro_metadata (thread_id, user_id, lore_msg_id, parent_channel_id, qa_msg_id, intro_channel_id)
                        VALUES (?, ?, ?, ?, NULL, NULL)
                    ''', (thread.id, interaction.user.id, hook_msg.id, parent_channel.id))
                    await db.commit()

                # Signal for other cogs (the Utility automations engine listens
                # for this). Scalar ids, guild first, matching the convention
                # already used by member_level_up.
                self.cog.bot.dispatch(
                    "newcomer_intro_posted", interaction.guild.id,
                    interaction.user.id, thread.id, ign)

                await interaction.followup.send("✅ Introduction posted!", ephemeral=True)

        except Exception as e:
            logger.error(f"Intro Fail: {e}", exc_info=True)
            await interaction.followup.send("Failed to generate intro.", ephemeral=True)


# --- THREAD CONTROL VIEWS ---
class CloseThreadView(ui.View):
    def __init__(self):
        super().__init__(timeout=None) # Persistent
    
    @ui.button(label="Close Thread", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="close_thread_btn")
    async def close_btn(self, interaction, button):
        pass # Handled in Cog listener

class CloseThreadConfirmView(ui.View):
    def __init__(self, cog, lore_msg_id, parent_channel_id):
        super().__init__(timeout=60)
        self.cog = cog
        self.lore_msg_id = lore_msg_id
        self.parent_channel_id = parent_channel_id

    @ui.button(label="Yes, Close it", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction, button):
        await interaction.response.defer()
        
        # 1. Delete Lore Message
        if self.parent_channel_id and self.lore_msg_id:
            try:
                parent_ch = interaction.guild.get_channel(self.parent_channel_id)
                if not parent_ch:
                    parent_ch = await interaction.guild.fetch_channel(self.parent_channel_id)
                
                msg = await parent_ch.fetch_message(self.lore_msg_id)
                await msg.delete()
            except Exception as e:
                logger.warning(f"Could not delete lore message: {e}")

        # 2. Delete Thread
        try:
            await interaction.channel.delete()
        except Exception as e:
            await interaction.followup.send(f"Failed to delete thread: {e}", ephemeral=True)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction, button):
        await interaction.response.edit_message(content="Cancelled.", view=None)


# --- INTRO BUTTON (member-facing; posted into the intro channel) ---
class IntroButtonView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="Introduce Yourself", style=discord.ButtonStyle.success, emoji="👋", custom_id="start_intro_modal")
    async def intro_btn(self, interaction, button):
        pass  # Handled in the cog's on_interaction listener


async def setup(bot):
    await bot.add_cog(Onboarding(bot))

