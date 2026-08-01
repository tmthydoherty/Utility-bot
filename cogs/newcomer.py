import discord
from discord import app_commands, ui
from discord.ext import commands, tasks
import aiosqlite
import asyncio
import logging
import io
import textwrap
import datetime
from dataclasses import dataclass
from PIL import Image, ImageDraw, ImageFont
import os

# --- CONFIGURATION ---
DB_NAME = "intro_system.db"
# How long an intro discussion thread lives before the bot deletes it (with its
# Lore Drop banner). The Q&A post in the intro channel is kept as the record.
INTRO_THREAD_LIFETIME_DAYS = 7
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


class IntroCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = DB_NAME
        # In-memory cache for hot settings read on every message.
        # Populated lazily by _get_setting and invalidated by _set_setting.
        # Sentinel distinguishes "cached as missing" from "not yet cached".
        self._settings_cache = {}
        self._settings_missing = object()
        self.bot.loop.create_task(self.init_db())
        self.decay_task.start()
        self.thread_cleanup_task.start()

    def cog_unload(self):
        self.decay_task.cancel()
        self.thread_cleanup_task.cancel()

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
            
            await db.commit()

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

    # --- ADMIN PANEL ---
    @app_commands.command(name="newcomer_panel", description="Admin: Configure the Intro System")
    async def newcomer_panel(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("Admin access only.", ephemeral=True)

        view = AdminPanelView(self)
        await interaction.response.send_message("**Newcomer Panel**\nSelect a module to configure:", view=view, ephemeral=True)

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
            async with aiosqlite.connect(self.db_path) as db:
                thread_parent_setting = await self._get_setting(db, 'thread_channel_id')
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

            if (
                replied_to
                and not replied_to.author.bot
                and replied_to.author.id != message.author.id
            ):
                # Resolve the target as a Member via the guild so we can read .roles.
                # replied_to.author may be a User, not a Member.
                target_member = message.guild.get_member(replied_to.author.id)

                async with aiosqlite.connect(self.db_path) as db:
                    role_value = await self._get_setting(db, 'newcomer_role')
                    if role_value and target_member:
                        try:
                            vip_role_id = int(role_value)
                        except ValueError:
                            vip_role_id = None

                        if vip_role_id and any(r.id == vip_role_id for r in target_member.roles):
                            pts_value = await self._get_setting(db, 'reply_points')
                            try:
                                reply_pts = float(pts_value) if pts_value else 0.5
                            except ValueError:
                                reply_pts = 0.5

                            # Apply hourly cap to reply points too
                            cap_value = await self._get_setting(db, 'hourly_point_cap')
                            try:
                                hourly_cap = int(cap_value) if cap_value else 0
                            except ValueError:
                                hourly_cap = 0

                            hour_key = datetime.datetime.now().strftime("%Y-%m-%d-%H")
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
                                logger.debug(
                                    f"VIP reply: awarded {award} to {message.author} for replying to {target_member}"
                                )
                                await self.check_role_upgrade(message.author, db)

        # Only process messages in threads for thread-points branch
        if not isinstance(message.channel, discord.Thread):
            return

        # Intro Thread Points Logic
        async with aiosqlite.connect(self.db_path) as db:
            thread_parent_setting = await self._get_setting(db, 'thread_channel_id')
            if not thread_parent_setting:
                return
            try:
                thread_parent_id = int(thread_parent_setting)
            except ValueError:
                return

            # Must be a thread under the intro channel
            if message.channel.parent_id != thread_parent_id:
                return

            if not message.content or len(message.content) < 3:
                return

            # Filter out 1-word messages (low effort)
            if len(message.content.split()) < 2:
                return

            user_id = message.author.id
            thread_id = message.channel.id

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

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        """Clean up all intro data when a user leaves the server"""
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

                await interaction.followup.send("✅ Introduction posted!", ephemeral=True)

        except Exception as e:
            logger.error(f"Intro Fail: {e}", exc_info=True)
            await interaction.followup.send("Failed to generate intro.", ephemeral=True)


class RegenerateIntroModal(ui.Modal, title="Regenerate Intro"):
    """Modal for admins to regenerate an existing intro with new avatar format.
    Format: Q1|A1;;Q2|A2;;Q3|A3 (use ;; to separate Q&A pairs, | to separate Q from A)
    """
    qa_data = ui.TextInput(
        label="Q&A Data (Q1|A1;;Q2|A2;;...)",
        style=discord.TextStyle.paragraph,
        placeholder="What's your name?|OG;;What games?|Valorant;;Hot take?|Wolves are cool",
        max_length=2000
    )

    def __init__(self, cog, user: discord.Member, message_id: str, intro_channel_id: int):
        super().__init__()
        self.cog = cog
        self.user = user
        self.message_id = message_id
        self.intro_channel_id = intro_channel_id

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        # Parse Q&A data: Q1|A1;;Q2|A2;;Q3|A3
        qa_list = []
        pairs = self.qa_data.value.split(";;")
        for pair in pairs:
            if "|" in pair:
                parts = pair.split("|", 1)
                if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                    qa_list.append((parts[0].strip(), parts[1].strip()))

        if not qa_list:
            return await interaction.followup.send("You need at least one Q&A pair.", ephemeral=True)

        try:
            # Fetch user's avatar
            avatar_bytes = None
            try:
                avatar_asset = self.user.display_avatar
                avatar_bytes = await avatar_asset.read()
            except Exception:
                pass

            username = self.user.display_name

            # Get current color index for this regeneration (use next in rotation)
            async with aiosqlite.connect(self.cog.db_path) as db:
                cursor = await db.execute("SELECT value FROM settings WHERE key='color_index'")
                res = await cursor.fetchone()
                color_index = int(res[0]) if res else 0

            # Generate new image
            qa_img = await self.cog.bot.loop.run_in_executor(
                None, self.cog.generate_qa_image, qa_list, avatar_bytes, username, color_index
            )

            bin_qa = io.BytesIO()
            qa_img.save(bin_qa, "PNG")
            bin_qa.seek(0)
            file_qa = discord.File(bin_qa, filename="qa.png")

            # Get the intro channel and edit the message
            intro_channel = interaction.guild.get_channel(self.intro_channel_id)
            if not intro_channel:
                intro_channel = await interaction.guild.fetch_channel(self.intro_channel_id)

            try:
                msg = await intro_channel.fetch_message(int(self.message_id))
                # Delete old message and send new one (can't edit attachments)
                await msg.delete()
                ign = qa_list[0][1].split()[0] if qa_list else username  # Use first word of first answer as IGN
                await intro_channel.send(f"**{username}** just introduced themselves!", file=file_qa)
                await interaction.followup.send(f"Intro regenerated for {self.user.mention}!", ephemeral=True)
            except discord.NotFound:
                return await interaction.followup.send(f"Message ID {self.message_id} not found in the intro channel.", ephemeral=True)

        except Exception as e:
            logger.error(f"Regenerate Intro Fail: {e}", exc_info=True)
            await interaction.followup.send(f"Failed to regenerate: {e}", ephemeral=True)


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

# --- ADMIN VIEWS ---
class AddQuestionModal(ui.Modal, title="Add Question"):
    order = ui.TextInput(label="Order (1-5)", max_length=1)
    q_text = ui.TextInput(label="Question Text", max_length=45)
    style_in = ui.TextInput(label="Style (short/long)", placeholder="short/long")
    is_opt = ui.TextInput(label="Optional?", placeholder="yes/no", max_length=3)

    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction):
        if self.style_in.value.lower() not in ['short', 'long']:
            return await interaction.response.send_message("Style must be 'short' or 'long'", ephemeral=True)
        
        is_optional = 1 if self.is_opt.value.lower() in ['yes', 'y', 'true'] else 0

        try:
            o_num = int(self.order.value)
            if not 1 <= o_num <= 5: raise ValueError
        except Exception:
             return await interaction.response.send_message("Order must be 1-5", ephemeral=True)

        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT INTO questions (text, style, order_num, is_optional) VALUES (?, ?, ?, ?)", 
                             (self.q_text.value, self.style_in.value.lower(), o_num, is_optional))
            await db.commit()
        await interaction.response.send_message("Question added.", ephemeral=True)

class AdminPanelView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @ui.button(label="Channels", style=discord.ButtonStyle.primary, row=0)
    async def channels_btn(self, interaction, button):
        await interaction.response.send_message("Select Channels:", view=ChannelSelectView(self.cog), ephemeral=True)

    @ui.button(label="Questions", style=discord.ButtonStyle.primary, row=0)
    async def questions_btn(self, interaction, button):
        await interaction.response.send_message("Manage Questions:", view=QuestionManagerView(self.cog), ephemeral=True)

    @ui.button(label="Newcomer Role", style=discord.ButtonStyle.secondary, row=0)
    async def newcomer_role_btn(self, interaction, button):
        await interaction.response.send_message("Select Newcomer Role to Track:", view=NewcomerRoleSelectView(self.cog), ephemeral=True)

    @ui.button(label="Tier / VIP Roles", style=discord.ButtonStyle.secondary, row=1)
    async def roles_btn(self, interaction, button):
        alerts_cog = self.cog._alerts_cog()
        if not alerts_cog:
            return await interaction.response.send_message(
                "The Alerts & Colors cog is not loaded.", ephemeral=True
            )
        await interaction.response.send_message(
            "Shared with the Alerts & Colors panel. **Tier 2/3 roles** are awarded by points; "
            "**VIP roles** are a permanent point floor worth their tier's threshold, which "
            "earned points stack on top of. Selecting nothing in a VIP menu clears it.",
            view=alerts_cog.make_gate_view(),
            ephemeral=True,
        )

    @ui.button(label="Points/Tiers", style=discord.ButtonStyle.secondary, row=1)
    async def points_btn(self, interaction, button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            cursor = await db.execute("SELECT tier, points_required FROM point_config")
            rows = await cursor.fetchall()
            pts = {2: 0, 3: 0}
            for t, p in rows:
                if t in pts:
                    pts[t] = p
            cursor = await db.execute("SELECT value FROM settings WHERE key = 'reply_points'")
            res = await cursor.fetchone()
            reply_pts = res[0] if res else "0.5"

        await interaction.response.send_modal(PointThresholdModal(self.cog, pts[2], pts[3], reply_pts))

    @ui.button(label="Hourly Cap", style=discord.ButtonStyle.secondary, row=1)
    async def hourly_cap_btn(self, interaction, button):
        await interaction.response.send_modal(HourlyCapModal(self.cog))
    
    @ui.button(label="Blacklist", style=discord.ButtonStyle.danger, row=2)
    async def bl_btn(self, interaction, button):
        await interaction.response.send_message("Select User to Block:", view=UserSelectView(self.cog, "blacklist"), ephemeral=True)

    @ui.button(label="Point List", style=discord.ButtonStyle.secondary, row=2)
    async def history_btn(self, interaction, button):
        view = PointListView(self.cog, interaction.guild)
        await view.load_data("30days")
        await interaction.response.send_message(content=await view.build_page(), view=view, ephemeral=True)

    @ui.button(label="User Point View", style=discord.ButtonStyle.secondary, row=3)
    async def user_points_btn(self, interaction, button):
        await interaction.response.send_message(
            "Select a member to see their active points and when each grant expires:",
            view=UserPointSelectView(self.cog),
            ephemeral=True,
        )

    @ui.button(label="Welcome Message", style=discord.ButtonStyle.success, row=2)
    async def msg_btn(self, interaction, button):
        await interaction.response.send_modal(WelcomeMsgModal(self.cog))

    @ui.button(label="Wipe Points", style=discord.ButtonStyle.danger, row=3)
    async def wipe_btn(self, interaction, button):
        await interaction.response.send_message("**Wipe Points**\nChoose an option:", view=WipeOptionsView(self.cog), ephemeral=True)

class IntroButtonView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @ui.button(label="Introduce Yourself", style=discord.ButtonStyle.success, emoji="👋", custom_id="start_intro_modal")
    async def intro_btn(self, interaction, button): pass

class ChannelSelectView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
    
    @ui.select(cls=ui.ChannelSelect, placeholder="Select Intro Channel (Button location)", min_values=1, max_values=1)
    async def sel_intro(self, interaction, select):
        ch_partial = select.values[0]
        # FIX: Fetch the full channel object to access .send()
        ch = interaction.guild.get_channel(ch_partial.id)
        if not ch:
            try: 
                ch = await interaction.guild.fetch_channel(ch_partial.id)
            except Exception: 
                return await interaction.response.send_message("Error fetching channel", ephemeral=True)

        try:
            btn_msg = await ch.send("Click below to introduce yourself!", view=IntroButtonView())
            async with aiosqlite.connect(self.cog.db_path) as db:
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('intro_channel_id', ?)", (str(ch.id),))
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('intro_button_msg_id', ?)", (str(btn_msg.id),))
                await db.commit()
            await interaction.response.send_message(f"Button sent to {ch.mention}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error: {e}", ephemeral=True)

    @ui.select(cls=ui.ChannelSelect, placeholder="Select Thread Parent Channel", min_values=1, max_values=1)
    async def sel_parent(self, interaction, select):
        ch_partial = select.values[0]
        ch = interaction.guild.get_channel(ch_partial.id)
        if not ch:
             try: ch = await interaction.guild.fetch_channel(ch_partial.id)
             except Exception: ch = ch_partial 

        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('thread_channel_id', ?)", (str(ch.id),))
            await db.commit()
        self.cog._invalidate_setting('thread_channel_id')
        await interaction.response.send_message(f"Thread Parent set to {ch.mention}", ephemeral=True)

class QuestionManagerView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @ui.button(label="Add Question", style=discord.ButtonStyle.success)
    async def add_q(self, interaction, button):
        await interaction.response.send_modal(AddQuestionModal(self.cog))

    @ui.button(label="Delete Question", style=discord.ButtonStyle.danger)
    async def del_q(self, interaction, button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            cursor = await db.execute("SELECT id, text, order_num FROM questions ORDER BY order_num")
            qs = await cursor.fetchall()
        
        if not qs:
            return await interaction.response.send_message("No questions to delete.", ephemeral=True)
        view = DeleteQuestionSelectView(self.cog, qs)
        await interaction.response.send_message("Select question to delete:", view=view, ephemeral=True)

class DeleteQuestionSelectView(ui.View):
    def __init__(self, cog, questions):
        super().__init__()
        self.cog = cog
        options = [discord.SelectOption(label=f"{q[2]}. {q[1][:20]}", value=str(q[0])) for q in questions]
        self.select = ui.Select(placeholder="Choose question...", options=options)
        self.select.callback = self.callback
        self.add_item(self.select)
    
    async def callback(self, interaction):
        q_id = int(self.select.values[0])
        await interaction.response.send_message(f"Question {q_id} selected. Shift remaining questions up?", 
                                                view=ShiftConfirmView(self.cog, q_id), ephemeral=True)

class ShiftConfirmView(ui.View):
    def __init__(self, cog, q_id):
        super().__init__()
        self.cog = cog
        self.q_id = q_id

    @ui.button(label="Yes, Shift", style=discord.ButtonStyle.success)
    async def yes(self, interaction, button): await self.execute_delete(interaction, shift=True)

    @ui.button(label="No, Just Delete", style=discord.ButtonStyle.secondary)
    async def no(self, interaction, button): await self.execute_delete(interaction, shift=False)

    async def execute_delete(self, interaction, shift):
        async with aiosqlite.connect(self.cog.db_path) as db:
            cursor = await db.execute("SELECT order_num FROM questions WHERE id = ?", (self.q_id,))
            res = await cursor.fetchone()
            if not res: return
            del_order = res[0]
            await db.execute("DELETE FROM questions WHERE id = ?", (self.q_id,))
            if shift:
                await db.execute("UPDATE questions SET order_num = order_num - 1 WHERE order_num > ?", (del_order,))
            await db.commit()
        await interaction.response.send_message("Deleted and updated.", ephemeral=True)

class NewcomerRoleSelectView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @ui.select(cls=ui.RoleSelect, placeholder="Select Newcomer Role", min_values=1, max_values=1)
    async def nc_role(self, interaction, select):
        role = select.values[0]
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('newcomer_role', ?)", (str(role.id),))
            await db.commit()
        self.cog._invalidate_setting('newcomer_role')
        await interaction.response.send_message(f"Newcomer role set to {role.name}", ephemeral=True)

class PointThresholdModal(ui.Modal, title="Points Required per Tier"):
    t2 = ui.TextInput(label="Tier 2 Points", max_length=4)
    t3 = ui.TextInput(label="Tier 3 Points", max_length=4)
    reply_pts = ui.TextInput(label="Points per Reply", max_length=6)

    def __init__(self, cog, p2, p3, p_reply):
        super().__init__()
        self.cog = cog
        self.t2.default = str(p2)
        self.t3.default = str(p3)
        self.reply_pts.default = str(p_reply)

    async def on_submit(self, interaction):
        try:
            p2, p3 = int(self.t2.value), int(self.t3.value)
            p_reply = float(self.reply_pts.value)
        except Exception: return await interaction.response.send_message("Must be valid numbers.", ephemeral=True)
        async with aiosqlite.connect(self.cog.db_path) as db:
            for t, p in [(2, p2), (3, p3)]:
                await db.execute("INSERT OR REPLACE INTO point_config (tier, points_required) VALUES (?, ?)", (t, p))
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('reply_points', ?)", (str(p_reply),))
            await db.commit()
        self.cog._invalidate_setting('reply_points')
        await interaction.response.send_message("Thresholds and reply points updated.", ephemeral=True)

class HourlyCapModal(ui.Modal, title="Hourly Point Cap"):
    cap = ui.TextInput(label="Max Points Per Hour (0 = no limit)", max_length=4, placeholder="e.g. 10")

    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction):
        try:
            cap_val = int(self.cap.value)
            if cap_val < 0:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Must be a positive number (or 0 for no limit).", ephemeral=True)

        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('hourly_point_cap', ?)", (str(cap_val),))
            await db.commit()
        self.cog._invalidate_setting('hourly_point_cap')

        if cap_val == 0:
            await interaction.response.send_message("Hourly cap disabled (no limit).", ephemeral=True)
        else:
            await interaction.response.send_message(f"Hourly cap set to **{cap_val}** points.", ephemeral=True)

class WelcomeMsgModal(ui.Modal, title="Set Welcome Message"):
    msg = ui.TextInput(label="Message ({username} to ping)", style=discord.TextStyle.paragraph)
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
    async def on_submit(self, interaction):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('welcome_msg', ?)", (self.msg.value,))
            await db.commit()
        await interaction.response.send_message("Welcome message saved.", ephemeral=True)

class UserSelectView(ui.View):
    def __init__(self, cog, mode):
        super().__init__()
        self.cog = cog
        self.mode = mode
    @ui.select(cls=ui.UserSelect, placeholder="Select User")
    async def callback(self, interaction, select):
        user = select.values[0]
        if self.mode == 'blacklist':
            async with aiosqlite.connect(self.cog.db_path) as db:
                await db.execute("INSERT OR IGNORE INTO blacklist (user_id) VALUES (?)", (user.id,))
                await db.commit()
            await interaction.response.send_message(f"{user.name} blacklisted.", ephemeral=True)

class WipeOptionsView(ui.View):
    """Main wipe options: Wipe All or Member Specific"""
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog

    @ui.button(label="Wipe All Members", style=discord.ButtonStyle.danger)
    async def wipe_all_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "⚠️ **Are you sure?**\nThis will wipe ALL points and thread logs for the current month.",
            view=WipeAllConfirmView(self.cog),
            ephemeral=True
        )

    @ui.button(label="Member Specific", style=discord.ButtonStyle.primary)
    async def member_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "Select a member:",
            view=WipeMemberSelectView(self.cog),
            ephemeral=True
        )

class WipeAllConfirmView(ui.View):
    """Confirmation for wiping all points"""
    def __init__(self, cog):
        super().__init__(timeout=30)
        self.cog = cog

    @ui.button(label="Yes, Wipe Everything", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("DELETE FROM user_points")
            await db.execute("DELETE FROM point_ledger")
            await db.execute("DELETE FROM thread_logs")
            await db.commit()
        await interaction.response.edit_message(content="All points, decay ledger, and thread logs wiped.", view=None)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)

class WipeMemberSelectView(ui.View):
    """Select a member to modify points"""
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog

    @ui.select(cls=ui.UserSelect, placeholder="Select Member")
    async def select_member(self, interaction: discord.Interaction, select: ui.UserSelect):
        user = select.values[0]
        member = interaction.guild.get_member(user.id)

        async with aiosqlite.connect(self.cog.db_path) as db:
            if member:
                earned, vip_base, effective = await self.cog.effective_points(member, db)
            else:
                cursor = await db.execute(
                    "SELECT points FROM user_points WHERE user_id = ?", (user.id,)
                )
                row = await cursor.fetchone()
                earned, vip_base, effective = (row[0] if row else 0), 0, (row[0] if row else 0)

        # The buttons below only ever touch earned points — the VIP floor is
        # virtual and comes and goes with the role.
        line = f"**{user.display_name}** - Current Points: **{earned}**"
        if vip_base:
            line += f"\nVIP floor: **+{vip_base}** → effective total **{effective}**"
        await interaction.response.edit_message(
            content=f"{line}\nChoose an action:",
            view=WipeMemberActionView(self.cog, user, earned)
        )

class WipeMemberActionView(ui.View):
    """Actions for a specific member's points"""
    def __init__(self, cog, user, current_points):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.current_points = current_points

    @ui.button(label="Add Points", style=discord.ButtonStyle.success)
    async def add_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PointAmountModal(self.cog, self.user, "add"))

    @ui.button(label="Remove Points", style=discord.ButtonStyle.primary)
    async def remove_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PointAmountModal(self.cog, self.user, "remove"))

    @ui.button(label="Set Points", style=discord.ButtonStyle.secondary)
    async def set_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PointAmountModal(self.cog, self.user, "set"))

    @ui.button(label="Wipe All", style=discord.ButtonStyle.danger)
    async def wipe_btn(self, interaction: discord.Interaction, button: ui.Button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("DELETE FROM user_points WHERE user_id = ?", (self.user.id,))
            await db.execute("DELETE FROM point_ledger WHERE user_id = ?", (self.user.id,))
            await db.execute("DELETE FROM thread_logs WHERE user_id = ?", (self.user.id,))
            await db.commit()
        await self.cog.resync_member_id(interaction.guild, self.user.id)
        await interaction.response.edit_message(
            content=f"Wiped all points, decay ledger, and thread logs for **{self.user.display_name}**.",
            view=None
        )

class PointAmountModal(ui.Modal, title="Enter Amount"):
    amount = ui.TextInput(label="Points", placeholder="Enter a number", max_length=6)

    def __init__(self, cog, user, action):
        super().__init__()
        self.cog = cog
        self.user = user
        self.action = action  # "add", "remove", or "set"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pts = int(self.amount.value)
            if pts < 0:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Must be a positive number.", ephemeral=True)

        earned_at = datetime.date.today().isoformat()
        expires_at = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()

        async with aiosqlite.connect(self.cog.db_path) as db:
            if self.action == "add":
                await db.execute(
                    "INSERT INTO user_points (user_id, points) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET points = points + ?",
                    (self.user.id, pts, pts)
                )
                await db.execute(
                    "INSERT INTO point_ledger (user_id, points, earned_at, expires_at) VALUES (?, ?, ?, ?)",
                    (self.user.id, pts, earned_at, expires_at)
                )
                msg = f"Added **{pts}** points to **{self.user.display_name}** (expire in 30 days)."
            elif self.action == "remove":
                await db.execute(
                    "UPDATE user_points SET points = MAX(0, points - ?) WHERE user_id = ?",
                    (pts, self.user.id)
                )
                msg = f"Removed **{pts}** points from **{self.user.display_name}**."
            else:  # set
                # Clear existing ledger and replace with a single entry for the new total
                await db.execute("DELETE FROM point_ledger WHERE user_id = ?", (self.user.id,))
                await db.execute(
                    "INSERT INTO user_points (user_id, points) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET points = ?",
                    (self.user.id, pts, pts)
                )
                if pts > 0:
                    await db.execute(
                        "INSERT INTO point_ledger (user_id, points, earned_at, expires_at) VALUES (?, ?, ?, ?)",
                        (self.user.id, pts, earned_at, expires_at)
                    )
                msg = f"Set **{self.user.display_name}**'s points to **{pts}** (expire in 30 days)."
            await db.commit()

        # Apply the new total to their tier role right away
        await self.cog.resync_member_id(interaction.guild, self.user.id)
        await interaction.response.send_message(msg, ephemeral=True)

class PointListView(ui.View):
    """Paginated point list.

    Three of the modes rank earned points over a time window. The fourth,
    "upgrade", ranks by *effective* points (earned + VIP floor) and draws the
    tier thresholds in as separator rows, so it's readable why a VIP sits where
    they do.
    """
    ITEMS_PER_PAGE = 20

    def __init__(self, cog, guild, current_mode="30days"):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild = guild
        self.current_mode = current_mode
        self.data = []
        # Upgrade mode only: entry index -> threshold lines to print before it.
        self.markers = {}
        self.page = 0
        self.max_page = 0

    async def load_data(self, mode):
        self.current_mode = mode
        self.page = 0
        self.markers = {}

        if mode == "upgrade":
            await self._load_upgrade_data()
            self.max_page = max(0, (len(self.data) - 1) // self.ITEMS_PER_PAGE)
            self.update_buttons()
            return

        async with aiosqlite.connect(self.cog.db_path) as db:
            if mode == "30days":
                cursor = await db.execute("SELECT user_id, points FROM user_points WHERE points > 0 ORDER BY points DESC")
                self.data = await cursor.fetchall()
            elif mode == "60days":
                thirty_days_ago = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
                sixty_days_ago = (datetime.date.today() - datetime.timedelta(days=60)).isoformat()
                cursor = await db.execute(
                    "SELECT user_id, SUM(points) FROM point_ledger WHERE earned_at >= ? AND earned_at < ? GROUP BY user_id HAVING SUM(points) > 0 ORDER BY SUM(points) DESC",
                    (sixty_days_ago, thirty_days_ago)
                )
                self.data = await cursor.fetchall()
            elif mode == "alltime":
                cursor = await db.execute(
                    "SELECT user_id, SUM(points) FROM point_ledger GROUP BY user_id HAVING SUM(points) > 0 ORDER BY SUM(points) DESC"
                )
                self.data = await cursor.fetchall()

        self.max_page = max(0, (len(self.data) - 1) // self.ITEMS_PER_PAGE)
        self.update_buttons()

    async def _load_upgrade_data(self):
        """Rank current members by effective points and place the tier lines.

        Members who left aren't included — their roles aren't ours to reason
        about any more. VIPs with no earned points still appear, since their
        floor alone can put them over a threshold.
        """
        alerts_cog = self.cog._alerts_cog()

        async with aiosqlite.connect(self.cog.db_path) as db:
            t2_req, t3_req = await self.cog._thresholds(db)
            cursor = await db.execute("SELECT user_id, points FROM user_points WHERE points > 0")
            points_map = dict(await cursor.fetchall())

        rows, seen = [], set()

        def add(member):
            if member.bot or member.id in seen:
                return
            seen.add(member.id)
            earned = points_map.get(member.id, 0)
            vip_base = self.cog.vip_base_points(alerts_cog, member, t2_req, t3_req)
            if earned <= 0 and vip_base <= 0:
                return
            rows.append((member, earned, vip_base, earned + vip_base))

        for user_id in points_map:
            member = self.guild.get_member(user_id)
            if member:
                add(member)

        if alerts_cog:
            for member in list(alerts_cog.iter_vip_members(self.guild)):
                add(member)

        rows.sort(key=lambda r: r[3], reverse=True)
        self.data = rows

        # A threshold line sits directly after the last member who clears it, so
        # its index is just how many members do. Tier 3 is registered first so it
        # prints above Tier 2 when both land in the same spot.
        for tier, req in ((3, t3_req), (2, t2_req)):
            if req is None:
                continue
            idx = sum(1 for r in rows if r[3] >= req)
            self.markers.setdefault(idx, []).append(f"**---Tier {tier} ({fmt_points(req)}pts)---**")

    def update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.max_page
        for btn, mode in (
            (self.mode_30_btn, "30days"),
            (self.mode_60_btn, "60days"),
            (self.mode_all_btn, "alltime"),
            (self.mode_upgrade_btn, "upgrade"),
        ):
            btn.style = discord.ButtonStyle.primary if self.current_mode == mode else discord.ButtonStyle.secondary

    async def build_page(self):
        if self.current_mode == "upgrade":
            return self._build_upgrade_page()

        start = self.page * self.ITEMS_PER_PAGE
        end = start + self.ITEMS_PER_PAGE
        page_data = self.data[start:end]

        mode_titles = {
            "30days": "Past 30 Days",
            "60days": "31-60 Days Ago",
            "alltime": "All-Time"
        }
        title = mode_titles.get(self.current_mode, "Point List")

        lines = [f"**{title} - Point List**\n"]

        if not page_data:
            lines.append("No data available.")

        for i, (user_id, points) in enumerate(page_data, start=start + 1):
            member = self.guild.get_member(user_id)
            name = shorten_name(member.display_name) if member else f"User {user_id}"
            lines.append(f"`{i}.` **{name}** - {fmt_points(points)} pts")

        lines.append(f"\nPage {self.page + 1}/{self.max_page + 1}")
        return "\n".join(lines)

    def _build_upgrade_page(self):
        start = self.page * self.ITEMS_PER_PAGE
        end = min(start + self.ITEMS_PER_PAGE, len(self.data))

        lines = ["**Upgrade Points List**\n"]
        if not self.data:
            lines.append("No data available.")

        for i in range(start, end):
            lines.extend(self.markers.get(i, []))
            member, earned, vip_base, _ = self.data[i]
            line = f"`{i + 1}.` **{shorten_name(member.display_name)}** - {fmt_points(earned)} pts"
            if vip_base:
                line += f" (+{fmt_points(vip_base)})"
            lines.append(line)

        # A threshold everyone on the list clears is marked at index len(data),
        # i.e. below the final entry.
        if end >= len(self.data):
            lines.extend(self.markers.get(len(self.data), []))

        lines.append(f"\nPage {self.page + 1}/{self.max_page + 1}")
        return "\n".join(lines)

    @ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary, row=0)
    async def prev_btn(self, interaction: discord.Interaction, button: ui.Button):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(content=await self.build_page(), view=self)

    @ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=0)
    async def next_btn(self, interaction: discord.Interaction, button: ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(content=await self.build_page(), view=self)

    @ui.button(label="Past 30 Days", style=discord.ButtonStyle.secondary, row=1)
    async def mode_30_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self.load_data("30days")
        await interaction.response.edit_message(content=await self.build_page(), view=self)

    @ui.button(label="31-60 Days", style=discord.ButtonStyle.secondary, row=1)
    async def mode_60_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self.load_data("60days")
        await interaction.response.edit_message(content=await self.build_page(), view=self)

    @ui.button(label="All-Time", style=discord.ButtonStyle.secondary, row=1)
    async def mode_all_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self.load_data("alltime")
        await interaction.response.edit_message(content=await self.build_page(), view=self)

    @ui.button(label="Upgrade List", style=discord.ButtonStyle.secondary, row=1)
    async def mode_upgrade_btn(self, interaction: discord.Interaction, button: ui.Button):
        await self.load_data("upgrade")
        await interaction.response.edit_message(content=await self.build_page(), view=self)

class UserPointSelectView(ui.View):
    """Pick a member to see every active point grant they hold and its expiry."""
    def __init__(self, cog):
        super().__init__(timeout=120)
        self.cog = cog

    @ui.select(cls=ui.UserSelect, placeholder="Select Member")
    async def select_member(self, interaction: discord.Interaction, select: ui.UserSelect):
        content = await self.cog.build_user_point_view(interaction.guild, select.values[0])
        await interaction.response.edit_message(content=content, view=self)

async def setup(bot):
    await bot.add_cog(IntroCog(bot))


