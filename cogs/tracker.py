import discord
from discord import app_commands, ui
from discord.ext import commands, tasks
import aiosqlite
import asyncio
from datetime import datetime, timedelta, timezone
import logging
import io
import typing
import traceback
import re
import aiohttp
from contextlib import asynccontextmanager
from pathlib import Path

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# --- CONFIGURATION ---
TRACKING_DB = "tracking_data.db"
DATA_RETENTION_DAYS = 365
EMOJI_PAGE_SIZE = 8
FONT_PATH = "/usr/share/fonts/truetype/noto"
TEMPLATE_DIR = Path(__file__).parent / "templates"

DONUT_COLORS = ['#5865F2', '#23a559', '#e8637a', '#f0b232', '#a78bfa', '#38bdf8', '#ef4444', '#06b6d4']

NO_ICON = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='48' height='48'%3E%3Crect width='48' height='48' rx='12' fill='%23313338'/%3E%3Ctext x='24' y='30' text-anchor='middle' fill='%23949ba4' font-size='20' font-family='sans-serif'%3E?%3C/text%3E%3C/svg%3E"

logger = logging.getLogger('betting_bot.tracker')
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)


# =========================================================================
# DATABASE MANAGER
# =========================================================================

class TrackingDB:
    def __init__(self):
        self.db_path = TRACKING_DB
        self._db: typing.Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def connect(self):
        async with self._lock:
            if self._db:
                return
            self._db = await aiosqlite.connect(self.db_path)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL;")
            await self._init_tables()
            logger.info("Tracking Database connected.")

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def _ensure_connected(self):
        if not self._db:
            await self.connect()

    @asynccontextmanager
    async def transaction(self):
        await self._ensure_connected()
        async with self._lock:
            await self._db.execute("BEGIN")
            try:
                yield self._db
                await self._db.commit()
            except Exception:
                await self._db.rollback()
                raise

    async def _init_tables(self):
        await self._db.execute("CREATE TABLE IF NOT EXISTS message_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, channel_id INTEGER, guild_id INTEGER, timestamp INTEGER, has_attachment BOOLEAN, is_reply BOOLEAN, reply_latency INTEGER, length INTEGER)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS social_interactions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, target_user_id INTEGER, guild_id INTEGER, channel_id INTEGER, timestamp INTEGER, interaction_type TEXT)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS voice_sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, channel_id INTEGER, guild_id INTEGER, start_time INTEGER, end_time INTEGER, duration INTEGER)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS reaction_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, target_message_author_id INTEGER, guild_id INTEGER, emoji_name TEXT, timestamp INTEGER)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS emoji_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, user_id INTEGER, emoji_id INTEGER, emoji_name TEXT, timestamp INTEGER, usage_type TEXT)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS guild_config (guild_id INTEGER PRIMARY KEY, vip_role_id INTEGER)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS member_events (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, guild_id INTEGER, event_type TEXT, timestamp INTEGER)")

        # Inactivity tables (preserve existing)
        await self._db.execute("CREATE TABLE IF NOT EXISTS inactivity_config (guild_id INTEGER PRIMARY KEY, log_channel_id INTEGER, msg_threshold INTEGER DEFAULT 5, period_days INTEGER DEFAULT 30, highlight_role_id INTEGER)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS user_inactivity_status (guild_id INTEGER, user_id INTEGER, status TEXT, snooze_until INTEGER, PRIMARY KEY (guild_id, user_id))")

        # Indexes
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_msg_user_time ON message_logs(user_id, timestamp)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_msg_channel_time ON message_logs(channel_id, timestamp)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_msg_guild_time ON message_logs(guild_id, timestamp)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_social_target ON social_interactions(target_user_id, guild_id)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_emoji_id ON emoji_logs(emoji_id, guild_id)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_member_events_guild_time ON member_events(guild_id, timestamp)")
        await self._db.commit()

    async def fetch_one(self, sql, params=()):
        await self._ensure_connected()
        async with self._db.execute(sql, params) as cursor:
            return await cursor.fetchone()

    async def fetch_all(self, sql, params=()):
        await self._ensure_connected()
        async with self._db.execute(sql, params) as cursor:
            return await cursor.fetchall()

    async def execute(self, sql, params=()):
        await self._ensure_connected()
        async with self._lock:
            await self._db.execute(sql, params)
            await self._db.commit()

    async def prune_old_data(self, cutoff_timestamp: int):
        await self._ensure_connected()
        async with self._lock:
            await self._db.execute("DELETE FROM message_logs WHERE timestamp < ?", (cutoff_timestamp,))
            await self._db.execute("DELETE FROM social_interactions WHERE timestamp < ?", (cutoff_timestamp,))
            await self._db.execute("DELETE FROM reaction_logs WHERE timestamp < ?", (cutoff_timestamp,))
            await self._db.execute("DELETE FROM voice_sessions WHERE end_time < ?", (cutoff_timestamp,))
            await self._db.execute("DELETE FROM emoji_logs WHERE timestamp < ?", (cutoff_timestamp,))
            await self._db.execute("DELETE FROM member_events WHERE timestamp < ?", (cutoff_timestamp,))
            await self._db.commit()


# =========================================================================
# TRACKER CARD GENERATOR (Playwright)
# =========================================================================

class TrackerCardGenerator:
    def __init__(self):
        self.browser = None
        self.playwright = None
        self._page_semaphore = asyncio.Semaphore(3)
        self._templates: dict[str, str] = {}

    async def initialize(self):
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not available for tracker cards.")
            return False
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                args=['--font-render-hinting=none', '--disable-lcd-text', '--enable-font-antialiasing']
            )
            for path in TEMPLATE_DIR.glob("tracker_*.html"):
                self._templates[path.stem] = path.read_text(encoding='utf-8')
            logger.info(f"Tracker card generator initialized ({len(self._templates)} templates).")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize tracker card generator: {e}")
            return False

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def render(self, template_name: str, data: dict, width: int = 640) -> typing.Optional[io.BytesIO]:
        template = self._templates.get(template_name)
        if not template or not self.browser:
            return None
        try:
            html = template.format(**data, font_path=FONT_PATH)
        except KeyError as e:
            logger.error(f"Template placeholder missing: {e}")
            return None

        async with self._page_semaphore:
            page = await self.browser.new_page(
                viewport={'width': width, 'height': 500},
                device_scale_factor=2
            )
            try:
                await page.set_content(html)
                await page.wait_for_timeout(150)
                body_height = await page.evaluate('document.body.scrollHeight')
                await page.set_viewport_size({'width': width, 'height': body_height + 20})
                screenshot = await page.screenshot(type='png')
            finally:
                await page.close()
        return io.BytesIO(screenshot)


# =========================================================================
# DASHBOARD VIEW
# =========================================================================

class DashboardView(ui.View):
    def __init__(self, cog, guild, user_id):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild
        self.user_id = user_id
        self.mode = "server"
        self.target_id = None
        self.time_filter = "14d"
        self.emoji_page = 1
        self.emoji_server_only = False
        self.showing_time_menu = False
        self.compare_target_1 = None
        self.compare_target_2 = None
        self.ch_compare_1 = None
        self.ch_compare_2 = None
        self.update_components()

    def update_components(self):
        self.clear_items()

        if self.showing_time_menu:
            self.add_item(self.TimeOption("24 Hours", "1d"))
            self.add_item(self.TimeOption("7 Days", "7d"))
            self.add_item(self.TimeOption("14 Days", "14d"))
            self.add_item(self.TimeOption("30 Days", "1mo"))
            self.add_item(self.TimeOption("All Time", "all"))
            self.add_item(self.BackButton())
            return

        self.add_item(self.ModeSelect(self.mode))

        if self.mode == "user":
            self.add_item(self.UserSelect())
        elif self.mode == "channel":
            self.add_item(self.ChannelSelect())
        elif self.mode == "compare":
            self.add_item(self.CompareUserSelect1())
            self.add_item(self.CompareUserSelect2())
        elif self.mode == "ch_compare":
            self.add_item(self.CompareChannelSelect1())
            self.add_item(self.CompareChannelSelect2())

        btn_row = 3 if self.mode in ("compare", "ch_compare") else 2
        self.add_item(self.ClockButton(btn_row))

        if self.mode == "emoji":
            self.add_item(self.ServerOnlyToggle(self.emoji_server_only))
            self.add_item(self.PagePrev())
            self.add_item(self.PageNext())

    async def refresh_embed(self, interaction):
        await interaction.response.defer()
        self.update_components()

        days_map = {"1d": 1, "7d": 7, "14d": 14, "1mo": 30, "all": 3650}
        days = days_map.get(self.time_filter, 14)
        img_file = None

        try:
            if self.mode == "server":
                img_file = await self.cog.gen_server_overview(self.guild, days)
            elif self.mode == "user":
                target = self.guild.get_member(self.target_id) if self.target_id else interaction.user
                if not target:
                    target = interaction.user
                img_file = await self.cog.gen_user_overview(self.guild, target, days)
            elif self.mode == "channel":
                cid = self.target_id if self.target_id else interaction.channel_id
                channel = self.guild.get_channel(cid) or self.guild.get_thread(cid)
                if channel:
                    img_file = await self.cog.gen_channel_overview(self.guild, channel, days)
                else:
                    return await interaction.followup.send("Channel not found.", ephemeral=True)
            elif self.mode == "emoji":
                img_file = await self.cog.gen_emoji_overview(self.guild, days, self.emoji_page, server_only=self.emoji_server_only)
            elif self.mode == "leaderboard":
                img_file = await self.cog.gen_leaderboard(self.guild, days)
            elif self.mode == "compare":
                if not self.compare_target_1 or not self.compare_target_2:
                    await interaction.edit_original_response(content="Select two users above to compare.", attachments=[], embed=None, view=self)
                    return
                u1 = self.guild.get_member(self.compare_target_1)
                u2 = self.guild.get_member(self.compare_target_2)
                if not u1 or not u2:
                    return await interaction.followup.send("Could not find one of the selected users.", ephemeral=True)
                if u1.id == u2.id:
                    return await interaction.followup.send("Select two different users to compare.", ephemeral=True)
                img_file = await self.cog.gen_comparison(self.guild, u1, u2, days)
            elif self.mode == "ch_compare":
                if not self.ch_compare_1 or not self.ch_compare_2:
                    await interaction.edit_original_response(content="Select two channels above to compare.", attachments=[], embed=None, view=self)
                    return
                c1 = self.guild.get_channel(self.ch_compare_1) or self.guild.get_thread(self.ch_compare_1)
                c2 = self.guild.get_channel(self.ch_compare_2) or self.guild.get_thread(self.ch_compare_2)
                if not c1 or not c2:
                    return await interaction.followup.send("Could not find one of the selected channels.", ephemeral=True)
                if c1.id == c2.id:
                    return await interaction.followup.send("Select two different channels to compare.", ephemeral=True)
                img_file = await self.cog.gen_channel_comparison(self.guild, c1, c2, days)

            if img_file:
                await interaction.edit_original_response(content="", embed=None, attachments=[img_file], view=self)
        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"Error generating stats: {e}", ephemeral=True)

    # --- BUTTONS ---
    class ClockButton(ui.Button):
        def __init__(self, row=2):
            super().__init__(style=discord.ButtonStyle.secondary, emoji="🕒", row=row)
        async def callback(self, interaction):
            self.view.showing_time_menu = True
            self.view.update_components()
            await interaction.response.edit_message(view=self.view)

    class TimeOption(ui.Button):
        def __init__(self, label, val):
            super().__init__(style=discord.ButtonStyle.secondary, label=label, row=0)
            self.val = val
        async def callback(self, interaction):
            self.view.time_filter = self.val
            self.view.showing_time_menu = False
            await self.view.refresh_embed(interaction)

    class BackButton(ui.Button):
        def __init__(self):
            super().__init__(style=discord.ButtonStyle.danger, label="Cancel", row=1)
        async def callback(self, interaction):
            self.view.showing_time_menu = False
            self.view.update_components()
            await interaction.response.edit_message(view=self.view)

    class ServerOnlyToggle(ui.Button):
        def __init__(self, active):
            label = "Server Emojis" if active else "All Emojis"
            style = discord.ButtonStyle.primary if active else discord.ButtonStyle.secondary
            super().__init__(style=style, label=label, emoji="🏠", row=2)
        async def callback(self, interaction):
            self.view.emoji_server_only = not self.view.emoji_server_only
            self.view.emoji_page = 1
            await self.view.refresh_embed(interaction)

    class PagePrev(ui.Button):
        def __init__(self):
            super().__init__(style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
        async def callback(self, interaction):
            if self.view.emoji_page > 1:
                self.view.emoji_page -= 1
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()

    class PageNext(ui.Button):
        def __init__(self):
            super().__init__(style=discord.ButtonStyle.secondary, emoji="➡️", row=2)
        async def callback(self, interaction):
            self.view.emoji_page += 1
            await self.view.refresh_embed(interaction)

    class ModeSelect(ui.Select):
        def __init__(self, current_mode):
            options = [
                discord.SelectOption(label="Server Overview", value="server"),
                discord.SelectOption(label="User Overview", value="user"),
                discord.SelectOption(label="Channel Overview", value="channel"),
                discord.SelectOption(label="Emoji Overview", value="emoji"),
                discord.SelectOption(label="Leaderboard", value="leaderboard"),
                discord.SelectOption(label="Compare Users", value="compare"),
                discord.SelectOption(label="Compare Channels", value="ch_compare"),
            ]
            for opt in options:
                if opt.value == current_mode:
                    opt.default = True
            super().__init__(placeholder="Select Dashboard Mode", options=options, row=0)
        async def callback(self, interaction):
            self.view.mode = self.values[0]
            self.view.target_id = None
            self.view.emoji_page = 1
            self.view.compare_target_1 = None
            self.view.compare_target_2 = None
            self.view.ch_compare_1 = None
            self.view.ch_compare_2 = None
            await self.view.refresh_embed(interaction)

    class UserSelect(ui.UserSelect):
        def __init__(self):
            super().__init__(placeholder="Search User...", row=1)
        async def callback(self, interaction):
            self.view.target_id = self.values[0].id
            await self.view.refresh_embed(interaction)

    class ChannelSelect(ui.ChannelSelect):
        def __init__(self):
            types = [discord.ChannelType.text, discord.ChannelType.voice, discord.ChannelType.public_thread, discord.ChannelType.forum]
            super().__init__(placeholder="Search Channel/Thread...", channel_types=types, row=1)
        async def callback(self, interaction):
            self.view.target_id = self.values[0].id
            await self.view.refresh_embed(interaction)

    class CompareUserSelect1(ui.UserSelect):
        def __init__(self):
            super().__init__(placeholder="User 1...", row=1)
        async def callback(self, interaction):
            self.view.compare_target_1 = self.values[0].id
            if self.view.compare_target_2:
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()

    class CompareUserSelect2(ui.UserSelect):
        def __init__(self):
            super().__init__(placeholder="User 2...", row=2)
        async def callback(self, interaction):
            self.view.compare_target_2 = self.values[0].id
            if self.view.compare_target_1:
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()

    class CompareChannelSelect1(ui.ChannelSelect):
        def __init__(self):
            types = [discord.ChannelType.text, discord.ChannelType.voice, discord.ChannelType.public_thread, discord.ChannelType.forum]
            super().__init__(placeholder="Channel 1...", channel_types=types, row=1)
        async def callback(self, interaction):
            self.view.ch_compare_1 = self.values[0].id
            if self.view.ch_compare_2:
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()

    class CompareChannelSelect2(ui.ChannelSelect):
        def __init__(self):
            types = [discord.ChannelType.text, discord.ChannelType.voice, discord.ChannelType.public_thread, discord.ChannelType.forum]
            super().__init__(placeholder="Channel 2...", channel_types=types, row=2)
        async def callback(self, interaction):
            self.view.ch_compare_2 = self.values[0].id
            if self.view.ch_compare_1:
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()


# =========================================================================
# MAIN COG
# =========================================================================

class UserTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = TrackingDB()
        self.card_gen = TrackerCardGenerator()
        self.session = None
        self._voice_join_times: dict[tuple[int, int], tuple[int, int]] = {}

    async def cog_load(self):
        self.session = aiohttp.ClientSession()
        self.bot.loop.create_task(self._async_setup())

    async def _async_setup(self):
        await self.bot.wait_until_ready()
        await self.db.connect()
        await self.card_gen.initialize()
        await self._reseed_voice_sessions()
        self.data_retention_task.start()
        self.voice_checkpoint_task.start()

    async def cog_unload(self):
        if self.session:
            await self.session.close()
        self.data_retention_task.cancel()
        self.voice_checkpoint_task.cancel()
        await self._flush_voice_sessions()
        await self.card_gen.close()
        await self.db.close()

    # --- VOICE SESSION HELPERS ---

    async def _reseed_voice_sessions(self):
        """Record join times for members already in voice when bot starts."""
        now = int(datetime.now(timezone.utc).timestamp())
        for guild in self.bot.guilds:
            for vc in guild.voice_channels + guild.stage_channels:
                for member in vc.members:
                    if not member.bot:
                        self._voice_join_times[(member.id, guild.id)] = (now, vc.id)
        if self._voice_join_times:
            logger.info(f"Reseeded {len(self._voice_join_times)} voice sessions.")

    async def _flush_voice_sessions(self):
        """Save all active voice sessions on shutdown."""
        now = int(datetime.now(timezone.utc).timestamp())
        for (uid, gid), (join_ts, cid) in self._voice_join_times.items():
            duration = now - join_ts
            if duration > 5:
                await self.db.execute(
                    "INSERT INTO voice_sessions (user_id, channel_id, guild_id, start_time, end_time, duration) VALUES (?,?,?,?,?,?)",
                    (uid, cid, gid, join_ts, now, duration)
                )
        self._voice_join_times.clear()

    # --- HELPERS ---

    def get_channel_safe(self, guild, cid):
        c = guild.get_channel(cid)
        if c:
            return c
        c = guild.get_thread(cid)
        if c:
            return c
        for thread in guild.threads:
            if thread.id == cid:
                return thread
        return None

    @staticmethod
    def _fmt_voice(seconds):
        """Format seconds into a readable voice time string."""
        if not seconds or seconds <= 0:
            return "0 hours"
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        if hours > 0:
            return f"{hours}h {mins}m" if mins > 0 else f"{hours} hours"
        return f"{mins} min"

    @staticmethod
    def _icon_url(guild):
        return str(guild.icon.url) if guild.icon else NO_ICON

    @staticmethod
    def _fill_daily(rows, days):
        """Fill in missing days with 0s. rows = [(date_str, msgs, [contribs]), ...]"""
        now = datetime.now(timezone.utc)
        lookup = {}
        for r in rows:
            try:
                lookup[r[0]] = (r[1], r[2])
            except (IndexError, KeyError):
                lookup[r[0]] = (r[1], 0)
        result = []
        for i in range(days):
            d = (now - timedelta(days=days - 1 - i)).strftime('%Y-%m-%d')
            msgs, contribs = lookup.get(d, (0, 0))
            result.append((d, msgs, contribs))
        return result

    @staticmethod
    def _build_bars_html(daily_data, key_idx=1):
        """Build CSS bar chart HTML from daily data."""
        values = [d[key_idx] for d in daily_data]
        max_val = max(values) if values else 1
        if max_val == 0:
            max_val = 1
        bars = []
        for v in values:
            pct = max(int((v / max_val) * 100), 2)
            bars.append(f'<div class="bar-wrapper"><div class="bar bar-msg" style="height: {pct}%"></div></div>')
        return "\n".join(bars)

    @staticmethod
    def _build_svg_line(daily_data, key_idx=1, svg_width=600, svg_height=150, forced_max=None):
        """Build SVG polyline points from daily data."""
        values = [d[key_idx] for d in daily_data]
        if not values:
            return ""
        max_val = forced_max if forced_max else max(values)
        if not max_val or max_val == 0:
            max_val = 1
        n = len(values)
        points = []
        for i, v in enumerate(values):
            x = int(i / max(n - 1, 1) * svg_width)
            y = int(svg_height - (v / max_val * (svg_height - 10)))
            points.append(f"{x},{y}")
        return " ".join(points)

    @staticmethod
    def _build_chart_labels(daily_data, max_labels=7):
        """Build date labels for chart x-axis."""
        n = len(daily_data)
        if n == 0:
            return ""
        step = max(1, n // max_labels)
        labels = []
        for i, (date_str, _, *_rest) in enumerate(daily_data):
            if i % step == 0 or i == n - 1:
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d')
                    label = dt.strftime('%-m/%-d')
                except ValueError:
                    label = date_str
                labels.append(f'<span class="chart-label">{label}</span>')
            else:
                labels.append('<span class="chart-label"></span>')
        return "\n".join(labels)

    @staticmethod
    def _reduce_data(daily_data, max_points=60):
        """Reduce data points by grouping consecutive entries."""
        if len(daily_data) <= max_points:
            return daily_data
        bucket_size = max(1, len(daily_data) // max_points)
        result = []
        for i in range(0, len(daily_data), bucket_size):
            bucket = daily_data[i:i + bucket_size]
            label = bucket[-1][0]
            total_v1 = sum(d[1] for d in bucket)
            if len(bucket[0]) > 2:
                total_v2 = sum(d[2] for d in bucket)
                result.append((label, total_v1, total_v2))
            else:
                result.append((label, total_v1))
        return result

    # --- GENERATORS ---

    async def gen_server_overview(self, guild, days):
        now_dt = datetime.now(timezone.utc)
        if days >= 3650:
            cutoff = 0
        else:
            cutoff = int((now_dt - timedelta(days=days)).timestamp())
        prev_cutoff = int((now_dt - timedelta(days=days * 2)).timestamp()) if days < 3650 else 0

        # Totals
        row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND timestamp > ?", (guild.id, cutoff))
        total_msgs = row[0] or 0

        prev_row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND timestamp BETWEEN ? AND ?", (guild.id, prev_cutoff, cutoff))
        prev_msgs = prev_row[0] or 0
        if prev_msgs > 0:
            growth = int(((total_msgs - prev_msgs) / prev_msgs) * 100)
        else:
            growth = 100 if total_msgs > 0 else 0

        active_row = await self.db.fetch_one("SELECT count(distinct user_id) FROM message_logs WHERE guild_id = ? AND timestamp > ?", (guild.id, cutoff))
        active_users = active_row[0] or 0

        new_row = await self.db.fetch_one("SELECT count(*) FROM member_events WHERE guild_id = ? AND event_type = 'join' AND timestamp > ?", (guild.id, cutoff))
        new_members = new_row[0] or 0

        voice_row = await self.db.fetch_one("SELECT coalesce(sum(duration), 0) FROM voice_sessions WHERE guild_id = ? AND start_time > ?", (guild.id, cutoff))
        voice_secs = voice_row[0] or 0
        # Add in-progress voice sessions from memory
        for (uid, gid), (join_ts, cid) in self._voice_join_times.items():
            if gid == guild.id and join_ts > cutoff:
                voice_secs += int(datetime.now(timezone.utc).timestamp()) - join_ts

        # Daily chart data
        daily_rows = await self.db.fetch_all(
            "SELECT date(timestamp, 'unixepoch') as day, count(*) as msgs, count(distinct user_id) as contribs FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY day ORDER BY day",
            (guild.id, cutoff)
        )
        if days >= 3650 and daily_rows:
            first_date = datetime.strptime(daily_rows[0][0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            chart_days = max(1, (now_dt - first_date).days + 1)
        elif days >= 3650:
            chart_days = 30
        else:
            chart_days = days
        daily = self._fill_daily(daily_rows, chart_days)
        daily = self._reduce_data(daily, max_points=60)

        # Top members
        u_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 5", (guild.id, cutoff))
        members_html = ""
        for i, r in enumerate(u_rows):
            u = guild.get_member(r[0])
            name = u.display_name if u else "Unknown"
            if len(name) > 16:
                name = name[:14] + ".."
            members_html += f'<div class="list-item"><span class="rank">#{i+1}</span><span class="item-name">{name}</span><span class="item-value">{r[1]:,}</span></div>\n'

        # Top channels
        c_rows = await self.db.fetch_all("SELECT channel_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY channel_id ORDER BY c DESC LIMIT 5", (guild.id, cutoff))
        channels_html = ""
        for i, r in enumerate(c_rows):
            ch = self.get_channel_safe(guild, r[0])
            name = ch.name if ch else "deleted-channel"
            if len(name) > 16:
                name = name[:14] + ".."
            channels_html += f'<div class="list-item"><span class="rank">#{i+1}</span><span class="item-name"><span class="channel-hash">#</span> {name}</span><span class="item-value">{r[1]:,}</span></div>\n'

        growth_class = "positive" if growth >= 0 else "negative"
        growth_sym = "+" if growth >= 0 else ""
        growth_text = f"{growth_sym}{growth}% vs prev" if prev_msgs > 0 else ""

        period_label = f"Last {days} days" if days < 3650 else "All Time"
        svg_w = max(len(daily) * 20, 100)

        data = {
            'server_icon_url': self._icon_url(guild),
            'server_name': guild.name,
            'total_msgs': f"{total_msgs:,}",
            'growth_class': growth_class,
            'growth_text': growth_text,
            'active_users': f"{active_users:,}",
            'new_members': f"+{new_members}" if new_members > 0 else "0",
            'voice_hours': self._fmt_voice(voice_secs),
            'bars_html': self._build_bars_html(daily),
            'svg_width': svg_w,
            'svg_points': self._build_svg_line(daily, key_idx=2, svg_width=svg_w),
            'chart_labels_html': self._build_chart_labels(daily),
            'members_html': members_html if members_html else '<div class="list-item"><span class="item-name" style="color:#6d6f78">No data yet</span></div>',
            'channels_html': channels_html if channels_html else '<div class="list-item"><span class="item-name" style="color:#6d6f78">No data yet</span></div>',
            'period_label': period_label,
        }
        buf = await self.card_gen.render('tracker_server_card', data, width=640)
        if buf:
            return discord.File(buf, filename="server_overview.png")
        return None

    async def gen_user_overview(self, guild, user, days):
        now_dt = datetime.now(timezone.utc)
        now_ts = int(now_dt.timestamp())
        if days >= 3650:
            cutoff = 0
        else:
            cutoff = int((now_dt - timedelta(days=days)).timestamp())

        # Message counts for 1d/7d/14d/30d
        msg_counts = {}
        for period, d in [('1d', 1), ('7d', 7), ('14d', 14), ('30d', 30)]:
            c = now_ts - d * 86400
            row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?", (guild.id, user.id, c))
            msg_counts[period] = row[0] or 0
        # All-time message count
        all_msg_row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND user_id = ?", (guild.id, user.id))
        msg_counts['all'] = all_msg_row[0] or 0

        # Voice time for 1d/7d/14d/30d (include in-progress session from memory)
        active_voice = self._voice_join_times.get((user.id, guild.id))
        active_dur = (now_ts - active_voice[0]) if active_voice else 0
        voice_times = {}
        for period, d in [('1d', 1), ('7d', 7), ('14d', 14), ('30d', 30)]:
            c = now_ts - d * 86400
            row = await self.db.fetch_one("SELECT coalesce(sum(duration), 0) FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ?", (guild.id, user.id, c))
            v = row[0] or 0
            if active_voice and active_voice[0] > c:
                v += active_dur
            voice_times[period] = v
        # All-time voice time
        all_voice_row = await self.db.fetch_one("SELECT coalesce(sum(duration), 0) FROM voice_sessions WHERE guild_id = ? AND user_id = ?", (guild.id, user.id))
        voice_times['all'] = (all_voice_row[0] or 0) + active_dur

        # Message rank
        rank_row = await self.db.fetch_one(
            "SELECT rank FROM (SELECT user_id, ROW_NUMBER() OVER (ORDER BY count(*) DESC) as rank FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id) WHERE user_id = ?",
            (guild.id, cutoff, user.id)
        )
        msg_rank = f"#{rank_row[0]}" if rank_row else "N/A"

        # Voice rank (merge DB + in-progress sessions)
        all_voice = await self.db.fetch_all(
            "SELECT user_id, sum(duration) as total FROM voice_sessions WHERE guild_id = ? AND start_time > ? GROUP BY user_id",
            (guild.id, cutoff)
        )
        voice_totals = {r[0]: r[1] for r in all_voice}
        for (uid, gid), (jts, _) in self._voice_join_times.items():
            if gid == guild.id and jts > cutoff:
                voice_totals[uid] = voice_totals.get(uid, 0) + (now_ts - jts)
        if voice_totals:
            sorted_voice = sorted(voice_totals.items(), key=lambda x: x[1], reverse=True)
            voice_rank_num = next((i + 1 for i, (uid, _) in enumerate(sorted_voice) if uid == user.id), None)
            voice_rank = f"#{voice_rank_num}" if voice_rank_num else "No Data"
            voice_rank_class = "" if voice_rank_num else "no-data"
        else:
            voice_rank = "No Data"
            voice_rank_class = "no-data"

        # Top channels
        ch_rows = await self.db.fetch_all("SELECT channel_id, count(*) as c FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY channel_id ORDER BY c DESC LIMIT 6", (guild.id, user.id, cutoff))
        channels_html = ""
        for r in ch_rows:
            ch = self.get_channel_safe(guild, r[0])
            name = ch.name if ch else "deleted"
            if len(name) > 18:
                name = name[:16] + ".."
            channels_html += f'<div class="channel-item"><span class="channel-hash">#</span><span class="channel-name">{name}</span><span class="channel-count">{r[1]:,}</span></div>\n'
        if not channels_html:
            channels_html = '<div style="color:#6d6f78;font-size:13px;padding:8px 0;">No data yet</div>'

        # Activity sparkline (daily messages + voice for the period)
        daily_rows = await self.db.fetch_all(
            "SELECT date(timestamp, 'unixepoch') as day, count(*) as msgs FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY day ORDER BY day",
            (guild.id, user.id, cutoff)
        )
        voice_daily_rows = await self.db.fetch_all(
            "SELECT date(start_time, 'unixepoch') as day, coalesce(sum(duration), 0) as secs FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ? GROUP BY day ORDER BY day",
            (guild.id, user.id, cutoff)
        )
        # Determine chart range
        if days >= 3650 and daily_rows:
            first_date = datetime.strptime(daily_rows[0][0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            chart_days = max(1, (now_dt - first_date).days + 1)
        elif days >= 3650:
            chart_days = 30
        else:
            chart_days = days
        # Fill daily data
        msg_daily = self._fill_daily(daily_rows, chart_days)
        voice_lookup = {r[0]: r[1] for r in voice_daily_rows}
        # Add in-progress voice session to today's total
        today_str = now_dt.strftime('%Y-%m-%d')
        if active_voice:
            voice_lookup[today_str] = voice_lookup.get(today_str, 0) + active_dur
        voice_daily = []
        for i in range(chart_days):
            d = (now_dt - timedelta(days=chart_days - 1 - i)).strftime('%Y-%m-%d')
            voice_daily.append((d, voice_lookup.get(d, 0)))
        # Reduce data points if needed
        msg_daily = self._reduce_data(msg_daily, max_points=60)
        voice_daily = self._reduce_data(voice_daily, max_points=60)

        chart_w = max(len(msg_daily) * 10, 100)
        msg_points = self._build_svg_line(msg_daily, key_idx=1, svg_width=chart_w, svg_height=80)
        voice_points = self._build_svg_line(voice_daily, key_idx=1, svg_width=chart_w, svg_height=80)

        # Axis labels
        msg_max_val = max((d[1] for d in msg_daily), default=0)
        voice_max_secs = max((d[1] for d in voice_daily), default=0)
        msg_max = str(msg_max_val) if msg_max_val > 0 else "0"
        voice_max = self._fmt_voice(voice_max_secs) if voice_max_secs > 0 else "0"

        chart_date_start = (now_dt - timedelta(days=chart_days - 1)).strftime('%-m/%-d')
        chart_date_end = now_dt.strftime('%-m/%-d')

        created = user.created_at.strftime('%b %d, %Y')
        joined = user.joined_at.strftime('%b %d, %Y') if user.joined_at else "Unknown"
        period_label = f"Last {days} days" if days < 3650 else "All Time"

        data = {
            'avatar_url': str(user.display_avatar.url),
            'display_name': user.display_name,
            'username': str(user),
            'created_date': created,
            'joined_date': joined,
            'msg_rank': msg_rank,
            'voice_rank': voice_rank,
            'voice_rank_class': voice_rank_class,
            'msgs_1d': msg_counts['1d'],
            'msgs_7d': msg_counts['7d'],
            'msgs_14d': msg_counts['14d'],
            'msgs_30d': msg_counts['30d'],
            'msgs_all': f"{msg_counts['all']:,}",
            'voice_1d': self._fmt_voice(voice_times['1d']),
            'voice_7d': self._fmt_voice(voice_times['7d']),
            'voice_14d': self._fmt_voice(voice_times['14d']),
            'voice_30d': self._fmt_voice(voice_times['30d']),
            'voice_all': self._fmt_voice(voice_times['all']),
            'channels_html': channels_html,
            'chart_width': chart_w,
            'msg_chart_points': msg_points,
            'voice_chart_points': voice_points,
            'msg_max': msg_max,
            'voice_max': voice_max,
            'chart_date_start': chart_date_start,
            'chart_date_end': chart_date_end,
            'period_label': period_label,
        }
        buf = await self.card_gen.render('tracker_user_card', data, width=640)
        if buf:
            return discord.File(buf, filename="user_overview.png")
        return None

    async def gen_channel_overview(self, guild, channel, days):
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

        row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ?", (guild.id, channel.id, cutoff))
        total_msgs = row[0] or 0

        contrib_row = await self.db.fetch_one("SELECT count(distinct user_id) FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ?", (guild.id, channel.id, cutoff))
        contributors = contrib_row[0] or 0

        # Daily chart
        daily_rows = await self.db.fetch_all(
            "SELECT date(timestamp, 'unixepoch') as day, count(*) as msgs FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ? GROUP BY day ORDER BY day",
            (guild.id, channel.id, cutoff)
        )
        daily = self._fill_daily(daily_rows, days)

        # Top contributors
        u_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 8", (guild.id, channel.id, cutoff))
        max_c = u_rows[0][1] if u_rows else 1
        contributors_html = ""
        for i, r in enumerate(u_rows):
            u = guild.get_member(r[0])
            name = u.display_name if u else "Unknown"
            if len(name) > 18:
                name = name[:16] + ".."
            bar_pct = max(int((r[1] / max_c) * 100), 3) if max_c > 0 else 3
            contributors_html += f'''<div class="contributor-item">
                <span class="rank">#{i+1}</span>
                <span class="contrib-name">{name}</span>
                <div class="contrib-bar-wrap"><div class="contrib-bar" style="width:{bar_pct}%"></div></div>
                <span class="contrib-value">{r[1]:,}</span>
            </div>\n'''
        if not contributors_html:
            contributors_html = '<div style="color:#6d6f78;font-size:13px;padding:8px 0;">No data yet</div>'

        ch_name = channel.name
        period_label = f"Last {days} days" if days < 3650 else "All Time"

        data = {
            'channel_name': ch_name,
            'period_label': period_label,
            'total_msgs': f"{total_msgs:,}",
            'contributors': f"{contributors:,}",
            'bars_html': self._build_bars_html(daily),
            'chart_labels_html': self._build_chart_labels(daily),
            'contributors_html': contributors_html,
        }
        buf = await self.card_gen.render('tracker_channel_card', data, width=580)
        if buf:
            return discord.File(buf, filename="channel_overview.png")
        return None

    async def gen_emoji_overview(self, guild, days, page, server_only=False):
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

        if server_only:
            guild_emoji_ids = [e.id for e in guild.emojis]
            if not guild_emoji_ids:
                rows = []
            else:
                placeholders = ','.join('?' for _ in guild_emoji_ids)
                rows = await self.db.fetch_all(
                    f"SELECT emoji_id, emoji_name, count(*) as uses, count(distinct user_id) as users FROM emoji_logs WHERE guild_id = ? AND timestamp > ? AND emoji_id IN ({placeholders}) GROUP BY emoji_id ORDER BY uses DESC",
                    (guild.id, cutoff, *guild_emoji_ids)
                )
        else:
            rows = await self.db.fetch_all(
                "SELECT emoji_id, emoji_name, count(*) as uses, count(distinct user_id) as users FROM emoji_logs WHERE guild_id = ? AND timestamp > ? GROUP BY emoji_id ORDER BY uses DESC",
                (guild.id, cutoff)
            )

        total_pages = max(1, (len(rows) + EMOJI_PAGE_SIZE - 1) // EMOJI_PAGE_SIZE)
        if page > total_pages:
            page = 1
        start = (page - 1) * EMOJI_PAGE_SIZE
        page_rows = rows[start:start + EMOJI_PAGE_SIZE]

        # Donut chart for top 5 (normalized to fill the whole circle)
        top5 = rows[:5]
        total_uses = sum(r[2] for r in rows) if rows else 0
        top5_total = sum(r[2] for r in top5) if top5 else 0
        if top5 and top5_total > 0:
            cumulative = 0
            stops = []
            for i, r in enumerate(top5):
                pct = r[2] / top5_total * 100
                color = DONUT_COLORS[i % len(DONUT_COLORS)]
                stops.append(f"{color} {cumulative:.1f}% {cumulative + pct:.1f}%")
                cumulative += pct
            donut_gradient = f"conic-gradient({', '.join(stops)})"
        else:
            donut_gradient = "conic-gradient(#3a3c4e 0% 100%)"

        # Legend
        legend_html = ""
        for i, r in enumerate(top5):
            color = DONUT_COLORS[i % len(DONUT_COLORS)]
            name = r[1] if len(r[1]) <= 16 else r[1][:14] + ".."
            legend_html += f'<div class="legend-item"><div class="legend-color" style="background:{color}"></div><span class="legend-name">:{name}:</span><span class="legend-count">{r[2]:,}</span></div>\n'

        # Emoji list
        emoji_list_html = ""
        if not page_rows:
            emoji_list_html = '<div class="no-data">No emoji usage data yet</div>'
        else:
            for i, r in enumerate(page_rows):
                rank = start + i + 1
                eid, ename, count, users = r[0], r[1], r[2], r[3]
                emoji_url = f"https://cdn.discordapp.com/emojis/{eid}.png?size=64"
                emoji_list_html += f'''<div class="emoji-item">
                    <span class="emoji-rank">#{rank}</span>
                    <img class="emoji-img" src="{emoji_url}" alt="">
                    <span class="emoji-name">:{ename}:</span>
                    <div class="emoji-stats"><div class="emoji-uses">{count:,} uses</div><div class="emoji-users">{users} users</div></div>
                </div>\n'''

        filter_label = "Server Emojis" if server_only else "Emoji Overview"
        period_label = f"Last {days} days" if days < 3650 else "All Time"

        data = {
            'server_icon_url': self._icon_url(guild),
            'server_name': guild.name,
            'filter_label': filter_label,
            'period_label': period_label,
            'donut_gradient': donut_gradient,
            'total_uses': f"{total_uses:,}",
            'legend_html': legend_html,
            'emoji_list_html': emoji_list_html,
            'page_num': page,
            'total_pages': total_pages,
        }
        buf = await self.card_gen.render('tracker_emoji_card', data, width=580)
        if buf:
            return discord.File(buf, filename="emoji_overview.png")
        return None

    async def gen_leaderboard(self, guild, days):
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

        # Message leaderboard
        msg_rows = await self.db.fetch_all(
            "SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 10",
            (guild.id, cutoff)
        )
        max_msgs = msg_rows[0][1] if msg_rows else 1
        msg_lb_html = ""
        for i, r in enumerate(msg_rows):
            u = guild.get_member(r[0])
            name = u.display_name if u else "Unknown"
            if len(name) > 16:
                name = name[:14] + ".."
            avatar_url = str(u.display_avatar.url) if u else NO_ICON
            rank_class = ['gold', 'silver', 'bronze'][i] if i < 3 else 'normal'
            top_class = f' top-{i+1}' if i < 3 else ''
            bar_pct = max(int((r[1] / max_msgs) * 100), 3) if max_msgs > 0 else 3
            msg_lb_html += f'''<div class="lb-item{top_class}">
                <span class="lb-rank {rank_class}">#{i+1}</span>
                <img class="lb-avatar" src="{avatar_url}" alt="">
                <span class="lb-name">{name}</span>
                <div class="lb-bar-wrap"><div class="lb-bar msg" style="width:{bar_pct}%"></div></div>
                <span class="lb-value">{r[1]:,}</span>
            </div>\n'''
        if not msg_lb_html:
            msg_lb_html = '<div class="no-data">No message data yet</div>'

        # Voice leaderboard (merge DB + in-progress sessions)
        voice_rows_raw = await self.db.fetch_all(
            "SELECT user_id, sum(duration) as total FROM voice_sessions WHERE guild_id = ? AND start_time > ? GROUP BY user_id ORDER BY total DESC",
            (guild.id, cutoff)
        )
        now_ts = int(datetime.now(timezone.utc).timestamp())
        voice_totals = {r[0]: r[1] for r in voice_rows_raw}
        for (uid, gid), (join_ts, cid) in self._voice_join_times.items():
            if gid == guild.id and join_ts > cutoff:
                voice_totals[uid] = voice_totals.get(uid, 0) + (now_ts - join_ts)
        voice_sorted = sorted(voice_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        max_voice = voice_sorted[0][1] if voice_sorted else 1
        voice_lb_html = ""
        for i, (uid, total) in enumerate(voice_sorted):
            u = guild.get_member(uid)
            name = u.display_name if u else "Unknown"
            if len(name) > 16:
                name = name[:14] + ".."
            avatar_url = str(u.display_avatar.url) if u else NO_ICON
            rank_class = ['gold', 'silver', 'bronze'][i] if i < 3 else 'normal'
            top_class = f' top-{i+1}' if i < 3 else ''
            bar_pct = max(int((total / max_voice) * 100), 3) if max_voice > 0 else 3
            voice_lb_html += f'''<div class="lb-item{top_class}">
                <span class="lb-rank {rank_class}">#{i+1}</span>
                <img class="lb-avatar" src="{avatar_url}" alt="">
                <span class="lb-name">{name}</span>
                <div class="lb-bar-wrap"><div class="lb-bar voice" style="width:{bar_pct}%"></div></div>
                <span class="lb-value">{self._fmt_voice(total)}</span>
            </div>\n'''
        if not voice_lb_html:
            voice_lb_html = '<div class="no-data">No voice data yet</div>'

        period_label = f"Last {days} days" if days < 3650 else "All Time"

        data = {
            'server_icon_url': self._icon_url(guild),
            'server_name': guild.name,
            'period_label': period_label,
            'msg_leaderboard_html': msg_lb_html,
            'voice_leaderboard_html': voice_lb_html,
        }
        buf = await self.card_gen.render('tracker_leaderboard_card', data, width=700)
        if buf:
            return discord.File(buf, filename="leaderboard.png")
        return None

    async def gen_comparison(self, guild, user1, user2, days):
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        now_dt = datetime.now(timezone.utc)

        async def _user_stats(uid):
            msg_row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?", (guild.id, uid, cutoff))
            msgs = msg_row[0] or 0
            voice_row = await self.db.fetch_one("SELECT coalesce(sum(duration), 0) FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ?", (guild.id, uid, cutoff))
            voice_secs = voice_row[0] or 0
            # Include in-progress voice session
            active = self._voice_join_times.get((uid, guild.id))
            if active and active[0] > cutoff:
                voice_secs += int(now_dt.timestamp()) - active[0]
            rank_row = await self.db.fetch_one("SELECT rank FROM (SELECT user_id, ROW_NUMBER() OVER (ORDER BY count(*) DESC) as rank FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id) WHERE user_id = ?", (guild.id, cutoff, uid))
            rank = rank_row[0] if rank_row else None
            react_row = await self.db.fetch_one("SELECT count(*) FROM reaction_logs WHERE guild_id = ? AND target_message_author_id = ? AND timestamp > ?", (guild.id, uid, cutoff))
            reacts = react_row[0] or 0
            daily_rows = await self.db.fetch_all("SELECT date(timestamp, 'unixepoch') as day, count(*) as msgs FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY day ORDER BY day", (guild.id, uid, cutoff))
            daily = self._fill_daily(daily_rows, days)
            ch_rows = await self.db.fetch_all("SELECT channel_id, count(*) as c FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY channel_id ORDER BY c DESC LIMIT 4", (guild.id, uid, cutoff))
            return {'msgs': msgs, 'voice_secs': voice_secs, 'rank': rank, 'reacts': reacts, 'daily': daily, 'channels': ch_rows}

        s1 = await _user_stats(user1.id)
        s2 = await _user_stats(user2.id)

        # Build stat rows
        stat_defs = [
            ("Messages", f"{s1['msgs']:,}", f"{s2['msgs']:,}", s1['msgs'] > s2['msgs'], s1['msgs'] < s2['msgs']),
            ("Voice Time", self._fmt_voice(s1['voice_secs']), self._fmt_voice(s2['voice_secs']), s1['voice_secs'] > s2['voice_secs'], s1['voice_secs'] < s2['voice_secs']),
            ("Msg Rank", f"#{s1['rank']}" if s1['rank'] else "N/A", f"#{s2['rank']}" if s2['rank'] else "N/A", (s1['rank'] or 999) < (s2['rank'] or 999), (s2['rank'] or 999) < (s1['rank'] or 999)),
            ("Reactions Received", f"{s1['reacts']:,}", f"{s2['reacts']:,}", s1['reacts'] > s2['reacts'], s1['reacts'] < s2['reacts']),
        ]
        stats_rows_html = ""
        for label, v1, v2, w1, w2 in stat_defs:
            c1 = ' winner' if w1 else ''
            c2 = ' winner' if w2 else ''
            stats_rows_html += f'''<div class="stat-row">
                <div class="stat-cell left"><div class="stat-value{c1}">{v1}</div></div>
                <div class="stat-cell center"><div class="stat-label">{label}</div><div class="stat-divider"></div></div>
                <div class="stat-cell right"><div class="stat-value{c2}">{v2}</div></div>
            </div>\n'''

        # Chart lines — normalize both to same max for fair visual comparison
        chart_w = max(days * 15, 100)
        shared_max = max(max((d[1] for d in s1['daily']), default=0), max((d[1] for d in s2['daily']), default=0))
        line_1 = self._build_svg_line(s1['daily'], key_idx=1, svg_width=chart_w, svg_height=100, forced_max=shared_max)
        line_2 = self._build_svg_line(s2['daily'], key_idx=1, svg_width=chart_w, svg_height=100, forced_max=shared_max)

        chart_date_start = (now_dt - timedelta(days=days - 1)).strftime('%-m/%-d')
        chart_date_end = now_dt.strftime('%-m/%-d')

        # Channel lists
        def _build_ch_html(ch_rows):
            html = ""
            for r in ch_rows:
                ch = self.get_channel_safe(guild, r[0])
                name = ch.name if ch else "deleted"
                if len(name) > 16:
                    name = name[:14] + ".."
                html += f'<div class="ch-item"><span class="ch-hash">#</span><span class="ch-name">{name}</span><span class="ch-count">{r[1]:,}</span></div>\n'
            return html or '<div style="color:#6d6f78;font-size:13px;">No data</div>'

        n1 = user1.display_name
        n2 = user2.display_name
        period_label = f"Last {days} days" if days < 3650 else "All Time"

        data = {
            'period_label': period_label,
            'avatar_1': str(user1.display_avatar.url),
            'name_1': n1[:16] if len(n1) > 16 else n1,
            'tag_1': str(user1),
            'avatar_2': str(user2.display_avatar.url),
            'name_2': n2[:16] if len(n2) > 16 else n2,
            'tag_2': str(user2),
            'stats_rows_html': stats_rows_html,
            'chart_width': chart_w,
            'line_1_points': line_1,
            'line_2_points': line_2,
            'chart_date_start': chart_date_start,
            'chart_date_end': chart_date_end,
            'channels_1_html': _build_ch_html(s1['channels']),
            'channels_2_html': _build_ch_html(s2['channels']),
        }
        buf = await self.card_gen.render('tracker_compare_card', data, width=700)
        if buf:
            return discord.File(buf, filename="compare.png")
        return None

    async def gen_channel_comparison(self, guild, ch1, ch2, days):
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        now_dt = datetime.now(timezone.utc)

        async def _ch_stats(cid):
            msg_row = await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ?", (guild.id, cid, cutoff))
            msgs = msg_row[0] or 0
            contrib_row = await self.db.fetch_one("SELECT count(distinct user_id) FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ?", (guild.id, cid, cutoff))
            contribs = contrib_row[0] or 0
            avg_day = round(msgs / max(days, 1), 1)
            daily_rows = await self.db.fetch_all("SELECT date(timestamp, 'unixepoch') as day, count(*) as msgs FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ? GROUP BY day ORDER BY day", (guild.id, cid, cutoff))
            daily = self._fill_daily(daily_rows, days)
            top_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 4", (guild.id, cid, cutoff))
            return {'msgs': msgs, 'contribs': contribs, 'avg_day': avg_day, 'daily': daily, 'top': top_rows}

        s1 = await _ch_stats(ch1.id)
        s2 = await _ch_stats(ch2.id)

        stat_defs = [
            ("Messages", f"{s1['msgs']:,}", f"{s2['msgs']:,}", s1['msgs'] > s2['msgs'], s1['msgs'] < s2['msgs']),
            ("Contributors", f"{s1['contribs']:,}", f"{s2['contribs']:,}", s1['contribs'] > s2['contribs'], s1['contribs'] < s2['contribs']),
            ("Avg/Day", f"{s1['avg_day']}", f"{s2['avg_day']}", s1['avg_day'] > s2['avg_day'], s1['avg_day'] < s2['avg_day']),
        ]
        stats_rows_html = ""
        for label, v1, v2, w1, w2 in stat_defs:
            c1 = ' winner' if w1 else ''
            c2 = ' winner' if w2 else ''
            stats_rows_html += f'''<div class="stat-row">
                <div class="stat-cell left"><div class="stat-value{c1}">{v1}</div></div>
                <div class="stat-cell center"><div class="stat-label">{label}</div><div class="stat-divider"></div></div>
                <div class="stat-cell right"><div class="stat-value{c2}">{v2}</div></div>
            </div>\n'''

        chart_w = max(days * 15, 100)
        shared_max = max(max((d[1] for d in s1['daily']), default=0), max((d[1] for d in s2['daily']), default=0))
        line_1 = self._build_svg_line(s1['daily'], key_idx=1, svg_width=chart_w, svg_height=100, forced_max=shared_max)
        line_2 = self._build_svg_line(s2['daily'], key_idx=1, svg_width=chart_w, svg_height=100, forced_max=shared_max)

        chart_date_start = (now_dt - timedelta(days=days - 1)).strftime('%-m/%-d')
        chart_date_end = now_dt.strftime('%-m/%-d')

        def _build_contrib_html(top_rows):
            html = ""
            for i, r in enumerate(top_rows):
                u = guild.get_member(r[0])
                name = u.display_name if u else "Unknown"
                if len(name) > 16:
                    name = name[:14] + ".."
                html += f'<div class="contrib-item"><span class="contrib-rank">#{i+1}</span><span class="contrib-name">{name}</span><span class="contrib-count">{r[1]:,}</span></div>\n'
            return html or '<div style="color:#6d6f78;font-size:13px;">No data</div>'

        def _ch_type(ch):
            if isinstance(ch, discord.Thread):
                return "Thread"
            if isinstance(ch, discord.VoiceChannel):
                return "Voice Channel"
            return "Text Channel"

        n1 = ch1.name if len(ch1.name) <= 20 else ch1.name[:18] + ".."
        n2 = ch2.name if len(ch2.name) <= 20 else ch2.name[:18] + ".."
        period_label = f"Last {days} days" if days < 3650 else "All Time"

        data = {
            'period_label': period_label,
            'name_1': n1, 'type_1': _ch_type(ch1),
            'name_2': n2, 'type_2': _ch_type(ch2),
            'stats_rows_html': stats_rows_html,
            'chart_width': chart_w,
            'line_1_points': line_1, 'line_2_points': line_2,
            'chart_date_start': chart_date_start, 'chart_date_end': chart_date_end,
            'contribs_1_html': _build_contrib_html(s1['top']),
            'contribs_2_html': _build_contrib_html(s2['top']),
        }
        buf = await self.card_gen.render('tracker_ch_compare_card', data, width=700)
        if buf:
            return discord.File(buf, filename="ch_compare.png")
        return None

    # --- COMMAND ---

    @app_commands.command(name="tracker", description="Open the Analytics Dashboard.")
    async def tracker_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer()
        img = await self.gen_server_overview(interaction.guild, 14)
        view = DashboardView(self, interaction.guild, interaction.user.id)
        if img:
            await interaction.followup.send(file=img, view=view)
        else:
            await interaction.followup.send("Failed to generate stats card. Playwright may not be available.", ephemeral=True)

    # --- LISTENERS ---

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        ts = int(message.created_at.timestamp())

        # Calculate reply latency if this is a reply
        reply_latency = None
        if message.reference and message.reference.resolved and isinstance(message.reference.resolved, discord.Message):
            delta = (message.created_at - message.reference.resolved.created_at).total_seconds()
            reply_latency = int(delta) if delta > 0 else None

        async with self.db.transaction() as conn:
            await conn.execute(
                "INSERT INTO message_logs (user_id, channel_id, guild_id, timestamp, has_attachment, is_reply, reply_latency, length) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (message.author.id, message.channel.id, message.guild.id, ts, bool(message.attachments), message.reference is not None, reply_latency, len(message.content))
            )
            # Social interactions (mentions)
            if message.mentions:
                for mention in message.mentions:
                    if not mention.bot and mention.id != message.author.id:
                        await conn.execute(
                            "INSERT INTO social_interactions (user_id, target_user_id, guild_id, channel_id, timestamp, interaction_type) VALUES (?, ?, ?, ?, ?, ?)",
                            (message.author.id, mention.id, message.guild.id, message.channel.id, ts, "mention")
                        )
            # Custom emoji tracking
            custom_emojis = re.findall(r'<a?:([\w-]+):(\d+)>', message.content)
            for name, eid in custom_emojis:
                await conn.execute(
                    "INSERT INTO emoji_logs (guild_id, user_id, emoji_id, emoji_name, timestamp, usage_type) VALUES (?, ?, ?, ?, ?, ?)",
                    (message.guild.id, message.author.id, int(eid), name, ts, "text")
                )

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction, user):
        if not reaction.message.guild or user.bot:
            return
        ts = int(datetime.now(timezone.utc).timestamp())
        guild_id = reaction.message.guild.id
        author_id = reaction.message.author.id

        # Log ALL reactions in reaction_logs for social impact tracking
        emoji_name = str(reaction.emoji) if isinstance(reaction.emoji, str) else reaction.emoji.name
        await self.db.execute(
            "INSERT INTO reaction_logs (user_id, target_message_author_id, guild_id, emoji_name, timestamp) VALUES (?, ?, ?, ?, ?)",
            (user.id, author_id, guild_id, emoji_name, ts)
        )

        # Log custom emojis in emoji_logs for emoji overview
        if isinstance(reaction.emoji, (discord.Emoji, discord.PartialEmoji)) and reaction.emoji.id:
            await self.db.execute(
                "INSERT INTO emoji_logs (guild_id, user_id, emoji_id, emoji_name, timestamp, usage_type) VALUES (?, ?, ?, ?, ?, ?)",
                (guild_id, user.id, reaction.emoji.id, reaction.emoji.name, ts, "reaction")
            )

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if member.bot or not member.guild:
            return
        key = (member.id, member.guild.id)
        now = int(datetime.now(timezone.utc).timestamp())

        # Left or moved from a channel — end that session
        if before.channel and (not after.channel or before.channel.id != after.channel.id):
            prev = self._voice_join_times.pop(key, None)
            if prev:
                join_ts, channel_id = prev
                duration = now - join_ts
                if duration > 5:
                    await self.db.execute(
                        "INSERT INTO voice_sessions (user_id, channel_id, guild_id, start_time, end_time, duration) VALUES (?,?,?,?,?,?)",
                        (member.id, channel_id, member.guild.id, join_ts, now, duration)
                    )
                    logger.info(f"Voice session saved: user={member.id} dur={duration}s ({duration//60}m) ch={channel_id}")
            else:
                # User left VC but we had no join record — they were in VC before bot started and weren't reseeded, or a restart lost it
                logger.warning(f"Voice leave with no join record: user={member.id} ch={before.channel.id}")

        # Joined or moved to a channel — start new session
        if after.channel and (not before.channel or before.channel.id != after.channel.id):
            self._voice_join_times[key] = (now, after.channel.id)
            logger.info(f"Voice join tracked: user={member.id} ch={after.channel.id}")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot:
            return
        ts = int(datetime.now(timezone.utc).timestamp())
        await self.db.execute(
            "INSERT INTO member_events (user_id, guild_id, event_type, timestamp) VALUES (?,?,?,?)",
            (member.id, member.guild.id, 'join', ts)
        )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        if member.bot:
            return
        ts = int(datetime.now(timezone.utc).timestamp())
        await self.db.execute(
            "INSERT INTO member_events (user_id, guild_id, event_type, timestamp) VALUES (?,?,?,?)",
            (member.id, member.guild.id, 'leave', ts)
        )

    # --- TASKS ---

    @tasks.loop(hours=24)
    async def data_retention_task(self):
        try:
            cutoff = int((datetime.now(timezone.utc) - timedelta(days=DATA_RETENTION_DAYS)).timestamp())
            await self.db.prune_old_data(cutoff)
        except Exception as e:
            await self.bot.error_reporter.report("Tracker", f"data_retention_task: {e}")

    @tasks.loop(minutes=5)
    async def voice_checkpoint_task(self):
        """Periodically save active voice sessions to DB so data survives crashes."""
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            saved = 0
            for (uid, gid), (join_ts, cid) in list(self._voice_join_times.items()):
                duration = now - join_ts
                if duration < 30:
                    continue
                # Write a session up to now, then reset the join time to now
                await self.db.execute(
                    "INSERT INTO voice_sessions (user_id, channel_id, guild_id, start_time, end_time, duration) VALUES (?,?,?,?,?,?)",
                    (uid, cid, gid, join_ts, now, duration)
                )
                self._voice_join_times[(uid, gid)] = (now, cid)
                saved += 1
            if saved:
                logger.info(f"Voice checkpoint: saved {saved} active sessions.")
        except Exception as e:
            await self.bot.error_reporter.report("Tracker", f"voice_checkpoint_task: {e}")


async def setup(bot):
    await bot.add_cog(UserTracker(bot))
