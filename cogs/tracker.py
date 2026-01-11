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
from collections import defaultdict
from contextlib import asynccontextmanager
import functools
import numpy as np

# --- IMAGE GENERATION IMPORTS ---
import matplotlib
matplotlib.use('Agg') 
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.projections.polar import PolarAxes
from matplotlib.projections import register_projection
from matplotlib.spines import Spine
from matplotlib.transforms import Affine2D
from PIL import Image, ImageDraw, ImageFont

# --- CONFIGURATION ---
TRACKING_DB = "tracking_data.db"
CONVERSATION_WINDOW = 120
CACHE_CLEANUP_INTERVAL = 120 
DATA_RETENTION_DAYS = 365
MSG_CACHE_TTL = 300 
MAX_CACHE_SIZE = 10000
EMOJI_PAGE_SIZE = 8

# Colors
COLOR_BG = "#0f1012"         
COLOR_SURFACE = "#1e1f22"    
COLOR_HEADER = "#2b2d31"     
COLOR_TEXT_MAIN = "#ffffff"
COLOR_TEXT_DIM = "#b5bac1"
COLOR_ACCENT_PRIMARY = "#5865F2" 
COLOR_ACCENT_GREEN = "#23a559"
COLOR_ACCENT_RED = "#da373c"
COLOR_BORDER = "#1e1f22"

logger = logging.getLogger('betting_bot.tracker')

# --- RADAR CHART SETUP ---
class RadarAxes(PolarAxes):
    name = 'radar'
    RESOLUTION = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_theta_zero_location('N')

    def fill(self, *args, closed=True, **kwargs):
        return super().fill(closed=closed, *args, **kwargs)

    def plot(self, *args, **kwargs):
        lines = super().plot(*args, **kwargs)
        for line in lines:
            self._close_line(line)

    def _close_line(self, line):
        x, y = line.get_data()
        if x[0] != x[-1]:
            x = np.append(x, x[0])
            y = np.append(y, y[0])
            line.set_data(x, y)

    def set_varlabels(self, labels):
        self.set_thetagrids(np.degrees(np.linspace(0, 2*np.pi, len(labels), endpoint=False)), labels)

    def _gen_axes_patch(self):
        return matplotlib.patches.Circle((0.5, 0.5), 0.5)

    def _gen_axes_spines(self):
        return super()._gen_axes_spines()

if 'radar' not in matplotlib.projections.get_projection_names():
    register_projection(RadarAxes)

# --- DATABASE MANAGER ---
class TrackingDB:
    def __init__(self):
        self.db_path = TRACKING_DB
        self._db: typing.Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def connect(self):
        async with self._lock:
            if self._db: return
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
        if not self._db: await self.connect()

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
        
        # INACTIVITY TABLES
        await self._db.execute("CREATE TABLE IF NOT EXISTS inactivity_config (guild_id INTEGER PRIMARY KEY, log_channel_id INTEGER, msg_threshold INTEGER DEFAULT 5, period_days INTEGER DEFAULT 30, highlight_role_id INTEGER)")
        await self._db.execute("CREATE TABLE IF NOT EXISTS user_inactivity_status (guild_id INTEGER, user_id INTEGER, status TEXT, snooze_until INTEGER, PRIMARY KEY (guild_id, user_id))")

        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_msg_user_time ON message_logs(user_id, timestamp)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_msg_channel_time ON message_logs(channel_id, timestamp)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_social_target ON social_interactions(target_user_id, guild_id)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_emoji_id ON emoji_logs(emoji_id, guild_id)")
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
            await self._db.commit()

# --- IMAGE GENERATOR ---
class StatsImageGenerator:
    def __init__(self):
        try:
            # BOLD FONTS FOR ALL TEXT SIZES
            self.font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 26)
            self.font_reg = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22) 
            self.font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18) 
            self.font_header = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 40)
            self.font_stat = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 60)
        except:
            self.font_bold = ImageFont.load_default()
            self.font_reg = ImageFont.load_default()
            self.font_small = ImageFont.load_default()
            self.font_header = ImageFont.load_default()
            self.font_stat = ImageFont.load_default()

    def get_avg_color(self, img_bytes):
        try:
            img = Image.open(io.BytesIO(img_bytes)).convert("RGB").resize((1, 1))
            return '#%02x%02x%02x' % img.getpixel((0, 0))
        except: return COLOR_ACCENT_PRIMARY

    def draw_rounded_rect(self, draw, xy, color, rad=15):
        draw.rounded_rectangle(xy, radius=rad, fill=color)

    def _render_radar_chart(self, stats_dict, color_hex):
        labels = list(stats_dict.keys())
        values = list(stats_dict.values())
        fig = Figure(figsize=(4, 4), dpi=100)
        fig.patch.set_alpha(0.0)
        ax = fig.add_subplot(111, projection='radar')
        ax.patch.set_alpha(0.0)
        theta = np.linspace(0, 2*np.pi, len(labels), endpoint=False)
        ax.plot(theta, values, color=color_hex, linewidth=2)
        ax.fill(theta, values, facecolor=color_hex, alpha=0.3)
        ax.set_varlabels(labels)
        ax.set_yticklabels([])
        ax.spines['polar'].set_color(COLOR_TEXT_DIM)
        ax.spines['polar'].set_alpha(0.3)
        ax.tick_params(axis='x', colors=COLOR_TEXT_DIM)
        canvas = FigureCanvasAgg(fig)
        canvas.draw()
        return Image.frombuffer("RGBA", canvas.get_width_height(), canvas.buffer_rgba(), "raw", "RGBA", 0, 1)

    def generate_card(self, title, subtitle, icon_bytes, stats_top, list_left, list_right, radar_data=None):
        W, H = 800, 1000
        bg = Image.new("RGB", (W, H), COLOR_BG)
        draw = ImageDraw.Draw(bg)
        accent = self.get_avg_color(icon_bytes) if icon_bytes else COLOR_ACCENT_PRIMARY

        self.draw_rounded_rect(draw, (20, 20, 780, 140), COLOR_HEADER)
        if icon_bytes:
            try:
                icon = Image.open(io.BytesIO(icon_bytes)).convert("RGBA").resize((80, 80))
                mask = Image.new("L", (80, 80), 0)
                ImageDraw.Draw(mask).ellipse((0, 0, 80, 80), fill=255)
                bg.paste(icon, (40, 30), mask)
            except: pass
        
        draw.text((140, 40), title, font=self.font_header, fill=COLOR_TEXT_MAIN)
        draw.text((140, 95), subtitle, font=self.font_small, fill=COLOR_TEXT_DIM)
        draw.rounded_rectangle((20, 20, 30, 140), radius=15, fill=accent)

        for i, (label, val, growth, col) in enumerate(stats_top):
            x = 20 + (i * 390)
            y = 160
            self.draw_rounded_rect(draw, (x, y, x+370, y+200), COLOR_SURFACE)
            draw.text((x+25, y+25), label.upper(), font=self.font_small, fill=COLOR_TEXT_DIM)
            draw.text((x+25, y+60), str(val), font=self.font_stat, fill=COLOR_TEXT_MAIN)
            if growth is not None:
                sym = "▲" if growth >= 0 else "▼"
                g_col = COLOR_ACCENT_GREEN if growth >= 0 else COLOR_ACCENT_RED
                draw.text((x+25, y+140), f"{sym} {abs(growth)}% vs prev", font=self.font_small, fill=g_col)
            draw.rectangle((x, y+195, x+370, y+200), fill=col)

        self.draw_rounded_rect(draw, (20, 380, 390, 920), COLOR_SURFACE)
        draw.text((40, 400), list_left[0], font=self.font_bold, fill=COLOR_TEXT_MAIN)
        y_item = 460
        for idx, (name, val) in enumerate(list_left[1][:8]):
            draw.text((40, y_item), f"#{idx+1}", font=self.font_reg, fill=COLOR_ACCENT_PRIMARY)
            d_name = name[:14] + ".." if len(name) > 14 else name
            draw.text((100, y_item), d_name, font=self.font_reg, fill=COLOR_TEXT_MAIN)
            val_str = f"{val:,}"
            w = draw.textbbox((0,0), val_str, font=self.font_reg)[2]
            draw.text((370-w, y_item), val_str, font=self.font_reg, fill=COLOR_TEXT_DIM)
            y_item += 55

        self.draw_rounded_rect(draw, (410, 380, 780, 920), COLOR_SURFACE)
        draw.text((430, 400), list_right[0], font=self.font_bold, fill=COLOR_TEXT_MAIN)

        if radar_data:
            radar_img = self._render_radar_chart(radar_data, accent)
            bg.paste(radar_img, (400, 460), radar_img)
            draw.text((480, 880), "Interaction Profile", font=self.font_small, fill=COLOR_TEXT_DIM)
        else:
            y_item = 460
            for idx, (name, val) in enumerate(list_right[1][:8]):
                draw.text((430, y_item), f"#{idx+1}", font=self.font_reg, fill=COLOR_ACCENT_PRIMARY)
                d_name = name[:14] + ".." if len(name) > 14 else name
                draw.text((490, y_item), d_name, font=self.font_reg, fill=COLOR_TEXT_MAIN)
                val_str = f"{val:,}"
                w = draw.textbbox((0,0), val_str, font=self.font_reg)[2]
                draw.text((760-w, y_item), val_str, font=self.font_reg, fill=COLOR_TEXT_DIM)
                y_item += 55

        draw.text((300, 960), "Generated by Vibey", font=self.font_small, fill=COLOR_TEXT_DIM)
        out = io.BytesIO()
        bg.save(out, format='PNG')
        out.seek(0)
        return out

    def generate_emoji_card(self, title, subtitle, icon_bytes, emoji_data, page_num, total_pages):
        W, H = 800, 1000
        bg = Image.new("RGB", (W, H), COLOR_BG)
        draw = ImageDraw.Draw(bg)
        accent = self.get_avg_color(icon_bytes) if icon_bytes else COLOR_ACCENT_PRIMARY

        self.draw_rounded_rect(draw, (20, 20, 780, 140), COLOR_HEADER)
        if icon_bytes:
            try:
                icon = Image.open(io.BytesIO(icon_bytes)).convert("RGBA").resize((80, 80))
                mask = Image.new("L", (80, 80), 0)
                ImageDraw.Draw(mask).ellipse((0, 0, 80, 80), fill=255)
                bg.paste(icon, (40, 30), mask)
            except: pass
        
        draw.text((140, 40), title, font=self.font_header, fill=COLOR_TEXT_MAIN)
        draw.text((140, 95), subtitle, font=self.font_small, fill=COLOR_TEXT_DIM)
        draw.rounded_rectangle((20, 20, 30, 140), radius=15, fill=accent)

        y_start = 180
        if not emoji_data:
            draw.text((250, 500), "No Emoji Usage Data Yet", font=self.font_reg, fill=COLOR_TEXT_DIM)
        
        for i, (e_name, e_count, e_users, e_img_bytes) in enumerate(emoji_data):
            y = y_start + (i * 95)
            self.draw_rounded_rect(draw, (20, y, 780, y+85), COLOR_SURFACE)
            rank = (page_num - 1) * 8 + (i + 1)
            draw.text((40, y+30), f"#{rank}", font=self.font_bold, fill=COLOR_ACCENT_PRIMARY)
            if e_img_bytes:
                try:
                    e_icon = Image.open(io.BytesIO(e_img_bytes)).convert("RGBA").resize((50, 50))
                    bg.paste(e_icon, (100, y+17), e_icon)
                except: pass
            draw.text((170, y+30), e_name, font=self.font_reg, fill=COLOR_TEXT_MAIN)
            stat_text = f"{e_count} uses • {e_users} users"
            w = draw.textbbox((0,0), stat_text, font=self.font_small)[2]
            draw.text((760-w, y+32), stat_text, font=self.font_small, fill=COLOR_TEXT_DIM)

        draw.text((300, 960), f"Page {page_num}/{total_pages} • Vibey", font=self.font_small, fill=COLOR_TEXT_DIM)
        out = io.BytesIO()
        bg.save(out, format='PNG')
        out.seek(0)
        return out

# --- ALERT VIEW (Inactivity) ---
class InactivityAlertView(ui.View):
    def __init__(self, cog, user_id):
        super().__init__(timeout=86400) 
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not self.cog.bot.is_bot_admin(interaction.user):
            await interaction.response.send_message("❌ Only administrators can use these controls.", ephemeral=True)
            return False
        return True

    @ui.button(label="Kick User", style=discord.ButtonStyle.danger, emoji="👢")
    async def kick_btn(self, interaction, button):
        member = interaction.guild.get_member(self.user_id)
        if member:
            try:
                await member.send(f"You have been kicked from **{interaction.guild.name}** due to inactivity.")
            except: pass 
            
            try:
                await member.kick(reason="Inactivity Monitor Auto-Kick")
                await interaction.response.send_message(f"👢 Kicked {member.mention}.", ephemeral=False)
                await self.cog.db.execute("DELETE FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (interaction.guild.id, self.user_id))
                self.stop()
            except Exception as e:
                await interaction.response.send_message(f"❌ Failed to kick user: {e}", ephemeral=True)
        else:
            await interaction.response.send_message("User no longer in server.", ephemeral=True)

    @ui.button(label="Snooze (Reset)", style=discord.ButtonStyle.primary, emoji="💤")
    async def snooze_btn(self, interaction, button):
        row = await self.cog.db.fetch_one("SELECT period_days FROM inactivity_config WHERE guild_id = ?", (interaction.guild.id,))
        days = row['period_days'] if row else 30
        snooze_until = int((datetime.now(timezone.utc) + timedelta(days=days)).timestamp())
        await self.cog.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'snoozed', ?)", (interaction.guild.id, self.user_id, snooze_until))
        await interaction.response.send_message(f"💤 Snoozed tracking for <@{self.user_id}> for {days} days.", ephemeral=False)
        self.stop()

    @ui.button(label="Forget User", style=discord.ButtonStyle.secondary, emoji="❌")
    async def forget_btn(self, interaction, button):
        await self.cog.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'forgotten', 0)", (interaction.guild.id, self.user_id))
        await interaction.response.send_message(f"❌ Permanently ignoring inactivity for <@{self.user_id}>.", ephemeral=False)
        self.stop()

# --- DASHBOARD VIEW ---
class DashboardView(ui.View):
    def __init__(self, cog, guild, admin_id):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild
        self.admin_id = admin_id
        
        self.mode = "server"
        self.target_id = None
        self.time_filter = "7d"
        self.emoji_page = 1
        self.showing_time_menu = False
        
        self.update_components()

    def update_components(self):
        self.clear_items()
        
        # Time Menu
        if self.showing_time_menu:
            self.add_item(self.TimeOption("24 Hours", "1d"))
            self.add_item(self.TimeOption("7 Days", "7d"))
            self.add_item(self.TimeOption("30 Days", "1mo"))
            self.add_item(self.TimeOption("All Time", "all"))
            self.add_item(self.BackButton())
            return

        # Config Mode (Admin Only View)
        if self.mode == "config":
            self.add_item(self.ModeSelect(self.mode))
            self.add_item(self.ConfigChannelSelect())
            self.add_item(self.ConfigRoleSelect())
            self.add_item(self.ConfigButton("Set Threshold", "threshold", discord.ButtonStyle.primary))
            self.add_item(self.ConfigButton("Set Period", "period", discord.ButtonStyle.secondary))
            self.add_item(self.TestButton())
            return

        # Standard Dashboard
        self.add_item(self.ModeSelect(self.mode))

        if self.mode == "user":
            self.add_item(self.UserSelect())
        elif self.mode == "channel":
            self.add_item(self.ChannelSelect())
        
        self.add_item(self.ClockButton())
        self.add_item(self.RefreshButton())
        
        if self.mode == "emoji":
            self.add_item(self.PagePrev())
            self.add_item(self.PageNext())

    async def refresh_embed(self, interaction):
        await interaction.response.defer()
        self.update_components()
        
        # HANDLE CONFIG MODE
        if self.mode == "config":
            if not self.cog.bot.is_bot_admin(interaction.user):
                self.mode = "server" # Revert
                await interaction.followup.send("❌ You need Administrator permissions.", ephemeral=True)
                await self.refresh_embed(interaction)
                return

            row = await self.cog.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (self.guild.id,))
            desc = "### ⚙️ Inactivity Settings\n\n"
            if row:
                chan = self.guild.get_channel(row['log_channel_id'])
                role = self.guild.get_role(row['highlight_role_id'])
                desc += f"**Log Channel:** {chan.mention if chan else 'Not Set'}\n"
                desc += f"**Highlight Role:** {role.mention if role else 'Not Set'}\n"
                desc += f"**Msg Threshold:** {row['msg_threshold']} messages\n"
                desc += f"**Time Period:** {row['period_days']} days\n"
            else:
                desc += "System not configured."
            
            embed = discord.Embed(description=desc, color=discord.Color.dark_grey())
            await interaction.edit_original_response(content="", attachments=[], embed=embed, view=self)
            return

        # HANDLE STATS MODES
        days_map = {"1d": 1, "7d": 7, "1mo": 30, "all": 3650}
        days = days_map.get(self.time_filter, 7)
        img_file = None
        
        try:
            if self.mode == "server":
                img_file = await self.cog.gen_server_overview(self.guild, days)
            elif self.mode == "user":
                target = self.guild.get_member(self.target_id) if self.target_id else interaction.user
                if target:
                    img_file = await self.cog.gen_user_overview(self.guild, target, days)
                else:
                    img_file = await self.cog.gen_user_overview(self.guild, interaction.user, days)
            elif self.mode == "channel":
                cid = self.target_id if self.target_id else interaction.channel_id
                channel = self.guild.get_channel(cid)
                if channel:
                    img_file = await self.cog.gen_channel_overview(self.guild, channel, days)
                else:
                    return await interaction.followup.send("Channel not found.", ephemeral=True)
            elif self.mode == "emoji":
                img_file = await self.cog.gen_emoji_overview(self.guild, days, self.emoji_page)

            if img_file:
                await interaction.edit_original_response(content="", embed=None, attachments=[img_file], view=self)
        except Exception as e:
            traceback.print_exc()
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    # --- MAIN BUTTONS ---
    class ClockButton(ui.Button):
        def __init__(self): super().__init__(style=discord.ButtonStyle.secondary, emoji="🕒", row=2)
        async def callback(self, interaction):
            self.view.showing_time_menu = True
            self.view.update_components()
            await interaction.response.edit_message(view=self.view)

    class RefreshButton(ui.Button):
        def __init__(self): super().__init__(style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
        async def callback(self, interaction): await self.view.refresh_embed(interaction)

    class TimeOption(ui.Button):
        def __init__(self, label, val):
            super().__init__(style=discord.ButtonStyle.secondary, label=label, row=0)
            self.val = val
        async def callback(self, interaction):
            self.view.time_filter = self.val
            self.view.showing_time_menu = False
            await self.view.refresh_embed(interaction)

    class BackButton(ui.Button):
        def __init__(self): super().__init__(style=discord.ButtonStyle.danger, label="Cancel", row=1)
        async def callback(self, interaction):
            self.view.showing_time_menu = False
            self.view.update_components()
            await interaction.response.edit_message(view=self.view)
            
    class PagePrev(ui.Button):
        def __init__(self): super().__init__(style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
        async def callback(self, interaction):
            if self.view.emoji_page > 1:
                self.view.emoji_page -= 1
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()

    class PageNext(ui.Button):
        def __init__(self): super().__init__(style=discord.ButtonStyle.secondary, emoji="➡️", row=2)
        async def callback(self, interaction):
            days_map = {"1d": 1, "7d": 7, "1mo": 30, "all": 3650}
            days = days_map.get(self.view.time_filter, 7)
            cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
            row = await self.view.cog.db.fetch_one("SELECT count(distinct emoji_id) FROM emoji_logs WHERE guild_id = ? AND timestamp > ?", (self.view.guild.id, cutoff))
            total = row[0] if row else 0
            max_pages = max(1, (total + EMOJI_PAGE_SIZE - 1) // EMOJI_PAGE_SIZE)
            if self.view.emoji_page < max_pages:
                self.view.emoji_page += 1
                await self.view.refresh_embed(interaction)
            else:
                await interaction.response.defer()

    class ModeSelect(ui.Select):
        def __init__(self, current_mode):
            options = [
                discord.SelectOption(label="Server Overview", value="server"),
                discord.SelectOption(label="User Overview", value="user"),
                discord.SelectOption(label="Channel Overview", value="channel"),
                discord.SelectOption(label="Emoji Overview", value="emoji"),
                discord.SelectOption(label="Inactivity Settings ⚙️", value="config")
            ]
            for opt in options:
                if opt.value == current_mode: opt.default = True
            super().__init__(placeholder="Select Dashboard Mode", options=options, row=0)
        async def callback(self, interaction):
            self.view.mode = self.values[0]
            self.view.target_id = None 
            self.view.emoji_page = 1
            await self.view.refresh_embed(interaction)

    class UserSelect(ui.UserSelect):
        def __init__(self): super().__init__(placeholder="Search User...", row=1)
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

    # --- CONFIG COMPONENTS (Settings Mode) ---
    class ConfigChannelSelect(ui.ChannelSelect):
        def __init__(self): super().__init__(placeholder="Set Log Channel", channel_types=[discord.ChannelType.text], row=1)
        async def callback(self, interaction):
            await self.view.cog.db.execute("INSERT INTO inactivity_config (guild_id, log_channel_id) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET log_channel_id = ?", (interaction.guild.id, self.values[0].id, self.values[0].id))
            await self.view.refresh_embed(interaction)

    class ConfigRoleSelect(ui.RoleSelect):
        def __init__(self): super().__init__(placeholder="Set Highlight Role", row=2)
        async def callback(self, interaction):
            await self.view.cog.db.execute("INSERT INTO inactivity_config (guild_id, highlight_role_id) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET highlight_role_id = ?", (interaction.guild.id, self.values[0].id, self.values[0].id))
            await self.view.refresh_embed(interaction)

    class ConfigButton(ui.Button):
        def __init__(self, label, mode, style):
            super().__init__(label=label, style=style, row=3)
            self.mode = mode
        async def callback(self, interaction):
            await interaction.response.send_modal(DashboardView.ConfigModal(interaction.client.get_cog("UserTracker"), self.mode, self.view))

    class TestButton(ui.Button):
        def __init__(self): super().__init__(label="Test Alert", style=discord.ButtonStyle.success, emoji="🧪", row=3)
        async def callback(self, interaction):
            row = await self.view.cog.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (interaction.guild.id,))
            if not row or not row['log_channel_id']: return await interaction.response.send_message("❌ Configure Log Channel first.", ephemeral=True)
            log_channel = interaction.guild.get_channel(row['log_channel_id'])
            
            member = interaction.user
            embed = discord.Embed(title="TEST: Inactivity Alert", description=f"{member.mention} has 0 messages.", color=discord.Color.orange())
            try:
                await log_channel.send(embed=embed, view=InactivityAlertView(self.view.cog, member.id))
                await interaction.response.send_message(f"✅ Sent test to {log_channel.mention}.", ephemeral=True)
            except:
                await interaction.response.send_message("❌ Failed to send. Check bot permissions.", ephemeral=True)

    # --- CONFIG MODAL (Inside DashboardView) ---
    class ConfigModal(ui.Modal):
        def __init__(self, cog, mode, parent_view):
            super().__init__(title=f"Set {mode.capitalize()}")
            self.cog = cog
            self.mode = mode
            self.parent_view = parent_view
            self.val = ui.TextInput(label="Value (Integer)", style=discord.TextStyle.short)
            self.add_item(self.val)
        
        async def on_submit(self, interaction):
            try:
                val = int(self.val.value)
                col = "msg_threshold" if self.mode == "threshold" else "period_days"
                await self.cog.db.execute(f"INSERT INTO inactivity_config (guild_id, {col}) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET {col} = ?", (interaction.guild.id, val, val))
                
                # Manually rebuild config embed
                row = await self.cog.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (interaction.guild.id,))
                desc = "### ⚙️ Inactivity Settings\n\n"
                if row:
                    chan = interaction.guild.get_channel(row['log_channel_id'])
                    role = interaction.guild.get_role(row['highlight_role_id'])
                    desc += f"**Log Channel:** {chan.mention if chan else 'Not Set'}\n"
                    desc += f"**Highlight Role:** {role.mention if role else 'Not Set'}\n"
                    desc += f"**Msg Threshold:** {row['msg_threshold']} messages\n"
                    desc += f"**Time Period:** {row['period_days']} days\n"
                else:
                    desc += "System not configured."
                
                embed = discord.Embed(description=desc, color=discord.Color.dark_grey())
                await interaction.response.edit_message(embed=embed, view=self.parent_view)
            except ValueError:
                await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

# --- MAIN COG ---
class UserTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = TrackingDB()
        self.img_gen = StatsImageGenerator()
        self.session = None

    async def cog_load(self):
        self.session = aiohttp.ClientSession()
        self.bot.loop.create_task(self._async_setup())

    async def _async_setup(self):
        await self.bot.wait_until_ready()
        await self.db.connect()
        self.data_retention_task.start()
        
        # Only start inactivity monitoring if configured (Prevents useless load)
        configured = await self.db.fetch_one("SELECT COUNT(*) FROM inactivity_config WHERE log_channel_id IS NOT NULL")
        if configured and configured[0] > 0:
            await asyncio.sleep(60)
            self.check_inactivity_task.start()

    async def cog_unload(self):
        if self.session: await self.session.close()
        self.data_retention_task.cancel()
        self.check_inactivity_task.cancel()
        await self.db.close()

    # --- HELPERS ---
    def get_channel_safe(self, guild, cid):
        c = guild.get_channel(cid)
        if c: return c
        c = guild.get_thread(cid)
        if c: return c
        for thread in guild.threads:
            if thread.id == cid: return thread
        return None

    # --- DATA FETCHERS ---
    async def get_stats(self, guild_id, days, user_id=None, channel_id=None):
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - (days * 86400)
        prev_cutoff = now - (days * 2 * 86400)
        where_clauses = ["guild_id = ?", "timestamp > ?"]
        params = [guild_id, cutoff]
        if user_id: where_clauses.append("user_id = ?"); params.append(user_id)
        if channel_id: where_clauses.append("channel_id = ?"); params.append(channel_id)
        where_sql = " AND ".join(where_clauses)
        row = await self.db.fetch_one(f"SELECT count(*) FROM message_logs WHERE {where_sql}", tuple(params))
        msgs = row[0] or 0
        prev_sql = where_sql.replace("timestamp > ?", "timestamp BETWEEN ? AND ?")
        prev_params = [guild_id, prev_cutoff, cutoff]
        if user_id: prev_params.append(user_id)
        if channel_id: prev_params.append(channel_id)
        p_row = await self.db.fetch_one(f"SELECT count(*) FROM message_logs WHERE {prev_sql}", tuple(prev_params))
        p_msgs = p_row[0] or 0
        growth = int(((msgs - p_msgs) / p_msgs) * 100) if p_msgs > 0 else 100 if msgs > 0 else 0
        return msgs, growth

    async def get_radar_data(self, guild, user, days):
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
        max_msgs = (await self.db.fetch_one("SELECT count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 1", (guild.id, cutoff)))
        max_msgs = max_msgs[0] if max_msgs and max_msgs[0] > 0 else 1
        max_voice = (await self.db.fetch_one("SELECT sum(duration) as c FROM voice_sessions WHERE guild_id = ? AND start_time > ? GROUP BY user_id ORDER BY c DESC LIMIT 1", (guild.id, cutoff)))
        max_voice = max_voice[0] if max_voice and max_voice[0] > 0 else 1
        max_social = (await self.db.fetch_one("SELECT count(*) as c FROM social_interactions WHERE guild_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 1", (guild.id, cutoff)))
        max_social = max_social[0] if max_social and max_social[0] > 0 else 1
        max_impact = (await self.db.fetch_one("SELECT count(*) as c FROM reaction_logs WHERE guild_id = ? AND timestamp > ? GROUP BY target_message_author_id ORDER BY c DESC LIMIT 1", (guild.id, cutoff)))
        max_impact = max_impact[0] if max_impact and max_impact[0] > 0 else 1
        u_msgs = (await self.db.fetch_one("SELECT count(*) FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?", (guild.id, user.id, cutoff)))[0] or 0
        u_voice = (await self.db.fetch_one("SELECT sum(duration) FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ?", (guild.id, user.id, cutoff)))[0] or 0
        u_social = (await self.db.fetch_one("SELECT count(*) FROM social_interactions WHERE guild_id = ? AND user_id = ? AND timestamp > ?", (guild.id, user.id, cutoff)))[0] or 0
        u_impact = (await self.db.fetch_one("SELECT count(*) FROM reaction_logs WHERE guild_id = ? AND target_message_author_id = ? AND timestamp > ?", (guild.id, user.id, cutoff)))[0] or 0
        return {
            "Activity": min(u_msgs / max_msgs, 1.0),
            "Voice": min(u_voice / max_voice, 1.0),
            "Social": min(u_social / max_social, 1.0),
            "Impact": min(u_impact / max_impact, 1.0),
            "Loyalty": 1.0 
        }

    # --- GENERATORS & COMMANDS ---
    async def gen_server_overview(self, guild, days):
        msgs, growth = await self.get_stats(guild.id, days)
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
        active_row = await self.db.fetch_one("SELECT count(distinct user_id) FROM message_logs WHERE guild_id = ? AND timestamp > ?", (guild.id, cutoff))
        active_users = active_row[0] or 0
        u_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 10", (guild.id, cutoff))
        user_list = []
        for r in u_rows:
            u = guild.get_member(r[0])
            user_list.append((u.name if u else "Unknown", r[1]))
        c_rows = await self.db.fetch_all("SELECT channel_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY channel_id ORDER BY c DESC LIMIT 10", (guild.id, cutoff))
        chan_list = []
        for r in c_rows:
            c = self.get_channel_safe(guild, r[0])
            name = f"#{c.name}" if c else "deleted-channel"
            if c and isinstance(c, (discord.Thread, discord.ForumChannel)): name = f"# {c.name}"
            chan_list.append((name, r[1]))
        stats_top = [("Total Messages", msgs, growth, COLOR_ACCENT_PRIMARY), ("Active Users", active_users, None, COLOR_ACCENT_GREEN)]
        list_left = ("Top Members", user_list)
        list_right = ("Top Channels", chan_list)
        icon = await guild.icon.read() if guild.icon else None
        buf = await asyncio.to_thread(self.img_gen.generate_card, guild.name, f"Server Overview ({days}d)", icon, stats_top, list_left, list_right)
        return discord.File(buf, filename="server.png")

    async def gen_user_overview(self, guild, user, days):
        msgs, growth = await self.get_stats(guild.id, days, user_id=user.id)
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
        co_rows = await self.db.fetch_all("SELECT target_user_id, count(*) as c FROM social_interactions WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY target_user_id ORDER BY c DESC LIMIT 8", (guild.id, user.id, cutoff))
        connections_list = []
        for r in co_rows:
            u = guild.get_member(r[0])
            connections_list.append((u.name if u else "Unknown", r[1]))
        c_rows = await self.db.fetch_all("SELECT channel_id, count(*) as c FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY channel_id ORDER BY c DESC LIMIT 8", (guild.id, user.id, cutoff))
        chan_list = []
        for r in c_rows:
            c = self.get_channel_safe(guild, r[0])
            name = f"#{c.name}" if c else "deleted"
            if c and isinstance(c, (discord.Thread, discord.ForumChannel)): name = f"# {c.name}"
            chan_list.append((name, r[1]))
        radar_data = await self.get_radar_data(guild, user, days)
        stats_top = [("Messages", msgs, growth, COLOR_ACCENT_PRIMARY), ("Interactions", sum(x[1] for x in connections_list), None, COLOR_ACCENT_GREEN)]
        list_left = ("Top Channels", chan_list)
        list_right = ("Radar", radar_data)
        icon = await user.display_avatar.read()
        buf = await asyncio.to_thread(self.img_gen.generate_card, user.display_name, f"User Overview ({days}d)", icon, stats_top, list_left, list_right, radar_data=radar_data)
        return discord.File(buf, filename="user.png")

    async def gen_channel_overview(self, guild, channel, days):
        msgs, growth = await self.get_stats(guild.id, days, channel_id=channel.id)
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
        u_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ? GROUP BY user_id ORDER BY c DESC LIMIT 8", (guild.id, channel.id, cutoff))
        user_list = []
        for r in u_rows:
            u = guild.get_member(r[0])
            user_list.append((u.name if u else "Unknown", r[1]))
        unique = len(u_rows)
        title = f"#{channel.name}"
        if isinstance(channel, (discord.Thread, discord.ForumChannel)): title = f"# {channel.name}"
        stats_top = [("Messages", msgs, growth, COLOR_ACCENT_PRIMARY), ("Chatters", unique, None, COLOR_ACCENT_GREEN)]
        list_left = ("Top Contributors", user_list)
        list_right = ("Top Contributors", []) 
        icon = await guild.icon.read() if guild.icon else None
        buf = await asyncio.to_thread(self.img_gen.generate_card, title, f"Channel Overview ({days}d)", icon, stats_top, list_left, list_right)
        return discord.File(buf, filename="channel.png")

    async def gen_emoji_overview(self, guild, days, page):
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
        rows = await self.db.fetch_all("SELECT emoji_id, emoji_name, count(*) as uses, count(distinct user_id) as users FROM emoji_logs WHERE guild_id = ? AND timestamp > ? GROUP BY emoji_id ORDER BY uses DESC", (guild.id, cutoff))
        total_pages = max(1, (len(rows) + EMOJI_PAGE_SIZE - 1) // EMOJI_PAGE_SIZE)
        if page > total_pages: page = 1
        start = (page - 1) * EMOJI_PAGE_SIZE
        page_rows = rows[start:start+EMOJI_PAGE_SIZE]
        emoji_data = []
        if self.session:
            for r in page_rows:
                eid, ename, count, users = r
                img_bytes = None
                emoji_obj = discord.utils.get(guild.emojis, id=eid)
                url = None
                if emoji_obj:
                    url = str(emoji_obj.url)
                else:
                    url = f"https://cdn.discordapp.com/emojis/{eid}.png"
                if url:
                    try:
                        async with self.session.get(url) as resp:
                            if resp.status == 200: img_bytes = await resp.read()
                    except: pass
                emoji_data.append((ename, count, users, img_bytes))
        icon = await guild.icon.read() if guild.icon else None
        buf = await asyncio.to_thread(self.img_gen.generate_emoji_card, guild.name, f"Emoji Overview ({days}d)", icon, emoji_data, page, total_pages)
        return discord.File(buf, filename="emojis.png")

    @app_commands.command(name="tracker", description="Open the Analytics Dashboard.")
    async def tracker_cmd(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        await interaction.response.defer()
        img = await self.gen_server_overview(interaction.guild, 7)
        view = DashboardView(self, interaction.guild, interaction.user.id)
        await interaction.followup.send(file=img, view=view)

    # --- LISTENERS ---
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot: return
        ts = int(message.created_at.timestamp())
        async with self.db.transaction() as conn:
            await conn.execute("INSERT INTO message_logs (user_id, channel_id, guild_id, timestamp, has_attachment, is_reply, length) VALUES (?, ?, ?, ?, ?, ?, ?)", (message.author.id, message.channel.id, message.guild.id, ts, bool(message.attachments), message.reference is not None, len(message.content)))
            if message.mentions:
                for mention in message.mentions:
                    if not mention.bot and mention.id != message.author.id:
                        await conn.execute("INSERT INTO social_interactions (user_id, target_user_id, guild_id, channel_id, timestamp, interaction_type) VALUES (?, ?, ?, ?, ?, ?)", (message.author.id, mention.id, message.guild.id, message.channel.id, ts, "mention"))
            custom_emojis = re.findall(r'<a?:([\w-]+):(\d+)>', message.content)
            for name, eid in custom_emojis:
                await conn.execute("INSERT INTO emoji_logs (guild_id, user_id, emoji_id, emoji_name, timestamp, usage_type) VALUES (?, ?, ?, ?, ?, ?)", (message.guild.id, message.author.id, int(eid), name, ts, "text"))

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction, user):
        if not reaction.message.guild or user.bot: return
        ts = int(datetime.now(timezone.utc).timestamp())
        if isinstance(reaction.emoji, discord.Emoji):
             async with self.db.transaction() as conn:
                 await conn.execute("INSERT INTO emoji_logs (guild_id, user_id, emoji_id, emoji_name, timestamp, usage_type) VALUES (?, ?, ?, ?, ?, ?)", (reaction.message.guild.id, user.id, reaction.emoji.id, reaction.emoji.name, ts, "reaction"))

    @tasks.loop(hours=24)
    async def data_retention_task(self):
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=DATA_RETENTION_DAYS)).timestamp())
        await self.db.prune_old_data(cutoff)

    @tasks.loop(hours=24)
    async def check_inactivity_task(self):
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            try:
                row = await self.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (guild.id,))
                if not row or not row['log_channel_id']: continue
                log_channel = guild.get_channel(row['log_channel_id'])
                if not log_channel: continue
                highlight_role = guild.get_role(row['highlight_role_id']) if row['highlight_role_id'] else None
                cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=row['period_days'])).timestamp())
                status_rows = await self.db.fetch_all("SELECT user_id, status, snooze_until FROM user_inactivity_status WHERE guild_id = ?", (guild.id,))
                statuses = {r['user_id']: {'status': r['status'], 'snooze_until': r['snooze_until']} for r in status_rows}
                count_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id", (guild.id, cutoff_ts))
                msg_counts = {r['user_id']: r['c'] for r in count_rows}
                updates_to_clear = []
                alerts_to_send = []
                for member in guild.members:
                    if member.bot: continue
                    if not member.joined_at: continue
                    count = msg_counts.get(member.id, 0)
                    if count >= row['msg_threshold']:
                        if member.id in statuses and statuses[member.id]['status'] in ('alerted', 'snoozed'):
                            updates_to_clear.append(member.id)
                        continue
                    join_ts = int(member.joined_at.timestamp())
                    if join_ts > cutoff_ts: continue
                    status_info = statuses.get(member.id)
                    if status_info:
                        if status_info['status'] == 'forgotten': continue
                        if status_info['status'] == 'snoozed' and datetime.now(timezone.utc).timestamp() < status_info['snooze_until']: continue
                        if status_info['status'] == 'alerted': continue
                    alerts_to_send.append((member, count))
                if updates_to_clear:
                    async with self.db.transaction() as conn:
                        for uid in updates_to_clear:
                            await conn.execute("DELETE FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (guild.id, uid))
                for member, count in alerts_to_send:
                    color = discord.Color.red() if (highlight_role and highlight_role in member.roles) else discord.Color.orange()
                    title = "⚠️ VIP Inactivity Alert" if (highlight_role and highlight_role in member.roles) else "Inactivity Alert"
                    embed = discord.Embed(title=title, description=f"{member.mention} has sent **{count}** messages in the last **{row['period_days']}** days (Threshold: {row['msg_threshold']}).", color=color)
                    embed.set_thumbnail(url=member.display_avatar.url)
                    embed.add_field(name="Joined", value=member.joined_at.strftime("%Y-%m-%d"), inline=True)
                    await log_channel.send(embed=embed, view=InactivityAlertView(self, member.id))
                    await self.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'alerted', 0)", (guild.id, member.id))
                    await asyncio.sleep(2)
            except Exception as e: logger.error(f"Inactivity check failed: {e}")
            await asyncio.sleep(1)

async def setup(bot):
    await bot.add_cog(UserTracker(bot))


