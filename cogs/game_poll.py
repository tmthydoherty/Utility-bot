import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import io
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import logging

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

EASTERN = ZoneInfo("America/New_York")

logger = logging.getLogger('bot_main.game_poll')

# --- CONSTANTS & CONFIG ---
DB_PATH = "game_poll.db"
FONTS_PATH = Path(__file__).parent.parent / "fonts"
RESULTS_TEMPLATE_PATH = Path(__file__).parent / "templates" / "results_card.html"
EMBED_COLOR = 0x2b2d31  # Sleek dark grey/blurple standard
SUCCESS_COLOR = 0x57F287 # Green
WINNER_COLOR = 0xFEE75C  # Gold
ERROR_COLOR = 0xED4245   # Red

MAX_GAME_NAME_LEN = 25
MIN_VC_SECONDS = 15 * 60 # 15 minutes to become a returning player

class DB:
    """Helper class for SQLite database operations."""
    @staticmethod
    async def setup():
        async with aiosqlite.connect(DB_PATH) as db:
            # Enable Write-Ahead Logging for high concurrency / race condition prevention
            await db.execute("PRAGMA journal_mode=WAL;")

            await db.execute("""CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, banner_url TEXT
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS active_poll (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER, message_id INTEGER, end_time REAL
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS poll_games (
                poll_id INTEGER, game_id INTEGER
            )""")

            # Added UNIQUE constraint to strictly prevent double voting race conditions
            await db.execute("""CREATE TABLE IF NOT EXISTS votes (
                poll_id INTEGER, user_id INTEGER, game_id INTEGER, rank INTEGER, multiplier_used INTEGER,
                UNIQUE(poll_id, user_id, rank)
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS returning_players (
                user_id INTEGER PRIMARY KEY
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS vc_sessions (
                user_id INTEGER PRIMARY KEY, total_seconds REAL, join_time REAL
            )""")
            # Migration: add join_time column if missing from older schema
            try:
                await db.execute("ALTER TABLE vc_sessions ADD COLUMN join_time REAL")
            except Exception:
                pass  # Column already exists
            await db.execute("""CREATE TABLE IF NOT EXISTS poll_messages (
                poll_id INTEGER, channel_id INTEGER, message_id INTEGER
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS poll_results (
                poll_id INTEGER PRIMARY KEY, data_json TEXT
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS results_messages (
                poll_id INTEGER, channel_id INTEGER, message_id INTEGER
            )""")

            # Default Weights
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('weight_1', '3')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('weight_2', '2')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('weight_3', '1')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('game_night_banner_url', '')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('poll_channel_ids', '[]')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('secondary_poll_channel_ids', '[]')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('game_night_role_id', '')")
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ping_role_enabled', '0')")
            await db.commit()

    @staticmethod
    async def get_setting(key: str, default=None):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else default

    @staticmethod
    async def set_setting(key: str, value: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
            await db.commit()

# --- UI MODALS ---

class GameModal(discord.ui.Modal):
    def __init__(self, panel, game_id=None, current_name=""):
        super().__init__(title="Manage Game" if game_id else "Add New Game")
        self.panel = panel
        self.game_id = game_id

        self.name_input = discord.ui.TextInput(
            label="Game Name",
            placeholder="e.g. Halo Infinite",
            default=current_name,
            max_length=MAX_GAME_NAME_LEN,
            required=True
        )
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name_input.value.strip()

        async with aiosqlite.connect(DB_PATH) as db:
            if self.game_id:
                await db.execute("UPDATE games SET name = ? WHERE id = ?", (name, self.game_id))
            else:
                await db.execute("INSERT INTO games (name) VALUES (?)", (name,))
            await db.commit()

        await interaction.response.defer()
        await self.panel.show_games(interaction, self.panel.games_page)

class WeightsModal(discord.ui.Modal, title="Set Voting Weights"):
    def __init__(self, panel, w1="3", w2="2", w3="1"):
        super().__init__()
        self.panel = panel
        self.weight_1 = discord.ui.TextInput(label="1st Place Points", default=str(w1), required=True)
        self.weight_2 = discord.ui.TextInput(label="2nd Place Points", default=str(w2), required=True)
        self.weight_3 = discord.ui.TextInput(label="3rd Place Points", default=str(w3), required=True)
        self.add_item(self.weight_1)
        self.add_item(self.weight_2)
        self.add_item(self.weight_3)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            w1, w2, w3 = int(self.weight_1.value), int(self.weight_2.value), int(self.weight_3.value)
        except ValueError:
            await interaction.response.defer()
            return await self.panel.show_home(interaction)

        await DB.set_setting('weight_1', w1)
        await DB.set_setting('weight_2', w2)
        await DB.set_setting('weight_3', w3)
        await interaction.response.defer()
        await self.panel.show_home(interaction)

class CloseTimeModal(discord.ui.Modal, title="Choose When the Poll Closes"):
    date_input = discord.ui.TextInput(
        label="Date (MM/DD or MM/DD/YYYY)",
        placeholder="e.g. 3/28 or 03/28/2026",
        required=True
    )
    time_input = discord.ui.TextInput(
        label="Time (Eastern)",
        placeholder="e.g. 7:00pm, 9:30 PM, 14:00",
        required=True
    )

    def __init__(self, panel):
        super().__init__()
        self.panel = panel

    async def on_submit(self, interaction: discord.Interaction):
        now_et = datetime.now(EASTERN)
        raw_date = self.date_input.value.strip()
        raw_time = self.time_input.value.strip()

        # Parse date
        try:
            parts = raw_date.replace("-", "/").split("/")
            month, day = int(parts[0]), int(parts[1])
            year = int(parts[2]) if len(parts) >= 3 else now_et.year
        except (ValueError, IndexError):
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        # Parse time — accept 7pm, 7:00pm, 7:00 PM, 14:00, etc.
        time_str = raw_time.upper().replace(" ", "")
        hour = minute = None
        for fmt in ("%I:%M%p", "%I%p", "%H:%M"):
            try:
                parsed = datetime.strptime(time_str, fmt)
                hour, minute = parsed.hour, parsed.minute
                break
            except ValueError:
                continue

        if hour is None:
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        try:
            end_time = datetime(year, month, day, hour, minute, tzinfo=EASTERN)
        except ValueError:
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        if end_time <= now_et:
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        self.panel.draft_end_time = end_time
        await interaction.response.defer()
        await self.panel.show_draft(interaction)

class GameNightTimeModal(discord.ui.Modal, title="When is Game Night?"):
    date_input = discord.ui.TextInput(
        label="Date (MM/DD or MM/DD/YYYY)",
        placeholder="e.g. 3/28 or 03/28/2026",
        required=True
    )
    time_input = discord.ui.TextInput(
        label="Time (Eastern)",
        placeholder="e.g. 7:00pm, 9:30 PM, 14:00",
        required=True
    )

    def __init__(self, panel):
        super().__init__()
        self.panel = panel

    async def on_submit(self, interaction: discord.Interaction):
        now_et = datetime.now(EASTERN)
        raw_date = self.date_input.value.strip()
        raw_time = self.time_input.value.strip()

        try:
            parts = raw_date.replace("-", "/").split("/")
            month, day = int(parts[0]), int(parts[1])
            year = int(parts[2]) if len(parts) >= 3 else now_et.year
        except (ValueError, IndexError):
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        time_str = raw_time.upper().replace(" ", "")
        hour = minute = None
        for fmt in ("%I:%M%p", "%I%p", "%H:%M"):
            try:
                parsed = datetime.strptime(time_str, fmt)
                hour, minute = parsed.hour, parsed.minute
                break
            except ValueError:
                continue

        if hour is None:
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        try:
            gn_time = datetime(year, month, day, hour, minute, tzinfo=EASTERN)
        except ValueError:
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        if gn_time <= now_et:
            await interaction.response.defer()
            return await self.panel.show_draft(interaction)

        self.panel.draft_game_night_time = gn_time
        await interaction.response.defer()
        await self.panel.show_draft(interaction)

class VCModal(discord.ui.Modal, title="Create Game Night VC"):
    vc_name = discord.ui.TextInput(label="Voice Channel Name (Optional)", default="Game Night", required=False)

    def __init__(self, cog, panel):
        super().__init__()
        self.cog = cog
        self.panel = panel

    async def on_submit(self, interaction: discord.Interaction):
        category_id = await DB.get_setting('vc_category_id')
        if not category_id:
            await interaction.response.defer()
            return await self.panel.show_home(interaction)

        category = interaction.guild.get_channel(int(category_id))
        if not category:
            await interaction.response.defer()
            return await self.panel.show_home(interaction)

        name = self.vc_name.value.strip() or "Game Night"

        active_vc = await DB.get_setting('active_vc_id')
        if active_vc and interaction.guild.get_channel(int(active_vc)):
            await interaction.response.defer()
            return await self.panel.show_home(interaction)

        await interaction.response.defer()

        try:
            vc = await interaction.guild.create_voice_channel(
                name=name,
                category=category,
                position=0
            )
            await DB.set_setting('active_vc_id', vc.id)
            self.cog.vc_empty_minutes = 0
        except discord.Forbidden:
            pass

        await self.panel.show_home(interaction)


# --- ADMIN PANEL ---

class AdminPanel:
    """Manages state and rendering for the single-message admin panel."""

    def __init__(self, cog):
        self.cog = cog
        self.msg = None
        self.draft_game_ids = []
        self.draft_end_time = None
        self.draft_game_night_time = None
        self.games_page = 0

    async def load_draft(self):
        raw_ids = await DB.get_setting('draft_game_ids', '')
        raw_time = await DB.get_setting('draft_end_time', '')
        raw_gn_time = await DB.get_setting('draft_game_night_time', '')
        if raw_ids:
            self.draft_game_ids = [int(x) for x in raw_ids.split(',') if x]
        if raw_time:
            try:
                self.draft_end_time = datetime.fromtimestamp(float(raw_time), tz=EASTERN)
                if self.draft_end_time <= datetime.now(EASTERN):
                    self.draft_end_time = None
            except (ValueError, OSError):
                self.draft_end_time = None
        if raw_gn_time:
            try:
                self.draft_game_night_time = datetime.fromtimestamp(float(raw_gn_time), tz=EASTERN)
            except (ValueError, OSError):
                self.draft_game_night_time = None

    async def save_draft(self):
        ids_str = ','.join(str(x) for x in self.draft_game_ids)
        time_str = str(self.draft_end_time.timestamp()) if self.draft_end_time else ''
        gn_time_str = str(self.draft_game_night_time.timestamp()) if self.draft_game_night_time else ''
        await DB.set_setting('draft_game_ids', ids_str)
        await DB.set_setting('draft_end_time', time_str)
        await DB.set_setting('draft_game_night_time', gn_time_str)

    async def clear_draft(self):
        self.draft_game_ids = []
        self.draft_end_time = None
        self.draft_game_night_time = None
        await DB.set_setting('draft_game_ids', '')
        await DB.set_setting('draft_end_time', '')
        await DB.set_setting('draft_game_night_time', '')

    async def edit(self, interaction, **kwargs):
        if not interaction.response.is_done():
            await interaction.response.edit_message(**kwargs)
        elif self.msg:
            await self.msg.edit(**kwargs)

    # --- SCREENS ---

    async def show_home(self, interaction, is_initial=False):
        embed = discord.Embed(title="Game Night Control Center", color=EMBED_COLOR)

        active_poll = await DB.get_setting('active_poll_id')
        active_vc = await DB.get_setting('active_vc_id')

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM returning_players") as cur:
                ret_players = (await cur.fetchone())[0]

        has_draft = bool(self.draft_game_ids) or bool(self.draft_end_time)

        role_id = await DB.get_setting('game_night_role_id')
        ping_enabled = await DB.get_setting('ping_role_enabled', '0')
        guild = interaction.guild
        role = guild.get_role(int(role_id)) if role_id else None

        status_parts = []
        status_parts.append(f"**Active Poll:** {'Yes' if active_poll else 'None'}")
        status_parts.append(f"**Active VC:** {'Yes' if active_vc else 'None'}")
        status_parts.append(f"**Returning Players:** {ret_players}")
        role_text = f"{role.mention} (Ping: {'ON' if ping_enabled == '1' else 'OFF'})" if role else "*Not set*"
        status_parts.append(f"**Game Night Role:** {role_text}")
        if has_draft:
            status_parts.append(f"**Saved Draft:** {len(self.draft_game_ids)} game(s) selected")
        embed.description = "\n".join(status_parts)

        view = HomeView(self, has_active_poll=bool(active_poll), has_active_vc=bool(active_vc), has_draft=has_draft)

        if is_initial:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            self.msg = await interaction.original_response()
        else:
            await self.edit(interaction, embed=embed, view=view)

    async def show_draft(self, interaction):
        embed = discord.Embed(title="Draft Poll", color=EMBED_COLOR)

        if self.draft_game_ids:
            async with aiosqlite.connect(DB_PATH) as db:
                placeholders = ",".join("?" for _ in self.draft_game_ids)
                async with db.execute(f"SELECT id, name FROM games WHERE id IN ({placeholders})", self.draft_game_ids) as cur:
                    game_rows = {r[0]: r[1] for r in await cur.fetchall()}
            # Filter out deleted games, preserve order
            valid_ids = [gid for gid in self.draft_game_ids if gid in game_rows]
            self.draft_game_ids = valid_ids
            games_text = "\n".join(f"**{game_rows[gid]}**" for gid in valid_ids)
            embed.add_field(name=f"Selected Games ({len(valid_ids)})", value=games_text or "*None*", inline=False)
        else:
            embed.add_field(name="Selected Games", value="*Use the dropdown below to pick 2-8 games*", inline=False)

        if self.draft_end_time:
            if self.draft_end_time <= datetime.now(EASTERN):
                self.draft_end_time = None
                embed.add_field(name="Closes", value="*Previous time expired — set a new one*", inline=False)
            else:
                ts = int(self.draft_end_time.timestamp())
                embed.add_field(name="Closes", value=f"<t:{ts}:F> (<t:{ts}:R>)", inline=False)
        else:
            embed.add_field(name="Closes", value="*Not set*", inline=False)

        if self.draft_game_night_time:
            gn_ts = int(self.draft_game_night_time.timestamp())
            embed.add_field(name="Game Night", value=f"<t:{gn_ts}:F> (<t:{gn_ts}:R>)", inline=False)
        else:
            embed.add_field(name="Game Night", value="*Not set (optional)*", inline=False)

        active_poll = await DB.get_setting('active_poll_id')
        ready = len(self.draft_game_ids) >= 2 and self.draft_end_time is not None and not active_poll
        if active_poll:
            embed.set_footer(text="A poll is currently active — end it before posting a new one.")

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT id, name FROM games") as cursor:
                all_games = await cursor.fetchall()

        view = DraftView(self, all_games, ready)
        await self.edit(interaction, embed=embed, view=view)

    async def show_games(self, interaction, page=0):
        self.games_page = page
        per_page = 5

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT id, name, banner_url FROM games LIMIT ? OFFSET ?", (per_page + 1, page * per_page)) as cursor:
                games = await cursor.fetchall()

        has_next = len(games) > per_page
        display_games = games[:per_page]

        embed = discord.Embed(title="Game Database", color=EMBED_COLOR)
        if not display_games:
            embed.description = "No games added yet."
        else:
            desc = ""
            for g in display_games:
                banner_tag = " [Banner]" if g[2] else ""
                desc += f"**{g[1]}**{banner_tag} *(ID: {g[0]})*\n"
            embed.description = desc

        view = GamesView(self, display_games, page, has_next)
        await self.edit(interaction, embed=embed, view=view)

    async def show_channels(self, interaction):
        embed = discord.Embed(title="Channel Configuration", color=EMBED_COLOR)

        raw_ids = await DB.get_setting('poll_channel_ids', '[]')
        channel_ids = json.loads(raw_ids)
        raw_secondary = await DB.get_setting('secondary_poll_channel_ids', '[]')
        secondary_ids = json.loads(raw_secondary)
        vc_cat_id = await DB.get_setting('vc_category_id')

        guild = interaction.guild
        primary_mentions = []
        for cid in channel_ids:
            ch = guild.get_channel(int(cid))
            if ch:
                primary_mentions.append(ch.mention)
        secondary_mentions = []
        for cid in secondary_ids:
            ch = guild.get_channel(int(cid))
            if ch:
                secondary_mentions.append(ch.mention)
        vc_cat = guild.get_channel(int(vc_cat_id)) if vc_cat_id else None

        embed.description = (
            f"**Primary Poll Channels:** {', '.join(primary_mentions) if primary_mentions else '*Not set*'}\n"
            f"**Secondary Poll Channels:** {', '.join(secondary_mentions) if secondary_mentions else '*Not set*'}\n"
            f"**VC Category:** {vc_cat.name if vc_cat else '*Not set*'}"
        )
        embed.set_footer(text="Primary channels receive the Game Night role ping. Secondary channels do not.")

        view = ChannelsView(self)
        await self.edit(interaction, embed=embed, view=view)

    async def show_returning_players(self, interaction):
        embed = discord.Embed(title="Returning Players", color=EMBED_COLOR)

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM returning_players") as cur:
                rows = await cur.fetchall()

        player_ids = [r[0] for r in rows]

        if player_ids:
            guild = interaction.guild
            lines = []
            for uid in player_ids:
                member = guild.get_member(uid)
                lines.append(f"**{member.display_name}** ({member.mention})" if member else f"Unknown User (`{uid}`)")
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"{len(player_ids)} player(s)")
        else:
            embed.description = "*No returning players tracked.*"

        view = ReturningPlayersView(self, player_ids, interaction.guild)
        await self.edit(interaction, embed=embed, view=view)

    async def show_role_settings(self, interaction):
        embed = discord.Embed(title="Game Night Role", color=EMBED_COLOR)

        role_id = await DB.get_setting('game_night_role_id')
        ping_enabled = await DB.get_setting('ping_role_enabled', '0')

        guild = interaction.guild
        role = guild.get_role(int(role_id)) if role_id else None

        embed.description = (
            f"**Role:** {role.mention if role else '*Not set*'}\n"
            f"**Ping on Post:** {'ON' if ping_enabled == '1' else 'OFF'}"
        )

        view = RoleSettingsView(self, has_role=bool(role), ping_on=(ping_enabled == '1'))
        await self.edit(interaction, embed=embed, view=view)

    async def show_banners(self, interaction):
        embed = discord.Embed(title="Banner Management", color=EMBED_COLOR)

        gn_banner = await DB.get_setting('game_night_banner_url')
        embed.description = (
            f"**Game Night Banner:** {'Set' if gn_banner else '*Not set*'}\n\n"
            f"Choose a banner type to manage below."
        )

        view = BannersMenuView(self)
        await self.edit(interaction, embed=embed, view=view)

    async def show_game_night_banner(self, interaction):
        embed = discord.Embed(title="Game Night Banner", color=EMBED_COLOR)

        banner_url = await DB.get_setting('game_night_banner_url')
        if banner_url:
            embed.description = "Current game night banner:"
            embed.set_image(url=banner_url)
        else:
            embed.description = "*No game night banner set.*"

        view = GameNightBannerView(self, has_banner=bool(banner_url))
        await self.edit(interaction, embed=embed, view=view)

    async def show_game_banners(self, interaction, selected_game_id=None):
        embed = discord.Embed(title="Game Banners", color=EMBED_COLOR)

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT id, name, banner_url FROM games") as cur:
                all_games = await cur.fetchall()

        if not all_games:
            embed.description = "*No games added yet.*"
            view = GameBannersView(self, [], None, None)
            return await self.edit(interaction, embed=embed, view=view)

        selected = None
        if selected_game_id:
            for g in all_games:
                if g[0] == selected_game_id:
                    selected = g
                    break

        if selected:
            embed.description = f"**{selected[1]}**"
            if selected[2]:
                embed.description += "\nCurrent banner:"
                embed.set_image(url=selected[2])
            else:
                embed.description += "\n*No banner set.*"
        else:
            embed.description = "Select a game below to manage its banner."

        view = GameBannersView(self, all_games, selected_game_id, selected[2] if selected else None)
        await self.edit(interaction, embed=embed, view=view)

    async def post_poll(self, interaction) -> tuple[bool, str]:
        if await DB.get_setting('active_poll_id'):
            return False, "A poll is already active. End it first."

        raw_primary = await DB.get_setting('poll_channel_ids', '[]')
        primary_ids = json.loads(raw_primary)
        raw_secondary = await DB.get_setting('secondary_poll_channel_ids', '[]')
        secondary_ids = json.loads(raw_secondary)

        if not primary_ids and not secondary_ids:
            return False, "No poll channels set! Configure them in Channels."

        primary_channels = []
        for cid in primary_ids:
            ch = interaction.guild.get_channel(int(cid))
            if ch:
                primary_channels.append(ch)
        secondary_channels = []
        for cid in secondary_ids:
            ch = interaction.guild.get_channel(int(cid))
            if ch:
                secondary_channels.append(ch)

        if not primary_channels and not secondary_channels:
            return False, "None of the configured channels were found. Re-configure in Channels."

        async with aiosqlite.connect(DB_PATH) as db:
            placeholders = ",".join("?" for _ in self.draft_game_ids)
            async with db.execute(f"SELECT name FROM games WHERE id IN ({placeholders})", self.draft_game_ids) as cur:
                game_names = [row[0] for row in await cur.fetchall()]

        games_list = "\n".join(f"**{name}**" for name in game_names)

        embed = discord.Embed(title="Next Game Night Poll", color=EMBED_COLOR)
        embed.description = (
            f"It's time to choose our next game!\n\n"
            f"{games_list}\n\n"
            f"**Weighted Voting:**\n"
            f"Your 1st, 2nd, and 3rd choices carry different point values. "
            f"Returning players from last week get a **2x multiplier** on their 1st place vote!"
        )
        ts = int(self.draft_end_time.timestamp())
        embed.add_field(name="Poll closes:", value=f"<t:{ts}:F>\n(<t:{ts}:R>)", inline=False)
        embed.set_footer(text="Votes cast: 0 | Add the alert role below")

        gn_banner = await DB.get_setting('game_night_banner_url')
        if gn_banner:
            embed.set_image(url=gn_banner)

        role_id = await DB.get_setting('game_night_role_id')
        ping_enabled = await DB.get_setting('ping_role_enabled', '0')
        ping_content = f"<@&{role_id}>" if role_id and ping_enabled == '1' else None

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "INSERT INTO active_poll (channel_id, message_id, end_time) VALUES (?, ?, ?)",
                (0, 0, self.draft_end_time.timestamp())
            )
            poll_id = cursor.lastrowid
            for gid in self.draft_game_ids:
                await db.execute("INSERT INTO poll_games (poll_id, game_id) VALUES (?, ?)", (poll_id, gid))
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_poll_id', ?)", (str(poll_id),))
            if self.draft_game_night_time:
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_game_night_time', ?)",
                                 (str(self.draft_game_night_time.timestamp()),))

            sent_any = False
            for ch in primary_channels:
                try:
                    msg = await ch.send(content=ping_content, embed=embed, view=PublicVoteView())
                    await db.execute("INSERT INTO poll_messages (poll_id, channel_id, message_id) VALUES (?, ?, ?)",
                                     (poll_id, ch.id, msg.id))
                    sent_any = True
                except Exception as e:
                    logger.warning(f"Failed to post poll to primary channel {ch.id}: {e}")

            for ch in secondary_channels:
                try:
                    msg = await ch.send(embed=embed, view=PublicVoteView())
                    await db.execute("INSERT INTO poll_messages (poll_id, channel_id, message_id) VALUES (?, ?, ?)",
                                     (poll_id, ch.id, msg.id))
                    sent_any = True
                except Exception as e:
                    logger.warning(f"Failed to post poll to secondary channel {ch.id}: {e}")

            if not sent_any:
                await db.execute("DELETE FROM active_poll WHERE id = ?", (poll_id,))
                await db.execute("DELETE FROM poll_games WHERE poll_id = ?", (poll_id,))
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_poll_id', '')")
                await db.commit()
                return False, "Failed to send poll to any channel."

            await db.commit()

        await self.clear_draft()
        return True, "Poll posted!"


# --- PANEL VIEWS ---

class HomeView(discord.ui.View):
    def __init__(self, panel, has_active_poll=False, has_active_vc=False, has_draft=False):
        super().__init__(timeout=900)
        self.panel = panel

        # Row 0: Poll actions
        draft_label = "Resume Draft" if has_draft else "Draft Poll"
        draft_btn = discord.ui.Button(label=draft_label, style=discord.ButtonStyle.primary, row=0)
        draft_btn.callback = self.draft_cb
        self.add_item(draft_btn)

        end_poll_btn = discord.ui.Button(label="End Poll Early", style=discord.ButtonStyle.danger, row=0, disabled=not has_active_poll)
        end_poll_btn.callback = self.end_poll_cb
        self.add_item(end_poll_btn)

        # Row 1: Configuration
        games_btn = discord.ui.Button(label="Manage Games", style=discord.ButtonStyle.secondary, row=1)
        games_btn.callback = self.games_cb
        self.add_item(games_btn)

        weights_btn = discord.ui.Button(label="Voting Weights", style=discord.ButtonStyle.secondary, row=1)
        weights_btn.callback = self.weights_cb
        self.add_item(weights_btn)

        channels_btn = discord.ui.Button(label="Set Channels", style=discord.ButtonStyle.secondary, row=1)
        channels_btn.callback = self.channels_cb
        self.add_item(channels_btn)

        returning_btn = discord.ui.Button(label="Returning Players", style=discord.ButtonStyle.secondary, row=2)
        returning_btn.callback = self.returning_cb
        self.add_item(returning_btn)

        role_btn = discord.ui.Button(label="Game Night Role", style=discord.ButtonStyle.secondary, row=2)
        role_btn.callback = self.role_cb
        self.add_item(role_btn)

        banners_btn = discord.ui.Button(label="Banners", style=discord.ButtonStyle.secondary, row=2)
        banners_btn.callback = self.banners_cb
        self.add_item(banners_btn)

        # Row 3: VC management
        create_vc_btn = discord.ui.Button(label="Create Game VC", style=discord.ButtonStyle.success, row=3, disabled=has_active_vc)
        create_vc_btn.callback = self.create_vc_cb
        self.add_item(create_vc_btn)

        end_vc_btn = discord.ui.Button(label="End Game VC", style=discord.ButtonStyle.danger, row=3, disabled=not has_active_vc)
        end_vc_btn.callback = self.end_vc_cb
        self.add_item(end_vc_btn)

    async def draft_cb(self, interaction: discord.Interaction):
        await self.panel.show_draft(interaction)

    async def end_poll_cb(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.panel.cog.end_poll()
        await self.panel.show_home(interaction)

    async def games_cb(self, interaction: discord.Interaction):
        await self.panel.show_games(interaction)

    async def weights_cb(self, interaction: discord.Interaction):
        w1 = await DB.get_setting('weight_1', '3')
        w2 = await DB.get_setting('weight_2', '2')
        w3 = await DB.get_setting('weight_3', '1')
        await interaction.response.send_modal(WeightsModal(self.panel, w1, w2, w3))

    async def channels_cb(self, interaction: discord.Interaction):
        await self.panel.show_channels(interaction)

    async def returning_cb(self, interaction: discord.Interaction):
        await self.panel.show_returning_players(interaction)

    async def role_cb(self, interaction: discord.Interaction):
        await self.panel.show_role_settings(interaction)

    async def banners_cb(self, interaction: discord.Interaction):
        await self.panel.show_banners(interaction)

    async def create_vc_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(VCModal(self.panel.cog, self.panel))

    async def end_vc_cb(self, interaction: discord.Interaction):
        active_vc_id = await DB.get_setting('active_vc_id')
        if active_vc_id:
            vc = interaction.guild.get_channel(int(active_vc_id))
            if vc:
                try:
                    await vc.delete(reason="Game Night ended by admin")
                except discord.NotFound:
                    pass
        await interaction.response.defer()
        await self.panel.cog.finalize_vc_session()
        await self.panel.show_home(interaction)


class DraftView(discord.ui.View):
    def __init__(self, panel, all_games, ready):
        super().__init__(timeout=900)
        self.panel = panel

        # Row 0: Game select
        if all_games:
            options = []
            for g in all_games[:25]:
                options.append(discord.SelectOption(
                    label=g[1], value=str(g[0]),
                    default=(g[0] in panel.draft_game_ids)
                ))
            select = discord.ui.Select(
                placeholder="Select 2 to 8 games...",
                min_values=2,
                max_values=min(8, len(options)),
                options=options,
                row=0
            )
            select.callback = self.select_cb
            self.add_item(select)

        # Row 1: Draft actions
        time_btn = discord.ui.Button(label="Set Close Time", style=discord.ButtonStyle.secondary, row=1)
        time_btn.callback = self.time_cb
        self.add_item(time_btn)

        gn_time_btn = discord.ui.Button(label="Set Game Night Time", style=discord.ButtonStyle.secondary, row=1)
        gn_time_btn.callback = self.gn_time_cb
        self.add_item(gn_time_btn)

        post_btn = discord.ui.Button(label="Post Poll", style=discord.ButtonStyle.success, row=1, disabled=not ready)
        post_btn.callback = self.post_cb
        self.add_item(post_btn)

        # Row 2: Save / Discard / Back
        save_btn = discord.ui.Button(label="Save Draft", style=discord.ButtonStyle.primary, row=2)
        save_btn.callback = self.save_cb
        self.add_item(save_btn)

        discard_btn = discord.ui.Button(label="Discard Draft", style=discord.ButtonStyle.danger, row=2)
        discard_btn.callback = self.discard_cb
        self.add_item(discard_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def select_cb(self, interaction: discord.Interaction):
        self.panel.draft_game_ids = [int(v) for v in interaction.data["values"]]
        await self.panel.show_draft(interaction)

    async def time_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CloseTimeModal(self.panel))

    async def gn_time_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(GameNightTimeModal(self.panel))

    async def post_cb(self, interaction: discord.Interaction):
        await interaction.response.defer()
        success, msg = await self.panel.post_poll(interaction)
        await self.panel.show_home(interaction)

    async def save_cb(self, interaction: discord.Interaction):
        await self.panel.save_draft()
        await self.panel.show_home(interaction)

    async def discard_cb(self, interaction: discord.Interaction):
        await self.panel.clear_draft()
        await self.panel.show_home(interaction)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_home(interaction)


class GamesView(discord.ui.View):
    def __init__(self, panel, display_games, page, has_next):
        super().__init__(timeout=900)
        self.panel = panel
        self.page = page

        # Row 0: Add game button
        add_btn = discord.ui.Button(label="Add Game", style=discord.ButtonStyle.success, row=0)
        add_btn.callback = self.add_cb
        self.add_item(add_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=0)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

        # Row 1: Edit select (if games exist)
        if display_games:
            options = [discord.SelectOption(label=g[1], value=str(g[0])) for g in display_games]
            select = discord.ui.Select(placeholder="Select a game to edit...", options=options, row=1)
            select.callback = self.edit_cb
            self.select = select
            self.add_item(select)

        # Row 2: Pagination
        if page > 0:
            prev_btn = discord.ui.Button(label="< Prev", row=2)
            prev_btn.callback = self.prev_cb
            self.add_item(prev_btn)

        if has_next:
            next_btn = discord.ui.Button(label="Next >", row=2)
            next_btn.callback = self.next_cb
            self.add_item(next_btn)

    async def add_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(GameModal(self.panel))

    async def edit_cb(self, interaction: discord.Interaction):
        game_id = int(self.select.values[0])
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT name FROM games WHERE id = ?", (game_id,)) as cur:
                row = await cur.fetchone()
        if row:
            await interaction.response.send_modal(GameModal(self.panel, game_id, row[0]))
        else:
            await self.panel.show_games(interaction, self.page)

    async def prev_cb(self, interaction: discord.Interaction):
        await self.panel.show_games(interaction, self.page - 1)

    async def next_cb(self, interaction: discord.Interaction):
        await self.panel.show_games(interaction, self.page + 1)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_home(interaction)


class ChannelsView(discord.ui.View):
    def __init__(self, panel):
        super().__init__(timeout=900)
        self.panel = panel

        primary_select = discord.ui.ChannelSelect(
            channel_types=[discord.ChannelType.text, discord.ChannelType.news, discord.ChannelType.public_thread, discord.ChannelType.private_thread, discord.ChannelType.news_thread],
            placeholder="Select Primary Poll Channels...",
            min_values=1,
            max_values=10,
            row=0
        )
        primary_select.callback = self.primary_ch_cb
        self.add_item(primary_select)

        secondary_select = discord.ui.ChannelSelect(
            channel_types=[discord.ChannelType.text, discord.ChannelType.news, discord.ChannelType.public_thread, discord.ChannelType.private_thread, discord.ChannelType.news_thread],
            placeholder="Select Secondary Poll Channels...",
            min_values=1,
            max_values=10,
            row=1
        )
        secondary_select.callback = self.secondary_ch_cb
        self.add_item(secondary_select)

        cat_select = discord.ui.ChannelSelect(
            channel_types=[discord.ChannelType.category],
            placeholder="Select VC Category",
            row=2
        )
        cat_select.callback = self.vc_cat_cb
        self.add_item(cat_select)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=3)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def primary_ch_cb(self, interaction: discord.Interaction):
        ids = [ch.id for ch in self.children[0].values]
        await DB.set_setting('poll_channel_ids', json.dumps(ids))
        await self.panel.show_channels(interaction)

    async def secondary_ch_cb(self, interaction: discord.Interaction):
        ids = [ch.id for ch in self.children[1].values]
        await DB.set_setting('secondary_poll_channel_ids', json.dumps(ids))
        await self.panel.show_channels(interaction)

    async def vc_cat_cb(self, interaction: discord.Interaction):
        await DB.set_setting('vc_category_id', self.children[2].values[0].id)
        await self.panel.show_channels(interaction)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_home(interaction)


class ReturningPlayersView(discord.ui.View):
    def __init__(self, panel, player_ids, guild):
        super().__init__(timeout=900)
        self.panel = panel

        # Row 0: User select to add a returning player
        add_select = discord.ui.UserSelect(placeholder="Add a returning player...", row=0)
        add_select.callback = self.add_cb
        self.add_item(add_select)

        # Row 1: Dropdown to remove (only if players exist)
        if player_ids:
            options = []
            for uid in player_ids[:25]:
                member = guild.get_member(uid)
                label = member.display_name if member else f"Unknown ({uid})"
                options.append(discord.SelectOption(label=label, value=str(uid)))
            remove_select = discord.ui.Select(placeholder="Remove a returning player...", options=options, row=1)
            remove_select.callback = self.remove_cb
            self.add_item(remove_select)

        # Row 2: Back
        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def add_cb(self, interaction: discord.Interaction):
        user = interaction.data["resolved"]["users"]
        user_id = int(list(user.keys())[0])
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO returning_players (user_id) VALUES (?)", (user_id,))
            await db.commit()
        await self.panel.show_returning_players(interaction)

    async def remove_cb(self, interaction: discord.Interaction):
        user_id = int(interaction.data["values"][0])
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM returning_players WHERE user_id = ?", (user_id,))
            await db.commit()
        await self.panel.show_returning_players(interaction)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_home(interaction)


class RoleSettingsView(discord.ui.View):
    def __init__(self, panel, has_role=False, ping_on=False):
        super().__init__(timeout=900)
        self.panel = panel

        role_select = discord.ui.RoleSelect(placeholder="Select Game Night Role...", row=0)
        role_select.callback = self.role_cb
        self.add_item(role_select)

        ping_label = "Ping: ON" if ping_on else "Ping: OFF"
        ping_style = discord.ButtonStyle.success if ping_on else discord.ButtonStyle.secondary
        ping_btn = discord.ui.Button(label=ping_label, style=ping_style, row=1)
        ping_btn.callback = self.ping_cb
        self.add_item(ping_btn)

        clear_btn = discord.ui.Button(label="Clear Role", style=discord.ButtonStyle.danger, row=1, disabled=not has_role)
        clear_btn.callback = self.clear_cb
        self.add_item(clear_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def role_cb(self, interaction: discord.Interaction):
        role = self.children[0].values[0]
        await DB.set_setting('game_night_role_id', str(role.id))
        await self.panel.show_role_settings(interaction)

    async def ping_cb(self, interaction: discord.Interaction):
        current = await DB.get_setting('ping_role_enabled', '0')
        await DB.set_setting('ping_role_enabled', '0' if current == '1' else '1')
        await self.panel.show_role_settings(interaction)

    async def clear_cb(self, interaction: discord.Interaction):
        await DB.set_setting('game_night_role_id', '')
        await DB.set_setting('ping_role_enabled', '0')
        await self.panel.show_role_settings(interaction)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_home(interaction)


class BannersMenuView(discord.ui.View):
    def __init__(self, panel):
        super().__init__(timeout=900)
        self.panel = panel

        gn_btn = discord.ui.Button(label="Game Night Banner", style=discord.ButtonStyle.primary, row=0)
        gn_btn.callback = self.gn_cb
        self.add_item(gn_btn)

        game_btn = discord.ui.Button(label="Game Banners", style=discord.ButtonStyle.primary, row=0)
        game_btn.callback = self.game_cb
        self.add_item(game_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def gn_cb(self, interaction: discord.Interaction):
        await self.panel.show_game_night_banner(interaction)

    async def game_cb(self, interaction: discord.Interaction):
        await self.panel.show_game_banners(interaction)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_home(interaction)


class BannerURLModal(discord.ui.Modal, title="Set Banner"):
    url_input = discord.ui.TextInput(
        label="Banner Image URL",
        placeholder="https://...",
        required=True
    )

    def __init__(self, callback_fn):
        super().__init__()
        self.callback_fn = callback_fn

    async def on_submit(self, interaction: discord.Interaction):
        await self.callback_fn(interaction, self.url_input.value.strip())


class GameNightBannerView(discord.ui.View):
    def __init__(self, panel, has_banner=False):
        super().__init__(timeout=900)
        self.panel = panel

        set_btn = discord.ui.Button(label="Set Banner", style=discord.ButtonStyle.success, row=0)
        set_btn.callback = self.set_cb
        self.add_item(set_btn)

        remove_btn = discord.ui.Button(label="Remove Banner", style=discord.ButtonStyle.danger, row=0, disabled=not has_banner)
        remove_btn.callback = self.remove_cb
        self.add_item(remove_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def set_cb(self, interaction: discord.Interaction):
        async def save_banner(i, url):
            await DB.set_setting('game_night_banner_url', url)
            await i.response.defer()
            await self.panel.show_game_night_banner(i)
        await interaction.response.send_modal(BannerURLModal(save_banner))

    async def remove_cb(self, interaction: discord.Interaction):
        await DB.set_setting('game_night_banner_url', '')
        await self.panel.show_game_night_banner(interaction)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_banners(interaction)


class GameBannersView(discord.ui.View):
    def __init__(self, panel, all_games, selected_id, selected_banner):
        super().__init__(timeout=900)
        self.panel = panel
        self.selected_id = selected_id

        if all_games:
            options = []
            for g in all_games[:25]:
                options.append(discord.SelectOption(
                    label=g[1], value=str(g[0]),
                    default=(g[0] == selected_id)
                ))
            select = discord.ui.Select(placeholder="Select a game...", options=options, row=0)
            select.callback = self.select_cb
            self.add_item(select)

        if selected_id is not None:
            set_btn = discord.ui.Button(label="Set Banner", style=discord.ButtonStyle.success, row=1)
            set_btn.callback = self.set_cb
            self.add_item(set_btn)

            remove_btn = discord.ui.Button(label="Remove Banner", style=discord.ButtonStyle.danger, row=1, disabled=not selected_banner)
            remove_btn.callback = self.remove_cb
            self.add_item(remove_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self.back_cb
        self.add_item(back_btn)

    async def select_cb(self, interaction: discord.Interaction):
        game_id = int(interaction.data["values"][0])
        await self.panel.show_game_banners(interaction, selected_game_id=game_id)

    async def set_cb(self, interaction: discord.Interaction):
        game_id = self.selected_id
        async def save_banner(i, url):
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE games SET banner_url = ? WHERE id = ?", (url, game_id))
                await db.commit()
            await i.response.defer()
            await self.panel.show_game_banners(i, selected_game_id=game_id)
        await interaction.response.send_modal(BannerURLModal(save_banner))

    async def remove_cb(self, interaction: discord.Interaction):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE games SET banner_url = '' WHERE id = ?", (self.selected_id,))
            await db.commit()
        await self.panel.show_game_banners(interaction, selected_game_id=self.selected_id)

    async def back_cb(self, interaction: discord.Interaction):
        await self.panel.show_banners(interaction)


class ResultsDetailView(discord.ui.View):
    """Lightweight view that only places the Details button. Logic is handled by on_interaction in the cog."""
    def __init__(self, poll_id: int):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Details",
            style=discord.ButtonStyle.secondary,
            custom_id=f"results_detail:{poll_id}"
        ))
        self.add_item(discord.ui.Button(
            label="Game Night Role",
            style=discord.ButtonStyle.secondary,
            custom_id="game_night_role_btn"
        ))


# --- PUBLIC VOTING ---

class PublicVoteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Vote Now", style=discord.ButtonStyle.success, custom_id="public_vote_btn")
    async def vote_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Defer immediately to avoid Discord's 3-second interaction timeout
        await interaction.response.defer(ephemeral=True)

        poll_id = await DB.get_setting('active_poll_id')
        if not poll_id:
            return await interaction.followup.send("This poll has already ended.", ephemeral=True)

        poll_id = int(poll_id)

        cog = interaction.client.get_cog('GamePoll')
        if cog and interaction.user.id in cog._active_voters:
            return await interaction.followup.send("You already have a voting session open!", ephemeral=True)

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT 1 FROM votes WHERE poll_id = ? AND user_id = ?", (poll_id, interaction.user.id)) as cursor:
                if await cursor.fetchone():
                    return await interaction.followup.send("You have already voted! Votes cannot be changed.", ephemeral=True)

            async with db.execute("""
                SELECT g.id, g.name
                FROM games g
                JOIN poll_games pg ON g.id = pg.game_id
                WHERE pg.poll_id = ?
            """, (poll_id,)) as cursor:
                games = await cursor.fetchall()

            async with db.execute("SELECT 1 FROM returning_players WHERE user_id = ?", (interaction.user.id,)) as cursor:
                has_multiplier = bool(await cursor.fetchone())

        if cog:
            cog._active_voters.add(interaction.user.id)

        view = VotingProcessView(poll_id, games, has_multiplier, interaction.user.id)
        await view.start(interaction)

    @discord.ui.button(label="Game Night Role", style=discord.ButtonStyle.secondary, custom_id="game_night_role_btn")
    async def role_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        role_id = await DB.get_setting('game_night_role_id')
        if not role_id:
            return await interaction.response.send_message("No Game Night role has been configured.", ephemeral=True)

        role = interaction.guild.get_role(int(role_id))
        if not role:
            return await interaction.response.send_message("The configured Game Night role no longer exists.", ephemeral=True)

        member = interaction.guild.get_member(interaction.user.id)
        if not member:
            return await interaction.response.send_message("Could not find your member info.", ephemeral=True)

        if role in member.roles:
            try:
                await member.remove_roles(role, reason="Game Night role self-remove")
            except discord.Forbidden:
                return await interaction.response.send_message("I don't have permission to manage that role.", ephemeral=True)
            await interaction.response.send_message(f"Removed the **{role.name}** role.", ephemeral=True)
        else:
            try:
                await member.add_roles(role, reason="Game Night role self-assign")
            except discord.Forbidden:
                return await interaction.response.send_message("I don't have permission to manage that role.", ephemeral=True)
            await interaction.response.send_message(
                f"Added the **{role.name}** role! This role is used for game night event alerts and poll notifications.",
                ephemeral=True
            )


class VotingProcessView(discord.ui.View):
    def __init__(self, poll_id, games, has_multiplier, user_id=None):
        super().__init__(timeout=900)
        self.poll_id = poll_id
        self.games = games
        self.has_multiplier = has_multiplier
        self.user_id = user_id
        self.bot = None

        self.choices = []
        self.max_choices = min(3, len(games))
        self.is_submitting = False
        self.message_sent = False

    def _cleanup_voter(self, bot):
        if self.user_id and bot:
            cog = bot.get_cog('GamePoll')
            if cog:
                cog._active_voters.discard(self.user_id)

    async def on_timeout(self):
        self._cleanup_voter(self.bot)

    async def start(self, interaction: discord.Interaction):
        self.bot = interaction.client
        await self.render_step(interaction)

    async def render_step(self, interaction: discord.Interaction):
        current_rank = len(self.choices) + 1

        if current_rank > self.max_choices:
            return await self.render_review(interaction)

        self.clear_items()

        embed = discord.Embed(title="Secret Ballot", color=EMBED_COLOR)
        rank_str = "1st" if current_rank == 1 else "2nd" if current_rank == 2 else "3rd"
        embed.description = f"Select your **{rank_str}** choice."
        if current_rank == 1 and self.has_multiplier:
            embed.description += "\n*(Your returning player 2x multiplier applies to this choice!)*"

        available_games = [g for g in self.games if g[0] not in [c[1] for c in self.choices]]
        for i, game in enumerate(available_games):
            btn = discord.ui.Button(label=game[1][:MAX_GAME_NAME_LEN], style=discord.ButtonStyle.secondary, row=i//4)

            async def make_callback(g=game):
                async def cb(i: discord.Interaction):
                    # Prevent rapid double-clicking adding two ranks at once
                    if current_rank != len(self.choices) + 1: return
                    self.choices.append((current_rank, g[0], g[1]))
                    await self.render_step(i)
                return cb

            btn.callback = await make_callback()
            self.add_item(btn)

        if not self.message_sent:
            # First render — interaction was already deferred, use followup
            await interaction.followup.send(embed=embed, view=self, ephemeral=True)
            self.message_sent = True
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def render_review(self, interaction: discord.Interaction):
        self.clear_items()
        embed = discord.Embed(title="Review Your Votes", color=EMBED_COLOR)

        desc = ""
        for rank, gid, name in self.choices:
            rank_str = "1st" if rank == 1 else "2nd" if rank == 2 else "3rd"
            mult_txt = " *(2x Multiplier applied!)*" if rank == 1 and self.has_multiplier else ""
            desc += f"**{rank_str}:** {name}{mult_txt}\n"

        embed.description = desc + "\nClick **Submit** to lock in your votes. This cannot be changed."

        btn_submit = discord.ui.Button(label="Confirm & Submit", style=discord.ButtonStyle.success)
        async def submit_cb(i: discord.Interaction):
            if self.is_submitting: return
            self.is_submitting = True

            btn_submit.disabled = True
            btn_cancel.disabled = True
            await i.response.edit_message(view=self) # Lock UI immediately

            async with aiosqlite.connect(DB_PATH) as db:
                try:
                    for rank, gid, name in self.choices:
                        used_mult = 1 if (rank == 1 and self.has_multiplier) else 0
                        await db.execute("INSERT INTO votes (poll_id, user_id, game_id, rank, multiplier_used) VALUES (?, ?, ?, ?, ?)",
                                       (self.poll_id, i.user.id, gid, rank, used_mult))
                    await db.commit()
                except aiosqlite.IntegrityError:
                    self._cleanup_voter(i.client)
                    return await i.edit_original_response(content="You have already submitted votes for this poll.", embed=None, view=None)

            self._cleanup_voter(i.client)
            await i.edit_original_response(content="Votes successfully submitted!", embed=None, view=None)

            # Update public embed footer across all channels
            try:
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute("SELECT COUNT(DISTINCT user_id) FROM votes WHERE poll_id = ?", (self.poll_id,)) as cur:
                        voters = (await cur.fetchone())[0]

                    async with db.execute("SELECT channel_id, message_id FROM poll_messages WHERE poll_id = ?", (self.poll_id,)) as cur:
                        rows = await cur.fetchall()

                for ch_id, msg_id in rows:
                    try:
                        channel = i.client.get_channel(ch_id)
                        if channel:
                            msg = await channel.fetch_message(msg_id)
                            new_embed = msg.embeds[0]
                            new_embed.set_footer(text=f"Votes cast: {voters} | Add the alert role below")
                            await msg.edit(embed=new_embed)
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Failed to update vote count footer: {e}")

        btn_submit.callback = submit_cb
        self.add_item(btn_submit)

        btn_cancel = discord.ui.Button(label="Cancel & Start Over", style=discord.ButtonStyle.danger)
        async def cancel_cb(i: discord.Interaction):
            if self.is_submitting: return
            self.choices = []
            await self.render_step(i)
        btn_cancel.callback = cancel_cb
        self.add_item(btn_cancel)

        await interaction.response.edit_message(embed=embed, view=self)


# --- THE COG ---

class GamePoll(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.vc_join_times = {}
        self.vc_empty_minutes = 0
        self.poll_lock = asyncio.Lock()
        self._active_voters = set()
        self.playwright = None
        self.browser = None
        self._page_semaphore = asyncio.Semaphore(2)
        self._results_template = None

    async def cog_load(self):
        await DB.setup()
        self.poll_monitor.start()
        self.vc_monitor.start()
        self.bot.add_view(PublicVoteView())
        # Retroactively update active poll messages to include the Game Night Role button
        self.bot.loop.create_task(self._update_active_poll_views())
        self.bot.loop.create_task(self._update_results_views())
        self.bot.loop.create_task(self._reseed_vc_join_times())
        # Initialize playwright for results card rendering
        if PLAYWRIGHT_AVAILABLE:
            try:
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(
                    args=['--font-render-hinting=none', '--disable-lcd-text', '--enable-font-antialiasing']
                )
                self._results_template = RESULTS_TEMPLATE_PATH.read_text()
                logger.info("GamePoll: Playwright browser initialized.")
            except Exception as e:
                logger.warning(f"GamePoll: Failed to initialize Playwright: {e}")

    async def cog_unload(self):
        self.poll_monitor.cancel()
        self.vc_monitor.cancel()
        self._active_voters.clear()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def generate_results_image(self, detail_data: dict) -> io.BytesIO | None:
        """Render the vote breakdown as a styled image using Playwright."""
        if not self.browser or not self._results_template:
            return None

        try:
            sorted_games = sorted(detail_data.values(), key=lambda x: x['points'], reverse=True)

            # Fetch banner URLs for game icons — map by both id and name for backwards compat
            banner_by_id = {}
            banner_by_name = {}
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT id, name, banner_url FROM games") as cur:
                    for row in await cur.fetchall():
                        if row[2]:
                            banner_by_id[row[0]] = row[2]
                            banner_by_name[row[1]] = row[2]

            rows_html = ""
            for i, g in enumerate(sorted_games):
                rank = i + 1
                rank_class = f"rank-{rank}" if rank <= 3 else ""
                parity = "odd" if rank % 2 == 1 else "even"

                # Game icon: look up by game_id first, fall back to name match
                game_id = g.get('game_id')
                banner_url = banner_by_id.get(game_id, '') if game_id else ''
                if not banner_url:
                    banner_url = banner_by_name.get(g['name'], '')
                if banner_url:
                    icon_html = f'<div class="game-icon-wrap"><img class="game-icon" src="{banner_url}" /></div>'
                else:
                    initial = g['name'][0].upper() if g['name'] else '?'
                    icon_html = f'<div class="game-icon-wrap"><div class="game-icon-placeholder">{initial}</div></div>'

                boost_class = "stat-boost" if g['votes_1st_mult'] > 0 else ""

                rows_html += f'''
                <div class="game-row {rank_class} {parity}">
                    <div class="rank">{rank}</div>
                    {icon_html}
                    <div class="game-name">{g['name']}</div>
                    <div class="stat stat-pts">{g['points']}</div>
                    <div class="stat">{g['votes_3rd']}</div>
                    <div class="stat">{g['votes_2nd']}</div>
                    <div class="stat">{g['votes_1st']}</div>
                    <div class="stat {boost_class}">{g['votes_1st_mult']}</div>
                </div>
                '''

            html = self._results_template.format(
                font_path=str(FONTS_PATH),
                rows_html=rows_html
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 716, 'height': 400},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 716, 'height': body_height})
                    screenshot = await page.screenshot(type='png')
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating results card: {e}")
            return None

    async def _update_active_poll_views(self):
        """Edit any active poll messages to include the updated PublicVoteView (e.g. new buttons)."""
        await self.bot.wait_until_ready()
        poll_id = await DB.get_setting('active_poll_id')
        if not poll_id:
            return
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT channel_id, message_id FROM poll_messages WHERE poll_id = ?", (poll_id,)) as cur:
                rows = await cur.fetchall()
        for ch_id, msg_id in rows:
            try:
                channel = self.bot.get_channel(ch_id)
                if not channel:
                    continue
                msg = await channel.fetch_message(msg_id)
                await msg.edit(view=PublicVoteView())
            except Exception as e:
                logger.debug(f"Could not update poll message {msg_id} in {ch_id}: {e}")

    async def _reseed_vc_join_times(self):
        """Re-seed vc_join_times for members already in the active VC after a bot restart."""
        await self.bot.wait_until_ready()
        vc_id = await DB.get_setting('active_vc_id')
        if not vc_id:
            return
        channel = self.bot.get_channel(int(vc_id))
        if not channel:
            return

        # Read persisted join times from the DB
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id, join_time FROM vc_sessions WHERE join_time IS NOT NULL") as cur:
                db_join_times = {r[0]: r[1] for r in await cur.fetchall()}

        now = time.time()
        reseeded = 0
        for member in channel.members:
            if not member.bot and member.id not in self.vc_join_times:
                # Use the persisted join_time if available, otherwise fall back to now
                self.vc_join_times[member.id] = db_join_times.get(member.id, now)
                reseeded += 1
        if reseeded:
            logger.info(f"GamePoll: Re-seeded {reseeded} VC member(s) after restart.")

    async def _update_results_views(self):
        """Edit any results messages to include the updated ResultsDetailView."""
        await self.bot.wait_until_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT poll_id, channel_id, message_id FROM results_messages") as cur:
                rows = await cur.fetchall()
        for poll_id, ch_id, msg_id in rows:
            try:
                channel = self.bot.get_channel(ch_id)
                if not channel:
                    continue
                msg = await channel.fetch_message(msg_id)
                await msg.edit(view=ResultsDetailView(poll_id))
            except Exception as e:
                logger.debug(f"Could not update results message {msg_id} in {ch_id}: {e}")

    # --- PERMISSIONS ---
    def is_admin(self, interaction: discord.Interaction) -> bool:
        """Uses the robust admin check defined in your main.py Vibey class."""
        if hasattr(self.bot, 'is_bot_admin'):
            return self.bot.is_bot_admin(interaction.user)
        return interaction.user.guild_permissions.administrator

    # --- COMMANDS ---
    @app_commands.command(name="gamepoll_panel", description="Open the Game Night Admin Panel")
    @app_commands.default_permissions(administrator=True)
    async def gamepoll_panel(self, interaction: discord.Interaction):
        if not self.is_admin(interaction):
            return await interaction.response.send_message("You do not have permission to use this.", ephemeral=True)

        panel = AdminPanel(self)
        await panel.load_draft()
        await panel.show_home(interaction, is_initial=True)

    # --- BACKGROUND TASKS ---
    @tasks.loop(minutes=1)
    async def vc_monitor(self):
        """Auto-cleanup empty Game Night VCs."""
        vc_id = await DB.get_setting('active_vc_id')
        if not vc_id: return

        channel = self.bot.get_channel(int(vc_id))
        if not channel:
            # Channel was manually deleted, wrap up session safely
            await self.finalize_vc_session()
            return

        members_in_vc = [m for m in channel.members if not m.bot]

        if len(members_in_vc) == 0:
            self.vc_empty_minutes += 1
            if self.vc_empty_minutes >= 5:
                try:
                    await channel.delete(reason="Auto-cleanup: Game Night VC empty for 5 minutes.")
                except discord.NotFound:
                    pass
                except discord.Forbidden:
                    logger.warning("Bot lacks permission to delete the Game Night VC.")
                await self.finalize_vc_session()
        else:
            self.vc_empty_minutes = 0
            # Flush elapsed time for active VC members and promote qualifiers live
            now = time.time()
            async with aiosqlite.connect(DB_PATH) as db:
                for user_id, join_time in list(self.vc_join_times.items()):
                    elapsed = now - join_time
                    await db.execute(
                        "INSERT INTO vc_sessions (user_id, total_seconds, join_time) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET total_seconds = total_seconds + ?, join_time = ?",
                        (user_id, elapsed, now, elapsed, now))
                    self.vc_join_times[user_id] = now  # Reset join time so we don't double-count
                await db.execute(f"""
                    INSERT OR IGNORE INTO returning_players (user_id)
                    SELECT user_id FROM vc_sessions WHERE total_seconds >= {MIN_VC_SECONDS}
                """)
                await db.commit()

    @tasks.loop(minutes=1)
    async def poll_monitor(self):
        """Check if active poll has expired."""
        poll_id = await DB.get_setting('active_poll_id')
        if not poll_id: return

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT end_time FROM active_poll WHERE id = ?", (poll_id,)) as cur:
                row = await cur.fetchone()
                if not row: return

                if datetime.now(EASTERN).timestamp() >= row[0]:
                    await self.end_poll()

    @poll_monitor.before_loop
    @vc_monitor.before_loop
    async def before_monitors(self):
        await self.bot.wait_until_ready()

    # --- LOGIC HELPERS ---
    async def end_poll(self) -> bool:
        """Tallies votes, posts results, and clears active poll safely. Returns True if fully successful."""
        async with self.poll_lock:
            poll_id = await DB.get_setting('active_poll_id')
            if not poll_id: return False

            w1 = int(await DB.get_setting('weight_1', 3))
            w2 = int(await DB.get_setting('weight_2', 2))
            w3 = int(await DB.get_setting('weight_3', 1))

            results = {}

            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT g.id, g.name, g.banner_url FROM games g JOIN poll_games pg ON g.id = pg.game_id WHERE pg.poll_id = ?", (poll_id,)) as cur:
                    for row in await cur.fetchall():
                        results[row[0]] = {'points': 0, 'first_places': 0, 'multiplier_points': 0, 'name': row[1], 'banner': row[2]}

                async with db.execute("SELECT game_id, rank, multiplier_used FROM votes WHERE poll_id = ?", (poll_id,)) as cur:
                    votes = await cur.fetchall()

                # Build detailed breakdown for Details button
                detail_data = {}
                for gid in results:
                    detail_data[str(gid)] = {
                        'name': results[gid]['name'],
                        'game_id': gid,
                        'points': 0,
                        'votes_3rd': 0, 'votes_2nd': 0, 'votes_1st': 0, 'votes_1st_mult': 0
                    }

                for gid, rank, used_mult in votes:
                    if gid not in results: continue

                    base_points = w1 if rank == 1 else w2 if rank == 2 else w3 if rank == 3 else 0
                    mult_points = base_points if used_mult else 0
                    total_pts = base_points + mult_points

                    results[gid]['points'] += total_pts
                    if rank == 1:
                        results[gid]['first_places'] += 1
                    results[gid]['multiplier_points'] += mult_points

                    key = str(gid)
                    if key in detail_data:
                        if rank == 1:
                            if used_mult:
                                detail_data[key]['votes_1st_mult'] += 1
                            else:
                                detail_data[key]['votes_1st'] += 1
                        elif rank == 2:
                            detail_data[key]['votes_2nd'] += 1
                        elif rank == 3:
                            detail_data[key]['votes_3rd'] += 1

                # Update point totals in detail_data
                for gid in results:
                    detail_data[str(gid)]['points'] = results[gid]['points']

                sorted_results = sorted(results.values(),
                                        key=lambda x: (x['points'], x['first_places'], x['multiplier_points']),
                                        reverse=True)

                embed = discord.Embed(title="Game Night Poll Results", color=WINNER_COLOR)
                desc = ""
                for i, res in enumerate(sorted_results):
                    medal = "**1.**" if i == 0 else "**2.**" if i == 1 else "**3.**" if i == 2 else f"**{i+1}.**"
                    game_format = f"**__\"{res['name']}\"__**" if i == 0 else f"**{res['name']}**"
                    desc += f"{medal} {game_format} — {res['points']} pts\n"

                embed.description = desc

                winner = sorted_results[0] if sorted_results else None
                if winner and winner['banner']:
                    embed.set_image(url=winner['banner'])

                gn_time = await DB.get_setting('active_game_night_time')
                if gn_time:
                    gn_ts = int(float(gn_time))
                    embed.add_field(name="Game Night", value=f"<t:{gn_ts}:F>\n(<t:{gn_ts}:R>)", inline=False)

                # Store vote breakdown for Details button
                await db.execute("INSERT OR REPLACE INTO poll_results (poll_id, data_json) VALUES (?, ?)",
                                 (int(poll_id), json.dumps(detail_data)))

                success = True
                async with db.execute("SELECT channel_id, message_id FROM poll_messages WHERE poll_id = ?", (poll_id,)) as cur:
                    poll_msgs = await cur.fetchall()

                for ch_id, msg_id in poll_msgs:
                    channel = self.bot.get_channel(ch_id)
                    if not channel:
                        logger.warning(f"Poll channel {ch_id} not found when posting results.")
                        continue
                    try:
                        results_msg = await channel.send(embed=embed, view=ResultsDetailView(int(poll_id)))
                        await db.execute("INSERT INTO results_messages (poll_id, channel_id, message_id) VALUES (?, ?, ?)",
                                         (int(poll_id), ch_id, results_msg.id))
                    except Exception as e:
                        logger.error(f"Error posting poll results to channel {ch_id}: {e}")
                        success = False
                    try:
                        old_msg = await channel.fetch_message(msg_id)
                        await old_msg.delete()
                    except discord.NotFound:
                        pass
                    except Exception as e:
                        logger.warning(f"Failed to delete poll message {msg_id}: {e}")

                # Always cleanup the DB even if posting failed
                await db.execute("DELETE FROM active_poll")
                await db.execute("DELETE FROM poll_messages WHERE poll_id = ?", (int(poll_id),))
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_poll_id', '')")
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_game_night_time', '')")
                await db.execute("DELETE FROM returning_players")
                await db.commit()

                return success

    async def finalize_vc_session(self):
        """Flushes memory VC times to DB safely."""
        now = time.time()

        async with aiosqlite.connect(DB_PATH) as db:
            for user_id, join_time in list(self.vc_join_times.items()):
                duration = now - join_time
                await db.execute(
                    "INSERT INTO vc_sessions (user_id, total_seconds, join_time) VALUES (?, ?, NULL) ON CONFLICT(user_id) DO UPDATE SET total_seconds = total_seconds + ?, join_time = NULL",
                    (user_id, duration, duration))
            self.vc_join_times.clear()

            await db.execute(f"""
                INSERT OR IGNORE INTO returning_players (user_id)
                SELECT user_id FROM vc_sessions WHERE total_seconds >= {MIN_VC_SECONDS}
            """)

            await db.execute("DELETE FROM vc_sessions")
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_vc_id', '')")
            self.vc_empty_minutes = 0
            await db.commit()

    # --- EVENTS ---
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle persistent buttons with dynamic custom_ids (survives bot restarts)."""
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get("custom_id", "")
        if custom_id == "game_night_role_btn":
            role_id = await DB.get_setting('game_night_role_id')
            if not role_id:
                return await interaction.response.send_message("No Game Night role has been configured.", ephemeral=True)

            role = interaction.guild.get_role(int(role_id))
            if not role:
                return await interaction.response.send_message("The configured Game Night role no longer exists.", ephemeral=True)

            member = interaction.guild.get_member(interaction.user.id)
            if not member:
                return await interaction.response.send_message("Could not find your member info.", ephemeral=True)

            if role in member.roles:
                try:
                    await member.remove_roles(role, reason="Game Night role self-remove")
                except discord.Forbidden:
                    return await interaction.response.send_message("I don't have permission to manage that role.", ephemeral=True)
                await interaction.response.send_message(f"Removed the **{role.name}** role.", ephemeral=True)
            else:
                try:
                    await member.add_roles(role, reason="Game Night role self-assign")
                except discord.Forbidden:
                    return await interaction.response.send_message("I don't have permission to manage that role.", ephemeral=True)
                await interaction.response.send_message(f"Added the **{role.name}** role!", ephemeral=True)
            return

        if custom_id.startswith("results_detail:"):
            poll_id = int(custom_id.split(":")[1])

            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT data_json FROM poll_results WHERE poll_id = ?", (poll_id,)) as cur:
                    row = await cur.fetchone()

            if not row:
                return await interaction.response.send_message("Details are no longer available.", ephemeral=True)

            data = json.loads(row[0])

            # Try to generate a styled image
            image = await self.generate_results_image(data)
            if image:
                file = discord.File(image, filename="vote_breakdown.png")
                await interaction.response.send_message(file=file, ephemeral=True)
            else:
                # Fallback to text embed if playwright unavailable
                sorted_games = sorted(data.values(), key=lambda x: x['points'], reverse=True)
                lines = []
                for g in sorted_games:
                    lines.append(f"**{g['name']}** — {g['points']} pts")
                    lines.append(f"> 3rd: {g['votes_3rd']}  |  2nd: {g['votes_2nd']}  |  1st: {g['votes_1st']}  |  1st+: {g['votes_1st_mult']}")
                embed = discord.Embed(title="Vote Breakdown", color=EMBED_COLOR)
                embed.description = "\n".join(lines)
                embed.set_footer(text="1st+ = Returning player's boosted 1st place vote")
                await interaction.response.send_message(embed=embed, ephemeral=True)

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if member.bot: return

        active_vc_id = await DB.get_setting('active_vc_id')
        if not active_vc_id: return
        active_vc_id = int(active_vc_id)

        joined_active = after.channel and after.channel.id == active_vc_id
        left_active = before.channel and before.channel.id == active_vc_id

        if joined_active and not left_active:
            now = time.time()
            self.vc_join_times[member.id] = now
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO vc_sessions (user_id, total_seconds, join_time) VALUES (?, 0, ?) ON CONFLICT(user_id) DO UPDATE SET join_time = ?",
                    (member.id, now, now))
                await db.commit()

        elif left_active and not joined_active:
            join_time = self.vc_join_times.pop(member.id, None)
            if join_time:
                duration = time.time() - join_time
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "INSERT INTO vc_sessions (user_id, total_seconds, join_time) VALUES (?, ?, NULL) ON CONFLICT(user_id) DO UPDATE SET total_seconds = total_seconds + ?, join_time = NULL",
                        (member.id, duration, duration))
                    await db.execute(f"""
                        INSERT OR IGNORE INTO returning_players (user_id)
                        SELECT user_id FROM vc_sessions WHERE user_id = ? AND total_seconds >= {MIN_VC_SECONDS}
                    """, (member.id,))
                    await db.commit()

async def setup(bot: commands.Bot):
    await bot.add_cog(GamePoll(bot))
