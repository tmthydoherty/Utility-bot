import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import os
import asyncio
import time
from datetime import datetime, timedelta
import re
from typing import Optional, Dict
import logging
from PIL import Image, ImageDraw, ImageFont
import io

logger = logging.getLogger('bot_main')

# --- CONFIG & CONSTANTS ---
DB_PATH = "data/pickems.db"
ASSETS_DIR = "data/pickems_assets"

# Clean, professional colors matching Discord's native UI
COLOR_PRIMARY = 0x2b2d31  
COLOR_ACCENT = 0x5865F2   

class AsyncPickemsDB:
    """Handles all SQLite database operations asynchronously to prevent event loop blocking."""
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(ASSETS_DIR, exist_ok=True)
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # Settings
            await db.execute('''CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )''')
            
            # Teams
            await db.execute('''CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                stream_url TEXT,
                logo_path TEXT
            )''')

            # Matches
            await db.execute('''CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                team1 TEXT,
                team2 TEXT,
                stream_url TEXT,
                team1_stream_url TEXT,
                team2_stream_url TEXT,
                body_text TEXT,
                logo_path TEXT,
                map_path TEXT,
                status TEXT DEFAULT 'draft',
                message_id TEXT,
                winner INTEGER,
                score_1 INTEGER,
                score_2 INTEGER
            )''')
            
            # Migration: Add close_time if it doesn't exist from the previous version
            try:
                await db.execute("ALTER TABLE matches ADD COLUMN close_time INTEGER")
            except aiosqlite.OperationalError:
                pass # Column already exists

            # Migration: Add team stream URLs if they don't exist
            try:
                await db.execute("ALTER TABLE matches ADD COLUMN team1_stream_url TEXT")
                await db.execute("ALTER TABLE matches ADD COLUMN team2_stream_url TEXT")
            except aiosqlite.OperationalError:
                pass # Columns already exist
            
            # Predictions
            await db.execute('''CREATE TABLE IF NOT EXISTS predictions (
                user_id TEXT,
                match_id INTEGER,
                predicted_winner INTEGER,
                predicted_score_1 INTEGER,
                predicted_score_2 INTEGER,
                points INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, match_id)
            )''')
            
            # Auto-cleanup orphaned drafts that never received team logos
            await db.execute("DELETE FROM matches WHERE status = 'draft' AND logo_path IS NULL")
            await db.commit()

    async def set_config(self, key: str, value: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
            await db.commit()

    async def get_config(self, key: str) -> Optional[str]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
                row = await cursor.fetchone()
                return row['value'] if row else None

    async def create_draft(self, title: str, team1: str, team2: str, stream_url: str, team1_url: str, team2_url: str, body_text: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "INSERT INTO matches (title, team1, team2, stream_url, team1_stream_url, team2_stream_url, body_text) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (title, team1, team2, stream_url, team1_url, team2_url, body_text)
            )
            await db.commit()
            return cursor.lastrowid

    async def update_draft(self, match_id: int, title: str, team1: str, team2: str, stream_url: str, team1_url: str, team2_url: str, body_text: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE matches SET title = ?, team1 = ?, team2 = ?, stream_url = ?, team1_stream_url = ?, team2_stream_url = ?, body_text = ? WHERE id = ?",
                (title, team1, team2, stream_url, team1_url, team2_url, body_text, match_id)
            )
            await db.commit()

    async def update_draft_images(self, match_id: int, logo_path: str, map_path: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE matches SET logo_path = ?, map_path = ? WHERE id = ?", (logo_path, map_path, match_id))
            await db.commit()
            
    async def delete_draft(self, match_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM matches WHERE id = ?", (match_id,))
            await db.commit()

    async def get_matches_by_status(self, status: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM matches WHERE status = ?", (status,)) as cursor:
                return await cursor.fetchall()

    async def get_resolvable_matches(self):
        """Returns matches that are published OR closed."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM matches WHERE status IN ('published', 'closed')") as cursor:
                return await cursor.fetchall()

    async def get_match_by_id(self, match_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM matches WHERE id = ?", (match_id,)) as cursor:
                return await cursor.fetchone()

    async def get_match_by_message_id(self, message_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM matches WHERE message_id = ?", (str(message_id),)) as cursor:
                return await cursor.fetchone()

    async def publish_match(self, match_id: int, message_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE matches SET status = 'published', message_id = ? WHERE id = ?", (message_id, match_id))
            await db.commit()

    async def set_match_status(self, match_id: int, status: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE matches SET status = ? WHERE id = ?", (status, match_id))
            await db.commit()

    async def set_match_close_time(self, match_id: int, close_time: Optional[int]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE matches SET close_time = ? WHERE id = ?", (close_time, match_id))
            await db.commit()

    async def get_user_prediction(self, user_id: int, match_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM predictions WHERE user_id = ? AND match_id = ?", (str(user_id), match_id)) as cursor:
                return await cursor.fetchone()

    async def save_prediction(self, user_id: int, match_id: int, winner: int, score_1: int, score_2: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR REPLACE INTO predictions (user_id, match_id, predicted_winner, predicted_score_1, predicted_score_2)
                VALUES (?, ?, ?, ?, ?)
            ''', (str(user_id), match_id, winner, score_1, score_2))
            await db.commit()

    async def resolve_match(self, match_id: int, winner: int, score_1: int, score_2: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute(
                "UPDATE matches SET status = 'resolved', winner = ?, score_1 = ?, score_2 = ? WHERE id = ?",
                (winner, score_1, score_2, match_id)
            )
            
            async with db.execute("SELECT * FROM predictions WHERE match_id = ?", (match_id,)) as cursor:
                predictions = await cursor.fetchall()
                
            for p in predictions:
                pts = 0
                if p['predicted_winner'] == winner:
                    pts += 1
                    if p['predicted_score_1'] == score_1 and p['predicted_score_2'] == score_2:
                        pts += 1
                await db.execute("UPDATE predictions SET points = ? WHERE user_id = ? AND match_id = ?", (pts, p['user_id'], match_id))
                
            await db.commit()

    async def get_leaderboard(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('''
                SELECT user_id, SUM(points) as total_points 
                FROM predictions 
                GROUP BY user_id 
                ORDER BY total_points DESC
            ''') as cursor:
                return await cursor.fetchall()

    # --- TEAM OPERATIONS ---
    async def get_all_teams(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM teams ORDER BY name ASC") as cursor:
                return await cursor.fetchall()

    async def get_team_by_id(self, team_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM teams WHERE id = ?", (team_id,)) as cursor:
                return await cursor.fetchone()

    async def upsert_team(self, name: str, stream_url: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR REPLACE INTO teams (name, stream_url) 
                VALUES (?, ?)
            ''', (name, stream_url))
            await db.commit()
            async with db.execute("SELECT id FROM teams WHERE name = ?", (name,)) as cursor:
                row = await cursor.fetchone()
                return row[0]

    async def update_team_logo(self, team_id: int, logo_path: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE teams SET logo_path = ? WHERE id = ?", (logo_path, team_id))
            await db.commit()

    async def delete_team(self, team_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM teams WHERE id = ?", (team_id,))
            await db.commit()

db = AsyncPickemsDB(DB_PATH)

# --- LOCKING SYSTEM ---
vote_locks: Dict[int, asyncio.Lock] = {}

def get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in vote_locks:
        vote_locks[user_id] = asyncio.Lock()
    return vote_locks[user_id]

# --- HELPER FUNCTIONS ---

def stitch_team_logos(logo1_path: str, logo2_path: str, dest_path: str):
    """Stitches two team logos side-by-side with a VS separator."""
    img1 = Image.open(logo1_path).convert("RGBA")
    img2 = Image.open(logo2_path).convert("RGBA")
    
    # Standardize height
    base_height = 400
    img1 = img1.resize((int(img1.width * (base_height / img1.height)), base_height), Image.Resampling.LANCZOS)
    img2 = img2.resize((int(img2.width * (base_height / img2.height)), base_height), Image.Resampling.LANCZOS)
    
    padding = 60
    vs_text = "VS"
    
    # Try to load a font
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 80)
    except:
        font = ImageFont.load_default()

    # Get VS text size
    dummy_draw = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
    bbox = dummy_draw.textbbox((0, 0), vs_text, font=font)
    vs_w = bbox[2] - bbox[0]
    vs_h = bbox[3] - bbox[1]
    
    total_width = img1.width + img2.width + vs_w + (padding * 4)
    canvas = Image.new("RGBA", (total_width, base_height), (0, 0, 0, 0))
    
    # Paste logos
    canvas.paste(img1, (padding, 0), img1)
    canvas.paste(img2, (img1.width + vs_w + (padding * 3), 0), img2)
    
    # Draw VS
    draw = ImageDraw.Draw(canvas)
    vs_x = img1.width + (padding * 2)
    vs_y = (base_height // 2) - (vs_h // 2) - 10
    draw.text((vs_x, vs_y), vs_text, fill=(255, 255, 255, 255), font=font)
    
    canvas.save(dest_path)

async def build_match_embed(match: dict) -> discord.Embed:
    """Dynamically builds the embed based on the current match state."""
    if not isinstance(match, dict):
        match = dict(match)
        
    twitch_emoji = await db.get_config('twitch_emoji') or ""
    prefix = f"{twitch_emoji} " if twitch_emoji else ""
    
    desc_lines = []
    desc_lines.append(f"**{match['team1']} vs {match['team2']}**")
    if match.get('title'):
        desc_lines.append(match['title'])
    
    if match.get('body_text'):
        desc_lines.append("")
        desc_lines.append(match['body_text'])

    streams = []
    if match.get('stream_url'):
        streams.append(f"{prefix}[Main Broadcast]({match['stream_url']})")
    if match.get('team1_stream_url'):
        streams.append(f"{prefix}[{match['team1']}]({match['team1_stream_url']})")
    if match.get('team2_stream_url'):
        streams.append(f"{prefix}[{match['team2']}]({match['team2_stream_url']})")

    if streams:
        desc_lines.append("")
        desc_lines.append("Stream(s):")
        desc_lines.extend(streams)

    embed = discord.Embed(description="\n".join(desc_lines), color=COLOR_PRIMARY)
        
    # Voting Status (Now in Footer)
    footer_text = ""
    if match['status'] == 'published':
        if match.get('close_time'):
            # We use a standard timestamp in footer because footers don't support dynamic Discord markdown timestamps
            t_str = time.strftime('%H:%M %p UTC', time.gmtime(match['close_time']))
            footer_text = f"🟢 Pick'em Open • Closes at {t_str}"
        else:
            footer_text = "🟢 Pick'em Open"
    elif match['status'] == 'closed':
        footer_text = "🔴 Pick'em Closed"
    elif match['status'] == 'resolved':
        footer_text = f"✅ Pick'em Resolved ({match['team1']} {match['score_1']} - {match['score_2']} {match['team2']})"
    
    if footer_text:
        embed.set_footer(text=footer_text)

    # Re-attach local asset URLs structurally
    if match.get('logo_path') and os.path.exists(match['logo_path']):
        embed.set_thumbnail(url="attachment://logo.png")
    if match.get('map_path') and os.path.exists(match['map_path']):
        embed.set_image(url="attachment://map.png")
        
    return embed

async def update_match_message(bot: commands.Bot, match_id: int):
    """Safely updates a match message in the announcement channel, removing buttons if closed."""
    match = await db.get_match_by_id(match_id)
    if not match or not match['message_id']: return
    
    channel_id = await db.get_config('announce_channel')
    if not channel_id: return
    
    channel = bot.get_channel(int(channel_id))
    if not channel: return
    
    try:
        msg = await channel.fetch_message(int(match['message_id']))
        embed = await build_match_embed(match)
        
        # If published, keep the button. If closed or resolved, remove the UI completely.
        view = PersistentMatchVoteView() if match['status'] == 'published' else None
        await msg.edit(embed=embed, view=view)
    except discord.NotFound:
        pass


# --- UI COMPONENTS ---

class LeaderboardPaginationView(discord.ui.View):
    def __init__(self, data: list, guild: discord.Guild):
        super().__init__(timeout=180)
        self.data = data
        self.guild = guild
        self.current_page = 0
        self.per_page = 10
        self.max_pages = max(1, (len(data) + self.per_page - 1) // self.per_page)
        self.update_buttons()

    def update_buttons(self):
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.max_pages - 1

    def generate_embed(self) -> discord.Embed:
        embed = discord.Embed(title="Pick'em Leaderboard", color=COLOR_ACCENT)
        start_idx = self.current_page * self.per_page
        end_idx = start_idx + self.per_page
        page_data = self.data[start_idx:end_idx]

        if not page_data:
            embed.description = "No predictions have been scored yet."
            return embed

        desc = ""
        for i, row in enumerate(page_data, start=start_idx + 1):
            member = self.guild.get_member(int(row['user_id']))
            name = member.display_name if member else f"User {row['user_id']}"
            desc += f"**{i}.** {name} — **{row['total_points']} pts**\n"
            
        embed.description = desc
        embed.set_footer(text=f"Page {self.current_page + 1}/{self.max_pages} | Max 2 points per match")
        return embed

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, custom_id="lb_ephemeral_prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.generate_embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, custom_id="lb_ephemeral_next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.generate_embed(), view=self)


class PersistentLeaderboardTrigger(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="View Full Leaderboard", style=discord.ButtonStyle.secondary, custom_id="bvl_lb_trigger")
    async def view_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await db.get_leaderboard()
        if not data:
            return await interaction.response.send_message("No data available yet.", ephemeral=True)
            
        view = LeaderboardPaginationView(data, interaction.guild)
        embed = view.generate_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class PickemChoiceView(discord.ui.View):
    def __init__(self, match_id: int, team1: str, team2: str, current_pred=None):
        super().__init__(timeout=300)
        self.match_id = match_id
        
        t1_20_label = f"{team1} (2-0)" + (" [Current]" if current_pred and current_pred['predicted_winner'] == 1 and current_pred['predicted_score_2'] == 0 else "")
        t1_21_label = f"{team1} (2-1)" + (" [Current]" if current_pred and current_pred['predicted_winner'] == 1 and current_pred['predicted_score_2'] == 1 else "")
        t2_20_label = f"{team2} (2-0)" + (" [Current]" if current_pred and current_pred['predicted_winner'] == 2 and current_pred['predicted_score_1'] == 0 else "")
        t2_21_label = f"{team2} (2-1)" + (" [Current]" if current_pred and current_pred['predicted_winner'] == 2 and current_pred['predicted_score_1'] == 1 else "")

        self.add_item(PredictionButton(label=t1_20_label, style=discord.ButtonStyle.primary, row=0, match_id=match_id, winner=1, s1=2, s2=0))
        self.add_item(PredictionButton(label=t1_21_label, style=discord.ButtonStyle.primary, row=0, match_id=match_id, winner=1, s1=2, s2=1))
        self.add_item(PredictionButton(label=t2_20_label, style=discord.ButtonStyle.danger, row=1, match_id=match_id, winner=2, s1=0, s2=2))
        self.add_item(PredictionButton(label=t2_21_label, style=discord.ButtonStyle.danger, row=1, match_id=match_id, winner=2, s1=1, s2=2))

class PredictionButton(discord.ui.Button):
    def __init__(self, label: str, style: discord.ButtonStyle, row: int, match_id: int, winner: int, s1: int, s2: int):
        super().__init__(label=label, style=style, row=row)
        self.match_id = match_id
        self.winner = winner
        self.s1 = s1
        self.s2 = s2

    async def callback(self, interaction: discord.Interaction):
        # Double check match status right before saving to prevent race condition voting
        match = await db.get_match_by_id(self.match_id)
        if match['status'] != 'published':
            embed = discord.Embed(description="Voting is closed for this match.", color=discord.Color.red())
            return await interaction.response.edit_message(embed=embed, view=None)

        lock = get_user_lock(interaction.user.id)
        async with lock:
            await db.save_prediction(interaction.user.id, self.match_id, self.winner, self.s1, self.s2)
            
            team_name = match['team1'] if self.winner == 1 else match['team2']
            score_str = "2-0" if (self.s1 == 2 and self.s2 == 0) or (self.s2 == 2 and self.s1 == 0) else "2-1"
            
            embed = discord.Embed(
                description=f"Prediction locked in: **{team_name} ({score_str})**",
                color=COLOR_PRIMARY
            )
            await interaction.response.edit_message(embed=embed, view=None)


class PersistentMatchVoteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Pick'em", style=discord.ButtonStyle.primary, custom_id="bvl_match_vote_btn")
    async def vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        match = await db.get_match_by_message_id(str(interaction.message.id))
        
        if not match:
            return await interaction.response.send_message("Cannot locate match data.", ephemeral=True)
            
        if match['status'] != 'published':
            return await interaction.response.send_message("Voting is permanently closed for this match.", ephemeral=True)

        current_pred = await db.get_user_prediction(interaction.user.id, match['id'])
        view = PickemChoiceView(match['id'], match['team1'], match['team2'], current_pred)
        
        prompt = "Select your predicted outcome:"
        if current_pred:
            prompt = "You have already voted. Selecting an option below will update your prediction:"
            
        await interaction.response.send_message(prompt, view=view, ephemeral=True)


class TeamInfoModal(discord.ui.Modal):
    name = discord.ui.TextInput(label='Team Name', required=True)
    stream_url = discord.ui.TextInput(label='Default Stream URL (Optional)', required=False)

    def __init__(self, team_data=None):
        title = 'Edit Team' if team_data else 'Add New Team'
        super().__init__(title=title)
        self.team_data = dict(team_data) if team_data else None
        if self.team_data:
            self.name.default = self.team_data['name']
            self.stream_url.default = self.team_data['stream_url'] or ''

    async def on_submit(self, interaction: discord.Interaction):
        team_id = await db.upsert_team(self.name.value, self.stream_url.value)
        
        if not self.team_data or not self.team_data['logo_path']:
            embed = discord.Embed(
                title="Team Saved: Step 2/2",
                description=f"Team '{self.name.value}' created. Now send **ONE** image for the team logo.\n\n*Awaiting logo for 3 minutes.*",
                color=COLOR_PRIMARY
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

            def check(m):
                return m.author == interaction.user and m.channel == interaction.channel

            try:
                msg = await interaction.client.wait_for('message', check=check, timeout=180.0)
                if msg.attachments:
                    logo_path = os.path.join(ASSETS_DIR, f"team_{team_id}_logo_{msg.attachments[0].filename}")
                    await msg.attachments[0].save(logo_path)
                    await db.update_team_logo(team_id, logo_path)
                    try: await msg.delete() 
                    except: pass
                    await interaction.followup.send(f"✅ Logo secured for {self.name.value}.", ephemeral=True)
                else:
                    await interaction.followup.send("⚠️ No logo attached. Team created without logo.", ephemeral=True)
            except asyncio.TimeoutError:
                await interaction.followup.send("⏰ Logo upload timed out. Team created without logo.", ephemeral=True)
        else:
            await interaction.response.send_message(f"✅ Team '{self.name.value}' updated.", ephemeral=True)


class TeamManagementView(discord.ui.View):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Add Team", style=discord.ButtonStyle.success, row=0)
    async def add_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(TeamInfoModal())

    @discord.ui.button(label="Edit/Delete Team", style=discord.ButtonStyle.secondary, row=0)
    async def edit_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        teams = await db.get_all_teams()
        if not teams:
            return await interaction.response.send_message("No teams configured.", ephemeral=True)
            
        view = discord.ui.View()
        options = [discord.SelectOption(label=t['name'], value=str(t['id'])) for t in teams[:25]]
        
        select = discord.ui.Select(placeholder="Select team to manage...", options=options)
        
        async def select_callback(inter: discord.Interaction):
            team = await db.get_team_by_id(int(select.values[0]))
            
            sub_view = discord.ui.View()
            
            edit_btn = discord.ui.Button(label="Edit Info", style=discord.ButtonStyle.primary)
            async def edit_cb(i): await i.response.send_modal(TeamInfoModal(team))
            edit_btn.callback = edit_cb
            
            del_btn = discord.ui.Button(label="Delete", style=discord.ButtonStyle.danger)
            async def del_cb(i):
                await db.delete_team(team['id'])
                await i.response.send_message(f"Team '{team['name']}' deleted.", ephemeral=True)
            del_btn.callback = del_cb
            
            sub_view.add_item(edit_btn)
            sub_view.add_item(del_btn)
            await inter.response.send_message(f"Managing team: **{team['name']}**", view=sub_view, ephemeral=True)
            
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a team:", view=view, ephemeral=True)


class TeamSelectionView(discord.ui.View):
    """Temporary view to let admin pick teams before opening DraftInfoModal."""
    def __init__(self, teams, bot: commands.Bot):
        super().__init__(timeout=300)
        self.teams = teams
        self.bot = bot
        self.t1_id = None
        self.t2_id = None

        options = [discord.SelectOption(label=t['name'], value=str(t['id'])) for t in teams[:25]]
        
        self.s1 = discord.ui.Select(placeholder="Select Team 1...", options=options, row=0)
        self.s2 = discord.ui.Select(placeholder="Select Team 2...", options=options, row=1)
        
        self.s1.callback = self.s1_cb
        self.s2.callback = self.s2_cb
        
        self.add_item(self.s1)
        self.add_item(self.s2)
        
        self.confirm = discord.ui.Button(label="Proceed to Draft", style=discord.ButtonStyle.success, row=2)
        self.confirm.callback = self.confirm_cb
        self.add_item(self.confirm)

    async def s1_cb(self, interaction: discord.Interaction):
        self.t1_id = int(self.s1.values[0])
        await interaction.response.defer()

    async def s2_cb(self, interaction: discord.Interaction):
        self.t2_id = int(self.s2.values[0])
        await interaction.response.defer()

    async def confirm_cb(self, interaction: discord.Interaction):
        if not self.t1_id or not self.t2_id or self.t1_id == self.t2_id:
            return await interaction.response.send_message("Select two different teams.", ephemeral=True)
            
        t1 = await db.get_team_by_id(self.t1_id)
        t2 = await db.get_team_by_id(self.t2_id)
        
        # Pre-fill modal data
        match_data = {
            'team1': t1['name'],
            'team2': t2['name'],
            'team1_stream_url': t1['stream_url'],
            'team2_stream_url': t2['stream_url'],
            'pre_t1_logo': t1['logo_path'],
            'pre_t2_logo': t2['logo_path']
        }
        await interaction.response.send_modal(DraftInfoModal(prefill=match_data))


class DraftInfoModal(discord.ui.Modal):
    match_title = discord.ui.TextInput(label='Match Title', placeholder='League Season 2 Playday #1', required=True)
    team1 = discord.ui.TextInput(label='Team 1 Name', required=True)
    team2 = discord.ui.TextInput(label='Team 2 Name', required=True)
    body_text = discord.ui.TextInput(label='Custom Body Text', style=discord.TextStyle.paragraph, required=False)

    def __init__(self, match_data=None, prefill=None):
        title = 'Edit Draft Info' if match_data else 'Draft New Match'
        super().__init__(title=title)
        self.match_data = dict(match_data) if match_data else None
        self.prefill = prefill
        
        if self.match_data:
            self.match_title.default = self.match_data['title']
            self.team1.default = self.match_data['team1']
            self.team2.default = self.match_data['team2']
            self.body_text.default = self.match_data['body_text']
        elif self.prefill:
            self.team1.default = self.prefill['team1']
            self.team2.default = self.prefill['team2']

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        if self.match_data:
            await db.update_draft(
                self.match_data['id'],
                self.match_title.value, 
                self.team1.value, 
                self.team2.value, 
                self.match_data['stream_url'],
                self.match_data.get('team1_stream_url', ''),
                self.match_data.get('team2_stream_url', ''),
                self.body_text.value
            )
            await interaction.channel.send(f"✅ {interaction.user.mention} Draft '{self.match_title.value}' info updated.", delete_after=10)
            return

        # Initial draft creation
        match_id = await db.create_draft(
            self.match_title.value, 
            self.team1.value, 
            self.team2.value, 
            "", # stream_url
            self.prefill.get('team1_stream_url', '') if self.prefill else '',
            self.prefill.get('team2_stream_url', '') if self.prefill else '',
            self.body_text.value
        )

        final_logo_path = os.path.join(ASSETS_DIR, f"{match_id}_stitched_logo.png")

        # LOGIC FOR AUTO-STITCHING IF PREFILL LOGOS EXIST
        if self.prefill and self.prefill.get('pre_t1_logo') and self.prefill.get('pre_t2_logo'):
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, stitch_team_logos, self.prefill['pre_t1_logo'], self.prefill['pre_t2_logo'], final_logo_path)
            
            embed_skip = discord.Embed(
                title="Draft: Step 1/1 (Logos Pre-stitched!)",
                description=f"Logos for **{self.team1.value}** and **{self.team2.value}** were used. Now send **ONE** image for the Map Bans.\n\n*Awaiting map image for 3 minutes. Type `skip` to save without one.*",
                color=COLOR_PRIMARY
            )
            await interaction.channel.send(content=interaction.user.mention, embed=embed_skip, delete_after=60)
        else:
            embed = discord.Embed(
                title="Draft Initialized: Step 1/2",
                description="Send a message with **TWO** team logos (Team 1 & Team 2) to be stitched.\n\n*Awaiting logos for 3 minutes. Type `cancel` to abort.*",
                color=COLOR_PRIMARY
            )
            await interaction.channel.send(content=interaction.user.mention, embed=embed, delete_after=60)

            def check(m):
                return m.author == interaction.user and m.channel == interaction.channel

            try:
                # Step 1: Logos
                msg = await interaction.client.wait_for('message', check=check, timeout=180.0)
                
                if msg.content.lower() == 'cancel':
                    try: await msg.delete() 
                    except: pass
                    await db.delete_draft(match_id)
                    return await interaction.channel.send(f"{interaction.user.mention} Draft cancelled.", delete_after=10)

                if len(msg.attachments) < 2:
                    try: await msg.delete() 
                    except: pass
                    await db.delete_draft(match_id)
                    return await interaction.channel.send(f"{interaction.user.mention} Error: 2 logo attachments required. Draft aborted.", delete_after=10)

                l1_temp = os.path.join(ASSETS_DIR, f"{match_id}_temp_l1.png")
                l2_temp = os.path.join(ASSETS_DIR, f"{match_id}_temp_l2.png")
                await msg.attachments[0].save(l1_temp)
                await msg.attachments[1].save(l2_temp)
                try: await msg.delete() 
                except: pass

                # Stitch logos
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, stitch_team_logos, l1_temp, l2_temp, final_logo_path)

                embed2 = discord.Embed(
                    title="Draft: Step 2/2", 
                    description="Logos stitched! Now send **ONE** image for the Map Bans.\n\n*Awaiting map image for 3 minutes. Type `skip` to save without one.*", 
                    color=COLOR_PRIMARY
                )
                await interaction.channel.send(content=interaction.user.mention, embed=embed2, delete_after=60)
            except asyncio.TimeoutError:
                await db.delete_draft(match_id)
                return await interaction.channel.send(f"⏰ {interaction.user.mention} Draft timed out.", delete_after=10)

        # Step 2: Map Bans (Unified for both paths)
        def check_map(m):
            return m.author == interaction.user and m.channel == interaction.channel

        try:
            msg2 = await interaction.client.wait_for('message', check=check_map, timeout=180.0)
            
            if msg2.content.lower() == 'skip':
                try: await msg2.delete() 
                except: pass
                await db.update_draft_images(match_id, final_logo_path, None)
                return await interaction.channel.send(f"✅ {interaction.user.mention} Draft saved without map image! Use 'Edit Images' later to add it.", delete_after=30)
            
            if not msg2.attachments:
                try: await msg2.delete() 
                except: pass
                await db.delete_draft(match_id)
                return await interaction.channel.send(f"{interaction.user.mention} Error: Map attachment required. Draft aborted.", delete_after=10)

            map_path = os.path.join(ASSETS_DIR, f"{match_id}_map_{msg2.attachments[0].filename}")
            await msg2.attachments[0].save(map_path)
            try: await msg2.delete() 
            except: pass 

            await db.update_draft_images(match_id, final_logo_path, map_path)
            await interaction.channel.send(f"✅ {interaction.user.mention} Draft complete! Use 'Manage Drafts' for stream URLs or preview.", delete_after=30)

        except asyncio.TimeoutError:
            await db.delete_draft(match_id)
            try: await interaction.channel.send(f"⏰ {interaction.user.mention} Draft timed out.", delete_after=10)
            except: pass
        except Exception as e:
            logger.error(f"Error in draft upload: {e}")
            try: await interaction.channel.send(f"❌ {interaction.user.mention} An error occurred: {e}", delete_after=10)
            except: pass


class DraftLinksModal(discord.ui.Modal, title='Edit Draft Links'):
    stream_url = discord.ui.TextInput(label='Main Stream URL (Optional)', required=False)
    team1_stream_url = discord.ui.TextInput(label='Team 1 Stream URL (Optional)', required=False)
    team2_stream_url = discord.ui.TextInput(label='Team 2 Stream URL (Optional)', required=False)

    def __init__(self, match_data):
        super().__init__()
        self.match_data = dict(match_data)
        self.stream_url.default = self.match_data['stream_url'] or ''
        self.team1_stream_url.default = self.match_data.get('team1_stream_url', '') or ''
        self.team2_stream_url.default = self.match_data.get('team2_stream_url', '') or ''

    async def on_submit(self, interaction: discord.Interaction):
        await db.update_draft(
            self.match_data['id'],
            self.match_data['title'],
            self.match_data['team1'],
            self.match_data['team2'],
            self.stream_url.value,
            self.team1_stream_url.value,
            self.team2_stream_url.value,
            self.match_data['body_text']
        )
        await interaction.response.send_message(f"Draft links updated.", ephemeral=True)


class DraftManagementView(discord.ui.View):
    def __init__(self, match_data, bot: commands.Bot, is_published: bool = False):
        super().__init__(timeout=300)
        self.match_data = dict(match_data)
        self.bot = bot
        self.is_published = is_published
        
        if self.is_published:
            self.add_item(ApplyLiveUpdatesButton())

    @discord.ui.button(label="Edit Info", style=discord.ButtonStyle.primary)
    async def edit_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DraftInfoModal(self.match_data))

    @discord.ui.button(label="Edit Links", style=discord.ButtonStyle.primary)
    async def edit_links(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DraftLinksModal(self.match_data))

    @discord.ui.button(label="Edit Images", style=discord.ButtonStyle.primary)
    async def edit_images(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        match_id = self.match_data['id']
        
        def check(m):
            return m.author == interaction.user and m.channel == interaction.channel

        has_logos = self.match_data.get('logo_path') and os.path.exists(self.match_data['logo_path'])

        if has_logos:
            # Skip asking for logos if they already exist for this draft
            embed_skip = discord.Embed(
                title="Update Images: Map Bans",
                description="Team logos are already set for this match. Send **ONE** image to update the Map Bans.\n\n*Awaiting map image for 3 minutes. Type `cancel` to abort.*",
                color=COLOR_PRIMARY
            )
            await interaction.channel.send(content=interaction.user.mention, embed=embed_skip, delete_after=60)
            
            try:
                msg2 = await interaction.client.wait_for('message', check=check, timeout=180.0)
                if msg2.content.lower() == 'cancel':
                    try: await msg2.delete() 
                    except: pass
                    return await interaction.channel.send(f"{interaction.user.mention} Image update cancelled.", delete_after=10)

                if not msg2.attachments:
                    try: await msg2.delete() 
                    except: pass
                    return await interaction.channel.send(f"{interaction.user.mention} Error: Map attachment required.", delete_after=10)

                map_path = os.path.join(ASSETS_DIR, f"{match_id}_map_{int(time.time())}_{msg2.attachments[0].filename}")
                await msg2.attachments[0].save(map_path)
                try: await msg2.delete() 
                except: pass 

                await db.update_draft_images(match_id, self.match_data['logo_path'], map_path)
                self.match_data['map_path'] = map_path
                
                await interaction.channel.send(f"✅ {interaction.user.mention} Map image updated! " + ("Use 'Apply Live Updates' to push." if self.is_published else ""), delete_after=30)
            except asyncio.TimeoutError:
                try: await interaction.channel.send(f"⏰ {interaction.user.mention} Image update timed out.", delete_after=10)
                except: pass
            except Exception as e:
                logger.error(f"Error in map image update: {e}")
                try: await interaction.channel.send(f"❌ {interaction.user.mention} An error occurred: {e}", delete_after=10)
                except: pass
            return

        # Original flow if no logos exist
        embed = discord.Embed(
            title="Update Images: Step 1/2",
            description="Send **TWO** team logos to be stitched.\n\n*Awaiting logos for 3 minutes. Type `cancel` to abort.*",
            color=COLOR_PRIMARY
        )
        await interaction.channel.send(content=interaction.user.mention, embed=embed, delete_after=60)

        try:
            # Step 1: Logos
            msg = await interaction.client.wait_for('message', check=check, timeout=180.0)
            
            if msg.content.lower() == 'cancel':
                try: await msg.delete() 
                except: pass
                return await interaction.channel.send(f"{interaction.user.mention} Image update cancelled.", delete_after=10)

            if len(msg.attachments) < 2:
                try: await msg.delete() 
                except: pass
                return await interaction.channel.send(f"{interaction.user.mention} Error: 2 logo attachments required.", delete_after=10)

            l1_path = os.path.join(ASSETS_DIR, f"{match_id}_raw_l1.png")
            l2_path = os.path.join(ASSETS_DIR, f"{match_id}_raw_l2.png")
            final_logo_path = os.path.join(ASSETS_DIR, f"{match_id}_stitched_logo_{int(time.time())}.png")

            # SAVE FIRST
            await msg.attachments[0].save(l1_path)
            await msg.attachments[1].save(l2_path)
            
            # DELETE AFTER SAVING
            try: await msg.delete() 
            except: pass 

            # Stitch logos
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, stitch_team_logos, l1_path, l2_path, final_logo_path)

            # Step 2: Map Bans
            embed2 = discord.Embed(
                title="Update Images: Step 2/2",
                description="Logos updated! Now send **ONE** image for the Map Bans.",
                color=COLOR_PRIMARY
            )
            await interaction.channel.send(content=interaction.user.mention, embed=embed2, delete_after=60)

            msg2 = await interaction.client.wait_for('message', check=check, timeout=180.0)
            
            if not msg2.attachments:
                try: await msg2.delete() 
                except: pass
                return await interaction.channel.send(f"{interaction.user.mention} Error: Map attachment required.", delete_after=10)

            map_path = os.path.join(ASSETS_DIR, f"{match_id}_map_{int(time.time())}_{msg2.attachments[0].filename}")
            
            # SAVE FIRST
            await msg2.attachments[0].save(map_path)
            
            # DELETE AFTER SAVING
            try: await msg2.delete() 
            except: pass 

            await db.update_draft_images(match_id, final_logo_path, map_path)
            
            # Update local state so preview/apply works immediately
            self.match_data['logo_path'] = final_logo_path
            self.match_data['map_path'] = map_path
            
            await interaction.channel.send(f"✅ {interaction.user.mention} Images updated in database. " + ("Use 'Apply Live Updates' to push to the announcement channel." if self.is_published else ""), delete_after=30)

        except asyncio.TimeoutError:
            try: await interaction.channel.send(f"⏰ {interaction.user.mention} Image update timed out.", delete_after=10)
            except: pass
        except Exception as e:
            logger.error(f"Error in draft image update: {e}")
            try: await interaction.channel.send(f"❌ {interaction.user.mention} An error occurred: {e}", delete_after=10)
            except: pass

    @discord.ui.button(label="Preview", style=discord.ButtonStyle.secondary)
    async def preview_draft(self, interaction: discord.Interaction, button: discord.ui.Button):
        match = await db.get_match_by_id(self.match_data['id'])
        embed = await build_match_embed(match)
        
        files = []
        if match['logo_path'] and os.path.exists(match['logo_path']):
            files.append(discord.File(match['logo_path'], filename="logo.png"))
        if match['map_path'] and os.path.exists(match['map_path']):
            files.append(discord.File(match['map_path'], filename="map.png"))
            
        await interaction.response.send_message(
            "Current State Preview (Ephemeral):", 
            embed=embed, 
            files=files, 
            ephemeral=True
        )

    @discord.ui.button(label="Delete", style=discord.ButtonStyle.danger)
    async def delete_draft(self, interaction: discord.Interaction, button: discord.ui.Button):
        await db.delete_draft(self.match_data['id'])
        await interaction.response.send_message("Draft/Match deleted from database.", ephemeral=True)
        self.stop()


class ApplyLiveUpdatesButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Apply Live Updates", style=discord.ButtonStyle.success, row=1)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        view: DraftManagementView = self.view
        
        # Get latest data from DB
        match = await db.get_match_by_id(view.match_data['id'])
        if not match or not match['message_id']:
            return await interaction.followup.send("Cannot find live message to update.", ephemeral=True)

        announce_id = await db.get_config('announce_channel')
        if not announce_id:
            return await interaction.followup.send("Announcement channel not configured.", ephemeral=True)

        channel = interaction.guild.get_channel(int(announce_id))
        if not channel:
            return await interaction.followup.send("Could not find announcement channel.", ephemeral=True)

        try:
            msg = await channel.fetch_message(int(match['message_id']))
            embed = await build_match_embed(match)
            
            # Since images might have changed, we MUST re-upload them to bypass Discord's caching
            files = []
            if match['logo_path'] and os.path.exists(match['logo_path']):
                files.append(discord.File(match['logo_path'], filename="logo.png"))
            if match['map_path'] and os.path.exists(match['map_path']):
                files.append(discord.File(match['map_path'], filename="map.png"))
            
            # Editing a message with new files actually replaces them
            await msg.edit(embed=embed, attachments=files, view=PersistentMatchVoteView())
            await interaction.followup.send("✅ Live announcement updated successfully.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to update live match: {e}")
            await interaction.followup.send(f"❌ Error updating live message: {e}", ephemeral=True)



class VotingTimerModal(discord.ui.Modal, title='Set Voting Close Time'):
    time_input = discord.ui.TextInput(
        label='Closing Time (e.g. 7:05pm, 19:05, or "now")',
        placeholder='7:05pm',
        required=True
    )

    def __init__(self, match_data, bot: commands.Bot):
        super().__init__()
        self.match_data = match_data
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.time_input.value.strip().lower()
        
        if raw == 'now':
            await db.set_match_status(self.match_data['id'], 'closed')
            await update_match_message(self.bot, self.match_data['id'])
            return await interaction.response.send_message("Voting closed immediately.", ephemeral=True)

        # Regex for common time formats
        match = re.match(r"(\d{1,2}):(\d{2})\s*(am|pm)?", raw)
        if not match:
            return await interaction.response.send_message("Invalid format. Use 7:05pm, 19:05, etc.", ephemeral=True)

        hr, mn, meridiem = match.groups()
        hr, mn = int(hr), int(mn)

        if meridiem:
            if meridiem == 'pm' and hr < 12: hr += 12
            if meridiem == 'am' and hr == 12: hr = 0
        
        now = datetime.now()
        target = now.replace(hour=hr, minute=mn, second=0, microsecond=0)
        
        # If target time is earlier today, assume they mean tomorrow
        if target < now:
            target += timedelta(days=1)

        close_timestamp = int(target.timestamp())
        await db.set_match_status(self.match_data['id'], 'published') # Ensure it's not 'closed'
        await db.set_match_close_time(self.match_data['id'], close_timestamp)
        await update_match_message(self.bot, self.match_data['id'])
        
        await interaction.response.send_message(f"Timer updated. Voting will close <t:{close_timestamp}:R> ({target.strftime('%I:%M %p')}).", ephemeral=True)


class ResolveModal(discord.ui.Modal):
    def __init__(self, match_data, bot: commands.Bot):
        super().__init__(title=f"Resolve Match")
        self.match_data = match_data
        self.bot = bot
        
        self.t1_score = discord.ui.TextInput(label=f"{match_data['team1']} Score (0-2)", required=True, max_length=1)
        self.t2_score = discord.ui.TextInput(label=f"{match_data['team2']} Score (0-2)", required=True, max_length=1)
        
        self.add_item(self.t1_score)
        self.add_item(self.t2_score)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            s1 = int(self.t1_score.value)
            s2 = int(self.t2_score.value)
        except ValueError:
            return await interaction.response.send_message("Scores must be integer values.", ephemeral=True)

        winner = 1 if s1 > s2 else 2
        await db.resolve_match(self.match_data['id'], winner, s1, s2)
        await update_match_message(self.bot, self.match_data['id'])

        await interaction.response.send_message(f"Match resolved: {self.match_data['team1']} {s1} - {s2} {self.match_data['team2']}.", ephemeral=True)


class EmojiModal(discord.ui.Modal, title='Set Twitch Emoji'):
    emoji_input = discord.ui.TextInput(label='Twitch Emoji (e.g. <:twitch:1234567>)', required=True)

    def __init__(self, current_emoji: str = ""):
        super().__init__()
        self.emoji_input.default = current_emoji

    async def on_submit(self, interaction: discord.Interaction):
        await db.set_config('twitch_emoji', self.emoji_input.value.strip())
        await interaction.response.send_message(f"Twitch emoji updated to {self.emoji_input.value.strip()}", ephemeral=True)


class AdminPanelView(discord.ui.View):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text, discord.ChannelType.news], placeholder="Set Announcement Channel", min_values=1, max_values=1, row=0)
    async def select_announce_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        await db.set_config('announce_channel', str(select.values[0].id))
        await interaction.response.send_message(f"Announcement channel bound to {select.values[0].mention}", ephemeral=True)

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text, discord.ChannelType.news], placeholder="Set Leaderboard Channel", min_values=1, max_values=1, row=1)
    async def select_leaderboard_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        await db.set_config('leaderboard_channel', str(select.values[0].id))
        await interaction.response.send_message(f"Leaderboard channel bound to {select.values[0].mention}", ephemeral=True)

    @discord.ui.button(label="Set Twitch Emoji", style=discord.ButtonStyle.secondary, row=3)
    async def set_emoji_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        current_emoji = await db.get_config('twitch_emoji') or ""
        await interaction.response.send_modal(EmojiModal(current_emoji))

    @discord.ui.button(label="Draft Match", style=discord.ButtonStyle.primary, row=2)
    async def draft_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        teams = await db.get_all_teams()
        if not teams:
            # Fallback to standard manual draft if no teams configured
            return await interaction.response.send_modal(DraftInfoModal())
            
        view = discord.ui.View()
        manual_btn = discord.ui.Button(label="Manual Entry", style=discord.ButtonStyle.secondary)
        async def manual_cb(i): await i.response.send_modal(DraftInfoModal())
        manual_btn.callback = manual_cb
        
        select_btn = discord.ui.Button(label="Pick Teams", style=discord.ButtonStyle.primary)
        async def select_cb(i): await i.response.send_message("Select teams for this draft:", view=TeamSelectionView(teams, self.bot), ephemeral=True)
        select_btn.callback = select_cb
        
        view.add_item(select_btn)
        view.add_item(manual_btn)
        await interaction.response.send_message("How would you like to initialize this draft?", view=view, ephemeral=True)

    @discord.ui.button(label="Manage Teams", style=discord.ButtonStyle.secondary, row=2)
    async def manage_teams_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Team Management Dashboard:", view=TeamManagementView(self.bot), ephemeral=True)

    @discord.ui.button(label="Manage Drafts", style=discord.ButtonStyle.secondary, row=2)
    async def manage_drafts_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        drafts = await db.get_matches_by_status('draft')
        if not drafts:
            return await interaction.response.send_message("No active drafts found.", ephemeral=True)
            
        view = discord.ui.View()
        options = [discord.SelectOption(label=f"{m['team1']} vs {m['team2']}", value=str(m['id'])) for m in drafts[:25]]
        
        select = discord.ui.Select(placeholder="Select draft to manage...", options=options)
        
        async def select_callback(inter: discord.Interaction):
            match = await db.get_match_by_id(int(select.values[0]))
            await inter.response.send_message(f"Managing draft: **{match['team1']} vs {match['team2']}**", view=DraftManagementView(match, self.bot), ephemeral=True)
            
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a draft to modify:", view=view, ephemeral=True)

    @discord.ui.button(label="Manage Published", style=discord.ButtonStyle.secondary, row=2)
    async def manage_published_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        published = await db.get_matches_by_status('published')
        if not published:
            return await interaction.response.send_message("No active published matches found.", ephemeral=True)
            
        view = discord.ui.View()
        options = [discord.SelectOption(label=f"{m['team1']} vs {m['team2']}", value=str(m['id'])) for m in published[:25]]
        
        select = discord.ui.Select(placeholder="Select match to edit...", options=options)
        
        async def select_callback(inter: discord.Interaction):
            match = await db.get_match_by_id(int(select.values[0]))
            await inter.response.send_message(f"Editing live match: **{match['team1']} vs {match['team2']}**", view=DraftManagementView(match, self.bot, is_published=True), ephemeral=True)
            
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a live match to modify:", view=view, ephemeral=True)

    @discord.ui.button(label="Publish Playday", style=discord.ButtonStyle.success, row=2)
    async def publish_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        announce_id = await db.get_config('announce_channel')
        if not announce_id:
            return await interaction.response.send_message("Announcement channel configuration missing.", ephemeral=True)
            
        channel = interaction.guild.get_channel(int(announce_id))
        drafts = await db.get_matches_by_status('draft')
        
        if not drafts:
            return await interaction.response.send_message("No valid drafts pending.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        for match in drafts:
            embed = await build_match_embed(match)
            
            # Attaching files on fresh send
            files = []
            if match['logo_path'] and os.path.exists(match['logo_path']):
                files.append(discord.File(match['logo_path'], filename="logo.png"))
            if match['map_path'] and os.path.exists(match['map_path']):
                files.append(discord.File(match['map_path'], filename="map.png"))

            msg = await channel.send(embed=embed, files=files, view=PersistentMatchVoteView())
            await db.publish_match(match['id'], str(msg.id))

        await interaction.followup.send(f"Playday published successfully. {len(drafts)} matches dispatched.", ephemeral=True)

    @discord.ui.button(label="Manage Voting", style=discord.ButtonStyle.secondary, row=3)
    async def manage_voting_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        published = await db.get_matches_by_status('published')
        if not published:
            return await interaction.response.send_message("No active matches found.", ephemeral=True)
            
        view = discord.ui.View()
        options = [discord.SelectOption(label=f"{m['team1']} vs {m['team2']}", value=str(m['id'])) for m in published[:25]]
        
        select = discord.ui.Select(placeholder="Select match to manage...", options=options)
        
        async def select_callback(inter: discord.Interaction):
            match = await db.get_match_by_id(int(select.values[0]))
            await inter.response.send_modal(VotingTimerModal(match, self.bot))
            
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a match to modify its voting state:", view=view, ephemeral=True)

    @discord.ui.button(label="Resolve Matches", style=discord.ButtonStyle.secondary, row=3)
    async def resolve_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        resolvable = await db.get_resolvable_matches()
        if not resolvable:
            return await interaction.response.send_message("No matches await resolution.", ephemeral=True)
            
        view = discord.ui.View()
        options = [discord.SelectOption(label=f"{m['team1']} vs {m['team2']}", value=str(m['id'])) for m in resolvable[:25]]
        
        select = discord.ui.Select(placeholder="Select match to score...", options=options)
        
        async def select_callback(inter: discord.Interaction):
            match = await db.get_match_by_id(int(select.values[0]))
            await inter.response.send_modal(ResolveModal(match, self.bot))
            
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Pending matches:", view=view, ephemeral=True)

    @discord.ui.button(label="Update Leaderboard", style=discord.ButtonStyle.secondary, row=4)
    async def leaderboard_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        lb_id = await db.get_config('leaderboard_channel')
        if not lb_id:
            return await interaction.response.send_message("Leaderboard channel configuration missing.", ephemeral=True)
            
        channel = interaction.guild.get_channel(int(lb_id))
        data = await db.get_leaderboard()
        
        embed = discord.Embed(title="Season Pick'em Leaderboard", color=COLOR_ACCENT)
        
        if not data:
            embed.description = "Awaiting match resolutions."
        else:
            desc = ""
            for i, row in enumerate(data[:10], start=1):
                member = interaction.guild.get_member(int(row['user_id']))
                name = member.display_name if member else f"User {row['user_id']}"
                desc += f"**{i}.** {name} — **{row['total_points']} pts**\n"
            embed.description = desc

        await channel.send(embed=embed, view=PersistentLeaderboardTrigger())
        await interaction.response.send_message(f"Leaderboard updated in {channel.mention}", ephemeral=True)

    @discord.ui.button(label="Resend Matches", style=discord.ButtonStyle.secondary, row=4)
    async def resend_matches_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        announce_id = await db.get_config('announce_channel')
        if not announce_id:
            return await interaction.response.send_message("Announcement channel configuration missing.", ephemeral=True)
            
        channel = interaction.guild.get_channel(int(announce_id))
        published = await db.get_matches_by_status('published')
        
        if not published:
            return await interaction.response.send_message("No active published matches to resend.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        for match in published:
            embed = await build_match_embed(match)
            files = []
            if match['logo_path'] and os.path.exists(match['logo_path']):
                files.append(discord.File(match['logo_path'], filename="logo.png"))
            if match['map_path'] and os.path.exists(match['map_path']):
                files.append(discord.File(match['map_path'], filename="map.png"))

            msg = await channel.send(embed=embed, files=files, view=PersistentMatchVoteView())
            # Update database with the NEW message ID so buttons work correctly
            await db.publish_match(match['id'], str(msg.id))

        await interaction.followup.send(f"Re-dispatched {len(published)} active matches to {channel.mention}.", ephemeral=True)

    @discord.ui.button(label="Test Run", style=discord.ButtonStyle.danger, row=4)
    async def test_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        announce_id = await db.get_config('announce_channel')
        lb_id = await db.get_config('leaderboard_channel')
        
        if not announce_id or not lb_id:
            return await interaction.response.send_message("Configure both channels to execute test.", ephemeral=True)
            
        await interaction.response.defer(ephemeral=True)
        
        channel_ann = interaction.guild.get_channel(int(announce_id))
        embed = discord.Embed(title="[TEST] League Season 2 Playday #1", description="Tune in to the [Casted Stream](https://twitch.tv) or choose a POV below!\n\n[Team A POV](https://youtube.com)\n[Team B POV](https://youtube.com)", color=COLOR_PRIMARY)
        embed.add_field(name="Voting Status", value="🟢 Open", inline=False)
        
        view = discord.ui.View()
        btn = discord.ui.Button(label="Pick'em (TEST)", style=discord.ButtonStyle.primary, disabled=True)
        view.add_item(btn)
        
        await channel_ann.send(embed=embed, view=view)
        
        channel_lb = interaction.guild.get_channel(int(lb_id))
        lb_embed = discord.Embed(title="[TEST] Pick'em Leaderboard", description="**1.** Admin — **10 pts**\n**2.** Player — **8 pts**", color=COLOR_ACCENT)
        lb_view = discord.ui.View()
        lb_btn = discord.ui.Button(label="View Full Leaderboard", style=discord.ButtonStyle.secondary, disabled=True)
        lb_view.add_item(lb_btn)
        
        await channel_lb.send(embed=lb_embed, view=lb_view)
        
        await interaction.followup.send("Test execution complete.", ephemeral=True)


class Pickems(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        await db.init_db()
        self.bot.add_view(PersistentMatchVoteView())
        self.bot.add_view(PersistentLeaderboardTrigger())
        self.auto_close_loop.start()

    async def cog_unload(self):
        self.auto_close_loop.cancel()

    @tasks.loop(seconds=30)
    async def auto_close_loop(self):
        """Background task that sweeps for expired timers and automatically closes them."""
        matches = await db.get_matches_by_status('published')
        current_time = int(time.time())
        for match in matches:
            if match['close_time'] and current_time >= match['close_time']:
                await db.set_match_status(match['id'], 'closed')
                await update_match_message(self.bot, match['id'])
                logger.info(f"Auto-closed Pick'em match {match['id']}")

    @auto_close_loop.before_loop
    async def before_auto_close(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="pickem_panel", description="Administration dashboard for Valorant Pick'ems.")
    @app_commands.default_permissions(administrator=True)
    async def pickem_panel(self, interaction: discord.Interaction):
        if getattr(self.bot, "is_bot_admin", None) and not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("Authorization denied.", ephemeral=True)

        embed = discord.Embed(
            title="Pick'em Administration", 
            description="Manage drafts, playdays, timers, and standings.",
            color=COLOR_PRIMARY
        )
        
        drafts = await db.get_matches_by_status('draft')
        if drafts:
            draft_text = "\n".join([f"- {m['team1']} vs {m['team2']}" for m in drafts])
            embed.add_field(name="Pending Drafts", value=draft_text, inline=False)
        else:
            embed.add_field(name="Pending Drafts", value="*No active drafts.*", inline=False)

        await interaction.response.send_message(embed=embed, view=AdminPanelView(self.bot), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(Pickems(bot))
    logger.info("Loaded robust Pickems cog with Auto-Close functionality.")


