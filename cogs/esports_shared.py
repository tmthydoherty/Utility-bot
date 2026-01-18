import discord
import json
import os
import datetime
import asyncio
import logging
import secrets
import shutil
import re
from typing import Dict, List, Optional, Any, Union, Set, Tuple
from PIL import Image, ImageOps, ImageFilter, ImageDraw, ImageFont
from io import BytesIO
from collections import OrderedDict

# --- CONFIGURATION ---
GAMES = {
    "valorant": "Valorant",
    "rl": "Rocket League",
    "r6siege": "Rainbow 6 Siege",
    "ow": "Overwatch"
    # Note: Apex Legends removed - PandaScore API does not support it
}

GAME_SHORT_NAMES = {
    "valorant": "Val",
    "rl": "RL",
    "r6siege": "R6",
    "ow": "OW"
}

# UPDATED: Valorant logo changed to user specified PNG
GAME_LOGOS = {
    "valorant": "https://i.postimg.cc/HsxQFd58/valorant.png",
    "rl": "https://i.postimg.cc/nrSL2j13/Rocket-League-Emblem-(2).png",
    "r6siege": "https://i.postimg.cc/52h6DzMM/rainbow-six-siege-logo-logo2.png",
    "ow": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/55/Overwatch_circle_logo.svg/600px-Overwatch_circle_logo.svg.png"
}

GAME_TWITCH_CHANNELS = {
    "valorant": "https://www.twitch.tv/valorant",
    "rl": "https://www.twitch.tv/rocketleague",
    "r6siege": "https://www.twitch.tv/rainbow6",
    "ow": "https://www.twitch.tv/overwatchleague"
}

GAME_PLACEHOLDERS = {
    "valorant": "https://i.postimg.cc/Fs3qHYjH/VS-20251127-025309-0000.png",
    "rl": "https://liquipedia.net/commons/images/9/90/RL_TeamImageMissing_darkmode.png",
    "r6siege": "https://liquipedia.net/commons/images/5/51/Rainbow_Six_Siege_default_darkmode.png",
    "ow": "https://liquipedia.net/commons/images/thumb/a/ae/Overwatch_ligaicon.png/100px-Overwatch_ligaicon.png"
}

DEFAULT_GAME_ICON_FALLBACK = "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3f/Trophy_icon.png/512px-Trophy_icon.png"

ALLOWED_TIERS = ["s", "a"]
TIER_BYPASS_KEYWORDS = [
    "championship", "champions", "major", "masters", "invitational",
    "world", "gamers8", "iem", "blast premier", "algs", "owl", "owcs"
]

STRAFE_GAME_SLUGS = {
    "valorant": "valorant",
    "rl": "rocketleague",
    "r6siege": "r6s",
    "ow": "overwatch"
}

# Strafe calendar/match URL paths (used for HTML scraping)
STRAFE_GAME_PATHS = {
    "valorant": "valorant",
    "rl": "rocket-league",
    "r6siege": "rainbow-six-siege",
    "ow": "overwatch"
}

GAME_MAP_FALLBACK = {
    "valorant": "Map",
    "rl": "Game",
    "r6siege": "Map",
    "ow": "Map"
}

DATA_FILE = "data/esports_data.json"
BACKUP_FILE = "data/esports_data.json.bak"
MAX_LEADERBOARD_NAME_LENGTH = 12
MAX_BUTTON_LABEL_LENGTH = 80
MAX_MAP_NAME_LENGTH = 18
USERNAME_PATTERN = re.compile(r'^[\w\-\.]{2,20}$', re.UNICODE)

logger = logging.getLogger("esports_shared")

# --- DATA HELPERS ---
def ensure_data_file():
    if not os.path.exists("data"):
        os.makedirs("data")
    
    if not os.path.exists(DATA_FILE):
        default_data = {
            "channel_id": None,
            "active_matches": {},
            "processed_matches": [],
            "leaderboards": {k: {} for k in GAMES.keys()},
            "last_reset_month": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m"),
            "emoji_map": {},
            "emoji_storage_guilds": []
        }
        try:
            with open(DATA_FILE, "w") as f:
                json.dump(default_data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to create data file: {e}")

def load_data_sync():
    def _load(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    try:
        data = _load(DATA_FILE)
    except (json.JSONDecodeError, FileNotFoundError):
        try:
            data = _load(BACKUP_FILE)
            save_data_sync(data)
        except Exception as e: 
            logger.error(f"Failed to load backup data: {e}")
            return {"channel_id": None, "active_matches": {}, "processed_matches": [], "leaderboards": {k: {} for k in GAMES.keys()}}

    # Ensure keys exist
    if "processed_matches" not in data: data["processed_matches"] = []
    if "last_reset_month" not in data: data["last_reset_month"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")
    if "emoji_map" not in data: data["emoji_map"] = {}
    if "emoji_storage_guilds" not in data: data["emoji_storage_guilds"] = []

    # Ensure leaderboards exists and has all game keys
    if "leaderboards" not in data:
        data["leaderboards"] = {k: {} for k in GAMES.keys()}
    else:
        # Ensure each game has its own leaderboard dict
        for game_key in GAMES.keys():
            if game_key not in data["leaderboards"]:
                data["leaderboards"][game_key] = {}
            # Ensure it's a dict, not some other type
            elif not isinstance(data["leaderboards"][game_key], dict):
                data["leaderboards"][game_key] = {}

    return data

def save_data_sync(data):
    temp_file = f"{DATA_FILE}.tmp"
    try:
        if os.path.exists(DATA_FILE):
            shutil.copy2(DATA_FILE, BACKUP_FILE)
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, DATA_FILE)
    except Exception as e:
        logger.error(f"Failed to save data to disk: {e}")
        if os.path.exists(temp_file): os.remove(temp_file)

def safe_parse_datetime(iso_string):
    if not iso_string: return None
    try:
        return datetime.datetime.fromisoformat(str(iso_string).replace("Z", "+00:00"))
    except Exception as e:
        logger.debug(f"Failed to parse datetime '{iso_string}': {e}")
        return None

# --- IMAGE HELPERS ---
def add_white_outline(img, thickness=4):
    alpha = img.getchannel('A')
    outline_mask = alpha.filter(ImageFilter.MaxFilter(thickness * 2 + 1))
    outline_img = Image.new("RGBA", img.size, (255, 255, 255, 255))
    outline_img.putalpha(outline_mask)
    final_img = Image.new("RGBA", img.size, (0, 0, 0, 0))
    final_img.paste(outline_img, (0, 0))
    final_img.paste(img, (0, 0), img)
    return final_img

def stitch_images(img_a, img_b, img_g, img_team_p, img_game_p, is_result=False, game_slug=""):
    try:
        width, height = 600, 200
        slot_w = width // 3
        canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0)) 
        
        if not img_team_p: img_team_p = Image.new("RGBA", (150, 150), (50, 50, 50, 255))
        if not img_game_p: img_game_p = Image.new("RGBA", (150, 150), (50, 50, 50, 255))

        final_a = img_a if img_a else img_team_p
        final_b = img_b if img_b else img_team_p
        final_g = img_g if img_g else img_game_p

        def place_image(img, x_offset, scale=1.0, add_outline=True):
            if not img: return
            if add_outline: img = add_white_outline(img)
            img_copy = img.copy()
            target_size = int(180 * scale)
            img_copy.thumbnail((target_size, target_size), Image.Resampling.LANCZOS)
            x = x_offset + (slot_w - img_copy.width) // 2
            y = (height - img_copy.height) // 2
            canvas.paste(img_copy, (x, y), img_copy)

        if is_result:
            place_image(final_g, 0, scale=0.6) 
            place_image(final_a, slot_w, scale=1.0)
            place_image(final_g, slot_w * 2, scale=0.6)
        else:
            place_image(final_a, 0)          
            mid_scale = 0.9 if game_slug == 'rl' else 0.6
            place_image(final_g, slot_w, scale=mid_scale, add_outline=False) 
            place_image(final_b, slot_w * 2) 

        buffer = BytesIO()
        canvas.save(buffer, format="PNG")
        buffer.seek(0)
        return discord.File(buffer, filename="match_banner.png")
    except Exception as e:
        logger.error(f"Banner generation failed: {e}")
        return None

# --- UI VIEWS ---

class LeaderboardView(discord.ui.View):
    def __init__(self, cog, interaction_user_id):
        super().__init__(timeout=None) 
        self.cog = cog
        self.interaction_user_id = interaction_user_id

    async def update_embed(self, interaction: discord.Interaction, game_slug: str):
        embed = await self.cog.generate_leaderboard_embed(interaction.guild, game_slug)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Val", style=discord.ButtonStyle.primary, custom_id="lb_btn_val")
    async def val_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "valorant")

    @discord.ui.button(label="RL", style=discord.ButtonStyle.success, custom_id="lb_btn_rl")
    async def rl_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "rl")

    @discord.ui.button(label="R6", style=discord.ButtonStyle.danger, custom_id="lb_btn_r6")
    async def r6_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "r6siege")

    @discord.ui.button(label="OW", style=discord.ButtonStyle.secondary, custom_id="lb_btn_ow")
    async def ow_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_embed(interaction, "ow")

class PredictionView(discord.ui.View):
    def __init__(self, match_id: str, team_a: dict, team_b: dict):
        super().__init__(timeout=None) 
        self.match_id = str(match_id)
        
        def get_label(name, acronym):
            label = f"Vote {acronym}" if acronym and len(name) > 15 else f"Vote {name}"
            return label[:MAX_BUTTON_LABEL_LENGTH]

        label_a = get_label(team_a.get('name', 'Team A'), team_a.get('acronym'))
        label_b = get_label(team_b.get('name', 'Team B'), team_b.get('acronym'))

        self.btn_a = discord.ui.Button(label=label_a, style=discord.ButtonStyle.primary, custom_id=f"vote_{match_id}_0")
        self.btn_a.callback = self.vote_team_a
        self.add_item(self.btn_a)

        self.btn_b = discord.ui.Button(label=label_b, style=discord.ButtonStyle.danger, custom_id=f"vote_{match_id}_1")
        self.btn_b.callback = self.vote_team_b
        self.add_item(self.btn_b)

    async def handle_vote(self, interaction: discord.Interaction, team_index: int):
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Error: Esports cog not found.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        match_id = str(self.match_id)
        team_name = "Selected Team"
        switched = False

        async with cog.data_lock:
            data = load_data_sync() 
            if match_id not in data["active_matches"]:
                await interaction.response.send_message("❌ This match is closed or invalid.", ephemeral=True)
                return
            
            match_info = data["active_matches"][match_id]

            start_dt = safe_parse_datetime(match_info.get('start_time'))
            if start_dt and datetime.datetime.now(datetime.timezone.utc) >= start_dt:
                await interaction.response.send_message("🔒 Voting is locked!", ephemeral=True)
                return

            current_vote = match_info["votes"].get(user_id)
            if current_vote == team_index:
                await interaction.response.send_message("⚠️ You already voted for this team!", ephemeral=True)
                return
            
            if current_vote is not None: switched = True
            
            data["active_matches"][match_id]["votes"][user_id] = team_index
            
            try:
                teams = match_info.get("teams", [])
                if len(teams) > team_index and team_index >= 0:
                    team_name = teams[team_index]["name"]
            except KeyError as e: 
                logger.warning(f"Missing key in team data for match {match_id}: {e}")
                team_name = "Selected Team"
            except Exception as e:
                logger.error(f"Error getting team name for match {match_id}: {e}")
                team_name = "Selected Team"

            save_data_sync(data)
        
        action = "switched your vote to" if switched else "voted for"
        await interaction.response.send_message(f"✅ You {action} **{team_name}**!", ephemeral=True)

    async def vote_team_a(self, interaction: discord.Interaction): await self.handle_vote(interaction, 0)
    async def vote_team_b(self, interaction: discord.Interaction): await self.handle_vote(interaction, 1)

class EmojiGuildSelect(discord.ui.Select):
    def __init__(self, bot, current_selected):
        self.bot = bot
        options = []
        for guild in bot.guilds[:25]:
            is_default = str(guild.id) in current_selected
            options.append(discord.SelectOption(
                label=guild.name[:100], value=str(guild.id),
                description=f"ID: {guild.id}", default=is_default
            ))
        super().__init__(placeholder="Select Storage Servers", min_values=0, max_values=len(options), options=options, row=1)

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Esports")
        selected_ids = self.values
        async with cog.data_lock:
            data = load_data_sync()
            data["emoji_storage_guilds"] = selected_ids
            save_data_sync(data)
        await interaction.response.send_message(f"Storage Updated! Saving to {len(selected_ids)} servers.", ephemeral=True)

class EsportsAdminView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None) 
        self.cog = cog
        data = load_data_sync()
        self.add_item(EmojiGuildSelect(cog.bot, data.get("emoji_storage_guilds", [])))

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text], placeholder="Select Feed Channel", row=0)
    async def select_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        async with self.cog.data_lock:
            data = load_data_sync()
            data["channel_id"] = channel.id
            save_data_sync(data)
        await interaction.response.send_message(f"Updates will appear in {channel.mention}.", ephemeral=True)

    @discord.ui.button(label="Cycle Test Post", style=discord.ButtonStyle.green, row=2)
    async def test_post(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.run_test(interaction, is_result=False)

    @discord.ui.button(label="Cycle Result Test", style=discord.ButtonStyle.blurple, row=2)
    async def test_result(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.run_test(interaction, is_result=True)

    @discord.ui.button(label="Sync Team Emojis", style=discord.ButtonStyle.primary, row=3)
    async def sync_emojis(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Sync Started...", ephemeral=True)
        count = await self.cog.manage_team_emojis(interaction)
        self.cog._update_emoji_cache()
        await interaction.followup.send(f"Sync Complete! Added {count} new emojis.")

    @discord.ui.button(label="Debug Strafe", style=discord.ButtonStyle.secondary, row=3)
    async def debug_strafe(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.debug_strafe(interaction)

    @discord.ui.button(label="Test Strafe Direct", style=discord.ButtonStyle.success, row=4)
    async def test_strafe_direct(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.test_strafe_direct(interaction)

    @discord.ui.button(label="Wipe Leaderboards", style=discord.ButtonStyle.danger, row=4)
    async def wipe_leaderboards(self, interaction: discord.Interaction, button: discord.ui.Button):
        confirm_view = discord.ui.View(timeout=30)
        async def confirm_cb(i: discord.Interaction):
            async with self.cog.data_lock:
                data = load_data_sync()
                data["leaderboards"] = {k: {} for k in GAMES.keys()}
                save_data_sync(data)
            await i.response.edit_message(content="Wiped.", view=None)

        btn = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.danger)
        btn.callback = confirm_cb
        confirm_view.add_item(btn)
        await interaction.response.send_message("Wipe all leaderboards?", view=confirm_view, ephemeral=True)


