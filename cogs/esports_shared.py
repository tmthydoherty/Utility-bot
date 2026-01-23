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

# --- REGION FILTERING (Whitelist approach) ---
# Only allow NA/EU regions - everything else gets filtered unless it's international
# These are checked as substrings in lowercase event names
ALLOWED_REGION_KEYWORDS = [
    # North America
    "north america",
    # Europe
    "europe", "emea",
]

# --- MAJOR LEAGUES BY GAME ---
# Only matches from these major circuits/leagues will be shown
# Valorant is strict (region-specific), others use excluded regions list for filtering
MAJOR_LEAGUE_KEYWORDS = {
    "valorant": [
        # Only NA (Americas) and EU (EMEA) VCT leagues - very specific
        "vct americas", "vct emea",
        "champions tour americas", "champions tour emea",
        "challengers na", "challengers emea",
        "ascension americas", "ascension emea",
        "game changers na", "game changers emea",
    ],
    "rl": [
        # Main RLCS circuit - region filtering handled separately
        "rlcs",
    ],
    "r6siege": [
        # Main R6 circuits
        "six invitational", "six major", "blast r6",
        "esl pro league", "pro league",
    ],
    "ow": [
        # OWL and OWCS
        "owl", "overwatch league",
        "owcs", "overwatch champions series",
        "world cup",
    ],
}

# Region keywords to EXCLUDE (catches regions not in whitelist)
# More comprehensive list with variations and sub-region formats
EXCLUDED_REGION_KEYWORDS = [
    # APAC / Pacific regions (including sub-regions with // format)
    "pacific", "apac", "asia", "asian",
    "pacific//north", "pacific//south", "pacific//east", "pacific//west",
    "north//east", "south//east",  # Common VCT sub-region formats
    "korea", "korean", "kr", "lck",
    "japan", "japanese", "jp", "ljl",
    "china", "chinese", "cn", "lpl",
    "sea", "southeast asia", "vcs",  # Vietnam
    "pcs",  # Pacific Championship Series
    "oce", "oceania", "lco",
    "india", "south asia",
    "mena",  # Middle East & North Africa (sometimes separate from EMEA)
    # Latin America / Brazil (separate from NA)
    "brazil", "brazilian", "br", "cblol",
    "latam", "latin america", "lla",
    "south america",
    # CIS (sometimes separate from EMEA)
    "cis", "lcl",
]

# International LAN keywords - these bypass ALL region filtering
# NOTE: "kickoff" removed - regional kickoffs (China, Pacific) are NOT international LANs
INTERNATIONAL_LAN_KEYWORDS = [
    "masters", "champions", "championship", "world",
    "major", "invitational", "grand final", "finals",
    "lock//in", "lockin", "lock-in",
    "gamers8", "iem", "blast premier",
    "six invitational", "six major",
    "all-star", "allstar",
]

# RLCS early round keywords - skip these matches (typically before top 16)
# RLCS starts with 32 teams in Swiss format, we only want top 16+ matches
RLCS_EARLY_ROUND_KEYWORDS = [
    "swiss", "swiss stage",
    "day 1", "day 2",  # Swiss days before top 16
    "round 1", "round 2", "round 3", "round 4", "round 5",  # Swiss rounds
    "round of 32", "ro32",
    "group stage", "group a", "group b", "group c", "group d",
    "open qualifier", "closed qualifier",
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


# --- LEADERBOARD ADMIN VIEWS ---

class GameSelectForWipeAll(discord.ui.Select):
    """Select dropdown for choosing which game's leaderboard to wipe."""
    def __init__(self):
        options = [
            discord.SelectOption(label="All Games", value="all", description="Wipe leaderboards for all games", emoji="🎮")
        ]
        for slug, name in GAMES.items():
            options.append(discord.SelectOption(label=name, value=slug, description=f"Wipe {name} leaderboard"))
        super().__init__(placeholder="Select game to wipe...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_game = self.values[0]
        game_display = "All Games" if self.values[0] == "all" else GAMES.get(self.values[0], self.values[0])
        self.view.confirm_btn.disabled = False
        self.view.confirm_btn.label = f"Confirm Wipe: {game_display}"
        await interaction.response.edit_message(view=self.view)


class WipeAllScoresView(discord.ui.View):
    """View for wiping all users' scores from a leaderboard."""
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog
        self.selected_game = None
        self.add_item(GameSelectForWipeAll())

        self.confirm_btn = discord.ui.Button(label="Select a game first", style=discord.ButtonStyle.danger, disabled=True, row=1)
        self.confirm_btn.callback = self.confirm_wipe
        self.add_item(self.confirm_btn)

    async def confirm_wipe(self, interaction: discord.Interaction):
        if not self.selected_game:
            await interaction.response.send_message("Please select a game first.", ephemeral=True)
            return

        async with self.cog.data_lock:
            data = load_data_sync()
            if self.selected_game == "all":
                data["leaderboards"] = {k: {} for k in GAMES.keys()}
                msg = "All leaderboards have been wiped."
            else:
                data["leaderboards"][self.selected_game] = {}
                msg = f"{GAMES.get(self.selected_game, self.selected_game)} leaderboard has been wiped."
            save_data_sync(data)

        await interaction.response.edit_message(content=f"✅ {msg}", view=None)


class UserIdModal(discord.ui.Modal):
    """Modal for entering a user ID or mention."""
    def __init__(self, action_type: str, cog):
        super().__init__(title=f"{action_type} - Enter User")
        self.cog = cog
        self.action_type = action_type

        self.user_input = discord.ui.TextInput(
            label="User ID or @mention",
            placeholder="Enter user ID (e.g., 123456789) or @mention",
            required=True,
            max_length=50
        )
        self.add_item(self.user_input)

    async def on_submit(self, interaction: discord.Interaction):
        user_input = self.user_input.value.strip()

        # Extract user ID from mention or raw ID
        user_id = None
        if user_input.startswith("<@") and user_input.endswith(">"):
            # Handle mention format <@123456> or <@!123456>
            user_id = user_input.replace("<@", "").replace("!", "").replace(">", "")
        else:
            user_id = user_input

        # Validate it's a number
        if not user_id.isdigit():
            await interaction.response.send_message("❌ Invalid user ID. Please enter a valid user ID or mention.", ephemeral=True)
            return

        # Check if user exists in guild
        member = interaction.guild.get_member(int(user_id))
        member_name = member.display_name if member else f"User {user_id}"

        if self.action_type == "Wipe User Score":
            view = WipeUserGameSelectView(self.cog, user_id, member_name)
            await interaction.response.send_message(
                f"Select game to wipe scores for **{member_name}**:",
                view=view,
                ephemeral=True
            )
        else:  # Modify Points
            modal = PointsInputModal(self.cog, user_id, member_name)
            await interaction.response.send_modal(modal)


class GameSelectForUser(discord.ui.Select):
    """Select dropdown for choosing which game when wiping a user's score."""
    def __init__(self, user_id: str, member_name: str):
        self.user_id = user_id
        self.member_name = member_name
        options = []
        for slug, name in GAMES.items():
            options.append(discord.SelectOption(label=name, value=slug, description=f"Wipe {name} score"))
        super().__init__(placeholder="Select game...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_game = self.values[0]
        game_display = GAMES.get(self.values[0], self.values[0])
        self.view.confirm_btn.disabled = False
        self.view.confirm_btn.label = f"Confirm Wipe: {game_display}"
        await interaction.response.edit_message(view=self.view)


class WipeUserGameSelectView(discord.ui.View):
    """View for selecting game when wiping a single user's score."""
    def __init__(self, cog, user_id: str, member_name: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id
        self.member_name = member_name
        self.selected_game = None
        self.add_item(GameSelectForUser(user_id, member_name))

        self.confirm_btn = discord.ui.Button(label="Select a game first", style=discord.ButtonStyle.danger, disabled=True, row=1)
        self.confirm_btn.callback = self.confirm_wipe
        self.add_item(self.confirm_btn)

    async def confirm_wipe(self, interaction: discord.Interaction):
        if not self.selected_game:
            await interaction.response.send_message("Please select a game first.", ephemeral=True)
            return

        async with self.cog.data_lock:
            data = load_data_sync()
            game_lb = data["leaderboards"].get(self.selected_game, {})

            if self.user_id in game_lb:
                del game_lb[self.user_id]
                data["leaderboards"][self.selected_game] = game_lb
                save_data_sync(data)
                msg = f"✅ Wiped **{self.member_name}**'s {GAMES.get(self.selected_game)} score."
            else:
                msg = f"⚠️ **{self.member_name}** has no score in {GAMES.get(self.selected_game)}."

        await interaction.response.edit_message(content=msg, view=None)


class PointsInputModal(discord.ui.Modal):
    """Modal for entering points to add/remove."""
    def __init__(self, cog, user_id: str, member_name: str):
        super().__init__(title=f"Modify Points - {member_name[:30]}")
        self.cog = cog
        self.user_id = user_id
        self.member_name = member_name

        self.wins_input = discord.ui.TextInput(
            label="Wins to add (use negative to remove)",
            placeholder="e.g., 5 or -3",
            required=True,
            max_length=10
        )
        self.add_item(self.wins_input)

        self.losses_input = discord.ui.TextInput(
            label="Losses to add (use negative to remove)",
            placeholder="e.g., 2 or -1",
            required=True,
            max_length=10
        )
        self.add_item(self.losses_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            wins_delta = int(self.wins_input.value.strip())
            losses_delta = int(self.losses_input.value.strip())
        except ValueError:
            await interaction.response.send_message("❌ Invalid input. Please enter numbers only.", ephemeral=True)
            return

        view = ModifyPointsGameSelectView(self.cog, self.user_id, self.member_name, wins_delta, losses_delta)
        await interaction.response.send_message(
            f"Select game to modify points for **{self.member_name}**:\n"
            f"Wins: {'+' if wins_delta >= 0 else ''}{wins_delta}, Losses: {'+' if losses_delta >= 0 else ''}{losses_delta}",
            view=view,
            ephemeral=True
        )


class GameSelectForModify(discord.ui.Select):
    """Select dropdown for choosing which game when modifying points."""
    def __init__(self):
        options = []
        for slug, name in GAMES.items():
            options.append(discord.SelectOption(label=name, value=slug, description=f"Modify {name} points"))
        super().__init__(placeholder="Select game...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_game = self.values[0]
        game_display = GAMES.get(self.values[0], self.values[0])
        self.view.confirm_btn.disabled = False
        self.view.confirm_btn.label = f"Confirm: {game_display}"
        await interaction.response.edit_message(view=self.view)


class ModifyPointsGameSelectView(discord.ui.View):
    """View for selecting game when modifying a user's points."""
    def __init__(self, cog, user_id: str, member_name: str, wins_delta: int, losses_delta: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id
        self.member_name = member_name
        self.wins_delta = wins_delta
        self.losses_delta = losses_delta
        self.selected_game = None
        self.add_item(GameSelectForModify())

        self.confirm_btn = discord.ui.Button(label="Select a game first", style=discord.ButtonStyle.primary, disabled=True, row=1)
        self.confirm_btn.callback = self.confirm_modify
        self.add_item(self.confirm_btn)

    async def confirm_modify(self, interaction: discord.Interaction):
        if not self.selected_game:
            await interaction.response.send_message("Please select a game first.", ephemeral=True)
            return

        async with self.cog.data_lock:
            data = load_data_sync()
            game_lb = data["leaderboards"].get(self.selected_game, {})

            if self.user_id not in game_lb:
                game_lb[self.user_id] = {"wins": 0, "losses": 0, "streak": 0}

            # Apply deltas
            game_lb[self.user_id]["wins"] = max(0, game_lb[self.user_id].get("wins", 0) + self.wins_delta)
            game_lb[self.user_id]["losses"] = max(0, game_lb[self.user_id].get("losses", 0) + self.losses_delta)

            # Reset streak if removing wins
            if self.wins_delta < 0:
                game_lb[self.user_id]["streak"] = 0

            new_wins = game_lb[self.user_id]["wins"]
            new_losses = game_lb[self.user_id]["losses"]

            data["leaderboards"][self.selected_game] = game_lb
            save_data_sync(data)

        game_name = GAMES.get(self.selected_game, self.selected_game)
        await interaction.response.edit_message(
            content=f"✅ Updated **{self.member_name}**'s {game_name} score:\n"
                    f"Wins: {new_wins}, Losses: {new_losses}",
            view=None
        )

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

    @discord.ui.button(label="Disable Updates", style=discord.ButtonStyle.secondary, row=2)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.data_lock:
            data = load_data_sync()
            current_channel = data.get("channel_id")
            if current_channel is None:
                await interaction.response.send_message("Updates are already disabled (no channel set).", ephemeral=True)
                return
            data["channel_id"] = None
            save_data_sync(data)
        await interaction.response.send_message("Updates disabled. No matches will be posted until a channel is selected.", ephemeral=True)

    @discord.ui.button(label="Sync Emojis", style=discord.ButtonStyle.primary, row=2)
    async def sync_emojis(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Sync Started...", ephemeral=True)
        count = await self.cog.manage_team_emojis(interaction)
        self.cog._update_emoji_cache()
        await interaction.followup.send(f"Sync Complete! Added {count} new emojis.")

    @discord.ui.button(label="Test Post", style=discord.ButtonStyle.green, row=2)
    async def test_post(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.run_test(interaction, is_result=False)

    @discord.ui.button(label="Test Result", style=discord.ButtonStyle.blurple, row=2)
    async def test_result(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.run_test(interaction, is_result=True)

    @discord.ui.button(label="Debug Strafe", style=discord.ButtonStyle.secondary, row=3)
    async def debug_strafe(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.debug_strafe(interaction)

    @discord.ui.button(label="Test Strafe", style=discord.ButtonStyle.success, row=3)
    async def test_strafe_direct(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.test_strafe_direct(interaction)

    # --- LEADERBOARD ADMIN BUTTONS ---

    @discord.ui.button(label="Wipe All Scores", style=discord.ButtonStyle.danger, row=4)
    async def wipe_all_scores(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Wipe all users' scores for a specific game or all games."""
        view = WipeAllScoresView(self.cog)
        await interaction.response.send_message(
            "Select which game's leaderboard to wipe:",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Wipe User Score", style=discord.ButtonStyle.danger, row=4)
    async def wipe_user_score(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Wipe a single user's score from a game's leaderboard."""
        modal = UserIdModal("Wipe User Score", self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Modify Points", style=discord.ButtonStyle.primary, row=4)
    async def modify_points(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Add or remove points for a specific user."""
        modal = UserIdModal("Modify Points", self.cog)
        await interaction.response.send_modal(modal)


