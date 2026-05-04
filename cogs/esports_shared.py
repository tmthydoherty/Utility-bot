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
    "ow": "https://i.postimg.cc/Hkh4bQQ1/Overwatch2-Primary-DKBKGD.png"
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

# Per-game tier overrides. OWCS on PandaScore is often labeled tier B, so we
# loosen the tier filter for OW to avoid dropping the main circuit.
GAME_ALLOWED_TIERS = {
    "ow": ["s", "a", "b"],
}

# Per-game region keyword extras. These are treated as allowed regions for the
# given game AND any matching entries in EXCLUDED_REGION_KEYWORDS are suppressed
# (including sub-region strings that contain the extra as a substring).
GAME_EXTRA_ALLOWED_REGIONS = {
    # VCT Pacific is a main VCT circuit and should be shown alongside Americas/EMEA
    "valorant": ["pacific"],
}

# Per-game blocked region keywords. If any of these appear in an event name,
# the match is filtered out for that game (regardless of other allow rules).
GAME_BLOCKED_REGIONS = {
    # User prefers OW posts to be NA-only
    "ow": ["europe", "emea"],
}

# --- REGION FILTERING (Whitelist approach) ---
# Only allow NA/EU regions - everything else gets filtered unless it's international
# These are checked as substrings in lowercase event names
ALLOWED_REGION_KEYWORDS = [
    # North America
    "north america", "na", "americas",
    # Europe (including EU/MENA combined regions used by R6 Siege etc.)
    "europe", "emea", "eu",
]

# --- MAJOR LEAGUES BY GAME ---
# Only matches from these major circuits/leagues will be shown
# Valorant is strict (region-specific), others use excluded regions list for filtering
MAJOR_LEAGUE_KEYWORDS = {
    "valorant": [
        # VCT regional leagues
        "vct americas", "vct emea", "vct",
        "champions tour americas", "champions tour emea", "champions tour",
        "challengers na", "challengers emea",
        "ascension americas", "ascension emea",
        "game changers na", "game changers emea",
        # Major events
        "kickoff", "masters", "champions",
    ],
    "rl": [
        # RLCS circuit + broader keywords for changing league names
        "rlcs", "league", "major", "championship", "world", "kickoff",
    ],
    "r6siege": [
        # R6 circuits — BLAST era + legacy ESL naming
        "six invitational", "six major", "blast r6", "blast",
        "esl pro league", "pro league",
        "league", "major", "kickoff",
    ],
    "ow": [
        # OWL, OWCS, and broader keywords
        "owl", "overwatch league",
        "owcs", "overwatch champions series", "champions series",
        "world cup", "kickoff", "league",
    ],
}

# Region keywords to EXCLUDE (catches regions not in whitelist)
# More comprehensive list with variations and sub-region formats
EXCLUDED_REGION_KEYWORDS = [
    # APAC / Pacific regions (including sub-regions with // format)
    "pacific", "apac", "asia", "asian",
    "pacific//north", "pacific//south", "pacific//east", "pacific//west",
    "korea", "korean", "kr", "lck",
    "japan", "japanese", "jp", "ljl",
    "china", "chinese", "cn", "lpl",
    "sea", "southeast asia", "vcs",  # Vietnam
    "pcs",  # Pacific Championship Series
    "oce", "oceania", "lco",
    "india", "south asia",
    # Note: "mena" removed — R6 Siege and Valorant use "EU/MENA" or "EMEA" as a combined region
    # Latin America / Brazil (separate from NA)
    "brazil", "brazilian", "br", "cblol",
    "latam", "latin america", "lla",
    "south america",
    # CIS (sometimes separate from EMEA)
    "cis", "lcl",
]

# VCT Challengers sub-regional leagues to ALWAYS exclude (even if they contain "emea")
# These are lower-tier regional leagues, not the main VCT EMEA/Americas circuits
VCT_CHALLENGERS_SUBREGION_KEYWORDS = [
    # EMEA sub-regions (Challengers split format)
    "north//east", "south//east", "north//west", "south//west",
    "challengers east", "challengers north", "challengers south", "challengers west",
    "challengers northern europe", "challengers southern europe",
    "challengers eastern europe", "challengers western europe",
    "challengers france", "challengers dach", "challengers turkey", "challengers spain",
    "challengers italy", "challengers portugal", "challengers benelux", "challengers poland",
    "challengers nordics", "challengers cis", "challengers mena",
    # Americas sub-regions
    "challengers brazil", "challengers latam", "challengers lan", "challengers las",
    "challengers argentina", "challengers chile", "challengers mexico",
    # Pacific sub-regions (shouldn't match anyway but being explicit)
    "challengers japan", "challengers korea", "challengers sea", "challengers oceania",
    "challengers indonesia", "challengers thailand", "challengers philippines",
    "challengers hong kong", "challengers taiwan",
]

# International LAN keywords - these bypass ALL region filtering
# "kickoff" re-added: non-NA/EU kickoffs are already caught by EXCLUDED_REGION_KEYWORDS,
# so this only lets through NA/EU kickoffs which we want to show
INTERNATIONAL_LAN_KEYWORDS = [
    "masters", "champions", "championship", "world",
    "major", "invitational", "grand final", "finals",
    "kickoff", "playoffs",
    "lock//in", "lockin", "lock-in",
    "gamers8", "iem", "blast premier",
    "six invitational", "six major",
    "all-star", "allstar",
]

# RLCS early round keywords - skip these matches to reduce spam
# League Play has 16 teams per region across 3 days (Fri/Sat/Sun)
# Only filter day 1 (Friday) which has the most matches; show day 2-3 for playoffs progression
RLCS_EARLY_ROUND_KEYWORDS = [
    "day 1",  # Filter first day of League Play (8 matches) to reduce spam
    # Note: "swiss"/"swiss stage" removed - RLCS Regional Opens use Swiss format for ALL 3 days,
    # so this was blocking Days 2-3 as well. "day 1" already handles Day 1 filtering.
    "round of 32", "ro32",
    "group stage", "group a", "group b", "group c", "group d",
    # Note: "open qualifier"/"closed qualifier" intentionally NOT filtered — RLCS
    # now names League Play days as "Open Qualifier N" (tech qualifiers for LANs),
    # which are real league-play matches we want to post.
]

# Additional keywords to filter for RLCS LAN events (Majors/Worlds)
# LANs have Swiss stage spanning days 1-2, so we filter more aggressively
RLCS_LAN_EXTRA_KEYWORDS = ["day 2"]

# Keywords that identify an RLCS LAN event (Major, World Championship, etc.)
RLCS_LAN_IDENTIFIERS = ["major", "world", "championship", "lan", "grand final"]

# Rocket League filter (top-8 playoff bracket only).
# Matched against match.tournament.name ONLY (not league/serie), because PandaScore
# splits each stage of an RLCS event (Swiss/Groups/Playoffs) into its own tournament
# entity. Keying off tournament.name is immune to circuit-name drift (e.g. the word
# "Major" appearing in league/serie names and causing false LAN classification).
RL_TOURNAMENT_BLACKLIST = [
    "group", "groups", "group stage",
    "group a", "group b", "group c", "group d", "group e", "group f",
    "swiss", "swiss stage",
    "day 1", "day 2", "day 3",
    "league play",
    "qualifier", "open qualifier", "closed qualifier",
    "round of 32", "ro32", "round of 16", "ro16",
    "play-in", "play in",
]
RL_TOURNAMENT_WHITELIST = [
    "playoff", "playoffs",
    "bracket", "upper bracket", "lower bracket",
    "knockout",
    "quarter", "quarterfinal", "quarter-final", "quarter final",
    "semi", "semifinal", "semi-final", "semi final",
    "final", "finals", "grand final",
    "top 8", "top8",
    "championship sunday",
]

LIQUIPEDIA_GAME_SLUGS = {
    "valorant": "valorant",
    "rl": "rocketleague",
    "r6siege": "rainbowsix",
    "ow": "overwatch",
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
    if "match_history" not in data: data["match_history"] = {}
    if "map_data_cache" not in data: data["map_data_cache"] = {}
    if "game_vote_emojis" not in data: data["game_vote_emojis"] = {}

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

def get_team_button_emoji(team_data: dict) -> Optional[discord.PartialEmoji]:
    """Resolve the custom emoji for a team's vote button.

    Checks the emoji_map for the team's acronym/name. Returns a PartialEmoji
    for custom Discord emojis (usable on buttons), or None if only a flag/fallback
    is available (flag shortcodes like :flag_us: aren't valid button emojis).
    """
    data = load_data_sync()
    emoji_map = data.get("emoji_map", {})

    # Check acronym first
    acronym = team_data.get('acronym')
    if acronym:
        for key in (acronym, acronym.upper()):
            if key in emoji_map:
                try:
                    return discord.PartialEmoji.from_str(str(emoji_map[key]))
                except Exception:
                    pass

    # Check name-based keys
    name = team_data.get('name', '')
    if name:
        key_short = "".join(c for c in name.split(' ')[0].upper() if c.isalnum())
        if key_short in emoji_map:
            try:
                return discord.PartialEmoji.from_str(str(emoji_map[key_short]))
            except Exception:
                pass
        key_long = "".join(c for c in name.upper() if c.isalnum())
        if key_long in emoji_map:
            try:
                return discord.PartialEmoji.from_str(str(emoji_map[key_long]))
            except Exception:
                pass

    return None


def get_game_vote_emoji(game_slug: str, bot=None) -> Optional[discord.PartialEmoji]:
    """Resolve the custom vote-button emoji for a game, if configured."""
    data = load_data_sync()
    emoji_str = data.get("game_vote_emojis", {}).get(game_slug)
    if not emoji_str:
        return None
    # Try parsing as a custom emoji string like <:name:123456>
    try:
        return discord.PartialEmoji.from_str(emoji_str)
    except Exception:
        return None

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

        emoji_a = get_team_button_emoji(team_a)
        emoji_b = get_team_button_emoji(team_b)
        self.btn_a = discord.ui.Button(label=label_a, style=discord.ButtonStyle.primary, custom_id=f"vote_{match_id}_0", emoji=emoji_a)
        self.btn_a.callback = self.vote_team_a
        self.add_item(self.btn_a)

        self.btn_b = discord.ui.Button(label=label_b, style=discord.ButtonStyle.danger, custom_id=f"vote_{match_id}_1", emoji=emoji_b)
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
        await interaction.response.send_message(f"✓ You {action} **{team_name}**!", ephemeral=True)

    async def vote_team_a(self, interaction: discord.Interaction): await self.handle_vote(interaction, 0)
    async def vote_team_b(self, interaction: discord.Interaction): await self.handle_vote(interaction, 1)


class VoteRevealView(discord.ui.View):
    """Green 'Vote' button on the minimal upcoming match embed in the channel.
    Clicking it reveals the full embed + vote buttons in an ephemeral message."""

    def __init__(self, match_id: str, game_slug: str):
        super().__init__(timeout=None)
        self.match_id = str(match_id)
        self.game_slug = game_slug

        emoji = get_game_vote_emoji(game_slug)
        btn = discord.ui.Button(
            label="Vote", style=discord.ButtonStyle.success,
            custom_id=f"vote_reveal_{match_id}",
            emoji=emoji
        )
        btn.callback = self.reveal_vote
        self.add_item(btn)

        details_btn = discord.ui.Button(
            label="Details", style=discord.ButtonStyle.secondary,
            custom_id=f"match_details_{match_id}"
        )
        details_btn.callback = self.show_details
        self.add_item(details_btn)

    async def show_details(self, interaction: discord.Interaction):
        """Show full match embed ephemerally without vote buttons."""
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        async with cog.data_lock:
            data = load_data_sync()
            match_info = data.get("active_matches", {}).get(self.match_id)

        if not match_info:
            await interaction.response.send_message("❌ Match details no longer available.", ephemeral=True)
            return

        teams = match_info.get("teams", [])
        if len(teams) < 2:
            await interaction.response.send_message("❌ Match data incomplete.", ephemeral=True)
            return

        match_details = await cog.get_pandascore_data(f"/matches/{self.match_id}")
        if not match_details:
            match_details = {"begin_at": match_info.get("start_time"), "status": "not_started",
                             "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

        banner = await cog.generate_banner(
            teams[0].get("image_url"), teams[1].get("image_url"),
            GAME_LOGOS.get(self.game_slug), self.game_slug
        )
        embed = cog.build_match_embed(
            self.game_slug, GAMES.get(self.game_slug), match_details,
            teams[0], teams[1], match_info.get("votes", {}),
            match_info.get("stream_url"), banner is not None
        )

        if banner:
            await interaction.response.send_message(embed=embed, file=banner, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def reveal_vote(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        user_id = str(interaction.user.id)

        async with cog.data_lock:
            data = load_data_sync()
            active = data.get("active_matches", {})

        # Build vote queue: all open matches for this game the user hasn't voted on
        now = datetime.datetime.now(datetime.timezone.utc)
        queue = []
        for mid, info in active.items():
            if info.get("game_slug") != self.game_slug:
                continue
            start_dt = safe_parse_datetime(info.get("start_time"))
            if start_dt and now >= start_dt:
                continue  # voting locked
            if user_id in info.get("votes", {}):
                continue  # already voted
            queue.append((mid, info, start_dt or now))

        # Sort by start_time, but put the clicked match first
        queue.sort(key=lambda x: x[2])
        # Move the clicked match to front if it's in the queue
        clicked_idx = next((i for i, (mid, *_) in enumerate(queue) if mid == self.match_id), None)
        if clicked_idx is not None and clicked_idx > 0:
            queue.insert(0, queue.pop(clicked_idx))

        if not queue:
            await interaction.response.send_message("✓ You've already voted on all open matches for this game!", ephemeral=True)
            return

        # Show first match in the queue
        first_mid, first_info, _ = queue[0]
        teams = first_info.get("teams", [])
        if len(teams) < 2:
            await interaction.response.send_message("❌ Match data incomplete.", ephemeral=True)
            return

        # Build full embed with banner
        match_details = await cog.get_pandascore_data(f"/matches/{first_mid}")
        if not match_details:
            # Fallback: build embed from stored data
            match_details = {"begin_at": first_info.get("start_time"), "status": "not_started",
                             "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

        banner = await cog.generate_banner(
            teams[0].get("image_url"), teams[1].get("image_url"),
            GAME_LOGOS.get(self.game_slug), self.game_slug
        )
        embed = cog.build_match_embed(
            self.game_slug, GAMES.get(self.game_slug), match_details,
            teams[0], teams[1], first_info.get("votes", {}),
            first_info.get("stream_url"), banner is not None
        )

        # Create the cycling vote view
        remaining_queue = [(mid, info) for mid, info, _ in queue[1:]]
        vote_view = VoteCycleView(first_mid, teams[0], teams[1], self.game_slug, remaining_queue, user_id)

        if banner:
            await interaction.response.send_message(embed=embed, file=banner, view=vote_view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=vote_view, ephemeral=True)


class VoteCycleView(discord.ui.View):
    """Vote buttons shown in the ephemeral message. After voting, cycles to next unvoted match."""

    def __init__(self, match_id: str, team_a: dict, team_b: dict, game_slug: str,
                 remaining_queue: list, user_id: str):
        super().__init__(timeout=300)  # 5 min timeout for ephemeral
        self.match_id = str(match_id)
        self.game_slug = game_slug
        self.remaining_queue = remaining_queue  # list of (mid, info) tuples
        self.user_id = user_id
        self.session_votes = []  # list of (team_name, match_display) for summary

        def get_label(name, acronym):
            label = f"Vote {acronym}" if acronym and len(name) > 15 else f"Vote {name}"
            return label[:MAX_BUTTON_LABEL_LENGTH]

        label_a = get_label(team_a.get('name', 'Team A'), team_a.get('acronym'))
        label_b = get_label(team_b.get('name', 'Team B'), team_b.get('acronym'))

        emoji_a = get_team_button_emoji(team_a)
        emoji_b = get_team_button_emoji(team_b)
        self.btn_a = discord.ui.Button(label=label_a, style=discord.ButtonStyle.primary, custom_id=f"vcycle_{match_id}_0_{secrets.token_hex(4)}", emoji=emoji_a)
        self.btn_a.callback = self.vote_a
        self.add_item(self.btn_a)

        self.btn_b = discord.ui.Button(label=label_b, style=discord.ButtonStyle.danger, custom_id=f"vcycle_{match_id}_1_{secrets.token_hex(4)}", emoji=emoji_b)
        self.btn_b.callback = self.vote_b
        self.add_item(self.btn_b)

    async def _handle_vote(self, interaction: discord.Interaction, team_index: int):
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        match_id = self.match_id
        team_name = "Selected Team"

        async with cog.data_lock:
            data = load_data_sync()
            if match_id not in data["active_matches"]:
                await interaction.response.send_message("❌ This match is closed.", ephemeral=True)
                return

            match_info = data["active_matches"][match_id]

            start_dt = safe_parse_datetime(match_info.get("start_time"))
            if start_dt and datetime.datetime.now(datetime.timezone.utc) >= start_dt:
                await interaction.response.send_message("🔒 Voting is locked for this match!", ephemeral=True)
                return

            data["active_matches"][match_id]["votes"][self.user_id] = team_index
            teams = match_info.get("teams", [])
            if len(teams) > team_index >= 0:
                team_name = teams[team_index]["name"]

            save_data_sync(data)

        # Record this vote in session
        team_a_name = teams[0]["name"] if len(teams) >= 1 else "?"
        team_b_name = teams[1]["name"] if len(teams) >= 2 else "?"
        self.session_votes.append((team_name, team_a_name, team_b_name, team_index))

        # Cycle to next match or show summary
        if self.remaining_queue:
            next_mid, next_info = self.remaining_queue.pop(0)
            next_teams = next_info.get("teams", [])

            # Re-check that user hasn't voted on this one in the meantime
            async with cog.data_lock:
                fresh = load_data_sync()
                next_match = fresh.get("active_matches", {}).get(next_mid)

            if not next_match or len(next_teams) < 2:
                # Skip invalid, try next
                return await self._show_summary(interaction)

            if self.user_id in next_match.get("votes", {}):
                # Already voted, skip to next
                if self.remaining_queue:
                    return await self._handle_vote_skip(interaction)
                else:
                    return await self._show_summary(interaction)

            # Build embed for next match
            match_details = await cog.get_pandascore_data(f"/matches/{next_mid}")
            if not match_details:
                match_details = {"begin_at": next_info.get("start_time"), "status": "not_started",
                                 "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

            banner = await cog.generate_banner(
                next_teams[0].get("image_url"), next_teams[1].get("image_url"),
                GAME_LOGOS.get(self.game_slug), self.game_slug
            )
            embed = cog.build_match_embed(
                self.game_slug, GAMES.get(self.game_slug), match_details,
                next_teams[0], next_teams[1], next_match.get("votes", {}),
                next_info.get("stream_url"), banner is not None
            )

            # Update this view's state for the next match
            self.match_id = next_mid

            # Create new view for next match buttons
            new_view = VoteCycleView(next_mid, next_teams[0], next_teams[1], self.game_slug,
                                     self.remaining_queue, self.user_id)
            new_view.session_votes = self.session_votes

            if banner:
                await interaction.response.edit_message(embed=embed, attachments=[banner], view=new_view)
            else:
                await interaction.response.edit_message(embed=embed, attachments=[], view=new_view)
        else:
            await self._show_summary(interaction)

    async def _handle_vote_skip(self, interaction: discord.Interaction):
        """Skip matches already voted on and continue cycling."""
        while self.remaining_queue:
            next_mid, next_info = self.remaining_queue.pop(0)
            cog = interaction.client.get_cog("Esports")
            async with cog.data_lock:
                fresh = load_data_sync()
                next_match = fresh.get("active_matches", {}).get(next_mid)
            if next_match and self.user_id not in next_match.get("votes", {}):
                next_teams = next_info.get("teams", [])
                if len(next_teams) >= 2:
                    match_details = await cog.get_pandascore_data(f"/matches/{next_mid}")
                    if not match_details:
                        match_details = {"begin_at": next_info.get("start_time"), "status": "not_started",
                                         "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}
                    banner = await cog.generate_banner(
                        next_teams[0].get("image_url"), next_teams[1].get("image_url"),
                        GAME_LOGOS.get(self.game_slug), self.game_slug
                    )
                    embed = cog.build_match_embed(
                        self.game_slug, GAMES.get(self.game_slug), match_details,
                        next_teams[0], next_teams[1], next_match.get("votes", {}),
                        next_info.get("stream_url"), banner is not None
                    )
                    self.match_id = next_mid
                    new_view = VoteCycleView(next_mid, next_teams[0], next_teams[1], self.game_slug,
                                             self.remaining_queue, self.user_id)
                    new_view.session_votes = self.session_votes
                    if banner:
                        await interaction.response.edit_message(embed=embed, attachments=[banner], view=new_view)
                    else:
                        await interaction.response.edit_message(embed=embed, attachments=[], view=new_view)
                    return
        await self._show_summary(interaction)

    async def _show_summary(self, interaction: discord.Interaction):
        """Show vote session summary."""
        count = len(self.session_votes)
        lines = [f"✓ {count} vote{'s' if count != 1 else ''} recorded!\n"]
        for team_name, team_a, team_b, idx in self.session_votes:
            if idx == 0:
                lines.append(f"• **✓{team_a}** vs {team_b}")
            else:
                lines.append(f"• {team_a} vs **✓{team_b}**")
        summary = "\n".join(lines)
        try:
            await interaction.response.edit_message(content=summary, embed=None, attachments=[], view=None)
        except discord.InteractionResponded:
            await interaction.edit_original_response(content=summary, embed=None, attachments=[], view=None)

    async def vote_a(self, interaction: discord.Interaction): await self._handle_vote(interaction, 0)
    async def vote_b(self, interaction: discord.Interaction): await self._handle_vote(interaction, 1)


class BatchVoteRevealView(discord.ui.View):
    """Single 'Vote' button for a batch of upcoming matches. Opens the VoteCycleView with all matches queued."""

    def __init__(self, match_ids: list, game_slug: str):
        super().__init__(timeout=None)
        self.match_ids = [str(mid) for mid in match_ids]
        self.game_slug = game_slug

        emoji = get_game_vote_emoji(game_slug)
        btn = discord.ui.Button(
            label="Vote", style=discord.ButtonStyle.success,
            custom_id=f"batch_vote_reveal_{'_'.join(self.match_ids[:5])}",
            emoji=emoji
        )
        btn.callback = self.reveal_vote
        self.add_item(btn)

    async def reveal_vote(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        user_id = str(interaction.user.id)

        async with cog.data_lock:
            data = load_data_sync()
            active = data.get("active_matches", {})

        # Build vote queue from batch match IDs
        now = datetime.datetime.now(datetime.timezone.utc)
        queue = []
        for mid in self.match_ids:
            info = active.get(mid)
            if not info or info.get("game_slug") != self.game_slug:
                continue
            start_dt = safe_parse_datetime(info.get("start_time"))
            if start_dt and now >= start_dt:
                continue  # voting locked
            if user_id in info.get("votes", {}):
                continue  # already voted
            queue.append((mid, info, start_dt or now))

        # Also include any other open matches for this game not in the batch
        for mid, info in active.items():
            if mid in self.match_ids:
                continue
            if info.get("game_slug") != self.game_slug:
                continue
            start_dt = safe_parse_datetime(info.get("start_time"))
            if start_dt and now >= start_dt:
                continue
            if user_id in info.get("votes", {}):
                continue
            queue.append((mid, info, start_dt or now))

        queue.sort(key=lambda x: x[2])

        if not queue:
            await interaction.response.send_message("✓ You've already voted on all open matches for this game!", ephemeral=True)
            return

        first_mid, first_info, _ = queue[0]
        teams = first_info.get("teams", [])
        if len(teams) < 2:
            await interaction.response.send_message("❌ Match data incomplete.", ephemeral=True)
            return

        match_details = await cog.get_pandascore_data(f"/matches/{first_mid}")
        if not match_details:
            match_details = {"begin_at": first_info.get("start_time"), "status": "not_started",
                             "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

        banner = await cog.generate_banner(
            teams[0].get("image_url"), teams[1].get("image_url"),
            GAME_LOGOS.get(self.game_slug), self.game_slug
        )
        embed = cog.build_match_embed(
            self.game_slug, GAMES.get(self.game_slug), match_details,
            teams[0], teams[1], first_info.get("votes", {}),
            first_info.get("stream_url"), banner is not None
        )

        remaining = [(mid, info) for mid, info, _ in queue[1:]]
        view = VoteCycleView(first_mid, teams[0], teams[1], self.game_slug, remaining, user_id)

        if banner:
            await interaction.response.send_message(embed=embed, file=banner, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class ResultDetailsView(discord.ui.View):
    """Grey 'Details' button on the minimal result embed. Reveals full result with event info + match history."""

    def __init__(self, match_id: str, game_slug: str):
        super().__init__(timeout=None)
        self.match_id = str(match_id)
        self.game_slug = game_slug

        btn = discord.ui.Button(
            label="Details", style=discord.ButtonStyle.secondary,
            custom_id=f"result_details_{match_id}"
        )
        btn.callback = self.show_details
        self.add_item(btn)

    async def show_details(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        async with cog.data_lock:
            data = load_data_sync()
            history = data.get("match_history", {}).get(self.match_id)
            map_cache = data.get("map_data_cache", {}).get(self.match_id)

        if not history:
            await interaction.response.send_message("❌ Match details no longer available.", ephemeral=True)
            return

        teams = history.get("teams", [])
        if len(teams) < 2:
            await interaction.response.send_message("❌ Match data incomplete.", ephemeral=True)
            return

        # Reconstruct match_details from stored history
        extra = history.get("match_details_extra", {})
        match_details = {
            "results": history.get("results", []),
            "number_of_games": extra.get("number_of_games"),
            "league": extra.get("league", {}),
            "serie": extra.get("serie", {}),
            "tournament": extra.get("tournament", {}),
            "official_stream_url": extra.get("official_stream_url"),
        }

        # Use cached map data if available
        map_data = map_cache.get("maps") if map_cache else None

        # Build team dicts with required fields
        team_a = {"name": teams[0]["name"], "id": teams[0].get("id"),
                  "flag": teams[0].get("flag"), "acronym": teams[0].get("acronym")}
        team_b = {"name": teams[1]["name"], "id": teams[1].get("id"),
                  "flag": teams[1].get("flag"), "acronym": teams[1].get("acronym")}

        embed = await cog.build_full_result_embed(
            self.game_slug, match_details, team_a, team_b,
            history.get("winner_idx", 0), map_data=map_data
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


class UnifiedUpcomingView(discord.ui.View):
    """Single view for the unified upcoming match embed per game.
    Vote button opens VoteCycleView, Details button shows match details with dropdown."""

    def __init__(self, game_slug: str):
        super().__init__(timeout=None)
        self.game_slug = game_slug

        emoji = get_game_vote_emoji(game_slug)
        vote_btn = discord.ui.Button(
            label="Vote", style=discord.ButtonStyle.success,
            custom_id=f"unified_vote_{game_slug}",
            emoji=emoji
        )
        vote_btn.callback = self.reveal_vote
        self.add_item(vote_btn)

        details_btn = discord.ui.Button(
            label="Details", style=discord.ButtonStyle.secondary,
            custom_id=f"unified_details_{game_slug}"
        )
        details_btn.callback = self.show_details
        self.add_item(details_btn)

    async def show_details(self, interaction: discord.Interaction):
        """Show earliest upcoming match details ephemerally, with dropdown if multiple."""
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        async with cog.data_lock:
            data = load_data_sync()
            active = data.get("active_matches", {})

        # Gather all active matches for this game
        now = datetime.datetime.now(datetime.timezone.utc)
        matches = []
        for mid, info in active.items():
            if info.get("game_slug") != self.game_slug:
                continue
            if info.get("is_test"):
                continue
            matches.append((mid, info))

        if not matches:
            await interaction.response.send_message("❌ No match details available.", ephemeral=True)
            return

        # Sort by start time (earliest first)
        matches.sort(key=lambda x: safe_parse_datetime(x[1].get("start_time")) or now)

        # Show first match details
        first_mid, first_info = matches[0]
        teams = first_info.get("teams", [])
        if len(teams) < 2:
            await interaction.response.send_message("❌ Match data incomplete.", ephemeral=True)
            return

        match_details = await cog.get_pandascore_data(f"/matches/{first_mid}")
        if not match_details:
            match_details = {"begin_at": first_info.get("start_time"), "status": "not_started",
                             "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

        banner = await cog.generate_banner(
            teams[0].get("image_url"), teams[1].get("image_url"),
            GAME_LOGOS.get(self.game_slug), self.game_slug
        )
        embed = cog.build_match_embed(
            self.game_slug, GAMES.get(self.game_slug), match_details,
            teams[0], teams[1], first_info.get("votes", {}),
            first_info.get("stream_url"), banner is not None
        )

        # Build view with dropdown if multiple matches
        view = None
        if len(matches) > 1:
            view = UpcomingDetailsDropdownView(self.game_slug, matches, first_mid)

        kwargs = {"embed": embed, "ephemeral": True}
        if banner:
            kwargs["file"] = banner
        if view:
            kwargs["view"] = view
        await interaction.response.send_message(**kwargs)

    async def reveal_vote(self, interaction: discord.Interaction):
        """Open VoteCycleView for all unvoted matches of this game."""
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        user_id = str(interaction.user.id)

        async with cog.data_lock:
            data = load_data_sync()
            active = data.get("active_matches", {})

        now = datetime.datetime.now(datetime.timezone.utc)
        queue = []
        for mid, info in active.items():
            if info.get("game_slug") != self.game_slug:
                continue
            start_dt = safe_parse_datetime(info.get("start_time"))
            if start_dt and now >= start_dt:
                continue
            if user_id in info.get("votes", {}):
                continue
            queue.append((mid, info, start_dt or now))

        queue.sort(key=lambda x: x[2])

        if not queue:
            await interaction.response.send_message("✓ You've already voted on all open matches for this game!", ephemeral=True)
            return

        first_mid, first_info, _ = queue[0]
        teams = first_info.get("teams", [])
        if len(teams) < 2:
            await interaction.response.send_message("❌ Match data incomplete.", ephemeral=True)
            return

        match_details = await cog.get_pandascore_data(f"/matches/{first_mid}")
        if not match_details:
            match_details = {"begin_at": first_info.get("start_time"), "status": "not_started",
                             "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

        banner = await cog.generate_banner(
            teams[0].get("image_url"), teams[1].get("image_url"),
            GAME_LOGOS.get(self.game_slug), self.game_slug
        )
        embed = cog.build_match_embed(
            self.game_slug, GAMES.get(self.game_slug), match_details,
            teams[0], teams[1], first_info.get("votes", {}),
            first_info.get("stream_url"), banner is not None
        )

        remaining = [(mid, info) for mid, info, _ in queue[1:]]
        vote_view = VoteCycleView(first_mid, teams[0], teams[1], self.game_slug, remaining, user_id)

        if banner:
            await interaction.response.send_message(embed=embed, file=banner, view=vote_view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=vote_view, ephemeral=True)


class UpcomingDetailsDropdownView(discord.ui.View):
    """Ephemeral view with dropdown to select which upcoming match details to show."""

    def __init__(self, game_slug: str, matches: list, selected_mid: str):
        super().__init__(timeout=300)
        self.game_slug = game_slug
        self.matches = matches  # list of (mid, info)

        options = []
        for mid, info in matches:
            teams = info.get("teams", [])
            if len(teams) >= 2:
                label = f"{teams[0]['name']} vs {teams[1]['name']}"
            else:
                label = f"Match {mid}"
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(
                label=label, value=mid, default=(mid == selected_mid)
            ))

        select = discord.ui.Select(
            placeholder="Select a match...",
            options=options,
            custom_id=f"upcoming_details_select_{secrets.token_hex(4)}"
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        selected_mid = interaction.data["values"][0]
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.edit_message(content="❌ Esports cog not found.", embed=None, view=None)
            return

        async with cog.data_lock:
            data = load_data_sync()
            match_info = data.get("active_matches", {}).get(selected_mid)

        if not match_info:
            await interaction.response.edit_message(content="❌ Match details no longer available.", embed=None, view=None)
            return

        teams = match_info.get("teams", [])
        if len(teams) < 2:
            await interaction.response.edit_message(content="❌ Match data incomplete.", embed=None, view=None)
            return

        match_details = await cog.get_pandascore_data(f"/matches/{selected_mid}")
        if not match_details:
            match_details = {"begin_at": match_info.get("start_time"), "status": "not_started",
                             "opponents": [], "number_of_games": None, "league": {}, "serie": {}, "tournament": {}}

        banner = await cog.generate_banner(
            teams[0].get("image_url"), teams[1].get("image_url"),
            GAME_LOGOS.get(self.game_slug), self.game_slug
        )
        embed = cog.build_match_embed(
            self.game_slug, GAMES.get(self.game_slug), match_details,
            teams[0], teams[1], match_info.get("votes", {}),
            match_info.get("stream_url"), banner is not None
        )

        # Update dropdown defaults
        for item in self.children:
            if isinstance(item, discord.ui.Select):
                for opt in item.options:
                    opt.default = (opt.value == selected_mid)

        if banner:
            await interaction.response.edit_message(embed=embed, attachments=[banner], view=self)
        else:
            await interaction.response.edit_message(embed=embed, attachments=[], view=self)


class UnifiedResultView(discord.ui.View):
    """Dropdown on the unified daily result embed. Shows match details from today and yesterday."""

    MAX_DROPDOWN_OPTIONS = 25  # Discord Select menu limit

    def __init__(self, game_slug: str, result_date: str, match_options: list = None):
        super().__init__(timeout=None)
        self.game_slug = game_slug
        self.result_date = result_date  # YYYY-MM-DD

        options = []
        if match_options:
            for mid, label in match_options[:self.MAX_DROPDOWN_OPTIONS]:
                if len(label) > 100:
                    label = label[:97] + "..."
                options.append(discord.SelectOption(label=label, value=mid))

        if not options:
            # Dummy option for persistent view registration (Discord requires at least one)
            options = [discord.SelectOption(label="No matches available", value="none")]

        select = discord.ui.Select(
            placeholder="Match Details",
            options=options,
            custom_id=f"result_dropdown_{game_slug}_{result_date}"
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        selected_mid = interaction.data["values"][0]
        if selected_mid == "none":
            await interaction.response.send_message("No match details available.", ephemeral=True)
            return

        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.send_message("❌ Esports cog not found.", ephemeral=True)
            return

        async with cog.data_lock:
            data = load_data_sync()
            mh = data.get("match_history", {}).get(selected_mid)
            mc = data.get("map_data_cache", {}).get(selected_mid)

        if not mh or len(mh.get("teams", [])) < 2:
            await interaction.response.send_message("❌ Match details no longer available.", ephemeral=True)
            return

        teams = mh["teams"]
        extra = mh.get("match_details_extra", {})
        match_details = {
            "results": mh.get("results", []),
            "number_of_games": extra.get("number_of_games"),
            "league": extra.get("league", {}),
            "serie": extra.get("serie", {}),
            "tournament": extra.get("tournament", {}),
            "official_stream_url": extra.get("official_stream_url"),
        }

        map_data = mc.get("maps") if mc else None

        team_a = {"name": teams[0]["name"], "id": teams[0].get("id"),
                  "flag": teams[0].get("flag"), "acronym": teams[0].get("acronym")}
        team_b = {"name": teams[1]["name"], "id": teams[1].get("id"),
                  "flag": teams[1].get("flag"), "acronym": teams[1].get("acronym")}

        game_slug = self.game_slug or mh.get("game_slug", "")
        embed = await cog.build_full_result_embed(
            game_slug, match_details, team_a, team_b,
            mh.get("winner_idx", 0), map_data=map_data,
            votes=mh.get("votes", {}), guild=interaction.guild
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


class ResultDetailsDropdownView(discord.ui.View):
    """Ephemeral view with dropdown to select which result details to show."""

    def __init__(self, game_slug: str, results: list, selected_mid: str):
        super().__init__(timeout=300)
        self.game_slug = game_slug
        self.results = results  # list of (mid, match_history_entry)

        options = []
        for mid, mh in results:
            teams = mh.get("teams", [])
            if len(teams) >= 2:
                label = f"{teams[0]['name']} vs {teams[1]['name']}"
            else:
                label = f"Match {mid}"
            if len(label) > 100:
                label = label[:97] + "..."
            options.append(discord.SelectOption(
                label=label, value=mid, default=(mid == selected_mid)
            ))

        select = discord.ui.Select(
            placeholder="Select a result...",
            options=options,
            custom_id=f"result_details_select_{secrets.token_hex(4)}"
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        selected_mid = interaction.data["values"][0]
        cog = interaction.client.get_cog("Esports")
        if not cog:
            await interaction.response.edit_message(content="❌ Esports cog not found.", embed=None, view=None)
            return

        async with cog.data_lock:
            data = load_data_sync()
            mh = data.get("match_history", {}).get(selected_mid)
            mc = data.get("map_data_cache", {}).get(selected_mid)

        if not mh or len(mh.get("teams", [])) < 2:
            await interaction.response.edit_message(content="❌ Result details no longer available.", embed=None, view=None)
            return

        teams = mh["teams"]
        extra = mh.get("match_details_extra", {})
        match_details = {
            "results": mh.get("results", []),
            "number_of_games": extra.get("number_of_games"),
            "league": extra.get("league", {}),
            "serie": extra.get("serie", {}),
            "tournament": extra.get("tournament", {}),
            "official_stream_url": extra.get("official_stream_url"),
        }

        map_data = mc.get("maps") if mc else None

        team_a = {"name": teams[0]["name"], "id": teams[0].get("id"),
                  "flag": teams[0].get("flag"), "acronym": teams[0].get("acronym")}
        team_b = {"name": teams[1]["name"], "id": teams[1].get("id"),
                  "flag": teams[1].get("flag"), "acronym": teams[1].get("acronym")}

        embed = await cog.build_full_result_embed(
            self.game_slug, match_details, team_a, team_b,
            mh.get("winner_idx", 0), map_data=map_data,
            votes=mh.get("votes", {}), guild=interaction.guild
        )

        for item in self.children:
            if isinstance(item, discord.ui.Select):
                for opt in item.options:
                    opt.default = (opt.value == selected_mid)

        await interaction.response.edit_message(embed=embed, view=self)


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


class MemberSelectView(discord.ui.View):
    """View with a member select dropdown for choosing a user."""
    def __init__(self, action_type: str, cog):
        super().__init__(timeout=120)
        self.action_type = action_type
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Search for a member...", row=0)
    async def select_member(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        member = select.values[0]
        user_id = str(member.id)
        member_name = member.display_name

        if self.action_type == "Wipe User Score":
            view = WipeUserGameSelectView(self.cog, user_id, member_name)
            await interaction.response.edit_message(
                content=f"Select game to wipe scores for **{member_name}**:",
                view=view
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

class GameEmojiModal(discord.ui.Modal, title="Game Vote Emojis"):
    """Modal for configuring custom emojis on vote buttons per game.
    Paste the custom emoji (e.g. <:val:123456789>) for each game, or leave blank to remove."""

    def __init__(self, cog, current: dict):
        super().__init__()
        self.cog = cog
        self._fields = {}
        for slug, name in GAMES.items():
            field = discord.ui.TextInput(
                label=name,
                placeholder="<:emoji_name:id> or leave blank",
                default=current.get(slug, ""),
                required=False,
                max_length=60
            )
            self._fields[slug] = field
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        async with self.cog.data_lock:
            data = load_data_sync()
            emojis = data.get("game_vote_emojis", {})
            for slug, field in self._fields.items():
                val = field.value.strip()
                if val:
                    emojis[slug] = val
                else:
                    emojis.pop(slug, None)
            data["game_vote_emojis"] = emojis
            save_data_sync(data)

        set_games = [GAMES[s] for s in self._fields if self._fields[s].value.strip()]
        if set_games:
            await interaction.response.send_message(f"Vote emojis updated for: {', '.join(set_games)}.", ephemeral=True)
        else:
            await interaction.response.send_message("All vote emojis cleared.", ephemeral=True)


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

    @discord.ui.button(label="Force Publish", style=discord.ButtonStyle.danger, row=3)
    async def force_publish(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_force_publish_menu(interaction)

    @discord.ui.button(label="Game Emojis", style=discord.ButtonStyle.primary, row=3)
    async def game_emojis(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Configure custom emojis shown on vote buttons per game."""
        data = load_data_sync()
        current = data.get("game_vote_emojis", {})
        modal = GameEmojiModal(self.cog, current)
        await interaction.response.send_modal(modal)

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
        view = MemberSelectView("Wipe User Score", self.cog)
        await interaction.response.send_message("Select a member to wipe their score:", view=view, ephemeral=True)

    @discord.ui.button(label="Modify Points", style=discord.ButtonStyle.primary, row=4)
    async def modify_points(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Add or remove points for a specific user."""
        view = MemberSelectView("Modify Points", self.cog)
        await interaction.response.send_message("Select a member to modify their points:", view=view, ephemeral=True)

    @discord.ui.button(label="Overturn Result", style=discord.ButtonStyle.danger, row=4)
    async def overturn_result(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Overturn a recent match result using stored match history."""
        await self.cog.show_overturn_menu(interaction)


