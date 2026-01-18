import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
import asyncio
import datetime
import traceback
import secrets
import os
import json
import re
from collections import OrderedDict
from io import BytesIO
from PIL import Image
from bs4 import BeautifulSoup
import urllib.parse
from difflib import SequenceMatcher
from typing import List, Dict, Any, Optional

# Import shared resources from Part 1
from .esports_shared import (
    GAMES, GAME_LOGOS, GAME_SHORT_NAMES, GAME_PLACEHOLDERS,
    DEFAULT_GAME_ICON_FALLBACK, ALLOWED_TIERS, TIER_BYPASS_KEYWORDS,
    STRAFE_GAME_SLUGS, STRAFE_GAME_PATHS, GAME_MAP_FALLBACK,
    logger, ensure_data_file, load_data_sync, save_data_sync,
    safe_parse_datetime, stitch_images, add_white_outline,
    LeaderboardView, PredictionView, EsportsAdminView,
    MAX_LEADERBOARD_NAME_LENGTH, MAX_MAP_NAME_LENGTH
)

# --- LOCAL CONFIGURATION ---
MATCH_TIMEOUT_SECONDS = 172800 # 48 hours
TEST_MATCH_TIMEOUT_SECONDS = 7200 # 2 hours
MAX_IMAGE_CACHE_SIZE = 100 
MAX_PROCESSED_HISTORY = 500

class StrafeClient:
    """Fetches esports data from Strafe.com by scraping __NEXT_DATA__."""

    def __init__(self):
        self.session = None
        self._match_cache = {}
        self._cache_times = {}
        self._cache_ttl = 300

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def find_strafe_match(self, team_a_name, team_b_name, game_slug, match_time=None):
        """Find a match on Strafe by scraping multiple pages. Returns match dict with 'slug' for URL."""
        strafe_slug = STRAFE_GAME_SLUGS.get(game_slug)
        if not strafe_slug:
            logger.debug(f"No Strafe slug for game: {game_slug}")
            return None

        # Build list of URLs to try - main page has latest_results, calendar/completed for all finished
        calendar_path = STRAFE_GAME_PATHS.get(game_slug, strafe_slug)
        urls_to_try = [
            f"https://www.strafe.com/{strafe_slug}",  # Main page (has latest_results for recent finished)
            f"https://www.strafe.com/calendar/{calendar_path}/completed/",  # Completed matches calendar
        ]

        # If we have match time, also try calendar for that specific date
        if match_time:
            date_str = match_time.strftime("%Y-%m-%d")
            urls_to_try.append(f"https://www.strafe.com/calendar/{calendar_path}/?date={date_str}")

        all_matches = []
        session = await self._get_session()

        for url in urls_to_try:
            try:
                logger.debug(f"Trying Strafe URL: {url}")
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.debug(f"Strafe page returned {resp.status} for {url}")
                        continue
                    html = await resp.text()

                soup = BeautifulSoup(html, 'html.parser')
                script = soup.find('script', id='__NEXT_DATA__')
                if not script:
                    logger.debug(f"No __NEXT_DATA__ found on {url}")
                    continue

                data = json.loads(script.string)
                page_props = data.get('props', {}).get('pageProps', {})

                def extract_matches(obj, found_list):
                    """Recursively find all match objects in the data."""
                    if isinstance(obj, dict):
                        if 'competitors' in obj and 'slug' in obj and len(obj.get('competitors', [])) >= 2:
                            found_list.append(obj)
                        for v in obj.values():
                            extract_matches(v, found_list)
                    elif isinstance(obj, list):
                        for item in obj:
                            extract_matches(item, found_list)

                page_matches = []
                extract_matches(page_props, page_matches)
                logger.debug(f"Found {len(page_matches)} matches on {url}")
                all_matches.extend(page_matches)

            except Exception as e:
                logger.debug(f"Error scraping {url}: {e}")
                continue

        if not all_matches:
            logger.info(f"No Strafe matches found across all pages for {game_slug}")
            return None

        logger.debug(f"Strafe scraped {len(all_matches)} total matches for {game_slug}")

        # Deduplicate by slug
        seen_slugs = set()
        unique_matches = []
        for m in all_matches:
            slug = m.get('slug')
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                unique_matches.append(m)

        best_match, best_score = None, 0
        for m in unique_matches:
            competitors = m.get('competitors', [])
            if len(competitors) < 2:
                continue

            home_name = ''
            away_name = ''

            for comp in competitors:
                side = comp.get('side_enum', '').upper()
                name = comp.get('name', '')
                if not name and 'competitor' in comp:
                    name = comp.get('competitor', {}).get('name', '')

                if side == 'HOME':
                    home_name = name
                elif side == 'AWAY':
                    away_name = name

            if not home_name and not away_name:
                home_name = competitors[0].get('name', '') or competitors[0].get('competitor', {}).get('name', '')
                away_name = competitors[1].get('name', '') or competitors[1].get('competitor', {}).get('name', '')

            if not home_name or not away_name:
                continue

            sim_a_home = SequenceMatcher(None, team_a_name.lower(), home_name.lower()).ratio()
            sim_b_away = SequenceMatcher(None, team_b_name.lower(), away_name.lower()).ratio()
            sim_a_away = SequenceMatcher(None, team_a_name.lower(), away_name.lower()).ratio()
            sim_b_home = SequenceMatcher(None, team_b_name.lower(), home_name.lower()).ratio()

            score = max((sim_a_home + sim_b_away) / 2, (sim_a_away + sim_b_home) / 2)

            if score > 0.25:
                logger.debug(f"Strafe candidate: {home_name} vs {away_name} (score: {score:.2f})")

            # Lowered threshold from 0.4 to 0.35 to catch more matches with slightly different names
            if score > best_score and score > 0.35:
                best_score, best_match = score, m

        if best_match:
            match_slug = best_match.get('slug')
            logger.info(f"Strafe match found: {team_a_name} vs {team_b_name} -> slug {match_slug} (score: {best_score:.2f})")
        else:
            logger.info(f"No Strafe match found for {team_a_name} vs {team_b_name}")
        return best_match

    async def get_map_scores(self, strafe_match_slug, team_a_name, team_b_name, game_slug):
        """Fetch map scores by scraping the match page directly."""
        maps = []
        if not strafe_match_slug:
            return maps

        url = f"https://www.strafe.com/match/{strafe_match_slug}/"
        session = await self._get_session()
        logger.debug(f"Fetching map scores from Strafe match page: {url}")

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(f"Strafe match page returned {resp.status} for {url}")
                    return maps
                html = await resp.text()

            soup = BeautifulSoup(html, 'html.parser')
            script = soup.find('script', id='__NEXT_DATA__')
            if not script:
                logger.warning(f"No __NEXT_DATA__ found on Strafe match page")
                return maps

            data = json.loads(script.string)
            page_props = data.get('props', {}).get('pageProps', {})
            legacy_match = page_props.get('legacyMatch', {})

            # Get home team name from header to determine team mapping
            header = legacy_match.get('header', {})
            competitors = header.get('competitors', {})
            home_team_name = ''
            away_team_name = ''

            if isinstance(competitors, dict):
                home_team_name = competitors.get('home', {}).get('name', '')
                away_team_name = competitors.get('away', {}).get('name', '')
            elif isinstance(competitors, list):
                for comp in competitors:
                    side = comp.get('side_enum', '').upper()
                    name = comp.get('name', '') or comp.get('competitor', {}).get('name', '')
                    if side == 'HOME':
                        home_team_name = name
                    elif side == 'AWAY':
                        away_team_name = name

            # Determine if team_a is the home team (use lower threshold for better matching)
            sim_a_home = SequenceMatcher(None, team_a_name.lower(), home_team_name.lower()).ratio()
            sim_a_away = SequenceMatcher(None, team_a_name.lower(), away_team_name.lower()).ratio()
            is_a_home = sim_a_home >= sim_a_away  # Use >= to prefer home if equal
            logger.debug(f"Team mapping: Home={home_team_name}, Away={away_team_name}, Team A ({team_a_name}) is_home={is_a_home} (sim: {sim_a_home:.2f} vs {sim_a_away:.2f})")

            # Extract map data from the 'live' array
            live_data = legacy_match.get('live', [])
            logger.debug(f"Strafe live_data has {len(live_data)} items")

            for item in live_data:
                key = item.get('key', '')
                item_data = item.get('data', {})

                # Skip items where data is not a dict
                if not isinstance(item_data, dict):
                    continue

                # Look for game/map items - more permissive detection
                map_info = item_data.get('map', {})
                game_info = item_data.get('game', {})
                winner_key = item_data.get('winner')  # 'home', 'away', or None

                # Accept if: key starts with 'game-', or has 'index' field (indicates a game), or has winner
                has_index = 'index' in item_data
                is_game_item = key.startswith('game-') or map_info or game_info or (has_index and winner_key)
                if not is_game_item:
                    continue

                logger.debug(f"Processing item: key='{key}', index={item_data.get('index')}, winner={winner_key}")

                # Get map name
                map_name = None
                if isinstance(map_info, dict):
                    map_name = map_info.get('name')
                elif isinstance(map_info, str):
                    map_name = map_info

                if not map_name:
                    idx = item_data.get('index', len(maps))
                    map_name = f"{GAME_MAP_FALLBACK.get(game_slug, 'Game')} {idx + 1}"

                # Get scores from game.final or directly from item_data
                final_scores = {}
                if isinstance(game_info, dict):
                    final_scores = game_info.get('final', {})
                # Also try score directly in item_data
                if not final_scores:
                    final_scores = item_data.get('score', {}) or item_data.get('final', {})

                score_home = final_scores.get('home', 0) or 0
                score_away = final_scores.get('away', 0) or 0

                # Map scores and winner to team_a/team_b
                if is_a_home:
                    score_a, score_b = score_home, score_away
                    if winner_key == 'home':
                        winner = 0
                    elif winner_key == 'away':
                        winner = 1
                    else:
                        winner = -1
                else:
                    score_a, score_b = score_away, score_home
                    if winner_key == 'home':
                        winner = 1
                    elif winner_key == 'away':
                        winner = 0
                    else:
                        winner = -1

                status = item_data.get('status', 'unknown')
                if winner_key:
                    status = 'finished'

                maps.append({
                    "name": map_name[:18],
                    "score_a": score_a,
                    "score_b": score_b,
                    "winner": winner,
                    "status": status
                })
                logger.debug(f"Map {len(maps)}: {map_name} - {score_a}:{score_b}, winner: {winner}")

            # If no maps found via 'live', try alternate data paths
            if not maps:
                logger.debug("No maps found in live array, checking alternate paths...")

                # Try 'games' array in header or root
                games_array = legacy_match.get('games', []) or header.get('games', [])
                for idx, game in enumerate(games_array):
                    if isinstance(game, dict):
                        map_name = game.get('map', {}).get('name') if isinstance(game.get('map'), dict) else game.get('map', f"Game {idx + 1}")
                        if not map_name:
                            map_name = f"{GAME_MAP_FALLBACK.get(game_slug, 'Game')} {idx + 1}"

                        score_data = game.get('score', {}) or game.get('final', {})
                        score_home = score_data.get('home', 0) or 0
                        score_away = score_data.get('away', 0) or 0

                        winner_key = game.get('winner')

                        if is_a_home:
                            score_a, score_b = score_home, score_away
                            winner = 0 if winner_key == 'home' else (1 if winner_key == 'away' else -1)
                        else:
                            score_a, score_b = score_away, score_home
                            winner = 1 if winner_key == 'home' else (0 if winner_key == 'away' else -1)

                        maps.append({
                            "name": str(map_name)[:18],
                            "score_a": score_a,
                            "score_b": score_b,
                            "winner": winner,
                            "status": "finished" if winner_key else "unknown"
                        })

            # Final fallback: try to extract from 'scores' in header (gives overall match result at minimum)
            if not maps:
                logger.debug("No games array found, trying header scores...")
                scores = header.get('scores', {})
                if isinstance(scores, dict) and 'home' in scores and 'away' in scores:
                    # This is just the overall match score, not per-map
                    # Only use this if we have nothing else
                    total_home = scores.get('home', 0) or 0
                    total_away = scores.get('away', 0) or 0
                    if total_home > 0 or total_away > 0:
                        logger.debug(f"Using header scores as fallback: {total_home}-{total_away}")
                        # Create synthetic "game" entries based on the score
                        num_games = total_home + total_away
                        home_wins = 0
                        for i in range(num_games):
                            # Alternate wins based on who won overall
                            if home_wins < total_home:
                                winner_key = 'home'
                                home_wins += 1
                            else:
                                winner_key = 'away'

                            if is_a_home:
                                winner = 0 if winner_key == 'home' else 1
                            else:
                                winner = 1 if winner_key == 'home' else 0

                            maps.append({
                                "name": f"{GAME_MAP_FALLBACK.get(game_slug, 'Game')} {i + 1}",
                                "score_a": 0,
                                "score_b": 0,
                                "winner": winner,
                                "status": "finished"
                            })

        except Exception as e:
            logger.warning(f"Error fetching Strafe map data: {e}")
            import traceback
            logger.debug(traceback.format_exc())

        logger.info(f"Strafe returned {len(maps)} maps for match {strafe_match_slug}")
        return maps


class Esports(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.api_key = os.getenv("PANDASCORE_KEY")
        
        self.data_lock = asyncio.Lock()
        self.processing_lock = asyncio.Lock()
        self.processing_matches = set() 
        self.test_game_idx = 0
        
        self.image_cache = OrderedDict()
        self.max_cache_size = MAX_IMAGE_CACHE_SIZE
        
        self.emoji_map_cache = {}
        self.error_queue = []
        self.strafe = StrafeClient()

        ensure_data_file()
        self._update_emoji_cache()
        self.match_tracker.start()
        self.error_reporting_loop.start()

    def cog_unload(self):
        self.match_tracker.cancel()
        self.error_reporting_loop.cancel()
        asyncio.create_task(self.strafe.close())

    # --- ERROR HANDLING ---
    async def report_error(self, error_msg):
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        tb_str = traceback.format_exc()
        full_msg = f"[{timestamp}] {error_msg}\n{tb_str}"
        self.error_queue.append(full_msg)
        if len(self.error_queue) > 50: self.error_queue = self.error_queue[-50:]
        logger.error(f"Queued error: {error_msg}")

    @tasks.loop(hours=2)
    async def error_reporting_loop(self):
        if not self.error_queue: return
        try:
            if not self.bot.owner_id:
                app_info = await self.bot.application_info()
                self.bot.owner_id = app_info.team.owner_id if app_info.team else app_info.owner.id

            owner = await self.bot.fetch_user(self.bot.owner_id)
            if owner:
                report_content = "\n".join(self.error_queue)
                self.error_queue.clear()
                if len(report_content) > 1900:
                    file_data = BytesIO(report_content.encode('utf-8'))
                    await owner.send("⚠️ **eSports Cog Error Report**", file=discord.File(file_data, filename="error_report.txt"))
                else:
                    await owner.send(f"⚠️ **eSports Cog Error Report**\n```\n{report_content}\n```")
        except Exception as e:
            logger.error(f"Failed error report loop: {e}")

    @error_reporting_loop.before_loop
    async def before_error_loop(self):
        await self.bot.wait_until_ready()

    # --- DATA & API HELPERS ---
    def _update_emoji_cache(self):
        try:
            data = load_data_sync()
            self.emoji_map_cache = data.get("emoji_map", {})
        except Exception as e:
            logger.error(f"Failed to update emoji cache: {e}")
            self.emoji_map_cache = {}

    def validate_team_data(self, team_dict: dict) -> bool:
        if not isinstance(team_dict, dict): return False
        return all(k in team_dict and team_dict[k] for k in ['name', 'id'])

    async def get_pandascore_data(self, endpoint, params=None):
        if not self.api_key: return None
        headers = {"Authorization": f"Bearer {self.api_key}"}
        url = f"https://api.pandascore.co{endpoint}"
        
        timeout = aiohttp.ClientTimeout(total=30)
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers, params=params) as resp:
                        if resp.status == 200: return await resp.json()
                        elif resp.status == 429:
                            await asyncio.sleep(int(resp.headers.get("Retry-After", 2 ** attempt)))
                        elif resp.status >= 500: await asyncio.sleep(1)
                        else: return None
            except Exception as e:
                logger.debug(f"API request attempt {attempt+1} failed: {e}")
                await asyncio.sleep(1)
        return None

    async def fetch_roster(self, team_id, team_name, game_slug):
        team_data = await self.get_pandascore_data(f"/teams/{team_id}")
        if team_data and 'players' in team_data:
            # First try active players only
            roster = [p.get('name', 'Unknown') for p in team_data['players'] if p.get('active', True)]
            # If no active players, try all players (some APIs don't set active correctly)
            if not roster:
                roster = [p.get('name', 'Unknown') for p in team_data['players']]
            # Return if we have at least 1 player (lowered from 3 to be more permissive)
            if roster:
                return roster[:6]
        return []

    # --- TEAM DISPLAY & EMOJIS ---
    def get_team_display(self, team_data):
        emoji_map = self.emoji_map_cache
        
        # 1. Check direct Acronym
        acronym = team_data.get('acronym')
        if acronym:
            if acronym in emoji_map: return str(emoji_map[acronym])
            if acronym.upper() in emoji_map: return str(emoji_map[acronym.upper()])

        # 2. Check Name Key (First word, uppercase, alphanumeric)
        name = team_data.get('name', '')
        if name:
            key_short = name.split(' ')[0].upper()
            key_short = "".join(c for c in key_short if c.isalnum())
            if key_short in emoji_map: return str(emoji_map[key_short])
            
            # 3. Check Full Name Key (no spaces)
            key_long = "".join(c for c in name.upper() if c.isalnum())
            if key_long in emoji_map: return str(emoji_map[key_long])
                
        # 4. Fallback to Flag/Location
        iso = team_data.get('flag') or team_data.get('location')
        if iso and len(iso) == 2 and iso.isalpha(): 
            return f":flag_{iso.lower()}:"
            
        return ":globe_with_meridians:"

    def is_quality_match(self, match):
        tier = match.get('tournament', {}).get('tier')
        if tier in ALLOWED_TIERS: return True
        
        event_name = (
            (match.get('league', {}).get('name', '') or "") + " " + 
            (match.get('serie', {}).get('full_name', '') or "") + " " + 
            (match.get('tournament', {}).get('name', '') or "")
        ).lower()
        return any(k in event_name for k in TIER_BYPASS_KEYWORDS)

    # --- STRAFE SCRAPING ---
    def _calculate_similarity(self, a, b):
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()

    async def find_strafe_match_url(self, team_a_name: str, team_b_name: str, game_slug: str, match_date: datetime.datetime) -> Optional[str]:
        strafe_game = STRAFE_GAME_PATHS.get(game_slug)
        if not strafe_game: return None
        
        calendar_url = f"https://www.strafe.com/calendar/{strafe_game}/"
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(calendar_url) as resp:
                    if resp.status != 200: 
                        logger.warning(f"Strafe calendar returned {resp.status}")
                        return None
                    html = await resp.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            script = soup.find('script', id='__NEXT_DATA__')
            if not script: 
                logger.warning("No __NEXT_DATA__ script in calendar")
                return None
            
            data = json.loads(script.string)
            matches = []
            
            # Recursive search for matches in props
            def extract_matches(obj):
                if isinstance(obj, dict):
                    # Check for match object structure
                    if 'home' in obj and 'away' in obj and 'slug' in obj and 'start_time' in obj:
                        matches.append(obj)
                    for v in obj.values():
                        extract_matches(v)
                elif isinstance(obj, list):
                    for item in obj:
                        extract_matches(item)
            
            extract_matches(data.get('props', {}))
            
            # Calculate match time similarity
            target_ts = match_date.timestamp() if match_date.tzinfo else match_date.replace(tzinfo=datetime.timezone.utc).timestamp()

            best_match = None
            best_score = 0
            
            for m in matches:
                home_name = m.get('home', {}).get('name', '')
                away_name = m.get('away', {}).get('name', '')
                slug = m.get('slug', '')
                start_str = m.get('start_time')
                
                # Check Time Proximity (within 24 hours)
                try:
                    match_dt = safe_parse_datetime(start_str)
                    if match_dt:
                        match_ts = match_dt.timestamp()
                        if abs(match_ts - target_ts) > 86400: # Skip if > 24 hours apart
                            continue
                except: pass

                # Check Name Similarity
                sim_a1 = self._calculate_similarity(team_a_name, home_name)
                sim_a2 = self._calculate_similarity(team_a_name, away_name)
                sim_b1 = self._calculate_similarity(team_b_name, home_name)
                sim_b2 = self._calculate_similarity(team_b_name, away_name)
                
                # A=Home, B=Away OR A=Away, B=Home
                score_normal = (sim_a1 + sim_b2) / 2
                score_swap = (sim_a2 + sim_b1) / 2
                current_max = max(score_normal, score_swap)
                
                if current_max > best_score and current_max > 0.6:
                    best_score = current_max
                    best_match = slug
            
            if best_match:
                logger.info(f"Found Strafe URL: {best_match} (Score: {best_score:.2f})")
                return f"https://www.strafe.com/match/{best_match}"
            else:
                logger.warning(f"No Strafe match found for {team_a_name} vs {team_b_name}")
                    
        except Exception as e:
            logger.error(f"Strafe URL find error: {e}")
        return None

    async def scrape_strafe_maps(self, strafe_url: str, team_a_name: str, team_b_name: str) -> List[Dict[str, Any]]:
        if not strafe_url: return []
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(strafe_url) as resp:
                    if resp.status != 200: return []
                    html = await resp.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            script = soup.find('script', id='__NEXT_DATA__')
            if not script: return []
            
            json_data = json.loads(script.string)
            
            # Path based on user provided HTML
            legacy_match = json_data.get('props', {}).get('pageProps', {}).get('legacyMatch', {})
            live_data = legacy_match.get('live', [])
            competitors = legacy_match.get('header', {}).get('competitors', {})
            home_team_name = competitors.get('home', {}).get('name', '')
            
            # Determine mapping: Is Team A == Home?
            sim_a_home = self._calculate_similarity(team_a_name, home_team_name)
            is_team_a_home = sim_a_home > 0.5 # Lowered threshold
            
            maps = []
            
            for item in live_data:
                # We need items that represent games/maps
                # Key usually starts with 'game-' like 'game-valorant' or 'game-rocketleague'
                key = item.get('key', '')
                if key.startswith('game-') and 'data' in item:
                    data = item['data']
                    
                    # Some games might not be finished, but we still want the name if available
                    status = data.get('status')
                    
                    map_info = data.get('map', {})
                    game_info = data.get('game', {})
                    
                    # Map Name
                    map_name = map_info.get('name')
                    if not map_name:
                        # Fallback for games without "map" (like RL matches sometimes just indexed)
                        index = data.get('index', 0)
                        map_name = f"Game {index + 1}"

                    # Scores
                    final_score = game_info.get('final', {})
                    score_home = final_score.get('home', 0)
                    score_away = final_score.get('away', 0)
                    
                    # Winner
                    winner_key = data.get('winner') # 'home', 'away', or None
                    
                    if is_team_a_home:
                        # A = Home, B = Away
                        s_a, s_b = score_home, score_away
                        if winner_key == 'home': winner_idx = 0
                        elif winner_key == 'away': winner_idx = 1
                        else: winner_idx = -1
                    else:
                        # A = Away, B = Home
                        s_a, s_b = score_away, score_home
                        if winner_key == 'home': winner_idx = 1
                        elif winner_key == 'away': winner_idx = 0
                        else: winner_idx = -1
                        
                    maps.append({
                        "name": map_name[:MAX_MAP_NAME_LENGTH],
                        "score_a": s_a,
                        "score_b": s_b,
                        "winner": winner_idx,
                        "status": status
                    })
            
            # If no maps found in 'live', try checking 'scores' object in header?
            # (Usually 'live' array is the source of truth for detailed maps)
            
            logger.info(f"Strafe scrape extracted {len(maps)} maps")
            return maps
            
        except Exception as e:
            logger.error(f"Strafe JSON scrape error: {e}")
            return []

    async def get_strafe_map_data(self, team_a_name, team_b_name, game_slug, match_time=None):
        if not STRAFE_GAME_SLUGS.get(game_slug):
            return []
        try:
            strafe_match = await self.strafe.find_strafe_match(team_a_name, team_b_name, game_slug, match_time)
            if strafe_match and strafe_match.get('slug'):
                return await self.strafe.get_map_scores(strafe_match['slug'], team_a_name, team_b_name, game_slug)
        except Exception as e:
            logger.error(f"Strafe error: {e}")
        return []

    # --- IMAGE & EMOJI MANAGEMENT ---
    async def download_image(self, session, url):
        if not url or not url.startswith(('http', 'https')): return None
        if url in self.image_cache: 
            self.image_cache.move_to_end(url)
            return self.image_cache[url].copy()

        try:
            async def fetch(s):
                async with s.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        img = Image.open(BytesIO(data)).convert("RGBA")
                        if len(self.image_cache) >= self.max_cache_size: self.image_cache.popitem(last=False)
                        self.image_cache[url] = img
                        return img.copy()
            
            if session: return await fetch(session)
            async with aiohttp.ClientSession() as s: return await fetch(s)
        except Exception as e:
            logger.debug(f"Image download failed for {url}: {e}")
            return None

    async def generate_banner(self, url_a, url_b, url_game, game_slug, is_result=False):
        t_ph = GAME_PLACEHOLDERS.get(game_slug, DEFAULT_GAME_ICON_FALLBACK)
        
        async with aiohttp.ClientSession() as session:
            imgs = await asyncio.gather(
                self.download_image(session, url_a or t_ph),
                self.download_image(session, url_b or t_ph),
                self.download_image(session, url_game or DEFAULT_GAME_ICON_FALLBACK),
                self.download_image(session, t_ph),
                self.download_image(session, DEFAULT_GAME_ICON_FALLBACK)
            )
        
        return await asyncio.to_thread(stitch_images, imgs[0], imgs[1], imgs[2], imgs[3], imgs[4], is_result, game_slug)

    async def manage_team_emojis(self, interaction: discord.Interaction) -> int:
        data = load_data_sync()
        target_guilds = [g for g in self.bot.guilds if str(g.id) in data.get("emoji_storage_guilds", [])]
        if not target_guilds: return 0

        unique_teams = {}
        for game_slug in GAMES.keys():
            matches = await self.get_pandascore_data(f"/{game_slug}/matches", params={"sort": "-begin_at", "page[size]": 100})
            if not matches: continue
            
            for m in matches:
                if m.get('tournament', {}).get('tier') not in ['s', 'a']: continue
                for opp in m.get('opponents', []):
                    t = opp.get('opponent')
                    if not t or not t.get('image_url'): continue
                    
                    # Generate keys used in get_team_display
                    keys_to_save = []
                    
                    # 1. Acronym
                    if t.get('acronym'):
                        keys_to_save.append(t['acronym'].upper())
                    
                    # 2. First Word Key
                    name_parts = t.get('name', '').split(' ')
                    if name_parts:
                        k1 = "".join(c for c in name_parts[0].upper() if c.isalnum())
                        keys_to_save.append(k1)
                    
                    # 3. Full Name Key
                    k2 = "".join(c for c in t.get('name', '').upper() if c.isalnum())
                    keys_to_save.append(k2)

                    for key in keys_to_save:
                        if len(key) >= 2 and key not in unique_teams:
                            unique_teams[key] = t['image_url']

        added_count = 0
        async with aiohttp.ClientSession() as session:
            for key, url in unique_teams.items():
                if added_count >= 150: break
                
                # Check exist
                exists = False
                # We need to check if ANY emoji maps to this key in our data
                if key in data["emoji_map"]: 
                    exists = True
                
                if exists: continue

                # Upload
                target = next((g for g in target_guilds if len(g.emojis) < g.emoji_limit), None)
                if not target: break

                try:
                    async with session.get(url) as resp:
                        if resp.status != 200: continue
                        img_data = await resp.read()
                    
                    img = Image.open(BytesIO(img_data)).convert("RGBA")
                    img.thumbnail((128, 128))
                    img = add_white_outline(img, thickness=3)
                    
                    out = BytesIO()
                    img.save(out, format="PNG")
                    out.seek(0)
                    
                    emoji_name = f"esp_{key}"[:32]
                    new_emoji = await target.create_custom_emoji(name=emoji_name, image=out.read())
                    async with self.data_lock:
                        d = load_data_sync()
                        d["emoji_map"][key] = str(new_emoji)
                        save_data_sync(d)
                    added_count += 1
                    await asyncio.sleep(1.5)
                except Exception as e:
                    logger.error(f"Emoji upload failed for {key}: {e}")

        return added_count

    # --- EMBED BUILDERS ---
    async def get_map_history(self, match_details, saved_teams, game_slug: str, map_data: list = None) -> Optional[str]:
        """Build map history string. Returns None if no meaningful data to display."""
        num_games = match_details.get('number_of_games') or 0

        # If no map data at all, return None to signal we should hide this section
        if not map_data:
            return None

        if len(saved_teams) < 2:
            return None

        lines = []
        has_real_data = False  # Track if we have any actual played map data

        for m in map_data:
            map_name = m.get('name', 'Map')
            status = m.get('status', 'finished')
            winner_idx = m.get('winner')

            # Construct display string
            if status == 'finished' and winner_idx is not None and winner_idx != -1:
                has_real_data = True
                w_disp = self.get_team_display(saved_teams[winner_idx])
                score_a = m.get('score_a', 0)
                score_b = m.get('score_b', 0)

                # Some games (RL) might just report set wins, not inner scores
                if score_a == 0 and score_b == 0:
                    lines.append(f"• {map_name}: Winner {w_disp}")
                else:
                    lines.append(f"• {map_name}: {score_a}-{score_b} {w_disp}")
            # Don't add "Not played" entries - just show what we have

        # If we have map_data but no actual played results, return None
        if not has_real_data or not lines:
            return None

        # Only pad with "Not played" if we KNOW the format (num_games) and have fewer maps
        # This handles cases like Bo3 where only 2 maps were played
        if num_games > 0 and len(lines) < num_games:
            remaining = num_games - len(lines)
            # Cap at reasonable number to avoid spam
            for i in range(min(remaining, 3)):
                lines.append(f"• Map {len(lines) + 1}: Not played")

        return f"||{chr(10).join(lines)}||"

    async def generate_leaderboard_embed(self, guild, game_slug: str):
        game_slug = game_slug if game_slug in GAMES else "valorant"
        data = load_data_sync()
        stats = data["leaderboards"].get(game_slug, {})
        
        if not stats:
            desc = "No data yet for this month."
        else:
            sorted_stats = sorted(stats.items(), key=lambda x: (x[1]['wins'], -x[1]['losses']), reverse=True)[:10]
            lines = []
            for i, (uid, s) in enumerate(sorted_stats, 1):
                member = guild.get_member(int(uid))
                name = member.display_name if member else f"User {uid}"
                if len(name) > MAX_LEADERBOARD_NAME_LENGTH: name = name[:MAX_LEADERBOARD_NAME_LENGTH] + ".."
                streak = f" 🔥x{s.get('streak',0)}" if s.get('streak', 0) >= 3 else ""
                lines.append(f"**{i}.** {name} - **{s['wins']}**W {s['losses']}L{streak}")
            desc = "\n".join(lines)

        embed = discord.Embed(title=f"🏆 {GAMES.get(game_slug)} Monthly Leaderboard", color=discord.Color.gold(), description=desc)
        embed.set_footer(text="Resets monthly | Showing Top 10")
        return embed

    def build_match_embed(self, game_slug, game_name, match_details, team_a_data, team_b_data, votes, stream_url=None, has_banner=False):
        status = match_details.get('status', 'not_started')
        results = match_details.get('results', [])
        s_a, s_b = 0, 0
        if results:
            for r in results:
                if r.get('team_id') == team_a_data.get('id'): s_a = r.get('score', 0)
                elif r.get('team_id') == team_b_data.get('id'): s_b = r.get('score', 0)

        dt = safe_parse_datetime(match_details.get('begin_at'))
        time_str, timestamp = "", ""
        if dt:
            now = datetime.datetime.now(datetime.timezone.utc)
            if dt > now:
                diff = dt - now
                days = diff.days
                h, r = divmod(diff.seconds, 3600)
                m, _ = divmod(r, 60)
                time_str = f" in {days}d {h}h" if days else f" in {h}h {m}m"
            timestamp = f"<t:{int(dt.timestamp())}:F>"

        if status == "running":
            title = f"🔴 Live: {team_a_data['name']} ({s_a}) vs {team_b_data['name']} ({s_b})"
            color = discord.Color.red()
        else:
            title = f"🔔 Upcoming Match{time_str}"
            color = discord.Color.green()

        f_a = self.get_team_display(team_a_data)
        f_b = self.get_team_display(team_b_data)
        
        num_games = match_details.get('number_of_games')
        bo_str = f" (Bo{num_games})" if num_games else ""
        
        desc = [f"{f_a} {team_a_data['name']} vs. {f_b} {team_b_data['name']}{bo_str}", ""]
        
        event_parts = []
        if match_details.get('league', {}).get('name'):
            event_parts.append(match_details['league']['name'])
        if match_details.get('serie', {}).get('full_name'):
            event_parts.append(match_details['serie']['full_name'])
        elif match_details.get('serie', {}).get('name'):
            event_parts.append(match_details['serie']['name'])
        if match_details.get('tournament', {}).get('name'):
            tourn_name = match_details['tournament']['name']
            if not event_parts or tourn_name.lower() not in event_parts[-1].lower():
                event_parts.append(tourn_name)
        
        if event_parts:
            desc.append(f"**{' - '.join(event_parts)}**")
        desc.append(timestamp)
        
        embed = discord.Embed(title=title, description="\n".join(desc), color=color)
        if stream_url: embed.url = stream_url
        embed.set_author(name=game_name, icon_url=GAME_LOGOS.get(game_slug))
        
        # Set banner image using attachment reference
        if has_banner:
            embed.set_image(url="attachment://match_banner.png")

        for t in [team_a_data, team_b_data]:
            roster = ", ".join(t.get('roster', [])) or "*Roster unavailable*"
            embed.add_field(name=f"{self.get_team_display(t)} {t['name']}", value=roster, inline=False)

        count = len(votes)
        if status != "not_started" and count > 0:
            a_v = sum(1 for v in votes.values() if v == 0)
            p_a = (a_v/count*100)
            embed.add_field(name="Server Picks", value=f"• {team_a_data['name']}: {p_a:.1f}%\n• {team_b_data['name']}: {100-p_a:.1f}%", inline=False)

        embed.set_footer(text=f"Predictions Open" if status == "not_started" else f"Predictions locked | {count} Votes")
        return embed

    async def build_result_embed(self, channel, game_slug, match_details, team_a, team_b, winner_idx, votes, lb_top_5, is_test=False, map_data=None):
        saved = [team_a, team_b]
        winner = saved[winner_idx]
        scores = match_details.get('results', [])
        s1 = next((s.get('score') for s in scores if s.get('team_id') == team_a['id']), 0)
        s2 = next((s.get('score') for s in scores if s.get('team_id') == team_b['id']), 0)

        embed = discord.Embed(title=f"🏆 {GAMES.get(game_slug, 'ESPORTS').upper()} RESULTS", color=discord.Color.greyple())
        if match_details.get('official_stream_url'): embed.url = match_details['official_stream_url']
        embed.set_author(name=GAMES.get(game_slug), icon_url=GAME_LOGOS.get(game_slug))

        num_games = match_details.get('number_of_games')
        bo_str = f" (Bo{num_games})" if num_games else ""
        
        desc = [
            f"||**{winner['name']} Wins {s1}-{s2}!**||", "",
            f"{self.get_team_display(team_a)} {team_a['name']} vs. {self.get_team_display(team_b)} {team_b['name']}{bo_str}", ""
        ]
        
        event_parts = []
        if match_details.get('league', {}).get('name'):
            event_parts.append(match_details['league']['name'])
        if match_details.get('serie', {}).get('full_name'):
            event_parts.append(match_details['serie']['full_name'])
        elif match_details.get('serie', {}).get('name'):
            event_parts.append(match_details['serie']['name'])
        if match_details.get('tournament', {}).get('name'):
            tourn_name = match_details['tournament']['name']
            if not event_parts or tourn_name.lower() not in event_parts[-1].lower():
                event_parts.append(tourn_name)
        
        if event_parts:
            desc.append(f"**{' - '.join(event_parts)}**")
        
        embed.description = "\n".join(desc)

        # Only add Match History if we have real map data to show
        map_hist = await self.get_map_history(match_details, saved, game_slug, map_data)
        if map_hist:
            embed.add_field(name="Match History", value=map_hist, inline=False)

        winners = ["TestUser"] if is_test else [f"<@{u}>" for u, v in votes.items() if v == winner_idx]
        w_text = ", ".join(winners)
        if len(w_text) > 1000: w_text = f"{len(winners)} players!"
        embed.add_field(name="Correct Predictors", value=w_text or "No one!", inline=False)

        if not lb_top_5 and not is_test:
            data = load_data_sync()
            stats = data["leaderboards"].get(game_slug, {})
            sorted_s = sorted(stats.items(), key=lambda x: (x[1]['wins'], -x[1]['losses']), reverse=True)[:5]
            lines = []
            for i, (u, s) in enumerate(sorted_s, 1):
                m = channel.guild.get_member(int(u))
                n = m.display_name if m else f"User {u}"
                lines.append(f"**{i}. {n[:12]}**: {s['wins']}W {s['losses']}L")
            lb_top_5 = "\n".join(lines)
        
        if lb_top_5: embed.add_field(name=f"Monthly Top 5 ({GAME_SHORT_NAMES.get(game_slug)})", value=lb_top_5, inline=False)
        return embed

    async def process_result(self, channel, info, winner_idx, details, saved_teams, map_data=None):
        match_id = str(details['id'])
        game_slug = info['game_slug']
        votes = info['votes']
        
        async with self.data_lock:
            data = load_data_sync()
            if match_id in data.get("processed_matches", []): return
            data["processed_matches"].append(match_id)
            if len(data["processed_matches"]) > MAX_PROCESSED_HISTORY: data["processed_matches"] = data["processed_matches"][-MAX_PROCESSED_HISTORY:]
            
            for uid, vote in votes.items():
                if uid not in data["leaderboards"][game_slug]:
                    data["leaderboards"][game_slug][uid] = {"wins": 0, "losses": 0, "streak": 0}
                s = data["leaderboards"][game_slug][uid]
                if vote == winner_idx:
                    s["wins"] += 1
                    s["streak"] = s.get("streak", 0) + 1
                else:
                    s["losses"] += 1
                    s["streak"] = 0
            save_data_sync(data)

        embed = await self.build_result_embed(channel, game_slug, details, saved_teams[0], saved_teams[1], winner_idx, votes, None, map_data=map_data)
        await channel.send(embed=embed)

    # --- CORE LOOPS ---
    def embeds_are_different(self, old, new):
        if not old or not new: return True
        if old.title != new.title or old.description != new.description or len(old.fields) != len(new.fields): return True
        return any(o.value != n.value for o, n in zip(old.fields, new.fields))

    @tasks.loop(minutes=5)
    async def match_tracker(self):
        try:
            async with self.data_lock:
                data = load_data_sync()
                chan_id = data.get("channel_id")
                active = data.get("active_matches", {}).copy()
                
                cur_m = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")
                if data.get("last_reset_month") != cur_m:
                    data["leaderboards"] = {k: {} for k in GAMES.keys()}
                    data["last_reset_month"] = cur_m
                    save_data_sync(data)

            if not chan_id: return
            channel = self.bot.get_channel(chan_id)
            if not channel: return

            now = datetime.datetime.now(datetime.timezone.utc)

            # 1. FETCH NEW
            for slug, name in GAMES.items():
                p = {"filter[status]": "not_started,running", "sort": "begin_at", "range[begin_at]": f"{(now - datetime.timedelta(hours=12)).isoformat()},{(now + datetime.timedelta(hours=24)).isoformat()}"}
                matches = await self.get_pandascore_data(f"/{slug}/matches", params=p)
                
                if matches:
                    for m in matches:
                        mid = str(m['id'])
                        
                        async with self.processing_lock:
                            if mid in self.processing_matches: continue
                            self.processing_matches.add(mid)

                        should_skip = False
                        async with self.data_lock:
                            data = load_data_sync()
                            if mid in data["active_matches"] or mid in data["processed_matches"]:
                                should_skip = True
                        
                        if should_skip or not self.is_quality_match(m) or len(m.get('opponents',[])) < 2:
                            async with self.processing_lock:
                                self.processing_matches.discard(mid)
                            continue
                        
                        try:
                            t_a_base, t_b_base = m['opponents'][0]['opponent'], m['opponents'][1]['opponent']
                            t_a = {"name": t_a_base['name'], "acronym": t_a_base.get('acronym'), "id": t_a_base['id'], "roster": await self.fetch_roster(t_a_base['id'], t_a_base['name'], slug), "flag": t_a_base.get('location'), "image_url": t_a_base.get('image_url')}
                            t_b = {"name": t_b_base['name'], "acronym": t_b_base.get('acronym'), "id": t_b_base['id'], "roster": await self.fetch_roster(t_b_base['id'], t_b_base['name'], slug), "flag": t_b_base.get('location'), "image_url": t_b_base.get('image_url')}

                            f = await self.generate_banner(t_a.get('image_url'), t_b.get('image_url'), GAME_LOGOS[slug], slug)
                            e = self.build_match_embed(slug, name, m, t_a, t_b, {}, m.get('official_stream_url'), f is not None)
                            msg = await channel.send(embed=e, file=f, view=PredictionView(mid, t_a, t_b))

                            async with self.data_lock:
                                d = load_data_sync()
                                d["active_matches"][mid] = {
                                    "message_id": msg.id, "channel_id": chan_id, "game_slug": slug,
                                    "start_time": m['begin_at'], "teams": [t_a, t_b], "votes": {},
                                    "fail_count": 0, "stream_url": m.get('official_stream_url'), "status": "active"
                                }
                                save_data_sync(d)
                        except Exception as e: logger.error(f"Init match {mid} failed: {e}")
                        finally: 
                            async with self.processing_lock:
                                self.processing_matches.discard(mid)

            # 2. UPDATE ACTIVE
            data = load_data_sync()
            to_remove = []
            
            for mid, info in data["active_matches"].items():
                start = safe_parse_datetime(info.get('start_time'))
                if info.get('is_test'):
                    if start and (now - start).total_seconds() > TEST_MATCH_TIMEOUT_SECONDS: to_remove.append(mid)
                    continue
                else:
                    if start and (now - start).total_seconds() > MATCH_TIMEOUT_SECONDS:
                        to_remove.append(mid)
                        continue

                teams = info.get('teams', [])
                if len(teams) < 2:
                    to_remove.append(mid)
                    continue

                details = await self.get_pandascore_data(f"/matches/{mid}")
                if not details: continue
                
                try: msg = await channel.fetch_message(info['message_id'])
                except discord.NotFound:
                    logger.warning(f"Message {mid} not found, removing"); to_remove.append(mid); continue
                except Exception as e:
                    logger.error(f"Error fetching {mid}: {e}"); to_remove.append(mid); continue

                status = details['status']
                
                if status in ["running", "not_started"]:
                    # Always regenerate banner to avoid expired CDN URLs
                    banner_file = await self.generate_banner(
                        teams[0].get('image_url'),
                        teams[1].get('image_url'),
                        GAME_LOGOS.get(info['game_slug']),
                        info['game_slug']
                    )
                    embed = self.build_match_embed(info['game_slug'], GAMES.get(info['game_slug']), details, teams[0], teams[1], info['votes'], info.get('stream_url'), banner_file is not None)

                    if self.embeds_are_different(msg.embeds[0], embed):
                        try:
                            if status == "running":
                                view = discord.ui.View()
                                if info.get('stream_url'):
                                    view.add_item(discord.ui.Button(label="Watch Live", url=info['stream_url'], emoji="📺"))
                                if banner_file:
                                    await msg.edit(embed=embed, attachments=[banner_file], view=view)
                                else:
                                    await msg.edit(embed=embed, view=view)
                            else:
                                if banner_file:
                                    await msg.edit(embed=embed, attachments=[banner_file])
                                else:
                                    await msg.edit(embed=embed)
                        except (asyncio.TimeoutError, TimeoutError) as e:
                            logger.warning(f"Timeout editing message for match {mid}, will retry next cycle: {e}")
                            continue
                        except discord.HTTPException as e:
                            logger.warning(f"HTTP error editing message for match {mid}: {e}")
                            continue
                
                elif status == "finished":
                    wid = details.get('winner_id')
                    w_idx = -1
                    if wid == teams[0].get('id'): w_idx = 0
                    elif wid == teams[1].get('id'): w_idx = 1
                    
                    if w_idx != -1:
                        # STRAFE API CALL
                        match_time = safe_parse_datetime(info.get('start_time'))
                        map_data = await self.get_strafe_map_data(
                            teams[0]['name'],
                            teams[1]['name'],
                            info['game_slug'],
                            match_time
                        )
                        if map_data:
                            logger.info(f"Successfully fetched {len(map_data)} maps from Strafe for match {mid}")
                        
                        await msg.delete()
                        await self.process_result(channel, info, w_idx, details, teams, map_data)
                        to_remove.append(mid)
                    elif details.get('draw') or status == "canceled":
                        to_remove.append(mid)

            if to_remove:
                async with self.data_lock:
                    d = load_data_sync()
                    for m in to_remove: d["active_matches"].pop(m, None)
                    save_data_sync(d)

        except Exception as e: await self.report_error(f"Tracker Loop: {e}")

    @match_tracker.before_loop
    async def before_match_tracker(self):
        await self.bot.wait_until_ready()
        self.processing_matches.clear()
        data = load_data_sync()
        for mid, info in data.get("active_matches", {}).items():
            t = info.get('teams', [])
            if len(t) >= 2: self.bot.add_view(PredictionView(mid, t[0], t[1]))

    # --- ADMIN / TEST ---
    async def test_strafe_direct(self, interaction: discord.Interaction):
        """Test by fetching a match directly from Strafe, bypassing PandaScore entirely."""
        data = load_data_sync()
        cid = data.get("channel_id")
        if not cid:
            return await interaction.response.send_message("❌ No channel set.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        # Cycle through games
        slug = list(GAMES.keys())[self.test_game_idx % len(GAMES)]
        self.test_game_idx += 1
        strafe_slug = STRAFE_GAME_SLUGS.get(slug)

        if not strafe_slug:
            return await interaction.followup.send(f"❌ No Strafe support for {GAMES[slug]}")

        try:
            # Fetch directly from Strafe
            url = f"https://www.strafe.com/{strafe_slug}"
            session = await self.strafe._get_session()

            async with session.get(url) as resp:
                if resp.status != 200:
                    return await interaction.followup.send(f"❌ Strafe returned {resp.status}")

                html = await resp.text()

            soup = BeautifulSoup(html, 'html.parser')
            script = soup.find('script', id='__NEXT_DATA__')
            if not script:
                return await interaction.followup.send("❌ No __NEXT_DATA__ found on Strafe")

            page_data = json.loads(script.string)
            page_props = page_data.get('props', {}).get('pageProps', {})
            latest_results = page_props.get('latest_results', [])

            if not latest_results:
                return await interaction.followup.send(f"❌ No recent results on Strafe for {GAMES[slug]}")

            # Pick the first finished match
            match = latest_results[0]
            match_slug = match.get('slug')
            competitors = match.get('competitors', [])

            if len(competitors) < 2:
                return await interaction.followup.send("❌ Match has fewer than 2 competitors")

            # Extract team names
            team_a_name = competitors[0].get('name', '') or competitors[0].get('competitor', {}).get('name', 'Team A')
            team_b_name = competitors[1].get('name', '') or competitors[1].get('competitor', {}).get('name', 'Team B')

            # Get team images from competitor data
            team_a_img = competitors[0].get('competitor', {}).get('picture') or competitors[0].get('picture')
            team_b_img = competitors[1].get('competitor', {}).get('picture') or competitors[1].get('picture')

            # Determine winner from result_enum
            winner_idx = 0
            for i, comp in enumerate(competitors):
                if comp.get('result_enum', '').upper() == 'WIN':
                    winner_idx = i
                    break

            # Get map data from the match page
            map_data = await self.strafe.get_map_scores(match_slug, team_a_name, team_b_name, slug)

            # Build team dicts (minimal, just for display)
            ta = {
                "name": team_a_name,
                "acronym": team_a_name[:4].upper(),
                "id": 0,
                "roster": [],
                "flag": None,
                "image_url": team_a_img
            }
            tb = {
                "name": team_b_name,
                "acronym": team_b_name[:4].upper(),
                "id": 1,
                "roster": [],
                "flag": None,
                "image_url": team_b_img
            }

            # Build a minimal details dict
            details = {
                "status": "finished",
                "begin_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "winner_id": ta['id'] if winner_idx == 0 else tb['id'],
                "name": f"{team_a_name} vs {team_b_name}",
                "league": {"name": "Strafe Direct Test"},
                "serie": {"full_name": ""},
                "number_of_games": len(map_data) if map_data else 0
            }

            # Send the result embed
            chan = self.bot.get_channel(cid)
            e = await self.build_result_embed(
                interaction.channel, slug, details, ta, tb, winner_idx,
                {str(interaction.user.id): winner_idx},
                "**1. TestUser**: 10W 0L",
                is_test=True, map_data=map_data
            )
            await chan.send(embed=e)

            if map_data:
                await interaction.followup.send(
                    f"✅ **Strafe Direct Test** for **{GAMES[slug]}**\n"
                    f"Match: **{team_a_name}** vs **{team_b_name}**\n"
                    f"Maps: {len(map_data)} ({', '.join(m['name'] for m in map_data)})\n"
                    f"Slug: `{match_slug}`"
                )
            else:
                await interaction.followup.send(
                    f"⚠️ **Strafe Direct Test** for **{GAMES[slug]}**\n"
                    f"Match: **{team_a_name}** vs **{team_b_name}**\n"
                    f"**No map data available** - Match History section hidden\n"
                    f"Slug: `{match_slug}`"
                )

        except Exception as e:
            logger.error(f"Strafe direct test error: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            await interaction.followup.send(f"❌ Error: {e}")

    async def debug_strafe(self, interaction: discord.Interaction):
        """Debug Strafe API by fetching a recent finished match and showing raw scrape results."""
        await interaction.response.defer(ephemeral=True)

        slug = list(GAMES.keys())[self.test_game_idx % len(GAMES)]
        self.test_game_idx += 1

        # Get a recently finished match
        matches = await self.get_pandascore_data(
            f"/{slug}/matches",
            params={"sort": "-end_at", "page[size]": 10, "filter[status]": "finished"}
        )

        if not matches:
            return await interaction.followup.send(f"❌ No finished matches found for {GAMES[slug]}.")

        real = next((m for m in matches if len(m.get('opponents', [])) >= 2), None)
        if not real:
            return await interaction.followup.send(f"❌ No valid match found for {GAMES[slug]}.")

        ta_name = real['opponents'][0]['opponent']['name']
        tb_name = real['opponents'][1]['opponent']['name']
        match_time = safe_parse_datetime(real.get('begin_at'))

        debug_lines = [
            f"**Debug Strafe Scrape for {GAMES[slug]}**",
            f"Match: **{ta_name}** vs **{tb_name}**",
            f"PandaScore ID: `{real['id']}`",
            f"Match Time: {match_time.isoformat() if match_time else 'Unknown'}",
            ""
        ]

        # Show which URLs will be tried
        strafe_slug = STRAFE_GAME_SLUGS.get(slug)
        calendar_path = STRAFE_GAME_PATHS.get(slug, strafe_slug)
        date_str = match_time.strftime("%Y-%m-%d") if match_time else "N/A"

        debug_lines.append(f"**URLs to try:**")
        debug_lines.append(f"  1. https://www.strafe.com/{strafe_slug} (main + latest_results)")
        debug_lines.append(f"  2. https://www.strafe.com/calendar/{calendar_path}/completed/")
        debug_lines.append(f"  3. https://www.strafe.com/calendar/{calendar_path}/?date={date_str}")
        debug_lines.append("")

        # Do a detailed search with verbose output
        strafe_slug = STRAFE_GAME_SLUGS.get(slug)
        calendar_path = STRAFE_GAME_PATHS.get(slug, strafe_slug)

        urls_to_try = [
            f"https://www.strafe.com/{strafe_slug}",
            f"https://www.strafe.com/calendar/{calendar_path}/completed/",
        ]
        if match_time:
            urls_to_try.append(f"https://www.strafe.com/calendar/{calendar_path}/?date={date_str}")

        all_matches = []
        session = await self.strafe._get_session()

        for url in urls_to_try:
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        debug_lines.append(f"❌ {url} returned {resp.status}")
                        continue

                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    script = soup.find('script', id='__NEXT_DATA__')
                    if not script:
                        debug_lines.append(f"❌ {url} - no __NEXT_DATA__")
                        continue

                    data = json.loads(script.string)
                    page_props = data.get('props', {}).get('pageProps', {})

                    page_matches = []

                    def extract_matches(obj, found_list):
                        if isinstance(obj, dict):
                            if 'competitors' in obj and 'slug' in obj and len(obj.get('competitors', [])) >= 2:
                                found_list.append(obj)
                            for v in obj.values():
                                extract_matches(v, found_list)
                        elif isinstance(obj, list):
                            for item in obj:
                                extract_matches(item, found_list)

                    extract_matches(page_props, page_matches)
                    debug_lines.append(f"✅ {url.split('strafe.com')[1]} - found {len(page_matches)} matches")
                    all_matches.extend(page_matches)

            except Exception as e:
                debug_lines.append(f"❌ {url} - error: {e}")

        debug_lines.append(f"\n**Total matches found: {len(all_matches)}**")

        # Show top 5 potential matches with similarity scores
        if all_matches:
            debug_lines.append(f"\n**Searching for: {ta_name} vs {tb_name}**")
            debug_lines.append("Top candidates:")

            candidates = []
            for m in all_matches:
                competitors = m.get('competitors', [])
                if len(competitors) < 2:
                    continue

                home_name = ''
                away_name = ''
                for comp in competitors:
                    side = comp.get('side_enum', '').upper()
                    name = comp.get('name', '')
                    if not name and 'competitor' in comp:
                        name = comp.get('competitor', {}).get('name', '')
                    if side == 'HOME':
                        home_name = name
                    elif side == 'AWAY':
                        away_name = name

                if not home_name and not away_name and len(competitors) >= 2:
                    home_name = competitors[0].get('name', '') or competitors[0].get('competitor', {}).get('name', '')
                    away_name = competitors[1].get('name', '') or competitors[1].get('competitor', {}).get('name', '')

                if home_name and away_name:
                    sim_a_home = SequenceMatcher(None, ta_name.lower(), home_name.lower()).ratio()
                    sim_b_away = SequenceMatcher(None, tb_name.lower(), away_name.lower()).ratio()
                    sim_a_away = SequenceMatcher(None, ta_name.lower(), away_name.lower()).ratio()
                    sim_b_home = SequenceMatcher(None, tb_name.lower(), home_name.lower()).ratio()
                    score = max((sim_a_home + sim_b_away) / 2, (sim_a_away + sim_b_home) / 2)
                    candidates.append((score, home_name, away_name, m.get('slug', '?')[:40]))

            candidates.sort(reverse=True)
            for score, h, a, s in candidates[:5]:
                marker = "✅" if score > 0.4 else "❌"
                debug_lines.append(f"  {marker} {score:.2f}: {h} vs {a}")
                debug_lines.append(f"      slug: {s}...")

        # Now do the actual search
        strafe_match = await self.strafe.find_strafe_match(ta_name, tb_name, slug, match_time)

        if strafe_match:
            match_slug = strafe_match.get('slug', 'N/A')
            debug_lines.append(f"✅ Strafe match found: `{match_slug}`")
            debug_lines.append(f"Strafe URL: https://www.strafe.com/match/{match_slug}/")

            # Step 2: Get map scores
            map_data = await self.strafe.get_map_scores(match_slug, ta_name, tb_name, slug)

            if map_data:
                debug_lines.append(f"\n✅ **Maps scraped: {len(map_data)}**")
                for i, m in enumerate(map_data, 1):
                    debug_lines.append(
                        f"  {i}. {m.get('name', '?')}: {m.get('score_a', 0)}-{m.get('score_b', 0)} "
                        f"(winner: {m.get('winner', -1)}, status: {m.get('status', '?')})"
                    )
            else:
                debug_lines.append("\n❌ **No map data returned from Strafe**")

                # Try alternate scraping approach for debugging
                url = f"https://www.strafe.com/match/{match_slug}/"
                debug_lines.append(f"\nAttempting direct page scrape of: {url}")

                try:
                    session = await self.strafe._get_session()
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            script = soup.find('script', id='__NEXT_DATA__')
                            if script:
                                data = json.loads(script.string)
                                legacy = data.get('props', {}).get('pageProps', {}).get('legacyMatch', {})
                                live_data = legacy.get('live', [])
                                debug_lines.append(f"Found __NEXT_DATA__, legacyMatch.live has {len(live_data)} items")

                                # Show structure of first few items
                                for idx, item in enumerate(live_data[:5]):
                                    item_data = item.get('data', {})
                                    keys = list(item_data.keys())[:8]
                                    debug_lines.append(f"  Item {idx}: keys={keys}")
                            else:
                                debug_lines.append("No __NEXT_DATA__ script found on page")
                        else:
                            debug_lines.append(f"Page returned status {resp.status}")
                except Exception as e:
                    debug_lines.append(f"Direct scrape error: {e}")
        else:
            debug_lines.append("❌ **Strafe match NOT found**")

            # Show what matches Strafe has for this game
            try:
                url = f"https://www.strafe.com/{strafe_slug}"
                session = await self.strafe._get_session()
                async with session.get(url) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        script = soup.find('script', id='__NEXT_DATA__')
                        if script:
                            data = json.loads(script.string)
                            page_props = data.get('props', {}).get('pageProps', {})

                            # Find all matches recursively
                            found_matches = []

                            def extract_matches(obj):
                                if isinstance(obj, dict):
                                    if 'competitors' in obj and 'slug' in obj and len(obj.get('competitors', [])) >= 2:
                                        found_matches.append(obj)
                                    for v in obj.values():
                                        extract_matches(v)
                                elif isinstance(obj, list):
                                    for item in obj:
                                        extract_matches(item)

                            extract_matches(page_props)
                            debug_lines.append(f"\nStrafe shows {len(found_matches)} matches on {strafe_slug} page")

                            # Show recent matches for comparison
                            debug_lines.append("\nRecent Strafe matches:")
                            for m in found_matches[:5]:
                                comps = m.get('competitors', [])
                                if len(comps) >= 2:
                                    h = comps[0].get('name', comps[0].get('competitor', {}).get('name', '?'))
                                    a = comps[1].get('name', comps[1].get('competitor', {}).get('name', '?'))
                                    s = m.get('slug', '?')[:30]
                                    debug_lines.append(f"  • {h} vs {a} ({s}...)")
            except Exception as e:
                debug_lines.append(f"Error listing Strafe matches: {e}")

        # Send debug output
        output = "\n".join(debug_lines)
        if len(output) > 1900:
            file_data = BytesIO(output.encode('utf-8'))
            await interaction.followup.send(
                f"Debug output for **{GAMES[slug]}** (see file):",
                file=discord.File(file_data, filename="strafe_debug.txt")
            )
        else:
            await interaction.followup.send(output)

    async def run_test(self, interaction, is_result):
        data = load_data_sync()
        cid = data.get("channel_id")
        if not cid: return await interaction.response.send_message("❌ No channel set.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        slug = list(GAMES.keys())[self.test_game_idx % len(GAMES)]
        self.test_game_idx += 1

        matches = await self.get_pandascore_data(f"/{slug}/matches", params={"sort": "-begin_at", "page[size]": 30, "filter[status]": "finished"})
        valid_matches = [m for m in matches if len(m.get('opponents', [])) >= 2]
        if not valid_matches: return await interaction.followup.send("⚠️ No test data found.")

        chan = self.bot.get_channel(cid)

        if is_result:
            # For result tests, try multiple matches until we find one with Strafe data
            map_data = None
            used_match = None
            tried_teams = []

            for candidate in valid_matches[:10]:  # Try up to 10 matches
                ta_base = candidate['opponents'][0]['opponent']
                tb_base = candidate['opponents'][1]['opponent']
                match_time = safe_parse_datetime(candidate.get('begin_at'))

                # Try Strafe scrape for this match
                candidate_map_data = await self.get_strafe_map_data(ta_base['name'], tb_base['name'], slug, match_time)

                if candidate_map_data:
                    map_data = candidate_map_data
                    used_match = candidate
                    logger.info(f"Test result: Found Strafe data for {ta_base['name']} vs {tb_base['name']} ({len(map_data)} maps)")
                    break
                else:
                    tried_teams.append(f"{ta_base['name']} vs {tb_base['name']}")

            # Use whichever match we found, or fall back to first valid match
            real = used_match or valid_matches[0]
            ta_base, tb_base = real['opponents'][0]['opponent'], real['opponents'][1]['opponent']

            ta = {
                "name": ta_base['name'], "acronym": ta_base.get('acronym'), "id": ta_base['id'],
                "roster": await self.fetch_roster(ta_base['id'], ta_base['name'], slug),
                "flag": ta_base.get('location'), "image_url": ta_base.get('image_url')
            }
            tb = {
                "name": tb_base['name'], "acronym": tb_base.get('acronym'), "id": tb_base['id'],
                "roster": await self.fetch_roster(tb_base['id'], tb_base['name'], slug),
                "flag": tb_base.get('location'), "image_url": tb_base.get('image_url')
            }

            details = real.copy()
            details['status'] = "finished"
            details['begin_at'] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)).isoformat()
            details['winner_id'] = ta['id']

            # Don't use mock data - show the embed as it would appear in live results
            # If no Strafe data, the Match History section will be hidden (which is the expected behavior)
            e = await self.build_result_embed(interaction.channel, slug, details, ta, tb, 0, {str(interaction.user.id): 0}, "**1. TestUser**: 10W 0L", is_test=True, map_data=map_data)
            await chan.send(embed=e)

            if map_data:
                await interaction.followup.send(f"✅ Test sent for **{GAMES[slug]}** with **{len(map_data)} maps** from Strafe - {ta['name']} vs {tb['name']}")
            else:
                logger.warning(f"Test result: No Strafe data found after trying: {tried_teams}")
                await interaction.followup.send(
                    f"⚠️ Test sent for **{GAMES[slug]}** - **No map data available**\n"
                    f"Match History section hidden (this is the expected fallback behavior)\n"
                    f"Tried {len(tried_teams)} matches: {', '.join(tried_teams[:3])}{'...' if len(tried_teams) > 3 else ''}"
                )
        else:
            # For upcoming match tests, just use the first valid match
            real = valid_matches[0]
            ta_base, tb_base = real['opponents'][0]['opponent'], real['opponents'][1]['opponent']

            ta = {
                "name": ta_base['name'], "acronym": ta_base.get('acronym'), "id": ta_base['id'],
                "roster": await self.fetch_roster(ta_base['id'], ta_base['name'], slug),
                "flag": ta_base.get('location'), "image_url": ta_base.get('image_url')
            }
            tb = {
                "name": tb_base['name'], "acronym": tb_base.get('acronym'), "id": tb_base['id'],
                "roster": await self.fetch_roster(tb_base['id'], tb_base['name'], slug),
                "flag": tb_base.get('location'), "image_url": tb_base.get('image_url')
            }

            details = real.copy()
            details['status'] = "not_started"
            details['begin_at'] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)).isoformat()

            mid = f"test_{secrets.token_hex(4)}"
            f = await self.generate_banner(ta['image_url'], tb['image_url'], GAME_LOGOS[slug], slug)
            e = self.build_match_embed(slug, GAMES[slug], details, ta, tb, {}, None, f is not None)
            msg = await chan.send(embed=e, file=f, view=PredictionView(mid, ta, tb))

            async with self.data_lock:
                d = load_data_sync()
                d["active_matches"][mid] = {
                    "message_id": msg.id, "channel_id": cid, "game_slug": slug,
                    "start_time": details['begin_at'], "teams": [ta, tb], "votes": {},
                    "is_test": True, "status": "active"
                }
                save_data_sync(d)

            await interaction.followup.send(f"✅ Test sent for {GAMES[slug]}")

    @app_commands.command(name="esports_admin")
    @app_commands.default_permissions(administrator=True)
    async def admin_panel(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=discord.Embed(title="🎮 Admin Panel", color=discord.Color.dark_grey()), view=EsportsAdminView(self), ephemeral=True)

    @app_commands.command(name="esports_leaderboard")
    async def leaderboard(self, interaction: discord.Interaction):
        embed = await self.generate_leaderboard_embed(interaction.guild, "valorant")
        await interaction.response.send_message(embed=embed, view=LeaderboardView(self, interaction.user.id))

async def setup(bot):
    await bot.add_cog(Esports(bot))


