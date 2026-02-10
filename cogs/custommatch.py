"""
Custom Matches Cog
A comprehensive queue and match management system for custom games.
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple
from pathlib import Path
import random
import secrets
from dataclasses import dataclass, field
from enum import Enum
import io
import json

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logger = logging.getLogger('custommatch')
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# =============================================================================
# CONSTANTS & ENUMS
# =============================================================================

class QueueType(Enum):
    MMR = "mmr"
    CAPTAINS = "captains"
    RANDOM = "random"

class CaptainSelection(Enum):
    RANDOM = "random"
    ADMIN = "admin"
    HIGHEST_MMR = "highest_mmr"

class Team(Enum):
    RED = "red"
    BLUE = "blue"

# K-Factor settings
K_FACTOR_NEWBIE = 50      # Games 1-5
K_FACTOR_LEARNING = 35    # Games 6-15
K_FACTOR_STABLE = 20      # Games 16+

def generate_short_id() -> str:
    """Generate a 5-character alphanumeric ID without confusing chars."""
    chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ123456789'  # No 0, O, I, L
    return ''.join(secrets.choice(chars) for _ in range(5))


def parse_duration_to_minutes(value: str) -> int:
    """Parse duration string to minutes. Supports: 60, 60m, 2h, 1d"""
    value = value.strip().lower()
    if value.endswith('d'):
        return int(value[:-1]) * 1440  # days to minutes
    elif value.endswith('h'):
        return int(value[:-1]) * 60    # hours to minutes
    elif value.endswith('m'):
        return int(value[:-1])         # already minutes
    else:
        return int(value)              # assume minutes

# Thresholds
NEWBIE_GAMES = 5
LEARNING_GAMES = 15
RIVALRY_MIN_GAMES = 5

# Colors (all white for consistent styling)
COLOR_WHITE = 0xFFFFFF
COLOR_RED = COLOR_WHITE
COLOR_BLUE = COLOR_WHITE
COLOR_NEUTRAL = COLOR_WHITE
COLOR_SUCCESS = COLOR_WHITE
COLOR_WARNING = COLOR_WHITE

# Stats card template paths
STATS_TEMPLATE_PATH = Path(__file__).parent / "templates" / "stats_card.html"
MATCH_TEMPLATE_PATH = Path(__file__).parent / "templates" / "match_card.html"
SCOREBOARD_TEMPLATE_PATH = Path(__file__).parent / "templates" / "scoreboard_card.html"
LEADERBOARD_TEMPLATE_PATH = Path(__file__).parent / "templates" / "leaderboard_card.html"
FONTS_PATH = Path(__file__).parent.parent / "fonts"

# =============================================================================
# STATS CARD GENERATOR
# =============================================================================

class StatsCardGenerator:
    """Generates stats card images using Playwright and HTML templates."""

    def __init__(self):
        self.browser = None
        self.playwright = None

    async def initialize(self):
        """Initialize the browser."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not available. Stats cards will use embeds.")
            return False

        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch()
            logger.info("Stats card generator initialized.")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize stats card generator: {e}")
            return False

    async def close(self):
        """Close the browser."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def generate_stats_image(self, player_data: dict) -> Optional[io.BytesIO]:
        """Generate a stats card image from player data."""
        if not self.browser:
            return None

        try:
            # Load template
            if not STATS_TEMPLATE_PATH.exists():
                logger.error(f"Stats template not found at {STATS_TEMPLATE_PATH}")
                return None

            template = STATS_TEMPLATE_PATH.read_text(encoding='utf-8')

            # Calculate values
            games_played = player_data.get('games_played', 0)
            wins = player_data.get('wins', 0)
            losses = player_data.get('losses', 0)
            winrate = round((wins / games_played * 100)) if games_played > 0 else 0

            # Calculate split bar widths for wins/losses
            total_games = wins + losses
            win_width = round((wins / total_games * 100)) if total_games > 0 else 50
            loss_width = 100 - win_width

            total_kills = player_data.get('total_kills', 0)
            total_deaths = player_data.get('total_deaths', 0)
            total_assists = player_data.get('total_assists', 0)
            total_score = player_data.get('total_score', 0)
            hs_percent = player_data.get('hs_percent', 0)
            total_damage = player_data.get('total_damage', 0)
            total_first_bloods = player_data.get('total_first_bloods', 0)

            # Calculate split bar widths for kills/deaths
            total_kd = total_kills + total_deaths
            kill_width = round((total_kills / total_kd * 100)) if total_kd > 0 else 50
            death_width = 100 - kill_width

            kd_ratio = round(total_kills / total_deaths, 2) if total_deaths > 0 else total_kills
            avg_score = round(total_score / games_played) if games_played > 0 else 0

            # Calculate ADR (Average Damage per Round) - estimate ~24 rounds per game
            total_rounds = games_played * 24  # Approximate
            adr = round(total_damage / total_rounds) if total_rounds > 0 else 0

            # Get streak data
            longest_win_streak = player_data.get('longest_win_streak', 0)
            longest_loss_streak = player_data.get('longest_loss_streak', 0)

            # Build stats list HTML (for middle section Performance box)
            stats_list_html = f'''
                <div class="stat-row"><span class="stat-name">Assists</span><span class="stat-val">{total_assists}</span></div>
                <div class="stat-row"><span class="stat-name">Avg Score</span><span class="stat-val">{avg_score}</span></div>
                <div class="stat-row"><span class="stat-name">Win Streak</span><span class="stat-val">{longest_win_streak}</span></div>
                <div class="stat-row"><span class="stat-name">Loss Streak</span><span class="stat-val">{longest_loss_streak}</span></div>'''

            # Build teammates HTML (two separate columns for the grid)
            best_teammates = player_data.get('best_teammates', [])
            worst_teammates = player_data.get('worst_teammates', [])

            best_items = ''.join(
                f'<div class="teammate-item">{t["name"]} ({t["wins"]}-{t["losses"]})</div>'
                for t in best_teammates
            ) if best_teammates else '<div class="no-data-small">Not enough data</div>'

            worst_items = ''.join(
                f'<div class="teammate-item cursed">{t["name"]} ({t["wins"]}-{t["losses"]})</div>'
                for t in worst_teammates
            ) if worst_teammates else '<div class="no-data-small">Not enough data</div>'

            teammates_html = f'''
        <div class="teammates-column">
            <div class="section-title">Top Teammates</div>
            <div class="teammates-list">{best_items}</div>
        </div>
        <div class="teammates-column">
            <div class="section-title cursed-title">Cursed Teammates</div>
            <div class="teammates-list">{worst_items}</div>
        </div>'''

            # Build maps HTML (all maps) - using data-box format
            map_stats = player_data.get('map_stats', [])
            if map_stats:
                maps_items = ''.join(
                    f'<div class="data-item">{m["name"]} ({m["wins"]}-{m["losses"]})</div>'
                    for m in map_stats
                )
                maps_html = f'''
        <div class="data-box">
            <div class="section-title">Map W/L</div>
            <div class="items-grid">{maps_items}</div>
        </div>'''
            else:
                maps_html = '<div class="data-box"><div class="section-title">Map W/L</div><div class="no-data-small">No data</div></div>'

            # Build agents HTML with W-L format (top 10) - using data-box format
            agent_stats = player_data.get('agent_stats', {})
            if agent_stats:
                sorted_agents = sorted(agent_stats.items(), key=lambda x: x[1]['games'], reverse=True)[:10]
                agents_items = ''.join(
                    f'<div class="data-item">{name} ({data["wins"]}-{data["losses"]})</div>'
                    for name, data in sorted_agents
                )
                agents_html = f'''
        <div class="data-box">
            <div class="section-title">Favorite Agents</div>
            <div class="items-grid">{agents_items}</div>
        </div>'''
            else:
                agents_html = '<div class="data-box"><div class="section-title">Favorite Agents</div><div class="no-data-small">No data</div></div>'

            # Build recent matches HTML - horizontal card layout
            recent_matches = player_data.get('recent_matches', [])
            if recent_matches:
                matches_html = ""
                for match in recent_matches[:5]:
                    result_class = "win" if match.get('won') else "loss"
                    result_letter = "W" if match.get('won') else "L"
                    map_text = match.get('map_name', '') or "?"
                    agent_text = match.get('agent', '') or ""
                    kda_text = ""
                    if match.get('kills') is not None:
                        kda_text = f"{match['kills']}/{match['deaths']}/{match['assists']}"

                    matches_html += f'''
                <div class="match-card {result_class}">
                    <div class="match-result-indicator {result_class}">{result_letter}</div>
                    <div class="match-map">{map_text}</div>
                    <div class="match-agent">{agent_text}</div>
                    <div class="match-kda">{kda_text}</div>
                </div>'''
                recent_matches_html = f'<div class="match-grid">{matches_html}</div>'
            else:
                recent_matches_html = '<div class="no-data">No recent matches</div>'

            # Truncate player name if too long
            player_name = player_data.get('player_name', 'Unknown')
            if len(player_name) > 18:
                player_name = player_name[:15] + '...'

            # Build rank badge HTML
            leaderboard_rank = player_data.get('leaderboard_rank')
            if leaderboard_rank:
                rank_badge_html = f'<div class="rank-badge">Leaderboard #{leaderboard_rank}</div>'
            else:
                rank_badge_html = ''

            # Replace placeholders
            html = template.format(
                font_path=str(FONTS_PATH),
                avatar_url=player_data.get('avatar_url', ''),
                player_name=player_name,
                period_title=player_data.get('period_title', 'Stats'),
                rank_badge_html=rank_badge_html,
                wins=wins,
                losses=losses,
                win_width=win_width,
                loss_width=loss_width,
                winrate=winrate,
                total_kills=total_kills,
                total_deaths=total_deaths,
                kill_width=kill_width,
                death_width=death_width,
                kd_ratio=kd_ratio,
                hs_percent=round(hs_percent),
                adr=adr,
                total_first_bloods=total_first_bloods,
                stats_list_html=stats_list_html,
                teammates_html=teammates_html,
                maps_html=maps_html,
                agents_html=agents_html,
                recent_matches_html=recent_matches_html
            )

            # Render to image with 2x scale for better quality
            page = await self.browser.new_page(
                viewport={'width': 580, 'height': 500},
                device_scale_factor=2
            )
            await page.set_content(html)

            # Wait for fonts to load
            await page.wait_for_timeout(100)

            # Get the actual content height
            body_height = await page.evaluate('document.body.scrollHeight')
            await page.set_viewport_size({'width': 580, 'height': body_height + 40})

            screenshot = await page.screenshot(type='png')
            await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating stats card: {e}")
            return None

    async def generate_match_image(self, match_data: dict) -> Optional[io.BytesIO]:
        """Generate a match scoreboard image from match data."""
        if not self.browser:
            return None

        try:
            # Load match template
            if not MATCH_TEMPLATE_PATH.exists():
                logger.error(f"Match template not found at {MATCH_TEMPLATE_PATH}")
                return None

            template = MATCH_TEMPLATE_PATH.read_text(encoding='utf-8')

            # Extract data
            kills = match_data.get('kills', 0) or 0
            deaths = match_data.get('deaths', 0) or 0
            assists = match_data.get('assists', 0) or 0
            score = match_data.get('score', 0) or 0
            damage = match_data.get('damage', 0) or 0
            first_bloods = match_data.get('first_bloods', 0) or 0
            headshots = match_data.get('headshots', 0) or 0
            bodyshots = match_data.get('bodyshots', 0) or 0
            legshots = match_data.get('legshots', 0) or 0
            won = match_data.get('won', False)

            # New stats
            plants = match_data.get('plants', 0) or 0
            defuses = match_data.get('defuses', 0) or 0
            c2k = match_data.get('c2k', 0) or 0
            c3k = match_data.get('c3k', 0) or 0
            c4k = match_data.get('c4k', 0) or 0
            c5k = match_data.get('c5k', 0) or 0
            econ_spent = match_data.get('econ_spent', 0) or 0
            econ_loadout = match_data.get('econ_loadout', 0) or 0

            # Calculate derived stats
            kd_ratio = round(kills / deaths, 2) if deaths > 0 else kills
            total_shots = headshots + bodyshots + legshots
            hs_percent = round(headshots / total_shots * 100) if total_shots > 0 else 0
            bs_percent = round(bodyshots / total_shots * 100) if total_shots > 0 else 0
            ls_percent = round(legshots / total_shots * 100) if total_shots > 0 else 0

            # Estimate ~24 rounds per match for ADR
            adr = round(damage / 24) if damage > 0 else 0
            # ACS = score / rounds (estimate 24 rounds)
            acs = round(score / 24) if score > 0 else 0
            # Average loadout value per round
            econ_rating = round(econ_loadout / 24) if econ_loadout > 0 else 0

            # Calculate body part colors (red gradient - darker = higher %)
            # Base color is a dark red, intensity increases with percentage
            def get_body_color(percent):
                # Range from #3d1a1a (low) to #ff4d4d (high)
                # Interpolate based on percentage
                min_r, min_g, min_b = 61, 26, 26  # Dark red
                max_r, max_g, max_b = 255, 77, 77  # Bright red
                factor = percent / 100
                r = int(min_r + (max_r - min_r) * factor)
                g = int(min_g + (max_g - min_g) * factor)
                b = int(min_b + (max_b - min_b) * factor)
                return f"#{r:02x}{g:02x}{b:02x}"

            head_color = get_body_color(hs_percent)
            body_color = get_body_color(bs_percent)
            legs_color = get_body_color(ls_percent)

            # Build multi-kills HTML
            multikills = []
            if c2k > 0:
                multikills.append(f'<div class="multi-kill">2K: {c2k}</div>')
            if c3k > 0:
                multikills.append(f'<div class="multi-kill">3K: {c3k}</div>')
            if c4k > 0:
                multikills.append(f'<div class="multi-kill highlight">4K: {c4k}</div>')
            if c5k > 0:
                multikills.append(f'<div class="multi-kill highlight">ACE: {c5k}</div>')
            if not multikills:
                multikills.append('<div class="multi-kill">None</div>')
            multikills_html = ''.join(multikills)

            # Truncate player name if too long
            player_name = match_data.get('player_name', 'Unknown')
            if len(player_name) > 18:
                player_name = player_name[:15] + '...'

            # Replace placeholders
            html = template.format(
                font_path=str(FONTS_PATH),
                avatar_url=match_data.get('avatar_url', ''),
                player_name=player_name,
                map_name=match_data.get('map_name', 'Unknown'),
                agent=match_data.get('agent', 'Unknown'),
                result_class='victory' if won else 'defeat',
                result_text='VICTORY' if won else 'DEFEAT',
                kills=kills,
                deaths=deaths,
                assists=assists,
                kd_ratio=kd_ratio,
                hs_percent=hs_percent,
                bs_percent=bs_percent,
                ls_percent=ls_percent,
                adr=adr,
                acs=acs,
                first_bloods=first_bloods,
                damage=damage,
                head_color=head_color,
                body_color=body_color,
                legs_color=legs_color,
                plants=plants,
                defuses=defuses,
                multikills_html=multikills_html,
                econ_rating=econ_rating
            )

            # Render to image
            page = await self.browser.new_page(
                viewport={'width': 520, 'height': 500},
                device_scale_factor=2
            )
            await page.set_content(html)

            # Wait for fonts to load
            await page.wait_for_timeout(100)

            # Get the actual content height
            body_height = await page.evaluate('document.body.scrollHeight')
            await page.set_viewport_size({'width': 520, 'height': body_height + 20})

            screenshot = await page.screenshot(type='png')
            await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating match card: {e}")
            return None

    async def generate_scoreboard_image(self, scoreboard_data: dict) -> Optional[io.BytesIO]:
        """Generate a full match scoreboard image showing all 10 players."""
        if not self.browser:
            return None

        try:
            # Load scoreboard template
            if not SCOREBOARD_TEMPLATE_PATH.exists():
                logger.error(f"Scoreboard template not found at {SCOREBOARD_TEMPLATE_PATH}")
                return None

            template = SCOREBOARD_TEMPLATE_PATH.read_text(encoding='utf-8')

            map_name = scoreboard_data.get('map_name', 'Unknown')
            winner_team_name = scoreboard_data.get('winner_team_name', 'Winners')
            loser_team_name = scoreboard_data.get('loser_team_name', 'Losers')
            winners = scoreboard_data.get('winners', [])
            losers = scoreboard_data.get('losers', [])

            def build_player_row(player: dict, is_mvp: bool = False) -> str:
                name = player.get('name', 'Unknown')
                if len(name) > 16:
                    name = name[:13] + '...'
                agent = player.get('agent', '?')
                if len(agent) > 10:
                    agent = agent[:8] + '..'
                kills = player.get('kills', 0) or 0
                deaths = player.get('deaths', 0) or 0
                assists = player.get('assists', 0) or 0
                kd = round(kills / deaths, 2) if deaths > 0 else kills

                # Calculate HS%
                headshots = player.get('headshots', 0) or 0
                bodyshots = player.get('bodyshots', 0) or 0
                legshots = player.get('legshots', 0) or 0
                total_shots = headshots + bodyshots + legshots
                hs_percent = round(headshots / total_shots * 100) if total_shots > 0 else 0

                # Calculate ADR and ACS (estimate 24 rounds)
                damage = player.get('damage', 0) or 0
                score = player.get('score', 0) or 0
                adr = round(damage / 24) if damage > 0 else 0
                acs = round(score / 24) if score > 0 else 0

                first_bloods = player.get('first_bloods', 0) or 0

                mvp_html = '<span class="mvp-badge">MVP</span>' if is_mvp else ''

                return f'''
                <div class="player-row">
                    <div class="player-info">
                        <span class="player-name">{name}{mvp_html}</span>
                    </div>
                    <span class="agent-name">{agent}</span>
                    <span class="kda-cell">
                        <span class="kda-kills">{kills}</span>
                        <span class="kda-slash">/</span>
                        <span class="kda-deaths">{deaths}</span>
                        <span class="kda-slash">/</span>
                        <span class="kda-assists">{assists}</span>
                    </span>
                    <span class="stat-cell">{kd}</span>
                    <span class="stat-cell">{hs_percent}%</span>
                    <span class="stat-cell">{adr}</span>
                    <span class="stat-cell">{acs}</span>
                    <span class="stat-cell">{first_bloods}</span>
                    <span class="stat-cell">{damage}</span>
                </div>'''

            # Sort players by ACS (score/24) for MVP detection
            def get_acs(p):
                return (p.get('score', 0) or 0) / 24

            sorted_winners = sorted(winners, key=get_acs, reverse=True)
            sorted_losers = sorted(losers, key=get_acs, reverse=True)

            # Build player rows - top player on winning team is MVP
            winner_rows = []
            for i, player in enumerate(sorted_winners):
                winner_rows.append(build_player_row(player, is_mvp=(i == 0)))

            loser_rows = []
            for player in sorted_losers:
                loser_rows.append(build_player_row(player, is_mvp=False))

            html = template.format(
                font_path=str(FONTS_PATH),
                map_name=map_name,
                winner_team_name=winner_team_name,
                loser_team_name=loser_team_name,
                winner_players_html=''.join(winner_rows),
                loser_players_html=''.join(loser_rows)
            )

            # Render to image
            page = await self.browser.new_page(
                viewport={'width': 750, 'height': 600},
                device_scale_factor=2
            )
            await page.set_content(html)
            await page.wait_for_timeout(100)

            body_height = await page.evaluate('document.body.scrollHeight')
            await page.set_viewport_size({'width': 750, 'height': body_height + 20})

            screenshot = await page.screenshot(type='png')
            await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating scoreboard card: {e}")
            return None

    async def generate_leaderboard_image(self, leaderboard_data: dict) -> Optional[io.BytesIO]:
        """Generate a leaderboard image with two columns (1-10 and 11-20)."""
        if not self.browser:
            return None

        try:
            if not LEADERBOARD_TEMPLATE_PATH.exists():
                logger.error(f"Leaderboard template not found at {LEADERBOARD_TEMPLATE_PATH}")
                return None

            template = LEADERBOARD_TEMPLATE_PATH.read_text(encoding='utf-8')

            title = leaderboard_data.get('title', 'Leaderboard')
            subtitle = leaderboard_data.get('subtitle', '')
            entries = leaderboard_data.get('entries', [])

            def generate_rows(entries_subset, start_rank):
                """Generate HTML rows for a subset of entries."""
                rows = ""
                for i, entry in enumerate(entries_subset):
                    rank = start_rank + i
                    name = entry.get('name', 'Unknown')
                    if len(name) > 14:
                        name = name[:12] + '..'
                    wins = entry.get('wins', 0)
                    losses = entry.get('losses', 0)
                    total = wins + losses
                    winrate = round((wins / total * 100)) if total > 0 else 0

                    # Determine row classes
                    classes = ['player-row']
                    if i % 2 == 1:
                        classes.append('shaded')
                    if rank <= 3:
                        classes.append('top-3')
                        classes.append(f'rank-{rank}')

                    rows += f'''
                    <div class="{' '.join(classes)}">
                        <span class="rank">{rank}</span>
                        <span class="player-name">{name}</span>
                        <span class="stat stat-wins">{wins}</span>
                        <span class="stat stat-losses">{losses}</span>
                        <span class="stat stat-winrate">{winrate}%</span>
                    </div>'''
                return rows

            if not entries:
                left_rows_html = '<div class="no-data">No matches played yet.</div>'
                right_rows_html = ''
            else:
                # Split entries: 1-10 on left, 11-20 on right
                left_entries = entries[:10]
                right_entries = entries[10:20]

                left_rows_html = generate_rows(left_entries, 1)
                right_rows_html = generate_rows(right_entries, 11) if right_entries else ''

            html = template.format(
                font_path=str(FONTS_PATH),
                title=title,
                subtitle=subtitle,
                left_rows_html=left_rows_html,
                right_rows_html=right_rows_html
            )

            # Render to image with higher resolution
            page = await self.browser.new_page(
                viewport={'width': 900, 'height': 600},
                device_scale_factor=3
            )
            await page.set_content(html)
            await page.wait_for_timeout(100)

            body_height = await page.evaluate('document.body.scrollHeight')
            await page.set_viewport_size({'width': 900, 'height': body_height + 20})

            screenshot = await page.screenshot(type='png')
            await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating leaderboard card: {e}")
            return None


# =============================================================================
# DATABASE SETUP
# =============================================================================

DB_PATH = Path("data/custommatch.db")

SCHEMA = """
-- Configuration
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Games
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    player_count INTEGER NOT NULL,
    queue_type TEXT NOT NULL DEFAULT 'mmr',
    captain_selection TEXT NOT NULL DEFAULT 'random',
    queue_channel_id INTEGER,
    verified_role_id INTEGER,
    ready_timer_seconds INTEGER DEFAULT 60,
    schedule_enabled INTEGER DEFAULT 0,
    schedule_open_days TEXT,
    schedule_open_time TEXT,
    schedule_close_time TEXT,
    schedule_down_message_id INTEGER
);

-- MMR roles per game
CREATE TABLE IF NOT EXISTS game_mmr_roles (
    game_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    mmr_value INTEGER NOT NULL,
    PRIMARY KEY (game_id, role_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Players
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    blacklisted_until TIMESTAMP
);

-- Player stats per game
CREATE TABLE IF NOT EXISTS player_game_stats (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    mmr INTEGER DEFAULT 1000,
    games_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    admin_offset INTEGER DEFAULT 0,
    last_played TIMESTAMP,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Matches
CREATE TABLE IF NOT EXISTS matches (
    match_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    channel_id INTEGER,
    draft_channel_id INTEGER,
    red_role_id INTEGER,
    blue_role_id INTEGER,
    queue_type TEXT NOT NULL,
    winning_team TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_at TIMESTAMP,
    cancelled INTEGER DEFAULT 0,
    queue_message_id INTEGER,
    map_name TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

-- Match players
CREATE TABLE IF NOT EXISTS match_players (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    team TEXT NOT NULL,
    was_captain INTEGER DEFAULT 0,
    was_sub INTEGER DEFAULT 0,
    original_player_id INTEGER,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Rivalries
CREATE TABLE IF NOT EXISTS rivalries (
    player_a_id INTEGER NOT NULL,
    player_b_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    player_a_wins INTEGER DEFAULT 0,
    player_b_wins INTEGER DEFAULT 0,
    PRIMARY KEY (player_a_id, player_b_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- MMR History
CREATE TABLE IF NOT EXISTS mmr_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    match_id INTEGER NOT NULL,
    mmr_before INTEGER NOT NULL,
    mmr_after INTEGER NOT NULL,
    change INTEGER NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

-- Win votes
CREATE TABLE IF NOT EXISTS win_votes (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    voted_team TEXT NOT NULL,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Active queues (in-memory tracking, but persist for recovery)
CREATE TABLE IF NOT EXISTS active_queues (
    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    message_id INTEGER,
    channel_id INTEGER,
    state TEXT DEFAULT 'waiting',
    ready_check_started TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Queue players
CREATE TABLE IF NOT EXISTS queue_players (
    queue_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_ready INTEGER DEFAULT 0,
    PRIMARY KEY (queue_id, player_id),
    FOREIGN KEY (queue_id) REFERENCES active_queues(queue_id) ON DELETE CASCADE
);

-- Store per-game IGN for players
CREATE TABLE IF NOT EXISTS player_igns (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    ign TEXT NOT NULL,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

-- Track ready-up penalty offenses with decay
CREATE TABLE IF NOT EXISTS ready_penalties (
    player_id INTEGER PRIMARY KEY,
    offense_count INTEGER DEFAULT 0,
    penalty_expires TIMESTAMP,
    last_offense TIMESTAMP
);

-- Track player suspensions (separate from blacklist)
CREATE TABLE IF NOT EXISTS suspensions (
    suspension_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    game_id INTEGER,
    suspended_until TIMESTAMP NOT NULL,
    reason TEXT,
    suspended_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track match abandon votes
CREATE TABLE IF NOT EXISTS abandon_votes (
    match_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    voted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Valorant match stats from HenrikDev API
CREATE TABLE IF NOT EXISTS valorant_match_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    valorant_match_id TEXT,
    player_id INTEGER NOT NULL,
    ign TEXT NOT NULL,
    agent TEXT,
    kills INTEGER DEFAULT 0,
    deaths INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    headshots INTEGER DEFAULT 0,
    bodyshots INTEGER DEFAULT 0,
    legshots INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    damage_dealt INTEGER DEFAULT 0,
    first_bloods INTEGER DEFAULT 0,
    plants INTEGER DEFAULT 0,
    defuses INTEGER DEFAULT 0,
    c2k INTEGER DEFAULT 0,
    c3k INTEGER DEFAULT 0,
    c4k INTEGER DEFAULT 0,
    c5k INTEGER DEFAULT 0,
    econ_spent INTEGER DEFAULT 0,
    econ_loadout INTEGER DEFAULT 0,
    map_name TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

-- Track players whose IGN works reliably for API lookups
CREATE TABLE IF NOT EXISTS valorant_player_regulars (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    ign TEXT NOT NULL,
    puuid TEXT,
    region TEXT DEFAULT 'na',
    verified_at TIMESTAMP,
    PRIMARY KEY (player_id, game_id)
);

-- Mod roles that get access to all match channels
CREATE TABLE IF NOT EXISTS mod_roles (
    role_id INTEGER PRIMARY KEY
);

-- Admin stat adjustments (tracks manual W/L changes for monthly leaderboards)
CREATE TABLE IF NOT EXISTS admin_stat_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    wins_delta INTEGER DEFAULT 0,
    losses_delta INTEGER DEFAULT 0,
    adjusted_by INTEGER,
    adjusted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);
"""

async def init_db():
    """Initialize the database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()
    await migrate_db()

async def migrate_db():
    """Run database migrations to add new columns to existing tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Get existing columns for games table
        async with db.execute("PRAGMA table_info(games)") as cursor:
            game_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to games table if they don't exist
        game_migrations = [
            ("vc_creation_enabled", "INTEGER DEFAULT 0"),
            ("queue_role_required", "INTEGER DEFAULT 1"),
            ("dm_ready_up", "INTEGER DEFAULT 0"),
            ("match_history_channel_id", "INTEGER"),
            ("queue_timeout_minutes", "INTEGER DEFAULT 0"),
            ("penalty_1st_minutes", "INTEGER DEFAULT 60"),
            ("penalty_2nd_minutes", "INTEGER DEFAULT 1440"),
            ("penalty_3rd_minutes", "INTEGER DEFAULT 10080"),
            ("penalty_decay_days", "INTEGER DEFAULT 30"),
            ("banner_url", "TEXT"),
            ("verification_topic", "TEXT"),
            ("game_channel_id", "INTEGER"),
            ("ready_loading_emoji", "TEXT DEFAULT '<a:loading:1234567890>'"),
            ("ready_done_emoji", "TEXT DEFAULT '<:check:1234567890>'"),
            ("schedule_enabled", "INTEGER DEFAULT 0"),
            ("schedule_open_days", "TEXT"),
            ("schedule_open_time", "TEXT"),
            ("schedule_close_time", "TEXT"),
            ("schedule_down_message_id", "INTEGER"),
            ("schedule_times", "TEXT"),  # JSON: {"0": {"open": "16:00", "close": "23:00"}, ...}
            ("leaderboard_channel_id", "INTEGER"),
            ("leaderboard_message_id", "INTEGER"),
        ]

        for col_name, col_def in game_migrations:
            if col_name not in game_columns:
                await db.execute(f"ALTER TABLE games ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to games table")

        # Get existing columns for matches table
        async with db.execute("PRAGMA table_info(matches)") as cursor:
            match_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to matches table if they don't exist
        match_migrations = [
            ("red_vc_id", "INTEGER"),
            ("blue_vc_id", "INTEGER"),
            ("short_id", "TEXT"),
            ("valorant_match_id", "TEXT"),
            ("map_name", "TEXT"),
        ]

        for col_name, col_def in match_migrations:
            if col_name not in match_columns:
                await db.execute(f"ALTER TABLE matches ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to matches table")

        # Get existing columns for active_queues table
        async with db.execute("PRAGMA table_info(active_queues)") as cursor:
            queue_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to active_queues table if they don't exist
        queue_migrations = [
            ("short_id", "TEXT"),
        ]

        for col_name, col_def in queue_migrations:
            if col_name not in queue_columns:
                await db.execute(f"ALTER TABLE active_queues ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to active_queues table")

        # Get existing columns for valorant_match_stats table
        async with db.execute("PRAGMA table_info(valorant_match_stats)") as cursor:
            val_columns = {row[1] for row in await cursor.fetchall()}

        # Add new columns to valorant_match_stats table if they don't exist
        val_migrations = [
            ("damage_dealt", "INTEGER DEFAULT 0"),
            ("first_bloods", "INTEGER DEFAULT 0"),
            ("plants", "INTEGER DEFAULT 0"),
            ("defuses", "INTEGER DEFAULT 0"),
            ("c2k", "INTEGER DEFAULT 0"),
            ("c3k", "INTEGER DEFAULT 0"),
            ("c4k", "INTEGER DEFAULT 0"),
            ("c5k", "INTEGER DEFAULT 0"),
            ("econ_spent", "INTEGER DEFAULT 0"),
            ("econ_loadout", "INTEGER DEFAULT 0"),
        ]

        for col_name, col_def in val_migrations:
            if col_name not in val_columns:
                await db.execute(f"ALTER TABLE valorant_match_stats ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to valorant_match_stats table")

        await db.commit()

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class GameConfig:
    game_id: int
    name: str
    player_count: int
    queue_type: QueueType
    captain_selection: CaptainSelection
    queue_channel_id: Optional[int]
    verified_role_id: Optional[int]
    ready_timer_seconds: int = 60
    vc_creation_enabled: bool = False
    queue_role_required: bool = True
    dm_ready_up: bool = False
    match_history_channel_id: Optional[int] = None
    queue_timeout_minutes: int = 0
    penalty_1st_minutes: int = 60
    penalty_2nd_minutes: int = 1440
    penalty_3rd_minutes: int = 10080
    penalty_decay_days: int = 30
    banner_url: Optional[str] = None
    verification_topic: Optional[str] = None
    game_channel_id: Optional[int] = None
    leaderboard_channel_id: Optional[int] = None
    leaderboard_message_id: Optional[int] = None
    ready_loading_emoji: str = "<a:loading:1234567890>"
    ready_done_emoji: str = "<:check:1234567890>"
    schedule_enabled: bool = False
    schedule_open_days: Optional[str] = None  # Comma-separated: "0,1,2,3,4,5,6" (Mon=0, Sun=6) - LEGACY
    schedule_open_time: Optional[str] = None  # "HH:MM" in 24h format - LEGACY
    schedule_close_time: Optional[str] = None  # "HH:MM" in 24h format - LEGACY
    schedule_down_message_id: Optional[int] = None  # Message ID of the "queue down" embed
    schedule_times: Optional[Dict[str, Dict[str, str]]] = None  # {"0": {"open": "16:00", "close": "23:00"}, ...}

@dataclass
class PlayerIGN:
    player_id: int
    game_id: int
    ign: str

@dataclass
class ReadyPenalty:
    player_id: int
    offense_count: int = 0
    penalty_expires: Optional[datetime] = None
    last_offense: Optional[datetime] = None

@dataclass
class Suspension:
    suspension_id: int
    player_id: int
    game_id: Optional[int]
    suspended_until: datetime
    reason: Optional[str]
    suspended_by: Optional[int]
    created_at: datetime

@dataclass
class PlayerStats:
    player_id: int
    game_id: int
    mmr: int = 1000
    games_played: int = 0
    wins: int = 0
    losses: int = 0
    admin_offset: int = 0
    last_played: Optional[datetime] = None
    
    @property
    def effective_mmr(self) -> int:
        return self.mmr + self.admin_offset
    
    def get_k_factor(self) -> int:
        """Get K-factor based on games played and inactivity."""
        base_k = K_FACTOR_STABLE
        if self.games_played <= NEWBIE_GAMES:
            base_k = K_FACTOR_NEWBIE
        elif self.games_played <= LEARNING_GAMES:
            base_k = K_FACTOR_LEARNING
        
        # Inactivity boost
        if self.last_played:
            days_inactive = (datetime.now(timezone.utc) - self.last_played).days
            if days_inactive >= 60:  # 2+ months
                return K_FACTOR_NEWBIE
            elif days_inactive >= 30:  # 1 month
                # Bump up one tier
                if base_k == K_FACTOR_STABLE:
                    return K_FACTOR_LEARNING
        
        return base_k

@dataclass
class QueueState:
    queue_id: int
    game_id: int
    message_id: Optional[int] = None
    channel_id: Optional[int] = None
    state: str = "waiting"  # waiting, ready_check, drafting, in_match
    players: Dict[int, bool] = field(default_factory=dict)  # player_id -> is_ready
    ready_check_started: Optional[datetime] = None
    short_id: Optional[str] = None

@dataclass
class MatchState:
    match_id: int
    game_id: int
    channel_id: Optional[int] = None
    draft_channel_id: Optional[int] = None
    red_role_id: Optional[int] = None
    blue_role_id: Optional[int] = None
    red_team: List[int] = field(default_factory=list)
    blue_team: List[int] = field(default_factory=list)
    captains: Dict[str, int] = field(default_factory=dict)  # team -> player_id
    queue_type: QueueType = QueueType.MMR

# =============================================================================
# DATABASE HELPERS
# =============================================================================

class DatabaseHelper:
    """Helper class for database operations."""
    
    @staticmethod
    async def get_config(key: str) -> Optional[str]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    
    @staticmethod
    async def set_config(key: str, value: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, value)
            )
            await db.commit()
    
    @staticmethod
    async def get_game(game_id: int) -> Optional[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM games WHERE game_id = ?", (game_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)
    
    @staticmethod
    async def get_game_by_name(name: str) -> Optional[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM games WHERE name = ?", (name,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)
    
    @staticmethod
    async def get_game_by_channel(channel_id: int) -> Optional[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM games WHERE queue_channel_id = ?", (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return DatabaseHelper._row_to_game_config(row)
    
    @staticmethod
    def _row_to_game_config(row) -> GameConfig:
        """Helper to convert a database row to GameConfig."""
        return GameConfig(
            game_id=row["game_id"],
            name=row["name"],
            player_count=row["player_count"],
            queue_type=QueueType(row["queue_type"]),
            captain_selection=CaptainSelection(row["captain_selection"]),
            queue_channel_id=row["queue_channel_id"],
            verified_role_id=row["verified_role_id"],
            ready_timer_seconds=row["ready_timer_seconds"] or 60,
            vc_creation_enabled=bool(row["vc_creation_enabled"]) if "vc_creation_enabled" in row.keys() else False,
            queue_role_required=bool(row["queue_role_required"]) if "queue_role_required" in row.keys() else True,
            dm_ready_up=bool(row["dm_ready_up"]) if "dm_ready_up" in row.keys() else False,
            match_history_channel_id=row["match_history_channel_id"] if "match_history_channel_id" in row.keys() else None,
            queue_timeout_minutes=row["queue_timeout_minutes"] if "queue_timeout_minutes" in row.keys() else 0,
            penalty_1st_minutes=row["penalty_1st_minutes"] if "penalty_1st_minutes" in row.keys() else 60,
            penalty_2nd_minutes=row["penalty_2nd_minutes"] if "penalty_2nd_minutes" in row.keys() else 1440,
            penalty_3rd_minutes=row["penalty_3rd_minutes"] if "penalty_3rd_minutes" in row.keys() else 10080,
            penalty_decay_days=row["penalty_decay_days"] if "penalty_decay_days" in row.keys() else 30,
            banner_url=row["banner_url"] if "banner_url" in row.keys() else None,
            verification_topic=row["verification_topic"] if "verification_topic" in row.keys() else None,
            game_channel_id=row["game_channel_id"] if "game_channel_id" in row.keys() else None,
            leaderboard_channel_id=row["leaderboard_channel_id"] if "leaderboard_channel_id" in row.keys() else None,
            leaderboard_message_id=row["leaderboard_message_id"] if "leaderboard_message_id" in row.keys() else None,
            ready_loading_emoji=row["ready_loading_emoji"] if "ready_loading_emoji" in row.keys() and row["ready_loading_emoji"] else "<a:loading:1234567890>",
            ready_done_emoji=row["ready_done_emoji"] if "ready_done_emoji" in row.keys() and row["ready_done_emoji"] else "<:check:1234567890>",
            schedule_enabled=bool(row["schedule_enabled"]) if "schedule_enabled" in row.keys() else False,
            schedule_open_days=row["schedule_open_days"] if "schedule_open_days" in row.keys() else None,
            schedule_open_time=row["schedule_open_time"] if "schedule_open_time" in row.keys() else None,
            schedule_close_time=row["schedule_close_time"] if "schedule_close_time" in row.keys() else None,
            schedule_down_message_id=row["schedule_down_message_id"] if "schedule_down_message_id" in row.keys() else None,
            schedule_times=json.loads(row["schedule_times"]) if "schedule_times" in row.keys() and row["schedule_times"] else None,
        )

    @staticmethod
    async def get_all_games() -> List[GameConfig]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM games") as cursor:
                rows = await cursor.fetchall()
                return [DatabaseHelper._row_to_game_config(row) for row in rows]
    
    @staticmethod
    async def add_game(name: str, player_count: int, queue_type: str = "mmr",
                       captain_selection: str = "random") -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """INSERT INTO games (name, player_count, queue_type, captain_selection)
                   VALUES (?, ?, ?, ?)""",
                (name, player_count, queue_type, captain_selection)
            )
            await db.commit()
            return cursor.lastrowid
    
    # Valid column names for games table - prevents SQL injection
    VALID_GAME_COLUMNS = {
        'name', 'player_count', 'queue_type', 'captain_selection',
        'queue_channel_id', 'verified_role_id', 'ready_timer_seconds',
        'schedule_enabled', 'schedule_open_days', 'schedule_open_time',
        'schedule_close_time', 'schedule_down_message_id', 'vc_creation_enabled',
        'queue_role_required', 'dm_ready_up', 'match_history_channel_id',
        'banner_url', 'verification_topic', 'game_channel_id',
        'leaderboard_channel_id', 'leaderboard_message_id'
    }

    VALID_MATCH_COLUMNS = {
        'channel_id', 'draft_channel_id', 'red_role_id', 'blue_role_id',
        'winning_team', 'decided_at', 'cancelled', 'queue_message_id',
        'map_name', 'red_vc_id', 'blue_vc_id', 'short_id',
        'valorant_match_id', 'queue_type'
    }

    @staticmethod
    async def update_game(game_id: int, **kwargs):
        async with aiosqlite.connect(DB_PATH) as db:
            for key, value in kwargs.items():
                if key not in DatabaseHelper.VALID_GAME_COLUMNS:
                    raise ValueError(f"Invalid column name: {key}")
                await db.execute(
                    f"UPDATE games SET {key} = ? WHERE game_id = ?",
                    (value, game_id)
                )
            await db.commit()
    
    @staticmethod
    async def delete_game(game_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM games WHERE game_id = ?", (game_id,))
            await db.commit()
    
    @staticmethod
    async def get_player_stats(player_id: int, game_id: int) -> PlayerStats:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM player_game_stats WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return PlayerStats(player_id=player_id, game_id=game_id)
                return PlayerStats(
                    player_id=row[0],
                    game_id=row[1],
                    mmr=row[2],
                    games_played=row[3],
                    wins=row[4],
                    losses=row[5],
                    admin_offset=row[6],
                    last_played=datetime.fromisoformat(row[7]) if row[7] else None
                )
    
    @staticmethod
    async def update_player_stats(stats: PlayerStats):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO player_game_stats 
                   (player_id, game_id, mmr, games_played, wins, losses, admin_offset, last_played)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (stats.player_id, stats.game_id, stats.mmr, stats.games_played,
                 stats.wins, stats.losses, stats.admin_offset,
                 stats.last_played.isoformat() if stats.last_played else None)
            )
            await db.commit()
    
    @staticmethod
    async def get_mmr_roles(game_id: int) -> Dict[int, int]:
        """Returns {role_id: mmr_value}"""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT role_id, mmr_value FROM game_mmr_roles WHERE game_id = ?",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}
    
    @staticmethod
    async def set_mmr_role(game_id: int, role_id: int, mmr_value: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO game_mmr_roles (game_id, role_id, mmr_value)
                   VALUES (?, ?, ?)""",
                (game_id, role_id, mmr_value)
            )
            await db.commit()
    
    @staticmethod
    async def remove_mmr_role(game_id: int, role_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM game_mmr_roles WHERE game_id = ? AND role_id = ?",
                (game_id, role_id)
            )
            await db.commit()
    
    @staticmethod
    async def is_blacklisted(player_id: int) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT blacklisted_until FROM players WHERE player_id = ?",
                (player_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row or not row[0]:
                    return False
                until = datetime.fromisoformat(row[0])
                return until > datetime.now(timezone.utc)
    
    @staticmethod
    async def blacklist_player(player_id: int, until: Optional[datetime] = None):
        """Blacklist a player. If until is None, permanent."""
        if until is None:
            until = datetime(2099, 12, 31, tzinfo=timezone.utc)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO players (player_id, blacklisted_until) VALUES (?, ?)
                   ON CONFLICT(player_id) DO UPDATE SET blacklisted_until = ?""",
                (player_id, until.isoformat(), until.isoformat())
            )
            await db.commit()
    
    @staticmethod
    async def unblacklist_player(player_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE players SET blacklisted_until = NULL WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()
    
    @staticmethod
    async def get_blacklisted_players() -> List[Tuple[int, datetime]]:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT player_id, blacklisted_until FROM players WHERE blacklisted_until IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
                return [(row[0], datetime.fromisoformat(row[1])) for row in rows if row[1]]
    
    @staticmethod
    async def create_match(game_id: int, queue_type: str, queue_message_id: Optional[int] = None,
                           short_id: Optional[str] = None) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """INSERT INTO matches (game_id, queue_type, queue_message_id, created_at, short_id)
                   VALUES (?, ?, ?, ?, ?)""",
                (game_id, queue_type, queue_message_id, datetime.now(timezone.utc).isoformat(), short_id)
            )
            await db.commit()
            return cursor.lastrowid
    
    @staticmethod
    async def update_match(match_id: int, **kwargs):
        async with aiosqlite.connect(DB_PATH) as db:
            for key, value in kwargs.items():
                if key not in DatabaseHelper.VALID_MATCH_COLUMNS:
                    raise ValueError(f"Invalid column name: {key}")
                await db.execute(
                    f"UPDATE matches SET {key} = ? WHERE match_id = ?",
                    (value, match_id)
                )
            await db.commit()
    
    @staticmethod
    async def get_match(match_id: int) -> Optional[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    
    @staticmethod
    async def get_match_by_channel(channel_id: int) -> Optional[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE channel_id = ? AND winning_team IS NULL AND cancelled = 0",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_match_by_short_id(short_id: str) -> Optional[dict]:
        """Get a match by its short ID."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE short_id = ?",
                (short_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_active_matches() -> List[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE winning_team IS NULL AND cancelled = 0"
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    @staticmethod
    async def add_match_player(match_id: int, player_id: int, team: str,
                               was_captain: bool = False, was_sub: bool = False,
                               original_player_id: Optional[int] = None):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO match_players 
                   (match_id, player_id, team, was_captain, was_sub, original_player_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (match_id, player_id, team, int(was_captain), int(was_sub), original_player_id)
            )
            await db.commit()
    
    @staticmethod
    async def get_match_players(match_id: int) -> List[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM match_players WHERE match_id = ?", (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    @staticmethod
    async def remove_match_player(match_id: int, player_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM match_players WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def set_match_map(match_id: int, map_name: str):
        """Set the map name for a match (from map vote)."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE matches SET map_name = ? WHERE match_id = ?",
                (map_name, match_id)
            )
            await db.commit()

    @staticmethod
    async def get_match_by_channel_id(channel_id: int) -> Optional[dict]:
        """Get match by channel ID (includes completed/cancelled matches)."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE channel_id = ? ORDER BY created_at DESC LIMIT 1",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def get_rivalry(player_a: int, player_b: int, game_id: int) -> Optional[Tuple[int, int]]:
        """Returns (player_a_wins, player_b_wins) or None."""
        # Ensure consistent ordering
        if player_a > player_b:
            player_a, player_b = player_b, player_a
            swapped = True
        else:
            swapped = False
        
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """SELECT player_a_wins, player_b_wins FROM rivalries 
                   WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                (player_a, player_b, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                if swapped:
                    return (row[1], row[0])
                return (row[0], row[1])
    
    @staticmethod
    async def update_rivalry(winner_id: int, loser_id: int, game_id: int):
        """Update rivalry stats after a match."""
        # Ensure consistent ordering
        if winner_id > loser_id:
            player_a, player_b = loser_id, winner_id
            winner_is_b = True
        else:
            player_a, player_b = winner_id, loser_id
            winner_is_b = False
        
        async with aiosqlite.connect(DB_PATH) as db:
            # Check if exists
            async with db.execute(
                """SELECT player_a_wins, player_b_wins FROM rivalries
                   WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                (player_a, player_b, game_id)
            ) as cursor:
                row = await cursor.fetchone()
            
            if row:
                if winner_is_b:
                    await db.execute(
                        """UPDATE rivalries SET player_b_wins = player_b_wins + 1
                           WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                        (player_a, player_b, game_id)
                    )
                else:
                    await db.execute(
                        """UPDATE rivalries SET player_a_wins = player_a_wins + 1
                           WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                        (player_a, player_b, game_id)
                    )
            else:
                a_wins = 0 if winner_is_b else 1
                b_wins = 1 if winner_is_b else 0
                await db.execute(
                    """INSERT INTO rivalries (player_a_id, player_b_id, game_id, player_a_wins, player_b_wins)
                       VALUES (?, ?, ?, ?, ?)""",
                    (player_a, player_b, game_id, a_wins, b_wins)
                )
            await db.commit()

    @staticmethod
    async def reverse_rivalry(old_winner_id: int, old_loser_id: int, game_id: int):
        """Reverse a rivalry record - swap a win from old_winner to old_loser."""
        # Ensure consistent ordering
        if old_winner_id > old_loser_id:
            player_a, player_b = old_loser_id, old_winner_id
            old_winner_is_b = True
        else:
            player_a, player_b = old_winner_id, old_loser_id
            old_winner_is_b = False

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """SELECT player_a_wins, player_b_wins FROM rivalries
                   WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                (player_a, player_b, game_id)
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                # Decrement old winner's wins, increment old loser's wins
                if old_winner_is_b:
                    new_a_wins = row[0] + 1  # old loser now has one more win
                    new_b_wins = max(0, row[1] - 1)  # old winner loses a win
                else:
                    new_a_wins = max(0, row[0] - 1)  # old winner loses a win
                    new_b_wins = row[1] + 1  # old loser now has one more win

                await db.execute(
                    """UPDATE rivalries SET player_a_wins = ?, player_b_wins = ?
                       WHERE player_a_id = ? AND player_b_id = ? AND game_id = ?""",
                    (new_a_wins, new_b_wins, player_a, player_b, game_id)
                )
                await db.commit()

    @staticmethod
    async def get_player_rivalries(player_id: int, game_id: int, limit: int = 3) -> List[dict]:
        """Get top rivalries for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """SELECT player_a_id, player_b_id, player_a_wins, player_b_wins
                   FROM rivalries WHERE (player_a_id = ? OR player_b_id = ?) AND game_id = ?
                   ORDER BY (player_a_wins + player_b_wins) DESC LIMIT ?""",
                (player_id, player_id, game_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    if row[0] == player_id:
                        results.append({
                            "opponent_id": row[1],
                            "wins": row[2],
                            "losses": row[3]
                        })
                    else:
                        results.append({
                            "opponent_id": row[0],
                            "wins": row[3],
                            "losses": row[2]
                        })
                return results
    
    @staticmethod
    async def add_win_vote(match_id: int, player_id: int, team: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO win_votes (match_id, player_id, voted_team)
                   VALUES (?, ?, ?)""",
                (match_id, player_id, team)
            )
            await db.commit()
    
    @staticmethod
    async def get_win_votes(match_id: int) -> Dict[str, int]:
        """Returns {team: vote_count}"""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT voted_team, COUNT(*) FROM win_votes WHERE match_id = ? GROUP BY voted_team",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}
    
    @staticmethod
    async def record_mmr_change(player_id: int, game_id: int, match_id: int,
                                mmr_before: int, mmr_after: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO mmr_history (player_id, game_id, match_id, mmr_before, mmr_after, change)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (player_id, game_id, match_id, mmr_before, mmr_after, mmr_after - mmr_before)
            )
            await db.commit()
    
    @staticmethod
    async def get_leaderboard(game_id: int, monthly: bool = True, limit: int = 20) -> List[dict]:
        """Get leaderboard for a game."""
        async with aiosqlite.connect(DB_PATH) as db:
            if monthly:
                # Get current month start
                now = datetime.now(timezone.utc)
                month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                # Get wins/losses from matches this month + admin adjustments
                query = """
                    WITH match_stats AS (
                        SELECT mp.player_id,
                               SUM(CASE WHEN m.winning_team = mp.team THEN 1 ELSE 0 END) as wins,
                               SUM(CASE WHEN m.winning_team != mp.team THEN 1 ELSE 0 END) as losses
                        FROM match_players mp
                        JOIN matches m ON mp.match_id = m.match_id
                        WHERE m.game_id = ? AND m.winning_team IS NOT NULL
                              AND m.decided_at >= ? AND m.cancelled = 0
                        GROUP BY mp.player_id
                    ),
                    admin_stats AS (
                        SELECT player_id,
                               SUM(wins_delta) as wins,
                               SUM(losses_delta) as losses
                        FROM admin_stat_adjustments
                        WHERE game_id = ? AND adjusted_at >= ?
                        GROUP BY player_id
                    ),
                    combined AS (
                        SELECT player_id, wins, losses FROM match_stats
                        UNION ALL
                        SELECT player_id, wins, losses FROM admin_stats
                    )
                    SELECT player_id, SUM(wins) as wins, SUM(losses) as losses
                    FROM combined
                    GROUP BY player_id
                    ORDER BY wins DESC, losses ASC
                    LIMIT ?
                """
                async with db.execute(query, (game_id, month_start.isoformat(),
                                              game_id, month_start.isoformat(), limit)) as cursor:
                    rows = await cursor.fetchall()
            else:
                # All-time from player_game_stats
                query = """
                    SELECT player_id, wins, losses FROM player_game_stats
                    WHERE game_id = ?
                    ORDER BY wins DESC, losses ASC
                    LIMIT ?
                """
                async with db.execute(query, (game_id, limit)) as cursor:
                    rows = await cursor.fetchall()

            return [{"player_id": row[0], "wins": row[1], "losses": row[2]} for row in rows]

    @staticmethod
    async def get_recent_completed_matches(game_id: int, limit: int = 5) -> List[dict]:
        """Get the most recent completed matches for a game."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT match_id, map_name, decided_at
                   FROM matches
                   WHERE game_id = ? AND winning_team IS NOT NULL AND cancelled = 0
                   ORDER BY decided_at DESC
                   LIMIT ?""",
                (game_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                return [{"match_id": row["match_id"], "map_name": row["map_name"],
                         "decided_at": row["decided_at"]} for row in rows]

    @staticmethod
    async def get_player_leaderboard_rank(player_id: int, game_id: int, monthly: bool = True) -> Optional[int]:
        """Get a player's rank on the leaderboard. Returns None if not on leaderboard."""
        leaderboard = await DatabaseHelper.get_leaderboard(game_id, monthly=monthly, limit=100)
        for i, entry in enumerate(leaderboard, 1):
            if entry["player_id"] == player_id:
                return i
        return None

    @staticmethod
    async def get_player_recent_matches(player_id: int, limit: int = 5) -> List[dict]:
        """Get recent match history for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT m.*, mp.team, g.name as game_name
                   FROM matches m
                   JOIN match_players mp ON m.match_id = mp.match_id
                   JOIN games g ON m.game_id = g.game_id
                   WHERE mp.player_id = ? AND m.winning_team IS NOT NULL
                   ORDER BY m.decided_at DESC
                   LIMIT ?""",
                (player_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    @staticmethod
    async def get_player_in_active_match(player_id: int, game_id: int) -> bool:
        """Check if player is in an active match for a specific game."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """SELECT 1 FROM match_players mp
                   JOIN matches m ON mp.match_id = m.match_id
                   WHERE mp.player_id = ? AND m.game_id = ?
                         AND m.winning_team IS NULL AND m.cancelled = 0
                   LIMIT 1""",
                (player_id, game_id)
            ) as cursor:
                return await cursor.fetchone() is not None

    # -------------------------------------------------------------------------
    # IGN METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_player_ign(player_id: int, game_id: int) -> Optional[str]:
        """Get a player's IGN for a specific game."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT ign FROM player_igns WHERE player_id = ? AND game_id = ?",
                (player_id, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def set_player_ign(player_id: int, game_id: int, ign: str):
        """Set a player's IGN for a specific game."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO player_igns (player_id, game_id, ign)
                   VALUES (?, ?, ?)""",
                (player_id, game_id, ign)
            )
            await db.commit()

    @staticmethod
    async def get_match_igns(match_id: int) -> Dict[int, str]:
        """Get all IGNs for players in a match. Returns {player_id: ign}."""
        async with aiosqlite.connect(DB_PATH) as db:
            # First get match game_id
            async with db.execute(
                "SELECT game_id FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {}
                game_id = row[0]

            # Get all player IGNs for this game
            async with db.execute(
                """SELECT pi.player_id, pi.ign FROM player_igns pi
                   JOIN match_players mp ON pi.player_id = mp.player_id
                   WHERE mp.match_id = ? AND pi.game_id = ?""",
                (match_id, game_id)
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows}

    @staticmethod
    async def get_player_all_igns(player_id: int) -> List[Tuple[int, str, str]]:
        """Get all IGNs for a player. Returns [(game_id, game_name, ign)]."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """SELECT pi.game_id, g.name, pi.ign FROM player_igns pi
                   JOIN games g ON pi.game_id = g.game_id
                   WHERE pi.player_id = ?""",
                (player_id,)
            ) as cursor:
                return await cursor.fetchall()

    # -------------------------------------------------------------------------
    # READY PENALTY METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_ready_penalty(player_id: int) -> ReadyPenalty:
        """Get a player's ready penalty status."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM ready_penalties WHERE player_id = ?",
                (player_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return ReadyPenalty(player_id=player_id)
                return ReadyPenalty(
                    player_id=row[0],
                    offense_count=row[1] or 0,
                    penalty_expires=datetime.fromisoformat(row[2]) if row[2] else None,
                    last_offense=datetime.fromisoformat(row[3]) if row[3] else None
                )

    @staticmethod
    async def add_ready_penalty_offense(player_id: int, game: GameConfig) -> Tuple[int, Optional[datetime]]:
        """Add a ready penalty offense. Returns (new_offense_count, penalty_expires)."""
        penalty = await DatabaseHelper.get_ready_penalty(player_id)
        now = datetime.now(timezone.utc)

        # Check for decay
        if penalty.last_offense:
            days_since = (now - penalty.last_offense).days
            if days_since >= game.penalty_decay_days:
                penalty.offense_count = 0

        penalty.offense_count += 1
        penalty.last_offense = now

        # Determine penalty duration based on offense count
        if penalty.offense_count == 1:
            duration_minutes = game.penalty_1st_minutes
        elif penalty.offense_count == 2:
            duration_minutes = game.penalty_2nd_minutes
        else:
            duration_minutes = game.penalty_3rd_minutes

        penalty.penalty_expires = now + timedelta(minutes=duration_minutes)

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO ready_penalties
                   (player_id, offense_count, penalty_expires, last_offense)
                   VALUES (?, ?, ?, ?)""",
                (player_id, penalty.offense_count,
                 penalty.penalty_expires.isoformat(),
                 penalty.last_offense.isoformat())
            )
            await db.commit()

        return penalty.offense_count, penalty.penalty_expires

    @staticmethod
    async def clear_ready_penalty(player_id: int):
        """Clear a player's ready penalty (admin action)."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM ready_penalties WHERE player_id = ?",
                (player_id,)
            )
            await db.commit()

    @staticmethod
    async def get_all_penalties() -> List[ReadyPenalty]:
        """Get all active penalties."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM ready_penalties WHERE penalty_expires > ?",
                (now,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    ReadyPenalty(
                        player_id=row[0],
                        offense_count=row[1] or 0,
                        penalty_expires=datetime.fromisoformat(row[2]) if row[2] else None,
                        last_offense=datetime.fromisoformat(row[3]) if row[3] else None
                    )
                    for row in rows
                ]

    @staticmethod
    async def is_penalized(player_id: int) -> Optional[datetime]:
        """Check if a player is currently penalized. Returns expiry time or None."""
        penalty = await DatabaseHelper.get_ready_penalty(player_id)
        if penalty.penalty_expires and penalty.penalty_expires > datetime.now(timezone.utc):
            return penalty.penalty_expires
        return None

    # -------------------------------------------------------------------------
    # SUSPENSION METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def add_suspension(player_id: int, game_id: Optional[int], until: datetime,
                             reason: Optional[str], suspended_by: Optional[int]) -> int:
        """Add a suspension. Returns suspension_id."""
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """INSERT INTO suspensions (player_id, game_id, suspended_until, reason, suspended_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (player_id, game_id, until.isoformat(), reason, suspended_by)
            )
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def remove_suspension(suspension_id: int):
        """Remove a suspension."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM suspensions WHERE suspension_id = ?",
                (suspension_id,)
            )
            await db.commit()

    @staticmethod
    async def is_suspended(player_id: int, game_id: int) -> Optional[Suspension]:
        """Check if a player is suspended for a game. Returns Suspension or None."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            # Check for game-specific or all-game suspension
            async with db.execute(
                """SELECT * FROM suspensions
                   WHERE player_id = ? AND suspended_until > ?
                         AND (game_id = ? OR game_id IS NULL)
                   ORDER BY suspended_until DESC LIMIT 1""",
                (player_id, now, game_id)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return Suspension(
                    suspension_id=row[0],
                    player_id=row[1],
                    game_id=row[2],
                    suspended_until=datetime.fromisoformat(row[3]),
                    reason=row[4],
                    suspended_by=row[5],
                    created_at=datetime.fromisoformat(row[6]) if row[6] else datetime.now(timezone.utc)
                )

    @staticmethod
    async def get_all_suspensions() -> List[Suspension]:
        """Get all active suspensions."""
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM suspensions WHERE suspended_until > ?",
                (now,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    Suspension(
                        suspension_id=row[0],
                        player_id=row[1],
                        game_id=row[2],
                        suspended_until=datetime.fromisoformat(row[3]),
                        reason=row[4],
                        suspended_by=row[5],
                        created_at=datetime.fromisoformat(row[6]) if row[6] else datetime.now(timezone.utc)
                    )
                    for row in rows
                ]

    # -------------------------------------------------------------------------
    # ABANDON VOTE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def add_abandon_vote(match_id: int, player_id: int):
        """Add an abandon vote."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT OR REPLACE INTO abandon_votes (match_id, player_id)
                   VALUES (?, ?)""",
                (match_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_abandon_votes(match_id: int) -> int:
        """Get number of abandon votes for a match."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM abandon_votes WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    @staticmethod
    async def has_voted_abandon(match_id: int, player_id: int) -> bool:
        """Check if a player has already voted to abandon."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM abandon_votes WHERE match_id = ? AND player_id = ?",
                (match_id, player_id)
            ) as cursor:
                return await cursor.fetchone() is not None

    # -------------------------------------------------------------------------
    # STATS METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def delete_player_stats(player_id: int):
        """Delete all stats for a player (when they leave the server)."""
        async with aiosqlite.connect(DB_PATH) as db:
            # Delete from player_game_stats
            await db.execute("DELETE FROM player_game_stats WHERE player_id = ?", (player_id,))
            # Delete from mmr_history
            await db.execute("DELETE FROM mmr_history WHERE player_id = ?", (player_id,))
            # Delete from rivalries (both sides)
            await db.execute(
                "DELETE FROM rivalries WHERE player_a_id = ? OR player_b_id = ?",
                (player_id, player_id)
            )
            # Delete IGNs
            await db.execute("DELETE FROM player_igns WHERE player_id = ?", (player_id,))
            # Delete from players table
            await db.execute("DELETE FROM players WHERE player_id = ?", (player_id,))
            await db.commit()

    @staticmethod
    async def adjust_player_stats(player_id: int, game_id: int, wins_delta: int, losses_delta: int,
                                   adjusted_by: Optional[int] = None):
        """Adjust a player's wins/losses by specified amounts. Records for monthly leaderboard."""
        stats = await DatabaseHelper.get_player_stats(player_id, game_id)
        stats.wins = max(0, stats.wins + wins_delta)
        stats.losses = max(0, stats.losses + losses_delta)
        stats.games_played = stats.wins + stats.losses
        await DatabaseHelper.update_player_stats(stats)

        # Also record in admin_stat_adjustments for monthly leaderboard tracking
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO admin_stat_adjustments
                   (player_id, game_id, wins_delta, losses_delta, adjusted_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (player_id, game_id, wins_delta, losses_delta, adjusted_by)
            )
            await db.commit()

    @staticmethod
    async def get_completed_match(match_id: int) -> Optional[dict]:
        """Get a match even if it's already decided."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM matches WHERE match_id = ?", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    @staticmethod
    async def reverse_match_result(match_id: int) -> bool:
        """Reverse all MMR changes from a completed match. Returns True if successful."""
        async with aiosqlite.connect(DB_PATH) as db:
            # Get MMR history for this match
            async with db.execute(
                "SELECT player_id, game_id, mmr_before, mmr_after FROM mmr_history WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                return False

            # Reverse the MMR changes
            for player_id, game_id, mmr_before, mmr_after in rows:
                change = mmr_after - mmr_before
                # Get current stats
                stats = await DatabaseHelper.get_player_stats(player_id, game_id)
                stats.mmr -= change  # Reverse the change

                # Also reverse W/L
                if change > 0:  # Was a winner
                    stats.wins = max(0, stats.wins - 1)
                else:  # Was a loser
                    stats.losses = max(0, stats.losses - 1)
                stats.games_played = stats.wins + stats.losses

                await DatabaseHelper.update_player_stats(stats)

            # Delete the MMR history entries
            await db.execute("DELETE FROM mmr_history WHERE match_id = ?", (match_id,))

            # Clear the winning_team so the match can be re-decided
            await db.execute(
                "UPDATE matches SET winning_team = NULL, decided_at = NULL WHERE match_id = ?",
                (match_id,)
            )
            await db.commit()

        return True

    # -------------------------------------------------------------------------
    # QUEUE METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def clear_queue(queue_id: int):
        """Clear all players from a queue."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (queue_id,))
            await db.commit()

    @staticmethod
    async def remove_player_from_queue(queue_id: int, player_id: int):
        """Remove a specific player from a queue."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                (queue_id, player_id)
            )
            await db.commit()

    @staticmethod
    async def get_queue_join_times(queue_id: int) -> Dict[int, datetime]:
        """Get join times for all players in a queue. Returns {player_id: joined_at}."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT player_id, joined_at FROM queue_players WHERE queue_id = ?",
                (queue_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return {
                    row[0]: datetime.fromisoformat(row[1]) if row[1] else datetime.now(timezone.utc)
                    for row in rows
                }

    # -------------------------------------------------------------------------
    # VALORANT STATS METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    async def save_valorant_match_stats(
        match_id: int, valorant_match_id: str, player_id: int, ign: str,
        agent: str, kills: int, deaths: int, assists: int,
        headshots: int, bodyshots: int, legshots: int, score: int, map_name: str,
        damage_dealt: int = 0, first_bloods: int = 0, plants: int = 0, defuses: int = 0,
        c2k: int = 0, c3k: int = 0, c4k: int = 0, c5k: int = 0,
        econ_spent: int = 0, econ_loadout: int = 0
    ):
        """Save Valorant match stats for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO valorant_match_stats
                (match_id, valorant_match_id, player_id, ign, agent, kills, deaths, assists,
                 headshots, bodyshots, legshots, score, map_name, damage_dealt, first_bloods,
                 plants, defuses, c2k, c3k, c4k, c5k, econ_spent, econ_loadout)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (match_id, valorant_match_id, player_id, ign, agent, kills, deaths, assists,
                  headshots, bodyshots, legshots, score, map_name, damage_dealt, first_bloods,
                  plants, defuses, c2k, c3k, c4k, c5k, econ_spent, econ_loadout))
            await db.commit()

    @staticmethod
    async def get_valorant_match_stats(match_id: int) -> List[dict]:
        """Get all Valorant stats for a match."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM valorant_match_stats WHERE match_id = ?",
                (match_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_valorant_player_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get aggregated Valorant stats for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            # Build date filter for monthly stats
            date_filter = ""
            if monthly:
                date_filter = "AND vms.fetched_at >= date('now', '-30 days')"

            # Get aggregate stats
            query = f"""
                SELECT
                    COUNT(*) as total_games,
                    COALESCE(SUM(vms.kills), 0) as total_kills,
                    COALESCE(SUM(vms.deaths), 0) as total_deaths,
                    COALESCE(SUM(vms.assists), 0) as total_assists,
                    COALESCE(SUM(vms.headshots), 0) as total_headshots,
                    COALESCE(SUM(vms.bodyshots), 0) as total_bodyshots,
                    COALESCE(SUM(vms.legshots), 0) as total_legshots,
                    COALESCE(SUM(vms.score), 0) as total_score,
                    COALESCE(SUM(vms.damage_dealt), 0) as total_damage,
                    COALESCE(SUM(vms.first_bloods), 0) as total_first_bloods
                FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                WHERE vms.player_id = ? AND m.game_id = ? {date_filter}
            """
            async with db.execute(query, (player_id, game_id)) as cursor:
                row = await cursor.fetchone()
                stats = dict(row) if row else {}

            # Calculate HS%
            total_shots = stats.get('total_headshots', 0) + stats.get('total_bodyshots', 0) + stats.get('total_legshots', 0)
            stats['hs_percent'] = round((stats.get('total_headshots', 0) / total_shots * 100), 1) if total_shots > 0 else 0

            # Get map stats with wins/losses
            map_query = f"""
                SELECT
                    vms.map_name,
                    COUNT(*) as games,
                    SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as wins
                FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                JOIN match_players mp ON m.match_id = mp.match_id AND mp.player_id = vms.player_id
                WHERE vms.player_id = ? AND m.game_id = ? AND vms.map_name IS NOT NULL {date_filter}
                GROUP BY vms.map_name
                ORDER BY CAST(wins AS FLOAT) / games DESC
            """
            async with db.execute(map_query, (player_id, game_id)) as cursor:
                map_rows = await cursor.fetchall()
                stats['map_stats'] = []
                for row in map_rows:
                    wins = row['wins']
                    losses = row['games'] - wins
                    winrate = round(wins / row['games'] * 100, 1) if row['games'] > 0 else 0
                    stats['map_stats'].append({
                        'name': row['map_name'],
                        'games': row['games'],
                        'wins': wins,
                        'losses': losses,
                        'winrate': winrate
                    })
                if map_rows:
                    best = map_rows[0]
                    worst = map_rows[-1]
                    stats['best_map'] = {
                        'name': best['map_name'],
                        'games': best['games'],
                        'winrate': round(best['wins'] / best['games'] * 100, 1)
                    }
                    stats['worst_map'] = {
                        'name': worst['map_name'],
                        'games': worst['games'],
                        'winrate': round(worst['wins'] / worst['games'] * 100, 1)
                    }

            # Get agent stats with wins/losses
            agent_query = f"""
                SELECT
                    vms.agent,
                    COUNT(*) as games,
                    SUM(CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END) as wins
                FROM valorant_match_stats vms
                JOIN matches m ON vms.match_id = m.match_id
                JOIN match_players mp ON m.match_id = mp.match_id AND mp.player_id = vms.player_id
                WHERE vms.player_id = ? AND m.game_id = ? AND vms.agent IS NOT NULL {date_filter}
                GROUP BY vms.agent
                ORDER BY games DESC
            """
            async with db.execute(agent_query, (player_id, game_id)) as cursor:
                agent_rows = await cursor.fetchall()
                stats['agent_stats'] = {}
                for row in agent_rows:
                    wins = row['wins']
                    losses = row['games'] - wins
                    stats['agent_stats'][row['agent']] = {
                        'games': row['games'],
                        'wins': wins,
                        'losses': losses
                    }

            return stats

    @staticmethod
    async def get_player_teammate_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get teammate win rates for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            date_filter = ""
            if monthly:
                date_filter = "AND m.decided_at >= date('now', '-30 days')"

            query = f"""
                SELECT
                    mp2.player_id as teammate_id,
                    COUNT(*) as games_together,
                    SUM(CASE WHEN mp1.team = m.winning_team THEN 1 ELSE 0 END) as wins_together
                FROM match_players mp1
                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team = mp2.team
                JOIN matches m ON mp1.match_id = m.match_id
                WHERE mp1.player_id = ? AND mp2.player_id != ? AND m.game_id = ?
                      AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp2.player_id
                HAVING games_together >= 3
                ORDER BY CAST(wins_together AS FLOAT) / games_together DESC
            """
            async with db.execute(query, (player_id, player_id, game_id)) as cursor:
                rows = await cursor.fetchall()

            result = {}
            if rows:
                best = rows[0]
                worst = rows[-1]
                result['best_teammate'] = {
                    'player_id': best['teammate_id'],
                    'games': best['games_together'],
                    'winrate': round(best['wins_together'] / best['games_together'] * 100, 1)
                }
                result['cursed_teammate'] = {
                    'player_id': worst['teammate_id'],
                    'games': worst['games_together'],
                    'winrate': round(worst['wins_together'] / worst['games_together'] * 100, 1)
                }
            return result

    @staticmethod
    async def get_player_recent_matches(player_id: int, game_id: int, limit: int = 5) -> List[dict]:
        """Get recent matches with Valorant stats for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT
                    m.match_id, m.short_id, m.winning_team, m.decided_at,
                    m.map_name as match_map_name,
                    mp.team,
                    vms.kills, vms.deaths, vms.assists, vms.agent, vms.map_name
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                LEFT JOIN valorant_match_stats vms ON m.match_id = vms.match_id AND vms.player_id = mp.player_id
                WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL
                ORDER BY m.decided_at DESC
                LIMIT ?
            """
            async with db.execute(query, (player_id, game_id, limit)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_player_match_stats(player_id: int, match_id: int) -> dict:
        """Get full Valorant stats for a specific match."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT
                    vms.agent, vms.kills, vms.deaths, vms.assists,
                    vms.headshots, vms.bodyshots, vms.legshots,
                    vms.score, vms.damage_dealt, vms.first_bloods, vms.map_name,
                    vms.plants, vms.defuses, vms.c2k, vms.c3k, vms.c4k, vms.c5k,
                    vms.econ_spent, vms.econ_loadout
                FROM valorant_match_stats vms
                WHERE vms.player_id = ? AND vms.match_id = ?
            """
            async with db.execute(query, (player_id, match_id)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else {}

    @staticmethod
    async def get_player_streak_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get longest winning and losing streaks for a player."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            date_filter = ""
            if monthly:
                date_filter = "AND m.decided_at >= date('now', '-30 days')"

            query = f"""
                SELECT
                    m.match_id,
                    CASE WHEN mp.team = m.winning_team THEN 1 ELSE 0 END as won
                FROM matches m
                JOIN match_players mp ON m.match_id = mp.match_id
                WHERE mp.player_id = ? AND m.game_id = ? AND m.winning_team IS NOT NULL {date_filter}
                ORDER BY m.decided_at ASC
            """
            async with db.execute(query, (player_id, game_id)) as cursor:
                rows = await cursor.fetchall()

            # Calculate streaks
            longest_win_streak = 0
            longest_loss_streak = 0
            current_win_streak = 0
            current_loss_streak = 0

            for row in rows:
                if row['won']:
                    current_win_streak += 1
                    current_loss_streak = 0
                    longest_win_streak = max(longest_win_streak, current_win_streak)
                else:
                    current_loss_streak += 1
                    current_win_streak = 0
                    longest_loss_streak = max(longest_loss_streak, current_loss_streak)

            return {
                'longest_win_streak': longest_win_streak,
                'longest_loss_streak': longest_loss_streak
            }

    @staticmethod
    async def get_all_teammate_stats(player_id: int, game_id: int, monthly: bool = False) -> dict:
        """Get all teammate stats for display (top 3 best, top 3 worst)."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row

            date_filter = ""
            if monthly:
                date_filter = "AND m.decided_at >= date('now', '-30 days')"

            query = f"""
                SELECT
                    mp2.player_id as teammate_id,
                    COUNT(*) as games_together,
                    SUM(CASE WHEN mp1.team = m.winning_team THEN 1 ELSE 0 END) as wins_together
                FROM match_players mp1
                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team = mp2.team
                JOIN matches m ON mp1.match_id = m.match_id
                WHERE mp1.player_id = ? AND mp2.player_id != ? AND m.game_id = ?
                      AND m.winning_team IS NOT NULL {date_filter}
                GROUP BY mp2.player_id
                HAVING games_together >= 3
                ORDER BY CAST(wins_together AS FLOAT) / games_together DESC
            """
            async with db.execute(query, (player_id, player_id, game_id)) as cursor:
                rows = await cursor.fetchall()

            result = {'best_teammates': [], 'worst_teammates': []}
            if rows:
                # Get top 3 best
                for row in rows[:3]:
                    wins = row['wins_together']
                    losses = row['games_together'] - wins
                    result['best_teammates'].append({
                        'player_id': row['teammate_id'],
                        'games': row['games_together'],
                        'wins': wins,
                        'losses': losses,
                        'winrate': round(wins / row['games_together'] * 100, 1)
                    })
                # Get top 3 worst (from the end)
                for row in rows[-3:] if len(rows) >= 3 else rows:
                    wins = row['wins_together']
                    losses = row['games_together'] - wins
                    result['worst_teammates'].append({
                        'player_id': row['teammate_id'],
                        'games': row['games_together'],
                        'wins': wins,
                        'losses': losses,
                        'winrate': round(wins / row['games_together'] * 100, 1)
                    })
                # Sort worst teammates by winrate ascending
                result['worst_teammates'] = sorted(result['worst_teammates'], key=lambda x: x['winrate'])[:3]
            return result

    @staticmethod
    async def mark_valorant_regular(player_id: int, game_id: int, ign: str, puuid: str = None, region: str = 'na'):
        """Mark a player as a verified Valorant regular for API lookups."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT OR REPLACE INTO valorant_player_regulars
                (player_id, game_id, ign, puuid, region, verified_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (player_id, game_id, ign, puuid, region))
            await db.commit()

    @staticmethod
    async def get_valorant_regulars(game_id: int) -> List[dict]:
        """Get all verified Valorant regulars for a game."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM valorant_player_regulars WHERE game_id = ?",
                (game_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    @staticmethod
    async def get_mod_roles() -> List[int]:
        """Get all mod role IDs."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT role_id FROM mod_roles") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    @staticmethod
    async def add_mod_role(role_id: int):
        """Add a mod role."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO mod_roles (role_id) VALUES (?)",
                (role_id,)
            )
            await db.commit()

    @staticmethod
    async def remove_mod_role(role_id: int):
        """Remove a mod role."""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM mod_roles WHERE role_id = ?",
                (role_id,)
            )
            await db.commit()

    @staticmethod
    async def get_match_valorant_id(match_id: int) -> Optional[str]:
        """Get the Valorant match ID for a custom match (if available)."""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT valorant_match_id FROM valorant_match_stats WHERE match_id = ? LIMIT 1",
                (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

# =============================================================================
# HENRIKDEV API
# =============================================================================

class HenrikDevAPI:
    """Wrapper for HenrikDev Valorant API (free tier - 30 req/min)."""

    BASE_URL = "https://api.henrikdev.xyz"

    def __init__(self, bot):
        self.bot = bot
        self._session: Optional['aiohttp.ClientSession'] = None
        self._semaphore = asyncio.Semaphore(25)  # Stay under 30 req/min limit
        self._last_requests: List[float] = []
        # Get API key from environment
        self._api_key = os.getenv("HENRIK_API_KEY")
        if not self._api_key:
            logger.warning("HENRIK_API_KEY not set - Valorant stats fetching will not work")

    async def _get_session(self):
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session

    async def _rate_limit(self):
        """Enforce rate limiting."""
        now = asyncio.get_event_loop().time()
        # Remove requests older than 60 seconds
        self._last_requests = [t for t in self._last_requests if now - t < 60]

        if len(self._last_requests) >= 25:
            # Wait until oldest request is 60s old
            wait_time = 60 - (now - self._last_requests[0])
            if wait_time > 0:
                await asyncio.sleep(wait_time)

        self._last_requests.append(now)

    async def _request(self, endpoint: str) -> Optional[dict]:
        """Make a rate-limited request to the API."""
        import aiohttp
        if not self._api_key:
            logger.debug("Skipping HenrikDev API request - no API key configured")
            return None
        async with self._semaphore:
            await self._rate_limit()
            session = await self._get_session()
            headers = {"Authorization": self._api_key}
            try:
                timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
                async with session.get(f"{self.BASE_URL}{endpoint}", headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 401:
                        logger.warning("HenrikDev API unauthorized - check HENRIK_API_KEY")
                        return None
                    elif resp.status == 429:
                        logger.warning("HenrikDev API rate limited")
                        return None
                    else:
                        logger.warning(f"HenrikDev API error: {resp.status}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"HenrikDev API request timed out: {endpoint}")
                return None
            except Exception as e:
                logger.error(f"HenrikDev API request failed: {e}")
                return None

    async def get_custom_match_history(self, name: str, tag: str, region: str = 'na') -> Optional[List[dict]]:
        """Fetch recent custom matches for a player."""
        from urllib.parse import quote
        endpoint = f"/valorant/v1/stored-matches/{quote(region)}/{quote(name)}/{quote(tag)}?mode=custom"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            return data.get('data', [])
        return None

    async def get_match_details(self, match_id: str) -> Optional[dict]:
        """Get full match details by match ID."""
        endpoint = f"/valorant/v2/match/{match_id}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            return data.get('data')
        return None

    async def find_and_fetch_match_stats(
        self,
        player_ign: str,
        our_match_id: int,
        game_id: int,
        match_end_time: datetime
    ) -> Optional[dict]:
        """
        Search a player's match history to find stats for our custom match.
        Returns match stats if found within 30 minutes of match end time.
        """
        # Parse IGN (format: Name#Tag)
        if '#' not in player_ign:
            return None

        name, tag = player_ign.rsplit('#', 1)
        matches = await self.get_custom_match_history(name, tag)
        if not matches:
            logger.info(f"Match lookup: No custom match history returned for '{name}#{tag}'")
            return None

        logger.info(f"Match lookup: Found {len(matches)} custom matches for '{name}#{tag}'")

        # Look for a match whose end time is close to our match end time
        for match in matches[:5]:  # Only check recent matches
            metadata = match.get('metadata', {})

            try:
                # Use game_start (Unix timestamp) + game_length to compute game end
                game_start_ts = metadata.get('game_start')
                game_length = metadata.get('game_length')  # seconds

                if not game_start_ts:
                    logger.debug(f"Match lookup: Skipping match with no game_start timestamp")
                    continue

                game_start = datetime.fromtimestamp(game_start_ts, tz=timezone.utc)

                # Compute game end time if we have game_length, otherwise use game_start
                if game_length:
                    # API returns milliseconds for large values, seconds for small
                    game_length_sec = game_length / 1000 if game_length > 10000 else game_length
                    game_end = game_start + timedelta(seconds=game_length_sec)
                else:
                    game_end = game_start

                time_diff = abs((game_end - match_end_time).total_seconds())

                logger.info(
                    f"Match lookup: game_end={game_end.isoformat()}, "
                    f"match_end={match_end_time.isoformat()}, diff={time_diff:.0f}s"
                )

                # If game ended within 30 minutes of our match decision, this is likely our match
                if time_diff <= 1800:
                    valorant_match_id = metadata.get('matchid')
                    if valorant_match_id:
                        details = await self.get_match_details(valorant_match_id)
                        if details:
                            # v2 API has map as string, v4 has map.name
                            map_data = details.get('metadata', {}).get('map')
                            if isinstance(map_data, dict):
                                map_name = map_data.get('name')
                            else:
                                map_name = map_data  # v2 format - direct string
                            return {
                                'valorant_match_id': valorant_match_id,
                                'details': details,
                                'map': map_name
                            }
            except (ValueError, TypeError) as e:
                logger.warning(f"Match lookup: Error parsing match time: {e}")
                continue

        logger.info(f"Match lookup: No custom match for '{name}#{tag}' matched within time window")
        return None

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

# =============================================================================
# VIEWS & MODALS
# =============================================================================

class GameSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a game."""
    
    def __init__(self, games: List[GameConfig], callback_func):
        options = [
            discord.SelectOption(label=g.name, value=str(g.game_id))
            for g in games
        ]
        if not options:
            options = [discord.SelectOption(label="No games configured", value="none")]
        super().__init__(placeholder="Select a game...", options=options)
        self.callback_func = callback_func
    
    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            await interaction.response.send_message("No games configured yet.", ephemeral=True)
            return
        await self.callback_func(interaction, int(self.values[0]))


class ConfirmView(discord.ui.View):
    """Simple confirmation view."""
    
    def __init__(self, timeout: float = 60.0):
        super().__init__(timeout=timeout)
        self.value = None
    
    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        await interaction.response.defer()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        await interaction.response.defer()


# =============================================================================
# CONSOLIDATED ACTION VIEWS
# =============================================================================

class BlacklistActionView(discord.ui.View):
    """Consolidated view for blacklist actions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add", style=discord.ButtonStyle.danger)
    async def add_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BlacklistUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to blacklist:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.success)
    async def remove_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        if not blacklisted:
            await interaction.response.send_message("No blacklisted players.", ephemeral=True)
            return
        view = UnblacklistSelectView(self.cog, blacklisted, interaction.guild)
        await interaction.response.send_message("Select a player to unblacklist:", view=view, ephemeral=True)

    @discord.ui.button(label="View", style=discord.ButtonStyle.secondary)
    async def view_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        if not blacklisted:
            await interaction.response.send_message("No blacklisted players.", ephemeral=True)
            return

        lines = ["**Blacklisted Players**\n"]
        now = datetime.now(timezone.utc)
        for player_id, until in blacklisted:
            if until > now:
                user = interaction.guild.get_member(player_id)
                name = user.display_name if user else str(player_id)
                if until.year == 2099:
                    lines.append(f"• {name}: Permanent")
                else:
                    lines.append(f"• {name}: Until {until.strftime('%Y-%m-%d %H:%M')} UTC")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)


class GameManagementView(discord.ui.View):
    """Consolidated view for game management actions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add", style=discord.ButtonStyle.success)
    async def add_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AddGameModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary)
    async def edit_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_edit_game_modal))
        await interaction.response.send_message("Select a game to edit:", view=view, ephemeral=True)

    async def show_edit_game_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = EditGameModal(self.cog, game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger)
    async def remove_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.confirm_remove_game))
        await interaction.response.send_message("Select a game to remove:", view=view, ephemeral=True)

    async def confirm_remove_game(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to remove **{game.name}**? This cannot be undone.",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            await DatabaseHelper.delete_game(game_id)
            await interaction.followup.send(f"Removed **{game.name}**.", ephemeral=True)

    @discord.ui.button(label="Set Banner", style=discord.ButtonStyle.secondary)
    async def set_banner(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_banner_modal))
        await interaction.response.send_message("Select a game to set banner URL:", view=view, ephemeral=True)

    async def show_banner_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = SetBannerModal(self.cog, game)
        await interaction.response.send_modal(modal)


class SetBannerModal(discord.ui.Modal, title="Set Queue Banner"):
    banner_url = discord.ui.TextInput(
        label="Banner URL (leave blank to clear)",
        placeholder="https://example.com/banner.png or .gif",
        required=False,
        style=discord.TextStyle.short
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        if game.banner_url:
            self.banner_url.default = game.banner_url

    async def on_submit(self, interaction: discord.Interaction):
        url = self.banner_url.value.strip() if self.banner_url.value else None
        await DatabaseHelper.update_game(self.game.game_id, banner_url=url)
        if url:
            await interaction.response.send_message(f"Banner set for **{self.game.name}**.", ephemeral=True)
        else:
            await interaction.response.send_message(f"Banner cleared for **{self.game.name}**.", ephemeral=True)


class ChannelSettingsView(discord.ui.View):
    """Consolidated view for channel settings."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Log Channel", style=discord.ButtonStyle.secondary)
    async def log_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = LogChannelSelectView(self.cog)
        await interaction.response.send_message("Select a log channel:", view=view, ephemeral=True)

    @discord.ui.button(label="Match History", style=discord.ButtonStyle.secondary)
    async def history_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_match_history_select))
        await interaction.response.send_message("Select a game to set match history channel:", view=view, ephemeral=True)

    async def show_match_history_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = MatchHistoryChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.match_history_channel_id) if game.match_history_channel_id else None
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current match history channel for **{game.name}**: {current_str}\n\nSelect a new channel:",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Game Channel", style=discord.ButtonStyle.secondary)
    async def game_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_game_channel_select))
        await interaction.response.send_message("Select a game to set game channel:", view=view, ephemeral=True)

    async def show_game_channel_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = GameChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.game_channel_id) if game.game_channel_id else None
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current game channel for **{game.name}**: {current_str}\n"
            f"(Match results will be posted here)\n\nSelect a new channel:",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Leaderboard", style=discord.ButtonStyle.secondary)
    async def leaderboard_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_leaderboard_channel_select))
        await interaction.response.send_message("Select a game to set leaderboard channel:", view=view, ephemeral=True)

    async def show_leaderboard_channel_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = LeaderboardChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.leaderboard_channel_id) if game.leaderboard_channel_id else None
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current leaderboard channel for **{game.name}**: {current_str}\n"
            f"(Persistent leaderboard will be posted here)\n\nSelect a new channel:",
            view=view, ephemeral=True
        )


# =============================================================================
# SETTINGS PANEL
# =============================================================================

class SettingsView(discord.ui.View):
    """Main settings panel for server admins."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(label="Select Category", style=discord.ButtonStyle.secondary, row=0)
    async def select_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = CategorySelectView(self.cog)
        await interaction.response.send_message("Select a category for match channels:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Channels", style=discord.ButtonStyle.secondary, row=0)
    async def set_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ChannelSettingsView(self.cog)
        await interaction.response.send_message("Select channel type to configure:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.secondary, row=0)
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AdminRoleSelectView(self.cog)
        await interaction.response.send_message("Select the CM Admin role:", view=view, ephemeral=True)

    @discord.ui.button(label="Games", style=discord.ButtonStyle.primary, row=1)
    async def games_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = GameManagementView(self.cog)
        await interaction.response.send_message("Select game action:", view=view, ephemeral=True)

    @discord.ui.button(label="Configure MMR Roles", style=discord.ButtonStyle.primary, row=1)
    async def config_mmr_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_mmr_roles_panel))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    async def show_mmr_roles_panel(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        mmr_roles = await DatabaseHelper.get_mmr_roles(game_id)

        lines = [f"**MMR Roles for {game.name}**\n"]
        if mmr_roles:
            for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
                role = interaction.guild.get_role(role_id)
                role_name = role.name if role else f"Unknown ({role_id})"
                lines.append(f"• {role_name}: {mmr} MMR")
        else:
            lines.append("No MMR roles configured.")

        view = MMRRolesView(self.cog, game_id)
        await interaction.response.send_message("\n".join(lines), view=view, ephemeral=True)

    @discord.ui.button(label="Blacklist", style=discord.ButtonStyle.danger, row=2)
    async def blacklist_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BlacklistActionView(self.cog)
        await interaction.response.send_message("Select blacklist action:", view=view, ephemeral=True)

    @discord.ui.button(label="Mod Roles", style=discord.ButtonStyle.secondary, row=2)
    async def mod_roles_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        lines = ["**Mod Roles**\n", "These roles get permission to type and manage messages in all match channels.\n"]
        if mod_role_ids:
            for role_id in mod_role_ids:
                role = interaction.guild.get_role(role_id)
                role_name = role.name if role else f"Unknown ({role_id})"
                lines.append(f"• {role_name}")
        else:
            lines.append("No mod roles configured.")
        view = ModRolesView(self.cog)
        await interaction.response.send_message("\n".join(lines), view=view, ephemeral=True)

    @discord.ui.button(label="Ready Emojis", style=discord.ButtonStyle.secondary, row=2)
    async def ready_emojis_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_ready_emojis_modal))
        await interaction.response.send_message("Select a game to configure ready emojis:", view=view, ephemeral=True)

    async def show_ready_emojis_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)

        # Show current emojis and prompt for new ones
        await interaction.response.send_message(
            f"**Configure Ready Emojis for {game.name}**\n\n"
            f"Current Loading Emoji: {game.ready_loading_emoji}\n"
            f"Current Ready Emoji: {game.ready_done_emoji}\n\n"
            f"**Step 1:** Send the **loading** emoji (shown while waiting for players):",
            ephemeral=True
        )

        # Wait for loading emoji
        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.cog.bot.wait_for('message', timeout=60.0, check=check)
            loading_emoji = msg.content.strip()
            try:
                await msg.delete()
            except Exception:
                pass

            await interaction.edit_original_response(
                content=f"**Configure Ready Emojis for {game.name}**\n\n"
                f"Loading Emoji: {loading_emoji}\n\n"
                f"**Step 2:** Now send the **ready** emoji (shown when a player is ready):"
            )

            # Wait for ready emoji
            msg = await self.cog.bot.wait_for('message', timeout=60.0, check=check)
            done_emoji = msg.content.strip()
            try:
                await msg.delete()
            except Exception:
                pass

            # Save to database
            await DatabaseHelper.update_game(
                game.game_id,
                ready_loading_emoji=loading_emoji,
                ready_done_emoji=done_emoji
            )

            await interaction.edit_original_response(
                content=f"**Ready emojis updated for {game.name}!**\n\n"
                f"Loading: {loading_emoji}\n"
                f"Ready: {done_emoji}"
            )

        except asyncio.TimeoutError:
            await interaction.edit_original_response(
                content="Emoji setup timed out. Please try again."
            )

    # Row 3: Game settings
    @discord.ui.button(label="Game Toggles", style=discord.ButtonStyle.primary, row=3)
    async def game_toggles(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_game_toggles))
        await interaction.response.send_message("Select a game to configure toggles:", view=view, ephemeral=True)

    async def show_game_toggles(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = GameTogglesView(self.cog, game)
        embed = discord.Embed(title=f"{game.name} Toggles", color=COLOR_NEUTRAL)
        embed.add_field(name="VC Creation", value="Enabled" if game.vc_creation_enabled else "Disabled", inline=True)
        embed.add_field(name="Queue Role Required", value="Yes" if game.queue_role_required else "No", inline=True)
        embed.add_field(name="DM Ready-Up", value="Enabled" if game.dm_ready_up else "Disabled", inline=True)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Penalty Settings", style=discord.ButtonStyle.primary, row=3)
    async def penalty_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_penalty_settings))
        await interaction.response.send_message("Select a game to configure penalties:", view=view, ephemeral=True)

    async def show_penalty_settings(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = PenaltySettingsView(self.cog, game)
        embed = discord.Embed(title=f"{game.name} Penalty Settings", color=COLOR_NEUTRAL)
        embed.add_field(name="1st Offense", value=f"{game.penalty_1st_minutes} min", inline=True)
        embed.add_field(name="2nd Offense", value=f"{game.penalty_2nd_minutes} min", inline=True)
        embed.add_field(name="3rd+ Offense", value=f"{game.penalty_3rd_minutes} min", inline=True)
        embed.add_field(name="Decay Period", value=f"{game.penalty_decay_days} days", inline=True)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Ready Timer", style=discord.ButtonStyle.primary, row=3)
    async def ready_timer_setting(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_ready_timer_modal))
        await interaction.response.send_message("Select a game to configure ready timer:", view=view, ephemeral=True)

    async def show_ready_timer_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = ReadyTimerModal(self.cog, game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Queue Schedule", style=discord.ButtonStyle.primary, row=3)
    async def queue_schedule_setting(self, interaction: discord.Interaction, button: discord.ui.Button):
        print("[CUSTOMMATCH] queue_schedule_setting button clicked", flush=True)
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_schedule_settings))
        await interaction.response.send_message("Select a game to configure queue schedule:", view=view, ephemeral=True)

    async def show_schedule_settings(self, interaction: discord.Interaction, game_id: int):
        print(f"[CUSTOMMATCH] show_schedule_settings called, game_id={game_id}")
        game = await DatabaseHelper.get_game(game_id)
        print(f"[CUSTOMMATCH] Creating QueueScheduleView for {game.name}")
        view = QueueScheduleView(self.cog, game)
        print(f"[CUSTOMMATCH] QueueScheduleView created with {len(view.children)} children")
        embed = discord.Embed(title=f"{game.name} Queue Schedule", color=COLOR_NEUTRAL)
        embed.add_field(name="Enabled", value="Yes" if game.schedule_enabled else "No", inline=False)

        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        # Show per-day schedule times if available
        if game.schedule_times:
            schedule_lines = []
            for day_num, times in sorted(game.schedule_times.items(), key=lambda x: int(x[0])):
                day_name = day_names[int(day_num)]
                open_time = times.get("open", "?")
                close_time = times.get("close", "?")
                schedule_lines.append(f"**{day_name}:** {open_time} - {close_time}")
            if schedule_lines:
                embed.add_field(name="Schedule", value="\n".join(schedule_lines), inline=False)
            else:
                embed.add_field(name="Schedule", value="No days configured", inline=False)
        # Fall back to legacy format
        elif game.schedule_open_days:
            days = game.schedule_open_days.split(",")
            open_days_str = ", ".join(day_names[int(d)] for d in days if d.isdigit())
            embed.add_field(name="Open Days", value=open_days_str, inline=True)
            embed.add_field(name="Open Time", value=game.schedule_open_time or "Not set", inline=True)
            embed.add_field(name="Close Time", value=game.schedule_close_time or "Not set", inline=True)
        else:
            embed.add_field(name="Schedule", value="Not configured", inline=False)

        embed.set_footer(text="Times are in server timezone (bot host time)")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Mass Register", style=discord.ButtonStyle.success, row=4)
    async def mass_register(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_mass_register))
        await interaction.response.send_message(
            "**Mass Register Players**\n"
            "This will scan all server members and register anyone with an MMR role for the selected game.\n\n"
            "Select a game:",
            view=view, ephemeral=True
        )

    async def do_mass_register(self, interaction: discord.Interaction, game_id: int):
        await interaction.response.defer(ephemeral=True)

        game = await DatabaseHelper.get_game(game_id)
        if not game:
            await interaction.followup.send("Game not found.", ephemeral=True)
            return

        # Get MMR roles for this game (returns {role_id: mmr_value})
        role_mmr_map = await DatabaseHelper.get_mmr_roles(game_id)
        if not role_mmr_map:
            await interaction.followup.send(
                f"No MMR roles configured for **{game.name}**. "
                "Set up MMR roles first in Game Settings.",
                ephemeral=True
            )
            return

        # Scan all members
        registered = 0
        skipped = 0
        errors = []

        for member in interaction.guild.members:
            if member.bot:
                continue

            # Find highest MMR role this member has
            member_mmr = None
            for role in member.roles:
                if role.id in role_mmr_map:
                    role_mmr = role_mmr_map[role.id]
                    if member_mmr is None or role_mmr > member_mmr:
                        member_mmr = role_mmr

            if member_mmr is not None:
                try:
                    # Check if player already has stats
                    stats = await DatabaseHelper.get_player_stats(member.id, game_id)
                    if stats.games_played > 0:
                        skipped += 1
                        continue

                    # Set their MMR
                    stats.mmr = member_mmr
                    await DatabaseHelper.update_player_stats(stats)
                    registered += 1
                except Exception as e:
                    errors.append(f"{member.display_name}: {e}")

        result = f"**Mass Registration Complete for {game.name}**\n"
        result += f"Registered: {registered} players\n"
        result += f"Skipped (already have games): {skipped} players\n"

        if errors:
            result += f"\nErrors ({len(errors)}):\n"
            result += "\n".join(errors[:5])
            if len(errors) > 5:
                result += f"\n... and {len(errors) - 5} more"

        await interaction.followup.send(result, ephemeral=True)

    @discord.ui.button(label="Test Stats Card", style=discord.ButtonStyle.secondary, row=4)
    async def test_stats_card(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = TestStatsCardView(self.cog, interaction.user)
        await interaction.response.send_message(
            "**Test Stats Card Generator**\n\n"
            "Choose a test mode:\n"
            "• **Random Data** - Generate card with sample/random stats\n"
            "• **My Stats** - Use your actual CM stats (requires a game with stats)\n",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Wipe Stats", style=discord.ButtonStyle.danger, row=4)
    async def wipe_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Wipe all player stats except MMR."""
        view = StatsWipeConfirmView(self.cog)
        await interaction.response.send_message(
            "**⚠️ DANGER: Wipe All Stats**\n\n"
            "This will reset the following for ALL players:\n"
            "• Wins, Losses, Games Played\n"
            "• Valorant match stats (K/D/A, damage, etc.)\n"
            "• Rivalries and teammate stats\n\n"
            "**MMR will be preserved.**\n\n"
            "Are you sure you want to continue?",
            view=view, ephemeral=True
        )


class StatsWipeConfirmView(discord.ui.View):
    """Confirmation view for wiping stats."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Stats wipe cancelled.", view=None)

    @discord.ui.button(label="Yes, Wipe Stats", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Wipe stats but preserve MMR
        async with aiosqlite.connect(DB_PATH) as db:
            # Reset player_stats (keep MMR)
            await db.execute("""
                UPDATE player_stats
                SET wins = 0, losses = 0, games_played = 0, last_played = NULL
            """)

            # Clear valorant match stats
            await db.execute("DELETE FROM valorant_match_stats")

            # Clear rivalries
            await db.execute("DELETE FROM rivalries")

            # Clear win votes and abandon votes
            await db.execute("DELETE FROM win_votes")
            await db.execute("DELETE FROM abandon_votes")

            # Clear valorant regulars
            await db.execute("DELETE FROM valorant_regulars")

            await db.commit()

        await self.cog.log_action(
            interaction.guild,
            f"Stats wiped by {interaction.user.display_name} (MMR preserved)"
        )

        await interaction.edit_original_response(
            content="✅ **Stats have been wiped.**\n\nAll wins, losses, and match data have been reset. MMR has been preserved.",
            view=None
        )


class TestStatsCardView(discord.ui.View):
    """View for testing stats card generation."""

    def __init__(self, cog: 'CustomMatch', user: discord.Member):
        super().__init__(timeout=120)
        self.cog = cog
        self.user = user

    @discord.ui.button(label="Random Data", style=discord.ButtonStyle.primary)
    async def test_random(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Check if stats generator is available
        if not self.cog.stats_generator.browser:
            await interaction.followup.send(
                "**Stats Card Generator Status: NOT WORKING**\n\n"
                "The Playwright browser is not initialized.\n"
                "Possible causes:\n"
                "• Playwright is not installed (`pip install playwright`)\n"
                "• Browser not installed (`playwright install chromium`)\n"
                "• Initialization failed on cog load\n\n"
                "Stats will fall back to embed-only mode.",
                ephemeral=True
            )
            return

        # Generate random test data
        import random
        wins = random.randint(15, 50)
        losses = random.randint(10, 40)

        # Generate random recent matches for dropdown
        recent_matches = [
            {'match_id': 1, 'short_id': 'ABC12', 'won': True, 'map_name': 'Ascent', 'agent': 'Jett', 'kills': 25, 'deaths': 15, 'assists': 5, 'team': 'red', 'winning_team': 'red'},
            {'match_id': 2, 'short_id': 'DEF34', 'won': False, 'map_name': 'Bind', 'agent': 'Reyna', 'kills': 18, 'deaths': 20, 'assists': 3, 'team': 'red', 'winning_team': 'blue'},
            {'match_id': 3, 'short_id': 'GHI56', 'won': True, 'map_name': 'Haven', 'agent': 'Omen', 'kills': 22, 'deaths': 12, 'assists': 8, 'team': 'blue', 'winning_team': 'blue'},
            {'match_id': 4, 'short_id': 'JKL78', 'won': True, 'map_name': 'Split', 'agent': 'Jett', 'kills': 30, 'deaths': 10, 'assists': 2, 'team': 'red', 'winning_team': 'red'},
            {'match_id': 5, 'short_id': 'MNO90', 'won': False, 'map_name': 'Icebox', 'agent': 'Sage', 'kills': 12, 'deaths': 18, 'assists': 15, 'team': 'blue', 'winning_team': 'red'},
        ]

        test_data = {
            'avatar_url': str(self.user.display_avatar.url),
            'player_name': self.user.display_name,
            'period_title': 'Test Card - Random Data',
            'games_played': wins + losses,
            'wins': wins,
            'losses': losses,
            'total_kills': random.randint(300, 1000),
            'total_deaths': random.randint(200, 800),
            'total_assists': random.randint(100, 500),
            'total_score': random.randint(10000, 50000),
            'total_damage': random.randint(50000, 150000),
            'total_first_bloods': random.randint(10, 50),
            'hs_percent': random.randint(18, 32),
            'longest_win_streak': random.randint(3, 8),
            'longest_loss_streak': random.randint(2, 5),
            'agent_stats': {
                'Jett': {'games': 15, 'wins': 9, 'losses': 6},
                'Reyna': {'games': 12, 'wins': 8, 'losses': 4},
                'Omen': {'games': 8, 'wins': 4, 'losses': 4},
                'Sage': {'games': 5, 'wins': 2, 'losses': 3},
                'Sova': {'games': 4, 'wins': 3, 'losses': 1},
                'Killjoy': {'games': 3, 'wins': 2, 'losses': 1},
                'Viper': {'games': 3, 'wins': 1, 'losses': 2},
                'Breach': {'games': 2, 'wins': 1, 'losses': 1},
                'Cypher': {'games': 2, 'wins': 2, 'losses': 0},
                'Phoenix': {'games': 1, 'wins': 0, 'losses': 1},
            },
            'map_stats': [
                {'name': 'Ascent', 'games': 10, 'wins': 7, 'losses': 3, 'winrate': 70.0},
                {'name': 'Haven', 'games': 8, 'wins': 5, 'losses': 3, 'winrate': 62.5},
                {'name': 'Bind', 'games': 7, 'wins': 4, 'losses': 3, 'winrate': 57.1},
                {'name': 'Split', 'games': 6, 'wins': 3, 'losses': 3, 'winrate': 50.0},
                {'name': 'Icebox', 'games': 5, 'wins': 2, 'losses': 3, 'winrate': 40.0},
                {'name': 'Pearl', 'games': 4, 'wins': 2, 'losses': 2, 'winrate': 50.0},
                {'name': 'Lotus', 'games': 3, 'wins': 1, 'losses': 2, 'winrate': 33.3},
                {'name': 'Abyss', 'games': 2, 'wins': 0, 'losses': 2, 'winrate': 0.0},
            ],
            'recent_matches': recent_matches,
            'best_teammates': [
                {'name': 'User 1', 'wins': 18, 'losses': 3},
                {'name': 'User 2', 'wins': 13, 'losses': 8},
                {'name': 'User 3', 'wins': 9, 'losses': 8},
            ],
            'worst_teammates': [
                {'name': 'User 1', 'wins': 18, 'losses': 31},
                {'name': 'User 2', 'wins': 13, 'losses': 21},
                {'name': 'User 3', 'wins': 9, 'losses': 18},
            ]
        }

        try:
            # Generate seasonal (random) and lifetime (same but different title) images
            seasonal_image = await self.cog.stats_generator.generate_stats_image(test_data)
            test_data['period_title'] = 'Test Card - Lifetime'
            lifetime_image = await self.cog.stats_generator.generate_stats_image(test_data)

            if seasonal_image and lifetime_image:
                images = {'seasonal': seasonal_image, 'lifetime': lifetime_image}
                # Create a mock game config for the view
                class MockGame:
                    name = "Test Game"
                view = TestStatsImageView(
                    self.cog, self.user, MockGame(), images, recent_matches,
                    invoker_id=interaction.user.id
                )

                images['seasonal'].seek(0)
                file = discord.File(images['seasonal'], filename='stats_seasonal.png')
                embed = discord.Embed(
                    title="Stats Card Test - SUCCESS",
                    description="Use the dropdown to switch between views.",
                    color=0x00ff00
                )
                embed.set_image(url="attachment://stats_seasonal.png")
                await interaction.followup.send(embed=embed, file=file, view=view, ephemeral=True)
            else:
                await interaction.followup.send(
                    "**Stats Card Test - FAILED**\n\n"
                    "The generator returned None. Check the logs for errors.\n"
                    "Common issues:\n"
                    "• Template file missing (`cogs/templates/stats_card.html`)\n"
                    "• Font files missing (`fonts/` directory)\n"
                    "• HTML template syntax errors",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.followup.send(
                f"**Stats Card Test - ERROR**\n\n"
                f"Exception: `{type(e).__name__}: {e}`\n\n"
                "Check the bot logs for full traceback.",
                ephemeral=True
            )

    @discord.ui.button(label="My Stats", style=discord.ButtonStyle.success)
    async def test_my_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_my_stats))
        await interaction.response.send_message("Select a game to view your stats:", view=view, ephemeral=True)

    async def show_my_stats(self, interaction: discord.Interaction, game_id: int):
        await interaction.response.defer()

        # Check if stats generator is available
        if not self.cog.stats_generator.browser:
            await interaction.followup.send(
                "**Stats Card Generator Status: NOT WORKING**\n\n"
                "Playwright browser not initialized. Stats will use embeds only.",
                ephemeral=True
            )
            return

        game = await DatabaseHelper.get_game(game_id)
        stats = await DatabaseHelper.get_player_stats(self.user.id, game_id)
        valorant_stats = await DatabaseHelper.get_valorant_player_stats(self.user.id, game_id, monthly=False)
        recent_matches_raw = await DatabaseHelper.get_player_recent_matches(self.user.id, game_id, limit=5)
        streak_stats = await DatabaseHelper.get_player_streak_stats(self.user.id, game_id, monthly=False)
        teammate_stats = await DatabaseHelper.get_all_teammate_stats(self.user.id, game_id, monthly=False)

        # Format recent matches
        recent_matches = []
        for match in recent_matches_raw:
            won = match.get('team') == match.get('winning_team')
            recent_matches.append({
                'won': won,
                'kills': match.get('kills'),
                'deaths': match.get('deaths'),
                'assists': match.get('assists'),
                'agent': match.get('agent'),
                'map_name': match.get('map_name')
            })

        # Resolve teammate names
        def get_teammate_name(player_id: int) -> str:
            m = interaction.guild.get_member(player_id)
            if m:
                name = m.display_name
                return name[:12] + '...' if len(name) > 15 else name
            return f"User {player_id}"

        best_teammates = []
        for t in teammate_stats.get('best_teammates', []):
            best_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        worst_teammates = []
        for t in teammate_stats.get('worst_teammates', []):
            worst_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        test_data = {
            'avatar_url': str(self.user.display_avatar.url),
            'player_name': self.user.display_name,
            'period_title': f'{game.name} Stats (Your Data)',
            'games_played': stats.games_played,
            'wins': stats.wins,
            'losses': stats.losses,
            'total_kills': valorant_stats.get('total_kills', 0),
            'total_deaths': valorant_stats.get('total_deaths', 0),
            'total_assists': valorant_stats.get('total_assists', 0),
            'total_score': valorant_stats.get('total_score', 0),
            'total_damage': valorant_stats.get('total_damage', 0),
            'total_first_bloods': valorant_stats.get('total_first_bloods', 0),
            'hs_percent': valorant_stats.get('hs_percent', 0),
            'longest_win_streak': streak_stats.get('longest_win_streak', 0),
            'longest_loss_streak': streak_stats.get('longest_loss_streak', 0),
            'agent_stats': valorant_stats.get('agent_stats', {}),
            'map_stats': valorant_stats.get('map_stats', []),
            'recent_matches': recent_matches,
            'best_teammates': best_teammates,
            'worst_teammates': worst_teammates
        }

        try:
            image = await self.cog.stats_generator.generate_stats_image(test_data)
            if image:
                file = discord.File(image, filename='my_stats.png')
                embed = discord.Embed(
                    title=f"Your {game.name} Stats Card",
                    color=0x00ff00
                )
                embed.set_image(url="attachment://my_stats.png")
                await interaction.followup.send(embed=embed, file=file, ephemeral=True)
            else:
                # Fallback to embed
                embed = discord.Embed(
                    title=f"Your {game.name} Stats (Embed Fallback)",
                    description="Image generation failed. Showing embed instead.",
                    color=0xff9900
                )
                embed.add_field(name="W/L", value=f"{stats.wins}/{stats.losses}", inline=True)
                embed.add_field(name="Games", value=str(stats.games_played), inline=True)
                tk = valorant_stats.get('total_kills', 0)
                td = valorant_stats.get('total_deaths', 0)
                ta = valorant_stats.get('total_assists', 0)
                if tk > 0:
                    embed.add_field(name="K/D/A", value=f"{tk}/{td}/{ta}", inline=True)
                await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"**Error:** `{e}`", ephemeral=True)


class TestStatsSelectDropdown(discord.ui.Select):
    """Dropdown for selecting test stats view."""

    def __init__(self, recent_matches: List[dict]):
        options = [
            discord.SelectOption(label="Monthly", value="seasonal", description="Monthly test stats", default=True),
            discord.SelectOption(label="Lifetime", value="lifetime", description="Lifetime test stats"),
        ]
        # Add recent matches as options
        for i, match in enumerate(recent_matches[:5]):
            map_name = match.get('map_name') or "Unknown"
            kills = match.get('kills', 0) or 0
            deaths = match.get('deaths', 0) or 0
            assists = match.get('assists', 0) or 0
            kda = f"{kills}/{deaths}/{assists}"
            label = f"{map_name} {kda}"
            if len(label) > 25:
                label = label[:22] + "..."
            match_id = match.get('match_id', i)
            options.append(discord.SelectOption(
                label=label,
                value=f"match_{match_id}",
                description=f"Match #{match.get('short_id') or match_id}"
            ))

        super().__init__(placeholder="Select stats view...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await self.view.handle_selection(interaction, self.values[0])


class TestStatsImageView(discord.ui.View):
    """View for test stats card image with dropdown selection."""

    def __init__(self, cog: 'CustomMatch', member: discord.Member, game,
                 images: Dict[str, io.BytesIO], recent_matches: List[dict], invoker_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.member = member
        self.game = game
        self.images = images
        self.recent_matches = recent_matches
        self.invoker_id = invoker_id
        self.current = 'seasonal'
        self.add_item(TestStatsSelectDropdown(recent_matches))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Only the person who ran this command can use this dropdown.",
                ephemeral=True
            )
            return False
        return True

    async def handle_selection(self, interaction: discord.Interaction, value: str):
        self.current = value

        # Update dropdown default
        for item in self.children:
            if isinstance(item, TestStatsSelectDropdown):
                for option in item.options:
                    option.default = (option.value == value)

        if value in self.images:
            self.images[value].seek(0)
            filename = f'stats_{value}.png'
            file = discord.File(self.images[value], filename=filename)
            embed = discord.Embed(
                title="Stats Card Test - SUCCESS",
                description="Use the dropdown to switch between views.",
                color=0x00ff00
            )
            embed.set_image(url=f"attachment://{filename}")
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
        elif value.startswith('match_'):
            # Generate match-specific scoreboard image on demand
            await interaction.response.defer()

            match_id = int(value.replace('match_', ''))
            match_data = None
            for m in self.recent_matches:
                if m.get('match_id') == match_id:
                    match_data = m
                    break

            if not match_data:
                await interaction.followup.send("Match not found.", ephemeral=True)
                return

            import random
            map_name = match_data.get('map_name') or "Unknown"
            kills = match_data.get('kills', 0) or 0
            deaths = match_data.get('deaths', 0) or 0
            assists = match_data.get('assists', 0) or 0
            agent = match_data.get('agent', 'Unknown')
            won = match_data.get('team') == match_data.get('winning_team')

            # Generate random test data for scoreboard
            headshots = random.randint(5, 15)
            bodyshots = random.randint(20, 50)
            legshots = random.randint(2, 10)
            scoreboard_data = {
                'player_name': self.member.display_name,
                'avatar_url': self.member.display_avatar.url,
                'map_name': map_name,
                'agent': agent,
                'won': won,
                'kills': kills,
                'deaths': deaths,
                'assists': assists,
                'score': random.randint(3000, 7000),
                'damage': random.randint(2000, 5000),
                'first_bloods': random.randint(0, 3),
                'headshots': headshots,
                'bodyshots': bodyshots,
                'legshots': legshots,
                'plants': random.randint(0, 3),
                'defuses': random.randint(0, 2),
                'c2k': random.randint(0, 3),
                'c3k': random.randint(0, 2),
                'c4k': random.randint(0, 1),
                'c5k': random.randint(0, 1),
                'econ_spent': random.randint(80000, 120000),
                'econ_loadout': random.randint(60000, 100000),
            }

            image = await self.cog.stats_generator.generate_match_image(scoreboard_data)
            if image:
                self.images[value] = image
                image.seek(0)
                filename = f'match_{match_id}.png'
                file = discord.File(image, filename=filename)
                embed = discord.Embed(
                    title="Match Scoreboard Test - SUCCESS",
                    description="Use the dropdown to switch between views.",
                    color=0x00ff00
                )
                embed.set_image(url=f"attachment://{filename}")
                await interaction.edit_original_response(embed=embed, attachments=[file], view=self)
            else:
                await interaction.followup.send("Failed to generate match scoreboard.", ephemeral=True)


class ModRolesView(discord.ui.View):
    """View for managing mod roles."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add Mod Role", style=discord.ButtonStyle.success)
    async def add_mod_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddModRoleSelectView(self.cog)
        await interaction.response.send_message("Select a role to add as mod role:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Mod Role", style=discord.ButtonStyle.danger)
    async def remove_mod_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        if not mod_role_ids:
            await interaction.response.send_message("No mod roles configured.", ephemeral=True)
            return
        view = RemoveModRoleSelectView(self.cog, mod_role_ids, interaction.guild)
        await interaction.response.send_message("Select a role to remove:", view=view, ephemeral=True)


class AddModRoleSelectView(discord.ui.View):
    """View for selecting a role to add as mod role."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await DatabaseHelper.add_mod_role(role.id)
        await interaction.response.send_message(f"Added **{role.name}** as a mod role.", ephemeral=True)


class RemoveModRoleSelectView(discord.ui.View):
    """View for selecting a mod role to remove."""

    def __init__(self, cog: 'CustomMatch', mod_role_ids: List[int], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        for role_id in mod_role_ids:
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=name, value=str(role_id)))

        select = discord.ui.Select(placeholder="Select role to remove...", options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_mod_role(role_id)
        await interaction.response.send_message("Removed mod role.", ephemeral=True)


class MMRRolesView(discord.ui.View):
    """View for managing MMR roles."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.button(label="Add/Update Role", style=discord.ButtonStyle.success)
    async def add_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddMMRRoleSelectView(self.cog, self.game_id)
        await interaction.response.send_message("Select a role:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Role", style=discord.ButtonStyle.danger)
    async def remove_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        if not mmr_roles:
            await interaction.response.send_message("No MMR roles configured.", ephemeral=True)
            return
        view = RemoveMMRRoleSelectView(self.cog, self.game_id, mmr_roles, interaction.guild)
        await interaction.response.send_message("Select a role to remove:", view=view, ephemeral=True)


class AddMMRRoleSelectView(discord.ui.View):
    """View for selecting a role to add as MMR role."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        modal = MMRValueModal(self.cog, self.game_id, role.id, role.name)
        await interaction.response.send_modal(modal)


class MMRValueModal(discord.ui.Modal, title="Set MMR Value"):
    mmr_value = discord.ui.TextInput(
        label="MMR Value",
        placeholder="e.g., 1500",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, role_id: int, role_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.role_id = role_id
        self.title = f"Set MMR for {role_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
            await DatabaseHelper.set_mmr_role(self.game_id, self.role_id, mmr)
            await interaction.response.send_message(f"Set role MMR to {mmr}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class RemoveMMRRoleSelectView(discord.ui.View):
    """View for selecting an MMR role to remove."""

    def __init__(self, cog: 'CustomMatch', game_id: int, mmr_roles: Dict[int, int], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=f"{name} ({mmr} MMR)", value=str(role_id)))

        select = discord.ui.Select(placeholder="Select role to remove...", options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_mmr_role(self.game_id, role_id)
        await interaction.response.send_message("Removed MMR role.", ephemeral=True)


class CategorySelectView(discord.ui.View):
    """View for selecting a category channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a category...",
                       channel_types=[discord.ChannelType.category])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        category = select.values[0]
        await DatabaseHelper.set_config("category_id", str(category.id))
        await interaction.response.send_message(f"Category set to **{category.name}**.", ephemeral=True)


class LogChannelSelectView(discord.ui.View):
    """View for selecting a log channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.set_config("log_channel_id", str(channel.id))
        await interaction.response.send_message(f"Log channel set to {channel.mention}.", ephemeral=True)


class AdminRoleSelectView(discord.ui.View):
    """View for selecting the admin role."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await DatabaseHelper.set_config("cm_admin_role_id", str(role.id))
        await interaction.response.send_message(f"CM Admin role set to **{role.name}**.", ephemeral=True)


class BlacklistUserSelectView(discord.ui.View):
    """View for selecting a user to blacklist."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        modal = BlacklistDurationModal(self.cog, user.id, user.display_name)
        await interaction.response.send_modal(modal)


class BlacklistDurationModal(discord.ui.Modal, title="Blacklist Duration"):
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, user_name: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.title = f"Blacklist {user_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.duration.value)
            if days <= 0:
                await DatabaseHelper.blacklist_player(self.user_id)
                await interaction.response.send_message(f"Permanently blacklisted user.", ephemeral=True)
            else:
                until = datetime.now(timezone.utc) + timedelta(days=days)
                await DatabaseHelper.blacklist_player(self.user_id, until)
                await interaction.response.send_message(f"Blacklisted user for {days} days.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)


class UnblacklistSelectView(discord.ui.View):
    """View for selecting a user to unblacklist."""

    def __init__(self, cog: 'CustomMatch', blacklisted: List[Tuple[int, datetime]], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        now = datetime.now(timezone.utc)
        for player_id, until in blacklisted:
            if until > now:
                user = guild.get_member(player_id)
                name = user.display_name if user else str(player_id)
                duration = "Permanent" if until.year == 2099 else f"Until {until.strftime('%Y-%m-%d')}"
                options.append(discord.SelectOption(label=name, value=str(player_id), description=duration))

        if options:
            select = discord.ui.Select(placeholder="Select player...", options=options[:25])
            select.callback = self.on_select
            self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        player_id = int(interaction.data["values"][0])
        await DatabaseHelper.unblacklist_player(player_id)
        await interaction.response.send_message("Player unblacklisted.", ephemeral=True)


class GameTogglesView(discord.ui.View):
    """View for toggling game settings."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()

        vc_btn = discord.ui.Button(
            label=f"VC Creation: {'ON' if self.game.vc_creation_enabled else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.vc_creation_enabled else discord.ButtonStyle.secondary
        )
        vc_btn.callback = self.toggle_vc
        self.add_item(vc_btn)

        role_btn = discord.ui.Button(
            label=f"Role Required: {'ON' if self.game.queue_role_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.queue_role_required else discord.ButtonStyle.secondary
        )
        role_btn.callback = self.toggle_role
        self.add_item(role_btn)

        dm_btn = discord.ui.Button(
            label=f"DM Ready: {'ON' if self.game.dm_ready_up else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.dm_ready_up else discord.ButtonStyle.secondary
        )
        dm_btn.callback = self.toggle_dm
        self.add_item(dm_btn)

        # Verification topic button
        topic_label = f"Verify Topic: {self.game.verification_topic or 'None'}"
        if len(topic_label) > 80:
            topic_label = topic_label[:77] + "..."
        topic_btn = discord.ui.Button(
            label=topic_label,
            style=discord.ButtonStyle.primary if self.game.verification_topic else discord.ButtonStyle.secondary,
            row=1
        )
        topic_btn.callback = self.set_verification_topic
        self.add_item(topic_btn)

    async def set_verification_topic(self, interaction: discord.Interaction):
        modal = VerificationTopicModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def toggle_vc(self, interaction: discord.Interaction):
        new_val = not self.game.vc_creation_enabled
        await DatabaseHelper.update_game(self.game.game_id, vc_creation_enabled=int(new_val))
        self.game.vc_creation_enabled = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def toggle_role(self, interaction: discord.Interaction):
        new_val = not self.game.queue_role_required
        await DatabaseHelper.update_game(self.game.game_id, queue_role_required=int(new_val))
        self.game.queue_role_required = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def toggle_dm(self, interaction: discord.Interaction):
        new_val = not self.game.dm_ready_up
        await DatabaseHelper.update_game(self.game.game_id, dm_ready_up=int(new_val))
        self.game.dm_ready_up = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def _refresh_queue_embeds(self, guild: discord.Guild):
        """Refresh all queue embeds for this game after settings change."""
        try:
            for queue_id, queue_state in self.cog.queues.items():
                if queue_state.game_id == self.game.game_id and queue_state.message_id:
                    channel = guild.get_channel(queue_state.channel_id)
                    if channel:
                        try:
                            msg = await channel.fetch_message(queue_state.message_id)
                            # Reload game config to get fresh settings
                            fresh_game = await DatabaseHelper.get_game(self.game.game_id)
                            embed = await self.cog.create_queue_embed(fresh_game, queue_state)
                            if queue_state.state == "ready_check":
                                view = ReadyCheckView(self.cog, self.game.game_id, queue_id)
                            else:
                                view = QueueView(self.cog, self.game.game_id, queue_id)
                            await msg.edit(embed=embed, view=view)
                        except discord.NotFound:
                            pass
                        except Exception as e:
                            logger.error(f"Error refreshing queue embed: {e}")
        except Exception as e:
            logger.error(f"Error refreshing queue embeds: {e}")


class VerificationTopicModal(discord.ui.Modal, title="Set Verification Topic"):
    topic_name = discord.ui.TextInput(
        label="Topic Name",
        placeholder="e.g., tenman-(val) - leave empty to clear",
        required=False,
        max_length=100
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: GameTogglesView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        if game.verification_topic:
            self.topic_name.default = game.verification_topic

    async def on_submit(self, interaction: discord.Interaction):
        topic = self.topic_name.value.strip() or None
        await DatabaseHelper.update_game(self.game.game_id, verification_topic=topic)
        self.game.verification_topic = topic
        self.parent_view.update_buttons()

        if topic:
            await interaction.response.send_message(
                f"Verification topic set to `{topic}`.\n"
                "Users without the verified role will see a button to open this ticket.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Verification topic cleared.",
                ephemeral=True
            )


class PenaltySettingsView(discord.ui.View):
    """View for configuring penalty settings."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game

    @discord.ui.button(label="Edit Penalty Durations", style=discord.ButtonStyle.primary)
    async def edit_durations(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = PenaltyDurationsModal(self.cog, self.game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="View Active Penalties", style=discord.ButtonStyle.secondary)
    async def view_penalties(self, interaction: discord.Interaction, button: discord.ui.Button):
        penalties = await DatabaseHelper.get_all_penalties()
        if not penalties:
            await interaction.response.send_message("No active penalties.", ephemeral=True)
            return

        lines = ["**Active Ready Penalties**\n"]
        for p in penalties:
            user = interaction.guild.get_member(p.player_id)
            name = user.display_name if user else str(p.player_id)
            expires = p.penalty_expires.strftime("%Y-%m-%d %H:%M UTC") if p.penalty_expires else "Unknown"
            lines.append(f"• {name}: Offense #{p.offense_count}, expires {expires}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Clear Player Penalty", style=discord.ButtonStyle.danger)
    async def clear_penalty(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ClearPenaltyUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to clear penalty:", view=view, ephemeral=True)


class PenaltyDurationsModal(discord.ui.Modal, title="Penalty Durations"):
    first_offense = discord.ui.TextInput(label="1st Offense (e.g., 60m, 1h, 1d)", required=True)
    second_offense = discord.ui.TextInput(label="2nd Offense (e.g., 60m, 1h, 1d)", required=True)
    third_offense = discord.ui.TextInput(label="3rd+ Offense (e.g., 60m, 1h, 1d)", required=True)
    decay_days = discord.ui.TextInput(label="Decay Period (days)", required=True)

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        self.first_offense.default = str(game.penalty_1st_minutes)
        self.second_offense.default = str(game.penalty_2nd_minutes)
        self.third_offense.default = str(game.penalty_3rd_minutes)
        self.decay_days.default = str(game.penalty_decay_days)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await DatabaseHelper.update_game(
                self.game.game_id,
                penalty_1st_minutes=parse_duration_to_minutes(self.first_offense.value),
                penalty_2nd_minutes=parse_duration_to_minutes(self.second_offense.value),
                penalty_3rd_minutes=parse_duration_to_minutes(self.third_offense.value),
                penalty_decay_days=int(self.decay_days.value)
            )
            await interaction.response.send_message("Penalty settings updated.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid values.", ephemeral=True)


class ClearPenaltyUserSelectView(discord.ui.View):
    """View for selecting a user to clear penalty."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        await DatabaseHelper.clear_ready_penalty(user.id)
        await interaction.response.send_message(f"Cleared penalty for **{user.display_name}**.", ephemeral=True)


class QueueScheduleView(discord.ui.View):
    """View for configuring queue schedule with per-day times."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game
        self.update_toggle_button()

    def update_toggle_button(self):
        for item in self.children:
            if hasattr(item, 'custom_id') and item.custom_id == 'toggle_schedule':
                item.label = f"Schedule: {'ON' if self.game.schedule_enabled else 'OFF'}"
                item.style = discord.ButtonStyle.success if self.game.schedule_enabled else discord.ButtonStyle.secondary

    @discord.ui.button(label="Schedule: OFF", style=discord.ButtonStyle.secondary, custom_id="toggle_schedule")
    async def toggle_schedule(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            new_val = not self.game.schedule_enabled
            print(f"[CUSTOMMATCH] toggle_schedule: {self.game.name} -> schedule_enabled={new_val}", flush=True)
            await DatabaseHelper.update_game(self.game.game_id, schedule_enabled=int(new_val))
            self.game.schedule_enabled = new_val
            button.label = f"Schedule: {'ON' if new_val else 'OFF'}"
            button.style = discord.ButtonStyle.success if new_val else discord.ButtonStyle.secondary
            await interaction.response.edit_message(view=self)

            # Reload game to get latest data
            fresh_game = await DatabaseHelper.get_game(self.game.game_id)
            if not fresh_game or not fresh_game.queue_channel_id:
                return

            channel = self.cog.bot.get_channel(fresh_game.queue_channel_id)
            if not channel:
                return

            if new_val:
                # Schedule ENABLED - apply current schedule state (close if outside hours)
                print(f"[CUSTOMMATCH] toggle_schedule: Applying schedule state for {self.game.name}", flush=True)
                await self.cog.apply_schedule_state(fresh_game)
            else:
                # Schedule DISABLED - delete countdown embed and start fresh queue
                print(f"[CUSTOMMATCH] toggle_schedule: Schedule disabled, cleaning up for {self.game.name}", flush=True)

                # Delete countdown embed if exists
                if fresh_game.schedule_down_message_id:
                    try:
                        down_msg = await channel.fetch_message(fresh_game.schedule_down_message_id)
                        await down_msg.delete()
                        print(f"[CUSTOMMATCH] toggle_schedule: Deleted countdown embed", flush=True)
                    except discord.NotFound:
                        pass
                    await DatabaseHelper.update_game(fresh_game.game_id, schedule_down_message_id=None)

                # Start a fresh queue
                await self.cog.start_queue(channel, fresh_game)
                print(f"[CUSTOMMATCH] toggle_schedule: Started fresh queue", flush=True)
        except Exception as e:
            print(f"[CUSTOMMATCH] toggle_schedule ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()

    @discord.ui.button(label="Add/Edit Day", style=discord.ButtonStyle.primary)
    async def add_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show dropdown to select a day to configure."""
        view = ScheduleDaySelectView(self.cog, self.game)
        await interaction.response.send_message(
            "Select a day to configure its schedule:",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Remove Day", style=discord.ButtonStyle.danger)
    async def remove_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show dropdown to remove a configured day."""
        if not self.game.schedule_times:
            await interaction.response.send_message("No days configured to remove.", ephemeral=True)
            return
        view = ScheduleDayRemoveView(self.cog, self.game)
        await interaction.response.send_message(
            "Select a day to remove from the schedule:",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Quick Setup: Weekdays", style=discord.ButtonStyle.secondary, row=1)
    async def quick_weekdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Quick setup: Mon-Fri with same times."""
        modal = QuickScheduleModal(self.cog, self.game, list(range(5)))  # 0-4 = Mon-Fri
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Quick Setup: Every Day", style=discord.ButtonStyle.secondary, row=1)
    async def quick_everyday(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Quick setup: Every day with same times."""
        modal = QuickScheduleModal(self.cog, self.game, list(range(7)))  # 0-6 = All days
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Clear All", style=discord.ButtonStyle.danger, row=1)
    async def clear_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clear all schedule configuration."""
        await DatabaseHelper.update_game(
            self.game.game_id,
            schedule_times=None,
            schedule_open_days=None,
            schedule_open_time=None,
            schedule_close_time=None
        )
        self.game.schedule_times = None
        await interaction.response.send_message(
            f"Cleared all schedule settings for **{self.game.name}**.",
            ephemeral=True
        )


class ScheduleDaySelectView(discord.ui.View):
    """Dropdown to select a day to configure."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

        options = []
        for i, day in enumerate(self.DAY_NAMES):
            current = ""
            if game.schedule_times and str(i) in game.schedule_times:
                times = game.schedule_times[str(i)]
                current = f" ({times.get('open', '?')} - {times.get('close', '?')})"
            options.append(discord.SelectOption(label=f"{day}{current}", value=str(i)))

        select = discord.ui.Select(placeholder="Select a day...", options=options)
        select.callback = self.day_selected
        self.add_item(select)

    async def day_selected(self, interaction: discord.Interaction):
        day_num = int(interaction.data["values"][0])
        modal = ScheduleDayModal(self.cog, self.game, day_num)
        await interaction.response.send_modal(modal)


class ScheduleDayRemoveView(discord.ui.View):
    """Dropdown to remove a configured day."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

        options = []
        if game.schedule_times:
            for day_num, times in sorted(game.schedule_times.items(), key=lambda x: int(x[0])):
                day_name = self.DAY_NAMES[int(day_num)]
                open_time = times.get("open", "?")
                close_time = times.get("close", "?")
                options.append(discord.SelectOption(
                    label=f"{day_name} ({open_time} - {close_time})",
                    value=day_num
                ))

        if options:
            select = discord.ui.Select(placeholder="Select a day to remove...", options=options)
            select.callback = self.day_removed
            self.add_item(select)

    async def day_removed(self, interaction: discord.Interaction):
        day_num = interaction.data["values"][0]
        if self.game.schedule_times and day_num in self.game.schedule_times:
            del self.game.schedule_times[day_num]
            await DatabaseHelper.update_game(
                self.game.game_id,
                schedule_times=json.dumps(self.game.schedule_times) if self.game.schedule_times else None
            )
            day_name = self.DAY_NAMES[int(day_num)]
            await interaction.response.send_message(
                f"Removed **{day_name}** from the schedule for **{self.game.name}**.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("Day not found in schedule.", ephemeral=True)


class ScheduleDayModal(discord.ui.Modal, title="Set Day Schedule"):
    """Modal for setting open/close times for a specific day."""

    open_time = discord.ui.TextInput(
        label="Open Time (24h format)",
        placeholder="e.g., 16:00",
        required=True,
        max_length=5
    )
    close_time = discord.ui.TextInput(
        label="Close Time (24h format)",
        placeholder="e.g., 23:00",
        required=True,
        max_length=5
    )

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig, day_num: int):
        super().__init__()
        self.cog = cog
        self.game = game
        self.day_num = day_num
        self.title = f"Set {self.DAY_NAMES[day_num]} Schedule"

        # Pre-fill with existing times if available
        if game.schedule_times and str(day_num) in game.schedule_times:
            times = game.schedule_times[str(day_num)]
            self.open_time.default = times.get("open", "")
            self.close_time.default = times.get("close", "")

    async def on_submit(self, interaction: discord.Interaction):
        import re
        time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'

        if not re.match(time_pattern, self.open_time.value):
            await interaction.response.send_message("Invalid open time. Use HH:MM format.", ephemeral=True)
            return
        if not re.match(time_pattern, self.close_time.value):
            await interaction.response.send_message("Invalid close time. Use HH:MM format.", ephemeral=True)
            return

        # Normalize times
        open_parts = self.open_time.value.split(":")
        close_parts = self.close_time.value.split(":")
        open_normalized = f"{int(open_parts[0]):02d}:{open_parts[1]}"
        close_normalized = f"{int(close_parts[0]):02d}:{close_parts[1]}"

        # Update schedule_times
        schedule_times = self.game.schedule_times or {}
        schedule_times[str(self.day_num)] = {"open": open_normalized, "close": close_normalized}

        await DatabaseHelper.update_game(
            self.game.game_id,
            schedule_times=json.dumps(schedule_times)
        )
        self.game.schedule_times = schedule_times

        day_name = self.DAY_NAMES[self.day_num]
        await interaction.response.send_message(
            f"Set **{day_name}** schedule for **{self.game.name}**:\n"
            f"Open: {open_normalized} | Close: {close_normalized}",
            ephemeral=True
        )


class QuickScheduleModal(discord.ui.Modal, title="Quick Schedule Setup"):
    """Modal for quick setup of multiple days with same times."""

    open_time = discord.ui.TextInput(
        label="Open Time (24h format)",
        placeholder="e.g., 16:00",
        required=True,
        max_length=5
    )
    close_time = discord.ui.TextInput(
        label="Close Time (24h format)",
        placeholder="e.g., 23:00",
        required=True,
        max_length=5
    )

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig, days: List[int]):
        super().__init__()
        self.cog = cog
        self.game = game
        self.days = days

        if len(days) == 5:
            self.title = "Weekday Schedule (Mon-Fri)"
        elif len(days) == 7:
            self.title = "Every Day Schedule"
        else:
            self.title = "Quick Schedule Setup"

    async def on_submit(self, interaction: discord.Interaction):
        import re
        time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'

        if not re.match(time_pattern, self.open_time.value):
            await interaction.response.send_message("Invalid open time. Use HH:MM format.", ephemeral=True)
            return
        if not re.match(time_pattern, self.close_time.value):
            await interaction.response.send_message("Invalid close time. Use HH:MM format.", ephemeral=True)
            return

        # Normalize times
        open_parts = self.open_time.value.split(":")
        close_parts = self.close_time.value.split(":")
        open_normalized = f"{int(open_parts[0]):02d}:{open_parts[1]}"
        close_normalized = f"{int(close_parts[0]):02d}:{close_parts[1]}"

        # Build schedule_times
        schedule_times = {}
        for day_num in self.days:
            schedule_times[str(day_num)] = {"open": open_normalized, "close": close_normalized}

        await DatabaseHelper.update_game(
            self.game.game_id,
            schedule_times=json.dumps(schedule_times)
        )
        self.game.schedule_times = schedule_times

        days_str = ", ".join(self.DAY_NAMES[d] for d in self.days)
        await interaction.response.send_message(
            f"Set schedule for **{self.game.name}**:\n"
            f"Days: {days_str}\n"
            f"Open: {open_normalized} | Close: {close_normalized}",
            ephemeral=True
        )


class ReadyTimerModal(discord.ui.Modal, title="Ready Timer Settings"):
    """Modal for configuring the ready timer duration."""

    timer_seconds = discord.ui.TextInput(
        label="Ready Timer (seconds)",
        placeholder="e.g., 60",
        required=True,
        max_length=5
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        self.timer_seconds.default = str(game.ready_timer_seconds)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            seconds = int(self.timer_seconds.value)
            if seconds < 10 or seconds > 600:
                await interaction.response.send_message(
                    "Ready timer must be between 10 and 600 seconds.",
                    ephemeral=True
                )
                return

            await DatabaseHelper.update_game(self.game.game_id, ready_timer_seconds=seconds)
            await interaction.response.send_message(
                f"Ready timer for **{self.game.name}** set to **{seconds} seconds**.\n"
                f"Players will have {seconds} seconds to ready up when a queue fills.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


class MatchHistoryChannelSelectView(discord.ui.View):
    """View for selecting match history channel."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.update_game(self.game_id, match_history_channel_id=channel.id)
        await interaction.response.send_message(f"Match history channel set to {channel.mention}.", ephemeral=True)

    @discord.ui.button(label="Clear (No History)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, match_history_channel_id=None)
        await interaction.response.send_message("Match history channel cleared.", ephemeral=True)


class GameChannelSelectView(discord.ui.View):
    """View for selecting game channel (for match results)."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.update_game(self.game_id, game_channel_id=channel.id)
        await interaction.response.send_message(f"Game channel set to {channel.mention}.", ephemeral=True)

    @discord.ui.button(label="Clear (No Game Channel)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, game_channel_id=None)
        await interaction.response.send_message("Game channel cleared.", ephemeral=True)


class LeaderboardChannelSelectView(discord.ui.View):
    """View for selecting leaderboard channel (persistent auto-updating leaderboard)."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        selected = select.values[0]
        channel = interaction.guild.get_channel(selected.id)
        if not channel:
            await interaction.response.send_message("Could not find that channel.", ephemeral=True)
            return
        game = await DatabaseHelper.get_game(self.game_id)
        if not game:
            await interaction.response.send_message("Game not found.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        # Build and send the initial leaderboard embed
        is_valorant = 'valorant' in game.name.lower()
        embed = await self.cog._build_leaderboard_text_embed(interaction.guild, self.game_id, monthly=True)
        view = PersistentLeaderboardView(self.cog, self.game_id, is_valorant=is_valorant)

        try:
            msg = await channel.send(embed=embed, view=view)
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to send messages in that channel.", ephemeral=True)
            return

        # Save channel and message IDs
        await DatabaseHelper.update_game(self.game_id, leaderboard_channel_id=channel.id, leaderboard_message_id=msg.id)

        # Register view for persistence
        self.cog.bot.add_view(view, message_id=msg.id)

        await interaction.followup.send(f"Leaderboard channel set to {channel.mention}.", ephemeral=True)

    @discord.ui.button(label="Clear (No Leaderboard)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, leaderboard_channel_id=None, leaderboard_message_id=None)
        await interaction.response.send_message("Leaderboard channel cleared.", ephemeral=True)


# =============================================================================
# SETTINGS MODALS (Legacy - kept for backwards compatibility)
# =============================================================================

class CategoryModal(discord.ui.Modal, title="Set Category"):
    category_id = discord.ui.TextInput(
        label="Category ID",
        placeholder="Right-click category > Copy ID",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            cat_id = int(self.category_id.value)
            category = interaction.guild.get_channel(cat_id)
            if not category or not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message("Invalid category ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("category_id", str(cat_id))
            await interaction.response.send_message(f"Category set to **{category.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class LogChannelModal(discord.ui.Modal, title="Set Log Channel"):
    channel_id = discord.ui.TextInput(
        label="Channel ID",
        placeholder="Right-click channel > Copy ID",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            ch_id = int(self.channel_id.value)
            channel = interaction.guild.get_channel(ch_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                await interaction.response.send_message("Invalid channel ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("log_channel_id", str(ch_id))
            await interaction.response.send_message(f"Log channel set to {channel.mention}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class AdminRoleModal(discord.ui.Modal, title="Set Custom Match Admin Role"):
    role_id = discord.ui.TextInput(
        label="Role ID",
        placeholder="Right-click role > Copy ID",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            role = interaction.guild.get_role(r_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("cm_admin_role_id", str(r_id))
            await interaction.response.send_message(f"CM Admin role set to **{role.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class AddGameModal(discord.ui.Modal, title="Add Game"):
    name = discord.ui.TextInput(label="Game Name", placeholder="e.g., Valorant", required=True)
    player_count = discord.ui.TextInput(label="Players per Queue", placeholder="e.g., 10", required=True)
    queue_type = discord.ui.TextInput(
        label="Queue Type (mmr/captains/random)",
        placeholder="mmr",
        default="mmr",
        required=True
    )
    captain_selection = discord.ui.TextInput(
        label="Captain Selection (random/admin/highest_mmr)",
        placeholder="random",
        default="random",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
            if count < 2 or count > 20:
                await interaction.response.send_message("Player count must be 2-20.", ephemeral=True)
                return
            
            qt = self.queue_type.value.lower()
            if qt not in ["mmr", "captains", "random"]:
                await interaction.response.send_message("Invalid queue type.", ephemeral=True)
                return
            
            cs = self.captain_selection.value.lower()
            if cs not in ["random", "admin", "highest_mmr"]:
                await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
                return
            
            game_id = await DatabaseHelper.add_game(self.name.value, count, qt, cs)
            await interaction.response.send_message(
                f"Added **{self.name.value}** ({count} players, {qt} queue).",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid player count.", ephemeral=True)
        except Exception as e:
            if "UNIQUE" in str(e):
                await interaction.response.send_message("A game with that name already exists.", ephemeral=True)
            else:
                raise


class EditGameModal(discord.ui.Modal, title="Edit Game"):
    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        
        self.player_count = discord.ui.TextInput(
            label="Players per Queue",
            default=str(game.player_count),
            required=True
        )
        self.queue_type = discord.ui.TextInput(
            label="Queue Type (mmr/captains/random)",
            default=game.queue_type.value,
            required=True
        )
        self.captain_selection = discord.ui.TextInput(
            label="Captain Selection (random/admin/highest_mmr)",
            default=game.captain_selection.value,
            required=True
        )
        self.queue_channel = discord.ui.TextInput(
            label="Queue Channel ID (blank to clear)",
            default=str(game.queue_channel_id) if game.queue_channel_id else "",
            required=False
        )
        self.verified_role = discord.ui.TextInput(
            label="Verified Role ID (blank to clear)",
            default=str(game.verified_role_id) if game.verified_role_id else "",
            required=False
        )
        
        self.add_item(self.player_count)
        self.add_item(self.queue_type)
        self.add_item(self.captain_selection)
        self.add_item(self.queue_channel)
        self.add_item(self.verified_role)
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
            qt = self.queue_type.value.lower()
            cs = self.captain_selection.value.lower()
            
            if qt not in ["mmr", "captains", "random"]:
                await interaction.response.send_message("Invalid queue type.", ephemeral=True)
                return
            if cs not in ["random", "admin", "highest_mmr"]:
                await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
                return
            
            updates = {
                "player_count": count,
                "queue_type": qt,
                "captain_selection": cs
            }
            
            if self.queue_channel.value:
                updates["queue_channel_id"] = int(self.queue_channel.value)
            else:
                updates["queue_channel_id"] = None
            
            if self.verified_role.value:
                updates["verified_role_id"] = int(self.verified_role.value)
            else:
                updates["verified_role_id"] = None
            
            await DatabaseHelper.update_game(self.game.game_id, **updates)
            await interaction.response.send_message(f"Updated **{self.game.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid number format.", ephemeral=True)


class AddMMRRoleModal(discord.ui.Modal, title="Add/Update MMR Role"):
    role_id = discord.ui.TextInput(label="Role ID", required=True)
    mmr_value = discord.ui.TextInput(label="MMR Value", placeholder="e.g., 1500", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            mmr = int(self.mmr_value.value)
            role = interaction.guild.get_role(r_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            await DatabaseHelper.set_mmr_role(self.game_id, r_id, mmr)
            await interaction.response.send_message(f"Set **{role.name}** to {mmr} MMR.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class RemoveMMRRoleModal(discord.ui.Modal, title="Remove MMR Role"):
    role_id = discord.ui.TextInput(label="Role ID", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            await DatabaseHelper.remove_mmr_role(self.game_id, r_id)
            await interaction.response.send_message("Removed MMR role.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class SetPlayerMMRModal(discord.ui.Modal, title="Set Player MMR"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse user ID from mention or raw ID
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            member = interaction.guild.get_member(user_id)
            if not member:
                await interaction.response.send_message("User not found in server.", ephemeral=True)
                return
            
            # Check for MMR roles
            mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
            detected_mmr = None
            detected_role = None
            
            for role in member.roles:
                if role.id in mmr_roles:
                    detected_mmr = mmr_roles[role.id]
                    detected_role = role
                    break
            
            if detected_mmr:
                # Set MMR from role
                stats = await DatabaseHelper.get_player_stats(user_id, self.game_id)
                stats.mmr = detected_mmr
                await DatabaseHelper.update_player_stats(stats)
                await interaction.response.send_message(
                    f"Set **{member.display_name}**'s MMR to {detected_mmr} (from {detected_role.name}).",
                    ephemeral=True
                )
            else:
                # Show role selection
                if not mmr_roles:
                    await interaction.response.send_message(
                        "No MMR roles configured for this game.",
                        ephemeral=True
                    )
                    return
                
                view = await MMRRoleSelectView.create(self.cog, self.game_id, user_id, interaction.guild)
                await interaction.response.send_message(
                    f"No MMR role detected on {member.display_name}. Select one:",
                    view=view,
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


class MMRRoleSelectView(discord.ui.View):
    """View for selecting an MMR role when none is detected."""

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int, guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id
        self.guild = guild

    @classmethod
    async def create(cls, cog: 'CustomMatch', game_id: int, user_id: int, guild: discord.Guild) -> 'MMRRoleSelectView':
        """Factory method to create view with async setup."""
        view = cls(cog, game_id, user_id, guild)
        await view.setup_select()
        return view

    async def setup_select(self):
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = self.guild.get_role(role_id)
            if role:
                options.append(discord.SelectOption(
                    label=f"{role.name} ({mmr} MMR)",
                    value=str(role_id)
                ))

        if options:
            select = discord.ui.Select(placeholder="Select MMR role...", options=options)
            select.callback = self.on_select
            self.add_item(select)
    
    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        mmr = mmr_roles.get(role_id, 1000)
        
        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        stats.mmr = mmr
        await DatabaseHelper.update_player_stats(stats)
        
        member = self.guild.get_member(self.user_id)
        role = self.guild.get_role(role_id)
        await interaction.response.send_message(
            f"Set **{member.display_name if member else self.user_id}**'s MMR to {mmr} (from {role.name if role else role_id}).",
            ephemeral=True
        )


class SetAdminOffsetModal(discord.ui.Modal, title="Set Admin Offset"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    offset = discord.ui.TextInput(label="Offset Value", placeholder="e.g., 100 or -50", required=True)
    
    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            offset = int(self.offset.value)
            
            member = interaction.guild.get_member(user_id)
            if not member:
                await interaction.response.send_message("User not found.", ephemeral=True)
                return
            
            stats = await DatabaseHelper.get_player_stats(user_id, self.game_id)
            stats.admin_offset = offset
            await DatabaseHelper.update_player_stats(stats)
            
            await interaction.response.send_message(
                f"Set **{member.display_name}**'s admin offset to {offset:+d}.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class BlacklistModal(discord.ui.Modal, title="Blacklist Player"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            days = int(self.duration.value)
            
            member = interaction.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)
            
            if days <= 0:
                await DatabaseHelper.blacklist_player(user_id)
                await interaction.response.send_message(f"Permanently blacklisted **{name}**.", ephemeral=True)
            else:
                until = datetime.now(timezone.utc) + timedelta(days=days)
                await DatabaseHelper.blacklist_player(user_id, until)
                await interaction.response.send_message(
                    f"Blacklisted **{name}** for {days} days.",
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class UnblacklistModal(discord.ui.Modal, title="Unblacklist Player"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    
    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            
            await DatabaseHelper.unblacklist_player(user_id)
            member = interaction.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)
            await interaction.response.send_message(f"Unblacklisted **{name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


# =============================================================================
# ADMIN PANEL
# =============================================================================

class AdminPanelView(discord.ui.View):
    """Admin panel for custom match admins."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=300)
        self.cog = cog

    # Row 0: Match management
    @discord.ui.button(label="Substitute Player", style=discord.ButtonStyle.primary, row=0)
    async def substitute(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = await MatchSelectView.create(self.cog, matches, self.show_sub_modal)
        await interaction.response.send_message("Select a match:", view=view, ephemeral=True)

    async def show_sub_modal(self, interaction: discord.Interaction, match_id: int):
        modal = SubstituteModal(self.cog, match_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Swap Players", style=discord.ButtonStyle.primary, row=0)
    async def swap(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = await MatchSelectView.create(self.cog, matches, self.show_swap_modal)
        await interaction.response.send_message("Select a match:", view=view, ephemeral=True)

    async def show_swap_modal(self, interaction: discord.Interaction, match_id: int):
        modal = SwapModal(self.cog, match_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Force Winner", style=discord.ButtonStyle.danger, row=0)
    async def force_winner(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = await MatchSelectView.create(self.cog, matches, self.show_force_winner)
        await interaction.response.send_message("Select a match:", view=view, ephemeral=True)

    async def show_force_winner(self, interaction: discord.Interaction, match_id: int):
        view = ForceWinnerView(self.cog, match_id)
        await interaction.response.send_message("Select the winning team:", view=view, ephemeral=True)

    @discord.ui.button(label="Cancel Match", style=discord.ButtonStyle.danger, row=0)
    async def cancel_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.send_message("No active matches.", ephemeral=True)
            return
        view = await MatchSelectView.create(self.cog, matches, self.confirm_cancel)
        await interaction.response.send_message("Select a match to cancel:", view=view, ephemeral=True)

    async def confirm_cancel(self, interaction: discord.Interaction, match_id: int):
        match = await DatabaseHelper.get_match(match_id)
        short_id = match.get("short_id") or str(match_id) if match else str(match_id)
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to cancel match {short_id}?",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            await self.cog.cancel_match(
                interaction.guild, match_id,
                reason="Admin cancellation",
                cancelled_by=interaction.user.id
            )
            await interaction.followup.send(f"Match {short_id} cancelled.", ephemeral=True)

    # Row 1: Advanced match/queue controls
    @discord.ui.button(label="Change Winner", style=discord.ButtonStyle.secondary, row=1)
    async def change_winner(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ChangeWinnerModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Force Start", style=discord.ButtonStyle.secondary, row=1)
    async def force_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_force_start))
        await interaction.response.send_message("Select a game to force start:", view=view, ephemeral=True)

    async def do_force_start(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        # Find active queue for this game
        queue_state = None
        for qid, qs in self.cog.queues.items():
            if qs.game_id == game_id and qs.state in ("waiting", "ready_check"):
                queue_state = qs
                break

        if not queue_state:
            await interaction.response.send_message("No active queue for this game.", ephemeral=True)
            return

        if len(queue_state.players) < game.player_count:
            await interaction.response.send_message(
                f"Queue is not full ({len(queue_state.players)}/{game.player_count}).",
                ephemeral=True
            )
            return

        # Cancel ready check if active
        if queue_state.queue_id in self.cog.ready_check_tasks:
            self.cog.ready_check_tasks[queue_state.queue_id].cancel()
            del self.cog.ready_check_tasks[queue_state.queue_id]

        # Mark all as ready
        for pid in queue_state.players:
            queue_state.players[pid] = True

        await interaction.response.defer()
        channel = interaction.guild.get_channel(queue_state.channel_id)
        if channel:
            await self.cog.proceed_to_match(channel, game, queue_state)
        await interaction.followup.send("Match force started.", ephemeral=True)

    @discord.ui.button(label="Clear Queue", style=discord.ButtonStyle.secondary, row=1)
    async def clear_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.confirm_clear_queue))
        await interaction.response.send_message("Select a game to clear its queue:", view=view, ephemeral=True)

    async def confirm_clear_queue(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to clear the queue for **{game.name}**?",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            # Find and clear queue
            for qid, qs in list(self.cog.queues.items()):
                if qs.game_id == game_id:
                    # Cancel any running ready check task
                    if qid in self.cog.ready_check_tasks:
                        self.cog.ready_check_tasks[qid].cancel()
                        del self.cog.ready_check_tasks[qid]
                    # Reset state and players
                    qs.state = "waiting"
                    qs.players.clear()
                    await DatabaseHelper.clear_queue(qid)
                    # Also reset state in DB
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                            (qid,)
                        )
                        await db.commit()
                    # Update embed
                    channel = interaction.guild.get_channel(qs.channel_id)
                    if channel and qs.message_id:
                        try:
                            msg = await channel.fetch_message(qs.message_id)
                            embed = await self.cog.create_queue_embed(game, qs)
                            new_view = QueueView(self.cog, game_id, qid)
                            await msg.edit(embed=embed, view=new_view)
                        except Exception:
                            pass
            await interaction.followup.send(f"Queue cleared for **{game.name}**.", ephemeral=True)

    @discord.ui.button(label="Queue Start", style=discord.ButtonStyle.secondary, row=1)
    async def queue_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_queue_start))
        await interaction.response.send_message("Select a game to start queue:", view=view, ephemeral=True)

    async def do_queue_start(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        await interaction.response.defer()

        # Delete existing queue embed for this game in this channel
        for qid, qs in list(self.cog.queues.items()):
            if qs.game_id == game_id and qs.channel_id == interaction.channel.id:
                if qs.message_id:
                    try:
                        old_msg = await interaction.channel.fetch_message(qs.message_id)
                        await old_msg.delete()
                    except discord.NotFound:
                        pass
                # Clear from memory and DB
                del self.cog.queues[qid]
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (qid,))
                    await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (qid,))
                    await db.commit()
                break

        await self.cog.start_queue(interaction.channel, game)
        await interaction.followup.send("Queue started.", ephemeral=True)

    # Row 2: Player management
    @discord.ui.button(label="Suspensions", style=discord.ButtonStyle.secondary, row=2)
    async def manage_suspensions(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SuspensionManagementView(self.cog)
        await interaction.response.send_message("Manage Suspensions:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove from Queue", style=discord.ButtonStyle.secondary, row=2)
    async def remove_from_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_queue_players))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    async def show_queue_players(self, interaction: discord.Interaction, game_id: int):
        # Find queue for this game
        queue_state = None
        for qid, qs in self.cog.queues.items():
            if qs.game_id == game_id and qs.state in ("waiting", "ready_check"):
                queue_state = qs
                break

        if not queue_state or not queue_state.players:
            await interaction.response.send_message("No players in queue.", ephemeral=True)
            return

        view = QueuePlayerRemoveView(self.cog, game_id, queue_state)
        await interaction.response.send_message("Select a player to remove:", view=view, ephemeral=True)

    @discord.ui.button(label="Adjust W/L", style=discord.ButtonStyle.secondary, row=2)
    async def adjust_wl(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AdjustWLUserSelectView(self.cog)
        await interaction.response.send_message("Select a user:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Player MMR", style=discord.ButtonStyle.secondary, row=2)
    async def set_player_mmr_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = SetMMRUserSelectView(self.cog, games)
        await interaction.response.send_message("Select a user and game:", view=view, ephemeral=True)

    @discord.ui.button(label="MMR View", style=discord.ButtonStyle.secondary, row=2)
    async def view_player_mmr_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = MMRViewUserSelectView(self.cog, games)
        await interaction.response.send_message("Select a user and game to view MMR history:", view=view, ephemeral=True)

    # Row 3: Setup and utilities
    @discord.ui.button(label="Setup New User", style=discord.ButtonStyle.success, row=3)
    async def setup_new_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = SetupUserGameSelectView(self.cog, games)
        await interaction.response.send_message("Step 1: Select a game for the new user:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Offset", style=discord.ButtonStyle.secondary, row=3)
    async def set_admin_offset(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_offset_modal))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    async def show_offset_modal(self, interaction: discord.Interaction, game_id: int):
        modal = SetAdminOffsetModal(self.cog, game_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Cleanup Stale Matches", style=discord.ButtonStyle.danger, row=3)
    async def cleanup_orphans(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clean up stale/stuck matches that are missing channels or roles."""
        await interaction.response.defer(ephemeral=True)

        count, details = await self.cog.manual_orphan_cleanup(interaction.guild)

        if count == 0:
            await interaction.followup.send("No stale matches found to clean up.", ephemeral=True)
        else:
            detail_text = "\n".join(details[:10])  # Max 10 entries
            if len(details) > 10:
                detail_text += f"\n... and {len(details) - 10} more"
            await interaction.followup.send(
                f"**Cleaned up {count} stale matches:**\n{detail_text}",
                ephemeral=True
            )


class ChangeWinnerModal(discord.ui.Modal, title="Change Match Winner"):
    match_id_input = discord.ui.TextInput(
        label="Match ID",
        placeholder="Enter the match ID",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            match_id = int(self.match_id_input.value)
            match = await DatabaseHelper.get_completed_match(match_id)
            if not match:
                await interaction.response.send_message("Match not found.", ephemeral=True)
                return

            short_id = match.get("short_id") or str(match_id)
            view = ChangeWinnerSelectView(self.cog, match_id, match.get("winning_team"), short_id)
            game = await DatabaseHelper.get_game(match["game_id"])
            current = match.get("winning_team", "None")
            await interaction.response.send_message(
                f"Match {short_id} ({game.name})\nCurrent winner: **{current}**\n\nSelect the new winner:",
                view=view, ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid match ID.", ephemeral=True)


class ChangeWinnerSelectView(discord.ui.View):
    """View for selecting a new winner for a match."""

    def __init__(self, cog: 'CustomMatch', match_id: int, current_winner: Optional[str], short_id: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.current_winner = current_winner
        self.short_id = short_id

    @discord.ui.button(label="Red Team Wins", style=discord.ButtonStyle.danger)
    async def red_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, Team.RED)

    @discord.ui.button(label="Blue Team Wins", style=discord.ButtonStyle.primary)
    async def blue_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, Team.BLUE)

    async def change_winner(self, interaction: discord.Interaction, new_winner: Team):
        # Reverse existing result if there was one
        if self.current_winner:
            await DatabaseHelper.reverse_match_result(self.match_id)

        # Apply new result
        await self.cog.finalize_match(interaction.guild, self.match_id, new_winner)
        await interaction.response.send_message(
            f"Match {self.short_id} winner changed to **{new_winner.value}** team.",
            ephemeral=True
        )
        await self.cog.log_action(
            interaction.guild,
            f"Match {self.short_id} winner changed to {new_winner.value} by {interaction.user.mention}"
        )


class SuspensionManagementView(discord.ui.View):
    """View for managing suspensions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add Suspension", style=discord.ButtonStyle.danger)
    async def add_suspension(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddSuspensionUserSelectView(self.cog)
        await interaction.response.send_message("Select a user to suspend:", view=view, ephemeral=True)

    @discord.ui.button(label="View Suspensions", style=discord.ButtonStyle.secondary)
    async def view_suspensions(self, interaction: discord.Interaction, button: discord.ui.Button):
        suspensions = await DatabaseHelper.get_all_suspensions()
        if not suspensions:
            await interaction.response.send_message("No active suspensions.", ephemeral=True)
            return

        lines = ["**Active Suspensions**\n"]
        for s in suspensions:
            user = interaction.guild.get_member(s.player_id)
            name = user.display_name if user else str(s.player_id)
            game = await DatabaseHelper.get_game(s.game_id) if s.game_id else None
            game_str = game.name if game else "All Games"
            until = s.suspended_until.strftime("%Y-%m-%d %H:%M UTC")
            reason = s.reason or "No reason"
            lines.append(f"**#{s.suspension_id}** - {name} ({game_str})")
            lines.append(f"  Until: {until} | Reason: {reason}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Remove Suspension", style=discord.ButtonStyle.success)
    async def remove_suspension(self, interaction: discord.Interaction, button: discord.ui.Button):
        suspensions = await DatabaseHelper.get_all_suspensions()
        if not suspensions:
            await interaction.response.send_message("No active suspensions.", ephemeral=True)
            return

        view = RemoveSuspensionSelectView(self.cog, suspensions, interaction.guild)
        await interaction.response.send_message("Select a suspension to remove:", view=view, ephemeral=True)


class AddSuspensionUserSelectView(discord.ui.View):
    """View for selecting a user to suspend."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        games = await DatabaseHelper.get_all_games()
        view = AddSuspensionGameSelectView(self.cog, user.id, games)
        await interaction.response.send_message(
            f"Suspending **{user.display_name}**\nSelect a game (or 'All Games'):",
            view=view, ephemeral=True
        )


class AddSuspensionGameSelectView(discord.ui.View):
    """View for selecting game for suspension."""

    def __init__(self, cog: 'CustomMatch', user_id: int, games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id

        options = [discord.SelectOption(label="All Games", value="all")]
        for g in games:
            options.append(discord.SelectOption(label=g.name, value=str(g.game_id)))

        select = discord.ui.Select(placeholder="Select game...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        game_id = None if value == "all" else int(value)
        modal = AddSuspensionModal(self.cog, self.user_id, game_id)
        await interaction.response.send_modal(modal)


class AddSuspensionModal(discord.ui.Modal, title="Add Suspension"):
    duration = discord.ui.TextInput(
        label="Duration (hours)",
        placeholder="e.g., 24 for 1 day, 168 for 1 week",
        required=True
    )
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="Reason for suspension",
        required=False,
        style=discord.TextStyle.paragraph
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: Optional[int]):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            hours = int(self.duration.value)
            until = datetime.now(timezone.utc) + timedelta(hours=hours)
            suspension_id = await DatabaseHelper.add_suspension(
                self.user_id, self.game_id, until,
                self.reason.value or None, interaction.user.id
            )

            user = interaction.guild.get_member(self.user_id)
            name = user.display_name if user else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id) if self.game_id else None
            game_str = game.name if game else "All Games"

            await interaction.response.send_message(
                f"Suspended **{name}** from **{game_str}** for {hours} hours.\nSuspension ID: #{suspension_id}",
                ephemeral=True
            )

            await self.cog.log_action(
                interaction.guild,
                f"Suspended {name} from {game_str} for {hours}h by {interaction.user.mention}"
            )
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)


class RemoveSuspensionSelectView(discord.ui.View):
    """View for selecting a suspension to remove."""

    def __init__(self, cog: 'CustomMatch', suspensions: List[Suspension], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        for s in suspensions[:25]:
            user = guild.get_member(s.player_id)
            name = user.display_name if user else str(s.player_id)
            options.append(discord.SelectOption(
                label=f"#{s.suspension_id} - {name}",
                value=str(s.suspension_id),
                description=s.reason[:50] if s.reason else "No reason"
            ))

        select = discord.ui.Select(placeholder="Select suspension...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        suspension_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_suspension(suspension_id)
        await interaction.response.send_message(
            f"Removed suspension #{suspension_id}.",
            ephemeral=True
        )


class QueuePlayerRemoveView(discord.ui.View):
    """View for removing a player from queue."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_state: 'QueueState'):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.queue_state = queue_state

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select player to remove...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        if user.id not in self.queue_state.players:
            await interaction.response.send_message("That player is not in the queue.", ephemeral=True)
            return

        del self.queue_state.players[user.id]
        await DatabaseHelper.remove_player_from_queue(self.queue_state.queue_id, user.id)

        game = await DatabaseHelper.get_game(self.game_id)
        channel = interaction.guild.get_channel(self.queue_state.channel_id)
        if channel and self.queue_state.message_id:
            try:
                msg = await channel.fetch_message(self.queue_state.message_id)
                embed = await self.cog.create_queue_embed(game, self.queue_state)
                view = QueueView(self.cog, self.game_id, self.queue_state.queue_id)
                await msg.edit(embed=embed, view=view)
            except Exception:
                pass

        await interaction.response.send_message(
            f"Removed **{user.display_name}** from the queue.",
            ephemeral=True
        )


class AdjustWLUserSelectView(discord.ui.View):
    """View for selecting a user to adjust W/L."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return

        async def show_adjust_modal(inter: discord.Interaction, game_id: int):
            modal = AdjustWLModal(self.cog, user.id, game_id)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, show_adjust_modal))
        await interaction.response.send_message(
            f"Adjusting stats for **{user.display_name}**\nSelect a game:",
            view=view, ephemeral=True
        )


class AdjustWLModal(discord.ui.Modal, title="Adjust Wins/Losses"):
    wins_delta = discord.ui.TextInput(
        label="Wins adjustment",
        placeholder="e.g., 1 or -2",
        default="0",
        required=True
    )
    losses_delta = discord.ui.TextInput(
        label="Losses adjustment",
        placeholder="e.g., 1 or -2",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            wins = int(self.wins_delta.value)
            losses = int(self.losses_delta.value)

            await DatabaseHelper.adjust_player_stats(
                self.user_id, self.game_id, wins, losses,
                adjusted_by=interaction.user.id
            )

            member = interaction.guild.get_member(self.user_id)
            name = member.display_name if member else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id)

            await interaction.response.send_message(
                f"Adjusted **{name}**'s stats for {game.name}: {wins:+d}W, {losses:+d}L\n"
                f"This adjustment applies to both all-time and monthly leaderboards.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid numbers.", ephemeral=True)


class SetMMRUserSelectView(discord.ui.View):
    """View for setting player MMR with user and game selection."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.games = games
        self.selected_user = None

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_user = select.values[0]

        async def show_mmr_modal(inter: discord.Interaction, game_id: int):
            modal = SetMMRModal(self.cog, self.selected_user.id, game_id)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(self.games, show_mmr_modal))
        await interaction.response.send_message(
            f"Setting MMR for **{self.selected_user.display_name}**\nSelect a game:",
            view=view, ephemeral=True
        )


class SetMMRModal(discord.ui.Modal, title="Set Player MMR"):
    mmr_value = discord.ui.TextInput(
        label="MMR Value",
        placeholder="e.g., 1500",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
            stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
            stats.mmr = mmr
            await DatabaseHelper.update_player_stats(stats)

            member = interaction.guild.get_member(self.user_id)
            name = member.display_name if member else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id)

            await interaction.response.send_message(
                f"Set **{name}**'s MMR for {game.name} to {mmr}.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class MMRViewUserSelectView(discord.ui.View):
    """View for viewing player MMR history with user and game selection."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.games = games
        self.selected_user = None

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_user = select.values[0]

        async def show_mmr_history(inter: discord.Interaction, game_id: int):
            await inter.response.defer(ephemeral=True)

            # Get player stats
            stats = await DatabaseHelper.get_player_stats(self.selected_user.id, game_id)
            game = await DatabaseHelper.get_game(game_id)

            # Get MMR history (last 10 games)
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    """SELECT match_id, mmr_before, mmr_after, change, timestamp
                       FROM mmr_history
                       WHERE player_id = ? AND game_id = ?
                       ORDER BY timestamp DESC LIMIT 10""",
                    (self.selected_user.id, game_id)
                ) as cursor:
                    history = await cursor.fetchall()

            # Build embed
            embed = discord.Embed(
                title=f"MMR History - {self.selected_user.display_name}",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=self.selected_user.display_avatar.url)

            # Current stats
            embed.add_field(
                name=f"{game.name} Stats",
                value=f"**Current MMR:** {stats.mmr}\n"
                      f"**Effective MMR:** {stats.effective_mmr}\n"
                      f"**Games Played:** {stats.games_played}\n"
                      f"**W/L:** {stats.wins}/{stats.losses}",
                inline=False
            )

            # History
            if history:
                history_lines = []
                for match_id, mmr_before, mmr_after, change, timestamp in history:
                    sign = "+" if change >= 0 else ""
                    history_lines.append(f"`#{match_id}` {mmr_before} → {mmr_after} ({sign}{change})")

                embed.add_field(
                    name="Last 10 Games",
                    value="\n".join(history_lines),
                    inline=False
                )
            else:
                embed.add_field(
                    name="Last 10 Games",
                    value="No match history found.",
                    inline=False
                )

            await inter.followup.send(embed=embed, ephemeral=True)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(self.games, show_mmr_history))
        await interaction.response.send_message(
            f"Viewing MMR for **{self.selected_user.display_name}**\nSelect a game:",
            view=view, ephemeral=True
        )


class SetupUserGameSelectView(discord.ui.View):
    """Step 1: Select game for new user setup."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=120)
        self.cog = cog
        self.games = games

        options = [discord.SelectOption(label=g.name, value=str(g.game_id)) for g in games]
        select = discord.ui.Select(placeholder="Select game...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        game_id = int(interaction.data["values"][0])
        view = SetupUserSelectView(self.cog, game_id)
        await interaction.response.send_message(
            "Step 2: Select the user to set up:",
            view=view, ephemeral=True
        )


class SetupUserSelectView(discord.ui.View):
    """Step 2: Select user for setup."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        modal = SetupUserModal(self.cog, self.game_id, user.id)
        await interaction.response.send_modal(modal)


class SetupUserModal(discord.ui.Modal, title="Setup New User"):
    ign = discord.ui.TextInput(
        label="IGN (leave blank to skip)",
        placeholder="In-game name",
        required=False
    )
    mmr = discord.ui.TextInput(
        label="Starting MMR",
        placeholder="e.g., 1000",
        default="1000",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr_val = int(self.mmr.value)

            # Set IGN if provided
            if self.ign.value:
                await DatabaseHelper.set_player_ign(self.user_id, self.game_id, self.ign.value)

            # Set MMR
            stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
            stats.mmr = mmr_val
            await DatabaseHelper.update_player_stats(stats)

            # Give verified role if configured
            game = await DatabaseHelper.get_game(self.game_id)
            member = interaction.guild.get_member(self.user_id)

            if game.verified_role_id and member:
                role = interaction.guild.get_role(game.verified_role_id)
                if role and role not in member.roles:
                    await member.add_roles(role)

            name = member.display_name if member else str(self.user_id)
            lines = [f"Setup complete for **{name}** ({game.name}):"]
            if self.ign.value:
                lines.append(f"- IGN: `{self.ign.value}`")
            lines.append(f"- MMR: {mmr_val}")
            if game.verified_role_id:
                lines.append(f"- Verified role assigned")

            await interaction.response.send_message("\n".join(lines), ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class MatchSelectView(discord.ui.View):
    """View for selecting an active match."""

    def __init__(self, cog: 'CustomMatch', matches: List[dict], callback, game_names: Dict[int, str] = None):
        super().__init__(timeout=60)
        self.cog = cog
        self.callback = callback
        game_names = game_names or {}

        options = []
        for m in matches[:25]:  # Discord limit
            game_name = game_names.get(m["game_id"], "Unknown")
            short_id = m.get("short_id") or str(m["match_id"])
            options.append(discord.SelectOption(
                label=f"{short_id} - {game_name}",
                value=str(m["match_id"])
            ))

        if options:
            select = discord.ui.Select(placeholder="Select match...", options=options)
            select.callback = self.on_select
            self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        match_id = int(interaction.data["values"][0])
        await self.callback(interaction, match_id)

    @classmethod
    async def create(cls, cog: 'CustomMatch', matches: List[dict], callback):
        """Factory method to create MatchSelectView with async game data loading."""
        # Pre-fetch all game names
        game_ids = set(m["game_id"] for m in matches)
        game_names = {}
        for game_id in game_ids:
            game = await DatabaseHelper.get_game(game_id)
            if game:
                game_names[game_id] = game.name
        return cls(cog, matches, callback, game_names)


class ForceWinnerView(discord.ui.View):
    """View for forcing a winner."""
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
    
    @discord.ui.button(label="Red Team Wins", style=discord.ButtonStyle.danger)
    async def red_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.finalize_match(interaction.guild, self.match_id, Team.RED)
        await interaction.response.send_message("Red team declared winner.", ephemeral=True)
    
    @discord.ui.button(label="Blue Team Wins", style=discord.ButtonStyle.primary)
    async def blue_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.finalize_match(interaction.guild, self.match_id, Team.BLUE)
        await interaction.response.send_message("Blue team declared winner.", ephemeral=True)


class SubstituteModal(discord.ui.Modal, title="Substitute Player"):
    out_player = discord.ui.TextInput(label="Player leaving (ID or @mention)", required=True)
    in_player = discord.ui.TextInput(label="Substitute player (ID or @mention)", required=True)
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__()
        self.cog = cog
        self.match_id = match_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            out_str = self.out_player.value.replace("<@", "").replace(">", "").replace("!", "")
            in_str = self.in_player.value.replace("<@", "").replace(">", "").replace("!", "")
            out_id = int(out_str)
            in_id = int(in_str)
            
            match = await DatabaseHelper.get_match(self.match_id)
            if not match:
                await interaction.response.send_message("Match not found.", ephemeral=True)
                return
            
            game = await DatabaseHelper.get_game(match["game_id"])
            
            # Check sub has verified role
            if game.verified_role_id:
                in_member = interaction.guild.get_member(in_id)
                if not in_member or not any(r.id == game.verified_role_id for r in in_member.roles):
                    await interaction.response.send_message(
                        "Substitute doesn't have the verified role.",
                        ephemeral=True
                    )
                    return
            
            # Get the outgoing player's team
            players = await DatabaseHelper.get_match_players(self.match_id)
            out_player_data = next((p for p in players if p["player_id"] == out_id), None)
            if not out_player_data:
                await interaction.response.send_message("Outgoing player not in this match.", ephemeral=True)
                return
            
            team = out_player_data["team"]
            
            # Remove old player, add new
            await DatabaseHelper.remove_match_player(self.match_id, out_id)
            await DatabaseHelper.add_match_player(
                self.match_id, in_id, team,
                was_sub=True, original_player_id=out_id
            )
            
            # Update roles
            out_member = interaction.guild.get_member(out_id)
            in_member = interaction.guild.get_member(in_id)
            
            role_id = match["red_role_id"] if team == "red" else match["blue_role_id"]
            role = interaction.guild.get_role(role_id)
            
            if role:
                if out_member:
                    await out_member.remove_roles(role)
                if in_member:
                    await in_member.add_roles(role)
            
            await interaction.response.send_message(
                f"Substituted <@{out_id}> with <@{in_id}> on {team} team.",
                ephemeral=True
            )

            # Log
            short_id = match.get("short_id") or str(self.match_id)
            await self.cog.log_action(
                interaction.guild,
                f"Match {short_id}: <@{out_id}> substituted by <@{in_id}>"
            )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


class SwapModal(discord.ui.Modal, title="Swap Players"):
    player1 = discord.ui.TextInput(label="Player 1 (ID or @mention)", required=True)
    player2 = discord.ui.TextInput(label="Player 2 (ID or @mention)", required=True)
    
    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__()
        self.cog = cog
        self.match_id = match_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            p1_str = self.player1.value.replace("<@", "").replace(">", "").replace("!", "")
            p2_str = self.player2.value.replace("<@", "").replace(">", "").replace("!", "")
            p1_id = int(p1_str)
            p2_id = int(p2_str)
            
            match = await DatabaseHelper.get_match(self.match_id)
            players = await DatabaseHelper.get_match_players(self.match_id)
            
            p1_data = next((p for p in players if p["player_id"] == p1_id), None)
            p2_data = next((p for p in players if p["player_id"] == p2_id), None)
            
            if not p1_data or not p2_data:
                await interaction.response.send_message("Both players must be in the match.", ephemeral=True)
                return
            
            if p1_data["team"] == p2_data["team"]:
                await interaction.response.send_message("Players must be on different teams.", ephemeral=True)
                return
            
            # Swap teams in database
            await DatabaseHelper.remove_match_player(self.match_id, p1_id)
            await DatabaseHelper.remove_match_player(self.match_id, p2_id)
            await DatabaseHelper.add_match_player(
                self.match_id, p1_id, p2_data["team"],
                was_captain=p1_data["was_captain"]
            )
            await DatabaseHelper.add_match_player(
                self.match_id, p2_id, p1_data["team"],
                was_captain=p2_data["was_captain"]
            )
            
            # Swap roles
            p1_member = interaction.guild.get_member(p1_id)
            p2_member = interaction.guild.get_member(p2_id)
            
            red_role = interaction.guild.get_role(match["red_role_id"])
            blue_role = interaction.guild.get_role(match["blue_role_id"])
            
            if red_role and blue_role and p1_member and p2_member:
                if p1_data["team"] == "red":
                    await p1_member.remove_roles(red_role)
                    await p1_member.add_roles(blue_role)
                    await p2_member.remove_roles(blue_role)
                    await p2_member.add_roles(red_role)
                else:
                    await p1_member.remove_roles(blue_role)
                    await p1_member.add_roles(red_role)
                    await p2_member.remove_roles(red_role)
                    await p2_member.add_roles(blue_role)
            
            await interaction.response.send_message(
                f"Swapped <@{p1_id}> and <@{p2_id}>.",
                ephemeral=True
            )

            short_id = match.get("short_id") or str(self.match_id)
            await self.cog.log_action(
                interaction.guild,
                f"Match {short_id}: Swapped <@{p1_id}> and <@{p2_id}>"
            )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


# =============================================================================
# VERIFICATION TICKET VIEW
# =============================================================================

class VerificationTicketView(discord.ui.View):
    """View with button to open a verification ticket."""

    def __init__(self, cog: 'CustomMatch', game: 'GameConfig'):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

    @discord.ui.button(label="Open Verification Ticket", style=discord.ButtonStyle.primary)
    async def open_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        ticketing_cog = self.cog.bot.get_cog("TicketSystem")
        if not ticketing_cog:
            await interaction.response.send_message(
                "Ticketing system is not available. Please contact an admin.",
                ephemeral=True
            )
            return

        # Load topics and find the verification topic
        import os
        import json
        topics_file = os.path.join("data", "topics.json")
        if not os.path.exists(topics_file):
            await interaction.response.send_message(
                "No ticket topics configured. Please contact an admin.",
                ephemeral=True
            )
            return

        with open(topics_file, "r", encoding="utf-8") as f:
            topics = json.load(f)

        if self.game.verification_topic not in topics:
            await interaction.response.send_message(
                f"Verification topic '{self.game.verification_topic}' not found. Please contact an admin.",
                ephemeral=True
            )
            return

        topic = topics[self.game.verification_topic]

        # Create the ticket channel
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            ch = await ticketing_cog._create_discussion_channel(
                interaction, topic, interaction.user, is_ticket=True
            )
            if ch:
                await interaction.followup.send(
                    f"Your verification ticket has been created: {ch.mention}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "Failed to create ticket. Please contact an admin.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error creating verification ticket: {e}")
            await interaction.followup.send(
                f"Error creating ticket: {e}",
                ephemeral=True
            )


# =============================================================================
# QUEUE VIEWS
# =============================================================================

class QueueView(discord.ui.View):
    """Main queue view with Join/Leave buttons. Uses dynamic custom_ids for resilience."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id

        # Create buttons with dynamic custom_ids including queue_id for resilience
        join_btn = discord.ui.Button(
            label="Join",
            style=discord.ButtonStyle.success,
            custom_id=f"cm_queue_join:{queue_id}"
        )
        join_btn.callback = self.join_callback
        self.add_item(join_btn)

        leave_btn = discord.ui.Button(
            label="Leave",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_queue_leave:{queue_id}"
        )
        leave_btn.callback = self.leave_callback
        self.add_item(leave_btn)

    async def join_callback(self, interaction: discord.Interaction):
        await self.cog.handle_queue_join(interaction, self.game_id, self.queue_id)

    async def leave_callback(self, interaction: discord.Interaction):
        await self.cog.handle_queue_leave(interaction, self.game_id, self.queue_id)


class ReadyCheckView(discord.ui.View):
    """Ready check view. Uses dynamic custom_ids for resilience."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id

        # Create buttons with dynamic custom_ids including queue_id for resilience
        ready_btn = discord.ui.Button(
            label="Ready",
            style=discord.ButtonStyle.success,
            custom_id=f"cm_ready_yes:{queue_id}"
        )
        ready_btn.callback = self.ready_callback
        self.add_item(ready_btn)

        not_ready_btn = discord.ui.Button(
            label="Not Ready",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_ready_no:{queue_id}"
        )
        not_ready_btn.callback = self.not_ready_callback
        self.add_item(not_ready_btn)

    async def ready_callback(self, interaction: discord.Interaction):
        await self.cog.handle_ready(interaction, self.queue_id, True)

    async def not_ready_callback(self, interaction: discord.Interaction):
        await self.cog.handle_ready(interaction, self.queue_id, False)


class WinVoteView(discord.ui.View):
    """Win vote view with persistent custom_ids."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id

        # Create buttons with dynamic custom_ids including match_id for persistence
        red_btn = discord.ui.Button(
            label="Red Team",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_vote_red:{match_id}"
        )
        red_btn.callback = self.vote_red_callback
        self.add_item(red_btn)

        blue_btn = discord.ui.Button(
            label="Blue Team",
            style=discord.ButtonStyle.primary,
            custom_id=f"cm_vote_blue:{match_id}"
        )
        blue_btn.callback = self.vote_blue_callback
        self.add_item(blue_btn)

    async def vote_red_callback(self, interaction: discord.Interaction):
        await self.cog.handle_win_vote(interaction, self.match_id, Team.RED)

    async def vote_blue_callback(self, interaction: discord.Interaction):
        await self.cog.handle_win_vote(interaction, self.match_id, Team.BLUE)


class AbandonVoteView(discord.ui.View):
    """Abandon vote view with persistent custom_ids."""

    def __init__(self, cog: 'CustomMatch', match_id: int, needed_votes: int):
        super().__init__(timeout=None)  # No timeout for persistence
        self.cog = cog
        self.match_id = match_id
        self.needed_votes = needed_votes

        # Create button with dynamic custom_id including match_id for persistence
        abandon_btn = discord.ui.Button(
            label="Vote to Abandon",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_abandon_vote:{match_id}"
        )
        abandon_btn.callback = self.vote_abandon_callback
        self.add_item(abandon_btn)

    async def vote_abandon_callback(self, interaction: discord.Interaction):
        await self.cog.handle_abandon_vote(interaction, self.match_id, self.needed_votes)


class IGNModal(discord.ui.Modal, title="Set Your IGN"):
    ign = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter your in-game name...",
        required=True,
        max_length=100
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.title = f"Set Your {game_name} IGN"

    async def on_submit(self, interaction: discord.Interaction):
        await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, self.ign.value)
        await interaction.response.send_message(
            f"Your IGN for this game has been set to: `{self.ign.value}`",
            ephemeral=True
        )


class CaptainDraftView(discord.ui.View):
    """View for captain drafting."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id


# =============================================================================
# PERSISTENT LEADERBOARD VIEW (for dedicated leaderboard channel)
# =============================================================================

class PersistentLeaderboardView(discord.ui.View):
    """Persistent view attached to the leaderboard embed in the dedicated channel."""

    def __init__(self, cog: 'CustomMatch', game_id: int, is_valorant: bool = False):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.is_valorant = is_valorant

        # All-time button
        alltime_btn = discord.ui.Button(
            label="All-time", style=discord.ButtonStyle.secondary,
            custom_id=f"cm_lb_alltime:{game_id}"
        )
        alltime_btn.callback = self.alltime_callback
        self.add_item(alltime_btn)

        # Matches button (Valorant only)
        if is_valorant:
            matches_btn = discord.ui.Button(
                label="Matches", style=discord.ButtonStyle.secondary,
                custom_id=f"cm_lb_matches:{game_id}"
            )
            matches_btn.callback = self.matches_callback
            self.add_item(matches_btn)

    async def alltime_callback(self, interaction: discord.Interaction):
        """Send ephemeral top 20 all-time leaderboard."""
        embed = await self.cog._build_leaderboard_text_embed(interaction.guild, self.game_id, monthly=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def matches_callback(self, interaction: discord.Interaction):
        """Send ephemeral scoreboard for most recent match + dropdown."""
        await interaction.response.defer(ephemeral=True)
        recent = await DatabaseHelper.get_recent_completed_matches(self.game_id, limit=5)
        if not recent:
            await interaction.followup.send("No completed matches found.", ephemeral=True)
            return

        # Generate scoreboard for the most recent match
        latest = recent[0]
        embed, file = await self.cog._generate_match_scoreboard(interaction.guild, latest["match_id"])

        view = MatchSelectView(self.cog, recent) if len(recent) > 1 else None
        kwargs = {"embed": embed, "ephemeral": True}
        if file:
            kwargs["file"] = file
        if view:
            kwargs["view"] = view
        await interaction.followup.send(**kwargs)


class MatchSelectView(discord.ui.View):
    """Ephemeral view with a dropdown to browse recent matches."""

    def __init__(self, cog: 'CustomMatch', matches: List[dict]):
        super().__init__(timeout=180)
        self.cog = cog
        self.matches = matches

        options = []
        for m in matches:
            map_name = m.get("map_name") or "Unknown"
            decided = m.get("decided_at") or ""
            # Parse decided_at to a short date
            try:
                dt = datetime.fromisoformat(decided)
                date_str = dt.strftime("%m/%d/%y")
            except (ValueError, TypeError):
                date_str = "?"
            options.append(discord.SelectOption(
                label=f"{map_name} {date_str}",
                value=str(m["match_id"])
            ))

        select = discord.ui.Select(placeholder="Select a match...", options=options)
        select.callback = self.select_callback
        self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        match_id = int(interaction.data["values"][0])
        await interaction.response.defer(ephemeral=True)
        embed, file = await self.cog._generate_match_scoreboard(interaction.guild, match_id)
        kwargs = {"embed": embed, "ephemeral": True}
        if file:
            kwargs["file"] = file
        await interaction.followup.send(**kwargs)


class PlayerStatsView(discord.ui.View):
    """Multi-page player stats view with navigation and time period toggle."""

    PAGE_TITLES = ["Overview", "Map Performance", "Teammates", "Recent Matches"]

    def __init__(self, cog: 'CustomMatch', guild: discord.Guild, player_id: int,
                 game: 'GameConfig', stats: 'PlayerStats', monthly: bool = True):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild = guild
        self.player_id = player_id
        self.game = game
        self.stats = stats
        self.monthly = monthly
        self.current_page = 0
        self.valorant_stats = {}
        self.teammate_stats = {}
        self.recent_matches = []
        self.update_buttons()

    def update_buttons(self):
        """Update button states based on current page."""
        self.clear_items()

        # Previous button
        prev_btn = discord.ui.Button(label="◀", style=discord.ButtonStyle.secondary, disabled=self.current_page == 0)
        prev_btn.callback = self.prev_page
        self.add_item(prev_btn)

        # Page indicator
        page_btn = discord.ui.Button(label=f"{self.current_page + 1}/{len(self.PAGE_TITLES)}", style=discord.ButtonStyle.secondary, disabled=True)
        self.add_item(page_btn)

        # Next button
        next_btn = discord.ui.Button(label="▶", style=discord.ButtonStyle.secondary, disabled=self.current_page == len(self.PAGE_TITLES) - 1)
        next_btn.callback = self.next_page
        self.add_item(next_btn)

        # Time period toggle
        period_label = "All Time" if self.monthly else "This Month"
        period_btn = discord.ui.Button(label=f"View: {period_label}", style=discord.ButtonStyle.primary)
        period_btn.callback = self.toggle_period
        self.add_item(period_btn)

    async def load_data(self):
        """Load Valorant stats and teammate data."""
        self.valorant_stats = await DatabaseHelper.get_valorant_player_stats(
            self.player_id, self.game.game_id, self.monthly
        )
        self.teammate_stats = await DatabaseHelper.get_player_teammate_stats(
            self.player_id, self.game.game_id, self.monthly
        )
        self.recent_matches = await DatabaseHelper.get_player_recent_matches(
            self.player_id, self.game.game_id, limit=5
        )

    async def build_embed(self) -> discord.Embed:
        """Build the embed for the current page."""
        member = self.guild.get_member(self.player_id)
        member_name = member.display_name if member else f"User {self.player_id}"
        period_text = "This Month" if self.monthly else "All Time"

        embed = discord.Embed(
            title=f"{member_name} - {self.game.name} Stats",
            color=COLOR_WHITE
        )
        embed.set_footer(text=f"{self.PAGE_TITLES[self.current_page]} • {period_text}")

        if self.current_page == 0:
            # Overview page
            winrate = (self.stats.wins / self.stats.games_played * 100) if self.stats.games_played > 0 else 0
            embed.add_field(name="Games", value=str(self.stats.games_played), inline=True)
            embed.add_field(name="Win Rate", value=f"{winrate:.1f}%", inline=True)
            embed.add_field(name="Wins", value=str(self.stats.wins), inline=True)
            embed.add_field(name="Losses", value=str(self.stats.losses), inline=True)

            # Add Valorant stats if available
            if self.valorant_stats.get('total_games', 0) > 0:
                total_k = self.valorant_stats.get('total_kills', 0)
                total_d = self.valorant_stats.get('total_deaths', 0)
                total_a = self.valorant_stats.get('total_assists', 0)
                hs_pct = self.valorant_stats.get('hs_percent', 0)
                kd = total_k / total_d if total_d > 0 else total_k

                embed.add_field(name="\u200b", value="**Valorant Stats**", inline=False)
                embed.add_field(name="K/D/A", value=f"{total_k}/{total_d}/{total_a}", inline=True)
                embed.add_field(name="K/D", value=f"{kd:.2f}", inline=True)
                embed.add_field(name="HS%", value=f"{hs_pct:.1f}%", inline=True)

        elif self.current_page == 1:
            # Map performance page
            if self.valorant_stats.get('best_map'):
                best = self.valorant_stats['best_map']
                embed.add_field(
                    name="🏆 Best Map",
                    value=f"**{best['name']}**\n{best['winrate']:.1f}% WR ({best['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="🏆 Best Map", value="Not enough data\n(min 3 games)", inline=True)

            if self.valorant_stats.get('worst_map'):
                worst = self.valorant_stats['worst_map']
                embed.add_field(
                    name="💀 Worst Map",
                    value=f"**{worst['name']}**\n{worst['winrate']:.1f}% WR ({worst['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="💀 Worst Map", value="Not enough data\n(min 3 games)", inline=True)

        elif self.current_page == 2:
            # Teammate page
            if self.teammate_stats.get('best_teammate'):
                best = self.teammate_stats['best_teammate']
                best_member = self.guild.get_member(best['player_id'])
                best_name = best_member.display_name if best_member else f"User {best['player_id']}"
                embed.add_field(
                    name="🤝 Best Teammate",
                    value=f"**{best_name}**\n{best['winrate']:.1f}% WR ({best['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="🤝 Best Teammate", value="Not enough data\n(min 3 games together)", inline=True)

            if self.teammate_stats.get('cursed_teammate'):
                cursed = self.teammate_stats['cursed_teammate']
                cursed_member = self.guild.get_member(cursed['player_id'])
                cursed_name = cursed_member.display_name if cursed_member else f"User {cursed['player_id']}"
                embed.add_field(
                    name="💀 Cursed Teammate",
                    value=f"**{cursed_name}**\n{cursed['winrate']:.1f}% WR ({cursed['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="💀 Cursed Teammate", value="Not enough data\n(min 3 games together)", inline=True)

        elif self.current_page == 3:
            # Recent matches page
            if self.recent_matches:
                lines = []
                for match in self.recent_matches:
                    won = match['team'] == match['winning_team']
                    result = "✅" if won else "❌"
                    kda = ""
                    if match.get('kills') is not None:
                        kda = f" - {match['kills']}/{match['deaths']}/{match['assists']}"
                        if match.get('agent'):
                            kda += f" ({match['agent']})"
                    # Use match_map_name (from map vote) if available, else valorant stats map_name
                    map_display = match.get('match_map_name') or match.get('map_name')
                    map_name = f" on {map_display}" if map_display else ""
                    short_id = match.get('short_id') or str(match['match_id'])
                    lines.append(f"{result} Match {short_id}{map_name}{kda}")
                embed.add_field(name="Recent Matches", value="\n".join(lines), inline=False)
            else:
                embed.add_field(name="Recent Matches", value="No completed matches yet", inline=False)

        return embed

    async def prev_page(self, interaction: discord.Interaction):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = await self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

    async def next_page(self, interaction: discord.Interaction):
        if self.current_page < len(self.PAGE_TITLES) - 1:
            self.current_page += 1
            self.update_buttons()
            embed = await self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

    async def toggle_period(self, interaction: discord.Interaction):
        self.monthly = not self.monthly
        await self.load_data()
        self.update_buttons()
        embed = await self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)


class StatsSelectDropdown(discord.ui.Select):
    """Dropdown for selecting stats view: Monthly, Lifetime, or specific matches."""

    def __init__(self, recent_matches: List[dict]):
        # Use current month name for the monthly option
        current_month = datetime.now(timezone.utc).strftime('%B')
        options = [
            discord.SelectOption(label=current_month, value="seasonal", description="This month's stats", default=True),
            discord.SelectOption(label="Lifetime", value="lifetime", description="All-time stats"),
        ]
        # Add recent matches as options
        for i, match in enumerate(recent_matches[:5]):
            map_name = match.get('map_name') or match.get('match_map_name') or "Unknown"
            kills = match.get('kills', 0) or 0
            deaths = match.get('deaths', 0) or 0
            assists = match.get('assists', 0) or 0
            kda = f"{kills}/{deaths}/{assists}"
            label = f"{map_name} {kda}"
            if len(label) > 25:
                label = label[:22] + "..."
            match_id = match.get('match_id', i)
            options.append(discord.SelectOption(
                label=label,
                value=f"match_{match_id}",
                description=f"Match #{match.get('short_id') or match_id}"
            ))

        super().__init__(placeholder="Select stats view...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await self.view.handle_selection(interaction, self.values[0])


class StatsImageView(discord.ui.View):
    """View for stats card image with dropdown selection."""

    def __init__(self, cog: 'CustomMatch', member: discord.Member, game: 'GameConfig',
                 images: Dict[str, io.BytesIO], recent_matches: List[dict], invoker_id: int,
                 guild: discord.Guild):
        super().__init__(timeout=None)  # Persistent
        self.cog = cog
        self.member = member
        self.game = game
        self.images = images  # {'lifetime': BytesIO, 'seasonal': BytesIO, 'match_X': BytesIO}
        self.recent_matches = recent_matches
        self.invoker_id = invoker_id
        self.guild = guild
        self.current = 'seasonal'
        self.add_item(StatsSelectDropdown(recent_matches))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Only the person who ran this command can use this dropdown.",
                ephemeral=True
            )
            return False
        return True

    async def handle_selection(self, interaction: discord.Interaction, value: str):
        self.current = value

        # Update dropdown default
        for item in self.children:
            if isinstance(item, StatsSelectDropdown):
                for option in item.options:
                    option.default = (option.value == value)

        if value in self.images:
            # Use cached image
            self.images[value].seek(0)
            filename = f'stats_{value}.png'
            file = discord.File(self.images[value], filename=filename)
            embed = discord.Embed(
                title=f"{self.member.display_name} - {self.game.name} Stats",
                color=COLOR_WHITE
            )
            embed.set_image(url=f"attachment://{filename}")
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
        elif value.startswith('match_'):
            # Generate match-specific scoreboard image on demand
            await interaction.response.defer()

            match_id = int(value.replace('match_', ''))
            match_data = None
            for m in self.recent_matches:
                if m.get('match_id') == match_id:
                    match_data = m
                    break

            if not match_data:
                await interaction.followup.send("Match not found.", ephemeral=True)
                return

            # Get full match stats from database
            full_stats = await DatabaseHelper.get_player_match_stats(self.member.id, match_id)

            # Build match scoreboard data
            map_name = match_data.get('map_name') or match_data.get('match_map_name') or "Unknown"
            won = match_data.get('team') == match_data.get('winning_team')

            scoreboard_data = {
                'player_name': self.member.display_name,
                'avatar_url': self.member.display_avatar.url,
                'map_name': map_name,
                'agent': full_stats.get('agent') or match_data.get('agent') or 'Unknown',
                'won': won,
                'kills': full_stats.get('kills') or match_data.get('kills', 0) or 0,
                'deaths': full_stats.get('deaths') or match_data.get('deaths', 0) or 0,
                'assists': full_stats.get('assists') or match_data.get('assists', 0) or 0,
                'score': full_stats.get('score', 0) or 0,
                'damage': full_stats.get('damage_dealt', 0) or 0,
                'first_bloods': full_stats.get('first_bloods', 0) or 0,
                'headshots': full_stats.get('headshots', 0) or 0,
                'bodyshots': full_stats.get('bodyshots', 0) or 0,
                'legshots': full_stats.get('legshots', 0) or 0,
                'plants': full_stats.get('plants', 0) or 0,
                'defuses': full_stats.get('defuses', 0) or 0,
                'c2k': full_stats.get('c2k', 0) or 0,
                'c3k': full_stats.get('c3k', 0) or 0,
                'c4k': full_stats.get('c4k', 0) or 0,
                'c5k': full_stats.get('c5k', 0) or 0,
                'econ_spent': full_stats.get('econ_spent', 0) or 0,
                'econ_loadout': full_stats.get('econ_loadout', 0) or 0,
            }

            image = await self.cog.stats_generator.generate_match_image(scoreboard_data)
            if image:
                self.images[value] = image
                image.seek(0)
                filename = f'match_{match_id}.png'
                file = discord.File(image, filename=filename)
                embed = discord.Embed(
                    title=f"{self.member.display_name} - Match Scoreboard",
                    color=COLOR_WHITE
                )
                embed.set_image(url=f"attachment://{filename}")
                await interaction.edit_original_response(embed=embed, attachments=[file], view=self)
            else:
                await interaction.followup.send("Failed to generate match scoreboard.", ephemeral=True)


# =============================================================================
# MAIN COG
# =============================================================================

class CustomMatch(commands.Cog):
    """Custom match management system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.queues: Dict[int, QueueState] = {}  # queue_id -> QueueState
        self.ready_check_tasks: Dict[int, asyncio.Task] = {}  # queue_id -> task
        self.match_timeout_tasks: Dict[int, asyncio.Task] = {}  # match_id -> task
        self.valorant_stats_tasks: Dict[int, asyncio.Task] = {}  # match_id -> stats fetch task
        self.queue_timeout_task: Optional[asyncio.Task] = None
        self.penalty_decay_task: Optional[asyncio.Task] = None
        self.queue_schedule_task: Optional[asyncio.Task] = None
        self.orphan_cleanup_task: Optional[asyncio.Task] = None
        self.henrik_api = HenrikDevAPI(bot)
        self.stats_generator = StatsCardGenerator()
        self.ready_check_lock: asyncio.Lock = asyncio.Lock()  # Prevents race in ready check
        self.queue_lock: asyncio.Lock = asyncio.Lock()  # Prevents race in queue join/leave
        self.match_finalize_locks: Dict[int, asyncio.Lock] = {}  # match_id -> lock to prevent double finalization

    async def cog_load(self):
        await init_db()
        # Restore active queues from database
        await self.restore_queues()
        # Restore persistent leaderboard views
        await self.restore_leaderboard_views()
        # Restore match timeout tasks for active matches
        await self.restore_match_timeout_tasks()
        # Restore Valorant stats fetch tasks for recently completed matches
        await self.restore_valorant_stats_tasks()
        # Start background tasks
        self.queue_timeout_task = asyncio.create_task(self.queue_timeout_check())
        self.penalty_decay_task = asyncio.create_task(self.penalty_decay_check())
        self.queue_schedule_task = asyncio.create_task(self.queue_schedule_check())
        self.orphan_cleanup_task = asyncio.create_task(self.orphan_match_cleanup())
        # Initialize stats card generator
        await self.stats_generator.initialize()
        logger.info("CustomMatch cog loaded, database initialized.")

    async def restore_queues(self):
        """Restore active queues from database after restart."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row

                # First, clean up orphaned queue_players (where queue doesn't exist)
                await db.execute("""
                    DELETE FROM queue_players
                    WHERE queue_id NOT IN (SELECT queue_id FROM active_queues)
                """)

                # Clean up stale ready_check queues (older than 1 hour)
                await db.execute("""
                    DELETE FROM queue_players WHERE queue_id IN (
                        SELECT queue_id FROM active_queues
                        WHERE state = 'ready_check'
                        AND datetime(ready_check_started) < datetime('now', '-1 hour')
                    )
                """)
                await db.execute("""
                    DELETE FROM active_queues
                    WHERE state = 'ready_check'
                    AND datetime(ready_check_started) < datetime('now', '-1 hour')
                """)

                # Reset any remaining ready_check queues to waiting state
                # We can't properly resume the timeout task after restart, so reset them
                await db.execute("""
                    UPDATE active_queues
                    SET state = 'waiting', ready_check_started = NULL
                    WHERE state = 'ready_check'
                """)
                # Also reset players' ready status in those queues
                await db.execute("""
                    UPDATE queue_players SET is_ready = 0
                    WHERE queue_id IN (SELECT queue_id FROM active_queues WHERE state = 'waiting')
                """)
                logger.info("Reset any ready_check queues to waiting state after restart")

                await db.commit()

                async with db.execute("SELECT * FROM active_queues") as cursor:
                    rows = await cursor.fetchall()

                for row in rows:
                    queue_id = row["queue_id"]
                    game_id = row["game_id"]
                    channel_id = row["channel_id"]
                    message_id = row["message_id"]
                    state = row["state"]
                    short_id = row["short_id"] if "short_id" in row.keys() else None

                    # Get players in this queue
                    async with db.execute(
                        "SELECT player_id FROM queue_players WHERE queue_id = ?",
                        (queue_id,)
                    ) as pcursor:
                        player_rows = await pcursor.fetchall()
                        players = {r[0]: False for r in player_rows}  # All not ready initially

                    # Create queue state
                    queue_state = QueueState(
                        queue_id=queue_id,
                        game_id=game_id,
                        channel_id=channel_id,
                        message_id=message_id,
                        players=players,
                        state=state,
                        short_id=short_id
                    )
                    self.queues[queue_id] = queue_state

                    # Re-register the view with the bot based on state
                    game = await DatabaseHelper.get_game(game_id)
                    if game and message_id:
                        if state == "ready_check":
                            view = ReadyCheckView(self, game_id, queue_id)
                        else:
                            view = QueueView(self, game_id, queue_id)
                        self.bot.add_view(view, message_id=message_id)

            logger.info(f"Restored {len(self.queues)} active queues from database.")
        except Exception as e:
            logger.error(f"Error restoring queues: {e}")

    async def restore_leaderboard_views(self):
        """Re-register persistent leaderboard views after restart."""
        try:
            games = await DatabaseHelper.get_all_games()
            count = 0
            for game in games:
                if game.leaderboard_channel_id and game.leaderboard_message_id:
                    is_valorant = 'valorant' in game.name.lower()
                    view = PersistentLeaderboardView(self, game.game_id, is_valorant=is_valorant)
                    self.bot.add_view(view, message_id=game.leaderboard_message_id)
                    count += 1
            logger.info(f"Restored {count} persistent leaderboard views.")
        except Exception as e:
            logger.error(f"Error restoring leaderboard views: {e}")

    async def restore_match_timeout_tasks(self):
        """Restore match timeout tasks for active matches after restart."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT match_id, channel_id, created_at FROM matches "
                    "WHERE winning_team IS NULL AND cancelled = 0 AND channel_id IS NOT NULL"
                ) as cursor:
                    rows = await cursor.fetchall()

            count = 0
            max_timeout = 3 * 60 * 60
            for row in rows:
                match_id = row["match_id"]
                channel_id = row["channel_id"]
                created_at_str = row["created_at"]

                if not created_at_str or not channel_id:
                    continue

                try:
                    created_at = datetime.fromisoformat(created_at_str)
                except (ValueError, TypeError):
                    continue

                # Calculate remaining timeout
                now = datetime.now(timezone.utc)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                elapsed = (now - created_at).total_seconds()
                remaining = max(0, max_timeout - elapsed)

                if remaining <= 0:
                    remaining = 60  # Fire soon for already-expired matches

                # Find the channel in any guild
                channel = None
                for guild in self.bot.guilds:
                    channel = guild.get_channel(channel_id)
                    if channel:
                        break

                if not channel:
                    continue

                task = asyncio.create_task(
                    self.match_timeout(channel.guild, match_id, channel, delay_seconds=int(remaining))
                )
                self.match_timeout_tasks[match_id] = task
                count += 1

            logger.info(f"Restored {count} match timeout tasks.")
        except Exception as e:
            logger.error(f"Error restoring match timeout tasks: {e}")

    async def restore_valorant_stats_tasks(self):
        """Restore Valorant stats fetch tasks for recently completed matches missing stats."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                # Find Valorant matches completed within the last 20 minutes that have no stats
                async with db.execute(
                    """SELECT m.match_id, m.game_id, m.decided_at
                       FROM matches m
                       JOIN games g ON m.game_id = g.game_id
                       WHERE m.winning_team IS NOT NULL
                         AND m.cancelled = 0
                         AND m.decided_at IS NOT NULL
                         AND LOWER(g.name) LIKE '%valorant%'
                         AND m.match_id NOT IN (
                             SELECT DISTINCT match_id FROM valorant_match_stats
                         )
                         AND m.decided_at >= datetime('now', '-20 minutes')"""
                ) as cursor:
                    rows = await cursor.fetchall()

            count = 0
            for row in rows:
                match_id = row["match_id"]
                game_id = row["game_id"]
                decided_at_str = row["decided_at"]

                try:
                    decided_at = datetime.fromisoformat(decided_at_str)
                except (ValueError, TypeError):
                    continue

                if decided_at.tzinfo is None:
                    decided_at = decided_at.replace(tzinfo=timezone.utc)

                # Get player IDs for this match
                players = await DatabaseHelper.get_match_players(match_id)
                player_ids = [p["player_id"] for p in players]

                if not player_ids:
                    continue

                # Find guild (use first guild the bot is in)
                guild = self.bot.guilds[0] if self.bot.guilds else None

                self.valorant_stats_tasks[match_id] = asyncio.create_task(
                    self.fetch_valorant_match_stats_with_retry(
                        match_id, game_id, player_ids, decided_at, guild
                    )
                )
                count += 1

            logger.info(f"Restored {count} Valorant stats fetch tasks.")
        except Exception as e:
            logger.error(f"Error restoring Valorant stats tasks: {e}")

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle orphaned queue interactions that weren't caught by registered views."""
        if interaction.type != discord.InteractionType.component:
            return

        # Skip if already handled by a registered view
        if interaction.response.is_done():
            return

        custom_id = interaction.data.get("custom_id", "")

        # Handle queue join/leave buttons
        if custom_id.startswith("cm_queue_join:") or custom_id.startswith("cm_queue_leave:"):
            try:
                queue_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            # Check if queue exists in memory
            if queue_id not in self.queues:
                await interaction.response.send_message(
                    "This queue is no longer active. Please use a new queue.",
                    ephemeral=True
                )
                return

            queue_state = self.queues[queue_id]
            game = await DatabaseHelper.get_game(queue_state.game_id)
            if not game:
                return

            # Route to appropriate handler
            if custom_id.startswith("cm_queue_join:"):
                await self.handle_queue_join(interaction, game.game_id, queue_id)
            else:
                await self.handle_queue_leave(interaction, game.game_id, queue_id)

        # Handle ready check buttons
        elif custom_id.startswith("cm_ready_yes:") or custom_id.startswith("cm_ready_no:"):
            try:
                queue_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            if queue_id not in self.queues:
                await interaction.response.send_message(
                    "This ready check has expired.",
                    ephemeral=True
                )
                return

            is_ready = custom_id.startswith("cm_ready_yes:")
            await self.handle_ready(interaction, queue_id, is_ready)

        # Handle win vote buttons (persistent across restart)
        elif custom_id.startswith("cm_vote_red:") or custom_id.startswith("cm_vote_blue:"):
            try:
                match_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            team = Team.RED if custom_id.startswith("cm_vote_red:") else Team.BLUE
            await self.handle_win_vote(interaction, match_id, team)

        # Handle abandon vote button (persistent across restart)
        elif custom_id.startswith("cm_abandon_vote:"):
            try:
                match_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            # Get match to determine needed votes
            match = await DatabaseHelper.get_match(match_id)
            if match:
                players = await DatabaseHelper.get_match_players(match_id)
                total_players = len(players)
                needed_votes = max(2, (total_players // 2) + 1)  # Majority vote
                await self.handle_abandon_vote(interaction, match_id, needed_votes)
            else:
                await interaction.response.send_message("Match not found.", ephemeral=True)

        # Handle persistent leaderboard buttons (fallback if view wasn't registered)
        elif custom_id.startswith("cm_lb_alltime:") or custom_id.startswith("cm_lb_matches:"):
            try:
                game_id = int(custom_id.split(":")[1])
            except (IndexError, ValueError):
                return

            game = await DatabaseHelper.get_game(game_id)
            if not game:
                await interaction.response.send_message("Game not found.", ephemeral=True)
                return

            if custom_id.startswith("cm_lb_alltime:"):
                embed = await self._build_leaderboard_text_embed(interaction.guild, game_id, monthly=False)
                await interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                # Matches button
                await interaction.response.defer(ephemeral=True)
                recent = await DatabaseHelper.get_recent_completed_matches(game_id, limit=5)
                if not recent:
                    await interaction.followup.send("No completed matches found.", ephemeral=True)
                    return
                latest = recent[0]
                embed, file = await self._generate_match_scoreboard(interaction.guild, latest["match_id"])
                view = MatchSelectView(self, recent) if len(recent) > 1 else None
                kwargs = {"embed": embed, "ephemeral": True}
                if file:
                    kwargs["file"] = file
                if view:
                    kwargs["view"] = view
                await interaction.followup.send(**kwargs)

    async def cog_unload(self):
        # Cancel all tasks
        for task in self.ready_check_tasks.values():
            task.cancel()
        for task in self.match_timeout_tasks.values():
            task.cancel()
        for task in self.valorant_stats_tasks.values():
            task.cancel()
        if self.queue_timeout_task:
            self.queue_timeout_task.cancel()
        if self.penalty_decay_task:
            self.penalty_decay_task.cancel()
        if self.queue_schedule_task:
            self.queue_schedule_task.cancel()
        if self.orphan_cleanup_task:
            self.orphan_cleanup_task.cancel()
        # Close API session
        await self.henrik_api.close()
        # Close stats generator
        await self.stats_generator.close()

    # -------------------------------------------------------------------------
    # BACKGROUND TASKS
    # -------------------------------------------------------------------------

    async def queue_timeout_check(self):
        """Background task to remove players who have been in queue too long."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                games = await DatabaseHelper.get_all_games()
                games_with_timeout = {g.game_id: g for g in games if g.queue_timeout_minutes > 0}

                for queue_id, queue_state in list(self.queues.items()):
                    if queue_state.state != "waiting":
                        continue

                    game = games_with_timeout.get(queue_state.game_id)
                    if not game:
                        continue

                    # Get join times
                    join_times = await DatabaseHelper.get_queue_join_times(queue_id)
                    now = datetime.now(timezone.utc)
                    timeout_delta = timedelta(minutes=game.queue_timeout_minutes)

                    removed = []
                    for pid, joined_at in join_times.items():
                        if pid in queue_state.players and (now - joined_at) > timeout_delta:
                            del queue_state.players[pid]
                            await DatabaseHelper.remove_player_from_queue(queue_id, pid)
                            removed.append(pid)

                    if removed:
                        # Update embed
                        channel = self.bot.get_channel(queue_state.channel_id)
                        if channel and queue_state.message_id:
                            try:
                                msg = await channel.fetch_message(queue_state.message_id)
                                embed = await self.create_queue_embed(game, queue_state)
                                view = QueueView(self, game.game_id, queue_id)
                                await msg.edit(embed=embed, view=view)
                                mentions = ", ".join([f"<@{pid}>" for pid in removed])
                                await channel.send(
                                    f"Removed from queue (timeout): {mentions}",
                                    delete_after=10
                                )
                            except Exception:
                                pass

            except Exception as e:
                logger.error(f"Queue timeout check error: {e}")

            await asyncio.sleep(60)  # Check every minute

    async def penalty_decay_check(self):
        """Background task to decay old penalty offenses."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                # This runs daily - actual decay logic is in add_ready_penalty_offense
                # based on penalty_decay_days setting
                # We just clean up expired penalties from the database here
                async with aiosqlite.connect(DB_PATH) as db:
                    now = datetime.now(timezone.utc).isoformat()
                    # Clear expired penalties (set to no active penalty)
                    await db.execute(
                        "UPDATE ready_penalties SET penalty_expires = NULL WHERE penalty_expires < ?",
                        (now,)
                    )
                    await db.commit()

            except Exception as e:
                logger.error(f"Penalty decay check error: {e}")

            await asyncio.sleep(3600 * 24)  # Check daily

    async def queue_schedule_check(self):
        """Background task to manage queue open/close schedules."""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                now = datetime.now()
                current_day = now.weekday()  # 0=Monday, 6=Sunday
                current_time = now.strftime("%H:%M")

                games = await DatabaseHelper.get_all_games()
                scheduled_games = [g for g in games if g.schedule_enabled and g.queue_channel_id]

                for game in scheduled_games:
                    # Get today's schedule (prefer new format, fall back to legacy)
                    open_time, close_time = self._get_day_schedule(game, current_day)
                    if not open_time or not close_time:
                        continue

                    channel = self.bot.get_channel(game.queue_channel_id)
                    if not channel:
                        continue
                    guild = channel.guild

                    # Check if we need to open or close the queue
                    queue_exists = any(qs.game_id == game.game_id and qs.state == "waiting" for qs in self.queues.values())
                    has_down_message = game.schedule_down_message_id is not None

                    if current_time == open_time:
                        # Time to open the queue
                        if not queue_exists:
                            # Delete the down message if it exists
                            if has_down_message:
                                try:
                                    old_msg = await channel.fetch_message(game.schedule_down_message_id)
                                    await old_msg.delete()
                                except discord.NotFound:
                                    pass
                                await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=None)

                            # Start fresh queue
                            await self.start_queue(channel, game)
                            await self.log_action(guild, f"Queue opened for **{game.name}** (scheduled)")

                    elif current_time == close_time:
                        # Time to close the queue
                        await self._close_queue_for_schedule(game, guild, channel)

            except Exception as e:
                logger.error(f"Queue schedule check error: {e}")

            await asyncio.sleep(60)  # Check every minute

    def _get_day_schedule(self, game: GameConfig, day: int) -> Tuple[Optional[str], Optional[str]]:
        """Get open/close times for a specific day. Returns (open_time, close_time) or (None, None)."""
        # Try new per-day format first
        if game.schedule_times and str(day) in game.schedule_times:
            times = game.schedule_times[str(day)]
            return times.get("open"), times.get("close")

        # Fall back to legacy format
        if game.schedule_open_days and game.schedule_open_time and game.schedule_close_time:
            open_days = [int(d) for d in game.schedule_open_days.split(",") if d.isdigit()]
            if day in open_days:
                return game.schedule_open_time, game.schedule_close_time

        return None, None

    def _is_currently_open(self, game: GameConfig) -> bool:
        """Check if queue should currently be open based on schedule."""
        now = datetime.now()
        current_day = now.weekday()
        current_time = now.strftime("%H:%M")

        open_time, close_time = self._get_day_schedule(game, current_day)
        if not open_time or not close_time:
            return False

        # Handle overnight schedules (close_time < open_time)
        if close_time < open_time:
            return current_time >= open_time or current_time < close_time
        else:
            return open_time <= current_time < close_time

    async def apply_schedule_state(self, game: GameConfig):
        """Apply the current schedule state immediately (called when schedule is toggled on)."""
        try:
            print(f"[CUSTOMMATCH] apply_schedule_state called for {game.name}", flush=True)
            if not game.queue_channel_id:
                print(f"[CUSTOMMATCH] apply_schedule_state: No queue channel for {game.name}", flush=True)
                return

            channel = self.bot.get_channel(game.queue_channel_id)
            if not channel:
                print(f"[CUSTOMMATCH] apply_schedule_state: Channel {game.queue_channel_id} not found", flush=True)
                return

            guild = channel.guild
            print(f"[CUSTOMMATCH] apply_schedule_state: Got channel {channel.name} in guild {guild.name}", flush=True)

            queue_exists = any(qs.game_id == game.game_id and qs.state == "waiting" for qs in self.queues.values())
            is_open = self._is_currently_open(game)
            print(f"[CUSTOMMATCH] apply_schedule_state: is_open={is_open}, queue_exists={queue_exists}", flush=True)

            if is_open:
                # Should be open
                print(f"[CUSTOMMATCH] apply_schedule_state: Queue SHOULD be open", flush=True)
                if not queue_exists:
                    # Delete down message if exists
                    if game.schedule_down_message_id:
                        try:
                            old_msg = await channel.fetch_message(game.schedule_down_message_id)
                            await old_msg.delete()
                        except discord.NotFound:
                            pass
                        await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=None)

                    # Start queue
                    await self.start_queue(channel, game)
                    await self.log_action(guild, f"Queue opened for **{game.name}** (schedule enabled)")
            else:
                # Should be closed
                print(f"[CUSTOMMATCH] apply_schedule_state: Queue SHOULD be closed, calling _close_queue_for_schedule", flush=True)
                await self._close_queue_for_schedule(game, guild, channel)
        except Exception as e:
            print(f"[CUSTOMMATCH] apply_schedule_state ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()

    async def _close_queue_for_schedule(self, game: GameConfig, guild: discord.Guild, channel: discord.TextChannel):
        """Close queue and show countdown embed."""
        has_down_message = game.schedule_down_message_id is not None
        print(f"[CUSTOMMATCH] _close_queue_for_schedule: {game.name} - has_down_message={has_down_message}", flush=True)

        # Close any existing queue from memory
        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Checking {len(self.queues)} queues in memory", flush=True)
        for qid, qs in list(self.queues.items()):
            if qs.game_id == game.game_id:
                print(f"[CUSTOMMATCH] _close_queue_for_schedule: Found queue {qid} in memory, deleting msg {qs.message_id}", flush=True)
                if qs.message_id:
                    try:
                        old_msg = await channel.fetch_message(qs.message_id)
                        await old_msg.delete()
                        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Deleted queue message {qs.message_id}", flush=True)
                    except discord.NotFound:
                        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Queue message {qs.message_id} not found", flush=True)
                # Remove from memory
                if qid in self.queues:
                    del self.queues[qid]
                # Remove from database
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (qid,))
                    await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (qid,))
                    await db.commit()
                print(f"[CUSTOMMATCH] _close_queue_for_schedule: Queue {qid} removed from memory and DB", flush=True)

        # Fallback: Check database for orphan queues not in memory
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT queue_id, message_id FROM active_queues WHERE game_id = ?",
                (game.game_id,)
            ) as cursor:
                orphan_queues = await cursor.fetchall()
            print(f"[CUSTOMMATCH] _close_queue_for_schedule: Found {len(orphan_queues)} orphan queues in DB", flush=True)

            for row in orphan_queues:
                qid = row["queue_id"]
                msg_id = row["message_id"]
                print(f"[CUSTOMMATCH] _close_queue_for_schedule: Orphan queue {qid}, msg {msg_id}", flush=True)
                if msg_id:
                    try:
                        old_msg = await channel.fetch_message(msg_id)
                        await old_msg.delete()
                        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Deleted orphan message {msg_id}", flush=True)
                    except discord.NotFound:
                        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Orphan message {msg_id} not found", flush=True)
                await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (qid,))
                await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (qid,))
            await db.commit()

        # Fallback: Scan recent messages for any queue embeds from this bot
        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Scanning recent messages for queue embeds", flush=True)
        try:
            async for msg in channel.history(limit=20):
                if msg.author.id == self.bot.user.id and msg.embeds:
                    embed = msg.embeds[0]
                    # Check if it's a queue embed for this game (has "Queue" in title and game name)
                    if embed.title and game.name in embed.title and "Queue" in embed.title and "Closed" not in embed.title:
                        await msg.delete()
                        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Deleted stale queue embed {msg.id}", flush=True)
        except Exception as e:
            print(f"[CUSTOMMATCH] _close_queue_for_schedule: Error scanning messages: {e}", flush=True)

        # Check if existing down message is still valid
        if has_down_message:
            try:
                existing_msg = await channel.fetch_message(game.schedule_down_message_id)
                print(f"[CUSTOMMATCH] _close_queue_for_schedule: Down message {game.schedule_down_message_id} still exists", flush=True)
                # Message exists, we're done
                return
            except discord.NotFound:
                print(f"[CUSTOMMATCH] _close_queue_for_schedule: Down message {game.schedule_down_message_id} not found, will create new one", flush=True)
                # Message was deleted, clear the ID
                await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=None)

        # Send new down message
        print(f"[CUSTOMMATCH] _close_queue_for_schedule: Creating countdown embed", flush=True)
        now = datetime.now()
        next_open_ts = self._calculate_next_open_time(game, now)

        embed = discord.Embed(
            title=f"{game.name} Queue - Closed",
            description=(
                f"The queue is currently closed.\n\n"
                f"**Opens:**\n"
                f"<t:{next_open_ts}:F>\n"
                f"<t:{next_open_ts}:R>"
            ),
            color=COLOR_NEUTRAL
        )
        if game.banner_url:
            embed.set_image(url=game.banner_url)

        down_msg = await channel.send(embed=embed)
        await DatabaseHelper.update_game(game.game_id, schedule_down_message_id=down_msg.id)
        logger.info(f"_close_queue_for_schedule: Sent countdown embed for {game.name}, msg_id={down_msg.id}")
        await self.log_action(guild, f"Queue closed for **{game.name}** (scheduled)")

    def _calculate_next_open_time(self, game: GameConfig, now: datetime) -> int:
        """Calculate the Unix timestamp of the next queue open time."""
        # Check schedule_times first (new format)
        if game.schedule_times:
            for days_ahead in range(1, 8):
                check_date = now + timedelta(days=days_ahead)
                day_num = str(check_date.weekday())
                if day_num in game.schedule_times:
                    times = game.schedule_times[day_num]
                    open_time = times.get("open", "00:00")
                    open_parts = open_time.split(":")
                    open_hour = int(open_parts[0])
                    open_minute = int(open_parts[1])
                    check_date = check_date.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
                    return int(check_date.timestamp())

            # If still on a scheduled day today but past open time, check if it's before close
            day_num = str(now.weekday())
            if day_num in game.schedule_times:
                times = game.schedule_times[day_num]
                open_time = times.get("open", "00:00")
                if now.strftime("%H:%M") < open_time:
                    open_parts = open_time.split(":")
                    open_hour = int(open_parts[0])
                    open_minute = int(open_parts[1])
                    today_open = now.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
                    return int(today_open.timestamp())

        # Fall back to legacy format
        if game.schedule_open_days and game.schedule_open_time:
            open_days = [int(d) for d in game.schedule_open_days.split(",") if d.isdigit()]
            open_parts = game.schedule_open_time.split(":")
            open_hour = int(open_parts[0])
            open_minute = int(open_parts[1])

            # Start from tomorrow
            check_date = now + timedelta(days=1)
            check_date = check_date.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)

            for _ in range(7):
                if check_date.weekday() in open_days:
                    return int(check_date.timestamp())
                check_date += timedelta(days=1)

        # Fallback to a week from now
        return int((now + timedelta(days=7)).timestamp())

    async def orphan_match_cleanup(self):
        """Background task to clean up orphaned matches - matches that are stuck without channels/roles."""
        await self.bot.wait_until_ready()
        # Run cleanup on startup
        await asyncio.sleep(30)  # Wait for bot to fully initialize
        await self._do_orphan_cleanup()

        while not self.bot.is_closed():
            try:
                await asyncio.sleep(3600)  # Check every hour
                await self._do_orphan_cleanup()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Orphan cleanup error: {e}")
                await asyncio.sleep(300)  # Wait 5 min on error before retry

    async def _do_orphan_cleanup(self):
        """Perform the actual orphan match cleanup."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row

                # Find active matches (not cancelled, no winner)
                async with db.execute(
                    """SELECT match_id, game_id, channel_id, red_role_id, blue_role_id,
                              red_vc_id, blue_vc_id, created_at, short_id
                       FROM matches
                       WHERE winning_team IS NULL AND cancelled = 0"""
                ) as cursor:
                    active_matches = await cursor.fetchall()

                orphaned_count = 0
                for match in active_matches:
                    # Convert Row to dict for easier access
                    match_dict = dict(match)
                    match_id = match_dict["match_id"]
                    game_id = match_dict["game_id"]
                    channel_id = match_dict["channel_id"]
                    red_role_id = match_dict["red_role_id"]
                    blue_role_id = match_dict["blue_role_id"]
                    created_at_str = match_dict["created_at"]
                    short_id = match_dict.get("short_id") or str(match_id)
                    red_vc_id = match_dict.get("red_vc_id")
                    blue_vc_id = match_dict.get("blue_vc_id")

                    # Get the game to find the guild
                    game = await DatabaseHelper.get_game(game_id)
                    if not game or not game.queue_channel_id:
                        continue

                    guild = None
                    for g in self.bot.guilds:
                        if g.get_channel(game.queue_channel_id):
                            guild = g
                            break

                    if not guild:
                        continue

                    is_orphaned = False
                    reasons = []

                    # Check 1: Match channel doesn't exist or was never created
                    if channel_id:
                        channel = guild.get_channel(channel_id)
                        if not channel:
                            is_orphaned = True
                            reasons.append("channel deleted")
                    else:
                        # No channel ID at all - definitely orphaned if older than 5 minutes
                        if created_at_str:
                            try:
                                created_at = datetime.fromisoformat(created_at_str)
                                if datetime.now(timezone.utc) - created_at > timedelta(minutes=5):
                                    is_orphaned = True
                                    reasons.append("no channel created")
                            except Exception:
                                is_orphaned = True
                                reasons.append("no channel, invalid date")

                    # Check 2: Both team roles don't exist
                    red_role = guild.get_role(red_role_id) if red_role_id else None
                    blue_role = guild.get_role(blue_role_id) if blue_role_id else None
                    if not red_role and not blue_role and (red_role_id or blue_role_id):
                        is_orphaned = True
                        if "channel" not in str(reasons):
                            reasons.append("roles deleted")

                    # Check 3: Match is older than 6 hours without activity (very stale)
                    if created_at_str and not is_orphaned:
                        try:
                            created_at = datetime.fromisoformat(created_at_str)
                            if datetime.now(timezone.utc) - created_at > timedelta(hours=6):
                                if not channel_id or not guild.get_channel(channel_id):
                                    is_orphaned = True
                                    reasons.append("stale (6h+)")
                        except Exception:
                            pass

                    if is_orphaned:
                        logger.info(f"Cleaning up orphaned match #{match_id} ({short_id}): {', '.join(reasons)}")

                        # Mark as cancelled
                        await db.execute(
                            "UPDATE matches SET cancelled = 1 WHERE match_id = ?",
                            (match_id,)
                        )

                        # Clean up roles if they still exist
                        if red_role:
                            try:
                                await red_role.delete()
                            except Exception:
                                pass
                        if blue_role:
                            try:
                                await blue_role.delete()
                            except Exception:
                                pass

                        # Clean up VCs if they exist
                        if red_vc_id:
                            vc = guild.get_channel(red_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception:
                                    pass
                        if blue_vc_id:
                            vc = guild.get_channel(blue_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception:
                                    pass

                        orphaned_count += 1

                await db.commit()

                if orphaned_count > 0:
                    logger.info(f"Cleaned up {orphaned_count} orphaned matches")

        except Exception as e:
            logger.error(f"Error in _do_orphan_cleanup: {e}", exc_info=True)

    async def manual_orphan_cleanup(self, guild: discord.Guild) -> Tuple[int, List[str]]:
        """Manually trigger orphan cleanup. Returns (cleaned_count, details)."""
        details = []
        cleaned_count = 0

        try:
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row

                # Find active matches
                async with db.execute(
                    """SELECT match_id, game_id, channel_id, red_role_id, blue_role_id,
                              red_vc_id, blue_vc_id, created_at, short_id
                       FROM matches
                       WHERE winning_team IS NULL AND cancelled = 0"""
                ) as cursor:
                    active_matches = await cursor.fetchall()

                for match in active_matches:
                    match_id = match["match_id"]
                    channel_id = match["channel_id"]
                    red_role_id = match["red_role_id"]
                    blue_role_id = match["blue_role_id"]
                    # Use dict() to convert Row, then .get() for optional fields
                    match_dict = dict(match)
                    short_id = match_dict.get("short_id") or str(match_id)
                    red_vc_id = match_dict.get("red_vc_id")
                    blue_vc_id = match_dict.get("blue_vc_id")

                    is_orphaned = False
                    reasons = []

                    # Check channel - if no channel, definitely stale
                    if channel_id:
                        channel = guild.get_channel(channel_id)
                        if not channel:
                            is_orphaned = True
                            reasons.append("channel deleted")
                    else:
                        is_orphaned = True
                        reasons.append("no channel")

                    # Check roles - if both roles are missing, also stale
                    red_role = guild.get_role(red_role_id) if red_role_id else None
                    blue_role = guild.get_role(blue_role_id) if blue_role_id else None
                    if not red_role and not blue_role and (red_role_id or blue_role_id):
                        is_orphaned = True
                        if "no channel" not in reasons and "channel deleted" not in reasons:
                            reasons.append("roles missing")

                    if is_orphaned:
                        details.append(f"Match {short_id}: {', '.join(reasons)}")

                        # Mark as cancelled
                        await db.execute(
                            "UPDATE matches SET cancelled = 1 WHERE match_id = ?",
                            (match_id,)
                        )

                        # Delete remaining roles
                        if red_role:
                            try:
                                await red_role.delete()
                            except Exception as e:
                                logger.warning(f"Failed to delete red role for match {match_id}: {e}")
                        if blue_role:
                            try:
                                await blue_role.delete()
                            except Exception as e:
                                logger.warning(f"Failed to delete blue role for match {match_id}: {e}")

                        # Delete VCs
                        if red_vc_id:
                            vc = guild.get_channel(red_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception as e:
                                    logger.warning(f"Failed to delete red VC for match {match_id}: {e}")
                        if blue_vc_id:
                            vc = guild.get_channel(blue_vc_id)
                            if vc:
                                try:
                                    await vc.delete()
                                except Exception as e:
                                    logger.warning(f"Failed to delete blue VC for match {match_id}: {e}")

                        cleaned_count += 1

                await db.commit()

        except Exception as e:
            logger.error(f"Error in manual_orphan_cleanup: {e}", exc_info=True)
            details.append(f"Error: {str(e)}")

        return cleaned_count, details

    # -------------------------------------------------------------------------
    # HELPER METHODS
    # -------------------------------------------------------------------------

    async def is_cm_admin(self, member: discord.Member) -> bool:
        """Check if member is a CM admin."""
        if member.guild_permissions.administrator:
            return True
        
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        if admin_role_id:
            return any(r.id == int(admin_role_id) for r in member.roles)
        return False
    
    async def log_action(self, guild: discord.Guild, message: str):
        """Log an action to the log channel."""
        channel_id = await DatabaseHelper.get_config("log_channel_id")
        if channel_id:
            channel = guild.get_channel(int(channel_id))
            if channel:
                await channel.send(f"`[{datetime.now().strftime('%H:%M:%S')}]` {message}")

    async def _get_match_short_id(self, match_id: int) -> str:
        """Get the short_id for a match, or return match_id as string."""
        match = await DatabaseHelper.get_match(match_id)
        if match and match.get("short_id"):
            return match["short_id"]
        return str(match_id)

    def _get_map_image_url(self, game_name: str, map_name: str) -> Optional[str]:
        """Get map image URL from map_voter_config.json for use as embed thumbnail."""
        try:
            with open("map_voter_config.json", "r") as f:
                config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

        universal = config.get("universal_games", {})
        # Case-insensitive game name lookup
        game_data = None
        for key, value in universal.items():
            if key.lower() == game_name.lower():
                game_data = value
                break
        if not game_data:
            return None

        maps = game_data.get("maps", {})
        map_data = maps.get(map_name)
        if not map_data:
            return None

        return map_data.get("url")  # May be null (e.g. Pearl)

    async def _send_mmr_embed_to_log(
        self, guild: discord.Guild, game: GameConfig, match_id: int,
        red_team: List[int], blue_team: List[int], igns: Dict[int, str],
        red_role: discord.Role, blue_role: discord.Role
    ):
        """Send team embed with MMR values to log channel (admin only)."""
        channel_id = await DatabaseHelper.get_config("log_channel_id")
        if not channel_id:
            return

        channel = guild.get_channel(int(channel_id))
        if not channel:
            return

        short_id = await self._get_match_short_id(match_id)
        embed = discord.Embed(
            title=f"Match {short_id} Teams - {game.name}",
            description="Team compositions with MMR values (admin view)",
            color=COLOR_NEUTRAL
        )

        # Build red team lines with MMR
        red_lines = []
        red_total_mmr = 0
        for pid in red_team:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            mmr = stats.effective_mmr
            red_total_mmr += mmr
            if pid in igns:
                line = f"`{igns[pid]}` [{mmr} MMR]"
            else:
                line = f"<@{pid}> [{mmr} MMR]"
            red_lines.append(line)

        # Build blue team lines with MMR
        blue_lines = []
        blue_total_mmr = 0
        for pid in blue_team:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            mmr = stats.effective_mmr
            blue_total_mmr += mmr
            if pid in igns:
                line = f"`{igns[pid]}` [{mmr} MMR]"
            else:
                line = f"<@{pid}> [{mmr} MMR]"
            blue_lines.append(line)

        red_avg = red_total_mmr // len(red_team) if red_team else 0
        blue_avg = blue_total_mmr // len(blue_team) if blue_team else 0

        embed.add_field(
            name=f"{red_role.name} (Avg: {red_avg})",
            value="\n".join(red_lines) or "None",
            inline=True
        )
        embed.add_field(
            name=f"{blue_role.name} (Avg: {blue_avg})",
            value="\n".join(blue_lines) or "None",
            inline=True
        )

        await channel.send(embed=embed)

    async def get_next_role_number(self, guild: discord.Guild, prefix: str) -> int:
        """Get the next available role number (e.g., Blue1, Blue2)."""
        existing = [r for r in guild.roles if r.name.startswith(prefix)]
        numbers = []
        for r in existing:
            try:
                num = int(r.name[len(prefix):])
                numbers.append(num)
            except ValueError:
                pass
        
        for i in range(1, 100):
            if i not in numbers:
                return i
        return 1
    
    async def get_next_channel_suffix(self, guild: discord.Guild, category: discord.CategoryChannel, 
                                       base_name: str) -> str:
        """Get the next available channel name."""
        existing = [c.name for c in category.channels if c.name.startswith(base_name)]
        if base_name not in existing:
            return base_name
        
        for i in range(2, 100):
            name = f"{base_name}-{i}"
            if name not in existing:
                return name
        return f"{base_name}-new"
    
    # -------------------------------------------------------------------------
    # QUEUE MANAGEMENT
    # -------------------------------------------------------------------------
    
    async def create_queue_embed(self, game: GameConfig, queue_state: QueueState) -> discord.Embed:
        """Create the queue embed."""
        player_count = len(queue_state.players)

        embed = discord.Embed(
            title=f"{game.name} Queue ({game.queue_type.value.upper()})",
            description=f"Players: {player_count}/{game.player_count}",
            color=COLOR_NEUTRAL
        )

        if queue_state.players:
            player_list = "\n".join([f"• <@{pid}>" for pid in queue_state.players.keys()])
            embed.add_field(name="Joined", value=player_list, inline=False)

        # Add banner image if configured
        if game.banner_url:
            embed.set_image(url=game.banner_url)

        # Add short_id to footer
        if queue_state.short_id:
            embed.set_footer(text=queue_state.short_id)

        return embed
    
    async def create_ready_check_embed(self, game: GameConfig, queue_state: QueueState,
                                        time_remaining: int) -> discord.Embed:
        """Create the ready check embed with emoji indicators."""
        ready_count = sum(1 for is_ready in queue_state.players.values() if is_ready)
        total_count = len(queue_state.players)

        embed = discord.Embed(
            title="Queue Full - Ready Check!",
            description=f"Time remaining: {time_remaining}s\n\nReady: {ready_count}/{total_count}",
            color=COLOR_WARNING
        )

        # Build player list with emoji indicators
        player_lines = []
        for pid, is_ready in queue_state.players.items():
            emoji = game.ready_done_emoji if is_ready else game.ready_loading_emoji
            player_lines.append(f"{emoji} <@{pid}>")

        embed.add_field(
            name="Players",
            value="\n".join(player_lines) if player_lines else "No players",
            inline=False
        )

        # Add banner image if configured
        if game.banner_url:
            embed.set_image(url=game.banner_url)

        return embed
    
    async def start_queue(self, channel: discord.TextChannel, game: GameConfig) -> int:
        """Start a new queue for a game."""
        # Generate a short ID for this queue
        short_id = generate_short_id()

        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "INSERT INTO active_queues (game_id, channel_id, state, short_id) VALUES (?, ?, 'waiting', ?)",
                (game.game_id, channel.id, short_id)
            )
            queue_id = cursor.lastrowid
            await db.commit()

        queue_state = QueueState(
            queue_id=queue_id,
            game_id=game.game_id,
            channel_id=channel.id,
            short_id=short_id
        )
        self.queues[queue_id] = queue_state

        embed = await self.create_queue_embed(game, queue_state)
        view = QueueView(self, game.game_id, queue_id)
        msg = await channel.send(embed=embed, view=view)

        queue_state.message_id = msg.id
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_queues SET message_id = ? WHERE queue_id = ?",
                (msg.id, queue_id)
            )
            await db.commit()

        return queue_id
    
    async def handle_queue_join(self, interaction: discord.Interaction, game_id: int, queue_id: int):
        """Handle a player joining the queue."""
        user = interaction.user
        game = await DatabaseHelper.get_game(game_id)

        if not game:
            await interaction.response.send_message("Game no longer exists.", ephemeral=True)
            return

        # Quick in-memory checks first (no DB calls)
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return

        queue_state = self.queues[queue_id]

        if queue_state.state != "waiting":
            await interaction.response.send_message("Queue is no longer accepting players.", ephemeral=True)
            return

        if user.id in queue_state.players:
            await interaction.response.send_message("You're already in this queue.", ephemeral=True)
            return

        # Check verified role (if required) - no DB call, just role check
        if game.queue_role_required and game.verified_role_id:
            if not any(r.id == game.verified_role_id for r in user.roles):
                if game.verification_topic:
                    view = VerificationTicketView(self, game)
                    await interaction.response.send_message(
                        "You need the verified role to join this queue. Click the button below to get set up.",
                        view=view,
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        "You need the verified role to queue for this game.",
                        ephemeral=True
                    )
                return

        # Defer early to prevent timeout - we'll do DB operations next
        await interaction.response.defer()

        try:
            # Check if schedule says queue should be closed
            if game.schedule_enabled and not self._is_currently_open(game):
                next_open_ts = self._calculate_next_open_time(game, datetime.now())
                await interaction.followup.send(
                    f"Queue is currently closed.\n**Opens:** <t:{next_open_ts}:R>",
                    ephemeral=True
                )
                return

            # Batch all DB checks in a single connection
            async with aiosqlite.connect(DB_PATH) as db:
                # Check blacklist
                async with db.execute(
                    "SELECT blacklisted_until FROM players WHERE player_id = ?",
                    (user.id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        until = datetime.fromisoformat(row[0])
                        if until > datetime.now(timezone.utc):
                            await interaction.followup.send("You are blacklisted from queues.", ephemeral=True)
                            return

                # Check suspension
                async with db.execute(
                    """SELECT suspended_until, reason FROM suspensions
                       WHERE player_id = ? AND (game_id = ? OR game_id IS NULL)
                       AND datetime(suspended_until) > datetime('now')
                       ORDER BY suspended_until DESC LIMIT 1""",
                    (user.id, game_id)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        until = datetime.fromisoformat(row[0]).strftime("%Y-%m-%d %H:%M UTC")
                        reason = row[1] or "No reason provided"
                        await interaction.followup.send(
                            f"You are suspended from this game until {until}.\nReason: {reason}",
                            ephemeral=True
                        )
                        return

                # Check penalty
                async with db.execute(
                    "SELECT penalty_expires FROM ready_penalties WHERE player_id = ?",
                    (user.id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        penalty_expires = datetime.fromisoformat(row[0])
                        if penalty_expires > datetime.now(timezone.utc):
                            until = penalty_expires.strftime("%Y-%m-%d %H:%M UTC")
                            await interaction.followup.send(
                                f"You are penalized for missing a ready check.\nPenalty expires: {until}",
                                ephemeral=True
                            )
                            return

                # Check if in active match
                async with db.execute(
                    """SELECT m.match_id FROM matches m
                       JOIN match_players mp ON m.match_id = mp.match_id
                       WHERE mp.player_id = ? AND m.game_id = ?
                       AND m.winning_team IS NULL AND m.cancelled = 0""",
                    (user.id, game_id)
                ) as cursor:
                    if await cursor.fetchone():
                        await interaction.followup.send(
                            "You're already in an active match for this game.",
                            ephemeral=True
                        )
                        return

                # Clean up orphaned queue entries for this user in a single query
                await db.execute(
                    "DELETE FROM queue_players WHERE player_id = ? AND queue_id != ?",
                    (user.id, queue_id)
                )

                # Add to queue in DB
                await db.execute(
                    "INSERT OR IGNORE INTO queue_players (queue_id, player_id) VALUES (?, ?)",
                    (queue_id, user.id)
                )
                await db.commit()

            # Lock to prevent race condition when multiple players join simultaneously
            async with self.queue_lock:
                # Remove from any other in-memory queues for this game
                for qid, qs in list(self.queues.items()):
                    if qid != queue_id and qs.game_id == game_id and user.id in qs.players:
                        del qs.players[user.id]

                # Add to in-memory queue
                queue_state.players[user.id] = False  # Not ready yet

                # Check if queue is full (must be checked atomically with add)
                queue_full = len(queue_state.players) >= game.player_count

            # Update embed using the message directly (since we deferred)
            embed = await self.create_queue_embed(game, queue_state)
            try:
                msg = await interaction.channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating queue embed: {e}")

            # Start ready check if queue became full
            if queue_full:
                await self.start_ready_check(interaction.channel, game, queue_state)

        except Exception as e:
            logger.error(f"Error in handle_queue_join: {e}", exc_info=True)
            try:
                await interaction.followup.send(
                    "An error occurred while joining the queue. Please try again.",
                    ephemeral=True
                )
            except Exception:
                pass
    
    async def handle_queue_leave(self, interaction: discord.Interaction, game_id: int, queue_id: int):
        """Handle a player leaving the queue."""
        user = interaction.user

        # Quick in-memory checks first
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return

        queue_state = self.queues[queue_id]

        if user.id not in queue_state.players:
            await interaction.response.send_message("You're not in this queue.", ephemeral=True)
            return

        if queue_state.state != "waiting":
            await interaction.response.send_message(
                "Use the Not Ready button during ready check.",
                ephemeral=True
            )
            return

        # Defer before DB operations
        await interaction.response.defer()

        try:
            game = await DatabaseHelper.get_game(game_id)

            # Do DB operation first, then in-memory update
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                    (queue_id, user.id)
                )
                await db.commit()

            # Now update in-memory state (locked to prevent race with join)
            async with self.queue_lock:
                if user.id in queue_state.players:
                    del queue_state.players[user.id]

            # Update embed using direct message edit (since we deferred)
            embed = await self.create_queue_embed(game, queue_state)
            try:
                msg = await interaction.channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating queue embed on leave: {e}")

        except Exception as e:
            logger.error(f"Error in handle_queue_leave: {e}", exc_info=True)
            try:
                await interaction.followup.send("An error occurred. Please try again.", ephemeral=True)
            except Exception:
                pass
    
    async def start_ready_check(self, channel: discord.TextChannel, game: GameConfig, 
                                 queue_state: QueueState):
        """Start the ready check phase."""
        queue_state.state = "ready_check"
        queue_state.ready_check_started = datetime.now(timezone.utc)
        
        # Remove players from other game queues
        for pid in list(queue_state.players.keys()):
            for qid, qs in list(self.queues.items()):
                if qid != queue_state.queue_id and pid in qs.players:
                    del qs.players[pid]
                    # Update that queue's embed
                    other_game = await DatabaseHelper.get_game(qs.game_id)
                    if other_game and qs.message_id:
                        try:
                            other_channel = channel.guild.get_channel(qs.channel_id)
                            if other_channel:
                                msg = await other_channel.fetch_message(qs.message_id)
                                embed = await self.create_queue_embed(other_game, qs)
                                await msg.edit(embed=embed)
                        except Exception:
                            pass
        
        # Update database
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_queues SET state = 'ready_check', ready_check_started = ? WHERE queue_id = ?",
                (queue_state.ready_check_started.isoformat(), queue_state.queue_id)
            )
            await db.commit()
        
        # Edit message with ready check
        embed = await self.create_ready_check_embed(game, queue_state, game.ready_timer_seconds)
        view = ReadyCheckView(self, game.game_id, queue_state.queue_id)
        
        try:
            msg = await channel.fetch_message(queue_state.message_id)
            await msg.edit(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error updating ready check embed: {e}")

        # Ping players
        mentions = " ".join([f"<@{pid}>" for pid in queue_state.players.keys()])
        try:
            ping_msg = await channel.send(f"Ready check! {mentions}")
            await asyncio.sleep(3)
            await ping_msg.delete()
        except Exception as e:
            logger.warning(f"Error with ready check ping: {e}")

        # DM players if enabled (with rate limiting)
        if game.dm_ready_up:
            for pid in queue_state.players.keys():
                member = channel.guild.get_member(pid)
                if member:
                    try:
                        await member.send(
                            f"**{game.name}** queue is ready!\n"
                            f"Click the Ready button in {channel.mention} within {game.ready_timer_seconds} seconds."
                        )
                        await asyncio.sleep(0.5)  # Rate limit DMs
                    except Exception as e:
                        logger.debug(f"Could not DM player {pid} for ready check: {e}")

        # Start timeout task
        task = asyncio.create_task(
            self.ready_check_timeout(channel, game, queue_state)
        )
        self.ready_check_tasks[queue_state.queue_id] = task
    
    async def ready_check_timeout(self, channel: discord.TextChannel, game: GameConfig,
                                   queue_state: QueueState):
        """Handle ready check timeout."""
        timer = game.ready_timer_seconds
        
        while timer > 0:
            await asyncio.sleep(5)
            timer -= 5
            
            # Check if all ready
            if queue_state.state != "ready_check":
                return
            
            if all(queue_state.players.values()):
                # All ready - atomically claim match creation
                if queue_state.state == "ready_check":
                    queue_state.state = "starting_match"
                    await self.proceed_to_match(channel, game, queue_state)
                return
            
            # Update embed with new time
            try:
                msg = await channel.fetch_message(queue_state.message_id)
                embed = await self.create_ready_check_embed(game, queue_state, timer)
                await msg.edit(embed=embed)
            except Exception:
                pass
        
        # Time's up - remove unready players and apply penalties
        unready = [pid for pid, ready in queue_state.players.items() if not ready]

        # Apply penalties to unready players
        penalty_messages = []
        for pid in unready:
            del queue_state.players[pid]
            # Apply penalty
            offense_count, penalty_expires = await DatabaseHelper.add_ready_penalty_offense(pid, game)
            penalty_messages.append(f"<@{pid}> (Offense #{offense_count})")

            # DM the player if DM is enabled
            if game.dm_ready_up:
                member = channel.guild.get_member(pid)
                if member:
                    try:
                        await member.send(
                            f"You missed the ready check for **{game.name}**.\n"
                            f"This is offense #{offense_count}. You are penalized until "
                            f"{penalty_expires.strftime('%Y-%m-%d %H:%M UTC')}."
                        )
                    except Exception as e:
                        logger.debug(f"Could not DM player {pid} about penalty: {e}")

        # Revert to waiting state
        queue_state.state = "waiting"
        for pid in queue_state.players:
            queue_state.players[pid] = False

        embed = await self.create_queue_embed(game, queue_state)
        view = QueueView(self, game.game_id, queue_state.queue_id)

        try:
            msg = await channel.fetch_message(queue_state.message_id)
            await msg.edit(embed=embed, view=view)

            if penalty_messages:
                not_ready_embed = discord.Embed(
                    title="Players Not Ready",
                    description="The following players did not ready up in time:\n" + "\n".join(penalty_messages),
                    color=COLOR_WARNING
                )
                await channel.send(embed=not_ready_embed, delete_after=60)
        except Exception as e:
            logger.error(f"Error updating queue after ready timeout: {e}")
    
    async def handle_ready(self, interaction: discord.Interaction, queue_id: int, is_ready: bool):
        """Handle ready/not ready button."""
        user = interaction.user
        
        if queue_id not in self.queues:
            await interaction.response.send_message("Queue no longer active.", ephemeral=True)
            return
        
        queue_state = self.queues[queue_id]
        
        if user.id not in queue_state.players:
            await interaction.response.send_message("You're not in this queue.", ephemeral=True)
            return
        
        if queue_state.state != "ready_check":
            await interaction.response.send_message("Ready check not active.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(queue_state.game_id)
        if not game:
            await interaction.response.send_message("Game no longer exists.", ephemeral=True)
            return

        if is_ready:
            queue_state.players[user.id] = True

            # Check if all ready - use lock to prevent race condition from concurrent clicks
            async with self.ready_check_lock:
                if all(queue_state.players.values()) and queue_state.state == "ready_check":
                    # Immediately set state to prevent concurrent triggers
                    queue_state.state = "starting_match"

                    # Cancel timeout task
                    if queue_id in self.ready_check_tasks:
                        self.ready_check_tasks[queue_id].cancel()
                        del self.ready_check_tasks[queue_id]

                    await interaction.response.send_message("All players ready! Starting match...", ephemeral=True)
                    try:
                        await self.proceed_to_match(interaction.channel, game, queue_state)
                    except Exception as e:
                        logger.error(f"Error in proceed_to_match: {e}", exc_info=True)
                        # Restore queue state on failure
                        queue_state.state = "ready_check"
                        try:
                            await interaction.followup.send(
                                f"Error starting match: {str(e)[:100]}. Please try again.",
                                ephemeral=True
                            )
                        except Exception:
                            pass
                    return

            # Send ephemeral confirmation and update embed
            ready_count = sum(1 for r in queue_state.players.values() if r)
            total_count = len(queue_state.players)
            await interaction.response.send_message(
                f"You're ready! ({ready_count}/{total_count})",
                ephemeral=True
            )

            # Update embed in background
            elapsed = (datetime.now(timezone.utc) - queue_state.ready_check_started).seconds
            remaining = max(0, game.ready_timer_seconds - elapsed)
            embed = await self.create_ready_check_embed(game, queue_state, remaining)
            try:
                msg = await interaction.channel.fetch_message(queue_state.message_id)
                await msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating ready check embed: {e}")
        else:
            # Not ready - remove from queue, revert to waiting
            try:
                # Update DB first
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "DELETE FROM queue_players WHERE queue_id = ? AND player_id = ?",
                        (queue_id, user.id)
                    )
                    await db.execute(
                        "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                        (queue_id,)
                    )
                    await db.commit()

                # Then update in-memory
                if user.id in queue_state.players:
                    del queue_state.players[user.id]

                # Cancel timeout
                if queue_id in self.ready_check_tasks:
                    self.ready_check_tasks[queue_id].cancel()
                    del self.ready_check_tasks[queue_id]

                queue_state.state = "waiting"
                for pid in queue_state.players:
                    queue_state.players[pid] = False

                embed = await self.create_queue_embed(game, queue_state)
                view = QueueView(self, game.game_id, queue_state.queue_id)
                await interaction.response.edit_message(embed=embed, view=view)

                # Send notification embed about who wasn't ready (auto-deletes after 60s)
                not_ready_embed = discord.Embed(
                    description=f"<@{user.id}> was not ready. Queue reset.",
                    color=COLOR_WARNING
                )
                await interaction.channel.send(embed=not_ready_embed, delete_after=60)
            except Exception as e:
                logger.error(f"Error handling not ready: {e}", exc_info=True)
                await interaction.response.send_message(
                    "An error occurred. Please try again.",
                    ephemeral=True
                )
    
    # -------------------------------------------------------------------------
    # MATCH CREATION
    # -------------------------------------------------------------------------
    
    async def proceed_to_match(self, channel: discord.TextChannel, game: GameConfig,
                                queue_state: QueueState):
        """Proceed from ready check to match creation."""
        # Verify we have the lock on match creation (state must be "starting_match")
        if queue_state.state != "starting_match":
            logger.warning(f"proceed_to_match called but state is {queue_state.state}, aborting")
            return

        # Immediately set to in_match to prevent any other calls
        queue_state.state = "in_match"

        guild = channel.guild
        player_ids = list(queue_state.players.keys())

        # Get category
        category_id = await DatabaseHelper.get_config("category_id")
        category = guild.get_channel(int(category_id)) if category_id else None

        if not category:
            await channel.send("Error: Category not configured. Contact an admin.")
            # Restore queue to waiting state instead of leaving it broken
            await self._restore_queue_to_waiting(channel, game, queue_state)
            return

        # Generate a short ID for this match
        match_short_id = generate_short_id()
        match_id = None
        red_role = None
        blue_role = None

        try:
            # Create match in database
            match_id = await DatabaseHelper.create_match(
                game.game_id,
                game.queue_type.value,
                queue_state.message_id,
                short_id=match_short_id
            )

            # Create team roles using the match short_id (mentionable so players can ping their team)
            red_role = await guild.create_role(name=f"Red {match_short_id}", color=discord.Color.red(), mentionable=True)
            blue_role = await guild.create_role(name=f"Blue {match_short_id}", color=discord.Color.blue(), mentionable=True)

            await DatabaseHelper.update_match(
                match_id,
                red_role_id=red_role.id,
                blue_role_id=blue_role.id
            )

            # Route based on queue type - this creates the match channel
            if game.queue_type == QueueType.CAPTAINS:
                await self.start_captain_draft(guild, category, game, match_id, player_ids, red_role, blue_role)
            else:
                # MMR or Random - balance teams and create match channel
                if game.queue_type == QueueType.MMR:
                    red_team, blue_team = await self.balance_teams_mmr(player_ids, game.game_id)
                else:
                    random.shuffle(player_ids)
                    mid = len(player_ids) // 2
                    red_team = player_ids[:mid]
                    blue_team = player_ids[mid:]

                await self.create_match_channel(guild, category, game, match_id,
                                                 red_team, blue_team, red_role, blue_role)

            # Match creation succeeded - NOW it's safe to clean up the old queue
            # Delete the queue message
            try:
                msg = await channel.fetch_message(queue_state.message_id)
                await msg.delete()
            except Exception:
                pass

            # Remove queue from active
            if queue_state.queue_id in self.queues:
                del self.queues[queue_state.queue_id]
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (queue_state.queue_id,))
                await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (queue_state.queue_id,))
                await db.commit()

            # Start new queue
            await self.start_queue(channel, game)

        except Exception as e:
            logger.error(f"Error creating match: {e}", exc_info=True)

            # Clean up partial match creation
            if match_id:
                await DatabaseHelper.update_match(match_id, cancelled=1)
            if red_role:
                try:
                    await red_role.delete()
                except Exception:
                    pass
            if blue_role:
                try:
                    await blue_role.delete()
                except Exception:
                    pass

            # Notify users and restore queue
            await channel.send(
                f"Error creating match: {str(e)[:100]}. Queue has been restored.",
                delete_after=15
            )
            await self._restore_queue_to_waiting(channel, game, queue_state)

    async def _restore_queue_to_waiting(self, channel: discord.TextChannel, game: GameConfig,
                                         queue_state: QueueState):
        """Restore a queue to waiting state after a failed match creation."""
        queue_state.state = "waiting"
        for pid in queue_state.players:
            queue_state.players[pid] = False

        # Update database
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                (queue_state.queue_id,)
            )
            await db.commit()

        # Update the embed
        try:
            if queue_state.message_id:
                msg = await channel.fetch_message(queue_state.message_id)
                embed = await self.create_queue_embed(game, queue_state)
                view = QueueView(self, game.game_id, queue_state.queue_id)
                await msg.edit(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error restoring queue embed: {e}")
    
    async def balance_teams_mmr(self, player_ids: List[int], game_id: int) -> Tuple[List[int], List[int]]:
        """Balance teams by MMR using snake draft."""
        # Get all player stats
        players_with_mmr = []
        for pid in player_ids:
            stats = await DatabaseHelper.get_player_stats(pid, game_id)
            players_with_mmr.append((pid, stats.effective_mmr))
        
        # Sort by MMR descending
        players_with_mmr.sort(key=lambda x: x[1], reverse=True)
        
        # Snake draft
        red_team = []
        blue_team = []
        
        for i, (pid, mmr) in enumerate(players_with_mmr):
            # Pattern: 0->A, 1->B, 2->B, 3->A, 4->A, 5->B, 6->B, 7->A...
            position_in_pair = i % 4
            if position_in_pair == 0 or position_in_pair == 3:
                red_team.append(pid)
            else:
                blue_team.append(pid)
        
        return red_team, blue_team
    
    async def start_captain_draft(self, guild: discord.Guild, category: discord.CategoryChannel,
                                   game: GameConfig, match_id: int, player_ids: List[int],
                                   red_role: discord.Role, blue_role: discord.Role):
        """Start the captain draft phase."""
        # Create draft channel
        channel_name = await self.get_next_channel_suffix(guild, category, "draft-lobby")
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }
        
        # All players can view
        for pid in player_ids:
            member = guild.get_member(pid)
            if member:
                overwrites[member] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=False
                )
        
        draft_channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
        await DatabaseHelper.update_match(match_id, draft_channel_id=draft_channel.id)
        
        # Select captains based on method
        if game.captain_selection == CaptainSelection.HIGHEST_MMR:
            # Get two highest MMR players
            players_with_mmr = []
            for pid in player_ids:
                stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
                players_with_mmr.append((pid, stats.effective_mmr))
            players_with_mmr.sort(key=lambda x: x[1], reverse=True)
            red_captain = players_with_mmr[0][0]
            blue_captain = players_with_mmr[1][0]
            
            await self.proceed_with_draft(
                guild, draft_channel, category, game, match_id, player_ids,
                red_role, blue_role, red_captain, blue_captain
            )
        
        elif game.captain_selection == CaptainSelection.RANDOM:
            captains = random.sample(player_ids, 2)
            red_captain, blue_captain = captains
            
            await self.proceed_with_draft(
                guild, draft_channel, category, game, match_id, player_ids,
                red_role, blue_role, red_captain, blue_captain
            )
        
        else:  # ADMIN selection
            # Show player list and wait for admin
            embed = discord.Embed(
                title=f"{game.name} - Captain Selection",
                description="An admin must select the captains.",
                color=COLOR_NEUTRAL
            )
            
            player_list = "\n".join([f"• <@{pid}>" for pid in player_ids])
            embed.add_field(name="Available Players", value=player_list, inline=False)
            embed.add_field(
                name="Instructions",
                value="Admin: Use `/cm admin` and select captains, or @ two players here.",
                inline=False
            )
            
            await draft_channel.send(embed=embed)
            
            # Store state for admin selection
            await DatabaseHelper.update_match(match_id, queue_type="captains_awaiting")
    
    async def proceed_with_draft(self, guild: discord.Guild, draft_channel: discord.TextChannel,
                                  category: discord.CategoryChannel, game: GameConfig,
                                  match_id: int, player_ids: List[int],
                                  red_role: discord.Role, blue_role: discord.Role,
                                  red_captain: int, blue_captain: int):
        """Proceed with the captain draft."""
        # Give captains send message permission
        red_member = guild.get_member(red_captain)
        blue_member = guild.get_member(blue_captain)
        
        if red_member:
            await draft_channel.set_permissions(red_member, view_channel=True, send_messages=True)
        if blue_member:
            await draft_channel.set_permissions(blue_member, view_channel=True, send_messages=True)
        
        # Initialize teams
        red_team = [red_captain]
        blue_team = [blue_captain]
        available = [pid for pid in player_ids if pid not in [red_captain, blue_captain]]
        
        # Snake draft order: Red, Blue, Blue, Red, Red, Blue, Blue, Red...
        # Total picks = len(available)
        picks_needed = len(available)
        current_picker = "red"  # Red picks first
        pick_count = {"red": 0, "blue": 0}

        # Fetch IGNs for display (show only IGN when available)
        igns = await DatabaseHelper.get_match_igns(match_id)

        def format_player(pid: int, is_captain: bool = False) -> str:
            """Format player display - show only IGN when available."""
            if pid in igns:
                line = f"`{igns[pid]}`"
            else:
                line = f"<@{pid}>"
            if is_captain:
                line += " (C)"
            return line

        async def update_draft_embed():
            embed = discord.Embed(
                title=f"{game.name} - Captain Draft",
                color=COLOR_NEUTRAL
            )

            red_list = "\n".join([f"• {format_player(pid, pid == red_captain)}"
                                  for pid in red_team])
            blue_list = "\n".join([f"• {format_player(pid, pid == blue_captain)}"
                                   for pid in blue_team])

            embed.add_field(name=f"Red Team ({red_role.mention})", value=red_list or "None", inline=True)
            embed.add_field(name=f"Blue Team ({blue_role.mention})", value=blue_list or "None", inline=True)

            if available:
                avail_list = "\n".join([f"• {format_player(pid)}" for pid in available])
                embed.add_field(name="Available", value=avail_list, inline=False)

                if current_picker == "red":
                    embed.add_field(name="Now Picking", value=f"{format_player(red_captain)} (Red)", inline=False)
                else:
                    embed.add_field(name="Now Picking", value=f"{format_player(blue_captain)} (Blue)", inline=False)
            else:
                embed.add_field(name="Status", value="Draft complete!", inline=False)

            return embed
        
        embed = await update_draft_embed()
        draft_msg = await draft_channel.send(embed=embed)
        
        # Wait for picks
        def check(m):
            if m.channel != draft_channel:
                return False
            if current_picker == "red" and m.author.id != red_captain:
                return False
            if current_picker == "blue" and m.author.id != blue_captain:
                return False
            # Check for mention
            if not m.mentions:
                return False
            return m.mentions[0].id in available
        
        while available:
            try:
                msg = await self.bot.wait_for("message", check=check, timeout=300)
                picked_player = msg.mentions[0].id
                
                if current_picker == "red":
                    red_team.append(picked_player)
                    pick_count["red"] += 1
                else:
                    blue_team.append(picked_player)
                    pick_count["blue"] += 1
                
                available.remove(picked_player)
                await msg.delete()
                
                # Determine next picker (snake draft)
                total_picks = pick_count["red"] + pick_count["blue"]
                # Pattern: R, B, B, R, R, B, B, R...
                # Position 0: R, 1: B, 2: B, 3: R, 4: R, 5: B...
                position_in_cycle = total_picks % 4
                if position_in_cycle in [0, 3]:
                    current_picker = "red"
                else:
                    current_picker = "blue"
                
                embed = await update_draft_embed()
                await draft_msg.edit(embed=embed)
                
            except asyncio.TimeoutError:
                # Auto-pick randomly
                picked_player = random.choice(available)
                if current_picker == "red":
                    red_team.append(picked_player)
                    pick_count["red"] += 1
                else:
                    blue_team.append(picked_player)
                    pick_count["blue"] += 1
                
                available.remove(picked_player)
                
                total_picks = pick_count["red"] + pick_count["blue"]
                position_in_cycle = total_picks % 4
                if position_in_cycle in [0, 3]:
                    current_picker = "red"
                else:
                    current_picker = "blue"
                
                embed = await update_draft_embed()
                await draft_msg.edit(embed=embed)
        
        # Draft complete - add captains flag
        await DatabaseHelper.add_match_player(match_id, red_captain, "red", was_captain=True)
        await DatabaseHelper.add_match_player(match_id, blue_captain, "blue", was_captain=True)
        
        # Create match channel
        await self.create_match_channel(
            guild, category, game, match_id,
            red_team, blue_team, red_role, blue_role,
            draft_channel=draft_channel
        )
    
    async def create_match_channel(self, guild: discord.Guild, category: discord.CategoryChannel,
                                    game: GameConfig, match_id: int,
                                    red_team: List[int], blue_team: List[int],
                                    red_role: discord.Role, blue_role: discord.Role,
                                    draft_channel: discord.TextChannel = None):
        """Create the match channel and assign roles."""
        # Get match short_id for naming
        match_data = await DatabaseHelper.get_match(match_id)
        short_id = match_data.get("short_id", str(match_id)) if match_data else str(match_id)

        # Create channel named "lobby-{short_id}"
        channel_name = f"lobby-{short_id}"

        # Get mod roles and admin role for permissions
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")

        # Channel viewable by everyone but only players, admins, and mods can type
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True),
            red_role: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            blue_role: discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }

        # Add CM admin role permissions
        if admin_role_id:
            admin_role = guild.get_role(int(admin_role_id))
            if admin_role:
                overwrites[admin_role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, manage_messages=True
                )

        # Add mod role permissions
        for mod_role_id in mod_role_ids:
            mod_role = guild.get_role(mod_role_id)
            if mod_role:
                overwrites[mod_role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, manage_messages=True
                )

        match_channel = await category.create_text_channel(name=channel_name, overwrites=overwrites)
        await DatabaseHelper.update_match(match_id, channel_id=match_channel.id)

        # Create team VCs if enabled
        red_vc_id = None
        blue_vc_id = None
        vc_error = None
        if game.vc_creation_enabled:
            try:
                # VC permissions: only team members can connect/speak, others can't even view
                red_vc_overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False),
                    guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    red_role: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    blue_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False)
                }
                blue_vc_overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False),
                    guild.me: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    blue_role: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
                    red_role: discord.PermissionOverwrite(view_channel=False, connect=False, speak=False)
                }

                # Name VCs after the team role (e.g., "Red 72KW9" or "Blue 72KW9")
                red_vc = None
                blue_vc = None
                try:
                    red_vc = await category.create_voice_channel(
                        name=red_role.name,
                        overwrites=red_vc_overwrites
                    )
                    blue_vc = await category.create_voice_channel(
                        name=blue_role.name,
                        overwrites=blue_vc_overwrites
                    )
                    red_vc_id = red_vc.id
                    blue_vc_id = blue_vc.id
                    await DatabaseHelper.update_match(match_id, red_vc_id=red_vc_id, blue_vc_id=blue_vc_id)
                except Exception as e:
                    # Cleanup any partially created VCs
                    if red_vc:
                        try:
                            await red_vc.delete(reason="Cleanup after blue VC creation failed")
                        except Exception:
                            pass
                    raise  # Re-raise to be caught by outer handler
            except Exception as e:
                logger.error(f"Failed to create VCs for match #{match_id}: {e}")
                vc_error = str(e)

        # Assign roles and add to database
        for pid in red_team:
            member = guild.get_member(pid)
            if member:
                await member.add_roles(red_role)
            # Check if already added (captains)
            players = await DatabaseHelper.get_match_players(match_id)
            if not any(p["player_id"] == pid for p in players):
                await DatabaseHelper.add_match_player(match_id, pid, "red")

        for pid in blue_team:
            member = guild.get_member(pid)
            if member:
                await member.add_roles(blue_role)
            players = await DatabaseHelper.get_match_players(match_id)
            if not any(p["player_id"] == pid for p in players):
                await DatabaseHelper.add_match_player(match_id, pid, "blue")

        # Get IGNs for all players
        igns = await DatabaseHelper.get_match_igns(match_id)

        # Build match embed with rivalries and IGNs
        embed = discord.Embed(
            title=f"{game.name} Match {short_id}",
            color=COLOR_NEUTRAL
        )

        # Get captain info
        players = await DatabaseHelper.get_match_players(match_id)
        red_captain = next((p["player_id"] for p in players if p["team"] == "red" and p["was_captain"]), None)
        blue_captain = next((p["player_id"] for p in players if p["team"] == "blue" and p["was_captain"]), None)

        # Build team lists with IGNs (show only IGN when available)
        red_lines = []
        for pid in red_team:
            if pid in igns:
                line = f"`{igns[pid]}`"
            else:
                line = f"<@{pid}>"
            if pid == red_captain:
                line += " (C)"
            red_lines.append(line)

        blue_lines = []
        for pid in blue_team:
            if pid in igns:
                line = f"`{igns[pid]}`"
            else:
                line = f"<@{pid}>"
            if pid == blue_captain:
                line += " (C)"
            blue_lines.append(line)

        embed.add_field(name="Red Team", value="\n".join(red_lines), inline=True)
        embed.add_field(name="Blue Team", value="\n".join(blue_lines), inline=True)

        # Find rivalries
        rivalries = []
        for red_pid in red_team:
            for blue_pid in blue_team:
                rivalry = await DatabaseHelper.get_rivalry(red_pid, blue_pid, game.game_id)
                if rivalry and (rivalry[0] + rivalry[1]) >= RIVALRY_MIN_GAMES:
                    total = rivalry[0] + rivalry[1]
                    red_wins = rivalry[0]
                    win_rate = (red_wins / total) * 100
                    rivalries.append((red_pid, blue_pid, red_wins, rivalry[1], win_rate))

        if rivalries:
            rivalry_lines = []
            for red_pid, blue_pid, r_wins, b_wins, win_rate in rivalries[:3]:
                if win_rate < 50:
                    rivalry_lines.append(
                        f"<@{red_pid}> has a {win_rate:.0f}% win rate vs <@{blue_pid}> ({r_wins}-{b_wins})"
                    )
                else:
                    rivalry_lines.append(
                        f"<@{red_pid}> vs <@{blue_pid}>: {r_wins}-{b_wins}"
                    )
            embed.add_field(name="Storylines", value="\n".join(rivalry_lines), inline=False)

        # Add VC info if created, or error message if VC creation failed
        if red_vc_id and blue_vc_id:
            embed.add_field(
                name="Voice Channels",
                value=f"Red: <#{red_vc_id}>\nBlue: <#{blue_vc_id}>",
                inline=False
            )
        elif vc_error:
            embed.add_field(
                name="Voice Channels",
                value=f"Failed to create VCs: {vc_error[:50]}...",
                inline=False
            )

        embed.add_field(
            name="Report Winner",
            value="Use `/cm win` when the match is over, or `/cm abandon` to cancel.",
            inline=False
        )

        # Add banner image if configured
        if game.banner_url:
            embed.set_image(url=game.banner_url)

        await match_channel.send(f"{red_role.mention} vs {blue_role.mention}", embed=embed)

        # Send MMR embed to log channel (admin view)
        await self._send_mmr_embed_to_log(guild, game, match_id, red_team, blue_team, igns, red_role, blue_role)

        # Start map vote if mapvote cog is loaded and game is configured
        mapvote_cog = self.bot.get_cog("mapvote")
        if mapvote_cog:
            try:
                # Check if game is configured for map voting
                game_configured = False
                async with mapvote_cog.config_lock:
                    games_cfg = mapvote_cog._get_games_config_sync()
                    game_configured = game.name in games_cfg

                if game_configured:
                    all_players = red_team + blue_team
                    await mapvote_cog.start_programmatic_vote(
                        guild_id=guild.id,
                        channel=match_channel,
                        game_name=game.name,
                        duration=3,  # 3 minute time limit
                        min_users=1,  # At least 1 vote needed
                        max_votes=len(all_players),  # Ends when all players vote
                        allowed_voters=all_players,  # Only match players can vote
                        red_role_id=red_role.id,
                        blue_role_id=blue_role.id,
                        match_id=match_id  # Store selected map in match record
                    )
            except Exception as e:
                logger.error(f"Error starting map vote for match {match_id}: {e}")

        # Delete draft channel if exists
        if draft_channel:
            await draft_channel.delete()

        # Start 3-hour timeout
        task = asyncio.create_task(self.match_timeout(guild, match_id, match_channel))
        self.match_timeout_tasks[match_id] = task

        # Log
        await self.log_action(guild, f"Match {short_id} started in {match_channel.mention}")
    
    async def match_timeout(self, guild: discord.Guild, match_id: int, channel: discord.TextChannel,
                            delay_seconds: int = 3 * 60 * 60):
        """Handle match timeout after delay_seconds (default 3 hours)."""
        await asyncio.sleep(delay_seconds)
        
        # Check if match still active
        match = await DatabaseHelper.get_match(match_id)
        if match and not match["winning_team"] and not match["cancelled"]:
            admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
            admin_mention = f"<@&{admin_role_id}>" if admin_role_id else "Admins"
            
            await channel.send(
                f"{admin_mention} - This match has been ongoing for 3 hours without a winner. "
                "Please use `/cm admin` to force a winner or cancel the match."
            )
    
    # -------------------------------------------------------------------------
    # WIN VOTING & FINALIZATION
    # -------------------------------------------------------------------------
    
    async def handle_win_vote(self, interaction: discord.Interaction, match_id: int, team: Team):
        """Handle a win vote."""
        user = interaction.user
        
        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match_id)
        if not any(p["player_id"] == user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return
        
        # Record vote
        await DatabaseHelper.add_win_vote(match_id, user.id, team.value)
        
        # Get vote counts
        votes = await DatabaseHelper.get_win_votes(match_id)
        red_votes = votes.get("red", 0)
        blue_votes = votes.get("blue", 0)
        
        # Check for majority
        match = await DatabaseHelper.get_match(match_id)
        game = await DatabaseHelper.get_game(match["game_id"])
        needed = (game.player_count // 2) + 1
        
        if red_votes >= needed:
            await interaction.response.defer()
            await self.finalize_match(interaction.guild, match_id, Team.RED)
        elif blue_votes >= needed:
            await interaction.response.defer()
            await self.finalize_match(interaction.guild, match_id, Team.BLUE)
        else:
            # Get IGNs and player lists to keep team names visible
            igns = await DatabaseHelper.get_match_igns(match_id)
            red_players = [p for p in players if p["team"] == "red"]
            blue_players = [p for p in players if p["team"] == "blue"]

            def format_player(p: dict) -> str:
                pid = p["player_id"]
                if pid in igns:
                    return f"`{igns[pid]}`"
                return f"<@{pid}>"

            red_lines = [format_player(p) for p in red_players]
            blue_lines = [format_player(p) for p in blue_players]

            # Update embed with team names still visible
            embed = discord.Embed(
                title="Who Won?",
                description=f"Cast your vote! ({needed} votes needed)",
                color=COLOR_NEUTRAL
            )
            embed.add_field(
                name=f"Red Team ({red_votes} votes)",
                value="\n".join(red_lines) or "None",
                inline=True
            )
            embed.add_field(
                name=f"Blue Team ({blue_votes} votes)",
                value="\n".join(blue_lines) or "None",
                inline=True
            )

            await interaction.response.edit_message(embed=embed)

    async def handle_abandon_vote(self, interaction: discord.Interaction, match_id: int, needed_votes: int):
        """Handle an abandon vote."""
        user = interaction.user

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match_id)
        if not any(p["player_id"] == user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        # Check if already voted
        if await DatabaseHelper.has_voted_abandon(match_id, user.id):
            await interaction.response.send_message("You've already voted to abandon.", ephemeral=True)
            return

        # Record vote
        await DatabaseHelper.add_abandon_vote(match_id, user.id)

        # Get vote count
        current_votes = await DatabaseHelper.get_abandon_votes(match_id)

        # Check for majority
        if current_votes >= needed_votes:
            await interaction.response.defer()
            await self.cancel_match(
                interaction.guild, match_id,
                reason=f"Vote to abandon ({current_votes}/{needed_votes} votes)"
            )
            await interaction.followup.send("Match abandoned by vote.", ephemeral=False)
        else:
            # Update embed
            embed = discord.Embed(
                title="Abandon Match?",
                description=f"Votes: {current_votes}/{needed_votes}",
                color=COLOR_WARNING
            )
            embed.add_field(
                name="Warning",
                value="If the match is abandoned, no stats will be recorded.",
                inline=False
            )
            await interaction.response.edit_message(embed=embed)

    async def finalize_match(self, guild: discord.Guild, match_id: int, winning_team: Team):
        """Finalize a match and update stats."""
        # Get or create lock for this match to prevent race conditions
        if match_id not in self.match_finalize_locks:
            self.match_finalize_locks[match_id] = asyncio.Lock()

        async with self.match_finalize_locks[match_id]:
            match = await DatabaseHelper.get_match(match_id)
            if not match or match["winning_team"]:
                return

            # Mark match as being finalized immediately to prevent any other path
            await DatabaseHelper.update_match(match_id, winning_team=winning_team.value)
        
        game = await DatabaseHelper.get_game(match["game_id"])
        players = await DatabaseHelper.get_match_players(match_id)
        
        # Determine winners and losers
        winners = [p["player_id"] for p in players if p["team"] == winning_team.value]
        losers = [p["player_id"] for p in players if p["team"] != winning_team.value]
        
        # Calculate average MMR for each team
        winner_mmr = []
        loser_mmr = []
        
        for pid in winners:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            winner_mmr.append(stats.effective_mmr)
        
        for pid in losers:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            loser_mmr.append(stats.effective_mmr)
        
        avg_winner_mmr = sum(winner_mmr) / len(winner_mmr) if winner_mmr else 1000
        avg_loser_mmr = sum(loser_mmr) / len(loser_mmr) if loser_mmr else 1000
        
        # Calculate expected scores (ELO formula)
        expected_winner = 1 / (1 + 10 ** ((avg_loser_mmr - avg_winner_mmr) / 400))
        expected_loser = 1 - expected_winner
        
        # Update winner stats
        for pid in winners:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            k = stats.get_k_factor()
            mmr_change = int(k * (1 - expected_winner))
            
            old_mmr = stats.mmr
            stats.mmr += mmr_change
            stats.wins += 1
            stats.games_played += 1
            stats.last_played = datetime.now(timezone.utc)
            
            await DatabaseHelper.update_player_stats(stats)
            await DatabaseHelper.record_mmr_change(pid, game.game_id, match_id, old_mmr, stats.mmr)
        
        # Update loser stats
        for pid in losers:
            stats = await DatabaseHelper.get_player_stats(pid, game.game_id)
            k = stats.get_k_factor()
            mmr_change = int(k * (0 - expected_loser))
            
            old_mmr = stats.mmr
            stats.mmr += mmr_change  # This will be negative
            stats.losses += 1
            stats.games_played += 1
            stats.last_played = datetime.now(timezone.utc)
            
            await DatabaseHelper.update_player_stats(stats)
            await DatabaseHelper.record_mmr_change(pid, game.game_id, match_id, old_mmr, stats.mmr)
        
        # Update rivalries
        for winner_id in winners:
            for loser_id in losers:
                await DatabaseHelper.update_rivalry(winner_id, loser_id, game.game_id)
        
        # Mark match complete (winning_team already set at start to prevent race conditions)
        await DatabaseHelper.update_match(
            match_id,
            decided_at=datetime.now(timezone.utc).isoformat()
        )

        # Cancel timeout task
        if match_id in self.match_timeout_tasks:
            self.match_timeout_tasks[match_id].cancel()
            del self.match_timeout_tasks[match_id]

        # Schedule Valorant stats fetch (runs in background with retries at 1, 5, 10, 15 min)
        if 'valorant' in game.name.lower():
            match_end_time = datetime.now(timezone.utc)
            player_ids = [p['player_id'] for p in players]
            self.valorant_stats_tasks[match_id] = asyncio.create_task(
                self.fetch_valorant_match_stats_with_retry(
                    match_id, game.game_id, player_ids, match_end_time, guild
                )
            )

        # Send match history embed if configured
        if game.match_history_channel_id:
            history_channel = guild.get_channel(game.match_history_channel_id)
            if history_channel:
                await self.send_match_history_embed(history_channel, game, match_id, players, winning_team)

        # Send winner/loser embed to game channel if configured
        if game.game_channel_id:
            game_channel = guild.get_channel(game.game_channel_id)
            if game_channel:
                await self._send_winner_loser_embed(game_channel, game, match_id, players, winning_team)

        # Update persistent leaderboard
        if game.leaderboard_channel_id:
            try:
                await self._update_persistent_leaderboard(guild, game)
            except Exception as e:
                logger.error(f"Error updating persistent leaderboard: {e}")

        # Clean up
        await self.cleanup_match(guild, match)

        # Log
        short_id = match.get("short_id") or str(match_id)
        await self.log_action(
            guild,
            f"Match {short_id} ({game.name}): {winning_team.value.capitalize()} team wins!"
        )

    async def fetch_valorant_match_stats_with_retry(
        self, match_id: int, game_id: int, player_ids: List[int], match_end_time: datetime,
        guild: discord.Guild = None
    ):
        """Fetch Valorant stats with retries at 1, 5, 10, 15 minutes from match end."""
        # Delays between attempts: wait 1 min, then 4 more, then 5 more, then 5 more
        # Results in attempts at: 1 min, 5 min, 10 min, 15 min from match end
        retry_delays = [60, 240, 300, 300]
        short_id = await self._get_match_short_id(match_id)

        try:
            for attempt, delay in enumerate(retry_delays, 1):
                await asyncio.sleep(delay)

                logger.info(f"Match #{match_id}: Valorant stats fetch attempt {attempt}/4 (~{sum(retry_delays[:attempt])//60} min after match)")

                success = await self.fetch_valorant_match_stats(
                    match_id, game_id, player_ids, match_end_time
                )

                if success:
                    logger.info(f"Match #{match_id}: Valorant stats fetched successfully on attempt {attempt}")
                    stats = await DatabaseHelper.get_valorant_match_stats(match_id)
                    if guild:
                        await self.log_action(
                            guild,
                            f"Match {short_id}: Valorant stats fetched ({len(stats)} players, attempt {attempt}/4)"
                        )
                    return

                if attempt < len(retry_delays):
                    logger.info(f"Match #{match_id}: Stats not available yet, will retry...")

            logger.warning(f"Match #{match_id}: Failed to fetch Valorant stats after {len(retry_delays)} attempts")
            if guild:
                await self.log_action(
                    guild,
                    f"Match {short_id}: Failed to fetch Valorant stats after {len(retry_delays)} attempts"
                )

        except asyncio.CancelledError:
            logger.info(f"Match #{match_id}: Valorant stats fetch task cancelled")
        except Exception as e:
            logger.error(f"Match #{match_id}: Error in stats fetch retry loop: {e}")
            if guild:
                await self.log_action(guild, f"Match {short_id}: Error fetching Valorant stats: {e}")
        finally:
            # Clean up task tracker
            self.valorant_stats_tasks.pop(match_id, None)

    async def fetch_valorant_match_stats(
        self, match_id: int, game_id: int, player_ids: List[int], match_end_time: datetime
    ) -> bool:
        """Fetch Valorant stats from HenrikDev API for a completed match. Returns True if successful."""
        try:
            igns = await DatabaseHelper.get_match_igns(match_id)
            if not igns:
                logger.info(f"Match #{match_id}: No IGNs linked for any players, skipping stats fetch")
                return False

            logger.info(f"Match #{match_id}: Found {len(igns)} linked IGNs: {list(igns.values())}")

            # Get list of regulars to prioritize
            regulars = await DatabaseHelper.get_valorant_regulars(game_id)
            regular_pids = {r['player_id'] for r in regulars}

            # Sort players - regulars first
            sorted_player_ids = sorted(player_ids, key=lambda pid: pid not in regular_pids)

            valorant_match_data = None

            # Try to find the match using any player's history
            for pid in sorted_player_ids:
                if pid not in igns:
                    continue

                player_ign = igns[pid]
                if '#' not in player_ign:
                    logger.info(f"Match #{match_id}: Skipping IGN '{player_ign}' (no # separator)")
                    continue

                logger.info(f"Match #{match_id}: Searching match history for '{player_ign}'")
                match_data = await self.henrik_api.find_and_fetch_match_stats(
                    player_ign, match_id, game_id, match_end_time
                )

                if match_data:
                    valorant_match_data = match_data
                    # Mark this player as a reliable regular
                    await DatabaseHelper.mark_valorant_regular(pid, game_id, player_ign)
                    logger.info(f"Match #{match_id}: Found match via '{player_ign}'")
                    break
                else:
                    logger.info(f"Match #{match_id}: No match found via '{player_ign}'")

            if not valorant_match_data:
                logger.info(f"Match #{match_id}: Could not find Valorant match data from any player")
                return False

            # Extract stats for all players in the match
            details = valorant_match_data['details']
            valorant_match_id = valorant_match_data['valorant_match_id']
            map_name = valorant_match_data.get('map')

            # Build a lookup of Valorant player data by normalized IGN
            # v2 API structure: details.players.all_players[] or details.players.red/blue[]
            val_players = {}
            players_data = details.get('players', {})

            # Check for all_players (v2 format)
            if isinstance(players_data, dict) and 'all_players' in players_data:
                for vp in players_data['all_players']:
                    if isinstance(vp, dict):
                        vp_name = vp.get('name', '')
                        vp_tag = vp.get('tag', '')
                        if vp_name and vp_tag:
                            key = f"{vp_name}#{vp_tag}".lower()
                            val_players[key] = vp
            # Check for team-based structure (red/blue)
            elif isinstance(players_data, dict):
                for team_name in ['red', 'blue', 'Red', 'Blue']:
                    if team_name in players_data:
                        for vp in players_data[team_name]:
                            if isinstance(vp, dict):
                                vp_name = vp.get('name', '')
                                vp_tag = vp.get('tag', '')
                                if vp_name and vp_tag:
                                    key = f"{vp_name}#{vp_tag}".lower()
                                    val_players[key] = vp
            # Check for list format (v4 format)
            elif isinstance(players_data, list):
                for team_data in players_data:
                    for vp in (team_data if isinstance(team_data, list) else [team_data]):
                        if isinstance(vp, dict):
                            vp_name = vp.get('name', '')
                            vp_tag = vp.get('tag', '')
                            if vp_name and vp_tag:
                                key = f"{vp_name}#{vp_tag}".lower()
                                val_players[key] = vp

            # Parse round data for plants, defuses, multi-kills, and economy
            rounds_data = details.get('rounds', [])

            # Build per-player round stats: {ign_key: {plants, defuses, kills_per_round[], econ_spent, econ_loadout}}
            player_round_stats = {}
            for ign_key in val_players:
                player_round_stats[ign_key] = {
                    'plants': 0,
                    'defuses': 0,
                    'kills_per_round': [],
                    'econ_spent': 0,
                    'econ_loadout': 0
                }

            for rnd in rounds_data:
                # Track kills per round for multi-kill calculation
                round_kills = {}  # ign_key -> kills this round

                # Get plant/defuse events
                plant_events = rnd.get('plant_events', {})
                defuse_events = rnd.get('defuse_events', {})

                # Plant - v2 format has planted_by with display_name already in "Name#Tag" format
                planted_by = plant_events.get('planted_by', {})
                if planted_by:
                    planter_display = planted_by.get('display_name', '')
                    if planter_display and '#' in planter_display:
                        planter_key = planter_display.lower()
                        if planter_key in player_round_stats:
                            player_round_stats[planter_key]['plants'] += 1

                # Defuse - same format
                defused_by = defuse_events.get('defused_by', {})
                if defused_by:
                    defuser_display = defused_by.get('display_name', '')
                    if defuser_display and '#' in defuser_display:
                        defuser_key = defuser_display.lower()
                        if defuser_key in player_round_stats:
                            player_round_stats[defuser_key]['defuses'] += 1

                # Player stats per round (for economy and kills)
                player_stats_list = rnd.get('player_stats', [])
                for ps in player_stats_list:
                    # player_display_name is already in "Name#Tag" format
                    ps_display = ps.get('player_display_name', '')
                    if ps_display and '#' in ps_display:
                        ps_key = ps_display.lower()
                        if ps_key in player_round_stats:
                            # Economy
                            economy = ps.get('economy', {})
                            spent = economy.get('spent', 0)
                            # Handle if spent is a dict with 'overall' key or just an int
                            if isinstance(spent, dict):
                                spent = spent.get('overall', 0) or 0
                            player_round_stats[ps_key]['econ_spent'] += spent or 0
                            player_round_stats[ps_key]['econ_loadout'] += economy.get('loadout_value', 0) or 0

                            # Count kills this round (use 'kills' field if available, otherwise count kill_events)
                            kills_this_round = ps.get('kills', 0) or len(ps.get('kill_events', []))
                            round_kills[ps_key] = kills_this_round

                # Record kills per round for multi-kill calculation
                for ign_key, kills in round_kills.items():
                    if ign_key in player_round_stats:
                        player_round_stats[ign_key]['kills_per_round'].append(kills)

            # Calculate multi-kills from kills_per_round
            def count_multikills(kills_per_round):
                c2k = c3k = c4k = c5k = 0
                for k in kills_per_round:
                    if k == 2:
                        c2k += 1
                    elif k == 3:
                        c3k += 1
                    elif k == 4:
                        c4k += 1
                    elif k >= 5:
                        c5k += 1
                return c2k, c3k, c4k, c5k

            # Save stats for each player in our match
            stats_saved = 0
            for pid in player_ids:
                if pid not in igns:
                    continue

                player_ign = igns[pid]
                ign_key = player_ign.lower()

                if ign_key not in val_players:
                    continue

                vp = val_players[ign_key]
                stats = vp.get('stats', {})

                # Agent name - v2 uses 'character', v4 uses 'agent'
                agent = vp.get('character') or (
                    vp.get('agent', {}).get('name') if isinstance(vp.get('agent'), dict) else vp.get('agent')
                )

                # Damage - v2 has damage_made at root, v4 has in stats
                damage = vp.get('damage_made', 0) or stats.get('damage', 0) or stats.get('damage_made', 0) or 0

                # First bloods - may be in different locations
                first_bloods = vp.get('first_bloods', 0) or vp.get('first_blood', 0) or 0

                # Get round-derived stats
                round_stats = player_round_stats.get(ign_key, {})
                plants = round_stats.get('plants', 0)
                defuses = round_stats.get('defuses', 0)
                econ_spent = round_stats.get('econ_spent', 0)
                econ_loadout = round_stats.get('econ_loadout', 0)
                c2k, c3k, c4k, c5k = count_multikills(round_stats.get('kills_per_round', []))

                await DatabaseHelper.save_valorant_match_stats(
                    match_id=match_id,
                    valorant_match_id=valorant_match_id,
                    player_id=pid,
                    ign=player_ign,
                    agent=agent,
                    kills=stats.get('kills', 0),
                    deaths=stats.get('deaths', 0),
                    assists=stats.get('assists', 0),
                    headshots=stats.get('headshots', 0),
                    bodyshots=stats.get('bodyshots', 0),
                    legshots=stats.get('legshots', 0),
                    score=stats.get('score', 0),
                    map_name=map_name,
                    damage_dealt=damage,
                    first_bloods=first_bloods,
                    plants=plants,
                    defuses=defuses,
                    c2k=c2k,
                    c3k=c3k,
                    c4k=c4k,
                    c5k=c5k,
                    econ_spent=econ_spent,
                    econ_loadout=econ_loadout
                )
                stats_saved += 1

            logger.info(f"Saved Valorant stats for {stats_saved} players in match #{match_id}")
            return True

        except Exception as e:
            logger.error(f"Error fetching Valorant stats for match #{match_id}: {e}")
            return False

    async def cancel_match(self, guild: discord.Guild, match_id: int, reason: str = "Admin action",
                           cancelled_by: Optional[int] = None):
        """Cancel a match without updating stats."""
        match = await DatabaseHelper.get_match(match_id)
        if not match:
            return

        await DatabaseHelper.update_match(match_id, cancelled=1)

        # Cancel timeout
        if match_id in self.match_timeout_tasks:
            self.match_timeout_tasks[match_id].cancel()
            del self.match_timeout_tasks[match_id]

        await self.cleanup_match(guild, match)

        game = await DatabaseHelper.get_game(match["game_id"])
        short_id = match.get("short_id") or str(match_id)

        # Build detailed log message
        log_msg = f"Match {short_id} cancelled. Reason: {reason}"
        if cancelled_by:
            log_msg += f" (by <@{cancelled_by}>)"
        await self.log_action(guild, log_msg)
    
    async def cleanup_match(self, guild: discord.Guild, match: dict):
        """Clean up match channels, VCs, and roles."""
        # Delete match channel
        if match["channel_id"]:
            channel = guild.get_channel(match["channel_id"])
            if channel:
                await channel.delete()

        # Delete draft channel
        if match["draft_channel_id"]:
            channel = guild.get_channel(match["draft_channel_id"])
            if channel:
                await channel.delete()

        # Delete team VCs if they exist
        if match.get("red_vc_id"):
            vc = guild.get_channel(match["red_vc_id"])
            if vc:
                await vc.delete()

        if match.get("blue_vc_id"):
            vc = guild.get_channel(match["blue_vc_id"])
            if vc:
                await vc.delete()

        # Delete roles
        if match["red_role_id"]:
            role = guild.get_role(match["red_role_id"])
            if role:
                await role.delete()

        if match["blue_role_id"]:
            role = guild.get_role(match["blue_role_id"])
            if role:
                await role.delete()

        # Delete queue message
        if match["queue_message_id"]:
            game = await DatabaseHelper.get_game(match["game_id"])
            if game and game.queue_channel_id:
                channel = guild.get_channel(game.queue_channel_id)
                if channel:
                    try:
                        msg = await channel.fetch_message(match["queue_message_id"])
                        await msg.delete()
                    except Exception:
                        pass

    async def send_match_history_embed(self, channel: discord.TextChannel, game: GameConfig,
                                       match_id: int, players: List[dict], winning_team: Team):
        """Send match history embed to configured channel."""
        igns = await DatabaseHelper.get_match_igns(match_id)

        # Get Valorant stats if available
        val_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
        val_stats_by_player = {s['player_id']: s for s in val_stats}

        # Get team members
        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        def format_player_line(p: dict) -> str:
            """Format a player line with IGN and optional Valorant stats."""
            pid = p["player_id"]
            if pid in igns:
                line = f"`{igns[pid]}`"
            else:
                line = f"<@{pid}>"
            if p.get("was_captain"):
                line += " (C)"

            # Add Valorant stats if available
            if pid in val_stats_by_player:
                vs = val_stats_by_player[pid]
                kda = f"{vs['kills']}/{vs['deaths']}/{vs['assists']}"
                total_shots = vs['headshots'] + vs['bodyshots'] + vs['legshots']
                hs_pct = round(vs['headshots'] / total_shots * 100) if total_shots > 0 else 0
                agent = vs.get('agent', '')
                if agent:
                    line += f" - {agent}"
                line += f" | {kda} ({hs_pct}% HS)"

            return line

        # Build team lists with IGNs and stats
        red_lines = [format_player_line(p) for p in red_players]
        blue_lines = [format_player_line(p) for p in blue_players]

        # Get map name from match record first (from map vote), fallback to Valorant stats
        match = await DatabaseHelper.get_match(match_id)
        map_name = match.get('map_name') if match else None
        if not map_name:
            map_name = val_stats[0].get('map_name') if val_stats and val_stats[0].get('map_name') else None
        short_id = await self._get_match_short_id(match_id)

        # Build title with map name if available
        if map_name:
            title = f"{map_name} - {game.name}"
        else:
            title = f"Match {short_id} - {game.name}"

        # Create embed
        winner_color = COLOR_RED if winning_team == Team.RED else COLOR_BLUE
        embed = discord.Embed(
            title=title,
            color=winner_color
        )

        # Set map thumbnail if available
        if map_name:
            image_url = self._get_map_image_url(game.name, map_name)
            if image_url:
                embed.set_thumbnail(url=image_url)

        # Add winner indicator
        red_header = "Red Team" + (" ★ WINNER" if winning_team == Team.RED else "")
        blue_header = "Blue Team" + (" ★ WINNER" if winning_team == Team.BLUE else "")

        embed.add_field(name=red_header, value="\n".join(red_lines) or "None", inline=True)
        embed.add_field(name=blue_header, value="\n".join(blue_lines) or "None", inline=True)

        embed.set_footer(text=f"ID: {short_id}")

        await channel.send(embed=embed)

    async def _send_winner_loser_embed(
        self, channel: discord.TextChannel, game: GameConfig,
        match_id: int, players: List[dict], winning_team: Team
    ):
        """Send match result embed to configured game channel."""
        igns = await DatabaseHelper.get_match_igns(match_id)

        # Separate by team
        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        def format_player(p: dict) -> str:
            pid = p["player_id"]
            if pid in igns:
                return f"`{igns[pid]}`"
            return f"<@{pid}>"

        red_lines = [format_player(p) for p in red_players]
        blue_lines = [format_player(p) for p in blue_players]

        # Determine winner/loser
        if winning_team == Team.RED:
            winner_name = "Red Team"
            winner_players = "\n".join(red_lines) or "None"
            loser_name = "Blue Team"
            loser_players = "\n".join(blue_lines) or "None"
        else:
            winner_name = "Blue Team"
            winner_players = "\n".join(blue_lines) or "None"
            loser_name = "Red Team"
            loser_players = "\n".join(red_lines) or "None"

        # Get map name from match record first (from map vote), fallback to Valorant stats
        match = await DatabaseHelper.get_match(match_id)
        map_name = match.get('map_name') if match else None

        # Fallback to Valorant stats if no map name in match record
        if not map_name:
            val_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
            map_name = val_stats[0].get('map_name') if val_stats and val_stats[0].get('map_name') else None

        # Build title with map name if available
        if map_name:
            title = f"{map_name} - Results"
        else:
            short_id = await self._get_match_short_id(match_id)
            title = f"Match {short_id} - Results"

        embed = discord.Embed(
            title=title,
            color=COLOR_SUCCESS
        )

        # Set map thumbnail if available
        if map_name:
            image_url = self._get_map_image_url(game.name, map_name)
            if image_url:
                embed.set_thumbnail(url=image_url)

        embed.add_field(
            name=f"WINNER - {winner_name}",
            value=winner_players,
            inline=True
        )
        embed.add_field(
            name=f"LOSER - {loser_name}",
            value=loser_players,
            inline=True
        )

        await channel.send(embed=embed)

    # -------------------------------------------------------------------------
    # LEADERBOARD & STATS
    # -------------------------------------------------------------------------

    async def _gather_stats_data(
        self, member: discord.Member, game: GameConfig, monthly: bool,
        guild: discord.Guild = None
    ) -> dict:
        """Gather stats data for image generation."""
        stats = await DatabaseHelper.get_player_stats(member.id, game.game_id)
        valorant_stats = await DatabaseHelper.get_valorant_player_stats(
            member.id, game.game_id, monthly
        )
        recent_matches = await DatabaseHelper.get_player_recent_matches(
            member.id, game.game_id, limit=5
        )
        streak_stats = await DatabaseHelper.get_player_streak_stats(
            member.id, game.game_id, monthly
        )
        teammate_stats = await DatabaseHelper.get_all_teammate_stats(
            member.id, game.game_id, monthly
        )

        now = datetime.now(timezone.utc)
        if monthly:
            period_title = f"{now.strftime('%B')}"
        else:
            period_title = "Lifetime Stats"

        # Get leaderboard rank
        leaderboard_rank = await DatabaseHelper.get_player_leaderboard_rank(
            member.id, game.game_id, monthly=monthly
        )

        # Build recent matches list
        formatted_matches = []
        for match in recent_matches:
            won = match.get('team') == match.get('winning_team')
            formatted_matches.append({
                'won': won,
                'kills': match.get('kills'),
                'deaths': match.get('deaths'),
                'assists': match.get('assists'),
                'agent': match.get('agent'),
                'map_name': match.get('map_name') or match.get('match_map_name')
            })

        # Resolve teammate names
        def get_teammate_name(player_id: int) -> str:
            if guild:
                m = guild.get_member(player_id)
                if m:
                    name = m.display_name
                    return name[:12] + '...' if len(name) > 15 else name
            return f"User {player_id}"

        best_teammates = []
        for t in teammate_stats.get('best_teammates', []):
            best_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        worst_teammates = []
        for t in teammate_stats.get('worst_teammates', []):
            worst_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        return {
            'player_name': member.display_name,
            'avatar_url': member.display_avatar.url,
            'period_title': period_title,
            'leaderboard_rank': leaderboard_rank,
            'games_played': stats.games_played,
            'wins': stats.wins,
            'losses': stats.losses,
            'total_kills': valorant_stats.get('total_kills', 0),
            'total_deaths': valorant_stats.get('total_deaths', 0),
            'total_assists': valorant_stats.get('total_assists', 0),
            'total_score': valorant_stats.get('total_score', 0),
            'total_damage': valorant_stats.get('total_damage', 0),
            'total_first_bloods': valorant_stats.get('total_first_bloods', 0),
            'hs_percent': valorant_stats.get('hs_percent', 0),
            'longest_win_streak': streak_stats.get('longest_win_streak', 0),
            'longest_loss_streak': streak_stats.get('longest_loss_streak', 0),
            'recent_matches': formatted_matches,
            'agent_stats': valorant_stats.get('agent_stats', {}),
            'map_stats': valorant_stats.get('map_stats', []),
            'best_teammates': best_teammates,
            'worst_teammates': worst_teammates
        }

    async def _build_leaderboard_text_embed(self, guild: discord.Guild, game_id: int,
                                             monthly: bool = True) -> discord.Embed:
        """Build a text-based leaderboard embed (top 20)."""
        game = await DatabaseHelper.get_game(game_id)
        leaderboard = await DatabaseHelper.get_leaderboard(game_id, monthly=monthly, limit=20)
        now = datetime.now(timezone.utc)

        if monthly:
            title = f"{game.name} Leaderboard — {now.strftime('%B')}"
        else:
            title = f"{game.name} Leaderboard — All-time"

        embed = discord.Embed(title=title, color=COLOR_NEUTRAL)

        if not leaderboard:
            embed.description = "No matches played yet."
        else:
            lines = []
            for i, entry in enumerate(leaderboard, 1):
                player_id = entry["player_id"]
                member = guild.get_member(player_id)
                name = member.display_name if member else str(player_id)
                wins = entry["wins"]
                losses = entry["losses"]
                total = wins + losses
                winrate = round((wins / total * 100)) if total > 0 else 0
                lines.append(f"**{i}) {name}**\n> -# {wins}W - {losses}L {winrate}% W/L")
            embed.description = "\n".join(lines)

        return embed

    async def _update_persistent_leaderboard(self, guild: discord.Guild, game: 'GameConfig'):
        """Edit the persistent leaderboard message with fresh data. Self-healing if deleted."""
        channel = guild.get_channel(game.leaderboard_channel_id)
        if not channel:
            return

        embed = await self._build_leaderboard_text_embed(guild, game.game_id, monthly=True)
        is_valorant = 'valorant' in game.name.lower()
        view = PersistentLeaderboardView(self, game.game_id, is_valorant=is_valorant)

        msg = None
        if game.leaderboard_message_id:
            try:
                msg = await channel.fetch_message(game.leaderboard_message_id)
                await msg.edit(embed=embed, view=view)
                return
            except discord.NotFound:
                pass  # Message was deleted, send a new one

        # Self-heal: send new message and save its ID
        try:
            msg = await channel.send(embed=embed, view=view)
            await DatabaseHelper.update_game(game.game_id, leaderboard_message_id=msg.id)
            self.bot.add_view(view, message_id=msg.id)
            # Update the in-memory game config too
            game.leaderboard_message_id = msg.id
        except discord.Forbidden:
            logger.warning(f"No permission to send leaderboard in channel {channel.id}")

    async def _generate_match_scoreboard(self, guild: discord.Guild, match_id: int) -> Tuple[discord.Embed, Optional[discord.File]]:
        """Generate a scoreboard image for a match. Returns (embed, file_or_none)."""
        match = await DatabaseHelper.get_match(match_id)
        if not match:
            return discord.Embed(description="Match not found.", color=COLOR_NEUTRAL), None

        val_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
        if not val_stats:
            return discord.Embed(description="No stats available for this match.", color=COLOR_NEUTRAL), None

        players = await DatabaseHelper.get_match_players(match_id)
        player_teams = {p['player_id']: p['team'] for p in players}

        winning_team = match.get('winning_team')
        map_name = match.get('map_name') or (val_stats[0].get('map_name') if val_stats else 'Unknown')

        winners = []
        losers = []

        for stat in val_stats:
            player_id = stat['player_id']
            team = player_teams.get(player_id, 'red')

            member = guild.get_member(player_id)
            name = member.display_name if member else stat.get('ign', f'User {player_id}')

            player_data = {
                'name': name,
                'agent': stat.get('agent', '?'),
                'kills': stat.get('kills', 0),
                'deaths': stat.get('deaths', 0),
                'assists': stat.get('assists', 0),
                'score': stat.get('score', 0),
                'damage': stat.get('damage_dealt', 0),
                'first_bloods': stat.get('first_bloods', 0),
                'headshots': stat.get('headshots', 0),
                'bodyshots': stat.get('bodyshots', 0),
                'legshots': stat.get('legshots', 0)
            }

            if team == winning_team:
                winners.append(player_data)
            else:
                losers.append(player_data)

        scoreboard_data = {
            'map_name': map_name,
            'winner_team_name': 'Red Team' if winning_team == 'red' else 'Blue Team',
            'loser_team_name': 'Blue Team' if winning_team == 'red' else 'Red Team',
            'winners': winners,
            'losers': losers
        }

        image = await self.stats_generator.generate_scoreboard_image(scoreboard_data)
        if image:
            image.seek(0)
            file = discord.File(image, filename='scoreboard.png')
            embed = discord.Embed(title=f"{map_name} - Scoreboard", color=COLOR_WHITE)
            embed.set_image(url="attachment://scoreboard.png")
            return embed, file
        else:
            return discord.Embed(description="Failed to generate scoreboard.", color=COLOR_NEUTRAL), None

    async def build_stats_embed(self, guild: discord.Guild, user: discord.Member) -> discord.Embed:
        """Build a stats embed for a user."""
        embed = discord.Embed(
            title=f"Stats for {user.display_name}",
            color=COLOR_NEUTRAL
        )
        
        games = await DatabaseHelper.get_all_games()
        
        for game in games:
            stats = await DatabaseHelper.get_player_stats(user.id, game.game_id)
            
            if stats.games_played > 0:
                winrate = (stats.wins / stats.games_played * 100)
                embed.add_field(
                    name=game.name,
                    value=f"Games: {stats.games_played} | Wins: {stats.wins} | Losses: {stats.losses}\nWin Rate: {winrate:.1f}%",
                    inline=False
                )
                
                # Top rivals
                rivals = await DatabaseHelper.get_player_rivalries(user.id, game.game_id)
                if rivals:
                    rival_lines = []
                    for r in rivals[:3]:
                        opponent = guild.get_member(r["opponent_id"])
                        opp_name = opponent.display_name if opponent else str(r["opponent_id"])
                        total = r["wins"] + r["losses"]
                        wr = (r["wins"] / total * 100) if total > 0 else 0
                        rival_lines.append(f"vs {opp_name}: {r['wins']}-{r['losses']} ({wr:.0f}%)")
                    
                    embed.add_field(
                        name=f"Top Rivals ({game.name})",
                        value="\n".join(rival_lines),
                        inline=False
                    )
        
        # Recent matches
        recent = await DatabaseHelper.get_player_recent_matches(user.id)
        if recent:
            match_lines = []
            for m in recent:
                result = "W" if m["winning_team"] == m["team"] else "L"
                date = datetime.fromisoformat(m["decided_at"]).strftime("%b %d")
                match_lines.append(f"{m['game_name']} - {result} - {date}")
            
            embed.add_field(
                name="Recent Matches",
                value="\n".join(match_lines),
                inline=False
            )
        
        if not games or all(
            (await DatabaseHelper.get_player_stats(user.id, g.game_id)).games_played == 0 
            for g in games
        ):
            embed.description = "No matches played yet."
        
        return embed
    
    # -------------------------------------------------------------------------
    # COMMANDS
    # -------------------------------------------------------------------------

    @app_commands.command(name="cm_settings", description="Open the settings panel (Server Admin)")
    async def settings_cmd(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You need Administrator permission.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="Custom Matches Settings",
            description="Configure your custom match system.",
            color=COLOR_NEUTRAL
        )
        
        # Show current config
        category_id = await DatabaseHelper.get_config("category_id")
        log_id = await DatabaseHelper.get_config("log_channel_id")
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        
        config_lines = []
        if category_id:
            cat = interaction.guild.get_channel(int(category_id))
            config_lines.append(f"Category: {cat.name if cat else 'Not found'}")
        else:
            config_lines.append("Category: Not set")
        
        if log_id:
            ch = interaction.guild.get_channel(int(log_id))
            config_lines.append(f"Log Channel: {ch.mention if ch else 'Not found'}")
        else:
            config_lines.append("Log Channel: Not set")
        
        if admin_role_id:
            role = interaction.guild.get_role(int(admin_role_id))
            config_lines.append(f"CM Admin Role: {role.name if role else 'Not found'}")
        else:
            config_lines.append("CM Admin Role: Not set")
        
        embed.add_field(name="Current Config", value="\n".join(config_lines), inline=False)
        
        # Show games
        games = await DatabaseHelper.get_all_games()
        if games:
            game_lines = []
            for g in games:
                ch = interaction.guild.get_channel(g.queue_channel_id) if g.queue_channel_id else None
                ch_str = ch.mention if ch else "No channel"
                game_lines.append(f"**{g.name}** — {g.player_count}p, {g.queue_type.value}, {ch_str}")
            embed.add_field(name="Games", value="\n".join(game_lines), inline=False)
        else:
            embed.add_field(name="Games", value="No games configured.", inline=False)
        
        view = SettingsView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @app_commands.command(name="cm_panel", description="Open the admin panel (CM Admins)")
    async def admin_cmd(self, interaction: discord.Interaction):
        if not await self.is_cm_admin(interaction.user):
            await interaction.response.send_message("You need the CM Admin role.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="Custom Matches Admin Panel",
            description="Manage active matches.",
            color=COLOR_NEUTRAL
        )
        
        matches = await DatabaseHelper.get_active_matches()
        if matches:
            match_lines = []
            for m in matches:
                game = await DatabaseHelper.get_game(m["game_id"])
                game_name = game.name if game else "Unknown"
                short_id = m.get("short_id") or str(m["match_id"])
                match_lines.append(f"Match {short_id} - {game_name}")
            embed.add_field(name="Active Matches", value="\n".join(match_lines), inline=False)
        else:
            embed.add_field(name="Active Matches", value="No active matches.", inline=False)
        
        view = AdminPanelView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @app_commands.command(name="cm_stats", description="View player stats")
    @app_commands.describe(user="The user to view stats for (defaults to yourself)")
    async def stats_cmd(self, interaction: discord.Interaction, user: discord.Member = None):
        target = user or interaction.user
        games = await DatabaseHelper.get_all_games()

        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return

        async def show_player_stats(inter: discord.Interaction, game_id: int):
            game = await DatabaseHelper.get_game(game_id)
            if not game:
                await inter.response.send_message("Game not found.", ephemeral=True)
                return

            # Try to use image generation if available
            if self.stats_generator.browser:
                await inter.response.defer()

                # Get recent matches for dropdown options
                recent_matches = await DatabaseHelper.get_player_recent_matches(
                    target.id, game.game_id, limit=5
                )

                # Generate both seasonal and lifetime images
                seasonal_data = await self._gather_stats_data(target, game, monthly=True, guild=inter.guild)
                lifetime_data = await self._gather_stats_data(target, game, monthly=False, guild=inter.guild)

                seasonal_image = await self.stats_generator.generate_stats_image(seasonal_data)
                lifetime_image = await self.stats_generator.generate_stats_image(lifetime_data)

                if seasonal_image and lifetime_image:
                    images = {'seasonal': seasonal_image, 'lifetime': lifetime_image}
                    view = StatsImageView(
                        self, target, game, images, recent_matches,
                        invoker_id=interaction.user.id, guild=inter.guild
                    )

                    # Show seasonal first
                    images['seasonal'].seek(0)
                    file = discord.File(images['seasonal'], filename='stats_seasonal.png')
                    embed = discord.Embed(
                        title=f"{target.display_name} - {game.name} Stats",
                        color=COLOR_WHITE
                    )
                    embed.set_image(url="attachment://stats_seasonal.png")
                    await inter.followup.send(embed=embed, file=file, view=view)
                    return

            # Fallback to embed-based stats
            stats = await DatabaseHelper.get_player_stats(target.id, game.game_id)
            view = PlayerStatsView(self, inter.guild, target.id, game, stats, monthly=True)
            await view.load_data()
            embed = await view.build_embed()
            if inter.response.is_done():
                await inter.followup.send(embed=embed, view=view)
            else:
                await inter.response.send_message(embed=embed, view=view)

        if len(games) == 1:
            await show_player_stats(interaction, games[0].game_id)
        else:
            view = discord.ui.View(timeout=60)
            view.add_item(GameSelectDropdown(games, show_player_stats))
            await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    @app_commands.command(name="cm_win", description="Report the match winner")
    async def win_cmd(self, interaction: discord.Interaction):
        # Check if in a match channel
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        needed = (game.player_count // 2) + 1

        # Get existing votes
        votes = await DatabaseHelper.get_win_votes(match["match_id"])
        red_votes = votes.get("red", 0)
        blue_votes = votes.get("blue", 0)

        red_role = interaction.guild.get_role(match["red_role_id"])
        blue_role = interaction.guild.get_role(match["blue_role_id"])

        # Get IGNs for display
        igns = await DatabaseHelper.get_match_igns(match["match_id"])

        # Build team lists
        red_players = [p for p in players if p["team"] == "red"]
        blue_players = [p for p in players if p["team"] == "blue"]

        def format_player(p: dict) -> str:
            pid = p["player_id"]
            if pid in igns:
                return f"`{igns[pid]}`"
            return f"<@{pid}>"

        red_lines = [format_player(p) for p in red_players]
        blue_lines = [format_player(p) for p in blue_players]

        embed = discord.Embed(
            title="Who Won?",
            description=f"Cast your vote! ({needed} votes needed)",
            color=COLOR_NEUTRAL
        )
        embed.add_field(
            name=f"Red Team ({red_votes} votes)",
            value="\n".join(red_lines) or "None",
            inline=True
        )
        embed.add_field(
            name=f"Blue Team ({blue_votes} votes)",
            value="\n".join(blue_lines) or "None",
            inline=True
        )

        view = WinVoteView(self, match["match_id"])
        # Ping both team roles
        await interaction.response.send_message(
            f"{red_role.mention} {blue_role.mention} - Vote for the winner!",
            embed=embed,
            view=view
        )

    @app_commands.command(name="cm_abandon", description="Vote to abandon the current match")
    async def abandon_cmd(self, interaction: discord.Interaction):
        # Check if in a match channel
        match = await DatabaseHelper.get_match_by_channel(interaction.channel.id)
        if not match:
            await interaction.response.send_message(
                "This command can only be used in a match channel.",
                ephemeral=True
            )
            return

        # Check user is in the match
        players = await DatabaseHelper.get_match_players(match["match_id"])
        if not any(p["player_id"] == interaction.user.id for p in players):
            await interaction.response.send_message("You're not in this match.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        needed = (game.player_count // 2) + 1  # Majority needed

        # Get existing votes
        current_votes = await DatabaseHelper.get_abandon_votes(match["match_id"])

        embed = discord.Embed(
            title="Abandon Match?",
            description=f"Vote to abandon this match.\nVotes: {current_votes}/{needed}",
            color=COLOR_WARNING
        )
        embed.add_field(
            name="Warning",
            value="If the match is abandoned, no stats will be recorded.",
            inline=False
        )

        view = AbandonVoteView(self, match["match_id"], needed)
        await interaction.response.send_message(embed=embed, view=view)

    @app_commands.command(name="cm_ign_set", description="Set your in-game name for a game")
    async def ign_set_cmd(self, interaction: discord.Interaction):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return

        async def show_ign_modal(inter: discord.Interaction, game_id: int):
            game = await DatabaseHelper.get_game(game_id)
            modal = IGNModal(self, game_id, game.name)
            await inter.response.send_modal(modal)

        if len(games) == 1:
            await show_ign_modal(interaction, games[0].game_id)
        else:
            view = discord.ui.View(timeout=60)
            view.add_item(GameSelectDropdown(games, show_ign_modal))
            await interaction.response.send_message("Select a game to set your IGN:", view=view, ephemeral=True)

    @app_commands.command(name="cm_fetch_stats", description="Manually fetch Valorant stats for a match (Admin)")
    @app_commands.describe(match_id="The match ID or short ID to fetch stats for")
    async def fetch_stats_cmd(self, interaction: discord.Interaction, match_id: str):
        """Manually trigger Valorant stats fetch for a completed match."""
        if not await self.is_cm_admin(interaction.user):
            await interaction.response.send_message("You need the CM Admin role.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        # Find the match by ID or short_id
        try:
            mid = int(match_id)
            match = await DatabaseHelper.get_match(mid)
        except ValueError:
            # Try short_id lookup
            match = await DatabaseHelper.get_match_by_short_id(match_id)

        if not match:
            await interaction.followup.send(f"Match `{match_id}` not found.", ephemeral=True)
            return

        if not match.get("winning_team"):
            await interaction.followup.send("This match hasn't been completed yet.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        if not game or 'valorant' not in game.name.lower():
            await interaction.followup.send("This is not a Valorant game.", ephemeral=True)
            return

        # Check if stats already exist
        existing_stats = await DatabaseHelper.get_valorant_match_stats(match["match_id"])
        if existing_stats:
            await interaction.followup.send(
                f"Stats already exist for match `{match_id}`. Found {len(existing_stats)} player records.",
                ephemeral=True
            )
            return

        # Get match info
        players = await DatabaseHelper.get_match_players(match["match_id"])
        player_ids = [p["player_id"] for p in players]

        # Use decided_at time if available, otherwise use current time minus some buffer
        if match.get("decided_at"):
            try:
                match_end_time = datetime.fromisoformat(match["decided_at"].replace('Z', '+00:00'))
            except Exception:
                match_end_time = datetime.now(timezone.utc)
        else:
            match_end_time = datetime.now(timezone.utc)

        await interaction.followup.send(
            f"Attempting to fetch Valorant stats for match `{match_id}`...",
            ephemeral=True
        )

        # Try to fetch stats now
        success = await self.fetch_valorant_match_stats(
            match["match_id"], game.game_id, player_ids, match_end_time
        )

        if success:
            stats = await DatabaseHelper.get_valorant_match_stats(match["match_id"])
            await interaction.followup.send(
                f"Successfully fetched stats for {len(stats)} players in match `{match_id}`!",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"Could not find match data in HenrikDev API. The match may not be indexed yet, "
                f"or player IGNs may not be linked. Try again in a few minutes.",
                ephemeral=True
            )

    # -------------------------------------------------------------------------
    # EVENT LISTENERS
    # -------------------------------------------------------------------------
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Handle member leaving - remove from queues and delete all stats."""
        try:
            # Remove from any active queues
            for queue_id, queue_state in list(self.queues.items()):
                if member.id in queue_state.players:
                    del queue_state.players[member.id]

                    # Update embed if possible
                    game = await DatabaseHelper.get_game(queue_state.game_id)
                    if game:
                        channel = member.guild.get_channel(queue_state.channel_id)
                        if channel and queue_state.message_id:
                            try:
                                msg = await channel.fetch_message(queue_state.message_id)
                                embed = await self.create_queue_embed(game, queue_state)
                                await msg.edit(embed=embed)
                            except Exception:
                                pass

            # Delete all player stats (permanently removed from leaderboards)
            await DatabaseHelper.delete_player_stats(member.id)
        except Exception as e:
            logger.error(f"Error in on_member_remove: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle queue embed deletion - clean up the queue state."""
        try:
            # Check if this message was a queue embed
            for queue_id, queue_state in list(self.queues.items()):
                if queue_state.message_id == payload.message_id:
                    # Clean up the queue from memory and database
                    del self.queues[queue_id]
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("DELETE FROM active_queues WHERE queue_id = ?", (queue_id,))
                        await db.execute("DELETE FROM queue_players WHERE queue_id = ?", (queue_id,))
                        await db.commit()
                    logger.info(f"Queue {queue_id} cleaned up due to message deletion")
                    break
        except Exception as e:
            logger.error(f"Error in on_raw_message_delete: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Handle captain selection via @mentions in draft channels."""
        try:
            if message.author.bot:
                return

            # Check if this is a draft channel awaiting admin captain selection
            match = await DatabaseHelper.get_match_by_channel(message.channel.id)
            if not match:
                # Check draft channels
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(
                        """SELECT * FROM matches WHERE draft_channel_id = ?
                           AND queue_type = 'captains_awaiting' AND winning_team IS NULL""",
                        (message.channel.id,)
                    ) as cursor:
                        row = await cursor.fetchone()
                        if not row:
                            return
                        match = dict(zip([d[0] for d in cursor.description], row))

            if match.get("queue_type") != "captains_awaiting":
                return

            # Check if admin
            if not await self.is_cm_admin(message.author):
                return

            # Check for two mentions
            if len(message.mentions) != 2:
                return

            # Get players in this match
            players = await DatabaseHelper.get_match_players(match["match_id"])
            player_ids = [p["player_id"] for p in players]

            # Verify both mentioned users are in the match
            captain1, captain2 = message.mentions
            if captain1.id not in player_ids or captain2.id not in player_ids:
                await message.channel.send("Both captains must be players in this match.", delete_after=5)
                return

            # Proceed with draft
            game = await DatabaseHelper.get_game(match["game_id"])
            guild = message.guild
            category = message.channel.category

            red_role = guild.get_role(match["red_role_id"])
            blue_role = guild.get_role(match["blue_role_id"])

            await DatabaseHelper.update_match(match["match_id"], queue_type="captains")

            await self.proceed_with_draft(
                guild, message.channel, category, game, match["match_id"],
                player_ids, red_role, blue_role, captain1.id, captain2.id
            )
        except Exception as e:
            logger.error(f"Error in on_message: {e}", exc_info=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CustomMatch(bot))
