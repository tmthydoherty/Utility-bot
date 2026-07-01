import asyncio
import base64
import io
import logging
import random
from typing import Optional
from pathlib import Path

from .models import (
    STATS_TEMPLATE_PATH, MATCH_TEMPLATE_PATH, SCOREBOARD_TEMPLATE_PATH,
    LEADERBOARD_TEMPLATE_PATH, SERVERSTATS_TEMPLATE_PATH, SIMPLE_STATS_TEMPLATE_PATH,
    RIVALS_RESULTS_TEMPLATE_PATH, RIVALS_SERVERSTATS_TEMPLATE_PATH,
    RIVALS_STATS_TEMPLATE_PATH, H2H_TEMPLATE_PATH, H2H_BG_PATH, FONTS_PATH,
)

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logger = logging.getLogger('custommatch')


# =============================================================================
# STATS CARD GENERATOR
# =============================================================================

class StatsCardGenerator:
    """Generates stats card images using Playwright and HTML templates."""

    def __init__(self):
        self.browser = None
        self.playwright = None
        self._page_semaphore = asyncio.Semaphore(3)
        # Cached templates (loaded once in initialize)
        self._stats_template: Optional[str] = None
        self._match_template: Optional[str] = None
        self._scoreboard_template: Optional[str] = None
        self._leaderboard_template: Optional[str] = None
        self._serverstats_template: Optional[str] = None
        self._simple_stats_template: Optional[str] = None
        self._rivals_results_template: Optional[str] = None
        self._rivals_serverstats_template: Optional[str] = None
        self._rivals_stats_template: Optional[str] = None
        self._h2h_template: Optional[str] = None

    async def initialize(self):
        """Initialize the browser and cache templates."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not available. Stats cards will use embeds.")
            return False

        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                args=[
                    '--font-render-hinting=none',
                    '--disable-lcd-text',
                    '--enable-font-antialiasing',
                ]
            )
            # Cache all templates at startup
            for attr, path in [
                ('_stats_template', STATS_TEMPLATE_PATH),
                ('_match_template', MATCH_TEMPLATE_PATH),
                ('_scoreboard_template', SCOREBOARD_TEMPLATE_PATH),
                ('_leaderboard_template', LEADERBOARD_TEMPLATE_PATH),
                ('_serverstats_template', SERVERSTATS_TEMPLATE_PATH),
                ('_simple_stats_template', SIMPLE_STATS_TEMPLATE_PATH),
                ('_rivals_results_template', RIVALS_RESULTS_TEMPLATE_PATH),
                ('_rivals_serverstats_template', RIVALS_SERVERSTATS_TEMPLATE_PATH),
                ('_rivals_stats_template', RIVALS_STATS_TEMPLATE_PATH),
                ('_h2h_template', H2H_TEMPLATE_PATH),
            ]:
                if path.exists():
                    setattr(self, attr, path.read_text(encoding='utf-8'))
                else:
                    logger.warning(f"Template not found at {path}")
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
            # Load cached template
            template = self._stats_template
            if not template:
                logger.error("Stats template not cached")
                return None

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
            <div class="section-title">Best Teammates</div>
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
            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 580, 'height': 500},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)

                    # Wait for fonts to load
                    await page.wait_for_timeout(100)

                    # Get the actual content height
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 580, 'height': body_height + 40})

                    screenshot = await page.screenshot(type='png')
                finally:
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
            # Load cached match template
            template = self._match_template
            if not template:
                logger.error("Match template not cached")
                return None

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

            # Use actual round count if available, fallback to 24
            total_rounds = match_data.get('total_rounds') or 24
            adr = round(damage / total_rounds) if damage > 0 else 0
            acs = round(score / total_rounds) if score > 0 else 0
            econ_rating = round(econ_loadout / total_rounds) if econ_loadout > 0 else 0

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
            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 520, 'height': 500},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)

                    # Wait for fonts to load
                    await page.wait_for_timeout(100)

                    # Get the actual content height
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 520, 'height': body_height + 20})

                    screenshot = await page.screenshot(type='png')
                finally:
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
            # Load cached scoreboard template
            template = self._scoreboard_template
            if not template:
                logger.error("Scoreboard template not cached")
                return None

            map_name = scoreboard_data.get('map_name', 'Unknown')
            red_score = scoreboard_data.get('red_score', 0)
            blue_score = scoreboard_data.get('blue_score', 0)
            red_is_winner = scoreboard_data.get('red_is_winner', False)
            red_players = scoreboard_data.get('red_players', [])
            blue_players = scoreboard_data.get('blue_players', [])
            total_rounds = (red_score or 0) + (blue_score or 0) or 24  # fallback to 24

            # Build team labels with round counts and trophy
            red_trophy = " \U0001f3c6" if red_is_winner else ""
            blue_trophy = " \U0001f3c6" if not red_is_winner else ""
            red_team_label = f"RED TEAM - {red_score}{red_trophy}"
            blue_team_label = f"BLUE TEAM - {blue_score}{blue_trophy}"

            def calc_player_stats(player: dict) -> dict:
                """Pre-calculate derived stats for a player."""
                kills = player.get('kills', 0) or 0
                deaths = player.get('deaths', 0) or 0
                assists = player.get('assists', 0) or 0
                kd = round(kills / deaths, 2) if deaths > 0 else float(kills)
                headshots = player.get('headshots', 0) or 0
                bodyshots = player.get('bodyshots', 0) or 0
                legshots = player.get('legshots', 0) or 0
                total_shots = headshots + bodyshots + legshots
                hs_percent = round(headshots / total_shots * 100) if total_shots > 0 else 0
                damage = player.get('damage', 0) or 0
                score = player.get('score', 0) or 0
                adr = round(damage / total_rounds) if damage > 0 else 0
                acs = round(score / total_rounds) if score > 0 else 0
                first_bloods = player.get('first_bloods', 0) or 0
                return {
                    'kills': kills, 'deaths': deaths, 'assists': assists,
                    'kd': kd, 'hs_percent': hs_percent, 'damage': damage,
                    'adr': adr, 'acs': acs, 'first_bloods': first_bloods,
                    'name': player.get('name', 'Unknown'),
                    'agent': player.get('agent', '?'),
                }

            def find_best_stats(team_stats: list) -> dict:
                """Find the best value for each stat column among a team."""
                best = {}
                for key in ('kd', 'hs_percent', 'adr', 'acs', 'first_bloods', 'damage'):
                    vals = [s[key] for s in team_stats]
                    best[key] = max(vals) if vals else 0
                return best

            def build_player_row(stats: dict, rank: int, team_class: str,
                                 best: dict, max_acs: int, is_mvp: bool = False) -> str:
                name = stats['name']
                if len(name) > 16:
                    name = name[:13] + '...'
                agent = stats['agent']
                if len(agent) > 10:
                    agent = agent[:8] + '..'

                mvp_html = '<span class="mvp-badge">MVP</span>' if is_mvp else ''
                row_class = f"mvp-row" if is_mvp else team_class
                rank_class = "top" if rank == 1 and is_mvp else ""

                # Highlight best-in-column stats
                def sc(key, val, fmt=None):
                    display = fmt if fmt else str(val)
                    cls = "stat-cell best" if val == best.get(key) and val > 0 else "stat-cell"
                    return f'<span class="{cls}">{display}</span>'

                # ACS bar width (relative to max across both teams)
                bar_width = round(stats['acs'] / max_acs * 70) if max_acs > 0 else 0
                bar_class = "red-team" if "red" in team_class else "blue-team"

                return f'''
                <div class="player-row {row_class}">
                    <span class="rank-cell {rank_class}">{rank}</span>
                    <div class="player-info">
                        <span class="player-name">{name}{mvp_html}</span>
                    </div>
                    <span class="agent-name">{agent}</span>
                    <span class="kda-cell">
                        <span class="kda-kills">{stats['kills']}</span>
                        <span class="kda-slash">/</span>
                        <span class="kda-deaths">{stats['deaths']}</span>
                        <span class="kda-slash">/</span>
                        <span class="kda-assists">{stats['assists']}</span>
                    </span>
                    {sc('kd', stats['kd'])}
                    {sc('hs_percent', stats['hs_percent'], f"{stats['hs_percent']}%")}
                    {sc('adr', stats['adr'])}
                    <span class="acs-cell {('stat-cell best' if stats['acs'] == best.get('acs') and stats['acs'] > 0 else 'stat-cell')}">{stats['acs']}<span class="acs-bar {bar_class}" style="width:{bar_width}%"></span></span>
                    {sc('first_bloods', stats['first_bloods'])}
                    {sc('damage', stats['damage'])}
                </div>'''

            # Sort players by ACS for MVP detection and ranking
            def get_acs(p):
                return (p.get('score', 0) or 0) / total_rounds

            sorted_red = sorted(red_players, key=get_acs, reverse=True)
            sorted_blue = sorted(blue_players, key=get_acs, reverse=True)

            # Pre-calculate stats for best-in-column detection
            red_stats = [calc_player_stats(p) for p in sorted_red]
            blue_stats = [calc_player_stats(p) for p in sorted_blue]
            red_best = find_best_stats(red_stats)
            blue_best = find_best_stats(blue_stats)

            # Max ACS across both teams for bar scaling
            all_acs = [s['acs'] for s in red_stats + blue_stats]
            max_acs = max(all_acs) if all_acs else 1

            # Build player rows — MVP is top ACS on the winning team
            red_rows = []
            for i, stats in enumerate(red_stats):
                red_rows.append(build_player_row(
                    stats, i + 1, 'red-row', red_best, max_acs, is_mvp=(i == 0 and red_is_winner)))

            blue_rows = []
            for i, stats in enumerate(blue_stats):
                blue_rows.append(build_player_row(
                    stats, i + 1, 'blue-row', blue_best, max_acs, is_mvp=(i == 0 and not red_is_winner)))

            html = template.format(
                font_path=str(FONTS_PATH),
                map_name=map_name,
                red_team_label=red_team_label,
                blue_team_label=blue_team_label,
                red_players_html=''.join(red_rows),
                blue_players_html=''.join(blue_rows)
            )

            # Render to image
            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 850, 'height': 600},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)

                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 850, 'height': body_height + 20})

                    screenshot = await page.screenshot(type='png')
                finally:
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
            template = self._leaderboard_template
            if not template:
                logger.error("Leaderboard template not cached")
                return None

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
            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 900, 'height': 600},
                    device_scale_factor=3
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)

                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 900, 'height': body_height + 20})

                    screenshot = await page.screenshot(type='png')
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating leaderboard card: {e}")
            return None

    # Accent color palette for server stats cards: (hex, r, g, b)
    SERVERSTATS_ACCENT_COLORS = [
        ('#d4af37', 212, 175, 55),   # Gold
        ('#e8637a', 232, 99, 122),   # Coral
        ('#2dd4a8', 45, 212, 168),   # Teal
        ('#a78bfa', 167, 139, 250),  # Violet
        ('#38bdf8', 56, 189, 248),   # Sky
    ]

    async def generate_serverstats_image(self, data: dict) -> Optional[io.BytesIO]:
        """Generate a server stats card image."""
        if not self.browser:
            return None

        try:
            template = self._serverstats_template
            if not template:
                logger.error("Server stats template not cached")
                return None

            # Pick a random accent color
            accent_hex, accent_r, accent_g, accent_b = random.choice(self.SERVERSTATS_ACCENT_COLORS)

            # Build maps bar HTML (horizontal bar chart style)
            maps_data = data.get('maps', [])
            total_matches = data.get('total_matches', 1) or 1
            if maps_data:
                maps_html = '<div class="agents-wrap">'
                for m in maps_data:
                    pct = round(m['count'] / total_matches * 100)
                    maps_html += (
                        f'<div class="agent-chip">'
                        f'<span class="agent-name">{m["name"]}</span>'
                        f'<span class="agent-count">{m["count"]} ({pct}%)</span>'
                        f'</div>'
                    )
                maps_html += '</div>'
            else:
                maps_html = '<div class="no-data">No map data</div>'

            # Build agents list HTML (compact chips)
            agents_data = data.get('agents', [])
            if agents_data:
                agents_html = ''
                for a in agents_data:
                    agents_html += (
                        f'<div class="agent-chip">'
                        f'<span class="agent-name">{a["name"]}</span>'
                        f'<span class="agent-count">{a["count"]}</span>'
                        f'</div>'
                    )
            else:
                agents_html = '<div class="no-data">No agent data</div>'

            # Build leaders grid HTML
            leaders = data.get('leaders', [])
            if leaders:
                leaders_html = ''
                for ldr in leaders:
                    min_badge = ''
                    if ldr.get('min_games'):
                        min_badge = f'<span class="min-games">{ldr["min_games"]}+ games</span>'
                    leaders_html += (
                        f'<div class="leader-card">'
                        f'<div class="leader-label">{ldr["label"]}{min_badge}</div>'
                        f'<div class="leader-player">{ldr["player"]}</div>'
                        f'<div class="leader-value">{ldr["value"]}</div>'
                        f'</div>'
                    )
            else:
                leaders_html = '<div class="no-data">Not enough data</div>'

            # Format large numbers
            def fmt_num(n):
                return f"{n:,}" if isinstance(n, int) else str(n)

            html = template.format(
                font_path=str(FONTS_PATH),
                accent_color=accent_hex,
                accent_r=accent_r,
                accent_g=accent_g,
                accent_b=accent_b,
                period_title=data.get('period_title', 'Server Stats'),
                game_name=data.get('game_name', ''),
                total_matches=fmt_num(data.get('total_matches', 0)),
                total_players=fmt_num(data.get('total_players', 0)),
                total_kills=fmt_num(data.get('total_kills', 0)),
                total_deaths=fmt_num(data.get('total_deaths', 0)),
                total_assists=fmt_num(data.get('total_assists', 0)),
                total_damage=fmt_num(data.get('total_damage', 0)),
                total_aces=fmt_num(data.get('total_aces', 0)),
                hs_pct=data.get('hs_pct', 0),
                maps_html=maps_html,
                agents_html=agents_html,
                leaders_html=leaders_html
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 580, 'height': 500},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)

                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 580, 'height': body_height + 40})

                    screenshot = await page.screenshot(type='png')
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating server stats card: {e}")
            return None

    async def generate_simple_stats_image(self, player_data: dict) -> Optional[io.BytesIO]:
        """Generate a simplified stats card for non-Valorant games."""
        if not self.browser:
            return None

        try:
            template = self._simple_stats_template
            if not template:
                logger.error("Simple stats template not cached")
                return None

            wins = player_data.get('wins', 0)
            losses = player_data.get('losses', 0)
            total = wins + losses
            winrate = round(wins / total * 100) if total > 0 else 0
            win_width = round(wins / total * 100) if total > 0 else 50
            loss_width = 100 - win_width

            # Best/cursed teammates
            best_teammates = player_data.get('best_teammates', [])
            worst_teammates = player_data.get('worst_teammates', [])

            best_teammates_html = ''.join(
                f'<div class="teammate-item">{t["name"]} ({t["wins"]}-{t["losses"]})</div>'
                for t in best_teammates
            ) if best_teammates else '<div class="no-data">Not enough data</div>'

            worst_teammates_html = ''.join(
                f'<div class="teammate-item cursed">{t["name"]} ({t["wins"]}-{t["losses"]})</div>'
                for t in worst_teammates
            ) if worst_teammates else '<div class="no-data">Not enough data</div>'

            # Maps with W/L
            map_stats = player_data.get('map_stats', [])
            maps_html = ''.join(
                f'<div class="map-chip">{m["name"]} ({m["wins"]}-{m["losses"]})</div>'
                for m in map_stats
            ) if map_stats else '<div class="no-data">No map data</div>'

            # Recent matches
            recent_matches = player_data.get('recent_matches', [])
            if recent_matches:
                recent_matches_html = ''
                for match in recent_matches:
                    result_class = "win" if match.get('won') else "loss"
                    result_letter = "W" if match.get('won') else "L"
                    map_text = match.get('map_name', '') or "Unknown"
                    recent_matches_html += (
                        f'<div class="match-row">'
                        f'<div class="result-badge {result_class}">{result_letter}</div>'
                        f'<div class="match-map">{map_text}</div>'
                        f'</div>'
                    )
            else:
                recent_matches_html = '<div class="no-data">No recent matches</div>'

            player_name = player_data.get('player_name', 'Unknown')
            if len(player_name) > 18:
                player_name = player_name[:15] + '...'

            html = template.format(
                font_path=str(FONTS_PATH),
                avatar_url=player_data.get('avatar_url', ''),
                player_name=player_name,
                game_name=player_data.get('game_name', ''),
                period_title=player_data.get('period_title', 'Stats'),
                wins=wins,
                losses=losses,
                winrate=winrate,
                win_width=win_width,
                loss_width=loss_width,
                best_teammates_html=best_teammates_html,
                worst_teammates_html=worst_teammates_html,
                maps_html=maps_html,
                recent_matches_html=recent_matches_html,
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 560, 'height': 400},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)

                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 560, 'height': body_height + 20})

                    screenshot = await page.screenshot(type='png')
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating simple stats card: {e}")
            return None

    async def generate_rivals_results_image(
        self,
        red_players: list,
        blue_players: list,
        winning_team: str,
    ) -> Optional[io.BytesIO]:
        """Generate a Marvel Rivals post-match results card."""
        if not self.browser:
            return None

        try:
            template = self._rivals_results_template
            if not template:
                logger.error("Rivals results template not cached")
                return None

            def fmt_num(n):
                try:
                    return f"{int(n):,}"
                except (TypeError, ValueError):
                    return str(n) if n is not None else "0"

            def role_class(role: Optional[str]) -> str:
                if not role:
                    return ""
                return role.strip().lower()

            def build_row(p: dict) -> str:
                role = p.get('role') or ''
                name = (p.get('ign') or 'Unknown')[:18]
                mvp_svp = p.get('mvp_svp')
                badge = ''
                if mvp_svp == 'MVP':
                    badge = '<span class="mvp">MVP</span>'
                elif mvp_svp == 'SVP':
                    badge = '<span class="svp">SVP</span>'
                team_cls = p.get('team', 'red').lower()
                return (
                    f'<div class="player-row {team_cls}">'
                    f'<div class="role {role_class(role)}">{role[:3].upper() if role else "—"}</div>'
                    f'<div class="name">{name}{badge}</div>'
                    f'<div class="stat">{p.get("kills", 0)}</div>'
                    f'<div class="stat">{p.get("deaths", 0)}</div>'
                    f'<div class="stat">{p.get("assists", 0)}</div>'
                    f'<div class="stat">{fmt_num(p.get("final_hits", 0))}</div>'
                    f'<div class="stat">{fmt_num(p.get("damage", 0))}</div>'
                    f'<div class="stat">{fmt_num(p.get("damage_blocked", 0))}</div>'
                    f'<div class="stat">{fmt_num(p.get("healing", 0))}</div>'
                    f'<div class="stat">{p.get("medal_count", 0)}</div>'
                    f'</div>'
                )

            red_rows = ''.join(build_row({**p, 'team': 'red'}) for p in red_players) or \
                '<div class="player-row red"><div class="name">No data</div></div>'
            blue_rows = ''.join(build_row({**p, 'team': 'blue'}) for p in blue_players) or \
                '<div class="player-row blue"><div class="name">No data</div></div>'

            winner_class = 'red' if (winning_team or '').upper() == 'RED' else 'blue'
            winner_label = f"{winner_class.upper()} TEAM WINS"

            html = template.format(
                font_path=str(FONTS_PATH),
                winner_class=winner_class,
                winner_label=winner_label,
                red_rows=red_rows,
                blue_rows=blue_rows,
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 920, 'height': 800},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 920, 'height': body_height + 20})
                    screenshot = await page.screenshot(type='png')
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating rivals results card: {e}")
            return None

    async def generate_rivals_serverstats_image(self, data: dict) -> Optional[io.BytesIO]:
        """Generate a Marvel Rivals server stats card."""
        if not self.browser:
            return None

        try:
            template = self._rivals_serverstats_template
            if not template:
                logger.error("Rivals server stats template not cached")
                return None

            def fmt_num(n):
                try:
                    return f"{int(n):,}"
                except (TypeError, ValueError):
                    return str(n) if n is not None else "0"

            leaders = data.get('leaders', [])
            if leaders:
                leaders_html = ''
                for ldr in leaders:
                    leaders_html += (
                        f'<div class="lb-tile">'
                        f'<div class="lb-label">{ldr.get("label", "")}</div>'
                        f'<div class="lb-winner">{ldr.get("player", "—")}</div>'
                        f'<div class="lb-value">{ldr.get("value", "")}</div>'
                        f'</div>'
                    )
            else:
                leaders_html = '<div class="lb-tile"><div class="lb-label">No data</div><div class="lb-winner">\u2014</div></div>'

            html = template.format(
                font_path=str(FONTS_PATH),
                period_title=data.get('period_title', 'Server Stats'),
                total_matches=fmt_num(data.get('total_matches', 0)),
                total_players=fmt_num(data.get('total_players', 0)),
                total_kills=fmt_num(data.get('total_kills', 0)),
                total_final_hits=fmt_num(data.get('total_final_hits', 0)),
                total_damage=fmt_num(data.get('total_damage', 0)),
                total_blocked=fmt_num(data.get('total_blocked', 0)),
                total_healing=fmt_num(data.get('total_healing', 0)),
                total_medals=fmt_num(data.get('total_medals', 0)),
                leaders_html=leaders_html,
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 1200, 'height': 800},
                    device_scale_factor=2
                )
                try:
                    await page.set_content(html)
                    await page.wait_for_timeout(100)
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 1200, 'height': body_height + 20})
                    screenshot = await page.screenshot(type='png')
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating rivals server stats card: {e}")
            return None

    async def generate_rivals_stats_image(self, data: dict) -> Optional[io.BytesIO]:
        """Generate a Marvel Rivals per-player stats card."""
        if not self.browser:
            return None

        try:
            template = self._rivals_stats_template
            if not template:
                logger.error("Rivals stats template not cached")
                return None

            def fmt_num(n):
                try:
                    return f"{int(n):,}"
                except (TypeError, ValueError):
                    return str(n) if n is not None else "0"

            wins = data.get('wins', 0) or 0
            losses = data.get('losses', 0) or 0
            games = data.get('games', wins + losses) or (wins + losses)
            total = wins + losses
            winrate = round(wins / total * 100) if total > 0 else 0
            win_width = round(wins / total * 100) if total > 0 else 50
            loss_width = 100 - win_width

            kills = data.get('total_kills', 0) or 0
            deaths = data.get('total_deaths', 0) or 0
            assists = data.get('total_assists', 0) or 0
            kd = round(kills / deaths, 2) if deaths > 0 else float(kills)
            kda = round((kills + assists) / deaths, 2) if deaths > 0 else float(kills + assists)

            # Top medals list (up to 3)
            top_medal_types = data.get('top_medal_types', []) or []
            if top_medal_types:
                top_medals_html = ''.join(
                    f'<div class="medal-row"><div class="mname">{name}</div><div class="mcount">\u00d7{cnt}</div></div>'
                    for name, cnt in top_medal_types
                )
            else:
                top_medals_html = '<div class="no-data">No medals yet</div>'

            # Recent matches pills
            recent_matches = data.get('recent_matches', []) or []
            if recent_matches:
                recent_matches_html = ''
                for match in recent_matches:
                    result_class = "win" if match.get('won') else "loss"
                    result_letter = "W" if match.get('won') else "L"
                    map_text = (match.get('map_name') or "Unknown")[:20]
                    recent_matches_html += (
                        f'<div class="recent-pill">'
                        f'<div class="result-dot {result_class}">{result_letter}</div>'
                        f'<div class="recent-map">{map_text}</div>'
                        f'</div>'
                    )
            else:
                recent_matches_html = '<div class="no-data">No recent matches</div>'

            # Role chip
            fav_role = data.get('favorite_role')
            role_chip_html = f'<div class="role-chip">{fav_role}</div>' if fav_role else ''

            # Accuracy
            ba = data.get('best_accuracy')
            la = data.get('last_accuracy')
            best_accuracy_display = f"{round(ba)}%" if ba is not None else "\u2014"
            last_accuracy_display = f"{round(la)}%" if la is not None else "\u2014"

            player_name = data.get('player_name', 'Unknown')
            if len(player_name) > 18:
                player_name = player_name[:15] + '...'

            html = template.format(
                font_path=str(FONTS_PATH),
                avatar_url=data.get('avatar_url', ''),
                player_name=player_name,
                game_name=data.get('game_name', 'Marvel Rivals'),
                period_title=data.get('period_title', 'Stats'),
                winrate=winrate,
                role_chip_html=role_chip_html,
                games=games,
                wins=wins,
                losses=losses,
                kd=kd,
                kda=kda,
                total_medals=fmt_num(data.get('total_medals', 0)),
                win_width=win_width,
                loss_width=loss_width,
                total_kills=fmt_num(kills),
                total_deaths=fmt_num(deaths),
                total_assists=fmt_num(assists),
                total_final_hits=fmt_num(data.get('total_final_hits', 0)),
                total_damage=fmt_num(data.get('total_damage', 0)),
                total_blocked=fmt_num(data.get('total_blocked', 0)),
                total_healing=fmt_num(data.get('total_healing', 0)),
                top_medals_html=top_medals_html,
                best_accuracy_display=best_accuracy_display,
                last_accuracy_display=last_accuracy_display,
                mvps=data.get('mvps', 0) or 0,
                svps=data.get('svps', 0) or 0,
                recent_matches_html=recent_matches_html,
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 1180, 'height': 900},
                    device_scale_factor=3,
                )
                try:
                    await page.set_content(html, wait_until='domcontentloaded')
                    # Block until @font-face resources are loaded so Cinzel
                    # doesn't fall back mid-paint on the first few renders.
                    try:
                        await page.evaluate('() => document.fonts.ready')
                    except Exception:
                        pass
                    await page.wait_for_timeout(120)
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 1180, 'height': body_height + 24})
                    screenshot = await page.screenshot(type='png', omit_background=False)
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating rivals stats card: {e}")
            return None

    async def generate_h2h_image(self, data: dict) -> Optional[io.BytesIO]:
        """Generate a premium Head-to-Head comparison card."""
        if not self.browser:
            return None

        try:
            template = self._h2h_template
            if not template:
                logger.error("H2H template not cached")
                return None

            # Encode background image as base64 data URI
            bg_data_uri = ''
            try:
                with open(H2H_BG_PATH, 'rb') as f:
                    bg_b64 = base64.b64encode(f.read()).decode()
                bg_data_uri = f'data:image/png;base64,{bg_b64}'
            except Exception:
                logger.warning("Could not load H2H background image")

            # Build stat comparison rows HTML
            stat_rows = data.get('stat_rows', [])
            stat_rows_html = ''
            for row in stat_rows:
                a_class = 'better' if row.get('a_better') else ''
                b_class = 'better' if row.get('b_better') else ''
                stat_rows_html += (
                    f'<div class="stat-row">'
                    f'<div class="stat-val left {a_class}">{row["a_val"]}</div>'
                    f'<div class="stat-label">{row["label"]}</div>'
                    f'<div class="stat-val right {b_class}">{row["b_val"]}</div>'
                    f'</div>'
                )

            # Build VS Stats section (only if there are stat rows)
            vs_stats_section = ''
            if stat_rows_html:
                vs_stats_section = (
                    f'<div class="section">'
                    f'<div class="section-title">&#9670; VS Stats</div>'
                    f'<div class="stat-grid">{stat_rows_html}</div>'
                    f'</div>'
                )

            # Teammate section HTML
            tm = data.get('teammate', {})
            teammate_html = ''
            if tm.get('games', 0) > 0:
                tm_wr = round(tm['wins'] / tm['games'] * 100) if tm['games'] > 0 else 0

                # Summary cards
                summary_html = (
                    f'<div class="tm-summary">'
                    f'<div class="tm-card"><div class="tm-val">{tm["games"]}</div><div class="tm-label">Games</div></div>'
                    f'<div class="tm-card"><div class="tm-val">{tm["wins"]}</div><div class="tm-label">Wins</div></div>'
                    f'<div class="tm-card"><div class="tm-val">{tm_wr}%</div><div class="tm-label">Win Rate</div></div>'
                    f'</div>'
                )

                # Teammate stat rows
                tm_stat_rows = data.get('teammate_stat_rows', [])
                tm_stats_html = ''
                for row in tm_stat_rows:
                    a_class = 'better' if row.get('a_better') else ''
                    b_class = 'better' if row.get('b_better') else ''
                    tm_stats_html += (
                        f'<div class="stat-row">'
                        f'<div class="stat-val left {a_class}">{row["a_val"]}</div>'
                        f'<div class="stat-label">{row["label"]}</div>'
                        f'<div class="stat-val right {b_class}">{row["b_val"]}</div>'
                        f'</div>'
                    )

                tm_grid = f'<div class="stat-grid">{tm_stats_html}</div>' if tm_stats_html else ''

                teammate_html = (
                    f'<div class="section">'
                    f'<div class="section-title">&#9670; When Teammates</div>'
                    f'{summary_html}'
                    f'{tm_grid}'
                    f'</div>'
                )

            # H2H record split bar
            a_wins = data.get('h2h_a_wins', 0)
            b_wins = data.get('h2h_b_wins', 0)
            total_h2h = a_wins + b_wins
            a_pct = round(a_wins / total_h2h * 100) if total_h2h > 0 else 50
            b_pct = 100 - a_pct

            # H2H streak text
            h2h_streak_text = data.get('h2h_streak_text', '')
            streak_html = f'<div class="h2h-streak">{h2h_streak_text}</div>' if h2h_streak_text else ''

            html = template.format(
                font_path=str(FONTS_PATH),
                bg_path=bg_data_uri,
                game_name=data.get('game_name', ''),
                a_avatar=data.get('a_avatar', ''),
                b_avatar=data.get('b_avatar', ''),
                a_name=data.get('a_name', 'Player 1'),
                b_name=data.get('b_name', 'Player 2'),
                a_record=data.get('a_record', '0-0'),
                b_record=data.get('b_record', '0-0'),
                a_wr=data.get('a_wr', '0'),
                b_wr=data.get('b_wr', '0'),
                h2h_a_wins=a_wins,
                h2h_b_wins=b_wins,
                h2h_a_pct=a_pct,
                h2h_b_pct=b_pct,
                streak_html=streak_html,
                vs_stats_section=vs_stats_section,
                teammate_html=teammate_html,
            )

            async with self._page_semaphore:
                page = await self.browser.new_page(
                    viewport={'width': 1180, 'height': 900},
                    device_scale_factor=3,
                )
                try:
                    await page.set_content(html, wait_until='domcontentloaded')
                    try:
                        await page.evaluate('() => document.fonts.ready')
                    except Exception:
                        pass
                    await page.wait_for_timeout(150)
                    body_height = await page.evaluate('document.body.scrollHeight')
                    await page.set_viewport_size({'width': 1180, 'height': body_height + 24})
                    screenshot = await page.screenshot(type='png', omit_background=False)
                finally:
                    await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            logger.error(f"Error generating H2H card: {e}")
            return None
