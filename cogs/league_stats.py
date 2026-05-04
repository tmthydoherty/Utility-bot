import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import logging
import os
from pathlib import Path
import aiohttp
import json
import re
import pandas as pd
import io
import base64

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

logger = logging.getLogger('league_stats')
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

BASE_DIR = Path("data/league_stats")
DB_PATH = BASE_DIR / "league.db"
LOGOS_DIR = BASE_DIR / "logos"
AGENTS_DIR = BASE_DIR / "agents"
TEMPLATES_DIR = BASE_DIR / "templates"

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    is_active INTEGER DEFAULT 0,
    post_channel_id INTEGER
);

CREATE TABLE IF NOT EXISTS stages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    name TEXT NOT NULL,
    FOREIGN KEY(event_id) REFERENCES events(id)
);

CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    name TEXT NOT NULL,
    abbreviation TEXT,
    logo_path TEXT,
    FOREIGN KEY(event_id) REFERENCES events(id)
);

CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id INTEGER,
    riot_id TEXT NOT NULL,
    is_sub INTEGER DEFAULT 0,
    FOREIGN KEY(team_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER,
    alias_riot_id TEXT NOT NULL,
    FOREIGN KEY(player_id) REFERENCES players(id)
);

CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    stage_id INTEGER,
    team_a_id INTEGER,
    team_b_id INTEGER,
    best_of INTEGER DEFAULT 1,
    FOREIGN KEY(event_id) REFERENCES events(id),
    FOREIGN KEY(stage_id) REFERENCES stages(id),
    FOREIGN KEY(team_a_id) REFERENCES teams(id),
    FOREIGN KEY(team_b_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS match_maps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER,
    map_number INTEGER,
    valorant_match_id TEXT,
    map_name TEXT,
    team_a_score INTEGER,
    team_b_score INTEGER,
    FOREIGN KEY(series_id) REFERENCES series(id)
);

CREATE TABLE IF NOT EXISTS player_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_map_id INTEGER,
    player_riot_id TEXT,
    agent TEXT,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    score INTEGER,
    damage INTEGER,
    headshots INTEGER,
    bodyshots INTEGER,
    legshots INTEGER,
    first_bloods INTEGER,
    plants INTEGER,
    defuses INTEGER,
    c2k INTEGER,
    c3k INTEGER,
    c4k INTEGER,
    c5k INTEGER,
    econ_rating INTEGER,
    first_deaths INTEGER,
    team_color TEXT,
    kast REAL DEFAULT 0,
    FOREIGN KEY(match_map_id) REFERENCES match_maps(id)
);
"""

ALLPLAYERS_HTML = """<!DOCTYPE html>
<html>
<head>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, sans-serif;
            background: #080c14;
            color: #e0e6ed;
            margin: 0;
            padding: 32px;
            width: 2000px;
        }
        .container {
            background: linear-gradient(180deg, #111827 0%, #0c1220 100%);
            border-radius: 20px;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.06);
            box-shadow: 0 20px 60px rgba(0,0,0,0.6);
        }
        .title-bar {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            padding: 22px 32px;
            border-bottom: 2px solid rgba(255,255,255,0.06);
        }
        .title-bar h1 {
            font-size: 36px;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 4px;
            color: #e2e8f0;
        }
        table { width: 100%; border-collapse: collapse; table-layout: fixed; }
        th {
            padding: 16px 8px;
            text-align: center;
            color: #7b8faa;
            font-size: 22px;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            background: #0a0f1a;
            border-bottom: 2px solid rgba(255,255,255,0.08);
            overflow: hidden;
        }
        td {
            padding: 18px 8px;
            text-align: center;
            border-bottom: 1px solid rgba(255,255,255,0.04);
            font-size: 32px;
            font-weight: 700;
            overflow: hidden;
        }
        .player-row { background: rgba(17, 24, 39, 0.5); }
        .player-row:nth-child(even) { background: rgba(12, 18, 32, 0.7); }
        .player-cell {
            display: flex;
            align-items: center;
            text-align: left;
        }
        .player-name {
            font-weight: 900;
            font-size: 34px;
            color: #fff;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .acs { color: #60a5fa; font-weight: 900; font-size: 38px; }
        .kd { font-weight: 900; font-size: 32px; }
        .kd-good { color: #34d399; }
        .kd-bad { color: #f87171; }
        .kda { font-size: 26px; color: #b0bdd0; font-weight: 700; white-space: nowrap; }
        .kda b { color: #fff; font-weight: 900; }
        .plus { color: #34d399; font-weight: 900; font-size: 32px; }
        .minus { color: #f87171; font-weight: 900; font-size: 32px; }
        .stat { font-size: 32px; font-weight: 700; color: #b0bdd0; }
        .hs { font-weight: 700; font-size: 32px; color: #b0bdd0; }
        .kast { color: #60a5fa; font-weight: 800; font-size: 32px; }
        .fb { color: #34d399; font-weight: 800; font-size: 32px; }
        .fd { color: #f87171; font-weight: 800; font-size: 32px; }
        .mk { color: #c084fc; font-weight: 800; font-size: 32px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="title-bar">
            <h1>Player Statistics</h1>
        </div>
        <table>
            <thead>
                <tr>
                    <th style="text-align: left; padding-left: 20px; width: 16%;">Player</th>
                    <th>Maps</th>
                    <th>ACS</th>
                    <th>K/D</th>
                    <th style="width: 16%;">KDA</th>
                    <th>+/-</th>
                    <th>ADR</th>
                    <th>HS%</th>
                    <th>KAST</th>
                    <th>FB</th>
                    <th>FD</th>
                    <th>MK</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </div>
</body>
</html>
"""

TEAMVSTEAM_HTML = """<!DOCTYPE html>
<html>
<head>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, sans-serif;
            background: #080c14;
            color: #e0e6ed;
            margin: 0;
            padding: 32px;
            width: 2000px;
        }
        .team-section {
            background: linear-gradient(180deg, #111827 0%, #0c1220 100%);
            border-radius: 20px;
            padding: 0;
            margin-bottom: 28px;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.06);
            box-shadow: 0 20px 60px rgba(0,0,0,0.6);
        }
        .team-header {
            display: flex;
            align-items: center;
            gap: 24px;
            padding: 24px 32px;
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border-bottom: 2px solid rgba(255,255,255,0.06);
        }
        .team-logo {
            width: 80px;
            height: 80px;
            object-fit: contain;
            border-radius: 12px;
            flex-shrink: 0;
        }
        .team-name {
            font-size: 36px;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 4px;
            color: #e2e8f0;
        }

        table { width: 100%; border-collapse: collapse; table-layout: fixed; }
        th {
            padding: 16px 8px;
            text-align: center;
            color: #7b8faa;
            font-size: 20px;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            background: #0a0f1a;
            border-bottom: 2px solid rgba(255,255,255,0.08);
            overflow: hidden;
        }
        td {
            padding: 18px 8px;
            text-align: center;
            border-bottom: 1px solid rgba(255,255,255,0.04);
            font-size: 28px;
            font-weight: 700;
            overflow: hidden;
        }
        tr:nth-child(odd) { background: rgba(17, 24, 39, 0.5); }
        tr:nth-child(even) { background: rgba(12, 18, 32, 0.7); }

        .player-cell {
            display: flex;
            align-items: center;
            gap: 10px;
            text-align: left;
        }
        .agents { display: flex; gap: 6px; flex-shrink: 0; }
        .agent-icon {
            width: 44px;
            height: 44px;
            border-radius: 50%;
            border: 3px solid #ff4655;
            object-fit: cover;
            background: #0a0f1a;
        }
        .player-name {
            font-weight: 900;
            font-size: 30px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            color: #fff;
            text-shadow: 0 1px 4px rgba(0,0,0,0.2);
        }
        .acs { color: #60a5fa; font-weight: 900; font-size: 34px; }
        .kd { font-weight: 900; font-size: 28px; }
        .kd-good { color: #34d399; }
        .kd-bad { color: #f87171; }
        .kda { font-size: 24px; color: #b0bdd0; font-weight: 700; white-space: nowrap; }
        .kda b { color: #fff; font-weight: 900; }
        .plus { color: #34d399; font-weight: 900; font-size: 28px; }
        .minus { color: #f87171; font-weight: 900; font-size: 28px; }
        .stat { font-size: 28px; font-weight: 700; color: #b0bdd0; }
        .hs { font-weight: 700; font-size: 28px; color: #b0bdd0; }
        .kast { color: #60a5fa; font-weight: 800; font-size: 28px; }
        .fb { color: #34d399; font-weight: 800; font-size: 28px; }
        .fd { color: #f87171; font-weight: 800; font-size: 28px; }
        .mk { color: #c084fc; font-weight: 800; font-size: 28px; }
        .no-data {
            text-align: center;
            padding: 40px;
            color: #4a5568;
            font-size: 24px;
            font-weight: 700;
        }
    </style>
</head>
<body>
    <div class="team-section">
        <div class="team-header">
            <img src="{team_a_logo}" class="team-logo">
            <div class="team-name">{team_a_name}</div>
        </div>
        <table>
            <thead><tr>
                <th style="text-align: left; padding-left: 20px; width: 20%;">Player</th>
                <th>Maps</th><th>ACS</th><th>K/D</th><th>KDA</th><th>+/-</th><th>ADR</th><th>HS%</th><th>KAST</th><th>FB</th><th>FD</th><th>MK</th>
            </tr></thead>
            <tbody>{team_a_rows}</tbody>
        </table>
    </div>
    <div class="team-section">
        <div class="team-header">
            <img src="{team_b_logo}" class="team-logo">
            <div class="team-name">{team_b_name}</div>
        </div>
        <table>
            <thead><tr>
                <th style="text-align: left; padding-left: 20px; width: 20%;">Player</th>
                <th>Maps</th><th>ACS</th><th>K/D</th><th>KDA</th><th>+/-</th><th>ADR</th><th>HS%</th><th>KAST</th><th>FB</th><th>FD</th><th>MK</th>
            </tr></thead>
            <tbody>{team_b_rows}</tbody>
        </table>
    </div>
</body>
</html>
"""

def get_image_base64(filepath) -> str:
    if not filepath: return ""
    filepath = str(filepath)
    if not os.path.exists(filepath): return ""
    try:
        with open(filepath, "rb") as img_file:
            b64_str = base64.b64encode(img_file.read()).decode('utf-8')
            ext = filepath.split('.')[-1].lower()
            mime = "image/png"
            if ext in ['jpg', 'jpeg']: mime = "image/jpeg"
            return f"data:{mime};base64,{b64_str}"
    except Exception as e:
        logger.error(f"Failed to encode image {filepath}: {e}")
        return ""

class DB:
    @staticmethod
    async def init():
        BASE_DIR.mkdir(parents=True, exist_ok=True)
        LOGOS_DIR.mkdir(parents=True, exist_ok=True)
        AGENTS_DIR.mkdir(parents=True, exist_ok=True)
        TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
        
        # Always overwrite templates to keep them in sync
        allplayers_path = TEMPLATES_DIR / "league_allplayers.html"
        teamvsteam_path = TEMPLATES_DIR / "league_teamvsteam.html"
        with open(allplayers_path, "w", encoding="utf-8") as f:
            f.write(ALLPLAYERS_HTML)
        with open(teamvsteam_path, "w", encoding="utf-8") as f:
            f.write(TEAMVSTEAM_HTML)

        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript(SCHEMA)
            # Migrate existing tables
            try:
                await db.execute("ALTER TABLE events ADD COLUMN post_channel_id INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE player_stats ADD COLUMN first_deaths INTEGER DEFAULT 0")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE player_stats ADD COLUMN team_color TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE player_stats ADD COLUMN kast REAL DEFAULT 0")
            except Exception:
                pass
            await db.commit()

    @staticmethod
    async def get_active_event():
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM events WHERE is_active = 1 LIMIT 1") as cursor:
                return await cursor.fetchone()

    @staticmethod
    async def fetch_all(query, params=()):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    @staticmethod
    async def fetch_one(query, params=()):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    @staticmethod
    async def execute(query, params=()):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(query, params)
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def get_all_player_stats(event_id: int):
        query = """
        WITH RECURSIVE
        riot_id_mapping AS (
            SELECT id as player_id, riot_id as match_riot_id, team_id, riot_id as primary_riot_id 
            FROM players
            UNION
            SELECT a.player_id, a.alias_riot_id as match_riot_id, p.team_id, p.riot_id as primary_riot_id
            FROM aliases a
            JOIN players p ON a.player_id = p.id
        )
        SELECT
            COALESCE(m.primary_riot_id, ps.player_riot_id) as primary_riot_id,
            COALESCE(t.name, 'Unknown') as team_name,
            COALESCE(t.abbreviation, 'UNK') as team_abbrev,
            t.logo_path,
            ps.agent,
            ps.kills, ps.deaths, ps.assists, ps.score, ps.damage,
            ps.headshots, ps.bodyshots, ps.legshots,
            ps.first_bloods, ps.first_deaths, ps.plants, ps.defuses,
            ps.c2k, ps.c3k, ps.c4k, ps.c5k, ps.econ_rating, ps.kast,
            ps.match_map_id,
            (mm.team_a_score + mm.team_b_score) as total_rounds,
            mm.series_id, s.stage_id, s.team_a_id, s.team_b_id
        FROM player_stats ps
        JOIN match_maps mm ON ps.match_map_id = mm.id
        JOIN series s ON mm.series_id = s.id
        LEFT JOIN riot_id_mapping m ON LOWER(ps.player_riot_id) = LOWER(m.match_riot_id)
        LEFT JOIN teams t ON m.team_id = t.id
        WHERE s.event_id = ?
        """
        return await DB.fetch_all(query, (event_id,))

    @staticmethod
    async def get_series_stats(series_id: int):
        query = """
        WITH RECURSIVE
        riot_id_mapping AS (
            SELECT id as player_id, riot_id as match_riot_id, team_id, riot_id as primary_riot_id FROM players
            UNION
            SELECT a.player_id, a.alias_riot_id as match_riot_id, p.team_id, p.riot_id as primary_riot_id
            FROM aliases a JOIN players p ON a.player_id = p.id
        )
        SELECT
            COALESCE(m.primary_riot_id, ps.player_riot_id) as primary_riot_id,
            COALESCE(t.id, 0) as team_id,
            COALESCE(t.name, 'Unknown') as team_name,
            COALESCE(t.abbreviation, 'UNK') as team_abbrev,
            t.logo_path,
            ps.agent, ps.team_color, ps.match_map_id,
            ps.kills, ps.deaths, ps.assists, ps.score, ps.damage,
            ps.headshots, ps.bodyshots, ps.legshots,
            ps.first_bloods, ps.first_deaths, ps.kast,
            ps.c2k, ps.c3k, ps.c4k, ps.c5k,
            (mm.team_a_score + mm.team_b_score) as total_rounds
        FROM player_stats ps
        JOIN match_maps mm ON ps.match_map_id = mm.id
        JOIN series s ON mm.series_id = s.id
        LEFT JOIN riot_id_mapping m ON LOWER(ps.player_riot_id) = LOWER(m.match_riot_id)
        LEFT JOIN teams t ON m.team_id = t.id
        WHERE s.id = ?
        """
        rows = await DB.fetch_all(query, (series_id,))

        series = await DB.fetch_one("""
            SELECT s.id, s.team_a_id, s.team_b_id,
                   ta.name as ta_name, ta.logo_path as ta_logo,
                   tb.name as tb_name, tb.logo_path as tb_logo
            FROM series s
            JOIN teams ta ON s.team_a_id = ta.id
            JOIN teams tb ON s.team_b_id = tb.id
            WHERE s.id = ?
        """, (series_id,))

        return series, rows

class PlaywrightGenerator:
    _playwright = None
    _browser = None

    @classmethod
    async def init(cls):
        if not async_playwright:
            logger.warning("Playwright is not installed. Images cannot be generated.")
            return
        if not cls._playwright:
            try:
                cls._playwright = await async_playwright().start()
                cls._browser = await cls._playwright.chromium.launch(headless=True)
                logger.info("League Playwright initialized.")
            except Exception as e:
                logger.error(f"Failed to init playwright in LeagueStats: {e}")

    @classmethod
    async def close(cls):
        try:
            if cls._browser: await cls._browser.close()
            if cls._playwright: await cls._playwright.stop()
        except: pass

    @classmethod
    async def generate_allplayers(cls, event_id: int) -> io.BytesIO:
        if not cls._browser: return None
        stats = await DB.get_all_player_stats(event_id)
        if not stats: return None

        df = pd.DataFrame([dict(r) for r in stats])
        if df.empty: return None

        # Fill NaN kast values with 0 for old data
        df['kast'] = df['kast'].fillna(0)
        # Convert KAST% to round counts for proper weighted aggregation
        df['kast_rounds'] = (df['kast'] * df['total_rounds'] / 100)

        agg_cols = {
            'kills': 'sum', 'deaths': 'sum', 'assists': 'sum', 'score': 'sum',
            'damage': 'sum', 'headshots': 'sum', 'bodyshots': 'sum', 'legshots': 'sum',
            'first_bloods': 'sum', 'first_deaths': 'sum', 'plants': 'sum', 'defuses': 'sum',
            'c2k': 'sum', 'c3k': 'sum', 'c4k': 'sum', 'c5k': 'sum',
            'total_rounds': 'sum', 'kast_rounds': 'sum',
            'match_map_id': 'nunique'
        }
        overall = df.groupby(['primary_riot_id', 'team_name', 'logo_path'], dropna=False).agg(agg_cols).reset_index()
        overall = overall.rename(columns={'match_map_id': 'maps'})

        overall['ACS'] = (overall['score'] / overall['total_rounds'].replace(0, 1)).round(0).astype(int)
        overall['ADR'] = (overall['damage'] / overall['total_rounds'].replace(0, 1)).round(0).astype(int)
        overall['diff'] = overall['kills'] - overall['deaths']
        overall['KD'] = (overall['kills'] / overall['deaths'].replace(0, 1)).round(2)
        total_shots = overall['headshots'] + overall['bodyshots'] + overall['legshots']
        overall['HS%'] = ((overall['headshots'] / total_shots.replace(0, 1)) * 100).round(1)
        overall['KAST'] = ((overall['kast_rounds'] / overall['total_rounds'].replace(0, 1)) * 100).round(1)
        overall = overall.sort_values('ACS', ascending=False)

        rows_html = ""
        for r in overall.to_dict('records'):
            name = str(r['primary_riot_id']).split('#')[0]
            diff = int(r['diff'])
            diff_cls = 'plus' if diff >= 0 else 'minus'
            diff_str = f"+{diff}" if diff >= 0 else str(diff)
            kd = r['KD']
            kd_cls = 'kd kd-good' if kd >= 1.0 else 'kd kd-bad'

            mk_total = int(r['c2k']) + int(r['c3k']) + int(r['c4k']) + int(r['c5k'])

            rows_html += f"""
            <tr class="player-row">
                <td style="padding-left: 20px;"><div class="player-cell"><span class="player-name">{name}</span></div></td>
                <td class="stat">{int(r['maps'])}</td>
                <td class="acs">{r['ACS']}</td>
                <td class="{kd_cls}">{kd:.2f}</td>
                <td class="kda"><b>{int(r['kills'])}</b> / {int(r['deaths'])} / {int(r['assists'])}</td>
                <td class="{diff_cls}">{diff_str}</td>
                <td class="stat">{r['ADR']}</td>
                <td class="hs">{r['HS%']}%</td>
                <td class="kast">{r['KAST']}%</td>
                <td class="fb">{int(r['first_bloods'])}</td>
                <td class="fd">{int(r['first_deaths'])}</td>
                <td class="mk">{mk_total}</td>
            </tr>
            """

        template_path = TEMPLATES_DIR / "league_allplayers.html"
        with open(template_path, 'r', encoding='utf-8') as f:
            html = f.read()

        html = html.replace('{rows}', rows_html)

        page = await cls._browser.new_page(viewport={'width': 2000, 'height': 800}, device_scale_factor=2)
        await page.set_content(html)
        await page.wait_for_timeout(200)
        body_height = await page.evaluate('document.body.scrollHeight')
        await page.set_viewport_size({'width': 2000, 'height': max(body_height, 400)})

        img = await page.screenshot(type='png')
        await page.close()
        return io.BytesIO(img)

    @classmethod
    async def generate_teamvsteam(cls, series_id: int) -> io.BytesIO:
        if not cls._browser: return None
        series, rows = await DB.get_series_stats(series_id)
        if not series or not rows: return None

        df = pd.DataFrame([dict(r) for r in rows])
        if df.empty:
            logger.warning(f"No stats data found for series {series_id}")
            return None

        # Teams swap colors (Red/Blue) between maps in a series, so we must
        # resolve color→team PER MAP, then tag every row with assigned_team.
        team_a_id = series['team_a_id']
        team_b_id = series['team_b_id']
        df['team_color_clean'] = df['team_color'].fillna('').str.strip().str.lower()
        df['assigned_team'] = 0  # will be set to team_a_id or team_b_id

        for map_id, map_df in df.groupby('match_map_id'):
            # Count registered roster players on each color for THIS map
            a_on_red = len(map_df[(map_df['team_id'] == team_a_id) & (map_df['team_color_clean'] == 'red')])
            a_on_blue = len(map_df[(map_df['team_id'] == team_a_id) & (map_df['team_color_clean'] == 'blue')])

            # Whichever color has more of team_a's roster is team_a's color for this map
            if a_on_red >= a_on_blue:
                color_to_team = {'red': team_a_id, 'blue': team_b_id}
            else:
                color_to_team = {'blue': team_a_id, 'red': team_b_id}

            # Assign every row in this map by its color
            for idx in map_df.index:
                color = df.at[idx, 'team_color_clean']
                df.at[idx, 'assigned_team'] = color_to_team.get(color, 0)

        def get_team_df(team_id):
            matched = df[df['assigned_team'] == team_id]
            if not matched.empty:
                return matched
            logger.warning(f"No players assigned to team {team_id} for series {series_id}")
            return pd.DataFrame()

        def build_team_html(team_id):
            try:
                team_df = get_team_df(team_id)
                if team_df.empty:
                    return '<tr><td colspan="12" class="no-data">No stats recorded</td></tr>'

                team_df_copy = team_df.copy()
                team_df_copy['kast'] = team_df_copy['kast'].fillna(0)
                team_df_copy['kast_rounds'] = (team_df_copy['kast'] * team_df_copy['total_rounds'] / 100)

                # get_series_stats doesn't select c2k-c5k, so handle missing columns
                for col in ['c2k', 'c3k', 'c4k', 'c5k']:
                    if col not in team_df_copy.columns:
                        team_df_copy[col] = 0

                grouped = team_df_copy.groupby('primary_riot_id').agg({
                    'kills': 'sum', 'deaths': 'sum', 'assists': 'sum',
                    'score': 'sum', 'damage': 'sum',
                    'headshots': 'sum', 'bodyshots': 'sum', 'legshots': 'sum',
                    'first_bloods': 'sum', 'first_deaths': 'sum',
                    'total_rounds': 'sum', 'kast_rounds': 'sum',
                    'c2k': 'sum', 'c3k': 'sum', 'c4k': 'sum', 'c5k': 'sum',
                    'match_map_id': 'nunique'
                }).reset_index()
                grouped = grouped.rename(columns={'match_map_id': 'maps'})
                grouped['ACS'] = (grouped['score'] / grouped['total_rounds'].replace(0, 1)).round(0).astype(int)
                grouped['ADR'] = (grouped['damage'] / grouped['total_rounds'].replace(0, 1)).round(0).astype(int)
                grouped['diff'] = grouped['kills'] - grouped['deaths']
                grouped['KD'] = (grouped['kills'] / grouped['deaths'].replace(0, 1)).round(2)
                total_shots = grouped['headshots'] + grouped['bodyshots'] + grouped['legshots']
                grouped['HS%'] = ((grouped['headshots'] / total_shots.replace(0, 1)) * 100).round(1)
                grouped['KAST'] = ((grouped['kast_rounds'] / grouped['total_rounds'].replace(0, 1)) * 100).round(1)
                grouped = grouped.sort_values('ACS', ascending=False)

                html = ""
                for r in grouped.to_dict('records'):
                    pid = r['primary_riot_id']
                    name = str(pid).split('#')[0]

                    agents = team_df[team_df['primary_riot_id'] == pid]['agent'].unique()
                    agents_html = '<div class="agents">'
                    for ag in agents[:3]:
                        safe_name = re.sub(r'[^a-zA-Z0-9]', '', str(ag)).lower()
                        icon_path = str(AGENTS_DIR / f"{safe_name}.png")
                        b64 = get_image_base64(icon_path)
                        if b64:
                            agents_html += f'<img src="{b64}" class="agent-icon">'
                        else:
                            agents_html += f'<span style="font-size:14px;color:#4a5568;">{ag}</span>'
                    agents_html += '</div>'

                    diff = int(r['diff'])
                    diff_cls = 'plus' if diff >= 0 else 'minus'
                    diff_str = f"+{diff}" if diff >= 0 else str(diff)
                    kd = r['KD']
                    kd_cls = 'kd kd-good' if kd >= 1.0 else 'kd kd-bad'

                    mk_total = int(r['c2k']) + int(r['c3k']) + int(r['c4k']) + int(r['c5k'])

                    html += f"""
                    <tr>
                        <td style="padding-left: 20px;"><div class="player-cell">{agents_html}<span class="player-name">{name}</span></div></td>
                        <td class="stat">{int(r['maps'])}</td>
                        <td class="acs">{r['ACS']}</td>
                        <td class="{kd_cls}">{kd:.2f}</td>
                        <td class="kda"><b>{int(r['kills'])}</b> / {int(r['deaths'])} / {int(r['assists'])}</td>
                        <td class="{diff_cls}">{diff_str}</td>
                        <td class="stat">{r['ADR']}</td>
                        <td class="hs">{r['HS%']}%</td>
                        <td class="kast">{r['KAST']}%</td>
                        <td class="fb">{int(r['first_bloods'])}</td>
                        <td class="fd">{int(r['first_deaths'])}</td>
                        <td class="mk">{mk_total}</td>
                    </tr>
                    """
                return html
            except Exception as e:
                logger.error(f"Error building team HTML for team {team_id}: {e}")
                return '<tr><td colspan="11" class="no-data">Error loading stats</td></tr>'

        ta_rows = build_team_html(team_a_id)
        tb_rows = build_team_html(team_b_id)

        ta_logo = get_image_base64(series['ta_logo']) or ""
        tb_logo = get_image_base64(series['tb_logo']) or ""

        template_path = TEMPLATES_DIR / "league_teamvsteam.html"
        with open(template_path, 'r', encoding='utf-8') as f:
            html = f.read()

        html = html.replace('{team_a_logo}', ta_logo)
        html = html.replace('{team_a_name}', series['ta_name'])
        html = html.replace('{team_b_logo}', tb_logo)
        html = html.replace('{team_b_name}', series['tb_name'])
        html = html.replace('{team_a_rows}', ta_rows)
        html = html.replace('{team_b_rows}', tb_rows)

        page = await cls._browser.new_page(viewport={'width': 2000, 'height': 800}, device_scale_factor=2)
        await page.set_content(html)
        await page.wait_for_timeout(200)
        body_height = await page.evaluate('document.body.scrollHeight')
        await page.set_viewport_size({'width': 2000, 'height': max(body_height, 400)})

        img = await page.screenshot(type='png')
        await page.close()
        return io.BytesIO(img)

class ExcelGenerator:
    @staticmethod
    async def generate_excel(event_id: int, event_name: str) -> io.BytesIO:
        stats_rows = await DB.get_all_player_stats(event_id)
        if not stats_rows:
            return None

        df = pd.DataFrame([dict(row) for row in stats_rows])
        if df.empty:
            return None

        # Fill NaN kast with 0
        df['kast'] = df['kast'].fillna(0)
        df['kast_rounds'] = (df['kast'] * df['total_rounds'] / 100)

        def build_stats_df(source_df):
            """Aggregate player stats and compute derived columns."""
            grouped = source_df.groupby(['primary_riot_id', 'team_name']).agg({
                'kills': 'sum', 'deaths': 'sum', 'assists': 'sum', 'score': 'sum',
                'damage': 'sum', 'headshots': 'sum', 'bodyshots': 'sum', 'legshots': 'sum',
                'first_bloods': 'sum', 'first_deaths': 'sum', 'plants': 'sum', 'defuses': 'sum',
                'c2k': 'sum', 'c3k': 'sum', 'c4k': 'sum', 'c5k': 'sum',
                'total_rounds': 'sum', 'kast_rounds': 'sum'
            }).reset_index()
            grouped['KD Ratio'] = (grouped['kills'] / grouped['deaths'].replace(0, 1)).round(2)
            grouped['ACS'] = (grouped['score'] / grouped['total_rounds'].replace(0, 1)).round(0).astype(int)
            grouped['ADR'] = (grouped['damage'] / grouped['total_rounds'].replace(0, 1)).round(0).astype(int)
            t_shots = grouped['headshots'] + grouped['bodyshots'] + grouped['legshots']
            grouped['HS%'] = ((grouped['headshots'] / t_shots.replace(0, 1)) * 100).round(1)
            grouped['KAST%'] = ((grouped['kast_rounds'] / grouped['total_rounds'].replace(0, 1)) * 100).round(1)
            # Rename multikill columns
            grouped = grouped.rename(columns={'c2k': '2k', 'c3k': '3k', 'c4k': '4k', 'c5k': '5k'})
            # Drop internal columns
            cols_to_drop = ['score', 'total_rounds', 'kast_rounds', 'series_id', 'stage_id', 'team_a_id', 'team_b_id', 'econ_rating']
            grouped = grouped.drop(columns=[c for c in cols_to_drop if c in grouped.columns], errors='ignore')
            # Reorder columns
            front = ['primary_riot_id', 'team_name', 'ACS', 'KD Ratio', 'kills', 'deaths', 'assists',
                      'ADR', 'HS%', 'KAST%', 'first_bloods', 'first_deaths', 'damage',
                      'headshots', 'bodyshots', 'legshots', 'plants', 'defuses', '2k', '3k', '4k', '5k']
            ordered = [c for c in front if c in grouped.columns] + [c for c in grouped.columns if c not in front]
            return grouped[ordered]

        overall_df = build_stats_df(df)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            overall_df.to_excel(writer, sheet_name='Overall Stats', index=False)

            # Series tabs
            series_ids = df['series_id'].unique()
            for s_id in series_ids:
                s_df = df[df['series_id'] == s_id]
                if s_df.empty: continue

                # Look up actual team names from the series/teams tables
                series_info = await DB.fetch_one(
                    "SELECT ta.abbreviation as ta_abbrev, tb.abbreviation as tb_abbrev "
                    "FROM series s "
                    "JOIN teams ta ON s.team_a_id = ta.id "
                    "JOIN teams tb ON s.team_b_id = tb.id "
                    "WHERE s.id = ?", (int(s_id),)
                )
                if series_info:
                    sheet_name = f"{series_info['ta_abbrev']} v {series_info['tb_abbrev']}"
                else:
                    teams_in_series = s_df['team_abbrev'].dropna().unique()
                    teams_in_series = [t for t in teams_in_series if t != 'UNK']
                    sheet_name = f"{teams_in_series[0]} v {teams_in_series[1]}" if len(teams_in_series) == 2 else f"Series {s_id}"

                s_grouped = build_stats_df(s_df)

                sheet_name = sheet_name[:31] # Excel limit
                s_grouped.to_excel(writer, sheet_name=sheet_name, index=False)

        output.seek(0)
        return output

class HenrikDevLeagueAPI:
    BASE_URL = "https://api.henrikdev.xyz"
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("HENRIK_API_KEY")
    
    async def get_match_details(self, match_id: str) -> dict:
        headers = {}
        if self.api_key:
            headers["Authorization"] = self.api_key
        
        async with aiohttp.ClientSession(headers=headers) as session:
            url = f"{self.BASE_URL}/valorant/v2/match/{match_id}"
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"HenrikDev API Error: {resp.status} - {await resp.text()}")
                    return None

# ==========================================
# UI COMPONENTS
# ==========================================

class CreateEventModal(discord.ui.Modal, title='Create New Event'):
    event_name = discord.ui.TextInput(label='Event Name', placeholder='e.g., Summer League 2026')
    async def on_submit(self, interaction: discord.Interaction):
        event_id = await DB.execute("INSERT INTO events (name) VALUES (?)", (self.event_name.value,))
        await interaction.response.send_message(f"Event '{self.event_name.value}' created! (ID: {event_id})", ephemeral=True)

class AddStageModal(discord.ui.Modal, title='Add Stage'):
    stage_name = discord.ui.TextInput(label='Stage Name', placeholder='e.g., Playday 1, Playoffs')
    def __init__(self, event_id: int):
        super().__init__()
        self.event_id = event_id
    async def on_submit(self, interaction: discord.Interaction):
        stages = await DB.fetch_all("SELECT * FROM stages WHERE event_id = ?", (self.event_id,))
        if len(stages) >= 8:
            await interaction.response.send_message("Maximum of 8 stages allowed per event.", ephemeral=True)
            return
        await DB.execute("INSERT INTO stages (event_id, name) VALUES (?, ?)", (self.event_id, self.stage_name.value))
        await interaction.response.send_message(f"Stage '{self.stage_name.value}' added to the active event.", ephemeral=True)

class AddTeamModal(discord.ui.Modal, title='Add Team'):
    team_name = discord.ui.TextInput(label='Team Name', placeholder='e.g., Sentinels')
    team_abbrev = discord.ui.TextInput(label='Abbreviation', placeholder='e.g., SEN', max_length=5)
    def __init__(self, event_id: int, bot: commands.Bot):
        super().__init__()
        self.event_id = event_id
        self.bot = bot
    async def on_submit(self, interaction: discord.Interaction):
        team_id = await DB.execute(
            "INSERT INTO teams (event_id, name, abbreviation) VALUES (?, ?, ?)",
            (self.event_id, self.team_name.value, self.team_abbrev.value)
        )
        await interaction.response.send_message(
            f"Team '{self.team_name.value}' created!\n\n**To upload a logo**, please send an image in this channel within the next 60 seconds.",
            ephemeral=True
        )
        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id and len(m.attachments) > 0
        try:
            msg = await self.bot.wait_for('message', timeout=60.0, check=check)
            attachment = msg.attachments[0]
            if attachment.content_type.startswith('image/'):
                ext = attachment.filename.split('.')[-1]
                filename = f"team_{team_id}.{ext}"
                filepath = LOGOS_DIR / filename
                await attachment.save(filepath)
                await DB.execute("UPDATE teams SET logo_path = ? WHERE id = ?", (str(filepath), team_id))
                await msg.reply("✅ Team logo saved successfully!", delete_after=10)
                await msg.delete(delay=5)
            else:
                await msg.reply("❌ That doesn't look like a valid image.", delete_after=10)
        except asyncio.TimeoutError:
            await interaction.followup.send("⏳ Logo upload timed out. You can update it later.", ephemeral=True)

class AddPlayersModal(discord.ui.Modal, title='Batch Add Players'):
    players_list = discord.ui.TextInput(
        label='Riot IDs (One per line)', 
        style=discord.TextStyle.paragraph, 
        placeholder='PlayerOne#NA1\nPlayerTwo#1234'
    )
    def __init__(self, team_id: int):
        super().__init__()
        self.team_id = team_id
    async def on_submit(self, interaction: discord.Interaction):
        lines = self.players_list.value.split('\n')
        added = 0
        for line in lines:
            riot_id = line.strip()
            if riot_id:
                await DB.execute("INSERT INTO players (team_id, riot_id) VALUES (?, ?)", (self.team_id, riot_id))
                added += 1
        await interaction.response.send_message(f"Added {added} players to the team.", ephemeral=True)

class AddAliasModal(discord.ui.Modal, title='Add Alias'):
    primary_id = discord.ui.TextInput(label='Primary Riot ID', placeholder='MainAccount#NA1')
    alias_id = discord.ui.TextInput(label='Alias/Smurf Riot ID', placeholder='SmurfAccount#1234')
    def __init__(self, team_id: int):
        super().__init__()
        self.team_id = team_id
    async def on_submit(self, interaction: discord.Interaction):
        player = await DB.fetch_one("SELECT * FROM players WHERE team_id = ? AND riot_id = ? COLLATE NOCASE", (self.team_id, self.primary_id.value.strip()))
        if not player:
            await interaction.response.send_message("❌ Could not find the primary Riot ID in this team.", ephemeral=True)
            return
        await DB.execute("INSERT INTO aliases (player_id, alias_riot_id) VALUES (?, ?)", (player['id'], self.alias_id.value.strip()))
        await interaction.response.send_message(f"✅ Alias '{self.alias_id.value}' added for '{self.primary_id.value}'.", ephemeral=True)

class MatchLinksModal(discord.ui.Modal, title='Add Match Links'):
    link1 = discord.ui.TextInput(label='Map 1 Tracker.gg Link', required=True)
    link2 = discord.ui.TextInput(label='Map 2 Tracker.gg Link', required=False)
    link3 = discord.ui.TextInput(label='Map 3 Tracker.gg Link', required=False)
    link4 = discord.ui.TextInput(label='Map 4 Tracker.gg Link', required=False)
    link5 = discord.ui.TextInput(label='Map 5 Tracker.gg Link', required=False)

    def __init__(self, event_id: int, stage_id: int, team_a_id: int, team_b_id: int, best_of: int, api: HenrikDevLeagueAPI):
        super().__init__()
        self.event_id = event_id
        self.stage_id = stage_id
        self.team_a_id = team_a_id
        self.team_b_id = team_b_id
        self.best_of = best_of
        self.api = api

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        links = [self.link1.value, self.link2.value, self.link3.value, self.link4.value, self.link5.value]
        uuids = []
        for link in links:
            if link:
                match = re.search(r"match/([a-fA-F0-9\-]{36})", link)
                if match:
                    uuids.append(match.group(1))

        if not uuids:
            await interaction.followup.send("❌ No valid match UUIDs found in the provided links.")
            return

        series_id = await DB.execute(
            "INSERT INTO series (event_id, stage_id, team_a_id, team_b_id, best_of) VALUES (?, ?, ?, ?, ?)",
            (self.event_id, self.stage_id, self.team_a_id, self.team_b_id, self.best_of)
        )

        success_count = 0
        for i, uuid in enumerate(uuids):
            data = await self.api.get_match_details(uuid)
            if not data or 'data' not in data: continue
            
            match_data = data['data']
            metadata = match_data['metadata']
            players = match_data['players']['all_players']
            
            map_name = metadata.get('map', 'Unknown')
            rounds = match_data.get('rounds', [])
            team_a_score, team_b_score = 0, 0
            if rounds:
                team_a_score = sum(1 for r in rounds if r['winning_team'] == 'Red')
                team_b_score = sum(1 for r in rounds if r['winning_team'] == 'Blue')

            map_id = await DB.execute(
                "INSERT INTO match_maps (series_id, map_number, valorant_match_id, map_name, team_a_score, team_b_score) VALUES (?, ?, ?, ?, ?, ?)",
                (series_id, i+1, uuid, map_name, team_a_score, team_b_score)
            )

            # Build puuid → riot_id mapping
            puuid_map = {}
            for p in players:
                puuid_map[p.get('puuid', '')] = f"{p['name']}#{p['tag']}"

            # Extract per-player round stats: first bloods, first deaths, plants, defuses, multikills
            fb_counts = {}
            fd_counts = {}
            plant_counts = {}
            defuse_counts = {}
            mk_counts = {}  # {ign: {2:n, 3:n, 4:n, 5:n}}

            for rd in rounds:
                all_kills = []
                for ps in rd.get('player_stats', []):
                    pu = ps.get('player_puuid', '') or ps.get('puuid', '')
                    ign = puuid_map.get(pu, pu)
                    kill_events = ps.get('kill_events', [])
                    kill_count = len(kill_events)

                    if kill_count >= 2:
                        mk_counts.setdefault(ign, {2: 0, 3: 0, 4: 0, 5: 0})
                        mk_counts[ign][min(kill_count, 5)] += 1

                    for ke in kill_events:
                        all_kills.append({
                            'killer': ign,
                            'victim': puuid_map.get(ke.get('victim_puuid', ''), ''),
                            'time': ke.get('kill_time_in_round', 0)
                        })

                if all_kills:
                    all_kills.sort(key=lambda k: k.get('time', 0))
                    fb_counts[all_kills[0]['killer']] = fb_counts.get(all_kills[0]['killer'], 0) + 1
                    if all_kills[0]['victim']:
                        fd_counts[all_kills[0]['victim']] = fd_counts.get(all_kills[0]['victim'], 0) + 1

                pe = rd.get('plant_events') or {}
                planted_by = pe.get('planted_by') if isinstance(pe, dict) else None
                if planted_by and isinstance(planted_by, dict) and planted_by.get('puuid'):
                    planter = puuid_map.get(planted_by['puuid'], '')
                    if planter:
                        plant_counts[planter] = plant_counts.get(planter, 0) + 1

                de = rd.get('defuse_events') or {}
                defused_by = de.get('defused_by') if isinstance(de, dict) else None
                if defused_by and isinstance(defused_by, dict) and defused_by.get('puuid'):
                    defuser = puuid_map.get(defused_by['puuid'], '')
                    if defuser:
                        defuse_counts[defuser] = defuse_counts.get(defuser, 0) + 1

            # Calculate KAST% per player for this map
            kast_counts = {}  # {ign: rounds_with_kast}
            all_igns = set(puuid_map.values())
            for ign in all_igns:
                kast_counts[ign] = 0

            for rd in rounds:
                round_kills = []  # (killer, victim, time, killer_team, victim_team)
                round_deaths = set()  # igns who died
                round_killers = set()  # igns who got a kill
                round_assistants = set()  # igns who assisted

                for ps in rd.get('player_stats', []):
                    pu = ps.get('player_puuid', '') or ps.get('puuid', '')
                    ign = puuid_map.get(pu, pu)
                    kill_events = ps.get('kill_events', [])

                    for ke in kill_events:
                        killer = ign
                        victim = puuid_map.get(ke.get('victim_puuid', ''), '')
                        kill_time = ke.get('kill_time_in_round', 0)
                        killer_team = ke.get('killer_team', '')
                        victim_team = ke.get('victim_team', '')
                        round_kills.append((killer, victim, kill_time, killer_team, victim_team))
                        round_killers.add(killer)
                        if victim:
                            round_deaths.add(victim)
                        for ast in ke.get('assistants', []):
                            ast_ign = puuid_map.get(ast.get('puuid', ''), ast.get('display_name', ''))
                            if ast_ign:
                                round_assistants.add(ast_ign)

                round_kills.sort(key=lambda x: x[2])

                # Build trade set: if A kills B, then someone kills A within 5s, B was "traded"
                traded = set()
                for i_k, (killer, victim, ktime, k_team, v_team) in enumerate(round_kills):
                    if not victim:
                        continue
                    # Look for a subsequent kill where the killer is killed within 5s
                    for j_k, (killer2, victim2, ktime2, k_team2, v_team2) in enumerate(round_kills):
                        if j_k <= i_k:
                            continue
                        if victim2 == killer and 0 < (ktime2 - ktime) <= 5000:
                            traded.add(victim)  # The original victim gets T credit (was traded out)
                            break

                for ign in all_igns:
                    k = ign in round_killers
                    a = ign in round_assistants
                    s = ign not in round_deaths
                    t = ign in traded
                    if k or a or s or t:
                        kast_counts[ign] = kast_counts.get(ign, 0) + 1

            total_rounds_count = len(rounds) if rounds else 1
            kast_pcts = {ign: round((count / total_rounds_count) * 100, 1) for ign, count in kast_counts.items()}

            for p in players:
                ign = f"{p['name']}#{p['tag']}"
                stats = p['stats']
                mk = mk_counts.get(ign, {2: 0, 3: 0, 4: 0, 5: 0})
                team_color = p.get('team', '')
                await DB.execute("""
                    INSERT INTO player_stats (
                        match_map_id, player_riot_id, agent, kills, deaths, assists, score, damage,
                        headshots, bodyshots, legshots, first_bloods, first_deaths, plants, defuses,
                        c2k, c3k, c4k, c5k, econ_rating, team_color, kast
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    map_id, ign, p['character'], stats['kills'], stats['deaths'], stats['assists'],
                    stats['score'], p.get('damage_made', 0), stats['headshots'], stats['bodyshots'], stats['legshots'],
                    fb_counts.get(ign, 0), fd_counts.get(ign, 0),
                    plant_counts.get(ign, 0), defuse_counts.get(ign, 0),
                    mk.get(2, 0), mk.get(3, 0), mk.get(4, 0), mk.get(5, 0), 0, team_color,
                    kast_pcts.get(ign, 0)
                ))
            success_count += 1
        await interaction.followup.send(f"Series created and imported {success_count}/{len(uuids)} maps.")

class AddSeriesView(discord.ui.View):
    def __init__(self, event_id: int, stages: list, teams: list, api: HenrikDevLeagueAPI, parent_view=None):
        super().__init__(timeout=300)
        self.event_id = event_id
        self.api = api
        self.parent_view = parent_view
        self.stage_id = None
        self.team_a_id = None
        self.team_b_id = None
        self.best_of = 1
        self.stage_name = None
        self.team_a_name = None
        self.team_b_name = None

        self._stages_map = {str(s['id']): s['name'] for s in stages}
        self._teams_map = {str(t['id']): t['name'] for t in teams}

        stage_options = [discord.SelectOption(label=s['name'], value=str(s['id'])) for s in stages]
        self.stage_select = discord.ui.Select(placeholder="Select Stage", options=stage_options, row=0)
        self.stage_select.callback = self.on_stage_select
        self.add_item(self.stage_select)

        team_options = [discord.SelectOption(label=t['name'], value=str(t['id'])) for t in teams][:25]
        self.team_a_select = discord.ui.Select(placeholder="Select Team A", options=team_options, row=1)
        self.team_a_select.callback = self.on_team_a_select
        self.add_item(self.team_a_select)

        self.team_b_select = discord.ui.Select(placeholder="Select Team B", options=team_options, row=2)
        self.team_b_select.callback = self.on_team_b_select
        self.add_item(self.team_b_select)

        self.bo_select = discord.ui.Select(placeholder="Best Of", options=[
            discord.SelectOption(label="Best of 1", value="1"),
            discord.SelectOption(label="Best of 3", value="3"),
            discord.SelectOption(label="Best of 5", value="5"),
        ], row=3)
        self.bo_select.callback = self.on_bo_select
        self.add_item(self.bo_select)

        self.continue_btn = discord.ui.Button(label="Continue to Links", style=discord.ButtonStyle.green, row=4, disabled=True)
        self.continue_btn.callback = self.on_continue
        self.add_item(self.continue_btn)

        if self.parent_view:
            self.back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
            self.back_btn.callback = self.on_back
            self.add_item(self.back_btn)

    def build_embed(self):
        stage = self.stage_name or "*Not selected*"
        team_a = self.team_a_name or "*Not selected*"
        team_b = self.team_b_name or "*Not selected*"
        bo = f"Best of {self.best_of}"

        desc = (
            f"**Stage:** {stage}\n"
            f"**Team A:** {team_a}\n"
            f"**Team B:** {team_b}\n"
            f"**Format:** {bo}"
        )

        if self.team_a_id and self.team_b_id and self.team_a_id == self.team_b_id:
            desc += "\n\n⚠️ Team A and Team B cannot be the same."

        return discord.Embed(title="Add Match/Series", description=desc, color=discord.Color.green())

    async def on_back(self, interaction: discord.Interaction):
        await interaction.response.edit_message(embed=self.parent_view.main_embed(), view=self.parent_view)

    async def on_stage_select(self, interaction: discord.Interaction):
        self.stage_id = int(self.stage_select.values[0])
        self.stage_name = self._stages_map.get(self.stage_select.values[0])
        self.check_ready()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)
    async def on_team_a_select(self, interaction: discord.Interaction):
        self.team_a_id = int(self.team_a_select.values[0])
        self.team_a_name = self._teams_map.get(self.team_a_select.values[0])
        self.check_ready()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)
    async def on_team_b_select(self, interaction: discord.Interaction):
        self.team_b_id = int(self.team_b_select.values[0])
        self.team_b_name = self._teams_map.get(self.team_b_select.values[0])
        self.check_ready()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)
    async def on_bo_select(self, interaction: discord.Interaction):
        self.best_of = int(self.bo_select.values[0])
        self.check_ready()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    def check_ready(self):
        if self.stage_id and self.team_a_id and self.team_b_id and self.team_a_id != self.team_b_id:
            self.continue_btn.disabled = False
        else:
            self.continue_btn.disabled = True

    async def on_continue(self, interaction: discord.Interaction):
        modal = MatchLinksModal(self.event_id, self.stage_id, self.team_a_id, self.team_b_id, self.best_of, self.api)
        await interaction.response.send_modal(modal)

class StatsAdminPanel(discord.ui.View):
    def __init__(self, bot: commands.Bot, api: HenrikDevLeagueAPI, event_id: int = None, event_name: str = None):
        super().__init__(timeout=None)
        self.bot = bot
        self.api = api
        self.event_id = event_id
        self.event_name = event_name

    @property
    def has_event(self):
        return self.event_id is not None

    def _no_event_embed(self, action: str):
        return discord.Embed(
            title="🏆 League Stats Admin Panel",
            description=f"⚠️ **{action}** requires an active event.\nUse **Manage Events** to create or select one.",
            color=discord.Color.orange()
        )

    def main_embed(self):
        if self.has_event:
            desc = f"Active Event: **{self.event_name}**\n\nManage your league, publish stats, and export data."
        else:
            desc = "⚠️ No active event set.\nUse **Manage Events** to create or select one."
        return discord.Embed(
            title="🏆 League Stats Admin Panel",
            description=desc,
            color=discord.Color.blue()
        )

    # ── League Management Buttons (Row 0) ──

    @discord.ui.button(label="Manage Events", style=discord.ButtonStyle.blurple, custom_id="league_btn_events", row=0)
    async def btn_events(self, interaction: discord.Interaction, button: discord.ui.Button):
        events = await DB.fetch_all("SELECT * FROM events")
        view = discord.ui.View(timeout=None)

        create_btn = discord.ui.Button(label="Create Event", style=discord.ButtonStyle.green)
        async def on_create(i: discord.Interaction):
            await i.response.send_modal(CreateEventModal())
        create_btn.callback = on_create
        view.add_item(create_btn)

        if events:
            options = [discord.SelectOption(label=e['name'], value=str(e['id']), default=bool(e['is_active'])) for e in events]
            select = discord.ui.Select(placeholder="Select Active Event", options=options)
            async def on_select(i: discord.Interaction):
                event_id = int(select.values[0])
                await DB.execute("UPDATE events SET is_active = 0")
                await DB.execute("UPDATE events SET is_active = 1 WHERE id = ?", (event_id,))
                event = await DB.fetch_one("SELECT * FROM events WHERE id = ?", (event_id,))
                self.event_id = event_id
                self.event_name = event['name']
                await i.response.edit_message(embed=self.main_embed(), view=self)
            select.callback = on_select
            view.add_item(select)

        back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
        async def on_back(i: discord.Interaction):
            await i.response.edit_message(embed=self.main_embed(), view=self)
        back_btn.callback = on_back
        view.add_item(back_btn)

        embed = discord.Embed(title="Manage Events", description="Create a new event or select an active one.", color=discord.Color.blurple())
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Manage Stages", style=discord.ButtonStyle.blurple, custom_id="league_btn_stages", row=0)
    async def btn_stages(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.has_event:
            await interaction.response.edit_message(embed=self._no_event_embed("Manage Stages"), view=self)
            return
        stages = await DB.fetch_all("SELECT * FROM stages WHERE event_id = ?", (self.event_id,))
        view = discord.ui.View(timeout=None)

        add_btn = discord.ui.Button(label="Add Stage", style=discord.ButtonStyle.green)
        async def on_add(i: discord.Interaction):
            await i.response.send_modal(AddStageModal(self.event_id))
        add_btn.callback = on_add
        view.add_item(add_btn)

        back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
        async def on_back(i: discord.Interaction):
            await i.response.edit_message(embed=self.main_embed(), view=self)
        back_btn.callback = on_back
        view.add_item(back_btn)

        stage_list = "\n".join(f"• {s['name']}" for s in stages) if stages else "No stages yet."
        embed = discord.Embed(title="Manage Stages", description=stage_list, color=discord.Color.blurple())
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Manage Teams", style=discord.ButtonStyle.blurple, custom_id="league_btn_teams", row=0)
    async def btn_teams(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.has_event:
            await interaction.response.edit_message(embed=self._no_event_embed("Manage Teams"), view=self)
            return
        await interaction.response.send_modal(AddTeamModal(self.event_id, self.bot))

    # ── League Management Buttons (Row 1) ──

    @discord.ui.button(label="Manage Players/Aliases", style=discord.ButtonStyle.blurple, custom_id="league_btn_players", row=1)
    async def btn_players(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.has_event:
            await interaction.response.edit_message(embed=self._no_event_embed("Manage Players"), view=self)
            return
        teams = await DB.fetch_all("SELECT * FROM teams WHERE event_id = ?", (self.event_id,))
        if not teams:
            embed = discord.Embed(title="⚠️ No Teams", description="Create some teams first.", color=discord.Color.orange())
            await interaction.response.edit_message(embed=embed, view=self)
            return

        view = discord.ui.View(timeout=None)
        team_options = [discord.SelectOption(label=t['name'], value=str(t['id'])) for t in teams][:25]

        add_players_select = discord.ui.Select(placeholder="Add Players to Team...", options=team_options, row=0)
        async def on_add_players(i: discord.Interaction):
            team_id = int(add_players_select.values[0])
            await i.response.send_modal(AddPlayersModal(team_id))
        add_players_select.callback = on_add_players
        view.add_item(add_players_select)

        add_alias_select = discord.ui.Select(placeholder="Add Alias for Player in Team...", options=team_options, row=1)
        async def on_add_alias(i: discord.Interaction):
            team_id = int(add_alias_select.values[0])
            await i.response.send_modal(AddAliasModal(team_id))
        add_alias_select.callback = on_add_alias
        view.add_item(add_alias_select)

        back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
        async def on_back(i: discord.Interaction):
            await i.response.edit_message(embed=self.main_embed(), view=self)
        back_btn.callback = on_back
        view.add_item(back_btn)

        embed = discord.Embed(title="Manage Players", description="Select a team to add players or aliases.", color=discord.Color.blurple())
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Add Match/Series", style=discord.ButtonStyle.green, custom_id="league_btn_add_match", row=1)
    async def btn_add_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.has_event:
            await interaction.response.edit_message(embed=self._no_event_embed("Add Match/Series"), view=self)
            return
        stages = await DB.fetch_all("SELECT * FROM stages WHERE event_id = ?", (self.event_id,))
        teams = await DB.fetch_all("SELECT * FROM teams WHERE event_id = ?", (self.event_id,))
        if not stages or not teams:
            embed = discord.Embed(title="⚠️ Missing Data", description="You need to create stages and teams first.", color=discord.Color.orange())
            await interaction.response.edit_message(embed=embed, view=self)
            return

        view = AddSeriesView(self.event_id, stages, teams, self.api, parent_view=self)
        await interaction.response.edit_message(embed=view.build_embed(), view=view)

    # ── Stats / Channel Buttons (Row 2) ──

    @discord.ui.button(label="Post Stats", style=discord.ButtonStyle.primary, custom_id="stat_btn_post", row=2)
    async def btn_post(self, interaction: discord.Interaction, button: discord.ui.Button):
        events = await DB.fetch_all("SELECT * FROM events")
        if not events:
            await interaction.response.edit_message(embed=self._no_event_embed("Post Stats"), view=self)
            return

        view = discord.ui.View(timeout=None)
        options = [discord.SelectOption(label=e['name'], value=str(e['id'])) for e in events]
        event_select = discord.ui.Select(placeholder="Select event to post stats for...", options=options, row=0)
        panel_self = self

        async def on_event(i: discord.Interaction):
            ev_id = int(event_select.values[0])
            ev = await DB.fetch_one("SELECT * FROM events WHERE id = ?", (ev_id,))
            if not ev or not ev['post_channel_id']:
                await i.response.edit_message(
                    embed=discord.Embed(title="⚠️ No Post Channel", description="Set a post channel for this event first.", color=discord.Color.orange()),
                    view=panel_self
                )
                return

            await i.response.defer(ephemeral=True)
            img_bytes = await PlaywrightGenerator.generate_allplayers(ev_id)
            if not img_bytes:
                await i.followup.send("Failed to generate image. No data found.", ephemeral=True)
                return

            channel = panel_self.bot.get_channel(ev['post_channel_id'])
            if not channel:
                await i.followup.send("Post channel not found. It may have been deleted.", ephemeral=True)
                return

            file = discord.File(img_bytes, filename="allplayers.png")
            embed = discord.Embed(color=discord.Color.red())
            embed.set_image(url="attachment://allplayers.png")

            # Build public stage dropdown
            public_view = await StatsPublicView.create(ev_id, ev['name'])
            await channel.send(embed=embed, file=file, view=public_view)
            await i.followup.send(f"✅ Stats posted to {channel.mention}.", ephemeral=True)

        event_select.callback = on_event
        view.add_item(event_select)

        back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
        async def on_back(i: discord.Interaction):
            await i.response.edit_message(embed=panel_self.main_embed(), view=panel_self)
        back_btn.callback = on_back
        view.add_item(back_btn)

        embed = discord.Embed(title="Post Stats", description="Select which event to post stats for.\nStats will be sent to the event's configured post channel.", color=discord.Color.blue())
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Preview Stats", style=discord.ButtonStyle.secondary, custom_id="stat_btn_preview", row=2)
    async def btn_preview(self, interaction: discord.Interaction, button: discord.ui.Button):
        events = await DB.fetch_all("SELECT * FROM events")
        if not events:
            await interaction.response.edit_message(embed=self._no_event_embed("Preview Stats"), view=self)
            return

        view = discord.ui.View(timeout=None)
        options = [discord.SelectOption(label=e['name'], value=str(e['id'])) for e in events]
        event_select = discord.ui.Select(placeholder="Select event to preview stats for...", options=options, row=0)
        panel_self = self

        async def on_event(i: discord.Interaction):
            ev_id = int(event_select.values[0])
            ev = await DB.fetch_one("SELECT * FROM events WHERE id = ?", (ev_id,))

            await i.response.defer(ephemeral=True)
            img_bytes = await PlaywrightGenerator.generate_allplayers(ev_id)
            if not img_bytes:
                await i.followup.send("Failed to generate image. No data found.", ephemeral=True)
                return

            file = discord.File(img_bytes, filename="allplayers.png")
            embed = discord.Embed(color=discord.Color.red())
            embed.set_image(url="attachment://allplayers.png")

            public_view = await StatsPublicView.create(ev_id, ev['name'])
            await i.followup.send(embed=embed, file=file, view=public_view, ephemeral=True)

        event_select.callback = on_event
        view.add_item(event_select)

        back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
        async def on_back(i: discord.Interaction):
            await i.response.edit_message(embed=panel_self.main_embed(), view=panel_self)
        back_btn.callback = on_back
        view.add_item(back_btn)

        embed = discord.Embed(title="Preview Stats", description="Select which event to preview.\nStats will be shown as a private message in this channel.", color=discord.Color.blue())
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Set Post Channel", style=discord.ButtonStyle.secondary, custom_id="stat_btn_channel", row=2)
    async def btn_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        events = await DB.fetch_all("SELECT * FROM events")
        if not events:
            await interaction.response.edit_message(embed=self._no_event_embed("Set Post Channel"), view=self)
            return

        view = discord.ui.View(timeout=None)
        options = [discord.SelectOption(label=e['name'], value=str(e['id'])) for e in events]
        event_select = discord.ui.Select(placeholder="Select event...", options=options, row=0)
        panel_self = self

        async def on_event(i: discord.Interaction):
            ev_id = int(event_select.values[0])
            ev = await DB.fetch_one("SELECT * FROM events WHERE id = ?", (ev_id,))

            ch_view = discord.ui.View(timeout=None)
            ch_select = discord.ui.ChannelSelect(placeholder="Select post channel...", channel_types=[discord.ChannelType.text], row=0)

            async def on_channel(i2: discord.Interaction):
                channel_id = ch_select.values[0].id
                await DB.execute("UPDATE events SET post_channel_id = ? WHERE id = ?", (channel_id, ev_id))
                if panel_self.event_id == ev_id:
                    pass  # same event, no state change needed
                await i2.response.edit_message(
                    embed=discord.Embed(title="✅ Channel Set", description=f"Post channel for **{ev['name']}** set to <#{channel_id}>.", color=discord.Color.green()),
                    view=panel_self
                )

            ch_select.callback = on_channel
            ch_view.add_item(ch_select)

            back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
            async def on_back(i2: discord.Interaction):
                await i2.response.edit_message(embed=panel_self.main_embed(), view=panel_self)
            back_btn.callback = on_back
            ch_view.add_item(back_btn)

            current = f"Current: <#{ev['post_channel_id']}>" if ev['post_channel_id'] else "No channel set."
            embed = discord.Embed(title=f"Set Post Channel — {ev['name']}", description=current, color=discord.Color.blue())
            await i.response.edit_message(embed=embed, view=ch_view)

        event_select.callback = on_event
        view.add_item(event_select)

        back_btn = discord.ui.Button(label="⬅ Back", style=discord.ButtonStyle.secondary, row=4)
        async def on_back(i: discord.Interaction):
            await i.response.edit_message(embed=self.main_embed(), view=self)
        back_btn.callback = on_back
        view.add_item(back_btn)

        embed = discord.Embed(title="Set Post Channel", description="Select an event to configure its post channel.", color=discord.Color.blue())
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Refresh Match Data", style=discord.ButtonStyle.danger, custom_id="stat_btn_refresh", row=3)
    async def btn_refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.has_event:
            await interaction.response.edit_message(embed=self._no_event_embed("Refresh"), view=self)
            return

        await interaction.response.edit_message(
            embed=discord.Embed(title="⏳ Refreshing Match Data...", description="Re-fetching all matches from the API. This may take a moment.", color=discord.Color.orange()),
            view=self
        )
        # Get all match maps for this event
        maps = await DB.fetch_all("""
            SELECT mm.id, mm.valorant_match_id
            FROM match_maps mm
            JOIN series s ON mm.series_id = s.id
            WHERE s.event_id = ? AND mm.valorant_match_id IS NOT NULL
        """, (self.event_id,))

        if not maps:
            await interaction.edit_original_response(embed=self.main_embed(), view=self)
            await interaction.followup.send("No matches found to refresh.", ephemeral=True)
            return

        refreshed = 0
        for mm in maps:
            data = await self.api.get_match_details(mm['valorant_match_id'])
            if not data or 'data' not in data:
                continue

            match_data = data['data']
            players = match_data['players']['all_players']
            rounds = match_data.get('rounds', [])

            # Recalculate scores
            team_a_score = sum(1 for r in rounds if r['winning_team'] == 'Red')
            team_b_score = sum(1 for r in rounds if r['winning_team'] == 'Blue')
            await DB.execute(
                "UPDATE match_maps SET team_a_score = ?, team_b_score = ? WHERE id = ?",
                (team_a_score, team_b_score, mm['id'])
            )

            # Build puuid → riot_id mapping
            puuid_map = {}
            for p in players:
                puuid_map[p.get('puuid', '')] = f"{p['name']}#{p['tag']}"

            # Extract round-level stats
            fb_counts, fd_counts, plant_counts, defuse_counts, mk_counts = {}, {}, {}, {}, {}
            for rd in rounds:
                all_kills = []
                for ps in rd.get('player_stats', []):
                    pu = ps.get('player_puuid', '') or ps.get('puuid', '')
                    ign = puuid_map.get(pu, pu)
                    kill_events = ps.get('kill_events', [])
                    kill_count = len(kill_events)
                    if kill_count >= 2:
                        mk_counts.setdefault(ign, {2: 0, 3: 0, 4: 0, 5: 0})
                        mk_counts[ign][min(kill_count, 5)] += 1
                    for ke in kill_events:
                        all_kills.append({
                            'killer': ign,
                            'victim': puuid_map.get(ke.get('victim_puuid', ''), ''),
                            'time': ke.get('kill_time_in_round', 0)
                        })
                if all_kills:
                    all_kills.sort(key=lambda k: k.get('time', 0))
                    fb_counts[all_kills[0]['killer']] = fb_counts.get(all_kills[0]['killer'], 0) + 1
                    if all_kills[0]['victim']:
                        fd_counts[all_kills[0]['victim']] = fd_counts.get(all_kills[0]['victim'], 0) + 1
                pe = rd.get('plant_events') or {}
                planted_by = pe.get('planted_by') if isinstance(pe, dict) else None
                if planted_by and isinstance(planted_by, dict) and planted_by.get('puuid'):
                    planter = puuid_map.get(planted_by['puuid'], '')
                    if planter:
                        plant_counts[planter] = plant_counts.get(planter, 0) + 1
                de = rd.get('defuse_events') or {}
                defused_by = de.get('defused_by') if isinstance(de, dict) else None
                if defused_by and isinstance(defused_by, dict) and defused_by.get('puuid'):
                    defuser = puuid_map.get(defused_by['puuid'], '')
                    if defuser:
                        defuse_counts[defuser] = defuse_counts.get(defuser, 0) + 1

            # Calculate KAST% per player for this map
            kast_counts = {}
            all_igns = set(puuid_map.values())
            for ign in all_igns:
                kast_counts[ign] = 0

            for rd in rounds:
                round_kills = []
                round_deaths = set()
                round_killers = set()
                round_assistants = set()

                for ps_r in rd.get('player_stats', []):
                    pu = ps_r.get('player_puuid', '') or ps_r.get('puuid', '')
                    ign = puuid_map.get(pu, pu)
                    for ke in ps_r.get('kill_events', []):
                        killer = ign
                        victim = puuid_map.get(ke.get('victim_puuid', ''), '')
                        kill_time = ke.get('kill_time_in_round', 0)
                        round_kills.append((killer, victim, kill_time))
                        round_killers.add(killer)
                        if victim:
                            round_deaths.add(victim)
                        for ast in ke.get('assistants', []):
                            ast_ign = puuid_map.get(ast.get('puuid', ''), ast.get('display_name', ''))
                            if ast_ign:
                                round_assistants.add(ast_ign)

                round_kills.sort(key=lambda x: x[2])
                traded = set()
                for i_k, (killer, victim, ktime) in enumerate(round_kills):
                    if not victim:
                        continue
                    for j_k, (killer2, victim2, ktime2) in enumerate(round_kills):
                        if j_k <= i_k:
                            continue
                        if victim2 == killer and 0 < (ktime2 - ktime) <= 5000:
                            traded.add(victim)  # The original victim gets T credit (was traded out)
                            break

                for ign in all_igns:
                    if ign in round_killers or ign in round_assistants or ign not in round_deaths or ign in traded:
                        kast_counts[ign] = kast_counts.get(ign, 0) + 1

            total_rounds_count = len(rounds) if rounds else 1
            kast_pcts = {ign: round((count / total_rounds_count) * 100, 1) for ign, count in kast_counts.items()}

            # Delete old stats and re-insert with full data
            await DB.execute("DELETE FROM player_stats WHERE match_map_id = ?", (mm['id'],))
            for p in players:
                ign = f"{p['name']}#{p['tag']}"
                stats = p['stats']
                mk = mk_counts.get(ign, {2: 0, 3: 0, 4: 0, 5: 0})
                team_color = p.get('team', '')
                await DB.execute("""
                    INSERT INTO player_stats (
                        match_map_id, player_riot_id, agent, kills, deaths, assists, score, damage,
                        headshots, bodyshots, legshots, first_bloods, first_deaths, plants, defuses,
                        c2k, c3k, c4k, c5k, econ_rating, team_color, kast
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    mm['id'], ign, p['character'], stats['kills'], stats['deaths'], stats['assists'],
                    stats['score'], p.get('damage_made', 0), stats['headshots'], stats['bodyshots'], stats['legshots'],
                    fb_counts.get(ign, 0), fd_counts.get(ign, 0),
                    plant_counts.get(ign, 0), defuse_counts.get(ign, 0),
                    mk.get(2, 0), mk.get(3, 0), mk.get(4, 0), mk.get(5, 0), 0, team_color,
                    kast_pcts.get(ign, 0)
                ))
            refreshed += 1

        # Regenerate and repost the stats image to the configured channel
        event = await DB.fetch_one("SELECT * FROM events WHERE id = ?", (self.event_id,))
        reposted = False
        if event and event['post_channel_id']:
            channel = self.bot.get_channel(event['post_channel_id'])
            if channel:
                img_bytes = await PlaywrightGenerator.generate_allplayers(self.event_id)
                if img_bytes:
                    file = discord.File(img_bytes, filename="allplayers.png")
                    embed = discord.Embed(color=discord.Color.red())
                    embed.set_image(url="attachment://allplayers.png")
                    public_view = await StatsPublicView.create(self.event_id, event['name'])
                    await channel.send(embed=embed, file=file, view=public_view)
                    reposted = True

        repost_msg = "\nUpdated stats image posted!" if reposted else "\nSet a post channel and use **Post Stats** to publish the updated image."
        await interaction.edit_original_response(
            embed=discord.Embed(
                title="Refresh Complete",
                description=f"Refreshed **{refreshed}/{len(maps)}** maps with full stats.{repost_msg}",
                color=discord.Color.green()
            ),
            view=self
        )


class StatsPublicView(discord.ui.View):
    """Persistent view for public stats posts.

    Uses custom_id prefixes so the on_interaction listener handles all
    interactions statelessly — each click fetches fresh data from the DB.
    No in-memory state means multiple users can interact simultaneously
    without race conditions, and the dropdown survives bot restarts.
    """

    def __init__(self):
        super().__init__(timeout=None)

    @classmethod
    async def create(cls, event_id: int, event_name: str):
        view = cls()
        stages_with_matches = await DB.fetch_all(
            "SELECT DISTINCT st.id, st.name FROM stages st "
            "JOIN series s ON s.stage_id = st.id WHERE st.event_id = ? "
            "ORDER BY st.id",
            (event_id,)
        )
        options = [discord.SelectOption(label=s['name'], value=f"stage_{s['id']}") for s in stages_with_matches]
        options.append(discord.SelectOption(label="Export Stats", value=f"export_{event_id}"))
        if options:
            select = discord.ui.Select(
                placeholder="Browse matchups or export stats...",
                options=options,
                custom_id=f"stats_public_{event_id}"
            )
            view.add_item(select)
        return view

    @staticmethod
    async def handle_interaction(interaction: discord.Interaction):
        """Called by the cog listener for any stats_public_ prefixed interaction.

        Fully stateless — each call fetches data from the DB. The original
        message is never edited, so concurrent users don't interfere.
        All responses are ephemeral (private to the clicking user).
        """
        custom_id = interaction.data.get('custom_id', '')
        values = interaction.data.get('values', [])

        # Handle the main event dropdown (stats_public_{event_id})
        if custom_id.startswith('stats_public_') and not custom_id.startswith('stats_public_matchup_'):
            if not values:
                return
            value = values[0]

            if value.startswith("export_"):
                event_id = int(value.replace("export_", ""))
                await interaction.response.defer(ephemeral=True)
                event = await DB.fetch_one("SELECT * FROM events WHERE id = ?", (event_id,))
                if not event:
                    await interaction.followup.send("Event not found.", ephemeral=True)
                    return
                excel_data = await ExcelGenerator.generate_excel(event_id, event['name'])
                if excel_data:
                    await interaction.followup.send(
                        file=discord.File(excel_data, f"LeagueStats_{event['name']}.xlsx"),
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send("No stats data available to export.", ephemeral=True)
                return

            stage_id = int(value.replace("stage_", ""))
            series_list = await DB.fetch_all(
                "SELECT s.id, ta.name as ta, tb.name as tb "
                "FROM series s JOIN teams ta ON s.team_a_id = ta.id JOIN teams tb ON s.team_b_id = tb.id "
                "WHERE s.stage_id = ?",
                (stage_id,)
            )

            if not series_list:
                await interaction.response.send_message("No matchups found for this stage.", ephemeral=True)
                return

            if len(series_list) == 1:
                await interaction.response.defer(ephemeral=True)
                img_bytes = await PlaywrightGenerator.generate_teamvsteam(series_list[0]['id'])
                if img_bytes:
                    file = discord.File(img_bytes, filename="teamvsteam.png")
                    embed = discord.Embed(color=discord.Color.blue())
                    embed.set_image(url="attachment://teamvsteam.png")
                    await interaction.followup.send(embed=embed, file=file, ephemeral=True)
                else:
                    await interaction.followup.send("Could not generate image.", ephemeral=True)
                return

            # Multiple series — show a persistent matchup selector
            view = discord.ui.View(timeout=None)
            opts = [discord.SelectOption(label=f"{s['ta']} vs {s['tb']}", value=str(s['id'])) for s in series_list]
            series_select = discord.ui.Select(
                placeholder="Select a matchup...",
                options=opts,
                custom_id=f"stats_public_matchup_{stage_id}"
            )
            view.add_item(series_select)
            await interaction.response.send_message("Select a matchup:", view=view, ephemeral=True)
            return

        # Handle the matchup sub-dropdown (stats_public_matchup_{stage_id})
        if custom_id.startswith('stats_public_matchup_'):
            if not values:
                return
            await interaction.response.defer(ephemeral=True)
            sid = int(values[0])
            img_bytes = await PlaywrightGenerator.generate_teamvsteam(sid)
            if img_bytes:
                file = discord.File(img_bytes, filename="teamvsteam.png")
                embed = discord.Embed(color=discord.Color.blue())
                embed.set_image(url="attachment://teamvsteam.png")
                await interaction.followup.send(embed=embed, file=file, ephemeral=True)
            else:
                await interaction.followup.send("Could not generate image.", ephemeral=True)

# ==========================================
# COG SETUP
# ==========================================

class LeagueStatsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.api = HenrikDevLeagueAPI()
        
    async def cog_load(self):
        await DB.init()
        await PlaywrightGenerator.init()
        self.bot.loop.create_task(self.download_agent_logos())

    async def cog_unload(self):
        await PlaywrightGenerator.close()

    async def download_agent_logos(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://valorant-api.com/v1/agents?isPlayableCharacter=true") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        agents = data.get("data", [])
                        for agent in agents:
                            name = agent["displayName"]
                            icon_url = agent["displayIcon"]
                            safe_name = re.sub(r'[^a-zA-Z0-9]', '', name).lower()
                            file_path = AGENTS_DIR / f"{safe_name}.png"
                            if not file_path.exists():
                                async with session.get(icon_url) as img_resp:
                                    if img_resp.status == 200:
                                        with open(file_path, "wb") as f:
                                            f.write(await img_resp.read())
        except Exception as e:
            logger.error(f"Failed to download agent logos: {e}")

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get('custom_id', '')
        if custom_id.startswith('stats_public_'):
            try:
                await StatsPublicView.handle_interaction(interaction)
            except Exception as e:
                logger.error(f"Error handling stats interaction {custom_id}: {e}")
                try:
                    if interaction.response.is_done():
                        await interaction.followup.send("Something went wrong. Please try again.", ephemeral=True)
                    else:
                        await interaction.response.send_message("Something went wrong. Please try again.", ephemeral=True)
                except Exception:
                    pass

    @app_commands.command(name="stats_adminpanel", description="League Stats Admin Panel — Manage league and publish stats")
    @app_commands.default_permissions(administrator=True)
    async def stats_adminpanel(self, interaction: discord.Interaction):
        event = await DB.get_active_event()
        event_id = event['id'] if event else None
        event_name = event['name'] if event else None
        view = StatsAdminPanel(self.bot, self.api, event_id, event_name)
        await interaction.response.send_message(embed=view.main_embed(), view=view, ephemeral=True)


async def setup(bot):
    await bot.add_cog(LeagueStatsCog(bot))
