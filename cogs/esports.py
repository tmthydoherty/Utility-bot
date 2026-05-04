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
import unicodedata
import zoneinfo
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
    DEFAULT_GAME_ICON_FALLBACK, ALLOWED_TIERS, GAME_ALLOWED_TIERS,
    GAME_EXTRA_ALLOWED_REGIONS, GAME_BLOCKED_REGIONS,
    ALLOWED_REGION_KEYWORDS, MAJOR_LEAGUE_KEYWORDS,
    EXCLUDED_REGION_KEYWORDS, INTERNATIONAL_LAN_KEYWORDS,
    RLCS_EARLY_ROUND_KEYWORDS, RLCS_LAN_EXTRA_KEYWORDS, RLCS_LAN_IDENTIFIERS,
    RL_TOURNAMENT_BLACKLIST, RL_TOURNAMENT_WHITELIST,
    VCT_CHALLENGERS_SUBREGION_KEYWORDS,
    LIQUIPEDIA_GAME_SLUGS, GAME_MAP_FALLBACK,
    logger, ensure_data_file, load_data_sync, save_data_sync,
    safe_parse_datetime, get_game_vote_emoji, stitch_images, add_white_outline,
    LeaderboardView, VoteRevealView, VoteCycleView, BatchVoteRevealView, ResultDetailsView, EsportsAdminView,
    UnifiedUpcomingView, UnifiedResultView,
    MAX_LEADERBOARD_NAME_LENGTH, MAX_MAP_NAME_LENGTH
)

# --- LOCAL CONFIGURATION ---
MATCH_TIMEOUT_SECONDS = 172800 # 48 hours
TEST_MATCH_TIMEOUT_SECONDS = 7200 # 2 hours
MAX_IMAGE_CACHE_SIZE = 100 
MAX_PROCESSED_HISTORY = 500

# Gemini map data uses REST API directly via aiohttp (no SDK needed)


def _normalize_team_name(name: str) -> str:
    """Normalize team name for fuzzy matching - strip suffixes, accents, etc."""
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = name.lower().strip()
    for suffix in [' esports', ' e-sports', ' esport', ' gaming', ' team', ' clan', ' org', ' gg']:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    return name


class GeminiMapClient:
    """Uses Gemini 2.5 Flash REST API with Google Search grounding to fetch per-map match results.

    Bypasses the google.generativeai SDK entirely and calls the REST API directly
    via aiohttp. This avoids SDK compatibility issues with Google Search grounding
    and gives us full control over the request/response lifecycle.
    """

    MODEL_NAME = "gemini-2.5-flash"
    API_BASE = "https://generativelanguage.googleapis.com/v1beta"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        self._semaphore = asyncio.Semaphore(2)
        if self.api_key:
            logger.info(f"GeminiMapClient: initialized with {self.MODEL_NAME} (REST API)")
        else:
            logger.warning("GeminiMapClient: no API key found")

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    # Game-specific sites that reliably have per-map results
    _GAME_RESULT_SITES = {
        "Valorant": "vlr.gg",
        "Rainbow 6 Siege": "siege.gg",
        "Overwatch": "over.gg",
    }

    def _build_search_prompt(self, game_name: str, event_name: str,
                             team_a_name: str, team_b_name: str,
                             bo_count: int = None, match_date: str = None,
                             series_score: str = None) -> str:
        """Phase 1 prompt: natural language question for grounded search.

        Designed to be search-engine-friendly so Google Search grounding finds
        the actual match results page. Includes date, teams, and series score
        to narrow down to the exact match.
        """
        # Build a search-friendly query string
        query_parts = [f"{game_name} {team_a_name} vs {team_b_name} map results"]
        if event_name:
            query_parts.append(event_name)
        if match_date:
            query_parts.append(match_date)
        if bo_count:
            query_parts.append(f"Bo{bo_count}")
        if series_score:
            query_parts.append(f"final score {series_score}")

        prompt = ", ".join(query_parts)

        # Add the actual question
        prompt += (
            "\n\nList every map played in this series with the map name and the "
            "round score for each map (e.g. 13-7). Include which team won each map."
        )

        return prompt

    def _build_structure_prompt(self, search_response: str,
                                team_a_name: str, team_b_name: str) -> str:
        """Phase 2 prompt: convert natural language map data into structured JSON."""
        return (
            "Extract the per-map results from the text below into JSON.\n\n"
            f"Text:\n{search_response}\n\n"
            "Return ONLY a JSON object in this exact format:\n"
            '{"maps": [{"name": "MapName", "score_a": 13, "score_b": 7, "winner": "team_a"}, ...]}\n\n'
            "Rules:\n"
            f'- team_a = "{team_a_name}"\n'
            f'- team_b = "{team_b_name}"\n'
            '- "winner" must be exactly "team_a" or "team_b"\n'
            '- "name" is the map name (e.g. Ascent, Haven, Bind)\n'
            '- "score_a" and "score_b" are the round scores for that map\n'
            "- Include ALL maps mentioned, in order played\n"
            "- If round scores are not mentioned but a map winner is, use 0 for both scores\n"
        )

    def _build_fallback_prompt(self, game_name: str, event_name: str,
                               team_a_name: str, team_b_name: str,
                               bo_count: int = None, match_date: str = None,
                               series_score: str = None) -> str:
        """Single-shot fallback prompt for non-grounded attempts."""
        parts = [game_name]
        if event_name:
            parts.append(event_name)
        matchup = f"{team_a_name} vs {team_b_name}"
        if bo_count:
            matchup += f", Bo{bo_count}"
        parts.append(matchup)
        if match_date:
            parts.append(match_date)
        if series_score:
            parts.append(f"final score {series_score}")

        prompt = ", ".join(parts)
        prompt += (
            "\n\nWhat maps were played and what were the round scores for each map?\n\n"
            "Respond ONLY with JSON, no other text:\n"
            '{"maps": [{"name": "MapName", "score_a": 13, "score_b": 7, "winner": "team_a"}, ...]}\n'
            f'team_a = "{team_a_name}", team_b = "{team_b_name}"\n'
            "Include all maps played in order. winner must be exactly \"team_a\" or \"team_b\"."
        )
        return prompt

    async def _call_gemini(self, prompt: str, use_grounding: bool = True) -> str:
        """Call Gemini API via REST with optional Google Search grounding."""
        url = f"{self.API_BASE}/models/{self.MODEL_NAME}:generateContent"

        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1},
        }

        if use_grounding:
            body["tools"] = [{"google_search": {}}]

        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with self._semaphore:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(
                        url, json=body, params={"key": self.api_key}
                    ) as resp:
                        if resp.status != 200:
                            error_text = await resp.text()
                            logger.warning(
                                f"Gemini API error {resp.status}: {error_text[:500]}"
                            )
                            return ""
                        data = await resp.json()
        except asyncio.TimeoutError:
            logger.warning("Gemini API call timed out")
            return ""
        except Exception as e:
            logger.warning(f"Gemini REST call failed: {e}")
            return ""

        candidates = data.get("candidates", [])
        if not candidates:
            # Check for prompt blocking
            block_reason = data.get("promptFeedback", {}).get("blockReason")
            if block_reason:
                logger.warning(f"Gemini prompt blocked: {block_reason}")
            else:
                logger.debug("Gemini API returned no candidates")
            return ""

        parts = candidates[0].get("content", {}).get("parts", [])
        text_parts = [p.get("text", "") for p in parts if "text" in p]
        result = "\n".join(text_parts)

        # Log grounding metadata if present (useful for debugging)
        grounding = candidates[0].get("groundingMetadata")
        if grounding and grounding.get("searchEntryPoint"):
            logger.debug("Gemini grounding: search was triggered")

        return result

    def _parse_maps_response(self, text: str, team_a_name: str, team_b_name: str) -> list:
        """Parse Gemini response text into map data list.

        Handles JSON wrapped in markdown code blocks, embedded in prose,
        or with minor formatting issues (trailing commas, etc.).
        """
        if not text:
            return []

        json_text = text.strip()

        # Strategy 1: Extract from markdown code block
        if "```" in json_text:
            match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', json_text)
            if match:
                json_text = match.group(1)

        # Strategy 2: Find raw JSON object with "maps" key
        if not json_text.lstrip().startswith("{"):
            # Try greedy match for the maps array
            match = re.search(r'(\{[^{}]*"maps"\s*:\s*\[[\s\S]*?\]\s*\})', json_text)
            if match:
                json_text = match.group(1)

        # Try parsing as-is first
        parsed = None
        try:
            parsed = json.loads(json_text)
        except json.JSONDecodeError:
            # Fix common issues: trailing commas, single quotes
            try:
                cleaned = re.sub(r',\s*([}\]])', r'\1', json_text)
                parsed = json.loads(cleaned)
            except json.JSONDecodeError:
                logger.debug(
                    f"GeminiMapClient: could not parse JSON from response "
                    f"(first 300 chars): {text[:300]}"
                )
                return []

        maps_raw = parsed.get("maps", [])
        if not maps_raw:
            return []

        maps = []
        for m in maps_raw:
            winner_str = m.get("winner", "")
            winner = 0 if winner_str == "team_a" else 1
            name = (m.get("name") or "Map")[:MAX_MAP_NAME_LENGTH]
            try:
                score_a = int(m.get("score_a", 0))
                score_b = int(m.get("score_b", 0))
            except (ValueError, TypeError):
                score_a, score_b = 0, 0
            maps.append({
                "name": name,
                "score_a": score_a,
                "score_b": score_b,
                "winner": winner,
                "status": "finished",
            })

        return maps

    async def extract_map_data(self, game_name: str, event_name: str,
                               team_a_name: str, team_b_name: str,
                               bo_count: int = None, match_date: str = None,
                               series_score: str = None) -> list:
        """Ask Gemini for per-map results of a completed match.

        Uses a two-phase approach:
          Phase 1: Grounded Google Search with a natural language prompt
                   (no JSON constraint — avoids the 'return empty' trap)
          Phase 2: Non-grounded call to convert the prose response to JSON

        Falls back to a single-shot non-grounded call if phase 1 fails.

        Returns list of map dicts: [{name, score_a, score_b, winner (0/1), status}]
        """
        if not self.available:
            logger.warning("GeminiMapClient: not available (missing API key)")
            return []

        # Phase 1: Grounded search — ask naturally, get prose with real data
        search_prompt = self._build_search_prompt(
            game_name, event_name, team_a_name, team_b_name,
            bo_count, match_date, series_score
        )
        search_response = await self._call_gemini(search_prompt, use_grounding=True)

        if search_response:
            # Try parsing Phase 1 directly as JSON first (sometimes grounding returns structured data)
            maps = self._parse_maps_response(search_response, team_a_name, team_b_name)
            if maps:
                logger.info(
                    f"GeminiMapClient: grounded search returned structured data directly — "
                    f"{len(maps)} maps for {team_a_name} vs {team_b_name}"
                )
                return maps

            # Phase 2: Convert the prose response into structured JSON
            structure_prompt = self._build_structure_prompt(
                search_response, team_a_name, team_b_name
            )
            json_response = await self._call_gemini(structure_prompt, use_grounding=False)
            if json_response:
                maps = self._parse_maps_response(json_response, team_a_name, team_b_name)
                if maps:
                    logger.info(
                        f"GeminiMapClient: two-phase grounded search returned "
                        f"{len(maps)} maps for {team_a_name} vs {team_b_name}"
                    )
                    return maps
            logger.debug("GeminiMapClient: phase 2 structuring failed, grounded response was: %s", search_response[:300])
        else:
            logger.debug("GeminiMapClient: grounded search returned empty for %s vs %s", team_a_name, team_b_name)

        # Fallback: single-shot non-grounded (uses Gemini's training knowledge)
        fallback_prompt = self._build_fallback_prompt(
            game_name, event_name, team_a_name, team_b_name,
            bo_count, match_date, series_score
        )
        text = await self._call_gemini(fallback_prompt, use_grounding=False)
        if text:
            maps = self._parse_maps_response(text, team_a_name, team_b_name)
            if maps:
                logger.info(
                    f"GeminiMapClient: non-grounded fallback returned {len(maps)} "
                    f"maps for {team_a_name} vs {team_b_name}"
                )
                return maps

        return []



def _match_event_str(m: dict) -> str:
    """Return a short event label for a PandaScore match.

    Ensures the league name (which often contains the region) is included
    even when the serie name takes priority, e.g.:
    'RLCS North America — Paris Major: Open 4 2026 — Playoffs'
    """
    league = (m.get('league') or {}).get('name', '')
    serie = (m.get('serie') or {}).get('full_name', '')
    tournament = (m.get('tournament') or {}).get('name', '')

    parts = []
    # Always include league name (typically has region info)
    if league:
        parts.append(league)
    # Add serie if it adds info beyond the league name
    if serie and serie.lower() != league.lower() and serie.lower() not in league.lower():
        parts.append(serie)
    # Add tournament if it adds info beyond what's already there
    if tournament:
        existing = ' '.join(parts).lower()
        if tournament.lower() not in existing:
            parts.append(tournament)

    return ' — '.join(parts) if parts else 'Unknown Event'


def _match_serie_key(slug: str, m: dict) -> str:
    """Stable key for grouping matches by their parent event (game + serie)."""
    serie_id = (m.get('serie') or {}).get('id', 0)
    league_id = (m.get('league') or {}).get('id', 0)
    return f"{slug}:{league_id}:{serie_id}"


def _build_match_details_embed(match: dict, game_slug: str) -> discord.Embed:
    """Rich details embed shown to admins before force-posting a match.

    Intentionally exposes every field that might matter for deciding whether
    to publish — the short select-option descriptions truncate heavily, so
    this is where the admin actually reads the full event info.
    """
    opps = match.get('opponents', [])
    name_a = opps[0]['opponent'].get('name', 'TBD') if len(opps) > 0 else "TBD"
    name_b = opps[1]['opponent'].get('name', 'TBD') if len(opps) > 1 else "TBD"
    acr_a = opps[0]['opponent'].get('acronym') if len(opps) > 0 else None
    acr_b = opps[1]['opponent'].get('acronym') if len(opps) > 1 else None
    id_a = opps[0]['opponent'].get('id') if len(opps) > 0 else None
    id_b = opps[1]['opponent'].get('id') if len(opps) > 1 else None

    game_name = GAMES.get(game_slug, game_slug.upper())
    embed = discord.Embed(
        title=f"Match Details — {name_a} vs {name_b}",
        color=discord.Color.blurple(),
    )
    embed.set_author(name=game_name, icon_url=GAME_LOGOS.get(game_slug))

    # Time
    dt = safe_parse_datetime(match.get('begin_at', ''))
    if dt:
        embed.add_field(
            name="Start Time",
            value=f"<t:{int(dt.timestamp())}:F> (<t:{int(dt.timestamp())}:R>)",
            inline=False,
        )
    else:
        embed.add_field(name="Start Time", value="TBD", inline=False)

    # Event chain
    league = (match.get('league') or {}).get('name') or "—"
    serie = (match.get('serie') or {}).get('full_name') or (match.get('serie') or {}).get('name') or "—"
    tournament = (match.get('tournament') or {}).get('name') or "—"
    embed.add_field(name="League", value=league, inline=True)
    embed.add_field(name="Serie", value=serie, inline=True)
    embed.add_field(name="Tournament", value=tournament, inline=True)

    # Format / tier / status / region
    num_games = match.get('number_of_games')
    bo = f"Bo{num_games}" if num_games else "—"
    tier = (match.get('tournament') or {}).get('tier') or "—"
    status = match.get('status') or "—"
    region = (match.get('tournament') or {}).get('region') or (match.get('league') or {}).get('region') or "—"
    match_type = match.get('match_type') or "—"
    embed.add_field(name="Format", value=str(bo), inline=True)
    embed.add_field(name="Tier", value=str(tier).upper(), inline=True)
    embed.add_field(name="Status", value=str(status), inline=True)
    embed.add_field(name="Region", value=str(region), inline=True)
    embed.add_field(name="Match Type", value=str(match_type), inline=True)
    embed.add_field(name="Match ID", value=f"`{match.get('id', '—')}`", inline=True)

    # Teams
    def _team_line(name, acr, tid):
        parts = [f"**{name}**"]
        extras = []
        if acr:
            extras.append(acr)
        if tid:
            extras.append(f"id `{tid}`")
        if extras:
            parts.append(f"({' · '.join(extras)})")
        return " ".join(parts)

    embed.add_field(
        name="Teams",
        value=f"{_team_line(name_a, acr_a, id_a)}\nvs\n{_team_line(name_b, acr_b, id_b)}",
        inline=False,
    )

    # Stream
    stream_url = match.get('official_stream_url')
    streams = match.get('streams_list') or []
    stream_lines = []
    if stream_url:
        stream_lines.append(f"[Official]({stream_url})")
    for s in streams[:4]:
        raw_url = s.get('raw_url') or s.get('embed_url')
        lang = s.get('language') or "?"
        if raw_url and raw_url != stream_url:
            stream_lines.append(f"[{lang}]({raw_url})")
    if stream_lines:
        embed.add_field(name="Streams", value=" · ".join(stream_lines), inline=False)

    # Scheduled slug (often mentions week / round / day info not in event chain)
    slug_text = match.get('slug')
    if slug_text:
        embed.set_footer(text=f"slug: {slug_text}")

    return embed


class ForcePublishDetailsView(discord.ui.View):
    """Shows detailed info about a selected match and offers Post / Back."""

    def __init__(self, cog, matches_by_id: dict, selected_id: str):
        super().__init__(timeout=120)
        self.cog = cog
        self.matches_by_id = matches_by_id
        self.selected_id = selected_id

    @discord.ui.button(label="Post Match", style=discord.ButtonStyle.green, row=0)
    async def post_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        slug, m = self.matches_by_id[self.selected_id]
        opps = m.get('opponents', [])
        name_a = opps[0]['opponent']['name'] if len(opps) > 0 else "TBD"
        name_b = opps[1]['opponent']['name'] if len(opps) > 1 else "TBD"
        view = ForcePublishConfirmView(self.cog, self.matches_by_id, self.selected_id)
        await interaction.response.edit_message(
            content=f"⚠️ Confirm posting **{name_a} vs {name_b}** to the esports channel?",
            embed=_build_match_details_embed(m, slug),
            view=view,
        )
        self.stop()

    @discord.ui.button(label="Back to List", style=discord.ButtonStyle.secondary, row=0)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ForcePublishView(self.cog, self.matches_by_id)
        await interaction.response.edit_message(
            content="Select a match to post:",
            embed=None,
            view=view,
        )
        self.stop()


class ForcePublishConfirmView(discord.ui.View):
    """Final confirmation before actually posting."""

    def __init__(self, cog, matches_by_id: dict, selected_id: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.matches_by_id = matches_by_id
        self.selected_id = selected_id

    @discord.ui.button(label="Confirm Post", style=discord.ButtonStyle.green, row=0)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        slug, m = self.matches_by_id[self.selected_id]
        await interaction.response.defer(ephemeral=True)
        success = await self.cog._post_match_to_channel(m, slug)
        opps = m.get('opponents', [])
        name_a = opps[0]['opponent']['name'] if len(opps) > 0 else "TBD"
        name_b = opps[1]['opponent']['name'] if len(opps) > 1 else "TBD"
        if success:
            await interaction.followup.send(f"✅ Posted **{name_a} vs {name_b}**!", ephemeral=True)
        else:
            await interaction.followup.send("❌ Failed to post match. Check bot logs.", ephemeral=True)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=0)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        slug, m = self.matches_by_id[self.selected_id]
        view = ForcePublishDetailsView(self.cog, self.matches_by_id, self.selected_id)
        await interaction.response.edit_message(
            content="Review match details:",
            embed=_build_match_details_embed(m, slug),
            view=view,
        )
        self.stop()


class OverturnSelectView(discord.ui.View):
    """Lets admins select a match from history to overturn."""

    def __init__(self, cog, options: list):
        super().__init__(timeout=120)
        self.cog = cog
        self.selected_id = None

        self.select = discord.ui.Select(
            placeholder="Select match to overturn...",
            options=options,
            row=0
        )
        self.select.callback = self.on_select
        self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        self.selected_id = self.select.values[0]
        data = load_data_sync()
        mh = data.get("match_history", {}).get(self.selected_id, {})
        teams = mh.get("teams", [])

        if len(teams) < 2:
            return await interaction.response.send_message("❌ Invalid match data.", ephemeral=True)

        # Show buttons for which team should be the correct winner
        view = OverturnConfirmView(self.cog, self.selected_id, teams)
        old_winner = teams[mh.get("winner_idx", 0)]["name"]
        await interaction.response.edit_message(
            content=f"**Current winner:** {old_winner}\n\nWho **actually** won?",
            view=view
        )


class OverturnConfirmView(discord.ui.View):
    """Shows two buttons for selecting the correct winner."""

    def __init__(self, cog, match_id: str, teams: list):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.teams = teams

        for i, team in enumerate(teams):
            btn = discord.ui.Button(
                label=team["name"][:80],
                style=discord.ButtonStyle.primary,
                custom_id=f"overturn_{match_id}_{i}",
                row=0
            )
            btn.callback = self._make_callback(i)
            self.add_item(btn)

    def _make_callback(self, winner_idx: int):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True)
            await self.cog.execute_overturn(interaction, self.match_id, winner_idx)
            self.stop()
        return callback


class ForcePublishModeView(discord.ui.View):
    """First step after clicking Force Publish: choose single match or entire event."""

    def __init__(self, cog, sorted_matches: dict):
        super().__init__(timeout=120)
        self.cog = cog
        self.sorted_matches = sorted_matches

    @discord.ui.button(label="Single Match", style=discord.ButtonStyle.primary, row=0)
    async def single_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ForcePublishView(self.cog, self.sorted_matches)
        await interaction.response.edit_message(content="Select a match to post:", view=view)

    @discord.ui.button(label="Post Entire Event", style=discord.ButtonStyle.green, row=0)
    async def entire_event(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ForcePublishEventView(self.cog, self.sorted_matches)
        await interaction.response.edit_message(
            content="Select an event to bulk-post all its upcoming matches:",
            view=view
        )


class ForcePublishView(discord.ui.View):
    """Lets admins select and force-post a specific upcoming match that was never published."""

    def __init__(self, cog, matches_by_id: dict):
        # matches_by_id: {mid_str: (game_slug, match_dict)}
        super().__init__(timeout=120)
        self.cog = cog
        self.matches_by_id = matches_by_id
        self.selected_id = None

        options = []
        for mid, (slug, m) in list(matches_by_id.items())[:25]:
            opps = m.get('opponents', [])
            name_a = opps[0]['opponent']['name'] if len(opps) > 0 else "TBD"
            name_b = opps[1]['opponent']['name'] if len(opps) > 1 else "TBD"
            game_tag = GAME_SHORT_NAMES.get(slug, slug.upper())
            label = f"[{game_tag}] {name_a} vs {name_b}"[:100]
            dt = safe_parse_datetime(m.get('begin_at', ''))
            time_str = dt.strftime("%b %d %H:%M UTC") if dt else "TBD"
            event = _match_event_str(m)
            desc = f"{event} · {time_str}"[:100]
            options.append(discord.SelectOption(label=label, value=mid, description=desc))

        self.select = discord.ui.Select(
            placeholder="Select match to force-publish...",
            options=options,
            row=0
        )
        self.select.callback = self.on_select
        self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        self.selected_id = self.select.values[0]
        slug, m = self.matches_by_id[self.selected_id]
        view = ForcePublishDetailsView(self.cog, self.matches_by_id, self.selected_id)
        await interaction.response.edit_message(
            content="Review match details:",
            embed=_build_match_details_embed(m, slug),
            view=view,
        )
        self.stop()


class ForcePublishEventView(discord.ui.View):
    """Groups unposted matches by event; lets admins bulk-post all matches for one event."""

    def __init__(self, cog, matches_by_id: dict):
        super().__init__(timeout=120)
        self.cog = cog
        self.selected_key = None

        # Group matches by their parent event (league + serie)
        self.events: Dict[str, List[tuple]] = {}
        for mid, (slug, m) in matches_by_id.items():
            key = _match_serie_key(slug, m)
            self.events.setdefault(key, []).append((slug, m))

        options = []
        for key, entries in list(self.events.items())[:25]:
            slug, m = entries[0]
            league = (m.get('league') or {}).get('name', '')
            serie = (m.get('serie') or {}).get('full_name', '')
            game_tag = GAME_SHORT_NAMES.get(slug, slug.upper())
            event_label = f"[{game_tag}] {serie or league or 'Unknown Event'}"[:100]
            count = len(entries)
            tournament = (m.get('tournament') or {}).get('name', '')
            desc = f"{count} match{'es' if count != 1 else ''}"
            if tournament:
                desc += f" · {tournament}"
            desc = desc[:100]
            options.append(discord.SelectOption(label=event_label, value=key, description=desc))

        self.select = discord.ui.Select(
            placeholder="Select event to bulk-post...",
            options=options,
            row=0
        )
        self.select.callback = self.on_select
        self.add_item(self.select)

        self.confirm_btn = discord.ui.Button(
            label="Select an event first",
            style=discord.ButtonStyle.green,
            disabled=True,
            row=1
        )
        self.confirm_btn.callback = self.on_confirm
        self.add_item(self.confirm_btn)

    async def on_select(self, interaction: discord.Interaction):
        self.selected_key = self.select.values[0]
        entries = self.events[self.selected_key]
        count = len(entries)
        slug, m = entries[0]
        serie = (m.get('serie') or {}).get('full_name', '') or (m.get('league') or {}).get('name', '')
        self.confirm_btn.disabled = False
        self.confirm_btn.label = f"Post All {count} Match{'es' if count != 1 else ''}: {serie}"[:80]
        await interaction.response.edit_message(view=self)

    async def on_confirm(self, interaction: discord.Interaction):
        if not self.selected_key:
            await interaction.response.send_message("Please select an event first.", ephemeral=True)
            return
        entries = self.events[self.selected_key]
        await interaction.response.defer(ephemeral=True)

        posted, skipped = 0, 0
        for slug, m in entries:
            success = await self.cog._post_match_to_channel(m, slug)
            if success:
                posted += 1
            else:
                skipped += 1

        slug, m = entries[0]
        serie = (m.get('serie') or {}).get('full_name', '') or (m.get('league') or {}).get('name', '')
        msg = f"✅ Posted **{posted}** match{'es' if posted != 1 else ''} for **{serie}**."
        if skipped:
            msg += f"\n⚠️ {skipped} already posted or failed — check bot logs."
        await interaction.followup.send(msg, ephemeral=True)
        self.stop()


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
        self.gemini_maps = GeminiMapClient()

        ensure_data_file()
        self._update_emoji_cache()
        self.match_tracker.start()
        self.map_data_fetcher.start()
        self.error_reporting_loop.start()
        self.upcoming_embed_bumper.start()
        self.result_lifecycle_checker.start()

    def cog_unload(self):
        self.match_tracker.cancel()
        self.map_data_fetcher.cancel()
        self.error_reporting_loop.cancel()
        self.upcoming_embed_bumper.cancel()
        self.result_lifecycle_checker.cancel()

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

    # Regex to reject strings that look like times (e.g. "17:00UTC") instead of player names
    _TIME_PATTERN = re.compile(r'^\d{1,2}:\d{2}')
    _last_liquipedia_request = 0.0  # rate limiter timestamp

    async def _liquipedia_rate_limit(self):
        """Enforce Liquipedia MediaWiki API rate limit: 1 request per 5 seconds."""
        import time
        now = time.monotonic()
        elapsed = now - self._last_liquipedia_request
        if elapsed < 5.0:
            await asyncio.sleep(5.0 - elapsed)
        Esports._last_liquipedia_request = time.monotonic()

    @staticmethod
    def _is_valid_player_name(name: str) -> bool:
        """Reject strings that look like times, dates, or other non-player data."""
        if not name or len(name) < 2 or len(name) > 20:
            return False
        if name.startswith('/'):
            return False
        # Reject time strings like "17:00UTC", "18:15UTC"
        if re.match(r'^\d{1,2}:\d{2}', name):
            return False
        # Reject pure numeric strings
        if name.replace('.', '').replace('-', '').isdigit():
            return False
        return True

    async def fetch_roster_liquipedia(self, team_name: str, game_slug: str) -> list:
        """Fetch roster from Liquipedia via the MediaWiki API (ToS-compliant)."""
        wiki_slug = LIQUIPEDIA_GAME_SLUGS.get(game_slug)
        if not wiki_slug:
            return []

        team_slug = team_name.strip().replace(" ", "_")
        api_url = f"https://liquipedia.net/{wiki_slug}/api.php"
        params = {
            "action": "parse",
            "page": team_slug,
            "format": "json",
            "prop": "text",
        }
        headers = {
            "User-Agent": "VibeyBot/1.0 (Discord esports prediction bot; github.com/vibey)",
            "Accept-Encoding": "gzip",
        }

        try:
            await self._liquipedia_rate_limit()
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(api_url, params=params) as resp:
                    if resp.status != 200:
                        logger.debug(f"Liquipedia API returned {resp.status} for {team_slug}")
                        return []
                    data = await resp.json()

            parse_data = data.get('parse', {})
            html_content = parse_data.get('text', {}).get('*', '')
            if not html_content:
                logger.debug(f"Liquipedia API returned empty content for {team_slug}")
                return []

            soup = BeautifulSoup(html_content, 'html.parser')

            players = []

            # Strategy 1: roster-card divs (modern Liquipedia format)
            roster_cards = soup.select('.roster-card .player-name, .roster-card .ID')
            if roster_cards:
                for card in roster_cards:
                    name = card.get_text(strip=True)
                    if self._is_valid_player_name(name):
                        players.append(name)

            # Strategy 2: "Active Squad" / "Player Roster" table heading
            if not players:
                for heading in soup.find_all(['h2', 'h3']):
                    heading_text = heading.get_text(strip=True).lower()
                    if any(kw in heading_text for kw in ['active', 'roster', 'squad', 'player']):
                        sibling = heading.find_next(['table', 'div'])
                        if sibling:
                            for row in sibling.find_all('tr'):
                                cells = row.find_all('td')
                                for cell in cells:
                                    id_link = cell.find('a', href=True)
                                    if id_link:
                                        text = id_link.get_text(strip=True)
                                        if self._is_valid_player_name(text):
                                            players.append(text)
                                            break
                        if players:
                            break

            # Strategy 3: .wikitable with player data
            if not players:
                for table in soup.select('.wikitable'):
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            id_text = cells[1].get_text(strip=True)
                            if self._is_valid_player_name(id_text):
                                players.append(id_text)

            # Deduplicate preserving order, limit to 6
            seen = set()
            unique_players = []
            for p in players:
                if p.lower() not in seen:
                    seen.add(p.lower())
                    unique_players.append(p)

            if unique_players:
                logger.debug(f"Liquipedia roster for {team_name}: {unique_players[:6]}")
                return unique_players[:6]

        except asyncio.TimeoutError:
            logger.debug(f"Liquipedia API timeout for {team_name}")
        except Exception as e:
            logger.debug(f"Liquipedia roster fetch failed for {team_name}: {e}")

        return []

    async def fetch_roster(self, team_id, team_name, game_slug):
        # Try Liquipedia first (more up-to-date rosters)
        roster = await self.fetch_roster_liquipedia(team_name, game_slug)
        if roster:
            return roster

        # Fall back to PandaScore API
        team_data = await self.get_pandascore_data(f"/teams/{team_id}")
        if team_data and 'players' in team_data:
            roster = [p.get('name', 'Unknown') for p in team_data['players'] if p.get('active', True)]
            if not roster:
                roster = [p.get('name', 'Unknown') for p in team_data['players']]
            if roster:
                return roster[:6]
        return []

    # --- LIQUIPEDIA MATCH COMPLETION FALLBACK ---
    _LIQUIPEDIA_MATCH_PAGES = {
        "valorant": "valorant",
        "r6siege": "rainbowsix",
        "rl": "rocketleague",
        "ow": "overwatch",
    }

    async def _verify_matches_liquipedia(self, game_slug: str, pending_new: list) -> list:
        """Cross-verify pending PandaScore matches against Liquipedia's Matches page.

        Fetches the Liquipedia Matches page ONCE for the game and checks whether
        both team names in each pending match appear. Matches that don't appear
        on Liquipedia are rejected (likely PandaScore errors).

        Returns the filtered list of verified matches.
        If Liquipedia is unreachable, returns all matches (don't block on failure).
        """
        wiki = self._LIQUIPEDIA_MATCH_PAGES.get(game_slug)
        if not wiki:
            return pending_new

        api_url = f"https://liquipedia.net/{wiki}/api.php"
        params = {
            "action": "parse",
            "page": "Liquipedia:Matches",
            "format": "json",
            "prop": "text",
        }
        headers = {
            "User-Agent": "VibeyBot/1.0 (Discord esports prediction bot; github.com/vibey)",
            "Accept-Encoding": "gzip",
        }

        try:
            await self._liquipedia_rate_limit()
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(api_url, params=params) as resp:
                    if resp.status != 200:
                        logger.debug(f"Liquipedia verification: page returned {resp.status}, passing all matches through")
                        return pending_new
                    data = await resp.json()

            html = data.get('parse', {}).get('text', {}).get('*', '')
            if not html:
                return pending_new

            page_text = BeautifulSoup(html, 'html.parser').get_text(separator=' ', strip=True).lower()

        except (asyncio.TimeoutError, Exception) as e:
            logger.debug(f"Liquipedia verification failed: {e}, passing all matches through")
            return pending_new

        verified = []
        for mid, m, t_a, t_b in pending_new:
            name_a = _normalize_team_name(t_a['name'])
            name_b = _normalize_team_name(t_b['name'])

            # Check if both teams appear on the Liquipedia matches page
            found_a = name_a in page_text
            found_b = name_b in page_text

            # Also try acronyms as fallback (Liquipedia sometimes uses short names)
            if not found_a and t_a.get('acronym'):
                found_a = t_a['acronym'].lower() in page_text
            if not found_b and t_b.get('acronym'):
                found_b = t_b['acronym'].lower() in page_text

            if found_a and found_b:
                verified.append((mid, m, t_a, t_b))
            else:
                missing = []
                if not found_a:
                    missing.append(t_a['name'])
                if not found_b:
                    missing.append(t_b['name'])
                logger.warning(
                    f"Liquipedia verification FAILED for {game_slug} match {mid}: "
                    f"{t_a['name']} vs {t_b['name']} — "
                    f"not found on Liquipedia Matches page: {', '.join(missing)}. "
                    f"Skipping (likely PandaScore error)."
                )

        return verified

    async def check_liquipedia_match_result(self, team_a_name: str, team_b_name: str, game_slug: str) -> Optional[int]:
        """Check Liquipedia Matches page to see if a match is finished.
        Returns winner index (0 or 1) if found, None otherwise."""
        wiki = self._LIQUIPEDIA_MATCH_PAGES.get(game_slug)
        if not wiki:
            return None

        api_url = f"https://liquipedia.net/{wiki}/api.php"
        params = {
            "action": "parse",
            "page": "Liquipedia:Matches",
            "format": "json",
            "prop": "text",
        }
        headers = {
            "User-Agent": "VibeyBot/1.0 (Discord esports prediction bot; github.com/vibey)",
            "Accept-Encoding": "gzip",
        }

        try:
            await self._liquipedia_rate_limit()
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(api_url, params=params) as resp:
                    if resp.status != 200:
                        logger.debug(f"Liquipedia Matches page returned {resp.status}")
                        return None
                    data = await resp.json()

            html = data.get('parse', {}).get('text', {}).get('*', '')
            if not html:
                return None

            soup = BeautifulSoup(html, 'html.parser')

            # Normalize team names for fuzzy matching
            norm_a = _normalize_team_name(team_a_name)
            norm_b = _normalize_team_name(team_b_name)

            # Find all match rows/entries on the page
            # Liquipedia match entries typically have team-left, score, team-right structure
            match_rows = soup.select('.infobox_matches_content, .match-row, [data-toggle-area-content]')
            if not match_rows:
                # Try broader search - find all elements with team links near score elements
                match_rows = soup.find_all('tr')

            for row in match_rows:
                text = row.get_text(separator=' ', strip=True)
                text_lower = text.lower()

                # Check if both team names appear in this row
                if norm_a not in text_lower and not any(
                    SequenceMatcher(None, norm_a, word).ratio() > 0.8
                    for word in text_lower.split()
                ):
                    continue
                if norm_b not in text_lower and not any(
                    SequenceMatcher(None, norm_b, word).ratio() > 0.8
                    for word in text_lower.split()
                ):
                    continue

                # Found a row with both teams - look for a score pattern like "2:1" or "2:0"
                score_match = re.search(r'(\d+)\s*[:\-]\s*(\d+)', text)
                if score_match:
                    score_left = int(score_match.group(1))
                    score_right = int(score_match.group(2))
                    if score_left == score_right:
                        continue  # Tied/in-progress, skip

                    # Determine which team is on which side
                    # The team appearing first (leftmost) in the text corresponds to score_left
                    pos_a = text_lower.find(norm_a)
                    pos_b = text_lower.find(norm_b)
                    if pos_a == -1 or pos_b == -1:
                        # Use fuzzy match positions
                        continue

                    if pos_a < pos_b:
                        # team_a is on the left
                        winner_idx = 0 if score_left > score_right else 1
                    else:
                        # team_b is on the left
                        winner_idx = 1 if score_left > score_right else 0

                    logger.info(
                        f"Liquipedia confirms match result: {team_a_name} vs {team_b_name} "
                        f"-> winner: team {winner_idx} ({[team_a_name, team_b_name][winner_idx]}), "
                        f"score: {score_left}-{score_right}"
                    )
                    return winner_idx

        except asyncio.TimeoutError:
            logger.debug("Liquipedia Matches page timeout")
        except Exception as e:
            logger.debug(f"Liquipedia match result check failed: {e}")

        return None

    async def get_gemini_map_data(self, team_a_name: str, team_b_name: str, game_slug: str,
                                    match_details: dict = None, winner_idx: int = None,
                                    match_time: datetime.datetime = None) -> list:
        """Ask Gemini for per-map results using match context (game, event, teams).
        Returns list of map dicts: {name, score_a, score_b, winner, status}."""
        if not self.gemini_maps.available:
            logger.debug("Gemini not available, skipping map data extraction")
            return []

        game_name = GAMES.get(game_slug, game_slug)
        event_name = self._build_event_string(match_details) if match_details else ""
        bo_count = match_details.get('number_of_games') if match_details else None

        # Format match date for temporal grounding
        match_date = None
        if match_time:
            match_date = match_time.strftime("%B %d, %Y")
        elif match_details and match_details.get('begin_at'):
            dt = safe_parse_datetime(match_details['begin_at'])
            if dt:
                match_date = dt.strftime("%B %d, %Y")

        # Extract series score from PandaScore results for context
        series_score = None
        if match_details:
            results = match_details.get('results', [])
            opponents = match_details.get('opponents', [])
            if results and len(results) >= 2 and len(opponents) >= 2:
                try:
                    # Match results to teams by team_id
                    opp_a_id = opponents[0].get('opponent', {}).get('id')
                    opp_b_id = opponents[1].get('opponent', {}).get('id')
                    score_a = next((r.get('score', 0) for r in results if r.get('team_id') == opp_a_id), None)
                    score_b = next((r.get('score', 0) for r in results if r.get('team_id') == opp_b_id), None)
                    if score_a is not None and score_b is not None:
                        series_score = f"{team_a_name} {score_a} - {score_b} {team_b_name}"
                except Exception:
                    pass

        maps = await self.gemini_maps.extract_map_data(
            game_name, event_name, team_a_name, team_b_name,
            bo_count, match_date, series_score
        )
        if maps and winner_idx is not None:
            maps = self._validate_winner_mapping(maps, winner_idx)

        return maps

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

    @staticmethod
    def _liquipedia_team_url(team_name: str, game_slug: str) -> str:
        """Build a Liquipedia URL for a team page."""
        wiki = LIQUIPEDIA_GAME_SLUGS.get(game_slug, 'commons')
        slug = urllib.parse.quote(team_name.strip().replace(" ", "_"))
        return f"https://liquipedia.net/{wiki}/{slug}"

    @staticmethod
    def _build_event_string(match_details: dict) -> str:
        """Build a full event name string from match details (league - serie - tournament)."""
        parts = []
        if match_details.get('league', {}).get('name'):
            parts.append(match_details['league']['name'])
        if match_details.get('serie', {}).get('full_name'):
            parts.append(match_details['serie']['full_name'])
        elif match_details.get('serie', {}).get('name'):
            parts.append(match_details['serie']['name'])
        if match_details.get('tournament', {}).get('name'):
            tourn_name = match_details['tournament']['name']
            if not parts or tourn_name.lower() not in parts[-1].lower():
                parts.append(tourn_name)
        return " - ".join(parts)

    def _common_event_footer(self, matches: list) -> str:
        """Build footer text from the common prefix of all match event names.

        If all matches share the exact same event string, use it as-is.
        Otherwise, keep only the leading ' - '-delimited segments that all
        matches share (e.g. 'VCT - EMEA Stage 1 2026').
        """
        event_strings = []
        for m in matches:
            s = self._build_event_string(m)
            if s:
                event_strings.append(s)
        if not event_strings:
            return ""
        if len(set(event_strings)) == 1:
            return event_strings[0]
        # Find common prefix by segments
        split_events = [s.split(" - ") for s in event_strings]
        common = []
        for parts in zip(*split_events):
            if len(set(parts)) == 1:
                common.append(parts[0])
            else:
                break
        return " - ".join(common) if common else event_strings[0].split(" - ")[0]

    def is_quality_match(self, match, game_slug: str = None):
        tier = match.get('tournament', {}).get('tier')

        event_name = (
            (match.get('league', {}).get('name', '') or "") + " " +
            (match.get('serie', {}).get('full_name', '') or "") + " " +
            (match.get('tournament', {}).get('name', '') or "")
        ).lower()

        # Per-game region overrides: add extra allowed keywords for this game,
        # and suppress any excluded keywords that are now explicitly allowed
        # (including sub-region strings that contain them as a substring).
        extra_allowed = GAME_EXTRA_ALLOWED_REGIONS.get(game_slug, [])
        allowed_keywords = ALLOWED_REGION_KEYWORDS + extra_allowed
        if extra_allowed:
            excluded_keywords = [
                k for k in EXCLUDED_REGION_KEYWORDS
                if not any(a in k for a in extra_allowed)
            ]
        else:
            excluded_keywords = EXCLUDED_REGION_KEYWORDS

        # Per-game hard blocks (e.g. OW is NA-only, so "europe"/"emea" are blocked)
        game_blocked = GAME_BLOCKED_REGIONS.get(game_slug, [])
        if game_blocked and any(k in event_name for k in game_blocked):
            return False

        # Check region status upfront - this is critical for correct filtering
        is_allowed_region = any(k in event_name for k in allowed_keywords)
        is_excluded_region = any(k in event_name for k in excluded_keywords)

        # 1. REGION FILTERING FIRST - explicitly excluded regions are blocked
        # UNLESS the event is a TRUE international LAN (not just "champions tour")
        # True international LANs: "masters", "world championship", "champions 2026" (finals), etc.
        # NOT international: "champions tour pacific" - this is a regional league
        if is_excluded_region and not is_allowed_region:
            # Only allow through if it's a TRUE international event
            # These are standalone events, not regional league matches
            true_international_keywords = [
                "masters", "world", "lock//in", "lockin", "lock-in",
                "gamers8", "iem", "blast premier", "six invitational",
                "six major", "all-star", "allstar", "grand final",
            ]
            # "champions" alone is NOT enough - it must be "champions 20XX" (finals event)
            # or similar standalone championship, not "champions tour [region]"
            is_true_international = any(k in event_name for k in true_international_keywords)

            # Special case: "champions 20XX" events (the yearly finals) are international
            # but "champions tour [region]" is a regional league
            champions_finals = re.search(r'\bchampions\s+20\d{2}\b', event_name)
            if champions_finals and "tour" not in event_name:
                is_true_international = True

            if not is_true_international:
                return False

        # 2. Check if it's a major league for this game
        # EXCEPTION: International LAN events (Masters, Majors, World Championship, etc.) bypass
        # this check so they always pass through regardless of which region hosts them.
        is_international_lan = any(k in event_name for k in INTERNATIONAL_LAN_KEYWORDS)
        if game_slug and game_slug in MAJOR_LEAGUE_KEYWORDS and not is_international_lan:
            is_major_league = any(k in event_name for k in MAJOR_LEAGUE_KEYWORDS[game_slug])
            if not is_major_league:
                return False

        # 3. RLCS early round filtering - only applied to REGIONAL events, never LANs
        # At LAN events (Majors/Worlds) every round is worth showing.
        if game_slug == "rl":
            # Key off tournament.name ONLY. PandaScore splits each stage of an RLCS
            # event (Swiss, Groups, Playoffs) into its own tournament entity, so the
            # tournament name is the canonical stage signal. The concatenated
            # league+serie+tournament blob used elsewhere produced false positives
            # because circuit names like "RLCS 2025-26 Major 1" contain "major" and
            # triggered LAN bypass for regional group-stage matches.
            t_name = (match.get('tournament', {}).get('name') or '').lower().strip()

            # Hard blacklist — never post these, regardless of tier.
            if any(k in t_name for k in RL_TOURNAMENT_BLACKLIST):
                return False

            # Whitelist — must be an explicit top-8 / bracket stage.
            if not any(k in t_name for k in RL_TOURNAMENT_WHITELIST):
                return False

        # 4. VCT Challengers sub-regional filtering - ALWAYS exclude these
        # These are lower-tier regional leagues (e.g., North//East, Challengers France)
        # and should be filtered even if they contain "emea" or other allowed keywords
        if game_slug == "valorant":
            is_subregion_challengers = any(k in event_name for k in VCT_CHALLENGERS_SUBREGION_KEYWORDS)
            if is_subregion_challengers:
                return False

        # 5. Tier check - S and A tier by default; OW allows tier B since
        # OWCS matches on PandaScore are frequently labeled lower than A.
        allowed_tiers_for_game = GAME_ALLOWED_TIERS.get(game_slug, ALLOWED_TIERS)
        if tier not in allowed_tiers_for_game:
            return False

        return True

    # --- MAP DATA EXTRACTION ---
    def _extract_pandascore_games(self, match_details: dict, game_slug: str) -> List[Dict[str, Any]]:
        """Build a minimal per-game list from PandaScore's own match['games'] array.

        This is the fallback floor for map data — used when Gemini+Liquipedia
        comes up empty. PandaScore does not expose
        map names or per-game round scores, but it does expose a per-game winner
        (by team id), so we can at least render a W/L sequence instead of hiding
        the map history section entirely.

        Returns [] if the match has no finished games or the team ids cannot be
        resolved against opponents[].
        """
        if not isinstance(match_details, dict):
            return []

        games = match_details.get('games') or []
        if not games:
            return []

        opponents = match_details.get('opponents') or []
        if len(opponents) < 2:
            return []

        try:
            team_a_id = opponents[0].get('opponent', {}).get('id')
            team_b_id = opponents[1].get('opponent', {}).get('id')
        except (AttributeError, IndexError):
            return []
        if team_a_id is None or team_b_id is None:
            return []

        fallback_label = GAME_MAP_FALLBACK.get(game_slug, 'Map')
        entries: List[Dict[str, Any]] = []

        for g in sorted(games, key=lambda x: x.get('position') or x.get('id') or 0):
            if not isinstance(g, dict):
                continue
            if g.get('status') not in ('finished', 'completed') and not g.get('finished'):
                continue

            # PandaScore winner can be a nested dict or a flat id — handle both.
            w = g.get('winner')
            winner_id = None
            if isinstance(w, dict):
                winner_id = w.get('id')
            elif isinstance(w, int):
                winner_id = w

            if winner_id == team_a_id:
                winner = 0
            elif winner_id == team_b_id:
                winner = 1
            else:
                continue  # Unresolvable — skip rather than guess.

            position = g.get('position') or (len(entries) + 1)
            entries.append({
                "name": f"{fallback_label} {position}"[:MAX_MAP_NAME_LENGTH],
                "score_a": None,
                "score_b": None,
                "winner": winner,
                "status": "finished",
            })

        return entries

    async def get_map_data(self, team_a_name, team_b_name, game_slug, match_time=None, winner_idx=None, match_details=None):
        """Layered map-data lookup.

        Order:
          1. Gemini knowledge (primary — asks Gemini for map results from match context)
          2. PandaScore games[] floor (secondary — winners only, no scores/names)

        Rocket League always returns [] regardless of source; per-game results
        aren't meaningful for RL and we disable the whole pipeline for it.
        """
        if game_slug == 'rl':
            return []

        # 1) Gemini knowledge lookup
        if match_details:
            try:
                gemini_data = await self.get_gemini_map_data(
                    team_a_name, team_b_name, game_slug, match_details, winner_idx, match_time
                )
                if gemini_data:
                    logger.info(f"Gemini returned {len(gemini_data)} maps for {team_a_name} vs {team_b_name}")
                    return gemini_data
            except Exception as e:
                logger.warning(f"Gemini map data error: {e}")

        # 2) PandaScore games[] floor
        if match_details:
            floor = self._extract_pandascore_games(match_details, game_slug)
            if floor:
                logger.info(
                    f"PandaScore floor returned {len(floor)} per-game winners for "
                    f"{team_a_name} vs {team_b_name} (no scores/map names)"
                )
                return floor

        return []

    def _validate_winner_mapping(self, map_data: list, winner_idx: int) -> list:
        """Check if map winners align with PandaScore's known match winner.

        If they disagree, trust the extracted map-level data (independent source)
        rather than blindly flipping to match PandaScore's winner_id.
        """
        played = [m for m in map_data if m.get('winner') in (0, 1)]
        if not played:
            return map_data

        map_wins_a = sum(1 for m in played if m['winner'] == 0)
        map_wins_b = sum(1 for m in played if m['winner'] == 1)

        disagrees = (
            (map_wins_a > map_wins_b and winner_idx == 1) or
            (map_wins_b > map_wins_a and winner_idx == 0)
        )

        if disagrees:
            logger.warning(
                f"Map data disagrees with PandaScore winner: "
                f"extracted {map_wins_a}-{map_wins_b}, PandaScore says team {winner_idx} won. "
                f"Keeping extracted data as-is (independent source)."
            )

        return map_data

    @staticmethod
    def _is_map_data_complete(map_data: list) -> bool:
        """Decide whether cached map data is good enough to stop retrying.

        Returns True only when the data has real map names or round scores —
        meaning Gemini+Liquipedia provided actual content. The PandaScore
        games[] floor (winners only, no names/scores) is displayable but NOT
        considered complete, so the background fetcher keeps trying for richer
        data.

        Returns False when:
          - No data at all
          - Only PandaScore floor data (fallback names like "Map 1", "Game 1")
          - Any entry has winner == -1 (still in-progress)
        """
        if not map_data:
            return False

        fallback_names = {"Map", "Game"}

        # Require at least one entry with a real name or real scores.
        for m in map_data:
            if m.get("winner") == -1:
                return False
            name = m.get("name", "") or ""
            name_prefix = name.rsplit(" ", 1)[0] if " " in name else name
            has_real_name = name_prefix not in fallback_names and bool(name_prefix)
            sa = m.get("score_a")
            sb = m.get("score_b")
            has_scores = (sa not in (None, 0)) or (sb not in (None, 0))
            if has_real_name or has_scores:
                return True
        return False

    @staticmethod
    def _is_map_data_displayable(map_data: list) -> bool:
        """Check if map data has enough info to display (even without real names).

        Returns True if every entry has a valid winner (0 or 1). This includes
        the PandaScore games[] floor where we only know winners.
        """
        if not map_data:
            return False
        return all(m.get("winner") in (0, 1) for m in map_data)

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

        # Group keys by team using image_url as identity
        # teams_by_logo: { image_url: { "keys": set(), "name": str } }
        teams_by_logo = {}
        for game_slug in GAMES.keys():
            # Fetch 3 pages per game to catch more teams
            for page in range(1, 4):
                matches = await self.get_pandascore_data(
                    f"/{game_slug}/matches",
                    params={"sort": "-begin_at", "page[size]": 100, "page[number]": page}
                )
                if not matches:
                    break
                for m in matches:
                    if m.get('tournament', {}).get('tier') not in ['s', 'a']:
                        continue
                    for opp in m.get('opponents', []):
                        t = opp.get('opponent')
                        if not t or not t.get('image_url'):
                            continue

                        img_url = t['image_url']
                        if img_url not in teams_by_logo:
                            teams_by_logo[img_url] = {"keys": set(), "name": t.get('name', '')}

                        # Generate all keys for this team
                        if t.get('acronym'):
                            key = t['acronym'].upper()
                            if len(key) >= 2:
                                teams_by_logo[img_url]["keys"].add(key)

                        name_parts = t.get('name', '').split(' ')
                        if name_parts:
                            k1 = "".join(c for c in name_parts[0].upper() if c.isalnum())
                            if len(k1) >= 2:
                                teams_by_logo[img_url]["keys"].add(k1)

                        k2 = "".join(c for c in t.get('name', '').upper() if c.isalnum())
                        if len(k2) >= 2:
                            teams_by_logo[img_url]["keys"].add(k2)

        emoji_map = data.get("emoji_map", {})
        added_count = 0

        async with aiohttp.ClientSession() as session:
            for img_url, team_info in teams_by_logo.items():
                keys = team_info["keys"]
                if not keys:
                    continue

                # Check if this team already has an emoji (any key already mapped)
                existing_emoji = None
                for key in keys:
                    if key in emoji_map:
                        existing_emoji = emoji_map[key]
                        break

                if existing_emoji:
                    # Backfill: map any missing keys to the same existing emoji
                    backfilled = False
                    for key in keys:
                        if key not in emoji_map:
                            emoji_map[key] = existing_emoji
                            backfilled = True
                    if backfilled:
                        async with self.data_lock:
                            d = load_data_sync()
                            d["emoji_map"] = emoji_map
                            save_data_sync(d)
                    continue

                # Upload ONE emoji for this team
                if added_count >= 150:
                    break

                target = next((g for g in target_guilds if len(g.emojis) < g.emoji_limit), None)
                if not target:
                    break

                try:
                    async with session.get(img_url) as resp:
                        if resp.status != 200:
                            continue
                        img_data = await resp.read()

                    img = Image.open(BytesIO(img_data)).convert("RGBA")
                    img.thumbnail((128, 128))
                    img = add_white_outline(img, thickness=3)

                    out = BytesIO()
                    img.save(out, format="PNG")
                    out.seek(0)

                    # Use the shortest key for the emoji name
                    primary_key = min(keys, key=len)
                    emoji_name = f"esp_{primary_key}"[:32]
                    new_emoji = await target.create_custom_emoji(name=emoji_name, image=out.read())
                    emoji_str = str(new_emoji)

                    # Map ALL keys for this team to the single emoji
                    async with self.data_lock:
                        d = load_data_sync()
                        for key in keys:
                            d["emoji_map"][key] = emoji_str
                        save_data_sync(d)
                        emoji_map = d["emoji_map"]

                    added_count += 1
                    await asyncio.sleep(1.5)
                except Exception as e:
                    logger.error(f"Emoji upload failed for {team_info['name']}: {e}")

        return added_count

    # --- EMBED BUILDERS ---
    async def get_map_history(self, match_details, saved_teams, game_slug: str, map_data: list = None) -> Optional[str]:
        """Build map history string. Returns None if no meaningful data to display.

        CRITICAL: If map data doesn't match the final score, return None to avoid
        displaying incorrect information. Showing nothing is better than showing wrong data.
        """
        num_games = match_details.get('number_of_games') or 0

        # Never show map history for Rocket League - individual game scores aren't meaningful
        if game_slug == 'rl':
            return None

        # If no map data at all, return None to signal we should hide this section
        if not map_data:
            return None

        if len(saved_teams) < 2:
            return None

        # Get the final match score from match_details
        results = match_details.get('results', [])
        final_score_a = 0
        final_score_b = 0
        for r in results:
            if r.get('team_id') == saved_teams[0].get('id'):
                final_score_a = r.get('score', 0) or 0
            elif r.get('team_id') == saved_teams[1].get('id'):
                final_score_b = r.get('score', 0) or 0

        total_maps_played = final_score_a + final_score_b

        # Count maps that have actual played data (winner != -1)
        played_maps = [m for m in map_data if m.get('winner', -1) != -1]
        played_count = len(played_maps)

        # Count map wins from the map data
        map_wins_a = sum(1 for m in played_maps if m.get('winner') == 0)
        map_wins_b = sum(1 for m in played_maps if m.get('winner') == 1)

        # Validate map wins match the final score - if they don't, data is unreliable
        if total_maps_played > 0:
            if map_wins_a != final_score_a or map_wins_b != final_score_b:
                logger.warning(
                    f"Map data mismatch! Final score: {final_score_a}-{final_score_b}, "
                    f"Map wins: {map_wins_a}-{map_wins_b}. Hiding match history to avoid false info."
                )
                return None

            # Note: count check removed - if wins match the score, partial data is still valid.

            # Sanity check: if we have MORE maps than the format allows, data is corrupted
            if num_games >= 1 and played_count > num_games:
                logger.warning(f"Too many maps for Bo{num_games}: got {played_count}, max is {num_games}")
                return None

        # Display maps up to the format limit, padded to full series length
        max_maps_to_show = num_games if num_games > 0 else 7
        maps_to_display = played_maps[:max_maps_to_show]

        lines = []
        has_real_data = False
        fallback_label = GAME_MAP_FALLBACK.get(game_slug, 'Map')

        for m in maps_to_display:
            map_name = m.get('name', fallback_label)
            status = m.get('status', 'finished')
            winner_idx = m.get('winner')

            if status == 'finished' and winner_idx is not None and winner_idx != -1:
                has_real_data = True
                w_disp = self.get_team_display(saved_teams[winner_idx])
                score_a = m.get('score_a')
                score_b = m.get('score_b')

                # None (PandaScore floor) or 0/0 (unknown scores) → show winner only.
                if score_a is None or score_b is None or (score_a == 0 and score_b == 0):
                    lines.append(f"\u2022 {map_name}: Winner {w_disp}")
                else:
                    lines.append(f"\u2022 {map_name}: {score_a}-{score_b} {w_disp}")

        if not has_real_data or not lines:
            return None

        return chr(10).join(lines)

    async def generate_leaderboard_embed(self, guild, game_slug: str):
        game_slug = game_slug if game_slug in GAMES else "valorant"
        data = load_data_sync()
        stats = data["leaderboards"].get(game_slug, {})

        if not stats:
            desc = "No data yet for this month."
        else:
            # Filter out corrupted entries and sort safely
            valid_stats = []
            for uid, s in stats.items():
                if isinstance(s, dict) and 'wins' in s and 'losses' in s:
                    valid_stats.append((uid, s))

            if not valid_stats:
                desc = "No data yet for this month."
            else:
                sorted_stats = sorted(valid_stats, key=lambda x: (x[1].get('wins', 0), -x[1].get('losses', 0)), reverse=True)[:10]
                lines = []
                for i, (uid, s) in enumerate(sorted_stats, 1):
                    try:
                        member = guild.get_member(int(uid))
                    except (ValueError, TypeError):
                        member = None
                    name = member.display_name if member else f"User {uid}"
                    if len(name) > MAX_LEADERBOARD_NAME_LENGTH: name = name[:MAX_LEADERBOARD_NAME_LENGTH] + ".."
                    wins = s.get('wins', 0)
                    losses = s.get('losses', 0)
                    streak = f" 🔥x{s.get('streak', 0)}" if s.get('streak', 0) >= 5 else ""
                    lines.append(f"**{i}.** {name} - **{wins}**W {losses}L{streak}")
                desc = "\n".join(lines)

        embed = discord.Embed(title=f"🏆 {GAMES.get(game_slug)} Monthly Leaderboard", color=discord.Color.gold(), description=desc)
        embed.set_footer(text="Resets monthly | Showing Top 10")
        return embed

    def build_minimal_match_embed(self, game_slug, game_name, match_details, team_a_data, team_b_data):
        """Build a compact upcoming match alert embed for the channel."""
        status = match_details.get('status', 'not_started')
        results = match_details.get('results', [])
        s_a, s_b = 0, 0
        if results:
            for r in results:
                if r.get('team_id') == team_a_data.get('id'): s_a = r.get('score', 0)
                elif r.get('team_id') == team_b_data.get('id'): s_b = r.get('score', 0)

        dt = safe_parse_datetime(match_details.get('begin_at'))
        if status == "running":
            title = f"🔴 Live: {team_a_data['name']} ({s_a}) vs {team_b_data['name']} ({s_b})"
            color = discord.Color.red()
        else:
            title = "🔔 Upcoming Match"
            color = discord.Color.green()

        f_a = self.get_team_display(team_a_data)
        f_b = self.get_team_display(team_b_data)
        link_a = self._liquipedia_team_url(team_a_data['name'], game_slug)
        link_b = self._liquipedia_team_url(team_b_data['name'], game_slug)

        desc = f"**{f_a} [{team_a_data['name']}]({link_a}) vs.**\n**{f_b} [{team_b_data['name']}]({link_b})**"

        embed = discord.Embed(title=title, description=desc, color=color)
        embed.set_thumbnail(url=GAME_LOGOS.get(game_slug))
        return embed

    def build_batch_match_embed(self, game_slug, game_name, matches_data):
        """Build a combined embed listing multiple upcoming matches for the same game.
        matches_data: list of (mid, match_details, team_a, team_b) tuples."""
        count = len(matches_data)
        title = f"🔔 {count} Upcoming Matches"
        embed = discord.Embed(title=title, color=discord.Color.green())
        embed.set_thumbnail(url=GAME_LOGOS.get(game_slug))
        embed.set_author(name=game_name, icon_url=GAME_LOGOS.get(game_slug))

        lines = []
        for mid, m, t_a, t_b in matches_data:
            f_a = self.get_team_display(t_a)
            f_b = self.get_team_display(t_b)
            link_a = self._liquipedia_team_url(t_a['name'], game_slug)
            link_b = self._liquipedia_team_url(t_b['name'], game_slug)
            dt = safe_parse_datetime(m.get('begin_at'))
            time_str = f" — <t:{int(dt.timestamp())}:t>" if dt else ""
            num_games = m.get('number_of_games')
            bo_str = f" (Bo{num_games})" if num_games else ""
            lines.append(f"{f_a} [{t_a['name']}]({link_a}) vs. {f_b} [{t_b['name']}]({link_b}){bo_str}{time_str}")

        embed.description = "\n\n".join(lines)

        # Event info — common prefix across all matches
        footer_text = self._common_event_footer([m for _, m, _, _ in matches_data])
        if footer_text:
            embed.set_footer(text=footer_text)

        return embed

    def build_unified_upcoming_embed(self, game_slug, upcoming_data, live_data=None):
        """Build a unified embed for all upcoming and live matches for a game.
        upcoming_data: list of (mid, match_details, team_a, team_b) — not_started matches
        live_data: list of (mid, match_details, team_a, team_b) — running matches
        """
        desc_parts = []

        if upcoming_data:
            desc_parts.append("### 🔔 Upcoming")
            desc_parts.append("")
            for mid, m, t_a, t_b in upcoming_data:
                f_a = self.get_team_display(t_a)
                f_b = self.get_team_display(t_b)
                link_a = self._liquipedia_team_url(t_a['name'], game_slug)
                link_b = self._liquipedia_team_url(t_b['name'], game_slug)
                dt = safe_parse_datetime(m.get('begin_at'))
                time_str = f"(<t:{int(dt.timestamp())}:R>)" if dt else ""
                desc_parts.append(f"> **{f_a} [{t_a['name']}]({link_a}) vs.**")
                desc_parts.append(f"> **{f_b} [{t_b['name']}]({link_b})**")
                if time_str:
                    desc_parts.append(f"> {time_str}")
                desc_parts.append("")

        if live_data:
            desc_parts.append("### 🔴 Live")
            desc_parts.append("")
            for mid, m, t_a, t_b in live_data:
                f_a = self.get_team_display(t_a)
                f_b = self.get_team_display(t_b)
                link_a = self._liquipedia_team_url(t_a['name'], game_slug)
                link_b = self._liquipedia_team_url(t_b['name'], game_slug)
                results = m.get('results', [])
                s_a, s_b = 0, 0
                if results:
                    for r in results:
                        if r.get('team_id') == t_a.get('id'): s_a = r.get('score', 0)
                        elif r.get('team_id') == t_b.get('id'): s_b = r.get('score', 0)
                desc_parts.append(f"> **{f_a} [{t_a['name']}]({link_a})({s_a}) vs.**")
                desc_parts.append(f"> **{f_b} [{t_b['name']}]({link_b})({s_b})**")
                desc_parts.append("")

        color = discord.Color.red() if live_data else discord.Color.green()
        embed = discord.Embed(description="\n".join(desc_parts), color=color)
        embed.set_author(name=GAMES.get(game_slug, game_slug), icon_url=GAME_LOGOS.get(game_slug))
        embed.set_thumbnail(url=GAME_LOGOS.get(game_slug))

        # Event info — common prefix across all matches
        all_matches = (upcoming_data or []) + (live_data or [])
        if all_matches:
            footer_text = self._common_event_footer([m for _, m, _, _ in all_matches])
            if footer_text:
                embed.set_footer(text=footer_text)

        return embed

    def _format_top5_block(self, guild, game_slug: str) -> Optional[str]:
        """Format Monthly Top 5 as a monospace code block with aligned columns.

        Returns a string like:
        Monthly Top 5 (Val)
        `TD:                 12W 14L
        jerkie:             11W 12Lx3
        ...`

        Returns None if no leaderboard data exists.
        """
        data = load_data_sync()
        stats = data["leaderboards"].get(game_slug, {})
        valid_stats = [(uid, s) for uid, s in stats.items()
                      if isinstance(s, dict) and 'wins' in s and 'losses' in s]
        sorted_s = sorted(valid_stats, key=lambda x: (x[1].get('wins', 0), -x[1].get('losses', 0)), reverse=True)[:5]
        if not sorted_s:
            return None

        NAME_COL = 20  # total width for "name:" column
        lines = []
        for uid, s in sorted_s:
            try:
                member = guild.get_member(int(uid))
            except (ValueError, TypeError):
                member = None
            name = member.display_name if member else f"User {uid}"
            name = name[:MAX_LEADERBOARD_NAME_LENGTH]
            streak = s.get('streak', 0)
            streak_str = f"🔥x{streak}" if streak >= 5 else ""
            wl = f"{s.get('wins', 0)}W {s.get('losses', 0)}L{streak_str}"
            name_part = f"{name}:"
            lines.append(f"{name_part:<{NAME_COL}}{wl}")

        short = GAME_SHORT_NAMES.get(game_slug, game_slug)
        block = "\n".join(lines)
        return f"**Monthly Top 5 ({short})**\n`{block}`"

    async def build_unified_result_embed(self, channel, game_slug, results_list, is_test=False):
        """Build a unified result embed for all results today for a game.
        results_list: list of dicts with keys: match_details, team_a, team_b, winner_idx, votes
        """
        game_name = GAMES.get(game_slug, game_slug)

        desc_parts = []

        for result in results_list:
            md = result['match_details']
            t_a = result['team_a']
            t_b = result['team_b']
            w_idx = result['winner_idx']
            votes = result['votes']

            scores = md.get('results', [])
            s_a = next((s.get('score') for s in scores if s.get('team_id') == t_a.get('id')), 0)
            s_b = next((s.get('score') for s in scores if s.get('team_id') == t_b.get('id')), 0)

            f_a = self.get_team_display(t_a)
            f_b = self.get_team_display(t_b)

            num_games = md.get('number_of_games')
            bo_str = f" (Bo{num_games})" if num_games else ""

            desc_parts.append(f"> {f_a} {t_a['name']} vs. {f_b} {t_b['name']}{bo_str}")

            # Spoilered winner announcement
            winner = t_a if w_idx == 0 else t_b
            w_score = s_a if w_idx == 0 else s_b
            l_score = s_b if w_idx == 0 else s_a
            verb = "Win" if winner['name'].rstrip().endswith('s') else "Wins"
            desc_parts.append(f"> ||{winner['name']} {verb} {w_score}-{l_score}!||")

            # Correct predictors
            if is_test:
                winners = ["TestUser"]
            else:
                winner_names = []
                for u, v in votes.items():
                    if v == w_idx:
                        try:
                            member = channel.guild.get_member(int(u))
                        except (ValueError, TypeError):
                            member = None
                        winner_names.append(member.display_name if member else f"User {u}")
                winners = winner_names
            w_text = ", ".join(winners)
            if len(w_text) > 500:
                w_text = f"{len(winners)} players!"
            desc_parts.append(f"> **Correct Predictors**")
            desc_parts.append(f"> {w_text or 'No one!'}")
            desc_parts.append("")

        # Monthly Top 5
        if not is_test:
            top5_block = self._format_top5_block(channel.guild, game_slug)
            if top5_block:
                desc_parts.append(top5_block)

        embed = discord.Embed(description="\n".join(desc_parts), color=discord.Color.greyple())
        embed.set_author(name=f"{game_name.upper()} RESULTS \U0001f3c6", icon_url=GAME_LOGOS.get(game_slug))

        count = sum(len(r['votes']) for r in results_list)
        embed.set_footer(text=f"{count} Total Vote{'s' if count != 1 else ''}")

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
        timestamp = ""
        if dt:
            timestamp = f"<t:{int(dt.timestamp())}:R>"

        if status == "running":
            title = f"🔴 Live: {team_a_data['name']} ({s_a}) vs {team_b_data['name']} ({s_b})"
            color = discord.Color.red()
        else:
            title = "🔔 Upcoming Match"
            color = discord.Color.green()

        f_a = self.get_team_display(team_a_data)
        f_b = self.get_team_display(team_b_data)
        link_a = self._liquipedia_team_url(team_a_data['name'], game_slug)
        link_b = self._liquipedia_team_url(team_b_data['name'], game_slug)

        num_games = match_details.get('number_of_games')
        bo_str = f" (Bo{num_games})" if num_games else ""

        desc = [f"{f_a} [{team_a_data['name']}]({link_a}) vs. {f_b} [{team_b_data['name']}]({link_b}){bo_str}", ""]
        
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
        # Voting is locked once the match start time has passed, even if PandaScore
        # hasn't flipped status to "running" yet.
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        voting_locked = status != "not_started" or (dt is not None and dt <= now_utc)
        if voting_locked and count > 0:
            a_v = sum(1 for v in votes.values() if v == 0)
            p_a = (a_v/count*100)
            embed.add_field(name="Server Picks", value=f"• {team_a_data['name']}: {p_a:.1f}%\n• {team_b_data['name']}: {100-p_a:.1f}%", inline=False)

        embed.set_footer(text=f"Predictions locked | {count} Votes" if voting_locked else f"Predictions Open")
        return embed

    async def build_minimal_result_embed(self, channel, game_slug, match_details, team_a, team_b, winner_idx, votes, is_test=False):
        """Build the compact result embed for the channel (no event info, no match history)."""
        saved = [team_a, team_b]
        winner = saved[winner_idx]
        scores = match_details.get('results', [])
        s1 = next((s.get('score') for s in scores if s.get('team_id') == team_a['id']), 0)
        s2 = next((s.get('score') for s in scores if s.get('team_id') == team_b['id']), 0)

        embed = discord.Embed(title=f"🏆 {GAMES.get(game_slug, 'ESPORTS').upper()} RESULTS", color=discord.Color.greyple())
        embed.set_author(name=GAMES.get(game_slug), icon_url=GAME_LOGOS.get(game_slug))

        num_games = match_details.get('number_of_games')
        bo_str = f" (Bo{num_games})" if num_games else ""

        winner_score, loser_score = (s1, s2) if winner_idx == 0 else (s2, s1)
        f_a = self.get_team_display(team_a)
        f_b = self.get_team_display(team_b)
        desc = [
            f"||**{winner['name']} Wins {winner_score}-{loser_score}!**||", "",
            f"{f_a} {team_a['name']} vs. {f_b} {team_b['name']}{bo_str}"
        ]
        embed.description = "\n".join(desc)

        # Correct predictors - plain text display names
        if is_test:
            winners = ["TestUser"]
        else:
            winner_names = []
            for u, v in votes.items():
                if v == winner_idx:
                    try:
                        member = channel.guild.get_member(int(u))
                    except (ValueError, TypeError):
                        member = None
                    winner_names.append(member.display_name if member else f"User {u}")
            winners = winner_names
        w_text = ", ".join(winners)
        if len(w_text) > 1000: w_text = f"{len(winners)} players!"
        embed.add_field(name="Correct Predictors", value=w_text or "No one!", inline=False)

        # Monthly Top 5 leaderboard
        if not is_test:
            top5_block = self._format_top5_block(channel.guild, game_slug)
            if top5_block:
                embed.add_field(name=f"Monthly Top 5 ({GAME_SHORT_NAMES.get(game_slug)})", value=top5_block, inline=False)

        count = len(votes)
        embed.set_footer(text=f"{count} Vote{'s' if count != 1 else ''}")
        return embed

    async def build_full_result_embed(self, game_slug, match_details, team_a, team_b, winner_idx, map_data=None, votes=None, guild=None):
        """Build the full result embed for the ephemeral Details view (event info + match history)."""
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

        winner_score, loser_score = (s1, s2) if winner_idx == 0 else (s2, s1)
        f_a = self.get_team_display(team_a)
        f_b = self.get_team_display(team_b)
        link_a = self._liquipedia_team_url(team_a['name'], game_slug)
        link_b = self._liquipedia_team_url(team_b['name'], game_slug)
        desc = [
            f"**{winner['name']} Wins {winner_score}-{loser_score}!**", "",
            f"{f_a} [{team_a['name']}]({link_a}) vs. {f_b} [{team_b['name']}]({link_b}){bo_str}", ""
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

        # Match History from map data
        map_hist = await self.get_map_history(match_details, saved, game_slug, map_data)
        if map_hist:
            embed.add_field(name="Match History", value=map_hist, inline=False)

        # Server Picks (vote percentages)
        if votes:
            count = len(votes)
            if count > 0:
                a_v = sum(1 for v in votes.values() if v == 0)
                p_a = a_v / count * 100
                embed.add_field(
                    name="Server Picks",
                    value=f"\u2022 {team_a['name']}: {p_a:.1f}%\n\u2022 {team_b['name']}: {100 - p_a:.1f}%",
                    inline=False
                )

        # Correct Predictors
        if votes is not None:
            winner_names = []
            for u, v in votes.items():
                if v == winner_idx:
                    member = None
                    if guild:
                        try:
                            member = guild.get_member(int(u))
                        except (ValueError, TypeError):
                            pass
                    winner_names.append(member.display_name if member else f"User {u}")
            w_text = ", ".join(winner_names)
            if len(w_text) > 1000:
                w_text = f"{len(winner_names)} players!"
            embed.add_field(name="Correct Predictors", value=w_text or "No one!", inline=False)

        return embed

    async def process_result(self, channel, info, winner_idx, details, saved_teams, map_data=None):
        match_id = str(details['id'])
        game_slug = info['game_slug']
        votes = info['votes']

        logger.info(
            f"Processing result for match {match_id}: "
            f"{saved_teams[0]['name']} (id={saved_teams[0].get('id')}) vs "
            f"{saved_teams[1]['name']} (id={saved_teams[1].get('id')}), "
            f"winner_idx={winner_idx} ({saved_teams[winner_idx]['name']}), "
            f"winner_id={details.get('winner_id')}, "
            f"results={details.get('results')}, "
            f"votes={votes}"
        )

        # Validate game_slug is a known game
        if game_slug not in GAMES:
            logger.error(f"Unknown game_slug '{game_slug}' in process_result for match {match_id}")
            return

        async with self.data_lock:
            data = load_data_sync()
            if match_id in data.get("processed_matches", []): return
            data["processed_matches"].append(match_id)
            if len(data["processed_matches"]) > MAX_PROCESSED_HISTORY: data["processed_matches"] = data["processed_matches"][-MAX_PROCESSED_HISTORY:]

            # Ensure the game's leaderboard exists (defensive check)
            if game_slug not in data["leaderboards"]:
                data["leaderboards"][game_slug] = {}

            for uid, vote in votes.items():
                # Ensure user ID is a string for consistency
                uid_str = str(uid)
                if uid_str not in data["leaderboards"][game_slug]:
                    data["leaderboards"][game_slug][uid_str] = {"wins": 0, "losses": 0, "streak": 0}
                s = data["leaderboards"][game_slug][uid_str]
                if vote == winner_idx:
                    s["wins"] += 1
                    s["streak"] = s.get("streak", 0) + 1
                else:
                    s["losses"] += 1
                    s["streak"] = 0

            # Save match history for 7-day retention (allows result overturns)
            if "match_history" not in data:
                data["match_history"] = {}
            data["match_history"][match_id] = {
                "game_slug": game_slug,
                "teams": [
                    {"name": saved_teams[0]["name"], "id": saved_teams[0].get("id"),
                     "flag": saved_teams[0].get("flag"), "acronym": saved_teams[0].get("acronym")},
                    {"name": saved_teams[1]["name"], "id": saved_teams[1].get("id"),
                     "flag": saved_teams[1].get("flag"), "acronym": saved_teams[1].get("acronym")}
                ],
                "winner_idx": winner_idx,
                "votes": votes,
                "processed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "winner_id": details.get("winner_id"),
                "results": details.get("results", []),
                "match_details_extra": {
                    "league": details.get("league", {}),
                    "serie": details.get("serie", {}),
                    "tournament": details.get("tournament", {}),
                    "number_of_games": details.get("number_of_games"),
                    "official_stream_url": details.get("official_stream_url")
                }
            }

            save_data_sync(data)

        # Build the single result embed for this game (one embed per game, max 3 results).
        # New results are prepended; the oldest is rotated off when a 4th arrives.
        # Each result has an 18hr lifespan enforced by result_lifecycle_checker.
        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

        async with self.data_lock:
            data = load_data_sync()
            result_info = data.get("daily_result_messages", {}).get(game_slug, {})
            existing_msg_id = result_info.get("message_id")
            existing_match_ids = result_info.get("match_ids", [])
            existing_added_at = result_info.get("match_added_at", {})

        # Always merge into the existing result embed (no date-based splitting).
        # Prepend new match, dedup in case of reprocessing.
        all_match_ids = [match_id] + [mid for mid in existing_match_ids if mid != match_id]

        # Build results_list from match_history
        async with self.data_lock:
            data = load_data_sync()
            history = data.get("match_history", {})

        valid_match_ids = []
        results_list = []
        for mid in all_match_ids:
            mh = history.get(mid)
            if not mh or len(mh.get("teams", [])) < 2:
                continue

            valid_match_ids.append(mid)
            extra = mh.get("match_details_extra", {})
            md = {
                "results": mh.get("results", []),
                "number_of_games": extra.get("number_of_games"),
                "league": extra.get("league", {}),
                "serie": extra.get("serie", {}),
                "tournament": extra.get("tournament", {}),
            }

            results_list.append({
                "match_details": md,
                "team_a": {"name": mh["teams"][0]["name"], "id": mh["teams"][0].get("id"),
                           "flag": mh["teams"][0].get("flag"), "acronym": mh["teams"][0].get("acronym")},
                "team_b": {"name": mh["teams"][1]["name"], "id": mh["teams"][1].get("id"),
                           "flag": mh["teams"][1].get("flag"), "acronym": mh["teams"][1].get("acronym")},
                "winner_idx": mh.get("winner_idx", 0),
                "votes": mh.get("votes", {})
            })

        if not results_list:
            return

        # Cap the embed to 3 most recent matches (newest first in all_match_ids)
        showcased_ids = valid_match_ids[:3]
        embed_results = results_list[:3]
        embed = await self.build_unified_result_embed(channel, game_slug, embed_results)

        # Build dropdown options from all tracked match IDs + recent history for this game
        dropdown_options = []
        seen_mids = set()

        for mid in all_match_ids:
            mh = history.get(mid)
            if mh and len(mh.get("teams", [])) >= 2:
                teams = mh["teams"]
                dropdown_options.append((mid, f"{teams[0]['name']} vs {teams[1]['name']}"))
                seen_mids.add(mid)

        # Also include recent history (last 48hrs) not already in all_match_ids
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=48)
        for mid, mh in history.items():
            if mid in seen_mids:
                continue
            if mh.get("game_slug") != game_slug:
                continue
            processed = safe_parse_datetime(mh.get("processed_at"))
            if processed and processed >= cutoff:
                teams = mh.get("teams", [])
                if len(teams) >= 2:
                    dropdown_options.append((mid, f"{teams[0]['name']} vs {teams[1]['name']}"))

        view = UnifiedResultView(game_slug, today, match_options=dropdown_options)

        # Delete old result message to bump (resend at bottom of channel)
        if existing_msg_id:
            try:
                old_msg = await channel.fetch_message(existing_msg_id)
                await old_msg.delete()
            except (discord.NotFound, discord.HTTPException):
                pass

        result_msg = None
        for attempt in range(3):
            try:
                result_msg = await channel.send(embed=embed, view=view)
                break
            except (asyncio.TimeoutError, TimeoutError) as e:
                if attempt < 2:
                    logger.warning(f"Timeout sending result for match {match_id}, retrying ({attempt+1}/3): {e}")
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Failed to send result for match {match_id} after 3 attempts: {e}")
                    raise

        if result_msg:
            async with self.data_lock:
                data = load_data_sync()
                if "daily_result_messages" not in data:
                    data["daily_result_messages"] = {}
                # Preserve existing timestamps for showcased results, add new ones
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                match_added_at = {}
                for mid in showcased_ids:
                    if mid in existing_added_at:
                        match_added_at[mid] = existing_added_at[mid]
                    else:
                        match_added_at[mid] = now_iso
                data["daily_result_messages"][game_slug] = {
                    "message_id": result_msg.id,
                    "channel_id": channel.id,
                    "date": today,
                    "match_ids": all_match_ids,
                    "showcased_ids": showcased_ids,
                    "match_added_at": match_added_at
                }
                save_data_sync(data)

    # --- CORE LOOPS ---
    def embeds_are_different(self, old, new):
        if not old or not new: return True
        if old.title != new.title or old.description != new.description or len(old.fields) != len(new.fields): return True
        old_author_icon = getattr(old.author, 'icon_url', None) if old.author else None
        new_author_icon = getattr(new.author, 'icon_url', None) if new.author else None
        old_thumb = old.thumbnail.url if old.thumbnail else None
        new_thumb = new.thumbnail.url if new.thumbnail else None
        if old_author_icon != new_author_icon or old_thumb != new_thumb: return True
        return any(o.value != n.value for o, n in zip(old.fields, new.fields))

    async def _rebuild_upcoming_embed(self, channel, game_slug, details_cache=None, bump=False):
        """Rebuild the unified upcoming embed for a game. Called after match state changes.
        If bump=True, deletes the old message and resends at the bottom of the channel."""
        if details_cache is None:
            details_cache = {}

        async with self.data_lock:
            data = load_data_sync()
            active = data.get("active_matches", {})
            upcoming_info = data.get("upcoming_messages", {}).get(game_slug, {})

        # Gather all non-test active matches for this game
        game_matches = {mid: info for mid, info in active.items()
                        if info.get('game_slug') == game_slug and not info.get('is_test')}

        if not game_matches:
            # No matches left — unpin and delete the upcoming message if it exists
            msg_id = upcoming_info.get("message_id")
            if msg_id:
                try:
                    msg = await channel.fetch_message(msg_id)
                    try:
                        await msg.unpin()
                    except discord.HTTPException:
                        pass
                    await msg.delete()
                except (discord.NotFound, discord.HTTPException):
                    pass
                async with self.data_lock:
                    d = load_data_sync()
                    d.get("upcoming_messages", {}).pop(game_slug, None)
                    save_data_sync(d)
            return

        # Categorise into upcoming and live
        upcoming_data = []
        live_data = []
        now = datetime.datetime.now(datetime.timezone.utc)

        for mid, info in game_matches.items():
            teams = info.get('teams', [])
            if len(teams) < 2:
                continue

            details = details_cache.get(mid)
            if not details:
                details = await self.get_pandascore_data(f"/matches/{mid}")
            if not details:
                details = {'begin_at': info.get('start_time'), 'status': 'not_started',
                           'results': [], 'league': {}, 'serie': {}, 'tournament': {},
                           'number_of_games': None}

            entry = (mid, details, teams[0], teams[1])
            if details.get('status') == 'running':
                live_data.append(entry)
            else:
                # Skip matches whose start time has passed — they shouldn't appear as upcoming
                start_dt = safe_parse_datetime(details.get('begin_at'))
                if start_dt and start_dt <= now:
                    continue
                upcoming_data.append(entry)

        # Sort upcoming by start time
        # Latest start time first — matches work their way down toward the Live section
        upcoming_data.sort(key=lambda x: safe_parse_datetime(x[1].get('begin_at')) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), reverse=True)

        if not upcoming_data and not live_data:
            return

        embed = self.build_unified_upcoming_embed(game_slug, upcoming_data, live_data)

        # Determine view: show Vote if any match is still votable
        now = datetime.datetime.now(datetime.timezone.utc)
        has_votable = False
        stream_url = None
        for mid, info in game_matches.items():
            start = safe_parse_datetime(info.get('start_time'))
            if start and start > now and not info.get('voting_locked'):
                has_votable = True
            if info.get('stream_url'):
                stream_url = info['stream_url']

        if has_votable:
            view = UnifiedUpcomingView(game_slug)
        else:
            view = discord.ui.View()
            if stream_url:
                view.add_item(discord.ui.Button(label="Watch Live", url=stream_url, emoji="📺"))
            # Still add Details button for locked view
            details_btn = discord.ui.Button(
                label="Details", style=discord.ButtonStyle.secondary,
                custom_id=f"unified_details_{game_slug}"
            )
            async def _show_details_locked(interaction, gs=game_slug):
                # Delegate to the UnifiedUpcomingView logic
                temp_view = UnifiedUpcomingView(gs)
                await temp_view.show_details(interaction)
            details_btn.callback = _show_details_locked
            view.add_item(details_btn)

        # Try to edit existing message (or bump by deleting + resending)
        msg_id = upcoming_info.get("message_id")
        if msg_id:
            if bump:
                # Bump: delete old message so a new one is sent at the bottom
                try:
                    old_msg = await channel.fetch_message(msg_id)
                    try:
                        await old_msg.unpin()
                    except discord.HTTPException:
                        pass
                    await old_msg.delete()
                except (discord.NotFound, discord.HTTPException):
                    pass
                async with self.data_lock:
                    d = load_data_sync()
                    if game_slug in d.get("upcoming_messages", {}):
                        d["upcoming_messages"][game_slug]["message_id"] = None
                        save_data_sync(d)
                logger.info(f"Bumping upcoming embed for {game_slug} (new match added)")
            else:
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(embed=embed, attachments=[], view=view)
                    return
                except discord.NotFound:
                    logger.info(f"Upcoming message for {game_slug} not found, sending new")
                except discord.HTTPException as e:
                    logger.warning(f"Failed to edit upcoming embed for {game_slug}: {e}. Will retry next cycle.")
                    return  # Don't create a duplicate — retry editing next cycle

        # Send new message and pin it
        try:
            msg = await channel.send(embed=embed, view=view)
            # Pin the upcoming embed silently (delete the system "pinned" message)
            try:
                await msg.pin()
                await asyncio.sleep(0.5)
                # Delete the "X pinned a message" system message
                async for sys_msg in channel.history(limit=5, after=msg):
                    if sys_msg.type == discord.MessageType.pins_add:
                        await sys_msg.delete()
                        break
            except discord.HTTPException as e:
                logger.debug(f"Failed to pin upcoming embed for {game_slug}: {e}")
            async with self.data_lock:
                d = load_data_sync()
                if "upcoming_messages" not in d:
                    d["upcoming_messages"] = {}
                d["upcoming_messages"][game_slug] = {"message_id": msg.id, "channel_id": channel.id}
                # Update all active matches for this game with the message_id
                for mid in game_matches:
                    if mid in d["active_matches"]:
                        d["active_matches"][mid]['message_id'] = msg.id
                save_data_sync(d)
        except Exception as e:
            logger.error(f"Failed to send upcoming embed for {game_slug}: {e}")

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

                # Purge match history older than 7 days
                history = data.get("match_history", {})
                if history:
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    expired = [
                        mid for mid, mh in history.items()
                        if (now_utc - safe_parse_datetime(mh.get("processed_at", ""))).total_seconds() > 7 * 86400
                        if safe_parse_datetime(mh.get("processed_at"))
                    ]
                    if expired:
                        for mid in expired:
                            del history[mid]
                        # Also purge corresponding map data cache entries
                        map_cache = data.get("map_data_cache", {})
                        for mid in expired:
                            map_cache.pop(mid, None)
                        logger.info(f"Purged {len(expired)} match history entries older than 7 days")
                        save_data_sync(data)

            if not chan_id: return
            channel = self.bot.get_channel(chan_id)
            if not channel: return

            now = datetime.datetime.now(datetime.timezone.utc)
            games_needing_refresh = set()  # game slugs whose unified upcoming embed needs rebuild
            games_needing_bump = set()  # game slugs that got new matches (bump to bottom of channel)
            fetched_details = {}  # mid -> API details (used by _rebuild_upcoming_embed)

            # 1. FETCH NEW
            for slug, name in GAMES.items():
                p = {"filter[status]": "not_started,running", "sort": "begin_at", "range[begin_at]": f"{(now - datetime.timedelta(hours=12)).isoformat()},{(now + datetime.timedelta(hours=24)).isoformat()}"}
                matches = await self.get_pandascore_data(f"/{slug}/matches", params=p)

                if not matches:
                    continue

                # Collect qualified new matches before sending
                pending_new = []  # list of (mid, match_details, team_a, team_b)

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

                        # Team-pair dedup: PandaScore can regenerate match IDs for the same matchup
                        # (especially during playoff bracket updates), so also check by team IDs
                        if not should_skip and len(m.get('opponents', [])) >= 2:
                            new_team_ids = {m['opponents'][0]['opponent']['id'], m['opponents'][1]['opponent']['id']}
                            for existing_mid, existing_info in data["active_matches"].items():
                                if existing_info.get('game_slug') != slug:
                                    continue
                                existing_teams = existing_info.get('teams', [])
                                if len(existing_teams) >= 2:
                                    existing_team_ids = {existing_teams[0].get('id'), existing_teams[1].get('id')}
                                    if new_team_ids == existing_team_ids:
                                        logger.info(f"Skipping match {mid}: same team pair already active as {existing_mid}")
                                        should_skip = True
                                        break

                    if should_skip or not self.is_quality_match(m, slug) or len(m.get('opponents',[])) < 2:
                        async with self.processing_lock:
                            self.processing_matches.discard(mid)
                        continue

                    try:
                        t_a_base, t_b_base = m['opponents'][0]['opponent'], m['opponents'][1]['opponent']
                        t_a = {"name": t_a_base['name'], "acronym": t_a_base.get('acronym'), "id": t_a_base['id'], "roster": await self.fetch_roster(t_a_base['id'], t_a_base['name'], slug), "flag": t_a_base.get('location'), "image_url": t_a_base.get('image_url')}
                        t_b = {"name": t_b_base['name'], "acronym": t_b_base.get('acronym'), "id": t_b_base['id'], "roster": await self.fetch_roster(t_b_base['id'], t_b_base['name'], slug), "flag": t_b_base.get('location'), "image_url": t_b_base.get('image_url')}
                        pending_new.append((mid, m, t_a, t_b))
                    except Exception as e:
                        logger.error(f"Init match {mid} failed: {e}")
                        async with self.processing_lock:
                            self.processing_matches.discard(mid)

                # Verify matches against Liquipedia before posting
                if pending_new:
                    verified_new = await self._verify_matches_liquipedia(slug, pending_new)
                    if len(verified_new) < len(pending_new):
                        rejected = len(pending_new) - len(verified_new)
                        logger.info(f"Liquipedia verification: {rejected} match(es) for {slug} failed verification and were skipped")
                        # Release processing locks for rejected matches
                        verified_mids = {mid for mid, _, _, _ in verified_new}
                        async with self.processing_lock:
                            for mid, _, _, _ in pending_new:
                                if mid not in verified_mids:
                                    self.processing_matches.discard(mid)
                    pending_new = verified_new

                # Store new matches and mark game for unified embed rebuild
                if pending_new:
                    try:
                        async with self.data_lock:
                            d = load_data_sync()
                            for mid, m, t_a, t_b in pending_new:
                                d["active_matches"][mid] = {
                                    "message_id": None, "channel_id": chan_id, "game_slug": slug,
                                    "start_time": m['begin_at'], "teams": [t_a, t_b], "votes": {},
                                    "fail_count": 0, "stream_url": m.get('official_stream_url'), "status": "active"
                                }
                                fetched_details[mid] = m
                            save_data_sync(d)
                        games_needing_refresh.add(slug)
                        games_needing_bump.add(slug)
                    except Exception as e:
                        logger.error(f"Storing new matches for {slug} failed: {e}")
                    finally:
                        async with self.processing_lock:
                            for mid, _, _, _ in pending_new:
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
                        games_needing_refresh.add(info.get('game_slug'))
                        continue

                teams = info.get('teams', [])
                if len(teams) < 2:
                    to_remove.append(mid)
                    games_needing_refresh.add(info.get('game_slug'))
                    continue

                details = await self.get_pandascore_data(f"/matches/{mid}")
                if not details: continue
                fetched_details[mid] = details

                status = details['status']

                # Track vote locking (per-match, used by unified embed rebuild)
                if not info.get('voting_locked') and start and now >= start:
                    async with self.data_lock:
                        d = load_data_sync()
                        if mid in d["active_matches"]:
                            d["active_matches"][mid]['voting_locked'] = True
                            save_data_sync(d)
                    games_needing_refresh.add(info.get('game_slug'))

                if status in ["running", "not_started"]:
                    # Check if status changed — triggers unified embed rebuild
                    old_status = info.get('last_known_status', 'not_started')
                    if old_status != status:
                        games_needing_refresh.add(info.get('game_slug'))
                        async with self.data_lock:
                            d = load_data_sync()
                            if mid in d["active_matches"]:
                                d["active_matches"][mid]['last_known_status'] = status
                                save_data_sync(d)
                    elif status == "running":
                        # Check if scores changed (live update)
                        old_results = info.get('last_details_results')
                        new_results = details.get('results', [])
                        if old_results != new_results:
                            games_needing_refresh.add(info.get('game_slug'))
                            async with self.data_lock:
                                d = load_data_sync()
                                if mid in d["active_matches"]:
                                    d["active_matches"][mid]['last_details_results'] = new_results
                                    save_data_sync(d)

                    # Liquipedia fallback: if match has been "running" for 3+ hours
                    if status == "running" and start and (now - start).total_seconds() > 10800:
                        last_lp_check = safe_parse_datetime(info.get('last_liquipedia_check'))
                        if not last_lp_check or (now - last_lp_check).total_seconds() > 900:
                            async with self.data_lock:
                                d = load_data_sync()
                                if mid in d["active_matches"]:
                                    d["active_matches"][mid]['last_liquipedia_check'] = now.isoformat()
                                    save_data_sync(d)

                            lp_winner = await self.check_liquipedia_match_result(
                                teams[0]['name'], teams[1]['name'], info['game_slug']
                            )
                            if lp_winner is not None:
                                logger.info(
                                    f"Match {mid}: PandaScore stuck on 'running' but Liquipedia confirms "
                                    f"winner is team {lp_winner} ({teams[lp_winner]['name']}). Processing result."
                                )
                                match_time = safe_parse_datetime(info.get('start_time'))
                                map_data = await self.get_map_data(
                                    teams[0]['name'], teams[1]['name'], info['game_slug'],
                                    match_time, winner_idx=lp_winner, match_details=details
                                )
                                if map_data and self._is_map_data_displayable(map_data):
                                    async with self.data_lock:
                                        d = load_data_sync()
                                        if "map_data_cache" not in d:
                                            d["map_data_cache"] = {}
                                        d["map_data_cache"][mid] = {"maps": map_data, "fetched_at": now.isoformat()}
                                        save_data_sync(d)

                                async with self.data_lock:
                                    fresh_data = load_data_sync()
                                    fresh_info = fresh_data["active_matches"].get(mid, info)

                                await self.process_result(channel, fresh_info, lp_winner, details, teams, map_data)
                                to_remove.append(mid)
                                games_needing_refresh.add(info.get('game_slug'))
                                continue

                elif status == "finished":
                    wid = details.get('winner_id')
                    w_idx = -1
                    if wid == teams[0].get('id'): w_idx = 0
                    elif wid == teams[1].get('id'): w_idx = 1

                    # Cross-validate winner_id against results scores
                    if w_idx != -1:
                        results = details.get('results', [])
                        if results:
                            score_a = next((r.get('score', 0) for r in results if r.get('team_id') == teams[0].get('id')), 0)
                            score_b = next((r.get('score', 0) for r in results if r.get('team_id') == teams[1].get('id')), 0)
                            if (score_a or 0) != (score_b or 0):
                                score_winner_idx = 0 if (score_a or 0) > (score_b or 0) else 1
                                if score_winner_idx != w_idx:
                                    fail_count = info.get('winner_mismatch_count', 0) + 1
                                    logger.warning(
                                        f"Match {mid}: winner_id says team {w_idx} ({teams[w_idx]['name']}) "
                                        f"but results scores say team {score_winner_idx} ({teams[score_winner_idx]['name']}) "
                                        f"(scores: {score_a}-{score_b}). Attempt {fail_count}/3"
                                    )
                                    if fail_count < 3:
                                        async with self.data_lock:
                                            d = load_data_sync()
                                            if mid in d["active_matches"]:
                                                d["active_matches"][mid]['winner_mismatch_count'] = fail_count
                                                save_data_sync(d)
                                        continue
                                    else:
                                        logger.warning(
                                            f"Match {mid}: overriding winner_id with results scores after {fail_count} mismatches. "
                                            f"Using team {score_winner_idx} ({teams[score_winner_idx]['name']}) as winner"
                                        )
                                        w_idx = score_winner_idx

                    if w_idx != -1:
                        async with self.data_lock:
                            pre_check = load_data_sync()
                            if mid in pre_check.get("processed_matches", []):
                                logger.info(f"Match {mid} already in processed_matches, skipping")
                                to_remove.append(mid)
                                games_needing_refresh.add(info.get('game_slug'))
                                continue

                        match_time = safe_parse_datetime(info.get('start_time'))
                        map_data = await self.get_map_data(
                            teams[0]['name'], teams[1]['name'], info['game_slug'],
                            match_time, winner_idx=w_idx, match_details=details
                        )
                        if map_data:
                            logger.info(f"Successfully fetched {len(map_data)} maps for match {mid}")
                            if self._is_map_data_displayable(map_data):
                                async with self.data_lock:
                                    d = load_data_sync()
                                    if "map_data_cache" not in d:
                                        d["map_data_cache"] = {}
                                    d["map_data_cache"][mid] = {
                                        "maps": map_data,
                                        "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                                    }
                                    save_data_sync(d)
                                if not self._is_map_data_complete(map_data):
                                    logger.info(f"Map data for {mid} cached (displayable) but lacks real names, fetcher will keep retrying")

                        async with self.data_lock:
                            fresh_data = load_data_sync()
                            fresh_info = fresh_data["active_matches"].get(mid, info)

                        await self.process_result(channel, fresh_info, w_idx, details, teams, map_data)
                        to_remove.append(mid)
                        games_needing_refresh.add(info.get('game_slug'))
                    elif details.get('draw') or status == "canceled":
                        to_remove.append(mid)
                        games_needing_refresh.add(info.get('game_slug'))

            if to_remove:
                async with self.data_lock:
                    d = load_data_sync()
                    for m in to_remove: d["active_matches"].pop(m, None)
                    save_data_sync(d)

            # 3. REBUILD unified upcoming embeds for all affected games
            for slug in games_needing_refresh:
                try:
                    await self._rebuild_upcoming_embed(channel, slug, fetched_details, bump=(slug in games_needing_bump))
                except Exception as e:
                    logger.error(f"Failed to rebuild upcoming embed for {slug}: {e}")

        except Exception as e: await self.report_error(f"Tracker Loop: {e}")

    async def _migrate_result_embeds(self):
        """One-time migration: strip leaderboard from old result embeds, remove Details button emoji."""
        try:
            data = load_data_sync()
            chan_id = data.get("channel_id")
            if not chan_id:
                return
            channel = self.bot.get_channel(chan_id)
            if not channel:
                return

            cur_month = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")
            logger.info("Running one-time result embed migration...")

            # Scan recent channel history for result embeds (up to 200 messages)
            # Group by game slug to find the latest per game
            result_msgs_by_game = {}  # game_slug -> list of (message, embed) sorted newest first

            async for msg in channel.history(limit=200):
                if not msg.embeds or msg.author.id != self.bot.user.id:
                    continue
                embed = msg.embeds[0]
                if not embed.title or "RESULTS" not in embed.title:
                    continue

                # Identify the game from the author name or title
                game_slug = None
                author_name = embed.author.name if embed.author else ""
                for slug, name in GAMES.items():
                    if name and (name == author_name or name.upper() in (embed.title or "").upper()):
                        game_slug = slug
                        break
                if not game_slug:
                    continue

                if game_slug not in result_msgs_by_game:
                    result_msgs_by_game[game_slug] = []
                result_msgs_by_game[game_slug].append(msg)

            edits = 0
            for game_slug, msgs in result_msgs_by_game.items():
                # msgs are already newest-first from channel.history
                for i, msg in enumerate(msgs):
                    embed = msg.embeds[0]
                    needs_edit = False

                    # Rebuild embed without Monthly Top 5 (except the latest one, i==0)
                    new_embed = discord.Embed(
                        title=embed.title, description=embed.description,
                        color=embed.color, url=embed.url
                    )
                    if embed.author:
                        new_embed.set_author(name=embed.author.name, icon_url=embed.author.icon_url)
                    if embed.footer:
                        new_embed.set_footer(text=embed.footer.text)
                    if embed.thumbnail:
                        new_embed.set_thumbnail(url=embed.thumbnail.url)

                    for field in embed.fields:
                        if "Monthly Top 5" in (field.name or "") and i > 0:
                            needs_edit = True
                            continue  # strip from non-latest
                        new_embed.add_field(name=field.name, value=field.value, inline=field.inline)

                    # Check if Details button has emoji
                    has_emoji_btn = False
                    for row in msg.components:
                        for comp in row.children:
                            if hasattr(comp, 'label') and comp.label == "Details" and comp.emoji:
                                has_emoji_btn = True
                                break

                    if needs_edit or has_emoji_btn:
                        # Find match_id from the button custom_id
                        match_id = None
                        for row in msg.components:
                            for comp in row.children:
                                cid = getattr(comp, 'custom_id', '') or ''
                                if cid.startswith("result_details_"):
                                    match_id = cid.replace("result_details_", "")
                                    break

                        view = ResultDetailsView(match_id, game_slug) if match_id else None
                        try:
                            await msg.edit(embed=new_embed, view=view)
                            edits += 1
                        except (discord.HTTPException, asyncio.TimeoutError) as e:
                            logger.debug(f"Migration edit failed for msg {msg.id}: {e}")
                        await asyncio.sleep(1)  # rate limit

                # Track the latest result message for future leaderboard management
                if msgs:
                    latest = msgs[0]
                    match_id = None
                    for row in latest.components:
                        for comp in row.children:
                            cid = getattr(comp, 'custom_id', '') or ''
                            if cid.startswith("result_details_"):
                                match_id = cid.replace("result_details_", "")
                                break
                    if match_id:
                        async with self.data_lock:
                            d = load_data_sync()
                            if "last_result_message" not in d:
                                d["last_result_message"] = {}
                            d["last_result_message"][game_slug] = {
                                "message_id": latest.id,
                                "channel_id": channel.id,
                                "match_id": match_id,
                                "month": cur_month
                            }
                            save_data_sync(d)

            # Mark migration complete
            async with self.data_lock:
                d = load_data_sync()
                d["_migrated_result_embeds_v1"] = True
                save_data_sync(d)

            logger.info(f"Result embed migration complete: {edits} embeds updated")

        except Exception as e:
            logger.error(f"Result embed migration failed: {e}")

    @match_tracker.before_loop
    async def before_match_tracker(self):
        await self.bot.wait_until_ready()
        self.processing_matches.clear()
        data = load_data_sync()

        # Register unified upcoming views for each game with active matches
        registered_games = set()
        for mid, info in data.get("active_matches", {}).items():
            game_slug = info.get('game_slug', '')
            if game_slug and game_slug not in registered_games:
                self.bot.add_view(UnifiedUpcomingView(game_slug))
                registered_games.add(game_slug)

        # Register unified result views for daily results
        for game_slug, daily_info in data.get("daily_result_messages", {}).items():
            result_date = daily_info.get("date", "")
            if result_date:
                self.bot.add_view(UnifiedResultView(game_slug, result_date))

        # Backward compat: register old views for pre-update messages still in channel
        registered_batches = set()
        for mid, info in data.get("active_matches", {}).items():
            game_slug = info.get('game_slug', '')
            batch_msg_id = info.get('batch_message_id')
            if batch_msg_id:
                if batch_msg_id not in registered_batches:
                    batch_ids = info.get('batch_match_ids', [mid])
                    self.bot.add_view(BatchVoteRevealView(batch_ids, game_slug))
                    registered_batches.add(batch_msg_id)
            else:
                self.bot.add_view(VoteRevealView(mid, game_slug))
        for mid, mh in data.get("match_history", {}).items():
            self.bot.add_view(ResultDetailsView(mid, mh.get('game_slug', '')))

        # Cleanup orphaned upcoming embeds (caused by past HTTPException fallthrough)
        await self._cleanup_orphaned_upcoming_embeds(data)

    async def _cleanup_orphaned_upcoming_embeds(self, data):
        """Delete any upcoming-style embeds in the channel that aren't tracked in upcoming_messages.

        Orphans occur when an edit fails and a new message is sent without deleting the old one.
        """
        chan_id = data.get("channel_id")
        if not chan_id:
            return
        channel = self.bot.get_channel(chan_id)
        if not channel:
            return

        # Collect all tracked upcoming message IDs
        tracked_ids = set()
        for game_slug, info in data.get("upcoming_messages", {}).items():
            msg_id = info.get("message_id")
            if msg_id:
                tracked_ids.add(msg_id)

        if not tracked_ids:
            return

        # Scan recent history for bot messages with upcoming-embed characteristics
        try:
            orphans_deleted = 0
            async for msg in channel.history(limit=100):
                if msg.author.id != self.bot.user.id:
                    continue
                if msg.id in tracked_ids:
                    continue
                # Identify upcoming embeds by their view's custom_id pattern
                if not msg.embeds:
                    continue
                # Check if this message has unified upcoming view buttons
                has_upcoming_buttons = False
                for component in (msg.components or []):
                    for child in getattr(component, 'children', []):
                        cid = getattr(child, 'custom_id', '') or ''
                        if cid.startswith('unified_vote_') or cid.startswith('unified_details_'):
                            has_upcoming_buttons = True
                            break
                    if has_upcoming_buttons:
                        break
                if has_upcoming_buttons:
                    try:
                        try:
                            await msg.unpin()
                        except discord.HTTPException:
                            pass
                        await msg.delete()
                        orphans_deleted += 1
                        logger.info(f"Deleted orphaned upcoming embed: message {msg.id}")
                    except discord.HTTPException:
                        pass
            if orphans_deleted:
                logger.info(f"Startup cleanup: deleted {orphans_deleted} orphaned upcoming embed(s)")
        except Exception as e:
            logger.warning(f"Orphan cleanup failed: {e}")

    @staticmethod
    def _get_retry_interval_seconds(age_seconds: float) -> int:
        """Return the minimum interval between retry attempts based on match age.

        Backoff schedule:
          0-1h:   every 10 minutes  (600s)
          1-6h:   every 30 minutes  (1800s)
          6-24h:  every 2 hours     (7200s)
          24-48h: every 4 hours     (14400s)
        """
        if age_seconds < 3600:       # 0-1h
            return 600
        elif age_seconds < 21600:    # 1-6h
            return 1800
        elif age_seconds < 86400:    # 6-24h
            return 7200
        else:                        # 24-48h
            return 14400

    @tasks.loop(minutes=10)
    async def map_data_fetcher(self):
        """Background task to retry fetching map data for recent results missing it."""
        try:
            async with self.data_lock:
                data = load_data_sync()
                history = data.get("match_history", {})
                cache = data.get("map_data_cache", {})

            now = datetime.datetime.now(datetime.timezone.utc)
            for mid, mh in history.items():
                # Skip if we already have COMPLETE cached map data
                cached_entry = cache.get(mid, {})
                cached_maps = cached_entry.get("maps")
                if cached_maps and self._is_map_data_complete(cached_maps):
                    continue
                # Skip Rocket League
                if mh.get("game_slug") == "rl":
                    continue
                # Retry for up to 48 hours with exponential backoff
                processed_at = safe_parse_datetime(mh.get("processed_at"))
                if not processed_at:
                    continue
                age_seconds = (now - processed_at).total_seconds()
                if age_seconds > 172800:  # 48 hours
                    continue

                # Check backoff interval — skip if too soon since last attempt
                last_attempt = safe_parse_datetime(cached_entry.get("last_fetch_attempt"))
                if last_attempt:
                    since_last = (now - last_attempt).total_seconds()
                    interval = self._get_retry_interval_seconds(age_seconds)
                    if since_last < interval:
                        continue

                teams = mh.get("teams", [])
                if len(teams) < 2:
                    continue

                age_minutes = age_seconds / 60
                match_details_extra = mh.get('match_details_extra') or {}

                # Build a more complete match_details dict so Gemini gets the
                # series score context (match_details_extra lacks results/opponents)
                match_details_for_fetch = dict(match_details_extra)
                match_details_for_fetch['results'] = mh.get('results', [])
                match_details_for_fetch['opponents'] = [
                    {'opponent': {'id': teams[0].get('id'), 'name': teams[0]['name']}},
                    {'opponent': {'id': teams[1].get('id'), 'name': teams[1]['name']}}
                ]

                # Record this attempt timestamp
                async with self.data_lock:
                    d = load_data_sync()
                    if "map_data_cache" not in d:
                        d["map_data_cache"] = {}
                    if mid not in d["map_data_cache"]:
                        d["map_data_cache"][mid] = {}
                    d["map_data_cache"][mid]["last_fetch_attempt"] = now.isoformat()
                    save_data_sync(d)

                logger.info(f"Map data fetcher: retrying for match {mid} ({teams[0]['name']} vs {teams[1]['name']}, age={age_minutes:.0f}m)")
                try:
                    map_data = await self.get_map_data(
                        teams[0]['name'], teams[1]['name'],
                        mh['game_slug'], processed_at,
                        winner_idx=mh.get('winner_idx'),
                        match_details=match_details_for_fetch
                    )

                    if map_data and self._is_map_data_displayable(map_data):
                        async with self.data_lock:
                            d = load_data_sync()
                            if "map_data_cache" not in d:
                                d["map_data_cache"] = {}
                            d["map_data_cache"][mid] = {
                                "maps": map_data,
                                "fetched_at": now.isoformat()
                            }
                            save_data_sync(d)
                        if self._is_map_data_complete(map_data):
                            logger.info(f"Map data fetcher: successfully cached {len(map_data)} maps with real names for match {mid}")
                        else:
                            logger.info(f"Map data fetcher: cached {len(map_data)} maps (floor data) for match {mid}, will keep retrying")
                except Exception as e:
                    logger.error(f"Map data fetcher error for match {mid}: {e}")

        except Exception as e:
            logger.error(f"Map data fetcher loop error: {e}")

    @map_data_fetcher.before_loop
    async def before_map_data_fetcher(self):
        await self.bot.wait_until_ready()

    # --- UPCOMING EMBED BUMPER (11 PM LOCAL) ---

    @tasks.loop(time=datetime.time(hour=23, minute=0, tzinfo=zoneinfo.ZoneInfo("America/Chicago")))
    async def upcoming_embed_bumper(self):
        """Bump all active upcoming match embeds at 11pm local time.

        Deletes the old embed and resends it so it appears at the bottom of the
        channel, making sure upcoming matches stay visible even if they get buried.
        The new embed is re-pinned automatically by _rebuild_upcoming_embed.
        """
        try:
            async with self.data_lock:
                data = load_data_sync()
                chan_id = data.get("channel_id")
                upcoming_messages = data.get("upcoming_messages", {}).copy()

            if not chan_id:
                return
            channel = self.bot.get_channel(chan_id)
            if not channel:
                return

            bumped = []
            for game_slug, info in upcoming_messages.items():
                msg_id = info.get("message_id")
                if not msg_id:
                    continue

                # Check if there are active non-test matches for this game
                has_active = any(
                    m_info.get('game_slug') == game_slug and not m_info.get('is_test')
                    for m_info in data.get("active_matches", {}).values()
                )
                if not has_active:
                    continue

                # Delete old message (unpin happens implicitly on delete)
                try:
                    old_msg = await channel.fetch_message(msg_id)
                    try:
                        await old_msg.unpin()
                    except discord.HTTPException:
                        pass
                    await old_msg.delete()
                except (discord.NotFound, discord.HTTPException):
                    pass

                # Clear the stored message_id so rebuild sends a fresh message
                async with self.data_lock:
                    d = load_data_sync()
                    if game_slug in d.get("upcoming_messages", {}):
                        d["upcoming_messages"][game_slug]["message_id"] = None
                        save_data_sync(d)

                # Rebuild sends a new embed at the bottom and pins it
                try:
                    await self._rebuild_upcoming_embed(channel, game_slug)
                    bumped.append(game_slug)
                except Exception as e:
                    logger.error(f"Upcoming bumper: failed to rebuild for {game_slug}: {e}")

                await asyncio.sleep(1)  # rate limit between games

            if bumped:
                logger.info(f"Upcoming embed bumper: bumped {', '.join(bumped)}")

        except Exception as e:
            logger.error(f"Upcoming embed bumper error: {e}")

    @upcoming_embed_bumper.before_loop
    async def before_upcoming_embed_bumper(self):
        await self.bot.wait_until_ready()

    # --- RESULT LIFECYCLE (18hr per match result) ---

    RESULT_LIFETIME_SECONDS = 18 * 3600  # 18 hours

    @tasks.loop(minutes=15)
    async def result_lifecycle_checker(self):
        """Remove individual match results from the result embed after 18hrs.
        When all showcased results expire, delete the embed entirely."""
        try:
            now = datetime.datetime.now(datetime.timezone.utc)

            async with self.data_lock:
                data = load_data_sync()
                chan_id = data.get("channel_id")
                daily_results = data.get("daily_result_messages", {}).copy()

            if not chan_id:
                return
            channel = self.bot.get_channel(chan_id)
            if not channel:
                return

            for game_slug, info in daily_results.items():
                msg_id = info.get("message_id")
                showcased_ids = info.get("showcased_ids", [])
                match_added_at = info.get("match_added_at", {})

                if not msg_id:
                    continue

                # Legacy entries without showcased_ids: clean up stale data
                if not showcased_ids:
                    async with self.data_lock:
                        d = load_data_sync()
                        d.get("daily_result_messages", {}).pop(game_slug, None)
                        save_data_sync(d)
                    # Best-effort delete of the orphaned message
                    try:
                        old_msg = await channel.fetch_message(msg_id)
                        await old_msg.delete()
                    except (discord.NotFound, discord.HTTPException):
                        pass
                    logger.info(f"Result lifecycle: cleaned up legacy result entry for {game_slug}")
                    continue

                # Find which showcased results are still within their 18hr window
                remaining = []
                for mid in showcased_ids:
                    added_str = match_added_at.get(mid)
                    if not added_str:
                        remaining.append(mid)  # No timestamp — keep (legacy data)
                        continue
                    added_dt = safe_parse_datetime(added_str)
                    if not added_dt or (now - added_dt).total_seconds() <= self.RESULT_LIFETIME_SECONDS:
                        remaining.append(mid)

                if remaining == showcased_ids:
                    # Nothing expired — but verify the message still exists
                    try:
                        await channel.fetch_message(msg_id)
                    except discord.NotFound:
                        # Message was manually deleted — clean up stale data
                        async with self.data_lock:
                            d = load_data_sync()
                            d.get("daily_result_messages", {}).pop(game_slug, None)
                            save_data_sync(d)
                        logger.info(f"Result lifecycle: cleaned up stale data for {game_slug} (message deleted externally)")
                    except discord.HTTPException:
                        pass
                    continue

                # Clean up data FIRST, then delete/edit the message.
                # This order ensures data never references a stale message_id —
                # if the bot crashes after data save but before Discord delete,
                # the next cycle won't find the entry and the message is just
                # a static embed with no harm (vs. the reverse where data
                # references a deleted message and loops on NotFound forever).

                if not remaining:
                    # All showcased results expired — clean up data, then delete
                    async with self.data_lock:
                        d = load_data_sync()
                        d.get("daily_result_messages", {}).pop(game_slug, None)
                        save_data_sync(d)
                    try:
                        old_msg = await channel.fetch_message(msg_id)
                        await old_msg.delete()
                    except (discord.NotFound, discord.HTTPException):
                        pass
                    logger.info(f"Result lifecycle: deleted expired result embed for {game_slug}")
                    continue

                # Some expired — rebuild embed with remaining showcased results
                async with self.data_lock:
                    data = load_data_sync()
                    history = data.get("match_history", {})

                results_list = []
                for mid in remaining:
                    mh = history.get(mid)
                    if not mh or len(mh.get("teams", [])) < 2:
                        continue
                    extra = mh.get("match_details_extra", {})
                    md = {
                        "results": mh.get("results", []),
                        "number_of_games": extra.get("number_of_games"),
                        "league": extra.get("league", {}),
                        "serie": extra.get("serie", {}),
                        "tournament": extra.get("tournament", {}),
                    }
                    results_list.append({
                        "match_details": md,
                        "team_a": {"name": mh["teams"][0]["name"], "id": mh["teams"][0].get("id"),
                                   "flag": mh["teams"][0].get("flag"), "acronym": mh["teams"][0].get("acronym")},
                        "team_b": {"name": mh["teams"][1]["name"], "id": mh["teams"][1].get("id"),
                                   "flag": mh["teams"][1].get("flag"), "acronym": mh["teams"][1].get("acronym")},
                        "winner_idx": mh.get("winner_idx", 0),
                        "votes": mh.get("votes", {})
                    })

                if not results_list:
                    # No valid results left — clean up data, then delete
                    async with self.data_lock:
                        d = load_data_sync()
                        d.get("daily_result_messages", {}).pop(game_slug, None)
                        save_data_sync(d)
                    try:
                        old_msg = await channel.fetch_message(msg_id)
                        await old_msg.delete()
                    except (discord.NotFound, discord.HTTPException):
                        pass
                    logger.info(f"Result lifecycle: deleted result embed for {game_slug} (no valid results)")
                    continue

                embed = await self.build_unified_result_embed(channel, game_slug, results_list)

                # Rebuild dropdown options from all match_ids
                all_match_ids = info.get("match_ids", [])
                dropdown_options = []
                for mid in all_match_ids:
                    mh = history.get(mid)
                    if mh and len(mh.get("teams", [])) >= 2:
                        teams = mh["teams"]
                        dropdown_options.append((mid, f"{teams[0]['name']} vs {teams[1]['name']}"))

                today = now.strftime("%Y-%m-%d")
                view = UnifiedResultView(game_slug, today, match_options=dropdown_options)

                # Update data FIRST, then edit the message
                remaining_added_at = {mid: match_added_at[mid] for mid in remaining if mid in match_added_at}
                async with self.data_lock:
                    d = load_data_sync()
                    if game_slug in d.get("daily_result_messages", {}):
                        d["daily_result_messages"][game_slug]["showcased_ids"] = remaining
                        d["daily_result_messages"][game_slug]["match_added_at"] = remaining_added_at
                        save_data_sync(d)

                try:
                    old_msg = await channel.fetch_message(msg_id)
                    await old_msg.edit(embed=embed, view=view)
                except discord.NotFound:
                    # Message was deleted externally — data is already updated,
                    # just clean up the message_id reference
                    async with self.data_lock:
                        d = load_data_sync()
                        d.get("daily_result_messages", {}).pop(game_slug, None)
                        save_data_sync(d)
                    logger.info(f"Result lifecycle: message for {game_slug} deleted externally, cleaned up data")
                    continue
                except discord.HTTPException as e:
                    logger.warning(f"Result lifecycle: failed to edit result embed for {game_slug}: {e}")
                    continue

                expired_count = len(showcased_ids) - len(remaining)
                logger.info(f"Result lifecycle: removed {expired_count} expired result(s) for {game_slug}, {len(remaining)} remaining")

        except Exception as e:
            logger.error(f"Result lifecycle checker error: {e}")

    @result_lifecycle_checker.before_loop
    async def before_result_lifecycle_checker(self):
        await self.bot.wait_until_ready()

    # --- FORCE PUBLISH ---

    async def _post_match_to_channel(self, m: dict, game_slug: str) -> bool:
        """Post a single match to the configured feed channel. Returns True on success."""
        mid = str(m['id'])
        name = GAMES.get(game_slug, game_slug)

        data = load_data_sync()
        chan_id = data.get("channel_id")
        if not chan_id:
            logger.warning("Force publish: no channel set")
            return False

        channel = self.bot.get_channel(chan_id)
        if not channel:
            logger.warning("Force publish: channel not found")
            return False

        async with self.data_lock:
            d = load_data_sync()
            if mid in d.get("active_matches", {}) or mid in d.get("processed_matches", []):
                logger.info(f"Force publish: match {mid} already posted/processed")
                return False

            # Team-pair dedup for force publish too
            opps = m.get('opponents', [])
            if len(opps) >= 2:
                new_team_ids = {opps[0]['opponent']['id'], opps[1]['opponent']['id']}
                for existing_mid, existing_info in d.get("active_matches", {}).items():
                    if existing_info.get('game_slug') != game_slug:
                        continue
                    existing_teams = existing_info.get('teams', [])
                    if len(existing_teams) >= 2:
                        existing_team_ids = {existing_teams[0].get('id'), existing_teams[1].get('id')}
                        if new_team_ids == existing_team_ids:
                            logger.info(f"Force publish: match {mid} same team pair already active as {existing_mid}")
                            return False

        try:
            opps = m.get('opponents', [])
            if len(opps) < 2:
                return False
            t_a_base, t_b_base = opps[0]['opponent'], opps[1]['opponent']
            t_a = {
                "name": t_a_base['name'], "acronym": t_a_base.get('acronym'),
                "id": t_a_base['id'],
                "roster": await self.fetch_roster(t_a_base['id'], t_a_base['name'], game_slug),
                "flag": t_a_base.get('location'), "image_url": t_a_base.get('image_url')
            }
            t_b = {
                "name": t_b_base['name'], "acronym": t_b_base.get('acronym'),
                "id": t_b_base['id'],
                "roster": await self.fetch_roster(t_b_base['id'], t_b_base['name'], game_slug),
                "flag": t_b_base.get('location'), "image_url": t_b_base.get('image_url')
            }
            # Store match and rebuild the unified upcoming embed
            async with self.data_lock:
                d = load_data_sync()
                d["active_matches"][mid] = {
                    "message_id": None, "channel_id": chan_id, "game_slug": game_slug,
                    "start_time": m['begin_at'], "teams": [t_a, t_b], "votes": {},
                    "fail_count": 0, "stream_url": m.get('official_stream_url'), "status": "active"
                }
                save_data_sync(d)
            await self._rebuild_upcoming_embed(channel, game_slug, {mid: m})
            logger.info(f"Force published match {mid}: {t_a['name']} vs {t_b['name']}")
            return True
        except Exception as e:
            logger.error(f"Force publish match {mid} failed: {e}")
            return False

    async def show_force_publish_menu(self, interaction: discord.Interaction):
        """Fetch upcoming unposted matches and show an admin menu to force-post one."""
        await interaction.response.defer(ephemeral=True)

        now = datetime.datetime.now(datetime.timezone.utc)
        all_matches: Dict[str, tuple] = {}  # {mid: (slug, match_dict)}

        for slug in GAMES:
            params = {
                "filter[status]": "not_started",
                "sort": "begin_at",
                "range[begin_at]": f"{now.isoformat()},{(now + datetime.timedelta(hours=48)).isoformat()}"
            }
            matches = await self.get_pandascore_data(f"/{slug}/matches", params=params)
            if not matches:
                continue
            async with self.data_lock:
                d = load_data_sync()
                active = d.get("active_matches", {})
                processed = d.get("processed_matches", [])
            # Build set of active team pairs for this game for dedup
            active_team_pairs = set()
            for info in active.values():
                if info.get('game_slug') != slug:
                    continue
                teams = info.get('teams', [])
                if len(teams) >= 2:
                    active_team_pairs.add(frozenset({teams[0].get('id'), teams[1].get('id')}))

            for m in matches:
                mid = str(m['id'])
                if len(m.get('opponents', [])) < 2:
                    continue
                if mid in active or mid in processed:
                    continue
                # Team-pair dedup: skip if same teams already active
                new_pair = frozenset({m['opponents'][0]['opponent']['id'], m['opponents'][1]['opponent']['id']})
                if new_pair in active_team_pairs:
                    continue
                all_matches[mid] = (slug, m)

        if not all_matches:
            await interaction.followup.send("No upcoming unposted matches found in the next 48 hours.", ephemeral=True)
            return

        # Sort by start time
        def _sort_key(item):
            _, m = item[1]
            dt = safe_parse_datetime(m.get('begin_at', ''))
            return dt or datetime.datetime(9999, 1, 1, tzinfo=datetime.timezone.utc)

        sorted_matches = dict(sorted(all_matches.items(), key=_sort_key))
        view = ForcePublishModeView(self, sorted_matches)
        await interaction.followup.send(
            f"Found **{len(sorted_matches)}** upcoming unposted match(es) in the next 48h.\n"
            "Post a **single match** or all matches for an **entire event**?",
            view=view,
            ephemeral=True
        )

    # --- OVERTURN RESULT ---
    async def show_overturn_menu(self, interaction: discord.Interaction):
        """Show recent match history for overturning a result."""
        data = load_data_sync()
        history = data.get("match_history", {})

        if not history:
            return await interaction.response.send_message(
                "❌ No match history available. History is stored for 7 days after a result is processed.",
                ephemeral=True
            )

        # Build options from match history (most recent first)
        options = []
        sorted_history = sorted(
            history.items(),
            key=lambda x: x[1].get("processed_at", ""),
            reverse=True
        )[:25]

        for mid, mh in sorted_history:
            teams = mh.get("teams", [])
            if len(teams) < 2:
                continue
            winner_idx = mh.get("winner_idx", -1)
            winner_name = teams[winner_idx]["name"] if 0 <= winner_idx < len(teams) else "?"
            game_tag = GAME_SHORT_NAMES.get(mh.get("game_slug", ""), "")
            processed = safe_parse_datetime(mh.get("processed_at", ""))
            time_str = processed.strftime("%b %d %H:%M") if processed else "?"
            label = f"[{game_tag}] {teams[0]['name']} vs {teams[1]['name']}"[:100]
            desc = f"Winner: {winner_name} · {time_str}"[:100]
            options.append(discord.SelectOption(label=label, value=mid, description=desc))

        if not options:
            return await interaction.response.send_message("❌ No valid matches in history.", ephemeral=True)

        view = OverturnSelectView(self, options)
        await interaction.response.send_message(
            "Select a match to overturn its result.\n"
            "This will reverse the leaderboard entries and send a corrected embed.",
            view=view,
            ephemeral=True
        )

    async def execute_overturn(self, interaction: discord.Interaction, match_id: str, new_winner_idx: int):
        """Overturn a match result: reverse leaderboard entries and send corrected embed."""
        async with self.data_lock:
            data = load_data_sync()
            history = data.get("match_history", {})
            mh = history.get(match_id)

            if not mh:
                return await interaction.followup.send("❌ Match not found in history.", ephemeral=True)

            old_winner_idx = mh["winner_idx"]
            if new_winner_idx == old_winner_idx:
                return await interaction.followup.send("❌ That team is already marked as the winner.", ephemeral=True)

            game_slug = mh["game_slug"]
            votes = mh["votes"]
            teams = mh["teams"]

            if game_slug not in data["leaderboards"]:
                data["leaderboards"][game_slug] = {}
            lb = data["leaderboards"][game_slug]

            # Reverse old results and apply new ones
            for uid, vote in votes.items():
                uid_str = str(uid)
                if uid_str not in lb:
                    lb[uid_str] = {"wins": 0, "losses": 0, "streak": 0}
                s = lb[uid_str]

                # Undo old result
                if vote == old_winner_idx:
                    s["wins"] = max(0, s["wins"] - 1)
                else:
                    s["losses"] = max(0, s["losses"] - 1)

                # Apply correct result
                if vote == new_winner_idx:
                    s["wins"] += 1
                    s["streak"] = s.get("streak", 0) + 1
                else:
                    s["losses"] += 1
                    s["streak"] = 0

            # Update match history with corrected winner
            mh["winner_idx"] = new_winner_idx
            mh["overturned_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            mh["original_winner_idx"] = old_winner_idx
            save_data_sync(data)

        # Send corrected embed
        chan_id = data.get("channel_id")
        channel = self.bot.get_channel(chan_id) if chan_id else None
        if not channel:
            return await interaction.followup.send("✅ Leaderboard corrected but no channel set to send embed.", ephemeral=True)

        # Re-fetch match details from PandaScore for the embed
        details = await self.get_pandascore_data(f"/matches/{match_id}")
        if not details:
            # Build minimal details from history
            details = {
                "id": match_id,
                "status": "finished",
                "results": mh.get("results", []),
                "winner_id": mh.get("winner_id"),
                "league": {"name": ""},
                "serie": {"full_name": ""},
                "tournament": {"name": ""},
                "number_of_games": 1
            }

        # Build full team dicts for the embed
        saved_teams = []
        for t in teams:
            saved_teams.append({
                "name": t["name"],
                "id": t.get("id"),
                "acronym": t.get("acronym", t["name"][:4].upper()),
                "roster": [],
                "flag": None,
                "image_url": None
            })

        # If results scores don't match the corrected winner, swap them
        results = details.get("results", [])
        if results:
            score_a = next((r.get('score', 0) for r in results if r.get('team_id') == saved_teams[0].get('id')), 0)
            score_b = next((r.get('score', 0) for r in results if r.get('team_id') == saved_teams[1].get('id')), 0)
            score_winner = 0 if (score_a or 0) > (score_b or 0) else 1
            if (score_a or 0) != (score_b or 0) and score_winner != new_winner_idx:
                # Swap the scores between the two teams
                for r in results:
                    if r.get('team_id') == saved_teams[0].get('id'):
                        r['score'] = score_b
                    elif r.get('team_id') == saved_teams[1].get('id'):
                        r['score'] = score_a

        embed = await self.build_minimal_result_embed(
            channel, game_slug, details, saved_teams[0], saved_teams[1],
            new_winner_idx, votes
        )
        embed.title = f"⚠️ CORRECTED: {GAMES.get(game_slug, 'ESPORTS').upper()} RESULTS"
        embed.color = discord.Color.orange()
        await channel.send(embed=embed)

        winner_name = teams[new_winner_idx]["name"]
        loser_name = teams[1 - new_winner_idx]["name"]
        await interaction.followup.send(
            f"✅ **Result overturned!**\n"
            f"Corrected winner: **{winner_name}** (was {loser_name})\n"
            f"Leaderboard updated for {len(votes)} voter(s).\n"
            f"Corrected embed sent to {channel.mention}.",
            ephemeral=True
        )

    # --- ADMIN / TEST ---
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
            # For result tests, try multiple matches until we find one with map data
            map_data = None
            used_match = None
            tried_teams = []

            for candidate in valid_matches[:10]:  # Try up to 10 matches
                ta_base = candidate['opponents'][0]['opponent']
                tb_base = candidate['opponents'][1]['opponent']
                match_time = safe_parse_datetime(candidate.get('begin_at'))

                # Try fetching map data for this match
                candidate_map_data = await self.get_map_data(ta_base['name'], tb_base['name'], slug, match_time)

                if candidate_map_data:
                    map_data = candidate_map_data
                    used_match = candidate
                    logger.info(f"Test result: Found map data for {ta_base['name']} vs {tb_base['name']} ({len(map_data)} maps)")
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

            # Build unified result embed for visual testing
            test_results_list = [{
                "match_details": details,
                "team_a": ta, "team_b": tb,
                "winner_idx": 0,
                "votes": {str(interaction.user.id): 0}
            }]
            e = await self.build_unified_result_embed(chan, slug, test_results_list, is_test=True)
            await chan.send(embed=e)

            if map_data:
                await interaction.followup.send(f"✅ Test sent for **{GAMES[slug]}** with **{len(map_data)} maps** - {ta['name']} vs {tb['name']}")
            else:
                logger.warning(f"Test result: No map data found after trying: {tried_teams}")
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
            upcoming_data = [(mid, details, ta, tb)]
            e = self.build_unified_upcoming_embed(slug, upcoming_data)
            msg = await chan.send(embed=e, view=UnifiedUpcomingView(slug))

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


