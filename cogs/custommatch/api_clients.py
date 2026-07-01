import asyncio
import json
import logging
import os
import time as _time
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, List, Dict
from dataclasses import dataclass, field
from urllib.parse import quote

from .models import normalize_rivals_role

logger = logging.getLogger('custommatch')

try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False


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
        self._last_transient_error_at: float = 0  # Epoch time of last 5xx — used to avoid burning retry budget
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

    async def _request(self, endpoint: str, _retry: bool = True) -> Optional[dict]:
        """Make a rate-limited request to the API. Retries once on 429/timeout."""
        import aiohttp
        if not self._api_key:
            logger.warning("Skipping HenrikDev API request - no API key configured")
            return None
        async with self._semaphore:
            await self._rate_limit()
            session = await self._get_session()
            headers = {"Authorization": self._api_key}
            url = f"{self.BASE_URL}{endpoint}"
            try:
                timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 401:
                        logger.warning(f"HenrikDev API unauthorized for {endpoint} - check HENRIK_API_KEY")
                        return None
                    elif resp.status == 429:
                        logger.warning(f"HenrikDev API rate limited on {endpoint}")
                        if _retry:
                            logger.info(f"HenrikDev API: Retrying {endpoint} after 10s rate limit delay")
                            await asyncio.sleep(10)
                            return await self._request(endpoint, _retry=False)
                        return None
                    elif resp.status >= 500:
                        # Server-side error — transient, retry once with backoff
                        logger.warning(f"HenrikDev API server error on {endpoint}: HTTP {resp.status}")
                        if _retry:
                            logger.info(f"HenrikDev API: Retrying {endpoint} after 30s server error delay")
                            await asyncio.sleep(30)
                            return await self._request(endpoint, _retry=False)
                        # Final failure — record timestamp so callers can detect API outage
                        import time as _time
                        self._last_transient_error_at = _time.monotonic()
                        return None
                    else:
                        body = await resp.text()
                        logger.warning(f"HenrikDev API error on {endpoint}: HTTP {resp.status} - {body[:200]}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"HenrikDev API request timed out (30s): {endpoint}")
                if self._session and not self._session.closed:
                    await self._session.close()
                self._session = None
                if _retry:
                    logger.info(f"HenrikDev API: Retrying {endpoint} after timeout")
                    await asyncio.sleep(5)
                    return await self._request(endpoint, _retry=False)
                return None
            except Exception as e:
                logger.error(f"HenrikDev API request failed for {endpoint}: {e}")
                if self._session and not self._session.closed:
                    await self._session.close()
                self._session = None
                return None

    async def get_custom_match_history(self, name: str, tag: str, region: str = 'na') -> Optional[List[dict]]:
        """Fetch recent custom matches for a player."""
        from urllib.parse import quote
        endpoint = f"/valorant/v1/stored-matches/{quote(region)}/{quote(name)}/{quote(tag)}?mode=custom"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            results = data.get('data', [])
            logger.info(f"HenrikAPI: Got {len(results)} custom matches for '{name}#{tag}'")
            return results
        if data:
            logger.warning(f"HenrikAPI: Non-200 status for custom matches '{name}#{tag}': status={data.get('status')}, errors={data.get('errors')}")
        else:
            logger.warning(f"HenrikAPI: No response for custom matches '{name}#{tag}'")
        return None

    async def get_stored_matches(self, name: str, tag: str, region: str = 'na') -> Optional[List[dict]]:
        """Fetch recent stored matches for a player (all modes, no filter)."""
        from urllib.parse import quote
        endpoint = f"/valorant/v1/stored-matches/{quote(region)}/{quote(name)}/{quote(tag)}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            results = data.get('data', [])
            logger.info(f"HenrikAPI: Got {len(results)} stored matches (all modes) for '{name}#{tag}'")
            return results
        if data:
            logger.warning(f"HenrikAPI: Non-200 status for stored matches '{name}#{tag}': status={data.get('status')}, errors={data.get('errors')}")
        else:
            logger.warning(f"HenrikAPI: No response for stored matches '{name}#{tag}'")
        return None

    async def get_match_details(self, match_id: str) -> Optional[dict]:
        """Get full match details by match ID."""
        endpoint = f"/valorant/v2/match/{match_id}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            return data.get('data')
        return None

    async def get_account(self, name: str, tag: str) -> Optional[dict]:
        """Resolve Name#Tag to account info including PUUID."""
        from urllib.parse import quote
        endpoint = f"/valorant/v1/account/{quote(name)}/{quote(tag)}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            account = data.get('data', {})
            logger.info(f"HenrikAPI: Resolved account '{name}#{tag}' -> puuid={account.get('puuid', 'N/A')[:8]}...")
            return account
        if data:
            logger.warning(f"HenrikAPI: Failed to resolve account '{name}#{tag}': status={data.get('status')}")
        return None

    async def get_account_by_puuid(self, puuid: str) -> Optional[dict]:
        """Resolve PUUID to current account info (Name, Tag)."""
        endpoint = f"/valorant/v1/by-puuid/account/{puuid}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            account = data.get('data', {})
            return account
        return None

    async def ping_tracker_for_refresh(self, name: str, tag: str) -> bool:
        """Send a dummy request to Tracker.gg to trigger Riot API match indexing."""
        import aiohttp
        import urllib.parse
        encoded_name = urllib.parse.quote(name)
        encoded_tag = urllib.parse.quote(tag)
        url = (
            f"https://api.tracker.gg/api/v2/valorant/standard/profile/riot/"
            f"{encoded_name}%23{encoded_tag}"
        )
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://tracker.gg",
            "Referer": "https://tracker.gg/",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    logger.info(f"Tracker ping for '{name}#{tag}': HTTP {resp.status}")
                    return resp.status == 200
        except Exception as e:
            logger.info(f"Tracker ping for '{name}#{tag}' failed: {e}")
            return False

    async def get_match_history_by_puuid(self, puuid: str, region: str = 'na', mode: str = None) -> Optional[List[dict]]:
        """Fetch match history by PUUID using v3 endpoint (more reliable)."""
        from urllib.parse import quote
        endpoint = f"/valorant/v3/by-puuid/matches/{quote(region)}/{quote(puuid)}"
        if mode:
            endpoint += f"?mode={quote(mode)}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            results = data.get('data', [])
            logger.info(f"HenrikAPI: Got {len(results)} matches (v3/puuid, mode={mode or 'all'}) for puuid={puuid[:8]}...")
            return results
        if data:
            logger.warning(f"HenrikAPI: Non-200 for v3/puuid matches puuid={puuid[:8]}...: status={data.get('status')}")
        return None

    async def get_stored_matches_by_puuid(self, puuid: str, region: str = 'na', mode: str = None) -> Optional[List[dict]]:
        """Fetch stored matches by PUUID (fallback for v3)."""
        from urllib.parse import quote
        endpoint = f"/valorant/v1/by-puuid/stored-matches/{quote(region)}/{quote(puuid)}"
        if mode:
            endpoint += f"?mode={quote(mode)}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            results = data.get('data', [])
            logger.info(f"HenrikAPI: Got {len(results)} stored matches (puuid, mode={mode or 'all'}) for puuid={puuid[:8]}...")
            return results
        if data:
            logger.warning(f"HenrikAPI: Non-200 for stored-matches/puuid puuid={puuid[:8]}...: status={data.get('status')}")
        return None

    async def get_match_history_v4_by_puuid(self, puuid: str, region: str = 'na', platform: str = 'console', mode: str = None) -> Optional[List[dict]]:
        """Fetch match history by PUUID using v4 endpoint with platform support (primary for console players)."""
        from urllib.parse import quote
        endpoint = f"/valorant/v4/by-puuid/matches/{quote(region)}/{quote(platform)}/{quote(puuid)}"
        if mode:
            endpoint += f"?mode={quote(mode)}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            results = data.get('data', [])
            logger.info(f"HenrikAPI: Got {len(results)} matches (v4/{platform}/puuid, mode={mode or 'all'}) for puuid={puuid[:8]}...")
            return results
        if data:
            logger.warning(f"HenrikAPI: Non-200 for v4/{platform}/puuid matches puuid={puuid[:8]}...: status={data.get('status')}")
        return None

    async def get_match_history_v4_by_name(self, name: str, tag: str, region: str = 'na', platform: str = 'console', mode: str = None) -> Optional[List[dict]]:
        """Fetch match history by name/tag using v4 endpoint with platform support (console fallback without PUUID)."""
        from urllib.parse import quote
        endpoint = f"/valorant/v4/matches/{quote(region)}/{quote(platform)}/{quote(name)}/{quote(tag)}"
        if mode:
            endpoint += f"?mode={quote(mode)}"
        data = await self._request(endpoint)
        if data and data.get('status') == 200:
            results = data.get('data', [])
            logger.info(f"HenrikAPI: Got {len(results)} matches (v4/{platform}/name, mode={mode or 'all'}) for '{name}#{tag}'")
            return results
        if data:
            logger.warning(f"HenrikAPI: Non-200 for v4/{platform}/name matches '{name}#{tag}': status={data.get('status')}")
        return None

    async def find_and_fetch_match_stats(
        self,
        player_ign: str,
        our_match_id: int,
        game_id: int,
        match_end_time: datetime,
        match_created_at: Optional[datetime] = None,
        our_player_igns: Optional[set] = None,
        our_player_puuids: Optional[set] = None,
        regulars: Optional[List[dict]] = None,
        exclude_val_ids: Optional[set] = None,
        player_puuid: Optional[str] = None,
        expected_map: Optional[str] = None
    ) -> Optional[dict]:
        """
        Multi-signal match identification.

        Scoring system (higher = more confident):
          - Time proximity:  up to 40 pts (best within 10min of expected window)
          - Player overlap:  up to 30 pts (% of our players found in match)
          - Map match:       15 pts if map matches the map vote
          - Round validity:  10 pts if winner has 13+ rounds (standard match)
          - Mode bonus:       5 pts if tagged as 'custom' mode

        The highest-scored candidate that meets minimum overlap (60%) wins.
        """
        if '#' not in player_ign:
            return None

        name, tag = player_ign.rsplit('#', 1)

        # --- Step 1: Find match ID candidates from match history ---
        matches = await self._search_match_history(name, tag, player_puuid)
        if not matches:
            logger.info(f"Match lookup: No match history found for '{name}#{tag}' via any endpoint")
            return None

        # 90-min window — matches are ~35-45min and a winner is called within ~5min.
        # A match that started >90min before decided_at is not our match.
        TIME_WINDOW = 5400  # 90 minutes

        # The Valorant game starts BEFORE our match_end_time (decided_at).
        # Typical timeline: game starts → plays 35-45min → winner called 0-5min after.
        # So game_start is roughly 35-50min BEFORE match_end_time.
        # And game_start is shortly AFTER match_created_at.
        # We use the best (smallest) diff against both timestamps.

        candidates = []
        seen_val_ids = set()
        for idx, match in enumerate(matches[:20]):
            meta = match.get('meta') or match.get('metadata') or {}

            try:
                game_start = self._parse_match_start_time(meta)
                if not game_start:
                    continue

                map_raw = meta.get('map', 'unknown')
                map_name = map_raw.get('name', 'unknown') if isinstance(map_raw, dict) else map_raw

                # Time proximity: use best diff against created_at and decided_at
                time_diffs = []
                if match_created_at:
                    # game_start should be close to match_created_at (within ~5-15min)
                    time_diffs.append(abs((game_start - match_created_at).total_seconds()))
                if match_end_time:
                    # game_start should be ~35-50min before match_end_time
                    time_diffs.append(abs((game_start - match_end_time).total_seconds()))

                best_diff = min(time_diffs) if time_diffs else None
                if best_diff is None or best_diff > TIME_WINDOW:
                    continue

                valorant_match_id = meta.get('id') or meta.get('matchid') or meta.get('match_id')
                if not valorant_match_id or valorant_match_id in seen_val_ids:
                    continue
                seen_val_ids.add(valorant_match_id)

                if exclude_val_ids and valorant_match_id in exclude_val_ids:
                    logger.info(f"Match lookup [{idx}]: {valorant_match_id} already linked, skipping")
                    continue

                # Check if API tags this as custom mode
                mode = meta.get('mode', '').lower() if meta.get('mode') else ''
                is_custom = mode in ('custom', 'custom game')

                logger.info(
                    f"Match lookup [{idx}]: map={map_name}, start={game_start.isoformat()}, "
                    f"diff={best_diff:.0f}s, mode={mode or '?'} — CANDIDATE"
                )
                candidates.append({
                    'valorant_match_id': valorant_match_id,
                    'time_diff': best_diff,
                    'map_name': map_name,
                    'is_custom': is_custom,
                })

            except (ValueError, TypeError) as e:
                logger.warning(f"Match lookup [{idx}]: Error parsing match: {e}")
                continue

        if not candidates:
            logger.info(f"Match lookup: No candidates for '{name}#{tag}' within {TIME_WINDOW}s window")
            return None

        # --- Step 2: Score each candidate with full details ---
        # Fetch details for top candidates (sorted by time proximity, limit API calls)
        candidates.sort(key=lambda c: c['time_diff'])
        scored = []

        for cand in candidates[:5]:  # Only fetch details for top 5 time-closest
            valorant_match_id = cand['valorant_match_id']
            details = await self.get_match_details(valorant_match_id)
            if not details:
                logger.info(f"Match lookup: Could not fetch details for {valorant_match_id}")
                continue

            # --- Player overlap (up to 30 pts) ---
            val_puuids = set()
            val_igns = set()
            players_data = details.get('players', {})
            self._extract_val_players(players_data, val_puuids, val_igns)

            puuid_matches = len(our_player_puuids & val_puuids) if our_player_puuids and val_puuids else 0
            our_lower = {ign.lower() for ign in our_player_igns} if our_player_igns else set()
            ign_matches = len(our_lower & val_igns) if our_lower and val_igns else 0
            best_matches = max(puuid_matches, ign_matches)
            total_our_players = max(len(our_player_puuids or set()), len(our_lower))
            overlap_ratio = best_matches / total_our_players if total_our_players > 0 else 0.0

            # Hard gate: require 60% overlap minimum
            if overlap_ratio < 0.6 and total_our_players > 0:
                logger.info(
                    f"Match lookup: Rejecting {valorant_match_id} — overlap "
                    f"{best_matches}/{total_our_players} = {overlap_ratio:.0%} < 60%"
                )
                continue

            overlap_score = overlap_ratio * 30  # 0-30 pts

            # --- Time proximity (up to 40 pts) ---
            # Best score if game started 0-10min from our created_at/ended_at.
            # Linearly decays to 0 at TIME_WINDOW.
            time_score = max(0, 40 * (1 - cand['time_diff'] / TIME_WINDOW))

            # --- Map match (15 pts) ---
            detail_map = details.get('metadata', {}).get('map')
            final_map = detail_map.get('name') if isinstance(detail_map, dict) else detail_map
            map_score = 0
            if expected_map and final_map:
                if expected_map.lower() == final_map.lower():
                    map_score = 15

            # --- Round validity (10 pts) ---
            # A real completed custom match has a winner with 13+ rounds
            round_score = 0
            teams_data = details.get('teams', {})
            max_rounds = 0
            if isinstance(teams_data, dict):
                for team_info in teams_data.values():
                    if isinstance(team_info, dict):
                        rw = team_info.get('rounds_won', 0) or team_info.get('rounds', {}).get('won', 0) or 0
                        max_rounds = max(max_rounds, rw)
            elif isinstance(teams_data, list):
                for t in teams_data:
                    if isinstance(t, dict):
                        rw = t.get('rounds_won', 0) or 0
                        max_rounds = max(max_rounds, rw)
            if max_rounds >= 13:
                round_score = 10

            # --- Mode bonus (5 pts) ---
            mode_score = 5 if cand['is_custom'] else 0

            total_score = overlap_score + time_score + map_score + round_score + mode_score

            logger.info(
                f"Match lookup: {valorant_match_id} SCORE={total_score:.1f} "
                f"(overlap={overlap_score:.1f} time={time_score:.1f} map={map_score} "
                f"rounds={round_score} mode={mode_score}) "
                f"overlap={best_matches}/{total_our_players}={overlap_ratio:.0%} "
                f"diff={cand['time_diff']:.0f}s map={final_map}"
            )

            scored.append({
                'valorant_match_id': valorant_match_id,
                'details': details,
                'map': final_map,
                'puuid': player_puuid,
                'score': total_score,
                'overlap_ratio': overlap_ratio,
                'time_diff': cand['time_diff'],
            })

        if not scored:
            logger.info(f"Match lookup: All candidates rejected for '{name}#{tag}'")
            return None

        # Pick the highest-scored candidate
        best = max(scored, key=lambda s: s['score'])
        logger.info(
            f"Match lookup: Selected {best['valorant_match_id']} "
            f"(score={best['score']:.1f}, overlap={best['overlap_ratio']:.0%}, "
            f"diff={best['time_diff']:.0f}s, map={best['map']})"
        )
        return {
            'valorant_match_id': best['valorant_match_id'],
            'details': best['details'],
            'map': best['map'],
            'puuid': player_puuid,
        }

    def _parse_match_start_time(self, meta: dict) -> Optional[datetime]:
        """Parse game start time from match metadata (supports v1/v3/v4 formats)."""
        started_at = meta.get('started_at')
        if started_at:
            from dateutil.parser import isoparse
            game_start = isoparse(started_at)
            if game_start.tzinfo is None:
                game_start = game_start.replace(tzinfo=timezone.utc)
            return game_start

        game_start_ts = meta.get('game_start')
        if game_start_ts:
            return datetime.fromtimestamp(game_start_ts, tz=timezone.utc)

        game_start_patched = meta.get('game_start_patched')
        if game_start_patched:
            try:
                from dateutil.parser import isoparse
                return isoparse(game_start_patched)
            except (ValueError, TypeError):
                pass

        return None

    @staticmethod
    def _extract_val_players(players_data, val_puuids: set, val_igns: set):
        """Extract PUUIDs and IGNs from Valorant match player data (supports v2/v4)."""
        def _extract(player_list):
            for vp in player_list:
                if not isinstance(vp, dict):
                    continue
                vp_puuid = vp.get('puuid')
                if vp_puuid:
                    val_puuids.add(vp_puuid)
                vp_name, vp_tag = vp.get('name', ''), vp.get('tag', '')
                if vp_name and vp_tag:
                    val_igns.add(f"{vp_name}#{vp_tag}".lower())

        if isinstance(players_data, dict):
            for team_key in ['all_players', 'red', 'blue', 'Red', 'Blue']:
                _extract(players_data.get(team_key, []))
        elif isinstance(players_data, list):
            for td in players_data:
                _extract(td if isinstance(td, list) else [td])

    async def _search_match_history(self, name: str, tag: str, player_puuid: Optional[str]) -> Optional[list]:
        """Search match history across all available endpoints. Returns merged list or None."""
        # Primary path: v4 (fastest, console-aware)
        matches = None
        if player_puuid:
            matches = await self.get_match_history_v4_by_puuid(player_puuid, 'na', 'console', mode='custom')
            if not matches:
                matches = await self.get_match_history_v4_by_puuid(player_puuid, 'na', 'console', mode=None)

        if not matches:
            matches = await self.get_match_history_v4_by_name(name, tag, 'na', 'console', mode='custom')
        if not matches:
            matches = await self.get_match_history_v4_by_name(name, tag, 'na', 'console', mode=None)

        # v3 / v1 fallbacks
        if not matches and player_puuid:
            matches = await self.get_match_history_by_puuid(player_puuid, 'na', mode='custom')
        if not matches and player_puuid:
            matches = await self.get_match_history_by_puuid(player_puuid, 'na', mode=None)
        if not matches and player_puuid:
            matches = await self.get_stored_matches_by_puuid(player_puuid, 'na', mode='custom')
        if not matches and player_puuid:
            matches = await self.get_stored_matches_by_puuid(player_puuid, 'na', mode=None)
        if not matches:
            matches = await self.get_custom_match_history(name, tag)
        if not matches:
            matches = await self.get_stored_matches(name, tag)

        return matches

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()


# =============================================================================
# MARVEL RIVALS API CLIENT (marvelrivalsapi.com — player lookup)
# =============================================================================

class MarvelRivalsAPI:
    """Thin wrapper around marvelrivalsapi.com for verifying Rivals IGNs.

    Used at IGN-set time to (a) confirm the player actually exists and
    (b) fetch the canonical cased display name so later scoreboard-OCR
    matching doesn't break on casing/typos.
    """

    BASE_URL = "https://marvelrivalsapi.com/api/v1"

    def __init__(self):
        self._session: Optional['aiohttp.ClientSession'] = None
        self._api_key = os.getenv("MARVEL_RIVALS_API_KEY")
        if not self._api_key:
            logger.warning("MARVEL_RIVALS_API_KEY not set - Rivals IGN verification disabled")

    @property
    def available(self) -> bool:
        return bool(self._api_key)

    async def _get_session(self):
        if self._session is None or self._session.closed:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session

    async def find_player(self, username: str) -> Optional[dict]:
        """Look up a player by username.

        Returns {'name': <canonical>, 'uid': <str>} on success,
        None if not found or the API is unavailable / errored.
        """
        import aiohttp
        from urllib.parse import quote
        if not self._api_key:
            return None
        username = (username or "").strip()
        if not username:
            return None
        session = await self._get_session()
        url = f"{self.BASE_URL}/find-player/{quote(username, safe='')}"
        headers = {"x-api-key": self._api_key}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, dict) and data.get("name"):
                        return {"name": data["name"], "uid": str(data.get("uid") or "")}
                    logger.warning(f"MarvelRivalsAPI: unexpected 200 payload for '{username}': {data}")
                    return None
                if resp.status in (400, 404):
                    # 400 "Player not found" is the documented miss response
                    return None
                if resp.status == 401:
                    logger.warning("MarvelRivalsAPI: 401 unauthorized — check MARVEL_RIVALS_API_KEY")
                    return None
                if resp.status == 429:
                    logger.warning(f"MarvelRivalsAPI: rate limited on find-player/{username}")
                    return None
                logger.warning(f"MarvelRivalsAPI: HTTP {resp.status} for find-player/{username}")
                return None
        except asyncio.TimeoutError:
            logger.warning(f"MarvelRivalsAPI: timeout for find-player/{username}")
            return None
        except Exception as e:
            logger.warning(f"MarvelRivalsAPI: error for find-player/{username}: {e}")
            return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# =============================================================================
# MARVEL RIVALS VISION CLIENT (Gemini 2.5 Flash)
# =============================================================================

@dataclass
class RivalsPlayerRow:
    ign: str
    team: str                       # 'red' or 'blue'
    role: Optional[str]             # Vanguard / Duelist / Strategist
    kills: int
    deaths: int
    assists: int
    final_hits: int
    damage: int
    damage_blocked: int
    healing: int
    accuracy_pct: Optional[float]
    mvp_svp: Optional[str]          # None / 'MVP' / 'SVP'
    medals: List[str] = field(default_factory=list)


@dataclass
class RivalsScoreboardResult:
    players: List[RivalsPlayerRow]
    winning_team: Optional[str]     # 'red' / 'blue' / None
    confidence: float               # 0.0 - 1.0
    warnings: List[str] = field(default_factory=list)
    raw_json: str = ""
    map_name: Optional[str] = None  # Map label extracted from the scoreboard header

    @classmethod
    def from_raw_json(cls, text: str) -> Optional["RivalsScoreboardResult"]:
        """Reconstruct a result from saved gemini_raw_json (no API call)."""
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return None
        players: List[RivalsPlayerRow] = []
        for p in data.get("players", []):
            try:
                mvp_svp = p.get("mvp_svp")
                if mvp_svp == "NONE":
                    mvp_svp = None
                players.append(RivalsPlayerRow(
                    ign=str(p.get("ign", "")).strip(),
                    team=str(p.get("team", "")).lower(),
                    role=normalize_rivals_role(p.get("role")),
                    kills=int(p.get("kills") or 0),
                    deaths=int(p.get("deaths") or 0),
                    assists=int(p.get("assists") or 0),
                    final_hits=int(p.get("final_hits") or 0),
                    damage=int(p.get("damage") or 0),
                    damage_blocked=int(p.get("damage_blocked") or 0),
                    healing=int(p.get("healing") or 0),
                    accuracy_pct=float(p["accuracy_pct"]) if p.get("accuracy_pct") is not None else None,
                    mvp_svp=mvp_svp,
                    medals=[str(m) for m in (p.get("medals") or [])],
                ))
            except (TypeError, ValueError):
                continue
        if not players:
            return None
        raw_map = data.get("map_name")
        map_name = str(raw_map).strip() if raw_map else None
        if map_name and map_name.lower() in ("null", "none", "unknown", ""):
            map_name = None
        return cls(
            players=players,
            winning_team=(data.get("winning_team") or "").lower() or None,
            confidence=float(data.get("confidence") or 0.0),
            warnings=[str(w) for w in (data.get("warnings") or [])],
            raw_json=text,
            map_name=map_name,
        )


class RivalsVisionClient:
    """Wraps Gemini 2.5 Flash for extracting Marvel Rivals scoreboard data from screenshots.

    Uses structured output (response_schema) so the model returns valid JSON that
    we can parse directly into a RivalsScoreboardResult. Intended for the
    post-match upload flow in custommatch.
    """

    MODEL_NAME = "gemini-2.5-flash"

    PROMPT = """You are extracting structured stats from a Marvel Rivals post-match scoreboard screenshot.

The scoreboard has two teams of (usually) 6 players each. The TOP team lost or won as marked; the BOTTOM team is the other side. Use the MVP label on the left side to tag the team: the team with 'MVP' is the winning team; the team with 'SVP' is the losing team.

For EACH player row, read the columns left-to-right:
- The leftmost icon column indicates the player's ROLE (not character): a shield/tank icon = 'Vanguard', a crosshair/gun icon = 'Duelist', a cross/support icon = 'Strategist'. Use exactly 'Vanguard', 'Duelist', or 'Strategist'.
- Player Name (exactly as shown, including any non-ASCII). Read the name character-by-character — do NOT autocorrect, do NOT guess, and do NOT substitute visually similar letters (o/u, l/1/i, g/q, m/rn, 5/S, 0/O, vv/w). If any single character is uncertain, lower your overall confidence and add a warning like 'row N: name char X uncertain' naming the exact position. IGNs are proper nouns; treat them like passwords — one wrong letter is a bug, not a typo.
- K (kills), D (deaths), A (assists)
- Medals column: a row of small medal icons. For each distinct medal icon, emit a short label string (e.g. 'mvp', 'svp', 'ace', 'damage', 'healing', 'blocked', 'kills', 'assists'). If you can't identify a medal precisely, use 'unknown_medal'. Include one entry per medal icon visible (so a player with 3 medals → 3 entries).
- Final Hits (integer)
- Damage (integer, no commas)
- Damage Blocked (integer, no commas)
- Healing (integer, no commas)
- Accuracy (percentage 0-100; may be absent — use null if not visible)

Additionally:
- For each player, determine team: 'red' for the top half of the scoreboard, 'blue' for the bottom half.
- Tag mvp_svp: 'MVP' for the single player with the MVP label, 'SVP' for the single player with SVP, otherwise null.
- winning_team: 'red' or 'blue' based on which team has the MVP label (or which side is marked 'VICTORY').
- map_name: the map label printed in the upper-right corner of the scoreboard header (above the match ID). Use Title Case exactly as shown (e.g. 'Lower Manhattan', 'Yggdrasil Path', 'Hall of Djalia', "K'un Lun"). If the map label is not visible or unreadable, return null.
- confidence: your own 0.0-1.0 estimate of how confidently you read the entire scoreboard. Lower it if any row is blurry, if numbers are illegible, or if role/medal icons are unclear.
- warnings: list of short strings describing any row you were unsure about (e.g. 'row 4: role icon unclear', 'row 7: damage partially obscured').

Only output rows that are actually present on the scoreboard; do not invent players. Output ALL present rows (typically 12 total). Return strictly valid JSON matching the provided schema."""

    RESPONSE_SCHEMA = {
        "type": "object",
        "properties": {
            "players": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ign": {"type": "string"},
                        "team": {"type": "string", "enum": ["red", "blue"]},
                        "role": {"type": "string", "enum": ["Vanguard", "Duelist", "Strategist"]},
                        "kills": {"type": "integer"},
                        "deaths": {"type": "integer"},
                        "assists": {"type": "integer"},
                        "final_hits": {"type": "integer"},
                        "damage": {"type": "integer"},
                        "damage_blocked": {"type": "integer"},
                        "healing": {"type": "integer"},
                        "accuracy_pct": {"type": "number", "nullable": True},
                        "mvp_svp": {"type": "string", "enum": ["MVP", "SVP", "NONE"]},
                        "medals": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": [
                        "ign", "team", "role", "kills", "deaths", "assists",
                        "final_hits", "damage", "damage_blocked", "healing",
                        "mvp_svp", "medals",
                    ],
                },
            },
            "winning_team": {"type": "string", "enum": ["red", "blue"]},
            "map_name": {"type": "string", "nullable": True},
            "confidence": {"type": "number"},
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["players", "winning_team", "confidence", "warnings"],
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY_VISION") or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        self._model = None
        self._semaphore = asyncio.Semaphore(1)  # serialize all Gemini calls
        self._queue_depth = 0  # callers waiting + in-progress
        self._last_request_time: float = 0.0  # monotonic — rate pacing
        if GENAI_AVAILABLE and self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                self._model = genai.GenerativeModel(
                    model_name=self.MODEL_NAME,
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                        response_schema=self.RESPONSE_SCHEMA,
                    ),
                )
                logger.info(f"RivalsVisionClient: initialized with {self.MODEL_NAME}")
            except Exception as e:
                logger.error(f"RivalsVisionClient init failed: {e}")
                self._model = None

    @property
    def available(self) -> bool:
        return self._model is not None

    @property
    def queue_depth(self) -> int:
        """Number of extract_scoreboard calls waiting + in-progress."""
        return self._queue_depth

    @staticmethod
    def _preprocess_image(image_bytes: bytes, min_width: int = 1920) -> tuple:
        """Preprocess screenshot for better OCR: upscale if needed, enhance
        contrast and sharpness, convert to PNG.

        Returns (processed bytes, mime_type).
        """
        import io
        from PIL import Image as PILImage, ImageEnhance
        img = PILImage.open(io.BytesIO(image_bytes))
        orig_w, orig_h = img.width, img.height

        # Upscale small images
        if img.width < min_width:
            scale = min_width / orig_w
            new_size = (int(orig_w * scale), int(orig_h * scale))
            img = img.resize(new_size, PILImage.LANCZOS)
            logger.info(f"Upscaled screenshot from {orig_w}x{orig_h} to {new_size[0]}x{new_size[1]}")

        # Enhance contrast and sharpness for clearer text/icon recognition
        img = ImageEnhance.Contrast(img).enhance(1.3)
        img = ImageEnhance.Sharpness(img).enhance(1.5)

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue(), "image/png"

    async def extract_scoreboard(
        self,
        image_bytes: bytes,
        mime_type: str = "image/png",
        on_status=None,
    ) -> Optional[RivalsScoreboardResult]:
        """Extract scoreboard data from the given image bytes.

        Returns None if the client is not available or the call failed entirely.

        on_status: optional ``async def callback(msg: str)`` invoked with
        progress updates (rate-limit waits, queue position, etc.) so the
        caller can relay them to the Discord user.
        """
        if not self.available:
            logger.warning("RivalsVisionClient.extract_scoreboard called but client unavailable")
            return None

        # Preprocess image for better OCR accuracy (upscale + contrast + sharpen)
        try:
            image_bytes, mime_type = await asyncio.to_thread(self._preprocess_image, image_bytes)
        except Exception as e:
            logger.warning(f"Image preprocessing failed, using original: {e}")

        MAX_ATTEMPTS = 3       # non-429 error budget
        MAX_429_WAITS = 6      # separate budget for rate-limit retries
        RATE_LIMIT_WAIT = 60   # full quota window — free tier resets per minute
        DEFAULT_BACKOFF = 5    # seconds between non-429 retries
        MIN_GAP = 4.0          # minimum seconds between any two API calls

        self._queue_depth += 1
        try:
            async with self._semaphore:
                loop = asyncio.get_event_loop()
                data = None
                text = ""

                attempt = 0       # counts non-429 attempts
                rate_waits = 0    # counts 429 waits (don't burn attempt budget)

                while attempt < MAX_ATTEMPTS:
                    # Rate pacing — enforce minimum gap between requests to
                    # avoid bursting through the free-tier RPM limit.
                    now = _time.monotonic()
                    gap = now - self._last_request_time
                    if gap < MIN_GAP:
                        await asyncio.sleep(MIN_GAP - gap)

                    total = attempt + rate_waits + 1
                    is_last_attempt = (attempt == MAX_ATTEMPTS - 1) and (rate_waits >= MAX_429_WAITS)
                    logger.info(
                        f"Gemini scoreboard attempt {total} "
                        f"(non-429: {attempt + 1}/{MAX_ATTEMPTS}, "
                        f"429-waits: {rate_waits}/{MAX_429_WAITS}, "
                        f"{len(image_bytes)} bytes)"
                    )

                    # On the final non-429 attempt, drop the structured output
                    # schema — sometimes the schema constraint causes Gemini to
                    # fail even though it can parse the image fine.
                    use_fallback = (attempt == MAX_ATTEMPTS - 1)
                    if use_fallback and GENAI_AVAILABLE:
                        try:
                            model_to_use = genai.GenerativeModel(
                                model_name=self.MODEL_NAME,
                                generation_config=genai.types.GenerationConfig(
                                    temperature=0.1,
                                    response_mime_type="application/json",
                                ),
                            )
                        except Exception:
                            model_to_use = self._model
                    else:
                        model_to_use = self._model

                    content_payload = [
                        self.PROMPT + (
                            "\n\nReturn strictly valid JSON matching this structure: "
                            "{players: [{ign, team, role, kills, deaths, assists, "
                            "final_hits, damage, damage_blocked, healing, accuracy_pct, "
                            "mvp_svp, medals}], winning_team, map_name, confidence, warnings}"
                            if use_fallback else ""
                        ),
                        {"mime_type": mime_type, "data": image_bytes},
                    ]

                    # --- Make the API call ---
                    try:
                        _m = model_to_use  # capture for lambda
                        response = await loop.run_in_executor(
                            None,
                            lambda: _m.generate_content(content_payload),
                        )
                        self._last_request_time = _time.monotonic()
                    except Exception as e:
                        self._last_request_time = _time.monotonic()
                        err_str = str(e)
                        is_rate_limit = "429" in err_str or "quota" in err_str.lower()

                        if is_rate_limit:
                            rate_waits += 1
                            if rate_waits >= MAX_429_WAITS:
                                logger.error(
                                    f"Gemini rate-limit retries exhausted "
                                    f"({rate_waits} waits, {attempt} attempts)"
                                )
                                return None
                            logger.warning(
                                f"Gemini 429 — waiting {RATE_LIMIT_WAIT}s "
                                f"(wait {rate_waits}/{MAX_429_WAITS})"
                            )
                            if on_status:
                                try:
                                    await on_status(
                                        f"Rate limited by Gemini — retrying in "
                                        f"{RATE_LIMIT_WAIT}s "
                                        f"(attempt {rate_waits}/{MAX_429_WAITS})..."
                                    )
                                except Exception:
                                    pass
                            await asyncio.sleep(RATE_LIMIT_WAIT)
                            continue  # don't burn an attempt
                        else:
                            logger.error(
                                f"Gemini generate_content error "
                                f"(attempt {attempt + 1}/{MAX_ATTEMPTS}): {e}"
                            )
                            attempt += 1
                            if attempt < MAX_ATTEMPTS:
                                await asyncio.sleep(DEFAULT_BACKOFF * attempt)
                            continue

                    # --- Validate response ---
                    try:
                        if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                            block_reason = getattr(response.prompt_feedback, 'block_reason', None)
                            if block_reason:
                                logger.error(f"Gemini safety block (attempt {attempt + 1}): {block_reason}")
                                attempt += 1
                                if attempt < MAX_ATTEMPTS:
                                    await asyncio.sleep(DEFAULT_BACKOFF * attempt)
                                continue
                        if not getattr(response, 'candidates', None):
                            logger.error(f"Gemini no candidates (attempt {attempt + 1})")
                            attempt += 1
                            if attempt < MAX_ATTEMPTS:
                                await asyncio.sleep(DEFAULT_BACKOFF * attempt)
                            continue
                    except Exception:
                        pass

                    try:
                        text = response.text
                    except Exception as e:
                        logger.error(f"Gemini response.text failed (attempt {attempt + 1}): {e}")
                        attempt += 1
                        if attempt < MAX_ATTEMPTS:
                            await asyncio.sleep(DEFAULT_BACKOFF * attempt)
                        continue

                    try:
                        data = json.loads(text)
                        break  # success
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Gemini JSON parse failed (attempt {attempt + 1}): "
                            f"{e}; raw={text[:500]}"
                        )
                        attempt += 1
                        if attempt < MAX_ATTEMPTS:
                            await asyncio.sleep(DEFAULT_BACKOFF * attempt)
                        continue

                if data is None:
                    return None
        finally:
            self._queue_depth -= 1

        # Parse into dataclass
        players: List[RivalsPlayerRow] = []
        for p in data.get("players", []):
            try:
                mvp_svp = p.get("mvp_svp")
                if mvp_svp == "NONE":
                    mvp_svp = None
                players.append(RivalsPlayerRow(
                    ign=str(p.get("ign", "")).strip(),
                    team=str(p.get("team", "")).lower(),
                    role=normalize_rivals_role(p.get("role")),
                    kills=int(p.get("kills") or 0),
                    deaths=int(p.get("deaths") or 0),
                    assists=int(p.get("assists") or 0),
                    final_hits=int(p.get("final_hits") or 0),
                    damage=int(p.get("damage") or 0),
                    damage_blocked=int(p.get("damage_blocked") or 0),
                    healing=int(p.get("healing") or 0),
                    accuracy_pct=float(p["accuracy_pct"]) if p.get("accuracy_pct") is not None else None,
                    mvp_svp=mvp_svp,
                    medals=[str(m) for m in (p.get("medals") or [])],
                ))
            except (TypeError, ValueError) as e:
                logger.warning(f"Skipping malformed Rivals player row {p}: {e}")

        raw_map = data.get("map_name")
        map_name = str(raw_map).strip() if raw_map else None
        if map_name and map_name.lower() in ("null", "none", "unknown", ""):
            map_name = None
        return RivalsScoreboardResult(
            players=players,
            winning_team=(data.get("winning_team") or "").lower() or None,
            confidence=float(data.get("confidence") or 0.0),
            warnings=[str(w) for w in (data.get("warnings") or [])],
            raw_json=text,
            map_name=map_name,
        )

    @staticmethod
    def medals_to_counts(medals: List[str]) -> Dict[str, int]:
        """Convert a flat medal list like ['mvp','kills','kills'] to a count dict."""
        out: Dict[str, int] = {}
        for m in medals:
            key = str(m).strip().lower()
            if not key:
                continue
            out[key] = out.get(key, 0) + 1
        return out
