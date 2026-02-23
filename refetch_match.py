#!/usr/bin/env python3
"""One-off script to refetch Valorant stats for match CGEQJ (match_id=25)."""

import asyncio
import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from dateutil.parser import isoparse

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import aiohttp

DB_PATH = Path("data/custommatch.db")
MATCH_ID = 25
GAME_ID = 1
API_KEY = os.getenv("HENRIK_API_KEY")
BASE_URL = "https://api.henrikdev.xyz"

if not API_KEY:
    print("ERROR: HENRIK_API_KEY not set")
    sys.exit(1)

# Rate limiting
_last_request_time = 0
REQUEST_INTERVAL = 2.5  # seconds between requests (stay well under 30/min)


async def api_request(session, endpoint, retries=2):
    """Make a rate-limited API request to HenrikDev."""
    global _last_request_time

    for attempt in range(retries + 1):
        # Rate limit
        now = asyncio.get_event_loop().time()
        wait = REQUEST_INTERVAL - (now - _last_request_time)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_time = asyncio.get_event_loop().time()

        url = f"{BASE_URL}{endpoint}"
        headers = {"Authorization": API_KEY}
        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429:
                    if attempt < retries:
                        print(f"  Rate limited, waiting 10s before retry...")
                        await asyncio.sleep(10)
                        continue
                    print(f"  Rate limited (exhausted retries) for {endpoint}")
                    return None
                else:
                    body = await resp.text()
                    print(f"  API error {resp.status} for {endpoint}: {body[:200]}")
                    return None
        except Exception as e:
            print(f"  API request failed: {e}")
            return None
    return None


async def main():
    # Get match data from DB
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM matches WHERE match_id = ?", (MATCH_ID,))
    match = dict(cur.fetchone())
    print(f"Match: id={match['match_id']}, short_id={match.get('short_id', 'N/A')}, decided={match['decided_at']}")

    # Get players and IGNs
    cur.execute("""
        SELECT mp.player_id, mp.team, pi.ign
        FROM match_players mp
        LEFT JOIN player_igns pi ON mp.player_id = pi.player_id AND pi.game_id = ?
        WHERE mp.match_id = ?
    """, (GAME_ID, MATCH_ID))
    players = cur.fetchall()
    igns = {}
    for p in players:
        if p['ign']:
            igns[p['player_id']] = p['ign']
            print(f"  {p['team']}: {p['ign']}")
        else:
            print(f"  {p['team']}: pid={p['player_id']} (NO IGN)")

    if not igns:
        print("ERROR: No IGNs found")
        return

    # Parse match times
    match_end_time = datetime.now(timezone.utc)
    if match.get("decided_at"):
        try:
            match_end_time = datetime.fromisoformat(match["decided_at"].replace('Z', '+00:00'))
        except Exception:
            pass

    match_created_at = None
    if match.get("created_at"):
        try:
            created_str = match["created_at"]
            if isinstance(created_str, str):
                created_str = created_str.replace('Z', '+00:00')
            match_created_at = datetime.fromisoformat(created_str)
            if match_created_at.tzinfo is None:
                match_created_at = match_created_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass

    print(f"\nMatch created: {match_created_at}")
    print(f"Match decided: {match_end_time}")

    our_player_igns = {ign.lower() for ign in igns.values() if '#' in ign}
    TIME_WINDOW = 10800  # 3 hours
    PLAYER_OVERLAP_THRESHOLD = 0.4

    async with aiohttp.ClientSession() as session:
        # Try each player's match history to find the Valorant match
        for pid, ign in igns.items():
            if '#' not in ign:
                continue

            name, tag = ign.rsplit('#', 1)
            print(f"\nSearching match history for '{ign}'...")

            # Try PUUID-based lookup first
            account_data = await api_request(session, f"/valorant/v1/account/{quote(name)}/{quote(tag)}")
            puuid = None
            if account_data and account_data.get('status') == 200:
                puuid = account_data.get('data', {}).get('puuid')
                if puuid:
                    print(f"  Resolved PUUID: {puuid[:12]}...")

            matches_list = None
            search_mode = None

            if puuid:
                # Try v3 PUUID custom matches
                data = await api_request(session, f"/valorant/v3/by-puuid/matches/na/{quote(puuid)}?mode=custom")
                if data and data.get('status') == 200:
                    matches_list = data.get('data', [])
                    search_mode = "v3-puuid-custom"

                if not matches_list:
                    data = await api_request(session, f"/valorant/v1/by-puuid/stored-matches/na/{quote(puuid)}?mode=custom")
                    if data and data.get('status') == 200:
                        matches_list = data.get('data', [])
                        search_mode = "stored-puuid-custom"

                if not matches_list:
                    data = await api_request(session, f"/valorant/v3/by-puuid/matches/na/{quote(puuid)}")
                    if data and data.get('status') == 200:
                        matches_list = data.get('data', [])
                        search_mode = "v3-puuid-all"

            if not matches_list:
                data = await api_request(session, f"/valorant/v1/stored-matches/na/{quote(name)}/{quote(tag)}?mode=custom")
                if data and data.get('status') == 200:
                    matches_list = data.get('data', [])
                    search_mode = "name-tag-custom"

                if not matches_list:
                    data = await api_request(session, f"/valorant/v1/stored-matches/na/{quote(name)}/{quote(tag)}")
                    if data and data.get('status') == 200:
                        matches_list = data.get('data', [])
                        search_mode = "name-tag-all"

            if not matches_list:
                print(f"  No match history found")
                continue

            print(f"  Found {len(matches_list)} matches via {search_mode}")

            # Score candidates by time proximity
            candidates = []
            for idx, m in enumerate(matches_list[:10]):
                metadata = m.get('metadata') or m.get('meta') or {}
                try:
                    game_start_ts = metadata.get('game_start')
                    game_length = metadata.get('game_length')
                    map_raw = metadata.get('map', 'unknown')
                    map_name = map_raw.get('name', 'unknown') if isinstance(map_raw, dict) else map_raw
                    mode = metadata.get('mode', 'unknown')

                    if not game_start_ts:
                        started_at = metadata.get('started_at')
                        if started_at:
                            game_start = isoparse(started_at)
                            if game_start.tzinfo is None:
                                game_start = game_start.replace(tzinfo=timezone.utc)
                        else:
                            continue
                    else:
                        game_start = datetime.fromtimestamp(game_start_ts, tz=timezone.utc)

                    game_end = None
                    if game_length and game_length > 0:
                        game_length_sec = game_length / 1000 if game_length > 10000 else game_length
                        game_end = game_start + timedelta(seconds=game_length_sec)

                    time_diffs = []
                    if match_created_at:
                        time_diffs.append(('start_vs_created', abs((game_start - match_created_at).total_seconds())))
                    if game_end and match_end_time:
                        time_diffs.append(('end_vs_decided', abs((game_end - match_end_time).total_seconds())))
                    if not time_diffs and match_end_time:
                        time_diffs.append(('start_vs_decided', abs((game_start - match_end_time).total_seconds())))
                    if not time_diffs:
                        continue

                    best_comparison, best_diff = min(time_diffs, key=lambda x: x[1])
                    within = "CANDIDATE" if best_diff <= TIME_WINDOW else "too far"
                    print(f"  [{idx}] map={map_name}, mode={mode}, start={game_start.isoformat()}, best_diff={best_diff:.0f}s ({best_comparison}) [{within}]")

                    if best_diff <= TIME_WINDOW:
                        val_match_id = metadata.get('matchid') or metadata.get('id')
                        if val_match_id:
                            candidates.append({
                                'idx': idx,
                                'valorant_match_id': val_match_id,
                                'time_diff': best_diff,
                                'comparison': best_comparison,
                                'map_name': map_name,
                            })
                except Exception as e:
                    print(f"  [{idx}] Error: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

            if not candidates:
                print(f"  No candidates within time window")
                continue

            candidates.sort(key=lambda c: c['time_diff'])
            print(f"  {len(candidates)} candidate(s), checking player overlap...")

            # Check player overlap
            for cand in candidates:
                val_match_id = cand['valorant_match_id']
                details_data = await api_request(session, f"/valorant/v2/match/{val_match_id}")
                if not details_data or details_data.get('status') != 200:
                    print(f"  Could not fetch details for {val_match_id}")
                    continue
                details = details_data.get('data', {})

                # Extract player IGNs from Valorant match
                val_igns = set()
                players_data = details.get('players', {})
                if isinstance(players_data, dict):
                    for team_key in ['all_players', 'red', 'blue', 'Red', 'Blue']:
                        for vp in players_data.get(team_key, []):
                            if isinstance(vp, dict):
                                vp_name = vp.get('name', '')
                                vp_tag = vp.get('tag', '')
                                if vp_name and vp_tag:
                                    val_igns.add(f"{vp_name}#{vp_tag}".lower())

                overlap = our_player_igns & val_igns
                overlap_ratio = len(overlap) / len(our_player_igns) if our_player_igns else 0
                print(f"  Candidate {val_match_id[:16]}...: overlap {len(overlap)}/{len(our_player_igns)} = {overlap_ratio:.0%}")
                if overlap:
                    print(f"    Matched: {sorted(overlap)}")
                if our_player_igns - val_igns:
                    print(f"    Not in Val match: {sorted(our_player_igns - val_igns)}")
                if val_igns - our_player_igns:
                    print(f"    In Val but not ours: {sorted(val_igns - our_player_igns)}")

                if overlap_ratio < PLAYER_OVERLAP_THRESHOLD:
                    print(f"  Rejecting - insufficient overlap")
                    continue

                # MATCH FOUND! Extract and save stats
                print(f"\n*** MATCH FOUND: {val_match_id} ***")
                map_data = details.get('metadata', {}).get('map')
                if isinstance(map_data, dict):
                    final_map = map_data.get('name')
                else:
                    final_map = map_data
                print(f"  Map: {final_map}")

                # Build val_players lookup
                val_players = {}
                if isinstance(players_data, dict):
                    for team_key in ['all_players', 'red', 'blue', 'Red', 'Blue']:
                        for vp in players_data.get(team_key, []):
                            if isinstance(vp, dict):
                                vp_name = vp.get('name', '')
                                vp_tag = vp.get('tag', '')
                                if vp_name and vp_tag:
                                    key = f"{vp_name}#{vp_tag}".lower()
                                    val_players[key] = vp

                # Save stats
                stats_saved = 0
                for save_pid, save_ign in igns.items():
                    normalized = save_ign.lower()
                    matched_key = None

                    # Exact match
                    if normalized in val_players:
                        matched_key = normalized
                    else:
                        for vk in val_players:
                            if vk.lower() == normalized.lower():
                                matched_key = vk
                                break

                    if not matched_key:
                        def strip_special(s):
                            return ''.join(c for c in s if c.isalnum() or c == '#').lower()
                        stripped = strip_special(normalized)
                        for vk in val_players:
                            if strip_special(vk) == stripped:
                                matched_key = vk
                                break

                    if not matched_key:
                        print(f"  Could not match '{save_ign}' to Valorant data")
                        continue

                    vp = val_players[matched_key]
                    stats = vp.get('stats', {})
                    agent = vp.get('character') or (
                        vp.get('agent', {}).get('name') if isinstance(vp.get('agent'), dict) else vp.get('agent')
                    )
                    damage = vp.get('damage_made', 0) or stats.get('damage', 0) or stats.get('damage_made', 0) or 0
                    first_bloods = vp.get('first_bloods', 0) or vp.get('first_blood', 0) or 0

                    k = stats.get('kills', 0)
                    d = stats.get('deaths', 0)
                    a = stats.get('assists', 0)
                    score = stats.get('score', 0)
                    hs = stats.get('headshots', 0)
                    bs = stats.get('bodyshots', 0)
                    ls = stats.get('legshots', 0)

                    print(f"  {save_ign}: {agent} | {k}/{d}/{a} | DMG:{damage}")

                    cur.execute("""
                        INSERT OR REPLACE INTO valorant_match_stats
                        (match_id, valorant_match_id, player_id, ign, agent, kills, deaths, assists,
                         headshots, bodyshots, legshots, score, map_name, damage_dealt, first_bloods,
                         plants, defuses, c2k, c3k, c4k, c5k, econ_spent, econ_loadout)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (MATCH_ID, val_match_id, save_pid, save_ign, agent,
                          k, d, a, hs, bs, ls, score, final_map, damage, first_bloods,
                          0, 0, 0, 0, 0, 0, 0, 0))
                    stats_saved += 1

                conn.commit()
                print(f"\nSaved stats for {stats_saved}/{len(igns)} players")
                conn.close()
                return

            print(f"  All candidates rejected for '{ign}'")

        print("\nFailed to find Valorant match via any player")
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
