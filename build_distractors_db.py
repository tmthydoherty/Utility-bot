#!/usr/bin/env python3
"""
Fetch popular movies & TV shows from TMDb and build a local distractors
database for the Daily Quote cog.

Run once to populate, re-run anytime to refresh:
    python3 build_distractors_db.py

Requires TMDB_API_KEY in the .env file.
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
import urllib.parse
import urllib.error

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "distractors.db")
ENV_PATH = os.path.join(SCRIPT_DIR, ".env")
BASE_URL = "https://api.themoviedb.org/3"

# ---------------------------------------------------------------------------
# TMDb genre ID -> display name
# ---------------------------------------------------------------------------
GENRE_MAP = {
    # Movie genres
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Sci-Fi",
    10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
    # TV-only genres
    10759: "Action & Adventure", 10762: "Kids", 10763: "News",
    10764: "Reality", 10765: "Sci-Fi & Fantasy", 10766: "Soap",
    10767: "Talk", 10768: "War & Politics",
}

# Skip these — not useful as trivia distractors
SKIP_GENRE_IDS = {99, 10763, 10764, 10766, 10767, 10770}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_api_key():
    key = os.environ.get("TMDB_API_KEY")
    if key:
        return key
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("TMDB_API_KEY="):
                    return line.split("=", 1)[1].strip()
    return None


API_KEY = get_api_key()
if not API_KEY:
    print("ERROR: TMDB_API_KEY not found. Add it to .env or export it.")
    sys.exit(1)


def tmdb_get(path, params=None):
    """GET request to TMDb with retry on rate-limit."""
    if params is None:
        params = {}
    params["api_key"] = API_KEY
    url = f"{BASE_URL}{path}?{urllib.parse.urlencode(params)}"
    for _ in range(3):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2)
                continue
            print(f"\n  HTTP {e.code} on {path}")
            return None
        except (urllib.error.URLError, TimeoutError):
            time.sleep(1)
            continue
    return None


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_titles(media_type, pages, min_votes):
    """Fetch popular titles from /discover endpoint."""
    titles = []
    seen_ids = set()
    for page in range(1, pages + 1):
        params = {
            "sort_by": "vote_count.desc",
            "vote_count.gte": min_votes,
            "with_original_language": "en",
            "page": page,
        }
        data = tmdb_get(f"/discover/{media_type}", params)
        if not data or not data.get("results"):
            break

        for item in data["results"]:
            tmdb_id = item["id"]
            if tmdb_id in seen_ids:
                continue
            seen_ids.add(tmdb_id)

            genre_ids = item.get("genre_ids", [])
            if any(g in SKIP_GENRE_IDS for g in genre_ids):
                continue

            title = item.get("title") if media_type == "movie" else item.get("name")
            date_str = item.get("release_date" if media_type == "movie" else "first_air_date", "")
            year = int(date_str[:4]) if date_str and len(date_str) >= 4 else None
            genres = ", ".join(GENRE_MAP.get(g, str(g)) for g in genre_ids if g not in SKIP_GENRE_IDS)

            titles.append({
                "tmdb_id": tmdb_id,
                "title": title,
                "year": year,
                "genre_ids": json.dumps(genre_ids),
                "genres": genres,
                "popularity": item.get("popularity", 0),
                "vote_count": item.get("vote_count", 0),
                "is_tv": 1 if media_type == "tv" else 0,
            })

        sys.stdout.write(f"\r  {media_type.capitalize()}s: page {page}/{pages} ({len(titles)} titles)")
        sys.stdout.flush()
        time.sleep(0.04)

    print()
    return titles


def fetch_credits(tmdb_id, is_tv, max_cast=5):
    """Fetch top cast for a title."""
    endpoint = f"/{'tv' if is_tv else 'movie'}/{tmdb_id}/credits"
    data = tmdb_get(endpoint)
    if not data:
        return []

    skip_names = {"self", "himself", "herself", "narrator", "various", ""}
    characters = []
    for member in data.get("cast", [])[:max_cast]:
        name = member.get("character", "").strip()
        name = name.split(" / ")[0].strip()
        for suffix in (" (voice)", " (uncredited)", " (archive footage)"):
            name = name.replace(suffix, "")
        name = name.strip()
        if name.lower() in skip_names or not name:
            continue
        characters.append({
            "character": name,
            "actor": member.get("name", ""),
            "order": member.get("order", 99),
        })
    return characters


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def create_db(conn):
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS characters")
    c.execute("DROP TABLE IF EXISTS titles")
    c.execute("""
        CREATE TABLE titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tmdb_id INTEGER UNIQUE NOT NULL,
            title TEXT NOT NULL,
            year INTEGER,
            genre_ids TEXT,
            genres TEXT,
            popularity REAL,
            vote_count INTEGER,
            is_tv INTEGER NOT NULL DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE characters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_id INTEGER NOT NULL,
            character_name TEXT NOT NULL,
            actor_name TEXT,
            cast_order INTEGER,
            FOREIGN KEY (title_id) REFERENCES titles(id)
        )
    """)
    c.execute("CREATE INDEX idx_titles_is_tv ON titles(is_tv)")
    c.execute("CREATE INDEX idx_titles_vote_count ON titles(vote_count)")
    c.execute("CREATE INDEX idx_titles_year ON titles(year)")
    c.execute("CREATE INDEX idx_char_title ON characters(title_id)")
    c.execute("CREATE INDEX idx_char_name ON characters(character_name)")
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  TMDb Distractors Database Builder")
    print("=" * 60)
    print()

    # -- Fetch titles --
    print("  [1/3] Fetching titles from TMDb...\n")
    movies = fetch_titles("movie", pages=30, min_votes=500)
    tv = fetch_titles("tv", pages=15, min_votes=200)
    all_titles = movies + tv
    print(f"\n  {len(movies)} movies + {len(tv)} TV = {len(all_titles)} total\n")

    # -- Build DB --
    conn = sqlite3.connect(DB_PATH)
    create_db(conn)
    cursor = conn.cursor()

    id_map = {}  # tmdb_id -> (db_id, is_tv)
    for t in all_titles:
        cursor.execute(
            """INSERT OR IGNORE INTO titles
               (tmdb_id, title, year, genre_ids, genres, popularity, vote_count, is_tv)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (t["tmdb_id"], t["title"], t["year"], t["genre_ids"],
             t["genres"], t["popularity"], t["vote_count"], t["is_tv"])
        )
        if cursor.lastrowid:
            id_map[t["tmdb_id"]] = (cursor.lastrowid, t["is_tv"])
    conn.commit()

    # -- Fetch credits --
    print(f"  [2/3] Fetching cast for {len(id_map)} titles...\n")
    total_chars = 0
    for i, (tmdb_id, (db_id, is_tv)) in enumerate(id_map.items()):
        chars = fetch_credits(tmdb_id, is_tv)
        for ch in chars:
            cursor.execute(
                "INSERT INTO characters (title_id, character_name, actor_name, cast_order) VALUES (?, ?, ?, ?)",
                (db_id, ch["character"], ch["actor"], ch["order"])
            )
            total_chars += 1

        if (i + 1) % 50 == 0 or i == len(id_map) - 1:
            conn.commit()
            sys.stdout.write(f"\r  Credits: {i+1}/{len(id_map)} titles | {total_chars} characters")
            sys.stdout.flush()
        time.sleep(0.03)

    conn.commit()
    print("\n")

    # -- Stats --
    print("  [3/3] Summary\n")
    cursor.execute("SELECT COUNT(*) FROM titles WHERE is_tv = 0")
    n_movies = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM titles WHERE is_tv = 1")
    n_tv = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT character_name) FROM characters")
    n_chars = cursor.fetchone()[0]

    print("=" * 60)
    print(f"  Movies:            {n_movies:,}")
    print(f"  TV Shows:          {n_tv:,}")
    print(f"  Unique Characters: {n_chars:,}")
    print("=" * 60)

    print("\n  By Decade:")
    for decade in range(1920, 2030, 10):
        cursor.execute("SELECT COUNT(*) FROM titles WHERE year BETWEEN ? AND ?", (decade, decade + 9))
        cnt = cursor.fetchone()[0]
        if cnt:
            print(f"    {decade}s: {cnt:>4}  {'|' * (cnt // 8)}")

    print(f"\n  Saved to: {DB_PATH}\n")
    conn.close()


if __name__ == "__main__":
    main()
