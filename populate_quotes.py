#!/usr/bin/env python3
"""
Populate quotes.db from the Cornell Movie-Dialogs Corpus.

Usage:
    python3 populate_quotes.py <path_to_corpus_directory>

    Example:
    python3 populate_quotes.py "cornell_corpus_raw/cornell movie-dialogs corpus"

The corpus directory should contain:
    - movie_lines.txt
    - movie_titles_metadata.txt

Lines are filtered to keep only substantial, quote-worthy dialogue (40-250 chars)
from popular movies (configurable vote threshold).
"""

import ast
import sqlite3
import sys
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quotes.db")

# --- FILTERING ---
MIN_LINE_LENGTH = 40   # Skip short filler dialogue
MAX_LINE_LENGTH = 250  # Skip overly long monologues (won't fit in buttons/embeds well)


def title_case(s):
    """Title-case a movie name, handling common patterns."""
    # The corpus stores titles in lowercase
    exceptions = {"i", "ii", "iii", "iv", "v", "vi", "a", "an", "the", "and",
                  "but", "or", "for", "nor", "on", "at", "to", "in", "of", "with"}
    words = s.split()
    result = []
    for i, word in enumerate(words):
        if i == 0 or word.lower() not in exceptions:
            # Handle special cases like "spider-man"
            if "-" in word:
                result.append("-".join(p.capitalize() for p in word.split("-")))
            else:
                result.append(word.capitalize())
        else:
            result.append(word.lower())
    return " ".join(result)


def parse_cornell_line(line):
    """Parse a +++$+++ delimited line into a list of fields."""
    return [field.strip() for field in line.split(" +++$+++ ")]


def create_db(conn):
    """Create the quotes table and indexes."""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS quotes")
    cursor.execute("""
        CREATE TABLE quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote TEXT NOT NULL,
            character TEXT NOT NULL,
            movie_title TEXT NOT NULL,
            genre TEXT NOT NULL,
            imdb_votes INTEGER NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX idx_genre ON quotes(genre)")
    cursor.execute("CREATE INDEX idx_movie_title ON quotes(movie_title)")
    cursor.execute("CREATE INDEX idx_imdb_votes ON quotes(imdb_votes)")
    conn.commit()


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 populate_quotes.py <path_to_corpus_directory>")
        print('Example: python3 populate_quotes.py "cornell_corpus_raw/cornell movie-dialogs corpus"')
        sys.exit(1)

    corpus_dir = sys.argv[1]
    lines_file = os.path.join(corpus_dir, "movie_lines.txt")
    titles_file = os.path.join(corpus_dir, "movie_titles_metadata.txt")

    for f in [lines_file, titles_file]:
        if not os.path.exists(f):
            print(f"Error: File not found: {f}")
            sys.exit(1)

    # --- Step 1: Parse movie metadata ---
    print("Parsing movie metadata...")
    movies = {}
    with open(titles_file, "r", encoding="latin-1") as f:
        for line in f:
            parts = parse_cornell_line(line)
            if len(parts) < 6:
                continue
            mid = parts[0]
            title = title_case(parts[1])
            year = parts[2].strip()
            try:
                votes = int(parts[4])
            except ValueError:
                votes = 0
            try:
                genres = ast.literal_eval(parts[5])
                # Title-case and join genres: ['comedy', 'romance'] -> "Comedy, Romance"
                genre_str = ", ".join(g.capitalize() for g in genres)
            except (ValueError, SyntaxError):
                genre_str = "Unknown"

            movies[mid] = {
                "title": title,
                "year": year,
                "votes": votes,
                "genre": genre_str,
            }

    print(f"  Found {len(movies)} movies")

    # --- Step 2: Parse and filter dialogue lines ---
    print("Parsing dialogue lines...")
    conn = sqlite3.connect(DB_PATH)
    create_db(conn)
    cursor = conn.cursor()

    imported = 0
    skipped_no_movie = 0
    skipped_length = 0
    skipped_missing = 0
    genre_counts = {}

    with open(lines_file, "r", encoding="latin-1") as f:
        for line in f:
            parts = parse_cornell_line(line)
            if len(parts) < 5:
                continue

            mid = parts[2]
            character = parts[3].strip()
            text = parts[4].strip()

            # Must have a matching movie
            if mid not in movies:
                skipped_no_movie += 1
                continue

            movie = movies[mid]

            # Skip if missing data
            if not text or not character:
                skipped_missing += 1
                continue

            # Length filter: skip filler and overly long lines
            if len(text) < MIN_LINE_LENGTH or len(text) > MAX_LINE_LENGTH:
                skipped_length += 1
                continue

            # Title-case character names (corpus has them in ALL CAPS)
            character = title_case(character)

            cursor.execute(
                "INSERT INTO quotes (quote, character, movie_title, genre, imdb_votes) VALUES (?, ?, ?, ?, ?)",
                (text, character, movie.get("title", ""), movie.get("genre", "Unknown"), movie["votes"])
            )
            imported += 1

            for g in movie["genre"].split(","):
                g = g.strip()
                if g:
                    genre_counts[g] = genre_counts.get(g, 0) + 1

    conn.commit()

    # --- Step 3: Print stats ---
    for threshold in [50000, 75000, 100000]:
        cursor.execute("SELECT COUNT(*) FROM quotes WHERE imdb_votes > ?", (threshold,))
        count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT movie_title) FROM quotes WHERE imdb_votes > ?", (threshold,))
        movie_count = cursor.fetchone()[0]
        print(f"  >{threshold:>7,} votes: {count:,} quotes from {movie_count} movies")

    cursor.execute("SELECT COUNT(*) FROM quotes")
    total_in_db = cursor.fetchone()[0]

    conn.close()

    print(f"\n{'='*50}")
    print(f"Import complete!")
    print(f"{'='*50}")
    print(f"  Lines imported:              {imported:,}")
    print(f"  Skipped (too short/long):    {skipped_length:,}")
    print(f"  Skipped (no matching movie): {skipped_no_movie:,}")
    print(f"  Skipped (missing data):      {skipped_missing:,}")
    print(f"  Total in DB:                 {total_in_db:,}")
    print(f"\nGenre distribution:")
    for genre, count in sorted(genre_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {genre}: {count:,}")
    print(f"\nDatabase saved to: {DB_PATH}")


if __name__ == "__main__":
    main()
