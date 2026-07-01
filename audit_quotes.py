#!/usr/bin/env python3
"""
Audit quotes.db for quality issues. Read-only — never modifies the database.
Run: python3 audit_quotes.py
"""

import sqlite3
import json
import os
import re
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quotes.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_quote_config.json")

TV_SHOWS = {
    'Breaking Bad', 'Game of Thrones', 'The Office', 'Friends', 'Stranger Things',
    'The Mandalorian', 'Rick and Morty', 'Parks and Recreation', 'Seinfeld',
    'The Simpsons', 'How I Met Your Mother', 'Brooklyn Nine-Nine',
    "It's Always Sunny in Philadelphia", 'South Park', 'Peaky Blinders',
    'The Wire', 'Dexter', 'The Big Bang Theory', 'Friday Night Lights',
    'Arrested Development', 'Community', 'Psych', 'Downton Abbey',
}

# Known non-quote entries (song lyrics, plot events, etc.)
NON_QUOTES = {
    "thrift shop": "Song lyric, not a movie/TV quote",
    "red wedding.": "Plot event description, not an actual spoken quote",
}


def load_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM quotes ORDER BY id")
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def load_used_ids():
    if not os.path.exists(CONFIG_PATH):
        return []
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    return config.get("global_data", {}).get("used_quote_ids", [])


def normalize(text):
    """Lowercase, strip punctuation/whitespace for comparison."""
    return re.sub(r'[^\w\s]', '', text.lower()).strip()


def find_near_duplicates(rows):
    """Find quotes that are substrings of each other within the same movie+character."""
    groups = defaultdict(list)
    for r in rows:
        key = (r["movie_title"].lower(), r["character"].lower())
        groups[key].append(r)

    duplicates = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                norm_a = normalize(a["quote"])
                norm_b = normalize(b["quote"])
                if norm_a == norm_b:
                    duplicates.append((a, b, "exact (after normalization)"))
                elif norm_a in norm_b or norm_b in norm_a:
                    duplicates.append((a, b, "substring"))
                else:
                    # Jaccard word overlap
                    words_a = set(norm_a.split())
                    words_b = set(norm_b.split())
                    if words_a and words_b:
                        overlap = len(words_a & words_b) / len(words_a | words_b)
                        if overlap > 0.6:
                            duplicates.append((a, b, f"high overlap ({overlap:.0%})"))
    return duplicates


def find_cross_movie_duplicates(rows):
    """Find the same character+quote appearing under different movie titles (e.g. franchise variants)."""
    seen = {}
    duplicates = []
    for r in rows:
        key = (normalize(r["quote"]), r["character"].lower())
        if key in seen:
            duplicates.append((seen[key], r, "same quote+character, different title"))
        else:
            seen[key] = r
    return duplicates


def find_non_quotes(rows):
    """Flag entries that aren't movie/TV quotes."""
    flagged = []
    for r in rows:
        q_lower = r["quote"].lower().strip().rstrip(".")
        for pattern, reason in NON_QUOTES.items():
            if pattern in q_lower:
                flagged.append((r, reason))
                break
    return flagged


def find_franchise_ambiguous(rows):
    """Find quotes where the character appears in 2+ movies AND the character
    name is leaked in the quote text — making BOTH question types unfair."""
    # Build character -> set of movie titles
    char_movies = defaultdict(set)
    for r in rows:
        char_movies[r["character"]].append(r["movie_title"]) if False else char_movies[r["character"]].add(r["movie_title"])

    franchise_chars = {char for char, movies in char_movies.items() if len(movies) > 1}

    results = {"both_bad": [], "movie_only_bad": [], "char_only_bad": []}
    for r in rows:
        is_franchise = r["character"] in franchise_chars
        # Check if character name leaks into the quote
        quote_lower = r["quote"].lower()
        char_parts = [p.strip().lower() for p in r["character"].replace("-", " ").split() if len(p.strip()) > 2]
        char_leaked = any(part in quote_lower for part in char_parts) if char_parts else False

        if is_franchise and char_leaked:
            movies = char_movies[r["character"]]
            results["both_bad"].append((r, movies))
        elif is_franchise:
            movies = char_movies[r["character"]]
            results["movie_only_bad"].append((r, movies))
        elif char_leaked:
            results["char_only_bad"].append((r, None))

    return results


def find_single_quote_titles(rows):
    """Titles with only 1 quote — character questions are trivial."""
    counts = defaultdict(list)
    for r in rows:
        counts[r["movie_title"]].append(r)
    return {title: quotes for title, quotes in counts.items() if len(quotes) == 1}


def find_single_character_titles(rows):
    """Titles with 2+ quotes but only 1 distinct character."""
    title_data = defaultdict(lambda: {"quotes": [], "characters": set()})
    for r in rows:
        title_data[r["movie_title"]]["quotes"].append(r)
        title_data[r["movie_title"]]["characters"].add(r["character"])
    return {
        title: data for title, data in title_data.items()
        if len(data["quotes"]) >= 2 and len(data["characters"]) == 1
    }


def analyze_genre_breadth(rows):
    """Show how many titles share each primary genre."""
    genre_titles = defaultdict(set)
    for r in rows:
        primary = r["genre"].split(",")[0].strip()
        genre_titles[primary].add(r["movie_title"])
    return {genre: titles for genre, titles in sorted(genre_titles.items(), key=lambda x: -len(x[1]))}


def check_tv_classification(rows):
    """Find titles that might be misclassified as TV/movie."""
    misclassified = []
    known_movies_in_tv_set = {"Office Space"}  # Known mistakes
    for title in known_movies_in_tv_set:
        for r in rows:
            if r["movie_title"] == title:
                misclassified.append((r, f"'{title}' is a movie but listed as TV in build script"))
                break
    return misclassified


def check_used_quotes(rows, used_ids):
    """Report on used quotes and cross-check with issues."""
    used = [r for r in rows if r["id"] in used_ids]
    return used


def main():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    rows = load_db()
    used_ids = load_used_ids()
    total = len(rows)

    print(f"{'=' * 70}")
    print(f"  DAILY QUOTE AUDIT REPORT")
    print(f"  Database: {DB_PATH}")
    print(f"  Total quotes: {total} | Used: {len(used_ids)} | Remaining: {total - len(used_ids)}")
    print(f"{'=' * 70}")

    # --- Near-duplicates ---
    print(f"\n{'─' * 70}")
    print("1. NEAR-DUPLICATE QUOTES (same movie + character)")
    print(f"{'─' * 70}")
    near_dupes = find_near_duplicates(rows)
    if near_dupes:
        removal_candidates = []
        for a, b, reason in near_dupes:
            print(f"\n  [{reason}]")
            print(f"    ID {a['id']:>3}: \"{a['quote'][:70]}...\" " if len(a['quote']) > 70 else f"    ID {a['id']:>3}: \"{a['quote']}\"")
            print(f"           ({a['character']} — {a['movie_title']})")
            print(f"    ID {b['id']:>3}: \"{b['quote'][:70]}...\" " if len(b['quote']) > 70 else f"    ID {b['id']:>3}: \"{b['quote']}\"")
            print(f"           ({b['character']} — {b['movie_title']})")
            # Recommend keeping the shorter/more iconic version
            shorter = a if len(a['quote']) <= len(b['quote']) else b
            longer = b if shorter == a else a
            is_used_shorter = shorter['id'] in used_ids
            is_used_longer = longer['id'] in used_ids
            if is_used_longer and not is_used_shorter:
                removal_candidates.append(shorter['id'])
                print(f"    → RECOMMEND: Remove ID {shorter['id']} (longer ID {longer['id']} is already used)")
            elif is_used_shorter and not is_used_longer:
                removal_candidates.append(longer['id'])
                print(f"    → RECOMMEND: Remove ID {longer['id']} (shorter ID {shorter['id']} is already used)")
            else:
                removal_candidates.append(longer['id'])
                print(f"    → RECOMMEND: Remove ID {longer['id']} (keep shorter/more iconic version)")
        print(f"\n  ACTION: Remove IDs: {removal_candidates}")
    else:
        print("  None found. ✓")

    # --- Cross-movie duplicates ---
    print(f"\n{'─' * 70}")
    print("2. CROSS-TITLE DUPLICATES (same quote+character, different movie)")
    print(f"{'─' * 70}")
    cross_dupes = find_cross_movie_duplicates(rows)
    if cross_dupes:
        for a, b, reason in cross_dupes:
            print(f"\n    ID {a['id']:>3}: \"{a['quote'][:60]}\" → {a['movie_title']}")
            print(f"    ID {b['id']:>3}: \"{b['quote'][:60]}\" → {b['movie_title']}")
            print(f"    → Keep whichever title is more recognized")
    else:
        print("  None found. ✓")

    # --- Non-quotes ---
    print(f"\n{'─' * 70}")
    print("3. NON-QUOTE ENTRIES")
    print(f"{'─' * 70}")
    non_quotes = find_non_quotes(rows)
    if non_quotes:
        for r, reason in non_quotes:
            used_flag = " [USED]" if r['id'] in used_ids else ""
            print(f"    ID {r['id']:>3}: \"{r['quote'][:60]}\" → {reason}{used_flag}")
        print(f"\n  ACTION: Remove these entries (IDs: {[r['id'] for r, _ in non_quotes]})")
    else:
        print("  None found. ✓")

    # --- Franchise ambiguity ---
    print(f"\n{'─' * 70}")
    print("4. FRANCHISE AMBIGUITY ANALYSIS")
    print(f"{'─' * 70}")
    ambig = find_franchise_ambiguous(rows)

    if ambig["both_bad"]:
        print(f"\n  ❌ BOTH QUESTION TYPES FAIL ({len(ambig['both_bad'])} quotes):")
        print(f"     (character in 2+ movies AND name leaked in quote — bot will skip these)")
        for r, movies in ambig["both_bad"]:
            used_flag = " [USED]" if r['id'] in used_ids else ""
            print(f"    ID {r['id']:>3}: \"{r['quote'][:55]}\" ({r['character']} — {r['movie_title']}){used_flag}")
            print(f"           Also in: {', '.join(m for m in movies if m != r['movie_title'])}")
    else:
        print("\n  No quotes fail both question types. ✓")

    if ambig["movie_only_bad"]:
        print(f"\n  ⚠ MOVIE QUESTION RISKY ({len(ambig['movie_only_bad'])} quotes):")
        print(f"     (character in 2+ movies — bot will force character question type)")
        for r, movies in ambig["movie_only_bad"]:
            used_flag = " [USED]" if r['id'] in used_ids else ""
            print(f"    ID {r['id']:>3}: \"{r['quote'][:55]}\" ({r['character']} — {r['movie_title']}){used_flag}")
            print(f"           Also in: {', '.join(m for m in movies if m != r['movie_title'])}")

    if ambig["char_only_bad"]:
        print(f"\n  ⚠ CHARACTER QUESTION TRIVIAL ({len(ambig['char_only_bad'])} quotes):")
        print(f"     (character name appears in quote text — bot will force movie question type)")
        for r, _ in ambig["char_only_bad"]:
            used_flag = " [USED]" if r['id'] in used_ids else ""
            print(f"    ID {r['id']:>3}: \"{r['quote'][:55]}\" ({r['character']} — {r['movie_title']}){used_flag}")

    total_ambig = len(ambig["both_bad"]) + len(ambig["movie_only_bad"]) + len(ambig["char_only_bad"])
    print(f"\n  Total flagged: {total_ambig} / {total} quotes ({total_ambig*100//total}%)")

    # --- Single-quote titles ---
    print(f"\n{'─' * 70}")
    print("5. SINGLE-QUOTE TITLES (character questions are trivial)")
    print(f"{'─' * 70}")
    singles = find_single_quote_titles(rows)
    print(f"  {len(singles)} titles have only 1 quote:")
    for title, quotes in sorted(singles.items()):
        q = quotes[0]
        tv_flag = " [TV]" if title in TV_SHOWS else ""
        print(f"    • {title}{tv_flag}: \"{q['quote'][:50]}...\" ({q['character']})" if len(q['quote']) > 50 else f"    • {title}{tv_flag}: \"{q['quote']}\" ({q['character']})")
    print(f"\n  ACTION: Add 1-2 more quotes per title, or force movie-type questions for these")

    # --- Single-character titles ---
    print(f"\n{'─' * 70}")
    print("6. SINGLE-CHARACTER TITLES (2+ quotes, all same character)")
    print(f"{'─' * 70}")
    single_chars = find_single_character_titles(rows)
    if single_chars:
        for title, data in sorted(single_chars.items(), key=lambda x: -len(x[1]["quotes"])):
            char = list(data["characters"])[0]
            print(f"    • {title}: {len(data['quotes'])} quotes, all from {char}")
        print(f"\n  ACTION: Add quotes from other characters, or force movie-type questions for these")
    else:
        print("  None found. ✓")

    # --- Genre breadth ---
    print(f"\n{'─' * 70}")
    print("7. GENRE BREADTH ANALYSIS")
    print(f"{'─' * 70}")
    genre_data = analyze_genre_breadth(rows)
    for genre, titles in genre_data.items():
        warning = " ⚠ TOO BROAD FOR DISTRACTORS" if len(titles) >= 25 else ""
        print(f"    {genre}: {len(titles)} titles{warning}")

    # --- TV classification ---
    print(f"\n{'─' * 70}")
    print("8. TV/MOVIE CLASSIFICATION ISSUES")
    print(f"{'─' * 70}")
    misclassified = check_tv_classification(rows)
    if misclassified:
        for r, reason in misclassified:
            print(f"    ID {r['id']:>3}: {reason}")
    else:
        print("  None found. ✓")

    # --- TV vs Movie stats ---
    print(f"\n{'─' * 70}")
    print("9. TV vs MOVIE BREAKDOWN")
    print(f"{'─' * 70}")
    all_titles = set(r["movie_title"] for r in rows)
    tv_titles = all_titles & TV_SHOWS
    movie_titles = all_titles - TV_SHOWS
    tv_quotes = [r for r in rows if r["movie_title"] in TV_SHOWS]
    movie_quotes = [r for r in rows if r["movie_title"] not in TV_SHOWS]
    print(f"    TV shows:  {len(tv_titles)} titles, {len(tv_quotes)} quotes")
    print(f"    Movies:    {len(movie_titles)} titles, {len(movie_quotes)} quotes")

    # --- Used quotes report ---
    print(f"\n{'─' * 70}")
    print("10. USED QUOTES (never reuse)")
    print(f"{'─' * 70}")
    used_quotes = check_used_quotes(rows, used_ids)
    if used_quotes:
        for r in used_quotes:
            print(f"    ID {r['id']:>3}: \"{r['quote'][:50]}\" ({r['movie_title']})")
    else:
        print("  No used quotes found in DB (IDs may have shifted).")

    # --- Quotes per title distribution ---
    print(f"\n{'─' * 70}")
    print("11. QUOTES PER TITLE (top 20)")
    print(f"{'─' * 70}")
    title_counts = defaultdict(int)
    for r in rows:
        title_counts[r["movie_title"]] += 1
    for title, count in sorted(title_counts.items(), key=lambda x: -x[1])[:20]:
        bar = "█" * count
        print(f"    {count:>2} {bar} {title}")

    # --- Summary ---
    dupe_removals = set()
    for a, b, _ in near_dupes:
        shorter = a if len(a['quote']) <= len(b['quote']) else b
        longer = b if shorter == a else a
        if longer['id'] in used_ids:
            dupe_removals.add(shorter['id'])
        else:
            dupe_removals.add(longer['id'])
    non_quote_ids = {r['id'] for r, _ in non_quotes}
    all_removals = dupe_removals | non_quote_ids

    print(f"\n{'=' * 70}")
    print("  SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Near-duplicate pairs:     {len(near_dupes)}")
    print(f"  Non-quote entries:        {len(non_quotes)}")
    print(f"  Franchise: both bad:      {len(ambig['both_bad'])} (bot will skip these)")
    print(f"  Franchise: movie risky:   {len(ambig['movie_only_bad'])} (bot forces character Q)")
    print(f"  Char name leaked:         {len(ambig['char_only_bad'])} (bot forces movie Q)")
    print(f"  Single-quote titles:      {len(singles)}")
    print(f"  Single-character titles:  {len(single_chars)}")
    print(f"  Total IDs to remove:      {len(all_removals)}")
    if all_removals:
        # Check if any removals are in used_ids
        used_removals = all_removals & set(used_ids)
        if used_removals:
            print(f"  ⚠ WARNING: {len(used_removals)} removal candidates are already used: {used_removals}")
        print(f"  Removal IDs: {sorted(all_removals)}")
    print(f"  Quotes after cleanup:     {total - len(all_removals)}")
    print()


if __name__ == "__main__":
    main()
