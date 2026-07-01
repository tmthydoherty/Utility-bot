from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from pathlib import Path
import re
import secrets
import unicodedata
import logging

logger = logging.getLogger('custommatch')

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
K_FACTOR_PLACEMENT = 80   # Games 1-10 (~40 per even game)
K_FACTOR_LEARNING = 40    # Games 11-20 (~20 per even game)
K_FACTOR_STABLE = 20      # Games 21+ (~10 per even game)


def is_valorant_game(game) -> bool:
    """True if the given game is Valorant (name-based)."""
    if game is None:
        return False
    name = getattr(game, "name", None) or (game if isinstance(game, str) else "")
    return 'valorant' in str(name).lower()


def is_rivals_game(game) -> bool:
    """True if the given game is Marvel Rivals (name-based)."""
    if game is None:
        return False
    name = getattr(game, "name", None) or (game if isinstance(game, str) else "")
    return 'rivals' in str(name).lower()


# Canonical Rivals roles
RIVALS_ROLES = ("Vanguard", "Duelist", "Strategist")


def _parse_tracker_url(url: str) -> Optional[str]:
    """Extract IGN from a tracker.gg profile URL.

    Supports:
      - tracker.gg/valorant/profile/riot/{name}%23{tag}/... -> name#tag
      - tracker.gg/rivals/profile/{name}/... -> name
    Returns None if URL doesn't match.
    """
    from urllib.parse import unquote
    url = url.strip()
    # Valorant: .../valorant/profile/riot/{encoded_name}%23{tag}/...
    val_marker = "tracker.gg/valorant/profile/riot/"
    idx = url.find(val_marker)
    if idx != -1:
        remainder = url[idx + len(val_marker):]
        # Take everything before the next slash (or end)
        slug = remainder.split("/")[0].split("?")[0]
        decoded = unquote(slug)
        # Should now be "Name#Tag"
        if '#' in decoded:
            return decoded
        return None
    # Rivals: .../marvel-rivals/profile/ign/{name}/... or .../rivals/profile/{name}/...
    for rivals_marker in ("tracker.gg/marvel-rivals/profile/ign/", "tracker.gg/rivals/profile/"):
        idx = url.find(rivals_marker)
        if idx != -1:
            remainder = url[idx + len(rivals_marker):]
            slug = remainder.split("/")[0].split("?")[0]
            decoded = unquote(slug)
            if decoded:
                return decoded
    return None


def _streak_bonus_multiplier(consecutive_wins: int) -> float:
    """Bonus MMR multiplier for 5+ win streaks.

    Returns 1.0 for streaks < 5, scaling up to 1.50 cap.
    5->1.25, 6->1.35, 7->1.45, 8+->1.50
    """
    if consecutive_wins < 5:
        return 1.0
    bonus = min(0.50, 0.25 + (consecutive_wins - 5) * 0.10)
    return 1.0 + bonus


def _levenshtein(a: str, b: str) -> int:
    """Standard Levenshtein edit distance. Inputs are short IGNs (<=20 chars)."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            curr[j] = min(
                curr[j - 1] + 1,        # insertion
                prev[j] + 1,            # deletion
                prev[j - 1] + cost,     # substitution
            )
        prev = curr
    return prev[-1]


def resolve_ocr_ign(ocr_ign: str, ign_to_player: Dict[str, int]) -> Optional[int]:
    """Map an OCR'd IGN to a player_id using multi-tier matching.

    1. Exact case-insensitive match.
    2. Alphanumeric-only match (strips tags/punctuation/whitespace).
    3. Levenshtein fallback against the alphanumeric form: tolerates 1 edit
       for names >=5 chars, 2 edits for names >=9 chars. Only accepted when
       exactly one candidate in the lookup fits the threshold -- any
       ambiguity falls through to unmapped so the admin resolves it
       manually instead of guessing wrong.

    This exists because single-character OCR misreads (e.g. 'Worldsbest55'
    vs 'Wurldsbest55') were otherwise forcing the resolver to re-prompt
    every single match.
    """
    if not ocr_ign:
        return None
    key = ocr_ign.strip().lower()
    if not key:
        return None

    # Tier 1: exact
    pid = ign_to_player.get(key)
    if pid is not None:
        return pid

    # Tier 2: alphanumeric-only (cheap, high-precision)
    # Use NFKD normalization to decompose diacritics (ã→a, š→s) before stripping
    simple = re.sub(r'[^a-z0-9]', '', unicodedata.normalize('NFKD', key).encode('ascii', 'ignore').decode())
    if not simple:
        return None
    simple_to_pid: Dict[str, int] = {}
    for known_ign, known_pid in ign_to_player.items():
        known_simple = re.sub(r'[^a-z0-9]', '', unicodedata.normalize('NFKD', known_ign).encode('ascii', 'ignore').decode())
        if known_simple:
            simple_to_pid.setdefault(known_simple, known_pid)
    if simple in simple_to_pid:
        return simple_to_pid[simple]

    # Tier 3: bounded Levenshtein over the simplified forms, unique match only
    if len(simple) >= 9:
        max_edits = 2
    elif len(simple) >= 5:
        max_edits = 1
    else:
        return None  # too short -- fuzzy is unsafe

    best_pid: Optional[int] = None
    best_distance = max_edits + 1
    tied = False
    for known_simple, known_pid in simple_to_pid.items():
        # Cheap length prefilter -- can't be within max_edits if lengths differ more
        if abs(len(known_simple) - len(simple)) > max_edits:
            continue
        d = _levenshtein(simple, known_simple)
        if d > max_edits:
            continue
        if d < best_distance:
            best_distance = d
            best_pid = known_pid
            tied = False
        elif d == best_distance and known_pid != best_pid:
            tied = True
    if tied:
        return None
    return best_pid


def normalize_rivals_role(value) -> Optional[str]:
    """Normalize a role string to one of the canonical RIVALS_ROLES, or None."""
    if not value:
        return None
    v = str(value).strip().lower()
    if v.startswith("van") or "tank" in v or "shield" in v:
        return "Vanguard"
    if v.startswith("due") or "dps" in v or "damage" in v:
        return "Duelist"
    if v.startswith("str") or "support" in v or "heal" in v:
        return "Strategist"
    return None


def display_width(s: str) -> int:
    """Calculate the monospace display width of a string.

    Wide/fullwidth characters (emojis, CJK, etc.) occupy 2 columns in a
    monospace font, while normal ASCII characters occupy 1.  Python's
    ``str.ljust`` and f-string ``<N`` padding count *characters*, not
    columns, so names with wide chars misalign fixed-width code blocks.
    """
    width = 0
    for ch in s:
        eaw = unicodedata.east_asian_width(ch)
        if eaw in ('W', 'F'):
            width += 2
        elif unicodedata.category(ch) in ('Mn', 'Me', 'Cf'):
            # Non-spacing marks and format chars take 0 columns
            width += 0
        else:
            width += 1
    return width


def pad_to_width(s: str, target_width: int) -> str:
    """Left-align *s* and pad with spaces to reach *target_width* columns.

    Unlike ``f'{s:<N}'`` this accounts for wide Unicode characters so that
    monospace code-block columns stay aligned.
    """
    current = display_width(s)
    if current >= target_width:
        return s
    return s + ' ' * (target_width - current)


def truncate_to_width(s: str, max_width: int) -> str:
    """Truncate *s* so its display width is at most *max_width*.

    If truncation is needed the last visible character is replaced with '.'.
    """
    width = 0
    for i, ch in enumerate(s):
        eaw = unicodedata.east_asian_width(ch)
        if eaw in ('W', 'F'):
            cw = 2
        elif unicodedata.category(ch) in ('Mn', 'Me', 'Cf'):
            cw = 0
        else:
            cw = 1
        if width + cw > max_width:
            return s[:i]
        width += cw
    return s


def _strip_to_latin(name: str) -> str:
    """Keep only ASCII printable + Latin Extended (U+0020..U+024F), collapse spaces."""
    out = []
    for ch in name:
        cp = ord(ch)
        if 0x20 <= cp <= 0x024F:
            out.append(ch)
    result = ''.join(out).strip()
    while '  ' in result:
        result = result.replace('  ', ' ')
    return result


def sanitize_for_codeblock(name: str, fallback: Optional[str] = None) -> str:
    """Strip non-Latin characters from a name so monospace code blocks align.

    Only keeps ASCII printable and Latin Extended (U+0020..U+024F) which are
    guaranteed to render as single-width glyphs in Discord's code-block font.
    Emoji, CJK, math-styled letters, and decorative Unicode symbols are all
    stripped because their rendered width is unpredictable in code blocks.

    If the stripped display name is empty (fully non-Latin glyphs), falls back
    to `fallback` (typically the user's raw Discord username, which is ASCII)
    before giving up on '???'.
    """
    result = _strip_to_latin(name or "")
    if result:
        return result
    if fallback:
        fb_result = _strip_to_latin(fallback)
        if fb_result:
            return fb_result
    return '???'


def safe_display_name(member_or_user) -> str:
    """Return a display_name that's safe for text / image rendering.

    Falls back to the underlying Discord username if display_name is empty or
    contains only non-Latin / non-renderable characters. Used for image and
    HTML stats-card rendering where an un-renderable nickname would otherwise
    become '???' or font-fallback tofu.
    """
    dn = getattr(member_or_user, "display_name", None) or ""
    name = getattr(member_or_user, "name", None) or ""
    if _strip_to_latin(dn):
        return dn
    return name or dn or "Unknown"


def _role_diversity_penalty(team_pids: List[int], role_prefs: Dict[int, Tuple[str, Optional[str]]]) -> int:
    """Penalty for role stacking on a team. Lower = better diversity.

    Uses quadratic scaling so stacking 3+ of the same role is heavily punished:
      2 of same role = 0, 3 = 1, 4 = 4, 5 = 9, 6 = 16
    Also gives a small bonus (-1) when a player's secondary role is represented
    on the team (encourages flex coverage).
    """
    counts: Dict[str, int] = {}
    for pid in team_pids:
        prefs = role_prefs.get(pid)
        role = prefs[0] if prefs else 'fill'
        counts[role] = counts.get(role, 0) + 1
    penalty = 0
    for role, count in counts.items():
        if role != 'fill':
            excess = max(0, count - 2)
            penalty += excess * excess  # quadratic: 1, 4, 9, 16...

    # Bonus: check if team has at least one of each core role via secondary prefs
    team_roles_covered = set(counts.keys()) - {'fill'}
    for pid in team_pids:
        prefs = role_prefs.get(pid)
        if prefs and prefs[1] and prefs[1] != 'fill' and prefs[1] not in team_roles_covered:
            # Secondary role not covered on team -- mild penalty
            penalty += 1
    return penalty


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


def normalize_ign(ign: str) -> str:
    """
    Normalize an IGN for matching purposes.
    - Lowercase
    - Remove spaces before #
    - Strip whitespace
    """
    ign = ign.strip().lower()
    # Remove space before # (common mistake)
    ign = ign.replace(' #', '#')
    return ign


def ign_similarity(ign1: str, ign2: str) -> float:
    """
    Calculate similarity between two IGNs (0.0 to 1.0).
    Uses character-level comparison accounting for common mistakes.
    """
    # Normalize both
    n1 = normalize_ign(ign1)
    n2 = normalize_ign(ign2)

    if n1 == n2:
        return 1.0

    # Split into name and tag
    if '#' not in n1 or '#' not in n2:
        return 0.0

    name1, tag1 = n1.split('#', 1)
    name2, tag2 = n2.split('#', 1)

    # Common character substitutions (visually similar)
    substitutions = {
        'l': '1iI|',
        'i': '1lI|',
        '1': 'liI|',
        'o': '0O',
        '0': 'oO',
        'O': 'o0',
        'I': 'l1i|',
        '|': 'l1iI',
        's': '5S$',
        '5': 'sS$',
        'S': 's5$',
    }

    def chars_similar(c1: str, c2: str) -> bool:
        if c1 == c2:
            return True
        # Check if c1 and c2 are commonly confused
        if c1 in substitutions and c2 in substitutions.get(c1, ''):
            return True
        if c2 in substitutions and c1 in substitutions.get(c2, ''):
            return True
        return False

    def sequence_similarity(s1: str, s2: str) -> float:
        if not s1 and not s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
        if len(s1) != len(s2):
            # Length mismatch - lower score
            return 0.0

        matches = sum(1 for c1, c2 in zip(s1, s2) if chars_similar(c1, c2))
        return matches / max(len(s1), len(s2))

    # Name and tag must both be similar
    name_sim = sequence_similarity(name1, name2)
    tag_sim = sequence_similarity(tag1, tag2)

    # Tags are usually short and must match exactly or very closely
    if tag_sim < 0.8:
        return 0.0

    # Weight name more heavily (70% name, 30% tag)
    return name_sim * 0.7 + tag_sim * 0.3


def find_best_ign_match(player_ign: str, available_igns: dict, threshold: float = 0.85) -> Optional[str]:
    """
    Find the best matching IGN from available options.

    Args:
        player_ign: The IGN we're trying to match
        available_igns: Dict of {normalized_ign: original_data}
        threshold: Minimum similarity score to consider a match

    Returns:
        The matching key from available_igns, or None if no good match
    """
    normalized_player = normalize_ign(player_ign)

    # First, try exact match with normalized IGN
    if normalized_player in available_igns:
        return normalized_player

    # Try case-insensitive exact match
    lower_player = normalized_player.lower()
    for available_key in available_igns.keys():
        if available_key.lower() == lower_player:
            logger.info(f"Case-insensitive IGN match: '{player_ign}' -> '{available_key}'")
            return available_key

    # Try stripped-special-chars match (remove non-alphanumeric except #)
    # Use NFKD normalization so diacritics (ã→a, š→s) decompose to ASCII
    def strip_special(s: str) -> str:
        ascii_s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
        return ''.join(c for c in ascii_s if c.isalnum() or c == '#').lower()

    stripped_player = strip_special(normalized_player)
    for available_key in available_igns.keys():
        if strip_special(available_key) == stripped_player:
            logger.info(f"Stripped-chars IGN match: '{player_ign}' -> '{available_key}'")
            return available_key

    # Try fuzzy matching
    best_match = None
    best_score = 0.0

    for available_key in available_igns.keys():
        score = ign_similarity(player_ign, available_key)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = available_key

    if best_match:
        logger.info(f"Fuzzy IGN match: '{player_ign}' -> '{best_match}' (score: {best_score:.2f})")

    return best_match


# Thresholds
PLACEMENT_GAMES = 10
LEARNING_GAMES = 20
RIVALRY_MIN_GAMES = 5
ROLE_MMR_TOLERANCE = 150  # Allow up to 150 MMR imbalance for better role diversity
SHAKE_MMR_TOLERANCE = 100  # Max MMR slack allowed for shake-up randomness
SHAKE_LOOKBACK_MATCHES = 20  # Recent matches to scan for previous team assignment
SHAKE_OVERLAP_THRESHOLD = 0.5  # Roster overlap required to treat as "same queue"

# Valid Rivals roles
RIVALS_ROLES = {"vanguard", "duelist", "strategist"}

# Rivals character roster for mirror match comp generation
RIVALS_ROSTER = {
    "Vanguard": [
        "Devil Dinosaur", "Venom", "Captain America", "Groot", "Doctor Strange",
        "Peni Parker", "The Thing", "Magneto", "Hulk (Bruce Banner)", "Thor",
        "Angela", "Emma Frost", "Rogue",
    ],
    "Strategist": [
        "Adam Warlock", "Cloak & Dagger", "Gambit", "Invisible Woman",
        "Jeff the Land Shark", "Loki", "Luna Snow", "Mantis",
        "Rocket Raccoon", "Ultron", "White Fox",
    ],
    "Duelist": [
        "Black Cat", "Black Panther", "Black Widow", "Blade", "Cyclops",
        "Daredevil", "Deadpool", "Elsa Bloodstone", "Hawkeye", "Hela",
        "Human Torch", "Iron Fist", "Iron Man", "Magik", "Mister Fantastic",
        "Moon Knight", "Namor", "Phoenix", "Psylocke", "Scarlet Witch",
        "Spider-Man", "Squirrel Girl", "Star-Lord", "Storm", "The Punisher",
        "Winter Soldier", "Wolverine",
    ],
}

# Colors (all white for consistent styling)
COLOR_WHITE = 0xFFFFFF
COLOR_RED = COLOR_WHITE
COLOR_BLUE = COLOR_WHITE
COLOR_NEUTRAL = COLOR_WHITE
COLOR_SUCCESS = COLOR_WHITE
COLOR_WARNING = COLOR_WHITE

# Stats card template paths
STATS_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "stats_card.html"
MATCH_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "match_card.html"
SCOREBOARD_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "scoreboard_card.html"
LEADERBOARD_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "leaderboard_card.html"
SERVERSTATS_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "serverstats_card.html"
SIMPLE_STATS_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "simple_stats_card.html"
RIVALS_RESULTS_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "rivals_results_card.html"
RIVALS_SERVERSTATS_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "rivals_serverstats_card.html"
RIVALS_STATS_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "rivals_stats_card.html"
H2H_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "h2h_card.html"
FONTS_PATH = Path(__file__).parent.parent.parent / "fonts"
H2H_BG_PATH = Path(__file__).parent.parent / "Images" / "custommatch" / "h2hbackground.png"

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

    queue_timeout_minutes: int = 180
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
    ign_required: bool = False
    role_required: bool = False
    category_id: Optional[int] = None  # Per-game category override (falls back to global)
    lf1_channel_id: Optional[int] = None  # Channel/thread to ping when queue is 1 player away
    grace_period_minutes: int = 10  # Per-player grace period for auto-ready
    not_ready_cooldown_minutes: int = 5  # Cooldown after clicking Not Ready
    decline_1st_minutes: int = 15  # 1st decline penalty duration
    decline_2nd_minutes: int = 60  # 2nd decline penalty duration
    decline_3rd_minutes: int = 1440  # 3rd+ decline penalty duration

    # Secondary queue configuration
    secondary_queue_enabled: bool = False
    secondary_queue_name: Optional[str] = None  # e.g., "Deathmatch", "2v2 Knife Only"
    secondary_queue_player_count: Optional[int] = None  # Independent from main queue
    secondary_queue_type: Optional[QueueType] = None  # Independent from main queue
    secondary_queue_channel_id: Optional[int] = None  # None = same as queue_channel_id
    secondary_schedule_times: Optional[Dict[str, Dict[str, str]]] = None  # Same format as schedule_times
    secondary_queue_match_limit: Optional[int] = None  # Max matches per window, None = unlimited
    secondary_banner_url: Optional[str] = None  # Banner image for secondary queue embeds

@dataclass
class SecondaryMode:
    mode_id: int
    game_id: int
    mode_name: str
    map_pool_type: str = 'none'  # 'none', 'standard', 'custom'
    custom_maps: Optional[List[str]] = None
    description: Optional[str] = None
    is_ffa: bool = False
    is_mirror: bool = False
    display_order: int = 0

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
    returning_games_remaining: int = 0
    is_new: bool = False  # True only when no DB row existed -- not persisted

    @property
    def effective_mmr(self) -> int:
        return self.mmr + self.admin_offset

    def get_k_factor(self) -> int:
        """Get K-factor based on games played, inactivity, and returning player status."""
        # Returning player boost (multi-game elevated K after long absence)
        if self.returning_games_remaining > 0:
            return K_FACTOR_PLACEMENT

        base_k = K_FACTOR_STABLE
        if self.games_played <= PLACEMENT_GAMES:
            base_k = K_FACTOR_PLACEMENT
        elif self.games_played <= LEARNING_GAMES:
            base_k = K_FACTOR_LEARNING

        # Inactivity fallback (first game back before returning_games_remaining is set)
        if self.last_played:
            days_inactive = (datetime.now(timezone.utc) - self.last_played).days
            if days_inactive >= 42:  # 6+ weeks
                return K_FACTOR_PLACEMENT
            elif days_inactive >= 21:  # 3-6 weeks — bump up one tier for 1 game
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
    grace_timers: Dict[int, datetime] = field(default_factory=dict)  # player_id -> join_timestamp (UTC)
    auto_readied: set = field(default_factory=set)  # player_ids that were auto-readied via grace period
    is_secondary: bool = False  # True if this is a secondary/fun-mode queue

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
