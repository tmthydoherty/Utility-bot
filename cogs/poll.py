
import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import re
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from pathlib import Path

logger = logging.getLogger('ranked_poll')

# =============================================================================
# CONSTANTS
# =============================================================================

DB_PATH = Path("data/ranked_polls.db")
MAX_OPTIONS = 10
MIN_DURATION_MINUTES = 3
MAX_DURATION_MINUTES = 7 * 24 * 60  # 7 days

EMBED_COLOR_ACTIVE = discord.Color.from_rgb(88, 101, 242)  # Blurple
EMBED_COLOR_CLOSED = discord.Color.from_rgb(87, 242, 135)  # Green
EMBED_COLOR_CANCELLED = discord.Color.from_rgb(237, 66, 69)  # Red

RANK_EMOJIS = ["\U0001f947", "\U0001f948", "\U0001f949", "4\ufe0f\u20e3", "5\ufe0f\u20e3", "6\ufe0f\u20e3", "7\ufe0f\u20e3", "8\ufe0f\u20e3", "9\ufe0f\u20e3", "\U0001f51f"]
MEDAL_EMOJIS = ["\U0001f3c6", "\U0001f948", "\U0001f949"]

# =============================================================================
# DURATION PARSER
# =============================================================================

def parse_duration(duration_str: str) -> Optional[int]:
    """Parse a human-friendly duration string into minutes."""
    duration_str = duration_str.lower().strip()
    pattern = r'(\d+)\s*(m(?:in(?:ute)?s?)?|h(?:(?:ou)?rs?)?|d(?:ays?)?)'
    matches = re.findall(pattern, duration_str)

    if not matches:
        try:
            minutes = int(duration_str)
            if MIN_DURATION_MINUTES <= minutes <= MAX_DURATION_MINUTES:
                return minutes
        except ValueError:
            pass
        return None

    total_minutes = 0
    for value, unit in matches:
        value = int(value)
        unit = unit[0]
        if unit == 'm':
            total_minutes += value
        elif unit == 'h':
            total_minutes += value * 60
        elif unit == 'd':
            total_minutes += value * 24 * 60

    if MIN_DURATION_MINUTES <= total_minutes <= MAX_DURATION_MINUTES:
        return total_minutes
    return None


def format_duration(minutes: int) -> str:
    """Format minutes into a human-readable string."""
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif minutes < 24 * 60:
        hours = minutes // 60
        mins = minutes % 60
        parts = [f"{hours} hour{'s' if hours != 1 else ''}"]
        if mins:
            parts.append(f"{mins} minute{'s' if mins != 1 else ''}")
        return " ".join(parts)
    else:
        days = minutes // (24 * 60)
        remaining = minutes % (24 * 60)
        hours = remaining // 60
        parts = [f"{days} day{'s' if days != 1 else ''}"]
        if hours:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        return " ".join(parts)


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

async def init_database():
    """Initialize the database with required tables."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS polls (
                poll_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER,
                creator_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                options TEXT NOT NULL,
                minimum_ranks INTEGER NOT NULL DEFAULT 1,
                allowed_role_id INTEGER,
                created_at TEXT NOT NULL,
                ends_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                is_cancelled INTEGER NOT NULL DEFAULT 0
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS poll_votes (
                vote_id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                rankings TEXT NOT NULL,
                voted_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (poll_id) REFERENCES polls(poll_id),
                UNIQUE(poll_id, user_id)
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_polls_guild ON polls(guild_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_polls_active ON polls(is_active)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_votes_poll ON poll_votes(poll_id)")
        await db.commit()


async def create_poll(guild_id: int, channel_id: int, creator_id: int, title: str,
                      options: List[str], duration_minutes: int, minimum_ranks: int = 1,
                      description: str = None, allowed_role_id: int = None) -> int:
    """Create a new poll and return its ID."""
    now = datetime.now(timezone.utc)
    ends_at = now + timedelta(minutes=duration_minutes)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO polls (guild_id, channel_id, creator_id, title, description,
                options, minimum_ranks, allowed_role_id, created_at, ends_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (guild_id, channel_id, creator_id, title, description,
              json.dumps(options), minimum_ranks, allowed_role_id,
              now.isoformat(), ends_at.isoformat()))
        await db.commit()
        return cursor.lastrowid


async def update_poll_message_id(poll_id: int, message_id: int):
    """Update the message ID for a poll after posting."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE polls SET message_id = ? WHERE poll_id = ?", (message_id, poll_id))
        await db.commit()


async def get_poll(poll_id: int) -> Optional[Dict[str, Any]]:
    """Get a poll by ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM polls WHERE poll_id = ?", (poll_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_active_polls(guild_id: int = None) -> List[Dict[str, Any]]:
    """Get all active polls, optionally filtered by guild."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if guild_id:
            cursor = await db.execute(
                "SELECT * FROM polls WHERE is_active = 1 AND guild_id = ? ORDER BY ends_at ASC",
                (guild_id,))
        else:
            cursor = await db.execute("SELECT * FROM polls WHERE is_active = 1 ORDER BY ends_at ASC")
        return [dict(row) for row in await cursor.fetchall()]


async def close_poll(poll_id: int, cancelled: bool = False):
    """Close a poll (either naturally or cancelled)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE polls SET is_active = 0, is_cancelled = ? WHERE poll_id = ?",
                        (1 if cancelled else 0, poll_id))
        await db.commit()


async def save_vote(poll_id: int, user_id: int, rankings: List[int]):
    """Save or update a user's vote."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT vote_id FROM poll_votes WHERE poll_id = ? AND user_id = ?",
            (poll_id, user_id))
        existing = await cursor.fetchone()

        if existing:
            await db.execute(
                "UPDATE poll_votes SET rankings = ?, updated_at = ? WHERE poll_id = ? AND user_id = ?",
                (json.dumps(rankings), now, poll_id, user_id))
        else:
            await db.execute(
                "INSERT INTO poll_votes (poll_id, user_id, rankings, voted_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (poll_id, user_id, json.dumps(rankings), now, now))
        await db.commit()


async def get_user_vote(poll_id: int, user_id: int) -> Optional[List[int]]:
    """Get a user's vote for a poll."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT rankings FROM poll_votes WHERE poll_id = ? AND user_id = ?",
            (poll_id, user_id))
        row = await cursor.fetchone()
        return json.loads(row[0]) if row else None


async def get_all_votes(poll_id: int) -> List[Dict[str, Any]]:
    """Get all votes for a poll."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT user_id, rankings FROM poll_votes WHERE poll_id = ?", (poll_id,))
        return [{"user_id": row["user_id"], "rankings": json.loads(row["rankings"])} for row in await cursor.fetchall()]


async def get_vote_count(poll_id: int) -> int:
    """Get the number of votes for a poll."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM poll_votes WHERE poll_id = ?", (poll_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0


# =============================================================================
# BORDA COUNT CALCULATION
# =============================================================================

def calculate_borda_results(votes: List[Dict], num_options: int) -> Dict[str, Any]:
    """Calculate Borda count results."""
    scores = [0] * num_options
    rank_distribution = [[0] * num_options for _ in range(num_options)]

    for vote in votes:
        rankings = vote["rankings"]
        for rank, option_idx in enumerate(rankings):
            points = num_options - rank
            scores[option_idx] += points
            rank_distribution[option_idx][rank] += 1

    max_possible = num_options * len(votes) if votes else 1
    sorted_options = sorted(range(num_options), key=lambda i: scores[i], reverse=True)

    return {
        "scores": scores,
        "rank_distribution": rank_distribution,
        "sorted_options": sorted_options,
        "max_possible": max_possible,
        "total_voters": len(votes)
    }


# =============================================================================
# EMBED BUILDERS
# =============================================================================

def build_active_poll_embed(poll: Dict, vote_count: int) -> discord.Embed:
    """Build the embed for an active poll."""
    options = json.loads(poll["options"])
    ends_at = datetime.fromisoformat(poll["ends_at"])
    ends_timestamp = int(ends_at.timestamp())

    embed = discord.Embed(title=f"\U0001f5f3\ufe0f {poll['title']}", color=EMBED_COLOR_ACTIVE)

    if poll["description"]:
        embed.description = poll["description"]

    options_text = "\n".join(f"\u3000\u2022 {opt}" for opt in options)
    embed.add_field(name=f"\U0001f4cb Options ({len(options)})", value=options_text, inline=False)

    min_ranks = poll["minimum_ranks"]
    if min_ranks < len(options):
        rank_info = f"Rank at least **{min_ranks}** option{'s' if min_ranks != 1 else ''} (rest optional)"
    else:
        rank_info = f"Rank all **{len(options)}** options"

    embed.add_field(name="\U0001f4ca How to Vote", value=rank_info, inline=False)

    status_parts = [
        f"\U0001f465 **{vote_count}** vote{'s' if vote_count != 1 else ''} so far",
        f"\u23f0 Closes <t:{ends_timestamp}:R>"
    ]
    if poll["allowed_role_id"]:
        status_parts.append(f"\U0001f512 <@&{poll['allowed_role_id']}> only")

    embed.add_field(name="Status", value=" \u2502 ".join(status_parts), inline=False)
    embed.set_footer(text=f"Poll ID: {poll['poll_id']} \u2022 Ranked Choice (Borda Count)")
    return embed


def build_results_embed(poll: Dict, results: Dict, options: List[str]) -> discord.Embed:
    """Build the results embed for a closed poll."""
    embed = discord.Embed(title=f"\U0001f3c6 Results: {poll['title']}", color=EMBED_COLOR_CLOSED)

    if poll["description"]:
        embed.description = poll["description"]

    results_lines = []
    for rank, option_idx in enumerate(results["sorted_options"]):
        score = results["scores"][option_idx]
        max_score = results["max_possible"]
        percentage = (score / max_score * 100) if max_score > 0 else 0
        filled = int(percentage / 5)
        bar = "\u2588" * filled + "\u2591" * (20 - filled)
        emoji = MEDAL_EMOJIS[rank] if rank < 3 else f"**{rank + 1}.**"
        results_lines.append(f"{emoji} **{options[option_idx]}** \u2014 {score:,} pts\n\u3000{bar} {percentage:.0f}%")

    embed.add_field(name="Final Standings", value="\n\n".join(results_lines), inline=False)
    embed.add_field(
        name="\u2501" * 27,
        value=f"\U0001f465 **{results['total_voters']}** voter{'s' if results['total_voters'] != 1 else ''} participated\n\U0001f4ca Method: Borda Count",
        inline=False)
    embed.set_footer(text=f"Poll ID: {poll['poll_id']} \u2022 Poll Closed")
    return embed


def build_breakdown_embed(poll: Dict, results: Dict, options: List[str]) -> discord.Embed:
    """Build a detailed breakdown embed showing rank distribution."""
    embed = discord.Embed(title=f"\U0001f4ca Detailed Breakdown: {poll['title']}", color=EMBED_COLOR_CLOSED)

    for option_idx in results["sorted_options"]:
        dist = results["rank_distribution"][option_idx]
        dist_parts = []
        for rank, count in enumerate(dist):
            if count > 0:
                emoji = RANK_EMOJIS[rank] if rank < len(RANK_EMOJIS) else f"#{rank+1}"
                dist_parts.append(f"{emoji} \u00d7{count}")
        dist_text = " \u2502 ".join(dist_parts) if dist_parts else "No votes"
        embed.add_field(name=f"{options[option_idx]} ({results['scores'][option_idx]} pts)", value=dist_text, inline=False)

    embed.set_footer(text=f"Poll ID: {poll['poll_id']}")
    return embed


def build_cancelled_embed(poll: Dict) -> discord.Embed:
    """Build the embed for a cancelled poll."""
    embed = discord.Embed(
        title=f"\u274c Cancelled: {poll['title']}",
        description="This poll was cancelled by an administrator.",
        color=EMBED_COLOR_CANCELLED)
    embed.set_footer(text=f"Poll ID: {poll['poll_id']}")
    return embed


# =============================================================================
# VOTING SESSION MANAGER
# =============================================================================

class VotingSession:
    """Manages an active voting session for a user."""

    def __init__(self, poll: Dict, user_id: int, existing_rankings: List[int] = None):
        self.poll = poll
        self.user_id = user_id
        self.options = json.loads(poll["options"])
        self.minimum_ranks = poll["minimum_ranks"]
        self.rankings = existing_rankings.copy() if existing_rankings else []
        self.message: Optional[discord.Message] = None

    @property
    def current_rank(self) -> int:
        return len(self.rankings) + 1

    @property
    def is_complete(self) -> bool:
        return len(self.rankings) == len(self.options)

    @property
    def can_submit(self) -> bool:
        return len(self.rankings) >= self.minimum_ranks

    @property
    def remaining_options(self) -> List[tuple]:
        return [(i, opt) for i, opt in enumerate(self.options) if i not in self.rankings]

    def add_ranking(self, option_index: int):
        if option_index not in self.rankings:
            self.rankings.append(option_index)

    def reset(self):
        self.rankings = []

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(title=f"\U0001f5f3\ufe0f Vote: {self.poll['title']}", color=EMBED_COLOR_ACTIVE)

        if self.rankings:
            ranking_lines = []
            for rank, opt_idx in enumerate(self.rankings):
                emoji = RANK_EMOJIS[rank] if rank < len(RANK_EMOJIS) else f"#{rank+1}"
                ranking_lines.append(f"{emoji} {self.options[opt_idx]}")
            embed.add_field(name="Your Rankings So Far", value="\n".join(ranking_lines), inline=False)

        if self.is_complete:
            embed.add_field(name="\u2705 All Options Ranked!", value="Review your rankings and click **Submit** to confirm.", inline=False)
        elif self.can_submit:
            remaining = len(self.options) - len(self.rankings)
            embed.add_field(name=f"Select your #{self.current_rank} pick (optional)",
                          value=f"You can **Submit** now or continue ranking the remaining {remaining} option{'s' if remaining != 1 else ''}.", inline=False)
        else:
            needed = self.minimum_ranks - len(self.rankings)
            embed.add_field(name=f"Select your #{self.current_rank} pick",
                          value=f"Rank {needed} more option{'s' if needed != 1 else ''} to submit.", inline=False)

        embed.set_footer(text="This is only visible to you \u2022 Your vote is private until you submit")
        return embed


# =============================================================================
# VOTING VIEWS (User-facing)
# =============================================================================

class PollView(discord.ui.View):
    """Main poll view with Vote and View Results buttons."""

    def __init__(self, poll_id: int):
        super().__init__(timeout=None)
        self.poll_id = poll_id

    @discord.ui.button(label="Vote", style=discord.ButtonStyle.primary, emoji="\U0001f5f3\ufe0f", custom_id="poll:vote")
    async def vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        poll = await get_poll(self.poll_id)

        if not poll or not poll["is_active"]:
            return await interaction.response.send_message("\u274c This poll is no longer active.", ephemeral=True)

        if poll["allowed_role_id"]:
            member = interaction.guild.get_member(interaction.user.id)
            if not any(r.id == poll["allowed_role_id"] for r in member.roles):
                return await interaction.response.send_message(
                    f"\u274c Only members with <@&{poll['allowed_role_id']}> can vote in this poll.", ephemeral=True)

        existing_vote = await get_user_vote(self.poll_id, interaction.user.id)
        session = VotingSession(poll, interaction.user.id, existing_vote)
        view = VotingView(session)
        await interaction.response.send_message(embed=session.build_embed(), view=view, ephemeral=True)
        session.message = await interaction.original_response()

    @discord.ui.button(label="My Vote", style=discord.ButtonStyle.secondary, emoji="\U0001f4cb", custom_id="poll:myvote")
    async def my_vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        poll = await get_poll(self.poll_id)
        if not poll:
            return await interaction.response.send_message("\u274c Poll not found.", ephemeral=True)

        existing_vote = await get_user_vote(self.poll_id, interaction.user.id)
        if not existing_vote:
            return await interaction.response.send_message(
                "\U0001f4cb You haven't voted in this poll yet.\nClick **Vote** to participate!", ephemeral=True)

        options = json.loads(poll["options"])
        embed = discord.Embed(title=f"\U0001f4cb Your Vote: {poll['title']}", color=EMBED_COLOR_ACTIVE)

        ranking_lines = []
        for rank, opt_idx in enumerate(existing_vote):
            emoji = RANK_EMOJIS[rank] if rank < len(RANK_EMOJIS) else f"#{rank+1}"
            ranking_lines.append(f"{emoji} {options[opt_idx]}")

        embed.add_field(name="Your Rankings", value="\n".join(ranking_lines), inline=False)
        if poll["is_active"]:
            embed.set_footer(text="Click Vote to change your rankings")

        await interaction.response.send_message(embed=embed, ephemeral=True)


class VotingView(discord.ui.View):
    """Ephemeral view for the step-by-step voting process."""

    def __init__(self, session: VotingSession):
        super().__init__(timeout=300)
        self.session = session
        self._update_components()

    def _update_components(self):
        self.clear_items()

        if not self.session.is_complete:
            self.add_item(RankingSelect(self.session))

        if self.session.can_submit:
            submit_btn = discord.ui.Button(label="Submit Vote", style=discord.ButtonStyle.success, emoji="\u2705")
            submit_btn.callback = self.submit_callback
            self.add_item(submit_btn)

        if self.session.rankings:
            reset_btn = discord.ui.Button(label="Start Over", style=discord.ButtonStyle.danger, emoji="\U0001f504")
            reset_btn.callback = self.reset_callback
            self.add_item(reset_btn)

        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="\u274c")
        cancel_btn.callback = self.cancel_callback
        self.add_item(cancel_btn)

    async def submit_callback(self, interaction: discord.Interaction):
        poll = await get_poll(self.session.poll["poll_id"])
        if not poll or not poll["is_active"]:
            return await interaction.response.edit_message(content="\u274c This poll has closed.", embed=None, view=None)

        await save_vote(self.session.poll["poll_id"], self.session.user_id, self.session.rankings)

        options = self.session.options
        ranking_lines = []
        for rank, opt_idx in enumerate(self.session.rankings):
            emoji = RANK_EMOJIS[rank] if rank < len(RANK_EMOJIS) else f"#{rank+1}"
            ranking_lines.append(f"{emoji} {options[opt_idx]}")

        embed = discord.Embed(title="\u2705 Vote Submitted!",
                             description=f"Your ranking for **{self.session.poll['title']}** has been recorded.",
                             color=EMBED_COLOR_CLOSED)
        embed.add_field(name="Your Final Rankings", value="\n".join(ranking_lines), inline=False)
        embed.set_footer(text="You can change your vote anytime before the poll closes")

        await interaction.response.edit_message(embed=embed, view=None)

        try:
            channel = interaction.guild.get_channel(poll["channel_id"])
            if channel and poll["message_id"]:
                message = await channel.fetch_message(poll["message_id"])
                vote_count = await get_vote_count(poll["poll_id"])
                await message.edit(embed=build_active_poll_embed(poll, vote_count))
        except Exception as e:
            logger.warning(f"Failed to update poll message: {e}")

    async def reset_callback(self, interaction: discord.Interaction):
        self.session.reset()
        self._update_components()
        await interaction.response.edit_message(embed=self.session.build_embed(), view=self)

    async def cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="\U0001f6ab Voting cancelled. Your previous vote (if any) remains unchanged.",
            embed=None, view=None)

    async def on_timeout(self):
        try:
            if self.session.message:
                await self.session.message.edit(
                    content="\u23f0 Voting session timed out. Click **Vote** on the poll to try again.",
                    embed=None, view=None)
        except:
            pass


class RankingSelect(discord.ui.Select):
    """Select menu for choosing the next ranked option."""

    def __init__(self, session: VotingSession):
        self.session = session
        options = [discord.SelectOption(label=opt_name[:100], value=str(idx), description=f"Rank as #{session.current_rank}")
                   for idx, opt_name in session.remaining_options]

        placeholder = f"Select your #{session.current_rank} choice"
        if session.can_submit and not session.is_complete:
            placeholder += " (optional)"

        super().__init__(placeholder=placeholder, options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        self.session.add_ranking(int(self.values[0]))
        self.view._update_components()
        await interaction.response.edit_message(embed=self.session.build_embed(), view=self.view)


class ResultsView(discord.ui.View):
    """View for closed polls with breakdown button."""

    def __init__(self, poll_id: int):
        super().__init__(timeout=None)
        self.poll_id = poll_id

    @discord.ui.button(label="View Detailed Breakdown", style=discord.ButtonStyle.secondary, emoji="\U0001f4ca", custom_id="poll:breakdown")
    async def breakdown_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        poll = await get_poll(self.poll_id)
        if not poll:
            return await interaction.response.send_message("\u274c Poll not found.", ephemeral=True)

        votes = await get_all_votes(self.poll_id)
        options = json.loads(poll["options"])
        results = calculate_borda_results(votes, len(options))
        await interaction.response.send_message(embed=build_breakdown_embed(poll, results, options), ephemeral=True)


# =============================================================================
# ADMIN PANEL MODALS
# =============================================================================

class CreatePollModal(discord.ui.Modal, title="Create New Poll"):
    """Modal for creating a new poll."""

    def __init__(self, cog: "RankedPoll"):
        super().__init__(timeout=300)
        self.cog = cog

    poll_title = discord.ui.TextInput(label="Poll Title", placeholder="What should we decide?", max_length=200)
    options = discord.ui.TextInput(label="Options (comma-separated)", placeholder="Option 1, Option 2, Option 3",
                                   style=discord.TextStyle.paragraph, max_length=1000)
    duration = discord.ui.TextInput(label="Duration", placeholder="e.g., 30m, 2h, 1d, 7d", default="1d", max_length=20)
    min_ranks = discord.ui.TextInput(label="Minimum Ranks Required", placeholder="1", default="1", max_length=2, required=False)
    description = discord.ui.TextInput(label="Description (optional)", placeholder="Additional context...",
                                       style=discord.TextStyle.paragraph, required=False, max_length=500)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        option_list = [opt.strip() for opt in self.options.value.split(",") if opt.strip()]

        if len(option_list) < 2:
            return await interaction.followup.send("\u274c You need at least 2 options.", ephemeral=True)
        if len(option_list) > MAX_OPTIONS:
            return await interaction.followup.send(f"\u274c Maximum {MAX_OPTIONS} options allowed.", ephemeral=True)
        if len(option_list) != len(set(option_list)):
            return await interaction.followup.send("\u274c Duplicate options are not allowed.", ephemeral=True)

        duration_minutes = parse_duration(self.duration.value)
        if duration_minutes is None:
            return await interaction.followup.send(
                f"\u274c Invalid duration. Use formats like `30m`, `2h`, `1d 12h`, `7d`.\nMinimum: 3 minutes, Maximum: 7 days.",
                ephemeral=True)

        try:
            minimum_ranks = int(self.min_ranks.value or "1")
            if minimum_ranks < 1 or minimum_ranks > len(option_list):
                return await interaction.followup.send(
                    f"\u274c Minimum ranks must be between 1 and {len(option_list)}.", ephemeral=True)
        except ValueError:
            return await interaction.followup.send("\u274c Minimum ranks must be a number.", ephemeral=True)

        poll_id = await create_poll(
            guild_id=interaction.guild.id,
            channel_id=interaction.channel.id,
            creator_id=interaction.user.id,
            title=self.poll_title.value,
            options=option_list,
            duration_minutes=duration_minutes,
            minimum_ranks=minimum_ranks,
            description=self.description.value or None)

        poll = await get_poll(poll_id)
        view = PollView(poll_id)
        embed = build_active_poll_embed(poll, 0)

        message = await interaction.channel.send(embed=embed, view=view)
        await update_poll_message_id(poll_id, message.id)

        self.cog.active_views[poll_id] = view
        self.cog.bot.add_view(view, message_id=message.id)

        await interaction.followup.send(f"\u2705 Poll **{self.poll_title.value}** created! (ID: {poll_id})", ephemeral=True)
        logger.info(f"Created poll {poll_id}: {self.poll_title.value} by {interaction.user}")


class PollActionModal(discord.ui.Modal):
    """Modal for close/cancel/info actions that need a poll ID."""

    def __init__(self, cog: "RankedPoll", action: str):
        self.cog = cog
        self.action = action
        title_map = {"close": "Close Poll Early", "cancel": "Cancel Poll", "info": "View Poll Info"}
        super().__init__(title=title_map.get(action, "Poll Action"), timeout=180)

    poll_id = discord.ui.TextInput(label="Poll ID", placeholder="Enter the poll ID number")

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            poll_id = int(self.poll_id.value)
        except ValueError:
            return await interaction.followup.send("\u274c Invalid poll ID.", ephemeral=True)

        poll = await get_poll(poll_id)
        if not poll:
            return await interaction.followup.send("\u274c Poll not found.", ephemeral=True)
        if poll["guild_id"] != interaction.guild.id:
            return await interaction.followup.send("\u274c Poll not found in this server.", ephemeral=True)

        if self.action == "info":
            options = json.loads(poll["options"])
            vote_count = await get_vote_count(poll_id)
            created_at = datetime.fromisoformat(poll["created_at"])
            ends_at = datetime.fromisoformat(poll["ends_at"])

            status = "Active" if poll["is_active"] else ("Cancelled" if poll["is_cancelled"] else "Closed")
            color = EMBED_COLOR_ACTIVE if poll["is_active"] else (EMBED_COLOR_CANCELLED if poll["is_cancelled"] else EMBED_COLOR_CLOSED)

            embed = discord.Embed(title=f"Poll Info: {poll['title']}", color=color)
            if poll["description"]:
                embed.description = poll["description"]

            embed.add_field(name="Status", value=status, inline=True)
            embed.add_field(name="Votes", value=str(vote_count), inline=True)
            embed.add_field(name="Options", value=str(len(options)), inline=True)
            embed.add_field(name="Min Ranks", value=str(poll["minimum_ranks"]), inline=True)
            embed.add_field(name="Created", value=f"<t:{int(created_at.timestamp())}:f>", inline=True)
            embed.add_field(name="Ends", value=f"<t:{int(ends_at.timestamp())}:f>", inline=True)
            embed.add_field(name="Creator", value=f"<@{poll['creator_id']}>", inline=True)
            embed.add_field(name="Channel", value=f"<#{poll['channel_id']}>", inline=True)
            if poll["allowed_role_id"]:
                embed.add_field(name="Restricted To", value=f"<@&{poll['allowed_role_id']}>", inline=True)
            embed.set_footer(text=f"Poll ID: {poll_id}")

            return await interaction.followup.send(embed=embed, ephemeral=True)

        if not poll["is_active"]:
            return await interaction.followup.send("\u274c This poll is already closed.", ephemeral=True)

        if self.action == "close":
            await self.cog._close_poll(poll)
            await interaction.followup.send(f"\u2705 Poll **{poll['title']}** has been closed and results posted.", ephemeral=True)

        elif self.action == "cancel":
            await close_poll(poll_id, cancelled=True)
            try:
                channel = interaction.guild.get_channel(poll["channel_id"])
                if channel and poll["message_id"]:
                    message = await channel.fetch_message(poll["message_id"])
                    await message.edit(embed=build_cancelled_embed(poll), view=None)
            except Exception as e:
                logger.warning(f"Failed to update cancelled poll message: {e}")

            self.cog.active_views.pop(poll_id, None)
            await interaction.followup.send(f"\u2705 Poll **{poll['title']}** has been cancelled.", ephemeral=True)
            logger.info(f"Cancelled poll {poll_id}: {poll['title']} by {interaction.user}")


# =============================================================================
# ADMIN PANEL VIEW
# =============================================================================

class AdminPanelView(discord.ui.View):
    """Admin panel for managing polls."""

    def __init__(self, cog: "RankedPoll"):
        super().__init__(timeout=180)
        self.cog = cog

    @discord.ui.button(label="Create Poll", style=discord.ButtonStyle.success, emoji="\u2795", row=0)
    async def create_poll(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CreatePollModal(self.cog))

    @discord.ui.button(label="Close Poll", style=discord.ButtonStyle.primary, emoji="\U0001f3c1", row=0)
    async def close_poll(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PollActionModal(self.cog, "close"))

    @discord.ui.button(label="Cancel Poll", style=discord.ButtonStyle.danger, emoji="\u274c", row=0)
    async def cancel_poll(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PollActionModal(self.cog, "cancel"))

    @discord.ui.button(label="Poll Info", style=discord.ButtonStyle.secondary, emoji="\U0001f4cb", row=1)
    async def poll_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PollActionModal(self.cog, "info"))

    @discord.ui.button(label="List Active Polls", style=discord.ButtonStyle.secondary, emoji="\U0001f4dc", row=1)
    async def list_polls(self, interaction: discord.Interaction, button: discord.ui.Button):
        polls = await get_active_polls(interaction.guild.id)

        if not polls:
            return await interaction.response.send_message("\U0001f4ed No active polls in this server.", ephemeral=True)

        embed = discord.Embed(title="\U0001f5f3\ufe0f Active Polls", color=EMBED_COLOR_ACTIVE)

        for poll in polls[:10]:
            ends_at = datetime.fromisoformat(poll["ends_at"])
            vote_count = await get_vote_count(poll["poll_id"])
            embed.add_field(
                name=f"#{poll['poll_id']}: {poll['title']}",
                value=f"\U0001f465 {vote_count} votes \u2022 \u23f0 Ends <t:{int(ends_at.timestamp())}:R>\n\U0001f4cd <#{poll['channel_id']}>",
                inline=False)

        if len(polls) > 10:
            embed.set_footer(text=f"Showing 10 of {len(polls)} active polls")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


# =============================================================================
# COG
# =============================================================================

class RankedPoll(commands.Cog):
    """Ranked Choice Voting System with Borda Count scoring."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.active_views: Dict[int, PollView] = {}

    async def cog_load(self):
        await init_database()
        await self._restore_views()
        self.check_polls.start()
        logger.info("RankedPoll cog loaded successfully")

    async def cog_unload(self):
        self.check_polls.cancel()

    async def _restore_views(self):
        """Restore persistent views for active polls after bot restart."""
        polls = await get_active_polls()
        for poll in polls:
            view = PollView(poll["poll_id"])
            self.bot.add_view(view, message_id=poll["message_id"])
            self.active_views[poll["poll_id"]] = view
        logger.info(f"Restored {len(polls)} active poll views")

    def _is_admin(self, member: discord.Member) -> bool:
        """Check if member is a bot admin."""
        if hasattr(self.bot, 'is_bot_admin'):
            return self.bot.is_bot_admin(member)
        return member.guild_permissions.administrator

    # =========================================================================
    # TASKS
    # =========================================================================

    @tasks.loop(seconds=30)
    async def check_polls(self):
        """Check for polls that need to be closed."""
        try:
            polls = await get_active_polls()
            now = datetime.now(timezone.utc)
            for poll in polls:
                ends_at = datetime.fromisoformat(poll["ends_at"])
                if now >= ends_at:
                    await self._close_poll(poll)
        except Exception as e:
            logger.error(f"Error in check_polls task: {e}")

    @check_polls.before_loop
    async def before_check_polls(self):
        await self.bot.wait_until_ready()

    async def _close_poll(self, poll: Dict):
        """Close a poll and post results."""
        poll_id = poll["poll_id"]
        await close_poll(poll_id, cancelled=False)

        votes = await get_all_votes(poll_id)
        options = json.loads(poll["options"])
        results = calculate_borda_results(votes, len(options))

        try:
            guild = self.bot.get_guild(poll["guild_id"])
            if guild:
                channel = guild.get_channel(poll["channel_id"])
                if channel and poll["message_id"]:
                    message = await channel.fetch_message(poll["message_id"])
                    await message.edit(embed=build_results_embed(poll, results, options), view=ResultsView(poll_id))
        except Exception as e:
            logger.error(f"Error closing poll {poll_id}: {e}")

        self.active_views.pop(poll_id, None)
        logger.info(f"Closed poll {poll_id}: {poll['title']}")

    # =========================================================================
    # COMMAND
    # =========================================================================

    @app_commands.command(name="poll", description="Open the poll administration panel")
    @app_commands.guild_only()
    async def poll_panel(self, interaction: discord.Interaction):
        """Open the poll admin panel."""
        if not self._is_admin(interaction.user):
            return await interaction.response.send_message(
                "\u274c You need administrator permissions to manage polls.", ephemeral=True)

        embed = discord.Embed(
            title="\U0001f5f3\ufe0f Poll Admin Panel",
            description="Create and manage ranked choice polls using Borda Count voting.",
            color=EMBED_COLOR_ACTIVE)
        embed.add_field(name="How It Works",
                       value="\u2022 Create polls with 2-10 options\n\u2022 Users rank their choices\n\u2022 Borda Count calculates the winner\n\u2022 Polls auto-close at scheduled time",
                       inline=False)

        await interaction.response.send_message(embed=embed, view=AdminPanelView(self), ephemeral=True)


async def setup(bot: commands.Bot):
    """Setup function for loading the cog."""
    await bot.add_cog(RankedPoll(bot))
