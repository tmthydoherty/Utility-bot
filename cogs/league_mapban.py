import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import random
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple, Any
import traceback
import re
from io import BytesIO
from pathlib import Path
import aiohttp

# Pillow for image stitching
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("⚠️ Pillow not installed. Final result images will not be generated.")


# =============================================================================
# CONSTANTS
# =============================================================================

DATABASE_PATH = "data/mapban.db"

# Embed colors
COLOR_PRIMARY = 0x5865F2      # Blurple
COLOR_SUCCESS = 0x57F287      # Green
COLOR_DANGER = 0xED4245       # Red
COLOR_WARNING = 0xFEE75C      # Yellow
COLOR_NEUTRAL = 0x99AAB5      # Grey
COLOR_PROTECT = 0x57F287      # Green

# Default settings
DEFAULT_TURN_TIMEOUT = 120    # 2 minutes in seconds
DEFAULT_READY_TIMEOUT = 300   # 5 minutes in seconds
REMINDER_TIME = 60            # Remind at 1 minute remaining
TIMER_UPDATE_INTERVAL = 10    # Update timer every 10 seconds
THREAD_AUTO_DELETE = 10800    # 3 hours in seconds
SESSION_DATA_RETENTION = 604800  # 1 week in seconds

# Progress bar characters
PROGRESS_FILLED = "█"
PROGRESS_EMPTY = "░"
PROGRESS_BAR_LENGTH = 10

# Button custom_id prefixes
PREFIX_MAP_SELECT = "mapban_map_"
PREFIX_SIDE_SELECT = "mapban_side_"
PREFIX_READY = "mapban_ready_"
PREFIX_CONFIRM = "mapban_confirm_"
PREFIX_CANCEL = "mapban_cancel_"
PREFIX_AGENT_SELECT = "mapban_agent_"

# Agent roles for Valorant
AGENT_ROLES = {
    "Duelist": ["Iso", "Jett", "Neon", "Phoenix", "Raze", "Reyna", "Waylay", "Yoru"],
    "Controller": ["Astra", "Brimstone", "Clove", "Harbor", "Miks", "Omen", "Viper"],
    "Sentinel": ["Chamber", "Cypher", "Deadlock", "Killjoy", "Sage", "Veto", "Vyse"],
    "Initiator": ["Breach", "Fade", "Gekko", "KAY/O", "Skye", "Sova", "Tejo"],
}


# =============================================================================
# PERSISTENT VIEWS (work after bot restart)
# =============================================================================

# PersistentReadyView removed - all button interactions now handled via on_interaction listener
# to avoid custom_id conflicts when the same static ID is used across multiple threads.


class PersistentMapSelectView(discord.ui.View):
    """Persistent view for map selection - parses session from custom_id."""

    def __init__(self, cog: "MapBanCog" = None):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="placeholder", style=discord.ButtonStyle.primary, custom_id="mapban_map_placeholder")
    async def map_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """This is a placeholder - actual buttons are created dynamically."""
        pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Handle any map selection button."""
        if not interaction.data or "custom_id" not in interaction.data:
            return False

        custom_id = interaction.data["custom_id"]
        if not custom_id.startswith(PREFIX_MAP_SELECT):
            return False

        if not self.cog:
            await interaction.response.send_message("Bot is restarting, please wait...", ephemeral=True)
            return False

        # Parse custom_id: mapban_map_{session_id}_{map_name}
        try:
            parts = custom_id[len(PREFIX_MAP_SELECT):].split("_", 1)
            session_id = parts[0]
            map_name = parts[1] if len(parts) > 1 else ""
        except (IndexError, ValueError):
            return False

        session = self.cog.active_sessions.get(session_id)
        if not session:
            session = await self.cog.get_session(session_id)
            if not session:
                await interaction.response.send_message("Session not found.", ephemeral=True)
                return False

        captain_id = interaction.user.id
        if captain_id != session["current_turn"]:
            await interaction.response.send_message("It's not your turn.", ephemeral=True)
            return False

        phase = session.get("current_phase")
        if phase == "banning":
            action_type = "ban"
        elif phase == "picking":
            action_type = "pick"
        else:
            # Not in a map selection phase — ignore
            await interaction.response.send_message("Not in a map selection phase.", ephemeral=True)
            return False

        # Show confirmation inline by editing the captain message
        embed = discord.Embed(
            title=f"Confirm {action_type.title()}",
            description=f"Are you sure you want to **{action_type}** **{map_name}**?",
            color=COLOR_WARNING
        )
        confirm_view = ConfirmMapSelectView(
            self.cog, session_id, captain_id, map_name, action_type
        )
        confirm_view.message = interaction.message
        await interaction.response.edit_message(
            embed=embed,
            view=confirm_view
        )
        return True


# PersistentSideSelectView removed - handled via on_interaction listener.


# PersistentAgentSelectView removed - handled via on_interaction listener.


class ObserveSessionView(discord.ui.View):
    """View with an Observe button that adds the admin to both session threads."""

    def __init__(self, cog: "MapBanCog", session_id: str, thread1_id: int, thread2_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.session_id = session_id
        self.thread1_id = thread1_id
        self.thread2_id = thread2_id

        button = discord.ui.Button(
            label="Observe",
            style=discord.ButtonStyle.secondary,
            custom_id=f"mapban_observe_{session_id}_{thread1_id}_{thread2_id}"
        )
        button.callback = self.observe_callback
        self.add_item(button)

    async def observe_callback(self, interaction: discord.Interaction):
        # Check admin role
        if self.cog:
            settings = await self.cog.get_guild_settings(interaction.guild_id)
            admin_role_id = settings.get("admin_role_id")
            if admin_role_id:
                role = interaction.guild.get_role(admin_role_id)
                if role and role not in interaction.user.roles:
                    if not self.cog.bot.is_bot_admin(interaction.user):
                        await interaction.response.send_message("You don't have the admin role.", ephemeral=True)
                        return

        thread1 = interaction.guild.get_thread(self.thread1_id)
        if thread1 is None and self.cog:
            try:
                thread1 = await self.cog.bot.fetch_channel(self.thread1_id)
            except Exception:
                thread1 = None
        thread2 = interaction.guild.get_thread(self.thread2_id)
        if thread2 is None and self.cog:
            try:
                thread2 = await self.cog.bot.fetch_channel(self.thread2_id)
            except Exception:
                thread2 = None

        added = False
        if thread1:
            try:
                await thread1.add_user(interaction.user)
                added = True
            except Exception:
                pass
        if thread2:
            try:
                await thread2.add_user(interaction.user)
                added = True
            except Exception:
                pass

        if added:
            await interaction.response.send_message("You've been added to the session threads.", ephemeral=True)
        else:
            await interaction.response.send_message("Could not add you — threads may no longer exist.", ephemeral=True)


class PersistentObserveView(discord.ui.View):
    """Persistent view for observe button - survives bot restart."""

    def __init__(self, cog: "MapBanCog" = None):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Observe", style=discord.ButtonStyle.secondary, custom_id="mapban_observe_placeholder")
    async def observe_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.data or "custom_id" not in interaction.data:
            return False

        custom_id = interaction.data["custom_id"]
        if not custom_id.startswith("mapban_observe_"):
            return False

        if not self.cog:
            await interaction.response.send_message("Bot is restarting, please wait...", ephemeral=True)
            return False

        # Parse custom_id: mapban_observe_{session_id}_{thread1_id}_{thread2_id}
        try:
            parts = custom_id[len("mapban_observe_"):].split("_")
            # session_id is like MB-20260207120000-1234, so rejoin the first 3 parts
            session_id = "_".join(parts[:-2])
            thread1_id = int(parts[-2])
            thread2_id = int(parts[-1])
        except (IndexError, ValueError):
            return False

        # Check admin role
        settings = await self.cog.get_guild_settings(interaction.guild_id)
        admin_role_id = settings.get("admin_role_id")
        if admin_role_id:
            role = interaction.guild.get_role(admin_role_id)
            if role and role not in interaction.user.roles:
                if not self.cog.bot.is_bot_admin(interaction.user):
                    await interaction.response.send_message("You don't have the admin role.", ephemeral=True)
                    return False

        thread1 = interaction.guild.get_thread(thread1_id)
        if thread1 is None and self.cog:
            try:
                thread1 = await self.cog.bot.fetch_channel(thread1_id)
            except Exception:
                thread1 = None
        thread2 = interaction.guild.get_thread(thread2_id)
        if thread2 is None and self.cog:
            try:
                thread2 = await self.cog.bot.fetch_channel(thread2_id)
            except Exception:
                thread2 = None

        added = False
        if thread1:
            try:
                await thread1.add_user(interaction.user)
                added = True
            except Exception:
                pass
        if thread2:
            try:
                await thread2.add_user(interaction.user)
                added = True
            except Exception:
                pass

        if added:
            await interaction.response.send_message("You've been added to the session threads.", ephemeral=True)
        else:
            await interaction.response.send_message("Could not add you — threads may no longer exist.", ephemeral=True)
        return True


class AgentRoleSelectView(discord.ui.View):
    """View with role buttons for agent selection (one per row)."""

    def __init__(self, cog: "MapBanCog", session_id: str, captain_id: int, action_type: str, available_agents: List[str]):
        super().__init__(timeout=180)
        self.cog = cog
        self.session_id = session_id
        self.captain_id = captain_id
        self.action_type = action_type
        self.available_agents = available_agents

        # Add role buttons - one per row
        for i, (role, agents) in enumerate(AGENT_ROLES.items()):
            available_in_role = [a for a in agents if a in available_agents]
            count = len(available_in_role)
            btn = discord.ui.Button(
                label=f"{role} ({count})",
                style=discord.ButtonStyle.primary if count > 0 else discord.ButtonStyle.secondary,
                disabled=count == 0,
                row=i
            )
            btn.callback = self._make_callback(role, available_in_role)
            self.add_item(btn)

    def _make_callback(self, role: str, agents_in_role: List[str]):
        async def callback(interaction: discord.Interaction):
            if not agents_in_role:
                await interaction.response.send_message("No available agents in this role.", ephemeral=True)
                return
            await interaction.response.edit_message(
                content=f"Select {role} to {self.action_type}:",
                view=AgentButtonsView(self.cog, self.session_id, self.captain_id, self.action_type, agents_in_role, self.available_agents)
            )
        return callback


class AgentButtonsView(discord.ui.View):
    """Buttons for each agent in a role, with pagination for large categories."""

    AGENTS_PER_PAGE = 8  # 4 rows x 2 per row, leaving row 4 for nav

    def __init__(self, cog: "MapBanCog", session_id: str, captain_id: int, action_type: str, agents: List[str], all_available: List[str], page: int = 0):
        super().__init__(timeout=180)
        self.cog = cog
        self.session_id = session_id
        self.captain_id = captain_id
        self.action_type = action_type
        self.all_available = all_available
        self.agents = sorted(agents)
        self.page = page
        self.total_pages = max(1, (len(self.agents) + self.AGENTS_PER_PAGE - 1) // self.AGENTS_PER_PAGE)

        # Get agents for current page
        start = page * self.AGENTS_PER_PAGE
        end = start + self.AGENTS_PER_PAGE
        page_agents = self.agents[start:end]

        # Add agent buttons - 2 per row for clean mobile look
        for i, agent in enumerate(page_agents):
            btn = discord.ui.Button(
                label=agent,
                style=discord.ButtonStyle.success if action_type == "protect" else discord.ButtonStyle.danger,
                row=i // 2
            )
            btn.callback = self._make_agent_callback(agent)
            self.add_item(btn)

        # Row 4: navigation buttons
        if self.total_pages > 1 and page > 0:
            prev_btn = discord.ui.Button(label="← Prev Page", style=discord.ButtonStyle.secondary, row=4)
            prev_btn.callback = self._prev_page_callback
            self.add_item(prev_btn)

        back_btn = discord.ui.Button(label="← Back", style=discord.ButtonStyle.secondary, row=4)
        back_btn.callback = self._back_callback
        self.add_item(back_btn)

        if self.total_pages > 1 and page < self.total_pages - 1:
            next_btn = discord.ui.Button(label="Next Page →", style=discord.ButtonStyle.secondary, row=4)
            next_btn.callback = self._next_page_callback
            self.add_item(next_btn)

    def _make_agent_callback(self, agent_name: str):
        async def callback(interaction: discord.Interaction):
            action_label = "Protect" if self.action_type == "protect" else "Ban"
            await interaction.response.edit_message(
                content=f"**{action_label} {agent_name}?**",
                view=ConfirmAgentSelectView(
                    self.cog, self.session_id, self.captain_id, agent_name,
                    self.action_type, self.agents, self.all_available, self.page
                )
            )
        return callback

    async def _back_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"Select a role to {self.action_type} an agent:",
            view=AgentRoleSelectView(self.cog, self.session_id, self.captain_id, self.action_type, self.all_available)
        )

    async def _prev_page_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            view=AgentButtonsView(self.cog, self.session_id, self.captain_id, self.action_type, self.agents, self.all_available, self.page - 1)
        )

    async def _next_page_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            view=AgentButtonsView(self.cog, self.session_id, self.captain_id, self.action_type, self.agents, self.all_available, self.page + 1)
        )


class ConfirmAgentSelectView(discord.ui.View):
    """Confirmation view for agent protect/ban selection."""

    def __init__(self, cog: "MapBanCog", session_id: str, captain_id: int,
                 agent_name: str, action_type: str, agents: List[str],
                 all_available: List[str], page: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.session_id = session_id
        self.captain_id = captain_id
        self.agent_name = agent_name
        self.action_type = action_type
        self.agents = agents
        self.all_available = all_available
        self.page = page

        confirm_btn = discord.ui.Button(
            label=f"Confirm {action_type.title()}",
            style=discord.ButtonStyle.success if action_type == "protect" else discord.ButtonStyle.danger,
        )
        confirm_btn.callback = self._confirm_callback
        self.add_item(confirm_btn)

        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel_btn.callback = self._cancel_callback
        self.add_item(cancel_btn)

    async def _confirm_callback(self, interaction: discord.Interaction):
        # Check if it's still this captain's turn before claiming success
        session = self.cog.active_sessions.get(self.session_id)
        if not session:
            session = await self.cog.get_session(self.session_id)
        if not session or session.get("current_turn") != self.captain_id:
            await interaction.response.edit_message(
                content="⏰ Your turn expired — an agent was auto-selected for you.",
                view=None
            )
            return
        await interaction.response.edit_message(
            content=f"✅ {self.action_type.title()}ed **{self.agent_name}**",
            view=None
        )
        await self.cog.handle_agent_selection(
            interaction, self.session_id, self.captain_id, self.agent_name, self.action_type
        )

    async def _cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"Select a role to {self.action_type} an agent:",
            view=AgentRoleSelectView(self.cog, self.session_id, self.captain_id, self.action_type, self.all_available)
        )


# =============================================================================
# DATABASE SETUP
# =============================================================================

async def init_database():
    """Initialize the database with required tables."""
    Path("data").mkdir(exist_ok=True)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        # Guild settings table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                admin_role_id INTEGER,
                parent_channel_id INTEGER,
                admin_log_channel_id INTEGER,
                spectator_channels TEXT DEFAULT '[]',
                turn_timeout INTEGER DEFAULT 120,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Maps table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS maps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                map_name TEXT NOT NULL,
                map_image_url TEXT,
                enabled INTEGER DEFAULT 1,
                display_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(guild_id, map_name)
            )
        """)
        
        # Active sessions table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                matchup_name TEXT NOT NULL,
                format TEXT NOT NULL,
                captain1_id INTEGER NOT NULL,
                captain2_id INTEGER NOT NULL,
                admin_id INTEGER NOT NULL,
                thread1_id INTEGER,
                thread2_id INTEGER,
                captain1_msg_id INTEGER,
                captain2_msg_id INTEGER,
                spectator_messages TEXT DEFAULT '[]',
                admin_log_msg_id INTEGER,
                first_ban TEXT NOT NULL,
                decider_side TEXT DEFAULT 'opponent',
                current_turn INTEGER,
                current_phase TEXT DEFAULT 'ready',
                map_pool TEXT NOT NULL,
                actions TEXT DEFAULT '[]',
                picked_maps TEXT DEFAULT '[]',
                side_selections TEXT DEFAULT '{}',
                captain1_ready INTEGER DEFAULT 0,
                captain2_ready INTEGER DEFAULT 0,
                scheduled_time TIMESTAMP,
                turn_start_time TIMESTAMP,
                reminder_sent INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                complete_time TIMESTAMP,
                current_side_select_map TEXT,
                agent_pool TEXT DEFAULT '[]',
                agent_protects TEXT DEFAULT '{}',
                agent_bans TEXT DEFAULT '{}',
                used_protects TEXT DEFAULT '{}',
                used_bans TEXT DEFAULT '{}',
                current_agent_phase TEXT,
                current_agent_map_index INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Agents table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                agent_name TEXT NOT NULL,
                agent_image_url TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(guild_id, agent_name)
            )
        """)

        # Migrations: add captain name columns if missing
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN captain1_name TEXT")
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN captain2_name TEXT")
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE guild_settings ADD COLUMN alert_channel_id INTEGER")
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN team1_name TEXT")
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN team2_name TEXT")
        except Exception:
            pass  # Column already exists

        # One-time cleanup: split corrupted agent entries containing newlines
        async with db.execute(
            "SELECT id, guild_id, agent_name FROM agents WHERE agent_name LIKE '%' || char(10) || '%'"
        ) as cursor:
            corrupted = await cursor.fetchall()
        for row in corrupted:
            row_id, guild_id, agent_name = row
            individual_names = [n.strip() for n in agent_name.split('\n') if n.strip()]
            for name in individual_names:
                try:
                    await db.execute(
                        "INSERT OR IGNORE INTO agents (guild_id, agent_name) VALUES (?, ?)",
                        (guild_id, name)
                    )
                except Exception:
                    pass
            await db.execute("DELETE FROM agents WHERE id = ?", (row_id,))

        await db.commit()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def generate_session_id() -> str:
    """Generate a unique session ID."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_suffix = random.randint(1000, 9999)
    return f"MB-{timestamp}-{random_suffix}"


def truncate_name(name: str, max_length: int = 12) -> str:
    """Truncate a name for mobile-friendly display."""
    if len(name) <= max_length:
        return name
    return name[:max_length-1] + "…"


def create_progress_bar(current: int, total: int) -> str:
    """Create a visual progress bar for the footer."""
    if total == 0:
        percentage = 100
        filled = PROGRESS_BAR_LENGTH
    else:
        percentage = int((current / total) * 100)
        filled = int((current / total) * PROGRESS_BAR_LENGTH)
    
    empty = PROGRESS_BAR_LENGTH - filled
    bar = PROGRESS_FILLED * filled + PROGRESS_EMPTY * empty
    return f"{bar} {current}/{total} ({percentage}%)"


def get_remaining_maps(map_pool: List[str], actions: List[Dict]) -> List[str]:
    """Get maps that haven't been banned or picked yet."""
    used_maps = {action["map"] for action in actions}
    return [m for m in map_pool if m not in used_maps]


def calculate_total_moves(format_type: str, map_count: int, session: Dict = None) -> int:
    """Calculate total moves needed for a format, including agent phases."""
    if format_type == "bo1":
        # All maps banned except 1, then side selection
        total = (map_count - 1) + 1  # bans + side select
    else:  # bo3
        # 2 bans, 2 picks, then bans until 1 remains, then 3 side selections
        # Total bans = map_count - 3, picks = 2, side selects = 3
        total = (map_count - 3) + 2 + 3

    # Add agent phase counts if agents are configured
    if session:
        agent_pool = json.loads(session.get("agent_pool", "[]")) if isinstance(session.get("agent_pool"), str) else session.get("agent_pool", [])
        if agent_pool:
            if format_type == "bo1":
                total += 4  # 2 protects + 2 bans for 1 map
            else:  # bo3
                total += 12  # 4 agent actions per map x 3 maps

    return total


def calculate_current_move(actions: List[Dict], side_selections: Dict, session: Dict = None) -> int:
    """Calculate current move number, including completed agent actions."""
    move = len(actions) + len(side_selections)

    if session:
        agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
        agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})
        for map_protects in agent_protects.values():
            move += len(map_protects)
        for map_bans in agent_bans.values():
            move += len(map_bans)

    return move


def format_time_remaining(seconds: int) -> str:
    """Format seconds into M:SS display."""
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}:{secs:02d}"


async def fetch_image(url: str) -> Optional[bytes]:
    """Fetch an image from a URL."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.read()
    except Exception as e:
        print(f"Failed to fetch image from {url}: {e}")
    return None


async def create_bo3_image(map_images: List[bytes]) -> Optional[BytesIO]:
    """Stitch 3 map images together horizontally for Bo3 result."""
    if not PIL_AVAILABLE or len(map_images) < 3:
        return None
    
    try:
        images = []
        for img_bytes in map_images:
            img = Image.open(BytesIO(img_bytes))
            images.append(img)
        
        # Resize all to same height, maintaining aspect ratio
        target_height = 300
        resized = []
        for img in images:
            ratio = target_height / img.height
            new_width = int(img.width * ratio)
            resized.append(img.resize((new_width, target_height), Image.Resampling.LANCZOS))
        
        # Calculate total width
        total_width = sum(img.width for img in resized)
        
        # Create new image
        result = Image.new('RGB', (total_width, target_height))
        
        # Paste images
        x_offset = 0
        for img in resized:
            result.paste(img, (x_offset, 0))
            x_offset += img.width
        
        # Save to BytesIO
        output = BytesIO()
        result.save(output, format='PNG', quality=95)
        output.seek(0)
        return output
    except Exception as e:
        print(f"Failed to create Bo3 image: {e}")
        return None


# =============================================================================
# SUMMARY CARD IMAGE GENERATION (HTML + Playwright)
# =============================================================================

import logging
import tempfile
logger = logging.getLogger(__name__)

SUMMARY_TEMPLATE_PATH = Path(__file__).parent / "templates" / "summary_card.html"
FONT_DIR = str(Path(__file__).parent.parent / "fonts")
ASSETS_DIR = Path(__file__).parent.parent / "assets"
MAPS_ASSET_DIR = ASSETS_DIR / "maps"
AGENTS_ASSET_DIR = ASSETS_DIR / "agents"

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright not installed. Summary cards will not be generated.")

# Shared browser instance (lazy-initialized)
_summary_browser = None
_summary_playwright = None


async def _get_summary_browser():
    """Get or create a shared Playwright browser for summary cards."""
    global _summary_browser, _summary_playwright
    if _summary_browser is None or not _summary_browser.is_connected():
        if _summary_playwright is None:
            _summary_playwright = await async_playwright().start()
        _summary_browser = await _summary_playwright.chromium.launch(
            executable_path="/usr/bin/chromium"
        )
    return _summary_browser


def _parse_summary_data(session: Dict) -> Dict:
    """Parse session data into a structure suitable for rendering."""
    actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
    map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
    side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
    agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
    agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})

    remaining = get_remaining_maps(map_pool, actions)

    captain1_id = session["captain1_id"]
    captain2_id = session["captain2_id"]

    # All bans in action order (with captain info for colouring)
    all_bans = [a for a in actions if a["type"] == "ban"]

    # Build played maps list (picks + decider)
    picks = [a for a in actions if a["type"] == "pick"]
    played_maps = []
    for p in picks:
        side_info = side_selections.get(p["map"], {})
        played_maps.append({
            "map": p["map"],
            "type": "pick",
            "picked_by": p["captain_id"],
            "picked_by_name": p["captain_name"],
            "side": side_info.get("side", ""),
            "side_chosen_by": side_info.get("chosen_by_name", ""),
        })

    if remaining:
        decider = remaining[0]
        side_info = side_selections.get(decider, {})
        played_maps.append({
            "map": decider,
            "type": "decider",
            "picked_by": None,
            "picked_by_name": "",
            "side": side_info.get("side", ""),
            "side_chosen_by": side_info.get("chosen_by_name", ""),
        })

    # Collect all map and agent names needed for image fetching
    all_map_names = list(set([a["map"] for a in actions] + [m["map"] for m in played_maps]))
    all_agent_names = set()
    for map_data in [agent_protects, agent_bans]:
        for map_name, captains in map_data.items():
            for cid, agent_name in captains.items():
                all_agent_names.add(agent_name)

    # Use team names if set, otherwise fall back to captain display names
    display_name1 = session.get("team1_name") or session.get("captain1_name", "Captain 1")
    display_name2 = session.get("team2_name") or session.get("captain2_name", "Captain 2")

    # Substitute display names (team names) throughout bans and played_maps
    name_map = {captain1_id: display_name1, captain2_id: display_name2}
    old_name1 = session.get("captain1_name", "")
    old_name2 = session.get("captain2_name", "")
    old_name_map = {old_name1: display_name1, old_name2: display_name2}
    all_bans = [
        {**ban, "captain_name": name_map.get(ban.get("captain_id"), ban["captain_name"])}
        for ban in all_bans
    ]
    for pm in played_maps:
        if pm.get("picked_by") in name_map:
            pm["picked_by_name"] = name_map[pm["picked_by"]]
        if pm.get("side_chosen_by") and pm["side_chosen_by"] in old_name_map:
            pm["side_chosen_by"] = old_name_map[pm["side_chosen_by"]]

    return {
        "matchup_name": session["matchup_name"],
        "format": session["format"],
        "captain1_name": display_name1,
        "captain2_name": display_name2,
        "captain1_id": captain1_id,
        "captain2_id": captain2_id,
        "all_bans": all_bans,
        "played_maps": played_maps,
        "agent_protects": agent_protects,
        "agent_bans": agent_bans,
        "all_map_names": all_map_names,
        "all_agent_names": list(all_agent_names),
    }


def _safe_filename(name: str) -> str:
    """Replace filesystem-unsafe characters for asset lookup."""
    return name.replace("/", "_").replace("\\", "_")


async def _get_image_urls(cog, guild_id: int, map_names: List[str], agent_names: List[str]) -> Tuple[Dict, Dict]:
    """Get map and agent image URLs. Prefers local assets, falls back to DB then API."""
    map_urls = {}
    for name in map_names:
        local = MAPS_ASSET_DIR / f"{_safe_filename(name)}.png"
        if local.exists():
            map_urls[name] = f"file://{local}"
        else:
            url = await cog.get_map_image_url(guild_id, name)
            if url:
                map_urls[name] = url

    agent_urls = {}
    for name in agent_names:
        local = AGENTS_ASSET_DIR / f"{_safe_filename(name)}.png"
        if local.exists():
            agent_urls[name] = f"file://{local}"
        else:
            # Try guild DB
            all_agents = await cog.get_agents(guild_id)
            for a in all_agents:
                if a["agent_name"] == name and a.get("agent_image_url"):
                    agent_urls[name] = a["agent_image_url"]
                    break

    return map_urls, agent_urls


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _build_ban_thumb_html(ban: Dict, map_urls: Dict, captain1_id: int) -> str:
    """Build HTML for a single ban thumbnail with captain colour."""
    map_name = ban["map"]
    captain_name = ban["captain_name"]
    captain_class = "c1" if ban["captain_id"] == captain1_id else "c2"
    url = map_urls.get(map_name, "")
    safe_name = _escape_html(map_name)
    safe_captain = _escape_html(captain_name)
    img_html = f'<img src="{url}" alt="{safe_name}">' if url else f'<div class="ban-placeholder">{safe_name}</div>'
    return (
        f'<div class="ban-thumb">'
        f'{img_html}'
        f'<div class="ban-overlay">'
        f'<svg viewBox="0 0 24 24" fill="none" stroke="#ED4245" stroke-width="3" stroke-linecap="round">'
        f'<line x1="4" y1="4" x2="20" y2="20"/><line x1="20" y1="4" x2="4" y2="20"/>'
        f'</svg></div>'
        f'<div class="ban-caption">'
        f'<div class="ban-captain {captain_class}">{safe_captain} banned</div>'
        f'<div class="ban-map-name">{safe_name}</div>'
        f'</div>'
        f'</div>'
    )


def _build_summary_html(template: str, data: Dict, map_urls: Dict, agent_urls: Dict) -> str:
    """Build the final HTML by formatting the template with data."""
    # Generate ban thumbnails HTML (all bans in action order)
    bans_html = "".join(
        _build_ban_thumb_html(ban, map_urls, data["captain1_id"])
        for ban in data["all_bans"]
    )

    # Generate played maps HTML
    played_maps_html = ""
    for pm in data["played_maps"]:
        safe_name = _escape_html(pm["map"])
        url = map_urls.get(pm["map"], "")
        img_html = f'<img src="{url}" alt="{safe_name}">' if url else f'<div class="map-placeholder">{safe_name}</div>'

        if pm["type"] == "decider":
            banner_class = "decider"
        elif pm.get("picked_by") == data["captain1_id"]:
            banner_class = "pick-c1"
        else:
            banner_class = "pick-c2"

        side_html = ""
        if pm["side"]:
            side_text = f"{pm['side'][:3].title()}"
            if pm["side_chosen_by"]:
                side_text += f" - {_escape_html(pm['side_chosen_by'])}"
            side_html = f'<div class="side-info">{side_text}</div>'

        played_maps_html += (
            f'<div class="played-map">'
            f'{img_html}'
            f'<div class="map-banner {banner_class}">{safe_name}</div>'
            f'{side_html}'
            f'</div>'
        )

    # Generate agent bans/protects HTML
    agents_html = ""
    for pm in data["played_maps"]:
        map_name = pm["map"]
        map_bans_data = data["agent_bans"].get(map_name, {})
        map_protects = data["agent_protects"].get(map_name, {})

        if not map_bans_data and not map_protects:
            continue

        bans_icons = ""
        for cid_str, agent_name in map_bans_data.items():
            safe_agent = _escape_html(agent_name)
            url = agent_urls.get(agent_name, "")
            icon = f'<img src="{url}" alt="{safe_agent}">' if url else f'<div class="agent-icon-placeholder">{safe_agent[0]}</div>'
            bans_icons += f'<div class="agent-icon-wrap">{icon}</div>'

        protects_icons = ""
        for cid_str, agent_name in map_protects.items():
            safe_agent = _escape_html(agent_name)
            url = agent_urls.get(agent_name, "")
            icon = f'<img src="{url}" alt="{safe_agent}">' if url else f'<div class="agent-icon-placeholder">{safe_agent[0]}</div>'
            protects_icons += f'<div class="agent-icon-wrap">{icon}</div>'

        agents_html += (
            f'<div class="agent-map-group">'
            f'<div class="agent-col">'
            f'<div class="agent-col-header bans">BANS</div>'
            f'<div class="agent-icons">{bans_icons}</div>'
            f'</div>'
            f'<div class="agent-col">'
            f'<div class="agent-col-header protects">PROTECTS</div>'
            f'<div class="agent-icons">{protects_icons}</div>'
            f'</div>'
            f'</div>'
        )

    is_bo1 = data["format"] == "bo1"
    return template.format(
        font_path=FONT_DIR,
        title=_escape_html(data["matchup_name"]),
        bans_html=bans_html,
        played_maps_html=played_maps_html,
        agents_html=agents_html,
        body_class="bo1" if is_bo1 else "bo3",
    )


async def generate_summary_card(session: Dict, guild_id: int, cog) -> Optional[BytesIO]:
    """Generate a summary card image via HTML template + Playwright screenshot."""
    if not PLAYWRIGHT_AVAILABLE:
        return None

    try:
        data = _parse_summary_data(session)
        map_urls, agent_urls = await _get_image_urls(
            cog, guild_id, data["all_map_names"], data["all_agent_names"]
        )

        template = SUMMARY_TEMPLATE_PATH.read_text()
        html = _build_summary_html(template, data, map_urls, agent_urls)

        # Write HTML to a temp file so Chromium can load file:// image URLs
        tmp = tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w", encoding="utf-8")
        tmp.write(html)
        tmp.close()
        tmp_path = tmp.name

        try:
            browser = await _get_summary_browser()
            page = await browser.new_page(
                viewport={"width": 960, "height": 540},
                device_scale_factor=2,
            )
            await page.goto(f"file://{tmp_path}")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(200)

            screenshot = await page.screenshot(type="png")
            await page.close()
            return BytesIO(screenshot)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    except Exception as e:
        logger.error(f"Summary card generation failed: {e}")
        return None


# =============================================================================
# EMBED BUILDERS
# =============================================================================

def build_picks_bans_text(actions: List[Dict], remaining_maps: List[str],
                          side_selections: Dict, show_arrow: bool = True,
                          map_pool: List[str] = None) -> str:
    """Build the picks/bans section text for embeds."""
    lines = []

    # Add completed actions
    for i, action in enumerate(actions):
        map_name = action["map"]
        captain_name = truncate_name(action["captain_name"])
        action_type = action["type"]
        is_last = (i == len(actions) - 1) and show_arrow and len(remaining_maps) != 1

        # Check for side selection on this map
        side_info = ""
        if map_name in side_selections and action_type == "pick":
            chooser_side = side_selections[map_name]["side"]
            # Picker starts on opposite side of what chooser chose
            picker_side = "Defense" if chooser_side.lower() == "attack" else "Attack"
            side_info = f" ({picker_side[:3].title()})"

        arrow = "➡️ " if is_last else ""

        if action_type == "ban":
            lines.append(f"{arrow}~~{map_name}~~ {captain_name}")
        else:  # pick
            lines.append(f"{arrow}**{map_name}** {captain_name}{side_info}")

    # Show decider map when exactly 1 map remains (all bans/picks done)
    if len(remaining_maps) == 1:
        decider = remaining_maps[0]
        side_info = ""
        if decider in side_selections:
            side = side_selections[decider]["side"]
            chosen_by = side_selections[decider].get("chosen_by_name", "")
            side_info = f" ({side[:3].title()} - {truncate_name(chosen_by)})" if chosen_by else f" ({side[:3].title()})"
        arrow = "➡️ " if show_arrow else ""
        lines.append(f"{arrow}**{decider}** (Decider){side_info}")

    if not lines:
        lines.append("*No picks or bans yet*")

    return "\n".join(lines)


def build_remaining_pool_text(remaining_maps: List[str]) -> str:
    """Build the remaining pool section text."""
    if not remaining_maps:
        return "*All maps selected*"
    return " • ".join(remaining_maps)


def build_captain_embed(session: Dict, is_captain1: bool,
                        time_remaining: Optional[int] = None) -> discord.Embed:
    """Build the embed for a captain's private thread."""
    format_display = "Bo1" if session["format"] == "bo1" else "Bo3"
    phase = session["current_phase"]

    # Phase-specific embed color
    if phase in ("banning", "agent_ban"):
        embed_color = COLOR_DANGER
    elif phase == "picking":
        embed_color = COLOR_SUCCESS
    elif phase == "side_select":
        embed_color = COLOR_PRIMARY
    elif phase == "agent_protect":
        embed_color = COLOR_PROTECT
    else:
        embed_color = COLOR_PRIMARY

    embed = discord.Embed(
        title=f"{session['matchup_name']} ({format_display})",
        color=embed_color
    )

    actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
    map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
    side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
    remaining = get_remaining_maps(map_pool, actions)

    # Picks/Bans section
    picks_bans = build_picks_bans_text(actions, remaining, side_selections, map_pool=map_pool)
    embed.add_field(name="**Picks/Bans**", value=picks_bans, inline=False)

    # Remaining pool
    if len(remaining) > 1:
        pool_text = build_remaining_pool_text(remaining)
        embed.add_field(name="**Remaining Pool**", value=pool_text, inline=False)

    # Agent selection history - ordered by map pick order, not alphabetically
    agent_protects_all = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
    agent_bans_all = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})

    if agent_protects_all or agent_bans_all:
        picked_in_order = [a["map"] for a in actions if a.get("type") == "pick"]
        decider_map = remaining[0] if remaining else None
        maps_in_order = picked_in_order + ([decider_map] if decider_map else [])
        all_agent_maps_set = set(list(agent_protects_all.keys()) + list(agent_bans_all.keys()))
        all_agent_maps = [m for m in maps_in_order if m in all_agent_maps_set]
        all_agent_maps += [m for m in all_agent_maps_set if m not in all_agent_maps]

        if all_agent_maps:
            history_lines = []
            for map_name in all_agent_maps:
                map_protects_hist = agent_protects_all.get(map_name, {})
                map_bans_hist = agent_bans_all.get(map_name, {})
                if map_protects_hist or map_bans_hist:
                    history_lines.append(f"**{map_name}:**")
                    for cid, agent in map_protects_hist.items():
                        cname = truncate_name(session.get("captain1_name" if int(cid) == session["captain1_id"] else "captain2_name", "Captain"))
                        history_lines.append(f"🟢{agent} ({cname})")
                    for cid, agent in map_bans_hist.items():
                        cname = truncate_name(session.get("captain1_name" if int(cid) == session["captain1_id"] else "captain2_name", "Captain"))
                        history_lines.append(f"🔴{agent} ({cname})")

            if history_lines:
                embed.add_field(
                    name="**Agent Selection**",
                    value="\n".join(history_lines),
                    inline=False
                )

    # Status section - always last, just above footer
    current_turn = session["current_turn"]
    captain_id = session["captain1_id"] if is_captain1 else session["captain2_id"]
    is_my_turn = current_turn == captain_id
    other_name = truncate_name(session.get("captain2_name" if is_captain1 else "captain1_name", "opponent"))

    if phase == "ready":
        c1_ready = "✅" if session["captain1_ready"] else "⏳"
        c2_ready = "✅" if session["captain2_ready"] else "⏳"
        c1_name = truncate_name(session.get("captain1_name", "Captain 1"))
        c2_name = truncate_name(session.get("captain2_name", "Captain 2"))

        status = f"{c1_name}: {c1_ready}\n{c2_name}: {c2_ready}"
        embed.add_field(name="**Ready Up**", value=status, inline=False)

    elif phase == "complete":
        embed.add_field(name="**Status**", value="✅ Map ban complete!", inline=False)

    elif phase in ("banning", "picking"):
        if is_my_turn:
            time_text = f"\n⏱️ {format_time_remaining(time_remaining)} remaining" if time_remaining is not None else ""
            if phase == "banning":
                embed.add_field(
                    name="\U0001f534 YOUR TURN \u2014 BAN A MAP",
                    value=f"Select a map to ban!{time_text}",
                    inline=False
                )
            else:
                embed.add_field(
                    name="\U0001f7e2 YOUR TURN \u2014 PICK A MAP",
                    value=f"Select a map to pick!{time_text}",
                    inline=False
                )
        else:
            action_word = "banning" if phase == "banning" else "picking"
            embed.add_field(
                name="**Waiting**",
                value=f"⏳ Waiting \u2014 {other_name} is {action_word}...",
                inline=False
            )

    elif phase == "side_select":
        current_map = session.get("current_side_select_map", "the map")
        if is_my_turn:
            time_text = f"\n⏱️ {format_time_remaining(time_remaining)} remaining" if time_remaining is not None else ""
            embed.add_field(
                name="\U0001f535 YOUR TURN \u2014 CHOOSE STARTING SIDE",
                value=f"Select Attack or Defense for **{current_map}**{time_text}",
                inline=False
            )
        else:
            embed.add_field(
                name="**Waiting**",
                value=f"⏳ Waiting \u2014 {other_name} is choosing a side...",
                inline=False
            )

    elif phase in ("agent_protect", "agent_ban"):
        current_index = session.get("current_agent_map_index", 0)
        picked = [a["map"] for a in actions if a.get("type") == "pick"]
        decider = remaining[0] if remaining else ""
        maps_to_play = picked + ([decider] if decider else [])
        current_map = maps_to_play[current_index] if current_index < len(maps_to_play) else ""

        if is_my_turn:
            time_text = f"\n⏱️ {format_time_remaining(time_remaining)} remaining" if time_remaining is not None else ""
            if phase == "agent_protect":
                embed.add_field(
                    name="🟢 YOUR TURN \u2014 PROTECT AN AGENT",
                    value=f"Select an agent to protect for **{current_map}** (Map {current_index + 1}){time_text}",
                    inline=False
                )
            else:
                embed.add_field(
                    name="\U0001f534 YOUR TURN \u2014 BAN AN AGENT",
                    value=f"Select an agent to ban for **{current_map}** (Map {current_index + 1}){time_text}",
                    inline=False
                )
        else:
            action_word = "protecting an agent" if phase == "agent_protect" else "banning an agent"
            embed.add_field(
                name="**Waiting**",
                value=f"⏳ Waiting \u2014 {other_name} is {action_word}...",
                inline=False
            )

    # Progress bar footer
    total_moves = calculate_total_moves(session["format"], len(map_pool), session)
    current_move = calculate_current_move(actions, side_selections, session)
    progress = create_progress_bar(current_move, total_moves)
    embed.set_footer(text=progress)

    return embed


def build_spectator_embed(session: Dict) -> discord.Embed:
    """Build the spectator view embed."""
    format_display = "Bo1" if session["format"] == "bo1" else "Bo3"
    phase = session["current_phase"]

    # Phase-specific embed color
    if phase in ("banning", "agent_ban"):
        embed_color = COLOR_DANGER
    elif phase == "picking":
        embed_color = COLOR_SUCCESS
    elif phase == "side_select":
        embed_color = COLOR_PRIMARY
    elif phase == "agent_protect":
        embed_color = COLOR_PROTECT
    else:
        embed_color = COLOR_PRIMARY

    embed = discord.Embed(
        title=f"{session['matchup_name']} ({format_display})",
        color=embed_color
    )

    actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
    map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
    side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
    remaining = get_remaining_maps(map_pool, actions)

    # Picks/Bans section
    picks_bans = build_picks_bans_text(actions, remaining, side_selections, map_pool=map_pool)
    embed.add_field(name="**Picks/Bans**", value=picks_bans, inline=False)

    # Remaining pool
    if len(remaining) > 1:
        pool_text = build_remaining_pool_text(remaining)
        embed.add_field(name="**Remaining Pool**", value=pool_text, inline=False)

    # Agent selection history
    agent_protects_all = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
    agent_bans_all = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})

    if agent_protects_all or agent_bans_all:
        picked_in_order = [a["map"] for a in actions if a.get("type") == "pick"]
        decider_map = remaining[0] if remaining else None
        maps_in_order = picked_in_order + ([decider_map] if decider_map else [])
        all_agent_maps_set = set(list(agent_protects_all.keys()) + list(agent_bans_all.keys()))
        all_agent_maps = [m for m in maps_in_order if m in all_agent_maps_set]
        all_agent_maps += [m for m in all_agent_maps_set if m not in all_agent_maps]

        if all_agent_maps:
            history_lines = []
            for map_name in all_agent_maps:
                map_protects_hist = agent_protects_all.get(map_name, {})
                map_bans_hist = agent_bans_all.get(map_name, {})
                if map_protects_hist or map_bans_hist:
                    history_lines.append(f"**{map_name}:**")
                    for cid, agent in map_protects_hist.items():
                        cname = truncate_name(session.get("captain1_name" if int(cid) == session["captain1_id"] else "captain2_name", "Captain"))
                        history_lines.append(f"🟢{agent} ({cname})")
                    for cid, agent in map_bans_hist.items():
                        cname = truncate_name(session.get("captain1_name" if int(cid) == session["captain1_id"] else "captain2_name", "Captain"))
                        history_lines.append(f"🔴{agent} ({cname})")

            if history_lines:
                embed.add_field(
                    name="**Agent Selection**",
                    value="\n".join(history_lines),
                    inline=False
                )

    # Status
    if phase == "ready":
        c1_ready = "✅" if session["captain1_ready"] else "⏳"
        c2_ready = "✅" if session["captain2_ready"] else "⏳"
        c1_name = truncate_name(session.get("captain1_name", "Captain 1"))
        c2_name = truncate_name(session.get("captain2_name", "Captain 2"))
        embed.add_field(
            name="**Waiting for Ready**",
            value=f"{c1_name}: {c1_ready}\n{c2_name}: {c2_ready}",
            inline=False
        )
    elif phase == "complete":
        embed.add_field(name="**Status**", value="✅ Complete!", inline=False)
    else:
        current_turn = session["current_turn"]
        if current_turn == session["captain1_id"]:
            turn_name = truncate_name(session.get("captain1_name", "Captain 1"))
        else:
            turn_name = truncate_name(session.get("captain2_name", "Captain 2"))

        if phase == "banning":
            status_text = f"⏳ {turn_name} is banning a map..."
        elif phase == "picking":
            status_text = f"⏳ {turn_name} is picking a map..."
        elif phase == "side_select":
            status_text = f"⏳ {turn_name} is choosing a side..."
        elif phase == "agent_protect":
            status_text = f"⏳ {turn_name} is protecting an agent..."
        elif phase == "agent_ban":
            status_text = f"⏳ {turn_name} is banning an agent..."
        else:
            status_text = f"⏳ {turn_name} is selecting..."

        embed.add_field(
            name="**Current Turn**",
            value=status_text,
            inline=False
        )

    # Progress bar footer
    total_moves = calculate_total_moves(session["format"], len(map_pool), session)
    current_move = calculate_current_move(actions, side_selections, session)
    progress = create_progress_bar(current_move, total_moves)
    embed.set_footer(text=progress)

    return embed


def build_final_result_embed(session: Dict) -> discord.Embed:
    """Build the final result embed."""
    format_display = "Bo1" if session["format"] == "bo1" else "Bo3"
    embed = discord.Embed(
        title=f"🏆 Map Ban Complete - {session['matchup_name']}",
        color=COLOR_SUCCESS
    )
    
    embed.add_field(name="**Format**", value=format_display, inline=True)
    
    actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
    side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
    
    # Maps to play section
    picked_maps = [a for a in actions if a["type"] == "pick"]
    
    # For Bo1, the last remaining map is the decider
    if session["format"] == "bo1":
        map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
        remaining = get_remaining_maps(map_pool, actions)
        if remaining:
            decider = remaining[0]
            side_info = side_selections.get(decider, {})
            side_text = f" ({side_info.get('side', 'TBD')[:3].title()})" if side_info else ""
            captain_name = side_info.get("chosen_by_name", "")
            maps_text = f"1. {decider}{side_text} - {truncate_name(captain_name)} (Decider)"
        else:
            maps_text = "*Error: No map remaining*"
    else:
        # Bo3 - show the 3 maps
        lines = []
        for i, action in enumerate(picked_maps):
            map_name = action["map"]
            side_info = side_selections.get(map_name, {})
            if side_info:
                chooser_side = side_info.get('side', 'TBD')
                # Picker starts on opposite side of what chooser chose
                picker_side = "Defense" if chooser_side.lower() == "attack" else "Attack"
                side_text = f"({picker_side[:3].title()})"
            else:
                side_text = ""
            captain_name = truncate_name(action["captain_name"])
            lines.append(f"{i+1}. {map_name} - {captain_name} {side_text}")
        
        # Add decider
        map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
        remaining = get_remaining_maps(map_pool, actions)
        if remaining:
            decider = remaining[0]
            side_info = side_selections.get(decider, {})
            side_text = f"({side_info.get('side', 'TBD')[:3].title()})" if side_info else ""
            captain_name = side_info.get("chosen_by_name", "")
            lines.append(f"3. {decider} (Decider) - {truncate_name(captain_name)} {side_text}")
        
        maps_text = "\n".join(lines)
    
    embed.add_field(name="**Maps to Play**", value=maps_text, inline=False)
    
    # Full ban/pick order
    full_order_lines = []
    for action in actions:
        map_name = action["map"]
        captain_name = truncate_name(action["captain_name"])
        if action["type"] == "ban":
            full_order_lines.append(f"~~{map_name}~~ {captain_name}")
        else:
            full_order_lines.append(f"**{map_name}** {captain_name}")
    
    # Add decider
    map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
    remaining = get_remaining_maps(map_pool, actions)
    if remaining:
        full_order_lines.append(f"**{remaining[0]}** (Decider)")
    
    embed.add_field(name="**Full Ban/Pick Order**", value="\n".join(full_order_lines), inline=False)
    
    return embed


def build_admin_panel_embed(settings: Dict) -> discord.Embed:
    """Build the admin panel embed."""
    embed = discord.Embed(
        title="Map Ban Admin Panel",
        description="Configure map ban settings for this server.",
        color=COLOR_PRIMARY
    )
    
    # Admin role
    admin_role = f"<@&{settings['admin_role_id']}>" if settings.get('admin_role_id') else "*Not set*"
    embed.add_field(name="Admin Role", value=admin_role, inline=True)
    
    # Parent channel
    parent_channel = f"<#{settings['parent_channel_id']}>" if settings.get('parent_channel_id') else "*Not set*"
    embed.add_field(name="Parent Channel", value=parent_channel, inline=True)
    
    # Admin log
    admin_log = f"<#{settings['admin_log_channel_id']}>" if settings.get('admin_log_channel_id') else "*Not set*"
    embed.add_field(name="Admin Log", value=admin_log, inline=True)
    
    # Alert channel
    alert_channel = f"<#{settings['alert_channel_id']}>" if settings.get('alert_channel_id') else "*Not set*"
    embed.add_field(name="Alert Channel", value=alert_channel, inline=True)

    # Spectator channels
    spectator_channels = json.loads(settings.get('spectator_channels', '[]'))
    if spectator_channels:
        spec_text = "\n".join([f"<#{ch}>" for ch in spectator_channels[:5]])
        if len(spectator_channels) > 5:
            spec_text += f"\n*...and {len(spectator_channels) - 5} more*"
    else:
        spec_text = "*Not set*"
    embed.add_field(name="Spectator Channels", value=spec_text, inline=True)

    # Turn timeout
    timeout = settings.get('turn_timeout', DEFAULT_TURN_TIMEOUT)
    embed.add_field(name="Turn Timeout", value=f"{timeout} seconds", inline=True)
    
    return embed


# =============================================================================
# VIEWS - ADMIN PANEL
# =============================================================================

class AdminPanelView(discord.ui.View):
    """Main admin panel view with configuration buttons."""
    
    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=None)
        self.cog = cog
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Check if user has admin permissions."""
        settings = await self.cog.get_guild_settings(interaction.guild_id)
        admin_role_id = settings.get("admin_role_id")
        
        if admin_role_id:
            role = interaction.guild.get_role(admin_role_id)
            if role and role in interaction.user.roles:
                return True
        
        # Fallback to administrator permission or bot admin role
        if self.cog.bot.is_bot_admin(interaction.user):
            return True
        
        await interaction.response.send_message(
            "❌ You don't have permission to use this panel.",
            ephemeral=True
        )
        return False
    
    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.primary, row=0)
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set the admin role for map bans."""
        await interaction.response.send_message("Select admin role:", view=SetAdminRoleView(self.cog), ephemeral=True)

    @discord.ui.button(label="Set Parent Channel", style=discord.ButtonStyle.primary, row=0)
    async def set_parent_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set the parent channel for threads."""
        await interaction.response.send_message("Select parent channel:", view=SetParentChannelView(self.cog), ephemeral=True)

    @discord.ui.button(label="Set Admin Log", style=discord.ButtonStyle.primary, row=0)
    async def set_admin_log(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set the admin log channel."""
        await interaction.response.send_message("Select admin log channel:", view=SetAdminLogView(self.cog), ephemeral=True)

    @discord.ui.button(label="Set Alert Channel", style=discord.ButtonStyle.primary, row=1)
    async def set_alert_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set the alert channel for new session notifications."""
        await interaction.response.send_message("Select alert channel:", view=SetAlertChannelView(self.cog), ephemeral=True)

    @discord.ui.button(label="Set Spectator Channels", style=discord.ButtonStyle.primary, row=1)
    async def set_spectator_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set spectator channels."""
        await interaction.response.send_message("Select spectator channels (up to 10):", view=SetSpectatorChannelsView(self.cog), ephemeral=True)

    @discord.ui.button(label="Set Turn Timeout", style=discord.ButtonStyle.secondary, row=1)
    async def set_turn_timeout(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set the turn timeout duration."""
        await interaction.response.send_modal(SetTurnTimeoutModal(self.cog))
    
    @discord.ui.button(label="Manage Maps", style=discord.ButtonStyle.success, row=2)
    async def manage_maps(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open map management view."""
        maps = await self.cog.get_maps(interaction.guild_id)
        embed = build_map_management_embed(maps)
        await interaction.response.send_message(embed=embed, view=MapManagementView(self.cog), ephemeral=True)
    
    @discord.ui.button(label="Manage Map Images", style=discord.ButtonStyle.success, row=2)
    async def manage_map_images(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open map image management."""
        maps = await self.cog.get_maps(interaction.guild_id)
        embed = build_map_images_embed(maps)
        await interaction.response.send_message(embed=embed, view=MapImageManagementView(self.cog), ephemeral=True)

    @discord.ui.button(label="Manage Agents", style=discord.ButtonStyle.success, row=2)
    async def manage_agents(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open agent management view."""
        agents = await self.cog.get_agents(interaction.guild_id)
        embed = build_agent_management_embed(agents)
        await interaction.response.send_message(embed=embed, view=AgentManagementView(self.cog), ephemeral=True)

    @discord.ui.button(label="Cancel Session", style=discord.ButtonStyle.danger, row=3)
    async def cancel_session(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel an active session."""
        sessions = await self.cog.get_active_sessions(interaction.guild_id)
        if not sessions:
            await interaction.response.send_message("❌ No active sessions to cancel.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🗑️ Cancel Session",
            description="Select a session to cancel:",
            color=COLOR_DANGER
        )
        await interaction.response.send_message(embed=embed, view=CancelSessionView(self.cog, sessions), ephemeral=True)
    
    @discord.ui.button(label="Clear Settings", style=discord.ButtonStyle.danger, row=3)
    async def clear_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clear a configured setting."""
        settings = await self.cog.get_guild_settings(interaction.guild_id)
        await interaction.response.send_message(
            "Select a setting to clear:",
            view=ClearSettingsView(self.cog, settings),
            ephemeral=True
        )

    @discord.ui.button(label="Refresh Panel", style=discord.ButtonStyle.secondary, row=3)
    async def refresh_panel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Refresh the admin panel."""
        settings = await self.cog.get_guild_settings(interaction.guild_id)
        embed = build_admin_panel_embed(settings)
        await interaction.response.edit_message(embed=embed, view=self)


# =============================================================================
# MODALS - ADMIN SETTINGS
# =============================================================================

class SetAdminRoleView(discord.ui.View):
    """View with role select for admin role."""

    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select admin role")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await self.cog.update_guild_setting(interaction.guild_id, "admin_role_id", role.id)
        await interaction.response.edit_message(content=f"✅ Admin role set to {role.mention}", view=None)


class SetParentChannelView(discord.ui.View):
    """View with channel select for parent channel."""

    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select parent channel", channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await self.cog.update_guild_setting(interaction.guild_id, "parent_channel_id", channel.id)
        await interaction.response.edit_message(content=f"✅ Parent channel set to {channel.mention}", view=None)


class SetAdminLogView(discord.ui.View):
    """View with channel select for admin log."""

    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select admin log channel", channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread, discord.ChannelType.private_thread])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await self.cog.update_guild_setting(interaction.guild_id, "admin_log_channel_id", channel.id)
        await interaction.response.edit_message(content=f"✅ Admin log set to {channel.mention}", view=None)


class SetAlertChannelView(discord.ui.View):
    """View with channel select for alert channel."""

    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select alert channel", channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await self.cog.update_guild_setting(interaction.guild_id, "alert_channel_id", channel.id)
        await interaction.response.edit_message(content=f"✅ Alert channel set to {channel.mention}", view=None)


class SetSpectatorChannelsView(discord.ui.View):
    """View with channel select for spectator channels (multi-select)."""

    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select spectator channels", channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread, discord.ChannelType.private_thread], min_values=1, max_values=10)
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel_ids = [ch.id for ch in select.values]
        await self.cog.update_guild_setting(interaction.guild_id, "spectator_channels", json.dumps(channel_ids))
        await interaction.response.edit_message(content=f"✅ Set {len(channel_ids)} spectator channel(s)", view=None)


class SetTurnTimeoutModal(discord.ui.Modal, title="Set Turn Timeout"):
    """Modal to set the turn timeout."""
    
    timeout = discord.ui.TextInput(
        label="Timeout (seconds)",
        placeholder="Enter timeout in seconds (e.g., 120)",
        required=True,
        max_length=4,
        default="120"
    )
    
    def __init__(self, cog: "MapBanCog"):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            timeout = int(self.timeout.value.strip())
            if timeout < 30 or timeout > 600:
                await interaction.response.send_message("❌ Timeout must be between 30 and 600 seconds.", ephemeral=True)
                return
            
            await self.cog.update_guild_setting(interaction.guild_id, "turn_timeout", timeout)
            await interaction.response.send_message(f"✅ Turn timeout set to {timeout} seconds", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ Invalid timeout value.", ephemeral=True)


class ClearSettingsView(discord.ui.View):
    """View for clearing configured settings."""

    def __init__(self, cog: "MapBanCog", settings: Dict):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        if settings.get("admin_role_id"):
            options.append(discord.SelectOption(label="Admin Role", value="admin_role_id", description="Remove admin role"))
        if settings.get("parent_channel_id"):
            options.append(discord.SelectOption(label="Parent Channel", value="parent_channel_id", description="Remove parent channel"))
        if settings.get("admin_log_channel_id"):
            options.append(discord.SelectOption(label="Admin Log Channel", value="admin_log_channel_id", description="Remove log channel"))
        if settings.get("alert_channel_id"):
            options.append(discord.SelectOption(label="Alert Channel", value="alert_channel_id", description="Remove alert channel"))

        spectator_channels = json.loads(settings.get("spectator_channels", "[]"))
        if spectator_channels:
            options.append(discord.SelectOption(label="All Spectator Channels", value="spectator_channels", description="Remove all spectator channels"))
            options.append(discord.SelectOption(label="Remove One Spectator Channel", value="remove_one_spectator", description="Pick a specific channel to remove"))

        if not options:
            # No settings configured - add a disabled placeholder
            options.append(discord.SelectOption(label="No settings to clear", value="none"))

        self.select = discord.ui.Select(placeholder="Select setting to clear", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        value = self.select.values[0]

        if value == "none":
            await interaction.response.edit_message(content="No settings configured to clear.", view=None)
            return

        if value == "remove_one_spectator":
            settings = await self.cog.get_guild_settings(interaction.guild_id)
            spectator_channels = json.loads(settings.get("spectator_channels", "[]"))
            await interaction.response.edit_message(
                content="Select a spectator channel to remove:",
                view=RemoveSpectatorChannelView(self.cog, spectator_channels)
            )
            return

        # Clear the setting
        if value == "spectator_channels":
            await self.cog.update_guild_setting(interaction.guild_id, value, json.dumps([]))
        else:
            await self.cog.update_guild_setting(interaction.guild_id, value, None)

        label_map = {
            "admin_role_id": "Admin Role",
            "parent_channel_id": "Parent Channel",
            "admin_log_channel_id": "Admin Log Channel",
            "alert_channel_id": "Alert Channel",
            "spectator_channels": "All Spectator Channels",
        }
        await interaction.response.edit_message(
            content=f"✅ Cleared **{label_map.get(value, value)}**",
            view=None
        )


class RemoveSpectatorChannelView(discord.ui.View):
    """View for removing a specific spectator channel."""

    def __init__(self, cog: "MapBanCog", channel_ids: List[int]):
        super().__init__(timeout=60)
        self.cog = cog
        self.channel_ids = channel_ids

        options = [
            discord.SelectOption(label=f"Channel {ch_id}", value=str(ch_id), description=f"ID: {ch_id}")
            for ch_id in channel_ids[:25]
        ]
        self.select = discord.ui.Select(placeholder="Select channel to remove", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        channel_id = int(self.select.values[0])
        new_channels = [ch for ch in self.channel_ids if ch != channel_id]
        await self.cog.update_guild_setting(interaction.guild_id, "spectator_channels", json.dumps(new_channels))

        channel_mention = f"<#{channel_id}>"
        await interaction.response.edit_message(
            content=f"✅ Removed spectator channel {channel_mention} ({len(new_channels)} remaining)",
            view=None
        )


# =============================================================================
# VIEWS & MODALS - MAP MANAGEMENT
# =============================================================================

def build_map_management_embed(maps: List[Dict]) -> discord.Embed:
    """Build the map management embed."""
    embed = discord.Embed(
        title="🗺️ Manage Maps",
        description="Add, remove, or toggle maps for the map pool.",
        color=COLOR_PRIMARY
    )
    
    if maps:
        enabled = [m["map_name"] for m in maps if m["enabled"]]
        disabled = [m["map_name"] for m in maps if not m["enabled"]]
        
        if enabled:
            embed.add_field(name="✅ Enabled Maps", value="\n".join(enabled), inline=True)
        if disabled:
            embed.add_field(name="❌ Disabled Maps", value="\n".join(disabled), inline=True)
    else:
        embed.add_field(name="Maps", value="*No maps configured*", inline=False)
    
    return embed


def build_map_images_embed(maps: List[Dict]) -> discord.Embed:
    """Build the map images management embed."""
    embed = discord.Embed(
        title="🖼️ Manage Map Images",
        description="Set image URLs for each map (used in final results).",
        color=COLOR_PRIMARY
    )
    
    if maps:
        lines = []
        for m in maps:
            has_image = "✅" if m.get("map_image_url") else "❌"
            lines.append(f"{has_image} {m['map_name']}")
        embed.add_field(name="Maps", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="Maps", value="*No maps configured*", inline=False)
    
    return embed


class MapManagementView(discord.ui.View):
    """View for managing maps."""
    
    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=300)
        self.cog = cog
    
    @discord.ui.button(label="Add Map", style=discord.ButtonStyle.success)
    async def add_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddMapModal(self.cog))
    
    @discord.ui.button(label="Remove Map", style=discord.ButtonStyle.danger)
    async def remove_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        maps = await self.cog.get_maps(interaction.guild_id)
        if not maps:
            await interaction.response.send_message("❌ No maps to remove.", ephemeral=True)
            return
        
        await interaction.response.send_message(
            "Select a map to remove:",
            view=RemoveMapView(self.cog, maps),
            ephemeral=True
        )
    
    @discord.ui.button(label="Toggle Map", style=discord.ButtonStyle.secondary)
    async def toggle_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        maps = await self.cog.get_maps(interaction.guild_id)
        if not maps:
            await interaction.response.send_message("❌ No maps to toggle.", ephemeral=True)
            return
        
        await interaction.response.send_message(
            "Select a map to toggle:",
            view=ToggleMapView(self.cog, maps),
            ephemeral=True
        )


class AddMapModal(discord.ui.Modal, title="Add Map"):
    """Modal to add a new map."""
    
    map_name = discord.ui.TextInput(
        label="Map Name",
        placeholder="Enter the map name (e.g., Haven)",
        required=True,
        max_length=50
    )
    
    def __init__(self, cog: "MapBanCog"):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        map_name = self.map_name.value.strip()
        success = await self.cog.add_map(interaction.guild_id, map_name)
        
        if success:
            await interaction.response.send_message(f"✅ Added map: **{map_name}**", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ Map **{map_name}** already exists.", ephemeral=True)


class RemoveMapView(discord.ui.View):
    """View for selecting a map to remove."""
    
    def __init__(self, cog: "MapBanCog", maps: List[Dict]):
        super().__init__(timeout=60)
        self.cog = cog
        
        options = [discord.SelectOption(label=m["map_name"], value=m["map_name"]) for m in maps[:25]]
        self.select = discord.ui.Select(placeholder="Select map to remove", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)
    
    async def select_callback(self, interaction: discord.Interaction):
        map_name = self.select.values[0]
        
        # Confirmation
        embed = discord.Embed(
            title="⚠️ Confirm Removal",
            description=f"Are you sure you want to remove **{map_name}**?",
            color=COLOR_WARNING
        )
        await interaction.response.edit_message(embed=embed, view=ConfirmRemoveMapView(self.cog, map_name))


class ConfirmRemoveMapView(discord.ui.View):
    """Confirmation view for map removal."""
    
    def __init__(self, cog: "MapBanCog", map_name: str):
        super().__init__(timeout=30)
        self.cog = cog
        self.map_name = map_name
    
    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.remove_map(interaction.guild_id, self.map_name)
        await interaction.response.edit_message(
            content=f"✅ Removed map: **{self.map_name}**",
            embed=None,
            view=None
        )
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", embed=None, view=None)


class ToggleMapView(discord.ui.View):
    """View for toggling map enabled status."""
    
    def __init__(self, cog: "MapBanCog", maps: List[Dict]):
        super().__init__(timeout=60)
        self.cog = cog
        
        options = []
        for m in maps[:25]:
            status = "✅" if m["enabled"] else "❌"
            options.append(discord.SelectOption(
                label=f"{status} {m['map_name']}",
                value=m["map_name"]
            ))
        
        self.select = discord.ui.Select(placeholder="Select map to toggle", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)
    
    async def select_callback(self, interaction: discord.Interaction):
        map_name = self.select.values[0]
        new_state = await self.cog.toggle_map(interaction.guild_id, map_name)
        status = "enabled" if new_state else "disabled"
        await interaction.response.edit_message(content=f"✅ **{map_name}** is now {status}.", view=None)


class MapImageManagementView(discord.ui.View):
    """View for managing map images."""
    
    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=300)
        self.cog = cog
    
    @discord.ui.button(label="Set Image URL", style=discord.ButtonStyle.primary)
    async def set_image(self, interaction: discord.Interaction, button: discord.ui.Button):
        maps = await self.cog.get_maps(interaction.guild_id)
        if not maps:
            await interaction.response.send_message("❌ No maps configured.", ephemeral=True)
            return
        
        await interaction.response.send_message(
            "Select a map to set image for:",
            view=SelectMapForImageView(self.cog, maps),
            ephemeral=True
        )
    
    @discord.ui.button(label="View Images", style=discord.ButtonStyle.secondary)
    async def view_images(self, interaction: discord.Interaction, button: discord.ui.Button):
        maps = await self.cog.get_maps(interaction.guild_id)
        
        embed = discord.Embed(title="🖼️ Map Images", color=COLOR_PRIMARY)
        for m in maps:
            url = m.get("map_image_url") or "*Not set*"
            if len(url) > 50 and url != "*Not set*":
                url = url[:47] + "..."
            embed.add_field(name=m["map_name"], value=url, inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)


class SelectMapForImageView(discord.ui.View):
    """View for selecting map to set image."""

    def __init__(self, cog: "MapBanCog", maps: List[Dict]):
        super().__init__(timeout=60)
        self.cog = cog

        options = [discord.SelectOption(label=m["map_name"], value=m["map_name"]) for m in maps[:25]]
        self.select = discord.ui.Select(placeholder="Select map", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        map_name = self.select.values[0]
        await interaction.response.send_modal(SetMapImageModal(self.cog, map_name))


class SetMapImageModal(discord.ui.Modal, title="Set Map Image"):
    """Modal to set a map's image URL."""

    image_url = discord.ui.TextInput(
        label="Image URL",
        placeholder="Enter the image URL",
        required=True,
        style=discord.TextStyle.paragraph
    )

    def __init__(self, cog: "MapBanCog", map_name: str):
        super().__init__()
        self.cog = cog
        self.map_name = map_name

    async def on_submit(self, interaction: discord.Interaction):
        url = self.image_url.value.strip()
        await self.cog.set_map_image(interaction.guild_id, self.map_name, url)
        await interaction.response.send_message(
            f"✅ Set image URL for **{self.map_name}**",
            ephemeral=True
        )


# =============================================================================
# VIEWS & MODALS - AGENT MANAGEMENT
# =============================================================================

def build_agent_management_embed(agents: List[Dict]) -> discord.Embed:
    """Build the agent management embed."""
    embed = discord.Embed(
        title="🎭 Manage Agents",
        description="Add, remove, or toggle agents for the agent pool.",
        color=COLOR_PRIMARY
    )

    if agents:
        enabled = [a["agent_name"] for a in agents if a["enabled"]]
        disabled = [a["agent_name"] for a in agents if not a["enabled"]]

        if enabled:
            embed.add_field(name="✅ Enabled", value="\n".join(enabled[:20]) + (f"\n*+{len(enabled)-20} more*" if len(enabled) > 20 else ""), inline=True)
        if disabled:
            embed.add_field(name="❌ Disabled", value="\n".join(disabled[:20]) + (f"\n*+{len(disabled)-20} more*" if len(disabled) > 20 else ""), inline=True)
    else:
        embed.add_field(name="Agents", value="*No agents configured*", inline=False)

    return embed


class AgentManagementView(discord.ui.View):
    """View for managing agents."""

    def __init__(self, cog: "MapBanCog"):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(label="Add Agents", style=discord.ButtonStyle.success)
    async def add_agents(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddAgentsModal(self.cog))

    @discord.ui.button(label="Remove Agent", style=discord.ButtonStyle.danger)
    async def remove_agent(self, interaction: discord.Interaction, button: discord.ui.Button):
        agents = await self.cog.get_agents(interaction.guild_id)
        if not agents:
            await interaction.response.send_message("❌ No agents to remove.", ephemeral=True)
            return
        await interaction.response.send_message("Select agent to remove:", view=RemoveAgentView(self.cog, agents), ephemeral=True)

    @discord.ui.button(label="Toggle Agent", style=discord.ButtonStyle.secondary)
    async def toggle_agent(self, interaction: discord.Interaction, button: discord.ui.Button):
        agents = await self.cog.get_agents(interaction.guild_id)
        if not agents:
            await interaction.response.send_message("❌ No agents to toggle.", ephemeral=True)
            return
        await interaction.response.send_message("Select agent to toggle:", view=ToggleAgentView(self.cog, agents), ephemeral=True)


class AddAgentsModal(discord.ui.Modal, title="Add Agents"):
    """Modal to add agents (comma-separated)."""

    agent_names = discord.ui.TextInput(
        label="Agent Names",
        placeholder="Jett, Reyna, Sage, Phoenix...",
        required=True,
        style=discord.TextStyle.paragraph
    )

    def __init__(self, cog: "MapBanCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        names = [n.strip() for n in re.split(r'[,\n]+', self.agent_names.value) if n.strip()]
        added = await self.cog.add_agents(interaction.guild_id, names)
        await interaction.response.send_message(f"✅ Added {added} agent(s)", ephemeral=True)


class RemoveAgentView(discord.ui.View):
    """View for selecting agent to remove."""

    def __init__(self, cog: "MapBanCog", agents: List[Dict]):
        super().__init__(timeout=60)
        self.cog = cog
        options = [discord.SelectOption(label=a["agent_name"], value=a["agent_name"]) for a in agents[:25]]
        self.select = discord.ui.Select(placeholder="Select agent", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        agent_name = self.select.values[0]
        await self.cog.remove_agent(interaction.guild_id, agent_name)
        await interaction.response.edit_message(content=f"✅ Removed **{agent_name}**", view=None)


class ToggleAgentView(discord.ui.View):
    """View for toggling agent enabled status."""

    def __init__(self, cog: "MapBanCog", agents: List[Dict]):
        super().__init__(timeout=60)
        self.cog = cog
        options = [discord.SelectOption(label=f"{'✅' if a['enabled'] else '❌'} {a['agent_name']}", value=a["agent_name"]) for a in agents[:25]]
        self.select = discord.ui.Select(placeholder="Select agent", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        agent_name = self.select.values[0]
        new_state = await self.cog.toggle_agent(interaction.guild_id, agent_name)
        status = "enabled" if new_state else "disabled"
        await interaction.response.edit_message(content=f"✅ **{agent_name}** is now {status}", view=None)


# =============================================================================
# VIEWS - CAPTAIN INTERACTION (MAP SELECTION, SIDE SELECTION, READY)
# =============================================================================

class ReadyUpView(discord.ui.View):
    """View for captains to ready up. Uses unique custom_id per session.
    Interaction handling is done via the cog's on_interaction listener."""

    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Ready",
            style=discord.ButtonStyle.success,
            custom_id=f"mapban_ready_{session_id}"
        ))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return False  # Handled by on_interaction listener


class MapSelectView(discord.ui.View):
    """View for selecting a map to ban/pick."""

    def __init__(self, cog: "MapBanCog", session_id: str, captain_id: int,
                 remaining_maps: List[str], action_type: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.session_id = session_id
        self.captain_id = captain_id
        self.action_type = action_type
        self.pending_selection: Optional[str] = None

        # Sort alphabetically
        sorted_maps = sorted(remaining_maps)

        # Create buttons for each map.
        # Discord allows max 5 rows (0-4) and max 5 buttons per row.
        # Calculate buttons_per_row to fit within 5 rows.
        maps_count = len(sorted_maps)
        buttons_per_row = max(2, (maps_count + 4) // 5)  # ceiling division by 5, min 2
        buttons_per_row = min(buttons_per_row, 5)  # cap at Discord's 5 per row limit

        button_style = discord.ButtonStyle.danger if action_type == "ban" else discord.ButtonStyle.success

        for i, map_name in enumerate(sorted_maps[:25]):  # Discord max 25 components
            button = discord.ui.Button(
                label=map_name,
                style=button_style,
                custom_id=f"{PREFIX_MAP_SELECT}{session_id}_{map_name}",
                row=i // buttons_per_row
            )
            button.callback = self.create_callback(map_name)
            self.add_item(button)
    
    def create_callback(self, map_name: str):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.captain_id:
                await interaction.response.send_message("❌ It's not your turn.", ephemeral=True)
                return

            self.pending_selection = map_name

            # Show confirmation inline by editing the captain message
            embed = discord.Embed(
                title=f"⚠️ Confirm {self.action_type.title()}",
                description=f"Are you sure you want to **{self.action_type}** **{map_name}**?",
                color=COLOR_WARNING
            )
            confirm_view = ConfirmMapSelectView(
                self.cog, self.session_id, self.captain_id,
                map_name, self.action_type
            )
            confirm_view.message = interaction.message
            await interaction.response.edit_message(
                embed=embed,
                view=confirm_view
            )

        return callback


class ConfirmMapSelectView(discord.ui.View):
    """Confirmation view for map selection (inline on captain message)."""

    def __init__(self, cog: "MapBanCog", session_id: str, captain_id: int,
                 map_name: str, action_type: str):
        super().__init__(timeout=30)
        self.cog = cog
        self.session_id = session_id
        self.captain_id = captain_id
        self.map_name = map_name
        self.action_type = action_type
        self.message: Optional[discord.Message] = None

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check turn and phase are still valid before proceeding
        session = self.cog.active_sessions.get(self.session_id)
        if not session:
            session = await self.cog.get_session(self.session_id)
        if not session or session.get("current_turn") != self.captain_id:
            await interaction.response.edit_message(
                content="⏰ Your turn expired — a map was auto-selected for you.",
                embed=None, view=None
            )
            return
        current_phase = session.get("current_phase")
        if current_phase not in ("banning", "picking"):
            await interaction.response.edit_message(
                content="⏰ Phase changed — please use the updated buttons.",
                embed=None, view=None
            )
            return
        await interaction.response.defer()
        # Pass phase-derived action_type to handle_map_selection (it will
        # also verify, but this avoids unnecessary lock acquisition)
        action_type = "ban" if current_phase == "banning" else "pick"
        await self.cog.handle_map_selection(
            interaction, self.session_id, self.captain_id,
            self.map_name, action_type
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._restore_original_view(interaction)

    async def on_timeout(self):
        """Restore the original map select view when confirmation times out."""
        await self._restore_original_view(None)

    async def _restore_original_view(self, interaction: Optional[discord.Interaction]):
        """Restore the captain embed with map select buttons."""
        session = self.cog.active_sessions.get(self.session_id)
        if not session:
            session = await self.cog.get_session(self.session_id)
        if not session:
            if interaction and not interaction.response.is_done():
                await interaction.response.edit_message(content="Session not found.", embed=None, view=None)
            return

        # If the turn already moved on (timeout auto-selected), don't fight with update_all_embeds
        if session.get("current_turn") != self.captain_id:
            return

        is_captain1 = self.captain_id == session["captain1_id"]
        embed = build_captain_embed(session, is_captain1=is_captain1)

        actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
        map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
        remaining = get_remaining_maps(map_pool, actions)

        view = MapSelectView(self.cog, self.session_id, self.captain_id, remaining, self.action_type)

        if interaction and not interaction.response.is_done():
            await interaction.response.edit_message(embed=embed, view=view)
        elif self.message:
            try:
                await self.message.edit(embed=embed, view=view)
            except Exception:
                pass


class SideSelectView(discord.ui.View):
    """View for selecting starting side. Uses unique custom_id per session.
    Interaction handling is done via the cog's on_interaction listener."""

    def __init__(self, session_id: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Attack",
            style=discord.ButtonStyle.primary,
            custom_id=f"mapban_side_attack_{session_id}"
        ))
        self.add_item(discord.ui.Button(
            label="Defense",
            style=discord.ButtonStyle.primary,
            custom_id=f"mapban_side_defense_{session_id}"
        ))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return False  # Handled by on_interaction listener


class ConfirmSideSelectView(discord.ui.View):
    """Confirmation view for side selection (inline on captain message)."""

    def __init__(self, cog: "MapBanCog", session_id: str, captain_id: int,
                 map_name: str, side: str):
        super().__init__(timeout=30)
        self.cog = cog
        self.session_id = session_id
        self.captain_id = captain_id
        self.map_name = map_name
        self.side = side
        self.message: Optional[discord.Message] = None

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check turn is still valid before proceeding
        session = self.cog.active_sessions.get(self.session_id)
        if not session:
            session = await self.cog.get_session(self.session_id)
        if not session or session.get("current_turn") != self.captain_id:
            await interaction.response.edit_message(
                content="⏰ Your turn expired — a side was auto-selected for you.",
                embed=None, view=None
            )
            return
        await interaction.response.defer()
        await self.cog.handle_side_selection(
            interaction, self.session_id, self.captain_id,
            self.map_name, self.side
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._restore_original_view(interaction)

    async def on_timeout(self):
        """Restore the original side select view when confirmation times out."""
        await self._restore_original_view(None)

    async def _restore_original_view(self, interaction: Optional[discord.Interaction]):
        """Restore the captain embed with side select buttons."""
        session = self.cog.active_sessions.get(self.session_id)
        if not session:
            session = await self.cog.get_session(self.session_id)
        if not session:
            if interaction and not interaction.response.is_done():
                await interaction.response.edit_message(content="Session not found.", embed=None, view=None)
            return

        # If the turn already moved on (timeout auto-selected), don't fight with update_all_embeds
        if session.get("current_turn") != self.captain_id:
            return

        is_captain1 = self.captain_id == session["captain1_id"]
        embed = build_captain_embed(session, is_captain1=is_captain1)
        view = SideSelectView(self.session_id)

        if interaction and not interaction.response.is_done():
            await interaction.response.edit_message(embed=embed, view=view)
        elif self.message:
            try:
                await self.message.edit(embed=embed, view=view)
            except Exception:
                pass


# =============================================================================
# VIEWS - AGENT SELECTION
# =============================================================================

class AgentSelectView(discord.ui.View):
    """View with a button to open role-based agent selection. Uses unique custom_id per session.
    Interaction handling is done via the cog's on_interaction listener."""

    def __init__(self, session_id: str, action_type: str):
        super().__init__(timeout=None)
        label = f"Select Agent to {action_type.title()}"
        self.add_item(discord.ui.Button(
            label=label,
            style=discord.ButtonStyle.primary,
            custom_id=f"mapban_agent_select_{session_id}"
        ))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return False  # Handled by on_interaction listener


class CancelSessionView(discord.ui.View):
    """View for cancelling active sessions."""
    
    def __init__(self, cog: "MapBanCog", sessions: List[Dict]):
        super().__init__(timeout=120)
        self.cog = cog
        
        options = []
        for s in sessions[:25]:
            format_type = "Bo3" if s["format"] == "bo3" else "Bo1"
            options.append(discord.SelectOption(
                label=f"{s['matchup_name']} ({format_type})",
                value=s["session_id"]
            ))
        
        self.select = discord.ui.Select(placeholder="Select session to cancel", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)
    
    async def select_callback(self, interaction: discord.Interaction):
        session_id = self.select.values[0]
        
        embed = discord.Embed(
            title="⚠️ Confirm Cancellation",
            description="Are you sure you want to cancel this session?\nThis action cannot be undone.",
            color=COLOR_DANGER
        )
        await interaction.response.edit_message(embed=embed, view=ConfirmCancelSessionView(self.cog, session_id))


class ConfirmCancelSessionView(discord.ui.View):
    """Confirmation for session cancellation."""
    
    def __init__(self, cog: "MapBanCog", session_id: str):
        super().__init__(timeout=30)
        self.cog = cog
        self.session_id = session_id
    
    @discord.ui.button(label="Confirm Cancel", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.cancel_session(interaction.guild, self.session_id)
        await interaction.response.edit_message(content="✅ Session cancelled.", embed=None, view=None)
    
    @discord.ui.button(label="Go Back", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", embed=None, view=None)


# =============================================================================
# MAIN COG CLASS - PART 1
# =============================================================================

class MapBanCog(commands.Cog):
    """Cog for managing Valorant map bans."""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.active_sessions: Dict[str, Dict] = {}  # In-memory cache
        self.reminder_messages: Dict[str, int] = {}  # session_id -> message_id
        self.timer_update_lock = asyncio.Lock()
        self.session_locks: Dict[str, asyncio.Lock] = {}  # Per-session locks for race condition prevention

    def get_session_lock(self, session_id: str) -> asyncio.Lock:
        """Get or create a lock for a specific session."""
        if session_id not in self.session_locks:
            self.session_locks[session_id] = asyncio.Lock()
        return self.session_locks[session_id]

    def cleanup_session_lock(self, session_id: str):
        """Remove a session lock when session is deleted."""
        if session_id in self.session_locks:
            del self.session_locks[session_id]
    
    async def cog_load(self):
        """Called when the cog is loaded."""
        await init_database()
        await self.load_active_sessions()

        # Register persistent views for bot restart recovery
        # Ready, Side, and Agent views now use dynamic custom_ids and are
        # handled via the on_interaction listener instead of persistent views.
        self.bot.add_view(PersistentMapSelectView(self))
        self.bot.add_view(PersistentObserveView(self))

        self.timer_task.start()
        self.cleanup_task.start()
        print("MapBan cog loaded")
    
    async def cog_unload(self):
        """Called when the cog is unloaded."""
        self.timer_task.cancel()
        self.cleanup_task.cancel()

    # =========================================================================
    # INTERACTION LISTENER (handles ready, side select, agent select buttons)
    # =========================================================================

    def _find_session_by_channel(self, channel_id: int, user_id: int) -> Optional[Dict]:
        """Find an active session where this user is a captain in this thread."""
        for session_id, session in self.active_sessions.items():
            if session.get("status") != "active":
                continue
            if channel_id in (session.get("thread1_id"), session.get("thread2_id")):
                if user_id in (session.get("captain1_id"), session.get("captain2_id")):
                    return session
        return None

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle all mapban button interactions via custom_id routing.

        This replaces the old PersistentReadyView/SideSelectView/AgentSelectView
        approach which used shared static custom_ids across threads, causing
        discord.py's view store to lose track of button handlers.

        We intentionally do NOT check interaction.response.is_done() here because
        after a cog reload, orphaned old view instances may still be in the view
        store and respond first with stale state. We need to always handle mapban
        buttons from this (current) cog regardless.
        """
        if interaction.type != discord.InteractionType.component:
            return

        custom_id = interaction.data.get("custom_id", "")
        if not custom_id.startswith("mapban_"):
            return

        print(f"[MapBan] on_interaction fired: custom_id={custom_id}, "
              f"user={interaction.user.id}, channel={interaction.channel_id}, "
              f"is_done={interaction.response.is_done()}")

        # --- Ready buttons ---
        # New format: "mapban_ready_{session_id}"
        # Old format: "mapban_ready_persistent"
        if custom_id.startswith("mapban_ready_"):
            try:
                session_id = custom_id[len("mapban_ready_"):]

                # Old format - find session by channel
                if session_id == "persistent":
                    session = self._find_session_by_channel(interaction.channel_id, interaction.user.id)
                    if not session:
                        try:
                            await interaction.response.send_message("Session not found.", ephemeral=True)
                        except Exception:
                            pass
                        return
                    session_id = session["session_id"]

                captain_id = interaction.user.id
                session = self.active_sessions.get(session_id)
                if not session:
                    print(f"[MapBan] Session {session_id} not in active_sessions, fetching from DB")
                    session = await self.get_session(session_id)
                if not session:
                    print(f"[MapBan] Session {session_id} not found anywhere!")
                    try:
                        await interaction.response.send_message("Session not found.", ephemeral=True)
                    except Exception:
                        pass
                    return

                if captain_id not in (session.get("captain1_id"), session.get("captain2_id")):
                    print(f"[MapBan] Captain ID mismatch: {captain_id} not in "
                          f"({session.get('captain1_id')}, {session.get('captain2_id')})")
                    try:
                        await interaction.response.send_message("You are not a captain in this session.", ephemeral=True)
                    except Exception:
                        pass
                    return

                print(f"[MapBan] Calling handle_ready for session={session_id}, captain={captain_id}")
                await self.handle_ready(interaction, session_id, captain_id)
            except Exception as e:
                print(f"[MapBan] Ready handler error: {e}\n{traceback.format_exc()}")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("Something went wrong. Please try again.", ephemeral=True)
                except Exception:
                    pass
            return

        # --- Side select buttons ---
        # New format: "mapban_side_attack_{session_id}" / "mapban_side_defense_{session_id}"
        # Old format: "mapban_side_attack" / "mapban_side_defense"
        if custom_id.startswith("mapban_side_attack") or custom_id.startswith("mapban_side_defense"):
            try:
                if custom_id.startswith("mapban_side_attack"):
                    side = "Attack"
                    suffix = custom_id[len("mapban_side_attack"):]
                else:
                    side = "Defense"
                    suffix = custom_id[len("mapban_side_defense"):]

                # Parse session_id (new format has "_{session_id}", old format has nothing)
                if suffix.startswith("_") and len(suffix) > 1:
                    session_id = suffix[1:]  # Strip leading underscore
                else:
                    # Old format - find by channel
                    session = self._find_session_by_channel(interaction.channel_id, interaction.user.id)
                    if not session:
                        try:
                            await interaction.response.send_message("Session not found.", ephemeral=True)
                        except Exception:
                            pass
                        return
                    session_id = session["session_id"]

                session = self.active_sessions.get(session_id)
                if not session:
                    session = await self.get_session(session_id)
                if not session:
                    try:
                        await interaction.response.send_message("Session not found.", ephemeral=True)
                    except Exception:
                        pass
                    return

                captain_id = interaction.user.id
                if captain_id != session.get("current_turn"):
                    try:
                        await interaction.response.send_message("It's not your turn.", ephemeral=True)
                    except Exception:
                        pass
                    return

                current_map = session.get("current_side_select_map", "")
                embed = discord.Embed(
                    title="Confirm Side Selection",
                    description=f"Start on **{side}** for **{current_map}**?",
                    color=COLOR_WARNING
                )
                confirm_view = ConfirmSideSelectView(self, session_id, captain_id, current_map, side)
                confirm_view.message = interaction.message
                await interaction.response.edit_message(
                    embed=embed,
                    view=confirm_view
                )
            except Exception as e:
                print(f"[MapBan] Side select error: {e}\n{traceback.format_exc()}")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("Something went wrong. Please try again.", ephemeral=True)
                except Exception:
                    pass
            return

        # --- Agent select buttons ---
        # New format: "mapban_agent_select_{session_id}"
        # Old format: "mapban_agent_select"
        if custom_id.startswith("mapban_agent_select"):
            try:
                suffix = custom_id[len("mapban_agent_select"):]

                if suffix.startswith("_") and len(suffix) > 1:
                    session_id = suffix[1:]
                else:
                    session = self._find_session_by_channel(interaction.channel_id, interaction.user.id)
                    if not session:
                        try:
                            await interaction.response.send_message("Session not found.", ephemeral=True)
                        except Exception:
                            pass
                        return
                    session_id = session["session_id"]

                session = self.active_sessions.get(session_id)
                if not session:
                    session = await self.get_session(session_id)
                if not session:
                    try:
                        await interaction.response.send_message("Session not found.", ephemeral=True)
                    except Exception:
                        pass
                    return

                captain_id = interaction.user.id
                if captain_id != session.get("current_turn"):
                    try:
                        await interaction.response.send_message("It's not your turn.", ephemeral=True)
                    except Exception:
                        pass
                    return

                phase = session.get("current_phase")
                action_type = "protect" if phase == "agent_protect" else "ban"
                available_agents = self.get_available_agents(session, captain_id, action_type)

                if not available_agents:
                    await interaction.response.send_message(
                        f"No agents available to {action_type}. Waiting for the timer to auto-skip.",
                        ephemeral=True
                    )
                    return

                await interaction.response.send_message(
                    f"Select a role to {action_type} an agent:",
                    view=AgentRoleSelectView(self, session_id, captain_id, action_type, available_agents),
                    ephemeral=True
                )
            except Exception as e:
                print(f"[MapBan] Agent select error: {e}\n{traceback.format_exc()}")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("Something went wrong. Please try again.", ephemeral=True)
                except Exception:
                    pass
            return

    # =========================================================================
    # DATABASE OPERATIONS
    # =========================================================================
    
    async def get_guild_settings(self, guild_id: int) -> Dict:
        """Get settings for a guild."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM guild_settings WHERE guild_id = ?",
                (guild_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                
                # Create default settings
                await db.execute(
                    "INSERT INTO guild_settings (guild_id) VALUES (?)",
                    (guild_id,)
                )
                await db.commit()
                return {"guild_id": guild_id, "spectator_channels": "[]"}
    
    # Whitelist of allowed guild settings columns to prevent SQL injection
    ALLOWED_GUILD_SETTINGS = {
        "admin_role_id", "parent_channel_id", "admin_log_channel_id",
        "spectator_channels", "turn_timeout", "alert_channel_id"
    }

    async def update_guild_setting(self, guild_id: int, key: str, value: Any):
        """Update a guild setting."""
        if key not in self.ALLOWED_GUILD_SETTINGS:
            raise ValueError(f"Invalid guild setting: {key}")

        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                f"UPDATE guild_settings SET {key} = ?, updated_at = CURRENT_TIMESTAMP WHERE guild_id = ?",
                (value, guild_id)
            )
            await db.commit()
    
    async def get_maps(self, guild_id: int) -> List[Dict]:
        """Get all maps for a guild."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM maps WHERE guild_id = ? ORDER BY map_name",
                (guild_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def get_enabled_maps(self, guild_id: int) -> List[str]:
        """Get enabled map names for a guild."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute(
                "SELECT map_name FROM maps WHERE guild_id = ? AND enabled = 1 ORDER BY map_name",
                (guild_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    
    async def add_map(self, guild_id: int, map_name: str) -> bool:
        """Add a map to the guild's pool."""
        try:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute(
                    "INSERT INTO maps (guild_id, map_name) VALUES (?, ?)",
                    (guild_id, map_name)
                )
                await db.commit()
                return True
        except aiosqlite.IntegrityError:
            return False
    
    async def remove_map(self, guild_id: int, map_name: str):
        """Remove a map from the guild's pool."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                "DELETE FROM maps WHERE guild_id = ? AND map_name = ?",
                (guild_id, map_name)
            )
            await db.commit()
    
    async def toggle_map(self, guild_id: int, map_name: str) -> bool:
        """Toggle a map's enabled status. Returns new state."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute(
                "SELECT enabled FROM maps WHERE guild_id = ? AND map_name = ?",
                (guild_id, map_name)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    new_state = 0 if row[0] else 1
                    await db.execute(
                        "UPDATE maps SET enabled = ? WHERE guild_id = ? AND map_name = ?",
                        (new_state, guild_id, map_name)
                    )
                    await db.commit()
                    return bool(new_state)
        return False
    
    async def set_map_image(self, guild_id: int, map_name: str, url: str):
        """Set a map's image URL."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                "UPDATE maps SET map_image_url = ? WHERE guild_id = ? AND map_name = ?",
                (url, guild_id, map_name)
            )
            await db.commit()
    
    async def get_map_image_url(self, guild_id: int, map_name: str) -> Optional[str]:
        """Get a map's image URL."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute(
                "SELECT map_image_url FROM maps WHERE guild_id = ? AND map_name = ?",
                (guild_id, map_name)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    # =========================================================================
    # AGENT DATABASE OPERATIONS
    # =========================================================================

    async def get_agents(self, guild_id: int) -> List[Dict]:
        """Get all agents for a guild."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM agents WHERE guild_id = ? ORDER BY agent_name",
                (guild_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_enabled_agents(self, guild_id: int) -> List[str]:
        """Get enabled agent names for a guild."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute(
                "SELECT agent_name FROM agents WHERE guild_id = ? AND enabled = 1 ORDER BY agent_name",
                (guild_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def add_agents(self, guild_id: int, agent_names: List[str]) -> int:
        """Add multiple agents to the guild's pool. Returns count of agents added."""
        added = 0
        async with aiosqlite.connect(DATABASE_PATH) as db:
            for name in agent_names:
                name = name.strip()
                if not name:
                    continue
                try:
                    await db.execute(
                        "INSERT INTO agents (guild_id, agent_name) VALUES (?, ?)",
                        (guild_id, name)
                    )
                    added += 1
                except aiosqlite.IntegrityError:
                    pass  # Agent already exists
            await db.commit()
        return added

    async def remove_agent(self, guild_id: int, agent_name: str):
        """Remove an agent from the guild's pool."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                "DELETE FROM agents WHERE guild_id = ? AND agent_name = ?",
                (guild_id, agent_name)
            )
            await db.commit()

    async def toggle_agent(self, guild_id: int, agent_name: str) -> bool:
        """Toggle an agent's enabled status. Returns new state."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute(
                "SELECT enabled FROM agents WHERE guild_id = ? AND agent_name = ?",
                (guild_id, agent_name)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    new_state = 0 if row[0] else 1
                    await db.execute(
                        "UPDATE agents SET enabled = ? WHERE guild_id = ? AND agent_name = ?",
                        (new_state, guild_id, agent_name)
                    )
                    await db.commit()
                    return bool(new_state)
        return False

    async def set_agent_image(self, guild_id: int, agent_name: str, url: str):
        """Set an agent's image URL."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute(
                "UPDATE agents SET agent_image_url = ? WHERE guild_id = ? AND agent_name = ?",
                (url, guild_id, agent_name)
            )
            await db.commit()

    # =========================================================================
    # AGENT SELECTION LOGIC
    # =========================================================================

    def get_available_agents(self, session: Dict, captain_id: int, action_type: str) -> List[str]:
        """Get agents available for a captain to protect or ban."""
        agent_pool = json.loads(session.get("agent_pool", "[]")) if isinstance(session.get("agent_pool"), str) else session.get("agent_pool", [])

        if action_type == "protect":
            return self._get_available_protects(session, captain_id, agent_pool)
        else:
            return self._get_available_bans(session, captain_id, agent_pool)

    def _get_available_protects(self, session: Dict, captain_id: int, agent_pool: List[str]) -> List[str]:
        """Get agents available for protection."""
        used_protects = json.loads(session.get("used_protects", "{}")) if isinstance(session.get("used_protects"), str) else session.get("used_protects", {})
        agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})

        # Get current map
        current_map_index = session.get("current_agent_map_index", 0)
        maps_to_play = self._get_maps_to_play(session)
        current_map = maps_to_play[current_map_index] if current_map_index < len(maps_to_play) else ""

        # Agents this captain has already protected (can't protect again)
        captain_used = used_protects.get(str(captain_id), [])

        # Agents already protected on this map by the other captain
        other_captain_protect = None
        if current_map in agent_protects:
            map_protects = agent_protects[current_map]
            for key, value in map_protects.items():
                if int(key) != captain_id:
                    other_captain_protect = value

        available = []
        for agent in agent_pool:
            if agent in captain_used:
                continue
            if agent == other_captain_protect:
                continue
            available.append(agent)

        return available

    def _get_available_bans(self, session: Dict, captain_id: int, agent_pool: List[str]) -> List[str]:
        """Get agents available for banning."""
        used_bans = json.loads(session.get("used_bans", "{}")) if isinstance(session.get("used_bans"), str) else session.get("used_bans", {})
        agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
        agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})

        # Get current map
        current_map_index = session.get("current_agent_map_index", 0)
        maps_to_play = self._get_maps_to_play(session)
        current_map = maps_to_play[current_map_index] if current_map_index < len(maps_to_play) else ""

        # Agents this captain has already banned (can't ban again)
        captain_used = used_bans.get(str(captain_id), [])

        # Protected agents on this map (can't be banned)
        protected_on_map = []
        if current_map in agent_protects:
            protected_on_map = list(agent_protects[current_map].values())

        # Agent already banned on this map by other captain
        other_captain_ban = None
        if current_map in agent_bans:
            map_bans = agent_bans[current_map]
            for key, value in map_bans.items():
                if int(key) != captain_id:
                    other_captain_ban = value

        available = []
        for agent in agent_pool:
            if agent in captain_used:
                continue
            if agent in protected_on_map:
                continue
            if agent == other_captain_ban:
                continue
            available.append(agent)

        return available

    def _get_maps_to_play(self, session: Dict) -> List[str]:
        """Get the ordered list of maps that will be played."""
        actions = json.loads(session.get("actions", "[]")) if isinstance(session.get("actions"), str) else session.get("actions", [])
        map_pool = json.loads(session.get("map_pool", "[]")) if isinstance(session.get("map_pool"), str) else session.get("map_pool", [])
        side_selections = json.loads(session.get("side_selections", "{}")) if isinstance(session.get("side_selections"), str) else session.get("side_selections", {})

        # Get picked maps in order
        picked = [a["map"] for a in actions if a.get("type") == "pick"]

        # Get decider (remaining map)
        remaining = get_remaining_maps(map_pool, actions)

        # Combine: picked maps + decider
        maps_to_play = picked.copy()
        if remaining:
            maps_to_play.append(remaining[0])

        return maps_to_play

    async def save_session(self, session: Dict):
        """Save a session to the database."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                INSERT OR REPLACE INTO sessions (
                    session_id, guild_id, matchup_name, format, captain1_id, captain2_id,
                    admin_id, thread1_id, thread2_id, captain1_msg_id, captain2_msg_id,
                    spectator_messages, admin_log_msg_id, first_ban, decider_side,
                    current_turn, current_phase, map_pool, actions, picked_maps,
                    side_selections, captain1_ready, captain2_ready, scheduled_time,
                    turn_start_time, reminder_sent, status, complete_time,
                    current_side_select_map, agent_pool, agent_protects, agent_bans,
                    used_protects, used_bans, current_agent_phase, current_agent_map_index,
                    captain1_name, captain2_name, team1_name, team2_name, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                session["session_id"],
                session["guild_id"],
                session["matchup_name"],
                session["format"],
                session["captain1_id"],
                session["captain2_id"],
                session["admin_id"],
                session.get("thread1_id"),
                session.get("thread2_id"),
                session.get("captain1_msg_id"),
                session.get("captain2_msg_id"),
                json.dumps(session.get("spectator_messages", [])),
                session.get("admin_log_msg_id"),
                session["first_ban"],
                session.get("decider_side", "opponent"),
                session.get("current_turn"),
                session.get("current_phase", "ready"),
                json.dumps(session["map_pool"]) if isinstance(session["map_pool"], list) else session["map_pool"],
                json.dumps(session.get("actions", [])) if isinstance(session.get("actions", []), list) else session.get("actions", "[]"),
                json.dumps(session.get("picked_maps", [])) if isinstance(session.get("picked_maps", []), list) else session.get("picked_maps", "[]"),
                json.dumps(session.get("side_selections", {})) if isinstance(session.get("side_selections", {}), dict) else session.get("side_selections", "{}"),
                session.get("captain1_ready", 0),
                session.get("captain2_ready", 0),
                session.get("scheduled_time"),
                session.get("turn_start_time"),
                session.get("reminder_sent", 0),
                session.get("status", "active"),
                session.get("complete_time"),
                session.get("current_side_select_map"),
                json.dumps(session.get("agent_pool", [])) if isinstance(session.get("agent_pool", []), list) else session.get("agent_pool", "[]"),
                json.dumps(session.get("agent_protects", {})) if isinstance(session.get("agent_protects", {}), dict) else session.get("agent_protects", "{}"),
                json.dumps(session.get("agent_bans", {})) if isinstance(session.get("agent_bans", {}), dict) else session.get("agent_bans", "{}"),
                json.dumps(session.get("used_protects", {})) if isinstance(session.get("used_protects", {}), dict) else session.get("used_protects", "{}"),
                json.dumps(session.get("used_bans", {})) if isinstance(session.get("used_bans", {}), dict) else session.get("used_bans", "{}"),
                session.get("current_agent_phase"),
                session.get("current_agent_map_index", 0),
                session.get("captain1_name"),
                session.get("captain2_name"),
                session.get("team1_name"),
                session.get("team2_name")
            ))
            await db.commit()
    
    async def get_session(self, session_id: str) -> Optional[Dict]:
        """Get a session from database."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM sessions WHERE session_id = ?",
                (session_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
        return None
    
    async def get_active_sessions(self, guild_id: int) -> List[Dict]:
        """Get all active sessions for a guild."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM sessions WHERE guild_id = ? AND status = 'active'",
                (guild_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def delete_session(self, session_id: str):
        """Delete a session from database."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
            await db.commit()

        if session_id in self.active_sessions:
            del self.active_sessions[session_id]

        # Clean up the session lock
        self.cleanup_session_lock(session_id)
    
    async def load_active_sessions(self):
        """Load active sessions into memory on startup."""
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM sessions WHERE status = 'active'"
            ) as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    session = dict(row)
                    self.active_sessions[session["session_id"]] = session
    
    # =========================================================================
    # SESSION MANAGEMENT
    # =========================================================================
    
    async def create_session(self, guild: discord.Guild, matchup_name: str,
                            format_type: str, captain1: discord.Member,
                            captain2: discord.Member, admin: discord.Member,
                            first_ban: str = "random", decider_side: str = "opponent",
                            team1_name: str = None, team2_name: str = None) -> Optional[str]:
        """Create a new map ban session."""
        settings = await self.get_guild_settings(guild.id)
        
        # Check required settings
        if not settings.get("parent_channel_id"):
            return None
        
        parent_channel = guild.get_channel(settings["parent_channel_id"])
        if not parent_channel:
            return None
        
        # Get enabled maps
        map_pool = await self.get_enabled_maps(guild.id)
        min_maps = 5 if format_type == "bo3" else 3
        if len(map_pool) < min_maps:
            return None

        # Get enabled agents (optional - session can work without them)
        agent_pool = await self.get_enabled_agents(guild.id)

        # Generate session ID
        session_id = generate_session_id()
        
        # Determine first ban
        if first_ban == "random":
            first_ban = random.choice(["captain1", "captain2"])
        
        first_captain_id = captain1.id if first_ban == "captain1" else captain2.id
        
        # Create session data
        session = {
            "session_id": session_id,
            "guild_id": guild.id,
            "matchup_name": matchup_name,
            "format": format_type,
            "captain1_id": captain1.id,
            "captain2_id": captain2.id,
            "captain1_name": captain1.display_name,
            "captain2_name": captain2.display_name,
            "admin_id": admin.id,
            "first_ban": first_ban,
            "decider_side": decider_side,
            "current_turn": first_captain_id,
            "current_phase": "ready",
            "map_pool": map_pool,
            "actions": [],
            "picked_maps": [],
            "side_selections": {},
            "captain1_ready": 0,
            "captain2_ready": 0,
            "status": "active",
            "spectator_messages": [],
            "turn_start_time": datetime.now(timezone.utc).isoformat(),
            # Agent selection fields
            "agent_pool": agent_pool,
            "agent_protects": {},
            "agent_bans": {},
            "used_protects": {},
            "used_bans": {},
            "current_agent_phase": None,
            "current_agent_map_index": 0,
            "team1_name": team1_name,
            "team2_name": team2_name
        }
        
        # Create private threads
        thread1_name = f"{matchup_name} - {captain1.display_name}"[:100]
        thread2_name = f"{matchup_name} - {captain2.display_name}"[:100]
        
        thread1 = await parent_channel.create_thread(
            name=thread1_name,
            type=discord.ChannelType.private_thread,
            auto_archive_duration=60
        )
        
        thread2 = await parent_channel.create_thread(
            name=thread2_name,
            type=discord.ChannelType.private_thread,
            auto_archive_duration=60
        )
        
        session["thread1_id"] = thread1.id
        session["thread2_id"] = thread2.id
        
        # Add captains to threads
        await thread1.add_user(captain1)
        await thread2.add_user(captain2)

        # Send alert to alert channel
        alert_channel_id = settings.get("alert_channel_id")
        if alert_channel_id:
            alert_channel = guild.get_channel(alert_channel_id)
            if alert_channel:
                try:
                    alert_embed = discord.Embed(
                        title="New Map Ban Session",
                        description=f"**{captain1.display_name}** vs **{captain2.display_name}**",
                        color=COLOR_PRIMARY
                    )
                    alert_embed.add_field(
                        name=f"{captain1.display_name} Thread",
                        value=f"[Jump to Thread]({thread1.jump_url})",
                        inline=True
                    )
                    alert_embed.add_field(
                        name=f"{captain2.display_name} Thread",
                        value=f"[Jump to Thread]({thread2.jump_url})",
                        inline=True
                    )
                    await alert_channel.send(embed=alert_embed)
                except Exception:
                    pass

        # Send captain embeds
        embed1 = build_captain_embed(session, is_captain1=True)
        embed2 = build_captain_embed(session, is_captain1=False)
        
        msg1 = await thread1.send(
            content=f"{captain1.mention} Ready up!",
            embed=embed1,
            view=ReadyUpView(session_id)
        )

        msg2 = await thread2.send(
            content=f"{captain2.mention} Ready up!",
            embed=embed2,
            view=ReadyUpView(session_id)
        )
        
        session["captain1_msg_id"] = msg1.id
        session["captain2_msg_id"] = msg2.id
        
        # Send spectator embeds
        spectator_channels = json.loads(settings.get("spectator_channels", "[]"))
        spectator_messages = []
        
        for channel_id in spectator_channels:
            channel = guild.get_channel_or_thread(channel_id)
            if channel:
                try:
                    spec_embed = build_spectator_embed(session)
                    spec_msg = await channel.send(embed=spec_embed)
                    spectator_messages.append({"channel_id": channel_id, "message_id": spec_msg.id})
                except Exception:
                    pass
        
        session["spectator_messages"] = spectator_messages

        # Save session
        await self.save_session(session)
        self.active_sessions[session_id] = session
        
        return session_id
    
    async def cancel_session(self, guild: discord.Guild, session_id: str):
        """Cancel an active session."""
        session = await self.get_session(session_id)
        if not session:
            return
        
        # Delete threads
        thread1 = await self._get_thread(guild, session.get("thread1_id"))
        thread2 = await self._get_thread(guild, session.get("thread2_id"))

        if thread1:
            try:
                await thread1.delete()
            except Exception:
                pass

        if thread2:
            try:
                await thread2.delete()
            except Exception:
                pass
        
        # Delete spectator messages
        spectator_messages = json.loads(session.get("spectator_messages", "[]"))
        for spec in spectator_messages:
            channel = guild.get_channel_or_thread(spec["channel_id"])
            if channel:
                try:
                    msg = await channel.fetch_message(spec["message_id"])
                    await msg.delete()
                except Exception:
                    pass
        
        # Delete from database
        await self.delete_session(session_id)
    
    # =========================================================================
    # GAME LOGIC HANDLERS
    # =========================================================================
    
    async def handle_ready(self, interaction: discord.Interaction, session_id: str, captain_id: int):
        """Handle a captain readying up."""
        print(f"[MapBan] handle_ready START: session={session_id}, captain={captain_id}, "
              f"is_done={interaction.response.is_done()}")

        async with self.get_session_lock(session_id):
            session = self.active_sessions.get(session_id)
            if not session:
                session = await self.get_session(session_id)
                if not session:
                    print(f"[MapBan] handle_ready: session {session_id} not found")
                    return

            # Mark ready
            if captain_id == session["captain1_id"]:
                session["captain1_ready"] = 1
                is_captain1 = True
            else:
                session["captain2_ready"] = 1
                is_captain1 = False

            # Check if both ready
            if session["captain1_ready"] and session["captain2_ready"]:
                session["current_phase"] = "banning"
                session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
                session["reminder_sent"] = 0

            # Save and update cache
            self.active_sessions[session_id] = session
            await self.save_session(session)

        print(f"[MapBan] handle_ready: state updated - phase={session.get('current_phase')}, "
              f"c1_ready={session.get('captain1_ready')}, c2_ready={session.get('captain2_ready')}")

        # Build embed for the captain who clicked, and respond immediately
        # using edit_message (type 7) instead of defer+edit to guarantee the
        # Discord client displays the update.
        settings = await self.get_guild_settings(interaction.guild.id)
        turn_timeout = settings.get("turn_timeout", DEFAULT_TURN_TIMEOUT)
        time_remaining = None
        turn_start = session.get("turn_start_time")
        if turn_start:
            if isinstance(turn_start, str):
                turn_start_dt = datetime.fromisoformat(turn_start.replace("Z", "+00:00"))
            else:
                turn_start_dt = turn_start
            elapsed = (datetime.now(timezone.utc) - turn_start_dt).total_seconds()
            time_remaining = max(0, int(turn_timeout - elapsed))

        embed = build_captain_embed(session, is_captain1=is_captain1, time_remaining=time_remaining)

        # Determine the view for the clicked captain
        phase = session.get("current_phase", "ready")
        cap_id = session["captain1_id"] if is_captain1 else session["captain2_id"]
        actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
        map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
        remaining = get_remaining_maps(map_pool, actions)

        view = None
        if phase == "ready":
            cap_ready = session.get("captain1_ready") if is_captain1 else session.get("captain2_ready")
            if not cap_ready:
                view = ReadyUpView(session_id)
        elif phase in ("banning", "picking") and session["current_turn"] == cap_id:
            action_type = "ban" if phase == "banning" else "pick"
            view = MapSelectView(self, session_id, cap_id, remaining, action_type)
        elif phase == "side_select" and session["current_turn"] == cap_id:
            view = SideSelectView(session_id)
        elif phase in ("agent_protect", "agent_ban") and session["current_turn"] == cap_id:
            action_type = "protect" if phase == "agent_protect" else "ban"
            view = AgentSelectView(session_id, action_type)

        # Respond to the interaction by directly editing the clicked captain's message
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
                print(f"[MapBan] handle_ready: edit_message SUCCESS for captain {captain_id}")
            else:
                # Interaction already responded to (possibly by orphaned view) - use followup
                print(f"[MapBan] handle_ready: interaction already done, trying edit_original_response")
                await interaction.edit_original_response(embed=embed, view=view)
                print(f"[MapBan] handle_ready: edit_original_response SUCCESS for captain {captain_id}")
        except Exception as e:
            print(f"[MapBan] handle_ready: FAILED to edit clicked captain's message: {e}\n"
                  f"{traceback.format_exc()}")

        # Update both captain embeds and spectators via msg.edit()
        # (redundant for the clicked captain but serves as backup)
        await self.update_all_embeds(interaction.guild, session)
    
    async def handle_map_selection(self, interaction: discord.Interaction, session_id: str,
                                    captain_id: int, map_name: str, action_type: str):
        """Handle a map being selected (banned or picked)."""
        async with self.get_session_lock(session_id):
            session = self.active_sessions.get(session_id)
            if not session:
                session = await self.get_session(session_id)
                if not session:
                    return

            # Verify it's this captain's turn
            if session["current_turn"] != captain_id:
                return

            # Verify action_type matches the current phase.
            # A stale view (e.g. from a cog reload or race with update_all_embeds)
            # could pass the wrong action_type captured when the view was created.
            current_phase = session.get("current_phase")
            if current_phase == "banning":
                action_type = "ban"
            elif current_phase == "picking":
                action_type = "pick"
            else:
                # Phase is not banning or picking — should not be selecting maps
                print(f"[MapBan] handle_map_selection: unexpected phase '{current_phase}' "
                      f"for session {session_id}, ignoring")
                return

            # Get captain name
            if captain_id == session["captain1_id"]:
                captain_name = session.get("captain1_name", "Captain 1")
            else:
                captain_name = session.get("captain2_name", "Captain 2")

            # Record action
            actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
            actions.append({
                "type": action_type,
                "map": map_name,
                "captain_id": captain_id,
                "captain_name": captain_name,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            session["actions"] = actions

            # Track picked maps
            if action_type == "pick":
                picked = json.loads(session["picked_maps"]) if isinstance(session["picked_maps"], str) else session["picked_maps"]
                picked.append(map_name)
                session["picked_maps"] = picked

            # Delete reminder message if exists
            if session_id in self.reminder_messages:
                try:
                    thread_id = session["thread1_id"] if captain_id == session["captain1_id"] else session["thread2_id"]
                    thread = await self._get_thread(interaction.guild, thread_id)
                    if thread:
                        msg = await thread.fetch_message(self.reminder_messages[session_id])
                        await msg.delete()
                except Exception:
                    pass
                del self.reminder_messages[session_id]

            # Determine next phase/turn
            await self.advance_session(interaction.guild, session)

            # Save and update cache
            self.active_sessions[session_id] = session
            await self.save_session(session)

        # Update all embeds (outside lock)
        await self.update_all_embeds(interaction.guild, session)
    
    async def handle_side_selection(self, interaction: discord.Interaction, session_id: str,
                                     captain_id: int, map_name: str, side: str):
        """Handle a side being selected for a map."""
        async with self.get_session_lock(session_id):
            session = self.active_sessions.get(session_id)
            if not session:
                session = await self.get_session(session_id)
                if not session:
                    return

            # Get captain name
            if captain_id == session["captain1_id"]:
                captain_name = session.get("captain1_name", "Captain 1")
            else:
                captain_name = session.get("captain2_name", "Captain 2")

            # Record side selection
            side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
            side_selections[map_name] = {
                "side": side,
                "chosen_by": captain_id,
                "chosen_by_name": captain_name
            }
            session["side_selections"] = side_selections

            # Delete reminder if exists
            if session_id in self.reminder_messages:
                try:
                    thread_id = session["thread1_id"] if captain_id == session["captain1_id"] else session["thread2_id"]
                    thread = await self._get_thread(interaction.guild, thread_id)
                    if thread:
                        msg = await thread.fetch_message(self.reminder_messages[session_id])
                        await msg.delete()
                except Exception:
                    pass
                del self.reminder_messages[session_id]

            # Advance session
            await self.advance_session(interaction.guild, session)

            # Save and update cache
            self.active_sessions[session_id] = session
            await self.save_session(session)

        # Update embeds (outside lock)
        await self.update_all_embeds(interaction.guild, session)

    async def handle_agent_selection(self, interaction: discord.Interaction, session_id: str,
                                      captain_id: int, agent_name: str, action_type: str):
        """Handle an agent being protected or banned."""
        async with self.get_session_lock(session_id):
            session = self.active_sessions.get(session_id)
            if not session:
                session = await self.get_session(session_id)
                if not session:
                    return

            # Verify it's this captain's turn
            if session["current_turn"] != captain_id:
                return

            # Get current map
            maps_to_play = self._get_maps_to_play(session)
            current_index = session.get("current_agent_map_index", 0)
            current_map = maps_to_play[current_index] if current_index < len(maps_to_play) else ""

            if action_type == "protect":
                # Record protect
                agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
                if current_map not in agent_protects:
                    agent_protects[current_map] = {}
                agent_protects[current_map][str(captain_id)] = agent_name
                session["agent_protects"] = agent_protects

                # Track used protects
                used_protects = json.loads(session.get("used_protects", "{}")) if isinstance(session.get("used_protects"), str) else session.get("used_protects", {})
                if str(captain_id) not in used_protects:
                    used_protects[str(captain_id)] = []
                used_protects[str(captain_id)].append(agent_name)
                session["used_protects"] = used_protects

            else:  # ban
                # Record ban
                agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})
                if current_map not in agent_bans:
                    agent_bans[current_map] = {}
                agent_bans[current_map][str(captain_id)] = agent_name
                session["agent_bans"] = agent_bans

                # Track used bans
                used_bans = json.loads(session.get("used_bans", "{}")) if isinstance(session.get("used_bans"), str) else session.get("used_bans", {})
                if str(captain_id) not in used_bans:
                    used_bans[str(captain_id)] = []
                used_bans[str(captain_id)].append(agent_name)
                session["used_bans"] = used_bans

            # Delete reminder if exists
            if session_id in self.reminder_messages:
                try:
                    thread_id = session["thread1_id"] if captain_id == session["captain1_id"] else session["thread2_id"]
                    thread = await self._get_thread(interaction.guild, thread_id)
                    if thread:
                        msg = await thread.fetch_message(self.reminder_messages[session_id])
                        await msg.delete()
                except Exception:
                    pass
                del self.reminder_messages[session_id]

            # Advance agent phase
            await self._advance_agent_phase(interaction.guild, session)

            # Save and update cache
            self.active_sessions[session_id] = session
            await self.save_session(session)

        # Update embeds (outside lock)
        await self.update_all_embeds(interaction.guild, session)

    async def _advance_agent_phase(self, guild: discord.Guild, session: Dict):
        """Advance the agent selection phase."""
        maps_to_play = self._get_maps_to_play(session)
        current_index = session.get("current_agent_map_index", 0)
        current_map = maps_to_play[current_index] if current_index < len(maps_to_play) else ""

        agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
        agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})
        side_selections = json.loads(session.get("side_selections", "{}")) if isinstance(session.get("side_selections"), str) else session.get("side_selections", {})

        current_phase = session.get("current_phase")
        map_protects = agent_protects.get(current_map, {})
        map_bans = agent_bans.get(current_map, {})

        # Get side chooser for this map (who goes first)
        side_chooser = side_selections.get(current_map, {}).get("chosen_by", session["captain1_id"])
        other_captain = session["captain2_id"] if side_chooser == session["captain1_id"] else session["captain1_id"]

        if current_phase == "agent_protect":
            if len(map_protects) < 2:
                # Next captain protects
                if len(map_protects) == 0:
                    session["current_turn"] = side_chooser  # Side chooser protects first
                else:
                    session["current_turn"] = other_captain  # Then the other captain
                # Reset the turn timer for whoever goes next
                session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
                session["reminder_sent"] = 0
            else:
                # Both protected, move to ban phase
                session["current_phase"] = "agent_ban"
                session["current_turn"] = side_chooser  # Side chooser bans first
                session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
                session["reminder_sent"] = 0

        elif current_phase == "agent_ban":
            if len(map_bans) < 2:
                # Next captain bans
                if len(map_bans) == 0:
                    session["current_turn"] = side_chooser  # Side chooser bans first
                else:
                    session["current_turn"] = other_captain  # Then the other captain
                # Reset the turn timer for whoever goes next
                session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
                session["reminder_sent"] = 0
            else:
                # Both banned, move to next map
                session["current_agent_map_index"] = current_index + 1

                if session["current_agent_map_index"] >= len(maps_to_play):
                    # All maps done - complete session
                    session["current_phase"] = "complete"
                    session["status"] = "complete"
                    await self.complete_session(guild, session)
                else:
                    # Next map - start with protect phase
                    next_map = maps_to_play[session["current_agent_map_index"]]
                    next_side_chooser = side_selections.get(next_map, {}).get("chosen_by", session["captain1_id"])
                    session["current_phase"] = "agent_protect"
                    session["current_turn"] = next_side_chooser
                    session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
                    session["reminder_sent"] = 0

    async def advance_session(self, guild: discord.Guild, session: Dict):
        """Advance the session to the next phase/turn."""
        actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
        map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
        side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
        remaining = get_remaining_maps(map_pool, actions)
        
        format_type = session["format"]
        
        if format_type == "bo1":
            await self._advance_bo1(guild, session, actions, remaining, side_selections)
        else:
            await self._advance_bo3(guild, session, actions, remaining, side_selections)
        
        # Reset turn timer
        session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
        session["reminder_sent"] = 0
    
    async def _advance_bo1(self, guild: discord.Guild, session: Dict, 
                          actions: List[Dict], remaining: List[str], side_selections: Dict):
        """Advance a Bo1 session."""
        # If only 1 map remains, move to side selection
        if len(remaining) == 1:
            if remaining[0] not in side_selections:
                # Decider map - determine who picks side
                session["current_phase"] = "side_select"
                session["current_side_select_map"] = remaining[0]
                
                # Last banner "picked" the map, so determine who picks side
                last_action = actions[-1] if actions else None
                if last_action:
                    last_banner = last_action["captain_id"]
                    if session["decider_side"] == "opponent":
                        # Opponent of last banner picks side
                        session["current_turn"] = session["captain1_id"] if last_banner == session["captain2_id"] else session["captain2_id"]
                    else:
                        # Banner picks side
                        session["current_turn"] = last_banner
                return
            else:
                # Side selected - check if agent selection is needed
                agent_pool = json.loads(session.get("agent_pool", "[]")) if isinstance(session.get("agent_pool"), str) else session.get("agent_pool", [])
                if agent_pool:
                    # Move to agent protect phase
                    session["current_phase"] = "agent_protect"
                    session["current_agent_map_index"] = 0
                    # Side chooser goes first
                    side_chooser = side_selections.get(remaining[0], {}).get("chosen_by", session["captain1_id"])
                    session["current_turn"] = side_chooser
                    session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
                    session["reminder_sent"] = 0
                else:
                    # No agents configured - complete
                    session["current_phase"] = "complete"
                    session["status"] = "complete"
                    await self.complete_session(guild, session)
                return
        
        # Continue banning - alternate turns
        session["current_phase"] = "banning"
        current = session["current_turn"]
        session["current_turn"] = session["captain1_id"] if current == session["captain2_id"] else session["captain2_id"]
    
    async def _advance_bo3(self, guild: discord.Guild, session: Dict,
                          actions: List[Dict], remaining: List[str], side_selections: Dict):
        """Advance a Bo3 session.

        Flow: Ban1 → Ban2 → Pick1 → Side1 → Pick2 → Side2 → Ban... → Decider Side → Agents → Complete

        Uses explicit turn calculation from action history and first_ban setting
        instead of alternating from current_turn (which breaks when side selects
        interrupt the pick/ban flow).
        """
        picks = [a for a in actions if a["type"] == "pick"]
        bans = [a for a in actions if a["type"] == "ban"]

        # Determine captain order from first_ban setting
        if session["first_ban"] == "captain1":
            first_captain = session["captain1_id"]
            second_captain = session["captain2_id"]
        else:
            first_captain = session["captain2_id"]
            second_captain = session["captain1_id"]

        # --- Priority 1: Check if a just-picked map needs side selection ---
        # This handles the immediate side-select-after-pick flow
        for pick_action in picks:
            if pick_action["map"] not in side_selections:
                # This picked map needs side selection - opponent of picker chooses
                picker_id = pick_action["captain_id"]
                session["current_phase"] = "side_select"
                session["current_side_select_map"] = pick_action["map"]
                session["current_turn"] = session["captain1_id"] if picker_id == session["captain2_id"] else session["captain2_id"]
                return

        # --- Priority 2: Initial bans (need 2) ---
        if len(bans) < 2:
            session["current_phase"] = "banning"
            # Ban 0 → first_captain, Ban 1 → second_captain
            session["current_turn"] = first_captain if len(bans) == 0 else second_captain
            return

        # --- Priority 3: Picks (need 2) ---
        if len(picks) < 2:
            session["current_phase"] = "picking"
            # Pick 0 → first_captain, Pick 1 → second_captain
            session["current_turn"] = first_captain if len(picks) == 0 else second_captain
            return

        # --- Priority 4: Continue banning until 1 map remains ---
        if len(remaining) > 1:
            session["current_phase"] = "banning"
            # Post-pick bans: calculate turn from ban count after initial 2
            post_pick_bans = len(bans) - 2
            # Even index → first_captain, odd → second_captain
            session["current_turn"] = first_captain if post_pick_bans % 2 == 0 else second_captain
            return

        # --- Priority 5: Decider side selection ---
        decider_map = remaining[0] if remaining else None
        if decider_map and decider_map not in side_selections:
            session["current_phase"] = "side_select"
            session["current_side_select_map"] = decider_map
            # Decider side based on decider_side setting
            last_ban = bans[-1] if bans else None
            if last_ban and session["decider_side"] == "opponent":
                session["current_turn"] = session["captain1_id"] if last_ban["captain_id"] == session["captain2_id"] else session["captain2_id"]
            else:
                session["current_turn"] = last_ban["captain_id"] if last_ban else first_captain
            return

        # --- Priority 6: Agent phases or complete ---
        agent_pool = json.loads(session.get("agent_pool", "[]")) if isinstance(session.get("agent_pool"), str) else session.get("agent_pool", [])
        if agent_pool:
            maps_to_play = self._get_maps_to_play(session)
            session["current_phase"] = "agent_protect"
            session["current_agent_map_index"] = 0
            first_map = maps_to_play[0] if maps_to_play else ""
            side_chooser = side_selections.get(first_map, {}).get("chosen_by", session["captain1_id"])
            session["current_turn"] = side_chooser
            session["turn_start_time"] = datetime.now(timezone.utc).isoformat()
            session["reminder_sent"] = 0
        else:
            session["current_phase"] = "complete"
            session["status"] = "complete"
            await self.complete_session(guild, session)
    
    async def complete_session(self, guild: discord.Guild, session: Dict):
        """Complete a session and send final results."""
        # Build final embed
        final_embed = build_final_result_embed(session)
        
        # Generate summary card image
        result_image = await generate_summary_card(session, guild.id, self)
        
        # Attach image if available
        file = None
        if result_image:
            file = discord.File(result_image, filename="result.png")
            final_embed.set_image(url="attachment://result.png")
        
        # Send to spectator channels
        settings = await self.get_guild_settings(guild.id)
        spectator_channels = json.loads(settings.get("spectator_channels", "[]"))

        for channel_id in spectator_channels:
            channel = guild.get_channel_or_thread(channel_id)
            if channel:
                try:
                    if file:
                        result_image.seek(0)
                        new_file = discord.File(result_image, filename="result.png")
                        await channel.send(embed=final_embed, file=new_file)
                    else:
                        await channel.send(embed=final_embed)
                except Exception as e:
                    print(f"Failed to send to spectator channel: {e}")

        # Delete old spectator messages (redundant with final result embed)
        spectator_messages = json.loads(session.get("spectator_messages", "[]")) if isinstance(session.get("spectator_messages", "[]"), str) else session.get("spectator_messages", [])
        for spec in spectator_messages:
            try:
                channel = guild.get_channel_or_thread(spec["channel_id"])
                if channel:
                    msg = await channel.fetch_message(spec["message_id"])
                    await msg.delete()
            except Exception:
                pass

        # Send to admin log
        admin_log_id = settings.get("admin_log_channel_id")
        if admin_log_id:
            channel = guild.get_channel_or_thread(admin_log_id)
            if channel:
                try:
                    if file:
                        result_image.seek(0)
                        new_file = discord.File(result_image, filename="result.png")
                        await channel.send(embed=final_embed, file=new_file)
                    else:
                        await channel.send(embed=final_embed)
                except Exception:
                    pass
        
        # Update captain threads with final status
        for thread_id in [session.get("thread1_id"), session.get("thread2_id")]:
            if thread_id:
                thread = await self._get_thread(guild, thread_id)
                if thread:
                    try:
                        if file:
                            result_image.seek(0)
                            new_file = discord.File(result_image, filename="result.png")
                            await thread.send(embed=final_embed, file=new_file)
                        else:
                            await thread.send(embed=final_embed)
                    except Exception:
                        pass
        
        # Schedule thread deletion
        session["complete_time"] = datetime.now(timezone.utc).isoformat()
        await self.save_session(session)
    
    # =========================================================================
    # EMBED UPDATES
    # =========================================================================
    
    async def _get_thread(self, guild: discord.Guild, thread_id: Optional[int]):
        """Get a thread by ID, falling back to fetch_channel if not in cache."""
        if thread_id is None:
            return None
        thread = guild.get_thread(thread_id)
        if thread is not None:
            return thread
        # Thread not in cache - fetch from API
        try:
            thread = await self.bot.fetch_channel(thread_id)
            return thread
        except Exception:
            return None

    async def update_all_embeds(self, guild: discord.Guild, session: Dict):
        """Update all embeds for a session."""
        session_id = session["session_id"]
        print(f"[MapBan] update_all_embeds START: session={session_id}, phase={session.get('current_phase')}")

        # Get settings for timeout
        settings = await self.get_guild_settings(guild.id)
        turn_timeout = settings.get("turn_timeout", DEFAULT_TURN_TIMEOUT)

        # Calculate time remaining
        turn_start = session.get("turn_start_time")
        time_remaining = None
        if turn_start:
            if isinstance(turn_start, str):
                turn_start = datetime.fromisoformat(turn_start.replace("Z", "+00:00"))
            elapsed = (datetime.now(timezone.utc) - turn_start).total_seconds()
            time_remaining = max(0, int(turn_timeout - elapsed))

        # Get current phase
        phase = session.get("current_phase", "ready")
        actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
        map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
        remaining = get_remaining_maps(map_pool, actions)

        # Build ALL embeds and views upfront before any async edits
        # to prevent race conditions where another coroutine modifies session
        # state between building embed1 and embed2
        embed1 = build_captain_embed(session, is_captain1=True, time_remaining=time_remaining)
        embed2 = build_captain_embed(session, is_captain1=False, time_remaining=time_remaining)

        view1 = None
        if phase == "ready" and not session.get("captain1_ready"):
            view1 = ReadyUpView(session_id)
        elif phase in ("banning", "picking") and session["current_turn"] == session["captain1_id"]:
            action_type = "ban" if phase == "banning" else "pick"
            view1 = MapSelectView(self, session_id, session["captain1_id"], remaining, action_type)
        elif phase == "side_select" and session["current_turn"] == session["captain1_id"]:
            view1 = SideSelectView(session_id)
        elif phase in ("agent_protect", "agent_ban") and session["current_turn"] == session["captain1_id"]:
            action_type = "protect" if phase == "agent_protect" else "ban"
            view1 = AgentSelectView(session_id, action_type)

        view2 = None
        if phase == "ready" and not session.get("captain2_ready"):
            view2 = ReadyUpView(session_id)
        elif phase in ("banning", "picking") and session["current_turn"] == session["captain2_id"]:
            action_type = "ban" if phase == "banning" else "pick"
            view2 = MapSelectView(self, session_id, session["captain2_id"], remaining, action_type)
        elif phase == "side_select" and session["current_turn"] == session["captain2_id"]:
            view2 = SideSelectView(session_id)
        elif phase in ("agent_protect", "agent_ban") and session["current_turn"] == session["captain2_id"]:
            action_type = "protect" if phase == "agent_protect" else "ban"
            view2 = AgentSelectView(session_id, action_type)

        spec_embed = None
        if phase != "complete":
            spec_embed = build_spectator_embed(session)

        # Now do all async edits with pre-built embeds/views
        thread1 = await self._get_thread(guild, session.get("thread1_id"))
        if thread1:
            try:
                msg1 = await thread1.fetch_message(session.get("captain1_msg_id"))
                await msg1.edit(embed=embed1, view=view1)
                print(f"[MapBan] update_all_embeds: captain 1 msg.edit SUCCESS "
                      f"(thread={session.get('thread1_id')}, msg={session.get('captain1_msg_id')}, "
                      f"view={'None' if view1 is None else type(view1).__name__})")
            except Exception as e:
                print(f"[MapBan] update_all_embeds: captain 1 FAILED "
                      f"(thread={session.get('thread1_id')}, msg={session.get('captain1_msg_id')}): "
                      f"{e}\n{traceback.format_exc()}")
        else:
            print(f"[MapBan] update_all_embeds: could not find thread for captain 1 "
                  f"(thread_id={session.get('thread1_id')})")

        thread2 = await self._get_thread(guild, session.get("thread2_id"))
        if thread2:
            try:
                msg2 = await thread2.fetch_message(session.get("captain2_msg_id"))
                await msg2.edit(embed=embed2, view=view2)
                print(f"[MapBan] update_all_embeds: captain 2 msg.edit SUCCESS "
                      f"(thread={session.get('thread2_id')}, msg={session.get('captain2_msg_id')}, "
                      f"view={'None' if view2 is None else type(view2).__name__})")
            except Exception as e:
                print(f"[MapBan] update_all_embeds: captain 2 FAILED "
                      f"(thread={session.get('thread2_id')}, msg={session.get('captain2_msg_id')}): "
                      f"{e}\n{traceback.format_exc()}")
        else:
            print(f"[MapBan] update_all_embeds: could not find thread for captain 2 "
                  f"(thread_id={session.get('thread2_id')})")

        # Update spectator embeds (skip if complete - messages already deleted)
        if spec_embed is not None:
            spectator_messages = json.loads(session.get("spectator_messages", "[]")) if isinstance(session.get("spectator_messages", "[]"), str) else session.get("spectator_messages", [])

            for spec in spectator_messages:
                channel = guild.get_channel_or_thread(spec["channel_id"])
                if channel:
                    try:
                        msg = await channel.fetch_message(spec["message_id"])
                        await msg.edit(embed=spec_embed)
                    except Exception:
                        pass
    
    # =========================================================================
    # BACKGROUND TASKS
    # =========================================================================
    
    @tasks.loop(seconds=TIMER_UPDATE_INTERVAL)
    async def timer_task(self):
        """Update timers and handle timeouts."""
        async with self.timer_update_lock:
            sessions_to_process = list(self.active_sessions.items())
        
        for session_id, session in sessions_to_process:
            try:
                await self._process_session_timer(session_id, session)
            except Exception as e:
                print(f"Error processing timer for {session_id}: {e}")
    
    async def _process_session_timer(self, session_id: str, session: Dict):
        """Process timer for a single session."""
        if session.get("status") != "active":
            return

        phase = session.get("current_phase")
        if phase not in ("banning", "picking", "side_select", "agent_protect", "agent_ban"):
            return

        guild = self.bot.get_guild(session["guild_id"])
        if not guild:
            return

        settings = await self.get_guild_settings(guild.id)
        turn_timeout = settings.get("turn_timeout", DEFAULT_TURN_TIMEOUT)

        turn_start = session.get("turn_start_time")
        if not turn_start:
            return

        if isinstance(turn_start, str):
            turn_start = datetime.fromisoformat(turn_start.replace("Z", "+00:00"))

        elapsed = (datetime.now(timezone.utc) - turn_start).total_seconds()
        time_remaining = max(0, int(turn_timeout - elapsed))

        # Use session lock for state-modifying operations
        async with self.get_session_lock(session_id):
            # Re-fetch session inside lock to get latest state
            session = self.active_sessions.get(session_id)
            if not session:
                session = await self.get_session(session_id)
                if not session:
                    return

            # Check if already handled by user action
            if session.get("status") != "active":
                return

            current_phase = session.get("current_phase")
            if current_phase not in ("banning", "picking", "side_select", "agent_protect", "agent_ban"):
                return

            # Recalculate time_remaining from the fresh session's turn_start_time.
            # The stale value computed before the lock can be from a previous turn that
            # already had its timer reset (e.g., after an auto-ban), which would
            # incorrectly trigger an immediate second auto-ban for the next captain.
            fresh_turn_start = session.get("turn_start_time")
            if fresh_turn_start:
                if isinstance(fresh_turn_start, str):
                    fresh_turn_start = datetime.fromisoformat(fresh_turn_start.replace("Z", "+00:00"))
                fresh_elapsed = (datetime.now(timezone.utc) - fresh_turn_start).total_seconds()
                time_remaining = max(0, int(turn_timeout - fresh_elapsed))

            # Send reminder at 1 minute remaining
            if time_remaining <= REMINDER_TIME and not session.get("reminder_sent"):
                await self._send_reminder(guild, session)
                session["reminder_sent"] = 1
                self.active_sessions[session_id] = session
                await self.save_session(session)

            # Handle timeout
            if time_remaining <= 0:
                await self._handle_timeout(guild, session)
            else:
                # Update embeds with new time (outside critical section)
                pass

        # Update embeds outside lock if not timeout
        if time_remaining > 0:
            await self.update_all_embeds(guild, session)
    
    async def _send_reminder(self, guild: discord.Guild, session: Dict):
        """Send a reminder to the current player."""
        current_turn = session["current_turn"]
        thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
        
        thread = await self._get_thread(guild, thread_id)
        if thread:
            try:
                msg = await thread.send(f"⏰ <@{current_turn}> - 1 minute remaining!")
                self.reminder_messages[session["session_id"]] = msg.id
            except Exception:
                pass
    
    async def _handle_timeout(self, guild: discord.Guild, session: Dict):
        """Handle a turn timeout by making a random selection."""
        phase = session.get("current_phase")
        current_turn = session["current_turn"]
        
        # Get captain name
        if current_turn == session["captain1_id"]:
            captain_name = session.get("captain1_name", "Captain 1")
        else:
            captain_name = session.get("captain2_name", "Captain 2")
        
        if phase in ("banning", "picking"):
            # Random map selection
            actions = json.loads(session["actions"]) if isinstance(session["actions"], str) else session["actions"]
            map_pool = json.loads(session["map_pool"]) if isinstance(session["map_pool"], str) else session["map_pool"]
            remaining = get_remaining_maps(map_pool, actions)
            
            if remaining:
                random_map = random.choice(remaining)
                action_type = "ban" if phase == "banning" else "pick"
                
                actions.append({
                    "type": action_type,
                    "map": random_map,
                    "captain_id": current_turn,
                    "captain_name": captain_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "timeout": True
                })
                session["actions"] = actions
                
                if action_type == "pick":
                    picked = json.loads(session["picked_maps"]) if isinstance(session["picked_maps"], str) else session["picked_maps"]
                    picked.append(random_map)
                    session["picked_maps"] = picked
                
                # Notify in thread
                thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
                thread = await self._get_thread(guild, thread_id)
                if thread:
                    try:
                        await thread.send(f"⏰ Time's up! Auto-{'banned' if action_type == 'ban' else 'picked'} **{random_map}**")
                    except Exception:
                        pass

        elif phase == "side_select":
            # Random side selection
            random_side = random.choice(["Attack", "Defense"])
            current_map = session.get("current_side_select_map", "")

            side_selections = json.loads(session["side_selections"]) if isinstance(session["side_selections"], str) else session["side_selections"]
            side_selections[current_map] = {
                "side": random_side,
                "chosen_by": current_turn,
                "chosen_by_name": captain_name,
                "timeout": True
            }
            session["side_selections"] = side_selections

            # Notify in thread
            thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
            thread = await self._get_thread(guild, thread_id)
            if thread:
                try:
                    await thread.send(f"Time's up! Auto-selected **{random_side}** for {current_map}")
                except Exception:
                    pass

        elif phase in ("agent_protect", "agent_ban"):
            # Random agent selection
            action_type = "protect" if phase == "agent_protect" else "ban"
            available_agents = self.get_available_agents(session, current_turn, action_type)

            maps_to_play = self._get_maps_to_play(session)
            current_index = session.get("current_agent_map_index", 0)
            current_map = maps_to_play[current_index] if current_index < len(maps_to_play) else ""

            if available_agents:
                random_agent = random.choice(available_agents)

                if action_type == "protect":
                    agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
                    if current_map not in agent_protects:
                        agent_protects[current_map] = {}
                    agent_protects[current_map][str(current_turn)] = random_agent
                    session["agent_protects"] = agent_protects

                    used_protects = json.loads(session.get("used_protects", "{}")) if isinstance(session.get("used_protects"), str) else session.get("used_protects", {})
                    if str(current_turn) not in used_protects:
                        used_protects[str(current_turn)] = []
                    used_protects[str(current_turn)].append(random_agent)
                    session["used_protects"] = used_protects
                else:
                    agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})
                    if current_map not in agent_bans:
                        agent_bans[current_map] = {}
                    agent_bans[current_map][str(current_turn)] = random_agent
                    session["agent_bans"] = agent_bans

                    used_bans = json.loads(session.get("used_bans", "{}")) if isinstance(session.get("used_bans"), str) else session.get("used_bans", {})
                    if str(current_turn) not in used_bans:
                        used_bans[str(current_turn)] = []
                    used_bans[str(current_turn)].append(random_agent)
                    session["used_bans"] = used_bans

                # Notify in thread
                thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
                thread = await self._get_thread(guild, thread_id)
                if thread:
                    try:
                        await thread.send(f"Time's up! Auto-{'protected' if action_type == 'protect' else 'banned'} **{random_agent}** for {current_map}")
                    except Exception:
                        pass
            else:
                # No available agents — record a placeholder so _advance_agent_phase
                # sees the count increase and doesn't reassign the same captain (infinite loop).
                print(f"[MapBan] Timeout: no available agents for {action_type} on {current_map} "
                      f"(captain={current_turn}, session={session['session_id']})")

                if action_type == "protect":
                    agent_protects = json.loads(session.get("agent_protects", "{}")) if isinstance(session.get("agent_protects"), str) else session.get("agent_protects", {})
                    if current_map not in agent_protects:
                        agent_protects[current_map] = {}
                    agent_protects[current_map][str(current_turn)] = "(skipped)"
                    session["agent_protects"] = agent_protects
                else:
                    agent_bans = json.loads(session.get("agent_bans", "{}")) if isinstance(session.get("agent_bans"), str) else session.get("agent_bans", {})
                    if current_map not in agent_bans:
                        agent_bans[current_map] = {}
                    agent_bans[current_map][str(current_turn)] = "(skipped)"
                    session["agent_bans"] = agent_bans

                thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
                thread = await self._get_thread(guild, thread_id)
                if thread:
                    try:
                        await thread.send(f"⏰ Time's up! No agents available to {action_type} — skipping.")
                    except Exception:
                        pass

            # Advance agent phase (regardless of whether an agent was selected)
            await self._advance_agent_phase(guild, session)

            # Delete reminder if exists
            if session["session_id"] in self.reminder_messages:
                try:
                    thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
                    thread = await self._get_thread(guild, thread_id)
                    if thread:
                        msg = await thread.fetch_message(self.reminder_messages[session["session_id"]])
                        await msg.delete()
                except Exception:
                    pass
                del self.reminder_messages[session["session_id"]]

            # Save and update cache
            self.active_sessions[session["session_id"]] = session
            await self.save_session(session)
            await self.update_all_embeds(guild, session)
            return  # Don't fall through to advance_session for agent phases

        # Delete reminder if exists
        if session["session_id"] in self.reminder_messages:
            try:
                thread_id = session["thread1_id"] if current_turn == session["captain1_id"] else session["thread2_id"]
                thread = await self._get_thread(guild, thread_id)
                if thread:
                    msg = await thread.fetch_message(self.reminder_messages[session["session_id"]])
                    await msg.delete()
            except Exception:
                pass
            del self.reminder_messages[session["session_id"]]

        # Advance session
        await self.advance_session(guild, session)

        # Save and update cache (already inside session lock from caller)
        self.active_sessions[session["session_id"]] = session
        await self.save_session(session)

        # Update embeds
        await self.update_all_embeds(guild, session)
    
    @tasks.loop(minutes=30)
    async def cleanup_task(self):
        """Clean up old threads and session data."""
        now = datetime.now(timezone.utc)
        
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            
            # Get completed sessions older than 3 hours for thread deletion
            async with db.execute(
                "SELECT * FROM sessions WHERE status = 'complete'"
            ) as cursor:
                sessions = await cursor.fetchall()
                
                for row in sessions:
                    session = dict(row)
                    complete_time = session.get("complete_time")
                    
                    if complete_time:
                        if isinstance(complete_time, str):
                            complete_time = datetime.fromisoformat(complete_time.replace("Z", "+00:00"))
                        
                        if (now - complete_time).total_seconds() > THREAD_AUTO_DELETE:
                            # Delete threads
                            guild = self.bot.get_guild(session["guild_id"])
                            if guild:
                                for thread_id in [session.get("thread1_id"), session.get("thread2_id")]:
                                    if thread_id:
                                        thread = await self._get_thread(guild, thread_id)
                                        if thread:
                                            try:
                                                await thread.delete()
                                            except Exception:
                                                pass
            
            # Delete session data older than 1 week
            cutoff = (now - timedelta(seconds=SESSION_DATA_RETENTION)).isoformat()
            await db.execute(
                "DELETE FROM sessions WHERE created_at < ? AND status = 'complete'",
                (cutoff,)
            )
            
            await db.commit()
    
    @timer_task.before_loop
    @cleanup_task.before_loop
    async def before_tasks(self):
        """Wait until bot is ready before starting tasks."""
        await self.bot.wait_until_ready()
    
    # =========================================================================
    # PERMISSION CHECK
    # =========================================================================
    
    async def is_mapban_admin(self, interaction: discord.Interaction) -> bool:
        """Check if user is a map ban admin."""
        settings = await self.get_guild_settings(interaction.guild_id)
        admin_role_id = settings.get("admin_role_id")
        
        if admin_role_id:
            role = interaction.guild.get_role(admin_role_id)
            if role and role in interaction.user.roles:
                return True
        
        return self.bot.is_bot_admin(interaction.user)
    
    # =========================================================================
    # SLASH COMMANDS
    # =========================================================================
    
    @app_commands.command(name="league_mapban_panel", description="Open the map ban admin panel")
    @app_commands.default_permissions(administrator=True)
    async def league_mapban_panel(self, interaction: discord.Interaction):
        """Display the admin panel."""
        if not await self.is_mapban_admin(interaction):
            await interaction.response.send_message("❌ You don't have permission.", ephemeral=True)
            return
        
        settings = await self.get_guild_settings(interaction.guild_id)
        embed = build_admin_panel_embed(settings)
        await interaction.response.send_message(embed=embed, view=AdminPanelView(self), ephemeral=True)
    
    @app_commands.command(name="league_mapban_start", description="Start a map ban session")
    @app_commands.describe(
        matchup_name="Name for this matchup (e.g., 'Team 1 vs Team 4')",
        captain_1="First captain",
        captain_2="Second captain",
        format="Bo1 or Bo3",
        first_ban="Who bans first (random by default)",
        decider_side="Who chooses side on decider map",
        team_name_1="Optional team name for captain 1 (max 20 chars, shown on summary image)",
        team_name_2="Optional team name for captain 2 (max 20 chars, shown on summary image)"
    )
    @app_commands.choices(
        format=[
            app_commands.Choice(name="Best of 1", value="bo1"),
            app_commands.Choice(name="Best of 3", value="bo3")
        ],
        first_ban=[
            app_commands.Choice(name="Random", value="random"),
            app_commands.Choice(name="Captain 1", value="captain1"),
            app_commands.Choice(name="Captain 2", value="captain2")
        ],
        decider_side=[
            app_commands.Choice(name="Opponent of last banner", value="opponent"),
            app_commands.Choice(name="Last banner", value="banner")
        ]
    )
    async def league_mapban_start(
        self,
        interaction: discord.Interaction,
        matchup_name: str,
        captain_1: discord.Member,
        captain_2: discord.Member,
        format: app_commands.Choice[str],
        first_ban: app_commands.Choice[str] = None,
        decider_side: app_commands.Choice[str] = None,
        team_name_1: str = None,
        team_name_2: str = None
    ):
        """Start a map ban session."""
        if not await self.is_mapban_admin(interaction):
            await interaction.response.send_message("❌ You don't have permission.", ephemeral=True)
            return
        
        # Truncate team names to 20 chars
        if team_name_1:
            team_name_1 = team_name_1[:20]
        if team_name_2:
            team_name_2 = team_name_2[:20]

        # Validate
        if captain_1 == captain_2:
            await interaction.response.send_message("❌ Captains must be different users.", ephemeral=True)
            return
        
        # Check settings
        settings = await self.get_guild_settings(interaction.guild_id)
        if not settings.get("parent_channel_id"):
            await interaction.response.send_message(
                "❌ Parent channel not configured. Use `/league_mapban_panel` to set it up.",
                ephemeral=True
            )
            return
        
        # Check maps
        maps = await self.get_enabled_maps(interaction.guild_id)
        min_maps = 5 if format.value == "bo3" else 3
        if len(maps) < min_maps:
            await interaction.response.send_message(
                f"❌ Need at least {min_maps} enabled maps for {format.name}. Use `/league_mapban_panel` to add maps.",
                ephemeral=True
            )
            return
        
        # Check if captains are in active sessions
        active = await self.get_active_sessions(interaction.guild_id)
        for session in active:
            if captain_1.id in (session["captain1_id"], session["captain2_id"]):
                await interaction.response.send_message(
                    f"❌ {captain_1.mention} is already in an active session.",
                    ephemeral=True
                )
                return
            if captain_2.id in (session["captain1_id"], session["captain2_id"]):
                await interaction.response.send_message(
                    f"❌ {captain_2.mention} is already in an active session.",
                    ephemeral=True
                )
                return
        
        await interaction.response.defer(ephemeral=True)
        
        # Create session
        first_ban_value = first_ban.value if first_ban else "random"
        decider_value = decider_side.value if decider_side else "opponent"
        
        session_id = await self.create_session(
            guild=interaction.guild,
            matchup_name=matchup_name,
            format_type=format.value,
            captain1=captain_1,
            captain2=captain_2,
            admin=interaction.user,
            first_ban=first_ban_value,
            decider_side=decider_value,
            team1_name=team_name_1,
            team2_name=team_name_2
        )
        
        if session_id:
            await interaction.followup.send(
                f"✅ Map ban session **{matchup_name}** started!\nSession ID: `{session_id}`",
                ephemeral=True
            )
        else:
            await interaction.followup.send("❌ Failed to create session.", ephemeral=True)


# =============================================================================
# SETUP
# =============================================================================

async def setup(bot: commands.Bot):
    """Set up the cog."""
    await bot.add_cog(MapBanCog(bot))
