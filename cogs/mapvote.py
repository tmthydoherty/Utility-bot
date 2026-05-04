import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import random
import asyncio
from datetime import datetime, timedelta, timezone
import logging
from collections import Counter
from typing import Optional, Dict, Any, List, Literal, Tuple
import copy
import io
import aiohttp
from PIL import Image
from pathlib import Path

# --- Constants ---
CONFIG_FILE_MAP = "map_voter_config.json"
EMBED_COLOR_MAP = 0xE91E63
ADMIN_EMBED_COLOR = 0x3498DB
VOTE_MAP_COUNT = 3
MAPS_ASSETS_DIR = Path(__file__).parent.parent / "assets" / "maps"
MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10MB limit for thumbnail downloads
_MAP_NAME_RE = None  # lazy-compiled regex

log_map = logging.getLogger(__name__)


def _safe_map_path(map_name: str) -> Optional[Path]:
    """Get a safe file path for a map image, preventing path traversal."""
    global _MAP_NAME_RE
    if _MAP_NAME_RE is None:
        import re
        _MAP_NAME_RE = re.compile(r"[^\w\s\-\.']", flags=re.UNICODE)
    sanitized = _MAP_NAME_RE.sub('', map_name).strip().rstrip('.')
    if not sanitized or sanitized != map_name.strip():
        return None
    filepath = (MAPS_ASSETS_DIR / f"{sanitized}.png").resolve()
    if not str(filepath).startswith(str(MAPS_ASSETS_DIR.resolve())):
        return None
    return filepath
AdminAction = Literal["remove_game", "remove_maps", "set_thumbnail", "set_max_votes", "map_stats", "view_maps"]


# --- Synchronous Helper Functions for File I/O ---
def _load_config_sync(file_path: str) -> Dict[str, Any]:
    """Loads configuration data from a JSON file."""
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log_map.error(f"Error loading config file {file_path}: {e}")
        return {}

def _save_config_sync(file_path: str, data: Dict[str, Any]):
    """Saves configuration data to a JSON file atomically."""
    try:
        temp_file = f"{file_path}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, file_path)
    except IOError as e:
        log_map.error(f"Error saving config file {file_path}: {e}")

# --- Configuration Management ---
class ConfigManager:
    """Handles loading and saving the JSON configuration file asynchronously."""
    def __init__(self, file_path: str):
        self.file_path = file_path

    async def load(self) -> Dict[str, Any]:
        """Loads the configuration from the JSON file asynchronously."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _load_config_sync, self.file_path)

    async def save(self, data: Dict[str, Any]):
        """Saves the configuration to the JSON file asynchronously."""
        data_to_save = copy.deepcopy(data)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _save_config_sync, self.file_path, data_to_save)


# --- UI Components ---
class VotingView(discord.ui.View):
    """A persistent view for the main map voting poll."""
    def __init__(self, cog_instance: 'MapVote'):
        super().__init__(timeout=None)
        self.cog = cog_instance

    async def handle_vote_interaction(self, interaction: discord.Interaction, map_index: int):
        """
        Main handler for processing a user's vote. Checks for max votes to conclude.
        """
        try:
            await interaction.response.defer(ephemeral=True)

            channel = interaction.channel
            if isinstance(channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
                permissions = channel.permissions_for(interaction.user)
                if not permissions.send_messages:
                    return await interaction.followup.send(
                        "You don't have permission to vote in this channel.",
                        ephemeral=True
                    )

            # Updated process_vote returns a bool to indicate if vote should conclude
            success, message, updated_vote_data, should_conclude = await self.cog.process_vote(interaction, map_index)
            await interaction.followup.send(message, ephemeral=True)
            
            if success and updated_vote_data:
                if should_conclude:
                    # Show the final vote count before concluding
                    await self.cog.update_vote_display(interaction.message, updated_vote_data)
                    await self.cog.conclude_vote(
                        guild_id_str=str(interaction.guild_id),
                        message_id_str=str(interaction.message.id)
                    )
                else:
                    # Just update the display
                    await self.cog.update_vote_display(interaction.message, updated_vote_data)

        except Exception as e:
            log_map.error(f"Error in handle_vote_interaction: {e}", exc_info=True)
            try:
                await interaction.followup.send("An unexpected error occurred while casting your vote.", ephemeral=True)
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Map 1", style=discord.ButtonStyle.secondary, custom_id="map_vote_1", row=0)
    async def map_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_vote_interaction(interaction, 0)

    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.secondary, custom_id="map_vote_2", row=0)
    async def map_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_vote_interaction(interaction, 1)

    @discord.ui.button(label="Map 3", style=discord.ButtonStyle.secondary, custom_id="map_vote_3", row=0)
    async def map_3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_vote_interaction(interaction, 2)
        
    @discord.ui.button(label="End Vote", style=discord.ButtonStyle.danger, custom_id="end_vote_early", row=1)
    async def end_vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Allows an admin to end the vote prematurely."""
        await interaction.response.defer(ephemeral=True)
        gid_str, msg_id_str = str(interaction.guild_id), str(interaction.message.id)

        # FIX: Check admin permissions first (without needing lock)
        if not await self.cog.is_map_admin(interaction.user):
            return await interaction.followup.send("❌ You don't have permission to end this vote.", ephemeral=True)

        # FIX: Verify vote exists inside conclude_vote which uses atomic pop
        # This prevents race conditions where vote could be concluded between check and call
        await interaction.followup.send("✅ Ending vote...", ephemeral=True)
        await self.cog.conclude_vote(guild_id_str=gid_str, message_id_str=msg_id_str, ended_by=interaction.user)


# --- Admin Panel Modals & Dynamic Views ---
class GameModal(discord.ui.Modal, title="Add a New Game"):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="e.g., Valorant, CS:GO, Overwatch")
    def __init__(self, cog_instance: 'MapVote'): super().__init__(); self.cog = cog_instance
    async def on_submit(self, interaction: discord.Interaction): await self.cog.logic_add_game(interaction, self.game_name.value)

class AddMapsModal(discord.ui.Modal, title="Add Maps to a Game"):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="The game to add maps to")
    maps = discord.ui.TextInput(label="Map Names (comma-separated)", style=discord.TextStyle.paragraph)
    def __init__(self, cog_instance: 'MapVote'): super().__init__(); self.cog = cog_instance
    async def on_submit(self, interaction: discord.Interaction): await self.cog.logic_add_maps(interaction, self.game_name.value, self.maps.value)

class AdminRoleModal(discord.ui.Modal):
    role_input = discord.ui.TextInput(label="Role Name, Mention, or ID")
    def __init__(self, cog_instance: 'MapVote', action: str):
        self.cog, self.action = cog_instance, action.lower(); super().__init__(title=f"{action.capitalize()} a Map Admin Role")
    async def on_submit(self, inter: discord.Interaction):
        if self.action == "add": await self.cog.logic_add_admin_role(inter, self.role_input.value)
        elif self.action == "remove": await self.cog.logic_remove_admin_role(inter, self.role_input.value)

class UserStatsModal(discord.ui.Modal, title="View User Vote Statistics"):
    user = discord.ui.TextInput(label="User ID or Mention (Optional)", required=False)
    def __init__(self, cog: 'MapVote'): super().__init__(); self.cog = cog
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            target_user = interaction.user if not self.user.value else await commands.MemberConverter().convert(await self.cog.bot.get_context(interaction), self.user.value)
            await self.cog.logic_user_stats(interaction, target_user)
        except commands.MemberNotFound: await interaction.followup.send(f"❌ Could not find member '{self.user.value}'.", ephemeral=True)

class FinalInputModal(discord.ui.Modal):
    final_value = discord.ui.TextInput(label="Value")
    def __init__(self, cog: 'MapVote', action: AdminAction, game_name: str, title: str, label: str, placeholder: str, map_name: Optional[str] = None):
        super().__init__(title=title)
        self.cog, self.action, self.game_name, self.map_name = cog, action, game_name, map_name
        self.final_value.label, self.final_value.placeholder = label, placeholder
    async def on_submit(self, interaction: discord.Interaction):
        if self.action == "set_thumbnail": await self.cog.logic_set_map_thumbnail(interaction, self.game_name, self.map_name, self.final_value.value)
        elif self.action == "set_max_votes": await self.cog.logic_set_max_votes(interaction, self.game_name, self.final_value.value)

class AdminActionView(discord.ui.View):
    def __init__(self, cog: 'MapVote', action: AdminAction, interaction: discord.Interaction, games: List[str]):
        super().__init__(timeout=180)
        self.cog, self.action, self.original_interaction = cog, action, interaction
        self.game_name: Optional[str] = None
        self.add_item(GameSelect(cog, action, games))
    async def on_timeout(self):
        try: await self.original_interaction.edit_original_response(content="This panel has timed out.", view=None)
        except discord.NotFound: pass

class GameSelect(discord.ui.Select):
    def __init__(self, cog: 'MapVote', action: AdminAction, games: List[str]):
        self.cog, self.action = cog, action
        options = [discord.SelectOption(label=game) for game in games] or [discord.SelectOption(label="No games configured", value="_placeholder")]
        super().__init__(placeholder="Select a game...", options=options)
    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "_placeholder": return await interaction.response.defer()
        self.view.game_name = self.values[0]
        if self.action in ["remove_game", "map_stats"]:
            await interaction.response.defer()
            if self.action == "remove_game": await self.cog.logic_remove_game(interaction, self.view.game_name)
            elif self.action == "map_stats": await self.cog.logic_map_stats(interaction, self.view.game_name)
            self.view.stop()
            await interaction.edit_original_response(content=f"Action completed for **{self.view.game_name}**.", view=None)
        elif self.action == "set_max_votes":
            modal = FinalInputModal(self.cog, self.action, self.view.game_name, title=f"Set Max Votes for {self.view.game_name}", label="Max Votes (3-25)", placeholder="e.g., 10")
            await interaction.response.send_modal(modal)
            self.view.stop(); await interaction.edit_original_response(view=None)
        elif self.action in ["remove_maps", "set_thumbnail"]:
            async with self.cog.config_lock:
                self.cog._ensure_latest_game_format(self.view.game_name)
                game_data = self.cog._get_games_config_sync().get(self.view.game_name, {})
                maps_list = list(game_data.get("maps", {}).keys())
            self.view.clear_items()
            self.view.add_item(MapSelect(self.cog, self.action, self.view.game_name, maps_list))
            await interaction.response.edit_message(view=self.view)
        elif self.action == "view_maps":
            await self.cog.logic_view_maps(interaction, self.view.game_name)
            self.view.stop()

class MapSelect(discord.ui.Select):
    def __init__(self, cog: 'MapVote', action: AdminAction, game_name: str, maps: List[str]):
        self.cog, self.action, self.game_name = cog, action, game_name
        options = [discord.SelectOption(label=m) for m in maps] or [discord.SelectOption(label="No maps for this game", value="_placeholder")]
        super().__init__(placeholder="Select a map...", min_values=1, max_values=len(options) if action == "remove_maps" else 1, options=options)
    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "_placeholder": return await interaction.response.defer()
        if self.action == "set_thumbnail":
            modal = FinalInputModal(self.cog, self.action, self.game_name, map_name=self.values[0], title=f"Set Thumbnail for {self.values[0]}", label="Image URL (or blank to remove)", placeholder="https://example.com/image.png")
            await interaction.response.send_modal(modal)
            self.view.stop(); await interaction.edit_original_response(view=None)
        elif self.action == "remove_maps":
            self.disabled = True
            self.view.add_item(ConfirmButton(self.cog, self.action, self.game_name, self.values))
            await interaction.response.edit_message(view=self.view)

class ConfirmButton(discord.ui.Button):
    def __init__(self, cog: 'MapVote', action: AdminAction, game_name: str, selected_maps: List[str]):
        super().__init__(label="Confirm Removal", style=discord.ButtonStyle.danger)
        self.cog, self.action, self.game_name, self.selected_maps = cog, action, game_name, selected_maps
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if self.action == "remove_maps": await self.cog.logic_remove_maps(interaction, self.game_name, ", ".join(self.selected_maps))
        self.view.stop(); await interaction.edit_original_response(content="Action completed.", view=None)

class MapPreviewSelect(discord.ui.Select):
    def __init__(self, cog: 'MapVote', game_name: str, maps_data: dict):
        self.cog = cog
        self.game_name = game_name
        self.maps_data = maps_data
        options = []
        for map_name in sorted(maps_data.keys()):
            has_thumb = bool(maps_data[map_name].get("url"))
            local_exists = (MAPS_ASSETS_DIR / f"{map_name}.png").exists()
            status = "Custom thumbnail set" if has_thumb else ("Local image only" if local_exists else "No image configured")
            options.append(discord.SelectOption(
                label=map_name, value=map_name,
                emoji="✅" if has_thumb else "❌",
                description=status[:100]
            ))
        if not options:
            options = [discord.SelectOption(label="No maps configured", value="_none")]
        super().__init__(placeholder="Select a map to preview...", options=options[:25])

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "_none":
            return await interaction.response.defer()
        map_name = self.values[0]
        data = self.maps_data.get(map_name, {})
        url = data.get("url")
        local_exists = (MAPS_ASSETS_DIR / f"{map_name}.png").exists()
        embed = discord.Embed(title=map_name, description=f"Game: **{self.game_name}**", color=ADMIN_EMBED_COLOR)
        embed.add_field(name="Custom Thumbnail", value="✅ Set" if url else "❌ Not set", inline=True)
        embed.add_field(name="Local Image", value="✅ Found" if local_exists else "❌ Missing", inline=True)
        if url:
            embed.set_image(url=url)
        await interaction.response.edit_message(embed=embed, view=self.view)


class MapListView(discord.ui.View):
    def __init__(self, cog: 'MapVote', game_name: str, maps_data: dict):
        super().__init__(timeout=120)
        self.add_item(MapPreviewSelect(cog, game_name, maps_data))


class AdminPanelView(discord.ui.View):
    def __init__(self, cog_instance: 'MapVote'): super().__init__(timeout=180); self.cog = cog_instance
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # This check remains per-guild, which is correct
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to use this panel.", ephemeral=True); return False
        return True
    async def _start_action(self, inter: discord.Interaction, act: AdminAction, title: str):
        async with self.cog.config_lock:
            games = list(self.cog._get_games_config_sync().keys())
        await inter.response.send_message(f"**{title}**", view=AdminActionView(self.cog, act, inter, games), ephemeral=True)
    @discord.ui.button(label="Add Game", style=discord.ButtonStyle.green, row=0)
    async def add_game(self, inter, button): await inter.response.send_modal(GameModal(self.cog))
    @discord.ui.button(label="Remove Game", style=discord.ButtonStyle.red, row=0)
    async def remove_game(self, inter, button): await self._start_action(inter, "remove_game", "Remove a Game")
    @discord.ui.button(label="Add Maps", style=discord.ButtonStyle.green, row=1)
    async def add_maps(self, inter, button): await inter.response.send_modal(AddMapsModal(self.cog))
    @discord.ui.button(label="Remove Maps", style=discord.ButtonStyle.red, row=1)
    async def remove_maps(self, inter, button): await self._start_action(inter, "remove_maps", "Remove Maps")
    @discord.ui.button(label="Add Admin Role", style=discord.ButtonStyle.secondary, row=2)
    async def add_admin(self, inter, button): await inter.response.send_modal(AdminRoleModal(self.cog, "add"))
    @discord.ui.button(label="Remove Admin Role", style=discord.ButtonStyle.secondary, row=2)
    async def remove_admin(self, inter, button): await inter.response.send_modal(AdminRoleModal(self.cog, "remove"))
    @discord.ui.button(label="Set Max Votes", style=discord.ButtonStyle.secondary, row=3)
    async def set_max_votes(self, inter, button): await self._start_action(inter, "set_max_votes", "Set Max Votes")
    @discord.ui.button(label="Set Map Thumbnail", style=discord.ButtonStyle.secondary, row=3)
    async def set_thumb(self, inter, button): await self._start_action(inter, "set_thumbnail", "Set Map Thumbnail")
    @discord.ui.button(label="View Map Stats", style=discord.ButtonStyle.secondary, row=4)
    async def map_stats(self, inter, button): await self._start_action(inter, "map_stats", "View Map Stats")
    @discord.ui.button(label="View User Stats", style=discord.ButtonStyle.secondary, row=4)
    async def user_stats(self, inter, button): await inter.response.send_modal(UserStatsModal(self.cog))
    @discord.ui.button(label="Map List", style=discord.ButtonStyle.secondary, row=4)
    async def map_list(self, inter, button): await self._start_action(inter, "view_maps", "Map List")

# --- Main Cog ---
class MapVote(commands.Cog, name="mapvote"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config_manager = ConfigManager(CONFIG_FILE_MAP)
        self.active_config: Dict[str, Any] = {}
        self.config_lock = asyncio.Lock()
        self.vote_check_loop.start()

    async def cog_load(self):
        try:
            async with self.config_lock:
                loaded_config = await self.config_manager.load()

                # --- MIGRATION LOGIC ---
                if not loaded_config or "universal_games" not in loaded_config:
                    log_map.warning("Old config format detected or config is empty. Migrating to new universal 'games' structure...")
                    new_config = {"universal_games": {}, "guild_data": {}}

                    for guild_id, guild_config in loaded_config.items():
                        if not guild_id.isdigit(): continue

                        new_config["universal_games"].update(guild_config.get("games", {}))

                        new_config["guild_data"][guild_id] = {
                            "admin_roles": guild_config.get("admin_roles", []),
                            "active_votes": guild_config.get("active_votes", {}),
                            "user_stats": guild_config.get("user_stats", {}),
                            "vote_counter": guild_config.get("vote_counter", 0)
                        }

                    self.active_config = new_config
                    await self._save_config()
                    log_map.info("Config migration complete.")
                else:
                    self.active_config = loaded_config

        except Exception as e:
            log_map.error(f"Failed to load MapVote cog: {e}", exc_info=True)
            raise e

    def cog_unload(self):
        self.vote_check_loop.cancel()
        
    
    # --- NEW/MODIFIED HELPER FUNCTIONS ---
    
    def _ensure_latest_game_format(self, game_name: str):
        """Ensures a game in the UNIVERSAL config has the latest data structure."""
        games = self._get_games_config_sync() # Get universal games
        if game_name not in games: return
        if isinstance(games[game_name], list): 
            games[game_name] = {"unseen_maps": games[game_name], "seen_maps": [], "win_history": {}}
        if "maps" not in games[game_name]: 
            games[game_name]["maps"] = { m: {"url": None} for m in games[game_name].get("unseen_maps", []) + games[game_name].get("seen_maps", [])}

    def _get_guild_config_sync(self, guild_id: int | str) -> Dict[str, Any]:
        """
        Gets the config for a SPECIFIC guild (admin roles, active votes, user stats).
        Accepts int or str, always uses str as key.
        """
        return self.active_config.setdefault("guild_data", {}).setdefault(str(guild_id), {})

    def _get_games_config_sync(self) -> Dict[str, Any]:
        """Gets the UNIVERSAL games config (games, maps, image URLs, win history)."""
        return self.active_config.setdefault("universal_games", {})
    
    async def _save_config(self): 
        """Saves the entire active_config (universal and guild-specific)."""
        await self.config_manager.save(self.active_config)

    # --- ---

    def _generate_vote_embed(self, vote_data: Dict[str, Any]) -> discord.Embed:
        game, end_time = vote_data.get("game", "Unknown Game"), datetime.fromisoformat(vote_data["end_time_iso"])
        min_users, max_votes = vote_data.get("min_users", 1), vote_data.get("max_votes", 10)
        desc = (f"Vote for the map to play! Winner is random, weighted by votes.\n**You can change your vote at any time.**\n\n"
                f"🕒 Concludes <t:{int(end_time.timestamp())}:R> or at **{max_votes}** votes.\n"
                f"👥 Needs **{min_users}** to be valid.")
        embed = discord.Embed(title=f"🗺️ {game} Map Vote", color=EMBED_COLOR_MAP, description=desc)
        return embed

    def _get_local_map_path(self, map_name: str) -> Optional[Path]:
        """Return the path to a local map image if it exists, or None."""
        filepath = _safe_map_path(map_name)
        if filepath is None:
            log_map.warning(f"Unsafe map name rejected: {map_name}")
            return None
        if filepath.exists():
            return filepath
        return None

    async def _download_and_save_map_image(self, map_name: str, url: str) -> bool:
        """Download an image from a URL and save it locally as {map_name}.png. Returns True on success."""
        filepath = _safe_map_path(map_name)
        if filepath is None:
            log_map.warning(f"Unsafe map name rejected for download: {map_name}")
            return False
        try:
            MAPS_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        log_map.warning(f"Failed to download thumbnail for '{map_name}': HTTP {resp.status}")
                        return False
                    if resp.content_length and resp.content_length > MAX_IMAGE_SIZE:
                        log_map.warning(f"Thumbnail for '{map_name}' too large: {resp.content_length} bytes")
                        return False
                    image_data = await resp.read()
                    if len(image_data) > MAX_IMAGE_SIZE:
                        log_map.warning(f"Thumbnail for '{map_name}' too large: {len(image_data)} bytes")
                        return False

            def convert_to_png():
                img = Image.open(io.BytesIO(image_data)).convert("RGBA")
                img.save(filepath, "PNG")

            await asyncio.to_thread(convert_to_png)
            log_map.info(f"Saved thumbnail for '{map_name}' to {filepath}")
            return True
        except Exception as e:
            log_map.error(f"Error downloading/saving thumbnail for '{map_name}': {e}")
            return False

    async def create_composite_image(self, map_names: List[str]) -> Optional[discord.File]:
        log_map.info(f"Attempting to create composite image for: {map_names}")

        # Collect valid file paths (no I/O yet)
        paths = [(name, self._get_local_map_path(name)) for name in map_names]
        valid_paths = [(name, p) for name, p in paths if p is not None]

        if len(valid_paths) != len(map_names):
            failed_maps = [name for name, p in paths if p is None]
            log_map.warning(f"Failed to find images for maps: {failed_maps}")

        if not valid_paths:
            log_map.warning("No valid image paths found to create composite image.")
            return None

        log_map.info(f"Found {len(valid_paths)} valid image(s) to process.")

        def run_pil():
            """Read files and compose image entirely in a thread."""
            try:
                images = [Image.open(str(p)).convert("RGBA") for _, p in valid_paths]
                h = 200
                w = sum(int(h * (img.width / img.height)) for img in images)
                resized = [img.resize((int(h * (img.width / img.height)), h), Image.Resampling.LANCZOS) for img in images]
                composite = Image.new('RGBA', (w, h))
                x_offset = 0
                for img in resized: composite.paste(img, (x_offset, 0)); x_offset += img.width
                buf = io.BytesIO(); composite.save(buf, 'PNG'); buf.seek(0)
                return buf
            except Exception as e:
                log_map.error(f"Pillow failed to process images: {e}")
                return None

        try:
            final_buffer = await asyncio.to_thread(run_pil)
            if final_buffer:
                log_map.info("Successfully created composite image file.")
                return discord.File(final_buffer, filename="map_vote.png")
        except Exception as e: log_map.error(f"Composite image failed in executor: {e}")
        return None

    async def is_map_admin(self, member: discord.Member) -> bool:
        if member.guild_permissions.manage_guild: return True
        async with self.config_lock: admin_ids = set(self._get_guild_config_sync(member.guild.id).get("admin_roles", []))
        return not {r.id for r in member.roles}.isdisjoint(admin_ids)

    async def game_autocomplete(self, inter: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        # FIX: Use lock to prevent race condition when reading games
        async with self.config_lock:
            games = list(self._get_games_config_sync().keys())
        return [app_commands.Choice(name=g, value=g) for g in games if current.lower() in g.lower()][:25]

    async def process_vote(self, inter: discord.Interaction, map_idx: int) -> Tuple[bool, str, Optional[Dict], bool]:
        """
        Processes a user's vote.
        Returns: (Success, Message, VoteData, ShouldConclude)
        """
        gid, mid, uid = str(inter.guild_id), str(inter.message.id), inter.user.id
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(gid)
            vote = guild_cfg.get("active_votes", {}).get(mid)
            if not vote:
                return False, "This vote has expired or is no longer active.", None, False

            # Check if voter is allowed (for custom match votes with restricted voters)
            allowed_voters = vote.get("allowed_voters")
            allowed_role_ids = vote.get("allowed_role_ids", [])

            if allowed_voters or allowed_role_ids:
                is_allowed = False
                # Check if user ID is in allowed_voters
                if allowed_voters and uid in allowed_voters:
                    is_allowed = True
                # Check if user has one of the allowed roles
                if allowed_role_ids and hasattr(inter.user, 'roles'):
                    user_role_ids = {r.id for r in inter.user.roles}
                    if not user_role_ids.isdisjoint(allowed_role_ids):
                        is_allowed = True

                if not is_allowed:
                    return False, "Only match players can vote in this map vote.", None, False

            if map_idx >= len(vote["maps"]):
                return False, "Invalid map selection.", None, False

            new_vote, votes = vote["maps"][map_idx], vote["votes"]
            old_vote = next((m for m, v in votes.items() if uid in v), None)

            if old_vote == new_vote:
                return False, f"You are already voting for **{new_vote}**.", None, False

            if old_vote:
                votes[old_vote].remove(uid)

            votes[new_vote].append(uid)

            # --- CRITICAL FIX 2 ---
            # Check for conclusion *inside* the lock
            voter_ids = {uid for v_list in votes.values() for uid in v_list}
            current_voters = len(voter_ids)
            max_votes = vote.get("max_votes", 10)
            # Conclude if all players voted OR if in overtime and majority reached
            majority = (max_votes // 2) + 1
            should_conclude = (current_voters >= max_votes
                               or (vote.get("overtime") and current_voters >= majority))
            # --- END FIX ---

            await self._save_config()
            
            msg = f"✅ Vote changed from **{old_vote}** to **{new_vote}**." if old_vote else f"✅ Vote for **{new_vote}** recorded."
            return True, msg, copy.deepcopy(vote), should_conclude

    async def update_vote_display(self, message: discord.Message, vote_data: Dict[str, Any]):
        """Update only the embed text and button labels — the image doesn't change between votes."""
        try:
            view = VotingView(self)
            maps, votes = vote_data.get("maps", []), vote_data.get("votes", {})
            voter_ids = {uid for v_list in votes.values() for uid in v_list}
            max_votes = vote_data.get("max_votes", 10)
            for i, child in enumerate(c for c in view.children if c.custom_id and c.custom_id.startswith("map_vote_")):
                if i < len(maps):
                    child.label = f"{maps[i]} ({len(votes.get(maps[i], []))})"
                    child.disabled = False
                else:
                    child.disabled = True

            # Put voter count on the End Vote button since editing the embed
            # breaks the attachment/embed image association on Discord.
            for child in view.children:
                if getattr(child, "custom_id", None) == "end_vote_early":
                    child.label = f"End Vote · {len(voter_ids)}/{max_votes}"
                    break

            await message.edit(view=view)

        except discord.NotFound:
            log_map.error(f"Message {message.id} not found for update")
        except Exception as e:
            log_map.error(f"Failed to update vote display: {e}", exc_info=True)


    async def update_voter_for_sub(self, guild_id: int, match_id: int, out_id: int, in_id: int):
        """
        Update an active map vote when a substitution occurs:
        - Remove out_id from allowed_voters and their existing vote
        - Add in_id to allowed_voters
        Returns the message_id of the active vote if found, else None.
        """
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(str(guild_id))
            active_votes = guild_cfg.get("active_votes", {})
            for mid, vote in active_votes.items():
                if vote.get("match_id") != match_id:
                    continue

                # Update allowed_voters list
                allowed = vote.get("allowed_voters")
                if allowed is not None:
                    if out_id in allowed:
                        allowed.remove(out_id)
                    if in_id not in allowed:
                        allowed.append(in_id)

                # Remove outgoing player's vote
                for map_name, voters in vote.get("votes", {}).items():
                    if out_id in voters:
                        voters.remove(out_id)

                await self._save_config()
                return int(mid)
        return None

    async def bump_vote_embed(self, guild_id: int, match_id: int, channel: discord.TextChannel):
        """Resend the map vote embed to a channel after a reshuffle, preserving all vote data."""
        # Mark the vote as "bumping" under lock so vote_check_loop and conclude_vote skip it
        vote_mid = None
        vote_data = None
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(str(guild_id))
            active_votes = guild_cfg.get("active_votes", {})
            for mid, vote in list(active_votes.items()):
                if vote.get("match_id") == match_id:
                    if vote.get("_bumping"):
                        return  # Another bump is already in progress
                    vote["_bumping"] = True
                    vote_mid = mid
                    vote_data = copy.deepcopy(vote)
                    break

        if not vote_data:
            return

        maps = vote_data.get("maps", [])

        view = VotingView(self)
        map_buttons = [
            c for c in view.children
            if getattr(c, "custom_id", None) and c.custom_id.startswith("map_vote_")
        ]
        voter_ids = {uid for v_list in vote_data.get("votes", {}).values() for uid in v_list}
        max_votes = vote_data.get("max_votes", 10)
        for i, child in enumerate(map_buttons):
            if i < len(maps):
                vote_count = len(vote_data.get("votes", {}).get(maps[i], []))
                child.label = f"{maps[i]} ({vote_count})" if vote_count else maps[i]
        for child in view.children:
            if getattr(child, "custom_id", None) == "end_vote_early":
                child.label = f"End Vote · {len(voter_ids)}/{max_votes}"
                break

        embed = self._generate_vote_embed(vote_data)
        img_file = await self.create_composite_image(maps)

        try:
            if img_file:
                new_msg = await channel.send(file=img_file)
                embed.set_image(url=f"attachment://{new_msg.attachments[0].filename}")
                await new_msg.edit(embed=embed, view=view)
            else:
                new_msg = await channel.send(embed=embed, view=view)
        except Exception as e:
            log_map.error(f"Failed to bump vote embed for match {match_id}: {e}")
            # Clear bumping flag on failure
            async with self.config_lock:
                guild_cfg = self._get_guild_config_sync(str(guild_id))
                av = guild_cfg.get("active_votes", {}).get(vote_mid)
                if av:
                    av.pop("_bumping", None)
            return

        # Atomically remap old message ID → new message ID
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(str(guild_id))
            active_votes = guild_cfg.get("active_votes", {})
            if vote_mid in active_votes:
                vote_entry = active_votes.pop(vote_mid)
                vote_entry.pop("_bumping", None)
                vote_entry["channel_id"] = channel.id
                active_votes[str(new_msg.id)] = vote_entry
                await self._save_config()

        try:
            old_msg = await channel.fetch_message(int(vote_mid))
            await old_msg.delete()
        except discord.NotFound:
            pass
        except Exception as e:
            log_map.warning(f"Failed to delete old vote message {vote_mid}: {e}")

    async def conclude_vote(self, guild_id_str: str, message_id_str: str, ended_by: Optional[discord.User] = None):
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(guild_id_str)
            vote = guild_cfg.get("active_votes", {}).get(message_id_str)
            if not vote or vote.get("_bumping"):
                return
            vote = guild_cfg["active_votes"].pop(message_id_str)

            pool = [m for m, v in vote.get("votes", {}).items() for _ in v]
            winner = random.choice(pool) if pool else random.choice(vote["maps"])

            if match_id := vote.get("match_id"):
                try:
                    custommatch_cog = self.bot.get_cog("CustomMatch")
                    if custommatch_cog:
                        from cogs.custommatch import DatabaseHelper
                        await DatabaseHelper.set_match_map(match_id, winner)
                        log_map.info(f"Stored map '{winner}' for match {match_id}")
                except Exception as e:
                    log_map.error(f"Failed to store map for match {match_id}: {e}")

            if game_name := vote.get("game"):
                self._ensure_latest_game_format(game_name)
                games_cfg = self._get_games_config_sync()
                gs = games_cfg.setdefault(game_name, {})
                gs.setdefault("win_history", {})[winner] = gs["win_history"].get(winner, 0) + 1

                guild_cfg.get("reserved_maps", {}).pop(game_name, None)

            for m, uids in vote.get("votes", {}).items():
                for uid in uids:
                    p = guild_cfg.setdefault("user_stats", {}).setdefault(str(uid), {"total_votes": 0, "wins": 0, "map_votes": {}})
                    p["total_votes"] += 1; p["map_votes"][m] = p["map_votes"].get(m, 0) + 1
                    if m == winner: p["wins"] += 1
            await self._save_config()

        if not (channel_id := vote.get("channel_id")) or not (channel := self.bot.get_channel(channel_id)):
             log_map.warning(f"Could not find channel_id for concluded vote {message_id_str}")
             return

        voters = {u for ul in vote.get("votes", {}).values() for u in ul}
        vote_counts = {m: len(v) for m, v in vote.get("votes", {}).items()}
        non_zero_counts = [c for c in vote_counts.values() if c > 0]
        winner_count = vote_counts.get(winner, 0)
        # If the least-voted map (above 0) wins and another map had more votes, rub it in
        is_underdog = (winner_count > 0 and non_zero_counts
                       and winner_count == min(non_zero_counts)
                       and max(non_zero_counts) > winner_count)
        desc = f"The chosen map is **{winner}**!" + (" ||Cry about it||" if is_underdog else "")
        if ended_by: desc += f"\n\n*Ended early by {ended_by.mention}.*"
        elif len(voters) >= vote.get("max_votes", 999): desc += f"\n\n*Concluded at {vote.get('max_votes')} votes.*"

        res_embed = discord.Embed(title="Map Vote Concluded", color=discord.Color.gold(), description=desc)
        total_v = sum(len(v) for v in vote.get("votes", {}).values())
        for m, v in sorted(vote.get("votes", {}).items(), key=lambda i: len(i[1]), reverse=True):
            res_embed.add_field(name=m, value=f"{len(v)} Votes ({(len(v)/total_v*100) if total_v else 0:.1f}% chance)")

        winner_path = self._get_local_map_path(winner)
        attachments = []
        if winner_path:
            attachments.append(discord.File(str(winner_path), filename="winner.png"))
            res_embed.set_image(url="attachment://winner.png")

        try:
            msg = await channel.fetch_message(int(message_id_str))
            disabled_view = VotingView(self)
            maps, final_votes = vote.get("maps", []), vote.get("votes", {})
            map_buttons = [c for c in disabled_view.children if c.custom_id and c.custom_id.startswith("map_vote_")]
            for i, child in enumerate(map_buttons):
                if i < len(maps): child.label = f"{maps[i]} ({len(final_votes.get(maps[i], []))})"
                child.disabled = True
            for item in disabled_view.children: item.disabled = True
            await msg.edit(view=disabled_view)
            result_msg = await msg.reply(embed=res_embed, files=attachments)
        except (discord.NotFound, discord.Forbidden) as e: log_map.warning(f"Failed conclude reply: {e}")

    async def _enter_overtime(self, gid_str: str, mid_str: str, vote: dict):
        """Timer expired without majority — mark vote as overtime and ping
        remaining non-voters. The vote will conclude the moment majority is
        reached via process_vote."""
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(gid_str)
            live_vote = guild_cfg.get("active_votes", {}).get(mid_str)
            if not live_vote:
                return
            live_vote["overtime"] = True
            await self._save_config()

        channel_id = vote.get("channel_id")
        if not channel_id:
            return
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return

        # Build set of users who have already voted
        voters = {u for ul in vote.get("votes", {}).values() for u in ul}
        # Build set of all allowed voters (match players)
        allowed = set(vote.get("allowed_voters") or [])
        non_voters = allowed - voters

        if non_voters:
            mentions = " ".join(f"<@{uid}>" for uid in non_voters)
            max_votes = vote.get("max_votes", 10)
            majority = (max_votes // 2) + 1
            try:
                await channel.send(
                    f"{mentions}\nThe map vote timer has ended but we need at least "
                    f"**{majority}** votes to decide. Please vote now!"
                )
            except Exception as e:
                log_map.error(f"Failed to ping non-voters for overtime vote {mid_str}: {e}")

    async def cancel_vote(self, gid_str: str, mid_str: str, vote: dict):
        async with self.config_lock:
            # Use gid_str directly (CRITICAL FIX 5)
            guild_cfg = self._get_guild_config_sync(gid_str)
            guild_cfg.get("active_votes", {}).pop(mid_str, None)

            # FIX: Reserve the same maps for next vote to prevent intentional vote dodging
            if (g_name := vote.get("game")) and (maps := vote.get("maps")):
                # Store these maps as reserved for this game in this guild
                reserved = guild_cfg.setdefault("reserved_maps", {})
                reserved[g_name] = maps
                log_map.info(f"Reserved maps {maps} for next {g_name} vote in guild {gid_str} (vote cancelled)")

            await self._save_config()
            
        # --- CRITICAL FIX 4 ---
        # Use .get() for channel_id
        if not (channel_id := vote.get("channel_id")) or not (channel := self.bot.get_channel(channel_id)):
             log_map.warning(f"Could not find channel_id for cancelled vote {mid_str}")
             return
        # --- END FIX ---
        
        voters = {u for ul in vote.get("votes", {}).values() for u in ul}
        desc = (f"Vote cancelled. Required **{vote.get('min_users', 1)}** voters, got **{len(voters)}**.\n\n"
                f"⚠️ **The same maps will be used for the next vote** to prevent intentional dodging.")
        try:
            msg = await channel.fetch_message(int(mid_str))
            disabled_view = VotingView(self)
            maps, final_votes = vote.get("maps", []), vote.get("votes", {})
            for i, child in enumerate(c for c in disabled_view.children if c.custom_id and c.custom_id.startswith("map_vote_")):
                if i < len(maps): child.label = f"{maps[i]} ({len(final_votes.get(maps[i], []))})"
                child.disabled = True
            for item in disabled_view.children: item.disabled = True
            await msg.edit(view=disabled_view); await msg.reply(embed=discord.Embed(title="🚫 Vote Cancelled", color=discord.Color.red(), description=desc))
        except (discord.NotFound, discord.Forbidden) as e: log_map.warning(f"Failed cancel reply: {e}")

    @tasks.loop(seconds=15)
    async def vote_check_loop(self):
        now = datetime.now(timezone.utc)
        to_process = []
        
        active_votes_copy = {}
        try:
            async with self.config_lock:
                active_votes_copy = copy.deepcopy(self.active_config.get("guild_data", {}))
        except Exception as e:
            log_map.error(f"Error copying active config in vote_check_loop: {e}", exc_info=True)
            return # Don't proceed if copy failed

        for gid, g_cfg in active_votes_copy.items():
            for mid, vote in g_cfg.get("active_votes", {}).items():
                try:
                    if now >= datetime.fromisoformat(vote["end_time_iso"]):
                        to_process.append((gid, mid, vote))
                except Exception as e:
                    log_map.error(f"Error parsing timestamp for vote {mid} in guild {gid}: {e}")

        for gid, mid, _vote in to_process:
            try:
                # Re-read current state under lock to avoid stale-copy race
                async with self.config_lock:
                    guild_cfg = self._get_guild_config_sync(gid)
                    live_vote = guild_cfg.get("active_votes", {}).get(mid)
                    if not live_vote or live_vote.get("_bumping"):
                        continue  # Already concluded/cancelled/being bumped
                    # Skip votes already in overtime (they conclude via process_vote)
                    if live_vote.get("overtime"):
                        continue
                    voter_count = len({u for ul in live_vote.get("votes", {}).values() for u in ul})
                    max_votes = live_vote.get("max_votes", 10)
                    majority = (max_votes // 2) + 1
                    vote_snapshot = copy.deepcopy(live_vote)

                if voter_count >= majority:
                    await self.conclude_vote(gid, mid)
                else:
                    # Not enough votes yet — enter overtime: ping non-voters
                    # and wait for majority before concluding
                    await self._enter_overtime(gid, mid, vote_snapshot)
            except Exception as e:
                log_map.error(f"Error processing vote {mid} in guild {gid}: {e}", exc_info=True)

    @vote_check_loop.before_loop
    async def before_loops(self): await self.bot.wait_until_ready();

    # --- Command Group ---
    mapvote = app_commands.Group(name="mapvote", description="Commands for map voting and configuration.")

    @mapvote.command(name="start", description="Start a vote to pick a map for a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(game="The game to vote on.", duration="Vote duration in minutes (1-10).", min_users="Minimum users required for the vote to pass (default: majority of max votes).")
    async def start(self, inter: discord.Interaction, game: str, duration: app_commands.Range[int, 1, 10]=2, min_users: Optional[app_commands.Range[int, 1, 12]]=None):
        await inter.response.defer(ephemeral=True)

        # Phase 1: Acquire lock, validate, pick maps, prepare vote_data, release lock
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(inter.guild_id)
            games_cfg = self._get_games_config_sync()

            self._ensure_latest_game_format(game)
            gd = games_cfg.get(game)

            if not gd: return await inter.followup.send(f"❌ Game '{game}' not configured.", ephemeral=True)
            game_max_votes = gd.get('max_votes', 10)
            if min_users is None:
                min_users = (game_max_votes // 2) + 1
            if min_users > game_max_votes: return await inter.followup.send(f"❌ Minimum users ({min_users}) cannot be greater than the max votes for this game ({game_max_votes}).", ephemeral=True)

            unseen = gd.get("unseen_maps", [])
            seen = gd.get("seen_maps", [])

            if len(unseen) + len(seen) < VOTE_MAP_COUNT:
                return await inter.followup.send(f"❌ '{game}' needs at least {VOTE_MAP_COUNT} maps.", ephemeral=True)

            reserved_maps = guild_cfg.get("reserved_maps", {}).get(game)
            chosen_maps = []

            if reserved_maps and len(reserved_maps) == VOTE_MAP_COUNT:
                all_maps = set(unseen + seen)
                valid_reserved = [m for m in reserved_maps if m in all_maps]

                if len(valid_reserved) == VOTE_MAP_COUNT:
                    chosen_maps = valid_reserved
                    log_map.info(f"Using reserved maps {chosen_maps} for {game} vote in guild {inter.guild_id}")
                    guild_cfg.get("reserved_maps", {}).pop(game, None)
                    gd["unseen_maps"] = [m for m in unseen if m not in chosen_maps]
                    gd["seen_maps"] = list(set(seen).union(chosen_maps))
                else:
                    log_map.warning(f"Reserved maps invalid for {game} (some maps removed), picking randomly")
                    guild_cfg.get("reserved_maps", {}).pop(game, None)
                    reserved_maps = None

            if not chosen_maps:
                if len(unseen) >= VOTE_MAP_COUNT:
                    chosen_maps = random.sample(unseen, VOTE_MAP_COUNT)
                    gd["unseen_maps"] = [m for m in unseen if m not in chosen_maps]
                    gd["seen_maps"].extend(chosen_maps)
                else:
                    chosen_maps.extend(unseen)
                    needed = VOTE_MAP_COUNT - len(chosen_maps)
                    fillers = random.sample(seen, needed)
                    chosen_maps.extend(fillers)
                    gd["unseen_maps"] = [m for m in seen if m not in fillers]
                    gd["seen_maps"] = chosen_maps

            guild_cfg["vote_counter"] = guild_cfg.get("vote_counter", 0) + 1
            vote_data = {"channel_id": inter.channel_id, "end_time_iso": (datetime.now(timezone.utc) + timedelta(minutes=duration)).isoformat(), "maps": chosen_maps, "votes": {m:[] for m in chosen_maps}, "game": game, "short_id": guild_cfg['vote_counter'], "min_users": min_users, "max_votes": gd.get('max_votes', 10)}
            await self._save_config()

        # Phase 2: I/O outside the lock (image generation + Discord API)
        img_file = await self.create_composite_image(chosen_maps)
        embed = self._generate_vote_embed(vote_data)
        view = VotingView(self)

        for i, child in enumerate(c for c in view.children if c.custom_id and c.custom_id.startswith("map_vote_")):
            if i < len(chosen_maps): child.label = f"{chosen_maps[i]} (0)"; child.disabled = False
            else: child.disabled = True
        for child in view.children:
            if getattr(child, "custom_id", None) == "end_vote_early":
                child.label = f"End Vote · 0/{vote_data.get('max_votes', 10)}"
                break

        if img_file:
            # Send the file first, then edit to add the embed referencing it.
            # This lets Discord consolidate the attachment into the embed image
            # instead of showing both a standalone preview and an embed image.
            msg = await inter.channel.send(file=img_file)
            embed.set_image(url=f"attachment://{msg.attachments[0].filename}")
            await msg.edit(embed=embed, view=view)
        else:
            msg = await inter.channel.send(embed=embed, view=view)

        await inter.followup.send("Vote started!", ephemeral=True)

        # Phase 3: Re-acquire lock to store the message ID
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(inter.guild_id)
            guild_cfg.setdefault("active_votes", {})[str(msg.id)] = vote_data
            await self._save_config()

    @mapvote.command(name="admin", description="Access the Map Voter admin panel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def admin(self, inter: discord.Interaction): await inter.response.send_message(view=AdminPanelView(self), ephemeral=True)

    # --- ADMIN LOGIC ---
    
    async def logic_add_game(self, inter: discord.Interaction, name: str):
        await inter.response.defer(ephemeral=True)
        async with self.config_lock:
            games = self._get_games_config_sync()
            if name in games: desc, color = f"❌ Game '{name}' already exists.", discord.Color.yellow()
            else: games[name] = {"maps": {}, "unseen_maps": [], "seen_maps": [], "win_history": {}, "max_votes": 10}; await self._save_config(); desc, color = f"✅ Game '{name}' added.", EMBED_COLOR_MAP
        await inter.followup.send(embed=discord.Embed(description=desc, color=color), ephemeral=True)

    async def logic_remove_game(self, inter: discord.Interaction, name: str):
        async with self.config_lock:
            if self._get_games_config_sync().pop(name, None):
                # FIX: Clean up reserved maps for this game across all guilds
                for guild_id, guild_cfg in self.active_config.get("guild_data", {}).items():
                    if "reserved_maps" in guild_cfg:
                        guild_cfg["reserved_maps"].pop(name, None)
                        log_map.info(f"Cleared reserved maps for removed game '{name}' in guild {guild_id}")

                await self._save_config(); desc, color = f"✅ Game '{name}' removed.", EMBED_COLOR_MAP
            else: desc, color = f"❌ Game '{name}' not found.", discord.Color.red()
        await inter.followup.send(embed=discord.Embed(description=desc, color=color), ephemeral=True)
        
    async def logic_add_maps(self, inter: discord.Interaction, game: str, maps_str: str):
        await inter.response.defer(ephemeral=True)
        added, existing, rejected = [], [], []
        map_names = [m.strip() for m in maps_str.split(',') if m.strip()]
        if not map_names: return await inter.followup.send("❌ No map names provided.", ephemeral=True)
        # Validate map names are safe for filesystem use
        safe_names = []
        for name in map_names:
            if _safe_map_path(name) is not None:
                safe_names.append(name)
            else:
                rejected.append(name)
        map_names = safe_names
        if not map_names and rejected:
            return await inter.followup.send(f"❌ Invalid map name(s): `{'`, `'.join(rejected)}`. Use only letters, numbers, spaces, and hyphens.", ephemeral=True)
        async with self.config_lock:
            games_cfg = self._get_games_config_sync()
            if not (gd := games_cfg.get(game)): return await inter.followup.send(f"❌ Game '{game}' not found.", ephemeral=True)
            self._ensure_latest_game_format(game)
            pool = set(gd.get("unseen_maps", []) + gd.get("seen_maps", []))
            for name in map_names:
                if name not in pool: gd["unseen_maps"].append(name); gd["maps"][name] = {"url": None}; added.append(name)
                else: existing.append(name)
            if added: await self._save_config()
        desc = (f"✅ Added: `{'`, `'.join(added)}`\n" if added else "") + (f"⚠️ Existed: `{'`, `'.join(existing)}`\n" if existing else "") + (f"❌ Rejected (invalid names): `{'`, `'.join(rejected)}`" if rejected else "")
        await inter.followup.send(embed=discord.Embed(title=f"Add Maps to {game}", description=desc.strip(), color=EMBED_COLOR_MAP), ephemeral=True)

    async def logic_remove_maps(self, inter: discord.Interaction, game: str, maps_str: str):
        removed, not_found = [], []
        map_names = [m.strip() for m in maps_str.split(',') if m.strip()]
        async with self.config_lock:
            games_cfg = self._get_games_config_sync()
            if not (gd := games_cfg.get(game)): return
            for name in map_names:
                was_removed = False
                if name in gd.get("unseen_maps", []): gd["unseen_maps"].remove(name); was_removed = True
                elif name in gd.get("seen_maps", []): gd["seen_maps"].remove(name); was_removed = True
                if was_removed: gd.get("maps", {}).pop(name, None); removed.append(name)
                else: not_found.append(name)
            if removed: await self._save_config()
        desc = (f"✅ Removed: `{'`, `'.join(removed)}`\n" if removed else "") + (f"⚠️ Not Found: `{'`, `'.join(not_found)}`" if not_found else "")
        await inter.followup.send(embed=discord.Embed(title=f"Remove Maps from {game}", description=desc.strip(), color=EMBED_COLOR_MAP), ephemeral=True)
    
    async def logic_set_map_thumbnail(self, inter: discord.Interaction, game: str, map_name: str, url: str):
        await inter.response.defer(ephemeral=True)
        if url and not url.startswith('http'): return await inter.followup.send("❌ Invalid URL.", ephemeral=True)
        async with self.config_lock:
            games_cfg = self._get_games_config_sync()
            game_data = games_cfg.get(game)
            if not game_data or map_name not in game_data.get("maps", {}):
                return await inter.followup.send(f"❌ Game or map no longer exists.", ephemeral=True)
            game_data["maps"][map_name]["url"] = url if url else None
            await self._save_config()

        download_note = ""
        if url:
            success = await self._download_and_save_map_image(map_name, url)
            download_note = "\n✅ Image downloaded and saved locally for vote display." if success else "\n⚠️ Could not download image locally — URL saved, but thumbnail may not appear in votes."

        desc = f"✅ Thumbnail for **{map_name}** has been {'set' if url else 'removed'}.{download_note}"
        embed = discord.Embed(description=desc, color=EMBED_COLOR_MAP)
        if url: embed.set_thumbnail(url=url)
        await inter.followup.send(embed=embed, ephemeral=True)

    async def logic_add_admin_role(self, inter: discord.Interaction, role_str: str):
        await inter.response.defer(ephemeral=True)
        try: role = await commands.RoleConverter().convert(await self.bot.get_context(inter), role_str)
        except commands.RoleNotFound: return await inter.followup.send(f"❌ Role '{role_str}' not found.", ephemeral=True)
        async with self.config_lock:
            admin_roles = self._get_guild_config_sync(inter.guild_id).setdefault("admin_roles", [])
            if role.id in admin_roles: await inter.followup.send(f"⚠️ {role.mention} is already an admin role.", ephemeral=True)
            else: admin_roles.append(role.id); await self._save_config(); await inter.followup.send(f"✅ {role.mention} added.", ephemeral=True)
    
    async def logic_remove_admin_role(self, inter: discord.Interaction, role_str: str):
        await inter.response.defer(ephemeral=True)
        try: role = await commands.RoleConverter().convert(await self.bot.get_context(inter), role_str)
        except commands.RoleNotFound: return await inter.followup.send(f"❌ Role '{role_str}' not found.", ephemeral=True)
        async with self.config_lock:
            admin_roles = self._get_guild_config_sync(inter.guild_id).setdefault("admin_roles", [])
            if role.id not in admin_roles: await inter.followup.send(f"⚠️ {role.mention} is not an admin role.", ephemeral=True)
            else: admin_roles.remove(role.id); await self._save_config(); await inter.followup.send(f"✅ {role.mention} removed.", ephemeral=True)

    async def logic_set_max_votes(self, inter: discord.Interaction, game: str, max_str: str):
        await inter.response.defer(ephemeral=True)
        try:
            max_v = int(max_str)
            if not 3 <= max_v <= 25: raise ValueError()
        except ValueError: return await inter.followup.send("❌ Invalid number. Must be 3-25.", ephemeral=True)
        async with self.config_lock:
            self._get_games_config_sync()[game]["max_votes"] = max_v
            await self._save_config()
        await inter.followup.send(embed=discord.Embed(description=f"✅ Max votes for **{game}** set to **{max_v}**.", color=EMBED_COLOR_MAP), ephemeral=True)

    async def logic_user_stats(self, inter: discord.Interaction, user: discord.Member):
        # FIX: Use lock to prevent race condition when reading stats
        async with self.config_lock:
            stats = self._get_guild_config_sync(inter.guild_id).get("user_stats", {}).get(str(user.id))
            if not stats:
                return await inter.followup.send(f"{user.display_name} has not voted yet.", ephemeral=True)
            # Make a copy to work with outside the lock
            stats = copy.deepcopy(stats)

        total, wins = stats.get("total_votes", 0), stats.get("wins", 0)
        embed = discord.Embed(title=f"📊 Stats for {user.display_name}", color=EMBED_COLOR_MAP).set_thumbnail(url=user.display_avatar.url)
        embed.add_field(name="Votes", value=f"`{total}`").add_field(name="Wins", value=f"`{wins}`").add_field(name="Win Rate", value=f"`{(wins/total*100) if total else 0:.1f}%`")
        if map_votes := stats.get("map_votes", {}):
            top_maps = "\n".join([f"• **{m}**: {v}" for m,v in sorted(map_votes.items(), key=lambda i:i[1], reverse=True)[:5]])
            embed.add_field(name="Most Voted Maps", value=top_maps, inline=False)
        await inter.followup.send(embed=embed, ephemeral=True)

    async def logic_map_stats(self, inter: discord.Interaction, game: str):
        # FIX: Use lock to prevent race condition when reading game stats
        async with self.config_lock:
            self._ensure_latest_game_format(game)
            games_cfg = self._get_games_config_sync()

            if not (gd := games_cfg.get(game)): return
            if not (all_maps := sorted(list(gd.get("maps", {}).keys()))): return await inter.followup.send(f"No maps for **{game}**.", ephemeral=True)

            # Make copies to work with outside the lock
            win_hist = copy.deepcopy(gd.get("win_history", {}))
            all_maps_copy = list(all_maps)

        total = sum(win_hist.values())
        stats = [f"• **{m}**: {win_hist.get(m, 0)} wins ({(win_hist.get(m, 0)/total*100) if total else 0:.1f}%)" for m in all_maps_copy]
        embed = discord.Embed(title=f"🏆 Stats for {game}", color=EMBED_COLOR_MAP, description="\n".join(stats))
        await inter.followup.send(embed=embed, ephemeral=True)

    async def logic_view_maps(self, inter: discord.Interaction, game_name: str):
        await inter.response.defer()
        async with self.config_lock:
            self._ensure_latest_game_format(game_name)
            gd = self._get_games_config_sync().get(game_name)
            if not gd:
                await inter.edit_original_response(content=f"❌ Game '{game_name}' not found.", embed=None, view=None)
                return
            maps_data = copy.deepcopy(gd.get("maps", {}))

        if not maps_data:
            await inter.edit_original_response(content=f"No maps configured for **{game_name}**.", embed=None, view=None)
            return

        has_thumb_count = sum(1 for d in maps_data.values() if d.get("url"))
        has_local_count = sum(1 for n in maps_data if (MAPS_ASSETS_DIR / f"{n}.png").exists())

        lines = []
        for map_name in sorted(maps_data.keys()):
            has_thumb = bool(maps_data[map_name].get("url"))
            local_exists = (MAPS_ASSETS_DIR / f"{map_name}.png").exists()
            icon = "✅" if has_thumb else "❌"
            local_icon = " 🖼️" if local_exists else ""
            lines.append(f"{icon}{local_icon} {map_name}")

        embed = discord.Embed(
            title=f"Maps — {game_name}",
            description=f"**{len(maps_data)}** maps · **{has_thumb_count}** custom thumbnails · **{has_local_count}** local images",
            color=ADMIN_EMBED_COLOR
        )
        map_list_text = "\n".join(lines)
        if len(map_list_text) > 1024:
            map_list_text = map_list_text[:1021] + "..."
        embed.add_field(name="✅ = custom thumbnail  🖼️ = local image", value=map_list_text, inline=False)

        view = MapListView(self, game_name, maps_data)
        await inter.edit_original_response(content="", embed=embed, view=view)

    # --- PROGRAMMATIC VOTE FOR CUSTOM MATCH INTEGRATION ---

    async def start_programmatic_vote(
        self,
        guild_id: int,
        channel: discord.TextChannel,
        game_name: str,
        duration: int = 3,
        min_users: Optional[int] = None,
        max_votes: int = 10,
        allowed_voters: Optional[List[int]] = None,
        red_role_id: Optional[int] = None,
        blue_role_id: Optional[int] = None,
        match_id: Optional[int] = None
    ) -> Optional[int]:
        """
        Start a map vote programmatically (for custom match integration).
        Returns message_id or None on failure.

        Args:
            guild_id: The guild ID
            channel: The channel to post the vote in
            game_name: The game to vote on
            duration: Vote duration in minutes (default 3)
            min_users: Minimum users required for vote to pass (default: majority of max_votes)
            max_votes: Maximum votes before auto-conclude (default 10)
            allowed_voters: List of user IDs who can vote (if None, anyone can vote)
            red_role_id: Optional role ID for red team (alternative voter restriction)
            blue_role_id: Optional role ID for blue team (alternative voter restriction)
            match_id: Optional match ID from custommatch cog (for storing selected map)
        """
        if min_users is None:
            min_users = (max_votes // 2) + 1
        try:
            # Phase 1: Acquire lock, validate, pick maps, prepare vote_data, release lock
            async with self.config_lock:
                guild_cfg = self._get_guild_config_sync(guild_id)
                games_cfg = self._get_games_config_sync()

                self._ensure_latest_game_format(game_name)
                gd = games_cfg.get(game_name)

                if not gd:
                    log_map.warning(f"Programmatic vote failed: Game '{game_name}' not configured")
                    return None

                unseen = gd.get("unseen_maps", [])
                seen = gd.get("seen_maps", [])

                if len(unseen) + len(seen) < VOTE_MAP_COUNT:
                    log_map.warning(f"Programmatic vote failed: '{game_name}' needs at least {VOTE_MAP_COUNT} maps")
                    return None

                if len(unseen) >= VOTE_MAP_COUNT:
                    chosen_maps = random.sample(unseen, VOTE_MAP_COUNT)
                    gd["unseen_maps"] = [m for m in unseen if m not in chosen_maps]
                    gd["seen_maps"].extend(chosen_maps)
                else:
                    chosen_maps = list(unseen)
                    needed = VOTE_MAP_COUNT - len(chosen_maps)
                    fillers = random.sample(seen, needed)
                    chosen_maps.extend(fillers)
                    gd["unseen_maps"] = [m for m in seen if m not in fillers]
                    gd["seen_maps"] = chosen_maps

                guild_cfg["vote_counter"] = guild_cfg.get("vote_counter", 0) + 1
                vote_data = {
                    "channel_id": channel.id,
                    "end_time_iso": (datetime.now(timezone.utc) + timedelta(minutes=duration)).isoformat(),
                    "maps": chosen_maps,
                    "votes": {m: [] for m in chosen_maps},
                    "game": game_name,
                    "short_id": guild_cfg["vote_counter"],
                    "min_users": min_users,
                    "max_votes": max_votes,
                    "allowed_voters": allowed_voters,
                    "allowed_role_ids": [r for r in [red_role_id, blue_role_id] if r],
                    "match_id": match_id
                }
                await self._save_config()

            # Phase 2: I/O outside the lock
            img_file = await self.create_composite_image(chosen_maps)
            embed = self._generate_vote_embed(vote_data)
            view = VotingView(self)

            for i, child in enumerate(c for c in view.children if c.custom_id and c.custom_id.startswith("map_vote_")):
                if i < len(chosen_maps):
                    child.label = f"{chosen_maps[i]} (0)"
                    child.disabled = False
                else:
                    child.disabled = True
            for child in view.children:
                if getattr(child, "custom_id", None) == "end_vote_early":
                    child.label = f"End Vote · 0/{vote_data.get('max_votes', 10)}"
                    break

            if img_file:
                msg = await channel.send(file=img_file)
                embed.set_image(url=f"attachment://{msg.attachments[0].filename}")
                await msg.edit(embed=embed, view=view)
            else:
                msg = await channel.send(embed=embed, view=view)

            # Phase 3: Re-acquire lock to store message ID
            async with self.config_lock:
                guild_cfg = self._get_guild_config_sync(guild_id)
                guild_cfg.setdefault("active_votes", {})[str(msg.id)] = vote_data
                await self._save_config()

            log_map.info(f"Programmatic vote started for {game_name} in channel {channel.id}")
            return msg.id

        except Exception as e:
            log_map.error(f"Error starting programmatic vote: {e}", exc_info=True)
            return None


async def setup(bot: commands.Bot):
    """The setup function called by discord.py to load the cog."""
    cog = MapVote(bot)
    bot.add_view(VotingView(cog))
    await bot.add_cog(cog)


