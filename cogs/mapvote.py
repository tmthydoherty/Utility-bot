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
import time

# --- Constants ---
CONFIG_FILE_MAP = "map_voter_config.json"
EMBED_COLOR_MAP = 0xE91E63
ADMIN_EMBED_COLOR = 0x3498DB
VOTE_MAP_COUNT = 3
IMAGE_CACHE_DIR = "image_cache" # Directory to store cached thumbnails
CACHE_LIFETIME_DAYS = 30 # Days to keep image thumbnails in cache

log_map = logging.getLogger(__name__)
AdminAction = Literal["remove_game", "remove_maps", "set_thumbnail", "set_max_votes", "map_stats"]


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
    def __init__(self, cog: 'MapVote', action: AdminAction, interaction: discord.Interaction):
        super().__init__(timeout=180)
        self.cog, self.action, self.original_interaction = cog, action, interaction
        self.game_name: Optional[str] = None
        self.add_item(GameSelect(cog, action, interaction))
    async def on_timeout(self):
        try: await self.original_interaction.edit_original_response(content="This panel has timed out.", view=None)
        except discord.NotFound: pass

class GameSelect(discord.ui.Select):
    def __init__(self, cog: 'MapVote', action: AdminAction, interaction: discord.Interaction):
        self.cog, self.action = cog, action
        # Get games from the universal config
        games = list(self.cog._get_games_config_sync().keys())
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
            self.view.clear_items()
            self.view.add_item(MapSelect(self.cog, self.action, self.view.game_name, interaction))
            await interaction.response.edit_message(view=self.view)

class MapSelect(discord.ui.Select):
    def __init__(self, cog: 'MapVote', action: AdminAction, game_name: str, interaction: discord.Interaction):
        self.cog, self.action, self.game_name = cog, action, game_name
        # Ensure format in universal config
        self.cog._ensure_latest_game_format(game_name)
        # Get maps from universal config
        game_data = self.cog._get_games_config_sync().get(game_name, {})
        maps = list(game_data.get("maps", {}).keys())
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

class AdminPanelView(discord.ui.View):
    def __init__(self, cog_instance: 'MapVote'): super().__init__(timeout=180); self.cog = cog_instance
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # This check remains per-guild, which is correct
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to use this panel.", ephemeral=True); return False
        return True
    async def _start_action(self, inter: discord.Interaction, act: AdminAction, title: str): await inter.response.send_message(f"**{title}**", view=AdminActionView(self.cog, act, inter), ephemeral=True)
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

# --- Main Cog ---
class MapVote(commands.Cog, name="mapvote"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config_manager = ConfigManager(CONFIG_FILE_MAP)
        self.active_config: Dict[str, Any] = {}
        self.config_lock = asyncio.Lock()
        self.session: Optional[aiohttp.ClientSession] = None # Initialize as None
        self.vote_check_loop.start()
        self.clean_image_cache_loop.start()

    async def cog_load(self):
        try: # Add try block for session safety
            self.session = aiohttp.ClientSession() # Initialize session here
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
                    
            os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)
        
        except Exception as e:
            log_map.error(f"Failed to load MapVote cog: {e}", exc_info=True)
            # Ensure session is closed if it was created
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            raise e # Re-raise error to prevent cog from loading
            
    def cog_unload(self):
        self.vote_check_loop.cancel()
        self.clean_image_cache_loop.cancel()
        # Check if session exists AND is not closed
        if self.session and not self.session.closed:
            asyncio.create_task(self._close_session())

    async def _close_session(self):
        if self.session: # Check again in case it was set to None
            await self.session.close()
        
    def get_footer_text(self) -> str: return f"{self.bot.user.name} • Map Voter"
    
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
        min_users, vote_id, max_votes = vote_data.get("min_users", 1), vote_data.get("short_id", "N/A"), vote_data.get("max_votes", 10)
        voter_ids = {uid for v_list in vote_data.get("votes", {}).values() for uid in v_list}
        desc = (f"Vote for the map to play! Winner is random, weighted by votes.\n**You can change your vote at any time.**\n\n"
                f"🕒 Concludes <t:{int(end_time.timestamp())}:R> or at **{max_votes}** votes.\n"
                f"👥 **{len(voter_ids)}/{max_votes}** voted. Needs **{min_users}** to be valid.")
        embed = discord.Embed(title=f"🗺️ {game} Map Vote", color=EMBED_COLOR_MAP, description=desc)
        return embed.set_footer(text=f"Vote ID: #{vote_id}  •  {self.get_footer_text()}")

    async def _get_cached_image(self, url: str) -> Optional[io.BytesIO]:
        if not url or not self.session: return None
        filepath = os.path.join(IMAGE_CACHE_DIR, f"{hash(url)}.png")
        try:
            if os.path.exists(filepath):
                loop = asyncio.get_running_loop()
                def read_file():
                    with open(filepath, "rb") as f:
                        return f.read()
                data = await loop.run_in_executor(None, read_file)
                return io.BytesIO(data)
            
            async with self.session.head(url, timeout=aiohttp.ClientTimeout(total=3)) as response:
                if response.status != 200 or not response.headers.get("Content-Type", "").startswith("image/"):
                    log_map.warning(f"URL is not a valid image or is inaccessible: {url}")
                    return None
            
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
                if response.status == 200:
                    data = await response.read()
                    loop = asyncio.get_running_loop()
                    
                    # --- CRITICAL FIX 1 ---
                    # Use a 'with' statement to ensure the file handle is closed
                    def write_file(img_data):
                        with open(filepath, "wb") as f:
                            f.write(img_data)
                    await loop.run_in_executor(None, write_file, data)
                    # --- END FIX ---
                    
                    return io.BytesIO(data)
                log_map.warning(f"Download failed for {url}: status {response.status}")
        except asyncio.TimeoutError:
            log_map.warning(f"Timeout while fetching image: {url}")
        except Exception as e: 
            log_map.error(f"Download/cache error for {url}: {e}")
        return None

    async def create_composite_image(self, map_names: List[str], game_name: str) -> Optional[discord.File]:
        log_map.info(f"Attempting to create composite image for: {map_names}")
        self._ensure_latest_game_format(game_name)
        map_defs = self._get_games_config_sync().get(game_name, {}).get("maps", {})

        buffers = await asyncio.gather(*[
            self._get_cached_image(map_defs.get(name, {}).get("url"))
            for name in map_names
        ])
        
        valid_buffers = [b for b in buffers if b]

        if len(valid_buffers) != len(map_names):
            failed_maps = [name for name, buf in zip(map_names, buffers) if not buf]
            log_map.warning(f"Failed to load images for maps: {failed_maps}")
            log_map.error(f"Only {len(valid_buffers)}/{len(map_names)} images available. Skipping composite.")
            return None
        
        if not valid_buffers:
            log_map.warning("No valid image buffers found to create composite image.")
            return None

        log_map.info(f"Found {len(valid_buffers)} valid image(s) to process.")
        def run_pil():
            try:
                images = [Image.open(buf).convert("RGBA") for buf in valid_buffers]
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
            should_conclude = current_voters >= max_votes
            # --- END FIX ---

            await self._save_config()
            
            msg = f"✅ Vote changed from **{old_vote}** to **{new_vote}**." if old_vote else f"✅ Vote for **{new_vote}** recorded."
            return True, msg, copy.deepcopy(vote), should_conclude

    async def update_vote_display(self, message: discord.Message, vote_data: Dict[str, Any]):
        try:
            view = VotingView(self)
            maps, votes = vote_data.get("maps", []), vote_data.get("votes", {})
            for i, child in enumerate(c for c in view.children if c.custom_id and c.custom_id.startswith("map_vote_")):
                if i < len(maps):
                    child.label = f"{maps[i]} ({len(votes.get(maps[i], []))})"
                    child.disabled = False
                else:
                    child.disabled = True
            
            embed = self._generate_vote_embed(vote_data)
            
            if composite_bytes_list := vote_data.get("composite_bytes"):
                composite_bytes = bytes(composite_bytes_list)
                img_file = discord.File(io.BytesIO(composite_bytes), filename="map_vote.png")
                embed.set_image(url="attachment://map_vote.png")
                await message.edit(embed=embed, view=view, attachments=[img_file])
            else:
                await message.edit(embed=embed, view=view, attachments=[])
                
        except discord.NotFound:
            log_map.error(f"Message {message.id} not found for update")
        except Exception as e:
            log_map.error(f"Failed to update vote display: {e}", exc_info=True)


    async def conclude_vote(self, guild_id_str: str, message_id_str: str, ended_by: Optional[discord.User] = None):
        winner_image_url = None
        async with self.config_lock:
            # Use guild_id_str directly (CRITICAL FIX 5)
            guild_cfg = self._get_guild_config_sync(guild_id_str)
            # Atomic pop ensures this only runs once
            if not (vote := guild_cfg.get("active_votes", {}).pop(message_id_str, None)):
                return

            pool = [m for m, v in vote.get("votes", {}).items() for _ in v]
            winner = random.choice(pool) if pool else random.choice(vote["maps"])

            if game_name := vote.get("game"):
                self._ensure_latest_game_format(game_name)
                games_cfg = self._get_games_config_sync()
                gs = games_cfg.setdefault(game_name, {})
                gs.setdefault("win_history", {})[winner] = gs["win_history"].get(winner, 0) + 1
                winner_image_url = gs.get("maps", {}).get(winner, {}).get("url")

                # FIX: Clear reserved maps for this game since vote concluded successfully
                guild_cfg.get("reserved_maps", {}).pop(game_name, None)
            
            for m, uids in vote.get("votes", {}).items():
                for uid in uids:
                    p = guild_cfg.setdefault("user_stats", {}).setdefault(str(uid), {"total_votes": 0, "wins": 0, "map_votes": {}})
                    p["total_votes"] += 1; p["map_votes"][m] = p["map_votes"].get(m, 0) + 1
                    if m == winner: p["wins"] += 1
            await self._save_config()
            
        # --- CRITICAL FIX 4 ---
        # Use .get() for channel_id
        if not (channel_id := vote.get("channel_id")) or not (channel := self.bot.get_channel(channel_id)):
             log_map.warning(f"Could not find channel_id for concluded vote {message_id_str}")
             return
        # --- END FIX ---
        
        voters = {u for ul in vote.get("votes", {}).values() for u in ul}
        desc = f"The chosen map is **{winner}**!"
        if ended_by: desc += f"\n\n*Ended early by {ended_by.mention}.*"
        elif len(voters) >= vote.get("max_votes", 999): desc += f"\n\n*Concluded at {vote.get('max_votes')} votes.*"
        
        res_embed = discord.Embed(title="Map Vote Concluded", color=discord.Color.gold(), description=desc)
        total_v = sum(len(v) for v in vote.get("votes", {}).values())
        for m, v in sorted(vote.get("votes", {}).items(), key=lambda i: len(i[1]), reverse=True):
            res_embed.add_field(name=m, value=f"{len(v)} Votes ({(len(v)/total_v*100) if total_v else 0:.1f}% chance)")
        if winner_image_url: res_embed.set_image(url=winner_image_url)
        
        try:
            msg = await channel.fetch_message(int(message_id_str))
            disabled_view = VotingView(self)
            maps, final_votes = vote.get("maps", []), vote.get("votes", {})
            map_buttons = [c for c in disabled_view.children if c.custom_id and c.custom_id.startswith("map_vote_")]
            for i, child in enumerate(map_buttons):
                if i < len(maps): child.label = f"{maps[i]} ({len(final_votes.get(maps[i], []))})"
                child.disabled = True
            for item in disabled_view.children: item.disabled = True
            await msg.edit(view=disabled_view); await msg.reply(embed=res_embed)
        except (discord.NotFound, discord.Forbidden) as e: log_map.warning(f"Failed conclude reply: {e}")

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

        # --- CRITICAL FIX 7 ---
        # Add try/except block to loop
        for gid, mid, vote in to_process:
            try:
                voter_count = len({u for ul in vote.get("votes", {}).values() for u in ul})
                if voter_count >= vote.get("min_users", 1):
                    await self.conclude_vote(gid, mid)
                else:
                    await self.cancel_vote(gid, mid, vote)
            except Exception as e:
                log_map.error(f"Error processing vote {mid} in guild {gid}: {e}", exc_info=True)
        # --- END FIX ---

    @tasks.loop(hours=24)
    async def clean_image_cache_loop(self):
        log_map.info("Running daily image cache cleanup...")
        now = time.time()
        cutoff = now - (CACHE_LIFETIME_DAYS * 86400)
        cache_dir = Path(IMAGE_CACHE_DIR)
        
        cleaned_count = 0
        try:
            for file_path in cache_dir.glob("*.png"):
                try:
                    if file_path.stat().st_mtime < cutoff:
                        file_path.unlink()
                        cleaned_count += 1
                except OSError as e:
                    log_map.error(f"Error removing cache file {file_path}: {e}")
            if cleaned_count > 0:
                log_map.info(f"Cleaned {cleaned_count} old image(s) from the cache.")
        except Exception as e:
            log_map.error(f"An unexpected error occurred during cache cleanup: {e}")

    @vote_check_loop.before_loop
    @clean_image_cache_loop.before_loop
    async def before_loops(self): await self.bot.wait_until_ready();

    # --- Command Group ---
    mapvote = app_commands.Group(name="mapvote", description="Commands for map voting and configuration.")

    @mapvote.command(name="start", description="Start a vote to pick a map for a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(game="The game to vote on.", duration="Vote duration in minutes (1-10).", min_users="Minimum users required for the vote to pass.")
    async def start(self, inter: discord.Interaction, game: str, duration: app_commands.Range[int, 1, 10]=2, min_users: app_commands.Range[int, 1, 12]=6):
        await inter.response.defer(ephemeral=True)
        
        # The entire /start command is wrapped in a lock to prevent
        # the race condition from Point 6.
        async with self.config_lock:
            guild_cfg = self._get_guild_config_sync(inter.guild_id)
            games_cfg = self._get_games_config_sync()
            
            self._ensure_latest_game_format(game)
            gd = games_cfg.get(game)
            
            if not gd: return await inter.followup.send(f"❌ Game '{game}' not configured.", ephemeral=True)
            if min_users > gd.get("max_votes", 10): return await inter.followup.send(f"❌ Minimum users ({min_users}) cannot be greater than the max votes for this game ({gd.get('max_votes', 10)}).", ephemeral=True)
            
            unseen = gd.get("unseen_maps", [])
            seen = gd.get("seen_maps", [])

            if len(unseen) + len(seen) < VOTE_MAP_COUNT:
                return await inter.followup.send(f"❌ '{game}' needs at least {VOTE_MAP_COUNT} maps.", ephemeral=True)

            # FIX: Check for reserved maps from cancelled vote (anti-reroll)
            reserved_maps = guild_cfg.get("reserved_maps", {}).get(game)
            chosen_maps = []

            if reserved_maps and len(reserved_maps) == VOTE_MAP_COUNT:
                # Validate that all reserved maps still exist in the game's map pool
                all_maps = set(unseen + seen)
                valid_reserved = [m for m in reserved_maps if m in all_maps]

                if len(valid_reserved) == VOTE_MAP_COUNT:
                    # Use the reserved maps from the cancelled vote
                    chosen_maps = valid_reserved
                    log_map.info(f"Using reserved maps {chosen_maps} for {game} vote in guild {inter.guild_id}")

                    # Clear the reservation now that we're using them
                    guild_cfg.get("reserved_maps", {}).pop(game, None)

                    # Update seen/unseen pools to reflect these maps being used
                    gd["unseen_maps"] = [m for m in unseen if m not in chosen_maps]
                    gd["seen_maps"] = list(set(seen).union(chosen_maps))
                else:
                    # Some reserved maps were removed, clear the reservation and pick randomly
                    log_map.warning(f"Reserved maps invalid for {game} (some maps removed), picking randomly")
                    guild_cfg.get("reserved_maps", {}).pop(game, None)
                    reserved_maps = None  # Fall through to random selection
            elif len(unseen) >= VOTE_MAP_COUNT:
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
            
            img_file = await self.create_composite_image(chosen_maps, game)
            embed = self._generate_vote_embed(vote_data)
            view = VotingView(self)
            
            for i, child in enumerate(c for c in view.children if c.custom_id and c.custom_id.startswith("map_vote_")):
                if i < len(chosen_maps): child.label = f"{chosen_maps[i]} (0)"; child.disabled = False
                else: child.disabled = True
            
            msg = None
            if img_file:
                # --- CRITICAL FIX 3 ---
                # Read bytes once, store, then create new buffer for sending
                img_bytes = img_file.fp.read()
                vote_data["composite_bytes"] = list(img_bytes)  # Store bytes
                
                img_file.fp.close() # Close original buffer
                send_buffer = io.BytesIO(img_bytes)
                send_file = discord.File(send_buffer, filename=img_file.filename)
                
                embed.set_image(url=f"attachment://{send_file.filename}")
                msg = await inter.channel.send(file=send_file, embed=embed, view=view)
                # --- END FIX ---
            else:
                msg = await inter.channel.send(embed=embed, view=view)

            await inter.followup.send("Vote started!", ephemeral=True)

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
        added, existing = [], []
        map_names = [m.strip() for m in maps_str.split(',') if m.strip()]
        if not map_names: return await inter.followup.send("❌ No map names provided.", ephemeral=True)
        async with self.config_lock:
            games_cfg = self._get_games_config_sync()
            if not (gd := games_cfg.get(game)): return await inter.followup.send(f"❌ Game '{game}' not found.", ephemeral=True)
            self._ensure_latest_game_format(game)
            pool = set(gd.get("unseen_maps", []) + gd.get("seen_maps", []))
            for name in map_names:
                if name not in pool: gd["unseen_maps"].append(name); gd["maps"][name] = {"url": None}; added.append(name)
                else: existing.append(name)
            if added: await self._save_config()
        desc = (f"✅ Added: `{'`, `'.join(added)}`\n" if added else "") + (f"⚠️ Existed: `{'`, `'.join(existing)}`" if existing else "")
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
            self._get_games_config_sync()[game]["maps"][map_name]["url"] = url if url else None
            await self._save_config()
        desc = f"✅ Thumbnail for **{map_name}** has been {'set' if url else 'removed'}."
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
        embed = discord.Embed(title=f"🏆 Stats for {game}", color=EMBED_COLOR_MAP, description="\n".join(stats)).set_footer(text=f"Based on {total} total wins.")
        await inter.followup.send(embed=embed, ephemeral=True)

async def setup(bot: commands.Bot):
    """The setup function called by discord.py to load the cog."""
    cog = MapVote(bot)
    bot.add_view(VotingView(cog))
    await bot.add_cog(cog)


