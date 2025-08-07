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
from typing import Optional, Dict, Any, List

# --- Constants ---
CONFIG_FILE_MAP = "map_voter_config.json"
EMBED_COLOR_MAP = 0xE91E63
ADMIN_EMBED_COLOR = 0x3498DB
VOTE_MAP_COUNT = 3
TOURNAMENT_MAP_COUNT = 8

log_map = logging.getLogger(__name__)

# --- Configuration Management ---
class ConfigManager:
    """Handles loading and saving the JSON configuration file with a lock."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.lock = asyncio.Lock()

    async def load(self) -> Dict[str, Any]:
        """Loads the configuration from the JSON file asynchronously."""
        async with self.lock:
            if not os.path.exists(self.file_path):
                return {}
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                log_map.error(f"Error loading config file {self.file_path}: {e}")
                return {}

    async def save(self, data: Dict[str, Any]):
        """Saves the configuration to the JSON file asynchronously."""
        async with self.lock:
            try:
                with open(self.file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            except IOError as e:
                log_map.error(f"Error saving config file {self.file_path}: {e}")

# --- UI Components ---
class VotingView(discord.ui.View):
    """A persistent view for the main map voting poll."""
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__(timeout=None)
        self.cog = cog_instance

    async def handle_vote_interaction(self, interaction: discord.Interaction, button: discord.ui.Button, map_index: int):
        await interaction.response.defer(ephemeral=True, thinking=True)
        success, message = await self.cog.process_vote(interaction, map_index)
        await interaction.followup.send(message, ephemeral=True)
        if success:
            await self.cog.update_vote_message(interaction.message)

    @discord.ui.button(label="Map 1", style=discord.ButtonStyle.secondary, emoji="1️⃣", custom_id="map_vote_1")
    async def map_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_vote_interaction(interaction, button, 0)

    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.secondary, emoji="2️⃣", custom_id="map_vote_2")
    async def map_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_vote_interaction(interaction, button, 1)

    @discord.ui.button(label="Map 3", style=discord.ButtonStyle.secondary, emoji="3️⃣", custom_id="map_vote_3")
    async def map_3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_vote_interaction(interaction, button, 2)

class TournamentView(discord.ui.View):
    """A view for a single 1v1 tournament matchup. Not persistent."""
    def __init__(self, map1: str, map2: str):
        super().__init__(timeout=None)
        self.votes: Dict[int, str] = {}
        self.map1_name = map1
        self.map2_name = map2
        self.children[0].label = map1
        self.children[1].label = map2

    @discord.ui.button(label="Map 1", style=discord.ButtonStyle.primary)
    async def map1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.votes[interaction.user.id] = self.map1_name
        await interaction.response.send_message(f"You voted for **{self.map1_name}**.", ephemeral=True)

    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.primary)
    async def map2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.votes[interaction.user.id] = self.map2_name
        await interaction.response.send_message(f"You voted for **{self.map2_name}**.", ephemeral=True)

# --- Admin Panel Modals ---

class GameModal(discord.ui.Modal):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="e.g., Valorant, CS:GO, Overwatch")
    def __init__(self, title: str, cog_instance: 'MapVoter'):
        super().__init__(title=title)
        self.cog = cog_instance

class AddMapsModal(discord.ui.Modal, title="Add Maps to a Game"):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="The game to add maps to")
    maps = discord.ui.TextInput(label="Map Names (comma-separated)", style=discord.TextStyle.paragraph, placeholder="e.g., Ascent, Bind, Haven")
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__()
        self.cog = cog_instance

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.logic_add_maps(interaction, self.game_name.value, self.maps.value)

class RemoveMapsModal(discord.ui.Modal, title="Remove Maps from a Game"):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="The game to remove maps from")
    maps = discord.ui.TextInput(label="Map Names (comma-separated)", style=discord.TextStyle.paragraph, placeholder="e.g., Ascent, Bind, Haven")
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__()
        self.cog = cog_instance

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.logic_remove_maps(interaction, self.game_name.value, self.maps.value)

class EndVoteModal(discord.ui.Modal, title="Manually End a Vote"):
    vote_id = discord.ui.TextInput(label="Vote ID #", placeholder="Find the ID from /mapadmin > List Votes")
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__()
        self.cog = cog_instance

    async def on_submit(self, interaction: discord.Interaction):
        try:
            vote_id_int = int(self.vote_id.value)
            await self.cog.logic_end_vote(interaction, vote_id_int)
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number for the Vote ID.", ephemeral=True)

class MapStatsModal(discord.ui.Modal, title="View Map Statistics"):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="The game to view stats for")
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__()
        self.cog = cog_instance

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.logic_map_stats(interaction, self.game_name.value)

class TournamentModal(discord.ui.Modal, title="Start a Map Tournament"):
    game_name = discord.ui.TextInput(label="Game Name", placeholder="The game for the tournament")
    duration = discord.ui.TextInput(label="Vote Duration per Match (seconds)", placeholder="e.g., 30 (10-120 seconds)")
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__()
        self.cog = cog_instance
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            duration_int = int(self.duration.value)
            if not 10 <= duration_int <= 120:
                raise ValueError("Duration out of range.")
            await self.cog.logic_tournament(interaction, self.game_name.value, duration_int)
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number between 10 and 120 for the duration.", ephemeral=True)


# --- Admin Panel View ---
class AdminPanelView(discord.ui.View):
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__(timeout=180)
        self.cog = cog_instance

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # This check is technically redundant due to the command's permissions, but it's good practice
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to use this panel.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Add Game", style=discord.ButtonStyle.green, row=0)
    async def add_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = GameModal(title="Add a New Game", cog_instance=self.cog)
        async def on_submit(inter: discord.Interaction):
            await self.cog.logic_add_game(inter, modal.game_name.value)
        modal.on_submit = on_submit
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Remove Game", style=discord.ButtonStyle.red, row=0)
    async def remove_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = GameModal(title="Remove a Game", cog_instance=self.cog)
        async def on_submit(inter: discord.Interaction):
            await self.cog.logic_remove_game(inter, modal.game_name.value)
        modal.on_submit = on_submit
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Add Maps", style=discord.ButtonStyle.green, row=1)
    async def add_maps(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddMapsModal(self.cog))

    @discord.ui.button(label="Remove Maps", style=discord.ButtonStyle.red, row=1)
    async def remove_maps(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RemoveMapsModal(self.cog))

    @discord.ui.button(label="List Active Votes", style=discord.ButtonStyle.blurple, row=2)
    async def list_votes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.logic_list_votes(interaction)

    @discord.ui.button(label="End a Vote", style=discord.ButtonStyle.danger, row=2)
    async def end_vote(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EndVoteModal(self.cog))

    @discord.ui.button(label="Start Tournament", style=discord.ButtonStyle.blurple, row=3)
    async def start_tournament(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(TournamentModal(self.cog))

    @discord.ui.button(label="View Map Stats", style=discord.ButtonStyle.secondary, row=4)
    async def map_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(MapStatsModal(self.cog))

    @discord.ui.button(label="View My Stats", style=discord.ButtonStyle.secondary, row=4)
    async def my_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.logic_my_stats(interaction, interaction.user)


# --- Main Cog ---
class MapVoter(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config_manager = ConfigManager(CONFIG_FILE_MAP)
        self.active_config: Dict[str, Any] = {}
        self.vote_check_loop.start()
        self.bot.add_view(VotingView(self))

    async def cog_load(self):
        self.active_config = await self.config_manager.load()

    def cog_unload(self):
        self.vote_check_loop.cancel()

    # --- Helper & Utility Methods ---
    def get_footer_text(self) -> str:
        return f"{self.bot.user.name} • Map Voter"

    def _ensure_new_map_format(self, guild_config: dict, game_name: str):
        games = guild_config.setdefault("games", {})
        if game_name in games and isinstance(games[game_name], list):
            log_map.info(f"Migrating map pool for game '{game_name}' to new format.")
            games[game_name] = {"unseen_maps": games[game_name], "seen_maps": [], "win_history": {}}

    async def _get_guild_config(self, guild_id: int) -> Dict[str, Any]:
        return self.active_config.setdefault(str(guild_id), {})

    async def _save_config(self):
        await self.config_manager.save(self.active_config)

    # --- Autocomplete Handlers ---
    async def game_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        guild_cfg = await self._get_guild_config(interaction.guild_id)
        games = guild_cfg.get("games", {}).keys()
        return [app_commands.Choice(name=game, value=game) for game in games if current.lower() in game.lower()][:25]

    # --- Vote Processing Logic ---
    async def process_vote(self, interaction: discord.Interaction, map_index: int) -> (bool, str):
        gid_str = str(interaction.guild_id)
        msg_id_str = str(interaction.message.id)
        
        async with self.config_manager.lock:
            guild_cfg = self.active_config.setdefault(gid_str, {})
            active_votes = guild_cfg.setdefault("active_votes", {})
            vote_data = active_votes.get(msg_id_str)

            if not vote_data:
                return False, "This vote seems to have expired or been removed."

            end_time = datetime.fromisoformat(vote_data["end_time_iso"])
            if datetime.now(timezone.utc) > end_time:
                return False, "❌ This vote has already ended."

            map_name = vote_data["maps"][map_index]
            votes = vote_data["votes"]
            
            for roaster in votes.values():
                if interaction.user.id in roaster:
                    roaster.remove(interaction.user.id)
            
            votes[map_name].append(interaction.user.id)
            await self._save_config()
            return True, f"✅ Your vote for **{map_name}** has been recorded."

    async def update_vote_message(self, message: discord.Message):
        gid_str = str(message.guild.id)
        msg_id_str = str(message.id)
        fresh_config = await self.config_manager.load()
        vote_data = fresh_config.get(gid_str, {}).get("active_votes", {}).get(msg_id_str)

        if not vote_data: return
        view = VotingView.from_message(message)
        if not view: return

        for i, child in enumerate(view.children):
            if isinstance(child, discord.ui.Button) and i < len(vote_data["maps"]):
                map_name = vote_data["maps"][i]
                vote_count = len(vote_data["votes"].get(map_name, []))
                child.label = f"{map_name} ({vote_count} Votes)"
        
        try:
            await message.edit(view=view)
        except discord.HTTPException as e:
            log_map.warning(f"Failed to edit message {message.id} after a vote: {e}")

    # --- Vote Lifecycle Management ---
    async def conclude_vote(self, guild_id_str: str, message_id_str: str, ended_by: Optional[discord.User] = None):
        async with self.config_manager.lock:
            vote_data = self.active_config.get(guild_id_str, {}).get("active_votes", {}).pop(message_id_str, None)
            if not vote_data: return

            total_votes = sum(len(v) for v in vote_data["votes"].values())
            if total_votes > 0:
                weighted_pool = [m for m, v in vote_data["votes"].items() for _ in v]
                winner = random.choice(weighted_pool) if weighted_pool else random.choice(vote_data["maps"])
            else:
                winner = random.choice(vote_data["maps"])

            game_name = vote_data.get("game")
            if game_name:
                game_stats = self.active_config[guild_id_str]["games"].setdefault(game_name, {})
                win_history = game_stats.setdefault("win_history", {})
                win_history[winner] = win_history.get(winner, 0) + 1
            
            user_stats = self.active_config[guild_id_str].setdefault("user_stats", {})
            for map_name, user_ids in vote_data["votes"].items():
                for user_id in user_ids:
                    player_data = user_stats.setdefault(str(user_id), {"total_votes": 0, "wins": 0, "map_votes": {}})
                    player_data["total_votes"] += 1
                    player_data["map_votes"][map_name] = player_data["map_votes"].get(map_name, 0) + 1
                    if map_name == winner:
                        player_data["wins"] += 1
            
            await self._save_config()

        channel = self.bot.get_channel(vote_data["channel_id"])
        if not channel: return

        description = f"The chosen map is **{winner}**!"
        if ended_by:
            description += f"\n\n*This vote was ended early by {ended_by.mention}.*"
        
        results_embed = discord.Embed(title="🗳️ Map Vote Concluded!", color=discord.Color.gold(), description=description)
        sorted_votes = sorted(vote_data["votes"].items(), key=lambda item: len(item[1]), reverse=True)
        for map_name_item, voters in sorted_votes:
            percentage = (len(voters) / total_votes * 100) if total_votes > 0 else 0
            results_embed.add_field(name=f"{map_name_item}", value=f"{len(voters)} Votes ({percentage:.1f}%)", inline=False)
        results_embed.set_footer(text=self.get_footer_text())
        
        try:
            original_msg = await channel.fetch_message(int(message_id_str))
            view = VotingView.from_message(original_msg)
            if view:
                for item in view.children: item.disabled = True
                await original_msg.edit(view=view)
            await original_msg.reply(embed=results_embed)
        except (discord.NotFound, discord.Forbidden) as e:
            log_map.warning(f"Could not find or edit original vote message {message_id_str}: {e}")

    async def cancel_vote(self, guild_id_str: str, message_id_str: str, vote_data: dict):
        async with self.config_manager.lock:
            self.active_config.get(guild_id_str, {}).get("active_votes", {}).pop(message_id_str, None)
            
            game_name = vote_data.get("game")
            maps_to_return = vote_data.get("maps", [])
            if game_name and maps_to_return:
                game_data = self.active_config.get(guild_id_str, {}).get("games", {}).get(game_name)
                if game_data:
                    game_data["seen_maps"] = [m for m in game_data.get("seen_maps", []) if m not in maps_to_return]
                    game_data["unseen_maps"].extend(maps_to_return)
                    game_data["unseen_maps"] = list(set(game_data["unseen_maps"]))
            
            await self._save_config()

        channel = self.bot.get_channel(vote_data["channel_id"])
        if not channel: return

        voter_ids = {user_id for user_list in vote_data["votes"].values() for user_id in user_list}
        description = (f"This vote did not meet the minimum requirement of **{vote_data.get('min_users', 1)}** voters.\n"
                       f"Only **{len(voter_ids)}** people voted. The maps have been returned to the pool.")
        cancel_embed = discord.Embed(title="🚫 Map Vote Cancelled", color=discord.Color.red(), description=description)
        cancel_embed.set_footer(text=self.get_footer_text())

        try:
            original_msg = await channel.fetch_message(int(message_id_str))
            view = VotingView.from_message(original_msg)
            if view:
                for item in view.children: item.disabled = True
                await original_msg.edit(view=view)
            await original_msg.reply(embed=cancel_embed)
        except (discord.NotFound, discord.Forbidden) as e:
            log_map.warning(f"Could not find or edit original cancelled vote message {message_id_str}: {e}")

    # --- Background Task ---
    @tasks.loop(seconds=15)
    async def vote_check_loop(self):
        now = datetime.now(timezone.utc)
        all_votes = {
            (gid, msg_id): vote_data
            for gid, g_cfg in self.active_config.items()
            for msg_id, vote_data in g_cfg.get("active_votes", {}).items()
        }

        for (gid, msg_id), vote_data in all_votes.items():
            if now >= datetime.fromisoformat(vote_data["end_time_iso"]):
                voter_ids = {uid for v in vote_data["votes"].values() for uid in v}
                if len(voter_ids) >= vote_data.get("min_users", 1):
                    log_map.info(f"Auto-concluding timed vote {msg_id}")
                    await self.conclude_vote(gid, msg_id)
                else:
                    log_map.info(f"Auto-cancelling timed vote {msg_id}")
                    await self.cancel_vote(gid, msg_id, vote_data)

    @vote_check_loop.before_loop
    async def before_vote_check_loop(self):
        await self.bot.wait_until_ready()
        await self.cog_load()

    # --- Command Groups ---
    mapvote = app_commands.Group(name="mapvote", description="Commands to start and manage map votes.")

    # --- Commands ---
    @mapvote.command(name="start", description="Start a vote to pick a map for a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(game="The game to vote on.", duration="Vote duration in minutes (1-10).", min_users="Minimum users required for the vote to pass.")
    async def mapvote_start(self, interaction: discord.Interaction, game: str, duration: app_commands.Range[int, 1, 10] = 2, min_users: app_commands.Range[int, 1, 12] = 6):
        await interaction.response.defer()
        gid_str = str(interaction.guild_id)

        async with self.config_manager.lock:
            guild_cfg = await self._get_guild_config(interaction.guild_id)
            self._ensure_new_map_format(guild_cfg, game)
            game_data = guild_cfg.get("games", {}).get(game)

            if not game_data:
                embed = discord.Embed(description=f"❌ The game '{game}' is not configured.", color=discord.Color.red())
                return await interaction.followup.send(embed=embed, ephemeral=True)

            unseen, seen = game_data.get("unseen_maps", []), game_data.get("seen_maps", [])
            if len(unseen) + len(seen) < VOTE_MAP_COUNT:
                embed = discord.Embed(description=f"❌ '{game}' needs at least {VOTE_MAP_COUNT} maps to start a vote.", color=discord.Color.red())
                return await interaction.followup.send(embed=embed, ephemeral=True)

            chosen_maps = []
            if len(unseen) < VOTE_MAP_COUNT:
                log_map.info(f"Pool for '{game}' in guild {gid_str} is low. Resetting.")
                chosen_maps.extend(unseen)
                needed = VOTE_MAP_COUNT - len(chosen_maps)
                chosen_maps.extend(random.sample(seen, needed))
                all_maps = unseen + seen
                game_data["seen_maps"] = chosen_maps
                game_data["unseen_maps"] = [m for m in all_maps if m not in chosen_maps]
            else:
                chosen_maps = random.sample(unseen, VOTE_MAP_COUNT)
                game_data["unseen_maps"] = [m for m in unseen if m not in chosen_maps]
                game_data["seen_maps"].extend(chosen_maps)

            vote_id = guild_cfg.get("vote_counter", 0) + 1
            guild_cfg["vote_counter"] = vote_id
            
            end_time = datetime.now(timezone.utc) + timedelta(minutes=duration)
            view = VotingView(self)
            for i, map_name in enumerate(chosen_maps):
                if i < len(view.children): view.children[i].label = f"{map_name} (0 Votes)"

            description = (f"Vote for the map you want to play! The winner is chosen randomly, with more votes increasing a map's chance.\n\n"
                           f"🕒 This vote will automatically conclude <t:{int(end_time.timestamp())}:R>.\n"
                           f"👥 Requires at least **{min_users}** unique voters to be valid.")
            embed = discord.Embed(title=f"🗺️ {game} Map Vote", color=EMBED_COLOR_MAP, description=description)
            embed.set_footer(text=f"Vote ID: #{vote_id}  •  {self.get_footer_text()}")
            
            message = await interaction.followup.send(embed=embed, view=view, wait=True)

            active_votes = guild_cfg.setdefault("active_votes", {})
            active_votes[str(message.id)] = {"channel_id": message.channel.id, "end_time_iso": end_time.isoformat(), "maps": chosen_maps, "votes": {map_name: [] for map_name in chosen_maps}, "game": game, "short_id": vote_id, "min_users": min_users}
            await self._save_config()
    
    @app_commands.command(name="mapadmin", description="Access the Map Voter admin panel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def mapadmin_panel(self, interaction: discord.Interaction):
        embed = discord.Embed(title="⚙️ Map Voter Admin Panel", description="Select an administrative action from the buttons below.", color=ADMIN_EMBED_COLOR)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, view=AdminPanelView(self), ephemeral=True)

    # --- Logic for Admin Panel Functions ---
    async def logic_add_game(self, interaction: discord.Interaction, name: str):
        async with self.config_manager.lock:
            guild_cfg = await self._get_guild_config(interaction.guild_id)
            games = guild_cfg.setdefault("games", {})
            if name in games:
                desc, color = f"❌ The game '{name}' already exists.", discord.Color.yellow()
            else:
                games[name] = {"unseen_maps": [], "seen_maps": [], "win_history": {}}
                await self._save_config()
                desc, color = f"✅ Game '{name}' has been added.", EMBED_COLOR_MAP
        await interaction.response.send_message(embed=discord.Embed(description=desc, color=color), ephemeral=True)

    async def logic_remove_game(self, interaction: discord.Interaction, name: str):
        async with self.config_manager.lock:
            guild_cfg = await self._get_guild_config(interaction.guild_id)
            games = guild_cfg.setdefault("games", {})
            if name not in games:
                desc, color = f"❌ The game '{name}' does not exist.", discord.Color.red()
            else:
                del games[name]
                await self._save_config()
                desc, color = f"✅ Game '{name}' has been removed.", EMBED_COLOR_MAP
        await interaction.response.send_message(embed=discord.Embed(description=desc, color=color), ephemeral=True)
        
    async def logic_add_maps(self, interaction: discord.Interaction, game: str, maps: str):
        await interaction.response.defer(ephemeral=True)
        added_maps, existing_maps = [], []
        map_names = [m.strip() for m in maps.split(',') if m.strip()]
        if not map_names:
            return await interaction.followup.send("❌ You must provide at least one map name.", ephemeral=True)
        
        async with self.config_manager.lock:
            guild_cfg = await self._get_guild_config(interaction.guild_id)
            self._ensure_new_map_format(guild_cfg, game)
            game_data = guild_cfg.get("games", {}).get(game)

            if not isinstance(game_data, dict):
                return await interaction.followup.send(f"❌ The game '{game}' does not exist.", ephemeral=True)
            
            current_pool = game_data.get("unseen_maps", []) + game_data.get("seen_maps", [])
            for map_name in map_names:
                if map_name not in current_pool:
                    game_data["unseen_maps"].append(map_name)
                    added_maps.append(map_name)
                else:
                    existing_maps.append(map_name)
            
            if added_maps:
                await self._save_config()

        description = ""
        if added_maps: description += f"✅ Added **{len(added_maps)}** new map(s) to **{game}**: `{'`, `'.join(added_maps)}`\n"
        if existing_maps: description += f"⚠️ **{len(existing_maps)}** map(s) already existed: `{'`, `'.join(existing_maps)}`"
        await interaction.followup.send(embed=discord.Embed(description=description.strip(), color=EMBED_COLOR_MAP), ephemeral=True)

    async def logic_remove_maps(self, interaction: discord.Interaction, game: str, maps: str):
        await interaction.response.defer(ephemeral=True)
        removed_maps, not_found_maps = [], []
        map_names = [m.strip() for m in maps.split(',') if m.strip()]
        if not map_names:
            return await interaction.followup.send("❌ You must provide at least one map name.", ephemeral=True)

        async with self.config_manager.lock:
            guild_cfg = await self._get_guild_config(interaction.guild_id)
            game_data = guild_cfg.get("games", {}).get(game)

            if not isinstance(game_data, dict):
                return await interaction.followup.send(f"❌ The game '{game}' does not exist.", ephemeral=True)

            for map_name in map_names:
                removed = False
                if map_name in game_data.get("unseen_maps", []):
                    game_data["unseen_maps"].remove(map_name)
                    removed = True
                elif map_name in game_data.get("seen_maps", []):
                    game_data["seen_maps"].remove(map_name)
                    removed = True
                
                if removed: removed_maps.append(map_name)
                else: not_found_maps.append(map_name)
            
            if removed_maps:
                await self._save_config()

        description = ""
        if removed_maps: description += f"✅ Removed **{len(removed_maps)}** map(s) from **{game}**: `{'`, `'.join(removed_maps)}`\n"
        if not_found_maps: description += f"⚠️ **{len(not_found_maps)}** map(s) were not found: `{'`, `'.join(not_found_maps)}`"
        await interaction.followup.send(embed=discord.Embed(description=description.strip(), color=EMBED_COLOR_MAP), ephemeral=True)

    async def logic_list_votes(self, interaction: discord.Interaction):
        guild_cfg = await self._get_guild_config(interaction.guild_id)
        active_votes = guild_cfg.get("active_votes", {})
        if not active_votes:
            return await interaction.response.send_message("There are no active map votes right now.", ephemeral=True)

        embed = discord.Embed(title="Active Map Votes", color=EMBED_COLOR_MAP)
        for msg_id, vote_data in active_votes.items():
            end_time = datetime.fromisoformat(vote_data["end_time_iso"])
            game = vote_data.get("game", "N/A")
            vote_id = vote_data.get("short_id", "N/A")
            field_value = (f"**Game**: {game}\n**Maps**: {', '.join(vote_data['maps'])}\n**Ends**: <t:{int(end_time.timestamp())}:R>")
            embed.add_field(name=f"Vote ID: #{vote_id}", value=field_value, inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def logic_end_vote(self, interaction: discord.Interaction, vote_id: int):
        gid_str = str(interaction.guild_id)
        guild_cfg = await self._get_guild_config(interaction.guild_id)
        active_votes = guild_cfg.get("active_votes", {})
        message_id_to_end = None
        for msg_id, vote_data in active_votes.items():
            if vote_data.get("short_id") == vote_id:
                message_id_to_end = msg_id
                break
        
        if message_id_to_end:
            await interaction.response.send_message(f"✅ Ending vote #{vote_id}...", ephemeral=True)
            await self.conclude_vote(gid_str, message_id_to_end, ended_by=interaction.user)
        else:
            await interaction.response.send_message(f"❌ Could not find an active vote with ID #{vote_id}.", ephemeral=True)

    async def logic_my_stats(self, interaction: discord.Interaction, user: discord.Member):
        await interaction.response.defer(ephemeral=True)
        guild_cfg = await self._get_guild_config(interaction.guild_id)
        user_stats = guild_cfg.get("user_stats", {}).get(str(user.id))

        if not user_stats or not user_stats.get("total_votes"):
            embed = discord.Embed(description=f"{user.mention} has not voted in any map polls yet.", color=discord.Color.yellow())
            return await interaction.followup.send(embed=embed, ephemeral=True)

        total_votes = user_stats.get("total_votes", 0)
        wins = user_stats.get("wins", 0)
        win_rate = (wins / total_votes * 100) if total_votes > 0 else 0

        embed = discord.Embed(title=f"📊 Voting Stats for {user.display_name}", color=EMBED_COLOR_MAP)
        embed.set_thumbnail(url=user.display_avatar.url)
        embed.add_field(name="Total Votes Cast", value=f"`{total_votes}`", inline=True)
        embed.add_field(name="Votes Won", value=f"`{wins}`", inline=True)
        embed.add_field(name="Win Rate", value=f"`{win_rate:.1f}%`", inline=True)

        map_votes = user_stats.get("map_votes", {})
        if map_votes:
            sorted_maps = sorted(map_votes.items(), key=lambda item: item[1], reverse=True)
            top_maps_str = "\n".join([f"• **{m}**: {v} times" for m, v in sorted_maps[:5]])
            embed.add_field(name="Most Voted For Maps", value=top_maps_str, inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def logic_map_stats(self, interaction: discord.Interaction, game: str):
        await interaction.response.defer(ephemeral=True)
        guild_cfg = await self._get_guild_config(interaction.guild_id)
        game_data = guild_cfg.get("games", {}).get(game, {})

        if not game_data:
            return await interaction.followup.send(f"The game **{game}** has not been configured.", ephemeral=True)
        
        all_maps = sorted(game_data.get("unseen_maps", []) + game_data.get("seen_maps", []))
        if not all_maps:
            return await interaction.followup.send(f"There are no maps in the pool for **{game}**.", ephemeral=True)

        win_history = game_data.get("win_history", {})
        total_wins = sum(win_history.values())
        
        stats_lines = [f"• **{m}**: {win_history.get(m, 0)} wins ({(win_history.get(m, 0) / total_wins * 100) if total_wins > 0 else 0:.1f}%)" for m in all_maps]
        
        embed = discord.Embed(title=f"🏆 Map Stats for {game}", color=EMBED_COLOR_MAP, description="\n".join(stats_lines))
        embed.set_footer(text=f"Based on {total_wins} total concluded votes.")
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def logic_tournament(self, interaction: discord.Interaction, game: str, vote_duration: int):
        await interaction.response.send_message(f"🔥 Starting a map tournament for **{game}**! Check the channel for updates.", ephemeral=True)
        guild_cfg = await self._get_guild_config(interaction.guild_id)
        game_data = guild_cfg.get("games", {}).get(game)
        if not game_data:
            return await interaction.followup.send(f"❌ The game '{game}' is not configured.", ephemeral=True)
        
        all_maps = game_data.get("unseen_maps", []) + game_data.get("seen_maps", [])
        if len(all_maps) < TOURNAMENT_MAP_COUNT:
            return await interaction.followup.send(f"❌ '{game}' needs at least {TOURNAMENT_MAP_COUNT} maps.", ephemeral=True)
        
        maps = random.sample(all_maps, TOURNAMENT_MAP_COUNT)
        current_round_maps = maps
        round_names = ["Quarter-Finals", "Semi-Finals", "Finals"]

        bracket_embed = discord.Embed(title=f"🏆 {game} Map Tournament Bracket", color=EMBED_COLOR_MAP)
        bracket_embed.add_field(name="Quarter-Finals", value="\n".join([f"`{maps[i]}` vs `{maps[i+1]}`" for i in range(0, 8, 2)]), inline=False)
        bracket_embed.add_field(name="Semi-Finals", value="*TBD*", inline=False)
        bracket_embed.add_field(name="Finals", value="*TBD*", inline=False)
        bracket_msg = await interaction.channel.send(embed=bracket_embed)

        for i, round_name in enumerate(round_names):
            winners = []
            matchups = list(zip(current_round_maps[0::2], current_round_maps[1::2]))
            
            for map1, map2 in matchups:
                winner = await self.run_matchup(interaction.channel, f"{round_name}: {map1} vs {map2}", map1, map2, vote_duration)
                winners.append(winner)
                await interaction.channel.send(f"**{winner}** wins the match and advances!")
            
            completed_field_value = "\n".join([f"~~`{m1}` vs `{m2}`~~ -> **{w}**" for (m1, m2), w in zip(matchups, winners)])
            bracket_embed.set_field_at(i, name=f"{round_name} (Complete)", value=completed_field_value, inline=False)

            if len(winners) == 1: break

            next_round_matchups = "\n".join([f"`{winners[j]}` vs `{winners[j+1]}`" for j in range(0, len(winners), 2)])
            if i + 1 < len(round_names):
                 bracket_embed.set_field_at(i + 1, name=round_names[i+1], value=next_round_matchups, inline=False)

            await bracket_msg.edit(embed=bracket_embed)
            current_round_maps = winners

        champion = current_round_maps[0]
        final_field = bracket_embed.fields[-1]
        bracket_embed.set_field_at(len(bracket_embed.fields)-1, name=final_field.name, value=f"{final_field.value.split('->')[0]}-> 🏆 **{champion}** 🏆", inline=False)
        await bracket_msg.edit(embed=bracket_embed)
        await interaction.channel.send(f"👑 The tournament champion for **{game}** is **{champion}**!")

    async def run_matchup(self, channel: discord.TextChannel, title: str, map1: str, map2: str, duration: int) -> str:
        view = TournamentView(map1, map2)
        end_time = datetime.now(timezone.utc) + timedelta(seconds=duration)
        embed = discord.Embed(title=title, color=discord.Color.gold(), description=f"Vote now! Poll ends <t:{int(end_time.timestamp())}:R>.")
        msg = await channel.send(embed=embed, view=view)
        
        await asyncio.sleep(duration)
        
        for item in view.children: item.disabled = True
        await msg.edit(view=view)
        
        vote_counts = Counter(view.votes.values())
        map1_votes = vote_counts.get(map1, 0)
        map2_votes = vote_counts.get(map2, 0)

        if map1_votes == map2_votes:
            winner = random.choice([map1, map2])
            await channel.send(f"The vote was a tie! Randomly selected **{winner}** to advance.")
            return winner
        else:
            return map1 if map1_votes > map2_votes else map2


async def setup(bot: commands.Bot):
    await bot.add_cog(MapVoter(bot))
