import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import random
import asyncio
from datetime import datetime, timedelta, timezone
import logging

log_map = logging.getLogger(__name__)

CONFIG_FILE_MAP = "map_voter_config.json"
EMBED_COLOR_MAP = 0xE91E63

def load_config_map():
    if os.path.exists(CONFIG_FILE_MAP):
        try:
            with open(CONFIG_FILE_MAP, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_map(config):
    with open(CONFIG_FILE_MAP, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

class VotingView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="Map 1", style=discord.ButtonStyle.secondary, emoji="1️⃣", custom_id="map_vote_1")
    async def map_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = interaction.client.get_cog("MapVoter"); 
        if cog: await cog.handle_vote(interaction, 0)
    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.secondary, emoji="2️⃣", custom_id="map_vote_2")
    async def map_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = interaction.client.get_cog("MapVoter"); 
        if cog: await cog.handle_vote(interaction, 1)
    @discord.ui.button(label="Map 3", style=discord.ButtonStyle.secondary, emoji="3️⃣", custom_id="map_vote_3")
    async def map_3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = interaction.client.get_cog("MapVoter"); 
        if cog: await cog.handle_vote(interaction, 2)

class MapVoter(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_map()
        self.config_lock = asyncio.Lock()
        self.vote_check_loop.start()
        self.bot.add_view(VotingView())

    def cog_unload(self):
        self.vote_check_loop.cancel()
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Map Voter"

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            embed = discord.Embed(description="❌ You do not have the required permissions for this command.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            log_map.error(f"An unhandled error occurred in a command: {error}")

    def _ensure_new_map_format(self, guild_id_str: str, game_name: str):
        games = self.config.get(guild_id_str, {}).get("games", {})
        if game_name in games and isinstance(games[game_name], list):
            log_map.info(f"Migrating map pool for game '{game_name}' in guild {guild_id_str} to new format.")
            games[game_name] = { "unseen_maps": games[game_name], "seen_maps": [] }
            save_config_map(self.config)

    async def game_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        guild_cfg = self.config.get(str(interaction.guild_id), {})
        games = guild_cfg.get("games", {}).keys()
        return [app_commands.Choice(name=game, value=game) for game in games if current.lower() in game.lower()][:25]

    async def map_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        game = interaction.namespace.game
        if not game: return []
        gid = str(interaction.guild_id)
        self._ensure_new_map_format(gid, game)
        guild_cfg = self.config.get(gid, {})
        map_pool = guild_cfg.get("games", {}).get(game, {})
        all_maps = map_pool.get("unseen_maps", []) + map_pool.get("seen_maps", [])
        return [app_commands.Choice(name=map_name, value=map_name) for map_name in all_maps if current.lower() in map_name.lower()][:25]

    async def handle_vote(self, interaction: discord.Interaction, map_index: int):
        async with self.config_lock:
            current_config = load_config_map()
            gid_str = str(interaction.guild_id)
            active_votes = current_config.setdefault(gid_str, {}).setdefault("active_votes", {})
            vote_data = active_votes.get(str(interaction.message.id))

            if not vote_data:
                return await interaction.response.send_message("This vote seems to have expired or been removed.", ephemeral=True)

            end_time = datetime.fromisoformat(vote_data["end_time_iso"])
            if datetime.now(timezone.utc) > end_time:
                return await interaction.response.send_message("❌ This vote has already ended.", ephemeral=True)

            map_name = vote_data["maps"][map_index]
            votes = vote_data["votes"]

            for map_votes in votes.values():
                if interaction.user.id in map_votes:
                    map_votes.remove(interaction.user.id)
            
            votes[map_name].append(interaction.user.id)
            save_config_map(current_config)
            self.config = current_config

        view = VotingView.from_message(interaction.message)
        for i, child in enumerate(view.children):
            if isinstance(child, discord.ui.Button):
                map_name_for_button = vote_data["maps"][i]
                child.label = f"{map_name_for_button} ({len(votes[map_name_for_button])} Votes)"
        
        await interaction.message.edit(view=view)
        await interaction.response.send_message(f"✅ Your vote for **{map_name}** has been recorded.", ephemeral=True)

    async def conclude_vote(self, guild_id_str: str, message_id_str: str):
        async with self.config_lock:
            current_config = load_config_map()
            vote_data = current_config.setdefault(guild_id_str, {}).setdefault("active_votes", {}).pop(message_id_str, None)
            if not vote_data: return
            save_config_map(current_config)
            self.config = current_config

        channel = self.bot.get_channel(vote_data["channel_id"])
        if not channel: return
        
        total_votes = sum(len(v) for v in vote_data["votes"].values())
        winner = random.choice(vote_data["maps"])
        if total_votes > 0:
            weighted_pool = [m for m, v in vote_data["votes"].items() for _ in v]
            if weighted_pool: winner = random.choice(weighted_pool)

        results_embed = discord.Embed(title="🗳️ Map Vote Concluded!", color=discord.Color.gold(), description=f"The chosen map is **{winner}**!", timestamp=datetime.now(timezone.utc))
        results_embed.set_footer(text=self.get_footer_text())
        sorted_votes = sorted(vote_data["votes"].items(), key=lambda item: len(item[1]), reverse=True)
        for map_name, voters in sorted_votes:
            percentage = (len(voters) / total_votes * 100) if total_votes > 0 else 0
            results_embed.add_field(name=f"{map_name}", value=f"{len(voters)} Votes ({percentage:.1f}%)", inline=False)
        
        try:
            original_msg = await channel.fetch_message(int(message_id_str))
            view = VotingView.from_message(original_msg)
            for item in view.children:
                item.disabled = True
            await original_msg.edit(view=view)
            await original_msg.reply(embed=results_embed)
        except (discord.NotFound, discord.Forbidden) as e:
            log_map.warning(f"Could not find or edit original vote message {message_id_str}: {e}")

    @tasks.loop(seconds=15)
    async def vote_check_loop(self):
        now = datetime.now(timezone.utc)
        concluded_votes = []
        # Create a copy for safe iteration, as conclude_vote modifies the config
        config_copy = self.config.copy()
        for gid, g_cfg in config_copy.items():
            for msg_id, vote_data in g_cfg.get("active_votes", {}).items():
                end_time = datetime.fromisoformat(vote_data["end_time_iso"])
                if now >= end_time:
                    concluded_votes.append((gid, msg_id))
        
        for gid, msg_id in concluded_votes:
            log_map.info(f"Auto-concluding timed vote {msg_id}")
            await self.conclude_vote(gid, msg_id)
            
    @vote_check_loop.before_loop
    async def before_vote_check_loop(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="mappick", description="Start a vote to pick a map for a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(duration="Vote duration in minutes (1-10). Defaults to 1 minute.")
    async def mappick(self, interaction: discord.Interaction, game: str, duration: app_commands.Range[int, 1, 10] = 1):
        gid = str(interaction.guild_id)
        async with self.config_lock:
            self._ensure_new_map_format(gid, game)
            game_data = self.config.get(gid, {}).get("games", {}).get(game)
            if not game_data:
                 embed = discord.Embed(description=f"❌ The game '{game}' is not configured.", color=discord.Color.red())
                 return await interaction.response.send_message(embed=embed, ephemeral=True)
            
            unseen_maps = game_data.get("unseen_maps", [])
            seen_maps = game_data.get("seen_maps", [])
            all_maps_count = len(unseen_maps) + len(seen_maps)

            if all_maps_count < 3:
                embed = discord.Embed(description=f"❌ The game '{game}' needs at least 3 maps in its pool to start a vote.", color=discord.Color.red())
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            chosen_maps = []
            if len(unseen_maps) < 3:
                log_map.info(f"Pool for '{game}' in guild {gid} is low. Using remaining maps and resetting.")
                chosen_maps.extend(unseen_maps)
                num_needed = 3 - len(chosen_maps)
                potential_picks = seen_maps
                if len(potential_picks) < num_needed:
                     embed = discord.Embed(description=f"❌ Error: Not enough maps in the total pool to conduct a vote for '{game}'.", color=discord.Color.red())
                     return await interaction.response.send_message(embed=embed, ephemeral=True)
                chosen_maps.extend(random.sample(potential_picks, num_needed))
                all_maps = unseen_maps + seen_maps
                game_data["seen_maps"] = chosen_maps
                game_data["unseen_maps"] = [m for m in all_maps if m not in chosen_maps]
            else:
                chosen_maps = random.sample(unseen_maps, 3)
                for m in chosen_maps:
                    unseen_maps.remove(m)
                    seen_maps.append(m)
            
            save_config_map(self.config)

        end_time = datetime.now(timezone.utc) + timedelta(minutes=duration)
        view = VotingView()
        for i, map_name in enumerate(chosen_maps):
            view.children[i].label = f"{map_name} (0 Votes)"
        
        embed = discord.Embed(
            title=f"🗺️ {game} Map Vote",
            color=EMBED_COLOR_MAP,
            description="Vote for the map you want to play! The winner is chosen randomly, with more votes increasing a map's chance of being selected.",
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url="https://i.imgur.com/g4518J5.png") # Map icon
        embed.add_field(name="Timer", value=f"This vote will automatically conclude <t:{int(end_time.timestamp())}:R>.")
        embed.set_footer(text=self.get_footer_text())
        
        await interaction.response.send_message(embed=embed, view=view)
        message = await interaction.original_response()

        async with self.config_lock:
            current_config = load_config_map()
            current_config.setdefault(gid, {}).setdefault("active_votes", {})
            current_config[gid]["active_votes"][str(message.id)] = {
                "channel_id": message.channel.id,
                "end_time_iso": end_time.isoformat(),
                "maps": chosen_maps,
                "votes": {map_name: [] for map_name in chosen_maps}
            }
            save_config_map(current_config)
            self.config = current_config

    mapadmin = app_commands.Group(name="mapadmin", description="Commands to manage games and maps.", default_permissions=discord.Permissions(manage_guild=True))

    @mapadmin.command(name="addgame", description="Add a new game.")
    async def addgame(self, interaction: discord.Interaction, name: str):
        gid = str(interaction.guild_id)
        async with self.config_lock:
            self.config.setdefault(gid, {}).setdefault("games", {})
            if name in self.config[gid]["games"]:
                embed = discord.Embed(description=f"❌ The game '{name}' already exists.", color=discord.Color.yellow())
                return await interaction.response.send_message(embed=embed, ephemeral=True)
            
            self.config[gid]["games"][name] = {"unseen_maps": [], "seen_maps": []}
            save_config_map(self.config)
        embed = discord.Embed(description=f"✅ Game '{name}' has been added.", color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @mapadmin.command(name="removegame", description="Remove a game and all its maps.")
    @app_commands.autocomplete(game=game_autocomplete)
    async def removegame(self, interaction: discord.Interaction, game: str):
        gid = str(interaction.guild_id)
        async with self.config_lock:
            if game not in self.config.get(gid, {}).get("games", {}):
                embed = discord.Embed(description=f"❌ The game '{game}' does not exist.", color=discord.Color.red())
                return await interaction.response.send_message(embed=embed, ephemeral=True)
                
            del self.config[gid]["games"][game]
            save_config_map(self.config)
        embed = discord.Embed(description=f"✅ Game '{game}' and all its maps have been removed.", color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="addmap", description="Add a map to a game's pool.")
    @app_commands.autocomplete(game=game_autocomplete)
    async def addmap(self, interaction: discord.Interaction, game: str, map_name: str):
        gid = str(interaction.guild_id)
        async with self.config_lock:
            self._ensure_new_map_format(gid, game)
            game_data = self.config.get(gid, {}).get("games", {}).get(game)
            if not isinstance(game_data, dict):
                embed = discord.Embed(description=f"❌ The game '{game}' does not exist or is in an invalid format.", color=discord.Color.red())
                return await interaction.response.send_message(embed=embed, ephemeral=True)
            
            game_data["unseen_maps"].append(map_name)
            save_config_map(self.config)
        embed = discord.Embed(description=f"✅ Added '{map_name}' to the map pool for '{game}'.", color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="removemap", description="Remove a map from a game's pool.")
    @app_commands.autocomplete(game=game_autocomplete, map_name=map_autocomplete)
    async def removemap(self, interaction: discord.Interaction, game: str, map_name: str):
        gid = str(interaction.guild_id)
        async with self.config_lock:
            self._ensure_new_map_format(gid, game)
            game_data = self.config.get(gid, {}).get("games", {}).get(game)

            if map_name in game_data.get("unseen_maps", []):
                game_data["unseen_maps"].remove(map_name)
            elif map_name in game_data.get("seen_maps", []):
                game_data["seen_maps"].remove(map_name)
            else:
                embed = discord.Embed(description=f"❌ The map '{map_name}' was not found in the pool for '{game}'.", color=discord.Color.red())
                return await interaction.response.send_message(embed=embed, ephemeral=True)
                
            save_config_map(self.config)
        embed = discord.Embed(description=f"✅ Removed '{map_name}' from the map pool for '{game}'.", color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="listmaps", description="List all maps for a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    async def listmaps(self, interaction: discord.Interaction, game: str):
        gid = str(interaction.guild_id)
        self._ensure_new_map_format(gid, game)
        game_data = self.config.get(gid, {}).get("games", {}).get(game)
        unseen_maps = game_data.get("unseen_maps", [])
        seen_maps = game_data.get("seen_maps", [])
        all_maps = unseen_maps + seen_maps
        if not all_maps:
            embed = discord.Embed(description=f"The game '{game}' has no maps configured.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        description = "\n".join(f"• {m}" for m in sorted(all_maps))
        embed = discord.Embed(title=f"🗺️ Map Pool for {game}", color=EMBED_COLOR_MAP, description=description)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
async def setup(bot: commands.Bot):
    await bot.add_cog(MapVoter(bot))