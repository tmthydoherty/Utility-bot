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

log_map = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE_MAP = "map_voter_config.json"
EMBED_COLOR_MAP = 0xE91E63

# --- Helper Functions ---
def load_config_map():
    """Loads the configuration from the JSON file."""
    if os.path.exists(CONFIG_FILE_MAP):
        try:
            with open(CONFIG_FILE_MAP, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_config_map(config):
    """Saves the configuration to the JSON file."""
    with open(CONFIG_FILE_MAP, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

# --- UI Components ---
class VotingView(discord.ui.View):
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__(timeout=None)
        self.cog = cog_instance

    async def handle_button_press(self, interaction: discord.Interaction, map_index: int):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.cog.handle_vote(interaction, map_index)

    @discord.ui.button(label="Map 1", style=discord.ButtonStyle.secondary, emoji="1️⃣", custom_id="map_vote_1")
    async def map_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, 0)

    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.secondary, emoji="2️⃣", custom_id="map_vote_2")
    async def map_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, 1)

    @discord.ui.button(label="Map 3", style=discord.ButtonStyle.secondary, emoji="3️⃣", custom_id="map_vote_3")
    async def map_3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, 2)

class TournamentView(discord.ui.View):
    def __init__(self, map1: str, map2: str):
        super().__init__(timeout=None)
        self.votes = {} # user_id: map_name
        self.children[0].label = map1
        self.children[1].label = map2
        self.map1_name = map1
        self.map2_name = map2

    @discord.ui.button(label="Map 1", style=discord.ButtonStyle.primary)
    async def map1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.votes[interaction.user.id] = self.map1_name
        await interaction.response.send_message(f"You voted for **{self.map1_name}**.", ephemeral=True)

    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.primary)
    async def map2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.votes[interaction.user.id] = self.map2_name
        await interaction.response.send_message(f"You voted for **{self.map2_name}**.", ephemeral=True)

# --- Main Cog ---
class MapVoter(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_map()
        self.config_lock = asyncio.Lock()
        self.vote_check_loop.start()
        self.bot.add_view(VotingView(self))

    def cog_unload(self):
        self.vote_check_loop.cancel()
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Map Voter"

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        embed = discord.Embed(color=discord.Color.red())
        embed.set_footer(text=self.get_footer_text())
        if isinstance(error, app_commands.MissingPermissions):
            embed.description = "❌ You don't have the required permissions for this command."
        else:
            log_map.error(f"An unhandled error occurred in a command: {error}")
            embed.description = "An unexpected error occurred. Please try again later."
        
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

    def _ensure_new_map_format(self, guild_config: dict, game_name: str):
        games = guild_config.get("games", {})
        if game_name in games and isinstance(games[game_name], list):
            log_map.info(f"Migrating map pool for game '{game_name}' to new format.")
            games[game_name] = { "unseen_maps": games[game_name], "seen_maps": [], "win_history": {} }

    async def game_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        guild_cfg = self.config.get(str(interaction.guild_id), {})
        games = guild_cfg.get("games", {}).keys()
        return [app_commands.Choice(name=game, value=game) for game in games if current.lower() in game.lower()][:25]

    async def map_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        game = interaction.namespace.game
        if not game: return []
        gid = str(interaction.guild_id)
        guild_cfg = self.config.get(gid, {})
        self._ensure_new_map_format(guild_cfg, game)
        map_pool = guild_cfg.get("games", {}).get(game, {})
        all_maps = map_pool.get("unseen_maps", []) + map_pool.get("seen_maps", [])
        return [app_commands.Choice(name=map_name, value=map_name) for map_name in all_maps if current.lower() in map_name.lower()][:25]

    async def handle_vote(self, interaction: discord.Interaction, map_index: int):
        message_to_send = ""
        async with self.config_lock:
            current_config = load_config_map()
            gid_str = str(interaction.guild_id)
            active_votes = current_config.setdefault(gid_str, {}).setdefault("active_votes", {})
            vote_data = active_votes.get(str(interaction.message.id))
            if not vote_data:
                message_to_send = "This vote seems to have expired or been removed."
            else:
                end_time = datetime.fromisoformat(vote_data["end_time_iso"])
                if datetime.now(timezone.utc) > end_time:
                    message_to_send = "❌ This vote has already ended."
                else:
                    map_name = vote_data["maps"][map_index]
                    votes = vote_data["votes"]
                    for map_votes in votes.values():
                        if interaction.user.id in map_votes:
                            map_votes.remove(interaction.user.id)
                    votes[map_name].append(interaction.user.id)
                    save_config_map(current_config)
                    self.config = current_config
                    message_to_send = f"✅ Your vote for **{map_name}** has been recorded."
        await interaction.followup.send(message_to_send, ephemeral=True)
        if "Your vote for" in message_to_send:
            fresh_config = load_config_map()
            fresh_vote_data = fresh_config.get(str(interaction.guild_id), {}).get("active_votes", {}).get(str(interaction.message.id))
            if fresh_vote_data:
                view = VotingView.from_message(interaction.message)
                for i, child in enumerate(view.children):
                    if isinstance(child, discord.ui.Button):
                        map_name_for_button = fresh_vote_data["maps"][i]
                        vote_count = len(fresh_vote_data['votes'][map_name_for_button])
                        child.label = f"{map_name_for_button} ({vote_count} Votes)"
                try:
                    await interaction.message.edit(view=view)
                except discord.HTTPException:
                    log_map.warning(f"Failed to edit message {interaction.message.id} after a vote.")

    async def conclude_vote(self, guild_id_str: str, message_id_str: str, ended_by: discord.User = None):
        vote_data = None
        winner = None
        async with self.config_lock:
            current_config = load_config_map()
            vote_data = current_config.setdefault(guild_id_str, {}).setdefault("active_votes", {}).pop(message_id_str, None)
            if not vote_data: return
            total_votes = sum(len(v) for v in vote_data["votes"].values())
            winner = random.choice(vote_data["maps"])
            if total_votes > 0:
                weighted_pool = [m for m, v in vote_data["votes"].items() for _ in v]
                if weighted_pool: winner = random.choice(weighted_pool)
            game_name = vote_data.get("game")
            if game_name:
                game_stats = current_config[guild_id_str]["games"].setdefault(game_name, {})
                win_history = game_stats.setdefault("win_history", {})
                win_history[winner] = win_history.get(winner, 0) + 1
            user_stats = current_config[guild_id_str].setdefault("user_stats", {})
            for map_name, user_ids in vote_data["votes"].items():
                for user_id in user_ids:
                    user_id_str = str(user_id)
                    player_data = user_stats.setdefault(user_id_str, {"total_votes": 0, "wins": 0, "map_votes": {}})
                    player_data["total_votes"] += 1
                    player_data["map_votes"][map_name] = player_data["map_votes"].get(map_name, 0) + 1
                    if map_name == winner:
                        player_data["wins"] += 1
            save_config_map(current_config)
            self.config = current_config
        channel = self.bot.get_channel(vote_data["channel_id"])
        if not channel: return
        description = f"The chosen map is **{winner}**!"
        if ended_by:
            description += f"\n\n*This vote was ended early by {ended_by.mention}.*"
        results_embed = discord.Embed(title="🗳️ Map Vote Concluded!", color=discord.Color.gold(), description=description)
        results_embed.set_footer(text=self.get_footer_text())
        total_votes = sum(len(v) for v in vote_data["votes"].values())
        sorted_votes = sorted(vote_data["votes"].items(), key=lambda item: len(item[1]), reverse=True)
        for map_name_item, voters in sorted_votes:
            percentage = (len(voters) / total_votes * 100) if total_votes > 0 else 0
            results_embed.add_field(name=f"{map_name_item}", value=f"{len(voters)} Votes ({percentage:.1f}%)", inline=False)
        try:
            original_msg = await channel.fetch_message(int(message_id_str))
            view = VotingView.from_message(original_msg)
            for item in view.children: item.disabled = True
            await original_msg.edit(view=view)
            await original_msg.reply(embed=results_embed)
        except (discord.NotFound, discord.Forbidden) as e:
            log_map.warning(f"Could not find or edit original vote message {message_id_str}: {e}")

    async def cancel_vote(self, guild_id_str: str, message_id_str: str, vote_data: dict):
        async with self.config_lock:
            current_config = load_config_map()
            current_config.setdefault(guild_id_str, {}).setdefault("active_votes", {}).pop(message_id_str, None)
            game_name = vote_data.get("game")
            maps_to_return = vote_data.get("maps", [])
            if game_name and maps_to_return:
                game_data = current_config.get(guild_id_str, {}).get("games", {}).get(game_name)
                if game_data:
                    game_data["seen_maps"] = [m for m in game_data.get("seen_maps", []) if m not in maps_to_return]
                    game_data["unseen_maps"].extend(maps_to_return)
                    game_data["unseen_maps"] = list(set(game_data["unseen_maps"]))
            save_config_map(current_config)
            self.config = current_config
        channel = self.bot.get_channel(vote_data["channel_id"])
        if not channel: return
        voter_ids = {user_id for user_list in vote_data["votes"].values() for user_id in user_list}
        actual_voters = len(voter_ids)
        required_voters = vote_data.get("min_users", 1)
        description = f"This vote did not meet the minimum requirement of **{required_voters}** voters.\nOnly **{actual_voters}** people voted. The maps have been returned to the pool."
        cancel_embed = discord.Embed(title="🚫 Map Vote Cancelled", color=discord.Color.red(), description=description)
        cancel_embed.set_footer(text=self.get_footer_text())
        try:
            original_msg = await channel.fetch_message(int(message_id_str))
            view = VotingView.from_message(original_msg)
            for item in view.children: item.disabled = True
            await original_msg.edit(view=view)
            await original_msg.reply(embed=cancel_embed)
        except (discord.NotFound, discord.Forbidden) as e:
            log_map.warning(f"Could not find or edit original cancelled vote message {message_id_str}: {e}")

    @tasks.loop(seconds=15)
    async def vote_check_loop(self):
        now = datetime.now(timezone.utc)
        votes_to_conclude, votes_to_cancel = [], []
        config_copy = json.loads(json.dumps(self.config))
        for gid, g_cfg in config_copy.items():
            for msg_id, vote_data in g_cfg.get("active_votes", {}).items():
                end_time = datetime.fromisoformat(vote_data["end_time_iso"])
                if now >= end_time:
                    min_users = vote_data.get("min_users", 1)
                    voter_ids = {user_id for user_list in vote_data["votes"].values() for user_id in user_list}
                    if len(voter_ids) >= min_users:
                        votes_to_conclude.append((gid, msg_id))
                    else:
                        votes_to_cancel.append((gid, msg_id, vote_data))
        for gid, msg_id in votes_to_conclude:
            log_map.info(f"Auto-concluding timed vote {msg_id} - Met user requirement")
            await self.conclude_vote(gid, msg_id)
        for gid, msg_id, vote_data in votes_to_cancel:
            log_map.info(f"Auto-cancelling timed vote {msg_id} - Did not meet user requirement")
            await self.cancel_vote(gid, msg_id, vote_data)
            
    @vote_check_loop.before_loop
    async def before_vote_check_loop(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="mappick", description="Start a vote to pick a map for a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(game="The game to vote on.", duration="Vote duration in minutes (1-10). Defaults to 2 minutes.", min_users="The minimum number of users required for the vote to pass. Defaults to 6.")
    async def mappick(self, interaction: discord.Interaction, game: str, duration: app_commands.Range[int, 1, 10] = 2, min_users: app_commands.Range[int, 1, 12] = 6):
        await interaction.response.defer()
        chosen_maps, vote_id = [], -1
        async with self.config_lock:
            current_config = load_config_map()
            gid = str(interaction.guild_id)
            guild_cfg = current_config.setdefault(gid, {})
            self._ensure_new_map_format(guild_cfg, game)
            game_data = guild_cfg.get("games", {}).get(game)
            vote_id = guild_cfg.get("vote_counter", 0) + 1
            guild_cfg["vote_counter"] = vote_id
            if not game_data:
                embed = discord.Embed(description=f"❌ The game '{game}' is not configured.", color=discord.Color.red())
                return await interaction.followup.send(embed=embed, ephemeral=True)
            unseen_maps, seen_maps = game_data.get("unseen_maps", []), game_data.get("seen_maps", [])
            if len(unseen_maps) + len(seen_maps) < 3:
                embed = discord.Embed(description=f"❌ '{game}' needs at least 3 maps to start a vote.", color=discord.Color.red())
                return await interaction.followup.send(embed=embed, ephemeral=True)
            if len(unseen_maps) < 3:
                log_map.info(f"Pool for '{game}' in guild {gid} is low. Resetting.")
                chosen_maps.extend(unseen_maps)
                needed = 3 - len(chosen_maps)
                if len(seen_maps) < needed:
                     embed = discord.Embed(description=f"❌ Error: Not enough maps in the total pool for '{game}'.", color=discord.Color.red())
                     return await interaction.followup.send(embed=embed, ephemeral=True)
                chosen_maps.extend(random.sample(seen_maps, needed))
                all_maps = unseen_maps + seen_maps
                game_data["seen_maps"] = chosen_maps
                game_data["unseen_maps"] = [m for m in all_maps if m not in chosen_maps]
            else:
                chosen_maps = random.sample(unseen_maps, 3)
                for m in chosen_maps:
                    unseen_maps.remove(m)
                    seen_maps.append(m)
            save_config_map(current_config)
            self.config = current_config
        end_time = datetime.now(timezone.utc) + timedelta(minutes=duration)
        view = VotingView(self)
        for i, map_name in enumerate(chosen_maps):
            if i < len(view.children): view.children[i].label = f"{map_name} (0 Votes)"
        description = (f"Vote for the map you want to play! The winner is chosen randomly, with more votes increasing a map's chance of being selected.\n\n"
                       f"🕒 This vote will automatically conclude <t:{int(end_time.timestamp())}:R>.\n"
                       f"👥 Requires at least **{min_users}** unique voters to be valid.")
        embed = discord.Embed(title=f"🗺️ {game} Map Vote", color=EMBED_COLOR_MAP, description=description)
        embed.set_footer(text=f"Vote ID: #{vote_id}  •  {self.get_footer_text()}")
        message = await interaction.followup.send(embed=embed, view=view, wait=True)
        async with self.config_lock:
            current_config = load_config_map()
            gid_str = str(interaction.guild_id)
            active_votes = current_config.setdefault(gid_str, {}).setdefault("active_votes", {})
            active_votes[str(message.id)] = {"channel_id": message.channel.id, "end_time_iso": end_time.isoformat(), "maps": chosen_maps, "votes": {map_name: [] for map_name in chosen_maps}, "game": game, "short_id": vote_id, "min_users": min_users}
            save_config_map(current_config)
            self.config = current_config

    @app_commands.command(name="mystats", description="Shows your personal voting statistics.")
    @app_commands.describe(user="The user whose stats you want to see (optional).")
    async def mystats(self, interaction: discord.Interaction, user: discord.Member = None):
        target_user = user or interaction.user
        gid = str(interaction.guild_id)
        user_stats = self.config.get(gid, {}).get("user_stats", {}).get(str(target_user.id))
        if not user_stats or not user_stats.get("total_votes"):
            embed = discord.Embed(description=f"{target_user.mention} has not voted in any map polls yet.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed)
        total_votes = user_stats.get("total_votes", 0)
        wins = user_stats.get("wins", 0)
        win_rate = (wins / total_votes * 100) if total_votes > 0 else 0
        embed = discord.Embed(title=f"📊 Voting Stats for {target_user.display_name}", color=EMBED_COLOR_MAP)
        embed.set_thumbnail(url=target_user.display_avatar.url)
        embed.add_field(name="Total Votes Cast", value=f"`{total_votes}`", inline=True)
        embed.add_field(name="Votes Won", value=f"`{wins}`", inline=True)
        embed.add_field(name="Win Rate", value=f"`{win_rate:.1f}%`", inline=True)
        map_votes = user_stats.get("map_votes", {})
        if map_votes:
            sorted_maps = sorted(map_votes.items(), key=lambda item: item[1], reverse=True)
            top_maps_str = "\n".join([f"• **{m}**: {v} times" for m, v in sorted_maps[:5]])
            embed.add_field(name="Most Voted For Maps", value=top_maps_str, inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="mapstats", description="Shows the pick rate and win count for all maps in a game.")
    @app_commands.autocomplete(game=game_autocomplete)
    async def mapstats(self, interaction: discord.Interaction, game: str):
        gid = str(interaction.guild_id)
        game_data = self.config.get(gid, {}).get("games", {}).get(game, {})
        if not game_data:
            embed = discord.Embed(description=f"The game **{game}** has not been configured.", color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        all_maps = sorted(game_data.get("unseen_maps", []) + game_data.get("seen_maps", []))
        win_history = game_data.get("win_history", {})
        total_wins = sum(win_history.values())

        if not all_maps:
            embed = discord.Embed(description=f"There are no maps in the pool for **{game}**.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        stats_lines = []
        for map_name in all_maps:
            wins = win_history.get(map_name, 0)
            pick_rate = (wins / total_wins * 100) if total_wins > 0 else 0
            stats_lines.append(f"• **{map_name}**: {wins} wins ({pick_rate:.1f}%)")
        
        description = "\n".join(stats_lines)
        
        # Handle potential for very long descriptions
        if len(description) > 4000:
            description = description[:4000] + "\n..."
            
        embed = discord.Embed(title=f"🏆 Map Stats for {game}", color=EMBED_COLOR_MAP, description=description)
        embed.set_footer(text=f"Based on {total_wins} total concluded votes.")
        await interaction.response.send_message(embed=embed)

    mapadmin = app_commands.Group(name="mapadmin", description="Commands to manage games and maps.", default_permissions=discord.Permissions(manage_guild=True))
    
    @mapadmin.command(name="tournament", description="Start an 8-map tournament bracket.")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(game="The game to host the tournament for.", vote_duration="Duration for each 1v1 vote in seconds (10-120).")
    async def tournament(self, interaction: discord.Interaction, game: str, vote_duration: app_commands.Range[int, 10, 120] = 30):
        gid = str(interaction.guild_id)
        game_data = self.config.get(gid, {}).get("games", {}).get(game)
        if not game_data:
            return await interaction.response.send_message(f"❌ The game '{game}' is not configured.", ephemeral=True)
        all_maps = game_data.get("unseen_maps", []) + game_data.get("seen_maps", [])
        if len(all_maps) < 8:
            return await interaction.response.send_message(f"❌ The game '{game}' needs at least 8 maps for a tournament.", ephemeral=True)
        
        await interaction.response.send_message(f"🔥 Starting a map tournament for **{game}**! Getting the bracket ready...")
        
        maps = random.sample(all_maps, 8)
        qf_winners, sf_winners, champion = [], [], None
        
        bracket_embed = discord.Embed(title=f"🏆 {game} Map Tournament Bracket", color=EMBED_COLOR_MAP)
        bracket_embed.add_field(name="Quarter-Finals", value="\n".join([f"`{maps[i]}` vs `{maps[i+1]}`" for i in range(0, 8, 2)]), inline=False)
        bracket_embed.add_field(name="Semi-Finals", value="*TBD*", inline=False)
        bracket_embed.add_field(name="Finals", value="*TBD*", inline=False)
        bracket_msg = await interaction.followup.send(embed=bracket_embed)

        # Quarter-Finals
        for i in range(0, 8, 2):
            map1, map2 = maps[i], maps[i+1]
            winner = await self.run_matchup(interaction, f"Quarter-Final: {map1} vs {map2}", map1, map2, vote_duration)
            qf_winners.append(winner)
            await interaction.channel.send(f"**{winner}** wins the quarter-final match and advances!")
        
        bracket_embed.set_field_at(0, name="Quarter-Finals (Complete)", value="\n".join([f"~~`{maps[i]}` vs `{maps[i+1]}`~~ -> **{qf_winners[i//2]}**" for i in range(0, 8, 2)]), inline=False)
        bracket_embed.set_field_at(1, name="Semi-Finals", value="\n".join([f"`{qf_winners[i]}` vs `{qf_winners[i+1]}`" for i in range(0, 4, 2)]), inline=False)
        await bracket_msg.edit(embed=bracket_embed)

        # Semi-Finals
        for i in range(0, 4, 2):
            map1, map2 = qf_winners[i], qf_winners[i+1]
            winner = await self.run_matchup(interaction, f"Semi-Final: {map1} vs {map2}", map1, map2, vote_duration)
            sf_winners.append(winner)
            await interaction.channel.send(f"**{winner}** wins the semi-final match and advances to the finals!")

        bracket_embed.set_field_at(1, name="Semi-Finals (Complete)", value="\n".join([f"~~`{qf_winners[i]}` vs `{qf_winners[i+1]}`~~ -> **{sf_winners[i//2]}**" for i in range(0, 4, 2)]), inline=False)
        bracket_embed.set_field_at(2, name="Finals", value=f"`{sf_winners[0]}` vs `{sf_winners[1]}`", inline=False)
        await bracket_msg.edit(embed=bracket_embed)

        # Finals
        champion = await self.run_matchup(interaction, f"🏆 FINAL ROUND: {sf_winners[0]} vs {sf_winners[1]}", sf_winners[0], sf_winners[1], vote_duration)
        await interaction.channel.send(f"👑 The tournament champion for **{game}** is **{champion}**!")
        
        bracket_embed.set_field_at(2, name="Finals (Complete)", value=f"~~`{sf_winners[0]}` vs `{sf_winners[1]}`~~ -> 🏆 **{champion}** 🏆", inline=False)
        await bracket_msg.edit(embed=bracket_embed)

    async def run_matchup(self, interaction: discord.Interaction, title: str, map1: str, map2: str, duration: int) -> str:
        view = TournamentView(map1, map2)
        end_time = datetime.now(timezone.utc) + timedelta(seconds=duration)
        embed = discord.Embed(title=title, color=discord.Color.gold(), description=f"Vote now! Poll ends <t:{int(end_time.timestamp())}:R>.")
        msg = await interaction.channel.send(embed=embed, view=view)
        await asyncio.sleep(duration)
        for item in view.children: item.disabled = True
        await msg.edit(view=view)
        
        vote_counts = Counter(view.votes.values())
        if not vote_counts or vote_counts[map1] == vote_counts[map2]:
            winner = random.choice([map1, map2])
            await interaction.channel.send(f"The vote was a tie! Randomly selected **{winner}** to advance.")
            return winner
        else:
            return vote_counts.most_common(1)[0][0]

    @mapadmin.command(name="addgame", description="Add a new game.")
    async def addgame(self, interaction: discord.Interaction, name: str):
        gid = str(interaction.guild_id)
        embed = None
        async with self.config_lock:
            current_config = load_config_map()
            games = current_config.setdefault(gid, {}).setdefault("games", {})
            if name in games:
                embed = discord.Embed(description=f"❌ The game '{name}' already exists.", color=discord.Color.yellow())
            else:
                games[name] = {"unseen_maps": [], "seen_maps": [], "win_history": {}}
                save_config_map(current_config)
                self.config = current_config
                embed = discord.Embed(description=f"✅ Game '{name}' has been added.", color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @mapadmin.command(name="removegame", description="Remove a game and all its maps.")
    @app_commands.autocomplete(game=game_autocomplete)
    async def removegame(self, interaction: discord.Interaction, game: str):
        gid = str(interaction.guild_id)
        embed = None
        async with self.config_lock:
            current_config = load_config_map()
            games = current_config.get(gid, {}).get("games", {})
            if game not in games:
                embed = discord.Embed(description=f"❌ The game '{game}' does not exist.", color=discord.Color.red())
            else:
                del games[game]
                save_config_map(current_config)
                self.config = current_config
                embed = discord.Embed(description=f"✅ Game '{game}' and all its maps have been removed.", color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="addmaps", description="Add one or more maps to a game's pool (comma-separated).")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(maps="A comma-separated list of map names to add.")
    async def addmaps(self, interaction: discord.Interaction, game: str, maps: str):
        gid = str(interaction.guild_id)
        added_maps, existing_maps = [], []
        map_names = [m.strip() for m in maps.split(',') if m.strip()]
        if not map_names:
            embed = discord.Embed(description="❌ You must provide at least one map name.", color=discord.Color.red())
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        async with self.config_lock:
            current_config = load_config_map()
            guild_cfg = current_config.setdefault(gid, {})
            self._ensure_new_map_format(guild_cfg, game)
            game_data = guild_cfg.get("games", {}).get(game)
            if not isinstance(game_data, dict):
                embed = discord.Embed(description=f"❌ The game '{game}' does not exist.", color=discord.Color.red())
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            current_pool = game_data.get("unseen_maps", []) + game_data.get("seen_maps", [])
            for map_name in map_names:
                if map_name not in current_pool:
                    game_data["unseen_maps"].append(map_name)
                    added_maps.append(map_name)
                else: existing_maps.append(map_name)
            if added_maps:
                save_config_map(current_config)
                self.config = current_config
        description = ""
        if added_maps: description += f"✅ Added **{len(added_maps)}** new map(s) to **{game}**: `{'`, `'.join(added_maps)}`\n"
        if existing_maps: description += f"⚠️ **{len(existing_maps)}** map(s) already existed: `{'`, `'.join(existing_maps)}`"
        embed = discord.Embed(description=description.strip(), color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="removemaps", description="Remove one or more maps from a game's pool (comma-separated).")
    @app_commands.autocomplete(game=game_autocomplete)
    @app_commands.describe(maps="A comma-separated list of map names to remove.")
    async def removemaps(self, interaction: discord.Interaction, game: str, maps: str):
        gid = str(interaction.guild_id)
        removed_maps, not_found_maps = [], []
        map_names = [m.strip() for m in maps.split(',') if m.strip()]
        if not map_names:
            embed = discord.Embed(description="❌ You must provide at least one map name.", color=discord.Color.red())
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        async with self.config_lock:
            current_config = load_config_map()
            guild_cfg = current_config.setdefault(gid, {})
            self._ensure_new_map_format(guild_cfg, game)
            game_data = guild_cfg.get("games", {}).get(game)
            if not isinstance(game_data, dict):
                embed = discord.Embed(description=f"❌ The game '{game}' does not exist.", color=discord.Color.red())
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
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
                save_config_map(current_config)
                self.config = current_config
        description = ""
        if removed_maps: description += f"✅ Removed **{len(removed_maps)}** map(s) from **{game}**: `{'`, `'.join(removed_maps)}`\n"
        if not_found_maps: description += f"⚠️ **{len(not_found_maps)}** map(s) were not found: `{'`, `'.join(not_found_maps)}`"
        embed = discord.Embed(description=description.strip(), color=EMBED_COLOR_MAP)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="listvotes", description="Lists all active map votes in this server.")
    async def listvotes(self, interaction: discord.Interaction):
        gid = str(interaction.guild_id)
        active_votes = self.config.get(gid, {}).get("active_votes", {})
        if not active_votes:
            embed = discord.Embed(description="There are no active map votes right now.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        embed = discord.Embed(title="Active Map Votes", color=EMBED_COLOR_MAP)
        for msg_id, vote_data in active_votes.items():
            end_time = datetime.fromisoformat(vote_data["end_time_iso"])
            game = vote_data.get("game", "N/A")
            vote_id = vote_data.get("short_id", "N/A")
            field_value = (f"**Game**: {game}\n**Maps**: {', '.join(vote_data['maps'])}\n**Ends**: <t:{int(end_time.timestamp())}:R>")
            embed.add_field(name=f"Vote ID: #{vote_id}", value=field_value, inline=False)
        embed.set_footer(text=f"Use /mapadmin endvote to conclude a vote early.")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mapadmin.command(name="endvote", description="Manually ends an active map vote.")
    @app_commands.describe(vote_id="The ID of the vote to end (from /mapadmin listvotes).")
    async def endvote(self, interaction: discord.Interaction, vote_id: int):
        gid = str(interaction.guild_id)
        active_votes = self.config.get(gid, {}).get("active_votes", {})
        message_id_to_end = None
        for msg_id, vote_data in active_votes.items():
            if vote_data.get("short_id") == vote_id:
                message_id_to_end = msg_id
                break
        if message_id_to_end:
            await interaction.response.send_message(f"✅ Ending vote #{vote_id}...", ephemeral=True)
            await self.conclude_vote(gid, message_id_to_end, ended_by=interaction.user)
        else:
            await interaction.response.send_message(f"❌ Could not find an active vote with ID #{vote_id}.", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(MapVoter(bot))

