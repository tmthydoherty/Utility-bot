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
    def __init__(self, cog_instance: 'MapVoter'):
        super().__init__(timeout=None)
        self.cog = cog_instance

    async def handle_button_press(self, interaction: discord.Interaction, map_index: int):
        # Acknowledge the interaction immediately before any logic
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

    # ... (rest of the MapVoter cog is unchanged, only handle_vote is modified below) ...

    async def handle_vote(self, interaction: discord.Interaction, map_index: int):
        message_to_send = ""
        vote_data = None # To use for UI updates outside the lock
        
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

        # Update the buttons on the original message if the vote was successful
        if "Your vote for" in message_to_send and vote_data:
            view = VotingView.from_message(interaction.message)
            for i, child in enumerate(view.children):
                if isinstance(child, discord.ui.Button):
                    map_name_for_button = vote_data["maps"][i]
                    child.label = f"{map_name_for_button} ({len(vote_data['votes'][map_name_for_button])} Votes)"
            await interaction.message.edit(view=view)

async def setup(bot: commands.Bot):
    await bot.add_cog(MapVoter(bot))