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
        cog = interaction.client.get_cog("MapVoter"); await cog.handle_vote(interaction, 0)
    @discord.ui.button(label="Map 2", style=discord.ButtonStyle.secondary, emoji="2️⃣", custom_id="map_vote_2")
    async def map_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = interaction.client.get_cog("MapVoter"); await cog.handle_vote(interaction, 1)
    @discord.ui.button(label="Map 3", style=discord.ButtonStyle.secondary, emoji="3️⃣", custom_id="map_vote_3")
    async def map_3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = interaction.client.get_cog("MapVoter"); await cog.handle_vote(interaction, 2)

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
        
    # ... (The rest of the cog's methods like _ensure_new_map_format, handle_vote, conclude_vote, loops, and commands go here)