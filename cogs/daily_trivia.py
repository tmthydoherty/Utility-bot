import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import aiohttp
import asyncio
from datetime import datetime, time, timedelta
import pytz
import html
import random
import logging

log_trivia = logging.getLogger(__name__)

CONFIG_FILE_TRIVIA = "trivia_config.json"
TRIVIA_API_URL = "https://opentdb.com/api.php?amount=10&type=multiple"
EMBED_COLOR_TRIVIA = 0x1ABC9C 

def load_config_trivia():
    if os.path.exists(CONFIG_FILE_TRIVIA):
        try:
            with open(CONFIG_FILE_TRIVIA, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_trivia(config):
    with open(CONFIG_FILE_TRIVIA, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

class TriviaView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia'):
        super().__init__(timeout=None)
        self.cog = cog_instance
    @discord.ui.button(label="Answer A", style=discord.ButtonStyle.secondary, custom_id="trivia_a")
    async def answer_a(self, interaction: discord.Interaction, button: discord.ui.Button): await self.cog.handle_trivia_answer(interaction, button)
    @discord.ui.button(label="Answer B", style=discord.ButtonStyle.secondary, custom_id="trivia_b")
    async def answer_b(self, interaction: discord.Interaction, button: discord.ui.Button): await self.cog.handle_trivia_answer(interaction, button)
    @discord.ui.button(label="Answer C", style=discord.ButtonStyle.secondary, custom_id="trivia_c")
    async def answer_c(self, interaction: discord.Interaction, button: discord.ui.Button): await self.cog.handle_trivia_answer(interaction, button)
    @discord.ui.button(label="Answer D", style=discord.ButtonStyle.secondary, custom_id="trivia_d")
    async def answer_d(self, interaction: discord.Interaction, button: discord.ui.Button): await self.cog.handle_trivia_answer(interaction, button)

class DailyTrivia(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_trivia()
        self.session = aiohttp.ClientSession()
        self.config_lock = asyncio.Lock()
        self.trivia_loop.start()
        self.monthly_winner_loop.start()
        self.cache_refill_loop.start()
        self.bot.add_view(TriviaView(self))

    def cog_unload(self):
        self.trivia_loop.cancel()
        self.monthly_winner_loop.cancel()
        self.cache_refill_loop.cancel()
        asyncio.create_task(self.session.close())
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Daily Trivia"

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            embed = discord.Embed(description="❌ You don't have the required permissions for this command.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            log_trivia.error(f"An unhandled error occurred in a command: {error}")

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = { "channel_id": None, "time": "12:00", "timezone": "UTC", "enabled": False, "pending_answers": [], "monthly_scores": {}, "last_winner_announcement": ""}
        self.config[gid].setdefault("monthly_scores", {})
        self.config[gid].setdefault("last_winner_announcement", "")
        return self.config[gid]

    async def save(self):
        async with self.config_lock:
            await self.bot.loop.run_in_executor(None, lambda: save_config_trivia(self.config))

    async def handle_trivia_answer(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_correct = False
        async with self.config_lock:
            current_config = load_config_trivia()
            gid_str = str(interaction.guild_id)
            pending_answers = current_config.get(gid_str, {}).get("pending_answers", [])
            target_question = next((q for q in pending_answers if q.get("message_id") == interaction.message.id), None)

            if not target_question:
                return await interaction.response.send_message("This trivia question has expired.", ephemeral=True)
            
            user_id_str = str(interaction.user.id)
            if user_id_str in target_question.get("all_answers", {}):
                return await interaction.response.send_message("You have already answered this question!", ephemeral=True)
            
            target_question.setdefault("all_answers", {})[user_id_str] = button.label
            is_correct = (button.label == target_question["answer"])
            
            if is_correct:
                target_question.setdefault("winners", []).append(interaction.user.id)
                await interaction.response.send_message("✅ Correct!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Sorry, that's incorrect.", ephemeral=True)
            
            save_config_trivia(current_config)
            self.config = current_config

        if is_correct:
            view = TriviaView.from_message(interaction.message)
            clicked_button = discord.utils.get(view.children, custom_id=button.custom_id)
            if clicked_button:
                clicked_button.style = discord.ButtonStyle.success
                clicked_button.disabled = True
            await interaction.message.edit(view=view)

    # ... (The rest of the cog's methods like reveal_trivia_answer, post_trivia_question, loops, and commands go here)