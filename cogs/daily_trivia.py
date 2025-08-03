import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import aiohttp
import asyncio
from datetime import datetime, time, timedelta, timezone
import pytz
import html
import random
import logging

log_trivia = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE_TRIVIA = "trivia_config.json"
TRIVIA_API_URL = "https://opentdb.com/api.php?amount=10&type=multiple"
EMBED_COLOR_TRIVIA = 0x1ABC9C 
CACHE_MIN_SIZE = 5
CACHE_TARGET_SIZE = 10

def load_config_trivia():
    if os.path.exists(CONFIG_FILE_TRIVIA):
        try:
            with open(CONFIG_FILE_TRIVIA, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_trivia(config):
    with open(CONFIG_FILE_TRIVIA, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

# --- UI Components ---
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

# --- Main Cog ---
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
            # Use followup if the interaction was already deferred
            if interaction.response.is_done():
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            log_trivia.error(f"An unhandled error occurred in a command: {error}")

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {
                "channel_id": None, "time": "12:00", "timezone": "UTC", "enabled": False,
                "pending_answers": [], "monthly_scores": {}, "last_winner_announcement": "", "asked_questions": [],
                "question_cache": [], "reveal_delay": 60
            }
        self.config[gid].setdefault("monthly_scores", {})
        self.config[gid].setdefault("last_winner_announcement", "2000-01-01T00:00:00.000000+00:00")
        self.config[gid].setdefault("question_cache", [])
        self.config[gid].setdefault("reveal_delay", 60)
        return self.config[gid]

    async def save(self):
        async with self.config_lock:
            await self.bot.loop.run_in_executor(None, lambda: save_config_trivia(self.config))

    # ... (code for fetch_api_questions, handle_trivia_answer, reveal_trivia_answer, etc. remains the same) ...
    # ... The main change is in the slash command definitions below ...

    trivia = app_commands.Group(name="trivia", description="Commands for the daily trivia.", default_permissions=discord.Permissions(manage_guild=True))

    @trivia.command(name="settings", description="Show the current trivia settings.")
    async def trivia_settings(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True) # DEFER FIRST
        cfg = self.get_guild_config(interaction.guild_id)
        channel = self.bot.get_channel(cfg.get('channel_id'))
        embed = discord.Embed(title="⚙️ Trivia Settings", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        embed.add_field(name="Status", value="✅ Enabled" if cfg.get('enabled') else "❌ Disabled", inline=True)
        embed.add_field(name="Post Time", value=f"{cfg.get('time')} ({cfg.get('timezone')})", inline=True)
        embed.add_field(name="Post Channel", value=channel.mention if channel else "Not Set", inline=False)
        embed.add_field(name="Answer Reveal Delay", value=f"{cfg.get('reveal_delay')} minutes", inline=False)
        await interaction.followup.send(embed=embed)
        
    @trivia.command(name="toggle", description="Toggle daily trivia on or off.")
    @app_commands.describe(enabled="Set to True to enable, False to disable.")
    async def trivia_toggle(self, interaction: discord.Interaction, enabled: bool):
        await interaction.response.defer(ephemeral=True) # DEFER FIRST
        cfg = self.get_guild_config(interaction.guild_id)
        cfg["enabled"] = enabled
        await self.save()
        state_text = "Enabled" if enabled else "Disabled"
        embed = discord.Embed(description=f"✅ Daily trivia is now **{state_text}**.", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)

    @trivia.command(name="channel", description="Set the channel for daily trivia questions.")
    async def set_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True) # DEFER FIRST
        cfg = self.get_guild_config(interaction.guild_id)
        cfg["channel_id"] = channel.id
        await self.save()
        embed = discord.Embed(description=f"✅ Trivia will now post in {channel.mention}.", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)

    @trivia.command(name="time", description="Set the daily post time (24h format HH:MM).")
    async def set_time(self, interaction: discord.Interaction, time_str: str):
        await interaction.response.defer(ephemeral=True) # DEFER FIRST
        try:
            time.fromisoformat(time_str)
            cfg = self.get_guild_config(interaction.guild_id)
            cfg["time"] = time_str
            await self.save()
            embed = discord.Embed(description=f"✅ Trivia will now post daily at **{time_str}**.", color=EMBED_COLOR_TRIVIA)
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)
        except ValueError:
            embed = discord.Embed(description="❌ Invalid time format. Use **HH:MM** (24-hour).", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)

    @trivia.command(name="timezone", description="Set your server's timezone for accurate posting.")
    @app_commands.describe(timezone="E.g., America/New_York, Europe/London, etc.")
    async def set_timezone(self, interaction: discord.Interaction, timezone: str):
        await interaction.response.defer(ephemeral=True) # DEFER FIRST
        if timezone not in pytz.all_timezones_set:
            embed = discord.Embed(title="❌ Invalid Timezone", description="Please use a valid **TZ Database Name**.\nFind a list [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.followup.send(embed=embed)
        
        cfg = self.get_guild_config(interaction.guild_id)
        cfg["timezone"] = timezone
        await self.save()
        now_local = datetime.now(pytz.timezone(timezone))
        embed = discord.Embed(description=f"✅ Timezone set to **{timezone}**.\nMy current time for you is `{now_local.strftime('%H:%M:%S')}`.", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)
        
    @trivia.command(name="reveal_delay", description="Set the delay in minutes to reveal the answer.")
    @app_commands.describe(minutes="Delay in minutes (1-1440).")
    async def set_reveal(self, interaction: discord.Interaction, minutes: app_commands.Range[int, 1, 1440]):
        await interaction.response.defer(ephemeral=True) # DEFER FIRST
        cfg = self.get_guild_config(interaction.guild_id)
        cfg["reveal_delay"] = minutes
        await self.save()
        embed = discord.Embed(description=f"⏰ The answer will now be revealed **{minutes}** minutes after the question.", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)
        
    @trivia.command(name="postnow", description="Manually post a new trivia question right now.")
    async def trivia_postnow(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        cfg = self.get_guild_config(interaction.guild_id)
        
        if not cfg.get("enabled"):
            return await interaction.followup.send("Trivia is currently disabled. Enable it with `/trivia toggle`.")
        if not cfg.get("channel_id"):
            return await interaction.followup.send("The trivia channel has not been set. Set it with `/trivia channel`.")
        
        is_active = any(p['channel_id'] == cfg['channel_id'] for p in cfg.get('pending_answers', []))
        if is_active:
            return await interaction.followup.send("A trivia question is already active in the configured channel.")

        posted_message = await self.post_trivia_question(interaction.guild_id, cfg)
        if posted_message:
            await interaction.followup.send(f"✅ Trivia question posted successfully in {posted_message.channel.mention}!")
        else:
            await interaction.followup.send("❌ Failed to post a trivia question. Please check the console for errors.")

    @trivia.command(name="skip", description="Skips the current active trivia question.")
    async def trivia_skip(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        active_question = None
        async with self.config_lock:
            current_config = load_config_trivia()
            cfg = current_config.setdefault(str(interaction.guild_id), self.get_guild_config(interaction.guild_id))
            pending = cfg.get("pending_answers", [])
            active_question = next((q for q in pending if q.get('channel_id') == cfg.get('channel_id')), None)
            
            if not active_question:
                return await interaction.followup.send("There is no active trivia question to skip.")
            
            cfg["pending_answers"].remove(active_question)
            save_config_trivia(current_config)
            self.config = current_config
        
        try:
            channel = self.bot.get_channel(active_question['channel_id'])
            original_msg = await channel.fetch_message(active_question['message_id'])
            await original_msg.edit(view=None)
            skip_embed = discord.Embed(description=f"This trivia question has been skipped by an administrator.", color=discord.Color.orange())
            skip_embed.set_footer(text=self.get_footer_text())
            await original_msg.reply(embed=skip_embed)
        except (discord.NotFound, discord.Forbidden):
            log_trivia.warning(f"Could not find or edit original trivia message {active_question['message_id']} to skip it.")
        
        await interaction.followup.send("✅ The active trivia question has been skipped.")

async def setup(bot: commands.Bot):
    await bot.add_cog(DailyTrivia(bot))