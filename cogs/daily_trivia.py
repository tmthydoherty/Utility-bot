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

    async def handle_button_press(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.cog.handle_trivia_answer(interaction, button)

    @discord.ui.button(label="Answer A", style=discord.ButtonStyle.secondary, custom_id="trivia_a")
    async def answer_a(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, button)

    @discord.ui.button(label="Answer B", style=discord.ButtonStyle.secondary, custom_id="trivia_b")
    async def answer_b(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, button)

    @discord.ui.button(label="Answer C", style=discord.ButtonStyle.secondary, custom_id="trivia_c")
    async def answer_c(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, button)

    @discord.ui.button(label="Answer D", style=discord.ButtonStyle.secondary, custom_id="trivia_d")
    async def answer_d(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_button_press(interaction, button)

# --- Main Cog ---
class DailyTrivia(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_trivia()
        self.session = aiohttp.ClientSession()
        self.config_lock = asyncio.Lock()
        self.config_is_dirty = False

        self.trivia_loop.start()
        self.monthly_winner_loop.start()
        self.cache_refill_loop.start()
        self.save_loop.start()
        self.bot.add_view(TriviaView(self))

    def cog_unload(self):
        if self.config_is_dirty:
            log_trivia.info("Performing final trivia config save on cog unload.")
            save_config_trivia(self.config)
        self.trivia_loop.cancel()
        self.monthly_winner_loop.cancel()
        self.cache_refill_loop.cancel()
        self.save_loop.cancel()
        asyncio.create_task(self.session.close())
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Daily Trivia"

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        embed = discord.Embed(color=discord.Color.red())
        embed.set_footer(text=self.get_footer_text())

        if isinstance(error, app_commands.MissingPermissions):
            embed.description = "❌ You don't have the required permissions for this command."
        else:
            log_trivia.error(f"An unhandled error occurred in a command: {error}")
            embed.description = "An unexpected error occurred. Please try again later."
        
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

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

    @tasks.loop(seconds=30)
    async def save_loop(self):
        async with self.config_lock:
            if self.config_is_dirty:
                await self.bot.loop.run_in_executor(None, lambda: save_config_trivia(self.config))
                self.config_is_dirty = False
                log_trivia.info("Trivia config changes saved to disk.")

    async def fetch_api_questions(self):
        try:
            async with self.session.get(TRIVIA_API_URL) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("response_code") == 0:
                        return data.get("results", [])
        except Exception as e:
            log_trivia.error(f"Failed to fetch trivia from API: {e}")
        return []

    async def handle_trivia_answer(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_correct = False
        message_to_send = ""
        
        async with self.config_lock:
            gid_str = str(interaction.guild_id)
            pending_answers = self.config.get(gid_str, {}).get("pending_answers", [])
            target_question = next((q for q in pending_answers if q.get("message_id") == interaction.message.id), None)

            if not target_question:
                message_to_send = "This trivia question has expired."
            else:
                user_id_str = str(interaction.user.id)
                if user_id_str in target_question.get("all_answers", {}):
                    message_to_send = "You have already answered this question!"
                else:
                    target_question.setdefault("all_answers", {})[user_id_str] = button.label
                    is_correct = (button.label == target_question["answer"])
                    
                    if is_correct:
                        target_question.setdefault("winners", []).append(interaction.user.id)
                        message_to_send = "✅ Correct!"
                    else:
                        message_to_send = "❌ Sorry, that's incorrect."
                    
                    self.config_is_dirty = True

        await interaction.followup.send(message_to_send, ephemeral=True)

        if is_correct:
            view = TriviaView.from_message(interaction.message)
            clicked_button = discord.utils.get(view.children, custom_id=button.custom_id)
            if clicked_button:
                clicked_button.style = discord.ButtonStyle.success
                clicked_button.disabled = True
            await interaction.message.edit(view=view)

    async def reveal_trivia_answer(self, answer_data: dict):
        channel = self.bot.get_channel(answer_data["channel_id"])
        if not channel: return
        try:
            original_msg = await channel.fetch_message(answer_data["message_id"])
            await original_msg.edit(view=None)
        except (discord.NotFound, discord.Forbidden):
            original_msg = None
        
        winner_ids = answer_data.get("winners", [])
        all_answers_dict = answer_data.get("all_answers", {})
        total_players = len(all_answers_dict)

        async with self.config_lock:
            cfg = self.get_guild_config(channel.guild.id)
            for winner_id in winner_ids:
                cfg["monthly_scores"][str(winner_id)] = cfg["monthly_scores"].get(str(winner_id), 0) + 1
            self.config_is_dirty = True

        results_embed = discord.Embed(title="🏆 Trivia Results", description=f"**Question:** {answer_data['question']}", color=discord.Color.gold())
        results_embed.add_field(name="Correct Answer", value=f"**`{answer_data['answer']}`**", inline=False)
        if not winner_ids:
            results_embed.add_field(name="🎉 Winners", value="No one got the correct answer this time!", inline=False)
        else:
            first_winner_id = winner_ids.pop(0)
            results_embed.add_field(name="🥇 First Correct Answer", value=f"<@{first_winner_id}>", inline=False)
            if winner_ids:
                mentions = [f"<@{uid}>" for uid in winner_ids]
                results_embed.add_field(name="Other Winners", value=", ".join(mentions), inline=False)
        
        if total_players > 0:
            winners_count = len(answer_data.get("winners", []))
            percent_correct = (winners_count / total_players) * 100
            stats_text = f"{total_players} player(s) participated.\n{percent_correct:.1f}% answered correctly."
            results_embed.add_field(name="📊 Statistics", value=stats_text, inline=False)
        
        results_embed.set_footer(text=self.get_footer_text()).timestamp = datetime.now(timezone.utc)
        if original_msg: await original_msg.reply(embed=results_embed)
        else: await channel.send(embed=results_embed)

    async def post_trivia_question(self, guild_id: int, cfg: dict):
        channel = self.bot.get_channel(cfg["channel_id"])
        if not channel: return
        question_data = None
        
        async with self.config_lock:
            if cfg.get("question_cache"):
                question_data = cfg["question_cache"].pop(0)
                self.config_is_dirty = True
        
        if not question_data:
            log_trivia.warning(f"Trivia cache for guild {guild_id} is empty. Fetching live question.")
            results = await self.fetch_api_questions()
            if results: question_data = results.pop(0)
        
        if not question_data:
            log_trivia.error(f"Could not retrieve any trivia question for guild {guild_id}.")
            return
        
        question_data["question"] = html.unescape(question_data["question"])
        question_data["correct_answer"] = html.unescape(question_data["correct_answer"])
        question_data["incorrect_answers"] = [html.unescape(ans) for ans in question_data["incorrect_answers"]]
        all_answers = question_data["incorrect_answers"] + [question_data["correct_answer"]]
        random.shuffle(all_answers)

        embed = discord.Embed(title="❓ Daily Trivia Question!", description=f"**{question_data['question']}**", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=f"{self.get_footer_text()} | Category: {html.unescape(question_data['category'])}")
        view = TriviaView(self)
        for i, answer_text in enumerate(all_answers):
            if i < len(view.children):
                view.children[i].label = answer_text

        try:
            msg = await channel.send(embed=embed, view=view)
            async with self.config_lock:
                current_cfg = self.get_guild_config(guild_id)
                current_cfg["asked_questions"].append(question_data["question"])
                if len(current_cfg["asked_questions"]) > 200: current_cfg["asked_questions"] = current_cfg["asked_questions"][-200:]
                reveal_time = datetime.now(timezone.utc) + timedelta(minutes=current_cfg["reveal_delay"])
                current_cfg["pending_answers"].append({
                    "message_id": msg.id, "channel_id": channel.id, "question": question_data["question"],
                    "answer": question_data["correct_answer"], "reveal_at_iso": reveal_time.isoformat(),
                    "winners": [], "all_answers": {}
                })
                current_cfg["last_posted_date"] = datetime.now(pytz.timezone(current_cfg["timezone"])).strftime("%Y-%m-%d")
                self.config_is_dirty = True
            return msg
        except discord.Forbidden:
            log_trivia.error(f"Missing permissions to post trivia in guild {guild_id}, channel {channel.id}.")
        return None

    @tasks.loop(minutes=1)
    async def trivia_loop(self):
        now_utc = datetime.now(timezone.utc)
        pending_reveals = []
        
        async with self.config_lock:
            for guild_id_str, cfg in self.config.items():
                if pending_answers := cfg.get("pending_answers", []):
                    still_pending = []
                    for ans in pending_answers:
                        if isinstance(ans.get("reveal_at_iso"), str) and now_utc >= datetime.fromisoformat(ans["reveal_at_iso"]):
                            pending_reveals.append(ans)
                        else:
                            still_pending.append(ans)
                    if len(still_pending) < len(pending_answers):
                        cfg["pending_answers"] = still_pending
                        self.config_is_dirty = True
        
        for reveal_data in pending_reveals:
            await self.reveal_trivia_answer(reveal_data)

        for guild_id_str, cfg in self.config.items():
            if cfg.get("enabled") and cfg.get("channel_id"):
                try:
                    tz = pytz.timezone(cfg.get("timezone", "UTC"))
                    post_time_obj = time.fromisoformat(cfg.get("time", "12:00"))
                    if now_utc.astimezone(tz).time() >= post_time_obj and cfg.get("last_posted_date") != now_utc.astimezone(tz).strftime("%Y-%m-%d"):
                        await self.post_trivia_question(int(guild_id_str), cfg)
                except Exception as e:
                    log_trivia.error(f"Error during trivia scheduling for guild {guild_id_str}: {e}")
    
    @tasks.loop(minutes=30)
    async def cache_refill_loop(self):
        async with self.config_lock:
            for guild_id_str, cfg in self.config.items():
                if not cfg.get("enabled"): continue
                cache = cfg.setdefault("question_cache", [])
                if len(cache) < CACHE_MIN_SIZE:
                    log_trivia.info(f"Trivia cache for guild {guild_id_str} is low. Refilling.")
                    new_questions = await self.fetch_api_questions()
                    asked_questions = set(cfg.get("asked_questions", []))
                    added_count = 0
                    for q_data in new_questions:
                        question_text = html.unescape(q_data["question"])
                        if question_text not in asked_questions:
                            cache.append(q_data)
                            added_count += 1
                        if len(cache) >= CACHE_TARGET_SIZE: break
                    if added_count > 0:
                        self.config_is_dirty = True
                        log_trivia.info(f"Added {added_count} new questions to the cache for guild {guild_id_str}.")

    @tasks.loop(hours=1)
    async def monthly_winner_loop(self):
        now = datetime.now(timezone.utc)
        for guild_id_str, cfg in list(self.config.items()):
            last_announcement_str = cfg.get("last_winner_announcement", "2000-01-01T00:00:00.000000+00:00")
            last_announcement_date = datetime.fromisoformat(last_announcement_str)

            if (now.year > last_announcement_date.year) or (now.month > last_announcement_date.month):
                scores = cfg.get("monthly_scores")
                month_to_announce = last_announcement_date
                
                async with self.config_lock:
                    if not scores:
                        cfg["monthly_scores"] = {}
                        cfg["last_winner_announcement"] = now.isoformat()
                        self.config_is_dirty = True
                        continue
                    
                    max_score = max(scores.values())
                    top_scorers = [int(uid) for uid, score in scores.items() if score == max_score]
                    channel = self.bot.get_channel(cfg.get("channel_id"))
                    if not channel: continue

                    month_name = month_to_announce.strftime("%B")
                    year = month_to_announce.strftime("%Y")
                    
                    embed = discord.Embed(title=f"🏅 Trivia Player of the Month: {month_name} {year}", description="A new month of trivia begins! Let's recognize the champion(s) from last month who demonstrated exceptional knowledge.", color=0xFFD700)
                    embed.set_thumbnail(url="https://i.imgur.com/SceEM4y.png")
                    winner_mentions = ", ".join(f"<@{uid}>" for uid in top_scorers)
                    embed.add_field(name="🏆 Top Scorer(s)", value=f"Congratulations to {winner_mentions} for achieving the highest score!", inline=False)
                    embed.add_field(name="Score", value=f"They answered an incredible **{max_score}** questions correctly!", inline=False)
                    embed.set_footer(text="Will they defend their title? A new challenge starts now!")

                    try:
                        await channel.send(content=winner_mentions, embed=embed)
                        log_trivia.info(f"Posted monthly trivia winners for guild {guild_id_str}.")
                        cfg["monthly_scores"] = {}
                        cfg["last_winner_announcement"] = now.isoformat()
                        self.config_is_dirty = True
                    except discord.Forbidden:
                        log_trivia.warning(f"Could not post monthly winner announcement in guild {guild_id_str}.")

    @trivia_loop.before_loop
    @cache_refill_loop.before_loop
    @monthly_winner_loop.before_loop
    @save_loop.before_loop
    async def before_any_loop(self):
        await self.bot.wait_until_ready()

    trivia = app_commands.Group(name="trivia", description="Commands for the daily trivia.", default_permissions=discord.Permissions(manage_guild=True))

    @trivia.command(name="settings", description="Show the current trivia settings.")
    async def trivia_settings(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
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
        await interaction.response.defer(ephemeral=True)
        async with self.config_lock:
            cfg = self.get_guild_config(interaction.guild_id)
            cfg["enabled"] = enabled
            self.config_is_dirty = True
        state_text = "Enabled" if enabled else "Disabled"
        embed = discord.Embed(description=f"✅ Daily trivia is now **{state_text}**.", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)

    @trivia.command(name="channel", description="Set the channel for daily trivia questions.")
    async def set_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)
        async with self.config_lock:
            cfg = self.get_guild_config(interaction.guild_id)
            cfg["channel_id"] = channel.id
            self.config_is_dirty = True
        embed = discord.Embed(description=f"✅ Trivia will now post in {channel.mention}.", color=EMBED_COLOR_TRIVIA)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)

    @trivia.command(name="time", description="Set the daily post time (24h format HH:MM).")
    async def set_time(self, interaction: discord.Interaction, time_str: str):
        await interaction.response.defer(ephemeral=True)
        try:
            time.fromisoformat(time_str)
            async with self.config_lock:
                cfg = self.get_guild_config(interaction.guild_id)
                cfg["time"] = time_str
                self.config_is_dirty = True
            embed = discord.Embed(description=f"✅ Trivia will now post daily at **{time_str}**.", color=EMBED_COLOR_TRIVIA)
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)
        except ValueError:
            embed = discord.Embed(description="❌ Invalid time format. Use **HH:MM** (24-hour).", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)

    @trivia.command(name="timezone", description="Set your server's timezone for accurate posting.")
    @app_commands.describe(timezone="E.g., America/New_York, Europe/London, etc.")
    async def set_timezone(self, interaction: discord.