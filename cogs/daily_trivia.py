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
from collections import Counter
import urllib.parse

log_trivia = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE_TRIVIA = "trivia_config.json"
TRIVIA_API_URL_BASE = "https://opentdb.com/api.php?type=multiple"
CACHE_FETCH_AMOUNT = 50
EMBED_COLOR_TRIVIA = 0x1ABC9C
CACHE_MIN_SIZE = 5
CACHE_TARGET_SIZE = 10
INTERACTION_HISTORY_DAYS = 60
LEADERBOARD_LIMIT = 15

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

class DoubleOrNothingView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia', user_id: int, original_question_url: str):
        super().__init__(timeout=180)
        self.cog = cog_instance
        self.user_id = user_id
        self.original_question_url = original_question_url
        self.message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This is not for you!", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Double or Nothing?", style=discord.ButtonStyle.success, emoji="🎲")
    async def double_or_nothing(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        button.disabled = True
        await interaction.edit_original_response(view=self)
        self.cog.don_pending_users.add(self.user_id)
        await self.cog.start_double_or_nothing_game(interaction, self.user_id)
        
    async def on_timeout(self):
        self.cog.don_pending_users.discard(self.user_id)
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(content="You took too long to decide. The offer has expired.", view=self)
            except (discord.NotFound, discord.HTTPException):
                pass


class DONQuestionView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia', user_id: int, correct_answer: str):
        super().__init__(timeout=30.0)
        self.cog = cog_instance
        self.user_id = user_id
        self.correct_answer = correct_answer
        self.answered = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This is not for you!", ephemeral=True)
            return False
        return True

    async def handle_don_answer(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.answered = True
        self.stop()
        self.cog.don_pending_users.discard(self.user_id)
        for item in self.children: item.disabled = True
        
        is_correct = (button.label == self.correct_answer)
        
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_config(interaction.guild_id)
            firsts = cfg.setdefault("monthly_firsts", {})
            user_id_str = str(self.user_id)

            if is_correct:
                firsts[user_id_str] = firsts.get(user_id_str, 0) + 1
                embed = discord.Embed(title="🎉 You did it!", description="You answered correctly and earned a bonus point!", color=discord.Color.green())
            else:
                firsts[user_id_str] = firsts.get(user_id_str, 1) - 1
                embed = discord.Embed(title="❌ Oh no!", description=f"That was incorrect. The correct answer was **{self.correct_answer}**.\nYou lost the point you just earned.", color=discord.Color.red())
            
            self.cog.config_is_dirty = True
        
        await interaction.response.edit_message(embed=embed, view=self)

class ConfirmView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=60)
        self.value = None
        self.interaction_user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.interaction_user_id:
            await interaction.response.send_message("This confirmation is not for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

class HelpView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia'):
        super().__init__(timeout=180)
        self.cog = cog_instance

    @discord.ui.select(
        placeholder="Choose a help category...",
        options=[
            discord.SelectOption(label="Game Rules", description="Learn how to play and how scoring works.", emoji="📜"),
            discord.SelectOption(label="User Commands", description="Commands available to everyone.", emoji="👤"),
            discord.SelectOption(label="Admin Commands", description="Commands for server administrators.", emoji="👑"),
        ]
    )
    async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        embed = self.cog.get_help_embed(select.values[0])
        await interaction.response.edit_message(embed=embed)

# --- NEW: Admin Panel UI ---
class AdminPanelView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia', original_interaction_user: discord.User):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_user = original_interaction_user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.original_user.id:
            await interaction.response.send_message("Only the user who opened the panel can use these buttons.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Set Channel", style=discord.ButtonStyle.secondary, emoji="📺")
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        # This would typically open a modal or use a channel select menu
        await interaction.response.send_message("Please use the `/trivia set channel` command for now.", ephemeral=True)

    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.secondary, emoji="👑")
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = discord.ui.View()
        select = discord.ui.RoleSelect(placeholder="Select the trivia admin role...")

        async def select_callback(inter: discord.Interaction):
            role = inter.data['values'][0]
            async with self.cog.config_lock:
                cfg = self.cog.get_guild_config(inter.guild_id)
                cfg['admin_role_id'] = int(role)
                self.cog.config_is_dirty = True
            await inter.response.send_message(f"✅ Trivia Admin role set to <@&{role}>.", ephemeral=True)
            view.stop()

        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a role to be the Trivia Admin:", view=view, ephemeral=True)

    @discord.ui.button(label="Force Daily Question", style=discord.ButtonStyle.primary, emoji="📅")
    async def force_daily_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.manual_post_daily_question(interaction)

    @discord.ui.button(label="Post Random Question", style=discord.ButtonStyle.success, emoji="🎲")
    async def post_random_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.post_random_question(interaction)

# --- Main Cog ---
class DailyTrivia(commands.Cog):
    trivia = app_commands.Group(name="trivia", description="Commands for the daily trivia.")
    trivia_admin = app_commands.Group(name="trivia_admin", description="Admin commands for the daily trivia.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_trivia()
        self.session = aiohttp.ClientSession()
        self.config_lock = asyncio.Lock()
        self.config_is_dirty = False
        self.don_pending_users = set()
        
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
        
    async def is_trivia_admin(self, interaction: discord.Interaction) -> bool:
        if await self.bot.is_owner(interaction.user): return True
        if interaction.user.guild_permissions.manage_guild: return True
        cfg = self.get_guild_config(interaction.guild_id)
        admin_role_id = cfg.get("admin_role_id")
        if admin_role_id:
            role = interaction.guild.get_role(admin_role_id)
            if role and role in interaction.user.roles:
                return True
        return False

    @commands.Cog.listener()
    async def on_guild_remove(self, guild: discord.Guild):
        async with self.config_lock:
            if str(guild.id) in self.config:
                del self.config[str(guild.id)]
                self.config_is_dirty = True
                log_trivia.info(f"Removed configuration for guild {guild.id} as I have left.")

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CommandInvokeError) and isinstance(error.original, app_commands.CheckFailure):
             embed = discord.Embed(description="❌ You don't have permission to use this command.", color=discord.Color.red())
             await interaction.response.send_message(embed=embed, ephemeral=True)
             return

        embed = discord.Embed(color=discord.Color.red())
        embed.set_footer(text="Daily Trivia")
        log_trivia.error(f"An unhandled error occurred in a command: {error}", exc_info=True)
        embed.description = "An unexpected error occurred. Please try again later."
        
        try:
            if interaction.response.is_done():
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
        except discord.NotFound:
             log_trivia.warning("Could not send error message to interaction, it might have expired.")
            
    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {
                "channel_id": None, "time": "12:00", "timezone": "UTC", "enabled": False,
                "pending_answers": [], "asked_questions": [], "question_cache": [], 
                "reveal_delay": 60, 
                "last_winner_announcement": datetime.now(timezone.utc).isoformat(),
                "daily_interactions": [], "admin_role_id": None, "last_day_winner_id": None,
                "last_question_data": None, "mutes": {}, "question_stats": [],
                "monthly_firsts": {}, "monthly_correct_answers": {}
            }
        # Ensure all default keys exist on load
        defaults = {
            "monthly_firsts": {}, "monthly_correct_answers": {}, 
            "last_winner_announcement": "2000-01-01T00:00:00.000000+00:00",
            "question_cache": [], "reveal_delay": 60, "daily_interactions": [],
            "admin_role_id": None, "last_day_winner_id": None, "last_question_data": None,
            "mutes": {}, "question_stats": [], "last_posted_date": None
        }
        for key, value in defaults.items():
            self.config[gid].setdefault(key, value)
        return self.config[gid]

    @tasks.loop(seconds=30)
    async def save_loop(self):
        try:
            async with self.config_lock:
                if self.config_is_dirty:
                    await self.bot.loop.run_in_executor(None, lambda: save_config_trivia(self.config))
                    self.config_is_dirty = False
                    log_trivia.info("Trivia config changes saved to disk.")
        except Exception as e:
            log_trivia.error(f"An unhandled error occurred in the save_loop: {e}", exc_info=True)

    async def fetch_api_questions(self, amount: int = CACHE_FETCH_AMOUNT):
        url = f"{TRIVIA_API_URL_BASE}&amount={amount}"
        try:
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("response_code") == 0:
                        return data.get("results", [])
        except Exception as e:
            log_trivia.error(f"Failed to fetch trivia from API: {e}")
        return []

    async def handle_trivia_answer(self, interaction: discord.Interaction, button: discord.ui.Button):
        cfg = self.get_guild_config(interaction.guild_id)
        mutes = cfg.get("mutes", {})
        user_id_str = str(interaction.user.id)
        
        if user_id_str in mutes:
            mute_end_time = datetime.fromisoformat(mutes[user_id_str])
            if datetime.now(timezone.utc) < mute_end_time:
                await interaction.followup.send("❌ You are currently muted from participating in trivia.", ephemeral=True)
                return
            else:
                async with self.config_lock:
                    mutes.pop(user_id_str, None)
                    self.config_is_dirty = True

        message_to_send = ""
        async with self.config_lock:
            pending_answers = cfg.get("pending_answers", [])
            target_question = next((q for q in pending_answers if q.get("message_id") == interaction.message.id), None)

            if not target_question:
                message_to_send = "This trivia question has expired."
            else:
                if user_id_str in target_question.get("all_answers", {}):
                    message_to_send = "You have already answered this question!"
                else:
                    target_question.setdefault("all_answers", {})[user_id_str] = button.label
                    is_correct = (button.label == target_question["answer"])
                    
                    if is_correct:
                        target_question.setdefault("winners", []).append(interaction.user.id)
                        message_to_send = f"✅ Correct! You answered: `{button.label}`."
                    else:
                        message_to_send = f"❌ Sorry, that's incorrect. You answered: `{button.label}`."
                    
                    self.config_is_dirty = True

        disabled_view = TriviaView(self)
        for item in disabled_view.children: item.disabled = True
        await interaction.followup.send(message_to_send, view=disabled_view, ephemeral=True)

    async def reveal_trivia_answer(self, answer_data: dict):
        channel = self.bot.get_channel(answer_data["channel_id"])
        if not channel: return
        
        original_msg = None
        try:
            original_msg = await channel.fetch_message(answer_data["message_id"])
            await original_msg.edit(view=None)
        except (discord.NotFound, discord.Forbidden):
            pass
        
        winner_ids = answer_data.get("winners", [])
        all_answers_dict = answer_data.get("all_answers", {})

        async with self.config_lock:
            cfg = self.get_guild_config(channel.guild.id)
            
            correct_answers_board = cfg.setdefault("monthly_correct_answers", {})
            for winner_id in winner_ids:
                winner_id_str = str(winner_id)
                correct_answers_board[winner_id_str] = correct_answers_board.get(winner_id_str, 0) + 1

            if winner_ids:
                first_winner_id = winner_ids[0]
                firsts_board = cfg.setdefault("monthly_firsts", {})
                firsts_board[str(first_winner_id)] = firsts_board.get(str(first_winner_id), 0) + 1
                cfg["last_day_winner_id"] = first_winner_id

                if first_winner_id not in self.don_pending_users:
                    try:
                        first_winner_user = await self.bot.fetch_user(first_winner_id)
                        if first_winner_user:
                            view = DoubleOrNothingView(self, first_winner_id, original_msg.jump_url if original_msg else "")
                            offer_text = (
                                f"You got the fastest answer in **{channel.guild.name}**! Want to risk your point for a bonus?\n"
                                f"[Click here to view the question]({view.original_question_url})\n\n"
                                f"⚠️ **Warning:** If you accept, you will only have **30 seconds** to answer the next question."
                            )
                            message = await first_winner_user.send(content=offer_text, view=view)
                            view.message = message
                            self.don_pending_users.add(first_winner_id)
                    except discord.Forbidden:
                        log_trivia.warning(f"Could not DM user {first_winner_id} for D-o-N prompt. They may have DMs disabled.")
                    except Exception as e:
                        log_trivia.error(f"Failed to send D-o-N prompt to {first_winner_id}: {e}")
            else:
                cfg["last_day_winner_id"] = None

            interactions = cfg.setdefault("daily_interactions", [])
            interactions.append({"date": datetime.now(timezone.utc).isoformat(), "first_winner": winner_ids[0] if winner_ids else None, "all_winners": winner_ids})
            while len(interactions) > INTERACTION_HISTORY_DAYS: interactions.pop(0)

            total_participants = len(all_answers_dict)
            if total_participants > 0:
                question_stats = cfg.setdefault("question_stats", [])
                question_stats.append({"question_text": answer_data["question"], "participants": total_participants, "correct_count": len(winner_ids), "date": datetime.now(timezone.utc).isoformat()})

            cfg["last_question_data"] = answer_data
            self.config_is_dirty = True

        results_embed = discord.Embed(title="🏆 Trivia Results", description=f"**Question:** {answer_data['question']}", color=discord.Color.gold())
        if winner_ids:
            try:
                winner_user = await self.bot.fetch_user(winner_ids[0])
                results_embed.set_thumbnail(url=winner_user.display_avatar.url)
            except (discord.NotFound, discord.Forbidden): pass
        
        results_embed.add_field(name="Correct Answer", value=f"**`{answer_data['answer']}`**", inline=False)
        
        label_map = answer_data.get("label_map", {})
        answer_counts = Counter(all_answers_dict.values())
        stats_value = ""
        for label, count in answer_counts.items():
            full_text = label_map.get(label, label)
            stats_value += f"`{full_text}`: {count} vote(s)\n"

        if stats_value: 
            results_embed.add_field(name="📊 Vote Distribution", value=stats_value, inline=False)
        
        if not winner_ids:
            results_embed.add_field(name="🎉 Winners", value="No one got the correct answer this time!", inline=False)
        else:
            results_embed.add_field(name="🥇 Fastest Correct Answer", value=f"<@{winner_ids[0]}>", inline=False)
            other_winners = winner_ids[1:]
            if other_winners:
                mentions = ", ".join(f"<@{uid}>" for uid in other_winners)
                results_embed.add_field(name="Other Correct Answers", value=mentions, inline=False)
        
        search_term = urllib.parse.quote_plus(answer_data['answer'])
        wiki_url = f"https://en.wikipedia.org/w/index.php?search={search_term}"
        results_embed.add_field(name="Learn More", value=f"[Search for '{answer_data['answer']}' on Wikipedia]({wiki_url})", inline=False)
        
        category = answer_data.get("category", "General Knowledge")
        results_embed.set_footer(text=f"Daily Trivia | Category: {category}")
        
        if original_msg:
            await original_msg.reply(embed=results_embed)
        else:
            await channel.send(embed=results_embed)

    async def start_double_or_nothing_game(self, interaction: discord.Interaction, user_id: int):
        question_data_list = await self.fetch_api_questions(10)
        if not question_data_list:
            self.don_pending_users.discard(user_id)
            await interaction.edit_original_response(content="I couldn't fetch a new question for you, sorry! Your point is safe.", view=None)
            return

        q_data = question_data_list[0]
        correct_answer = html.unescape(q_data["correct_answer"])
        all_answers = [html.unescape(ans) for ans in q_data["incorrect_answers"]] + [correct_answer]
        random.shuffle(all_answers)

        end_time = datetime.now(timezone.utc) + timedelta(seconds=30)
        end_timestamp = int(end_time.timestamp())

        embed = discord.Embed(
            title="🎲 Double or Nothing!",
            description=f"**Question:** {html.unescape(q_data['question'])}\n\n"
                        f"⏳ Time remaining: <t:{end_timestamp}:R>",
            color=discord.Color.orange())
        
        view = DONQuestionView(self, user_id, correct_answer)
        for answer_text in all_answers:
            if len(answer_text) > 80: answer_text = answer_text[:77] + "..."
            button = discord.ui.Button(label=answer_text, style=discord.ButtonStyle.secondary)
            async def button_callback(interaction: discord.Interaction, btn=button):
                await view.handle_don_answer(interaction, btn)
            button.callback = button_callback
            view.add_item(button)

        await interaction.edit_original_response(embed=embed, view=view)
        
        timed_out = await view.wait()
        if timed_out and not view.answered:
            self.don_pending_users.discard(user_id)
            async with self.config_lock:
                cfg = self.get_guild_config(interaction.guild_id)
                firsts = cfg.setdefault("monthly_firsts", {})
                user_id_str = str(user_id)
                firsts[user_id_str] = firsts.get(user_id_str, 1) - 1
                self.config_is_dirty = True
            
            timeout_embed = discord.Embed(title="⌛ Time's Up!", description=f"You ran out of time. The correct answer was **{correct_answer}**.\nYou lost the point you just earned.", color=discord.Color.red())
            await interaction.edit_original_response(embed=timeout_embed, view=None)

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
            results = await self.fetch_api_questions(10)
            if results: question_data = results.pop(0)
        
        if not question_data:
            await channel.send("Could not retrieve any trivia question. The trivia API might be down. Please try again later.")
            return
        
        question_text = html.unescape(question_data["question"])
        correct_answer = html.unescape(question_data["correct_answer"])
        all_answers = [html.unescape(ans) for ans in question_data["incorrect_answers"]] + [correct_answer]
        random.shuffle(all_answers)

        reveal_time = datetime.now(timezone.utc) + timedelta(minutes=cfg["reveal_delay"])
        reveal_timestamp = int(reveal_time.timestamp())

        description = (
            f"**{question_text}**\n\n"
            f"*This question closes at <t:{reveal_timestamp}:t> (<t:{reveal_timestamp}:R>).*"
        )
        
        embed = discord.Embed(title="❓ Daily Trivia Question!", description=description, color=EMBED_COLOR_TRIVIA)
        
        last_winner_id = cfg.get("last_day_winner_id")
        if last_winner_id:
            embed.add_field(name="Yesterday's Fastest Answer", value=f"From <@{last_winner_id}>! 🏆", inline=False)

        category = html.unescape(question_data['category'])
        embed.set_footer(text=f"Daily Trivia | Category: {category}")
        
        view = TriviaView(self)
        label_map = {}
        for i, answer_text in enumerate(all_answers):
            if i < len(view.children):
                label = answer_text
                if len(label) > 80:
                    label = label[:77] + "..."
                view.children[i].label = label
                label_map[label] = answer_text

        try:
            msg = await channel.send(embed=embed, view=view)
            async with self.config_lock:
                current_cfg = self.get_guild_config(guild_id)
                pending_question = {
                    "message_id": msg.id,
                    "channel_id": channel.id,
                    "question": question_text,
                    "answer": correct_answer,
                    "reveal_at_iso": reveal_time.isoformat(),
                    "winners": [],
                    "all_answers": {},
                    "category": category,
                    "label_map": label_map
                }
                current_cfg["pending_answers"].append(pending_question)
                current_cfg["last_posted_date"] = datetime.now(pytz.timezone(current_cfg["timezone"])).strftime("%Y-%m-%d")
                self.config_is_dirty = True
            return msg
        except discord.Forbidden:
            log_trivia.error(f"Missing permissions to post trivia in guild {guild_id}, channel {channel.id}.")
        return None

    @tasks.loop(minutes=1)
    async def trivia_loop(self):
        try:
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
                        now_local = now_utc.astimezone(tz)
                        last_posted = cfg.get("last_posted_date")
                        
                        if now_local.time() >= post_time_obj and last_posted != now_local.strftime("%Y-%m-%d"):
                            is_active = any(p['channel_id'] == cfg['channel_id'] for p in cfg.get('pending_answers', []))
                            if not is_active:
                                await self.post_trivia_question(int(guild_id_str), cfg)
                    except Exception as e:
                        log_trivia.error(f"Error during trivia scheduling for guild {guild_id_str}: {e}")
        except Exception as e:
            log_trivia.error(f"An unhandled error occurred in the trivia_loop: {e}", exc_info=True)
    
    @tasks.loop(minutes=30)
    async def cache_refill_loop(self):
        try:
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
        except Exception as e:
            log_trivia.error(f"An unhandled error occurred in the cache_refill_loop: {e}", exc_info=True)

    @tasks.loop(hours=1)
    async def monthly_winner_loop(self):
        try:
            now = datetime.now(timezone.utc)
            if now.day != 1: return

            for guild_id_str, cfg in list(self.config.items()):
                last_announcement_str = cfg.get("last_winner_announcement") or "2000-01-01T00:00:00.000000+00:00"
                try:
                    last_announcement_date = datetime.fromisoformat(last_announcement_str)
                except ValueError:
                        last_announcement_date = datetime.fromisoformat("2000-01-01T00:00:00.000000+00:00")

                if (now.year > last_announcement_date.year) or (now.month > last_announcement_date.month):
                    channel = self.bot.get_channel(cfg.get("channel_id"))
                    if not channel: continue
                    
                    firsts = cfg.get("monthly_firsts", {})
                    if not firsts:
                        async with self.config_lock:
                            cfg["monthly_firsts"] = {}
                            cfg["monthly_correct_answers"] = {}
                            cfg["question_stats"] = []
                            cfg["last_winner_announcement"] = now.isoformat()
                            self.config_is_dirty = True
                        continue
                    
                    max_score = max(firsts.values())
                    top_scorers = [int(uid) for uid, score in firsts.items() if score == max_score]
                    
                    if len(top_scorers) > 1:
                        tie_embed = discord.Embed(title="⚔️ Monthly Tiebreaker! ⚔️", description="We have a tie for Player of the Month! A live Sudden Death round will begin shortly to determine the ultimate champion.", color=discord.Color.orange())
                        tie_embed.add_field(name="Contenders", value=", ".join(f"<@{uid}>" for uid in top_scorers))
                        await channel.send(embed=tie_embed)
                        await asyncio.sleep(10)
                        await self.run_sudden_death(channel, top_scorers)
                    else:
                        month_to_announce = last_announcement_date
                        month_name = month_to_announce.strftime("%B")
                        year = month_to_announce.strftime("%Y")
                        embed = discord.Embed(title=f"🏅 Trivia Player of the Month: {month_name} {year}", description=f"A new month of trivia begins! Let's recognize the champion from last month.", color=0xFFD700)
                        embed.set_thumbnail(url="https://i.imgur.com/SceEM4y.png")
                        winner_mentions = f"<@{top_scorers[0]}>"
                        embed.add_field(name="🏆 Champion of Firsts", value=f"Congratulations to {winner_mentions}!", inline=False)
                        embed.add_field(name="Top Score", value=f"They achieved an incredible **{max_score}** first correct answers!", inline=False)
                        embed.set_footer(text="Will they defend their title? A new challenge starts now!").timestamp = now
                        await channel.send(content=winner_mentions, embed=embed)
                    
                    question_stats = cfg.get("question_stats", [])
                    if question_stats:
                        hardest_question = min(question_stats, key=lambda q: (q['correct_count'] / q['participants']) if q['participants'] > 0 else 1)
                        h_embed = discord.Embed(title="🧠 Most Elusive Question of the Month", color=0x992D22)
                        h_embed.add_field(name="Question", value=hardest_question['question_text'], inline=False)
                        correct_percent = (hardest_question['correct_count'] / hardest_question['participants']) * 100 if hardest_question['participants'] > 0 else 0
                        h_embed.add_field(name="Statistics", value=f"{hardest_question['participants']} Participants, only **{correct_percent:.1f}%** answered correctly!", inline=False)
                        await channel.send(embed=h_embed)

                    async with self.config_lock:
                        cfg_to_update = self.get_guild_config(int(guild_id_str))
                        cfg_to_update["monthly_firsts"] = {}
                        cfg_to_update["monthly_correct_answers"] = {}
                        cfg_to_update["question_stats"] = []
                        cfg_to_update["last_winner_announcement"] = now.isoformat()
                        self.config_is_dirty = True
        except Exception as e:
            log_trivia.error(f"An unhandled error occurred in the monthly_winner_loop: {e}", exc_info=True)

    async def run_sudden_death(self, channel: discord.TextChannel, contenders: list[int]):
        scores = Counter()
        contender_set = set(contenders)

        for i in range(5):
            q_list = await self.fetch_api_questions(1)
            if not q_list:
                await channel.send("Could not fetch a question for the tiebreaker. Ending now.")
                break
            
            q = q_list[0]
            correct_answer = html.unescape(q["correct_answer"])
            all_answers = [html.unescape(ans) for ans in q["incorrect_answers"]] + [correct_answer]
            random.shuffle(all_answers)
            
            view = TriviaView(self)
            for j, ans_text in enumerate(all_answers):
                view.children[j].label = ans_text[:80]
            
            embed = discord.Embed(title=f"Tiebreaker Round {i+1}/5", description=f"**{html.unescape(q['question'])}**", color=discord.Color.red())
            msg = await channel.send(embed=embed, view=view)

            try:
                def check(interaction: discord.Interaction):
                    return interaction.user.id in contender_set and interaction.message.id == msg.id

                interaction = await self.bot.wait_for("interaction", timeout=20.0, check=check)
                
                answered_button = discord.utils.get(view.children, custom_id=interaction.custom_id)
                if answered_button.label == correct_answer:
                    scores[interaction.user.id] += 1
                    await interaction.response.send_message(f"✅ {interaction.user.mention} answered correctly and gets a point!", ephemeral=False)
                else:
                    scores[interaction.user.id] -= 1
                    await interaction.response.send_message(f"❌ {interaction.user.mention} was first, but incorrect! **They lose a point.**", ephemeral=False)

            except asyncio.TimeoutError:
                await msg.channel.send("No one answered in time for this round!")
            
            await msg.edit(view=None)
            await asyncio.sleep(5)

        if not scores:
            final_winners = contenders
        else:
            max_score = max(scores.values())
            final_winners = [uid for uid, score in scores.items() if score == max_score]

        embed = discord.Embed(title="Sudden Death Concluded!", color=0xFFD700)
        winner_mentions = ", ".join(f"<@{uid}>" for uid in final_winners)
        embed.description = f"Congratulations to our new Player(s) of the Month: {winner_mentions}!"
        await channel.send(embed=embed)

    @trivia_loop.before_loop
    async def before_trivia_loop(self):
        await self.bot.wait_until_ready()

    @cache_refill_loop.before_loop
    async def before_cache_refill_loop(self):
        await self.bot.wait_until_ready()

    @monthly_winner_loop.before_loop
    async def before_monthly_winner_loop(self):
        await self.bot.wait_until_ready()

    @save_loop.before_loop
    async def before_save_loop(self):
        await self.bot.wait_until_ready()
        
    def get_help_embed(self, category: str) -> discord.Embed:
        embed = discord.Embed(title="❓ Trivia Help", color=EMBED_COLOR_TRIVIA)
        if category == "Game Rules":
            embed.description = "Detailed explanation of the trivia game rules."
            embed.add_field(name="📜 Gameplay Flow", value="A new question is posted daily. Click the buttons to submit your answer. After a delay, the answer is revealed, and scores are updated.", inline=False)
            embed.add_field(name="🥇 Scoring: Firsts vs. Totals", value="There are two leaderboards: `/trivia firstsboard` for the fastest correct answer, and `/trivia leaderboard` for the most total correct answers. Both reset monthly.", inline=False)
            embed.add_field(name="🎲 Double or Nothing", value="The first winner is offered a high-stakes bonus round. Win, and you get a bonus point on the `firstsboard`. Lose, and you lose the point you just earned.", inline=False)
            embed.add_field(name="⚔️ Sudden Death Tiebreaker", value="If the month ends in a tie on the `firstsboard`, the contenders face off in a live match to determine the champion.", inline=False)
            embed.add_field(name="🤝 Nemesis & Ally", value="The `/trivia_stats view` command shows which user most often beats you to the first answer (your Nemesis) and who you most often win alongside (your Ally).", inline=False)
        elif category == "User Commands":
            embed.description = "Commands available to everyone."
            embed.add_field(name="`/trivia help`", value="Shows this interactive help message.", inline=False)
            embed.add_field(name="`/trivia leaderboard`", value="Displays the monthly leaderboard for most correct answers.", inline=False)
            embed.add_field(name="`/trivia firstsboard`", value="Displays the monthly leaderboard for fastest answers.", inline=False)
            embed.add_field(name="`/trivia_stats view`", value="Shows your personal trivia stats.", inline=False)
            embed.add_field(name="`/trivia_stats compare`", value="Compares your stats against another user.", inline=False)
            embed.add_field(name="`/trivia lastquestion`", value="Shows the results of the most recent trivia question.", inline=False)
        elif category == "Admin Commands":
            embed.description = "Commands for server administrators."
            embed.add_field(name="`/trivia_admin panel`", value="Opens the main admin control panel.", inline=False)
            embed.add_field(name="`/trivia_admin set`", value="Sets core settings like the trivia channel and time.", inline=False)
            embed.add_field(name="`/trivia_admin toggle`", value="Enables or disables the daily trivia.", inline=False)
            embed.add_field(name="`/trivia_admin mute/unmute`", value="Manages a user's ability to participate in trivia.", inline=False)
        return embed

    # --- NEW: Admin Panel Logic ---
    async def manual_post_daily_question(self, interaction: discord.Interaction):
        cfg = self.get_guild_config(interaction.guild_id)
        if not cfg.get("channel_id"):
            await interaction.followup.send("❌ A trivia channel must be set before posting a question.", ephemeral=True)
            return

        is_active = any(p['channel_id'] == cfg['channel_id'] for p in cfg.get('pending_answers', []))
        if is_active:
            await interaction.followup.send("❌ A daily trivia question is already active in the channel.", ephemeral=True)
            return

        await self.post_trivia_question(interaction.guild_id, cfg)
        await interaction.followup.send("✅ Successfully posted the daily trivia question.", ephemeral=True)

    async def post_random_question(self, interaction: discord.Interaction):
        cfg = self.get_guild_config(interaction.guild_id)
        channel = self.bot.get_channel(cfg.get("channel_id"))
        if not channel:
            await interaction.followup.send("❌ A trivia channel must be set before posting a random question.", ephemeral=True)
            return

        q_list = await self.fetch_api_questions(1)
        if not q_list:
            await interaction.followup.send("❌ Could not fetch a question from the API.", ephemeral=True)
            return

        q = q_list[0]
        question_text = html.unescape(q["question"])
        correct_answer = html.unescape(q["correct_answer"])
        all_answers = [html.unescape(ans) for ans in q["incorrect_answers"]] + [correct_answer]
        random.shuffle(all_answers)

        embed = discord.Embed(title="🎲 Random Trivia Question!", description=f"**{question_text}**", color=0x7289DA)
        embed.set_footer(text=f"Posted by {interaction.user.display_name} | This question is just for fun and does not count for points.")

        view = discord.ui.View(timeout=120)
        for answer_text in all_answers:
            button = discord.ui.Button(label=answer_text[:80], style=discord.ButtonStyle.secondary)
            
            async def btn_callback(inter: discord.Interaction, btn=button, correct_ans=correct_answer):
                for child in view.children:
                    child.disabled = True
                
                if btn.label == correct_ans:
                    result_embed = discord.Embed(title="🎉 Correct!", description=f"{inter.user.mention} got the right answer: **{correct_ans}**", color=discord.Color.green())
                else:
                    result_embed = discord.Embed(title="❌ Incorrect!", description=f"{inter.user.mention} chose `{btn.label}`. The correct answer was **{correct_ans}**", color=discord.Color.red())
                
                await inter.response.edit_message(embed=result_embed, view=view)
                view.stop()

            button.callback = btn_callback
            view.add_item(button)
        
        await channel.send(embed=embed, view=view)
        await interaction.followup.send(f"✅ Random question posted in {channel.mention}.", ephemeral=True)

    # --- Application Commands ---

    @trivia.command(name="help", description="Explains the trivia rules and lists commands.")
    async def trivia_help(self, interaction: discord.Interaction):
        embed = self.get_help_embed("Game Rules")
        await interaction.response.send_message(embed=embed, view=HelpView(self), ephemeral=True)

    @trivia.command(name="leaderboard", description="Shows the monthly leaderboard for total correct answers.")
    async def trivia_leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        cfg = self.get_guild_config(interaction.guild_id)
        scores = cfg.get("monthly_correct_answers", {})

        if not scores:
            embed = discord.Embed(description="The monthly leaderboard is empty! Be the first to get a correct answer.", color=EMBED_COLOR_TRIVIA)
            await interaction.followup.send(embed=embed)
            return

        sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        
        month_name = datetime.now(pytz.timezone(cfg.get("timezone", "UTC"))).strftime("%B")
        embed = discord.Embed(title=f"📊 Monthly Leaderboard: {month_name}", description="Top players by total correct answers.", color=EMBED_COLOR_TRIVIA)
        
        lines = []
        for i, (user_id, score) in enumerate(sorted_scores[:LEADERBOARD_LIMIT]):
            rank_emoji = {0: "🥇", 1: "🥈", 2: "🥉"}.get(i, f"**#{i+1}**")
            lines.append(f"{rank_emoji} <@{user_id}>: `{score}` point(s)")
        
        embed.description = "\n".join(lines)
        embed.set_footer(text="Scores reset on the 1st of each month.")
        await interaction.followup.send(embed=embed)

    @trivia.command(name="firstsboard", description="Shows the monthly leaderboard for fastest correct answers.")
    async def trivia_firstsboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        cfg = self.get_guild_config(interaction.guild_id)
        scores = cfg.get("monthly_firsts", {})

        if not scores:
            embed = discord.Embed(description="The monthly firsts board is empty! Be the first to get a fastest answer.", color=EMBED_COLOR_TRIVIA)
            await interaction.followup.send(embed=embed)
            return

        sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        
        month_name = datetime.now(pytz.timezone(cfg.get("timezone", "UTC"))).strftime("%B")
        embed = discord.Embed(title=f"🏆 Monthly Firsts Board: {month_name}", description="Top players by fastest correct answers.", color=0xFFD700)
        
        lines = []
        for i, (user_id, score) in enumerate(sorted_scores[:LEADERBOARD_LIMIT]):
            rank_emoji = {0: "🥇", 1: "🥈", 2: "🥉"}.get(i, f"**#{i+1}**")
            lines.append(f"{rank_emoji} <@{user_id}>: `{score}` point(s)")
        
        embed.description = "\n".join(lines)
        embed.set_footer(text="Scores reset on the 1st of each month. Winner may be decided by a tiebreaker.")
        await interaction.followup.send(embed=embed)


    @trivia.command(name="lastquestion", description="Shows the results of the last trivia question.")
    async def lastquestion(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        cfg = self.get_guild_config(interaction.guild_id)
        last_question_data = cfg.get("last_question_data")
        
        if not last_question_data:
            await interaction.followup.send("There is no previous question data to show.", ephemeral=True)
            return
        
        winner_ids = last_question_data.get("winners", [])
        all_answers_dict = last_question_data.get("all_answers", {})
        reconstructed_embed = discord.Embed(title="🏆 Last Trivia Results", description=f"**Question:** {last_question_data['question']}", color=discord.Color.gold())
        if winner_ids:
            try:
                winner_user = await self.bot.fetch_user(winner_ids[0])
                reconstructed_embed.set_thumbnail(url=winner_user.display_avatar.url)
            except (discord.NotFound, discord.Forbidden): pass
        reconstructed_embed.add_field(name="Correct Answer", value=f"**`{last_question_data['answer']}`**", inline=False)
        answer_counts = Counter(all_answers_dict.values())
        stats_value = ""
        for answer, count in answer_counts.items(): stats_value += f"`{answer}`: {count} vote(s)\n"
        if stats_value: reconstructed_embed.add_field(name="📊 Vote Distribution", value=stats_value, inline=False)
        if not winner_ids:
            reconstructed_embed.add_field(name="🎉 Winners", value="No one got the correct answer this time!", inline=False)
        else:
            reconstructed_embed.add_field(name="🥇 Fastest Correct Answer", value=f"<@{winner_ids[0]}>", inline=False)
            other_winners = winner_ids[1:]
            if other_winners:
                mentions = ", ".join(f"<@{uid}>" for uid in other_winners)
                reconstructed_embed.add_field(name="Other Correct Answers", value=mentions, inline=False)
        
        search_term = urllib.parse.quote_plus(last_question_data['answer'])
        wiki_url = f"https://en.wikipedia.org/w/index.php?search={search_term}"
        reconstructed_embed.add_field(name="Learn More", value=f"[Search for '{last_question_data['answer']}' on Wikipedia]({wiki_url})", inline=False)
        reconstructed_embed.set_footer(text="Daily Trivia")
        
        await interaction.followup.send(embed=reconstructed_embed)

    @trivia.command(name="stats", description="Shows your personal trivia statistics or those of another user.")
    @app_commands.describe(user="The user whose stats you want to see (optional).")
    async def trivia_stats(self, interaction: discord.Interaction, user: discord.Member = None):
        target_user = user or interaction.user
        await interaction.response.defer(ephemeral=True)
        
        cfg = self.get_guild_config(interaction.guild_id)
        interactions = cfg.get("daily_interactions", [])
        
        correct_answers, nemesis_counter, ally_counter = 0, Counter(), Counter()

        for event in interactions:
            all_winners = event.get("all_winners", [])
            if target_user.id in all_winners:
                correct_answers += 1
                for winner_id in all_winners:
                    if winner_id != target_user.id:
                        ally_counter[winner_id] += 1
                first_winner = event.get("first_winner")
                if first_winner and first_winner != target_user.id:
                    nemesis_counter[first_winner] += 1
        
        if correct_answers == 0:
            embed = discord.Embed(description=f"{target_user.mention} has not answered any trivia questions correctly yet.", color=discord.Color.yellow())
            return await interaction.followup.send(embed=embed)

        embed = discord.Embed(title=f"📊 Trivia Stats for {target_user.display_name}", color=EMBED_COLOR_TRIVIA)
        embed.set_thumbnail(url=target_user.display_avatar.url)
        embed.add_field(name="Correct Answers", value=f"`{correct_answers}`", inline=True)
        
        if nemesis_counter:
            nemesis_id, _ = nemesis_counter.most_common(1)[0]
            embed.add_field(name="Nemesis ⚔️", value=f"<@{nemesis_id}>", inline=True)
        else:
            embed.add_field(name="Nemesis ⚔️", value="None", inline=True)
            
        if ally_counter:
            ally_id, _ = ally_counter.most_common(1)[0]
            embed.add_field(name="Ally 🤝", value=f"<@{ally_id}>", inline=True)
        else:
            embed.add_field(name="Ally 🤝", value="None", inline=True)
        
        embed.set_footer(text=f"Stats based on the last {len(interactions)} questions.")
        await interaction.followup.send(embed=embed)

    # --- NEW: Admin Command Group ---
    @trivia_admin.command(name="panel", description="Opens the trivia admin control panel.")
    @app_commands.check(is_trivia_admin)
    async def admin_panel(self, interaction: discord.Interaction):
        cfg = self.get_guild_config(interaction.guild_id)
        embed = discord.Embed(title="⚙️ Trivia Admin Panel", description="Manage the daily trivia game for this server.", color=EMBED_COLOR_TRIVIA)
        
        channel_mention = f"<#{cfg['channel_id']}>" if cfg.get('channel_id') else "Not Set"
        admin_role_mention = f"<@&{cfg['admin_role_id']}>" if cfg.get('admin_role_id') else "Not Set"

        embed.add_field(name="Status", value="Enabled" if cfg.get('enabled') else "Disabled", inline=True)
        embed.add_field(name="Channel", value=channel_mention, inline=True)
        embed.add_field(name="Post Time", value=f"`{cfg.get('time', '12:00')}` (`{cfg.get('timezone', 'UTC')}`)", inline=True)
        embed.add_field(name="Admin Role", value=admin_role_mention, inline=False)

        view = AdminPanelView(self, interaction.user)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(DailyTrivia(bot))
