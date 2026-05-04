import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
import html
import random
import copy
import os
import json
import logging
import typing
import io
from pathlib import Path

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# =====================================================================================
# UTILS & CONSTANTS
# =====================================================================================

log_trivia = logging.getLogger("discord.trivia")

# --- ROBUST FILE PATH ---
# Gets the directory of this cog file (Vibey/cogs/)
_cog_dir = os.path.dirname(os.path.abspath(__file__))
# Joins it with '..' (parent folder) and the filename
CONFIG_FILE_TRIVIA = os.path.join(_cog_dir, '..', 'trivia_config.json')
TRIVIA_RECAP_TEMPLATE_PATH = Path(_cog_dir) / "templates" / "trivia_recap_card.html"

# =====================================================================================
# IMAGE GENERATOR
# =====================================================================================

class TriviaImageGenerator:
    """Generates trivia recap card images using Playwright and HTML templates."""

    def __init__(self):
        self.browser = None
        self.playwright = None

    async def initialize(self):
        if not PLAYWRIGHT_AVAILABLE:
            log_trivia.warning("Playwright not available. Recap cards will fall back to embed.")
            return False
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch()
            log_trivia.info("Trivia Image Generator initialized.")
            return True
        except Exception as e:
            log_trivia.error(f"Failed to initialize trivia image generator: {e}")
            return False

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def generate_recap_image(self, data: dict) -> typing.Optional[io.BytesIO]:
        if not self.browser:
            return None

        try:
            if not TRIVIA_RECAP_TEMPLATE_PATH.exists():
                log_trivia.error(f"Recap template not found at {TRIVIA_RECAP_TEMPLATE_PATH}")
                return None

            template = TRIVIA_RECAP_TEMPLATE_PATH.read_text(encoding='utf-8')

            html = template.format(**data)

            page = await self.browser.new_page(
                viewport={'width': 1080, 'height': data['dynamic_height']},
                device_scale_factor=2
            )
            await page.set_content(html)
            await page.wait_for_timeout(100)

            screenshot = await page.screenshot(type='png')
            await page.close()

            return io.BytesIO(screenshot)

        except Exception as e:
            log_trivia.error(f"Error generating recap card: {e}")
            return None

# This will now *always* correctly point to /home/tmthy/Vibey/trivia_config.json
# ---

TRIVIA_API_URL_BASE = "https://opentdb.com/api.php"
CACHE_FETCH_AMOUNT = 50
EMBED_COLOR_TRIVIA = 0x1ABC9C
CACHE_MIN_SIZE = 10
LEADERBOARD_LIMIT = 20
EPHEMERAL_QUESTION_TIMEOUT = 20.0
TRIVIA_TIMEZONE = ZoneInfo("America/New_York")
POST_TIMES = [time(0, 0), time(12, 0), time(18, 0)]
RESET_HOUR = 0
STREAK_BONUS_MILESTONE = 5

PRESTIGE_RANKS = [
    (0, "Newcomer"), (10, "Novice"), (25, "Apprentice"), (50, "Contender"),
    (100, "Scholar"), (200, "Adept"), (350, "Expert"), (500, "Veteran"),
    (750, "Master"), (1000, "Grandmaster"), (1500, "Champion"), (2250, "Prodigy"),
    (3500, "Oracle"), (5000, "Luminary"), (7000, "Historian"), (10000, "Legend"),
    (15000, "Mythic"), (22000, "Titan"), (30000, "Demigod"), (50000, "The Ascended")
]

ANTI_CHEAT_QUESTIONS = [
    { "question": "In the original 1983 C++ specification, what was the keyword for a destructor?", "answers": ["~", "delete", "destroy", "finalize", "dispose", "free"], "correct": "~" },
    { "question": "What is the common name for the species *Psychrolutes marcidus*?", "answers": ["Blobfish", "Fangtooth", "Viperfish", "Anglerfish", "Hagfish", "Frilled Shark"], "correct": "Blobfish" },
    { "question": "The 'Great Emu War' of 1932 was a real-life military operation in which country?", "answers": ["Australia", "South Africa", "New Zealand", "India", "Argentina", "Canada"], "correct": "Australia" },
    { "question": "Which astronomical object, a millisecond pulsar, is nicknamed 'the Black Widow'?", "answers": ["PSR J1959+2048", "PSR B1257+12", "Cygnus X-1", "Sagittarius A*"], "correct": "PSR J1959+2048"}
]

FAQ_TEXT = """
**How does this work?**
A new trivia gateway is posted daily at **midnight EST**. It is then reposted at **12pm and 6pm EST** to keep it visible. Click 'Play Trivia' to get a private, timed question.

**Scoring & Streaks:**
- **Correct Answer:** +1 point
- **Incorrect Answer:** 0 points
- **Answer Streak:** Earn a **+1 bonus point** for every 5 consecutive daily questions answered correctly!
- **Double or Nothing:** After a correct daily answer, you'll be offered a challenge. Win for **+1 point**, lose and **lose the point** you just earned!

**Leaderboard & Ranks:**
- Use `/trivia leaderboard` to see both monthly and all-time rankings.
- Use `/trivia stats` to see your personal stats and rank progress!
- If the trivia message ever seems stuck, use `/trivia bump` to fix it!
- The 1st place winner at the end of the month receives a special role!
- Scores reset on the first day of each month, but your all-time score and rank never reset.
"""

class TriviaPostingError(Exception):
    pass

def load_config_trivia():
    if os.path.exists(CONFIG_FILE_TRIVIA):
        try:
            with open(CONFIG_FILE_TRIVIA, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log_trivia.error(f"Error loading trivia config: {e}")
    # Return structure with new global keys
    return {"global_data": {}, "guild_settings": {}}

def save_config_trivia(config):
    try:
        with open(CONFIG_FILE_TRIVIA, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)
    except IOError as e: log_trivia.error(f"Error saving trivia config: {e}")

async def is_trivia_admin_check(interaction: discord.Interaction) -> bool:
    cog = interaction.client.get_cog("DailyTrivia")
    if not cog: return await interaction.client.is_owner(interaction.user)
    return await cog.is_user_admin(interaction)

# =====================================================================================
# UI VIEWS (PLAYER-FACING)
# =====================================================================================

class DailyGatewayView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia"):
        super().__init__(timeout=None)
        self.cog = cog
        
    @discord.ui.button(label="▶️ Play Trivia", style=discord.ButtonStyle.success, custom_id="play_daily_trivia_persistent")
    async def play_trivia(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if await self.cog._is_user_blocked(interaction.user.id):
                return await interaction.response.send_message("You are currently blocked from participating.", ephemeral=True)
            async with self.cog.config_lock:
                is_flagged = str(interaction.user.id) in self.cog.get_global_data().get("cheater_test_users", {})
            if is_flagged: return await self.cog._start_cheater_test(interaction)
            
            view = PlayTriviaView(self.cog)
            log_trivia.debug(f"Attempting to send PlayTriviaView to {interaction.user.id}")
            msg = f"Click below to get your private question!\n\n⚠️ **You only have {int(EPHEMERAL_QUESTION_TIMEOUT)} seconds to answer.**"
            await interaction.response.send_message(msg, view=view, ephemeral=True)
            try:
                view.message = await interaction.original_response()
            except discord.HTTPException:
                pass
        except discord.HTTPException as e:
            log_trivia.warning(f"Failed to send trivia prompt to {interaction.user} ({interaction.user.id}): {e}")

    # Fix: Daily Recap button was missing from the persistent gateway view
    @discord.ui.button(label="📊 Daily Recap", style=discord.ButtonStyle.secondary, custom_id="view_yesterdays_results_persistent")
    async def view_yesterdays_results(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with self.cog.config_lock:
                # Get a copy of global data for the embed builder (run in executor to avoid blocking event loop)
                full_global_data_copy = await self.cog.bot.loop.run_in_executor(None, copy.deepcopy, self.cog.get_global_data())
                recap_data = full_global_data_copy.get("yesterdays_recap_data")

            if recap_data and recap_data.get("daily_question"):
                if PLAYWRIGHT_AVAILABLE and self.cog.image_generator.browser:
                    recap_data_dict = await self.cog.build_recap_image_data(interaction.guild, recap_data, full_global_data_copy)
                    image_buffer = await self.cog.image_generator.generate_recap_image(recap_data_dict)
                    if image_buffer:
                        file = discord.File(fp=image_buffer, filename="recap.png")
                        await interaction.followup.send(
                            file=file,
                            view=RecapView(self.cog), 
                            ephemeral=True
                        )
                        return
                
                # Fallback
                await interaction.followup.send(
                    embed=await self.cog.build_daily_awards_embed(interaction.guild, recap_data, full_global_data_copy), 
                    view=RecapView(self.cog), 
                    ephemeral=True
                )
            else:
                await interaction.followup.send("Yesterday's results are not available yet.", ephemeral=True)
        except discord.HTTPException as e:
            log_trivia.warning(f"Failed to send recap to {interaction.user} ({interaction.user.id}): {e}")

class PlayTriviaView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia"):
        super().__init__(timeout=60)
        self.cog = cog
        self.message = None

    @discord.ui.button(label="✅ Reveal Question", style=discord.ButtonStyle.primary)
    async def reveal_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        user_id_str = str(interaction.user.id)
        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            if any(d.get("user_id") == user_id_str for d in global_data.get("daily_interactions", [])):
                return await interaction.followup.send("You have already played today's question!", ephemeral=True)
            q_data = global_data.get("daily_question_data")
        if not q_data: return await interaction.followup.send("The daily question data is missing. Please contact an admin.", ephemeral=True)

        self.cog.reveal_timestamps[(interaction.guild.id, interaction.user.id)] = datetime.now(timezone.utc)

        answers = q_data.get("answers", [])
        if not answers:
            log_trivia.error(f"Empty answers list for daily question data: {q_data}")
            return await interaction.followup.send("Error: Question data is invalid (no answers). Please contact an admin.", ephemeral=True)

        shuffled = random.sample(answers, len(answers))
        try:
            correct_index = shuffled.index(q_data.get("correct"))
        except (ValueError, TypeError):
             return await interaction.followup.send("The daily question data is corrupted. Please contact an admin.", ephemeral=True)

        embed = discord.Embed(description=f"**{q_data.get('question','')}**", color=discord.Color.blurple())
        embed.set_footer(text=f"You have {int(EPHEMERAL_QUESTION_TIMEOUT)} seconds.")
        view = EphemeralQuestionView(self.cog, shuffled, correct_index, interaction.guild.id, interaction.user.id)

        try:
            msg = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = msg
        except discord.HTTPException as e:
            log_trivia.error(f"Failed to send question to {interaction.user.id}: {e}")
            self.cog.reveal_timestamps.pop((interaction.guild.id, interaction.user.id), None)
            try:
                await interaction.followup.send("Failed to load your question. Please try clicking **Play Trivia** again.", ephemeral=True)
            except discord.HTTPException:
                pass

    async def on_timeout(self):
        if self.message:
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content="⏱️ This prompt has expired. Click **Play Trivia** again to start.", view=self)
            except discord.HTTPException: pass
        
class EphemeralQuestionView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia", answers: list[str], correct_index: int, guild_id: int, user_id: int):
        super().__init__(timeout=EPHEMERAL_QUESTION_TIMEOUT)
        self.cog, self.correct_index, self.answered, self.message = cog, correct_index, False, None
        self.guild_id, self.user_id = guild_id, user_id

        unique_id_suffix = f"_{user_id}_{int(datetime.now(timezone.utc).timestamp())}"
        shuffled = answers

        for idx, ans in enumerate(shuffled):
            btn = discord.ui.Button(label=ans[:80], style=discord.ButtonStyle.secondary, custom_id=f"trivia_{idx}_{unique_id_suffix}")
            btn.callback = self.answer_callback
            self.add_item(btn)

        if len(self.children) != len(answers):
            log_trivia.error(f"Button mismatch in EphemeralQuestionView: {len(self.children)} vs {len(answers)}")

    async def answer_callback(self, interaction: discord.Interaction):
        # Defer IMMEDIATELY to guarantee Discord gets a response, even if processing is slow
        try:
            await interaction.response.defer()
        except discord.HTTPException as e:
            log_trivia.warning(f"answer_callback: defer failed for {interaction.user.id}: {e}")
            return  # Interaction expired (e.g., timeout race) — nothing we can do

        if self.answered:
            try: await interaction.followup.send("You have already answered this question.", ephemeral=True, delete_after=5)
            except discord.HTTPException: pass
            return
        self.answered = True
        try:
            await self.cog.handle_trivia_answer(interaction, self)
        except Exception as e:
            log_trivia.error(f"answer_callback: handle_trivia_answer crashed for {interaction.user.id}: {e}", exc_info=True)
            try: await interaction.followup.send("Something went wrong processing your answer. Please report this.", ephemeral=True)
            except discord.HTTPException: pass
        self.stop()

    async def on_timeout(self):
        # Mark as answered FIRST to prevent any in-flight answer_callback from processing
        self.answered = True
        self.cog.reveal_timestamps.pop((self.guild_id, self.user_id), None)
        if self.message:
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content="⌛ Time's up!", view=self, embed=None)
            except discord.HTTPException: pass

# --- START Replacement for DoubleOrNothingPromptView ---
class DoubleOrNothingPromptView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia", user: discord.User, points_won: int, don_category: str):
        super().__init__(timeout=30.0)
        self.cog, self.user, self.points_won, self.don_category, self.message = cog, user, points_won, don_category, None
        
    async def start(self, interaction: discord.Interaction):
        pts = "point" if self.points_won == 1 else "points"
        
        # Create embed with the proposition
        embed = discord.Embed(
            title="🎲 Double or Nothing Challenge!",
            description=(
                f"Want to risk the **{self.points_won} {pts}** you just won for a chance at **1 more point**?\n\n"
                f"**Category:** {self.don_category}\n\n"
                f"**Rules:**\n"
                f"• Correct answer = **+1 pt**\n"
                f"• Wrong answer = **-{self.points_won} {pts}**\n"
                f"• You'll have **{int(EPHEMERAL_QUESTION_TIMEOUT)} seconds** to answer"
            ),
            color=discord.Color.gold()
        )
        embed.set_footer(text="⚠️ Choose wisely! This cannot be undone.")
        
        # The message will be a new ephemeral message sent via followup.send
        self.message = await interaction.followup.send(embed=embed, view=self, ephemeral=True)
        
    @discord.ui.button(label="✅ Accept Challenge", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Defer IMMEDIATELY to guarantee Discord gets a response
        try:
            await interaction.response.defer()
        except discord.HTTPException as e:
            log_trivia.warning(f"DoN accept: defer failed for {interaction.user.id}: {e}")
            return
        
        # --- Data Prep ---
        async with self.cog.config_lock:
            self.cog.get_user_stats(interaction.user.id)["don_accepted"] += 1
            
            # --- BUG FIX: Deduct points IMMEDIATELY when accepted ---
            # This prevents users from ghosting the second question to keep points
            global_data = self.cog.get_global_data()
            score_data = global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})
            stats = self.cog.get_user_stats(interaction.user.id)
            
            # Deduct the points they are risking right now
            # NOTE: Do NOT update timestamp when losing points - timestamp only updates when gaining
            score_data["score"] -= self.points_won
            stats["all_time_score"] -= self.points_won
            # Do NOT update all_time_timestamp when losing points
            
            self.cog.config_is_dirty = True
            q_data = global_data.get("daily_don_question_data")
        
        # --- Validation: Question Data ---
        if not q_data: 
            # Refund if error occurs
            async with self.cog.config_lock:
                self.cog.get_user_stats(interaction.user.id)["don_accepted"] -= 1
                stats = self.cog.get_user_stats(interaction.user.id)
                global_data = self.cog.get_global_data()
                stats["all_time_score"] += self.points_won
                global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})["score"] += self.points_won
                self.cog.config_is_dirty = True
            return await interaction.followup.send(content="❌ Could not get challenge question. Acceptance canceled and points refunded.", ephemeral=True)
        
        answers = q_data.get("answers", [])
        if not answers or len(answers) != 4:
            log_trivia.error(f"DoN question invalid: {len(answers)} answers for user {interaction.user.id}")
            async with self.cog.config_lock:
                # Refund if error occurs
                self.cog.get_user_stats(interaction.user.id)["don_accepted"] -= 1
                self.cog.get_user_stats(interaction.user.id)["all_time_score"] += self.points_won
                global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})["score"] += self.points_won
                self.cog.config_is_dirty = True
                
            return await interaction.followup.send(
                content="❌ Challenge question is invalid. Your acceptance has been canceled and points returned!", 
                ephemeral=True
            )

        # --- View Creation and Check ---
        view = DoubleOrNothingQuestionView(self.cog, q_data, self.points_won, interaction.guild.id, interaction.user.id)
        
        # 4 answer buttons + 1 report button = 5 children
        if len(view.children) != 5:
            log_trivia.error(f"DoN View failed: {len(view.children)} buttons for user {interaction.user.id}")
            async with self.cog.config_lock:
                # Refund if error occurs
                self.cog.get_user_stats(interaction.user.id)["don_accepted"] -= 1
                self.cog.get_user_stats(interaction.user.id)["all_time_score"] += self.points_won
                global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})["score"] += self.points_won
                self.cog.config_is_dirty = True
            return await interaction.followup.send(
                content="❌ Failed to create buttons. Your acceptance has been canceled and points returned!", 
                ephemeral=True
            )
        
        # --- Build question embed ---
        question_embed = discord.Embed(
            title="Double or Nothing!", 
            description=f"**{q_data.get('question','')}**", 
            color=discord.Color.gold()
        )
        question_embed.set_footer(text=f"⚠️ If you don't see all answer buttons, click the red bug button! • {int(EPHEMERAL_QUESTION_TIMEOUT)}s to answer")
        
        # --- Send question as NEW ephemeral message via followup.send ---
        try:
            # We must use followup.send here to send a *new* ephemeral message.
            msg = await interaction.followup.send(embed=question_embed, view=view, ephemeral=True)
            view.message = msg # Set the message reference on the question view
            self.cog.don_reveal_timestamps[(interaction.guild.id, interaction.user.id)] = datetime.now(timezone.utc)
            log_trivia.info(f"DoN question sent to user {interaction.user.id} with {len(view.children)} buttons")
            
            # Since the original interaction was the PROMPT message, we must edit it 
            # to remove the accept/decline buttons so the user can only interact with the new question.
            try:
                await self.message.edit(content="Challenge accepted! See your question above.", embed=None, view=None)
            except discord.HTTPException:
                pass
            
            # Update gateway message to reflect the temporary score drop
            await self.cog.update_gateway_message(interaction.guild.id)

        except discord.HTTPException as e:
            log_trivia.error(f"Failed to send DoN question as new ephemeral message: {e}")
            # --- FIX: COMPLETE REFUND AND NOTIFICATION LOGIC ---
            async with self.cog.config_lock:
                self.cog.get_user_stats(interaction.user.id)["don_accepted"] -= 1
                stats = self.cog.get_user_stats(interaction.user.id)
                global_data = self.cog.get_global_data()
                stats["all_time_score"] += self.points_won
                global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})["score"] += self.points_won
                self.cog.config_is_dirty = True
            
            # Update gateway to reflect refund
            await self.cog.update_gateway_message(interaction.guild.id)
            
            # Notify user
            try:
                await interaction.followup.send("❌ Failed to send challenge question due to a technical error. Your points have been refunded.", ephemeral=True)
            except discord.HTTPException:
                pass
            
            self.stop()  # Clean up the view
            return
        
        self.stop()
        
    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            self.cog.get_user_stats(interaction.user.id)["don_declined"] += 1
            self.cog.config_is_dirty = True
        # Edit the original prompt message
        await interaction.response.edit_message(content="✅ Challenge declined. Your points are safe!", embed=None, view=None)
        self.stop()
        
    async def on_timeout(self):
        if self.message:
            try:
                async with self.cog.config_lock:
                    stats = self.cog.get_user_stats(self.user.id)
                    stats["don_declined"] += 1
                    self.cog.config_is_dirty = True
                for item in self.children: item.disabled = True
                await self.message.edit(content="⏱️ Challenge offer expired. Your points are safe!", embed=None, view=self)
            except discord.HTTPException: pass
# --- END Replacement for DoubleOrNothingPromptView ---


class DoubleOrNothingQuestionView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia", question_data: dict, points_risked: int, guild_id: int, user_id: int):
        super().__init__(timeout=EPHEMERAL_QUESTION_TIMEOUT)
        self.cog, self.points_risked, self.correct_answer, self.answered, self.message = cog, points_risked, question_data.get("correct"), False, None
        self.guild_id, self.user_id = guild_id, user_id
        
        answers = question_data.get("answers", [])
        
        # Comprehensive logging
        log_trivia.info(f"DoN View init: user={user_id}, answers_count={len(answers)}, question='{question_data.get('question', '')[:50]}...'")

        # CRITICAL: Validate we have exactly 4 answers
        if len(answers) != 4:
            log_trivia.error(f"DoN CRITICAL: Expected 4 answers, got {len(answers)} for user {user_id}. Answers: {answers}")
            # Don't create buttons - parent will check len(self.children)
            return
        
        shuffled = random.sample(answers, len(answers))
        unique_id_suffix = f"_{user_id}_{int(datetime.now(timezone.utc).timestamp())}"

        # Add answer buttons (4)
        for idx, ans in enumerate(shuffled):
            btn = discord.ui.Button(
                label=ans[:80], 
                style=discord.ButtonStyle.secondary, 
                custom_id=f"don_{idx}_{unique_id_suffix}",
                row=idx // 2  # Rows 0-1 for answers
            )
            btn.full_text = ans
            btn.callback = self.answer_callback
            self.add_item(btn)
        
        # Add report button on row 2
        report_btn = discord.ui.Button(
            label="🐛 Missing Buttons? Click Here",
            style=discord.ButtonStyle.danger,
            custom_id=f"don_report_{unique_id_suffix}",
            row=2
        )
        report_btn.callback = self.report_issue_callback
        self.add_item(report_btn)
        
        # Final validation logging
        buttons_created = len(self.children)
        log_trivia.info(f"DoN View complete: {buttons_created} total items created for user {user_id}")
        
        if buttons_created != 5: # 4 answers + 1 report button
            log_trivia.error(f"DoN MISMATCH: Created {buttons_created} items but expected 5!")

    async def answer_callback(self, interaction: discord.Interaction):
        # Defer IMMEDIATELY to guarantee Discord gets a response, even if processing is slow
        try:
            await interaction.response.defer()
        except discord.HTTPException as e:
            log_trivia.warning(f"DoN answer_callback: defer failed for {interaction.user.id}: {e}")
            return  # Interaction expired (e.g., timeout race) — nothing we can do

        if self.answered:
            try: await interaction.followup.send("You have already answered this question.", ephemeral=True, delete_after=5)
            except discord.HTTPException: pass
            return
        self.answered = True

        answer_time = datetime.now(timezone.utc)
        chosen_button: discord.ui.Button = discord.utils.get(self.children, custom_id=interaction.data['custom_id'])

        # Check if chosen_button is None (should not happen with proper custom IDs, but good safeguard)
        if not chosen_button:
            log_trivia.error(f"Answer callback received but button not found for custom_id: {interaction.data['custom_id']}")
            return

        is_correct = (getattr(chosen_button, 'full_text', None) == self.correct_answer)

        # --- BUG FIX: Points logic update ---
        # If CORRECT: Add back (points_risked) + win bonus (1) = points_risked + 1
        # If INCORRECT: Do nothing (points were already deducted on accept)
        pts_change = (self.points_risked + 1) if is_correct else 0

        async with self.cog.config_lock:
            global_data = self.cog.get_global_data()
            stats = self.cog.get_user_stats(interaction.user.id)
            stats["don_successes"] += 1 if is_correct else 0
            stats["all_time_score"] = stats.get("all_time_score", 0) + pts_change
            # FIX: Update all_time_timestamp ONLY when gaining points (correct answer)
            if is_correct:
                stats["all_time_timestamp"] = answer_time.isoformat()
            
            score_data = global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})
            score_data["score"] = score_data.get("score", 0) + pts_change
            # Only update timestamp when gaining points
            if is_correct:
                score_data["timestamp"] = answer_time.isoformat()
            new_score = score_data["score"]

            if is_correct:
                don_reveal_ts = self.cog.don_reveal_timestamps.pop((interaction.guild.id, interaction.user.id), None)
                if don_reveal_ts is not None:
                    delta = (answer_time - don_reveal_ts).total_seconds()
                    global_data.setdefault("daily_don_answer_times", []).append({"user_id": str(interaction.user.id), "time": delta})
                else:
                    log_trivia.warning(f"Missing DoN reveal timestamp for user {interaction.user.id} (likely bot restart); skipping answer-time recording.")
            
            self.cog.config_is_dirty = True
        
        for item in self.children:
            item.disabled = True
            # Only style the answer buttons, skip the report button
            if not item.custom_id.startswith("don_report_"):
                if getattr(item, 'full_text', None) == self.correct_answer: item.style = discord.ButtonStyle.success
                elif item == chosen_button: item.style = discord.ButtonStyle.danger
        
        msg = f"🎉 **Correct!** You won an extra point!" if is_correct else f"💥 **Incorrect!** You lost {self.points_risked} points."
        await interaction.edit_original_response(content=f"{msg}\nYour new score is **{new_score}**.", view=self, embed=None)
        await self.cog.update_gateway_message(interaction.guild.id)
        self.stop()
        
    # Bug Report Callback
    async def report_issue_callback(self, interaction: discord.Interaction):
        """Handle bug reports from users"""
        # Defer IMMEDIATELY to guarantee Discord gets a response
        try:
            await interaction.response.defer(ephemeral=True)
        except discord.HTTPException as e:
            log_trivia.warning(f"DoN report_issue_callback: defer failed for {interaction.user.id}: {e}")
            return

        if self.answered:
            try: await interaction.followup.send("This question has already been handled (answered or reported).", ephemeral=True, delete_after=5)
            except discord.HTTPException: pass
            return
        # Mark as answered/handled immediately
        self.answered = True 
        
        cfg_settings = self.cog.get_guild_settings(interaction.guild.id)
        report_channel_id = cfg_settings.get("anti_cheat_results_channel_id")
        
        if report_channel_id:
            try:
                channel = self.cog.bot.get_channel(report_channel_id) or await self.cog.bot.fetch_channel(report_channel_id)
                embed = discord.Embed(
                    title="🐛 DoN Button Bug Report",
                    color=0xFF6B6B,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_author(
                    name=f"{interaction.user} ({interaction.user.id})",
                    icon_url=interaction.user.display_avatar.url
                )
                embed.add_field(name="Issue", value="User reported missing buttons on Double or Nothing", inline=False)
                embed.add_field(name="Expected Buttons", value="4 Answers + 1 Report", inline=True)
                embed.add_field(name="Guild", value=f"{interaction.guild.name} ({interaction.guild.id})", inline=False)
                
                await channel.send(embed=embed)
            except discord.HTTPException as e:
                log_trivia.error(f"Failed to send bug report: {e}")
        
        # Log it
        log_trivia.warning(f"DoN bug report from user {interaction.user.id} in guild {interaction.guild.id}")
        
        # Cancel the question and refund
        async with self.cog.config_lock:
            stats = self.cog.get_user_stats(interaction.user.id)
            stats["don_accepted"] -= 1
            
            # --- BUG FIX: REFUND POINTS ON REPORT ---
            global_data = self.cog.get_global_data()
            stats["all_time_score"] += self.points_risked
            global_data.setdefault("scores", {}).setdefault(str(interaction.user.id), {"score": 0, "timestamp": None})["score"] += self.points_risked
            # ----------------------------------------
            
            self.cog.config_is_dirty = True
        
        # Thank the user
        await interaction.followup.send(
            "✅ Thank you for reporting! This helps us fix the issue.\n\n"
            "The question has been canceled and your acceptance doesn't count against you. Feel free to try the daily question again tomorrow!",
            ephemeral=True
        )
        
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        try:
            # Edit the question message (this is the original response for this view)
            await interaction.message.edit(
                content="❌ Question canceled due to reported issue.",
                view=self,
                embed=None
            )
        except discord.HTTPException:
            pass
        
        self.stop()

    async def on_timeout(self):
        """If user times out without answering, they might have had missing buttons"""
        self.cog.don_reveal_timestamps.pop((self.guild_id, self.user_id), None)
        if self.message and not self.answered:
            # Auto-report potential issue
            log_trivia.warning(f"DoN timeout for user {self.user_id} - possible button rendering issue")
            
            try:
                for item in self.children: 
                    item.disabled = True
                
                # Update the message to indicate a LOSS (not a refund), unless they clicked report
                await self.message.edit(
                    content=(
                        "⌛ Time's up on the challenge!\n\n"
                        "You have **lost the wagered points** because no answer was selected in time."
                        "\n\n💡 **Didn't see buttons?** Please use the red **Bug Report** button next time to get a refund."
                    ),
                    view=self,
                    embed=None
                )
                # Ensure acceptance is reset on timeout
                async with self.cog.config_lock:
                    # --- FIX: Timeout is a loss, do not refund points ---
                    stats = self.cog.get_user_stats(self.user_id)
                    # We do NOT refund points here because we deducted them on accept. 
                    # Timeout = Loss. We also do NOT decrement don_accepted, because they accepted.
                    self.cog.config_is_dirty = True
            except discord.HTTPException: 
                pass

class LeaderboardView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia"):
        super().__init__(timeout=300)
        self.cog = cog
    async def _update_board(self, interaction: discord.Interaction, board_type: str):
        await interaction.response.defer()
        if board_type == 'monthly':
            desc = await self.cog._get_leaderboard_text(interaction.guild)
            embed = discord.Embed(title="📊 Monthly Trivia Leaderboard", description=desc, color=EMBED_COLOR_TRIVIA)
            self.monthly_button.disabled, self.alltime_button.disabled = True, False
            self.monthly_button.style, self.alltime_button.style = discord.ButtonStyle.primary, discord.ButtonStyle.secondary
        else: # all-time
            desc = await self.cog._get_alltime_leaderboard_text(interaction.guild)
            embed = discord.Embed(title="👑 All-Time Trivia Leaderboard", description=desc, color=0xFFD700)
            self.monthly_button.disabled, self.alltime_button.disabled = False, True
            self.monthly_button.style, self.alltime_button.style = discord.ButtonStyle.secondary, discord.ButtonStyle.primary
        await interaction.edit_original_response(embed=embed, view=self)
    @discord.ui.button(label="Monthly", style=discord.ButtonStyle.primary, disabled=True)
    async def monthly_button(self, i: discord.Interaction, b: discord.ui.Button): await self._update_board(i, 'monthly')
    @discord.ui.button(label="All-Time", style=discord.ButtonStyle.secondary)
    async def alltime_button(self, i: discord.Interaction, b: discord.ui.Button): await self._update_board(i, 'alltime')

# MODIFICATION: Added RecapView to show Leaderboard from recap
class RecapView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia"):
        super().__init__(timeout=300) # 5 min timeout
        self.cog = cog
    
    @discord.ui.button(label="Leaderboard", style=discord.ButtonStyle.primary)
    async def show_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        # This is copy-pasted from the /trivia leaderboard command
        await interaction.response.defer(ephemeral=True, thinking=True) # Send a new ephemeral message
        desc = await self.cog._get_leaderboard_text(interaction.guild, limit=LEADERBOARD_LIMIT)
        embed = discord.Embed(title="📊 Monthly Leaderboard", description=desc, color=EMBED_COLOR_TRIVIA)
        await interaction.followup.send(embed=embed, view=LeaderboardView(self.cog), ephemeral=True)
        
        # Disable the button on the original message
        button.disabled = True
        try:
            await interaction.message.edit(view=self)
        except discord.HTTPException:
            pass # Ignore if original message is gone

class EphemeralCheaterTestView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia", answers: list[str], correct_answer: str, question_type: str, guild_id: int, user_id: int):
        super().__init__(timeout=EPHEMERAL_QUESTION_TIMEOUT)
        self.cog, self.correct_answer, self.question_type, self.answered, self.message = cog, correct_answer, question_type, False, None
        self.guild_id, self.user_id = guild_id, user_id
        
        unique_id_suffix = f"_{user_id}_{int(datetime.now(timezone.utc).timestamp())}"

        if not answers:
            log_trivia.error(f"Cheater test question answers list is empty for user {user_id}. Cannot create buttons.")
            shuffled = []
        else:
            shuffled = random.sample(answers, len(answers))
            for idx, ans in enumerate(shuffled):
                btn = discord.ui.Button(label=ans[:80], style=discord.ButtonStyle.secondary, custom_id=f"cheat_{question_type}_{idx}_{unique_id_suffix}")
                btn.full_text = ans
                btn.callback = self.answer_callback
                self.add_item(btn)
        
        # Validation that buttons were added
        if len(self.children) != len(shuffled):
            log_trivia.error(f"Button mismatch in EphemeralCheaterTestView: {len(self.children)} vs {len(shuffled)}")

    async def answer_callback(self, interaction: discord.Interaction):
        # Defer IMMEDIATELY to guarantee Discord gets a response, even if processing is slow
        try:
            await interaction.response.defer()
        except discord.HTTPException as e:
            log_trivia.warning(f"Cheater test answer_callback: defer failed for {interaction.user.id}: {e}")
            return  # Interaction expired (e.g., timeout race) — nothing we can do

        if self.answered:
            try: await interaction.followup.send("You have already answered this question.", ephemeral=True, delete_after=5)
            except discord.HTTPException: pass
            return
        self.answered = True
        await self.cog.handle_cheater_test_answer(interaction, self)
        self.stop()

    async def on_timeout(self):
        self.cog.cheat_test_timestamps.pop((self.guild_id, self.user_id), None)
        if self.message and not self.answered:
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content="⌛ Time's up!", view=self, embed=None)
            except discord.HTTPException: pass

class BonusGatewayView(discord.ui.View):
    def __init__(self, cog: "DailyTrivia", question_data: dict):
        super().__init__(timeout=600.0)
        self.cog, self.question_data, self.played_user_ids, self.fastest_correct_users = cog, question_data, set(), []
        self.reveal_timestamps, self.message, self.update_lock = {}, None, asyncio.Lock()
    async def on_timeout(self):
        if self.message:
            try:
                embed = self.message.embeds[0]
                embed.description, embed.color = "This bonus round has ended.", discord.Color.dark_grey()
                play_button = discord.utils.get(self.children, custom_id="play_bonus_question")
                if play_button: play_button.disabled = True
                await self.message.edit(embed=embed, view=self)
            except (discord.HTTPException, IndexError): pass
        self.stop()
    async def update_leaderboard(self, interaction: discord.Interaction, time_taken: float):
        async with self.update_lock:
            self.fastest_correct_users.append({"user_id": interaction.user.id, "time": time_taken})
            self.fastest_correct_users.sort(key=lambda x: x["time"])
            medals, lines = ["🥇", "🥈", "🥉"], []
            for i, entry in enumerate(self.fastest_correct_users[:3]):
                lines.append(f"{medals[i]} <@{entry['user_id']}> ({entry['time']:.2f}s)")
            if self.message:
                try:
                    new_embed = self.message.embeds[0].copy()
                    new_embed.set_field_at(0, name="🏆 Fastest Correct", value="\n".join(lines), inline=False)
                    await self.message.edit(embed=new_embed)
                except (discord.HTTPException, IndexError): pass
    @discord.ui.button(label="▶️ Play Bonus Question", style=discord.ButtonStyle.primary, custom_id="play_bonus_question")
    async def play_bonus_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.played_user_ids:
            return await interaction.response.send_message("You have already played this bonus question.", ephemeral=True)
        self.played_user_ids.add(interaction.user.id)
        
        # Set timestamp NOW, before the view is sent
        self.reveal_timestamps[interaction.user.id] = datetime.now(timezone.utc) 
        
        answers = self.question_data.get("answers",[])
        if not answers:
            log_trivia.error(f"Bonus question answers list is empty. Cannot start game for user {interaction.user.id}.")
            return await interaction.response.send_message("Error: Bonus question data is invalid (no answers).", ephemeral=True)

        shuffled = random.sample(answers, len(answers))
        embed = discord.Embed(title="Bonus Question!", description=f"**{self.question_data.get('question','')}**", color=discord.Color.blurple())
        embed.set_footer(text=f"You have {int(EPHEMERAL_QUESTION_TIMEOUT)} seconds.")
        view = EphemeralBonusQuestionView(self, shuffled, self.question_data.get("correct"))
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        
        # FIX: For response.send_message (not followup), original_response() is correct
        view.message = await interaction.original_response()

class EphemeralBonusQuestionView(discord.ui.View):
    def __init__(self, gateway_view: BonusGatewayView, answers: list[str], correct_answer: str):
        super().__init__(timeout=EPHEMERAL_QUESTION_TIMEOUT)
        self.gateway_view, self.correct_answer, self.answered, self.message = gateway_view, correct_answer, False, None
        
        # We use the message ID of the public Bonus Gateway message for uniqueness
        gateway_message_id = getattr(gateway_view.message, 'id', '0')
        unique_id_suffix = f"_{gateway_message_id}_{int(datetime.now(timezone.utc).timestamp())}"
        
        for idx, ans in enumerate(answers):
            btn = discord.ui.Button(label=ans[:80], style=discord.ButtonStyle.secondary, custom_id=f"bonus_{idx}_{unique_id_suffix}")
            btn.full_text = ans
            btn.callback = self.answer_callback
            self.add_item(btn)

        # Validation that buttons were added
        if len(self.children) != len(answers):
            log_trivia.error(f"Button mismatch in EphemeralBonusQuestionView: {len(self.children)} vs {len(answers)}")

    async def answer_callback(self, interaction: discord.Interaction):
        # Defer IMMEDIATELY to guarantee Discord gets a response, even if processing is slow
        try:
            await interaction.response.defer()
        except discord.HTTPException as e:
            log_trivia.warning(f"Bonus answer_callback: defer failed for {interaction.user.id}: {e}")
            return  # Interaction expired (e.g., timeout race) — nothing we can do

        if self.answered:
            try: await interaction.followup.send("You have already answered this question.", ephemeral=True, delete_after=5)
            except discord.HTTPException: pass
            return
        self.answered = True
        answer_time = datetime.now(timezone.utc)
        chosen_button: discord.ui.Button = discord.utils.get(self.children, custom_id=interaction.data['custom_id'])

        # Check if chosen_button is None
        if not chosen_button:
            log_trivia.error(f"Bonus Answer callback received but button not found for custom_id: {interaction.data['custom_id']}")
            return

        is_correct = (getattr(chosen_button, 'full_text', '') == self.correct_answer)
        
        for item in self.children:
            item.disabled = True
            if getattr(item, 'full_text', '') == self.correct_answer: item.style = discord.ButtonStyle.success
            elif item == chosen_button: item.style = discord.ButtonStyle.danger
        
        await interaction.edit_original_response(content="✅ Correct!" if is_correct else "❌ Incorrect!", view=self)
        if is_correct:
            bonus_reveal_ts = self.gateway_view.reveal_timestamps.pop(interaction.user.id, None)
            if bonus_reveal_ts is not None:
                delta = (answer_time - bonus_reveal_ts).total_seconds()
                asyncio.create_task(self.gateway_view.update_leaderboard(interaction, delta))
            else:
                log_trivia.warning(f"Missing bonus reveal timestamp for user {interaction.user.id} (likely bot restart); skipping leaderboard update.")
        self.stop()

    async def on_timeout(self):
        if self.message:
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content="⌛ Time's up!", view=self)
            except discord.HTTPException: pass

# =====================================================================================
# MAIN COG CLASS
# =====================================================================================

@app_commands.guild_only()
class DailyTrivia(commands.Cog, name="DailyTrivia"):
    trivia = app_commands.Group(name="trivia", description="Commands for the daily trivia.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_trivia()
        self.session = aiohttp.ClientSession()
        self.config_lock = asyncio.Lock()
        self.config_is_dirty = False
        self.reveal_timestamps, self.don_reveal_timestamps, self.cheat_test_timestamps = {}, {}, {}
        self.image_generator = TriviaImageGenerator()
        self.bot.loop.create_task(self.setup_hook())

    async def setup_hook(self):
        await self.bot.wait_until_ready()
        await self.image_generator.initialize()
        self.bot.add_view(DailyGatewayView(self))

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.trivia_loop.is_running(): self.trivia_loop.start()
        if not self.cache_refill_loop.is_running(): self.cache_refill_loop.start()
        if not self.backup_save_loop.is_running(): self.backup_save_loop.start()
        log_trivia.info(f"DailyTrivia cog is ready. Config path: {CONFIG_FILE_TRIVIA}")

    async def cog_unload(self):
        if self.config_is_dirty: save_config_trivia(self.config)
        self.trivia_loop.cancel(); self.cache_refill_loop.cancel(); self.backup_save_loop.cancel()
        if self.session and not self.session.closed:
            await self.session.close()
        await self.image_generator.close()
    
    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        log_trivia.error(f"Error in command '{interaction.command.name}': {error}", exc_info=True)
        msg = "You don't have permission for this." if isinstance(error, app_commands.CheckFailure) else "An unexpected error occurred."
        try:
            if interaction.response.is_done(): await interaction.followup.send(msg, ephemeral=True)
            else: await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass

    # === Config & Data Management ===
            
    def get_default_guild_settings(self) -> dict:
        # This now ONLY contains guild-specific settings
        return {
            "channel_id": None, "enabled": False, "admin_role_id": None, "last_winner_announcement": None,
            "last_posted_date": None, "gateway_message_id": None,
            "last_post_hour": -1, "yesterdays_recap_data": None, "winner_role_id": None, 
            "last_month_winner_id": None, "anti_cheat_results_channel_id": None
        }

    def get_global_data(self) -> dict:
        # Helper to get the global_data dict, ensuring defaults
        global_data = self.config.setdefault("global_data", {})
        
        # Define default global data structure
        default_global = {
            "scores": {}, "user_stats": {}, "question_cache": [],
            "daily_question_data": None, "daily_don_question_data": None,
            "daily_interactions": [], "daily_don_interactions": [],
            "daily_don_answer_times": [], "daily_answer_times": [],
            "yesterdays_recap_data": None, "blocked_users": [],
            "cheater_test_users": {}
        }
        
        # Ensure all default keys exist
        updated = False
        for key, value in default_global.items():
            if key not in global_data:
                global_data[key] = copy.deepcopy(value)
                updated = True
        if updated: self.config_is_dirty = True
        
        return global_data

    def get_guild_settings(self, guild_id: int) -> dict:
        # This function replaces get_guild_config
        gid = str(guild_id)
        guild_settings_pool = self.config.setdefault("guild_settings", {})
        
        if gid not in guild_settings_pool:
            guild_settings_pool[gid], self.config_is_dirty = self.get_default_guild_settings(), True
        
        guild_cfg = guild_settings_pool[gid]
        default_cfg = self.get_default_guild_settings()
        updated = False
        
        # Ensure all default guild settings keys exist
        for key, value in default_cfg.items():
            if key not in guild_cfg:
                guild_cfg[key], updated = copy.deepcopy(value), True
        if updated: self.config_is_dirty = True
        
        return guild_cfg
    
    def get_user_stats(self, user_id: int) -> dict:
        # This function is now global and doesn't need guild_id
        uid = str(user_id)
        global_data = self.get_global_data()
        
        # FIX: Added all_time_timestamp for tie-breaking
        default_stats = {
            "correct": 0, "incorrect": 0, "current_streak": 0, "longest_streak": 0, 
            "don_declined": 0, "don_accepted": 0, "don_successes": 0, 
            "categories": {}, "all_time_score": 0, "current_incorrect_streak": 0,
            "all_time_timestamp": None,  # NEW: for tie-breaking
            "participation_streak": 0,
            "longest_participation_streak": 0,
            "last_participation_date": None
        }
        user_stats_pool = global_data.setdefault("user_stats", {})
        
        if uid not in user_stats_pool:
            user_stats_pool[uid], self.config_is_dirty = default_stats.copy(), True
            
        user_stats_ref = user_stats_pool[uid]
        updated = False
        
        # Ensure all default stat keys exist
        for key, value in default_stats.items():
            if key not in user_stats_ref:
                user_stats_ref[key], updated = copy.deepcopy(value), True
        if updated: self.config_is_dirty = True
        
        return user_stats_ref

    async def save_config_now(self):
        json_str = None
        async with self.config_lock:
            if self.config_is_dirty:
                # Serialize to JSON string in executor to avoid blocking the event loop.
                # json.dumps produces an immutable string, so no deepcopy needed.
                # (self.config is ~275K and serialization can stall the Pi for 500ms+)
                json_str = await self.bot.loop.run_in_executor(None, lambda: json.dumps(self.config, indent=4))
                self.config_is_dirty = False
        if json_str:
            await self.bot.loop.run_in_executor(None, lambda: Path(CONFIG_FILE_TRIVIA).write_text(json_str, encoding="utf-8"))
            log_trivia.debug("Trivia config saved to disk.")

    async def is_user_admin(self, interaction: discord.Interaction) -> bool:
        if await self.bot.is_owner(interaction.user) or self.bot.is_bot_admin(interaction.user): return True
        cfg_settings = self.get_guild_settings(interaction.guild.id)
        admin_role_id = cfg_settings.get("admin_role_id")
        return admin_role_id and any(role.id == admin_role_id for role in interaction.user.roles)
    
    async def _is_user_blocked(self, user_id: int) -> bool:
        # This is now global
        async with self.config_lock:
            return user_id in self.get_global_data().get("blocked_users", [])

    # === Leaderboard & Formatting ===
            
    async def _get_formatted_name(self, guild: discord.Guild, user_id_str: str) -> str:
        try:
            # Try to get member from guild first (for display_name/nickname)
            member = guild.get_member(int(user_id_str)) or await guild.fetch_member(int(user_id_str))
            return member.display_name
        except (discord.NotFound, discord.Forbidden):
            # Fallback: fetch user globally (not guild-specific)
            try:
                user = self.bot.get_user(int(user_id_str)) or await self.bot.fetch_user(int(user_id_str))
                return user.name
            except (discord.NotFound, discord.Forbidden):
                return f"User ({user_id_str[-4:]})"
            
    def _get_score_sort_key(self, item):
        """Sort key for monthly scores: by score desc, then by timestamp asc (earlier wins ties)"""
        _, data = item
        score = data.get("score", 0) if isinstance(data, dict) else data
        timestamp = datetime.fromisoformat(data["timestamp"]).timestamp() if isinstance(data, dict) and data.get("timestamp") else float('inf')
        return (-score, timestamp)
    
    def _get_alltime_sort_key(self, item):
        """Sort key for all-time scores: by score desc, then by timestamp asc (earlier wins ties)"""
        _, data = item
        score = data.get("all_time_score", 0)
        timestamp = datetime.fromisoformat(data["all_time_timestamp"]).timestamp() if data.get("all_time_timestamp") else float('inf')
        return (-score, timestamp)

    async def _get_leaderboard_text(self, guild: discord.Guild, limit: int = 5) -> str:
        # Gets scores from global data
        global_data = self.get_global_data()
        scores = global_data.get("scores", {})
        if not scores: return "*No scores yet this month!*"
        
        sorted_scores = sorted(scores.items(), key=self._get_score_sort_key)
        top = sorted_scores[:limit]

        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, (uid, dat) in enumerate(top):
            name = await self._get_formatted_name(guild, uid)
            name_formatted = f"***{name[:15]}***" if i < 3 else f"*{name[:15]}*"
            prefix = medals[i] if i < 3 else f'**{i+1}.**'
            lines.append(f"{prefix} {name_formatted} - {dat.get('score', 0)} pts")
        
        return "\n".join(lines)

    async def _get_gateway_leaderboard_text(self, guild: discord.Guild) -> str:
        # Gets scores from global data
        global_data = self.get_global_data()
        scores = global_data.get("scores", {})
        if not scores: return "> *No scores yet this month!*"
        
        sorted_scores = sorted(scores.items(), key=self._get_score_sort_key)
        top = sorted_scores[:5] # Show top 5
        if not top: return "> *No scores yet this month!*" # Check again after slice
        
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, (uid, dat) in enumerate(top):
            name = await self._get_formatted_name(guild, uid)
            
            if i < 3:
                # Top 3: Medal + Bold name
                name_formatted = f"**{name[:15]}**"
                prefix = medals[i]
            else:
                # 4th and 5th: Number + Regular name
                name_formatted = f"{name[:15]}" # Not bold
                prefix = f"{i+1}." # e.g., "4." or "5."

            lines.append(f"> {prefix} {name_formatted} - {dat.get('score', 0)} pts")
        return "\n".join(lines)

    async def _get_alltime_leaderboard_text(self, guild: discord.Guild) -> str:
        # Gets user_stats from global data
        global_data = self.get_global_data()
        all_stats = global_data.get("user_stats", {})
        if not all_stats: return "*No all-time stats recorded yet!*"
        
        # FIX: Use _get_alltime_sort_key for tie-breaking instead of simple score sort
        top = sorted(all_stats.items(), key=self._get_alltime_sort_key)[:LEADERBOARD_LIMIT]
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, (uid, dat) in enumerate(top):
            name = await self._get_formatted_name(guild, uid)
            name_formatted = f"***{name[:15]}***" if i < 3 else f"*{name[:15]}*"
            prefix = medals[i] if i < 3 else f'**{i+1}.**'
            lines.append(f"{prefix} {name_formatted} - {dat.get('all_time_score', 0)} pts")
        return "\n".join(lines)
    
    async def _format_podium_text(self, guild: discord.Guild, data: list, default_text: str, format_type: str = "recap") -> str:
        if not data:
            if format_type == "gateway":
                return "> *Be the first to answer!*"
            else:
                return f"> {default_text}"
        
        sorted_data = sorted(data, key=lambda x: x['time'])
        lines = []
        medals = ["🥇", "🥈", "🥉"]

        if format_type == "gateway":
            limit = 3
            sorted_data = sorted_data[:limit]
            if not sorted_data: return "> *Be the first to answer!*"

            for i, entry in enumerate(sorted_data):
                name = await self._get_formatted_name(guild, entry['user_id'])
                name_formatted = f"**{name[:15]}**"
                lines.append(f"> {medals[i]} {name_formatted} ({entry['time']:.2f}s)")
        
        else: # default to "recap"
            sorted_data = sorted_data[:5] # Keep original limit for recap
            for i, entry in enumerate(sorted_data):
                name = await self._get_formatted_name(guild, entry['user_id'])
                name_formatted = f"***{name[:15]}***" if i < 3 else f"*{name[:15]}*" # Keep original style
                prefix = medals[i] if i < 3 else f"**{i+1}.**" # Keep original style
                lines.append(f"> {prefix} {name_formatted} ({entry['time']:.2f}s)")
        
        return "\n".join(lines)

    def get_prestige_rank(self, score: int) -> tuple[str, int, int, str]:
        current_rank, next_rank_score, next_rank_name = PRESTIGE_RANKS[0][1], PRESTIGE_RANKS[1][0], PRESTIGE_RANKS[1][1]
        for i, (req, name) in enumerate(PRESTIGE_RANKS):
            if score >= req:
                current_rank = name
                if i + 1 < len(PRESTIGE_RANKS):
                    next_rank_score, next_rank_name = PRESTIGE_RANKS[i+1]
                else: 
                    next_rank_score, next_rank_name = req, name
            else: break
        return current_rank, next_rank_score, next_rank_score - score, next_rank_name

    # === Background Tasks & Loops ===

    @tasks.loop(minutes=1)
    async def trivia_loop(self):
        now_est = datetime.now(TRIVIA_TIMEZONE)
        if not any(now_est.hour == t.hour and now_est.minute == t.minute for t in POST_TIMES): return

        guild_settings_pool = self.config.get("guild_settings", {})
        ran_monthly_reset = False
        
        for gid_str, cfg_settings in list(guild_settings_pool.items()):
            if not cfg_settings.get("enabled") or not cfg_settings.get("channel_id"): continue
            
            try:
                guild = self.bot.get_guild(int(gid_str))
                if not guild or not guild.me: 
                    log_trivia.warning(f"Could not find or access guild {gid_str}, skipping trivia loop.")
                    continue
                
                if cfg_settings.get("last_post_hour") == now_est.hour and cfg_settings.get("last_posted_date") == now_est.date().isoformat(): continue
                
                log_trivia.info(f"Met time condition for guild {guild.id} at {now_est.hour}:00 EST.")
                if now_est.hour == RESET_HOUR and cfg_settings.get("last_posted_date") != now_est.date().isoformat():
                    # Check for 1st of month here
                    if now_est.day == 1:
                        await self._process_monthly_winners(guild)
                        ran_monthly_reset = True
                    await self._trigger_daily_reset_and_post(guild)
                elif cfg_settings.get("last_posted_date") == now_est.date().isoformat():
                    await self._bump_messages(guild)

            except TriviaPostingError as e:
                log_trivia.error(f"Failed to post trivia in loop for guild {gid_str}: {e}")
            except Exception as e:
                log_trivia.error(f"Unexpected error in trivia loop for guild {gid_str}: {e}", exc_info=True)

        if ran_monthly_reset:
            async with self.config_lock:
                scores = self.get_global_data().get("scores", {})
                if scores:
                    self.get_global_data()["scores"] = {}
                    self.config_is_dirty = True
                    log_trivia.info("Global monthly scores have been reset.")
    
    @tasks.loop(minutes=15)
    async def cache_refill_loop(self):
        # This loop is now global, doesn't iterate guilds
        global_data = self.get_global_data()
        if len(global_data.get("question_cache", [])) >= CACHE_MIN_SIZE:
            return

        try:
            log_trivia.info("Global cache is low, refilling with custom category distribution...")
            
            # Boosted Categories
            # 15: Games, 12: Music, 31: Anime, 32: Cartoons, 11: Film, 14: TV, 10: Books
            # 17: Science/Nature, 18: Computers, 30: Gadgets, 21: Sports, 16: Board Games
            boosted = [15, 12, 31, 32, 11, 14, 10, 17, 18, 30, 21, 16]
            
            fetch_tasks = []
            selected_boosted = random.sample(boosted, 5) # Pick 5 boosted categories
            for cat in selected_boosted:
                url = f"{TRIVIA_API_URL_BASE}?amount=7&category={cat}&type=multiple"
                fetch_tasks.append(self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)))
                
            # 15 random questions
            url_random = f"{TRIVIA_API_URL_BASE}?amount=15&type=multiple"
            fetch_tasks.append(self.session.get(url_random, timeout=aiohttp.ClientTimeout(total=10)))
            
            responses = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            
            all_results = []
            for resp in responses:
                try:
                    if isinstance(resp, Exception) or resp.status != 200:
                        log_trivia.warning(f"API call failed during distributed cache refill: {resp}")
                        continue
                    
                    data = await resp.json()
                    if data.get("response_code") == 0:
                        all_results.extend(data.get("results", []))
                    else:
                        log_trivia.info(f'OpenTDB API returned code {data.get("response_code")} for a difficulty fetch.')
                except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                    log_trivia.warning(f"Failed to decode JSON from API response: {e}")
                finally:
                    if not isinstance(resp, Exception):
                        resp.close()

            if all_results:
                new_q = [{"question": html.unescape(q["question"]), "answers": [html.unescape(a) for a in q["incorrect_answers"]] + [html.unescape(q["correct_answer"])], "correct": html.unescape(q["correct_answer"]), "category": html.unescape(q["category"])} for q in all_results]
                random.shuffle(new_q)
                async with self.config_lock:
                    live_global_data = self.get_global_data()
                    live_global_data["question_cache"].extend(new_q)
                    self.config_is_dirty = True
                log_trivia.info(f"Refilled global cache with {len(new_q)} questions from {len(all_results)} results.")

        except Exception as e:
            log_trivia.error(f"Unexpected error during global cache refill: {e}", exc_info=True)

    async def _process_monthly_winners(self, guild: discord.Guild):
        now_est = datetime.now(TRIVIA_TIMEZONE)
        
        global_data = self.get_global_data()
        scores = global_data.get("scores", {})
        
        cfg_settings = self.get_guild_settings(guild.id)
        last_announce = datetime.fromisoformat(cfg_settings["last_winner_announcement"]) if cfg_settings.get("last_winner_announcement") else None
        if last_announce and last_announce.month == now_est.month and last_announce.year == now_est.year: 
            return # Already announced for this guild

        try:
            channel = guild.get_channel(cfg_settings.get("channel_id", 0))
            if channel and scores:
                prev_month = now_est - timedelta(days=1)
                leaderboard_desc = await self._get_leaderboard_text(guild, limit=LEADERBOARD_LIMIT) 
                embed = discord.Embed(title=f"🏆 Trivia Champions for {prev_month.strftime('%B %Y')}", description=leaderboard_desc, color=0xFFD700)
                await channel.send(embed=embed)
            
            await self._handle_monthly_role_reward(guild, scores)
            
            async with self.config_lock:
                self.get_guild_settings(guild.id)["last_winner_announcement"] = now_est.isoformat()
                self.config_is_dirty = True
        except Exception as e:
            log_trivia.error(f"Monthly winner logic failed for guild {guild.id}: {e}", exc_info=True)

    @tasks.loop(seconds=60)
    async def backup_save_loop(self):
        await self.save_config_now()

    # === Core Gameplay Logic ===

    async def _get_cached_question(self) -> typing.Optional[dict]:
        # No guild_id needed
        async with self.config_lock:
            global_data = self.get_global_data()
            if not global_data.get("question_cache"):
                self.cache_refill_loop.restart()
                return None
            self.config_is_dirty = True
            return global_data["question_cache"].pop(0)

    async def _handle_monthly_role_reward(self, guild: discord.Guild, scores: dict):
        # scores (global) are passed in, but settings are guild-specific
        cfg_settings = self.get_guild_settings(guild.id)
        winner_role_id = cfg_settings.get("winner_role_id")
        if not winner_role_id or not (reward_role := guild.get_role(winner_role_id)): return

        if prev_winner_id := cfg_settings.get("last_month_winner_id"):
            try:
                member = await guild.fetch_member(prev_winner_id)
                if reward_role in member.roles: await member.remove_roles(reward_role, reason="Trivia month ended.")
            except discord.HTTPException: pass

        new_winner_id = None
        if scores:
            winner_id_str, _ = sorted(scores.items(), key=self._get_score_sort_key)[0]
            new_winner_id = int(winner_id_str)
            try:
                member = await guild.fetch_member(new_winner_id)
                # Added guild.me check
                if guild.me and guild.me.top_role > reward_role: 
                    await member.add_roles(reward_role, reason="Trivia monthly winner.")
            except discord.HTTPException as e:
                log_trivia.error(f"Failed to add winner role in guild {guild.id}: {e}")
                new_winner_id = None
        
        # FIX: Added config_is_dirty = True after saving winner
        async with self.config_lock: 
            self.get_guild_settings(guild.id)["last_month_winner_id"] = new_winner_id
            self.config_is_dirty = True

    async def _build_daily_embed(self, guild: discord.Guild) -> discord.Embed:
        # Gets settings from guild, data from global
        global_data = self.get_global_data()
        today_q = global_data.get("daily_question_data", {})
        now_est = datetime.now(TRIVIA_TIMEZONE)
        
        desc = f"**Category:** {today_q.get('category', 'Unknown')}"
        embed = discord.Embed(title=f"🎯 {now_est.strftime('%A')}'s Daily Trivia", description=desc, color=EMBED_COLOR_TRIVIA)

        fastest_times_text = await self._format_podium_text(guild, global_data.get("daily_answer_times", []), "", format_type="gateway")
        embed.add_field(name="⚡Fastest Times", value=fastest_times_text, inline=False)
        
        monthly_scores_text = await self._get_gateway_leaderboard_text(guild) # Fetches global data
        embed.add_field(name="🏆Monthly Top 5", value=monthly_scores_text, inline=False)
        
        embed.set_footer(text=f"{len(global_data.get('daily_interactions', []))} users have attempted.")
        return embed

    async def _trigger_daily_reset_and_post(self, guild: discord.Guild):
        self.reveal_timestamps.clear(); self.don_reveal_timestamps.clear(); self.cheat_test_timestamps.clear()
        
        async with self.config_lock:
            global_data = self.get_global_data()
            cfg_settings = self.get_guild_settings(guild.id)
            
            if len(global_data.get("question_cache", [])) < 2:
                self.cache_refill_loop.restart()
                raise TriviaPostingError("Insufficient questions in cache. Refilling now.")

            # Save recap data to GLOBAL
            global_data["yesterdays_recap_data"] = {
                "daily_question": global_data.get("daily_question_data"), 
                "daily_don_question": global_data.get("daily_don_question_data"),
                "daily_interactions": global_data.get("daily_interactions", []).copy(), 
                "daily_don_interactions": global_data.get("daily_don_interactions", []).copy(), 
                "daily_answer_times": global_data.get("daily_answer_times", []).copy(), 
                "daily_don_answer_times": global_data.get("daily_don_answer_times", []).copy()
            }

            # Reset GLOBAL daily data
            global_data["daily_question_data"] = global_data["question_cache"].pop(0)
            global_data["daily_don_question_data"] = global_data["question_cache"].pop(0)
            global_data["daily_interactions"], global_data["daily_don_interactions"], global_data["daily_answer_times"], global_data["daily_don_answer_times"] = [], [], [], []
            
            now_est = datetime.now(TRIVIA_TIMEZONE)
            # Update GUILD-SPECIFIC post time
            cfg_settings["last_posted_date"], cfg_settings["last_post_hour"] = now_est.date().isoformat(), now_est.hour
            self.config_is_dirty = True
        
        await self._post_or_bump_messages(guild)

    async def _bump_messages(self, guild: discord.Guild):
        async with self.config_lock:
            cfg_settings = self.get_guild_settings(guild.id)
            cfg_settings["last_post_hour"], self.config_is_dirty = datetime.now(TRIVIA_TIMEZONE).hour, True
        await self._post_or_bump_messages(guild)
    
    async def _post_or_bump_messages(self, guild: discord.Guild):
        cfg_settings = self.get_guild_settings(guild.id)
        if not (channel_id := cfg_settings.get("channel_id")): raise TriviaPostingError("Trivia channel not set.")
        
        try:
            channel = await self.bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden) as e: 
            raise TriviaPostingError(f"Cannot access channel `{channel_id}`. Please check the ID and my permissions.")
        
        perms = channel.permissions_for(guild.me)
        # Added view_channel permission check
        if not perms.view_channel: raise TriviaPostingError(f"I lack `View Channel` permission in {channel.mention}.")
        if not perms.send_messages: raise TriviaPostingError(f"I lack `Send Messages` permission in {channel.mention}.")
        if not perms.embed_links: raise TriviaPostingError(f"I lack `Embed Links` permission in {channel.mention}.")

        if isinstance(channel, discord.Thread) and (channel.archived or channel.locked):
            try:
                await channel.edit(archived=False, locked=False)
            except discord.Forbidden:
                raise TriviaPostingError(f"I need `Manage Threads` permission to unarchive {channel.mention}.")

        if gateway_id := cfg_settings.get("gateway_message_id"):
            try: await channel.get_partial_message(gateway_id).delete()
            except discord.HTTPException: pass

        try:
            msg = await channel.send(embed=await self._build_daily_embed(guild), view=DailyGatewayView(self))
            async with self.config_lock: self.get_guild_settings(guild.id)["gateway_message_id"], self.config_is_dirty = msg.id, True
        except discord.HTTPException as e: raise TriviaPostingError(f"Failed to send message to {channel.mention}. Error: {e}")

    async def build_recap_image_data(self, guild: discord.Guild, data: dict, full_global_data: dict) -> dict:
        dq = data.get("daily_question", {})
        yesterday = datetime.now(TRIVIA_TIMEZONE) - timedelta(days=1)
        
        user_stats = full_global_data.get("user_stats", {})
        
        async def format_podium(times_data):
            if not times_data: return '<div style="color:#aaa;">No data</div>'
            sorted_data = sorted(times_data, key=lambda x: x['time'])[:5]
            html = ""
            for i, entry in enumerate(sorted_data):
                name = await self._get_formatted_name(guild, entry['user_id'])
                if len(name) > 15: name = name[:12] + '...'
                html += f'<div class="rank">{i+1}. {name} ({entry["time"]:.2f}s)</div>'
            return html
            
        podium_html = await format_podium(data.get("daily_answer_times", []))
        don_podium_html = await format_podium(data.get("daily_don_answer_times", []))
        
        awards = []
        def add_award(title, value, is_orange=False):
            t_class = "stat-badge-title orange" if is_orange else "stat-badge-title"
            awards.append(f'<div class="stat-badge"><div class="{t_class}">{title}</div><div class="stat-badge-value">{value}</div></div>')

        q_cat = dq.get("category", "Unknown")
        correct_user_ids = {i['user_id'] for i in data.get("daily_interactions", []) if i.get("correct")}
        
        if correct_user_ids and q_cat != "Unknown":
            best_acc, best_uid, best_stats = 0, None, None
            for uid in correct_user_ids:
                cat_stats = user_stats.get(uid, {}).get("categories", {}).get(q_cat)
                if not cat_stats: continue
                total = cat_stats.get("correct", 0) + cat_stats.get("incorrect", 0)
                if total >= 5:
                    acc = cat_stats.get("correct", 0) / total
                    if acc > best_acc:
                        best_acc, best_uid, best_stats = acc, uid, cat_stats
            if best_uid:
                name = await self._get_formatted_name(guild, best_uid)
                add_award("THE SPECIALIST", f"{name[:12]} {best_stats.get('correct')}-{best_stats.get('incorrect')} ({best_acc*100:.0f}%)", True)
                
        if correct_user_ids:
            best_streak, best_uid = 0, None
            for uid in correct_user_ids:
                streak = user_stats.get(uid, {}).get("current_streak", 0)
                if streak > best_streak: best_streak, best_uid = streak, uid
            if best_streak >= 3:
                name = await self._get_formatted_name(guild, best_uid)
                add_award("LONGEST STREAK", f"{name[:15]} ({best_streak})", False)
                
        comebacks = [i for i in data.get("daily_interactions", []) if i.get("missed_before", 0) > 0 and i.get("correct")]
        if comebacks:
            best_cb = max(comebacks, key=lambda x: x["missed_before"])
            name = await self._get_formatted_name(guild, best_cb["user_id"])
            add_award("COMEBACK KID", f"{name[:15]} ({best_cb['missed_before']} missed)", True)
            
        leaps = [i for i in data.get("daily_interactions", []) if i.get("leap", 0) > 0]
        if leaps:
            best_leap = max(leaps, key=lambda x: x["leap"])
            name = await self._get_formatted_name(guild, best_leap["user_id"])
            add_award("BIGGEST LEAP", f"{name[:15]} (#{best_leap['old_rank']} ➔ #{best_leap['new_rank']})", False)
            
        scholars = [i for i in data.get("daily_interactions", []) if i.get("participation_streak", 0) >= 5]
        if scholars:
            best_scholar = max(scholars, key=lambda x: x["participation_streak"])
            name = await self._get_formatted_name(guild, best_scholar["user_id"])
            add_award("LOYAL SCHOLAR", f"{name[:15]} ({best_scholar['participation_streak']} days)", True)
            
        rank_ups = [i for i in data.get("daily_interactions", []) if i.get("rank_up")]
        if rank_ups:
            best_ru = rank_ups[0]
            name = await self._get_formatted_name(guild, best_ru["user_id"])
            add_award("RANK UP", f"{name[:12]} ({best_ru['rank_up']})", False)
            
        if len(awards) % 2 != 0:
            awards.append('<div class="stat-badge" style="opacity:0.3;"><div class="stat-badge-title">???</div></div>')

        async def format_at_list(items, value_fmt=lambda x: str(x)):
            if not items: return '<div style="color:#aaa;">No data</div>'
            html = ""
            for i, (uid, val) in enumerate(items[:5]):
                name = await self._get_formatted_name(guild, uid)
                if len(name) > 12: name = name[:10] + '..'
                html += f'<div><span class="at-rank">{i+1}.</span> {name} <span class="at-score">{value_fmt(val)}</span></div>'
            return html

        all_time_scores = [(uid, stats.get("all_time_score", 0)) for uid, stats in user_stats.items() if stats.get("all_time_score", 0) > 0]
        all_time_scores.sort(key=lambda x: x[1], reverse=True)
        
        all_time_correct = [(uid, stats.get("correct", 0)) for uid, stats in user_stats.items() if stats.get("correct", 0) > 0]
        all_time_correct.sort(key=lambda x: x[1], reverse=True)
        
        all_time_don_wins = [(uid, stats.get("don_successes", 0)) for uid, stats in user_stats.items() if stats.get("don_successes", 0) > 0]
        all_time_don_wins.sort(key=lambda x: x[1], reverse=True)
        
        all_time_acc = []
        all_time_don_acc = []
        for uid, stats in user_stats.items():
            tot = stats.get("correct", 0) + stats.get("incorrect", 0)
            if tot >= 20: all_time_acc.append((uid, stats.get("correct", 0) / tot))
            don_tot = stats.get("don_accepted", 0)
            if don_tot >= 5: all_time_don_acc.append((uid, stats.get("don_successes", 0) / don_tot))
                
        all_time_acc.sort(key=lambda x: x[1], reverse=True)
        all_time_don_acc.sort(key=lambda x: x[1], reverse=True)

        # Calculate dynamic height based on number of award badges
        # Base height to fit header + questions + podiums + all-time section = 2000
        # Each row of badges (2 badges per row) takes roughly 150px
        num_badges = len(awards)
        num_rows = (num_badges + 1) // 2
        dynamic_height = 1900 + (num_rows * 160)

        return {
            "day_of_week": yesterday.strftime('%A'),
            "question_text": dq.get("question", "N/A"),
            "answer_text": dq.get("correct", "N/A"),
            "don_question_text": data.get("daily_don_question", {}).get("question", "No DoN Question Played"),
            "don_answer_text": data.get("daily_don_question", {}).get("correct", "N/A"),
            "podium_html": podium_html,
            "don_podium_html": don_podium_html,
            "awards_html": "".join(awards),
            "at_score_html": await format_at_list(all_time_scores),
            "at_correct_html": await format_at_list(all_time_correct),
            "at_don_wins_html": await format_at_list(all_time_don_wins[:3]),
            "at_accuracy_html": await format_at_list(all_time_acc[:3], lambda x: f"{x*100:.1f}%"),
            "at_don_acc_html": await format_at_list(all_time_don_acc[:3], lambda x: f"{x*100:.1f}%"),
            "dynamic_height": dynamic_height
        }

    async def build_daily_awards_embed(self, guild: discord.Guild, data: dict, full_global_data: dict) -> discord.Embed:
        # `data` is the recap_data
        # `full_global_data` is the snapshot of global_data
        dq = data.get("daily_question", {})
        total = len(data.get("daily_interactions", []))
        correct = sum(1 for i in data.get("daily_interactions", []) if i.get("correct"))
        yesterday = datetime.now(TRIVIA_TIMEZONE) - timedelta(days=1)
        
        embed = discord.Embed(title=f"📈 {yesterday.strftime('%A')}'s Trivia Report", color=0x3498DB)
        success = (correct / total * 100) if total > 0 else 0
        embed.description = f"**{total}** users participated with a **{success:.1f}%** success rate.\n\n> **Q:** {dq.get('question', 'N/A')}\n> **A:** {dq.get('correct', 'N/A')}"
        
        # --- Podiums ---
        podium_text = await self._format_podium_text(guild, data.get("daily_answer_times", []), "*No one answered correctly in time.*", format_type="recap")
        embed.add_field(name="🏆 The Podium", value=podium_text, inline=False)

        don_podium_text = await self._format_podium_text(guild, data.get("daily_don_answer_times", []), "*No one won the challenge round.*", format_type="recap")
        embed.add_field(name="🎲 Double or Nothing Podium", value=don_podium_text, inline=False)

        # --- Daily Awards ---
        user_stats = full_global_data.get("user_stats", {}) # Get user_stats from the global snapshot
        correct_user_ids = {i['user_id'] for i in data.get("daily_interactions", []) if i.get("correct")}

        # Award 1: Longest Streaks
        streak_winners = []
        if correct_user_ids:
            max_streak = 0
            eligible_streaks = []
            for uid in correct_user_ids:
                streak = user_stats.get(uid, {}).get("current_streak", 0)
                if streak >= 3:
                    eligible_streaks.append((uid, streak))
                    if streak > max_streak:
                        max_streak = streak
            if max_streak > 0:
                streak_winners = [uid for uid, streak in eligible_streaks if streak == max_streak]
        
        if streak_winners:
            names = [f"> *{(await self._get_formatted_name(guild, uid))[:15]}* ({user_stats[uid]['current_streak']} days)" for uid in streak_winners]
            embed.add_field(name="🎯 Longest Streaks", value="\n".join(names), inline=False)

        # Award 2: The Specialist
        specialist_winners = []
        q_cat = dq.get("category", "Unknown")
        if correct_user_ids and q_cat != "Unknown":
            best_accuracy = 0
            eligible_specialists = []
            for uid in correct_user_ids:
                cat_stats = user_stats.get(uid, {}).get("categories", {}).get(q_cat)
                if not cat_stats: continue
                
                total = cat_stats.get("correct", 0) + cat_stats.get("incorrect", 0)
                if total >= 5:
                    accuracy = cat_stats.get("correct", 0) / total
                    if accuracy >= best_accuracy: # Use >= to include ties
                        eligible_specialists.append((uid, accuracy))
                        best_accuracy = accuracy

            if best_accuracy > 0:
                specialist_winners = [(uid, acc) for uid, acc in eligible_specialists if acc == best_accuracy]

        if specialist_winners:
            names = [f"> *{(await self._get_formatted_name(guild, uid))[:15]}* ({acc * 100:.0f}%)" for uid, acc in specialist_winners]
            embed.add_field(name=f"🧠 The Specialist ({q_cat})", value="\n".join(names), inline=False)

        # Award 3: The Comeback Kid
        comeback_kids = []
        correct_interactions = [i for i in data.get("daily_interactions", []) if i.get("correct")]
        for i in correct_interactions:
            uid = i["user_id"]
            if user_stats.get(uid, {}).get("current_streak") == 1 and i.get("missed_before", 0) > 0:
                comeback_kids.append((uid, i["missed_before"]))
        
        if comeback_kids:
            winner_id, missed = random.choice(comeback_kids)
            name = await self._get_formatted_name(guild, winner_id)
            embed.add_field(name="💪 The Comeback Kid", value=f"> *{name[:15]}* ({missed} missed)", inline=False)

        # Award 4: The Biggest Leap
        biggest_leap = 0
        leap_winner = None
        for interaction_data in data.get("daily_interactions", []):
            leap = interaction_data.get("leap", 0)
            if leap > biggest_leap:
                biggest_leap = leap
                leap_winner = interaction_data
        
        if leap_winner:
            name = await self._get_formatted_name(guild, leap_winner['user_id'])
            rank_text = f"(#{leap_winner['old_rank']} > #{leap_winner['new_rank']})"
            embed.add_field(name="🧗 The Biggest Leap", value=f"> *{name[:15]}* {rank_text}", inline=False)

        return embed

    async def handle_trivia_answer(self, interaction: discord.Interaction, view: EphemeralQuestionView):
        try:
            # Ensure interaction is deferred (answer_callback may have already deferred)
            if not interaction.response.is_done():
                await interaction.response.defer()
            
            answer_time, user_id_str = datetime.now(timezone.utc), str(interaction.user.id)
            
            # Validation check for custom_id
            custom_id_parts = interaction.data["custom_id"].split('_')
            if len(custom_id_parts) < 2:
                log_trivia.error(f"Malformed custom_id: {interaction.data['custom_id']}")
                return await interaction.edit_original_response(content="Invalid interaction data.", view=None)
            answer_index_str = custom_id_parts[1] 
            
            try:
                is_correct = (int(answer_index_str) == view.correct_index)
            except ValueError:
                log_trivia.error(f"Failed to parse answer index from custom_id: {interaction.data['custom_id']}")
                return await interaction.edit_original_response(content="An unexpected data format error occurred.", view=None)

            points, final_score, streak_msg, don_view = 0, 0, "", None
            
            async with self.config_lock:
                global_data = self.get_global_data()
                stats = self.get_user_stats(interaction.user.id)
                q_cat = global_data.get("daily_question_data", {}).get("category", "Unknown")
                stats.setdefault("categories", {}).setdefault(q_cat, {"correct": 0, "incorrect": 0})

                # --- Pre-answer rank calculation for "Biggest Leap" and "Rank Up" awards ---
                old_rank, new_rank, leap = None, None, 0
                
                old_score = stats.get("all_time_score", 0)
                old_prestige_rank, _, _, _ = self.get_prestige_rank(old_score)
                
                if is_correct:
                    # No deepcopy needed — scores haven't been modified yet at this point
                    sorted_before = sorted(global_data.get("scores", {}).items(), key=self._get_score_sort_key)
                    rank_map_before = {uid: i + 1 for i, (uid, _) in enumerate(sorted_before)}
                    old_rank = rank_map_before.get(user_id_str, len(sorted_before) + 1)
                
                # Added logic for Comeback Kid
                missed_before = stats.get("current_incorrect_streak", 0)

                # --- Participation Tracking ---
                today_date = answer_time.date().isoformat()
                last_part = stats.get("last_participation_date")
                last_part_date = datetime.fromisoformat(last_part).date() if last_part else None
                if last_part_date != answer_time.date():
                    if last_part_date == answer_time.date() - timedelta(days=1):
                        stats["participation_streak"] = stats.get("participation_streak", 0) + 1
                    else:
                        stats["participation_streak"] = 1
                    stats["last_participation_date"] = today_date
                    stats["longest_participation_streak"] = max(stats.get("longest_participation_streak", 0), stats["participation_streak"])

                if is_correct:
                    stats["correct"] += 1; stats["categories"][q_cat]["correct"] += 1
                    stats["current_streak"] += 1
                    stats["longest_streak"] = max(stats["current_streak"], stats["longest_streak"])
                    stats["current_incorrect_streak"] = 0 # Reset incorrect streak
                    points = 1
                    reveal_ts = self.reveal_timestamps.pop((interaction.guild.id, interaction.user.id), None)
                    if reveal_ts is not None:
                        delta = (answer_time - reveal_ts).total_seconds()
                        global_data.setdefault("daily_answer_times", []).append({"user_id": user_id_str, "time": delta})
                    else:
                        log_trivia.warning(f"Missing reveal timestamp for user {user_id_str} (likely bot restart); skipping answer-time recording.")
                    don_cat = global_data.get("daily_don_question_data", {}).get("category", "a surprise")
                    don_view = DoubleOrNothingPromptView(self, interaction.user, points, don_cat)
                else:
                    stats["incorrect"] += 1; stats["categories"][q_cat]["incorrect"] += 1
                    stats["current_incorrect_streak"] = stats.get("current_incorrect_streak", 0) + 1 # Increment incorrect streak
                    stats["current_streak"] = 0
                    self.reveal_timestamps.pop((interaction.guild.id, interaction.user.id), None)

                score_data = global_data.setdefault("scores", {}).setdefault(user_id_str, {"score": 0, "timestamp": None})
                if isinstance(score_data, int): score_data = {"score": score_data, "timestamp": None}
                score_data["score"] = score_data.get("score", 0) + points
                stats["all_time_score"] = stats.get("all_time_score", 0) + points
                
                # FIX: Only update timestamps when GAINING points
                if points > 0:
                    score_data["timestamp"] = answer_time.isoformat()
                    stats["all_time_timestamp"] = answer_time.isoformat()
                    
                final_score = score_data["score"]

                # --- Post-answer rank calculation ---
                new_prestige_rank, _, _, _ = self.get_prestige_rank(stats.get("all_time_score", 0))
                rank_up_str = None
                if is_correct and old_prestige_rank != new_prestige_rank:
                    rank_up_str = f"{old_prestige_rank} ➔ {new_prestige_rank}"

                if is_correct:
                    scores_after = global_data.get("scores", {})
                    sorted_after = sorted(scores_after.items(), key=self._get_score_sort_key)
                    rank_map_after = {uid: i + 1 for i, (uid, _) in enumerate(sorted_after)}
                    new_rank = rank_map_after.get(user_id_str)
                    if old_rank and new_rank:
                        leap = old_rank - new_rank

                interaction_record = {"user_id": user_id_str, "correct": is_correct}
                interaction_record["participation_streak"] = stats.get("participation_streak", 1)

                if leap > 0:
                    interaction_record.update({"leap": leap, "old_rank": old_rank, "new_rank": new_rank})
                if rank_up_str:
                    interaction_record["rank_up"] = rank_up_str
                
                # Store missed_before for Comeback Kid
                if is_correct and stats["current_streak"] == 1 and missed_before > 0:
                    interaction_record["missed_before"] = missed_before
                
                global_data.setdefault("daily_interactions", []).append(interaction_record)

                self.config_is_dirty = True
            
            for i, item in enumerate(view.children):
                item.disabled = True
                # Check for correct answer match using index
                if i == view.correct_index:
                    item.style = discord.ButtonStyle.success
                # Check for the clicked button's custom_id match
                elif item.custom_id == interaction.data["custom_id"]: 
                    item.style = discord.ButtonStyle.danger
            
            response = "✅ **Correct!**" if is_correct else "❌ Incorrect."
            # Edit the original response (the ephemeral message)
            await interaction.edit_original_response(content=f"{response}\nYour score is now **{final_score}**.", view=view)
            
            if don_view: 
                # Send the prompt as a new ephemeral message via followup.send
                await don_view.start(interaction)
            
            # Update gateway message in the guild this was triggered from
            await self.update_gateway_message(interaction.guild.id)
        except Exception as e:
            log_trivia.error(f"Critical error in handle_trivia_answer for {interaction.user}: {e}", exc_info=True)
            try:
                await interaction.followup.send(content="A critical error occurred. Please contact an admin.", ephemeral=True)
            except discord.HTTPException:
                pass

    async def update_gateway_message(self, guild_id: int):
        cfg_settings = self.get_guild_settings(guild_id)
        if not (msg_id := cfg_settings.get("gateway_message_id")) or not (chan_id := cfg_settings.get("channel_id")): return
        try:
            channel = self.bot.get_channel(chan_id) or await self.bot.fetch_channel(chan_id)
            message = await channel.fetch_message(msg_id)
            await message.edit(embed=await self._build_daily_embed(message.guild), view=DailyGatewayView(self))
        except discord.HTTPException: pass

    # === Anti-Cheat System ===

    async def _start_cheater_test(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user_id_str = str(interaction.user.id)
        async with self.config_lock:
            test_data = self.get_global_data().get("cheater_test_users", {}).get(user_id_str)
        if not test_data: return await interaction.followup.send("Test data not found.", ephemeral=True)

        q_data = test_data['main_q']
        answers = q_data.get("answers",[])
        if not answers:
            log_trivia.error(f"Anti-Cheat main question answers list is empty for user {interaction.user.id}. Cannot start game.")
            return await interaction.followup.send("Error: Anti-Cheat question data is invalid (no answers).", ephemeral=True)
            
        shuffled = random.sample(answers, len(answers))
        self.cheat_test_timestamps[(interaction.guild.id, interaction.user.id)] = datetime.now(timezone.utc)
        embed = discord.Embed(title="Question", description=f"**{q_data.get('question','')}**", color=discord.Color.dark_red())
        embed.set_footer(text=f"You have {int(EPHEMERAL_QUESTION_TIMEOUT)} seconds.")
        view = EphemeralCheaterTestView(self, shuffled, q_data.get("correct"), 'main', interaction.guild.id, interaction.user.id)
        # FIX: Capture the returned message from followup.send
        msg = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        view.message = msg

    async def handle_cheater_test_answer(self, interaction: discord.Interaction, view: EphemeralCheaterTestView):
        # Interaction response is already deferred by answer_callback
        answer_time, user_id_str = datetime.now(timezone.utc), str(interaction.user.id)
        chosen_button: discord.ui.Button = discord.utils.get(view.children, custom_id=interaction.data['custom_id'])
        
        # Check if chosen_button is None
        if not chosen_button:
            log_trivia.error(f"Cheat Test Answer callback received but button not found for custom_id: {interaction.data['custom_id']}")
            return

        is_correct = (getattr(chosen_button, 'full_text', None) == view.correct_answer)
        delta = (answer_time - self.cheat_test_timestamps.pop((interaction.guild.id, interaction.user.id), answer_time)).total_seconds()

        async with self.config_lock:
            test_data = self.get_global_data().get("cheater_test_users", {}).get(user_id_str)
        if not test_data: return await interaction.edit_original_response(content="Test expired.", view=None, embed=None)

        await self._send_cheat_test_report(interaction, test_data, view.question_type, is_correct, delta, chosen_button.full_text)

        for item in view.children:
            item.disabled = True
            if getattr(item, 'full_text', None) == view.correct_answer: item.style = discord.ButtonStyle.success
            elif item == chosen_button: item.style = discord.ButtonStyle.danger
        
        # Use edit_original_response for the deferred ephemeral message
        await interaction.edit_original_response(content="✅ Correct!" if is_correct else "❌ Incorrect.", view=view, embed=None)

        if view.question_type == 'main' and is_correct:
            q_data = test_data['don_q']
            answers = q_data.get("answers",[])
            if not answers:
                log_trivia.error(f"Anti-Cheat bonus question answers list is empty for user {interaction.user.id}.")
                return
                
            shuffled = random.sample(answers, len(answers))
            self.cheat_test_timestamps[(interaction.guild.id, interaction.user.id)] = datetime.now(timezone.utc)
            embed = discord.Embed(title="Bonus Challenge!", description=f"**{q_data.get('question','')}**", color=discord.Color.dark_gold())
            embed.set_footer(text=f"You have {int(EPHEMERAL_QUESTION_TIMEOUT)} seconds.")
            don_view = EphemeralCheaterTestView(self, shuffled, q_data.get("correct"), 'don', interaction.guild.id, interaction.user.id)
            # FIX: Capture the returned message from followup.send
            msg = await interaction.followup.send(content="Bonus question (no points):", embed=embed, view=don_view, ephemeral=True)
            don_view.message = msg
        else: # Test is over
            async with self.config_lock:
                self.get_global_data().get("cheater_test_users", {}).pop(user_id_str, None)
                self.config_is_dirty = True

    async def _send_cheat_test_report(self, interaction: discord.Interaction, test_data: dict, q_type: str, is_correct: bool, time: float, choice: str):
        cfg_settings = self.get_guild_settings(interaction.guild.id) # Report channel is guild-specific
        if not (channel_id := cfg_settings.get("anti_cheat_results_channel_id")): return
        try:
            channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
            q_data = test_data['main_q'] if q_type == 'main' else test_data['don_q']
            
            # Should be: Red (0xE74C3C) if suspicious (correct), Green (0x2ECC71) if passed (wrong)
            color=0xE74C3C if is_correct else 0x2ECC71 
            
            embed = discord.Embed(title="🕵️ Anti-Cheat Result", color=color, timestamp=datetime.now(timezone.utc))
            embed.set_author(name=f"{interaction.user} ({interaction.user.id})", icon_url=interaction.user.display_avatar.url)
            embed.add_field(name="Question", value=f"```{q_data.get('question','')}```", inline=False)
            embed.add_field(name="User's Answer", value=choice, inline=True)
            embed.add_field(name="Correct Answer", value=q_data.get('correct'), inline=True)
            embed.add_field(name="Time", value=f"{time:.2f}s", inline=True)
            embed.add_field(name="Result", value=f"**{'⚠️ SUSPICIOUS' if is_correct else '✅ PASSED'}**", inline=False)
            await channel.send(embed=embed)
        except discord.HTTPException as e:
            log_trivia.warning(f"Could not send anti-cheat report to {channel_id}: {e}")

    # === User Slash Commands ===

    @trivia.command(name="leaderboard", description="Shows the monthly and all-time leaderboards.")
    async def leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False, thinking=True)
        # Guild is still needed for name formatting
        desc = await self._get_leaderboard_text(interaction.guild, limit=LEADERBOARD_LIMIT)
        embed = discord.Embed(title="📊 Monthly Leaderboard", description=desc, color=EMBED_COLOR_TRIVIA)
        await interaction.followup.send(embed=embed, view=LeaderboardView(self))
    
    @trivia.command(name="stats", description="Shows a user's trivia statistics.")
    @app_commands.describe(user="The user whose stats you want to see (optional).")
    async def stats(self, interaction: discord.Interaction, user: discord.Member = None):
        await interaction.response.defer(ephemeral=False, thinking=True)
        target = user or interaction.user
        async with self.config_lock:
            global_data = self.get_global_data()
            stats_data = self.get_user_stats(target.id)
            monthly_scores = global_data.get("scores", {})
            all_user_stats = global_data.get("user_stats", {})

        score = stats_data.get("all_time_score", 0)
        rank, next_score, to_next, next_name = self.get_prestige_rank(score)
        total = stats_data.get("correct", 0) + stats_data.get("incorrect", 0)
        accuracy = (stats_data.get("correct", 0) / total * 100) if total > 0 else 0
        don_accepted = stats_data.get("don_accepted", 0)
        don_total = stats_data.get("don_declined", 0) + don_accepted
        don_acceptance_rate = (don_accepted / don_total * 100) if don_total > 0 else 0
        don_win_rate = (stats_data.get("don_successes", 0) / don_accepted * 100) if don_accepted > 0 else 0
        
        # FIX: Use _get_alltime_sort_key for tie-breaking in all_time_rank calculation
        all_time_rank = next((f"#{i+1}" for i, (uid, _) in enumerate(sorted(all_user_stats.items(), key=self._get_alltime_sort_key)) if uid == str(target.id)), "N/A")
        monthly_rank = next((f"#{i+1}" for i, (uid, _) in enumerate(sorted(monthly_scores.items(), key=self._get_score_sort_key)) if uid == str(target.id)), "N/A")

        embed = discord.Embed(title=f"Trivia Stats for {target.display_name}", color=target.color).set_thumbnail(url=target.display_avatar.url)
        progress = "Max Rank! 🏆" if rank == next_name else f"Next: **{next_name}** ({to_next} pts)"
        embed.add_field(name="🌟 Prestige", value=f"**Rank:** {rank}\n{progress}", inline=False)
        
        embed.add_field(name="📊 Core Stats", value=f"**All-Time Score:** {score}\n**All-Time Rank:** {all_time_rank}\n**Accuracy:** {accuracy:.2f}%")
        embed.add_field(name="📈 Streaks", value=f"**Current:** 🔥 {stats_data.get('current_streak', 0)}\n**Longest:** 🌟 {stats_data.get('longest_streak', 0)}")
        embed.add_field(name="🗓️ This Month", value=f"**Score:** {monthly_scores.get(str(target.id), {}).get('score', 0)}\n**Rank:** {monthly_rank}\n**Attempted:** {total}")
        embed.add_field(name="🎲 Double or Nothing", value=f"**Acceptance:** {don_acceptance_rate:.1f}%\n**Success Rate:** {don_win_rate:.1f}%")
        await interaction.followup.send(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(DailyTrivia(bot))
