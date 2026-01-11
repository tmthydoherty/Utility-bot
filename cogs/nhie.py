import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import random
import asyncio
import uuid # For Fix #2 & #7
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

# --- ( HELPER FUNCTIONS FOR DATA ) ---
# These functions manage the JSON files for settings and questions.

# --- (MODIFIED: File Path Definitions) ---
# Get the directory of this cog file (e.g., /path/to/bot/cogs)
COG_DIR = os.path.dirname(__file__)
# Get the main bot root directory (e.g., /path/to/bot)
BOT_ROOT = os.path.dirname(COG_DIR)

# Point all JSON files to the main bot root directory
SETTINGS_FILE = os.path.join(BOT_ROOT, 'nhie_settings.json')
QUESTIONS_FILE = os.path.join(BOT_ROOT, 'nhie_questions.json')
HISTORY_FILE = os.path.join(BOT_ROOT, 'nhie_history.json')
VOTES_FILE = os.path.join(BOT_ROOT, 'nhie_votes.json')
PENDING_SUGGESTIONS_FILE = os.path.join(BOT_ROOT, 'nhie_pending_suggestions.json') # For Fix #2
# --- (END OF MODIFICATION) ---

# Thread-safe locks for file I/O
settings_lock = asyncio.Lock()
questions_lock = asyncio.Lock()
history_lock = asyncio.Lock()
votes_lock = asyncio.Lock()
pending_suggestions_lock = asyncio.Lock() # For Fix #2

async def load_json(file_path: str, lock: asyncio.Lock) -> Dict[str, Any]:
    """Safely loads a JSON file."""
    async with lock:
        if not os.path.exists(file_path):
            return {}
        try:
            # --- (FIXED SYNTAX ERROR) ---
            with open(file_path, 'r', encoding='utf-8') as f:
                # --- (END OF FIX) ---
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

async def save_json(file_path: str, data: Dict[str, Any], lock: asyncio.Lock):
    """Safely saves data to a JSON file."""
    async with lock:
        try:
            # --- (FIXED SYNTAX ERROR) ---
            with open(file_path, 'w', encoding='utf-8') as f:
                # --- (END OF FIX) ---
                json.dump(data, f, indent=4)
        except IOError as e:
            print(f"Error saving JSON to {file_path}: {e}")

# --- ( DATA STRUCTURES ) ---

# Default settings for a new guild
def get_default_settings() -> Dict[str, Any]:
    return {
        "post_channels": [],
        "log_channel": None,
        "trigger_role": None,
        "cooldown_hours": 1.0,
        "last_post_time": 0.0
    }

# Default question structure for a new guild
def get_default_questions() -> Dict[str, List]:
    return {
        "pool": [] # Format: {"question": "...", "suggester_id": 12345}
    }

# Default history structure for a new guild
def get_default_history() -> Dict[str, List]:
    return {
        "questions": [] # Format: {"all_message_ids": [...], "question": "...", ...}
    }
    
# Default votes structure (separate file to avoid large history file)
# Key: message_id, Value: {"have": [user_id...], "never": [user_id...]}
def get_default_votes() -> Dict[str, Dict]:
    return {}
    
# Default pending suggestions structure (For Fix #2)
def get_default_pending_suggestions() -> Dict[str, Dict]:
    # Key: suggestion_id, Value: {"suggester_id": ..., "question_text": ..., "guild_id": ..., "timestamp": ...}
    return {}


# --- ( MODALS ) ---

class SuggestionModal(discord.ui.Modal, title='Suggest a Question'):
    """Modal for a user to suggest a new NHIE question."""
    # --- (MODIFIED: Clarify suggestion text) ---
    question = discord.ui.TextInput(
        label="Your suggestion (after 'Never have I ever')",
        style=discord.TextStyle.paragraph,
        placeholder='...eaten a whole pizza by myself.',
        required=True,
        max_length=250
    )
    # --- (END OF MODIFICATION) ---

    def __init__(self, cog: 'NeverHaveIEver'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild.id)
        settings = (await load_json(SETTINGS_FILE, settings_lock)).get(guild_id, get_default_settings())
        log_channel_id = settings.get('log_channel')

        if not log_channel_id:
            await interaction.response.send_message(
                "Sorry, the suggestion system isn't set up yet. An admin needs to set a log channel.",
                ephemeral=True
            )
            return

        log_channel = interaction.guild.get_channel(log_channel_id)
        if not log_channel:
            await interaction.response.send_message(
                "I can't find the log channel. Please notify an admin.",
                ephemeral=True
            )
            return
            
        # --- (Fix #2 & #6: Save suggestion to JSON) ---
        suggestion_id = str(uuid.uuid4())
        pending_data = await load_json(PENDING_SUGGESTIONS_FILE, pending_suggestions_lock)
        pending_data[suggestion_id] = {
            "suggester_id": interaction.user.id,
            "question_text": self.question.value,
            "guild_id": guild_id,
            "timestamp": datetime.now().timestamp() # (Fix #6)
        }
        await save_json(PENDING_SUGGESTIONS_FILE, pending_data, pending_suggestions_lock)
        # --- (End of Fix #2 & #6) ---

        # Send suggestion to log channel for approval
        embed = discord.Embed(
            title="New NHIE Suggestion",
            description=f"**Never have I ever...**\n{self.question.value}",
            color=discord.Color.blue()
        )
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
        embed.set_footer(text=f"Suggested by: {interaction.user} (ID: {interaction.user.id})")

        # Add approval buttons (now referencing the suggestion_id)
        view = SuggestionApprovalView(
            cog=self.cog,
            suggestion_id=suggestion_id
        )
        
        try:
            await log_channel.send(embed=embed, view=view)
            await interaction.response.send_message(
                "Your suggestion has been sent to the admins for review. Thank you!",
                ephemeral=True
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to send messages in the log channel. Please notify an admin.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(f"An error occurred: {e}", ephemeral=True)

class AddQuestionsModal(discord.ui.Modal, title='Add NHIE Questions'):
    """Modal for admins to bulk-add questions."""
    questions = discord.ui.TextInput(
        label='Questions (one per line)',
        style=discord.TextStyle.paragraph,
        placeholder='...eaten a whole pizza.\n...been to another continent.\n...pulled an all-nighter.',
        required=True
    )

    def __init__(self, cog: 'NeverHaveIEver'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild.id)
        lines = self.questions.value.split('\n')
        added_count = 0
        
        questions_data = await load_json(QUESTIONS_FILE, questions_lock)
        if guild_id not in questions_data:
            questions_data[guild_id] = get_default_questions()
        
        question_pool = {q['question'].lower().strip() for q in questions_data[guild_id]['pool']}

        for line in lines:
            stripped_line = line.strip()
            if stripped_line and stripped_line.lower() not in question_pool:
                questions_data[guild_id]['pool'].append({
                    "question": stripped_line,
                    "suggester_id": interaction.user.id  # Admin who added it
                })
                question_pool.add(stripped_line.lower()) # Add to set to prevent duplicates in same batch
                added_count += 1
        
        await save_json(QUESTIONS_FILE, questions_data, questions_lock)
        
        await interaction.response.send_message(
            f"Successfully added {added_count} new questions. "
            f"Total questions in the pool: {len(questions_data[guild_id]['pool'])}",
            ephemeral=True
        )

class SetCooldownModal(discord.ui.Modal, title='Set Question Cooldown'):
    """Modal for admins to set the cooldown period."""
    cooldown = discord.ui.TextInput(
        label='Cooldown in Hours (e.g., 0.5 for 30 mins)',
        style=discord.TextStyle.short,
        placeholder='1.0',
        required=True
    )

    def __init__(self, cog: 'NeverHaveIEver'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild.id)
        try:
            cooldown_hours = float(self.cooldown.value)
            if cooldown_hours < 0:
                raise ValueError("Cooldown cannot be negative.")
                
            settings_data = await load_json(SETTINGS_FILE, settings_lock)
            if guild_id not in settings_data:
                settings_data[guild_id] = get_default_settings()
            
            settings_data[guild_id]['cooldown_hours'] = cooldown_hours
            await save_json(SETTINGS_FILE, settings_data, settings_lock)
            
            await interaction.response.send_message(
                f"Question cooldown set to {cooldown_hours} hours.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message(
                "Invalid input. Please enter a valid number (e.g., `1`, `0.5`, `2.25`).",
                ephemeral=True
            )

class ConfirmDeleteModal(discord.ui.Modal, title='Confirm Deletion'):
    """Modal to confirm deleting a question by number."""
    question_number = discord.ui.TextInput(
        label='Question Number to Delete',
        style=discord.TextStyle.short,
        placeholder='e.g., 12',
        required=True
    )

    def __init__(self, cog: 'NeverHaveIEver', parent_view: 'QuestionViewerView'):
        super().__init__()
        self.cog = cog
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            num = int(self.question_number.value)
            if num <= 0:
                raise ValueError("Number must be positive.")
        except ValueError:
            await interaction.response.send_message(
                "Invalid number. Please enter a valid question number from the list.",
                ephemeral=True
            )
            return

        guild_id = str(interaction.guild.id)
        questions_data = await load_json(QUESTIONS_FILE, questions_lock)
        question_pool = questions_data.get(guild_id, get_default_questions())['pool']
        
        target_index = num - 1 # 1-indexed to 0-indexed
        
        if 0 <= target_index < len(question_pool):
            deleted_question = question_pool.pop(target_index)
            await save_json(QUESTIONS_FILE, questions_data, questions_lock)
            
            # Refresh the parent view to show the updated list
            await self.parent_view.update_message(interaction)
            await interaction.followup.send_message(
                f"Deleted question #{num}: `...{deleted_question['question']}`",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"Invalid number. There is no question #{num} in the pool.",
                ephemeral=True
            )


# --- ( VIEWS ) ---

class SuggestionApprovalView(discord.ui.View):
    """View with Approve/Deny buttons for the log channel. (Fix #1)"""
    
    def __init__(self, cog: 'NeverHaveIEver', suggestion_id: Optional[str]):
        # (Fix #1): Set a timeout, do not make persistent
        super().__init__(timeout=60 * 60 * 24 * 3) # 3 day timeout
        self.cog = cog
        
        # Update custom_ids to use the persistent suggestion_id
        if suggestion_id:
            self.approve_button.custom_id = f"nhie_approve:{suggestion_id}"
            self.deny_button.custom_id = f"nhie_deny:{suggestion_id}"

    @discord.ui.button(label='Approve', style=discord.ButtonStyle.success, custom_id='nhie_approve_base')
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)

        # --- (Fix #4 & #7: Validate, Read, then Pop) ---
        try:
            suggestion_id_str = interaction.custom_id.split(':')[-1]
            uuid.UUID(suggestion_id_str) # Validate it
            suggestion_id = suggestion_id_str
        except (ValueError, IndexError):
            await interaction.response.send_message("Invalid suggestion ID format.", ephemeral=True)
            return
        
        # 1. Read data (no pop)
        pending_data = await load_json(PENDING_SUGGESTIONS_FILE, pending_suggestions_lock)
        suggestion_data = pending_data.get(suggestion_id)

        if not suggestion_data:
            await interaction.response.send_message("This suggestion has already been actioned or has expired.", ephemeral=True)
            return

        suggester_id = suggestion_data['suggester_id']
        question_text = suggestion_data['question_text']
        
        # 2. Check for duplicates
        questions_data = await load_json(QUESTIONS_FILE, questions_lock)
        if guild_id not in questions_data:
            questions_data[guild_id] = get_default_questions()
        
        if any(q['question'].lower() == question_text.lower() for q in questions_data[guild_id]['pool']):
            await interaction.response.send_message("This question is already in the pool.", ephemeral=True)
            # Do not pop, just return
            return

        # 3. Not a duplicate. Add to pool.
        questions_data[guild_id]['pool'].append({
            "question": question_text,
            "suggester_id": suggester_id
        })
        await save_json(QUESTIONS_FILE, questions_data, questions_lock)
            
        # 4. Now pop from pending
        if pending_data.pop(suggestion_id, None): # Safely pop
            await save_json(PENDING_SUGGESTIONS_FILE, pending_data, pending_suggestions_lock)
        # --- (End of Fix #4) ---
            
        # Edit original message
        embed = interaction.message.embeds[0]
        embed.title = "✅ Approved Suggestion"
        embed.color = discord.Color.green()
        embed.add_field(name="Action By", value=interaction.user.mention, inline=False)
        await interaction.message.edit(embed=embed, view=None)
        await interaction.response.send_message("Suggestion approved and added to the pool.", ephemeral=True)
        
        # DM Suggester
        try:
            suggester = await self.cog.bot.fetch_user(suggester_id)
            if suggester:
                await suggester.send(
                    f"Your 'Never have I ever...' suggestion in **{interaction.guild.name}** was approved!\n"
                    f"> ...{question_text}"
                )
        except (discord.Forbidden, discord.NotFound):
            pass # Can't DM them


    @discord.ui.button(label='Deny', style=discord.ButtonStyle.danger, custom_id='nhie_deny_base')
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # --- (Fix #4 & #7: Validate, Read, then Pop) ---
        try:
            suggestion_id_str = interaction.custom_id.split(':')[-1]
            uuid.UUID(suggestion_id_str) # Validate it
            suggestion_id = suggestion_id_str
        except (ValueError, IndexError):
            await interaction.response.send_message("Invalid suggestion ID format.", ephemeral=True)
            return
            
        # 1. Read and pop
        pending_data = await load_json(PENDING_SUGGESTIONS_FILE, pending_suggestions_lock)
        suggestion_data = pending_data.pop(suggestion_id, None)

        if not suggestion_data:
            await interaction.response.send_message("This suggestion has already been actioned or has expired.", ephemeral=True)
            return
            
        # 2. Save popped data
        await save_json(PENDING_SUGGESTIONS_FILE, pending_data, pending_suggestions_lock)
        # --- (End of Fix #4) ---

        suggester_id = suggestion_data['suggester_id']
        question_text = suggestion_data['question_text']

        # Edit original message
        embed = interaction.message.embeds[0]
        embed.title = "❌ Denied Suggestion"
        embed.color = discord.Color.red()
        embed.add_field(name="Action By", value=interaction.user.mention, inline=False)
        await interaction.message.edit(embed=embed, view=None)
        await interaction.response.send_message("Suggestion denied.", ephemeral=True)
        
        # DM Suggester
        try:
            suggester = await self.cog.bot.fetch_user(suggester_id)
            if suggester:
                await suggester.send(
                    f"Your 'Never have I ever...' suggestion in **{interaction.guild.name}** was denied.\n"
                    f"> ...{question_text}"
                )
        except (discord.Forbidden, discord.NotFound):
            pass # Can't DM them

class MainQuestionView(discord.ui.View):
    """The main persistent view for NHIE questions."""
    
    def __init__(self, cog: 'NeverHaveIEver'):
        super().__init__(timeout=None)
        self.cog = cog

    # --- (MODIFIED: Button style) ---
    @discord.ui.button(label='Suggest', style=discord.ButtonStyle.secondary, custom_id='nhie_suggest', row=0)
    async def suggest_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Opens the suggestion modal."""
        try:
            await interaction.response.send_modal(SuggestionModal(self.cog))
        except Exception as e:
            print(f"NHIE Suggest Button Error: {e}")
            try:
                await interaction.response.send_message(f"An error occurred: {e}", ephemeral=True)
            except:
                pass

    # --- (MODIFIED: Label and logic) ---
    @discord.ui.button(label='Recap', style=discord.ButtonStyle.secondary, custom_id='nhie_recap', row=0)
    async def recap_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Shows the recap view, starting on the last question."""
        await interaction.response.defer(ephemeral=True, thinking=True)
        
        guild_id = str(interaction.guild.id)
        
        history_data = (await load_json(HISTORY_FILE, history_lock)).get(guild_id, get_default_history())
        votes_data = await load_json(VOTES_FILE, votes_lock)
        
        if not history_data['questions']:
            await interaction.followup.send("There are no past questions to recap.", ephemeral=True)
            return
            
        # --- (MODIFIED: Start on last page) ---
        current_page = len(history_data['questions']) - 1
        # --- (END OF MODIFICATION) ---
            
        view = RecapView(
            cog=self.cog,
            history=history_data['questions'],
            all_votes=votes_data,
            page=current_page
        )
        embed = await view.get_page_embed()
        
        if embed is None:
             await interaction.followup.send("Could not load results data.", ephemeral=True)
             return
             
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label='Have', style=discord.ButtonStyle.success, custom_id='nhie_have', row=1)
    async def have_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Records a 'Have' vote."""
        await self.handle_vote(interaction, 'have')

    @discord.ui.button(label='Never', style=discord.ButtonStyle.danger, custom_id='nhie_never', row=1)
    async def never_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Records a 'Never' vote."""
        await self.handle_vote(interaction, 'never')
        
    # --- (CHANGE 1: Replaced handle_vote) ---
    async def handle_vote(self, interaction: discord.Interaction, vote_type: str):
        """Handles the logic for adding/changing a vote."""
        message_id = str(interaction.message.id)
        user_id = interaction.user.id
        
        votes_data = await load_json(VOTES_FILE, votes_lock)
        if message_id not in votes_data:
            votes_data[message_id] = {"have": [], "never": []}
            
        votes = votes_data[message_id]
        other_type = 'never' if vote_type == 'have' else 'have'
        
        if user_id in votes[other_type]:
            votes[other_type].remove(user_id)
            
        if user_id in votes[vote_type]:
            votes[vote_type].remove(user_id)
            await interaction.response.send_message("Your vote has been removed.", ephemeral=True)
        else:
            votes[vote_type].append(user_id)
            await interaction.response.send_message(f"You voted **{vote_type.title()}**.", ephemeral=True)
            
        await save_json(VOTES_FILE, votes_data, votes_lock)
        
        # Fetch usernames and build the lists
        try:
            embed = interaction.message.embeds[0]
            
            # Build "Have" user list
            have_users = []
            for uid in votes['have']:
                try:
                    user = self.cog.bot.get_user(uid)
                    if user is None:
                        user = await self.cog.bot.fetch_user(uid)
                    name = user.display_name
                    truncated_name = (name[:17] + '...') if len(name) > 20 else name
                    have_users.append(truncated_name)
                except discord.NotFound:
                    have_users.append(f"User {uid}")
            
            # Build "Never" user list
            never_users = []
            for uid in votes['never']:
                try:
                    user = self.cog.bot.get_user(uid)
                    if user is None:
                        user = await self.cog.bot.fetch_user(uid)
                    name = user.display_name
                    truncated_name = (name[:17] + '...') if len(name) > 20 else name
                    never_users.append(truncated_name)
                except discord.NotFound:
                    never_users.append(f"User {uid}")
            
            have_text = "\n".join(have_users) or "No one"
            never_text = "\n".join(never_users) or "No one"
            
            # Cap length to avoid embed errors
            if len(have_text) > 1020:
                have_text = have_text[:1020] + "..."
            if len(never_text) > 1020:
                never_text = never_text[:1020] + "..."
            
            # Update with just 2 fields for side-by-side display
            embed.clear_fields()
            embed.add_field(name="✅ Have", value=have_text, inline=True)
            embed.add_field(name="❌ Never", value=never_text, inline=True)
            
            await interaction.message.edit(embed=embed)
        except (IndexError, discord.HTTPException, AttributeError) as e:
            print(f"Failed to update vote counts on embed: {e}")
    # --- (END OF CHANGE 1) ---


class RecapView(discord.ui.View):
    """Paginated view for recapping past questions."""
    
    def __init__(self, cog: 'NeverHaveIEver', history: List[Dict], all_votes: Dict, page: int):
        super().__init__(timeout=180) # Ephemeral views should have a timeout
        self.cog = cog
        self.history = history # List of question dicts
        self.all_votes = all_votes # Dict of all votes {msg_id: {have:[], never:[]}}
        self.page = page
        self.update_buttons()

    async def get_page_embed(self) -> Optional[discord.Embed]:
        """Generates the embed for the current page."""
        if not (0 <= self.page < len(self.history)):
            return None
            
        question_data = self.history[self.page]
        question_text = question_data['question']
        suggester_id = question_data.get('suggester_id')
        
        all_msg_ids = question_data.get('all_message_ids', [])
        if not all_msg_ids and 'message_id' in question_data:
            all_msg_ids = [str(question_data.get('message_id'))]

        have_users_set = set()
        never_users_set = set()
        
        for msg_id in all_msg_ids:
            votes = self.all_votes.get(str(msg_id), {"have": [], "never": []})
            have_users_set.update(votes.get("have", []))
            never_users_set.update(votes.get("never", []))
            
        votes = {"have": list(have_users_set), "never": list(never_users_set)}
        
        embed = discord.Embed(
            title=f"Recap: Question {self.page + 1} / {len(self.history)}",
            description=f"**Never have I ever...**\n{question_text}",
            color=discord.Color.blue()
        )
        
        # --- (MODIFIED: Fetch display names, no mentions, truncate) ---
        have_users = []
        for user_id in votes['have']:
            try:
                # Try to get member from guild first for nickname, fall back to user
                user = self.cog.bot.get_user(user_id)
                if user is None:
                    user = await self.cog.bot.fetch_user(user_id)
                
                name = user.display_name
                # Truncate name to 20 chars
                truncated_name = (name[:17] + '...') if len(name) > 20 else name
                have_users.append(truncated_name)
            except discord.NotFound:
                have_users.append(f"User {user_id}")
                
        never_users = []
        for user_id in votes['never']:
            try:
                user = self.cog.bot.get_user(user_id)
                if user is None:
                    user = await self.cog.bot.fetch_user(user_id)
                    
                name = user.display_name
                # Truncate name to 20 chars
                truncated_name = (name[:17] + '...') if len(name) > 20 else name
                never_users.append(truncated_name)
            except discord.NotFound:
                never_users.append(f"User {user_id}")

        have_text = "\n".join(have_users) or "No one"
        never_text = "\n".join(never_users) or "No one"
        
        if len(have_text) > 1020:
            have_text = have_text[:1020] + "..."
        if len(never_text) > 1020:
            never_text = never_text[:1020] + "..."
            
        # --- (CHANGE 3: Replaced field layout) ---
        # Two fields for side-by-side display
        embed.add_field(name="✅ Have", value=have_text, inline=True)
        embed.add_field(name="❌ Never", value=never_text, inline=True)
        # --- (END OF CHANGE 3) ---
        
        if suggester_id:
            try:
                suggester = await self.cog.bot.fetch_user(suggester_id)
                embed.set_footer(text=f"Suggested by: {suggester.display_name}")
            except discord.NotFound:
                embed.set_footer(text="Suggested by: an unknown user")
                
        return embed

    def update_buttons(self):
        """Enables/disables page buttons based on current page."""
        self.prev_button.disabled = self.page == 0
        self.next_button.disabled = self.page == len(self.history) - 1

    async def update_message(self, interaction: discord.Interaction):
        """Edits the interaction message with the new page."""
        self.update_buttons()
        embed = await self.get_page_embed()
        if embed:
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.edit_message(content="No data for this page.", embed=None, view=self)

    @discord.ui.button(label='Previous', style=discord.ButtonStyle.primary, custom_id='recap_prev')
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            await self.update_message(interaction)

    @discord.ui.button(label='Next', style=discord.ButtonStyle.primary, custom_id='recap_next')
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < len(self.history) - 1:
            self.page += 1
            await self.update_message(interaction)


class QuestionViewerView(discord.ui.View):
    """Paginated view for admins to see and delete questions."""
    
    QUESTIONS_PER_PAGE = 10
    
    def __init__(self, cog: 'NeverHaveIEver', question_pool: List[Dict]):
        super().__init__(timeout=180)
        self.cog = cog
        self.question_pool = question_pool
        self.page = 0
        self.max_pages = max(0, (len(self.question_pool) - 1) // self.QUESTIONS_PER_PAGE)
        self.update_buttons()
    
    def get_page_embed(self) -> discord.Embed:
        """Generates the embed for the current page of questions."""
        start_index = self.page * self.QUESTIONS_PER_PAGE
        end_index = start_index + self.QUESTIONS_PER_PAGE
        page_questions = self.question_pool[start_index:end_index]
        
        if not page_questions and self.page == 0:
            return discord.Embed(
                title="Question Pool",
                description="The question pool is empty.",
                color=discord.Color.orange()
            )
            
        embed = discord.Embed(
            title=f"Question Pool (Page {self.page + 1} / {self.max_pages + 1})",
            color=discord.Color.blue()
        )
        
        description = ""
        for i, q_data in enumerate(page_questions):
            q_num = start_index + i + 1
            q_text = q_data['question']
            description += f"**{q_num}.** {q_text}\n"
            
        embed.description = description
        embed.set_footer(text=f"Total questions: {len(self.question_pool)}")
        return embed

    def update_buttons(self):
        """Enables/disables page buttons."""
        self.prev_page.disabled = self.page == 0
        self.next_page.disabled = self.page == self.max_pages
        self.delete_question.disabled = len(self.question_pool) == 0
        
    async def update_message(self, interaction: discord.Interaction):
        """Shared logic to edit the message with the new state."""
        # Refresh question pool in case it was modified
        guild_id = str(interaction.guild.id)
        questions_data = await load_json(QUESTIONS_FILE, questions_lock)
        self.question_pool = questions_data.get(guild_id, get_default_questions())['pool']
        self.max_pages = max(0, (len(self.question_pool) - 1) // self.QUESTIONS_PER_PAGE)
        
        # Fix page number if questions were deleted
        if self.page > self.max_pages:
            self.page = self.max_pages
        if self.page < 0:
            self.page = 0
            
        self.update_buttons()
        embed = self.get_page_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label='Previous Page', style=discord.ButtonStyle.primary, custom_id='qview_prev')
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            await self.update_message(interaction)

    @discord.ui.button(label='Next Page', style=discord.ButtonStyle.primary, custom_id='qview_next')
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.max_pages:
            self.page += 1
            await self.update_message(interaction)
            
    @discord.ui.button(label='Delete Question', style=discord.ButtonStyle.danger, custom_id='qview_delete')
    async def delete_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Opens the delete confirmation modal."""
        await interaction.response.send_modal(ConfirmDeleteModal(self.cog, self))


class AdminPanelView(discord.ui.View):
    """The main admin panel, with buttons for all settings."""
    
    def __init__(self, cog: 'NeverHaveIEver'):
        super().__init__(timeout=None) # Persistent
        self.cog = cog

    async def get_panel_embed(self, guild: discord.Guild) -> discord.Embed:
        """Generates the embed for the admin panel, showing current settings."""
        guild_id = str(guild.id)
        settings = (await load_json(SETTINGS_FILE, settings_lock)).get(guild_id, get_default_settings())
        
        # Get channel names
        post_channels = []
        for ch_id in settings['post_channels']:
            ch = guild.get_channel(ch_id)
            post_channels.append(ch.mention if ch else f"`Deleted Channel {ch_id}`")
        post_channels_str = "\n".join(post_channels) or "Not set"
        
        # Get log channel
        log_ch = guild.get_channel(settings['log_channel']) if settings['log_channel'] else None
        log_channel_str = log_ch.mention if log_ch else "Not set"
        
        # Get trigger role
        trigger_role = guild.get_role(settings['trigger_role']) if settings['trigger_role'] else None
        trigger_role_str = trigger_role.mention if trigger_role else "Not set"
        
        # Get cooldown
        cooldown_str = f"{settings['cooldown_hours']} hours"
        
        # Get question count
        q_data = (await load_json(QUESTIONS_FILE, questions_lock)).get(guild_id, get_default_questions())
        question_count = len(q_data['pool'])

        embed = discord.Embed(
            title="NHIE Admin Panel",
            description="Manage the 'Never Have I Ever' bot settings.",
            color=discord.Color.dark_orange()
        )
        embed.add_field(name="🖥️ Post Channels", value=post_channels_str, inline=False)
        embed.add_field(name="📋 Log Channel", value=log_channel_str, inline=True)
        embed.add_field(name="👤 Trigger Role", value=trigger_role_str, inline=True)
        embed.add_field(name="⏰ Cooldown", value=cooldown_str, inline=True)
        embed.add_field(name="❓ Question Pool", value=f"{question_count} questions", inline=True)
        
        embed.set_footer(text="Click the buttons below to manage settings.")
        return embed

    async def update_panel(self, message: discord.Message): # (Fix #5)
        """Updates the panel embed after a setting is changed."""
        try:
            embed = await self.get_panel_embed(message.guild)
            await message.edit(embed=embed)
        except (discord.HTTPException, AttributeError) as e:
            print(f"Failed to update admin panel: {e}")

    # --- ( ROW 1: CHANNEL/ROLE SETTINGS ) ---
    # --- (FIX: Added custom_id to all buttons in this View) ---
    @discord.ui.button(label='Set Post Channels', style=discord.ButtonStyle.secondary, row=0, custom_id='nhie_admin_post_ch')
    async def set_post_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Sends a ChannelSelect to pick post channels."""
        view = discord.ui.View()
        select = discord.ui.ChannelSelect(
            custom_id="nhie_post_ch_select",
            placeholder="Select channels to post questions in...",
            min_values=1,
            max_values=10, # Max 10 channels
            channel_types=[discord.ChannelType.text]
        )
        view.add_item(select)
        
        async def select_callback(select_interaction: discord.Interaction):
            try:
                guild_id = str(select_interaction.guild.id)
                channel_ids = [ch.id for ch in select.values]
                
                settings_data = await load_json(SETTINGS_FILE, settings_lock)
                if guild_id not in settings_data:
                    settings_data[guild_id] = get_default_settings()
                
                settings_data[guild_id]['post_channels'] = channel_ids
                await save_json(SETTINGS_FILE, settings_data, settings_lock)
                
                await select_interaction.response.send_message(
                    f"Post channels set to: {', '.join(ch.mention for ch in select.values)}",
                    ephemeral=True
                )
                await self.update_panel(interaction.message) # (Fix #5)
            except Exception as e:
                print(f"NHIE: Error in set_post_channels callback: {e}")
                try:
                    await select_interaction.response.send_message("An error occurred. Could not save settings.", ephemeral=True)
                except discord.HTTPException:
                    pass
            
        select.callback = select_callback
        await interaction.response.send_message("Select the channels where questions should be posted:", view=view, ephemeral=True)

    @discord.ui.button(label='Set Log Channel', style=discord.ButtonStyle.secondary, row=0, custom_id='nhie_admin_log_ch')
    async def set_log_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Sends a ChannelSelect for the log channel."""
        view = discord.ui.View()
        select = discord.ui.ChannelSelect(
            custom_id="nhie_log_ch_select",
            placeholder="Select one channel for suggestions...",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text]
        )
        view.add_item(select)
        
        async def select_callback(select_interaction: discord.Interaction):
            try:
                guild_id = str(select_interaction.guild.id)
                log_channel = select.values[0]
                
                settings_data = await load_json(SETTINGS_FILE, settings_lock)
                if guild_id not in settings_data:
                    settings_data[guild_id] = get_default_settings()
                
                settings_data[guild_id]['log_channel'] = log_channel.id
                await save_json(SETTINGS_FILE, settings_data, settings_lock)
                
                await select_interaction.response.send_message(
                    f"Suggestion log channel set to: {log_channel.mention}",
                    ephemeral=True
                )
                await self.update_panel(interaction.message) # (Fix #5)
            except Exception as e:
                print(f"NHIE: Error in set_log_channel callback: {e}")
                try:
                    await select_interaction.response.send_message("An error occurred. Could not save settings.", ephemeral=True)
                except discord.HTTPException:
                    pass
            
        select.callback = select_callback
        await interaction.response.send_message("Select the channel for suggestion approvals:", view=view, ephemeral=True)

    @discord.ui.button(label='Set Trigger Role', style=discord.ButtonStyle.secondary, row=0, custom_id='nhie_admin_trigger_role')
    async def set_trigger_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Sends a RoleSelect for the trigger role."""
        view = discord.ui.View()
        select = discord.ui.RoleSelect(
            custom_id="nhie_role_select",
            placeholder="Select one role to trigger questions...",
            min_values=1,
            max_values=1
        )
        view.add_item(select)
        
        async def select_callback(select_interaction: discord.Interaction):
            try:
                guild_id = str(select_interaction.guild.id)
                trigger_role = select.values[0]
                
                settings_data = await load_json(SETTINGS_FILE, settings_lock)
                if guild_id not in settings_data:
                    settings_data[guild_id] = get_default_settings()
                
                settings_data[guild_id]['trigger_role'] = trigger_role.id
                await save_json(SETTINGS_FILE, settings_data, settings_lock)
                
                await select_interaction.response.send_message(
                    f"Trigger role set to: {trigger_role.mention}",
                    ephemeral=True
                )
                await self.update_panel(interaction.message) # (Fix #5)
            except Exception as e:
                print(f"NHIE: Error in set_trigger_role callback: {e}")
                try:
                    await select_interaction.response.send_message("An error occurred. Could not save settings.", ephemeral=True)
                except discord.HTTPException:
                    pass
            
        select.callback = select_callback
        await interaction.response.send_message("Select the role that triggers new questions:", view=view, ephemeral=True)

    @discord.ui.button(label='Remove Trigger Role', style=discord.ButtonStyle.danger, row=0, custom_id='nhie_admin_rem_trigger')
    async def remove_trigger_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        settings_data = await load_json(SETTINGS_FILE, settings_lock)
        if guild_id in settings_data:
            settings_data[guild_id]['trigger_role'] = None
            await save_json(SETTINGS_FILE, settings_data, settings_lock)
        
        await interaction.response.send_message("Trigger role removed.", ephemeral=True)
        await self.update_panel(interaction.message) # (Fix #5)

    # --- ( ROW 2: QUESTION MANAGEMENT ) ---
    @discord.ui.button(label='Add Questions', style=discord.ButtonStyle.success, row=1, custom_id='nhie_admin_add_q')
    async def add_questions(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddQuestionsModal(self.cog))

    @discord.ui.button(label='View/Delete Questions', style=discord.ButtonStyle.primary, row=1, custom_id='nhie_admin_view_q')
    async def view_questions(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild_id = str(interaction.guild.id)
        q_data = (await load_json(QUESTIONS_FILE, questions_lock)).get(guild_id, get_default_questions())
        
        view = QuestionViewerView(self.cog, q_data['pool'])
        embed = view.get_page_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    # --- ( ROW 3: BOT SETTINGS ) ---
    @discord.ui.button(label='Set Cooldown', style=discord.ButtonStyle.secondary, row=2, custom_id='nhie_admin_cooldown')
    async def set_cooldown(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetCooldownModal(self.cog))

    @discord.ui.button(label='Post Question Now', style=discord.ButtonStyle.success, row=2, custom_id='nhie_admin_post_now')
    async def post_now(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Manually posts a question, bypassing cooldown."""
        await interaction.response.defer(ephemeral=True)
        success = await self.cog.post_question(interaction.guild, force=True)
        if success:
            await interaction.followup.send("A new question has been posted.", ephemeral=True)
        else:
            await interaction.followup.send("Could not post a question. Check settings and question pool.", ephemeral=True)


# --- ( MAIN COG CLASS ) ---

class NeverHaveIEver(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.main_view = MainQuestionView(self)
        self.admin_view = AdminPanelView(self)
        self.post_lock = asyncio.Lock()
        
        # --- (CRITICAL FIX: Per-guild flag) ---
        self.delay_tasks_pending: Dict[str, bool] = {}
        # --- (END OF FIX) ---
        
    async def cog_load(self):
        """Called when the cog is loaded."""
        # Register persistent views here, not in __init__
        self.bot.add_view(self.main_view)
        self.bot.add_view(self.admin_view)

        # Ensure data files exist
        await load_json(SETTINGS_FILE, settings_lock)
        await load_json(QUESTIONS_FILE, questions_lock)
        await load_json(HISTORY_FILE, history_lock)
        await load_json(VOTES_FILE, votes_lock)
        await load_json(PENDING_SUGGESTIONS_FILE, pending_suggestions_lock)
        
        # (Fix #5): Start the task loop here
        self.cleanup_task.start()

    async def cog_unload(self):
        """Called when the cog is unloaded."""
        self.cleanup_task.cancel()

    # --- (Fix #5 & #6: Cleanup Task) ---
    @tasks.loop(hours=24)
    async def cleanup_task(self):
        print("NHIE: Running daily data cleanup...")
        
        try:
            cutoff = (datetime.now() - timedelta(days=30)).timestamp()
            
            votes_data = await load_json(VOTES_FILE, votes_lock)
            history_data = await load_json(HISTORY_FILE, history_lock)
            pending_data = await load_json(PENDING_SUGGESTIONS_FILE, pending_suggestions_lock)
            
            active_message_ids = set()
            cleaned_history = {}
            history_count = 0
            
            # Clean history
            for guild_id, data in history_data.items():
                cleaned_questions = [
                    q for q in data.get('questions', []) 
                    if q.get('timestamp', 0) > cutoff
                ]
                history_count += len(cleaned_questions)
                if cleaned_questions:
                    cleaned_history[guild_id] = {"questions": cleaned_questions}
                    for q in cleaned_questions:
                        active_message_ids.update(q.get('all_message_ids', []))
                        if 'message_id' in q: # Backwards compatibility
                            active_message_ids.add(q['message_id'])
            
            # Clean votes
            cleaned_votes = {
                msg_id: votes for msg_id, votes in votes_data.items() 
                if msg_id in active_message_ids
            }
            
            # Clean pending suggestions (older than 3 days)
            pending_cutoff = (datetime.now() - timedelta(days=3)).timestamp()
            cleaned_pending = {
                sid: data for sid, data in pending_data.items()
                if data.get('timestamp', float('inf')) > pending_cutoff
            }
            
            await save_json(HISTORY_FILE, cleaned_history, history_lock)
            await save_json(VOTES_FILE, cleaned_votes, votes_lock)
            await save_json(PENDING_SUGGESTIONS_FILE, cleaned_pending, pending_suggestions_lock)
            
            print(f"NHIE Cleanup: Retained {history_count} history entries, {len(cleaned_votes)} vote entries, and {len(cleaned_pending)} pending suggestions.")

        except Exception as e:
            print(f"NHIE: Error during data cleanup: {e}")
            
    @cleanup_task.before_loop
    async def before_cleanup_task(self):
        # (Fix #5): Wait until bot is ready
        await self.bot.wait_until_ready()
    # --- (End of Fix #5 & #6) ---

    # --- ( SLASH COMMANDS ) ---
    
    nhie_group = app_commands.Group(
        name="neverhaveiever",
        description="Admin commands for Never Have I Ever."
    )

    @nhie_group.command(name="admin_panel")
    async def admin_panel(self, interaction: discord.Interaction):
        """Sends the persistent admin panel."""
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        embed = await self.admin_view.get_panel_embed(interaction.guild)
        await interaction.response.send_message(
            "Here is the admin panel. This message will stay active.",
            embed=embed,
            view=self.admin_view
        )

    # --- (MODIFIED: Removed post_now_cmd) ---

    @admin_panel.error
    # --- (MODIFIED: Removed @post_now_cmd.error) ---
    async def on_nhie_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Error handler for NHIE commands."""
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("You must be an administrator to use this command.", ephemeral=True)
        else:
            await interaction.response.send_message(f"An unexpected error occurred: {error}", ephemeral=True)

    # --- ( EVENT LISTENERS ) ---
    
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Listens for role changes to trigger a question."""
        guild_id = str(after.guild.id)
        settings = (await load_json(SETTINGS_FILE, settings_lock)).get(guild_id)
        
        if not settings:
            return # Not configured for this guild
            
        trigger_role_id = settings.get('trigger_role')
        if not trigger_role_id:
            return # No trigger role set
            
        role_added = trigger_role_id not in [r.id for r in before.roles] and \
                     trigger_role_id in [r.id for r in after.roles]
                     
        if role_added:
            # --- (NEW: Check main cooldown FIRST) ---
            cooldown = timedelta(hours=settings.get('cooldown_hours', 1.0))
            last_post_time = datetime.fromtimestamp(settings.get('last_post_time', 0.0))
            if datetime.now() - last_post_time < cooldown:
                # Cooldown is active, do nothing, as requested.
                return
            # --- (END OF NEW BLOCK) ---

            # --- (CRITICAL FIX: Per-guild flag check) ---
            if self.delay_tasks_pending.get(guild_id, False):
                print(f"NHIE: Role added in {after.guild.name}, but a 30s post task is already pending. Ignoring.")
                return
                
            print(f"Trigger role {trigger_role_id} added to {after.name} in {after.guild.name}. Waiting 30 seconds...")
            self.delay_tasks_pending[guild_id] = True
            # Don't block the event listener. Create a new task.
            asyncio.create_task(self.delayed_post_question(after.guild, 30))
            # --- (END OF FIX) ---

    # --- ( CORE LOGIC ) ---

    async def delayed_post_question(self, guild: discord.Guild, delay_seconds: int):
        """Waits for a delay, then attempts to post a question."""
        # --- (CRITICAL FIX: Per-guild flag) ---
        guild_id = str(guild.id)
        try:
            await asyncio.sleep(delay_seconds)
            print(f"NHIE: Delay finished. Attempting to post in {guild.name}...")
            await self.post_question(guild, force=False)
        except Exception as e:
            print(f"NHIE: Error in delayed_post_question: {e}")
        finally:
            # Reset the flag for THIS guild
            self.delay_tasks_pending[guild_id] = False
            print(f"NHIE: Delay task finished for {guild.name}. Trigger is re-armed.")
        # --- (END OF FIX) ---

    async def post_question(self, guild: discord.Guild, force: bool = False) -> bool:
        """The main logic to post a new question."""
        guild_id = str(guild.id)
        
        # --- (Minor Fix #2: Optimized Settings Load) ---
        last_post_time = None  # Define here
        
        async with self.post_lock:
            settings_data = await load_json(SETTINGS_FILE, settings_lock)
            settings = settings_data.get(guild_id)
            
            if not settings or not settings.get('post_channels'):
                print(f"NHIE: Aborted post in {guild.name}: No settings or post channels.")
                return False
            
            # Now get the last post time
            last_post_time = datetime.fromtimestamp(settings.get('last_post_time', 0.0))
            
            if not force:
                cooldown = timedelta(hours=settings.get('cooldown_hours', 1.0))
                if datetime.now() - last_post_time < cooldown:
                    print(f"NHIE: Aborted post in {guild.name}: Still on cooldown.")
                    return False # On cooldown
            
            # Update last post time *immediately* to prevent race conditions
            settings['last_post_time'] = datetime.now().timestamp()
            await save_json(SETTINGS_FILE, settings_data, settings_lock)
        # --- (End of Minor Fix #2) ---

        # --- ( 2. Get a Question ) ---
        questions_data = await load_json(QUESTIONS_FILE, questions_lock)
        q_pool = questions_data.get(guild_id, get_default_questions())['pool']
        
        if not q_pool:
            print(f"NHIE: Aborted post in {guild.name}: Question pool is empty.")
            # Roll back the timestamp update since we failed
            async with self.post_lock:
                settings_data = await load_json(SETTINGS_FILE, settings_lock)
                if settings_data.get(guild_id):
                    if last_post_time is not None: # Check if it was set
                        settings_data[guild_id]['last_post_time'] = last_post_time.timestamp()
                        await save_json(SETTINGS_FILE, settings_data, settings_lock)
            return False
            
        # Select and remove question
        question_obj = q_pool.pop(random.randrange(len(q_pool)))
        question_text = question_obj['question']
        suggester_id = question_obj.get('suggester_id')
        
        # --- ( 3. Prepare Embed ) ---
        
        embed = discord.Embed(
            title="Never Have I Ever...",
            description=f"**...{question_text}**",
            color=discord.Color.random()
        )
        
        # --- (CHANGE 2: Replaced field layout) ---
        # Two fields for side-by-side display
        embed.add_field(name="✅ Have", value="No one", inline=True)
        embed.add_field(name="❌ Never", value="No one", inline=True)
        # --- (END OF CHANGE 2) ---
        
        if suggester_id:
            try:
                suggester = await self.bot.fetch_user(suggester_id)
                embed.set_footer(text=f"Suggested by: {suggester.display_name}")
            except discord.NotFound:
                pass # User left or deleted

        # --- ( 4. Post to Channels - (Fix #1 format) ) ---
        post_channels_ids = settings.get('post_channels', [])
        all_message_ids = []
        
        for ch_id in post_channels_ids:
            channel = guild.get_channel(ch_id)
            if channel and isinstance(channel, discord.TextChannel):
                try:
                    msg = await channel.send(embed=embed, view=self.main_view)
                    all_message_ids.append(str(msg.id))
                except discord.Forbidden:
                    print(f"NHIE: Failed to post in {channel.name} (ID: {ch_id}): No permissions.")
                except Exception as e:
                    print(f"NHIE: Failed to post in {channel.name}: {e}")
            else:
                print(f"NHIE: Invalid post channel (ID: {ch_id}) in {guild.name}")

        if not all_message_ids:
            print(f"NHIE: Failed to post in any channel in {guild.name}.")
            # Add question back to pool since it failed
            q_pool.append(question_obj)
            await save_json(QUESTIONS_FILE, questions_data, questions_lock) # Save the reverted pool
            
            # Roll back the timestamp
            async with self.post_lock:
                settings_data = await load_json(SETTINGS_FILE, settings_lock)
                if settings_data.get(guild_id):
                    if last_post_time is not None:
                        settings_data[guild_id]['last_post_time'] = last_post_time.timestamp()
                        await save_json(SETTINGS_FILE, settings_data, settings_lock)
            return False
            
        # --- ( 5. Update Data Files ) ---
        
        # Save question pool (since we popped a question)
        await save_json(QUESTIONS_FILE, questions_data, questions_lock)
        
        # (Timestamp already saved)
        
        # Add to history (Fix #1 format)
        history_data = await load_json(HISTORY_FILE, history_lock)
        if guild_id not in history_data:
            history_data[guild_id] = get_default_history()
            
        history_data[guild_id]['questions'].append({
            "all_message_ids": all_message_ids, # Store ALL IDs
            "question": question_text,
            "suggester_id": suggester_id,
            "timestamp": datetime.now().timestamp()
        })
        await save_json(HISTORY_FILE, history_data, history_lock)
        
        # --- (Fix #3: Initialize votes for all new messages) ---
        votes_data = await load_json(VOTES_FILE, votes_lock)
        for msg_id in all_message_ids:
            if msg_id not in votes_data:
                votes_data[msg_id] = {"have": [], "never": []}
        await save_json(VOTES_FILE, votes_data, votes_lock)
        # --- (End of Fix #3) ---
        
        print(f"NHIE: Successfully posted new question in {guild.name}")
        return True
        

# --- ( COG SETUP ) ---
async def setup(bot: commands.Bot):
    # We must ensure the bot has the necessary intents
    if not bot.intents.members:
        print("WARNING: 'NeverHaveIEver' cog requires the 'members' intent to be enabled.")
    
    await bot.add_cog(NeverHaveIEver(bot))
    print("'NeverHaveIEver' cog loaded successfully.")


