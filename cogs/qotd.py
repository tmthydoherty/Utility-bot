import discord
from discord.ext import commands, tasks
from discord import app_commands
import sqlite3
import datetime
import pytz
import os
import random
from typing import Optional, List, Tuple
import io
import math
import json
import asyncio

# --- Configuration ---
DB_FILE = "qotd_database.db"

# A whitelist of setting keys to prevent SQL injection
ALLOWED_SETTINGS_KEYS = [
    'enabled', 'source_channel_id', 'source_bot_id', 'post_channel_ids',
    'ping_role_id', 'suggestion_log_channel_id', 'post_time', 'timezone',
    'auto_thread', 'last_post_timestamp'
]

# --- Database Setup and Helpers ---
def db_init():
    """Initializes the database. This version is safe and will not fail on load."""
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS questions (id INTEGER PRIMARY KEY AUTOINCREMENT, question_text TEXT NOT NULL UNIQUE, added_by_id INTEGER, last_used_timestamp INTEGER DEFAULT 0, times_used INTEGER DEFAULT 0)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS guild_settings (guild_id INTEGER PRIMARY KEY, enabled BOOLEAN DEFAULT FALSE, source_channel_id INTEGER, source_bot_id INTEGER, post_channel_ids TEXT DEFAULT '[]', ping_role_id INTEGER, suggestion_log_channel_id INTEGER, post_time TEXT DEFAULT '10:00', timezone TEXT DEFAULT 'UTC', auto_thread BOOLEAN DEFAULT TRUE)''')
    
    # Add last_post_timestamp column for robust task looping
    try:
        cur.execute("ALTER TABLE guild_settings ADD COLUMN last_post_timestamp INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column likely already exists

    cur.execute('''CREATE TABLE IF NOT EXISTS suggestions (id INTEGER PRIMARY KEY AUTOINCREMENT, question_text TEXT NOT NULL, suggester_id INTEGER NOT NULL, guild_id INTEGER NOT NULL, status TEXT DEFAULT 'pending', review_message_id INTEGER)''')
    con.commit()
    con.close()

def get_guild_settings(guild_id: int):
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM guild_settings WHERE guild_id = ?", (guild_id,))
    settings = cur.fetchone()
    if not settings:
        cur.execute("INSERT INTO guild_settings (guild_id) VALUES (?)", (guild_id,))
        con.commit()
        cur.execute("SELECT * FROM guild_settings WHERE guild_id = ?", (guild_id,))
        settings = cur.fetchone()
    con.close()
    return dict(settings) if settings else None

def update_guild_setting(guild_id: int, key: str, value):
    # CRITICAL: Validate key against a whitelist to prevent SQL injection
    if key not in ALLOWED_SETTINGS_KEYS:
        print(f"CRITICAL: Attempted to update invalid setting key: {key}")
        return

    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute(f"UPDATE guild_settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
    con.commit()
    con.close()

def get_question_counts():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM questions")
    total_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM questions WHERE last_used_timestamp = 0")
    unseen_count = cur.fetchone()[0]
    con.close()
    return total_count, unseen_count

# --- Modals for User Input ---

class ReasonModal(discord.ui.Modal, title="Reason for Decision"):
    reason = discord.ui.TextInput(label="Reason", style=discord.TextStyle.paragraph, required=True, max_length=512)
    def __init__(self, original_interaction: discord.Interaction, decision: str): super().__init__(); self.original_interaction = original_interaction; self.decision = decision
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
        except discord.HTTPException:
            pass # Failed to defer, but proceed anyway

class SuggestionModal(discord.ui.Modal, title="Suggest a Question"):
    question = discord.ui.TextInput(label="Your Question Suggestion", style=discord.TextStyle.paragraph, required=True, max_length=256)
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.HTTPException as e:
            print(f"Failed to defer suggestion modal: {e}")

class AddQuestionModal(discord.ui.Modal, title="Add Questions in Bulk"):
    question_text = discord.ui.TextInput(label="Questions (one per line)", style=discord.TextStyle.paragraph, required=True, placeholder="What is your favorite movie and why?\nIf you could have any superpower, what would it be?")
    def __init__(self, cog, panel_message: discord.Message):
        super().__init__(); self.cog = cog; self.panel_message = panel_message
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
            questions_to_add = [q.strip() for q in self.question_text.value.split('\n') if q.strip()]
            if not questions_to_add:
                await interaction.followup.send("No valid questions provided.", ephemeral=True)
                return
            
            added, skipped = 0, 0
            con = sqlite3.connect(DB_FILE); cur = con.cursor()
            for q in questions_to_add:
                try:
                    cur.execute("INSERT INTO questions (question_text, added_by_id) VALUES (?, ?)", (q, interaction.user.id))
                    added += 1
                except sqlite3.IntegrityError:
                    skipped += 1
            con.commit(); con.close()

            summary = f"✅ Added **{added}** questions."
            if skipped > 0: summary += f"\nℹ️ Skipped **{skipped}** duplicates."
            
            await interaction.followup.send(summary, ephemeral=True)
            await self.cog.update_admin_panel(self.panel_message)
        
        except discord.HTTPException as e:
            print(f"Failed to respond in AddQuestionModal: {e}")
        except Exception as e:
            print(f"Error in AddQuestionModal.on_submit: {e}")
            try:
                await interaction.followup.send("An unexpected error occurred.", ephemeral=True)
            except discord.HTTPException:
                pass # Can't even send error

# --- Single-Purpose Setting Modals ---

class PingRoleModal(discord.ui.Modal, title="Set Ping Role"):
    role_id = discord.ui.TextInput(label="Ping Role ID", required=False, placeholder="Enter Role ID or leave blank to remove")
    def __init__(self, current_value: Optional[int]):
        super().__init__()
        self.role_id.default = str(current_value) if current_value else ""
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
            value = self.role_id.value.strip()
            if not value:
                update_guild_setting(interaction.guild.id, 'ping_role_id', None)
                await interaction.followup.send("✅ Ping role has been removed.", ephemeral=True)
            elif value.isdigit():
                update_guild_setting(interaction.guild.id, 'ping_role_id', int(value))
                await interaction.followup.send(f"✅ Ping role ID set to `{value}`.", ephemeral=True)
            else:
                await interaction.followup.send("❌ That is not a valid ID. Please provide a numerical role ID.", ephemeral=True)
        except discord.HTTPException as e:
            print(f"Failed to respond in PingRoleModal: {e}")

class SuggestionChannelModal(discord.ui.Modal, title="Set Suggestion Log Channel"):
    channel_id = discord.ui.TextInput(label="Channel ID for Suggestion Logs", required=False, placeholder="Enter Channel ID or leave blank to remove")
    def __init__(self, current_value: Optional[int]):
        super().__init__()
        self.channel_id.default = str(current_value) if current_value else ""
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
            value = self.channel_id.value.strip()
            if not value:
                update_guild_setting(interaction.guild.id, 'suggestion_log_channel_id', None)
                await interaction.followup.send("✅ Suggestion log channel has been removed.", ephemeral=True)
            elif value.isdigit():
                update_guild_setting(interaction.guild.id, 'suggestion_log_channel_id', int(value))
                await interaction.followup.send(f"✅ Suggestion log channel ID set to `{value}`.", ephemeral=True)
            else:
                await interaction.followup.send("❌ That is not a valid ID. Please provide a numerical channel ID.", ephemeral=True)
        except discord.HTTPException as e:
            print(f"Failed to respond in SuggestionChannelModal: {e}")

class PostTimeModal(discord.ui.Modal, title="Set Post Time"):
    post_time = discord.ui.TextInput(label="Post Time (HH:MM 24-hr format)", min_length=5, max_length=5)
    def __init__(self, current_value: str):
        super().__init__()
        self.post_time.default = current_value
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
            datetime.datetime.strptime(self.post_time.value, '%H:%M')
            update_guild_setting(interaction.guild.id, 'post_time', self.post_time.value)
            await interaction.followup.send(f"✅ Post time set to `{self.post_time.value}`.", ephemeral=True)
        except ValueError:
            await interaction.followup.send("❌ Invalid time format. Please use HH:MM.", ephemeral=True)
        except discord.HTTPException as e:
            print(f"Failed to respond in PostTimeModal: {e}")

class TimezoneModal(discord.ui.Modal, title="Set Timezone"):
    timezone = discord.ui.TextInput(label="Timezone", placeholder="e.g., US/Central, Europe/London")
    def __init__(self, current_value: str):
        super().__init__()
        self.timezone.default = current_value
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
            if self.timezone.value in pytz.all_timezones:
                update_guild_setting(interaction.guild.id, 'timezone', self.timezone.value)
                await interaction.followup.send(f"✅ Timezone set to `{self.timezone.value}`.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Invalid timezone. A list can be found online.", ephemeral=True)
        except discord.HTTPException as e:
            print(f"Failed to respond in TimezoneModal: {e}")


# --- Interactive Views ---

class QuestionPagesView(discord.ui.View):
    # This view is ephemeral, timeout=None removed
    def __init__(self, questions: list):
        super().__init__(timeout=180) 
        self.questions = questions; self.current_page = 0; self.per_page = 5
        self.total_pages = math.ceil(len(self.questions) / self.per_page); self.update_buttons()
    
    def create_page_embed(self) -> discord.Embed:
        start_index = self.current_page * self.per_page; end_index = start_index + self.per_page; page_questions = self.questions[start_index:end_index]
        embed = discord.Embed(title=f"Question Pool (Page {self.current_page + 1}/{self.total_pages})", color=discord.Color.blue())
        description = "".join([f"**ID: {q_id}**\n> {q_text}\n\n" for q_id, q_text in page_questions]) if page_questions else "No questions on this page."
        embed.description = description; embed.set_footer(text=f"Showing questions {start_index + 1}-{min(end_index, len(self.questions))} of {len(self.questions)}")
        return embed
    
    def update_buttons(self):
        self.first_page.disabled = self.current_page == 0; self.prev_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page >= self.total_pages - 1; self.last_page.disabled = self.current_page >= self.total_pages - 1
    
    async def update_message(self, interaction: discord.Interaction):
        self.update_buttons(); embed = self.create_page_embed(); await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="|<", style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button): self.current_page = 0; await self.update_message(interaction)
    @discord.ui.button(label="<", style=discord.ButtonStyle.primary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button): self.current_page -= 1; await self.update_message(interaction)
    @discord.ui.button(label=">", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button): self.current_page += 1; await self.update_message(interaction)
    @discord.ui.button(label=">|", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button): self.current_page = self.total_pages - 1; await self.update_message(interaction)

class DeleteQuestionView(discord.ui.View):
    # This view is ephemeral, timeout=None removed
    def __init__(self, questions: List[Tuple[int, str]], cog, panel_message: discord.Message):
        super().__init__(timeout=180) 
        self.questions = questions; self.cog = cog; self.panel_message = panel_message
        self.current_page = 0; self.per_page = 5; self.selected_question_id = None
        self.recalculate_pages(); self.update_components()
    
    def recalculate_pages(self):
        self.total_pages = math.ceil(len(self.questions) / self.per_page) if self.questions else 1
        if self.current_page >= self.total_pages and self.total_pages > 0: self.current_page = self.total_pages - 1
    
    def create_page_embed(self) -> discord.Embed:
        start_index = self.current_page * self.per_page; end_index = start_index + self.per_page; page_questions = self.questions[start_index:end_index]
        embed = discord.Embed(title=f"Delete a Question (Page {self.current_page + 1}/{self.total_pages})", color=discord.Color.red())
        description = "".join([f"**ID: {q_id}**\n> {q_text}\n\n" for q_id, q_text in page_questions]) if page_questions else "No questions left to delete."
        embed.description = description; embed.set_footer(text="Select a question from the dropdown to delete it.")
        return embed
    
    def update_components(self):
        is_empty = not self.questions; self.first_page.disabled = self.current_page == 0 or is_empty; self.prev_page.disabled = self.current_page == 0 or is_empty
        self.next_page.disabled = self.current_page >= self.total_pages - 1 or is_empty; self.last_page.disabled = self.current_page >= self.total_pages - 1 or is_empty
        self.confirm_delete_button.disabled = self.selected_question_id is None; self.question_select.options.clear()
        page_questions = self.questions[self.current_page * self.per_page:(self.current_page * self.per_page) + self.per_page]
        if page_questions:
            self.question_select.disabled = False; self.question_select.placeholder = "Select a question on this page..."
            for q_id, q_text in page_questions: self.question_select.add_option(label=f"ID: {q_id}", description=q_text[:100], value=str(q_id))
        else: self.question_select.disabled = True; self.question_select.placeholder = "No questions to select."
    
    async def update_message(self, interaction: discord.Interaction):
        self.update_components(); embed = self.create_page_embed(); await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="|<", style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button): self.current_page = 0; self.selected_question_id = None; await self.update_message(interaction)
    @discord.ui.button(label="<", style=discord.ButtonStyle.primary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0: self.current_page -= 1
        self.selected_question_id = None; await self.update_message(interaction)
    @discord.ui.button(label=">", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1: self.current_page += 1
        self.selected_question_id = None; await self.update_message(interaction)
    @discord.ui.button(label=">|", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button): self.current_page = self.total_pages - 1; self.selected_question_id = None; await self.update_message(interaction)
    @discord.ui.select(placeholder="Select a question to delete...")
    async def question_select(self, interaction: discord.Interaction, select: discord.ui.Select): self.selected_question_id = int(select.values[0]); self.update_components(); await interaction.response.edit_message(view=self)
    @discord.ui.button(label="Confirm Deletion", style=discord.ButtonStyle.danger, disabled=True)
    async def confirm_delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.selected_question_id is None: await interaction.response.send_message("You must select a question first.", ephemeral=True); return
        
        con = sqlite3.connect(DB_FILE); cur = con.cursor(); cur.execute("DELETE FROM questions WHERE id = ?", (self.selected_question_id,)); con.commit()
        cur.execute("SELECT id, question_text FROM questions ORDER BY id"); self.questions = cur.fetchall(); con.close()
        
        await interaction.response.send_message(f"✅ Question ID `{self.selected_question_id}` deleted.", ephemeral=True)
        self.selected_question_id = None; self.recalculate_pages(); await self.cog.update_admin_panel(self.panel_message)
        self.update_components(); embed = self.create_page_embed(); await interaction.message.edit(embed=embed, view=self)

class PersistentSuggestView(discord.ui.View):
    # This view IS persistent and registered in cog_load
    def __init__(self): super().__init__(timeout=None)
    
    @discord.ui.button(label="💡 Suggest a Question", style=discord.ButtonStyle.success, custom_id="qotd_suggest_button")
    async def suggest_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SuggestionModal(); await interaction.response.send_modal(modal); timed_out = await modal.wait()
        
        if timed_out or not modal.question.value:
            try:
                await interaction.followup.send("Suggestion cancelled.", ephemeral=True)
            except discord.HTTPException:
                pass # Interaction likely expired
            return
        
        try:
            question_text = modal.question.value; settings = get_guild_settings(interaction.guild.id); log_channel_id = settings.get('suggestion_log_channel_id')
            if not log_channel_id:
                await interaction.followup.send("Your suggestion was received, but the server admin has not set up a suggestion log channel.", ephemeral=True)
                return
            
            log_channel = interaction.guild.get_channel(log_channel_id)
            if not log_channel:
                await interaction.followup.send("Your suggestion was received, but the suggestion log channel could not be found.", ephemeral=True)
                return
            
            con = sqlite3.connect(DB_FILE); cur = con.cursor()
            cur.execute("INSERT INTO suggestions (question_text, suggester_id, guild_id) VALUES (?, ?, ?)",(question_text, interaction.user.id, interaction.guild.id))
            suggestion_id = cur.lastrowid; con.commit()
            
            embed = discord.Embed(title="New Question Suggestion", description=f"**\"{question_text}\"**", color=discord.Color.gold())
            embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.avatar.url if interaction.user.avatar else None)
            embed.add_field(name="Status", value="⏳ Pending Review", inline=False); embed.set_footer(text=f"Suggestion ID: {suggestion_id}")
            
            review_message = await log_channel.send(embed=embed, view=SuggestionReviewView(suggestion_id))
            
            cur.execute("UPDATE suggestions SET review_message_id = ? WHERE id = ?", (review_message.id, suggestion_id)); con.commit(); con.close()
            
            await interaction.followup.send("✅ Your suggestion has been submitted for review!", ephemeral=True)
        
        except discord.HTTPException as e:
            print(f"Failed to process suggestion: {e}")
            try:
                await interaction.followup.send(f"An error occurred while submitting your suggestion: {e}", ephemeral=True)
            except discord.HTTPException:
                pass
        except Exception as e:
            print(f"An unexpected error occurred in suggestion submission: {e}")

class SuggestionReviewView(discord.ui.View):
    # This view IS persistent and registered in cog_load
    def __init__(self, suggestion_id: int):
        super().__init__(timeout=None); self.suggestion_id = suggestion_id
        self.approve_button.custom_id = f"qotd_approve_{suggestion_id}"; self.approve_w_reason_button.custom_id = f"qotd_approve_reason_{suggestion_id}"
        self.deny_button.custom_id = f"qotd_deny_{suggestion_id}"; self.deny_w_reason_button.custom_id = f"qotd_deny_reason_{suggestion_id}"
    
    async def _handle_decision(self, interaction: discord.Interaction, decision: str, reason: Optional[str] = None):
        con = sqlite3.connect(DB_FILE); con.row_factory = sqlite3.Row; cur = con.cursor()
        cur.execute("SELECT * FROM suggestions WHERE id = ?", (self.suggestion_id,)); suggestion = cur.fetchone()
        
        if not suggestion:
            await interaction.response.send_message("This suggestion no longer exists.", ephemeral=True); return
        if suggestion['status'] != 'pending':
            await interaction.response.send_message("This suggestion has already been reviewed.", ephemeral=True); return
        
        original_embed = interaction.message.embeds[0]
        suggester = None # Fetched later

        if decision == "approve":
            try:
                cur.execute("INSERT INTO questions (question_text, added_by_id) VALUES (?, ?)", (suggestion['question_text'], suggestion['suggester_id']))
                status_text, new_color = "✅ Approved", discord.Color.green(); dm_message = f"🎉 Your QOTD suggestion was approved in **{interaction.guild.name}**!\n\n> {suggestion['question_text']}"
            except sqlite3.IntegrityError: await interaction.response.send_message("This question already exists in the pool.", ephemeral=True); con.close(); return
        else: status_text, new_color = "❌ Denied", discord.Color.red(); dm_message = f"Your QOTD suggestion in **{interaction.guild.name}** was not approved.\n\n> {suggestion['question_text']}"
        
        # Atomic update to prevent race conditions
        cur.execute("UPDATE suggestions SET status = ? WHERE id = ? AND status = 'pending'", (decision, self.suggestion_id))
        if cur.rowcount == 0:
            await interaction.response.send_message("This suggestion was *just* reviewed by someone else.", ephemeral=True)
            con.close()
            return
        
        con.commit(); con.close()
        
        original_embed.color = new_color; original_embed.set_field_at(0, name="Status", value=f"{status_text} by {interaction.user.mention}", inline=False)
        await interaction.message.edit(embed=original_embed, view=None)
        
        if suggestion['suggester_id']:
            try:
                suggester = await interaction.guild.fetch_member(suggestion['suggester_id'])
            except (discord.NotFound, discord.HTTPException):
                suggester = None # User may have left

        if suggester:
            try:
                if reason: dm_message += f"\n\n**Reason:**\n{reason}"
                await suggester.send(dm_message)
            except discord.Forbidden: pass # Cannot DM user
        
        await interaction.response.send_message(f"Suggestion {self.suggestion_id} has been {decision}d.", ephemeral=True)
    
    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button): await self._handle_decision(interaction, "approve")
    @discord.ui.button(label="Approve w/ Reason", style=discord.ButtonStyle.success, row=1)
    async def approve_w_reason_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ReasonModal(interaction, "approve"); await interaction.response.send_modal(modal); timed_out = await modal.wait()
        if not timed_out: await self._handle_decision(modal.original_interaction, "approve", modal.reason.value)
    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button): await self._handle_decision(interaction, "deny")
    @discord.ui.button(label="Deny w/ Reason", style=discord.ButtonStyle.danger, row=1)
    async def deny_w_reason_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ReasonModal(interaction, "deny"); await interaction.response.send_modal(modal); timed_out = await modal.wait()
        if not timed_out: await self._handle_decision(modal.original_interaction, "deny", modal.reason.value)

class ResetConfirmView(discord.ui.View):
    def __init__(self, cog, panel_message: discord.Message):
        super().__init__(timeout=60); self.cog = cog; self.panel_message = panel_message
    
    @discord.ui.button(label="Confirm Reset", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        con = sqlite3.connect(DB_FILE); cur = con.cursor(); cur.execute("UPDATE questions SET last_used_timestamp = 0"); con.commit(); con.close()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content="✅ The entire question pool has been reset.", view=self)
        await self.cog.update_admin_panel(self.panel_message)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children: item.disabled = True; await interaction.response.edit_message(content="Reset cancelled.", view=self)

class ClearSeenConfirmView(discord.ui.View):
    def __init__(self, cog, panel_message: discord.Message):
        super().__init__(timeout=60); self.cog = cog; self.panel_message = panel_message
    
    @discord.ui.button(label="Confirm Clear Seen", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        con = sqlite3.connect(DB_FILE); cur = con.cursor()
        cur.execute("DELETE FROM questions WHERE last_used_timestamp > 0")
        deleted_count = cur.rowcount # Get how many rows were deleted
        con.commit(); con.close()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(content=f"✅ Cleared **{deleted_count}** previously seen questions.", view=self)
        await self.cog.update_admin_panel(self.panel_message)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children: item.disabled = True; await interaction.response.edit_message(content="Clear seen questions cancelled.", view=self)

class ManagePostChannelsView(discord.ui.View):
    # This view is ephemeral, timeout=None removed
    def __init__(self, cog):
        super().__init__(timeout=180) 
        self.cog = cog

        # Create and add the 'add' channel select menu
        self.add_channel_select = discord.ui.ChannelSelect(
            placeholder="Add channels to the post list...",
            min_values=1, max_values=10,
            channel_types=[discord.ChannelType.text], row=0
        )
        self.add_channel_select.callback = self.add_channel_callback
        self.add_item(self.add_channel_select)

        # Create and add the 'remove' channel select menu
        self.remove_channel_select = discord.ui.ChannelSelect(
            placeholder="Remove channels from the post list...",
            min_values=1, max_values=10,
            channel_types=[discord.ChannelType.text], row=1
        )
        self.remove_channel_select.callback = self.remove_channel_callback
        self.add_item(self.remove_channel_select)

        # Create and add the back button
        back_button = discord.ui.Button(label="Back to Setup Menu", style=discord.ButtonStyle.grey, row=2)
        back_button.callback = self.go_back_callback
        self.add_item(back_button)

    def create_embed(self, guild: discord.Guild) -> discord.Embed:
        settings = get_guild_settings(guild.id)
        
        try:
            post_channel_ids = json.loads(settings.get('post_channel_ids', '[]'))
        except json.JSONDecodeError:
            post_channel_ids = []
            
        channel_mentions = [f"<#{cid}>" for cid in post_channel_ids if guild.get_channel(cid)]
        description = "**Current Post Channels:**\n" + ("\n".join(channel_mentions) if channel_mentions else "None set.")
        embed = discord.Embed(title="Manage Post Channels", description=description, color=discord.Color.purple())
        return embed

    async def go_back_callback(self, interaction: discord.Interaction):
        embed = self.cog.create_setup_embed()
        await interaction.response.edit_message(embed=embed, view=SetupView(self.cog))

    async def add_channel_callback(self, interaction: discord.Interaction):
        selected_channels = self.add_channel_select.values
        settings = get_guild_settings(interaction.guild.id)
        
        try:
            p_ids = json.loads(settings.get('post_channel_ids', '[]'))
        except json.JSONDecodeError:
            p_ids = []
            
        added_channels = [ch.mention for ch in selected_channels if ch.id not in p_ids]

        if not added_channels:
            await interaction.response.send_message("All selected channels are already in the list.", ephemeral=True)
            return

        for ch in selected_channels:
            if ch.id not in p_ids:
                p_ids.append(ch.id)

        update_guild_setting(interaction.guild.id, 'post_channel_ids', json.dumps(p_ids))
        await interaction.response.send_message(f"✅ Added: {', '.join(added_channels)}", ephemeral=True)
        await interaction.message.edit(embed=self.create_embed(interaction.guild))

    async def remove_channel_callback(self, interaction: discord.Interaction):
        selected_channels = self.remove_channel_select.values
        settings = get_guild_settings(interaction.guild.id)
        
        try:
            p_ids = json.loads(settings.get('post_channel_ids', '[]'))
        except json.JSONDecodeError:
            p_ids = []
            
        removed_channels = [ch.mention for ch in selected_channels if ch.id in p_ids]

        if not removed_channels:
            await interaction.response.send_message("None of the selected channels were in the list.", ephemeral=True)
            return

        p_ids = [pid for pid in p_ids if pid not in [ch.id for ch in selected_channels]]

        update_guild_setting(interaction.guild.id, 'post_channel_ids', json.dumps(p_ids))
        await interaction.response.send_message(f"✅ Removed: {', '.join(removed_channels)}", ephemeral=True)
        await interaction.message.edit(embed=self.create_embed(interaction.guild))

class SetupView(discord.ui.View):
    # This view is ephemeral, timeout=None removed
    def __init__(self, cog):
        super().__init__(timeout=180) 
        self.cog = cog

    @discord.ui.button(label="Set Ping Role", style=discord.ButtonStyle.primary, row=0)
    async def set_ping_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_guild_settings(interaction.guild.id)
        modal = PingRoleModal(settings.get('ping_role_id'))
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Suggestion Channel", style=discord.ButtonStyle.primary, row=0)
    async def set_suggestion_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_guild_settings(interaction.guild.id)
        modal = SuggestionChannelModal(settings.get('suggestion_log_channel_id'))
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Set Post Time", style=discord.ButtonStyle.secondary, row=1)
    async def set_post_time(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_guild_settings(interaction.guild.id)
        modal = PostTimeModal(settings.get('post_time', '10:00'))
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Timezone", style=discord.ButtonStyle.secondary, row=1)
    async def set_timezone(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = get_guild_settings(interaction.guild.id)
        modal = TimezoneModal(settings.get('timezone', 'UTC'))
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Manage Post Channels", style=discord.ButtonStyle.success, row=2)
    async def manage_post_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ManagePostChannelsView(self.cog)
        embed = view.create_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed, view=view)

# --- Admin Panel View ---
class AdminPanelView(discord.ui.View):
    # This view is ephemeral, timeout=None removed
    def __init__(self, cog):
        super().__init__(timeout=180)
        self.cog = cog

    def update_toggle_buttons(self, settings: dict):
        if settings['enabled']: self.toggle_system_button.label = "Disable System"; self.toggle_system_button.style = discord.ButtonStyle.danger
        else: self.toggle_system_button.label = "Enable System"; self.toggle_system_button.style = discord.ButtonStyle.success
        if settings['auto_thread']: self.toggle_autothread_button.label = "Auto-Threading: ON"; self.toggle_autothread_button.style = discord.ButtonStyle.success
        else: self.toggle_autothread_button.label = "Auto-Threading: OFF"; self.toggle_autothread_button.style = discord.ButtonStyle.secondary

    @discord.ui.button(label="Enable System", style=discord.ButtonStyle.success, row=0)
    async def toggle_system_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True)
        update_guild_setting(interaction.guild.id, 'enabled', not get_guild_settings(interaction.guild.id)['enabled'])
        await self.cog.update_admin_panel(interaction.message)

    @discord.ui.button(label="Auto-Threading: ON", style=discord.ButtonStyle.success, row=0)
    async def toggle_autothread_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True)
        update_guild_setting(interaction.guild.id, 'auto_thread', not get_guild_settings(interaction.guild.id)['auto_thread'])
        await self.cog.update_admin_panel(interaction.message)

    @discord.ui.button(label="View Questions", style=discord.ButtonStyle.primary, row=1)
    async def view_pool_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        con = sqlite3.connect(DB_FILE); cur = con.cursor(); cur.execute("SELECT id, question_text FROM questions ORDER BY id"); questions = cur.fetchall(); con.close()
        if not questions: await interaction.response.send_message("The question pool is empty.", ephemeral=True); return
        view = QuestionPagesView(questions); embed = view.create_page_embed(); await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Add Questions", style=discord.ButtonStyle.success, row=1)
    async def add_question_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AddQuestionModal(self.cog, interaction.message); await interaction.response.send_modal(modal)

    @discord.ui.button(label="Delete Question", style=discord.ButtonStyle.danger, row=1)
    async def delete_question_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        con = sqlite3.connect(DB_FILE); cur = con.cursor(); cur.execute("SELECT id, question_text FROM questions ORDER BY id"); questions = cur.fetchall(); con.close()
        if not questions: await interaction.response.send_message("The question pool is empty.", ephemeral=True); return
        view = DeleteQuestionView(questions, self.cog, interaction.message); embed = view.create_page_embed(); await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Reset Pool", style=discord.ButtonStyle.danger, row=2)
    async def reset_pool_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ResetConfirmView(self.cog, interaction.message)
        await interaction.response.send_message("**Are you sure you want to reset the question pool?**\nThis will make all previously asked questions available to be asked again.", view=view, ephemeral=True)

    @discord.ui.button(label="Clear Seen", style=discord.ButtonStyle.danger, row=2)
    async def clear_seen_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ClearSeenConfirmView(self.cog, interaction.message)
        await interaction.response.send_message("**Are you sure you want to delete all seen questions?**\nThis will permanently remove any question that has `last_used_timestamp > 0`.", view=view, ephemeral=True)

    @discord.ui.button(label="Post Suggestion Panel", style=discord.ButtonStyle.success, row=3)
    async def post_suggestion_panel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Posts a persistent panel for users to suggest questions."""
        await interaction.response.defer(ephemeral=True, thinking=True)
        embed = discord.Embed(
            title="💡 Suggest a Question!",
            description="Help us build our Question of the Day pool!\n\nClick the button below to suggest a question. If approved by an admin, it will be added to the rotation!",
            color=discord.Color.gold()
        )
        try:
            await interaction.channel.send(embed=embed, view=PersistentSuggestView())
            await interaction.followup.send("✅ Public suggestion panel has been posted in this channel.", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ I don't have permission to send messages in this channel.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"An unknown error occurred: {e}", ephemeral=True)

    @discord.ui.button(label="Manual Post", style=discord.ButtonStyle.secondary, row=4)
    async def manual_post_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True); await self.cog.post_qotd(interaction.guild.id, is_test=False, interaction=interaction)

    @discord.ui.button(label="Test Post", style=discord.ButtonStyle.secondary, row=4)
    async def test_post_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True); await self.cog.post_qotd(interaction.guild.id, is_test=True, interaction=interaction)

# --- Cog Class ---
class QOTDCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # self.last_posted_time = {} # Replaced with DB storage
        db_init()
        self.qotd_task.start()

    async def cog_load(self):
        self.bot.add_view(PersistentSuggestView())
        con = sqlite3.connect(DB_FILE); cur = con.cursor(); cur.execute("SELECT id FROM suggestions WHERE status = 'pending'"); pending_suggestions = cur.fetchall(); con.close()
        for (suggestion_id,) in pending_suggestions: self.bot.add_view(SuggestionReviewView(suggestion_id))
        print(f"Registered {len(pending_suggestions)} pending suggestion views.")
    
    def cog_unload(self): self.qotd_task.cancel()

    def create_setup_embed(self):
         return discord.Embed(
            title="QOTD Setup",
            description="Use the buttons below to configure each setting individually.",
            color=discord.Color.orange()
        )

    async def update_admin_panel(self, message: discord.Message):
        try:
            if not message.guild: return
            settings = get_guild_settings(message.guild.id)
            embed = self.create_admin_embed(message.guild, settings)
            view = AdminPanelView(self)
            view.update_toggle_buttons(settings) # Pass settings directly
            await message.edit(embed=embed, view=view)
        except (discord.NotFound, discord.HTTPException) as e: print(f"Failed to update admin panel, maybe it was deleted: {e}")
        except Exception as e: print(f"An unexpected error occurred while updating admin panel: {e}")

    @tasks.loop(minutes=1)
    async def qotd_task(self):
        await self.bot.wait_until_ready()
        con = sqlite3.connect(DB_FILE); con.row_factory = sqlite3.Row; cur = con.cursor(); cur.execute("SELECT * FROM guild_settings WHERE enabled = TRUE"); enabled_guilds = cur.fetchall()

        for settings_row in enabled_guilds:
            settings = dict(settings_row); guild_id = settings['guild_id']
            try:
                try:
                    tz = pytz.timezone(settings['timezone'])
                except pytz.UnknownTimeZoneError:
                    print(f"Guild {guild_id} has invalid timezone: {settings['timezone']}. Defaulting to UTC.")
                    tz = pytz.timezone('UTC')

                now_in_tz = datetime.datetime.now(tz)
                post_hour, post_minute = map(int, settings['post_time'].split(':'))

                last_post_ts = settings.get('last_post_timestamp', 0)
                last_post = datetime.datetime.fromtimestamp(last_post_ts, tz) if last_post_ts > 0 else None

                # Check if we already posted today
                if last_post and last_post.date() == now_in_tz.date():
                    continue # Already posted today

                # Post if it's the scheduled time OR if we missed today's post
                scheduled_time_today = now_in_tz.replace(hour=post_hour, minute=post_minute, second=0, microsecond=0)
                is_scheduled_time = now_in_tz.hour == post_hour and now_in_tz.minute == post_minute
                missed_todays_post = now_in_tz > scheduled_time_today and (not last_post or last_post.date() < now_in_tz.date())

                if is_scheduled_time or missed_todays_post:
                    if missed_todays_post:
                        print(f"Guild {guild_id}: Posting missed QOTD (scheduled for {post_hour}:{post_minute:02d}, now is {now_in_tz.hour}:{now_in_tz.minute:02d})")

                    # Pass the DB connection to post_qotd to run in a transaction
                    await self.post_qotd(guild_id, db_connection=con)

            except Exception as e: print(f"Error in QOTD task for guild {guild_id}: {e}")

        con.commit() # Commit all changes from the loop at once
        con.close()

    @qotd_task.before_loop
    async def before_qotd_task(self): await self.bot.wait_until_ready()

    async def post_qotd(self, guild_id: int, is_test: bool = False, interaction: Optional[discord.Interaction] = None, db_connection: Optional[sqlite3.Connection] = None):
        settings = get_guild_settings(guild_id)
        if not settings.get('enabled') and not is_test:
            if interaction: await interaction.followup.send("❌ The QOTD system is disabled.", ephemeral=True)
            return
        
        try:
            post_channel_ids = json.loads(settings.get('post_channel_ids', '[]'))
        except json.JSONDecodeError:
            post_channel_ids = []
            
        if not post_channel_ids:
            if interaction: await interaction.followup.send(f"❌ Error: No post channels set. Use the `/qotd setup` command.", ephemeral=True)
            return
        
        # --- DB Connection Handling ---
        local_con = False
        if db_connection is None:
            con = sqlite3.connect(DB_FILE)
            local_con = True
        else:
            con = db_connection
        cur = con.cursor()
        # ---
        
        # Select an unseen question
        cur.execute("SELECT id, question_text, added_by_id FROM questions WHERE last_used_timestamp = 0 ORDER BY RANDOM() LIMIT 1");
        question_data = cur.fetchone()
        
        if not question_data:
            if interaction: await interaction.followup.send(f"❌ Error: I've run out of unseen questions! Use 'Reset Pool' in the admin panel.", ephemeral=True)
            else:
                try:
                    first_channel_id = post_channel_ids[0]
                    first_channel = self.bot.get_channel(first_channel_id)
                    if first_channel: await first_channel.send("I've run out of questions! An admin can use the 'Reset Pool' button to make them available again.")
                except (IndexError, AttributeError):
                     pass # Fails silently if no channels are set or found
            if local_con: con.close(); 
            return
        
        question_id, question_text, added_by_id = question_data
        
        embed = discord.Embed(title="❓ Question of the Day ❓", description=f"## {question_text}", color=discord.Color.blue())
        total_questions, unseen_count = get_question_counts()
        footer_text = f"{unseen_count - 1 if not is_test and unseen_count > 0 else unseen_count} questions remaining."
        
        suggester = None
        if added_by_id and added_by_id > 1: # 0 or 1 might be system/default IDs
            try: 
                suggester = await self.bot.fetch_user(added_by_id)
            except (discord.NotFound, discord.HTTPException): 
                pass # User not found
        if suggester: footer_text = f"Suggested by: {suggester.display_name} • {footer_text}"
        embed.set_footer(text=footer_text)
        
        content = ""; ping_role_id = settings.get('ping_role_id'); role = None
        if ping_role_id and not is_test:
            guild = self.bot.get_guild(guild_id)
            if guild: role = guild.get_role(ping_role_id)
        if role and role.mentionable: # Added permission check
            content = role.mention
            
        posted_successfully = False
        for channel_id in post_channel_ids:
            post_channel = self.bot.get_channel(channel_id)
            if not post_channel: continue
            try:
                if is_test and interaction: 
                    await interaction.followup.send(f"This is a test post for {post_channel.mention}.", embed=embed, ephemeral=True)
                else:
                    message = await post_channel.send(content=content, embed=embed, view=PersistentSuggestView())
                    # Added thread creation checks
                    if settings.get('auto_thread') and isinstance(post_channel, (discord.TextChannel, discord.VoiceChannel, discord.ForumChannel)):
                        try:
                            await message.create_thread(name=f"Discussion for QOTD - {datetime.datetime.now().strftime('%Y-%m-%d')}")
                        except (discord.Forbidden, discord.HTTPException) as e:
                            print(f"Failed to create thread in {post_channel.name}: {e}")
                posted_successfully = True
            except discord.Forbidden:
                if interaction: await interaction.followup.send(f"❌ Error: I don't have permission to post in {post_channel.mention}.", ephemeral=True)
            except Exception as e:
                if interaction: await interaction.followup.send(f"❌ An unknown error occurred: {e}", ephemeral=True)
        
        if posted_successfully and not is_test:
            # Delete question and update last post time
            cur.execute("DELETE FROM questions WHERE id = ?", (question_id,))
            cur.execute("UPDATE guild_settings SET last_post_timestamp = ? WHERE guild_id = ?", (int(datetime.datetime.now().timestamp()), guild_id))
            
            if local_con: con.commit() # Commit if we own the connection
            
            if interaction and interaction.message:
                await interaction.followup.send("✅ Manually posted the Question of the Day.", ephemeral=True); 
                await self.update_admin_panel(interaction.message)
        elif interaction and not posted_successfully:
             await interaction.followup.send("❌ Failed to post. Check bot permissions for the configured channels.", ephemeral=True)
        
        if local_con: con.close() # Close if we own the connection

    qotd_group = app_commands.Group(name="qotd", description="Commands for the Question of the Day feature.")

    def create_admin_embed(self, guild: discord.Guild, settings: dict):
        def get_name(obj_id, type):
            if not obj_id: return "`Not Set`"
            if type == 'bot': return f"<@{obj_id}>"
            obj = guild.get_channel(obj_id) if type == 'channel' else guild.get_role(obj_id)
            if obj: return obj.mention
            return f"`Not Found (ID: {obj_id})`"
        
        try:
            post_channel_ids = json.loads(settings.get('post_channel_ids', '[]'))
        except json.JSONDecodeError:
            post_channel_ids = []
            
        channel_mentions = [get_name(cid, 'channel') for cid in post_channel_ids]
        post_channel_display = ", ".join(channel_mentions) if channel_mentions else "`Not Set`"
        
        embed = discord.Embed(title="QOTD Admin Panel", color=discord.Color.dark_purple()); embed.set_author(name=guild.name, icon_url=guild.icon.url if guild.icon else None)
        status = "✅ Enabled" if settings.get('enabled') else "❌ Disabled"; total_questions, unseen_questions = get_question_counts()
        embed.description = (f"**System Status:** {status}\nUse the `/qotd setup` command to configure the bot.\n"
                             "This panel is for managing the question pool.")
        embed.add_field(name="📣 Post Channels", value=post_channel_display, inline=False)
        embed.add_field(name="📅 Schedule", value=f"`{settings.get('post_time')}` (`{settings.get('timezone')}`)", inline=True)
        embed.add_field(name="❓ Question Pool", value=f"**Total:** `{total_questions}`\n**Unseen:** `{unseen_questions}`", inline=True); return embed

    @qotd_group.command(name="setup", description="Brings up the setup menu for the QOTD bot.")
    @app_commands.default_permissions(manage_guild=True)
    async def setup(self, interaction: discord.Interaction):
        embed = self.create_setup_embed()
        await interaction.response.send_message(embed=embed, view=SetupView(self), ephemeral=True)


    @qotd_group.command(name="admin_panel", description="Access the main admin panel for QOTD settings.")
    @app_commands.default_permissions(manage_guild=True)
    async def admin_panel(self, interaction: discord.Interaction):
        settings = get_guild_settings(interaction.guild.id); embed = self.create_admin_embed(interaction.guild, settings)
        view = AdminPanelView(self)
        view.update_toggle_buttons(settings) # Pass settings directly
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(QOTDCog(bot))


