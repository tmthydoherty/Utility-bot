import discord
from discord.ext import commands
from discord import app_commands, ui, Interaction, ButtonStyle
from typing import Literal, Optional, Dict, Any
import json
import asyncio
import os
import functools # For running blocking I/O in a thread

# --- Constants ---
SETTINGS_FILE = "guild_settings.json"
DATA_FILE = "suggestion_data.json"
MIN_SUGGESTION_LENGTH = 20

# --- Helper Functions for JSON IO (Async via Executor) ---

async def load_json(filename: str) -> Dict[str, Any]:
    """Asynchronously loads a JSON file in an executor thread."""
    loop = asyncio.get_event_loop()
    
    def read_file():
        if not os.path.exists(filename):
            return {}
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
        except Exception as e:
            print(f"Error loading JSON file {filename}: {e}")
            return {}

    return await loop.run_in_executor(None, read_file)

async def save_json(filename: str, data: Dict[str, Any]) -> None:
    """Asynchronously saves data to a JSON file in an executor thread."""
    loop = asyncio.get_event_loop()
    
    def write_file():
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"Error saving JSON file {filename}: {e}")

    await loop.run_in_executor(None, write_file)

# --- Modals ---

class SuggestionModal(ui.Modal, title="Make a Suggestion"):
    """Modal for a user to submit a suggestion."""
    suggestion = ui.TextInput(
        label="What's your suggestion?",
        style=discord.TextStyle.paragraph,
        placeholder=f"I think you should add... (min {MIN_SUGGESTION_LENGTH} characters)",
        required=True,
        min_length=MIN_SUGGESTION_LENGTH,
        max_length=1000
    )

    def __init__(self, cog: 'Suggestions'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        
        guild_id = str(interaction.guild.id)
        suggestion_id = -1
        guild_settings = None # Initialize
        
        async with self.cog.settings_lock:
            settings = await load_json(SETTINGS_FILE)
            guild_settings = settings.get(guild_id)

            if not guild_settings or not guild_settings.get("suggestion_channel_id") or not guild_settings.get("log_channel_id"):
                await interaction.followup.send("The suggestion system is not fully set up. Please contact an admin.", ephemeral=True)
                return

            guild_settings.setdefault("suggestion_counter", 0)
            guild_settings["suggestion_counter"] += 1
            suggestion_id = guild_settings["suggestion_counter"]
            await save_json(SETTINGS_FILE, settings)
        
        suggestion_channel = self.cog.bot.get_channel(guild_settings["suggestion_channel_id"])
        log_channel = self.cog.bot.get_channel(guild_settings["log_channel_id"])

        if not suggestion_channel or not log_channel:
            await interaction.followup.send("Suggestion or log channel not found. Please contact an admin.", ephemeral=True)
            return

        # 1. Create the embed
        embed = self.cog.create_suggestion_embed(
            author=interaction.user,
            suggestion_id=suggestion_id,
            content=self.suggestion.value,
            status="Pending"
        )

        try:
            # 2. Send to suggestion channel
            suggestion_msg = await suggestion_channel.send(embed=embed)
            
            try:
                await suggestion_msg.add_reaction("⬆️")
                await suggestion_msg.add_reaction("⬇️")
            except discord.Forbidden:
                print(f"Failed to add reactions in {suggestion_channel.name} (Guild: {interaction.guild.name})")

            # 3. Create thread
            await suggestion_msg.create_thread(
                name=f"Suggestion #{suggestion_id} Discussion",
                auto_archive_duration=10080
            )

            # 4. Send to log channel with buttons
            admin_view = AdminActionView(self.cog)
            log_msg = await log_channel.send(embed=embed, view=admin_view)

            # 5. Save data
            async with self.cog.data_lock:
                data = await load_json(DATA_FILE)
                data[str(log_msg.id)] = {
                    "guild_id": guild_id,
                    "suggestion_id": suggestion_id,
                    "suggestion_message_id": suggestion_msg.id,
                    "author_id": interaction.user.id,
                    "status": "Pending",
                    "reason": None,
                    "content": self.suggestion.value,
                    "admin_id": None
                }
                await save_json(DATA_FILE, data)

            await interaction.followup.send("✅ Your suggestion has been submitted!", ephemeral=True)

            # 6. Re-post the suggestion button to the bottom
            await self.cog.repost_button(interaction.guild)

        except discord.Forbidden:
            await interaction.followup.send("The bot lacks permissions to post in the suggestion or log channel.", ephemeral=True)
        except Exception as e:
            print(f"Error in suggestion submission: {e}")
            await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)


class AdminReasonModal(ui.Modal, title="Admin Action"):
    """Modal for an admin to provide a reason for their action."""
    reason = ui.TextInput(
        label="Reason",
        style=discord.TextStyle.paragraph,
        placeholder="Enter a reason for this decision (optional).",
        required=False,
        max_length=500
    )

    def __init__(self, cog: 'Suggestions', action: Literal["Approved", "Denied", "Implemented"], log_message_id: int):
        super().__init__()
        self.cog = cog
        self.action = action
        self.log_message_id = log_message_id
    
    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        reason_text = self.reason.value if self.reason.value else "No reason provided."
        
        await self.cog.update_suggestion_status(
            interaction=interaction,
            log_message_id=self.log_message_id,
            new_status=self.action,
            reason=reason_text,
            admin=interaction.user
        )
        await interaction.followup.send(f"Suggestion has been marked as **{self.action}**.", ephemeral=True)

class ManageModal(ui.Modal, title="Manage Suggestion"):
    """Modal for an admin to change a decision."""
    new_status = ui.TextInput(
        label="New Status",
        placeholder="Enter: Approved, Denied, Implemented, or Pending",
        required=True,
        max_length=20
    )
    reason = ui.TextInput(
        label="New Reason",
        style=discord.TextStyle.paragraph,
        placeholder="Enter the new reason for this change.",
        required=True,
        max_length=500
    )

    def __init__(self, cog: 'Suggestions', log_message_id: int):
        super().__init__()
        self.cog = cog
        self.log_message_id = log_message_id

    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        
        new_status = self.new_status.value.strip().title()
        valid_statuses = ["Approved", "Denied", "Implemented", "Pending"]
        
        if new_status not in valid_statuses:
            await interaction.followup.send(
                f"Invalid status. Must be one of: {', '.join(valid_statuses)}", 
                ephemeral=True
            )
            return
        
        await self.cog.update_suggestion_status(
            interaction=interaction,
            log_message_id=self.log_message_id,
            new_status=new_status,
            reason=self.reason.value,
            admin=interaction.user
        )
        await interaction.followup.send(f"Suggestion has been updated to **{new_status}**.", ephemeral=True)


# --- Views ---

class PersistentSuggestionView(ui.View):
    """Persistent view with the 'Make a suggestion' button."""
    def __init__(self, cog: 'Suggestions'):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="Make a Suggestion", style=ButtonStyle.success, custom_id="make_suggestion_button")
    async def make_suggestion(self, interaction: Interaction, button: ui.Button):
        await interaction.response.send_modal(SuggestionModal(self.cog))

class AdminActionView(ui.View):
    """View with Approve, Deny, and Implemented buttons for the log channel."""
    def __init__(self, cog: 'Suggestions'):
        super().__init__(timeout=None)
        self.cog = cog

    async def handle_action(self, interaction: Interaction, action: Literal["Approved", "Denied", "Implemented"]):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        
        await interaction.response.send_modal(
            AdminReasonModal(self.cog, action, interaction.message.id)
        )

    @ui.button(label="Approve", style=ButtonStyle.green, custom_id="suggestion_approve")
    async def approve(self, interaction: Interaction, button: ui.Button):
        await self.handle_action(interaction, "Approved")

    @ui.button(label="Deny", style=ButtonStyle.red, custom_id="suggestion_deny")
    async def deny(self, interaction: Interaction, button: ui.Button):
        await self.handle_action(interaction, "Denied")

    @ui.button(label="Implemented", style=ButtonStyle.blurple, custom_id="suggestion_implemented")
    async def implemented(self, interaction: Interaction, button: ui.Button):
        await self.handle_action(interaction, "Implemented")

class ManageView(ui.View):
    """Persistent view with a 'Manage' button to change a decision."""
    def __init__(self, cog: 'Suggestions'):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="Manage", style=ButtonStyle.secondary, custom_id="suggestion_manage")
    async def manage(self, interaction: Interaction, button: ui.Button):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return

        await interaction.response.send_modal(
            ManageModal(self.cog, interaction.message.id)
        )

# --- Cog Class ---

class Suggestions(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.settings_lock = asyncio.Lock()
        self.data_lock = asyncio.Lock()
        self.bot.add_view(PersistentSuggestionView(self))
        self.bot.add_view(AdminActionView(self))
        self.bot.add_view(ManageView(self))

    @commands.Cog.listener()
    async def on_ready(self):
        print("✅ Suggestions Cog loaded.")
        print("Checking for persistent views...")
        if not self.bot.persistent_views:
            self.bot.add_view(PersistentSuggestionView(self))
            self.bot.add_view(AdminActionView(self))
            self.bot.add_view(ManageView(self))
            print("Re-added persistent views.")
        else:
            print("Persistent views already loaded.")
            
    # --- Admin Command Group ---
    panel = app_commands.Group(
        name="suggestionpanel", 
        description="Admin panel for the suggestion system.",
        default_permissions=discord.Permissions(manage_guild=True),
        guild_only=True
    )

    @panel.command(name="setupchannel")
    @app_commands.describe(channel="The channel where users will submit suggestions.")
    async def setup_channel(self, interaction: Interaction, channel: discord.TextChannel):
        """Sets the suggestion submission channel."""
        await interaction.response.defer(ephemeral=True)
        
        async with self.settings_lock:
            settings = await load_json(SETTINGS_FILE)
            guild_settings = settings.setdefault(str(interaction.guild.id), {})
            
            if guild_settings.get("suggestion_button_message_id"):
                try:
                    old_channel = self.bot.get_channel(guild_settings.get("suggestion_channel_id", 0))
                    if old_channel:
                        old_msg = await old_channel.fetch_message(guild_settings["suggestion_button_message_id"])
                        await old_msg.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
            
            guild_settings["suggestion_channel_id"] = channel.id
            
            try:
                view = PersistentSuggestionView(self)
                msg = await channel.send(view=view)
                guild_settings["suggestion_button_message_id"] = msg.id
                await save_json(SETTINGS_FILE, settings)
                await interaction.followup.send(f"✅ Suggestion channel set to {channel.mention}.")
            
            except discord.Forbidden:
                await interaction.followup.send("Error: The bot does not have permission to send messages in that channel.")
        
    @panel.command(name="logchannel")
    @app_commands.describe(channel="The channel where admin-facing suggestions will be logged.")
    async def log_channel(self, interaction: Interaction, channel: discord.TextChannel):
        """Sets the suggestion log channel."""
        await interaction.response.defer(ephemeral=True)
        
        async with self.settings_lock:
            settings = await load_json(SETTINGS_FILE)
            guild_settings = settings.setdefault(str(interaction.guild.id), {})
            guild_settings["log_channel_id"] = channel.id
            await save_json(SETTINGS_FILE, settings)
        
        # --- THIS IS THE FIXED LINE ---
        await interaction.followup.send(f"✅ Suggestion log channel set to {channel.mention}.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Deletes messages in the suggestion channel that aren't from the bot."""
        if message.author.bot or not message.guild:
            return

        suggestion_channel_id = None
        async with self.settings_lock:
            settings = await load_json(SETTINGS_FILE)
            guild_settings = settings.get(str(message.guild.id))

            if not guild_settings:
                return
            suggestion_channel_id = guild_settings.get("suggestion_channel_id")
        
        if message.channel.id == suggestion_channel_id:
            try:
                await message.delete()
            except discord.Forbidden:
                print(f"Failed to delete message in suggestion channel (ID: {suggestion_channel_id}) - No permissions.")
            except discord.NotFound:
                pass

    # --- Core Logic ---
    
    async def repost_button(self, guild: discord.Guild):
        """Deletes the old button message and posts a new one at the bottom."""
        async with self.settings_lock:
            settings = await load_json(SETTINGS_FILE)
            guild_settings = settings.get(str(guild.id))

            if not guild_settings or not guild_settings.get("suggestion_channel_id"):
                return

            channel = self.bot.get_channel(guild_settings["suggestion_channel_id"])
            if not channel:
                return

            if guild_settings.get("suggestion_button_message_id"):
                try:
                    old_msg = await channel.fetch_message(guild_settings["suggestion_button_message_id"])
                    await old_msg.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass

            try:
                view = PersistentSuggestionView(self)
                msg = await channel.send(view=view)
                guild_settings["suggestion_button_message_id"] = msg.id
                await save_json(SETTINGS_FILE, settings)
            except discord.Forbidden:
                print(f"Failed to repost button in {guild.name} - No permissions.")

    def get_status_color(self, status: str) -> discord.Color:
        """Returns a color based on the suggestion status."""
        if status == "Approved":
            return discord.Color.green()
        if status == "Denied":
            return discord.Color.red()
        if status == "Implemented":
            return discord.Color(0x3498db) # Light Blue
        return discord.Color(0xFFFFFF) # White for Pending

    def create_suggestion_embed(self, *, author: discord.Member, suggestion_id: int, content: str, status: str, reason: Optional[str] = None, admin: Optional[discord.Member] = None) -> discord.Embed:
        """Helper function to create a consistent suggestion embed."""
        
        color = self.get_status_color(status)
        embed = discord.Embed(
            title=f"Suggestion #{suggestion_id} - {status}",
            description=content,
            color=color
        )
        embed.set_author(name=f"Submitted by {author.display_name}", icon_url=author.display_avatar.url)
        
        if reason:
            admin_name = admin.display_name if admin else "an admin"
            embed.add_field(name=f"Reason from {admin_name}", value=reason, inline=False)
            
        return embed

    async def update_suggestion_status(self, *, interaction: Interaction, log_message_id: int, new_status: str, reason: str, admin: discord.Member):
        
        suggestion_data = None
        async with self.data_lock:
            data = await load_json(DATA_FILE)
            suggestion_data = data.get(str(log_message_id))
            
            if not suggestion_data:
                await interaction.followup.send("Error: Could not find suggestion data.", ephemeral=True)
                return

            suggestion_data["status"] = new_status
            suggestion_data["reason"] = reason
            suggestion_data["admin_id"] = admin.id
            await save_json(DATA_FILE, data)
        
        try:
            author = await self.bot.fetch_user(suggestion_data["author_id"])
        except discord.NotFound:
            author = None

        new_embed = self.create_suggestion_embed(
            author=author if author else admin, 
            suggestion_id=suggestion_data["suggestion_id"],
            content=suggestion_data["content"],
            status=new_status,
            reason=reason,
            admin=admin
        )
        if not author:
             new_embed.set_author(name=f"Submitted by [Unknown User]", icon_url=self.bot.user.display_avatar.url)

        
        guild_settings = None
        async with self.settings_lock:
            settings = await load_json(SETTINGS_FILE)
            guild_settings = settings.get(suggestion_data["guild_id"])
        
        if not guild_settings:
            print(f"Error: Could not find guild settings for guild {suggestion_data['guild_id']}")
            return

        # 3. Update Log Channel Message
        try:
            log_channel_id = guild_settings.get("log_channel_id")
            if not log_channel_id:
                raise ValueError("Log channel not configured in settings.")
                
            log_channel = self.bot.get_channel(log_channel_id)
            if not log_channel:
                raise ValueError(f"Could not find log channel with ID {log_channel_id}")

            log_msg = await log_channel.fetch_message(log_message_id)
            new_view = ManageView(self) if new_status != "Pending" else AdminActionView(self)
            await log_msg.edit(embed=new_embed, view=new_view)
        except Exception as e:
            print(f"Failed to update log message: {e}")
            await interaction.followup.send("Error: Failed to update the log message.", ephemeral=True)

        # 4. Update Suggestion Channel Message
        try:
            suggestion_channel_id = guild_settings.get("suggestion_channel_id")
            if not suggestion_channel_id:
                 raise ValueError("Suggestion channel not configured in settings.")
            
            suggestion_channel = self.bot.get_channel(suggestion_channel_id)
            if not suggestion_channel:
                raise ValueError(f"Could not find suggestion channel with ID {suggestion_channel_id}")

            suggestion_msg = await suggestion_channel.fetch_message(suggestion_data["suggestion_message_id"])
            await suggestion_msg.edit(embed=new_embed)
        except Exception as e:
            print(f"Failed to update suggestion message: {e}")

        # 5. DM the Author
        if author:
            try:
                dm_embed = new_embed.copy()
                dm_embed.title = f"Your Suggestion (#{suggestion_data['suggestion_id']}) was Updated"
                dm_embed.set_footer(text=f"Server: {interaction.guild.name}")
                
                await author.send(f"A decision has been made on your suggestion in **{interaction.guild.name}**:", embed=dm_embed)
            except discord.Forbidden:
                print(f"Failed to DM user {author.id} - DMs are closed.")
            except Exception as e:
                print(f"Failed to DM user: {e}")


# --- Setup Function ---
async def setup(bot: commands.Bot):
    await bot.add_cog(Suggestions(bot))


