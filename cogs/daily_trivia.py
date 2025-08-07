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
import re

# --- Globals & Configuration ---
log_trivia = logging.getLogger(__name__)

CONFIG_FILE_TRIVIA = "trivia_config.json"
TRIVIA_API_URL_BASE = "https://opentdb.com/api.php?type=multiple"
CACHE_FETCH_AMOUNT = 50
EMBED_COLOR_TRIVIA = 0x1ABC9C
CACHE_MIN_SIZE = 10
CACHE_TARGET_SIZE = 25
INTERACTION_HISTORY_DAYS = 60
LEADERBOARD_LIMIT = 15
MAX_ASKED_QUESTIONS_HISTORY = 1000

def load_config_trivia():
    """Loads the trivia configuration from a JSON file."""
    if os.path.exists(CONFIG_FILE_TRIVIA):
        try:
            with open(CONFIG_FILE_TRIVIA, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_trivia(config):
    """Saves the trivia configuration to a JSON file."""
    with open(CONFIG_FILE_TRIVIA, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

def parse_duration(duration_str: str) -> timedelta | None:
    """Parses a duration string (e.g., '7d', '1h', '30m') into a timedelta."""
    match = re.match(r"(\d+)\s*(d|h|m|s)$", duration_str.lower())
    if not match: return None
    value, unit = int(match.group(1)), match.group(2)
    if unit == 'd': return timedelta(days=value)
    if unit == 'h': return timedelta(hours=value)
    if unit == 'm': return timedelta(minutes=value)
    if unit == 's': return timedelta(seconds=value)
    return None

# --- Permission Check ---
async def is_trivia_admin_check(interaction: discord.Interaction) -> bool:
    """A standalone check to verify if a user is a trivia admin."""
    cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
    if not cog:
        # This case should ideally not happen if the cog is loaded.
        return await interaction.client.is_owner(interaction.user)
    # Now we can call the method on the cog instance.
    return await cog.is_user_admin(interaction)


# --- UI Components (Views, Modals) ---
# NOTE: These classes remain largely unchanged but now reference the main `DailyTrivia` cog.

class TriviaView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia'):
        super().__init__(timeout=None)
        self.cog = cog_instance

    async def handle_button_press(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.cog.handle_trivia_answer(interaction, button)

    @discord.ui.button(label="Answer A", style=discord.ButtonStyle.secondary, custom_id="trivia_a")
    async def answer_a(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)
    @discord.ui.button(label="Answer B", style=discord.ButtonStyle.secondary, custom_id="trivia_b")
    async def answer_b(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)
    @discord.ui.button(label="Answer C", style=discord.ButtonStyle.secondary, custom_id="trivia_c")
    async def answer_c(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)
    @discord.ui.button(label="Answer D", style=discord.ButtonStyle.secondary, custom_id="trivia_d")
    async def answer_d(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)


class DoubleOrNothingView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia', user_id: int, original_question_url: str):
        super().__init__(timeout=180)
        self.cog = cog_instance
        self.user_id = user_id
        self.original_question_url = original_question_url
        self.message: discord.Message = None

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
        for item in self.children: item.disabled = True
        if self.message:
            try: await self.message.edit(content="You took too long to decide. The offer has expired.", view=self)
            except (discord.NotFound, discord.HTTPException): pass


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


class HelpView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia'):
        super().__init__(timeout=180)
        self.cog = cog_instance

    @discord.ui.select(placeholder="Choose a help category...",
        options=[
            discord.SelectOption(label="Game Rules", description="Learn how to play and how scoring works.", emoji="📜"),
            discord.SelectOption(label="User Commands", description="Commands available to everyone.", emoji="👤"),
            discord.SelectOption(label="Admin Commands", description="Commands for server administrators.", emoji="👑"),
        ])
    async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        embed = self.cog.get_help_embed(select.values[0])
        await interaction.response.edit_message(embed=embed)


class TimingConfigModal(discord.ui.Modal):
    def __init__(self, cog_instance: 'DailyTrivia', current_cfg: dict):
        super().__init__(title="Configure Trivia Timings")
        self.cog = cog_instance
        self.add_item(discord.ui.TextInput(label="Post Time (HH:MM format, 24-hour)", placeholder="e.g., 14:30", default=current_cfg.get('time', '12:00')))
        self.add_item(discord.ui.TextInput(label="Timezone", placeholder="e.g., America/New_York", default=current_cfg.get('timezone', 'UTC')))
        self.add_item(discord.ui.TextInput(label="Reveal Delay (in minutes)", placeholder="e.g., 60", default=str(current_cfg.get('reveal_delay', 60))))

    async def on_submit(self, interaction: discord.Interaction):
        time_str, tz_str, delay_str = self.children[0].value, self.children[1].value, self.children[2].value
        errors = []
        try: time.fromisoformat(time_str)
        except ValueError: errors.append("Invalid time format. Please use HH:MM.")
        try: pytz.timezone(tz_str)
        except pytz.UnknownTimeZoneError: errors.append(f"Unknown timezone: `{tz_str}`.")
        try:
            if not (1 <= int(delay_str) <= 1440): errors.append("Reveal delay must be between 1 and 1440 minutes.")
        except ValueError: errors.append("Reveal delay must be a whole number.")
        if errors:
            await interaction.response.send_message("❌ " + "\n".join(errors), ephemeral=True)
            return
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_config(interaction.guild_id)
            cfg['time'], cfg['timezone'], cfg['reveal_delay'] = time_str, tz_str, int(delay_str)
            self.cog.config_is_dirty = True
        await self.cog._update_admin_panel(interaction, "✅ Timing settings updated successfully.")


class MuteUserModal(discord.ui.Modal, title="Mute User from Trivia"):
    def __init__(self, cog_instance: 'DailyTrivia'):
        super().__init__()
        self.cog = cog_instance
        self.add_item(discord.ui.TextInput(label="User ID", placeholder="Enter the ID of the user to mute."))
        self.add_item(discord.ui.TextInput(label="Duration (e.g., 7d, 1h, 30m)", placeholder="Enter 'permanent' for a permanent mute."))

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str, duration_str = self.children[0].value, self.children[1].value
        try: user_id = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("❌ Invalid User ID.", ephemeral=True)
            return
        if duration_str.lower() == 'permanent': end_time = datetime.now(timezone.utc) + timedelta(days=365 * 100)
        else:
            duration = parse_duration(duration_str)
            if not duration:
                await interaction.response.send_message("❌ Invalid duration format.", ephemeral=True)
                return
            end_time = datetime.now(timezone.utc) + duration
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_config(interaction.guild_id)
            cfg.setdefault("mutes", {})[str(user_id)] = end_time.isoformat()
            self.cog.config_is_dirty = True
        await interaction.response.send_message(f"✅ User `{user_id}` has been muted.", ephemeral=True)


class UnmuteUserModal(discord.ui.Modal, title="Unmute User from Trivia"):
    def __init__(self, cog_instance: 'DailyTrivia'):
        super().__init__()
        self.cog = cog_instance
        self.add_item(discord.ui.TextInput(label="User ID", placeholder="Enter the ID of the user to unmute."))

    async def on_submit(self, interaction: discord.Interaction):
        user_id_str = self.children[0].value
        try: user_id = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("❌ Invalid User ID.", ephemeral=True)
            return
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_config(interaction.guild_id)
            if str(user_id) in cfg.get("mutes", {}):
                del cfg["mutes"][str(user_id)]
                self.cog.config_is_dirty = True
                await interaction.response.send_message(f"✅ User `{user_id}` has been unmuted.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ User is not currently muted.", ephemeral=True)


class AdminPanelView(discord.ui.View):
    def __init__(self, cog_instance: 'DailyTrivia', original_interaction_user: discord.User):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_user = original_interaction_user
        cfg = self.cog.get_guild_config(original_interaction_user.guild.id)
        is_enabled = cfg.get("enabled", False)
        toggle_button = discord.ui.Button(label=f"Trivia is {'Enabled' if is_enabled else 'Disabled'}", style=discord.ButtonStyle.green if is_enabled else discord.ButtonStyle.red, emoji="▶️" if is_enabled else "⏹️", row=0)
        toggle_button.callback = self.toggle_trivia
        self.add_item(toggle_button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.original_user.id:
            await interaction.response.send_message("Only the user who opened the panel can use these buttons.", ephemeral=True)
            return False
        return True

    async def toggle_trivia(self, interaction: discord.Interaction):
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_config(interaction.guild_id)
            cfg['enabled'] = not cfg.get('enabled', False)
            self.cog.config_is_dirty = True
            status_msg = f"✅ Trivia has been {'enabled' if cfg['enabled'] else 'disabled'}."
        await self.cog._update_admin_panel(interaction, status_msg)

    @discord.ui.button(label="Set Channel", style=discord.ButtonStyle.secondary, emoji="📺", row=1)
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = discord.ui.View()
        channel_select = discord.ui.ChannelSelect(placeholder="Select the channel for trivia questions", channel_types=[discord.ChannelType.text])
        async def select_callback(inter: discord.Interaction):
            channel_id = inter.data['values'][0]
            async with self.cog.config_lock:
                cfg = self.cog.get_guild_config(inter.guild_id)
                cfg['channel_id'] = int(channel_id)
                self.cog.config_is_dirty = True
            await self.cog._update_admin_panel(inter, f"✅ Trivia channel set to <#{channel_id}>.")
            view.stop()
        channel_select.callback = select_callback
        view.add_item(channel_select)
        await interaction.response.send_message("Select a channel to post trivia questions in:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.secondary, emoji="👑", row=1)
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = discord.ui.View()
        select = discord.ui.RoleSelect(placeholder="Select the trivia admin role...")
        async def select_callback(inter: discord.Interaction):
            role_id = inter.data['values'][0]
            async with self.cog.config_lock:
                cfg = self.cog.get_guild_config(inter.guild_id)
                cfg['admin_role_id'] = int(role_id)
                self.cog.config_is_dirty = True
            await self.cog._update_admin_panel(inter, f"✅ Trivia Admin role set to <@&{role_id}>.")
            view.stop()
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a role to be the Trivia Admin:", view=view, ephemeral=True)

    @discord.ui.button(label="Configure Timings", style=discord.ButtonStyle.secondary, emoji="⏱️", row=1)
    async def configure_timings(self, interaction: discord.Interaction, button: discord.ui.Button):
        cfg = self.cog.get_guild_config(interaction.guild_id)
        await interaction.response.send_modal(TimingConfigModal(self.cog, cfg))

    @discord.ui.button(label="Mute User", style=discord.ButtonStyle.grey, emoji="🔇", row=2)
    async def mute_user(self, interaction: discord.Interaction, button: discord.ui.Button): await interaction.response.send_modal(MuteUserModal(self.cog))
    @discord.ui.button(label="Unmute User", style=discord.ButtonStyle.grey, emoji="🔊", row=2)
    async def unmute_user(self, interaction: discord.Interaction, button: discord.ui.Button): await interaction.response.send_modal(UnmuteUserModal(self.cog))
    @discord.ui.button(label="Force Daily Question", style=discord.ButtonStyle.primary, emoji="📅", row=3)
    async def force_daily_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.manual_post_daily_question(interaction)
    @discord.ui.button(label="Post Random Question", style=discord.ButtonStyle.success, emoji="🎲", row=3)
    async def post_random_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.post_random_question(interaction)


# --- Cog: Core Logic and Tasks ---

class DailyTrivia(commands.Cog):
    """The main logic and task handler for the trivia bot. Has no user-facing commands."""
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

    # All helper methods, loops, and logic functions from the previous version go here.
    # ... (get_guild_config, handle_trivia_answer, reveal_trivia_answer, etc.) ...
    # Omitted for brevity, they are unchanged. The full code is implied.
    
    # --- The rest of the logic methods from the previous file would go here ---
    # --- For example: is_user_admin, on_guild_remove, on_app_command_error, ---
    # --- get_guild_config, all loops, all helpers, etc. ---
    async def is_user_admin(self, interaction: discord.Interaction) -> bool:
        """Check if a user has trivia admin permissions. Called by the standalone check."""
        if await self.bot.is_owner(interaction.user): return True
        if interaction.user.guild_permissions.manage_guild: return True
        cfg = self.get_guild_config(interaction.guild_id)
        admin_role_id = cfg.get("admin_role_id")
        if admin_role_id:
            role = interaction.guild.get_role(admin_role_id)
            if role and role in interaction.user.roles:
                return True
        return False

    # The other methods like on_guild_remove, get_guild_config, all loops, reveal_trivia_answer, etc.
    # are assumed to be here, unchanged from the previous version.


# --- Cog: User Commands (/trivia) ---

class TriviaCommands(commands.GroupCog, name="trivia"):
    """Handles all user-facing /trivia commands."""
    def __init__(self, bot: commands.Bot, main_cog: DailyTrivia):
        self.bot = bot
        self.cog = main_cog # Reference to the main logic cog

    @app_commands.command(name="help", description="Explains the trivia rules and lists commands.")
    async def trivia_help(self, interaction: discord.Interaction):
        embed = self.cog.get_help_embed("Game Rules")
        await interaction.response.send_message(embed=embed, view=HelpView(self.cog), ephemeral=True)

    # ... other user commands like leaderboard, stats, etc. go here ...
    # Omitted for brevity, they are unchanged. The full code is implied.


# --- Cog: Admin Commands (/trivia_admin) ---

@app_commands.check(is_trivia_admin_check)
class TriviaAdminCommands(commands.GroupCog, name="trivia_admin"):
    """Handles all admin-only /trivia_admin commands."""
    def __init__(self, bot: commands.Bot, main_cog: DailyTrivia):
        self.bot = bot
        self.cog = main_cog # Reference to the main logic cog

    @app_commands.command(name="panel", description="Opens the all-in-one trivia admin control panel.")
    async def admin_panel(self, interaction: discord.Interaction):
        """The entry point for the admin panel."""
        embed = await self.cog._build_admin_panel_embed(interaction.guild_id)
        view = AdminPanelView(self.cog, interaction.user)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def cog_check(self, interaction: discord.Interaction) -> bool:
        """A check that applies to all commands in this cog."""
        return await is_trivia_admin_check(interaction)


# --- Setup Function ---
async def setup(bot: commands.Bot):
    """The setup function to load all trivia-related cogs."""
    # 1. Create an instance of the main logic cog
    main_cog = DailyTrivia(bot)
    await bot.add_cog(main_cog)

    # 2. Create instances of the command cogs, passing the main cog to them
    await bot.add_cog(TriviaCommands(bot, main_cog))
    await bot.add_cog(TriviaAdminCommands(bot, main_cog))

