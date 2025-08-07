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
    if os.path.exists(CONFIG_FILE_TRIVIA):
        try:
            with open(CONFIG_FILE_TRIVIA, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_trivia(config):
    with open(CONFIG_FILE_TRIVIA, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

def parse_duration(duration_str: str) -> timedelta | None:
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
    if not cog: return await interaction.client.is_owner(interaction.user)
    return await cog.is_user_admin(interaction)

# --- UI Components (Views, Modals) ---
# These classes are now self-contained and reference the main cog via interaction.client
class TriviaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def handle_button_press(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await cog.handle_trivia_answer(interaction, button)

    @discord.ui.button(label="Answer A", style=discord.ButtonStyle.secondary, custom_id="trivia_a")
    async def answer_a(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)
    @discord.ui.button(label="Answer B", style=discord.ButtonStyle.secondary, custom_id="trivia_b")
    async def answer_b(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)
    @discord.ui.button(label="Answer C", style=discord.ButtonStyle.secondary, custom_id="trivia_c")
    async def answer_c(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)
    @discord.ui.button(label="Answer D", style=discord.ButtonStyle.secondary, custom_id="trivia_d")
    async def answer_d(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_button_press(interaction, button)

class DoubleOrNothingView(discord.ui.View):
    def __init__(self, user_id: int, original_question_url: str):
        super().__init__(timeout=180)
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
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
        await interaction.response.defer()
        button.disabled = True
        await interaction.edit_original_response(view=self)
        cog.don_pending_users.add(self.user_id)
        await cog.start_double_or_nothing_game(interaction, self.user_id)

    async def on_timeout(self):
        cog: 'DailyTrivia' = self.message.client.get_cog("DailyTrivia") if self.message else None
        if cog: cog.don_pending_users.discard(self.user_id)
        for item in self.children: item.disabled = True
        if self.message:
            try: await self.message.edit(content="You took too long to decide. The offer has expired.", view=self)
            except (discord.NotFound, discord.HTTPException): pass

class DONQuestionView(discord.ui.View):
    def __init__(self, user_id: int, correct_answer: str):
        super().__init__(timeout=30.0)
        self.user_id = user_id
        self.correct_answer = correct_answer
        self.answered = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This is not for you!", ephemeral=True)
            return False
        return True

    async def handle_don_answer(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
        self.answered = True
        self.stop()
        cog.don_pending_users.discard(self.user_id)
        for item in self.children: item.disabled = True
        is_correct = (button.label == self.correct_answer)
        async with cog.config_lock:
            cfg = cog.get_guild_config(interaction.guild_id)
            firsts = cfg.setdefault("monthly_firsts", {})
            user_id_str = str(self.user_id)
            if is_correct:
                firsts[user_id_str] = firsts.get(user_id_str, 0) + 1
                embed = discord.Embed(title="🎉 You did it!", description="You answered correctly and earned a bonus point!", color=discord.Color.green())
            else:
                firsts[user_id_str] = firsts.get(user_id_str, 1) - 1
                embed = discord.Embed(title="❌ Oh no!", description=f"That was incorrect. The correct answer was **{self.correct_answer}**.\nYou lost the point you just earned.", color=discord.Color.red())
            cog.config_is_dirty = True
        await interaction.response.edit_message(embed=embed, view=self)

class HelpView(discord.ui.View):
    @discord.ui.select(placeholder="Choose a help category...",
        options=[
            discord.SelectOption(label="Game Rules", description="Learn how to play and how scoring works.", emoji="📜"),
            discord.SelectOption(label="User Commands", description="Commands available to everyone.", emoji="👤"),
            discord.SelectOption(label="Admin Commands", description="Commands for server administrators.", emoji="👑"),
        ])
    async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
        embed = cog.get_help_embed(select.values[0])
        await interaction.response.edit_message(embed=embed)

class TimingConfigModal(discord.ui.Modal):
    def __init__(self, current_cfg: dict):
        super().__init__(title="Configure Trivia Timings")
        self.add_item(discord.ui.TextInput(label="Post Time (HH:MM format, 24-hour)", placeholder="e.g., 14:30", default=current_cfg.get('time', '12:00')))
        self.add_item(discord.ui.TextInput(label="Timezone", placeholder="e.g., America/New_York", default=current_cfg.get('timezone', 'UTC')))
        self.add_item(discord.ui.TextInput(label="Reveal Delay (in minutes)", placeholder="e.g., 60", default=str(current_cfg.get('reveal_delay', 60))))

    async def on_submit(self, interaction: discord.Interaction):
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
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
        async with cog.config_lock:
            cfg = cog.get_guild_config(interaction.guild_id)
            cfg['time'], cfg['timezone'], cfg['reveal_delay'] = time_str, tz_str, int(delay_str)
            cog.config_is_dirty = True
        await cog._update_admin_panel(interaction, "✅ Timing settings updated successfully.")

class MuteUserModal(discord.ui.Modal, title="Mute User from Trivia"):
    def __init__(self):
        super().__init__()
        self.add_item(discord.ui.TextInput(label="User ID", placeholder="Enter the ID of the user to mute."))
        self.add_item(discord.ui.TextInput(label="Duration (e.g., 7d, 1h, 30m)", placeholder="Enter 'permanent' for a permanent mute."))

    async def on_submit(self, interaction: discord.Interaction):
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
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
        async with cog.config_lock:
            cfg = cog.get_guild_config(interaction.guild_id)
            cfg.setdefault("mutes", {})[str(user_id)] = end_time.isoformat()
            cog.config_is_dirty = True
        await interaction.response.send_message(f"✅ User `{user_id}` has been muted.", ephemeral=True)

class UnmuteUserModal(discord.ui.Modal, title="Unmute User from Trivia"):
    def __init__(self):
        super().__init__()
        self.add_item(discord.ui.TextInput(label="User ID", placeholder="Enter the ID of the user to unmute."))

    async def on_submit(self, interaction: discord.Interaction):
        cog: 'DailyTrivia' = interaction.client.get_cog("DailyTrivia")
        if not cog: return
        user_id_str = self.children[0].value
        try: user_id = int(user_id_str)
        except ValueError:
            await interaction.response.send_message("❌ Invalid User ID.", ephemeral=True)
            return
        async with cog.config_lock:
            cfg = cog.get_guild_config(interaction.guild_id)
            if str(user_id) in cfg.get("mutes", {}):
                del cfg["mutes"][str(user_id)]
                cog.config_is_dirty = True
                await interaction.response.send_message(f"✅ User `{user_id}` has been unmuted.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ User is not currently muted.", ephemeral=True)

class AdminPanelView(discord.ui.View):
    def __init__(self, original_interaction_user: discord.User):
        super().__init__(timeout=300)
        self.original_user = original_interaction_user
        cog: 'DailyTrivia' = original_interaction_user.client.get_cog("DailyTrivia")
        cfg = cog.get_guild_config(original_interaction_user.guild.id)
        is_enabled = cfg.get("enabled", False)
        toggle_button = discord.ui.Button(label=f"Trivia is {'Enabled' if is_enabled else 'Disabled'}", style=discord.ButtonStyle.green if is_enabled else discord.ButtonStyle.red, emoji="▶️" if is_enabled else "⏹️", row=0)
        toggle_button.callback = self.toggle_trivia
        self.add_item(toggle_button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.original_user.id:
            await interaction.response.send_message("Only the user who opened the panel can use these buttons.", ephemeral=True)
            return False
        return True
    
    async def get_cog(self, interaction: discord.Interaction) -> 'DailyTrivia':
        return interaction.client.get_cog("DailyTrivia")

    async def toggle_trivia(self, interaction: discord.Interaction):
        cog = await self.get_cog(interaction)
        if not cog: return
        async with cog.config_lock:
            cfg = cog.get_guild_config(interaction.guild_id)
            cfg['enabled'] = not cfg.get('enabled', False)
            cog.config_is_dirty = True
            status_msg = f"✅ Trivia has been {'enabled' if cfg['enabled'] else 'disabled'}."
        await cog._update_admin_panel(interaction, status_msg)

    @discord.ui.button(label="Set Channel", style=discord.ButtonStyle.secondary, emoji="📺", row=1)
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self.get_cog(interaction)
        if not cog: return
        view = discord.ui.View()
        channel_select = discord.ui.ChannelSelect(placeholder="Select the channel for trivia questions", channel_types=[discord.ChannelType.text])
        async def select_callback(inter: discord.Interaction):
            channel_id = inter.data['values'][0]
            async with cog.config_lock:
                cfg = cog.get_guild_config(inter.guild_id)
                cfg['channel_id'] = int(channel_id)
                cog.config_is_dirty = True
            await cog._update_admin_panel(inter, f"✅ Trivia channel set to <#{channel_id}>.")
            view.stop()
        channel_select.callback = select_callback
        view.add_item(channel_select)
        await interaction.response.send_message("Select a channel to post trivia questions in:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.secondary, emoji="👑", row=1)
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self.get_cog(interaction)
        if not cog: return
        view = discord.ui.View()
        select = discord.ui.RoleSelect(placeholder="Select the trivia admin role...")
        async def select_callback(inter: discord.Interaction):
            role_id = inter.data['values'][0]
            async with cog.config_lock:
                cfg = cog.get_guild_config(inter.guild_id)
                cfg['admin_role_id'] = int(role_id)
                cog.config_is_dirty = True
            await cog._update_admin_panel(inter, f"✅ Trivia Admin role set to <@&{role_id}>.")
            view.stop()
        select.callback = select_callback
        view.add_item(select)
        await interaction.response.send_message("Select a role to be the Trivia Admin:", view=view, ephemeral=True)

    @discord.ui.button(label="Configure Timings", style=discord.ButtonStyle.secondary, emoji="⏱️", row=1)
    async def configure_timings(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self.get_cog(interaction)
        if not cog: return
        cfg = cog.get_guild_config(interaction.guild_id)
        await interaction.response.send_modal(TimingConfigModal(cfg))

    @discord.ui.button(label="Mute User", style=discord.ButtonStyle.grey, emoji="🔇", row=2)
    async def mute_user(self, interaction: discord.Interaction, button: discord.ui.Button): await interaction.response.send_modal(MuteUserModal())
    @discord.ui.button(label="Unmute User", style=discord.ButtonStyle.grey, emoji="🔊", row=2)
    async def unmute_user(self, interaction: discord.Interaction, button: discord.ui.Button): await interaction.response.send_modal(UnmuteUserModal())
    @discord.ui.button(label="Force Daily Question", style=discord.ButtonStyle.primary, emoji="📅", row=3)
    async def force_daily_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self.get_cog(interaction)
        if not cog: return
        await interaction.response.defer(ephemeral=True)
        await cog.manual_post_daily_question(interaction)
    @discord.ui.button(label="Post Random Question", style=discord.ButtonStyle.success, emoji="🎲", row=3)
    async def post_random_question(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self.get_cog(interaction)
        if not cog: return
        await interaction.response.defer(ephemeral=True)
        await cog.post_random_question(interaction)

# --- Main Cog Class ---
# This class now holds ALL logic, tasks, AND commands.
# This is a simpler, more robust structure that avoids the previous error.
@app_commands.guild_only()
class DailyTrivia(commands.Cog):
    # Create command groups directly in the class
    trivia = app_commands.Group(name="trivia", description="Commands for the daily trivia.")
    trivia_admin = app_commands.Group(name="trivia_admin", description="Admin commands for the daily trivia.", default_permissions=discord.Permissions(manage_guild=True))

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
        self.bot.add_view(TriviaView())

    def cog_unload(self):
        if self.config_is_dirty:
            log_trivia.info("Performing final trivia config save on cog unload.")
            save_config_trivia(self.config)
        self.trivia_loop.cancel()
        self.monthly_winner_loop.cancel()
        self.cache_refill_loop.cancel()
        self.save_loop.cancel()
        asyncio.create_task(self.session.close())

    async def is_user_admin(self, interaction: discord.Interaction) -> bool:
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

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {
                "channel_id": None, "time": "12:00", "timezone": "UTC", "enabled": False,
                "admin_role_id": None, "reveal_delay": 60,
                "last_winner_announcement": datetime.now(timezone.utc).isoformat(),
                "last_day_winner_id": None, "last_question_data": None, "last_posted_date": None,
                "pending_answers": [], "asked_questions": [], "question_cache": [],
                "daily_interactions": [], "mutes": {}, "question_stats": [],
                "monthly_firsts": {}, "monthly_correct_answers": {}
            }
        defaults = {
            "monthly_firsts": {}, "monthly_correct_answers": {}, "last_winner_announcement": "2000-01-01T00:00:00.000000+00:00",
            "question_cache": [], "reveal_delay": 60, "daily_interactions": [], "admin_role_id": None,
            "last_day_winner_id": None, "last_question_data": None, "mutes": {}, "question_stats": [],
            "last_posted_date": None, "asked_questions": []
        }
        for key, value in defaults.items(): self.config[gid].setdefault(key, value)
        return self.config[gid]

    # --- All other helper methods, loops, and logic functions go here ---
    # Omitted for brevity, they are unchanged from the previous correct version.
    # ... (save_loop, fetch_api_questions, handle_trivia_answer, etc.) ...
    
    # --- Application Commands ---
    
    @trivia.command(name="help", description="Explains the trivia rules and lists commands.")
    async def trivia_help(self, interaction: discord.Interaction):
        embed = self.get_help_embed("Game Rules")
        await interaction.response.send_message(embed=embed, view=HelpView(), ephemeral=True)

    @trivia.command(name="leaderboard", description="Shows the monthly leaderboard for total correct answers.")
    async def trivia_leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        cfg = self.get_guild_config(interaction.guild_id)
        scores = cfg.get("monthly_correct_answers", {})
        if not scores:
            await interaction.followup.send(embed=discord.Embed(description="The monthly leaderboard is empty!", color=EMBED_COLOR_TRIVIA))
            return
        # ... leaderboard logic ...

    @trivia.command(name="firstsboard", description="Shows the monthly leaderboard for fastest correct answers.")
    async def trivia_firstsboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        cfg = self.get_guild_config(interaction.guild_id)
        scores = cfg.get("monthly_firsts", {})
        if not scores:
            await interaction.followup.send(embed=discord.Embed(description="The monthly firsts board is empty!", color=EMBED_COLOR_TRIVIA))
            return
        # ... firstsboard logic ...

    @trivia.command(name="stats", description="Shows your personal trivia statistics or those of another user.")
    @app_commands.describe(user="The user whose stats you want to see (optional).")
    async def trivia_stats(self, interaction: discord.Interaction, user: discord.Member = None):
        # ... stats logic ...
        pass

    @trivia_admin.command(name="panel", description="Opens the all-in-one trivia admin control panel.")
    @app_commands.check(is_trivia_admin_check)
    async def admin_panel(self, interaction: discord.Interaction):
        embed = await self._build_admin_panel_embed(interaction.guild_id)
        view = AdminPanelView(interaction.user)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    # --- Helper methods for building embeds and updating the panel ---
    async def _build_admin_panel_embed(self, guild_id: int) -> discord.Embed:
        cfg = self.get_guild_config(guild_id)
        embed = discord.Embed(title="⚙️ Trivia Admin Panel", description="Manage the daily trivia game for this server.", color=EMBED_COLOR_TRIVIA)
        status = "Enabled" if cfg.get('enabled') else "Disabled"
        channel_mention = f"<#{cfg['channel_id']}>" if cfg.get('channel_id') else "Not Set"
        admin_role_mention = f"<@&{cfg['admin_role_id']}>" if cfg.get('admin_role_id') else "Not Set"
        embed.add_field(name="Status", value=f"**{status}**", inline=True)
        embed.add_field(name="Post Channel", value=channel_mention, inline=True)
        embed.add_field(name="Admin Role", value=admin_role_mention, inline=True)
        embed.add_field(name="Timings", value=f"Post Time: `{cfg.get('time', '12:00')}`\nTimezone: `{cfg.get('timezone', 'UTC')}`\nReveal Delay: `{cfg.get('reveal_delay', 60)}m`", inline=False)
        return embed

    async def _update_admin_panel(self, interaction: discord.Interaction, response_text: str | None = None):
        embed = await self._build_admin_panel_embed(interaction.guild_id)
        view = AdminPanelView(interaction.user)
        # Use response.edit_message for subsequent updates from the panel itself
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=response_text, embed=embed, view=view)
        else:
            await interaction.edit_original_response(content=response_text, embed=embed, view=view)
            
    # NOTE: The rest of the cog's methods (loops, helpers, etc.) are assumed to be here, unchanged.
    # They have been omitted to keep the code block focused on the structural fix.


# --- Setup Function ---
async def setup(bot: commands.Bot):
    """The setup function to load the single, unified trivia cog."""
    await bot.add_cog(DailyTrivia(bot))


After you perform these two steps, your bot should load correctly. We're very close!
