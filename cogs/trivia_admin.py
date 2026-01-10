import discord
from discord.ext import commands
from discord import app_commands
import random
import typing
from datetime import datetime, timezone
import logging

# Import from the main cog's file
from .trivia_main import FAQ_TEXT, EMBED_COLOR_TRIVIA, ANTI_CHEAT_QUESTIONS, BonusGatewayView
if typing.TYPE_CHECKING:
    from .trivia_main import DailyTrivia
from .trivia_main import is_trivia_admin_check, TriviaPostingError

# Use the same logger as trivia_main
log_trivia = logging.getLogger("discord.trivia")

# =====================================================================================
# ADMIN-ONLY UI COMPONENTS
# =====================================================================================

class ConfirmView(discord.ui.View):
    def __init__(self, interaction_check_user: discord.User):
        super().__init__(timeout=60)
        self.value, self.interaction_check_user = None, interaction_check_user
        self.message = None # To store message reference for cleanup
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.interaction_check_user.id:
            await interaction.response.send_message("You cannot interact with this confirmation.", ephemeral=True)
            return False
        return True
    @discord.ui.button(label='Confirm', style=discord.ButtonStyle.danger)
    async def confirm(self, i: discord.Interaction, b: discord.ui.Button): self.value=True; self.stop(); await i.response.defer()
    @discord.ui.button(label='Cancel', style=discord.ButtonStyle.grey)
    async def cancel(self, i: discord.Interaction, b: discord.ui.Button): self.value=False; self.stop(); await i.response.defer()

    async def on_timeout(self):
        # Cleanup logic for ConfirmView
        if self.value is None and self.message:
            try:
                for item in self.children: item.disabled = True
                await self.message.edit(content="Confirmation timed out.", view=self)
            except discord.HTTPException: pass

class TriviaSettingsModal(discord.ui.Modal, title='Trivia Settings'):
    def __init__(self, main_cog: "DailyTrivia", guild_settings: dict):
        super().__init__(timeout=180)
        self.main_cog = main_cog
        # Use guild_settings
        self.channel_id = discord.ui.TextInput(label="Trivia Channel/Thread ID", default=str(guild_settings.get("channel_id") or ""), required=True)
        self.enabled = discord.ui.TextInput(label="Enable Trivia (True/False)", default=str(guild_settings.get("enabled", False)), max_length=5, required=True)
        self.results_channel_id = discord.ui.TextInput(label="Anti-Cheat Results Channel ID", default=str(guild_settings.get("anti_cheat_results_channel_id") or ""), required=False)
        self.add_item(self.channel_id); self.add_item(self.enabled); self.add_item(self.results_channel_id)
        
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            channel_id = int(self.channel_id.value); channel = await self.main_cog.bot.fetch_channel(channel_id)
        except (ValueError, discord.NotFound): return await interaction.followup.send("Invalid Channel ID.", ephemeral=True)
        
        enabled_input = self.enabled.value.strip().lower()
        if enabled_input not in ['true', 'false']:
            return await interaction.followup.send("Enable Trivia must be 'True' or 'False'.", ephemeral=True)
        is_enabled = enabled_input == 'true'

        results_id = int(self.results_channel_id.value) if self.results_channel_id.value.strip().isdigit() else None
        async with self.main_cog.config_lock:
            # Get guild_settings
            cfg = self.main_cog.get_guild_settings(interaction.guild.id)
            cfg["channel_id"], cfg["enabled"], cfg["anti_cheat_results_channel_id"] = channel_id, is_enabled, results_id
            self.main_cog.config_is_dirty = True
        await interaction.followup.send(f"✅ Settings updated!", ephemeral=True)
        self.main_cog.trivia_loop.restart()

class RoleSettingsModal(discord.ui.Modal, title='Set Trivia Winner Role'):
    def __init__(self, main_cog: "DailyTrivia", guild_settings: dict):
        super().__init__(timeout=180)
        self.main_cog, self.guild_settings = main_cog, guild_settings
        self.role_id = discord.ui.TextInput(label="Winner Role ID (blank to disable)", default=str(guild_settings.get("winner_role_id") or ""), required=False)
        self.add_item(self.role_id)
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if not (role_id_str := self.role_id.value.strip()):
            async with self.main_cog.config_lock:
                # Get guild_settings
                self.main_cog.get_guild_settings(interaction.guild.id)["winner_role_id"] = None; self.main_cog.config_is_dirty = True
            return await interaction.followup.send("✅ Trivia winner role disabled.", ephemeral=True)
        try:
            role_id = int(role_id_str)
            role = interaction.guild.get_role(role_id)
            if not role: return await interaction.followup.send(f"❌ Role with ID `{role_id}` not found.", ephemeral=True)
            if interaction.guild.me.top_role <= role: return await interaction.followup.send(f"❌ My role is too low to manage {role.mention}.", ephemeral=True)
        except ValueError: return await interaction.followup.send("Role ID must be a number.", ephemeral=True)
        async with self.main_cog.config_lock:
            # Get guild_settings
            self.main_cog.get_guild_settings(interaction.guild.id)["winner_role_id"] = role_id; self.main_cog.config_is_dirty = True
        await interaction.followup.send(f"✅ Trivia winner role set to {role.mention}!", ephemeral=True)

class AdjustPointsModal(discord.ui.Modal, title="Adjust User Points"):
    def __init__(self, main_cog: "DailyTrivia"):
        super().__init__(timeout=180)
        self.main_cog = main_cog
        self.user_id = discord.ui.TextInput(label="User ID")
        self.points = discord.ui.TextInput(label="Points to Add/Remove", placeholder="e.g., 5 or -5")
        self.add_item(self.user_id); self.add_item(self.points)
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            user_id_val, points_val = int(self.user_id.value), int(self.points.value)
            user = await self.main_cog.bot.fetch_user(user_id_val)
        except (ValueError, discord.NotFound): return await interaction.followup.send("Invalid User ID or Points.", ephemeral=True)
        
        async with self.main_cog.config_lock:
            # Get global data
            global_data = self.main_cog.get_global_data()
            stats = self.main_cog.get_user_stats(user_id_val) # Global
            score_data = global_data.setdefault("scores", {}).setdefault(str(user_id_val), {"score": 0, "timestamp": None}) # Global
            
            # Ensure score_data is a dict (migration from old int format)
            if isinstance(score_data, int):
                score_data = {"score": score_data, "timestamp": None}
                global_data["scores"][str(user_id_val)] = score_data
            
            score_data["score"] = score_data.get("score", 0) + points_val
            stats["all_time_score"] = stats.get("all_time_score", 0) + points_val
            
            # FIX: Update timestamps ONLY when ADDING points (for proper tie-breaking)
            if points_val > 0:
                now_iso = datetime.now(timezone.utc).isoformat()
                score_data["timestamp"] = now_iso
                stats["all_time_timestamp"] = now_iso
            
            self.main_cog.config_is_dirty = True
        await interaction.followup.send(f"✅ Adjusted `{user.name}`'s score by **{points_val}**.", ephemeral=True)
        await self.main_cog.update_gateway_message(interaction.guild.id)

class UserActionModal(discord.ui.Modal):
    def __init__(self, main_cog: "DailyTrivia", action: str):
        self.main_cog, self.action = main_cog, action
        super().__init__(title=f"{action.replace('_', ' ').title()} a User", timeout=180)
        self.user_id = discord.ui.TextInput(label="User ID", placeholder=f"Enter User ID to {action.replace('_', ' ')}")
        self.add_item(self.user_id)
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try: user_id_val = int(self.user_id.value)
        except ValueError: return await interaction.followup.send("Invalid User ID.", ephemeral=True)
        
        message, user_id_str = "", str(user_id_val)
        async with self.main_cog.config_lock:
            # Get global data
            global_data = self.main_cog.get_global_data()
            if self.action == 'purge_all_data':
                # Purge from global data
                global_data.get("scores", {}).pop(user_id_str, None)
                global_data.get("user_stats", {}).pop(user_id_str, None)
                message = f"✅ All trivia data purged for user `{user_id_val}`."
            elif self.action == 'block':
                # Block in global data
                if user_id_val not in global_data.setdefault("blocked_users", []): global_data["blocked_users"].append(user_id_val)
                message = f"✅ User `{user_id_val}` is now blocked."
            elif self.action == 'unblock':
                # Unblock in global data
                if user_id_val in global_data.setdefault("blocked_users", []): global_data["blocked_users"].remove(user_id_val)
                message = f"✅ User `{user_id_val}` has been unblocked."
            self.main_cog.config_is_dirty = True
        await interaction.followup.send(message, ephemeral=True)

class UserManagementView(discord.ui.View):
    def __init__(self, main_cog: "DailyTrivia"): super().__init__(timeout=180); self.main_cog = main_cog
    @discord.ui.button(label="Adjust Points", style=discord.ButtonStyle.primary)
    async def adjust(self, i: discord.Interaction, b: discord.ui.Button): await i.response.send_modal(AdjustPointsModal(self.main_cog))
    @discord.ui.button(label="Block User", style=discord.ButtonStyle.danger)
    async def block(self, i: discord.Interaction, b: discord.ui.Button): await i.response.send_modal(UserActionModal(self.main_cog, "block"))
    @discord.ui.button(label="Unblock User", style=discord.ButtonStyle.secondary)
    async def unblock(self, i: discord.Interaction, b: discord.ui.Button): await i.response.send_modal(UserActionModal(self.main_cog, "unblock"))
    @discord.ui.button(label="Purge User Data", style=discord.ButtonStyle.danger, row=2)
    async def purge(self, i: discord.Interaction, b: discord.ui.Button): await i.response.send_modal(UserActionModal(self.main_cog, "purge_all_data"))

class DataManagementView(discord.ui.View):
    def __init__(self, main_cog: "DailyTrivia"): super().__init__(timeout=180); self.main_cog = main_cog
    async def _confirm_and_act(self, interaction: discord.Interaction, prompt: str, action):
        confirm = ConfirmView(interaction.user)
        await interaction.response.send_message(prompt, view=confirm, ephemeral=True)
        confirm.message = await interaction.original_response()
        await confirm.wait()
        if confirm.value: 
            await action()
            await interaction.edit_original_response(content="✅ Action completed.", view=None)
        else: 
            await interaction.edit_original_response(content="Action cancelled.", view=None)
    @discord.ui.button(label="Wipe Monthly Scores", style=discord.ButtonStyle.danger)
    async def wipe_monthly(self, i: discord.Interaction, b: discord.ui.Button):
        async def action():
            async with self.main_cog.config_lock: 
                # Wipe global scores
                self.main_cog.get_global_data()["scores"] = {}
                self.main_cog.config_is_dirty = True
            await self.main_cog.update_gateway_message(i.guild.id)
        await self._confirm_and_act(i, "⚠️ **Are you sure?** This will reset the **GLOBAL** monthly leaderboard.", action)
    @discord.ui.button(label="Wipe All-Time Stats", style=discord.ButtonStyle.danger)
    async def wipe_alltime(self, i: discord.Interaction, b: discord.ui.Button):
        async def action():
            async with self.main_cog.config_lock: 
                # Wipe global stats
                self.main_cog.get_global_data()["user_stats"] = {}
                self.main_cog.config_is_dirty = True
        await self._confirm_and_act(i, "🔥🔥 **DANGER** 🔥🔥\nThis will reset **everyone's ranks and permanent scores** across all servers. This cannot be undone.", action)
    @discord.ui.button(label="Purge Question Cache", style=discord.ButtonStyle.secondary)
    async def purge_cache(self, i: discord.Interaction, b: discord.ui.Button):
        async def action():
            async with self.main_cog.config_lock: 
                # Purge global cache
                self.main_cog.get_global_data()["question_cache"] = []
                self.main_cog.config_is_dirty = True
            self.main_cog.cache_refill_loop.restart()
        await self._confirm_and_act(i, "Are you sure you want to delete all cached questions?", action)

class AdminPanelView(discord.ui.View):
    def __init__(self, main_cog: "DailyTrivia"): super().__init__(timeout=180); self.main_cog = main_cog
    @discord.ui.button(label="⚙️ General Settings", style=discord.ButtonStyle.primary, row=0)
    async def settings(self, i: discord.Interaction, b: discord.ui.Button): 
        # Pass guild_settings
        await i.response.send_modal(TriviaSettingsModal(self.main_cog, self.main_cog.get_guild_settings(i.guild.id)))
    @discord.ui.button(label="🏆 Set Reward Role", style=discord.ButtonStyle.primary, row=0)
    async def set_role(self, i: discord.Interaction, b: discord.ui.Button): 
        # Pass guild_settings
        await i.response.send_modal(RoleSettingsModal(self.main_cog, self.main_cog.get_guild_settings(i.guild.id)))
    @discord.ui.button(label="📊 Info & Status", style=discord.ButtonStyle.success, row=0)
    async def info(self, i: discord.Interaction, b: discord.ui.Button): 
        cfg_settings = self.main_cog.get_guild_settings(i.guild.id)
        global_data = self.main_cog.get_global_data()
        
        embed = discord.Embed(title="Trivia Info & Status", color=EMBED_COLOR_TRIVIA, description=FAQ_TEXT)
        channel = self.main_cog.bot.get_channel(cfg_settings.get("channel_id", 0))
        role = i.guild.get_role(cfg_settings.get("winner_role_id", 0))
        
        details = (
            f"**Status**: {'ENABLED ✅' if cfg_settings.get('enabled') else 'DISABLED ❌'} (This Guild)\n"
            f"**Post Channel**: {channel.mention if channel else 'Not Set'} (This Guild)\n"
            f"**Reward Role**: {role.mention if role else 'Not Set'} (This Guild)\n"
            f"**Cached Questions**: `{len(global_data.get('question_cache', []))}` (Global)"
        )
        embed.add_field(name="Configuration", value=details)
        await i.response.send_message(embed=embed, ephemeral=True)
    @discord.ui.button(label="👤 User Management", style=discord.ButtonStyle.secondary, row=1)
    async def user_mgmt(self, i: discord.Interaction, b: discord.ui.Button): await i.response.send_message("Select a user management action (all actions are global):", view=UserManagementView(self.main_cog), ephemeral=True)
    @discord.ui.button(label="💾 Data Management", style=discord.ButtonStyle.secondary, row=1)
    async def data_mgmt(self, i: discord.Interaction, b: discord.ui.Button): await i.response.send_message("Select a data management action (all actions are global):", view=DataManagementView(self.main_cog), ephemeral=True)
    
    @discord.ui.button(label="🕵️ Anti-Cheat", style=discord.ButtonStyle.danger, row=1)
    async def anti_cheat(self, i: discord.Interaction, b: discord.ui.Button):
        main_cog = self.main_cog
        class FlagModal(discord.ui.Modal, title="Flag User for Test"):
            user_id = discord.ui.TextInput(label="User ID to Flag")
            async def on_submit(s, inner_i: discord.Interaction):
                await inner_i.response.defer(ephemeral=True)
                try: uid = int(s.user_id.value)
                except ValueError: return await inner_i.followup.send("Invalid ID.", ephemeral=True)
                async with main_cog.config_lock:
                    if len(ANTI_CHEAT_QUESTIONS) < 2: return await inner_i.followup.send("Not enough anti-cheat questions.", ephemeral=True)
                    questions = random.sample(ANTI_CHEAT_QUESTIONS, 2)
                    # Flag in global data
                    global_data = main_cog.get_global_data()
                    global_data.setdefault("cheater_test_users", {})[str(uid)] = {"main_q": questions[0], "don_q": questions[1]}
                    main_cog.config_is_dirty = True
                await inner_i.followup.send(f"✅ User `{uid}` will be tested.", ephemeral=True)
        await i.response.send_modal(FlagModal())

    @discord.ui.button(label="Force Daily Post", style=discord.ButtonStyle.secondary, row=2)
    async def force_post(self, i: discord.Interaction, b: discord.ui.Button):
        confirm = ConfirmView(i.user)
        await i.response.send_message("This will reset the **GLOBAL** daily question and post now in *this* channel. Are you sure?", view=confirm, ephemeral=True)
        confirm.message = await i.original_response() # Set message reference
        await confirm.wait()
        if confirm.value: 
            await i.edit_original_response(content="Forcing post...", view=None)
            await self.main_cog._trigger_daily_reset_and_post(i.guild)
        else:
            await i.edit_original_response(content="Post cancelled.", view=None)
            
    @discord.ui.button(label="Post Bonus Question", style=discord.ButtonStyle.secondary, row=2)
    async def post_bonus(self, i: discord.Interaction, b: discord.ui.Button):
        await i.response.defer(ephemeral=True)
        q = await self.main_cog._get_cached_question() # No guild_id
        if not q: return await i.followup.send("Cache is empty.", ephemeral=True)
        
        view = BonusGatewayView(self.main_cog, q)
        embed = discord.Embed(title="🎲 Bonus Trivia!", description=f"A wild bonus question appears!\n\n**Category:** {q.get('category', 'Unknown')}", color=0x7289DA)
        embed.add_field(name="🏆 Fastest Correct", value="Be the first to answer!", inline=False)
        msg = await i.channel.send(embed=embed, view=view); view.message = msg
        await i.followup.send("✅ Bonus question posted!", ephemeral=True)

    async def on_timeout(self):
        try:
            for item in self.children: 
                item.disabled = True
        except: 
            pass

# =====================================================================================
# ADMIN COG CLASS
# =====================================================================================

@app_commands.guild_only()
class TriviaAdmin(commands.Cog, name="TriviaAdmin"):
    """Administrative commands for the Daily Trivia cog."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_check(self, interaction: discord.Interaction) -> bool:
        return await is_trivia_admin_check(interaction)

    def get_main_cog(self) -> "typing.Optional[DailyTrivia]":
        return self.bot.get_cog("DailyTrivia")

    @app_commands.command(name="trivia-panel", description="Displays the trivia administration panel.")
    async def panel(self, interaction: discord.Interaction):
        main_cog = self.get_main_cog()
        if not main_cog:
            return await interaction.response.send_message("Trivia main cog is not loaded.", ephemeral=True)
        await interaction.response.send_message("Welcome to the Trivia Admin Panel.", view=AdminPanelView(main_cog), ephemeral=True)

    @app_commands.command(name="trivia-bump", description="Refreshes the trivia message.")
    async def bump(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        main_cog = self.get_main_cog()
        if not main_cog: return await interaction.followup.send("Trivia main cog is not loaded.", ephemeral=True)
        try:
            await main_cog._post_or_bump_messages(interaction.guild)
            await interaction.followup.send("✅ Trivia message refreshed.", ephemeral=True)
        except TriviaPostingError as e:
            await interaction.followup.send(f"❌ **Could not refresh:** {e}", ephemeral=True)
    
    @commands.command(name="triviasync")
    @commands.guild_only()
    @commands.is_owner()
    async def triviasync(self, ctx: commands.Context):
        try:
            synced = await self.bot.tree.sync(guild=ctx.guild)
            await ctx.send(f"Synced {len(synced)} application commands to this guild.")
        except Exception as e:
            await ctx.send(f"Failed to sync commands: {e}")

async def setup(bot: commands.Bot):
    await bot.add_cog(TriviaAdmin(bot))
