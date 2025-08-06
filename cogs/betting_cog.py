import discord
from discord import app_commands
from discord.ext import commands, tasks
import json
import os
from datetime import datetime, timedelta, timezone
import asyncio
import typing
from io import BytesIO
import re

# --- Pillow (PIL) for image generation ---
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    raise ImportError("Pillow is not installed. Please run 'pip install Pillow' to use this cog.")

# --- Configuration & Asset Paths ---
DATA_FILE = "bot_data.json"
ASSETS_DIR = "assets"
PROFILE_BG_PATH = os.path.join(ASSETS_DIR, "profile_background.png")
FONT_PATH = os.path.join(ASSETS_DIR, "font.ttf")

# --- Bot Theming and Configuration ---
CURRENCY_NAME = "Tokens"
CURRENCY_EMOJI = "🪙"
FIRST_MSG_REWARD = 100
SUBSEQUENT_MSG_REWARD = 10
MESSAGE_CAP_PER_DAY = 20
STREAK_BONUS_DAYS = 7
STREAK_BONUS_AMOUNT = 500
LARGE_BET_THRESHOLD = 0.5  # 50% of user's balance
LEADERBOARD_TOP_N = 10

# --- Professional Color Palette ---
COLOR_PRIMARY = discord.Color.from_rgb(88, 101, 242) # Discord Blurple
COLOR_SUCCESS = discord.Color.from_rgb(87, 242, 135) # Green
COLOR_ERROR = discord.Color.from_rgb(237, 66, 69)   # Red
COLOR_WARNING = discord.Color.from_rgb(254, 231, 92) # Yellow
COLOR_INFO = discord.Color.from_rgb(59, 165, 235)    # Blue
COLOR_GOLD = discord.Color.gold()
COLOR_GREY = discord.Color.light_grey()

# --- UI Modals & Views ---

class BetAmountModal(discord.ui.Modal, title="Place Your Bet"):
    """A modal to get the amount a user wants to bet."""
    def __init__(self, bet_id: str, option_index: int, cog_instance):
        super().__init__(timeout=180)
        self.bet_id = bet_id
        self.option_index = option_index
        self.cog = cog_instance

        self.amount_input = discord.ui.TextInput(
            label=f"Amount of {CURRENCY_NAME} to Bet",
            placeholder="e.g., 1000",
            style=discord.TextStyle.short,
            required=True,
        )
        self.add_item(self.amount_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Handles the logic after the user submits their bet amount."""
        await self.cog._execute_bet(interaction, self.bet_id, self.option_index, self.amount_input.value)

class BetOptionButton(discord.ui.Button):
    """A custom button for each betting option."""
    def __init__(self, bet_id: str, option_index: int, option_text: str, cog_instance):
        super().__init__(
            label=option_text[:80], # Max label length is 80 characters
            style=discord.ButtonStyle.secondary,
            custom_id=f"bet_btn:{bet_id}:{option_index}"
        )
        self.bet_id = bet_id
        self.option_index = option_index
        self.cog = cog_instance

    async def callback(self, interaction: discord.Interaction):
        """When a user clicks a bet option, show the amount modal."""
        bet_info = self.cog.data.get("bets", {}).get(self.bet_id)
        if not bet_info or bet_info.get("status") != "open":
            await interaction.response.send_message("Betting on this event has closed.", ephemeral=True)
            # Disable all buttons on the view if the bet is no longer open
            for item in self.view.children:
                item.disabled = True
            await interaction.message.edit(view=self.view)
            return

        modal = BetAmountModal(bet_id=self.bet_id, option_index=self.option_index, cog_instance=self.cog)
        await interaction.response.send_modal(modal)

class PlaceBetView(discord.ui.View):
    """A view that holds the BetOptionButtons for a specific bet."""
    def __init__(self, bet_id: str, bet_info: dict, cog_instance):
        # Timeout=None makes the view persistent across bot restarts
        super().__init__(timeout=None)
        for i, option_text in enumerate(bet_info["options"]):
            self.add_item(BetOptionButton(bet_id, i + 1, option_text, cog_instance))

class ConfirmLargeBetView(discord.ui.View):
    """A view to confirm a large bet after the amount has been entered."""
    def __init__(self, author: discord.User, on_confirm, on_cancel):
        super().__init__(timeout=60)
        self.author = author
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author.id

    async def disable_all(self, interaction: discord.Interaction):
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        self.stop()

    @discord.ui.button(label="Confirm Bet", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.on_confirm(interaction)
        await self.disable_all(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.on_cancel(interaction)
        await self.disable_all(interaction)


class HelpView(discord.ui.View):
    """The view for the interactive help command."""
    def __init__(self, author: discord.User):
        super().__init__(timeout=180)
        self.author = author
        self.add_item(self.HelpSelect())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("This isn't for you! Use `/bet help` to create your own panel.", ephemeral=True)
            return False
        return True

    class HelpSelect(discord.ui.Select):
        def __init__(self):
            options = [
                discord.SelectOption(label="User Commands", description="General commands for all users."),
                discord.SelectOption(label="Betting Commands", description="Commands for placing and viewing bets."),
                discord.SelectOption(label="Shop Commands", description="Commands for the server shop."),
                discord.SelectOption(label="Admin Commands", description="Commands for server managers."),
            ]
            super().__init__(placeholder="Choose a command category...", options=options)

        async def callback(self, interaction: discord.Interaction):
            category = self.values[0]
            embed = discord.Embed(title=f"{category}", color=COLOR_PRIMARY)

            if category == "User Commands":
                embed.description = "Commands for checking balances and stats."
                embed.add_field(name="/bet balance `[user]`", value="Check your own or another user's balance.", inline=False)
                embed.add_field(name="/bet stats", value="View your detailed personal stats and daily message count.", inline=False)
                embed.add_field(name="/bet leaderboard", value="Shows the server's top earners (weekly, monthly, all-time).", inline=False)
                embed.add_field(name="/bet profile `[user]`", value="Displays a graphical profile card for a user.", inline=False)
                embed.add_field(name="/bet globalstats", value="Shows server-wide betting statistics.", inline=False)
            elif category == "Betting Commands":
                embed.description = "Commands for interacting with the betting system."
                embed.add_field(name="/bet list", value="Lists all currently active bets with buttons to place a wager.", inline=False)
                embed.add_field(name="/bet info `<bet_id>`", value="Get detailed info, including odds, for a specific bet.", inline=False)
                embed.add_field(name="/bet mybets", value="See a list of all bets you are currently in.", inline=False)
                embed.add_field(name="/bet history `<bet_id>`", value="View the results of a past, resolved bet.", inline=False)
            elif category == "Shop Commands":
                embed.description = "Commands for buying items."
                embed.add_field(name="/bet shop", value="View all items available for purchase.", inline=False)
                embed.add_field(name="/bet buy `<item>`", value="Purchase an item from the shop.", inline=False)
            elif category == "Admin Commands":
                embed.description = "Commands for managing the bot and betting system. You must have the Bet Admin role or Manage Server permission to use these."
                embed.add_field(name="/bet admin create", value="Creates a new bet.", inline=False)
                embed.add_field(name="/bet admin resolve", value="Resolves a bet and distributes winnings.", inline=False)
                embed.add_field(name="/bet admin cancel", value="Cancels a bet and refunds users.", inline=False)
                embed.add_field(name="/bet admin ...", value="Many more commands for settings, currency, shop, and data management.", inline=False)

            await interaction.response.edit_message(embed=embed)

class LeaderboardView(discord.ui.View):
    """The view for the interactive leaderboard command."""
    def __init__(self, author_id: int, cog_instance):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.cog = cog_instance

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This isn't for you! Use `/bet leaderboard` to create your own.", ephemeral=True)
            return False
        return True

    async def generate_leaderboard_embed(self, timeframe: str, guild: discord.Guild):
        return await self.cog._generate_leaderboard_embed(timeframe, guild)

    @discord.ui.button(label="Weekly", style=discord.ButtonStyle.primary)
    async def weekly_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        embed = await self.generate_leaderboard_embed("weekly", interaction.guild)
        await interaction.edit_original_response(embed=embed)

    @discord.ui.button(label="Monthly", style=discord.ButtonStyle.primary)
    async def monthly_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        embed = await self.generate_leaderboard_embed("monthly", interaction.guild)
        await interaction.edit_original_response(embed=embed)

    @discord.ui.button(label="All-Time", style=discord.ButtonStyle.secondary)
    async def all_time_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        embed = await self.generate_leaderboard_embed("all_time", interaction.guild)
        await interaction.edit_original_response(embed=embed)

class ConfirmPruneView(discord.ui.View):
    """A view to confirm pruning users."""
    def __init__(self, author: discord.User, on_confirm, on_cancel):
        super().__init__(timeout=60)
        self.author = author
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author.id

    @discord.ui.button(label="Confirm Prune", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.on_confirm(interaction)
        for child in self.children: child.disabled = True
        await interaction.message.edit(view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.on_cancel(interaction)
        for child in self.children: child.disabled = True
        await interaction.message.edit(view=self)
        self.stop()

class ShopView(discord.ui.View):
    """The view for the interactive shop."""
    def __init__(self, author: discord.User, cog_instance):
        super().__init__(timeout=180)
        self.author = author
        self.cog = cog_instance
        shop_items = self.cog._get_guild_data(author.guild.id).get("shop_items", {})
        if shop_items:
            self.add_item(self.ShopSelect(shop_items))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("This isn't for you! Use `/bet shop` to open your own.", ephemeral=True)
            return False
        return True

    class ShopSelect(discord.ui.Select):
        def __init__(self, shop_items: dict):
            options = [
                discord.SelectOption(
                    label=f"{name} ({item['price']} {CURRENCY_EMOJI})",
                    description=item.get("description", "No description available.")[:100],
                    value=name
                ) for name, item in shop_items.items()
            ]
            if not options:
                options.append(discord.SelectOption(label="The shop is empty.", value="empty", default=True))
            super().__init__(placeholder="Select an item to purchase...", options=options, disabled=(not shop_items))

        async def callback(self, interaction: discord.Interaction):
            item_name = self.values[0]
            if item_name == "empty":
                return await interaction.response.send_message("There is nothing to buy.", ephemeral=True)
            await interaction.response.defer(ephemeral=True)
            await self.view.cog._buy_item(interaction, item_name)


# --- Main Cog Class ---
class BettingCog(commands.Cog):
    """The complete, feature-rich betting cog for a Discord server."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.file_lock = asyncio.Lock()
        self.data = self._load_data()
        self.check_bet_timers.start()
        # Add persistent views for bets on startup
        self.bot.add_view(PlaceBetView(None, {"options": []}, self))


    def cog_unload(self):
        self.check_bet_timers.cancel()

    # --- Command Groups ---
    bet_group = app_commands.Group(name="bet", description="Commands for the betting and economy system.")
    admin_group = app_commands.Group(name="admin", parent=bet_group, description="Admin-only commands for the betting bot.")

    # --- Custom Permission Check ---
    async def has_admin_role(self, interaction: discord.Interaction) -> bool:
        """Checks if the user has Manage Guild perms OR the configured Bet Admin role."""
        if await self.bot.is_owner(interaction.user):
            return True
        if interaction.user.guild_permissions.manage_guild:
            return True

        guild_data = self._get_guild_data(interaction.guild_id)
        admin_role_id = guild_data.get("bet_admin_role_id")
        if admin_role_id:
            admin_role = interaction.guild.get_role(admin_role_id)
            if admin_role and admin_role in interaction.user.roles:
                return True
        return False

    # --- Data & Helper Functions ---
    def _get_default_user_data(self):
        return {
            "balance": 0, "messages_today": 0, "last_message_date": "1970-01-01",
            "streak": 0, "wins": 0, "losses": 0, "total_won": 0, "total_lost": 0,
            "transactions": []
        }

    def _get_default_guild_data(self):
        return {
            "log_channel_id": None, "bettor_role_id": None,
            "transaction_tax": 0.0, "server_bank": 0,
            "shop_items": {},
            "stats": {"total_bets_made": 0},
            "bet_admin_role_id": None,
            "active_bets_channel_id": None
        }

    def _get_default_bet_data(self):
        return {
            "title": "", "options": [], "status": "open", "creator": 0,
            "participants": {}, "message_references": []
        }

    def _load_data(self):
        if not os.path.exists(DATA_FILE):
            return {"users": {}, "bets": {}, "resolved_bets": {}, "guilds": {}}
        try:
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"users": {}, "bets": {}, "resolved_bets": {}, "guilds": {}}

    async def _save_data(self):
        async with self.file_lock:
            temp_file = f"{DATA_FILE}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.data, f, indent=4)
            os.replace(temp_file, DATA_FILE)

    def _get_user_data(self, user_id: int):
        user_id_str = str(user_id)
        if user_id_str not in self.data["users"]:
            self.data["users"][user_id_str] = self._get_default_user_data()
        # Ensure all default keys are present for existing users
        for key, value in self._get_default_user_data().items():
            self.data["users"][user_id_str].setdefault(key, value)
        return self.data["users"][user_id_str]

    def _get_guild_data(self, guild_id: int):
        guild_id_str = str(guild_id)
        if guild_id_str not in self.data["guilds"]:
            self.data["guilds"][guild_id_str] = self._get_default_guild_data()
        # Ensure all default keys are present for existing guilds
        for key, value in self._get_default_guild_data().items():
            self.data["guilds"][guild_id_str].setdefault(key, value)
        return self.data["guilds"][guild_id_str]

    async def _add_transaction(self, user_id: int, amount: int, reason: str):
        user_data = self._get_user_data(user_id)
        user_data["transactions"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "amount": amount,
            "reason": reason
        })
        # Keep transactions for the last year only
        one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
        user_data["transactions"] = [
            t for t in user_data.get("transactions", [])
            if datetime.fromisoformat(t["timestamp"]) > one_year_ago
        ]

    async def _log_action(self, interaction: discord.Interaction, embed: discord.Embed, diff_info: typing.Optional[dict] = None):
        guild_data = self._get_guild_data(interaction.guild_id)
        log_channel_id = guild_data.get("log_channel_id")
        if log_channel_id:
            try:
                log_channel = await self.bot.fetch_channel(log_channel_id)
                embed.set_footer(text=f"Action by: {interaction.user.name} ({interaction.user.id})")
                embed.timestamp = datetime.now(timezone.utc)
                if diff_info:
                    before_text = diff_info.get("before", "N/A")
                    after_text = diff_info.get("after", "N/A")
                    embed.add_field(name="Change Details", value=f"**Before:** {before_text}\n**After:** {after_text}", inline=False)
                await log_channel.send(embed=embed)
            except (discord.NotFound, discord.Forbidden):
                pass # Fail silently if log channel is invalid

    async def _generate_leaderboard_embed(self, timeframe: str, guild: discord.Guild):
        now = datetime.now(timezone.utc)
        if timeframe == "weekly":
            start_date = now - timedelta(days=7)
            title = f"{guild.name} Weekly Leaderboard"
        elif timeframe == "monthly":
            start_date = now - timedelta(days=30)
            title = f"{guild.name} Monthly Leaderboard"
        else: # all_time
            title = f"{guild.name} All-Time Leaderboard"

        user_gains = {}
        for user_id, user_data in self.data.get("users", {}).items():
            if timeframe == "all_time":
                user_gains[user_id] = user_data.get("balance", 0)
            else:
                total_gain = sum(
                    t["amount"] for t in user_data.get("transactions", [])
                    if datetime.fromisoformat(t["timestamp"]) >= start_date and t["amount"] > 0
                )
                user_gains[user_id] = total_gain

        sorted_users = sorted(user_gains.items(), key=lambda item: item[1], reverse=True)
        embed = discord.Embed(title=title, color=COLOR_GOLD)
        if timeframe != "all_time":
            embed.description = f"Showing net gains since {start_date.strftime('%Y-%m-%d %H:%M UTC')}"

        description_lines = [embed.description] if embed.description else []
        rank_count = 0
        for user_id, gain in sorted_users:
            if rank_count >= LEADERBOARD_TOP_N: break
            if gain == 0 and timeframe != "all_time": continue
            try:
                user = await self.bot.fetch_user(int(user_id))
                description_lines.append(f"**{rank_count+1}. {user.display_name}** — {gain:,} {CURRENCY_EMOJI}")
                rank_count += 1
            except discord.NotFound:
                pass

        if rank_count == 0:
            description_lines.append("\nThe leaderboard is empty for this period!")

        embed.description = "\n".join(description_lines)
        return embed

    async def _buy_item(self, interaction: discord.Interaction, item_name: str):
        guild_data = self._get_guild_data(interaction.guild.id)
        shop_items = guild_data.get("shop_items", {})
        item = shop_items.get(item_name)

        if not item:
            return await interaction.followup.send("This item does not exist.", ephemeral=True)

        user_data = self._get_user_data(interaction.user.id)
        if user_data["balance"] < item["price"]:
            return await interaction.followup.send(f"You cannot afford this item. You need {item['price'] - user_data['balance']:,} more {CURRENCY_EMOJI}.", ephemeral=True)

        if item.get("role_id"):
            role = interaction.guild.get_role(item["role_id"])
            if not role:
                return await interaction.followup.send("The role associated with this item no longer exists. Please contact an admin.", ephemeral=True)
            if role in interaction.user.roles:
                return await interaction.followup.send("You already have this role!", ephemeral=True)

            try:
                await interaction.user.add_roles(role, reason=f"Purchased '{item_name}' from shop.")
            except discord.Forbidden:
                return await interaction.followup.send("I don't have permission to assign this role. Please contact an admin.", ephemeral=True)

        user_data["balance"] -= item["price"]
        await self._add_transaction(interaction.user.id, -item["price"], f"Bought item: {item_name}")
        await self._save_data()

        await interaction.followup.send(f"You have successfully purchased **{item_name}** for {item['price']:,} {CURRENCY_EMOJI}!", ephemeral=True)

    # --- Background Task & Listeners ---
    @tasks.loop(seconds=60)
    async def check_bet_timers(self):
        now = datetime.now(timezone.utc)
        bets_to_lock = []
        for bet_id, bet_info in self.data.get("bets", {}).items():
            if bet_info.get("status") == "open" and "auto_close_timestamp" in bet_info:
                try:
                    close_time = datetime.fromisoformat(bet_info["auto_close_timestamp"])
                    if now >= close_time:
                        bets_to_lock.append(bet_id)
                except (ValueError, TypeError):
                    continue

        for bet_id in bets_to_lock:
            bet_info = self.data["bets"][bet_id]
            bet_info["status"] = "locked"
            await self._save_data()
            try:
                for msg_ref in bet_info.get("message_references", []):
                    channel = await self.bot.fetch_channel(msg_ref["channel_id"])
                    msg = await channel.fetch_message(msg_ref["message_id"])
                    original_embed = msg.embeds[0]
                    new_embed = original_embed.copy()
                    new_embed.color = COLOR_WARNING
                    # Remove old status field if it exists
                    fields_to_keep = [f for f in new_embed.fields if f.name.lower() != "status"]
                    new_embed.clear_fields()
                    for f in fields_to_keep:
                        new_embed.add_field(name=f.name, value=f.value, inline=f.inline)
                    new_embed.add_field(name="Status", value="Betting Closed", inline=False)
                    
                    # Disable buttons on the view
                    view = PlaceBetView(bet_id, bet_info, self)
                    for item in view.children:
                        item.disabled = True

                    await msg.edit(embed=new_embed, view=view)
            except (discord.NotFound, discord.Forbidden):
                print(f"Could not announce locking for bet {bet_id}.")

    @check_bet_timers.before_loop
    async def before_check_bet_timers(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_ready(self):
        print(f"Betting Cog is ready and running as {self.bot.user.name}.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild: return
        user_data = self._get_user_data(message.author.id)
        today = datetime.now(timezone.utc).date()
        try:
            last_message_date = datetime.fromisoformat(user_data["last_message_date"]).date()
        except (ValueError, TypeError):
            last_message_date = datetime(1970, 1, 1).date()

        if last_message_date != today:
            user_data["messages_today"] = 0
            if today == last_message_date + timedelta(days=1):
                user_data["streak"] += 1
            else:
                user_data["streak"] = 1
            user_data["last_message_date"] = today.isoformat()

        if user_data["messages_today"] < MESSAGE_CAP_PER_DAY:
            user_data["messages_today"] += 1
            reward = 0
            reason = ""
            if user_data["messages_today"] == 1:
                reward = FIRST_MSG_REWARD
                reason = "First message of the day"
                user_data["balance"] += reward
                if user_data["streak"] > 0 and user_data["streak"] % STREAK_BONUS_DAYS == 0:
                    user_data["balance"] += STREAK_BONUS_AMOUNT
                    await self._add_transaction(message.author.id, STREAK_BONUS_AMOUNT, f"{user_data['streak']}-day streak bonus")
                    try:
                        await message.author.send(f"Congratulations! You've hit a {user_data['streak']}-day messaging streak and earned a bonus of **{STREAK_BONUS_AMOUNT:,}** {CURRENCY_EMOJI}!")
                    except discord.Forbidden: pass
            else:
                reward = SUBSEQUENT_MSG_REWARD
                reason = f"Message #{user_data['messages_today']}"
                user_data["balance"] += reward

            await self._add_transaction(message.author.id, reward, reason)
            await self._save_data()

    # --- Autocomplete Functions ---
    async def open_bet_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        choices = []
        open_bets = {k: v for k, v in self.data.get("bets", {}).items() if v.get("status") in ["open", "locked"]}
        for bet_id, bet_info in open_bets.items():
            if len(choices) >= 25: break
            if current.lower() in bet_info.get('title', '').lower() or current in bet_id:
                choices.append(app_commands.Choice(name=f"{bet_info['title']} (ID: {bet_id})", value=bet_id))
        return choices

    async def resolved_bet_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        choices = []
        resolved_bets = self.data.get("resolved_bets", {})
        for bet_id, bet_info in resolved_bets.items():
            if len(choices) >= 25: break
            if current.lower() in bet_info.get('title', '').lower() or current in bet_id:
                choices.append(app_commands.Choice(name=f"{bet_info['title']} (ID: {bet_id})", value=bet_id))
        return choices

    async def shop_item_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        guild_data = self._get_guild_data(interaction.guild.id)
        shop_items = guild_data.get("shop_items", {})
        return [
            app_commands.Choice(name=name, value=name)
            for name in shop_items if current.lower() in name.lower()
        ][:25]

    # --- USER COMMANDS ---
    @bet_group.command(name="help", description="Shows an interactive help panel for the bot.")
    async def help(self, interaction: discord.Interaction):
        embed = discord.Embed(title="Bot Help Panel", description="Please select a category from the dropdown menu below.", color=COLOR_INFO)
        await interaction.response.send_message(embed=embed, view=HelpView(interaction.user), ephemeral=True)

    @bet_group.command(name="balance", description="Check your own or another user's balance.")
    @app_commands.describe(user="The user whose balance you want to check (optional).")
    async def balance(self, interaction: discord.Interaction, user: typing.Optional[discord.Member] = None):
        target_user = user or interaction.user
        user_data = self._get_user_data(target_user.id)
        embed = discord.Embed(title=f"{target_user.display_name}'s Wallet", color=COLOR_INFO)
        embed.set_thumbnail(url=target_user.display_avatar.url)
        embed.add_field(name="Balance", value=f"{user_data.get('balance', 0):,} {CURRENCY_EMOJI} {CURRENCY_NAME}")
        await interaction.response.send_message(embed=embed)

    @bet_group.command(name="stats", description="View your detailed personal stats and daily progress.")
    async def mybetstats(self, interaction: discord.Interaction):
        user_data = self._get_user_data(interaction.user.id)
        embed = discord.Embed(title=f"{interaction.user.display_name}'s Statistics", color=COLOR_INFO)
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.add_field(name="Balance", value=f"{user_data.get('balance', 0):,} {CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Daily Messages", value=f"{user_data.get('messages_today', 0)}/{MESSAGE_CAP_PER_DAY}", inline=True)
        embed.add_field(name="Message Streak", value=f"{user_data.get('streak', 0)} Days", inline=True)
        total_bets = user_data.get('wins', 0) + user_data.get('losses', 0)
        win_rate = (user_data.get('wins', 0) / total_bets * 100) if total_bets > 0 else 0
        embed.add_field(name="Win Rate", value=f"{win_rate:.2f}% ({user_data.get('wins', 0)}W / {user_data.get('losses', 0)}L)", inline=False)
        embed.add_field(name="Total Won", value=f"{user_data.get('total_won', 0):,} {CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Total Lost", value=f"{user_data.get('total_lost', 0):,} {CURRENCY_EMOJI}", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @bet_group.command(name="profile", description="Displays a user's betting profile card.")
    @app_commands.describe(user="The user whose profile you want to view (optional).")
    async def profile(self, interaction: discord.Interaction, user: typing.Optional[discord.Member] = None):
        if not os.path.exists(PROFILE_BG_PATH) or not os.path.exists(FONT_PATH):
            return await interaction.response.send_message("Profile card assets are not configured correctly by the bot owner.", ephemeral=True)
        await interaction.response.defer()
        target_user = user or interaction.user
        user_data = self._get_user_data(target_user.id)
        bg = Image.open(PROFILE_BG_PATH).convert("RGBA")
        draw = ImageDraw.Draw(bg)
        try:
            font_big = ImageFont.truetype(FONT_PATH, 60)
            font_medium = ImageFont.truetype(FONT_PATH, 40)
            font_small = ImageFont.truetype(FONT_PATH, 30)
        except IOError:
            return await interaction.followup.send("The font file could not be loaded. Please check the assets folder.", ephemeral=True)

        avatar_bytes = await target_user.display_avatar.read()
        avatar = Image.open(BytesIO(avatar_bytes)).convert("RGBA").resize((200, 200))
        mask = Image.new("L", (200, 200), 0)
        draw_mask = ImageDraw.Draw(mask)
        draw_mask.ellipse((0, 0, 200, 200), fill=255)
        bg.paste(avatar, (50, 50), mask)
        draw.text((280, 60), target_user.name, font=font_big, fill="#FFFFFF")
        draw.text((280, 130), f"User ID: {target_user.id}", font=font_small, fill="#CCCCCC")
        draw.text((70, 300), "Balance", font=font_medium, fill="#FFFFFF")
        draw.text((70, 350), f"{user_data.get('balance', 0):,}{CURRENCY_EMOJI}", font=font_medium, fill="#40C040")
        total_bets = user_data.get('wins', 0) + user_data.get('losses', 0)
        win_rate = (user_data.get('wins', 0) / total_bets * 100) if total_bets > 0 else 0
        draw.text((400, 300), "Win Rate", font=font_medium, fill="#FFFFFF")
        draw.text((400, 350), f"{win_rate:.1f}%", font=font_medium, fill="#40C040")
        draw.text((650, 300), "Record", font=font_medium, fill="#FFFFFF")
        draw.text((650, 350), f"{user_data.get('wins', 0)}W / {user_data.get('losses', 0)}L", font=font_medium, fill="#40C040")
        buffer = BytesIO()
        bg.save(buffer, "PNG")
        buffer.seek(0)
        await interaction.followup.send(file=discord.File(buffer, "profile.png"))

    @bet_group.command(name="leaderboard", description="Shows the server's top earners.")
    async def leaderboard(self, interaction: discord.Interaction):
        embed = await self._generate_leaderboard_embed("all_time", interaction.guild)
        view = LeaderboardView(interaction.user.id, self)
        await interaction.response.send_message(embed=embed, view=view)

    @bet_group.command(name="globalstats", description="Shows server-wide betting statistics.")
    async def globalstats(self, interaction: discord.Interaction):
        guild_data = self._get_guild_data(interaction.guild_id)
        guild_stats = guild_data.get("stats", self._get_default_guild_data()["stats"])
        total_currency = sum(u.get('balance', 0) for u in self.data.get("users", {}).values())
        embed = discord.Embed(title=f"{interaction.guild.name} - Global Statistics", color=COLOR_INFO)
        embed.add_field(name="Total Currency in Circulation", value=f"{total_currency:,} {CURRENCY_EMOJI}", inline=False)
        embed.add_field(name=f"Currency in Server Bank (Taxes)", value=f"{guild_data['server_bank']:,} {CURRENCY_EMOJI}", inline=False)
        embed.add_field(name="Total Bets Ever Made", value=f"{guild_stats['total_bets_made']:,}", inline=False)
        await interaction.response.send_message(embed=embed)

    @bet_group.command(name="shop", description="View all items available for purchase.")
    async def shop(self, interaction: discord.Interaction):
        guild_data = self._get_guild_data(interaction.guild.id)
        shop_items = guild_data.get("shop_items", {})
        embed = discord.Embed(title="Server Shop", description="Select an item from the menu below to purchase it.", color=COLOR_PRIMARY)
        if not shop_items:
            embed.description = "The shop is currently empty. Check back later!"
        await interaction.response.send_message(embed=embed, view=ShopView(interaction.user, self), ephemeral=True)

    @bet_group.command(name="buy", description="Purchase an item from the shop.")
    @app_commands.describe(item="The name of the item you want to purchase.")
    @app_commands.autocomplete(item=shop_item_autocomplete)
    async def buy(self, interaction: discord.Interaction, item: str):
        await interaction.response.defer(ephemeral=True)
        await self._buy_item(interaction, item)

    # --- BETTING COMMANDS ---
    @bet_group.command(name="list", description="Lists all currently active bets.")
    async def list_bets(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        active_bets = {k: v for k, v in self.data.get("bets", {}).items() if v["status"] in ["open", "locked"]}
        if not active_bets:
            return await interaction.followup.send("There are no active bets right now.", ephemeral=True)
        
        await interaction.followup.send("Here are the currently active bets:", ephemeral=True)
        
        for bet_id, bet_info in active_bets.items():
            total_pot = sum(p['amount'] for p in bet_info['participants'].values())
            embed = discord.Embed(title=bet_info['title'], color=COLOR_PRIMARY)
            embed.add_field(name="Total Pot", value=f"{total_pot:,} {CURRENCY_EMOJI}", inline=False)
            
            if bet_info['status'] == 'locked':
                embed.add_field(name="Status", value="Betting Closed", inline=False)
                embed.color = COLOR_WARNING

            embed.set_footer(text=f"Bet ID: {bet_id}")
            
            view = PlaceBetView(bet_id, bet_info, self)
            if bet_info['status'] == 'locked':
                for item in view.children:
                    item.disabled = True

            # Send a new message for each bet
            await interaction.channel.send(embed=embed, view=view)

    async def _execute_bet(self, interaction: discord.Interaction, bet_id: str, option: int, amount_str: str):
        """The core logic for placing a bet, called from the BetAmountModal."""
        try:
            amount = int(re.sub(r'[,\s]', '', amount_str))
        except ValueError:
            return await interaction.response.send_message("Invalid amount. Please enter a whole number.", ephemeral=True)
        
        if amount <= 0:
            return await interaction.response.send_message("You must bet a positive amount.", ephemeral=True)

        bet_info = self.data.get("bets", {}).get(bet_id)
        if not bet_info or bet_info["status"] != "open":
            return await interaction.response.send_message("That bet is no longer open for wagers.", ephemeral=True)
        
        user_id_str = str(interaction.user.id)
        user_data = self._get_user_data(interaction.user.id)

        if user_id_str in bet_info["participants"]:
            return await interaction.response.send_message("You have already placed a bet on this event.", ephemeral=True)

        if user_data["balance"] < amount:
            return await interaction.response.send_message(f"You don't have enough {CURRENCY_NAME}. Your balance is {user_data['balance']:,} {CURRENCY_EMOJI}.", ephemeral=True)

        async def do_bet(confirm_interaction: discord.Interaction, bet_amount: int):
            # Re-fetch data to ensure it's current
            current_user_data = self._get_user_data(interaction.user.id)
            current_bet_info = self.data.get("bets", {}).get(bet_id)

            current_user_data["balance"] -= bet_amount
            # This is a temporary holding state; it gets corrected on win/loss
            current_user_data["total_lost"] += bet_amount 
            current_bet_info["participants"][user_id_str] = {"option": option, "amount": bet_amount, "name": interaction.user.name}
            self._get_guild_data(interaction.guild_id)["stats"]["total_bets_made"] += 1
            await self._add_transaction(interaction.user.id, -bet_amount, f"Placed Bet ID: {bet_id}")
            await self._save_data()
            
            embed = discord.Embed(
                title="Bet Placed",
                description=f"You have bet **{bet_amount:,} {CURRENCY_EMOJI}** on **'{current_bet_info['options'][option-1]}'** for the bet '{current_bet_info['title']}'.",
                color=COLOR_SUCCESS
            )
            embed.set_footer(text=f"Bet ID: {bet_id}")
            await confirm_interaction.response.send_message(embed=embed, ephemeral=True)

        # Large bet confirmation flow
        if amount >= user_data["balance"] * LARGE_BET_THRESHOLD and user_data["balance"] > 0:
            async def on_confirm(confirm_interaction: discord.Interaction):
                await do_bet(confirm_interaction, amount)

            async def on_cancel(cancel_interaction: discord.Interaction):
                await cancel_interaction.response.send_message("Bet cancelled.", ephemeral=True)
            
            view = ConfirmLargeBetView(interaction.user, on_confirm=on_confirm, on_cancel=on_cancel)
            embed = discord.Embed(
                title="Large Bet Confirmation",
                description=f"This bet is for **{amount:,} {CURRENCY_EMOJI}**, which is a large portion of your balance. Are you sure?",
                color=COLOR_WARNING
            )
            return await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            # If not a large bet, place it directly
            await do_bet(interaction, amount)

    @bet_group.command(name="info", description="Get detailed info, including odds, for a specific bet.")
    @app_commands.describe(bet_id="The ID of the bet to view.")
    @app_commands.autocomplete(bet_id=open_bet_autocomplete)
    @app_commands.rename(bet_id='bet_id')
    async def betinfo(self, interaction: discord.Interaction, bet_id: str):
        bet_info = self.data.get("bets", {}).get(bet_id)
        if not bet_info:
            return await interaction.response.send_message("That bet ID is invalid.", ephemeral=True)
        total_pot = sum(p['amount'] for p in bet_info['participants'].values())
        embed = discord.Embed(title=bet_info['title'], description=f"**Status:** {bet_info['status'].capitalize()}\n**Total Pot: {total_pot:,} {CURRENCY_EMOJI}**", color=COLOR_GOLD)
        option_pots = {i: 0 for i in range(len(bet_info['options']))}
        for participant in bet_info['participants'].values():
            option_index = participant['option'] - 1
            option_pots[option_index] += participant['amount']
        for i, option_text in enumerate(bet_info['options']):
            pot_on_option = option_pots[i]
            percentage = (pot_on_option / total_pot * 100) if total_pot > 0 else 0
            embed.add_field(name=f"{i+1}. {option_text}", value=f"**Pot:** {pot_on_option:,} {CURRENCY_EMOJI} ({percentage:.2f}%)", inline=False)
        embed.set_footer(text=f"Bet ID: {bet_id}")
        await interaction.response.send_message(embed=embed)

    @bet_group.command(name="mybets", description="See a list of all bets you are currently in.")
    async def mybets(self, interaction: discord.Interaction):
        user_id_str = str(interaction.user.id)
        user_bets = []
        for bet_id, bet_info in self.data.get("bets", {}).items():
            if bet_info["status"] in ["open", "locked"] and user_id_str in bet_info["participants"]:
                user_wager = bet_info["participants"][user_id_str]
                user_bets.append({"id": bet_id, "title": bet_info["title"], "option": bet_info["options"][user_wager["option"] - 1], "amount": user_wager["amount"]})
        if not user_bets:
            return await interaction.response.send_message("You have not placed any active bets.", ephemeral=True)
        embed = discord.Embed(title=f"{interaction.user.display_name}'s Active Bets", color=COLOR_INFO)
        for bet in user_bets:
            embed.add_field(name=f"{bet['title']}", value=f"You bet **{bet['amount']:,} {CURRENCY_EMOJI}** on **'{bet['option']}'**\n*Bet ID: {bet['id']}*", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @bet_group.command(name="history", description="View the results of a past, resolved bet.")
    @app_commands.describe(bet_id="The ID of the resolved bet to view.")
    @app_commands.autocomplete(bet_id=resolved_bet_autocomplete)
    @app_commands.rename(bet_id='bet_id')
    async def bethistory(self, interaction: discord.Interaction, bet_id: str):
        bet_info = self.data.get("resolved_bets", {}).get(bet_id)
        if not bet_info:
            return await interaction.response.send_message("That resolved bet ID could not be found.", ephemeral=True)
        winning_option_text = bet_info['options'][bet_info['winning_option'] - 1]
        embed = discord.Embed(title=bet_info['title'], description=f"This bet was resolved. The winning option was:\n**{winning_option_text}**", color=COLOR_GREY)
        winners_text = bet_info.get("winners_text", "No winners were recorded for this bet.")
        embed.add_field(name="Winners", value=winners_text, inline=False)
        embed.set_footer(text=f"Bet ID: {bet_id}")
        await interaction.response.send_message(embed=embed)

    # --- ADMIN COMMANDS ---
    @admin_group.command(name="set-admin-role", description="Sets the role that can use admin commands.")
    @app_commands.describe(role="The role to designate as Bet Admins.")
    async def admin_set_admin_role(self, interaction: discord.Interaction, role: discord.Role):
        if not await self.has_admin_role(interaction):
            return await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
        guild_data = self._get_guild_data(interaction.guild_id)
        guild_data["bet_admin_role_id"] = role.id
        await self._save_data()
        embed = discord.Embed(title="Admin Role Set", description=f"Users with the {role.mention} role can now use admin commands.", color=COLOR_SUCCESS)
        await interaction.response.send_message(embed=embed)

    @admin_group.command(name="set-bets-channel", description="Sets the channel where all new bets are announced.")
    @app_commands.describe(channel="The channel to use for active bet announcements.")
    async def admin_set_bets_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not await self.has_admin_role(interaction):
            return await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
        guild_data = self._get_guild_data(interaction.guild_id)
        guild_data["active_bets_channel_id"] = channel.id
        await self._save_data()
        embed = discord.Embed(title="Active Bets Channel Set", description=f"All new bets will now be announced in {channel.mention}.", color=COLOR_SUCCESS)
        await interaction.response.send_message(embed=embed)

    @admin_group.command(name="create", description="Creates a new bet.")
    @app_commands.describe(title="The title of the bet.", options="A comma-separated list of options.", ping_role="Whether to ping the bettor role (if set).", duration="Auto-lock duration (e.g., 1d, 12h, 30m).")
    async def admin_create_bet(self, interaction: discord.Interaction, title: str, options: str, ping_role: bool = False, duration: typing.Optional[str] = None):
        if not await self.has_admin_role(interaction):
            return await interaction.response.send_message("You don't have permission for this.", ephemeral=True)
        option_list = [opt.strip() for opt in options.split(',')]
        if len(option_list) < 2:
            return await interaction.response.send_message("You must provide at least two options, separated by commas.", ephemeral=True)

        bet_id = str(max([int(k) for k in self.data.get("bets", {}).keys()] + [0]) + 1)
        bet_info = self._get_default_bet_data()
        bet_info.update({"title": title, "options": option_list, "creator": interaction.user.id, "channel_id": interaction.channel_id})

        auto_close_time = None
        if duration:
            try:
                num = int(duration[:-1]); unit = duration[-1].lower()
                if unit == 'd': delta = timedelta(days=num)
                elif unit == 'h': delta = timedelta(hours=num)
                elif unit == 'm': delta = timedelta(minutes=num)
                else: raise ValueError
                auto_close_time = datetime.now(timezone.utc) + delta
            except (ValueError, IndexError):
                return await interaction.response.send_message("Invalid duration format. Use d, h, or m (e.g., '1d', '12h', '30m').", ephemeral=True)
            bet_info["auto_close_timestamp"] = auto_close_time.isoformat()

        embed = discord.Embed(title="New Bet Created", description=f"**{title}**", color=COLOR_PRIMARY)
        if auto_close_time:
            embed.add_field(name="Betting Ends", value=f"<t:{int(auto_close_time.timestamp())}:R>", inline=False)
        embed.set_footer(text=f"Bet ID: {bet_id}")
        
        view = PlaceBetView(bet_id, bet_info, self)

        content = None
        allowed_mentions = discord.AllowedMentions.none()
        if ping_role:
            guild_data = self._get_guild_data(interaction.guild_id)
            bettor_role_id = guild_data.get("bettor_role_id")
            if bettor_role_id:
                content = f"<@&{bettor_role_id}>"
                allowed_mentions = discord.AllowedMentions(roles=True)

        await interaction.response.send_message(content=content, embed=embed, view=view, allowed_mentions=allowed_mentions)
        original_message = await interaction.original_response()
        bet_info["message_references"].append({"channel_id": original_message.channel.id, "message_id": original_message.id})

        guild_data = self._get_guild_data(interaction.guild_id)
        active_bets_channel_id = guild_data.get("active_bets_channel_id")
        if active_bets_channel_id and active_bets_channel_id != interaction.channel_id:
            try:
                bets_channel = await self.bot.fetch_channel(active_bets_channel_id)
                duplicate_message = await bets_channel.send(embed=embed, view=view, allowed_mentions=allowed_mentions)
                bet_info["message_references"].append({"channel_id": duplicate_message.channel.id, "message_id": duplicate_message.id})
            except (discord.NotFound, discord.Forbidden):
                await interaction.followup.send("Warning: Could not send bet to the active bets channel.", ephemeral=True)

        self.data["bets"][bet_id] = bet_info
        await self._save_data()
        await self._log_action(interaction, embed)

    @admin_group.command(name="resolve", description="Resolves a bet and distributes winnings.")
    @app_commands.describe(bet_id="The ID of the bet to resolve.", winning_option="The number of the winning option.")
    @app_commands.autocomplete(bet_id=open_bet_autocomplete)
    @app_commands.rename(bet_id='bet_id')
    async def admin_resolve_bet(self, interaction: discord.Interaction, bet_id: str, winning_option: int):
        if not await self.has_admin_role(interaction):
            return await interaction.response.send_message("You don't have permission for this.", ephemeral=True)

        bet_info = self.data.get("bets", {}).get(bet_id)
        if not bet_info:
            return await interaction.response.send_message("That bet ID is invalid.", ephemeral=True)
        if not (1 <= winning_option <= len(bet_info["options"])):
            return await interaction.response.send_message("Invalid winning option number.", ephemeral=True)

        await interaction.response.defer()

        guild_data = self._get_guild_data(interaction.guild_id)
        tax_rate = guild_data.get("transaction_tax", 0.0)
        participants = bet_info["participants"]
        total_pot = sum(p['amount'] for p in participants.values())
        winners = {uid: p for uid, p in participants.items() if p['option'] == winning_option}
        losers = {uid: p for uid, p in participants.items() if p['option'] != winning_option}
        winning_pot = sum(w['amount'] for w in winners.values())

        total_participants = len(participants)
        correct_participants = len(winners)
        correct_percentage = (correct_participants / total_participants * 100) if total_participants > 0 else 0

        embed = discord.Embed(title=f"Bet Resolved: {bet_info['title']}", description=f"The winning option was: **{bet_info['options'][winning_option - 1]}**", color=COLOR_SUCCESS)
        embed.set_footer(text=f"Bet ID: {bet_id}")
        
        winnings_text = ""
        if not winners:
            winnings_text = "No one guessed the correct outcome. The house wins!"
            guild_data["server_bank"] += total_pot
        else:
            for user_id, winner_data in winners.items():
                user_obj = self._get_user_data(user_id)
                bet_amount = winner_data['amount']
                payout = int(total_pot * (bet_amount / winning_pot)) if winning_pot > 0 else 0
                tax_amount = int((payout - bet_amount) * tax_rate)
                final_payout = payout - tax_amount
                net_gain = final_payout - bet_amount

                user_obj['balance'] += final_payout
                user_obj['wins'] += 1
                user_obj['total_lost'] -= bet_amount # Correct the temporary loss
                user_obj['total_won'] += net_gain
                guild_data["server_bank"] += tax_amount
                await self._add_transaction(int(user_id), final_payout, f"Won Bet ID: {bet_id}")
                winnings_text += f"• **{winner_data['name']}** won **{final_payout:,}** {CURRENCY_EMOJI} (bet {bet_amount:,})\n"
        
        for user_id in losers.keys():
            self._get_user_data(user_id)['losses'] += 1
            
        embed.add_field(name="Winners", value=winnings_text or "None", inline=False)
        embed.add_field(name="Statistics", value=f"{correct_percentage:.1f}% of participants guessed correctly.", inline=False)
        bet_info["winners_text"] = winnings_text
        
        await interaction.followup.send(f"Resolving bet `{bet_id}`...", ephemeral=True)
        for msg_ref in bet_info.get("message_references", []):
            try:
                channel = await self.bot.fetch_channel(msg_ref["channel_id"])
                message = await channel.fetch_message(msg_ref["message_id"])
                await message.edit(content=None, embed=embed, view=None)
            except (discord.NotFound, discord.Forbidden): continue
        
        bet_info["status"] = "closed"
        bet_info["winning_option"] = winning_option
        self.data["resolved_bets"][bet_id] = bet_info
        del self.data["bets"][bet_id]
        await self._save_data()
        await self._log_action(interaction, embed)

    @admin_group.command(name="cancel", description="Cancels a bet and refunds participants.")
    @app_commands.describe(bet_id="The ID of the bet to cancel.")
    @app_commands.autocomplete(bet_id=open_bet_autocomplete)
    @app_commands.rename(bet_id='bet_id')
    async def admin_cancel_bet(self, interaction: discord.Interaction, bet_id: str):
        if not await self.has_admin_role(interaction):
            return await interaction.response.send_message("You don't have permission for this.", ephemeral=True)
        bet_info = self.data.get("bets", {}).get(bet_id)
        if not bet_info: return await interaction.response.send_message("Invalid bet ID.", ephemeral=True)

        total_refunded = 0
        for user_id, participant_data in bet_info["participants"].items():
            user_obj = self._get_user_data(user_id)
            refund_amount = participant_data['amount']
            user_obj['balance'] += refund_amount
            user_obj['total_lost'] -= refund_amount # Correct the temporary loss
            await self._add_transaction(int(user_id), refund_amount, f"Refund for Bet ID: {bet_id}")
            total_refunded += refund_amount
        
        embed = discord.Embed(title="Bet Canceled", description=f"The bet '{bet_info['title']}' has been canceled by an admin. All stakes have been refunded.", color=COLOR_ERROR)
        embed.set_footer(text=f"Bet ID: {bet_id}")

        for msg_ref in bet_info.get("message_references", []):
            try:
                channel = await self.bot.fetch_channel(msg_ref["channel_id"])
                message = await channel.fetch_message(msg_ref["message_id"])
                await message.edit(content=None, embed=embed, view=None)
            except (discord.NotFound, discord.Forbidden): continue
        
        del self.data["bets"][bet_id]
        await self._save_data()
        await interaction.response.send_message(f"Bet {bet_id} cancelled and {len(bet_info['participants'])} participants refunded.", ephemeral=True)
        await self._log_action(interaction, embed)

    @admin_group.command(name="prune-users", description="Removes users from the database who are no longer in the server.")
    async def admin_prune_users(self, interaction: discord.Interaction):
        if not await self.has_admin_role(interaction):
            return await interaction.response.send_message("You don't have permission for this.", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        server_member_ids = {str(member.id) for member in interaction.guild.members}
        bot_user_ids = set(self.data["users"].keys())
        users_to_prune = bot_user_ids - server_member_ids
        
        if not users_to_prune:
            return await interaction.followup.send("No users to prune. The database is up to date with server members.", ephemeral=True)

        async def do_prune(confirm_interaction: discord.Interaction):
            pruned_count = 0
            for user_id in users_to_prune:
                del self.data["users"][user_id]
                pruned_count += 1
            await self._save_data()
            await confirm_interaction.response.send_message(f"Successfully pruned {pruned_count} user(s) from the database.", ephemeral=True)

        async def cancel_prune(cancel_interaction: discord.Interaction):
            await cancel_interaction.response.send_message("Prune operation cancelled.", ephemeral=True)

        view = ConfirmPruneView(interaction.user, on_confirm=do_prune, on_cancel=cancel_prune)
        embed = discord.Embed(title="Confirm Prune Operation", description=f"This will permanently remove **{len(users_to_prune)} user(s)** from the bot's database because they are no longer in this server.\n\nThis action cannot be undone.", color=COLOR_ERROR)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    """Sets up the cog and ensures necessary assets exist."""
    if not os.path.exists(ASSETS_DIR):
        os.makedirs(ASSETS_DIR)
        print(f"Created '{ASSETS_DIR}' directory. Please add 'profile_background.png' and 'font.ttf' to it.")
    await bot.add_cog(BettingCog(bot))

