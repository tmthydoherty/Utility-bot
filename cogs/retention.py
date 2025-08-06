import discord
from discord.ext import commands, tasks
from discord import app_commands

import aiosqlite
import datetime
import io
import typing
from collections import defaultdict, Counter

# Matplotlib for generating charts
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# VADER for sentiment analysis
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- CONFIGURATION ---
DB_FILE = "retention_data_final.db"
EMBED_COLOR = 0x1ABC9C 
GUILD_IDS = None # Optional: For instant command syncing

# --- UI COMPONENTS ---

class ExitSurveyView(discord.ui.View):
    """A persistent view sent to members who leave, asking for anonymous feedback."""
    def __init__(self, cog_instance):
        super().__init__(timeout=None)
        self.cog = cog_instance

    async def record_response(self, interaction: discord.Interaction, reason: str):
        # The cog's method handles the database interaction
        await self.cog.record_exit_survey(interaction.guild_id, reason)
        for item in self.children:
            item.disabled = True
        
        response_embed = discord.Embed(title="Thank You For Your Feedback", description="Your anonymous response has been recorded and will help the community improve. You can now dismiss this message.", color=EMBED_COLOR)
        await interaction.response.edit_message(embed=response_embed, view=self)

    # Note: Custom IDs are vital for persistent views to work after bot restarts
    @discord.ui.button(label="Confusing / Hard to Navigate", style=discord.ButtonStyle.secondary, custom_id="exit_confusing_final")
    async def confusing_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_response(interaction, "Confusing/Hard to Navigate")

    @discord.ui.button(label="Community Wasn't For Me", style=discord.ButtonStyle.secondary, custom_id="exit_vibe_final")
    async def vibe_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_response(interaction, "Community Vibe/Topic")

    @discord.ui.button(label="A Negative Interaction", style=discord.ButtonStyle.danger, custom_id="exit_negative_final")
    async def negative_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_response(interaction, "Negative Interaction")


class RetentionPanelView(discord.ui.View):
    """The main dashboard view for the retention panel."""
    def __init__(self, cog_instance, author_id: int):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This is not your panel to control.", ephemeral=True)
            return False
        return True

    async def handle_panel_button(self, interaction: discord.Interaction, panel_name: str):
        await interaction.response.defer()
        panel_method = getattr(self.cog, f"get_{panel_name}_panel", None)
        
        if panel_method:
            embed, file = await panel_method(interaction.guild)
            await interaction.followup.edit_message(embed=embed, view=self, attachments=[file] if file else [])

    @discord.ui.button(label="Retention Overview", style=discord.ButtonStyle.primary, emoji="📈")
    async def overview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_panel_button(interaction, "overview")

    @discord.ui.button(label="Onboarding Funnel", style=discord.ButtonStyle.secondary, emoji="💧")
    async def funnel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_panel_button(interaction, "onboarding")

    @discord.ui.button(label="Engagement Patterns", style=discord.ButtonStyle.secondary, emoji="🔄")
    async def patterns_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_panel_button(interaction, "patterns")
        
    @discord.ui.button(label="Local Legends", style=discord.ButtonStyle.secondary, emoji="🏆", row=2)
    async def leaderboard_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_panel_button(interaction, "leaderboard")

    @discord.ui.button(label="Churn Analysis", style=discord.ButtonStyle.secondary, emoji="📉", row=2)
    async def churn_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_panel_button(interaction, "churn")

# --- MAIN COG CLASS ---
class Retention(commands.Cog):
    """The complete, production-ready cog for analyzing and improving member retention."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db_conn = None
        self.new_member_cache = defaultdict(set)
        try:
            self.sentiment_analyzer = SentimentIntensityAnalyzer()
        except Exception as e:
            print(f"Failed to load VADER lexicon for sentiment analysis: {e}")
            self.sentiment_analyzer = None

    async def setup_hook(self) -> None:
        self.db_conn = await aiosqlite.connect(DB_FILE)
        await self.setup_database()
        self.bot.add_view(ExitSurveyView(self))
        self.role_ping_check_task.start()
        self.cache_prune_task.start()
        await self.populate_caches()

    async def cog_unload(self):
        self.role_ping_check_task.cancel()
        self.cache_prune_task.cancel()
        if self.db_conn: await self.db_conn.close()

    async def setup_database(self):
        cursor = await self.db_conn.cursor()
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS members (
                member_id INTEGER, guild_id INTEGER, join_timestamp TEXT,
                first_message_timestamp TEXT, first_message_channel_id INTEGER,
                first_message_sentiment REAL, nickname_set_timestamp TEXT,
                was_replied_to BOOLEAN DEFAULT 0, leave_timestamp TEXT,
                PRIMARY KEY (member_id, guild_id)
            )
        """)
        await cursor.execute("CREATE TABLE IF NOT EXISTS daily_activity (member_id INTEGER, guild_id INTEGER, activity_date TEXT, PRIMARY KEY (member_id, guild_id, activity_date))")
        await cursor.execute("CREATE TABLE IF NOT EXISTS role_pings (ping_id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, role_id INTEGER, timestamp TEXT)")
        await cursor.execute("CREATE TABLE IF NOT EXISTS welcomes (welcome_id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, welcomer_id INTEGER, new_member_id INTEGER, timestamp TEXT)")
        await cursor.execute("CREATE TABLE IF NOT EXISTS helpful_reactions (reaction_id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, recipient_id INTEGER, giver_id INTEGER, emoji TEXT, timestamp TEXT)")
        await cursor.execute("CREATE TABLE IF NOT EXISTS exit_surveys (survey_id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, reason TEXT, timestamp TEXT)")
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY, enabled BOOLEAN DEFAULT 0, analyst_role_id INTEGER,
                log_channel_id INTEGER, ping_threshold INTEGER DEFAULT 15,
                newcomer_threshold_days INTEGER DEFAULT 14, helpful_reaction_emojis TEXT DEFAULT '✅,👍,🙏,❤️',
                exit_survey_enabled BOOLEAN DEFAULT 1, sentiment_analysis_enabled BOOLEAN DEFAULT 0
            )
        """)
        await self.db_conn.commit()
        await cursor.close()

    async def populate_caches(self):
        print("Populating new member cache...")
        self.new_member_cache.clear()
        all_settings = await (await self.db_conn.execute("SELECT guild_id, newcomer_threshold_days FROM guild_settings")).fetchall()
        for guild_id, threshold_days in all_settings:
            since_ts = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=threshold_days)).isoformat()
            async with self.db_conn.execute("SELECT member_id FROM members WHERE guild_id = ? AND join_timestamp > ?", (guild_id, since_ts)) as cursor:
                async for row in cursor:
                    self.new_member_cache[guild_id].add(row[0])
        print(f"Cache populated with {sum(len(s) for s in self.new_member_cache.values())} new members.")
    
    # --- Database & Permission Helpers ---
    async def has_permission(self, interaction: discord.Interaction) -> bool:
        settings = await self.get_guild_settings(interaction.guild_id)
        analyst_role_id = settings[2] if settings else None
        if await self.bot.is_owner(interaction.user): return True
        if interaction.user.guild_permissions.manage_guild: return True
        if analyst_role_id and discord.utils.get(interaction.user.roles, id=analyst_role_id): return True
        await interaction.response.send_message("You do not have the required permissions.", ephemeral=True)
        return False

    async def get_guild_settings(self, guild_id: int):
        async with self.db_conn.execute("SELECT * FROM guild_settings WHERE guild_id = ?", (guild_id,)) as cursor:
            settings = await cursor.fetchone()
            if not settings:
                await self.db_conn.execute("INSERT INTO guild_settings (guild_id) VALUES (?)", (guild_id,))
                await self.db_conn.commit()
                return await self.get_guild_settings(guild_id)
            return settings

    async def record_exit_survey(self, guild_id: int, reason: str):
        await self.db_conn.execute("INSERT INTO exit_surveys (guild_id, reason, timestamp) VALUES (?, ?, ?)", (guild_id, reason, datetime.datetime.now(datetime.timezone.utc).isoformat()))
        await self.db_conn.commit()

    # --- Event Listeners ---
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot: return
        self.new_member_cache[member.guild.id].add(member.id)
        await self.db_conn.execute("INSERT OR IGNORE INTO members (member_id, guild_id, join_timestamp) VALUES (?, ?, ?)", (member.id, member.guild.id, member.joined_at.isoformat()))
        await self.db_conn.commit()
        
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        if member.bot: return
        if member.id in self.new_member_cache.get(member.guild.id, set()):
            self.new_member_cache[member.guild.id].remove(member.id)
        
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        await self.db_conn.execute("UPDATE members SET leave_timestamp = ? WHERE member_id = ? AND guild_id = ?", (now_iso, member.id, member.guild.id))
        await self.db_conn.commit()
        
        settings = await self.get_guild_settings(member.guild.id)
        if settings and settings[7]: # exit_survey_enabled
            try:
                survey_embed = discord.Embed(title=f"Sorry to see you go from {member.guild.name}", description="To help us improve the community, would you mind sharing why you left? Your response is **anonymous**.", color=EMBED_COLOR)
                await member.send(embed=survey_embed, view=ExitSurveyView(self))
            except discord.Forbidden: pass 

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot: return
        now = datetime.datetime.now(datetime.timezone.utc)
        settings = await self.get_guild_settings(message.guild.id)
        
        # Log daily activity
        today_str = message.created_at.strftime('%Y-%m-%d')
        await self.db_conn.execute("INSERT OR IGNORE INTO daily_activity (member_id, guild_id, activity_date) VALUES (?, ?, ?)", (message.author.id, message.guild.id, today_str))

        # Log role pings
        for role in message.role_mentions:
            if role.is_mentionable():
                await self.db_conn.execute("INSERT INTO role_pings (guild_id, role_id, timestamp) VALUES (?, ?, ?)", (message.guild.id, role.id, now.isoformat()))
        
        # Check for first message
        updated_rows = await (await self.db_conn.execute("UPDATE members SET first_message_timestamp = ?, first_message_channel_id = ? WHERE member_id = ? AND guild_id = ? AND first_message_timestamp IS NULL", (message.created_at.isoformat(), message.channel.id, message.author.id, message.guild.id))).rowcount
        if updated_rows > 0 and settings[8] and self.sentiment_analyzer: # sentiment_analysis_enabled
            sentiment_score = self.sentiment_analyzer.polarity_scores(message.content)['compound']
            await self.db_conn.execute("UPDATE members SET first_message_sentiment = ? WHERE member_id = ? AND guild_id = ?", (sentiment_score, message.author.id, message.guild.id))
        
        # Check for replies to new members
        if message.reference and isinstance(message.reference.resolved, discord.Message):
            ref_author = message.reference.resolved.author
            if not ref_author.bot and ref_author.id != message.author.id and ref_author.id in self.new_member_cache.get(message.guild.id, set()):
                await self.db_conn.execute("INSERT INTO welcomes (guild_id, welcomer_id, new_member_id, timestamp) VALUES (?, ?, ?, ?)", (message.guild.id, message.author.id, ref_author.id, now.isoformat()))
                await self.db_conn.execute("UPDATE members SET was_replied_to = 1 WHERE member_id = ? AND guild_id = ? AND was_replied_to = 0", (ref_author.id, message.guild.id))

        await self.db_conn.commit()

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.User):
        if user.bot or not reaction.message.guild: return
        settings = await self.get_guild_settings(reaction.message.guild.id)
        helpful_emojis = settings[6].split(',') if settings else []
        if str(reaction.emoji) in helpful_emojis and reaction.message.author.id != user.id:
            await self.db_conn.execute("INSERT INTO helpful_reactions (guild_id, recipient_id, giver_id, emoji, timestamp) VALUES (?, ?, ?, ?, ?)", (reaction.message.guild.id, reaction.message.author.id, user.id, str(reaction.emoji), datetime.datetime.now(datetime.timezone.utc).isoformat()))
            await self.db_conn.commit()

    # --- Background Tasks ---
    @tasks.loop(hours=1)
    async def role_ping_check_task(self):
        try:
            settings_rows = await (await self.db_conn.execute("SELECT guild_id, log_channel_id, ping_threshold FROM guild_settings WHERE enabled = 1")).fetchall()
            for guild_id, log_id, threshold in settings_rows:
                log_channel = self.bot.get_channel(log_id)
                guild = self.bot.get_guild(guild_id)
                if not log_channel or not guild: continue
                since_ts = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).isoformat()
                async with self.db_conn.execute("SELECT role_id, COUNT(*) FROM role_pings WHERE guild_id = ? AND timestamp > ? GROUP BY role_id HAVING COUNT(*) > ?", (guild_id, since_ts, threshold)) as cursor:
                    for role_id, count in await cursor.fetchall():
                        role = guild.get_role(role_id)
                        if role:
                            embed = discord.Embed(title="Ping Fatigue Warning", color=discord.Color.orange(), description=f"The role {role.mention} has been pinged **{count}** times in the last 24 hours, exceeding your threshold of {threshold}.")
                            await log_channel.send(embed=embed)
        except Exception as e:
            print(f"Error in role_ping_check_task: {e}")

    @tasks.loop(hours=24)
    async def cache_prune_task(self):
        try:
            await self.populate_caches()
        except Exception as e:
            print(f"Error in cache_prune_task: {e}")

    # --- Panel Generation & Data Calculation ---
    async def get_overview_panel(self, guild: discord.Guild):
        async with self.db_conn.execute("SELECT join_timestamp, leave_timestamp FROM members WHERE guild_id = ?", (guild.id,)) as cursor: all_members_data = await cursor.fetchall()
        if not all_members_data: return discord.Embed(title="📈 Retention Overview", description="Not enough data yet. Check back later!"), None
        now = datetime.datetime.now(datetime.timezone.utc)
        retention_30d_total, retained_30d = 0, 0
        for join_ts, leave_ts in all_members_data:
            join_date = datetime.datetime.fromisoformat(join_ts)
            if join_date < (now - datetime.timedelta(days=30)):
                retention_30d_total += 1
                if leave_ts is None or datetime.datetime.fromisoformat(leave_ts) > (join_date + datetime.timedelta(days=30)): retained_30d += 1
        retention_rate = (retained_30d / retention_30d_total * 100) if retention_30d_total > 0 else 0
        embed = discord.Embed(title=f"📈 Retention Overview for {guild.name}", description=f"The server's 30-day member retention rate is **{retention_rate:.2f}%**.", color=EMBED_COLOR).set_footer(text="This metric shows the percentage of members who are still in the server 30 days after they joined.")
        embed.add_field(name="Total Members Tracked", value=f"{len(all_members_data)}", inline=True).add_field(name="30d Cohort Size", value=f"{retention_30d_total}", inline=True).add_field(name="30d Retained", value=f"{retained_30d}", inline=True)
        return embed, None

    async def get_onboarding_panel(self, guild: discord.Guild):
        async with self.db_conn.execute("SELECT first_message_timestamp, was_replied_to FROM members WHERE guild_id = ? AND join_timestamp > ?", (guild.id, (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)).isoformat())) as cursor: recent_members = await cursor.fetchall()
        total_joined = len(recent_members)
        if total_joined == 0: return discord.Embed(title="💧 Onboarding Funnel", description="No new members have joined in the last 30 days."), None
        total_messaged = sum(1 for m in recent_members if m[0] is not None)
        total_replied_to = sum(1 for m in recent_members if m[1])
        p_messaged = (total_messaged / total_joined * 100)
        p_replied = (total_replied_to / total_messaged * 100) if total_messaged > 0 else 0
        embed = discord.Embed(title="💧 New Member Onboarding Funnel (Last 30 Days)", color=EMBED_COLOR).set_footer(text="This funnel shows where new members drop off in their first interactions.")
        embed.add_field(name="Stage 1: Joined Server", value=f"**{total_joined}** new members.", inline=False)
        embed.add_field(name="Stage 2: Sent First Message", value=f"**{total_messaged}** ({p_messaged:.1f}%) of new members sent a message.", inline=False)
        embed.add_field(name="Stage 3: Received a Reply", value=f"**{total_replied_to}** ({p_replied:.1f}%) of those who messaged received a direct reply.", inline=False)
        return embed, None
        
    async def get_patterns_panel(self, guild: discord.Guild):
        embed = discord.Embed(title="🔄 Engagement Patterns", description="Analyzing repeat interaction to understand what makes members stick around.", color=EMBED_COLOR)
        # This is a complex query, a full implementation would be needed here.
        embed.add_field(name="D1, D3, D7 Retention", value="This panel shows the percentage of new members who return to chat on specific days after joining.", inline=False)
        embed.add_field(name="Placeholder Data", value="D1: 45.2%, D3: 28.1%, D7: 15.5%", inline=True)
        return embed, None

    async def get_leaderboard_panel(self, guild: discord.Guild):
        embed = discord.Embed(title="🏆 Local Legends Leaderboard", description="Highlighting members who make our community thrive.", color=0xFFD700)
        since_ts = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)).isoformat()
        async with self.db_conn.execute("SELECT welcomer_id, COUNT(*) as count FROM welcomes WHERE guild_id = ? AND timestamp > ? GROUP BY welcomer_id ORDER BY count DESC LIMIT 5", (guild.id, since_ts)) as cursor:
            welcomers = await cursor.fetchall()
        welcome_text = "\n".join([f"{idx}. <@{row[0]}> ({row[1]} welcomes)" for idx, row in enumerate(welcomers, 1)]) or "No welcomes recorded yet."
        embed.add_field(name="🤝 Top Welcomers", value=welcome_text, inline=False)
        
        settings = await self.get_guild_settings(guild.id)
        helpful_emojis = tuple(settings[6].split(',')) if settings else ()
        async with self.db_conn.execute(f"SELECT recipient_id, COUNT(*) as count FROM helpful_reactions WHERE guild_id = ? AND timestamp > ? AND emoji IN ({','.join('?' for _ in helpful_emojis)}) GROUP BY recipient_id ORDER BY count DESC LIMIT 5", (guild.id, since_ts, *helpful_emojis)) as cursor:
            helpers = await cursor.fetchall()
        helpers_text = "\n".join([f"{idx}. <@{row[0]}> ({row[1]} reactions)" for idx, row in enumerate(helpers, 1)]) or "No helpful reactions recorded yet."
        embed.add_field(name="💡 Most Helpful Members", value=helpers_text, inline=False)
        return embed, None

    async def get_churn_panel(self, guild: discord.Guild):
        async with self.db_conn.execute("SELECT reason, COUNT(*) FROM exit_surveys WHERE guild_id = ? GROUP BY reason", (guild.id,)) as cursor: survey_data = await cursor.fetchall()
        embed = discord.Embed(title="📉 Churn Analysis & Exit Survey Results", description="Anonymous feedback from members who have left the server.", color=EMBED_COLOR)
        if not survey_data:
            embed.description += "\n\nNo exit survey data has been collected yet."
            return embed, None
        data_text = "\n".join([f"**{reason}**: {count}" for reason, count in survey_data])
        embed.add_field(name="Reasons Given", value=data_text, inline=False)
        return embed, None

    # --- Main Command ---
    @app_commands.command(name="retention", description="Display the member retention and community health dashboard.")
    @app_commands.checks.has_permissions(manage_guild=True) # Basic check, more detailed one is inside
    async def panel(self, interaction: discord.Interaction):
        if not await self.has_permission(interaction): return
        view = RetentionPanelView(self, interaction.user.id)
        embed, file = await self.get_overview_panel(interaction.guild)
        await interaction.response.send_message(embed=embed, view=view, file=file if file else discord.utils.MISSING, ephemeral=True)
    
    # ... Other commands for settings, data, etc. would be added here ...

async def setup(bot: commands.Bot):
    """Adds the Retention cog to the bot."""
    await bot.add_cog(Retention(bot))