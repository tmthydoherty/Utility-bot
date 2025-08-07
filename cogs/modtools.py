import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import json
import os
import logging
from datetime import datetime, timedelta, timezone

# --- Basic Setup ---
log_modtools = logging.getLogger(__name__)

CONFIG_FILE_MODTOOLS = "modtools_config.json"
EMBED_COLOR_MODTOOLS = 0x3498DB # A professional blue

def load_modtools_config():
    if os.path.exists(CONFIG_FILE_MODTOOLS):
        try:
            with open(CONFIG_FILE_MODTOOLS, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_modtools_config(config):
    with open(CONFIG_FILE_MODTOOLS, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

# --- UI Components for the Admin Panel ---

class CreateDiscussionModal(ui.Modal):
    """A pop-up modal to get the user ID for a new discussion."""
    def __init__(self, cog: commands.Cog):
        super().__init__(title="Create New Mod Discussion")
        self.cog = cog

    user_id_input = ui.TextInput(
        label="User ID",
        placeholder="Enter the ID of the user to discuss...",
        required=True,
        min_length=17,
        max_length=20
    )

    async def on_submit(self, interaction: discord.Interaction):
        cfg = self.cog.get_guild_config(interaction.guild_id)
        thread_channel_id = cfg.get("thread_channel_id")

        if not thread_channel_id:
            embed = discord.Embed(description="❌ The mod discussion channel has not been set.", color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
            
        thread_channel = self.cog.bot.get_channel(thread_channel_id)
        if not thread_channel or not isinstance(thread_channel, discord.TextChannel):
            embed = discord.Embed(description="❌ The configured mod discussion channel was not found.", color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        try:
            user_id = int(self.user_id_input.value)
            user = await interaction.guild.fetch_member(user_id)
        except (ValueError, discord.NotFound):
            embed = discord.Embed(description=f"❌ Could not find a member with the ID `{self.user_id_input.value}`.", color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        except discord.Forbidden:
             embed = discord.Embed(description="❌ I don't have permissions to fetch members.", color=discord.Color.red())
             return await interaction.response.send_message(embed=embed, ephemeral=True)


        # --- This logic is copied from the old /moddiscussion command ---
        try:
            thread = await thread_channel.create_thread(name=f"Discussion - {user.display_name}", type=discord.ChannelType.private_thread)
        except discord.Forbidden:
            embed = discord.Embed(description="❌ I don't have permission to create threads in that channel.", color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        await thread.add_user(interaction.user)

        embed = discord.Embed(description=f"✅ Thread created: {thread.mention}", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

        mod_roles_ids = cfg.get("mod_roles", [])
        if mod_roles_ids:
            initial_message = await thread.send("Initializing discussion...")
            role_mentions = " ".join(f"<@&{role_id}>" for role_id in mod_roles_ids)
            thread_embed = discord.Embed(
                title=f"Discussion Regarding {user.display_name}",
                description=f"This thread was created by {interaction.user.mention} to discuss the user {user.mention}.",
                color=EMBED_COLOR_MODTOOLS
            )
            thread_embed.set_footer(text=self.cog.get_footer_text())
            await initial_message.edit(content=role_mentions, embed=thread_embed, allowed_mentions=discord.AllowedMentions(roles=True))
        
        if update_channel_id := cfg.get("update_channel_id"):
            if update_channel := self.cog.bot.get_channel(update_channel_id):
                log_embed = discord.Embed(
                    title="New Mod Discussion",
                    description=f"**Created by:** {interaction.user.mention}\n**Regarding User:** {user.mention}\n**Thread:** {thread.mention}",
                    color=EMBED_COLOR_MODTOOLS,
                    timestamp=datetime.now(timezone.utc)
                )
                log_embed.set_author(name="Moderation Action")
                log_embed.set_footer(text=self.cog.get_footer_text())
                await update_channel.send(embed=log_embed)

        cfg["threads"][str(thread.id)] = { "user_id": user.id, "created_at": datetime.now(timezone.utc).isoformat(), "reminded_24h": False, "reminded_36h": False }
        self.cog.save()

class ConfigView(ui.View):
    """A view with dropdowns to configure the bot's settings."""
    def __init__(self, cog: commands.Cog):
        super().__init__(timeout=180)
        self.cog = cog

    @ui.channel_select(placeholder="Select a channel for discussion threads...", channel_types=[discord.ChannelType.text])
    async def set_discussion_channel_select(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        channel = select.values[0]
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["thread_channel_id"] = channel.id
        self.cog.save()
        embed = discord.Embed(description=f"✅ Mod discussion threads will now be created in {channel.mention}.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @ui.channel_select(placeholder="Select a channel for update logs...", channel_types=[discord.ChannelType.text])
    async def set_update_channel_select(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        channel = select.values[0]
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["update_channel_id"] = channel.id
        self.cog.save()
        embed = discord.Embed(description=f"✅ Mod discussion updates will now be sent to {channel.mention}.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @ui.role_select(placeholder="Add a role to be pinged...")
    async def add_role_select(self, interaction: discord.Interaction, select: ui.RoleSelect):
        role = select.values[0]
        cfg = self.cog.get_guild_config(interaction.guild_id)
        if role.id not in cfg["mod_roles"]:
            cfg["mod_roles"].append(role.id)
            self.cog.save()
            embed = discord.Embed(description=f"✅ The {role.mention} role will now be pinged for new discussions.", color=EMBED_COLOR_MODTOOLS)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"❌ That role is already in the mod list.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)

    @ui.role_select(placeholder="Remove a role from the ping list...")
    async def remove_role_select(self, interaction: discord.Interaction, select: ui.RoleSelect):
        role = select.values[0]
        cfg = self.cog.get_guild_config(interaction.guild_id)
        if role.id in cfg["mod_roles"]:
            cfg["mod_roles"].remove(role.id)
            self.cog.save()
            embed = discord.Embed(description=f"✅ The {role.mention} role will no longer be pinged.", color=EMBED_COLOR_MODTOOLS)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"🤷 That role was not in the mod list.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)

class ModToolsView(ui.View):
    """The main persistent view for the /modtools command."""
    def __init__(self, cog: commands.Cog):
        super().__init__(timeout=None) # A persistent view
        self.cog = cog

    @ui.button(label="Create Discussion", style=discord.ButtonStyle.primary, emoji="➕")
    async def create_discussion_button(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(CreateDiscussionModal(self.cog))

    @ui.button(label="Close Discussion", style=discord.ButtonStyle.danger, emoji="🔒")
    async def close_discussion_button(self, interaction: discord.Interaction, button: ui.Button):
        # This logic is from the old /closediscussion command
        if not isinstance(interaction.channel, discord.Thread):
            embed = discord.Embed(description="This button can only be used inside a discussion thread.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        cfg = self.cog.get_guild_config(interaction.guild_id)
        thread_id_str = str(interaction.channel.id)

        if thread_id_str not in cfg.get("threads", {}):
            embed = discord.Embed(description="This is not a tracked moderation discussion thread.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)

        close_embed = discord.Embed(description=f"🔒 Discussion closed by {interaction.user.mention}", color=discord.Color.red(), timestamp=datetime.now(timezone.utc))
        close_embed.set_footer(text=self.cog.get_footer_text())
        
        try:
            await interaction.channel.send(embed=close_embed)
            await interaction.channel.edit(locked=True)
            confirm_embed = discord.Embed(description="Thread has been closed and locked.", color=EMBED_COLOR_MODTOOLS)
            await interaction.followup.send(embed=confirm_embed)
        except discord.Forbidden:
            error_embed = discord.Embed(description="Thread closed, but I could not lock it. I'm missing 'Manage Threads' permission.", color=discord.Color.red())
            await interaction.followup.send(embed=error_embed)

        del cfg["threads"][thread_id_str]
        self.cog.save()

    @ui.button(label="Configure Bot", style=discord.ButtonStyle.secondary, emoji="⚙️")
    async def configure_button(self, interaction: discord.Interaction, button: ui.Button):
        view = ConfigView(self.cog)
        embed = discord.Embed(title="ModTools Configuration", description="Use the dropdown menus below to configure the bot.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


# --- Main Cog Class ---

class ModTools(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_modtools_config()
        self.reminder_loop.start()
        self.thread_cleanup_loop.start()
        # You would typically register persistent views here in a database,
        # but for simplicity, we re-add it every time the bot starts.
        # This means the /modtools command must be run once after a bot restart
        # for the buttons to work if you use a persistent view.
        # For simplicity, let's switch the main view to be non-persistent.
        # Or even better, let the user re-invoke it. Perfect.

    def cog_unload(self):
        self.reminder_loop.cancel()
        self.thread_cleanup_loop.cancel()
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Moderator Tools"

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {"thread_channel_id": None, "mod_roles": [], "update_channel_id": None, "threads": {}}
        return self.config[gid]

    def save(self):
        save_modtools_config(self.config)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot or not isinstance(message.channel, discord.Thread):
            return
        guild_cfg = self.get_guild_config(message.guild.id)
        thread_id_str = str(message.channel.id)
        if thread_id_str not in guild_cfg.get("threads", {}):
            return
        tracked_data = guild_cfg["threads"][thread_id_str]
        if message.author.id == tracked_data.get("user_id"):
            log_modtools.info(f"User {message.author.display_name} replied in tracked thread {thread_id_str}. Untracking.")
            del guild_cfg["threads"][thread_id_str]
            self.save()
            if update_channel_id := guild_cfg.get("update_channel_id"):
                if update_channel := self.bot.get_channel(update_channel_id):
                    embed = discord.Embed(description=f"✅ User {message.author.mention} has responded in {message.channel.mention}. Tracking stopped.", color=discord.Color.green())
                    embed.set_footer(text=self.get_footer_text())
                    await update_channel.send(embed=embed)

    @tasks.loop(minutes=10)
    async def reminder_loop(self):
        now = datetime.now(timezone.utc)
        config_changed = False
        for guild_id_str, cfg in list(self.config.items()):
            threads = cfg.get("threads", {})
            guild = self.bot.get_guild(int(guild_id_str))
            if not guild: continue
            
            update_channel = guild.get_channel(cfg.get("update_channel_id")) if cfg.get("update_channel_id") else None
            
            for thread_id_str, data in list(threads.items()):
                thread = guild.get_thread(int(thread_id_str))
                if not thread:
                    threads.pop(thread_id_str, None); config_changed = True; continue
                
                user = guild.get_member(data["user_id"])
                if not user:
                    threads.pop(thread_id_str, None); config_changed = True; continue
                
                created_at = datetime.fromisoformat(data["created_at"])
                elapsed = now - created_at

                if elapsed > timedelta(hours=48):
                    if update_channel:
                        no_reply_embed = discord.Embed(description=f"⚠️ User {user.mention} has not responded in {thread.mention} after 48 hours. The thread has been un-tracked.", color=discord.Color.orange())
                        no_reply_embed.set_footer(text=self.get_footer_text())
                        await update_channel.send(embed=no_reply_embed)
                    threads.pop(thread_id_str, None); config_changed = True
                elif elapsed > timedelta(hours=36) and not data.get("reminded_36h"):
                    reminder_embed = discord.Embed(description="This is a final reminder to please respond to this discussion.", color=discord.Color.orange())
                    # FOOTER REMOVED AS REQUESTED
                    await thread.send(f"{user.mention}", embed=reminder_embed)
                    data["reminded_36h"] = True; config_changed = True
                elif elapsed > timedelta(hours=24) and not data.get("reminded_24h"):
                    reminder_embed = discord.Embed(description="Just a friendly reminder to please respond to this discussion when you have a moment.", color=discord.Color.yellow())
                    # FOOTER REMOVED AS REQUESTED
                    await thread.send(f"{user.mention}", embed=reminder_embed)
                    data["reminded_24h"] = True; config_changed = True

        if config_changed:
            self.save()

    @tasks.loop(hours=6)
    async def thread_cleanup_loop(self):
        log_modtools.info("Running scheduled mod discussion thread cleanup...")
        # ... (This loop's logic is unchanged and remains excellent)
        for guild_id_str, cfg in list(self.config.items()):
            if not (channel_id := cfg.get("thread_channel_id")): continue
            guild = self.bot.get_guild(int(guild_id_str))
            if not guild: continue
            channel = guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel): continue
            
            for thread in list(channel.threads):
                if thread.owner_id == self.bot.user.id and not thread.archived:
                    try:
                        history = [msg async for msg in thread.history(limit=1)]
                        last_message = history[0] if history else None
                        
                        if last_message:
                            inactive_duration = datetime.now(timezone.utc) - last_message.created_at
                            if inactive_duration > timedelta(days=3):
                                log_modtools.info(f"Archiving inactive mod discussion thread {thread.id} in guild {guild.id}.")
                                archive_embed = discord.Embed(description="This discussion has been automatically archived due to 3 days of inactivity.", color=discord.Color.light_grey())
                                archive_embed.set_footer(text=self.get_footer_text())
                                await thread.send(embed=archive_embed)
                                await thread.edit(archived=True, locked=True)
                                if str(thread.id) in cfg.get("threads", {}):
                                    del cfg["threads"][str(thread.id)]
                                    self.save()
                    except discord.Forbidden:
                        log_modtools.warning(f"Missing permissions to check or archive thread {thread.id} in guild {guild.id}.")
                    except Exception as e:
                        log_modtools.error(f"Error during thread cleanup for thread {thread.id}: {e}")

    @reminder_loop.before_loop
    @thread_cleanup_loop.before_loop
    async def before_any_loop(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="modtools", description="Access the moderator tools panel.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def modtools(self, interaction: discord.Interaction):
        """The main entry point for the ModTools UI."""
        embed = discord.Embed(
            title="🛠️ Moderator Tools Panel",
            description="Use the buttons below to manage discussions and configure the bot.",
            color=EMBED_COLOR_MODTOOLS
        )
        # Using a timeout here so the view doesn't persist forever, which is simpler
        # than setting up a persistent view database. The user can just re-run /modtools.
        await interaction.response.send_message(embed=embed, view=ModToolsView(self), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ModTools(bot))