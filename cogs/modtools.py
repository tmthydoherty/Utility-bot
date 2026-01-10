import discord
from discord.ext import commands, tasks
from discord import app_commands, ui
import json
import os
import logging
from datetime import datetime, timedelta, timezone
import asyncio

# --- Basic Setup ---
log_modtools = logging.getLogger(__name__)

CONFIG_FILE_MODTOOLS = "modtools_config.json"
EMBED_COLOR_MODTOOLS = 0x3498DB # A professional blue

def load_modtools_config():
    """Loads the configuration from a JSON file."""
    if os.path.exists(CONFIG_FILE_MODTOOLS):
        try:
            with open(CONFIG_FILE_MODTOOLS, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_modtools_config(config):
    """Saves the configuration to a JSON file."""
    with open(CONFIG_FILE_MODTOOLS, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

# --- UI Components ---

class DiscussionUserSelect(ui.UserSelect):
    """A user select menu to choose a member for a new discussion."""
    def __init__(self, cog: commands.Cog):
        self.cog = cog
        super().__init__(placeholder="Select a user to start a discussion...", min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        """This function is called when a moderator selects a user from the dropdown."""
        await interaction.response.defer(ephemeral=True, thinking=True)
        user = self.values[0]
        cfg = self.cog.get_guild_config(interaction.guild_id)
        thread_channel_id = cfg.get("thread_channel_id")

        if not thread_channel_id:
            embed = discord.Embed(description="❌ The mod discussion channel has not been set. Use `/modtools config` to set it.", color=discord.Color.red())
            return await interaction.followup.send(embed=embed, ephemeral=True)

        thread_channel = self.cog.bot.get_channel(thread_channel_id)
        if not thread_channel or not isinstance(thread_channel, discord.TextChannel):
            embed = discord.Embed(description="❌ The configured mod discussion channel was not found.", color=discord.Color.red())
            return await interaction.followup.send(embed=embed, ephemeral=True)

        try:
            thread = await thread_channel.create_thread(
                name=f"Discussion - {user.display_name}",
                type=discord.ChannelType.private_thread,
                invitable=False
            )
        except discord.Forbidden:
            embed = discord.Embed(description="❌ I don't have permission to create private threads in that channel.", color=discord.Color.red())
            return await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            log_modtools.error(f"Failed to create thread in guild {interaction.guild_id}: {e}")
            embed = discord.Embed(description="❌ An unexpected error occurred while creating the thread.", color=discord.Color.red())
            return await interaction.followup.send(embed=embed, ephemeral=True)

        await thread.add_user(interaction.user)
        embed = discord.Embed(description=f"✅ Thread created: {thread.mention}", color=EMBED_COLOR_MODTOOLS)
        await interaction.followup.send(embed=embed, ephemeral=True)

        thread_embed = discord.Embed(
            title=f"Discussion Regarding {user.display_name}",
            description=f"This thread was created by {interaction.user.mention} to discuss the user {user.mention}.",
            color=EMBED_COLOR_MODTOOLS
        ).set_footer(text=self.cog.get_footer_text())
        initial_message = await thread.send(embed=thread_embed)

        mod_roles_ids = cfg.get("mod_roles", [])
        if mod_roles_ids:
            # **FIX**: Increased delay to 2.5 seconds to give Discord API more time.
            await asyncio.sleep(2.5)
            role_mentions = " ".join(f"<@&{role_id}>" for role_id in mod_roles_ids)
            # **FIX**: Explicitly allow role mentions on the edit. This is the key to making
            # Discord process the mention and add members to the thread without a loud ping.
            await initial_message.edit(content=role_mentions, allowed_mentions=discord.AllowedMentions(roles=True))

        if update_channel_id := cfg.get("update_channel_id"):
            if update_channel := self.cog.bot.get_channel(update_channel_id):
                log_embed = discord.Embed(
                    title="New Mod Discussion",
                    description=f"**Created by:** {interaction.user.mention}\n**Regarding User:** {user.mention}\n**Thread:** {thread.mention}",
                    color=EMBED_COLOR_MODTOOLS,
                    timestamp=datetime.now(timezone.utc)
                ).set_author(name="Moderation Action").set_footer(text=self.cog.get_footer_text())
                await update_channel.send(embed=log_embed)

        cfg.setdefault("threads", {})[str(thread.id)] = { "user_id": user.id, "created_at": datetime.now(timezone.utc).isoformat(), "reminded_24h": False, "reminded_36h": False }
        self.cog.save()

class ConfigView(ui.View):
    """A view with dropdowns to configure the bot's settings."""
    def __init__(self, cog: commands.Cog):
        super().__init__(timeout=180)
        self.cog = cog

        discussion_select = ui.ChannelSelect(
            placeholder="Select a channel for discussion threads...",
            channel_types=[discord.ChannelType.text],
            custom_id="config_discussion_channel"
        )
        discussion_select.callback = self.set_discussion_channel_callback
        self.add_item(discussion_select)
        
        update_select = ui.ChannelSelect(
            placeholder="Select a channel for update logs...",
            channel_types=[discord.ChannelType.text],
            custom_id="config_update_channel",
            row=1
        )
        update_select.callback = self.set_update_channel_callback
        self.add_item(update_select)

        add_role_select = ui.RoleSelect(
            placeholder="Add a role for silent thread invites...",
            custom_id="config_add_role",
            row=2
        )
        add_role_select.callback = self.add_role_callback
        self.add_item(add_role_select)

        remove_role_select = ui.RoleSelect(
            placeholder="Remove a role from the invite list...",
            custom_id="config_remove_role",
            row=3
        )
        remove_role_select.callback = self.remove_role_callback
        self.add_item(remove_role_select)

    async def set_discussion_channel_callback(self, interaction: discord.Interaction):
        channel_id_str = interaction.data['values'][0]
        channel_obj = interaction.guild.get_channel(int(channel_id_str))
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["thread_channel_id"] = int(channel_id_str)
        self.cog.save()
        embed = discord.Embed(description=f"✅ Mod discussion threads will now be created in {channel_obj.mention}.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def set_update_channel_callback(self, interaction: discord.Interaction):
        channel_id_str = interaction.data['values'][0]
        channel_obj = interaction.guild.get_channel(int(channel_id_str))
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["update_channel_id"] = int(channel_id_str)
        self.cog.save()
        embed = discord.Embed(description=f"✅ Mod discussion updates will now be sent to {channel_obj.mention}.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def add_role_callback(self, interaction: discord.Interaction):
        role_id_str = interaction.data['values'][0]
        role_obj = interaction.guild.get_role(int(role_id_str))
        cfg = self.cog.get_guild_config(interaction.guild_id)
        if int(role_id_str) not in cfg.get("mod_roles", []):
            cfg.setdefault("mod_roles", []).append(int(role_id_str))
            self.cog.save()
            embed = discord.Embed(description=f"✅ The {role_obj.mention} role will now be silently added to new discussions.", color=EMBED_COLOR_MODTOOLS)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"❌ That role is already in the mod list.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def remove_role_callback(self, interaction: discord.Interaction):
        role_id_str = interaction.data['values'][0]
        role_obj = interaction.guild.get_role(int(role_id_str))
        cfg = self.cog.get_guild_config(interaction.guild_id)
        if int(role_id_str) in cfg.get("mod_roles", []):
            cfg["mod_roles"].remove(int(role_id_str))
            self.cog.save()
            embed = discord.Embed(description=f"✅ The {role_obj.mention} role will no longer be added.", color=EMBED_COLOR_MODTOOLS)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"🤷 That role was not in the mod list.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)

class ModToolsView(ui.View):
    """The main view for the /modtools panel command."""
    def __init__(self, cog: commands.Cog):
        super().__init__(timeout=None) 
        self.cog = cog
        self.add_item(DiscussionUserSelect(cog))

    @ui.button(label="Close Discussion", style=discord.ButtonStyle.danger, emoji="🔒", row=1)
    async def close_discussion_button(self, interaction: discord.Interaction, button: ui.Button):
        if not isinstance(interaction.channel, discord.Thread):
            embed = discord.Embed(description="This button can only be used inside a discussion thread.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        cfg = self.cog.get_guild_config(interaction.guild_id)
        thread_id_str = str(interaction.channel.id)

        if thread_id_str not in cfg.get("threads", {}):
            embed = discord.Embed(description="This is not a tracked moderation discussion thread.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        await interaction.response.defer(ephemeral=True, thinking=True)
        close_embed = discord.Embed(description=f"🔒 Discussion closed by {interaction.user.mention}", color=discord.Color.red(), timestamp=datetime.now(timezone.utc)).set_footer(text=self.cog.get_footer_text())
        
        try:
            await interaction.channel.send(embed=close_embed)
            await interaction.channel.edit(locked=True, archived=True)
            confirm_embed = discord.Embed(description="Thread has been closed, locked, and archived.", color=EMBED_COLOR_MODTOOLS)
            await interaction.followup.send(embed=confirm_embed)
        except discord.Forbidden:
            error_embed = discord.Embed(description="Thread closed, but I could not lock or archive it. I'm missing 'Manage Threads' permission.", color=discord.Color.red())
            await interaction.followup.send(embed=error_embed)

        if thread_id_str in cfg["threads"]:
            del cfg["threads"][thread_id_str]
            self.cog.save()

# --- Main Cog Class ---

class ModTools(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_modtools_config()
        self.reminder_loop.start()
        self.thread_cleanup_loop.start()

    def cog_unload(self):
        self.reminder_loop.cancel()
        self.thread_cleanup_loop.cancel()
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Moderator Tools"

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {"thread_channel_id": None, "mod_roles": [], "update_channel_id": None, "threads": {}}
        self.config[gid].setdefault("mod_roles", [])
        self.config[gid].setdefault("threads", {})
        return self.config[gid]

    def save(self):
        save_modtools_config(self.config)

    modtools = app_commands.Group(name="modtools", description="Moderator tools commands.")

    @modtools.command(name="discussion", description="Opens the panel to start or close a moderator discussion.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def modtools_discussion(self, interaction: discord.Interaction):
        """The main entry point for the ModTools UI."""
        embed = discord.Embed(
            title="🛠️ Moderator Discussion Panel",
            description="Use the menu to start a new discussion or the button to close one.",
            color=EMBED_COLOR_MODTOOLS
        )
        await interaction.response.send_message(embed=embed, view=ModToolsView(self), ephemeral=True)

    @modtools.command(name="config", description="Configure the ModTools bot.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def modtools_config(self, interaction: discord.Interaction):
        """Opens the configuration panel."""
        embed = discord.Embed(
            title="ModTools Configuration",
            description="Use the dropdown menus below to configure the bot.",
            color=EMBED_COLOR_MODTOOLS
        )
        await interaction.response.send_message(embed=embed, view=ConfigView(self), ephemeral=True)

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
                try:
                    thread = guild.get_thread(int(thread_id_str)) or await guild.fetch_channel(int(thread_id_str))
                except (discord.NotFound, discord.Forbidden):
                    thread = None

                if not thread:
                    threads.pop(thread_id_str, None); config_changed = True; continue
                
                try:
                    user = guild.get_member(data["user_id"]) or await guild.fetch_member(data["user_id"])
                except (discord.NotFound, discord.Forbidden):
                    user = None

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
                    await thread.send(f"{user.mention}", embed=reminder_embed)
                    data["reminded_36h"] = True; config_changed = True
                elif elapsed > timedelta(hours=24) and not data.get("reminded_24h"):
                    reminder_embed = discord.Embed(description="Just a friendly reminder to please respond to this discussion when you have a moment.", color=discord.Color.yellow())
                    await thread.send(f"{user.mention}", embed=reminder_embed)
                    data["reminded_24h"] = True; config_changed = True

        if config_changed:
            self.save()

    @tasks.loop(hours=6)
    async def thread_cleanup_loop(self):
        log_modtools.info("Running scheduled mod discussion thread cleanup...")
        for guild_id_str, cfg in list(self.config.items()):
            if not (channel_id := cfg.get("thread_channel_id")): continue
            guild = self.bot.get_guild(int(guild_id_str))
            if not guild: continue
            channel = guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel): continue
            
            try:
                async for thread in channel.archived_threads(limit=100):
                    if thread.owner_id == self.bot.user.id:
                        last_message_time = thread.archive_timestamp
                        if (datetime.now(timezone.utc) - last_message_time) > timedelta(days=7):
                            log_modtools.info(f"Deleting old, archived thread {thread.id} in guild {guild.id}")
                            await thread.delete()
                
                for thread in channel.threads:
                    if thread.owner_id == self.bot.user.id and not thread.archived:
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
                log_modtools.warning(f"Missing permissions to check/archive/delete threads in guild {guild.id}.")
            except Exception as e:
                log_modtools.error(f"Error during thread cleanup for guild {guild.id}: {e}")

    @reminder_loop.before_loop
    @thread_cleanup_loop.before_loop
    async def before_any_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(ModTools(bot))
