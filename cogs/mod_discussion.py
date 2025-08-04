import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import logging
from datetime import datetime, timedelta, timezone

log_mod = logging.getLogger(__name__)

CONFIG_FILE_MOD = "mod_discussion_config.json"
EMBED_COLOR_MOD = 0x3498DB # A professional blue

def load_config_mod():
    if os.path.exists(CONFIG_FILE_MOD):
        try:
            with open(CONFIG_FILE_MOD, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_mod(config):
    with open(CONFIG_FILE_MOD, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

class ModDiscussion(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_mod()
        self.reminder_loop.start()
        self.thread_cleanup_loop.start()

    def cog_unload(self):
        self.reminder_loop.cancel()
        self.thread_cleanup_loop.cancel()
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Moderator Tools"

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            embed = discord.Embed(description="❌ You don't have the required permissions for this command.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            log_mod.error(f"An unhandled error occurred in a command: {error}")

    def get_guild_config(self, guild_id: int) -> dict:
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {"thread_channel_id": None, "mod_roles": [], "update_channel_id": None, "threads": {}}
        return self.config[gid]

    def save(self):
        save_config_mod(self.config)

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
            log_mod.info(f"User {message.author.display_name} replied in tracked thread {thread_id_str}. Untracking.")
            del guild_cfg["threads"][thread_id_str]
            self.save()
            if update_channel_id := guild_cfg.get("update_channel_id"):
                if update_channel := self.bot.get_channel(update_channel_id):
                    embed = discord.Embed(description=f"✅ User {message.author.mention} has responded in {message.channel.mention}. Tracking stopped.", color=discord.Color.green())
                    embed.set_footer(text=self.get_footer_text())
                    await update_channel.send(embed=embed)

    @app_commands.command(name="moddiscussion", description="Create a private mod discussion thread about a user.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def moddiscussion(self, interaction: discord.Interaction, user: discord.Member):
        cfg = self.get_guild_config(interaction.guild_id)
        thread_channel_id = cfg.get("thread_channel_id")
        
        if not thread_channel_id:
            embed = discord.Embed(description="❌ The mod discussion channel has not been set. Use `/modconfig set_channel`.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
            
        thread_channel = self.bot.get_channel(thread_channel_id)
        if not thread_channel or not isinstance(thread_channel, discord.TextChannel):
            embed = discord.Embed(description="❌ The configured mod discussion channel was not found.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        try:
            thread = await thread_channel.create_thread(name=f"Discussion - {user.display_name}", type=discord.ChannelType.private_thread)
        except discord.Forbidden:
            embed = discord.Embed(description="❌ I don't have permission to create threads in that channel.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Add the command author to the thread
        await thread.add_user(interaction.user)

        # Confirm creation to the moderator
        embed = discord.Embed(description=f"✅ Thread created: {thread.mention}", color=EMBED_COLOR_MOD)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

        # Send an initial message in the new thread, then edit it to ping mods
        mod_roles_ids = cfg.get("mod_roles", [])
        if mod_roles_ids:
            initial_message = await thread.send("Initializing discussion...")
            role_mentions = " ".join(f"<@&{role_id}>" for role_id in mod_roles_ids)
            thread_embed = discord.Embed(
                title=f"Discussion Regarding {user.display_name}",
                description=f"This thread was created by {interaction.user.mention} to discuss the user {user.mention}.",
                color=EMBED_COLOR_MOD
            )
            thread_embed.set_footer(text=self.get_footer_text())
            await initial_message.edit(content=role_mentions, embed=thread_embed, allowed_mentions=discord.AllowedMentions(roles=True))
        
        # Send a clean log to the update channel (no image, no pings)
        if update_channel_id := cfg.get("update_channel_id"):
            if update_channel := self.bot.get_channel(update_channel_id):
                log_embed = discord.Embed(
                    title="New Mod Discussion", 
                    description=f"**Created by:** {interaction.user.mention}\n**Regarding User:** {user.mention}\n**Thread:** {thread.mention}", 
                    color=EMBED_COLOR_MOD, 
                    timestamp=datetime.now(timezone.utc)
                )
                log_embed.set_author(name=f"Moderation Action")
                log_embed.set_footer(text=self.get_footer_text())
                await update_channel.send(embed=log_embed)

        # Note: The reminder system is still tied to the target user.
        # This functionality may need to be reconsidered if the user is not in the thread.
        cfg["threads"][str(thread.id)] = { "user_id": user.id, "created_at": datetime.now(timezone.utc).isoformat(), "reminded_24h": False, "reminded_36h": False }
        self.save()

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
                    reminder_embed = discord.Embed(description=f"This is a final reminder to please respond to this discussion.", color=discord.Color.orange())
                    reminder_embed.set_footer(text=self.get_footer_text())
                    await thread.send(f"{user.mention}", embed=reminder_embed); data["reminded_36h"] = True; config_changed = True
                elif elapsed > timedelta(hours=24) and not data.get("reminded_24h"):
                    reminder_embed = discord.Embed(description=f"Just a friendly reminder to please respond to this discussion when you have a moment.", color=discord.Color.yellow())
                    reminder_embed.set_footer(text=self.get_footer_text())
                    await thread.send(f"{user.mention}", embed=reminder_embed); data["reminded_24h"] = True; config_changed = True
        if config_changed: self.save()

    @tasks.loop(hours=6)
    async def thread_cleanup_loop(self):
        log_mod.info("Running scheduled mod discussion thread cleanup...")
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
                                log_mod.info(f"Archiving inactive mod discussion thread {thread.id} in guild {guild.id}.")
                                archive_embed = discord.Embed(description="This discussion has been automatically archived due to 3 days of inactivity.", color=discord.Color.light_grey())
                                archive_embed.set_footer(text=self.get_footer_text())
                                await thread.send(embed=archive_embed)
                                await thread.edit(archived=True, locked=True)
                                if str(thread.id) in cfg.get("threads", {}):
                                    del cfg["threads"][str(thread.id)]
                                    self.save()
                    except discord.Forbidden:
                        log_mod.warning(f"Missing permissions to check or archive thread {thread.id} in guild {guild.id}.")
                    except Exception as e:
                        log_mod.error(f"Error during thread cleanup for thread {thread.id}: {e}")

    @reminder_loop.before_loop
    @thread_cleanup_loop.before_loop
    async def before_any_loop(self):
        await self.bot.wait_until_ready()

    modconfig = app_commands.Group(name="modconfig", description="Configure the mod discussion feature.", default_permissions=discord.Permissions(manage_guild=True))
    
    @modconfig.command(name="set_channel", description="Set the channel where discussion threads are created.")
    async def set_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        cfg = self.get_guild_config(interaction.guild_id); cfg["thread_channel_id"] = channel.id; self.save()
        embed = discord.Embed(description=f"✅ Mod discussion threads will now be created in {channel.mention}.", color=EMBED_COLOR_MOD)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @modconfig.command(name="set_update_channel", description="Set the channel for logging discussion updates.")
    async def set_update_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        cfg = self.get_guild_config(interaction.guild_id); cfg["update_channel_id"] = channel.id; self.save()
        embed = discord.Embed(description=f"✅ Mod discussion updates will now be sent to {channel.mention}.", color=EMBED_COLOR_MOD)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @modconfig.command(name="add_role", description="Add a role to be pinged for new discussions.")
    async def add_role(self, interaction: discord.Interaction, role: discord.Role):
        cfg = self.get_guild_config(interaction.guild_id)
        if role.id not in cfg["mod_roles"]:
            cfg["mod_roles"].append(role.id); self.save()
            embed = discord.Embed(description=f"✅ The {role.mention} role will now be pinged for new discussions.", color=EMBED_COLOR_MOD)
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"❌ That role is already in the mod list.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)

    @modconfig.command(name="remove_role", description="Remove a role from the ping list.")
    async def remove_role(self, interaction: discord.Interaction, role: discord.Role):
        cfg = self.get_guild_config(interaction.guild_id)
        if role.id in cfg["mod_roles"]:
            cfg["mod_roles"].remove(role.id); self.save()
            embed = discord.Embed(description=f"❌ The {role.mention} role will no longer be pinged.", color=EMBED_COLOR_MOD)
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"🤷 That role was not in the mod list.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="closediscussion", description="Closes and locks the current mod discussion thread.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def close_discussion(self, interaction: discord.Interaction):
        if not isinstance(interaction.channel, discord.Thread):
            embed = discord.Embed(description="This command can only be used in a thread.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        cfg = self.get_guild_config(interaction.guild_id); thread_id_str = str(interaction.channel.id)
        if thread_id_str not in cfg.get("threads", {}):
            embed = discord.Embed(description="This is not a tracked moderation discussion thread.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        close_embed = discord.Embed(description=f"🔒 Discussion closed by {interaction.user.mention}", color=discord.Color.red(), timestamp=datetime.now(timezone.utc))
        close_embed.set_footer(text=self.get_footer_text())
        try:
            await interaction.response.defer(ephemeral=True); await interaction.channel.send(embed=close_embed); await interaction.channel.edit(locked=True)
            confirm_embed = discord.Embed(description="Thread has been closed and locked.", color=EMBED_COLOR_MOD)
            confirm_embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=confirm_embed)
        except discord.Forbidden:
            error_embed = discord.Embed(description="Thread closed, but I could not lock it. I'm missing 'Manage Threads' permission.", color=discord.Color.red())
            error_embed.set_footer(text=self.get_footer_text())
            if not interaction.response.is_done(): await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else: await interaction.followup.send(embed=error_embed, ephemeral=True)
        del cfg["threads"][thread_id_str]; self.save()

async def setup(bot: commands.Bot):
    await bot.add_cog(ModDiscussion(bot))
