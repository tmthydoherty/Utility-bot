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
EMBED_COLOR_MODTOOLS = 0x3498DB  # A professional blue

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
    """Saves the configuration to a JSON file (called via asyncio.to_thread)."""
    with open(CONFIG_FILE_MODTOOLS, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

# --- UI Components ---

class DiscussionUserSelect(ui.UserSelect):
    """A user select menu to choose a member for a new discussion."""
    def __init__(self, cog: 'ModTools'):
        self.cog = cog
        super().__init__(
            placeholder="Select a user to start a discussion...",
            min_values=1, max_values=1,
            custom_id="modtools:discussion_user_select"
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        user = self.values[0]

        # Debounce: prevent duplicate thread creation
        debounce_key = (interaction.guild_id, user.id)
        if debounce_key in self.cog._creating_for:
            embed = discord.Embed(description="A discussion for this user is already being created. Please wait.", color=discord.Color.yellow())
            return await interaction.followup.send(embed=embed, ephemeral=True)
        self.cog._creating_for.add(debounce_key)

        try:
            await self._create_discussion(interaction, user)
        finally:
            self.cog._creating_for.discard(debounce_key)

    async def _create_discussion(self, interaction: discord.Interaction, user: discord.Member):
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
                name=f"Discussion - {user.display_name[:80]}",
                type=discord.ChannelType.private_thread,
                invitable=False
            )
        except discord.Forbidden:
            embed = discord.Embed(description="❌ I don't have permission to create private threads in that channel.", color=discord.Color.red())
            return await interaction.followup.send(embed=embed, ephemeral=True)
        except discord.HTTPException as e:
            log_modtools.error(f"Failed to create thread in guild {interaction.guild_id}: {e}")
            embed = discord.Embed(description="❌ An unexpected error occurred while creating the thread.", color=discord.Color.red())
            return await interaction.followup.send(embed=embed, ephemeral=True)

        try:
            await thread.add_user(interaction.user)
        except (discord.Forbidden, discord.HTTPException):
            log_modtools.warning(f"Could not add user {interaction.user.id} to thread {thread.id}")

        embed = discord.Embed(description=f"✅ Thread created: {thread.mention}", color=EMBED_COLOR_MODTOOLS)
        await interaction.followup.send(embed=embed, ephemeral=True)

        thread_embed = discord.Embed(
            title=f"Discussion Regarding {user.display_name}",
            description=f"This thread was created by {interaction.user.mention} to discuss the user {user.mention}.",
            color=EMBED_COLOR_MODTOOLS
        ).set_footer(text=self.cog.get_footer_text())
        initial_message = await thread.send(embed=thread_embed)

        # Save EARLY — track the thread before best-effort operations so a crash can't orphan it
        async with self.cog._config_lock:
            cfg.setdefault("threads", {})[str(thread.id)] = {
                "user_id": user.id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "reminded_24h": False,
                "reminded_36h": False,
                "role_given": False
            }
            await self.cog._save()

        # Best-effort: silently invite mod roles via message edit
        mod_roles_ids = cfg.get("mod_roles", [])
        if mod_roles_ids:
            await asyncio.sleep(2.5)
            role_mentions = " ".join(f"<@&{role_id}>" for role_id in mod_roles_ids)
            try:
                await initial_message.edit(content=role_mentions, allowed_mentions=discord.AllowedMentions(roles=True))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                log_modtools.warning(f"Failed to edit role mentions into thread {thread.id}: {e}")

        # Best-effort: send update log
        if update_channel_id := cfg.get("update_channel_id"):
            if update_channel := self.cog.bot.get_channel(update_channel_id):
                try:
                    log_embed = discord.Embed(
                        title="New Mod Discussion",
                        description=f"**Created by:** {interaction.user.mention}\n**Regarding User:** {user.mention}\n**Thread:** {thread.mention}",
                        color=EMBED_COLOR_MODTOOLS,
                        timestamp=datetime.now(timezone.utc)
                    ).set_author(name="Moderation Action").set_footer(text=self.cog.get_footer_text())
                    await update_channel.send(embed=log_embed)
                except (discord.Forbidden, discord.HTTPException) as e:
                    log_modtools.warning(f"Failed to send update log: {e}")


class ConfigView(ui.View):
    """A view with dropdowns to configure the bot's settings."""
    def __init__(self, cog: 'ModTools'):
        super().__init__(timeout=180)
        self.cog = cog
        self.message: discord.Message | None = None

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

        no_response_role_select = ui.RoleSelect(
            placeholder="Set the no-response penalty role...",
            custom_id="config_no_response_role",
            row=4
        )
        no_response_role_select.callback = self.set_no_response_role_callback
        self.add_item(no_response_role_select)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except (discord.NotFound, discord.HTTPException):
                pass

    async def set_discussion_channel_callback(self, interaction: discord.Interaction):
        channel_id = int(interaction.data['values'][0])
        channel_obj = interaction.guild.get_channel(channel_id)
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["thread_channel_id"] = channel_id
        await self.cog._save()
        label = channel_obj.mention if channel_obj else f"channel `{channel_id}`"
        embed = discord.Embed(description=f"✅ Mod discussion threads will now be created in {label}.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def set_update_channel_callback(self, interaction: discord.Interaction):
        channel_id = int(interaction.data['values'][0])
        channel_obj = interaction.guild.get_channel(channel_id)
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["update_channel_id"] = channel_id
        await self.cog._save()
        label = channel_obj.mention if channel_obj else f"channel `{channel_id}`"
        embed = discord.Embed(description=f"✅ Mod discussion updates will now be sent to {label}.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def add_role_callback(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0])
        role_obj = interaction.guild.get_role(role_id)
        cfg = self.cog.get_guild_config(interaction.guild_id)
        if role_id not in cfg.get("mod_roles", []):
            cfg.setdefault("mod_roles", []).append(role_id)
            await self.cog._save()
            label = role_obj.mention if role_obj else f"role `{role_id}`"
            embed = discord.Embed(description=f"✅ The {label} role will now be silently added to new discussions.", color=EMBED_COLOR_MODTOOLS)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description="❌ That role is already in the mod list.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def remove_role_callback(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0])
        role_obj = interaction.guild.get_role(role_id)
        cfg = self.cog.get_guild_config(interaction.guild_id)
        if role_id in cfg.get("mod_roles", []):
            cfg["mod_roles"].remove(role_id)
            await self.cog._save()
            label = role_obj.mention if role_obj else f"role `{role_id}`"
            embed = discord.Embed(description=f"✅ The {label} role will no longer be added.", color=EMBED_COLOR_MODTOOLS)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description="🤷 That role was not in the mod list.", color=discord.Color.yellow())
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def set_no_response_role_callback(self, interaction: discord.Interaction):
        role_id = int(interaction.data['values'][0])
        role_obj = interaction.guild.get_role(role_id)
        cfg = self.cog.get_guild_config(interaction.guild_id)
        cfg["no_response_role_id"] = role_id
        await self.cog._save()
        label = role_obj.mention if role_obj else f"role `{role_id}`"
        embed = discord.Embed(description=f"✅ The {label} role will be assigned to users who don't respond within 48 hours.", color=EMBED_COLOR_MODTOOLS)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ModToolsView(ui.View):
    """The main view for the /modtools panel command."""
    def __init__(self, cog: 'ModTools'):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(DiscussionUserSelect(cog))

    @ui.button(label="Close Discussion", style=discord.ButtonStyle.danger, emoji="🔒", row=1, custom_id="modtools:close_discussion")
    async def close_discussion_button(self, interaction: discord.Interaction, button: ui.Button):
        # Permission gate
        if not interaction.user.guild_permissions.manage_messages:
            embed = discord.Embed(description="❌ You don't have permission to close discussions.", color=discord.Color.red())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        if not isinstance(interaction.channel, discord.Thread):
            embed = discord.Embed(description="This button can only be used inside a discussion thread.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        cfg = self.cog.get_guild_config(interaction.guild_id)
        thread_id_str = str(interaction.channel.id)

        if thread_id_str not in cfg.get("threads", {}):
            embed = discord.Embed(description="This is not a tracked moderation discussion thread.", color=discord.Color.yellow())
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Remove the no-response role if it was given
        thread_data = cfg["threads"][thread_id_str]
        if thread_data.get("role_given"):
            no_response_role_id = cfg.get("no_response_role_id")
            if no_response_role_id:
                member = interaction.guild.get_member(thread_data["user_id"])
                if member:
                    role = interaction.guild.get_role(no_response_role_id)
                    if role and role in member.roles:
                        try:
                            await member.remove_roles(role, reason="Mod discussion closed")
                        except (discord.Forbidden, discord.HTTPException) as e:
                            log_modtools.warning(f"Failed to remove no-response role from {thread_data['user_id']}: {e}")

        close_embed = discord.Embed(
            description=f"🔒 Discussion closed by {interaction.user.mention}",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        ).set_footer(text=self.cog.get_footer_text())

        try:
            await interaction.channel.send(embed=close_embed)
            await interaction.channel.edit(locked=True, archived=True)
            confirm_embed = discord.Embed(description="Thread has been closed, locked, and archived.", color=EMBED_COLOR_MODTOOLS)
            await interaction.followup.send(embed=confirm_embed)
        except discord.Forbidden:
            error_embed = discord.Embed(description="Thread closed, but I could not lock or archive it. I'm missing 'Manage Threads' permission.", color=discord.Color.red())
            await interaction.followup.send(embed=error_embed)

        async with self.cog._config_lock:
            if thread_id_str in cfg.get("threads", {}):
                del cfg["threads"][thread_id_str]
                await self.cog._save()

# --- Main Cog Class ---

class ModTools(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_modtools_config()
        self._config_lock = asyncio.Lock()
        self._creating_for: set[tuple[int, int]] = set()
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
            self.config[gid] = {
                "thread_channel_id": None,
                "mod_roles": [],
                "update_channel_id": None,
                "no_response_role_id": None,
                "threads": {}
            }
        self.config[gid].setdefault("mod_roles", [])
        self.config[gid].setdefault("threads", {})
        self.config[gid].setdefault("no_response_role_id", None)
        return self.config[gid]

    async def _save(self):
        """Async-safe config save. Offloads blocking I/O to a thread."""
        await asyncio.to_thread(save_modtools_config, self.config)

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
        view = ConfigView(self)
        embed = discord.Embed(
            title="ModTools Configuration",
            description="Use the dropdown menus below to configure the bot.",
            color=EMBED_COLOR_MODTOOLS
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot or not isinstance(message.channel, discord.Thread):
            return

        guild_cfg = self.get_guild_config(message.guild.id)
        thread_id_str = str(message.channel.id)

        # Quick check before acquiring the lock
        if thread_id_str not in guild_cfg.get("threads", {}):
            return
        tracked_data = guild_cfg["threads"][thread_id_str]
        if message.author.id != tracked_data.get("user_id"):
            return

        # User replied — untrack under lock
        role_was_given = tracked_data.get("role_given", False)
        no_response_role_id = guild_cfg.get("no_response_role_id")

        async with self._config_lock:
            # Re-check under lock in case another coroutine already untracked
            if thread_id_str not in guild_cfg.get("threads", {}):
                return
            log_modtools.info(f"User {message.author.display_name} replied in tracked thread {thread_id_str}. Untracking.")
            del guild_cfg["threads"][thread_id_str]
            await self._save()

        # Remove the no-response role if it was given (outside lock — no need to hold it for API calls)
        if role_was_given and no_response_role_id:
            role = message.guild.get_role(no_response_role_id)
            if role and role in message.author.roles:
                try:
                    await message.author.remove_roles(role, reason="User responded in mod discussion")
                except (discord.Forbidden, discord.HTTPException) as e:
                    log_modtools.warning(f"Failed to remove no-response role from {message.author.id}: {e}")

        # Send update log (outside lock)
        if update_channel_id := guild_cfg.get("update_channel_id"):
            if update_channel := self.bot.get_channel(update_channel_id):
                try:
                    description = f"✅ User {message.author.mention} has responded in {message.channel.mention}. Tracking stopped."
                    if role_was_given and no_response_role_id:
                        role = message.guild.get_role(no_response_role_id)
                        role_label = role.mention if role else f"role `{no_response_role_id}`"
                        description += f"\nThe {role_label} role has been removed."
                    embed = discord.Embed(description=description, color=discord.Color.green())
                    embed.set_footer(text=self.get_footer_text())
                    await update_channel.send(embed=embed)
                except (discord.Forbidden, discord.HTTPException) as e:
                    log_modtools.warning(f"Failed to send update log: {e}")

    @tasks.loop(minutes=10)
    async def reminder_loop(self):
        try:
            now = datetime.now(timezone.utc)
            config_changed = False
            for guild_id_str, cfg in list(self.config.items()):
                threads = cfg.get("threads", {})
                guild = self.bot.get_guild(int(guild_id_str))
                if not guild:
                    continue

                update_channel = guild.get_channel(cfg.get("update_channel_id")) if cfg.get("update_channel_id") else None

                for thread_id_str, data in list(threads.items()):
                    try:
                        thread = guild.get_thread(int(thread_id_str)) or await guild.fetch_channel(int(thread_id_str))
                    except (discord.NotFound, discord.Forbidden):
                        thread = None

                    if not thread:
                        async with self._config_lock:
                            threads.pop(thread_id_str, None)
                            config_changed = True
                        continue

                    try:
                        user = guild.get_member(data["user_id"]) or await guild.fetch_member(data["user_id"])
                    except (discord.NotFound, discord.Forbidden):
                        user = None

                    if not user:
                        async with self._config_lock:
                            threads.pop(thread_id_str, None)
                            config_changed = True
                        continue

                    created_at = datetime.fromisoformat(data["created_at"])
                    elapsed = now - created_at

                    # 48h: assign no-response role, alert mods, keep tracking
                    if elapsed > timedelta(hours=48) and not data.get("role_given"):
                        no_response_role_id = cfg.get("no_response_role_id")
                        role_assigned = False
                        role_label = None

                        if no_response_role_id:
                            role = guild.get_role(no_response_role_id)
                            if role:
                                try:
                                    await user.add_roles(role, reason="No response in mod discussion after 48h")
                                    role_assigned = True
                                    role_label = role.mention
                                except (discord.Forbidden, discord.HTTPException) as e:
                                    log_modtools.warning(f"Failed to assign no-response role to {user.id}: {e}")

                        # Re-check under lock — thread may have been untracked during the API call
                        need_role_undo = False
                        async with self._config_lock:
                            if thread_id_str in threads:
                                threads[thread_id_str]["role_given"] = True
                                config_changed = True
                            elif role_assigned:
                                need_role_undo = True

                        # If the thread was untracked while we were assigning the role, undo it
                        if need_role_undo:
                            role = guild.get_role(no_response_role_id) if no_response_role_id else None
                            if role and role in user.roles:
                                try:
                                    await user.remove_roles(role, reason="User responded while role was being assigned")
                                except (discord.Forbidden, discord.HTTPException) as e:
                                    log_modtools.warning(f"Failed to undo no-response role for {user.id}: {e}")
                            continue

                        if update_channel:
                            try:
                                description = f"⚠️ User {user.mention} has not responded in {thread.mention} after 48 hours."
                                if role_label:
                                    description += f"\nThey have been given the {role_label} role."
                                alert_embed = discord.Embed(description=description, color=discord.Color.orange())
                                alert_embed.set_footer(text=self.get_footer_text())
                                await update_channel.send(embed=alert_embed)
                            except (discord.Forbidden, discord.HTTPException) as e:
                                log_modtools.warning(f"Failed to send 48h alert: {e}")

                    # 36h: final reminder
                    elif elapsed > timedelta(hours=36) and not data.get("reminded_36h"):
                        try:
                            reminder_embed = discord.Embed(description="This is a final reminder to please respond to this discussion.", color=discord.Color.orange())
                            await thread.send(f"{user.mention}", embed=reminder_embed)
                        except (discord.Forbidden, discord.HTTPException) as e:
                            log_modtools.warning(f"Failed to send 36h reminder in thread {thread_id_str}: {e}")
                        async with self._config_lock:
                            if thread_id_str in threads:
                                data["reminded_36h"] = True
                                config_changed = True

                    # 24h: friendly reminder
                    elif elapsed > timedelta(hours=24) and not data.get("reminded_24h"):
                        try:
                            reminder_embed = discord.Embed(description="Just a friendly reminder to please respond to this discussion when you have a moment.", color=discord.Color.yellow())
                            await thread.send(f"{user.mention}", embed=reminder_embed)
                        except (discord.Forbidden, discord.HTTPException) as e:
                            log_modtools.warning(f"Failed to send 24h reminder in thread {thread_id_str}: {e}")
                        async with self._config_lock:
                            if thread_id_str in threads:
                                data["reminded_24h"] = True
                                config_changed = True

            if config_changed:
                async with self._config_lock:
                    await self._save()
        except Exception as e:
            await self.bot.error_reporter.report("ModTools", f"reminder_loop: {e}")

    @tasks.loop(hours=6)
    async def thread_cleanup_loop(self):
        try:
            log_modtools.info("Running scheduled mod discussion thread cleanup...")
            for guild_id_str, cfg in list(self.config.items()):
                if not (channel_id := cfg.get("thread_channel_id")):
                    continue
                guild = self.bot.get_guild(int(guild_id_str))
                if not guild:
                    continue
                channel = guild.get_channel(channel_id)
                if not channel or not isinstance(channel, discord.TextChannel):
                    continue

                tracked_thread_ids = set(cfg.get("threads", {}).keys())

                try:
                    async for thread in channel.archived_threads(limit=None):
                        if thread.owner_id == self.bot.user.id and str(thread.id) not in tracked_thread_ids:
                            last_message_time = thread.archive_timestamp
                            if (datetime.now(timezone.utc) - last_message_time) > timedelta(days=7):
                                log_modtools.info(f"Deleting old, archived thread {thread.id} in guild {guild.id}")
                                await thread.delete()
                                await asyncio.sleep(1.5)

                    for thread in channel.threads:
                        if thread.owner_id == self.bot.user.id and not thread.archived:
                            # Skip actively tracked threads — they're managed by the reminder loop
                            if str(thread.id) in tracked_thread_ids:
                                continue

                            history = [msg async for msg in thread.history(limit=1)]
                            last_message = history[0] if history else None

                            if last_message:
                                inactive_duration = datetime.now(timezone.utc) - last_message.created_at
                                if inactive_duration > timedelta(days=3):
                                    log_modtools.info(f"Archiving inactive mod discussion thread {thread.id} in guild {guild.id}.")
                                    try:
                                        archive_embed = discord.Embed(description="This discussion has been automatically archived due to 3 days of inactivity.", color=discord.Color.light_grey())
                                        archive_embed.set_footer(text=self.get_footer_text())
                                        await thread.send(embed=archive_embed)
                                        await thread.edit(archived=True, locked=True)
                                    except (discord.Forbidden, discord.HTTPException) as e:
                                        log_modtools.warning(f"Failed to archive thread {thread.id}: {e}")

                                    await asyncio.sleep(1.5)
                except discord.Forbidden:
                    log_modtools.warning(f"Missing permissions to check/archive/delete threads in guild {guild.id}.")
                except Exception as e:
                    log_modtools.error(f"Error during thread cleanup for guild {guild.id}: {e}", exc_info=True)
        except Exception as e:
            await self.bot.error_reporter.report("ModTools", f"thread_cleanup_loop: {e}")

    @reminder_loop.before_loop
    @thread_cleanup_loop.before_loop
    async def before_any_loop(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    cog = ModTools(bot)
    await bot.add_cog(cog)
    bot.add_view(ModToolsView(cog))
