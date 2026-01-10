import discord
from discord.ext import commands, tasks
from discord import app_commands, ui, ButtonStyle, Interaction, Embed, File
from typing import List, Optional, Dict, Any, Set
import io
import aiohttp
import asyncio
import random
import datetime
import logging
import json
import os

log = logging.getLogger(__name__)

# --- SAFE RATE LIMITS ---
DM_DELAY_MIN = 3.0
DM_DELAY_MAX = 5.0
HOURLY_LIMIT = 50
DAILY_LIMIT = 200
EMBED_RECREATE_HOURS = 12
SAVE_INTERVAL = 10  # Save progress every 10 DMs

# JSON file for persistence
TASKS_FILE = "dm_tasks.json"

# --- DM QUEUE ITEM ---
class DMTask:
    def __init__(self, guild_id: int, member_ids: List[int], embed_dict: Dict, image_data: Optional[bytes], 
                 requester_id: int, status_channel_id: Optional[int], task_id: str):
        self.guild_id = guild_id
        self.member_ids = member_ids  # Store IDs, not Member objects
        self.total = len(member_ids)
        self.embed_dict = embed_dict  # Store as dict for JSON serialization
        self.image_data = image_data
        self.requester_id = requester_id
        self.status_channel_id = status_channel_id
        self.task_id = task_id
        
        self.current_index = 0
        self.success_count = 0
        self.fail_count = 0
        self.failed_members = []
        self.start_time = None
        self.is_cancelled = False
        self.is_paused = False
        self.channel_status_message_id: Optional[int] = None
        self.last_embed_recreate = None
        self.last_save = None
        self.daily_reset_date = discord.utils.utcnow().date()
    
    def to_dict(self) -> Dict:
        """Serialize task to JSON-compatible dict"""
        return {
            "task_id": self.task_id,
            "guild_id": self.guild_id,
            "member_ids": self.member_ids,
            "embed_dict": self.embed_dict,
            "requester_id": self.requester_id,
            "status_channel_id": self.status_channel_id,
            "current_index": self.current_index,
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "failed_members": self.failed_members,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "is_cancelled": self.is_cancelled,
            "is_paused": self.is_paused,
            "channel_status_message_id": self.channel_status_message_id,
            "last_embed_recreate": self.last_embed_recreate.isoformat() if self.last_embed_recreate else None,
            "daily_reset_date": self.daily_reset_date.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict, image_data: Optional[bytes] = None):
        """Deserialize task from dict"""
        task = cls(
            guild_id=data["guild_id"],
            member_ids=data["member_ids"],
            embed_dict=data["embed_dict"],
            image_data=image_data,
            requester_id=data["requester_id"],
            status_channel_id=data.get("status_channel_id"),
            task_id=data["task_id"]
        )
        task.current_index = data["current_index"]
        task.success_count = data["success_count"]
        task.fail_count = data["fail_count"]
        task.failed_members = data.get("failed_members", [])
        task.start_time = datetime.datetime.fromisoformat(data["start_time"]) if data.get("start_time") else None
        task.is_cancelled = data.get("is_cancelled", False)
        task.is_paused = data.get("is_paused", False)
        task.channel_status_message_id = data.get("channel_status_message_id")
        task.last_embed_recreate = datetime.datetime.fromisoformat(data["last_embed_recreate"]) if data.get("last_embed_recreate") else None
        task.daily_reset_date = datetime.date.fromisoformat(data["daily_reset_date"])
        return task

# --- CONTROL VIEW ---
class ControlView(ui.View):
    def __init__(self, cog: 'DM', task: DMTask):
        super().__init__(timeout=None)
        self.cog = cog
        self.task = task

    @ui.button(label="Pause", style=ButtonStyle.secondary, emoji="⏸️", custom_id="pause_dm")
    async def pause_button(self, interaction: Interaction, button: ui.Button):
        if interaction.user.id != self.task.requester_id:
            await interaction.response.send_message("❌ Only the requester can control this.", ephemeral=True)
            return
        
        if self.task.is_paused:
            self.task.is_paused = False
            button.label = "Pause"
            button.emoji = "⏸️"
            await interaction.response.edit_message(view=self)
            await interaction.followup.send("▶️ Resuming send process...", ephemeral=True)
        else:
            self.task.is_paused = True
            button.label = "Resume"
            button.emoji = "▶️"
            await interaction.response.edit_message(view=self)
            await interaction.followup.send("⏸️ Send process paused.", ephemeral=True)

    @ui.button(label="Cancel", style=ButtonStyle.danger, emoji="⛔", custom_id="cancel_dm")
    async def cancel_button(self, interaction: Interaction, button: ui.Button):
        if interaction.user.id != self.task.requester_id:
            await interaction.response.send_message("❌ Only the requester can control this.", ephemeral=True)
            return
        
        self.task.is_cancelled = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)
        await interaction.followup.send("🛑 Cancellation requested. Stopping after current DM...", ephemeral=True)

# --- CONFIRMATION VIEW ---
class ConfirmationView(ui.View):
    def __init__(self, cog: 'DM', task: DMTask):
        super().__init__(timeout=180)
        self.cog = cog
        self.task = task
        self.message: Optional[discord.InteractionMessage] = None

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id == self.task.requester_id:
            return True
        await interaction.response.send_message("❌ This is not for you.", ephemeral=True)
        return False

    @ui.button(label="Confirm Send", style=ButtonStyle.success)
    async def confirm_button(self, interaction: Interaction, button: ui.Button):
        sent_today = self.cog.get_dms_sent_today()
        
        await interaction.response.defer(ephemeral=True)
        
        for item in self.children:
            item.disabled = True
        await self.message.edit(view=self)
        
        # Start the task
        self.cog.current_task = self.task
        self.task.start_time = discord.utils.utcnow()
        self.task.last_embed_recreate = discord.utils.utcnow()
        self.task.last_save = discord.utils.utcnow()
        
        # Save initial state
        self.cog.save_task_state()
        
        # Create status embed in channel if selected
        if self.task.status_channel_id:
            try:
                status_channel = interaction.guild.get_channel(self.task.status_channel_id)
                if status_channel:
                    status_embed = self.cog.create_status_embed(self.task, interaction.guild)
                    control_view = ControlView(self.cog, self.task)
                    status_message = await status_channel.send(embed=status_embed, view=control_view)
                    self.task.channel_status_message_id = status_message.id
                    self.cog.save_task_state()
            except discord.Forbidden:
                await interaction.followup.send("⚠️ Can't post in status channel. Continuing anyway...", ephemeral=True)
        
        # Send ephemeral confirmation
        eta_seconds = self.task.total * ((DM_DELAY_MIN + DM_DELAY_MAX) / 2)
        eta = datetime.timedelta(seconds=int(eta_seconds))
        
        status_channel = interaction.guild.get_channel(self.task.status_channel_id) if self.task.status_channel_id else None
        
        await interaction.followup.send(
            f"🚀 **DM Send Started!**\n\n"
            f"📊 Total: {self.task.total} members\n"
            f"⏱️ ETA: {eta}\n"
            f"📈 Daily limit: {DAILY_LIMIT} DMs/day\n\n"
            f"{'📺 Status updates in: ' + status_channel.mention if status_channel else '⚠️ No status channel selected'}\n\n"
            f"**If daily limit is reached, the bot will automatically resume tomorrow.**\n\n"
            f"*Running safely in the background (3-5s per DM)*",
            ephemeral=True
        )
        
        # Start sender task
        if not self.cog.dm_sender_task.is_running():
            self.cog.dm_sender_task.start()
        
        self.stop()

    @ui.button(label="Cancel", style=ButtonStyle.danger)
    async def cancel_button(self, interaction: Interaction, button: ui.Button):
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="❌ Send operation cancelled.", view=self)
        self.stop()

# --- MESSAGE MODAL ---
class MessageModal(ui.Modal, title='Create/Edit DM Message'):
    def __init__(self, view: 'DMAdminView'):
        super().__init__()
        self.view = view

        self.embed_title = ui.TextInput(
            label='Embed Title', 
            placeholder='Your main headline', 
            default=self.view.message_data.get('title'), 
            required=True, 
            style=discord.TextStyle.short, 
            row=0
        )
        self.embed_body = ui.TextInput(
            label='Embed Body', 
            placeholder='Main content. Supports markdown.', 
            default=self.view.message_data.get('body'), 
            required=True, 
            style=discord.TextStyle.paragraph, 
            row=1
        )
        self.image_url = ui.TextInput(
            label='Image URL (Optional)', 
            placeholder='https://your-image-url.com/image.png', 
            default=self.view.message_data.get('image_url'), 
            required=False, 
            row=2
        )
        self.embed_footer = ui.TextInput(
            label='Footer (Optional)', 
            placeholder='Small text at bottom', 
            default=self.view.message_data.get('footer'), 
            required=False, 
            row=3
        )

        self.add_item(self.embed_title)
        self.add_item(self.embed_body)
        self.add_item(self.image_url)
        self.add_item(self.embed_footer)

    async def on_submit(self, interaction: Interaction):
        image_url_value = self.image_url.value
        if image_url_value and not (image_url_value.startswith('http://') or image_url_value.startswith('https://')):
            await interaction.response.send_message("❌ Invalid Image URL. Must start with `http://` or `https://`.", ephemeral=True)
            return

        self.view.message_data['title'] = self.embed_title.value
        self.view.message_data['body'] = self.embed_body.value
        self.view.message_data['image_url'] = image_url_value if image_url_value else None
        self.view.message_data['footer'] = self.embed_footer.value if self.embed_footer.value else None
        
        await interaction.response.send_message('✅ Message has been saved.', ephemeral=True)
        await self.view.update_panel_message()

# --- ADMIN PANEL VIEW ---
class DMAdminView(ui.View):
    def __init__(self, author: discord.Member, guild_id: int, cog: 'DM', http_session: aiohttp.ClientSession):
        super().__init__(timeout=3600)
        self.author = author
        self.guild_id = guild_id
        self.cog = cog
        self.http_session = http_session
        self.message: Optional[discord.WebhookMessage] = None
        
        self.message_data = self.cog.get_guild_message(self.guild_id)
        
        self.selected_roles: List[discord.Role] = []
        self.target_everyone: bool = False
        self.status_channel: Optional[discord.TextChannel] = None
        self.update_buttons()

    def create_panel_embed(self) -> Embed:
        embed = Embed(title="📬 DM Admin Panel", color=discord.Color.blue())
        
        sent_today = self.cog.get_dms_sent_today()
        remaining_today = max(0, DAILY_LIMIT - sent_today)
        stats_text = (f"**DMs sent today:** {sent_today}/{DAILY_LIMIT}\n"
                     f"**Remaining today:** {remaining_today}\n"
                     f"**Hourly limit:** {HOURLY_LIMIT}/hour")
        
        # Show if task is running
        if self.cog.current_task:
            task = self.cog.current_task
            progress = (task.current_index / task.total) * 100 if task.total > 0 else 0
            stats_text += f"\n\n🚀 **Active Send:** {task.current_index}/{task.total} ({progress:.1f}%)"
        
        embed.description = stats_text

        content_value = "No message created. Click 'Create Message' to begin."
        if self.message_data:
            title = self.message_data.get('title', 'N/A')
            body = self.message_data.get('body', '')
            footer = self.message_data.get('footer')
            content_value = f"**Title:** {title}\n**Body:** {body[:200]}..."
            if footer:
                content_value += f"\n**Footer:** {footer}"
        embed.add_field(name="📝 Saved Message", value=content_value, inline=False)

        target_value = "No roles selected."
        if self.target_everyone:
            target_value = f"🌍 **Everyone** in the server"
        elif self.selected_roles:
            target_value = ", ".join(r.mention for r in self.selected_roles)
        embed.add_field(name="🎯 Current Target", value=target_value, inline=False)
        
        status_channel_value = self.status_channel.mention if self.status_channel else "Not selected"
        embed.add_field(name="📺 Status Updates Channel", value=status_channel_value, inline=False)
        
        if self.message_data and self.message_data.get('image_url'):
            embed.set_thumbnail(url=self.message_data.get('image_url'))

        embed.set_footer(text=f"Panel invoked by {self.author.display_name}")
        return embed

    def update_buttons(self):
        has_message = bool(self.message_data)
        
        self.create_edit_button.label = "Edit Message" if has_message else "Create Message"
        self.delete_message_button.disabled = not has_message
        
        can_send = has_message and (self.selected_roles or self.target_everyone) and not self.cog.current_task
        self.preview_button.disabled = not has_message
        self.test_send_button.disabled = not has_message
        self.send_button.disabled = not can_send
        
        if self.cog.current_task:
            self.send_button.label = "⏳ Sending..."
        else:
            self.send_button.label = "Send DMs"
    
    async def update_panel_message(self):
        if self.message:
            self.update_buttons()
            embed = self.create_panel_embed()
            await self.message.edit(embed=embed, view=self)

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id == self.author.id:
            return True
        await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
        return False

    async def on_timeout(self):
        if self.message:
            timeout_embed = self.create_panel_embed()
            timeout_embed.title += " (Expired)"
            timeout_embed.color = discord.Color.greyple()
            for item in self.children:
                item.disabled = True
            try:
                await self.message.edit(embed=timeout_embed, view=self)
            except discord.NotFound:
                pass

    @ui.button(label="Create Message", style=ButtonStyle.primary, emoji="✍️", row=0)
    async def create_edit_button(self, interaction: Interaction, button: ui.Button):
        modal = MessageModal(self)
        await interaction.response.send_modal(modal)

    @ui.button(label="Delete Message", style=ButtonStyle.danger, emoji="🗑️", row=0)
    async def delete_message_button(self, interaction: Interaction, button: ui.Button):
        self.cog.delete_guild_message(self.guild_id)
        self.message_data.clear()
        await interaction.response.send_message("✅ Saved message deleted.", ephemeral=True)
        await self.update_panel_message()

    @ui.button(label="Target @everyone", style=ButtonStyle.secondary, emoji="🌍", row=0)
    async def target_everyone_button(self, interaction: Interaction, button: ui.Button):
        await interaction.response.defer()
        self.target_everyone = True
        self.selected_roles = []
        await self.update_panel_message()

    @ui.select(cls=ui.RoleSelect, placeholder="Select roles to DM", min_values=1, max_values=25, row=1)
    async def role_select(self, interaction: Interaction, select: ui.RoleSelect):
        await interaction.response.defer()
        self.target_everyone = False
        self.selected_roles = select.values
        await self.update_panel_message()

    @ui.select(cls=ui.ChannelSelect, placeholder="Select status updates channel (optional)", 
               channel_types=[discord.ChannelType.text], row=2)
    async def channel_select(self, interaction: Interaction, select: ui.ChannelSelect):
        await interaction.response.defer()
        self.status_channel = select.values[0]
        await self.update_panel_message()

    @ui.button(label="Preview", style=ButtonStyle.secondary, emoji="👀", row=3)
    async def preview_button(self, interaction: Interaction, button: ui.Button):
        embed = Embed(
            title=self.message_data.get('title'), 
            description=self.message_data.get('body'), 
            color=discord.Color.green()
        )
        if self.message_data.get('image_url'):
            embed.set_image(url=self.message_data.get('image_url'))
        if self.message_data.get('footer'):
            embed.set_footer(text=self.message_data.get('footer'))
        await interaction.response.send_message("**Message Preview:**", embed=embed, ephemeral=True)
    
    @ui.button(label="Test Send to Me", style=ButtonStyle.secondary, emoji="👨‍🔬", row=3)
    async def test_send_button(self, interaction: Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        embed = Embed(
            title=self.message_data.get('title'), 
            description=self.message_data.get('body'), 
            color=discord.Color.purple()
        )
        if self.message_data.get('footer'):
            embed.set_footer(text=self.message_data.get('footer'))
        
        dm_file = None
        if self.message_data.get('image_url'):
            try:
                async with self.http_session.get(self.message_data.get('image_url')) as resp:
                    if resp.status == 200:
                        image_data = await resp.read()
                        embed.set_image(url="attachment://image.png")
                        dm_file = File(io.BytesIO(image_data), filename="image.png")
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to fetch image: {e}", ephemeral=True)
                return
        
        try:
            await self.author.send(embed=embed, file=dm_file)
            await interaction.followup.send("✅ Test DM sent!", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ Could not send DM. Check your privacy settings.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @ui.button(label="Send DMs", style=ButtonStyle.danger, emoji="🚀", row=3)
    async def send_button(self, interaction: Interaction, button: ui.Button):
        if self.cog.current_task:
            await interaction.response.send_message("❌ A DM send is already in progress.", ephemeral=True)
            return

        members_to_dm = set()
        if self.target_everyone:
            members_to_dm = {m for m in interaction.guild.members if not m.bot}
        else:
            for role in self.selected_roles:
                members_to_dm.update(m for m in role.members if not m.bot)

        total_members = len(members_to_dm)
        if total_members == 0:
            await interaction.response.send_message("❌ No valid members to DM.", ephemeral=True)
            return
        
        # Create embed dict for serialization
        embed_dict = {
            "title": self.message_data.get('title'),
            "description": self.message_data.get('body'),
            "color": discord.Color.purple().value,
            "footer": self.message_data.get('footer')
        }
        
        image_data = None
        if self.message_data.get('image_url'):
            await interaction.response.defer(ephemeral=True)
            try:
                async with self.http_session.get(self.message_data.get('image_url')) as resp:
                    if resp.status == 200:
                        image_data = await resp.read()
                        embed_dict["image_url"] = "attachment://image.png"
                    else:
                        await interaction.followup.send(
                            f"❌ Failed to fetch image (HTTP {resp.status}). Aborting.", 
                            ephemeral=True
                        )
                        return
            except Exception as e:
                await interaction.followup.send(f"❌ Image fetch error: {e}", ephemeral=True)
                return
        else:
            await interaction.response.defer(ephemeral=True)

        # Generate unique task ID
        task_id = f"{interaction.guild_id}_{int(discord.utils.utcnow().timestamp())}"
        
        # Create task with member IDs instead of Member objects
        member_ids = [m.id for m in members_to_dm]
        task = DMTask(
            guild_id=interaction.guild_id,
            member_ids=member_ids,
            embed_dict=embed_dict,
            image_data=image_data,
            requester_id=self.author.id,
            status_channel_id=self.status_channel.id if self.status_channel else None,
            task_id=task_id
        )
        
        view = ConfirmationView(self.cog, task)
        
        # Calculate stats
        days_needed = (total_members // DAILY_LIMIT) + (1 if total_members % DAILY_LIMIT else 0)
        eta_seconds = total_members * ((DM_DELAY_MIN + DM_DELAY_MAX) / 2)
        total_eta = datetime.timedelta(seconds=int(eta_seconds))
        
        await interaction.followup.send(
            f"⚠️ **Confirm DM Send**\n\n"
            f"📊 Members: **{total_members}**\n"
            f"📅 Estimated days: **{days_needed}** (at {DAILY_LIMIT} DMs/day)\n"
            f"⏱️ Total send time: **{total_eta}**\n"
            f"🚦 Rate: 3-5 seconds per DM\n"
            f"📺 Updates in: {self.status_channel.mention if self.status_channel else '*No channel selected*'}\n\n"
            f"**The bot will automatically pause at daily limits and resume the next day.**\n"
            f"*Process runs in background and survives bot restarts.*",
            view=view,
            ephemeral=True
        )
        view.message = await interaction.original_response()

# --- COG ---
class DM(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.http_session = aiohttp.ClientSession()
        self.guild_messages: Dict[int, Dict[str, Any]] = {}
        self.current_task: Optional[DMTask] = None
        self.dm_log: List[datetime.datetime] = []
        
        # Load saved task state on startup
        self.load_task_state()

    def get_guild_message(self, guild_id: int) -> Dict[str, Any]:
        return self.guild_messages.setdefault(guild_id, {})

    def delete_guild_message(self, guild_id: int):
        if guild_id in self.guild_messages:
            del self.guild_messages[guild_id]

    def get_dms_sent_today(self) -> int:
        """Count DMs sent in last 24 hours"""
        now = discord.utils.utcnow()
        today = now.date()
        
        # Clean up old logs
        self.dm_log = [timestamp for timestamp in self.dm_log if timestamp.date() == today]
        return len(self.dm_log)

    def log_dm_sent(self):
        """Log a DM send"""
        self.dm_log.append(discord.utils.utcnow())

    def save_task_state(self):
        """Save current task to JSON file"""
        if not self.current_task:
            # No task to save, delete file if exists
            if os.path.exists(TASKS_FILE):
                os.remove(TASKS_FILE)
            return
        
        try:
            task_dict = self.current_task.to_dict()
            # Note: image_data is not saved to keep file size small
            # Will need to re-fetch on resume if needed
            
            with open(TASKS_FILE, 'w') as f:
                json.dump(task_dict, f, indent=2)
            
            log.info(f"Task state saved: {self.current_task.current_index}/{self.current_task.total}")
        except Exception as e:
            log.error(f"Failed to save task state: {e}")

    def load_task_state(self):
        """Load task from JSON file on startup"""
        if not os.path.exists(TASKS_FILE):
            return
        
        try:
            with open(TASKS_FILE, 'r') as f:
                task_dict = json.load(f)
            
            # Check if task was completed
            if task_dict.get("current_index", 0) >= len(task_dict.get("member_ids", [])):
                log.info("Loaded task was already complete, removing save file")
                os.remove(TASKS_FILE)
                return
            
            # Check if task was cancelled
            if task_dict.get("is_cancelled"):
                log.info("Loaded task was cancelled, removing save file")
                os.remove(TASKS_FILE)
                return
            
            # Note: image_data needs to be re-fetched if task had an image
            # For now, we'll set it to None and handle it when resuming
            self.current_task = DMTask.from_dict(task_dict, image_data=None)
            
            log.info(f"Task loaded from save file: {self.current_task.current_index}/{self.current_task.total} complete")
            
            # Auto-resume the task
            if not self.dm_sender_task.is_running():
                self.dm_sender_task.start()
                log.info("Auto-resuming saved task...")
        except Exception as e:
            log.error(f"Failed to load task state: {e}")

    def create_status_embed(self, task: DMTask, guild: discord.Guild) -> Embed:
        """Create the status embed for tracking"""
        progress = (task.current_index / task.total) * 100 if task.total > 0 else 0
        
        embed = Embed(
            title="🚀 DM Send Status",
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        
        embed.add_field(name="📊 Progress", value=f"{task.current_index}/{task.total} ({progress:.1f}%)", inline=True)
        embed.add_field(name="✅ Successful", value=str(task.success_count), inline=True)
        embed.add_field(name="❌ Failed", value=str(task.fail_count), inline=True)
        
        sent_today = self.get_dms_sent_today()
        remaining_today = max(0, DAILY_LIMIT - sent_today)
        embed.add_field(name="📈 Today's Progress", value=f"{sent_today}/{DAILY_LIMIT} sent\n{remaining_today} remaining", inline=True)
        
        if task.start_time:
            elapsed = discord.utils.utcnow() - task.start_time
            embed.add_field(name="⏱️ Elapsed", value=str(elapsed).split('.')[0], inline=True)
            
            if task.current_index > 0:
                avg_per_dm = elapsed.total_seconds() / task.current_index
                remaining_dms = task.total - task.current_index
                eta_seconds = remaining_dms * avg_per_dm
                eta = datetime.timedelta(seconds=int(eta_seconds))
                embed.add_field(name="⏳ ETA", value=str(eta), inline=True)
            else:
                embed.add_field(name="⏳ ETA", value="Calculating...", inline=True)
        
        status_text = "🟢 Running"
        if task.is_paused:
            status_text = "⏸️ Paused"
        elif task.is_cancelled:
            status_text = "🛑 Cancelled"
        elif remaining_today == 0:
            status_text = "🌙 Waiting for daily reset"
        
        embed.add_field(name="Status", value=status_text, inline=False)
        
        requester = guild.get_member(task.requester_id)
        embed.set_footer(text=f"Requested by {requester.display_name if requester else 'Unknown'}")
        
        return embed

    @tasks.loop(seconds=5)
    async def dm_sender_task(self):
        """Background task that sends DMs safely"""
        if not self.current_task:
            self.dm_sender_task.stop()
            return

        task = self.current_task
        guild = self.bot.get_guild(task.guild_id)
        if not guild:
            log.error(f"Guild {task.guild_id} not found, stopping task")
            self.current_task = None
            self.save_task_state()
            self.dm_sender_task.stop()
            return
        
        # Handle cancellation
        if task.is_cancelled:
            await self.finish_task("🛑 Send Cancelled", guild)
            return
        
        # Handle pause
        if task.is_paused:
            return
        
        # Check if finished
        if task.current_index >= task.total:
            await self.finish_task("✅ Send Complete!", guild)
            return

        # Check if new day (reset daily counter)
        today = discord.utils.utcnow().date()
        if today != task.daily_reset_date:
            task.daily_reset_date = today
            log.info("New day detected, daily limit reset")
            self.save_task_state()

        # Check daily limit
        sent_today = self.get_dms_sent_today()
        if sent_today >= DAILY_LIMIT:
            log.info(f"Daily limit reached ({sent_today}/{DAILY_LIMIT}), waiting for next day...")
            # Update status to show waiting
            await self.update_status_embed(guild)
            return

        # Check hourly limit
        now = discord.utils.utcnow()
        hour_ago = now - datetime.timedelta(hours=1)
        sent_last_hour = len([t for t in self.dm_log if t > hour_ago])
        
        if sent_last_hour >= HOURLY_LIMIT:
            log.info(f"Hourly rate limit reached ({sent_last_hour}/{HOURLY_LIMIT}), waiting...")
            return

        # Get current member
        member_id = task.member_ids[task.current_index]
        member = guild.get_member(member_id)
        
        if not member:
            # Member left server or is no longer accessible
            task.fail_count += 1
            task.failed_members.append(f"ID:{member_id} - Member not found")
            task.current_index += 1
            self.save_task_state()
            return

        # Re-fetch image if needed and not cached
        if task.embed_dict.get("image_url") == "attachment://image.png" and not task.image_data:
            # Can't send without image data, skip this member
            log.warning("Image data missing, cannot send DM with image")
            task.fail_count += 1
            task.failed_members.append(f"{member.name} ({member.id}) - Image data unavailable")
            task.current_index += 1
            self.save_task_state()
            return

        # Create embed from dict
        embed = Embed(
            title=task.embed_dict.get("title"),
            description=task.embed_dict.get("description"),
            color=discord.Color(task.embed_dict.get("color", discord.Color.purple().value))
        )
        if task.embed_dict.get("footer"):
            embed.set_footer(text=task.embed_dict["footer"])
        if task.embed_dict.get("image_url"):
            embed.set_image(url=task.embed_dict["image_url"])

        # Send DM
        try:
            send_file = File(io.BytesIO(task.image_data), filename="image.png") if task.image_data else None
            await member.send(embed=embed, file=send_file)
            task.success_count += 1
            self.log_dm_sent()
            log.info(f"DM sent to {member.name} ({task.current_index + 1}/{task.total})")
        except discord.Forbidden:
            task.fail_count += 1
            task.failed_members.append(f"{member.name} ({member.id}) - Forbidden")
            log.warning(f"Failed to DM {member.name}: DMs disabled")
        except discord.HTTPException as e:
            task.fail_count += 1
            task.failed_members.append(f"{member.name} ({member.id}) - HTTPException")
            log.warning(f"Failed to DM {member.name}: {e}")
        
        task.current_index += 1
        
        # Save state periodically
        if task.current_index % SAVE_INTERVAL == 0:
            self.save_task_state()
            log.info(f"Progress saved: {task.current_index}/{task.total}")
        
        # Update status embed every 5 DMs or on completion
        if task.current_index % 5 == 0 or task.current_index == task.total:
            await self.update_status_embed(guild)
        
        # Recreate embed periodically
        if task.channel_status_message_id and task.last_embed_recreate:
            time_since_recreate = discord.utils.utcnow() - task.last_embed_recreate
            if time_since_recreate.total_seconds() > (EMBED_RECREATE_HOURS * 3600):
                await self.recreate_status_embed(guild)
        
        # Random delay between DMs
        await asyncio.sleep(random.uniform(DM_DELAY_MIN, DM_DELAY_MAX))

    async def update_status_embed(self, guild: discord.Guild):
        """Update the status embed in the channel"""
        task = self.current_task
        if not task or not task.channel_status_message_id or not task.status_channel_id:
            return

        try:
            channel = guild.get_channel(task.status_channel_id)
            if not channel:
                return
            
            message = await channel.fetch_message(task.channel_status_message_id)
            new_embed = self.create_status_embed(task, guild)
            await message.edit(embed=new_embed)
        except discord.NotFound:
            log.warning("Status message was deleted")
            task.channel_status_message_id = None
        except discord.HTTPException as e:
            log.error(f"Failed to update status embed: {e}")

    async def recreate_status_embed(self, guild: discord.Guild):
        """Recreate the status embed to prevent Discord issues"""
        task = self.current_task
        if not task or not task.status_channel_id:
            return

        try:
            channel = guild.get_channel(task.status_channel_id)
            if not channel:
                return
            
            # Delete old message
            if task.channel_status_message_id:
                try:
                    old_message = await channel.fetch_message(task.channel_status_message_id)
                    await old_message.delete()
                except (discord.NotFound, discord.HTTPException):
                    pass

            # Create new message
            new_embed = self.create_status_embed(task, guild)
            control_view = ControlView(self, task)
            new_message = await channel.send(embed=new_embed, view=control_view)
            task.channel_status_message_id = new_message.id
            task.last_embed_recreate = discord.utils.utcnow()
            self.save_task_state()
            log.info("Status embed recreated")
        except discord.Forbidden:
            log.warning("Can't send to status channel anymore")
            task.channel_status_message_id = None

    async def finish_task(self, title: str, guild: discord.Guild):
        """Complete the task and send final report"""
        task = self.current_task
        if not task:
            return

        elapsed = discord.utils.utcnow() - task.start_time if task.start_time else datetime.timedelta(0)
        
        # Create final report embed
        report_embed = Embed(
            title=title, 
            color=discord.Color.green() if not task.is_cancelled else discord.Color.orange(),
            timestamp=discord.utils.utcnow()
        )
        report_embed.add_field(name="✅ Successful", value=str(task.success_count), inline=True)
        report_embed.add_field(name="❌ Failed", value=str(task.fail_count), inline=True)
        report_embed.add_field(name="📊 Total", value=str(task.total), inline=True)
        report_embed.add_field(name="⏱️ Time Taken", value=str(elapsed).split('.')[0], inline=False)
        
        requester = guild.get_member(task.requester_id)
        report_embed.set_footer(text=f"Completed for {requester.display_name if requester else 'Unknown'}")
        
        # Update channel status message with final report
        if task.channel_status_message_id and task.status_channel_id:
            try:
                channel = guild.get_channel(task.status_channel_id)
                if channel:
                    message = await channel.fetch_message(task.channel_status_message_id)
                    await message.edit(embed=report_embed, view=None)
            except (discord.NotFound, discord.HTTPException) as e:
                log.error(f"Failed to update final status: {e}")
        
        # Send failure log if there were failures
        if task.failed_members:
            failure_log = "\n".join(task.failed_members)
            log_file = File(io.StringIO(failure_log), filename="dm_failures.txt")
            
            # Try to send to status channel first
            if task.status_channel_id:
                try:
                    channel = guild.get_channel(task.status_channel_id)
                    if channel and requester:
                        await channel.send(
                            f"{requester.mention} Some DMs failed. See attached log.",
                            file=log_file
                        )
                except discord.Forbidden:
                    # Fallback to DM
                    if requester:
                        try:
                            await requester.send("Some DMs failed. See attached log.", file=log_file)
                        except discord.Forbidden:
                            log.error("Could not send failure log anywhere")
            else:
                # No channel, try DM
                if requester:
                    try:
                        await requester.send("Some DMs failed. See attached log.", file=log_file)
                    except discord.Forbidden:
                        log.error("Could not DM failure log to requester")
        
        # Clean up
        self.current_task = None
        self.save_task_state()  # This will delete the save file
        self.dm_sender_task.stop()
        log.info(f"DM task completed: {task.success_count} sent, {task.fail_count} failed")

    async def cog_unload(self):
        """Cleanup when cog unloads"""
        # Save state before unloading
        self.save_task_state()
        
        await self.http_session.close()
        if self.dm_sender_task.is_running():
            self.dm_sender_task.cancel()

    @app_commands.command(name="dm_admin_panel", description="Access the DM admin panel")
    @app_commands.checks.has_permissions(administrator=True)
    async def dm_admin_panel(self, interaction: Interaction):
        view = DMAdminView(interaction.user, interaction.guild_id, self, self.http_session)
        embed = view.create_panel_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    @dm_admin_panel.error
    async def on_dm_command_error(self, interaction: Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        else:
            log.error(f"Error in dm_admin_panel: {error}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message("An unexpected error occurred. Check logs.", ephemeral=True)
            else:
                await interaction.followup.send("An unexpected error occurred. Check logs.", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(DM(bot))