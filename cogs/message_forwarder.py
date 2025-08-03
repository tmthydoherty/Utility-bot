import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import aiohttp
import logging
import asyncio

log_forwarder = logging.getLogger(__name__)

CONFIG_FILE_FORWARDER = "message_forwarder_config.json"
EMBED_COLOR_FORWARDER = 0x5865F2 # Brand Color

def load_config_forwarder():
    if os.path.exists(CONFIG_FILE_FORWARDER):
        try:
            with open(CONFIG_FILE_FORWARDER, "r", encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, IOError): return {}
    return {}

def save_config_forwarder(config):
    with open(CONFIG_FILE_FORWARDER, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)

class MessageForwarder(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config_forwarder()
        self.session = aiohttp.ClientSession()
        self.config_lock = asyncio.Lock()
        self.config_is_dirty = False
        self.save_loop.start()

    def cog_unload(self):
        if self.config_is_dirty:
            save_config_forwarder(self.config)
        self.save_loop.cancel()
        asyncio.create_task(self.session.close())
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Message Forwarder"

    @tasks.loop(seconds=30)
    async def save_loop(self):
        async with self.config_lock:
            if self.config_is_dirty:
                await self.bot.loop.run_in_executor(None, lambda: save_config_forwarder(self.config))
                self.config_is_dirty = False
                log_forwarder.info("Message forwarder config saved to disk.")

    @save_loop.before_loop
    async def before_save_loop(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.webhook_id: return
        
        rule = None
        async with self.config_lock:
            rule = self.config.get(str(message.guild.id), {}).get(str(message.channel.id))
        
        if not rule or not rule.get("webhook_url"): return

        try:
            webhook = discord.Webhook.from_url(rule["webhook_url"], session=self.session)
            target_thread = self.bot.get_thread(rule.get("thread_id"))

            if not target_thread:
                log_forwarder.warning(f"Target thread {rule.get('thread_id')} not found for forwarding. Skipping.")
                return

            files = [await attachment.to_file() for attachment in message.attachments]
            content_with_link = f"{message.content}\n\n[Jump to Original]({message.jump_url})"
            if not content_with_link.strip() and not files and not message.embeds:
                content_with_link = "*Message had no text content or embeds.*"

            await webhook.send(
                content=content_with_link,
                username=message.author.display_name,
                avatar_url=message.author.display_avatar.url,
                files=files,
                embed=message.embeds[0] if message.embeds else None,
                allowed_mentions=discord.AllowedMentions.none(),
                thread=target_thread
            )
        except (discord.NotFound, discord.InvalidArgument):
            log_forwarder.warning(f"Webhook for guild {message.guild.id}, channel {message.channel.id} not found. Removing rule.")
            async with self.config_lock:
                guild_cfg = self.config.get(str(message.guild.id), {})
                guild_cfg.pop(str(message.channel.id), None)
                self.config_is_dirty = True
        except Exception as e:
            log_forwarder.error(f"Failed to forward message via webhook: {e}")

    @app_commands.command(name="forwardset", description="Set a channel to be forwarded to a specific thread.")
    @app_commands.checks.has_permissions(administrator=True)
    async def forward_set(self, interaction: discord.Interaction, source_channel: discord.TextChannel, target_thread: discord.Thread):
        await interaction.response.defer(ephemeral=True)
        
        if not isinstance(target_thread.parent, discord.TextChannel):
            embed = discord.Embed(description="❌ Cannot create webhooks in this type of thread. Please select a thread in a standard text channel.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_