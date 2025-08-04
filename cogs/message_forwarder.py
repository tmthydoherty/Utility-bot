import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import aiohttp
import logging
import asyncio
from datetime import datetime, timedelta, timezone

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
        self.loading_message_ids = set() # Bot's own memory for loading messages
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

    async def _forward_message(self, message: discord.Message):
        """Internal helper function to handle the actual forwarding logic."""
        if not message.content.strip() and not message.embeds and not message.attachments and not message.stickers:
            return

        async with self.config_lock:
            guild_cfg = self.config.get(str(message.guild.id), {})
            rule = guild_cfg.get(str(message.channel.id))
            
            if not rule or not rule.get("webhook_url"): return
            
            if message.webhook_id:
                managed_webhook_ids = guild_cfg.get("managed_webhook_ids", [])
                if message.webhook_id in managed_webhook_ids: return
        
        try:
            webhook = discord.Webhook.from_url(rule["webhook_url"], session=self.session)
            target_thread = self.bot.get_channel(rule.get("thread_id"))

            if not target_thread: return

            files = [await attachment.to_file() for attachment in message.attachments]
            content_to_send = message.content
            
            if message.stickers:
                sticker_urls = "\n".join([sticker.url for sticker in message.stickers])
                separator = "\n" if content_to_send else ""
                content_to_send = f"{content_to_send}{separator}{sticker_urls}"

            await webhook.send(
                content=content_to_send,
                username=message.author.display_name,
                avatar_url=message.author.display_avatar.url,
                files=files,
                embeds=message.embeds,
                allowed_mentions=discord.AllowedMentions.none(),
                thread=target_thread
            )
        except Exception as e:
            log_forwarder.error(f"Failed to forward message via webhook: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild: return

        # If it's a loading message, add it to our own memory and stop.
        if message.flags.loading:
            self.loading_message_ids.add(message.id)
            # Set a timer to remove it from memory after a while, in case an edit never comes.
            await asyncio.sleep(60)
            self.loading_message_ids.discard(message.id)
            return
        
        # If it's a normal message, process it.
        await self._forward_message(message)

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        # If this message was in our loading memory, it means it just finished loading.
        if after.id in self.loading_message_ids:
            # Process the final, updated message
            await self._forward_message(after)
            # Remove it from our memory so we don't process it again
            self.loading_message_ids.discard(after.id)

    # ... (all slash commands like /forwardset and /forwardremove remain the same) ...

async def setup(bot: commands.Bot):
    await bot.add_cog(MessageForwarder(bot))