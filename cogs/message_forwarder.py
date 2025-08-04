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
        if not message.guild:
            return

        # First, check if this message should be forwarded at all.
        async with self.config_lock:
            guild_cfg = self.config.get(str(message.guild.id), {})
            rule = guild_cfg.get(str(message.channel.id))
            
            # If there's no rule for this channel, stop.
            if not rule or not rule.get("webhook_url"):
                return
            
            # If the message is from a webhook, check if it's one of our own to prevent loops.
            if message.webhook_id:
                managed_webhook_ids = guild_cfg.get("managed_webhook_ids", [])
                if message.webhook_id in managed_webhook_ids:
                    return # It's our own message, so we stop.
        
        # If we reach here, it's a valid message to be forwarded.
        try:
            webhook = discord.Webhook.from_url(rule["webhook_url"], session=self.session)
            target_thread = self.bot.get_channel(rule.get("thread_id"))

            if not target_thread:
                log_forwarder.warning(f"Target thread {rule.get('thread_id')} not found. Skipping.")
                return

            files = [await attachment.to_file() for attachment in message.attachments]
            content_to_send = message.content
            
            if message.stickers:
                sticker_urls = "\n".join([sticker.url for sticker in message.stickers])
                separator = "\n" if content_to_send else ""
                content_to_send = f"{content_to_send}{separator}{sticker_urls}"

            # Only send the placeholder if there's truly nothing in the message
            if not content_to_send.strip() and not files and not message.embeds:
                content_to_send = "*Message had no text content or embeds.*"

            await webhook.send(
                content=content_to_send,
                username=message.author.display_name,
                avatar_url=message.author.display_avatar.url,
                files=files,
                embeds=message.embeds, # Now correctly sends all embeds
                allowed_mentions=discord.AllowedMentions.none(),
                thread=target_thread
            )
        except (discord.NotFound, discord.HTTPException) as e:
            if isinstance(e, discord.NotFound) or (isinstance(e, discord.HTTPException) and e.code == 10015):
                log_forwarder.warning(f"Webhook for guild {message.guild.id}, channel {message.channel.id} not found. Removing rule.")
                async with self.config_lock:
                    current_guild_cfg = self.config.get(str(message.guild.id), {})
                    current_guild_cfg.pop(str(message.channel.id), None)
                    self.config_is_dirty = True
            else:
                log_forwarder.error(f"Failed to forward message via webhook: {e}")

    @app_commands.command(name="forwardset", description="Set a channel to be forwarded to a specific thread.")
    @app_commands.checks.has_permissions(administrator=True)
    async def forward_set(self, interaction: discord.Interaction, source_channel: discord.TextChannel, target_thread: discord.Thread):
        await interaction.response.defer(ephemeral=True)
        
        if not isinstance(target_thread.parent, (discord.TextChannel, discord.ForumChannel)):
            embed = discord.Embed(description="❌ Cannot create webhooks in this type of thread. Please select a thread in a standard text or forum channel.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.followup.send(embed=embed)

        webhook = None
        try:
            webhook = await asyncio.wait_for(
                target_thread.parent.create_webhook(name=f"Forwarder - {source_channel.name}"),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            embed = discord.Embed(description="❌ The request to create a webhook timed out. Discord might be having issues. Please try again later.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.followup.send(embed=embed)
        except discord.HTTPException as e:
            log_forwarder.error(f"Failed to create webhook: {e}")
            embed = discord.Embed(description=f"❌ An error occurred while creating the webhook. I may be missing permissions, or the thread is archived.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.followup.send(embed=embed)
        
        async with self.config_lock:
            guild_id_str = str(interaction.guild_id)
            guild_cfg = self.config.setdefault(guild_id_str, {})
            guild_cfg[str(source_channel.id)] = {"thread_id": target_thread.id, "webhook_url": webhook.url}
            
            managed_ids = guild_cfg.setdefault("managed_webhook_ids", [])
            if webhook.id not in managed_ids:
                managed_ids.append(webhook.id)
                
            self.config_is_dirty = True
        
        embed = discord.Embed(description=f"✅ Forwarding enabled from {source_channel.mention} to **{target_thread.name}**.", color=EMBED_COLOR_FORWARDER)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="forwardremove", description="Stop forwarding messages from a channel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def forward_remove(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)
        
        webhook_to_delete = None
        rule_was_found = False
        async with self.config_lock:
            guild_cfg = self.config.get(str(interaction.guild.id), {})
            rule = guild_cfg.pop(str(channel.id), None)
            
            if rule:
                rule_was_found = True
                webhook_url = rule.get("webhook_url")
                if webhook_url:
                    try:
                        webhook_id = int(webhook_url.split('/')[-2])
                        if "managed_webhook_ids" in guild_cfg and webhook_id in guild_cfg["managed_webhook_ids"]:
                            guild_cfg["managed_webhook_ids"].remove(webhook_id)
                        webhook_to_delete = discord.Webhook.from_url(webhook_url, session=self.session)
                    except (ValueError, IndexError):
                        pass
                self.config_is_dirty = True
        
        if rule_was_found:
            if webhook_to_delete:
                try:
                    await asyncio.wait_for(webhook_to_delete.delete(), timeout=10.0)
                except (discord.NotFound, discord.HTTPException, asyncio.TimeoutError):
                    pass
            
            embed = discord.Embed(description=f"✅ Forwarding has been disabled for {channel.mention}.", color=EMBED_COLOR_FORWARDER)
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)
        else:
            embed = discord.Embed(description=f"There was no forwarding rule set up for {channel.mention} to remove.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text