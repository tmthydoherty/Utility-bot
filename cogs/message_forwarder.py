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
        
        async with self.config_lock:
            rule = self.config.get(str(message.guild.id), {}).get(str(message.channel.id))
        
        if not rule or not rule.get("webhook_url"): return

        try:
            webhook = discord.Webhook.from_url(rule["webhook_url"], session=self.session)
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
                allowed_mentions=discord.AllowedMentions.none()
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
        
        try:
            webhook = await target_thread.create_webhook(name=f"Forwarder - {source_channel.name}")
        except discord.Forbidden:
            embed = discord.Embed(description="❌ I don't have permission to create webhooks in that thread.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.followup.send(embed=embed)
        
        async with self.config_lock:
            guild_id_str = str(interaction.guild_id)
            if guild_id_str not in self.config: self.config[guild_id_str] = {}
            self.config[guild_id_str][str(source_channel.id)] = {"thread_id": target_thread.id, "webhook_url": webhook.url}
            self.config_is_dirty = True
        
        embed = discord.Embed(description=f"✅ Forwarding enabled from {source_channel.mention} to **{target_thread.name}**.", color=EMBED_COLOR_FORWARDER)
        embed.set_footer(text=self.get_footer_text())
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="forwardremove", description="Stop forwarding messages from a channel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def forward_remove(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True)
        
        webhook_url_to_delete = None
        rule_was_found = False
        async with self.config_lock:
            rule = self.config.get(str(interaction.guild_id), {}).pop(str(channel.id), None)
            if rule:
                rule_was_found = True
                webhook_url_to_delete = rule.get("webhook_url")
                self.config_is_dirty = True
        
        if rule_was_found:
            if webhook_url_to_delete:
                try:
                    await discord.Webhook.from_url(webhook_url_to_delete, session=self.session).delete()
                except (discord.NotFound, discord.InvalidArgument): pass
            
            embed = discord.Embed(description=f"✅ Forwarding has been disabled for {channel.mention}.", color=EMBED_COLOR_FORWARDER)
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)
        else:
            embed = discord.Embed(description=f"There was no forwarding rule set up for {channel.mention} to remove.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text())
            await interaction.followup.send(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(MessageForwarder(bot))