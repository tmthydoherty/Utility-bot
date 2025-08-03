import discord
from discord.ext import commands
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

    def cog_unload(self):
        asyncio.create_task(self.session.close())
        
    def get_footer_text(self):
        return f"{self.bot.user.name} • Message Forwarder"

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.webhook_id: return
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
            guild_cfg = self.config.get(str(message.guild.id), {})
            guild_cfg.pop(str(message.channel.id), None)
            save_config_forwarder(self.config)
        except Exception as e:
            log_forwarder.error(f"Failed to forward message via webhook: {e}")

    @app_commands.command(name="forwardset", description="Set a channel to be forwarded to a specific thread.")
    @app_commands.checks.has_permissions(administrator=True)
    async def forward_set(self, interaction: discord.Interaction, source_channel: discord.TextChannel, target_thread: discord.Thread):
        guild_id_str = str(interaction.guild_id)
        if guild_id_str not in self.config: self.config[guild_id_str] = {}
        
        try:
            webhook = await target_thread.create_webhook(name=f"Forwarder - {source_channel.name}")
        except discord.Forbidden:
            embed = discord.Embed(description="❌ I don't have permission to create webhooks in that thread.", color=discord.Color.red())
            embed.set_footer(text=self.get_footer_text())
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        self.config[guild_id_str][str(source_channel.id)] = {"thread_id": target_thread.id, "webhook_url": webhook.url}
        save_config_forwarder(self.config)
        
        embed = discord.Embed(description=f"✅ Forwarding enabled from {source_channel.mention} to **{target_thread.name}**.", color=EMBED_COLOR_FORWARDER)
        embed.set_footer(text=self.get_footer_text())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="forwardremove", description="Stop forwarding messages from a channel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def forward_remove(self, interaction: discord.Interaction, channel: discord.TextChannel):
        rule = self.config.get(str(interaction.guild_id), {}).pop(str(channel.id), None)
        
        if rule:
            if webhook_url := rule.get("webhook_url"):
                try:
                    await discord.Webhook.from_url(webhook_url, session=self.session).delete()
                except (discord.NotFound, discord.InvalidArgument): pass
            
            save_config_forwarder(self.config)
            embed = discord.Embed(description=f"✅ Forwarding has been disabled for {channel.mention}.", color=EMBED_COLOR_FORWARDER)
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(description=f"There was no forwarding rule set up for {channel.mention} to remove.", color=discord.Color.yellow())
            embed.set_footer(text=self.get_footer_text())
            await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(MessageForwarder(bot))