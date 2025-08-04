import discord
from discord.ext import commands
from discord import app_commands
import json

class Diagnostics(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        print("Diagnostics Cog Loaded.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # --- This will print data for EVERY message the bot can see ---
        print("\n--- Diagnostic Log for Message ---")
        print(f"Timestamp: {message.created_at}")
        print(f"Channel: #{message.channel.name} ({message.channel.id})")
        print(f"Author: {message.author} ({message.author.id})")
        print(f"Message Type: {message.type}")
        print(f"Content: '{message.content}'")
        print(f"Number of Embeds: {len(message.embeds)}")
        if message.embeds:
            # to_dict() is a great way to see the raw data
            print(f"Embeds Data: {json.dumps([e.to_dict() for e in message.embeds], indent=2)}")
        print(f"Flags: {message.flags}")
        print("--- End Log ---\n")

    @app_commands.command(name="check_permissions", description="Checks the bot's actual permissions and intents.")
    async def check_permissions(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        intents = self.bot.intents
        permissions = interaction.channel.permissions_for(interaction.guild.me)

        embed = discord.Embed(title="Bot Diagnostics Report", color=0x00FF00)
        embed.add_field(
            name="Registered Intents", 
            inline=False, 
            value=f"Message Content Intent: **{intents.message_content}**\n"
                  f"Server Members Intent: **{intents.members}**\n"
                  f"Presence Intent: **{intents.presences}**"
        )
        embed.add_field(
            name=f"Permissions in #{interaction.channel.name}", 
            inline=False, 
            value=f"Can Read Messages: **{permissions.read_messages}**\n"
                  f"Can Read History: **{permissions.read_message_history}**\n"
                  f"Can Send Messages: **{permissions.send_messages}**"
        )
        await interaction.followup.send(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(Diagnostics(bot))