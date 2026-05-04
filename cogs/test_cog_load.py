import discord
from discord.ext import commands
from discord import app_commands
import asyncio

class EmojiStealer(commands.Cog):
    def is_bot_admin_check(interaction: discord.Interaction) -> bool:
        return True

    @app_commands.command(name="steal_emoji")
    @app_commands.check(is_bot_admin_check)
    async def steal_emoji(self, interaction: discord.Interaction, emoji: str, name: str = None):
        pass

print("Loaded successfully")
