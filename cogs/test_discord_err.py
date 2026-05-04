import discord
from discord.ext import commands
from discord import app_commands

class TestCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="test")
    async def test(self, interaction: discord.Interaction):
        pass

    @test.error
    async def test_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        pass

print("Correctly initialized error handler")
