import discord
from discord.ext import commands

class BotPresence(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        """Sets the bot's presence once it has connected to Discord."""
        # This event is triggered when the bot has finished logging in and is ready.
        # It's the perfect place to set a static presence.
        activity = discord.Activity(type=discord.ActivityType.watching, name="Better Vibes")
        await self.bot.change_presence(status=discord.Status.online, activity=activity)
        print("Bot presence has been set to 'Watching Better Vibes'")

async def setup(bot: commands.Bot):
    await bot.add_cog(BotPresence(bot))

