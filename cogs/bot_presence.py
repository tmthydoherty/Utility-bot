import discord
from discord.ext import commands, tasks
import random

class BotPresence(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.change_status.start()

    def cog_unload(self):
        self.change_status.cancel()

    @tasks.loop(seconds=60)
    async def change_status(self):
        # This will only show a server count if the bot has the necessary intents
        server_count = len(self.bot.guilds)
        statuses = [
            "Type /help for commands",
            f"Watching over {server_count} server(s)",
            "Playing Daily Trivia!",
            "Picking the next map!"
        ]
        await self.bot.change_presence(activity=discord.Game(random.choice(statuses)))

    @change_status.before_loop
    async def before_change_status(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(BotPresence(bot))