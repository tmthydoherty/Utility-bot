from discord.ext import commands

from .cog import Utility


async def setup(bot: commands.Bot):
    await bot.add_cog(Utility(bot))
