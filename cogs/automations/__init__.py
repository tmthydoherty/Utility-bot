from discord.ext import commands

from .cog import Automations


async def setup(bot: commands.Bot):
    await bot.add_cog(Automations(bot))
