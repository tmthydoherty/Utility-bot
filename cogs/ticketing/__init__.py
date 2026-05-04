from .cog import TicketSystem


async def setup(bot):
    cog = TicketSystem(bot)
    await bot.add_cog(cog)
