# cogs/core.py

import discord
from discord.ext import commands

class CoreCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # The @commands.command() decorator was added here. This is the fix.
    @commands.command()
    @commands.is_owner()
    async def sync(self, ctx: commands.Context, guild: str = None):
        """
        Manually syncs slash commands.
        Usage: !sync -> global sync
               !sync guild -> syncs to the current guild
        """
        if guild and guild.lower() == 'guild':
            try:
                synced = await self.bot.tree.sync(guild=ctx.guild)
                await ctx.send(f"✅ Synced {len(synced)} commands to this guild.")
            except Exception as e:
                await ctx.send(f"❌ Failed to sync to guild: {e}")
        else:
            try:
                synced = await self.bot.tree.sync()
                await ctx.send(f"✅ Synced {len(synced)} commands globally.")
            except Exception as e:
                await ctx.send(f"❌ Failed to sync globally: {e}")

# This async function is required for the cog to be loaded
async def setup(bot: commands.Bot):
    await bot.add_cog(CoreCog(bot))

