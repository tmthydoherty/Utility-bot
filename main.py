import discord
from discord.ext import commands
import os
import asyncio
import logging
from dotenv import load_dotenv

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

# Load environment variables from .env file
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

class MyBot(commands.Bot):
    def __init__(self):
        # Define the intents your bot needs
        intents = discord.Intents.default()
        intents.message_content = True 
        intents.members = True
        intents.presences = True

        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        # This function is called when the bot logs in
        # It finds and loads all .py files in the 'cogs' directory
        for filename in os.listdir('./cogs'):
            if filename.endswith('.py') and not filename.startswith('__'):
                try:
                    await self.load_extension(f'cogs.{filename[:-3]}')
                    print(f"✅ Loaded Cog: {filename}")
                except Exception as e:
                    print(f"❌ Failed to load cog {filename}: {e}")
        
        # This initial sync is a good default, but the manual command is faster for testing
        await self.tree.sync()

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')
    
    # MODIFIED - More powerful manual sync command
    @commands.command()
    @commands.is_owner()
    async def sync(self, ctx: commands.Context, guild: str = None):
        """
        Manually syncs slash commands.
        Usage: !sync -> global sync
               !sync guild -> syncs to the current guild
        """
        if guild and guild.lower() == 'guild':
            synced = await self.tree.sync(guild=ctx.guild)
            await ctx.send(f"✅ Synced {len(synced)} commands to this guild.")
        else:
            synced = await self.tree.sync()
            await ctx.send(f"✅ Synced {len(synced)} commands globally.")

async def main():
    if not BOT_TOKEN:
        print("Error: DISCORD_BOT_TOKEN not found in .env file.")
        return
        
    bot = MyBot()
    await bot.start(BOT_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())