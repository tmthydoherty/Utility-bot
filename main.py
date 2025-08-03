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
        intents.message_content = True # Needed for some commands/features
        intents.members = True         # Needed to get member data
        intents.presences = True       # Needed for status-based features

        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        # This function is called when the bot logs in
        # It finds and loads all .py files in the 'cogs' directory
        for filename in os.listdir('./cogs'):
            if filename.endswith('.py'):
                try:
                    await self.load_extension(f'cogs.{filename[:-3]}')
                    print(f"✅ Loaded Cog: {filename}")
                except Exception as e:
                    print(f"❌ Failed to load cog {filename}: {e}")
        
        # Sync slash commands to Discord
        await self.tree.sync()

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')

async def main():
    if not BOT_TOKEN:
        print("Error: DISCORD_BOT_TOKEN not found in .env file.")
        return
        
    bot = MyBot()
    await bot.start(BOT_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())