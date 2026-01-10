import discord
from discord.ext import commands
from discord import app_commands

class Management(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="reload", description="Reloads a specific cog.")
    @app_commands.describe(cog_name="The name of the cog to reload (e.g., ticketing).")
    @commands.is_owner()
    async def reload_cog(self, interaction: discord.Interaction, cog_name: str):
        """
        A robust command to reload a cog.
        It handles cases where the cog is not loaded or fails to load.
        """
        full_cog_name = f"cogs.{cog_name.lower()}"
        
        try:
            # First, try to unload the cog if it's already loaded
            if full_cog_name in self.bot.extensions:
                await self.bot.unload_extension(full_cog_name)
            
            # Now, load the cog. This will also work if it was never loaded.
            await self.bot.load_extension(full_cog_name)
            
            await interaction.response.send_message(f"✅ Successfully reloaded the `{cog_name}` cog.", ephemeral=True)

        except commands.ExtensionNotFound:
            await interaction.response.send_message(f"❌ The cog `{cog_name}` was not found.", ephemeral=True)
        except Exception as e:
            # Send back the full error traceback for easy debugging
            error_message = f"❌ An error occurred while reloading `{cog_name}`:\n```py\n{e}\n```"
            await interaction.response.send_message(error_message, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(Management(bot))

