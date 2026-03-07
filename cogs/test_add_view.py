import discord
from discord.ext import commands

class MyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Test1", custom_id="btn1")
    async def btn1(self, interaction, button): pass

    @discord.ui.button(label="Test2") # No custom ID
    async def btn2(self, interaction, button): pass

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

@bot.event
async def on_ready():
    try:
        bot.add_view(MyView())
        print("Success")
    except Exception as e:
        print("Error:", type(e).__name__, str(e))
    await bot.close()

bot.run("MTE0NTg3MjMzOTM1ODExOTMyNw.xxxxxxxxxxxxxxx") # Fake token, just test if it runs enough to fail
