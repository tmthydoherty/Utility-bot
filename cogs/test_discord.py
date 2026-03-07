import discord

class MyView(discord.ui.View):
    @discord.ui.button(label="Test")
    async def my_btn(self, interaction, button):
        pass

    def __init__(self):
        super().__init__()
        print(type(self.my_btn))

MyView()
