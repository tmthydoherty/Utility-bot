import discord
from discord.ext import commands
from discord import app_commands


class MessageModal(discord.ui.Modal, title="Send a Message"):
    message = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.long,
        placeholder="Type your message here...",
        required=True,
        max_length=2000,
    )

    def __init__(self, channel: discord.abc.Messageable):
        super().__init__()
        self.channel = channel

    async def on_submit(self, interaction: discord.Interaction):
        await self.channel.send(self.message.value)
        name = getattr(self.channel, "mention", f"#{self.channel}")
        await interaction.response.send_message(
            f"✅ Message sent to {name}.", ephemeral=True
        )


class ChannelSelect(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=60)
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.news,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
            discord.ChannelType.forum,
        ],
        placeholder="Select a channel or thread...",
    )
    async def channel_callback(
        self, interaction: discord.Interaction, select: discord.ui.ChannelSelect
    ):
        channel = interaction.guild.get_channel_or_thread(select.values[0].id)
        if channel is None:
            channel = await interaction.guild.fetch_channel(select.values[0].id)
        await interaction.response.send_modal(MessageModal(channel))
        self.stop()


class SendMsg(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="sendmsg", description="Send a message as the bot to any channel.")
    @commands.is_owner()
    async def sendmsg(self, interaction: discord.Interaction):
        view = ChannelSelect(interaction.user.id)
        await interaction.response.send_message(
            "Pick a channel or thread:", view=view, ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(SendMsg(bot))
