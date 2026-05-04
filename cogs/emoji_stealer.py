import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import io

# Custom check that taps into the 'is_bot_admin' function defined in your main.py
def is_bot_admin_check(interaction: discord.Interaction) -> bool:
    if hasattr(interaction.client, 'is_bot_admin'):
        return interaction.client.is_bot_admin(interaction.user)
    return interaction.user.guild_permissions.administrator


def parse_emoji(text: str) -> discord.PartialEmoji | None:
    """Try to parse a custom emoji from a message string."""
    text = text.strip()
    try:
        partial = discord.PartialEmoji.from_str(text)
        if partial.is_unicode_emoji():
            return None
        return partial
    except Exception:
        return None


class EmojiPanelView(discord.ui.View):
    """Persistent view with Steal and Download buttons."""

    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def _wait_for_emoji(self, interaction: discord.Interaction, prompt: str):
        """Send an ephemeral prompt and wait for the user to send a custom emoji in chat."""
        await interaction.response.send_message(prompt, ephemeral=True)

        def check(m: discord.Message):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.bot.wait_for("message", check=check, timeout=30.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("⏰ Timed out. Please try again.", ephemeral=True)
            return None, None

        partial = parse_emoji(msg.content)
        if partial is None:
            await interaction.followup.send(
                "❌ That doesn't look like a valid custom emoji. Please try again.",
                ephemeral=True,
            )
        # Try to clean up the user's message
        try:
            await msg.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass

        return partial, msg

    @discord.ui.button(label="Steal Emoji", style=discord.ButtonStyle.primary, emoji="🔓", custom_id="emoji_panel:steal")
    async def steal_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_bot_admin_check(interaction):
            await interaction.response.send_message(
                "❌ You do not have permission to steal emojis.", ephemeral=True
            )
            return

        partial, msg = await self._wait_for_emoji(
            interaction,
            "Send the custom emoji you want to steal in chat. You have 30 seconds.\n"
            "*(Optionally send it as `<emoji> new_name` to rename it)*",
        )
        if partial is None:
            return

        # Check if a custom name was provided after the emoji
        parts = msg.content.strip().split()
        custom_name = parts[1] if len(parts) > 1 else None

        try:
            partial._state = self.bot._connection
            image_data = await partial.read()
            final_name = custom_name if custom_name else partial.name
            new_emoji = await interaction.guild.create_custom_emoji(
                name=final_name,
                image=image_data,
                reason=f"Emoji stolen by {interaction.user}",
            )
            await interaction.followup.send(
                f"✅ Successfully stole {new_emoji} and added it as `{new_emoji.name}`",
                ephemeral=True,
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ I do not have the 'Manage Expressions' permission to add emojis here.",
                ephemeral=True,
            )
        except discord.HTTPException as e:
            if e.code == 30008:
                await interaction.followup.send("❌ This server has reached its maximum emoji limit.", ephemeral=True)
            elif e.code == 50035:
                await interaction.followup.send("❌ The emoji file size is too big for Discord.", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ Failed to upload emoji: {e.text}", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ An unexpected error occurred: {e}", ephemeral=True)

    @discord.ui.button(label="Download Emoji", style=discord.ButtonStyle.secondary, emoji="📥", custom_id="emoji_panel:download")
    async def download_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        partial, msg = await self._wait_for_emoji(
            interaction,
            "Send the custom emoji you want to download in chat. You have 30 seconds.",
        )
        if partial is None:
            return

        try:
            partial._state = self.bot._connection
            image_data = await partial.read()
            ext = "gif" if partial.animated else "png"
            filename = f"{partial.name}.{ext}"
            file = discord.File(io.BytesIO(image_data), filename=filename)
            await interaction.followup.send(
                f"📥 Here's **{partial.name}** as a `.{ext}` file:",
                file=file,
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ An unexpected error occurred: {e}", ephemeral=True)


class EmojiStealer(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.add_view(EmojiPanelView(bot))

    @app_commands.command(name="emoji_stealer_panel", description="Open the Emoji Stealer panel with steal & download buttons.")
    @app_commands.guild_only()
    @app_commands.check(is_bot_admin_check)
    async def emoji_stealer_panel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="🎨 Emoji Stealer",
            description=(
                "**Steal Emoji** — Add a custom emoji from another server to this one.\n"
                "**Download Emoji** — Download any custom emoji as a PNG or GIF file."
            ),
            color=discord.Color.blurple(),
        )
        view = EmojiPanelView(self.bot)
        await interaction.response.send_message(embed=embed, view=view)

    @emoji_stealer_panel.error
    async def panel_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
        else:
            if not interaction.response.is_done():
                await interaction.response.send_message(f"❌ An error occurred: {error}", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ An error occurred: {error}", ephemeral=True)
            raise error


async def setup(bot):
    await bot.add_cog(EmojiStealer(bot))
