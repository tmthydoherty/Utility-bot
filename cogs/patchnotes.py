import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import logging
import re

# Updated import to match your new file name
from utils.helper_fetcher import fetch_valorant_patch, fetch_steam_patch, fetch_overwatch_patch

logger = logging.getLogger('bot_main')

GAMES = {
    "valorant": {"name": "Valorant", "color": discord.Color.red(), "banner": "https://playvalorant.com/assets/images/share.jpg"},
    "marvel_rivals": {"name": "Marvel Rivals", "app_id": "2767030", "color": discord.Color.blue(), "banner": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/2767030/capsule_616x353.jpg"},
    "arc_raiders": {"name": "Arc Raiders", "app_id": "1808500", "color": discord.Color.yellow()},
    "rainbow_6_siege": {"name": "Rainbow 6 Siege", "app_id": "359550", "color": discord.Color.dark_grey()},
    "overwatch_2": {"name": "Overwatch 2", "app_id": "2357570", "color": discord.Color.orange(), "banner": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/2357570/capsule_616x353.jpg"},
    "apex_legends": {"name": "Apex Legends", "app_id": "1172470", "color": discord.Color.red()}
}

class PatchnotesPanel(discord.ui.View):
    def __init__(self, bot):
        super().__init__(timeout=None)
        self.bot = bot
        self.selected_game = None
        
        # Initialize disabled state for buttons dependent on game selection
        for item in self.children:
            if isinstance(item, (discord.ui.ChannelSelect, discord.ui.Button)):
                item.disabled = True

    async def generate_embed(self, guild_id):
        embed = discord.Embed(
            title="🛠️ Patch Notes Dashboard",
            color=discord.Color.dark_theme()
        )
        
        async with aiosqlite.connect("patchnotes.db") as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM configs WHERE server_id = ?", (guild_id,)) as cursor:
                configs = await cursor.fetchall()
                
        config_map = {row['game']: row for row in configs}
        
        desc = "**Current Setup:**\n\n"
        for key, data in GAMES.items():
            conf = config_map.get(key)
            if conf and conf['is_enabled'] and conf['channel_id']:
                desc += f"✅ **{data['name']}**: <#{conf['channel_id']}>\n"
            else:
                desc += f"❌ **{data['name']}**: Disabled\n"
                
        desc += "\n**How to configure:**\n1️⃣ Select a game from the first dropdown.\n2️⃣ Select a channel from the second dropdown to automatically enable & save it."
        
        embed.description = desc
        
        if self.selected_game:
            embed.add_field(name="Currently Managing", value=f"**{GAMES[self.selected_game]['name']}**", inline=False)
            embed.color = GAMES[self.selected_game]['color']
            
        return embed

    async def refresh_view(self, interaction: discord.Interaction):
        # Enable controls if a game is selected
        for item in self.children:
            if isinstance(item, (discord.ui.ChannelSelect, discord.ui.Button)):
                item.disabled = self.selected_game is None
                
        embed = await self.generate_embed(interaction.guild_id)
        if not interaction.response.is_done():
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.message.edit(embed=embed, view=self)

    @discord.ui.select(
        placeholder="1. Select a Game to Manage",
        options=[discord.SelectOption(label=data["name"], value=key) for key, data in GAMES.items()],
        row=0
    )
    async def game_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.selected_game = select.values[0]
        await self.refresh_view(interaction)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread],
        placeholder="2. Set Channel (Auto-Saves)",
        row=1
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        if not self.selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)
            
        channel_id = select.values[0].id
        
        async with aiosqlite.connect("patchnotes.db") as db:
            await db.execute("""
                INSERT INTO configs (server_id, game, channel_id, is_enabled) 
                VALUES (?, ?, ?, 1)
                ON CONFLICT(server_id, game) DO UPDATE SET channel_id = ?, is_enabled = 1
            """, (interaction.guild_id, self.selected_game, channel_id, channel_id))
            await db.commit()
            
        await self.refresh_view(interaction)
        await interaction.followup.send(f"✅ Auto-posting enabled for **{GAMES[self.selected_game]['name']}** in <#{channel_id}>.", ephemeral=True)

    @discord.ui.button(label="Disable Game", style=discord.ButtonStyle.secondary, row=2)
    async def disable_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)

        async with aiosqlite.connect("patchnotes.db") as db:
            await db.execute("UPDATE configs SET is_enabled = 0 WHERE server_id = ? AND game = ?", (interaction.guild_id, self.selected_game))
            await db.commit()
            
        await self.refresh_view(interaction)
        await interaction.followup.send(f"❌ Auto-posting disabled for **{GAMES[self.selected_game]['name']}**.", ephemeral=True)

    @discord.ui.button(label="Test Alert (Ephemeral)", style=discord.ButtonStyle.blurple, row=3)
    async def test_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)
            
        await interaction.response.defer(ephemeral=True) 
        
        patch_data = None
        if self.selected_game == "valorant":
            patch_data = await fetch_valorant_patch()
        elif self.selected_game == "overwatch_2":
            patch_data = await fetch_overwatch_patch()
        elif "app_id" in GAMES[self.selected_game]:
            patch_data = await fetch_steam_patch(GAMES[self.selected_game]["app_id"])
            
        if not patch_data:
            return await interaction.followup.send("Could not fetch patch notes at this time.", ephemeral=True)
            
        embed = discord.Embed(
            title=patch_data['title'],
            url=patch_data['url'],
            description=re.sub(r'(?m)^#{4,}\s?', '### ', patch_data['content']),
            color=GAMES[self.selected_game]['color']
        )
        if patch_data.get('image'):
            embed.set_image(url=patch_data['image'])
        elif "banner" in GAMES[self.selected_game]:
            embed.set_image(url=GAMES[self.selected_game]['banner'])
        elif "app_id" in GAMES[self.selected_game]:
            embed.set_image(url=f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{GAMES[self.selected_game]['app_id']}/header.jpg")
        embed.set_footer(text=f"{GAMES[self.selected_game]['name']} Updates (TEST PREVIEW)")
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Force Post & Pin", style=discord.ButtonStyle.danger, row=3)
    async def force_post_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)
            
        await interaction.response.defer(ephemeral=True)
        
        async with aiosqlite.connect("patchnotes.db") as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM configs WHERE server_id = ? AND game = ?", (interaction.guild_id, self.selected_game)) as cursor:
                config = await cursor.fetchone()
                
        if not config or not config['is_enabled']:
            return await interaction.followup.send("This game is not enabled. Please assign it a channel first.", ephemeral=True)
            
        channel = self.bot.get_channel(config['channel_id'])
        if not channel:
            return await interaction.followup.send("The configured channel could not be found.", ephemeral=True)
            
        patch_data = None
        if self.selected_game == "valorant":
            patch_data = await fetch_valorant_patch()
        elif self.selected_game == "overwatch_2":
            patch_data = await fetch_overwatch_patch()
        elif "app_id" in GAMES[self.selected_game]:
            patch_data = await fetch_steam_patch(GAMES[self.selected_game]["app_id"])
            
        if not patch_data:
            return await interaction.followup.send("Could not fetch patch notes.", ephemeral=True)
            
        if config['last_message_id']:
            try:
                old_msg = await channel.fetch_message(config['last_message_id'])
                await old_msg.delete()
            except (discord.NotFound, discord.Forbidden):
                pass
                
        embed = discord.Embed(
            title=patch_data['title'],
            url=patch_data['url'],
            description=re.sub(r'(?m)^#{4,}\s?', '### ', patch_data['content']),
            color=GAMES[self.selected_game]['color']
        )
        if patch_data.get('image'):
            embed.set_image(url=patch_data['image'])
        elif "banner" in GAMES[self.selected_game]:
            embed.set_image(url=GAMES[self.selected_game]['banner'])
        elif "app_id" in GAMES[self.selected_game]:
            embed.set_image(url=f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{GAMES[self.selected_game]['app_id']}/header.jpg")
        embed.set_footer(text=f"{GAMES[self.selected_game]['name']} Updates")
        
        try:
            new_msg = await channel.send(embed=embed)
            await new_msg.pin(reason="Forced Patch Notes")
            
            async for msg in channel.history(limit=5):
                if msg.type == discord.MessageType.pins_add:
                    await msg.delete()
                    break
        except discord.Forbidden:
            return await interaction.followup.send("I don't have permission to send or pin messages in that channel!", ephemeral=True)
            
        async with aiosqlite.connect("patchnotes.db") as db:
            await db.execute("""
                UPDATE configs 
                SET last_patch_id = ?, last_message_id = ? 
                WHERE server_id = ? AND game = ?
            """, (patch_data['id'], new_msg.id, interaction.guild_id, self.selected_game))
            await db.commit()
            
        await interaction.followup.send(f"✅ Successfully force-posted and pinned the latest patch notes to <#{channel.id}>!", ephemeral=True)


class Patchnotes(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        self.bot.loop.create_task(self.setup_db())

    async def setup_db(self):
        """Creates the database table if it doesn't exist."""
        async with aiosqlite.connect("patchnotes.db") as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS configs (
                    server_id INTEGER,
                    game TEXT,
                    channel_id INTEGER,
                    last_patch_id TEXT,
                    last_message_id INTEGER,
                    is_enabled INTEGER DEFAULT 1,
                    PRIMARY KEY (server_id, game)
                )
            """)
            await db.commit()
        self.patch_checker.start()

    def cog_unload(self):
        self.patch_checker.cancel()

    @app_commands.command(name="patchnotes_panel", description="Admin dashboard for configuring auto-patchnotes.")
    @app_commands.default_permissions(manage_guild=True)
    async def patchnotes_panel(self, interaction: discord.Interaction):
        # Using your custom admin check from main.py
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("You do not have permission to use this panel.", ephemeral=True)
            
        view = PatchnotesPanel(self.bot)
        embed = await view.generate_embed(interaction.guild_id)
        await interaction.response.send_message(embed=embed, view=view)

    @tasks.loop(minutes=10)
    async def patch_checker(self):
        """Runs every 10 minutes to check for new patches and update Discord."""
        logger.info("Checking for new patch notes...")
        
        async with aiosqlite.connect("patchnotes.db") as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM configs WHERE is_enabled = 1") as cursor:
                configs = await cursor.fetchall()

            for config in configs:
                game = config['game']
                try:
                    patch_data = None

                    if game == "valorant":
                        patch_data = await fetch_valorant_patch()
                    elif game == "overwatch_2":
                        patch_data = await fetch_overwatch_patch()
                    elif "app_id" in GAMES[game]:
                        patch_data = await fetch_steam_patch(GAMES[game]["app_id"])

                    if patch_data and patch_data['id'] != config['last_patch_id']:
                        channel = self.bot.get_channel(config['channel_id'])
                        if not channel:
                            continue

                        # Delete old message
                        if config['last_message_id']:
                            try:
                                old_msg = await channel.fetch_message(config['last_message_id'])
                                await old_msg.delete()
                            except (discord.NotFound, discord.Forbidden):
                                pass

                        # Build new Embed
                        embed = discord.Embed(
                            title=patch_data['title'],
                            url=patch_data['url'],
                            description=re.sub(r'(?m)^#{4,}\s?', '### ', patch_data['content']),
                            color=GAMES[game]['color']
                        )
                        if patch_data.get('image'):
                            embed.set_image(url=patch_data['image'])
                        elif "banner" in GAMES[game]:
                            embed.set_image(url=GAMES[game]['banner'])
                        elif "app_id" in GAMES[game]:
                            embed.set_image(url=f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{GAMES[game]['app_id']}/header.jpg")
                        embed.set_footer(text=f"{GAMES[game]['name']} Updates")

                        # Send and Pin
                        try:
                            new_msg = await channel.send(embed=embed)
                            await new_msg.pin(reason="New Patch Notes")

                            async for msg in channel.history(limit=5):
                                if msg.type == discord.MessageType.pins_add:
                                    await msg.delete()
                                    break
                        except discord.Forbidden:
                            logger.warning(f"Lacking permissions to send/pin in {channel.name}")
                            continue

                        # Update Database
                        await db.execute("""
                            UPDATE configs
                            SET last_patch_id = ?, last_message_id = ?
                            WHERE server_id = ? AND game = ?
                        """, (patch_data['id'], new_msg.id, config['server_id'], game))
                        await db.commit()
                except Exception as e:
                    logger.error(f"Error checking patches for {game}: {e}")
                    continue

    @patch_checker.before_loop
    async def before_patch_checker(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Patchnotes(bot))
