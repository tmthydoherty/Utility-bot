import aiohttp
import asyncio
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
        # V4 fix: per-user game selections instead of shared self.selected_game
        self._user_selections: dict[int, str] = {}

    def _get_selected_game(self, user_id: int) -> str | None:
        return self._user_selections.get(user_id)

    async def _check_admin(self, interaction: discord.Interaction) -> bool:
        """V1 fix: gate every callback behind an admin check."""
        if not self.bot.is_bot_admin(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to use this panel.", ephemeral=True
            )
            return False
        return True

    async def generate_embed(self, guild_id, user_id: int | None = None):
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

        selected = self._user_selections.get(user_id) if user_id else None
        if selected:
            embed.add_field(name="Currently Managing", value=f"**{GAMES[selected]['name']}**", inline=False)
            embed.color = GAMES[selected]['color']

        return embed

    # V2 fix: explicit custom_id on every component for persistent view survival
    @discord.ui.select(
        custom_id="patchnotes:game_select",
        placeholder="1. Select a Game to Manage",
        options=[discord.SelectOption(label=data["name"], value=key) for key, data in GAMES.items()],
        row=0
    )
    async def game_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        if not await self._check_admin(interaction):
            return
        self._user_selections[interaction.user.id] = select.values[0]
        embed = await self.generate_embed(interaction.guild_id, interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        custom_id="patchnotes:channel_select",
        channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread],
        placeholder="2. Set Channel (Auto-Saves)",
        row=1
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        if not await self._check_admin(interaction):
            return

        selected_game = self._get_selected_game(interaction.user.id)
        if not selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)

        # V7 fix: defer before DB write to avoid 3-second interaction timeout
        await interaction.response.defer()

        channel_id = select.values[0].id

        async with aiosqlite.connect("patchnotes.db") as db:
            await db.execute("""
                INSERT INTO configs (server_id, game, channel_id, is_enabled)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(server_id, game) DO UPDATE SET channel_id = ?, is_enabled = 1
            """, (interaction.guild_id, selected_game, channel_id, channel_id))
            await db.commit()

        embed = await self.generate_embed(interaction.guild_id, interaction.user.id)
        await interaction.message.edit(embed=embed, view=self)
        await interaction.followup.send(
            f"✅ Auto-posting enabled for **{GAMES[selected_game]['name']}** in <#{channel_id}>.",
            ephemeral=True
        )

    @discord.ui.button(label="Disable Game", style=discord.ButtonStyle.secondary, custom_id="patchnotes:disable", row=2)
    async def disable_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_admin(interaction):
            return

        selected_game = self._get_selected_game(interaction.user.id)
        if not selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)

        # V7 fix: defer before DB write
        await interaction.response.defer()

        async with aiosqlite.connect("patchnotes.db") as db:
            await db.execute("UPDATE configs SET is_enabled = 0 WHERE server_id = ? AND game = ?", (interaction.guild_id, selected_game))
            await db.commit()

        embed = await self.generate_embed(interaction.guild_id, interaction.user.id)
        await interaction.message.edit(embed=embed, view=self)
        await interaction.followup.send(
            f"❌ Auto-posting disabled for **{GAMES[selected_game]['name']}**.",
            ephemeral=True
        )

    @discord.ui.button(label="Test Alert (Ephemeral)", style=discord.ButtonStyle.blurple, custom_id="patchnotes:test", row=3)
    async def test_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_admin(interaction):
            return

        selected_game = self._get_selected_game(interaction.user.id)
        if not selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        # V8 fix: guard fetch calls against network failures
        try:
            patch_data = await self._fetch_patch(selected_game)
        except Exception:
            logger.exception(f"Failed to fetch patch for {selected_game}")
            return await interaction.followup.send("Could not fetch patch notes at this time.", ephemeral=True)

        if not patch_data:
            return await interaction.followup.send("Could not fetch patch notes at this time.", ephemeral=True)

        embed = self._build_embed(selected_game, patch_data, test=True)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Force Post & Pin", style=discord.ButtonStyle.danger, custom_id="patchnotes:force_post", row=3)
    async def force_post_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check_admin(interaction):
            return

        selected_game = self._get_selected_game(interaction.user.id)
        if not selected_game:
            return await interaction.response.send_message("Please select a game first!", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        # V3/V12 fix: per-game lock prevents races with patch_checker and double-clicks
        lock = self.bot._patchnotes_locks.setdefault(selected_game, asyncio.Lock())
        async with lock:
            async with aiosqlite.connect("patchnotes.db") as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM configs WHERE server_id = ? AND game = ?", (interaction.guild_id, selected_game)) as cursor:
                    config = await cursor.fetchone()

            if not config or not config['is_enabled']:
                return await interaction.followup.send("This game is not enabled. Please assign it a channel first.", ephemeral=True)

            channel = self.bot.get_channel(config['channel_id'])
            if not channel:
                return await interaction.followup.send("The configured channel could not be found.", ephemeral=True)

            # V8 fix: guard fetch calls
            try:
                patch_data = await self._fetch_patch(selected_game)
            except Exception:
                logger.exception(f"Failed to fetch patch for {selected_game}")
                return await interaction.followup.send("Could not fetch patch notes.", ephemeral=True)

            if not patch_data:
                return await interaction.followup.send("Could not fetch patch notes.", ephemeral=True)

            if config['last_message_id']:
                try:
                    old_msg = await channel.fetch_message(config['last_message_id'])
                    await old_msg.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass

            embed = self._build_embed(selected_game, patch_data)

            try:
                new_msg = await channel.send(embed=embed)
            except discord.Forbidden:
                return await interaction.followup.send("I don't have permission to send messages in that channel!", ephemeral=True)

            # V13 fix: update DB immediately after sending to minimize orphan window
            async with aiosqlite.connect("patchnotes.db") as db:
                await db.execute("""
                    UPDATE configs
                    SET last_patch_id = ?, last_message_id = ?
                    WHERE server_id = ? AND game = ?
                """, (patch_data['id'], new_msg.id, interaction.guild_id, selected_game))
                await db.commit()

            # Pinning is best-effort after message is tracked
            try:
                await new_msg.pin(reason="Forced Patch Notes")
                async for msg in channel.history(limit=5):
                    if msg.type == discord.MessageType.pins_add:
                        await msg.delete()
                        break
            except discord.Forbidden:
                pass

        await interaction.followup.send(f"✅ Successfully force-posted and pinned the latest patch notes to <#{channel.id}>!", ephemeral=True)

    @staticmethod
    async def _fetch_patch(game: str) -> dict | None:
        """Centralized fetch dispatcher. Caller must handle exceptions."""
        if game == "valorant":
            return await fetch_valorant_patch()
        elif game == "overwatch_2":
            return await fetch_overwatch_patch()
        elif "app_id" in GAMES[game]:
            return await fetch_steam_patch(GAMES[game]["app_id"])
        return None

    @staticmethod
    def _build_embed(game: str, patch_data: dict, test: bool = False) -> discord.Embed:
        """Centralized embed builder — eliminates 3x copy-paste."""
        content = re.sub(r'(?m)^#{4,}\s?', '### ', patch_data['content'])
        # V15 fix: hard-clamp to embed description limit
        if len(content) > 4096:
            content = content[:4090] + "\n[...]"

        embed = discord.Embed(
            title=patch_data['title'],
            url=patch_data['url'],
            description=content,
            color=GAMES[game]['color']
        )
        if patch_data.get('image'):
            embed.set_image(url=patch_data['image'])
        elif "banner" in GAMES[game]:
            embed.set_image(url=GAMES[game]['banner'])
        elif "app_id" in GAMES[game]:
            embed.set_image(url=f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{GAMES[game]['app_id']}/header.jpg")
        embed.set_footer(text=f"{GAMES[game]['name']} Updates{' (TEST PREVIEW)' if test else ''}")
        return embed


class Patchnotes(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # V3 fix: per-game locks shared between force_post_btn and patch_checker
        if not hasattr(bot, '_patchnotes_locks'):
            bot._patchnotes_locks = {}

    async def cog_load(self):
        # V6 fix: await DB setup directly instead of fire-and-forget create_task
        await self.setup_db()
        # V2 fix: register persistent view so panels survive bot restarts
        self.bot.add_view(PatchnotesPanel(self.bot))

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
        # V14 fix: guard against double-start on cog reload
        if not self.patch_checker.is_running():
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
        try:
            logger.info("Checking for new patch notes...")

            # V9 fix: read configs and release DB connection BEFORE HTTP fetches
            async with aiosqlite.connect("patchnotes.db") as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM configs WHERE is_enabled = 1") as cursor:
                    configs = await cursor.fetchall()

            for config in configs:
                game = config['game']
                try:
                    # V3 fix: per-game lock prevents races with force_post_btn
                    lock = self.bot._patchnotes_locks.setdefault(game, asyncio.Lock())
                    async with lock:
                        # V8/V11 fix: guarded fetch with full traceback
                        try:
                            patch_data = await PatchnotesPanel._fetch_patch(game)
                        except Exception:
                            logger.exception(f"Failed to fetch patch data for {game}")
                            continue

                        if not patch_data or patch_data['id'] == config['last_patch_id']:
                            continue

                        channel = self.bot.get_channel(config['channel_id'])
                        if not channel:
                            continue

                        # Delete old message
                        if config['last_message_id']:
                            try:
                                old_msg = await channel.fetch_message(config['last_message_id'])
                                await old_msg.delete()
                            except (discord.NotFound, discord.Forbidden, aiohttp.ClientError):
                                pass

                        embed = PatchnotesPanel._build_embed(game, patch_data)

                        # Send new Embed
                        try:
                            new_msg = await channel.send(embed=embed)
                        except discord.Forbidden:
                            logger.warning(f"Lacking permissions to send in {channel.name}")
                            continue

                        # V9/V13 fix: separate short-lived DB connection, update immediately after send
                        async with aiosqlite.connect("patchnotes.db") as db:
                            await db.execute("""
                                UPDATE configs
                                SET last_patch_id = ?, last_message_id = ?
                                WHERE server_id = ? AND game = ?
                            """, (patch_data['id'], new_msg.id, config['server_id'], game))
                            await db.commit()

                        # Pin is best-effort after message is tracked in DB
                        try:
                            await new_msg.pin(reason="New Patch Notes")
                            async for msg in channel.history(limit=5):
                                if msg.type == discord.MessageType.pins_add:
                                    await msg.delete()
                                    break
                        except discord.Forbidden:
                            logger.warning(f"Lacking permissions to pin in {channel.name}")

                except Exception:
                    # V11 fix: full traceback instead of str(e)
                    logger.exception(f"Unexpected error processing patches for {game}")
                    continue
        except Exception as e:
            await self.bot.error_reporter.report("Patchnotes", f"patch_checker: {e}")

    @patch_checker.before_loop
    async def before_patch_checker(self):
        await self.bot.wait_until_ready()

    # V5 fix: error handler prevents silent loop death after 3 consecutive failures
    @patch_checker.error
    async def patch_checker_error(self, error):
        logger.exception("patch_checker loop crashed — restarting in 60 seconds")
        await asyncio.sleep(60)
        if not self.patch_checker.is_running():
            self.patch_checker.start()

async def setup(bot):
    await bot.add_cog(Patchnotes(bot))
