import discord
from discord.ext import commands
import os
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler

from utils.error_reporter import ErrorReporter

# --- LOGGING SETUP ---
# Stream handlers alone meant every log line went to whatever terminal launched
# the bot — an SSH session in practice — and vanished with it. Diagnostics the
# cogs already emit (balancer fallbacks, role-sync failures, view errors) were
# unrecoverable after the fact. Everything now also lands in a rotating file.
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "bot.log"

_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _build_handlers():
    """A stream handler (for journald) plus a rotating file (for after the fact)."""
    stream = logging.StreamHandler()
    stream.setFormatter(_formatter)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(_formatter)
    return [stream, file_handler]


logger = logging.getLogger('bot_main')
if not logger.handlers:
    for _h in _build_handlers():
        logger.addHandler(_h)
    logger.setLevel(logging.INFO)
    # These loggers carry their own handlers, so let them stop here rather than
    # bubbling to the root handlers below and writing every line twice.
    logger.propagate = False

    # Cog modules log under 'cogs.<name>' and discord.py swallows UI/view
    # exceptions into its own 'discord.*' loggers — neither had a handler,
    # so those errors were invisible. Surface both.
    for _name, _level in (('cogs', logging.INFO), ('discord', logging.WARNING)):
        _lg = logging.getLogger(_name)
        if not _lg.handlers:
            for _h in _build_handlers():
                _lg.addHandler(_h)
            _lg.setLevel(_level)
            _lg.propagate = False

    # Anything that reaches the root logger unhandled (third-party libraries,
    # bare `logging.error(...)` calls) would otherwise be dropped entirely.
    _root = logging.getLogger()
    if not _root.handlers:
        for _h in _build_handlers():
            _root.addHandler(_h)
        _root.setLevel(logging.WARNING)

    logger.info(f"Logging to {LOG_FILE} (5MB x 5 rotation)")

# --- BOT SETUP ---
ADMIN_ROLE_ID = 1431565435819528302  # Role treated as admin by the bot

class Vibey(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        intents.invites = True
        intents.voice_states = True  # <--- CRITICAL: Needed to track voice activity
        intents.moderation = True    # Needed for on_audit_log_entry_create (anti-nuke)

        super().__init__(command_prefix="!", intents=intents)

    def is_bot_admin(self, member: discord.Member) -> bool:
        """Check if a member is considered a bot admin (has admin perms OR the admin role)."""
        if member.guild_permissions.administrator:
            return True
        return any(role.id == ADMIN_ROLE_ID for role in member.roles)

    async def setup_hook(self):
        """This is called when the bot is starting up (before on_ready)."""
        logger.info("Setting up the bot...")

        self.error_reporter = ErrorReporter(self, flush_interval=300)
        self.error_reporter.start()

        cogs_folder = "cogs"
        if not os.path.exists(cogs_folder):
            os.makedirs(cogs_folder)
            logger.warning(f"Created '{cogs_folder}' directory. Please add your cogs there.")
            return

        for filename in os.listdir(cogs_folder):
            # Load .py cog files (skip __init__, _shared modules, etc.)
            if filename.endswith(".py") and not filename.startswith("__") and not filename.endswith("_shared.py") and not filename.endswith("_fetcher.py"):
                cog_name = f"{cogs_folder}.{filename[:-3]}"
                try:
                    await self.load_extension(cog_name)
                    logger.info(f"✅ Successfully loaded cog: {cog_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to load cog {cog_name}. Error: {e}", exc_info=True)
            # Load cog packages (directories with __init__.py)
            elif os.path.isdir(os.path.join(cogs_folder, filename)) and not filename.startswith("__"):
                init_path = os.path.join(cogs_folder, filename, "__init__.py")
                if os.path.exists(init_path):
                    cog_name = f"{cogs_folder}.{filename}"
                    try:
                        await self.load_extension(cog_name)
                        logger.info(f"✅ Successfully loaded cog package: {cog_name}")
                    except Exception as e:
                        logger.error(f"❌ Failed to load cog package {cog_name}. Error: {e}", exc_info=True)

    async def on_ready(self):
        """This is called when the bot has successfully connected to Discord."""
        logger.info("=" * 50)
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info("Bot is ready. Syncing slash commands globally...")

        # --- CHANGE: Switched to Global Sync ---
        # This will sync all commands to all servers the bot is in.
        # Note: Global commands can take up to an hour to propagate.
        try:
            synced = await self.tree.sync()
            logger.info(f"✅ Synced {len(synced)} commands globally.")
            # Log each command for debugging
            for cmd in synced:
                logger.info(f"   - /{cmd.name}: {cmd.description}")
        except Exception as e:
            logger.error(f"❌ Failed to sync commands globally: {e}", exc_info=True)

        logger.info("=" * 50)

# --- COMMAND TOOLKIT ---
# Note: sync command moved to cogs/core.py

@commands.command(name="debug")
@commands.guild_only()
@commands.is_owner()
async def debug(ctx: commands.Context):
    """Shows diagnostic information about loaded cogs and commands."""
    loaded_cogs = list(ctx.bot.cogs.keys())
    cogs_text = "\n".join(f"- `{cog}`" for cog in loaded_cogs) if loaded_cogs else "None"
    global_commands = await ctx.bot.tree.fetch_commands()
    global_text = "\n".join(f"- `/{cmd.name}`" for cmd in global_commands) if global_commands else "None"
    guild_commands = await ctx.bot.tree.fetch_commands(guild=ctx.guild)
    guild_text = "\n".join(f"- `/{cmd.name}`" for cmd in guild_commands) if guild_commands else "None"
    
    embed = discord.Embed(title="Bot Diagnostics", color=discord.Color.orange())
    embed.add_field(name="✅ Loaded Cogs", value=cogs_text, inline=False)
    embed.add_field(name="🌍 Global Slash Commands", value=global_text, inline=False)
    embed.add_field(name="🏠 This Server's Slash Commands", value=guild_text, inline=False)
    await ctx.send(embed=embed)

# --- MAIN ENTRY ---
async def main():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    TOKEN = os.getenv('DISCORD_TOKEN')
    if TOKEN is None:
        logger.error("❌ Error: Bot token not found in .env file.")
        return

    bot = Vibey()
    bot.add_command(debug)

    try:
        await bot.start(TOKEN)
    except Exception as e:
        logger.error(f"❌ Fatal error starting bot: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())


