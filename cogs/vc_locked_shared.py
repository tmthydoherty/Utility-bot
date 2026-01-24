import discord
from discord.ext import commands
import aiosqlite
import json
import asyncio
import logging
import time

# --- CONSTANTS ---
DB_FILE = "vc_data.db"
TRIGGER_NAME = "➕ Join to create locked vc"  # Locked VCs trigger
TRIGGER_NAME_BASIC = "➕ Join to create vc"   # Basic VCs trigger
BANNED_WORDS = ["badword1", "badword2", "naughty"] 

# --- LOGGING SETUP ---
logger = logging.getLogger('VC_Cog')
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- DATABASE SETUP & HELPERS ---
DB_SEMAPHORE = asyncio.Semaphore(10)

# FIX: Connection pool for better performance
_db_pool = None
_db_pool_lock = asyncio.Lock()

async def get_db_connection():
    """Get a database connection with proper settings"""
    conn = await aiosqlite.connect(DB_FILE)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA busy_timeout=5000")
    return conn

async def init_db():
    logger.info("Initializing database...")
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=5000")
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS active_vcs (
                    vc_id INTEGER PRIMARY KEY,
                    owner_id INTEGER NOT NULL,
                    message_id INTEGER,
                    knock_mgmt_msg_id INTEGER,
                    thread_id INTEGER,
                    ghost INTEGER DEFAULT 0,
                    unlocked INTEGER DEFAULT 0,
                    bans TEXT DEFAULT '[]',
                    mute_knock_pings INTEGER DEFAULT 0,
                    guild_id INTEGER,
                    is_basic INTEGER DEFAULT 0,
                    last_seen_occupied REAL DEFAULT 0,
                    created_at REAL DEFAULT 0
                )
            ''')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_owner ON active_vcs(owner_id)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_guild ON active_vcs(guild_id)')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS presets (
                    user_id INTEGER,
                    preset_name TEXT,
                    data TEXT,
                    PRIMARY KEY (user_id, preset_name)
                )
            ''')
            
            # Migrations
            try:
                async with db.execute("PRAGMA table_info(active_vcs)") as cursor:
                    columns = [row[1] for row in await cursor.fetchall()]
                    if 'thread_id' not in columns:
                        logger.info("Migrating DB: Adding thread_id column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN thread_id INTEGER")
                    if 'knock_mgmt_msg_id' not in columns:
                        logger.info("Migrating DB: Adding knock_mgmt_msg_id column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN knock_mgmt_msg_id INTEGER")
                    if 'mute_knock_pings' not in columns:
                        logger.info("Migrating DB: Adding mute_knock_pings column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN mute_knock_pings INTEGER DEFAULT 0")
                    if 'guild_id' not in columns:
                        logger.info("Migrating DB: Adding guild_id column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN guild_id INTEGER")
                    if 'is_basic' not in columns:
                        logger.info("Migrating DB: Adding is_basic column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN is_basic INTEGER DEFAULT 0")
                    if 'last_seen_occupied' not in columns:
                        logger.info("Migrating DB: Adding last_seen_occupied column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN last_seen_occupied REAL DEFAULT 0")
                    if 'created_at' not in columns:
                        logger.info("Migrating DB: Adding created_at column...")
                        await db.execute("ALTER TABLE active_vcs ADD COLUMN created_at REAL DEFAULT 0")
            except Exception as e:
                logger.error(f"Migration failed: {e}")

            await db.commit()
            logger.info("Database initialization complete")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

async def get_config(key, default=None):
    try:
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else default
    except Exception as e:
        logger.error(f"Failed to get config {key}: {e}")
        return default

async def set_config(key, value):
    try:
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value) if value is not None else None))
                await db.commit()
    except Exception as e:
        logger.error(f"Failed to set config {key}: {e}")
        raise

async def load_active_vcs():
    logger.info("Loading active VCs from database...")
    try:
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute("SELECT vc_id, owner_id, message_id, knock_mgmt_msg_id, thread_id, ghost, unlocked, bans, mute_knock_pings, guild_id, is_basic, last_seen_occupied, created_at FROM active_vcs") as cursor:
                    rows = await cursor.fetchall()
                    result = {}
                    corrupted = []
                    for row in rows:
                        try:
                            # FIX: Better validation of each field
                            bans_data = row[7]
                            if bans_data:
                                try:
                                    bans_list = json.loads(bans_data)
                                    if not isinstance(bans_list, list):
                                        bans_list = []
                                    # Ensure all bans are integers
                                    bans_list = [int(b) for b in bans_list if isinstance(b, (int, str)) and str(b).isdigit()]
                                except (json.JSONDecodeError, TypeError):
                                    bans_list = []
                            else:
                                bans_list = []

                            result[row[0]] = {
                                'owner_id': int(row[1]) if row[1] else 0,
                                'message_id': int(row[2]) if row[2] else None,
                                'knock_mgmt_msg_id': int(row[3]) if row[3] else None,
                                'thread_id': int(row[4]) if row[4] else None,
                                'ghost': bool(row[5]),
                                'unlocked': bool(row[6]),
                                'bans': bans_list,
                                'mute_knock_pings': bool(row[8]) if len(row) > 8 else False,
                                'guild_id': int(row[9]) if len(row) > 9 and row[9] else None,
                                'is_basic': bool(row[10]) if len(row) > 10 else False,
                                'last_seen_occupied': float(row[11]) if len(row) > 11 and row[11] else time.time(),
                                'created_at': float(row[12]) if len(row) > 12 and row[12] else time.time()
                            }
                        except (json.JSONDecodeError, TypeError, ValueError) as e:
                            logger.error(f"Corrupted data for VC {row[0]}: {e}")
                            corrupted.append(row[0])

                    if corrupted:
                        await db.executemany("DELETE FROM active_vcs WHERE vc_id = ?", [(vid,) for vid in corrupted])
                        await db.commit()
                        logger.warning(f"Removed {len(corrupted)} corrupted VC records")

                    logger.info(f"Loaded {len(result)} active VCs")
                    return result
    except Exception as e:
        logger.error(f"Failed to load active VCs: {e}")
        return {}

async def save_multiple_vcs(vcs_dict):
    logger.debug(f"Saving {len(vcs_dict)} VCs to database...")
    try:
        data_list = []
        for vc_id, data in vcs_dict.items():
            # FIX: Validate data before saving
            try:
                bans = data.get('bans', [])
                if not isinstance(bans, list):
                    bans = []
                bans_json = json.dumps([int(b) for b in bans if isinstance(b, (int, str)) and str(b).isdigit()])
            except (TypeError, ValueError):
                bans_json = '[]'
            
            data_list.append((
                int(vc_id),
                int(data['owner_id']),
                int(data['message_id']) if data.get('message_id') else None,
                int(data['knock_mgmt_msg_id']) if data.get('knock_mgmt_msg_id') else None,
                int(data['thread_id']) if data.get('thread_id') else None,
                int(data.get('ghost', False)),
                int(data.get('unlocked', False)),
                bans_json,
                int(data.get('mute_knock_pings', False)),
                int(data['guild_id']) if data.get('guild_id') else None,
                int(data.get('is_basic', False))
            ))
        if not data_list: 
            return
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.executemany('''
                    INSERT OR REPLACE INTO active_vcs (vc_id, owner_id, message_id, knock_mgmt_msg_id, thread_id, ghost, unlocked, bans, mute_knock_pings, guild_id, is_basic)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_list)
                await db.commit()
    except Exception as e:
        logger.error(f"Failed to save VCs: {e}")
        raise

async def delete_vc_data(vc_id):
    logger.debug(f"Deleting VC data for {vc_id}")
    try:
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("DELETE FROM active_vcs WHERE vc_id = ?", (vc_id,))
                await db.commit()
    except Exception as e:
        logger.error(f"Failed to delete VC data for {vc_id}: {e}")
        raise

async def get_user_presets(user_id):
    try:
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute("SELECT preset_name, data FROM presets WHERE user_id = ?", (user_id,)) as cursor:
                    rows = await cursor.fetchall()
                    result = {}
                    corrupted = []
                    for row in rows:
                        try:
                            data = json.loads(row[1])
                            # FIX: Validate preset structure
                            if isinstance(data, dict):
                                result[row[0]] = data
                            else:
                                corrupted.append((user_id, row[0]))
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.error(f"Corrupted preset '{row[0]}' for user {user_id}: {e}")
                            corrupted.append((user_id, row[0]))
                    if corrupted:
                        await db.executemany("DELETE FROM presets WHERE user_id = ? AND preset_name = ?", corrupted)
                        await db.commit()
                    return result
    except Exception as e:
        logger.error(f"Failed to load presets for user {user_id}: {e}")
        return {}

async def save_preset(user_id, preset_name, data):
    # FIX: Better validation
    if not preset_name or not isinstance(preset_name, str):
        raise ValueError("Preset name must be a non-empty string")
    if len(preset_name) > 50: 
        raise ValueError("Preset name length 1-50 chars")
    # Allow alphanumeric, underscore, hyphen, and spaces
    if not all(c.isalnum() or c in '_- ' for c in preset_name): 
        raise ValueError("Invalid chars in preset name")
    if not isinstance(data, dict):
        raise ValueError("Preset data must be a dictionary")
    
    try: 
        json_data = json.dumps(data)
    except (TypeError, ValueError) as e: 
        raise ValueError(f"Preset data not serializable: {e}")
    
    async with DB_SEMAPHORE:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT COUNT(*) FROM presets WHERE user_id = ?", (user_id,)) as cursor:
                count = (await cursor.fetchone())[0]
                if count >= 10: 
                    raise ValueError("Max 10 presets")
            await db.execute("INSERT OR REPLACE INTO presets (user_id, preset_name, data) VALUES (?, ?, ?)", (user_id, preset_name, json_data))
            await db.commit()

async def delete_preset(user_id, preset_name):
    try:
        async with DB_SEMAPHORE:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("DELETE FROM presets WHERE user_id = ? AND preset_name = ?", (user_id, preset_name))
                await db.commit()
    except Exception as e:
        logger.error(f"Failed to delete preset '{preset_name}' for user {user_id}: {e}")
        raise

# --- UTILS ---

def contains_banned_word(text):
    if not text: 
        return False
    normalized = ''.join(c.lower() for c in text if c.isalnum())
    for word in BANNED_WORDS:
        normalized_word = ''.join(c.lower() for c in word if c.isalnum())
        if normalized_word in normalized: 
            return True
    return False

def sanitize_name(name, user_id=None):
    if not name:
        return f"user-{str(user_id)[-4:]}" if user_id else "user"
    clean = "".join(c for c in name if c.isalnum() or c in "-_").lower()
    clean = clean.strip('-_')
    if clean and len(clean) > 0: 
        return clean[:20]
    else: 
        return f"user-{str(user_id)[-4:]}" if user_id else "user"

def create_knock_management_embed(owner, pending_knocks, guild, vc_data=None):
    """Create the knock management embed for the private thread"""
    embed = discord.Embed(
        title="⚙️ VC Control Panel",
        color=discord.Color.blue()
    )
    
    if pending_knocks:
        knock_list = []
        for user_id in pending_knocks[:5]:  # Show max 5
            member = guild.get_member(user_id)
            if member:
                knock_list.append(f"• {member.mention}")
            else:
                knock_list.append(f"• User {user_id} (left server)")
        
        if len(pending_knocks) > 5:
            knock_list.append(f"• *...and {len(pending_knocks) - 5} more*")
        
        embed.description = f"**🔔 Knock Requests ({len(pending_knocks)}):**\n" + "\n".join(knock_list)
    else:
        if vc_data:
            lock_status = "🔒 **LOCKED**" if not vc_data.get('unlocked', False) else "🔓 **UNLOCKED**"
            ghost_status = "**ON**" if vc_data.get('ghost', False) else "**OFF**"
            
            embed.description = (
                f"- **Lock status:** {lock_status}\n"
                f"*(Unlocking your vc opens it up to the public for anyone to join)*\n\n"
                f"- **Ghost mode:** {ghost_status}\n"
                f"*(Enabling ghost mode will keep your vc locked but remove the knock ability from the public)*\n\n"
                f"- To manually add users to your vc either @ them here or add them to the VIP list with the settings menu below"
            )
        else:
            embed.description = (
                f"Welcome {owner.mention}!\n\n"
                "**Quick VIP Add:** @ mention users here to grant them access!"
            )
    
    return embed

class MockMessage:
    def __init__(self, author):
        self.author = author
        self.guild = getattr(author, 'guild', None)
        self.channel = None
        self.created_at = discord.utils.utcnow()
        self.id = 0
        self.content = ""
        self.mentions = []
        self.attachments = []
        self.embeds = []

# --- HELPER FUNCTIONS ---

def get_cog_safe(bot):
    """Safely get the VC cog reference"""
    cog = bot.get_cog("VC")
    if not cog:
        logger.warning("VC cog not loaded")
    return cog

async def delete_thread_safe(bot, thread_id):
    """Safely delete a thread - never archive"""
    if not thread_id:
        return
    try:
        thread = bot.get_channel(thread_id)
        if thread:
            await thread.delete()
            logger.debug(f"Deleted thread {thread_id}")
    except discord.NotFound:
        logger.debug(f"Thread {thread_id} already deleted")
    except Exception as e:
        logger.error(f"Failed to delete thread {thread_id}: {e}")

async def check_thread_valid(bot, thread_id):
    """Check if thread is valid and not archived. If archived, delete it."""
    if not thread_id:
        return None

    thread = bot.get_channel(thread_id)
    if not thread:
        logger.debug(f"Thread {thread_id} not found in cache")
        return None

    # Verify it's actually a thread
    if not isinstance(thread, discord.Thread):
        logger.warning(f"Channel {thread_id} is not a thread (type: {type(thread).__name__})")
        return None

    if thread.archived:
        # Thread is archived - delete it instead of unarchiving
        logger.info(f"Thread {thread_id} is archived, deleting")
        await delete_thread_safe(bot, thread_id)
        return None

    return thread

async def validate_vc_channel(bot, vc_id):
    """Validate a VC channel exists and is a voice channel"""
    if not vc_id:
        return None

    channel = bot.get_channel(vc_id)
    if not channel:
        return None

    if not isinstance(channel, discord.VoiceChannel):
        logger.warning(f"Channel {vc_id} is not a voice channel (type: {type(channel).__name__})")
        return None

    return channel

async def validate_member(guild, user_id):
    """Validate a user is a member of the guild"""
    if not guild or not user_id:
        return None

    member = guild.get_member(user_id)
    if not member:
        logger.debug(f"User {user_id} is not a member of guild {guild.id}")
        return None

    return member

async def safe_message_fetch(channel, message_id):
    """Safely fetch a message, returning None if not found"""
    if not channel or not message_id:
        return None

    try:
        return await channel.fetch_message(message_id)
    except discord.NotFound:
        logger.debug(f"Message {message_id} not found in channel {channel.id}")
        return None
    except discord.Forbidden:
        logger.warning(f"No permission to fetch message {message_id} in channel {channel.id}")
        return None
    except Exception as e:
        logger.error(f"Error fetching message {message_id}: {e}")
        return None

async def verify_channel_exists(bot, guild, channel_id):
    """
    Fetch channel from Discord API (not cache) to verify it exists.
    This is the core of "Verify Before Destroy" - we never trust cache for cleanup decisions.

    Returns:
        (channel, member_count) - Channel exists with N members
        (None, 0) - Channel confirmed deleted (NotFound)
        ("FORBIDDEN", -1) - Can't access but exists, don't cleanup
        ("ERROR", -1) - API error, don't cleanup to be safe
    """
    if not guild or not channel_id:
        logger.warning(f"VERIFY: Missing guild or channel_id for verification")
        return "ERROR", -1

    try:
        channel = await guild.fetch_channel(channel_id)
        if isinstance(channel, discord.VoiceChannel):
            member_count = len(channel.members)
            logger.debug(f"VERIFY: Channel {channel_id} exists with {member_count} members")
            return channel, member_count
        else:
            logger.warning(f"VERIFY: Channel {channel_id} exists but is not a VoiceChannel (type: {type(channel).__name__})")
            return None, 0
    except discord.NotFound:
        logger.debug(f"VERIFY: Channel {channel_id} confirmed deleted (NotFound)")
        return None, 0
    except discord.Forbidden:
        logger.warning(f"VERIFY: Cannot access channel {channel_id} (Forbidden) - assuming exists, aborting cleanup")
        return "FORBIDDEN", -1
    except Exception as e:
        logger.error(f"VERIFY: Error fetching channel {channel_id}: {e} - aborting cleanup to be safe")
        return "ERROR", -1

# --- MODALS ---

class InfoContentModal(discord.ui.Modal, title="Set Info Button Content"):
    content = discord.ui.TextInput(label="Info Message Content", style=discord.TextStyle.paragraph, placeholder="Enter text...", default="Locked VCs allow you to create...", max_length=2000)
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
    
    async def on_submit(self, interaction: discord.Interaction):
        if contains_banned_word(self.content.value): 
            return await interaction.response.send_message("❌ Content contains banned words.", ephemeral=True)
        try:
            await set_config(f'info_content_{interaction.guild_id}', self.content.value)
            await interaction.response.send_message("✅ Content updated!", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to save info content: {e}")
            await interaction.response.send_message("❌ Failed to save content. Please try again.", ephemeral=True)

class IdleNameModal(discord.ui.Modal, title="Set Idle Channel Name"):
    name = discord.ui.TextInput(label="Channel Name (0 Active VCs)", placeholder="e.g. 💤-locked-vcs-idle", default="locked-vcs-idle", max_length=100)
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
    
    async def on_submit(self, interaction: discord.Interaction):
        if contains_banned_word(self.name.value): 
            return await interaction.response.send_message("❌ Name contains banned words.", ephemeral=True)
        try:
            await set_config(f'idle_name_{interaction.guild_id}', self.name.value)
            await interaction.response.send_message(f"✅ Idle name set to: **{self.name.value}**", ephemeral=True)
            cog = get_cog_safe(self.bot)
            if cog: 
                await cog.update_hub_name(interaction.guild, force=True)
        except Exception as e:
            logger.error(f"Failed to set idle name: {e}")
            await interaction.response.send_message("❌ Failed to save name.", ephemeral=True)

class RulesEmbedModal(discord.ui.Modal, title="Post Rules Embed"):
    title_input = discord.ui.TextInput(label="Embed Title", placeholder="How to use Locked VCs...", max_length=256)
    description_input = discord.ui.TextInput(label="Embed Description", style=discord.TextStyle.paragraph, placeholder="1. Join...", max_length=4000)
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
    
    async def on_submit(self, interaction: discord.Interaction):
        hub_id = await get_config(f'hub_channel_id_{interaction.guild_id}')
        if not hub_id: 
            return await interaction.response.send_message("❌ Hub channel not set.", ephemeral=True)
        
        hub_channel = interaction.guild.get_channel(int(hub_id))
        if not hub_channel: 
            return await interaction.response.send_message("❌ Hub channel not found.", ephemeral=True)
        
        embed = discord.Embed(
            title=self.title_input.value,
            description=self.description_input.value,
            color=discord.Color.blue()
        )
        
        try:
            await hub_channel.send(embed=embed, view=RulesView(self.bot))
            await interaction.response.send_message("✅ Rules embed posted!", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to post rules: {e}")
            await interaction.response.send_message("❌ Failed to post rules embed.", ephemeral=True)

class ExclusionsModal(discord.ui.Modal, title="Set VC Exclusions"):
    exclusions = discord.ui.TextInput(
        label="Excluded VC Names (comma-separated)",
        style=discord.TextStyle.paragraph,
        placeholder="e.g. General, Music, AFK",
        required=False,
        max_length=1000
    )

    def __init__(self, bot):
        super().__init__()
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Clean and validate the input
            raw_names = self.exclusions.value.strip()
            if raw_names:
                # Split by comma, clean each name
                names = [n.strip() for n in raw_names.split(",") if n.strip()]
                # Store as comma-separated string
                value = ",".join(names)
            else:
                value = ""

            await set_config(f'excluded_vc_names_{interaction.guild_id}', value)

            if value:
                await interaction.response.send_message(
                    f"✅ Exclusions set! The following VC names will be ignored:\n`{value}`",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "✅ Exclusions cleared. No VCs will be excluded.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Failed to save exclusions: {e}")
            await interaction.response.send_message("❌ Failed to save exclusions.", ephemeral=True)


class SavePresetModal(discord.ui.Modal, title="Save Preset"):
    preset_name = discord.ui.TextInput(label="Preset Name", placeholder="Enter a name...", max_length=50)
    def __init__(self, vc, bans):
        super().__init__()
        self.vc = vc
        self.bans = bans
    
    async def on_submit(self, interaction: discord.Interaction):
        name = self.preset_name.value.strip()
        if not name: 
            return await interaction.response.send_message("❌ Name required.", ephemeral=True)
        if contains_banned_word(name): 
            return await interaction.response.send_message("❌ Name contains banned words.", ephemeral=True)
        try:
            data = {
                "name": self.vc.name,
                "limit": self.vc.user_limit,
                "bitrate": self.vc.bitrate,
                "bans": [int(b) for b in self.bans if isinstance(b, (int, str)) and str(b).isdigit()]
            }
            await save_preset(interaction.user.id, name, data)
            await interaction.response.send_message(f"✅ Preset **{name}** saved!", ephemeral=True)
        except ValueError as e: 
            await interaction.response.send_message(f"❌ {str(e)}", ephemeral=True)
        except Exception as e:
            logger.error(f"Preset save error: {e}")
            await interaction.response.send_message("❌ Failed to save preset.", ephemeral=True)

# --- VIEWS ---

class AdminPanelView(discord.ui.View):
    def __init__(self, bot):
        super().__init__(timeout=None)
        self.bot = bot
    
    @discord.ui.button(label="Set Category", style=discord.ButtonStyle.primary, emoji="📁", custom_id="admin_set_category")
    async def set_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        categories = [c for c in interaction.guild.categories if c.permissions_for(interaction.guild.me).manage_channels]
        if not categories: 
            return await interaction.response.send_message("❌ No categories with manage permissions.", ephemeral=True)
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in categories[:25]]
        
        async def callback(inter):
            cat_id = int(select.values[0])
            # FIX: Validate category still exists
            category = inter.guild.get_channel(cat_id)
            if not category:
                return await inter.response.send_message("❌ Category no longer exists.", ephemeral=True)
            if not category.permissions_for(inter.guild.me).manage_channels:
                return await inter.response.send_message("❌ Missing manage permissions for this category.", ephemeral=True)
            
            await set_config(f'category_id_{inter.guild_id}', cat_id)
            await inter.response.send_message(f"✅ Category set to **{category.name}**!", ephemeral=True)
        
        select = discord.ui.Select(placeholder="Select category...", options=options)
        select.callback = callback
        view = discord.ui.View(timeout=60)
        view.add_item(select)
        await interaction.response.send_message("Select category:", view=view, ephemeral=True)
    
    @discord.ui.button(label="Set Info Content", style=discord.ButtonStyle.secondary, emoji="ℹ️", custom_id="admin_set_info")
    async def set_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(InfoContentModal(self.bot))
    
    @discord.ui.button(label="Set Idle Name", style=discord.ButtonStyle.secondary, emoji="💤", custom_id="admin_idle_name")
    async def set_idle_name(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(IdleNameModal(self.bot))
    
    @discord.ui.button(label="Post Rules", style=discord.ButtonStyle.success, emoji="📋", custom_id="admin_post_rules")
    async def post_rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RulesEmbedModal(self.bot))

    @discord.ui.button(label="Set Exclusions", style=discord.ButtonStyle.secondary, emoji="🚫", custom_id="admin_set_exclusions")
    async def set_exclusions(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set VC names to exclude from the locked VC system"""
        # Pre-populate the modal with current exclusions
        current_exclusions = await get_config(f'excluded_vc_names_{interaction.guild_id}', "")
        modal = ExclusionsModal(self.bot)
        if current_exclusions:
            modal.exclusions.default = current_exclusions
        await interaction.response.send_modal(modal)


class RulesView(discord.ui.View):
    def __init__(self, bot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Info", style=discord.ButtonStyle.primary, emoji="ℹ️", custom_id="rules_info")
    async def info_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        content = await get_config(f'info_content_{interaction.guild_id}', "No info set.")
        await interaction.response.send_message(content, ephemeral=True)

    @discord.ui.button(label="Lock my vc", style=discord.ButtonStyle.success, emoji="🔒", custom_id="rules_lock_vc")
    async def lock_vc_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Convert an unlocked or basic VC to a locked VC"""
        cog = get_cog_safe(self.bot)
        if not cog:
            return await interaction.response.send_message("❌ System temporarily unavailable.", ephemeral=True)

        # Find if user owns a VC
        user_vc_id = None
        user_vc_data = None
        for vc_id, data in cog.active_vcs.items():
            if data['owner_id'] == interaction.user.id:
                user_vc_id = vc_id
                user_vc_data = data
                break

        if not user_vc_id:
            return await interaction.response.send_message("❌ You don't own a VC currently.", ephemeral=True)

        # Check if already locked (not unlocked and not basic)
        if not user_vc_data.get('unlocked', False) and not user_vc_data.get('is_basic', False):
            return await interaction.response.send_message("✅ Your VC is already locked.", ephemeral=True)

        # Get VC channel
        vc = interaction.guild.get_channel(user_vc_id)
        if not vc:
            return await interaction.response.send_message("❌ Your VC no longer exists.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        # Convert to locked VC
        try:
            # Get current members to grandfather them in
            current_members = [m for m in vc.members if not m.bot and m.id != interaction.user.id]

            # Update permissions: lock to public
            await cog.safe_set_permissions(vc, interaction.guild.default_role, connect=False)

            # Grandfather in existing members with VIP access
            for member in current_members:
                await cog.safe_set_permissions(vc, member, connect=True, speak=True)

            # CRITICAL: Update database state BEFORE renaming to prevent race condition
            # The on_guild_channel_update event will fire when we rename, and it needs
            # to see the correct state (unlocked=False, is_basic=False)
            user_vc_data['unlocked'] = False
            user_vc_data['is_basic'] = False
            await cog.save_state()

            logger.info(f"Updated VC {vc.id} database state: unlocked=False, is_basic=False")

            # Update VC name to have lock emoji for ALL conversions (basic and unlocked)
            clean_name = sanitize_name(interaction.user.display_name, interaction.user.id)[:20]
            lock_emoji = "🔒 "

            # Ensure lock emoji is always added when converting to locked VC
            if not vc.name.startswith(lock_emoji):
                # Remove existing name and add lock emoji
                old_name = vc.name
                new_name = f"{lock_emoji}{clean_name}'s VC"
                logger.info(f"Converting VC {vc.id} to locked, renaming: '{old_name}' -> '{new_name}'")

                success = await cog.safe_edit_channel(vc, name=new_name)
                if success:
                    logger.info(f"Successfully added lock emoji to VC {vc.id}")
                else:
                    logger.error(f"Failed to rename VC {vc.id} to add lock emoji")
            else:
                logger.info(f"VC {vc.id} already has lock emoji: {vc.name}")

            # Create hub message
            await cog.create_hub_message(vc)

            # Create settings thread
            hub_id = await get_config(f"hub_channel_id_{interaction.guild.id}")
            if hub_id:
                hub = interaction.guild.get_channel(int(hub_id))
                if hub:
                    perms = hub.permissions_for(interaction.guild.me)
                    try:
                        if not perms.create_private_threads or not perms.manage_threads:
                            thread = await hub.create_thread(
                                name=f"🔒 {clean_name}'s VC Settings",
                                auto_archive_duration=1440
                            )
                        else:
                            thread = await hub.create_thread(
                                name=f"🔒 {clean_name}'s VC Settings",
                                type=discord.ChannelType.private_thread,
                                auto_archive_duration=1440,
                                invitable=False
                            )

                        try:
                            await thread.add_user(interaction.user)
                        except discord.Forbidden:
                            await thread.send(f"⚠️ {interaction.user.mention} - Access VC settings here!")

                        view = KnockManagementView(self.bot, cog, interaction.user.id, vc.id)
                        embed = create_knock_management_embed(interaction.user, [], interaction.guild, user_vc_data)
                        knock_msg = await thread.send(content=interaction.user.mention, embed=embed, view=view)
                        self.bot.add_view(view, message_id=knock_msg.id)

                        user_vc_data['knock_mgmt_msg_id'] = knock_msg.id
                        user_vc_data['thread_id'] = thread.id
                        await cog.save_state()
                    except Exception as e:
                        logger.error(f"Failed to create thread: {e}")

            # Update hub name
            await cog.update_hub_name(interaction.guild, force=True)

            grandfathered_msg = ""
            if current_members:
                grandfathered_msg = f"\n✅ {len(current_members)} member(s) grandfathered in with VIP access."

            await interaction.followup.send(
                f"🔒 **Your VC is now locked!**\n\n"
                f"• A knock button has been added to the hub channel\n"
                f"• You can manage settings in your private thread{grandfathered_msg}",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Failed to lock VC: {e}")
            await interaction.followup.send("❌ Failed to lock your VC. Please try again.", ephemeral=True)

class UserSelectView(discord.ui.View):
    def __init__(self, action, voice_channel, cog_ref):
        super().__init__(timeout=60)
        self.action = action
        self.voice_channel = voice_channel
        self.cog_ref = cog_ref
    
    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select user(s)...", min_values=1, max_values=10)
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        await interaction.response.defer(ephemeral=True)
        
        # FIX: Validate cog is still loaded
        if not self.cog_ref or not hasattr(self.cog_ref, 'get_vc_data'):
            return await interaction.followup.send("❌ System error. Please try again.", ephemeral=True)
        
        vc_data = self.cog_ref.get_vc_data(self.voice_channel.id)
        if not vc_data: 
            return await interaction.followup.send("❌ VC no longer exists.", ephemeral=True)
        
        vc = interaction.guild.get_channel(self.voice_channel.id)
        if not vc: 
            return await interaction.followup.send("❌ VC no longer exists.", ephemeral=True)
        
        try:
            if self.action == 'ban':
                banned, unbanned, failed = [], [], []
                for user in select.values:
                    if user.bot: 
                        continue
                    if user.id == vc_data['owner_id']: 
                        continue
                    
                    # FIX: Ensure user is a Member
                    member = interaction.guild.get_member(user.id)
                    if not member:
                        failed.append(f"{user.mention} (not in server)")
                        continue
                    
                    # FIX: Ensure bans list exists
                    if 'bans' not in vc_data:
                        vc_data['bans'] = []
                    
                    if member.id in vc_data['bans']:
                        vc_data['bans'].remove(member.id)
                        if await self.cog_ref.safe_set_permissions(vc, member, overwrite=None):
                            unbanned.append(member.mention)
                        else:
                            failed.append(f"{member.mention} (permission error)")
                    else:
                        if member.id not in vc_data['bans']: 
                            vc_data['bans'].append(member.id)
                        if await self.cog_ref.safe_set_permissions(vc, member, connect=False):
                            if member in vc.members:
                                try: 
                                    await member.move_to(None)
                                except Exception: 
                                    pass
                            banned.append(member.mention)
                        else:
                            failed.append(f"{member.mention} (permission error)")
                
                await self.cog_ref.save_state()
                msg = ""
                if banned: 
                    msg += f"⛔ **Banned:** {', '.join(banned)}\n"
                if unbanned: 
                    msg += f"✅ **Unbanned:** {', '.join(unbanned)}\n"
                if failed:
                    msg += f"❌ **Failed:** {', '.join(failed)}"
                if not msg:
                    msg = "❌ No changes made."
            
            elif self.action == 'kick':
                kicked, failed = [], []
                for user in select.values:
                    if user.bot or user.id == vc_data['owner_id']: 
                        continue
                    
                    # FIX: Ensure user is a Member
                    member = interaction.guild.get_member(user.id)
                    if not member:
                        failed.append(f"{user.mention} (not in server)")
                        continue
                    
                    if member in vc.members:
                        try:
                            await member.move_to(None)
                            kicked.append(member.mention)
                        except discord.Forbidden:
                            failed.append(f"{member.mention} (no permission)")
                        except Exception as e:
                            logger.debug(f"Failed to kick {member.id}: {e}")
                            failed.append(f"{member.mention} (error)")
                    else:
                        failed.append(f"{member.mention} (not in VC)")
                
                msg = ""
                if kicked:
                    msg += f"👢 **Kicked:** {', '.join(kicked)}"
                if failed:
                    msg += f"\n❌ **Failed:** {', '.join(failed)}" if msg else f"❌ **Failed:** {', '.join(failed)}"
                if not msg:
                    msg = "❌ No users kicked."
            
            elif self.action == 'vip':
                added, skipped, already_vip, failed = [], [], [], []
                for user in select.values:
                    if user.bot: 
                        continue
                    
                    # FIX: Ensure user is a Member, not just a User
                    member = interaction.guild.get_member(user.id)
                    if not member:
                        failed.append(f"{user.mention} (not in server)")
                        continue
                    
                    if member.id in vc_data.get('bans', []): 
                        skipped.append(f"{member.mention} (banned)")
                        continue
                    
                    # FIX: Check if already has VIP access
                    current_perms = vc.overwrites_for(member)
                    if current_perms.connect is True:
                        already_vip.append(member.mention)
                        continue
                    
                    # FIX: Better error handling for permission setting
                    try:
                        if await self.cog_ref.safe_set_permissions(vc, member, connect=True, speak=True):
                            added.append(member.mention)
                        else:
                            failed.append(f"{member.mention} (permission error)")
                    except Exception as e:
                        logger.error(f"Failed to add VIP {member.id}: {e}")
                        failed.append(f"{member.mention} (error)")
                
                msg = ""
                if added:
                    msg += f"⭐ **VIP Access:** {', '.join(added)}\n"
                if already_vip:
                    msg += f"ℹ️ **Already VIP:** {', '.join(already_vip)}\n"
                if skipped:
                    msg += f"⚠️ **Skipped:** {', '.join(skipped)}\n"
                if failed:
                    msg += f"❌ **Failed:** {', '.join(failed)}"
                if not msg:
                    msg = "❌ No users added."
                else:
                    msg = msg.strip()
            
            elif self.action == 'transfer':
                if len(select.values) != 1:
                    return await interaction.followup.send("❌ Select exactly 1 user.", ephemeral=True)
                target = select.values[0]
                if target.bot:
                    return await interaction.followup.send("❌ Cannot transfer to a bot.", ephemeral=True)
                
                # FIX: Ensure target is a Member
                target_member = interaction.guild.get_member(target.id)
                if not target_member:
                    return await interaction.followup.send("❌ User is not in this server.", ephemeral=True)
                
                if target_member.id == vc_data['owner_id']:
                    return await interaction.followup.send("❌ User is already owner.", ephemeral=True)
                if target_member not in vc.members:
                    return await interaction.followup.send("❌ User must be in the VC to become owner.", ephemeral=True)
                if target_member.id in vc_data.get('bans', []):
                    return await interaction.followup.send("❌ Cannot transfer to a banned user.", ephemeral=True)
                
                await self.cog_ref.transfer_ownership(self.voice_channel, target_member)
                msg = f"👑 Transferred to **{target_member.display_name}**."
            else: 
                msg = "❌ Unknown action."
        except Exception as e: 
            logger.error(f"User select error ({self.action}): {e}", exc_info=True)
            msg = f"❌ Error processing request."
        
        await interaction.followup.send(msg, ephemeral=True)

class HubEntryView(discord.ui.View):
    def __init__(self, bot, cog_ref, owner_id, voice_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog_ref = cog_ref
        self.owner_id = owner_id
        self.voice_id = voice_id
        self.knock_btn.custom_id = f"knock:{voice_id}"
    
    @discord.ui.button(label="Knock", style=discord.ButtonStyle.primary)
    async def knock_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        # FIX: Validate cog is loaded
        cog = get_cog_safe(self.bot)
        if not cog:
            return await interaction.response.send_message("❌ System temporarily unavailable. Please try again in a moment.", ephemeral=True)

        # Validate VC still exists
        vc = await validate_vc_channel(self.bot, self.voice_id)
        if not vc:
            return await interaction.response.send_message("❌ This VC no longer exists.", ephemeral=True)

        # Validate VC data
        vc_data = cog.get_vc_data(self.voice_id)
        if not vc_data:
            logger.warning(f"VC data not found for knock on VC {self.voice_id}")
            return await interaction.response.send_message("❌ This VC is no longer active.", ephemeral=True)

        # Validate user is a member
        user = await validate_member(interaction.guild, interaction.user.id)
        if not user:
            logger.warning(f"User {interaction.user.id} not a member during knock attempt")
            return await interaction.response.send_message("❌ You must be a member of this server.", ephemeral=True)

        # Check if banned
        if user.id in vc_data.get('bans', []):
            return await interaction.response.send_message("❌ You are banned from this VC.", ephemeral=True)

        # Check if owner
        if user.id == vc_data['owner_id']:
            return await interaction.response.send_message("❌ You own this VC.", ephemeral=True)

        # Check if already in VC
        if user in vc.members:
            return await interaction.response.send_message("❌ You're already in this VC.", ephemeral=True)

        # Check if already accepted
        if self.voice_id in cog.accepted_knocks and user.id in cog.accepted_knocks[self.voice_id]:
            return await interaction.response.send_message("✅ You already have access to this VC! Just join.", ephemeral=True)

        # Check perms directly
        overwrites = vc.overwrites_for(user)
        if overwrites.connect is True:
            return await interaction.response.send_message("✅ You already have access to this VC! Just join.", ephemeral=True)

        # Check cooldown
        bucket = cog.knock_cooldown.get_bucket(MockMessage(user))
        retry_after = bucket.update_rate_limit()
        if retry_after:
            minutes = int(retry_after // 60)
            seconds = int(retry_after % 60)
            return await interaction.response.send_message(
                f"⏱️ You're on knock cooldown! Try again in {minutes}m {seconds}s.",
                ephemeral=True
            )

        # Initialize pending knocks if needed
        if self.voice_id not in cog.pending_knocks:
            cog.pending_knocks[self.voice_id] = []

        # Check if already pending
        if user.id in cog.pending_knocks[self.voice_id]:
            return await interaction.response.send_message("⏳ You already have a pending knock request.", ephemeral=True)

        # Add to pending knocks
        cog.pending_knocks[self.voice_id].append(user.id)

        # Update panel and send ping
        try:
            await cog.update_knock_panel(self.voice_id)
            await cog.handle_knock_ping(self.voice_id)
        except Exception as e:
            logger.error(f"Error updating knock panel/ping for VC {self.voice_id}: {e}")
            # Still allow the knock to go through even if panel update fails

        await interaction.response.send_message("✅ Knock sent! Wait for the owner to respond.", ephemeral=True)
        logger.info(f"User {user.id} knocked on VC {self.voice_id}")

class KnockManagementView(discord.ui.View):
    def __init__(self, bot, cog_ref, owner_id, voice_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog_ref = cog_ref
        self.owner_id = owner_id
        self.voice_id = voice_id
        
        self.accept_btn.custom_id = f"knock_accept:{voice_id}"
        self.deny_btn.custom_id = f"knock_deny:{voice_id}"
        self.settings_select.custom_id = f"knock_settings:{voice_id}"
    
    def _get_cog(self):
        """Get cog reference, refreshing if needed"""
        if self.cog_ref and hasattr(self.cog_ref, 'get_vc_data'):
            return self.cog_ref
        return get_cog_safe(self.bot)
    
    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success, emoji="✅", row=0)
    async def accept_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = self._get_cog()
        if not cog:
            return await interaction.response.send_message("❌ System temporarily unavailable.", ephemeral=True)
        
        vc_data = cog.get_vc_data(self.voice_id)
        if not vc_data or interaction.user.id != vc_data['owner_id']:
            return await interaction.response.send_message("❌ Only the owner can do this.", ephemeral=True)
        
        pending = cog.pending_knocks.get(self.voice_id, [])
        if not pending:
            return await interaction.response.send_message("❌ No pending knocks.", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        vc = interaction.guild.get_channel(self.voice_id)
        if not vc:
            return await interaction.followup.send("❌ VC no longer exists.", ephemeral=True)
        
        # FIX: Safe pop with race condition handling
        try:
            user_id = pending.pop(0)
        except IndexError:
            return await interaction.followup.send("❌ No pending knocks.", ephemeral=True)
        
        user = interaction.guild.get_member(user_id)
        
        if user:
            await cog.safe_set_permissions(vc, user, connect=True, speak=True)
            
            # Notify in thread - check thread is valid first
            thread_id = vc_data.get('thread_id')
            thread = await check_thread_valid(self.bot, thread_id)
            if thread:
                await cog.handle_knock_accepted(self.voice_id, user_id, thread)
            elif thread_id:
                # Thread was invalid/archived - clear references
                vc_data['thread_id'] = None
                vc_data['knock_mgmt_msg_id'] = None
                await cog.save_state()
        
        # FIX: Safe cooldown reset
        if user:
            try:
                bucket = cog.knock_cooldown.get_bucket(MockMessage(user))
                if hasattr(bucket, '_window'):
                    bucket._window = 0
                elif hasattr(bucket, 'reset'):
                    bucket.reset()
            except Exception as e:
                logger.debug(f"Failed to reset cooldown: {e}")

        # Delete ping notification
        await self._delete_knock_ping_notification(interaction.channel)
        
        await cog.update_knock_panel(self.voice_id)
        name = user.display_name if user else f"User {user_id}"
        await interaction.followup.send(f"✅ Accepted **{name}**", ephemeral=True)
    
    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, emoji="❌", row=0)
    async def deny_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = self._get_cog()
        if not cog:
            return await interaction.response.send_message("❌ System temporarily unavailable.", ephemeral=True)
        
        vc_data = cog.get_vc_data(self.voice_id)
        if not vc_data or interaction.user.id != vc_data['owner_id']:
            return await interaction.response.send_message("❌ Only the owner can do this.", ephemeral=True)
        
        pending = cog.pending_knocks.get(self.voice_id, [])
        if not pending:
            return await interaction.response.send_message("❌ No pending knocks.", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        # FIX: Safe pop with race condition handling
        try:
            user_id = pending.pop(0)
        except IndexError:
            return await interaction.followup.send("❌ No pending knocks.", ephemeral=True)
        
        user = interaction.guild.get_member(user_id)
        
        # Delete ping notification
        await self._delete_knock_ping_notification(interaction.channel)
        
        await cog.update_knock_panel(self.voice_id)
        name = user.display_name if user else f"User {user_id}"
        await interaction.followup.send(f"❌ Denied **{name}**", ephemeral=True)

    async def _delete_knock_ping_notification(self, thread):
        """Delete the '🔔 You have X pending knock requests' message."""
        try:
            async for msg in thread.history(limit=10):
                if msg.author.id == self.bot.user.id and msg.content.startswith("🔔") and "pending knock" in msg.content:
                    await msg.delete()
                    logger.debug(f"Deleted knock ping notification in thread {thread.id}")
                    break
        except discord.NotFound:
            pass
        except Exception as e:
            logger.debug(f"Failed to delete knock ping notification: {e}")
    
    @discord.ui.select(
        placeholder="⚙️ VC Settings",
        options=[
            discord.SelectOption(label="Unlock/Lock", description="Toggle VC lock status", emoji="🔓"),
            discord.SelectOption(label="Ghost Mode", description="Toggle ghost mode", emoji="👻"),
            discord.SelectOption(label="Mute Knock Pings", description="Toggle knock notifications", emoji="🔕"),
            discord.SelectOption(label="Transfer Ownership", description="Transfer VC to another user", emoji="👑"),
            discord.SelectOption(label="Add VIPs", description="Grant access to specific users", emoji="⭐"),
            discord.SelectOption(label="Kick Users", description="Remove users from VC", emoji="👢"),
            discord.SelectOption(label="Ban/Unban", description="Ban or unban users", emoji="⛔"),
            discord.SelectOption(label="Manage Presets", description="Save/load VC presets", emoji="💾"),
        ],
        row=1
    )
    async def settings_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        cog = self._get_cog()
        if not cog:
            return await interaction.response.send_message("❌ System temporarily unavailable.", ephemeral=True)
        
        vc_data = cog.get_vc_data(self.voice_id)
        if not vc_data:
            return await interaction.response.send_message("❌ VC no longer exists.", ephemeral=True)
        if interaction.user.id != vc_data['owner_id']:
            return await interaction.response.send_message("❌ Only the VC owner can access settings.", ephemeral=True)
        
        vc = interaction.guild.get_channel(self.voice_id)
        if not vc:
            return await interaction.response.send_message("❌ VC no longer exists.", ephemeral=True)
        
        choice = select.values[0]
        
        if choice == "Unlock/Lock":
            await interaction.response.defer(ephemeral=True)
            if vc_data.get('ghost', False) and not vc_data.get('unlocked', False):
                return await interaction.followup.send("❌ Disable **Ghost Mode** before unlocking your VC.", ephemeral=True)
            
            new_state = not vc_data.get('unlocked', False)
            vc_data['unlocked'] = new_state
            
            # Save state before operations
            await cog.save_state()
            
            status = "UNLOCKED" if new_state else "LOCKED"
            
            try:
                await cog.safe_set_permissions(vc, interaction.guild.default_role, connect=new_state)
            except Exception as e:
                logger.error(f"Failed to set permissions: {e}")
                return await interaction.followup.send(f"❌ Error updating permissions: {e}", ephemeral=True)
            
            prefix = "🔒 "
            try:
                name = vc.name
                if new_state and name.startswith(prefix): 
                    await cog.safe_edit_channel(vc, name=name.replace(prefix, "", 1))
                elif not new_state and not name.startswith(prefix): 
                    await cog.safe_edit_channel(vc, name=f"{prefix}{name}")
            except Exception as e:
                logger.error(f"Failed to rename VC: {e}")
            
            # FIX: Message operations now use internal locking to prevent races
            if new_state:
                # Unlocking: delete hub message (delete_hub_message handles locking and clearing message_id)
                await cog.delete_hub_message(vc.id)
                logger.info(f"Deleted hub message when unlocking VC {vc.id}")
            else:
                # Locking: delete old message and create new one
                await cog.delete_hub_message(vc.id)

                # Only create hub message if not in ghost mode and not basic VC
                if not vc_data.get('ghost', False) and not vc_data.get('is_basic', False):
                    # FIX: Check if creation succeeded and notify user
                    success = await cog.create_hub_message(vc)
                    if success:
                        logger.info(f"Created hub message when locking VC {vc.id}")
                    else:
                        logger.error(f"Failed to create hub message for VC {vc.id}")
                        await interaction.followup.send(f"⚠️ Channel **{status}** but knock message creation failed.", ephemeral=True)
                        return
            
            await cog.update_knock_panel(self.voice_id)
            
            # Always force hub rename after lock/unlock
            await cog.update_hub_name(interaction.guild, force=True)
            
            await interaction.followup.send(f"🔓 Channel is now **{status}**", ephemeral=True)
        
        elif choice == "Ghost Mode":
            await interaction.response.defer(ephemeral=True)
            
            # FIX: Double-check VC is actually locked before enabling ghost
            if not vc_data.get('unlocked', False):
                # Verify permissions match expected locked state
                default_perms = vc.overwrites_for(interaction.guild.default_role)
                if default_perms.connect is True or default_perms.connect is None:
                    logger.warning(f"VC {vc.id} database shows locked but permissions are wrong")
                    await interaction.followup.send("⚠️ VC permissions don't match locked state. Please lock your VC first.", ephemeral=True)
                    return
            else:
                return await interaction.followup.send("❌ You must **Lock** your VC before enabling Ghost Mode.", ephemeral=True)

            new_state = not vc_data.get('ghost', False)
            vc_data['ghost'] = new_state
            
            # Save state before operations
            await cog.save_state()

            # FIX: Message operations now use internal locking to prevent races
            if new_state:
                # Enabling ghost: delete hub message (delete_hub_message handles locking and clearing message_id)
                await cog.delete_hub_message(vc.id)
                logger.info(f"Deleted hub message when enabling ghost mode for VC {vc.id}")
            else:
                # Disabling ghost: delete old message and create new one
                await cog.delete_hub_message(vc.id)

                # Create hub message (VC is locked since ghost requires locked state)
                # FIX: Check if creation succeeded and notify user
                success = await cog.create_hub_message(vc)
                if success:
                    logger.info(f"Created hub message when disabling ghost mode for VC {vc.id}")
                else:
                    logger.error(f"Failed to create hub message for VC {vc.id}")
                    msg = "👻 **Ghost mode disabled** (⚠️ knock message creation failed)"
                    await interaction.followup.send(msg, ephemeral=True)
                    return
            
            await cog.update_knock_panel(self.voice_id)
            
            # Always force hub rename after ghost mode toggle
            await cog.update_hub_name(interaction.guild, force=True)
            
            msg = "👻 **Ghost mode enabled**" if new_state else "👻 **Ghost mode disabled**"
            await interaction.followup.send(msg, ephemeral=True)
        
        elif choice == "Mute Knock Pings":
            new_state = not vc_data.get('mute_knock_pings', False)
            vc_data['mute_knock_pings'] = new_state
            await cog.save_state()
            await cog.update_knock_panel(self.voice_id)
            msg = "🔕 **Knock pings muted**" if new_state else "🔔 **Knock pings enabled**"
            await interaction.response.send_message(msg, ephemeral=True)
        
        elif choice == "Ban/Unban":
            await interaction.response.send_message("Select to Ban/Unban:", view=UserSelectView('ban', vc, cog), ephemeral=True)
        
        elif choice == "Transfer Ownership":
            await interaction.response.send_message("Select new Owner:", view=UserSelectView('transfer', vc, cog), ephemeral=True)
        
        elif choice == "Add VIPs":
            await interaction.response.send_message("Select VIPs:", view=UserSelectView('vip', vc, cog), ephemeral=True)
        
        elif choice == "Kick Users":
            await interaction.response.send_message("Select to Kick:", view=UserSelectView('kick', vc, cog), ephemeral=True)
        
        elif choice == "Manage Presets":
            view = discord.ui.View(timeout=60)
            # Store voice_id for callbacks to use (more stable than vc object)
            voice_id = self.voice_id

            async def save_cb(inter):
                # FIX: Refresh cog and vc references inside callback
                fresh_cog = get_cog_safe(self.bot)
                if not fresh_cog:
                    return await inter.response.send_message("❌ System unavailable.", ephemeral=True)
                fresh_vc = inter.guild.get_channel(voice_id)
                if not fresh_vc:
                    return await inter.response.send_message("❌ VC no longer exists.", ephemeral=True)
                fresh_vc_data = fresh_cog.get_vc_data(voice_id)
                if not fresh_vc_data:
                    return await inter.response.send_message("❌ VC data not found.", ephemeral=True)
                await inter.response.send_modal(SavePresetModal(fresh_vc, fresh_vc_data.get('bans', [])))

            save_btn = discord.ui.Button(label="Save New", style=discord.ButtonStyle.success)
            save_btn.callback = save_cb
            view.add_item(save_btn)

            user_presets = await get_user_presets(interaction.user.id)
            if user_presets:
                options = [discord.SelectOption(label=name[:100]) for name in list(user_presets.keys())[:25]]

                async def load_cb(inter):
                    p_name = select_load.values[0]
                    data = user_presets.get(p_name)
                    if not data or not isinstance(data, dict):
                        return await inter.response.send_message("❌ Corrupted preset data.", ephemeral=True)

                    # FIX: Refresh cog reference inside callback
                    fresh_cog = get_cog_safe(self.bot)
                    if not fresh_cog:
                        return await inter.response.send_message("❌ System unavailable.", ephemeral=True)

                    # FIX: Refresh vc reference inside callback
                    fresh_vc = inter.guild.get_channel(voice_id)
                    if not fresh_vc:
                        return await inter.response.send_message("❌ VC no longer exists.", ephemeral=True)

                    # FIX: Refresh vc_data reference
                    fresh_vc_data = fresh_cog.get_vc_data(voice_id)
                    if not fresh_vc_data:
                        return await inter.response.send_message("❌ VC data not found.", ephemeral=True)

                    # FIX: Better validation of preset data
                    safe_name = str(data.get("name", "VC"))[:100]
                    try:
                        safe_limit = max(0, min(int(data.get("limit", 0)), fresh_cog.get_max_voice_limit(inter.guild)))
                    except (ValueError, TypeError):
                        safe_limit = 0
                    try:
                        safe_bitrate = min(int(data.get("bitrate", 64000)), fresh_cog.get_guild_bitrate_limit(inter.guild))
                    except (ValueError, TypeError):
                        safe_bitrate = 64000

                    # FIX: Preserve lock prefix when loading preset name
                    # If VC is locked, ensure the lock emoji is preserved
                    is_locked = not fresh_vc_data.get('unlocked', False)
                    prefix = "🔒 "

                    if is_locked:
                        # Remove any existing prefix from preset name, then add correct prefix
                        clean_preset_name = safe_name.replace(prefix, "").strip()
                        final_name = f"{prefix}{clean_preset_name}"
                    else:
                        # Unlocked VC - remove prefix if present
                        final_name = safe_name.replace(prefix, "").strip()

                    # FIX: Set debounce key BEFORE editing channel to prevent on_guild_channel_update interference
                    import time
                    debounce_key = f"preset_load_{voice_id}"
                    fresh_cog._name_update_debounce[debounce_key] = time.time()

                    await fresh_cog.safe_edit_channel(fresh_vc, name=final_name, user_limit=safe_limit, bitrate=safe_bitrate)

                    if 'bans' in data and isinstance(data['bans'], list):
                        current_bans = set(fresh_vc_data.get('bans', []))
                        valid_preset_bans = set()
                        for b in data['bans']:
                            try:
                                valid_preset_bans.add(int(b))
                            except (ValueError, TypeError):
                                continue

                        new_bans = list(current_bans.union(valid_preset_bans))
                        fresh_vc_data['bans'] = new_bans
                        await fresh_cog.save_state()
                        bans_to_apply = valid_preset_bans - current_bans
                        ops = []
                        for bid in bans_to_apply:
                            m = inter.guild.get_member(bid)
                            if m:
                                ops.append(fresh_cog.safe_set_permissions(fresh_vc, m, connect=False))
                        if ops:
                            await fresh_cog.batch_operations(ops)

                    await fresh_cog.update_hub_embed(voice_id)
                    await inter.response.send_message(f"✅ Loaded **{p_name}**.", ephemeral=True)

                async def delete_cb(inter):
                    if not select_load.values:
                        return await inter.response.send_message("❌ Select preset.", ephemeral=True)
                    p_name = select_load.values[0]
                    await delete_preset(inter.user.id, p_name)
                    await inter.response.send_message(f"🗑️ Deleted **{p_name}**.", ephemeral=True)

                select_load = discord.ui.Select(placeholder="Load Preset...", options=options)
                select_load.callback = load_cb
                view.add_item(select_load)
                delete_btn = discord.ui.Button(label="Delete", style=discord.ButtonStyle.danger, emoji="🗑️")
                delete_btn.callback = delete_cb
                view.add_item(delete_btn)

            await interaction.response.send_message("💾 **Preset Manager**", view=view, ephemeral=True)

    @discord.ui.button(label="Reconnect VC", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def reconnect_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Manual reconnect button for restoring disconnected VCs"""
        cog = self._get_cog()
        if not cog:
            return await interaction.response.send_message("❌ System temporarily unavailable.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        # Parse owner name from thread name (format: "🔒 {owner}'s VC Settings")
        thread = interaction.channel
        if not isinstance(thread, discord.Thread):
            return await interaction.followup.send("❌ This button can only be used in a VC settings thread.", ephemeral=True)

        # Check if VC is still tracked
        vc_data = cog.get_vc_data(self.voice_id)
        vc = interaction.guild.get_channel(self.voice_id)

        # Determine if user is authorized
        is_owner = False
        is_in_vc = False

        if vc_data:
            is_owner = interaction.user.id == vc_data['owner_id']
        else:
            # VC data lost - parse owner from thread name
            thread_name = thread.name
            if "'s VC Settings" in thread_name:
                # Extract owner name from thread
                owner_name_part = thread_name.replace("🔒 ", "").replace("'s VC Settings", "")
                # Check if user's display name matches
                if owner_name_part.lower() in interaction.user.display_name.lower():
                    is_owner = True

        # Check if user is in the VC (if VC exists)
        if vc:
            is_in_vc = interaction.user in vc.members

        # Authorization check: must be owner OR (if owner not in VC, must be in VC)
        if not is_owner and not is_in_vc:
            return await interaction.followup.send(
                "❌ You must be the VC owner or be in the VC to use this button.",
                ephemeral=True
            )

        # Call the manual reconnect method
        try:
            success, message = await cog.reconnect_vc_manual(self.voice_id, interaction.guild, interaction.user)
            if success:
                await interaction.followup.send(f"✅ {message}", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ {message}", ephemeral=True)
        except Exception as e:
            logger.error(f"Manual reconnect failed: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Reconnect failed: {str(e)}", ephemeral=True)
