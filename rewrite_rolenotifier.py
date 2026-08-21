import re

with open('cogs/RoleNotifier.py', 'r') as f:
    content = f.read()

# 1. Add ConfigSyncAgent import
content = content.replace("from utils.perms import is_admin_interaction", "from utils.perms import is_admin_interaction\nfrom utils.module_config_sync import ConfigSyncAgent")

# 2. Remove guild_settings and tracked_roles tables from init_db
init_db_new = """async def init_db():
    import os
    os.makedirs("data", exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute('''
            CREATE TABLE IF NOT EXISTS active_alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                channel_id INTEGER,
                message_id INTEGER,
                user_id INTEGER,
                role_id INTEGER,
                claimed_by INTEGER,
                thread_id INTEGER,
                created_at TEXT,
                claimed_at TEXT,
                status TEXT DEFAULT 'pending'
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS role_cooldowns (
                guild_id INTEGER,
                user_id INTEGER,
                role_id INTEGER,
                last_alert TEXT,
                PRIMARY KEY (guild_id, user_id, role_id)
            )
        ''')
        await db.commit()"""

# We'll just replace the entire init_db function
content = re.sub(r'async def init_db\(\):.*?# ============================================================================', init_db_new + '\n\n\n# ============================================================================', content, flags=re.DOTALL)


# 3. Remove ConfigPanel and modals
content = re.sub(r'# ============================================================================\n# CONFIG PANEL - New/Manage flow\n# ============================================================================.*?# ============================================================================\n# MAIN COG\n# ============================================================================', '# ============================================================================\n# MAIN COG\n# ============================================================================', content, flags=re.DOTALL)

# 4. Replace RoleAlerts -> RoleNotifier
content = content.replace('RoleAlerts', 'RoleNotifier')
content = content.replace('rolealerts', 'RoleNotifier')

# 5. Add ConfigSyncAgent init in __init__
init_new = """    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cooldowns: dict[tuple[int, int, int], datetime] = {}
        self.COOLDOWN_SECONDS = 60
        self.config_agent = ConfigSyncAgent(
            module_name="role-notifier",
            bot=bot,
            default_config_path="role_notifier_config.json"
        )
"""
content = re.sub(r'    def __init__\(self, bot: commands\.Bot\):.*?self\.COOLDOWN_SECONDS = 60', init_new.strip('\n'), content, flags=re.DOTALL)


# 6. Replace DB methods with config logic
db_methods = """    async def get_guild_settings(self, guild_id: int) -> Optional[dict]:
        pass

    async def get_tracked_roles(self, guild_id: int) -> list[int]:
        config = self.config_agent.get_guild_config(guild_id)
        if not config: return []
        roles = []
        for i in range(1, 4):
            role_id = config.get(f'role{i}_id')
            if role_id: roles.append(role_id)
        return roles

    async def get_tracked_role_settings(self, guild_id: int, role_id: int) -> Optional[dict]:
        config = self.config_agent.get_guild_config(guild_id)
        if not config: return None
        for i in range(1, 4):
            if config.get(f'role{i}_id') == role_id:
                return {
                    'role_id': role_id,
                    'alert_channel_id': config.get(f'role{i}_alert_channel'),
                    'thread_channel_id': config.get(f'role{i}_thread_channel'),
                    'admin_role_id': config.get(f'role{i}_admin_role'),
                    'ping_enabled': bool(config.get(f'role{i}_ping_role')),
                    'ping_role_id': config.get(f'role{i}_ping_role'),
                    'bypass_role_id': config.get(f'role{i}_bypass_role'),
                    'thread_name_format': config.get(f'role{i}_thread_format'),
                    'welcome_message': config.get(f'role{i}_welcome_msg')
                }
        return None"""

content = re.sub(r'    async def get_guild_settings.*?    async def get_alert_data', db_methods + '\n\n    async def get_alert_data', content, flags=re.DOTALL)

# 7. Remove the app_command for role_alerts_config
content = re.sub(r'    @app_commands\.command\(name="RoleNotifier", description="Configure role alerts system"\).*?panel\.message = await interaction\.original_response\(\)', '', content, flags=re.DOTALL)
content = content.replace('@app_commands.command(name="role_alerts",', '# Removed command')

with open('cogs/RoleNotifier.py', 'w') as f:
    f.write(content)
