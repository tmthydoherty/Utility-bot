import re

with open('cogs/RoleNotifier.py', 'r') as f:
    content = f.read()

# 1. Remove ConfigPanel and modals
content = re.sub(r'# ============================================================================\n# CONFIG PANEL - New/Manage flow\n# ============================================================================.*?# ============================================================================\n# MAIN COG\n# ============================================================================', '# ============================================================================\n# MAIN COG\n# ============================================================================', content, flags=re.DOTALL)

# 2. Replace RoleAlerts -> RoleNotifier
content = content.replace('RoleAlerts', 'RoleNotifier')
content = content.replace('rolealerts', 'RoleNotifier')

# 3. Remove the app_command for role_alerts_config
content = re.sub(r'    @app_commands\.command\(name="RoleNotifier", description="Configure role alerts system"\).*?panel\.message = await interaction\.original_response\(\)', '', content, flags=re.DOTALL)
content = content.replace('@app_commands.command(name="role_alerts",', '# Removed command')

with open('cogs/RoleNotifier.py', 'w') as f:
    f.write(content)
