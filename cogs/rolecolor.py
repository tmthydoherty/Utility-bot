import discord
from discord.ext import commands
from discord import app_commands, ui
import logging
import asyncio
import json
from pathlib import Path

log = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "rolecolor_config.json"

EMBED_COLOR = 0x5865F2
COLORS_PER_PAGE = 50
TOTAL_COLORS = 150
TOTAL_PAGES = 3
ROLE_CREATE_DELAY = 1.2   # seconds between each role creation
ROLE_DELETE_DELAY = 0.5    # seconds between each role deletion
ROLE_NAME = "RoleColor"


# ─── 150 Gradient Pairs (color1, color2) ───
# Mixed variety — classic, neon, pastel, dark, bold contrasts on every page.

GRADIENTS: list[tuple[int, int]] = [
    # ── Section 1 (1-50) ──
    (0xC0C0C0, 0x2C2F33),  # 1.  Silver → Black
    (0xE74C3C, 0x2196F3),  # 2.  Red → Blue
    (0xFFD700, 0x1A1A2E),  # 3.  Gold → Black
    (0xE91E63, 0x9C27B0),  # 4.  Pink → Purple
    (0x2ECC71, 0xF1C40F),  # 5.  Green → Gold
    (0x00BCD4, 0xFF5722),  # 6.  Cyan → Deep Orange
    (0xFFFFFF, 0x3498DB),  # 7.  White → Blue
    (0x8E44AD, 0x2ECC71),  # 8.  Purple → Emerald
    (0xF39C12, 0xE74C3C),  # 9.  Amber → Red
    (0x1ABC9C, 0x2C3E50),  # 10. Teal → Dark Navy
    (0xFF4081, 0xFFD740),  # 11. Hot Pink → Gold
    (0x3498DB, 0x1ABC9C),  # 12. Blue → Teal
    (0xE74C3C, 0xFFD700),  # 13. Red → Gold
    (0x212121, 0xE53935),  # 14. Charcoal → Red
    (0x7C4DFF, 0x18FFFF),  # 15. Violet → Cyan
    (0xF48FB1, 0x90CAF9),  # 16. Soft Pink → Soft Blue
    (0x00E676, 0x651FFF),  # 17. Neon Green → Deep Purple
    (0xBDBDBD, 0x616161),  # 18. Light Gray → Dark Gray
    (0xFF6D00, 0x304FFE),  # 19. Orange → Blue
    (0x880E4F, 0xFFD600),  # 20. Wine → Yellow
    (0xC0C0C0, 0x4A148C),  # 21. Silver → Deep Purple
    (0xE53935, 0x1A1A2E),  # 22. Red → Black
    (0x42A5F5, 0xEF5350),  # 23. Sky Blue → Coral
    (0x00E5FF, 0xD500F9),  # 24. Electric Blue → Magenta
    (0x2E7D32, 0xC0C0C0),  # 25. Forest → Silver
    (0xFFAB00, 0x6200EA),  # 26. Amber → Violet
    (0x263238, 0x00BFA5),  # 27. Dark Slate → Teal
    (0xD50000, 0x2196F3),  # 28. Vivid Red → Blue
    (0xCE93D8, 0x4FC3F7),  # 29. Orchid → Sky Blue
    (0xFFD700, 0xE53935),  # 30. Gold → Red
    (0x1A237E, 0xC0C0C0),  # 31. Navy → Silver
    (0x76FF03, 0xFF3D00),  # 32. Neon Green → Red-Orange
    (0x455A64, 0xFFCA28),  # 33. Steel → Amber
    (0xAA00FF, 0x00E5FF),  # 34. Purple → Electric Blue
    (0xF5F5F5, 0xE91E63),  # 35. White → Pink
    (0x004D40, 0xFFD740),  # 36. Deep Teal → Gold
    (0xFF5252, 0x536DFE),  # 37. Light Red → Indigo
    (0xB2DFDB, 0xB39DDB),  # 38. Mint → Lavender
    (0x212121, 0xFFD700),  # 39. Black → Gold
    (0x0097A7, 0xF06292),  # 40. Teal → Pink
    (0xC62828, 0x283593),  # 41. Dark Red → Dark Blue
    (0xFFCC80, 0x7E57C2),  # 42. Peach → Purple
    (0x00C853, 0x2962FF),  # 43. Green → Blue
    (0x37474F, 0xE53935),  # 44. Gunmetal → Red
    (0xD500F9, 0x76FF03),  # 45. Magenta → Neon Green
    (0xF9A825, 0x4527A0),  # 46. Mustard → Deep Purple
    (0x81D4FA, 0xF8BBD0),  # 47. Baby Blue → Baby Pink
    (0xBF360C, 0x00695C),  # 48. Rust → Emerald
    (0xE0E0E0, 0x0D47A1),  # 49. Silver → Navy
    (0xFF1744, 0x00E676),  # 50. Red → Neon Green

    # ── Section 2 (51-100) ──
    (0x5C6BC0, 0xEF6C00),  # 51. Indigo → Orange
    (0x1B5E20, 0xF44336),  # 52. Forest → Red
    (0xFDD835, 0xE91E63),  # 53. Lemon → Pink
    (0x00BCD4, 0x212121),  # 54. Cyan → Black
    (0x8D6E63, 0xFFCA28),  # 55. Brown → Amber
    (0xE040FB, 0x00BFA5),  # 56. Pink → Teal
    (0x3E2723, 0xC0C0C0),  # 57. Dark Brown → Silver
    (0xFF6F00, 0x1565C0),  # 58. Deep Orange → Royal Blue
    (0x9C27B0, 0xFFEB3B),  # 59. Purple → Yellow
    (0x78909C, 0xD32F2F),  # 60. Blue-Gray → Red
    (0xEC407A, 0x26C6DA),  # 61. Rose → Turquoise
    (0x2C2F33, 0x99AAB5),  # 62. Discord Dark → Discord Gray
    (0xF44336, 0xFFC107),  # 63. Red → Amber
    (0x1DE9B6, 0x651FFF),  # 64. Mint → Deep Purple
    (0xFFCDD2, 0xC5CAE9),  # 65. Rose → Periwinkle
    (0xAD1457, 0x00838F),  # 66. Crimson → Dark Teal
    (0xFFA726, 0x8E24AA),  # 67. Orange → Purple
    (0x004D40, 0xE0E0E0),  # 68. Dark Green → Silver
    (0x536DFE, 0xFF4081),  # 69. Indigo → Hot Pink
    (0x2E7D32, 0x0288D1),  # 70. Green → Blue
    (0xE65100, 0x1A237E),  # 71. Burnt Orange → Navy
    (0x90CAF9, 0xFFCC80),  # 72. Light Blue → Peach
    (0x6200EA, 0xC0C0C0),  # 73. Violet → Silver
    (0x00E5FF, 0x1B1B2F),  # 74. Electric Blue → Midnight
    (0xB71C1C, 0x2E7D32),  # 75. Dark Red → Green
    (0xD7CCC8, 0x5D4037),  # 76. Warm Gray → Brown
    (0xFF9100, 0x00C853),  # 77. Orange → Green
    (0x4A148C, 0xE53935),  # 78. Deep Purple → Red
    (0x64FFDA, 0x311B92),  # 79. Aqua → Deep Indigo
    (0xC6FF00, 0xAA00FF),  # 80. Lime → Purple
    (0x2C2F33, 0xFFD700),  # 81. Black → Gold
    (0xE91E63, 0x00BCD4),  # 82. Pink → Cyan
    (0x1A237E, 0xE94560),  # 83. Navy → Ruby
    (0xA5D6A7, 0xEF9A9A),  # 84. Sage → Salmon
    (0xD50000, 0xC0C0C0),  # 85. Red → Silver
    (0x00695C, 0xD500F9),  # 86. Emerald → Magenta
    (0xFFEB3B, 0x1565C0),  # 87. Yellow → Royal Blue
    (0x37474F, 0x66BB6A),  # 88. Gunmetal → Green
    (0xF50057, 0x304FFE),  # 89. Pink → Blue
    (0xFFD740, 0x00B0FF),  # 90. Gold → Light Blue
    (0x4E342E, 0xFFA726),  # 91. Brown → Orange
    (0x7C4DFF, 0xFF6D00),  # 92. Violet → Orange
    (0x212121, 0x00E676),  # 93. Black → Neon Green
    (0xEF5350, 0xAB47BC),  # 94. Coral → Purple
    (0x0D47A1, 0xFFD600),  # 95. Navy → Yellow
    (0x880E4F, 0x1DE9B6),  # 96. Wine → Mint
    (0xE0E0E0, 0x263238),  # 97. Silver → Dark Slate
    (0x2196F3, 0xFF9800),  # 98. Blue → Orange
    (0x4A148C, 0x00BFA5),  # 99. Deep Purple → Teal
    (0xC62828, 0xFFD700),  # 100. Crimson → Gold

    # ── Section 3 (101-150) ──
    (0xFFFFFF, 0x212121),  # 101. White → Black
    (0xE53935, 0x43A047),  # 102. Red → Green
    (0x00ACC1, 0xF4511E),  # 103. Cyan → Flame
    (0x6A1B9A, 0xFFD54F),  # 104. Purple → Gold
    (0x558B2F, 0x1565C0),  # 105. Olive → Blue
    (0xFF6E40, 0x7C4DFF),  # 106. Deep Orange → Violet
    (0x1A237E, 0xFFEB3B),  # 107. Navy → Yellow
    (0xEF5350, 0x26A69A),  # 108. Coral → Sea Green
    (0xBDBDBD, 0xB71C1C),  # 109. Gray → Dark Red
    (0x00897B, 0xE91E63),  # 110. Jade → Pink
    (0xFBC02D, 0x5E35B1),  # 111. Gold → Deep Purple
    (0x0277BD, 0xF57F17),  # 112. Ocean → Saffron
    (0x2C2F33, 0x18FFFF),  # 113. Black → Electric Cyan
    (0xD81B60, 0x7CB342),  # 114. Magenta → Olive Green
    (0xFFB300, 0x1E88E5),  # 115. Amber → Blue
    (0x4DB6AC, 0xE57373),  # 116. Seafoam → Light Red
    (0x5D4037, 0xA1887F),  # 117. Brown → Taupe
    (0x0288D1, 0xC0CA33),  # 118. Blue → Lime
    (0xAB47BC, 0x29B6F6),  # 119. Purple → Sky Blue
    (0x2C2F33, 0xD500F9),  # 120. Black → Magenta
    (0xF44336, 0x00BCD4),  # 121. Red → Cyan
    (0x43A047, 0xFDD835),  # 122. Green → Lemon
    (0xAD1457, 0x283593),  # 123. Deep Pink → Indigo
    (0xFFAB40, 0x00695C),  # 124. Light Orange → Emerald
    (0x9FA8DA, 0xF48FB1),  # 125. Periwinkle → Pink
    (0x00838F, 0xFFAB00),  # 126. Dark Teal → Amber
    (0x6D4C41, 0x66BB6A),  # 127. Brown → Green
    (0x1565C0, 0xEF5350),  # 128. Royal Blue → Coral
    (0xE0E0E0, 0x6200EA),  # 129. Silver → Violet
    (0xFF3D00, 0x1DE9B6),  # 130. Red-Orange → Mint
    (0x311B92, 0xFF6D00),  # 131. Deep Indigo → Orange
    (0xC0CA33, 0x8E24AA),  # 132. Lime → Purple
    (0x00BFA5, 0xF44336),  # 133. Teal → Red
    (0xFFD600, 0x0D47A1),  # 134. Yellow → Navy
    (0x78909C, 0xFFA726),  # 135. Blue-Gray → Orange
    (0xD500F9, 0xFFD700),  # 136. Magenta → Gold
    (0x004D40, 0xEF5350),  # 137. Deep Teal → Coral
    (0xFFCC80, 0x283593),  # 138. Peach → Indigo
    (0x2E7D32, 0xE91E63),  # 139. Forest → Pink
    (0x42A5F5, 0xFFCA28),  # 140. Blue → Amber
    (0xBF360C, 0x00BCD4),  # 141. Rust → Cyan
    (0xE1BEE7, 0x00695C),  # 142. Lilac → Emerald
    (0x263238, 0xFF4081),  # 143. Dark Slate → Hot Pink
    (0x1DE9B6, 0xE65100),  # 144. Mint → Burnt Orange
    (0x5C6BC0, 0xC0C0C0),  # 145. Indigo → Silver
    (0xEF9A9A, 0x1A237E),  # 146. Salmon → Navy
    (0x00E676, 0xE91E63),  # 147. Neon Green → Pink
    (0xFFD740, 0x4A148C),  # 148. Gold → Deep Purple
    (0x0097A7, 0xFFFFFF),  # 149. Teal → White
    (0xF50057, 0x00E5FF),  # 150. Pink → Electric Blue
]


# ─── Raw Discord API helpers (discord.py doesn't support the `colors` field) ───

def _colors_payload(c1: int, c2: int) -> dict:
    """Build the `colors` dict Discord expects for a 2-color gradient role."""
    return {
        "primary_color": c1,
        "secondary_color": c2,
        "tertiary_color": None,
    }


async def _api_create_role(http, guild_id: int, name: str,
                           c1: int, c2: int, reason: str = None) -> dict:
    """Create a role with gradient colors via raw Discord API."""
    payload = {
        "name": name,
        "colors": _colors_payload(c1, c2),
        "permissions": "0",
    }
    route = discord.http.Route(
        "POST", "/guilds/{guild_id}/roles", guild_id=guild_id
    )
    return await http.request(route, json=payload, reason=reason)


async def _api_edit_role_colors(http, guild_id: int, role_id: int,
                                c1: int, c2: int, reason: str = None) -> dict:
    """Patch only the gradient colors on an existing role via raw Discord API."""
    payload = {"colors": _colors_payload(c1, c2)}
    route = discord.http.Route(
        "PATCH", "/guilds/{guild_id}/roles/{role_id}",
        guild_id=guild_id, role_id=role_id,
    )
    return await http.request(route, json=payload, reason=reason)


# ============================================================
#  Persistent Views — survive bot restarts via custom_id
# ============================================================

class RoleColorPanelView(ui.View):
    """Main admin panel — Showcase + Apply buttons."""

    def __init__(self, cog: "RoleColor"):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="Showcase All Gradients", style=discord.ButtonStyle.primary,
               custom_id="rolecolor:showcase", emoji="\U0001f308", row=0)
    async def showcase_btn(self, interaction: discord.Interaction, button: ui.Button):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("Admin only.", ephemeral=True)
        await self.cog.showcase_all(interaction)

    @ui.button(label="Apply Gradient to Role", style=discord.ButtonStyle.success,
               custom_id="rolecolor:apply", emoji="\U0001f3a8", row=0)
    async def apply_btn(self, interaction: discord.Interaction, button: ui.Button):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("Admin only.", ephemeral=True)
        await interaction.response.send_modal(ApplyColorModal(self.cog))


class ApplyColorModal(ui.Modal, title="Apply Gradient to Role"):
    """Modal that asks for the gradient number (1-150)."""

    color_num = ui.TextInput(
        label="Gradient Number (1-150)",
        placeholder="e.g. 7",
        min_length=1,
        max_length=3,
        required=True,
    )

    def __init__(self, cog: "RoleColor"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            num = int(self.color_num.value)
            if not 1 <= num <= TOTAL_COLORS:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message(
                f"Enter a number between 1 and {TOTAL_COLORS}.", ephemeral=True
            )

        c1, c2 = GRADIENTS[num - 1]
        hex1, hex2 = f"#{c1:06X}", f"#{c2:06X}"

        view = TargetRoleSelectView(self.cog, num, c1, c2)
        embed = discord.Embed(
            title=f"Apply Gradient #{num}",
            description=(
                f"Gradient: **{hex1}** → **{hex2}**\n\n"
                "Select the role to apply this gradient to:"
            ),
            color=c1,
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class TargetRoleSelectView(ui.View):
    """Ephemeral role select that appears after the modal."""

    def __init__(self, cog: "RoleColor", gradient_num: int, c1: int, c2: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.gradient_num = gradient_num
        self.c1 = c1
        self.c2 = c2

    @ui.select(cls=ui.RoleSelect, placeholder="Select target role...",
               min_values=1, max_values=1)
    async def role_select(self, interaction: discord.Interaction, select: ui.RoleSelect):
        role = select.values[0]
        await interaction.response.defer(ephemeral=True, thinking=True)

        hex1, hex2 = f"#{self.c1:06X}", f"#{self.c2:06X}"

        try:
            await _api_edit_role_colors(
                interaction.client.http,
                interaction.guild_id,
                role.id,
                self.c1, self.c2,
                reason=f"Gradient #{self.gradient_num} applied by {interaction.user}",
            )
            embed = discord.Embed(
                description=(
                    f"Applied gradient **#{self.gradient_num}** "
                    f"(`{hex1}` → `{hex2}`) to {role.mention}."
                ),
                color=self.c1,
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to edit that role. "
                "Make sure my role is positioned above it.",
                ephemeral=True,
            )
        except discord.HTTPException as e:
            log.error(f"Failed to apply gradient: {e}", exc_info=True)
            await interaction.followup.send(
                f"Failed to apply gradient: {e}", ephemeral=True
            )


# ============================================================
#  Cog
# ============================================================

class RoleColor(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._creating: set[int] = set()  # guild IDs currently running showcase
        self._config: dict = self._load_config()
        self.bot.add_view(RoleColorPanelView(self))

    def _load_config(self) -> dict:
        try:
            return json.loads(CONFIG_PATH.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_config(self):
        CONFIG_PATH.write_text(json.dumps(self._config, indent=4))

    def _track_roles(self, guild_id: int, role_ids: list[int], page: int):
        """Persist created showcase role IDs so they survive restarts."""
        gid = str(guild_id)
        existing = self._config.get(gid, {}).get("showcase_roles", [])
        self._config[gid] = {
            "showcase_roles": existing + role_ids,
            "showcase_page": page,
        }
        self._save_config()

    def _clear_tracked_roles(self, guild_id: int):
        gid = str(guild_id)
        self._config.pop(gid, None)
        self._save_config()

    async def cog_load(self):
        """Clean up any leftover showcase roles from prior runs."""
        self.bot.loop.create_task(self._startup_cleanup())

    async def _startup_cleanup(self):
        await self.bot.wait_until_ready()
        for gid_str, data in list(self._config.items()):
            role_ids = data.get("showcase_roles", [])
            if not role_ids:
                continue
            guild = self.bot.get_guild(int(gid_str))
            if not guild:
                continue
            log.info(f"Cleaning up {len(role_ids)} leftover showcase roles in {guild.name}")
            await self._delete_roles(guild, role_ids)
            self._clear_tracked_roles(guild.id)

    # ─── Helpers ───

    async def _delete_roles(self, guild: discord.Guild, role_ids: list[int]):
        """Delete a list of roles by ID with rate-limit spacing."""
        for role_id in role_ids:
            role = guild.get_role(role_id)
            if role:
                try:
                    await role.delete(reason="Showcase cleanup")
                    await asyncio.sleep(ROLE_DELETE_DELAY)
                except Exception as e:
                    log.warning(f"Failed to delete showcase role {role_id}: {e}")

    async def _create_and_post_section(self, channel, guild, section: int):
        """Create 50 roles for a section, post the embed, then delete the roles."""
        start_idx = section * COLORS_PER_PAGE
        section_gradients = GRADIENTS[start_idx:start_idx + COLORS_PER_PAGE]

        created_role_ids: list[int] = []
        created_mentions: list[tuple[int, str]] = []  # (global_num, mention)
        failed = 0

        # Progress message
        progress = discord.Embed(
            title=f"Creating Gradients {start_idx + 1}-{start_idx + COLORS_PER_PAGE}...",
            description=f"Creating **0/{COLORS_PER_PAGE}** gradient roles — please wait.",
            color=EMBED_COLOR,
        )
        progress_msg = await channel.send(embed=progress)

        for i, (c1, c2) in enumerate(section_gradients):
            global_num = start_idx + i + 1
            try:
                data = await _api_create_role(
                    self.bot.http,
                    guild.id,
                    name=f"{ROLE_NAME} #{global_num}",
                    c1=c1, c2=c2,
                    reason=f"Rolecolor showcase section {section + 1}",
                )
                rid = int(data["id"])
                created_role_ids.append(rid)
                created_mentions.append((global_num, f"<@&{rid}>"))
            except discord.HTTPException as e:
                log.warning(f"Failed to create showcase role #{global_num}: {e}")
                failed += 1

            await asyncio.sleep(ROLE_CREATE_DELAY)

            # Progress update every 10 roles
            if (i + 1) % 10 == 0:
                try:
                    progress.description = (
                        f"Creating **{i + 1}/{COLORS_PER_PAGE}** "
                        "gradient roles — please wait."
                    )
                    await progress_msg.edit(embed=progress)
                except Exception:
                    pass

        # Persist role IDs so they can be cleaned up if the bot restarts
        self._track_roles(guild.id, created_role_ids, section)

        # Build showcase embed
        lines = [f"**{num}.** {mention}" for num, mention in created_mentions]
        desc = "\n".join(lines)
        if failed:
            desc += f"\n\n*{failed} role(s) failed to create.*"

        showcase = discord.Embed(
            title=f"Role Gradients — {start_idx + 1} to {start_idx + COLORS_PER_PAGE}",
            description=desc,
            color=EMBED_COLOR,
        )
        showcase.set_footer(text="Use 'Apply Gradient to Role' and enter the number you want")

        await progress_msg.edit(embed=showcase)

        # Delete the showcase roles now that the embed is posted
        await self._delete_roles(guild, created_role_ids)
        self._clear_tracked_roles(guild.id)

    async def showcase_all(self, interaction: discord.Interaction):
        """Run all 3 sections: create 50, post embed, delete, repeat."""
        guild = interaction.guild

        if guild.id in self._creating:
            return await interaction.response.send_message(
                "Already running a showcase — please wait.", ephemeral=True
            )

        self._creating.add(guild.id)
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            await interaction.followup.send(
                embed=discord.Embed(
                    description=(
                        "Starting showcase — creating **3 sections** of 50 gradient roles each.\n"
                        "Each section will be posted to this channel and the preview roles "
                        "will be cleaned up automatically."
                    ),
                    color=EMBED_COLOR,
                ),
                ephemeral=True,
            )

            for section in range(TOTAL_PAGES):
                await self._create_and_post_section(
                    interaction.channel, guild, section
                )

        except Exception as e:
            log.error(f"Error in showcase_all: {e}", exc_info=True)
            try:
                await interaction.followup.send(
                    f"Error during showcase: {e}", ephemeral=True
                )
            except Exception:
                pass
        finally:
            self._creating.discard(guild.id)

    # ─── Slash Command ───

    @app_commands.command(
        name="rolecolor_panel",
        description="Post the role color gradient showcase panel (Admin only)",
    )
    @app_commands.default_permissions(administrator=True)
    async def rolecolor_panel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Role Gradient Showcase",
            description=(
                "Browse **150 curated 2-color gradients** — "
                "a mix of classic, neon, pastel, and bold combos.\n\n"
                "**Showcase All Gradients** — Creates 3 embeds of 50 gradient previews "
                "each. Roles are created temporarily and deleted right after posting.\n\n"
                "**Apply Gradient to Role** — Enter a gradient number (1-150) "
                "and pick a role to apply it to."
            ),
            color=EMBED_COLOR,
        )
        await interaction.response.send_message(
            embed=embed, view=RoleColorPanelView(self), ephemeral=True
        )


async def setup(bot: commands.Bot):
    cog = RoleColor(bot)
    await bot.add_cog(cog)
