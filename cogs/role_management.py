import discord
from discord.ext import commands
from discord import app_commands
import logging

logger = logging.getLogger(__name__)

EMBED_COLOR = 0x5865F2


# ─── Step Views ───

class SelectTriggerRolesView(discord.ui.View):
    """Step 1: Select the trigger roles (users must have ALL of these)."""
    def __init__(self):
        super().__init__(timeout=120)
        self.trigger_roles: list[discord.Role] = []

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select trigger roles...",
        min_values=2,
        max_values=10,
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        self.trigger_roles = list(select.values)
        role_list = ", ".join(r.mention for r in self.trigger_roles)
        view = SelectGainRoleView(self.trigger_roles)
        embed = discord.Embed(
            title="Role Management — Step 2",
            description=f"**Trigger roles:** {role_list}\n\nNow select the role users will **gain**:",
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=view)


class SelectGainRoleView(discord.ui.View):
    """Step 2: Select the role to grant."""
    def __init__(self, trigger_roles: list[discord.Role]):
        super().__init__(timeout=120)
        self.trigger_roles = trigger_roles

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select the role to grant...",
        min_values=1,
        max_values=1,
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        gain_role = select.values[0]
        role_list = ", ".join(r.mention for r in self.trigger_roles)
        view = SelectLoseRoleView(self.trigger_roles, gain_role)
        embed = discord.Embed(
            title="Role Management — Step 3",
            description=(
                f"**Trigger roles:** {role_list}\n"
                f"**Gain role:** {gain_role.mention}\n\n"
                "Optionally select a trigger role for users to **lose**, or skip:"
            ),
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=view)


class SelectLoseRoleView(discord.ui.View):
    """Step 3: Optionally select a trigger role to remove, or skip."""
    def __init__(self, trigger_roles: list[discord.Role], gain_role: discord.Role):
        super().__init__(timeout=120)
        self.trigger_roles = trigger_roles
        self.gain_role = gain_role

        # Build a dropdown of trigger roles to pick from
        options = [
            discord.SelectOption(label=r.name[:100], value=str(r.id))
            for r in trigger_roles
        ]
        self._lose_select = discord.ui.Select(
            placeholder="Select a role to remove (optional)...",
            options=options[:25],
            min_values=0,
            max_values=1,
            row=0,
        )
        self._lose_select.callback = self._on_lose_select
        self.add_item(self._lose_select)

    async def _on_lose_select(self, interaction: discord.Interaction):
        lose_role = None
        if self._lose_select.values:
            role_id = int(self._lose_select.values[0])
            lose_role = discord.utils.get(self.trigger_roles, id=role_id)
        await self._show_confirmation(interaction, lose_role)

    @discord.ui.button(label="Skip (no role removal)", style=discord.ButtonStyle.secondary, row=1)
    async def skip_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._show_confirmation(interaction, None)

    async def _show_confirmation(self, interaction: discord.Interaction, lose_role: discord.Role | None):
        role_list = ", ".join(r.mention for r in self.trigger_roles)
        lose_text = lose_role.mention if lose_role else "None"
        view = ConfirmView(self.trigger_roles, self.gain_role, lose_role)
        embed = discord.Embed(
            title="Role Management — Confirm",
            description=(
                f"**Trigger roles:** {role_list}\n"
                f"**Gain:** {self.gain_role.mention}\n"
                f"**Lose:** {lose_text}\n\n"
                "All members who have **every** trigger role will be affected.\n"
                "Press **Execute** to proceed or **Cancel** to abort."
            ),
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=view)


class ConfirmView(discord.ui.View):
    """Step 4: Confirm and execute."""
    def __init__(self, trigger_roles: list[discord.Role], gain_role: discord.Role, lose_role: discord.Role | None):
        super().__init__(timeout=120)
        self.trigger_roles = trigger_roles
        self.gain_role = gain_role
        self.lose_role = lose_role

    @discord.ui.button(label="Execute", style=discord.ButtonStyle.success)
    async def execute_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        trigger_ids = {r.id for r in self.trigger_roles}
        guild = interaction.guild
        if not guild.chunked:
            await guild.chunk()
        affected = []
        errors = []

        for member in guild.members:
            member_role_ids = {r.id for r in member.roles}
            if not trigger_ids.issubset(member_role_ids):
                continue

            try:
                await member.add_roles(self.gain_role, reason="Role Management shuffle")
                if self.lose_role:
                    await member.remove_roles(self.lose_role, reason="Role Management shuffle")
                affected.append(member)
            except discord.Forbidden:
                errors.append(f"Missing perms for {member.mention}")
            except discord.HTTPException as e:
                errors.append(f"Error for {member.mention}: {e}")

        lose_text = f" and lost {self.lose_role.mention}" if self.lose_role else ""
        desc = f"**{len(affected)}** member(s) gained {self.gain_role.mention}{lose_text}."
        if errors:
            desc += f"\n\n**Errors ({len(errors)}):**\n" + "\n".join(errors[:10])

        embed = discord.Embed(
            title="Role Management — Complete",
            description=desc,
            color=discord.Color.green() if not errors else discord.Color.orange(),
        )
        await interaction.edit_original_response(embed=embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="Role Management — Cancelled",
            description="No changes were made.",
            color=discord.Color.red(),
        )
        await interaction.response.edit_message(embed=embed, view=None)


# ─── Admin Panel View ───

class AdminPanelView(discord.ui.View):
    """Main admin panel with a button to start the role shuffle flow."""
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.button(label="Role Shuffle", style=discord.ButtonStyle.primary, emoji="🔀")
    async def shuffle_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SelectTriggerRolesView()
        embed = discord.Embed(
            title="Role Management — Step 1",
            description="Select the **trigger roles**. Members must have **all** selected roles to be affected:",
            color=EMBED_COLOR,
        )
        await interaction.response.edit_message(embed=embed, view=view)


# ─── Cog ───

class RoleManagement(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="role_management", description="Open the role management admin panel")
    @app_commands.default_permissions(manage_roles=True)
    async def role_management(self, interaction: discord.Interaction):
        view = AdminPanelView()
        embed = discord.Embed(
            title="Role Management",
            description="Use the button below to start a role shuffle operation.",
            color=EMBED_COLOR,
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleManagement(bot))
