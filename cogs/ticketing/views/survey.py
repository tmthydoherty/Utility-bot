import discord
import asyncio
import logging
from typing import Dict, Any, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from discord.ext.commands import Bot
    from ..cog import TicketSystem

logger = logging.getLogger("ticketing_cog")


class StartSurveyView(discord.ui.View):
    def __init__(self, topic: Dict, bot: "Bot"):
        super().__init__(timeout=None)
        self.topic = topic
        self.bot = bot
        topic_name = topic.get('name', 'unknown')[:80]
        self.children[0].custom_id = f"start_survey::{topic_name}"

    @discord.ui.button(label="Start", style=discord.ButtonStyle.primary, emoji="\U0001f4dd")
    async def start_survey(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog: "TicketSystem" = self.bot.get_cog("TicketSystem")
        if not cog:
            return await interaction.response.send_message("An error occurred.", ephemeral=True)

        await interaction.response.send_message(
            "The survey is starting in your DMs. Please check your direct messages.",
            ephemeral=True, delete_after=10
        )
        asyncio.create_task(cog.conduct_survey_flow(interaction, self.topic))


class ResumeOrRestartView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", topic: Dict, session: Dict, interaction: discord.Interaction):
        super().__init__(timeout=60)
        self.cog = cog
        self.topic = topic
        self.session = session
        self.original_interaction = interaction
        self.choice = None

    @discord.ui.button(label="Resume", style=discord.ButtonStyle.success)
    async def resume_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "resume"
        await interaction.response.defer()
        self.stop()

    @discord.ui.button(label="Start Over", style=discord.ButtonStyle.primary)
    async def restart_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "restart"
        await interaction.response.defer()
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "cancel"
        await interaction.response.defer()
        self.stop()


class SurveyTargetView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", topic_data: Dict):
        super().__init__(timeout=180)
        self.cog = cog
        self.topic_data = topic_data
        self.selected_roles: List[discord.Role] = []
        self.selected_users: List[Union[discord.Member, discord.User]] = []

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select roles to send to...", max_values=25, row=0)
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        self.selected_roles = list(select.values)
        await interaction.response.defer()

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select members to send to...", max_values=25, row=1)
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_users = list(select.values)
        await interaction.response.defer()

    @discord.ui.button(label="Send Survey", style=discord.ButtonStyle.success, row=2)
    async def send(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        targets = set()
        for role in self.selected_roles:
            targets.update(role.members)
        targets.update(self.selected_users)

        if not targets:
            return await interaction.followup.send("You must select at least one role or member.", ephemeral=True)

        embed = discord.Embed(
            title=f"Survey Invitation: {self.topic_data.get('label')}",
            description=f"You have been invited to participate in a survey from **{interaction.guild.name}**. Please click the button below to begin.",
            color=discord.Color.blue()
        )
        view = StartSurveyView(self.topic_data, self.cog.bot)

        success_count = 0
        fail_count = 0
        for target in targets:
            if target.bot:
                continue
            try:
                await target.send(embed=embed, view=view)
                success_count += 1
                await asyncio.sleep(0.1)
            except (discord.Forbidden, discord.HTTPException):
                fail_count += 1

        await interaction.followup.send(
            f"Survey sent!\n- **Successful DMs:** {success_count}\n- **Failed DMs (privacy settings):** {fail_count}",
            ephemeral=True
        )
        try:
            await interaction.edit_original_response(content="Survey sent.", view=None)
        except discord.NotFound:
            pass
