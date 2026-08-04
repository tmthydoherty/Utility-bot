import discord
from discord import ui
import asyncio
import io
import logging
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple, TYPE_CHECKING

from .models import (
    GameConfig, PlayerStats, QueueState, MatchState, QueueType, CaptainSelection,
    Team, Suspension,
    COLOR_WHITE, COLOR_SUCCESS, COLOR_WARNING, COLOR_RED, COLOR_BLUE, COLOR_NEUTRAL,
    RIVALS_ROLES, is_valorant_game, is_rivals_game,
    is_overwatch_game, OW_ROLES, OW_ROLE_EMOJI,
    OW_ROLE_EMOJI_ID, OW_ROLE_DISPLAY_ORDER,
    safe_display_name, sanitize_for_codeblock, generate_short_id,
    normalize_ign, normalize_rivals_role, _parse_tracker_url,
)
from .database import DatabaseHelper
from .views_settings import (
    BaseMatchView, ConfirmView, GameSelectDropdown, PenaltySettingsView,
    BackButton, SettingsPage,
    _resolve_role_emojis, seed_mmr_for_rank,
)

if TYPE_CHECKING:
    from .cog import CustomMatch

logger = logging.getLogger('cogs.custommatch')


# =============================================================================
# ADMIN PANEL
# =============================================================================

class SubTypeSelectView(discord.ui.View):
    """Sub-view to choose between standard sub and no-reshuffle sub."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Standard Sub", style=discord.ButtonStyle.primary, row=0)
    async def standard_sub(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.edit_message(content="No active matches.", view=None)
            return
        view = await MatchSelectView.create(self.cog, matches, self._show_standard)
        await interaction.response.edit_message(content="Select a match:", view=view)

    async def _show_standard(self, interaction: discord.Interaction, match_id: int):
        view = await SubstituteOutPlayerView.create(self.cog, match_id, interaction.guild)
        if view:
            await interaction.response.edit_message(content="Select the player to replace:", view=view)
        else:
            await interaction.response.edit_message(content="No players found in this match.", view=None)

    @discord.ui.button(label="No Reshuffle", style=discord.ButtonStyle.secondary, row=0)
    async def no_reshuffle_sub(self, interaction: discord.Interaction, button: discord.ui.Button):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await interaction.response.edit_message(content="No active matches.", view=None)
            return
        view = await MatchSelectView.create(self.cog, matches, self._show_no_reshuffle)
        await interaction.response.edit_message(content="Select a match:", view=view)

    async def _show_no_reshuffle(self, interaction: discord.Interaction, match_id: int):
        view = await SubstituteOutPlayerView.create(self.cog, match_id, interaction.guild, skip_reshuffle=True)
        if view:
            await interaction.response.edit_message(content="Select the player to replace (no reshuffle):", view=view)
        else:
            await interaction.response.edit_message(content="No players found in this match.", view=None)


class QueueTypeSelectView(discord.ui.View):
    """Select Standard or Arcade queue type when starting from the admin panel."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

    @discord.ui.button(label="Standard", style=discord.ButtonStyle.primary, row=0)
    async def standard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        panel = AdminPanelView(self.cog)
        await panel.do_queue_start(interaction, self.game.game_id, is_secondary=False)

    @discord.ui.button(label="Arcade", style=discord.ButtonStyle.secondary, row=0)
    async def arcade(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        panel = AdminPanelView(self.cog)
        await panel.do_queue_start(interaction, self.game.game_id, is_secondary=True)


class AdminPage(SettingsPage):
    """Base for every /cm_panel screen.

    Reuses the /cm_settings page machinery: one ephemeral message that every
    screen draws onto, a Back button that redraws the parent with fresh data,
    ancestor timeouts kept alive while a nested page is in use, and the admin
    role re-checked on every click. Before this the panel was 18 flat buttons
    that each spawned their own ephemeral, so a single action left a trail of
    dead messages behind it.
    """

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(title=self.page_title, description=self.page_blurb,
                             color=COLOR_NEUTRAL)

    page_title = "Custom Matches Admin"
    page_blurb = None

    async def render(self, interaction: discord.Interaction, *, flash: Optional[str] = None):
        # Same as SettingsPage.render but also clears any content left behind by
        # a selector view, which would otherwise linger above the embed.
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = False
        embed = await self.build_embed(interaction.guild)
        if flash:
            embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        await interaction.response.edit_message(content=None, embed=embed, view=self)
        self.message = interaction.message

    async def render_original(self, interaction: discord.Interaction, flash: str):
        """Redraw this page onto the panel message after the interaction has
        already been answered — the tail of an action that deferred or edited on
        its way through. edit_original_response targets the same message, so the
        result lands on the panel instead of stacking another ephemeral."""
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = False
        embed = await self.build_embed(interaction.guild)
        embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        try:
            await interaction.edit_original_response(content=None, embed=embed, view=self)
        except discord.HTTPException:
            await interaction.followup.send(flash, ephemeral=True)

    async def show_child(self, interaction: discord.Interaction, view: discord.ui.View,
                         title: str, description: Optional[str] = None):
        """Draw a selector view onto the panel message with a way back.

        Takes any plain discord.ui.View — the existing selector views are not
        ExpiringViews, so this adds the Back button directly rather than going
        through SettingsPage.show_view. Their stock 60s timeout is stretched to
        the panel's so a selector can't go dead while the page around it is
        still live.
        """
        if (view.timeout or 0) < 300:
            view.timeout = 300
        if not any(isinstance(c, BackButton) for c in view.children):
            view.add_item(BackButton(self, row=4))
        embed = discord.Embed(title=title, description=description, color=COLOR_NEUTRAL)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class AdminActions:
    """Every /cm_panel action, shared by the pages that surface them.

    A mixin rather than a base class so the child pages inherit the handlers
    without also inheriting the top-level page's buttons. Because the handler
    runs with ``self`` bound to whichever page invoked it, ``show_child``
    attaches Back to that page — so Back returns where the admin actually was.
    """


    async def ow_role_ranks(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        games = [g for g in await DatabaseHelper.get_all_games() if is_overwatch_game(g)]
        if not games:
            await self.render(interaction, flash="⚠️ No Overwatch game configured.")
            return
        view = OWRoleRanksUserSelectView(self.cog, games)
        await self.show_child(
            interaction, view, "Overwatch Role Ranks",
            "Rank or re-rank a player's individual roles. Roles you don't pick "
            "keep the rating they already have."
        )

    async def substitute(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        view = SubTypeSelectView(self.cog)
        await self.show_child(interaction, view, "Substitute — select sub type")

    async def show_sub_modal(self, interaction: discord.Interaction, match_id: int):
        view = await SubstituteOutPlayerView.create(self.cog, match_id, interaction.guild)
        if view:
            await interaction.response.edit_message(content="Select the player to replace:", view=view)
        else:
            await interaction.response.edit_message(content="No players found in this match.", view=None)

    async def show_sub_no_reshuffle_modal(self, interaction: discord.Interaction, match_id: int):
        view = await SubstituteOutPlayerView.create(self.cog, match_id, interaction.guild, skip_reshuffle=True)
        if view:
            await interaction.response.edit_message(content="Select the player to replace (no reshuffle):", view=view)
        else:
            await interaction.response.edit_message(content="No players found in this match.", view=None)

    async def swap(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await self.render(interaction, flash="⚠️ No active matches.")
            return
        view = await MatchSelectView.create(self.cog, matches, self.show_swap_modal)
        await self.show_child(interaction, view, "Substitute — select a match")

    async def show_swap_modal(self, interaction: discord.Interaction, match_id: int):
        view = await SwapPlayer1View.create(self.cog, match_id, interaction.guild)
        if view:
            await interaction.response.edit_message(content="Select the first player to swap:", view=view)
        else:
            await interaction.response.edit_message(content="No players found in this match.", view=None)

    async def force_winner(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await self.render(interaction, flash="⚠️ No active matches.")
            return
        view = await MatchSelectView.create(self.cog, matches, self.show_force_winner)
        await self.show_child(interaction, view, "Force Winner — select a match")

    async def show_force_winner(self, interaction: discord.Interaction, match_id: int):
        view = ForceWinnerView(self.cog, match_id)
        await interaction.response.edit_message(content="Select the winning team:", view=view)

    async def cancel_match(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        matches = await DatabaseHelper.get_active_matches()
        if not matches:
            await self.render(interaction, flash="⚠️ No active matches.")
            return
        view = await MatchSelectView.create(self.cog, matches, self.confirm_cancel)
        await self.show_child(interaction, view, "Cancel Match — select a match")

    async def confirm_cancel(self, interaction: discord.Interaction, match_id: int):
        match = await DatabaseHelper.get_match(match_id)
        short_id = match.get("short_id") or str(match_id) if match else str(match_id)
        modal = CancelMatchModal(self.cog, match_id, short_id)
        await interaction.response.send_modal(modal)

    # Row 1: Advanced match/queue controls
    async def change_winner(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        modal = ChangeWinnerModal(self.cog)
        await interaction.response.send_modal(modal)

    async def force_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_force_start))
        await self.show_child(interaction, view, "Force Start — select a game")

    async def do_force_start(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        # Find active queue for this game (waiting or ready_check)
        queue_state = None
        for qid, qs in self.cog.queues.items():
            if qs.game_id == game_id and qs.state in ("waiting", "ready_check"):
                queue_state = qs
                break

        if not queue_state:
            # Check if it's already starting (all readied up)
            for qid, qs in self.cog.queues.items():
                if qs.game_id == game_id and qs.state == "starting_match":
                    await self.render(
                        interaction,
                        flash="⚠️ Match is already starting (all players readied up).")
                    return
            await self.render(interaction, flash="⚠️ No active queue found for this game.")
            return

        if len(queue_state.players) < game.player_count and queue_state.state == "waiting":
            await self.render(
                interaction,
                flash=f"⚠️ Queue is not full ({len(queue_state.players)}/{game.player_count}).")
            return

        if len(queue_state.players) < game.player_count and queue_state.state == "ready_check":
            # During ready check, some players may have dropped — allow force start with remaining players
            # but require at least enough for two teams
            if len(queue_state.players) < 2:
                await self.render(
                    interaction,
                    flash=f"⚠️ Not enough players remaining ({len(queue_state.players)}) "
                          "to start a match.")
                return

        # Cancel ready check task if active
        if queue_state.queue_id in self.cog.ready_check_tasks:
            self.cog.ready_check_tasks[queue_state.queue_id].cancel()
            del self.cog.ready_check_tasks[queue_state.queue_id]

        # Mark all remaining players as ready
        for pid in queue_state.players:
            queue_state.players[pid] = True

        # Transition state to starting_match so proceed_to_match accepts it
        queue_state.state = "starting_match"
        async with DatabaseHelper._get_db() as db:
            await db.execute(
                "UPDATE active_queues SET state = 'starting_match' WHERE queue_id = ?",
                (queue_state.queue_id,)
            )
            await db.commit()

        await interaction.response.defer()
        channel = interaction.guild.get_channel(queue_state.channel_id)
        if channel:
            await self.cog.proceed_to_match(channel, game, queue_state)
        await self.render_original(interaction, "✅ Match force started.")
        await self.cog.log_action(
            interaction.guild,
            f"Match force started for **{game.name}** by {interaction.user.display_name} ({len(queue_state.players)} players)"
        )

    async def clear_queue(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.confirm_clear_queue))
        await self.show_child(interaction, view, "Clear Queue — select a game")

    async def confirm_clear_queue(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = ConfirmView()
        # Deliberately not show_child: this one blocks on view.wait(), so a Back
        # button would leave that coroutine parked until the timeout and then
        # redraw the panel over whatever the admin had navigated to since.
        # Cancel is the way out of a confirmation.
        embed = discord.Embed(
            title="Clear Queue",
            description=f"Are you sure you want to clear the queue for **{game.name}**?",
            color=COLOR_WARNING,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)
        await view.wait()
        if view.value:
            # Find and clear queue
            for qid, qs in list(self.cog.queues.items()):
                if qs.game_id == game_id:
                    # Cancel any running ready check or grace period task
                    if qid in self.cog.ready_check_tasks:
                        self.cog.ready_check_tasks[qid].cancel()
                        del self.cog.ready_check_tasks[qid]
                    # Reset state and players
                    qs.state = "waiting"
                    qs.players.clear()
                    qs.grace_timers.clear()
                    await DatabaseHelper.clear_queue(qid)
                    await DatabaseHelper.clear_queue_subscribers(qid)
                    # Also reset state in DB
                    async with DatabaseHelper._get_db() as db:
                        await db.execute(
                            "UPDATE active_queues SET state = 'waiting', ready_check_started = NULL WHERE queue_id = ?",
                            (qid,)
                        )
                        await db.commit()
                    # Update embed
                    channel = interaction.guild.get_channel(qs.channel_id)
                    if channel and qs.message_id:
                        try:
                            msg = await channel.fetch_message(qs.message_id)
                            embed = await self.cog.create_queue_embed(game, qs, interaction.guild)
                            new_view = QueueView(self.cog, game_id, qid)
                            await msg.edit(embed=embed, view=new_view)
                        except Exception:
                            pass
            await self.render_original(interaction, f"✅ Queue cleared for **{game.name}**.")
        else:
            await self.render_original(interaction, "Cancelled.")

    async def queue_start(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self._queue_start_game_selected))
        await self.show_child(interaction, view, "New Queue — select a game")

    async def _queue_start_game_selected(self, interaction: discord.Interaction, game_id: int):
        """After game selection, show Standard/Arcade choice if secondary is enabled."""
        game = await DatabaseHelper.get_game(game_id)
        if game.secondary_queue_enabled:
            view = QueueTypeSelectView(self.cog, game)
            await interaction.response.edit_message(content=f"**{game.name}** — Select queue type:", view=view)
        else:
            await self.do_queue_start(interaction, game_id, is_secondary=False)

    async def do_queue_start(self, interaction: discord.Interaction, game_id: int, is_secondary: bool = False):
        game = await DatabaseHelper.get_game(game_id)
        if not interaction.response.is_done():
            await interaction.response.defer()

        # Resolve target channel
        if is_secondary:
            target_channel_id = game.secondary_queue_channel_id or game.queue_channel_id
            target_channel = interaction.guild.get_channel(target_channel_id) if target_channel_id else interaction.channel
        else:
            target_channel_id = game.queue_channel_id
            target_channel = interaction.guild.get_channel(target_channel_id) if target_channel_id else interaction.channel

        if not target_channel:
            await self.render_original(interaction, "⚠️ Queue channel not found.")
            return

        await self.cog.start_queue(target_channel, game, is_secondary=is_secondary)
        mode_label = (game.secondary_queue_name or "Arcade") if is_secondary else "Standard"
        await self.render_original(interaction, f"✅ {mode_label} queue started for **{game.name}**.")

    # Row 2: Player management
    async def manage_suspensions(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SuspensionManagementView(self.cog)
        await self.show_child(interaction, view, "Suspensions")

    async def remove_from_queue(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_queue_players))
        await self.show_child(interaction, view, "Remove from Queue — select a game")

    async def show_queue_players(self, interaction: discord.Interaction, game_id: int):
        # Find queue for this game — pick the one with the most players
        queue_state = None
        for qid, qs in self.cog.queues.items():
            if qs.game_id == game_id and qs.state in ("waiting", "ready_check") and qs.players:
                if queue_state is None or len(qs.players) > len(queue_state.players):
                    queue_state = qs

        if not queue_state or not queue_state.players:
            await interaction.response.edit_message(content="No players in queue.", view=None)
            return

        view = QueuePlayerRemoveView(self.cog, game_id, queue_state, interaction.guild)
        await interaction.response.edit_message(content="Select a player to remove:", view=view)

    async def adjust_wl(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        view = AdjustWLUserSelectView(self.cog)
        await self.show_child(interaction, view, "Adjust W/L — select a user")

    async def set_player_mmr_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = SetMMRUserSelectView(self.cog, games)
        await self.show_child(interaction, view, "Set Player MMR — select a user and game")

    async def view_player_mmr_btn(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = MMRViewUserSelectView(self.cog, games)
        await self.show_child(interaction, view, "User View — select a user and game")

    async def set_platform_btn(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        games = [g for g in await DatabaseHelper.get_all_games() if g.pc_enabled]
        if not games:
            await interaction.response.send_message(
                "No PC-enabled games. Enable PC players in Game Settings > Toggles first.",
                ephemeral=True
            )
            return
        view = SetPlatformUserSelectView(self.cog, games)
        await self.show_child(interaction, view, "Set Platform — select a user and game")

    # Row 3: Setup and utilities
    async def manage_ign(self, interaction: discord.Interaction, button: discord.ui.Button = None):
        view = AdminIGNUserSelectView(self.cog)
        await self.show_child(interaction, view, "IGN — select a user")

    async def setup_new_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = SetupUserGameSelectView(self.cog, games)
        await self.show_child(interaction, view, "Setup New User — select a game")

    async def start_discussion(self, interaction: discord.Interaction, button: discord.ui.Button):
        parent_id = await DatabaseHelper.get_config("cm_discussion_parent_channel_id")
        if not parent_id:
            await interaction.response.send_message(
                "No discussion parent channel configured. Set one in Settings > Channels > Discussion Channel.",
                ephemeral=True
            )
            return
        parent_channel = interaction.guild.get_channel(int(parent_id))
        if not parent_channel:
            await self.render(interaction, flash="⚠️ Discussion parent channel not found.")
            return
        view = PanelDiscussionUserSelectView(self.cog, parent_channel)
        await self.show_child(interaction, view, "Start Discussion — select a user")

    async def penalty_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await self.render(interaction, flash="⚠️ No games configured.")
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_penalty_settings))
        await self.show_child(interaction, view, "Penalty Settings — select a game")

    async def show_penalty_settings(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = PenaltySettingsView(self.cog, game)
        embed = discord.Embed(title=f"{game.name} Penalty Settings", color=COLOR_NEUTRAL)
        embed.add_field(
            name="Timeout Penalties",
            value=(
                f"1st: {game.penalty_1st_minutes} min\n"
                f"2nd: {game.penalty_2nd_minutes} min\n"
                f"3rd+: {game.penalty_3rd_minutes} min\n"
                f"Decay: {game.penalty_decay_days} days"
            ),
            inline=True,
        )
        embed.add_field(
            name="Decline Penalties",
            value=(
                f"1st: {game.decline_1st_minutes} min\n"
                f"2nd: {game.decline_2nd_minutes} min\n"
                f"3rd+: {game.decline_3rd_minutes} min"
            ),
            inline=True,
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)



class AdminPanelView(AdminActions, AdminPage):
    """Top level of /cm_panel.

    Match, queue and user actions live on the child pages below; everything
    else stays here.
    """

    page_title = "Custom Matches Admin Panel"

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(cog, timeout=300)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(title=self.page_title, color=COLOR_NEUTRAL)

        matches = await DatabaseHelper.get_active_matches()
        if matches:
            match_lines = []
            for m in matches:
                game = await DatabaseHelper.get_game(m["game_id"])
                game_name = game.name if game else "Unknown"
                short_id = m.get("short_id") or str(m["match_id"])
                match_lines.append(f"Match {short_id} - {game_name}")
            embed.add_field(name="Active Matches", value="\n".join(match_lines), inline=False)
        else:
            embed.add_field(name="Active Matches", value="No active matches.", inline=False)

        # Show queues in ready-check state so admins know Force Start applies to them
        ready_check_lines = []
        for qid, qs in self.cog.queues.items():
            if qs.state == "ready_check":
                g = await DatabaseHelper.get_game(qs.game_id)
                g_name = g.name if g else f"Game {qs.game_id}"
                ready_check_lines.append(f"{g_name} — {len(qs.players)} players readying up")
        if ready_check_lines:
            embed.add_field(name="Active Ready Checks", value="\n".join(ready_check_lines), inline=False)
        return embed

    # Row 0: grouping pages
    @discord.ui.button(label="Queue", style=discord.ButtonStyle.primary, row=0)
    async def queue_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AdminQueuePage(self.cog, self).render(interaction)

    @discord.ui.button(label="User", style=discord.ButtonStyle.primary, row=0)
    async def user_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AdminUserPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Match", style=discord.ButtonStyle.primary, row=0)
    async def match_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AdminMatchPage(self.cog, self).render(interaction)

    # Row 1: player setup
    @discord.ui.button(label="Setup New User", style=discord.ButtonStyle.success, row=1)
    async def setup_new_user_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.setup_new_user(interaction, button)

    @discord.ui.button(label="Set Player MMR", style=discord.ButtonStyle.secondary, row=1)
    async def set_player_mmr_btn_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.set_player_mmr_btn(interaction, button)

    @discord.ui.button(label="OW Role Ranks", style=discord.ButtonStyle.secondary, row=1)
    async def ow_role_ranks_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.ow_role_ranks(interaction, button)

    # Row 2: everything else
    @discord.ui.button(label="Force Start", style=discord.ButtonStyle.secondary, row=2)
    async def force_start_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.force_start(interaction, button)

    @discord.ui.button(label="Suspensions", style=discord.ButtonStyle.secondary, row=2)
    async def manage_suspensions_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.manage_suspensions(interaction, button)

    @discord.ui.button(label="Penalty Settings", style=discord.ButtonStyle.secondary, row=2)
    async def penalty_settings_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.penalty_settings(interaction, button)

    @discord.ui.button(label="Start Discussion", style=discord.ButtonStyle.primary, row=2)
    async def start_discussion_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.start_discussion(interaction, button)


class AdminQueuePage(AdminActions, AdminPage):
    """Queue controls."""

    page_title = "Queue"
    page_blurb = "Start, clear, or pull a player out of a queue."

    @discord.ui.button(label="New Queue", style=discord.ButtonStyle.success, row=0)
    async def new_queue_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.queue_start(interaction, button)

    @discord.ui.button(label="Clear Queue", style=discord.ButtonStyle.danger, row=0)
    async def clear_queue_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.clear_queue(interaction, button)

    @discord.ui.button(label="Remove from Queue", style=discord.ButtonStyle.secondary, row=0)
    async def remove_from_queue_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.remove_from_queue(interaction, button)


class AdminUserPage(AdminActions, AdminPage):
    """Per-player admin actions."""

    page_title = "User"
    page_blurb = "Platform, IGN, record and overview for a single player."

    @discord.ui.button(label="Set Platform", style=discord.ButtonStyle.secondary, row=0)
    async def set_platform_btn_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.set_platform_btn(interaction, button)

    @discord.ui.button(label="IGN", style=discord.ButtonStyle.secondary, row=0)
    async def ign_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.manage_ign(interaction, button)

    @discord.ui.button(label="User View", style=discord.ButtonStyle.secondary, row=0)
    async def user_view_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.view_player_mmr_btn(interaction, button)

    @discord.ui.button(label="Adjust W/L", style=discord.ButtonStyle.secondary, row=0)
    async def adjust_wl_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.adjust_wl(interaction, button)


class AdminMatchPage(AdminActions, AdminPage):
    """Live match controls."""

    page_title = "Match"
    page_blurb = "Substitutions, swaps and result corrections for a live match."

    @discord.ui.button(label="Sub Player", style=discord.ButtonStyle.primary, row=0)
    async def sub_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.substitute(interaction, button)

    @discord.ui.button(label="Swap Players", style=discord.ButtonStyle.primary, row=0)
    async def swap_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.swap(interaction, button)

    @discord.ui.button(label="Force Winner", style=discord.ButtonStyle.danger, row=1)
    async def force_winner_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.force_winner(interaction, button)

    @discord.ui.button(label="Change Winner", style=discord.ButtonStyle.secondary, row=1)
    async def change_winner_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, button)

    @discord.ui.button(label="Cancel Match", style=discord.ButtonStyle.danger, row=1)
    async def cancel_match_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cancel_match(interaction, button)


class AdminIGNUserSelectView(discord.ui.View):
    """Admin view for selecting a user to view/edit their IGN."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        igns = await DatabaseHelper.get_player_all_igns(user.id)

        if not igns:
            # No IGNs set — offer to set one
            games = await DatabaseHelper.get_all_games()
            if not games:
                await interaction.response.edit_message(content="No games configured.", view=None)
                return

            async def show_set_ign_modal(inter: discord.Interaction, game_id: int):
                game = await DatabaseHelper.get_game(game_id)
                modal = AdminSetIGNModal(self.cog, user.id, game_id, game.name)
                await inter.response.send_modal(modal)

            view = discord.ui.View(timeout=60)
            view.add_item(GameSelectDropdown(games, show_set_ign_modal))
            await interaction.response.edit_message(
                content=f"**{user.display_name}** has no IGNs set. Select a game to set one:",
                view=view
            )
            return

        # Show all IGNs and let admin pick a game to edit
        lines = [f"**{user.display_name}**'s IGNs:"]
        for game_id, game_name, ign in igns:
            lines.append(f"- **{game_name}**: `{ign}`")

        games = await DatabaseHelper.get_all_games()

        async def show_edit_ign_modal(inter: discord.Interaction, game_id: int):
            game = await DatabaseHelper.get_game(game_id)
            existing = await DatabaseHelper.get_player_ign(user.id, game_id)
            modal = AdminSetIGNModal(self.cog, user.id, game_id, game.name, existing)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, show_edit_ign_modal))
        await interaction.response.edit_message(
            content="\n".join(lines) + "\n\nSelect a game to edit:",
            view=view
        )


class AdminSetIGNModal(discord.ui.Modal, title="Set Player IGN"):
    ign = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter IGN (e.g., Username#TAG)",
        required=False,
        max_length=100
    )
    tracker_url = discord.ui.TextInput(
        label="Tracker URL (Optional)",
        placeholder="https://tracker.gg/valorant/profile/riot/Name%23Tag/overview",
        required=False,
        max_length=200
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int, game_name: str, existing_ign: str = None):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id
        self.game_name = game_name
        self.title = f"Set {game_name} IGN"
        if existing_ign:
            self.ign.default = existing_ign

    async def on_submit(self, interaction: discord.Interaction):
        # If tracker URL provided, parse IGN from it
        tracker_val = self.tracker_url.value.strip() if self.tracker_url.value else ""
        from_tracker = False
        if tracker_val:
            parsed = _parse_tracker_url(tracker_val)
            if parsed:
                ign_value = parsed
                from_tracker = True
            elif self.ign.value and self.ign.value.strip():
                ign_value = self.ign.value.strip()
            else:
                await interaction.response.send_message(
                    "Could not parse an IGN from that tracker URL. Please enter the IGN manually.",
                    ephemeral=True
                )
                return
        elif self.ign.value and self.ign.value.strip():
            ign_value = self.ign.value.strip()
        else:
            await interaction.response.send_message(
                "Please provide either an IGN or a tracker URL.",
                ephemeral=True
            )
            return

        # Apply Valorant API verification for Valorant games
        if 'valorant' in self.game_name.lower() and '#' in ign_value:
            hash_idx = ign_value.find('#')
            name_part = ign_value[:hash_idx]
            tag = ign_value[hash_idx + 1:]

            if not tag or not tag.isalnum() or not (3 <= len(tag) <= 5):
                await interaction.response.send_message(
                    "Invalid Valorant tag. Must be 3-5 alphanumeric characters.",
                    ephemeral=True
                )
                return

            await interaction.response.defer(ephemeral=True)
            account = await self.cog.henrik_api.get_account(name_part, tag)
            puuid = account.get('puuid') if account else None

            await DatabaseHelper.set_player_ign(self.user_id, self.game_id, ign_value, puuid=puuid)
            member = interaction.guild.get_member(self.user_id)
            display_name = member.display_name if member else str(self.user_id)

            if not account:
                await interaction.followup.send(
                    f"\u26a0\ufe0f Warning: Could not verify `{ign_value}` with the Valorant API.\n"
                    f"However, **{display_name}**'s {self.game_name} IGN has been force-set to: `{ign_value}`.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"Set **{display_name}**'s {self.game_name} IGN to: `{ign_value}`",
                    ephemeral=True
                )
            return

        # Marvel Rivals: verify via API but always store the user's exact input
        if 'rivals' in self.game_name.lower() and self.cog.rivals_api.available:
            await interaction.response.defer(ephemeral=True)
            player = await self.cog.rivals_api.find_player(ign_value)
            await DatabaseHelper.set_player_ign(self.user_id, self.game_id, ign_value)
            member = interaction.guild.get_member(self.user_id)
            name = member.display_name if member else str(self.user_id)
            if player or from_tracker:
                await interaction.followup.send(
                    f"Set **{name}**'s {self.game_name} IGN to: `{ign_value}`",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"Set **{name}**'s {self.game_name} IGN to: `{ign_value}`\n"
                    "**Note:** Could not verify this name with the Marvel Rivals API.",
                    ephemeral=True
                )
            return

        await DatabaseHelper.set_player_ign(self.user_id, self.game_id, ign_value)
        member = interaction.guild.get_member(self.user_id)
        name = member.display_name if member else str(self.user_id)
        await interaction.response.send_message(
            f"Set **{name}**'s {self.game_name} IGN to: `{ign_value}`",
            ephemeral=True
        )


class FetchStatsModal(discord.ui.Modal, title="Fetch Match Stats"):
    """Modal for entering match ID to fetch stats."""
    match_id_input = discord.ui.TextInput(
        label="Match ID or Short ID",
        placeholder="Enter match ID (e.g., 123 or ABC12)",
        required=True,
        max_length=20
    )
    tracker_url_input = discord.ui.TextInput(
        label="Tracker.gg URL (Optional)",
        placeholder="https://tracker.gg/valorant/match/...",
        required=False,
        max_length=200
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        match_id_str = self.match_id_input.value.strip()
        tracker_url = self.tracker_url_input.value.strip() if self.tracker_url_input.value else None

        # Find the match by ID or short_id
        try:
            mid = int(match_id_str)
            match = await DatabaseHelper.get_match(mid)
        except ValueError:
            match = await DatabaseHelper.get_match_by_short_id(match_id_str)

        if not match:
            await interaction.followup.send(f"Match `{match_id_str}` not found.", ephemeral=True)
            return

        if not match.get("winning_team"):
            await interaction.followup.send("This match hasn't been completed yet.", ephemeral=True)
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        if not game or 'valorant' not in game.name.lower():
            await interaction.followup.send("This is not a Valorant game.", ephemeral=True)
            return

        # Get match info
        players = await DatabaseHelper.get_match_players(match["match_id"])
        player_ids = [p["player_id"] for p in players]

        # Count existing stats for status display only — do NOT clear them.
        # fetch_valorant_match_stats uses INSERT OR REPLACE, so any existing rows
        # are overwritten per-player. This means if the fetch partially fails,
        # the previously-good stats survive instead of being wiped.
        existing_stats = await DatabaseHelper.get_valorant_match_stats(match["match_id"])
        existing_count = len(existing_stats) if existing_stats else 0

        # Parse timestamps
        match_end_time = datetime.now(timezone.utc)
        if match.get("decided_at"):
            try:
                match_end_time = datetime.fromisoformat(match["decided_at"].replace('Z', '+00:00'))
            except Exception:
                pass

        match_created_at = None
        if match.get("created_at"):
            try:
                created_str = match["created_at"]
                if isinstance(created_str, str):
                    created_str = created_str.replace('Z', '+00:00')
                match_created_at = datetime.fromisoformat(created_str)
                if match_created_at.tzinfo is None:
                    match_created_at = match_created_at.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass

        # Extract Valorant match UUID from tracker.gg URL if provided
        valorant_match_id = None
        if tracker_url:
            marker = "tracker.gg/valorant/match/"
            idx = tracker_url.find(marker)
            if idx != -1:
                uuid_part = tracker_url[idx + len(marker):]
                valorant_match_id = uuid_part.split("?")[0].split("/")[0].strip()

        short_id = match.get("short_id") or match_id_str
        status = f"overwriting {existing_count} existing, " if existing_count > 0 else ""
        await interaction.followup.send(
            f"Fetching stats for match `{match_id_str}`... ({status}fetching {len(player_ids)} players)",
            ephemeral=True
        )

        # Fetch stats — force_refetch=True skips the early-exit check so existing rows
        # get overwritten cleanly; stats are never wiped before a successful fetch.
        success, reason = await self.cog.fetch_valorant_match_stats(
            match["match_id"], game.game_id, player_ids, match_end_time,
            match_created_at=match_created_at,
            valorant_match_id=valorant_match_id,
            force_refetch=True
        )

        if success:
            final_stats = await DatabaseHelper.get_valorant_match_stats(match["match_id"])
            still_missing = len(player_ids) - len(final_stats)
            if still_missing > 0:
                # Find which players are missing stats
                matched_pids = {s['player_id'] for s in final_stats}
                igns = await DatabaseHelper.get_match_igns(match["match_id"])
                missing_lines = []
                for pid in player_ids:
                    if pid not in matched_pids:
                        ign = igns.get(pid)
                        member = interaction.guild.get_member(pid)
                        name = member.display_name if member else str(pid)
                        if ign:
                            missing_lines.append(f"- **{name}**: `{ign}`")
                        else:
                            missing_lines.append(f"- **{name}**: no IGN set")
                missing_text = "\n".join(missing_lines) if missing_lines else "Unknown"
                await interaction.followup.send(
                    f"Fetched stats: {len(final_stats)}/{len(player_ids)} players.\n\n"
                    f"**Missing stats for:**\n{missing_text}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"Successfully fetched stats for all {len(final_stats)} players!",
                    ephemeral=True
                )
            await self.cog.log_action(
                interaction.guild,
                f"Match {short_id}: Stats manually fetched "
                f"({len(final_stats)}/{len(player_ids)} players) by {interaction.user.display_name}"
            )
        else:
            await interaction.followup.send(f"Failed to fetch stats: {reason}", ephemeral=True)
            await self.cog.log_action(
                interaction.guild,
                f"Match {short_id}: Manual stats fetch failed ({reason}) by {interaction.user.display_name}"
            )


class RefetchModeSelectView(discord.ui.View):
    """View for selecting refetch mode (incomplete only vs force all)."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Incomplete Only", style=discord.ButtonStyle.primary)
    async def incomplete_only(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Only refetch matches with missing player stats."""
        view = RefetchAllConfirmView(self.cog, force_all=False)
        await interaction.response.edit_message(
            content="**Refetch Incomplete Stats**\n\n"
            "This will find matches with missing player stats and re-fetch them.\n\n"
            "Select the maximum number of matches to process:",
            view=view
        )

    @discord.ui.button(label="Force All", style=discord.ButtonStyle.danger)
    async def force_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Force refetch ALL recent matches, overwriting existing stats."""
        view = RefetchAllConfirmView(self.cog, force_all=True)
        await interaction.response.edit_message(
            content="**Force Refetch ALL Stats**\n\n"
            "\u26a0\ufe0f This will clear and re-fetch stats for ALL recent matches, "
            "even those with complete data.\n\n"
            "Select the maximum number of matches to process:",
            view=view
        )


class IGNSuggestionView(discord.ui.View):
    """Sent to the admin channel when a player's stats couldn't be linked.
    Asks whether the unmatched Discord user changed their IGN to the unmatched
    Valorant player, and optionally auto-updates the IGN and refetches stats."""

    def __init__(self, cog: 'CustomMatch', match_id: int, game_id: int,
                 player_id: int, suggested_ign: str, suggested_puuid: Optional[str]):
        super().__init__(timeout=86400)  # 24-hour window
        self.cog = cog
        self.match_id = match_id
        self.game_id = game_id
        self.player_id = player_id
        self.suggested_ign = suggested_ign
        self.suggested_puuid = suggested_puuid

    @discord.ui.button(label="Yes \u2014 Update IGN & Refetch", style=discord.ButtonStyle.success)
    async def yes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        # Update the IGN (and PUUID) for the player
        await DatabaseHelper.set_player_ign(
            self.player_id, self.game_id, self.suggested_ign, puuid=self.suggested_puuid
        )
        member = interaction.guild.get_member(self.player_id)
        name = member.display_name if member else str(self.player_id)
        game = await DatabaseHelper.get_game(self.game_id)

        # Refetch stats — pass force_refetch=True so the existing-stats check is bypassed
        match = await DatabaseHelper.get_match(self.match_id)
        players = await DatabaseHelper.get_match_players(self.match_id)
        player_ids = [p['player_id'] for p in players]
        match_end_time = None
        if match and match.get('decided_at'):
            try:
                match_end_time = datetime.fromisoformat(match['decided_at'])
                if match_end_time.tzinfo is None:
                    match_end_time = match_end_time.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass
        if not match_end_time:
            match_end_time = datetime.now(timezone.utc)

        success, reason = await self.cog.fetch_valorant_match_stats(
            self.match_id, self.game_id, player_ids, match_end_time,
            guild=interaction.guild, attempt=1, force_refetch=True
        )

        if success:
            await interaction.followup.send(
                f"IGN for **{name}** updated to `{self.suggested_ign}` and stats refetched successfully.",
                ephemeral=True
            )
            await self.cog.log_action(
                interaction.guild,
                f"IGN for **{name}** ({game.name if game else 'Unknown'}) auto-corrected to "
                f"`{self.suggested_ign}` and stats refetched for match "
                f"{await self.cog._get_match_short_id(self.match_id)} by {interaction.user.display_name}"
            )
        else:
            await interaction.followup.send(
                f"IGN updated to `{self.suggested_ign}` but stat refetch failed: {reason}",
                ephemeral=True
            )

        # Disable buttons on the admin embed after action
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def no_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        await interaction.followup.send("No change made.", ephemeral=True)


class RefetchAllConfirmView(discord.ui.View):
    """View for confirming bulk stats re-fetch with limit selection."""

    def __init__(self, cog: 'CustomMatch', force_all: bool = False):
        super().__init__(timeout=60)
        self.cog = cog
        self.force_all = force_all

        # Add dropdown for limit selection
        options = [
            discord.SelectOption(label="10 matches", value="10"),
            discord.SelectOption(label="25 matches", value="25"),
            discord.SelectOption(label="50 matches", value="50", default=True),
            discord.SelectOption(label="100 matches", value="100"),
        ]
        select = discord.ui.Select(placeholder="Select limit...", options=options)
        select.callback = self.select_callback
        self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        limit = int(interaction.data["values"][0])
        await interaction.response.defer(ephemeral=True)

        # Find all Valorant games
        games = await DatabaseHelper.get_all_games()
        valorant_games = [g for g in games if 'valorant' in g.name.lower()]

        if not valorant_games:
            await interaction.followup.send("No Valorant games configured.", ephemeral=True)
            return

        # Get matches based on mode
        matches_to_process = []
        for game in valorant_games:
            async with DatabaseHelper._get_db() as db:
                if self.force_all:
                    # Force all: get ALL completed matches regardless of stats count
                    query = """SELECT m.match_id, m.short_id, m.game_id, m.decided_at, m.created_at,
                                      COUNT(DISTINCT mp.player_id) as player_count,
                                      COUNT(DISTINCT vs.player_id) as stats_count
                               FROM matches m
                               JOIN match_players mp ON m.match_id = mp.match_id
                               LEFT JOIN valorant_match_stats vs ON m.match_id = vs.match_id
                               WHERE m.game_id = ? AND m.winning_team IS NOT NULL AND m.cancelled = 0
                               GROUP BY m.match_id
                               ORDER BY m.decided_at DESC
                               LIMIT ?"""
                else:
                    # Incomplete only: only matches with missing stats
                    query = """SELECT m.match_id, m.short_id, m.game_id, m.decided_at, m.created_at,
                                      COUNT(DISTINCT mp.player_id) as player_count,
                                      COUNT(DISTINCT vs.player_id) as stats_count
                               FROM matches m
                               JOIN match_players mp ON m.match_id = mp.match_id
                               LEFT JOIN valorant_match_stats vs ON m.match_id = vs.match_id
                               WHERE m.game_id = ? AND m.winning_team IS NOT NULL AND m.cancelled = 0
                               GROUP BY m.match_id
                               HAVING stats_count < player_count
                               ORDER BY m.decided_at DESC
                               LIMIT ?"""

                async with db.execute(query, (game.game_id, limit)) as cursor:
                    rows = await cursor.fetchall()
                    matches_to_process.extend([dict(row) for row in rows])

        if not matches_to_process:
            if self.force_all:
                await interaction.followup.send("No completed Valorant matches found.", ephemeral=True)
            else:
                await interaction.followup.send("All matches have complete stats! Use 'Force All' to re-fetch anyway.", ephemeral=True)
            return

        mode_str = "ALL" if self.force_all else "incomplete"
        await interaction.followup.send(
            f"Processing {len(matches_to_process)} {mode_str} matches...",
            ephemeral=True
        )

        # Process each match
        success_count = 0
        partial_count = 0
        fail_count = 0
        results = []

        for match_data in matches_to_process:
            match_id = match_data["match_id"]
            short_id = match_data.get("short_id") or str(match_id)
            game_id = match_data["game_id"]
            player_count = match_data["player_count"]
            stats_before = match_data["stats_count"]

            # Get all players
            players = await DatabaseHelper.get_match_players(match_id)
            player_ids = [p["player_id"] for p in players]

            # Do NOT clear existing stats before fetching — fetch_valorant_match_stats
            # uses INSERT OR REPLACE, so per-player rows are updated atomically.
            # This means partial failures leave previously-good stats intact.

            # Parse timestamps
            match_end_time = datetime.now(timezone.utc)
            if match_data.get("decided_at"):
                try:
                    match_end_time = datetime.fromisoformat(
                        match_data["decided_at"].replace('Z', '+00:00')
                    )
                except Exception:
                    pass

            match_created_at = None
            if match_data.get("created_at"):
                try:
                    created_str = match_data["created_at"]
                    if isinstance(created_str, str):
                        created_str = created_str.replace('Z', '+00:00')
                    match_created_at = datetime.fromisoformat(created_str)
                    if match_created_at.tzinfo is None:
                        match_created_at = match_created_at.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    pass

            # Fetch stats — force_refetch=True so existing rows are overwritten.
            success, reason = await self.cog.fetch_valorant_match_stats(
                match_id, game_id, player_ids, match_end_time,
                match_created_at=match_created_at,
                force_refetch=True
            )

            # Check final state
            final_stats = await DatabaseHelper.get_valorant_match_stats(match_id)
            stats_after = len(final_stats)

            if stats_after == player_count:
                success_count += 1
                results.append(f"{short_id}: {stats_before} -> {stats_after}/{player_count} \u2713")
            elif stats_after > stats_before:
                partial_count += 1
                # Show which IGNs are missing
                matched_pids = {s['player_id'] for s in final_stats}
                igns = await DatabaseHelper.get_match_igns(match_id)
                missing = [igns.get(pid, f"ID:{pid}") for pid in player_ids if pid not in matched_pids]
                missing_str = f" (missing: {', '.join(missing)})" if missing else ""
                results.append(f"{short_id}: {stats_before} -> {stats_after}/{player_count}{missing_str}")
            else:
                fail_count += 1
                results.append(f"{short_id}: {stats_before}/{player_count} (failed: {reason})")

        # Send summary
        summary = f"**Re-fetch Complete:**\n"
        summary += f"\u2713 Completed: {success_count}\n"
        summary += f"\u25d0 Partial: {partial_count}\n"
        summary += f"\u2717 Failed: {fail_count}\n\n"
        summary += "**Details:**\n" + "\n".join(results[:15])
        if len(results) > 15:
            summary += f"\n... and {len(results) - 15} more"

        await interaction.followup.send(summary, ephemeral=True)

        # Log to log channel
        mode_label = "Force All" if self.force_all else "Incomplete Only"
        log_msg = (
            f"Stats refetch ({mode_label}) by {interaction.user.display_name}: "
            f"{success_count} complete, {partial_count} partial, {fail_count} failed "
            f"({len(matches_to_process)} matches processed)"
        )
        await self.cog.log_action(interaction.guild, log_msg)


class CancelMatchModal(discord.ui.Modal, title="Cancel Match"):
    """Modal requiring a reason when cancelling a match via admin panel."""
    reason_input = discord.ui.TextInput(
        label="Reason for cancellation",
        placeholder="e.g., Player disconnected, Remake requested",
        required=True,
        min_length=3,
        max_length=200,
        style=discord.TextStyle.short
    )

    def __init__(self, cog: 'CustomMatch', match_id: int, short_id: str):
        super().__init__()
        self.cog = cog
        self.match_id = match_id
        self.short_id = short_id

    async def on_submit(self, interaction: discord.Interaction):
        reason = self.reason_input.value.strip()
        admin = interaction.user

        # Answer the modal first. Cancelling tears down channels, VCs and roles,
        # which comfortably outruns the 3s interaction window; deferring means a
        # slow cleanup can no longer cost the admin their confirmation.
        await interaction.response.defer(ephemeral=True)

        # The cancellation embed is posted by cancel_match itself, to the game
        # channel and the log channel — not the match channel, which cleanup is
        # about to delete.
        await self.cog.cancel_match(
            interaction.guild, self.match_id,
            reason=reason,
            cancelled_by=admin.id
        )
        await interaction.followup.send(
            f"Match {self.short_id} cancelled. Reason: {reason}", ephemeral=True
        )


class ChangeWinnerModal(discord.ui.Modal, title="Change Match Winner"):
    match_id_input = discord.ui.TextInput(
        label="Match ID",
        placeholder="Enter the match ID",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            match_id = int(self.match_id_input.value)
            match = await DatabaseHelper.get_completed_match(match_id)
            if not match:
                await interaction.response.send_message("Match not found.", ephemeral=True)
                return

            short_id = match.get("short_id") or str(match_id)
            view = ChangeWinnerSelectView(self.cog, match_id, match.get("winning_team"), short_id)
            game = await DatabaseHelper.get_game(match["game_id"])
            current = match.get("winning_team", "None")
            await interaction.response.send_message(
                f"Match {short_id} ({game.name})\nCurrent winner: **{current}**\n\nSelect the new winner:",
                view=view, ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid match ID.", ephemeral=True)


class ChangeWinnerSelectView(discord.ui.View):
    """View for selecting a new winner for a match."""

    def __init__(self, cog: 'CustomMatch', match_id: int, current_winner: Optional[str], short_id: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.current_winner = current_winner
        self.short_id = short_id

    @discord.ui.button(label="Red Team Wins", style=discord.ButtonStyle.danger)
    async def red_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, Team.RED)

    @discord.ui.button(label="Blue Team Wins", style=discord.ButtonStyle.primary)
    async def blue_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.change_winner(interaction, Team.BLUE)

    async def change_winner(self, interaction: discord.Interaction, new_winner: Team):
        # Acquire the match lock so reverse + re-finalize are atomic
        if self.match_id not in self.cog.match_finalize_locks:
            self.cog.match_finalize_locks[self.match_id] = asyncio.Lock()

        async with self.cog.match_finalize_locks[self.match_id]:
            # Reverse existing result if there was one
            if self.current_winner:
                await DatabaseHelper.reverse_match_result(self.match_id)

            # Apply new result (inner method — lock already held)
            await self.cog._finalize_match_inner(interaction.guild, self.match_id, new_winner)

        await interaction.response.send_message(
            f"Match {self.short_id} winner changed to **{new_winner.value}** team.",
            ephemeral=True
        )
        await self.cog.log_action(
            interaction.guild,
            f"Match {self.short_id} winner changed to {new_winner.value} by {interaction.user.display_name}"
        )


class FixMatchModal(discord.ui.Modal, title="Fix Match"):
    match_id_input = discord.ui.TextInput(
        label="Match Short ID",
        placeholder="e.g. G7MGG",
        required=True,
        max_length=10
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.match_id_input.value.strip()
        match = await DatabaseHelper.get_match_by_short_id(raw.upper())
        if not match:
            match = await DatabaseHelper.get_match_by_short_id(raw)
        if not match:
            try:
                match = await DatabaseHelper.get_match(int(raw))
            except ValueError:
                pass
        if not match:
            await interaction.response.send_message("Match not found.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        view = await FixMatchView.create(self.cog, match, interaction.guild)
        msg = await interaction.followup.send(embed=view.build_embed(), view=view, ephemeral=True, wait=True)
        view.message = msg


class FixMatchView(discord.ui.View):
    """Interactive view to edit match teams — move, add, or remove players."""

    def __init__(self, cog: 'CustomMatch', match: dict, game, red: list, blue: list, igns: dict, guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.match = match
        self.game = game
        self.match_id = match["match_id"]
        self.short_id = match.get("short_id") or str(self.match_id)
        self.guild = guild
        self.igns = igns
        self.message = None  # Set after send
        # Working copies of teams (player_id lists)
        self.red = list(red)
        self.blue = list(blue)
        # Track original roster for diff
        self.original_red = set(red)
        self.original_blue = set(blue)
        self.original_all = self.original_red | self.original_blue

    @classmethod
    async def create(cls, cog, match: dict, guild):
        game = await DatabaseHelper.get_game(match["game_id"])
        players = await DatabaseHelper.get_match_players(match["match_id"])
        igns = await DatabaseHelper.get_match_igns(match["match_id"])
        red = [p["player_id"] for p in players if p["team"] == "red"]
        blue = [p["player_id"] for p in players if p["team"] == "blue"]
        return cls(cog, match, game, red, blue, igns, guild)

    def _name(self, pid):
        if pid in self.igns:
            return self.igns[pid]
        m = self.guild.get_member(pid)
        return m.display_name if m else str(pid)

    def build_embed(self):
        red_names = [f"`{self._name(p)}`" for p in self.red] or ["\u2014"]
        blue_names = [f"`{self._name(p)}`" for p in self.blue] or ["\u2014"]
        game_name = self.game.name if self.game else "Unknown"
        winner = self.match.get("winning_team")
        status = f"Winner: **{winner.capitalize()}**" if winner else "**In Progress**"

        embed = discord.Embed(
            title=f"Fix Match \u2014 {self.short_id}",
            description=f"{game_name} | {status}",
            color=COLOR_NEUTRAL
        )
        embed.add_field(name=f"Red Team ({len(self.red)})", value="\n".join(red_names), inline=True)
        embed.add_field(name=f"Blue Team ({len(self.blue)})", value="\n".join(blue_names), inline=True)

        # Show pending changes
        changes = self._get_changes_summary()
        if changes:
            embed.add_field(name="Pending Changes", value=changes, inline=False)

        return embed

    def _get_changes_summary(self):
        current_all = set(self.red) | set(self.blue)
        added = current_all - self.original_all
        removed = self.original_all - current_all
        moved = []
        for pid in current_all & self.original_all:
            was_red = pid in self.original_red
            is_red = pid in self.red
            if was_red != is_red:
                moved.append(pid)

        lines = []
        for pid in added:
            team = "Red" if pid in self.red else "Blue"
            lines.append(f"+ `{self._name(pid)}` added to {team}")
        for pid in removed:
            lines.append(f"- `{self._name(pid)}` removed")
        for pid in moved:
            new_team = "Red" if pid in self.red else "Blue"
            lines.append(f"~ `{self._name(pid)}` moved to {new_team}")

        return "\n".join(lines) if lines else ""

    @discord.ui.button(label="Move Player", style=discord.ButtonStyle.primary, row=0)
    async def move_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        all_players = self.red + self.blue
        if not all_players:
            await interaction.response.send_message("No players to move.", ephemeral=True)
            return
        options = []
        for pid in all_players:
            team = "Red" if pid in self.red else "Blue"
            options.append(discord.SelectOption(
                label=f"{team} \u2014 {self._name(pid)}"[:100], value=str(pid)
            ))
        view = _FixSelectPlayerView(self, options, "move")
        await interaction.response.send_message("Select a player to move to the other team:", view=view, ephemeral=True)

    @discord.ui.button(label="Add Player", style=discord.ButtonStyle.success, row=0)
    async def add_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = _FixAddPlayerView(self)
        await interaction.response.send_message("Select a player to add:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Player", style=discord.ButtonStyle.danger, row=0)
    async def remove_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        all_players = self.red + self.blue
        if not all_players:
            await interaction.response.send_message("No players to remove.", ephemeral=True)
            return
        options = []
        for pid in all_players:
            team = "Red" if pid in self.red else "Blue"
            options.append(discord.SelectOption(
                label=f"{team} \u2014 {self._name(pid)}"[:100], value=str(pid)
            ))
        view = _FixSelectPlayerView(self, options, "remove")
        await interaction.response.send_message("Select a player to remove:", view=view, ephemeral=True)

    @discord.ui.button(label="Apply & Recalculate", style=discord.ButtonStyle.danger, row=1)
    async def apply_changes(self, interaction: discord.Interaction, button: discord.ui.Button):
        changes = self._get_changes_summary()
        if not changes:
            await interaction.response.send_message("No changes to apply.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        match_id = self.match_id
        game_id = self.match["game_id"]
        winning_team = self.match.get("winning_team")

        # Step 1: If the match was decided, reverse the old stats
        if winning_team:
            reversed_ok = await DatabaseHelper.reverse_match_result(match_id)
            if not reversed_ok:
                logger.warning(f"FixMatch: No MMR history to reverse for match {match_id}")

        # Step 2: Update match_players in DB to match our working copy
        # Remove all existing players
        async with DatabaseHelper._get_db() as db:
            await db.execute("DELETE FROM match_players WHERE match_id = ?", (match_id,))
            await db.commit()

        # Re-add current red/blue rosters
        for pid in self.red:
            await DatabaseHelper.add_match_player(match_id, pid, "red")
        for pid in self.blue:
            await DatabaseHelper.add_match_player(match_id, pid, "blue")

        # Step 2b: Remove valorant stats for players no longer in the roster
        new_roster = set(self.red) | set(self.blue)
        removed_players = self.original_all - new_roster
        if removed_players:
            async with DatabaseHelper._get_db() as db:
                for pid in removed_players:
                    await db.execute(
                        "DELETE FROM valorant_match_stats WHERE match_id = ? AND player_id = ?",
                        (match_id, pid)
                    )
                await db.commit()

        result_msg = f"**Match {self.short_id} roster updated.**\n{changes}"

        # Step 3: If the match had a winner, re-finalize to recalculate MMR
        if winning_team:
            await self.cog.finalize_match(interaction.guild, match_id, Team(winning_team))
            result_msg += "\n\nMMR has been recalculated with the corrected roster."

        await interaction.followup.send(result_msg, ephemeral=True)

        # Log the action
        await self.cog.log_action(
            interaction.guild,
            f"Match **{self.short_id}** roster fixed by {interaction.user.display_name}:\n{changes}",
            prefix="\U0001f527"
        )

        # Refresh the embed in this message
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Match fix cancelled.", embed=None, view=None)
        self.stop()


class _FixSelectPlayerView(discord.ui.View):
    """Helper select for move/remove actions in FixMatchView."""

    def __init__(self, parent: FixMatchView, options: list, action: str):
        super().__init__(timeout=60)
        self.parent = parent
        self.action = action
        select = discord.ui.Select(placeholder="Select player...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        pid = int(interaction.data["values"][0])
        parent = self.parent

        if self.action == "move":
            if pid in parent.red:
                parent.red.remove(pid)
                parent.blue.append(pid)
            elif pid in parent.blue:
                parent.blue.remove(pid)
                parent.red.append(pid)
        elif self.action == "remove":
            if pid in parent.red:
                parent.red.remove(pid)
            elif pid in parent.blue:
                parent.blue.remove(pid)

        # Update the parent message embed
        try:
            await parent.message.edit(embed=parent.build_embed())
        except Exception:
            pass

        await interaction.response.edit_message(
            content=f"Done. Use the main view to continue editing or apply.",
            view=None
        )


class _FixAddPlayerView(discord.ui.View):
    """UserSelect to add a player, then pick their team."""

    def __init__(self, parent: FixMatchView):
        super().__init__(timeout=60)
        self.parent = parent

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select player to add...")
    async def select_user(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        member = select.values[0]
        pid = member.id
        parent = self.parent

        if pid in parent.red or pid in parent.blue:
            await interaction.response.edit_message(content="Player is already in this match.", view=None)
            return

        # Fetch their IGN for display
        ign = await DatabaseHelper.get_player_ign(pid, parent.match["game_id"])
        if ign:
            parent.igns[pid] = ign

        view = _FixPickTeamView(parent, pid, member.display_name)
        await interaction.response.edit_message(
            content=f"Add **{member.display_name}** to which team?", view=view
        )


class _FixPickTeamView(discord.ui.View):
    """Pick red or blue for an added player."""

    def __init__(self, parent: FixMatchView, player_id: int, display_name: str):
        super().__init__(timeout=60)
        self.parent = parent
        self.player_id = player_id
        self.display_name = display_name

    @discord.ui.button(label="Red Team", style=discord.ButtonStyle.danger)
    async def red(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.parent.red.append(self.player_id)
        await self._finish(interaction)

    @discord.ui.button(label="Blue Team", style=discord.ButtonStyle.primary)
    async def blue(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.parent.blue.append(self.player_id)
        await self._finish(interaction)

    async def _finish(self, interaction: discord.Interaction):
        try:
            await self.parent.message.edit(embed=self.parent.build_embed())
        except Exception:
            pass
        await interaction.response.edit_message(
            content=f"Added **{self.display_name}**. Use the main view to continue or apply.",
            view=None
        )


class SuspensionManagementView(discord.ui.View):
    """View for managing suspensions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add Suspension", style=discord.ButtonStyle.danger)
    async def add_suspension(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddSuspensionUserSelectView(self.cog)
        await interaction.response.send_message("Select a user to suspend:", view=view, ephemeral=True)

    @discord.ui.button(label="View Suspensions", style=discord.ButtonStyle.secondary)
    async def view_suspensions(self, interaction: discord.Interaction, button: discord.ui.Button):
        suspensions = await DatabaseHelper.get_all_suspensions()
        if not suspensions:
            await interaction.response.send_message("No active suspensions.", ephemeral=True)
            return

        lines = ["**Active Suspensions**\n"]
        for s in suspensions:
            user = interaction.guild.get_member(s.player_id)
            name = user.display_name if user else str(s.player_id)
            game = await DatabaseHelper.get_game(s.game_id) if s.game_id else None
            game_str = game.name if game else "All Games"
            until = s.suspended_until.strftime("%Y-%m-%d %H:%M UTC")
            reason = s.reason or "No reason"
            lines.append(f"**#{s.suspension_id}** - {name} ({game_str})")
            lines.append(f"  Until: {until} | Reason: {reason}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Remove Suspension", style=discord.ButtonStyle.success)
    async def remove_suspension(self, interaction: discord.Interaction, button: discord.ui.Button):
        suspensions = await DatabaseHelper.get_all_suspensions()
        if not suspensions:
            await interaction.response.send_message("No active suspensions.", ephemeral=True)
            return

        view = RemoveSuspensionSelectView(self.cog, suspensions, interaction.guild)
        await interaction.response.send_message("Select a suspension to remove:", view=view, ephemeral=True)


class AddSuspensionUserSelectView(discord.ui.View):
    """View for selecting a user to suspend."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        games = await DatabaseHelper.get_all_games()
        view = AddSuspensionGameSelectView(self.cog, user.id, games)
        await interaction.response.edit_message(
            content=f"Suspending **{user.display_name}**\nSelect a game (or 'All Games'):",
            view=view
        )


class AddSuspensionGameSelectView(discord.ui.View):
    """View for selecting game for suspension."""

    def __init__(self, cog: 'CustomMatch', user_id: int, games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id

        options = [discord.SelectOption(label="All Games", value="all")]
        for g in games:
            options.append(discord.SelectOption(label=g.name, value=str(g.game_id)))

        select = discord.ui.Select(placeholder="Select game...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        game_id = None if value == "all" else int(value)
        modal = AddSuspensionModal(self.cog, self.user_id, game_id)
        await interaction.response.send_modal(modal)


class AddSuspensionModal(discord.ui.Modal, title="Add Suspension"):
    duration = discord.ui.TextInput(
        label="Duration (hours)",
        placeholder="e.g., 24 for 1 day, 168 for 1 week",
        required=True
    )
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="Reason for suspension",
        required=True,
        style=discord.TextStyle.paragraph
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: Optional[int]):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            hours = int(self.duration.value)
            until = datetime.now(timezone.utc) + timedelta(hours=hours)
            suspension_id = await DatabaseHelper.add_suspension(
                self.user_id, self.game_id, until,
                self.reason.value or None, interaction.user.id
            )

            user = interaction.guild.get_member(self.user_id)
            name = user.display_name if user else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id) if self.game_id else None
            game_str = game.name if game else "All Games"

            view = SuspensionFollowUpView(
                self.cog, self.user_id, name, game_str,
                self.reason.value, hours, until
            )
            await interaction.response.send_message(
                f"Suspended **{name}** from **{game_str}** for {hours} hours.\nSuspension ID: #{suspension_id}",
                view=view, ephemeral=True
            )

            reason_str = f" \u2014 Reason: {self.reason.value}" if self.reason.value else ""
            await self.cog.log_action(
                interaction.guild,
                f"Suspended {name} from {game_str} for {hours}h by {interaction.user.display_name}{reason_str}",
                prefix="\u203c\ufe0f"
            )
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)


class SuspensionFollowUpView(discord.ui.View):
    """View shown after a suspension is applied, with a Next button."""

    def __init__(self, cog: 'CustomMatch', user_id: int, user_name: str,
                 game_str: str, reason: str, hours: int, until: datetime):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.user_name = user_name
        self.game_str = game_str
        self.reason = reason
        self.hours = hours
        self.until = until

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SuspensionContactView(
            self.cog, self.user_id, self.user_name,
            self.game_str, self.reason, self.hours, self.until
        )
        await interaction.response.edit_message(
            content=(
                f"**Suspended {self.user_name}** from **{self.game_str}** for {self.hours} hours.\n\n"
                f"How would you like to follow up with the user?"
            ),
            view=view
        )


class SuspensionContactView(discord.ui.View):
    """View asking admin whether to start a discussion thread or send a DM."""

    def __init__(self, cog: 'CustomMatch', user_id: int, user_name: str,
                 game_str: str, reason: str, hours: int, until: datetime):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.user_name = user_name
        self.game_str = game_str
        self.reason = reason
        self.hours = hours
        self.until = until

    @discord.ui.button(label="Start Discussion", style=discord.ButtonStyle.primary)
    async def start_discussion(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        parent_id = await DatabaseHelper.get_config("cm_discussion_parent_channel_id")
        if not parent_id:
            await interaction.followup.send(
                "No discussion parent channel configured. Set one in Settings > Channels > Discussion Channel.",
                ephemeral=True
            )
            return

        parent_channel = interaction.guild.get_channel(int(parent_id))
        if not parent_channel:
            await interaction.followup.send("Discussion parent channel not found.", ephemeral=True)
            return

        target = interaction.guild.get_member(self.user_id)
        if not target:
            await interaction.followup.send("User not found in server.", ephemeral=True)
            return

        # Create a private thread in the parent channel
        thread = await parent_channel.create_thread(
            name=f"Suspension - {self.user_name}",
            type=discord.ChannelType.private_thread,
            invitable=False
        )

        # Add the suspended user and the admin who created it
        await thread.add_user(target)
        await thread.add_user(interaction.user)

        # Send an opening message in the thread
        expiry_unix = int(self.until.timestamp())
        reason_str = f"\n**Reason:** {self.reason}" if self.reason else ""
        await thread.send(
            f"**Suspension Discussion**\n\n"
            f"{target.mention}, you have been suspended from **{self.game_str}** for **{self.hours}** hours.{reason_str}\n"
            f"**Expires:** <t:{expiry_unix}:F>\n\n"
            f"An admin has opened this thread to discuss the suspension with you."
        )

        # Send silent notification to admin channel
        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")
        if admin_channel_id:
            admin_channel = interaction.guild.get_channel(int(admin_channel_id))
            if admin_channel:
                embed = discord.Embed(
                    title="Suspension Discussion Opened",
                    description=f"A discussion thread has been created for **{self.user_name}**'s suspension.",
                    color=discord.Color.orange()
                )
                embed.add_field(name="Thread", value=thread.mention, inline=True)
                embed.add_field(name="Game", value=self.game_str, inline=True)
                embed.add_field(name="Opened by", value=interaction.user.mention, inline=True)

                view = DiscussionNotificationView(thread.id)
                self.cog.bot.add_view(view)
                await admin_channel.send(embed=embed, view=view, silent=True)

        await interaction.edit_original_response(
            content=f"Discussion thread created: {thread.mention}",
            view=None
        )

    @discord.ui.button(label="Send DM", style=discord.ButtonStyle.secondary)
    async def send_dm(self, interaction: discord.Interaction, button: discord.ui.Button):
        target = interaction.guild.get_member(self.user_id)
        if not target:
            await interaction.response.send_message("User not found in server.", ephemeral=True)
            return

        try:
            expiry_unix = int(self.until.timestamp())
            dm_msg = f"You have been suspended from **{self.game_str}** for **{self.hours}** hours."
            if self.reason:
                dm_msg += f"\n**Reason:** {self.reason}"
            dm_msg += f"\n**Expires:** <t:{expiry_unix}:F>"
            await target.send(dm_msg)
            await interaction.response.edit_message(
                content=f"DM sent to **{self.user_name}**.",
                view=None
            )
        except discord.Forbidden:
            await interaction.response.edit_message(
                content=f"Could not DM **{self.user_name}** \u2014 they may have DMs disabled.",
                view=None
            )

    @discord.ui.button(label="Skip", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content=f"Suspended **{self.user_name}** from **{self.game_str}** for {self.hours} hours.",
            view=None
        )


class DiscussionNotificationView(discord.ui.View):
    """Persistent view on the admin channel notification for a discussion thread."""

    def __init__(self, thread_id: int):
        super().__init__(timeout=None)  # Persistent
        self.thread_id = thread_id

        join_btn = discord.ui.Button(
            label="Join", style=discord.ButtonStyle.primary,
            custom_id=f"cm_discussion_join:{thread_id}"
        )
        join_btn.callback = self.join_callback
        self.add_item(join_btn)

        close_btn = discord.ui.Button(
            label="Close", style=discord.ButtonStyle.danger,
            custom_id=f"cm_discussion_close:{thread_id}"
        )
        close_btn.callback = self.close_callback
        self.add_item(close_btn)

    async def join_callback(self, interaction: discord.Interaction):
        thread = interaction.guild.get_thread(self.thread_id)
        if not thread:
            try:
                thread = await interaction.guild.fetch_channel(self.thread_id)
            except Exception:
                await interaction.response.send_message("Thread not found or already deleted.", ephemeral=True)
                return
        await thread.add_user(interaction.user)
        await interaction.response.send_message(f"You've been added to {thread.mention}.", ephemeral=True)

    async def close_callback(self, interaction: discord.Interaction):
        thread = interaction.guild.get_thread(self.thread_id)
        if not thread:
            try:
                thread = await interaction.guild.fetch_channel(self.thread_id)
            except Exception:
                # Thread already gone, just clean up the message
                await interaction.response.edit_message(
                    content="Thread already deleted.",
                    embed=None, view=None
                )
                return

        await thread.send("This discussion has been closed by an admin.")
        await thread.edit(archived=True, locked=True)

        # Update the admin notification
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = discord.Color.dark_grey()
            embed.title = "Suspension Discussion Closed"
            embed.set_footer(text=f"Closed by {interaction.user.display_name}")

        await interaction.response.edit_message(embed=embed, view=None)


class PanelDiscussionUserSelectView(discord.ui.View):
    """User select dropdown for starting a discussion from the admin panel."""

    def __init__(self, cog: 'CustomMatch', parent_channel: discord.TextChannel):
        super().__init__(timeout=60)
        self.cog = cog
        self.parent_channel = parent_channel

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        target = select.values[0]
        if not isinstance(target, discord.Member):
            await interaction.response.edit_message(content="User not found in server.", view=None)
            return

        await interaction.response.defer(ephemeral=True)

        # Create a private thread in the parent channel
        thread = await self.parent_channel.create_thread(
            name=f"Discussion - {target.display_name}",
            type=discord.ChannelType.private_thread,
            invitable=False
        )

        await thread.send(
            f"**Discussion**\n\n"
            f"{target.mention}, an admin has opened this thread to discuss with you."
        )

        # Add the target user and the admin who started it
        await thread.add_user(target)
        await thread.add_user(interaction.user)

        # Send notification to admin channel
        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")
        if admin_channel_id:
            admin_channel = interaction.guild.get_channel(int(admin_channel_id))
            if admin_channel:
                embed = discord.Embed(
                    title="Discussion Opened",
                    description=f"A discussion thread has been created with **{target.display_name}**.",
                    color=discord.Color.orange()
                )
                embed.add_field(name="Thread", value=thread.mention, inline=True)
                embed.add_field(name="Opened by", value=interaction.user.mention, inline=True)

                view = DiscussionNotificationView(thread.id)
                self.cog.bot.add_view(view)
                await admin_channel.send(embed=embed, view=view, silent=True)

        await interaction.edit_original_response(
            content=f"Discussion thread created: {thread.mention}",
            view=None
        )


class RemoveSuspensionSelectView(discord.ui.View):
    """View for selecting a suspension to remove."""

    def __init__(self, cog: 'CustomMatch', suspensions: List[Suspension], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.suspensions_by_id = {s.suspension_id: s for s in suspensions}
        self.guild = guild

        options = []
        for s in suspensions[:25]:
            user = guild.get_member(s.player_id)
            name = user.display_name if user else str(s.player_id)
            options.append(discord.SelectOption(
                label=f"#{s.suspension_id} - {name}",
                value=str(s.suspension_id),
                description=s.reason[:50] if s.reason else "No reason"
            ))

        select = discord.ui.Select(placeholder="Select suspension...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        suspension_id = int(interaction.data["values"][0])
        susp = self.suspensions_by_id.get(suspension_id)
        await DatabaseHelper.remove_suspension(suspension_id)
        await interaction.response.edit_message(
            content=f"Removed suspension #{suspension_id}.",
            view=None
        )
        if susp:
            user = self.guild.get_member(susp.player_id)
            name = user.display_name if user else str(susp.player_id)
            game = await DatabaseHelper.get_game(susp.game_id) if susp.game_id else None
            game_str = game.name if game else "All Games"
            await self.cog.log_action(
                interaction.guild,
                f"Suspension #{suspension_id} removed for **{name}** ({game_str}) by {interaction.user.display_name}",
                prefix="\u203c\ufe0f"
            )


class QueuePlayerRemoveView(discord.ui.View):
    """View for removing a player from queue — shows only queued players."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_state: 'QueueState',
                 guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.queue_state = queue_state

        # Build dropdown options from actual queue players
        options = []
        for pid in list(queue_state.players.keys()):
            member = guild.get_member(pid)
            name = member.display_name if member else str(pid)
            options.append(discord.SelectOption(label=name, value=str(pid)))

        select = discord.ui.Select(
            placeholder="Select player to remove...",
            options=options[:25]
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        player_id = int(interaction.data["values"][0])
        if player_id not in self.queue_state.players:
            await interaction.response.edit_message(content="That player is no longer in the queue.", view=None)
            return

        member = interaction.guild.get_member(player_id)
        name = member.display_name if member else str(player_id)

        del self.queue_state.players[player_id]
        self.queue_state.grace_timers.pop(player_id, None)
        await DatabaseHelper.remove_player_from_queue(self.queue_state.queue_id, player_id)

        game = await DatabaseHelper.get_game(self.game_id)
        channel = interaction.guild.get_channel(self.queue_state.channel_id)
        if channel and self.queue_state.message_id:
            try:
                msg = await channel.fetch_message(self.queue_state.message_id)
                embed = await self.cog.create_queue_embed(game, self.queue_state, interaction.guild)
                view = QueueView(self.cog, self.game_id, self.queue_state.queue_id)
                await msg.edit(embed=embed, view=view)
            except Exception:
                pass

        await interaction.response.edit_message(
            content=f"Removed **{name}** from the queue.",
            view=None
        )


class AdjustWLUserSelectView(discord.ui.View):
    """View for selecting a user to adjust W/L."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.edit_message(content="No games configured.", view=None)
            return

        async def show_adjust_modal(inter: discord.Interaction, game_id: int):
            modal = AdjustWLModal(self.cog, user.id, game_id)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, show_adjust_modal))
        await interaction.response.edit_message(
            content=f"Adjusting stats for **{user.display_name}**\nSelect a game:",
            view=view
        )


class AdjustWLModal(discord.ui.Modal, title="Adjust Wins/Losses"):
    wins_delta = discord.ui.TextInput(
        label="Wins adjustment",
        placeholder="e.g., 1 or -2",
        default="0",
        required=True
    )
    losses_delta = discord.ui.TextInput(
        label="Losses adjustment",
        placeholder="e.g., 1 or -2",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            wins = int(self.wins_delta.value)
            losses = int(self.losses_delta.value)

            await DatabaseHelper.adjust_player_stats(
                self.user_id, self.game_id, wins, losses,
                adjusted_by=interaction.user.id
            )

            member = interaction.guild.get_member(self.user_id)
            name = member.display_name if member else str(self.user_id)
            game = await DatabaseHelper.get_game(self.game_id)

            await interaction.response.send_message(
                f"Adjusted **{name}**'s stats for {game.name}: {wins:+d}W, {losses:+d}L\n"
                f"This adjustment applies to both all-time and monthly leaderboards.",
                ephemeral=True
            )
            await self.cog.log_action(
                interaction.guild,
                f"W/L adjusted for **{name}** ({game.name}): {wins:+d}W, {losses:+d}L by {interaction.user.display_name}",
                prefix="\u2755"
            )
        except ValueError:
            await interaction.response.send_message("Invalid numbers.", ephemeral=True)


class SetMMRUserSelectView(discord.ui.View):
    """View for setting player MMR with user and game selection."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.games = games
        self.selected_user = None

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_user = select.values[0]

        async def show_mmr_modal(inter: discord.Interaction, game_id: int):
            game = await DatabaseHelper.get_game(game_id)
            if game and is_overwatch_game(game):
                # Overwatch has no single MMR to set -- the balancer reads a
                # rating per role, so ask which one before opening the modal.
                view = OWSetMMRRoleSelectView(self.cog, self.selected_user.id, game_id)
                await inter.response.edit_message(
                    content=f"Setting **{game.name}** MMR for "
                            f"**{self.selected_user.display_name}**\nSelect a role:",
                    view=view
                )
                return
            modal = SetMMRModal(self.cog, self.selected_user.id, game_id)
            await inter.response.send_modal(modal)

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(self.games, show_mmr_modal))
        await interaction.response.edit_message(
            content=f"Setting MMR for **{self.selected_user.display_name}**\nSelect a game:",
            view=view
        )


class OWRoleRanksUserSelectView(discord.ui.View):
    """Entry point for re-ranking an existing player's Overwatch roles.

    The "they were set up for Tank, now they want to play DPS" case: same rank
    dropdowns as new-user setup, but scoped to the roles the admin picks so the
    ratings they already earned on other roles are never touched.
    """

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=180)
        self.cog = cog
        self.games = games

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        if len(self.games) == 1:
            await _ow_role_ranks_show(interaction, self.cog, self.games[0], user.id)
            return

        async def pick_game(inter: discord.Interaction, game_id: int):
            game = await DatabaseHelper.get_game(game_id)
            await _ow_role_ranks_show(inter, self.cog, game, user.id)

        view = discord.ui.View(timeout=120)
        view.add_item(GameSelectDropdown(self.games, pick_game))
        await interaction.response.edit_message(
            content=f"Role ranks for **{user.display_name}**\nSelect a game:", view=view
        )


async def _ow_role_ranks_show(interaction, cog, game, user_id: int):
    """Show a player's current per-role ratings and offer roles to (re)rank."""
    member = interaction.guild.get_member(user_id)
    name = member.display_name if member else str(user_id)
    current = await DatabaseHelper.get_all_ow_role_stats(user_id, game.game_id)
    bands = await DatabaseHelper.get_mmr_roles_with_labels(game.game_id)

    def band_label(mmr: int) -> str:
        best = None
        for data in bands.values():
            if data['mmr'] <= mmr and (best is None or data['mmr'] > best['mmr']):
                best = data
        return (best['label'] or f"{best['mmr']} MMR") if best else "unranked"

    lines = [f"**{name}** — {game.name} role ranks"]
    for role in OW_ROLE_DISPLAY_ORDER:
        if role in current:
            mmr = current[role].effective_mmr
            played = current[role].games_played
            lines.append(f"{OW_ROLE_EMOJI[role]} **{role}**: {band_label(mmr)} "
                         f"({mmr} MMR, {played} games)")
        else:
            lines.append(f"{OW_ROLE_EMOJI[role]} **{role}**: *unranked*")
    lines.append("")
    lines.append("-# Select the roles to (re)rank — every other role is left untouched.")

    view = OWRoleRanksPickView(cog, game.game_id, user_id, set(current))
    await interaction.response.edit_message(content="\n".join(lines), view=view)


class OWRoleRanksPickView(discord.ui.View):
    """Which roles to (re)rank. Feeds straight into the setup rank dropdowns."""

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int, rated: set):
        super().__init__(timeout=180)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id

        select = discord.ui.Select(
            placeholder="Select role(s) to rank...",
            min_values=1,
            max_values=len(OW_ROLE_DISPLAY_ORDER),
            options=[
                discord.SelectOption(
                    label=r, value=r,
                    description="Currently rated" if r in rated else "Currently unranked",
                    emoji=discord.PartialEmoji(name=r.lower(), id=OW_ROLE_EMOJI_ID[r]),
                )
                for r in OW_ROLE_DISPLAY_ORDER
            ],
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        chosen = set(interaction.data["values"])
        roles = [r for r in OW_ROLE_DISPLAY_ORDER if r in chosen]

        mmr_roles = await DatabaseHelper.get_mmr_roles_with_labels(self.game_id)
        if not mmr_roles:
            await interaction.response.edit_message(
                content="No rank labels configured for this game — set them up in "
                        "Game Settings first.", view=None)
            return

        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        member = interaction.guild.get_member(self.user_id)
        name = member.display_name if member else str(self.user_id)

        view = OWSetupRoleRankView(
            self.cog, self.game_id, self.user_id, mmr_roles, stats.platform,
            roles, existing_only=True,
        )
        await interaction.response.edit_message(content=view.prompt(name), view=view)


class OWSetMMRRoleSelectView(discord.ui.View):
    """Which Overwatch role's rating is being set. One role per edit."""

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id

        select = discord.ui.Select(
            placeholder="Select role...",
            options=[
                discord.SelectOption(
                    label=r, value=r,
                    emoji=discord.PartialEmoji(name=r.lower(), id=OW_ROLE_EMOJI_ID[r]),
                )
                for r in OW_ROLE_DISPLAY_ORDER
            ],
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role = interaction.data["values"][0]
        current = await DatabaseHelper.get_all_ow_role_stats(self.user_id, self.game_id)
        await interaction.response.send_modal(
            SetMMRModal(self.cog, self.user_id, self.game_id, role=role,
                        current=current.get(role))
        )


async def apply_platform_change(interaction, cog, game_id, user_id, new_platform):
    """Update a player's platform tag (admin-only label).

    The PC bump is a *one-time seed prior* applied at setup, not a permanent
    offset, so changing the tag later does NOT re-seed MMR -- a player's rank
    role drifts up with their bumped MMR, so the pre-bump rank can't be reliably
    recovered here. To re-seed with the correct bump, re-run "Setup New User"
    (it re-picks the rank and re-applies the seed cleanly)."""
    game = await DatabaseHelper.get_game(game_id)
    stats = await DatabaseHelper.get_player_stats(user_id, game_id)
    old_platform = stats.platform
    stats.platform = new_platform
    await DatabaseHelper.update_player_stats(stats)

    member = interaction.guild.get_member(user_id)
    name = member.display_name if member else str(user_id)

    msg = f"Set **{name}**'s platform for {game.name}: {old_platform.upper()} → {new_platform.upper()}.\nMMR unchanged ({stats.mmr})."
    if stats.games_played == 0:
        msg += "\nTo re-seed their MMR with the new platform's bump, re-run **Setup New User**."
    await interaction.response.edit_message(content=msg, view=None)
    await cog.log_action(
        interaction.guild,
        f"Platform change: **{name}** ({game.name}) {old_platform.upper()} → {new_platform.upper()} by {interaction.user.display_name}",
        prefix="❕"
    )


class SetPlatformChoiceView(discord.ui.View):
    """Console/PC buttons for editing an existing player's platform."""

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id

    @discord.ui.button(label="Console", style=discord.ButtonStyle.primary)
    async def console_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await apply_platform_change(interaction, self.cog, self.game_id, self.user_id, 'console')

    @discord.ui.button(label="PC", style=discord.ButtonStyle.success)
    async def pc_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await apply_platform_change(interaction, self.cog, self.game_id, self.user_id, 'pc')


class SetPlatformUserSelectView(discord.ui.View):
    """Select a user then a (PC-enabled) game to edit their platform."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=60)
        self.cog = cog
        self.games = games
        self.selected_user = None

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_user = select.values[0]

        async def show_platform_choice(inter: discord.Interaction, game_id: int):
            stats = await DatabaseHelper.get_player_stats(self.selected_user.id, game_id)
            view = SetPlatformChoiceView(self.cog, game_id, self.selected_user.id)
            await inter.response.edit_message(
                content=(
                    f"Platform for **{self.selected_user.display_name}** "
                    f"(currently **{stats.platform.upper()}**, {stats.games_played} games played).\n"
                    f"Choose their platform:"
                ),
                view=view
            )

        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(self.games, show_platform_choice))
        await interaction.response.edit_message(
            content=f"Setting platform for **{self.selected_user.display_name}**\nSelect a game:",
            view=view
        )


class SetMMRModal(discord.ui.Modal, title="Set Player MMR"):
    mmr_value = discord.ui.TextInput(
        label="MMR Value",
        placeholder="e.g., 1500",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, game_id: int,
                 role: Optional[str] = None, current: Optional['PlayerRoleStats'] = None):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.game_id = game_id
        # Set for Overwatch: the rating lives per role, not on the aggregate column.
        self.role = role
        if role:
            self.title = f"Set {role} MMR"
            self.mmr_value.label = f"{role} MMR"
            if current is not None:
                self.mmr_value.default = str(current.effective_mmr)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)
            return

        member = interaction.guild.get_member(self.user_id)
        name = member.display_name if member else str(self.user_id)
        game = await DatabaseHelper.get_game(self.game_id)

        if self.role:
            await self._submit_role(interaction, game, name, mmr)
            return

        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        old_mmr = stats.mmr
        stats.mmr = mmr
        await DatabaseHelper.update_player_stats(stats)

        # Overwatch balances off per-role MMR, not this column.
        await DatabaseHelper.sync_ow_role_seed(
            self.user_id, self.game_id, stats.effective_mmr, game
        )

        await interaction.response.send_message(
            f"Set **{name}**'s MMR for {game.name} to {mmr}.",
            ephemeral=True
        )
        await self.cog.update_mmr_roles(interaction.guild, self.user_id, self.game_id, stats.effective_mmr)
        await self.cog.log_action(
            interaction.guild,
            f"MMR updated for **{name}** ({game.name}): {old_mmr} \u2192 {mmr} (by {interaction.user.display_name})",
            prefix="\u2755"
        )

    async def _submit_role(self, interaction: discord.Interaction, game, name: str, mmr: int):
        """Write one Overwatch role's rating, then re-derive the visible rank."""
        bands = sorted((await DatabaseHelper.get_mmr_roles(self.game_id)).values())
        floor = bands[0] if bands else 500
        ceiling = max(bands[-1], floor) if bands else 6000
        # A value under the lowest band leaves update_mmr_roles with no role to
        # grant: it strips the player's current rank role and hands back nothing.
        if not (floor <= mmr <= ceiling):
            await interaction.response.send_message(
                f"MMR must be between {floor} and {ceiling} "
                f"(the configured rank ladder for {game.name}).",
                ephemeral=True
            )
            return

        role_stats = await DatabaseHelper.get_ow_role_stats(self.user_id, self.game_id, self.role)
        old = role_stats.effective_mmr
        existed = not role_stats.is_new
        role_stats.mmr = mmr - role_stats.admin_offset  # keep effective_mmr == mmr
        role_stats.is_new = False
        await DatabaseHelper.upsert_ow_role_stats(role_stats)

        # The rank role tracks the player's best role, so re-derive it rather
        # than assuming this edit was the high-water mark.
        peak = await DatabaseHelper.get_ow_player_peak_mmr(self.user_id, self.game_id) or mmr
        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        stats.mmr = peak - stats.admin_offset
        await DatabaseHelper.update_player_stats(stats)
        await self.cog.update_mmr_roles(interaction.guild, self.user_id, self.game_id, peak)

        was = f"{old} \u2192 " if existed else "previously unranked, "
        await interaction.response.edit_message(
            content=f"Set **{name}**'s {OW_ROLE_EMOJI[self.role]} **{self.role}** MMR "
                    f"for {game.name}: {was}**{mmr}**.\n"
                    f"-# Rank role now follows their best role ({peak} MMR).",
            view=None
        )
        await self.cog.log_action(
            interaction.guild,
            f"OW role MMR updated for **{name}** ({game.name}): {self.role} "
            f"{old if existed else 'unranked'} \u2192 {mmr} (by {interaction.user.display_name})",
            prefix="\u2755"
        )


class MMRViewUserSelectView(discord.ui.View):
    """View for viewing player MMR history with user and game selection."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=120)
        self.cog = cog
        self.games = games
        self.selected_user = None

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        self.selected_user = select.values[0]
        selected_user = self.selected_user  # capture for closure

        async def show_mmr_history(inter: discord.Interaction, game_id: int):
            try:
                # Defer as component update so we can edit the same message
                await inter.response.defer()

                # Get player stats
                stats = await DatabaseHelper.get_player_stats(selected_user.id, game_id)
                game = await DatabaseHelper.get_game(game_id)

                if not game:
                    await inter.edit_original_response(content="Game not found.", embed=None, view=None)
                    return

                # Get MMR history (last 10 games)
                async with DatabaseHelper._get_db() as db:
                    async with db.execute(
                        """SELECT match_id, mmr_before, mmr_after, change, timestamp
                           FROM mmr_history
                           WHERE player_id = ? AND game_id = ?
                           ORDER BY timestamp DESC LIMIT 10""",
                        (selected_user.id, game_id)
                    ) as cursor:
                        history = await cursor.fetchall()

                # Build embed
                is_valorant = 'valorant' in game.name.lower()
                is_rivals = 'rivals' in game.name.lower()
                ign = await DatabaseHelper.get_player_ign(selected_user.id, game_id)
                tracker_url = None
                if is_valorant and ign and '#' in ign:
                    ign_name, ign_tag = ign.rsplit('#', 1)
                    from urllib.parse import quote
                    tracker_url = f"https://tracker.gg/valorant/profile/riot/{quote(ign_name)}%23{quote(ign_tag)}/overview"
                elif is_rivals and ign:
                    from urllib.parse import quote
                    tracker_url = f"https://tracker.gg/rivals/profile/{quote(ign)}/overview"

                embed = discord.Embed(
                    title=f"User Overview - {selected_user.display_name}",
                    color=discord.Color.blue(),
                    url=tracker_url
                )
                embed.set_thumbnail(url=selected_user.display_avatar.url)

                # Current stats
                stats_value = ""
                if ign:
                    stats_value += f"**IGN:** `{ign}`\n"
                stats_value += (
                    f"**Current MMR:** {stats.mmr}\n"
                    f"**Effective MMR:** {stats.effective_mmr}\n"
                    f"**Games Played:** {stats.games_played}\n"
                    f"**W/L:** {stats.wins}/{stats.losses}"
                )
                if getattr(game, 'pc_enabled', False):
                    stats_value += f"\n**Platform:** {stats.platform.upper()}"
                if tracker_url:
                    stats_value += f"\n[View on Tracker.gg]({tracker_url})"
                embed.add_field(
                    name=f"{game.name} Stats",
                    value=stats_value,
                    inline=False
                )

                # Roles (Overwatch: a rating per role, so show all three).
                # This is where a player's Overwatch rank actually lives now --
                # a single Discord rank badge could only ever report one of the
                # three honestly, so it reports none of them.
                if is_overwatch_game(game):
                    role_stats = await DatabaseHelper.get_all_ow_role_stats(selected_user.id, game_id)
                    bands = await DatabaseHelper.get_mmr_roles_with_labels(game_id)

                    def band_label(mmr):
                        best = None
                        for data in bands.values():
                            if data['mmr'] <= mmr and (best is None or data['mmr'] > best['mmr']):
                                best = data
                        return (best['label'] or f"{best['mmr']} MMR") if best else "unranked"

                    role_lines = []
                    for role in OW_ROLE_DISPLAY_ORDER:
                        emoji = OW_ROLE_EMOJI[role]
                        rs = role_stats.get(role)
                        if rs:
                            role_lines.append(
                                f"{emoji} **{role}:** {band_label(rs.effective_mmr)} "
                                f"· {rs.effective_mmr} MMR · {rs.games_played}g "
                                f"({rs.wins}W/{rs.losses}L)"
                            )
                        else:
                            # No stored row: get_ow_role_stats would seed this a
                            # band below their best if they queued it, so say so
                            # rather than implying a rating they don't have.
                            seeded = await DatabaseHelper.get_ow_role_stats(
                                selected_user.id, game_id, role)
                            role_lines.append(
                                f"{emoji} **{role}:** *unranked* "
                                f"— would seed at {seeded.effective_mmr}"
                            )
                    sel = await DatabaseHelper.get_ow_role_selection(selected_user.id, game_id)
                    if sel:
                        queued = " ".join(OW_ROLE_EMOJI[r] for r, _rank in sel)
                        role_lines.append(f"\n**Queues as:** {queued}")
                    embed.add_field(name="Role Ranks", value="\n".join(role_lines), inline=False)

                # Roles (Rivals only)
                if 'rivals' in game.name.lower():
                    role_prefs = await DatabaseHelper.get_player_role_prefs(selected_user.id, game_id)
                    role_emojis = _resolve_role_emojis(await DatabaseHelper.get_role_emojis(), inter.client)
                    if role_prefs:
                        primary_role, secondary_role = role_prefs
                        p_emoji = role_emojis.get(primary_role, "")
                        s_emoji = role_emojis.get(secondary_role, "") if secondary_role else role_emojis.get("none", "")
                        role_lines = f"{p_emoji} **Primary:** {primary_role.title()}"
                        if secondary_role:
                            role_lines += f"\n{s_emoji} **Secondary:** {secondary_role.title()}"
                        else:
                            role_lines += f"\n{s_emoji} **Secondary:** None"
                    else:
                        none_emoji = role_emojis.get("none", "")
                        role_lines = f"{none_emoji} **Primary:** Not set\n{none_emoji} **Secondary:** Not set"
                    embed.add_field(name="Roles", value=role_lines, inline=False)

                # History
                if history:
                    history_lines = []
                    for m_id, mmr_before, mmr_after, change, timestamp in history:
                        sign = "+" if change >= 0 else ""
                        history_lines.append(f"`#{m_id}` {mmr_before} \u2192 {mmr_after} ({sign}{change})")

                    embed.add_field(
                        name="Last 10 Games",
                        value="\n".join(history_lines),
                        inline=False
                    )
                else:
                    embed.add_field(
                        name="Last 10 Games",
                        value="No match history found.",
                        inline=False
                    )

                await inter.edit_original_response(content=None, embed=embed, view=None)
            except Exception as e:
                logger.error(f"Error in show_mmr_history: {e}")
                try:
                    if inter.response.is_done():
                        await inter.edit_original_response(content=f"Error loading MMR history: {e}", embed=None, view=None)
                    else:
                        await inter.response.edit_message(content=f"Error loading MMR history: {e}", embed=None, view=None)
                except Exception:
                    pass

        view = discord.ui.View(timeout=120)
        view.add_item(GameSelectDropdown(self.games, show_mmr_history))
        await interaction.response.edit_message(
            content=f"Viewing MMR for **{selected_user.display_name}**\nSelect a game:",
            view=view
        )


async def _setup_advance_to_rank(interaction, cog, game_id, user_id, display_name, platform):
    """Advance new-user setup to the rank step (or the manual-MMR fallback),
    carrying the chosen platform through so the PC seed bump can be applied.

    Overwatch forks here: it rates every role separately, so it walks one rank
    dropdown per role instead of a single one for the player."""
    mmr_roles = await DatabaseHelper.get_mmr_roles_with_labels(game_id)
    if not mmr_roles:
        # No labeled ranks configured: fall back to manual MMR entry.
        modal = SetupUserModal(cog, game_id, user_id, platform)
        await interaction.response.send_modal(modal)
        return

    game = await DatabaseHelper.get_game(game_id)
    if game and is_overwatch_game(game):
        view = OWSetupRoleRankView(cog, game_id, user_id, mmr_roles, platform,
                                   list(OW_ROLE_DISPLAY_ORDER))
        await interaction.response.edit_message(content=view.prompt(display_name), view=view)
        return

    view = SetupUserRankSelectView(cog, game_id, user_id, mmr_roles, platform)
    await interaction.response.edit_message(
        content=f"Select rank for **{display_name}**:",
        view=view
    )


UNRANKED_VALUE = "UNRANKED"
# Where a player with no rank on any role starts. This is the Silver band floor
# on the configured Overwatch ladder, deliberately: the old 1000 default sits
# *between* bands, so update_mmr_roles granted those players Bronze and left
# them stranded under the floor.
OW_ALL_UNRANKED_MMR = 1200


class OWSetupRoleRankView(discord.ui.View):
    """One rank dropdown per Overwatch role, asked in sequence.

    Overwatch balances off a rating *per role*, so a single rank for the whole
    player rated a Tank main identically on DPS. Every role is asked for, and
    "Unranked" is a first-class answer: an unranked role gets no row at all, and
    ``DatabaseHelper.get_ow_role_stats`` seeds it a band below the player's best
    rated role if they ever queue it. That keeps ranking a role *later* a real
    operation instead of overwriting a value that already existed.

    ``ranks`` accumulates role -> rank MMR (absent = unranked) across the chain.
    """

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int,
                 mmr_roles: Dict[int, dict], platform: str = 'console',
                 remaining: Optional[List[str]] = None,
                 ranks: Optional[Dict[str, int]] = None,
                 role_ids: Optional[Dict[str, int]] = None,
                 existing_only: bool = False):
        super().__init__(timeout=180)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id
        self.mmr_roles = mmr_roles  # {role_id: {'mmr': int, 'label': str|None}}
        self.platform = platform
        self.remaining = list(remaining or OW_ROLE_DISPLAY_ORDER)
        self.ranks = dict(ranks or {})
        self.role_ids = dict(role_ids or {})
        # True when re-ranking an existing player: don't re-grant the verified
        # role and never lower an aggregate MMR they already earned.
        self.existing_only = existing_only

        options = []
        # Unranked belongs to first-time setup, where the admin is asked about
        # every role and may not know them all. In the re-rank flow they chose
        # exactly which roles to rank, so offering "no rank" there would just be
        # a way to silently discard their own selection.
        if not existing_only:
            options.append(discord.SelectOption(
                label="Unranked",
                value=UNRANKED_VALUE,
                description="No rank yet — seeded from their other roles",
            ))
        # Lowest rung first, matching the single-rank setup dropdown — the ladder
        # reads bottom-up everywhere an admin picks a *player's* rank.
        for role_id, data in sorted(mmr_roles.items(), key=lambda x: x[1]['mmr']):
            options.append(discord.SelectOption(
                label=data['label'] or f"{data['mmr']} MMR",
                description=f"{data['mmr']} MMR",
                value=str(role_id),
            ))

        select = discord.ui.Select(placeholder=f"Select {self.current} rank...",
                                   options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    @property
    def current(self) -> str:
        return self.remaining[0]

    def prompt(self, display_name: str) -> str:
        done = len(self.ranks) + 1
        total = done + len(self.remaining) - 1
        return (f"**{OW_ROLE_EMOJI[self.current]} {self.current}** rank for "
                f"**{display_name}**  ({done}/{total})")

    async def on_select(self, interaction: discord.Interaction):
        choice = interaction.data["values"][0]
        role = self.current
        if choice != UNRANKED_VALUE:
            role_id = int(choice)
            self.ranks[role] = self.mmr_roles[role_id]['mmr']
            self.role_ids[role] = role_id

        member = interaction.guild.get_member(self.user_id)
        name = member.display_name if member else str(self.user_id)

        remaining = self.remaining[1:]
        if remaining:
            nxt = OWSetupRoleRankView(
                self.cog, self.game_id, self.user_id, self.mmr_roles, self.platform,
                remaining, self.ranks, self.role_ids, self.existing_only,
            )
            await interaction.response.edit_message(content=nxt.prompt(name), view=nxt)
            return

        await _ow_finalize_setup(
            interaction, self.cog, self.game_id, self.user_id, self.platform,
            self.ranks, self.role_ids, self.mmr_roles, self.existing_only,
        )


async def _ow_finalize_setup(interaction, cog, game_id, user_id, platform,
                             ranks: Dict[str, int], role_ids: Dict[str, int],
                             mmr_roles: Dict[int, dict], existing_only: bool = False):
    """Persist an Overwatch per-role setup.

    Only roles the admin actually ranked get an ``ow_role_stats`` row -- an
    unranked role is left absent on purpose so ``get_ow_role_stats`` can seed it
    from the player's best rated role at read time.

    Marking *every* role Unranked during setup means "I don't know this player",
    so they land on ``OW_ALL_UNRANKED_MMR`` across the board. Roles they have
    actually played are exempt from that reset: a rating earned over real games
    is not something an admin should be able to wipe by clicking through the
    setup flow, and Set Player MMR exists for when they genuinely mean to.

    The visible rank role and ``player_game_stats.mmr`` follow the player's
    highest role, which is also the reference ``get_ow_role_stats`` uses when it
    seeds a new one.
    """
    game = await DatabaseHelper.get_game(game_id)
    member = interaction.guild.get_member(user_id)
    name = member.display_name if member else str(user_id)

    existing = await DatabaseHelper.get_all_ow_role_stats(user_id, game_id)
    fallback = not ranks and not existing_only
    protected: List[str] = []
    if fallback:
        for role in OW_ROLES:
            prior = existing.get(role)
            if prior is not None and prior.games_played > 0:
                protected.append(role)
            else:
                ranks[role] = OW_ALL_UNRANKED_MMR

    # The PC bump is applied per role, exactly as the single-rank path applies it
    # once -- a PC player out-aims a console player at the same rank on any role.
    written: Dict[str, int] = {}
    for role, rank_mmr in ranks.items():
        mmr_val = await seed_mmr_for_rank(rank_mmr, game, platform)
        stats = await DatabaseHelper.get_ow_role_stats(user_id, game_id, role)
        stats.mmr = mmr_val - stats.admin_offset  # keep effective_mmr == mmr_val
        stats.games_played = 0                    # placement K corrects a misseed
        stats.is_new = False
        await DatabaseHelper.upsert_ow_role_stats(stats)
        written[role] = mmr_val

    # Aggregate MMR tracks the player's best role, read back *after* the writes
    # so it reflects what is actually stored. Deriving it from the pre-write
    # snapshot let a stale high role -- or the all-unranked default when nothing
    # was written at all -- outlive the edit that was supposed to replace it.
    best = await DatabaseHelper.get_ow_player_peak_mmr(user_id, game_id)
    if best is None:
        best = OW_ALL_UNRANKED_MMR

    stats = await DatabaseHelper.get_player_stats(user_id, game_id)
    stats.mmr = best - stats.admin_offset
    stats.platform = platform
    await DatabaseHelper.update_player_stats(stats)

    if member and not existing_only:
        if game.verified_role_id:
            verified_role = interaction.guild.get_role(game.verified_role_id)
            if verified_role and verified_role not in member.roles:
                await member.add_roles(verified_role, reason="Setup new user verification")

    await cog.update_mmr_roles(interaction.guild, user_id, game_id, stats.effective_mmr)

    label_by_mmr = {d['mmr']: (d['label'] or f"{d['mmr']} MMR") for d in mmr_roles.values()}
    lines = [f"{'Roles updated' if existing_only else 'Setup complete'} for "
             f"**{name}** ({game.name}):"]
    for role in OW_ROLE_DISPLAY_ORDER:
        if role in written:
            tag = "Unranked" if fallback else label_by_mmr.get(ranks[role], str(ranks[role]))
            lines.append(f"- {OW_ROLE_EMOJI[role]} **{role}**: {tag} — {written[role]} MMR")
        elif role in protected:
            lines.append(f"- {OW_ROLE_EMOJI[role]} **{role}**: kept at "
                         f"{existing[role].effective_mmr} MMR — "
                         f"{existing[role].games_played} games played, not reset")
        elif role in existing:
            lines.append(f"- {OW_ROLE_EMOJI[role]} **{role}**: unchanged "
                         f"({existing[role].effective_mmr} MMR)")
        else:
            lines.append(f"- {OW_ROLE_EMOJI[role]} **{role}**: unranked — "
                         "seeded from their other roles on first queue")
    if game.pc_enabled:
        lines.append(f"- Platform: {platform.upper()}")
    if game.verified_role_id and not existing_only:
        lines.append("- Verified role assigned")

    await interaction.response.edit_message(content="\n".join(lines), view=None)

    summary = ", ".join(f"{r} {written[r]}" for r in OW_ROLE_DISPLAY_ORDER if r in written)
    log_platform = f" [{platform.upper()}]" if game.pc_enabled else ""
    await cog.log_action(
        interaction.guild,
        f"{'OW role ranks updated' if existing_only else 'New user setup'}: **{name}**"
        f"{log_platform} ({game.name}) — {summary or 'no changes'} by {interaction.user.display_name}",
        prefix="❕"
    )


class SetupUserPlatformSelectView(discord.ui.View):
    """New-user setup platform step (Console/PC), shown before rank when a game
    has PC players enabled."""

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id

    async def _choose(self, interaction: discord.Interaction, platform: str):
        member = interaction.guild.get_member(self.user_id)
        name = member.display_name if member else str(self.user_id)
        await _setup_advance_to_rank(interaction, self.cog, self.game_id, self.user_id, name, platform)

    @discord.ui.button(label="Console", style=discord.ButtonStyle.primary)
    async def console_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, 'console')

    @discord.ui.button(label="PC", style=discord.ButtonStyle.success)
    async def pc_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, 'pc')


class SetupUserGameSelectView(discord.ui.View):
    """Step 1: Select game for new user setup."""

    def __init__(self, cog: 'CustomMatch', games: List[GameConfig]):
        super().__init__(timeout=120)
        self.cog = cog
        self.games = games

        options = [discord.SelectOption(label=g.name, value=str(g.game_id)) for g in games]
        select = discord.ui.Select(placeholder="Select game...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        game_id = int(interaction.data["values"][0])
        view = SetupUserSelectView(self.cog, game_id)
        await interaction.response.edit_message(
            content="Step 2: Select the user to set up:",
            view=view
        )


class SetupUserSelectView(discord.ui.View):
    """Step 2: Select user for setup."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        game = await DatabaseHelper.get_game(self.game_id)
        if game and game.pc_enabled:
            # Crossplay game: ask Console/PC before rank.
            view = SetupUserPlatformSelectView(self.cog, self.game_id, user.id)
            await interaction.response.edit_message(
                content=f"Step 3: Is **{user.display_name}** on Console or PC?",
                view=view
            )
            return
        # Console-only game: go straight to rank (platform stays 'console').
        await _setup_advance_to_rank(interaction, self.cog, self.game_id, user.id, user.display_name, 'console')


class SetupUserRankSelectView(discord.ui.View):
    """Step 3: Select rank label to assign to the new user."""

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int, mmr_roles: Dict[int, dict], platform: str = 'console'):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id
        self.mmr_roles = mmr_roles  # {role_id: {'mmr': int, 'label': str|None}}
        self.platform = platform

        options = []
        for role_id, data in sorted(mmr_roles.items(), key=lambda x: x[1]['mmr']):
            display = data['label'] or f"{data['mmr']} MMR"
            options.append(discord.SelectOption(
                label=display,
                description=f"{data['mmr']} MMR",
                value=str(role_id)
            ))

        select = discord.ui.Select(placeholder="Select rank...", options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        data = self.mmr_roles[role_id]
        rank_mmr = data['mmr']
        label_used = data['label'] or f"{rank_mmr} MMR"

        game = await DatabaseHelper.get_game(self.game_id)

        # Apply the one-time PC seed bump for PC players in crossplay games.
        mmr_val = await seed_mmr_for_rank(rank_mmr, game, self.platform)
        pc_bump = mmr_val - rank_mmr

        # Set MMR + platform
        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        stats.mmr = mmr_val
        stats.platform = self.platform
        await DatabaseHelper.update_player_stats(stats)

        # Overwatch balances off per-role MMR, not this column. Seeded from the
        # post-PC-bump value so a crossplay PC player keeps their offset.
        await DatabaseHelper.sync_ow_role_seed(
            self.user_id, self.game_id, stats.effective_mmr, game
        )

        member = interaction.guild.get_member(self.user_id)

        # Give the selected rank role
        if member:
            rank_role = interaction.guild.get_role(role_id)
            if rank_role and rank_role not in member.roles:
                await member.add_roles(rank_role, reason="Setup new user rank assignment")

        # Give verified role if configured
        if game.verified_role_id and member:
            verified_role = interaction.guild.get_role(game.verified_role_id)
            if verified_role and verified_role not in member.roles:
                await member.add_roles(verified_role, reason="Setup new user verification")

        # Update MMR roles
        await self.cog.update_mmr_roles(interaction.guild, self.user_id, self.game_id, mmr_val)

        name = member.display_name if member else str(self.user_id)
        lines = [f"Setup complete for **{name}** ({game.name}):"]
        lines.append(f"- Rank: {label_used}")
        if game.pc_enabled:
            lines.append(f"- Platform: {self.platform.upper()}")
        if pc_bump > 0:
            lines.append(f"- MMR: {mmr_val} ({rank_mmr} + {pc_bump} PC seed bump)")
        else:
            lines.append(f"- MMR: {mmr_val}")
        if game.verified_role_id:
            lines.append(f"- Verified role assigned")

        await interaction.response.edit_message(content="\n".join(lines), view=None)
        log_platform = f" [{self.platform.upper()}]" if game.pc_enabled else ""
        await self.cog.log_action(
            interaction.guild,
            f"New user setup: **{name}**{log_platform} registered for **{game.name}** with rank **{label_used}** ({mmr_val} MMR) by {interaction.user.display_name}",
            prefix="\u2755"
        )


class SetupUserModal(discord.ui.Modal, title="Setup New User"):
    """Fallback modal used when no rank labels are configured."""
    mmr = discord.ui.TextInput(
        label="Starting MMR",
        placeholder="e.g., 1000",
        default="1000",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int, platform: str = 'console'):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id
        self.platform = platform

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Manual MMR entry (no rank ladder configured): the entered value is
            # authoritative, so no PC bump is applied here -- only the platform tag.
            mmr_val = int(self.mmr.value)

            # Set MMR + platform
            stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
            stats.mmr = mmr_val
            stats.platform = self.platform
            await DatabaseHelper.update_player_stats(stats)

            # Give verified role if configured
            game = await DatabaseHelper.get_game(self.game_id)
            # Overwatch balances off per-role MMR, not this column.
            await DatabaseHelper.sync_ow_role_seed(
                self.user_id, self.game_id, stats.effective_mmr, game
            )
            member = interaction.guild.get_member(self.user_id)

            if game.verified_role_id and member:
                role = interaction.guild.get_role(game.verified_role_id)
                if role and role not in member.roles:
                    await member.add_roles(role)

            # Update MMR roles
            await self.cog.update_mmr_roles(interaction.guild, self.user_id, self.game_id, stats.effective_mmr)

            name = member.display_name if member else str(self.user_id)
            lines = [f"Setup complete for **{name}** ({game.name}):"]
            if game.pc_enabled:
                lines.append(f"- Platform: {self.platform.upper()}")
            lines.append(f"- MMR: {mmr_val}")
            if game.verified_role_id:
                lines.append(f"- Verified role assigned")

            await interaction.response.send_message("\n".join(lines), ephemeral=True)
            log_platform = f" [{self.platform.upper()}]" if game.pc_enabled else ""
            await self.cog.log_action(
                interaction.guild,
                f"New user setup: **{name}**{log_platform} registered for **{game.name}** with {mmr_val} MMR by {interaction.user.display_name}",
                prefix="\u2755"
            )
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class MatchSelectView(BaseMatchView):
    """View for selecting an active match."""

    def __init__(self, cog: 'CustomMatch', matches: List[dict], callback, game_names: Dict[int, str] = None):
        super().__init__(timeout=60)
        self.cog = cog
        self.callback = callback
        game_names = game_names or {}

        options = []
        for m in matches[:25]:  # Discord limit
            game_name = game_names.get(m["game_id"], "Unknown")
            short_id = m.get("short_id") or str(m["match_id"])
            options.append(discord.SelectOption(
                label=f"{short_id} - {game_name}",
                value=str(m["match_id"])
            ))

        if options:
            select = discord.ui.Select(placeholder="Select match...", options=options)
            select.callback = self.on_select
            self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        match_id = int(interaction.data["values"][0])
        try:
            await self.callback(interaction, match_id)
        except Exception as e:
            logger.error(f"Error in MatchSelectView callback: {e}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
                else:
                    await interaction.response.send_message(f"An error occurred: {e}", ephemeral=True)
            except Exception:
                pass

    @classmethod
    async def create(cls, cog: 'CustomMatch', matches: List[dict], callback):
        """Factory method to create MatchSelectView with async game data loading."""
        # Pre-fetch all game names
        game_ids = set(m["game_id"] for m in matches)
        game_names = {}
        for game_id in game_ids:
            game = await DatabaseHelper.get_game(game_id)
            if game:
                game_names[game_id] = game.name
        return cls(cog, matches, callback, game_names)


class ForceWinnerView(BaseMatchView):
    """View for forcing a winner."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id

    @discord.ui.button(label="Red Team Wins", style=discord.ButtonStyle.danger)
    async def red_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.finalize_match(interaction.guild, self.match_id, Team.RED)
        await interaction.followup.send("Red team declared winner.", ephemeral=True)
        short_id = await self.cog._get_match_short_id(self.match_id)
        match = await DatabaseHelper.get_match(self.match_id)
        game = await DatabaseHelper.get_game(match["game_id"]) if match else None
        game_name = game.name if game else "Unknown"
        await self.cog.log_action(
            interaction.guild,
            f"{game_name} match {short_id} force winner: Red \u2014 by {interaction.user.display_name}"
        )

    @discord.ui.button(label="Blue Team Wins", style=discord.ButtonStyle.primary)
    async def blue_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.finalize_match(interaction.guild, self.match_id, Team.BLUE)
        await interaction.followup.send("Blue team declared winner.", ephemeral=True)
        short_id = await self.cog._get_match_short_id(self.match_id)
        match = await DatabaseHelper.get_match(self.match_id)
        game = await DatabaseHelper.get_game(match["game_id"]) if match else None
        game_name = game.name if game else "Unknown"
        await self.cog.log_action(
            interaction.guild,
            f"{game_name} match {short_id} force winner: Blue \u2014 by {interaction.user.display_name}"
        )


class SubstituteOutPlayerView(discord.ui.View):
    """Dropdown to select which player to replace in a match."""

    def __init__(self, cog: 'CustomMatch', match_id: int, options: list, skip_reshuffle: bool = False):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.skip_reshuffle = skip_reshuffle
        select = discord.ui.Select(placeholder="Select player to replace...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    @classmethod
    async def create(cls, cog, match_id, guild, skip_reshuffle: bool = False):
        players = await DatabaseHelper.get_match_players(match_id)
        igns = await DatabaseHelper.get_match_igns(match_id)
        if not players:
            return None
        options = []
        for p in players:
            pid = p["player_id"]
            team = p["team"].capitalize()
            member = guild.get_member(pid)
            name = igns.get(pid) or (member.display_name if member else str(pid))
            options.append(discord.SelectOption(label=f"{team} \u2014 {name}"[:100], value=str(pid)))
        return cls(cog, match_id, options, skip_reshuffle=skip_reshuffle)

    async def on_select(self, interaction: discord.Interaction):
        out_id = int(interaction.data["values"][0])
        view = SubstituteInPlayerView(self.cog, self.match_id, out_id, skip_reshuffle=self.skip_reshuffle)
        await interaction.response.edit_message(content="Select the substitute player:", view=view)


class SubstituteInPlayerView(discord.ui.View):
    """UserSelect to pick a substitute player."""

    def __init__(self, cog: 'CustomMatch', match_id: int, out_id: int, skip_reshuffle: bool = False):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.out_id = out_id
        self.skip_reshuffle = skip_reshuffle

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select substitute...")
    async def select_sub(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        in_member = select.values[0]
        in_id = in_member.id

        if in_id == self.out_id:
            await interaction.response.edit_message(content="Cannot substitute a player with themselves.", view=None)
            return

        # Validate incoming player is a guild Member (not just a User object)
        if not isinstance(in_member, discord.Member):
            in_member = interaction.guild.get_member(in_id)
            if not in_member:
                await interaction.response.edit_message(content="That user is not in this server.", view=None)
                return

        # Defer immediately to prevent interaction timeout
        await interaction.response.defer()

        match = await DatabaseHelper.get_match(self.match_id)
        if not match:
            await interaction.edit_original_response(content="Match not found.")
            return

        if match.get("winning_team") or match.get("cancelled"):
            await interaction.edit_original_response(content="This match has already ended.")
            return

        game = await DatabaseHelper.get_game(match["game_id"])

        # Check incoming player is not suspended
        suspension = await DatabaseHelper.is_suspended(in_id, match["game_id"])
        if suspension:
            until = suspension.suspended_until.strftime("%Y-%m-%d %H:%M UTC")
            await interaction.edit_original_response(
                content=f"<@{in_id}> is suspended until {until} and cannot sub in."
            )
            return

        # Check incoming player is not already in an active match
        existing_match = await DatabaseHelper.get_active_match_for_player(in_id)
        if existing_match and existing_match["match_id"] != self.match_id:
            await interaction.edit_original_response(
                content=f"<@{in_id}> is already in an active match."
            )
            return

        # Check sub has verified role
        if game.verified_role_id:
            if not any(r.id == game.verified_role_id for r in in_member.roles):
                await interaction.edit_original_response(
                    content="Substitute doesn't have the verified role."
                )
                return

        # Get the outgoing player's team
        players = await DatabaseHelper.get_match_players(self.match_id)
        out_player_data = next((p for p in players if p["player_id"] == self.out_id), None)
        if not out_player_data:
            await interaction.edit_original_response(content="Outgoing player not in this match.")
            return

        team = out_player_data["team"]

        red_role = interaction.guild.get_role(match["red_role_id"])
        blue_role = interaction.guild.get_role(match["blue_role_id"])

        # Check if the incoming player is already in this match — if so, swap them
        in_player_data = next((p for p in players if p["player_id"] == in_id), None)
        if in_player_data:
            in_team = in_player_data["team"]
            if in_team == team:
                await interaction.edit_original_response(
                    content="Both players are on the same team. Use this to swap players between teams."
                )
                return

            # Overwatch: a cross-team swap only preserves strict 2-2-2 when both
            # players play the same role (a Tank↔Tank swap keeps each team 2-2-2,
            # but a Tank↔DPS swap breaks both). Block mismatched-role swaps.
            if is_overwatch_game(game):
                out_role = out_player_data.get("role")
                in_role = in_player_data.get("role")
                if out_role != in_role:
                    await interaction.edit_original_response(
                        content=(
                            f"⚠️ Can't swap — <@{self.out_id}> plays "
                            f"**{out_role or 'unknown'}** and <@{in_id}> plays "
                            f"**{in_role or 'unknown'}**. In Overwatch you can only swap two "
                            f"players of the **same role** (to keep teams 2-2-2). "
                            f"Use **Shuffle Teams** to rebalance instead."
                        )
                    )
                    return

            # Swap teams in DB
            await DatabaseHelper.update_match_player_team(self.match_id, self.out_id, in_team)
            await DatabaseHelper.update_match_player_team(self.match_id, in_id, team)

            # Swap Discord roles
            out_member = interaction.guild.get_member(self.out_id)
            out_old_role = red_role if team == "red" else blue_role
            out_new_role = blue_role if team == "red" else red_role
            in_old_role = out_new_role
            in_new_role = out_old_role
            for member, old_r, new_r in [
                (out_member, out_old_role, out_new_role),
                (in_member, in_old_role, in_new_role),
            ]:
                if member and old_r and new_r:
                    try:
                        await member.remove_roles(old_r)
                        await member.add_roles(new_r)
                    except Exception as e:
                        logger.warning(f"Failed to swap roles for {member.id}: {e}")

            sub_msg = f"Swapped <@{self.out_id}> and <@{in_id}> between teams."
            await interaction.edit_original_response(content=sub_msg)
            await self.cog.refresh_match_embeds(interaction.guild, self.match_id, reshuffled=False)

            # Log the swap
            short_id = match.get("short_id") or str(self.match_id)
            log_channel_id = await DatabaseHelper.get_config("log_channel_id")
            if log_channel_id:
                log_channel = interaction.guild.get_channel(int(log_channel_id))
                if log_channel:
                    log_embed = discord.Embed(
                        title=f"Player Swap \u2014 Match {short_id}",
                        description=(
                            f"<@{self.out_id}> \u2192 **{in_team.capitalize()}**\n"
                            f"<@{in_id}> \u2192 **{team.capitalize()}**"
                        ),
                        color=COLOR_NEUTRAL
                    )
                    log_embed.set_footer(text=f"By {interaction.user.display_name}")
                    await log_channel.send(embed=log_embed)

            # Notify match channel
            match_channel = interaction.guild.get_channel(match["channel_id"]) if match.get("channel_id") else None
            if match_channel:
                await match_channel.send(
                    f"<@{self.out_id}> and <@{in_id}> have been swapped between teams."
                )
            return

        # Overwatch: the sub inherits the outgoing player's role so the match
        # stays a valid 2-2-2 (unless a full reshuffle reassigns below).
        out_role = None
        if is_overwatch_game(game):
            for p in await DatabaseHelper.get_match_players(self.match_id):
                if p["player_id"] == self.out_id:
                    out_role = p.get("role")
                    break

        # Get MMR stats for both players to check if reshuffle is needed.
        # Always fetched — the substitute log embed reports them further down.
        out_stats = await DatabaseHelper.get_player_stats(self.out_id, match["game_id"])
        in_stats = await DatabaseHelper.get_player_stats(in_id, match["game_id"])
        mmr_diff = abs(in_stats.effective_mmr - out_stats.effective_mmr)

        role_warning = ""
        if is_overwatch_game(game) and out_role:
            # Compare the ratings for the slot actually being filled. Overwatch
            # never updates the aggregate MMR column, so measuring the swap with
            # it would compare two frozen setup values and never trigger.
            out_rs = await DatabaseHelper.get_ow_role_stats(
                self.out_id, match["game_id"], out_role)
            in_rs = await DatabaseHelper.get_ow_role_stats(
                in_id, match["game_id"], out_role)
            mmr_diff = abs(in_rs.effective_mmr - out_rs.effective_mmr)

            # The sub keeps 2-2-2 intact, but nothing guarantees they queue the
            # role they're inheriting — surface it rather than silently building
            # them a rating for a role they never picked.
            in_sel = {r for r, _ in await DatabaseHelper.get_ow_role_selection(
                in_id, match["game_id"])}
            if in_sel and out_role not in in_sel:
                role_warning = (
                    f"\n⚠️ <@{in_id}> hasn't queued **{out_role}** (plays "
                    f"{', '.join(sorted(in_sel))}) but is filling that slot to keep 2-2-2."
                )

        # Remove old player, add new
        await DatabaseHelper.remove_match_player(self.match_id, self.out_id)
        await DatabaseHelper.add_match_player(
            self.match_id, in_id, team,
            was_sub=True, original_player_id=self.out_id, role=out_role
        )

        # Remove roles from outgoing player
        out_member = interaction.guild.get_member(self.out_id)
        old_role_id = match["red_role_id"] if team == "red" else match["blue_role_id"]
        old_role = interaction.guild.get_role(old_role_id)

        if old_role and out_member:
            try:
                await out_member.remove_roles(old_role)
            except Exception as e:
                logger.warning(f"Failed to remove role from outgoing player {self.out_id}: {e}")

        reshuffled = False
        if mmr_diff > 200 and game and not self.skip_reshuffle:
            # MMR difference too large — reshuffle all players
            all_players = await DatabaseHelper.get_match_players(self.match_id)
            all_player_ids = [p["player_id"] for p in all_players]

            ow_role_map = {}
            if is_overwatch_game(game):
                new_red, new_blue, ow_role_map = await self.cog.balance_teams_overwatch(
                    all_player_ids, game.game_id
                )
            else:
                new_red, new_blue = await self.cog.balance_teams_mmr(all_player_ids, game.game_id)

            # Update team assignments in DB (and OW role assignments)
            for pid in new_red:
                await DatabaseHelper.update_match_player_team(self.match_id, pid, "red")
            for pid in new_blue:
                await DatabaseHelper.update_match_player_team(self.match_id, pid, "blue")
            for pid, role in ow_role_map.items():
                await DatabaseHelper.update_match_player_role(self.match_id, pid, role)

            # Update Discord roles for all players
            if red_role and blue_role:
                for pid in all_player_ids:
                    member = interaction.guild.get_member(pid)
                    if not member:
                        continue
                    try:
                        if pid in new_red:
                            await member.remove_roles(blue_role)
                            await member.add_roles(red_role)
                        else:
                            await member.remove_roles(red_role)
                            await member.add_roles(blue_role)
                    except Exception as e:
                        logger.warning(f"Failed to update roles for player {pid} during reshuffle: {e}")

            reshuffled = True
            team = "red" if in_id in new_red else "blue"
        else:
            # Within 200 MMR — simple swap onto same team
            new_role_id = match["red_role_id"] if team == "red" else match["blue_role_id"]
            new_role = interaction.guild.get_role(new_role_id)
            if new_role:
                try:
                    await in_member.add_roles(new_role)
                except Exception as e:
                    logger.warning(f"Failed to add role to incoming player {in_id}: {e}")

        sub_msg = f"Substituted <@{self.out_id}> with <@{in_id}> on {team} team."
        if reshuffled:
            sub_msg += f"\n\nMMR difference was {mmr_diff} (>200) \u2014 teams have been reshuffled!"
        elif mmr_diff > 200 and self.skip_reshuffle:
            sub_msg += f"\n\nMMR difference was {mmr_diff} (>200) \u2014 reshuffle skipped."
        sub_msg += role_warning

        await interaction.edit_original_response(content=sub_msg)

        # Refresh all match embeds (match channel + queue channel)
        await self.cog.refresh_match_embeds(interaction.guild, self.match_id, reshuffled=reshuffled)

        # Log with embed showing both players + MMR
        short_id = match.get("short_id") or str(self.match_id)
        out_name = out_member.display_name if out_member else str(self.out_id)
        in_name = in_member.display_name
        log_channel_id = await DatabaseHelper.get_config("log_channel_id")
        if log_channel_id:
            log_channel = interaction.guild.get_channel(int(log_channel_id))
            if log_channel:
                log_embed = discord.Embed(
                    title=f"Substitute \u2014 Match {short_id}",
                    color=COLOR_NEUTRAL
                )
                # Overwatch MMR is hidden everywhere else \u2014 report the role slot
                # instead of leaking a rating into the log channel.
                ow_log = is_overwatch_game(game)
                out_tag = f"{OW_ROLE_EMOJI.get(out_role, '')} {out_role}" if ow_log and out_role \
                    else f"[{out_stats.effective_mmr} MMR]"
                in_tag = out_tag if ow_log else f"[{in_stats.effective_mmr} MMR]"
                log_embed.add_field(
                    name="OUT",
                    value=f"<@{self.out_id}> ({out_name})\n{out_tag}",
                    inline=True
                )
                log_embed.add_field(
                    name="IN",
                    value=f"<@{in_id}> ({in_name})\n{in_tag}",
                    inline=True
                )
                if role_warning:
                    log_embed.add_field(
                        name="Role mismatch", value=role_warning.strip(), inline=False
                    )
                if reshuffled:
                    log_embed.add_field(
                        name="Reshuffle",
                        value=("Role-MMR diff" if ow_log else "MMR diff")
                              + f" {mmr_diff} > 200 \u2014 teams rebalanced",
                        inline=False
                    )
                else:
                    log_embed.add_field(
                        name="Team",
                        value=team.capitalize(),
                        inline=False
                    )
                log_embed.set_footer(text=f"By {interaction.user.display_name}")
                await log_channel.send(embed=log_embed)

                # Resend updated teams + MMR embed to log channel
                if game:
                    updated_players = await DatabaseHelper.get_match_players(self.match_id)
                    updated_igns = await DatabaseHelper.get_match_igns(self.match_id)
                    red_team = [p["player_id"] for p in updated_players if p["team"] == "red"]
                    blue_team = [p["player_id"] for p in updated_players if p["team"] == "blue"]
                    if red_role and blue_role:
                        await self.cog._send_mmr_embed_to_log(
                            interaction.guild, game, self.match_id,
                            red_team, blue_team, updated_igns, red_role, blue_role,
                            reshuffled=reshuffled
                        )

        # Notify match channel about the substitution
        match_channel = interaction.guild.get_channel(match["channel_id"]) if match.get("channel_id") else None
        if match_channel:
            if reshuffled:
                # Reshuffle: send a dedicated embed with role pings to the match channel
                updated_players_mc = await DatabaseHelper.get_match_players(self.match_id)
                updated_igns_mc = await DatabaseHelper.get_match_igns(self.match_id)
                red_team_mc = [p["player_id"] for p in updated_players_mc if p["team"] == "red"]
                blue_team_mc = [p["player_id"] for p in updated_players_mc if p["team"] == "blue"]

                reshuffle_embed = discord.Embed(
                    title="Updated Teams",
                    description=f"MMR difference was **{mmr_diff}** (>200) \u2014 teams have been reshuffled.",
                    color=COLOR_NEUTRAL
                )
                reshuffle_embed.set_footer(
                    text=f"Match {short_id} | Sub: {in_member.display_name} in for "
                         f"{out_member.display_name if out_member else str(self.out_id)}"
                )

                red_names_mc = []
                for pid in red_team_mc:
                    if pid in updated_igns_mc:
                        red_names_mc.append(f"`{updated_igns_mc[pid]}`")
                    else:
                        m = interaction.guild.get_member(pid)
                        red_names_mc.append(m.display_name if m else f"<@{pid}>")
                blue_names_mc = []
                for pid in blue_team_mc:
                    if pid in updated_igns_mc:
                        blue_names_mc.append(f"`{updated_igns_mc[pid]}`")
                    else:
                        m = interaction.guild.get_member(pid)
                        blue_names_mc.append(m.display_name if m else f"<@{pid}>")

                reshuffle_embed.add_field(name="Red Team", value="\n".join(red_names_mc) or "\u2014", inline=True)
                reshuffle_embed.add_field(name="Blue Team", value="\n".join(blue_names_mc) or "\u2014", inline=True)

                ping_content = ""
                if red_role:
                    ping_content += red_role.mention
                if blue_role:
                    ping_content += f" {blue_role.mention}"

                await match_channel.send(content=ping_content.strip() or None, embed=reshuffle_embed)
            else:
                notify_msg = f"<@{self.out_id}> has been substituted by <@{in_id}> on **{team.capitalize()}** team."
                await match_channel.send(notify_msg)

        # Update mapvote: swap allowed_voters and remove old player's vote
        mapvote_cog = self.cog.bot.get_cog("mapvote")
        if mapvote_cog:
            try:
                await mapvote_cog.update_voter_for_sub(
                    interaction.guild.id, self.match_id, self.out_id, in_id
                )
                # Reshuffle: bump the map vote embed so it's visible in the refreshed channel
                if reshuffled and match_channel:
                    await mapvote_cog.bump_vote_embed(
                        interaction.guild.id, self.match_id, match_channel
                    )
            except Exception as e:
                logger.warning(f"Failed to update mapvote voters after sub for match {self.match_id}: {e}")


class SwapPlayer1View(discord.ui.View):
    """Dropdown to select the first player for a swap."""

    def __init__(self, cog: 'CustomMatch', match_id: int, players: list, options: list):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.players = players
        select = discord.ui.Select(placeholder="Select first player...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    @classmethod
    async def create(cls, cog, match_id, guild):
        players = await DatabaseHelper.get_match_players(match_id)
        igns = await DatabaseHelper.get_match_igns(match_id)
        if not players:
            return None
        options = []
        for p in players:
            pid = p["player_id"]
            team = p["team"].capitalize()
            member = guild.get_member(pid)
            name = igns.get(pid) or (member.display_name if member else str(pid))
            options.append(discord.SelectOption(label=f"{team} \u2014 {name}"[:100], value=str(pid)))
        return cls(cog, match_id, players, options)

    async def on_select(self, interaction: discord.Interaction):
        p1_id = int(interaction.data["values"][0])
        p1_data = next((p for p in self.players if p["player_id"] == p1_id), None)
        if not p1_data:
            await interaction.response.edit_message(content="Player not found.", view=None)
            return

        # Build dropdown for the opposite team only
        opposite_team = "blue" if p1_data["team"] == "red" else "red"
        igns = await DatabaseHelper.get_match_igns(self.match_id)
        options = []
        for p in self.players:
            if p["team"] == opposite_team:
                pid = p["player_id"]
                team = p["team"].capitalize()
                member = interaction.guild.get_member(pid)
                name = igns.get(pid) or (member.display_name if member else str(pid))
                options.append(discord.SelectOption(label=f"{team} \u2014 {name}"[:100], value=str(pid)))

        if not options:
            await interaction.response.edit_message(content="No players on the opposite team.", view=None)
            return

        view = SwapPlayer2View(self.cog, self.match_id, p1_id, p1_data, self.players, options)
        await interaction.response.edit_message(content="Select the second player to swap with:", view=view)


class SwapPlayer2View(discord.ui.View):
    """Dropdown to select the second player for a swap (opposite team only)."""

    def __init__(self, cog: 'CustomMatch', match_id: int, p1_id: int,
                 p1_data: dict, players: list, options: list):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id
        self.p1_id = p1_id
        self.p1_data = p1_data
        self.players = players
        select = discord.ui.Select(placeholder="Select second player...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        p2_id = int(interaction.data["values"][0])
        p2_data = next((p for p in self.players if p["player_id"] == p2_id), None)
        if not p2_data:
            await interaction.response.edit_message(content="Player not found.", view=None)
            return

        match = await DatabaseHelper.get_match(self.match_id)
        game = await DatabaseHelper.get_game(match["game_id"])

        # Overwatch: a cross-team swap only keeps strict 2-2-2 when both players
        # share the same role. Block mismatched-role swaps.
        if is_overwatch_game(game):
            p1_role = self.p1_data.get("role")
            p2_role = p2_data.get("role")
            if p1_role != p2_role:
                await interaction.response.edit_message(
                    content=(
                        f"⚠️ Can't swap — <@{self.p1_id}> plays **{p1_role or 'unknown'}** and "
                        f"<@{p2_id}> plays **{p2_role or 'unknown'}**. In Overwatch you can only "
                        f"swap two players of the **same role** (to keep teams 2-2-2). "
                        f"Use **Shuffle Teams** to rebalance instead."
                    ),
                    view=None
                )
                return

        # Swap teams in database (preserving each player's OW role record)
        await DatabaseHelper.remove_match_player(self.match_id, self.p1_id)
        await DatabaseHelper.remove_match_player(self.match_id, p2_id)
        await DatabaseHelper.add_match_player(
            self.match_id, self.p1_id, p2_data["team"],
            was_captain=self.p1_data["was_captain"], role=self.p1_data.get("role")
        )
        await DatabaseHelper.add_match_player(
            self.match_id, p2_id, self.p1_data["team"],
            was_captain=p2_data["was_captain"], role=p2_data.get("role")
        )

        # Swap roles
        p1_member = interaction.guild.get_member(self.p1_id)
        p2_member = interaction.guild.get_member(p2_id)

        red_role = interaction.guild.get_role(match["red_role_id"])
        blue_role = interaction.guild.get_role(match["blue_role_id"])

        # Swap roles for both players
        if red_role and blue_role:
            try:
                if self.p1_data["team"] == "red":
                    if p1_member:
                        await p1_member.remove_roles(red_role)
                        await p1_member.add_roles(blue_role)
                    if p2_member:
                        await p2_member.remove_roles(blue_role)
                        await p2_member.add_roles(red_role)
                else:
                    if p1_member:
                        await p1_member.remove_roles(blue_role)
                        await p1_member.add_roles(red_role)
                    if p2_member:
                        await p2_member.remove_roles(red_role)
                        await p2_member.add_roles(blue_role)
            except Exception as e:
                logger.warning(f"Failed to swap roles for match {self.match_id}: {e}")

        await interaction.response.edit_message(
            content=f"Swapped <@{self.p1_id}> and <@{p2_id}>.",
            view=None
        )

        # Refresh all match embeds (match channel + queue channel)
        await self.cog.refresh_match_embeds(interaction.guild, self.match_id)

        # Log with embed showing both players + MMR
        short_id = match.get("short_id") or str(self.match_id)
        p1_name = p1_member.display_name if p1_member else str(self.p1_id)
        p2_name = p2_member.display_name if p2_member else str(p2_id)
        p1_old_team = self.p1_data["team"].capitalize()
        p2_old_team = p2_data["team"].capitalize()
        p1_stats = await DatabaseHelper.get_player_stats(self.p1_id, match["game_id"])
        p2_stats = await DatabaseHelper.get_player_stats(p2_id, match["game_id"])
        log_channel_id = await DatabaseHelper.get_config("log_channel_id")
        if log_channel_id:
            log_channel = interaction.guild.get_channel(int(log_channel_id))
            if log_channel:
                swap_desc = (
                    f"<@{self.p1_id}> ({p1_name})\n"
                    f"[{p1_stats.effective_mmr} MMR] {p1_old_team} \u2192 {p2_old_team}\n"
                    f"<@{p2_id}> ({p2_name})\n"
                    f"[{p2_stats.effective_mmr} MMR] {p2_old_team} \u2192 {p1_old_team}"
                )
                log_embed = discord.Embed(
                    title=f"Swap \u2014 Match {short_id}",
                    description=swap_desc,
                    color=COLOR_NEUTRAL
                )
                log_embed.set_footer(text=f"By {interaction.user.display_name}")
                await log_channel.send(embed=log_embed)

                # Resend updated teams + MMR embed to log channel
                game = await DatabaseHelper.get_game(match["game_id"])
                if game:
                    updated_players = await DatabaseHelper.get_match_players(self.match_id)
                    updated_igns = await DatabaseHelper.get_match_igns(self.match_id)
                    red_team = [p["player_id"] for p in updated_players if p["team"] == "red"]
                    blue_team = [p["player_id"] for p in updated_players if p["team"] == "blue"]
                    red_role = interaction.guild.get_role(match["red_role_id"])
                    blue_role = interaction.guild.get_role(match["blue_role_id"])
                    if red_role and blue_role:
                        await self.cog._send_mmr_embed_to_log(
                            interaction.guild, game, self.match_id,
                            red_team, blue_team, updated_igns, red_role, blue_role
                        )

        # Notify match channel about the swap
        match_channel = interaction.guild.get_channel(match["channel_id"]) if match.get("channel_id") else None
        if match_channel:
            await match_channel.send(
                f"<@{self.p1_id}> ({p1_old_team}) and <@{p2_id}> ({p2_old_team}) have been swapped."
            )

        # Edit the existing queue channel teams embed in place (no ping, no new message)
        game_swap = await DatabaseHelper.get_game(match["game_id"])
        queue_channel = None
        if game_swap and game_swap.queue_channel_id:
            queue_channel = interaction.guild.get_channel(game_swap.queue_channel_id)
        if not queue_channel and game_swap:
            for _qstate in self.cog.queues.values():
                if _qstate.game_id == game_swap.game_id and _qstate.channel_id:
                    queue_channel = interaction.guild.get_channel(_qstate.channel_id)
                    if queue_channel:
                        break

        updated_players_lobby = await DatabaseHelper.get_match_players(self.match_id)
        updated_igns_lobby = await DatabaseHelper.get_match_igns(self.match_id)
        red_team_lobby = [p["player_id"] for p in updated_players_lobby if p["team"] == "red"]
        blue_team_lobby = [p["player_id"] for p in updated_players_lobby if p["team"] == "blue"]

        red_names_lobby = []
        for pid in red_team_lobby:
            if pid in updated_igns_lobby:
                red_names_lobby.append(f"`{updated_igns_lobby[pid]}`")
            else:
                m = interaction.guild.get_member(pid)
                red_names_lobby.append(m.display_name if m else f"<@{pid}>")
        blue_names_lobby = []
        for pid in blue_team_lobby:
            if pid in updated_igns_lobby:
                blue_names_lobby.append(f"`{updated_igns_lobby[pid]}`")
            else:
                m = interaction.guild.get_member(pid)
                blue_names_lobby.append(m.display_name if m else f"<@{pid}>")

        # Edit the existing ongoing match embed in the queue channel
        if queue_channel and match.get("queue_teams_msg_id"):
            try:
                teams_msg = await queue_channel.fetch_message(match["queue_teams_msg_id"])
                updated_embed = discord.Embed(
                    title="Ongoing Match",
                    url=match_channel.jump_url if match_channel else None,
                    color=COLOR_NEUTRAL
                )
                updated_embed.set_footer(text=f"Match {short_id}")
                updated_embed.add_field(name="Red Team", value="\n".join(red_names_lobby) or "\u2014", inline=True)
                updated_embed.add_field(name="Blue Team", value="\n".join(blue_names_lobby) or "\u2014", inline=True)
                await teams_msg.edit(
                    embed=updated_embed,
                    view=self.cog._build_listen_view(self.match_id, match)
                )
            except Exception as e:
                logger.warning(f"Failed to edit queue teams embed after swap for match {self.match_id}: {e}")

        # Send updated lineup notification to match lobby channel with role pings
        if match_channel:
            try:
                lobby_embed = discord.Embed(
                    title="Ongoing Match \u2014 Updated Lineup",
                    color=COLOR_NEUTRAL
                )
                lobby_embed.set_footer(text=f"Match {short_id} | Swap: {p1_name} \u2194 {p2_name}")
                lobby_embed.add_field(name="Red Team", value="\n".join(red_names_lobby) or "\u2014", inline=True)
                lobby_embed.add_field(name="Blue Team", value="\n".join(blue_names_lobby) or "\u2014", inline=True)

                ping_content = ""
                if red_role:
                    ping_content += red_role.mention
                if blue_role:
                    ping_content += f" {blue_role.mention}"

                await match_channel.send(content=ping_content.strip() or None, embed=lobby_embed)
            except Exception as e:
                logger.warning(f"Failed to send updated lineup to match channel after swap for match {self.match_id}: {e}")


# =============================================================================
# VERIFICATION TICKET VIEW
# =============================================================================

class VerificationTicketView(discord.ui.View):
    """View with button to open a verification ticket."""

    def __init__(self, cog: 'CustomMatch', game: 'GameConfig'):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

    @discord.ui.button(label="Open Verification Ticket", style=discord.ButtonStyle.primary)
    async def open_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        ticketing_cog = self.cog.bot.get_cog("TicketSystem")
        if not ticketing_cog:
            await interaction.response.send_message(
                "Ticketing system is not available. Please contact an admin.",
                ephemeral=True
            )
            return

        # Load topics and find the verification topic
        import os
        import json
        topics_file = os.path.join("data", "topics.json")
        if not os.path.exists(topics_file):
            await interaction.response.send_message(
                "No ticket topics configured. Please contact an admin.",
                ephemeral=True
            )
            return

        def _load_topics():
            with open(topics_file, "r", encoding="utf-8") as f:
                return json.load(f)
        topics = await asyncio.to_thread(_load_topics)

        if self.game.verification_topic not in topics:
            await interaction.response.send_message(
                f"Verification topic '{self.game.verification_topic}' not found. Please contact an admin.",
                ephemeral=True
            )
            return

        topic = topics[self.game.verification_topic]

        # Create the ticket channel
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            ch = await ticketing_cog._create_discussion_channel(
                interaction, topic, interaction.user, is_ticket=True
            )
            if ch:
                await interaction.followup.send(
                    f"Your verification ticket has been created: {ch.mention}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "Failed to create ticket. Please contact an admin.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error creating verification ticket: {e}")
            await interaction.followup.send(
                f"Error creating ticket: {e}",
                ephemeral=True
            )


# =============================================================================
# QUEUE VIEWS
# =============================================================================

class QueueView(BaseMatchView):
    """Main queue view with Join/Leave buttons. Uses dynamic custom_ids for resilience."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id

        # Create buttons with dynamic custom_ids including queue_id for resilience
        join_btn = discord.ui.Button(
            label="Join",
            style=discord.ButtonStyle.success,
            custom_id=f"cm_queue_join:{queue_id}"
        )
        join_btn.callback = self.join_callback
        self.add_item(join_btn)

        leave_btn = discord.ui.Button(
            label="Leave",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_queue_leave:{queue_id}"
        )
        leave_btn.callback = self.leave_callback
        self.add_item(leave_btn)

        menu_btn = discord.ui.Button(
            label="\u2630",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cm_queue_menu:{queue_id}"
        )
        menu_btn.callback = self.menu_callback
        self.add_item(menu_btn)

    async def join_callback(self, interaction: discord.Interaction):
        await self.cog.handle_queue_join(interaction, self.game_id, self.queue_id)

    async def leave_callback(self, interaction: discord.Interaction):
        await self.cog.handle_queue_leave(interaction, self.game_id, self.queue_id)

    async def menu_callback(self, interaction: discord.Interaction):
        """Open the queue menu with DM request options."""
        current_sub = await DatabaseHelper.get_player_subscription(self.queue_id, interaction.user.id)
        queue_state = self.cog.queues.get(self.queue_id)
        player_count = len(queue_state.players) if queue_state else 0
        game = await DatabaseHelper.get_game(self.game_id)
        view = QueueMenuView(
            self.cog, self.game_id, self.queue_id, current_sub, player_count,
            is_overwatch=is_overwatch_game(game),
        )
        if current_sub is not None:
            msg = f"**Queue Menu**\nYou have a DM request active for when **{current_sub}** more needed."
        else:
            msg = "**Queue Menu**\nRequest a DM when the queue is almost full."
        await interaction.response.send_message(msg, view=view, ephemeral=True)


class QueueMenuView(BaseMatchView):
    """Ephemeral menu for queue options (DM requests, etc.)."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int, current_threshold: Optional[int], player_count: int = 0, is_overwatch: bool = False):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id
        self.player_count = player_count

        # Overwatch: let a queued player change their roles without leaving.
        if is_overwatch:
            edit_roles_btn = discord.ui.Button(
                label="Edit Roles",
                style=discord.ButtonStyle.primary,
                emoji="\U0001f6e1️",
                custom_id=f"cm_queue_editroles:{queue_id}",
                row=2,
            )
            edit_roles_btn.callback = self.edit_roles_callback
            self.add_item(edit_roles_btn)

        options = [
            discord.SelectOption(label="Request DM \u2014 1 more needed", value="1", description="DM me when 1 more player is needed"),
            discord.SelectOption(label="Request DM \u2014 2 more needed", value="2", description="DM me when 2 more players are needed"),
            discord.SelectOption(label="Request DM \u2014 3 more needed", value="3", description="DM me when 3 more players are needed"),
        ]
        if current_threshold is not None:
            for opt in options:
                if opt.value == str(current_threshold):
                    opt.default = True

        select = discord.ui.Select(
            placeholder="Request a DM...",
            options=options,
            custom_id=f"cm_queue_sub_select:{queue_id}"
        )
        select.callback = self.select_callback
        self.add_item(select)

        if current_threshold is not None:
            cancel_btn = discord.ui.Button(
                label="Cancel DM",
                style=discord.ButtonStyle.danger,
                custom_id=f"cm_queue_sub_cancel:{queue_id}",
                row=1
            )
            cancel_btn.callback = self.cancel_callback
            self.add_item(cancel_btn)

    async def edit_roles_callback(self, interaction: discord.Interaction):
        # Roles are locked once the queue leaves "waiting" (ready check / match),
        # so a mid-check change can't break the teams being formed.
        queue_state = self.cog.queues.get(self.queue_id)
        if queue_state and queue_state.state != "waiting":
            await interaction.response.send_message(
                "Roles are locked during the ready check — you can change them after this match.",
                ephemeral=True,
            )
            return

        game = await DatabaseHelper.get_game(self.game_id)
        current = await DatabaseHelper.get_ow_role_selection(interaction.user.id, self.game_id)
        current_str = (
            "  ".join(f"{OW_ROLE_EMOJI[r]} {r}" for r, _rank in current)
            if current else "*none set*"
        )

        async def after_save(inter: discord.Interaction, ordered):
            await self.cog.handle_ow_role_edit_while_queued(
                inter, self.game_id, self.queue_id, ordered
            )

        view = OWRoleSelectView(
            self.cog, self.game_id, game.name, rejoin_hint=False, after_save=after_save,
            preselected=[r for r, _rank in current],
        )
        await interaction.response.send_message(
            f"**Current roles:** {current_str}\n\nSelect every role you'll play:",
            view=view,
            ephemeral=True,
        )

    async def select_callback(self, interaction: discord.Interaction):
        threshold = int(interaction.data["values"][0])
        # Require at least 4 players in queue before allowing DM requests
        if self.player_count < 4:
            await interaction.response.edit_message(
                content="The lobby needs at least **4 players** before you can request a DM.",
                view=None
            )
            return
        await DatabaseHelper.subscribe_to_queue(self.queue_id, interaction.user.id, threshold)
        await interaction.response.edit_message(
            content=f"You'll be DM'd when **{threshold}** more player{'s are' if threshold > 1 else ' is'} needed. This request expires in **60 minutes**.",
            view=None
        )

    async def cancel_callback(self, interaction: discord.Interaction):
        await DatabaseHelper.unsubscribe_from_queue(self.queue_id, interaction.user.id)
        await interaction.response.edit_message(
            content="Your DM request has been cancelled.",
            view=None
        )


class ReadyCheckView(BaseMatchView):
    """Ready check view. Uses dynamic custom_ids for resilience."""

    def __init__(self, cog: 'CustomMatch', game_id: int, queue_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.queue_id = queue_id

        # Create buttons with dynamic custom_ids including queue_id for resilience
        ready_btn = discord.ui.Button(
            label="Ready",
            style=discord.ButtonStyle.success,
            custom_id=f"cm_ready_yes:{queue_id}"
        )
        ready_btn.callback = self.ready_callback
        self.add_item(ready_btn)

        not_ready_btn = discord.ui.Button(
            label="Not Ready",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_ready_no:{queue_id}"
        )
        not_ready_btn.callback = self.not_ready_callback
        self.add_item(not_ready_btn)

    async def ready_callback(self, interaction: discord.Interaction):
        await self.cog.handle_ready(interaction, self.queue_id, True)

    async def not_ready_callback(self, interaction: discord.Interaction):
        await self.cog.handle_ready(interaction, self.queue_id, False)


class WinVoteView(BaseMatchView):
    """Win vote view with persistent custom_ids."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id

        # Create buttons with dynamic custom_ids including match_id for persistence
        red_btn = discord.ui.Button(
            label="Red Team",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_vote_red:{match_id}"
        )
        red_btn.callback = self.vote_red_callback
        self.add_item(red_btn)

        blue_btn = discord.ui.Button(
            label="Blue Team",
            style=discord.ButtonStyle.primary,
            custom_id=f"cm_vote_blue:{match_id}"
        )
        blue_btn.callback = self.vote_blue_callback
        self.add_item(blue_btn)

    async def vote_red_callback(self, interaction: discord.Interaction):
        await self.cog.handle_win_vote(interaction, self.match_id, Team.RED)

    async def vote_blue_callback(self, interaction: discord.Interaction):
        await self.cog.handle_win_vote(interaction, self.match_id, Team.BLUE)


class AbandonVoteView(BaseMatchView):
    """Abandon vote view with persistent custom_ids."""

    def __init__(self, cog: 'CustomMatch', match_id: int, needed_votes: int):
        super().__init__(timeout=None)  # No timeout for persistence
        self.cog = cog
        self.match_id = match_id
        self.needed_votes = needed_votes

        # Create button with dynamic custom_id including match_id for persistence
        abandon_btn = discord.ui.Button(
            label="Vote to Abandon",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_abandon_vote:{match_id}"
        )
        abandon_btn.callback = self.vote_abandon_callback
        self.add_item(abandon_btn)

    async def vote_abandon_callback(self, interaction: discord.Interaction):
        await self.cog.handle_abandon_vote(interaction, self.match_id, self.needed_votes)


class IGNModal(discord.ui.Modal, title="Set Your IGN"):
    ign = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter your in-game name...",
        required=False,
        max_length=100
    )
    tracker_url = discord.ui.TextInput(
        label="Tracker URL (Optional)",
        placeholder="https://tracker.gg/valorant/profile/riot/Name%23Tag/overview",
        required=False,
        max_length=200
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.game_name = game_name
        self.title = f"Set Your {game_name} IGN"

    async def on_submit(self, interaction: discord.Interaction):
        # If tracker URL provided, parse IGN from it
        tracker_val = self.tracker_url.value.strip() if self.tracker_url.value else ""
        from_tracker = False
        if tracker_val:
            parsed = _parse_tracker_url(tracker_val)
            if parsed:
                ign_value = parsed
                from_tracker = True
            elif self.ign.value and self.ign.value.strip():
                ign_value = self.ign.value.strip()
            else:
                await interaction.response.send_message(
                    "Could not parse an IGN from that tracker URL. Please enter your IGN manually.",
                    ephemeral=True
                )
                return
        elif self.ign.value and self.ign.value.strip():
            ign_value = self.ign.value.strip()
        else:
            await interaction.response.send_message(
                "Please provide either an IGN or a tracker URL.",
                ephemeral=True
            )
            return

        # Validate Valorant IGN format
        is_valorant = 'valorant' in self.game_name.lower()
        if is_valorant:
            if '#' not in ign_value:
                await interaction.response.send_message(
                    "Invalid Valorant IGN format. Must include `#` (e.g., `Username#TAG`).",
                    ephemeral=True
                )
                return

            # Check for space before #
            hash_idx = ign_value.find('#')
            if hash_idx > 0 and ign_value[hash_idx - 1] == ' ':
                await interaction.response.send_message(
                    "Invalid Valorant IGN format. Remove the space before `#` (e.g., `Username#TAG` not `Username #TAG`).",
                    ephemeral=True
                )
                return

            # Check tag is not empty
            name_part = ign_value[:hash_idx]
            tag = ign_value[hash_idx + 1:]
            if not tag:
                await interaction.response.send_message(
                    "Invalid Valorant IGN format. Tag after `#` cannot be empty.",
                    ephemeral=True
                )
                return

            # Validate tag length (3-5 alphanumeric chars)
            if not tag.isalnum() or not (3 <= len(tag) <= 5):
                await interaction.response.send_message(
                    "Invalid Valorant tag. Must be 3-5 alphanumeric characters (e.g., `NA1`, `XOXO`).",
                    ephemeral=True
                )
                return

            # Verify account exists via API
            await interaction.response.defer(ephemeral=True)
            account = await self.cog.henrik_api.get_account(name_part, tag)
            if not account:
                await interaction.followup.send(
                    f"Could not verify `{ign_value}` with the Valorant API. "
                    "Please double-check your Riot ID and try again.",
                    ephemeral=True
                )
                return

            await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, ign_value, puuid=account.get('puuid'))
            await interaction.followup.send(
                f"Your IGN for this game has been set to: `{ign_value}`",
                ephemeral=True
            )
            return

        # Marvel Rivals: verify via API but always store the user's exact input
        if 'rivals' in self.game_name.lower() and self.cog.rivals_api.available:
            await interaction.response.defer(ephemeral=True)
            player = await self.cog.rivals_api.find_player(ign_value)
            await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, ign_value)
            if player or from_tracker:
                await interaction.followup.send(
                    f"Your IGN for this game has been set to: `{ign_value}`",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"Your IGN has been set to: `{ign_value}`\n"
                    "**Note:** Could not verify this name with the Marvel Rivals API. "
                    "If stats matching fails later, double-check your spelling.",
                    ephemeral=True
                )
            return

        await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, ign_value)
        await interaction.response.send_message(
            f"Your IGN for this game has been set to: `{ign_value}`",
            ephemeral=True
        )


class IGNRequiredModal(discord.ui.Modal, title="Set Your IGN"):
    """Modal shown when a player tries to join a queue that requires an IGN."""
    ign = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter your in-game name...",
        required=False,
        max_length=100
    )
    tracker_url = discord.ui.TextInput(
        label="Tracker URL (Optional)",
        placeholder="https://tracker.gg/valorant/profile/riot/Name%23Tag/overview",
        required=False,
        max_length=200
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.game_name = game_name
        self.title = f"Set Your {game_name} IGN"

    async def on_submit(self, interaction: discord.Interaction):
        # If tracker URL provided, parse IGN from it
        tracker_val = self.tracker_url.value.strip() if self.tracker_url.value else ""
        from_tracker = False
        if tracker_val:
            parsed = _parse_tracker_url(tracker_val)
            if parsed:
                ign_value = parsed
                from_tracker = True
            elif self.ign.value and self.ign.value.strip():
                ign_value = self.ign.value.strip()
            else:
                await interaction.response.send_message(
                    "Could not parse an IGN from that tracker URL. Please enter your IGN manually.",
                    ephemeral=True
                )
                return
        elif self.ign.value and self.ign.value.strip():
            ign_value = self.ign.value.strip()
        else:
            await interaction.response.send_message(
                "Please provide either an IGN or a tracker URL.",
                ephemeral=True
            )
            return

        # Validate Valorant IGN format
        is_valorant = 'valorant' in self.game_name.lower()
        if is_valorant:
            if '#' not in ign_value:
                await interaction.response.send_message(
                    "Invalid Valorant IGN format. Must include `#` (e.g., `Username#TAG`).",
                    ephemeral=True
                )
                return

            hash_idx = ign_value.find('#')
            if hash_idx > 0 and ign_value[hash_idx - 1] == ' ':
                await interaction.response.send_message(
                    "Invalid Valorant IGN format. Remove the space before `#` (e.g., `Username#TAG` not `Username #TAG`).",
                    ephemeral=True
                )
                return

            name_part = ign_value[:hash_idx]
            tag = ign_value[hash_idx + 1:]
            if not tag:
                await interaction.response.send_message(
                    "Invalid Valorant IGN format. Tag after `#` cannot be empty.",
                    ephemeral=True
                )
                return

            # Validate tag length (3-5 alphanumeric chars)
            if not tag.isalnum() or not (3 <= len(tag) <= 5):
                await interaction.response.send_message(
                    "Invalid Valorant tag. Must be 3-5 alphanumeric characters (e.g., `NA1`, `XOXO`).",
                    ephemeral=True
                )
                return

            # Verify account exists via API
            await interaction.response.defer(ephemeral=True)
            account = await self.cog.henrik_api.get_account(name_part, tag)
            if not account:
                await interaction.followup.send(
                    f"Could not verify `{ign_value}` with the Valorant API. "
                    "Please double-check your Riot ID and try again.",
                    ephemeral=True
                )
                return

            await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, ign_value, puuid=account.get('puuid'))
            await interaction.followup.send(
                f"Your IGN has been set to: `{ign_value}`. Please click **Join** again to enter the queue.",
                ephemeral=True
            )
            return

        # Marvel Rivals: verify via API but always store the user's exact input
        if 'rivals' in self.game_name.lower() and self.cog.rivals_api.available:
            await interaction.response.defer(ephemeral=True)
            player = await self.cog.rivals_api.find_player(ign_value)
            await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, ign_value)
            if player or from_tracker:
                await interaction.followup.send(
                    f"Your IGN has been set to: `{ign_value}`. Please click **Join** again to enter the queue.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"Your IGN has been set to: `{ign_value}`. Please click **Join** again to enter the queue.\n"
                    "**Note:** Could not verify this name with the Marvel Rivals API. "
                    "If stats matching fails later, double-check your spelling.",
                    ephemeral=True
                )
            return

        await DatabaseHelper.set_player_ign(interaction.user.id, self.game_id, ign_value)
        await interaction.response.send_message(
            f"Your IGN has been set to: `{ign_value}`. Please click **Join** again to enter the queue.",
            ephemeral=True
        )


class ListenInView(BaseMatchView):
    """Grey "Listen In" button on the queue-channel Ongoing Match embed.

    Lets a non-participant join one of the team VCs as a permanently muted
    spectator. Uses a persistent custom_id so the button keeps working after
    a bot restart (handled by CustomMatch.on_interaction)."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id

        btn = discord.ui.Button(
            label="Listen In",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cm_listen_in:{match_id}"
        )
        btn.callback = self.listen_callback
        self.add_item(btn)

    async def listen_callback(self, interaction: discord.Interaction):
        await self.cog.handle_listen_in(interaction, self.match_id)


class ListenInTeamView(BaseMatchView):
    """Ephemeral Red/Blue picker shown after clicking Listen In."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.match_id = match_id

        red_btn = discord.ui.Button(
            label="Red Team",
            style=discord.ButtonStyle.danger,
            custom_id=f"cm_listen_team:{match_id}:red"
        )
        red_btn.callback = self.red_callback
        self.add_item(red_btn)

        blue_btn = discord.ui.Button(
            label="Blue Team",
            style=discord.ButtonStyle.primary,
            custom_id=f"cm_listen_team:{match_id}:blue"
        )
        blue_btn.callback = self.blue_callback
        self.add_item(blue_btn)

    async def red_callback(self, interaction: discord.Interaction):
        await self.cog.handle_listen_in_team(interaction, self.match_id, "red")

    async def blue_callback(self, interaction: discord.Interaction):
        await self.cog.handle_listen_in_team(interaction, self.match_id, "blue")


class PersistentIGNView(BaseMatchView):
    """Persistent view with a button for users to set their IGN."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Set IGN", style=discord.ButtonStyle.primary, custom_id="persistent_ign_set", emoji="\U0001f3ae")
    async def set_ign(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return

        if len(games) == 1:
            game = games[0]
            # Pre-fill with existing IGN if set
            existing = await DatabaseHelper.get_player_ign(interaction.user.id, game.game_id)
            modal = IGNModal(self.cog, game.game_id, game.name)
            if existing:
                modal.ign.default = existing
            await interaction.response.send_modal(modal)
        else:
            async def show_ign_modal(inter: discord.Interaction, game_id: int):
                game = await DatabaseHelper.get_game(game_id)
                existing = await DatabaseHelper.get_player_ign(inter.user.id, game.game_id)
                modal = IGNModal(self.cog, game_id, game.name)
                if existing:
                    modal.ign.default = existing
                await inter.response.send_modal(modal)

            view = discord.ui.View(timeout=60)
            view.add_item(GameSelectDropdown(games, show_ign_modal))
            await interaction.response.send_message("Select a game to set your IGN:", view=view, ephemeral=True)


class RoleSelectModal(discord.ui.Modal, title="Set Your Role Preferences"):
    """Modal for setting Rivals role preferences."""
    primary = discord.ui.TextInput(
        label="Primary Role",
        placeholder="vanguard, duelist, or strategist",
        required=True,
        max_length=20
    )
    secondary = discord.ui.TextInput(
        label="Secondary Role (optional)",
        placeholder="vanguard, duelist, strategist, or fill/flex",
        required=False,
        max_length=20
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.game_name = game_name
        self.title = f"Set Your {game_name} Role"

    async def on_submit(self, interaction: discord.Interaction):
        valid_roles = ['vanguard', 'duelist', 'strategist']
        primary_val = self.primary.value.strip().lower()
        secondary_val = self.secondary.value.strip().lower() if self.secondary.value and self.secondary.value.strip() else None

        if primary_val not in valid_roles:
            await interaction.response.send_message(
                f"Invalid primary role `{primary_val}`. Must be one of: `vanguard`, `duelist`, `strategist`.",
                ephemeral=True
            )
            return

        if secondary_val:
            if secondary_val in ('flex', 'fill/flex'):
                secondary_val = 'fill'
            if secondary_val not in valid_roles + ['fill']:
                await interaction.response.send_message(
                    f"Invalid secondary role `{secondary_val}`. Must be one of: `vanguard`, `duelist`, `strategist`, `fill/flex`.",
                    ephemeral=True
                )
                return
            if secondary_val != "fill" and secondary_val == primary_val:
                await interaction.response.send_message(
                    "Primary and secondary roles must be different.", ephemeral=True
                )
                return
            if secondary_val == "fill":
                secondary_val = None

        await DatabaseHelper.set_player_role_prefs(
            interaction.user.id, self.game_id, primary_val, secondary_val
        )

        desc = f"**Primary:** {primary_val.title()}"
        if secondary_val:
            desc += f"\n**Secondary:** {secondary_val.title()}"
        else:
            desc += "\n**Secondary:** Fill"

        embed = discord.Embed(
            title=f"Role Preferences Updated \u2014 {self.game_name}",
            description=desc,
            color=COLOR_SUCCESS
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


ROLE_SELECT_OPTIONS = [
    discord.SelectOption(label="Vanguard", value="vanguard"),
    discord.SelectOption(label="Duelist", value="duelist"),
    discord.SelectOption(label="Strategist", value="strategist"),
]

SECONDARY_ROLE_OPTIONS = [
    discord.SelectOption(label="Vanguard", value="vanguard"),
    discord.SelectOption(label="Duelist", value="duelist"),
    discord.SelectOption(label="Strategist", value="strategist"),
    discord.SelectOption(label="Fill / Flex", value="fill"),
    discord.SelectOption(label="Skip (no secondary)", value="skip"),
]


class RoleRequiredView(discord.ui.View):
    """View shown when a player tries to join a queue that requires role selection.

    Uses a single ephemeral message that gets edited through each step.
    """

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.game_name = game_name
        self.primary_role = None

        primary_select = discord.ui.Select(
            placeholder="Select your primary role...",
            options=[discord.SelectOption(label=o.label, value=o.value) for o in ROLE_SELECT_OPTIONS],
        )
        primary_select.callback = self.on_primary_select
        self.add_item(primary_select)

    async def on_primary_select(self, interaction: discord.Interaction):
        self.primary_role = interaction.data["values"][0]

        # Replace with secondary select
        self.clear_items()
        # Filter out the primary role from secondary options
        secondary_options = [
            discord.SelectOption(label=o.label, value=o.value)
            for o in SECONDARY_ROLE_OPTIONS
            if o.value != self.primary_role
        ]
        secondary_select = discord.ui.Select(
            placeholder="Select your secondary role (optional)...",
            options=secondary_options,
        )
        secondary_select.callback = self.on_secondary_select
        self.add_item(secondary_select)

        await interaction.response.edit_message(
            content=f"**Primary role:** {self.primary_role.title()}\n\n"
            "Now select your **secondary role** (or skip):",
            view=self
        )

    async def on_secondary_select(self, interaction: discord.Interaction):
        secondary_val = interaction.data["values"][0]
        if secondary_val in ("fill", "skip"):
            secondary_val = None

        await DatabaseHelper.set_player_role_prefs(
            interaction.user.id, self.game_id, self.primary_role, secondary_val
        )

        desc = f"**Primary:** {self.primary_role.title()}"
        if secondary_val:
            desc += f"\n**Secondary:** {secondary_val.title()}"
        else:
            desc += "\n**Secondary:** Fill"

        self.clear_items()
        await interaction.response.edit_message(
            content=f"**Roles set for {self.game_name}!**\n{desc}\n\n"
            "Please click **Join** again to enter the queue.",
            view=self
        )
        self.stop()


class PersistentRoleDropdownView(discord.ui.View):
    """Ephemeral dropdown flow for setting role preferences from the persistent button.

    Uses a single ephemeral message that gets edited through each step,
    matching the same UX as RoleRequiredView.
    """

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.game_name = game_name
        self.primary_role = None

        primary_select = discord.ui.Select(
            placeholder="Select your primary role...",
            options=[discord.SelectOption(label=o.label, value=o.value) for o in ROLE_SELECT_OPTIONS],
        )
        primary_select.callback = self.on_primary_select
        self.add_item(primary_select)

    async def on_primary_select(self, interaction: discord.Interaction):
        self.primary_role = interaction.data["values"][0]

        self.clear_items()
        secondary_options = [
            discord.SelectOption(label=o.label, value=o.value)
            for o in SECONDARY_ROLE_OPTIONS
            if o.value != self.primary_role
        ]
        secondary_select = discord.ui.Select(
            placeholder="Select your secondary role (optional)...",
            options=secondary_options,
        )
        secondary_select.callback = self.on_secondary_select
        self.add_item(secondary_select)

        await interaction.response.edit_message(
            content=f"**Primary role:** {self.primary_role.title()}\n\n"
            "Now select your **secondary role** (or skip):",
            view=self
        )

    async def on_secondary_select(self, interaction: discord.Interaction):
        secondary_val = interaction.data["values"][0]
        if secondary_val in ("fill", "skip"):
            secondary_val = None

        await DatabaseHelper.set_player_role_prefs(
            interaction.user.id, self.game_id, self.primary_role, secondary_val
        )

        desc = f"**Primary:** {self.primary_role.title()}"
        if secondary_val:
            desc += f"\n**Secondary:** {secondary_val.title()}"
        else:
            desc += "\n**Secondary:** Fill"

        self.clear_items()
        await interaction.response.edit_message(
            content=f"**Roles updated for {self.game_name}!**\n{desc}",
            view=self
        )
        self.stop()


class OWRoleSelectView(discord.ui.View):
    """Overwatch flexible role selection.

    One multi-select: the player picks every role they're willing to queue for
    (1-3). Stored canonically (Tank, DPS, Support) — the 2-2-2 balancer treats it
    as a willing-set and assigns each player their strongest viable role.
    """

    ALL_VALUE = "ALL"

    def __init__(self, cog: 'CustomMatch', game_id: int, game_name: str,
                 rejoin_hint: bool = True, after_save=None,
                 preselected: Optional[List[str]] = None):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id
        self.game_name = game_name
        self.rejoin_hint = rejoin_hint
        # Optional async hook(interaction, ordered_roles) that takes over the
        # response after the selection is saved (used to edit roles while queued
        # and to finish a queue join).
        self.after_save = after_save

        prior = set(preselected or ())
        options = [discord.SelectOption(label="All Roles", value=self.ALL_VALUE)]
        for r in OW_ROLE_DISPLAY_ORDER:
            options.append(discord.SelectOption(
                label=r,
                value=r,
                emoji=discord.PartialEmoji(name=r.lower(), id=OW_ROLE_EMOJI_ID[r]),
                default=r in prior,
            ))

        select = discord.ui.Select(
            placeholder="Select every role you'll play...",
            min_values=1,
            # 4, not 3: "All Roles" occupies a slot, so a player who ticks it
            # alongside all three roles must not hit a validation error.
            max_values=len(options),
            options=options,
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        chosen = set(interaction.data["values"])
        if self.ALL_VALUE in chosen:
            chosen = set(OW_ROLES)
        # Store in canonical order for stability (balancer ignores order)
        ordered = [r for r in OW_ROLES if r in chosen]
        if not ordered:
            await interaction.response.edit_message(
                content="No roles selected — pick at least one.", view=self
            )
            return
        await DatabaseHelper.set_ow_role_selection(interaction.user.id, self.game_id, ordered)

        self.clear_items()
        if self.after_save:
            await self.after_save(interaction, ordered)
        else:
            desc = "  ".join(f"{OW_ROLE_EMOJI[r]} {r}" for r in OW_ROLE_DISPLAY_ORDER if r in ordered)
            hint = "\n\nClick **Join** again to enter the queue." if self.rejoin_hint else ""
            await interaction.response.edit_message(
                content=f"**{self.game_name} roles set:**  {desc}{hint}",
                view=self,
            )
        self.stop()


class PersistentRoleView(BaseMatchView):
    """Persistent view with a button for users to set their role preferences."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Set Role", style=discord.ButtonStyle.primary, custom_id="persistent_role_set", emoji="\U0001f6e1\ufe0f")
    async def set_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        rivals_game = None
        for g in (games or []):
            if 'rivals' in g.name.lower():
                rivals_game = g
                break

        if not rivals_game:
            await interaction.response.send_message("No Rivals game configured.", ephemeral=True)
            return

        # Show existing prefs if any
        existing = await DatabaseHelper.get_player_role_prefs(interaction.user.id, rivals_game.game_id)
        current = ""
        if existing:
            current = f"\n\nCurrent: **{existing[0].title()}**"
            if existing[1]:
                current += f" / **{existing[1].title()}**"
            else:
                current += " / **Fill**"

        view = PersistentRoleDropdownView(self.cog, rivals_game.game_id, rivals_game.name)
        await interaction.response.send_message(
            f"Select your **primary role** for {rivals_game.name}:{current}",
            view=view,
            ephemeral=True
        )


class CaptainDraftView(BaseMatchView):
    """View for captain drafting."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id


# =============================================================================
# PERSISTENT LEADERBOARD VIEW (for dedicated leaderboard channel)
# =============================================================================

class ServerStatsToggleView(discord.ui.View):
    """Persistent toggle between Monthly and All-Time server stats, with a Matches dropdown."""

    def __init__(self, cog: 'CustomMatch', game_id: int, is_monthly: bool = True):
        super().__init__(timeout=None)
        self.cog = cog
        self.game_id = game_id
        self.is_monthly = is_monthly

        # Toggle button — label shows the OTHER state (what clicking will switch to)
        toggle_label = "All-Time" if is_monthly else "Monthly"
        toggle_btn = discord.ui.Button(
            label=toggle_label,
            style=discord.ButtonStyle.secondary,
            custom_id=f"cm_srvstats_toggle:{game_id}:{int(is_monthly)}",
        )
        toggle_btn.callback = self.toggle_callback
        self.add_item(toggle_btn)

        matches_btn = discord.ui.Button(
            label="Matches",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cm_srvstats_matches:{game_id}",
        )
        matches_btn.callback = self.matches_callback
        self.add_item(matches_btn)

    @staticmethod
    def _parse_toggle_id(custom_id: str) -> Tuple[int, bool]:
        # cm_srvstats_toggle:{game_id}:{is_monthly}
        parts = custom_id.split(":")
        return int(parts[1]), bool(int(parts[2]))

    async def toggle_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        # Read state from custom_id so this works across restarts
        try:
            game_id, current_monthly = self._parse_toggle_id(
                interaction.data.get("custom_id", "") if interaction.data else ""
            )
        except (ValueError, IndexError):
            game_id, current_monthly = self.game_id, self.is_monthly

        new_monthly = not current_monthly
        game = await DatabaseHelper.get_game(game_id)
        if not game:
            await interaction.followup.send("Game no longer configured.", ephemeral=True)
            return
        guild = interaction.guild
        if is_rivals_game(game):
            data = await self.cog._gather_rivals_serverstats_data(guild, game, monthly=new_monthly)
            image = await self.cog.stats_generator.generate_rivals_serverstats_image(data)
        else:
            data = await self.cog._gather_serverstats_data(guild, game, monthly=new_monthly)
            image = await self.cog.stats_generator.generate_serverstats_image(data)
        if image:
            image.seek(0)
            filename = 'serverstats.png'
            file = discord.File(image, filename=filename)
            embed = discord.Embed(color=COLOR_NEUTRAL)
            embed.set_image(url=f"attachment://{filename}")
            new_view = ServerStatsToggleView(self.cog, game_id, is_monthly=new_monthly)
            await interaction.edit_original_response(embed=embed, attachments=[file], view=new_view)
        else:
            await interaction.followup.send("Failed to generate stats image.", ephemeral=True)

    async def matches_callback(self, interaction: discord.Interaction):
        """Send an ephemeral dropdown of recent matches."""
        # Read game_id from the custom_id so it survives bot restarts
        try:
            cid = interaction.data.get("custom_id", "") if interaction.data else ""
            game_id = int(cid.split(":")[1])
        except (ValueError, IndexError):
            game_id = self.game_id

        await interaction.response.defer(ephemeral=True)
        # For Rivals, require that the match has at least one stats row —
        # otherwise abandoned/never-uploaded matches show up as empty
        # "Unknown" entries in the dropdown.
        game = await DatabaseHelper.get_game(game_id)
        require_rivals_stats = bool(game and is_rivals_game(game))
        recent = await DatabaseHelper.get_recent_completed_matches(
            game_id, limit=10, require_rivals_stats=require_rivals_stats
        )
        if not recent:
            await interaction.followup.send("No completed matches found.", ephemeral=True)
            return

        view = MatchHistorySelectView(self.cog, recent)
        await interaction.followup.send(
            "Pick a match to view its scoreboard:", view=view, ephemeral=True
        )


class LeaderboardPagerView(discord.ui.View):
    """Ephemeral paginated leaderboard (20/page) with a Monthly/All-time toggle.

    Opened from the grey Leaderboard button on a match results embed. All buttons grey.
    """

    def __init__(self, cog: 'CustomMatch', game_id: int, monthly: bool = True):
        super().__init__(timeout=180)
        self.cog = cog
        self.game_id = game_id
        self.monthly = monthly
        self.page = 0
        self.total_pages = 1
        self.sync_buttons()

    def sync_buttons(self):
        """Rebuild the buttons to reflect the current page / total / period state."""
        self.clear_items()

        prev_btn = discord.ui.Button(label="◀", style=discord.ButtonStyle.secondary,
                                     disabled=self.page <= 0)
        prev_btn.callback = self.prev_page
        self.add_item(prev_btn)

        indicator = discord.ui.Button(label=f"{self.page + 1}/{self.total_pages}",
                                      style=discord.ButtonStyle.secondary, disabled=True)
        self.add_item(indicator)

        next_btn = discord.ui.Button(label="▶", style=discord.ButtonStyle.secondary,
                                     disabled=self.page >= self.total_pages - 1)
        next_btn.callback = self.next_page
        self.add_item(next_btn)

        toggle_btn = discord.ui.Button(
            label="All-time" if self.monthly else "Monthly",
            style=discord.ButtonStyle.secondary
        )
        toggle_btn.callback = self.toggle_period
        self.add_item(toggle_btn)

    async def _refresh(self, interaction: discord.Interaction):
        embed, self.total_pages = await self.cog._build_leaderboard_page_embed(
            interaction.guild, self.game_id, monthly=self.monthly, page=self.page
        )
        # Clamp in case the underlying list shrank
        self.page = max(0, min(self.page, self.total_pages - 1))
        self.sync_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def prev_page(self, interaction: discord.Interaction):
        if self.page > 0:
            self.page -= 1
        await self._refresh(interaction)

    async def next_page(self, interaction: discord.Interaction):
        if self.page < self.total_pages - 1:
            self.page += 1
        await self._refresh(interaction)

    async def toggle_period(self, interaction: discord.Interaction):
        self.monthly = not self.monthly
        self.page = 0
        await self._refresh(interaction)


class MatchHistorySelectView(discord.ui.View):
    """Ephemeral view with a dropdown to browse recent matches."""

    def __init__(self, cog: 'CustomMatch', matches: List[dict]):
        super().__init__(timeout=180)
        self.cog = cog
        self.matches = matches

        options = []
        for m in matches:
            map_name = m.get("map_name") or "Unknown"
            decided = m.get("decided_at") or ""
            # Parse decided_at to a short date
            try:
                dt = datetime.fromisoformat(decided)
                date_str = dt.strftime("%m/%d/%y")
            except (ValueError, TypeError):
                date_str = "?"
            options.append(discord.SelectOption(
                label=f"{map_name} {date_str}",
                value=str(m["match_id"])
            ))

        select = discord.ui.Select(placeholder="Select a match...", options=options)
        select.callback = self.select_callback
        self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        match_id = int(interaction.data["values"][0])
        await interaction.response.defer(ephemeral=True)
        embed, file = await self.cog._generate_match_scoreboard(interaction.guild, match_id)
        kwargs = {"embed": embed, "ephemeral": True}
        if file:
            kwargs["file"] = file
        await interaction.followup.send(**kwargs)


class PlayerStatsView(discord.ui.View):
    """Multi-page player stats view with navigation and time period toggle."""

    PAGE_TITLES = ["Overview", "Map Performance", "Teammates", "Recent Matches"]

    def __init__(self, cog: 'CustomMatch', guild: discord.Guild, player_id: int,
                 game: 'GameConfig', stats: 'PlayerStats', monthly: bool = True):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild = guild
        self.player_id = player_id
        self.game = game
        self.stats = stats
        self.monthly = monthly
        self.current_page = 0
        self.valorant_stats = {}
        self.teammate_stats = {}
        self.recent_matches = []
        self.update_buttons()

    def update_buttons(self):
        """Update button states based on current page."""
        self.clear_items()

        # Previous button
        prev_btn = discord.ui.Button(label="\u25c0", style=discord.ButtonStyle.secondary, disabled=self.current_page == 0)
        prev_btn.callback = self.prev_page
        self.add_item(prev_btn)

        # Page indicator
        page_btn = discord.ui.Button(label=f"{self.current_page + 1}/{len(self.PAGE_TITLES)}", style=discord.ButtonStyle.secondary, disabled=True)
        self.add_item(page_btn)

        # Next button
        next_btn = discord.ui.Button(label="\u25b6", style=discord.ButtonStyle.secondary, disabled=self.current_page == len(self.PAGE_TITLES) - 1)
        next_btn.callback = self.next_page
        self.add_item(next_btn)

        # Time period toggle
        period_label = "All Time" if self.monthly else "This Month"
        period_btn = discord.ui.Button(label=f"View: {period_label}", style=discord.ButtonStyle.primary)
        period_btn.callback = self.toggle_period
        self.add_item(period_btn)

    async def load_data(self):
        """Load Valorant stats and teammate data."""
        self.valorant_stats = await DatabaseHelper.get_valorant_player_stats(
            self.player_id, self.game.game_id, self.monthly
        )
        self.teammate_stats = await DatabaseHelper.get_player_teammate_stats(
            self.player_id, self.game.game_id, self.monthly
        )
        self.recent_matches = await DatabaseHelper.get_player_recent_matches(
            self.player_id, self.game.game_id, limit=5
        )

    async def build_embed(self) -> discord.Embed:
        """Build the embed for the current page."""
        member = self.guild.get_member(self.player_id)
        member_name = member.display_name if member else f"User {self.player_id}"
        period_text = "This Month" if self.monthly else "All Time"

        embed = discord.Embed(
            title=f"{member_name} - {self.game.name} Stats",
            color=COLOR_WHITE
        )
        embed.set_footer(text=f"{self.PAGE_TITLES[self.current_page]} \u2022 {period_text}")

        if self.current_page == 0:
            # Overview page
            winrate = (self.stats.wins / self.stats.games_played * 100) if self.stats.games_played > 0 else 0
            embed.add_field(name="Games", value=str(self.stats.games_played), inline=True)
            embed.add_field(name="Win Rate", value=f"{winrate:.1f}%", inline=True)
            embed.add_field(name="Wins", value=str(self.stats.wins), inline=True)
            embed.add_field(name="Losses", value=str(self.stats.losses), inline=True)

            # Add Valorant stats if available
            if self.valorant_stats.get('total_games', 0) > 0:
                total_k = self.valorant_stats.get('total_kills', 0)
                total_d = self.valorant_stats.get('total_deaths', 0)
                total_a = self.valorant_stats.get('total_assists', 0)
                hs_pct = self.valorant_stats.get('hs_percent', 0)
                kd = total_k / total_d if total_d > 0 else total_k

                embed.add_field(name="\u200b", value="**Valorant Stats**", inline=False)
                embed.add_field(name="K/D/A", value=f"{total_k}/{total_d}/{total_a}", inline=True)
                embed.add_field(name="K/D", value=f"{kd:.2f}", inline=True)
                embed.add_field(name="HS%", value=f"{hs_pct:.1f}%", inline=True)

        elif self.current_page == 1:
            # Map performance page
            if self.valorant_stats.get('best_map'):
                best = self.valorant_stats['best_map']
                embed.add_field(
                    name="\U0001f3c6 Best Map",
                    value=f"**{best['name']}**\n{best['winrate']:.1f}% WR ({best['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="\U0001f3c6 Best Map", value="Not enough data\n(min 3 games)", inline=True)

            if self.valorant_stats.get('worst_map'):
                worst = self.valorant_stats['worst_map']
                embed.add_field(
                    name="\U0001f480 Worst Map",
                    value=f"**{worst['name']}**\n{worst['winrate']:.1f}% WR ({worst['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="\U0001f480 Worst Map", value="Not enough data\n(min 3 games)", inline=True)

        elif self.current_page == 2:
            # Teammate page
            if self.teammate_stats.get('best_teammate'):
                best = self.teammate_stats['best_teammate']
                best_member = self.guild.get_member(best['player_id'])
                best_name = best_member.display_name if best_member else f"User {best['player_id']}"
                embed.add_field(
                    name="\U0001f91d Best Teammate",
                    value=f"**{best_name}**\n{best['winrate']:.1f}% WR ({best['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="\U0001f91d Best Teammate", value="Not enough data\n(min 3 games together)", inline=True)

            if self.teammate_stats.get('cursed_teammate'):
                cursed = self.teammate_stats['cursed_teammate']
                cursed_member = self.guild.get_member(cursed['player_id'])
                cursed_name = cursed_member.display_name if cursed_member else f"User {cursed['player_id']}"
                embed.add_field(
                    name="\U0001f480 Cursed Teammate",
                    value=f"**{cursed_name}**\n{cursed['winrate']:.1f}% WR ({cursed['games']} games)",
                    inline=True
                )
            else:
                embed.add_field(name="\U0001f480 Cursed Teammate", value="Not enough data\n(min 3 games together)", inline=True)

        elif self.current_page == 3:
            # Recent matches page
            if self.recent_matches:
                lines = []
                for match in self.recent_matches:
                    won = match['team'] == match['winning_team']
                    result = "\u2705" if won else "\u274c"
                    kda = ""
                    if match.get('kills') is not None:
                        kda = f" - {match['kills']}/{match['deaths']}/{match['assists']}"
                        if match.get('agent'):
                            kda += f" ({match['agent']})"
                    # Use match_map_name (from map vote) if available, else valorant stats map_name
                    map_display = match.get('match_map_name') or match.get('map_name')
                    map_name = f" on {map_display}" if map_display else ""
                    short_id = match.get('short_id') or str(match['match_id'])
                    lines.append(f"{result} Match {short_id}{map_name}{kda}")
                embed.add_field(name="Recent Matches", value="\n".join(lines), inline=False)
            else:
                embed.add_field(name="Recent Matches", value="No completed matches yet", inline=False)

        return embed

    async def prev_page(self, interaction: discord.Interaction):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = await self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

    async def next_page(self, interaction: discord.Interaction):
        if self.current_page < len(self.PAGE_TITLES) - 1:
            self.current_page += 1
            self.update_buttons()
            embed = await self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

    async def toggle_period(self, interaction: discord.Interaction):
        self.monthly = not self.monthly
        await self.load_data()
        self.update_buttons()
        embed = await self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)


class StatsSelectDropdown(discord.ui.Select):
    """Dropdown for selecting stats view: Monthly, Lifetime, or specific matches."""

    def __init__(self, recent_matches: List[dict]):
        # Use current month name for the monthly option
        current_month = datetime.now(timezone.utc).strftime('%B')
        options = [
            discord.SelectOption(label="All-Time", value="lifetime", description="All-time stats", default=True),
            discord.SelectOption(label=current_month, value="seasonal", description="This month's stats"),
        ]
        # Add recent matches as options
        for i, match in enumerate(recent_matches[:5]):
            map_name = match.get('map_name') or match.get('match_map_name') or "Unknown"
            kills = match.get('kills', 0) or 0
            deaths = match.get('deaths', 0) or 0
            assists = match.get('assists', 0) or 0
            kda = f"{kills}/{deaths}/{assists}"
            label = f"{map_name} {kda}"
            if len(label) > 25:
                label = label[:22] + "..."
            match_id = match.get('match_id', i)
            options.append(discord.SelectOption(
                label=label,
                value=f"match_{match_id}",
                description=f"Match #{match.get('short_id') or match_id}"
            ))

        super().__init__(placeholder="Select stats view...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await self.view.handle_selection(interaction, self.values[0])


class StatsImageView(discord.ui.View):
    """View for stats card image with dropdown selection."""

    def __init__(self, cog: 'CustomMatch', member: discord.Member, game: 'GameConfig',
                 images: Dict[str, io.BytesIO], recent_matches: List[dict], invoker_id: int,
                 guild: discord.Guild):
        super().__init__(timeout=None)  # Persistent
        self.cog = cog
        self.member = member
        self.game = game
        self.images = images  # {'lifetime': BytesIO, 'seasonal': BytesIO, 'match_X': BytesIO}
        self.recent_matches = recent_matches
        self.invoker_id = invoker_id
        self.guild = guild
        self.current = 'lifetime'
        self.add_item(StatsSelectDropdown(recent_matches))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Only the person who ran this command can use this dropdown.",
                ephemeral=True
            )
            return False
        return True

    async def handle_selection(self, interaction: discord.Interaction, value: str):
        self.current = value

        # Update dropdown default
        for item in self.children:
            if isinstance(item, StatsSelectDropdown):
                for option in item.options:
                    option.default = (option.value == value)

        if value in self.images:
            # Use cached image
            self.images[value].seek(0)
            filename = f'stats_{value}.png'
            file = discord.File(self.images[value], filename=filename)
            embed = discord.Embed(
                title=f"{self.member.display_name} - {self.game.name} Stats",
                color=COLOR_WHITE
            )
            embed.set_image(url=f"attachment://{filename}")
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
        elif value.startswith('match_'):
            # Generate match-specific scoreboard image on demand
            await interaction.response.defer()

            match_id = int(value.replace('match_', ''))
            match_data = None
            for m in self.recent_matches:
                if m.get('match_id') == match_id:
                    match_data = m
                    break

            if not match_data:
                await interaction.followup.send("Match not found.", ephemeral=True)
                return

            # Get full match stats from database
            full_stats = await DatabaseHelper.get_player_match_stats(self.member.id, match_id)

            # Build match scoreboard data
            map_name = match_data.get('map_name') or match_data.get('match_map_name') or "Unknown"
            won = match_data.get('team') == match_data.get('winning_team')

            # Get round scores for accurate ADR/ACS calculation
            match_record = await DatabaseHelper.get_match(match_id)
            total_rounds = 24  # fallback
            if match_record:
                val_red = match_record.get('val_red_rounds') or 0
                val_blue = match_record.get('val_blue_rounds') or 0
                if val_red + val_blue > 0:
                    total_rounds = val_red + val_blue

            scoreboard_data = {
                'player_name': safe_display_name(self.member),
                'avatar_url': self.member.display_avatar.url,
                'map_name': map_name,
                'agent': full_stats.get('agent') or match_data.get('agent') or 'Unknown',
                'won': won,
                'kills': full_stats.get('kills') or match_data.get('kills', 0) or 0,
                'deaths': full_stats.get('deaths') or match_data.get('deaths', 0) or 0,
                'assists': full_stats.get('assists') or match_data.get('assists', 0) or 0,
                'score': full_stats.get('score', 0) or 0,
                'damage': full_stats.get('damage_dealt', 0) or 0,
                'first_bloods': full_stats.get('first_bloods', 0) or 0,
                'headshots': full_stats.get('headshots', 0) or 0,
                'bodyshots': full_stats.get('bodyshots', 0) or 0,
                'legshots': full_stats.get('legshots', 0) or 0,
                'plants': full_stats.get('plants', 0) or 0,
                'defuses': full_stats.get('defuses', 0) or 0,
                'c2k': full_stats.get('c2k', 0) or 0,
                'c3k': full_stats.get('c3k', 0) or 0,
                'c4k': full_stats.get('c4k', 0) or 0,
                'c5k': full_stats.get('c5k', 0) or 0,
                'econ_spent': full_stats.get('econ_spent', 0) or 0,
                'econ_loadout': full_stats.get('econ_loadout', 0) or 0,
                'total_rounds': total_rounds,
            }

            image = await self.cog.stats_generator.generate_match_image(scoreboard_data)
            if image:
                self.images[value] = image
                image.seek(0)
                filename = f'match_{match_id}.png'
                file = discord.File(image, filename=filename)
                embed = discord.Embed(
                    title=f"{self.member.display_name} - Match Scoreboard",
                    color=COLOR_WHITE
                )
                embed.set_image(url=f"attachment://{filename}")
                await interaction.edit_original_response(embed=embed, attachments=[file], view=self)
            else:
                await interaction.followup.send("Failed to generate match scoreboard.", ephemeral=True)


class SimpleStatsSelectDropdown(discord.ui.Select):
    """Dropdown for selecting stats view on non-Valorant games: All-Time, Monthly, or specific matches."""

    def __init__(self, recent_matches: List[dict]):
        current_month = datetime.now(timezone.utc).strftime('%B')
        options = [
            discord.SelectOption(label="All-Time", value="lifetime", description="All-time stats", default=True),
            discord.SelectOption(label=current_month, value="monthly", description="This month's stats"),
        ]
        for i, match in enumerate(recent_matches[:5]):
            map_name = match.get('map_name') or match.get('match_map_name') or "Unknown"
            won = match.get('team') == match.get('winning_team')
            result = "W" if won else "L"
            label = f"{map_name} ({result})"
            if len(label) > 25:
                label = label[:22] + "..."
            match_id = match.get('match_id', i)
            options.append(discord.SelectOption(
                label=label,
                value=f"match_{match_id}",
                description=f"Match #{match.get('short_id') or match_id}"
            ))
        super().__init__(placeholder="Select stats view...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await self.view.handle_selection(interaction, self.values[0])


class SimpleStatsImageView(discord.ui.View):
    """View for simple (non-Valorant) stats card with dropdown selection."""

    def __init__(self, cog: 'CustomMatch', member: discord.Member, game: 'GameConfig',
                 images: Dict[str, io.BytesIO], invoker_id: int, guild: discord.Guild,
                 rivals_extras: Optional[Dict[str, dict]] = None,
                 recent_matches: Optional[List[dict]] = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.member = member
        self.game = game
        self.images = images  # {'monthly': BytesIO, 'lifetime': BytesIO, 'match_X': BytesIO}
        self.invoker_id = invoker_id
        self.guild = guild
        self.rivals_extras = rivals_extras or {}
        self.recent_matches = recent_matches or []
        self.current = 'lifetime'
        self.add_item(SimpleStatsSelectDropdown(self.recent_matches))

    def _build_embed(self, variant: str) -> discord.Embed:
        filename = f"stats_{variant}.png"
        embed = discord.Embed(
            title=f"{self.member.display_name} - {self.game.name} Stats",
            color=COLOR_WHITE
        )
        embed.set_image(url=f"attachment://{filename}")
        extras = self.rivals_extras.get(variant)
        if extras:
            field_name = (
                f"Marvel Rivals \u2014 {datetime.now(timezone.utc).strftime('%B')}"
                if variant == 'monthly'
                else "Marvel Rivals \u2014 All-Time"
            )
            embed.add_field(
                name=field_name,
                value=self.cog._build_rivals_player_embed_field(extras),
                inline=False,
            )
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "Only the person who ran this command can use this.", ephemeral=True
            )
            return False
        return True

    async def handle_selection(self, interaction: discord.Interaction, value: str):
        self.current = value

        # Update dropdown default
        for item in self.children:
            if isinstance(item, SimpleStatsSelectDropdown):
                for option in item.options:
                    option.default = (option.value == value)

        if value in self.images:
            self.images[value].seek(0)
            if value.startswith('match_'):
                filename = 'scoreboard.png'
                file = discord.File(self.images[value], filename=filename)
                embed = discord.Embed(
                    title=f"{self.member.display_name} - Match Scoreboard",
                    color=COLOR_WHITE
                )
                embed.set_image(url=f"attachment://{filename}")
            else:
                filename = f'stats_{value}.png'
                file = discord.File(self.images[value], filename=filename)
                embed = self._build_embed(value)
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
        elif value.startswith('match_'):
            await interaction.response.defer()
            match_id = int(value.replace('match_', ''))
            embed, scoreboard_file = await self.cog._generate_match_scoreboard(self.guild, match_id)
            if scoreboard_file:
                # Cache the underlying BytesIO for re-selection
                buf = scoreboard_file.fp
                self.images[value] = buf
                await interaction.edit_original_response(embed=embed, attachments=[scoreboard_file], view=self)
            else:
                await interaction.edit_original_response(embed=embed, attachments=[], view=self)


# -------------------------------------------------------------------------
# TEAM SHUFFLE VOTE VIEWS
# -------------------------------------------------------------------------


class ShuffleMatchSelectView(discord.ui.View):
    """Dropdown to select which active match to initiate a shuffle vote for."""

    def __init__(self, cog: 'CustomMatch', matches: List[dict]):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        for m in matches[:25]:  # Discord max 25 options
            short_id = m.get("short_id") or str(m["match_id"])
            game = m.get("_game_name", "Match")
            options.append(discord.SelectOption(
                label=f"{game} — {short_id}",
                value=str(m["match_id"]),
                description=f"Match {short_id}",
            ))

        select = discord.ui.Select(
            placeholder="Select a match...",
            options=options,
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        match_id = int(interaction.data["values"][0])
        view = ShuffleStartedCheckView(self.cog, match_id)
        await interaction.response.edit_message(
            content="Has the match started yet?",
            view=view,
        )


class ShuffleStartedCheckView(discord.ui.View):
    """Yes/No buttons asking if the match has started."""

    def __init__(self, cog: 'CustomMatch', match_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.match_id = match_id

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.danger)
    async def yes_started(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="Teams can't be shuffled after the match has started.",
            view=None,
        )

    @discord.ui.button(label="No", style=discord.ButtonStyle.success)
    async def no_not_started(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._initiate_shuffle_vote(interaction, self.match_id)


class ShuffleVoteView(BaseMatchView):
    """Persistent vote button in the lobby channel for team shuffle."""

    def __init__(self, cog: 'CustomMatch', match_id: int, needed_votes: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id
        self.needed_votes = needed_votes

        btn = discord.ui.Button(
            label="Vote to Shuffle Teams",
            style=discord.ButtonStyle.primary,
            custom_id=f"cm_shuffle_vote:{match_id}",
        )
        btn.callback = self.vote_callback
        self.add_item(btn)

    async def vote_callback(self, interaction: discord.Interaction):
        await self.cog.handle_shuffle_vote(interaction, self.match_id, self.needed_votes)


# =============================================================================
# SECONDARY MODE VOTE VIEW
# =============================================================================

class SecondaryModeVoteView(discord.ui.View):
    """Hidden voting view for secondary queue mode selection."""

    def __init__(self, modes: list, match_id: int, allowed_voters: list,
                 round_num: int, result_future: asyncio.Future):
        super().__init__(timeout=120)
        self.modes = modes
        self.match_id = match_id
        self.allowed_voters = set(allowed_voters)
        self.round_num = round_num
        self.result_future = result_future

        self.votes: Dict[str, list] = {m["mode_name"]: [] for m in modes}
        self.voters: set = set()
        self.first_vote_time: Optional[datetime] = None
        self.concluded = False

        for i, mode in enumerate(modes):
            btn = discord.ui.Button(
                label=mode["mode_name"],
                style=discord.ButtonStyle.primary,
                custom_id=f"cm_mode_vote:{match_id}:{round_num}:{i}",
                row=i // 5,
            )
            btn.callback = self._make_callback(mode["mode_name"])
            self.add_item(btn)

    def _make_callback(self, mode_name: str):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id not in self.allowed_voters:
                await interaction.response.send_message(
                    "You are not part of this match.", ephemeral=True
                )
                return

            if interaction.user.id in self.voters:
                await interaction.response.send_message(
                    "You have already voted this round.", ephemeral=True
                )
                return

            if self.concluded:
                await interaction.response.send_message(
                    "Voting has ended.", ephemeral=True
                )
                return

            # Record vote
            self.voters.add(interaction.user.id)
            self.votes[mode_name].append(interaction.user.id)

            if self.first_vote_time is None:
                self.first_vote_time = datetime.now(timezone.utc)

            await interaction.response.send_message(
                f"Vote recorded for **{mode_name}**!", ephemeral=True
            )

            # Check if all allowed voters have voted
            if self.voters >= self.allowed_voters:
                if not self.concluded:
                    self.concluded = True
                    if not self.result_future.done():
                        self.result_future.set_result("all_voted")

        return callback

