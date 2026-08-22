import discord
from discord import ui
import asyncio
import logging
import json
import re
import io
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple, TYPE_CHECKING

from .models import (
    GameConfig, QueueType, CaptainSelection, QueueState,
    COLOR_WHITE, COLOR_SUCCESS, COLOR_WARNING,
    RIVALS_ROLES, parse_duration_to_minutes, safe_display_name,
    is_valorant_game, is_rivals_game, is_overwatch_game, COLOR_NEUTRAL, resolve_ocr_ign,
    OW_ROLES, OW_ROLE_EMOJI, OW_DEFAULT_ROLE_WEIGHTS, normalize_ow_role,
    pc_seed_mmr,
)
from .database import DatabaseHelper

if TYPE_CHECKING:
    from .cog import CustomMatch
    from .api_clients import RivalsScoreboardResult, RivalsVisionClient

logger = logging.getLogger('cogs.custommatch')


async def seed_mmr_for_rank(rank_mmr: int, game: 'GameConfig', platform: str) -> int:
    """MMR to seed a player at, given their rank-derived MMR and platform.

    For a PC player in a PC-enabled (crossplay) game, applies the one-time PC seed
    bump via :func:`pc_seed_mmr` using the game's configured tier offset. Console
    players, and any game without PC support, get their raw rank MMR unchanged.
    """
    if game and getattr(game, 'pc_enabled', False) and platform == 'pc':
        thresholds = list((await DatabaseHelper.get_mmr_roles(game.game_id)).values())
        return pc_seed_mmr(rank_mmr, thresholds, game.pc_offset_tiers)
    return rank_mmr


# =============================================================================
# VIEWS & MODALS
# =============================================================================

class GameSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a game."""

    def __init__(self, games: List[GameConfig], callback_func):
        options = [
            discord.SelectOption(label=g.name, value=str(g.game_id))
            for g in games
        ]
        if not options:
            options = [discord.SelectOption(label="No games configured", value="none")]
        super().__init__(placeholder="Select a game...", options=options)
        self.callback_func = callback_func

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            await interaction.response.send_message("No games configured yet.", ephemeral=True)
            return
        try:
            await self.callback_func(interaction, int(self.values[0]))
        except Exception as e:
            logger.error(f"Error in GameSelectDropdown callback: {e}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
                else:
                    await interaction.response.send_message(f"An error occurred: {e}", ephemeral=True)
            except Exception:
                pass


class BaseMatchView(discord.ui.View):
    """Base view with unified error handling for match-related views."""

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item):
        # item.callback is discord.py's _ViewCallback wrapper — it has no __name__,
        # so identify the item by custom_id (falling back to label / type).
        name = getattr(item, "custom_id", None) or getattr(item, "label", None) or type(item).__name__
        if isinstance(error, discord.NotFound) and error.code == 10062:
            # Discord's 3s ack window closed before we responded; the token is dead,
            # so there's nothing to reply to and nothing actionable to log.
            logger.warning(f"{self.__class__.__name__}.{name}: interaction expired before response")
            return
        logger.error(f"Error in {self.__class__.__name__}.{name}: {error}", exc_info=True)
        try:
            msg = "An error occurred. Please try again."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass


class ExpiringView(discord.ui.View):
    """A view that dies visibly instead of silently going dead.

    discord.py stops dispatching to a view once its timeout elapses, but the
    components on screen still look live. Every click then lands on nothing,
    Discord's 3-second ack window closes, and the user sees "<bot> didn't
    respond in time" — which reads as an outage rather than an expired panel.

    Subclasses get two things:
      * components greyed out and a note appended when the timeout fires, so
        an expired panel is obvious at a glance;
      * the timeout clock restarted on every interaction, so a panel stays
        alive as long as it is being used and only expires once genuinely idle.

    Call ``await view.track(interaction)`` right after sending. Ephemeral
    messages are editable for 15 minutes via the interaction token, so keep
    timeouts comfortably under that.
    """

    expiry_note = "\n\n*This panel expired — re-open it to continue.*"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message: Optional[discord.Message] = None

    async def track(self, interaction: discord.Interaction):
        """Remember the message this view was sent on so on_timeout can edit it."""
        try:
            self.message = await interaction.original_response()
        except discord.HTTPException:
            self.message = None  # nothing to grey out later; timeout still fires

    def attach_back(self, parent: 'SettingsPage', row: int = 4):
        """Wire this view into the /cm_settings tree with a Back button.

        Lets a standalone editor view (toggles, schedules, weights) be rendered
        onto the settings message itself instead of spawning a loose ephemeral
        that dead-ends.
        """
        self._back_button = BackButton(parent, row=row)
        self.add_item(self._back_button)
        return self

    def _restore_back(self):
        """Re-add the Back button after a rebuild that cleared the view."""
        button = getattr(self, "_back_button", None)
        if button is not None and button not in self.children:
            self.add_item(button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        self._refresh_timeout()
        return True

    async def on_timeout(self):
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True
        if self.message is None:
            return
        try:
            content = (self.message.content or "") + self.expiry_note
            await self.message.edit(content=content, view=self)
        except discord.HTTPException:
            pass  # message already gone, or the token expired first


class ConfirmView(ExpiringView):
    """Simple confirmation view."""

    def __init__(self, timeout: float = 60.0):
        super().__init__(timeout=timeout)
        self.value = None

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        await interaction.response.defer()


class RivalsAdminChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, parent: "RivalsSettingsView"):
        super().__init__(
            channel_types=[discord.ChannelType.text],
            placeholder="Select the Rivals admin/review channel...",
            min_values=1,
            max_values=1,
            row=0,
        )
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction):
        channel = self.values[0]
        await DatabaseHelper.set_config("rivals_admin_channel_id", str(channel.id))
        embed = await self.parent_view.build_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)


class RivalsBlacklistUserSelect(discord.ui.UserSelect):
    def __init__(self, parent: "RivalsSettingsView", action: str):
        # action: 'add' or 'remove'
        super().__init__(
            placeholder=f"{action.title()} user {'to' if action == 'add' else 'from'} upload blacklist...",
            min_values=1,
            max_values=1,
            row=1 if action == 'add' else 2,
        )
        self.parent_view = parent
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        user = self.values[0]
        if self.action == "add":
            await DatabaseHelper.add_rivals_blacklist(
                interaction.guild.id, user.id, interaction.user.id, reason=None
            )
            msg = f"Added {user.mention} to the Rivals upload blacklist."
        else:
            removed = await DatabaseHelper.remove_rivals_blacklist(interaction.guild.id, user.id)
            msg = (f"Removed {user.mention} from the Rivals upload blacklist."
                   if removed else f"{user.mention} was not on the blacklist.")
        embed = await self.parent_view.build_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)
        try:
            await interaction.followup.send(msg, ephemeral=True)
        except Exception:
            pass


class RivalsSettingsView(ExpiringView):
    """Sub-view opened from SettingsView for Marvel Rivals stats config."""

    def __init__(self, cog):
        super().__init__(timeout=300)
        self.cog = cog
        self.add_item(RivalsAdminChannelSelect(self))
        self.add_item(RivalsBlacklistUserSelect(self, action="add"))
        self.add_item(RivalsBlacklistUserSelect(self, action="remove"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        self._refresh_timeout()  # keep the panel alive while in use
        if await self.cog.is_cm_admin(interaction.user):
            return True
        await interaction.response.send_message(
            "You no longer have permission to use this panel.", ephemeral=True
        )
        return False

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        admin_channel_id = await DatabaseHelper.get_config("rivals_admin_channel_id")
        admin_channel_str = "*not set*"
        if admin_channel_id:
            ch = guild.get_channel(int(admin_channel_id))
            admin_channel_str = ch.mention if ch else f"*missing ({admin_channel_id})*"

        blacklist = await DatabaseHelper.list_rivals_blacklist(guild.id)
        if blacklist:
            bl_lines = []
            for row in blacklist[:10]:
                member = guild.get_member(row["player_id"])
                name = member.mention if member else f"<@{row['player_id']}>"
                bl_lines.append(f"• {name}")
            if len(blacklist) > 10:
                bl_lines.append(f"*...and {len(blacklist) - 10} more*")
            bl_text = "\n".join(bl_lines)
        else:
            bl_text = "*empty*"

        gemini_status = "✅ ready" if self.cog.rivals_vision.available else "❌ missing GEMINI_API_KEY"

        embed = discord.Embed(
            title="Marvel Rivals Stats — Settings",
            color=discord.Color.blurple(),
            description=(
                "Configure where Rivals scoreboard reviews land, manage the upload "
                "blacklist, and correct stats for a specific match.\n\n"
                f"**Gemini OCR:** {gemini_status}\n"
                f"**Review / Admin channel:** {admin_channel_str}"
            ),
        )
        embed.add_field(name="Upload blacklist", value=bl_text, inline=False)
        embed.set_footer(
            text="Use 'Correct Match Stats' to re-upload a screenshot for a specific match."
        )
        return embed

    @discord.ui.button(label="Correct Match Stats", style=discord.ButtonStyle.danger, row=3)
    async def correct_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = RivalsCorrectStatsModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=3)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.build_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed, view=self)


class RivalsCorrectStatsModal(discord.ui.Modal, title="Correct Rivals Match Stats"):
    """Admin opens this to start a correction flow for a specific match."""

    match_id_input = discord.ui.TextInput(
        label="Match ID (number or short_id)",
        placeholder="e.g. 1234 or A7XK",
        required=True,
        max_length=20,
    )

    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.match_id_input.value.strip()
        match = None
        if raw.isdigit():
            match = await DatabaseHelper.get_match(int(raw))
        if match is None:
            async with DatabaseHelper._get_db() as db:
                async with db.execute(
                    "SELECT * FROM matches WHERE UPPER(short_id) = UPPER(?) LIMIT 1", (raw,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        match = dict(row)

        if not match:
            await interaction.response.send_message(
                f"No match found for `{raw}`.", ephemeral=True
            )
            return

        game = await DatabaseHelper.get_game(match["game_id"])
        if not game or not is_rivals_game(game):
            await interaction.response.send_message(
                f"Match {raw} is not a Rivals match.", ephemeral=True
            )
            return

        target_channel = interaction.channel
        if target_channel is None:
            await interaction.response.send_message(
                "Run this from a server text channel or thread.", ephemeral=True
            )
            return

        # Collision: there is already a pending Rivals upload keyed on this
        # channel (e.g. a post-match upload window is still open). Refuse so
        # we don't conflate a post-match screenshot with a correction.
        existing = self.cog.rivals_pending_uploads.get(target_channel.id)
        if existing:
            if existing["expires_at"] < datetime.now(timezone.utc):
                # Expired — clean it up and allow the new correction
                self.cog.rivals_pending_uploads.pop(target_channel.id, None)
            else:
                await interaction.response.send_message(
                    "There's already a pending Rivals upload in this channel — "
                    "finish or wait for it to expire.",
                    ephemeral=True,
                )
                return

        # Send the single ephemeral message that will get edited throughout
        # the whole correction flow (progress → discrepancy resolver → final
        # success). We store a handle to it on the pending-upload entry so
        # the on_message handler and all downstream views can edit it in
        # place instead of posting additional messages.
        await interaction.response.send_message(
            f"Ready to correct stats for match **{match.get('short_id') or match['match_id']}**. "
            f"Upload the corrected scoreboard in this channel within 15 minutes.",
            ephemeral=True,
        )
        try:
            ephemeral_msg = await interaction.original_response()
        except Exception as e:
            logger.error(f"Failed to fetch ephemeral for correction flow: {e}")
            ephemeral_msg = None

        # Register a pending-upload entry keyed on the invoking channel/thread.
        # The next image attachment from this admin in this channel will be
        # processed as a correction for the chosen match.
        self.cog.rivals_pending_uploads[target_channel.id] = {
            "match_id": match["match_id"],
            "game_id": game.game_id,
            "guild_id": interaction.guild.id,
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=15),
            "is_correction": True,
            "initiator_id": interaction.user.id,
            "ephemeral_msg": ephemeral_msg,
        }


class _ConfirmLinkResolverView(ExpiringView):
    """Yes/No confirmation for linking an IGN to a user who isn't in the match roster."""

    def __init__(self, parent: "RivalsIGNResolverView", interaction_user_id: int, selected_user_id: int):
        super().__init__(timeout=120)
        self.parent = parent
        self.interaction_user_id = interaction_user_id
        self.selected_user_id = selected_user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        self._refresh_timeout()  # keep the panel alive while in use
        return interaction.user.id == self.interaction_user_id

    @discord.ui.button(label="Link anyway", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.edit_message(content="Linked.", view=None)
        except Exception:
            pass
        await self.parent._apply_link(interaction, self.selected_user_id)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)
        self.stop()


class RivalsIGNResolverView(ExpiringView):
    """Minimal resolver for unmapped IGNs after Rivals scoreboard OCR.

    Uses process-of-elimination to suggest which roster member owns each
    unknown IGN.  For a single mismatch the admin gets a one-click button;
    for multiple mismatches a sequential dropdown flow walks them through
    each IGN.  After all IGNs are linked the stats auto-commit — no extra
    screenshot or "Confirm" step required.
    """

    def __init__(
        self,
        cog: 'CustomMatch',
        guild: discord.Guild,
        match_id: int,
        game_id: int,
        upload_id: int,
        result: "RivalsScoreboardResult",
        rows_out: List[dict],
        unmapped: List[str],
        match_players: List[dict],
        winning_team: Optional[str],
        timeout: float = 7200,
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild = guild
        self.match_id = match_id
        self.game_id = game_id
        self.upload_id = upload_id
        self.result = result
        self.rows_out = list(rows_out)
        self.remaining: List[str] = list(unmapped)
        self._original_unmapped: List[str] = list(unmapped)
        self.match_players = match_players
        self.winning_team = winning_team
        self._player_by_ign = {p.ign: p for p in result.players if p.ign}
        self.current_index = 0

        # Process-of-elimination: figure out which roster members are unmatched
        mapped_pids = {r["player_id"] for r in rows_out}
        roster_pids = {mp["player_id"] for mp in match_players}
        self.unmatched_roster: List[int] = list(roster_pids - mapped_pids)

        # Build suggestions: ign -> suggested player_id
        self.suggestions: Dict[str, int] = {}
        if len(unmapped) == 1 and len(self.unmatched_roster) == 1:
            self.suggestions[unmapped[0]] = self.unmatched_roster[0]
        elif len(unmapped) == len(self.unmatched_roster) and len(unmapped) > 1:
            self._build_team_aligned_suggestions()

        # Track resolved IGNs for embed display
        self.resolved: Dict[str, int] = {}  # ign -> player_id

        self._rebuild_items()

    def _build_team_aligned_suggestions(self):
        """Align unmapped IGNs to unmatched roster members by team."""
        mp_by_id = {mp["player_id"]: mp for mp in self.match_players}
        used: set = set()
        for ign in self.remaining:
            p = self._player_by_ign.get(ign)
            if not p:
                continue
            ign_team = (p.team or "").lower()
            same_team = [
                pid for pid in self.unmatched_roster
                if (mp_by_id.get(pid, {}).get("team") or "").lower() == ign_team
                and pid not in used
            ]
            if len(same_team) == 1:
                self.suggestions[ign] = same_team[0]
                used.add(same_team[0])

    def _rebuild_items(self):
        self.clear_items()
        if not self.remaining:
            return

        current_ign = self.remaining[self.current_index]
        suggested_pid = self.suggestions.get(current_ign)
        is_multi = len(self.remaining) > 1

        # Row 0: IGN picker (multi-mismatch only)
        if is_multi:
            options = []
            for ign in self.remaining[:25]:
                p = self._player_by_ign.get(ign)
                desc = ""
                if p:
                    desc = f"{p.role or '?'} | {p.kills}/{p.deaths}/{p.assists}"
                options.append(discord.SelectOption(
                    label=ign[:100] or "?",
                    description=desc[:100] if desc else None,
                    value=ign,
                    default=(ign == current_ign),
                ))
            ign_picker = discord.ui.Select(
                placeholder="Select IGN to resolve",
                options=options,
                row=0,
            )
            ign_picker.callback = self._on_pick_ign
            self.add_item(ign_picker)

        # Suggestion button
        suggest_row = 1 if is_multi else 0
        if suggested_pid is not None:
            member = self.guild.get_member(suggested_pid)
            label = member.display_name if member else str(suggested_pid)
            btn = discord.ui.Button(
                label=f"It's {label}"[:80],
                style=discord.ButtonStyle.success,
                row=suggest_row,
            )
            btn.callback = self._on_suggestion_accept
            self.add_item(btn)

        # UserSelect for manual pick
        user_row = min(suggest_row + (1 if suggested_pid else 0), 3)
        user_select = discord.ui.UserSelect(
            placeholder=f"Someone else — pick user for {current_ign}"[:150],
            row=user_row,
        )
        user_select.callback = self._on_user_select
        self.add_item(user_select)

        # Reject button
        reject_row = min(user_row + 1, 4)
        reject_btn = discord.ui.Button(
            label="Reject Upload",
            style=discord.ButtonStyle.danger,
            row=reject_row,
        )
        reject_btn.callback = self._on_reject
        self.add_item(reject_btn)

    def build_embed(self) -> discord.Embed:
        if not self.remaining:
            return discord.Embed(
                title=f"Match #{self.match_id} — stats saved",
                description=f"All IGNs resolved. **{len(self.rows_out)}** player rows committed.",
                color=discord.Color.green(),
            )

        current_ign = self.remaining[self.current_index]
        suggested_pid = self.suggestions.get(current_ign)

        lines = [f"**{len(self.remaining)}** IGN(s) could not be auto-mapped.", ""]

        # Show resolved IGNs first
        for ign, pid in self.resolved.items():
            member = self.guild.get_member(pid)
            name = member.display_name if member else str(pid)
            lines.append(f"\u2705 `{ign}` \u2192 **{name}**")

        # Show remaining IGNs
        for ign in self.remaining:
            p = self._player_by_ign.get(ign)
            stat_info = ""
            if p:
                stat_info = f" \u2014 {p.role or '?'} | {p.kills}/{p.deaths}/{p.assists}"
            pointer = "\u25b6 " if ign == current_ign else "   "
            lines.append(f"{pointer}`{ign}`{stat_info}")

        if suggested_pid is not None:
            member = self.guild.get_member(suggested_pid)
            name = member.mention if member else f"<@{suggested_pid}>"
            lines.append("")
            lines.append(f"Suggested: {name}")

        embed = discord.Embed(
            title=f"Match #{self.match_id} \u2014 unmapped IGNs",
            description="\n".join(lines),
            color=discord.Color.orange(),
        )
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        self._refresh_timeout()  # keep the panel alive while in use
        if not isinstance(interaction.user, discord.Member):
            return False
        if not await self.cog.is_cm_admin(interaction.user):
            await interaction.response.send_message(
                "Only admins can resolve IGN mappings.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_pick_ign(self, interaction: discord.Interaction):
        select = interaction.data.get("values") if interaction.data else None
        chosen = select[0] if select else None
        if chosen and chosen in self.remaining:
            self.current_index = self.remaining.index(chosen)
            self._rebuild_items()
            embed = self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    async def _on_suggestion_accept(self, interaction: discord.Interaction):
        if not self.remaining:
            return
        current_ign = self.remaining[self.current_index]
        suggested_pid = self.suggestions.get(current_ign)
        if suggested_pid is None:
            await interaction.response.send_message("No suggestion available.", ephemeral=True)
            return
        await self._apply_link(interaction, suggested_pid)

    async def _on_user_select(self, interaction: discord.Interaction):
        raw_values = interaction.data.get("values") if interaction.data else None
        if not raw_values:
            await interaction.response.send_message("No user selected.", ephemeral=True)
            return
        try:
            selected_id = int(raw_values[0])
        except (TypeError, ValueError):
            await interaction.response.send_message("Invalid selection.", ephemeral=True)
            return

        # Check if already mapped to another IGN
        already_mapped = {r["player_id"] for r in self.rows_out}
        if selected_id in already_mapped:
            mapped_ign = next((r["ign"] for r in self.rows_out if r["player_id"] == selected_id), "?")
            await interaction.response.send_message(
                f"<@{selected_id}> is already mapped to IGN `{mapped_ign}`. Pick someone else.",
                ephemeral=True,
            )
            return

        # Verify membership in the match roster
        roster = {mp["player_id"] for mp in self.match_players}
        if selected_id not in roster:
            current_ign = self.remaining[self.current_index] if self.remaining else "?"
            confirm_view = _ConfirmLinkResolverView(self, interaction.user.id, selected_id)
            await interaction.response.send_message(
                f"<@{selected_id}> is not in match #{self.match_id}. Link IGN `{current_ign}` anyway?",
                view=confirm_view,
                ephemeral=True,
            )
            return

        await self._apply_link(interaction, selected_id)

    async def _apply_link(self, interaction: discord.Interaction, selected_id: int):
        if not self.remaining:
            return
        current_ign = self.remaining[self.current_index]

        try:
            # Only overwrite the stored IGN if the player doesn't already have
            # one set — otherwise keep their authoritative IGN (which may have
            # unicode characters that OCR can't reproduce).
            existing_ign = await DatabaseHelper.get_player_ign(selected_id, self.game_id)
            if existing_ign:
                logger.info(
                    f"Resolver: player {selected_id} already has IGN '{existing_ign}', "
                    f"keeping it instead of OCR '{current_ign}' "
                    f"(game_id={self.game_id}, match_id={self.match_id})"
                )
            else:
                await DatabaseHelper.set_player_ign(selected_id, self.game_id, current_ign)
                logger.info(
                    f"Linked Rivals IGN '{current_ign}' -> player_id={selected_id} "
                    f"(game_id={self.game_id}, match_id={self.match_id})"
                )
        except Exception as e:
            logger.error(f"Failed to set player IGN during resolver: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Failed to save IGN link. Try again.", ephemeral=True
                )
            return

        # Track resolution for embed display
        self.resolved[current_ign] = selected_id
        self.remaining.pop(self.current_index)
        if self.current_index >= len(self.remaining):
            self.current_index = 0
        if selected_id in self.unmatched_roster:
            self.unmatched_roster.remove(selected_id)

        await self._remap()

        if not self.remaining:
            await self._commit_and_finalize(interaction)
            return

        # Still more to resolve — update embed and view
        self._rebuild_items()
        embed = self.build_embed()
        if interaction.response.is_done():
            try:
                await interaction.message.edit(embed=embed, view=self)
            except Exception:
                pass
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def _commit_and_finalize(self, interaction: discord.Interaction):
        """All IGNs resolved. Commit stats, clean up, log."""
        if not interaction.response.is_done():
            await interaction.response.defer()

        try:
            await DatabaseHelper.save_rivals_match_stats(self.match_id, self.rows_out)
            for row in self.rows_out:
                row_ign = (row.get("ign") or "").strip()
                if row_ign and row.get("player_id"):
                    try:
                        await DatabaseHelper.set_player_ign(
                            row["player_id"], self.game_id, row_ign
                        )
                    except Exception as e:
                        logger.error(f"IGN refresh failed for {row.get('player_id')}: {e}")
            await DatabaseHelper.mark_rivals_upload_status(self.upload_id, "committed")
            await DatabaseHelper.supersede_prior_rivals_uploads(
                self.match_id, keep_upload_id=self.upload_id
            )
        except Exception as e:
            logger.error(f"Failed to commit stats after resolver: {e}")
            await interaction.followup.send(
                "Failed to save stats. Check logs.", ephemeral=True
            )
            return

        # Render + post results card to the game channel (lobby is already gone)
        try:
            red_players = [
                {**r, "medal_count": sum((r.get("medals") or {}).values())}
                for r in self.rows_out if (r.get("team") or "").upper() == "RED"
            ]
            blue_players = [
                {**r, "medal_count": sum((r.get("medals") or {}).values())}
                for r in self.rows_out if (r.get("team") or "").upper() == "BLUE"
            ]
            target_channel, map_name = await self.cog._rivals_results_target(
                self.guild, self.match_id
            )
            image_buf = await self.cog.stats_generator.generate_rivals_results_image(
                red_players=red_players,
                blue_players=blue_players,
                winning_team=self.winning_team or "",
                map_name=map_name,
            )
            if image_buf and target_channel is not None:
                await target_channel.send(
                    file=discord.File(image_buf, filename=f"rivals_match_{self.match_id}.png")
                )
        except Exception as e:
            logger.error(f"Failed to render results card from resolver: {e}")

        # Cleanup match (no-ops on already-deleted channels)
        try:
            match_row = await DatabaseHelper.get_match(self.match_id)
            if match_row and self.guild:
                await self.cog.cleanup_match(self.guild, match_row)
        except Exception as e:
            logger.error(f"cleanup_match after resolver commit failed: {e}")

        # Log channel notification
        await self.cog._send_rivals_stats_log(
            guild=self.guild,
            match_id=self.match_id,
            rows_out=self.rows_out,
            winning_team=self.winning_team,
            source="ign-resolver",
        )

        # Update embed to success
        for child in self.children:
            try:
                child.disabled = True
            except Exception:
                pass

        embed = discord.Embed(
            title=f"Match #{self.match_id} \u2014 stats saved",
            description=f"All IGNs resolved. **{len(self.rows_out)}** player rows committed.",
            color=discord.Color.green(),
        )
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass
        self.stop()

    async def _on_reject(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await DatabaseHelper.mark_rivals_upload_status(self.upload_id, "rejected")
        for child in self.children:
            try:
                child.disabled = True
            except Exception:
                pass
        embed = discord.Embed(
            title=f"Match #{self.match_id} \u2014 upload rejected",
            description="An admin rejected this upload. Stats were not saved.",
            color=discord.Color.red(),
        )
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass
        self.stop()

    async def _remap(self):
        """Re-run the full IGN-mapping loop against the current player_ign table."""
        from .api_clients import RivalsVisionClient

        ign_to_player = await DatabaseHelper.build_ign_lookup(self.match_id, self.game_id)

        rows_out: List[dict] = []
        unmapped: List[str] = []
        for p in self.result.players:
            # Honor manual resolutions — don't re-fuzzy-match IGNs the admin
            # already linked (the stored IGN may differ from the OCR form).
            pid = self.resolved.get(p.ign)
            if pid is None:
                pid = resolve_ocr_ign(p.ign or "", ign_to_player)
            if pid is None:
                unmapped.append(p.ign)
                continue
            rows_out.append({
                "player_id": pid,
                "ign": p.ign,
                "role": p.role,
                "team": p.team,
                "kills": p.kills,
                "deaths": p.deaths,
                "assists": p.assists,
                "final_hits": p.final_hits,
                "damage": p.damage,
                "damage_blocked": p.damage_blocked,
                "healing": p.healing,
                "accuracy_pct": p.accuracy_pct,
                "mvp_svp": p.mvp_svp,
                "medals": RivalsVisionClient.medals_to_counts(p.medals),
            })
        self.rows_out = rows_out
        self.remaining = unmapped

    async def on_timeout(self):
        try:
            await DatabaseHelper.mark_rivals_upload_status(self.upload_id, "timed_out")
        except Exception:
            pass
        logger.warning(f"IGN resolver for match {self.match_id} timed out with "
                       f"{len(self.remaining)} unresolved IGNs")


# =============================================================================
# CONSOLIDATED ACTION VIEWS
# =============================================================================

class OverwatchWeightsView(ExpiringView):
    """Editor for a game's Overwatch role-balance weights.

    Weights only affect team balancing (how heavily a role's MMR gap counts) —
    they never touch stored MMR. Higher weight = the balancer guards that role's
    balance harder.
    """

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        self._refresh_timeout()  # keep the panel alive while in use
        if await self.cog.is_cm_admin(interaction.user):
            return True
        await interaction.response.send_message(
            "You no longer have permission to use this panel.", ephemeral=True
        )
        return False

    async def build_embed(self) -> discord.Embed:
        weights = await DatabaseHelper.get_ow_role_weights(self.game.game_id)
        lines = [
            f"{OW_ROLE_EMOJI.get(r, '')} **{r}** — `{weights.get(r, 1.0):.2f}`"
            for r in OW_ROLES
        ]
        # Everything Overwatch-specific — the 2-2-2 balancer, the role-coverage
        # gate, the queue composition line — is scoped to the 12-player main
        # queue. Any other size silently falls back to the generic MMR balancer,
        # which reads an aggregate MMR column Overwatch never updates.
        warnings = []
        if self.game.player_count != 12:
            warnings.append(
                f"⚠️ **player_count is {self.game.player_count}, not 12.** Role queue, "
                "the 2-2-2 balancer and the composition gate are all disabled at "
                "this size, and teams fall back to an MMR that never updates for "
                "Overwatch. Set it to 12 for the real 6v6 queue."
            )
        if not self.game.role_required:
            warnings.append(
                "⚠️ **Role selection is off.** Players can queue without picking "
                "roles; the balancer treats them as fill and may assign anything."
            )
        if not await DatabaseHelper.get_mmr_roles(self.game.game_id):
            warnings.append(
                "⚠️ **No rank ladder configured.** New players seed at the 1000 "
                "default instead of their rank, and rank roles can't be granted."
            )
        body = (
            "Higher weight makes the balancer treat that role's MMR gap as "
            "more important when forming 2-2-2 teams. Balancing only — stored "
            "MMR is never scaled.\n\n" + "\n".join(lines)
        )
        if warnings:
            body += "\n\n" + "\n\n".join(warnings)
        embed = discord.Embed(
            title=f"Overwatch Role Weights — {self.game.name}",
            color=COLOR_WARNING if warnings else COLOR_NEUTRAL,
            description=body,
        )
        embed.set_footer(text="Defaults: Tank 1.30 · Support 1.15 · DPS 1.00")
        return embed

    @discord.ui.button(label="Edit Weights", style=discord.ButtonStyle.primary)
    async def edit_weights(self, interaction: discord.Interaction, button: discord.ui.Button):
        weights = await DatabaseHelper.get_ow_role_weights(self.game.game_id)
        await interaction.response.send_modal(OWWeightsModal(self, weights))

    @discord.ui.button(label="Reset to Defaults", style=discord.ButtonStyle.secondary)
    async def reset_weights(self, interaction: discord.Interaction, button: discord.ui.Button):
        for role, weight in OW_DEFAULT_ROLE_WEIGHTS.items():
            await DatabaseHelper.set_ow_role_weight(self.game.game_id, role, weight)
        embed = await self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Adjust Role MMR", style=discord.ButtonStyle.secondary)
    async def adjust_role_mmr(self, interaction: discord.Interaction, button: discord.ui.Button):
        bands = sorted((await DatabaseHelper.get_mmr_roles(self.game.game_id)).values())
        floor = bands[0] if bands else 500
        ceiling = max(bands[-1], floor) if bands else 6000
        await interaction.response.send_modal(OWRoleMMRModal(self, floor, ceiling))


class OWRoleMMRModal(discord.ui.Modal, title="Adjust Overwatch Role MMR"):
    """Admin correction for a player's hidden per-role MMR (e.g. a smurf).
    Sets the role's rating directly; a fresh role starts at placement K."""
    player_input = discord.ui.TextInput(label="Player (ID or @mention)", required=True, max_length=32)
    role_input = discord.ui.TextInput(label="Role (Tank / DPS / Support)", required=True, max_length=10)
    mmr_input = discord.ui.TextInput(label="New MMR", required=True, max_length=5)

    def __init__(self, parent: 'OverwatchWeightsView', floor: int = 500, ceiling: int = 6000):
        super().__init__()
        self.parent = parent
        # Bounds come from the game's own ladder. A value under the lowest band
        # leaves the player where update_mmr_roles can find no role to grant —
        # it strips their current rank role and hands back nothing.
        self.floor = floor
        self.ceiling = ceiling
        self.mmr_input.label = f"New MMR ({floor}-{ceiling})"

    async def on_submit(self, interaction: discord.Interaction):
        digits = re.sub(r'\D', '', self.player_input.value or '')
        if not digits:
            await interaction.response.send_message("Invalid player ID / mention.", ephemeral=True)
            return
        player_id = int(digits)

        role = normalize_ow_role(self.role_input.value)
        if role not in OW_ROLES:
            await interaction.response.send_message(
                "Role must be one of: Tank, DPS, Support.", ephemeral=True
            )
            return

        try:
            new_mmr = int(self.mmr_input.value)
        except ValueError:
            await interaction.response.send_message("MMR must be a whole number.", ephemeral=True)
            return
        if not (self.floor <= new_mmr <= self.ceiling):
            await interaction.response.send_message(
                f"MMR must be between {self.floor} and {self.ceiling} "
                f"(the configured rank ladder for {self.parent.game.name}).",
                ephemeral=True
            )
            return

        stats = await DatabaseHelper.get_ow_role_stats(player_id, self.parent.game.game_id, role)
        old = stats.effective_mmr
        stats.mmr = new_mmr - stats.admin_offset  # keep effective_mmr == new_mmr
        stats.is_new = False
        await DatabaseHelper.upsert_ow_role_stats(stats)

        await interaction.response.send_message(
            f"{OW_ROLE_EMOJI.get(role, '')} Set <@{player_id}>'s **{role}** MMR: "
            f"`{old}` → `{new_mmr}` for {self.parent.game.name} (hidden).",
            ephemeral=True,
        )


class OWWeightsModal(discord.ui.Modal, title="Edit Overwatch Weights"):
    tank = discord.ui.TextInput(label="Tank weight", placeholder="1.30", required=True, max_length=6)
    dps = discord.ui.TextInput(label="DPS weight", placeholder="1.00", required=True, max_length=6)
    support = discord.ui.TextInput(label="Support weight", placeholder="1.15", required=True, max_length=6)

    def __init__(self, parent: 'OverwatchWeightsView', weights: Dict[str, float]):
        super().__init__()
        self.parent = parent
        self.tank.default = f"{weights.get('Tank', 1.30):.2f}"
        self.dps.default = f"{weights.get('DPS', 1.00):.2f}"
        self.support.default = f"{weights.get('Support', 1.15):.2f}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            values = {
                "Tank": float(self.tank.value),
                "DPS": float(self.dps.value),
                "Support": float(self.support.value),
            }
        except ValueError:
            await interaction.response.send_message(
                "Weights must be numbers (e.g. `1.30`).", ephemeral=True
            )
            return
        for role, weight in values.items():
            if weight <= 0 or weight > 5:
                await interaction.response.send_message(
                    f"{role} weight `{weight}` is out of range (must be 0-5).", ephemeral=True
                )
                return
            await DatabaseHelper.set_ow_role_weight(self.parent.game.game_id, role, weight)
        embed = await self.parent.build_embed()
        await interaction.response.edit_message(embed=embed, view=self.parent)


class SetBannerModal(discord.ui.Modal, title="Set Queue Banner"):
    banner_url = discord.ui.TextInput(
        label="Banner URL (leave blank to clear)",
        placeholder="https://example.com/banner.png or .gif",
        required=False,
        style=discord.TextStyle.short
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_page = parent_page
        if game.banner_url:
            self.banner_url.default = game.banner_url

    async def on_submit(self, interaction: discord.Interaction):
        url = self.banner_url.value.strip() if self.banner_url.value else None
        await DatabaseHelper.update_game(self.game.game_id, banner_url=url)
        note = f"Banner {'set' if url else 'cleared'} for **{self.game.name}**."
        await _respond_to_modal(interaction, self.parent_page, note)


class SetShortNameModal(discord.ui.Modal, title="Set Game Short Name"):
    """Set the per-game prefix used in lobby/VC channel names (e.g. 'rivals')."""

    short_name = discord.ui.TextInput(
        label="Short name (leave blank to auto-derive)",
        placeholder="e.g. rivals  →  rivals-lobby9P83E",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_page = parent_page
        if game.short_name:
            self.short_name.default = game.short_name

    async def on_submit(self, interaction: discord.Interaction):
        raw = (self.short_name.value or "").strip().lower()
        # Keep only alphanumerics so it's always a valid channel-name fragment
        cleaned = re.sub(r'[^a-z0-9]', '', raw)
        value = cleaned or None
        await DatabaseHelper.update_game(self.game.game_id, short_name=value)
        if value:
            note = f"Short name set to `{value}` — lobbies will be named `{value}-lobby…`."
        else:
            from .models import slugify_game_name
            note = (
                f"Short name cleared; auto-deriving "
                f"`{slugify_game_name(self.game.name)}` from the name."
            )
        await _respond_to_modal(interaction, self.parent_page, note)


# =============================================================================
# SETTINGS PANEL
#
# /cm_settings is a tree of pages drawn onto a single ephemeral message: a hub,
# five category pages, and a per-game page reached by picking the game *once*
# instead of re-picking it for every individual setting. Navigation happens
# through edit_message, so an admin never accumulates a trail of dead menus,
# and every page below the hub carries a Back button to its parent.
# =============================================================================


def _channel_label(guild: discord.Guild, channel_id) -> str:
    """Render a stored channel id as a mention, or a clear unset/missing marker."""
    if not channel_id:
        return "`not set`"
    cid = int(channel_id)
    channel = guild.get_channel(cid) or guild.get_thread(cid)
    if channel is None:
        return f"`missing ({cid})`"
    if isinstance(channel, discord.CategoryChannel):
        return f"**{channel.name}**"
    return channel.mention


def _role_label(guild: discord.Guild, role_id) -> str:
    """Render a stored role id as a mention, or a clear unset/missing marker."""
    if not role_id:
        return "`not set`"
    role = guild.get_role(int(role_id))
    return role.mention if role else f"`missing ({role_id})`"


async def _respond_to_modal(interaction: discord.Interaction, page: 'SettingsPage', note: str):
    """Close a modal by repainting the settings page behind it.

    A modal opened from a component carries that component's message, so the
    normal path redraws the panel with the change already visible instead of
    stacking a confirmation the admin has to dismiss. The ephemeral reply is
    the fallback for a modal reached any other way.
    """
    if page is not None and interaction.message is not None:
        try:
            await page.render(interaction, flash=f"✅ {note}")
            return
        except discord.HTTPException:
            pass
    if not interaction.response.is_done():
        await interaction.response.send_message(note, ephemeral=True)


class BackButton(discord.ui.Button):
    """Returns to the page that opened this one, redrawn with fresh data."""

    def __init__(self, parent: 'SettingsPage', row: int = 4, label: str = "Back"):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=row)
        self.parent_page = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_page.render(interaction)


class SettingsPage(ExpiringView):
    """One screen of /cm_settings.

    Subclasses declare their components as usual and override
    :meth:`build_embed`. Passing ``parent`` adds a Back button and keeps the
    whole ancestor chain's timeout alive while a nested page is in use, so a
    parent can't quietly expire underneath a long editing session.
    """

    back_row = 4

    def __init__(self, cog: 'CustomMatch', parent: Optional['SettingsPage'] = None,
                 timeout: float = 300):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.parent_page = parent
        if parent is not None:
            self.add_item(BackButton(parent, row=self.back_row))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        page = self
        while page is not None:
            page._refresh_timeout()
            page = getattr(page, "parent_page", None)
        if await self.cog.is_cm_admin(interaction.user):
            return True
        await interaction.response.send_message(
            "You no longer have permission to use this panel.", ephemeral=True
        )
        return False

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        raise NotImplementedError

    async def render(self, interaction: discord.Interaction, *, flash: Optional[str] = None):
        """Draw this page onto the message the interaction came from."""
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = False
        embed = await self.build_embed(interaction.guild)
        if flash:
            embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        await interaction.response.edit_message(embed=embed, view=self)
        self.message = interaction.message

    async def open(self, interaction: discord.Interaction):
        """Send this page as a fresh ephemeral message — the panel entry point."""
        embed = await self.build_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, view=self, ephemeral=True)
        await self.track(interaction)

    async def show_view(self, interaction: discord.Interaction, view: ExpiringView,
                        embed: discord.Embed, *, back_row: int = 4):
        """Render a standalone editor view onto the panel, with a way back here."""
        view.attach_back(self, row=back_row)
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = interaction.message

    async def open_flow(self, interaction: discord.Interaction, view: discord.ui.View, *,
                        content: str = None, embed: discord.Embed = None):
        """Hand off to a self-contained wizard on its own ephemeral message.

        Flows that finish by replacing their own message can't live on the
        panel — they'd take the panel down with them when they complete.
        """
        await interaction.response.send_message(
            content=content, embed=embed, view=view, ephemeral=True
        )
        if isinstance(view, ExpiringView):
            await view.track(interaction)


def _game_option(game: GameConfig) -> discord.SelectOption:
    return discord.SelectOption(
        label=game.name[:100],
        value=str(game.game_id),
        description=f"{game.player_count} players · {game.queue_type.value} queue"[:100],
    )


class GamePickSelect(discord.ui.Select):
    """Game picker that hands the chosen game to a coroutine."""

    def __init__(self, games: List[GameConfig], on_pick, *,
                 placeholder: str = "Select a game…", row: int = 0):
        super().__init__(
            placeholder=placeholder,
            options=[_game_option(g) for g in games[:25]],
            row=row,
        )
        self.on_pick = on_pick

    async def callback(self, interaction: discord.Interaction):
        game = await DatabaseHelper.get_game(int(self.values[0]))
        if game is None:
            await interaction.response.send_message("That game no longer exists.", ephemeral=True)
            return
        await self.on_pick(interaction, game)


class GamePickerPage(SettingsPage):
    """A standalone 'which game?' step for actions that don't live on a game page."""

    def __init__(self, cog: 'CustomMatch', parent: SettingsPage, games: List[GameConfig],
                 on_pick, *, title: str, blurb: str, accent: int = COLOR_NEUTRAL):
        super().__init__(cog, parent, timeout=180)
        self._title = title
        self._blurb = blurb
        self._accent = accent
        self.add_item(GamePickSelect(games, on_pick))

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(title=self._title, description=self._blurb, color=self._accent)


async def _require_games(interaction: discord.Interaction) -> Optional[List[GameConfig]]:
    """Games, or None after telling the admin there aren't any yet."""
    games = await DatabaseHelper.get_all_games()
    if games:
        return games
    await interaction.response.send_message(
        "No games configured yet — add one from **Games → Add Game**.", ephemeral=True
    )
    return None


# -----------------------------------------------------------------------------
# Hub
# -----------------------------------------------------------------------------

class SettingsView(SettingsPage):
    """Main settings panel for server admins — the hub every other page hangs off."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(cog, parent=None, timeout=600)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        from .models import resolve_short_name

        log_id = await DatabaseHelper.get_config("log_channel_id")
        admin_channel_id = await DatabaseHelper.get_config("cm_admin_channel_id")
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        games = await DatabaseHelper.get_all_games()

        embed = discord.Embed(
            title="Custom Matches · Settings",
            description=(
                "Everything for the custom match system, grouped by what you're "
                "trying to change. Pages open right here — use **Back** to move "
                "around without re-running the command."
            ),
            color=COLOR_NEUTRAL,
        )
        embed.add_field(
            name="🎮 Games",
            value="Queues, toggles, timers, schedules, ranks, emojis",
            inline=True,
        )
        embed.add_field(
            name="📺 Channels",
            value="Where queues, results, logs and alerts go",
            inline=True,
        )
        embed.add_field(
            name="🔑 Access",
            value="Admin role, mod roles, player blacklist",
            inline=True,
        )
        embed.add_field(
            name="📊 Stats & Data",
            value="Rivals OCR, Valorant fetches, stat wipes",
            inline=True,
        )
        embed.add_field(
            name="🧰 Maintenance",
            value="Fix a match, clear stale lobbies, bulk register",
            inline=True,
        )
        embed.add_field(name="​", value="​", inline=True)

        embed.add_field(
            name="Server setup",
            value=(
                f"Log channel · {_channel_label(guild, log_id)}\n"
                f"Admin channel · {_channel_label(guild, admin_channel_id)}\n"
                f"CM Admin role · {_role_label(guild, admin_role_id)}"
            ),
            inline=False,
        )

        if games:
            lines = []
            for game in games:
                lines.append(
                    f"**{game.name}** · {game.player_count}p · {game.queue_type.value} · "
                    f"{_channel_label(guild, game.queue_channel_id)} · "
                    f"prefix `{resolve_short_name(game)}`"
                )
            embed.add_field(name=f"Games ({len(games)})", value="\n".join(lines), inline=False)
        else:
            embed.add_field(
                name="Games",
                value="None yet — start with **Games → Add Game**.",
                inline=False,
            )
        embed.set_footer(text="Only CM Admins can use this panel.")
        return embed

    @discord.ui.button(label="Games", style=discord.ButtonStyle.primary, row=0)
    async def open_games(self, interaction: discord.Interaction, button: discord.ui.Button):
        await GamesPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Channels", style=discord.ButtonStyle.secondary, row=0)
    async def open_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ChannelsPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Access", style=discord.ButtonStyle.secondary, row=0)
    async def open_access(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AccessPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Stats & Data", style=discord.ButtonStyle.secondary, row=1)
    async def open_data(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DataPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Maintenance", style=discord.ButtonStyle.secondary, row=1)
    async def open_maintenance(self, interaction: discord.Interaction, button: discord.ui.Button):
        await MaintenancePage(self.cog, self).render(interaction)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=1)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.render(interaction)


# -----------------------------------------------------------------------------
# Games
# -----------------------------------------------------------------------------

class GamesPage(SettingsPage):
    """Game roster: pick one to configure, or add/remove from the list."""

    def __init__(self, cog: 'CustomMatch', parent: SettingsPage):
        super().__init__(cog, parent, timeout=300)
        self._select: Optional[GamePickSelect] = None

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        from .models import resolve_short_name

        games = await DatabaseHelper.get_all_games()

        # The picker is rebuilt on every render so a game added or deleted this
        # session shows up without leaving the page.
        if self._select is not None:
            self.remove_item(self._select)
            self._select = None
        if games:
            self._select = GamePickSelect(
                games, self._open_game, placeholder="Configure a game…", row=0
            )
            self.add_item(self._select)

        embed = discord.Embed(
            title="Games",
            description=(
                "Pick a game to open everything that belongs to it — queue rules, "
                "toggles, timers, schedule, rank ladder and secondary queue."
                if games else
                "No games yet. **Add Game** creates one; you can set its channel "
                "and rules straight after."
            ),
            color=COLOR_NEUTRAL,
        )
        for game in games:
            details = [
                f"{game.player_count} players · {game.queue_type.value} queue",
                f"Queue channel · {_channel_label(guild, game.queue_channel_id)}",
                f"Channel prefix · `{resolve_short_name(game)}`",
            ]
            if game.secondary_queue_enabled:
                details.append(f"Secondary · **{game.secondary_queue_name or 'enabled'}**")
            if game.schedule_enabled:
                details.append("Schedule · **on**")
            if not game.enabled:
                details.insert(0, "⛔ **Disabled**")
            name = game.name if game.enabled else f"{game.name} (disabled)"
            embed.add_field(name=name, value="\n".join(details), inline=True)
        return embed

    async def _open_game(self, interaction: discord.Interaction, game: GameConfig):
        await GameSettingsPage(self.cog, self, game).render(interaction)

    @discord.ui.button(label="Add Game", style=discord.ButtonStyle.success, row=1)
    async def add_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddGameModal(self.cog, self))

    @discord.ui.button(label="Emojis", style=discord.ButtonStyle.secondary, row=1)
    async def emojis(self, interaction: discord.Interaction, button: discord.ui.Button):
        await EmojisPage(self.cog, self).render(interaction)


class GameSettingsPage(SettingsPage):
    """Everything scoped to one game, so the game is chosen once and stays chosen."""

    def __init__(self, cog: 'CustomMatch', parent: SettingsPage, game: GameConfig):
        super().__init__(cog, parent, timeout=600)
        self.game = game
        # Overwatch weights only mean something for an Overwatch game; hiding the
        # button beats a button that exists only to explain it doesn't apply.
        if not is_overwatch_game(game):
            self.remove_item(self.overwatch_weights)

    async def _reload(self) -> GameConfig:
        fresh = await DatabaseHelper.get_game(self.game.game_id)
        if fresh is not None:
            self.game = fresh
        return self.game

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        from .models import resolve_short_name

        game = await self._reload()
        embed = discord.Embed(
            title=f"{game.name} · Settings" + ("" if game.enabled else " · ⛔ disabled"),
            color=COLOR_NEUTRAL if game.enabled else COLOR_WARNING,
        )
        if not game.enabled:
            embed.description = (
                "⛔ **This game is disabled.** Its data is preserved, but it's "
                "hidden from setup and stat menus and its queue is closed. "
                "Re-enable it under **Toggles**."
            )
        embed.add_field(
            name="Queue",
            value=(
                f"Players · **{game.player_count}**\n"
                f"Type · **{game.queue_type.value}**\n"
                f"Captains · **{game.captain_selection.value}**\n"
                f"Ready timer · **{game.ready_timer_seconds}s**"
            ),
            inline=True,
        )
        toggles = [
            f"VC creation · {'**on**' if game.vc_creation_enabled else 'off'}",
            f"Queue role · {'**required**' if game.queue_role_required else 'off'}",
            f"IGN · {'**required**' if game.ign_required else 'off'}",
            f"Role prefs · {'**required**' if game.role_required else 'off'}",
        ]
        if not is_valorant_game(game):
            toggles.append(
                f"PC players · {f'**+{game.pc_offset_tiers:g} tier**' if game.pc_enabled else 'off'}"
            )
        embed.add_field(name="Rules", value="\n".join(toggles), inline=True)
        embed.add_field(
            name="Identity",
            value=(
                f"Queue channel · {_channel_label(guild, game.queue_channel_id)}\n"
                f"Queue role · {_role_label(guild, game.verified_role_id)}\n"
                f"Channel prefix · `{resolve_short_name(game)}`\n"
                f"Banner · {'**set**' if game.banner_url else '`none`'}"
            ),
            inline=True,
        )

        mmr_roles = await DatabaseHelper.get_mmr_roles(game.game_id)
        extras = [
            f"Rank ladder · {f'**{len(mmr_roles)} roles**' if mmr_roles else '`not configured`'}",
            f"Schedule · {'**on**' if game.schedule_enabled else 'off'}",
        ]
        if game.secondary_queue_enabled:
            extras.append(f"Secondary queue · **{game.secondary_queue_name or 'enabled'}**")
        else:
            extras.append("Secondary queue · off")
        embed.add_field(name="Ranking & extras", value="\n".join(extras), inline=False)
        embed.set_footer(text="Channels for this game live under Settings → Channels.")
        return embed

    # -- Row 0: how the queue behaves ----------------------------------------

    @discord.ui.button(label="Basics", style=discord.ButtonStyle.primary, row=0)
    async def basics(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EditGameModal(self.cog, await self._reload(), self))

    @discord.ui.button(label="Toggles", style=discord.ButtonStyle.primary, row=0)
    async def toggles(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = await self._reload()
        view = GameTogglesView(self.cog, game)
        await self.show_view(interaction, view, view.build_embed(), back_row=2)

    @discord.ui.button(label="Ready Timer", style=discord.ButtonStyle.primary, row=0)
    async def ready_timer(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            ReadyTimerModal(self.cog, await self._reload(), self)
        )

    @discord.ui.button(label="Schedule", style=discord.ButtonStyle.primary, row=0)
    async def schedule(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = await self._reload()
        view = QueueScheduleView(self.cog, game)
        await self.show_view(interaction, view, build_schedule_embed(game), back_row=3)

    @discord.ui.button(label="Secondary Queue", style=discord.ButtonStyle.primary, row=0)
    async def secondary_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = await self._reload()
        view = SecondaryQueueSettingsView(self.cog, game)
        await self.show_view(interaction, view, await view.build_status_embed(), back_row=3)

    # -- Row 1: identity ------------------------------------------------------

    @discord.ui.button(label="Banner", style=discord.ButtonStyle.secondary, row=1)
    async def banner(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetBannerModal(self.cog, await self._reload(), self))

    @discord.ui.button(label="Short Name", style=discord.ButtonStyle.secondary, row=1)
    async def short_name(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetShortNameModal(self.cog, await self._reload(), self))

    @discord.ui.button(label="Queue Role", style=discord.ButtonStyle.secondary, row=1)
    async def queue_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        await QueueRolePage(self.cog, self, await self._reload()).render(interaction)

    # -- Row 2: ranking & players --------------------------------------------

    @discord.ui.button(label="Rank Ladder", style=discord.ButtonStyle.primary, row=2)
    async def rank_ladder(self, interaction: discord.Interaction, button: discord.ui.Button):
        await RankLadderPage(self.cog, self, await self._reload()).render(interaction)

    @discord.ui.button(label="Overwatch Weights", style=discord.ButtonStyle.primary, row=2)
    async def overwatch_weights(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = await self._reload()
        view = OverwatchWeightsView(self.cog, game)
        await self.show_view(interaction, view, await view.build_embed(), back_row=1)

    @discord.ui.button(label="Player MMR", style=discord.ButtonStyle.secondary, row=2)
    async def player_mmr(self, interaction: discord.Interaction, button: discord.ui.Button):
        await PlayerMMRPage(self.cog, self, await self._reload()).render(interaction)

    @discord.ui.button(label="Admin Offset", style=discord.ButtonStyle.secondary, row=2)
    async def admin_offset(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AdminOffsetPage(self.cog, self, await self._reload()).render(interaction)

    # -- Row 3: destructive ---------------------------------------------------

    @discord.ui.button(label="Delete Game", style=discord.ButtonStyle.danger, row=3)
    async def delete_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DeleteGamePage(self.cog, self, await self._reload()).render(interaction)


class QueueRolePage(SettingsPage):
    """Set or clear the role a player must hold to join this game's queue."""

    def __init__(self, cog: 'CustomMatch', parent: GameSettingsPage, game: GameConfig):
        super().__init__(cog, parent, timeout=180)
        self.game = game
        self.select = discord.ui.RoleSelect(placeholder="Pick the queue role…", row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        game = await DatabaseHelper.get_game(self.game.game_id) or self.game
        gate = "**enforced**" if game.queue_role_required else "not enforced (toggle it under **Toggles**)"
        return discord.Embed(
            title=f"{game.name} · Queue Role",
            description=(
                f"Current role · {_role_label(guild, game.verified_role_id)}\n"
                f"Gate · {gate}\n\n"
                "Players without this role are turned away at the queue when the "
                "**Queue role required** toggle is on."
            ),
            color=COLOR_NEUTRAL,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        role = self.select.values[0]
        await DatabaseHelper.update_game(self.game.game_id, verified_role_id=role.id)
        await self.render(interaction, flash=f"✅ Queue role set to **{role.name}**.")

    @discord.ui.button(label="Clear", style=discord.ButtonStyle.danger, row=1)
    async def clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game.game_id, verified_role_id=None)
        await self.render(interaction, flash="✅ Queue role cleared.")


async def _seed_player_from_rank(cog: 'CustomMatch', guild: discord.Guild, user_id: int,
                                 game_id: int, mmr: int) -> str:
    """Write a player's MMR for a game and bring everything downstream in line."""
    stats = await DatabaseHelper.get_player_stats(user_id, game_id)
    stats.mmr = mmr
    await DatabaseHelper.update_player_stats(stats)
    # Overwatch balances off per-role MMR, not this column — seed that too.
    seeded = await DatabaseHelper.sync_ow_role_seed(user_id, game_id, stats.effective_mmr)
    await cog.update_mmr_roles(guild, user_id, game_id, stats.effective_mmr)
    return f" Seeded {', '.join(seeded)} MMR." if seeded else ""


class PlayerMMRPage(SettingsPage):
    """Set one player's MMR for this game from the rank role they hold."""

    def __init__(self, cog: 'CustomMatch', parent: 'GameSettingsPage', game: GameConfig):
        super().__init__(cog, parent, timeout=300)
        self.game = game
        self.select = discord.ui.UserSelect(placeholder="Pick a player…", row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        ranks = await DatabaseHelper.get_mmr_roles(self.game.game_id)
        if ranks:
            body = (
                "Sets a player's MMR from the rank role they hold, and grants "
                "the matching rank roles back. If they hold no rank role you'll "
                f"be asked which rank to use.\n\nRanks configured · **{len(ranks)}**"
            )
        else:
            body = (
                "⚠️ No rank ladder configured for this game — set one up under "
                "**Rank Ladder** before seeding players."
            )
        return discord.Embed(
            title=f"{self.game.name} · Set Player MMR",
            description=body,
            color=COLOR_NEUTRAL if ranks else COLOR_WARNING,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        picked = self.select.values[0]
        member = interaction.guild.get_member(picked.id)
        if member is None:
            await interaction.response.send_message("That user isn't in the server.", ephemeral=True)
            return

        ranks = await DatabaseHelper.get_mmr_roles(self.game.game_id)
        if not ranks:
            await interaction.response.send_message(
                "No rank ladder configured for this game.", ephemeral=True
            )
            return

        # Highest rank wins, so a player holding several isn't seeded off
        # whichever one Discord happens to list first.
        detected = max((r for r in member.roles if r.id in ranks),
                       key=lambda r: ranks[r.id], default=None)
        if detected is None:
            await PlayerRankPickPage(
                self.cog, self, self.game, member, ranks, interaction.guild
            ).render(interaction)
            return

        seeded = await _seed_player_from_rank(
            self.cog, interaction.guild, member.id, self.game.game_id, ranks[detected.id]
        )
        await self.render(
            interaction,
            flash=f"✅ **{member.display_name}** set to {ranks[detected.id]} MMR "
                  f"(from {detected.name}).{seeded}",
        )


class PlayerRankPickPage(SettingsPage):
    """Which rank to seed from, for a player who holds none of them."""

    def __init__(self, cog: 'CustomMatch', parent: PlayerMMRPage, game: GameConfig,
                 member: discord.Member, ranks: Dict[int, int], guild: discord.Guild):
        super().__init__(cog, parent, timeout=180)
        self.game = game
        self.member = member
        self.ranks = ranks
        options = []
        for role_id, mmr in sorted(ranks.items(), key=lambda x: x[1], reverse=True):
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=f"{name} — {mmr} MMR"[:100],
                                                value=str(role_id)))
        self.select = discord.ui.Select(placeholder="Pick a rank…", options=options[:25], row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=f"{self.game.name} · Rank for {self.member.display_name}",
            description=(
                f"**{self.member.display_name}** holds no rank role for this game. "
                "Pick the rank to seed their MMR from."
            ),
            color=COLOR_NEUTRAL,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        role_id = int(self.select.values[0])
        mmr = self.ranks.get(role_id, 1000)
        seeded = await _seed_player_from_rank(
            self.cog, interaction.guild, self.member.id, self.game.game_id, mmr
        )
        role = interaction.guild.get_role(role_id)
        await self.parent_page.render(
            interaction,
            flash=f"✅ **{self.member.display_name}** set to {mmr} MMR "
                  f"(from {role.name if role else role_id}).{seeded}",
        )


class AdminOffsetPage(SettingsPage):
    """A manual MMR nudge applied on top of a player's earned rating."""

    def __init__(self, cog: 'CustomMatch', parent: 'GameSettingsPage', game: GameConfig):
        super().__init__(cog, parent, timeout=300)
        self.game = game
        self.select = discord.ui.UserSelect(placeholder="Pick a player…", row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=f"{self.game.name} · Admin Offset",
            description=(
                "A flat adjustment added to a player's MMR for balancing purposes. "
                "It rides on top of their earned rating rather than replacing it, "
                "so wins and losses still move them normally.\n\n"
                "Pick a player to set or clear their offset."
            ),
            color=COLOR_NEUTRAL,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        picked = self.select.values[0]
        member = interaction.guild.get_member(picked.id)
        if member is None:
            await interaction.response.send_message("That user isn't in the server.", ephemeral=True)
            return
        stats = await DatabaseHelper.get_player_stats(member.id, self.game.game_id)
        await interaction.response.send_modal(
            AdminOffsetModal(self.cog, self.game.game_id, member, stats.admin_offset, self)
        )


class AdminOffsetModal(discord.ui.Modal, title="Set Admin Offset"):
    offset = discord.ui.TextInput(label="Offset (e.g. 100 or -50, 0 to clear)", required=True)

    def __init__(self, cog: 'CustomMatch', game_id: int, member: discord.Member,
                 current: int, parent_page: 'AdminOffsetPage'):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.member = member
        self.parent_page = parent_page
        self.title = f"Offset for {member.display_name}"[:45]
        self.offset.default = str(current or 0)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(self.offset.value)
        except ValueError:
            await interaction.response.send_message("Offset must be a whole number.", ephemeral=True)
            return

        stats = await DatabaseHelper.get_player_stats(self.member.id, self.game_id)
        stats.admin_offset = value
        await DatabaseHelper.update_player_stats(stats)
        await self.cog.update_mmr_roles(
            interaction.guild, self.member.id, self.game_id, stats.effective_mmr
        )
        await _respond_to_modal(
            interaction, self.parent_page,
            f"**{self.member.display_name}**'s offset set to {value:+d} "
            f"(effective MMR {stats.effective_mmr})."
        )


class RankLadderPage(SettingsPage):
    """The MMR-role ladder for one game."""

    def __init__(self, cog: 'CustomMatch', parent: GameSettingsPage, game: GameConfig):
        super().__init__(cog, parent, timeout=600)
        self.game = game

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        roles = await DatabaseHelper.get_mmr_roles_with_labels(self.game.game_id)
        granting = await DatabaseHelper.get_rank_roles_enabled()
        if roles:
            lines = []
            missing = 0
            for role_id, data in sorted(roles.items(), key=lambda x: x[1]['mmr'], reverse=True):
                role = guild.get_role(role_id)
                label = data['label'] or f"{data['mmr']} MMR"
                if role:
                    lines.append(f"`{data['mmr']:>5}` {role.mention} — **{label}**")
                else:
                    missing += 1
                    # A deleted role is only a problem if we're meant to grant it.
                    # With granting off the rung is still doing its real work —
                    # naming a rank and spacing the ladder — so don't cry wolf.
                    note = "" if not granting else "  ⚠️ role deleted"
                    lines.append(f"`{data['mmr']:>5}` **{label}**{note}")
            body = "\n".join(lines)
            if missing and granting:
                body += (f"\n\n⚠️ **{missing}** rung(s) point at a deleted role — those "
                         "ranks can't be granted until they're re-mapped.")
        else:
            body = (
                "No ranks configured. Until at least one rung is mapped, new "
                "players seed at the 1000 default."
            )

        # The ladder earns its keep whether or not the badges are granted: it is
        # the vocabulary of the setup dropdowns, the band-drop that seeds a new
        # Overwatch role, the step size of the PC bump, and the loss floor.
        if granting:
            state = ("**Granting is ON** — the bot keeps each player wearing the "
                     "role matching their MMR.")
        else:
            state = ("**Granting is OFF** (global) — these rungs still drive rank "
                     "labels, seeding and the loss floor, but no Discord role is "
                     "handed out. Ratings still self-heal hourly.\n"
                     "-# Rungs whose Discord role was deleted show as a label only. "
                     "Re-map them before switching granting back on.")

        return discord.Embed(
            title=f"{self.game.name} · Rank Ladder",
            description=f"{state}\n\n{body}",
            color=COLOR_NEUTRAL if roles else COLOR_WARNING,
        )

    @discord.ui.button(label="Add / Update Rank", style=discord.ButtonStyle.success, row=0)
    async def add_rank(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AddRankPage(self.cog, self, self.game).render(interaction)

    @discord.ui.button(label="Remove Rank", style=discord.ButtonStyle.danger, row=0)
    async def remove_rank(self, interaction: discord.Interaction, button: discord.ui.Button):
        roles = await DatabaseHelper.get_mmr_roles(self.game.game_id)
        if not roles:
            await interaction.response.send_message("No ranks configured yet.", ephemeral=True)
            return
        await RemoveRankPage(self.cog, self, self.game, roles, interaction.guild).render(interaction)

    @discord.ui.button(label="Insert / Re-space Tier", style=discord.ButtonStyle.primary, row=2)
    async def remap_ladder(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Add / Update Rank edits a rung in place. This flow instead re-spaces the
        # whole ladder and *remaps every player onto it* so a new tier (e.g.
        # Emerald) can be inserted without wiping earned rating. Staged first,
        # previewed, then applied — see LadderRemapPage.
        staged = await DatabaseHelper.get_mmr_roles_with_labels(self.game.game_id)
        if not staged:
            await interaction.response.send_message(
                "Configure at least a couple of ranks first — there's nothing to re-space.",
                ephemeral=True,
            )
            return
        await LadderRemapPage(self.cog, self, self.game, staged).render(interaction)

    @discord.ui.button(label="Toggle Role Granting", style=discord.ButtonStyle.secondary, row=1)
    async def toggle_granting(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Global, not per-game: the switch is about whether the bot manages rank
        # badges at all, and a half-on state would be more confusing than useful.
        new_val = not await DatabaseHelper.get_rank_roles_enabled()
        await DatabaseHelper.set_rank_roles_enabled(new_val)
        await self.render(interaction, flash=(
            "✅ Rank-role granting **ON** for every game. The hourly audit will "
            "put everyone on the right role within the hour."
            if new_val else
            "✅ Rank-role granting **OFF** for every game. Nobody loses a role they "
            "already hold — use **Strip Rank Roles** to clear them."
        ))

    @discord.ui.button(label="Strip Rank Roles", style=discord.ButtonStyle.danger, row=1)
    async def strip_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        roles = await DatabaseHelper.get_mmr_roles(self.game.game_id)
        if not roles:
            await interaction.response.send_message("No ranks configured yet.", ephemeral=True)
            return
        await StripRankRolesPage(self.cog, self, self.game, roles).render(interaction)


class StripRankRolesPage(SettingsPage):
    """Remove this game's rank roles from everyone who holds one.

    Deliberately its own confirmed action rather than something the toggle does
    on the way past: switching granting off is reversible in one click, a bulk
    role removal across the whole server is not.
    """

    def __init__(self, cog: 'CustomMatch', parent: RankLadderPage, game: GameConfig,
                 mmr_roles: Dict[int, int]):
        super().__init__(cog, parent, timeout=300)
        self.game = game
        self.role_ids = set(mmr_roles)

    def _holders(self, guild: discord.Guild) -> Dict[int, int]:
        counts = {}
        for role_id in self.role_ids:
            role = guild.get_role(role_id)
            if role:
                counts[role_id] = len(role.members)
        return counts

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        counts = self._holders(guild)
        total = sum(counts.values())
        lines = []
        for role_id, n in sorted(counts.items(), key=lambda kv: kv[1], reverse=True):
            role = guild.get_role(role_id)
            lines.append(f"{role.mention if role else role_id} · **{n}**")
        body = "\n".join(lines) if lines else "None of these roles exist on this server."
        return discord.Embed(
            title=f"{self.game.name} · Strip Rank Roles",
            description=(
                f"Removes every **{self.game.name}** rank role from the "
                f"**{total}** member(s) holding one. The roles themselves are left "
                "on the server — delete them yourself once you're happy.\n\n"
                f"{body}"
            ),
            color=COLOR_WARNING,
        )

    @discord.ui.button(label="Strip them", style=discord.ButtonStyle.danger, row=0)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        await interaction.response.defer()
        removed = 0
        failed = 0
        for role_id in self.role_ids:
            role = guild.get_role(role_id)
            if not role:
                continue
            for member in list(role.members):
                try:
                    await member.remove_roles(role, reason="Rank roles disabled")
                    removed += 1
                except Exception as e:
                    failed += 1
                    logger.warning(f"strip rank roles: {member.id} / {role_id}: {e}")
                await asyncio.sleep(1)  # stay clear of role rate limits
        note = f"✅ Removed **{removed}** rank role(s)."
        if failed:
            note += f" **{failed}** failed — see the log."
        await self.render_after(interaction, note)

    async def render_after(self, interaction: discord.Interaction, flash: str):
        embed = await self.build_embed(interaction.guild)
        embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        await interaction.edit_original_response(embed=embed, view=self)


class AddRankPage(SettingsPage):
    """Pick a role, then name its MMR — the two halves of one ladder rung."""

    def __init__(self, cog: 'CustomMatch', parent: RankLadderPage, game: GameConfig):
        super().__init__(cog, parent, timeout=300)
        self.game = game
        self.select = discord.ui.RoleSelect(placeholder="Pick the rank role…", row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=f"{self.game.name} · Add Rank",
            description=(
                "Pick the Discord role for a rank; you'll set its MMR next. "
                "Picking a role that's already on the ladder updates it."
            ),
            color=COLOR_NEUTRAL,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        role = self.select.values[0]
        await interaction.response.send_modal(
            MMRValueModal(self.cog, self.game.game_id, role.id, role.name, self.parent_page)
        )


class RemoveRankPage(SettingsPage):
    """Drop a rung off the ladder."""

    def __init__(self, cog: 'CustomMatch', parent: RankLadderPage, game: GameConfig,
                 mmr_roles: Dict[int, int], guild: discord.Guild):
        super().__init__(cog, parent, timeout=300)
        self.game = game
        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=f"{name} — {mmr} MMR"[:100], value=str(role_id)))
        self.select = discord.ui.Select(placeholder="Rank to remove…", options=options[:25], row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=f"{self.game.name} · Remove Rank",
            description=(
                "Removing a rung stops the bot granting that role and stops it "
                "seeding MMR. Players keep the MMR they already have."
            ),
            color=COLOR_WARNING,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        await DatabaseHelper.remove_mmr_role(self.game.game_id, int(self.select.values[0]))
        await self.parent_page.render(interaction, flash="✅ Rank removed.")


class _EditTierModal(discord.ui.Modal, title="Tier"):
    """Add or edit a staged tier (label + MMR). Edits the in-memory staged ladder
    on the LadderRemapPage — nothing is written to the DB until Apply.

    Rank roles are off in this deployment, so a tier is just a label + an MMR
    floor: a brand-new tier gets a synthetic negative id as its ladder key (never
    a real Discord role), and existing tiers keep whatever id they already have.
    """

    tier_label = discord.ui.TextInput(label="Rank Label", placeholder="e.g., Emerald", required=True)
    mmr_value = discord.ui.TextInput(label="MMR Value", placeholder="e.g., 2900", required=True)

    def __init__(self, page: 'LadderRemapPage', role_id: Optional[int]):
        super().__init__()
        self.page = page
        self.role_id = role_id
        existing = page.staged.get(role_id) if role_id is not None else None
        if existing:
            self.tier_label.default = existing['label'] or ""
            self.mmr_value.default = str(existing['mmr'])
            self.title = f"Edit {existing['label'] or existing['mmr']}"[:45]
        else:
            self.title = "Add a new tier"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)
            return
        label = self.tier_label.value.strip() or None
        rid = self.role_id
        if rid is None:
            # Synthetic, always-negative key — real Discord snowflakes are positive,
            # so this can never collide with a role and is simply never granted.
            rid = -int(datetime.now().timestamp() * 1000)
        self.page.staged[rid] = {'mmr': mmr, 'label': label}
        self.page.preview = None  # staged changed — old preview is stale
        await self.page.render(
            interaction,
            flash=f"Staged **{label or mmr}** at **{mmr}** MMR. Hit **Preview** to see the impact.",
        )


class LadderRemapPage(SettingsPage):
    """Insert or re-space rank tiers, remapping every player onto the new ladder.

    Unlike *Add / Update Rank* (which edits one rung in place), this stages the
    whole ladder in memory so the *old* floors survive to build the remap, then —
    on Apply — backs up the DB, writes the new floors and slides every player's
    rating onto them so earned progress rides along. That's what lets a new tier
    (e.g. Emerald between Platinum and Diamond) be inserted without wiping or
    distorting anyone's rating. Preview is a dry run; nothing is written until
    Apply is confirmed. See ``cog.remap_ladder`` / ``cog._preview_ladder_remap``.
    """

    def __init__(self, cog: 'CustomMatch', parent: RankLadderPage, game: GameConfig,
                 live: Dict[int, dict]):
        super().__init__(cog, parent, timeout=600)
        self.game = game
        self.live = {rid: {'mmr': int(d['mmr']), 'label': d.get('label')} for rid, d in live.items()}
        self.staged = {rid: {'mmr': int(d['mmr']), 'label': d.get('label')} for rid, d in live.items()}
        self.preview: Optional[dict] = None
        self._edit_select: Optional[discord.ui.Select] = None
        self._remove_select: Optional[discord.ui.Select] = None
        self._build_items()

    def _build_items(self):
        self.clear_items()
        edit_opts = [discord.SelectOption(
            label="➕ Add a new tier", value="__add__",
            description="Insert a tier (e.g. Emerald) — label + MMR floor",
        )]
        for rid, d in sorted(self.staged.items(), key=lambda x: x[1]['mmr'], reverse=True):
            label = d['label'] or f"{d['mmr']} MMR"
            edit_opts.append(discord.SelectOption(
                label=f"{label} — {d['mmr']} MMR"[:100], value=str(rid),
                description="Edit this tier's label or MMR floor",
            ))
        edit = discord.ui.Select(placeholder="Add or edit a tier…", options=edit_opts[:25], row=0)
        edit.callback = self._on_edit
        self._edit_select = edit
        self.add_item(edit)

        opts = []
        for rid, d in sorted(self.staged.items(), key=lambda x: x[1]['mmr'], reverse=True):
            label = d['label'] or f"{d['mmr']} MMR"
            opts.append(discord.SelectOption(label=f"{label} — {d['mmr']} MMR"[:100], value=str(rid)))
        if opts:
            rem = discord.ui.Select(placeholder="Remove a staged tier…", options=opts[:25], row=1)
            rem.callback = self._on_remove
            self._remove_select = rem
            self.add_item(rem)

        prev = discord.ui.Button(label="Preview", style=discord.ButtonStyle.primary, row=2)
        prev.callback = self._on_preview
        self.add_item(prev)
        apply = discord.ui.Button(label="Apply", style=discord.ButtonStyle.success, row=2,
                                  disabled=self.preview is None)
        apply.callback = self._on_apply
        self.add_item(apply)
        reset = discord.ui.Button(label="Reset", style=discord.ButtonStyle.secondary, row=2)
        reset.callback = self._on_reset
        self.add_item(reset)

        if self.parent_page is not None:
            self.add_item(BackButton(self.parent_page, row=self.back_row))

    async def render(self, interaction: discord.Interaction, *, flash: Optional[str] = None):
        # Own render (not SettingsPage's): rebuild items each time so the staged
        # ladder + Apply-enabled state stay in sync, and don't blanket-enable
        # buttons the way the base does (that would un-gate Apply before Preview).
        self._build_items()
        embed = await self.build_embed(interaction.guild)
        if flash:
            embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)
        self.message = interaction.message

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        lines = []
        for rid, d in sorted(self.staged.items(), key=lambda x: x[1]['mmr'], reverse=True):
            label = d['label'] or f"{d['mmr']} MMR"
            tag = ""
            if rid not in self.live:
                tag = "  🆕 *inserted*"
            elif self.live[rid]['mmr'] != d['mmr']:
                tag = f"  ↕ *was {self.live[rid]['mmr']}*"
            lines.append(f"`{d['mmr']:>5}` **{label}**{tag}")
        for rid, d in self.live.items():
            if rid not in self.staged:
                label = d['label'] or f"{d['mmr']} MMR"
                lines.append(f"~~`{d['mmr']:>5}` **{label}**~~  🗑️ *removed*")

        desc = (
            "**Staged ladder** (not saved yet). Use the pickers to insert a tier or "
            "restage a floor, then **Preview** the impact and **Apply**. Applying "
            "backs up the database, writes the new floors and remaps every player "
            "onto them — earned rating is preserved, only the ladder geometry moves."
            "\n\n" + "\n".join(lines)
        )

        if self.preview is not None:
            pv = self.preview
            trans = "\n".join(
                f"• {a} → **{b}**: {n}"
                for (a, b), n in sorted(pv["label_changes"].items(), key=lambda kv: -kv[1])
            ) or "• none"
            sample = "\n".join(pv["samples"]) if pv["samples"] else "_no rank-label changes_"
            desc += (
                f"\n\n**Preview** — {pv['players']} player(s): "
                f"**{pv['mmr_changed']}** rating(s) remapped, "
                f"**{pv['label_changed_total']}** rank label(s) change.\n"
                f"__Label moves__\n{trans}\n\n__Sample__\n{sample}\n\n"
                "-# Apply is now enabled. Re-preview after any further edit."
            )
        else:
            desc += "\n\n-# Preview to enable Apply."

        return discord.Embed(
            title=f"{self.game.name} · Insert / Re-space Tier",
            description=desc[:4000],
            color=COLOR_NEUTRAL,
        )

    def _order_error(self) -> Optional[str]:
        floors = [d['mmr'] for d in self.staged.values()]
        if len(self.staged) < 2:
            return "Keep at least two tiers on the ladder."
        if len(set(floors)) != len(floors):
            return "Two tiers share an MMR floor — give each a distinct value."
        return None

    async def _on_edit(self, interaction: discord.Interaction):
        val = self._edit_select.values[0]
        role_id = None if val == "__add__" else int(val)
        await interaction.response.send_modal(_EditTierModal(self, role_id))

    async def _on_remove(self, interaction: discord.Interaction):
        rid = int(self._remove_select.values[0])
        self.staged.pop(rid, None)
        self.preview = None
        await self.render(interaction, flash="Tier unstaged. Preview to see the impact.")

    async def _on_reset(self, interaction: discord.Interaction):
        self.staged = {rid: dict(d) for rid, d in self.live.items()}
        self.preview = None
        await self.render(interaction, flash="Reset to the live ladder.")

    async def _on_preview(self, interaction: discord.Interaction):
        err = self._order_error()
        if err:
            await interaction.response.send_message(err, ephemeral=True)
            return
        self.preview = await self.cog._preview_ladder_remap(self.game, self.staged)
        await self.render(interaction)

    async def _on_apply(self, interaction: discord.Interaction):
        if self.preview is None:
            await interaction.response.send_message("Run **Preview** first.", ephemeral=True)
            return
        err = self._order_error()
        if err:
            await interaction.response.send_message(err, ephemeral=True)
            return
        await interaction.response.defer()
        try:
            summary = await self.cog.remap_ladder(interaction.guild, self.game, self.staged)
        except Exception as e:
            logger.error(f"ladder remap apply failed: {e}", exc_info=True)
            await interaction.followup.send(f"Remap failed: {e}", ephemeral=True)
            return
        self.live = {rid: dict(d) for rid, d in self.staged.items()}
        self.preview = None
        backup_name = str(summary.get("backup", "")).rpartition("/")[2]
        role_note = (f", **{summary['roles_applied']}** role(s) reapplied"
                     if summary.get("granting") else "")
        flash = (
            f"✅ Ladder re-spaced. **{summary['mmr_changed']}** rating(s) remapped"
            f"{role_note}. Backup `{backup_name}`."
        )
        self._build_items()
        embed = await self.build_embed(interaction.guild)
        embed.description = f"{flash}\n\n{embed.description or ''}".strip()
        await interaction.edit_original_response(embed=embed, view=self)


class DeleteGamePage(SettingsPage):
    """Deleting a game is unrecoverable, so it gets its own confirmation screen."""

    def __init__(self, cog: 'CustomMatch', parent: GameSettingsPage, game: GameConfig):
        super().__init__(cog, parent, timeout=120)
        self.game = game

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=f"Delete {self.game.name}?",
            description=(
                f"This removes **{self.game.name}** and its configuration — queue "
                "channel, toggles, schedule and rank ladder. **This cannot be undone.**"
            ),
            color=COLOR_WARNING,
        )

    @discord.ui.button(label="Delete permanently", style=discord.ButtonStyle.danger, row=0)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        name = self.game.name
        await DatabaseHelper.delete_game(self.game.game_id)
        # The game page underneath is now pointing at nothing — go up to the roster.
        games_page = self.parent_page.parent_page
        await games_page.render(interaction, flash=f"✅ Removed **{name}**.")


# -----------------------------------------------------------------------------
# Channels
# -----------------------------------------------------------------------------

_TEXTISH = [discord.ChannelType.text]
_THREADISH = [
    discord.ChannelType.text,
    discord.ChannelType.public_thread,
    discord.ChannelType.private_thread,
]

# One table replaces six near-identical select views. `field` marks a per-game
# target (a games column); `config_key` marks a server-wide one.
CHANNEL_TARGETS: Dict[str, dict] = {
    "queue": {
        "label": "Queue channel",
        "emoji": "🎮",
        "field": "queue_channel_id",
        "types": _TEXTISH,
        "blurb": "Where this game's queue embed lives and players join from.",
    },
    "results": {
        "label": "Results channel",
        "emoji": "🏆",
        "field": "game_channel_id",
        "types": _TEXTISH,
        "blurb": "Finished match results for this game are posted here.",
    },
    "lf1": {
        "label": "LF1 channel",
        "emoji": "🔔",
        "field": "lf1_channel_id",
        "types": _THREADISH,
        "blurb": (
            "\"Looking for 1\" pings when this game's queue needs one more player. "
            "30-minute cooldown per game; the ping auto-deletes after 20 minutes."
        ),
    },
    "log": {
        "label": "Log channel",
        "emoji": "📜",
        "config_key": "log_channel_id",
        "types": _TEXTISH,
        "blurb": "Queue events, match results and admin actions are logged here.",
    },
    "admin": {
        "label": "Admin channel",
        "emoji": "🛎️",
        "config_key": "cm_admin_channel_id",
        "types": _TEXTISH,
        "blurb": "Stats-fetch failures and anything else needing an admin lands here.",
    },
    "category": {
        "label": "Fallback match category",
        "emoji": "🗂️",
        "config_key": "category_id",
        "types": [discord.ChannelType.category],
        "blurb": (
            "Lobbies and VCs are normally created in the same category as the "
            "game's queue channel, directly under it. This fallback is only used "
            "when a queue channel isn't inside any category."
        ),
    },
    "lobby_archive": {
        "label": "Lobby archive category",
        "emoji": "🗄️",
        "config_key": "lobby_archive_category_id",
        "types": [discord.ChannelType.category],
        "blurb": (
            "Finished lobbies move here instead of being deleted — hidden from "
            "everyone except the CM Admin and mod roles, then deleted "
            "automatically after 48 hours. Leave this unset and the bot creates a "
            "private \"Match Archive\" category the first time it needs one."
        ),
    },
    "discussion": {
        "label": "Discussion parent",
        "emoji": "💬",
        "config_key": "cm_discussion_parent_channel_id",
        "types": _TEXTISH,
        "blurb": "Private discussion threads with suspended players are created here.",
    },
}


class ChannelTargetSelect(discord.ui.Select):
    """The single 'what am I routing?' dropdown on the Channels page."""

    def __init__(self, parent: 'ChannelsPage'):
        super().__init__(
            placeholder="Choose a channel to set…",
            options=[
                discord.SelectOption(
                    label=spec["label"],
                    value=key,
                    emoji=spec["emoji"],
                    description=("Per game · " if "field" in spec else "Server-wide · ")
                    + spec["blurb"].split(".")[0][:80],
                )
                for key, spec in CHANNEL_TARGETS.items()
            ],
            row=0,
        )
        self.parent_page = parent

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        spec = CHANNEL_TARGETS[key]
        if "field" not in spec:
            await ChannelTargetPage(self.parent_page.cog, self.parent_page, key).render(interaction)
            return
        games = await _require_games(interaction)
        if not games:
            return
        if len(games) == 1:
            await ChannelTargetPage(
                self.parent_page.cog, self.parent_page, key, games[0]
            ).render(interaction)
            return

        async def on_pick(pick_interaction: discord.Interaction, game: GameConfig):
            await ChannelTargetPage(
                self.parent_page.cog, self.parent_page, key, game
            ).render(pick_interaction)

        await GamePickerPage(
            self.parent_page.cog, self.parent_page, games, on_pick,
            title=f"{spec['label']} · Which game?",
            blurb=f"{spec['blurb']}\n\nThis is set per game.",
        ).render(interaction)


class ChannelsPage(SettingsPage):
    """Every channel the system writes to, server-wide and per game, in one place."""

    def __init__(self, cog: 'CustomMatch', parent: SettingsPage):
        super().__init__(cog, parent, timeout=300)
        self.add_item(ChannelTargetSelect(self))

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(
            title="Channels",
            description="Pick a destination below to point it somewhere new.",
            color=COLOR_NEUTRAL,
        )
        server_lines = []
        for key, spec in CHANNEL_TARGETS.items():
            if "config_key" not in spec:
                continue
            value = await DatabaseHelper.get_config(spec["config_key"])
            server_lines.append(f"{spec['emoji']} {spec['label']} · {_channel_label(guild, value)}")
            if key == "lobby_archive":
                held = len(await DatabaseHelper.get_archived_lobbies())
                server_lines.append(
                    f"⤷ holding **{held}** lobb{'y' if held == 1 else 'ies'} for mod review"
                )
        embed.add_field(name="Server-wide", value="\n".join(server_lines), inline=False)

        games = await DatabaseHelper.get_all_games()
        for game in games:
            embed.add_field(
                name=game.name,
                value=(
                    f"🎮 Queue · {_channel_label(guild, game.queue_channel_id)}\n"
                    f"🏆 Results · {_channel_label(guild, game.game_channel_id)}\n"
                    f"🔔 LF1 · {_channel_label(guild, game.lf1_channel_id)}"
                ),
                inline=True,
            )
        if not games:
            embed.add_field(
                name="Per game",
                value="No games configured yet.",
                inline=False,
            )
        return embed


class ChannelTargetPage(SettingsPage):
    """Set (or clear) one routing target, server-wide or for one game."""

    def __init__(self, cog: 'CustomMatch', parent: ChannelsPage, key: str,
                 game: Optional[GameConfig] = None):
        super().__init__(cog, parent, timeout=180)
        self.key = key
        self.spec = CHANNEL_TARGETS[key]
        self.game = game
        self.select = discord.ui.ChannelSelect(
            placeholder=f"Pick a channel for {self.spec['label'].lower()}…",
            channel_types=self.spec["types"],
            row=0,
        )
        self.select.callback = self._on_pick
        self.add_item(self.select)

    def _title(self) -> str:
        if self.game is not None:
            return f"{self.game.name} · {self.spec['label']}"
        return self.spec["label"]

    async def _current(self):
        if self.game is not None:
            game = await DatabaseHelper.get_game(self.game.game_id) or self.game
            return getattr(game, self.spec["field"])
        return await DatabaseHelper.get_config(self.spec["config_key"])

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title=self._title(),
            description=(
                f"Currently · {_channel_label(guild, await self._current())}\n\n"
                f"{self.spec['blurb']}"
            ),
            color=COLOR_NEUTRAL,
        )

    async def _save(self, value):
        if self.game is not None:
            await DatabaseHelper.update_game(self.game.game_id, **{self.spec["field"]: value})
        else:
            # No delete_config helper — an empty string reads as unset everywhere,
            # since every consumer guards on truthiness before int()-ing it.
            await DatabaseHelper.set_config(self.spec["config_key"], str(value) if value else "")

    async def _on_pick(self, interaction: discord.Interaction):
        channel = self.select.values[0]
        await self._save(channel.id)
        await self.render(interaction, flash=f"✅ {self.spec['label']} set to {channel.mention}.")

    @discord.ui.button(label="Clear", style=discord.ButtonStyle.danger, row=1)
    async def clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._save(None)
        await self.render(interaction, flash=f"✅ {self.spec['label']} cleared.")


# -----------------------------------------------------------------------------
# Access
# -----------------------------------------------------------------------------

class AccessPage(SettingsPage):
    """Who administers the system, who moderates it, and who's locked out."""

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        admin_role_id = await DatabaseHelper.get_config("cm_admin_role_id")
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        now = datetime.now(timezone.utc)
        active = [entry for entry in blacklisted if entry[1] > now]

        if mod_role_ids:
            mods = "\n".join(_role_label(guild, rid) for rid in mod_role_ids)
        else:
            mods = "`none`"

        embed = discord.Embed(title="Access", color=COLOR_NEUTRAL)
        embed.add_field(
            name="🛡️ CM Admin role",
            value=(
                f"{_role_label(guild, admin_role_id)}\n"
                "*Full access to this panel and the admin panel.*"
            ),
            inline=False,
        )
        embed.add_field(
            name="🔧 Mod roles",
            value=(
                f"{mods}\n*Can speak and manage messages in every match channel, "
                "and read archived lobbies after the match.*"
            ),
            inline=False,
        )
        embed.add_field(
            name="⛔ Blacklist",
            value=(
                f"**{len(active)}** player{'' if len(active) == 1 else 's'} currently blocked "
                "from queueing."
            ),
            inline=False,
        )
        return embed

    @discord.ui.button(label="CM Admin Role", style=discord.ButtonStyle.primary, row=0)
    async def admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        await AdminRolePage(self.cog, self).render(interaction)

    @discord.ui.button(label="Mod Roles", style=discord.ButtonStyle.primary, row=0)
    async def mod_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ModRolesPage(self.cog, self).render(interaction)

    @discord.ui.button(label="Blacklist", style=discord.ButtonStyle.danger, row=0)
    async def blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        await BlacklistPage(self.cog, self).render(interaction)


class AdminRolePage(SettingsPage):
    """The single role that unlocks the admin surface."""

    def __init__(self, cog: 'CustomMatch', parent: AccessPage):
        super().__init__(cog, parent, timeout=180)
        self.select = discord.ui.RoleSelect(placeholder="Pick the CM Admin role…", row=0)
        self.select.callback = self._on_pick
        self.add_item(self.select)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        current = await DatabaseHelper.get_config("cm_admin_role_id")
        return discord.Embed(
            title="CM Admin Role",
            description=(
                f"Currently · {_role_label(guild, current)}\n\n"
                "Holders get this settings panel, `/cm_panel`, and every admin "
                "control inside a match. Server administrators always have access "
                "regardless of this role."
            ),
            color=COLOR_NEUTRAL,
        )

    async def _on_pick(self, interaction: discord.Interaction):
        role = self.select.values[0]
        await DatabaseHelper.set_config("cm_admin_role_id", str(role.id))
        await self.render(interaction, flash=f"✅ CM Admin role set to **{role.name}**.")


class ModRolesPage(SettingsPage):
    """Roles granted speaking rights inside match channels."""

    def __init__(self, cog: 'CustomMatch', parent: AccessPage):
        super().__init__(cog, parent, timeout=300)
        self.add_select = discord.ui.RoleSelect(placeholder="Add a mod role…", row=0)
        self.add_select.callback = self._on_add
        self.add_item(self.add_select)
        self.remove_select: Optional[discord.ui.Select] = None

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        role_ids = await DatabaseHelper.get_mod_roles()

        if self.remove_select is not None:
            self.remove_item(self.remove_select)
            self.remove_select = None
        if role_ids:
            options = []
            for role_id in role_ids[:25]:
                role = guild.get_role(role_id)
                options.append(discord.SelectOption(
                    label=(role.name if role else f"Unknown ({role_id})")[:100],
                    value=str(role_id),
                ))
            self.remove_select = discord.ui.Select(
                placeholder="Remove a mod role…", options=options, row=1
            )
            self.remove_select.callback = self._on_remove
            self.add_item(self.remove_select)

        body = "\n".join(_role_label(guild, rid) for rid in role_ids) if role_ids else "`none`"
        return discord.Embed(
            title="Mod Roles",
            description=(
                "These roles can type in and manage messages in every match "
                "channel the bot creates, and they are the only ones who keep "
                "read access once a finished lobby is archived.\n\n"
                f"{body}"
            ),
            color=COLOR_NEUTRAL,
        )

    async def _on_add(self, interaction: discord.Interaction):
        role = self.add_select.values[0]
        await DatabaseHelper.add_mod_role(role.id)
        await self.render(interaction, flash=f"✅ **{role.name}** added as a mod role.")

    async def _on_remove(self, interaction: discord.Interaction):
        role_id = int(self.remove_select.values[0])
        await DatabaseHelper.remove_mod_role(role_id)
        role = interaction.guild.get_role(role_id)
        name = role.name if role else role_id
        await self.render(interaction, flash=f"✅ **{name}** removed as a mod role.")


class BlacklistPage(SettingsPage):
    """Players barred from queueing, and the controls to change that."""

    def __init__(self, cog: 'CustomMatch', parent: AccessPage):
        super().__init__(cog, parent, timeout=300)
        self.add_select = discord.ui.UserSelect(placeholder="Blacklist a player…", row=0)
        self.add_select.callback = self._on_add
        self.add_item(self.add_select)
        self.remove_select: Optional[discord.ui.Select] = None

    async def _active(self, guild: discord.Guild):
        now = datetime.now(timezone.utc)
        return [(pid, until) for pid, until in await DatabaseHelper.get_blacklisted_players()
                if until > now]

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        active = await self._active(guild)

        if self.remove_select is not None:
            self.remove_item(self.remove_select)
            self.remove_select = None
        if active:
            options = []
            for player_id, until in active[:25]:
                member = guild.get_member(player_id)
                name = member.display_name if member else str(player_id)
                expiry = "Permanent" if until.year == 2099 else f"Until {until:%Y-%m-%d}"
                options.append(discord.SelectOption(
                    label=name[:100], value=str(player_id), description=expiry
                ))
            self.remove_select = discord.ui.Select(
                placeholder="Lift a blacklist…", options=options, row=1
            )
            self.remove_select.callback = self._on_remove
            self.add_item(self.remove_select)

        if active:
            lines = []
            for player_id, until in active[:20]:
                member = guild.get_member(player_id)
                name = member.mention if member else f"<@{player_id}>"
                when = "permanent" if until.year == 2099 else f"until {until:%Y-%m-%d %H:%M} UTC"
                lines.append(f"• {name} — {when}")
            if len(active) > 20:
                lines.append(f"*…and {len(active) - 20} more*")
            body = "\n".join(lines)
        else:
            body = "Nobody is blacklisted."

        return discord.Embed(
            title="Blacklist",
            description=f"Blacklisted players can't join any queue.\n\n{body}",
            color=COLOR_WARNING if active else COLOR_NEUTRAL,
        )

    async def _on_add(self, interaction: discord.Interaction):
        user = self.add_select.values[0]
        await interaction.response.send_modal(
            BlacklistDurationModal(self.cog, user.id, user.display_name, self)
        )

    async def _on_remove(self, interaction: discord.Interaction):
        player_id = int(self.remove_select.values[0])
        await DatabaseHelper.unblacklist_player(player_id)
        member = interaction.guild.get_member(player_id)
        name = member.display_name if member else player_id
        await self.render(interaction, flash=f"✅ **{name}** un-blacklisted.")


# -----------------------------------------------------------------------------
# Stats & data
# -----------------------------------------------------------------------------

class DataPage(SettingsPage):
    """Stat ingestion and the one button that throws stats away."""

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(
            title="Stats & Data",
            description="Where match stats come from, and how to repair them.",
            color=COLOR_NEUTRAL,
        )
        embed.add_field(
            name="🦸 Rivals stats",
            value="Screenshot OCR review channel, upload blacklist, per-match corrections.",
            inline=False,
        )
        embed.add_field(
            name="🎯 Valorant stats",
            value=(
                "**Fetch Match** pulls stats for one match by ID.\n"
                "**Refetch** sweeps recent matches — incomplete only, or force all."
            ),
            inline=False,
        )
        embed.add_field(
            name="🧨 Wipe stats",
            value="Resets every player's record. MMR survives; nothing else does.",
            inline=False,
        )
        return embed

    @discord.ui.button(label="Rivals Stats", style=discord.ButtonStyle.primary, row=0)
    async def rivals(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RivalsSettingsView(self.cog)
        await self.show_view(interaction, view, await view.build_embed(interaction.guild))

    @discord.ui.button(label="Fetch Match", style=discord.ButtonStyle.primary, row=0)
    async def fetch_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        from .views_gameplay import FetchStatsModal
        await interaction.response.send_modal(FetchStatsModal(self.cog))

    @discord.ui.button(label="Refetch", style=discord.ButtonStyle.primary, row=0)
    async def refetch(self, interaction: discord.Interaction, button: discord.ui.Button):
        from .views_gameplay import RefetchModeSelectView
        await self.open_flow(
            interaction, RefetchModeSelectView(self.cog),
            content=(
                "**Refetch Valorant stats**\n"
                "• **Incomplete Only** — just the matches missing player stats\n"
                "• **Force All** — re-fetch every recent match, overwriting what's stored"
            ),
        )

    @discord.ui.button(label="Wipe Stats", style=discord.ButtonStyle.danger, row=1)
    async def wipe(self, interaction: discord.Interaction, button: discord.ui.Button):
        await WipeStatsPage(self.cog, self).render(interaction)


# -----------------------------------------------------------------------------
# Maintenance
# -----------------------------------------------------------------------------

class MaintenancePage(SettingsPage):
    """Repair tools — the things you reach for when something is already wrong."""

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(
            title="Maintenance",
            description="Repair tools for when the system and reality disagree.",
            color=COLOR_NEUTRAL,
        )
        embed.add_field(
            name="🔧 Fix Match",
            value="Re-open, re-decide or unstick a specific match by ID.",
            inline=False,
        )
        embed.add_field(
            name="🧹 Cleanup Stale Matches",
            value="Closes matches whose channels or roles no longer exist.",
            inline=False,
        )
        embed.add_field(
            name="👥 Mass Register",
            value=(
                "Scans every member and seeds MMR from their rank role for one "
                "game. Safe to re-run — it also repairs missing per-role ratings."
            ),
            inline=False,
        )
        return embed

    @discord.ui.button(label="Fix Match", style=discord.ButtonStyle.primary, row=0)
    async def fix_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        from .views_gameplay import FixMatchModal
        await interaction.response.send_modal(FixMatchModal(self.cog))

    @discord.ui.button(label="Cleanup Stale Matches", style=discord.ButtonStyle.secondary, row=0)
    async def cleanup(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        count, details = await self.cog.manual_orphan_cleanup(interaction.guild)
        if count == 0:
            await interaction.followup.send("No stale matches found.", ephemeral=True)
            return
        text = "\n".join(details[:10])
        if len(details) > 10:
            text += f"\n… and {len(details) - 10} more"
        await interaction.followup.send(f"**Cleaned up {count} stale matches:**\n{text}",
                                        ephemeral=True)

    @discord.ui.button(label="Mass Register", style=discord.ButtonStyle.success, row=0)
    async def mass_register(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await _require_games(interaction)
        if not games:
            return
        await GamePickerPage(
            self.cog, self, games, self._do_mass_register,
            title="Mass Register · Which game?",
            blurb=(
                "Scans every server member and seeds MMR from their rank role for "
                "the chosen game. Players who already have the right MMR are left "
                "alone, but their per-role ratings are repaired either way."
            ),
        ).render(interaction)

    async def _do_mass_register(self, interaction: discord.Interaction, game: GameConfig):
        await interaction.response.defer(ephemeral=True)

        role_mmr_map = await DatabaseHelper.get_mmr_roles(game.game_id)
        if not role_mmr_map:
            await interaction.followup.send(
                f"No rank ladder configured for **{game.name}**. "
                f"Set one up under **Games → {game.name} → Rank Ladder** first.",
                ephemeral=True,
            )
            return

        registered = corrected = skipped = 0
        errors: List[str] = []
        corrections: List[str] = []

        for member in interaction.guild.members:
            if member.bot:
                continue

            # Highest rank role wins, so a player holding several isn't
            # seeded off whichever one Discord happens to list first.
            member_mmr = None
            for role in member.roles:
                if role.id in role_mmr_map:
                    role_mmr = role_mmr_map[role.id]
                    if member_mmr is None or role_mmr > member_mmr:
                        member_mmr = role_mmr
            if member_mmr is None:
                continue

            try:
                stats = await DatabaseHelper.get_player_stats(member.id, game.game_id)
                if stats.mmr != member_mmr and stats.games_played == 0:
                    stats.mmr = member_mmr
                    await DatabaseHelper.update_player_stats(stats)
                    registered += 1
                elif stats.mmr != member_mmr and stats.games_played > 0:
                    old_mmr = stats.mmr
                    stats.mmr = member_mmr
                    await DatabaseHelper.update_player_stats(stats)
                    corrected += 1
                    corrections.append(f"{member.display_name}: {old_mmr} → {member_mmr}")
                else:
                    skipped += 1
                # Every branch, including "already correct": a player whose
                # aggregate MMR is right can still have no per-role rows,
                # which is exactly what mass registration exists to repair.
                await DatabaseHelper.sync_ow_role_seed(
                    member.id, game.game_id, stats.effective_mmr, game
                )
            except Exception as e:
                errors.append(f"{member.display_name}: {e}")

        result = f"**Mass registration complete — {game.name}**\n"
        result += f"Registered: {registered}\n"
        if corrected:
            result += f"Corrected: {corrected}\n"
        result += f"Already correct: {skipped}\n"
        if corrections:
            result += f"\nMMR corrections ({len(corrections)}):\n" + "\n".join(corrections[:10])
            if len(corrections) > 10:
                result += f"\n… and {len(corrections) - 10} more"
        if errors:
            result += f"\nErrors ({len(errors)}):\n" + "\n".join(errors[:5])
            if len(errors) > 5:
                result += f"\n… and {len(errors) - 5} more"

        await interaction.followup.send(result, ephemeral=True)


def build_schedule_embed(game: GameConfig) -> discord.Embed:
    """Human-readable summary of a game's per-day queue schedule."""
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    embed = discord.Embed(title=f"{game.name} · Queue Schedule", color=COLOR_NEUTRAL)
    embed.add_field(name="Enabled", value="Yes" if game.schedule_enabled else "No", inline=False)

    if game.schedule_times:
        lines = []
        for day_num, times in sorted(game.schedule_times.items(), key=lambda x: int(x[0])):
            day_name = day_names[int(day_num)]
            mode = times.get("mode")
            if mode == "open":
                lines.append(f"**{day_name}:** Open all day")
            elif mode == "closed":
                lines.append(f"**{day_name}:** Closed all day")
            else:
                open_time, close_time = times.get("open"), times.get("close")
                if open_time and close_time:
                    lines.append(f"**{day_name}:** {open_time} - {close_time}")
                elif open_time:
                    lines.append(f"**{day_name}:** Opens at {open_time} →")
                elif close_time:
                    lines.append(f"**{day_name}:** → Closes at {close_time}")
        embed.add_field(name="Schedule", value="\n".join(lines) or "No days configured",
                        inline=False)
    elif game.schedule_open_days:
        days = game.schedule_open_days.split(",")
        open_days_str = ", ".join(day_names[int(d)] for d in days if d.isdigit())
        embed.add_field(name="Open Days", value=open_days_str, inline=True)
        embed.add_field(name="Open Time", value=game.schedule_open_time or "Not set", inline=True)
        embed.add_field(name="Close Time", value=game.schedule_close_time or "Not set", inline=True)
    else:
        embed.add_field(name="Schedule", value="Not configured", inline=False)

    embed.set_footer(text="Times are in server timezone (bot host time)")
    return embed

class WipeStatsPage(SettingsPage):
    """The one irreversible button in the panel, on a screen of its own."""

    back_row = 1

    def __init__(self, cog: 'CustomMatch', parent: 'DataPage'):
        super().__init__(cog, parent, timeout=120)

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        return discord.Embed(
            title="⚠️ Wipe all stats",
            description=(
                "Resets the following for **every** player:\n"
                "• Wins, losses, games played\n"
                "• Valorant match stats (K/D/A, damage, …)\n"
                "• Rivalries and teammate stats\n"
                "• Completed and cancelled match history\n\n"
                "**MMR is preserved.** Nothing else here is recoverable."
            ),
            color=COLOR_WARNING,
        )

    @discord.ui.button(label="Yes, wipe stats", style=discord.ButtonStyle.danger, row=0)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            # Wipe stats but preserve MMR
            async with DatabaseHelper._get_db() as db:
                # Reset player_stats (keep MMR)
                await db.execute("""
                    UPDATE player_game_stats
                    SET wins = 0, losses = 0, games_played = 0, last_played = NULL, returning_games_remaining = 0
                """)

                # Clear valorant match stats
                await db.execute("DELETE FROM valorant_match_stats")

                # Clear match players for completed/cancelled matches
                await db.execute("""
                    DELETE FROM match_players WHERE match_id IN (
                        SELECT match_id FROM matches WHERE winning_team IS NOT NULL OR cancelled = 1
                    )
                """)

                # Clear completed/cancelled matches (keeps active matches intact)
                await db.execute("DELETE FROM matches WHERE winning_team IS NOT NULL OR cancelled = 1")

                # Clear MMR history
                await db.execute("DELETE FROM mmr_history")

                # Clear rivalries
                await db.execute("DELETE FROM rivalries")

                # Clear win votes and abandon votes
                await db.execute("DELETE FROM win_votes")
                await db.execute("DELETE FROM abandon_votes")

                # Clear valorant regulars
                await db.execute("DELETE FROM valorant_player_regulars")

                # Clear admin stat adjustments — must be wiped with stats or they re-apply
                # on next restart via reconcile_player_stats(), corrupting the fresh season
                await db.execute("DELETE FROM admin_stat_adjustments")

                # Clear stale stats retry rows (completed/exhausted) — pending rows for
                # active matches are intentionally left so in-flight fetches can finish
                await db.execute(
                    "DELETE FROM valorant_stats_retry WHERE status IN ('success', 'exhausted')"
                )

                await db.commit()

            await self.cog.log_action(
                interaction.guild,
                f"Stats wiped by {interaction.user.display_name} (MMR preserved)"
            )

            embed = await self.parent_page.build_embed(interaction.guild)
            embed.description = (
                "✅ **Stats wiped.** Wins, losses and match data are reset; "
                f"MMR was preserved.\n\n{embed.description or ''}"
            ).strip()
            await interaction.edit_original_response(embed=embed, view=self.parent_page)
            self.parent_page.message = interaction.message
        except Exception as e:
            logger.error(f"Stats wipe error: {e}")
            embed = await self.build_embed(interaction.guild)
            embed.description = f"❌ Error wiping stats: {e}\n\n{embed.description}"
            await interaction.edit_original_response(embed=embed, view=self)


class EmojisPage(SettingsPage):
    """Emoji configuration, grouped under Games since that's what it dresses up."""

    async def build_embed(self, guild: discord.Guild) -> discord.Embed:
        games = await DatabaseHelper.get_all_games()
        emojis = await DatabaseHelper.get_role_emojis()
        resolved = _resolve_role_emojis(emojis, self.cog.bot)

        embed = discord.Embed(
            title="Emojis",
            description="How the queue and ready-check render per game, plus the shared Rivals role icons.",
            color=COLOR_NEUTRAL,
        )
        if games:
            embed.add_field(
                name="Ready emojis (per game)",
                value="\n".join(
                    f"**{g.name}** · {g.ready_loading_emoji} waiting → {g.ready_done_emoji} ready"
                    for g in games
                ),
                inline=False,
            )
        embed.add_field(
            name="Rivals role emojis",
            value="\n".join(
                f"{label} · {resolved.get(key) or '`not set`'}"
                for key, label in ROLE_EMOJI_CHOICES.items()
            ),
            inline=False,
        )
        return embed

    @discord.ui.button(label="Ready Emojis", style=discord.ButtonStyle.primary, row=0)
    async def ready_emojis_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await _require_games(interaction)
        if not games:
            return
        await GamePickerPage(
            self.cog, self, games, self._show_ready_emojis_flow,
            title="Ready Emojis · Which game?",
            blurb=(
                "You'll be asked to send the **waiting** emoji, then the **ready** "
                "emoji, as ordinary messages in this channel."
            ),
        ).render(interaction)

    @discord.ui.button(label="Role Emojis", style=discord.ButtonStyle.primary, row=0)
    async def role_emojis_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RoleEmojisView(self.cog)
        emojis = await DatabaseHelper.get_role_emojis()
        resolved = _resolve_role_emojis(emojis, interaction.client)
        embed = discord.Embed(
            title="Rivals Role Emojis",
            description="\n".join(
                f"• **{label}:** {resolved.get(key) or 'Not set'}"
                for key, label in ROLE_EMOJI_CHOICES.items()
            ),
            color=COLOR_NEUTRAL,
        )
        await self.show_view(interaction, view, embed, back_row=1)

    async def _show_ready_emojis_flow(self, interaction: discord.Interaction, game: GameConfig):
        # The emoji is typed as a normal message rather than picked from a
        # component: custom emoji from other servers can't be entered any other
        # way, and the bot only needs the raw string.
        await interaction.response.send_message(
            f"**Configure Ready Emojis for {game.name}**\n\n"
            f"Current Loading Emoji: {game.ready_loading_emoji}\n"
            f"Current Ready Emoji: {game.ready_done_emoji}\n\n"
            f"**Step 1:** Send the **loading** emoji (shown while waiting for players):",
            ephemeral=True
        )

        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.cog.bot.wait_for('message', timeout=60.0, check=check)
            loading_emoji = msg.content.strip()
            try:
                await msg.delete()
            except Exception:
                pass

            await interaction.edit_original_response(
                content=f"**Configure Ready Emojis for {game.name}**\n\n"
                f"Loading Emoji: {loading_emoji}\n\n"
                f"**Step 2:** Now send the **ready** emoji (shown when a player is ready):"
            )

            msg = await self.cog.bot.wait_for('message', timeout=60.0, check=check)
            done_emoji = msg.content.strip()
            try:
                await msg.delete()
            except Exception:
                pass

            await DatabaseHelper.update_game(
                game.game_id,
                ready_loading_emoji=loading_emoji,
                ready_done_emoji=done_emoji
            )

            await interaction.edit_original_response(
                content=f"**Ready emojis updated for {game.name}!**\n\n"
                f"Loading: {loading_emoji}\n"
                f"Ready: {done_emoji}"
            )

        except asyncio.TimeoutError:
            await interaction.edit_original_response(
                content="Emoji setup timed out. Please try again."
            )


def _resolve_role_emojis(emojis: dict, bot: discord.Client = None) -> dict:
    """Resolve stored emoji strings to actual emoji objects.

    Stored strings like '<:vanguard:123>' may have stale IDs. This resolves
    them by looking up the emoji by ID across ALL guilds the bot is in,
    falling back to name matching if the ID isn't found.
    """
    if not bot or not emojis:
        return emojis
    total_emojis = sum(len(g.emojis) for g in bot.guilds)
    logger.info(f"[RoleEmoji] Bot has {len(bot.guilds)} guilds, {total_emojis} total emojis cached")
    resolved = {}
    for key, emoji_str in emojis.items():
        match = re.search(r'<(a?):(\w+):(\d+)>', emoji_str)
        if match:
            animated, name, eid = match.group(1), match.group(2), int(match.group(3))
            # Try by ID across all guilds
            emoji_obj = bot.get_emoji(eid)
            logger.info(f"[RoleEmoji] Looking up {key}: id={eid}, name='{name}', found_by_id={emoji_obj is not None}")
            if emoji_obj:
                resolved[key] = str(emoji_obj)
                continue
            # ID not found — try by name across all guilds
            for g in bot.guilds:
                for e in g.emojis:
                    if e.name.lower() == name.lower():
                        resolved[key] = str(e)
                        break
                if key in resolved:
                    break
            else:
                resolved[key] = emoji_str
        else:
            resolved[key] = emoji_str
    return resolved


ROLE_EMOJI_CHOICES = {
    "vanguard": "Vanguard",
    "duelist": "Duelist",
    "strategist": "Strategist",
    "fill": "Fill / Flex",
    "none": "No Role",
}


class RoleEmojisView(ExpiringView):
    """View for managing Rivals role emojis (add/change, view, remove)."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add/Change", style=discord.ButtonStyle.success, row=0)
    async def add_change_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RoleEmojiSelectView(self.cog, action="set")
        await interaction.response.send_message(
            "Select which role to set an emoji for:", view=view, ephemeral=True
        )

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, row=0)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RoleEmojiSelectView(self.cog, action="remove")
        await interaction.response.send_message(
            "Select which role emoji to remove:", view=view, ephemeral=True
        )


class RoleEmojiSelectView(ExpiringView):
    """Dropdown to select which role to set/remove emoji for."""

    def __init__(self, cog: 'CustomMatch', action: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.action = action
        options = [
            discord.SelectOption(label=label, value=key)
            for key, label in ROLE_EMOJI_CHOICES.items()
        ]
        select = discord.ui.Select(placeholder="Select a role...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_key = interaction.data["values"][0]
        role_label = ROLE_EMOJI_CHOICES[role_key]

        if self.action == "remove":
            emojis = await DatabaseHelper.get_role_emojis()
            if role_key in emojis:
                del emojis[role_key]
                await DatabaseHelper.set_role_emojis(emojis)
                await interaction.response.send_message(
                    f"Removed emoji for **{role_label}**.", ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"No emoji set for **{role_label}**.", ephemeral=True
                )
            return

        # action == "set": prompt user to send an emoji
        await interaction.response.send_message(
            f"Send the emoji you want to use for **{role_label}**:", ephemeral=True
        )

        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.cog.bot.wait_for('message', timeout=60.0, check=check)
            emoji_str = msg.content.strip()
            logger.info(f"[RoleEmoji] Raw content: {repr(emoji_str)}")
            try:
                await msg.delete()
            except Exception:
                pass

            # Resolve :name: shortcodes to full custom emoji format
            shortcode_match = re.fullmatch(r':(\w+):', emoji_str)
            if shortcode_match:
                emoji_name = shortcode_match.group(1).lower()
                guild = interaction.guild
                if guild:
                    for e in guild.emojis:
                        if e.name.lower() == emoji_name:
                            emoji_str = str(e)
                            break
            logger.info(f"[RoleEmoji] Final stored: {repr(emoji_str)}")

            emojis = await DatabaseHelper.get_role_emojis()
            emojis[role_key] = emoji_str
            await DatabaseHelper.set_role_emojis(emojis)

            await interaction.edit_original_response(
                content=f"**{role_label}** emoji set to: {emoji_str}"
            )
        except asyncio.TimeoutError:
            await interaction.edit_original_response(
                content="Timed out. Please try again."
            )


class MMRValueModal(discord.ui.Modal, title="Set MMR Value"):
    mmr_value = discord.ui.TextInput(
        label="MMR Value",
        placeholder="e.g., 1500",
        required=True
    )
    rank_label = discord.ui.TextInput(
        label="Rank Label",
        placeholder="e.g., Gold, Platinum, Diamond",
        required=False
    )

    def __init__(self, cog: 'CustomMatch', game_id: int, role_id: int, role_name: str,
                 parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.role_id = role_id
        self.role_name = role_name
        self.parent_page = parent_page
        self.title = f"Set MMR for {role_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)
            return
        label = self.rank_label.value.strip() or None
        await DatabaseHelper.set_mmr_role(self.game_id, self.role_id, mmr, label)
        label_str = f" — **{label}**" if label else ""
        await _respond_to_modal(
            interaction, self.parent_page, f"**{self.role_name}** set to {mmr} MMR{label_str}."
        )


class BlacklistDurationModal(discord.ui.Modal, title="Blacklist Duration"):
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, user_name: str,
                 parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.user_name = user_name
        self.parent_page = parent_page
        self.title = f"Blacklist {user_name}"[:45]

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.duration.value)
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)
            return
        if days <= 0:
            await DatabaseHelper.blacklist_player(self.user_id)
            note = f"**{self.user_name}** blacklisted permanently."
        else:
            until = datetime.now(timezone.utc) + timedelta(days=days)
            await DatabaseHelper.blacklist_player(self.user_id, until)
            note = f"**{self.user_name}** blacklisted for {days} days."
        await _respond_to_modal(interaction, self.parent_page, note)


class GameTogglesView(ExpiringView):
    """View for toggling game settings."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game
        self.update_buttons()

    def build_embed(self) -> discord.Embed:
        state = (
            "**Game is ENABLED.**"
            if self.game.enabled else
            "**Game is DISABLED** — its data is safe, but it's hidden from setup "
            "and stat menus and its queue won't accept new players."
        )
        return discord.Embed(
            title=f"{self.game.name} · Toggles",
            description=(
                f"{state}\n\n"
                "Each button shows its current state — click to flip it. "
                "Changes take effect on open queues immediately."
            ),
            color=COLOR_NEUTRAL if self.game.enabled else COLOR_WARNING,
        )

    def update_buttons(self):
        self.clear_items()

        # Master on/off. Its own row so it reads as a state switch for the whole
        # game rather than one rule among many. OFF keeps all data but takes the
        # game off every operational surface and closes its queue.
        enabled_btn = discord.ui.Button(
            label=f"Game: {'ENABLED' if self.game.enabled else 'DISABLED'}",
            style=discord.ButtonStyle.success if self.game.enabled else discord.ButtonStyle.danger,
            row=0,
        )
        enabled_btn.callback = self.toggle_enabled
        self.add_item(enabled_btn)

        vc_btn = discord.ui.Button(
            label=f"VC Creation: {'ON' if self.game.vc_creation_enabled else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.vc_creation_enabled else discord.ButtonStyle.secondary,
            row=1,
        )
        vc_btn.callback = self.toggle_vc
        self.add_item(vc_btn)

        role_btn = discord.ui.Button(
            label=f"Role Required: {'ON' if self.game.queue_role_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.queue_role_required else discord.ButtonStyle.secondary,
            row=1,
        )
        role_btn.callback = self.toggle_role
        self.add_item(role_btn)

        dm_btn = discord.ui.Button(
            label=f"DM Ready: {'ON' if self.game.dm_ready_up else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.dm_ready_up else discord.ButtonStyle.secondary,
            row=1,
        )
        dm_btn.callback = self.toggle_dm
        self.add_item(dm_btn)

        ign_btn = discord.ui.Button(
            label=f"IGN Required: {'ON' if self.game.ign_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.ign_required else discord.ButtonStyle.secondary,
            row=1,
        )
        ign_btn.callback = self.toggle_ign_required
        self.add_item(ign_btn)

        role_req_btn = discord.ui.Button(
            label=f"Role Prefs Required: {'ON' if self.game.role_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.role_required else discord.ButtonStyle.secondary,
            row=1,
        )
        role_req_btn.callback = self.toggle_role_required
        self.add_item(role_req_btn)

        # PC players toggle + seed offset (crossplay games only; never Valorant)
        if not is_valorant_game(self.game):
            pc_btn = discord.ui.Button(
                label=f"PC Players: {'ON' if self.game.pc_enabled else 'OFF'}",
                style=discord.ButtonStyle.success if self.game.pc_enabled else discord.ButtonStyle.secondary,
                row=2
            )
            pc_btn.callback = self.toggle_pc_enabled
            self.add_item(pc_btn)

            if self.game.pc_enabled:
                offset_btn = discord.ui.Button(
                    label=f"PC Offset: +{self.game.pc_offset_tiers:g} tier",
                    style=discord.ButtonStyle.primary,
                    row=2
                )
                offset_btn.callback = self.set_pc_offset
                self.add_item(offset_btn)

        # Grace period button
        grace_btn = discord.ui.Button(
            label=f"Grace Period: {self.game.grace_period_minutes}min",
            style=discord.ButtonStyle.primary,
            row=2
        )
        grace_btn.callback = self.set_grace_period
        self.add_item(grace_btn)

        # Not Ready Cooldown button
        nr_cd_btn = discord.ui.Button(
            label=f"Not Ready CD: {self.game.not_ready_cooldown_minutes}min",
            style=discord.ButtonStyle.primary,
            row=2
        )
        nr_cd_btn.callback = self.set_not_ready_cooldown
        self.add_item(nr_cd_btn)

        # Verification topic button
        topic_label = f"Verify Topic: {self.game.verification_topic or 'None'}"
        if len(topic_label) > 80:
            topic_label = topic_label[:77] + "..."
        topic_btn = discord.ui.Button(
            label=topic_label,
            style=discord.ButtonStyle.primary if self.game.verification_topic else discord.ButtonStyle.secondary,
            row=2
        )
        topic_btn.callback = self.set_verification_topic
        self.add_item(topic_btn)

        # clear_items() above drops the settings panel's Back button along with
        # the toggles, so put it back on every rebuild.
        self._restore_back()

    async def set_verification_topic(self, interaction: discord.Interaction):
        modal = VerificationTopicModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def set_grace_period(self, interaction: discord.Interaction):
        modal = GracePeriodModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def toggle_pc_enabled(self, interaction: discord.Interaction):
        new_val = not self.game.pc_enabled
        await DatabaseHelper.update_game(self.game.game_id, pc_enabled=int(new_val))
        self.game.pc_enabled = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)

    async def set_pc_offset(self, interaction: discord.Interaction):
        modal = PCOffsetModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def set_not_ready_cooldown(self, interaction: discord.Interaction):
        modal = NotReadyCooldownModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def toggle_enabled(self, interaction: discord.Interaction):
        new_val = not self.game.enabled
        await DatabaseHelper.update_game(self.game.game_id, enabled=int(new_val))
        self.game.enabled = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        # Reflect the state change on any live queue message immediately.
        await self._refresh_queue_embeds(interaction.guild)
        await self.cog.log_action(
            interaction.guild,
            f"Game **{self.game.name}** {'enabled' if new_val else 'disabled'} "
            f"by {interaction.user.display_name}",
            prefix="🎮",
        )

    async def toggle_vc(self, interaction: discord.Interaction):
        new_val = not self.game.vc_creation_enabled
        await DatabaseHelper.update_game(self.game.game_id, vc_creation_enabled=int(new_val))
        self.game.vc_creation_enabled = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def toggle_role(self, interaction: discord.Interaction):
        new_val = not self.game.queue_role_required
        await DatabaseHelper.update_game(self.game.game_id, queue_role_required=int(new_val))
        self.game.queue_role_required = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def toggle_dm(self, interaction: discord.Interaction):
        new_val = not self.game.dm_ready_up
        await DatabaseHelper.update_game(self.game.game_id, dm_ready_up=int(new_val))
        self.game.dm_ready_up = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def toggle_ign_required(self, interaction: discord.Interaction):
        new_val = not self.game.ign_required
        await DatabaseHelper.update_game(self.game.game_id, ign_required=int(new_val))
        self.game.ign_required = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def toggle_role_required(self, interaction: discord.Interaction):
        new_val = not self.game.role_required
        await DatabaseHelper.update_game(self.game.game_id, role_required=int(new_val))
        self.game.role_required = new_val
        self.update_buttons()
        await interaction.response.edit_message(view=self)
        await self._refresh_queue_embeds(interaction.guild)

    async def _refresh_queue_embeds(self, guild: discord.Guild):
        """Refresh all queue embeds for this game after settings change."""
        try:
            from .views_gameplay import QueueView, ReadyCheckView

            for queue_id, queue_state in self.cog.queues.items():
                if queue_state.game_id == self.game.game_id and queue_state.message_id:
                    channel = guild.get_channel(queue_state.channel_id)
                    if channel:
                        try:
                            msg = await channel.fetch_message(queue_state.message_id)
                            # Reload game config to get fresh settings
                            fresh_game = await DatabaseHelper.get_game(self.game.game_id)
                            embed = await self.cog.create_queue_embed(fresh_game, queue_state, guild)
                            if queue_state.state == "ready_check":
                                view = ReadyCheckView(self.cog, self.game.game_id, queue_id)
                            else:
                                view = QueueView(self.cog, self.game.game_id, queue_id)
                            await msg.edit(embed=embed, view=view)
                        except discord.NotFound:
                            pass
                        except Exception as e:
                            logger.error(f"Error refreshing queue embed: {e}")
        except Exception as e:
            logger.error(f"Error refreshing queue embeds: {e}")


class VerificationTopicModal(discord.ui.Modal, title="Set Verification Topic"):
    topic_name = discord.ui.TextInput(
        label="Topic Name",
        placeholder="e.g., tenman-(val) - leave empty to clear",
        required=False,
        max_length=100
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: GameTogglesView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        if game.verification_topic:
            self.topic_name.default = game.verification_topic

    async def on_submit(self, interaction: discord.Interaction):
        topic = self.topic_name.value.strip() or None
        await DatabaseHelper.update_game(self.game.game_id, verification_topic=topic)
        self.game.verification_topic = topic
        self.parent_view.update_buttons()

        if topic:
            await interaction.response.send_message(
                f"Verification topic set to `{topic}`.\n"
                "Users without the verified role will see a button to open this ticket.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Verification topic cleared.",
                ephemeral=True
            )


class PenaltySettingsView(ExpiringView):
    """View for configuring penalty settings (timeout + decline)."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game

    # --- Timeout penalties (didn't ready up in time) ---

    @discord.ui.button(label="Timeout Durations", style=discord.ButtonStyle.primary, row=0)
    async def edit_durations(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = PenaltyDurationsModal(self.cog, self.game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="View Timeout Penalties", style=discord.ButtonStyle.secondary, row=0)
    async def view_penalties(self, interaction: discord.Interaction, button: discord.ui.Button):
        penalties = await DatabaseHelper.get_all_penalties()
        if not penalties:
            await interaction.response.send_message("No active timeout penalties.", ephemeral=True)
            return

        lines = ["**Active Timeout Penalties**\n"]
        for p in penalties:
            user = interaction.guild.get_member(p.player_id)
            name = user.display_name if user else str(p.player_id)
            expires = p.penalty_expires.strftime("%Y-%m-%d %H:%M UTC") if p.penalty_expires else "Unknown"
            lines.append(f"• {name}: Offense #{p.offense_count}, expires {expires}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Clear Timeout Penalty", style=discord.ButtonStyle.danger, row=0)
    async def clear_penalty(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ClearPenaltyUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to clear timeout penalty:", view=view, ephemeral=True)

    # --- Decline penalties (clicked Not Ready) ---

    @discord.ui.button(label="Decline Durations", style=discord.ButtonStyle.primary, row=1)
    async def edit_decline_durations(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = DeclinePenaltyDurationsModal(self.cog, self.game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="View Decline Penalties", style=discord.ButtonStyle.secondary, row=1)
    async def view_decline_penalties(self, interaction: discord.Interaction, button: discord.ui.Button):
        penalties = await DatabaseHelper.get_all_decline_penalties()
        if not penalties:
            await interaction.response.send_message("No active decline penalties.", ephemeral=True)
            return

        lines = ["**Active Decline Penalties**\n"]
        for p in penalties:
            user = interaction.guild.get_member(p.player_id)
            name = user.display_name if user else str(p.player_id)
            expires = p.penalty_expires.strftime("%Y-%m-%d %H:%M UTC") if p.penalty_expires else "Unknown"
            lines.append(f"• {name}: Offense #{p.offense_count}, expires {expires}")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Clear Decline Penalty", style=discord.ButtonStyle.danger, row=1)
    async def clear_decline_penalty(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ClearDeclinePenaltyUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to clear decline penalty:", view=view, ephemeral=True)


class PenaltyDurationsModal(discord.ui.Modal, title="Penalty Durations"):
    first_offense = discord.ui.TextInput(label="1st Offense (e.g., 60m, 1h, 1d)", required=True)
    second_offense = discord.ui.TextInput(label="2nd Offense (e.g., 60m, 1h, 1d)", required=True)
    third_offense = discord.ui.TextInput(label="3rd+ Offense (e.g., 60m, 1h, 1d)", required=True)
    decay_days = discord.ui.TextInput(label="Decay Period (days)", required=True)

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        self.first_offense.default = str(game.penalty_1st_minutes)
        self.second_offense.default = str(game.penalty_2nd_minutes)
        self.third_offense.default = str(game.penalty_3rd_minutes)
        self.decay_days.default = str(game.penalty_decay_days)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await DatabaseHelper.update_game(
                self.game.game_id,
                penalty_1st_minutes=parse_duration_to_minutes(self.first_offense.value),
                penalty_2nd_minutes=parse_duration_to_minutes(self.second_offense.value),
                penalty_3rd_minutes=parse_duration_to_minutes(self.third_offense.value),
                penalty_decay_days=int(self.decay_days.value)
            )
            await interaction.response.send_message("Penalty settings updated.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid values.", ephemeral=True)


class ClearPenaltyUserSelectView(ExpiringView):
    """View for selecting a user to clear timeout penalty."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        await DatabaseHelper.clear_ready_penalty(user.id)
        await interaction.response.edit_message(content=f"Cleared timeout penalty for **{user.display_name}**.", view=None)
        await self.cog.log_action(
            interaction.guild,
            f"Ready-up penalty cleared for **{user.display_name}** by {interaction.user.display_name}"
        )


class DeclinePenaltyDurationsModal(discord.ui.Modal, title="Decline Penalty Durations"):
    first_offense = discord.ui.TextInput(label="1st Decline (e.g., 15m, 1h)", required=True)
    second_offense = discord.ui.TextInput(label="2nd Decline (e.g., 60m, 1h)", required=True)
    third_offense = discord.ui.TextInput(label="3rd+ Decline (e.g., 1d, 1440m)", required=True)

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        self.first_offense.default = str(game.decline_1st_minutes)
        self.second_offense.default = str(game.decline_2nd_minutes)
        self.third_offense.default = str(game.decline_3rd_minutes)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await DatabaseHelper.update_game(
                self.game.game_id,
                decline_1st_minutes=parse_duration_to_minutes(self.first_offense.value),
                decline_2nd_minutes=parse_duration_to_minutes(self.second_offense.value),
                decline_3rd_minutes=parse_duration_to_minutes(self.third_offense.value),
            )
            await interaction.response.send_message("Decline penalty settings updated.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid values.", ephemeral=True)


class ClearDeclinePenaltyUserSelectView(ExpiringView):
    """View for selecting a user to clear decline penalty."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        await DatabaseHelper.clear_decline_penalty(user.id)
        await interaction.response.edit_message(content=f"Cleared decline penalty for **{user.display_name}**.", view=None)
        await self.cog.log_action(
            interaction.guild,
            f"Decline penalty cleared for **{user.display_name}** by {interaction.user.display_name}"
        )


class QueueScheduleView(ExpiringView):
    """View for configuring queue schedule with per-day times."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game
        self.update_toggle_button()

    def update_toggle_button(self):
        for item in self.children:
            if hasattr(item, 'custom_id') and item.custom_id == 'toggle_schedule':
                item.label = f"Schedule: {'ON' if self.game.schedule_enabled else 'OFF'}"
                item.style = discord.ButtonStyle.success if self.game.schedule_enabled else discord.ButtonStyle.secondary

    @discord.ui.button(label="Schedule: OFF", style=discord.ButtonStyle.secondary, custom_id="toggle_schedule")
    async def toggle_schedule(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            new_val = not self.game.schedule_enabled
            logger.debug(f"toggle_schedule: {self.game.name} -> schedule_enabled={new_val}")
            await DatabaseHelper.update_game(self.game.game_id, schedule_enabled=int(new_val))
            self.game.schedule_enabled = new_val
            button.label = f"Schedule: {'ON' if new_val else 'OFF'}"
            button.style = discord.ButtonStyle.success if new_val else discord.ButtonStyle.secondary
            await interaction.response.edit_message(view=self)

            # Reload game to get latest data
            fresh_game = await DatabaseHelper.get_game(self.game.game_id)
            if not fresh_game or not fresh_game.queue_channel_id:
                return

            channel = self.cog.bot.get_channel(fresh_game.queue_channel_id)
            if not channel:
                return

            if new_val:
                # Schedule ENABLED - apply current schedule state (close if outside hours)
                logger.debug(f"toggle_schedule: Applying schedule state for {self.game.name}")
                await self.cog.apply_schedule_state(fresh_game)
            else:
                # Schedule DISABLED - delete countdown embed and start fresh queue
                logger.debug(f"toggle_schedule: Schedule disabled, cleaning up for {self.game.name}")

                # Delete countdown embed if exists
                if fresh_game.schedule_down_message_id:
                    try:
                        down_msg = await channel.fetch_message(fresh_game.schedule_down_message_id)
                        await down_msg.delete()
                        logger.debug(f"toggle_schedule: Deleted countdown embed")
                    except discord.NotFound:
                        pass
                    await DatabaseHelper.update_game(fresh_game.game_id, schedule_down_message_id=None)

                # Start a fresh queue
                await self.cog.start_queue(channel, fresh_game)
                logger.debug(f"toggle_schedule: Started fresh queue")
        except Exception as e:
            logger.error(f"toggle_schedule ERROR: {e}")
            import traceback
            traceback.print_exc()

    @discord.ui.button(label="Add/Edit Day", style=discord.ButtonStyle.primary)
    async def add_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show dropdown to select a day to configure."""
        view = ScheduleDaySelectView(self.cog, self.game)
        await interaction.response.send_message(
            "Select a day to configure its schedule:",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Remove Day", style=discord.ButtonStyle.danger)
    async def remove_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show dropdown to remove a configured day."""
        if not self.game.schedule_times:
            await interaction.response.send_message("No days configured to remove.", ephemeral=True)
            return
        view = ScheduleDayRemoveView(self.cog, self.game)
        await interaction.response.send_message(
            "Select a day to remove from the schedule:",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Quick Setup: Weekdays", style=discord.ButtonStyle.secondary, row=1)
    async def quick_weekdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Quick setup: Mon-Fri with same times."""
        modal = QuickScheduleModal(self.cog, self.game, list(range(5)), parent_view=self)  # 0-4 = Mon-Fri
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Quick Setup: Every Day", style=discord.ButtonStyle.secondary, row=1)
    async def quick_everyday(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Quick setup: Every day with same times."""
        modal = QuickScheduleModal(self.cog, self.game, list(range(7)), parent_view=self)  # 0-6 = All days
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Quick Setup: Weekends Only", style=discord.ButtonStyle.secondary, row=1)
    async def quick_weekends(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Quick setup: Sat/Sun open all day, Mon-Fri closed all day."""
        schedule_times = {}
        for d in range(5):  # Mon-Fri
            schedule_times[str(d)] = {"mode": "closed"}
        for d in range(5, 7):  # Sat-Sun
            schedule_times[str(d)] = {"mode": "open"}
        await DatabaseHelper.update_game(self.game.game_id, schedule_times=json.dumps(schedule_times), schedule_enabled=1)
        self.game.schedule_times = schedule_times
        self.game.schedule_enabled = True
        self.update_toggle_button()
        await interaction.response.send_message(
            f"Set **{self.game.name}** schedule to **Weekends Only**:\n"
            f"Mon-Fri: Closed all day\nSat-Sun: Open all day",
            ephemeral=True
        )
        await self.cog.apply_schedule_state(self.game)

    @discord.ui.button(label="Clear All", style=discord.ButtonStyle.danger, row=2)
    async def clear_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clear all schedule configuration."""
        await DatabaseHelper.update_game(
            self.game.game_id,
            schedule_times=None,
            schedule_open_days=None,
            schedule_open_time=None,
            schedule_close_time=None
        )
        self.game.schedule_times = None
        await interaction.response.send_message(
            f"Cleared all schedule settings for **{self.game.name}**.",
            ephemeral=True
        )


class ScheduleDaySelectView(ExpiringView):
    """Dropdown to select a day to configure."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

        options = []
        for i, day in enumerate(self.DAY_NAMES):
            current = ""
            if game.schedule_times and str(i) in game.schedule_times:
                config = game.schedule_times[str(i)]
                mode = config.get("mode")
                if mode == "open":
                    current = " (Open all day)"
                elif mode == "closed":
                    current = " (Closed all day)"
                else:
                    open_time = config.get('open')
                    close_time = config.get('close')
                    if open_time and close_time:
                        current = f" ({open_time} - {close_time})"
                    elif open_time:
                        current = f" (Opens {open_time} →)"
                    elif close_time:
                        current = f" (→ Closes {close_time})"
                    else:
                        current = ""
            options.append(discord.SelectOption(label=f"{day}{current}", value=str(i)))

        select = discord.ui.Select(placeholder="Select a day...", options=options)
        select.callback = self.day_selected
        self.add_item(select)

    async def day_selected(self, interaction: discord.Interaction):
        day_num = int(interaction.data["values"][0])
        view = ScheduleDayModeView(self.cog, self.game, day_num)
        day_name = self.DAY_NAMES[day_num]
        await interaction.response.edit_message(
            content=f"How should **{day_name}** be configured?",
            view=view
        )


class ScheduleDayModeView(ExpiringView):
    """View for choosing day mode: Open All Day, Closed All Day, or Set Times."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig, day_num: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game
        self.day_num = day_num

    @discord.ui.button(label="Open All Day", style=discord.ButtonStyle.success)
    async def open_all_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        schedule_times = self.game.schedule_times or {}
        schedule_times[str(self.day_num)] = {"mode": "open"}
        await DatabaseHelper.update_game(self.game.game_id, schedule_times=json.dumps(schedule_times))
        self.game.schedule_times = schedule_times
        day_name = self.DAY_NAMES[self.day_num]
        await interaction.response.send_message(
            f"Set **{day_name}** to **Open all day** for **{self.game.name}**.",
            ephemeral=True
        )

    @discord.ui.button(label="Closed All Day", style=discord.ButtonStyle.danger)
    async def closed_all_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        schedule_times = self.game.schedule_times or {}
        schedule_times[str(self.day_num)] = {"mode": "closed"}
        await DatabaseHelper.update_game(self.game.game_id, schedule_times=json.dumps(schedule_times))
        self.game.schedule_times = schedule_times
        day_name = self.DAY_NAMES[self.day_num]
        await interaction.response.send_message(
            f"Set **{day_name}** to **Closed all day** for **{self.game.name}**.",
            ephemeral=True
        )

    @discord.ui.button(label="Set Times", style=discord.ButtonStyle.primary)
    async def set_times(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ScheduleDayModal(self.cog, self.game, self.day_num)
        await interaction.response.send_modal(modal)


class ScheduleDayRemoveView(ExpiringView):
    """Dropdown to remove a configured day."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

        options = []
        if game.schedule_times:
            for day_num, times in sorted(game.schedule_times.items(), key=lambda x: int(x[0])):
                day_name = self.DAY_NAMES[int(day_num)]
                mode = times.get("mode")
                if mode == "open":
                    desc = "Open all day"
                elif mode == "closed":
                    desc = "Closed all day"
                else:
                    open_time = times.get('open')
                    close_time = times.get('close')
                    if open_time and close_time:
                        desc = f"{open_time} - {close_time}"
                    elif open_time:
                        desc = f"Opens {open_time} →"
                    elif close_time:
                        desc = f"→ Closes {close_time}"
                    else:
                        desc = "Configured"
                options.append(discord.SelectOption(
                    label=f"{day_name} ({desc})",
                    value=day_num
                ))

        if options:
            select = discord.ui.Select(placeholder="Select a day to remove...", options=options)
            select.callback = self.day_removed
            self.add_item(select)

    async def day_removed(self, interaction: discord.Interaction):
        day_num = interaction.data["values"][0]
        if self.game.schedule_times and day_num in self.game.schedule_times:
            del self.game.schedule_times[day_num]
            await DatabaseHelper.update_game(
                self.game.game_id,
                schedule_times=json.dumps(self.game.schedule_times) if self.game.schedule_times else None
            )
            day_name = self.DAY_NAMES[int(day_num)]
            await interaction.response.send_message(
                f"Removed **{day_name}** from the schedule for **{self.game.name}**.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("Day not found in schedule.", ephemeral=True)


class ScheduleDayModal(discord.ui.Modal, title="Set Day Schedule"):
    """Modal for setting open/close times for a specific day."""

    open_time = discord.ui.TextInput(
        label="Open Time (24h format)",
        placeholder="e.g., 16:00 (blank = roll over)",
        required=False,
        max_length=5
    )
    close_time = discord.ui.TextInput(
        label="Close Time (24h format)",
        placeholder="e.g., 23:00 (blank = roll over)",
        required=False,
        max_length=5
    )

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig, day_num: int):
        super().__init__()
        self.cog = cog
        self.game = game
        self.day_num = day_num
        self.title = f"Set {self.DAY_NAMES[day_num]} Schedule"

        # Pre-fill with existing times if available
        if game.schedule_times and str(day_num) in game.schedule_times:
            times = game.schedule_times[str(day_num)]
            self.open_time.default = times.get("open", "")
            self.close_time.default = times.get("close", "")

    async def on_submit(self, interaction: discord.Interaction):
        import re as _re
        time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'

        open_val = self.open_time.value.strip()
        close_val = self.close_time.value.strip()

        if not open_val and not close_val:
            await interaction.response.send_message("At least one time (open or close) must be provided.", ephemeral=True)
            return

        if open_val and not _re.match(time_pattern, open_val):
            await interaction.response.send_message("Invalid open time. Use HH:MM format.", ephemeral=True)
            return
        if close_val and not _re.match(time_pattern, close_val):
            await interaction.response.send_message("Invalid close time. Use HH:MM format.", ephemeral=True)
            return

        # Build entry with only provided times
        entry = {}
        if open_val:
            open_parts = open_val.split(":")
            entry["open"] = f"{int(open_parts[0]):02d}:{open_parts[1]}"
        if close_val:
            close_parts = close_val.split(":")
            entry["close"] = f"{int(close_parts[0]):02d}:{close_parts[1]}"

        # Update schedule_times
        schedule_times = self.game.schedule_times or {}
        schedule_times[str(self.day_num)] = entry

        await DatabaseHelper.update_game(
            self.game.game_id,
            schedule_times=json.dumps(schedule_times)
        )
        self.game.schedule_times = schedule_times

        day_name = self.DAY_NAMES[self.day_num]
        if "open" in entry and "close" in entry:
            desc = f"Open: {entry['open']} | Close: {entry['close']}"
        elif "open" in entry:
            desc = f"Opens at {entry['open']} (rolls over to next day)"
        else:
            desc = f"Closes at {entry['close']} (rolled over from previous day)"
        await interaction.response.send_message(
            f"Set **{day_name}** schedule for **{self.game.name}**:\n{desc}",
            ephemeral=True
        )


class QuickScheduleModal(discord.ui.Modal, title="Quick Schedule Setup"):
    """Modal for quick setup of multiple days with same times."""

    open_time = discord.ui.TextInput(
        label="Open Time (24h format)",
        placeholder="e.g., 16:00",
        required=True,
        max_length=5
    )
    close_time = discord.ui.TextInput(
        label="Close Time (24h format)",
        placeholder="e.g., 23:00",
        required=True,
        max_length=5
    )

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig, days: List[int], parent_view=None):
        super().__init__()
        self.cog = cog
        self.game = game
        self.days = days
        self.parent_view = parent_view

        if len(days) == 5:
            self.title = "Weekday Schedule (Mon-Fri)"
        elif len(days) == 7:
            self.title = "Every Day Schedule"
        else:
            self.title = "Quick Schedule Setup"

    async def on_submit(self, interaction: discord.Interaction):
        import re as _re
        time_pattern = r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'

        if not _re.match(time_pattern, self.open_time.value):
            await interaction.response.send_message("Invalid open time. Use HH:MM format.", ephemeral=True)
            return
        if not _re.match(time_pattern, self.close_time.value):
            await interaction.response.send_message("Invalid close time. Use HH:MM format.", ephemeral=True)
            return

        # Normalize times
        open_parts = self.open_time.value.split(":")
        close_parts = self.close_time.value.split(":")
        open_normalized = f"{int(open_parts[0]):02d}:{open_parts[1]}"
        close_normalized = f"{int(close_parts[0]):02d}:{close_parts[1]}"

        # Build schedule_times
        schedule_times = {}
        for day_num in self.days:
            schedule_times[str(day_num)] = {"open": open_normalized, "close": close_normalized}

        await DatabaseHelper.update_game(
            self.game.game_id,
            schedule_times=json.dumps(schedule_times),
            schedule_enabled=1
        )
        self.game.schedule_times = schedule_times
        self.game.schedule_enabled = True
        if self.parent_view:
            self.parent_view.update_toggle_button()

        days_str = ", ".join(self.DAY_NAMES[d] for d in self.days)
        await interaction.response.send_message(
            f"Set schedule for **{self.game.name}**:\n"
            f"Days: {days_str}\n"
            f"Open: {open_normalized} | Close: {close_normalized}",
            ephemeral=True
        )
        await self.cog.apply_schedule_state(self.game)


class ReadyTimerModal(discord.ui.Modal, title="Ready Timer Settings"):
    """Modal for configuring the ready timer duration."""

    timer_seconds = discord.ui.TextInput(
        label="Ready Timer (seconds)",
        placeholder="e.g., 60",
        required=True,
        max_length=5
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig,
                 parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_page = parent_page
        self.timer_seconds.default = str(game.ready_timer_seconds)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            seconds = int(self.timer_seconds.value)
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)
            return
        if seconds < 10 or seconds > 600:
            await interaction.response.send_message(
                "Ready timer must be between 10 and 600 seconds.", ephemeral=True
            )
            return

        await DatabaseHelper.update_game(self.game.game_id, ready_timer_seconds=seconds)
        await _respond_to_modal(
            interaction, self.parent_page,
            f"Ready timer for **{self.game.name}** set to **{seconds} seconds**."
        )


class GracePeriodModal(discord.ui.Modal, title="Grace Period Settings"):
    """Modal for configuring the per-player grace period duration."""

    grace_minutes = discord.ui.TextInput(
        label="Grace Period (minutes)",
        placeholder="e.g., 10",
        required=True,
        max_length=3
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: GameTogglesView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        self.grace_minutes.default = str(game.grace_period_minutes)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            minutes = int(self.grace_minutes.value)
            if minutes < 1 or minutes > 60:
                await interaction.response.send_message(
                    "Grace period must be between 1 and 60 minutes.",
                    ephemeral=True
                )
                return

            await DatabaseHelper.update_game(self.game.game_id, grace_period_minutes=minutes)
            self.game.grace_period_minutes = minutes
            self.parent_view.update_buttons()
            await interaction.response.send_message(
                f"Grace period for **{self.game.name}** set to **{minutes} minutes**.\n"
                f"Players who joined within the last {minutes} minutes will be auto-readied.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


class PCOffsetModal(discord.ui.Modal, title="PC Seed Offset"):
    """Modal for setting the rank-tier bump applied when seeding a PC player."""

    offset_tiers = discord.ui.TextInput(
        label="PC Offset (rank tiers)",
        placeholder="e.g., 1.0  (0 = no bump)",
        required=True,
        max_length=4
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: GameTogglesView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        self.offset_tiers.default = f"{game.pc_offset_tiers:g}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            tiers = float(self.offset_tiers.value)
            if tiers < 0 or tiers > 3:
                await interaction.response.send_message(
                    "PC offset must be between 0 and 3 tiers.",
                    ephemeral=True
                )
                return

            await DatabaseHelper.update_game(self.game.game_id, pc_offset_tiers=tiers)
            self.game.pc_offset_tiers = tiers
            self.parent_view.update_buttons()
            await interaction.response.send_message(
                f"PC seed offset for **{self.game.name}** set to **+{tiers:g} tier(s)**.\n"
                f"Applied once when seeding a PC player from their rank; MMR converges "
                f"to their true skill over placements.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


class NotReadyCooldownModal(discord.ui.Modal, title="Not Ready Cooldown"):
    """Modal for configuring the cooldown applied when a player clicks Not Ready."""

    cooldown_minutes = discord.ui.TextInput(
        label="Cooldown (minutes) — 0 to disable",
        placeholder="e.g., 5",
        required=True,
        max_length=3
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: GameTogglesView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        self.cooldown_minutes.default = str(game.not_ready_cooldown_minutes)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            minutes = int(self.cooldown_minutes.value)
            if minutes < 0 or minutes > 60:
                await interaction.response.send_message(
                    "Cooldown must be between 0 and 60 minutes (0 to disable).",
                    ephemeral=True
                )
                return

            await DatabaseHelper.update_game(self.game.game_id, not_ready_cooldown_minutes=minutes)
            self.game.not_ready_cooldown_minutes = minutes
            self.parent_view.update_buttons()
            if minutes == 0:
                await interaction.response.send_message(
                    f"Not Ready cooldown **disabled** for **{self.game.name}**.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"Not Ready cooldown for **{self.game.name}** set to **{minutes} minutes**.\n"
                    f"Players who click Not Ready will be unable to join any queue for {minutes} minutes.",
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


# =============================================================================
# SETTINGS MODALS (Legacy - kept for backwards compatibility)
# =============================================================================

class AddGameModal(discord.ui.Modal, title="Add Game"):
    name = discord.ui.TextInput(label="Game Name", placeholder="e.g., Valorant", required=True)
    player_count = discord.ui.TextInput(label="Players per Queue", placeholder="e.g., 10", required=True)
    queue_type = discord.ui.TextInput(
        label="Queue Type (mmr/captains/random)",
        placeholder="mmr",
        default="mmr",
        required=True
    )
    captain_selection = discord.ui.TextInput(
        label="Captain Selection (random/admin/highest_mmr)",
        placeholder="random",
        default="random",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.parent_page = parent_page

    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
        except ValueError:
            await interaction.response.send_message("Invalid player count.", ephemeral=True)
            return
        if count < 2 or count > 20:
            await interaction.response.send_message("Player count must be 2-20.", ephemeral=True)
            return

        qt = self.queue_type.value.lower()
        if qt not in ["mmr", "captains", "random"]:
            await interaction.response.send_message("Invalid queue type.", ephemeral=True)
            return

        cs = self.captain_selection.value.lower()
        if cs not in ["random", "admin", "highest_mmr"]:
            await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
            return

        try:
            await DatabaseHelper.add_game(self.name.value, count, qt, cs)
        except Exception as e:
            if "UNIQUE" in str(e):
                await interaction.response.send_message(
                    "A game with that name already exists.", ephemeral=True
                )
                return
            raise

        note = f"Added **{self.name.value}** ({count} players, {qt} queue)."
        if 'overwatch' in self.name.value.lower():
            note += (
                "\n🛡️ **Overwatch mode enabled** — role selection is now required, "
                "and per-role weights were seeded (Tank 1.30 · Support 1.15 · DPS 1.00)."
            )
            if count != 12 or qt != "mmr":
                note += (
                    "\n⚠️ Strict 2-2-2 expects **12 players** on an **mmr** queue; "
                    "current settings will fall back to standard balancing."
                )
        await _respond_to_modal(interaction, self.parent_page, note)


class EditGameModal(discord.ui.Modal, title="Edit Game"):
    """Queue shape only.

    The queue channel and queue role used to be typed in here as raw IDs; both
    now have real pickers (Settings → Channels, and the game's Queue Role page),
    so this modal is just the three values that have no better widget.
    """

    def __init__(self, cog: 'CustomMatch', game: GameConfig,
                 parent_page: 'SettingsPage' = None):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_page = parent_page
        self.title = f"Edit {game.name}"[:45]

        self.player_count = discord.ui.TextInput(
            label="Players per Queue",
            default=str(game.player_count),
            required=True
        )
        self.queue_type = discord.ui.TextInput(
            label="Queue Type (mmr/captains/random)",
            default=game.queue_type.value,
            required=True
        )
        self.captain_selection = discord.ui.TextInput(
            label="Captain Selection (random/admin/highest_mmr)",
            default=game.captain_selection.value,
            required=True
        )

        self.add_item(self.player_count)
        self.add_item(self.queue_type)
        self.add_item(self.captain_selection)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
        except ValueError:
            await interaction.response.send_message("Invalid number format.", ephemeral=True)
            return

        qt = self.queue_type.value.lower()
        cs = self.captain_selection.value.lower()
        if qt not in ["mmr", "captains", "random"]:
            await interaction.response.send_message("Invalid queue type.", ephemeral=True)
            return
        if cs not in ["random", "admin", "highest_mmr"]:
            await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
            return

        await DatabaseHelper.update_game(
            self.game.game_id, player_count=count, queue_type=qt, captain_selection=cs
        )
        await _respond_to_modal(
            interaction, self.parent_page,
            f"**{self.game.name}** now {count} players · {qt} queue · {cs} captains."
        )


# =============================================================================
# SECONDARY QUEUE SETTINGS
# =============================================================================

class SecondaryQueueSettingsView(ExpiringView):
    """Configure the secondary/fun-mode queue for a game."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game
        self._rebuild_buttons()

    def _rebuild_buttons(self):
        self.clear_items()
        g = self.game

        # Row 0: Enable/disable toggle
        toggle_btn = discord.ui.Button(
            label=f"Secondary Queue: {'ON' if g.secondary_queue_enabled else 'OFF'}",
            style=discord.ButtonStyle.success if g.secondary_queue_enabled else discord.ButtonStyle.secondary,
            row=0,
        )
        toggle_btn.callback = self._toggle_enabled
        self.add_item(toggle_btn)

        # Row 1: Configuration buttons (only shown when enabled)
        if g.secondary_queue_enabled:
            name_btn = discord.ui.Button(label="Mode Name", style=discord.ButtonStyle.primary, row=1)
            name_btn.callback = self._set_name
            self.add_item(name_btn)

            pc_btn = discord.ui.Button(label="Player Count", style=discord.ButtonStyle.primary, row=1)
            pc_btn.callback = self._set_player_count
            self.add_item(pc_btn)

            qt_btn = discord.ui.Button(label="Queue Type", style=discord.ButtonStyle.primary, row=1)
            qt_btn.callback = self._set_queue_type
            self.add_item(qt_btn)

            ch_btn = discord.ui.Button(label="Channel", style=discord.ButtonStyle.primary, row=1)
            ch_btn.callback = self._set_channel
            self.add_item(ch_btn)

            # Row 2: Schedule, match limit, and mode vote game
            sched_btn = discord.ui.Button(label="Schedule", style=discord.ButtonStyle.primary, row=2)
            sched_btn.callback = self._configure_schedule
            self.add_item(sched_btn)

            limit_btn = discord.ui.Button(label="Match Limit", style=discord.ButtonStyle.primary, row=2)
            limit_btn.callback = self._set_match_limit
            self.add_item(limit_btn)

            modes_btn = discord.ui.Button(label="Game Modes", style=discord.ButtonStyle.primary, row=2)
            modes_btn.callback = self._manage_modes
            self.add_item(modes_btn)

            banner_btn = discord.ui.Button(label="Banner", style=discord.ButtonStyle.primary, row=2)
            banner_btn.callback = self._set_banner
            self.add_item(banner_btn)

        # clear_items() above drops the settings panel's Back button along with
        # the controls, so put it back on every rebuild.
        self._restore_back()

    async def build_status_embed(self) -> discord.Embed:
        g = self.game
        embed = discord.Embed(title=f"{g.name} — Secondary Queue", color=COLOR_NEUTRAL)

        status = "Enabled" if g.secondary_queue_enabled else "Disabled"
        embed.add_field(name="Status", value=status, inline=True)

        if g.secondary_queue_enabled:
            embed.add_field(name="Mode Name", value=g.secondary_queue_name or "Not set", inline=True)
            embed.add_field(
                name="Player Count",
                value=str(g.secondary_queue_player_count) if g.secondary_queue_player_count else "Same as main",
                inline=True,
            )
            qt_display = g.secondary_queue_type.value.upper() if g.secondary_queue_type else "Same as main"
            embed.add_field(name="Queue Type", value=qt_display, inline=True)

            if g.secondary_queue_channel_id:
                embed.add_field(name="Channel", value=f"<#{g.secondary_queue_channel_id}>", inline=True)
            else:
                embed.add_field(name="Channel", value="Same as main queue", inline=True)

            limit_str = str(g.secondary_queue_match_limit) if g.secondary_queue_match_limit is not None else "Unlimited"
            embed.add_field(name="Match Limit/Window", value=limit_str, inline=True)

            modes = await DatabaseHelper.get_secondary_modes(g.game_id)
            if modes:
                pool_labels = {"none": "No Map", "standard": "Standard", "custom": "Custom"}
                mode_lines = [f"{m['mode_name']} ({pool_labels.get(m['map_pool_type'], m['map_pool_type'])})" for m in modes]
                embed.add_field(name="Game Modes", value="\n".join(mode_lines), inline=True)
            else:
                embed.add_field(name="Game Modes", value="None configured", inline=True)

            banner_val = "Set" if g.secondary_banner_url else "Not set (uses main)"
            embed.add_field(name="Banner", value=banner_val, inline=True)

            # Schedule summary
            if g.secondary_schedule_times:
                lines = []
                for day_idx in range(7):
                    key = str(day_idx)
                    if key in g.secondary_schedule_times:
                        config = g.secondary_schedule_times[key]
                        mode = config.get("mode")
                        if mode == "open":
                            lines.append(f"{self.DAY_NAMES[day_idx]}: All day")
                        elif mode == "closed":
                            lines.append(f"{self.DAY_NAMES[day_idx]}: Closed")
                        else:
                            o = config.get("open", "?")
                            c = config.get("close")
                            if c:
                                lines.append(f"{self.DAY_NAMES[day_idx]}: {o} - {c}")
                            else:
                                lines.append(f"{self.DAY_NAMES[day_idx]}: {o} - roll over")
                embed.add_field(name="Schedule", value="\n".join(lines) if lines else "No days configured", inline=False)
            else:
                embed.add_field(name="Schedule", value="Not configured", inline=False)

        return embed

    async def _refresh(self, interaction: discord.Interaction):
        self.game = await DatabaseHelper.get_game(self.game.game_id)
        self._rebuild_buttons()
        embed = await self.build_status_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    async def _toggle_enabled(self, interaction: discord.Interaction):
        new_val = not self.game.secondary_queue_enabled
        await DatabaseHelper.update_game(self.game.game_id, secondary_queue_enabled=int(new_val))
        await self._refresh(interaction)

    async def _set_name(self, interaction: discord.Interaction):
        modal = SecondaryQueueNameModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def _set_player_count(self, interaction: discord.Interaction):
        modal = SecondaryQueuePlayerCountModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def _set_queue_type(self, interaction: discord.Interaction):
        view = SecondaryQueueTypeSelectView(self.cog, self.game, self)
        await interaction.response.send_message("Select queue type for secondary queue:", view=view, ephemeral=True)

    async def _set_channel(self, interaction: discord.Interaction):
        view = SecondaryQueueChannelSelectView(self.cog, self.game, self)
        await interaction.response.send_message(
            "Select a channel for the secondary queue, or leave empty to use the main queue channel:",
            view=view, ephemeral=True,
        )

    async def _configure_schedule(self, interaction: discord.Interaction):
        view = SecondaryScheduleView(self.cog, self.game, self)
        await interaction.response.send_message("Configure secondary queue schedule:", view=view, ephemeral=True)

    async def _set_match_limit(self, interaction: discord.Interaction):
        modal = SecondaryQueueMatchLimitModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def _manage_modes(self, interaction: discord.Interaction):
        view = SecondaryModesManageView(self.cog, self.game, self)
        await view.populate_items()
        embed = await view.build_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _set_banner(self, interaction: discord.Interaction):
        modal = SecondaryBannerModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)


class SecondaryQueueNameModal(discord.ui.Modal, title="Set Mode Name"):
    name_input = discord.ui.TextInput(
        label="Mode Name (e.g., Deathmatch, 2v2 Knife Only)",
        required=True,
        max_length=50,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        if game.secondary_queue_name:
            self.name_input.default = game.secondary_queue_name

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name_input.value.strip()
        if not name:
            await interaction.response.send_message("Name cannot be empty.", ephemeral=True)
            return
        await DatabaseHelper.update_game(self.game.game_id, secondary_queue_name=name)
        self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
        self.parent_view._rebuild_buttons()
        embed = await self.parent_view.build_status_embed()
        await interaction.response.edit_message(embed=embed, view=self.parent_view)


class SecondaryQueuePlayerCountModal(discord.ui.Modal, title="Set Player Count"):
    count_input = discord.ui.TextInput(
        label="Total players (e.g., 4 for 2v2, 6 for 3v3)",
        required=True,
        max_length=3,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        if game.secondary_queue_player_count:
            self.count_input.default = str(game.secondary_queue_player_count)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.count_input.value.strip())
            if count < 2 or count % 2 != 0:
                await interaction.response.send_message("Player count must be an even number >= 2.", ephemeral=True)
                return
            await DatabaseHelper.update_game(self.game.game_id, secondary_queue_player_count=count)
            self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
            self.parent_view._rebuild_buttons()
            embed = await self.parent_view.build_status_embed()
            await interaction.response.edit_message(embed=embed, view=self.parent_view)
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


class SecondaryQueueMatchLimitModal(discord.ui.Modal, title="Set Match Limit"):
    limit_input = discord.ui.TextInput(
        label="Max matches per window (0 = unlimited)",
        required=True,
        max_length=5,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        if game.secondary_queue_match_limit is not None:
            self.limit_input.default = str(game.secondary_queue_match_limit)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            limit = int(self.limit_input.value.strip())
            if limit < 0:
                await interaction.response.send_message("Limit must be >= 0.", ephemeral=True)
                return
            # 0 means unlimited (store as None)
            db_val = limit if limit > 0 else None
            await DatabaseHelper.update_game(self.game.game_id, secondary_queue_match_limit=db_val)
            self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
            self.parent_view._rebuild_buttons()
            embed = await self.parent_view.build_status_embed()
            await interaction.response.edit_message(embed=embed, view=self.parent_view)
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


class SecondaryModesManageView(ExpiringView):
    """Manage game modes for a secondary queue."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game
        self.parent_view = parent_view

    async def build_embed(self) -> discord.Embed:
        modes = await DatabaseHelper.get_secondary_modes(self.game.game_id)
        embed = discord.Embed(title=f"{self.game.name} — Game Modes", color=COLOR_NEUTRAL)
        if modes:
            pool_labels = {"none": "blank", "standard": "standard", "custom": "custom"}
            for m in modes:
                pool = pool_labels.get(m["map_pool_type"], m["map_pool_type"])
                desc = f"-# {m['description']}\n" if m.get("description") else ""
                tags = []
                if m.get("is_ffa"):
                    tags.append("FFA")
                if m.get("is_mirror"):
                    tags.append("Mirror")
                tag_str = " · " + " · ".join(f"**{t}**" for t in tags) if tags else ""
                embed.add_field(name=m["mode_name"], value=f"{desc}Map pool: **{pool}**{tag_str}", inline=False)
        else:
            embed.description = "No modes configured. Add up to 5 game modes."
        return embed

    async def populate_items(self):
        """Add buttons/selects based on current modes. Call before first display and after changes."""
        modes = await DatabaseHelper.get_secondary_modes(self.game.game_id)
        self.clear_items()

        add_btn = discord.ui.Button(
            label="Add Mode", style=discord.ButtonStyle.success, row=0,
            disabled=len(modes) >= 5
        )
        add_btn.callback = self._add_mode
        self.add_item(add_btn)

        if modes:
            options = [discord.SelectOption(label=m["mode_name"], value=str(m["mode_id"])) for m in modes]

            edit_select = discord.ui.Select(placeholder="Edit mode map pool...", options=options, row=1)
            edit_select.callback = self._edit_mode_pool
            self.add_item(edit_select)

            desc_options = [discord.SelectOption(label=m["mode_name"], value=str(m["mode_id"])) for m in modes]
            desc_select = discord.ui.Select(placeholder="Edit mode description...", options=desc_options, row=2)
            desc_select.callback = self._edit_mode_description
            self.add_item(desc_select)

            flag_options = []
            for m in modes:
                flags = []
                if m.get("is_ffa"):
                    flags.append("FFA")
                if m.get("is_mirror"):
                    flags.append("Mirror")
                flag_str = ", ".join(flags) if flags else "None"
                flag_options.append(discord.SelectOption(
                    label=m["mode_name"],
                    value=str(m["mode_id"]),
                    description=f"Flags: {flag_str}"
                ))
            flag_select = discord.ui.Select(placeholder="Toggle mode flags (FFA / Mirror)...", options=flag_options, row=3)
            flag_select.callback = self._toggle_flags
            self.add_item(flag_select)

            remove_select = discord.ui.Select(placeholder="Remove mode...", options=options, row=4)
            remove_select.callback = self._remove_mode
            self.add_item(remove_select)

    async def _rebuild(self, interaction: discord.Interaction):
        await self.populate_items()
        embed = await self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    async def _add_mode(self, interaction: discord.Interaction):
        modal = AddSecondaryModeModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def _edit_mode_pool(self, interaction: discord.Interaction):
        mode_id = int(interaction.data["values"][0])
        modes = await DatabaseHelper.get_secondary_modes(self.game.game_id)
        mode = next((m for m in modes if m["mode_id"] == mode_id), None)
        if not mode:
            await interaction.response.send_message("Mode not found.", ephemeral=True)
            return
        view = EditModePoolView(self.cog, self.game, mode, self)
        await interaction.response.send_message(
            f"Configure map pool for **{mode['mode_name']}**:", view=view, ephemeral=True
        )

    async def _edit_mode_description(self, interaction: discord.Interaction):
        mode_id = int(interaction.data["values"][0])
        modes = await DatabaseHelper.get_secondary_modes(self.game.game_id)
        mode = next((m for m in modes if m["mode_id"] == mode_id), None)
        if not mode:
            await interaction.response.send_message("Mode not found.", ephemeral=True)
            return
        modal = EditModeDescriptionModal(self.cog, self.game, mode, self)
        await interaction.response.send_modal(modal)

    async def _toggle_flags(self, interaction: discord.Interaction):
        mode_id = int(interaction.data["values"][0])
        modes = await DatabaseHelper.get_secondary_modes(self.game.game_id)
        mode = next((m for m in modes if m["mode_id"] == mode_id), None)
        if not mode:
            await interaction.response.send_message("Mode not found.", ephemeral=True)
            return
        view = ModeFlagToggleView(self.cog, self.game, mode, self)
        await interaction.response.send_message(
            f"Toggle flags for **{mode['mode_name']}**:", view=view, ephemeral=True
        )

    async def _remove_mode(self, interaction: discord.Interaction):
        mode_id = int(interaction.data["values"][0])
        modes = await DatabaseHelper.get_secondary_modes(self.game.game_id)
        mode = next((m for m in modes if m["mode_id"] == mode_id), None)
        if not mode:
            await interaction.response.send_message("Mode not found.", ephemeral=True)
            return
        await DatabaseHelper.remove_secondary_mode(mode_id)
        await self._rebuild(interaction)


class AddSecondaryModeModal(discord.ui.Modal, title="Add Game Mode"):
    name_input = discord.ui.TextInput(
        label="Mode name (e.g., Deathmatch, CTF)",
        required=True,
        max_length=50,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryModesManageView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name_input.value.strip()
        if not name:
            await interaction.response.send_message("Name cannot be empty.", ephemeral=True)
            return
        try:
            await DatabaseHelper.add_secondary_mode(self.game.game_id, name)
        except ValueError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return
        except Exception:
            await interaction.response.send_message("A mode with that name already exists.", ephemeral=True)
            return
        await self.parent_view._rebuild(interaction)


class EditModePoolView(ExpiringView):
    """Choose map pool type for a mode."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig, mode: dict,
                 parent_view: SecondaryModesManageView):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game
        self.mode = mode
        self.parent_view = parent_view

    @discord.ui.button(label="None (skip maps)", style=discord.ButtonStyle.secondary, row=0)
    async def set_none(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_secondary_mode(self.mode["mode_id"], map_pool_type="none", custom_maps=None)
        await self.parent_view._rebuild(interaction)

    @discord.ui.button(label="Standard (main game maps)", style=discord.ButtonStyle.primary, row=0)
    async def set_standard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_secondary_mode(self.mode["mode_id"], map_pool_type="standard", custom_maps=None)
        await self.parent_view._rebuild(interaction)

    @discord.ui.button(label="Custom map list", style=discord.ButtonStyle.success, row=0)
    async def set_custom(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = EditCustomMapsModal(self.cog, self.game, self.mode, self.parent_view)
        await interaction.response.send_modal(modal)


class EditCustomMapsModal(discord.ui.Modal, title="Set Custom Maps"):
    maps_input = discord.ui.TextInput(
        label="Map names (one per line)",
        placeholder="Hydra\nRoyal Palace\nYggsgard",
        required=True,
        style=discord.TextStyle.paragraph,
        max_length=500,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, mode: dict,
                 parent_view: SecondaryModesManageView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.mode = mode
        self.parent_view = parent_view
        if mode.get("custom_maps"):
            self.maps_input.default = "\n".join(mode["custom_maps"])

    async def on_submit(self, interaction: discord.Interaction):
        lines = [line.strip() for line in self.maps_input.value.strip().splitlines() if line.strip()]
        if not lines:
            await interaction.response.send_message("At least one map is required.", ephemeral=True)
            return
        await DatabaseHelper.update_secondary_mode(
            self.mode["mode_id"], map_pool_type="custom", custom_maps=lines
        )
        await self.parent_view._rebuild(interaction)


class ModeFlagToggleView(ExpiringView):
    """Toggle FFA and Mirror flags for a mode."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig, mode: dict,
                 parent_view: SecondaryModesManageView):
        super().__init__(timeout=30)
        self.cog = cog
        self.game = game
        self.mode = mode
        self.parent_view = parent_view

        ffa_label = "FFA: ON" if mode.get("is_ffa") else "FFA: OFF"
        ffa_style = discord.ButtonStyle.success if mode.get("is_ffa") else discord.ButtonStyle.secondary
        ffa_btn = discord.ui.Button(label=ffa_label, style=ffa_style, row=0)
        ffa_btn.callback = self._toggle_ffa
        self.add_item(ffa_btn)

        mirror_label = "Mirror: ON" if mode.get("is_mirror") else "Mirror: OFF"
        mirror_style = discord.ButtonStyle.success if mode.get("is_mirror") else discord.ButtonStyle.secondary
        mirror_btn = discord.ui.Button(label=mirror_label, style=mirror_style, row=0)
        mirror_btn.callback = self._toggle_mirror
        self.add_item(mirror_btn)

    async def _toggle_ffa(self, interaction: discord.Interaction):
        new_val = not self.mode.get("is_ffa", False)
        await DatabaseHelper.update_secondary_mode(self.mode["mode_id"], is_ffa=int(new_val))
        self.mode["is_ffa"] = new_val
        await self.parent_view._rebuild(interaction)

    async def _toggle_mirror(self, interaction: discord.Interaction):
        new_val = not self.mode.get("is_mirror", False)
        await DatabaseHelper.update_secondary_mode(self.mode["mode_id"], is_mirror=int(new_val))
        self.mode["is_mirror"] = new_val
        await self.parent_view._rebuild(interaction)


class EditModeDescriptionModal(discord.ui.Modal, title="Edit Mode Description"):
    desc_input = discord.ui.TextInput(
        label="Description (leave blank to clear)",
        placeholder="e.g. Free-for-all elimination, last one standing wins",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=200,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, mode: dict,
                 parent_view: SecondaryModesManageView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.mode = mode
        self.parent_view = parent_view
        if mode.get("description"):
            self.desc_input.default = mode["description"]

    async def on_submit(self, interaction: discord.Interaction):
        desc = self.desc_input.value.strip() or None
        await DatabaseHelper.update_secondary_mode(self.mode["mode_id"], description=desc)
        await self.parent_view._rebuild(interaction)


class SecondaryBannerModal(discord.ui.Modal, title="Set Secondary Queue Banner"):
    banner_url = discord.ui.TextInput(
        label="Banner URL (leave blank to clear)",
        placeholder="https://example.com/banner.png or .gif",
        required=False,
        style=discord.TextStyle.short,
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.parent_view = parent_view
        if game.secondary_banner_url:
            self.banner_url.default = game.secondary_banner_url

    async def on_submit(self, interaction: discord.Interaction):
        url = self.banner_url.value.strip() or None
        await DatabaseHelper.update_game(self.game.game_id, secondary_banner_url=url)
        self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
        self.parent_view._rebuild_buttons()
        embed = await self.parent_view.build_status_embed()
        await interaction.response.edit_message(embed=embed, view=self.parent_view)


class SecondaryQueueTypeSelectView(ExpiringView):
    """Select queue type for the secondary queue."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game
        self.parent_view = parent_view

        options = [
            discord.SelectOption(label="MMR", value="mmr", description="Balanced teams by MMR"),
            discord.SelectOption(label="Captains", value="captains", description="Captain draft picks"),
            discord.SelectOption(label="Random", value="random", description="Random team assignment"),
        ]
        select = discord.ui.Select(placeholder="Select queue type", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def _on_select(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        await DatabaseHelper.update_game(self.game.game_id, secondary_queue_type=value)
        self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
        self.parent_view._rebuild_buttons()
        embed = await self.parent_view.build_status_embed()
        # Edit the parent message (the settings view)
        await interaction.response.send_message(
            f"Queue type set to **{value.upper()}**.", ephemeral=True
        )


class SecondaryQueueChannelSelectView(ExpiringView):
    """Select channel for the secondary queue."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game
        self.parent_view = parent_view

        select = discord.ui.ChannelSelect(
            placeholder="Select channel (or skip for same as main)",
            channel_types=[discord.ChannelType.text],
            min_values=0,
            max_values=1,
        )
        select.callback = self._on_select
        self.add_item(select)

        clear_btn = discord.ui.Button(label="Use Main Queue Channel", style=discord.ButtonStyle.secondary, row=1)
        clear_btn.callback = self._clear_channel
        self.add_item(clear_btn)

    async def _on_select(self, interaction: discord.Interaction):
        if interaction.data.get("values"):
            channel_id = int(interaction.data["values"][0])
            await DatabaseHelper.update_game(self.game.game_id, secondary_queue_channel_id=channel_id)
            await interaction.response.send_message(f"Secondary queue channel set to <#{channel_id}>.", ephemeral=True)
        else:
            await interaction.response.send_message("No channel selected.", ephemeral=True)

    async def _clear_channel(self, interaction: discord.Interaction):
        await DatabaseHelper.update_game(self.game.game_id, secondary_queue_channel_id=None)
        await interaction.response.send_message("Secondary queue will use the main queue channel.", ephemeral=True)


class SecondaryScheduleView(ExpiringView):
    """Configure the secondary queue schedule (per-day open/close times)."""

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def __init__(self, cog: 'CustomMatch', game: GameConfig, parent_view: SecondaryQueueSettingsView):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game
        self.parent_view = parent_view

    @discord.ui.button(label="Add/Edit Day", style=discord.ButtonStyle.primary, row=0)
    async def add_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        options = [
            discord.SelectOption(label=self.DAY_NAMES[i], value=str(i))
            for i in range(7)
        ]
        view = discord.ui.View(timeout=60)
        select = discord.ui.Select(placeholder="Select a day to configure", options=options)

        async def on_select(inter: discord.Interaction):
            day = inter.data["values"][0]
            modal = SecondaryScheduleDayModal(self.cog, self.game, int(day), self.parent_view)
            await inter.response.send_modal(modal)

        select.callback = on_select
        view.add_item(select)
        await interaction.response.send_message("Select a day:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Day", style=discord.ButtonStyle.danger, row=0)
    async def remove_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        times = self.game.secondary_schedule_times or {}
        if not times:
            await interaction.response.send_message("No days configured.", ephemeral=True)
            return
        options = [
            discord.SelectOption(label=self.DAY_NAMES[int(k)], value=k)
            for k in sorted(times.keys())
        ]
        view = discord.ui.View(timeout=60)
        select = discord.ui.Select(placeholder="Select a day to remove", options=options)

        async def on_select(inter: discord.Interaction):
            day = inter.data["values"][0]
            times_copy = dict(self.game.secondary_schedule_times or {})
            times_copy.pop(day, None)
            await DatabaseHelper.update_game(
                self.game.game_id,
                secondary_schedule_times=json.dumps(times_copy) if times_copy else None,
            )
            self.game = await DatabaseHelper.get_game(self.game.game_id)
            self.parent_view.game = self.game
            await inter.response.send_message(f"Removed {self.DAY_NAMES[int(day)]} from schedule.", ephemeral=True)

        select.callback = on_select
        view.add_item(select)
        await interaction.response.send_message("Select a day to remove:", view=view, ephemeral=True)

    @discord.ui.button(label="Quick: Weekdays", style=discord.ButtonStyle.secondary, row=1)
    async def quick_weekdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SecondaryQuickScheduleModal(self.cog, self.game, list(range(5)), self.parent_view)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Quick: Every Day", style=discord.ButtonStyle.secondary, row=1)
    async def quick_everyday(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SecondaryQuickScheduleModal(self.cog, self.game, list(range(7)), self.parent_view)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Quick: Weekends", style=discord.ButtonStyle.secondary, row=1)
    async def quick_weekends(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SecondaryQuickScheduleModal(self.cog, self.game, [5, 6], self.parent_view)
        await interaction.response.send_modal(modal)


class SecondaryScheduleDayModal(discord.ui.Modal, title="Set Schedule Times"):
    open_time = discord.ui.TextInput(label="Open time (HH:MM, 24h)", required=True, placeholder="18:00")
    close_time = discord.ui.TextInput(label="Close time (blank = roll over)", required=False, placeholder="22:00 (blank = stays open next day)")

    def __init__(self, cog: 'CustomMatch', game: GameConfig, day: int, parent_view: SecondaryQueueSettingsView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.day = day
        self.parent_view = parent_view
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        self.title = f"Schedule: {day_names[day]}"

    async def on_submit(self, interaction: discord.Interaction):
        import re as _re
        time_re = _re.compile(r'^([01]?\d|2[0-3]):[0-5]\d$')
        open_val = self.open_time.value.strip()
        close_val = self.close_time.value.strip()
        if not time_re.match(open_val):
            await interaction.response.send_message("Invalid open time format. Use HH:MM (24h).", ephemeral=True)
            return
        open_val = open_val.zfill(5)
        entry = {"open": open_val}
        display_close = "roll over"
        if close_val:
            if not time_re.match(close_val):
                await interaction.response.send_message("Invalid close time format. Use HH:MM (24h).", ephemeral=True)
                return
            close_val = close_val.zfill(5)
            if close_val <= open_val:
                await interaction.response.send_message(
                    "Close time must be after open time (e.g. 18:00 - 22:00). "
                    "For all-day availability, use the 'All Day' option instead.",
                    ephemeral=True,
                )
                return
            entry["close"] = close_val
            display_close = close_val

        times = dict(self.game.secondary_schedule_times or {})
        times[str(self.day)] = entry
        await DatabaseHelper.update_game(
            self.game.game_id,
            secondary_schedule_times=json.dumps(times),
        )
        self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
        self.parent_view._rebuild_buttons()
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        await interaction.response.send_message(
            f"Set {day_names[self.day]}: {open_val} - {display_close}", ephemeral=True
        )


class SecondaryQuickScheduleModal(discord.ui.Modal, title="Quick Schedule Setup"):
    open_time = discord.ui.TextInput(label="Open time (HH:MM, 24h)", required=True, placeholder="18:00")
    close_time = discord.ui.TextInput(label="Close time (blank = roll over)", required=False, placeholder="22:00 (blank = stays open next day)")

    def __init__(self, cog: 'CustomMatch', game: GameConfig, days: list, parent_view: SecondaryQueueSettingsView):
        super().__init__()
        self.cog = cog
        self.game = game
        self.days = days
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        import re as _re
        time_re = _re.compile(r'^([01]?\d|2[0-3]):[0-5]\d$')
        open_val = self.open_time.value.strip()
        close_val = self.close_time.value.strip()
        if not time_re.match(open_val):
            await interaction.response.send_message("Invalid open time format. Use HH:MM (24h).", ephemeral=True)
            return
        open_val = open_val.zfill(5)
        entry = {"open": open_val}
        if close_val:
            if not time_re.match(close_val):
                await interaction.response.send_message("Invalid close time format. Use HH:MM (24h).", ephemeral=True)
                return
            close_val = close_val.zfill(5)
            if close_val <= open_val:
                await interaction.response.send_message(
                    "Close time must be after open time (e.g. 18:00 - 22:00). "
                    "For all-day availability, use the 'All Day' option instead.",
                    ephemeral=True,
                )
                return
            entry["close"] = close_val

        times = dict(self.game.secondary_schedule_times or {})
        for day in self.days:
            times[str(day)] = entry
        await DatabaseHelper.update_game(
            self.game.game_id,
            secondary_schedule_times=json.dumps(times),
        )
        self.parent_view.game = await DatabaseHelper.get_game(self.game.game_id)
        self.parent_view._rebuild_buttons()
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_list = ", ".join(day_names[d] for d in self.days)
        await interaction.response.send_message(
            f"Set {day_list}: {open_val} - {close_val}", ephemeral=True
        )
