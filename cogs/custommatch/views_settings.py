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
    is_valorant_game, is_rivals_game, COLOR_NEUTRAL, resolve_ocr_ign,
)
from .database import DatabaseHelper

if TYPE_CHECKING:
    from .cog import CustomMatch
    from .api_clients import RivalsScoreboardResult, RivalsVisionClient

logger = logging.getLogger('custommatch')


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
        logger.error(f"Error in {self.__class__.__name__}.{item.callback.__name__}: {error}", exc_info=True)
        try:
            msg = "An error occurred. Please try again."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass


class ConfirmView(discord.ui.View):
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


class RivalsSettingsView(discord.ui.View):
    """Sub-view opened from SettingsView for Marvel Rivals stats config."""

    def __init__(self, cog):
        super().__init__(timeout=300)
        self.cog = cog
        self.add_item(RivalsAdminChannelSelect(self))
        self.add_item(RivalsBlacklistUserSelect(self, action="add"))
        self.add_item(RivalsBlacklistUserSelect(self, action="remove"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
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


class _ConfirmLinkResolverView(discord.ui.View):
    """Yes/No confirmation for linking an IGN to a user who isn't in the match roster."""

    def __init__(self, parent: "RivalsIGNResolverView", interaction_user_id: int, selected_user_id: int):
        super().__init__(timeout=120)
        self.parent = parent
        self.interaction_user_id = interaction_user_id
        self.selected_user_id = selected_user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
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


class RivalsIGNResolverView(discord.ui.View):
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

        # Render + post results card to log channel (lobby is already gone)
        try:
            red_players = [
                {**r, "medal_count": sum((r.get("medals") or {}).values())}
                for r in self.rows_out if (r.get("team") or "").upper() == "RED"
            ]
            blue_players = [
                {**r, "medal_count": sum((r.get("medals") or {}).values())}
                for r in self.rows_out if (r.get("team") or "").upper() == "BLUE"
            ]
            image_buf = await self.cog.stats_generator.generate_rivals_results_image(
                red_players=red_players,
                blue_players=blue_players,
                winning_team=self.winning_team or "",
            )
            if image_buf:
                log_channel_id = await DatabaseHelper.get_config("log_channel_id")
                if log_channel_id:
                    log_ch = self.guild.get_channel(int(log_channel_id))
                    if log_ch:
                        await log_ch.send(
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

class BlacklistActionView(discord.ui.View):
    """Consolidated view for blacklist actions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add", style=discord.ButtonStyle.danger)
    async def add_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BlacklistUserSelectView(self.cog)
        await interaction.response.send_message("Select a player to blacklist:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.success)
    async def remove_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        if not blacklisted:
            await interaction.response.send_message("No blacklisted players.", ephemeral=True)
            return
        view = UnblacklistSelectView(self.cog, blacklisted, interaction.guild)
        await interaction.response.send_message("Select a player to unblacklist:", view=view, ephemeral=True)

    @discord.ui.button(label="View", style=discord.ButtonStyle.secondary)
    async def view_blacklist(self, interaction: discord.Interaction, button: discord.ui.Button):
        blacklisted = await DatabaseHelper.get_blacklisted_players()
        if not blacklisted:
            await interaction.response.send_message("No blacklisted players.", ephemeral=True)
            return

        lines = ["**Blacklisted Players**\n"]
        now = datetime.now(timezone.utc)
        for player_id, until in blacklisted:
            if until > now:
                user = interaction.guild.get_member(player_id)
                name = user.display_name if user else str(player_id)
                if until.year == 2099:
                    lines.append(f"• {name}: Permanent")
                else:
                    lines.append(f"• {name}: Until {until.strftime('%Y-%m-%d %H:%M')} UTC")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)


class GameManagementView(discord.ui.View):
    """Consolidated view for game management actions."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add", style=discord.ButtonStyle.success)
    async def add_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AddGameModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary)
    async def edit_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_edit_game_modal))
        await interaction.response.send_message("Select a game to edit:", view=view, ephemeral=True)

    async def show_edit_game_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = EditGameModal(self.cog, game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger)
    async def remove_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.confirm_remove_game))
        await interaction.response.send_message("Select a game to remove:", view=view, ephemeral=True)

    async def confirm_remove_game(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = ConfirmView()
        await interaction.response.send_message(
            f"Are you sure you want to remove **{game.name}**? This cannot be undone.",
            view=view, ephemeral=True
        )
        await view.wait()
        if view.value:
            await DatabaseHelper.delete_game(game_id)
            await interaction.followup.send(f"Removed **{game.name}**.", ephemeral=True)

    @discord.ui.button(label="Set Banner", style=discord.ButtonStyle.secondary)
    async def set_banner(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_banner_modal))
        await interaction.response.send_message("Select a game to set banner URL:", view=view, ephemeral=True)

    async def show_banner_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = SetBannerModal(self.cog, game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Category", style=discord.ButtonStyle.secondary)
    async def set_game_category(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_category_select))
        await interaction.response.send_message("Select a game to set its match category:", view=view, ephemeral=True)

    async def show_category_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = GameCategorySelectView(self.cog, game)
        current = f" (current: <#{game.category_id}>)" if game.category_id else ""
        await interaction.response.send_message(
            f"Select the category for **{game.name}** match channels{current}:",
            view=view, ephemeral=True
        )


class SetBannerModal(discord.ui.Modal, title="Set Queue Banner"):
    banner_url = discord.ui.TextInput(
        label="Banner URL (leave blank to clear)",
        placeholder="https://example.com/banner.png or .gif",
        required=False,
        style=discord.TextStyle.short
    )

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        if game.banner_url:
            self.banner_url.default = game.banner_url

    async def on_submit(self, interaction: discord.Interaction):
        url = self.banner_url.value.strip() if self.banner_url.value else None
        await DatabaseHelper.update_game(self.game.game_id, banner_url=url)
        if url:
            await interaction.response.send_message(f"Banner set for **{self.game.name}**.", ephemeral=True)
        else:
            await interaction.response.send_message(f"Banner cleared for **{self.game.name}**.", ephemeral=True)


class GameCategorySelectView(discord.ui.View):
    """View for selecting a per-game category channel."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=60)
        self.cog = cog
        self.game = game

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        placeholder="Select a category...",
        channel_types=[discord.ChannelType.category]
    )
    async def category_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        category = select.values[0]
        await DatabaseHelper.update_game(self.game.game_id, category_id=category.id)
        await interaction.response.edit_message(
            content=f"Category for **{self.game.name}** set to **{category.name}**.",
            view=None
        )


class ChannelSettingsView(discord.ui.View):
    """Consolidated view for channel settings."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Log Channel", style=discord.ButtonStyle.secondary)
    async def log_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = LogChannelSelectView(self.cog)
        await interaction.response.send_message("Select a log channel:", view=view, ephemeral=True)

    @discord.ui.button(label="Admin Channel", style=discord.ButtonStyle.secondary)
    async def admin_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        current_id = await DatabaseHelper.get_config("cm_admin_channel_id")
        current = interaction.guild.get_channel(int(current_id)) if current_id else None
        current_str = current.mention if current else "Not set"
        view = AdminChannelSelectView(self.cog)
        await interaction.response.send_message(
            f"Current admin channel: {current_str}\n"
            f"(Stats fetch failure notifications will be sent here)\n\nSelect a new channel:",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Game Channel", style=discord.ButtonStyle.secondary)
    async def game_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_game_channel_select))
        await interaction.response.send_message("Select a game to set game channel:", view=view, ephemeral=True)

    async def show_game_channel_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = GameChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.game_channel_id) if game.game_channel_id else None
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current game channel for **{game.name}**: {current_str}\n"
            f"(Match results will be posted here)\n\nSelect a new channel:",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="LF1 Channel", style=discord.ButtonStyle.secondary)
    async def lf1_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_lf1_channel_select))
        await interaction.response.send_message("Select a game to set LF1 channel:", view=view, ephemeral=True)

    async def show_lf1_channel_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = LF1ChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.lf1_channel_id) if game.lf1_channel_id else None
        if not current and game.lf1_channel_id:
            current = interaction.guild.get_thread(game.lf1_channel_id)
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current LF1 channel for **{game.name}**: {current_str}\n"
            f"(\"Looking for 1\" notifications sent here when queue needs 1 more player)\n"
            f"30-minute cooldown per game, messages auto-delete after 20 minutes.\n\nSelect a new channel:",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Discussion Channel", style=discord.ButtonStyle.secondary)
    async def discussion_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        current_id = await DatabaseHelper.get_config("cm_discussion_parent_channel_id")
        current = interaction.guild.get_channel(int(current_id)) if current_id else None
        current_str = current.mention if current else "Not set"
        view = DiscussionParentChannelSelectView(self.cog)
        await interaction.response.send_message(
            f"Current discussion parent channel: {current_str}\n"
            f"(Private discussion threads with suspended users will be created here)\n\nSelect a new channel:",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Leaderboard", style=discord.ButtonStyle.secondary)
    async def leaderboard_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_leaderboard_channel_select))
        await interaction.response.send_message("Select a game to set leaderboard channel:", view=view, ephemeral=True)

    async def show_leaderboard_channel_select(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = LeaderboardChannelSelectView(self.cog, game_id)
        current = interaction.guild.get_channel(game.leaderboard_channel_id) if game.leaderboard_channel_id else None
        current_str = current.mention if current else "Not set"
        await interaction.response.send_message(
            f"Current leaderboard channel for **{game.name}**: {current_str}\n"
            f"(Persistent leaderboard will be posted here)\n\nSelect a new channel:",
            view=view, ephemeral=True
        )


# =============================================================================
# SETTINGS PANEL
# =============================================================================

class SettingsView(discord.ui.View):
    """Main settings panel for server admins."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=300)
        self.cog = cog

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Re-verify admin role on every button press."""
        if await self.cog.is_cm_admin(interaction.user):
            return True
        await interaction.response.send_message("You no longer have permission to use this panel.", ephemeral=True)
        return False

    @discord.ui.button(label="Set Channels", style=discord.ButtonStyle.secondary, row=0)
    async def set_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ChannelSettingsView(self.cog)
        await interaction.response.send_message("Select channel type to configure:", view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Role", style=discord.ButtonStyle.secondary, row=0)
    async def set_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AdminRoleSelectView(self.cog)
        await interaction.response.send_message("Select the CM Admin role:", view=view, ephemeral=True)

    @discord.ui.button(label="Games", style=discord.ButtonStyle.primary, row=1)
    async def games_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = GameManagementView(self.cog)
        await interaction.response.send_message("Select game action:", view=view, ephemeral=True)

    @discord.ui.button(label="Secondary Queue", style=discord.ButtonStyle.primary, row=1)
    async def secondary_queue_setting(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self._show_secondary_queue_settings))
        await interaction.response.send_message("Select a game to configure secondary queue:", view=view, ephemeral=True)

    async def _show_secondary_queue_settings(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = SecondaryQueueSettingsView(self.cog, game)
        embed = await view.build_status_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Configure MMR Roles", style=discord.ButtonStyle.primary, row=1)
    async def config_mmr_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_mmr_roles_panel))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    async def show_mmr_roles_panel(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        mmr_roles = await DatabaseHelper.get_mmr_roles_with_labels(game_id)

        lines = [f"**MMR Roles for {game.name}**\n"]
        if mmr_roles:
            for role_id, data in sorted(mmr_roles.items(), key=lambda x: x[1]['mmr'], reverse=True):
                role = interaction.guild.get_role(role_id)
                role_name = role.name if role else f"Unknown ({role_id})"
                label_str = f" — **{data['label']}**" if data['label'] else ""
                lines.append(f"• {role_name}: {data['mmr']} MMR{label_str}")
        else:
            lines.append("No MMR roles configured.")

        view = MMRRolesView(self.cog, game_id)
        await interaction.response.send_message("\n".join(lines), view=view, ephemeral=True)

    @discord.ui.button(label="Blacklist", style=discord.ButtonStyle.danger, row=2)
    async def blacklist_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = BlacklistActionView(self.cog)
        await interaction.response.send_message("Select blacklist action:", view=view, ephemeral=True)

    @discord.ui.button(label="Mod Roles", style=discord.ButtonStyle.secondary, row=2)
    async def mod_roles_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        lines = ["**Mod Roles**\n", "These roles get permission to type and manage messages in all match channels.\n"]
        if mod_role_ids:
            for role_id in mod_role_ids:
                role = interaction.guild.get_role(role_id)
                role_name = role.name if role else f"Unknown ({role_id})"
                lines.append(f"• {role_name}")
        else:
            lines.append("No mod roles configured.")
        view = ModRolesView(self.cog)
        await interaction.response.send_message("\n".join(lines), view=view, ephemeral=True)

    @discord.ui.button(label="Emojis", style=discord.ButtonStyle.secondary, row=2)
    async def emojis_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = EmojisSubView(self.cog)
        await interaction.response.send_message("Select which emojis to configure:", view=view, ephemeral=True)

    @discord.ui.button(label="Fix Match", style=discord.ButtonStyle.danger, row=2)
    async def fix_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        from .views_gameplay import FixMatchModal
        modal = FixMatchModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Cleanup Stale Matches", style=discord.ButtonStyle.danger, row=2)
    async def cleanup_orphans(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clean up stale/stuck matches that are missing channels or roles."""
        await interaction.response.defer(ephemeral=True)

        count, details = await self.cog.manual_orphan_cleanup(interaction.guild)

        if count == 0:
            await interaction.followup.send("No stale matches found to clean up.", ephemeral=True)
        else:
            detail_text = "\n".join(details[:10])  # Max 10 entries
            if len(details) > 10:
                detail_text += f"\n... and {len(details) - 10} more"
            await interaction.followup.send(
                f"**Cleaned up {count} stale matches:**\n{detail_text}",
                ephemeral=True
            )

    # Row 3: Game settings
    @discord.ui.button(label="Game Toggles", style=discord.ButtonStyle.primary, row=3)
    async def game_toggles(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_game_toggles))
        await interaction.response.send_message("Select a game to configure toggles:", view=view, ephemeral=True)

    async def show_game_toggles(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        view = GameTogglesView(self.cog, game)
        embed = discord.Embed(title=f"{game.name} Toggles", color=COLOR_NEUTRAL)
        embed.add_field(name="VC Creation", value="Enabled" if game.vc_creation_enabled else "Disabled", inline=True)
        embed.add_field(name="Queue Role Required", value="Yes" if game.queue_role_required else "No", inline=True)
        embed.add_field(name="DM Ready-Up", value="Enabled" if game.dm_ready_up else "Disabled", inline=True)
        embed.add_field(name="IGN Required", value="Yes" if game.ign_required else "No", inline=True)
        embed.add_field(name="Role Prefs Required", value="Yes" if game.role_required else "No", inline=True)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Ready Timer", style=discord.ButtonStyle.primary, row=3)
    async def ready_timer_setting(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_ready_timer_modal))
        await interaction.response.send_message("Select a game to configure ready timer:", view=view, ephemeral=True)

    async def show_ready_timer_modal(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)
        modal = ReadyTimerModal(self.cog, game)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Queue Schedule", style=discord.ButtonStyle.primary, row=3)
    async def queue_schedule_setting(self, interaction: discord.Interaction, button: discord.ui.Button):
        print("[CUSTOMMATCH] queue_schedule_setting button clicked", flush=True)
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_schedule_settings))
        await interaction.response.send_message("Select a game to configure queue schedule:", view=view, ephemeral=True)

    async def show_schedule_settings(self, interaction: discord.Interaction, game_id: int):
        logger.debug(f"show_schedule_settings called, game_id={game_id}")
        game = await DatabaseHelper.get_game(game_id)
        logger.debug(f"Creating QueueScheduleView for {game.name}")
        view = QueueScheduleView(self.cog, game)
        logger.debug(f"QueueScheduleView created with {len(view.children)} children")
        embed = discord.Embed(title=f"{game.name} Queue Schedule", color=COLOR_NEUTRAL)
        embed.add_field(name="Enabled", value="Yes" if game.schedule_enabled else "No", inline=False)

        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        # Show per-day schedule times if available
        if game.schedule_times:
            schedule_lines = []
            for day_num, times in sorted(game.schedule_times.items(), key=lambda x: int(x[0])):
                day_name = day_names[int(day_num)]
                mode = times.get("mode")
                if mode == "open":
                    schedule_lines.append(f"**{day_name}:** Open all day")
                elif mode == "closed":
                    schedule_lines.append(f"**{day_name}:** Closed all day")
                else:
                    open_time = times.get("open")
                    close_time = times.get("close")
                    if open_time and close_time:
                        schedule_lines.append(f"**{day_name}:** {open_time} - {close_time}")
                    elif open_time:
                        schedule_lines.append(f"**{day_name}:** Opens at {open_time} →")
                    elif close_time:
                        schedule_lines.append(f"**{day_name}:** → Closes at {close_time}")
            if schedule_lines:
                embed.add_field(name="Schedule", value="\n".join(schedule_lines), inline=False)
            else:
                embed.add_field(name="Schedule", value="No days configured", inline=False)
        # Fall back to legacy format
        elif game.schedule_open_days:
            days = game.schedule_open_days.split(",")
            open_days_str = ", ".join(day_names[int(d)] for d in days if d.isdigit())
            embed.add_field(name="Open Days", value=open_days_str, inline=True)
            embed.add_field(name="Open Time", value=game.schedule_open_time or "Not set", inline=True)
            embed.add_field(name="Close Time", value=game.schedule_close_time or "Not set", inline=True)
        else:
            embed.add_field(name="Schedule", value="Not configured", inline=False)

        embed.set_footer(text="Times are in server timezone (bot host time)")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Set Admin Offset", style=discord.ButtonStyle.secondary, row=3)
    async def set_admin_offset(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_offset_modal))
        await interaction.response.send_message("Select a game:", view=view, ephemeral=True)

    async def show_offset_modal(self, interaction: discord.Interaction, game_id: int):
        modal = SetAdminOffsetModal(self.cog, game_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Rivals Stats", style=discord.ButtonStyle.primary, row=3)
    async def rivals_stats_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RivalsSettingsView(self.cog)
        embed = await view.build_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Mass Register", style=discord.ButtonStyle.success, row=4)
    async def mass_register(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.do_mass_register))
        await interaction.response.send_message(
            "**Mass Register Players**\n"
            "This will scan all server members and register anyone with an MMR role for the selected game.\n\n"
            "Select a game:",
            view=view, ephemeral=True
        )

    async def do_mass_register(self, interaction: discord.Interaction, game_id: int):
        await interaction.response.defer(ephemeral=True)

        game = await DatabaseHelper.get_game(game_id)
        if not game:
            await interaction.followup.send("Game not found.", ephemeral=True)
            return

        # Get MMR roles for this game (returns {role_id: mmr_value})
        role_mmr_map = await DatabaseHelper.get_mmr_roles(game_id)
        if not role_mmr_map:
            await interaction.followup.send(
                f"No MMR roles configured for **{game.name}**. "
                "Set up MMR roles first in Game Settings.",
                ephemeral=True
            )
            return

        # Scan all members
        registered = 0
        corrected = 0
        skipped = 0
        errors = []
        corrections = []

        for member in interaction.guild.members:
            if member.bot:
                continue

            # Find highest MMR role this member has
            member_mmr = None
            for role in member.roles:
                if role.id in role_mmr_map:
                    role_mmr = role_mmr_map[role.id]
                    if member_mmr is None or role_mmr > member_mmr:
                        member_mmr = role_mmr

            if member_mmr is not None:
                try:
                    stats = await DatabaseHelper.get_player_stats(member.id, game_id)
                    if stats.mmr != member_mmr and stats.games_played == 0:
                        # New player or MMR mismatch with no games — set to role value
                        stats.mmr = member_mmr
                        await DatabaseHelper.update_player_stats(stats)
                        registered += 1
                    elif stats.mmr != member_mmr and stats.games_played > 0:
                        # Existing player with mismatched MMR — correct and log
                        old_mmr = stats.mmr
                        stats.mmr = member_mmr
                        await DatabaseHelper.update_player_stats(stats)
                        corrected += 1
                        corrections.append(f"{member.display_name}: {old_mmr} → {member_mmr}")
                    else:
                        skipped += 1  # MMR already correct
                except Exception as e:
                    errors.append(f"{member.display_name}: {e}")

        result = f"**Mass Registration Complete for {game.name}**\n"
        result += f"Registered: {registered} players\n"
        if corrected > 0:
            result += f"Corrected: {corrected} players\n"
        result += f"Skipped (MMR already correct): {skipped} players\n"

        if corrections:
            result += f"\nMMR Corrections ({len(corrections)}):\n"
            result += "\n".join(corrections[:10])
            if len(corrections) > 10:
                result += f"\n... and {len(corrections) - 10} more"

        if errors:
            result += f"\nErrors ({len(errors)}):\n"
            result += "\n".join(errors[:5])
            if len(errors) > 5:
                result += f"\n... and {len(errors) - 5} more"

        await interaction.followup.send(result, ephemeral=True)

    @discord.ui.button(label="Test Stats Card", style=discord.ButtonStyle.secondary, row=4)
    async def test_stats_card(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = TestStatsCardView(self.cog, interaction.user)
        await interaction.response.send_message(
            "**Test Stats Card Generator**\n\n"
            "Choose a test mode:\n"
            "• **Random Data** - Generate card with sample/random stats\n"
            "• **My Stats** - Use your actual CM stats (requires a game with stats)\n",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Wipe Stats", style=discord.ButtonStyle.danger, row=4)
    async def wipe_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Wipe all player stats except MMR."""
        view = StatsWipeConfirmView(self.cog)
        await interaction.response.send_message(
            "**⚠️ DANGER: Wipe All Stats**\n\n"
            "This will reset the following for ALL players:\n"
            "• Wins, Losses, Games Played\n"
            "• Valorant match stats (K/D/A, damage, etc.)\n"
            "• Rivalries and teammate stats\n\n"
            "**MMR will be preserved.**\n\n"
            "Are you sure you want to continue?",
            view=view, ephemeral=True
        )

    @discord.ui.button(label="Fetch Match Stats", style=discord.ButtonStyle.primary, row=4)
    async def fetch_match_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Fetch/re-fetch Valorant stats for a specific match."""
        from .views_gameplay import FetchStatsModal
        modal = FetchStatsModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Refetch Stats", style=discord.ButtonStyle.primary, row=4)
    async def refetch_all_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Re-fetch stats for matches."""
        from .views_gameplay import RefetchModeSelectView
        view = RefetchModeSelectView(self.cog)
        await interaction.response.send_message(
            "**Refetch Valorant Stats**\n\n"
            "Choose a mode:\n"
            "• **Incomplete Only** - Only matches missing player stats\n"
            "• **Force All** - Re-fetch ALL recent matches (overwrites existing stats)",
            view=view,
            ephemeral=True
        )


class StatsWipeConfirmView(discord.ui.View):
    """Confirmation view for wiping stats."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Stats wipe cancelled.", view=None)

    @discord.ui.button(label="Yes, Wipe Stats", style=discord.ButtonStyle.danger)
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

            await interaction.edit_original_response(
                content="✅ **Stats have been wiped.**\n\nAll wins, losses, and match data have been reset. MMR has been preserved.",
                view=None
            )
        except Exception as e:
            logger.error(f"Stats wipe error: {e}")
            await interaction.edit_original_response(
                content=f"Error wiping stats: {e}",
                view=None
            )


class TestStatsCardView(discord.ui.View):
    """View for testing stats card generation."""

    def __init__(self, cog: 'CustomMatch', user: discord.Member):
        super().__init__(timeout=120)
        self.cog = cog
        self.user = user

    @discord.ui.button(label="Random Data", style=discord.ButtonStyle.primary)
    async def test_random(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Check if stats generator is available
        if not self.cog.stats_generator.browser:
            await interaction.followup.send(
                "**Stats Card Generator Status: NOT WORKING**\n\n"
                "The Playwright browser is not initialized.\n"
                "Possible causes:\n"
                "• Playwright is not installed (`pip install playwright`)\n"
                "• Browser not installed (`playwright install chromium`)\n"
                "• Initialization failed on cog load\n\n"
                "Stats will fall back to embed-only mode.",
                ephemeral=True
            )
            return

        # Generate random test data
        import random
        wins = random.randint(15, 50)
        losses = random.randint(10, 40)

        # Generate random recent matches for dropdown
        recent_matches = [
            {'match_id': 1, 'short_id': 'ABC12', 'won': True, 'map_name': 'Ascent', 'agent': 'Jett', 'kills': 25, 'deaths': 15, 'assists': 5, 'team': 'red', 'winning_team': 'red'},
            {'match_id': 2, 'short_id': 'DEF34', 'won': False, 'map_name': 'Bind', 'agent': 'Reyna', 'kills': 18, 'deaths': 20, 'assists': 3, 'team': 'red', 'winning_team': 'blue'},
            {'match_id': 3, 'short_id': 'GHI56', 'won': True, 'map_name': 'Haven', 'agent': 'Omen', 'kills': 22, 'deaths': 12, 'assists': 8, 'team': 'blue', 'winning_team': 'blue'},
            {'match_id': 4, 'short_id': 'JKL78', 'won': True, 'map_name': 'Split', 'agent': 'Jett', 'kills': 30, 'deaths': 10, 'assists': 2, 'team': 'red', 'winning_team': 'red'},
            {'match_id': 5, 'short_id': 'MNO90', 'won': False, 'map_name': 'Icebox', 'agent': 'Sage', 'kills': 12, 'deaths': 18, 'assists': 15, 'team': 'blue', 'winning_team': 'red'},
        ]

        test_data = {
            'avatar_url': str(self.user.display_avatar.url),
            'player_name': safe_display_name(self.user),
            'period_title': 'Test Card - Random Data',
            'games_played': wins + losses,
            'wins': wins,
            'losses': losses,
            'total_kills': random.randint(300, 1000),
            'total_deaths': random.randint(200, 800),
            'total_assists': random.randint(100, 500),
            'total_score': random.randint(10000, 50000),
            'total_damage': random.randint(50000, 150000),
            'total_first_bloods': random.randint(10, 50),
            'hs_percent': random.randint(18, 32),
            'longest_win_streak': random.randint(3, 8),
            'longest_loss_streak': random.randint(2, 5),
            'agent_stats': {
                'Jett': {'games': 15, 'wins': 9, 'losses': 6},
                'Reyna': {'games': 12, 'wins': 8, 'losses': 4},
                'Omen': {'games': 8, 'wins': 4, 'losses': 4},
                'Sage': {'games': 5, 'wins': 2, 'losses': 3},
                'Sova': {'games': 4, 'wins': 3, 'losses': 1},
                'Killjoy': {'games': 3, 'wins': 2, 'losses': 1},
                'Viper': {'games': 3, 'wins': 1, 'losses': 2},
                'Breach': {'games': 2, 'wins': 1, 'losses': 1},
                'Cypher': {'games': 2, 'wins': 2, 'losses': 0},
                'Phoenix': {'games': 1, 'wins': 0, 'losses': 1},
            },
            'map_stats': [
                {'name': 'Ascent', 'games': 10, 'wins': 7, 'losses': 3, 'winrate': 70.0},
                {'name': 'Haven', 'games': 8, 'wins': 5, 'losses': 3, 'winrate': 62.5},
                {'name': 'Bind', 'games': 7, 'wins': 4, 'losses': 3, 'winrate': 57.1},
                {'name': 'Split', 'games': 6, 'wins': 3, 'losses': 3, 'winrate': 50.0},
                {'name': 'Icebox', 'games': 5, 'wins': 2, 'losses': 3, 'winrate': 40.0},
                {'name': 'Pearl', 'games': 4, 'wins': 2, 'losses': 2, 'winrate': 50.0},
                {'name': 'Lotus', 'games': 3, 'wins': 1, 'losses': 2, 'winrate': 33.3},
                {'name': 'Abyss', 'games': 2, 'wins': 0, 'losses': 2, 'winrate': 0.0},
            ],
            'recent_matches': recent_matches,
            'best_teammates': [
                {'name': 'User 1', 'wins': 18, 'losses': 3},
                {'name': 'User 2', 'wins': 13, 'losses': 8},
                {'name': 'User 3', 'wins': 9, 'losses': 8},
            ],
            'worst_teammates': [
                {'name': 'User 1', 'wins': 18, 'losses': 31},
                {'name': 'User 2', 'wins': 13, 'losses': 21},
                {'name': 'User 3', 'wins': 9, 'losses': 18},
            ]
        }

        try:
            # Generate seasonal (random) and lifetime (same but different title) images
            seasonal_image = await self.cog.stats_generator.generate_stats_image(test_data)
            test_data['period_title'] = 'Test Card - Lifetime'
            lifetime_image = await self.cog.stats_generator.generate_stats_image(test_data)

            if seasonal_image and lifetime_image:
                images = {'seasonal': seasonal_image, 'lifetime': lifetime_image}
                # Create a mock game config for the view
                class MockGame:
                    name = "Test Game"
                view = TestStatsImageView(
                    self.cog, self.user, MockGame(), images, recent_matches,
                    invoker_id=interaction.user.id
                )

                images['seasonal'].seek(0)
                file = discord.File(images['seasonal'], filename='stats_seasonal.png')
                embed = discord.Embed(
                    title="Stats Card Test - SUCCESS",
                    description="Use the dropdown to switch between views.",
                    color=0x00ff00
                )
                embed.set_image(url="attachment://stats_seasonal.png")
                await interaction.followup.send(embed=embed, file=file, view=view, ephemeral=True)
            else:
                await interaction.followup.send(
                    "**Stats Card Test - FAILED**\n\n"
                    "The generator returned None. Check the logs for errors.\n"
                    "Common issues:\n"
                    "• Template file missing (`cogs/templates/stats_card.html`)\n"
                    "• Font files missing (`fonts/` directory)\n"
                    "• HTML template syntax errors",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.followup.send(
                f"**Stats Card Test - ERROR**\n\n"
                f"Exception: `{type(e).__name__}: {e}`\n\n"
                "Check the bot logs for full traceback.",
                ephemeral=True
            )

    @discord.ui.button(label="My Stats", style=discord.ButtonStyle.success)
    async def test_my_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self.show_my_stats))
        await interaction.response.send_message("Select a game to view your stats:", view=view, ephemeral=True)

    async def show_my_stats(self, interaction: discord.Interaction, game_id: int):
        await interaction.response.defer()

        # Check if stats generator is available
        if not self.cog.stats_generator.browser:
            await interaction.followup.send(
                "**Stats Card Generator Status: NOT WORKING**\n\n"
                "Playwright browser not initialized. Stats will use embeds only.",
                ephemeral=True
            )
            return

        game = await DatabaseHelper.get_game(game_id)
        stats = await DatabaseHelper.get_player_stats(self.user.id, game_id)
        valorant_stats = await DatabaseHelper.get_valorant_player_stats(self.user.id, game_id, monthly=False)
        recent_matches_raw = await DatabaseHelper.get_player_recent_matches(self.user.id, game_id, limit=5)
        streak_stats = await DatabaseHelper.get_player_streak_stats(self.user.id, game_id, monthly=False)
        teammate_stats = await DatabaseHelper.get_all_teammate_stats(self.user.id, game_id, monthly=False)

        # Format recent matches
        recent_matches = []
        for match in recent_matches_raw:
            won = match.get('team') == match.get('winning_team')
            recent_matches.append({
                'won': won,
                'kills': match.get('kills'),
                'deaths': match.get('deaths'),
                'assists': match.get('assists'),
                'agent': match.get('agent'),
                'map_name': match.get('map_name')
            })

        # Resolve teammate names
        def get_teammate_name(player_id: int) -> str:
            m = interaction.guild.get_member(player_id)
            if m:
                name = m.display_name
                return name[:12] + '...' if len(name) > 15 else name
            return f"User {player_id}"

        best_teammates = []
        for t in teammate_stats.get('best_teammates', []):
            best_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        worst_teammates = []
        for t in teammate_stats.get('worst_teammates', []):
            worst_teammates.append({
                'name': get_teammate_name(t['player_id']),
                'wins': t['wins'],
                'losses': t['losses']
            })

        test_data = {
            'avatar_url': str(self.user.display_avatar.url),
            'player_name': safe_display_name(self.user),
            'period_title': f'{game.name} Stats (Your Data)',
            'games_played': stats.games_played,
            'wins': stats.wins,
            'losses': stats.losses,
            'total_kills': valorant_stats.get('total_kills', 0),
            'total_deaths': valorant_stats.get('total_deaths', 0),
            'total_assists': valorant_stats.get('total_assists', 0),
            'total_score': valorant_stats.get('total_score', 0),
            'total_damage': valorant_stats.get('total_damage', 0),
            'total_first_bloods': valorant_stats.get('total_first_bloods', 0),
            'hs_percent': valorant_stats.get('hs_percent', 0),
            'longest_win_streak': streak_stats.get('longest_win_streak', 0),
            'longest_loss_streak': streak_stats.get('longest_loss_streak', 0),
            'agent_stats': valorant_stats.get('agent_stats', {}),
            'map_stats': valorant_stats.get('map_stats', []),
            'recent_matches': recent_matches,
            'best_teammates': best_teammates,
            'worst_teammates': worst_teammates
        }

        try:
            image = await self.cog.stats_generator.generate_stats_image(test_data)
            if image:
                file = discord.File(image, filename='my_stats.png')
                embed = discord.Embed(
                    title=f"Your {game.name} Stats Card",
                    color=0x00ff00
                )
                embed.set_image(url="attachment://my_stats.png")
                await interaction.followup.send(embed=embed, file=file, ephemeral=True)
            else:
                # Fallback to embed
                embed = discord.Embed(
                    title=f"Your {game.name} Stats (Embed Fallback)",
                    description="Image generation failed. Showing embed instead.",
                    color=0xff9900
                )
                embed.add_field(name="W/L", value=f"{stats.wins}/{stats.losses}", inline=True)
                embed.add_field(name="Games", value=str(stats.games_played), inline=True)
                tk = valorant_stats.get('total_kills', 0)
                td = valorant_stats.get('total_deaths', 0)
                ta = valorant_stats.get('total_assists', 0)
                if tk > 0:
                    embed.add_field(name="K/D/A", value=f"{tk}/{td}/{ta}", inline=True)
                await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"**Error:** `{e}`", ephemeral=True)


class TestStatsSelectDropdown(discord.ui.Select):
    """Dropdown for selecting test stats view."""

    def __init__(self, recent_matches: List[dict]):
        options = [
            discord.SelectOption(label="Monthly", value="seasonal", description="Monthly test stats", default=True),
            discord.SelectOption(label="Lifetime", value="lifetime", description="Lifetime test stats"),
        ]
        # Add recent matches as options
        for i, match in enumerate(recent_matches[:5]):
            map_name = match.get('map_name') or "Unknown"
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


class TestStatsImageView(discord.ui.View):
    """View for test stats card image with dropdown selection."""

    def __init__(self, cog: 'CustomMatch', member: discord.Member, game,
                 images: Dict[str, io.BytesIO], recent_matches: List[dict], invoker_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.member = member
        self.game = game
        self.images = images
        self.recent_matches = recent_matches
        self.invoker_id = invoker_id
        self.current = 'seasonal'
        self.add_item(TestStatsSelectDropdown(recent_matches))

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
            if isinstance(item, TestStatsSelectDropdown):
                for option in item.options:
                    option.default = (option.value == value)

        if value in self.images:
            self.images[value].seek(0)
            filename = f'stats_{value}.png'
            file = discord.File(self.images[value], filename=filename)
            embed = discord.Embed(
                title="Stats Card Test - SUCCESS",
                description="Use the dropdown to switch between views.",
                color=0x00ff00
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

            import random
            map_name = match_data.get('map_name') or "Unknown"
            kills = match_data.get('kills', 0) or 0
            deaths = match_data.get('deaths', 0) or 0
            assists = match_data.get('assists', 0) or 0
            agent = match_data.get('agent', 'Unknown')
            won = match_data.get('team') == match_data.get('winning_team')

            # Generate random test data for scoreboard
            headshots = random.randint(5, 15)
            bodyshots = random.randint(20, 50)
            legshots = random.randint(2, 10)
            scoreboard_data = {
                'player_name': safe_display_name(self.member),
                'avatar_url': self.member.display_avatar.url,
                'map_name': map_name,
                'agent': agent,
                'won': won,
                'kills': kills,
                'deaths': deaths,
                'assists': assists,
                'score': random.randint(3000, 7000),
                'damage': random.randint(2000, 5000),
                'first_bloods': random.randint(0, 3),
                'headshots': headshots,
                'bodyshots': bodyshots,
                'legshots': legshots,
                'plants': random.randint(0, 3),
                'defuses': random.randint(0, 2),
                'c2k': random.randint(0, 3),
                'c3k': random.randint(0, 2),
                'c4k': random.randint(0, 1),
                'c5k': random.randint(0, 1),
                'econ_spent': random.randint(80000, 120000),
                'econ_loadout': random.randint(60000, 100000),
            }

            image = await self.cog.stats_generator.generate_match_image(scoreboard_data)
            if image:
                self.images[value] = image
                image.seek(0)
                filename = f'match_{match_id}.png'
                file = discord.File(image, filename=filename)
                embed = discord.Embed(
                    title="Match Scoreboard Test - SUCCESS",
                    description="Use the dropdown to switch between views.",
                    color=0x00ff00
                )
                embed.set_image(url=f"attachment://{filename}")
                await interaction.edit_original_response(embed=embed, attachments=[file], view=self)
            else:
                await interaction.followup.send("Failed to generate match scoreboard.", ephemeral=True)


class EmojisSubView(discord.ui.View):
    """Sub-view with buttons for Ready Emojis and Role Emojis configuration."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Ready Emojis", style=discord.ButtonStyle.primary, row=0)
    async def ready_emojis_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        games = await DatabaseHelper.get_all_games()
        if not games:
            await interaction.response.send_message("No games configured.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(GameSelectDropdown(games, self._show_ready_emojis_flow))
        await interaction.response.send_message("Select a game to configure ready emojis:", view=view, ephemeral=True)

    async def _show_ready_emojis_flow(self, interaction: discord.Interaction, game_id: int):
        game = await DatabaseHelper.get_game(game_id)

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

    @discord.ui.button(label="Role Emojis", style=discord.ButtonStyle.primary, row=0)
    async def role_emojis_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RoleEmojisView(self.cog)
        emojis = await DatabaseHelper.get_role_emojis()
        resolved = _resolve_role_emojis(emojis, interaction.client)
        lines = []
        for key, label in ROLE_EMOJI_CHOICES.items():
            emoji = resolved.get(key)
            lines.append(f"• **{label}:** {emoji if emoji else 'Not set'}")
        embed = discord.Embed(
            title="Rivals Role Emojis",
            description="\n".join(lines),
            color=COLOR_NEUTRAL
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


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


class RoleEmojisView(discord.ui.View):
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

    @discord.ui.button(label="View", style=discord.ButtonStyle.secondary, row=0)
    async def view_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        emojis = await DatabaseHelper.get_role_emojis()
        resolved = _resolve_role_emojis(emojis, interaction.client)
        lines = []
        for key, label in ROLE_EMOJI_CHOICES.items():
            emoji = resolved.get(key)
            lines.append(f"• **{label}:** {emoji if emoji else 'Not set'}")
        embed = discord.Embed(
            title="Current Role Emojis",
            description="\n".join(lines),
            color=COLOR_NEUTRAL
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, row=0)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = RoleEmojiSelectView(self.cog, action="remove")
        await interaction.response.send_message(
            "Select which role emoji to remove:", view=view, ephemeral=True
        )


class RoleEmojiSelectView(discord.ui.View):
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


class ModRolesView(discord.ui.View):
    """View for managing mod roles."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.button(label="Add Mod Role", style=discord.ButtonStyle.success)
    async def add_mod_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddModRoleSelectView(self.cog)
        await interaction.response.send_message("Select a role to add as mod role:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Mod Role", style=discord.ButtonStyle.danger)
    async def remove_mod_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        mod_role_ids = await DatabaseHelper.get_mod_roles()
        if not mod_role_ids:
            await interaction.response.send_message("No mod roles configured.", ephemeral=True)
            return
        view = RemoveModRoleSelectView(self.cog, mod_role_ids, interaction.guild)
        await interaction.response.send_message("Select a role to remove:", view=view, ephemeral=True)


class AddModRoleSelectView(discord.ui.View):
    """View for selecting a role to add as mod role."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await DatabaseHelper.add_mod_role(role.id)
        await interaction.response.edit_message(content=f"Added **{role.name}** as a mod role.", view=None)


class RemoveModRoleSelectView(discord.ui.View):
    """View for selecting a mod role to remove."""

    def __init__(self, cog: 'CustomMatch', mod_role_ids: List[int], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        for role_id in mod_role_ids:
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=name, value=str(role_id)))

        select = discord.ui.Select(placeholder="Select role to remove...", options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_mod_role(role_id)
        await interaction.response.edit_message(content="Removed mod role.", view=None)


class MMRRolesView(discord.ui.View):
    """View for managing MMR roles."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.button(label="Add/Update Role", style=discord.ButtonStyle.success)
    async def add_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = AddMMRRoleSelectView(self.cog, self.game_id)
        await interaction.response.send_message("Select a role:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove Role", style=discord.ButtonStyle.danger)
    async def remove_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        if not mmr_roles:
            await interaction.response.send_message("No MMR roles configured.", ephemeral=True)
            return
        view = RemoveMMRRoleSelectView(self.cog, self.game_id, mmr_roles, interaction.guild)
        await interaction.response.send_message("Select a role to remove:", view=view, ephemeral=True)


class AddMMRRoleSelectView(discord.ui.View):
    """View for selecting a role to add as MMR role."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        modal = MMRValueModal(self.cog, self.game_id, role.id, role.name)
        await interaction.response.send_modal(modal)


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

    def __init__(self, cog: 'CustomMatch', game_id: int, role_id: int, role_name: str):
        super().__init__()
        self.cog = cog
        self.game_id = game_id
        self.role_id = role_id
        self.title = f"Set MMR for {role_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mmr = int(self.mmr_value.value)
            label = self.rank_label.value.strip() or None
            await DatabaseHelper.set_mmr_role(self.game_id, self.role_id, mmr, label)
            label_str = f" (label: **{label}**)" if label else ""
            await interaction.response.send_message(f"Set role MMR to {mmr}{label_str}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid MMR value.", ephemeral=True)


class RemoveMMRRoleSelectView(discord.ui.View):
    """View for selecting an MMR role to remove."""

    def __init__(self, cog: 'CustomMatch', game_id: int, mmr_roles: Dict[int, int], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = guild.get_role(role_id)
            name = role.name if role else f"Unknown ({role_id})"
            options.append(discord.SelectOption(label=f"{name} ({mmr} MMR)", value=str(role_id)))

        select = discord.ui.Select(placeholder="Select role to remove...", options=options[:25])
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        await DatabaseHelper.remove_mmr_role(self.game_id, role_id)
        await interaction.response.edit_message(content="Removed MMR role.", view=None)


class CategorySelectView(discord.ui.View):
    """View for selecting a category channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a category...",
                       channel_types=[discord.ChannelType.category])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        category = select.values[0]
        await DatabaseHelper.set_config("category_id", str(category.id))
        await interaction.response.edit_message(content=f"Category set to **{category.name}**.", view=None)


class LogChannelSelectView(discord.ui.View):
    """View for selecting a log channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.set_config("log_channel_id", str(channel.id))
        await interaction.response.edit_message(content=f"Log channel set to {channel.mention}.", view=None)


class AdminChannelSelectView(discord.ui.View):
    """View for selecting the CM admin notification channel."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.set_config("cm_admin_channel_id", str(channel.id))
        await interaction.response.edit_message(content=f"CM Admin channel set to {channel.mention}.", view=None)


class DiscussionParentChannelSelectView(discord.ui.View):
    """View for selecting the discussion parent channel (where private threads are created)."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.set_config("cm_discussion_parent_channel_id", str(channel.id))
        await interaction.response.edit_message(content=f"Discussion parent channel set to {channel.mention}.", view=None)


class AdminRoleSelectView(discord.ui.View):
    """View for selecting the admin role."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select a role...")
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await DatabaseHelper.set_config("cm_admin_role_id", str(role.id))
        await interaction.response.edit_message(content=f"CM Admin role set to **{role.name}**.", view=None)


class BlacklistUserSelectView(discord.ui.View):
    """View for selecting a user to blacklist."""

    def __init__(self, cog: 'CustomMatch'):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.select(cls=discord.ui.UserSelect, placeholder="Select a user...")
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        user = select.values[0]
        modal = BlacklistDurationModal(self.cog, user.id, user.display_name)
        await interaction.response.send_modal(modal)


class BlacklistDurationModal(discord.ui.Modal, title="Blacklist Duration"):
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch', user_id: int, user_name: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.title = f"Blacklist {user_name}"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.duration.value)
            if days <= 0:
                await DatabaseHelper.blacklist_player(self.user_id)
                await interaction.response.send_message(f"Permanently blacklisted user.", ephemeral=True)
            else:
                until = datetime.now(timezone.utc) + timedelta(days=days)
                await DatabaseHelper.blacklist_player(self.user_id, until)
                await interaction.response.send_message(f"Blacklisted user for {days} days.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid duration.", ephemeral=True)


class UnblacklistSelectView(discord.ui.View):
    """View for selecting a user to unblacklist."""

    def __init__(self, cog: 'CustomMatch', blacklisted: List[Tuple[int, datetime]], guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog

        options = []
        now = datetime.now(timezone.utc)
        for player_id, until in blacklisted:
            if until > now:
                user = guild.get_member(player_id)
                name = user.display_name if user else str(player_id)
                duration = "Permanent" if until.year == 2099 else f"Until {until.strftime('%Y-%m-%d')}"
                options.append(discord.SelectOption(label=name, value=str(player_id), description=duration))

        if options:
            select = discord.ui.Select(placeholder="Select player...", options=options[:25])
            select.callback = self.on_select
            self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        player_id = int(interaction.data["values"][0])
        await DatabaseHelper.unblacklist_player(player_id)
        await interaction.response.send_message("Player unblacklisted.", ephemeral=True)


class GameTogglesView(discord.ui.View):
    """View for toggling game settings."""

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__(timeout=120)
        self.cog = cog
        self.game = game
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()

        vc_btn = discord.ui.Button(
            label=f"VC Creation: {'ON' if self.game.vc_creation_enabled else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.vc_creation_enabled else discord.ButtonStyle.secondary
        )
        vc_btn.callback = self.toggle_vc
        self.add_item(vc_btn)

        role_btn = discord.ui.Button(
            label=f"Role Required: {'ON' if self.game.queue_role_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.queue_role_required else discord.ButtonStyle.secondary
        )
        role_btn.callback = self.toggle_role
        self.add_item(role_btn)

        dm_btn = discord.ui.Button(
            label=f"DM Ready: {'ON' if self.game.dm_ready_up else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.dm_ready_up else discord.ButtonStyle.secondary
        )
        dm_btn.callback = self.toggle_dm
        self.add_item(dm_btn)

        ign_btn = discord.ui.Button(
            label=f"IGN Required: {'ON' if self.game.ign_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.ign_required else discord.ButtonStyle.secondary
        )
        ign_btn.callback = self.toggle_ign_required
        self.add_item(ign_btn)

        role_req_btn = discord.ui.Button(
            label=f"Role Prefs Required: {'ON' if self.game.role_required else 'OFF'}",
            style=discord.ButtonStyle.success if self.game.role_required else discord.ButtonStyle.secondary
        )
        role_req_btn.callback = self.toggle_role_required
        self.add_item(role_req_btn)

        # Grace period button
        grace_btn = discord.ui.Button(
            label=f"Grace Period: {self.game.grace_period_minutes}min",
            style=discord.ButtonStyle.primary,
            row=1
        )
        grace_btn.callback = self.set_grace_period
        self.add_item(grace_btn)

        # Not Ready Cooldown button
        nr_cd_btn = discord.ui.Button(
            label=f"Not Ready CD: {self.game.not_ready_cooldown_minutes}min",
            style=discord.ButtonStyle.primary,
            row=1
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
            row=1
        )
        topic_btn.callback = self.set_verification_topic
        self.add_item(topic_btn)

    async def set_verification_topic(self, interaction: discord.Interaction):
        modal = VerificationTopicModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def set_grace_period(self, interaction: discord.Interaction):
        modal = GracePeriodModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

    async def set_not_ready_cooldown(self, interaction: discord.Interaction):
        modal = NotReadyCooldownModal(self.cog, self.game, self)
        await interaction.response.send_modal(modal)

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


class PenaltySettingsView(discord.ui.View):
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


class ClearPenaltyUserSelectView(discord.ui.View):
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


class ClearDeclinePenaltyUserSelectView(discord.ui.View):
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


class QueueScheduleView(discord.ui.View):
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


class ScheduleDaySelectView(discord.ui.View):
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


class ScheduleDayModeView(discord.ui.View):
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


class ScheduleDayRemoveView(discord.ui.View):
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

    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game
        self.timer_seconds.default = str(game.ready_timer_seconds)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            seconds = int(self.timer_seconds.value)
            if seconds < 10 or seconds > 600:
                await interaction.response.send_message(
                    "Ready timer must be between 10 and 600 seconds.",
                    ephemeral=True
                )
                return

            await DatabaseHelper.update_game(self.game.game_id, ready_timer_seconds=seconds)
            await interaction.response.send_message(
                f"Ready timer for **{self.game.name}** set to **{seconds} seconds**.\n"
                f"Players will have {seconds} seconds to ready up when a queue fills.",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid number.", ephemeral=True)


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


class GameChannelSelectView(discord.ui.View):
    """View for selecting game channel (for match results)."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.update_game(self.game_id, game_channel_id=channel.id)
        await interaction.response.edit_message(content=f"Game channel set to {channel.mention}.", view=None)

    @discord.ui.button(label="Clear (No Game Channel)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, game_channel_id=None)
        await interaction.response.edit_message(content="Game channel cleared.", view=None)


class LF1ChannelSelectView(discord.ui.View):
    """View for selecting LF1 channel (Looking for 1 notifications)."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel or thread...",
                       channel_types=[discord.ChannelType.text, discord.ChannelType.public_thread,
                                      discord.ChannelType.private_thread])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        await DatabaseHelper.update_game(self.game_id, lf1_channel_id=channel.id)
        await interaction.response.edit_message(content=f"LF1 channel set to {channel.mention}.", view=None)

    @discord.ui.button(label="Clear (No LF1 Channel)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, lf1_channel_id=None)
        await interaction.response.edit_message(content="LF1 channel cleared.", view=None)


class LeaderboardChannelSelectView(discord.ui.View):
    """View for selecting leaderboard channel (persistent auto-updating leaderboard)."""

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select a channel...",
                       channel_types=[discord.ChannelType.text])
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        selected = select.values[0]
        channel = interaction.guild.get_channel(selected.id)
        if not channel:
            await interaction.response.edit_message(content="Could not find that channel.", view=None)
            return
        game = await DatabaseHelper.get_game(self.game_id)
        if not game:
            await interaction.response.edit_message(content="Game not found.", view=None)
            return

        await interaction.response.defer()

        # Build and send the initial leaderboard embed
        is_valorant = 'valorant' in game.name.lower()

        from .views_gameplay import PersistentLeaderboardView

        embed = await self.cog._build_leaderboard_text_embed(interaction.guild, self.game_id, monthly=True)
        view = PersistentLeaderboardView(self.cog, self.game_id, is_valorant=is_valorant)

        try:
            msg = await channel.send(embed=embed, view=view)
        except discord.Forbidden:
            await interaction.edit_original_response(content="I don't have permission to send messages in that channel.", view=None)
            return

        # Save channel and message IDs
        await DatabaseHelper.update_game(self.game_id, leaderboard_channel_id=channel.id, leaderboard_message_id=msg.id)

        # Register view for persistence
        self.cog.bot.add_view(view, message_id=msg.id)

        await interaction.edit_original_response(content=f"Leaderboard channel set to {channel.mention}.", view=None)

    @discord.ui.button(label="Clear (No Leaderboard)", style=discord.ButtonStyle.secondary)
    async def clear_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await DatabaseHelper.update_game(self.game_id, leaderboard_channel_id=None, leaderboard_message_id=None)
        await interaction.response.edit_message(content="Leaderboard channel cleared.", view=None)


# =============================================================================
# SETTINGS MODALS (Legacy - kept for backwards compatibility)
# =============================================================================

class CategoryModal(discord.ui.Modal, title="Set Category"):
    category_id = discord.ui.TextInput(
        label="Category ID",
        placeholder="Right-click category > Copy ID",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            cat_id = int(self.category_id.value)
            category = interaction.guild.get_channel(cat_id)
            if not category or not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message("Invalid category ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("category_id", str(cat_id))
            await interaction.response.send_message(f"Category set to **{category.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class LogChannelModal(discord.ui.Modal, title="Set Log Channel"):
    channel_id = discord.ui.TextInput(
        label="Channel ID",
        placeholder="Right-click channel > Copy ID",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            ch_id = int(self.channel_id.value)
            channel = interaction.guild.get_channel(ch_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                await interaction.response.send_message("Invalid channel ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("log_channel_id", str(ch_id))
            await interaction.response.send_message(f"Log channel set to {channel.mention}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


class AdminRoleModal(discord.ui.Modal, title="Set Custom Match Admin Role"):
    role_id = discord.ui.TextInput(
        label="Role ID",
        placeholder="Right-click role > Copy ID",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            role = interaction.guild.get_role(r_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            await DatabaseHelper.set_config("cm_admin_role_id", str(r_id))
            await interaction.response.send_message(f"CM Admin role set to **{role.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid ID format.", ephemeral=True)


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

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
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

            game_id = await DatabaseHelper.add_game(self.name.value, count, qt, cs)
            await interaction.response.send_message(
                f"Added **{self.name.value}** ({count} players, {qt} queue).",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("Invalid player count.", ephemeral=True)
        except Exception as e:
            if "UNIQUE" in str(e):
                await interaction.response.send_message("A game with that name already exists.", ephemeral=True)
            else:
                raise


class EditGameModal(discord.ui.Modal, title="Edit Game"):
    def __init__(self, cog: 'CustomMatch', game: GameConfig):
        super().__init__()
        self.cog = cog
        self.game = game

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
        self.queue_channel = discord.ui.TextInput(
            label="Queue Channel ID (blank to clear)",
            default=str(game.queue_channel_id) if game.queue_channel_id else "",
            required=False
        )
        self.verified_role = discord.ui.TextInput(
            label="Verified Role ID (blank to clear)",
            default=str(game.verified_role_id) if game.verified_role_id else "",
            required=False
        )

        self.add_item(self.player_count)
        self.add_item(self.queue_type)
        self.add_item(self.captain_selection)
        self.add_item(self.queue_channel)
        self.add_item(self.verified_role)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.player_count.value)
            qt = self.queue_type.value.lower()
            cs = self.captain_selection.value.lower()

            if qt not in ["mmr", "captains", "random"]:
                await interaction.response.send_message("Invalid queue type.", ephemeral=True)
                return
            if cs not in ["random", "admin", "highest_mmr"]:
                await interaction.response.send_message("Invalid captain selection.", ephemeral=True)
                return

            updates = {
                "player_count": count,
                "queue_type": qt,
                "captain_selection": cs
            }

            if self.queue_channel.value:
                updates["queue_channel_id"] = int(self.queue_channel.value)
            else:
                updates["queue_channel_id"] = None

            if self.verified_role.value:
                updates["verified_role_id"] = int(self.verified_role.value)
            else:
                updates["verified_role_id"] = None

            await DatabaseHelper.update_game(self.game.game_id, **updates)
            await interaction.response.send_message(f"Updated **{self.game.name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid number format.", ephemeral=True)


class AddMMRRoleModal(discord.ui.Modal, title="Add/Update MMR Role"):
    role_id = discord.ui.TextInput(label="Role ID", required=True)
    mmr_value = discord.ui.TextInput(label="MMR Value", placeholder="e.g., 1500", required=True)

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            mmr = int(self.mmr_value.value)
            role = interaction.guild.get_role(r_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            await DatabaseHelper.set_mmr_role(self.game_id, r_id, mmr)
            await interaction.response.send_message(f"Set **{role.name}** to {mmr} MMR.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class RemoveMMRRoleModal(discord.ui.Modal, title="Remove MMR Role"):
    role_id = discord.ui.TextInput(label="Role ID", required=True)

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            r_id = int(self.role_id.value)
            await DatabaseHelper.remove_mmr_role(self.game_id, r_id)
            await interaction.response.send_message("Removed MMR role.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class SetPlayerMMRModal(discord.ui.Modal, title="Set Player MMR"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse user ID from mention or raw ID
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            member = interaction.guild.get_member(user_id)
            if not member:
                await interaction.response.send_message("User not found in server.", ephemeral=True)
                return

            # Check for MMR roles
            mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
            detected_mmr = None
            detected_role = None

            for role in member.roles:
                if role.id in mmr_roles:
                    detected_mmr = mmr_roles[role.id]
                    detected_role = role
                    break

            if detected_mmr:
                # Set MMR from role
                stats = await DatabaseHelper.get_player_stats(user_id, self.game_id)
                stats.mmr = detected_mmr
                await DatabaseHelper.update_player_stats(stats)
                await interaction.response.send_message(
                    f"Set **{member.display_name}**'s MMR to {detected_mmr} (from {detected_role.name}).",
                    ephemeral=True
                )
                await self.cog.update_mmr_roles(interaction.guild, user_id, self.game_id, stats.effective_mmr)
            else:
                # Show role selection
                if not mmr_roles:
                    await interaction.response.send_message(
                        "No MMR roles configured for this game.",
                        ephemeral=True
                    )
                    return

                view = await MMRRoleSelectView.create(self.cog, self.game_id, user_id, interaction.guild)
                await interaction.response.send_message(
                    f"No MMR role detected on {member.display_name}. Select one:",
                    view=view,
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid user ID.", ephemeral=True)


class MMRRoleSelectView(discord.ui.View):
    """View for selecting an MMR role when none is detected."""

    def __init__(self, cog: 'CustomMatch', game_id: int, user_id: int, guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.game_id = game_id
        self.user_id = user_id
        self.guild = guild

    @classmethod
    async def create(cls, cog: 'CustomMatch', game_id: int, user_id: int, guild: discord.Guild) -> 'MMRRoleSelectView':
        """Factory method to create view with async setup."""
        view = cls(cog, game_id, user_id, guild)
        await view.setup_select()
        return view

    async def setup_select(self):
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        options = []
        for role_id, mmr in sorted(mmr_roles.items(), key=lambda x: x[1], reverse=True):
            role = self.guild.get_role(role_id)
            if role:
                options.append(discord.SelectOption(
                    label=f"{role.name} ({mmr} MMR)",
                    value=str(role_id)
                ))

        if options:
            select = discord.ui.Select(placeholder="Select MMR role...", options=options)
            select.callback = self.on_select
            self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        mmr_roles = await DatabaseHelper.get_mmr_roles(self.game_id)
        mmr = mmr_roles.get(role_id, 1000)

        stats = await DatabaseHelper.get_player_stats(self.user_id, self.game_id)
        stats.mmr = mmr
        await DatabaseHelper.update_player_stats(stats)

        member = self.guild.get_member(self.user_id)
        role = self.guild.get_role(role_id)
        await interaction.response.edit_message(
            content=f"Set **{member.display_name if member else self.user_id}**'s MMR to {mmr} (from {role.name if role else role_id}).",
            view=None
        )
        await self.cog.update_mmr_roles(self.guild, self.user_id, self.game_id, stats.effective_mmr)


class SetAdminOffsetModal(discord.ui.Modal, title="Set Admin Offset"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    offset = discord.ui.TextInput(label="Offset Value", placeholder="e.g., 100 or -50", required=True)

    def __init__(self, cog: 'CustomMatch', game_id: int):
        super().__init__()
        self.cog = cog
        self.game_id = game_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            offset = int(self.offset.value)

            member = interaction.guild.get_member(user_id)
            if not member:
                await interaction.response.send_message("User not found.", ephemeral=True)
                return

            stats = await DatabaseHelper.get_player_stats(user_id, self.game_id)
            stats.admin_offset = offset
            await DatabaseHelper.update_player_stats(stats)

            await interaction.response.send_message(
                f"Set **{member.display_name}**'s admin offset to {offset:+d}.",
                ephemeral=True
            )
            await self.cog.update_mmr_roles(interaction.guild, user_id, self.game_id, stats.effective_mmr)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class BlacklistModal(discord.ui.Modal, title="Blacklist Player"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)
    duration = discord.ui.TextInput(
        label="Duration (days, 0 = permanent)",
        placeholder="e.g., 7",
        default="0",
        required=True
    )

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)
            days = int(self.duration.value)

            member = interaction.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)

            if days <= 0:
                await DatabaseHelper.blacklist_player(user_id)
                await interaction.response.send_message(f"Permanently blacklisted **{name}**.", ephemeral=True)
            else:
                until = datetime.now(timezone.utc) + timedelta(days=days)
                await DatabaseHelper.blacklist_player(user_id, until)
                await interaction.response.send_message(
                    f"Blacklisted **{name}** for {days} days.",
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


class UnblacklistModal(discord.ui.Modal, title="Unblacklist Player"):
    user_id = discord.ui.TextInput(label="User ID or @mention", required=True)

    def __init__(self, cog: 'CustomMatch'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_str = self.user_id.value.replace("<@", "").replace(">", "").replace("!", "")
            user_id = int(user_str)

            await DatabaseHelper.unblacklist_player(user_id)
            member = interaction.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)
            await interaction.response.send_message(f"Unblacklisted **{name}**.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Invalid format.", ephemeral=True)


# =============================================================================
# SECONDARY QUEUE SETTINGS
# =============================================================================

class SecondaryQueueSettingsView(discord.ui.View):
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


class SecondaryModesManageView(discord.ui.View):
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


class EditModePoolView(discord.ui.View):
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


class ModeFlagToggleView(discord.ui.View):
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


class SecondaryQueueTypeSelectView(discord.ui.View):
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


class SecondaryQueueChannelSelectView(discord.ui.View):
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


class SecondaryScheduleView(discord.ui.View):
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
