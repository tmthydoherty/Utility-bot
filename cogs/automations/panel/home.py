"""The `/automations` panel root.

Live counts rather than a static blurb: the point of opening the panel is
usually to find out what is currently switched on, and how much of it is still
only pretending.

The website at /dashboard is the roomier way to build one of these, so this
screen says so — but it says it as a footnote, because someone on a phone with
no browser to hand still has to be able to do everything from here.
"""

from __future__ import annotations

import logging
import time

import discord

from utils.panel import CloseButton
from utils.panel_rules import AdminPage

from .. import readiness
from ..models import Automation
from ..storage import KILL_SWITCH_KEY
from .builder import AutomationListPage

logger = logging.getLogger('cogs.automations.panel.home')


class AutomationsHomePage(AdminPage):
    title = "Automations"

    def __init__(self, cog, **kwargs):
        super().__init__(cog, None, **kwargs)

        browse = discord.ui.Button(label="My automations",
                                   style=discord.ButtonStyle.primary, row=0)
        browse.callback = self._open_list
        self.add_item(browse)

        self._pause = discord.ui.Button(label="Pause everything",
                                        style=discord.ButtonStyle.secondary, row=1)
        self._pause.callback = self._toggle_pause
        self.add_item(self._pause)

        refresh = discord.ui.Button(label="↻ Refresh",
                                    style=discord.ButtonStyle.secondary, row=4)
        refresh.callback = self._refresh
        self.add_item(refresh)
        self.add_item(CloseButton(row=4))

    async def _counts(self, guild_id: int) -> tuple:
        db = self.cog.db
        totals = await db.fetchone(
            "SELECT COUNT(*) AS total, SUM(enabled) AS live, "
            "SUM(CASE WHEN enabled = 1 AND dry_run = 1 THEN 1 ELSE 0 END) AS dry "
            "FROM automations WHERE guild_id = ?", (guild_id,))
        runs = await db.fetchone(
            "SELECT COUNT(*) AS c FROM automation_runs WHERE ts > ?",
            (int(time.time()) - 86400,))
        return totals, (runs["c"] if runs else 0)

    async def _unfinished(self, guild_id: int) -> int:
        """How many automations still have blanks in them."""
        rows = await self.cog.db.list_rows("automations", guild_id)
        total = 0
        for row in rows:
            try:
                if readiness.summary(Automation.from_row(row)):
                    total += 1
            except Exception as e:
                # A row the current registry can't read must not take the whole
                # panel down — the list screen reports it properly.
                logger.debug(f"Skipped readiness for {row['id']}: {e}")
        return total

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        totals, runs = await self._counts(interaction.guild_id)
        paused = not await self.cog.automations_enabled()
        self._pause.label = "Resume everything" if paused else "Pause everything"
        self._pause.style = (discord.ButtonStyle.success if paused
                             else discord.ButtonStyle.secondary)

        embed = discord.Embed(
            title="Automations",
            description=(
                "An automation makes the bot do something by itself — when "
                "someone joins, when someone says a word, when someone posts a "
                "picture.\n\nStart from a ready-made one and change the bits "
                "you disagree with; that is much quicker than building from "
                "scratch."),
            color=discord.Color.red() if paused else discord.Color.blurple(),
        )

        total = totals["total"] if totals else 0
        live = (totals["live"] or 0) if totals else 0
        dry = (totals["dry"] or 0) if totals else 0
        if not total:
            summary = "*None set up yet — press **My automations** to start one*"
        else:
            parts = [f"**{live - dry}** running"]
            if dry:
                parts.append(f"**{dry}** in test mode")
            if total - live:
                parts.append(f"**{total - live}** switched off")
            summary = ", ".join(parts)

        lines = [summary, f"Set off {runs} time(s) in the last day"]
        # Surfaced here because an automation with blanks in it cannot be turned
        # on, and someone who set one up days ago has no other reason to go
        # looking for it.
        unfinished = await self._unfinished(interaction.guild_id)
        if unfinished:
            lines.append(f"🛠️ **{unfinished}** still need finishing")
        embed.add_field(name="Right now", value="\n".join(lines), inline=False)

        if paused:
            embed.add_field(
                name="⚠️ Everything is paused",
                value=("No automation will run at all until you press "
                       "**Resume everything**."),
                inline=False,
            )

        embed.set_footer(text="There is a roomier version of this at vibe-y.us")
        return embed

    async def _open_list(self, interaction: discord.Interaction):
        await AutomationListPage(self.cog, self).render(interaction)

    async def _toggle_pause(self, interaction: discord.Interaction):
        paused = not await self.cog.automations_enabled()
        await self.cog.db.set_bool(KILL_SWITCH_KEY, paused)  # paused -> resume
        await self.cog.save_made()
        await self.render(
            interaction,
            flash="✅ Automations resumed." if paused else "⏸️ All automations paused.")

    async def _refresh(self, interaction: discord.Interaction):
        await self.cog.refresh_cache()
        await self.render(interaction, flash="↻ Refreshed.")
