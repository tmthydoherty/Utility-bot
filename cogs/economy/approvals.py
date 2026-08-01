"""Admin approval queue for GIF commands, emoji submissions and emoji reviews.

Buttons are DynamicItems keyed by approval id, so pending approvals stay
clickable across restarts without re-registering a view per row.
"""

import json
import logging
import re
import time

import discord
from discord import ui

logger = logging.getLogger('cogs.economy.approvals')

KIND_TITLES = {
    "gif": "GIF Command Submission",
    "emoji": "Emoji Submission",
    "emoji_permanent": "Emoji Permanent Status Review",
}


# --------------------------------------------------------------------------
# Buttons
# --------------------------------------------------------------------------

class ApprovalButton(
    ui.DynamicItem[ui.Button],
    template=r"eco:approval:(?P<action>approve|deny):(?P<approval_id>\d+)",
):
    def __init__(self, action: str, approval_id: int):
        self.action = action
        self.approval_id = approval_id
        super().__init__(
            ui.Button(
                label="Approve" if action == "approve" else "Deny",
                style=discord.ButtonStyle.success if action == "approve"
                else discord.ButtonStyle.danger,
                custom_id=f"eco:approval:{action}:{approval_id}",
            )
        )

    @classmethod
    async def from_custom_id(cls, interaction, item, match: re.Match):
        return cls(match["action"], int(match["approval_id"]))

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Economy")
        if cog is None:
            await interaction.response.send_message(
                "The Economy cog is not loaded.", ephemeral=True
            )
            return
        if not cog.is_admin(interaction.user):
            await interaction.response.send_message(
                "Only admins can decide this.", ephemeral=True
            )
            return

        row = await cog.db.fetchone(
            "SELECT * FROM approvals WHERE id = ?", (self.approval_id,)
        )
        if row is None:
            await interaction.response.send_message(
                "That submission no longer exists.", ephemeral=True
            )
            return
        if row["status"] != "pending":
            await interaction.response.send_message(
                f"Already {row['status']} by <@{row['decided_by']}>.", ephemeral=True
            )
            return

        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"Confirm {self.action}",
                description=f"{KIND_TITLES.get(row['kind'], row['kind'])} from "
                            f"<@{row['user_id']}>.\n\nThis cannot be undone.",
                color=discord.Color.orange(),
            ),
            view=ConfirmDecisionView(cog, self.approval_id, self.action),
            ephemeral=True,
        )


class ConfirmDecisionView(ui.View):
    def __init__(self, cog, approval_id: int, action: str):
        super().__init__(timeout=120)
        self.cog = cog
        self.approval_id = approval_id
        self.action = action

    @ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        result = await resolve(self.cog, self.approval_id, self.action, interaction.user)
        await interaction.edit_original_response(content=result, embed=None, view=None)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(
            content="Cancelled.", embed=None, view=None
        )
        self.stop()


def build_approval_view(approval_id: int) -> ui.View:
    view = ui.View(timeout=None)
    view.add_item(ApprovalButton("approve", approval_id))
    view.add_item(ApprovalButton("deny", approval_id))
    return view


# --------------------------------------------------------------------------
# Submission + resolution
# --------------------------------------------------------------------------

async def submit(cog, kind: str, user_id: int, payload: dict, *,
                 price_paid: int = 0, inventory_id: int = None,
                 embed: discord.Embed = None) -> int:
    """Post a submission to the approval channel. Returns the approval id."""
    channel_id = cog.config.get_int("ch_approval", 0)
    channel = cog.bot.get_channel(channel_id) if channel_id else None

    cursor = await cog.db.execute(
        "INSERT INTO approvals (kind, user_id, payload, status, price_paid, "
        "inventory_id, created_ts) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
        (kind, user_id, json.dumps(payload), price_paid, inventory_id, int(time.time())),
    )
    approval_id = cursor.lastrowid

    if channel is None:
        logger.warning(f"No approval channel configured; approval {approval_id} is orphaned.")
        return approval_id

    if embed is None:
        embed = discord.Embed(
            title=KIND_TITLES.get(kind, kind),
            color=discord.Color.orange(),
        )
    embed.set_footer(text=f"Submission #{approval_id}")

    try:
        message = await channel.send(embed=embed, view=build_approval_view(approval_id))
    except discord.HTTPException as e:
        logger.error(f"Could not post approval {approval_id}: {e}")
        return approval_id

    await cog.db.execute(
        "UPDATE approvals SET channel_id = ?, message_id = ? WHERE id = ?",
        (channel.id, message.id, approval_id),
    )
    return approval_id


async def resolve(cog, approval_id: int, action: str, admin: discord.Member) -> str:
    """Apply an approve/deny decision and hand off to the owning item."""
    row = await cog.db.fetchone("SELECT * FROM approvals WHERE id = ?", (approval_id,))
    if row is None:
        return "That submission no longer exists."
    if row["status"] != "pending":
        return f"Already {row['status']}."

    status = "approved" if action == "approve" else "denied"
    await cog.db.execute(
        "UPDATE approvals SET status = ?, decided_by = ?, decided_ts = ? WHERE id = ?",
        (status, admin.id, int(time.time()), approval_id),
    )

    handler = cog.items.approval_handler(row["kind"])
    outcome = ""
    if handler is not None:
        try:
            outcome = await handler(cog, row, status, admin) or ""
        except Exception as e:
            logger.error(f"Approval handler for {row['kind']} failed: {e}", exc_info=True)
            outcome = f"\n\nThe handler errored: `{e}`"

    await _stamp_message(cog, row, status, admin)
    return f"Submission #{approval_id} {status}.{outcome}"


async def _stamp_message(cog, row, status: str, admin: discord.Member):
    """Mark the original approval message as decided and disable its buttons."""
    if not row["channel_id"] or not row["message_id"]:
        return
    channel = cog.bot.get_channel(row["channel_id"])
    if channel is None:
        return
    try:
        message = await channel.fetch_message(row["message_id"])
    except discord.HTTPException:
        return

    embed = message.embeds[0] if message.embeds else discord.Embed()
    embed.color = (discord.Color.green() if status == "approved"
                   else discord.Color.red())
    embed.add_field(
        name="Decision",
        value=f"**{status.title()}** by {admin.mention} <t:{int(time.time())}:R>",
        inline=False,
    )
    try:
        await message.edit(embed=embed, view=None)
    except discord.HTTPException:
        pass


async def pending(cog, limit: int = 25) -> list:
    return await cog.db.fetchall(
        "SELECT * FROM approvals WHERE status = 'pending' ORDER BY created_ts ASC LIMIT ?",
        (limit,),
    )
