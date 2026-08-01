"""Proxy — send one message anonymously through the bot.

Anonymous to the server, never to the admins: sender, content, channel and a
jump link go to the private proxy log channel set in the panel.
"""

import logging

import discord
from discord import ui

from .base import ClaimedFlowView, ShopItem

logger = logging.getLogger('cogs.economy.items.proxy')


class ProxyItem(ShopItem):
    key = "proxy"
    name = "Proxy"

    async def activate(self, interaction: discord.Interaction, inv_row):
        view = ProxySetupView(self, inv_row)
        await interaction.response.send_message(
            embed=view.build_embed(), view=view, ephemeral=True
        )

    async def send(self, interaction: discord.Interaction, view,
                   channel: discord.TextChannel, content: str):
        inv_row = view.inv_row
        member = interaction.user
        allowed, reason = self.validate(member, channel, content)
        if not allowed:
            await interaction.response.send_message(reason, ephemeral=True)
            return

        try:
            sent = await channel.send(
                content,
                # Role and @everyone pings are stripped no matter what the
                # message text contains.
                allowed_mentions=discord.AllowedMentions(
                    everyone=False, roles=False, users=True, replied_user=False
                ),
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "The bot cannot post in that channel.", ephemeral=True
            )
            return
        except discord.HTTPException as e:
            await interaction.response.send_message(
                f"Discord rejected that message: {e}", ephemeral=True
            )
            return

        await self.consume(inv_row)
        view.settle()
        await self.cog.db.execute(
            "INSERT INTO proxy_log (user_id, channel_id, message_id, content, ts) "
            "VALUES (?, ?, ?, ?, ?)",
            (member.id, channel.id, sent.id, content, self.now()),
        )
        await self._log(member, channel, content, sent)

        await interaction.response.edit_message(
            content=f"Sent anonymously to {channel.mention}.", embed=None, view=None
        )

    def validate(self, member: discord.Member, channel, content: str) -> tuple:
        if channel is None:
            return False, "Choose a channel first."

        blocked = {int(c) for c in self.cog.config.get_list("proxy_blocklist")}
        if channel.id in blocked or getattr(channel, "parent_id", None) in blocked:
            return False, "Proxy is not allowed in that channel."

        # The buyer must genuinely have access — no posting into channels they
        # cannot see themselves.
        perms = channel.permissions_for(member)
        if not perms.view_channel or not perms.send_messages:
            return False, "You cannot post in that channel yourself."

        max_chars = self.cog.config.get_int("proxy_max_chars", 500)
        if len(content) > max_chars:
            return False, f"Keep it under {max_chars} characters."
        if not content.strip():
            return False, "The message is empty."

        return True, ""

    async def _log(self, member, channel, content, sent):
        log_id = self.cog.config.get_int("ch_proxy_log", 0)
        if not log_id:
            return
        log_channel = self.cog.bot.get_channel(log_id)
        if log_channel is None:
            return

        embed = discord.Embed(
            title="Proxy Message Sent",
            description=content[:3000],
            color=discord.Color.dark_grey(),
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Sender", value=f"{member.mention} (`{member.id}`)")
        embed.add_field(name="Channel", value=channel.mention)
        embed.add_field(name="Message", value=f"[Jump]({sent.jump_url})", inline=False)
        embed.set_footer(text="Visible to admins only.")
        try:
            await log_channel.send(embed=embed)
        except discord.HTTPException as e:
            logger.warning(f"Could not write proxy log: {e}")


class ProxySetupView(ClaimedFlowView):
    def __init__(self, item: ProxyItem, inv_row):
        super().__init__(item, inv_row)
        self.channel = None
        self.content = None
        self._sync()

    def build_embed(self) -> discord.Embed:
        max_chars = self.item.cog.config.get_int("proxy_max_chars", 500)
        embed = discord.Embed(
            title="Proxy",
            description="Your message is posted by Vibey with no attribution.",
            color=discord.Color.dark_grey(),
        )
        embed.add_field(
            name="Channel",
            value=self.channel.mention if self.channel else "*Not chosen*",
            inline=False,
        )
        embed.add_field(
            name="Message",
            value=f"```{self.content[:400]}```" if self.content else "*Not written*",
            inline=False,
        )
        embed.set_footer(
            text=f"Max {max_chars} characters · no @everyone or role pings · "
                 f"cannot be edited once sent"
        )
        return embed

    def _sync(self):
        self.send_button.disabled = not (self.channel and self.content)

    @ui.select(
        cls=ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="Choose a channel...",
        row=0,
    )
    async def pick_channel(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        resolved = interaction.guild.get_channel(select.values[0].id)
        allowed, reason = self.item.validate(
            interaction.user, resolved, self.content or "placeholder"
        )
        if not allowed and "characters" not in reason and "empty" not in reason:
            await interaction.response.send_message(reason, ephemeral=True)
            return
        self.channel = resolved
        self._sync()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @ui.button(label="Write Message", style=discord.ButtonStyle.secondary, row=1)
    async def write(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(ProxyModal(self))

    @ui.button(label="Send", style=discord.ButtonStyle.success, row=1)
    async def send_button(self, interaction: discord.Interaction, button: ui.Button):
        await self.item.send(interaction, self, self.channel, self.content)
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await self.cancel_flow(interaction)


class ProxyModal(ui.Modal, title="Anonymous Message"):
    def __init__(self, view: ProxySetupView):
        super().__init__()
        self.view = view
        self.body = ui.TextInput(
            label="Message",
            style=discord.TextStyle.paragraph,
            max_length=view.item.cog.config.get_int("proxy_max_chars", 500),
        )
        self.add_item(self.body)

    async def on_submit(self, interaction: discord.Interaction):
        self.view.content = str(self.body.value)
        self.view._sync()
        await interaction.response.edit_message(
            embed=self.view.build_embed(), view=self.view
        )
