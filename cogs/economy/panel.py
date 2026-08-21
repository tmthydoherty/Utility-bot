"""Admin panel — /economy_panel.

Every admin action lives here as a button. There are no admin slash commands.
"""

import logging

import discord
from discord import ui

from .config import BOX_OUTCOMES, EARNING_SOURCES, SHOP_ITEMS
from .render import commas
from utils.newcomer_role_sources import summary as newcomer_role_summary

logger = logging.getLogger('cogs.economy.panel')


def _channel_value(cog, key: str) -> str:
    cid = cog.config.get_int(key, 0)
    return f"<#{cid}>" if cid else "*Not set*"


def _role_value(cog, key: str) -> str:
    rid = cog.config.get_int(key, 0)
    return f"<@&{rid}>" if rid else "*Not set*"


async def build_panel_embed(cog) -> discord.Embed:
    embed = discord.Embed(
        title="Economy Panel",
        description="Configure leveling, Points earning and the shop.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Leveling",
        value=(
            f"Message XP: **{cog.config.get_int('lvl_msg_xp_min', 15)}–"
            f"{cog.config.get_int('lvl_msg_xp_max', 25)}** "
            f"(every {cog.config.get_int('lvl_xp_cooldown', 60)}s)\n"
            f"Voice XP: **{cog.config.get_int('lvl_voice_xp_per_min', 5)}**/min\n"
            f"Excluded channels: **{len(cog.config.get_list('lvl_excluded_channels'))}**"
        ),
        inline=False,
    )
    embed.add_field(
        name="Roles",
        value=(
            f"Throne: {_role_value(cog, 'role_throne')}\n"
            f"Vibes: {_role_value(cog, 'role_vibes')}\n"
            f"Newcomer: {_role_value(cog, 'role_newcomer')}"
        ),
    )
    embed.add_field(
        name="Channels",
        value=(
            f"Approvals: {_channel_value(cog, 'ch_approval')}\n"
            f"Hall of Fame: {_channel_value(cog, 'ch_hof')}\n"
            f"Proxy log: {_channel_value(cog, 'ch_proxy_log')}\n"
            f"Queue: {_channel_value(cog, 'ch_match_queue')}"
        ),
    )

    total = await cog.db.fetchone(
        "SELECT COUNT(*) AS users, SUM(points) AS points FROM users WHERE points > 0"
    )
    pending = await cog.db.fetchone(
        "SELECT COUNT(*) AS c FROM approvals WHERE status = 'pending'"
    )
    effects = await cog.db.fetchone(
        "SELECT COUNT(*) AS c FROM active_effects WHERE expires_ts > strftime('%s','now')"
    )
    embed.add_field(
        name="Live",
        value=(
            f"Holders: **{commas(total['users'] if total else 0)}**\n"
            f"Points in circulation: **{commas(total['points'] if total and total['points'] else 0)}**\n"
            f"Pending approvals: **{pending['c'] if pending else 0}**\n"
            f"Active effects: **{effects['c'] if effects else 0}**"
        ),
        inline=False,
    )
    return embed


class AdminView(ui.View):
    """Base for every panel screen — admin-gated with a Back button."""

    def __init__(self, cog, *, back: bool = True, back_row: int = 4):
        super().__init__(timeout=900)
        self.cog = cog
        if back:
            self.add_item(BackToPanel(cog, row=back_row))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not self.cog.is_admin(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to use this.", ephemeral=True
            )
            return False
        return True


class BackToPanel(ui.Button):
    def __init__(self, cog, row: int = 4):
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, row=row)
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            embed=await build_panel_embed(self.cog), view=PanelView(self.cog)
        )


class PanelView(AdminView):
    def __init__(self, cog):
        super().__init__(cog, back=False)

    async def _open(self, interaction, view, embed):
        await interaction.response.edit_message(embed=embed, view=view)

    @ui.button(label="Leveling", style=discord.ButtonStyle.primary, row=0)
    async def leveling(self, interaction: discord.Interaction, button: ui.Button):
        view = LevelingView(self.cog)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Earning Rules", style=discord.ButtonStyle.primary, row=0)
    async def earning(self, interaction: discord.Interaction, button: ui.Button):
        view = EarningView(self.cog)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Shop", style=discord.ButtonStyle.primary, row=0)
    async def shop(self, interaction: discord.Interaction, button: ui.Button):
        view = ShopSettingsView(self.cog)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Mystery Box", style=discord.ButtonStyle.primary, row=0)
    async def box(self, interaction: discord.Interaction, button: ui.Button):
        view = MysteryBoxView(self.cog)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Roles", style=discord.ButtonStyle.secondary, row=1)
    async def roles(self, interaction: discord.Interaction, button: ui.Button):
        view = RolesView(self.cog, guild=interaction.guild)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Channels", style=discord.ButtonStyle.secondary, row=1)
    async def channels(self, interaction: discord.Interaction, button: ui.Button):
        view = ChannelsView(self.cog)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Members", style=discord.ButtonStyle.secondary, row=1)
    async def members(self, interaction: discord.Interaction, button: ui.Button):
        view = MembersView(self.cog)
        await self._open(interaction, view, await view.embed())

    @ui.button(label="Maintenance", style=discord.ButtonStyle.secondary, row=1)
    async def maintenance(self, interaction: discord.Interaction, button: ui.Button):
        view = MaintenanceView(self.cog)
        await self._open(interaction, view, await view.embed())


# --------------------------------------------------------------------------
# Leveling
# --------------------------------------------------------------------------

class LevelingView(AdminView):
    async def embed(self) -> discord.Embed:
        excluded = self.cog.config.get_list("lvl_excluded_channels")
        embed = discord.Embed(title="Leveling", color=discord.Color.blurple())
        embed.add_field(
            name="Message XP",
            value=f"{self.cog.config.get_int('lvl_msg_xp_min', 15)}–"
                  f"{self.cog.config.get_int('lvl_msg_xp_max', 25)} per message",
        )
        embed.add_field(
            name="XP Cooldown",
            value=f"{self.cog.config.get_int('lvl_xp_cooldown', 60)}s",
        )
        embed.add_field(
            name="Voice XP",
            value=f"{self.cog.config.get_int('lvl_voice_xp_per_min', 5)} per minute",
        )
        embed.add_field(
            name="Excluded Channels",
            value=", ".join(f"<#{c}>" for c in excluded) if excluded else "*None*",
            inline=False,
        )
        embed.set_footer(
            text="Voice XP is only awarded when at least two people are in a "
                 "channel and nobody is deafened."
        )
        return embed

    @ui.button(label="Edit Rates", style=discord.ButtonStyle.primary, row=1)
    async def edit(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(LevelingModal(self))

    @ui.select(
        cls=ui.ChannelSelect,
        channel_types=[discord.ChannelType.text, discord.ChannelType.voice],
        placeholder="Set excluded channels (replaces the list)...",
        min_values=0, max_values=25, row=0,
    )
    async def exclusions(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        await self.cog.config.set(
            "lvl_excluded_channels", [c.id for c in select.values]
        )
        await interaction.response.edit_message(embed=await self.embed(), view=self)


class LevelingModal(ui.Modal, title="Leveling Rates"):
    def __init__(self, view: LevelingView):
        super().__init__()
        self.view = view
        cfg = view.cog.config
        self.xp_min = ui.TextInput(
            label="Message XP minimum", default=str(cfg.get_int("lvl_msg_xp_min", 15)),
            max_length=6)
        self.xp_max = ui.TextInput(
            label="Message XP maximum", default=str(cfg.get_int("lvl_msg_xp_max", 25)),
            max_length=6)
        self.cooldown = ui.TextInput(
            label="XP cooldown (seconds)", default=str(cfg.get_int("lvl_xp_cooldown", 60)),
            max_length=6)
        self.voice = ui.TextInput(
            label="Voice XP per minute",
            default=str(cfg.get_int("lvl_voice_xp_per_min", 5)), max_length=6)
        for field in (self.xp_min, self.xp_max, self.cooldown, self.voice):
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        values = {}
        for key, field in (
            ("lvl_msg_xp_min", self.xp_min), ("lvl_msg_xp_max", self.xp_max),
            ("lvl_xp_cooldown", self.cooldown), ("lvl_voice_xp_per_min", self.voice),
        ):
            raw = str(field.value).strip()
            if not raw.isdigit():
                await interaction.response.send_message(
                    f"`{field.label}` must be a whole number.", ephemeral=True
                )
                return
            values[key] = int(raw)

        if values["lvl_msg_xp_max"] < values["lvl_msg_xp_min"]:
            await interaction.response.send_message(
                "The maximum cannot be below the minimum.", ephemeral=True
            )
            return

        for key, value in values.items():
            await self.view.cog.config.set(key, value)
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


# --------------------------------------------------------------------------
# Earning rules
# --------------------------------------------------------------------------

class EarningView(AdminView):
    def __init__(self, cog):
        super().__init__(cog)
        self.picker.options = [
            discord.SelectOption(label=meta["name"][:100], value=key)
            for key, meta in EARNING_SOURCES.items()
        ]

    async def embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Earning Rules",
            description="A cap of `—` means uncapped. Once-daily sources pay "
                        "at most once per day regardless of cap.",
            color=discord.Color.blurple(),
        )
        lines = []
        for key, meta in EARNING_SOURCES.items():
            value = self.cog.config.earn_value(key)
            cap = self.cog.config.earn_cap(key)
            if meta["once_daily"]:
                limit = "once daily"
            else:
                limit = f"cap {commas(cap)}/day" if cap > 0 else "uncapped"
            lines.append(f"**{meta['name']}** — {commas(value)} Points · {limit}")
        embed.description += "\n\n" + "\n".join(lines)
        return embed

    @ui.select(placeholder="Choose a source to edit...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        await interaction.response.send_modal(EarningModal(self, select.values[0]))


class EarningModal(ui.Modal):
    def __init__(self, view: EarningView, source: str):
        meta = EARNING_SOURCES[source]
        super().__init__(title=meta["name"][:45])
        self.view = view
        self.source = source
        self.once_daily = meta["once_daily"]

        self.value = ui.TextInput(
            label="Points awarded",
            default=str(view.cog.config.earn_value(source)), max_length=8,
        )
        self.add_item(self.value)

        if not self.once_daily:
            self.cap = ui.TextInput(
                label="Daily cap (0 = uncapped)",
                default=str(view.cog.config.earn_cap(source)), max_length=8,
            )
            self.add_item(self.cap)

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.value.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message(
                "Points must be a whole number.", ephemeral=True
            )
            return
        await self.view.cog.config.set(f"earn_{self.source}_value", int(raw))

        if not self.once_daily:
            cap_raw = str(self.cap.value).strip()
            if not cap_raw.isdigit():
                await interaction.response.send_message(
                    "The cap must be a whole number.", ephemeral=True
                )
                return
            await self.view.cog.config.set(f"earn_{self.source}_cap", int(cap_raw))

        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


# --------------------------------------------------------------------------
# Shop settings
# --------------------------------------------------------------------------

class ShopSettingsView(AdminView):
    def __init__(self, cog):
        super().__init__(cog)
        self.selected = None
        self._refresh_options()

    def _refresh_options(self):
        self.picker.options = [
            discord.SelectOption(
                label=meta["name"][:100],
                value=key,
                description=f"{commas(self.cog.config.price(key))} Points · "
                            f"{'enabled' if self.cog.config.item_enabled(key) else 'disabled'}",
                default=key == self.selected,
            )
            for key, meta in SHOP_ITEMS.items()
        ]
        self.toggle.disabled = self.selected is None
        self.edit_price.disabled = self.selected is None
        if self.selected:
            enabled = self.cog.config.item_enabled(self.selected)
            self.toggle.label = "Disable Item" if enabled else "Enable Item"
            self.toggle.style = (discord.ButtonStyle.danger if enabled
                                 else discord.ButtonStyle.success)

    async def embed(self) -> discord.Embed:
        embed = discord.Embed(title="Shop", color=discord.Color.blurple())
        lines = []
        for key, meta in SHOP_ITEMS.items():
            state = "" if self.cog.config.item_enabled(key) else " *(disabled)*"
            price = commas(self.cog.config.price(key))
            if key == "gif_command":
                price = " / ".join(
                    commas(self.cog.config.gif_price(slot)) for slot in (1, 2, 3)
                )
            elif key == "throne":
                item = self.cog.items.get("throne")
                minimum = await item.minimum_bid()
                price = f"{commas(minimum)} to seize"
            lines.append(f"**{meta['name']}** — {price} Points{state}")
        embed.description = "\n".join(lines)
        embed.add_field(
            name="GIF Command Tiers",
            value=" · ".join(
                f"#{slot}: {commas(self.cog.config.gif_price(slot))}"
                for slot in (1, 2, 3)
            ),
            inline=False,
        )
        embed.set_footer(
            text="The Throne's price is always one more than the current holder paid."
        )
        return embed

    @ui.select(placeholder="Choose an item...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        self.selected = select.values[0]
        self._refresh_options()
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.button(label="Edit Price", style=discord.ButtonStyle.primary, row=1, disabled=True)
    async def edit_price(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PriceModal(self, self.selected))

    @ui.button(label="Toggle", style=discord.ButtonStyle.secondary, row=1, disabled=True)
    async def toggle(self, interaction: discord.Interaction, button: ui.Button):
        current = self.cog.config.item_enabled(self.selected)
        await self.cog.config.set(f"enabled_{self.selected}", 0 if current else 1)
        self._refresh_options()
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.button(label="GIF Tier Prices", style=discord.ButtonStyle.secondary, row=1)
    async def gif_tiers(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(GifTierModal(self))

    @ui.button(label="Item Timings", style=discord.ButtonStyle.secondary, row=2)
    async def timings(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(TimingsModal(self))


class PriceModal(ui.Modal):
    def __init__(self, view: ShopSettingsView, item_key: str):
        super().__init__(title=f"Price — {SHOP_ITEMS[item_key]['name']}"[:45])
        self.view = view
        self.item_key = item_key
        label = ("Starting price" if item_key == "throne" else "Price in Points")
        self.price = ui.TextInput(
            label=label, default=str(view.cog.config.price(item_key)), max_length=10
        )
        self.add_item(self.price)

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.price.value).replace(",", "").strip()
        if not raw.isdigit():
            await interaction.response.send_message(
                "The price must be a whole number.", ephemeral=True
            )
            return
        await self.view.cog.config.set(f"price_{self.item_key}", int(raw))
        self.view._refresh_options()
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


class GifTierModal(ui.Modal, title="GIF Command Tier Prices"):
    def __init__(self, view: ShopSettingsView):
        super().__init__()
        self.view = view
        cfg = view.cog.config
        self.tier1 = ui.TextInput(label="1st command", default=str(cfg.gif_price(1)))
        self.tier2 = ui.TextInput(label="2nd command", default=str(cfg.gif_price(2)))
        self.tier3 = ui.TextInput(label="3rd command", default=str(cfg.gif_price(3)))
        self.limit = ui.TextInput(
            label="Max commands per user",
            default=str(cfg.get_int("gif_max_per_user", 3)), max_length=2,
        )
        for field in (self.tier1, self.tier2, self.tier3, self.limit):
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        for key, field in (
            ("price_gif_1", self.tier1), ("price_gif_2", self.tier2),
            ("price_gif_3", self.tier3), ("gif_max_per_user", self.limit),
        ):
            raw = str(field.value).replace(",", "").strip()
            if not raw.isdigit():
                await interaction.response.send_message(
                    f"`{field.label}` must be a whole number.", ephemeral=True
                )
                return
            await self.view.cog.config.set(key, int(raw))
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


class TimingsModal(ui.Modal, title="Item Timings"):
    def __init__(self, view: ShopSettingsView):
        super().__init__()
        self.view = view
        cfg = view.cog.config
        self.auto_react = ui.TextInput(
            label="Auto React hours", default=str(cfg.get_int("auto_react_hours", 24)))
        self.hof = ui.TextInput(
            label="HoF access window (minutes)",
            default=str(cfg.get_int("hof_grant_minutes", 15)))
        self.emoji = ui.TextInput(
            label="Emoji trial days", default=str(cfg.get_int("emoji_trial_days", 30)))
        self.proxy = ui.TextInput(
            label="Proxy character limit",
            default=str(cfg.get_int("proxy_max_chars", 500)))
        self.queue = ui.TextInput(
            label="Customs queue hours",
            default=str(cfg.get_int("match_queue_hours", 3)))
        for field in (self.auto_react, self.hof, self.emoji, self.proxy, self.queue):
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        for key, field in (
            ("auto_react_hours", self.auto_react), ("hof_grant_minutes", self.hof),
            ("emoji_trial_days", self.emoji), ("proxy_max_chars", self.proxy),
            ("match_queue_hours", self.queue),
        ):
            raw = str(field.value).strip()
            if not raw.isdigit() or int(raw) <= 0:
                await interaction.response.send_message(
                    f"`{field.label}` must be a positive whole number.", ephemeral=True
                )
                return
            await self.view.cog.config.set(key, int(raw))
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


# --------------------------------------------------------------------------
# Mystery Box
# --------------------------------------------------------------------------

class MysteryBoxView(AdminView):
    def __init__(self, cog):
        super().__init__(cog)
        self.selected = None
        self._refresh_options()

    def _refresh_options(self):
        self.picker.options = [
            discord.SelectOption(
                label=meta["name"][:100],
                value=key,
                description=(
                    f"weight {self.cog.config.get_int(f'mbox_{key}_weight', meta['weight'])}"
                    f" · {'on' if self.cog.config.get_bool(f'mbox_{key}_enabled', True) else 'off'}"
                    f" · {meta['kind']}"
                )[:100],
                default=key == self.selected,
            )
            for key, meta in BOX_OUTCOMES.items()
        ]
        self.edit_weight.disabled = self.selected is None
        self.toggle.disabled = self.selected is None
        if self.selected:
            on = self.cog.config.get_bool(f"mbox_{self.selected}_enabled", True)
            self.toggle.label = "Disable Outcome" if on else "Enable Outcome"
            self.toggle.style = (discord.ButtonStyle.danger if on
                                 else discord.ButtonStyle.success)

    async def embed(self) -> discord.Embed:
        weights = self.cog.config.box_weights()
        total = sum(weights.values()) or 1

        embed = discord.Embed(
            title="Mystery Box",
            description=f"Box price: **{commas(self.cog.config.price('mystery_box'))}** "
                        f"Points\nWeights are relative — the percentages below are "
                        f"what players actually see.",
            color=discord.Color.blurple(),
        )

        lines = []
        for key, meta in BOX_OUTCOMES.items():
            weight = self.cog.config.get_int(f"mbox_{key}_weight", meta["weight"])
            if key not in weights:
                lines.append(f"~~{meta['name']}~~ — disabled")
                continue
            pct = weight / total * 100
            bar = "█" * max(1, round(pct / 4))
            lines.append(f"`{pct:5.1f}%` {bar} **{meta['name']}**")
        embed.add_field(name="Odds", value="\n".join(lines), inline=False)
        embed.add_field(
            name="Points Payouts",
            value=f"Small: {commas(self.cog.config.get_int('box_points_small', 250))} · "
                  f"Big: {commas(self.cog.config.get_int('box_points_big', 1500))}",
            inline=False,
        )
        return embed

    @ui.select(placeholder="Choose an outcome...", row=0)
    async def picker(self, interaction: discord.Interaction, select: ui.Select):
        self.selected = select.values[0]
        self._refresh_options()
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.button(label="Edit Weight", style=discord.ButtonStyle.primary, row=1,
               disabled=True)
    async def edit_weight(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(WeightModal(self, self.selected))

    @ui.button(label="Toggle", style=discord.ButtonStyle.secondary, row=1, disabled=True)
    async def toggle(self, interaction: discord.Interaction, button: ui.Button):
        current = self.cog.config.get_bool(f"mbox_{self.selected}_enabled", True)
        await self.cog.config.set(f"mbox_{self.selected}_enabled", 0 if current else 1)
        self._refresh_options()
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.button(label="Payout Amounts", style=discord.ButtonStyle.secondary, row=1)
    async def payouts(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PayoutModal(self))

    @ui.button(label="Curse Settings", style=discord.ButtonStyle.secondary, row=2)
    async def curse_settings(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(CurseSettingsModal(self))


class WeightModal(ui.Modal):
    def __init__(self, view: MysteryBoxView, outcome: str):
        super().__init__(title=f"Weight — {BOX_OUTCOMES[outcome]['name']}"[:45])
        self.view = view
        self.outcome = outcome
        self.weight = ui.TextInput(
            label="Relative weight",
            default=str(view.cog.config.get_int(
                f"mbox_{outcome}_weight", BOX_OUTCOMES[outcome]["weight"])),
            max_length=6,
        )
        self.add_item(self.weight)

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.weight.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message(
                "The weight must be a whole number.", ephemeral=True
            )
            return
        await self.view.cog.config.set(f"mbox_{self.outcome}_weight", int(raw))
        self.view._refresh_options()
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


class PayoutModal(ui.Modal, title="Mystery Box Payouts"):
    def __init__(self, view: MysteryBoxView):
        super().__init__()
        self.view = view
        cfg = view.cog.config
        self.small = ui.TextInput(
            label="Small payout", default=str(cfg.get_int("box_points_small", 250)))
        self.big = ui.TextInput(
            label="Big payout", default=str(cfg.get_int("box_points_big", 1500)))
        self.add_item(self.small)
        self.add_item(self.big)

    async def on_submit(self, interaction: discord.Interaction):
        for key, field in (("box_points_small", self.small), ("box_points_big", self.big)):
            raw = str(field.value).replace(",", "").strip()
            if not raw.isdigit():
                await interaction.response.send_message(
                    f"`{field.label}` must be a whole number.", ephemeral=True
                )
                return
            await self.view.cog.config.set(key, int(raw))
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


class CurseSettingsModal(ui.Modal, title="Curse Settings"):
    def __init__(self, view: MysteryBoxView):
        super().__init__()
        self.view = view
        cfg = view.cog.config
        self.clown = ui.TextInput(
            label="Clown Mode hours", default=str(cfg.get_int("clown_hours", 24)))
        self.slow_len = ui.TextInput(
            label="Slowmo duration (minutes)",
            default=str(cfg.get_int("slowmo_minutes", 60)))
        self.slow_gap = ui.TextInput(
            label="Slowmo gap between messages (minutes)",
            default=str(cfg.get_int("slowmo_interval_minutes", 5)))
        self.sponge_days = ui.TextInput(
            label="SpongeBob duration (days)",
            default=str(cfg.get_int("spongebob_days", 7)))
        self.sponge_chance = ui.TextInput(
            label="SpongeBob chance (%)",
            default=str(cfg.get_int("spongebob_chance", 5)))
        for field in (self.clown, self.slow_len, self.slow_gap,
                      self.sponge_days, self.sponge_chance):
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        for key, field in (
            ("clown_hours", self.clown), ("slowmo_minutes", self.slow_len),
            ("slowmo_interval_minutes", self.slow_gap),
            ("spongebob_days", self.sponge_days),
            ("spongebob_chance", self.sponge_chance),
        ):
            raw = str(field.value).strip()
            if not raw.isdigit():
                await interaction.response.send_message(
                    f"`{field.label}` must be a whole number.", ephemeral=True
                )
                return
            await self.view.cog.config.set(key, int(raw))
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


# --------------------------------------------------------------------------
# Roles and channels
# --------------------------------------------------------------------------

class RolesView(AdminView):
    def __init__(self, cog, *, guild=None, **kwargs):
        super().__init__(cog, **kwargs)
        # Only needed to show what the other cogs' newcomer roles point at.
        self.guild = guild

    async def embed(self) -> discord.Embed:
        embed = discord.Embed(title="Roles", color=discord.Color.blurple())
        embed.add_field(
            name="The Throne", value=_role_value(self.cog, "role_throne"), inline=False)
        embed.add_field(
            name="Vibes Role", value=_role_value(self.cog, "role_vibes"), inline=False)
        embed.add_field(
            name="Newcomer Role",
            value=f"{_role_value(self.cog, 'role_newcomer')}\n"
                  f"*Replies to members with this role earn Points.*",
            inline=False,
        )
        others = (
            await newcomer_role_summary(self.cog.bot, self.guild, exclude="Economy")
            if self.guild else ""
        )
        if others:
            embed.add_field(name="Newcomer role elsewhere", value=others, inline=False)
        embed.set_footer(
            text="The bot's own role must sit above the Throne and Vibes roles "
                 "to assign them."
        )
        return embed

    @ui.select(cls=ui.RoleSelect, placeholder="Set the Throne role...", row=0)
    async def throne(self, interaction: discord.Interaction, select: ui.RoleSelect):
        await self.cog.config.set("role_throne", select.values[0].id)
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.select(cls=ui.RoleSelect, placeholder="Set the Vibes role...", row=1)
    async def vibes(self, interaction: discord.Interaction, select: ui.RoleSelect):
        await self.cog.config.set("role_vibes", select.values[0].id)
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.select(cls=ui.RoleSelect, placeholder="Set the newcomer role...", row=2)
    async def newcomer(self, interaction: discord.Interaction, select: ui.RoleSelect):
        await self.cog.config.set("role_newcomer", select.values[0].id)
        await interaction.response.edit_message(embed=await self.embed(), view=self)


class ChannelsView(AdminView):
    async def embed(self) -> discord.Embed:
        blocked = self.cog.config.get_list("proxy_blocklist")
        embed = discord.Embed(title="Channels", color=discord.Color.blurple())
        embed.add_field(
            name="Approvals",
            value=f"{_channel_value(self.cog, 'ch_approval')}\n"
                  f"*GIF and emoji submissions land here.*",
            inline=False,
        )
        embed.add_field(
            name="Hall of Fame", value=_channel_value(self.cog, "ch_hof"), inline=False)
        embed.add_field(
            name="Proxy Log",
            value=f"{_channel_value(self.cog, 'ch_proxy_log')}\n"
                  f"*Admin-only record of who sent each anonymous message.*",
            inline=False,
        )
        embed.add_field(
            name="Customs Queue Override",
            value=f"{_channel_value(self.cog, 'ch_match_queue')}\n"
                  f"*Leave unset to use each game's own queue channel.*",
            inline=False,
        )
        embed.add_field(
            name="Proxy Blocklist",
            value=", ".join(f"<#{c}>" for c in blocked) if blocked else "*None*",
            inline=False,
        )
        return embed

    @ui.select(
        cls=ui.ChannelSelect, channel_types=[discord.ChannelType.text],
        placeholder="Set the approval channel...", row=0,
    )
    async def approval(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        await self.cog.config.set("ch_approval", select.values[0].id)
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.select(
        cls=ui.ChannelSelect, channel_types=[discord.ChannelType.text],
        placeholder="Set the Hall of Fame channel...", row=1,
    )
    async def hof(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        await self.cog.config.set("ch_hof", select.values[0].id)
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.select(
        cls=ui.ChannelSelect, channel_types=[discord.ChannelType.text],
        placeholder="Set the proxy log channel...", row=2,
    )
    async def proxy_log(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        await self.cog.config.set("ch_proxy_log", select.values[0].id)
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.select(
        cls=ui.ChannelSelect, channel_types=[discord.ChannelType.text],
        placeholder="Set the Proxy blocklist (replaces the list)...",
        min_values=0, max_values=25, row=3,
    )
    async def blocklist(self, interaction: discord.Interaction, select: ui.ChannelSelect):
        await self.cog.config.set("proxy_blocklist", [c.id for c in select.values])
        await interaction.response.edit_message(embed=await self.embed(), view=self)


# --------------------------------------------------------------------------
# Members
# --------------------------------------------------------------------------

class MembersView(AdminView):
    def __init__(self, cog):
        super().__init__(cog)
        self.target = None
        self._sync()

    def _sync(self):
        has_target = self.target is not None
        for button in (self.grant, self.take, self.ledger, self.wipe, self.reset):
            button.disabled = not has_target

    async def embed(self) -> discord.Embed:
        embed = discord.Embed(title="Members", color=discord.Color.blurple())
        if self.target is None:
            embed.description = "Choose a member to manage."
            return embed

        row = await self.cog.db.get_user(self.target.id)
        streak = await self.cog.earning.get_streak(self.target.id)
        rank, total = await self.cog.leveling.get_rank(self.target.id)
        owned = await self.cog.db.get_inventory(self.target.id, states=('owned',))
        effects = await self.cog.effects.effects_for(self.target.id)

        embed.description = f"Managing {self.target.mention}"
        embed.add_field(name="Points", value=commas(row["points"]))
        embed.add_field(name="Lifetime", value=commas(row["lifetime_points"]))
        embed.add_field(name="Level", value=f"{row['level']} (#{rank or '—'} of {total})")
        embed.add_field(
            name="Streak",
            value=f"{(streak['current_streak'] if streak else 0)} days",
        )
        embed.add_field(name="Items Owned", value=str(len(owned)))
        embed.add_field(name="Active Effects", value=str(len(effects)))
        embed.set_thumbnail(url=self.target.display_avatar.url)
        return embed

    @ui.select(cls=ui.UserSelect, placeholder="Choose a member...", row=0)
    async def pick(self, interaction: discord.Interaction, select: ui.UserSelect):
        self.target = select.values[0]
        self._sync()
        await interaction.response.edit_message(embed=await self.embed(), view=self)

    @ui.button(label="Grant Points", style=discord.ButtonStyle.success, row=1,
               disabled=True)
    async def grant(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(AdjustModal(self, positive=True))

    @ui.button(label="Remove Points", style=discord.ButtonStyle.danger, row=1,
               disabled=True)
    async def take(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(AdjustModal(self, positive=False))

    @ui.button(label="View Ledger", style=discord.ButtonStyle.secondary, row=1,
               disabled=True)
    async def ledger(self, interaction: discord.Interaction, button: ui.Button):
        rows = await self.cog.earning.recent_ledger(self.target.id, limit=20)
        embed = discord.Embed(
            title=f"Ledger — {self.target.display_name}",
            color=discord.Color.blurple(),
        )
        if not rows:
            embed.description = "*No Points movement recorded.*"
        else:
            embed.description = "\n".join(
                f"<t:{r['ts']}:d> `{r['delta']:+,}` — {r['source']}" for r in rows
            )[:4000]
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @ui.button(label="Wipe Curses", style=discord.ButtonStyle.secondary, row=2,
               disabled=True)
    async def wipe(self, interaction: discord.Interaction, button: ui.Button):
        effects = await self.cog.effects.effects_for(self.target.id)
        if not effects:
            await interaction.response.send_message(
                "Nothing is active on them.", ephemeral=True
            )
            return
        for row in effects:
            await self.cog.effects.clear_effect(row)
        await interaction.response.send_message(
            f"Cleared {len(effects)} effect(s) from {self.target.mention}.",
            ephemeral=True,
        )

    @ui.button(label="Reset Member", style=discord.ButtonStyle.danger, row=2,
               disabled=True)
    async def reset(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Reset this member?",
                description=f"This wipes {self.target.mention}'s XP, level, Points, "
                            f"streak and inventory. The ledger is kept as a record.\n\n"
                            f"This cannot be undone.",
                color=discord.Color.red(),
            ),
            view=ConfirmResetView(self.cog, self.target),
            ephemeral=True,
        )


class AdjustModal(ui.Modal):
    def __init__(self, view: MembersView, *, positive: bool):
        super().__init__(title="Grant Points" if positive else "Remove Points")
        self.view = view
        self.positive = positive
        self.amount = ui.TextInput(label="Amount", max_length=10)
        self.reason = ui.TextInput(
            label="Reason (recorded in the ledger)", required=False, max_length=100
        )
        self.add_item(self.amount)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.amount.value).replace(",", "").strip()
        if not raw.isdigit() or int(raw) <= 0:
            await interaction.response.send_message(
                "Enter a positive whole number.", ephemeral=True
            )
            return
        delta = int(raw) if self.positive else -int(raw)
        await self.view.cog.db.adjust_points(
            self.view.target.id, delta, "admin",
            {"by": interaction.user.id, "reason": str(self.reason.value or "")},
        )
        await interaction.response.edit_message(
            embed=await self.view.embed(), view=self.view
        )


class ConfirmResetView(ui.View):
    def __init__(self, cog, target):
        super().__init__(timeout=120)
        self.cog = cog
        self.target = target

    @ui.button(label="Confirm Reset", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        uid = self.target.id
        await self.cog.db.execute(
            "UPDATE users SET xp = 0, level = 0, points = 0, lifetime_points = 0, "
            "messages = 0, voice_seconds = 0 WHERE user_id = ?", (uid,)
        )
        await self.cog.db.execute("DELETE FROM streaks WHERE user_id = ?", (uid,))
        await self.cog.db.execute("DELETE FROM voice_accum WHERE user_id = ?", (uid,))
        await self.cog.db.execute("DELETE FROM daily_activity WHERE user_id = ?", (uid,))
        await self.cog.db.execute(
            "UPDATE inventory SET state = 'expired' WHERE user_id = ? AND state = 'owned'",
            (uid,),
        )
        for row in await self.cog.effects.effects_for(uid):
            await self.cog.effects.clear_effect(row)

        await interaction.response.edit_message(
            content=f"{self.target.mention} has been reset.", embed=None, view=None
        )
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(
            content="Cancelled.", embed=None, view=None
        )
        self.stop()


# --------------------------------------------------------------------------
# Maintenance
# --------------------------------------------------------------------------

class MaintenanceView(AdminView):
    async def embed(self) -> discord.Embed:
        embed = discord.Embed(title="Maintenance", color=discord.Color.blurple())

        effects = await self.cog.db.get_effects()
        if effects:
            from .items.curses import CURSE_LABELS
            embed.add_field(
                name=f"Active Effects ({len(effects)})",
                value="\n".join(
                    f"{CURSE_LABELS.get(e['effect_key'], e['effect_key'])} on "
                    f"<@{e['target_id']}> — ends <t:{e['expires_ts']}:R>"
                    for e in effects[:10]
                )[:1024],
                inline=False,
            )
        else:
            embed.add_field(name="Active Effects", value="*None*", inline=False)

        from . import approvals as approvals_module
        pending = await approvals_module.pending(self.cog)
        embed.add_field(
            name=f"Pending Approvals ({len(pending)})",
            value="\n".join(
                f"#{p['id']} · {p['kind']} · <@{p['user_id']}>" for p in pending[:10]
            ) or "*None*",
            inline=False,
        )

        gifs = await self.cog.db.fetchall(
            "SELECT name, user_id FROM gif_commands ORDER BY approved_ts DESC LIMIT 15"
        )
        embed.add_field(
            name=f"Live GIF Commands ({len(gifs)})",
            value=", ".join(f"`!{g['name']}`" for g in gifs) or "*None*",
            inline=False,
        )

        emojis = await self.cog.db.fetchall(
            "SELECT name, status, expires_ts FROM shop_emojis "
            "WHERE status IN ('trial', 'permanent') ORDER BY added_ts DESC LIMIT 10"
        )
        embed.add_field(
            name="Shop Emojis",
            value="\n".join(
                f"`:{e['name']}:` — {e['status']}"
                + (f", ends <t:{e['expires_ts']}:R>" if e["expires_ts"] else "")
                for e in emojis
            ) or "*None*",
            inline=False,
        )
        return embed

    @ui.button(label="Recalculate Levels", style=discord.ButtonStyle.primary, row=0)
    async def recalc(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        changed = await self.cog.leveling.recalculate_levels()
        await interaction.followup.send(
            f"Recalculated every stored level. {changed} changed.", ephemeral=True
        )

    @ui.button(label="Run Upkeep Now", style=discord.ButtonStyle.secondary, row=0)
    async def upkeep(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.cog.effects.expire_tick()
        await self.cog.items.maintenance(self.cog)
        await interaction.followup.send(
            "Upkeep run — effects expired, item maintenance complete.", ephemeral=True
        )

    @ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=0)
    async def refresh(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(embed=await self.embed(), view=self)
