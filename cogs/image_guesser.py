import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import random
import os
import io
import json
import logging
import uuid
from PIL import Image
from thefuzz import fuzz
from cogs.image_guesser_fetcher import ImageFetcher

# =====================================================================================
# UTILS & CONSTANTS
# =====================================================================================

log = logging.getLogger("discord.image_guesser")

_cog_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(_cog_dir, '..', 'image_guesser_config.json')
DB_PATH = os.path.join(_cog_dir, '..', 'image_guesser.db')
ASSETS_DIR = os.path.join(_cog_dir, '..', 'assets', 'image_guesser')

EMBED_COLOR = 0x3498DB  # Blue
VALID_IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.webp', '.gif'}
DEFAULT_REVEAL_STAGES = 3
DEFAULT_REVEAL_INTERVAL_MINS = 240
DEFAULT_FUZZY_THRESHOLD = 80
DEFAULT_GUESSES_PER_STAGE = 3
DEFAULT_POST_TIME = "12:00"
DEFAULT_TIMEZONE = "America/Chicago"

CATEGORIES = [
    "Name this Location",
    "Name this Country",
    "Name this Movie",
    "Name this TV Show",
    "Name this Video Game",
    "Name this Album",
    "Name this Person",
    "Name this Character",
    "Name this Anime",
    "Name this Historical Event",
]

# Day-of-week category rotation (0 = Monday, 6 = Sunday)
DAILY_CATEGORIES = {
    0: ["Name this Movie", "Name this TV Show"],           # Monday
    1: ["Name this Person"],                                # Tuesday
    2: ["Name this Location", "Name this Country"],         # Wednesday
    3: ["Name this Anime"],                                 # Thursday
    4: ["Name this Video Game"],                            # Friday
    5: ["Name this Character", "Name this Album"],          # Saturday
    6: ["Name this Historical Event"],                      # Sunday
}


def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.error(f"Error loading image guesser config: {e}")
    return {"guild_settings": {}}


def save_config(config):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
    except IOError as e:
        log.error(f"Error saving image guesser config: {e}")


def _crop_center_for_image(file_path: str, w: int, h: int) -> tuple[float, float]:
    """Deterministic random focal point for an image based on its file path.
    Returns (cx, cy) as fractions (0.0-1.0) biased away from edges."""
    import hashlib
    seed = int(hashlib.md5(file_path.encode()).hexdigest(), 16)
    # Generate two pseudo-random floats in 0.2–0.8 range to avoid dead edges
    cx = 0.2 + ((seed >> 0) % 10000) / 10000 * 0.6
    cy = 0.2 + ((seed >> 16) % 10000) / 10000 * 0.6
    return cx, cy


def process_stage(image: Image.Image, stage: int, total_stages: int,
                  file_path: str = "") -> Image.Image:
    """Apply crop (zoom) and pixelation for a given reveal stage.

    Stage 1 = very tight crop on a random spot + extreme pixelation.
    Middle stages stay heavily obscured — the game should be hard.
    Second-to-last stage is where things start becoming recognizable.
    Last stage = full image, fully clear.
    """
    w, h = image.size
    progress = (stage - 1) / (total_stages - 1) if total_stages > 1 else 1.0

    # --- Crop / zoom ---
    # Hand-tuned crop anchors per stage (fraction of full image).
    # Interpolated for any total_stages count; final stage is always 100%.
    _CROP_ANCHORS = {
        1: 0.17,   # stage 1: 17%
        2: 0.21,   # stage 2: 21%
        3: 0.26,   # stage 3: 26%
        4: 0.42,   # stage 4: 42%
        5: 0.67,   # stage 5: 67%
    }
    if stage >= total_stages:
        crop_frac = 1.0
    elif total_stages <= 1:
        crop_frac = 1.0
    else:
        anchor_pos = 1 + (stage - 1) / (total_stages - 1) * 4  # 1.0 → 5.0
        lo = max(1, int(anchor_pos))
        hi = min(5, lo + 1)
        t = anchor_pos - lo
        crop_frac = _CROP_ANCHORS[lo] * (1 - t) + _CROP_ANCHORS[hi] * t

    crop_w = max(1, int(w * crop_frac))
    crop_h = max(1, int(h * crop_frac))

    # Random focal point (deterministic per image) — drifts toward center as crop widens
    cx_frac, cy_frac = _crop_center_for_image(file_path, w, h)
    # Blend toward center as crop_frac grows (at 100% it must be centered)
    cx = int(cx_frac * w * (1 - progress) + (w / 2) * progress)
    cy = int(cy_frac * h * (1 - progress) + (h / 2) * progress)

    # Clamp crop box within image bounds
    left = max(0, min(cx - crop_w // 2, w - crop_w))
    top = max(0, min(cy - crop_h // 2, h - crop_h))

    cropped = image.crop((left, top, left + crop_w, top + crop_h))
    cropped = cropped.resize((w, h), Image.LANCZOS)

    # --- Pixelation ---
    # Final stage = crystal clear, no pixelation.
    if stage >= total_stages:
        return cropped
    # Hand-tuned clarity per stage (fraction of original resolution).
    # Builds a lookup for any total_stages count by interpolating these anchor points.
    _CLARITY_ANCHORS = {
        1: 0.03,   # stage 1: 3%
        2: 0.04,   # stage 2: 4%
        3: 0.08,   # stage 3: 8%
        4: 0.15,   # stage 4: 15%
        5: 0.40,   # stage 5: 40%
    }
    # Map current stage (1-based) into the anchor table
    if total_stages <= 1:
        scale = 1.0
    else:
        # Normalise stage position into 1-5 anchor range
        anchor_pos = 1 + (stage - 1) / (total_stages - 1) * 4  # 1.0 → 5.0
        lo = max(1, int(anchor_pos))
        hi = min(5, lo + 1)
        t = anchor_pos - lo
        scale = _CLARITY_ANCHORS[lo] * (1 - t) + _CLARITY_ANCHORS[hi] * t
    small_w = max(4, int(w * scale))
    small_h = max(4, int(h * scale))
    small = cropped.resize((small_w, small_h), Image.BILINEAR)
    return small.resize((w, h), Image.NEAREST)


def image_to_bytes(image: Image.Image) -> io.BytesIO:
    """Convert a PIL Image to a BytesIO buffer for Discord upload."""
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    buf.seek(0)
    return buf


async def is_guesser_admin_check(interaction: discord.Interaction) -> bool:
    cog = interaction.client.get_cog("ImageGuesser")
    if not cog:
        return await interaction.client.is_owner(interaction.user)
    return interaction.client.is_bot_admin(interaction.user)


# =====================================================================================
# ADMIN VIEWS
# =====================================================================================

class GuesserChannelSelectView(discord.ui.View):
    def __init__(self, cog: "ImageGuesser"):
        super().__init__(timeout=120)
        self.cog = cog

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="Select a channel...",
        min_values=1, max_values=1
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["channel_id"] = channel.id
            self.cog.config_is_dirty = True

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"Channel set to {channel.mention}.",
            view=self
        )

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class PostTimeModal(discord.ui.Modal, title="Set Post Time"):
    post_time = discord.ui.TextInput(
        label="Post time (HH:MM, 24-hour format)",
        placeholder="12:00",
        required=True,
        max_length=5,
        default=DEFAULT_POST_TIME
    )
    tz = discord.ui.TextInput(
        label="Timezone (e.g. America/Chicago)",
        placeholder=DEFAULT_TIMEZONE,
        required=True,
        max_length=40,
        default=DEFAULT_TIMEZONE
    )

    def __init__(self, cog: "ImageGuesser"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        time_str = self.post_time.value.strip()
        tz_str = self.tz.value.strip()

        # Validate time
        try:
            parts = time_str.split(":")
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (ValueError, IndexError):
            return await interaction.response.send_message("Invalid time format. Use HH:MM (e.g. 14:30).", ephemeral=True)

        # Validate timezone
        try:
            ZoneInfo(tz_str)
        except (KeyError, Exception):
            return await interaction.response.send_message(f"Invalid timezone: `{tz_str}`. Use IANA format (e.g. `America/Chicago`).", ephemeral=True)

        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["post_time"] = time_str
            cfg["timezone"] = tz_str
            self.cog.config_is_dirty = True

        await interaction.response.send_message(f"Post time set to **{time_str}** ({tz_str}).", ephemeral=True)


class RevealSettingsModal(discord.ui.Modal, title="Reveal Settings"):
    stages = discord.ui.TextInput(
        label="Number of reveal stages (2-5)",
        placeholder="3",
        required=True,
        max_length=1,
    )
    interval = discord.ui.TextInput(
        label="Minutes between reveals (5-1440)",
        placeholder="240",
        required=True,
        max_length=4,
    )

    def __init__(self, cog: "ImageGuesser", guild_id: int):
        super().__init__()
        self.cog = cog
        cfg = cog.get_guild_settings(guild_id)
        self.stages.default = str(cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES))
        self.interval.default = str(cfg.get("reveal_interval_mins", DEFAULT_REVEAL_INTERVAL_MINS))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            stages_val = int(self.stages.value.strip())
            interval_val = int(self.interval.value.strip())
            if not (2 <= stages_val <= 5):
                return await interaction.response.send_message("Stages must be between 2 and 5.", ephemeral=True)
            if not (5 <= interval_val <= 1440):
                return await interaction.response.send_message("Interval must be between 5 and 1440 minutes.", ephemeral=True)
        except ValueError:
            return await interaction.response.send_message("Please enter valid numbers.", ephemeral=True)

        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["reveal_stages"] = stages_val
            cfg["reveal_interval_mins"] = interval_val
            self.cog.config_is_dirty = True

        await interaction.response.send_message(
            f"Reveal settings updated: **{stages_val} stages**, every **{interval_val} minute(s)**.",
            ephemeral=True
        )


class FuzzyThresholdModal(discord.ui.Modal, title="Fuzzy Match Threshold"):
    threshold = discord.ui.TextInput(
        label="Match threshold (50-100, lower = more lenient)",
        placeholder="80",
        required=True,
        max_length=3,
        default=str(DEFAULT_FUZZY_THRESHOLD)
    )

    def __init__(self, cog: "ImageGuesser"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = int(self.threshold.value.strip())
            if not (50 <= val <= 100):
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Threshold must be a number between 50 and 100.", ephemeral=True)

        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["fuzzy_threshold"] = val
            self.cog.config_is_dirty = True

        await interaction.response.send_message(f"Fuzzy match threshold set to **{val}**.", ephemeral=True)


class GuessLimitModal(discord.ui.Modal, title="Guess Limit Per Stage"):
    limit = discord.ui.TextInput(
        label="Max guesses per user per stage (1-20, 0 = unlimited)",
        placeholder="3",
        required=True,
        max_length=2,
        default=str(DEFAULT_GUESSES_PER_STAGE)
    )

    def __init__(self, cog: "ImageGuesser"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = int(self.limit.value.strip())
            if not (0 <= val <= 20):
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Must be a number between 0 and 20 (0 = unlimited).", ephemeral=True)

        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["guesses_per_stage"] = val
            self.cog.config_is_dirty = True

        label = f"**{val}** guess(es) per stage" if val > 0 else "**unlimited** guesses"
        await interaction.response.send_message(f"Guess limit set to {label}.", ephemeral=True)


class GuesserAdminPanelView(discord.ui.View):
    def __init__(self, cog: "ImageGuesser"):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(label="Set Channel", style=discord.ButtonStyle.primary, row=0)
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Select the channel for the daily image game:",
            view=GuesserChannelSelectView(self.cog),
            ephemeral=True
        )

    @discord.ui.button(label="Set Post Time", style=discord.ButtonStyle.primary, row=0)
    async def set_post_time(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PostTimeModal(self.cog))

    @discord.ui.button(label="Reveal Settings", style=discord.ButtonStyle.primary, row=0)
    async def reveal_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RevealSettingsModal(self.cog, interaction.guild.id))

    @discord.ui.button(label="Enable / Disable", style=discord.ButtonStyle.secondary, row=1)
    async def toggle_enabled(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with self.cog.config_lock:
            cfg = self.cog.get_guild_settings(interaction.guild.id)
            cfg["enabled"] = not cfg.get("enabled", False)
            new_state = cfg["enabled"]
            self.cog.config_is_dirty = True

        await interaction.response.send_message(
            f"Image Guesser is now **{'enabled' if new_state else 'disabled'}**.",
            ephemeral=True
        )

    @discord.ui.button(label="Fuzzy Threshold", style=discord.ButtonStyle.secondary, row=1)
    async def fuzzy_threshold(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(FuzzyThresholdModal(self.cog))

    @discord.ui.button(label="Guess Limit", style=discord.ButtonStyle.secondary, row=1)
    async def guess_limit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(GuessLimitModal(self.cog))

    @discord.ui.button(label="Force Post", style=discord.ButtonStyle.success, row=2)
    async def force_post(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        cfg = self.cog.get_guild_settings(interaction.guild.id)
        active = cfg.get("active_game")
        if active and not active.get("solved"):
            return await interaction.followup.send("There's already an active game. End it first with **Skip / End Game**.", ephemeral=True)
        # Clear solved game if present
        if active and active.get("solved"):
            async with self.cog.config_lock:
                cfg["active_game"] = None
                self.cog.config_is_dirty = True
        try:
            result = await self.cog._start_daily_game(interaction.guild)
            if result:
                await interaction.followup.send("Game posted!", ephemeral=True)
            else:
                await interaction.followup.send("Failed to post. Check the queue and channel settings.", ephemeral=True)
        except Exception as e:
            log.error(f"Force post error: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    @discord.ui.button(label="Force Reveal", style=discord.ButtonStyle.secondary, row=2)
    async def force_reveal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await self.cog._advance_reveal(interaction.guild)
            if result:
                await interaction.followup.send("Revealed next stage!", ephemeral=True)
            else:
                await interaction.followup.send("No active game to reveal, or already fully revealed.", ephemeral=True)
        except Exception as e:
            log.error(f"Force reveal error: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    @discord.ui.button(label="Skip Stage", style=discord.ButtonStyle.secondary, row=2)
    async def skip_stage(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        cfg = self.cog.get_guild_settings(interaction.guild.id)
        active = cfg.get("active_game")
        if not active or active.get("solved"):
            return await interaction.followup.send("No active game to advance.", ephemeral=True)
        stages = cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES)
        current = active.get("current_stage", 1)
        if current >= stages:
            await self.cog._end_game(interaction.guild, reason="time_up")
            return await interaction.followup.send(f"Was on the final stage — ended the game and revealed the answer.", ephemeral=True)
        try:
            result = await self.cog._advance_reveal(interaction.guild)
            if result:
                await interaction.followup.send(f"Skipped to stage {current + 1}/{stages}.", ephemeral=True)
            else:
                await interaction.followup.send("Failed to advance stage.", ephemeral=True)
        except Exception as e:
            log.error(f"Skip stage error: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    @discord.ui.button(label="Skip / End Game", style=discord.ButtonStyle.danger, row=2)
    async def skip_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        cfg = self.cog.get_guild_settings(interaction.guild.id)
        if not cfg.get("active_game"):
            return await interaction.followup.send("No active game to end.", ephemeral=True)
        try:
            await self.cog._end_game(interaction.guild, reason="skipped")
            await interaction.followup.send("Current game ended.", ephemeral=True)
        except Exception as e:
            log.error(f"Skip game error: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    @discord.ui.button(label="Info & Status", style=discord.ButtonStyle.secondary, row=3)
    async def info_status(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        cfg = self.cog.get_guild_settings(interaction.guild.id)
        active = cfg.get("active_game")

        # Queue count
        queue_count = 0
        try:
            async with aiosqlite.connect(self.cog.db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM images WHERE used = 0 AND guild_id = ?", (interaction.guild.id,)) as c:
                    queue_count = (await c.fetchone())[0]
        except Exception as e:
            log.error(f"Error reading queue count: {e}")

        embed = discord.Embed(title="Image Guesser Info", color=EMBED_COLOR)
        embed.add_field(
            name="Settings",
            value=(
                f"**Channel:** {'<#' + str(cfg['channel_id']) + '>' if cfg.get('channel_id') else 'Not set'}\n"
                f"**Enabled:** {cfg.get('enabled', False)}\n"
                f"**Post Time:** {cfg.get('post_time', DEFAULT_POST_TIME)} ({cfg.get('timezone', DEFAULT_TIMEZONE)})\n"
                f"**Stages:** {cfg.get('reveal_stages', DEFAULT_REVEAL_STAGES)}\n"
                f"**Interval:** {cfg.get('reveal_interval_mins', DEFAULT_REVEAL_INTERVAL_MINS)}m\n"
                f"**Fuzzy Threshold:** {cfg.get('fuzzy_threshold', DEFAULT_FUZZY_THRESHOLD)}\n"
                f"**Guesses/Stage:** {cfg.get('guesses_per_stage', DEFAULT_GUESSES_PER_STAGE) or 'Unlimited'}"
            ),
            inline=False
        )
        embed.add_field(
            name="Queue",
            value=f"**Images waiting:** {queue_count}",
            inline=False
        )

        if active:
            embed.add_field(
                name="Active Game",
                value=(
                    f"**Image ID:** {active.get('image_id')}\n"
                    f"**Stage:** {active.get('current_stage', 0)}/{cfg.get('reveal_stages', DEFAULT_REVEAL_STAGES)}\n"
                    f"**Solved:** {active.get('solved', False)}\n"
                    f"**Thread:** <#{active.get('thread_id', 0)}>\n"
                    f"**Started:** {active.get('started_at', 'Unknown')}"
                ),
                inline=False
            )
        else:
            embed.add_field(name="Active Game", value="No active game.", inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="View Queue", style=discord.ButtonStyle.secondary, row=3)
    async def view_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        images = []
        try:
            async with aiosqlite.connect(self.cog.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM images WHERE used = 0 AND guild_id = ? ORDER BY added_at ASC LIMIT 25", (interaction.guild.id,)) as c:
                    rows = await c.fetchall()
                images = [dict(r) for r in rows]
        except Exception as e:
            log.error(f"Error reading queue: {e}", exc_info=True)
            return await interaction.followup.send(f"Error: {e}", ephemeral=True)

        if not images:
            return await interaction.followup.send("The queue is empty. Use **Upload Image** to add images.", ephemeral=True)

        lines = []
        for img in images:
            lines.append(
                f"**#{img['id']}** — {img['answer']}"
                f" | {img.get('category') or 'No category'}"
                f" | by <@{img['added_by']}>"
            )

        embed = discord.Embed(
            title=f"Image Queue ({len(images)} image{'s' if len(images) != 1 else ''})",
            description="\n".join(lines),
            color=EMBED_COLOR,
        )
        await interaction.followup.send(embed=embed, view=QueueRemoveView(self.cog, images), ephemeral=True)

    @discord.ui.button(label="Reset Queue", style=discord.ButtonStyle.danger, row=3)
    async def reset_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with aiosqlite.connect(self.cog.db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM images WHERE used = 1 AND guild_id = ?", (interaction.guild.id,)) as c:
                    count = (await c.fetchone())[0]
                await db.execute("UPDATE images SET used = 0, used_date = NULL WHERE guild_id = ?", (interaction.guild.id,))
                await db.commit()
            await interaction.followup.send(f"Reset **{count}** used image(s) back to the queue.", ephemeral=True)
        except Exception as e:
            log.error(f"Error resetting queue: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    @discord.ui.button(label="Upload Image", style=discord.ButtonStyle.success, row=4)
    async def upload_image(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "**Step 1:** Select a category for the image:",
            view=UploadCategoryView(self.cog),
            ephemeral=True,
        )

    @discord.ui.button(label="Auto-Fill Queue", style=discord.ButtonStyle.success, row=4)
    async def auto_fill(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            results = await self.cog.fetcher.auto_fill(interaction.guild.id)
            if results:
                lines = [f"**{cat}:** +{n}" for cat, n in results.items()]
                total = sum(results.values())
                await interaction.followup.send(
                    f"Auto-filled **{total}** image(s):\n" + "\n".join(lines),
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "All categories are already at minimum queue depth. No images fetched.",
                    ephemeral=True
                )
        except Exception as e:
            log.error(f"Manual auto-fill error: {e}", exc_info=True)
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class UploadCategoryView(discord.ui.View):
    """Step 1 of upload flow: pick a category."""
    def __init__(self, cog: "ImageGuesser"):
        super().__init__(timeout=120)
        self.cog = cog
        options = [discord.SelectOption(label=cat, value=cat) for cat in CATEGORIES]
        self.select = discord.ui.Select(
            placeholder="Select a category...",
            options=options,
            min_values=1, max_values=1
        )
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        category = self.select.values[0]
        await interaction.response.send_modal(UploadDetailsModal(self.cog, category))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class UploadDetailsModal(discord.ui.Modal, title="Image Details"):
    answer = discord.ui.TextInput(
        label="Accepted answers (comma-separated)",
        placeholder="Eiffel Tower, The Eiffel Tower",
        required=True,
        max_length=200,
    )
    hint = discord.ui.TextInput(
        label="Hint (optional)",
        placeholder="Located in Europe",
        required=False,
        max_length=200,
    )

    def __init__(self, cog: "ImageGuesser", category: str):
        super().__init__()
        self.cog = cog
        self.category = category

    async def on_submit(self, interaction: discord.Interaction):
        # Store pending upload state on the cog
        self.cog.pending_uploads[interaction.user.id] = {
            "guild_id": interaction.guild.id,
            "channel_id": interaction.channel.id,
            "category": self.category,
            "answer": self.answer.value.strip(),
            "hint": self.hint.value.strip() or None,
        }
        await interaction.response.send_message(
            f"**Category:** {self.category}\n**Answer:** {self.answer.value}\n\n"
            "Now **send the image** as your next message in this channel.",
            ephemeral=True,
        )


class QueueRemoveView(discord.ui.View):
    """View for removing images from the queue by selecting from a dropdown."""
    def __init__(self, cog: "ImageGuesser", images: list[dict]):
        super().__init__(timeout=120)
        self.cog = cog
        options = []
        for img in images[:25]:  # Discord max 25 options
            label = f"#{img['id']}: {img['answer'][:90]}"
            desc = f"Category: {img.get('category') or 'None'}"
            options.append(discord.SelectOption(label=label, description=desc, value=str(img['id'])))
        self.select = discord.ui.Select(
            placeholder="Select an image to remove...",
            options=options,
            min_values=1, max_values=1
        )
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        if not interaction.client.is_bot_admin(interaction.user):
            return await interaction.response.send_message("You don't have permission for this.", ephemeral=True)
        image_id = int(self.select.values[0])
        try:
            async with aiosqlite.connect(self.cog.db_path) as db:
                # Get the file path to delete the image file too
                async with db.execute("SELECT file_path FROM images WHERE id = ?", (image_id,)) as c:
                    row = await c.fetchone()
                await db.execute("DELETE FROM images WHERE id = ?", (image_id,))
                await db.commit()

            # Delete the actual image file
            if row and row[0] and os.path.exists(row[0]):
                os.remove(row[0])

            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(
                content=f"Removed image **#{image_id}** from the queue.",
                view=self
            )
        except Exception as e:
            log.error(f"Error removing image: {e}", exc_info=True)
            await interaction.response.send_message(f"Error: {e}", ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


# =====================================================================================
# MAIN COG
# =====================================================================================

class ImageGuesser(commands.Cog, name="ImageGuesser"):
    image_guesser = app_commands.Group(name="image_guesser", description="Where in the World? image guessing game.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = load_config()
        self.config_lock = asyncio.Lock()
        self.config_is_dirty = False
        self.db_path = DB_PATH
        self.assets_dir = ASSETS_DIR
        self.pending_uploads = {}  # user_id → {guild_id, channel_id, category, answer, hint}
        self.fetcher = ImageFetcher()
        os.makedirs(self.assets_dir, exist_ok=True)
        self.bot.loop.create_task(self._init_db())

    async def _init_db(self):
        """Create the images table if it doesn't exist."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                answer TEXT NOT NULL,
                category TEXT DEFAULT NULL,
                hint TEXT DEFAULT NULL,
                added_by INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                used INTEGER DEFAULT 0,
                used_date TEXT DEFAULT NULL
            )''')
            # Migrate: add guild_id column if missing (existing DBs)
            try:
                await db.execute("SELECT guild_id FROM images LIMIT 1")
            except Exception:
                await db.execute("ALTER TABLE images ADD COLUMN guild_id INTEGER NOT NULL DEFAULT 0")
                log.info("Migrated images table: added guild_id column.")
            await db.commit()
        log.info("Image guesser DB initialized.")

    def get_guild_settings(self, guild_id: int) -> dict:
        gid = str(guild_id)
        gs = self.config.setdefault("guild_settings", {})
        if gid not in gs:
            gs[gid] = {
                "enabled": False,
                "channel_id": None,
                "post_time": DEFAULT_POST_TIME,
                "timezone": DEFAULT_TIMEZONE,
                "reveal_stages": DEFAULT_REVEAL_STAGES,
                "reveal_interval_mins": DEFAULT_REVEAL_INTERVAL_MINS,
                "fuzzy_threshold": DEFAULT_FUZZY_THRESHOLD,
                "guesses_per_stage": DEFAULT_GUESSES_PER_STAGE,
                "active_game": None,
            }
        return gs[gid]

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.game_loop.is_running():
            self.game_loop.start()
        if not self.backup_save_loop.is_running():
            self.backup_save_loop.start()
        if not self.auto_fill_loop.is_running():
            self.auto_fill_loop.start()
        log.info("ImageGuesser cog is ready.")

    async def cog_unload(self):
        if self.config_is_dirty:
            save_config(self.config)
        self.game_loop.cancel()
        self.backup_save_loop.cancel()
        self.auto_fill_loop.cancel()
        await self.fetcher.close()

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        log.error(f"Error in command '{interaction.command.name}': {error}", exc_info=True)
        msg = "You don't have permission for this." if isinstance(error, app_commands.CheckFailure) else "An unexpected error occurred."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass

    # ---------------------------------------------------------------------------------
    # Config persistence
    # ---------------------------------------------------------------------------------

    @tasks.loop(seconds=60)
    async def backup_save_loop(self):
        try:
            if self.config_is_dirty:
                async with self.config_lock:
                    save_config(self.config)
                    self.config_is_dirty = False
        except Exception as e:
            await self.bot.error_reporter.report("ImageGuesser", f"backup_save_loop: {e}")

    @backup_save_loop.before_loop
    async def before_backup_save_loop(self):
        await self.bot.wait_until_ready()

    # ---------------------------------------------------------------------------------
    # Auto-fill loop — tops up image queue from APIs every 6 hours
    # ---------------------------------------------------------------------------------

    @tasks.loop(hours=6)
    async def auto_fill_loop(self):
        try:
            for guild in self.bot.guilds:
                cfg = self.get_guild_settings(guild.id)
                if not cfg.get("enabled"):
                    continue
                try:
                    results = await self.fetcher.auto_fill(guild.id)
                    if results:
                        total = sum(results.values())
                        log.info(f"Auto-fill for guild {guild.id}: added {total} images — {results}")
                except Exception as e:
                    log.error(f"Auto-fill error for guild {guild.id}: {e}", exc_info=True)
        except Exception as e:
            await self.bot.error_reporter.report("ImageGuesser", f"auto_fill_loop: {e}")

    @auto_fill_loop.before_loop
    async def before_auto_fill_loop(self):
        await self.bot.wait_until_ready()

    # ---------------------------------------------------------------------------------
    # Game loop — checks every minute for post time and reveal times
    # ---------------------------------------------------------------------------------

    @tasks.loop(minutes=1)
    async def game_loop(self):
        try:
            for guild in self.bot.guilds:
                try:
                    await self._tick_guild(guild)
                except Exception as e:
                    log.error(f"Game loop error for guild {guild.id}: {e}", exc_info=True)
        except Exception as e:
            await self.bot.error_reporter.report("ImageGuesser", f"game_loop: {e}")

    @game_loop.before_loop
    async def before_game_loop(self):
        await self.bot.wait_until_ready()

    async def _tick_guild(self, guild: discord.Guild):
        cfg = self.get_guild_settings(guild.id)
        if not cfg.get("enabled") or not cfg.get("channel_id"):
            return

        tz = ZoneInfo(cfg.get("timezone", DEFAULT_TIMEZONE))
        now = datetime.now(tz)

        # --- Delete threads scheduled for cleanup (48h after game end) ---
        pending = cfg.get("pending_thread_cleanup", [])
        if pending:
            remaining = []
            for entry in pending:
                delete_after = datetime.fromisoformat(entry["delete_after"])
                if now >= delete_after:
                    try:
                        thread = guild.get_channel_or_thread(entry["thread_id"])
                        if not thread:
                            thread = await guild.fetch_channel(entry["thread_id"])
                        await thread.delete()
                        log.info(f"Deleted expired game thread {entry['thread_id']} in guild {guild.id}")
                    except discord.NotFound:
                        pass  # Already deleted
                    except Exception as e:
                        log.error(f"Error deleting thread {entry['thread_id']}: {e}")
                        remaining.append(entry)  # Retry next tick
                else:
                    remaining.append(entry)
            if len(remaining) != len(pending):
                async with self.config_lock:
                    cfg["pending_thread_cleanup"] = remaining
                    self.config_is_dirty = True

        active = cfg.get("active_game")

        # --- If game is solved, clear it so a new one can post ---
        if active and active.get("solved"):
            thread_id = active.get("thread_id")
            if thread_id:
                cleanup_list = cfg.setdefault("pending_thread_cleanup", [])
                cleanup_list.append({
                    "thread_id": thread_id,
                    "delete_after": (now + timedelta(hours=48)).isoformat(),
                })
            async with self.config_lock:
                cfg["active_game"] = None
                self.config_is_dirty = True
            active = None

        # --- Check if we should post a new game ---
        if not active:
            post_time_str = cfg.get("post_time", DEFAULT_POST_TIME)
            try:
                h, m = map(int, post_time_str.split(":"))
            except (ValueError, AttributeError):
                return
            # Post if past the target time and haven't posted today
            last_date = cfg.get("last_posted_date")
            today_str = now.strftime("%Y-%m-%d")
            if last_date != today_str and (now.hour > h or (now.hour == h and now.minute >= m)):
                await self._start_daily_game(guild)
            return

        stages = cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES)
        interval_mins = cfg.get("reveal_interval_mins", DEFAULT_REVEAL_INTERVAL_MINS)
        current_stage = active.get("current_stage", 1)
        last_reveal_at = datetime.fromisoformat(active.get("last_reveal_at", active["started_at"]))

        # Check if it's time for the next reveal (interval since the LAST stage, not from start)
        if current_stage < stages:
            next_reveal_at = last_reveal_at + timedelta(minutes=interval_mins)
            if now >= next_reveal_at:
                await self._advance_reveal(guild)
        else:
            # All stages revealed — check if game should end (1 interval after last stage)
            end_at = last_reveal_at + timedelta(minutes=interval_mins)
            if now >= end_at:
                await self._end_game(guild, reason="time_up")

    # ---------------------------------------------------------------------------------
    # Game actions
    # ---------------------------------------------------------------------------------

    async def _start_daily_game(self, guild: discord.Guild) -> bool:
        """Start a new daily image game for a guild."""
        cfg = self.get_guild_settings(guild.id)
        channel = guild.get_channel(cfg["channel_id"])
        if not channel:
            log.warning(f"Channel {cfg['channel_id']} not found in guild {guild.id}")
            return False

        # Pick a random unused image, preferring today's scheduled categories
        tz = ZoneInfo(cfg.get("timezone", DEFAULT_TIMEZONE))
        today_weekday = datetime.now(tz).weekday()  # 0 = Monday
        today_categories = DAILY_CATEGORIES.get(today_weekday, [])

        image_data = None
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                # Try today's scheduled categories first
                if today_categories:
                    placeholders = ",".join("?" for _ in today_categories)
                    async with db.execute(
                        f"SELECT * FROM images WHERE used = 0 AND guild_id = ? AND category IN ({placeholders}) ORDER BY RANDOM() LIMIT 1",
                        (guild.id, *today_categories)
                    ) as c:
                        row = await c.fetchone()
                    if row:
                        image_data = dict(row)
                # Fallback to any unused image if none match today's rotation
                if not image_data:
                    if today_categories:
                        log.info(f"No images for today's categories {today_categories} in guild {guild.id}, falling back to any.")
                    async with db.execute("SELECT * FROM images WHERE used = 0 AND guild_id = ? ORDER BY RANDOM() LIMIT 1", (guild.id,)) as c:
                        row = await c.fetchone()
                    if row:
                        image_data = dict(row)
        except Exception as e:
            log.error(f"Error fetching image from DB: {e}", exc_info=True)
            return False

        if not image_data:
            log.warning(f"No unused images in queue for guild {guild.id}")
            return False

        if not os.path.exists(image_data["file_path"]):
            log.error(f"Image file missing: {image_data['file_path']}")
            return False

        # Generate the first (most pixelated + zoomed) stage
        stages = cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES)
        try:
            original = Image.open(image_data["file_path"]).convert("RGB")
            processed = process_stage(original, stage=1, total_stages=stages, file_path=image_data["file_path"])
            image_bytes = image_to_bytes(processed)
            original.close()
        except Exception as e:
            log.error(f"Error processing image: {e}", exc_info=True)
            return False

        # Build embed
        now = datetime.now(tz)

        category = image_data.get("category") or "Guess the Image"
        embed = discord.Embed(
            title=category,
            description=f"The image will get clearer over time ({stages} stages, every {cfg.get('reveal_interval_mins', DEFAULT_REVEAL_INTERVAL_MINS)}m).",
            color=EMBED_COLOR,
        )
        embed.set_footer(text=f"Stage 1/{stages}")
        embed.set_image(url="attachment://mystery.png")

        file = discord.File(image_bytes, filename="mystery.png")
        try:
            # Unpin the previous game's message if it exists
            try:
                pins = await channel.pins()
                for pin in pins:
                    if pin.author == self.bot.user:
                        await pin.unpin()
            except Exception:
                pass
            msg = await channel.send(embed=embed, file=file)
            try:
                await msg.pin()
            except Exception:
                pass
            # Delete the "pinned a message" system message
            try:
                async for sys_msg in channel.history(limit=5):
                    if sys_msg.type == discord.MessageType.pins_add and sys_msg.author == self.bot.user:
                        await sys_msg.delete()
                        break
            except Exception:
                pass
        except Exception as e:
            log.error(f"Error posting image: {e}", exc_info=True)
            return False

        # Create thread
        guesses_per_stage = cfg.get("guesses_per_stage", DEFAULT_GUESSES_PER_STAGE)
        short_cat = category.replace("Name this ", "").lower().replace(" ", "-")
        thread_name = f"img-guesser-{short_cat}"
        try:
            thread = await msg.create_thread(name=thread_name)
            hint_text = f"\n**Hint:** {image_data['hint']}" if image_data.get("hint") else ""
            limit_text = f"\nYou get **{guesses_per_stage}** guess(es) per stage." if guesses_per_stage > 0 else ""
            starter = f"Start your guess with **?** to submit it (e.g. `?Eiffel Tower`). Chat normally without the prefix.{limit_text}{hint_text}"
            await thread.send(starter.strip())
        except Exception as e:
            log.error(f"Error creating thread: {e}", exc_info=True)
            return False

        # Save active game state — parse comma-separated answers into a list
        answers = [a.strip() for a in image_data["answer"].split(",") if a.strip()]
        async with self.config_lock:
            cfg["active_game"] = {
                "image_id": image_data["id"],
                "thread_id": thread.id,
                "message_id": msg.id,
                "current_stage": 1,
                "solved": False,
                "solved_by": None,
                "started_at": now.isoformat(),
                "last_reveal_at": now.isoformat(),
                "answers": answers,
                "display_answer": answers[0],
                "file_path": image_data["file_path"],
                "guess_counts": {},
            }
            cfg["last_posted_date"] = now.strftime("%Y-%m-%d")
            self.config_is_dirty = True

        log.info(f"Started image guesser game in guild {guild.id}, image #{image_data['id']}")
        return True

    async def _advance_reveal(self, guild: discord.Guild) -> bool:
        """Advance to the next reveal stage."""
        cfg = self.get_guild_settings(guild.id)
        active = cfg.get("active_game")
        if not active or active.get("solved"):
            return False

        stages = cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES)
        current_stage = active.get("current_stage", 1)
        if current_stage >= stages:
            # All stages already revealed — end the game instead
            await self._end_game(guild, reason="time_up")
            return True

        new_stage = current_stage + 1

        # If advancing to the final stage, end the game and reveal the answer
        if new_stage >= stages:
            await self._end_game(guild, reason="time_up")
            return True

        file_path = active.get("file_path")
        if not file_path or not os.path.exists(file_path):
            log.error(f"Image file missing for reveal: {file_path}")
            return False

        try:
            original = Image.open(file_path).convert("RGB")
            processed = process_stage(original, stage=new_stage, total_stages=stages, file_path=file_path)
            image_bytes = image_to_bytes(processed)
            original.close()
        except Exception as e:
            log.error(f"Error processing reveal image: {e}", exc_info=True)
            return False

        # Post in the thread
        thread = guild.get_channel_or_thread(active["thread_id"])
        if not thread:
            try:
                thread = await guild.fetch_channel(active["thread_id"])
            except Exception:
                log.error(f"Could not find thread {active['thread_id']}")
                return False

        embed = discord.Embed(
            title=f"Getting clearer... (Stage {new_stage}/{stages})",
            color=EMBED_COLOR,
        )
        embed.set_image(url="attachment://mystery.png")
        embed.set_footer(text=f"Stage {new_stage}/{stages}")

        # Post hint at the middle stage if available
        # For 2 stages: hint at stage 2 (only option)
        # For 3 stages: hint at stage 2
        # For 4 stages: hint at stage 2
        # For 5 stages: hint at stage 3
        hint = None
        middle_stage = (stages // 2) + 1
        if new_stage == middle_stage:
            # Get the hint from the image
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    async with db.execute("SELECT hint FROM images WHERE id = ?", (active["image_id"],)) as c:
                        row = await c.fetchone()
                    if row and row[0]:
                        hint = row[0]
            except Exception:
                pass

        file = discord.File(image_bytes, filename="mystery.png")
        try:
            reveal_msg = await thread.send(embed=embed, file=file)
            try:
                await reveal_msg.pin()
                # Delete the "pinned a message" system message
                async for sys_msg in thread.history(limit=5):
                    if sys_msg.type == discord.MessageType.pins_add and sys_msg.author == self.bot.user:
                        await sys_msg.delete()
                        break
            except Exception:
                pass
            if hint:
                await thread.send(f"**Hint:** {hint}")
        except Exception as e:
            log.error(f"Error posting reveal: {e}", exc_info=True)
            return False

        # Update state — reset guess counts for the new stage
        tz = ZoneInfo(cfg.get("timezone", DEFAULT_TIMEZONE))
        async with self.config_lock:
            active["current_stage"] = new_stage
            active["last_reveal_at"] = datetime.now(tz).isoformat()
            active["guess_counts"] = {}
            self.config_is_dirty = True

        log.info(f"Advanced to stage {new_stage}/{stages} in guild {guild.id}")
        return True

    async def _end_game(self, guild: discord.Guild, reason: str = "time_up"):
        """End the current game — reveal the answer."""
        cfg = self.get_guild_settings(guild.id)
        active = cfg.get("active_game")
        if not active:
            return

        thread = guild.get_channel_or_thread(active.get("thread_id"))
        if not thread:
            try:
                thread = await guild.fetch_channel(active["thread_id"])
            except Exception:
                thread = None

        answer = active.get("display_answer") or (active.get("answers", ["Unknown"])[0])
        file_path = active.get("file_path")

        # Unpin the game message from the channel
        msg_id = active.get("message_id")
        if msg_id:
            try:
                channel = guild.get_channel(cfg.get("channel_id"))
                if channel:
                    msg = await channel.fetch_message(msg_id)
                    await msg.unpin()
            except Exception:
                pass

        if thread:
            # Post the clear original image
            if file_path and os.path.exists(file_path):
                try:
                    original = Image.open(file_path).convert("RGB")
                    image_bytes = image_to_bytes(original)
                    original.close()
                    file = discord.File(image_bytes, filename="answer.png")

                    if reason == "time_up":
                        embed = discord.Embed(
                            title="Time's up!",
                            description=f"The answer was **{answer}**!",
                            color=0xE74C3C,
                        )
                    elif reason == "skipped":
                        embed = discord.Embed(
                            title="Game Skipped",
                            description=f"The answer was **{answer}**.",
                            color=0x95A5A6,
                        )
                    else:
                        embed = discord.Embed(
                            title="Game Over",
                            description=f"The answer was **{answer}**.",
                            color=EMBED_COLOR,
                        )
                    embed.set_image(url="attachment://answer.png")
                    answer_msg = await thread.send(embed=embed, file=file)
                    try:
                        await answer_msg.pin()
                        async for sys_msg in thread.history(limit=5):
                            if sys_msg.type == discord.MessageType.pins_add and sys_msg.author == self.bot.user:
                                await sys_msg.delete()
                                break
                    except Exception:
                        pass
                except Exception as e:
                    log.error(f"Error posting answer: {e}", exc_info=True)
                    await thread.send(f"The answer was **{answer}**!")

            # Rename thread to show it's over
            try:
                old_name = thread.name
                if reason == "time_up":
                    await thread.edit(name=f"{old_name} — Unsolved")
                elif reason == "skipped":
                    await thread.edit(name=f"{old_name} — Skipped")
            except Exception:
                pass

        # Mark image as used in DB
        if active.get("image_id"):
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        "UPDATE images SET used = 1, used_date = ? WHERE id = ?",
                        (datetime.now().strftime("%Y-%m-%d"), active["image_id"])
                    )
                    await db.commit()
            except Exception as e:
                log.error(f"Error marking image as used: {e}", exc_info=True)

        # Schedule thread for deletion in 48 hours
        thread_id = active.get("thread_id")
        if thread_id:
            cleanup_list = cfg.setdefault("pending_thread_cleanup", [])
            cleanup_list.append({
                "thread_id": thread_id,
                "delete_after": (datetime.now(tz=ZoneInfo(cfg.get("timezone", DEFAULT_TIMEZONE))) + timedelta(hours=48)).isoformat(),
            })

        # Clear active game
        async with self.config_lock:
            cfg["active_game"] = None
            self.config_is_dirty = True

        log.info(f"Ended game in guild {guild.id} (reason: {reason})")

    # ---------------------------------------------------------------------------------
    # Guess listener
    # ---------------------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if not message.guild:
            return

        # --- Handle pending image uploads ---
        pending = self.pending_uploads.get(message.author.id)
        if pending and message.attachments and pending["channel_id"] == message.channel.id:
            del self.pending_uploads[message.author.id]
            attachment = message.attachments[0]

            ext = os.path.splitext(attachment.filename)[1].lower()
            if ext not in VALID_IMAGE_EXTENSIONS:
                return await message.reply(
                    f"Invalid file type `{ext}`. Supported: {', '.join(VALID_IMAGE_EXTENSIONS)}",
                    delete_after=10,
                )
            if attachment.size > 10 * 1024 * 1024:
                return await message.reply("Image must be under 10MB.", delete_after=10)

            unique_name = f"{uuid.uuid4().hex}{ext}"
            file_path = os.path.join(self.assets_dir, unique_name)
            try:
                await attachment.save(file_path)
            except Exception as e:
                log.error(f"Error saving upload: {e}", exc_info=True)
                return await message.reply(f"Failed to save image: {e}", delete_after=10)

            try:
                with Image.open(file_path) as img:
                    img.verify()
            except Exception:
                os.remove(file_path)
                return await message.reply("That doesn't appear to be a valid image.", delete_after=10)

            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        "INSERT INTO images (guild_id, file_path, answer, category, hint, added_by, added_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (message.guild.id, file_path, pending["answer"], pending["category"],
                         pending["hint"], message.author.id, datetime.now().isoformat())
                    )
                    await db.commit()
                    async with db.execute("SELECT COUNT(*) FROM images WHERE used = 0 AND guild_id = ?", (message.guild.id,)) as c:
                        queue_count = (await c.fetchone())[0]
            except Exception as e:
                log.error(f"Error inserting image: {e}", exc_info=True)
                os.remove(file_path)
                return await message.reply(f"Database error: {e}", delete_after=10)

            parsed_answers = [a.strip() for a in pending["answer"].split(",") if a.strip()]
            answers_display = ", ".join(f"`{a}`" for a in parsed_answers)
            embed = discord.Embed(
                title="Image Uploaded",
                description=f"**Category:** {pending['category']}\n**Accepted answers:** {answers_display}\n**Hint:** {pending['hint'] or 'None'}",
                color=0x2ECC71,
            )
            embed.set_footer(text=f"Queue: {queue_count} image(s) waiting")
            await message.reply(embed=embed)
            try:
                await message.delete()
            except Exception:
                pass
            return

        # Only messages starting with ? are guesses — everything else is chat
        if not message.content.startswith("?"):
            return
        guess = message.content[1:].strip()
        if len(guess) < 2:
            return

        cfg = self.get_guild_settings(message.guild.id)
        active = cfg.get("active_game")
        if not active or active.get("solved"):
            return

        # Check if the message is in the game thread
        if message.channel.id != active.get("thread_id"):
            return

        answers = active.get("answers", [])
        if not answers:
            return

        # --- Guess limit check ---
        guesses_per_stage = cfg.get("guesses_per_stage", DEFAULT_GUESSES_PER_STAGE)
        if guesses_per_stage > 0:
            guess_counts = active.setdefault("guess_counts", {})
            uid = str(message.author.id)
            user_guesses = guess_counts.get(uid, 0)
            if user_guesses >= guesses_per_stage:
                remaining_stages = cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES) - active.get("current_stage", 1)
                if remaining_stages > 0:
                    await message.reply(
                        f"You've used all **{guesses_per_stage}** guesses for this stage. "
                        f"You'll get **{guesses_per_stage}** more when the next reveal drops!",
                        delete_after=8,
                    )
                else:
                    await message.reply(
                        f"You've used all **{guesses_per_stage}** guesses for the final stage.",
                        delete_after=8,
                    )
                return

        display_answer = active.get("display_answer", answers[0])
        threshold = cfg.get("fuzzy_threshold", DEFAULT_FUZZY_THRESHOLD)

        # Check fuzzy match against all accepted answers
        best_score = 0
        for ans in answers:
            g = guess.lower()
            a = ans.lower()
            # Always check full string similarity
            best_score = max(best_score, fuzz.ratio(g, a))
            # Only use partial_ratio for answers longer than 4 chars
            # to avoid false positives on short answers like "It", "Up", "Go"
            if len(a) > 4:
                best_score = max(best_score, fuzz.partial_ratio(g, a))
            # token_sort_ratio handles word reordering ("Tower Eiffel" vs "Eiffel Tower")
            best_score = max(best_score, fuzz.token_sort_ratio(g, a))

        # Increment guess count for this user
        if guesses_per_stage > 0:
            async with self.config_lock:
                guess_counts[uid] = user_guesses + 1
                self.config_is_dirty = True

        if best_score >= threshold:
            # Correct guess!
            async with self.config_lock:
                active["solved"] = True
                active["solved_by"] = message.author.id
                self.config_is_dirty = True

            # Post the clear image
            file_path = active.get("file_path")
            file = None
            if file_path and os.path.exists(file_path):
                try:
                    original = Image.open(file_path).convert("RGB")
                    image_bytes = image_to_bytes(original)
                    original.close()
                    file = discord.File(image_bytes, filename="answer.png")
                except Exception:
                    pass

            stages = cfg.get("reveal_stages", DEFAULT_REVEAL_STAGES)
            stage_solved = active.get("current_stage", 1)

            embed = discord.Embed(
                title="Correct!",
                description=f"**{message.author.display_name}** got it! The answer was **{display_answer}**!",
                color=0x2ECC71,
            )
            embed.set_footer(text=f"Solved at stage {stage_solved}/{stages}")
            if file:
                embed.set_image(url="attachment://answer.png")

            try:
                reply_msg = await message.reply(embed=embed, file=file)
                try:
                    await reply_msg.pin()
                    async for sys_msg in message.channel.history(limit=5):
                        if sys_msg.type == discord.MessageType.pins_add and sys_msg.author == self.bot.user:
                            await sys_msg.delete()
                            break
                except Exception:
                    pass
            except Exception as e:
                log.error(f"Error posting correct guess: {e}", exc_info=True)
                await message.reply(f"Correct! The answer was **{display_answer}**!")

            # Update thread name
            try:
                thread = message.channel
                await thread.edit(name=f"{thread.name} — Solved")
            except Exception:
                pass

            # Mark image as used
            if active.get("image_id"):
                try:
                    async with aiosqlite.connect(self.db_path) as db:
                        await db.execute(
                            "UPDATE images SET used = 1, used_date = ? WHERE id = ?",
                            (datetime.now().strftime("%Y-%m-%d"), active["image_id"])
                        )
                        await db.commit()
                except Exception as e:
                    log.error(f"Error marking image as used: {e}", exc_info=True)

    # ---------------------------------------------------------------------------------
    # Slash commands
    # ---------------------------------------------------------------------------------

    @image_guesser.command(name="panel", description="Open the Image Guesser admin panel.")
    @app_commands.check(is_guesser_admin_check)
    async def panel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Image Guesser Admin Panel",
            description="Use the buttons below to manage the Where in the World? game.",
            color=EMBED_COLOR
        )
        await interaction.response.send_message(embed=embed, view=GuesserAdminPanelView(self), ephemeral=True)



async def setup(bot: commands.Bot):
    await bot.add_cog(ImageGuesser(bot))
