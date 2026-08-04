import discord
from discord.ext import commands
import json
import os
import aiohttp
import urllib.parse
import asyncio
import time
import base64
import re
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from typing import Optional, Dict, List, Tuple
import colorsys
import logging

# Set up logging for this cog (INFO level for production)
logger = logging.getLogger("FM_Cog")
logger.setLevel(logging.INFO)

# --- CONFIGURATION ---
# Add these to your .env file:
#   LASTFM_API_KEY=your_key_here
#   SPOTIFY_CLIENT_ID=your_spotify_client_id
#   SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
# 
# Get Last.fm API key at: https://www.last.fm/api/account/create
# Get Spotify credentials at: https://developer.spotify.com/dashboard


class FM(commands.Cog):
    """Last.fm integration cog for Discord."""

    # Last.fm serves this image hash as its "no artwork" placeholder - a grey
    # star, not real art, so treat it as missing.
    LASTFM_PLACEHOLDER_HASH = "2a96cbd8b46e442fc41c2b86b821562f"

    # Fully-resolved lookups are effectively permanent; ones that fell back to a
    # search page get retried the next day, since the miss is usually the
    # catalogue lagging a new release rather than the track being absent.
    MEDIA_CACHE_TTL_RESOLVED = 30 * 24 * 3600
    MEDIA_CACHE_TTL_PARTIAL = 24 * 3600
    MEDIA_CACHE_MAX_ENTRIES = 5000

    # A search.list call costs 100 of the 10,000 free units. Quota resets at
    # midnight Pacific, so usage days are counted in that timezone rather than
    # the host's, or the daily totals would straddle two quota windows.
    YOUTUBE_SEARCH_COST = 100
    YOUTUBE_DAILY_QUOTA = 10000
    QUOTA_TIMEZONE = "America/Los_Angeles"

    # URL patterns for music link detection
    MUSIC_URL_PATTERNS = [
        # Spotify patterns
        re.compile(r'https?://open\.spotify\.com/(?:intl-[a-z]{2}/)?track/([a-zA-Z0-9]+)'),
        re.compile(r'https?://open\.spotify\.com/(?:intl-[a-z]{2}/)?album/([a-zA-Z0-9]+)'),
        re.compile(r'https?://spotify\.link/([a-zA-Z0-9]+)'),
        # Apple Music patterns
        re.compile(r'https?://music\.apple\.com/[a-z]{2}/album/[^/]+/(\d+)\?i=(\d+)'),
        re.compile(r'https?://music\.apple\.com/[a-z]{2}/album/[^/]+/(\d+)'),
        re.compile(r'https?://music\.apple\.com/[a-z]{2}/song/[^/]+/(\d+)'),
        # YouTube Music patterns
        re.compile(r'https?://music\.youtube\.com/watch\?v=([a-zA-Z0-9_-]+)'),
        re.compile(r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]+)'),
        re.compile(r'https?://youtu\.be/([a-zA-Z0-9_-]+)'),
    ]
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.api_url = "https://ws.audioscrobbler.com/2.0/"
        self.users_file = "fm_users.json"
        self.settings_file = "fm_settings.json"
        self._file_lock = asyncio.Lock()  # In-memory lock for file operations
        self.api_key = os.getenv("LASTFM_API_KEY")
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit_delay = 0.15  # Slightly increased for safety
        self._api_timeout = aiohttp.ClientTimeout(total=10)
        
        # Link listener cooldown per user (prevent spam)
        self._link_cooldowns: Dict[int, float] = {}
        self._link_cooldown_seconds = 5
        
        # Spotify API credentials
        self.spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.spotify_token: Optional[str] = None
        self.spotify_token_expires: float = 0

        # YouTube Data API. Each search costs 100 of the 10,000 daily quota
        # units, so roughly 100 uncached lookups a day - the media cache below
        # is what keeps that workable.
        self.youtube_api_key = os.getenv("YOUTUBE_API_KEY")
        self._youtube_quota_exhausted_until: float = 0

        # Resolved links and artwork, keyed on artist+track. A track's links
        # never change, so this is a straight saving on every repeat play.
        self.media_cache_file = "fm_media_cache.json"
        self._media_cache: Dict[str, Dict] = {}

        # Per-day API usage, so real consumption can be checked against the
        # quota instead of estimated.
        self.usage_file = "fm_usage.json"
        self._usage: Dict[str, object] = {}
        self._usage_last_save: float = 0
        
        # Image generation settings
        self.font_path = "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"
        self.font_bold_path = "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf"
        self._font_available = self._check_fonts()
        
        # Color scheme - Modern dark theme
        self.colors = {
            "bg_dark": (18, 18, 24),
            "bg_card": (28, 28, 36),
            "bg_lighter": (48, 48, 60),
            "accent": (185, 0, 0),  # Last.fm red
            "accent_glow": (255, 50, 50),
            "text_primary": (255, 255, 255),
            "text_secondary": (200, 200, 210),
            "text_muted": (150, 150, 165),
            "gold": (255, 215, 0),
            "silver": (192, 192, 192),
            "bronze": (205, 127, 50),
            "bar_bg": (55, 55, 70),
            "bar_fill": (185, 0, 0),
        }

    def _check_fonts(self) -> bool:
        """Check if custom fonts are available."""
        try:
            ImageFont.truetype(self.font_path, 12)
            return True
        except OSError:
            logger.warning("Custom fonts not found, using default fonts")
            return False

    async def cog_load(self):
        """Called when the cog is loaded."""
        try:
            self._session = aiohttp.ClientSession(timeout=self._api_timeout)
            logger.info("FM Cog loaded successfully!")

            self._media_cache = await self.load_media_cache()
            logger.info(f"Media cache loaded - {len(self._media_cache)} tracks")

            # Restoring counters keeps a restart from resetting the day's total
            # and understating consumption against the quota.
            self._usage = await self.load_usage()
            await self._roll_usage_day()
            logger.info(f"FM usage today ({self._usage['day']}): {self.usage_summary()}")

            if not self.api_key:
                logger.error("LASTFM_API_KEY not configured!")

            if self.spotify_client_id and self.spotify_client_secret:
                logger.info("Spotify API configured - direct links enabled")
            else:
                logger.warning("Spotify API not configured - using search URLs")

            if self.youtube_api_key:
                logger.info("YouTube API configured - direct links enabled")
            else:
                logger.warning("YouTube API not configured - using search URLs")


        except Exception as e:
            logger.error(f"FM Cog failed to initialize: {e}")

    async def cog_unload(self):
        """Called when the cog is unloaded."""
        # Counters are only flushed once a minute, so persist whatever the
        # last write missed before going away.
        if self._usage:
            await self.save_usage()
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("FM Cog unloaded.")

    @property
    def session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._api_timeout)
        return self._session

    # =========================================================================
    # IMAGE UTILITIES
    # =========================================================================
    
    def get_font(self, size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
        """Load a font with fallback."""
        if not self._font_available:
            return ImageFont.load_default()
        try:
            path = self.font_bold_path if bold else self.font_path
            return ImageFont.truetype(path, size)
        except OSError:
            return ImageFont.load_default()

    def round_corners(self, img: Image.Image, radius: int) -> Image.Image:
        """Apply rounded corners to an image."""
        if img.mode != "RGBA":
            img = img.convert("RGBA")
        
        mask = Image.new("L", img.size, 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle([(0, 0), img.size], radius=radius, fill=255)
        
        result = img.copy()
        result.putalpha(mask)
        return result

    def draw_rounded_rect(self, draw: ImageDraw.Draw, coords: tuple, radius: int, fill: tuple):
        """Draw a rounded rectangle."""
        draw.rounded_rectangle(coords, radius=radius, fill=fill)

    def truncate_text(self, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> str:
        """Truncate text to fit within max_width."""
        if not text:
            return ""
        try:
            if font.getlength(text) <= max_width:
                return text
            
            while font.getlength(text + "...") > max_width and len(text) > 0:
                text = text[:-1]
            return text + "..." if text else "..."
        except Exception:
            # Fallback for default font
            return text[:30] + "..." if len(text) > 30 else text

    async def fetch_image(self, url: str) -> Optional[Image.Image]:
        """Fetch an image from URL with timeout and error handling."""
        if not url or not url.strip():
            return None
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    if "image" not in content_type.lower() and not url.endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                        logger.debug(f"Non-image content type: {content_type}")
                        return None
                    data = await resp.read()
                    if len(data) > 10 * 1024 * 1024:  # 10MB limit
                        logger.warning(f"Image too large: {len(data)} bytes")
                        return None
                    return Image.open(BytesIO(data)).convert("RGBA")
        except asyncio.TimeoutError:
            logger.warning(f"Album art fetch timed out: {url}")
        except Exception as e:
            logger.warning(f"Album art fetch failed for {url}: {e}")
        return None

    def get_dominant_color(self, img: Image.Image) -> tuple:
        """Extract dominant color from image for accent."""
        try:
            small = img.resize((50, 50))
            pixels = list(small.getdata())
            
            # Filter out very dark and very light pixels
            filtered = [p for p in pixels if len(p) >= 3 and 30 < sum(p[:3])/3 < 225]
            if not filtered:
                return self.colors["accent"]
            
            # Get average color
            r = sum(p[0] for p in filtered) // len(filtered)
            g = sum(p[1] for p in filtered) // len(filtered)
            b = sum(p[2] for p in filtered) // len(filtered)
            
            # Boost saturation for more vibrant accent
            h, l, s = colorsys.rgb_to_hls(r/255, g/255, b/255)
            s = min(1.0, s * 1.5)
            l = max(0.4, min(0.6, l))
            r, g, b = colorsys.hls_to_rgb(h, l, s)
            
            return (int(r * 255), int(g * 255), int(b * 255))
        except Exception as e:
            logger.debug(f"Failed to get dominant color: {e}")
            return self.colors["accent"]

    def draw_play_glyph(self, draw: ImageDraw.Draw, x: int, y: int, size: int, fill: tuple):
        """Draw a play triangle with (x, y) as its top-left bound.

        Noto Sans carries no U+25B6, so typing the character renders a tofu
        box. Drawing the shape also keeps this working under the default-font
        fallback, where no symbol coverage is guaranteed at all.
        """
        draw.polygon(
            [(x, y), (x, y + size), (x + size * 0.87, y + size / 2)],
            fill=fill
        )

    def draw_note_glyph(self, draw: ImageDraw.Draw, x: int, y: int, size: int, fill: tuple):
        """Draw an eighth note with (x, y) as its top-left bound.

        Stands in for U+266A on the missing-artwork placeholder, which Noto
        Sans likewise cannot render.
        """
        head_w = size * 0.46
        head_h = size * 0.34
        stem_w = max(2, size * 0.07)

        head_y = y + size - head_h
        draw.ellipse((x, head_y, x + head_w, head_y + head_h), fill=fill)

        # Stem rises from the right edge of the head.
        stem_x = x + head_w - stem_w
        draw.rectangle((stem_x, y, stem_x + stem_w, head_y + head_h / 2), fill=fill)

        # Flag, angled off the top of the stem.
        draw.polygon(
            [
                (stem_x + stem_w, y),
                (stem_x + stem_w + size * 0.30, y + size * 0.19),
                (stem_x + stem_w + size * 0.25, y + size * 0.42),
                (stem_x + stem_w, y + size * 0.23),
            ],
            fill=fill
        )

    def draw_medal(self, draw: ImageDraw.Draw, x: int, y: int, rank: int, size: int = 24):
        """Draw a medal circle for top 3 ranks or rank number for others."""
        if rank == 1:
            color = self.colors["gold"]
            text = "1"
        elif rank == 2:
            color = self.colors["silver"]
            text = "2"
        elif rank == 3:
            color = self.colors["bronze"]
            text = "3"
        else:
            # For ranks 4+, just draw the number
            font = self.get_font(14, bold=True)
            draw.text((x, y + 2), f"#{rank}", font=font, fill=self.colors["text_muted"])
            return
        
        # Draw medal circle for top 3
        circle_x = x + size // 2
        circle_y = y + size // 2
        radius = size // 2
        
        # Draw outer circle
        draw.ellipse(
            [circle_x - radius, circle_y - radius, circle_x + radius, circle_y + radius],
            fill=color
        )
        
        # Draw inner darker circle for depth
        inner_radius = radius - 3
        darker_color = tuple(max(0, c - 40) for c in color)
        draw.ellipse(
            [circle_x - inner_radius, circle_y - inner_radius, 
             circle_x + inner_radius, circle_y + inner_radius],
            fill=darker_color
        )
        
        # Draw rank number in center
        font = self.get_font(14, bold=True)
        try:
            text_bbox = font.getbbox(text)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]
        except AttributeError:
            # Fallback for older PIL versions
            text_width, text_height = 10, 14
        
        text_x = circle_x - text_width // 2
        text_y = circle_y - text_height // 2 - 2
        
        draw.text((text_x, text_y), text, font=font, fill=(255, 255, 255))

    # =========================================================================
    # NOW PLAYING IMAGE (Unified for !fm and link shares)
    # =========================================================================
    
    async def create_now_playing_image(
        self,
        track_name: str,
        artist: str,
        album: str,
        album_art_url: str,
        display_name: str,
        is_now_playing: bool = True,
        playcount: Optional[str] = None  # None = don't show plays section
    ) -> BytesIO:
        """Create a modern now playing card.
        
        Args:
            track_name: The track title
            artist: The artist name
            album: The album name
            album_art_url: URL to album artwork
            display_name: Discord display name to show (max 20 chars)
            is_now_playing: True for "NOW PLAYING", False for "LAST PLAYED"
            playcount: If provided, shows play count. If None, hides plays section.
        """
        
        # Canvas dimensions - taller for more impact
        width, height = 1000, 480
        
        # Create base image
        img = Image.new("RGBA", (width, height), self.colors["bg_dark"])
        draw = ImageDraw.Draw(img)
        
        # Fetch album art
        album_art = await self.fetch_image(album_art_url)
        accent_color = self.colors["accent"]
        
        if album_art:
            # Get accent from album art
            accent_color = self.get_dominant_color(album_art)
            
            # Create blurred background from album art
            bg_art = album_art.resize((width, height))
            bg_art = bg_art.filter(ImageFilter.GaussianBlur(radius=30))
            
            # Darken the blurred background
            dark_overlay = Image.new("RGBA", (width, height), (0, 0, 0, 180))
            img.paste(bg_art, (0, 0))
            img = Image.alpha_composite(img, dark_overlay)
            draw = ImageDraw.Draw(img)
        
        # Draw main card background
        card_margin = 16
        self.draw_rounded_rect(
            draw,
            (card_margin, card_margin, width - card_margin, height - card_margin),
            radius=24,
            fill=(*self.colors["bg_card"], 240)
        )
        
        # Album art section (left side) - LARGE
        art_size = 390
        art_x, art_y = 32, 45
        
        if album_art:
            album_art = album_art.resize((art_size, art_size), Image.Resampling.LANCZOS)
            album_art = self.round_corners(album_art, 20)
            img.paste(album_art, (art_x, art_y), album_art)
        else:
            # Placeholder
            self.draw_rounded_rect(
                draw,
                (art_x, art_y, art_x + art_size, art_y + art_size),
                radius=20,
                fill=self.colors["bg_lighter"]
            )
            # Music note icon placeholder, centred in the empty art square
            note_size = 150
            note_w = note_size * 0.76  # head width plus the flag's reach
            self.draw_note_glyph(
                draw,
                art_x + (art_size - note_w) / 2,
                art_y + (art_size - note_size) / 2,
                note_size,
                self.colors["text_muted"]
            )
        
        # Accent line on left of album art
        draw.rectangle((art_x - 6, art_y, art_x - 2, art_y + art_size), fill=accent_color)
        
        # Text section (right side)
        text_x = art_x + art_size + 36
        text_y = 55
        max_text_width = width - text_x - 40
        
        # Status badge
        status_text = "NOW PLAYING" if is_now_playing else "LAST PLAYED"
        status_font = self.get_font(18, bold=True)
        badge_width = int(status_font.getlength(status_text)) + 30
        badge_color = accent_color if is_now_playing else self.colors["bg_lighter"]
        
        self.draw_rounded_rect(
            draw,
            (text_x, text_y, text_x + badge_width, text_y + 36),
            radius=8,
            fill=badge_color
        )
        draw.text((text_x + 15, text_y + 7), status_text, font=status_font, fill=self.colors["text_primary"])
        
        # Track name - LARGE
        text_y += 58
        track_font = self.get_font(44, bold=True)
        track_display = self.truncate_text(track_name, track_font, max_text_width)
        draw.text((text_x, text_y), track_display, font=track_font, fill=self.colors["text_primary"])
        
        # Artist name - White text with shadow
        text_y += 62
        artist_font = self.get_font(34, bold=True)
        artist_display = self.truncate_text(artist, artist_font, max_text_width)
        
        # Draw artist shadow for visibility
        draw.text((text_x + 1, text_y + 1), artist_display, font=artist_font, fill=(0, 0, 0, 140))
        draw.text((text_x, text_y), artist_display, font=artist_font, fill=self.colors["text_primary"])
        
        # Album name
        if album:
            text_y += 52
            album_font = self.get_font(24, bold=True)
            album_display = self.truncate_text(f"on {album}", album_font, max_text_width)
            draw.text((text_x, text_y), album_display, font=album_font, fill=self.colors["text_secondary"])
        
        # Playcount badge at bottom (only if playcount provided)
        if playcount is not None:
            plays_y = 360
            plays_font = self.get_font(20, bold=True)
            plays_text = f"{playcount} plays"

            badge_h = 42
            glyph_size = 15
            pad_x = 17
            gap = 9

            text_w = int(plays_font.getlength(plays_text))
            badge_w = pad_x + glyph_size + gap + text_w + pad_x

            self.draw_rounded_rect(
                draw,
                (text_x, plays_y, text_x + badge_w, plays_y + badge_h),
                radius=10,
                fill=self.colors["bg_lighter"]
            )
            self.draw_play_glyph(
                draw,
                text_x + pad_x,
                plays_y + (badge_h - glyph_size) / 2,
                glyph_size,
                self.colors["text_secondary"]
            )
            draw.text(
                (text_x + pad_x + glyph_size + gap, plays_y + 9),
                plays_text,
                font=plays_font,
                fill=self.colors["text_secondary"]
            )
        
        # Discord username in bottom right - LARGE
        # Truncate display name to 20 characters
        safe_display_name = display_name[:20] if len(display_name) > 20 else display_name
        user_font = self.get_font(36, bold=True)
        username_text = f"@{safe_display_name}"
        username_width = int(user_font.getlength(username_text))
        
        # Draw username with shadow for visibility
        username_x = width - card_margin - username_width - 26
        username_y = height - card_margin - 58
        draw.text((username_x + 2, username_y + 2), username_text, font=user_font, fill=(0, 0, 0, 150))
        draw.text((username_x, username_y), username_text, font=user_font, fill=self.colors["text_primary"])
        
        # Apply final rounded corners to entire image
        img = self.round_corners(img, 24)
        
        # Save to BytesIO
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)
        return buffer

    # =========================================================================
    # WHO KNOWS IMAGE
    # =========================================================================
    
    async def create_whoknows_image(
        self,
        artist_name: str,
        leaderboard: List[Tuple[str, int, str]],  # (display_name, plays, avatar_url)
        artist_image_url: Optional[str] = None
    ) -> BytesIO:
        """Create a modern who knows leaderboard image."""
        
        # Dimensions based on entries
        entry_height = 55
        header_height = 100
        padding = 20
        num_entries = min(len(leaderboard), 10)
        
        width = 600
        height = header_height + (num_entries * entry_height) + padding * 2
        
        # Create base image
        img = Image.new("RGBA", (width, height), self.colors["bg_dark"])
        draw = ImageDraw.Draw(img)
        
        # Draw main card
        self.draw_rounded_rect(
            draw,
            (10, 10, width - 10, height - 10),
            radius=20,
            fill=self.colors["bg_card"]
        )
        
        # Header section
        header_font = self.get_font(13, bold=True)
        title_font = self.get_font(24, bold=True)
        
        # "WHO KNOWS" label
        draw.text((30, 25), "WHO KNOWS", font=header_font, fill=self.colors["accent"])
        
        # Artist name
        artist_display = self.truncate_text(artist_name, title_font, width - 80)
        draw.text((30, 48), artist_display, font=title_font, fill=self.colors["text_primary"])
        
        # Accent line under header
        draw.rectangle((30, 90, width - 30, 92), fill=self.colors["bg_lighter"])
        
        # Get max plays for bar scaling
        max_plays = leaderboard[0][1] if leaderboard else 1
        
        # Draw entries
        y_offset = header_height + 10
        
        for i, (name, plays, avatar_url) in enumerate(leaderboard[:10]):
            entry_y = y_offset + (i * entry_height)
            
            # Draw medal or rank number
            rank_x = 30
            self.draw_medal(draw, rank_x, entry_y + 14, i + 1, size=24)
            
            # Username
            name_x = 80
            name_font = self.get_font(16, bold=True)
            name_display = self.truncate_text(name, name_font, 200)
            name_color = self.colors["text_primary"]
            draw.text((name_x, entry_y + 8), name_display, font=name_font, fill=name_color)
            
            # Play count
            plays_font = self.get_font(12, bold=True)
            plays_text = f"{plays:,} plays"
            draw.text((name_x, entry_y + 30), plays_text, font=plays_font, fill=self.colors["text_secondary"])
            
            # Progress bar
            bar_x = 320
            bar_width = 220
            bar_height = 12
            bar_y = entry_y + 20
            
            # Background bar
            self.draw_rounded_rect(
                draw,
                (bar_x, bar_y, bar_x + bar_width, bar_y + bar_height),
                radius=6,
                fill=self.colors["bar_bg"]
            )
            
            # Fill bar
            fill_width = int((plays / max_plays) * bar_width) if max_plays > 0 else 0
            if fill_width > 0:
                # Color based on rank
                if i == 0:
                    bar_color = self.colors["gold"]
                else:
                    bar_color = self.colors["accent"]
                
                self.draw_rounded_rect(
                    draw,
                    (bar_x, bar_y, bar_x + max(fill_width, 12), bar_y + bar_height),
                    radius=6,
                    fill=bar_color
                )
            
            # Separator line (except for last entry)
            if i < num_entries - 1:
                sep_y = entry_y + entry_height - 2
                draw.rectangle((30, sep_y, width - 30, sep_y + 1), fill=self.colors["bg_lighter"])
        
        # Footer
        footer_font = self.get_font(11, bold=True)
        footer_text = f"Showing top {num_entries} listeners"
        footer_width = int(footer_font.getlength(footer_text))
        draw.text(
            (width - footer_width - 30, height - 30),
            footer_text,
            font=footer_font,
            fill=self.colors["text_muted"]
        )
        
        # Apply rounded corners
        img = self.round_corners(img, 20)
        
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)
        return buffer

    # =========================================================================
    # DATABASE HELPERS
    # =========================================================================
    
    async def load_users(self) -> Dict[str, str]:
        """Load users from JSON file with locking."""
        async with self._file_lock:
            def _load():
                try:
                    if not os.path.exists(self.users_file):
                        with open(self.users_file, "w", encoding="utf-8") as f:
                            json.dump({}, f)
                        return {}
                    with open(self.users_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            logger.error("Invalid users file format, resetting")
                            return {}
                        return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    return {}
                except Exception as e:
                    logger.error(f"Failed to load users: {e}")
                    return {}
            return await asyncio.to_thread(_load)

    async def save_users(self, users: Dict[str, str]) -> bool:
        """Save users to JSON file with locking. Returns success status."""
        async with self._file_lock:
            def _save():
                try:
                    # Write to temp file first, then rename (atomic operation)
                    temp_file = self.users_file + ".tmp"
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(users, f, indent=4)
                    os.replace(temp_file, self.users_file)
                    return True
                except Exception as e:
                    logger.error(f"Failed to save users: {e}")
                    # Clean up temp file if it exists
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception:
                        pass
                    return False
            return await asyncio.to_thread(_save)

    async def load_settings(self) -> Dict:
        """Load settings from JSON file."""
        async with self._file_lock:
            def _load():
                try:
                    if not os.path.exists(self.settings_file):
                        return {}
                    with open(self.settings_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            return {}
                        return data
                except (json.JSONDecodeError, Exception) as e:
                    logger.error(f"Failed to load settings: {e}")
                    return {}
            return await asyncio.to_thread(_load)

    async def save_settings(self, settings: Dict) -> bool:
        """Save settings to JSON file."""
        async with self._file_lock:
            def _save():
                try:
                    temp_file = self.settings_file + ".tmp"
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(settings, f, indent=4)
                    os.replace(temp_file, self.settings_file)
                    return True
                except Exception as e:
                    logger.error(f"Failed to save settings: {e}")
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception:
                        pass
                    return False
            return await asyncio.to_thread(_save)

    async def get_music_channel(self, guild_id: int) -> Optional[int]:
        """Get the configured music channel for a guild."""
        settings = await self.load_settings()
        guild_settings = settings.get(str(guild_id), {})
        return guild_settings.get("music_channel")

    async def set_music_channel(self, guild_id: int, channel_id: Optional[int]) -> bool:
        """Set the music channel for a guild. Pass None to clear."""
        settings = await self.load_settings()
        guild_key = str(guild_id)
        
        if guild_key not in settings:
            settings[guild_key] = {}
        
        if channel_id is None:
            settings[guild_key].pop("music_channel", None)
        else:
            settings[guild_key]["music_channel"] = channel_id
        
        return await self.save_settings(settings)

    async def get_lastfm_username(self, user_id: int) -> Optional[str]:
        """Get Last.fm username for a Discord user."""
        users = await self.load_users()
        return users.get(str(user_id))

    async def api_request(self, params: dict) -> Optional[dict]:
        """Make a request to the Last.fm API with timeout and error handling."""
        try:
            async with self.session.get(
                self.api_url, 
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 403:
                    logger.error("Last.fm API key invalid or rate limited")
                    return None
                if resp.status == 404:
                    logger.debug("Last.fm resource not found")
                    return None
                if resp.status != 200:
                    logger.warning(f"Last.fm API returned {resp.status}")
                    return None
                data = await resp.json()
                # Check for API error response
                if "error" in data:
                    logger.warning(f"Last.fm API error: {data.get('message', 'Unknown error')}")
                    return None
                return data
        except asyncio.TimeoutError:
            logger.warning("Last.fm API request timed out")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Last.fm response: {e}")
            return None

    # =========================================================================
    # STREAMING LINK HELPERS
    # =========================================================================

    @staticmethod
    def clean_query(artist: str, track: str) -> Tuple[str, str]:
        """Strip featured-artist and version suffixes that break catalogue search."""
        clean_track = track.split(" (")[0].split(" feat")[0].split(" ft.")[0].strip()
        clean_artist = artist.split(" feat")[0].split(" ft.")[0].split(",")[0].strip()
        return clean_artist, clean_track

    @staticmethod
    def _norm(text: str) -> str:
        """Reduce a title to comparable form: lowercase, alphanumerics only."""
        return re.sub(r"[^a-z0-9]", "", (text or "").lower())

    def _titles_match(self, want: str, got: str) -> bool:
        """Check two track titles refer to the same song.

        Containment allows for suffixes the catalogue adds ("- Remastered",
        "(Deluxe)"), but only past a length floor - without it short titles
        collide freely, e.g. "Go" would match "Gone".
        """
        w, g = self._norm(want), self._norm(got)
        if not w or not g:
            return False
        if w == g:
            return True
        shorter, longer = (w, g) if len(w) <= len(g) else (g, w)
        return len(shorter) >= 6 and shorter in longer

    def _video_title_contains(self, track: str, title: str) -> bool:
        """Check a YouTube video title contains the track as a whole phrase.

        Video titles wrap the track in artist names and "(Official Video)", so
        the plain title comparison can't be used - but matching on a bare
        substring would let "Go" hit "Gone", hence the word boundaries.
        """
        needle = track.lower().strip()
        if not needle:
            return False
        # Lookarounds rather than \b, which misbehaves when the track name
        # starts or ends with punctuation (e.g. "F*CK YOURSELF").
        if re.search(rf"(?<!\w){re.escape(needle)}(?!\w)", title.lower()):
            return True
        # Punctuation and spacing differ between scrobbles and uploads, so fall
        # back to the normalized comparison for titles long enough to be safe.
        return self._titles_match(track, title)

    def _artists_match(self, want: str, got: str) -> bool:
        """Check two artist names refer to the same act, allowing collaborations."""
        w, g = self._norm(want), self._norm(got)
        if not w or not g:
            return False
        return w == g or w in g or g in w

    async def load_media_cache(self) -> Dict[str, Dict]:
        """Load the resolved-media cache from disk."""
        async with self._file_lock:
            def _load():
                try:
                    if not os.path.exists(self.media_cache_file):
                        return {}
                    with open(self.media_cache_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return data if isinstance(data, dict) else {}
                except Exception as e:
                    logger.error(f"Failed to load media cache: {e}")
                    return {}
            return await asyncio.to_thread(_load)

    async def save_media_cache(self) -> bool:
        """Write the resolved-media cache to disk atomically."""
        async with self._file_lock:
            snapshot = dict(self._media_cache)

            def _save():
                temp_file = self.media_cache_file + ".tmp"
                try:
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(snapshot, f)
                    os.replace(temp_file, self.media_cache_file)
                    return True
                except Exception as e:
                    logger.error(f"Failed to save media cache: {e}")
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception:
                        pass
                    return False
            return await asyncio.to_thread(_save)

    async def load_usage(self) -> Dict[str, object]:
        """Load today's API usage counters from disk."""
        async with self._file_lock:
            def _load():
                try:
                    if not os.path.exists(self.usage_file):
                        return {}
                    with open(self.usage_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return data if isinstance(data, dict) else {}
                except Exception as e:
                    logger.error(f"Failed to load usage counters: {e}")
                    return {}
            return await asyncio.to_thread(_load)

    async def save_usage(self) -> bool:
        """Write usage counters to disk atomically."""
        async with self._file_lock:
            snapshot = dict(self._usage)

            def _save():
                temp_file = self.usage_file + ".tmp"
                try:
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(snapshot, f, indent=2)
                    os.replace(temp_file, self.usage_file)
                    return True
                except Exception as e:
                    logger.error(f"Failed to save usage counters: {e}")
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception:
                        pass
                    return False
            return await asyncio.to_thread(_save)

    def _quota_day(self) -> str:
        """Today's date in the timezone YouTube resets quota on."""
        return datetime.now(ZoneInfo(self.QUOTA_TIMEZONE)).strftime("%Y-%m-%d")

    @staticmethod
    def _blank_usage(day: str) -> Dict[str, object]:
        return {
            "day": day,
            "youtube_units": 0,
            "youtube_searches": 0,
            "spotify_searches": 0,
            "itunes_searches": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }

    def usage_summary(self, usage: Optional[Dict[str, object]] = None) -> str:
        """One-line description of a day's API consumption."""
        u = usage if usage is not None else self._usage
        looks = int(u.get("cache_hits", 0)) + int(u.get("cache_misses", 0))
        rate = f"{int(u.get('cache_hits', 0)) / looks:.0%}" if looks else "n/a"
        units = int(u.get("youtube_units", 0))
        return (
            f"youtube {units}/{self.YOUTUBE_DAILY_QUOTA} units "
            f"({u.get('youtube_searches', 0)} searches) | "
            f"spotify {u.get('spotify_searches', 0)} | "
            f"itunes {u.get('itunes_searches', 0)} | "
            f"cache {u.get('cache_hits', 0)}/{looks} hits ({rate})"
        )

    async def _roll_usage_day(self):
        """Log and reset counters when the quota day rolls over."""
        day = self._quota_day()
        if self._usage.get("day") == day:
            return

        if self._usage.get("day"):
            logger.info(f"FM usage for {self._usage['day']}: {self.usage_summary()}")

        self._usage = self._blank_usage(day)
        self._usage_last_save = 0

    async def record_usage(self, field: str, amount: int = 1):
        """Increment a usage counter, rolling the day over first if needed."""
        await self._roll_usage_day()
        self._usage[field] = int(self._usage.get(field, 0)) + amount

        if field == "youtube_searches":
            self._usage["youtube_units"] = (
                int(self._usage.get("youtube_units", 0)) + self.YOUTUBE_SEARCH_COST * amount
            )

        # Counters change on nearly every command; writing at most once a minute
        # keeps that off the SD card without risking much on an unclean restart.
        if time.time() - self._usage_last_save > 60:
            self._usage_last_save = time.time()
            await self.save_usage()

    def _cache_key(self, artist: str, track: str) -> str:
        return f"{self._norm(artist)}|{self._norm(track)}"

    def _cache_get(self, artist: str, track: str) -> Optional[Dict[str, str]]:
        """Return a cached lookup, or None if absent or stale."""
        entry = self._media_cache.get(self._cache_key(artist, track))
        if not entry:
            return None

        ttl = self.MEDIA_CACHE_TTL_RESOLVED if entry.get("resolved") else self.MEDIA_CACHE_TTL_PARTIAL
        if time.time() - entry.get("ts", 0) > ttl:
            return None

        return {k: v for k, v in entry.items() if k not in ("ts", "resolved")}

    def _cache_put(self, artist: str, track: str, media: Dict[str, str], resolved: bool):
        """Store a lookup, evicting the oldest entries once the cap is hit."""
        if len(self._media_cache) >= self.MEDIA_CACHE_MAX_ENTRIES:
            for key in sorted(
                self._media_cache,
                key=lambda k: self._media_cache[k].get("ts", 0)
            )[:len(self._media_cache) - self.MEDIA_CACHE_MAX_ENTRIES + 1]:
                self._media_cache.pop(key, None)

        self._media_cache[self._cache_key(artist, track)] = {
            **media, "ts": time.time(), "resolved": resolved
        }

    async def search_youtube(self, artist: str, track: str) -> Optional[str]:
        """Find a track on YouTube Music via the Data API.

        Returns None rather than a guess when nothing matches confidently, so
        the caller falls back to a search page instead of linking a reaction
        video or an unrelated upload.
        """
        if not self.youtube_api_key:
            return None

        # Quota resets at midnight Pacific. Rather than track that precisely,
        # back off for an hour at a time once exhausted - the calls would only
        # fail anyway, and this keeps the log from filling up.
        if time.time() < self._youtube_quota_exhausted_until:
            return None

        try:
            params = {
                "part": "snippet",
                "type": "video",
                "maxResults": 5,
                "q": f"{artist} {track}",
                "key": self.youtube_api_key
            }

            async with self.session.get(
                "https://www.googleapis.com/youtube/v3/search",
                params=params,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 403:
                    body = await resp.text()
                    if "quotaExceeded" in body or "quota" in body.lower():
                        self._youtube_quota_exhausted_until = time.time() + 3600
                        logger.warning(
                            "YouTube API daily quota exhausted - falling back to "
                            "search links, will retry in an hour"
                        )
                    else:
                        logger.error(f"YouTube API forbidden (403): {body[:200]}")
                    return None
                if resp.status != 200:
                    logger.warning(f"YouTube search returned {resp.status} for {artist} - {track}")
                    return None

                await self.record_usage("youtube_searches")

                data = await resp.json()
                items = data.get("items", [])

                artist_norm = self._norm(artist)
                for item in items:
                    snippet = item.get("snippet", {})
                    title = snippet.get("title", "")
                    channel = snippet.get("channelTitle", "")

                    # YouTube titles read "Artist - Track (Official Video)", so
                    # require the track in the title and the artist somewhere.
                    if not self._video_title_contains(track, title):
                        continue
                    if artist_norm not in self._norm(title) and artist_norm not in self._norm(channel):
                        continue

                    video_id = item.get("id", {}).get("videoId")
                    if video_id:
                        return f"https://music.youtube.com/watch?v={video_id}"

                logger.info(f"YouTube found no confident match for {artist} - {track}")
                return None

        except asyncio.TimeoutError:
            logger.warning(f"YouTube API timeout for {artist} - {track}")
            return None
        except Exception as e:
            logger.warning(f"YouTube search error for {artist} - {track}: {e}")
            return None

    async def resolve_track_media(self, artist: str, track: str) -> Dict[str, str]:
        """Resolve streaming links and album art for a track in one pass.

        Returns spotify/apple/youtube links plus album_art and album_name. Link
        values always point somewhere usable: a direct track URL where one could
        be resolved, otherwise that platform's search page.
        """
        cached = self._cache_get(artist, track)
        if cached:
            await self.record_usage("cache_hits")
            return cached
        await self.record_usage("cache_misses")

        clean_artist, clean_track = self.clean_query(artist, track)

        q_enc = urllib.parse.quote_plus(f"{clean_artist} {clean_track}")
        fallback = {
            "spotify": f"https://open.spotify.com/search/{q_enc}",
            "apple": f"https://music.apple.com/us/search?term={q_enc}",
            "youtube": f"https://music.youtube.com/search?q={q_enc}"
        }

        album_art = ""
        album_name = ""

        # One Spotify search serves both the link and the preferred album art.
        spotify_result = await self._spotify_search(clean_artist, clean_track)
        spotify_url = spotify_result.get("url") if spotify_result else None
        if spotify_result:
            album_art = spotify_result.get("album_art") or ""
            album_name = spotify_result.get("album_name") or ""

        # iTunes supplies the Apple Music link, and album art when Spotify is
        # unavailable - it covers a good share of the smaller artists Last.fm
        # has no artwork for.
        apple_result = await self.search_apple_music(clean_artist, clean_track)
        apple_url = apple_result.get("url") if apple_result else None
        if apple_result:
            album_art = album_art or apple_result.get("album_art") or ""
            album_name = album_name or apple_result.get("album_name") or ""

        # Odesli only cross-matches from a Spotify URL, and no longer returns
        # Apple Music or YouTube at all - take whatever it gives as a bonus.
        youtube_url = None
        if spotify_url:
            odesli_links = await self.get_odesli_links(spotify_url)
            if odesli_links:
                apple_url = apple_url or odesli_links.get("apple")
                youtube_url = odesli_links.get("youtube")

        # Only spend YouTube quota when Odesli has left us without a link.
        if not youtube_url:
            youtube_url = await self.search_youtube(clean_artist, clean_track)

        media = {
            "spotify": spotify_url or fallback["spotify"],
            "apple": apple_url or fallback["apple"],
            "youtube": youtube_url or fallback["youtube"],
            "album_art": album_art,
            "album_name": album_name
        }

        fully_resolved = bool(spotify_url and apple_url and youtube_url and album_art)
        self._cache_put(artist, track, media, fully_resolved)
        await self.save_media_cache()

        return media

    async def get_odesli_links(self, music_url: str) -> Optional[Dict[str, str]]:
        """Get links from other platforms using Odesli/song.link API."""
        try:
            odesli_url = "https://api.song.link/v1-alpha.1/links"
            params = {"url": music_url, "userCountry": "US"}
            
            async with self.session.get(
                odesli_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"Odesli returned {resp.status}")
                    return None
                
                data = await resp.json()
                links = data.get("linksByPlatform", {})
                
                result = {}
                
                if "spotify" in links:
                    result["spotify"] = links["spotify"].get("url")
                
                if "appleMusic" in links:
                    result["apple"] = links["appleMusic"].get("url")
                
                if "youtubeMusic" in links:
                    result["youtube"] = links["youtubeMusic"].get("url")
                elif "youtube" in links:
                    result["youtube"] = links["youtube"].get("url")
                
                return result if result else None
                
        except asyncio.TimeoutError:
            logger.warning("Odesli API timeout")
            return None
        except Exception as e:
            logger.warning(f"Odesli API error: {e}")
            return None

    async def get_odesli_metadata(self, music_url: str) -> Optional[Dict]:
        """Get full metadata from Odesli including track info and album art."""
        try:
            odesli_url = "https://api.song.link/v1-alpha.1/links"
            params = {"url": music_url, "userCountry": "US"}
            
            async with self.session.get(
                odesli_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"Odesli returned {resp.status}")
                    return None
                
                data = await resp.json()
                
                # Get the entity (song) data
                entity_id = data.get("entityUniqueId")
                if not entity_id:
                    return None
                
                entities = data.get("entitiesByUniqueId", {})
                entity = entities.get(entity_id, {})
                
                if not entity:
                    return None
                
                # Extract links
                links = data.get("linksByPlatform", {})
                
                result = {
                    "title": entity.get("title", "Unknown Track"),
                    "artist": entity.get("artistName", "Unknown Artist"),
                    "album": entity.get("albumName", ""),
                    "album_art": entity.get("thumbnailUrl", ""),
                    "links": {
                        "spotify": links.get("spotify", {}).get("url"),
                        "apple": links.get("appleMusic", {}).get("url"),
                        "youtube": links.get("youtubeMusic", {}).get("url") or links.get("youtube", {}).get("url")
                    }
                }
                
                return result
                
        except asyncio.TimeoutError:
            logger.debug("Odesli metadata timeout")
            return None
        except Exception as e:
            logger.debug(f"Odesli metadata error: {e}")
            return None

    async def search_apple_music(self, artist: str, track: str) -> Optional[Dict[str, str]]:
        """Search iTunes for a track, returning its Apple Music URL and album art.

        Both artist and title must match. Matching on artist alone returns a
        different song by the same artist roughly one time in seven, which is
        worse than returning nothing and falling back to a search page.
        """
        try:
            params = {
                "term": f"{artist} {track}",
                "entity": "song",
                "limit": 25,
                "country": "US"
            }

            async with self.session.get(
                "https://itunes.apple.com/search",
                params=params,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"iTunes search returned {resp.status} for {artist} - {track}")
                    return None

                # iTunes serves JSON as text/javascript, which aiohttp refuses
                # to decode unless the content-type check is disabled.
                await self.record_usage("itunes_searches")

                data = await resp.json(content_type=None)
                results = data.get("results", [])

                if not results:
                    logger.info(f"iTunes has no entry for {artist} - {track}")
                    return None

                for result in results:
                    if not self._artists_match(artist, result.get("artistName", "")):
                        continue
                    if not self._titles_match(track, result.get("trackName", "")):
                        continue

                    track_url = result.get("trackViewUrl")
                    if not track_url:
                        continue

                    # artworkUrl100 is a 100px thumbnail; the same path serves
                    # any size, and 600 is enough for the 390px card art.
                    artwork = result.get("artworkUrl100") or ""
                    if artwork:
                        artwork = artwork.replace("100x100bb", "600x600bb")

                    return {
                        "url": track_url,
                        "album_art": artwork,
                        "album_name": result.get("collectionName") or ""
                    }

                logger.info(f"iTunes found no confident match for {artist} - {track}")
                return None

        except asyncio.TimeoutError:
            logger.warning(f"iTunes API timeout for {artist} - {track}")
            return None
        except Exception as e:
            logger.warning(f"iTunes search error for {artist} - {track}: {e}")
            return None

    async def get_lastfm_album_art(self, artist: str, album: str) -> str:
        """Look up album art via album.getinfo, for tracks whose scrobble has none."""
        if not artist or not album or not self.api_key:
            return ""

        data = await self.api_request({
            "method": "album.getinfo",
            "artist": artist,
            "album": album,
            "api_key": self.api_key,
            "format": "json"
        })
        if not data:
            return ""

        images = data.get("album", {}).get("image", [])
        if not images or not isinstance(images, list):
            return ""

        url = images[-1].get("#text", "") if images[-1] else ""
        return url if url and self.LASTFM_PLACEHOLDER_HASH not in url else ""

    async def get_spotify_token(self) -> Optional[str]:
        """Get a Spotify access token using client credentials flow."""
        if not self.spotify_client_id or not self.spotify_client_secret:
            return None
        
        # Check if current token is still valid (with 60 second buffer)
        if self.spotify_token and time.time() < self.spotify_token_expires - 60:
            return self.spotify_token
        
        try:
            # Encode credentials
            credentials = f"{self.spotify_client_id}:{self.spotify_client_secret}"
            encoded = base64.b64encode(credentials.encode()).decode()
            
            headers = {
                "Authorization": f"Basic {encoded}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {"grant_type": "client_credentials"}
            
            async with self.session.post(
                "https://accounts.spotify.com/api/token",
                headers=headers,
                data=data,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Spotify auth failed: {resp.status}")
                    return None
                
                result = await resp.json()
                self.spotify_token = result.get("access_token")
                expires_in = result.get("expires_in", 3600)
                self.spotify_token_expires = time.time() + expires_in
                
                logger.debug("Got new Spotify access token")
                return self.spotify_token
                
        except Exception as e:
            logger.error(f"Spotify auth error: {e}")
            return None

    async def _spotify_search(self, artist: str, track: str) -> Optional[Dict[str, str]]:
        """Search Spotify for a track and return URL and album art."""
        token = await self.get_spotify_token()
        if not token:
            return None
        
        try:
            # Build search query
            query = f"track:{track} artist:{artist}"
            params = {
                "q": query,
                "type": "track",
                "limit": 1,
                "market": "US"
            }
            headers = {"Authorization": f"Bearer {token}"}
            
            async with self.session.get(
                "https://api.spotify.com/v1/search",
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 401:
                    # Token expired, clear it
                    logger.warning("Spotify token rejected (401), clearing for retry")
                    self.spotify_token = None
                    self.spotify_token_expires = 0
                    return None
                if resp.status == 403:
                    # App-level rejection, not a per-track miss. Usually means the
                    # account owning the app has no active Premium subscription,
                    # which disables every endpoint until it is restored.
                    body = (await resp.text())[:200]
                    logger.error(f"Spotify API forbidden (403) - album art degraded: {body}")
                    return None
                if resp.status != 200:
                    logger.warning(f"Spotify search failed: {resp.status}")
                    return None
                
                await self.record_usage("spotify_searches")

                data = await resp.json()
                tracks = data.get("tracks", {}).get("items", [])
                
                if not tracks:
                    # Try a broader search without the track: prefix
                    params["q"] = f"{artist} {track}"
                    async with self.session.get(
                        "https://api.spotify.com/v1/search",
                        params=params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp2:
                        if resp2.status != 200:
                            return None
                        await self.record_usage("spotify_searches")
                        data = await resp2.json()
                        tracks = data.get("tracks", {}).get("items", [])
                        if not tracks:
                            return None
                
                # Extract URL, album art, and album name
                track_data = tracks[0]
                external_urls = track_data.get("external_urls", {})
                spotify_url = external_urls.get("spotify")
                
                # Get largest album art image
                album_art = None
                album_name = None
                album_data = track_data.get("album", {})
                
                images = album_data.get("images", [])
                if images:
                    # Images are sorted by size descending, first is largest
                    album_art = images[0].get("url")
                
                album_name = album_data.get("name")
                
                return {"url": spotify_url, "album_art": album_art, "album_name": album_name}
                
        except Exception as e:
            logger.warning(f"Spotify search error for {artist} - {track}: {e}")
            return None

    # =========================================================================
    # LINK DETECTION AND PARSING
    # =========================================================================

    def extract_music_url(self, content: str) -> Optional[str]:
        """Extract first music URL from message content."""
        for pattern in self.MUSIC_URL_PATTERNS:
            match = pattern.search(content)
            if match:
                return match.group(0)
        return None

    def is_on_cooldown(self, user_id: int) -> bool:
        """Check if a user is on link share cooldown."""
        now = time.time()
        last_use = self._link_cooldowns.get(user_id, 0)
        return now - last_use < self._link_cooldown_seconds

    def set_cooldown(self, user_id: int):
        """Set cooldown for a user."""
        self._link_cooldowns[user_id] = time.time()
        
        # Cleanup old entries periodically (keep dict small)
        if len(self._link_cooldowns) > 1000:
            cutoff = time.time() - self._link_cooldown_seconds * 2
            self._link_cooldowns = {
                uid: ts for uid, ts in self._link_cooldowns.items() 
                if ts > cutoff
            }

    # =========================================================================
    # EVENT LISTENER
    # =========================================================================

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for music links in configured channel/thread."""
        # Ignore bots
        if message.author.bot:
            return
        
        # Ignore DMs
        if not message.guild:
            return
        
        # Check if this channel/thread is the configured music channel
        music_channel_id = await self.get_music_channel(message.guild.id)
        if not music_channel_id:
            return
        
        # Only match the exact configured channel/thread
        if message.channel.id != music_channel_id:
            return
        
        # Extract music URL
        music_url = self.extract_music_url(message.content)
        if not music_url:
            return
        
        # Check cooldown
        if self.is_on_cooldown(message.author.id):
            return  # Silently ignore, don't spam errors
        
        self.set_cooldown(message.author.id)
        
        # Check bot permissions
        perms = message.channel.permissions_for(message.guild.me)
        if not perms.send_messages:
            logger.warning(f"Missing send_messages permission in {message.channel.id}")
            return
        if not perms.attach_files:
            logger.warning(f"Missing attach_files permission in {message.channel.id}")
            return
        
        can_delete = perms.manage_messages
        
        try:
            # Get metadata from Odesli
            metadata = await self.get_odesli_metadata(music_url)
            
            if not metadata:
                # Odesli failed - try to respond gracefully
                logger.debug(f"Odesli lookup failed for {music_url}")
                return
            
            # Get streaming links with fallbacks
            links = metadata.get("links", {})
            query = f"{metadata['artist']} {metadata['title']}"
            q_enc = urllib.parse.quote_plus(query)
            
            spotify_link = links.get("spotify") or f"https://open.spotify.com/search/{q_enc}"
            apple_link = links.get("apple") or f"https://music.apple.com/us/search?term={q_enc}"
            youtube_link = links.get("youtube") or f"https://music.youtube.com/search?q={q_enc}"
            
            # Odesli resolved this from the shared link itself, so its art and
            # album name describe the exact track - trust them over a search.
            album_art_url = metadata.get("album_art") or ""
            album_name = metadata.get("album") or ""

            # Only search the catalogues if Odesli came back without artwork.
            if not album_art_url:
                shared_media = await self.resolve_track_media(
                    metadata["artist"],
                    metadata["title"]
                )
                album_art_url = shared_media.get("album_art") or ""
                album_name = album_name or shared_media.get("album_name") or ""

            # Create the image (no playcount for link shares)
            try:
                np_image = await self.create_now_playing_image(
                    track_name=metadata["title"],
                    artist=metadata["artist"],
                    album=album_name,
                    album_art_url=album_art_url,
                    display_name=message.author.display_name,
                    is_now_playing=True,
                    playcount=None  # Don't show plays for link shares
                )
                
                links_text = (
                    f"-# [Spotify]({spotify_link}) • "
                    f"[Apple Music]({apple_link}) • "
                    f"[YouTube]({youtube_link})"
                )
                
                embed = discord.Embed(description=links_text, color=discord.Color.from_rgb(185, 0, 0))
                await message.channel.send(file=discord.File(np_image, "nowplaying.png"), embed=embed)
                
                # Delete original message after successful send
                if can_delete:
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass  # Message may already be deleted
                        
            except Exception as img_error:
                logger.error(f"Link share image generation failed: {img_error}", exc_info=True)
                # Fallback to simple embed
                embed = discord.Embed(
                    title=metadata["title"],
                    description=f"by **{metadata['artist']}**",
                    color=discord.Color.from_rgb(185, 0, 0)
                )
                if album_art_url:
                    embed.set_thumbnail(url=album_art_url)
                embed.add_field(
                    name="Listen on",
                    value=f"[Spotify]({spotify_link}) • [Apple Music]({apple_link}) • [YouTube]({youtube_link})",
                    inline=False
                )
                embed.set_footer(text=f"Shared by {message.author.display_name}")
                await message.channel.send(embed=embed)
                
                if can_delete:
                    try:
                        await message.delete()
                    except discord.HTTPException:
                        pass
                        
        except Exception as e:
            logger.error(f"Link listener error: {e}", exc_info=True)

    # =========================================================================
    # ERROR HANDLER
    # =========================================================================
    
    async def cog_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Handle errors for all commands in this cog."""
        # Unwrap CommandInvokeError
        if isinstance(error, commands.CommandInvokeError):
            error = error.original
        
        if isinstance(error, commands.MissingRequiredArgument):
            if ctx.command and ctx.command.name == "login":
                await ctx.reply("❌ Usage: `!login <lastfm_username>`", mention_author=False)
            else:
                await ctx.reply(f"❌ Missing argument: `{error.param.name}`", mention_author=False)
        elif isinstance(error, commands.BadArgument):
            await ctx.reply(f"❌ Invalid argument provided.", mention_author=False)
        elif isinstance(error, commands.NoPrivateMessage):
            await ctx.reply("❌ This command can't be used in DMs.", mention_author=False)
        elif isinstance(error, commands.CommandOnCooldown):
            await ctx.reply(f"❌ Slow down! Try again in {error.retry_after:.1f}s.", mention_author=False)
        elif isinstance(error, commands.MissingPermissions):
            await ctx.reply("❌ You don't have permission to use this command.", mention_author=False)
        elif isinstance(error, commands.BotMissingPermissions):
            missing = ", ".join(error.missing_permissions)
            await ctx.reply(f"❌ I'm missing permissions: {missing}", mention_author=False)
        else:
            logger.error(f"Command {ctx.command}: {error}", exc_info=error)
            await ctx.reply("❌ Something went wrong.", mention_author=False)

    # =========================================================================
    # COMMANDS
    # =========================================================================

    @commands.command(name="setmusicchannel", aliases=["smc"])
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def setmusicchannel(self, ctx: commands.Context, *, channel_input: Optional[str] = None):
        """Set or clear the music sharing channel/thread.
        
        Usage: 
            !setmusicchannel #channel - Set a text channel
            !setmusicchannel <thread_id> - Set a specific thread
            !setmusicchannel - Clear (disable link detection)
        
        Requires Manage Server permission.
        """
        # If no input, clear the channel
        if not channel_input:
            success = await self.set_music_channel(ctx.guild.id, None)
            if success:
                await ctx.reply("✅ Music channel cleared. Link detection disabled.", mention_author=False)
            else:
                await ctx.reply("❌ Failed to save settings.", mention_author=False)
            return
        
        channel_input = channel_input.strip()
        target = None
        
        # Extract ID from mention format <#123456789>
        if channel_input.startswith("<#") and channel_input.endswith(">"):
            try:
                channel_id = int(channel_input[2:-1])
                target = ctx.guild.get_channel_or_thread(channel_id)
            except ValueError:
                pass
        # Try as raw ID (could be channel or thread)
        elif channel_input.isdigit():
            target = ctx.guild.get_channel_or_thread(int(channel_input))
        # Try by name (channels only)
        else:
            target = discord.utils.get(ctx.guild.text_channels, name=channel_input)
        
        if not target:
            return await ctx.reply(
                f"❌ Channel/thread not found: `{channel_input}`\n"
                f"Use a #mention, channel ID, or thread ID.",
                mention_author=False
            )
        
        # Validate it's a text channel or thread
        if not isinstance(target, (discord.TextChannel, discord.Thread)):
            return await ctx.reply("❌ Please specify a text channel or thread.", mention_author=False)
        
        # Check bot permissions
        perms = target.permissions_for(ctx.guild.me)
        missing = []
        if not perms.send_messages:
            missing.append("Send Messages")
        if not perms.attach_files:
            missing.append("Attach Files")
        if not perms.manage_messages:
            missing.append("Manage Messages")
        
        if missing:
            await ctx.reply(
                f"⚠️ I'm missing permissions in {target.mention}: {', '.join(missing)}\n"
                f"Link detection may not work properly.",
                mention_author=False
            )
        
        success = await self.set_music_channel(ctx.guild.id, target.id)
        if success:
            target_type = "thread" if isinstance(target, discord.Thread) else "channel"
            await ctx.reply(
                f"✅ Music {target_type} set to {target.mention}\n"
                f"-# Spotify, Apple Music, and YouTube links will be auto-converted.",
                mention_author=False
            )
        else:
            await ctx.reply("❌ Failed to save settings.", mention_author=False)
        
        # Delete command message
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

    @commands.command(name="login", aliases=["fmset", "fmlink"])
    async def login(self, ctx: commands.Context, username: str):
        """Link your Last.fm account.
        
        Usage: !login <lastfm_username>
        """
        # Validate username (Last.fm usernames are 2-15 chars, alphanumeric + hyphens/underscores)
        if len(username) < 2 or len(username) > 15:
            return await ctx.reply("❌ Invalid username (must be 2-15 characters).", mention_author=False)
        
        if not all(c.isalnum() or c in '-_' for c in username):
            return await ctx.reply("❌ Invalid username (letters, numbers, hyphens, underscores only).", mention_author=False)
        
        # Verify the username exists on Last.fm
        if self.api_key:
            params = {
                "method": "user.getinfo",
                "user": username,
                "api_key": self.api_key,
                "format": "json"
            }
            data = await self.api_request(params)
            if not data or "user" not in data:
                return await ctx.reply(f"❌ Last.fm user `{username}` not found.", mention_author=False)
        
        try:
            users = await self.load_users()
            users[str(ctx.author.id)] = username
            success = await self.save_users(users)
            if success:
                await ctx.reply(f"✅ Linked to **{username}**", mention_author=False)
            else:
                await ctx.reply("❌ Failed to save. Please try again.", mention_author=False)
        except Exception as e:
            logger.error(f"Login error: {e}")
            await ctx.reply("❌ Failed to save.", mention_author=False)

    @commands.command(name="logout", aliases=["fmunlink", "fmunset"])
    async def logout(self, ctx: commands.Context):
        """Unlink your Last.fm account."""
        try:
            users = await self.load_users()
            user_id = str(ctx.author.id)
            
            if user_id not in users:
                return await ctx.reply("❌ No account linked.", mention_author=False)
            
            del users[user_id]
            success = await self.save_users(users)
            if success:
                await ctx.reply("✅ Account unlinked.", mention_author=False)
            else:
                await ctx.reply("❌ Failed to unlink. Please try again.", mention_author=False)
        except Exception as e:
            logger.error(f"Logout error: {e}")
            await ctx.reply("❌ Failed to unlink.", mention_author=False)

    @commands.command(name="fmping")
    async def fmping(self, ctx: commands.Context):
        """Test if the FM cog is working."""
        api_status = "✅" if self.api_key else "❌"
        spotify_status = "✅" if (self.spotify_client_id and self.spotify_client_secret) else "❌"
        youtube_status = "✅" if self.youtube_api_key else "❌"

        # Check music channel
        music_channel_id = None
        if ctx.guild:
            music_channel_id = await self.get_music_channel(ctx.guild.id)

        music_channel_status = f"<#{music_channel_id}>" if music_channel_id else "Not set"

        await ctx.reply(
            f"✅ FM cog is loaded!\n"
            f"-# Last.fm API: {api_status} | Spotify API: {spotify_status} | YouTube API: {youtube_status}\n"
            f"-# Music Channel: {music_channel_status}",
            mention_author=False
        )

    @commands.command(name="fmquota", aliases=["fmusage"])
    async def fmquota(self, ctx: commands.Context):
        """Show today's API usage against the YouTube quota."""
        await self._roll_usage_day()

        units = int(self._usage.get("youtube_units", 0))
        pct = units / self.YOUTUBE_DAILY_QUOTA
        looks = int(self._usage.get("cache_hits", 0)) + int(self._usage.get("cache_misses", 0))
        hit_rate = f"{int(self._usage.get('cache_hits', 0)) / looks:.0%}" if looks else "n/a"

        # Twelve blocks reads clearly on mobile without wrapping.
        filled = min(12, int(pct * 12))
        bar = "█" * filled + "░" * (12 - filled)

        remaining = max(0, self.YOUTUBE_DAILY_QUOTA - units)
        colour = discord.Color.green() if pct < 0.6 else (
            discord.Color.orange() if pct < 0.9 else discord.Color.red()
        )

        embed = discord.Embed(
            title="FM API usage",
            description=f"`{bar}` **{units:,}** / {self.YOUTUBE_DAILY_QUOTA:,} units ({pct:.0%})",
            color=colour
        )
        embed.add_field(
            name="YouTube",
            value=f"{self._usage.get('youtube_searches', 0)} searches\n"
                  f"~{remaining // self.YOUTUBE_SEARCH_COST} left today",
            inline=True
        )
        embed.add_field(
            name="Other lookups",
            value=f"Spotify: {self._usage.get('spotify_searches', 0)}\n"
                  f"iTunes: {self._usage.get('itunes_searches', 0)}",
            inline=True
        )
        embed.add_field(
            name="Cache",
            value=f"{hit_rate} hit rate\n{len(self._media_cache):,} tracks stored",
            inline=True
        )
        embed.set_footer(text=f"Quota day {self._usage.get('day', '?')} · resets midnight Pacific")

        await ctx.reply(embed=embed, mention_author=False)

    @commands.command(name="fm", aliases=["np"])
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def fm(self, ctx: commands.Context):
        """Show what you're currently listening to."""
        username = await self.get_lastfm_username(ctx.author.id)
        if not username:
            return await ctx.reply("❌ Use `!login <username>` to link your account first.", mention_author=False)

        if not self.api_key:
            return await ctx.reply("⚠️ Last.fm API key not configured.", mention_author=False)

        async with ctx.typing():
            params = {
                "method": "user.getrecenttracks",
                "user": username,
                "api_key": self.api_key,
                "format": "json",
                "limit": 1
            }

            data = await self.api_request(params)
            if not data:
                return await ctx.reply("❌ Couldn't connect to Last.fm.", mention_author=False)

            tracks = data.get("recenttracks", {}).get("track", [])
            if not tracks:
                return await ctx.reply("❌ No recent tracks found.", mention_author=False)
            
            # Handle case where tracks is a single dict instead of list
            if isinstance(tracks, dict):
                tracks = [tracks]
            
            track = tracks[0]
            artist = track.get("artist", {}).get("#text", "Unknown Artist")
            track_name = track.get("name", "Unknown Track")
            album = track.get("album", {}).get("#text", "")
            
            # Art from the scrobble itself is track-accurate and free, so prefer
            # it over anything a catalogue search guesses at.
            image_url = ""
            images = track.get("image", [])
            if images and isinstance(images, list):
                lastfm_url = images[-1].get("#text", "") if images[-1] else ""
                if lastfm_url and self.LASTFM_PLACEHOLDER_HASH not in lastfm_url:
                    image_url = lastfm_url

            now_playing = "@attr" in track and track.get("@attr", {}).get("nowplaying") == "true"

            # Get playcount for this artist
            playcount = "0"
            artist_params = {
                "method": "artist.getinfo",
                "artist": artist,
                "username": username,
                "api_key": self.api_key,
                "format": "json"
            }
            a_data = await self.api_request(artist_params)
            if a_data and "artist" in a_data:
                try:
                    playcount = a_data["artist"].get("stats", {}).get("userplaycount", "0")
                except (KeyError, TypeError):
                    pass

            # Links and, where the scrobble had no usable art, a cover to fall
            # back on. Spotify and iTunes are both searched here exactly once.
            media = await self.resolve_track_media(artist, track_name)

            if not image_url:
                image_url = media.get("album_art") or ""
            if not album:
                album = media.get("album_name") or ""

            # Last resort: ask Last.fm for the album's own art. Only reachable
            # when the scrobble carried an album name but no image.
            if not image_url and album:
                image_url = await self.get_lastfm_album_art(artist, album)

            if not image_url:
                logger.info(f"No album art found for {artist} - {track_name}")

            # Create the image
            try:
                np_image = await self.create_now_playing_image(
                    track_name=track_name,
                    artist=artist,
                    album=album,
                    album_art_url=image_url,
                    display_name=ctx.author.display_name,
                    is_now_playing=now_playing,
                    playcount=playcount
                )
                
                # Build embed with streaming links
                spotify_link = media.get("spotify", "")
                apple_link = media.get("apple", "")
                youtube_link = media.get("youtube", "")

                links_text = (
                    f"-# [Spotify]({spotify_link}) • "
                    f"[Apple Music]({apple_link}) • "
                    f"[YouTube]({youtube_link})"
                )
                
                embed = discord.Embed(description=links_text, color=discord.Color.from_rgb(185, 0, 0))
                await ctx.send(file=discord.File(np_image, "nowplaying.png"), embed=embed)
                
                # Delete command message
                try:
                    await ctx.message.delete()
                except discord.HTTPException:
                    pass
                
            except Exception as img_error:
                logger.error(f"Image generation failed: {img_error}", exc_info=True)
                # Fallback to embed-only response
                state = "Now Playing" if now_playing else "Last Played"
                
                embed = discord.Embed(color=discord.Color.from_rgb(185, 0, 0))
                embed.set_author(name=f"{ctx.author.display_name} — {state}", icon_url=ctx.author.display_avatar.url)
                embed.add_field(name="Track", value=f"**{track_name}**", inline=True)
                embed.add_field(name="Artist", value=f"**{artist}**", inline=True)
                embed.set_footer(text=f"Plays: {playcount}" + (f" • Album: {album}" if album else ""))
                if image_url:
                    embed.set_thumbnail(url=image_url)
                await ctx.send(embed=embed)
                
                try:
                    await ctx.message.delete()
                except discord.HTTPException:
                    pass

    @commands.command(name="whoknows", aliases=["wk"])
    @commands.guild_only()
    @commands.cooldown(1, 10, commands.BucketType.guild)
    async def whoknows(self, ctx: commands.Context, *, artist_name: Optional[str] = None):
        """Show a leaderboard of who knows an artist the most.
        
        Usage: !whoknows [artist_name]
        If no artist is provided, uses your currently playing artist.
        """
        if not self.api_key:
            return await ctx.reply("⚠️ Last.fm API key not configured.", mention_author=False)
        
        # If no artist provided, get from user's current track
        if not artist_name:
            username = await self.get_lastfm_username(ctx.author.id)
            if not username:
                return await ctx.reply("❌ Link your account or specify an artist: `!wk <artist>`", mention_author=False)
            
            params = {
                "method": "user.getrecenttracks",
                "user": username,
                "api_key": self.api_key,
                "format": "json",
                "limit": 1
            }
            data = await self.api_request(params)
            if not data:
                return await ctx.reply("❌ Couldn't connect to Last.fm.", mention_author=False)
            
            try:
                tracks = data.get("recenttracks", {}).get("track", [])
                if isinstance(tracks, dict):
                    tracks = [tracks]
                if not tracks:
                    return await ctx.reply("❌ No recent tracks to get artist from.", mention_author=False)
                artist_name = tracks[0].get("artist", {}).get("#text")
                if not artist_name:
                    return await ctx.reply("❌ Couldn't get your current artist.", mention_author=False)
            except (KeyError, IndexError, TypeError):
                return await ctx.reply("❌ Couldn't get your current artist.", mention_author=False)

        # Sanitize artist name
        artist_name = artist_name.strip()[:100]  # Limit length
        
        # Get all linked users in this server
        users = await self.load_users()
        
        guild_users: Dict[str, Tuple[str, str]] = {}
        for discord_id, lastfm_user in users.items():
            try:
                member = ctx.guild.get_member(int(discord_id))
                if member:
                    guild_users[discord_id] = (lastfm_user, member.display_name)
            except (ValueError, AttributeError):
                continue
        
        if not guild_users:
            return await ctx.reply("❌ No linked users in this server.", mention_author=False)

        # Fetch play counts for all users
        async with ctx.typing():
            leaderboard: List[Tuple[str, int, str]] = []
            
            async def fetch_user_plays(lastfm_user: str, display_name: str) -> Optional[Tuple[str, int, str]]:
                params = {
                    "method": "artist.getinfo",
                    "artist": artist_name,
                    "username": lastfm_user,
                    "api_key": self.api_key,
                    "format": "json"
                }
                data = await self.api_request(params)
                if data and "artist" in data:
                    try:
                        plays = int(data["artist"].get("stats", {}).get("userplaycount", 0))
                        return (display_name, plays, "")
                    except (KeyError, TypeError, ValueError):
                        pass
                return None

            # Process in batches to avoid rate limiting
            batch_size = 5
            user_list = list(guild_users.values())
            
            for i in range(0, len(user_list), batch_size):
                batch = user_list[i:i + batch_size]
                tasks = [fetch_user_plays(lastfm_user, display_name) for lastfm_user, display_name in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, tuple) and result[1] > 0:
                        leaderboard.append(result)
                
                if i + batch_size < len(user_list):
                    await asyncio.sleep(self._rate_limit_delay)

            leaderboard.sort(key=lambda x: x[1], reverse=True)

        if not leaderboard:
            return await ctx.reply(f"❌ Nobody has listened to **{artist_name}**.", mention_author=False)

        # Try image response, fall back to embed
        try:
            wk_image = await self.create_whoknows_image(
                artist_name=artist_name,
                leaderboard=leaderboard
            )
            await ctx.send(file=discord.File(wk_image, "whoknows.png"))
        except Exception as img_error:
            logger.error(f"Who Knows image generation failed: {img_error}", exc_info=True)
            # Fallback to embed
            desc = ""
            for i, (name, plays, _) in enumerate(leaderboard[:10], 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**{i}.**"
                desc += f"{medal} **{name}** — {plays:,} plays\n"
            
            embed = discord.Embed(
                title=f"Who knows {artist_name}?",
                description=desc,
                color=discord.Color.gold()
            )
            embed.set_footer(text=f"Showing top {min(len(leaderboard), 10)} of {len(leaderboard)} listeners")
            await ctx.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(FM(bot))
