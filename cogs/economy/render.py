"""Builds the data payloads handed to the card templates.

Keeps all presentation logic — number formatting, colour derivation and the
inline activity chart — out of the cog itself.
"""

import html
import logging
from datetime import datetime, timezone

from .leveling import level_progress

logger = logging.getLogger('cogs.economy.render')

DEFAULT_ACCENT = "#5865F2"
VOICE_COLOR = "#4dd4ac"

CHART_WIDTH = 824
CHART_BODY = 152
CHART_LABELS = 20


# --------------------------------------------------------------------------
# Formatting helpers
# --------------------------------------------------------------------------

def commas(n) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def compact(n) -> str:
    """Shorten large numbers so the big tile numerals never wrap."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "0"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 100_000:
        return f"{n / 1000:.0f}K"
    return f"{n:,}"


def esc(text) -> str:
    """Escape for HTML and neutralise braces, which str.format() would eat."""
    return html.escape(str(text)).replace("{", "&#123;").replace("}", "&#125;")


def hours(seconds) -> str:
    h = (seconds or 0) / 3600
    if h >= 1000:
        return f"{h / 1000:.1f}K"
    if h >= 100:
        return f"{h:.0f}"
    return f"{h:.1f}"


def minutes_label(minutes) -> str:
    """Compact duration for the chart's scale hint."""
    minutes = int(minutes or 0)
    if minutes >= 60:
        hrs = minutes / 60
        return f"{hrs:.1f}h".replace(".0h", "h")
    return f"{minutes}m"


def hour_label(hour) -> str:
    if hour is None:
        return "—"
    suffix = "AM" if hour < 12 else "PM"
    display = hour % 12 or 12
    return f"{display}:00 {suffix}"


def shade(hex_color: str, factor: float) -> str:
    """Lighten (factor > 1) or darken (factor < 1) a #rrggbb colour."""
    try:
        raw = hex_color.lstrip("#")
        r, g, b = (int(raw[i:i + 2], 16) for i in (0, 2, 4))
    except (ValueError, IndexError):
        return hex_color
    if factor >= 1:
        r = int(r + (255 - r) * (factor - 1))
        g = int(g + (255 - g) * (factor - 1))
        b = int(b + (255 - b) * (factor - 1))
    else:
        r, g, b = int(r * factor), int(g * factor), int(b * factor)
    return f"#{max(0, min(255, r)):02x}{max(0, min(255, g)):02x}{max(0, min(255, b)):02x}"


def rgba(hex_color: str, alpha: float) -> str:
    try:
        raw = hex_color.lstrip("#")
        r, g, b = (int(raw[i:i + 2], 16) for i in (0, 2, 4))
    except (ValueError, IndexError):
        return f"rgba(88, 101, 242, {alpha})"
    return f"rgba({r}, {g}, {b}, {alpha})"


def accent_for(member) -> str:
    """Use the member's highest coloured role as the card accent."""
    try:
        for role in reversed(member.roles):
            if role.color and role.color.value:
                return f"#{role.color.value:06x}"
    except Exception:
        pass
    return DEFAULT_ACCENT


# --------------------------------------------------------------------------
# Activity chart
# --------------------------------------------------------------------------

def build_chart_svg(series: dict, accent: str) -> str:
    """Message bars plus a voice-minutes line, on independent scales.

    The two series have wildly different magnitudes, so each is normalised to
    its own max — the chart shows shape and rhythm, not absolute comparison.
    """
    messages = series.get("messages") or []
    voice = series.get("voice_minutes") or []
    labels = series.get("labels") or []
    count = len(messages)

    if count == 0:
        return (f'<svg width="{CHART_WIDTH}" height="{CHART_BODY + CHART_LABELS}"></svg>')

    total_h = CHART_BODY + CHART_LABELS
    slot = CHART_WIDTH / count
    bar_w = max(4.0, min(18.0, slot * 0.58))
    msg_max = max(messages) or 1
    voice_max = max(voice) if voice and max(voice) > 0 else 1

    parts = [
        f'<svg width="{CHART_WIDTH}" height="{total_h}" '
        f'viewBox="0 0 {CHART_WIDTH} {total_h}" xmlns="http://www.w3.org/2000/svg">',
        '<defs>',
        f'<linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{shade(accent, 1.28)}"/>'
        f'<stop offset="100%" stop-color="{rgba(accent, 0.35)}"/></linearGradient>',
        f'<linearGradient id="voiceGrad" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{rgba(VOICE_COLOR, 0.30)}"/>'
        f'<stop offset="100%" stop-color="{rgba(VOICE_COLOR, 0.0)}"/></linearGradient>',
        '</defs>',
    ]

    # Horizontal gridlines at 0 / 33 / 66 / 100%.
    for frac in (0.0, 0.33, 0.66, 1.0):
        y = round(CHART_BODY - CHART_BODY * frac, 1)
        opacity = 0.10 if frac == 0.0 else 0.05
        parts.append(
            f'<line x1="0" y1="{y}" x2="{CHART_WIDTH}" y2="{y}" '
            f'stroke="#ffffff" stroke-opacity="{opacity}" stroke-width="1"/>'
        )

    # Message bars.
    for i, value in enumerate(messages):
        height = (value / msg_max) * (CHART_BODY - 8)
        if value > 0:
            height = max(height, 2.5)
        if height <= 0:
            continue
        x = round(i * slot + (slot - bar_w) / 2, 2)
        y = round(CHART_BODY - height, 2)
        radius = round(min(bar_w / 2, 3), 2)
        parts.append(
            f'<rect x="{x}" y="{y}" width="{round(bar_w, 2)}" height="{round(height, 2)}" '
            f'rx="{radius}" fill="url(#barGrad)"/>'
        )

    # Voice line + soft area beneath it.
    if voice and any(voice):
        points = []
        for i, value in enumerate(voice):
            x = round(i * slot + slot / 2, 2)
            y = round(CHART_BODY - (value / voice_max) * (CHART_BODY - 14), 2)
            points.append((x, y))

        line = " ".join(f"{x},{y}" for x, y in points)
        area = (f"{points[0][0]},{CHART_BODY} " + line +
                f" {points[-1][0]},{CHART_BODY}")
        parts.append(f'<polygon points="{area}" fill="url(#voiceGrad)"/>')
        parts.append(
            f'<polyline points="{line}" fill="none" stroke="{VOICE_COLOR}" '
            f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round" '
            f'stroke-opacity="0.9"/>'
        )
        # Dot the final day so "today" reads clearly.
        parts.append(
            f'<circle cx="{points[-1][0]}" cy="{points[-1][1]}" r="3" '
            f'fill="{VOICE_COLOR}"/>'
        )

    # Sparse date labels — first, two interior marks, and last.
    label_y = CHART_BODY + 14
    for idx in sorted({0, count // 3, (2 * count) // 3, count - 1}):
        if idx >= len(labels):
            continue
        try:
            text = datetime.fromisoformat(labels[idx]).strftime("%b %-d")
        except (ValueError, TypeError):
            text = labels[idx]
        x = round(idx * slot + slot / 2, 2)
        anchor = "start" if idx == 0 else ("end" if idx == count - 1 else "middle")
        if idx == 0:
            x = 0
        elif idx == count - 1:
            x = CHART_WIDTH
        parts.append(
            f'<text x="{x}" y="{label_y}" fill="#6f7480" font-size="10.5" '
            f'font-family="NotoSans, sans-serif" font-weight="700" '
            f'letter-spacing="0.4" text-anchor="{anchor}">{esc(text)}</text>'
        )

    parts.append('</svg>')
    return "".join(parts)


# --------------------------------------------------------------------------
# Level card payload
# --------------------------------------------------------------------------

def build_level_card_data(member, user_row, profile: dict, rank: int, total_ranked: int,
                          streak_row, channel_name: str) -> dict:
    """Assemble every placeholder eco_level_card.html expects."""
    xp = user_row["xp"] if user_row else 0
    level, into, needed, percent = level_progress(xp)

    accent = accent_for(member)

    # Prefer tracker's history for lifetime totals; fall back to our own
    # counters when tracking_data.db is unavailable.
    messages = profile.get("messages") or (user_row["messages"] if user_row else 0)
    voice_seconds = profile.get("voice_seconds") or (
        user_row["voice_seconds"] if user_row else 0)

    streak = streak_row["current_streak"] if streak_row else 0
    longest = streak_row["longest_streak"] if streak_row else 0

    series = profile.get("series") or {}
    month_messages = sum(series.get("messages") or [])
    month_voice = sum(series.get("voice_minutes") or [])

    top_emoji = profile.get("top_emoji")
    avg_len = profile.get("avg_length") or 0

    joined = "—"
    if getattr(member, "joined_at", None):
        joined = member.joined_at.strftime("%b %Y")

    rank_display = f"#{rank}" if rank else "Unranked"
    if rank and total_ranked:
        rank_display = f"#{rank} of {total_ranked}"

    return {
        "accent": accent,
        "accent_light": shade(accent, 1.35),
        "accent_glow": rgba(accent, 0.24),
        "accent_faint": rgba(accent, 0.13),
        "accent_border": rgba(accent, 0.32),

        "avatar_url": member.display_avatar.replace(size=256, static_format="png").url,
        "display_name": esc(member.display_name),
        "username": esc(f"@{member.name}"),
        "joined": esc(joined),

        "level": level,
        "next_level": level + 1,
        "total_xp": commas(xp),
        "xp_into": commas(into),
        "xp_needed": commas(needed),
        "xp_percent": percent,
        "ring_deg": round(percent * 3.6, 1),
        "rank_display": esc(rank_display),

        "messages": compact(messages),
        "messages_sub": f"{commas(month_messages)} in 30 days",
        "voice_hours": hours(voice_seconds),
        "voice_sub": f"{commas(round(month_voice / 60))}h in 30 days",
        "days_active": commas(profile.get("days_active") or 0),
        "days_sub": "days with a message",
        "streak": commas(streak),
        "streak_sub": f"best {commas(longest)}",

        "chart_svg": build_chart_svg(series, accent),
        # The two series use independent scales, so each legend entry states
        # its own peak — otherwise a quiet voice month reads as a loud one.
        "chart_msg_peak": commas(max(series.get("messages") or [0])),
        "chart_voice_peak": minutes_label(max(series.get("voice_minutes") or [0])),

        "top_channel": esc(f"#{channel_name}") if channel_name else "—",
        "top_channel_class": "" if channel_name else "muted",
        "peak_hour": esc(hour_label(profile.get("peak_hour"))),
        "peak_hour_class": "" if profile.get("peak_hour") is not None else "muted",
        "peak_day": esc(profile.get("peak_weekday") or "—"),
        "peak_day_class": "" if profile.get("peak_weekday") else "muted",
        "avg_len": f"{avg_len} chars" if avg_len else "—",
        "avg_len_class": "" if avg_len else "muted",
        "reactions_given": commas(profile.get("reactions_given") or 0),
        "reactions_received": commas(profile.get("reactions_received") or 0),
        "replies": commas(profile.get("replies") or 0),
        "top_emoji": esc(f":{top_emoji['name']}: x{top_emoji['count']}") if top_emoji else "—",
        "top_emoji_class": "" if top_emoji else "muted",

        "footer_left": esc(f"{member.guild.name} · Level Profile"),
        "footer_right": esc(datetime.now(timezone.utc).strftime("%d %b %Y")),
    }
