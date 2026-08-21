"""Where the "newcomer role" is configured, across the three cogs that use one.

Each cog keeps its own setting in its own database and none of them read the
others, so pointing one panel at a role does nothing for the other two. That is
deliberate — they are independent features — but a mismatch is invisible and
silently half-works. Every panel that sets one of these renders this summary so
the other two are visible at the point of editing.

Adding a fourth consumer means adding it here, not writing another lookup.
"""

import logging

logger = logging.getLogger('bot_main')


async def _newcomer_cog_role(bot, guild):
    """cogs/onboarding.py — grants on join, swaps away at the member level."""
    cog = bot.get_cog("Onboarding")
    if not cog:
        return None
    newcomer_id, _ = await cog._role_setting_ids()
    return newcomer_id


async def _economy_role(bot, guild):
    """cogs/economy — pays currency for replying to a holder."""
    cog = bot.get_cog("Economy")
    if not cog or not getattr(cog, "config", None):
        return None
    return cog.config.get_int("role_newcomer", 0) or None


async def _inactivity_role(bot, guild):
    """cogs/inactivity.py — only holders are eligible for inactivity nudges."""
    cog = bot.get_cog("Inactivity")
    if not cog or not getattr(cog, "db", None):
        return None
    row = await cog.db.fetch_one(
        "SELECT highlight_role_id FROM inactivity_config WHERE guild_id = ?", (guild.id,)
    )
    return (row["highlight_role_id"] if row else None) or None


# (label, panel that sets it, resolver). Order is the order they render in.
SOURCES = (
    ("Onboarding", "Welcome & Onboarding module → Roles & graduation", _newcomer_cog_role),
    ("Economy", "/economy_panel → Roles", _economy_role),
    ("Inactivity", "inactivity panel", _inactivity_role),
)


async def collect(bot, guild):
    """[(label, panel, role_id_or_None), ...] for every consumer."""
    results = []
    for label, panel, resolver in SOURCES:
        try:
            role_id = await resolver(bot, guild)
        except Exception as e:
            logger.warning(f"Could not read the {label} newcomer role: {e}")
            role_id = None
        results.append((label, panel, role_id))
    return results


async def summary(bot, guild, *, exclude=None):
    """Markdown block naming each cog's newcomer role, flagging any mismatch.

    `exclude` drops one label, so a panel doesn't restate the setting the admin
    is already looking at.
    """
    everything = await collect(bot, guild)
    rows = [r for r in everything if r[0] != exclude]
    if not rows:
        return ""

    lines = []
    for label, panel, role_id in rows:
        role = guild.get_role(role_id) if role_id else None
        if role:
            value = role.mention
        elif role_id:
            value = f"*deleted role {role_id}*"
        else:
            value = "*unset*"
        lines.append(f"• **{label}** — {value}  ·  `{panel}`")

    # Only compare what's actually been set; unset is incomplete, not a conflict.
    configured = {r[2] for r in everything if r[2]}
    if len(configured) > 1:
        lines.append("⚠️ These point at **different roles** — each feature follows its own.")

    return "\n".join(lines)
