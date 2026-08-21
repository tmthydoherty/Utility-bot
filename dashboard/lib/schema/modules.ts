import { ChannelType } from "@/lib/discord/types";
import { FieldType, type ModuleSchema } from "./types";

/**
 * The module catalogue.
 *
 * Every cog the bot loads appears here so the Modules page is an honest
 * inventory rather than a list of the three things that happen to be wired up.
 * `configurable: false` means "listed, not yet editable from the web" — the
 * card says so plainly instead of opening an empty settings page.
 *
 * The three schemas that are filled in were picked because their configs are
 * already flat and readable on disk (welcome_config.json, security_config.json,
 * guild_settings.json). They prove the renderer against real field types —
 * channels, roles, multi-selects, conditional visibility — without needing any
 * of the bot's more involved config shapes decoded first.
 *
 * Nested config keys are written dotted (`enabled_modules.invite_links`). The
 * adapter that eventually writes real files un-nests them; the form only ever
 * sees a flat map, which is what keeps FieldRenderer simple.
 */

/**
 * Welcome & Onboarding — the merged home for the greeting, the introduction
 * system, and the newcomer→member points journey (cogs/onboarding.py, formerly
 * the separate welcome and newcomer cogs).
 *
 * The flat settings here ride the shared per-guild bridge under the module key
 * `welcome` (kept from the old welcome module so routes and the bridge don't
 * churn). The intro/points half is global on the bot; it's echoed under the
 * current server and edited from here. The interactive parts that aren't flat
 * settings — the question pool, the blacklist, per-member point editing, the
 * ranked lists and the game→LFG mappings — get their own panels on this page,
 * driven by a snapshot+commands bridge (see lib/onboarding/store.ts and
 * cogs/onboarding_config_sync.py).
 *
 * The intro button's channel is `intro_button_channel_id` here so it doesn't
 * collide with the greeting's own `intro_channel_id` nudge target; the cog maps
 * it back to the intro DB's `intro_channel_id`. Tier and VIP roles live in the
 * Role Alerts & Colours cog; the cog fans those keys out to it.
 */
export const welcome: ModuleSchema = {
  id: "welcome",
  name: "Welcome & Onboarding",
  description: "Greet arrivals, run introductions, and carry newcomers to member with reply points and tiers.",
  icon: "DoorOpen",
  category: "community",
  cog: "cogs/onboarding.py",
  configurable: true,
  sections: [
    {
      id: "welcome",
      title: "Welcome message",
      description: "The greeting posted when someone joins, and where it points them.",
      fields: [
        {
          key: "welcome_channel_id",
          label: "Welcome channel",
          type: FieldType.CHANNEL,
          help: "Arrival messages are posted here.",
          required: true,
          channelTypes: [ChannelType.GuildText, ChannelType.GuildAnnouncement],
        },
        {
          key: "suggest_introduction",
          label: "Suggest an introduction",
          type: FieldType.BOOL,
          default: false,
          help: "Add a line to the greeting nudging new members to introduce themselves. This is only a friendly pointer — the actual Introduce Yourself button is set under Introductions below.",
        },
        {
          key: "intro_channel_id",
          label: "Channel to suggest introductions in",
          type: FieldType.CHANNEL,
          help: "The greeting points new members here to say hello. Shown only when the nudge above is on.",
          channelTypes: [ChannelType.GuildText, ChannelType.GuildForum],
          visibleWhen: { key: "suggest_introduction", equals: true },
        },
        {
          key: "lfg_forum_id",
          label: "LFG forum",
          type: FieldType.CHANNEL,
          help: "The forum new members are pointed to when they've picked games. Per-game threads are set under Game → LFG mappings below.",
          channelTypes: [ChannelType.GuildForum],
        },
      ],
    },
    {
      id: "introductions",
      title: "Introductions",
      description: "The Introduce Yourself button and the discussion threads it opens.",
      fields: [
        {
          key: "intro_button_channel_id",
          label: "Introduce Yourself channel",
          type: FieldType.CHANNEL,
          help: "Where the Introduce Yourself button is posted. Setting or changing it re-posts the button here automatically.",
          channelTypes: [ChannelType.GuildText],
        },
        {
          key: "thread_channel_id",
          label: "Intro thread parent",
          type: FieldType.CHANNEL,
          help: "Intro discussion threads (and their Lore Drop banners) are opened under this channel.",
          channelTypes: [ChannelType.GuildText, ChannelType.GuildForum],
        },
        {
          key: "welcome_msg",
          label: "Intro thread message",
          type: FieldType.MULTILINE,
          help: "Shown at the top of a new intro thread. Use {username} for a mention. Leave blank for the built-in default.",
          wide: true,
        },
      ],
    },
    {
      id: "roles",
      title: "Roles & graduation",
      description: "The starter role and when a newcomer becomes a full member.",
      fields: [
        {
          key: "newcomer_role",
          label: "Newcomer role",
          type: FieldType.ROLE,
          help: "Granted on join. Replies to a holder earn reply points, and it's removed once they reach the member level.",
        },
        {
          key: "member_role",
          label: "Member role",
          type: FieldType.ROLE,
          help: "Given when a newcomer reaches the level below, replacing the newcomer role.",
        },
        {
          key: "member_role_level",
          label: "Member level",
          type: FieldType.NUMBER,
          help: "The economy level at which a newcomer graduates to the member role. Voice time counts toward it.",
          min: 1,
          max: 100,
          default: 2,
        },
      ],
    },
    {
      id: "tiers",
      title: "Tier & VIP roles",
      description: "Reward roles awarded by points. These are shared with the Role Alerts & Colours module — a change here changes them there too. A gate role can't also be its own VIP role.",
      fields: [
        {
          key: "tier1_role",
          label: "Tier 1 (baseline) role",
          type: FieldType.ROLE,
          help: "The baseline everyone sits at. Usually left unset.",
        },
        {
          key: "tier2_role",
          label: "Tier 2 role",
          type: FieldType.ROLE,
          help: "Awarded once a member's effective points reach the Tier 2 threshold below.",
        },
        {
          key: "tier3_role",
          label: "Tier 3 role",
          type: FieldType.ROLE,
          help: "Awarded once effective points reach the Tier 3 threshold below.",
        },
        {
          key: "tier2_vip_roles",
          label: "Tier 2 VIP roles",
          type: FieldType.ROLE,
          multi: true,
          help: "A permanent point floor worth the Tier 2 threshold. Earned points stack on top, so a Tier 2 VIP can climb to Tier 3.",
          wide: true,
        },
        {
          key: "tier3_vip_roles",
          label: "Tier 3 VIP roles",
          type: FieldType.ROLE,
          multi: true,
          help: "A permanent point floor worth the Tier 3 threshold.",
          wide: true,
        },
      ],
    },
    {
      id: "points",
      title: "Points",
      description: "What replies and intros are worth, and the thresholds that define the tiers.",
      fields: [
        {
          key: "reply_points",
          label: "Points per newcomer reply",
          type: FieldType.NUMBER,
          help: "Paid to a regular for replying to someone holding the newcomer role. Decimals are allowed (e.g. 0.5).",
          min: 0,
          max: 1000,
          default: 0.5,
        },
        {
          key: "hourly_point_cap",
          label: "Hourly point cap",
          type: FieldType.NUMBER,
          help: "Most points a member can earn in one hour from replies and intro threads. 0 means no cap.",
          min: 0,
          max: 100000,
          default: 0,
        },
        {
          key: "tier2_points",
          label: "Tier 2 threshold",
          type: FieldType.NUMBER,
          help: "Effective points needed to hold the Tier 2 role.",
          min: 0,
          max: 10000000,
          default: 0,
        },
        {
          key: "tier3_points",
          label: "Tier 3 threshold",
          type: FieldType.NUMBER,
          help: "Effective points needed to hold the Tier 3 role.",
          min: 0,
          max: 10000000,
          default: 0,
        },
      ],
    },
  ],
};

export const gamePoll: ModuleSchema = {
  id: "game-poll",
  name: "Game Polls",
  description: "Polls for what the server plays next, with weighted voting and VC tracking.",
  icon: "Vote",
  category: "engagement",
  cog: "cogs/game_poll.py",
  configurable: true,
  link: "/modules/game-poll",
  sections: [
    {
      id: "general",
      title: "Module",
      description: "Turn Game Polls on or off for the server.",
      fields: [
        {
          key: "enabled",
          label: "Enable Game Polls",
          type: FieldType.BOOL,
          default: true,
          help: "When off, Vibey stops posting polls, closing them, and tracking voice sessions. Your games, channels and settings are kept.",
          wide: true,
        },
      ],
    },
    {
      id: "channels",
      title: "Channels & Categories",
      description: "Where polls are posted, and where the Game Night voice channel is created.",
      fields: [
        {
          key: "poll_channel_ids",
          label: "Primary Poll Channels",
          type: FieldType.CHANNEL,
          multi: true,
          channelTypes: [ChannelType.GuildText, ChannelType.PublicThread, ChannelType.PrivateThread],
          help: "Polls are posted here and receive the Game Night role ping.",
        },
        {
          key: "secondary_poll_channel_ids",
          label: "Secondary Poll Channels",
          type: FieldType.CHANNEL,
          multi: true,
          channelTypes: [ChannelType.GuildText, ChannelType.PublicThread, ChannelType.PrivateThread],
          help: "Polls are posted here too, but without pinging the role.",
        },
        {
          key: "vc_category_id",
          label: "Voice Category",
          type: FieldType.CHANNEL,
          channelTypes: [ChannelType.GuildCategory],
          help: "Where Vibey puts the Game Night voice channel it creates when you click Create Game VC on the panel. Only time in that channel counts toward returning-player status and Participant History — other voice channels aren't tracked.",
        }
      ]
    },
    {
      id: "role_settings",
      title: "Role Settings",
      description: "Role to ping when a new poll drops.",
      fields: [
        {
          key: "game_night_role_id",
          label: "Game Night Role",
          type: FieldType.ROLE,
          help: "The role pinged for Game Night polls.",
        },
        {
          key: "ping_role_enabled",
          label: "Enable Ping",
          type: FieldType.BOOL,
          default: false,
          help: "Turn the ping on or off.",
        }
      ]
    },
    {
      id: "voting_weights",
      title: "Voting Weights",
      description: "How many points a vote is worth based on player status.",
      fields: [
        {
          key: "weight_1",
          label: "Returning Player Weight",
          type: FieldType.NUMBER,
          default: 3,
          min: 1,
          max: 10,
          help: "Points for users who played in a recent voice session.",
        },
        {
          key: "weight_2",
          label: "Active Voter Weight",
          type: FieldType.NUMBER,
          default: 2,
          min: 1,
          max: 10,
          help: "Points for users who voted on the previous poll but didn't play.",
        },
        {
          key: "weight_3",
          label: "Default Voter Weight",
          type: FieldType.NUMBER,
          default: 1,
          min: 1,
          max: 10,
          help: "Points for everyone else.",
        }
      ]
    },
    {
      id: "visuals",
      title: "Visuals",
      description: "Customize the look of the Game Night polls.",
      fields: [
        {
          key: "game_night_banner_url",
          label: "Banner Image URL",
          type: FieldType.TEXT,
          help: "A direct link to an image to show on the active poll embed.",
          wide: true,
        }
      ]
    }
  ]
};

export const rolesAndAlerts: ModuleSchema = {
  id: "roles-and-alerts",
  name: "Roles & Alerts",
  description: "Self-serve color and alert roles based on tiers, plus bulk role assignment.",
  icon: "UserCog",
  category: "community",
  cog: "cogs/RolesAndAlerts.py",
  configurable: true,
  link: "/modules/roles-and-alerts",
  sections: [],
};

const security: ModuleSchema = {
  id: "security",
  name: "Security & Anti-Nuke",
  description: "Spam filtering, mass-mention limits, and destructive-action tripwires.",
  icon: "ShieldAlert",
  category: "moderation",
  cog: "cogs/security.py",
  configurable: true,
  sections: [
    {
      id: "response",
      title: "Response",
      description: "What happens when something trips.",
      fields: [
        {
          key: "alert_channel_id",
          label: "Alert channel",
          type: FieldType.CHANNEL,
          help: "Every detection is reported here. Keep it staff-only.",
          required: true,
          channelTypes: [ChannelType.GuildText],
        },
        {
          key: "quarantine_role_id",
          label: "Quarantine role",
          type: FieldType.ROLE,
          help: "Applied to an account that trips a tripwire. Should deny sending everywhere.",
          required: true,
        },
        // The trip thresholds and windows (mass-mention count, nuke limits, the
        // action window) are fixed constants in the security cog, not per-guild
        // settings — so they aren't offered here.
      ],
    },
    {
      id: "modules",
      title: "Detections",
      description: "Each can be turned off independently.",
      fields: [
        { key: "enabled_modules.invite_links", label: "Invite links", type: FieldType.BOOL, default: true, help: "Blocks unsolicited server invites." },
        { key: "enabled_modules.message_flood", label: "Message flooding", type: FieldType.BOOL, default: true, help: "Many messages in a short window." },
        { key: "enabled_modules.duplicate_spam", label: "Duplicate spam", type: FieldType.BOOL, default: true, help: "The same message across many channels." },
        { key: "enabled_modules.mass_mentions", label: "Mass mentions", type: FieldType.BOOL, default: true, help: "Pings above the limit below." },
        { key: "enabled_modules.nuke_channel_delete", label: "Channel deletion", type: FieldType.BOOL, default: true, help: "Rapid channel deletions." },
        { key: "enabled_modules.nuke_role_delete", label: "Role deletion", type: FieldType.BOOL, default: true, help: "Rapid role deletions." },
        { key: "enabled_modules.nuke_mass_ban", label: "Mass bans", type: FieldType.BOOL, default: true, help: "Bans faster than a human issues them." },
      ],
    },
    {
      id: "limits",
      title: "Limits",
      fields: [
        {
          key: "mention_exempt_roles",
          label: "Exempt from mention limits",
          type: FieldType.ROLE,
          multi: true,
          help: "Roles that legitimately ping large groups.",
          visibleWhen: { key: "enabled_modules.mass_mentions", equals: true },
        },
      ],
    },
    {
      id: "exemptions",
      title: "Exemptions",
      description: "Trusted actors the filters ignore entirely.",
      fields: [
        { key: "mod_roles", label: "Moderator roles", type: FieldType.ROLE, multi: true },
        { key: "admin_roles", label: "Admin roles", type: FieldType.ROLE, multi: true, help: "May change these settings from Discord." },
        { key: "exempt_channels", label: "Exempt channels", type: FieldType.CHANNEL, multi: true, help: "Filters never run here." },
        { key: "whitelisted_users", label: "Whitelisted users", type: FieldType.USER, multi: true, help: "Use sparingly — this is a full bypass." },
      ],
    },
  ],
};

/**
 * Leveling — the XP half of the old paired "Economy & Levels" card.
 *
 * Split out on purpose: XP accrual and the Points economy are two systems that
 * happen to share a cog, and a single settings page for both buried the four
 * numbers most servers ever touch under the shop, the box and the curses. This
 * page is only those numbers.
 *
 * Keys are the bot's own config keys (cogs/economy/config.py), verbatim, so
 * wiring the cog up later is a lookup rather than a translation table.
 */
const leveling: ModuleSchema = {
  id: "leveling",
  name: "Leveling",
  description: "Message and voice XP, the level curve, and channels that don't count.",
  icon: "TrendingUp",
  category: "leveling",
  cog: "cogs/economy/leveling.py",
  configurable: true,
  sections: [
    {
      id: "message",
      title: "Message XP",
      description: "XP for chatting. Each qualifying message grants a random amount in this range.",
      fields: [
        {
          key: "lvl_msg_xp_min",
          label: "Minimum per message",
          type: FieldType.NUMBER,
          help: "The low end of the roll. Set it equal to the maximum for a flat rate.",
          min: 0,
          max: 1000,
          default: 15,
        },
        {
          key: "lvl_msg_xp_max",
          label: "Maximum per message",
          type: FieldType.NUMBER,
          help: "The high end of the roll. Must not be below the minimum.",
          min: 0,
          max: 1000,
          default: 25,
        },
        {
          key: "lvl_xp_cooldown",
          label: "Cooldown between grants",
          type: FieldType.DURATION,
          help: "A member earns message XP at most once per window. Messages sent inside it still count toward activity stats — they just don't pay XP.",
          min: 0,
          max: 3600,
          default: 60,
          wide: true,
        },
      ],
    },
    {
      id: "voice",
      title: "Voice XP",
      description: "XP for time spent talking.",
      fields: [
        {
          key: "lvl_voice_xp_per_min",
          label: "XP per minute in voice",
          type: FieldType.NUMBER,
          help: "Only paid while at least two people share a channel and nobody is deafened. AFK channels never count. Set to 0 to switch voice XP off entirely.",
          min: 0,
          max: 1000,
          default: 5,
          wide: true,
        },
      ],
    },
    {
      id: "exclusions",
      title: "Exclusions",
      fields: [
        {
          key: "lvl_excluded_channels",
          label: "Channels that earn no XP",
          type: FieldType.CHANNEL,
          multi: true,
          help: "Off-topic, spam and bot-command channels usually belong here. Excluding a channel also excludes any threads under it.",
          channelTypes: [ChannelType.GuildText, ChannelType.GuildVoice, ChannelType.GuildAnnouncement, ChannelType.GuildForum],
          wide: true,
        },
      ],
    },
  ],
};

/**
 * Economy — the Points half of the old paired card.
 *
 * Everything that earns, prices or spends Points. Organised the way the Discord
 * panel is (earning, roles, channels, shop, box) so an admin who knows one
 * knows the other. Item prices hide when the item is switched off, and each
 * curse's timing hides until its Mystery Box outcome is enabled, so the page is
 * only ever as long as the current configuration warrants.
 */
export const economy: ModuleSchema = {
  id: "economy",
  name: "Economy",
  description: "Points earning, the shop, the Mystery Box and its curses.",
  icon: "Coins",
  category: "economy",
  cog: "cogs/economy/cog.py",
  configurable: true,
  // Lifted out of the Modules grid into its own top-level section — see
  // app/dashboard/[guildId]/economy. Still listed in MODULES so getModule and
  // the config bridge resolve it; the grid hides it and /modules/economy
  // redirects here.
  relocated: true,
  link: "/economy",
  sections: [
    {
      id: "earning",
      title: "Earning",
      description: "What each activity pays. A cap of 0 means no daily limit; once-daily sources pay at most once a day regardless.",
      fields: [
        { key: "earn_message_value", label: "Message sent", type: FieldType.NUMBER, help: "Paid per message, on the same cooldown as message XP.", min: 0, max: 100000, default: 1 },
        { key: "earn_message_cap", label: "Message — daily cap", type: FieldType.NUMBER, help: "Most Points a member can earn from messages in a day.", min: 0, max: 100000, default: 50 },
        { key: "earn_first_message_value", label: "First message of the day", type: FieldType.NUMBER, help: "A one-time bonus for showing up. Paid once daily.", min: 0, max: 100000, default: 15 },
        { key: "earn_streak7_value", label: "7-day streak bonus", type: FieldType.NUMBER, help: "Paid when a member's daily streak reaches seven.", min: 0, max: 100000, default: 100 },
        { key: "earn_newcomer_reply_value", label: "Reply to a newcomer", type: FieldType.NUMBER, help: "Rewards regulars for welcoming members who hold the newcomer role.", min: 0, max: 100000, default: 5 },
        { key: "earn_newcomer_reply_cap", label: "Newcomer reply — daily cap", type: FieldType.NUMBER, help: "Stops reply-farming for Points.", min: 0, max: 100000, default: 25 },
        { key: "earn_trivia_value", label: "Daily trivia played", type: FieldType.NUMBER, help: "Paid once daily for taking part in trivia.", min: 0, max: 100000, default: 25 },
        { key: "earn_daily_quote_value", label: "Daily quote played", type: FieldType.NUMBER, help: "Paid once daily for the daily-quote game.", min: 0, max: 100000, default: 25 },
        { key: "earn_qotd_value", label: "Question of the Day answered", type: FieldType.NUMBER, help: "Paid once daily for answering the QOTD, by reply or in its thread.", min: 0, max: 100000, default: 25 },
        { key: "earn_custom_match_value", label: "Custom match played", type: FieldType.NUMBER, help: "Paid for taking part in a custom match.", min: 0, max: 100000, default: 50 },
        { key: "earn_custom_match_cap", label: "Custom match — daily cap", type: FieldType.NUMBER, help: "0 leaves it uncapped.", min: 0, max: 100000, default: 0 },
        { key: "earn_game_night_value", label: "Game night attended", type: FieldType.NUMBER, help: "Paid once daily for attending a game night.", min: 0, max: 100000, default: 75 },
      ],
    },
    {
      id: "roles",
      title: "Roles",
      description: "The bot's own role must sit above the Throne and Vibes roles to be able to assign them.",
      fields: [
        { key: "role_throne", label: "The Throne", type: FieldType.ROLE, help: "The 1-of-1 role the Throne shop item hands between members." },
        { key: "role_vibes", label: "Vibes role", type: FieldType.ROLE, help: "Awarded by the rarest Mystery Box outcome." },
        { key: "role_newcomer", label: "Newcomer role", type: FieldType.ROLE, help: "Replies to members holding this role earn the newcomer-reply bonus above." },
      ],
    },
    {
      id: "channels",
      title: "Channels",
      fields: [
        { key: "ch_approval", label: "Approvals", type: FieldType.CHANNEL, help: "GIF-command and emoji submissions land here for staff to approve.", channelTypes: [ChannelType.GuildText] },
        { key: "ch_hof", label: "Hall of Fame", type: FieldType.CHANNEL, help: "Where the Hall of Fame shop item posts.", channelTypes: [ChannelType.GuildText] },
        { key: "ch_proxy_log", label: "Proxy log", type: FieldType.CHANNEL, help: "Staff-only record of who sent each anonymous Proxy message. Keep it private.", channelTypes: [ChannelType.GuildText] },
        { key: "ch_match_queue", label: "Customs queue override", type: FieldType.CHANNEL, help: "Leave empty to use each game's own queue channel.", channelTypes: [ChannelType.GuildText] },
        { key: "proxy_blocklist", label: "Proxy blocklist", type: FieldType.CHANNEL, multi: true, help: "Channels the anonymous Proxy item may never post into.", channelTypes: [ChannelType.GuildText], wide: true },
      ],
    },
    {
      id: "shop_availability",
      title: "Shop — Availability",
      description: "Turn an item off to pull it from the shop without losing its price.",
      fields: [
        { key: "enabled_auto_react", label: "Auto React", type: FieldType.BOOL, default: true, help: "Adds an emoji to every message a target sends for a day." },
        { key: "enabled_hof_post", label: "HoF Post", type: FieldType.BOOL, default: true, help: "Post one message into the Hall of Fame." },
        { key: "enabled_gif_command", label: "Add GIF Command", type: FieldType.BOOL, default: true, help: "Buy a personal !command that posts a GIF." },
        { key: "enabled_add_emoji", label: "Add Emoji", type: FieldType.BOOL, default: true, help: "Add a server emoji on trial; it stays if it catches on." },
        { key: "enabled_throne", label: "The Throne", type: FieldType.BOOL, default: true, help: "Seize the 1-of-1 Throne role by outbidding the holder." },
        { key: "enabled_customs_match", label: "Customs Match", type: FieldType.BOOL, default: true, help: "Open a custom-match queue for a single match." },
        { key: "enabled_proxy", label: "Proxy", type: FieldType.BOOL, default: true, help: "Send one message anonymously through the bot." },
        { key: "enabled_mystery_box", label: "Mystery Box", type: FieldType.BOOL, default: true, help: "One random outcome — some a reward, some a curse." },
      ],
    },
    {
      id: "shop_pricing",
      title: "Shop — Pricing",
      description: "Prices in Points. Each appears once its item is switched on above.",
      fields: [
        { key: "price_auto_react", label: "Auto React", type: FieldType.NUMBER, min: 0, max: 10000000, default: 500, visibleWhen: { key: "enabled_auto_react", equals: true } },
        { key: "price_hof_post", label: "HoF Post", type: FieldType.NUMBER, min: 0, max: 10000000, default: 1200, visibleWhen: { key: "enabled_hof_post", equals: true } },
        { key: "price_add_emoji", label: "Add Emoji", type: FieldType.NUMBER, min: 0, max: 10000000, default: 1500, visibleWhen: { key: "enabled_add_emoji", equals: true } },
        { key: "price_throne", label: "The Throne — starting price", type: FieldType.NUMBER, help: "The opening bid. Once seized, the price is always one more than the current holder paid.", min: 0, max: 10000000, default: 2000, visibleWhen: { key: "enabled_throne", equals: true } },
        { key: "price_customs_match", label: "Customs Match", type: FieldType.NUMBER, help: "Non-refundable if the queue never fills.", min: 0, max: 10000000, default: 1000, visibleWhen: { key: "enabled_customs_match", equals: true } },
        { key: "price_proxy", label: "Proxy", type: FieldType.NUMBER, min: 0, max: 10000000, default: 600, visibleWhen: { key: "enabled_proxy", equals: true } },
        { key: "price_mystery_box", label: "Mystery Box", type: FieldType.NUMBER, min: 0, max: 10000000, default: 750, visibleWhen: { key: "enabled_mystery_box", equals: true } },
      ],
    },
    {
      id: "gif_commands",
      title: "GIF Commands",
      description: "The Add GIF Command item is tiered — each slot a member buys costs more than the last.",
      fields: [
        { key: "price_gif_1", label: "First command", type: FieldType.NUMBER, min: 0, max: 10000000, default: 800, visibleWhen: { key: "enabled_gif_command", equals: true } },
        { key: "price_gif_2", label: "Second command", type: FieldType.NUMBER, min: 0, max: 10000000, default: 1600, visibleWhen: { key: "enabled_gif_command", equals: true } },
        { key: "price_gif_3", label: "Third command", type: FieldType.NUMBER, min: 0, max: 10000000, default: 3200, visibleWhen: { key: "enabled_gif_command", equals: true } },
        { key: "gif_max_per_user", label: "Max commands per member", type: FieldType.NUMBER, help: "Only three tiers are priced, so this tops out at 3.", min: 1, max: 3, default: 3, visibleWhen: { key: "enabled_gif_command", equals: true } },
      ],
    },
    {
      id: "timings",
      title: "Item Timings",
      description: "How long each item's effect lasts. Shown only for items that are switched on.",
      fields: [
        { key: "auto_react_hours", label: "Auto React duration (hours)", type: FieldType.NUMBER, min: 1, max: 168, default: 24, visibleWhen: { key: "enabled_auto_react", equals: true } },
        { key: "hof_grant_minutes", label: "HoF posting window (minutes)", type: FieldType.NUMBER, help: "How long the buyer keeps access to post in the Hall of Fame.", min: 1, max: 1440, default: 15, visibleWhen: { key: "enabled_hof_post", equals: true } },
        { key: "emoji_trial_days", label: "Emoji trial (days)", type: FieldType.NUMBER, help: "How long a bought emoji stays before it has to earn its keep.", min: 1, max: 365, default: 30, visibleWhen: { key: "enabled_add_emoji", equals: true } },
        { key: "proxy_max_chars", label: "Proxy character limit", type: FieldType.NUMBER, help: "Longest anonymous message the Proxy item will send.", min: 1, max: 2000, default: 500, visibleWhen: { key: "enabled_proxy", equals: true } },
        { key: "match_queue_hours", label: "Customs queue lifetime (hours)", type: FieldType.NUMBER, help: "How long a bought custom-match queue stays open before it lapses.", min: 1, max: 72, default: 3, visibleWhen: { key: "enabled_customs_match", equals: true } },
      ],
    },
    {
      id: "box_odds",
      title: "Mystery Box",
      description: "Weights are relative — a heavier outcome simply comes up more often. Turn an outcome off to remove it from the wheel.",
      fields: [
        { key: "box_points_small", label: "Small Points payout", type: FieldType.NUMBER, help: "The lower of the two cash outcomes.", min: 0, max: 10000000, default: 250, visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "box_points_big", label: "Big Points payout", type: FieldType.NUMBER, help: "The jackpot outcome.", min: 0, max: 10000000, default: 1500, visibleWhen: { key: "enabled_mystery_box", equals: true } },

        { key: "mbox_vibes_role_enabled", label: "Vibes Role — enabled", type: FieldType.BOOL, default: true, help: "The rarest reward: hands over the Vibes role.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_vibes_role_weight", label: "Vibes Role — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 2, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_vibes_role_enabled", equals: true }] } },
        { key: "mbox_curse_wipe_enabled", label: "Curse Wipe — enabled", type: FieldType.BOOL, default: true, help: "A reward that clears one active curse.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_curse_wipe_weight", label: "Curse Wipe — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 14, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_curse_wipe_enabled", equals: true }] } },
        { key: "mbox_nickname_hijack_enabled", label: "Nickname Hijack — enabled", type: FieldType.BOOL, default: true, help: "Rename one member for a day; they can't change it back.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_nickname_hijack_weight", label: "Nickname Hijack — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 14, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_nickname_hijack_enabled", equals: true }] } },
        { key: "mbox_points_small_enabled", label: "Points (small) — enabled", type: FieldType.BOOL, default: true, help: "Pays the small Points amount set above.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_points_small_weight", label: "Points (small) — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 20, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_points_small_enabled", equals: true }] } },
        { key: "mbox_points_big_enabled", label: "Points (big) — enabled", type: FieldType.BOOL, default: true, help: "Pays the big Points amount set above.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_points_big_weight", label: "Points (big) — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 5, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_points_big_enabled", equals: true }] } },
        { key: "mbox_clown_mode_enabled", label: "Clown Mode — enabled", type: FieldType.BOOL, default: true, help: "A curse. Timing is under Curses below.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_clown_mode_weight", label: "Clown Mode — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 17, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_clown_mode_enabled", equals: true }] } },
        { key: "mbox_slowmo_enabled", label: "Slowmo — enabled", type: FieldType.BOOL, default: true, help: "A curse. Timing is under Curses below.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_slowmo_weight", label: "Slowmo — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 17, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_slowmo_enabled", equals: true }] } },
        { key: "mbox_spongebob_enabled", label: "SpongeBob Case — enabled", type: FieldType.BOOL, default: true, help: "A curse. Timing is under Curses below.", visibleWhen: { key: "enabled_mystery_box", equals: true } },
        { key: "mbox_spongebob_weight", label: "SpongeBob Case — weight", type: FieldType.NUMBER, min: 0, max: 1000, default: 11, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_spongebob_enabled", equals: true }] } },
      ],
    },
    {
      id: "curses",
      title: "Curses",
      description: "How long each curse lasts when the Mystery Box inflicts it. Shown only for outcomes that are enabled.",
      fields: [
        { key: "nickname_hijack_hours", label: "Nickname Hijack (hours)", type: FieldType.NUMBER, min: 1, max: 168, default: 24, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_nickname_hijack_enabled", equals: true }] } },
        { key: "clown_hours", label: "Clown Mode (hours)", type: FieldType.NUMBER, min: 1, max: 168, default: 24, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_clown_mode_enabled", equals: true }] } },
        { key: "slowmo_minutes", label: "Slowmo duration (minutes)", type: FieldType.NUMBER, min: 1, max: 1440, default: 60, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_slowmo_enabled", equals: true }] } },
        { key: "slowmo_interval_minutes", label: "Slowmo gap between messages (minutes)", type: FieldType.NUMBER, help: "How long the cursed member must wait between messages.", min: 1, max: 1440, default: 5, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_slowmo_enabled", equals: true }] } },
        { key: "spongebob_days", label: "SpongeBob duration (days)", type: FieldType.NUMBER, min: 1, max: 30, default: 7, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_spongebob_enabled", equals: true }] } },
        { key: "spongebob_chance", label: "SpongeBob trigger chance (%)", type: FieldType.NUMBER, help: "The chance each message gets SpongeBob-cased while the curse is active.", min: 0, max: 100, default: 5, visibleWhen: { all: [{ key: "enabled_mystery_box", equals: true }, { key: "mbox_spongebob_enabled", equals: true }] } },
      ],
    },
  ],
};

/**
 * Question of the Day — a scheduled prompt plus a shared question pool.
 *
 * The flat settings here (schedule, channels, ping role) ride the shared
 * per-guild module-config bridge, exactly like welcome/security/suggestions, so
 * a save reaches the bot within about ten seconds. The question *pool* — adding
 * questions, editing the list, and previewing tomorrow's — isn't a flat setting,
 * so it's managed by a purpose-built panel on this module's page that talks to
 * its own bridge (see lib/qotd/store.ts and cogs/qotd_config_sync.py). Keys are
 * the cog's own, verbatim (cogs/qotd.py).
 */
const qotd: ModuleSchema = {
  id: "qotd",
  name: "Question of the Day",
  description: "A daily prompt posted on a schedule, with a community question pool.",
  icon: "MessageCircleQuestion",
  category: "engagement",
  cog: "cogs/qotd.py",
  configurable: true,
  sections: [
    {
      id: "posting",
      title: "Posting",
      description: "When and where the daily question goes out.",
      fields: [
        {
          key: "enabled",
          label: "Post automatically each day",
          type: FieldType.BOOL,
          default: false,
          help: "When off, a question only goes out when you post one by hand from the panel below or the Discord admin panel. This is separate from the module's own on/off switch above.",
          wide: true,
        },
        {
          key: "post_channel_ids",
          label: "Post channels",
          type: FieldType.CHANNEL,
          multi: true,
          required: true,
          help: "The question is posted to every channel here.",
          channelTypes: [ChannelType.GuildText, ChannelType.GuildAnnouncement],
          wide: true,
        },
        {
          key: "post_time",
          label: "Post time",
          type: FieldType.TEXT,
          placeholder: "10:00",
          max: 5,
          default: "10:00",
          help: "24-hour HH:MM, in the timezone below. Vibey ignores anything that isn't a valid time.",
        },
        {
          key: "timezone",
          label: "Timezone",
          type: FieldType.TEXT,
          placeholder: "UTC",
          max: 64,
          default: "UTC",
          help: "An IANA name like UTC, Europe/London or US/Central.",
        },
        {
          key: "auto_thread",
          label: "Start a discussion thread",
          type: FieldType.BOOL,
          default: true,
          help: "Opens a thread under each posted question for people to answer in.",
          wide: true,
        },
      ],
    },
    {
      id: "engagement",
      title: "Pings & suggestions",
      fields: [
        {
          key: "ping_role_id",
          label: "Ping role",
          type: FieldType.ROLE,
          help: "Mentioned when a question posts. Leave empty for no ping. The role must be mentionable.",
        },
        {
          key: "suggestion_log_channel_id",
          label: "Suggestion review channel",
          type: FieldType.CHANNEL,
          help: "Where member-submitted questions land for a mod to approve or deny.",
          channelTypes: [ChannelType.GuildText],
        },
      ],
    },
  ],
};

const nhie: ModuleSchema = {
  id: "nhie",
  name: "Never Have I Ever",
  description: "Voting rounds with community-submitted prompts.",
  icon: "Hand",
  category: "engagement",
  cog: "cogs/nhie.py",
  configurable: true,
  sections: [
    {
      id: "posting",
      title: "Posting",
      description: "Where the questions are posted and who can suggest them.",
      fields: [
        {
          key: "post_channels",
          label: "Post channels",
          type: FieldType.CHANNEL,
          multi: true,
          required: true,
          help: "The question is posted to every channel here.",
          channelTypes: [ChannelType.GuildText, ChannelType.GuildAnnouncement],
          wide: true,
        },
        {
          key: "log_channel",
          label: "Suggestion review channel",
          type: FieldType.CHANNEL,
          help: "Where member-submitted questions land for a mod to approve or deny.",
          channelTypes: [ChannelType.GuildText],
        },
        {
          key: "trigger_role",
          label: "Trigger role",
          type: FieldType.ROLE,
          help: "Mentioned or required role for the module.",
        },
        {
          key: "cooldown_hours",
          label: "Cooldown (hours)",
          type: FieldType.NUMBER,
          default: 1.0,
          help: "Time between questions.",
        },
      ],
    },
  ],
};

/**
 * Bot Presence — the status dot and activity line Vibey shows on its own
 * profile, everywhere it is a member.
 *
 * This is the one module whose settings are bot-wide rather than per-server:
 * Discord gives a bot a single presence across every server it's in, so a change
 * here changes what everyone sees. The copy says so plainly. It rides the shared
 * config bridge like welcome/security, but under a fixed global scope — see
 * `GLOBAL` in lib/bot/module-config.ts and the cog in cogs/bot_presence.py. Keys
 * are the cog's own, verbatim.
 */
const botPresence: ModuleSchema = {
  id: "bot-presence",
  name: "Bot Presence",
  description: "The status dot and activity line Vibey shows on its own profile.",
  icon: "Activity",
  category: "utility",
  cog: "cogs/bot_presence.py",
  configurable: true,
  // Discord always shows the bot some presence, so there's no "off" — no toggle.
  alwaysOn: true,
  relocated: true,
  link: "/control/presence",
  sections: [
    {
      id: "status",
      title: "Status",
      description:
        "This is bot-wide — Discord shows Vibey the same status in every server, so a change here changes what everyone sees.",
      fields: [
        {
          key: "status",
          label: "Status dot",
          type: FieldType.CHOICE,
          default: "online",
          help: "The colour of the dot on Vibey's avatar. Invisible looks offline but the bot keeps working.",
          choices: [
            { value: "online", label: "Online", description: "Green." },
            { value: "idle", label: "Idle", description: "Yellow — the away moon." },
            { value: "dnd", label: "Do Not Disturb", description: "Red." },
            { value: "invisible", label: "Invisible", description: "Appears offline, still runs." },
          ],
        },
      ],
    },
    {
      id: "activity",
      title: "Activity",
      description: "The line under Vibey's name, like “Watching Better Vibes”.",
      fields: [
        {
          key: "activity_type",
          label: "Activity",
          type: FieldType.CHOICE,
          default: "watching",
          wide: true,
          help: "What kind of line to show. Choose None to show nothing under the name.",
          choices: [
            { value: "none", label: "None", description: "No activity line." },
            { value: "playing", label: "Playing …", description: "“Playing <text>”." },
            { value: "listening", label: "Listening to …", description: "“Listening to <text>”." },
            { value: "watching", label: "Watching …", description: "“Watching <text>”." },
            { value: "competing", label: "Competing in …", description: "“Competing in <text>”." },
            { value: "streaming", label: "Streaming …", description: "Shows a live badge and links to a stream." },
            { value: "custom", label: "Custom text", description: "Shows exactly the text you type, with no prefix." },
          ],
        },
        {
          key: "activity_text",
          label: "Text",
          type: FieldType.TEXT,
          max: 128,
          placeholder: "Better Vibes",
          help: "The words shown after the activity. For Custom text, this is the whole line.",
          visibleWhen: { key: "activity_type", notEquals: "none" },
          wide: true,
        },
        {
          key: "stream_url",
          label: "Stream link",
          type: FieldType.TEXT,
          max: 200,
          placeholder: "https://twitch.tv/…",
          help: "The stream the live badge links to. Must be a Twitch or YouTube URL for the badge to show.",
          visibleWhen: { key: "activity_type", equals: "streaming" },
          wide: true,
        },
      ],
    },
  ],
};

/** Cogs that are listed but not yet editable here. */
function listed(
  id: string,
  name: string,
  description: string,
  icon: string,
  category: ModuleSchema["category"],
  cog: string,
): ModuleSchema {
  return { id, name, description, icon, category, cog, configurable: false };
}

export const MODULES: ModuleSchema[] = [
  welcome,
  security,

  // Engagement
  qotd,
  nhie,
  gamePoll,
  listed("daily-quote", "Daily Quote", "A quote a day, from a curated database.", "Quote", "engagement", "cogs/daily_quote.py"),
  listed("mediavote", "Media Voting", "Bracket voting for films, shows and music.", "Clapperboard", "engagement", "cogs/mediavote.py"),

  // Economy & Levels — two separate systems that share a cog, split into two
  // pages so neither buries the other.
  leveling,
  economy,

  // Community
  //
  // Ticketing keeps its own cog, JSON store and builder pages — topics, panels
  // and responses don't fit a flat settings form — so like Automations it earns
  // a card here that links straight to the builder rather than a settings page.
  {
    id: "ticketing",
    name: "Ticketing",
    description: "Support tickets, applications and surveys — panels, topics and responses.",
    icon: "TicketCheck",
    category: "community",
    cog: "cogs/ticketing/",
    configurable: false,
    external: true,
    link: "/ticketing",
  },

  // Moderation
  listed("modtools", "Mod Tools", "Mod threads, warnings and follow-up tracking.", "Gavel", "moderation", "cogs/modtools.py"),
  listed("inactivity", "Inactivity", "Find and prune inactive members.", "UserMinus", "moderation", "cogs/inactivity.py"),
  // "Server Management" (cogs/management.py) is not a module: it's the owner-only
  // cog-reload channel, which now lives in the Bot Control section next to the
  // audit log rather than the module grid. See app/dashboard/[guildId]/control.

  // Esports
  //
  // Custom Matches keeps its own cog, database and a whole multi-page section —
  // games, rank ladders, schedules, per-player MMR, leaderboards and analytics
  // don't fit a flat settings form — so like Ticketing and Automations it earns
  // a card here that links straight to its section rather than a settings page.
  {
    id: "custommatch",
    name: "Custom Matches",
    description: "In-house matchmaking — queues, MMR, rank ladders, schedules, leaderboards and player stats.",
    icon: "Swords",
    category: "esports",
    cog: "cogs/custommatch/",
    configurable: false,
    external: true,
    link: "/custom-matches",
  },

  // Utility
  //
  // Automations keep their own cog, database and builder pages, but they earn a
  // card here too so the Modules grid is the one front door for everything —
  // the card links straight to the builder rather than opening a settings form.
  {
    id: "automations",
    name: "Automations",
    description: "Make Vibey do things by itself — on a join, a keyword, or a schedule.",
    icon: "Zap",
    category: "utility",
    cog: "cogs/automations/",
    configurable: false,
    external: true,
    link: "/automations",
  },
  // Utility keeps its own cog, database and builder pages — custom embeds,
  // reminders, one-off messages, stickies, reaction rules and media channels
  // don't fit a flat settings form — so like Automations it earns a card here
  // that links straight to the builder. It absorbed the old Reminders and Send
  // Message modules; the mass-DM tool was removed entirely.
  {
    id: "utility",
    name: "Utility",
    description: "Custom embeds, reminders, one-off messages, sticky notes, reaction rules and media-only channels.",
    icon: "Wand2",
    category: "utility",
    cog: "cogs/utility/",
    configurable: false,
    external: true,
    link: "/utility",
  },
  // Custom Commands keeps its own cog, database and builder pages — a command is
  // a trigger, a set of random responses, gating and a cooldown, none of which
  // fit a flat settings form — so like Automations and Utility it earns a card
  // that links straight to the builder. It also owns the economy's purchased
  // GIF commands: they're listed here, marked ★ econ purchased, and can be
  // moderated from the same page.
  {
    id: "custom-commands",
    name: "Custom Commands",
    description: "Your own !commands — text, GIFs or embeds — plus members' purchased GIF commands, all in one list.",
    icon: "TerminalSquare",
    category: "utility",
    cog: "cogs/custom_commands/",
    configurable: false,
    external: true,
    link: "/custom-commands",
  },
  listed("emoji-stealer", "Emoji Stealer", "Copy emoji in from other servers.", "Smile", "utility", "cogs/emoji_stealer.py"),
  botPresence,

  // Logging
  listed("audit-log", "Audit Log", "Message, member, role and channel logging.", "ScrollText", "logging", "cogs/audit_log.py"),
  rolesAndAlerts,

  // The Activity Tracker is no longer a module: its live analytics are the
  // Overview page (Server / Member / Channel / Emoji / Leaderboard / Compare
  // tabs), read straight from tracking_data.db.
];

const BY_ID = new Map(MODULES.map((module) => [module.id, module]));

export function getModule(id: string): ModuleSchema | undefined {
  return BY_ID.get(id);
}

export function configurableModules(): ModuleSchema[] {
  return MODULES.filter((module) => module.configurable);
}

/**
 * A module is "supported" on the web when it has a real page — a settings form
 * (`configurable`) or a live dashboard (`analytics`). Everything else is listed
 * for completeness but still lives entirely in Discord, and the Modules page
 * files those separately.
 */
export function isSupported(module: ModuleSchema): boolean {
  return module.configurable || Boolean(module.analytics) || Boolean(module.external);
}
