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

const welcome: ModuleSchema = {
  id: "welcome",
  name: "Welcome & Goodbye",
  description: "Greet arrivals, note departures, and route new members to onboarding.",
  icon: "DoorOpen",
  category: "community",
  cog: "cogs/welcome.py",
  configurable: true,
  sections: [
    {
      id: "channels",
      title: "Channels",
      description: "Where the bot posts, and where new members are pointed.",
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
          key: "exit_channel_id",
          label: "Goodbye channel",
          type: FieldType.CHANNEL,
          help: "Leave messages go here. Often a staff-only channel.",
          channelTypes: [ChannelType.GuildText],
        },
        {
          key: "intro_channel_id",
          label: "Introductions channel",
          type: FieldType.CHANNEL,
          help: "Linked in the welcome message. Leave empty to omit the link.",
          channelTypes: [ChannelType.GuildText, ChannelType.GuildForum],
        },
        {
          key: "mod_channel_id",
          label: "Staff notifications",
          type: FieldType.CHANNEL,
          help: "Onboarding problems are reported here.",
          channelTypes: [ChannelType.GuildText],
        },
      ],
    },
    {
      id: "message",
      title: "Message",
      fields: [
        {
          key: "welcome_enabled",
          label: "Post a welcome message",
          type: FieldType.BOOL,
          help: "Turn off to keep onboarding roles without announcing arrivals.",
          default: true,
        },
        {
          key: "welcome_text",
          label: "Welcome text",
          type: FieldType.MULTILINE,
          help: "Supports {user}, {server} and {count}.",
          placeholder: "Welcome to {server}, {user}! You're member #{count}.",
          max: 2000,
          wide: true,
          // Hidden rather than disabled: an editor for a message that will
          // never be sent is just something to misread later.
          visibleWhen: { key: "welcome_enabled", equals: true },
        },
        {
          key: "ping_on_join",
          label: "Mention the new member",
          type: FieldType.BOOL,
          help: "Sends a real ping so the greeting reaches them.",
          visibleWhen: { key: "welcome_enabled", equals: true },
        },
      ],
    },
    {
      id: "onboarding",
      title: "Onboarding",
      fields: [
        {
          key: "onboarding_role_ids",
          label: "Roles given on join",
          type: FieldType.ROLE,
          multi: true,
          help: "Applied the moment someone joins, before they pick anything.",
        },
        {
          key: "lfg_forum_id",
          label: "LFG forum",
          type: FieldType.CHANNEL,
          help: "Game threads new members are matched into.",
          channelTypes: [ChannelType.GuildForum],
        },
      ],
    },
  ],
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
        {
          key: "action_threshold",
          label: "Actions before tripping",
          type: FieldType.NUMBER,
          help: "How many destructive actions in a row count as an attack.",
          min: 1,
          max: 25,
          default: 3,
        },
        {
          key: "action_window",
          label: "Counted within",
          type: FieldType.DURATION,
          help: "The window those actions have to happen in.",
          min: 5,
          max: 3600,
          default: 30,
        },
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
          key: "mention_limit",
          label: "Mentions per message",
          type: FieldType.NUMBER,
          help: "Above this counts as a mass mention.",
          min: 1,
          max: 50,
          default: 5,
          visibleWhen: { key: "enabled_modules.mass_mentions", equals: true },
        },
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

const suggestions: ModuleSchema = {
  id: "suggestions",
  name: "Suggestions",
  description: "A suggestion box with voting, staff review, and a decision log.",
  icon: "Lightbulb",
  category: "community",
  cog: "cogs/suggestion.py",
  configurable: true,
  sections: [
    {
      id: "channels",
      title: "Channels",
      fields: [
        {
          key: "suggestion_channel_id",
          label: "Suggestion channel",
          type: FieldType.CHANNEL,
          help: "Where the submit button lives and suggestions are posted.",
          required: true,
          channelTypes: [ChannelType.GuildText],
        },
        {
          key: "log_channel_id",
          label: "Decision log",
          type: FieldType.CHANNEL,
          help: "Approvals and rejections are recorded here.",
          channelTypes: [ChannelType.GuildText],
        },
      ],
    },
    {
      id: "behaviour",
      title: "Behaviour",
      fields: [
        {
          key: "allow_anonymous",
          label: "Allow anonymous suggestions",
          type: FieldType.BOOL,
          help: "The author is still recorded for staff, just not shown publicly.",
        },
        {
          key: "auto_thread",
          label: "Open a discussion thread",
          type: FieldType.BOOL,
          help: "Creates a thread on each suggestion for replies.",
          default: true,
        },
        {
          key: "cooldown",
          label: "Per-user cooldown",
          type: FieldType.DURATION,
          help: "How long someone waits between suggestions. 0 disables it.",
          min: 0,
          max: 604800,
          default: 3600,
        },
        {
          key: "required_role",
          label: "Restrict to role",
          type: FieldType.ROLE,
          help: "Leave empty to let everyone suggest.",
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
  suggestions,

  // Engagement
  listed("trivia", "Trivia", "Scheduled and on-demand trivia with leaderboards.", "Brain", "engagement", "cogs/trivia_main.py"),
  listed("qotd", "Question of the Day", "A daily prompt posted on a schedule.", "MessageCircleQuestion", "engagement", "cogs/qotd.py"),
  listed("nhie", "Never Have I Ever", "Voting rounds with community-submitted prompts.", "Hand", "engagement", "cogs/nhie.py"),
  listed("daily-quote", "Daily Quote", "A quote a day, from a curated database.", "Quote", "engagement", "cogs/daily_quote.py"),
  listed("game-poll", "Game Polls", "Polls for what the server plays next.", "Vote", "engagement", "cogs/game_poll.py"),
  listed("image-guesser", "Image Guesser", "Guess-the-image rounds with scoring.", "Image", "engagement", "cogs/image_guesser.py"),
  listed("mediavote", "Media Voting", "Bracket voting for films, shows and music.", "Clapperboard", "engagement", "cogs/mediavote.py"),

  // Economy
  listed("economy", "Economy & Levels", "Currency, shop, cards, XP and level rewards.", "Coins", "economy", "cogs/economy/"),

  // Community
  listed("ticketing", "Ticketing", "Support tickets with routing, transcripts and archives.", "TicketCheck", "community", "cogs/ticketing/"),
  listed("newcomer", "Newcomer Onboarding", "Introductions, reply points and starter roles.", "Sparkles", "community", "cogs/newcomer.py"),
  listed("lfg", "Looking For Group", "Per-game LFG threads and pings.", "Users", "community", "cogs/lfg.py"),
  listed("alerts-colors", "Role Alerts & Colours", "Self-assign alert pings and name colours.", "Palette", "community", "cogs/alerts_and_colors.py"),
  listed("role-management", "Role Management", "Reaction and button role assignment.", "UserCog", "community", "cogs/role_management.py"),
  listed("invite-roles", "Invite Roles", "Grant roles based on the invite used.", "Link2", "community", "cogs/inviterole.py"),
  listed("voice", "Voice Channels", "On-demand locked and shared voice rooms.", "Mic", "community", "cogs/vc_locked.py"),
  listed("lastfm", "Last.fm", "Now-playing cards and listening stats.", "Music", "community", "cogs/fm.py"),

  // Moderation
  listed("modtools", "Mod Tools", "Mod threads, warnings and follow-up tracking.", "Gavel", "moderation", "cogs/modtools.py"),
  listed("inactivity", "Inactivity", "Find and prune inactive members.", "UserMinus", "moderation", "cogs/inactivity.py"),
  listed("management", "Server Management", "Bulk role and channel operations.", "SlidersHorizontal", "moderation", "cogs/management.py"),

  // Esports
  listed("custom-match", "Custom Matches", "Team balancing, lobbies and results.", "Swords", "esports", "cogs/custommatch/"),
  listed("mapvote", "Map Voting", "Map picks and bans for scrims.", "Map", "esports", "cogs/mapvote.py"),
  listed("league-mapban", "League Map Ban", "Structured ban phases for league play.", "Ban", "esports", "cogs/league_mapban.py"),
  listed("pickems", "Pick'ems", "Predict match results and score them.", "Trophy", "esports", "cogs/pickems.py"),
  listed("esports", "Esports Feeds", "Schedules, results and stream alerts.", "Radio", "esports", "cogs/esports.py"),

  // Utility
  listed("utility", "Utility & Automations", "Sticky messages, reactions, media rules and the automation engine.", "Wand2", "utility", "cogs/utility/"),
  listed("reminders", "Reminders", "Personal and channel reminders.", "AlarmClock", "utility", "cogs/reminder.py"),
  listed("patchnotes", "Patch Notes", "Game patch notes pulled and posted.", "FileText", "utility", "cogs/patchnotes.py"),
  listed("emoji-stealer", "Emoji Stealer", "Copy emoji in from other servers.", "Smile", "utility", "cogs/emoji_stealer.py"),
  listed("bot-presence", "Bot Presence", "Status and activity shown by the bot.", "Activity", "utility", "cogs/bot_presence.py"),
  listed("sendmsg", "Send Message", "Post and edit bot messages and embeds.", "Send", "utility", "cogs/sendmsg.py"),
  listed("dm", "Direct Messages", "Templated DMs to members.", "Mail", "utility", "cogs/DM.py"),

  // Logging
  listed("audit-log", "Audit Log", "Message, member, role and channel logging.", "ScrollText", "logging", "cogs/audit_log.py"),
  listed("role-alerts", "Role Alerts", "Notify when specific roles change hands.", "BellRing", "logging", "cogs/rolealerts.py"),
  listed("tracker", "Activity Tracker", "Message and voice activity statistics.", "ChartLine", "logging", "cogs/tracker.py"),
];

const BY_ID = new Map(MODULES.map((module) => [module.id, module]));

export function getModule(id: string): ModuleSchema | undefined {
  return BY_ID.get(id);
}

export function configurableModules(): ModuleSchema[] {
  return MODULES.filter((module) => module.configurable);
}
