import { ChannelType } from "@/lib/discord/types";
import { FieldType, type Section } from "@/lib/schema/types";

/**
 * The flat, buffered settings of a game, grouped into the editor's tabs.
 *
 * Every field `key` is a real `games` column, so the editor round-trips a change
 * straight back through the `updateGame` action and the bot's `game_update`
 * command with no translation. The rank ladder, the weekly schedule and the
 * danger-zone actions aren't flat columns, so they live in their own panels on
 * the page rather than here — see the game editor page.
 *
 * This is the same `Section`/`Field`/`FieldType` vocabulary the module settings
 * forms use, so the editor reuses `FieldRow` and gets the dashboard's channel and
 * role pickers, switches and number steppers for free.
 */

const QUEUE_TYPE_CHOICES = [
  { value: "mmr", label: "MMR balanced", description: "Teams balanced by hidden rating." },
  { value: "captains", label: "Captain draft", description: "Two captains pick their teams." },
  { value: "random", label: "Random", description: "Teams shuffled at random." },
];

export const GAME_SECTIONS: Section[] = [
  {
    id: "rules",
    title: "Overview & rules",
    description: "What the game is, how teams form, and what a player needs to join.",
    fields: [
      { key: "name", label: "Name", type: FieldType.TEXT, required: true, min: 1, max: 100, help: "Shown everywhere this game appears." },
      {
        key: "enabled",
        label: "Enabled",
        type: FieldType.BOOL,
        default: true,
        help: "When off the game keeps all its data but drops out of setup lists, leaderboards and schedules, and its queue refuses new joiners.",
      },
      { key: "player_count", label: "Players per match", type: FieldType.NUMBER, min: 2, max: 40, default: 10, help: "The queue pops once this many are ready." },
      { key: "queue_type", label: "Team formation", type: FieldType.CHOICE, choices: QUEUE_TYPE_CHOICES, default: "mmr" },
      {
        key: "captain_selection",
        label: "Captains chosen by",
        type: FieldType.CHOICE,
        choices: [
          { value: "random", label: "Random" },
          { value: "highest_mmr", label: "Highest MMR" },
          { value: "admin", label: "Admin picks" },
        ],
        default: "random",
        visibleWhen: { key: "queue_type", equals: "captains" },
      },
      { key: "queue_role_required", label: "Require a queue role", type: FieldType.BOOL, default: true, help: "Only members with the role below can join. Unregistered members get the self-setup flow instead." },
      { key: "verified_role_id", label: "Queue role", type: FieldType.ROLE, help: "The role that gates the queue and is granted on setup.", visibleWhen: { key: "queue_role_required", equals: true } },
      { key: "role_required", label: "Require a rank on join", type: FieldType.BOOL, default: false, help: "Members must have a rank from the ladder to queue. Set the ladder up under Rank ladder." },
      { key: "ign_required", label: "Require an in-game name", type: FieldType.BOOL, default: false, help: "Members must set their IGN before they can queue." },
      { key: "vc_creation_enabled", label: "Create match voice channels", type: FieldType.BOOL, default: false, help: "Spin up per-team voice channels when a match starts." },
      { key: "dm_ready_up", label: "DM the ready-up prompt", type: FieldType.BOOL, default: false, help: "Also DM players when the ready check begins." },
      { key: "pc_enabled", label: "PC crossplay seeding", type: FieldType.BOOL, default: false, help: "Ask Console/PC at setup and bump PC players once when seeding." },
      { key: "pc_offset_tiers", label: "PC seed bump (tiers)", type: FieldType.NUMBER, min: 0, max: 10, default: 1, help: "How many rank tiers to add when first seeding a PC player.", visibleWhen: { key: "pc_enabled", equals: true } },
    ],
  },
  {
    id: "queue",
    title: "Queue & timers",
    description: "How long players get to ready up, and when the queue gives up on them.",
    fields: [
      { key: "ready_timer_seconds", label: "Ready-check timer (seconds)", type: FieldType.NUMBER, min: 10, max: 600, default: 60, help: "How long players have to ready up once the queue pops." },
      { key: "grace_period_minutes", label: "Auto-ready grace (minutes)", type: FieldType.NUMBER, min: 0, max: 120, default: 10, help: "A player who joined within this window is auto-readied. 0 turns it off." },
      { key: "not_ready_cooldown_minutes", label: "Not-Ready cooldown (minutes)", type: FieldType.NUMBER, min: 0, max: 240, default: 5, help: "How long after clicking Not Ready before a player can rejoin." },
      { key: "queue_timeout_minutes", label: "Queue timeout (minutes)", type: FieldType.NUMBER, min: 5, max: 1440, default: 180, help: "Idle players are dropped from the queue after this long." },
    ],
  },
  {
    id: "channels",
    title: "Channels",
    description: "Where the queue lives and where results and leaderboards go.",
    fields: [
      { key: "queue_channel_id", label: "Queue channel", type: FieldType.CHANNEL, channelTypes: [ChannelType.GuildText], help: "The queue message is posted and kept here." },
      { key: "game_channel_id", label: "Game / results channel", type: FieldType.CHANNEL, channelTypes: [ChannelType.GuildText], help: "Match results and announcements post here." },
      { key: "leaderboard_channel_id", label: "Leaderboard channel", type: FieldType.CHANNEL, channelTypes: [ChannelType.GuildText], help: "The auto-updating standings image posts here." },
      { key: "category_id", label: "Match category", type: FieldType.CHANNEL, channelTypes: [ChannelType.GuildCategory], help: "Per-match lobby and voice channels are created under this category. Falls back to the global one." },
      { key: "lf1_channel_id", label: "Looking-for-one channel", type: FieldType.CHANNEL, channelTypes: [ChannelType.GuildText], help: "Where 'one more needed' pings are posted." },
    ],
  },
  {
    id: "penalties",
    title: "Penalties",
    description: "How long players sit out for missing a ready check or declining one. 0 disables a tier.",
    fields: [
      { key: "penalty_1st_minutes", label: "Missed ready — 1st (minutes)", type: FieldType.NUMBER, min: 0, max: 10080, default: 0 },
      { key: "penalty_2nd_minutes", label: "Missed ready — 2nd (minutes)", type: FieldType.NUMBER, min: 0, max: 10080, default: 0 },
      { key: "penalty_3rd_minutes", label: "Missed ready — 3rd+ (minutes)", type: FieldType.NUMBER, min: 0, max: 10080, default: 0 },
      { key: "decline_1st_minutes", label: "Declined — 1st (minutes)", type: FieldType.NUMBER, min: 0, max: 10080, default: 0 },
      { key: "decline_2nd_minutes", label: "Declined — 2nd (minutes)", type: FieldType.NUMBER, min: 0, max: 10080, default: 0 },
      { key: "decline_3rd_minutes", label: "Declined — 3rd+ (minutes)", type: FieldType.NUMBER, min: 0, max: 10080, default: 0 },
      { key: "penalty_decay_days", label: "Offence decay (days)", type: FieldType.NUMBER, min: 1, max: 365, default: 30, help: "A clean stretch this long resets a player's offence count." },
    ],
  },
  {
    id: "funmodes",
    title: "Fun modes",
    description: "An optional second queue for a casual or off-format mode, with its own settings.",
    fields: [
      { key: "secondary_queue_enabled", label: "Enable a fun-mode queue", type: FieldType.BOOL, default: false },
      { key: "secondary_queue_name", label: "Mode name", type: FieldType.TEXT, max: 60, placeholder: "e.g. Deathmatch", visibleWhen: { key: "secondary_queue_enabled", equals: true } },
      { key: "secondary_queue_player_count", label: "Players per match", type: FieldType.NUMBER, min: 2, max: 40, visibleWhen: { key: "secondary_queue_enabled", equals: true } },
      { key: "secondary_queue_type", label: "Team formation", type: FieldType.CHOICE, choices: QUEUE_TYPE_CHOICES, visibleWhen: { key: "secondary_queue_enabled", equals: true } },
      { key: "secondary_queue_channel_id", label: "Queue channel", type: FieldType.CHANNEL, channelTypes: [ChannelType.GuildText], help: "Leave empty to use the main queue channel.", visibleWhen: { key: "secondary_queue_enabled", equals: true } },
      { key: "secondary_queue_match_limit", label: "Matches per window", type: FieldType.NUMBER, min: 1, max: 100, help: "Cap on matches per open window. Empty means unlimited.", visibleWhen: { key: "secondary_queue_enabled", equals: true } },
      { key: "secondary_banner_url", label: "Banner image URL", type: FieldType.TEXT, max: 500, visibleWhen: { key: "secondary_queue_enabled", equals: true } },
    ],
  },
  {
    id: "appearance",
    title: "Appearance",
    description: "Branding for the queue and match embeds.",
    fields: [
      { key: "banner_url", label: "Queue banner URL", type: FieldType.TEXT, max: 500, help: "Shown on the queue embed.", wide: true },
      { key: "short_name", label: "Channel name prefix", type: FieldType.TEXT, max: 20, help: "Used for lobby and voice channel names. Falls back to a slug of the game name." },
      { key: "verification_topic", label: "Verification topic", type: FieldType.TEXT, max: 200, help: "When self-setup isn't available, unregistered members open a verification ticket about this." },
      { key: "ready_loading_emoji", label: "Loading emoji", type: FieldType.TEXT, max: 60, help: "Shown while a player's ready state is pending. A custom emoji like <a:loading:123…>." },
      { key: "ready_done_emoji", label: "Ready emoji", type: FieldType.TEXT, max: 60, help: "Shown once a player has readied up." },
    ],
  },
];
