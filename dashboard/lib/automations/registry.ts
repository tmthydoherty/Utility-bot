/**
 * Everything an automation can watch for, check, and do — written for the web.
 *
 * This mirrors `cogs/automations/registry.py` entry for entry: the same keys,
 * the same fields, the same defaults. What it does *not* mirror is the wording,
 * and that difference is the whole point of this file.
 *
 * The Discord panel has to fit a label into a select option, so it settles for
 * "The message contains certain words". A web page has room for three things
 * at once, so every entry here carries:
 *
 *   label    — what it is, as a heading
 *   plain    — what it means, in the plainest words that are still true
 *   example  — one concrete case, because an example teaches faster than a
 *              definition and is what people actually read
 *   phrase   — how it reads inside the sentence at the top of the builder
 *
 * `phrase` is what lets the page say *"When someone sends a message, if the
 * message contains 'hello', reply to them"* instead of showing three boxes and
 * hoping. It is data rather than a function because a schema crosses the
 * server → client boundary and a function does not.
 *
 * **Rule for every string in this file:** if it needs a manual to understand,
 * it is the wrong string. Nothing here says trigger, condition, predicate,
 * node, dry run or boolean.
 */

import { FieldType, type Condition, type Field } from "@/lib/schema/types";

/**
 * One reading of an entry, for the sentence at the top of the builder.
 *
 * `{key}` is replaced with that field's value, rendered the way a person would
 * say it — a channel as #general, a role as @Members, a duration as "10
 * minutes". Entries are tried in order and the first whose `when` matches
 * wins, so a single action can read differently depending on how it is set up;
 * the last entry carries no `when` and is the fallback.
 */
export interface Phrase {
  when?: Condition;
  text: string;
}

interface Entry {
  key: string;
  label: string;
  plain: string;
  example?: string;
  /** lucide-react icon name, resolved through components/ui/icon.tsx. */
  icon: string;
  category: string;
  fields?: Field[];
  phrase: Phrase[] | string;
}

export interface TriggerSpec extends Entry {
  /** What the event hands on: "message", "channel", "member". */
  provides: readonly string[];
}

export interface ConditionSpec extends Entry {
  /** Context this check needs; it can only be offered on a matching trigger. */
  requires?: readonly string[];
}

export interface ActionSpec extends Entry {
  /** Discord permissions this needs, checked before it can be switched on. */
  perms?: readonly string[];
  requires?: readonly string[];
  /** Deletes, timeouts, disconnects — anything that takes something away. */
  destructive?: boolean;
}

// Shorthand so a field declaration reads as one line, the way it does in the
// Python registry. Nothing clever, just less noise per entry.
const f = (
  key: string,
  label: string,
  type: FieldType,
  extra: Partial<Field> = {},
): Field => ({ key, label, type, ...extra });

/**
 * Lets an action work on the bot's own message instead of the trigger's.
 *
 * Without this an automation can only ever touch the message that set it off,
 * so "announce something, then react to the announcement" is impossible — and
 * on a trigger like gaining a role there is no message at all.
 */
const whichMessage = (): Field =>
  f("target", "Which message", FieldType.CHOICE, {
    help: "Pick the one Vibey sent if an earlier step posted it.",
    choices: [
      { value: "trigger", label: "The message that set this off" },
      { value: "sent", label: "The message Vibey sent earlier in this automation" },
    ],
    default: "trigger",
  });

// Embed settings, shared by every action that sends something. Mirrors
// `embed_fields()` in utils/embed_builder.py.
const EMBED_COLOURS = [
  { value: "blurple", label: "Discord blurple" },
  { value: "green", label: "Green" },
  { value: "red", label: "Red" },
  { value: "orange", label: "Orange" },
  { value: "yellow", label: "Yellow" },
  { value: "blue", label: "Blue" },
  { value: "purple", label: "Purple" },
  { value: "pink", label: "Pink" },
  { value: "teal", label: "Teal" },
  { value: "white", label: "White" },
  { value: "black", label: "Black" },
  { value: "user", label: "Match their top role colour" },
];

/**
 * The embed settings, shared by every action that sends something. `prefix`
 * namespaces the keys (e.g. `reply_`) so a second embed — the button reply —
 * can live in the same config without clashing with the message's own. `gate`
 * hides the whole block behind another toggle, like "add a button".
 */
const embedFields = (
  { prefix = "", gate }: { prefix?: string; gate?: Condition } = {},
): Field[] => {
  const usingEmbed: Condition = gate
    ? { all: [gate, { key: `${prefix}use_embed`, equals: true }] }
    : { key: `${prefix}use_embed`, equals: true };
  const toggleVisible = gate ? { visibleWhen: gate } : {};

  return [
    f(`${prefix}use_embed`, "Send it as a fancy box (embed)", FieldType.BOOL, {
      help: "A coloured panel with a title, a picture and a footer, instead of plain text.",
      wide: true,
      ...toggleVisible,
    }),
    f(`${prefix}text_above`, "Text above the embed", FieldType.MULTILINE, {
      // This is the single most surprising rule in Discord, and someone
      // building "welcome them and ping them" hits it immediately and concludes
      // the bot is broken.
      help: "Pings don't work inside the embed. Put @mentions here instead.",
      visibleWhen: usingEmbed,
      wide: true,
    }),
    f(`${prefix}embed_title`, "Embed title", FieldType.TEXT, { visibleWhen: usingEmbed }),
    f(`${prefix}embed_colour`, "Embed Color", FieldType.CHOICE, {
      choices: EMBED_COLOURS,
      default: "blurple",
      visibleWhen: usingEmbed,
    }),
    f(`${prefix}embed_image`, "Big picture (image)", FieldType.TEXT, {
      help: "A link to an image or GIF, shown full width at the bottom.",
      placeholder: "https://…",
      visibleWhen: usingEmbed,
      wide: true,
    }),
    f(`${prefix}embed_thumbnail`, "Small picture (thumbnail)", FieldType.TEXT, {
      help: "Shown in the top corner. {user.avatar} uses their profile picture.",
      placeholder: "{user.avatar}",
      visibleWhen: usingEmbed,
      wide: true,
    }),
    f(`${prefix}embed_footer`, "Small print at the bottom (footer)", FieldType.TEXT, {
      visibleWhen: usingEmbed,
      wide: true,
    }),
    f(`${prefix}embed_author`, "Name along the top (author)", FieldType.TEXT, { visibleWhen: usingEmbed }),
  ];
};

// The button's reply is shown only once its "add a button" toggle is on.
const hasButton: Condition = { key: "add_button", equals: true };

/**
 * The "attach a button" settings for a send action. The button opens a private
 * (ephemeral) reply, which can be plain text or a full embed of its own.
 */
const buttonFields = (): Field[] => [
  f("add_button", "Add a button people can click", FieldType.BOOL, {
    help: "Puts a button under the message. Anyone who clicks it gets a private reply only they can see.",
    wide: true,
  }),
  f("button_label", "Button text", FieldType.TEXT, {
    placeholder: "Read the rules",
    help: "Leave blank to show just the emoji. If there's no emoji either, the button reads “Click me”.",
    visibleWhen: hasButton,
  }),
  f("button_colour", "Button colour", FieldType.CHOICE, {
    choices: [
      { value: "blurple", label: "Blurple" },
      { value: "green", label: "Green" },
      { value: "grey", label: "Grey" },
      { value: "red", label: "Red" },
    ],
    default: "blurple",
    visibleWhen: hasButton,
  }),
  f("button_emoji", "Button emoji", FieldType.EMOJI, {
    help: "Optional. Pick one from any server Vibey is in, or type a standard emoji.",
    emojiScope: "all",
    visibleWhen: hasButton,
  }),
  f("reply_content", "What the button shows", FieldType.MULTILINE, {
    help: "The private message the clicker sees. You can use {user.mention} and the rest — see the cheat sheet.",
    wide: true,
    visibleWhen: hasButton,
  }),
  ...embedFields({ prefix: "reply_", gate: hasButton }),
];

// ---------------------------------------------------------------- when this happens

export const TRIGGERS: Record<string, TriggerSpec> = Object.fromEntries(
  (
    [
      {
        key: "message_sent",
        label: "Someone sends a message",
        plain: "Runs every time anyone posts in the server.",
        example: "Use this with a requirement like “the message contains…” so it only reacts to some messages.",
        icon: "MessageSquare",
        category: "Messages",
        provides: ["message", "channel", "member"],
        phrase: "someone sends a message",
      },
      {
        key: "message_edited",
        label: "Someone edits a message",
        plain: "Runs when a message is changed after it was sent.",
        icon: "PencilLine",
        category: "Messages",
        provides: ["message", "channel", "member"],
        phrase: "someone edits a message",
      },
      {
        key: "message_deleted",
        label: "A message is deleted",
        plain: "Runs when a message disappears, whoever removed it.",
        icon: "Trash2",
        category: "Messages",
        provides: ["message", "channel", "member"],
        phrase: "a message is deleted",
      },
      {
        key: "reaction_added",
        label: "Someone reacts to a message",
        plain: "Runs when anyone adds an emoji reaction.",
        icon: "SmilePlus",
        category: "Messages",
        provides: ["message", "channel", "member"],
        phrase: "someone reacts to a message",
      },

      {
        key: "member_joined",
        label: "Someone joins the server",
        plain: "Runs the moment a new person arrives.",
        example: "The usual starting point for a welcome message.",
        icon: "UserPlus",
        category: "People",
        provides: ["member"],
        phrase: "someone joins the server",
      },
      {
        key: "member_left",
        label: "Someone leaves the server",
        plain: "Runs when a person leaves, is kicked, or is banned.",
        icon: "UserMinus",
        category: "People",
        provides: ["member"],
        phrase: "someone leaves the server",
      },
      {
        key: "member_boosted",
        label: "Someone boosts the server",
        plain: "Runs when a person starts boosting.",
        icon: "Sparkles",
        category: "People",
        provides: ["member"],
        phrase: "someone boosts the server",
      },
      {
        key: "nickname_changed",
        label: "Someone changes their nickname",
        plain: "Runs when a person renames themselves in this server.",
        icon: "Signature",
        category: "People",
        provides: ["member"],
        phrase: "someone changes their nickname",
      },
      {
        key: "role_added",
        label: "Someone gets a role",
        plain: "Runs when a person is given a role.",
        icon: "ShieldPlus",
        category: "People",
        provides: ["member"],
        fields: [
          f("roles", "Which roles", FieldType.ROLE, {
            help: "Leave empty to run whenever anyone gets any role at all.",
            multi: true,
          }),
        ],
        phrase: [
          { when: { key: "roles", isSet: true }, text: "someone gets the {roles} role" },
          { text: "someone gets any role" },
        ],
      },
      {
        key: "role_removed",
        label: "Someone loses a role",
        plain: "Runs when a role is taken away from a person.",
        icon: "ShieldMinus",
        category: "People",
        provides: ["member"],
        fields: [
          f("roles", "Which roles", FieldType.ROLE, {
            help: "Leave empty to run whenever anyone loses any role at all.",
            multi: true,
          }),
        ],
        phrase: [
          { when: { key: "roles", isSet: true }, text: "someone loses the {roles} role" },
          { text: "someone loses any role" },
        ],
      },

      {
        key: "voice_joined",
        label: "Someone joins a voice channel",
        plain: "Runs when a person connects to voice chat.",
        icon: "Mic",
        category: "Voice",
        provides: ["member", "channel"],
        phrase: "someone joins a voice channel",
      },
      {
        key: "voice_left",
        label: "Someone leaves a voice channel",
        plain: "Runs when a person disconnects from voice chat.",
        icon: "MicOff",
        category: "Voice",
        provides: ["member", "channel"],
        phrase: "someone leaves a voice channel",
      },
      {
        key: "vc_lobby_created",
        label: "A voice lobby opens",
        plain: "Runs when someone opens one of the temporary voice lobbies.",
        icon: "DoorOpen",
        category: "Voice",
        provides: ["member", "channel"],
        phrase: "a voice lobby opens",
      },
      {
        key: "vc_lobby_closed",
        label: "A voice lobby closes",
        plain: "Runs when a temporary voice lobby is cleaned up.",
        icon: "DoorClosed",
        category: "Voice",
        provides: ["member"],
        phrase: "a voice lobby closes",
      },

      {
        key: "member_level_up",
        label: "Someone levels up",
        plain: "Runs when a person reaches a new level.",
        icon: "TrendingUp",
        category: "Levels & tickets",
        provides: ["member"],
        phrase: "someone levels up",
      },
      {
        key: "ticket_opened",
        label: "Someone opens a ticket",
        plain: "Runs when a new support ticket is created.",
        icon: "Ticket",
        category: "Levels & tickets",
        provides: ["member", "channel"],
        phrase: "someone opens a ticket",
      },
      {
        key: "ticket_closed",
        label: "A ticket is closed",
        plain: "Runs when a ticket is closed or archived.",
        icon: "TicketX",
        category: "Levels & tickets",
        provides: ["member", "channel"],
        phrase: "a ticket is closed",
      },
      {
        key: "newcomer_intro_posted",
        label: "A newcomer posts their intro",
        plain: "Runs when someone completes their introduction.",
        icon: "MessageSquareHeart",
        category: "Levels & tickets",
        provides: ["member"],
        phrase: "a newcomer posts their intro",
      },
      {
        key: "newcomer_graduated",
        label: "A newcomer becomes a member",
        plain: "Runs when someone graduates out of the newcomer role.",
        icon: "GraduationCap",
        category: "Levels & tickets",
        provides: ["member"],
        phrase: "a newcomer becomes a full member",
      },

      {
        key: "schedule",
        label: "At a set time",
        plain: "Runs on a repeating clock instead of reacting to anything.",
        example: "Every day at 09:00, post the question of the day.",
        icon: "Clock",
        category: "On a timer",
        provides: [],
        fields: [
          f("frequency", "How often", FieldType.CHOICE, {
            choices: [
              { value: "daily", label: "Every day" },
              { value: "weekly", label: "Once a week" },
              { value: "interval", label: "Every few hours" },
            ],
            default: "daily",
          }),
          f("time_utc", "What time", FieldType.TEXT, {
            help: "24-hour clock, in UTC. 18:30 is half past six in the evening.",
            placeholder: "18:30",
            visibleWhen: { key: "frequency", notEquals: "interval" },
          }),
          f("weekday", "Which day", FieldType.CHOICE, {
            choices: [
              "Monday",
              "Tuesday",
              "Wednesday",
              "Thursday",
              "Friday",
              "Saturday",
              "Sunday",
            ].map((day, index) => ({ value: String(index), label: day })),
            visibleWhen: { key: "frequency", equals: "weekly" },
          }),
          f("interval_hours", "How many hours apart", FieldType.NUMBER, {
            min: 1,
            max: 168,
            visibleWhen: { key: "frequency", equals: "interval" },
          }),
        ],
        phrase: [
          {
            when: { key: "frequency", equals: "interval" },
            text: "every {interval_hours} hours",
          },
          {
            when: { key: "frequency", equals: "weekly" },
            text: "every {weekday} at {time_utc} UTC",
          },
          { text: "every day at {time_utc} UTC" },
        ],
      },
      {
        key: "manual",
        label: "Only when I press the button",
        plain: "Never runs on its own. You set it off yourself, or another automation calls it.",
        example: "Handy for a block of steps several automations share.",
        icon: "MousePointerClick",
        category: "On a timer",
        provides: [],
        phrase: "you press Test",
      },
    ] satisfies TriggerSpec[]
  ).map((spec) => [spec.key, spec]),
);

// ---------------------------------------------------------------------- only if

const matchCase = (): Field =>
  f("case_sensitive", "Capital letters have to match too", FieldType.BOOL);

export const CONDITIONS: Record<string, ConditionSpec> = Object.fromEntries(
  (
    [
      // --- the message
      {
        key: "content_contains",
        label: "The message contains certain words",
        plain: "Looks anywhere in the message for any of the words you list.",
        example: "“hello, hi, hey” matches “oh hi there”.",
        icon: "Search",
        category: "The message",
        requires: ["message"],
        fields: [
          f("text", "Words or phrases to look for", FieldType.TEXT, {
            help: "Separate several with commas. Any one of them counts.",
            placeholder: "hello, hi, hey",
            required: true,
            wide: true,
          }),
          matchCase(),
        ],
        phrase: "the message includes one or more of {text}",
      },
      {
        key: "content_starts_with",
        label: "The message starts with something",
        plain: "Only looks at the very beginning of the message.",
        icon: "CornerDownRight",
        category: "The message",
        requires: ["message"],
        fields: [
          f("text", "Words or phrases", FieldType.TEXT, {
            help: "Separate several with commas. Any one of them counts.",
            placeholder: "!, ?, hey",
            required: true,
            wide: true,
          }),
          matchCase(),
        ],
        phrase: "the message starts with {text}",
      },
      {
        key: "content_ends_with",
        label: "The message ends with something",
        plain: "Only looks at the very end of the message.",
        icon: "CornerRightDown",
        category: "The message",
        requires: ["message"],
        fields: [
          f("text", "Words or phrases", FieldType.TEXT, {
            help: "Separate several with commas. Any one of them counts.",
            placeholder: "?, please",
            required: true,
            wide: true,
          }),
          matchCase(),
        ],
        phrase: "the message ends with {text}",
      },
      {
        key: "content_exactly",
        label: "The message is exactly something",
        plain: "The whole message and nothing else.",
        example: "“gg wp” will not match a message that just says “gg”.",
        icon: "Equal",
        category: "The message",
        requires: ["message"],
        fields: [
          f("text", "The exact wording", FieldType.TEXT, {
            help: "Separate several with commas. Any one of them counts.",
            placeholder: "gg, gg wp",
            required: true,
            wide: true,
          }),
          matchCase(),
        ],
        phrase: "the message is exactly {text}",
      },
      {
        key: "is_reply",
        label: "The message is a reply",
        plain: "Someone used Discord's reply button. Forwarded messages don't count.",
        icon: "Reply",
        category: "The message",
        requires: ["message"],
        phrase: "the message is a reply",
      },
      {
        key: "has_image",
        label: "The message has a picture or video",
        plain: "Photos, screenshots and clips. Other kinds of file don't count.",
        icon: "Image",
        category: "The message",
        requires: ["message"],
        phrase: "the message has a picture or video",
      },
      {
        key: "has_attachment",
        label: "The message has a file attached",
        plain: "Any uploaded file at all, pictures included.",
        icon: "Paperclip",
        category: "The message",
        requires: ["message"],
        phrase: "the message has a file attached",
      },
      {
        key: "has_link",
        label: "The message has a link",
        plain: "Any web address.",
        icon: "Link",
        category: "The message",
        requires: ["message"],
        phrase: "the message has a link",
      },
      {
        key: "has_invite",
        label: "The message has a Discord invite",
        plain: "An invite link to another server.",
        icon: "ExternalLink",
        category: "The message",
        requires: ["message"],
        phrase: "the message has a Discord invite",
      },
      {
        key: "has_emoji",
        label: "The message has an emoji",
        plain: "Either standard or custom server emoji.",
        icon: "Smile",
        category: "The message",
        requires: ["message"],
        phrase: "the message has an emoji",
      },
      {
        key: "mention_count",
        label: "The message pings people",
        plain: "Counts both people and roles that were pinged.",
        icon: "AtSign",
        category: "The message",
        requires: ["message"],
        fields: [
          f("min", "At least this many pings", FieldType.NUMBER, { min: 1, default: 1 }),
        ],
        phrase: "the message pings at least {min} people",
      },
      {
        key: "caps_ratio",
        label: "The message is mostly CAPITALS",
        plain: "Catches shouting.",
        icon: "CaseUpper",
        category: "The message",
        requires: ["message"],
        fields: [
          f("percent", "Capitals must be at least this much", FieldType.NUMBER, {
            help: "Out of all the letters in the message. 70 means seven letters in ten.",
            min: 1,
            max: 100,
            default: 70,
          }),
          f("min_length", "Ignore messages shorter than", FieldType.NUMBER, {
            help: "Number of letters. Stops short words like “OK” counting as shouting.",
            min: 1,
            max: 200,
            default: 8,
          }),
        ],
        phrase: "the message is at least {percent}% capitals",
      },
      {
        key: "content_length",
        label: "The message is a certain length",
        plain: "Counts characters, spaces included.",
        icon: "Ruler",
        category: "The message",
        requires: ["message"],
        fields: [
          f("min", "At least this many characters", FieldType.NUMBER, { min: 0 }),
          f("max", "At most this many characters", FieldType.NUMBER, { min: 0 }),
        ],
        phrase: "the message is between {min} and {max} characters",
      },
      {
        key: "content_regex",
        label: "The message matches a search pattern",
        plain: "For advanced use. Matches wildcards, and lets you reuse the matched text later as {match.1}.",
        icon: "Regex",
        category: "The message",
        requires: ["message"],
        fields: [
          f("pattern", "Pattern", FieldType.TEXT, {
            help: "A regular expression.",
            placeholder: "\\b(gg|good game)\\b",
            required: true,
            wide: true,
          }),
        ],
        phrase: "the message matches {pattern}",
      },

      // --- who did it
      {
        key: "role_has",
        label: "They have (or don't have) a role",
        plain: "Checks the roles of whoever set this off.",
        example: "The usual way to let staff skip a rule.",
        icon: "Shield",
        category: "Who did it",
        fields: [
          f("roles", "Which roles", FieldType.ROLE, { multi: true, required: true, wide: true }),
          f("match", "They must have", FieldType.CHOICE, {
            choices: [
              { value: "any", label: "At least one of these roles" },
              { value: "all", label: "Every one of these roles" },
              { value: "none", label: "None of these roles" },
            ],
            default: "any",
          }),
        ],
        phrase: [
          { when: { key: "match", equals: "none" }, text: "they don't have any of these roles: {roles}" },
          { when: { key: "match", equals: "all" }, text: "they have all of these roles: {roles}" },
          { text: "they have one of these roles: {roles}" },
        ],
      },
      {
        key: "user_is",
        label: "It's a specific person",
        plain: "Only runs for the people you name.",
        icon: "User",
        category: "Who did it",
        fields: [
          f("users", "Which people", FieldType.USER, { multi: true, required: true, wide: true }),
        ],
        phrase: "it's {users}",
      },
      {
        key: "is_bot",
        label: "They're a bot",
        plain: "Whether another bot set this off rather than a person.",
        icon: "Bot",
        category: "Who did it",
        phrase: "they're a bot",
      },
      {
        key: "is_boosting",
        label: "They're boosting the server",
        plain: "Only runs for people currently boosting.",
        icon: "Sparkles",
        category: "Who did it",
        phrase: "they're boosting the server",
      },
      {
        key: "has_permission",
        label: "They have a permission",
        plain: "Checks one of Discord's own permissions.",
        example: "Set this to “Manage Messages” and switch it to “doesn't match” so moderators are left alone.",
        icon: "KeyRound",
        category: "Who did it",
        fields: [
          f("permission", "Which permission", FieldType.CHOICE, {
            required: true,
            wide: true,
            choices: [
              { value: "administrator", label: "Administrator" },
              { value: "manage_guild", label: "Manage Server" },
              { value: "manage_messages", label: "Manage Messages" },
              { value: "manage_roles", label: "Manage Roles" },
              { value: "manage_channels", label: "Manage Channels" },
              { value: "moderate_members", label: "Time Out Members" },
              { value: "kick_members", label: "Kick Members" },
              { value: "ban_members", label: "Ban Members" },
              { value: "mention_everyone", label: "Mention Everyone" },
            ],
          }),
        ],
        phrase: "they have {permission}",
      },
      {
        key: "account_age",
        label: "Their Discord account is a certain age",
        plain: "How long ago they made their Discord account — not when they joined here.",
        example: "Brand-new accounts are the usual sign of a spam wave.",
        icon: "CalendarClock",
        category: "Who did it",
        fields: [
          f("min_days", "At least this many days old", FieldType.NUMBER, { min: 0 }),
          f("max_days", "At most this many days old", FieldType.NUMBER, { min: 0 }),
        ],
        phrase: "their account is {min_days}–{max_days} days old",
      },
      {
        key: "member_age",
        label: "They've been here a certain time",
        plain: "How long ago they joined this server.",
        icon: "CalendarDays",
        category: "Who did it",
        fields: [
          f("min_days", "Here at least this many days", FieldType.NUMBER, { min: 0 }),
          f("max_days", "Here at most this many days", FieldType.NUMBER, { min: 0 }),
        ],
        phrase: "they've been here {min_days}–{max_days} days",
      },
      {
        key: "level_at_least",
        label: "They're at least a certain level",
        plain: "Uses their level from the levelling system.",
        icon: "TrendingUp",
        category: "Who did it",
        fields: [f("level", "Level", FieldType.NUMBER, { min: 0 })],
        phrase: "they're level {level} or higher",
      },

      // --- where it happened
      {
        key: "channel_is",
        label: "It's in certain channels",
        plain: "Picking a category covers every channel inside it. Threads and forum posts are left out unless you switch them on.",
        example: "Pointing this at #clips won't fire in every thread under it — turn on threads for that.",
        icon: "Hash",
        category: "Where it happened",
        fields: [
          f("channels", "Which channels", FieldType.CHANNEL, {
            multi: true,
            required: true,
            wide: true,
          }),
          f("include_threads", "Also match threads and forum posts inside them", FieldType.BOOL, {
            help: "Off by default, so a rule pointed at a channel won't fire in every thread under it.",
            default: false,
            wide: true,
          }),
        ],
        phrase: [
          {
            when: { key: "include_threads", equals: true },
            text: "it's in {channels} or any thread inside them",
          },
          { text: "it's in {channels}" },
        ],
      },
      {
        key: "in_thread",
        label: "It's in a thread",
        plain: "Switch this to “doesn't match” for things in the main channel instead.",
        icon: "MessagesSquare",
        category: "Where it happened",
        phrase: "it's in a thread",
      },

      // --- other
      {
        key: "counter_compare",
        label: "A tally has reached a number",
        plain: "Tallies count things over time, like warnings. Add to one with the “Add to a tally” step.",
        example: "Three strikes in a day, then a timeout.",
        icon: "Hash",
        category: "Anything else",
        fields: [
          f("key", "Tally name", FieldType.TEXT, {
            help: "Must match the name you used in the “Add to a tally” step.",
            placeholder: "strikes",
            required: true,
          }),
          f("scope", "Counted separately for", FieldType.CHOICE, {
            choices: [
              { value: "user", label: "Each person" },
              { value: "channel", label: "Each channel" },
              { value: "guild", label: "The whole server" },
            ],
            default: "user",
          }),
          f("op", "The tally must be", FieldType.CHOICE, {
            choices: [
              { value: "gte", label: "This number or higher" },
              { value: "lte", label: "This number or lower" },
              { value: "eq", label: "Exactly this number" },
              { value: "gt", label: "Higher than this" },
              { value: "lt", label: "Lower than this" },
            ],
            default: "gte",
          }),
          f("value", "Number", FieldType.NUMBER, { default: 1 }),
        ],
        phrase: "their {key} tally has reached {value}",
      },
      {
        key: "variable_compare",
        label: "A note from earlier says something",
        plain: "Checks a value stored earlier in this same run by the “Jot something down” step.",
        icon: "StickyNote",
        category: "Anything else",
        fields: [
          f("key", "Note name", FieldType.TEXT, { required: true }),
          f("op", "The note must", FieldType.CHOICE, {
            choices: [
              { value: "eq", label: "Be exactly this" },
              { value: "neq", label: "Be anything but this" },
              { value: "contains", label: "Contain this" },
              { value: "set", label: "Have anything at all in it" },
            ],
            default: "eq",
          }),
          f("value", "Value", FieldType.TEXT, {
            visibleWhen: { key: "op", notEquals: "set" },
          }),
        ],
        phrase: "the note {key} says {value}",
      },
      {
        key: "time_window",
        label: "It's between two times of day",
        plain: "Times are in UTC, and the window can run past midnight.",
        icon: "Clock",
        category: "Anything else",
        fields: [
          f("start", "From", FieldType.TEXT, { placeholder: "09:00" }),
          f("end", "Until", FieldType.TEXT, { placeholder: "17:00" }),
        ],
        phrase: "it's between {start} and {end} UTC",
      },
      {
        key: "day_of_week",
        label: "It's a certain day of the week",
        plain: "Days are counted in UTC.",
        icon: "CalendarRange",
        category: "Anything else",
        fields: [
          f("days", "Which days", FieldType.CHOICE, {
            multi: true,
            required: true,
            wide: true,
            choices: [
              "Monday",
              "Tuesday",
              "Wednesday",
              "Thursday",
              "Friday",
              "Saturday",
              "Sunday",
            ].map((day, index) => ({ value: String(index), label: day })),
          }),
        ],
        phrase: "it's a {days}",
      },
      {
        key: "chance",
        label: "Only some of the time",
        plain: "Rolls a dice, so this only happens now and then.",
        example: "Set it to 10% and the bot replies to roughly one message in ten.",
        icon: "Dices",
        category: "Anything else",
        fields: [
          f("percent", "Run this much of the time", FieldType.NUMBER, {
            min: 1,
            max: 100,
            default: 50,
          }),
        ],
        phrase: "the dice comes up {percent}% of the time",
      },
    ] satisfies ConditionSpec[]
  ).map((spec) => [spec.key, spec]),
);

// ------------------------------------------------------------------ then do this

const sendingToChannel: Condition = { key: "destination", equals: "channel" };

export const ACTIONS: Record<string, ActionSpec> = Object.fromEntries(
  (
    [
      // --- messages
      {
        key: "send_message",
        label: "Send a message",
        plain: "Post in a channel, reply to them, or send them a private DM.",
        icon: "Send",
        category: "Messages",
        perms: ["send_messages"],
        fields: [
          f("destination", "Where should it go", FieldType.CHOICE, {
            choices: [
              { value: "channel", label: "Into a channel" },
              { value: "reply", label: "As a reply to their message" },
              { value: "dm", label: "As a private DM to them" },
            ],
            default: "channel",
          }),
          f("channel", "Which channel", FieldType.CHANNEL, {
            help: "Leave blank to post wherever the thing happened.",
            visibleWhen: sendingToChannel,
          }),
          f("content", "What should it say", FieldType.MULTILINE, {
            help: "You can use {user.mention}, {guild.name} and more — see the cheat sheet.",
            wide: true,
          }),
          f("ping", "Ping them in the reply", FieldType.BOOL, {
            visibleWhen: { key: "destination", equals: "reply" },
          }),
          ...embedFields(),
          ...buttonFields(),
        ],
        phrase: [
          { when: { key: "destination", equals: "dm" }, text: "DM them {content}" },
          { when: { key: "destination", equals: "reply" }, text: "reply {content}" },
          { when: { key: "channel", isSet: true }, text: "post {content} in {channel}" },
          { text: "post {content}" },
        ],
      },
      {
        key: "delete_message",
        label: "Delete their message",
        plain: "Removes the message that set this off.",
        icon: "Trash2",
        category: "Messages",
        perms: ["manage_messages"],
        requires: ["message"],
        destructive: true,
        phrase: "delete their message",
      },
      {
        key: "add_reaction",
        label: "React to a message",
        plain: "Adds as many reactions as you like, in the order you list them.",
        icon: "SmilePlus",
        category: "Messages",
        perms: ["add_reactions"],
        fields: [
          f("emoji", "Which emoji", FieldType.EMOJI, {
            help: "Put several in, separated by spaces. They're added left to right.",
            multi: true,
            wide: true,
          }),
          whichMessage(),
        ],
        phrase: "react with {emoji}",
      },
      {
        key: "remove_reaction",
        label: "Take back my reaction",
        plain: "Removes a reaction Vibey added earlier.",
        icon: "Eraser",
        category: "Messages",
        requires: ["message"],
        fields: [f("emoji", "Which emoji", FieldType.EMOJI, { multi: true, wide: true })],
        phrase: "take back the {emoji} reaction",
      },
      {
        key: "clear_reactions",
        label: "Clear everyone's reactions",
        plain: "Wipes all reactions off the message.",
        icon: "Eraser",
        category: "Messages",
        perms: ["manage_messages"],
        requires: ["message"],
        destructive: true,
        phrase: "clear every reaction",
      },
      {
        key: "create_thread",
        label: "Start a thread on a message",
        plain: "Opens a thread underneath for people to talk in.",
        icon: "MessagesSquare",
        category: "Messages",
        perms: ["create_public_threads"],
        fields: [
          f("name", "Thread name", FieldType.TEXT, {
            placeholder: "{user} — discussion",
            wide: true,
          }),
          f("message", "First message in the thread", FieldType.MULTILINE, {
            help: "Leave blank to post nothing.",
            wide: true,
          }),
          f("archive_minutes", "Close it after no activity for", FieldType.CHOICE, {
            choices: [
              { value: "60", label: "1 hour" },
              { value: "1440", label: "1 day" },
              { value: "4320", label: "3 days" },
              { value: "10080", label: "1 week" },
            ],
            default: "1440",
          }),
          whichMessage(),
        ],
        phrase: "start a thread called {name}",
      },
      {
        key: "pin_message",
        label: "Pin a message",
        plain: "Adds it to the channel's pins.",
        icon: "Pin",
        category: "Messages",
        perms: ["manage_messages"],
        fields: [whichMessage()],
        phrase: "pin the message",
      },
      {
        key: "unpin_message",
        label: "Unpin their message",
        plain: "Takes it back out of the pins.",
        icon: "PinOff",
        category: "Messages",
        perms: ["manage_messages"],
        requires: ["message"],
        phrase: "unpin their message",
      },
      {
        key: "publish_message",
        label: "Publish their message",
        plain: "Sends it out to servers following this announcement channel.",
        icon: "Megaphone",
        category: "Messages",
        perms: ["manage_messages"],
        requires: ["message"],
        phrase: "publish their message",
      },

      // --- roles and names
      {
        key: "add_role",
        label: "Give them a role",
        plain: "Hands out a role, and can take it back again later on its own.",
        icon: "ShieldPlus",
        category: "Roles & names",
        perms: ["manage_roles"],
        fields: [
          f("role", "Which role", FieldType.ROLE, { required: true, wide: true }),
          f("duration", "Take it back after", FieldType.DURATION, {
            help: "Leave at zero to keep it forever. This still works if the bot restarts.",
            wide: true,
          }),
        ],
        phrase: [
          { when: { key: "duration", isSet: true }, text: "give them {role} for {duration}" },
          { text: "give them {role}" },
        ],
      },
      {
        key: "remove_role",
        label: "Take a role away",
        plain: "Removes the role if they have it.",
        icon: "ShieldMinus",
        category: "Roles & names",
        perms: ["manage_roles"],
        fields: [f("role", "Which role", FieldType.ROLE, { required: true, wide: true })],
        phrase: "take {role} away",
      },
      {
        key: "toggle_role",
        label: "Give or take a role",
        plain: "Gives it if they don't have it, takes it if they do.",
        example: "The usual way to build a self-serve “ping me for events” role.",
        icon: "ToggleLeft",
        category: "Roles & names",
        perms: ["manage_roles"],
        fields: [f("role", "Which role", FieldType.ROLE, { required: true, wide: true })],
        phrase: "give or take {role}",
      },
      {
        key: "set_nickname",
        label: "Change their nickname",
        plain: "Leave it blank to reset their nickname back to their username.",
        icon: "Signature",
        category: "Roles & names",
        perms: ["manage_nicknames"],
        fields: [
          f("nickname", "New nickname", FieldType.TEXT, {
            placeholder: "{user.name} 🌟",
            wide: true,
          }),
        ],
        phrase: "rename them to {nickname}",
      },

      // --- moderation
      {
        key: "timeout_member",
        label: "Time them out",
        plain: "Stops them talking for a while. Kicking and banning are handled by the security tools, not here.",
        icon: "TimerOff",
        category: "Moderation",
        perms: ["moderate_members"],
        destructive: true,
        fields: [
          f("duration", "For how long", FieldType.DURATION, {
            help: "Anything from a minute to 28 days.",
            default: 600,
            wide: true,
          }),
          f("reason", "Reason", FieldType.TEXT, {
            help: "Shows up in Discord's own audit log.",
            wide: true,
          }),
        ],
        phrase: "time them out for {duration}",
      },
      {
        key: "remove_timeout",
        label: "End their timeout early",
        plain: "Lets them talk again straight away.",
        icon: "Timer",
        category: "Moderation",
        perms: ["moderate_members"],
        phrase: "end their timeout",
      },
      {
        key: "set_slowmode",
        label: "Change a channel's slowmode",
        plain: "How long people have to wait between messages. Set it to 0 to turn slowmode off.",
        icon: "Gauge",
        category: "Moderation",
        perms: ["manage_channels"],
        fields: [
          f("channel", "Which channel", FieldType.CHANNEL, {
            help: "Leave blank to use the channel this happened in.",
            wide: true,
          }),
          f("seconds", "Seconds between messages", FieldType.NUMBER, {
            min: 0,
            max: 21600,
          }),
        ],
        phrase: "set slowmode in {channel} to {seconds} seconds",
      },
      {
        key: "voice_disconnect",
        label: "Disconnect them from voice",
        plain: "Kicks them out of whichever voice channel they're in.",
        icon: "PhoneOff",
        category: "Moderation",
        perms: ["move_members"],
        destructive: true,
        phrase: "disconnect them from voice",
      },
      {
        key: "voice_move",
        label: "Move them to a voice channel",
        plain: "Only works if they're already in voice somewhere.",
        icon: "MoveRight",
        category: "Moderation",
        perms: ["move_members"],
        fields: [
          f("channel", "Which voice channel", FieldType.CHANNEL, {
            required: true,
            wide: true,
          }),
        ],
        phrase: "move them to {channel}",
      },

      // --- remembering things
      {
        key: "counter_change",
        label: "Add to a tally",
        plain: "Tallies count things over time — warnings, wins, anything. Check one later with “A tally has reached a number”.",
        example: "Add 1 to “strikes”, forget it after a day, and time them out on the third.",
        icon: "Plus",
        category: "Remembering things",
        fields: [
          f("key", "Tally name", FieldType.TEXT, {
            help: "Make one up. Use the same name to check it later.",
            placeholder: "strikes",
          }),
          f("scope", "Counted separately for", FieldType.CHOICE, {
            choices: [
              { value: "user", label: "Each person" },
              { value: "channel", label: "Each channel" },
              { value: "guild", label: "The whole server" },
            ],
            default: "user",
          }),
          f("amount", "Add this much", FieldType.NUMBER, {
            help: "Use a negative number to take away instead.",
            default: 1,
          }),
          f("window", "Forget it again after", FieldType.DURATION, {
            help: "Leave at zero to remember forever. Set 1 day for “3 strikes in a day”.",
          }),
        ],
        phrase: "add {amount} to their {key} tally",
      },
      {
        key: "counter_reset",
        label: "Reset a tally to zero",
        plain: "Wipes the count and starts again.",
        icon: "RotateCcw",
        category: "Remembering things",
        fields: [
          f("key", "Tally name", FieldType.TEXT, { placeholder: "strikes" }),
          f("scope", "Counted separately for", FieldType.CHOICE, {
            choices: [
              { value: "user", label: "Each person" },
              { value: "channel", label: "Each channel" },
              { value: "guild", label: "The whole server" },
            ],
            default: "user",
          }),
        ],
        phrase: "reset their {key} tally",
      },
      {
        key: "set_variable",
        label: "Jot something down for later steps",
        plain: "Stores a value you can use further down as {var.name}. Forgotten the moment this automation finishes.",
        icon: "StickyNote",
        category: "Remembering things",
        fields: [
          f("key", "Note name", FieldType.TEXT, { placeholder: "greeting" }),
          f("value", "What to jot down", FieldType.TEXT, { wide: true }),
        ],
        phrase: "jot down {value} as {key}",
      },
      {
        key: "award_points",
        label: "Give them Points",
        plain: "Adds to their balance in the economy system.",
        icon: "Coins",
        category: "Remembering things",
        fields: [
          f("amount", "How many Points", FieldType.NUMBER, { default: 10 }),
          f("reason", "Reason", FieldType.TEXT, { wide: true }),
        ],
        phrase: "give them {amount} Points",
      },

      // --- controlling the automation
      {
        key: "log_line",
        label: "Write to a log channel",
        plain: "Posts a note so staff can see this ran.",
        icon: "ScrollText",
        category: "Controls",
        perms: ["send_messages"],
        fields: [
          f("channel", "Which channel", FieldType.CHANNEL, { wide: true }),
          f("content", "What to write", FieldType.MULTILINE, { wide: true }),
          ...embedFields(),
        ],
        phrase: "write {content} to {channel}",
      },
      {
        key: "wait",
        label: "Wait a while",
        plain: "Pauses before the next step. This still works if the bot restarts partway through.",
        icon: "Hourglass",
        category: "Controls",
        fields: [f("duration", "Wait for", FieldType.DURATION, { default: 60, wide: true })],
        phrase: "wait {duration}",
      },
      {
        key: "stop",
        label: "Stop here",
        plain: "Skips everything after this point.",
        icon: "OctagonX",
        category: "Controls",
        phrase: "stop",
      },
      {
        key: "run_automation",
        label: "Run another automation",
        plain: "Hands over to one of your other automations.",
        icon: "GitBranch",
        category: "Controls",
        fields: [f("automation", "Which one", FieldType.CHOICE, { wide: true })],
        phrase: "run another automation",
      },
    ] satisfies ActionSpec[]
  ).map((spec) => [spec.key, spec]),
);

// ------------------------------------------------------------------- lookups

/** Group a catalogue by category, keeping declaration order. */
export function byCategory<T extends Entry>(specs: Record<string, T>): [string, T[]][] {
  const grouped = new Map<string, T[]>();
  for (const spec of Object.values(specs)) {
    const existing = grouped.get(spec.category);
    if (existing) existing.push(spec);
    else grouped.set(spec.category, [spec]);
  }
  return [...grouped.entries()];
}

/**
 * The checks that make sense on a given trigger.
 *
 * "The message contains…" on a trigger with no message is not a stricter
 * automation, it is one that never runs — so it is not offered at all rather
 * than offered and then quietly failing.
 */
export function conditionsFor(triggerType: string): ConditionSpec[] {
  const provides = new Set(TRIGGERS[triggerType]?.provides ?? []);
  return Object.values(CONDITIONS).filter((spec) =>
    (spec.requires ?? []).every((need) => provides.has(need)),
  );
}

export function actionsFor(triggerType: string): ActionSpec[] {
  const provides = new Set(TRIGGERS[triggerType]?.provides ?? []);
  return Object.values(ACTIONS).filter((spec) =>
    (spec.requires ?? []).every((need) => provides.has(need)),
  );
}

/** What each permission is actually called in Discord's own settings. */
export const PERMISSION_NAMES: Record<string, string> = {
  send_messages: "Send Messages",
  manage_messages: "Manage Messages",
  manage_roles: "Manage Roles",
  manage_channels: "Manage Channels",
  manage_nicknames: "Manage Nicknames",
  moderate_members: "Time Out Members",
  move_members: "Move Members",
  add_reactions: "Add Reactions",
  create_public_threads: "Create Public Threads",
};

export function permissionName(key: string): string {
  return PERMISSION_NAMES[key] ?? key.replace(/_/g, " ");
}

export function fieldsOf(spec: Entry | undefined): Field[] {
  return spec?.fields ?? [];
}
