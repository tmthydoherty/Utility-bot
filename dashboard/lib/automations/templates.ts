/**
 * Ready-made automations.
 *
 * Mirrors `cogs/automations/templates.py`, key for key, so an automation
 * started on Discord and one started here are the same automation.
 *
 * Starting from a blank automation means understanding the whole model before
 * you can build anything — which trigger, which checks, which steps, in what
 * order. Starting from one that already works means reading something concrete
 * and changing the parts you disagree with, which is a much shorter path to
 * understanding. So this is the front door of the web builder, not a menu item
 * tucked behind one.
 *
 * Each template lands fully wired and **switched off in test mode**. Nothing
 * here can act until it is explicitly turned on.
 *
 * `asks` names the settings only the template's author knows are worth a
 * decision, by path, so the setup flow can open each one instead of describing
 * where to go and hoping. Blanks that stop it working are *derived* by
 * `readiness.ts` and never repeated here — being asked the same question
 * twice, once as a requirement and once as a suggestion, is how a checklist
 * starts getting skimmed.
 */

import type { SettingsValues } from "@/lib/schema/types";

import type { StepPath } from "./types";

export interface Ask {
  where: "trigger" | "condition" | "action";
  path: StepPath;
  field: string;
  /** The question, in the reader's language rather than the field's label. */
  prompt: string;
}

export interface Template {
  key: string;
  name: string;
  /** One line, plain language. Shown on the card. */
  blurb: string;
  category: string;
  /** lucide-react icon name. */
  icon: string;
  triggerType: string;
  triggerConfig?: SettingsValues;
  conditions?: Record<string, unknown>;
  steps: Record<string, unknown>[];
  asks?: Ask[];
  /**
   * Advice that points at no single setting — "make a second copy for the
   * morning", "consider exempting staff". Shown as prose at the end, because
   * there is nothing to open.
   */
  needs?: string[];
  /**
   * Roughly how long setting this one up takes, in questions. Shown on the
   * card so nobody starts the seven-question one thinking it is the two.
   */
  questions: number;
}

const all = (...items: unknown[]) => ({ op: "and", items });
const check = (type: string, config: SettingsValues = {}) => ({ type, config });
const checkNot = (type: string, config: SettingsValues = {}) => ({
  type,
  config,
  negate: true,
});
const step = (type: string, config: SettingsValues = {}) => ({ type, config });

export const TEMPLATES: Template[] = [
  {
    key: "welcome",
    name: "Welcome new members",
    blurb: "Post a greeting in a channel whenever someone joins.",
    category: "Popular",
    icon: "PartyPopper",
    triggerType: "member_joined",
    steps: [
      step("send_message", {
        destination: "channel",
        content: "Welcome to {guild.name}, {user.mention}! 👋",
      }),
    ],
    questions: 2,
  },
  {
    key: "keyword_reply",
    name: "Reply to a word or phrase",
    blurb: "When someone says something, Vibey replies to their message.",
    category: "Popular",
    icon: "MessageSquareReply",
    triggerType: "message_sent",
    conditions: all(checkNot("is_reply"), check("content_contains", { text: "hello, hi, hey" })),
    steps: [step("send_message", { destination: "reply", content: "Hey {user.name}! 👋" })],
    asks: [
      { where: "condition", path: [1], field: "text", prompt: "Which words should this reply to?" },
      { where: "action", path: [0], field: "content", prompt: "What should Vibey say back?" },
    ],
    questions: 2,
  },
  {
    key: "react_to_images",
    name: "React to every image",
    blurb: "Add reactions to any picture or clip posted in a channel.",
    category: "Popular",
    icon: "ThumbsUp",
    triggerType: "message_sent",
    conditions: all(check("channel_is", { channels: [] }), check("has_image")),
    steps: [step("add_reaction", { emoji: ["⬆️", "⬇️"] })],
    asks: [{ where: "action", path: [0], field: "emoji", prompt: "Which reactions should it add?" }],
    questions: 2,
  },
  {
    key: "auto_thread",
    name: "Start a thread on every post",
    blurb: "Opens a comment thread under each message in a channel.",
    category: "Popular",
    icon: "MessagesSquare",
    triggerType: "message_sent",
    conditions: all(check("channel_is", { channels: [] })),
    steps: [step("create_thread", { name: "{user} — discussion", archive_minutes: "1440" })],
    questions: 1,
  },
  {
    key: "goodbye",
    name: "Say goodbye when someone leaves",
    blurb: "Posts a note in a channel when a member leaves the server.",
    category: "Popular",
    icon: "DoorOpen",
    triggerType: "member_left",
    steps: [
      step("send_message", { destination: "channel", content: "{user.name} has left the server." }),
    ],
    questions: 2,
  },

  {
    key: "no_links",
    name: "No links in this channel",
    blurb: "Deletes messages containing links, and DMs the person why.",
    category: "Keeping order",
    icon: "Unlink",
    triggerType: "message_sent",
    conditions: all(check("channel_is", { channels: [] }), check("has_link")),
    steps: [
      step("send_message", {
        destination: "dm",
        content: "Heads up — links aren't allowed in that channel.",
      }),
      step("delete_message"),
    ],
    needs: [
      "Consider adding a requirement so staff aren't caught by it — add “They have a permission”, set it to Manage Messages, then switch it to “doesn't match”.",
    ],
    questions: 1,
  },
  {
    key: "calm_the_caps",
    name: "Ask people not to shout",
    blurb: "Replies to messages that are mostly capital letters.",
    category: "Keeping order",
    icon: "CaseUpper",
    triggerType: "message_sent",
    conditions: all(check("caps_ratio", { percent: 70, min_length: 10 })),
    steps: [step("send_message", { destination: "reply", content: "No need to shout! 🙂" })],
    asks: [
      {
        where: "condition",
        path: [0],
        field: "percent",
        prompt: "How much of the message has to be capitals to count as shouting?",
      },
    ],
    questions: 1,
  },
  {
    key: "three_strikes",
    name: "Three strikes, then a timeout",
    blurb: "Counts how often someone breaks a rule and times them out on the third time in a day.",
    category: "Keeping order",
    icon: "AlertTriangle",
    triggerType: "message_sent",
    conditions: all(check("content_contains", { text: "badword" })),
    steps: [
      step("counter_change", { key: "strikes", scope: "user", amount: 1, window: 86400 }),
      step("delete_message"),
      {
        type: "if",
        conditions: all(
          check("counter_compare", { key: "strikes", scope: "user", op: "gte", value: 3 }),
        ),
        then: [
          step("timeout_member", { duration: 600, reason: "Three strikes in one day" }),
          step("send_message", {
            destination: "dm",
            content: "That's 3 strikes today, so you're timed out for 10 minutes.",
          }),
        ],
        else: [
          step("send_message", {
            destination: "dm",
            content: "That's strike {counter.strikes} of 3 today.",
          }),
        ],
      },
    ],
    asks: [
      {
        where: "condition",
        path: [0],
        field: "text",
        prompt: "Which words should count as a strike?",
      },
      { where: "action", path: [2, 0, 0], field: "duration", prompt: "How long should the timeout last?" },
    ],
    questions: 2,
  },
  {
    key: "quiet_hours",
    name: "Slow the chat down overnight",
    blurb: "Turns slowmode on at night so the chat stays calm while you sleep.",
    category: "Keeping order",
    icon: "Moon",
    triggerType: "schedule",
    triggerConfig: { frequency: "daily", time_utc: "02:00" },
    steps: [step("set_slowmode", { channel: 0, seconds: 30 })],
    asks: [
      {
        where: "trigger",
        path: [],
        field: "time_utc",
        prompt: "What time should the chat slow down? (24-hour clock, UTC)",
      },
      { where: "action", path: [0], field: "seconds", prompt: "How many seconds between messages?" },
    ],
    needs: [
      "Make a second copy set to the morning, with slowmode back to 0, to turn it off again.",
    ],
    questions: 3,
  },

  {
    key: "role_on_keyword",
    name: "Give a role when someone asks",
    blurb: "Someone types a word and gets a role automatically.",
    category: "Roles",
    icon: "ShieldPlus",
    triggerType: "message_sent",
    conditions: all(check("content_exactly", { text: "!pings" })),
    steps: [step("toggle_role", { role: 0 }), step("add_reaction", { emoji: ["✅"] })],
    asks: [
      {
        where: "condition",
        path: [0],
        field: "text",
        prompt: "What should people type to get the role?",
      },
    ],
    questions: 2,
  },
  {
    key: "temporary_role",
    name: "Give a role for an hour",
    blurb: "Hands out a role that takes itself back later, all on its own.",
    category: "Roles",
    icon: "TimerReset",
    triggerType: "message_sent",
    conditions: all(check("content_exactly", { text: "!vip" })),
    steps: [step("add_role", { role: 0, duration: 3600 })],
    asks: [
      {
        where: "condition",
        path: [0],
        field: "text",
        prompt: "What should people type to get the role?",
      },
      { where: "action", path: [0], field: "duration", prompt: "How long should they keep it?" },
    ],
    questions: 3,
  },
  {
    key: "role_welcome_pack",
    name: "Big welcome when someone gets a role",
    blurb: "DMs them a picture, announces it in a channel, and reacts to that announcement.",
    category: "Roles",
    icon: "Gift",
    triggerType: "role_added",
    triggerConfig: { roles: [] },
    steps: [
      step("send_message", {
        destination: "dm",
        use_embed: true,
        embed_title: "Welcome aboard!",
        content: "You've just been given a new role in {guild.name}. 🎉",
        embed_colour: "green",
        embed_thumbnail: "{user.avatar}",
        embed_image: "",
        embed_footer: "{guild.name}",
      }),
      step("send_message", {
        destination: "channel",
        channel: 0,
        content: "Everyone welcome {user.mention}! 🎉",
      }),
      // Reacts to the announcement above, not to any incoming message — a role
      // change has no message of its own.
      step("add_reaction", { emoji: ["🎉", "👋", "❤️"], target: "sent" }),
    ],
    asks: [
      {
        where: "trigger",
        path: [],
        field: "roles",
        prompt: "Which role should set this off? Leaving it empty runs for any role at all.",
      },
      { where: "action", path: [0], field: "embed_image", prompt: "A GIF or image for the DM, if you want one" },
      { where: "action", path: [2], field: "emoji", prompt: "Which emoji should it react with?" },
    ],
    questions: 4,
  },

  {
    key: "level_up",
    name: "Congratulate a level up",
    blurb: "Celebrates when someone reaches a new level.",
    category: "Rewards",
    icon: "TrendingUp",
    triggerType: "member_level_up",
    steps: [
      step("send_message", {
        destination: "channel",
        content: "🎉 {user.mention} just levelled up!",
      }),
    ],
    questions: 2,
  },
  {
    key: "thank_booster",
    name: "Thank a new booster",
    blurb: "Says thanks and gives a role when someone boosts the server.",
    category: "Rewards",
    icon: "Sparkles",
    triggerType: "member_boosted",
    steps: [
      step("send_message", {
        destination: "channel",
        content: "💜 Thank you for boosting, {user.mention}!",
      }),
      step("add_role", { role: 0 }),
    ],
    questions: 3,
  },

  {
    key: "ticket_alert",
    name: "Alert staff about new tickets",
    blurb: "Posts in a staff channel whenever a ticket is opened.",
    category: "Staff",
    icon: "BellRing",
    triggerType: "ticket_opened",
    steps: [
      step("send_message", { destination: "channel", content: "🎫 {user.mention} opened a ticket." }),
    ],
    // A ticket supplies its own channel, so nothing here is *blank* — left
    // alone the alert posts into the ticket, where no one is watching.
    asks: [
      {
        where: "action",
        path: [0],
        field: "channel",
        prompt: "Which staff channel should the alert go to?",
      },
    ],
    questions: 1,
  },
];

export const TEMPLATES_BY_KEY: Record<string, Template> = Object.fromEntries(
  TEMPLATES.map((template) => [template.key, template]),
);

export const TEMPLATE_CATEGORIES = [
  "Popular",
  "Keeping order",
  "Roles",
  "Rewards",
  "Staff",
] as const;

export function templatesByCategory(): [string, Template[]][] {
  return TEMPLATE_CATEGORIES.map(
    (category): [string, Template[]] => [
      category,
      TEMPLATES.filter((template) => template.category === category),
    ],
  ).filter(([, list]) => list.length > 0);
}
