/**
 * The web mirror of the ticketing cog's data model.
 *
 * These shapes match `cogs/ticketing/defaults.py` field-for-field, because a
 * topic or panel object saved here is merged straight into the bot's
 * `topics.json` / `panels.json` — the dashboard is editing the same objects the
 * cog reads, just through the SQLite bridge (see `store.ts`).
 *
 * One deliberate difference: every Discord id is a `string` here, not a number.
 * A snowflake is a 64-bit integer and a JavaScript number rounds it (see
 * `lib/automations/json.ts`), so ids travel as strings and the bot coerces them
 * back to ints when it applies a change (`_coerce_topic_ids` in the cog).
 */

/** How a topic's ticket is housed: a channel under a category, or a thread. */
export type TopicMode = "channel" | "thread";

/** A semantic label only — the cog branches on feature flags, not this. */
export type TopicType = "ticket" | "application" | "survey";

export type ButtonColor = "primary" | "secondary" | "success" | "danger";

export type ApplicationChannelMode = "dm" | "channel";

/** A ticket topic — everything a `/ticketing` topic wizard can set. */
export interface Topic {
  name: string;
  label: string;
  emoji: string | null;
  type: TopicType;
  mode: TopicMode;
  /** Category id (channel mode) or text-channel id (thread mode). */
  parentId: string | null;
  staffRoleIds: string[];
  logChannelId: string | null;
  welcomeMessage: string;

  // Applications & surveys
  questions: string[];
  approvalMode: boolean;
  discussionMode: boolean;
  applicationChannelMode: ApplicationChannelMode;
  buttonColor: ButtonColor;

  // Pre-ticket requirement 1
  preModalEnabled: boolean;
  preModalQuestion: string;
  preModalRedirectUrl: string | null;
  preModalRedirectChannelId: string | null;
  preModalYesLabel: string;
  preModalNoLabel: string;
  preModalNoMessage: string;
  preModalReadyButtonEnabled: boolean;
  preModalReadyButtonLabel: string;

  // Pre-ticket required answer
  preModalAnswerEnabled: boolean;
  preModalAnswerQuestion: string;

  // Pre-ticket requirement 2
  preModal2Enabled: boolean;
  preModal2Question: string;
  preModal2RedirectUrl: string | null;
  preModal2RedirectChannelId: string | null;
  preModal2YesLabel: string;
  preModal2NoLabel: string;
  preModal2NoMessage: string;
  preModal2ReadyButtonEnabled: boolean;
  preModal2ReadyButtonLabel: string;

  cooldownMinutes: number;
  pingStaffOnCreate: boolean;

  // Closing
  deleteOnClose: boolean;
  memberCanClose: boolean;
  closeMessage: string;

  // Claiming
  claimEnabled: boolean;
  claimAlertsChannelId: string | null;
  claimRoleId: string | null;

  // Naming & appearance
  useNumbering: boolean;
  ticketCounter: number;
  channelNameFormat: string | null;
  blacklistedUserIds: string[];
  embedColor: string | null;
}

export type PanelDisplayMode = "buttons" | "dropdown" | "mixed";
export type ImageType = "banner" | "thumbnail";
export type TopicDisplay = "button" | "dropdown";

export interface PanelCategory {
  slug: string;
  label: string;
  emoji: string | null;
  displayMode: PanelDisplayMode;
  buttonColor: ButtonColor;
  topicNames: string[];
}

/** A support panel — the message members click to open a ticket. */
export interface Panel {
  name: string;
  title: string | null;
  description: string;
  channelId: string | null;
  /** Set by the bot once the panel is posted; the sign it is live. */
  messageId: string | null;
  displayMode: PanelDisplayMode;
  imageUrl: string | null;
  imageType: ImageType;
  topicNames: string[];
  /** Interleaved order of `topicName` and `cat:<slug>` entries. */
  topicOrder: string[];
  topicDisplayMap: Record<string, TopicDisplay>;
  categories: PanelCategory[];
}

export interface SurveyResponse {
  userId: string;
  userName: string;
  timestamp: string;
  answers: Record<string, string>;
}

// ------------------------------------------------------------------ defaults

export function topicDefaults(name: string): Topic {
  return {
    name,
    label: name,
    emoji: null,
    type: "ticket",
    mode: "thread",
    parentId: null,
    staffRoleIds: [],
    logChannelId: null,
    welcomeMessage:
      "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**",
    questions: [],
    approvalMode: false,
    discussionMode: false,
    applicationChannelMode: "dm",
    buttonColor: "secondary",
    preModalEnabled: false,
    preModalQuestion: "Do you have your profile link ready?",
    preModalRedirectUrl: null,
    preModalRedirectChannelId: null,
    preModalYesLabel: "Yes, I have it",
    preModalNoLabel: "No, I need to get it",
    preModalNoMessage:
      "Please get what you need ready, then click the button below to continue.",
    preModalReadyButtonEnabled: true,
    preModalReadyButtonLabel: "I'm ready now",
    preModalAnswerEnabled: false,
    preModalAnswerQuestion: "Please provide your information:",
    preModal2Enabled: false,
    preModal2Question: "Do you have your second requirement ready?",
    preModal2RedirectUrl: null,
    preModal2RedirectChannelId: null,
    preModal2YesLabel: "Yes",
    preModal2NoLabel: "No",
    preModal2NoMessage:
      "Please get what you need ready, then click the button below to continue.",
    preModal2ReadyButtonEnabled: true,
    preModal2ReadyButtonLabel: "I'm ready now",
    cooldownMinutes: 5,
    pingStaffOnCreate: false,
    deleteOnClose: true,
    memberCanClose: true,
    closeMessage:
      "Your ticket `{channel}` in **{server}** has been closed by {closer}.",
    claimEnabled: false,
    claimAlertsChannelId: null,
    claimRoleId: null,
    useNumbering: false,
    ticketCounter: 0,
    channelNameFormat: null,
    blacklistedUserIds: [],
    embedColor: null,
  };
}

export function panelDefaults(name: string): Panel {
  return {
    name,
    title: null,
    description: "Please select an option below.",
    channelId: null,
    messageId: null,
    displayMode: "buttons",
    imageUrl: null,
    imageType: "banner",
    topicNames: [],
    topicOrder: [],
    topicDisplayMap: {},
    categories: [],
  };
}

// --------------------------------------------------------- parse / serialise

type Raw = Record<string, unknown>;

const asString = (v: unknown): string | null =>
  v === null || v === undefined || v === "" || v === 0 || v === "0"
    ? null
    : String(v);

const asStringList = (v: unknown): string[] =>
  Array.isArray(v) ? v.map((x) => String(x)) : [];

const asBool = (v: unknown, fallback: boolean): boolean =>
  typeof v === "boolean" ? v : v === undefined || v === null ? fallback : Boolean(v);

const asText = (v: unknown, fallback: string): string =>
  v === undefined || v === null ? fallback : String(v);

const asNum = (v: unknown, fallback: number): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

/** Turn a snapshot row (already ID-preserving parsed) into a `Topic`. */
export function parseTopic(name: string, raw: Raw): Topic {
  const d = topicDefaults(name);
  return {
    ...d,
    name,
    label: asText(raw.label, name),
    emoji: raw.emoji == null ? null : String(raw.emoji),
    type: (raw.type as TopicType) ?? d.type,
    mode: (raw.mode as TopicMode) ?? d.mode,
    parentId: asString(raw.parent_id),
    staffRoleIds: asStringList(raw.staff_role_ids),
    logChannelId: asString(raw.log_channel_id),
    welcomeMessage: asText(raw.welcome_message, d.welcomeMessage),
    questions: asStringList(raw.questions),
    approvalMode: asBool(raw.approval_mode, d.approvalMode),
    discussionMode: asBool(raw.discussion_mode, d.discussionMode),
    applicationChannelMode:
      (raw.application_channel_mode as ApplicationChannelMode) ?? d.applicationChannelMode,
    buttonColor: (raw.button_color as ButtonColor) ?? d.buttonColor,
    preModalEnabled: asBool(raw.pre_modal_enabled, d.preModalEnabled),
    preModalQuestion: asText(raw.pre_modal_question, d.preModalQuestion),
    preModalRedirectUrl: raw.pre_modal_redirect_url == null ? null : String(raw.pre_modal_redirect_url),
    preModalRedirectChannelId: asString(raw.pre_modal_redirect_channel_id),
    preModalYesLabel: asText(raw.pre_modal_yes_label, d.preModalYesLabel),
    preModalNoLabel: asText(raw.pre_modal_no_label, d.preModalNoLabel),
    preModalNoMessage: asText(raw.pre_modal_no_message, d.preModalNoMessage),
    preModalReadyButtonEnabled: asBool(raw.pre_modal_ready_button_enabled, d.preModalReadyButtonEnabled),
    preModalReadyButtonLabel: asText(raw.pre_modal_ready_button_label, d.preModalReadyButtonLabel),
    preModalAnswerEnabled: asBool(raw.pre_modal_answer_enabled, d.preModalAnswerEnabled),
    preModalAnswerQuestion: asText(raw.pre_modal_answer_question, d.preModalAnswerQuestion),
    preModal2Enabled: asBool(raw.pre_modal_2_enabled, d.preModal2Enabled),
    preModal2Question: asText(raw.pre_modal_2_question, d.preModal2Question),
    preModal2RedirectUrl: raw.pre_modal_2_redirect_url == null ? null : String(raw.pre_modal_2_redirect_url),
    preModal2RedirectChannelId: asString(raw.pre_modal_2_redirect_channel_id),
    preModal2YesLabel: asText(raw.pre_modal_2_yes_label, d.preModal2YesLabel),
    preModal2NoLabel: asText(raw.pre_modal_2_no_label, d.preModal2NoLabel),
    preModal2NoMessage: asText(raw.pre_modal_2_no_message, d.preModal2NoMessage),
    preModal2ReadyButtonEnabled: asBool(raw.pre_modal_2_ready_button_enabled, d.preModal2ReadyButtonEnabled),
    preModal2ReadyButtonLabel: asText(raw.pre_modal_2_ready_button_label, d.preModal2ReadyButtonLabel),
    // Older data used survey_cooldown_minutes; the cog migrates it, but a
    // freshly-read snapshot may still carry either.
    cooldownMinutes: asNum(raw.cooldown_minutes ?? raw.survey_cooldown_minutes, d.cooldownMinutes),
    pingStaffOnCreate: asBool(raw.ping_staff_on_create, d.pingStaffOnCreate),
    deleteOnClose: asBool(raw.delete_on_close, d.deleteOnClose),
    memberCanClose: asBool(raw.member_can_close, d.memberCanClose),
    closeMessage: asText(raw.close_message, d.closeMessage),
    claimEnabled: asBool(raw.claim_enabled, d.claimEnabled),
    claimAlertsChannelId: asString(raw.claim_alerts_channel_id),
    claimRoleId: asString(raw.claim_role_id),
    useNumbering: asBool(raw.use_numbering, d.useNumbering),
    ticketCounter: asNum(raw.ticket_counter, d.ticketCounter),
    channelNameFormat: raw.channel_name_format == null ? null : String(raw.channel_name_format),
    blacklistedUserIds: asStringList(raw.blacklisted_user_ids),
    embedColor: raw.embed_color == null ? null : String(raw.embed_color),
  };
}

/** Serialise a `Topic` into the snake_case object the cog stores. */
export function serialiseTopic(t: Topic): Raw {
  return {
    name: t.name,
    label: t.label,
    emoji: t.emoji,
    type: t.type,
    mode: t.mode,
    parent_id: t.parentId,
    staff_role_ids: t.staffRoleIds,
    log_channel_id: t.logChannelId,
    welcome_message: t.welcomeMessage,
    questions: t.questions,
    approval_mode: t.approvalMode,
    discussion_mode: t.discussionMode,
    application_channel_mode: t.applicationChannelMode,
    button_color: t.buttonColor,
    pre_modal_enabled: t.preModalEnabled,
    pre_modal_question: t.preModalQuestion,
    pre_modal_redirect_url: t.preModalRedirectUrl,
    pre_modal_redirect_channel_id: t.preModalRedirectChannelId,
    pre_modal_yes_label: t.preModalYesLabel,
    pre_modal_no_label: t.preModalNoLabel,
    pre_modal_no_message: t.preModalNoMessage,
    pre_modal_ready_button_enabled: t.preModalReadyButtonEnabled,
    pre_modal_ready_button_label: t.preModalReadyButtonLabel,
    pre_modal_answer_enabled: t.preModalAnswerEnabled,
    pre_modal_answer_question: t.preModalAnswerQuestion,
    pre_modal_2_enabled: t.preModal2Enabled,
    pre_modal_2_question: t.preModal2Question,
    pre_modal_2_redirect_url: t.preModal2RedirectUrl,
    pre_modal_2_redirect_channel_id: t.preModal2RedirectChannelId,
    pre_modal_2_yes_label: t.preModal2YesLabel,
    pre_modal_2_no_label: t.preModal2NoLabel,
    pre_modal_2_no_message: t.preModal2NoMessage,
    pre_modal_2_ready_button_enabled: t.preModal2ReadyButtonEnabled,
    pre_modal_2_ready_button_label: t.preModal2ReadyButtonLabel,
    cooldown_minutes: t.cooldownMinutes,
    ping_staff_on_create: t.pingStaffOnCreate,
    delete_on_close: t.deleteOnClose,
    member_can_close: t.memberCanClose,
    close_message: t.closeMessage,
    claim_enabled: t.claimEnabled,
    claim_alerts_channel_id: t.claimAlertsChannelId,
    claim_role_id: t.claimRoleId,
    use_numbering: t.useNumbering,
    ticket_counter: t.ticketCounter,
    channel_name_format: t.channelNameFormat,
    blacklisted_user_ids: t.blacklistedUserIds,
    embed_color: t.embedColor,
  };
}

function parseCategory(slug: string, raw: Raw): PanelCategory {
  return {
    slug,
    label: asText(raw.label, slug),
    emoji: raw.emoji == null ? null : String(raw.emoji),
    displayMode: (raw.display_mode as PanelDisplayMode) ?? "buttons",
    buttonColor: (raw.button_color as ButtonColor) ?? "secondary",
    topicNames: asStringList(raw.topic_names),
  };
}

export function parsePanel(name: string, raw: Raw): Panel {
  const d = panelDefaults(name);
  const categoriesRaw = (raw.categories as Record<string, Raw>) ?? {};
  return {
    ...d,
    name,
    title: raw.title == null ? null : String(raw.title),
    description: asText(raw.description, d.description),
    channelId: asString(raw.channel_id),
    messageId: asString(raw.message_id),
    displayMode: (raw.display_mode as PanelDisplayMode) ?? d.displayMode,
    imageUrl: raw.image_url == null ? null : String(raw.image_url),
    imageType: (raw.image_type as ImageType) ?? d.imageType,
    topicNames: asStringList(raw.topic_names),
    topicOrder: asStringList(raw.topic_order),
    topicDisplayMap: (raw.topic_display_map as Record<string, TopicDisplay>) ?? {},
    categories: Object.entries(categoriesRaw).map(([slug, c]) => parseCategory(slug, c)),
  };
}

export function serialisePanel(p: Panel): Raw {
  const categories: Record<string, Raw> = {};
  for (const c of p.categories) {
    categories[c.slug] = {
      label: c.label,
      emoji: c.emoji,
      display_mode: c.displayMode,
      button_color: c.buttonColor,
      topic_names: c.topicNames,
    };
  }
  return {
    name: p.name,
    title: p.title,
    description: p.description,
    channel_id: p.channelId,
    // message_id is intentionally omitted — the bot owns it. Publishing is a
    // separate command, and a null here would clear a live panel's id.
    display_mode: p.displayMode,
    image_url: p.imageUrl,
    image_type: p.imageType,
    topic_names: p.topicNames,
    topic_order: p.topicOrder,
    topic_display_map: p.topicDisplayMap,
    categories,
  };
}

export function parseResponses(raw: unknown): SurveyResponse[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((r) => {
    const row = r as Raw;
    return {
      userId: String(row.user_id ?? ""),
      userName: String(row.user_name ?? ""),
      timestamp: String(row.timestamp ?? ""),
      answers: (row.answers as Record<string, string>) ?? {},
    };
  });
}

/** Discord's limit on components a single panel can carry. */
export const MAX_PANEL_ITEMS = 25;

/** A topic slug: lowercase, no spaces — mirrors CreateTopicNameModal. */
export function slugify(raw: string): string {
  return raw
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9\-_/]/g, "")
    .slice(0, 90);
}
