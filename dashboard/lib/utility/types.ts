/**
 * The Utility module's domain types, and the serialise/parse helpers that move
 * between them and the bot's SQLite rows.
 *
 * Everything a reminder or sticky can hold — a full embed, ping role, buttons,
 * schedule — is expressed here once, so the store, the server actions and the
 * builder all agree on the shape. IDs are strings throughout: a Discord
 * snowflake is a 64-bit integer that `JSON.parse` would silently round, so the
 * store reads JSON columns with `parseJsonPreservingIds` and everything we
 * write back is a string that the Python side coerces with `int()`.
 */

export type ReminderKind = "scheduled" | "interval" | "oneoff";

export type ScheduleFrequency =
  | "daily"
  | "weekly"
  | "biweekly"
  | "monthly"
  | "every_x_days";

export interface EmbedField {
  name: string;
  value: string;
  inline: boolean;
}

export interface EmbedAuthor {
  name: string;
  url?: string;
  iconUrl?: string;
}

export interface EmbedFooter {
  text: string;
  iconUrl?: string;
}

/** A full Discord embed, as the builder edits it. `color` is a 24-bit int. */
export interface Embed {
  title: string;
  description: string;
  color: number | null;
  url: string;
  imageUrl: string;
  thumbnailUrl: string;
  author: EmbedAuthor;
  footer: EmbedFooter;
  fields: EmbedField[];
}

/** A link button under the message. */
export interface LinkButton {
  type: "link";
  label: string;
  url: string;
  emoji?: string;
}

/** A button that toggles a role on the clicker. */
export interface RoleButton {
  roleId: string;
  label: string;
  emoji?: string;
  style: "primary" | "secondary" | "success" | "danger";
}

export interface Schedule {
  frequency: ScheduleFrequency;
  /** HH:MM in UTC — the builder converts the admin's local time before saving. */
  timeUtc: string;
  /** The timezone the admin authored in, kept so the builder can show it back. */
  timezone: string;
  daysOfWeek: number[];
  dayOfMonth: number;
  intervalDays: number;
}

export interface Reminder {
  id: string;
  guildId: string;
  kind: ReminderKind;
  name: string;
  enabled: boolean;
  plainText: boolean;
  embed: Embed;
  content: string;
  pingRoleId: string | null;
  channelIds: string[];
  buttons: LinkButton[];
  reactionRole: RoleButton | null;
  useTimestamp: boolean;
  deletePrevious: boolean;
  intervalSeconds: number;
  schedule: Schedule | null;
  lastSentTs: number;
  createdAt: number;
  updatedAt: number;
}

export interface Sticky {
  id: string;
  guildId: string;
  channelId: string;
  enabled: boolean;
  name: string;
  plainText: boolean;
  embed: Embed;
  content: string;
  pingRoleId: string | null;
  buttons: LinkButton[];
  reactionRole: RoleButton | null;
  minIntervalSeconds: number;
}

export type ReactionScope = "all" | "role_mention" | "from_user";
export type ReactionMode = "remove" | "allowlist" | "blocklist";

export interface ReactionRule {
  id: string;
  guildId: string;
  channelId: string;
  enabled: boolean;
  scope: ReactionScope;
  mode: ReactionMode;
  roleIds: string[];
  userIds: string[];
  emoji: string[];
  bypassRoleIds: string[];
  removeAfterSeconds: number;
  maxReactions: number;
  includeThreads: boolean;
}

export interface MediaChannel {
  id: string;
  guildId: string;
  channelId: string;
  enabled: boolean;
  allowAttachments: boolean;
  allowLinks: boolean;
  allowEmbeds: boolean;
  allowStickers: boolean;
  bypassRoleIds: string[];
  threadEnabled: boolean;
  threadNameTemplate: string;
  threadArchiveMinutes: number;
  autoReact: string[];
  postCooldownSeconds: number;
  dmOnDelete: boolean;
}

// ------------------------------------------------------------------- defaults

export function emptyEmbed(): Embed {
  return {
    title: "",
    description: "",
    color: 0x57f287,
    url: "",
    imageUrl: "",
    thumbnailUrl: "",
    author: { name: "", url: "", iconUrl: "" },
    footer: { text: "", iconUrl: "" },
    fields: [],
  };
}

export function emptySchedule(): Schedule {
  return {
    frequency: "weekly",
    timeUtc: "12:00",
    timezone: "UTC",
    daysOfWeek: [],
    dayOfMonth: 1,
    intervalDays: 1,
  };
}

// -------------------------------------------------------- embed (de)serialise

/**
 * The bot stores the embed as the JSON document `render.build_embed` reads:
 * snake_case keys, only the ones that are set. This maps between that and the
 * always-fully-populated shape the builder edits.
 */
export function embedToJson(embed: Embed): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (embed.title.trim()) out.title = embed.title;
  if (embed.description.trim()) out.description = embed.description;
  if (embed.color !== null) out.color = embed.color;
  if (embed.url.trim()) out.url = embed.url;
  if (embed.imageUrl.trim()) out.image_url = embed.imageUrl;
  if (embed.thumbnailUrl.trim()) out.thumbnail_url = embed.thumbnailUrl;
  if (embed.author.name.trim()) {
    out.author = {
      name: embed.author.name,
      ...(embed.author.url?.trim() ? { url: embed.author.url } : {}),
      ...(embed.author.iconUrl?.trim() ? { icon_url: embed.author.iconUrl } : {}),
    };
  }
  if (embed.footer.text.trim()) {
    out.footer = {
      text: embed.footer.text,
      ...(embed.footer.iconUrl?.trim() ? { icon_url: embed.footer.iconUrl } : {}),
    };
  }
  const fields = embed.fields.filter((f) => f.name.trim() || f.value.trim());
  if (fields.length) out.fields = fields;
  return out;
}

export function embedFromJson(raw: Record<string, unknown> | null | undefined): Embed {
  const base = emptyEmbed();
  if (!raw || typeof raw !== "object") return base;
  const author = (raw.author ?? {}) as Record<string, unknown>;
  const footer = (raw.footer ?? {}) as Record<string, unknown>;
  const fields = Array.isArray(raw.fields) ? (raw.fields as Record<string, unknown>[]) : [];
  return {
    title: str(raw.title),
    description: str(raw.description),
    color: typeof raw.color === "number" ? raw.color : base.color,
    url: str(raw.url),
    imageUrl: str(raw.image_url),
    thumbnailUrl: str(raw.thumbnail_url),
    author: { name: str(author.name), url: str(author.url), iconUrl: str(author.icon_url) },
    footer: { text: str(footer.text), iconUrl: str(footer.icon_url) },
    fields: fields.map((f) => ({
      name: str(f.name),
      value: str(f.value),
      inline: Boolean(f.inline),
    })),
  };
}

/** The reaction-role button, mapped to the `{role_id, button_*}` shape the
 * cog's `render.ReactionRoleButton` expects. */
export function reactionRoleToJson(btn: RoleButton | null): Record<string, unknown> | null {
  if (!btn || !btn.roleId) return null;
  return {
    role_id: btn.roleId,
    button_label: btn.label,
    button_style: btn.style,
    ...(btn.emoji ? { button_emoji: btn.emoji } : {}),
  };
}

export function reactionRoleFromJson(raw: Record<string, unknown> | null | undefined): RoleButton | null {
  if (!raw || typeof raw !== "object" || !raw.role_id) return null;
  const style = str(raw.button_style) as RoleButton["style"];
  return {
    roleId: String(raw.role_id),
    label: str(raw.button_label) || "Get Role",
    emoji: str(raw.button_emoji) || undefined,
    style: ["primary", "secondary", "success", "danger"].includes(style) ? style : "secondary",
  };
}

export function buttonsToJson(buttons: LinkButton[]): Record<string, unknown>[] {
  return buttons
    .filter((b) => b.url.trim() && b.label.trim())
    .map((b) => ({ type: "link", label: b.label, url: b.url, ...(b.emoji ? { emoji: b.emoji } : {}) }));
}

export function buttonsFromJson(raw: unknown): LinkButton[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .filter((b): b is Record<string, unknown> => !!b && typeof b === "object" && (b as Record<string, unknown>).type === "link")
    .map((b) => ({ type: "link", label: str(b.label), url: str(b.url), emoji: str(b.emoji) || undefined }));
}

function str(value: unknown): string {
  return typeof value === "string" ? value : value == null ? "" : String(value);
}
