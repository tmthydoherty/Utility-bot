import "server-only";

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";

import {
  buttonsFromJson,
  buttonsToJson,
  embedFromJson,
  embedToJson,
  reactionRoleFromJson,
  reactionRoleToJson,
  type Embed,
  type MediaChannel,
  type ReactionRule,
  type Reminder,
  type ReminderKind,
  type RoleButton,
  type LinkButton,
  type Schedule,
  type Sticky,
} from "./types";

/**
 * The bot's Utility database, opened directly.
 *
 * The same deliberate exception the automations builder makes, for the same
 * reasons and with the same safeguards: WAL with a busy timeout on both sides,
 * and a `revision` counter the cog watches so a change here is live within ten
 * seconds. A reminder saved to a database the cog never reads would be a demo,
 * not a feature. The worst a bad write can do is create a reminder, which is
 * created switched off; posting it is a separate, explicit action.
 *
 * The path is configurable so a development machine can point at a copy.
 */
const DB_PATH =
  env.VIBEY_UTILITY_DB ?? path.join(process.cwd(), "..", "data", "utility", "utility.db");

let instance: Database.Database | null = null;

export class UtilityUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's utility database could not be opened.");
    this.name = "UtilityUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;
  // Never created here: a missing file means the bot has not started, and a
  // fresh empty database would be one the cog never reads.
  if (!fs.existsSync(DB_PATH)) {
    throw new UtilityUnavailable(new Error(`no database at ${DB_PATH}`));
  }
  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  instance = db;
  return db;
}

export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM reminders LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

function parseJson<T>(raw: string | null | undefined, fallback: T): T {
  return parseJsonPreservingIds<T>(raw, fallback);
}

function now(): number {
  return Math.floor(Date.now() / 1000);
}

function newId(): string {
  return crypto.randomUUID().replace(/-/g, "").slice(0, 12);
}

function bumpRevision(db: Database.Database): void {
  db.prepare(
    `INSERT INTO settings (key, value) VALUES ('revision', '1')
     ON CONFLICT(key) DO UPDATE SET
       value = CAST(CAST(settings.value AS INTEGER) + 1 AS TEXT)`,
  ).run();
}

function audit(
  db: Database.Database,
  actorId: string,
  entity: string,
  entityId: string,
  action: string,
  detail: string,
): void {
  db.prepare(
    `INSERT INTO audit (ts, user_id, entity, entity_id, action, detail)
     VALUES (?, ?, ?, ?, ?, ?)`,
  ).run(now(), actorId, entity, entityId, action, detail.slice(0, 500));
}

// =====================================================================
// Reminders & one-off messages
// =====================================================================

interface ReminderRow {
  id: string;
  guild_id: number;
  kind: string;
  name: string;
  enabled: number;
  plain_text: number;
  embed_json: string;
  content: string;
  ping_role_id: number | null;
  channel_ids_json: string;
  buttons_json: string;
  reaction_role_json: string | null;
  use_timestamp: number;
  delete_previous: number;
  interval_s: number;
  schedule_json: string | null;
  last_sent_ts: number;
  pending_edit: number;
  created_ts: number;
  updated_ts: number;
}

function toReminder(row: ReminderRow): Reminder {
  const schedule = row.schedule_json
    ? scheduleFromJson(parseJson<Record<string, unknown>>(row.schedule_json, {}))
    : null;
  return {
    id: row.id,
    guildId: String(row.guild_id),
    kind: (row.kind as ReminderKind) ?? "scheduled",
    name: row.name ?? "",
    enabled: row.enabled === 1,
    plainText: row.plain_text === 1,
    embed: embedFromJson(parseJson<Record<string, unknown>>(row.embed_json, {})),
    content: row.content ?? "",
    pingRoleId: row.ping_role_id != null ? String(row.ping_role_id) : null,
    channelIds: (parseJson<unknown[]>(row.channel_ids_json, []) as unknown[]).map(String),
    buttons: buttonsFromJson(parseJson<unknown>(row.buttons_json, [])),
    reactionRole: reactionRoleFromJson(parseJson<Record<string, unknown>>(row.reaction_role_json, {})),
    useTimestamp: row.use_timestamp === 1,
    deletePrevious: row.delete_previous === 1,
    intervalSeconds: row.interval_s ?? 0,
    schedule,
    lastSentTs: row.last_sent_ts ?? 0,
    createdAt: row.created_ts ?? 0,
    updatedAt: row.updated_ts ?? 0,
  };
}

function scheduleToJson(schedule: Schedule): Record<string, unknown> {
  const out: Record<string, unknown> = {
    frequency: schedule.frequency,
    time_utc: schedule.timeUtc,
    creation_timezone: schedule.timezone,
  };
  if (schedule.frequency === "weekly" || schedule.frequency === "biweekly") {
    out.days_of_week = schedule.daysOfWeek;
  } else if (schedule.frequency === "monthly") {
    out.day_of_month = schedule.dayOfMonth;
  } else if (schedule.frequency === "every_x_days") {
    out.interval_days = schedule.intervalDays;
  }
  return out;
}

function scheduleFromJson(raw: Record<string, unknown>): Schedule {
  return {
    frequency: (raw.frequency as Schedule["frequency"]) ?? "weekly",
    timeUtc: typeof raw.time_utc === "string" ? raw.time_utc : "12:00",
    timezone: typeof raw.creation_timezone === "string" ? raw.creation_timezone : "UTC",
    daysOfWeek: Array.isArray(raw.days_of_week) ? (raw.days_of_week as number[]).map(Number) : [],
    dayOfMonth: typeof raw.day_of_month === "number" ? raw.day_of_month : 1,
    intervalDays: typeof raw.interval_days === "number" ? raw.interval_days : 1,
  };
}

export function listReminders(guildId: string): Reminder[] {
  const rows = open()
    .prepare(`SELECT * FROM reminders WHERE guild_id = ? ORDER BY created_ts DESC`)
    .all(guildId) as ReminderRow[];
  return rows.map(toReminder);
}

export function getReminder(guildId: string, id: string): Reminder | null {
  const row = open()
    .prepare(`SELECT * FROM reminders WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as ReminderRow | undefined;
  return row ? toReminder(row) : null;
}

export interface ReminderInput {
  kind: ReminderKind;
  name: string;
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
}

function reminderColumns(input: ReminderInput) {
  return {
    kind: input.kind,
    name: input.name.slice(0, 100),
    plain_text: input.plainText ? 1 : 0,
    embed_json: JSON.stringify(embedToJson(input.embed)),
    content: input.content.slice(0, 2000),
    ping_role_id: input.pingRoleId || null,
    channel_ids_json: JSON.stringify(input.channelIds),
    buttons_json: JSON.stringify(buttonsToJson(input.buttons)),
    reaction_role_json: input.reactionRole
      ? JSON.stringify(reactionRoleToJson(input.reactionRole))
      : null,
    use_timestamp: input.useTimestamp ? 1 : 0,
    delete_previous: input.deletePrevious ? 1 : 0,
    interval_s: input.kind === "interval" ? Math.max(0, Math.floor(input.intervalSeconds)) : 0,
    schedule_json:
      input.kind === "scheduled" && input.schedule
        ? JSON.stringify(scheduleToJson(input.schedule))
        : null,
  };
}

export function createReminder(
  guildId: string,
  input: ReminderInput,
  actorId: string,
): string {
  const db = open();
  const id = newId();
  const ts = now();
  const cols = reminderColumns(input);
  db.transaction(() => {
    db.prepare(
      `INSERT INTO reminders (
         id, guild_id, kind, name, enabled, plain_text, embed_json, content,
         ping_role_id, channel_ids_json, buttons_json, reaction_role_json,
         use_timestamp, delete_previous, interval_s, schedule_json,
         skip_next, skipped_dates_json, last_sent_ts, last_message_ids_json,
         pending_edit, created_ts, updated_ts
       ) VALUES (
         @id, @guild_id, @kind, @name, 0, @plain_text, @embed_json, @content,
         @ping_role_id, @channel_ids_json, @buttons_json, @reaction_role_json,
         @use_timestamp, @delete_previous, @interval_s, @schedule_json,
         0, '[]', 0, '{}', 0, @ts, @ts
       )`,
    ).run({ id, guild_id: guildId, ts, ...cols });
    audit(db, actorId, "reminder", id, "create", input.name);
    bumpRevision(db);
  })();
  return id;
}

export function updateReminder(
  guildId: string,
  id: string,
  input: ReminderInput,
  actorId: string,
): boolean {
  const db = open();
  const existing = db
    .prepare(`SELECT kind, last_sent_ts FROM reminders WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as { kind: string; last_sent_ts: number } | undefined;
  if (!existing) return false;
  const cols = reminderColumns(input);
  // A one-off that has already posted is edited in place: flag it so the cog
  // updates the live message on its next tick rather than doing nothing.
  const pendingEdit = existing.kind === "oneoff" && existing.last_sent_ts > 0 ? 1 : 0;
  db.transaction(() => {
    db.prepare(
      `UPDATE reminders SET
         kind=@kind, name=@name, plain_text=@plain_text, embed_json=@embed_json,
         content=@content, ping_role_id=@ping_role_id, channel_ids_json=@channel_ids_json,
         buttons_json=@buttons_json, reaction_role_json=@reaction_role_json,
         use_timestamp=@use_timestamp, delete_previous=@delete_previous,
         interval_s=@interval_s, schedule_json=@schedule_json,
         pending_edit=@pending_edit, updated_ts=@ts
       WHERE id=@id AND guild_id=@guild_id`,
    ).run({ id, guild_id: guildId, ts: now(), pending_edit: pendingEdit, ...cols });
    audit(db, actorId, "reminder", id, "edit", input.name);
    bumpRevision(db);
  })();
  return true;
}

export function setReminderEnabled(
  guildId: string,
  id: string,
  enabled: boolean,
  actorId: string,
): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(`UPDATE reminders SET enabled = ?, updated_ts = ? WHERE id = ? AND guild_id = ?`)
      .run(enabled ? 1 : 0, now(), id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "reminder", id, enabled ? "enable" : "disable", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function deleteReminder(guildId: string, id: string, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(`DELETE FROM reminders WHERE id = ? AND guild_id = ?`)
      .run(id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "reminder", id, "delete", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

// =====================================================================
// Sticky messages
// =====================================================================

interface StickyRow {
  id: string;
  guild_id: number;
  channel_id: number;
  enabled: number;
  name: string;
  content: string;
  embed_json: string | null;
  plain_text: number;
  ping_role_id: number | null;
  buttons_json: string;
  min_interval_s: number;
}

function toSticky(row: StickyRow): Sticky {
  const buttonsRaw = parseJson<unknown[]>(row.buttons_json, []);
  return {
    id: row.id,
    guildId: String(row.guild_id),
    channelId: String(row.channel_id),
    enabled: row.enabled === 1,
    name: row.name ?? "",
    content: row.content ?? "",
    embed: embedFromJson(parseJson<Record<string, unknown>>(row.embed_json, {})),
    plainText: row.plain_text === 1,
    pingRoleId: row.ping_role_id != null ? String(row.ping_role_id) : null,
    buttons: buttonsFromJson(buttonsRaw),
    reactionRole: reactionRoleFromRawList(buttonsRaw),
    minIntervalSeconds: row.min_interval_s ?? 30,
  };
}

// A sticky keeps its role button inside buttons_json (the migration wrote it
// there); pull the first role-shaped entry back out for the editor.
function reactionRoleFromRawList(raw: unknown): RoleButton | null {
  if (!Array.isArray(raw)) return null;
  const entry = raw.find(
    (b): b is Record<string, unknown> => !!b && typeof b === "object" && "role_id" in (b as object),
  );
  return entry ? reactionRoleFromJson(entry) : null;
}

export function listStickies(guildId: string): Sticky[] {
  const rows = open()
    .prepare(`SELECT * FROM sticky_messages WHERE guild_id = ? ORDER BY created_ts DESC`)
    .all(guildId) as StickyRow[];
  return rows.map(toSticky);
}

export function getSticky(guildId: string, id: string): Sticky | null {
  const row = open()
    .prepare(`SELECT * FROM sticky_messages WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as StickyRow | undefined;
  return row ? toSticky(row) : null;
}

export interface StickyInput {
  channelId: string;
  name: string;
  plainText: boolean;
  embed: Embed;
  content: string;
  pingRoleId: string | null;
  buttons: LinkButton[];
  reactionRole: RoleButton | null;
  minIntervalSeconds: number;
}

function stickyButtonsJson(input: StickyInput): string {
  const list: Record<string, unknown>[] = buttonsToJson(input.buttons);
  const role = reactionRoleToJson(input.reactionRole);
  if (role) list.unshift(role);
  return JSON.stringify(list);
}

export function createSticky(guildId: string, input: StickyInput, actorId: string): string | null {
  const db = open();
  // One sticky per channel — the table is keyed that way and the cog assumes it.
  const clash = db
    .prepare(`SELECT id FROM sticky_messages WHERE channel_id = ?`)
    .get(input.channelId);
  if (clash) return null;
  const id = newId();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO sticky_messages (
         id, guild_id, channel_id, enabled, name, content, embed_json,
         plain_text, ping_role_id, buttons_json, message_id, min_interval_s,
         last_posted_ts, created_ts
       ) VALUES (@id, @guild_id, @channel_id, 1, @name, @content, @embed_json,
         @plain_text, @ping_role_id, @buttons_json, NULL, @min_interval_s, 0, @ts)`,
    ).run({
      id,
      guild_id: guildId,
      channel_id: input.channelId,
      name: input.name.slice(0, 100),
      content: input.content.slice(0, 2000),
      embed_json: JSON.stringify(embedToJson(input.embed)),
      plain_text: input.plainText ? 1 : 0,
      ping_role_id: input.pingRoleId || null,
      buttons_json: stickyButtonsJson(input),
      min_interval_s: Math.max(0, Math.floor(input.minIntervalSeconds)),
      ts: now(),
    });
    audit(db, actorId, "sticky", id, "create", input.name);
    bumpRevision(db);
  })();
  return id;
}

export function updateSticky(guildId: string, id: string, input: StickyInput, actorId: string): boolean {
  const db = open();
  const clash = db
    .prepare(`SELECT id FROM sticky_messages WHERE channel_id = ? AND id != ?`)
    .get(input.channelId, id);
  if (clash) return false;
  const info = db.transaction(() => {
    const res = db
      .prepare(
        `UPDATE sticky_messages SET
           channel_id=@channel_id, name=@name, content=@content, embed_json=@embed_json,
           plain_text=@plain_text, ping_role_id=@ping_role_id, buttons_json=@buttons_json,
           min_interval_s=@min_interval_s
         WHERE id=@id AND guild_id=@guild_id`,
      )
      .run({
        id,
        guild_id: guildId,
        channel_id: input.channelId,
        name: input.name.slice(0, 100),
        content: input.content.slice(0, 2000),
        embed_json: JSON.stringify(embedToJson(input.embed)),
        plain_text: input.plainText ? 1 : 0,
        ping_role_id: input.pingRoleId || null,
        buttons_json: stickyButtonsJson(input),
        min_interval_s: Math.max(0, Math.floor(input.minIntervalSeconds)),
      });
    if (res.changes > 0) {
      audit(db, actorId, "sticky", id, "edit", input.name);
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function setStickyEnabled(guildId: string, id: string, enabled: boolean, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(`UPDATE sticky_messages SET enabled = ? WHERE id = ? AND guild_id = ?`)
      .run(enabled ? 1 : 0, id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "sticky", id, enabled ? "enable" : "disable", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function deleteSticky(guildId: string, id: string, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db.prepare(`DELETE FROM sticky_messages WHERE id = ? AND guild_id = ?`).run(id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "sticky", id, "delete", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

// =====================================================================
// Reaction rules
// =====================================================================

interface ReactionRow {
  id: string;
  guild_id: number;
  channel_id: number;
  enabled: number;
  scope: string;
  mode: string;
  role_ids: string;
  user_ids: string;
  emoji: string;
  bypass_role_ids: string;
  remove_after_s: number;
  max_reactions: number;
  include_threads: number;
}

function toReaction(row: ReactionRow): ReactionRule {
  return {
    id: row.id,
    guildId: String(row.guild_id),
    channelId: String(row.channel_id),
    enabled: row.enabled === 1,
    scope: row.scope as ReactionRule["scope"],
    mode: row.mode as ReactionRule["mode"],
    roleIds: (parseJson<unknown[]>(row.role_ids, []) as unknown[]).map(String),
    userIds: (parseJson<unknown[]>(row.user_ids, []) as unknown[]).map(String),
    emoji: parseJson<string[]>(row.emoji, []),
    bypassRoleIds: (parseJson<unknown[]>(row.bypass_role_ids, []) as unknown[]).map(String),
    removeAfterSeconds: row.remove_after_s ?? 0,
    maxReactions: row.max_reactions ?? 0,
    includeThreads: row.include_threads === 1,
  };
}

export function listReactionRules(guildId: string): ReactionRule[] {
  const rows = open()
    .prepare(`SELECT * FROM reaction_rules WHERE guild_id = ? ORDER BY created_ts DESC`)
    .all(guildId) as ReactionRow[];
  return rows.map(toReaction);
}

export function getReactionRule(guildId: string, id: string): ReactionRule | null {
  const row = open()
    .prepare(`SELECT * FROM reaction_rules WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as ReactionRow | undefined;
  return row ? toReaction(row) : null;
}

export interface ReactionInput {
  channelId: string;
  scope: ReactionRule["scope"];
  mode: ReactionRule["mode"];
  roleIds: string[];
  userIds: string[];
  emoji: string[];
  bypassRoleIds: string[];
  removeAfterSeconds: number;
  maxReactions: number;
  includeThreads: boolean;
}

function reactionColumns(input: ReactionInput) {
  return {
    channel_id: input.channelId,
    scope: input.scope,
    mode: input.mode,
    role_ids: JSON.stringify(input.roleIds),
    user_ids: JSON.stringify(input.userIds),
    emoji: JSON.stringify(input.emoji),
    bypass_role_ids: JSON.stringify(input.bypassRoleIds),
    remove_after_s: Math.max(0, Math.floor(input.removeAfterSeconds)),
    max_reactions: Math.max(0, Math.floor(input.maxReactions)),
    include_threads: input.includeThreads ? 1 : 0,
  };
}

export function createReactionRule(guildId: string, input: ReactionInput, actorId: string): string {
  const db = open();
  const id = newId();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO reaction_rules (
         id, guild_id, channel_id, enabled, scope, mode, role_ids, user_ids,
         emoji, bypass_role_ids, remove_after_s, max_reactions, include_threads, created_ts
       ) VALUES (@id, @guild_id, @channel_id, 1, @scope, @mode, @role_ids, @user_ids,
         @emoji, @bypass_role_ids, @remove_after_s, @max_reactions, @include_threads, @ts)`,
    ).run({ id, guild_id: guildId, ts: now(), ...reactionColumns(input) });
    audit(db, actorId, "reaction_rule", id, "create", "");
    bumpRevision(db);
  })();
  return id;
}

export function updateReactionRule(guildId: string, id: string, input: ReactionInput, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(
        `UPDATE reaction_rules SET
           channel_id=@channel_id, scope=@scope, mode=@mode, role_ids=@role_ids,
           user_ids=@user_ids, emoji=@emoji, bypass_role_ids=@bypass_role_ids,
           remove_after_s=@remove_after_s, max_reactions=@max_reactions,
           include_threads=@include_threads
         WHERE id=@id AND guild_id=@guild_id`,
      )
      .run({ id, guild_id: guildId, ...reactionColumns(input) });
    if (res.changes > 0) {
      audit(db, actorId, "reaction_rule", id, "edit", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function setReactionEnabled(guildId: string, id: string, enabled: boolean, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(`UPDATE reaction_rules SET enabled = ? WHERE id = ? AND guild_id = ?`)
      .run(enabled ? 1 : 0, id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "reaction_rule", id, enabled ? "enable" : "disable", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function deleteReactionRule(guildId: string, id: string, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db.prepare(`DELETE FROM reaction_rules WHERE id = ? AND guild_id = ?`).run(id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "reaction_rule", id, "delete", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

// =====================================================================
// Media-only channels
// =====================================================================

interface MediaRow {
  id: string;
  guild_id: number;
  channel_id: number;
  enabled: number;
  allow_attachments: number;
  allow_links: number;
  allow_embeds: number;
  allow_stickers: number;
  bypass_role_ids: string;
  thread_enabled: number;
  thread_name_template: string;
  thread_archive_minutes: number;
  auto_react: string;
  post_cooldown_s: number;
  dm_on_delete: number;
}

function toMedia(row: MediaRow): MediaChannel {
  return {
    id: row.id,
    guildId: String(row.guild_id),
    channelId: String(row.channel_id),
    enabled: row.enabled === 1,
    allowAttachments: row.allow_attachments === 1,
    allowLinks: row.allow_links === 1,
    allowEmbeds: row.allow_embeds === 1,
    allowStickers: row.allow_stickers === 1,
    bypassRoleIds: (parseJson<unknown[]>(row.bypass_role_ids, []) as unknown[]).map(String),
    threadEnabled: row.thread_enabled === 1,
    threadNameTemplate: row.thread_name_template ?? "{user} - {date}",
    threadArchiveMinutes: row.thread_archive_minutes ?? 60,
    autoReact: parseJson<string[]>(row.auto_react, []),
    postCooldownSeconds: row.post_cooldown_s ?? 0,
    dmOnDelete: row.dm_on_delete === 1,
  };
}

export function listMediaChannels(guildId: string): MediaChannel[] {
  const rows = open()
    .prepare(`SELECT * FROM media_channels WHERE guild_id = ? ORDER BY created_ts DESC`)
    .all(guildId) as MediaRow[];
  return rows.map(toMedia);
}

export function getMediaChannel(guildId: string, id: string): MediaChannel | null {
  const row = open()
    .prepare(`SELECT * FROM media_channels WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as MediaRow | undefined;
  return row ? toMedia(row) : null;
}

export interface MediaInput {
  channelId: string;
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

function mediaColumns(input: MediaInput) {
  return {
    channel_id: input.channelId,
    allow_attachments: input.allowAttachments ? 1 : 0,
    allow_links: input.allowLinks ? 1 : 0,
    allow_embeds: input.allowEmbeds ? 1 : 0,
    allow_stickers: input.allowStickers ? 1 : 0,
    bypass_role_ids: JSON.stringify(input.bypassRoleIds),
    thread_enabled: input.threadEnabled ? 1 : 0,
    thread_name_template: input.threadNameTemplate.slice(0, 100),
    thread_archive_minutes: input.threadArchiveMinutes,
    auto_react: JSON.stringify(input.autoReact),
    post_cooldown_s: Math.max(0, Math.floor(input.postCooldownSeconds)),
    dm_on_delete: input.dmOnDelete ? 1 : 0,
  };
}

export function createMediaChannel(guildId: string, input: MediaInput, actorId: string): string | null {
  const db = open();
  const clash = db.prepare(`SELECT id FROM media_channels WHERE channel_id = ?`).get(input.channelId);
  if (clash) return null;
  const id = newId();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO media_channels (
         id, guild_id, channel_id, enabled, allow_attachments, allow_links, allow_embeds,
         allow_stickers, bypass_role_ids, thread_enabled, thread_name_template,
         thread_archive_minutes, auto_react, post_cooldown_s, dm_on_delete, created_ts
       ) VALUES (@id, @guild_id, @channel_id, 1, @allow_attachments, @allow_links, @allow_embeds,
         @allow_stickers, @bypass_role_ids, @thread_enabled, @thread_name_template,
         @thread_archive_minutes, @auto_react, @post_cooldown_s, @dm_on_delete, @ts)`,
    ).run({ id, guild_id: guildId, ts: now(), ...mediaColumns(input) });
    audit(db, actorId, "media_channel", id, "create", "");
    bumpRevision(db);
  })();
  return id;
}

export function updateMediaChannel(guildId: string, id: string, input: MediaInput, actorId: string): boolean {
  const db = open();
  const clash = db
    .prepare(`SELECT id FROM media_channels WHERE channel_id = ? AND id != ?`)
    .get(input.channelId, id);
  if (clash) return false;
  const info = db.transaction(() => {
    const res = db
      .prepare(
        `UPDATE media_channels SET
           channel_id=@channel_id, allow_attachments=@allow_attachments, allow_links=@allow_links,
           allow_embeds=@allow_embeds, allow_stickers=@allow_stickers, bypass_role_ids=@bypass_role_ids,
           thread_enabled=@thread_enabled, thread_name_template=@thread_name_template,
           thread_archive_minutes=@thread_archive_minutes, auto_react=@auto_react,
           post_cooldown_s=@post_cooldown_s, dm_on_delete=@dm_on_delete
         WHERE id=@id AND guild_id=@guild_id`,
      )
      .run({ id, guild_id: guildId, ...mediaColumns(input) });
    if (res.changes > 0) {
      audit(db, actorId, "media_channel", id, "edit", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function setMediaEnabled(guildId: string, id: string, enabled: boolean, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(`UPDATE media_channels SET enabled = ? WHERE id = ? AND guild_id = ?`)
      .run(enabled ? 1 : 0, id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "media_channel", id, enabled ? "enable" : "disable", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function deleteMediaChannel(guildId: string, id: string, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db.prepare(`DELETE FROM media_channels WHERE id = ? AND guild_id = ?`).run(id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, "media_channel", id, "delete", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}
