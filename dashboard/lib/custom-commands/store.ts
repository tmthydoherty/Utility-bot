import "server-only";

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";
import { embedFromJson, embedToJson } from "@/lib/utility/types";
import type {
  CooldownScope,
  CustomCommand,
  Delivery,
  GifCommand,
  ListEntry,
  MatchType,
} from "./types";

/**
 * The bot's Custom Commands database, opened directly.
 *
 * The same deliberate exception the Utility and Automations builders make: the
 * dashboard writes the cog's live database and bumps a `revision` the cog
 * watches, so a change here is live within ten seconds. The worst a bad write
 * can do is create a command, which is created switched off.
 *
 * The economy-purchased GIF commands live in a different database — economy.db —
 * which this reads strictly read-only (the dashboard sandbox cannot write it).
 * Disabling or deleting one is therefore not a direct write: it is queued in
 * this database's `gif_moderation` table for the cog to apply, since only the
 * cog can touch economy.db.
 */
const DB_PATH =
  env.VIBEY_CUSTOM_COMMANDS_DB ??
  path.join(process.cwd(), "..", "data", "custom_commands", "custom_commands.db");

const ECONOMY_DB_PATH = env.VIBEY_ECONOMY_DB ?? path.join(process.cwd(), "..", "economy.db");

let instance: Database.Database | null = null;
let economyInstance: Database.Database | null = null;

export class CustomCommandsUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's custom commands database could not be opened.");
    this.name = "CustomCommandsUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;
  // Never created here: a missing file means the bot has not started, and a
  // fresh empty database would be one the cog never reads.
  if (!fs.existsSync(DB_PATH)) {
    throw new CustomCommandsUnavailable(new Error(`no database at ${DB_PATH}`));
  }
  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  instance = db;
  return db;
}

/** economy.db, read-only — the source of the purchased GIF commands. */
function openEconomy(): Database.Database | null {
  if (economyInstance) return economyInstance;
  if (!fs.existsSync(ECONOMY_DB_PATH)) return null;
  try {
    const db = new Database(ECONOMY_DB_PATH, { readonly: true, fileMustExist: true });
    db.pragma("busy_timeout = 5000");
    economyInstance = db;
    return db;
  } catch {
    return null;
  }
}

export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM commands LIMIT 1").get();
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

/** Normalise a trigger to the `[a-z0-9_]` a command name may hold, no `!`. */
export function normalizeName(raw: string): string {
  return raw.trim().toLowerCase().replace(/^!+/, "").replace(/[^a-z0-9_]/g, "");
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
  entityId: string,
  action: string,
  detail: string,
): void {
  db.prepare(
    `INSERT INTO audit (ts, user_id, entity, entity_id, action, detail)
     VALUES (?, ?, 'command', ?, ?, ?)`,
  ).run(now(), actorId, entityId, action, detail.slice(0, 500));
}

// =====================================================================
// Custom commands
// =====================================================================

interface CommandRow {
  id: string;
  guild_id: number;
  name: string;
  enabled: number;
  match_type: string;
  responses_json: string;
  plain_text: number;
  embed_json: string | null;
  delivery: string;
  delete_trigger: number;
  react_emoji: string;
  allowed_role_ids: string;
  denied_role_ids: string;
  allowed_channel_ids: string;
  denied_channel_ids: string;
  cooldown_s: number;
  cooldown_scope: string;
  use_count: number;
  last_used_ts: number;
  created_ts: number;
  updated_ts: number;
}

function toCommand(row: CommandRow): CustomCommand {
  return {
    id: row.id,
    guildId: String(row.guild_id),
    name: row.name,
    enabled: row.enabled === 1,
    matchType: (row.match_type as MatchType) ?? "exact",
    responses: parseJson<string[]>(row.responses_json, []).map(String),
    plainText: row.plain_text === 1,
    embed: embedFromJson(parseJson<Record<string, unknown>>(row.embed_json, {})),
    delivery: (row.delivery as Delivery) ?? "channel",
    deleteTrigger: row.delete_trigger === 1,
    reactEmoji: row.react_emoji ?? "",
    allowedRoleIds: parseJson<unknown[]>(row.allowed_role_ids, []).map(String),
    deniedRoleIds: parseJson<unknown[]>(row.denied_role_ids, []).map(String),
    allowedChannelIds: parseJson<unknown[]>(row.allowed_channel_ids, []).map(String),
    deniedChannelIds: parseJson<unknown[]>(row.denied_channel_ids, []).map(String),
    cooldownSeconds: row.cooldown_s ?? 0,
    cooldownScope: (row.cooldown_scope as CooldownScope) ?? "user",
    useCount: row.use_count ?? 0,
    lastUsedAt: row.last_used_ts ?? 0,
    createdAt: row.created_ts ?? 0,
    updatedAt: row.updated_ts ?? 0,
  };
}

export interface CommandInput {
  name: string;
  matchType: MatchType;
  responses: string[];
  plainText: boolean;
  embed: CustomCommand["embed"];
  delivery: Delivery;
  deleteTrigger: boolean;
  reactEmoji: string;
  allowedRoleIds: string[];
  deniedRoleIds: string[];
  allowedChannelIds: string[];
  deniedChannelIds: string[];
  cooldownSeconds: number;
  cooldownScope: CooldownScope;
}

function commandColumns(input: CommandInput) {
  const embedJson = JSON.stringify(embedToJson(input.embed));
  return {
    name: normalizeName(input.name),
    match_type: input.matchType,
    responses_json: JSON.stringify(input.responses.map((r) => r.slice(0, 2000)).slice(0, 25)),
    plain_text: input.plainText ? 1 : 0,
    // Store null rather than "{}" when there's nothing, so the cog's "has an
    // embed" test is a simple truthiness check.
    embed_json: embedJson === "{}" ? null : embedJson,
    delivery: input.delivery,
    delete_trigger: input.deleteTrigger ? 1 : 0,
    react_emoji: input.reactEmoji.slice(0, 64),
    allowed_role_ids: JSON.stringify(input.allowedRoleIds),
    denied_role_ids: JSON.stringify(input.deniedRoleIds),
    allowed_channel_ids: JSON.stringify(input.allowedChannelIds),
    denied_channel_ids: JSON.stringify(input.deniedChannelIds),
    cooldown_s: Math.max(0, Math.floor(input.cooldownSeconds)),
    cooldown_scope: input.cooldownScope,
  };
}

export function listCommands(guildId: string): CustomCommand[] {
  const rows = open()
    .prepare(`SELECT * FROM commands WHERE guild_id = ? ORDER BY name ASC`)
    .all(guildId) as CommandRow[];
  return rows.map(toCommand);
}

export function getCommand(guildId: string, id: string): CustomCommand | null {
  const row = open()
    .prepare(`SELECT * FROM commands WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as CommandRow | undefined;
  return row ? toCommand(row) : null;
}

/** True if a command with this name already exists (optionally excluding one id). */
export function nameTaken(guildId: string, name: string, exceptId?: string): boolean {
  const row = open()
    .prepare(`SELECT id FROM commands WHERE guild_id = ? AND name = ?`)
    .get(guildId, normalizeName(name)) as { id: string } | undefined;
  return !!row && row.id !== exceptId;
}

/** True if a purchased GIF command already owns this name. */
export function gifNameExists(name: string): boolean {
  const db = openEconomy();
  if (!db) return false;
  try {
    const row = db
      .prepare(`SELECT name FROM gif_commands WHERE name = ?`)
      .get(normalizeName(name)) as { name: string } | undefined;
    return !!row;
  } catch {
    return false;
  }
}

export function createCommand(guildId: string, input: CommandInput, actorId: string): string {
  const db = open();
  const id = newId();
  const ts = now();
  const cols = commandColumns(input);
  db.transaction(() => {
    db.prepare(
      `INSERT INTO commands (
         id, guild_id, name, enabled, match_type, responses_json, plain_text, embed_json,
         delivery, delete_trigger, react_emoji, allowed_role_ids, denied_role_ids,
         allowed_channel_ids, denied_channel_ids, cooldown_s, cooldown_scope,
         use_count, last_used_ts, created_ts, updated_ts
       ) VALUES (
         @id, @guild_id, @name, 0, @match_type, @responses_json, @plain_text, @embed_json,
         @delivery, @delete_trigger, @react_emoji, @allowed_role_ids, @denied_role_ids,
         @allowed_channel_ids, @denied_channel_ids, @cooldown_s, @cooldown_scope,
         0, 0, @ts, @ts
       )`,
    ).run({ id, guild_id: guildId, ts, ...cols });
    audit(db, actorId, id, "create", cols.name);
    bumpRevision(db);
  })();
  return id;
}

export function updateCommand(
  guildId: string,
  id: string,
  input: CommandInput,
  actorId: string,
): boolean {
  const db = open();
  const cols = commandColumns(input);
  const info = db.transaction(() => {
    const res = db
      .prepare(
        `UPDATE commands SET
           name=@name, match_type=@match_type, responses_json=@responses_json,
           plain_text=@plain_text, embed_json=@embed_json, delivery=@delivery,
           delete_trigger=@delete_trigger, react_emoji=@react_emoji,
           allowed_role_ids=@allowed_role_ids, denied_role_ids=@denied_role_ids,
           allowed_channel_ids=@allowed_channel_ids, denied_channel_ids=@denied_channel_ids,
           cooldown_s=@cooldown_s, cooldown_scope=@cooldown_scope, updated_ts=@ts
         WHERE id=@id AND guild_id=@guild_id`,
      )
      .run({ id, guild_id: guildId, ts: now(), ...cols });
    if (res.changes > 0) {
      audit(db, actorId, id, "edit", cols.name);
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function setCommandEnabled(
  guildId: string,
  id: string,
  enabled: boolean,
  actorId: string,
): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db
      .prepare(`UPDATE commands SET enabled = ?, updated_ts = ? WHERE id = ? AND guild_id = ?`)
      .run(enabled ? 1 : 0, now(), id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, id, enabled ? "enable" : "disable", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

export function deleteCommand(guildId: string, id: string, actorId: string): boolean {
  const db = open();
  const info = db.transaction(() => {
    const res = db.prepare(`DELETE FROM commands WHERE id = ? AND guild_id = ?`).run(id, guildId);
    if (res.changes > 0) {
      audit(db, actorId, id, "delete", "");
      bumpRevision(db);
    }
    return res;
  })();
  return info.changes > 0;
}

// =====================================================================
// Economy-purchased GIF commands (read from economy.db, moderated via a queue)
// =====================================================================

interface GifRow {
  name: string;
  user_id: number;
  url: string;
  approved_ts: number;
  disabled: number | null;
}

export function listGifCommands(): GifCommand[] {
  const db = openEconomy();
  if (!db) return [];
  let rows: GifRow[];
  try {
    rows = db
      .prepare(
        `SELECT name, user_id, url, approved_ts, COALESCE(disabled, 0) AS disabled
         FROM gif_commands ORDER BY name ASC`,
      )
      .all() as GifRow[];
  } catch {
    // A very old economy.db without the `disabled` column, or no table yet.
    try {
      rows = (
        db.prepare(`SELECT name, user_id, url, approved_ts FROM gif_commands ORDER BY name ASC`).all() as GifRow[]
      ).map((r) => ({ ...r, disabled: 0 }));
    } catch {
      return [];
    }
  }

  const pending = pendingModeration();
  return rows.map((r) => ({
    name: r.name,
    ownerId: String(r.user_id),
    url: r.url,
    approvedAt: r.approved_ts ?? 0,
    disabled: (r.disabled ?? 0) === 1,
    pending: pending.get(r.name) ?? null,
  }));
}

/** The GIF-moderation intents the cog hasn't applied yet, by command name. */
function pendingModeration(): Map<string, GifCommand["pending"]> {
  const map = new Map<string, GifCommand["pending"]>();
  try {
    const rows = open().prepare(`SELECT name, action FROM gif_moderation`).all() as {
      name: string;
      action: string;
    }[];
    for (const row of rows) {
      if (row.action === "disable" || row.action === "enable" || row.action === "delete") {
        map.set(row.name, row.action);
      }
    }
  } catch {
    // custom_commands.db not reachable — no pending state to overlay.
  }
  return map;
}

export type GifModerationAction = "disable" | "enable" | "delete";

/**
 * Queue a moderation action against a purchased GIF for the cog to apply. The
 * name is validated against economy.db first, so a stale row can't queue an
 * action for a command that no longer exists. Returns false if unknown.
 */
export function moderateGif(name: string, action: GifModerationAction, actorId: string): boolean {
  const normalized = normalizeName(name);
  if (!gifNameExists(normalized)) return false;
  const db = open();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO gif_moderation (name, action, updated_ts, updated_by)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(name) DO UPDATE SET
         action = excluded.action, updated_ts = excluded.updated_ts, updated_by = excluded.updated_by`,
    ).run(normalized, action, now(), actorId);
    audit(db, actorId, `gif:${normalized}`, `gif.${action}`, normalized);
    bumpRevision(db);
  })();
  return true;
}

// =====================================================================
// The unified, alphabetised list
// =====================================================================

/** Every command — admin-authored and purchased GIF — sorted by name. */
export function listAll(guildId: string): ListEntry[] {
  const custom: ListEntry[] = listCommands(guildId).map((command) => ({ kind: "custom", command }));
  const gifs: ListEntry[] = listGifCommands().map((gif) => ({ kind: "gif", gif }));
  return [...custom, ...gifs].sort((a, b) => nameOf(a).localeCompare(nameOf(b)));
}

function nameOf(entry: ListEntry): string {
  return entry.kind === "custom" ? entry.command.name : entry.gif.name;
}
