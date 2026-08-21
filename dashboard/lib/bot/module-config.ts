import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";
import {
  FieldType,
  allFields,
  defaultsFor,
  type Field,
  type FieldValue,
  type ModuleSchema,
  type SettingsValues,
} from "@/lib/schema/types";

/**
 * Live wiring for the per-guild cog modules — welcome, security, suggestions.
 *
 * These used to draft into the dashboard's own database because they had no
 * consumer; now they write to a shared SQLite bridge each cog polls, so a save
 * here reaches the bot within about ten seconds. It is the same mechanism as
 * lib/bot/economy-config.ts, generalised: the economy config is global, but
 * these settings are per-guild, so every row is keyed by `(module, guild, key)`.
 *
 * The division of labour with the bot (utils/module_config_sync.py):
 *
 * * `config`    — each cog's current settings, republished after every change
 *                 (web or Discord panel), so reads here reflect reality.
 * * `overrides` — desired values written here; the cog applies them, then clears
 *                 them, so `config` is the single source of truth between saves.
 * * `meta`      — a per-module `revision:<module>` this bumps on write and the
 *                 cog's `applied:<module>`. That counter is the whole protocol.
 *
 * The values on both sides are JSON, and IDs are read back through
 * `parseJsonPreservingIds` so a snowflake past 2^53 never loses its low digits.
 * The file is never created here: if it's missing the bot hasn't started, so
 * reads fall back to schema defaults and writes fail loudly.
 */

const DB_PATH =
  env.VIBEY_MODULE_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "module_config", "module_config.db");

/** The modules backed by the shared per-guild bridge rather than the draft store. */
const SYNCED = new Set(["welcome", "security", "qotd", "bot-presence", "nhie", "game-poll"]);

export function isModuleSynced(moduleId: string): boolean {
  return SYNCED.has(moduleId);
}

/**
 * Modules whose settings are bot-wide, not per-server. Discord gives a bot one
 * presence across every server, so Bot Presence is stored under a single fixed
 * scope — every guild's dashboard reads and writes the same shared row, and the
 * cog publishes its snapshot under the same scope. `"0"` is never a real guild id.
 */
const GLOBAL = new Set(["bot-presence", "game-poll"]);
const GLOBAL_SCOPE = "0";

/** The guild id to key a module's rows under — the real guild, or the global scope. */
function scopeFor(moduleId: string, guildId: string): string {
  return GLOBAL.has(moduleId) ? GLOBAL_SCOPE : guildId;
}

let instance: Database.Database | null = null;

export class ModuleConfigUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's module config database could not be opened.");
    this.name = "ModuleConfigUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;

  if (!fs.existsSync(DB_PATH)) {
    throw new ModuleConfigUnavailable(new Error(`no database at ${DB_PATH}`));
  }

  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");

  instance = db;
  return db;
}

/** True when the shared bridge is reachable — drives the live badge. */
export function moduleConfigStoreAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM meta LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

/**
 * Normalise a bridge value — already JSON-parsed, IDs preserved as strings — into
 * the typed shape the form expects for this field.
 */
function coerce(field: Field, parsed: FieldValue): FieldValue {
  switch (field.type) {
    case FieldType.BOOL:
      return Boolean(parsed);

    case FieldType.NUMBER:
    case FieldType.DURATION: {
      const n = Number(parsed);
      return Number.isFinite(n) ? n : (field.min ?? 0);
    }

    case FieldType.CHANNEL:
    case FieldType.ROLE:
    case FieldType.USER:
      if (field.multi) {
        return Array.isArray(parsed) ? parsed.map(String) : [];
      }
      return parsed == null || parsed === 0 || parsed === "0" ? null : String(parsed);

    default:
      return parsed == null ? "" : String(parsed);
  }
}

/** A module's values for one guild: defaults, under the snapshot, under overrides. */
export function readModuleValues(module: ModuleSchema, guildId: string): SettingsValues {
  const values = defaultsFor(module);
  const scope = scopeFor(module.id, guildId);

  let db: Database.Database;
  try {
    db = open();
  } catch {
    // Bot not up yet. Defaults are the honest answer, and the page still loads.
    return values;
  }

  const fields = allFields(module);
  const keys = fields.map((field) => field.key);
  if (keys.length === 0) return values;
  const placeholders = keys.map(() => "?").join(",");

  const read = (table: "config" | "overrides") => {
    const map = new Map<string, string>();
    for (const row of db
      .prepare(
        `SELECT key, value FROM ${table} WHERE module = ? AND guild_id = ? AND key IN (${placeholders})`,
      )
      .all(module.id, scope, ...keys) as { key: string; value: string }[]) {
      map.set(row.key, row.value);
    }
    return map;
  };

  const snapshot = read("config");
  const overrides = read("overrides");

  for (const field of fields) {
    const raw = overrides.get(field.key) ?? snapshot.get(field.key);
    if (raw !== undefined) {
      values[field.key] = coerce(
        field,
        parseJsonPreservingIds<FieldValue>(raw, values[field.key] ?? null),
      );
    }
  }

  return values;
}

/**
 * Record desired values for one guild and signal the owning cog. Every field is
 * upserted so the overrides are a complete statement of what the site asserts.
 * Throws `ModuleConfigUnavailable` if the bot has not created the store yet.
 */
export function writeModuleValues(
  module: ModuleSchema,
  guildId: string,
  values: SettingsValues,
  actorId: string,
): void {
  const db = open();
  const scope = scopeFor(module.id, guildId);
  const fields = allFields(module);
  const now = Date.now();

  const upsert = db.prepare(
    `INSERT INTO overrides (module, guild_id, key, value, updated_ts, updated_by)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(module, guild_id, key) DO UPDATE SET
       value = excluded.value, updated_ts = excluded.updated_ts, updated_by = excluded.updated_by`,
  );
  const bumpRevision = db.prepare(
    `INSERT INTO meta (key, value) VALUES (?, '1')
     ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT)`,
  );
  // Overrides are cleared once the bot applies them, so the "last changed" line
  // reads from a meta key that survives instead.
  const setUpdated = db.prepare(
    `INSERT INTO meta (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
  );

  db.transaction(() => {
    for (const field of fields) {
      if (!(field.key in values)) continue;
      upsert.run(
        module.id,
        scope,
        field.key,
        JSON.stringify(values[field.key] ?? null),
        Math.floor(now / 1000),
        actorId,
      );
    }
    bumpRevision.run(`revision:${module.id}`);
    setUpdated.run(`updated:${module.id}:${scope}`, String(now));
  })();
}

/** When this module's settings last moved for this guild, for the "last changed" line. */
export function readModuleMeta(
  module: ModuleSchema,
  guildId: string,
): { updatedAt: number } | null {
  let db: Database.Database;
  try {
    db = open();
  } catch {
    return null;
  }

  const row = db
    .prepare(`SELECT value FROM meta WHERE key = ?`)
    .get(`updated:${module.id}:${scopeFor(module.id, guildId)}`) as { value: string } | undefined;

  const ms = row ? Number(row.value) : NaN;
  return Number.isFinite(ms) ? { updatedAt: ms } : null;
}
