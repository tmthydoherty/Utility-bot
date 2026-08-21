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
 * The live wiring for the Economy and Leveling modules.
 *
 * Unlike every other module — which drafts into the dashboard's own database
 * because it has no consumer yet — these two write to a small SQLite file the
 * economy cog polls, so a save here reaches the bot within about ten seconds.
 * It is the same deliberate exception, and the same mechanism, as
 * lib/automations/store.ts; read that file's header for the full reasoning.
 *
 * The division of labour with the bot (cogs/economy/config_sync.py):
 *
 * * `config`    — the bot's current settings, republished after every change,
 *                 so reads here reflect reality including `/economy_panel` edits.
 * * `overrides` — desired values written here; the bot applies them and they
 *                 then agree with the snapshot.
 * * `meta`      — a `revision` this bumps on write and an `applied` the bot
 *                 advances once it has caught up. That counter is the whole
 *                 synchronisation protocol; there is no socket and no endpoint.
 *
 * The file is never created here. If it is missing the bot has not started, and
 * conjuring an empty one would give the dashboard a database the engine never
 * reads — so reads fall back to schema defaults and writes fail loudly.
 */

const DB_PATH =
  env.VIBEY_ECONOMY_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "economy_config", "economy_config.db");

/** The modules backed by the bot rather than the draft store. */
const BOT_BACKED = new Set(["economy", "leveling"]);

export function isBotBacked(moduleId: string): boolean {
  return BOT_BACKED.has(moduleId);
}

let instance: Database.Database | null = null;

export class EconomyConfigUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's economy config database could not be opened.");
    this.name = "EconomyConfigUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;

  if (!fs.existsSync(DB_PATH)) {
    throw new EconomyConfigUnavailable(new Error(`no database at ${DB_PATH}`));
  }

  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  // Matches the bot's own timeout, so a save waits out a bot-side write rather
  // than failing while the cog finishes one.
  db.pragma("busy_timeout = 5000");

  instance = db;
  return db;
}

/** True when the bot's config store is reachable — drives the live badge. */
export function economyStoreAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM meta LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

/**
 * Decode a snapshot value — stored the way the Python side stringifies it — into
 * the typed shape the form expects. Overrides skip this: they were written here
 * as the dashboard's own JSON and come back through `parseJsonPreservingIds`.
 */
function coerceSnapshot(field: Field, raw: string): FieldValue {
  switch (field.type) {
    case FieldType.BOOL:
      // Mirrors Config.get_bool: anything but these reads as true.
      return !["0", "false", "False", ""].includes(raw);

    case FieldType.NUMBER:
    case FieldType.DURATION: {
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : (field.min ?? 0);
    }

    case FieldType.CHANNEL:
    case FieldType.ROLE:
    case FieldType.USER:
      if (field.multi) {
        const list = parseJsonPreservingIds<unknown[]>(raw, []);
        return Array.isArray(list) ? list.map(String) : [];
      }
      // A single ID is stored as a bare integer; 0 is the unset placeholder.
      // Kept as the raw digit string so a snowflake never loses precision.
      return raw && raw !== "0" ? raw : null;

    default:
      // TEXT, MULTILINE, EMOJI, CHOICE — already a plain string.
      return raw;
  }
}

/** A module's values: schema defaults, under the bot snapshot, under overrides. */
export function readEconomyModuleValues(module: ModuleSchema): SettingsValues {
  const values = defaultsFor(module);

  let db: Database.Database;
  try {
    db = open();
  } catch {
    // Bot not up yet. Defaults are the honest answer, and the page still loads.
    return values;
  }

  const fields = allFields(module);
  const keys = fields.map((field) => field.key);
  const placeholders = keys.map(() => "?").join(",");

  const snapshot = new Map<string, string>();
  for (const row of db
    .prepare(`SELECT key, value FROM config WHERE key IN (${placeholders})`)
    .all(...keys) as { key: string; value: string }[]) {
    snapshot.set(row.key, row.value);
  }

  const overrides = new Map<string, string>();
  for (const row of db
    .prepare(`SELECT key, value FROM overrides WHERE key IN (${placeholders})`)
    .all(...keys) as { key: string; value: string }[]) {
    overrides.set(row.key, row.value);
  }

  for (const field of fields) {
    if (overrides.has(field.key)) {
      values[field.key] = parseJsonPreservingIds<FieldValue>(
        overrides.get(field.key)!,
        values[field.key] ?? null,
      );
    } else if (snapshot.has(field.key)) {
      values[field.key] = coerceSnapshot(field, snapshot.get(field.key)!);
    }
  }

  return values;
}

function nowSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

/** One statement, so a bump from the bot side between read and write is safe. */
function bumpRevision(db: Database.Database): void {
  db.prepare(
    `INSERT INTO meta (key, value) VALUES ('revision', '1')
     ON CONFLICT(key) DO UPDATE SET
       value = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT)`,
  ).run();
}

/**
 * Record the desired values and signal the bot.
 *
 * Every field is upserted, not only the changed ones, so the overrides table is
 * always a complete statement of what the site is asserting — the bot converges
 * economy.db to exactly what the admin last saw and saved. Throws
 * `EconomyConfigUnavailable` if the bot has not created the store yet.
 */
export function writeEconomyModuleValues(
  module: ModuleSchema,
  values: SettingsValues,
  actorId: string,
): void {
  const db = open();
  const fields = allFields(module);
  const timestamp = nowSeconds();

  const upsert = db.prepare(
    `INSERT INTO overrides (key, value, updated_ts, updated_by)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(key) DO UPDATE SET
       value = excluded.value, updated_ts = excluded.updated_ts, updated_by = excluded.updated_by`,
  );

  db.transaction(() => {
    for (const field of fields) {
      if (!(field.key in values)) continue;
      upsert.run(field.key, JSON.stringify(values[field.key] ?? null), timestamp, actorId);
    }
    bumpRevision(db);
  })();
}

/** When this module's settings last moved, for the "last changed" line. */
export function readEconomyMeta(module: ModuleSchema): { updatedAt: number } | null {
  let db: Database.Database;
  try {
    db = open();
  } catch {
    return null;
  }

  const keys = allFields(module).map((field) => field.key);
  const placeholders = keys.map(() => "?").join(",");
  const row = db
    .prepare(`SELECT MAX(updated_ts) AS ts FROM overrides WHERE key IN (${placeholders})`)
    .get(...keys) as { ts: number | null } | undefined;

  return row?.ts ? { updatedAt: row.ts * 1000 } : null;
}
