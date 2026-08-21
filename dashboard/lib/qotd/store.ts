import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";

/**
 * Live wiring for the Question of the Day question pool.
 *
 * QOTD's flat settings (schedule, channels, ping role) travel through the shared
 * per-guild module-config bridge like welcome/security/suggestions. The question
 * *pool*, though, is a list of records with add/edit/delete semantics a flat
 * key/value bridge can't express, so it has its own small SQLite bridge — the
 * same mechanism and shape as `lib/ticketing/store.ts`: a `snapshot` the bot
 * republishes every tick (the whole pool, each guild's queued question, the
 * counts) and a `commands` queue this appends to and the cog drains
 * (`cogs/qotd_config_sync.py`).
 *
 * The file is never created here: a missing file means the bot has not started,
 * so reads return an empty, "unavailable" pool and writes fail loudly.
 */

const DB_PATH =
  env.VIBEY_QOTD_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "qotd_config", "qotd_config.db");

let instance: Database.Database | null = null;

export class QotdUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's QOTD config database could not be opened.");
    this.name = "QotdUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;
  if (!fs.existsSync(DB_PATH)) {
    throw new QotdUnavailable(new Error(`no database at ${DB_PATH}`));
  }
  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  instance = db;
  return db;
}

/** True when the bridge is reachable — drives the panel's "bot is offline" state. */
export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM snapshot LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

// ------------------------------------------------------------------- reading

export interface QotdQuestion {
  id: number;
  text: string;
  /** The submitter/adder, if the pool recorded one. A snowflake string. */
  addedById: string | null;
  timesUsed: number;
  /** Already asked at least once this cycle — hidden from the next post. */
  seen: boolean;
}

export interface QotdTomorrow {
  id: number;
  text: string;
}

export interface QotdPool {
  questions: QotdQuestion[];
  /** The question queued to post next for this guild, if any. */
  tomorrow: QotdTomorrow | null;
  total: number;
  unseen: number;
}

function snapshotValue(db: Database.Database, key: string): unknown {
  const row = db.prepare(`SELECT value FROM snapshot WHERE key = ?`).get(key) as
    | { value: string }
    | undefined;
  return row ? parseJsonPreservingIds<unknown>(row.value, null) : null;
}

const EMPTY_POOL: QotdPool = { questions: [], tomorrow: null, total: 0, unseen: 0 };

export function readPool(guildId: string): QotdPool {
  let db: Database.Database;
  try {
    db = open();
  } catch {
    return EMPTY_POOL;
  }

  const rawQuestions = snapshotValue(db, "questions");
  const questions: QotdQuestion[] = Array.isArray(rawQuestions)
    ? rawQuestions.map((q) => {
        const row = q as Record<string, unknown>;
        return {
          id: Number(row.id),
          text: String(row.text ?? ""),
          addedById: row.addedById == null ? null : String(row.addedById),
          timesUsed: Number(row.timesUsed ?? 0),
          seen: Boolean(row.seen),
        };
      })
    : [];

  const tomorrowMap = (snapshotValue(db, "tomorrow") ?? {}) as Record<string, unknown>;
  const rawTomorrow = tomorrowMap[guildId] as Record<string, unknown> | null | undefined;
  const tomorrow: QotdTomorrow | null =
    rawTomorrow && rawTomorrow.id != null
      ? { id: Number(rawTomorrow.id), text: String(rawTomorrow.text ?? "") }
      : null;

  const counts = (snapshotValue(db, "counts") ?? {}) as Record<string, unknown>;
  const total = Number(counts.total ?? questions.length);
  const unseen = Number(counts.unseen ?? questions.filter((q) => !q.seen).length);

  return { questions, tomorrow, total, unseen };
}

/** When the pool last changed, for the "last changed" line. */
export function lastChangedAt(): number | null {
  let db: Database.Database;
  try {
    db = open();
  } catch {
    return null;
  }
  const row = db.prepare(`SELECT value FROM meta WHERE key = 'updated'`).get() as
    | { value: string }
    | undefined;
  const ms = row ? Number(row.value) : NaN;
  return Number.isFinite(ms) ? ms : null;
}

// ------------------------------------------------------------------- writing

export type QotdCommandType =
  | "add_questions"
  | "edit_question"
  | "delete_question"
  | "reset_pool"
  | "clear_seen"
  | "reroll_tomorrow";

function bumpRevision(db: Database.Database): void {
  db.prepare(
    `INSERT INTO meta (key, value) VALUES ('revision', '1')
     ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT)`,
  ).run();
}

function setUpdated(db: Database.Database, now: number): void {
  db.prepare(
    `INSERT INTO meta (key, value) VALUES ('updated', ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
  ).run(String(now));
}

/** Queue an imperative pool change for the bot (add, edit, delete, reset, …). */
export function queueCommand(
  type: QotdCommandType,
  payload: Record<string, unknown>,
  actorId: string,
): void {
  const db = open();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO commands (type, payload, created_ts, actor) VALUES (?, ?, ?, ?)`,
    ).run(type, JSON.stringify(payload), Math.floor(Date.now() / 1000), actorId);
    bumpRevision(db);
    setUpdated(db, Date.now());
  })();
}
