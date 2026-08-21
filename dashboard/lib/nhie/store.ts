import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";

const DB_PATH =
  env.VIBEY_NHIE_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "nhie_config", "nhie_config.db");

let instance: Database.Database | null = null;

export class NhieUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's NHIE config database could not be opened.");
    this.name = "NhieUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;
  if (!fs.existsSync(DB_PATH)) {
    throw new NhieUnavailable(new Error(`no database at ${DB_PATH}`));
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
    open().prepare("SELECT 1 FROM snapshot LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

export interface NhieQuestion {
  index: number;
  text: string;
  type: string;
  suggesterId: string | null;
}

export interface NhiePool {
  questions: NhieQuestion[];
  total: number;
}

function snapshotValue(db: Database.Database, key: string): unknown {
  const row = db.prepare(`SELECT value FROM snapshot WHERE key = ?`).get(key) as
    | { value: string }
    | undefined;
  return row ? parseJsonPreservingIds<unknown>(row.value, null) : null;
}

const EMPTY_POOL: NhiePool = { questions: [], total: 0 };

export function readPool(guildId: string): NhiePool {
  let db: Database.Database;
  try {
    db = open();
  } catch {
    return EMPTY_POOL;
  }

  const rawQuestionsData = snapshotValue(db, "questions") as Record<string, unknown> | null;
  if (!rawQuestionsData || !rawQuestionsData[guildId]) {
    return EMPTY_POOL;
  }

  const guildData = rawQuestionsData[guildId] as { pool?: Record<string, unknown>[] };
  const rawPool = guildData.pool || [];

  const questions: NhieQuestion[] = rawPool.map((q, i) => {
    return {
      index: i,
      text: String(q.question ?? ""),
      type: String(q.type ?? "nhie"),
      suggesterId: q.suggester_id == null ? null : String(q.suggester_id),
    };
  });

  return { questions, total: questions.length };
}

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

export type NhieCommandType =
  | "add_questions"
  | "delete_question"
  | "reset_pool";

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

export function queueCommand(
  type: NhieCommandType,
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
