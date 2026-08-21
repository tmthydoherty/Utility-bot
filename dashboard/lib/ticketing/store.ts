import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";

import {
  parsePanel,
  parseResponses,
  parseTopic,
  serialisePanel,
  serialiseTopic,
  type Panel,
  type SurveyResponse,
  type Topic,
} from "./types";

/**
 * Live wiring for the Ticketing module.
 *
 * The bot owns `topics.json` / `panels.json` / `survey_data.json`, which are not
 * safe for two processes to write, so the dashboard never touches them. It reads
 * and writes a small SQLite bridge instead — the same mechanism as
 * `lib/automations/store.ts` and `lib/bot/module-config.ts` — that the ticketing
 * cog polls every ten seconds (`cogs/ticketing/config_sync.py`).
 *
 * Reads overlay `desired ?? snapshot`, exactly like the module-config bridge: an
 * edit reads back at once from the `desired` row it just wrote, then falls
 * through to the bot's republished `snapshot` once it has been applied and the
 * desired row cleared. Ids come back as strings via `parseJsonPreservingIds` so
 * a snowflake never loses its low digits.
 */

// A directory of its own, so the systemd `ReadWritePaths` grant (WAL writes
// three files) doesn't expose anything else to the internet-facing process.
const DB_PATH =
  env.VIBEY_TICKETING_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "ticketing_config", "ticketing_config.db");

let instance: Database.Database | null = null;

export class TicketingUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's ticketing config database could not be opened.");
    this.name = "TicketingUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;

  // Never created here: a missing file means the bot has not started, and
  // creating an empty one would give the dashboard a database the cog never
  // reads — a silent version of the exact failure this file exists to avoid.
  if (!fs.existsSync(DB_PATH)) {
    throw new TicketingUnavailable(new Error(`no database at ${DB_PATH}`));
  }

  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  instance = db;
  return db;
}

/** True when the bridge is reachable — drives the page's status state. */
export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM snapshot LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

// ------------------------------------------------------------------- reading

interface Row {
  kind: string;
  name: string;
  data: string | null;
  deleted?: number;
}

/**
 * Merge the snapshot and desired lanes for one kind into `{name: parsedData}`.
 * A desired tombstone drops the name; a desired object replaces the snapshot.
 */
function overlay<T>(
  db: Database.Database,
  kind: "topic" | "panel",
  parse: (name: string, raw: Record<string, unknown>) => T,
): Map<string, T> {
  const snapshot = db
    .prepare(`SELECT kind, name, data FROM snapshot WHERE kind = ?`)
    .all(kind) as Row[];
  const desired = db
    .prepare(`SELECT kind, name, data, deleted FROM desired WHERE kind = ?`)
    .all(kind) as Row[];

  const raw = new Map<string, Record<string, unknown> | null>();
  for (const row of snapshot) {
    raw.set(row.name, parseJsonPreservingIds<Record<string, unknown>>(row.data, {}));
  }
  for (const row of desired) {
    if (row.deleted) raw.set(row.name, null);
    else raw.set(row.name, parseJsonPreservingIds<Record<string, unknown>>(row.data, {}));
  }

  const out = new Map<string, T>();
  for (const [name, data] of raw) {
    if (data === null) continue; // tombstoned
    out.set(name, parse(name, data));
  }
  return out;
}

export function listTopics(): Topic[] {
  const map = overlay(open(), "topic", parseTopic);
  return [...map.values()].sort((a, b) => a.label.localeCompare(b.label));
}

export function getTopic(name: string): Topic | null {
  return overlay(open(), "topic", parseTopic).get(name) ?? null;
}

export function listPanels(): Panel[] {
  const map = overlay(open(), "panel", parsePanel);
  return [...map.values()].sort((a, b) => a.name.localeCompare(b.name));
}

export function getPanel(name: string): Panel | null {
  return overlay(open(), "panel", parsePanel).get(name) ?? null;
}

/** Responses come from the read-only snapshot only — the bot owns them. */
export function listResponses(): Record<string, SurveyResponse[]> {
  const rows = open()
    .prepare(`SELECT name, data FROM snapshot WHERE kind = 'responses'`)
    .all() as Row[];
  const out: Record<string, SurveyResponse[]> = {};
  for (const row of rows) {
    out[row.name] = parseResponses(parseJsonPreservingIds<unknown>(row.data, []));
  }
  return out;
}

export function getResponses(survey: string): SurveyResponse[] {
  return listResponses()[survey] ?? [];
}

// ------------------------------------------------------------------- writing

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

const upsertDesired = (
  db: Database.Database,
  kind: "topic" | "panel",
  name: string,
  data: string | null,
  deleted: number,
  actorId: string,
) =>
  db
    .prepare(
      `INSERT INTO desired (kind, name, data, deleted, updated_ts, updated_by)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(kind, name) DO UPDATE SET
         data = excluded.data, deleted = excluded.deleted,
         updated_ts = excluded.updated_ts, updated_by = excluded.updated_by`,
    )
    .run(kind, name, data, deleted, Math.floor(Date.now() / 1000), actorId);

export function saveTopic(topic: Topic, actorId: string): void {
  const db = open();
  db.transaction(() => {
    upsertDesired(db, "topic", topic.name, JSON.stringify(serialiseTopic(topic)), 0, actorId);
    bumpRevision(db);
    setUpdated(db, Date.now());
  })();
}

export function deleteTopic(name: string, actorId: string): void {
  const db = open();
  db.transaction(() => {
    upsertDesired(db, "topic", name, null, 1, actorId);
    bumpRevision(db);
    setUpdated(db, Date.now());
  })();
}

export function savePanel(panel: Panel, actorId: string): void {
  const db = open();
  db.transaction(() => {
    upsertDesired(db, "panel", panel.name, JSON.stringify(serialisePanel(panel)), 0, actorId);
    bumpRevision(db);
    setUpdated(db, Date.now());
  })();
}

export function deletePanel(name: string, actorId: string): void {
  const db = open();
  db.transaction(() => {
    upsertDesired(db, "panel", name, null, 1, actorId);
    bumpRevision(db);
    setUpdated(db, Date.now());
  })();
}

export type CommandType =
  | "publish_panel"
  | "unpublish_panel"
  | "send_survey"
  | "delete_responses"
  | "delete_response";

/** Queue an imperative action for the bot (post a panel, DM a survey, …). */
export function queueCommand(
  type: CommandType,
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

/** When anything last changed, for the "last changed" line. */
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
