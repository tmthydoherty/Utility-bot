import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import type { SettingsValues } from "@/lib/schema/types";

import { parseJsonPreservingIds } from "./json";

import {
  emptyConditions,
  isGroup,
  parseCondition,
  parseSteps,
  serialiseCondition,
  serialiseStep,
  type Automation,
  type ConditionGroup,
  type CooldownScope,
  type RunRecord,
  type Step,
  type TraceEntry,
} from "./types";

/**
 * The bot's real automations database, opened directly.
 *
 * This is the one place in the dashboard that writes to something the bot
 * owns, and it is a deliberate exception to the rule the rest of the app
 * follows. Everything else drafts into the dashboard's own file because
 * nothing else has a live consumer; an automation does. A builder that saves
 * somewhere the engine never reads is a demo, not a feature.
 *
 * Three things make that safe rather than reckless:
 *
 * * **WAL, and a busy timeout on both sides.** The bot's `AutomationsDB` sets
 *   `busy_timeout=5000` for exactly this reason. Two processes writing the
 *   same SQLite file is a supported configuration, not a trick, as long as
 *   neither holds a transaction open across an await — and nothing here does.
 *
 * * **A revision counter.** Every write here bumps `settings.revision`, and
 *   the cog reads that integer every ten seconds and reloads when it moves.
 *   That is the whole synchronisation protocol: no socket, no HTTP endpoint on
 *   the bot, nothing to keep running. An automation switched on here is live
 *   within ten seconds.
 *
 * * **Nothing here can make the bot act.** The worst a bad write can do is
 *   create an automation, and a new automation is created switched off and in
 *   test mode. Turning one on is a separate, explicit action.
 *
 * The path is configurable so a development machine can point at a copy.
 */

// A directory of its own, holding this database and nothing else. SQLite in
// WAL mode writes three files, so the systemd `ReadWritePaths` grant has to
// cover the directory — and anything else sitting in it would become writable
// by the one process here that is reachable from the internet.
const DB_PATH =
  env.VIBEY_AUTOMATIONS_DB ??
  path.join(process.cwd(), "..", "data", "automations", "automations.db");

let instance: Database.Database | null = null;

export class AutomationsUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's automations database could not be opened.");
    this.name = "AutomationsUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;

  // Opened read-write but never created: if the file is missing, the bot has
  // not started yet, and silently creating an empty one here would give the
  // dashboard its own database that the engine never reads — the exact failure
  // this whole file exists to avoid, and a silent one.
  if (!fs.existsSync(DB_PATH)) {
    throw new AutomationsUnavailable(new Error(`no database at ${DB_PATH}`));
  }

  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  // Matches the bot's own timeout. A web request would rather wait 5 seconds
  // than fail while the engine finishes a write.
  db.pragma("busy_timeout = 5000");

  instance = db;
  return db;
}

/** True when the bot's database is reachable — drives the page's empty state. */
export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM automations LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

interface AutomationRow {
  id: string;
  guild_id: number;
  name: string;
  description: string;
  enabled: number;
  dry_run: number;
  priority: number;
  stop_after: number;
  trigger_type: string;
  trigger_config: string;
  conditions: string;
  graph: string;
  cooldown_s: number;
  cooldown_scope: string;
  allow_bots: number;
  last_fired_ts: number;
  run_count: number;
  created_ts: number;
  updated_ts: number;
  updated_by: number | null;
  template_key: string;
}

/**
 * Every JSON column here can contain a Discord ID, so none of them may go
 * through a plain `JSON.parse` — see lib/automations/json.ts for what that
 * does to a snowflake.
 */
function parseJson<T>(raw: string | null | undefined, fallback: T): T {
  return parseJsonPreservingIds<T>(raw, fallback);
}

function toAutomation(row: AutomationRow): Automation {
  const parsed = parseCondition(parseJson<unknown>(row.conditions, {}));
  return {
    id: row.id,
    guildId: String(row.guild_id),
    name: row.name,
    description: row.description ?? "",
    triggerType: row.trigger_type,
    triggerConfig: parseJson<SettingsValues>(row.trigger_config, {}),
    // A leaf at the root would be a hand-edited database; wrap it rather than
    // refusing to show the automation at all.
    conditions: isGroup(parsed) ? parsed : { op: "and", items: [parsed] },
    steps: parseSteps(parseJson<{ steps?: unknown }>(row.graph, {}).steps),
    enabled: row.enabled === 1,
    dryRun: row.dry_run === 1,
    priority: row.priority,
    stopAfter: row.stop_after === 1,
    cooldownSeconds: row.cooldown_s,
    cooldownScope: (row.cooldown_scope as CooldownScope) ?? "user",
    allowBots: row.allow_bots === 1,
    lastFiredAt: row.last_fired_ts,
    runCount: row.run_count,
    createdAt: row.created_ts,
    updatedAt: row.updated_ts,
    updatedBy: row.updated_by ? String(row.updated_by) : null,
    templateKey: row.template_key ?? "",
  };
}

// ------------------------------------------------------------------- reading

export function listAutomations(guildId: string): Automation[] {
  const rows = open()
    .prepare(
      `SELECT * FROM automations WHERE guild_id = ? ORDER BY priority, created_ts DESC`,
    )
    .all(guildId) as AutomationRow[];
  return rows.map(toAutomation);
}

export function getAutomation(guildId: string, id: string): Automation | null {
  const row = open()
    .prepare(`SELECT * FROM automations WHERE id = ? AND guild_id = ?`)
    .get(id, guildId) as AutomationRow | undefined;
  return row ? toAutomation(row) : null;
}

export function recentRuns(automationId: string, limit = 20): RunRecord[] {
  const rows = open()
    .prepare(
      `SELECT * FROM automation_runs WHERE automation_id = ? ORDER BY ts DESC LIMIT ?`,
    )
    .all(automationId, limit) as {
    id: number;
    automation_id: string;
    ts: number;
    outcome: string;
    summary: string;
    duration_ms: number;
    trace: string;
  }[];

  return rows.map((row) => ({
    id: row.id,
    automationId: row.automation_id,
    at: row.ts,
    outcome: row.outcome as RunRecord["outcome"],
    summary: row.summary,
    durationMs: row.duration_ms,
    trace: parseJson<TraceEntry[]>(row.trace, []),
  }));
}

/** How often anything ran in the last day, for the section header. */
export function runsSince(guildId: string, since: number): number {
  const row = open()
    .prepare(
      `SELECT COUNT(*) AS total FROM automation_runs
       WHERE ts > ? AND automation_id IN (SELECT id FROM automations WHERE guild_id = ?)`,
    )
    .get(since, guildId) as { total: number };
  return row.total;
}

/** The master pause. Off means no automation runs at all, whatever its state. */
export function automationsPaused(): boolean {
  const row = open()
    .prepare(`SELECT value FROM settings WHERE key = 'automations_enabled'`)
    .get() as { value: string } | undefined;
  // Absent means never set, which the bot treats as on.
  return row?.value === "0";
}

// ------------------------------------------------------------------- writing

function now(): number {
  return Math.floor(Date.now() / 1000);
}

/**
 * Tell the bot something changed.
 *
 * One statement rather than read-then-write, so a bump from the Discord panel
 * landing between the two cannot be lost.
 */
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
  automationId: string,
  action: string,
  detail: string,
): void {
  db.prepare(
    `INSERT INTO audit (ts, user_id, entity, entity_id, action, detail, source)
     VALUES (?, ?, 'automations', ?, ?, ?, 'dashboard')`,
  ).run(now(), actorId, automationId, action, detail.slice(0, 500));
}

/** A short random id, matching `new_id()` in the cog's storage. */
function newId(): string {
  return crypto.randomUUID().replace(/-/g, "").slice(0, 12);
}

export interface CreateInput {
  guildId: string;
  name: string;
  triggerType: string;
  triggerConfig?: SettingsValues;
  conditions?: ConditionGroup;
  steps?: Step[];
  templateKey?: string;
  actorId: string;
}

/**
 * Create an automation — always switched off, always in test mode.
 *
 * Not configurable, and not a parameter. A brand-new automation that could act
 * the moment it was saved would mean a mistyped word in a template deleting
 * messages before anyone had read the thing back.
 */
export function createAutomation(input: CreateInput): string {
  const db = open();
  const id = newId();
  const timestamp = now();

  db.transaction(() => {
    db.prepare(
      `INSERT INTO automations (
         id, guild_id, name, description, enabled, dry_run, priority, stop_after,
         trigger_type, trigger_config, conditions, graph,
         cooldown_s, cooldown_scope, allow_bots, last_fired_ts, run_count,
         created_by, updated_by, created_ts, updated_ts, version, template_key
       ) VALUES (?, ?, ?, '', 0, 1, 100, 0, ?, ?, ?, ?, 0, 'user', 0, 0, 0, ?, ?, ?, ?, 1, ?)`,
    ).run(
      id,
      input.guildId,
      input.name.slice(0, 100),
      input.triggerType,
      JSON.stringify(input.triggerConfig ?? {}),
      JSON.stringify(serialiseCondition(input.conditions ?? emptyConditions())),
      JSON.stringify({ steps: (input.steps ?? []).map(serialiseStep) }),
      input.actorId,
      input.actorId,
      timestamp,
      timestamp,
      input.templateKey ?? "",
    );
    audit(db, input.actorId, id, "create", input.name);
    bumpRevision(db);
  })();

  return id;
}

export interface UpdateInput {
  name?: string;
  description?: string;
  triggerType?: string;
  triggerConfig?: SettingsValues;
  conditions?: ConditionGroup;
  steps?: Step[];
  enabled?: boolean;
  dryRun?: boolean;
  priority?: number;
  stopAfter?: boolean;
  cooldownSeconds?: number;
  cooldownScope?: CooldownScope;
  allowBots?: boolean;
}

export function updateAutomation(
  guildId: string,
  id: string,
  changes: UpdateInput,
  actorId: string,
  auditDetail: string,
): boolean {
  const db = open();

  const assignments: string[] = [];
  const values: (string | number)[] = [];

  const set = (column: string, value: string | number) => {
    assignments.push(`${column} = ?`);
    values.push(value);
  };

  if (changes.name !== undefined) set("name", changes.name.slice(0, 100));
  if (changes.description !== undefined) set("description", changes.description.slice(0, 500));
  if (changes.triggerType !== undefined) set("trigger_type", changes.triggerType);
  if (changes.triggerConfig !== undefined) {
    set("trigger_config", JSON.stringify(changes.triggerConfig));
  }
  if (changes.conditions !== undefined) {
    set("conditions", JSON.stringify(serialiseCondition(changes.conditions)));
  }
  if (changes.steps !== undefined) {
    set("graph", JSON.stringify({ steps: changes.steps.map(serialiseStep) }));
  }
  if (changes.enabled !== undefined) set("enabled", changes.enabled ? 1 : 0);
  if (changes.dryRun !== undefined) set("dry_run", changes.dryRun ? 1 : 0);
  if (changes.priority !== undefined) set("priority", changes.priority);
  if (changes.stopAfter !== undefined) set("stop_after", changes.stopAfter ? 1 : 0);
  if (changes.cooldownSeconds !== undefined) set("cooldown_s", changes.cooldownSeconds);
  if (changes.cooldownScope !== undefined) set("cooldown_scope", changes.cooldownScope);
  if (changes.allowBots !== undefined) set("allow_bots", changes.allowBots ? 1 : 0);

  if (assignments.length === 0) return true;

  set("updated_ts", now());
  set("updated_by", actorId);

  let changed = 0;
  db.transaction(() => {
    const result = db
      .prepare(`UPDATE automations SET ${assignments.join(", ")} WHERE id = ? AND guild_id = ?`)
      .run(...values, id, guildId);
    changed = result.changes;
    if (changed > 0) {
      audit(db, actorId, id, "edit", auditDetail);
      bumpRevision(db);
    }
  })();

  return changed > 0;
}

export function deleteAutomation(
  guildId: string,
  id: string,
  actorId: string,
  name: string,
): boolean {
  const db = open();
  let changed = 0;

  db.transaction(() => {
    const result = db
      .prepare(`DELETE FROM automations WHERE id = ? AND guild_id = ?`)
      .run(id, guildId);
    changed = result.changes;
    if (changed > 0) {
      // The engine keys cooldowns and history by automation id, and an id is
      // never reused — but leaving them would slowly fill the file with rows
      // nothing can ever reach.
      db.prepare(`DELETE FROM automation_runs WHERE automation_id = ?`).run(id);
      db.prepare(`DELETE FROM cooldowns WHERE automation_id = ?`).run(id);
      db.prepare(`DELETE FROM pending_actions WHERE automation_id = ?`).run(id);
      audit(db, actorId, id, "delete", name);
      bumpRevision(db);
    }
  })();

  return changed > 0;
}

/** Duplicate an automation, off and in test mode like any new one. */
export function duplicateAutomation(
  guildId: string,
  id: string,
  actorId: string,
): string | null {
  const original = getAutomation(guildId, id);
  if (!original) return null;

  return createAutomation({
    guildId,
    name: `${original.name} (copy)`.slice(0, 100),
    triggerType: original.triggerType,
    triggerConfig: original.triggerConfig,
    conditions: original.conditions,
    steps: original.steps,
    templateKey: original.templateKey,
    actorId,
  });
}

export function setPaused(paused: boolean, actorId: string): void {
  const db = open();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO settings (key, value) VALUES ('automations_enabled', ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    ).run(paused ? "0" : "1");
    audit(db, actorId, "-", paused ? "pause-all" : "resume-all", "every automation");
    bumpRevision(db);
  })();
}
