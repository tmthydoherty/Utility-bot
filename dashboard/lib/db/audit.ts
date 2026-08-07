import "server-only";

import { getDb } from "./index";

/**
 * Every change the dashboard makes, recorded before it is applied.
 *
 * This exists from day one rather than "later" for a specific reason: the
 * dashboard is a second door into a server that previously only had one, and
 * the first question after anything unexpected is "who did that". A trail
 * added after the incident is worth nothing.
 */

export interface AuditEntry {
  id: number;
  createdAt: number;
  actorId: string;
  actorName: string;
  guildId: string;
  action: string;
  target: string | null;
  changes: Record<string, { from: unknown; to: unknown }> | null;
  ip: string | null;
  userAgent: string | null;
}

export interface RecordAuditInput {
  actorId: string;
  actorName: string;
  guildId: string;
  /** Dotted verb, e.g. "settings.update" or "module.toggle". */
  action: string;
  target?: string | null;
  changes?: Record<string, { from: unknown; to: unknown }> | null;
  ip?: string | null;
  userAgent?: string | null;
}

export function recordAudit(input: RecordAuditInput): void {
  getDb()
    .prepare(
      `INSERT INTO audit_log
         (created_at, actor_id, actor_name, guild_id, action, target, changes, ip, user_agent)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      Date.now(),
      input.actorId,
      input.actorName,
      input.guildId,
      input.action,
      input.target ?? null,
      input.changes ? JSON.stringify(input.changes) : null,
      input.ip ?? null,
      // Long enough to identify a browser, short enough not to become a
      // storage problem.
      input.userAgent?.slice(0, 300) ?? null,
    );
}

interface AuditRow {
  id: number;
  created_at: number;
  actor_id: string;
  actor_name: string;
  guild_id: string;
  action: string;
  target: string | null;
  changes: string | null;
  ip: string | null;
  user_agent: string | null;
}

function hydrate(row: AuditRow): AuditEntry {
  return {
    id: row.id,
    createdAt: row.created_at,
    actorId: row.actor_id,
    actorName: row.actor_name,
    guildId: row.guild_id,
    action: row.action,
    target: row.target,
    changes: row.changes ? (JSON.parse(row.changes) as AuditEntry["changes"]) : null,
    ip: row.ip,
    userAgent: row.user_agent,
  };
}

export function listAudit(guildId: string, limit = 100, offset = 0): AuditEntry[] {
  const rows = getDb()
    .prepare(
      `SELECT * FROM audit_log
       WHERE guild_id = ?
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
    )
    .all(guildId, limit, offset) as AuditRow[];
  return rows.map(hydrate);
}

export function countAudit(guildId: string): number {
  const row = getDb()
    .prepare(`SELECT COUNT(*) AS n FROM audit_log WHERE guild_id = ?`)
    .get(guildId) as { n: number };
  return row.n;
}

/**
 * A field-by-field diff, skipping anything that did not actually move.
 *
 * Storing the whole submitted form would make the log unreadable — most saves
 * touch one field out of twenty — and would also mean re-recording every
 * unrelated value on every save.
 */
export function diffValues(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
): Record<string, { from: unknown; to: unknown }> {
  const changes: Record<string, { from: unknown; to: unknown }> = {};
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of keys) {
    const from = before[key];
    const to = after[key];
    if (JSON.stringify(from) !== JSON.stringify(to)) {
      changes[key] = { from: from ?? null, to: to ?? null };
    }
  }
  return changes;
}
