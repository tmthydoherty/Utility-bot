import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";
import { parseJsonPreservingIds } from "@/lib/automations/json";

/**
 * Live wiring for the Welcome & Onboarding module's rich state.
 *
 * Onboarding's flat settings (channels, roles, thresholds, tier/VIP roles) ride
 * the shared per-guild module-config bridge like welcome/security/qotd. Its
 * *rich* state — the intro question pool, the blacklist, per-member points and
 * their decay grants, the ranked point lists, and the game→LFG mappings — is a
 * list of records the bot alone can render (VIP floors come from Discord roles),
 * so it has its own SQLite bridge, the same shape as `lib/qotd/store.ts`: a
 * `snapshot` the bot republishes every tick and a `commands` queue this appends
 * to and the cog drains (`cogs/onboarding_config_sync.py`).
 *
 * The file is never created here: a missing file means the bot has not started,
 * so reads return empty, "unavailable" values and writes fail loudly.
 */

const DB_PATH =
  env.VIBEY_ONBOARDING_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "onboarding_config", "onboarding_config.db");

let instance: Database.Database | null = null;

export class OnboardingUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's onboarding config database could not be opened.");
    this.name = "OnboardingUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;
  if (!fs.existsSync(DB_PATH)) {
    throw new OnboardingUnavailable(new Error(`no database at ${DB_PATH}`));
  }
  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  instance = db;
  return db;
}

/** True when the bridge is reachable — drives the panels' "bot is offline" state. */
export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM snapshot LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

// ------------------------------------------------------------------- reading

export interface OnbQuestion {
  id: number;
  text: string;
  style: "short" | "long";
  order: number;
  optional: boolean;
}

export interface OnbBlacklistEntry {
  userId: string;
  name: string;
}

export interface OnbGrant {
  expiresAt: string;
  points: number;
}

export interface OnbMember {
  userId: string;
  name: string;
  earned: number;
  vipBase: number;
  effective: number;
  tier: number;
  grants: OnbGrant[];
}

export interface OnbListRow {
  userId: string;
  name: string;
  points: number;
}

export interface OnbUpgradeRow {
  userId: string;
  name: string;
  earned: number;
  vipBase: number;
  effective: number;
}

export interface OnbMapping {
  roleId: string;
  roleName: string;
  threadId: string;
}

export interface OnbMeta {
  tier2Points: number | null;
  tier3Points: number | null;
  replyPoints: string | null;
  hourlyPointCap: string | null;
  primaryGuildId: string | null;
}

export interface OnbLists {
  days30: OnbListRow[];
  days60: OnbListRow[];
  allTime: OnbListRow[];
  upgrade: OnbUpgradeRow[];
}

function snapshotValue(db: Database.Database, key: string): unknown {
  const row = db.prepare(`SELECT value FROM snapshot WHERE key = ?`).get(key) as
    | { value: string }
    | undefined;
  return row ? parseJsonPreservingIds<unknown>(row.value, null) : null;
}

function readSnapshot<T>(key: string, fallback: T, map: (raw: unknown) => T): T {
  let db: Database.Database;
  try {
    db = open();
  } catch {
    return fallback;
  }
  const raw = snapshotValue(db, key);
  if (raw == null) return fallback;
  try {
    return map(raw);
  } catch {
    return fallback;
  }
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

export function readQuestions(): OnbQuestion[] {
  return readSnapshot<OnbQuestion[]>("questions", [], (raw) =>
    (Array.isArray(raw) ? raw : []).map((q) => {
      const row = asRecord(q);
      return {
        id: Number(row.id),
        text: String(row.text ?? ""),
        style: row.style === "long" ? "long" : "short",
        order: Number(row.order_num ?? 0),
        optional: Boolean(row.is_optional),
      };
    }),
  );
}

export function readBlacklist(): OnbBlacklistEntry[] {
  return readSnapshot<OnbBlacklistEntry[]>("blacklist", [], (raw) =>
    (Array.isArray(raw) ? raw : []).map((b) => {
      const row = asRecord(b);
      return { userId: String(row.user_id), name: String(row.name ?? `User ${row.user_id}`) };
    }),
  );
}

function mapGrants(raw: unknown): OnbGrant[] {
  return (Array.isArray(raw) ? raw : []).map((g) => {
    const row = asRecord(g);
    return { expiresAt: String(row.expires_at ?? ""), points: Number(row.points ?? 0) };
  });
}

export function readMembers(): OnbMember[] {
  return readSnapshot<OnbMember[]>("members", [], (raw) => {
    const map = asRecord(raw);
    return Object.entries(map).map(([userId, value]) => {
      const row = asRecord(value);
      return {
        userId,
        name: String(row.name ?? `User ${userId}`),
        earned: Number(row.earned ?? 0),
        vipBase: Number(row.vip_base ?? 0),
        effective: Number(row.effective ?? 0),
        tier: Number(row.tier ?? 1),
        grants: mapGrants(row.grants),
      };
    });
  });
}

export function readLists(): OnbLists {
  const empty: OnbLists = { days30: [], days60: [], allTime: [], upgrade: [] };
  return readSnapshot<OnbLists>("lists", empty, (raw) => {
    const map = asRecord(raw);
    const rows = (value: unknown): OnbListRow[] =>
      (Array.isArray(value) ? value : []).map((r) => {
        const row = asRecord(r);
        return {
          userId: String(row.user_id),
          name: String(row.name ?? `User ${row.user_id}`),
          points: Number(row.points ?? 0),
        };
      });
    const upgrade: OnbUpgradeRow[] = (Array.isArray(map.upgrade) ? map.upgrade : []).map((r) => {
      const row = asRecord(r);
      return {
        userId: String(row.user_id),
        name: String(row.name ?? `User ${row.user_id}`),
        earned: Number(row.earned ?? 0),
        vipBase: Number(row.vip_base ?? 0),
        effective: Number(row.effective ?? 0),
      };
    });
    return {
      days30: rows(map["30days"]),
      days60: rows(map["60days"]),
      allTime: rows(map.alltime),
      upgrade,
    };
  });
}

export function readMappings(guildId: string): OnbMapping[] {
  return readSnapshot<OnbMapping[]>("mappings", [], (raw) => {
    const map = asRecord(raw);
    const list = map[guildId];
    return (Array.isArray(list) ? list : []).map((m) => {
      const row = asRecord(m);
      return {
        roleId: String(row.role_id),
        roleName: String(row.role_name ?? `Role ${row.role_id}`),
        threadId: String(row.thread_id),
      };
    });
  });
}

export function readMeta(): OnbMeta {
  const empty: OnbMeta = {
    tier2Points: null, tier3Points: null, replyPoints: null,
    hourlyPointCap: null, primaryGuildId: null,
  };
  return readSnapshot<OnbMeta>("meta", empty, (raw) => {
    const row = asRecord(raw);
    return {
      tier2Points: row.tier2_points == null ? null : Number(row.tier2_points),
      tier3Points: row.tier3_points == null ? null : Number(row.tier3_points),
      replyPoints: row.reply_points == null ? null : String(row.reply_points),
      hourlyPointCap: row.hourly_point_cap == null ? null : String(row.hourly_point_cap),
      primaryGuildId: row.primary_guild_id == null ? null : String(row.primary_guild_id),
    };
  });
}

/** When the rich state last changed, for the "last changed" line. */
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

export type OnbCommandType =
  | "add_question"
  | "edit_question"
  | "delete_question"
  | "reorder_questions"
  | "blacklist_add"
  | "blacklist_remove"
  | "points_adjust"
  | "points_wipe"
  | "wipe_all"
  | "mapping_add"
  | "mapping_remove";

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

/** Queue an imperative change for the bot to drain within about ten seconds. */
export function queueCommand(
  type: OnbCommandType,
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
