import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";

/**
 * The write half of the Custom Matches bridge.
 *
 * Reads never come through here — the module reads the bot's `custommatch.db`
 * directly and read-only (see lib/custommatch/read.ts), the same arrangement the
 * Overview uses for `tracking_data.db`. What a raw DB write *can't* do is the
 * Discord-side work a change implies (re-render the live queue embed, refresh the
 * leaderboard, seed Overwatch role weights), so every mutation is queued here as
 * an imperative command and the cog drains it within about ten seconds —
 * `cogs/custommatch_config_sync.py`.
 *
 * It is the command half of `lib/onboarding/store.ts` with no snapshot: a
 * `commands` table this appends to and a `meta` `revision` it bumps, which the
 * cog's `applied` counter chases. The file is never created here — a missing file
 * means the bot has not started, so writes fail loudly and the UI says so.
 */

const DB_PATH =
  env.VIBEY_CUSTOMMATCH_CONFIG_DB ??
  path.join(process.cwd(), "..", "data", "custommatch_config", "custommatch_config.db");

let instance: Database.Database | null = null;

export class CustomMatchUnavailable extends Error {
  constructor(cause: unknown) {
    super("The bot's Custom Matches command bridge could not be opened.");
    this.name = "CustomMatchUnavailable";
    this.cause = cause;
  }
}

function open(): Database.Database {
  if (instance) return instance;
  if (!fs.existsSync(DB_PATH)) {
    throw new CustomMatchUnavailable(new Error(`no database at ${DB_PATH}`));
  }
  const db = new Database(DB_PATH, { fileMustExist: true });
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  instance = db;
  return db;
}

/** True when the bridge is reachable — drives the "bot is offline" badge. */
export function isAvailable(): boolean {
  try {
    open().prepare("SELECT 1 FROM commands LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

/** Every imperative the dashboard can queue. Kept in step with the cog's
 *  `_apply_config_command` dispatch — a value here with no case there is a
 *  change that silently does nothing. */
export type CustomMatchCommandType =
  | "game_update"
  | "game_add"
  | "game_clone"
  | "game_delete"
  | "global_set"
  | "mod_role_add"
  | "mod_role_remove"
  | "blacklist_add"
  | "blacklist_remove"
  | "rank_set"
  | "rank_remove"
  | "player_mmr_set"
  | "player_offset_set"
  | "player_ign_set"
  | "penalty_clear"
  | "suspension_remove";

/** Queue one command for the bot to drain, bumping the revision the cog chases. */
export function queueCommand(
  type: CustomMatchCommandType,
  payload: Record<string, unknown>,
  actorId: string,
): void {
  const db = open();
  db.transaction(() => {
    db.prepare(
      `INSERT INTO commands (type, payload, created_ts, actor) VALUES (?, ?, ?, ?)`,
    ).run(type, JSON.stringify(payload), Math.floor(Date.now() / 1000), actorId);
    db.prepare(
      `INSERT INTO meta (key, value) VALUES ('revision', '1')
       ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT)`,
    ).run();
  })();
}
