import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";

/**
 * The read-only view of the bot's Custom Matches database.
 *
 * Everything here reads `custommatch.db` and never writes it — the same strictly
 * read-only arrangement the Overview uses for `tracking_data.db` and the economy
 * inspector uses for `economy.db`. The bot writes games, player stats, MMR
 * history, matches and rivalries continuously; this just reads what is already
 * there, so the module's pages need no snapshot job. All *writes* go through the
 * command bridge instead (see lib/custommatch/store.ts).
 *
 * Discord IDs are snowflakes past 2^53, so every id column is `SELECT`ed
 * `CAST(... AS TEXT)` and read as a string — a plain read would silently round
 * its low digits. Small autoincrement ids (game_id, match_id) and counts stay
 * numbers. The games table has grown by migration, so its columns are read via
 * `PRAGMA table_info` rather than a hard-coded list: a copy that predates a
 * column degrades to that field being absent, not a thrown query.
 */

const DB_PATH =
  env.VIBEY_CUSTOMMATCH_DB ?? path.join(process.cwd(), "..", "data", "custommatch.db");

let instance: Database.Database | null = null;

function open(): Database.Database | null {
  if (instance) return instance;
  try {
    if (!fs.existsSync(DB_PATH)) return null;
    instance = new Database(DB_PATH, { readonly: true, fileMustExist: true });
    return instance;
  } catch {
    return null;
  }
}

/** True when the database is present and readable — drives the offline state. */
export function isReadable(): boolean {
  try {
    const db = open();
    if (!db) return false;
    db.prepare("SELECT 1 FROM games LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

// ------------------------------------------------------------------ helpers

type Row = Record<string, unknown>;

/** Column names of a table, or [] if the table is absent. */
function tableColumns(db: Database.Database, table: string): string[] {
  try {
    return (db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[]).map(
      (c) => c.name,
    );
  } catch {
    return [];
  }
}

function num(value: unknown, fallback = 0): number {
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : fallback;
}

/** A snowflake id column (already `CAST` to TEXT): 0/empty means "unset". */
function id(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const s = String(value);
  return s === "" || s === "0" ? null : s;
}

function text(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const s = String(value);
  return s === "" ? null : s;
}

function bool(value: unknown): boolean {
  return value === 1 || value === true || value === "1";
}

// ------------------------------------------------------------------- games

/** Snowflake columns on `games`, cast to TEXT so precision survives. */
const GAME_ID_COLUMNS = new Set([
  "queue_channel_id",
  "verified_role_id",
  "schedule_down_message_id",
  "game_channel_id",
  "leaderboard_channel_id",
  "leaderboard_message_id",
  "category_id",
  "lf1_channel_id",
  "secondary_queue_channel_id",
]);

/** A parsed weekly schedule: weekday index (0=Mon) → open/close "HH:MM". */
export type ScheduleTimes = Record<string, { open: string; close: string }>;

/** The full, typed settings of one game — snake_case keys mirror the DB columns
 *  so the editor round-trips a change straight back through `game_update`. */
export interface CmGame {
  game_id: number;
  name: string;
  enabled: boolean;
  player_count: number;
  queue_type: string;
  captain_selection: string;
  // channels & roles (snowflake strings, or null when unset)
  queue_channel_id: string | null;
  verified_role_id: string | null;
  game_channel_id: string | null;
  leaderboard_channel_id: string | null;
  category_id: string | null;
  lf1_channel_id: string | null;
  // rules
  vc_creation_enabled: boolean;
  queue_role_required: boolean;
  dm_ready_up: boolean;
  ign_required: boolean;
  role_required: boolean;
  pc_enabled: boolean;
  pc_offset_tiers: number;
  // timers
  ready_timer_seconds: number;
  grace_period_minutes: number;
  not_ready_cooldown_minutes: number;
  queue_timeout_minutes: number;
  // penalties
  penalty_1st_minutes: number;
  penalty_2nd_minutes: number;
  penalty_3rd_minutes: number;
  penalty_decay_days: number;
  decline_1st_minutes: number;
  decline_2nd_minutes: number;
  decline_3rd_minutes: number;
  // appearance
  banner_url: string | null;
  short_name: string | null;
  ready_loading_emoji: string | null;
  ready_done_emoji: string | null;
  verification_topic: string | null;
  // schedule
  schedule_enabled: boolean;
  schedule_times: ScheduleTimes | null;
  // secondary "fun mode" queue
  secondary_queue_enabled: boolean;
  secondary_queue_name: string | null;
  secondary_queue_player_count: number | null;
  secondary_queue_type: string | null;
  secondary_queue_channel_id: string | null;
  secondary_queue_match_limit: number | null;
  secondary_banner_url: string | null;
  secondary_schedule_times: ScheduleTimes | null;
}

/** A game as shown on the landing grid: the essentials plus live/derived facts. */
export interface CmGameCard {
  game_id: number;
  name: string;
  enabled: boolean;
  player_count: number;
  queue_type: string;
  queue_channel_id: string | null;
  rank_count: number;
  /** A queue message is currently posted for this game. */
  queue_open: boolean;
  /** Players sitting in this game's queue(s) right now. */
  queued: number;
  /** Configuration problems worth flagging on the card; empty means healthy. */
  health: string[];
}

function parseSchedule(value: unknown): ScheduleTimes | null {
  if (value == null || value === "") return null;
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    return parsed && typeof parsed === "object" ? (parsed as ScheduleTimes) : null;
  } catch {
    return null;
  }
}

/** Build a `SELECT` list for `games` that casts id columns to TEXT. */
function gameSelect(db: Database.Database): string | null {
  const cols = tableColumns(db, "games");
  if (cols.length === 0) return null;
  return cols
    .map((c) => (GAME_ID_COLUMNS.has(c) ? `CAST(${c} AS TEXT) AS ${c}` : c))
    .join(", ");
}

function mapGame(row: Row): CmGame {
  const g = (key: string) => row[key];
  return {
    game_id: num(g("game_id")),
    name: String(g("name") ?? ""),
    enabled: g("enabled") === undefined ? true : bool(g("enabled")),
    player_count: num(g("player_count"), 10),
    queue_type: String(g("queue_type") ?? "mmr"),
    captain_selection: String(g("captain_selection") ?? "random"),
    queue_channel_id: id(g("queue_channel_id")),
    verified_role_id: id(g("verified_role_id")),
    game_channel_id: id(g("game_channel_id")),
    leaderboard_channel_id: id(g("leaderboard_channel_id")),
    category_id: id(g("category_id")),
    lf1_channel_id: id(g("lf1_channel_id")),
    vc_creation_enabled: bool(g("vc_creation_enabled")),
    queue_role_required: g("queue_role_required") === undefined ? true : bool(g("queue_role_required")),
    dm_ready_up: bool(g("dm_ready_up")),
    ign_required: bool(g("ign_required")),
    role_required: bool(g("role_required")),
    pc_enabled: bool(g("pc_enabled")),
    pc_offset_tiers: num(g("pc_offset_tiers"), 1),
    ready_timer_seconds: num(g("ready_timer_seconds"), 60),
    grace_period_minutes: num(g("grace_period_minutes"), 10),
    not_ready_cooldown_minutes: num(g("not_ready_cooldown_minutes"), 5),
    queue_timeout_minutes: num(g("queue_timeout_minutes"), 180),
    penalty_1st_minutes: num(g("penalty_1st_minutes"), 0),
    penalty_2nd_minutes: num(g("penalty_2nd_minutes"), 0),
    penalty_3rd_minutes: num(g("penalty_3rd_minutes"), 0),
    penalty_decay_days: num(g("penalty_decay_days"), 30),
    decline_1st_minutes: num(g("decline_1st_minutes"), 0),
    decline_2nd_minutes: num(g("decline_2nd_minutes"), 0),
    decline_3rd_minutes: num(g("decline_3rd_minutes"), 0),
    banner_url: text(g("banner_url")),
    short_name: text(g("short_name")),
    ready_loading_emoji: text(g("ready_loading_emoji")),
    ready_done_emoji: text(g("ready_done_emoji")),
    verification_topic: text(g("verification_topic")),
    schedule_enabled: bool(g("schedule_enabled")),
    schedule_times: parseSchedule(g("schedule_times")),
    secondary_queue_enabled: bool(g("secondary_queue_enabled")),
    secondary_queue_name: text(g("secondary_queue_name")),
    secondary_queue_player_count:
      g("secondary_queue_player_count") == null ? null : num(g("secondary_queue_player_count")),
    secondary_queue_type: text(g("secondary_queue_type")),
    secondary_queue_channel_id: id(g("secondary_queue_channel_id")),
    secondary_queue_match_limit:
      g("secondary_queue_match_limit") == null ? null : num(g("secondary_queue_match_limit")),
    secondary_banner_url: text(g("secondary_banner_url")),
    secondary_schedule_times: parseSchedule(g("secondary_schedule_times")),
  };
}

/** Every game, unfiltered, for the editor and the settings hub. */
export function readGames(): CmGame[] {
  const db = open();
  if (!db) return [];
  const select = gameSelect(db);
  if (!select) return [];
  const rows = db.prepare(`SELECT ${select} FROM games ORDER BY name COLLATE NOCASE`).all() as Row[];
  return rows.map(mapGame);
}

/** One game's full settings, or null if it's gone. */
export function readGame(gameId: number): CmGame | null {
  const db = open();
  if (!db) return null;
  const select = gameSelect(db);
  if (!select) return null;
  const row = db.prepare(`SELECT ${select} FROM games WHERE game_id = ?`).get(gameId) as
    | Row
    | undefined;
  return row ? mapGame(row) : null;
}

// ------------------------------------------------------------- rank ladder

export interface CmRank {
  role_id: string;
  mmr_value: number;
  label: string | null;
}

/** The MMR→role ladder for a game, lowest threshold first. */
export function readRanks(gameId: number): CmRank[] {
  const db = open();
  if (!db) return [];
  const cols = tableColumns(db, "game_mmr_roles");
  if (cols.length === 0) return [];
  const hasLabel = cols.includes("label");
  const rows = db
    .prepare(
      `SELECT CAST(role_id AS TEXT) AS role_id, mmr_value${hasLabel ? ", label" : ""}
       FROM game_mmr_roles WHERE game_id = ? ORDER BY mmr_value ASC`,
    )
    .all(gameId) as Row[];
  return rows.map((r) => ({
    role_id: String(r.role_id),
    mmr_value: num(r.mmr_value),
    label: hasLabel ? text(r.label) : null,
  }));
}

/** role_id → rank count per game, for the landing cards in one query. */
function rankCounts(db: Database.Database): Map<number, number> {
  const out = new Map<number, number>();
  try {
    for (const r of db
      .prepare(`SELECT game_id, COUNT(*) AS n FROM game_mmr_roles GROUP BY game_id`)
      .all() as { game_id: number; n: number }[]) {
      out.set(r.game_id, r.n);
    }
  } catch {
    /* table absent on an old copy */
  }
  return out;
}

// --------------------------------------------------------------- live queues

export interface CmQueueInfo {
  open: boolean;
  queued: number;
  state: string;
}

/** game_id → live queue facts (is one posted, how many are in it). */
export function readQueues(): Map<number, CmQueueInfo> {
  const out = new Map<number, CmQueueInfo>();
  const db = open();
  if (!db) return out;
  try {
    const rows = db
      .prepare(
        `SELECT q.game_id AS game_id, q.state AS state, q.message_id AS message_id,
                COUNT(qp.player_id) AS n
         FROM active_queues q
         LEFT JOIN queue_players qp ON qp.queue_id = q.queue_id
         GROUP BY q.queue_id`,
      )
      .all() as { game_id: number; state: string; message_id: unknown; n: number }[];
    for (const r of rows) {
      const prev = out.get(r.game_id);
      const queued = num(r.n) + (prev?.queued ?? 0);
      const open = r.message_id != null || (prev?.open ?? false);
      out.set(r.game_id, { open, queued, state: String(r.state ?? "waiting") });
    }
  } catch {
    /* tables absent */
  }
  return out;
}

// ------------------------------------------------------------------ health

/** Configuration problems worth surfacing on a game's card. Empty = healthy. */
export function gameHealth(game: CmGame, rankCount: number): string[] {
  const issues: string[] = [];
  if (!game.queue_channel_id) issues.push("No queue channel set");
  if (game.queue_role_required && !game.verified_role_id)
    issues.push("Queue role required, but no verified role is set");
  if (game.role_required && rankCount === 0)
    issues.push("Rank ladder required, but no ranks are configured");
  if (game.schedule_enabled && (!game.schedule_times || Object.keys(game.schedule_times).length === 0))
    issues.push("Schedule is on, but no open hours are set");
  if (game.secondary_queue_enabled && !game.secondary_queue_player_count)
    issues.push("Fun-mode queue is on, but has no player count");
  return issues;
}

/** The landing grid in one call: each game with its rank count, live queue and health. */
export function readGameCards(): CmGameCard[] {
  const db = open();
  if (!db) return [];
  const games = readGames();
  const ranks = rankCounts(db);
  const queues = readQueues();
  return games.map((g) => {
    const rank_count = ranks.get(g.game_id) ?? 0;
    const q = queues.get(g.game_id);
    return {
      game_id: g.game_id,
      name: g.name,
      enabled: g.enabled,
      player_count: g.player_count,
      queue_type: g.queue_type,
      queue_channel_id: g.queue_channel_id,
      rank_count,
      queue_open: q?.open ?? false,
      queued: q?.queued ?? 0,
      health: gameHealth(g, rank_count),
    };
  });
}

// ------------------------------------------------------------ global config

export interface CmGlobal {
  log_channel_id: string | null;
  cm_admin_channel_id: string | null;
  cm_admin_role_id: string | null;
  cm_discussion_parent_channel_id: string | null;
  category_id: string | null;
  rivals_admin_channel_id: string | null;
  mod_role_ids: string[];
  blacklist: { player_id: string; until: string | null }[];
}

export function readGlobal(): CmGlobal {
  const empty: CmGlobal = {
    log_channel_id: null,
    cm_admin_channel_id: null,
    cm_admin_role_id: null,
    cm_discussion_parent_channel_id: null,
    category_id: null,
    rivals_admin_channel_id: null,
    mod_role_ids: [],
    blacklist: [],
  };
  const db = open();
  if (!db) return empty;

  const config = new Map<string, string>();
  try {
    for (const r of db.prepare(`SELECT key, value FROM config`).all() as {
      key: string;
      value: string;
    }[]) {
      config.set(r.key, r.value);
    }
  } catch {
    return empty;
  }

  const modRoles: string[] = [];
  try {
    for (const r of db.prepare(`SELECT CAST(role_id AS TEXT) AS role_id FROM mod_roles`).all() as {
      role_id: string;
    }[]) {
      modRoles.push(String(r.role_id));
    }
  } catch {
    /* table absent */
  }

  const blacklist: { player_id: string; until: string | null }[] = [];
  try {
    for (const r of db
      .prepare(
        `SELECT CAST(player_id AS TEXT) AS player_id, blacklisted_until
         FROM players WHERE blacklisted_until IS NOT NULL`,
      )
      .all() as { player_id: string; blacklisted_until: string | null }[]) {
      blacklist.push({ player_id: String(r.player_id), until: text(r.blacklisted_until) });
    }
  } catch {
    /* table absent */
  }

  return {
    log_channel_id: id(config.get("log_channel_id")),
    cm_admin_channel_id: id(config.get("cm_admin_channel_id")),
    cm_admin_role_id: id(config.get("cm_admin_role_id")),
    cm_discussion_parent_channel_id: id(config.get("cm_discussion_parent_channel_id")),
    category_id: id(config.get("category_id")),
    rivals_admin_channel_id: id(config.get("rivals_admin_channel_id")),
    mod_role_ids: modRoles,
    blacklist,
  };
}

// ------------------------------------------------------------- leaderboard

export interface CmLeaderRow {
  player_id: string;
  mmr: number;
  effective: number;
  games_played: number;
  wins: number;
  losses: number;
  last_played: string | null;
}

/** A game's standings, best effective MMR first. Only players who have played. */
export function readLeaderboard(gameId: number, limit = 100): CmLeaderRow[] {
  const db = open();
  if (!db) return [];
  try {
    const rows = db
      .prepare(
        `SELECT CAST(player_id AS TEXT) AS player_id, mmr, games_played, wins, losses,
                admin_offset, last_played
         FROM player_game_stats
         WHERE game_id = ? AND games_played > 0
         ORDER BY (mmr + admin_offset) DESC
         LIMIT ?`,
      )
      .all(gameId, limit) as Row[];
    return rows.map((r) => ({
      player_id: String(r.player_id),
      mmr: num(r.mmr, 1000),
      effective: num(r.mmr, 1000) + num(r.admin_offset),
      games_played: num(r.games_played),
      wins: num(r.wins),
      losses: num(r.losses),
      last_played: text(r.last_played),
    }));
  } catch {
    return [];
  }
}

// -------------------------------------------------------------- rivalries

export interface CmRivalry {
  player_a_id: string;
  player_b_id: string;
  a_wins: number;
  b_wins: number;
}

/** The most-played head-to-heads for a game, hottest first. */
export function readRivalries(gameId: number, limit = 25): CmRivalry[] {
  const db = open();
  if (!db) return [];
  try {
    const rows = db
      .prepare(
        `SELECT CAST(player_a_id AS TEXT) AS player_a_id,
                CAST(player_b_id AS TEXT) AS player_b_id,
                player_a_wins, player_b_wins
         FROM rivalries
         WHERE game_id = ?
         ORDER BY (player_a_wins + player_b_wins) DESC
         LIMIT ?`,
      )
      .all(gameId, limit) as Row[];
    return rows.map((r) => ({
      player_a_id: String(r.player_a_id),
      player_b_id: String(r.player_b_id),
      a_wins: num(r.player_a_wins),
      b_wins: num(r.player_b_wins),
    }));
  } catch {
    return [];
  }
}

// --------------------------------------------------------------- analytics

export interface CmAnalytics {
  totalMatches: number;
  totalPlayers: number;
  avgMmr: number;
  /** Last 30 days of decided matches, oldest first. */
  matchesPerDay: { day: string; count: number }[];
  /** 24 buckets, index = UTC hour. */
  peakHours: number[];
  /** MMR distribution in 100-wide buckets: { floor, count }. */
  mmrHistogram: { floor: number; count: number }[];
}

export function readAnalytics(gameId: number): CmAnalytics {
  const empty: CmAnalytics = {
    totalMatches: 0,
    totalPlayers: 0,
    avgMmr: 0,
    matchesPerDay: [],
    peakHours: new Array(24).fill(0),
    mmrHistogram: [],
  };
  const db = open();
  if (!db) return empty;

  try {
    const totalMatches = num(
      (
        db
          .prepare(`SELECT COUNT(*) AS n FROM matches WHERE game_id = ? AND cancelled = 0`)
          .get(gameId) as { n: number }
      ).n,
    );

    const playerAgg = db
      .prepare(
        `SELECT COUNT(*) AS n, AVG(mmr) AS avg
         FROM player_game_stats WHERE game_id = ? AND games_played > 0`,
      )
      .get(gameId) as { n: number; avg: number | null };

    const perDay = db
      .prepare(
        `SELECT date(created_at) AS day, COUNT(*) AS count
         FROM matches
         WHERE game_id = ? AND cancelled = 0 AND created_at >= date('now', '-30 days')
         GROUP BY day ORDER BY day ASC`,
      )
      .all(gameId) as { day: string; count: number }[];

    const peakHours = new Array(24).fill(0);
    for (const r of db
      .prepare(
        `SELECT CAST(strftime('%H', created_at) AS INTEGER) AS hr, COUNT(*) AS count
         FROM matches WHERE game_id = ? AND cancelled = 0 GROUP BY hr`,
      )
      .all(gameId) as { hr: number; count: number }[]) {
      if (r.hr >= 0 && r.hr < 24) peakHours[r.hr] = num(r.count);
    }

    const histo = new Map<number, number>();
    for (const r of db
      .prepare(
        `SELECT (mmr / 100) * 100 AS floor, COUNT(*) AS count
         FROM player_game_stats WHERE game_id = ? AND games_played > 0 GROUP BY floor ORDER BY floor ASC`,
      )
      .all(gameId) as { floor: number; count: number }[]) {
      histo.set(num(r.floor), num(r.count));
    }

    return {
      totalMatches,
      totalPlayers: num(playerAgg?.n),
      avgMmr: Math.round(num(playerAgg?.avg)),
      matchesPerDay: perDay.map((r) => ({ day: String(r.day), count: num(r.count) })),
      peakHours,
      mmrHistogram: [...histo.entries()].map(([floor, count]) => ({ floor, count })),
    };
  } catch {
    return empty;
  }
}

// ----------------------------------------------------------- player profile

export interface CmProfileMatch {
  match_id: number;
  team: string;
  winning_team: string | null;
  won: boolean | null;
  map_name: string | null;
  created_at: string | null;
}

export interface CmRoleStat {
  role: string;
  mmr: number;
  games_played: number;
  wins: number;
  losses: number;
}

export interface CmProfileRival {
  opponent_id: string;
  wins: number;
  losses: number;
}

export interface CmPlayerProfile {
  player_id: string;
  found: boolean;
  mmr: number;
  effective: number;
  admin_offset: number;
  games_played: number;
  wins: number;
  losses: number;
  platform: string;
  last_played: string | null;
  ign: string | null;
  /** Effective MMR after each match, oldest first — the sparkline. */
  mmrTrend: number[];
  /** Current win (+n) or loss (-n) streak from the most recent matches. */
  streak: number;
  recentMatches: CmProfileMatch[];
  roleStats: CmRoleStat[];
  rivals: CmProfileRival[];
  penalties: { kind: "ready" | "decline"; offenses: number; expires: string | null }[];
  suspensions: { suspension_id: number; until: string | null; reason: string | null }[];
}

export function readPlayerProfile(gameId: number, playerId: string): CmPlayerProfile {
  const base: CmPlayerProfile = {
    player_id: playerId,
    found: false,
    mmr: 1000,
    effective: 1000,
    admin_offset: 0,
    games_played: 0,
    wins: 0,
    losses: 0,
    platform: "console",
    last_played: null,
    ign: null,
    mmrTrend: [],
    streak: 0,
    recentMatches: [],
    roleStats: [],
    rivals: [],
    penalties: [],
    suspensions: [],
  };
  const db = open();
  if (!db) return base;
  const pid = BigInt(playerId);

  try {
    const stats = db
      .prepare(
        `SELECT mmr, games_played, wins, losses, admin_offset, last_played, platform
         FROM player_game_stats WHERE player_id = ? AND game_id = ?`,
      )
      .get(pid, gameId) as Row | undefined;

    if (stats) {
      base.found = true;
      base.mmr = num(stats.mmr, 1000);
      base.admin_offset = num(stats.admin_offset);
      base.effective = base.mmr + base.admin_offset;
      base.games_played = num(stats.games_played);
      base.wins = num(stats.wins);
      base.losses = num(stats.losses);
      base.platform = String(stats.platform ?? "console");
      base.last_played = text(stats.last_played);
    }

    const ign = db
      .prepare(`SELECT ign FROM player_igns WHERE player_id = ? AND game_id = ?`)
      .get(pid, gameId) as { ign: string } | undefined;
    if (ign) base.ign = text(ign.ign);

    const trend = db
      .prepare(
        `SELECT mmr_after FROM mmr_history
         WHERE player_id = ? AND game_id = ? ORDER BY id ASC LIMIT 100`,
      )
      .all(pid, gameId) as { mmr_after: number }[];
    base.mmrTrend = trend.map((r) => num(r.mmr_after) + base.admin_offset);

    const matches = db
      .prepare(
        `SELECT m.match_id AS match_id, mp.team AS team, m.winning_team AS winning_team,
                m.map_name AS map_name, m.created_at AS created_at
         FROM match_players mp
         JOIN matches m ON m.match_id = mp.match_id
         WHERE mp.player_id = ? AND m.game_id = ? AND m.cancelled = 0
         ORDER BY m.match_id DESC LIMIT 20`,
      )
      .all(pid, gameId) as Row[];
    base.recentMatches = matches.map((r) => {
      const team = String(r.team ?? "");
      const winning = text(r.winning_team);
      const won = winning == null ? null : winning === team;
      return {
        match_id: num(r.match_id),
        team,
        winning_team: winning,
        won,
        map_name: text(r.map_name),
        created_at: text(r.created_at),
      };
    });

    // Streak: walk the most-recent decided matches while the result holds.
    let streak = 0;
    for (const m of base.recentMatches) {
      if (m.won === null) break;
      if (streak === 0) streak = m.won ? 1 : -1;
      else if (m.won && streak > 0) streak += 1;
      else if (!m.won && streak < 0) streak -= 1;
      else break;
    }
    base.streak = streak;

    for (const r of db
      .prepare(
        `SELECT role, mmr, games_played, wins, losses
         FROM ow_role_stats WHERE player_id = ? AND game_id = ? ORDER BY role`,
      )
      .all(pid, gameId) as Row[]) {
      base.roleStats.push({
        role: String(r.role ?? ""),
        mmr: num(r.mmr, 1000),
        games_played: num(r.games_played),
        wins: num(r.wins),
        losses: num(r.losses),
      });
    }

    for (const r of db
      .prepare(
        `SELECT CAST(player_a_id AS TEXT) AS a, CAST(player_b_id AS TEXT) AS b,
                player_a_wins, player_b_wins
         FROM rivalries
         WHERE game_id = ? AND (player_a_id = ? OR player_b_id = ?)
         ORDER BY (player_a_wins + player_b_wins) DESC LIMIT 10`,
      )
      .all(gameId, pid, pid) as Row[]) {
      const isA = String(r.a) === playerId;
      base.rivals.push({
        opponent_id: isA ? String(r.b) : String(r.a),
        wins: isA ? num(r.player_a_wins) : num(r.player_b_wins),
        losses: isA ? num(r.player_b_wins) : num(r.player_a_wins),
      });
    }

    for (const [table, kind] of [
      ["ready_penalties", "ready"],
      ["decline_penalties", "decline"],
    ] as const) {
      try {
        const row = db
          .prepare(`SELECT offense_count, penalty_expires FROM ${table} WHERE player_id = ?`)
          .get(pid) as { offense_count: number; penalty_expires: string | null } | undefined;
        if (row && num(row.offense_count) > 0) {
          base.penalties.push({
            kind,
            offenses: num(row.offense_count),
            expires: text(row.penalty_expires),
          });
        }
      } catch {
        /* table absent */
      }
    }

    for (const r of db
      .prepare(
        `SELECT suspension_id, suspended_until, reason
         FROM suspensions WHERE player_id = ? AND (game_id = ? OR game_id IS NULL)`,
      )
      .all(pid, gameId) as Row[]) {
      base.suspensions.push({
        suspension_id: num(r.suspension_id),
        until: text(r.suspended_until),
        reason: text(r.reason),
      });
    }
  } catch {
    /* leave partially-filled profile; the page still renders */
  }

  return base;
}

/** Player ids that appear in a game's stats — the searchable roster. */
export function readRoster(gameId: number): string[] {
  const db = open();
  if (!db) return [];
  try {
    return (
      db
        .prepare(`SELECT CAST(player_id AS TEXT) AS player_id FROM player_game_stats WHERE game_id = ?`)
        .all(gameId) as { player_id: string }[]
    ).map((r) => String(r.player_id));
  } catch {
    return [];
  }
}
