import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";

/**
 * Live activity statistics, read straight from the bot's databases.
 *
 * Two files, both opened strictly read-only:
 *
 * * `tracking_data.db` — every message, voice session, reaction and emoji use
 *   the tracker cog logs (cogs/tracker.py).
 * * `economy.db` — the levels table, for the XP leaderboard (cogs/economy).
 *
 * Unlike the Economy *config* store (lib/bot/economy-config.ts), nothing here is
 * ever written, so these do not need a shared writable directory: the dashboard
 * only reads. That matters for the systemd sandbox — ProtectHome=read-only lets
 * the process read the whole home tree but write only its own data dir, which is
 * exactly enough for read-only stats and no more.
 *
 * Every entry point degrades to null/empty when a database is missing or cannot
 * be opened (bot never started, dev machine without a copy), so a page calling
 * in here renders a "connect the bot" state instead of crashing.
 *
 * IDs are a precision hazard: a Discord snowflake overflows a JS number, and
 * better-sqlite3 returns integer columns as numbers. So guild IDs are *bound* as
 * BigInt (an exact 64-bit compare) and every ID column is SELECTed `CAST(... AS
 * TEXT)`, so it comes back as a string that never lost its low digits. Counts
 * and durations stay numbers — they are nowhere near the safe-integer ceiling.
 */

const DAY = 86_400;

const TRACKING_DB_PATH =
  env.VIBEY_TRACKING_DB ?? path.join(process.cwd(), "..", "tracking_data.db");
const ECONOMY_DB_PATH =
  env.VIBEY_ECONOMY_DB ?? path.join(process.cwd(), "..", "economy.db");

export interface RankedUser {
  userId: string;
  messages: number;
}

export interface RankedChannel {
  channelId: string;
  messages: number;
}

export interface RankedEmoji {
  /** Null for a unicode emoji, whose glyph is carried in `name`. */
  emojiId: string | null;
  name: string;
  uses: number;
  users: number;
}

export interface LevelEntry {
  userId: string;
  level: number;
  xp: number;
  messages: number;
  voiceSeconds: number;
}

export interface ActivityStats {
  /** Selected-window totals, each with the prior equal-length window for a delta. */
  messages: number;
  messagesPrev: number;
  activeUsers: number;
  activeUsersPrev: number;
  newMembers: number;
  newMembersPrev: number;
  voiceSeconds: number;
  voiceSecondsPrev: number;
  reactions: number;
  reactionsPrev: number;

  /** Days spanned by the window — the divisor for per-day averages. For the
   *  all-time window it's the number of days actually tracked. */
  windowDays: number;
  /** Whether a comparable prior window exists. False for the all-time window,
   *  which has nothing before it to compare against, so deltas are suppressed. */
  hasPrev: boolean;

  /** All-time, this guild. */
  totalMessages: number;
  /** Earliest logged message, in ms — how far back the data goes. */
  trackedSince: number | null;

  /** Daily message counts over {@link trendDays}, oldest first, ending today (UTC). */
  messagesTrend: number[];
  /** Length of {@link messagesTrend} — the chart's span in days. */
  trendDays: number;

  topUsers: RankedUser[];
  topChannels: RankedChannel[];
  topEmojis: RankedEmoji[];

  /** Top XP holders, from economy.db. Empty if that database is absent. */
  levels: LevelEntry[];
}

let trackingDb: Database.Database | null = null;
let economyDb: Database.Database | null = null;

/**
 * Open a database read-only, or null if it can't be opened.
 *
 * `readonly` keeps the sandbox honest — no attempt is ever made to write the
 * file or its WAL siblings. It works against a live WAL database because the
 * bot, being up, keeps the shared-memory index initialised for readers to map.
 */
function openReadOnly(
  file: string,
  handle: Database.Database | null,
): Database.Database | null {
  if (handle) return handle;
  try {
    if (!fs.existsSync(file)) return null;
    return new Database(file, { readonly: true, fileMustExist: true });
  } catch {
    return null;
  }
}

function tracking(): Database.Database | null {
  return (trackingDb = openReadOnly(TRACKING_DB_PATH, trackingDb));
}

function economy(): Database.Database | null {
  return (economyDb = openReadOnly(ECONOMY_DB_PATH, economyDb));
}

/** Read-only handles for the detail readers (lib/bot/tracker-detail.ts). */
export function getTrackingDb(): Database.Database | null {
  return tracking();
}
export function getEconomyDb(): Database.Database | null {
  return economy();
}

/** The time windows the tracker page offers, mapped to a day count. */
export const WINDOW_DAYS: Record<string, number> = {
  "1d": 1,
  "7d": 7,
  "14d": 14,
  "30d": 30,
  all: 3650,
};

/** The unix cutoff for a day count; 0 (everything) once it's the all-time span. */
export function cutoffFor(days: number): number {
  if (days >= 3650) return 0;
  return Math.floor(Date.now() / 1000) - days * DAY;
}

/** A guild ID as a BigInt for exact 64-bit binding, or null if it isn't one. */
export function guildBigInt(guildId: string): bigint | null {
  try {
    return BigInt(guildId);
  } catch {
    return null;
  }
}

/** True when the tracker's database is readable — drives the live/empty state. */
export function trackerStoreAvailable(): boolean {
  const db = tracking();
  if (!db) return false;
  try {
    db.prepare("SELECT 1 FROM message_logs LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

// A short cache so a page that reads several slices — and a reader who reloads —
// doesn't re-run the group-bys against a 37MB table every time. The numbers move
// on the scale of minutes; 30 seconds of staleness is invisible and the queries
// stop being a per-request cost.
const CACHE_TTL_MS = 30_000;
const cache = new Map<string, { value: ActivityStats | null; expires: number }>();

/**
 * Stats for a guild over a rolling window of `days` (a large value — 3650 — is
 * the all-time window). Cached per guild *and* window, since the overview lets a
 * reader switch the range and each range is a distinct set of group-bys.
 */
export function readActivityStats(guildId: string, days = 7): ActivityStats | null {
  const key = `${guildId}:${days}`;
  const hit = cache.get(key);
  if (hit && hit.expires > Date.now()) return hit.value;

  const value = computeActivityStats(guildId, days);
  cache.set(key, { value, expires: Date.now() + CACHE_TTL_MS });
  return value;
}

function computeActivityStats(guildId: string, days: number): ActivityStats | null {
  const db = tracking();
  if (!db) return null;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return null;
  }

  try {
    const now = Math.floor(Date.now() / 1000);
    // All-time uses a floor of 0 (every row); a finite window runs from `days`
    // ago to now, with the equal-length window immediately before it as the
    // comparison baseline. The all-time window has no baseline before it.
    const isAll = days >= 3650;
    const curStart = isAll ? 0 : now - days * DAY;
    const prevStart = now - 2 * days * DAY;
    const hasPrev = !isAll;

    // --- window totals, this window vs the one before -------------------
    const countMessages = db.prepare(
      "SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND timestamp BETWEEN ? AND ?",
    );
    const messages = num(countMessages.get(gid, curStart, now));
    const messagesPrev = hasPrev ? num(countMessages.get(gid, prevStart, curStart)) : 0;

    const countActive = db.prepare(
      "SELECT count(DISTINCT user_id) AS c FROM message_logs WHERE guild_id = ? AND timestamp BETWEEN ? AND ?",
    );
    const activeUsers = num(countActive.get(gid, curStart, now));
    const activeUsersPrev = hasPrev ? num(countActive.get(gid, prevStart, curStart)) : 0;

    const countJoins = db.prepare(
      "SELECT count(*) AS c FROM member_events WHERE guild_id = ? AND event_type = 'join' AND timestamp BETWEEN ? AND ?",
    );
    const newMembers = num(countJoins.get(gid, curStart, now));
    const newMembersPrev = hasPrev ? num(countJoins.get(gid, prevStart, curStart)) : 0;

    const sumVoice = db.prepare(
      "SELECT coalesce(sum(duration), 0) AS c FROM voice_sessions WHERE guild_id = ? AND start_time BETWEEN ? AND ?",
    );
    const voiceSeconds = num(sumVoice.get(gid, curStart, now));
    const voiceSecondsPrev = hasPrev ? num(sumVoice.get(gid, prevStart, curStart)) : 0;

    const countReactions = db.prepare(
      "SELECT count(*) AS c FROM reaction_logs WHERE guild_id = ? AND timestamp BETWEEN ? AND ?",
    );
    const reactions = num(countReactions.get(gid, curStart, now));
    const reactionsPrev = hasPrev ? num(countReactions.get(gid, prevStart, curStart)) : 0;

    // --- all-time context ----------------------------------------------
    const totalMessages = num(
      db.prepare("SELECT count(*) AS c FROM message_logs WHERE guild_id = ?").get(gid),
    );
    const earliest = db
      .prepare("SELECT min(timestamp) AS t FROM message_logs WHERE guild_id = ?")
      .get(gid) as { t: number | null } | undefined;
    const trackedSince = earliest?.t ? earliest.t * 1000 : null;

    // Per-day averages divide by the window's span. All-time spans from the
    // first logged message to now (at least a day, so it never divides by zero).
    const windowDays = isAll
      ? Math.max(1, Math.ceil((now - (earliest?.t ?? now)) / DAY))
      : days;

    // --- trend ----------------------------------------------------------
    // The chart tracks the window, but stays a legible daily bar: never fewer
    // than a week, never more than 30 days (so all-time shows the last month).
    const trendDays = Math.min(30, Math.max(7, isAll ? 30 : days));
    const dayRows = db
      .prepare(
        `SELECT date(timestamp, 'unixepoch') AS day, count(*) AS c
           FROM message_logs
          WHERE guild_id = ? AND timestamp >= ?
          GROUP BY day`,
      )
      .all(gid, now - trendDays * DAY) as { day: string; c: number }[];
    const messagesTrend = fillTrend(dayRows, trendDays);

    // --- leaderboards, over the window ---------------------------------
    const topUsers = (
      db
        .prepare(
          `SELECT CAST(user_id AS TEXT) AS userId, count(*) AS messages
             FROM message_logs
            WHERE guild_id = ? AND timestamp >= ?
            GROUP BY user_id ORDER BY messages DESC LIMIT 5`,
        )
        .all(gid, curStart) as RankedUser[]
    ).map((r) => ({ userId: r.userId, messages: Number(r.messages) }));

    const topChannels = (
      db
        .prepare(
          `SELECT CAST(channel_id AS TEXT) AS channelId, count(*) AS messages
             FROM message_logs
            WHERE guild_id = ? AND timestamp >= ?
            GROUP BY channel_id ORDER BY messages DESC LIMIT 5`,
        )
        .all(gid, curStart) as RankedChannel[]
    ).map((r) => ({ channelId: r.channelId, messages: Number(r.messages) }));

    // Unicode emoji have a NULL id and must group on the character; custom emoji
    // group on the id. coalesce keeps each kind in its own bucket.
    const topEmojis = (
      db
        .prepare(
          `SELECT CAST(emoji_id AS TEXT) AS emojiId, emoji_name AS name,
                  count(*) AS uses, count(DISTINCT user_id) AS users
             FROM emoji_logs
            WHERE guild_id = ? AND timestamp >= ?
            GROUP BY coalesce(emoji_id, emoji_name)
            ORDER BY uses DESC LIMIT 5`,
        )
        .all(gid, curStart) as { emojiId: string | null; name: string; uses: number; users: number }[]
    ).map((r) => ({
      emojiId: r.emojiId ?? null,
      name: r.name,
      uses: Number(r.uses),
      users: Number(r.users),
    }));

    return {
      messages,
      messagesPrev,
      activeUsers,
      activeUsersPrev,
      newMembers,
      newMembersPrev,
      voiceSeconds,
      voiceSecondsPrev,
      reactions,
      reactionsPrev,
      windowDays,
      hasPrev,
      totalMessages,
      trackedSince,
      messagesTrend,
      trendDays,
      topUsers,
      topChannels,
      topEmojis,
      levels: readLevels(),
    };
  } catch {
    // A schema that isn't what we expect (an old copy, a half-migrated file) is
    // an empty overview with a notice, not a 500.
    return null;
  }
}

/** Top XP holders. Its own function, and its own try/catch — economy.db can be
 *  absent while tracking_data.db is present, and the page still wants the rest. */
function readLevels(): LevelEntry[] {
  const db = economy();
  if (!db) return [];
  try {
    return (
      db
        .prepare(
          `SELECT CAST(user_id AS TEXT) AS userId, level, xp, messages, voice_seconds AS voiceSeconds
             FROM users
            WHERE xp > 0
            ORDER BY xp DESC LIMIT 5`,
        )
        .all() as LevelEntry[]
    ).map((r) => ({
      userId: r.userId,
      level: Number(r.level),
      xp: Number(r.xp),
      messages: Number(r.messages),
      voiceSeconds: Number(r.voiceSeconds),
    }));
  } catch {
    return [];
  }
}

function num(row: unknown): number {
  return Number((row as { c?: number } | undefined)?.c ?? 0);
}

/**
 * Line up grouped day-counts into a dense array ending today (UTC).
 *
 * The tracker groups on `date(timestamp,'unixepoch')`, which is a UTC calendar
 * day, so the day keys here are built in UTC to match — a local-time key would
 * be off by one for anyone not on UTC and silently drop a day's messages.
 */
export function fillTrend(rows: { day: string; c: number }[], days: number): number[] {
  const byDay = new Map(rows.map((r) => [r.day, Number(r.c)]));
  const out: number[] = [];
  const today = new Date();
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(today);
    d.setUTCDate(d.getUTCDate() - i);
    out.push(byDay.get(d.toISOString().slice(0, 10)) ?? 0);
  }
  return out;
}
