import "server-only";

import type Database from "better-sqlite3";

import {
  cutoffFor,
  fillTrend,
  getTrackingDb,
  guildBigInt,
} from "@/lib/bot/tracker-stats";

/**
 * Per-target reads for the Activity Tracker page — the web equivalents of the
 * User, Channel, Emoji, Leaderboard and Compare modes in `/tracker`
 * (cogs/tracker.py). The overview's own numbers live in tracker-stats.ts; this
 * is everything driven by a picked user, channel or time window.
 *
 * Same rules as the stats layer: guild IDs bound as BigInt, every ID column
 * `CAST … AS TEXT` so a snowflake survives the trip, and any failure degrades to
 * an empty result rather than throwing. In-progress voice sessions (still only
 * in the bot's memory) aren't visible here, so voice figures reflect what's been
 * checkpointed to disk — a few minutes behind live at most.
 */

const DAY = 86_400;

export interface WindowedCount {
  d1: number;
  d7: number;
  d14: number;
  d30: number;
  all: number;
}

export interface UserDetail {
  messages: WindowedCount;
  voiceSeconds: WindowedCount;
  /** Rank within the last 30 days; null when the user sent nothing. */
  messageRank: number | null;
  voiceRank: number | null;
  reactionsReceived30d: number;
  topChannels: { channelId: string; messages: number }[];
  /** 14 daily message counts, oldest first. */
  trend14: number[];
}

export interface ChannelDetail {
  messages: number;
  contributors: number;
  avgPerDay: number;
  trend: number[];
  topContributors: { userId: string; messages: number }[];
}

export interface ChannelRank {
  channelId: string;
  messages: number;
  contributors: number;
}

export interface VoiceChannelRank {
  channelId: string;
  seconds: number;
  members: number;
}

export interface ChannelsOverview {
  totalMessages: number;
  activeChannels: number;
  voiceSeconds: number;
  /** Busiest text channels by messages. */
  topByMessages: ChannelRank[];
  /** Broadest reach — channels ranked by distinct contributors. */
  topByContributors: ChannelRank[];
  /** Busiest voice channels by time spent in them. */
  topVoice: VoiceChannelRank[];
}

export interface EmojiRow {
  emojiId: string | null;
  name: string;
  uses: number;
  users: number;
}

export interface EmojiBoard {
  rows: EmojiRow[];
  totalUses: number;
  distinctCount: number;
}

export interface LeaderboardRow {
  userId: string;
  value: number;
}

/** The viewer's own standing on each board, so they can be pinned on even when
 *  they're outside the top ten. `rank` is their true placement (null if they
 *  have no activity in the window). */
export interface ViewerBoardStat {
  userId: string;
  messages: { value: number; rank: number | null };
  voice: { value: number; rank: number | null };
}

export interface Leaderboards {
  messages: LeaderboardRow[];
  voice: LeaderboardRow[];
  viewer: ViewerBoardStat | null;
}

export interface CompareUserStats {
  messages: number;
  voiceSeconds: number;
  rank: number | null;
  reactionsReceived: number;
  /** Daily message counts over the window, oldest first. */
  trend: number[];
  /** Daily voice seconds over the window, oldest first. */
  voiceTrend: number[];
  topChannels: { channelId: string; messages: number }[];
}

export interface CompareChannelStats {
  messages: number;
  contributors: number;
  avgPerDay: number;
  trend: number[];
  topContributors: { userId: string; messages: number }[];
}

// ---------------------------------------------------------------- user detail

export function readUserDetail(guildId: string, userId: string): UserDetail | null {
  const db = getTrackingDb();
  const gid = guildBigInt(guildId);
  const uid = guildBigInt(userId);
  if (!db || gid === null || uid === null) return null;

  try {
    const now = Math.floor(Date.now() / 1000);
    const windows: [keyof WindowedCount, number][] = [
      ["d1", 1],
      ["d7", 7],
      ["d14", 14],
      ["d30", 30],
    ];

    const msgWindow = db.prepare(
      "SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?",
    );
    const voiceWindow = db.prepare(
      "SELECT coalesce(sum(duration), 0) AS c FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ?",
    );

    const messages = blankWindowed();
    const voiceSeconds = blankWindowed();
    for (const [key, days] of windows) {
      const cut = now - days * DAY;
      messages[key] = numC(msgWindow.get(gid, uid, cut));
      voiceSeconds[key] = numC(voiceWindow.get(gid, uid, cut));
    }
    messages.all = numC(
      db.prepare("SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND user_id = ?").get(gid, uid),
    );
    voiceSeconds.all = numC(
      db
        .prepare("SELECT coalesce(sum(duration), 0) AS c FROM voice_sessions WHERE guild_id = ? AND user_id = ?")
        .get(gid, uid),
    );

    const cut30 = now - 30 * DAY;

    const messageRank = rankWithin(
      db,
      "message_logs",
      "timestamp",
      "count(*)",
      gid,
      uid,
      cut30,
    );
    const voiceRank = rankWithin(
      db,
      "voice_sessions",
      "start_time",
      "sum(duration)",
      gid,
      uid,
      cut30,
    );

    const reactionsReceived30d = numC(
      db
        .prepare(
          "SELECT count(*) AS c FROM reaction_logs WHERE guild_id = ? AND target_message_author_id = ? AND timestamp > ?",
        )
        .get(gid, uid, cut30),
    );

    const topChannels = (
      db
        .prepare(
          `SELECT CAST(channel_id AS TEXT) AS channelId, count(*) AS messages
             FROM message_logs
            WHERE guild_id = ? AND user_id = ? AND timestamp > ?
            GROUP BY channel_id ORDER BY messages DESC LIMIT 6`,
        )
        .all(gid, uid, cut30) as { channelId: string; messages: number }[]
    ).map((r) => ({ channelId: r.channelId, messages: Number(r.messages) }));

    const dayRows = db
      .prepare(
        `SELECT date(timestamp, 'unixepoch') AS day, count(*) AS c
           FROM message_logs
          WHERE guild_id = ? AND user_id = ? AND timestamp > ?
          GROUP BY day`,
      )
      .all(gid, uid, now - 14 * DAY) as { day: string; c: number }[];

    return {
      messages,
      voiceSeconds,
      messageRank,
      voiceRank,
      reactionsReceived30d,
      topChannels,
      trend14: fillTrend(dayRows, 14),
    };
  } catch {
    return null;
  }
}

// ------------------------------------------------------------- channel detail

export function readChannelDetail(
  guildId: string,
  channelId: string,
  days: number,
): ChannelDetail | null {
  const db = getTrackingDb();
  const gid = guildBigInt(guildId);
  const cid = guildBigInt(channelId);
  if (!db || gid === null || cid === null) return null;

  try {
    const cut = cutoffFor(days);
    const span = days >= 3650 ? 30 : days;

    const messages = numC(
      db
        .prepare("SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ?")
        .get(gid, cid, cut),
    );
    const contributors = numC(
      db
        .prepare(
          "SELECT count(DISTINCT user_id) AS c FROM message_logs WHERE guild_id = ? AND channel_id = ? AND timestamp > ?",
        )
        .get(gid, cid, cut),
    );

    const dayRows = db
      .prepare(
        `SELECT date(timestamp, 'unixepoch') AS day, count(*) AS c
           FROM message_logs
          WHERE guild_id = ? AND channel_id = ? AND timestamp > ?
          GROUP BY day`,
      )
      .all(gid, cid, cut) as { day: string; c: number }[];

    const topContributors = (
      db
        .prepare(
          `SELECT CAST(user_id AS TEXT) AS userId, count(*) AS messages
             FROM message_logs
            WHERE guild_id = ? AND channel_id = ? AND timestamp > ?
            GROUP BY user_id ORDER BY messages DESC LIMIT 8`,
        )
        .all(gid, cid, cut) as { userId: string; messages: number }[]
    ).map((r) => ({ userId: r.userId, messages: Number(r.messages) }));

    return {
      messages,
      contributors,
      avgPerDay: Math.round((messages / Math.max(span, 1)) * 10) / 10,
      trend: fillTrend(dayRows, span),
      topContributors,
    };
  } catch {
    return null;
  }
}

// -------------------------------------------------------- channels overview

/**
 * The server-wide channel picture — what shows before a single channel is
 * picked. Deliberately holds figures the per-channel view can't: channels
 * ranked against each other by messages *and* by contributor breadth, plus the
 * busiest voice channels (voice never appears on the text-only channel detail).
 */
export function readChannelsOverview(guildId: string, days: number): ChannelsOverview | null {
  const db = getTrackingDb();
  const gid = guildBigInt(guildId);
  if (!db || gid === null) return null;

  try {
    const cut = cutoffFor(days);

    const totalMessages = numC(
      db.prepare("SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND timestamp > ?").get(gid, cut),
    );
    const activeChannels = numC(
      db
        .prepare("SELECT count(DISTINCT channel_id) AS c FROM message_logs WHERE guild_id = ? AND timestamp > ?")
        .get(gid, cut),
    );
    const voiceSeconds = numC(
      db
        .prepare("SELECT coalesce(sum(duration), 0) AS c FROM voice_sessions WHERE guild_id = ? AND start_time > ?")
        .get(gid, cut),
    );

    const byMessages = (
      db
        .prepare(
          `SELECT CAST(channel_id AS TEXT) AS channelId,
                  count(*) AS messages, count(DISTINCT user_id) AS contributors
             FROM message_logs WHERE guild_id = ? AND timestamp > ?
            GROUP BY channel_id ORDER BY messages DESC LIMIT 8`,
        )
        .all(gid, cut) as { channelId: string; messages: number; contributors: number }[]
    ).map((r) => ({ channelId: r.channelId, messages: Number(r.messages), contributors: Number(r.contributors) }));

    const byContributors = (
      db
        .prepare(
          `SELECT CAST(channel_id AS TEXT) AS channelId,
                  count(*) AS messages, count(DISTINCT user_id) AS contributors
             FROM message_logs WHERE guild_id = ? AND timestamp > ?
            GROUP BY channel_id ORDER BY contributors DESC, messages DESC LIMIT 6`,
        )
        .all(gid, cut) as { channelId: string; messages: number; contributors: number }[]
    ).map((r) => ({ channelId: r.channelId, messages: Number(r.messages), contributors: Number(r.contributors) }));

    const topVoice = (
      db
        .prepare(
          `SELECT CAST(channel_id AS TEXT) AS channelId,
                  coalesce(sum(duration), 0) AS seconds, count(DISTINCT user_id) AS members
             FROM voice_sessions WHERE guild_id = ? AND start_time > ? AND channel_id IS NOT NULL
            GROUP BY channel_id ORDER BY seconds DESC LIMIT 6`,
        )
        .all(gid, cut) as { channelId: string; seconds: number; members: number }[]
    ).map((r) => ({ channelId: r.channelId, seconds: Number(r.seconds), members: Number(r.members) }));

    return {
      totalMessages,
      activeChannels,
      voiceSeconds,
      topByMessages: byMessages,
      topByContributors: byContributors,
      topVoice,
    };
  } catch {
    return null;
  }
}

// --------------------------------------------------------------- emoji board

export function readEmojiBoard(
  guildId: string,
  days: number,
  serverEmojis: { id: string; name: string }[] | null,
): EmojiBoard | null {
  const db = getTrackingDb();
  const gid = guildBigInt(guildId);
  if (!db || gid === null) return null;

  try {
    const cut = cutoffFor(days);

    // All-emoji scope: whatever's been used in the window, most-used first.
    if (!serverEmojis) {
      const rows = (
        db
          .prepare(
            `SELECT CAST(emoji_id AS TEXT) AS emojiId, emoji_name AS name,
                    count(*) AS uses, count(DISTINCT user_id) AS users
               FROM emoji_logs
              WHERE guild_id = ? AND timestamp > ?
              GROUP BY coalesce(emoji_id, emoji_name) ORDER BY uses DESC`,
          )
          .all(gid, cut) as EmojiRow[]
      ).map((r) => ({
        emojiId: r.emojiId ?? null,
        name: r.name,
        uses: Number(r.uses),
        users: Number(r.users),
      }));

      const totals = db
        .prepare(
          "SELECT count(*) AS uses, count(DISTINCT coalesce(emoji_id, emoji_name)) AS distinctCount FROM emoji_logs WHERE guild_id = ? AND timestamp > ?",
        )
        .get(gid, cut) as { uses: number; distinctCount: number } | undefined;

      return {
        rows,
        totalUses: Number(totals?.uses ?? 0),
        distinctCount: Number(totals?.distinctCount ?? 0),
      };
    }

    // Server scope: every custom emoji the guild owns, used or not. Usage counts
    // are read once and looked up per emoji, so an unused one still lists with a
    // zero rather than vanishing from the board.
    const usageById = new Map<string, { uses: number; users: number }>();
    for (const row of db
      .prepare(
        `SELECT CAST(emoji_id AS TEXT) AS emojiId,
                count(*) AS uses, count(DISTINCT user_id) AS users
           FROM emoji_logs
          WHERE guild_id = ? AND timestamp > ? AND emoji_id IS NOT NULL
          GROUP BY emoji_id`,
      )
      .all(gid, cut) as { emojiId: string; uses: number; users: number }[]) {
      usageById.set(row.emojiId, { uses: Number(row.uses), users: Number(row.users) });
    }

    const rows: EmojiRow[] = serverEmojis
      .map((e) => {
        const usage = usageById.get(e.id) ?? { uses: 0, users: 0 };
        return { emojiId: e.id, name: e.name, uses: usage.uses, users: usage.users };
      })
      // Most-used first; unused ones fall to the bottom, then alphabetical.
      .sort((a, b) => b.uses - a.uses || a.name.localeCompare(b.name));

    return {
      rows,
      totalUses: rows.reduce((sum, r) => sum + r.uses, 0),
      distinctCount: rows.filter((r) => r.uses > 0).length,
    };
  } catch {
    return null;
  }
}

// --------------------------------------------------------------- leaderboards

export function readLeaderboards(
  guildId: string,
  days: number,
  viewerId?: string | null,
): Leaderboards | null {
  const db = getTrackingDb();
  const gid = guildBigInt(guildId);
  if (!db || gid === null) return null;

  try {
    const cut = cutoffFor(days);
    // Over-fetch: the view drops members who have since left, so a flat top-ten
    // would come back short. Reading 30 leaves plenty to fill ten active rows.
    const messages = (
      db
        .prepare(
          `SELECT CAST(user_id AS TEXT) AS userId, count(*) AS value
             FROM message_logs WHERE guild_id = ? AND timestamp > ?
            GROUP BY user_id ORDER BY value DESC LIMIT 30`,
        )
        .all(gid, cut) as LeaderboardRow[]
    ).map((r) => ({ userId: r.userId, value: Number(r.value) }));

    const voice = (
      db
        .prepare(
          `SELECT CAST(user_id AS TEXT) AS userId, sum(duration) AS value
             FROM voice_sessions WHERE guild_id = ? AND start_time > ?
            GROUP BY user_id ORDER BY value DESC LIMIT 30`,
        )
        .all(gid, cut) as LeaderboardRow[]
    ).map((r) => ({ userId: r.userId, value: Number(r.value) }));

    // The viewer's own standing, so the view can pin them on even when they sit
    // outside the top ten — their true rank, computed the same way as the board.
    let viewer: ViewerBoardStat | null = null;
    const vid = viewerId ? guildBigInt(viewerId) : null;
    if (viewerId && vid !== null) {
      const msgVal = numC(
        db
          .prepare("SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?")
          .get(gid, vid, cut),
      );
      const voiceVal = numC(
        db
          .prepare("SELECT coalesce(sum(duration), 0) AS c FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ?")
          .get(gid, vid, cut),
      );
      viewer = {
        userId: viewerId,
        messages: {
          value: msgVal,
          rank: msgVal > 0 ? rankWithin(db, "message_logs", "timestamp", "count(*)", gid, vid, cut) : null,
        },
        voice: {
          value: voiceVal,
          rank: voiceVal > 0 ? rankWithin(db, "voice_sessions", "start_time", "sum(duration)", gid, vid, cut) : null,
        },
      };
    }

    return { messages, voice, viewer };
  } catch {
    return null;
  }
}

// ------------------------------------------------------------------- compare

export function readCompareUser(
  guildId: string,
  userId: string,
  days: number,
): CompareUserStats | null {
  const db = getTrackingDb();
  const gid = guildBigInt(guildId);
  const uid = guildBigInt(userId);
  if (!db || gid === null || uid === null) return null;

  try {
    const cut = cutoffFor(days);
    const span = days >= 3650 ? 30 : days;

    const messages = numC(
      db.prepare("SELECT count(*) AS c FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?").get(gid, uid, cut),
    );
    const voiceSeconds = numC(
      db
        .prepare("SELECT coalesce(sum(duration), 0) AS c FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ?")
        .get(gid, uid, cut),
    );
    const rank = rankWithin(db, "message_logs", "timestamp", "count(*)", gid, uid, cut);
    const reactionsReceived = numC(
      db
        .prepare("SELECT count(*) AS c FROM reaction_logs WHERE guild_id = ? AND target_message_author_id = ? AND timestamp > ?")
        .get(gid, uid, cut),
    );
    const dayRows = db
      .prepare(
        `SELECT date(timestamp, 'unixepoch') AS day, count(*) AS c
           FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ? GROUP BY day`,
      )
      .all(gid, uid, cut) as { day: string; c: number }[];
    const voiceDayRows = db
      .prepare(
        `SELECT date(start_time, 'unixepoch') AS day, coalesce(sum(duration), 0) AS c
           FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND start_time > ? GROUP BY day`,
      )
      .all(gid, uid, cut) as { day: string; c: number }[];
    const topChannels = (
      db
        .prepare(
          `SELECT CAST(channel_id AS TEXT) AS channelId, count(*) AS messages
             FROM message_logs WHERE guild_id = ? AND user_id = ? AND timestamp > ?
            GROUP BY channel_id ORDER BY messages DESC LIMIT 4`,
        )
        .all(gid, uid, cut) as { channelId: string; messages: number }[]
    ).map((r) => ({ channelId: r.channelId, messages: Number(r.messages) }));

    return {
      messages,
      voiceSeconds,
      rank,
      reactionsReceived,
      trend: fillTrend(dayRows, span),
      voiceTrend: fillTrend(voiceDayRows, span),
      topChannels,
    };
  } catch {
    return null;
  }
}

export function readCompareChannel(
  guildId: string,
  channelId: string,
  days: number,
): CompareChannelStats | null {
  const detail = readChannelDetail(guildId, channelId, days);
  if (!detail) return null;
  return {
    messages: detail.messages,
    contributors: detail.contributors,
    avgPerDay: detail.avgPerDay,
    trend: detail.trend,
    topContributors: detail.topContributors.slice(0, 4),
  };
}

// --------------------------------------------------------------------- utils

function blankWindowed(): WindowedCount {
  return { d1: 0, d7: 0, d14: 0, d30: 0, all: 0 };
}

function numC(row: unknown): number {
  return Number((row as { c?: number } | undefined)?.c ?? 0);
}

/**
 * A user's 1-based rank by an aggregate over a window, or null if they don't
 * appear. One windowed group-by ranked by ROW_NUMBER, same as the Discord card.
 */
function rankWithin(
  db: Database.Database,
  table: string,
  tsColumn: string,
  agg: string,
  gid: bigint,
  uid: bigint,
  cut: number,
): number | null {
  const row = db
    .prepare(
      `SELECT rank FROM (
         SELECT user_id, ROW_NUMBER() OVER (ORDER BY ${agg} DESC) AS rank
           FROM ${table} WHERE guild_id = ? AND ${tsColumn} > ? GROUP BY user_id
       ) WHERE user_id = ?`,
    )
    .get(gid, cut, uid) as { rank: number } | undefined;
  return row ? Number(row.rank) : null;
}
