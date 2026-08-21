import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

import { env } from "@/lib/env";

/**
 * Live wiring for the Game Poll cog.
 *
 * Same arrangement as the other bridges (see lib/bot/module-config.ts): the
 * file belongs to the bot and is never created here — `fileMustExist` means a
 * missing database is reported as "the bot isn't running" rather than silently
 * conjuring an empty one the cog will never read. Reads degrade to empty; the
 * write path throws `GamePollUnavailable` and the action surfaces it.
 */

const DB_PATH =
  env.VIBEY_GAME_POLL_DB ??
  path.join(process.cwd(), "..", "data", "game_poll_config", "game_poll.db");

export class GamePollUnavailable extends Error {
  constructor(cause?: unknown) {
    super("The bot's game poll database could not be opened.");
    this.name = "GamePollUnavailable";
    this.cause = cause;
  }
}

let db: Database.Database | null = null;

/** The writer entry point: throws `GamePollUnavailable` when the bot is down. */
export function getDb() {
  if (!db) {
    if (!fs.existsSync(DB_PATH)) throw new GamePollUnavailable(new Error(`no database at ${DB_PATH}`));
    db = new Database(DB_PATH, { fileMustExist: true });
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");
    db.pragma("busy_timeout = 5000");
  }
  return db;
}

/** The reader entry point: null instead of throwing, so a page still renders. */
function readDb(): Database.Database | null {
  try {
    return getDb();
  } catch {
    return null;
  }
}

export interface Game {
  id: number;
  name: string;
  banner_url: string | null;
}

export function getGames(): Game[] {
  const db = readDb();
  if (!db) return [];
  return db.prepare("SELECT * FROM games ORDER BY name ASC").all() as Game[];
}

export function addGame(name: string, banner_url: string | null) {
  const db = getDb();
  db.prepare("INSERT INTO games (name, banner_url) VALUES (?, ?)").run(name, banner_url);
}

export function updateGame(id: number, name: string, banner_url: string | null) {
  const db = getDb();
  db.prepare("UPDATE games SET name = ?, banner_url = ? WHERE id = ?").run(name, banner_url, id);
}

export function deleteGame(id: number) {
  const db = getDb();
  db.prepare("DELETE FROM games WHERE id = ?").run(id);
}

// We need to parse BIGINT out for user_ids safely.
function parseBigInts<T>(rows: Record<string, unknown>[]): T[] {
  return rows.map((r) => {
    const copy = { ...r };
    for (const k in copy) {
      if (typeof copy[k] === "bigint") copy[k] = copy[k].toString();
      // SQLite returns number for 53-bit ints, so we must enforce string conversion for ID columns
      if ((k === "user_id" || k === "poll_id") && typeof copy[k] === "number") {
         copy[k] = copy[k].toString();
      }
    }
    return copy as T;
  });
}

export interface HistoryRecord {
  id: number;
  session_date: number;
  user_id: string;
  user_name: string | null;
  duration: number;
}

export function getGameNightHistory(): HistoryRecord[] {
  const db = readDb();
  if (!db) return [];
  const rows = db.prepare("SELECT * FROM game_night_history ORDER BY session_date DESC").all();
  return parseBigInts(rows as Record<string, unknown>[]) as HistoryRecord[];
}

// ============================================================ VOTING HISTORY
//
// Everything below reads the vote ledger the cog keeps but never surfaced: the
// `votes` table (one row per ranked pick), `poll_games` (which games ran each
// week) and `poll_results` (the closed-poll snapshot, now stamped with when it
// closed and the weights in force). We rebuild the whole picture from the raw
// votes so a single code path covers every poll — including the earliest ones
// that predate the results snapshot — and only lean on `poll_results` for the
// name a game had at the time and the week's date.

/** The four kinds of vote a game can receive, ordered weakest → strongest. */
export type VoteKind = "third" | "second" | "first" | "firstBoost";

/** Points a ranked pick is worth under a given weight set. */
function pointsFor(rank: number, mult: number, w: VoteWeights): number {
  const base = rank === 1 ? w.first : rank === 2 ? w.second : rank === 3 ? w.third : 0;
  // A returning player's boosted 1st counts double — the cog only ever sets the
  // multiplier flag on a rank-1 pick, so doubling here mirrors it exactly.
  return mult ? base * 2 : base;
}

export interface VoteWeights {
  first: number;
  second: number;
  third: number;
}

export function getVoteWeights(): VoteWeights {
  const db = readDb();
  if (!db) return { first: 3, second: 2, third: 1 };
  const rows = db.prepare("SELECT key, value FROM settings WHERE key IN ('weight_1','weight_2','weight_3')").all() as {
    key: string;
    value: string;
  }[];
  const map = new Map(rows.map((r) => [r.key, Number(r.value)]));
  return {
    first: map.get("weight_1") ?? 3,
    second: map.get("weight_2") ?? 2,
    third: map.get("weight_3") ?? 1,
  };
}

/** A game's tally within one poll (one week). */
export interface GameTally {
  gameId: number;
  name: string;
  points: number;
  votes: number;
  third: number;
  second: number;
  first: number;
  firstBoost: number;
}

/** One closed poll — a week's outcome. */
export interface PollWeek {
  pollId: number;
  endedAt: number | null;
  voters: number;
  totalVotes: number;
  totalPoints: number;
  games: GameTally[]; // ranked, highest points first
  winner: GameTally | null;
  /** The full ballot: each voter's ranked picks that week. */
  ballots: Ballot[];
}

export interface BallotPick {
  gameId: number;
  name: string;
  rank: number;
  kind: VoteKind;
  points: number;
}

export interface Ballot {
  userId: string;
  points: number;
  picks: BallotPick[]; // ordered 1st → 3rd
}

/** A game's all-time record across every poll it appeared in. */
export interface GameRecord {
  gameId: number;
  name: string;
  banner: string | null;
  totalPoints: number;
  totalVotes: number;
  third: number;
  second: number;
  first: number;
  firstBoost: number;
  appearances: number;
  wins: number;
}

/** A member's all-time voting record. */
export interface VoterRecord {
  userId: string;
  totalPoints: number;
  totalVotes: number;
  pollsVotedIn: number;
  boosts: number; // times their 1st-place carried the returning-player 2x
  /** Points this voter sent to each game, highest first. */
  favourites: { gameId: number; name: string; points: number; votes: number }[];
}

export interface VotingHistory {
  weeks: PollWeek[]; // most recent poll first
  games: GameRecord[]; // ranked by all-time points
  voters: VoterRecord[]; // ranked by all-time points contributed
  weights: VoteWeights;
  totals: {
    polls: number;
    votesCast: number;
    pointsAwarded: number;
    voters: number;
  };
  /** Every distinct voter id, for the page to resolve names + avatars. */
  userIds: string[];
}

interface RawVote {
  poll_id: number;
  user_id: string;
  game_id: number;
  rank: number;
  multiplier_used: number;
}

const KIND_BY: (rank: number, mult: number) => VoteKind = (rank, mult) =>
  rank === 1 ? (mult ? "firstBoost" : "first") : rank === 2 ? "second" : "third";

export function getVotingHistory(): VotingHistory {
  const weights = getVoteWeights();
  const db = readDb();
  if (!db) {
    return {
      weeks: [],
      games: [],
      voters: [],
      weights,
      totals: { polls: 0, votesCast: 0, pointsAwarded: 0, voters: 0 },
      userIds: [],
    };
  }

  const games = getGames();
  const gameName = new Map(games.map((g) => [g.id, g.name] as const));
  const gameBanner = new Map(games.map((g) => [g.id, g.banner_url] as const));

  // Poll-level metadata: names as they were at close, the week's date, and the
  // weights that produced its points. Falls back to current values where a poll
  // predates the stamped snapshot.
  interface PollMeta {
    endedAt: number | null;
    weights: VoteWeights;
    names: Map<number, string>;
  }
  const pollMeta = new Map<number, PollMeta>();
  const resultRows = db
    .prepare("SELECT poll_id, data_json, ended_at, weight_1, weight_2, weight_3 FROM poll_results")
    .all() as {
    poll_id: number;
    data_json: string;
    ended_at: number | null;
    weight_1: number | null;
    weight_2: number | null;
    weight_3: number | null;
  }[];
  for (const r of resultRows) {
    const names = new Map<number, string>();
    try {
      const data = JSON.parse(r.data_json) as Record<string, { name?: string; game_id?: number }>;
      for (const entry of Object.values(data)) {
        if (typeof entry.game_id === "number" && entry.name) names.set(entry.game_id, entry.name);
      }
    } catch {
      /* malformed snapshot — fall back to current names below */
    }
    pollMeta.set(r.poll_id, {
      endedAt: r.ended_at ?? null,
      weights: {
        first: r.weight_1 ?? weights.first,
        second: r.weight_2 ?? weights.second,
        third: r.weight_3 ?? weights.third,
      },
      names,
    });
  }

  const nameFor = (pollId: number, gameId: number): string =>
    pollMeta.get(pollId)?.names.get(gameId) ?? gameName.get(gameId) ?? `Game #${gameId}`;
  const weightsFor = (pollId: number): VoteWeights => pollMeta.get(pollId)?.weights ?? weights;

  // user_id is a Discord snowflake — larger than 2^53, so better-sqlite3's
  // default number binding silently rounds it and the id no longer matches any
  // real member (everyone resolves as "former"). CAST to TEXT hands us the exact
  // decimal string straight from SQLite.
  const votes = parseBigInts<RawVote>(
    db
      .prepare(
        "SELECT poll_id, CAST(user_id AS TEXT) AS user_id, game_id, rank, multiplier_used FROM votes",
      )
      .all() as Record<string, unknown>[],
  );

  // --- Per-poll aggregation --------------------------------------------------
  const byPoll = new Map<number, RawVote[]>();
  for (const v of votes) {
    if (!byPoll.has(v.poll_id)) byPoll.set(v.poll_id, []);
    byPoll.get(v.poll_id)!.push(v);
  }

  const weeks: PollWeek[] = [];
  for (const [pollId, pollVotes] of byPoll) {
    const w = weightsFor(pollId);
    const tallies = new Map<number, GameTally>();
    const ballotMap = new Map<string, Ballot>();

    const ensureTally = (gameId: number): GameTally => {
      let t = tallies.get(gameId);
      if (!t) {
        t = { gameId, name: nameFor(pollId, gameId), points: 0, votes: 0, third: 0, second: 0, first: 0, firstBoost: 0 };
        tallies.set(gameId, t);
      }
      return t;
    };

    for (const v of pollVotes) {
      const kind = KIND_BY(v.rank, v.multiplier_used);
      const pts = pointsFor(v.rank, v.multiplier_used, w);
      const t = ensureTally(v.game_id);
      t.points += pts;
      t.votes += 1;
      t[kind] += 1;

      let b = ballotMap.get(v.user_id);
      if (!b) {
        b = { userId: v.user_id, points: 0, picks: [] };
        ballotMap.set(v.user_id, b);
      }
      b.points += pts;
      b.picks.push({ gameId: v.game_id, name: nameFor(pollId, v.game_id), rank: v.rank, kind, points: pts });
    }

    const games = [...tallies.values()].sort((a, b) => b.points - a.points || b.first - a.first);
    const ballots = [...ballotMap.values()].sort((a, b) => b.points - a.points);
    for (const b of ballots) b.picks.sort((a, c) => a.rank - c.rank);

    weeks.push({
      pollId,
      endedAt: pollMeta.get(pollId)?.endedAt ?? null,
      voters: ballotMap.size,
      totalVotes: pollVotes.length,
      totalPoints: games.reduce((s, g) => s + g.points, 0),
      games,
      winner: games[0] ?? null,
      ballots,
    });
  }
  weeks.sort((a, b) => b.pollId - a.pollId);

  // --- Per-game all-time record ---------------------------------------------
  const gameRec = new Map<number, GameRecord>();
  const ensureGame = (gameId: number, pollId: number): GameRecord => {
    let g = gameRec.get(gameId);
    if (!g) {
      g = {
        gameId,
        name: gameName.get(gameId) ?? nameFor(pollId, gameId),
        banner: gameBanner.get(gameId) ?? null,
        totalPoints: 0,
        totalVotes: 0,
        third: 0,
        second: 0,
        first: 0,
        firstBoost: 0,
        appearances: 0,
        wins: 0,
      };
      gameRec.set(gameId, g);
    }
    return g;
  };
  for (const week of weeks) {
    for (const t of week.games) {
      const g = ensureGame(t.gameId, week.pollId);
      g.totalPoints += t.points;
      g.totalVotes += t.votes;
      g.third += t.third;
      g.second += t.second;
      g.first += t.first;
      g.firstBoost += t.firstBoost;
      g.appearances += 1;
    }
    if (week.winner) ensureGame(week.winner.gameId, week.pollId).wins += 1;
  }
  const gameRecords = [...gameRec.values()].sort((a, b) => b.totalPoints - a.totalPoints);

  // --- Per-voter all-time record --------------------------------------------
  interface VoterAcc {
    userId: string;
    totalPoints: number;
    totalVotes: number;
    polls: Set<number>;
    boosts: number;
    perGame: Map<number, { name: string; points: number; votes: number }>;
  }
  const voterAcc = new Map<string, VoterAcc>();
  for (const week of weeks) {
    for (const b of week.ballots) {
      let acc = voterAcc.get(b.userId);
      if (!acc) {
        acc = { userId: b.userId, totalPoints: 0, totalVotes: 0, polls: new Set(), boosts: 0, perGame: new Map() };
        voterAcc.set(b.userId, acc);
      }
      acc.polls.add(week.pollId);
      for (const pick of b.picks) {
        acc.totalPoints += pick.points;
        acc.totalVotes += 1;
        if (pick.kind === "firstBoost") acc.boosts += 1;
        let pg = acc.perGame.get(pick.gameId);
        if (!pg) {
          pg = { name: gameName.get(pick.gameId) ?? pick.name, points: 0, votes: 0 };
          acc.perGame.set(pick.gameId, pg);
        }
        pg.points += pick.points;
        pg.votes += 1;
      }
    }
  }
  const voterRecords: VoterRecord[] = [...voterAcc.values()]
    .map((acc) => ({
      userId: acc.userId,
      totalPoints: acc.totalPoints,
      totalVotes: acc.totalVotes,
      pollsVotedIn: acc.polls.size,
      boosts: acc.boosts,
      favourites: [...acc.perGame.entries()]
        .map(([gameId, v]) => ({ gameId, ...v }))
        .sort((a, b) => b.points - a.points),
    }))
    .sort((a, b) => b.totalPoints - a.totalPoints);

  return {
    weeks,
    games: gameRecords,
    voters: voterRecords,
    weights,
    totals: {
      polls: weeks.length,
      votesCast: votes.length,
      pointsAwarded: weeks.reduce((s, w) => s + w.totalPoints, 0),
      voters: voterRecords.length,
    },
    userIds: voterRecords.map((v) => v.userId),
  };
}
