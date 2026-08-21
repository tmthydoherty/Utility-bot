import "server-only";

import { getEconomyDb } from "@/lib/bot/tracker-stats";
import { earningBucket, itemLabel, sourceLabel } from "@/lib/economy/labels";

/**
 * The read-only view of the bot's economy database, for the member inspector.
 *
 * Everything here reads `economy.db` and never writes it — the same strictly
 * read-only arrangement the Overview uses for `tracking_data.db`, and it shares
 * that file's already-open handle (`getEconomyDb`) rather than opening a second
 * connection. The bot writes the ledger, inventory and balances continuously as
 * members earn and spend; this just aggregates what is already there. That is
 * why the inspector needs no hourly job and no new bot code: the log is the
 * ledger, and the ledger is always current.
 *
 * IDs are bound as BigInt for an exact 64-bit compare and any id column is
 * SELECTed `CAST(... AS TEXT)`, so a snowflake never loses its low digits to a
 * JS number. Points, counts and timestamps stay numbers — nowhere near the
 * safe-integer ceiling.
 */

export interface OwnedItem {
  itemKey: string;
  label: string;
  count: number;
  /** Most recent purchase, unix seconds. */
  lastTs: number;
}

export interface UsedItem {
  itemKey: string;
  label: string;
  count: number;
  /** Most recent use, unix seconds. */
  lastTs: number;
}

export interface EarningDay {
  /** UTC date, `YYYY-MM-DD`. */
  day: string;
  total: number;
  /** The sources that paid out that day, largest first. */
  sources: { bucket: string; points: number }[];
}

export interface SpendRow {
  label: string;
  /** A positive number — what left the balance. */
  amount: number;
  ts: number;
}

export interface UserEconomy {
  userId: string;
  balance: number;
  lifetime: number;
  firstSeen: number | null;
  itemsOwned: OwnedItem[];
  itemsUsed: UsedItem[];
  ownedCount: number;
  usedCount: number;
  /** Days with any earnings, newest first. */
  earningDays: EarningDay[];
  totalEarned: number;
  recentSpends: SpendRow[];
}

export interface EconomyTotals {
  /** Members holding at least one point. */
  holders: number;
  /** Every point currently held, summed. */
  pointsInCirculation: number;
  /** Items members currently hold (owned or in use). */
  itemsOwned: number;
}

function userBigInt(userId: string): bigint | null {
  try {
    return BigInt(userId);
  } catch {
    return null;
  }
}

/** True when the economy database is present and has been initialised. */
export function economyStatsAvailable(): boolean {
  const db = getEconomyDb();
  if (!db) return false;
  try {
    db.prepare("SELECT 1 FROM users LIMIT 1").get();
    return true;
  } catch {
    return false;
  }
}

/** Headline figures for the Economy landing strip; null if the store is down. */
export function readEconomyTotals(): EconomyTotals | null {
  const db = getEconomyDb();
  if (!db) return null;
  try {
    const balances = db
      .prepare(
        "SELECT COUNT(*) AS holders, COALESCE(SUM(points), 0) AS total FROM users WHERE points > 0",
      )
      .get() as { holders: number; total: number };
    const items = db
      .prepare("SELECT COUNT(*) AS c FROM inventory WHERE state IN ('owned','active')")
      .get() as { c: number };

    return {
      holders: balances.holders,
      pointsInCirculation: balances.total,
      itemsOwned: items.c,
    };
  } catch {
    return null;
  }
}

/**
 * Everything the inspector shows for one member.
 *
 * Returns null only when the database can't be read at all. A member who simply
 * has no economy activity comes back as a fully-formed object of zeros and
 * empty lists, so the page shows a clean "nothing yet" rather than an error.
 */
export function readUserEconomy(userId: string): UserEconomy | null {
  const db = getEconomyDb();
  if (!db) return null;

  const id = userBigInt(userId);
  if (id === null) return null;

  try {
    const user = db
      .prepare(
        "SELECT points, lifetime_points, first_seen_ts FROM users WHERE user_id = ?",
      )
      .get(id) as
      | { points: number; lifetime_points: number; first_seen_ts: number }
      | undefined;

    const ownedRows = db
      .prepare(
        `SELECT item_key, COUNT(*) AS count, MAX(purchased_ts) AS lastTs
         FROM inventory WHERE user_id = ? AND state IN ('owned','active')
         GROUP BY item_key ORDER BY lastTs DESC`,
      )
      .all(id) as { item_key: string; count: number; lastTs: number }[];

    const usedRows = db
      .prepare(
        `SELECT item_key, COUNT(*) AS count,
                MAX(COALESCE(activated_ts, purchased_ts)) AS lastTs
         FROM inventory WHERE user_id = ? AND state = 'consumed'
         GROUP BY item_key ORDER BY lastTs DESC`,
      )
      .all(id) as { item_key: string; count: number; lastTs: number }[];

    const earnRows = db
      .prepare(
        `SELECT date(ts, 'unixepoch') AS day, source, SUM(delta) AS points
         FROM points_ledger WHERE user_id = ? AND delta > 0
         GROUP BY day, source ORDER BY day DESC`,
      )
      .all(id) as { day: string; source: string; points: number }[];

    const spendRows = db
      .prepare(
        `SELECT source, delta, ts FROM points_ledger
         WHERE user_id = ? AND delta < 0 ORDER BY ts DESC LIMIT 20`,
      )
      .all(id) as { source: string; delta: number; ts: number }[];

    const itemsOwned: OwnedItem[] = ownedRows.map((row) => ({
      itemKey: row.item_key,
      label: itemLabel(row.item_key),
      count: row.count,
      lastTs: row.lastTs,
    }));
    const itemsUsed: UsedItem[] = usedRows.map((row) => ({
      itemKey: row.item_key,
      label: itemLabel(row.item_key),
      count: row.count,
      lastTs: row.lastTs,
    }));

    // Fold (day, source) rows into one entry per day, merging sources that map
    // to the same bucket (several refund reasons, say) and ordering each day's
    // sources by size.
    const dayMap = new Map<string, Map<string, number>>();
    let totalEarned = 0;
    for (const row of earnRows) {
      totalEarned += row.points;
      const bucket = earningBucket(row.source);
      const buckets = dayMap.get(row.day) ?? new Map<string, number>();
      buckets.set(bucket, (buckets.get(bucket) ?? 0) + row.points);
      dayMap.set(row.day, buckets);
    }
    const earningDays: EarningDay[] = [...dayMap.entries()].map(([day, buckets]) => ({
      day,
      total: [...buckets.values()].reduce((sum, n) => sum + n, 0),
      sources: [...buckets.entries()]
        .map(([bucket, points]) => ({ bucket, points }))
        .sort((a, b) => b.points - a.points),
    }));

    const recentSpends: SpendRow[] = spendRows.map((row) => ({
      label: sourceLabel(row.source),
      amount: -row.delta,
      ts: row.ts,
    }));

    return {
      userId,
      balance: user?.points ?? 0,
      lifetime: user?.lifetime_points ?? 0,
      firstSeen: user?.first_seen_ts ? user.first_seen_ts : null,
      itemsOwned,
      itemsUsed,
      ownedCount: itemsOwned.reduce((sum, item) => sum + item.count, 0),
      usedCount: itemsUsed.reduce((sum, item) => sum + item.count, 0),
      earningDays,
      totalEarned,
      recentSpends,
    };
  } catch {
    return null;
  }
}
