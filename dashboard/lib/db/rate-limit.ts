import "server-only";

import { getDb } from "./index";

/**
 * Fixed-window rate limiter.
 *
 * A sliding window would be more precise, but this dashboard has a handful of
 * legitimate users and the job here is to blunt automated abuse, not to meter
 * an API. A fixed window does that in one statement and cannot drift.
 */

export type Bucket = "signin" | "mutation";

const LIMITS: Record<Bucket, { max: number; windowMs: number }> = {
  // Ten sign-in attempts per IP per ten minutes. Discord OAuth means a real
  // person needs one.
  signin: { max: 10, windowMs: 10 * 60 * 1000 },
  // Sixty writes a minute is far above hand-driven use and far below a script.
  mutation: { max: 60, windowMs: 60 * 1000 },
};

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  /** Epoch ms when the current window rolls over. */
  resetAt: number;
}

export function checkRateLimit(bucket: Bucket, key: string): RateLimitResult {
  const { max, windowMs } = LIMITS[bucket];
  const now = Date.now();
  const windowStart = Math.floor(now / windowMs) * windowMs;
  const db = getDb();

  const row = db
    .prepare(
      `INSERT INTO rate_limit (bucket, key, window_start, count)
       VALUES (?, ?, ?, 1)
       ON CONFLICT (bucket, key) DO UPDATE SET
         count        = CASE WHEN rate_limit.window_start = excluded.window_start
                             THEN rate_limit.count + 1 ELSE 1 END,
         window_start = excluded.window_start
       RETURNING count`,
    )
    .get(bucket, key, windowStart) as { count: number } | undefined;

  const count = row?.count ?? 1;

  return {
    allowed: count <= max,
    remaining: Math.max(0, max - count),
    resetAt: windowStart + windowMs,
  };
}

/** Drops windows that have long since rolled over. Cheap; call opportunistically. */
export function pruneRateLimits(): void {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  getDb().prepare(`DELETE FROM rate_limit WHERE window_start < ?`).run(cutoff);
}

/**
 * The caller's IP.
 *
 * cloudflared is the only path to this server, and it always sets
 * CF-Connecting-IP — so unlike a bare `X-Forwarded-For`, that header cannot be
 * spoofed by the client here. The fallbacks exist only for local development.
 */
export function clientIp(headers: Headers): string {
  return (
    headers.get("cf-connecting-ip") ??
    headers.get("x-real-ip") ??
    headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
    "127.0.0.1"
  );
}
