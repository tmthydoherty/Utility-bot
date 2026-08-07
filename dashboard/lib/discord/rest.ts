import "server-only";

import { env } from "@/lib/env";
import {
  type DiscordChannel,
  type DiscordEmoji,
  type DiscordGuild,
  type DiscordMember,
  type DiscordRole,
} from "./types";

/**
 * Bot-token Discord client.
 *
 * Every call in here is authenticated as *the bot*, never as the signed-in
 * user. That is deliberate: the user's OAuth token is thrown away right after
 * sign-in, so nothing in the session cookie can be replayed against Discord if
 * it ever leaks, and role checks can't be spoofed by a user who revoked the
 * app's access but still holds a valid cookie.
 *
 * `server-only` makes importing this from a client component a build error
 * rather than a token in the browser bundle.
 */

const API = "https://discord.com/api/v10";

/** Guild structure barely changes; 60s keeps pickers instant without going stale. */
const TTL_MS = 60_000;

type CacheEntry = { value: unknown; expires: number };
const cache = new Map<string, CacheEntry>();
/** De-duplicates concurrent misses so ten pickers rendering at once make one call. */
const inflight = new Map<string, Promise<unknown>>();

export class DiscordApiError extends Error {
  constructor(
    readonly status: number,
    readonly path: string,
    message: string,
  ) {
    super(`Discord ${status} on ${path}: ${message}`);
    this.name = "DiscordApiError";
  }
}

async function request<T>(path: string, attempt = 0): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    headers: {
      Authorization: `Bot ${env.DISCORD_BOT_TOKEN}`,
      "User-Agent": "VibeyDashboard (https://vibe-y.us, 0.1.0)",
    },
    // Next's fetch cache would hold responses past the point they're true and
    // is keyed without the auth header; the in-process cache below is the one
    // source of caching so there is only one thing to reason about.
    cache: "no-store",
  });

  if (res.status === 429 && attempt < 2) {
    // Discord tells us exactly how long to wait. Honour it rather than
    // hammering — a bucket exhausted by the dashboard is a bucket the bot
    // cannot use either.
    const retryAfter = Number(res.headers.get("retry-after") ?? "1");
    await new Promise((r) => setTimeout(r, Math.min(retryAfter * 1000, 5000)));
    return request<T>(path, attempt + 1);
  }

  if (!res.ok) {
    throw new DiscordApiError(res.status, path, await res.text().catch(() => res.statusText));
  }

  return (await res.json()) as T;
}

async function cached<T>(key: string, path: string): Promise<T> {
  const hit = cache.get(key);
  if (hit && hit.expires > Date.now()) return hit.value as T;

  const pending = inflight.get(key);
  if (pending) return pending as Promise<T>;

  const promise = request<T>(path)
    .then((value) => {
      cache.set(key, { value, expires: Date.now() + TTL_MS });
      return value;
    })
    .finally(() => {
      inflight.delete(key);
    });

  inflight.set(key, promise);
  return promise;
}

/** Drops cached guild structure — call after any write that changes it. */
export function invalidateGuild(guildId: string): void {
  for (const key of cache.keys()) {
    if (key.includes(guildId)) cache.delete(key);
  }
}

export function getGuild(guildId: string): Promise<DiscordGuild> {
  return cached(`guild:${guildId}`, `/guilds/${guildId}?with_counts=true`);
}

export function getChannels(guildId: string): Promise<DiscordChannel[]> {
  return cached(`channels:${guildId}`, `/guilds/${guildId}/channels`);
}

export function getRoles(guildId: string): Promise<DiscordRole[]> {
  return cached(`roles:${guildId}`, `/guilds/${guildId}/roles`);
}

export function getEmojis(guildId: string): Promise<DiscordEmoji[]> {
  return cached(`emojis:${guildId}`, `/guilds/${guildId}/emojis`);
}

/**
 * A member, or null when they are not in the guild.
 *
 * 404 is a legitimate answer to "is this person a member" and must not be an
 * exception, or the authorisation path can't tell "not a member" apart from
 * "Discord is down" — and those two need opposite handling.
 */
export async function getMember(
  guildId: string,
  userId: string,
): Promise<DiscordMember | null> {
  try {
    return await request<DiscordMember>(`/guilds/${guildId}/members/${userId}`);
  } catch (error) {
    if (error instanceof DiscordApiError && error.status === 404) return null;
    throw error;
  }
}
