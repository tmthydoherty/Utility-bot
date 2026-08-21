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

/**
 * The guild's active (non-archived) threads, including forum posts.
 *
 * Threads don't appear in the channel list, but the tracker logs per thread, so
 * the channel picker needs them to be searchable. One call returns every active
 * thread the bot can see. Archived threads aren't included — that would be a
 * fan-out of per-channel calls — so a long-dead thread stays unlisted. 403/404
 * degrade to an empty list rather than erroring the whole page.
 */
export async function getActiveThreads(guildId: string): Promise<DiscordChannel[]> {
  try {
    const data = await cached<{ threads: DiscordChannel[] }>(
      `threads:${guildId}`,
      `/guilds/${guildId}/threads/active`,
    );
    return data.threads ?? [];
  } catch (error) {
    if (error instanceof DiscordApiError && (error.status === 403 || error.status === 404)) {
      return [];
    }
    throw error;
  }
}

/**
 * A single channel by ID, or null when it is gone or hidden from the bot.
 *
 * The guild channel list doesn't include threads, and the activity tracker logs
 * per thread, so a top-thread lookup falls through to this. 404 (deleted) and
 * 403 (the bot can't see it) are both "no name to show", not errors to surface.
 */
export async function getChannel(channelId: string): Promise<DiscordChannel | null> {
  try {
    return await cached<DiscordChannel>(`channel:${channelId}`, `/channels/${channelId}`);
  } catch (error) {
    if (error instanceof DiscordApiError && (error.status === 404 || error.status === 403)) {
      return null;
    }
    throw error;
  }
}

export function getRoles(guildId: string): Promise<DiscordRole[]> {
  return cached(`roles:${guildId}`, `/guilds/${guildId}/roles`);
}

export function getEmojis(guildId: string): Promise<DiscordEmoji[]> {
  return cached(`emojis:${guildId}`, `/guilds/${guildId}/emojis`);
}

/**
 * Every server the bot is in, as partial guilds (id + name is all we need).
 *
 * Used to gather custom emoji from all of them — a button can carry an emoji
 * from any server the bot shares, not just the one being administered.
 */
export function getBotGuilds(): Promise<{ id: string; name: string }[]> {
  return cached("bot-guilds", "/users/@me/guilds");
}

/**
 * A page of guild members, for the tracker's user picker.
 *
 * Needs the GUILD_MEMBERS privileged intent; the bot has it (it tracks joins).
 * If it's ever off Discord answers 403, which we treat as "no roster to offer"
 * — the picker falls back to whoever is already selected rather than erroring.
 * One call, cached 60s like the rest; `limit` caps at Discord's 1000.
 */
export async function listMembers(
  guildId: string,
  limit = 1000,
): Promise<DiscordMember[]> {
  try {
    return await cached<DiscordMember[]>(
      `members:${guildId}:${limit}`,
      `/guilds/${guildId}/members?limit=${limit}`,
    );
  } catch (error) {
    if (error instanceof DiscordApiError && (error.status === 403 || error.status === 404)) {
      return [];
    }
    throw error;
  }
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
