/**
 * The Custom Commands module's domain types.
 *
 * IDs are strings throughout: a Discord snowflake is a 64-bit integer that
 * `JSON.parse` would silently round, so the store reads JSON columns with
 * `parseJsonPreservingIds` and everything written back is a string the Python
 * side coerces with `int()`. The embed reuses the Utility builder's `Embed`
 * shape so a command can share the same rich editor a reminder uses.
 */

import type { Embed } from "@/lib/utility/types";

export type { Embed };

/** How a command decides a message triggers it. */
export type MatchType = "exact" | "startswith" | "contains";

/** Where the response lands. */
export type Delivery = "channel" | "reply" | "dm";

/** What the per-command cooldown is counted against. */
export type CooldownScope = "user" | "channel" | "guild";

/** One admin-authored custom command. */
export interface CustomCommand {
  id: string;
  guildId: string;
  /** The bare trigger word, lowercased, with no `!`. */
  name: string;
  enabled: boolean;
  matchType: MatchType;
  /** One is chosen at random each time the command fires. */
  responses: string[];
  plainText: boolean;
  embed: Embed;
  delivery: Delivery;
  deleteTrigger: boolean;
  reactEmoji: string;
  allowedRoleIds: string[];
  deniedRoleIds: string[];
  allowedChannelIds: string[];
  deniedChannelIds: string[];
  cooldownSeconds: number;
  cooldownScope: CooldownScope;
  useCount: number;
  lastUsedAt: number;
  createdAt: number;
  updatedAt: number;
}

/**
 * A GIF command a member bought and had approved through the economy shop.
 *
 * These live in economy.db and are shown alongside the admin commands, marked
 * "*econ purchased". A moderator can disable or delete one from here; because
 * the dashboard can't write economy.db, that intent is queued for the cog to
 * apply, and `pending` reflects an intent not yet carried out.
 */
export interface GifCommand {
  /** The trigger word, no `!`. */
  name: string;
  ownerId: string;
  url: string;
  approvedAt: number;
  disabled: boolean;
  /** A queued moderation action the cog has not applied yet, if any. */
  pending: "disable" | "enable" | "delete" | null;
}

/** A command in the unified, alphabetised list — one of ours or a purchased GIF. */
export type ListEntry =
  | { kind: "custom"; command: CustomCommand }
  | { kind: "gif"; gif: GifCommand };
