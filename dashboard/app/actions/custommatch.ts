"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/custommatch/store";
import { readPlayerProfile, type CmPlayerProfile } from "@/lib/custommatch/read";
import { resolveUsers } from "@/lib/bot/directory";

/**
 * Every Custom Matches mutation the dashboard can perform: game settings, adding
 * / cloning / deleting a game, the global channels and roles, the rank ladder,
 * and per-player MMR, IGN, penalties and suspensions. Like the onboarding
 * actions, each re-authenticates, re-authorises the guild, rate-limits and
 * records what changed — regardless of what the client sent — then queues an
 * imperative command onto the bot's bridge. The cog drains it within about ten
 * seconds; nothing here writes the custom-match database directly (that file is
 * read strictly read-only, see lib/custommatch/read.ts).
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
}

async function guard(
  guildId: string,
): Promise<{ ok: true; actorId: string; actorName: string; ip: string; userAgent: string } | { ok: false; result: ActionResult }> {
  const session = await getActiveSession();
  if (!session) {
    return { ok: false, result: { ok: false, error: "Your session has expired. Sign in again." } };
  }
  if (guildId !== env.VIBEY_GUILD_ID) {
    return { ok: false, result: { ok: false, error: "Unknown server." } };
  }
  const headerList = await headers();
  const limit = checkRateLimit("mutation", session.user.id);
  if (!limit.allowed) {
    const seconds = Math.ceil((limit.resetAt - Date.now()) / 1000);
    return {
      ok: false,
      result: { ok: false, error: `Too many changes at once. Try again in ${seconds}s.` },
    };
  }
  return {
    ok: true,
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    ip: clientIp(headerList),
    userAgent: headerList.get("user-agent") ?? "",
  };
}

function unavailable(error: unknown): ActionResult {
  if (error instanceof store.CustomMatchUnavailable) {
    return {
      ok: false,
      error: "Vibey isn't running, so this change can't be sent right now. It'll need to be up.",
    };
  }
  console.error("[custommatch] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function revalidate(guildId: string): void {
  revalidatePath(`/dashboard/${guildId}/custom-matches`, "layout");
}

async function run(
  guildId: string,
  action: string,
  target: string,
  changes: Record<string, { from: unknown; to: unknown }>,
  queue: (actorId: string) => void,
): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;
  try {
    queue(actorId);
    recordAudit({ actorId, actorName, guildId, action, target, changes, ip, userAgent });
  } catch (error) {
    return unavailable(error);
  }
  revalidate(guildId);
  return { ok: true };
}

function isSnowflake(value: string): boolean {
  return /^\d{17,20}$/.test(value);
}

// The game columns a dashboard edit is allowed to set. Mirrors the bot's
// VALID_GAME_COLUMNS minus the live-message pointers, which are the bot's own
// bookkeeping and must never be written from here.
const ALLOWED_GAME_FIELDS = new Set([
  "name",
  "player_count",
  "queue_type",
  "captain_selection",
  "enabled",
  "queue_channel_id",
  "verified_role_id",
  "game_channel_id",
  "leaderboard_channel_id",
  "category_id",
  "lf1_channel_id",
  "vc_creation_enabled",
  "queue_role_required",
  "dm_ready_up",
  "ign_required",
  "role_required",
  "pc_enabled",
  "pc_offset_tiers",
  "ready_timer_seconds",
  "grace_period_minutes",
  "not_ready_cooldown_minutes",
  "queue_timeout_minutes",
  "penalty_1st_minutes",
  "penalty_2nd_minutes",
  "penalty_3rd_minutes",
  "penalty_decay_days",
  "decline_1st_minutes",
  "decline_2nd_minutes",
  "decline_3rd_minutes",
  "banner_url",
  "short_name",
  "ready_loading_emoji",
  "ready_done_emoji",
  "verification_topic",
  "schedule_enabled",
  "schedule_times",
  "secondary_queue_enabled",
  "secondary_queue_name",
  "secondary_queue_player_count",
  "secondary_queue_type",
  "secondary_queue_channel_id",
  "secondary_queue_match_limit",
  "secondary_banner_url",
  "secondary_schedule_times",
]);

const GLOBAL_KEYS = new Set([
  "log_channel_id",
  "cm_admin_channel_id",
  "cm_admin_role_id",
  "cm_discussion_parent_channel_id",
  "category_id",
  "rivals_admin_channel_id",
]);

/** Keep only the fields the schema owns, so a stray key can never reach the bot. */
function cleanGameFields(fields: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (ALLOWED_GAME_FIELDS.has(key)) out[key] = value;
  }
  return out;
}

// --- Games ---

export async function updateGame(
  guildId: string,
  gameId: number,
  fields: Record<string, unknown>,
): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  const clean = cleanGameFields(fields);
  if (Object.keys(clean).length === 0) return { ok: false, error: "Nothing to change." };
  return run(
    guildId, "custommatch.game.update", String(gameId),
    { fields: { from: null, to: Object.keys(clean).join(", ") } },
    (actorId) => store.queueCommand("game_update", { game_id: gameId, fields: clean }, actorId),
  );
}

export async function addGame(
  guildId: string,
  name: string,
  playerCount: number,
  queueType: string,
  captainSelection: string,
  fields: Record<string, unknown> = {},
): Promise<ActionResult> {
  const trimmed = name.trim().slice(0, 100);
  if (!trimmed) return { ok: false, error: "Give the game a name." };
  if (!Number.isInteger(playerCount) || playerCount < 2 || playerCount > 40)
    return { ok: false, error: "Player count must be between 2 and 40." };
  return run(
    guildId, "custommatch.game.add", trimmed,
    { name: { from: null, to: trimmed } },
    (actorId) =>
      store.queueCommand(
        "game_add",
        {
          name: trimmed,
          player_count: playerCount,
          queue_type: queueType,
          captain_selection: captainSelection,
          fields: cleanGameFields(fields),
        },
        actorId,
      ),
  );
}

export async function cloneGame(
  guildId: string,
  sourceGameId: number,
  name: string,
): Promise<ActionResult> {
  if (!Number.isInteger(sourceGameId) || sourceGameId <= 0)
    return { ok: false, error: "Unknown source game." };
  const trimmed = name.trim().slice(0, 100);
  if (!trimmed) return { ok: false, error: "Name the new copy." };
  return run(
    guildId, "custommatch.game.clone", trimmed,
    { source: { from: sourceGameId, to: trimmed } },
    (actorId) =>
      store.queueCommand("game_clone", { source_game_id: sourceGameId, name: trimmed }, actorId),
  );
}

export async function deleteGame(guildId: string, gameId: number): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  return run(
    guildId, "custommatch.game.delete", String(gameId),
    { game: { from: gameId, to: null } },
    (actorId) => store.queueCommand("game_delete", { game_id: gameId }, actorId),
  );
}

// --- Global config ---

export async function setGlobal(
  guildId: string,
  key: string,
  value: string | null,
): Promise<ActionResult> {
  if (!GLOBAL_KEYS.has(key)) return { ok: false, error: "Unknown setting." };
  if (value && !isSnowflake(value)) return { ok: false, error: "Pick a valid channel or role." };
  return run(
    guildId, "custommatch.global.set", key,
    { [key]: { from: null, to: value } },
    (actorId) => store.queueCommand("global_set", { key, value: value ?? "" }, actorId),
  );
}

export async function addModRole(guildId: string, roleId: string): Promise<ActionResult> {
  if (!isSnowflake(roleId)) return { ok: false, error: "Pick a role." };
  return run(
    guildId, "custommatch.modrole.add", roleId,
    { role: { from: null, to: roleId } },
    (actorId) => store.queueCommand("mod_role_add", { role_id: roleId }, actorId),
  );
}

export async function removeModRole(guildId: string, roleId: string): Promise<ActionResult> {
  if (!isSnowflake(roleId)) return { ok: false, error: "Unknown role." };
  return run(
    guildId, "custommatch.modrole.remove", roleId,
    { role: { from: roleId, to: null } },
    (actorId) => store.queueCommand("mod_role_remove", { role_id: roleId }, actorId),
  );
}

export async function blacklistAdd(
  guildId: string,
  playerId: string,
  until: string | null = null,
): Promise<ActionResult> {
  if (!isSnowflake(playerId)) return { ok: false, error: "Pick a valid member." };
  return run(
    guildId, "custommatch.blacklist.add", playerId,
    { member: { from: null, to: playerId } },
    (actorId) => store.queueCommand("blacklist_add", { player_id: playerId, until }, actorId),
  );
}

export async function blacklistRemove(guildId: string, playerId: string): Promise<ActionResult> {
  if (!isSnowflake(playerId)) return { ok: false, error: "Unknown member." };
  return run(
    guildId, "custommatch.blacklist.remove", playerId,
    { member: { from: playerId, to: null } },
    (actorId) => store.queueCommand("blacklist_remove", { player_id: playerId }, actorId),
  );
}

// --- Rank ladder ---

export async function setRank(
  guildId: string,
  gameId: number,
  roleId: string,
  mmrValue: number,
  label: string | null,
): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  if (!isSnowflake(roleId)) return { ok: false, error: "Pick a rank role." };
  if (!Number.isFinite(mmrValue)) return { ok: false, error: "Enter an MMR value." };
  const trimmedLabel = (label ?? "").trim().slice(0, 50) || null;
  return run(
    guildId, "custommatch.rank.set", `${gameId}:${roleId}`,
    { mmr: { from: null, to: Math.round(mmrValue) } },
    (actorId) =>
      store.queueCommand(
        "rank_set",
        { game_id: gameId, role_id: roleId, mmr_value: Math.round(mmrValue), label: trimmedLabel },
        actorId,
      ),
  );
}

export async function removeRank(
  guildId: string,
  gameId: number,
  roleId: string,
): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  if (!isSnowflake(roleId)) return { ok: false, error: "Unknown rank." };
  return run(
    guildId, "custommatch.rank.remove", `${gameId}:${roleId}`,
    { rank: { from: roleId, to: null } },
    (actorId) => store.queueCommand("rank_remove", { game_id: gameId, role_id: roleId }, actorId),
  );
}

// --- Players ---

export async function setPlayerMmr(
  guildId: string,
  gameId: number,
  playerId: string,
  mmr: number,
): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  if (!isSnowflake(playerId)) return { ok: false, error: "Pick a valid member." };
  if (!Number.isFinite(mmr)) return { ok: false, error: "Enter an MMR value." };
  return run(
    guildId, "custommatch.player.mmr", `${gameId}:${playerId}`,
    { mmr: { from: null, to: Math.round(mmr) } },
    (actorId) =>
      store.queueCommand("player_mmr_set", { game_id: gameId, player_id: playerId, mmr: Math.round(mmr) }, actorId),
  );
}

export async function setPlayerOffset(
  guildId: string,
  gameId: number,
  playerId: string,
  offset: number,
): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  if (!isSnowflake(playerId)) return { ok: false, error: "Pick a valid member." };
  if (!Number.isFinite(offset)) return { ok: false, error: "Enter an offset." };
  return run(
    guildId, "custommatch.player.offset", `${gameId}:${playerId}`,
    { offset: { from: null, to: Math.round(offset) } },
    (actorId) =>
      store.queueCommand(
        "player_offset_set",
        { game_id: gameId, player_id: playerId, admin_offset: Math.round(offset) },
        actorId,
      ),
  );
}

export async function setPlayerIgn(
  guildId: string,
  gameId: number,
  playerId: string,
  ign: string,
): Promise<ActionResult> {
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  if (!isSnowflake(playerId)) return { ok: false, error: "Pick a valid member." };
  const trimmed = ign.trim().slice(0, 100);
  if (!trimmed) return { ok: false, error: "Enter an IGN." };
  return run(
    guildId, "custommatch.player.ign", `${gameId}:${playerId}`,
    { ign: { from: null, to: trimmed } },
    (actorId) =>
      store.queueCommand("player_ign_set", { game_id: gameId, player_id: playerId, ign: trimmed }, actorId),
  );
}

export async function clearPenalty(
  guildId: string,
  playerId: string,
  kind: "ready" | "decline",
): Promise<ActionResult> {
  if (!isSnowflake(playerId)) return { ok: false, error: "Unknown member." };
  return run(
    guildId, "custommatch.penalty.clear", playerId,
    { penalty: { from: kind, to: null } },
    (actorId) => store.queueCommand("penalty_clear", { player_id: playerId, kind }, actorId),
  );
}

export async function removeSuspension(
  guildId: string,
  suspensionId: number,
): Promise<ActionResult> {
  if (!Number.isInteger(suspensionId) || suspensionId <= 0)
    return { ok: false, error: "Unknown suspension." };
  return run(
    guildId, "custommatch.suspension.remove", String(suspensionId),
    { suspension: { from: suspensionId, to: null } },
    (actorId) => store.queueCommand("suspension_remove", { suspension_id: suspensionId }, actorId),
  );
}

// --- Player profile lookup (read) ---

export interface PlayerProfileResult {
  ok: boolean;
  error?: string;
  profile?: CmPlayerProfile;
  /** opponent player id → display name, for the rivalries list. */
  names?: Record<string, string>;
}

/**
 * Read one player's profile for a game, on demand. A read, not a mutation, so it
 * only checks the session and the guild — no rate limit, no audit — and resolves
 * the rivals' display names in the same round the profile is fetched.
 */
export async function lookupPlayerProfile(
  guildId: string,
  gameId: number,
  playerId: string,
): Promise<PlayerProfileResult> {
  const session = await getActiveSession();
  if (!session) return { ok: false, error: "Your session has expired. Sign in again." };
  if (guildId !== env.VIBEY_GUILD_ID) return { ok: false, error: "Unknown server." };
  if (!Number.isInteger(gameId) || gameId <= 0) return { ok: false, error: "Unknown game." };
  if (!isSnowflake(playerId)) return { ok: false, error: "That isn't a member." };

  const profile = readPlayerProfile(gameId, playerId);
  const names: Record<string, string> = {};
  const rivalIds = profile.rivals.map((r) => r.opponent_id);
  if (rivalIds.length > 0) {
    const resolved = await resolveUsers(guildId, rivalIds);
    for (const [id, user] of resolved) names[id] = user.name;
  }
  return { ok: true, profile, names };
}
