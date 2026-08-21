"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/onboarding/store";

/**
 * Every Welcome & Onboarding rich-state mutation the dashboard can perform:
 * intro questions, the blacklist, per-member point edits, wipes, and the game→LFG
 * mappings. Like the QOTD actions, each re-authenticates, re-authorises the
 * guild, rate-limits and records what changed — regardless of what the client did
 * — then queues an imperative command onto the bot's bridge. The bot drains the
 * queue every ten seconds; nothing here writes the intro database directly. The
 * flat settings (channels, roles, thresholds, tier/VIP roles) go through the
 * standard settings form and the module-config bridge instead.
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
}

async function guard(
  guildId: string,
): Promise<
  | { ok: true; actorId: string; actorName: string; ip: string; userAgent: string }
  | { ok: false; result: ActionResult }
> {
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
  if (error instanceof store.OnboardingUnavailable) {
    return {
      ok: false,
      error: "Vibey isn't running, so its onboarding data can't be reached right now.",
    };
  }
  console.error("[onboarding] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function revalidate(guildId: string): void {
  // "layout" so every sub-page under the module (questions, mappings, points,
  // blacklist) picks up the fresh snapshot, not just the landing.
  revalidatePath(`/dashboard/${guildId}/modules/welcome`, "layout");
}

function isSnowflake(value: string): boolean {
  return /^\d{17,20}$/.test(value);
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

// --- Questions ---

export async function addQuestion(
  guildId: string,
  text: string,
  style: "short" | "long",
  optional: boolean,
): Promise<ActionResult> {
  const trimmed = text.trim().slice(0, 200);
  if (!trimmed) return { ok: false, error: "Type a question first." };
  return run(
    guildId, "onboarding.question.add", "questions",
    { text: { from: null, to: trimmed } },
    (actorId) =>
      store.queueCommand(
        "add_question",
        { text: trimmed, style: style === "long" ? "long" : "short", is_optional: optional },
        actorId,
      ),
  );
}

export async function editQuestion(
  guildId: string,
  id: number,
  text: string,
  style: "short" | "long",
  optional: boolean,
): Promise<ActionResult> {
  if (!Number.isInteger(id) || id <= 0) return { ok: false, error: "That question no longer exists." };
  const trimmed = text.trim().slice(0, 200);
  if (!trimmed) return { ok: false, error: "A question can't be empty." };
  return run(
    guildId, "onboarding.question.edit", String(id),
    { text: { from: null, to: trimmed } },
    (actorId) =>
      store.queueCommand(
        "edit_question",
        { id, text: trimmed, style: style === "long" ? "long" : "short", is_optional: optional },
        actorId,
      ),
  );
}

export async function deleteQuestion(
  guildId: string,
  id: number,
  shift: boolean,
): Promise<ActionResult> {
  if (!Number.isInteger(id) || id <= 0) return { ok: false, error: "That question no longer exists." };
  return run(
    guildId, "onboarding.question.delete", String(id),
    { id: { from: id, to: null } },
    (actorId) => store.queueCommand("delete_question", { id, shift: Boolean(shift) }, actorId),
  );
}

export async function reorderQuestions(guildId: string, order: number[]): Promise<ActionResult> {
  const ids = order.filter((n) => Number.isInteger(n) && n > 0);
  if (ids.length === 0) return { ok: false, error: "Nothing to reorder." };
  return run(
    guildId, "onboarding.questions.reorder", "questions",
    { order: { from: null, to: ids.length } },
    (actorId) => store.queueCommand("reorder_questions", { order: ids }, actorId),
  );
}

// --- Blacklist ---

export async function blacklistAdd(guildId: string, userId: string): Promise<ActionResult> {
  if (!isSnowflake(userId)) return { ok: false, error: "Pick a valid member." };
  return run(
    guildId, "onboarding.blacklist.add", userId,
    { user: { from: null, to: userId } },
    (actorId) => store.queueCommand("blacklist_add", { user_id: userId }, actorId),
  );
}

export async function blacklistRemove(guildId: string, userId: string): Promise<ActionResult> {
  if (!isSnowflake(userId)) return { ok: false, error: "Unknown member." };
  return run(
    guildId, "onboarding.blacklist.remove", userId,
    { user: { from: userId, to: null } },
    (actorId) => store.queueCommand("blacklist_remove", { user_id: userId }, actorId),
  );
}

// --- Points ---

export async function adjustPoints(
  guildId: string,
  userId: string,
  op: "add" | "remove" | "set",
  amount: number,
): Promise<ActionResult> {
  if (!isSnowflake(userId)) return { ok: false, error: "Pick a valid member." };
  if (!Number.isFinite(amount) || amount < 0) return { ok: false, error: "Enter a number of 0 or more." };
  const rounded = Math.floor(amount);
  return run(
    guildId, `onboarding.points.${op}`, userId,
    { points: { from: null, to: rounded } },
    (actorId) =>
      store.queueCommand("points_adjust", { user_id: userId, action: op, amount: rounded }, actorId),
  );
}

export async function wipeMember(guildId: string, userId: string): Promise<ActionResult> {
  if (!isSnowflake(userId)) return { ok: false, error: "Pick a valid member." };
  return run(
    guildId, "onboarding.points.wipe_member", userId,
    { points: { from: null, to: 0 } },
    (actorId) => store.queueCommand("points_wipe", { user_id: userId }, actorId),
  );
}

export async function wipeAllPoints(guildId: string): Promise<ActionResult> {
  return run(
    guildId, "onboarding.points.wipe_all", "points",
    { points: { from: "all", to: null } },
    (actorId) => store.queueCommand("wipe_all", {}, actorId),
  );
}

// --- Game → LFG mappings ---

export async function mappingAdd(
  guildId: string,
  roleId: string,
  threadId: string,
): Promise<ActionResult> {
  if (!isSnowflake(roleId)) return { ok: false, error: "Pick a game role." };
  if (!isSnowflake(threadId)) return { ok: false, error: "Pick an LFG thread or channel." };
  return run(
    guildId, "onboarding.mapping.add", roleId,
    { thread: { from: null, to: threadId } },
    (actorId) =>
      store.queueCommand("mapping_add", { guild_id: guildId, role_id: roleId, thread_id: threadId }, actorId),
  );
}

export async function mappingRemove(guildId: string, roleId: string): Promise<ActionResult> {
  if (!isSnowflake(roleId)) return { ok: false, error: "Unknown mapping." };
  return run(
    guildId, "onboarding.mapping.remove", roleId,
    { mapping: { from: roleId, to: null } },
    (actorId) => store.queueCommand("mapping_remove", { guild_id: guildId, role_id: roleId }, actorId),
  );
}
