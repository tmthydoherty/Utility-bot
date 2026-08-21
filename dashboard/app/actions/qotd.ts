"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/qotd/store";

/**
 * Every Question of the Day pool mutation the dashboard can perform.
 *
 * Like the ticketing actions, each re-authenticates, re-authorises the guild,
 * rate-limits and records what changed — regardless of what the client did —
 * then queues an imperative command onto the bot's bridge. The bot drains the
 * queue every ten seconds; nothing here writes the question database directly.
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
  if (error instanceof store.QotdUnavailable) {
    return {
      ok: false,
      error: "Vibey isn't running, so its question pool can't be reached right now.",
    };
  }
  console.error("[qotd] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function revalidate(guildId: string): void {
  revalidatePath(`/dashboard/${guildId}/modules/qotd`);
}

export async function addQuestions(guildId: string, rawText: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  // One question per line, trimmed, blanks dropped, capped so a paste can't
  // queue a runaway command. Discord's own text-input limit is 256 chars.
  const texts = rawText
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.slice(0, 256))
    .slice(0, 200);

  if (texts.length === 0) {
    return { ok: false, error: "Type at least one question, one per line." };
  }

  try {
    store.queueCommand("add_questions", { texts, actor_id: actorId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "qotd.questions.add", target: "pool",
      changes: { added: { from: null, to: texts.length } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}

export async function editQuestion(
  guildId: string,
  id: number,
  text: string,
): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  const trimmed = text.trim().slice(0, 256);
  if (!Number.isInteger(id) || id <= 0) return { ok: false, error: "That question no longer exists." };
  if (!trimmed) return { ok: false, error: "A question can't be empty." };

  try {
    store.queueCommand("edit_question", { id, text: trimmed }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "qotd.question.edit", target: String(id),
      changes: { text: { from: null, to: trimmed } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}

export async function deleteQuestion(guildId: string, id: number): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  if (!Number.isInteger(id) || id <= 0) return { ok: false, error: "That question no longer exists." };

  try {
    store.queueCommand("delete_question", { id }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "qotd.question.delete", target: String(id),
      changes: { id: { from: id, to: null } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}

export async function resetPool(guildId: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.queueCommand("reset_pool", {}, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "qotd.pool.reset", target: "pool", changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}

export async function clearSeen(guildId: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.queueCommand("clear_seen", {}, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "qotd.pool.clear_seen", target: "pool", changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}

export async function rerollTomorrow(guildId: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.queueCommand("reroll_tomorrow", { guild_id: guildId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "qotd.tomorrow.reroll", target: "pool", changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}
