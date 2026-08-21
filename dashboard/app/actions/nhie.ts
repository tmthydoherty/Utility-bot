"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/nhie/store";

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
  if (error instanceof store.NhieUnavailable) {
    return {
      ok: false,
      error: "Vibey isn't running, so its question pool can't be reached right now.",
    };
  }
  console.error("[nhie] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function revalidate(guildId: string): void {
  revalidatePath(`/dashboard/${guildId}/modules/nhie`);
}

export async function addQuestions(guildId: string, rawText: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

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
    store.queueCommand("add_questions", { guild_id: guildId, texts, actor_id: actorId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "nhie.questions.add", target: "pool",
      changes: { added: { from: null, to: texts.length } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}

export async function deleteQuestion(guildId: string, index: number): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  if (!Number.isInteger(index) || index < 0) return { ok: false, error: "That question no longer exists." };

  try {
    store.queueCommand("delete_question", { guild_id: guildId, index }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "nhie.question.delete", target: String(index),
      changes: { index: { from: index, to: null } }, ip, userAgent,
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
    store.queueCommand("reset_pool", { guild_id: guildId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "nhie.pool.reset", target: "pool", changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidate(guildId);
  return { ok: true };
}
