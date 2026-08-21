"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { isOwnerId } from "@/lib/auth/owner";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import {
  ControlUnavailable,
  cogAction,
  reloadAllCogs,
  restartBot,
  type CogAction,
} from "@/lib/bot/control";

/**
 * The Bot Control mutations.
 *
 * These are the sharpest actions the dashboard exposes — reloading the bot's
 * code, restarting the process — so they are held to a stricter bar than the
 * module-config actions in actions/settings.ts:
 *
 * 1. A valid session (not expired).
 * 2. The viewer is the bot *owner*, not merely an admin (isOwnerId). This is
 *    re-checked here and never trusted from the client, because a server action
 *    is a public endpoint regardless of which page can render its button.
 * 3. Rate-limited, and recorded in the audit log before it takes effect.
 *
 * The unavailable/offline case (bot down, token mismatch) comes back as a
 * friendly message rather than a thrown 500.
 */

export interface ControlResult {
  ok: boolean;
  error?: string;
  /** A Python traceback from a failed cog action, shown inline. */
  traceback?: string;
  note?: string;
}

async function ownerGuard(): Promise<
  | { ok: true; actorId: string; actorName: string; ip: string; userAgent: string }
  | { ok: false; result: ControlResult }
> {
  const session = await getActiveSession();
  if (!session) {
    return { ok: false, result: { ok: false, error: "Your session has expired. Sign in again." } };
  }
  if (!isOwnerId(session.user.id)) {
    // Same message whether the section is misconfigured or the caller simply
    // isn't the owner — there's nothing here for a non-owner to learn.
    return { ok: false, result: { ok: false, error: "This action is limited to the bot owner." } };
  }

  const headerList = await headers();
  const ip = clientIp(headerList);

  const limit = checkRateLimit("mutation", session.user.id);
  if (!limit.allowed) {
    const seconds = Math.ceil((limit.resetAt - Date.now()) / 1000);
    return {
      ok: false,
      result: { ok: false, error: `Too many actions at once. Try again in ${seconds}s.` },
    };
  }

  return {
    ok: true,
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    ip,
    userAgent: headerList.get("user-agent") ?? "",
  };
}

function unavailableMessage(): string {
  return "Vibey isn't reachable right now, so the action couldn't be sent.";
}

export async function runCogAction(
  guildId: string,
  name: string,
  action: CogAction,
): Promise<ControlResult> {
  const guarded = await ownerGuard();
  if (!guarded.ok) return guarded.result;
  if (guildId !== env.VIBEY_GUILD_ID) return { ok: false, error: "Unknown server." };

  let result;
  try {
    result = await cogAction(name, action);
  } catch (error) {
    if (error instanceof ControlUnavailable) {
      return { ok: false, error: error.message || unavailableMessage() };
    }
    throw error;
  }

  // Record the attempt regardless of the bot's verdict — an audit trail that
  // only logged successes would hide exactly the failed reload someone came
  // looking for.
  recordAudit({
    actorId: guarded.actorId,
    actorName: guarded.actorName,
    guildId,
    action: `bot.cog.${action}`,
    target: name,
    changes: result.ok ? null : { error: { from: null, to: "failed" } },
    ip: guarded.ip,
    userAgent: guarded.userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/control/cogs`);
  revalidatePath(`/dashboard/${guildId}/control`);

  if (!result.ok) {
    return { ok: false, error: `The ${action} failed.`, traceback: result.error };
  }
  return { ok: true, note: result.note };
}

export async function restartBotAction(guildId: string): Promise<ControlResult> {
  const guarded = await ownerGuard();
  if (!guarded.ok) return guarded.result;
  if (guildId !== env.VIBEY_GUILD_ID) return { ok: false, error: "Unknown server." };

  try {
    await restartBot();
  } catch (error) {
    if (error instanceof ControlUnavailable) {
      return { ok: false, error: error.message || unavailableMessage() };
    }
    throw error;
  }

  recordAudit({
    actorId: guarded.actorId,
    actorName: guarded.actorName,
    guildId,
    action: "bot.restart",
    target: null,
    changes: null,
    ip: guarded.ip,
    userAgent: guarded.userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/control`);
  return { ok: true, note: "Restarting — Vibey will be back in a few seconds." };
}

export async function reloadAllCogsAction(guildId: string): Promise<ControlResult> {
  const guarded = await ownerGuard();
  if (!guarded.ok) return guarded.result;
  if (guildId !== env.VIBEY_GUILD_ID) return { ok: false, error: "Unknown server." };

  let result;
  try {
    result = await reloadAllCogs();
  } catch (error) {
    if (error instanceof ControlUnavailable) {
      return { ok: false, error: error.message || unavailableMessage() };
    }
    throw error;
  }

  const failedNames = Object.keys(result.failed);
  recordAudit({
    actorId: guarded.actorId,
    actorName: guarded.actorName,
    guildId,
    action: "bot.reload_all",
    target: null,
    changes: failedNames.length
      ? { failed: { from: null, to: failedNames.join(", ") } }
      : null,
    ip: guarded.ip,
    userAgent: guarded.userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/control/cogs`);
  revalidatePath(`/dashboard/${guildId}/control`);

  if (failedNames.length) {
    return {
      ok: false,
      error: `Reloaded ${result.reloaded.length}, but ${failedNames.length} failed: ${failedNames.join(", ")}.`,
    };
  }
  return { ok: true, note: `Reloaded all ${result.reloaded.length} cogs.` };
}
