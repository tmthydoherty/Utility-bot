"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { diffValues, recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import {
  EconomyConfigUnavailable,
  ModuleConfigUnavailable,
  readSettings,
  setModuleEnabled,
  writeSettings,
} from "@/lib/bot/adapter";
import { getModule } from "@/lib/schema/modules";
import { validateModule } from "@/lib/schema/validate";
import type { SettingsValues } from "@/lib/schema/types";

/**
 * Every mutation the dashboard can perform.
 *
 * A server action is a public HTTP endpoint with a nicer calling convention —
 * the arguments arrive from the network and nothing about them is trustworthy
 * just because a form on our own page sent them. So each one re-authenticates,
 * re-authorises the guild, rate-limits, re-validates against the schema, and
 * records what changed, in that order, regardless of what the client did.
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
  /** Field-level errors, keyed by field key. */
  fieldErrors?: Record<string, string>;
}

async function requireSession() {
  const session = await getActiveSession();
  if (!session) throw new Error("unauthorized");
  return session;
}

async function guard(): Promise<
  | { ok: true; session: Awaited<ReturnType<typeof requireSession>>; ip: string; userAgent: string }
  | { ok: false; result: ActionResult }
> {
  let session;
  try {
    session = await requireSession();
  } catch {
    return { ok: false, result: { ok: false, error: "Your session has expired. Sign in again." } };
  }

  const headerList = await headers();
  const ip = clientIp(headerList);

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
    session,
    ip,
    userAgent: headerList.get("user-agent") ?? "",
  };
}

export async function saveModuleSettings(
  guildId: string,
  moduleId: string,
  values: SettingsValues,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  const { session, ip, userAgent } = guarded;

  // The session proves admin in *our* guild specifically, so a request naming
  // any other guild is either a bug or someone probing.
  if (guildId !== env.VIBEY_GUILD_ID) {
    return { ok: false, error: "Unknown server." };
  }

  const moduleSchema = getModule(moduleId);
  if (!moduleSchema || !moduleSchema.configurable) {
    return { ok: false, error: "That module can't be configured here yet." };
  }

  const validation = validateModule(moduleSchema, values);
  if (!validation.ok) {
    return {
      ok: false,
      error: "Some fields need attention.",
      fieldErrors: validation.errors,
    };
  }

  const before = readSettings(guildId, moduleId);
  const changes = diffValues(before, validation.values);

  if (Object.keys(changes).length === 0) {
    // Nothing moved. Report success rather than an error — the user's intent
    // ("make it look like this") is satisfied — but don't write an audit row
    // for a no-op.
    return { ok: true };
  }

  try {
    writeSettings(guildId, moduleId, validation.values, session.user.id);
  } catch (error) {
    if (
      error instanceof EconomyConfigUnavailable ||
      error instanceof ModuleConfigUnavailable
    ) {
      return {
        ok: false,
        error: "Vibey isn't running, so its settings can't be updated right now.",
      };
    }
    throw error;
  }

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: "settings.update",
    target: moduleId,
    changes,
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules/${moduleId}`);
  // A relocated module (Economy) is edited from its own section, whose pages
  // read the same values through the same bridge — revalidate that whole
  // subtree too, or a save on one tab would leave the others showing stale
  // numbers until the next full load.
  if (moduleSchema.relocated && moduleSchema.link) {
    revalidatePath(`/dashboard/${guildId}${moduleSchema.link}`, "layout");
  }
  return { ok: true };
}

export async function toggleModule(
  guildId: string,
  moduleId: string,
  enabled: boolean,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  const { session, ip, userAgent } = guarded;

  if (guildId !== env.VIBEY_GUILD_ID) return { ok: false, error: "Unknown server." };

  const moduleSchema = getModule(moduleId);
  if (!moduleSchema) return { ok: false, error: "Unknown module." };

  setModuleEnabled(guildId, moduleId, enabled);

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: "module.toggle",
    target: moduleId,
    changes: { enabled: { from: !enabled, to: enabled } },
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules`);
  return { ok: true };
}
