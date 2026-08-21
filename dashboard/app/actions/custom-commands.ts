"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/custom-commands/store";

/**
 * Every Custom Commands mutation the dashboard can perform — the admin `!`
 * commands, and moderation of the economy's purchased GIF commands.
 *
 * A server action is a public HTTP endpoint with a nicer calling convention, so
 * each one re-authenticates, re-authorises the guild, rate-limits and records
 * what changed regardless of what the client did. A new command is created
 * switched off, so it never fires until someone turns it on.
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
  id?: string;
}

// Names that are, or could shadow, a real bot command. Serving already skips
// these, but rejecting them here means the admin finds out at save time rather
// than wondering why `!help` never answers.
const RESERVED = new Set(["help", "debug", "sync", "ping"]);

async function guard(): Promise<
  | { ok: true; actorId: string; actorName: string; ip: string; userAgent: string }
  | { ok: false; result: ActionResult }
> {
  const session = await getActiveSession();
  if (!session) {
    return { ok: false, result: { ok: false, error: "Your session has expired. Sign in again." } };
  }
  const headerList = await headers();
  const limit = checkRateLimit("mutation", session.user.id);
  if (!limit.allowed) {
    const seconds = Math.ceil((limit.resetAt - Date.now()) / 1000);
    return { ok: false, result: { ok: false, error: `Too many changes at once. Try again in ${seconds}s.` } };
  }
  return {
    ok: true,
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    ip: clientIp(headerList),
    userAgent: headerList.get("user-agent") ?? "",
  };
}

function wrongGuild(guildId: string): boolean {
  return guildId !== env.VIBEY_GUILD_ID;
}

function unavailable(error: unknown): ActionResult {
  if (error instanceof store.CustomCommandsUnavailable) {
    return { ok: false, error: "Vibey isn't running, so its custom commands can't be reached right now." };
  }
  console.error("[custom-commands] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function refresh(guildId: string, id?: string): void {
  revalidatePath(`/dashboard/${guildId}/custom-commands`);
  if (id) revalidatePath(`/dashboard/${guildId}/custom-commands/${id}`);
}

function log(
  guarded: { actorId: string; actorName: string; ip: string; userAgent: string },
  guildId: string,
  action: string,
  target: string,
  detail: string,
): void {
  recordAudit({
    actorId: guarded.actorId,
    actorName: guarded.actorName,
    guildId,
    action,
    target,
    changes: { detail: { from: null, to: detail } },
    ip: guarded.ip,
    userAgent: guarded.userAgent,
  });
}

// ------------------------------------------------------------- commands

export async function saveCommand(
  guildId: string,
  id: string | null,
  input: store.CommandInput,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  const name = store.normalizeName(input.name);
  if (!name) return { ok: false, error: "Give it a name — letters, numbers or underscores." };
  if (name.length < 2) return { ok: false, error: "Names need to be at least two characters." };
  if (RESERVED.has(name)) return { ok: false, error: `\`!${name}\` is a built-in command. Pick another name.` };

  const hasText = input.responses.some((r) => r.trim());
  const hasEmbed = !input.plainText;
  if (!hasText && !hasEmbed) return { ok: false, error: "Add at least one response." };

  try {
    if (store.nameTaken(guildId, name, id ?? undefined)) {
      return { ok: false, error: `\`!${name}\` is already one of your commands.` };
    }
    if (store.gifNameExists(name)) {
      return { ok: false, error: `\`!${name}\` is a purchased GIF command. Pick another name.` };
    }

    if (id) {
      const ok = store.updateCommand(guildId, id, input, guarded.actorId);
      if (!ok) return { ok: false, error: "That command no longer exists." };
      log(guarded, guildId, "custom_commands.command.edit", id, name);
      refresh(guildId, id);
      return { ok: true, id };
    }
    const newId = store.createCommand(guildId, input, guarded.actorId);
    log(guarded, guildId, "custom_commands.command.create", newId, name);
    refresh(guildId);
    return { ok: true, id: newId };
  } catch (error) {
    return unavailable(error);
  }
}

export async function setCommandEnabled(
  guildId: string,
  id: string,
  enabled: boolean,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.setCommandEnabled(guildId, id, enabled, guarded.actorId);
    if (!ok) return { ok: false, error: "That command no longer exists." };
    log(guarded, guildId, "custom_commands.command.state", id, enabled ? "on" : "off");
    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function deleteCommand(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.deleteCommand(guildId, id, guarded.actorId);
    if (!ok) return { ok: false, error: "That command no longer exists." };
    log(guarded, guildId, "custom_commands.command.delete", id, "");
    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

// ------------------------------------------------------ purchased GIFs

export async function moderateGif(
  guildId: string,
  name: string,
  action: store.GifModerationAction,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.moderateGif(name, action, guarded.actorId);
    if (!ok) return { ok: false, error: "That GIF command no longer exists." };
    log(guarded, guildId, `custom_commands.gif.${action}`, store.normalizeName(name), name);
    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}
