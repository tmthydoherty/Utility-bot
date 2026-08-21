"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/utility/store";

/**
 * Every Utility mutation the dashboard can perform — reminders, one-off
 * messages, stickies, reaction rules and media channels.
 *
 * A server action is a public HTTP endpoint with a nicer calling convention, so
 * each one re-authenticates, re-authorises the guild, rate-limits and records
 * what changed regardless of what the client did. These write the bot's live
 * database, but the worst a bad write can do is create a reminder — and a new
 * reminder is created switched off, so posting it is always a separate action.
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
  id?: string;
}

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
  if (error instanceof store.UtilityUnavailable) {
    return { ok: false, error: "Vibey isn't running, so its utility settings can't be reached right now." };
  }
  console.error("[utility] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function refresh(guildId: string, id?: string): void {
  revalidatePath(`/dashboard/${guildId}/utility`);
  if (id) revalidatePath(`/dashboard/${guildId}/utility/${id}`);
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

// ------------------------------------------------------------- reminders

export async function saveReminder(
  guildId: string,
  id: string | null,
  input: store.ReminderInput,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  if (!input.name.trim()) return { ok: false, error: "Give it a name first." };
  if (input.channelIds.length === 0) return { ok: false, error: "Pick at least one channel to post in." };

  try {
    if (id) {
      const ok = store.updateReminder(guildId, id, input, guarded.actorId);
      if (!ok) return { ok: false, error: "That reminder no longer exists." };
      log(guarded, guildId, "utility.reminder.edit", id, input.name);
      refresh(guildId, id);
      return { ok: true, id };
    }
    const newId = store.createReminder(guildId, input, guarded.actorId);
    log(guarded, guildId, "utility.reminder.create", newId, input.name);
    refresh(guildId);
    return { ok: true, id: newId };
  } catch (error) {
    return unavailable(error);
  }
}

export async function setReminderEnabled(
  guildId: string,
  id: string,
  enabled: boolean,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.setReminderEnabled(guildId, id, enabled, guarded.actorId);
    if (!ok) return { ok: false, error: "That reminder no longer exists." };
    log(guarded, guildId, "utility.reminder.state", id, enabled ? "on" : "off");
    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function deleteReminder(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.deleteReminder(guildId, id, guarded.actorId);
    if (!ok) return { ok: false, error: "That reminder no longer exists." };
    log(guarded, guildId, "utility.reminder.delete", id, "");
    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

// --------------------------------------------------------------- stickies

export async function saveSticky(
  guildId: string,
  id: string | null,
  input: store.StickyInput,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  if (!input.channelId) return { ok: false, error: "Pick a channel for the sticky." };

  try {
    if (id) {
      const ok = store.updateSticky(guildId, id, input, guarded.actorId);
      if (!ok) return { ok: false, error: "That channel already has a different sticky." };
      log(guarded, guildId, "utility.sticky.edit", id, input.name);
      refresh(guildId, id);
      return { ok: true, id };
    }
    const newId = store.createSticky(guildId, input, guarded.actorId);
    if (!newId) return { ok: false, error: "That channel already has a sticky. Edit that one instead." };
    log(guarded, guildId, "utility.sticky.create", newId, input.name);
    refresh(guildId);
    return { ok: true, id: newId };
  } catch (error) {
    return unavailable(error);
  }
}

export async function setStickyEnabled(guildId: string, id: string, enabled: boolean): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.setStickyEnabled(guildId, id, enabled, guarded.actorId);
    if (!ok) return { ok: false, error: "That sticky no longer exists." };
    log(guarded, guildId, "utility.sticky.state", id, enabled ? "on" : "off");
    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function deleteSticky(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.deleteSticky(guildId, id, guarded.actorId);
    if (!ok) return { ok: false, error: "That sticky no longer exists." };
    log(guarded, guildId, "utility.sticky.delete", id, "");
    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

// --------------------------------------------------------- reaction rules

export async function saveReactionRule(
  guildId: string,
  id: string | null,
  input: store.ReactionInput,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  if (!input.channelId) return { ok: false, error: "Pick a channel." };

  try {
    if (id) {
      const ok = store.updateReactionRule(guildId, id, input, guarded.actorId);
      if (!ok) return { ok: false, error: "That rule no longer exists." };
      log(guarded, guildId, "utility.reaction.edit", id, "");
      refresh(guildId, id);
      return { ok: true, id };
    }
    const newId = store.createReactionRule(guildId, input, guarded.actorId);
    log(guarded, guildId, "utility.reaction.create", newId, "");
    refresh(guildId);
    return { ok: true, id: newId };
  } catch (error) {
    return unavailable(error);
  }
}

export async function setReactionEnabled(guildId: string, id: string, enabled: boolean): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.setReactionEnabled(guildId, id, enabled, guarded.actorId);
    if (!ok) return { ok: false, error: "That rule no longer exists." };
    log(guarded, guildId, "utility.reaction.state", id, enabled ? "on" : "off");
    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function deleteReactionRule(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.deleteReactionRule(guildId, id, guarded.actorId);
    if (!ok) return { ok: false, error: "That rule no longer exists." };
    log(guarded, guildId, "utility.reaction.delete", id, "");
    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

// -------------------------------------------------------- media channels

export async function saveMediaChannel(
  guildId: string,
  id: string | null,
  input: store.MediaInput,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  if (!input.channelId) return { ok: false, error: "Pick a channel." };

  try {
    if (id) {
      const ok = store.updateMediaChannel(guildId, id, input, guarded.actorId);
      if (!ok) return { ok: false, error: "That channel already has a different rule." };
      log(guarded, guildId, "utility.media.edit", id, "");
      refresh(guildId, id);
      return { ok: true, id };
    }
    const newId = store.createMediaChannel(guildId, input, guarded.actorId);
    if (!newId) return { ok: false, error: "That channel is already media-only. Edit that rule instead." };
    log(guarded, guildId, "utility.media.create", newId, "");
    refresh(guildId);
    return { ok: true, id: newId };
  } catch (error) {
    return unavailable(error);
  }
}

export async function setMediaEnabled(guildId: string, id: string, enabled: boolean): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.setMediaEnabled(guildId, id, enabled, guarded.actorId);
    if (!ok) return { ok: false, error: "That rule no longer exists." };
    log(guarded, guildId, "utility.media.state", id, enabled ? "on" : "off");
    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function deleteMediaChannel(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };
  try {
    const ok = store.deleteMediaChannel(guildId, id, guarded.actorId);
    if (!ok) return { ok: false, error: "That rule no longer exists." };
    log(guarded, guildId, "utility.media.delete", id, "");
    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}
