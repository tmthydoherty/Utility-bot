"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/ticketing/store";
import { getTopicTemplate } from "@/lib/ticketing/templates";
import {
  panelDefaults,
  slugify,
  topicDefaults,
  type Panel,
  type Topic,
} from "@/lib/ticketing/types";

/**
 * Every ticketing mutation the dashboard can perform.
 *
 * A server action is a public HTTP endpoint with a nicer calling convention, so
 * each one re-authenticates, re-authorises the guild, rate-limits and records
 * what changed — regardless of what the client did — the same chain
 * `app/actions/automations.ts` follows.
 *
 * These write to the bot's live bridge, so publishing (the one thing that posts
 * to Discord) is always a separate, explicit action, never a side effect of a
 * config save.
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
  /** Set on create, so the page can navigate to the new topic/panel. */
  name?: string;
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
  if (error instanceof store.TicketingUnavailable) {
    return {
      ok: false,
      error: "Vibey isn't running, so its ticketing settings can't be reached right now.",
    };
  }
  console.error("[ticketing] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

// ------------------------------------------------------------------- topics

export async function createTopic(
  guildId: string,
  rawName: string,
  templateKey?: string,
): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  const name = slugify(rawName);
  if (!name) return { ok: false, error: "Give the topic a name (letters, numbers and dashes)." };

  try {
    if (store.getTopic(name)) {
      return { ok: false, error: `A topic called "${name}" already exists.` };
    }
    const template = templateKey ? getTopicTemplate(templateKey) : undefined;
    const topic: Topic = template ? { ...template.build(), name } : topicDefaults(name);
    store.saveTopic(topic, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.topic.create", target: name,
      changes: { name: { from: null, to: name } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  return { ok: true, name };
}

export async function updateTopic(guildId: string, topic: Topic): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  if (!topic?.name) return { ok: false, error: "That topic is missing its name." };

  try {
    store.saveTopic(topic, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.topic.update", target: topic.name,
      changes: { label: { from: null, to: topic.label } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  revalidatePath(`/dashboard/${guildId}/ticketing/topics/${topic.name}`);
  return { ok: true, name: topic.name };
}

export async function deleteTopic(guildId: string, name: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.deleteTopic(name, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.topic.delete", target: name,
      changes: { name: { from: name, to: null } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  return { ok: true };
}

// ------------------------------------------------------------------- panels

export async function createPanel(guildId: string, rawName: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  const name = slugify(rawName);
  if (!name) return { ok: false, error: "Give the panel a name (letters, numbers and dashes)." };

  try {
    if (store.getPanel(name)) {
      return { ok: false, error: `A panel called "${name}" already exists.` };
    }
    store.savePanel(panelDefaults(name), actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.panel.create", target: name,
      changes: { name: { from: null, to: name } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  return { ok: true, name };
}

export async function updatePanel(guildId: string, panel: Panel): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  if (!panel?.name) return { ok: false, error: "That panel is missing its name." };

  try {
    store.savePanel(panel, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.panel.update", target: panel.name,
      changes: { title: { from: null, to: panel.title ?? panel.name } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  revalidatePath(`/dashboard/${guildId}/ticketing/panels/${panel.name}`);
  return { ok: true, name: panel.name };
}

export async function deletePanel(guildId: string, name: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    // Take the live message down first, then remove the config.
    store.queueCommand("unpublish_panel", { name, guild_id: guildId }, actorId);
    store.deletePanel(name, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.panel.delete", target: name,
      changes: { name: { from: name, to: null } }, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  return { ok: true };
}

export async function publishPanel(guildId: string, name: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    const panel = store.getPanel(name);
    if (!panel) return { ok: false, error: "That panel no longer exists." };
    if (!panel.channelId) return { ok: false, error: "Choose a channel for the panel first." };
    if (panel.topicNames.length === 0) {
      return { ok: false, error: "Add at least one topic before publishing." };
    }
    store.queueCommand("publish_panel", { name, guild_id: guildId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.panel.publish", target: name,
      changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  revalidatePath(`/dashboard/${guildId}/ticketing/panels/${name}`);
  return { ok: true };
}

export async function unpublishPanel(guildId: string, name: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.queueCommand("unpublish_panel", { name, guild_id: guildId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.panel.unpublish", target: name,
      changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing`);
  revalidatePath(`/dashboard/${guildId}/ticketing/panels/${name}`);
  return { ok: true };
}

// ---------------------------------------------------------------- responses

export async function sendSurvey(
  guildId: string,
  name: string,
  roleIds: string[],
  userIds: string[],
): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  if (roleIds.length === 0 && userIds.length === 0) {
    return { ok: false, error: "Pick at least one role or member to send to." };
  }

  try {
    store.queueCommand(
      "send_survey",
      { name, role_ids: roleIds, user_ids: userIds, guild_id: guildId },
      actorId,
    );
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.survey.send", target: name,
      changes: { roles: { from: null, to: roleIds.length }, members: { from: null, to: userIds.length } },
      ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing/responses`);
  return { ok: true };
}

export async function deleteResponses(guildId: string, survey: string): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.queueCommand("delete_responses", { name: survey, guild_id: guildId }, actorId);
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.responses.delete", target: survey,
      changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing/responses`);
  return { ok: true };
}

export async function deleteResponse(
  guildId: string,
  survey: string,
  userId: string,
  timestamp: string,
): Promise<ActionResult> {
  const guarded = await guard(guildId);
  if (!guarded.ok) return guarded.result;
  const { actorId, actorName, ip, userAgent } = guarded;

  try {
    store.queueCommand(
      "delete_response",
      { name: survey, user_id: userId, timestamp, guild_id: guildId },
      actorId,
    );
    recordAudit({
      actorId, actorName, guildId,
      action: "ticketing.response.delete", target: survey,
      changes: {}, ip, userAgent,
    });
  } catch (error) {
    return unavailable(error);
  }

  revalidatePath(`/dashboard/${guildId}/ticketing/responses`);
  return { ok: true };
}
