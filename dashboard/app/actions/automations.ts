"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import * as store from "@/lib/automations/store";
import { blanks, isReady } from "@/lib/automations/readiness";
import { TEMPLATES_BY_KEY } from "@/lib/automations/templates";
import {
  emptyConditions,
  isGroup,
  parseCondition,
  parseSteps,
  countSteps,
  MAX_STEPS,
  type ConditionGroup,
  type CooldownScope,
  type Step,
} from "@/lib/automations/types";
import { TRIGGERS } from "@/lib/automations/registry";
import type { SettingsValues } from "@/lib/schema/types";

/**
 * Every automation mutation the dashboard can perform.
 *
 * A server action is a public HTTP endpoint with a nicer calling convention —
 * the arguments arrive from the network and nothing about them is trustworthy
 * just because a form on our own page sent them. So each one re-authenticates,
 * re-authorises the guild, rate-limits, re-validates, and records what changed,
 * in that order, regardless of what the client did.
 *
 * These write to the bot's live database, so there is one extra rule on top of
 * that: **nothing here can arm an automation that isn't finished.** `setState`
 * re-derives readiness on the server and refuses, because a client that has
 * gone stale must not be able to switch on something with a blank in it.
 */

export interface ActionResult {
  ok: boolean;
  error?: string;
  /** Set on create, so the page can navigate to the new automation. */
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

/** The session proves admin in *our* guild specifically. */
function wrongGuild(guildId: string): boolean {
  return guildId !== env.VIBEY_GUILD_ID;
}

function unavailable(error: unknown): ActionResult {
  if (error instanceof store.AutomationsUnavailable) {
    return {
      ok: false,
      error: "Vibey isn't running, so its automations can't be reached right now.",
    };
  }
  console.error("[automations] a write failed:", error);
  return { ok: false, error: "Something went wrong saving that. Nothing was changed." };
}

function refresh(guildId: string, id?: string): void {
  revalidatePath(`/dashboard/${guildId}/automations`);
  if (id) revalidatePath(`/dashboard/${guildId}/automations/${id}`);
}

// -------------------------------------------------------------------- create

export async function createFromTemplate(
  guildId: string,
  templateKey: string,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  const template = TEMPLATES_BY_KEY[templateKey];
  if (!template) return { ok: false, error: "That ready-made automation no longer exists." };

  try {
    const conditionRoot = parseCondition(template.conditions ?? {});
    const id = store.createAutomation({
      guildId,
      name: template.name,
      triggerType: template.triggerType,
      triggerConfig: template.triggerConfig ?? {},
      conditions: isGroup(conditionRoot) ? conditionRoot : emptyConditions(),
      steps: parseSteps(template.steps),
      templateKey: template.key,
      actorId: guarded.actorId,
    });

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: "automation.create",
      target: id,
      changes: { name: { from: null, to: template.name } },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId);
    return { ok: true, id };
  } catch (error) {
    return unavailable(error);
  }
}

export async function createBlank(guildId: string, name: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  const trimmed = name.trim();
  if (!trimmed) return { ok: false, error: "Give it a name first." };

  try {
    const id = store.createAutomation({
      guildId,
      name: trimmed,
      triggerType: "message_sent",
      actorId: guarded.actorId,
    });

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: "automation.create",
      target: id,
      changes: { name: { from: null, to: trimmed } },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId);
    return { ok: true, id };
  } catch (error) {
    return unavailable(error);
  }
}

export async function duplicate(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  try {
    const copyId = store.duplicateAutomation(guildId, id, guarded.actorId);
    if (!copyId) return { ok: false, error: "That automation no longer exists." };

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: "automation.duplicate",
      target: copyId,
      changes: { copiedFrom: { from: null, to: id } },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId);
    return { ok: true, id: copyId };
  } catch (error) {
    return unavailable(error);
  }
}

// -------------------------------------------------------------------- update

export interface SavePayload {
  name?: string;
  triggerType?: string;
  triggerConfig?: SettingsValues;
  conditions?: ConditionGroup;
  steps?: Step[];
  priority?: number;
  stopAfter?: boolean;
  cooldownSeconds?: number;
  cooldownScope?: CooldownScope;
  allowBots?: boolean;
}

/**
 * Give every button that needs one a stable id, in place.
 *
 * A message button's click is matched back to its config by an id baked into
 * the button's custom_id when the message is posted. It has to be assigned
 * before the automation is stored — a button posted without one would have no
 * way to find its reply — and it has to be stable across edits, so an existing
 * id is left alone and only a newly-enabled button gets a fresh one.
 */
function stampButtonIds(steps: Step[]): void {
  for (const step of steps) {
    if (step.config?.add_button && !step.config.button_id) {
      step.config.button_id = crypto.randomUUID().replace(/-/g, "").slice(0, 20);
    }
    if (step.then) stampButtonIds(step.then);
    if (step.otherwise) stampButtonIds(step.otherwise);
  }
}

export async function save(
  guildId: string,
  id: string,
  payload: SavePayload,
  summary: string,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  if (payload.triggerType !== undefined && !TRIGGERS[payload.triggerType]) {
    return { ok: false, error: "That isn't something Vibey can watch for." };
  }
  if (payload.steps) stampButtonIds(payload.steps);
  // The cap exists so a hand-edited or runaway graph cannot make the engine
  // walk forever. Checked here as well as in the engine, because this is the
  // side that can refuse with a sentence rather than a truncated run.
  if (payload.steps && countSteps(payload.steps) > MAX_STEPS) {
    return { ok: false, error: `That's more than ${MAX_STEPS} steps — split it into two automations.` };
  }

  try {
    const before = store.getAutomation(guildId, id);
    if (!before) return { ok: false, error: "That automation no longer exists." };

    const changed = store.updateAutomation(guildId, id, payload, guarded.actorId, summary);
    if (!changed) return { ok: false, error: "That automation no longer exists." };

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: "automation.edit",
      target: id,
      changes: { summary: { from: before.name, to: summary } },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export type DesiredState = "off" | "testing" | "live";

/**
 * Switch an automation off, into test mode, or fully on.
 *
 * The readiness check here is the important part. The button is already
 * disabled on the page when something is blank, but the page's copy of the
 * automation can be seconds out of date — someone else may have emptied a
 * field in Discord in the meantime — and an automation with a blank in it does
 * not misbehave subtly, it fails on every single event.
 */
export async function setState(
  guildId: string,
  id: string,
  state: DesiredState,
): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  try {
    const automation = store.getAutomation(guildId, id);
    if (!automation) return { ok: false, error: "That automation no longer exists." };

    if (state !== "off" && !isReady(automation)) {
      const remaining = blanks(automation);
      const first = remaining[0];
      return {
        ok: false,
        error: first
          ? `“${first.field.label}” still needs filling in before this can run.`
          : "This isn't finished yet.",
      };
    }

    store.updateAutomation(
      guildId,
      id,
      { enabled: state !== "off", dryRun: state === "testing" },
      guarded.actorId,
      state === "off" ? "switched off" : state === "testing" ? "put in test mode" : "switched on",
    );

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: "automation.state",
      target: id,
      changes: {
        state: {
          from: automation.enabled ? (automation.dryRun ? "testing" : "live") : "off",
          to: state,
        },
      },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId, id);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function remove(guildId: string, id: string): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  try {
    const automation = store.getAutomation(guildId, id);
    if (!automation) return { ok: false, error: "That automation no longer exists." };

    store.deleteAutomation(guildId, id, guarded.actorId, automation.name);

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: "automation.delete",
      target: id,
      changes: { name: { from: automation.name, to: null } },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}

export async function setPausedAll(guildId: string, paused: boolean): Promise<ActionResult> {
  const guarded = await guard();
  if (!guarded.ok) return guarded.result;
  if (wrongGuild(guildId)) return { ok: false, error: "Unknown server." };

  try {
    store.setPaused(paused, guarded.actorId);

    recordAudit({
      actorId: guarded.actorId,
      actorName: guarded.actorName,
      guildId,
      action: paused ? "automations.pause" : "automations.resume",
      target: "all",
      changes: { paused: { from: !paused, to: paused } },
      ip: guarded.ip,
      userAgent: guarded.userAgent,
    });

    refresh(guildId);
    return { ok: true };
  } catch (error) {
    return unavailable(error);
  }
}
