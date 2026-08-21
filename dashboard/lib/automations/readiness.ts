/**
 * What is still blank in an automation, and where to go and fill it in.
 *
 * The web mirror of `cogs/automations/readiness.py`, and it exists for the
 * same reason: templates land deliberately unfinished — a welcome message with
 * no channel picked, a "give them a role" step with no role. That is the right
 * trade, because a concrete automation you adjust beats an empty one you have
 * to design. It only works if something knows which parts are still blank.
 *
 * A blank is only reported when it is genuinely a problem, because a checklist
 * that cries wolf is one people learn to dismiss. Two things earn a mention:
 *
 *   it cannot work — no role picked, so the step fails every single time.
 *   it matches all — an empty "it's in certain channels" check matches *every*
 *                    channel, so a delete-messages automation carrying one
 *                    would sweep the whole server.
 *
 * Everything else stays quiet. "Send a message" with no channel falls back to
 * wherever the trigger happened, which is right on a message trigger and
 * impossible on "someone joins" — so the trigger's own `provides` decides
 * whether that counts, and nobody is nagged about a field that is fine empty.
 */

import { isUnset, isVisible, type Field, type SettingsValues } from "@/lib/schema/types";

import { ACTIONS, CONDITIONS, TRIGGERS } from "./registry";
import {
  isBranch,
  isGroup,
  walkConditions,
  walkSteps,
  type Automation,
  type StepPath,
} from "./types";

export type BlankWhere = "trigger" | "condition" | "action";

export interface Blank {
  where: BlankWhere;
  path: StepPath;
  field: Field;
  /** The entry it belongs to, so the checklist can say which step. */
  owner: string;
  /** Why leaving it blank is a problem. Empty when it simply has to be set. */
  note: string;
  /** Stable identity, so the UI can key a list without re-deriving. */
  ref: string;
}

/**
 * Why an empty check matters, per check.
 *
 * Every one of these falls back to "matches anything" when its main setting is
 * blank, so leaving one empty does not merely fail to narrow the automation —
 * it widens it, which is the opposite of what adding a check was for.
 */
const MATCHES_EVERYTHING: Record<string, string> = {
  channel_is: "left blank this matches **every channel**, not just the ones you meant",
  role_has: "left blank this matches **everyone**, whatever roles they have",
  user_is: "left blank this matches **anyone**",
  has_permission: "left blank this matches **everyone**",
  content_contains: "left blank this matches **every message**",
  content_starts_with: "left blank this matches **every message**",
  content_ends_with: "left blank this matches **every message**",
  content_exactly: "left blank this matches **every message**",
  content_regex: "left blank this matches **every message**",
  day_of_week: "left blank this matches **every day**",
  counter_compare: "without a tally name there is nothing to compare",
  variable_compare: "without a note name there is nothing to check",
};

/**
 * True when "Send a message" would have nothing at all to send.
 *
 * Cross-field by nature: an embed with only a title and no body is perfectly
 * valid, and so is plain text with no embed, so neither field can be marked
 * required on its own. Without this the step fails at runtime, and the failure
 * is buried in a run log nobody is watching.
 */
function nothingToSend(config: SettingsValues): boolean {
  if (String(config.content ?? "").trim()) return false;
  if (String(config.text_above ?? "").trim()) return false;
  if (!config.use_embed) return true;
  return !["embed_title", "embed_image", "embed_thumbnail", "embed_footer", "embed_author"].some(
    (key) => String(config[key] ?? "").trim(),
  );
}

const ACTION_CHECKS: Record<
  string,
  { predicate: (config: SettingsValues) => boolean; key: string; note: string }
> = {
  send_message: {
    predicate: nothingToSend,
    key: "content",
    note: "there's nothing to send yet, so this step would fail every time",
  },
};

/**
 * Fields that carry a fallback rather than a value.
 *
 * `channel` on "Send a message" means "wherever this happened" when blank, so
 * it is only a problem on a trigger that has no channel of its own.
 */
const NEEDS_CONTEXT: Record<string, Record<string, string>> = {
  send_message: { channel: "channel" },
  set_slowmode: { channel: "channel" },
};

function refOf(where: BlankWhere, path: StepPath, key: string): string {
  return `${where}:${path.join(".")}:${key}`;
}

function blanksIn(
  fields: Field[],
  config: SettingsValues,
  provides: ReadonlySet<string>,
  contextFallbacks: Record<string, string> = {},
): { field: Field; note: string }[] {
  const found: { field: Field; note: string }[] = [];
  for (const field of fields) {
    if (!isVisible(field, config)) continue;
    if (!isUnset(field, config)) continue;

    const fallback = contextFallbacks[field.key];
    if (fallback) {
      // Blank means "use the one from the event", so this is only a problem
      // when the event hasn't got one.
      if (!provides.has(fallback)) {
        found.push({
          field,
          note: `this doesn't happen anywhere in particular, so a ${fallback} has to be picked here`,
        });
      }
      continue;
    }
    if (field.required) found.push({ field, note: "" });
  }
  return found;
}

export function blanks(automation: Automation): Blank[] {
  const found: Blank[] = [];
  const trigger = TRIGGERS[automation.triggerType];
  const provides = new Set(trigger?.provides ?? []);

  if (trigger) {
    for (const { field, note } of blanksIn(trigger.fields ?? [], automation.triggerConfig, provides)) {
      found.push({
        where: "trigger",
        path: [],
        field,
        owner: trigger.label,
        note,
        ref: refOf("trigger", [], field.key),
      });
    }
  }

  for (const { path, node } of walkConditions(automation.conditions)) {
    if (path.length === 0 || isGroup(node)) continue;
    const spec = CONDITIONS[node.type];
    if (!spec) continue;
    const why = MATCHES_EVERYTHING[node.type] ?? "";
    for (const { field, note } of blanksIn(spec.fields ?? [], node.config, provides)) {
      found.push({
        where: "condition",
        path,
        field,
        owner: spec.label,
        note: note || why,
        ref: refOf("condition", path, field.key),
      });
    }
  }

  for (const { path, step } of walkSteps(automation.steps)) {
    if (isBranch(step)) continue;
    const spec = ACTIONS[step.type];
    if (!spec) continue;

    const seen = new Set<string>();
    for (const { field, note } of blanksIn(
      spec.fields ?? [],
      step.config,
      provides,
      NEEDS_CONTEXT[step.type] ?? {},
    )) {
      seen.add(field.key);
      found.push({
        where: "action",
        path,
        field,
        owner: spec.label,
        note,
        ref: refOf("action", path, field.key),
      });
    }

    const extra = ACTION_CHECKS[step.type];
    if (extra && !seen.has(extra.key) && extra.predicate(step.config)) {
      const field = (spec.fields ?? []).find((candidate) => candidate.key === extra.key);
      if (field) {
        found.push({
          where: "action",
          path,
          field,
          owner: spec.label,
          note: extra.note,
          ref: refOf("action", path, field.key),
        });
      }
    }
  }

  return found;
}

/** Whether this could actually run: a trigger, something to do, no blanks. */
export function isReady(automation: Automation): boolean {
  if (!TRIGGERS[automation.triggerType]) return false;
  if (automation.steps.length === 0) return false;
  return blanks(automation).length === 0;
}

/** One line for a card — what is left, or "" when nothing is. */
export function readinessSummary(automation: Automation): string {
  if (!TRIGGERS[automation.triggerType]) return "no trigger chosen yet";
  if (automation.steps.length === 0) return "nothing for it to do yet";
  const count = blanks(automation).length;
  if (!count) return "";
  return `${count} thing${count === 1 ? "" : "s"} still to fill in`;
}

/**
 * Permissions the bot needs for these steps but might not hold.
 *
 * Gathered here and checked against the guild's real permissions on the page,
 * so a missing permission is a warning while you build rather than a silent
 * failure at 3am.
 */
export function requiredPermissions(automation: Automation): string[] {
  const needed = new Set<string>();
  for (const { step } of walkSteps(automation.steps)) {
    if (isBranch(step)) continue;
    for (const perm of ACTIONS[step.type]?.perms ?? []) needed.add(perm);
  }
  return [...needed].sort();
}

/** Steps that take something away, so the UI can say so before Turn on. */
export function destructiveSteps(automation: Automation): string[] {
  const found: string[] = [];
  for (const { step } of walkSteps(automation.steps)) {
    if (isBranch(step)) continue;
    const spec = ACTIONS[step.type];
    if (spec?.destructive) found.push(spec.label);
  }
  return found;
}
