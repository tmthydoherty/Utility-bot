/**
 * The web mirror of `cogs/automations/models.py`.
 *
 * An automation is a trigger plus a list of steps. A step is either an action
 * or an `if`, and an `if` holds two more step lists. That one recursive shape
 * gives branching and nesting without a node-graph editor, and composition
 * comes free because "run another automation" is just another action.
 *
 * Conditions are a tree of their own: `and`/`or` groups holding leaves or more
 * groups, each of which may be negated.
 *
 * These types describe exactly what is stored in `automations.db`, so the
 * engine and this dashboard can never disagree about what an automation *is*.
 * Where they differ is only in what they call things: the Python side names
 * them for the person reading the code, and the registry here names them for
 * the person reading the screen.
 */

import type { FieldValue, SettingsValues } from "@/lib/schema/types";

/** Guards a hand-edited graph from recursing forever. Mirrors MAX_DEPTH. */
export const MAX_DEPTH = 8;
/** Per automation, counted across the whole tree. Mirrors MAX_STEPS. */
export const MAX_STEPS = 50;

// ------------------------------------------------------------------ conditions

export interface ConditionLeaf {
  type: string;
  config: SettingsValues;
  negate?: boolean;
}

export interface ConditionGroup {
  op: "and" | "or";
  items: ConditionNode[];
  negate?: boolean;
}

export type ConditionNode = ConditionGroup | ConditionLeaf;

export function isGroup(node: ConditionNode): node is ConditionGroup {
  return "op" in node;
}

/** An empty group matches everything — worth saying out loud in the UI. */
export function isEmptyGroup(node: ConditionNode): boolean {
  return isGroup(node) && node.items.length === 0;
}

export function emptyConditions(): ConditionGroup {
  return { op: "and", items: [] };
}

/**
 * Parse whatever is in the database column, however old or hand-edited.
 *
 * `{}` is what an automation with no conditions stores, and a leaf with no
 * type is not a condition — it is the absence of one. Returning a leaf there
 * would put a nameless entry in the tree, which renders as a blank row.
 */
export function parseCondition(data: unknown, depth = 0): ConditionNode {
  if (depth > MAX_DEPTH || typeof data !== "object" || data === null) {
    return emptyConditions();
  }
  const record = data as Record<string, unknown>;

  if ("op" in record) {
    const items = Array.isArray(record.items) ? record.items : [];
    return {
      op: record.op === "or" ? "or" : "and",
      items: items.filter(isRealCondition).map((item) => parseCondition(item, depth + 1)),
      negate: Boolean(record.negate),
    };
  }
  if (!record.type) return emptyConditions();

  return {
    type: String(record.type),
    config: (record.config as SettingsValues) ?? {},
    negate: Boolean(record.negate),
  };
}

function isRealCondition(data: unknown): boolean {
  if (typeof data !== "object" || data === null) return false;
  const record = data as Record<string, unknown>;
  return Boolean(record.op || record.type);
}

// ----------------------------------------------------------------------- steps

export interface Step {
  type: string;
  config: SettingsValues;
  /** Only on an `if` step. */
  conditions?: ConditionGroup;
  then?: Step[];
  otherwise?: Step[];
}

export function isBranch(step: Step): boolean {
  return step.type === "if";
}

export function parseStep(data: unknown, depth = 0): Step | null {
  if (depth > MAX_DEPTH || typeof data !== "object" || data === null) return null;
  const record = data as Record<string, unknown>;
  const type = String(record.type ?? "");
  if (!type) return null;

  if (type === "if") {
    const conditions = parseCondition(record.conditions ?? {});
    return {
      type: "if",
      config: {},
      conditions: isGroup(conditions) ? conditions : { op: "and", items: [conditions] },
      then: parseSteps(record.then, depth + 1),
      otherwise: parseSteps(record.else, depth + 1),
    };
  }
  return { type, config: (record.config as SettingsValues) ?? {} };
}

export function parseSteps(data: unknown, depth = 0): Step[] {
  if (!Array.isArray(data)) return [];
  return data
    .map((entry) => parseStep(entry, depth))
    .filter((step): step is Step => step !== null);
}

/** Back to the exact JSON shape `models.py` reads. */
export function serialiseStep(step: Step): Record<string, unknown> {
  if (isBranch(step)) {
    return {
      type: "if",
      conditions: serialiseCondition(step.conditions ?? emptyConditions()),
      then: (step.then ?? []).map(serialiseStep),
      // `else` in the stored JSON; `otherwise` here only because `else` is a
      // reserved word in a JS object destructure and reads badly as a field.
      else: (step.otherwise ?? []).map(serialiseStep),
    };
  }
  return { type: step.type, config: step.config };
}

export function serialiseCondition(node: ConditionNode): Record<string, unknown> {
  if (isGroup(node)) {
    const data: Record<string, unknown> = {
      op: node.op,
      items: node.items.map(serialiseCondition),
    };
    if (node.negate) data.negate = true;
    return data;
  }
  const data: Record<string, unknown> = { type: node.type, config: node.config };
  if (node.negate) data.negate = true;
  return data;
}

export function countSteps(steps: Step[]): number {
  return steps.reduce(
    (total, step) =>
      total + 1 + countSteps(step.then ?? []) + countSteps(step.otherwise ?? []),
    0,
  );
}

// ------------------------------------------------------------------ addressing

/**
 * A step's address: alternating index and branch side, matching `walk_steps`
 * and `find_step_in` on the Python side.
 *
 * `[2, 0, 1]` reads as "step 2, its *then* branch, step 1". Keeping the same
 * encoding on both sides means a path written by the Discord panel resolves
 * here and vice versa.
 */
export type StepPath = number[];

export function findStep(steps: Step[], path: StepPath): Step | null {
  let current = steps;
  let step: Step | null = null;
  let position = 0;

  while (position < path.length) {
    const index = path[position]!;
    if (index < 0 || index >= current.length) return null;
    step = current[index]!;
    position += 1;
    if (position < path.length) {
      if (!isBranch(step)) return null;
      const side = path[position];
      current = (side === 0 ? step.then : step.otherwise) ?? [];
      position += 1;
    }
  }
  return step;
}

/** The list a path lives in, for appending, removing or reordering. */
export function containerFor(steps: Step[], path: StepPath): Step[] | null {
  let current = steps;
  let position = 0;
  while (position < path.length - 1) {
    const index = path[position]!;
    if (index < 0 || index >= current.length) return null;
    const step = current[index]!;
    position += 1;
    if (!isBranch(step)) return null;
    current = (path[position] === 0 ? step.then : step.otherwise) ?? [];
    position += 1;
  }
  return current;
}

export interface WalkedStep {
  path: StepPath;
  step: Step;
  depth: number;
}

/** Every step, parents before children — the order they read on screen. */
export function walkSteps(steps: Step[], path: StepPath = [], depth = 0): WalkedStep[] {
  const found: WalkedStep[] = [];
  steps.forEach((step, index) => {
    const here = [...path, index];
    found.push({ path: here, step, depth });
    if (isBranch(step)) {
      found.push(...walkSteps(step.then ?? [], [...here, 0], depth + 1));
      found.push(...walkSteps(step.otherwise ?? [], [...here, 1], depth + 1));
    }
  });
  return found;
}

export function findCondition(root: ConditionGroup, path: number[]): ConditionNode | null {
  let node: ConditionNode = root;
  for (const index of path) {
    if (!isGroup(node)) return null;
    if (index < 0 || index >= node.items.length) return null;
    node = node.items[index]!;
  }
  return node;
}

export interface WalkedCondition {
  path: number[];
  node: ConditionNode;
  depth: number;
}

export function walkConditions(
  node: ConditionNode,
  path: number[] = [],
  depth = 0,
): WalkedCondition[] {
  const found: WalkedCondition[] = [{ path, node, depth }];
  if (isGroup(node)) {
    node.items.forEach((child, index) => {
      found.push(...walkConditions(child, [...path, index], depth + 1));
    });
  }
  return found;
}

// ------------------------------------------------------------------ automation

export type CooldownScope = "user" | "channel" | "guild";

export interface Automation {
  id: string;
  guildId: string;
  name: string;
  description: string;
  triggerType: string;
  triggerConfig: SettingsValues;
  /** The top-level gate, checked before any step runs. */
  conditions: ConditionGroup;
  steps: Step[];
  enabled: boolean;
  /** "Test mode" on screen — it decides, and reports, but never acts. */
  dryRun: boolean;
  priority: number;
  stopAfter: boolean;
  cooldownSeconds: number;
  cooldownScope: CooldownScope;
  allowBots: boolean;
  lastFiredAt: number;
  runCount: number;
  createdAt: number;
  updatedAt: number;
  updatedBy: string | null;
  /** Which ready-made automation this started from. Never read by the engine. */
  templateKey: string;
}

/** The three states an automation can be in, as the UI talks about them. */
export type AutomationState = "off" | "testing" | "live";

export function stateOf(automation: Pick<Automation, "enabled" | "dryRun">): AutomationState {
  if (!automation.enabled) return "off";
  return automation.dryRun ? "testing" : "live";
}

export const STATE_LABELS: Record<AutomationState, string> = {
  off: "Off",
  testing: "Test mode",
  live: "Running",
};

export const STATE_BLURBS: Record<AutomationState, string> = {
  off: "Switched off. Nothing happens.",
  testing: "Watching for its trigger and writing down what it would do — but not doing it.",
  live: "Running for real.",
};

export interface RunRecord {
  id: number;
  automationId: string;
  at: number;
  outcome: "fired" | "skipped" | "error" | "dry_run" | "deferred";
  summary: string;
  durationMs: number;
  trace: TraceEntry[];
}

export interface TraceEntry {
  kind: string;
  label: string;
  result: boolean | null;
  detail: string;
}

export type { FieldValue, SettingsValues };
