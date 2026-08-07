/**
 * The web mirror of `cogs/utility/schema.py`.
 *
 * The bot already describes a setting as data — type, label, help text, when
 * it applies — and renders Discord components from that description
 * (`cogs/utility/panel/fields.py`) rather than hand-building each panel. The
 * same descriptions drive this dashboard: `FieldRenderer` maps a `FieldType`
 * to a web control once, and every module after the first is a schema file
 * rather than a page.
 *
 * Keep `FieldType` in step with the Python enum. A value that exists on one
 * side and not the other is a module that renders on Discord and not here (or
 * worse, the reverse).
 */

export const FieldType = {
  BOOL: "bool",
  TEXT: "text",
  MULTILINE: "multiline",
  NUMBER: "number",
  DURATION: "duration",
  CHANNEL: "channel",
  ROLE: "role",
  USER: "user",
  EMOJI: "emoji",
  CHOICE: "choice",
} as const;

export type FieldType = (typeof FieldType)[keyof typeof FieldType];

/** A settings value, in the shape it is stored in. */
export type FieldValue = string | number | boolean | string[] | null;

export type SettingsValues = Record<string, FieldValue>;

export interface Choice {
  value: string;
  label: string;
  description?: string;
}

/**
 * Conditional visibility, expressed as data rather than as a predicate.
 *
 * The obvious design is `visibleWhen: (values) => values.x === true`, and it
 * was the first one here. It cannot work: module schemas are read on the
 * server and handed to a client component, and a function is not serialisable
 * — React refuses to send it, and every settings page fails to render.
 *
 * Describing the condition instead means it crosses the boundary intact, can
 * be evaluated identically on both sides, and stays comparable to the
 * `visible_when` predicates in cogs/utility/schema.py.
 */
export type Condition =
  | { key: string; equals: FieldValue }
  | { key: string; notEquals: FieldValue }
  | { key: string; isSet: true }
  | { all: Condition[] }
  | { any: Condition[] };

function sameValue(a: FieldValue, b: FieldValue): boolean {
  if (Array.isArray(a) || Array.isArray(b)) return JSON.stringify(a) === JSON.stringify(b);
  return a === b;
}

export function evaluateCondition(condition: Condition, values: SettingsValues): boolean {
  if ("all" in condition) return condition.all.every((c) => evaluateCondition(c, values));
  if ("any" in condition) return condition.any.some((c) => evaluateCondition(c, values));

  const value = values[condition.key] ?? null;

  if ("isSet" in condition) {
    if (value === null || value === undefined) return false;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === "string") return value.trim() !== "";
    return true;
  }
  if ("notEquals" in condition) return !sameValue(value, condition.notEquals);
  return sameValue(value, condition.equals);
}

export interface Field {
  key: string;
  label: string;
  type: FieldType;
  /** Shown under the control. Explain the consequence, not the noun. */
  help?: string;
  /** CHOICE only. */
  choices?: Choice[];
  /** CHANNEL / ROLE / USER: a list rather than a single value. */
  multi?: boolean;
  /** NUMBER / DURATION bounds; also the length bounds for TEXT. */
  min?: number;
  max?: number;
  placeholder?: string;
  default?: FieldValue;
  required?: boolean;
  /**
   * Hide a field that only makes sense given another field's value — the same
   * idea as `visible_when` in schema.py. Re-evaluated on every change, so the
   * form reshapes as the user types.
   */
  visibleWhen?: Condition;
  /** CHANNEL only: restrict the picker (e.g. voice-only destinations). */
  channelTypes?: number[];
  /** Renders the control across the full width of a two-column section. */
  wide?: boolean;
}

export interface Section {
  id: string;
  title: string;
  description?: string;
  fields: Field[];
}

export type ModuleCategory =
  | "engagement"
  | "economy"
  | "moderation"
  | "community"
  | "esports"
  | "utility"
  | "logging";

export interface ModuleSchema {
  id: string;
  name: string;
  description: string;
  /** lucide-react icon name, resolved through components/ui/icon.tsx. */
  icon: string;
  category: ModuleCategory;
  /** The cog this maps to, so the eventual wiring is unambiguous. */
  cog: string;
  /** False while a module is listed but not yet configurable from the web. */
  configurable: boolean;
  sections?: Section[];
}

export const CATEGORY_LABELS: Record<ModuleCategory, string> = {
  engagement: "Engagement",
  economy: "Economy & Levels",
  moderation: "Moderation & Safety",
  community: "Community",
  esports: "Esports",
  utility: "Utility",
  logging: "Logging",
};

export const CATEGORY_ORDER: ModuleCategory[] = [
  "engagement",
  "economy",
  "community",
  "moderation",
  "esports",
  "utility",
  "logging",
];

/** Every field in a module, flattened out of its sections. */
export function allFields(module: ModuleSchema): Field[] {
  return (module.sections ?? []).flatMap((section) => section.fields);
}

/**
 * The blank-slate value for a field.
 *
 * Multi-valued ID fields default to an empty array rather than null so form
 * state never has to special-case "not set yet" against "set to nothing".
 */
export function defaultValue(field: Field): FieldValue {
  if (field.default !== undefined) return field.default;
  switch (field.type) {
    case FieldType.BOOL:
      return false;
    case FieldType.NUMBER:
    case FieldType.DURATION:
      return field.min ?? 0;
    case FieldType.TEXT:
    case FieldType.MULTILINE:
      return "";
    default:
      return field.multi ? [] : null;
  }
}

export function defaultsFor(module: ModuleSchema): SettingsValues {
  const values: SettingsValues = {};
  for (const field of allFields(module)) values[field.key] = defaultValue(field);
  return values;
}

/**
 * Whether a field has no usable value.
 *
 * Mirrors `is_unset` in schema.py, including its one sharp edge: zero counts
 * as empty for an ID field (an unfilled channel placeholder is 0) but is a
 * perfectly good NUMBER — slowmode 0 means "off", not "unconfigured".
 */
export function isUnset(field: Field, values: SettingsValues): boolean {
  const value = values[field.key];
  if (value === null || value === undefined) return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "string") return value.trim() === "";
  if (typeof value === "number") {
    const isIdField =
      field.type === FieldType.CHANNEL ||
      field.type === FieldType.ROLE ||
      field.type === FieldType.USER;
    return isIdField && value === 0;
  }
  return false;
}

export function isVisible(field: Field, values: SettingsValues): boolean {
  return field.visibleWhen ? evaluateCondition(field.visibleWhen, values) : true;
}

/** Required fields that are still empty — drives the setup checklist. */
export function missingRequired(module: ModuleSchema, values: SettingsValues): Field[] {
  return allFields(module).filter(
    (field) => field.required && isVisible(field, values) && isUnset(field, values),
  );
}
