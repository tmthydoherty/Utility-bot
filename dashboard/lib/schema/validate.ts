import { z } from "zod";

import {
  FieldType,
  allFields,
  isVisible,
  type Field,
  type ModuleSchema,
  type SettingsValues,
} from "./types";

/**
 * Validation derived from the field spec, so the browser and the server are
 * incapable of disagreeing.
 *
 * The server re-validates from the same source on every save. Client-side
 * validation here is a courtesy to the user; the server's run is the one that
 * counts, because anything posted to a server action is attacker-controlled no
 * matter how nice the form was.
 */

const SNOWFLAKE = /^\d{17,20}$/;

function schemaForField(field: Field): z.ZodTypeAny {
  switch (field.type) {
    case FieldType.BOOL:
      return z.boolean();

    case FieldType.TEXT: {
      let text = z.string();
      if (field.min !== undefined) text = text.min(field.min);
      if (field.max !== undefined) text = text.max(field.max);
      return field.required ? text.min(1, "Required") : text;
    }

    case FieldType.MULTILINE: {
      // Discord's own message limit; anything longer cannot be sent anyway, so
      // catching it here beats a runtime failure inside the bot.
      const max = field.max ?? 2000;
      const text = z.string().max(max);
      return field.required ? text.min(1, "Required") : text;
    }

    case FieldType.NUMBER:
    case FieldType.DURATION: {
      let num = z.number().int();
      if (field.min !== undefined) num = num.min(field.min);
      if (field.max !== undefined) num = num.max(field.max);
      return field.required ? num : num.nullable();
    }

    case FieldType.CHANNEL:
    case FieldType.ROLE:
    case FieldType.USER: {
      const id = z.string().regex(SNOWFLAKE, "Not a valid Discord ID");
      if (field.multi) {
        const list = z.array(id);
        return field.required ? list.min(1, "Pick at least one") : list;
      }
      return field.required ? id : id.nullable();
    }

    case FieldType.EMOJI: {
      const emoji = z.string().max(64);
      return field.required ? emoji.min(1, "Required") : emoji.nullable();
    }

    case FieldType.CHOICE: {
      const values = (field.choices ?? []).map((c) => c.value);
      if (values.length === 0) return z.string().nullable();
      // z.enum needs a non-empty tuple; the guard above guarantees it.
      const choice = z.enum(values as [string, ...string[]]);
      return field.required ? choice : choice.nullable();
    }

    default:
      return z.unknown();
  }
}

/**
 * A validator for one module, given the values being submitted.
 *
 * Hidden fields are skipped: a `visibleWhen` field the user can't see must not
 * be able to block a save, and requiring one that isn't on screen produces an
 * error with nowhere to point.
 */
export function buildValidator(module: ModuleSchema, values: SettingsValues) {
  const shape: Record<string, z.ZodTypeAny> = {};
  for (const field of allFields(module)) {
    if (!isVisible(field, values)) continue;
    shape[field.key] = schemaForField(field);
  }
  // Unknown keys are dropped rather than rejected, so a stale browser tab
  // posting a field that has since been removed still saves the rest.
  return z.object(shape).strip();
}

export interface ValidationResult {
  ok: boolean;
  values: SettingsValues;
  /** Field key -> first error message. */
  errors: Record<string, string>;
}

export function validateModule(
  module: ModuleSchema,
  values: SettingsValues,
): ValidationResult {
  const parsed = buildValidator(module, values).safeParse(values);

  if (parsed.success) {
    return { ok: true, values: parsed.data as SettingsValues, errors: {} };
  }

  const errors: Record<string, string> = {};
  for (const issue of parsed.error.issues) {
    const key = issue.path[0];
    if (typeof key === "string" && !errors[key]) errors[key] = issue.message;
  }
  return { ok: false, values, errors };
}

/** Validation for a single field, for use as the user leaves an input. */
export function validateField(field: Field, value: unknown): string | null {
  const result = schemaForField(field).safeParse(value);
  return result.success ? null : (result.error.issues[0]?.message ?? "Invalid value");
}
