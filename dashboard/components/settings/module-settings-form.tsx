"use client";

import * as React from "react";
import { motion } from "motion/react";

import { cn } from "@/lib/utils";
import { listItemVariants } from "@/lib/motion";
import {
  allFields,
  isVisible,
  missingRequired,
  type FieldValue,
  type ModuleSchema,
  type SettingsValues,
} from "@/lib/schema/types";
import { validateModule } from "@/lib/schema/validate";
import { saveModuleSettings } from "@/app/actions/settings";
import { Card } from "@/components/ui/card";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { FieldRow } from "./field-renderer";

/**
 * A module's settings, rendered from its schema.
 *
 * Holds the working copy of the values and the saved baseline side by side.
 * Everything the user sees about "unsaved" — the bar, the change count, the
 * discard — is derived from comparing those two, so it cannot fall out of sync
 * with reality the way a separate `isDirty` flag does.
 */
export function ModuleSettingsForm({
  module,
  guildId,
  initialValues,
}: {
  module: ModuleSchema;
  guildId: string;
  initialValues: SettingsValues;
}) {
  const toast = useToast();

  const [saved, setSaved] = React.useState(initialValues);
  const [values, setValues] = React.useState(initialValues);
  const [errors, setErrors] = React.useState<Record<string, string>>({});
  const [saving, setSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);

  const changedKeys = React.useMemo(
    () =>
      allFields(module)
        .map((field) => field.key)
        .filter((key) => JSON.stringify(values[key]) !== JSON.stringify(saved[key])),
    [module, values, saved],
  );

  const dirty = changedKeys.length > 0;

  // Browsers only honour this if the page has been interacted with, which is
  // exactly the case we care about.
  React.useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  const setValue = (key: string, value: FieldValue) => {
    setValues((current) => ({ ...current, [key]: value }));
    // Clear the error as soon as the field is touched; re-validating on every
    // keystroke means an error appears before the value is finished being typed.
    setErrors((current) => {
      if (!current[key]) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  };

  const reset = () => {
    setValues(saved);
    setErrors({});
    setSaveError(null);
  };

  const save = async () => {
    const local = validateModule(module, values);
    if (!local.ok) {
      setErrors(local.errors);
      setSaveError("Some fields need attention.");
      // Bring the first problem into view — on a long settings page it is
      // routinely off screen, and a save that appears to do nothing is worse
      // than one that fails loudly.
      const firstKey = Object.keys(local.errors)[0];
      if (firstKey) {
        document
          .getElementById(`field-${firstKey}`)
          ?.scrollIntoView({ behavior: "smooth", block: "center" });
      }
      return;
    }

    setSaving(true);
    setSaveError(null);
    try {
      const result = await saveModuleSettings(guildId, module.id, values);
      if (result.ok) {
        setSaved(values);
        setErrors({});
        toast.success("Settings saved", `${module.name} updated.`);
      } else {
        setErrors(result.fieldErrors ?? {});
        setSaveError(result.error ?? "Something went wrong.");
      }
    } catch {
      setSaveError("Couldn't reach the server. Your changes are still here.");
    } finally {
      setSaving(false);
    }
  };

  const incomplete = missingRequired(module, values);

  return (
    <div className="space-y-5 pb-28 lg:pb-24">
      {incomplete.length > 0 && (
        <Card className="border-[var(--warning)]/30 bg-[var(--warning-soft)] p-4">
          <p className="text-sm font-medium text-[var(--warning)]">
            {incomplete.length} required{" "}
            {incomplete.length === 1 ? "setting is" : "settings are"} still empty
          </p>
          <p className="mt-1 text-sm text-fg-muted">
            {incomplete.map((field) => field.label).join(", ")}
          </p>
        </Card>
      )}

      {(module.sections ?? []).map((section, index) => (
        <motion.div
          key={section.id}
          custom={index}
          variants={listItemVariants}
          initial="hidden"
          animate="visible"
        >
          <Card>
            <div className="space-y-1 p-5 pb-0 sm:p-6 sm:pb-0">
              <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">
                {section.title}
              </h2>
              {section.description && (
                <p className="text-sm text-fg-muted">{section.description}</p>
              )}
            </div>
            <div
              className={cn(
                "grid gap-5 p-5 sm:p-6",
                // Two columns once there is room. Anything marked `wide` — a
                // message template, mostly — spans both.
                "sm:grid-cols-2",
              )}
            >
              {section.fields.map((field) =>
                isVisible(field, values) ? (
                  <FieldRow
                    key={field.key}
                    field={field}
                    value={values[field.key] ?? null}
                    onChange={(value) => setValue(field.key, value)}
                    error={errors[field.key]}
                    disabled={saving}
                  />
                ) : null,
              )}
            </div>
          </Card>
        </motion.div>
      ))}

      <SaveBar
        visible={dirty}
        saving={saving}
        error={saveError}
        changeCount={changedKeys.length}
        onSave={save}
        onReset={reset}
      />
    </div>
  );
}
