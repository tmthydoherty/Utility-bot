"use client";

import * as React from "react";
import { motion } from "motion/react";

import { cn } from "@/lib/utils";
import { listItemVariants } from "@/lib/motion";
import { economy } from "@/lib/schema/modules";
import { isVisible, type FieldValue, type Section, type SettingsValues } from "@/lib/schema/types";
import { validateModule } from "@/lib/schema/validate";
import { saveModuleSettings } from "@/app/actions/settings";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { FieldRow } from "@/components/settings/field-renderer";
import { SectionCard } from "./section-card";

/**
 * One Economy tab's settings, rendered from a subset of the economy schema.
 *
 * It is a close cousin of `ModuleSettingsForm`, with two differences that earn
 * the separate component: every section is a `SectionCard` that stays read-only
 * until its pencil is clicked, and the panel only *renders* the sections this
 * tab owns while still holding and saving the module's full value set. That
 * second part matters — the bridge write replaces the whole overrides table, so
 * submitting a partial set would blank every field the other tabs manage.
 * Keeping the untouched values in `values` and passing them straight through on
 * save is what keeps Points, Shop and Config from clobbering each other.
 */
export function EconomySettingsPanel({
  guildId,
  sections,
  initialValues,
}: {
  guildId: string;
  sections: Section[];
  initialValues: SettingsValues;
}) {
  const toast = useToast();

  const [saved, setSaved] = React.useState(initialValues);
  const [values, setValues] = React.useState(initialValues);
  const [errors, setErrors] = React.useState<Record<string, string>>({});
  const [saving, setSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);
  const [editing, setEditing] = React.useState<Set<string>>(() => new Set());

  // Only the fields this tab shows can move, so the change count and the dirty
  // flag are computed over exactly those — the rest of the module's values ride
  // along untouched.
  const renderedKeys = React.useMemo(
    () => sections.flatMap((section) => section.fields.map((field) => field.key)),
    [sections],
  );
  const changedKeys = React.useMemo(
    () => renderedKeys.filter((key) => JSON.stringify(values[key]) !== JSON.stringify(saved[key])),
    [renderedKeys, values, saved],
  );
  const dirty = changedKeys.length > 0;

  React.useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  const setValue = (key: string, value: FieldValue) => {
    setValues((current) => ({ ...current, [key]: value }));
    setErrors((current) => {
      if (!current[key]) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  };

  const toggleSection = (id: string) => {
    setEditing((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const reset = () => {
    setValues(saved);
    setErrors({});
    setSaveError(null);
    setEditing(new Set());
  };

  const save = async () => {
    const local = validateModule(economy, values);
    if (!local.ok) {
      setErrors(local.errors);
      setSaveError("Some fields need attention.");
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
      const result = await saveModuleSettings(guildId, economy.id, values);
      if (result.ok) {
        setSaved(values);
        setErrors({});
        setEditing(new Set());
        toast.success("Saved", "Your changes are on their way to Vibey.");
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

  return (
    <div className="space-y-5 pb-28 lg:pb-24">
      {sections.map((section, index) => {
        const isEditing = editing.has(section.id);
        return (
          <motion.div
            key={section.id}
            custom={index}
            variants={listItemVariants}
            initial="hidden"
            animate="visible"
          >
            <SectionCard
              title={section.title}
              description={section.description}
              editing={isEditing}
              onToggle={() => toggleSection(section.id)}
            >
              <div className={cn("grid gap-5 p-5 sm:grid-cols-2 sm:p-6")}>
                {section.fields.map((field) =>
                  isVisible(field, values) ? (
                    <FieldRow
                      key={field.key}
                      field={field}
                      value={values[field.key] ?? null}
                      onChange={(value) => setValue(field.key, value)}
                      error={errors[field.key]}
                      disabled={saving || !isEditing}
                    />
                  ) : null,
                )}
              </div>
            </SectionCard>
          </motion.div>
        );
      })}

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
