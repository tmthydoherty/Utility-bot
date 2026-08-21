"use client";

import * as React from "react";
import { AnimatePresence, motion } from "motion/react";
import { ChevronDown, Plus, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { transitions } from "@/lib/motion";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import type { SettingsValues } from "@/lib/schema/types";
import {
  ACTIONS,
  CONDITIONS,
  TRIGGERS,
  actionsFor,
  conditionsFor,
} from "@/lib/automations/registry";
import { conditionPhrase, actionPhrase } from "@/lib/automations/sentence";
import {
  isGroup,
  type ConditionGroup,
  type ConditionLeaf,
  type ConditionNode,
  type Step,
} from "@/lib/automations/types";

import { CataloguePicker, type CatalogueEntry } from "./catalogue-picker";
import { FieldGroup } from "./field-group";
import { Fragments, useNamer } from "./sentence";

/**
 * The three sections the builder is made of.
 *
 * They are numbered ① ② ③ and named after the questions they answer, not after
 * what the engine calls them. Section ② is explicitly marked optional, because
 * the single most common thing a first automation needs is no checks at all
 * and an empty section that looks required is how people talk themselves out
 * of finishing.
 */

function SectionShell({
  step,
  title,
  subtitle,
  optional,
  children,
  id,
}: {
  step: string;
  title: string;
  subtitle: string;
  optional?: boolean;
  children: React.ReactNode;
  id?: string;
}) {
  return (
    <Card className="min-w-0 p-5 sm:p-6" id={id}>
      <div className="mb-4 flex items-start gap-3">
        <span
          className="grid size-7 shrink-0 place-items-center rounded-full bg-[var(--accent-soft)] text-sm font-semibold text-[var(--accent)]"
          aria-hidden
        >
          {step}
        </span>
        <div className="min-w-0">
          <h2 className="flex flex-wrap items-center gap-2 font-semibold tracking-tight">
            {title}
            {optional && (
              <span className="rounded-full bg-[var(--surface-active)] px-2 py-0.5 text-[11px] font-normal text-fg-subtle">
                optional
              </span>
            )}
          </h2>
          <p className="mt-0.5 text-sm text-fg-muted">{subtitle}</p>
        </div>
      </div>
      {children}
    </Card>
  );
}

// ------------------------------------------------------------------- trigger

export function TriggerSection({
  triggerType,
  config,
  onChangeType,
  onChangeConfig,
}: {
  triggerType: string;
  config: SettingsValues;
  onChangeType: (type: string) => void;
  onChangeConfig: (config: SettingsValues) => void;
}) {
  const [picking, setPicking] = React.useState(false);
  const spec = TRIGGERS[triggerType];

  const entries: CatalogueEntry[] = Object.values(TRIGGERS);

  return (
    <SectionShell
      id="section-when"
      step="1"
      title="When this happens"
      subtitle="What sets the automation off."
    >
      <button
        type="button"
        onClick={() => setPicking(true)}
        className={cn(
          "flex w-full items-center gap-3 rounded-lg border border-[var(--border)] p-3 text-left",
          "transition-colors hover:border-[var(--border-strong)] hover:bg-[var(--surface-hover)]",
        )}
      >
        <span
          className="grid size-9 shrink-0 place-items-center rounded-md bg-[var(--accent-soft)] text-[var(--accent)]"
          aria-hidden
        >
          <Icon name={spec?.icon ?? "Zap"} className="size-[18px]" />
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-sm font-medium">{spec?.label ?? "Pick something"}</span>
          <span className="mt-0.5 block text-xs text-fg-muted">
            {spec?.plain ?? "Choose what sets this automation off."}
          </span>
        </span>
        <span className="shrink-0 text-xs font-medium text-[var(--accent)]">Change</span>
      </button>

      {spec?.fields && spec.fields.length > 0 && (
        <div className="mt-4 border-t border-[var(--border)] pt-4">
          <FieldGroup fields={spec.fields} values={config} onChange={onChangeConfig} />
        </div>
      )}

      <CataloguePicker
        open={picking}
        onOpenChange={setPicking}
        title="What should set this off?"
        description="Pick the thing that happens. Everything else follows from this."
        entries={entries}
        selected={triggerType}
        onPick={onChangeType}
      />
    </SectionShell>
  );
}

// ---------------------------------------------------------------- conditions

export function ConditionsSection({
  triggerType,
  conditions,
  onChange,
}: {
  triggerType: string;
  conditions: ConditionGroup;
  onChange: (next: ConditionGroup) => void;
}) {
  const [picking, setPicking] = React.useState(false);
  const names = useNamer();

  const available = React.useMemo(() => conditionsFor(triggerType), [triggerType]);
  const entries: CatalogueEntry[] = available;

  // Only the top level is edited here. Nesting groups is real and rare, and
  // putting an and/or tree editor on the main screen would make the common
  // case — two or three flat checks — look far harder than it is. Anything
  // nested is shown read-only with a pointer at the Discord panel.
  const leaves = conditions.items;

  const setLeaf = (index: number, config: SettingsValues) => {
    const items = [...conditions.items];
    const node = items[index];
    if (!node || isGroup(node)) return;
    items[index] = { ...node, config };
    onChange({ ...conditions, items });
  };

  const setNegate = (index: number, negate: boolean) => {
    const items = [...conditions.items];
    const node = items[index];
    if (!node || isGroup(node)) return;
    items[index] = { ...node, negate };
    onChange({ ...conditions, items });
  };

  const removeAt = (index: number) => {
    onChange({ ...conditions, items: conditions.items.filter((_, i) => i !== index) });
  };

  const add = (key: string) => {
    const spec = CONDITIONS[key];
    if (!spec) return;
    const config: SettingsValues = {};
    for (const field of spec.fields ?? []) {
      if (field.default !== undefined) config[field.key] = field.default;
    }
    onChange({ ...conditions, items: [...conditions.items, { type: key, config }] });
  };

  return (
    <SectionShell
      id="section-only-if"
      step="2"
      title="Only if…"
      subtitle="Extra requirements before it runs. Leave this empty and it runs every time."
      optional
    >
      {leaves.length > 1 && (
        <div className="mb-3 flex flex-wrap items-center gap-2 text-sm">
          <span className="text-fg-muted">These requirements must</span>
          <div className="inline-flex rounded-md border border-[var(--border)] p-0.5">
            {(["and", "or"] as const).map((op) => (
              <button
                key={op}
                type="button"
                onClick={() => onChange({ ...conditions, op })}
                className={cn(
                  "rounded px-2.5 py-1 text-xs font-medium transition-colors",
                  conditions.op === op
                    ? "bg-[var(--accent-soft)] text-[var(--accent)]"
                    : "text-fg-subtle hover:text-fg",
                )}
              >
                {op === "and" ? "all be true" : "any one be true"}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="space-y-3">
        <AnimatePresence initial={false}>
          {leaves.map((node, index) => (
            <motion.div
              key={`${node && !isGroup(node) ? node.type : "group"}-${index}`}
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              transition={transitions.standard}
              className="overflow-hidden"
            >
              <ConditionRow
                node={node}
                onChangeConfig={(config) => setLeaf(index, config)}
                onChangeNegate={(negate) => setNegate(index, negate)}
                onRemove={() => removeAt(index)}
                names={names}
              />
            </motion.div>
          ))}
        </AnimatePresence>

        {leaves.length === 0 && (
          <p className="rounded-lg border border-dashed border-[var(--border)] px-4 py-6 text-center text-sm text-fg-subtle">
            No requirements. This runs every single time the thing above happens.
          </p>
        )}

        <Button variant="secondary" onClick={() => setPicking(true)} className="w-full justify-center">
          <Plus aria-hidden />
          Add a requirement
        </Button>
      </div>

      <CataloguePicker
        open={picking}
        onOpenChange={setPicking}
        title="Only run it if…"
        description="Narrow down when this automation fires."
        entries={entries}
        onPick={add}
      />
    </SectionShell>
  );
}

function ConditionRow({
  node,
  onChangeConfig,
  onChangeNegate,
  onRemove,
  names,
}: {
  node: ConditionNode;
  onChangeConfig: (config: SettingsValues) => void;
  onChangeNegate: (negate: boolean) => void;
  onRemove: () => void;
  names: ReturnType<typeof useNamer>;
}) {
  const [expanded, setExpanded] = React.useState(false);

  if (isGroup(node)) {
    return (
      <div className="rounded-lg border border-dashed border-[var(--border)] p-3 text-sm text-fg-muted">
        <p className="font-medium text-fg">A group of requirements</p>
        <p className="mt-1 text-xs">
          Groups within groups are edited from <code>/automations</code> in Discord. This one is
          left exactly as it is.
        </p>
      </div>
    );
  }

  const leaf = node as ConditionLeaf;
  const spec = CONDITIONS[leaf.type];
  const hasSettings = (spec?.fields ?? []).length > 0;

  return (
    <div className="rounded-lg border border-[var(--border)]">
      <div className="flex items-start gap-2 p-3">
        <span
          className="mt-0.5 grid size-7 shrink-0 place-items-center rounded-md bg-[var(--surface-active)] text-fg-muted"
          aria-hidden
        >
          <Icon name={spec?.icon ?? "Filter"} className="size-3.5" />
        </span>

        <div className="min-w-0 flex-1">
          <p className="text-sm">
            <Fragments fragments={conditionPhrase(leaf, names)} />
          </p>
          {spec && <p className="mt-0.5 text-xs text-fg-subtle">{spec.plain}</p>}
        </div>

        <div className="flex shrink-0 items-center gap-1">
          {hasSettings && (
            <button
              type="button"
              onClick={() => setExpanded((current) => !current)}
              aria-expanded={expanded}
              className="grid size-7 place-items-center rounded-md text-fg-subtle transition-colors hover:bg-[var(--surface-hover)] hover:text-fg"
              aria-label={expanded ? "Hide settings" : "Show settings"}
            >
              <ChevronDown
                className={cn("size-4 transition-transform", expanded && "rotate-180")}
                aria-hidden
              />
            </button>
          )}
          <button
            type="button"
            onClick={onRemove}
            className="grid size-7 place-items-center rounded-md text-fg-subtle transition-colors hover:bg-[var(--danger-soft)] hover:text-[var(--danger)]"
            aria-label="Remove this requirement"
          >
            <X className="size-4" aria-hidden />
          </button>
        </div>
      </div>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={transitions.standard}
            className="overflow-hidden"
          >
            <div className="space-y-4 border-t border-[var(--border)] p-3">
              <FieldGroup
                fields={spec?.fields ?? []}
                values={leaf.config}
                onChange={onChangeConfig}
              />
              {/* Negation as a plain-English choice rather than a NOT button.
                  "This requirement should — not match" is the wording the
                  Discord panel settled on for the same reason. */}
              <div className="flex flex-wrap items-center gap-2 border-t border-[var(--border)] pt-3 text-sm">
                <span className="text-fg-muted">This requirement should</span>
                <div className="inline-flex rounded-md border border-[var(--border)] p-0.5">
                  {[
                    { value: false, label: "match" },
                    { value: true, label: "not match" },
                  ].map((option) => (
                    <button
                      key={String(option.value)}
                      type="button"
                      onClick={() => onChangeNegate(option.value)}
                      className={cn(
                        "rounded px-2.5 py-1 text-xs font-medium transition-colors",
                        Boolean(leaf.negate) === option.value
                          ? "bg-[var(--accent-soft)] text-[var(--accent)]"
                          : "text-fg-subtle hover:text-fg",
                      )}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// --------------------------------------------------------------------- steps

export function StepsSection({
  triggerType,
  steps,
  onChange,
}: {
  triggerType: string;
  steps: Step[];
  onChange: (next: Step[]) => void;
}) {
  const [picking, setPicking] = React.useState(false);
  const names = useNamer();

  const entries: CatalogueEntry[] = React.useMemo(
    () => actionsFor(triggerType).map((spec) => ({ ...spec, destructive: spec.destructive })),
    [triggerType],
  );

  const add = (key: string) => {
    const spec = ACTIONS[key];
    if (!spec) return;
    const config: SettingsValues = {};
    for (const field of spec.fields ?? []) {
      if (field.default !== undefined) config[field.key] = field.default;
    }
    onChange([...steps, { type: key, config }]);
  };

  const setConfig = (index: number, config: SettingsValues) => {
    const next = [...steps];
    const step = next[index];
    if (!step) return;
    next[index] = { ...step, config };
    onChange(next);
  };

  const move = (index: number, delta: number) => {
    const target = index + delta;
    if (target < 0 || target >= steps.length) return;
    const next = [...steps];
    const [moved] = next.splice(index, 1);
    if (moved) next.splice(target, 0, moved);
    onChange(next);
  };

  return (
    <SectionShell
      id="section-then"
      step="3"
      title="Then do this"
      subtitle="What Vibey actually does. Steps run top to bottom."
    >
      <div className="space-y-3">
        <AnimatePresence initial={false}>
          {steps.map((step, index) => (
            <motion.div
              key={`${step.type}-${index}`}
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              transition={transitions.standard}
              className="overflow-hidden"
            >
              <StepRow
                step={step}
                index={index}
                total={steps.length}
                names={names}
                onChangeConfig={(config) => setConfig(index, config)}
                onRemove={() => onChange(steps.filter((_, i) => i !== index))}
                onMove={(delta) => move(index, delta)}
              />
            </motion.div>
          ))}
        </AnimatePresence>

        {steps.length === 0 && (
          <p className="rounded-lg border border-dashed border-[var(--border)] px-4 py-6 text-center text-sm text-fg-subtle">
            Nothing yet. Add at least one step or this automation has no job.
          </p>
        )}

        <Button variant="secondary" onClick={() => setPicking(true)} className="w-full justify-center">
          <Plus aria-hidden />
          Add a step
        </Button>
      </div>

      <CataloguePicker
        open={picking}
        onOpenChange={setPicking}
        title="Then do this…"
        description="What should Vibey do when everything above is true?"
        entries={entries}
        onPick={add}
      />
    </SectionShell>
  );
}

function StepRow({
  step,
  index,
  total,
  names,
  onChangeConfig,
  onRemove,
  onMove,
}: {
  step: Step;
  index: number;
  total: number;
  names: ReturnType<typeof useNamer>;
  onChangeConfig: (config: SettingsValues) => void;
  onRemove: () => void;
  onMove: (delta: number) => void;
}) {
  // A step with settings opens expanded when it is the only one, because on a
  // one-step automation the settings *are* the automation.
  const [expanded, setExpanded] = React.useState(total === 1);

  if (step.type === "if") {
    return (
      <div className="rounded-lg border border-dashed border-[var(--border)] p-3 text-sm">
        <p className="font-medium">A split — different steps depending on a requirement</p>
        <p className="mt-1 text-xs text-fg-muted">
          <Fragments fragments={actionPhrase(step, names)} />
        </p>
        <p className="mt-2 text-xs text-fg-subtle">
          Splits are edited from <code>/automations</code> in Discord. This one runs exactly as it
          is.
        </p>
      </div>
    );
  }

  const spec = ACTIONS[step.type];
  const hasSettings = (spec?.fields ?? []).length > 0;

  return (
    <div className="rounded-lg border border-[var(--border)]">
      <div className="flex items-start gap-2 p-3">
        <span
          className="mt-0.5 grid size-7 shrink-0 place-items-center rounded-full bg-[var(--surface-active)] text-xs font-semibold text-fg-muted"
          aria-hidden
        >
          {index + 1}
        </span>

        <div className="min-w-0 flex-1">
          <p className="flex flex-wrap items-center gap-2 text-sm">
            <Fragments fragments={actionPhrase(step, names)} />
            {spec?.destructive && (
              <span className="rounded-full bg-[var(--danger-soft)] px-1.5 py-0.5 text-[10px] font-medium text-[var(--danger)]">
                takes something away
              </span>
            )}
          </p>
          {spec && <p className="mt-0.5 text-xs text-fg-subtle">{spec.plain}</p>}
        </div>

        <div className="flex shrink-0 items-center gap-1">
          {total > 1 && (
            <div className="flex flex-col">
              <button
                type="button"
                onClick={() => onMove(-1)}
                disabled={index === 0}
                className="grid h-4 w-6 place-items-center rounded text-fg-subtle transition-colors hover:text-fg disabled:opacity-25"
                aria-label="Move this step up"
              >
                <ChevronDown className="size-3 rotate-180" aria-hidden />
              </button>
              <button
                type="button"
                onClick={() => onMove(1)}
                disabled={index === total - 1}
                className="grid h-4 w-6 place-items-center rounded text-fg-subtle transition-colors hover:text-fg disabled:opacity-25"
                aria-label="Move this step down"
              >
                <ChevronDown className="size-3" aria-hidden />
              </button>
            </div>
          )}
          {hasSettings && (
            <button
              type="button"
              onClick={() => setExpanded((current) => !current)}
              aria-expanded={expanded}
              className="grid size-7 place-items-center rounded-md text-fg-subtle transition-colors hover:bg-[var(--surface-hover)] hover:text-fg"
              aria-label={expanded ? "Hide settings" : "Show settings"}
            >
              <ChevronDown
                className={cn("size-4 transition-transform", expanded && "rotate-180")}
                aria-hidden
              />
            </button>
          )}
          <button
            type="button"
            onClick={onRemove}
            className="grid size-7 place-items-center rounded-md text-fg-subtle transition-colors hover:bg-[var(--danger-soft)] hover:text-[var(--danger)]"
            aria-label="Remove this step"
          >
            <X className="size-4" aria-hidden />
          </button>
        </div>
      </div>

      <AnimatePresence initial={false}>
        {expanded && hasSettings && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={transitions.standard}
            className="overflow-hidden"
          >
            <div className="border-t border-[var(--border)] p-3">
              <FieldGroup fields={spec?.fields ?? []} values={step.config} onChange={onChangeConfig} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
