"use client";

import * as React from "react";
import { motion, AnimatePresence } from "motion/react";
import { ArrowLeft, ArrowRight, PartyPopper } from "lucide-react";

import { cn } from "@/lib/utils";
import { transitions } from "@/lib/motion";
import { Button } from "@/components/ui/button";
import { Sheet } from "@/components/ui/sheet";
import type { Field, SettingsValues } from "@/lib/schema/types";
import { blanks, type Blank } from "@/lib/automations/readiness";
import { TEMPLATES_BY_KEY, type Ask } from "@/lib/automations/templates";
import { ACTIONS, CONDITIONS, TRIGGERS } from "@/lib/automations/registry";
import { findStep, isGroup, findCondition, type Automation } from "@/lib/automations/types";

import { AutomationSentence } from "./sentence";
import { FieldGroup } from "./field-group";

/**
 * The setup flow: one question at a time, until nothing is blank.
 *
 * The builder page shows everything at once, which is right for editing
 * something you already understand and wrong for the thirty seconds after you
 * pick a ready-made automation. At that moment three cards of settings is a
 * wall, and the only thing that actually needs doing is answering two
 * questions.
 *
 * So this asks them, one per screen, in the order they occur in the sentence —
 * and shows the sentence updating underneath as each answer lands, which is
 * what makes the connection between "the box I just filled in" and "what the
 * bot will do".
 *
 * Two kinds of question, in this order:
 *
 *   required — derived by `readiness`, and the automation cannot run without
 *              them. Not skippable.
 *   suggested — the template's own `asks`: settings that already have a
 *              working default but that the template's author knows are worth
 *              a decision. Always skippable.
 */

interface Question {
  id: string;
  prompt: string;
  /** Why this matters, when there is something worth saying. */
  note: string;
  field: Field;
  where: Blank["where"];
  path: number[];
  optional: boolean;
  owner: string;
}

/** Reads the config a question's answer lives in, out of the automation. */
function configAt(
  automation: Automation,
  where: Question["where"],
  path: number[],
): SettingsValues | null {
  if (where === "trigger") return automation.triggerConfig;
  if (where === "condition") {
    const node = findCondition(automation.conditions, path);
    if (!node || isGroup(node)) return null;
    return node.config;
  }
  const step = findStep(automation.steps, path);
  if (!step || step.type === "if") return null;
  return step.config;
}

/** Writes an answer back, returning a new automation rather than mutating. */
function withConfigAt(
  automation: Automation,
  where: Question["where"],
  path: number[],
  config: SettingsValues,
): Automation {
  if (where === "trigger") return { ...automation, triggerConfig: config };

  if (where === "condition") {
    // Only one level deep is reachable from the web builder, which is also the
    // only depth a template's asks address.
    const index = path[0];
    if (index === undefined) return automation;
    const items = [...automation.conditions.items];
    const node = items[index];
    if (!node || isGroup(node)) return automation;
    items[index] = { ...node, config };
    return { ...automation, conditions: { ...automation.conditions, items } };
  }

  // Steps can be nested inside a split, so the path is walked and the tree
  // rebuilt along the way. Copying rather than mutating matters here: the
  // builder holds the previous automation in state and compares against it to
  // decide whether Save should light up.
  const replace = (steps: Automation["steps"], remaining: number[]): Automation["steps"] => {
    const index = remaining[0];
    if (index === undefined || index < 0 || index >= steps.length) return steps;
    const next = [...steps];
    const step = next[index]!;

    if (remaining.length === 1) {
      next[index] = { ...step, config };
      return next;
    }
    const side = remaining[1];
    const rest = remaining.slice(2);
    if (side === 0) next[index] = { ...step, then: replace(step.then ?? [], rest) };
    else next[index] = { ...step, otherwise: replace(step.otherwise ?? [], rest) };
    return next;
  };

  return { ...automation, steps: replace(automation.steps, path) };
}

function specFor(where: Question["where"], automation: Automation, path: number[]) {
  if (where === "trigger") return TRIGGERS[automation.triggerType];
  if (where === "condition") {
    const node = findCondition(automation.conditions, path);
    return node && !isGroup(node) ? CONDITIONS[node.type] : undefined;
  }
  const step = findStep(automation.steps, path);
  return step ? ACTIONS[step.type] : undefined;
}

function buildQuestions(automation: Automation): Question[] {
  const required: Question[] = blanks(automation).map((blank) => ({
    id: blank.ref,
    // The field's own label already reads as a question most of the time —
    // "Which channel", "Which role" — so it is used as-is rather than wrapped
    // in a template that would make it read like a form letter.
    prompt: blank.field.label,
    note: blank.note,
    field: blank.field,
    where: blank.where,
    path: blank.path,
    optional: false,
    owner: blank.owner,
  }));

  const template = automation.templateKey ? TEMPLATES_BY_KEY[automation.templateKey] : undefined;
  const alreadyAsked = new Set(required.map((question) => question.id));

  const suggested: Question[] = [];
  for (const ask of (template?.asks ?? []) as Ask[]) {
    const spec = specFor(ask.where, automation, ask.path);
    const field = (spec?.fields ?? []).find((candidate) => candidate.key === ask.field);
    if (!field) continue;

    const id = `${ask.where}:${ask.path.join(".")}:${ask.field}`;
    // Being asked the same question twice, once as a requirement and once as a
    // suggestion, is how a checklist starts getting skimmed.
    if (alreadyAsked.has(id)) continue;

    suggested.push({
      id,
      prompt: ask.prompt,
      note: "",
      field,
      where: ask.where,
      path: ask.path,
      optional: true,
      owner: spec?.label ?? "",
    });
  }

  return [...required, ...suggested];
}

export function SetupFlow({
  open,
  onOpenChange,
  automation,
  onChange,
  onFinish,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  automation: Automation;
  onChange: (next: Automation) => void;
  onFinish: () => void;
}) {
  const [index, setIndex] = React.useState(0);

  // Frozen when the flow opens. Re-deriving on every answer would renumber the
  // questions underneath the person answering them — fill in a channel and
  // "question 2 of 4" silently becomes "question 2 of 3", which reads as a bug
  // even though it is technically more accurate.
  const [questions, setQuestions] = React.useState<Question[]>([]);

  React.useEffect(() => {
    if (!open) return;
    setQuestions(buildQuestions(automation));
    setIndex(0);
    // Deliberately keyed on `open` alone — see above.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const current = questions[index];
  const done = index >= questions.length;

  const answer = (config: SettingsValues) => {
    if (!current) return;
    onChange(withConfigAt(automation, current.where, current.path, config));
  };

  const finish = () => {
    onOpenChange(false);
    onFinish();
  };

  return (
    <Sheet
      open={open}
      onOpenChange={onOpenChange}
      title={done ? "All done" : "Let's finish this off"}
      description={
        done
          ? undefined
          : questions.length > 0
            ? `Question ${index + 1} of ${questions.length}`
            : undefined
      }
      size="lg"
    >
      <div className="space-y-5">
        {questions.length > 0 && !done && (
          <div className="flex gap-1" aria-hidden>
            {questions.map((question, position) => (
              <span
                key={question.id}
                className={cn(
                  "h-1 flex-1 rounded-full transition-colors",
                  position < index
                    ? "bg-[var(--success)]"
                    : position === index
                      ? "bg-[var(--accent)]"
                      : "bg-[var(--bg-inset)]",
                )}
              />
            ))}
          </div>
        )}

        <AnimatePresence mode="wait" initial={false}>
          {done ? (
            <motion.div
              key="done"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={transitions.standard}
              className="space-y-4 text-center"
            >
              <span
                className="mx-auto grid size-12 place-items-center rounded-full bg-[var(--success-soft)] text-[var(--success)]"
                aria-hidden
              >
                <PartyPopper className="size-5" />
              </span>
              <div className="space-y-1">
                <p className="font-medium">That&apos;s everything</p>
                <p className="text-sm text-fg-muted">
                  Here&apos;s what it will do. If that reads right, save it — then put it in test
                  mode for a bit before switching it on properly.
                </p>
              </div>
              <div className="rounded-lg border border-[var(--border)] bg-[var(--bg-inset)] p-4 text-left">
                <AutomationSentence automation={automation} size="sm" />
              </div>
              <Button onClick={finish} className="w-full justify-center">
                Save it
              </Button>
            </motion.div>
          ) : current ? (
            <motion.div
              key={current.id}
              initial={{ opacity: 0, x: 12 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -12 }}
              transition={transitions.standard}
              className="space-y-4"
            >
              <div className="space-y-1">
                <h3 className="text-lg font-semibold tracking-tight text-balance">
                  {current.prompt}
                </h3>
                {current.owner && (
                  <p className="text-xs text-fg-subtle">
                    for “{current.owner}”
                    {current.optional && " · you can skip this"}
                  </p>
                )}
                {current.note && (
                  <p className="text-sm text-[var(--warning)]">{stripMarkdown(current.note)}</p>
                )}
              </div>

              <FieldGroup
                fields={[{ ...current.field, wide: true }]}
                values={configAt(automation, current.where, current.path) ?? {}}
                onChange={answer}
              />

              <div className="rounded-lg border border-[var(--border)] bg-[var(--bg-inset)] p-3">
                <p className="mb-1 text-[11px] font-medium uppercase tracking-wide text-fg-subtle">
                  So far
                </p>
                <AutomationSentence automation={automation} size="sm" />
              </div>

              <div className="flex items-center gap-2">
                {index > 0 && (
                  <Button variant="ghost" onClick={() => setIndex(index - 1)}>
                    <ArrowLeft aria-hidden />
                    Back
                  </Button>
                )}
                {current.optional && (
                  <Button variant="ghost" onClick={() => setIndex(index + 1)}>
                    Skip
                  </Button>
                )}
                <Button onClick={() => setIndex(index + 1)} className="ml-auto">
                  {index === questions.length - 1 ? "Finish" : "Next"}
                  <ArrowRight aria-hidden />
                </Button>
              </div>
            </motion.div>
          ) : (
            <motion.div
              key="empty"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="space-y-4 text-center"
            >
              <p className="text-sm text-fg-muted">
                Nothing left to fill in — this one is ready to go.
              </p>
              <Button onClick={() => onOpenChange(false)} className="w-full justify-center">
                Close
              </Button>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </Sheet>
  );
}

/** The readiness notes carry **bold** markers for the Discord embeds. */
function stripMarkdown(text: string): string {
  return text.replace(/\*\*/g, "");
}
