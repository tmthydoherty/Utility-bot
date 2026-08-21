"use client";

import * as React from "react";
import { AnimatePresence, motion } from "motion/react";
import { ChevronDown, SlidersHorizontal } from "lucide-react";

import { cn, formatDuration } from "@/lib/utils";
import { transitions } from "@/lib/motion";
import { Card } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { EntityPicker } from "@/components/ui/entity-picker";
import type { CooldownScope } from "@/lib/automations/types";

/**
 * The settings almost nobody needs, written so that the person who does need
 * one can understand it without a manual.
 *
 * These are the four the engine calls `cooldown_s`, `cooldown_scope`,
 * `priority`, `stop_after` and `allow_bots`, and the names are the problem:
 * every one of them is a word from the implementation. Someone who wants "stop
 * it replying twenty times in a row" does not search for "cooldown scope".
 *
 * So each one is stated as the problem it solves, in a sentence, with the
 * consequence spelled out — and the whole block starts collapsed with an
 * honest label on it. "Most people never need these" is not modesty; it is the
 * single most useful thing the section can say, because it tells someone
 * skimming that they can stop reading.
 */

export interface AdvancedValues {
  cooldownSeconds: number;
  cooldownScope: CooldownScope;
  priority: number;
  stopAfter: boolean;
  allowBots: boolean;
}

const COOLDOWN_PRESETS = [
  { value: 0, label: "No limit" },
  { value: 10, label: "Once every 10 seconds" },
  { value: 60, label: "Once a minute" },
  { value: 300, label: "Once every 5 minutes" },
  { value: 3600, label: "Once an hour" },
  { value: 86400, label: "Once a day" },
];

const SCOPES: { value: CooldownScope; label: string; plain: string }[] = [
  { value: "user", label: "Each person", plain: "Ash and Sam each get their own limit." },
  { value: "channel", label: "Each channel", plain: "#general and #chat each get their own limit." },
  { value: "guild", label: "The whole server", plain: "One shared limit for everyone." },
];

export function AdvancedSection({
  values,
  onChange,
}: {
  values: AdvancedValues;
  onChange: (next: AdvancedValues) => void;
}) {
  const [open, setOpen] = React.useState(false);

  const set = <K extends keyof AdvancedValues>(key: K, value: AdvancedValues[K]) =>
    onChange({ ...values, [key]: value });

  // Anything away from the default is worth surfacing on the collapsed header,
  // so a setting changed six months ago is not invisible behind a chevron.
  const changed: string[] = [];
  if (values.cooldownSeconds > 0) changed.push(`runs at most ${cooldownLabel(values.cooldownSeconds)}`);
  if (values.stopAfter) changed.push("stops other automations");
  if (values.allowBots) changed.push("reacts to bots");
  if (values.priority !== 100) changed.push(`order ${values.priority}`);

  return (
    <Card className="min-w-0 overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-expanded={open}
        className="flex w-full items-center gap-3 p-5 text-left transition-colors hover:bg-[var(--surface-hover)] sm:p-6"
      >
        <span
          className="grid size-7 shrink-0 place-items-center rounded-full bg-[var(--surface-active)] text-fg-muted"
          aria-hidden
        >
          <SlidersHorizontal className="size-3.5" />
        </span>
        <span className="min-w-0 flex-1">
          <span className="block font-semibold tracking-tight">Fine tuning</span>
          <span className="mt-0.5 block text-sm text-fg-muted">
            {changed.length > 0 ? changed.join(" · ") : "Most people never need these."}
          </span>
        </span>
        <ChevronDown
          className={cn("size-4 shrink-0 text-fg-subtle transition-transform", open && "rotate-180")}
          aria-hidden
        />
      </button>

      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={transitions.standard}
            className="overflow-hidden"
          >
            <div className="space-y-6 border-t border-[var(--border)] p-5 sm:p-6">
              <Setting
                title="Don't let it run too often"
                plain="Useful when an automation replies to messages and you don't want a wall of them."
              >
                <div className="grid gap-3 sm:grid-cols-2">
                  <EntityPicker
                    items={COOLDOWN_PRESETS.map((preset) => ({
                      value: String(preset.value),
                      label: preset.label,
                    }))}
                    value={String(nearestPreset(values.cooldownSeconds))}
                    onChange={(next) => set("cooldownSeconds", Number(next ?? 0))}
                    clearable={false}
                    aria-label="How often at most"
                  />
                  {values.cooldownSeconds > 0 && (
                    <EntityPicker
                      items={SCOPES.map((scope) => ({
                        value: scope.value,
                        label: scope.label,
                        description: scope.plain,
                      }))}
                      value={values.cooldownScope}
                      onChange={(next) => set("cooldownScope", (next as CooldownScope) ?? "user")}
                      clearable={false}
                      aria-label="Counted separately for"
                    />
                  )}
                </div>
                {values.cooldownSeconds > 0 && (
                  <p className="mt-2 text-xs text-fg-subtle">
                    So this runs at most once every {formatDuration(values.cooldownSeconds)}, counted
                    separately for{" "}
                    {SCOPES.find((s) => s.value === values.cooldownScope)?.label.toLowerCase()}.
                  </p>
                )}
              </Setting>

              <Setting
                title="Stop other automations after this one"
                plain="If two automations react to the same thing, this one wins and the rest are skipped."
              >
                <Toggle
                  checked={values.stopAfter}
                  onChange={(next) => set("stopAfter", next)}
                  label="Skip the others"
                />
              </Setting>

              <Setting
                title="React to other bots as well"
                plain="Off by default, and that default exists for a reason: two bots answering each other never stops on its own."
              >
                <Toggle
                  checked={values.allowBots}
                  onChange={(next) => set("allowBots", next)}
                  label="Include messages from bots"
                />
              </Setting>

              <Setting
                title="Run order"
                plain="Only matters when several automations react to the same thing. Lower numbers go first; leave it at 100 unless you have a reason."
              >
                <div className="flex items-center gap-3">
                  <input
                    type="range"
                    min={1}
                    max={200}
                    value={values.priority}
                    onChange={(event) => set("priority", Number(event.target.value))}
                    className="h-1.5 flex-1 cursor-pointer appearance-none rounded-full bg-[var(--bg-inset)] accent-[var(--accent)]"
                    aria-label="Run order"
                  />
                  <span className="w-10 shrink-0 text-right text-sm [font-variant-numeric:tabular-nums]">
                    {values.priority}
                  </span>
                </div>
                <p className="mt-1.5 text-xs text-fg-subtle">
                  {values.priority < 100
                    ? "Runs before the usual ones."
                    : values.priority > 100
                      ? "Runs after the usual ones."
                      : "The usual place in the queue."}
                </p>
              </Setting>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </Card>
  );
}

function Setting({
  title,
  plain,
  children,
}: {
  title: string;
  plain: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2">
      <div>
        <p className="text-sm font-medium">{title}</p>
        <p className="mt-0.5 text-xs leading-relaxed text-fg-muted">{plain}</p>
      </div>
      {children}
    </div>
  );
}

function Toggle({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  label: string;
}) {
  const id = React.useId();
  return (
    <div className="flex items-center gap-3">
      <Switch id={id} checked={checked} onCheckedChange={onChange} />
      <label htmlFor={id} className="text-sm text-fg-muted">
        {label}
      </label>
    </div>
  );
}

/** Snaps a stored value to the nearest preset, so a hand-set 45s still shows. */
function nearestPreset(seconds: number): number {
  if (seconds <= 0) return 0;
  return COOLDOWN_PRESETS.reduce((best, preset) =>
    Math.abs(preset.value - seconds) < Math.abs(best.value - seconds) ? preset : best,
  ).value;
}

function cooldownLabel(seconds: number): string {
  return COOLDOWN_PRESETS.find((preset) => preset.value === nearestPreset(seconds))?.label
    .toLowerCase()
    .replace("once ", "once ") ?? formatDuration(seconds);
}
