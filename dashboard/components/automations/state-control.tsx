"use client";

import * as React from "react";
import { motion } from "motion/react";
import { Eye, Play, Power } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";
import { STATE_BLURBS, type AutomationState } from "@/lib/automations/types";

/**
 * Off · Test mode · Running, as one control.
 *
 * The bot stores this as two separate booleans (`enabled` and `dry_run`), and
 * the Discord panel exposes them as two separate buttons. That is four
 * combinations for three real states, and one of the four — off but not in
 * test mode — means nothing at all. Anyone who has to reason about which
 * checkbox does what is being asked to hold the implementation in their head.
 *
 * So it is one slider with three positions, and the middle one is explained
 * every time rather than being a word you have to already know. Test mode is
 * the whole reason someone can try this without being frightened of it: it
 * watches, decides, and writes down what it *would* have done, and touches
 * nothing.
 */

const STATES: { value: AutomationState; label: string; icon: typeof Power }[] = [
  { value: "off", label: "Off", icon: Power },
  { value: "testing", label: "Test", icon: Eye },
  { value: "live", label: "On", icon: Play },
];

export function StateControl({
  value,
  onChange,
  disabled,
  blockedReason,
  size = "md",
  className,
}: {
  value: AutomationState;
  onChange: (next: AutomationState) => void;
  disabled?: boolean;
  /** Why Test and On can't be chosen yet — shown instead of silently failing. */
  blockedReason?: string;
  size?: "sm" | "md";
  className?: string;
}) {
  const groupId = React.useId();

  return (
    <div className={cn("space-y-2", className)}>
      <div
        role="radiogroup"
        aria-label="Whether this automation runs"
        className={cn(
          "inline-flex rounded-lg border border-[var(--border)] bg-[var(--surface)] p-1",
          disabled && "opacity-60",
        )}
      >
        {STATES.map((state) => {
          const active = state.value === value;
          // A blocked automation can always be switched *off* — the only
          // direction that is never unsafe.
          const locked = Boolean(blockedReason) && state.value !== "off";
          const Glyph = state.icon;

          return (
            <button
              key={state.value}
              type="button"
              role="radio"
              aria-checked={active}
              disabled={disabled || locked}
              onClick={() => onChange(state.value)}
              title={locked ? blockedReason : undefined}
              className={cn(
                "relative flex items-center gap-1.5 rounded-md font-medium transition-colors",
                size === "sm" ? "h-7 px-2.5 text-xs" : "h-9 px-3.5 text-sm",
                active ? "text-fg" : "text-fg-subtle hover:text-fg-muted",
                locked && "cursor-not-allowed opacity-40 hover:text-fg-subtle",
              )}
            >
              {active && (
                <motion.span
                  layoutId={`${groupId}-active`}
                  transition={SPRING}
                  className={cn(
                    "absolute inset-0 rounded-md",
                    state.value === "live" && "bg-[var(--success-soft)]",
                    state.value === "testing" && "bg-[var(--warning-soft)]",
                    state.value === "off" && "bg-[var(--surface-active)]",
                  )}
                />
              )}
              <Glyph
                className={cn(
                  "relative size-3.5",
                  active && state.value === "live" && "text-[var(--success)]",
                  active && state.value === "testing" && "text-[var(--warning)]",
                )}
                aria-hidden
              />
              <span className="relative">{state.label}</span>
            </button>
          );
        })}
      </div>

      <p className="text-xs text-fg-muted">
        {blockedReason ? (
          <span className="text-[var(--warning)]">{blockedReason}</span>
        ) : (
          STATE_BLURBS[value]
        )}
      </p>
    </div>
  );
}

/** The same three states as a read-only pill, for cards and lists. */
export function StatePill({ state }: { state: AutomationState }) {
  const config = {
    live: { label: "Running", className: "bg-[var(--success-soft)] text-[var(--success)]" },
    testing: { label: "Test mode", className: "bg-[var(--warning-soft)] text-[var(--warning)]" },
    off: { label: "Off", className: "bg-[var(--surface-active)] text-fg-subtle" },
  }[state];

  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium",
        config.className,
      )}
    >
      <span
        className={cn(
          "size-1.5 rounded-full",
          state === "live" && "bg-[var(--success)]",
          state === "testing" && "bg-[var(--warning)]",
          state === "off" && "bg-[var(--fg-subtle)]",
        )}
        aria-hidden
      />
      {config.label}
    </span>
  );
}
