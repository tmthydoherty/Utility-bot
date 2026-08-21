"use client";

import * as React from "react";
import { AnimatePresence, motion } from "motion/react";
import { ChevronDown } from "lucide-react";

import { cn, formatRelative } from "@/lib/utils";
import { transitions } from "@/lib/motion";
import { Card } from "@/components/ui/card";
import type { RunRecord } from "@/lib/automations/types";

/**
 * What actually happened, and why.
 *
 * This is the answer to "why didn't it fire?", which is the single most common
 * complaint about every automation system there is, and the answer is almost
 * always one check that did not match. The engine records a trace of every
 * decision it made — including for runs where nothing happened — precisely so
 * this screen can exist.
 *
 * So a skipped run is shown, not hidden. A history that only lists successes
 * cannot answer the question anyone is actually asking.
 */

const OUTCOMES: Record<RunRecord["outcome"], { label: string; dot: string; text: string }> = {
  fired: { label: "Ran", dot: "bg-[var(--success)]", text: "text-[var(--success)]" },
  dry_run: { label: "Test only", dot: "bg-[var(--warning)]", text: "text-[var(--warning)]" },
  skipped: { label: "Didn't match", dot: "bg-[var(--fg-subtle)]", text: "text-fg-subtle" },
  deferred: { label: "Waiting", dot: "bg-[var(--accent)]", text: "text-[var(--accent)]" },
  error: { label: "Problem", dot: "bg-[var(--danger)]", text: "text-[var(--danger)]" },
};

export function RunHistory({
  runs,
  enabled,
  dryRun,
}: {
  runs: RunRecord[];
  enabled: boolean;
  dryRun: boolean;
}) {
  return (
    <Card className="mt-6 p-5 sm:p-6">
      <div className="mb-4">
        <h2 className="font-semibold tracking-tight">Recent activity</h2>
        <p className="mt-0.5 text-sm text-fg-muted">
          Every time this was set off, including the times it decided to do nothing.
        </p>
      </div>

      {runs.length === 0 ? (
        <p className="rounded-lg border border-dashed border-[var(--border)] px-4 py-8 text-center text-sm text-fg-subtle">
          {!enabled
            ? "Nothing yet — this automation is switched off, so it isn't watching for anything."
            : dryRun
              ? "Nothing yet. It's in test mode and watching; activity shows up here the first time its trigger happens."
              : "Nothing yet. Activity shows up here the first time its trigger happens."}
        </p>
      ) : (
        <ul className="space-y-1.5">
          {runs.map((run) => (
            <RunRow key={run.id} run={run} />
          ))}
        </ul>
      )}
    </Card>
  );
}

function RunRow({ run }: { run: RunRecord }) {
  const [open, setOpen] = React.useState(false);
  const outcome = OUTCOMES[run.outcome] ?? OUTCOMES.skipped;
  const hasTrace = run.trace.length > 0;

  return (
    <li className="rounded-lg border border-[var(--border)]">
      <button
        type="button"
        onClick={() => hasTrace && setOpen((current) => !current)}
        aria-expanded={hasTrace ? open : undefined}
        disabled={!hasTrace}
        className={cn(
          "flex w-full items-center gap-3 px-3 py-2.5 text-left",
          hasTrace && "transition-colors hover:bg-[var(--surface-hover)]",
        )}
      >
        <span className={cn("size-1.5 shrink-0 rounded-full", outcome.dot)} aria-hidden />
        <span className={cn("shrink-0 text-xs font-medium", outcome.text)}>{outcome.label}</span>
        <span className="min-w-0 flex-1 truncate text-sm text-fg-muted">{run.summary || "—"}</span>
        <span className="shrink-0 text-xs text-fg-subtle">{formatRelative(run.at * 1000)}</span>
        {hasTrace && (
          <ChevronDown
            className={cn("size-4 shrink-0 text-fg-subtle transition-transform", open && "rotate-180")}
            aria-hidden
          />
        )}
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
            <ol className="space-y-1 border-t border-[var(--border)] px-3 py-2.5">
              {run.trace.map((entry, index) => (
                <li key={index} className="flex items-start gap-2 text-xs">
                  <span className="mt-1 shrink-0" aria-hidden>
                    {entry.result === true ? "✓" : entry.result === false ? "✗" : "·"}
                  </span>
                  <span className="min-w-0">
                    <span
                      className={cn(
                        entry.result === false ? "text-fg-subtle" : "text-fg-muted",
                      )}
                    >
                      {entry.label}
                    </span>
                    {entry.detail && (
                      <span className="block text-fg-subtle">{entry.detail}</span>
                    )}
                  </span>
                </li>
              ))}
              {run.durationMs > 0 && (
                <li className="pt-1 text-xs text-fg-subtle">Took {run.durationMs}ms</li>
              )}
            </ol>
          </motion.div>
        )}
      </AnimatePresence>
    </li>
  );
}
