"use client";

import * as React from "react";
import { Check, Pencil } from "lucide-react";

import { cn } from "@/lib/utils";
import { Card } from "@/components/ui/card";

/**
 * A settings card that is read-only until an admin unlocks it.
 *
 * The economy holds prices and payouts a stray click shouldn't be able to move,
 * so every section starts locked: its fields render disabled, and the pencil in
 * the header is the deliberate act that turns editing on for that one section.
 * Locking and unlocking is the parent panel's state — this component only shows
 * it and reports the toggle — so several sections never fight over one lock and
 * the floating Save bar still governs what actually gets written.
 */
export function SectionCard({
  title,
  description,
  editing,
  onToggle,
  children,
}: {
  title: string;
  description?: string;
  editing: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  return (
    <Card
      className={cn(
        "overflow-hidden transition-shadow",
        // A quiet accent frame while unlocked, so it's obvious at a glance which
        // section is live without shouting over the rest of the page.
        editing && "ring-1 ring-[var(--accent)]/40",
      )}
    >
      <div className="flex items-start justify-between gap-4 p-5 pb-0 sm:p-6 sm:pb-0">
        <div className="min-w-0 space-y-1">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">
            {title}
          </h2>
          {description && <p className="text-sm text-fg-muted">{description}</p>}
        </div>
        <button
          type="button"
          onClick={onToggle}
          aria-pressed={editing}
          aria-label={editing ? `Finish editing ${title}` : `Edit ${title}`}
          className={cn(
            "inline-flex shrink-0 items-center gap-1.5 rounded-md px-2.5 py-1.5 text-xs font-medium",
            "transition-colors focus:outline-none focus:ring-4 focus:ring-[var(--accent-soft)]",
            editing
              ? "bg-[var(--accent-soft)] text-[var(--accent)]"
              : "text-fg-subtle hover:bg-[var(--surface-hover)] hover:text-fg",
          )}
        >
          {editing ? (
            <>
              <Check className="size-3.5" aria-hidden />
              <span>Done</span>
            </>
          ) : (
            <>
              <Pencil className="size-3.5" aria-hidden />
              <span>Edit</span>
            </>
          )}
        </button>
      </div>
      {children}
    </Card>
  );
}
