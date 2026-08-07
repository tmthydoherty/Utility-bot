"use client";

import { AnimatePresence, motion } from "motion/react";
import { RotateCcw, TriangleAlert } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";
import { Button } from "./button";

/**
 * The unsaved-changes bar.
 *
 * Appears only when the form is dirty, sits above the mobile tab bar, and
 * clears the home indicator. It exists because the alternative — a Save button
 * pinned at the bottom of a long settings page — means scrolling past twenty
 * fields to commit a change to the first one.
 *
 * It also carries the error state: a save that fails silently is the single
 * worst outcome for a settings screen, so a failure keeps the bar on screen,
 * keeps the changes, and says so.
 */
export function SaveBar({
  visible,
  saving,
  error,
  changeCount,
  onSave,
  onReset,
}: {
  visible: boolean;
  saving?: boolean;
  error?: string | null;
  changeCount: number;
  onSave: () => void;
  onReset: () => void;
}) {
  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ y: 100, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: 100, opacity: 0 }}
          transition={SPRING}
          role="region"
          aria-label="Unsaved changes"
          className={cn(
            "fixed inset-x-0 z-40 px-3",
            // Clears the mobile tab bar; on desktop there is no tab bar to clear.
            "bottom-[calc(env(safe-area-inset-bottom)+4.75rem)] lg:bottom-4",
            "lg:left-[var(--sidebar-width,17rem)] lg:px-6",
          )}
        >
          <div
            className={cn(
              "glass glass-highlight mx-auto flex max-w-3xl items-center gap-3 rounded-xl p-3 shadow-[var(--elev-4)]",
            )}
          >
            <div className="min-w-0 flex-1">
              {error ? (
                <p className="flex items-center gap-2 text-sm font-medium text-[var(--danger)]">
                  <TriangleAlert className="size-4 shrink-0" aria-hidden />
                  <span className="truncate">{error}</span>
                </p>
              ) : (
                <p className="truncate text-sm">
                  <span className="font-medium">
                    {changeCount} unsaved {changeCount === 1 ? "change" : "changes"}
                  </span>
                  <span className="ml-2 hidden text-fg-muted sm:inline">
                    Nothing is applied until you save.
                  </span>
                </p>
              )}
            </div>

            <Button
              variant="ghost"
              size="sm"
              onClick={onReset}
              disabled={saving}
              aria-label="Discard changes"
            >
              <RotateCcw aria-hidden />
              <span className="hidden sm:inline">Discard</span>
            </Button>
            <Button variant="primary" size="sm" onClick={onSave} loading={saving}>
              Save changes
            </Button>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
