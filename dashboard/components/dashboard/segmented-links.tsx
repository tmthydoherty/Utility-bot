import Link from "next/link";

import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/icon";

/**
 * A segmented control built from links, not buttons.
 *
 * The tracker's mode tabs and time windows are navigation — each is a distinct
 * URL the server renders — so they're `<Link>`s. That means they work as an
 * SSR'd control with no client JavaScript, are middle-clickable and
 * shareable, and the active one is decided by the current params rather than by
 * component state that could disagree with the page.
 */

export interface SegmentOption {
  label: string;
  href: string;
  active: boolean;
  icon?: string;
}

/** Fixed grid-column counts, listed statically so Tailwind keeps the classes. */
const GRID_COLS: Record<number, string> = {
  2: "grid-cols-2",
  3: "grid-cols-3",
  4: "grid-cols-4",
};

export function SegmentedLinks({
  options,
  size = "md",
  wrap = false,
  columns,
  "aria-label": ariaLabel,
}: {
  options: SegmentOption[];
  size?: "sm" | "md";
  /**
   * Wrap onto a second row instead of scrolling. Use it for the primary mode
   * tabs, where a horizontally-scrolling strip hides options off the right edge
   * with no affordance; wrapping keeps every choice visible on a narrow screen.
   */
  wrap?: boolean;
  /**
   * Lay the options out as an even grid of this many columns on narrow screens
   * (so six tabs are two tidy rows of three rather than an uneven wrap), then
   * collapse to a single inline row from `sm` up where they fit. Takes priority
   * over `wrap`.
   */
  columns?: number;
  "aria-label"?: string;
}) {
  const grid = columns != null;
  return (
    <div
      role="tablist"
      aria-label={ariaLabel}
      className={cn(
        "glass max-w-full items-center gap-1 rounded-lg p-1",
        grid
          ? cn("grid w-full sm:inline-flex sm:w-auto sm:flex-wrap", GRID_COLS[columns] ?? "grid-cols-3")
          : cn(
              "inline-flex",
              wrap ? "flex-wrap" : "overflow-x-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden",
            ),
        // A fixed height only makes sense for a single row.
        grid || wrap ? "min-h-11" : size === "md" ? "h-11" : "h-9",
      )}
    >
      {options.map((option) => (
        <Link
          key={option.href}
          href={option.href}
          role="tab"
          aria-selected={option.active}
          scroll={false}
          className={cn(
            "relative inline-flex items-center justify-center gap-2 rounded-md font-medium transition-colors",
            grid ? "w-full sm:w-auto sm:shrink-0" : "shrink-0",
            size === "md" ? "h-9 px-3.5 text-sm" : "h-7 px-3 text-xs",
            "[&_svg]:size-4",
            option.active
              ? "bg-[var(--surface-active)] text-fg shadow-[var(--elev-1)]"
              : "text-fg-muted hover:text-fg",
          )}
        >
          {option.icon && <Icon name={option.icon} className="shrink-0" />}
          {option.label}
        </Link>
      ))}
    </div>
  );
}
