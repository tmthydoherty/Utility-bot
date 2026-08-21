import * as React from "react";

import { cn } from "@/lib/utils";
import { Avatar } from "@/components/ui/avatar";
import { EmojiGlyph } from "./emoji-glyph";

/**
 * A ranked list — the shape every "top N this week" on the dashboard shares.
 *
 * One component behind the levels board, the most-active board, the busiest
 * channels and the top emoji, so a bar, a rank chip and the medal colours are
 * defined once. The visual on the left is caller's choice (avatar, emoji, a
 * channel glyph); everything else — rank, name, subtitle, value, the relative
 * bar — is uniform.
 */

export interface LeaderItem {
  key: string;
  /** Left-hand visual. Omit for a plain rank + name row. */
  visual?: React.ReactNode;
  name: string;
  subtitle?: string;
  /** Formatted for display (e.g. "1,204" or "3h 20m"). */
  value: string;
  /** 0–100, bar width relative to the top row. */
  barPct: number;
  /**
   * Shows this number as the rank instead of the row's position — for a row
   * pinned out of sequence, like the viewer appended below the top ten with
   * their real placement (#47).
   */
  rankLabel?: number;
  /** Tints the row — used to pick the viewer out of the board. */
  highlight?: boolean;
}

/** Gold, silver, bronze for the top three; the rail colour otherwise. */
const RANK_COLOR = [
  "text-[#f0b232]",
  "text-[#c4ccd4]",
  "text-[#cd7f42]",
] as const;

export function LeaderList({
  items,
  emptyLabel = "No activity yet",
  startRank = 0,
}: {
  items: LeaderItem[];
  emptyLabel?: string;
  /**
   * Rank of the first item minus one — for a board split across columns, the
   * right column passes the left column's length so numbering runs 1..N
   * continuously instead of restarting at 1 per column.
   */
  startRank?: number;
}) {
  if (items.length === 0) {
    return <p className="py-6 text-center text-sm text-fg-subtle">{emptyLabel}</p>;
  }

  return (
    <ol className="space-y-1">
      {items.map((item, index) => {
        const rank = startRank + index;
        const pinned = item.rankLabel !== undefined;
        return (
        <li
          key={item.key}
          className={cn(
            "flex items-center gap-3 rounded-md px-1.5 py-1.5",
            item.highlight && "bg-[var(--accent-soft)]",
          )}
        >
          <span
            className={cn(
              "min-w-5 shrink-0 px-0.5 text-center text-sm font-semibold tabular-nums",
              !pinned && rank < 3 ? RANK_COLOR[rank] : "text-fg-subtle",
            )}
          >
            {pinned ? item.rankLabel : rank + 1}
          </span>

          {item.visual && <div className="shrink-0">{item.visual}</div>}

          <div className="min-w-0 flex-1">
            <div className="flex items-baseline justify-between gap-3">
              <span className="truncate text-sm font-medium">{item.name}</span>
              <span className="shrink-0 text-sm font-semibold tabular-nums">
                {item.value}
              </span>
            </div>
            <div className="mt-1.5 flex items-center gap-2">
              <div className="h-1.5 min-w-0 flex-1 overflow-hidden rounded-full bg-[var(--surface-hover)]">
                <div
                  className="h-full rounded-full bg-[var(--accent)]"
                  style={{ width: `${Math.max(item.barPct, 3)}%` }}
                />
              </div>
              {item.subtitle && (
                <span className="shrink-0 text-xs text-fg-subtle">{item.subtitle}</span>
              )}
            </div>
          </div>
        </li>
        );
      })}
    </ol>
  );
}

/** A small circular avatar for a leader row, from a resolved user. */
export function UserVisual({ src, name }: { src?: string | null; name: string }) {
  return <Avatar src={src} name={name} size="sm" />;
}

export { EmojiGlyph };
