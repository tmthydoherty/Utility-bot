import { ArrowDown, ArrowUp } from "lucide-react";

import { cn, formatCompact } from "@/lib/utils";
import { Card } from "./card";
import { Icon } from "./icon";
import { Sparkline } from "./sparkline";

/**
 * Stat tile: label, value, optional delta, optional trend.
 *
 * The value uses the font's proportional figures rather than tabular ones —
 * tabular gives every digit the width of a zero, which looks visibly loose at
 * display sizes. Tabular is for columns of numbers that have to line up, which
 * this is not.
 */
export function StatTile({
  label,
  value,
  unit,
  icon,
  delta,
  deltaPeriod = "vs last week",
  /** Whether a rise is good. Members up is good; infractions up is not. */
  upIsGood = true,
  trend,
  className,
}: {
  label: string;
  value: number | string;
  unit?: string;
  icon?: string;
  delta?: number;
  deltaPeriod?: string;
  upIsGood?: boolean;
  trend?: number[];
  className?: string;
}) {
  const display = typeof value === "number" ? formatCompact(value) : value;

  const hasDelta = typeof delta === "number" && delta !== 0;
  const rising = (delta ?? 0) > 0;
  // Direction alone does not decide the colour — "good" does.
  const isGood = rising === upIsGood;
  const DeltaIcon = rising ? ArrowUp : ArrowDown;

  return (
    <Card className={cn("p-5", className)}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 space-y-1">
          <p className="truncate text-sm text-fg-muted">{label}</p>
          <p className="text-3xl font-semibold leading-none tracking-tight">
            {display}
            {unit && <span className="ml-1 text-base font-medium text-fg-muted">{unit}</span>}
          </p>
        </div>
        {icon && (
          <div className="grid size-9 shrink-0 place-items-center rounded-md bg-[var(--accent-soft)] text-[var(--accent)]">
            <Icon name={icon} className="size-[18px]" />
          </div>
        )}
      </div>

      {(hasDelta || trend) && (
        <div className="mt-4 flex items-end justify-between gap-3">
          {hasDelta ? (
            <div className="flex min-w-0 items-center gap-1.5 text-xs">
              <span
                className={cn(
                  "inline-flex items-center gap-0.5 font-medium",
                  isGood ? "text-[var(--success)]" : "text-[var(--danger)]",
                )}
              >
                <DeltaIcon className="size-3" aria-hidden />
                {Math.abs(delta!).toFixed(1)}%
              </span>
              <span className="truncate text-fg-subtle">{deltaPeriod}</span>
            </div>
          ) : (
            <span />
          )}
          {trend && <Sparkline data={trend} label={`${label} trend`} />}
        </div>
      )}
    </Card>
  );
}
