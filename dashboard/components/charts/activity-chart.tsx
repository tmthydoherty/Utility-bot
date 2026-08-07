"use client";

import * as React from "react";

import { cn, formatNumber } from "@/lib/utils";

/**
 * Single-series area chart with a crosshair and tooltip.
 *
 * One series, so no legend — the title above it says what is plotted, and a
 * one-swatch legend would only restate it. Values are carried by the y-axis
 * ticks and the tooltip rather than by a label on every point.
 *
 * Hand-drawn SVG rather than a charting library on purpose: this is one form,
 * it has to be themeable from CSS variables so it follows light/dark without a
 * re-render, and it must not add ~50KB to the first dashboard page a phone
 * loads over mobile data.
 */

export interface ActivityPoint {
  label: string;
  value: number;
}

const PAD = { top: 12, right: 12, bottom: 24, left: 44 };

export function ActivityChart({
  data,
  height = 200,
  className,
  valueLabel = "messages",
}: {
  data: ActivityPoint[];
  height?: number;
  className?: string;
  valueLabel?: string;
}) {
  const [hover, setHover] = React.useState<number | null>(null);
  const [width, setWidth] = React.useState(720);
  const wrapRef = React.useRef<HTMLDivElement>(null);
  const gradientId = React.useId();

  // Measured rather than viewBox-scaled: a fixed viewBox would stretch the
  // 2px stroke and the tick text along with the plot on a wide screen.
  React.useEffect(() => {
    const element = wrapRef.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry) setWidth(entry.contentRect.width);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const innerW = Math.max(1, width - PAD.left - PAD.right);
  const innerH = Math.max(1, height - PAD.top - PAD.bottom);

  const values = data.map((d) => d.value);
  const max = Math.max(...values, 1);
  // Baseline at zero, and a top rounded up to a clean number so the ticks read
  // as 0 / 500 / 1,000 rather than 0 / 437 / 874.
  const step = niceStep(max / 3);
  const top = Math.ceil(max / step) * step;

  const x = (i: number) => PAD.left + (i / Math.max(1, data.length - 1)) * innerW;
  const y = (value: number) => PAD.top + innerH - (value / top) * innerH;

  const linePath = data.map((d, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(d.value)}`).join(" ");
  const areaPath = `${linePath} L${x(data.length - 1)},${PAD.top + innerH} L${x(0)},${PAD.top + innerH} Z`;

  const ticks = Array.from({ length: Math.round(top / step) + 1 }, (_, i) => i * step);

  const pointFromClientX = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return null;
    const ratio = (clientX - rect.left - PAD.left) / innerW;
    const index = Math.round(ratio * (data.length - 1));
    return Math.min(data.length - 1, Math.max(0, index));
  };

  const active = hover !== null ? data[hover] : undefined;

  return (
    <div ref={wrapRef} className={cn("relative w-full select-none", className)}>
      <svg
        width={width}
        height={height}
        // The whole plot is one hit target rather than one per point: chasing
        // a 4px dot with a finger is a losing game, and the crosshair snaps to
        // the nearest column anyway.
        onPointerMove={(event) => setHover(pointFromClientX(event.clientX))}
        onPointerLeave={() => setHover(null)}
        role="img"
        aria-label={`${valueLabel} over the last ${data.length} days`}
        className="touch-pan-y"
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            {/* A wash, not a block — ~10% at the top, fading out entirely. */}
            <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.22" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
          </linearGradient>
        </defs>

        {/* Gridlines: hairline, solid, one step off the surface. Never dashed. */}
        {ticks.map((tick) => (
          <g key={tick}>
            <line
              x1={PAD.left}
              x2={width - PAD.right}
              y1={y(tick)}
              y2={y(tick)}
              stroke="var(--border)"
              strokeWidth={1}
            />
            <text
              x={PAD.left - 8}
              y={y(tick)}
              textAnchor="end"
              dominantBaseline="middle"
              className="fill-[var(--fg-subtle)] text-[10px] [font-variant-numeric:tabular-nums]"
            >
              {formatNumber(tick)}
            </text>
          </g>
        ))}

        <path d={areaPath} fill={`url(#${gradientId})`} />
        <path
          d={linePath}
          fill="none"
          stroke="var(--accent)"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {hover !== null && data[hover] && (
          <g>
            <line
              x1={x(hover)}
              x2={x(hover)}
              y1={PAD.top}
              y2={PAD.top + innerH}
              stroke="var(--border-strong)"
              strokeWidth={1}
            />
            <circle
              cx={x(hover)}
              cy={y(data[hover].value)}
              r={5}
              fill="var(--accent)"
              stroke="var(--bg-elevated)"
              strokeWidth={2}
            />
          </g>
        )}

        {/* First and last x labels only. A tick under every column collides on
            a phone, and the tooltip names the exact day anyway. */}
        <text
          x={PAD.left}
          y={height - 6}
          className="fill-[var(--fg-subtle)] text-[10px]"
        >
          {data[0]?.label}
        </text>
        <text
          x={width - PAD.right}
          y={height - 6}
          textAnchor="end"
          className="fill-[var(--fg-subtle)] text-[10px]"
        >
          {data[data.length - 1]?.label}
        </text>
      </svg>

      {active && hover !== null && (
        <div
          className={cn(
            "pointer-events-none absolute z-10 -translate-x-1/2 -translate-y-full",
            "glass rounded-md px-2.5 py-1.5 text-xs shadow-[var(--elev-3)] whitespace-nowrap",
          )}
          style={{
            left: Math.min(Math.max(x(hover), 60), width - 60),
            top: y(active.value) - 10,
          }}
        >
          <span className="text-fg-muted">{active.label}</span>
          <span className="ml-2 font-semibold [font-variant-numeric:tabular-nums]">
            {formatNumber(active.value)}
          </span>
        </div>
      )}
    </div>
  );
}

/** Rounds a raw step up to 1, 2, or 5 times a power of ten. */
function niceStep(raw: number): number {
  const magnitude = 10 ** Math.floor(Math.log10(Math.max(raw, 1)));
  const normalised = raw / magnitude;
  const snapped = normalised <= 1 ? 1 : normalised <= 2 ? 2 : normalised <= 5 ? 5 : 10;
  return snapped * magnitude;
}
