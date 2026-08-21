"use client";

import * as React from "react";

import { cn, formatNumber, formatVoice } from "@/lib/utils";

/**
 * Area/line chart with a crosshair, day gridlines and a tooltip.
 *
 * One series by default (the title above names it, so no legend); pass `series`
 * for a head-to-head — each series its own colour, a legend, and lines rather
 * than stacked fills so neither buries the other. Colours are the caller's, and
 * are expected to be a CVD-validated categorical pair (see the dataviz skill and
 * the `--accent` / `--chart-2` tokens).
 *
 * Hand-drawn SVG rather than a charting library on purpose: it has to theme from
 * CSS variables so it follows light/dark without a re-render, and it must not add
 * ~50KB to the first dashboard page a phone loads over mobile data.
 */

export interface ActivityPoint {
  label: string;
  value: number;
}

export interface ChartSeries {
  label: string;
  /** A CSS colour, e.g. "var(--accent)". Carries the series' identity. */
  color: string;
  values: number[];
}

const PAD = { top: 12, right: 12, bottom: 24, left: 44 };

export function ActivityChart({
  data,
  series,
  height = 200,
  className,
  valueLabel = "messages",
  /** How to format tooltip / axis values. "voice" renders durations; the
   *  default is a plain count. A string, not a function, so this stays passable
   *  from a server component to this client chart. */
  format = "number",
}: {
  data: ActivityPoint[];
  series?: ChartSeries[];
  height?: number;
  className?: string;
  valueLabel?: string;
  format?: "number" | "voice";
}) {
  const formatValue = format === "voice" ? formatVoice : formatNumber;
  const [hover, setHover] = React.useState<number | null>(null);
  // Starts at zero, not at a guess — see the note on sideways scroll below.
  const [width, setWidth] = React.useState(0);
  const wrapRef = React.useRef<HTMLDivElement>(null);
  const gradientId = React.useId();

  // Measured rather than viewBox-scaled: a fixed viewBox would stretch the
  // 2px stroke and the tick text along with the plot on a wide screen. And an
  // <svg width="720"> makes its grid track 720px wide with `min-width: auto`, so
  // the whole page scrolled sideways on a phone forever — hence width from zero.
  React.useEffect(() => {
    const element = wrapRef.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry) setWidth(entry.contentRect.width);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  // Normalise to a list of series so single- and multi-series share one path.
  const seriesList: ChartSeries[] = React.useMemo(
    () =>
      series && series.length > 0
        ? series
        : [{ label: valueLabel, color: "var(--accent)", values: data.map((d) => d.value) }],
    [series, data, valueLabel],
  );
  const multi = Boolean(series && series.length > 1);

  const innerW = Math.max(1, width - PAD.left - PAD.right);
  const innerH = Math.max(1, height - PAD.top - PAD.bottom);

  const count = data.length;
  const max = Math.max(1, ...seriesList.flatMap((s) => s.values));
  // Baseline at zero, top rounded to a clean number so ticks read 0/500/1,000.
  const step = niceStep(max / 3);
  const top = Math.ceil(max / step) * step;

  const x = (i: number) => PAD.left + (i / Math.max(1, count - 1)) * innerW;
  const y = (value: number) => PAD.top + innerH - (value / top) * innerH;

  const linePath = (values: number[]) =>
    values.map((v, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(v)}`).join(" ");

  const ticks = Array.from({ length: Math.round(top / step) + 1 }, (_, i) => i * step);

  const pointFromClientX = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return null;
    const ratio = (clientX - rect.left - PAD.left) / innerW;
    const index = Math.round(ratio * (count - 1));
    return Math.min(count - 1, Math.max(0, index));
  };

  const hoverLabel = hover !== null ? data[hover]?.label : undefined;

  return (
    <div
      ref={wrapRef}
      // The ref has to be mounted for the observer to measure anything, so the
      // wrapper always renders and reserves the height. min-w-0 stops it from
      // being sized by its own contents.
      className={cn("relative w-full min-w-0 select-none", className)}
      style={{ minHeight: height }}
    >
      {/* Legend — always present for two or more series so identity is never
          carried by colour alone. Text stays in ink tokens; the swatch colours. */}
      {multi && (
        <div className="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1.5">
          {seriesList.map((s) => (
            <div key={s.label} className="flex items-center gap-1.5">
              <span
                className="size-2.5 shrink-0 rounded-full"
                style={{ backgroundColor: s.color }}
                aria-hidden
              />
              <span className="text-xs font-medium text-fg-muted">{s.label}</span>
            </div>
          ))}
        </div>
      )}

      {width > 0 && (
      <svg
        width={width}
        height={height}
        // The whole plot is one hit target rather than one per point: chasing
        // a 4px dot with a finger is a losing game, and the crosshair snaps to
        // the nearest column anyway.
        onPointerMove={(event) => setHover(pointFromClientX(event.clientX))}
        onPointerLeave={() => setHover(null)}
        role="img"
        aria-label={
          multi
            ? `${seriesList.map((s) => s.label).join(" vs ")} over the last ${count} days`
            : `${valueLabel} over the last ${count} days`
        }
        className="touch-pan-y"
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            {/* A wash, not a block — ~10% at the top, fading out entirely. */}
            <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.22" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
          </linearGradient>
        </defs>

        {/* Horizontal gridlines: hairline, solid, one step off the surface. */}
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

        {/* Vertical day markers: one faint line per day, so a week reads as seven
            columns between the two dated ends. Recessive — below the data. */}
        {data.map((_, i) => (
          <line
            key={`day-${i}`}
            x1={x(i)}
            x2={x(i)}
            y1={PAD.top}
            y2={PAD.top + innerH}
            stroke="var(--border)"
            strokeWidth={1}
            strokeOpacity={0.5}
          />
        ))}

        {/* Single series keeps its area wash; multi-series is lines only so the
            fills never muddy where they overlap. */}
        {!multi && seriesList[0] && (
          <path
            d={`${linePath(seriesList[0].values)} L${x(count - 1)},${PAD.top + innerH} L${x(0)},${PAD.top + innerH} Z`}
            fill={`url(#${gradientId})`}
          />
        )}

        {seriesList.map((s) => (
          <path
            key={s.label}
            d={linePath(s.values)}
            fill="none"
            stroke={s.color}
            strokeWidth={2}
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        ))}

        {hover !== null && (
          <g>
            <line
              x1={x(hover)}
              x2={x(hover)}
              y1={PAD.top}
              y2={PAD.top + innerH}
              stroke="var(--border-strong)"
              strokeWidth={1}
            />
            {seriesList.map((s) => (
              <circle
                key={s.label}
                cx={x(hover)}
                cy={y(s.values[hover] ?? 0)}
                r={5}
                fill={s.color}
                stroke="var(--bg-elevated)"
                strokeWidth={2}
              />
            ))}
          </g>
        )}

        {/* First and last x labels only. A tick under every column collides on
            a phone, and the tooltip names the exact day anyway. */}
        <text x={PAD.left} y={height - 6} className="fill-[var(--fg-subtle)] text-[10px]">
          {data[0]?.label}
        </text>
        <text
          x={width - PAD.right}
          y={height - 6}
          textAnchor="end"
          className="fill-[var(--fg-subtle)] text-[10px]"
        >
          {data[count - 1]?.label}
        </text>
      </svg>
      )}

      {hover !== null && hoverLabel !== undefined && (
        <div
          className={cn(
            "pointer-events-none absolute z-10 -translate-x-1/2 -translate-y-full",
            "glass rounded-md px-2.5 py-1.5 text-xs shadow-[var(--elev-3)] whitespace-nowrap",
          )}
          style={{
            left: Math.min(Math.max(x(hover), 60), width - 60),
            top: y(Math.max(...seriesList.map((s) => s.values[hover] ?? 0))) - 10,
          }}
        >
          <div className="text-fg-muted">{hoverLabel}</div>
          {seriesList.map((s) => (
            <div key={s.label} className="mt-0.5 flex items-center gap-1.5">
              <span
                className="size-2 shrink-0 rounded-full"
                style={{ backgroundColor: s.color }}
                aria-hidden
              />
              {multi && <span className="text-fg-muted">{s.label}</span>}
              <span className="ml-auto pl-2 font-semibold [font-variant-numeric:tabular-nums]">
                {formatValue(s.values[hover] ?? 0)}
              </span>
            </div>
          ))}
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
