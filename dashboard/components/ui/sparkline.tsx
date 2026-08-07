import { cn } from "@/lib/utils";

/**
 * A 12-point trend line for a stat tile.
 *
 * Single series, so there is no legend and no direct labels — the tile's own
 * label and value say what is plotted, and a number on every point would be
 * unreadable at this size. The history is drawn in the de-emphasis ink and only
 * the current period is in the accent, so the eye lands on "now" first.
 *
 * No axes, no gridlines: a sparkline shows shape, not values. Anything that
 * needs a readable magnitude belongs in the tile's value, not in here.
 */
export function Sparkline({
  data,
  className,
  width = 96,
  height = 28,
  label,
}: {
  data: number[];
  className?: string;
  width?: number;
  height?: number;
  /** Screen-reader summary — the sparkline itself is decorative to AT. */
  label?: string;
}) {
  // Twelve points is the contract; more detail is invisible at this size.
  const points = data.slice(-12);
  if (points.length < 2) return null;

  const min = Math.min(...points);
  const max = Math.max(...points);
  // A flat series would divide by zero; draw it down the middle instead.
  const span = max - min || 1;

  // Inset by the marker radius so the end dot and its surface ring are never
  // clipped by the viewBox.
  const pad = 4;
  const innerW = width - pad * 2;
  const innerH = height - pad * 2;

  const coords = points.map((value, i) => ({
    x: pad + (i / (points.length - 1)) * innerW,
    y: pad + innerH - ((value - min) / span) * innerH,
  }));

  const path = coords.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(" ");

  // The final segment is redrawn in the accent on top of the muted line.
  const tail = coords.slice(-2);
  const tailPath = tail
    .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(2)},${p.y.toFixed(2)}`)
    .join(" ");
  const last = coords[coords.length - 1]!;

  return (
    <svg
      className={cn("overflow-visible", className)}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role={label ? "img" : "presentation"}
      aria-label={label}
      aria-hidden={label ? undefined : true}
    >
      <path
        d={path}
        fill="none"
        stroke="var(--fg-subtle)"
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d={tailPath}
        fill="none"
        stroke="var(--accent)"
        strokeWidth={2}
        strokeLinecap="round"
      />
      {/* 2px ring in the surface colour keeps the marker legible where it sits
          on top of the line. Elevated, not canvas — these live on cards, and
          in light mode the two are visibly different whites. */}
      <circle
        cx={last.x}
        cy={last.y}
        r={4}
        fill="var(--accent)"
        stroke="var(--bg-elevated)"
        strokeWidth={2}
      />
    </svg>
  );
}
