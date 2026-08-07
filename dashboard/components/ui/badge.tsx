import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium leading-5 [&_svg]:size-3",
  {
    variants: {
      variant: {
        neutral: "bg-[var(--surface-hover)] text-fg-muted",
        accent: "bg-[var(--accent-soft)] text-[var(--accent)]",
        success: "bg-[var(--success-soft)] text-[var(--success)]",
        warning: "bg-[var(--warning-soft)] text-[var(--warning)]",
        danger: "bg-[var(--danger-soft)] text-[var(--danger)]",
        outline: "border border-[var(--border-strong)] text-fg-muted",
      },
    },
    defaultVariants: { variant: "neutral" },
  },
);

export function Badge({
  className,
  variant,
  ...props
}: React.HTMLAttributes<HTMLSpanElement> & VariantProps<typeof badgeVariants>) {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />;
}

/** A coloured dot with a live-region-safe label, for on/off state. */
export function StatusDot({
  active,
  className,
  label,
}: {
  active: boolean;
  className?: string;
  label?: string;
}) {
  return (
    <span className={cn("relative flex size-2 shrink-0", className)}>
      {active && (
        // Pings only when active — a pulsing dot next to "disabled" reads as
        // activity and undoes the point of the indicator.
        <span
          aria-hidden
          className="absolute inline-flex size-full animate-ping rounded-full bg-[var(--success)] opacity-60"
        />
      )}
      <span
        className={cn(
          "relative inline-flex size-2 rounded-full",
          active ? "bg-[var(--success)]" : "bg-[var(--fg-subtle)]",
        )}
      />
      {label && <span className="sr-only">{label}</span>}
    </span>
  );
}
