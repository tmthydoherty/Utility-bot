import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * The house surface.
 *
 * `glass-highlight` draws a one-pixel gradient border that is bright at the
 * top-left and fades out — the cue that reads as "lit from above" and is most
 * of the difference between a glass panel and a grey rectangle.
 */
export function Card({
  className,
  interactive,
  ...props
}: React.HTMLAttributes<HTMLDivElement> & { interactive?: boolean }) {
  return (
    <div
      className={cn(
        "glass glass-highlight rounded-lg shadow-[var(--elev-2)]",
        interactive &&
          cn(
            "transition-all duration-200 ease-out",
            "hover:border-[var(--border-strong)] hover:bg-[var(--surface-hover)]",
            "hover:shadow-[var(--elev-3)] hover:-translate-y-0.5",
          ),
        className,
      )}
      {...props}
    />
  );
}

export function CardHeader({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("flex flex-col gap-1.5 p-5 sm:p-6", className)} {...props} />;
}

export function CardTitle({ className, ...props }: React.HTMLAttributes<HTMLHeadingElement>) {
  return (
    <h3 className={cn("text-base font-semibold leading-tight tracking-tight", className)} {...props} />
  );
}

export function CardDescription({
  className,
  ...props
}: React.HTMLAttributes<HTMLParagraphElement>) {
  return <p className={cn("text-sm leading-relaxed text-fg-muted", className)} {...props} />;
}

export function CardContent({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("p-5 pt-0 sm:p-6 sm:pt-0", className)} {...props} />;
}

export function CardFooter({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("flex items-center gap-3 border-t border-[var(--border)] p-5 sm:p-6", className)}
      {...props}
    />
  );
}
