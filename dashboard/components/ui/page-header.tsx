import { cn } from "@/lib/utils";

export function PageHeader({
  title,
  description,
  action,
  className,
}: {
  title: string;
  description?: string;
  action?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex flex-wrap items-start justify-between gap-4", className)}>
      <div className="min-w-0 space-y-1.5">
        <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">{title}</h1>
        {description && (
          <p className="max-w-2xl text-sm leading-relaxed text-fg-muted">{description}</p>
        )}
      </div>
      {action && <div className="shrink-0">{action}</div>}
    </div>
  );
}

/**
 * Marks numbers that are not yet coming from the bot.
 *
 * The alternative to shipping placeholder figures is an empty overview, but
 * unlabelled placeholders are how someone ends up making a decision on an
 * invented number. This is the compromise, and it stays until the tracker data
 * is wired in.
 */
export function SampleDataNotice({ className }: { className?: string }) {
  return (
    <p
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full bg-[var(--warning-soft)] px-2.5 py-1",
        "text-xs font-medium text-[var(--warning)]",
        className,
      )}
    >
      <span className="size-1.5 rounded-full bg-current" aria-hidden />
      Sample data — not yet connected to the bot
    </p>
  );
}
