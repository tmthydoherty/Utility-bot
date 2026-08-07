import * as React from "react";

import { cn } from "@/lib/utils";
import { Icon } from "./icon";

/**
 * The "nothing here" state.
 *
 * Always says what would put something here, because an empty panel with no
 * next action is indistinguishable from a broken one.
 */
export function EmptyState({
  icon = "Sparkles",
  title,
  description,
  action,
  className,
}: {
  icon?: string;
  title: string;
  description?: string;
  action?: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center gap-3 px-6 py-14 text-center",
        className,
      )}
    >
      <div className="glass grid size-14 place-items-center rounded-xl text-fg-subtle">
        <Icon name={icon} className="size-6" />
      </div>
      <div className="space-y-1">
        <p className="font-medium text-fg">{title}</p>
        {description && (
          <p className="mx-auto max-w-sm text-sm leading-relaxed text-fg-muted">{description}</p>
        )}
      </div>
      {action}
    </div>
  );
}
