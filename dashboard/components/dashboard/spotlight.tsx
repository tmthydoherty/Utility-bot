import * as React from "react";

import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";

/**
 * A "this week's ___" highlight — the top member, the busiest channel, the
 * emoji of the week. A labelled eyebrow, a big piece of media, a headline and
 * a supporting value, in one consistent frame so the three read as a set.
 */
export function Spotlight({
  eyebrow,
  icon,
  media,
  headline,
  value,
  sub,
  empty,
}: {
  eyebrow: string;
  icon: string;
  media?: React.ReactNode;
  headline: string;
  value: string;
  sub?: string;
  /** When there's nothing to show, render this instead of the body. */
  empty?: boolean;
}) {
  return (
    <Card className="flex h-full flex-col gap-4 p-5">
      <div className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-fg-subtle">
        <Icon name={icon} className="size-3.5" />
        {eyebrow}
      </div>

      {empty ? (
        <p className="flex flex-1 items-center text-sm text-fg-subtle">
          Nothing yet this week.
        </p>
      ) : (
        <div className="flex items-center gap-3.5">
          {media && <div className="shrink-0">{media}</div>}
          <div className="min-w-0">
            <p className="truncate text-lg font-semibold leading-tight">{headline}</p>
            <p className="mt-0.5 text-sm text-fg-muted">
              <span className="font-semibold text-fg tabular-nums">{value}</span>
              {sub && <span className="text-fg-subtle"> {sub}</span>}
            </p>
          </div>
        </div>
      )}
    </Card>
  );
}
