"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { RotateCcw } from "lucide-react";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";

/**
 * The log tail.
 *
 * A refresh button rather than a live stream: the log is read on demand from the
 * bot, and a poll every few seconds would be a standing load on the process for
 * a page that's open rarely and briefly. The button re-runs the server fetch via
 * `router.refresh()`, so the newest lines are always one click away.
 *
 * Coloured by level so an ERROR doesn't hide in a wall of INFO — the same rule
 * the log formatter uses, matched on the level token main.py writes.
 */
export function LogView({ lines }: { lines: string[] | null }) {
  const router = useRouter();
  const [pending, startTransition] = React.useTransition();

  const refresh = () => startTransition(() => router.refresh());

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm text-fg-muted">
          {lines === null
            ? "Unavailable"
            : lines.length === 0
              ? "The log is empty."
              : `${lines.length} lines`}
        </p>
        <Button size="sm" variant="secondary" onClick={refresh} loading={pending}>
          <RotateCcw />
          Refresh
        </Button>
      </div>

      {lines === null ? (
        <Card className="p-5 sm:p-6">
          <div className="flex items-start gap-3">
            <Icon name="AlertTriangle" className="mt-0.5 size-5 shrink-0 text-[var(--danger)]" />
            <div className="space-y-1 text-sm">
              <p className="font-medium">Vibey isn&apos;t reachable.</p>
              <p className="text-fg-muted">
                The log is read from the running bot, and it didn&apos;t answer. Try again once
                it&apos;s back.
              </p>
            </div>
          </div>
        </Card>
      ) : (
        <Card className="overflow-hidden">
          <pre className="max-h-[65vh] overflow-auto p-4 font-mono text-xs leading-relaxed">
            {lines.map((line, i) => (
              <div key={i} className={cn("whitespace-pre-wrap break-words", levelClass(line))}>
                {line || " "}
              </div>
            ))}
          </pre>
        </Card>
      )}
    </div>
  );
}

function levelClass(line: string): string {
  if (/ - (ERROR|CRITICAL) - /.test(line)) return "text-[var(--danger)]";
  if (/ - WARNING - /.test(line)) return "text-[var(--warning,var(--accent))]";
  return "text-fg-muted";
}
