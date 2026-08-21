"use client";

import * as React from "react";
import { useRouter } from "next/navigation";

import { cn } from "@/lib/utils";
import type { CogAction, CogInfo } from "@/lib/bot/control";
import { runCogAction } from "@/app/actions/control";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { useToast } from "@/components/ui/toast";

/**
 * The cog manager.
 *
 * One row per cog on disk, each showing whether the bot currently has it loaded.
 * The actions are deliberately asymmetric: a loaded cog can be reloaded or
 * unloaded, an unloaded one can only be loaded. When an action fails, the bot
 * returns the full Python traceback and it's shown inline under the row — the
 * whole reason to do this from a browser instead of blind over SSH.
 */
export function CogList({
  guildId,
  initialCogs,
}: {
  guildId: string;
  initialCogs: CogInfo[] | null;
}) {
  const router = useRouter();
  const { success, error: toastError } = useToast();
  const [pending, startTransition] = React.useTransition();
  const [busyKey, setBusyKey] = React.useState<string | null>(null);
  const [tracebacks, setTracebacks] = React.useState<Record<string, string>>({});

  if (initialCogs === null) {
    return (
      <Card className="p-5 sm:p-6">
        <div className="flex items-start gap-3">
          <Icon name="AlertTriangle" className="mt-0.5 size-5 shrink-0 text-[var(--danger)]" />
          <div className="space-y-1 text-sm">
            <p className="font-medium">Vibey isn&apos;t reachable.</p>
            <p className="text-fg-muted">
              The cog list comes straight from the running bot, and it didn&apos;t answer. Try again
              once it&apos;s back up.
            </p>
          </div>
        </div>
      </Card>
    );
  }

  function run(name: string, action: CogAction) {
    const key = `${name}:${action}`;
    setBusyKey(key);
    startTransition(async () => {
      const result = await runCogAction(guildId, name, action);
      if (result.ok) {
        success(`${labelFor(action)} ${name}`, result.note);
        // Clear any stale traceback now that it succeeded.
        setTracebacks((current) => {
          const next = { ...current };
          delete next[name];
          return next;
        });
      } else {
        toastError(`Couldn't ${action} ${name}`, result.error);
        if (result.traceback) {
          setTracebacks((current) => ({ ...current, [name]: result.traceback! }));
        }
      }
      router.refresh();
      setBusyKey(null);
    });
  }

  const loadedCount = initialCogs.filter((c) => c.loaded).length;

  return (
    <div className="space-y-3">
      <p className="text-sm text-fg-muted">
        {loadedCount} of {initialCogs.length} loaded.
      </p>

      <Card className="divide-y divide-[var(--border)]">
        {initialCogs.map((cog) => {
          const traceback = tracebacks[cog.name];
          return (
            <div key={cog.name} className="p-4 sm:p-5">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex min-w-0 items-center gap-3">
                  <span
                    className={cn(
                      "size-2 shrink-0 rounded-full",
                      cog.loaded ? "bg-[var(--success)]" : "bg-[var(--fg-subtle)]",
                    )}
                    aria-hidden
                  />
                  <div className="min-w-0">
                    <p className="flex items-center gap-2 font-medium">
                      <span className="truncate">{cog.name}</span>
                      {cog.isPackage && (
                        <span className="shrink-0 rounded bg-[var(--surface)] px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-fg-subtle">
                          package
                        </span>
                      )}
                    </p>
                    <p className="text-xs text-fg-subtle">
                      {cog.loaded ? "Loaded" : "Not loaded"} · {cog.module}
                    </p>
                  </div>
                </div>

                <div className="flex shrink-0 items-center gap-2">
                  {cog.loaded ? (
                    <>
                      <Button
                        size="sm"
                        variant="secondary"
                        loading={busyKey === `${cog.name}:reload`}
                        disabled={pending}
                        onClick={() => run(cog.name, "reload")}
                      >
                        <Icon name="RotateCcw" />
                        Reload
                      </Button>
                      <Button
                        size="sm"
                        variant="ghost"
                        loading={busyKey === `${cog.name}:unload`}
                        disabled={pending}
                        onClick={() => run(cog.name, "unload")}
                      >
                        Unload
                      </Button>
                    </>
                  ) : (
                    <Button
                      size="sm"
                      variant="secondary"
                      loading={busyKey === `${cog.name}:load`}
                      disabled={pending}
                      onClick={() => run(cog.name, "load")}
                    >
                      Load
                    </Button>
                  )}
                </div>
              </div>

              {traceback && (
                <pre className="mt-3 max-h-64 overflow-auto rounded-md border border-[var(--danger)]/40 bg-[var(--danger-soft)] p-3 text-xs leading-relaxed text-[var(--danger)]">
                  {traceback}
                </pre>
              )}
            </div>
          );
        })}
      </Card>
    </div>
  );
}

function labelFor(action: CogAction): string {
  return action === "reload" ? "Reloaded" : action === "load" ? "Loaded" : "Unloaded";
}
