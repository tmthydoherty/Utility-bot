"use client";

import * as React from "react";
import { useRouter } from "next/navigation";

import { reloadAllCogsAction, restartBotAction } from "@/app/actions/control";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { useToast } from "@/components/ui/toast";

/**
 * The two power actions.
 *
 * Reload-all is soft — no downtime, so a single button. Restart is not, so it
 * takes a deliberate second click: the first arms it, the second commits, and
 * anything else disarms it. A `window.confirm` would do the same job, but an
 * inline two-step keeps the warning on-brand and readable on a phone, where the
 * native dialog is easy to dismiss by reflex.
 */
export function PowerControls({
  guildId,
  reachable,
}: {
  guildId: string;
  reachable: boolean;
}) {
  const router = useRouter();
  const { success, error: toastError } = useToast();
  const [pending, startTransition] = React.useTransition();
  const [action, setAction] = React.useState<"reload_all" | "restart" | null>(null);
  const [armed, setArmed] = React.useState(false);

  function reloadAll() {
    setAction("reload_all");
    startTransition(async () => {
      const result = await reloadAllCogsAction(guildId);
      if (result.ok) success("Reloaded all cogs", result.note);
      else toastError("Reload failed", result.error);
      router.refresh();
      setAction(null);
    });
  }

  function restart() {
    if (!armed) {
      setArmed(true);
      return;
    }
    setArmed(false);
    setAction("restart");
    startTransition(async () => {
      const result = await restartBotAction(guildId);
      if (result.ok) success("Restarting Vibey", result.note);
      else toastError("Couldn't restart", result.error);
      router.refresh();
      setAction(null);
    });
  }

  return (
    <div className="space-y-4">
      {!reachable && (
        <Card className="p-4 sm:p-5">
          <div className="flex items-start gap-3">
            <Icon name="AlertTriangle" className="mt-0.5 size-5 shrink-0 text-[var(--danger)]" />
            <p className="text-sm text-fg-muted">
              Vibey isn&apos;t responding right now. These actions will be sent anyway, but they can
              only take effect once it&apos;s up.
            </p>
          </div>
        </Card>
      )}

      <Card className="flex flex-wrap items-center justify-between gap-4 p-5 sm:p-6">
        <div className="min-w-0">
          <p className="font-medium">Reload all cogs</p>
          <p className="mt-1 text-sm text-fg-muted">
            Re-reads every feature&apos;s code with no downtime. Good after deploying changes to
            several cogs at once.
          </p>
        </div>
        <Button
          variant="secondary"
          onClick={reloadAll}
          loading={pending && action === "reload_all"}
          disabled={pending}
        >
          <Icon name="RotateCcw" />
          Reload all
        </Button>
      </Card>

      <Card className="flex flex-wrap items-center justify-between gap-4 p-5 sm:p-6">
        <div className="min-w-0">
          <p className="font-medium">Restart Vibey</p>
          <p className="mt-1 text-sm text-fg-muted">
            {armed
              ? "This drops Vibey off Discord for a few seconds while it restarts. Click again to confirm."
              : "Stops and relaunches the whole bot. Use this when a reload isn't enough."}
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {armed && (
            <Button variant="ghost" onClick={() => setArmed(false)} disabled={pending}>
              Cancel
            </Button>
          )}
          <Button
            variant="danger"
            onClick={restart}
            loading={pending && action === "restart"}
            disabled={pending}
          >
            <Icon name="Power" />
            {armed ? "Confirm restart" : "Restart"}
          </Button>
        </div>
      </Card>
    </div>
  );
}
