"use client";

import * as React from "react";
import { useRouter } from "next/navigation";

import { useToast } from "@/components/ui/toast";
import type { ActionResult } from "@/app/actions/onboarding";

// The cog polls the bridge every ten seconds; a hair more than that and a
// refresh almost always catches the applied change on the first try.
const RECONCILE_MS = 11_000;

/**
 * Shared "queue a command, tell the user, reconcile" runner for the onboarding
 * panels — the same optimistic-then-refresh flow the QOTD pool manager uses.
 * The optional optimistic update is applied only when the queue accepted the
 * command, so a failure never leaves a phantom row behind.
 */
export function useRun() {
  const router = useRouter();
  const toast = useToast();
  const [busy, setBusy] = React.useState(false);

  const run = React.useCallback(
    async (
      fn: () => Promise<ActionResult>,
      { success, optimistic }: { success: string; optimistic?: () => void },
    ): Promise<boolean> => {
      setBusy(true);
      try {
        const result = await fn();
        if (result.ok) {
          optimistic?.();
          toast.success(success, "Vibey applies this within about 10 seconds.");
          window.setTimeout(() => router.refresh(), RECONCILE_MS);
          return true;
        }
        toast.error("Couldn't save that", result.error ?? "Try again.");
        return false;
      } catch {
        toast.error("Couldn't reach the server", "Nothing was changed.");
        return false;
      } finally {
        setBusy(false);
      }
    },
    [router, toast],
  );

  return { run, busy };
}
