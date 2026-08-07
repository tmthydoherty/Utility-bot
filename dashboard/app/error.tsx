"use client";

import { useEffect } from "react";
import { RotateCcw, TriangleAlert } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

/**
 * Top-level error boundary.
 *
 * Shows the digest rather than the message: Next strips server error messages
 * in production precisely so they can't leak internals to the browser, and the
 * digest is what ties this screen to a line in the service log.
 */
export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Dashboard error boundary:", error);
  }, [error]);

  return (
    <main id="main" className="grid min-h-dvh place-items-center px-5 py-16">
      <Card className="w-full max-w-sm p-8 text-center">
        <div className="mx-auto grid size-12 place-items-center rounded-xl bg-[var(--danger-soft)] text-[var(--danger)]">
          <TriangleAlert className="size-6" aria-hidden />
        </div>
        <h1 className="mt-6 text-lg font-semibold">Something broke</h1>
        <p className="mt-2 text-sm leading-relaxed text-fg-muted">
          The page failed to render. Nothing was changed.
        </p>
        {error.digest && (
          <p className="mt-3 font-mono text-[11px] text-fg-subtle">
            Reference: {error.digest}
          </p>
        )}
        <Button variant="secondary" className="mt-6 w-full" onClick={reset}>
          <RotateCcw aria-hidden />
          Try again
        </Button>
      </Card>
    </main>
  );
}
