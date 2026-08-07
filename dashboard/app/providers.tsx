"use client";

import { SessionProvider } from "next-auth/react";

import { ThemeProvider } from "@/components/providers/theme-provider";
import { ToastProvider } from "@/components/ui/toast";
import { TooltipProvider } from "@/components/ui/tooltip";

/**
 * Client-side context, in one place.
 *
 * Kept as a single leaf so the root layout stays a server component — pulling
 * any of these into layout.tsx directly would turn the whole tree into client
 * code and undo the point of server components.
 */
export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <SessionProvider
      // The authoritative role re-check already happens server-side on every
      // request; polling the session endpoint from the browser as well would
      // be pure duplicate traffic.
      refetchOnWindowFocus={false}
    >
      <ThemeProvider>
        <TooltipProvider delayDuration={200} skipDelayDuration={400}>
          <ToastProvider>{children}</ToastProvider>
        </TooltipProvider>
      </ThemeProvider>
    </SessionProvider>
  );
}
