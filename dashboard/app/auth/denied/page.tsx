import type { Metadata } from "next";
import Link from "next/link";
import { ShieldAlert } from "lucide-react";

import { DENY_MESSAGES, type DenyReason } from "@/lib/auth/authorize";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export const metadata: Metadata = { title: "Access denied" };

/**
 * Why the sign-in was refused.
 *
 * Saying which of the three reasons applied is a deliberate choice. It reveals
 * only what the person could establish themselves by opening Discord, and the
 * alternative — a generic "access denied" — sends a real admin hunting for a
 * problem that is usually one missing role.
 */
const NEXT_STEPS: Record<DenyReason, string> = {
  "not-member": "Join the server first, then sign in again.",
  "no-admin": "Ask an existing administrator to grant you the admin role.",
  unavailable: "This is usually temporary. Wait a minute and try again.",
};

export default async function DeniedPage({
  searchParams,
}: {
  searchParams: Promise<{ reason?: string }>;
}) {
  const { reason: raw } = await searchParams;
  const reason: DenyReason =
    raw === "not-member" || raw === "no-admin" || raw === "unavailable" ? raw : "no-admin";

  return (
    <main id="main" className="grid min-h-dvh place-items-center px-5 py-16">
      <Card className="w-full max-w-sm p-8 text-center">
        <div className="mx-auto grid size-12 place-items-center rounded-xl bg-[var(--danger-soft)] text-[var(--danger)]">
          <ShieldAlert className="size-6" aria-hidden />
        </div>

        <h1 className="mt-6 text-xl font-semibold tracking-tight">Access denied</h1>
        <p className="mt-2 text-sm leading-relaxed text-fg-muted">{DENY_MESSAGES[reason]}</p>
        <p className="mt-3 text-sm leading-relaxed text-fg-subtle">{NEXT_STEPS[reason]}</p>

        <div className="mt-7 flex flex-col gap-2">
          <Button asChild variant="secondary">
            <Link href="/login">Try a different account</Link>
          </Button>
          <Button asChild variant="ghost" size="sm">
            <Link href="/">Back to the front page</Link>
          </Button>
        </div>
      </Card>
    </main>
  );
}
