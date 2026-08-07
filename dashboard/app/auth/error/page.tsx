import type { Metadata } from "next";
import Link from "next/link";
import { TriangleAlert } from "lucide-react";

import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export const metadata: Metadata = { title: "Sign-in problem" };

/**
 * Auth.js error codes, translated.
 *
 * The raw codes ("OAuthCallbackError") mean nothing to the person reading
 * them, and the default Auth.js error page leaks a little more about the
 * configuration than a public page should.
 */
const MESSAGES: Record<string, { title: string; body: string }> = {
  RateLimited: {
    title: "Too many attempts",
    body: "Sign-in has been paused briefly from this address.",
  },
  AccessDenied: {
    title: "Access denied",
    body: "That account isn't an administrator in the Vibey server.",
  },
  Configuration: {
    title: "Sign-in isn't set up yet",
    body: "The Discord OAuth credentials are missing or wrong. Check dashboard/.env.local.",
  },
  OAuthCallbackError: {
    title: "Discord didn't complete the sign-in",
    body: "The callback came back incomplete. Trying again usually clears it.",
  },
  Verification: {
    title: "That link has expired",
    body: "Start the sign-in again from the beginning.",
  },
};

const FALLBACK = {
  title: "Something went wrong signing in",
  body: "Try again. If it keeps happening, check the dashboard service logs.",
};

export default async function AuthErrorPage({
  searchParams,
}: {
  searchParams: Promise<{ error?: string; retry?: string }>;
}) {
  const { error, retry } = await searchParams;
  const message = error ? (MESSAGES[error] ?? FALLBACK) : FALLBACK;

  return (
    <main id="main" className="grid min-h-dvh place-items-center px-5 py-16">
      <Card className="w-full max-w-sm p-8 text-center">
        <div className="mx-auto grid size-12 place-items-center rounded-xl bg-[var(--warning-soft)] text-[var(--warning)]">
          <TriangleAlert className="size-6" aria-hidden />
        </div>

        <h1 className="mt-6 text-xl font-semibold tracking-tight">{message.title}</h1>
        <p className="mt-2 text-sm leading-relaxed text-fg-muted">{message.body}</p>
        {retry && (
          <p className="mt-3 text-sm text-fg-subtle">Try again in about {retry} seconds.</p>
        )}

        <div className="mt-7">
          <Button asChild variant="secondary" className="w-full">
            <Link href="/login">Back to sign in</Link>
          </Button>
        </div>
      </Card>
    </main>
  );
}
