import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";

import { getActiveSession } from "@/auth";
import { DiscordSignIn } from "@/components/auth/discord-sign-in";
import { Card } from "@/components/ui/card";

export const metadata: Metadata = { title: "Sign in" };

export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ next?: string }>;
}) {
  if (await getActiveSession()) redirect("/dashboard");

  const { next } = await searchParams;

  return (
    <main id="main" className="grid min-h-dvh place-items-center px-5 py-16">
      <Card className="w-full max-w-sm p-8 text-center">
        <div className="accent-gradient mx-auto grid size-12 place-items-center rounded-xl text-white shadow-[var(--glow)]">
          <span className="text-lg font-semibold">V</span>
        </div>

        <h1 className="mt-6 text-xl font-semibold tracking-tight">Sign in to Vibey</h1>
        <p className="mt-2 text-sm leading-relaxed text-fg-muted">
          Use the Discord account that has administrator access in your server.
        </p>

        <div className="mt-7">
          <DiscordSignIn next={next} className="w-full" />
        </div>

        <p className="mt-5 text-xs leading-relaxed text-fg-subtle">
          Vibey only reads your Discord username and avatar. It never posts as you.
        </p>

        <Link
          href="/"
          className="mt-6 inline-block text-xs text-fg-subtle transition-colors hover:text-fg"
        >
          Back to the front page
        </Link>
      </Card>
    </main>
  );
}
