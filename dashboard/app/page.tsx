import Link from "next/link";
import { redirect } from "next/navigation";
import { ArrowRight, Blocks, Gauge, ShieldCheck, Smartphone } from "lucide-react";

import { getActiveSession } from "@/auth";
import { DiscordSignIn } from "@/components/auth/discord-sign-in";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { MODULES } from "@/lib/schema/modules";
import { Reveal } from "@/components/ui/reveal";

export default async function LandingPage() {
  // Anyone already signed in is here by accident (a bookmark, usually).
  if (await getActiveSession()) redirect("/dashboard");

  return (
    <main id="main" className="mx-auto w-full max-w-5xl px-5 py-16 sm:px-6 sm:py-24">
      <Reveal>
        <div className="flex flex-col items-center text-center">
          <Badge variant="accent" className="mb-6">
            <span className="size-1.5 rounded-full bg-current" aria-hidden />
            {MODULES.length} modules
          </Badge>

          <h1 className="max-w-3xl text-balance text-4xl font-semibold leading-[1.1] tracking-tight sm:text-6xl">
            Everything Vibey does,{" "}
            <span className="text-gradient">in one place</span>
          </h1>

          <p className="mt-5 max-w-xl text-balance text-base leading-relaxed text-fg-muted sm:text-lg">
            Configure economy, tickets, trivia, moderation and the rest without
            scrolling through a Discord panel on your phone.
          </p>

          <div className="mt-9">
            <DiscordSignIn />
          </div>

          <p className="mt-4 text-xs text-fg-subtle">
            Server administrators only. Access is checked against Discord on every visit.
          </p>
        </div>
      </Reveal>

      <div className="mt-20 grid gap-4 sm:grid-cols-2">
        {[
          {
            icon: Blocks,
            title: "Every module, one surface",
            body: "Each cog gets a real settings page built from the same field definitions the Discord panels use — so the two can never disagree.",
          },
          {
            icon: Smartphone,
            title: "Built for a phone first",
            body: "Thumb-reachable navigation, sheets instead of dialogs, and a save bar that follows you down the page.",
          },
          {
            icon: ShieldCheck,
            title: "Locked to your admins",
            body: "Discord sign-in, re-verified against your server's roles every few minutes. Lose the role, lose access.",
          },
          {
            icon: Gauge,
            title: "Fast on the hardware you have",
            body: "Server-rendered, minimal JavaScript, and no chart library shipped to the browser.",
          },
        ].map((feature, index) => (
          <Reveal key={feature.title} delay={index}>
            <Card className="h-full p-6">
              <feature.icon className="size-5 text-[var(--accent)]" aria-hidden />
              <h2 className="mt-4 font-semibold">{feature.title}</h2>
              <p className="mt-2 text-sm leading-relaxed text-fg-muted">{feature.body}</p>
            </Card>
          </Reveal>
        ))}
      </div>

      <footer className="mt-20 flex flex-col items-center gap-2 text-xs text-fg-subtle">
        <Link href="/login" className="inline-flex items-center gap-1 transition-colors hover:text-fg">
          Already an admin? Sign in
          <ArrowRight className="size-3" aria-hidden />
        </Link>
        <p>Vibey · self-hosted</p>
      </footer>
    </main>
  );
}
