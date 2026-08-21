import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft, Plus } from "lucide-react";

import * as store from "@/lib/automations/store";
import { readinessSummary } from "@/lib/automations/readiness";
import { AutomationCard } from "@/components/automations/automation-card";
import { AutomationsToggle } from "@/components/automations/automations-toggle";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { Reveal } from "@/components/ui/reveal";

export const metadata: Metadata = { title: "Automations" };

// The bot writes to this database from another process, so a cached render
// would show yesterday's automations after an edit made in Discord.
export const dynamic = "force-dynamic";

export default async function AutomationsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  if (!store.isAvailable()) return <Unavailable />;

  const automations = store.listAutomations(guildId);
  const paused = store.automationsPaused();
  const runsToday = store.runsSince(guildId, Math.floor(Date.now() / 1000) - 86400);

  const live = automations.filter((a) => a.enabled && !a.dryRun).length;
  const testing = automations.filter((a) => a.enabled && a.dryRun).length;

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader
        title="Automations"
        description="Make Vibey do things by itself — when someone joins, when someone says a word, at a set time."
        action={
          <Button asChild>
            <Link href={`/dashboard/${guildId}/automations/new`}>
              <Plus aria-hidden />
              New automation
            </Link>
          </Button>
        }
      />

      {/* The module's own on/off switch, in the same place every other module
          keeps it. Off pauses every automation at once. */}
      <AutomationsToggle guildId={guildId} initialEnabled={!paused} />

      {automations.length > 0 && (
        <Reveal>
          <Card className="flex flex-wrap items-center gap-x-6 gap-y-2 px-5 py-4 text-sm">
            <Stat value={live} label={live === 1 ? "running" : "running"} tone="success" />
            {testing > 0 && <Stat value={testing} label="in test mode" tone="warning" />}
            <Stat
              value={automations.length - live - testing}
              label="switched off"
              tone="muted"
            />
            <span className="ml-auto text-fg-subtle">
              {runsToday === 0
                ? "Nothing has run in the last day"
                : `Ran ${runsToday.toLocaleString()} time${runsToday === 1 ? "" : "s"} in the last day`}
            </span>
          </Card>
        </Reveal>
      )}

      {automations.length === 0 ? (
        <EmptyState
          icon="Zap"
          title="No automations yet"
          description="Start from a ready-made one — it's already set up, you just change the bits you disagree with."
          action={
            <Button asChild>
              <Link href={`/dashboard/${guildId}/automations/new`}>
                Browse ready-made automations
              </Link>
            </Button>
          }
        />
      ) : (
        <>
          <div className="grid gap-4 lg:grid-cols-2">
            {automations.map((automation, index) => (
              <Reveal key={automation.id} delay={index} className="min-w-0">
                <AutomationCard
                  automation={automation}
                  guildId={guildId}
                  // Derived on the server so a card renders complete, rather
                  // than flashing "ready" and then correcting itself.
                  unfinished={readinessSummary(automation)}
                />
              </Reveal>
            ))}
          </div>

          {/* A second entry point at the foot of the list: after scrolling a
              long list of cards, the header button is well out of view. */}
          <div className="flex justify-center pt-2">
            <Button asChild variant="secondary">
              <Link href={`/dashboard/${guildId}/automations/new`}>
                <Plus aria-hidden />
                New automation
              </Link>
            </Button>
          </div>
        </>
      )}
    </div>
  );
}

function Stat({
  value,
  label,
  tone,
}: {
  value: number;
  label: string;
  tone: "success" | "warning" | "muted";
}) {
  const color =
    tone === "success"
      ? "text-[var(--success)]"
      : tone === "warning"
        ? "text-[var(--warning)]"
        : "text-fg-subtle";
  return (
    <span className="flex items-baseline gap-1.5">
      <span className={`text-lg font-semibold [font-variant-numeric:tabular-nums] ${color}`}>
        {value}
      </span>
      <span className="text-fg-muted">{label}</span>
    </span>
  );
}

/**
 * What the page says when the bot's database can't be opened.
 *
 * Not an error page. The overwhelmingly likely cause is that Vibey is
 * restarting, which is a thirty-second problem, so it reads as a status rather
 * than a fault — and it says what to check rather than showing a stack trace.
 */
function Unavailable() {
  return (
    <div className="space-y-6">
      <PageHeader title="Automations" description="Make Vibey do things by itself." />
      <EmptyState
        icon="Zap"
        title="Can't reach Vibey right now"
        description="Automations are stored by the bot itself, so this page needs it to be running. If it's restarting, this clears on its own in a few seconds."
      />
    </div>
  );
}
