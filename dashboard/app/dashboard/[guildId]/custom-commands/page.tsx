import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/custom-commands/store";
import { CommandList } from "@/components/custom-commands/command-list";
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: "Custom Commands" };

// The bot writes usage counts to this database, and the economy approves GIF
// commands into another, so a cached render would show stale counts and miss a
// just-approved command.
export const dynamic = "force-dynamic";

export default async function CustomCommandsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  if (!store.isAvailable()) return <Unavailable guildId={guildId} />;

  const entries = store.listAll(guildId);

  const commands = entries.filter((e) => e.kind === "custom");
  const gifs = entries.filter((e) => e.kind === "gif");
  const uses = commands.reduce((sum, e) => sum + (e.kind === "custom" ? e.command.useCount : 0), 0);
  const topUsed = commands
    .flatMap((e) => (e.kind === "custom" ? [{ name: e.command.name, count: e.command.useCount }] : []))
    .filter((t) => t.count > 0)
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader
        title="Custom Commands"
        description="Make your own !commands — text, GIFs or full embeds — with random responses, role and channel limits, cooldowns and placeholders. Members' purchased GIF commands appear here too, marked ★ econ purchased."
      />

      <CommandList
        guildId={guildId}
        entries={entries}
        stats={{ commands: commands.length, gifs: gifs.length, uses, topUsed }}
      />
    </div>
  );
}

function Unavailable({ guildId }: { guildId: string }) {
  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>
      <PageHeader title="Custom Commands" description="Your own !commands, served by the bot." />
      <EmptyState
        icon="TerminalSquare"
        title="Can't reach Vibey right now"
        description="These commands are stored by the bot itself, so this page needs it to be running. If it's restarting, this clears on its own in a few seconds."
      />
    </div>
  );
}
