import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/ticketing/store";
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { TicketingHome } from "@/components/ticketing/ticketing-home";

export const metadata: Metadata = { title: "Ticketing" };

// The bot writes to this database from another process, so a cached render would
// show a stale panel after an edit made in Discord.
export const dynamic = "force-dynamic";

export default async function TicketingPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  if (!store.isAvailable()) return <Unavailable />;

  const topics = store.listTopics();
  const panels = store.listPanels();
  const responses = store.listResponses();
  const responseCount = Object.values(responses).reduce((sum, list) => sum + list.length, 0);

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>
      <PageHeader
        title="Ticketing"
        description="Support tickets, applications and surveys — the topics people open, the panels they click, and the answers they leave."
      />
      <TicketingHome
        guildId={guildId}
        topics={topics}
        panels={panels}
        responseCount={responseCount}
      />
    </div>
  );
}

function Unavailable() {
  return (
    <div className="space-y-6">
      <PageHeader title="Ticketing" description="Support tickets, applications and surveys." />
      <EmptyState
        icon="TicketCheck"
        title="Can't reach Vibey right now"
        description="Ticketing is stored by the bot itself, so this page needs it to be running. If it's restarting, this clears on its own in a few seconds."
      />
    </div>
  );
}
