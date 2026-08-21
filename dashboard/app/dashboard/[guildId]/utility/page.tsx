import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/utility/store";
import { UtilityTabs } from "@/components/utility/utility-tabs";
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: "Utility" };

// The bot writes to this database from another process, so a cached render
// would show yesterday's reminders after an edit.
export const dynamic = "force-dynamic";

export default async function UtilityPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  if (!store.isAvailable()) return <Unavailable guildId={guildId} />;

  const [reminders, stickies, reactions, media] = [
    store.listReminders(guildId),
    store.listStickies(guildId),
    store.listReactionRules(guildId),
    store.listMediaChannels(guildId),
  ];

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader
        title="Utility"
        description="Custom embeds, reminders and one-off messages, sticky notes, reaction rules and media-only channels — all in one place."
      />

      <UtilityTabs
        guildId={guildId}
        reminders={reminders}
        stickies={stickies}
        reactions={reactions}
        media={media}
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
      <PageHeader title="Utility" description="Reminders, messages, stickies and channel rules." />
      <EmptyState
        icon="Wand2"
        title="Can't reach Vibey right now"
        description="These settings are stored by the bot itself, so this page needs it to be running. If it's restarting, this clears on its own in a few seconds."
      />
    </div>
  );
}
