import type { Metadata } from "next";

import { loadGuild } from "@/lib/guild";
import { TrackerAnalytics } from "@/components/dashboard/tracker-analytics";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: "Overview" };

export default async function OverviewPage({
  params,
  searchParams,
}: {
  params: Promise<{ guildId: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { guildId } = await params;
  const sp = await searchParams;
  const guild = await loadGuild(guildId);

  return (
    <div className="space-y-6">
      <PageHeader
        title={guild.name}
        description="Everything Vibey is seeing in this server, at a glance."
      />

      {/* The overview leads with the server view and carries the Activity
          Tracker's other modes as tabs: Member, Channel, Emoji, Leaderboard,
          Compare. State lives in the URL, so a tab is a navigation. */}
      <TrackerAnalytics guildId={guildId} searchParams={sp} basePath={`/dashboard/${guildId}`} />
    </div>
  );
}
