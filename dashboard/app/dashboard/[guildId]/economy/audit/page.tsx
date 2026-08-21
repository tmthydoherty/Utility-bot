import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { loadMemberPool } from "@/lib/bot/directory";
import { economyStatsAvailable } from "@/lib/bot/economy-stats";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { MemberInspector } from "@/components/economy/member-inspector";

export const metadata: Metadata = { title: "Economy audit" };

export default async function EconomyAuditPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  const available = economyStatsAvailable();
  const members = available ? await loadMemberPool(guildId).catch(() => []) : [];

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/economy`}>
          <ArrowLeft aria-hidden />
          Economy
        </Link>
      </Button>

      <PageHeader
        title="Audit log"
        description="Look up any member to see their balance, the items they hold and have used, and a day-by-day account of how they earned every point."
      />

      {available ? (
        <MemberInspector guildId={guildId} members={members} />
      ) : (
        <Card>
          <EmptyState
            icon="ScrollText"
            title="Vibey isn't running"
            description="The economy's records are read straight from the bot. Once Vibey is up, search here for any member's full economy history."
          />
        </Card>
      )}
    </div>
  );
}
