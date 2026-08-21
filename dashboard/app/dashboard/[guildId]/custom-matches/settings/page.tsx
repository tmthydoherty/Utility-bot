import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { readGlobal, isReadable } from "@/lib/custommatch/read";
import { isAvailable } from "@/lib/custommatch/store";
import { loadMemberPool, resolveUsers } from "@/lib/bot/directory";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { GlobalSetup } from "@/components/custommatch/global-setup";

export const metadata: Metadata = { title: "Global setup · Custom Matches" };

export default async function GlobalSetupPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const readable = isReadable();
  const global = readable ? readGlobal() : null;
  const members = await loadMemberPool(guildId).catch(() => []);

  // Resolve blacklisted members — some may have left, so they aren't in the pool.
  const names: Record<string, string> = {};
  if (global && global.blacklist.length > 0) {
    const resolved = await resolveUsers(
      guildId,
      global.blacklist.map((b) => b.player_id),
    ).catch(() => new Map());
    for (const [id, user] of resolved) names[id] = user.name;
  }

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/custom-matches`}>
          <ArrowLeft aria-hidden />
          Custom Matches
        </Link>
      </Button>

      <PageHeader
        title="Global setup"
        description="Settings shared across every game — where the system logs and posts, who administers it, and the player blacklist."
        action={!isAvailable() ? <Badge variant="warning">Vibey isn&apos;t running</Badge> : undefined}
      />

      {!readable || !global ? (
        <Card>
          <EmptyState
            icon="SlidersHorizontal"
            title="Vibey isn't running"
            description="The Custom Matches database can't be reached right now."
          />
        </Card>
      ) : (
        <GlobalSetup
          guildId={guildId}
          global={global}
          members={members.map((m) => ({ id: m.id, name: m.name, username: m.username }))}
          names={names}
        />
      )}

      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">cogs/custommatch/</p>
    </div>
  );
}
