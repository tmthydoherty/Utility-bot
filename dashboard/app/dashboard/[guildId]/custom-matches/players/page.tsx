import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { readGames, isReadable } from "@/lib/custommatch/read";
import { loadMemberPool } from "@/lib/bot/directory";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { PlayerInspector } from "@/components/custommatch/player-inspector";

export const metadata: Metadata = { title: "Players · Custom Matches" };

export default async function PlayersPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const readable = isReadable();
  const games = readable ? readGames().map((g) => ({ id: g.game_id, name: g.name, enabled: g.enabled })) : [];
  const members = await loadMemberPool(guildId).catch(() => []);

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/custom-matches`}>
          <ArrowLeft aria-hidden />
          Custom Matches
        </Link>
      </Button>

      <PageHeader
        title="Players"
        description="Any member's standing in a game — MMR and its trend, streaks, per-role rating, rivalries and recent matches. Adjust their MMR, offset or IGN, and clear penalties or suspensions."
      />

      {!readable || games.length === 0 ? (
        <Card>
          <EmptyState
            icon="Users"
            title={readable ? "No games yet" : "Vibey isn't running"}
            description={
              readable
                ? "Add a game before inspecting players."
                : "The Custom Matches database can't be reached right now."
            }
          />
        </Card>
      ) : (
        <PlayerInspector guildId={guildId} games={games} members={members} />
      )}

      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">cogs/custommatch/</p>
    </div>
  );
}
