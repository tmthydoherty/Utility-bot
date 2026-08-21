import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { readGames, readAnalytics, readRivalries, isReadable } from "@/lib/custommatch/read";
import { resolveUsers } from "@/lib/bot/directory";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { AnalyticsView, type GameAnalytics } from "@/components/custommatch/analytics-view";

export const metadata: Metadata = { title: "Analytics · Custom Matches" };

export default async function AnalyticsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const readable = isReadable();

  const games: GameAnalytics[] = readable
    ? readGames().map((g) => ({
        id: g.game_id,
        name: g.name,
        enabled: g.enabled,
        analytics: readAnalytics(g.game_id),
        rivalries: readRivalries(g.game_id),
      }))
    : [];

  // Resolve every player who appears in any rivalry once, in a single roster call.
  const rivalIds = new Set<string>();
  for (const game of games) {
    for (const r of game.rivalries) {
      rivalIds.add(r.player_a_id);
      rivalIds.add(r.player_b_id);
    }
  }
  const names: Record<string, string> = {};
  if (rivalIds.size > 0) {
    const resolved = await resolveUsers(guildId, [...rivalIds]).catch(() => new Map());
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
        title="Analytics"
        description="Match volume, the busiest hours, how MMR is spread across the ladder, and the server's hottest rivalries — per game."
      />

      {!readable || games.length === 0 ? (
        <Card>
          <EmptyState
            icon="ChartLine"
            title={readable ? "No games yet" : "Vibey isn't running"}
            description={
              readable
                ? "Analytics appear once a game exists and matches are played."
                : "The Custom Matches database can't be reached right now."
            }
          />
        </Card>
      ) : (
        <AnalyticsView games={games} names={names} />
      )}

      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">cogs/custommatch/</p>
    </div>
  );
}
