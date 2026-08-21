import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import { readGame, readRanks, isReadable } from "@/lib/custommatch/read";
import { isAvailable } from "@/lib/custommatch/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { Badge } from "@/components/ui/badge";
import { GameEditor } from "@/components/custommatch/game-editor";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ gameId: string }>;
}): Promise<Metadata> {
  const { gameId } = await params;
  const game = isReadable() ? readGame(Number(gameId)) : null;
  return { title: game ? `${game.name} · Custom Matches` : "Custom Matches" };
}

export default async function GameEditorPage({
  params,
}: {
  params: Promise<{ guildId: string; gameId: string }>;
}) {
  const { guildId, gameId } = await params;
  const id = Number(gameId);
  if (!Number.isInteger(id) || id <= 0) notFound();

  const game = isReadable() ? readGame(id) : null;
  if (!game) notFound();

  const ranks = readRanks(id);
  const botOnline = isAvailable();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/custom-matches`}>
          <ArrowLeft aria-hidden />
          Custom Matches
        </Link>
      </Button>

      <PageHeader
        title={game.name}
        description="Everything about this game's queue, ranks, schedule and penalties. Settings buffer until you save; ranks and the danger zone apply immediately."
        action={game.enabled ? undefined : <Badge variant="warning">Disabled</Badge>}
      />

      <GameEditor guildId={guildId} game={game} ranks={ranks} botOnline={botOnline} />

      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">cogs/custommatch/</p>
    </div>
  );
}
