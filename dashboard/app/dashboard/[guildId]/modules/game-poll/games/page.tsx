import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getGames } from "@/lib/game-poll/store";
import { GamesForm } from "./form";

export default async function GamesPage({ params }: { params: Promise<{ guildId: string }> }) {
  const { guildId } = await params;
  const games = getGames();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/game-poll`}>
          <ArrowLeft aria-hidden />
          Game Polls
        </Link>
      </Button>
      <GamesForm guildId={guildId} initialGames={games} />
    </div>
  );
}
