import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getGames } from "@/lib/game-poll/store";
import { readSettings } from "@/lib/bot/adapter";
import { CreatePollForm } from "./form";

export default async function CreatePollPage({ params }: { params: Promise<{ guildId: string }> }) {
  const { guildId } = await params;
  const games = getGames();
  const settings = readSettings(guildId, "game-poll");
  const bannerUrl = (settings.game_night_banner_url as string) || "";

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/game-poll`}>
          <ArrowLeft aria-hidden />
          Game Polls
        </Link>
      </Button>

      <div className="space-y-2">
        <h2 className="text-lg font-medium">Create New Poll</h2>
        <p className="text-sm text-fg-muted">Set up the next game night poll and post it directly to your channels.</p>
      </div>

      <CreatePollForm guildId={guildId} games={games} bannerUrl={bannerUrl} />
    </div>
  );
}
