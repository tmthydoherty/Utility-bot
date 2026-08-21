import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getGameNightHistory } from "@/lib/game-poll/store";
import { HistoryClient } from "./client";

export default async function HistoryPage({ params }: { params: Promise<{ guildId: string }> }) {
  const { guildId } = await params;
  const history = getGameNightHistory();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/game-poll`}>
          <ArrowLeft aria-hidden />
          Game Polls
        </Link>
      </Button>
      <div className="space-y-2">
        <h2 className="text-lg font-medium">Participant History</h2>
        <p className="text-sm text-fg-muted">View attendance records from previous game nights.</p>
      </div>

      <HistoryClient history={history} />
    </div>
  );
}
