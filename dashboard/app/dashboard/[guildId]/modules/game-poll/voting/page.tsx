import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { Button } from "@/components/ui/button";
import { getVotingHistory } from "@/lib/game-poll/store";
import { resolveUsers } from "@/lib/bot/directory";
import { VotingClient, type ResolvedVoter } from "./client";

export default async function VotingHistoryPage({ params }: { params: Promise<{ guildId: string }> }) {
  const { guildId } = await params;
  const history = getVotingHistory();

  // Names and avatars are resolved here, once, off the raw voter ids — the same
  // batched lookup the activity tracker uses — so the client stays a pure
  // renderer with everything it needs already in hand.
  const resolved = await resolveUsers(guildId, history.userIds).catch(() => new Map());
  const voters: Record<string, ResolvedVoter> = {};
  for (const id of history.userIds) {
    const r = resolved.get(id);
    voters[id] = r
      ? { name: r.name, avatar: r.avatar, former: Boolean(r.former) }
      : { name: "Member", avatar: null, former: false };
  }

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/game-poll`}>
          <ArrowLeft aria-hidden />
          Game Polls
        </Link>
      </Button>

      <VotingClient history={history} voters={voters} />
    </div>
  );
}
