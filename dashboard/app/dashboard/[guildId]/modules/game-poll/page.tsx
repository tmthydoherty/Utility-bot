import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft, ArrowRight, PlusCircle, Gamepad2, History, Trophy, Settings } from "lucide-react";

import { gamePoll } from "@/lib/schema/modules";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: gamePoll.name };

const TILES = [
  {
    slug: "create",
    title: "Create Poll",
    description: "Pick games, set the voting window, and post the next game night.",
    icon: PlusCircle,
    accent: true,
  },
  {
    slug: "games",
    title: "Games Library",
    description: "The catalogue of games to vote on, with optional banners.",
    icon: Gamepad2,
    accent: true,
  },
  {
    slug: "history",
    title: "Participant History",
    description: "Who played each game night, and their all-time attendance.",
    icon: History,
    accent: true,
  },
  {
    slug: "voting",
    title: "Voting History",
    description: "Every week's votes: which games won, the points, and who they came from.",
    icon: Trophy,
    accent: true,
  },
  {
    slug: "settings",
    title: "Settings",
    description: "Channels, role pings, voting weights and the on/off switch.",
    icon: Settings,
    accent: false,
  },
] as const;

export default async function GamePollOverview({ params }: { params: Promise<{ guildId: string }> }) {
  const { guildId } = await params;

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader title={gamePoll.name} description={gamePoll.description} />

      <div className="grid gap-4 sm:grid-cols-2">
        {TILES.map((tile) => {
          const TileIcon = tile.icon;
          return (
            <Card key={tile.slug} interactive className="group relative p-5 sm:p-6">
              <div className="flex items-start gap-4">
                <div
                  className={
                    tile.accent
                      ? "grid size-11 shrink-0 place-items-center rounded-lg bg-[var(--primary)]/10 text-[var(--primary)]"
                      : "grid size-11 shrink-0 place-items-center rounded-lg bg-bg-surface-2 text-fg-muted"
                  }
                >
                  <TileIcon className="size-5" />
                </div>
                <div className="min-w-0 flex-1">
                  <h2 className="font-semibold leading-tight">
                    <Link
                      href={`/dashboard/${guildId}/modules/game-poll/${tile.slug}`}
                      className="after:absolute after:inset-0 after:content-['']"
                    >
                      {tile.title}
                    </Link>
                  </h2>
                  <p className="mt-1 text-sm leading-relaxed text-fg-muted">{tile.description}</p>
                </div>
                <ArrowRight className="size-4 shrink-0 text-fg-subtle transition-transform duration-200 group-hover:translate-x-0.5" />
              </div>
            </Card>
          );
        })}
      </div>

      {/* The source of truth in Discord, kept out of the way — provenance, not
          something to act on. */}
      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">{gamePoll.cog}</p>
    </div>
  );
}
