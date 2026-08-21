import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { readGameCards, readGlobal, isReadable, type CmGameCard } from "@/lib/custommatch/read";
import { getChannels } from "@/lib/discord/rest";
import { Badge, StatusDot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";
import { StatTile } from "@/components/ui/stat-tile";
import { AddGameButton } from "@/components/custommatch/add-game-button";

export const metadata: Metadata = { title: "Custom Matches" };

/** The sub-sections of the module, shown as tiles under the game grid. */
const SECTION_TILES = [
  {
    slug: "players",
    title: "Players",
    description: "Search any member for their MMR, streaks, rivalries and match history — and adjust them.",
    icon: "Users",
  },
  {
    slug: "analytics",
    title: "Analytics",
    description: "Match volume, peak hours, MMR spread, and the server's hottest rivalries.",
    icon: "ChartLine",
  },
  {
    slug: "settings",
    title: "Global setup",
    description: "The log and admin channels, the CM admin and mod roles, and the player blacklist.",
    icon: "SlidersHorizontal",
  },
] as const;

export default async function CustomMatchesPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const readable = isReadable();
  const cards = readable ? readGameCards() : [];
  const global = readable ? readGlobal() : null;

  const channels = await getChannels(guildId).catch(() => []);
  const channelName = new Map(channels.map((c) => [c.id, c.name]));

  const enabledCount = cards.filter((c) => c.enabled).length;
  const inQueueNow = cards.reduce((sum, c) => sum + c.queued, 0);

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader
        title="Custom Matches"
        description="In-house matchmaking — every game's queue, MMR, ranks and schedule, plus player stats and leaderboards. Read-only until you open a game or a section, so nothing moves by accident."
        action={readable ? <AddGameButton guildId={guildId} /> : undefined}
      />

      {!readable ? (
        <Card>
          <EmptyState
            icon="Swords"
            title="Vibey isn't running"
            description="The Custom Matches database can't be reached right now, so there's nothing to show. It'll appear here as soon as the bot is up."
          />
        </Card>
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-3">
            <StatTile label="Games" value={cards.length} icon="Swords" />
            <StatTile label="Enabled" value={enabledCount} icon="Trophy" />
            <StatTile label="In queue now" value={inQueueNow} icon="Users" />
          </div>

          {cards.length === 0 ? (
            <Card>
              <EmptyState
                icon="Swords"
                title="No games yet"
                description="Create one from a preset to get started — you can fine-tune everything afterwards."
                action={<AddGameButton guildId={guildId} />}
              />
            </Card>
          ) : (
            <section className="space-y-3">
              <h2 className="text-sm font-medium text-fg-muted">Games</h2>
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                {cards.map((game) => (
                  <GameCard
                    key={game.game_id}
                    game={game}
                    guildId={guildId}
                    channelName={game.queue_channel_id ? channelName.get(game.queue_channel_id) : undefined}
                  />
                ))}
              </div>
            </section>
          )}

          <section className="space-y-3">
            <h2 className="text-sm font-medium text-fg-muted">Manage</h2>
            <div className="grid gap-4 sm:grid-cols-3">
              {SECTION_TILES.map((tile) => (
                <Card key={tile.slug} interactive className="group relative p-5">
                  <div className="flex items-start gap-4">
                    <div className="grid size-11 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
                      <Icon name={tile.icon} className="size-5" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <h3 className="font-semibold leading-tight">
                        <Link
                          href={`/dashboard/${guildId}/custom-matches/${tile.slug}`}
                          className="after:absolute after:inset-0 after:content-['']"
                        >
                          {tile.title}
                        </Link>
                      </h3>
                      <p className="mt-1 text-sm leading-relaxed text-fg-muted">{tile.description}</p>
                    </div>
                    <Icon
                      name="ArrowRight"
                      className="size-4 shrink-0 text-fg-subtle transition-transform duration-200 group-hover:translate-x-0.5"
                    />
                  </div>
                </Card>
              ))}
            </div>
          </section>

          {global && global.blacklist.length > 0 && (
            <p className="text-xs text-fg-subtle">
              {global.blacklist.length} member{global.blacklist.length === 1 ? "" : "s"} blacklisted ·
              managed under Global setup.
            </p>
          )}
        </>
      )}

      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">cogs/custommatch/</p>
    </div>
  );
}

function GameCard({
  game,
  guildId,
  channelName,
}: {
  game: CmGameCard;
  guildId: string;
  channelName?: string;
}) {
  const unhealthy = game.health.length > 0;
  return (
    <Card
      interactive
      className="group relative flex h-full flex-col gap-4 p-5"
    >
      <div className="flex items-start gap-3">
        <div
          className={
            "grid size-10 shrink-0 place-items-center rounded-lg transition-colors " +
            (game.enabled
              ? "bg-[var(--accent-soft)] text-[var(--accent)]"
              : "bg-[var(--surface-hover)] text-fg-subtle")
          }
        >
          <Icon name="Swords" className="size-5" />
        </div>
        <div className="min-w-0 flex-1">
          <h3 className="flex items-center gap-2 font-semibold leading-tight">
            <Link
              href={`/dashboard/${guildId}/custom-matches/${game.game_id}`}
              className="after:absolute after:inset-0 after:content-['']"
            >
              {game.name}
            </Link>
            {!game.enabled && <Badge variant="warning">Disabled</Badge>}
          </h3>
          <p className="mt-1 text-sm text-fg-muted">
            {game.player_count}-player · {game.queue_type.toUpperCase()}
            {channelName ? ` · #${channelName}` : ""}
          </p>
        </div>
      </div>

      <div className="mt-auto flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-fg-subtle">
        <span className="inline-flex items-center gap-1.5">
          <Icon name="Trophy" className="size-3.5" />
          {game.rank_count} rank{game.rank_count === 1 ? "" : "s"}
        </span>
        {game.queue_open ? (
          <span className="inline-flex items-center gap-1.5 text-[var(--success)]">
            <StatusDot active label="Queue open" />
            {game.queued} in queue
          </span>
        ) : (
          <span className="inline-flex items-center gap-1.5">
            <StatusDot active={false} label="Queue closed" />
            Queue closed
          </span>
        )}
        {unhealthy && (
          <span
            className="inline-flex items-center gap-1.5 text-[var(--warning)]"
            title={game.health.join("\n")}
          >
            <Icon name="AlertTriangle" className="size-3.5" />
            {game.health.length} to fix
          </span>
        )}
      </div>
    </Card>
  );
}
