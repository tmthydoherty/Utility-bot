import type { Metadata } from "next";
import Link from "next/link";

import { readEconomyTotals } from "@/lib/bot/economy-stats";
import { ECONOMY_TABS, ECONOMY_AUDIT_TILE } from "@/lib/economy/tabs";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";
import { StatTile } from "@/components/ui/stat-tile";

export const metadata: Metadata = { title: "Economy" };

/** The four tiles, in the order they sit on the page. */
const TILES = [
  ...ECONOMY_TABS.map((tab) => ({
    slug: tab.slug,
    title: tab.title,
    description: tab.description,
    icon: tab.icon,
  })),
  { ...ECONOMY_AUDIT_TILE },
];

export default async function EconomyPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  const totals = readEconomyTotals();

  return (
    <div className="space-y-6">
      <PageHeader
        title="Economy"
        description="Points, the shop, and a look at any member's balance. Values here are read-only until you click a section's pencil, so nothing moves by accident."
      />

      {totals && (
        <div className="grid gap-4 sm:grid-cols-3">
          <StatTile label="Members with points" value={totals.holders} icon="Users" />
          <StatTile label="Points in circulation" value={totals.pointsInCirculation} icon="Coins" />
          <StatTile label="Items held" value={totals.itemsOwned} icon="Tag" />
        </div>
      )}

      <div className="grid gap-4 sm:grid-cols-2">
        {TILES.map((tile) => (
          <Card key={tile.slug} interactive className="group relative p-5 sm:p-6">
            <div className="flex items-start gap-4">
              <div className="grid size-11 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
                <Icon name={tile.icon} className="size-5" />
              </div>
              <div className="min-w-0 flex-1">
                <h2 className="font-semibold leading-tight">
                  <Link
                    href={`/dashboard/${guildId}/economy/${tile.slug}`}
                    className="after:absolute after:inset-0 after:content-['']"
                  >
                    {tile.title}
                  </Link>
                </h2>
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
    </div>
  );
}
