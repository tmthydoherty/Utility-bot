"use client";

import * as React from "react";

import { cn, formatNumber, formatRelative } from "@/lib/utils";
import { lookupMemberEconomy, type MemberEconomy } from "@/app/actions/economy";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { EntityPicker, type PickerItem } from "@/components/ui/entity-picker";
import { Icon } from "@/components/ui/icon";
import { Skeleton } from "@/components/ui/skeleton";

/**
 * Look up any member's standing in the economy.
 *
 * Everything shown here is read straight from the bot's ledger and inventory —
 * total points, what they hold, what they've spent, and a day-by-day account of
 * how every point arrived. It's a viewer, not an editor: prices and payouts are
 * changed on the settings tabs, and this only reports what happened.
 *
 * The daily breakdown is a list, not a stacked chart, on purpose. Exact numbers
 * per source matter more here than comparing eight source colours across days,
 * so each day is text (source + amount) with a single-hue magnitude bar to show
 * at a glance which days were big ones.
 */

export interface InspectorMember {
  id: string;
  name: string;
  username: string;
  avatar: string;
}

export function MemberInspector({
  guildId,
  members,
}: {
  guildId: string;
  members: InspectorMember[];
}) {
  const [selected, setSelected] = React.useState<string | null>(null);
  const [data, setData] = React.useState<MemberEconomy | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [pending, startTransition] = React.useTransition();

  const items: PickerItem[] = React.useMemo(
    () =>
      members.map((member) => ({
        value: member.id,
        label: member.name,
        keywords: [member.username],
      })),
    [members],
  );

  const onSelect = (value: string | null) => {
    setSelected(value);
    setData(null);
    setError(null);
    if (!value) return;
    startTransition(async () => {
      const result = await lookupMemberEconomy(guildId, value);
      if (result.ok && result.data) setData(result.data);
      else setError(result.error ?? "Couldn't load that member.");
    });
  };

  return (
    <div className="space-y-6">
      <div className="max-w-md">
        <EntityPicker
          items={items}
          value={selected}
          onChange={onSelect}
          placeholder="Search for a member…"
          searchPlaceholder="Search by name…"
          emptyMessage="No members match that."
          aria-label="Choose a member to inspect"
        />
      </div>

      {pending && <InspectorSkeleton />}

      {!pending && error && (
        <Card>
          <EmptyState icon="ShieldAlert" title="Couldn't load that member" description={error} />
        </Card>
      )}

      {!pending && !error && !selected && (
        <Card>
          <EmptyState
            icon="Search"
            title="Pick a member to begin"
            description="Search above to see anyone's balance, the items they hold, and how they earned every point."
          />
        </Card>
      )}

      {!pending && !error && data && <MemberDetail data={data} />}
    </div>
  );
}

function InspectorSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-24 rounded-lg" />
        ))}
      </div>
      <Skeleton className="h-64 rounded-lg" />
    </div>
  );
}

function MemberDetail({ data }: { data: MemberEconomy }) {
  const maxDay = data.earningDays.reduce((max, day) => Math.max(max, day.total), 0);

  return (
    <div className="space-y-6">
      <Card className="flex items-center gap-4 p-5 sm:p-6">
        <Avatar src={data.avatar} name={data.name} size="lg" />
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <h2 className="truncate text-lg font-semibold">{data.name}</h2>
            {data.former && <Badge variant="neutral">Left the server</Badge>}
          </div>
          <p className="font-mono text-xs text-fg-subtle">{data.userId}</p>
        </div>
      </Card>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <MiniStat label="Balance" value={data.balance} icon="Coins" />
        <MiniStat label="Earned all-time" value={data.lifetime} icon="TrendingUp" />
        <MiniStat label="Items held" value={data.ownedCount} icon="Tag" />
        <MiniStat label="Items used" value={data.usedCount} icon="Check" />
      </div>

      <Card>
        <div className="space-y-1 p-5 pb-0 sm:p-6 sm:pb-0">
          <h3 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">
            How they earned points
          </h3>
          <p className="text-sm text-fg-muted">
            Every day they earned anything, newest first — {formatNumber(data.totalEarned)} points
            in all.
          </p>
        </div>
        {data.earningDays.length === 0 ? (
          <EmptyState
            icon="Coins"
            title="No points earned yet"
            description="Once this member starts chatting or joining in, the days they earn will show up here."
          />
        ) : (
          <ul className="divide-y divide-[var(--border)]">
            {data.earningDays.map((day) => (
              <li key={day.day} className="p-5 sm:px-6">
                <div className="flex items-baseline justify-between gap-3">
                  <span className="text-sm font-medium">{formatDay(day.day)}</span>
                  <span className="text-sm font-semibold tabular-nums text-[var(--accent)]">
                    +{formatNumber(day.total)}
                  </span>
                </div>
                {/* Single-hue magnitude bar: this day's total against the member's
                    biggest day. Text carries the exact numbers; the bar is only a
                    glanceable sense of scale. */}
                <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-[var(--surface-hover)]">
                  <div
                    className="h-full rounded-full bg-[var(--accent)]"
                    style={{ width: `${maxDay > 0 ? (day.total / maxDay) * 100 : 0}%` }}
                  />
                </div>
                <div className="mt-2.5 flex flex-wrap gap-x-4 gap-y-1">
                  {day.sources.map((source) => (
                    <span key={source.bucket} className="text-xs text-fg-muted">
                      {source.bucket}{" "}
                      <span className="tabular-nums text-fg-subtle">
                        +{formatNumber(source.points)}
                      </span>
                    </span>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        )}
      </Card>

      <div className="grid gap-6 lg:grid-cols-2">
        <ItemList
          title="Items held"
          empty="Nothing in their inventory right now."
          items={data.itemsOwned}
          tsLabel="bought"
        />
        <ItemList
          title="Items used"
          empty="They haven't used any items yet."
          items={data.itemsUsed}
          tsLabel="last used"
        />
      </div>

      {data.recentSpends.length > 0 && (
        <Card>
          <div className="p-5 pb-0 sm:p-6 sm:pb-0">
            <h3 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">
              Recent spending
            </h3>
          </div>
          <ul className="divide-y divide-[var(--border)]">
            {data.recentSpends.map((spend, i) => (
              <li
                key={`${spend.ts}-${i}`}
                className="flex items-center justify-between gap-3 p-4 sm:px-6"
              >
                <div className="min-w-0">
                  <p className="truncate text-sm">{spend.label}</p>
                  <p className="text-xs text-fg-subtle">{formatRelative(spend.ts * 1000)}</p>
                </div>
                <span className="shrink-0 text-sm font-semibold tabular-nums text-[var(--danger)]">
                  −{formatNumber(spend.amount)}
                </span>
              </li>
            ))}
          </ul>
        </Card>
      )}
    </div>
  );
}

function MiniStat({ label, value, icon }: { label: string; value: number; icon: string }) {
  return (
    <Card className="p-5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 space-y-1">
          <p className="truncate text-sm text-fg-muted">{label}</p>
          <p className="text-2xl font-semibold leading-none tracking-tight tabular-nums">
            {formatNumber(value)}
          </p>
        </div>
        <div className="grid size-9 shrink-0 place-items-center rounded-md bg-[var(--accent-soft)] text-[var(--accent)]">
          <Icon name={icon} className="size-[18px]" />
        </div>
      </div>
    </Card>
  );
}

function ItemList({
  title,
  empty,
  items,
  tsLabel,
}: {
  title: string;
  empty: string;
  items: { itemKey: string; label: string; count: number; lastTs: number }[];
  tsLabel: string;
}) {
  return (
    <Card className={cn(items.length === 0 && "flex flex-col")}>
      <div className="p-5 pb-0 sm:p-6 sm:pb-0">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">{title}</h3>
      </div>
      {items.length === 0 ? (
        <EmptyState icon="Tag" title={empty} />
      ) : (
        <ul className="divide-y divide-[var(--border)]">
          {items.map((item) => (
            <li
              key={item.itemKey}
              className="flex items-center justify-between gap-3 p-4 sm:px-6"
            >
              <div className="min-w-0">
                <p className="truncate text-sm font-medium">{item.label}</p>
                <p className="text-xs text-fg-subtle">
                  {tsLabel} {formatRelative(item.lastTs * 1000)}
                </p>
              </div>
              {item.count > 1 && (
                <Badge variant="neutral">
                  ×{item.count}
                </Badge>
              )}
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

/** `2026-08-09` → `Sat, 9 Aug 2026`, in UTC to match how the day was bucketed. */
function formatDay(day: string): string {
  const date = new Date(`${day}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return day;
  return new Intl.DateTimeFormat("en-GB", {
    weekday: "short",
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }).format(date);
}
