"use client";

import * as React from "react";

import type { CmAnalytics, CmRivalry } from "@/lib/custommatch/read";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { StatTile } from "@/components/ui/stat-tile";

export interface GameAnalytics {
  id: number;
  name: string;
  enabled: boolean;
  analytics: CmAnalytics;
  rivalries: CmRivalry[];
}

export function AnalyticsView({
  games,
  names,
}: {
  games: GameAnalytics[];
  names: Record<string, string>;
}) {
  const [gameId, setGameId] = React.useState<number>(games[0]?.id ?? 0);
  const game = games.find((g) => g.id === gameId) ?? games[0];
  if (!game) return null;
  const a = game.analytics;

  return (
    <div className="space-y-6">
      {games.length > 1 && (
        <div className="flex flex-wrap gap-2">
          {games.map((g) => (
            <Button
              key={g.id}
              size="sm"
              variant={g.id === game.id ? "primary" : "outline"}
              onClick={() => setGameId(g.id)}
            >
              {g.name}
              {!g.enabled && <span className="ml-1 text-xs opacity-70">(off)</span>}
            </Button>
          ))}
        </div>
      )}

      <div className="grid gap-4 sm:grid-cols-3">
        <StatTile label="Matches played" value={a.totalMatches} icon="Swords" />
        <StatTile label="Ranked players" value={a.totalPlayers} icon="Users" />
        <StatTile label="Average MMR" value={a.avgMmr} icon="Gauge" />
      </div>

      <Card className="p-5">
        <h3 className="mb-1 text-sm font-semibold">Matches per day</h3>
        <p className="mb-4 text-xs text-fg-subtle">Decided matches over the last 30 days.</p>
        {a.matchesPerDay.length === 0 ? (
          <p className="text-sm text-fg-subtle">No matches in this window.</p>
        ) : (
          <Bars
            bars={a.matchesPerDay.map((d) => ({
              key: d.day,
              value: d.count,
              title: `${d.day}: ${d.count} match${d.count === 1 ? "" : "es"}`,
            }))}
          />
        )}
      </Card>

      <div className="grid gap-5 lg:grid-cols-2">
        <Card className="p-5">
          <h3 className="mb-1 text-sm font-semibold">Busiest hours</h3>
          <p className="mb-4 text-xs text-fg-subtle">When matches start, by hour (bot timezone).</p>
          <Bars
            bars={a.peakHours.map((count, hour) => ({
              key: String(hour),
              value: count,
              title: `${String(hour).padStart(2, "0")}:00 — ${count} match${count === 1 ? "" : "es"}`,
              label: hour % 6 === 0 ? String(hour) : undefined,
            }))}
          />
        </Card>

        <Card className="p-5">
          <h3 className="mb-1 text-sm font-semibold">MMR spread</h3>
          <p className="mb-4 text-xs text-fg-subtle">How players are distributed across the ladder.</p>
          {a.mmrHistogram.length === 0 ? (
            <p className="text-sm text-fg-subtle">No ranked players yet.</p>
          ) : (
            <Bars
              bars={a.mmrHistogram.map((b) => ({
                key: String(b.floor),
                value: b.count,
                title: `${b.floor}–${b.floor + 99}: ${b.count} player${b.count === 1 ? "" : "s"}`,
                label: b.floor % 500 === 0 ? String(b.floor) : undefined,
              }))}
              color="var(--accent)"
            />
          )}
        </Card>
      </div>

      <Card className="p-5">
        <h3 className="mb-1 text-sm font-semibold">Hottest rivalries</h3>
        <p className="mb-4 text-xs text-fg-subtle">The most-played head-to-heads in this game.</p>
        {game.rivalries.length === 0 ? (
          <EmptyState icon="Swords" title="No rivalries yet" description="They build up as members play each other." />
        ) : (
          <ul className="space-y-3">
            {game.rivalries.map((r) => (
              <RivalryRow
                key={`${r.player_a_id}-${r.player_b_id}`}
                rivalry={r}
                names={names}
              />
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}

function Bars({
  bars,
  color = "var(--fg-muted)",
}: {
  bars: { key: string; value: number; title: string; label?: string }[];
  color?: string;
}) {
  const max = Math.max(1, ...bars.map((b) => b.value));
  return (
    <div>
      <div className="flex h-28 items-end gap-[3px]">
        {bars.map((b) => (
          <div
            key={b.key}
            className="group relative flex-1 rounded-t-sm transition-colors"
            style={{ height: `${Math.max(2, (b.value / max) * 100)}%`, backgroundColor: color, opacity: 0.85 }}
            title={b.title}
          />
        ))}
      </div>
      <div className="mt-1.5 flex gap-[3px]">
        {bars.map((b) => (
          <div key={b.key} className="flex-1 text-center text-[10px] text-fg-subtle">
            {b.label ?? ""}
          </div>
        ))}
      </div>
    </div>
  );
}

function RivalryRow({ rivalry, names }: { rivalry: CmRivalry; names: Record<string, string> }) {
  const a = names[rivalry.player_a_id] ?? `Member ${rivalry.player_a_id}`;
  const b = names[rivalry.player_b_id] ?? `Member ${rivalry.player_b_id}`;
  const total = rivalry.a_wins + rivalry.b_wins;
  const aPct = total > 0 ? (rivalry.a_wins / total) * 100 : 50;
  const leader = rivalry.a_wins === rivalry.b_wins ? null : rivalry.a_wins > rivalry.b_wins ? a : b;

  return (
    <li className="space-y-1.5">
      <div className="flex items-center justify-between gap-3 text-sm">
        <span className="min-w-0 truncate font-medium">{a}</span>
        <span className="shrink-0 tabular-nums text-fg-muted">
          {rivalry.a_wins} – {rivalry.b_wins}
        </span>
        <span className="min-w-0 truncate text-right font-medium">{b}</span>
      </div>
      <div className="flex h-1.5 overflow-hidden rounded-full bg-[var(--danger-soft)]">
        <div className="bg-[var(--success)]" style={{ width: `${aPct}%` }} />
      </div>
      {leader && (
        <p className="text-xs text-fg-subtle">
          {leader} leads{total > 0 ? ` · ${total} games` : ""}
        </p>
      )}
    </li>
  );
}
