"use client";

import * as React from "react";
import type { HistoryRecord } from "@/lib/game-poll/store";
import { Card } from "@/components/ui/card";
import { Users, Clock } from "lucide-react";

export function HistoryClient({ history }: { history: HistoryRecord[] }) {
  // Group by session date (roughly, round to nearest hour to group sessions on the same day)
  const sessionsMap = React.useMemo(() => {
    const map = new Map<string, HistoryRecord[]>();
    for (const record of history) {
      // Round timestamp to nearest 12 hours to group them logically
      const dateRounded = new Date(Math.round(record.session_date / (12 * 3600)) * (12 * 3600) * 1000);
      const dateKey = dateRounded.toLocaleDateString();
      
      if (!map.has(dateKey)) {
        map.set(dateKey, []);
      }
      map.get(dateKey)!.push(record);
    }
    return map;
  }, [history]);

  // Get up to 4 most recent unique dates
  const sortedDates = React.useMemo(() => {
    return Array.from(sessionsMap.keys()).sort((a, b) => new Date(b).getTime() - new Date(a).getTime());
  }, [sessionsMap]);
  
  const [filter, setFilter] = React.useState<string>(sortedDates.length > 0 ? (sortedDates[0] ?? "all") : "all");

  const displayData = React.useMemo(() => {
    if (filter === "all") {
      // Aggregate all time
      const agg = new Map<string, { name: string, count: number, duration: number }>();
      for (const record of history) {
        const id = record.user_id;
        if (!agg.has(id)) {
          agg.set(id, { name: record.user_name || "Unknown User", count: 0, duration: 0 });
        }
        const curr = agg.get(id)!;
        curr.count += 1;
        curr.duration += record.duration;
        // Keep most recent name
        if (record.user_name) curr.name = record.user_name;
      }
      return Array.from(agg.entries())
        .map(([id, data]) => ({ id, ...data }))
        .sort((a, b) => b.count - a.count || b.duration - a.duration);
    } else {
      // Specific date
      const records = sessionsMap.get(filter) || [];
      const agg = new Map<string, { name: string, count: number, duration: number }>();
      for (const record of records) {
        const id = record.user_id;
        if (!agg.has(id)) {
          agg.set(id, { name: record.user_name || "Unknown User", count: 0, duration: 0 });
        }
        const curr = agg.get(id)!;
        curr.count = 1; // It's one session
        curr.duration += record.duration;
        if (record.user_name) curr.name = record.user_name;
      }
      return Array.from(agg.entries())
        .map(([id, data]) => ({ id, ...data }))
        .sort((a, b) => b.duration - a.duration);
    }
  }, [filter, history, sessionsMap]);

  function formatDuration(sec: number) {
    if (sec < 60) return `${Math.round(sec)}s`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m`;
    const hr = Math.floor(min / 60);
    const rem = min % 60;
    return `${hr}h ${rem}m`;
  }

  return (
    <Card className="flex flex-col">
      <div className="flex flex-col gap-3 border-b border-[var(--border)] p-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-center gap-2 text-sm font-medium">
          <Users className="size-4 shrink-0 text-fg-muted" />
          <span className="truncate">
            {displayData.length} Participants {filter === "all" ? "(All Time)" : ""}
          </span>
        </div>

        <select
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="w-full shrink-0 rounded-md border border-[var(--border)] bg-bg-surface px-3 py-1.5 text-sm outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)] sm:w-[180px]"
        >
          {sortedDates.slice(0, 4).map(d => (
            <option key={d} value={d}>{d}</option>
          ))}
          <option value="all">All-Time Leaderboard</option>
        </select>
      </div>

      <div className="divide-y divide-[var(--border)]">
        {displayData.length === 0 ? (
          <div className="p-8 text-center text-sm text-fg-muted">
            No history recorded yet. Participants will appear here after a game night voice session ends.
          </div>
        ) : (
          displayData.map((user, idx) => (
            <div key={user.id} className="flex items-center justify-between gap-3 p-4 hover:bg-bg-hover transition-colors">
              <div className="flex min-w-0 items-center gap-3">
                {filter === "all" && (
                  <div className="flex size-6 shrink-0 items-center justify-center rounded bg-bg-surface-2 text-xs font-medium text-fg-muted">
                    #{idx + 1}
                  </div>
                )}
                <div className="truncate font-medium" title={user.name}>{user.name}</div>
              </div>
              <div className="flex shrink-0 items-center gap-3 text-sm text-fg-muted sm:gap-4">
                {/* Duration is only known for real recorded sessions; seeded/legacy
                    nights have none, so we simply omit it rather than show 0m. */}
                {user.duration > 0 && (
                  <div className="flex items-center justify-end gap-1.5 whitespace-nowrap">
                    <Clock className="size-3.5 shrink-0 opacity-70" />
                    <span>{formatDuration(user.duration)}</span>
                  </div>
                )}
                {filter === "all" && (
                  <span
                    className="inline-flex min-w-[2.75rem] justify-center rounded-md bg-bg-surface-2 px-2 py-0.5 text-xs font-semibold tabular-nums text-fg"
                    title={`Played ${user.count} game night${user.count === 1 ? "" : "s"}`}
                  >
                    {user.count}x
                  </span>
                )}
              </div>
            </div>
          ))
        )}
      </div>
    </Card>
  );
}
