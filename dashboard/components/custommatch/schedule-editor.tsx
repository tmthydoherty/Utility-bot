"use client";

import * as React from "react";

import type { ScheduleTimes } from "@/lib/custommatch/read";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";

/**
 * A controlled weekly open/close grid for a game's queue schedule.
 *
 * The value is the same `schedule_times` object the bot stores — weekday index
 * (Mon=0 … Sun=6) → `{ open, close }` in 24h "HH:MM". A day with no entry is
 * closed. It's a plain controlled input: edits flow up through `onChange` and
 * are saved with the rest of the game's settings through the SaveBar, so there's
 * no separate "apply" here.
 */

const DAYS = [
  { key: "0", label: "Monday" },
  { key: "1", label: "Tuesday" },
  { key: "2", label: "Wednesday" },
  { key: "3", label: "Thursday" },
  { key: "4", label: "Friday" },
  { key: "5", label: "Saturday" },
  { key: "6", label: "Sunday" },
];

export function ScheduleEditor({
  value,
  onChange,
  disabled,
}: {
  value: ScheduleTimes | null;
  onChange: (value: ScheduleTimes | null) => void;
  disabled?: boolean;
}) {
  const times = value ?? {};

  const setDay = (key: string, next: { open: string; close: string } | null) => {
    const draft: ScheduleTimes = { ...times };
    if (next) draft[key] = next;
    else delete draft[key];
    onChange(Object.keys(draft).length > 0 ? draft : null);
  };

  return (
    <div className="space-y-2">
      {DAYS.map((day) => {
        const entry = times[day.key];
        const open = Boolean(entry);
        return (
          <div
            key={day.key}
            className="flex flex-wrap items-center gap-3 rounded-lg border border-[var(--border)] p-3"
          >
            <div className="flex w-32 items-center gap-3">
              <Switch
                checked={open}
                disabled={disabled}
                onCheckedChange={(checked) =>
                  setDay(day.key, checked ? { open: "18:00", close: "23:00" } : null)
                }
                aria-label={`${day.label} open`}
              />
              <span className={open ? "text-sm font-medium" : "text-sm text-fg-subtle"}>
                {day.label}
              </span>
            </div>
            {entry ? (
              <div className="flex items-center gap-2 text-sm text-fg-muted">
                <Input
                  type="time"
                  value={entry.open}
                  disabled={disabled}
                  onChange={(e) => setDay(day.key, { open: e.target.value, close: entry.close })}
                  className="w-32"
                  aria-label={`${day.label} opens`}
                />
                <span>to</span>
                <Input
                  type="time"
                  value={entry.close}
                  disabled={disabled}
                  onChange={(e) => setDay(day.key, { open: entry.open, close: e.target.value })}
                  className="w-32"
                  aria-label={`${day.label} closes`}
                />
              </div>
            ) : (
              <span className="text-sm text-fg-subtle">Closed</span>
            )}
          </div>
        );
      })}
      <p className="text-xs text-fg-subtle">
        Times are in the bot&apos;s timezone. A closed day means the queue never auto-opens then.
      </p>
    </div>
  );
}
