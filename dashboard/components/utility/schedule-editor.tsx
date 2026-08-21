"use client";

import { EntityPicker } from "@/components/ui/entity-picker";
import { Input, Label, FieldHint } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import type { Schedule, ScheduleFrequency } from "@/lib/utility/types";

const FREQUENCIES: { value: ScheduleFrequency; label: string }[] = [
  { value: "daily", label: "Every day" },
  { value: "weekly", label: "Every week" },
  { value: "biweekly", label: "Every two weeks" },
  { value: "monthly", label: "Every month" },
  { value: "every_x_days", label: "Every N days" },
];

// Monday-first, matching Python's weekday() (Mon=0 … Sun=6).
const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/** HH:MM local → HH:MM UTC, using today's offset (good enough away from the
 * DST seam, and the same approach the original cog took). */
function localToUtc(hhmm: string): string {
  const [h, m] = hhmm.split(":").map(Number);
  const d = new Date();
  d.setHours(h ?? 0, m ?? 0, 0, 0);
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

function utcToLocal(hhmm: string): string {
  const [h, m] = hhmm.split(":").map(Number);
  const d = new Date();
  d.setUTCHours(h ?? 0, m ?? 0, 0, 0);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

export function ScheduleEditor({
  value,
  onChange,
}: {
  value: Schedule;
  onChange: (schedule: Schedule) => void;
}) {
  const set = <K extends keyof Schedule>(key: K, val: Schedule[K]) => onChange({ ...value, [key]: val });

  const localTime = utcToLocal(value.timeUtc);
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;

  const toggleDay = (day: number) => {
    const next = value.daysOfWeek.includes(day)
      ? value.daysOfWeek.filter((d) => d !== day)
      : [...value.daysOfWeek, day].sort((a, b) => a - b);
    set("daysOfWeek", next);
  };

  return (
    <div className="space-y-5">
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="space-y-2">
          <Label>How often</Label>
          <EntityPicker
            items={FREQUENCIES.map((f) => ({ value: f.value, label: f.label }))}
            value={value.frequency}
            onChange={(next) => set("frequency", (next as ScheduleFrequency) ?? "weekly")}
            clearable={false}
            aria-label="Frequency"
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="sched-time">At (your time)</Label>
          <Input
            id="sched-time"
            type="time"
            value={localTime}
            onChange={(e) =>
              onChange({ ...value, timeUtc: localToUtc(e.target.value), timezone: tz })
            }
          />
          <FieldHint>{tz} — stored as {value.timeUtc} UTC.</FieldHint>
        </div>
      </div>

      {(value.frequency === "weekly" || value.frequency === "biweekly") && (
        <div className="space-y-2">
          <Label>On which days</Label>
          <div className="flex flex-wrap gap-2">
            {DAYS.map((label, day) => {
              const active = value.daysOfWeek.includes(day);
              return (
                <button
                  key={day}
                  type="button"
                  onClick={() => toggleDay(day)}
                  className={cn(
                    "rounded-md border px-3 py-1.5 text-sm transition-colors",
                    active
                      ? "border-transparent bg-[var(--accent)] text-white shadow-[var(--glow)]"
                      : "border-[var(--border)] text-fg-muted hover:bg-[var(--surface-hover)]",
                  )}
                  aria-pressed={active}
                >
                  {label}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {value.frequency === "monthly" && (
        <div className="max-w-[10rem] space-y-2">
          <Label htmlFor="sched-dom">Day of the month</Label>
          <Input
            id="sched-dom"
            type="number"
            min={1}
            max={31}
            value={value.dayOfMonth}
            onChange={(e) => set("dayOfMonth", Math.min(31, Math.max(1, Number(e.target.value) || 1)))}
          />
        </div>
      )}

      {value.frequency === "every_x_days" && (
        <div className="max-w-[10rem] space-y-2">
          <Label htmlFor="sched-interval">Every how many days</Label>
          <Input
            id="sched-interval"
            type="number"
            min={1}
            value={value.intervalDays}
            onChange={(e) => set("intervalDays", Math.max(1, Number(e.target.value) || 1))}
          />
        </div>
      )}
    </div>
  );
}
