"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EntityPicker } from "@/components/ui/entity-picker";
import { Input, Label, FieldHint } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { useChannelItems } from "@/components/providers/guild-provider";
import { ChannelType } from "@/lib/discord/types";
import {
  deleteReminder,
  saveReminder,
  setReminderEnabled,
} from "@/app/actions/utility";
import type { ReminderInput } from "@/lib/utility/store";
import {
  emptyEmbed,
  emptySchedule,
  type Reminder,
  type ReminderKind,
} from "@/lib/utility/types";

import { MessageComposer, type MessageContent } from "./message-composer";
import { ScheduleEditor } from "./schedule-editor";

const POST_TYPES = [
  ChannelType.GuildText,
  ChannelType.GuildAnnouncement,
  ChannelType.GuildVoice,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
  ChannelType.AnnouncementThread,
];

const KINDS: { value: ReminderKind; label: string; hint: string }[] = [
  { value: "scheduled", label: "On a schedule", hint: "Posts on a repeating schedule — daily, weekly, monthly." },
  { value: "interval", label: "Every so often", hint: "Posts on a repeat every N minutes/hours/days." },
  { value: "oneoff", label: "One-off message", hint: "Posts once. You can edit it in place afterwards." },
];

interface Draft extends MessageContent {
  kind: ReminderKind;
  name: string;
  channelIds: string[];
  useTimestamp: boolean;
  deletePrevious: boolean;
  intervalSeconds: number;
  schedule: ReturnType<typeof emptySchedule>;
}

function draftFrom(reminder: Reminder | null): Draft {
  if (!reminder) {
    return {
      kind: "scheduled",
      name: "",
      channelIds: [],
      plainText: false,
      content: "",
      pingRoleId: null,
      embed: emptyEmbed(),
      buttons: [],
      reactionRole: null,
      useTimestamp: false,
      deletePrevious: false,
      intervalSeconds: 3600,
      schedule: emptySchedule(),
    };
  }
  return {
    kind: reminder.kind,
    name: reminder.name,
    channelIds: reminder.channelIds,
    plainText: reminder.plainText,
    content: reminder.content,
    pingRoleId: reminder.pingRoleId,
    embed: reminder.embed,
    buttons: reminder.buttons,
    reactionRole: reminder.reactionRole,
    useTimestamp: reminder.useTimestamp,
    deletePrevious: reminder.deletePrevious,
    intervalSeconds: reminder.intervalSeconds || 3600,
    schedule: reminder.schedule ?? emptySchedule(),
  };
}

function toInput(draft: Draft): ReminderInput {
  return {
    kind: draft.kind,
    name: draft.name,
    plainText: draft.plainText,
    embed: draft.embed,
    content: draft.content,
    pingRoleId: draft.pingRoleId,
    channelIds: draft.channelIds,
    buttons: draft.buttons,
    reactionRole: draft.reactionRole,
    useTimestamp: draft.useTimestamp,
    deletePrevious: draft.deletePrevious,
    intervalSeconds: draft.intervalSeconds,
    schedule: draft.kind === "scheduled" ? draft.schedule : null,
  };
}

// Interval as a number + unit, for a friendlier control than raw seconds.
const UNITS: { value: string; label: string; seconds: number }[] = [
  { value: "minutes", label: "minutes", seconds: 60 },
  { value: "hours", label: "hours", seconds: 3600 },
  { value: "days", label: "days", seconds: 86400 },
];

export function ReminderEditor({
  guildId,
  reminder,
}: {
  guildId: string;
  reminder: Reminder | null;
}) {
  const router = useRouter();
  const toast = useToast();
  const channelItems = useChannelItems(POST_TYPES);

  const [initial, setInitial] = React.useState<Draft>(() => draftFrom(reminder));
  const [draft, setDraft] = React.useState<Draft>(initial);
  const [enabled, setEnabled] = React.useState(reminder?.enabled ?? false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const id = reminder?.id ?? null;

  const dirty = React.useMemo(() => JSON.stringify(draft) !== JSON.stringify(initial), [draft, initial]);
  const set = <K extends keyof Draft>(key: K, val: Draft[K]) => setDraft((d) => ({ ...d, [key]: val }));

  const bestUnit = React.useMemo(() => {
    const s = draft.intervalSeconds;
    if (s % 86400 === 0) return "days";
    if (s % 3600 === 0) return "hours";
    return "minutes";
  }, [draft.intervalSeconds]);
  const unitSeconds = UNITS.find((u) => u.value === bestUnit)!.seconds;

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const result = await saveReminder(guildId, id, toInput(draft));
      if (result.ok) {
        setInitial(draft);
        toast.success("Saved", "Live within about 10 seconds.");
        if (!id && result.id) {
          router.replace(`/dashboard/${guildId}/utility/reminders/${result.id}`);
        }
      } else {
        setError(result.error ?? "Something went wrong.");
      }
    } catch {
      setError("Couldn't reach the server. Your changes are still here.");
    } finally {
      setSaving(false);
    }
  };

  const toggleEnabled = async (next: boolean) => {
    if (!id) return;
    setEnabled(next);
    const result = await setReminderEnabled(guildId, id, next);
    if (!result.ok) {
      setEnabled(!next);
      toast.error("Couldn't change that", result.error);
    } else {
      toast.success(next ? "Switched on" : "Switched off");
    }
  };

  const remove = async () => {
    if (!id) {
      router.push(`/dashboard/${guildId}/utility`);
      return;
    }
    if (!confirm(`Delete "${draft.name || "this reminder"}"? This can't be undone.`)) return;
    const result = await deleteReminder(guildId, id);
    if (result.ok) {
      toast.success("Deleted");
      router.push(`/dashboard/${guildId}/utility`);
    } else {
      toast.error("Couldn't delete that", result.error);
    }
  };

  return (
    <div className="space-y-6 pb-28 lg:pb-6">
      <Card className="space-y-5 p-5 sm:p-6">
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="rem-name">Name</Label>
            <Input
              id="rem-name"
              value={draft.name}
              maxLength={100}
              onChange={(e) => set("name", e.target.value)}
              placeholder="What this is, for your own reference"
            />
          </div>
          {id && (
            <div className="space-y-2">
              <Label>Live</Label>
              <label className="flex h-9 items-center gap-2 text-sm text-fg-muted">
                <Switch checked={enabled} onCheckedChange={toggleEnabled} />
                {enabled ? "Posting on schedule" : "Switched off"}
              </label>
            </div>
          )}
        </div>

        <div className="space-y-2">
          <Label>Type</Label>
          <div className="grid gap-2 sm:grid-cols-3">
            {KINDS.map((k) => (
              <button
                key={k.value}
                type="button"
                onClick={() => set("kind", k.value)}
                className={
                  "rounded-lg border p-3 text-left text-sm transition-colors " +
                  (draft.kind === k.value
                    ? "border-[var(--accent)] bg-[var(--surface-hover)]"
                    : "border-[var(--border)] hover:bg-[var(--surface-hover)]")
                }
                aria-pressed={draft.kind === k.value}
              >
                <div className="font-medium">{k.label}</div>
                <div className="mt-0.5 text-xs text-fg-subtle">{k.hint}</div>
              </button>
            ))}
          </div>
        </div>

        <div className="space-y-2">
          <Label>Post in</Label>
          <EntityPicker
            multiple
            items={channelItems}
            value={draft.channelIds}
            onChange={(next) => set("channelIds", next)}
            placeholder="Pick one or more channels"
            aria-label="Channels to post in"
          />
        </div>
      </Card>

      {draft.kind === "scheduled" && (
        <Card className="space-y-4 p-5 sm:p-6">
          <Label className="text-base">Schedule</Label>
          <ScheduleEditor value={draft.schedule} onChange={(schedule) => set("schedule", schedule)} />
        </Card>
      )}

      {draft.kind === "interval" && (
        <Card className="space-y-3 p-5 sm:p-6">
          <Label className="text-base">Repeat every</Label>
          <div className="flex items-center gap-3">
            <Input
              type="number"
              min={1}
              className="max-w-[8rem]"
              value={Math.max(1, Math.round(draft.intervalSeconds / unitSeconds))}
              onChange={(e) =>
                set("intervalSeconds", Math.max(1, Number(e.target.value) || 1) * unitSeconds)
              }
            />
            <div className="w-40">
              <EntityPicker
                items={UNITS.map((u) => ({ value: u.value, label: u.label }))}
                value={bestUnit}
                onChange={(next) => {
                  const seconds = UNITS.find((u) => u.value === next)?.seconds ?? 3600;
                  const count = Math.max(1, Math.round(draft.intervalSeconds / unitSeconds));
                  set("intervalSeconds", count * seconds);
                }}
                clearable={false}
                aria-label="Interval unit"
              />
            </div>
          </div>
          <FieldHint>The first post goes out on the next check after you switch it on.</FieldHint>
        </Card>
      )}

      <Card className="space-y-5 p-5 sm:p-6">
        <Label className="text-base">Message</Label>
        <MessageComposer
          value={{
            plainText: draft.plainText,
            content: draft.content,
            pingRoleId: draft.pingRoleId,
            embed: draft.embed,
            buttons: draft.buttons,
            reactionRole: draft.reactionRole,
          }}
          onChange={(msg) => setDraft((d) => ({ ...d, ...msg }))}
        />
      </Card>

      <Card className="space-y-4 p-5 sm:p-6">
        <Label className="text-base">Options</Label>
        <label className="flex items-start gap-3 text-sm">
          <Switch checked={draft.useTimestamp} onCheckedChange={(c) => set("useTimestamp", c)} />
          <span>
            <span className="font-medium">Show when it was posted</span>
            <span className="block text-xs text-fg-subtle">Adds a “Posted: …” line to the message.</span>
          </span>
        </label>
        {draft.kind !== "oneoff" && (
          <label className="flex items-start gap-3 text-sm">
            <Switch checked={draft.deletePrevious} onCheckedChange={(c) => set("deletePrevious", c)} />
            <span>
              <span className="font-medium">Delete the previous post first</span>
              <span className="block text-xs text-fg-subtle">
                Keeps only the latest copy in the channel — needs Manage Messages.
              </span>
            </span>
          </label>
        )}
      </Card>

      <div className="flex justify-between">
        <Button variant="danger" size="sm" onClick={remove}>
          <Trash2 aria-hidden /> {id ? "Delete" : "Discard"}
        </Button>
      </div>

      <SaveBar
        visible={dirty || saving}
        saving={saving}
        error={error}
        changeCount={dirty ? 1 : 0}
        onSave={save}
        onReset={() => setDraft(initial)}
      />
    </div>
  );
}
