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
import { useChannelItems, useRoleItems } from "@/components/providers/guild-provider";
import { ChannelType } from "@/lib/discord/types";
import { deleteMediaChannel, saveMediaChannel, setMediaEnabled } from "@/app/actions/utility";
import type { MediaInput } from "@/lib/utility/store";
import type { MediaChannel } from "@/lib/utility/types";

const CHANNEL_TYPES = [ChannelType.GuildText, ChannelType.GuildAnnouncement, ChannelType.GuildForum];

interface Draft {
  channelId: string;
  allowAttachments: boolean;
  allowLinks: boolean;
  allowEmbeds: boolean;
  allowStickers: boolean;
  bypassRoleIds: string[];
  threadEnabled: boolean;
  threadNameTemplate: string;
  threadArchiveMinutes: number;
  autoReact: string[];
  postCooldownSeconds: number;
  dmOnDelete: boolean;
}

function draftFrom(media: MediaChannel | null): Draft {
  return media
    ? {
        channelId: media.channelId,
        allowAttachments: media.allowAttachments,
        allowLinks: media.allowLinks,
        allowEmbeds: media.allowEmbeds,
        allowStickers: media.allowStickers,
        bypassRoleIds: media.bypassRoleIds,
        threadEnabled: media.threadEnabled,
        threadNameTemplate: media.threadNameTemplate,
        threadArchiveMinutes: media.threadArchiveMinutes,
        autoReact: media.autoReact,
        postCooldownSeconds: media.postCooldownSeconds,
        dmOnDelete: media.dmOnDelete,
      }
    : {
        channelId: "",
        allowAttachments: true,
        allowLinks: false,
        allowEmbeds: false,
        allowStickers: false,
        bypassRoleIds: [],
        threadEnabled: true,
        threadNameTemplate: "{user} - {date}",
        threadArchiveMinutes: 60,
        autoReact: [],
        postCooldownSeconds: 0,
        dmOnDelete: false,
      };
}

const toList = (text: string): string[] =>
  text.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean);

const ARCHIVE_OPTIONS = [
  { value: "60", label: "1 hour" },
  { value: "1440", label: "1 day" },
  { value: "4320", label: "3 days" },
  { value: "10080", label: "1 week" },
];

export function MediaEditor({ guildId, media }: { guildId: string; media: MediaChannel | null }) {
  const router = useRouter();
  const toast = useToast();
  const channelItems = useChannelItems(CHANNEL_TYPES);
  const roleItems = useRoleItems();

  const [initial, setInitial] = React.useState<Draft>(() => draftFrom(media));
  const [draft, setDraft] = React.useState<Draft>(initial);
  const [enabled, setEnabled] = React.useState(media?.enabled ?? true);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const id = media?.id ?? null;

  const dirty = React.useMemo(() => JSON.stringify(draft) !== JSON.stringify(initial), [draft, initial]);
  const set = <K extends keyof Draft>(key: K, val: Draft[K]) => setDraft((d) => ({ ...d, [key]: val }));

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const result = await saveMediaChannel(guildId, id, draft as MediaInput);
      if (result.ok) {
        setInitial(draft);
        toast.success("Saved", "Live within about 10 seconds.");
        if (!id && result.id) router.replace(`/dashboard/${guildId}/utility/media/${result.id}`);
      } else {
        setError(result.error ?? "Something went wrong.");
      }
    } catch {
      setError("Couldn't reach the server. Your changes are still here.");
    } finally {
      setSaving(false);
    }
  };

  const remove = async () => {
    if (!id) {
      router.push(`/dashboard/${guildId}/utility`);
      return;
    }
    if (!confirm("Remove this media-only rule?")) return;
    const result = await deleteMediaChannel(guildId, id);
    if (result.ok) {
      toast.success("Deleted");
      router.push(`/dashboard/${guildId}/utility`);
    } else {
      toast.error("Couldn't delete that", result.error);
    }
  };

  const allow = (key: keyof Draft, label: string, hint: string) => (
    <label className="flex items-start gap-3 text-sm">
      <Switch checked={draft[key] as boolean} onCheckedChange={(c) => set(key, c as Draft[typeof key])} />
      <span>
        <span className="font-medium">{label}</span>
        <span className="block text-xs text-fg-subtle">{hint}</span>
      </span>
    </label>
  );

  return (
    <div className="space-y-6 pb-28 lg:pb-6">
      <Card className="space-y-5 p-5 sm:p-6">
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label>Channel</Label>
            <EntityPicker
              items={channelItems}
              value={draft.channelId || null}
              onChange={(next) => set("channelId", next ?? "")}
              placeholder="Pick a channel"
              aria-label="Channel"
            />
            <FieldHint>Messages without allowed content are removed.</FieldHint>
          </div>
          {id && (
            <div className="space-y-2">
              <Label>Live</Label>
              <label className="flex h-9 items-center gap-2 text-sm text-fg-muted">
                <Switch
                  checked={enabled}
                  onCheckedChange={async (next) => {
                    setEnabled(next);
                    const r = await setMediaEnabled(guildId, id, next);
                    if (!r.ok) {
                      setEnabled(!next);
                      toast.error("Couldn't change that", r.error);
                    }
                  }}
                />
                {enabled ? "Enforcing" : "Switched off"}
              </label>
            </div>
          )}
        </div>

        <div className="space-y-3">
          <Label className="text-base">What&apos;s allowed</Label>
          {allow("allowAttachments", "Attachments", "Images, videos and files.")}
          {allow("allowLinks", "Links", "Messages that are just a URL.")}
          {allow("allowEmbeds", "Embeds", "Link previews and rich embeds.")}
          {allow("allowStickers", "Stickers", "Sticker-only messages.")}
        </div>
      </Card>

      <Card className="space-y-4 p-5 sm:p-6">
        <Label className="text-base">Discussion threads</Label>
        {allow("threadEnabled", "Open a thread under each post", "So people can talk about it without cluttering the channel.")}
        {draft.threadEnabled && (
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="media-thread-name">Thread name</Label>
              <Input
                id="media-thread-name"
                value={draft.threadNameTemplate}
                onChange={(e) => set("threadNameTemplate", e.target.value)}
              />
              <FieldHint>{"{user} and {date} are filled in."}</FieldHint>
            </div>
            <div className="space-y-2">
              <Label>Auto-archive after</Label>
              <EntityPicker
                items={ARCHIVE_OPTIONS}
                value={String(draft.threadArchiveMinutes)}
                onChange={(next) => set("threadArchiveMinutes", Number(next ?? 60))}
                clearable={false}
                aria-label="Archive duration"
              />
            </div>
          </div>
        )}
      </Card>

      <Card className="space-y-4 p-5 sm:p-6">
        <Label className="text-base">Fine tuning</Label>
        <div className="space-y-2">
          <Label>Roles that can post anything</Label>
          <EntityPicker
            multiple
            items={roleItems}
            value={draft.bypassRoleIds}
            onChange={(next) => set("bypassRoleIds", next)}
            placeholder="No exemptions"
            aria-label="Bypass roles"
          />
        </div>
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="media-react">Auto-react with</Label>
            <Input
              id="media-react"
              value={draft.autoReact.join(", ")}
              onChange={(e) => set("autoReact", toList(e.target.value))}
              placeholder="👍, ❤️"
            />
            <FieldHint>Added to each valid post.</FieldHint>
          </div>
          <div className="space-y-2">
            <Label htmlFor="media-cooldown">Cooldown between posts (seconds)</Label>
            <Input
              id="media-cooldown"
              type="number"
              min={0}
              value={draft.postCooldownSeconds}
              onChange={(e) => set("postCooldownSeconds", Math.max(0, Number(e.target.value) || 0))}
            />
          </div>
        </div>
        {allow("dmOnDelete", "DM people when their post is removed", "Tells them why, so it doesn't seem like the bot ate their message.")}
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
