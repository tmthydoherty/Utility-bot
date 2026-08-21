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
import { deleteSticky, saveSticky, setStickyEnabled } from "@/app/actions/utility";
import type { StickyInput } from "@/lib/utility/store";
import { emptyEmbed, type Sticky } from "@/lib/utility/types";

import { MessageComposer, type MessageContent } from "./message-composer";

const STICKY_TYPES = [
  ChannelType.GuildText,
  ChannelType.GuildAnnouncement,
  ChannelType.GuildVoice,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
];

interface Draft extends MessageContent {
  channelId: string;
  name: string;
  minIntervalSeconds: number;
}

function draftFrom(sticky: Sticky | null): Draft {
  if (!sticky) {
    return {
      channelId: "",
      name: "",
      minIntervalSeconds: 30,
      plainText: false,
      content: "",
      pingRoleId: null,
      embed: emptyEmbed(),
      buttons: [],
      reactionRole: null,
    };
  }
  return {
    channelId: sticky.channelId,
    name: sticky.name,
    minIntervalSeconds: sticky.minIntervalSeconds,
    plainText: sticky.plainText,
    content: sticky.content,
    pingRoleId: sticky.pingRoleId,
    embed: sticky.embed,
    buttons: sticky.buttons,
    reactionRole: sticky.reactionRole,
  };
}

function toInput(draft: Draft): StickyInput {
  return {
    channelId: draft.channelId,
    name: draft.name,
    plainText: draft.plainText,
    embed: draft.embed,
    content: draft.content,
    pingRoleId: draft.pingRoleId,
    buttons: draft.buttons,
    reactionRole: draft.reactionRole,
    minIntervalSeconds: draft.minIntervalSeconds,
  };
}

export function StickyEditor({ guildId, sticky }: { guildId: string; sticky: Sticky | null }) {
  const router = useRouter();
  const toast = useToast();
  const channelItems = useChannelItems(STICKY_TYPES);

  const [initial, setInitial] = React.useState<Draft>(() => draftFrom(sticky));
  const [draft, setDraft] = React.useState<Draft>(initial);
  const [enabled, setEnabled] = React.useState(sticky?.enabled ?? true);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const id = sticky?.id ?? null;

  const dirty = React.useMemo(() => JSON.stringify(draft) !== JSON.stringify(initial), [draft, initial]);
  const set = <K extends keyof Draft>(key: K, val: Draft[K]) => setDraft((d) => ({ ...d, [key]: val }));

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const result = await saveSticky(guildId, id, toInput(draft));
      if (result.ok) {
        setInitial(draft);
        toast.success("Saved", "Live within about 10 seconds.");
        if (!id && result.id) router.replace(`/dashboard/${guildId}/utility/stickies/${result.id}`);
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
    const result = await setStickyEnabled(guildId, id, next);
    if (!result.ok) {
      setEnabled(!next);
      toast.error("Couldn't change that", result.error);
    }
  };

  const remove = async () => {
    if (!id) {
      router.push(`/dashboard/${guildId}/utility`);
      return;
    }
    if (!confirm("Delete this sticky? This can't be undone.")) return;
    const result = await deleteSticky(guildId, id);
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
            <Label htmlFor="sticky-name">Name</Label>
            <Input
              id="sticky-name"
              value={draft.name}
              maxLength={100}
              onChange={(e) => set("name", e.target.value)}
              placeholder="For your own reference"
            />
          </div>
          {id && (
            <div className="space-y-2">
              <Label>Live</Label>
              <label className="flex h-9 items-center gap-2 text-sm text-fg-muted">
                <Switch checked={enabled} onCheckedChange={toggleEnabled} />
                {enabled ? "Sticking to the bottom" : "Switched off"}
              </label>
            </div>
          )}
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label>Channel</Label>
            <EntityPicker
              items={channelItems}
              value={draft.channelId || null}
              onChange={(next) => set("channelId", next ?? "")}
              placeholder="Pick a channel"
              aria-label="Sticky channel"
            />
            <FieldHint>One sticky per channel.</FieldHint>
          </div>
          <div className="space-y-2">
            <Label htmlFor="sticky-interval">Minimum seconds between reposts</Label>
            <Input
              id="sticky-interval"
              type="number"
              min={0}
              value={draft.minIntervalSeconds}
              onChange={(e) => set("minIntervalSeconds", Math.max(0, Number(e.target.value) || 0))}
            />
            <FieldHint>Stops a busy channel from strobing the sticky.</FieldHint>
          </div>
        </div>
      </Card>

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
