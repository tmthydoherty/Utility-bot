"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Plus, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EntityPicker } from "@/components/ui/entity-picker";
import { Input, Textarea, Label, FieldHint } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { useChannelItems, useRoleItems } from "@/components/providers/guild-provider";
import { EmbedBuilder } from "@/components/utility/embed-builder";
import { ChannelType } from "@/lib/discord/types";
import { embedFromJson } from "@/lib/utility/types";
import { deleteCommand, saveCommand, setCommandEnabled } from "@/app/actions/custom-commands";
import type { CommandInput } from "@/lib/custom-commands/store";
import type {
  CooldownScope,
  CustomCommand,
  Delivery,
  Embed,
  MatchType,
} from "@/lib/custom-commands/types";

const CHANNEL_TYPES = [
  ChannelType.GuildText,
  ChannelType.GuildAnnouncement,
  ChannelType.GuildVoice,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
  ChannelType.GuildForum,
];

const MATCH_TYPES: { value: MatchType; label: string }[] = [
  { value: "exact", label: "Exact — !name" },
  { value: "startswith", label: "Starts with the word" },
  { value: "contains", label: "Contains the word" },
];

const DELIVERIES: { value: Delivery; label: string }[] = [
  { value: "channel", label: "In the channel" },
  { value: "reply", label: "As a reply" },
  { value: "dm", label: "In a DM" },
];

const SCOPES: { value: CooldownScope; label: string }[] = [
  { value: "user", label: "Per person" },
  { value: "channel", label: "Per channel" },
  { value: "guild", label: "Whole server" },
];

interface Draft {
  name: string;
  matchType: MatchType;
  responses: string[];
  plainText: boolean;
  embed: Embed;
  delivery: Delivery;
  deleteTrigger: boolean;
  reactEmoji: string;
  allowedRoleIds: string[];
  deniedRoleIds: string[];
  allowedChannelIds: string[];
  deniedChannelIds: string[];
  cooldownSeconds: number;
  cooldownScope: CooldownScope;
}

function draftFrom(command: CustomCommand | null): Draft {
  return command
    ? {
        name: command.name,
        matchType: command.matchType,
        responses: command.responses.length ? command.responses : [""],
        plainText: command.plainText,
        embed: command.embed,
        delivery: command.delivery,
        deleteTrigger: command.deleteTrigger,
        reactEmoji: command.reactEmoji,
        allowedRoleIds: command.allowedRoleIds,
        deniedRoleIds: command.deniedRoleIds,
        allowedChannelIds: command.allowedChannelIds,
        deniedChannelIds: command.deniedChannelIds,
        cooldownSeconds: command.cooldownSeconds,
        cooldownScope: command.cooldownScope,
      }
    : {
        name: "",
        matchType: "exact",
        responses: [""],
        plainText: true,
        embed: embedFromJson({}),
        delivery: "channel",
        deleteTrigger: false,
        reactEmoji: "",
        allowedRoleIds: [],
        deniedRoleIds: [],
        allowedChannelIds: [],
        deniedChannelIds: [],
        cooldownSeconds: 0,
        cooldownScope: "user",
      };
}

function toInput(draft: Draft): CommandInput {
  return {
    name: draft.name,
    matchType: draft.matchType,
    responses: draft.responses.map((r) => r.trim()).filter(Boolean),
    plainText: draft.plainText,
    embed: draft.embed,
    delivery: draft.delivery,
    deleteTrigger: draft.deleteTrigger,
    reactEmoji: draft.reactEmoji.trim(),
    allowedRoleIds: draft.allowedRoleIds,
    deniedRoleIds: draft.deniedRoleIds,
    allowedChannelIds: draft.allowedChannelIds,
    deniedChannelIds: draft.deniedChannelIds,
    cooldownSeconds: draft.cooldownSeconds,
    cooldownScope: draft.cooldownScope,
  };
}

export function CommandEditor({ guildId, command }: { guildId: string; command: CustomCommand | null }) {
  const router = useRouter();
  const toast = useToast();
  const roleItems = useRoleItems();
  const channelItems = useChannelItems(CHANNEL_TYPES);

  const [initial, setInitial] = React.useState<Draft>(() => draftFrom(command));
  const [draft, setDraft] = React.useState<Draft>(initial);
  const [enabled, setEnabled] = React.useState(command?.enabled ?? false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const id = command?.id ?? null;

  const dirty = React.useMemo(() => JSON.stringify(draft) !== JSON.stringify(initial), [draft, initial]);
  const set = <K extends keyof Draft>(key: K, val: Draft[K]) => setDraft((d) => ({ ...d, [key]: val }));

  const setResponse = (index: number, value: string) =>
    set("responses", draft.responses.map((r, i) => (i === index ? value : r)));
  const addResponse = () => {
    if (draft.responses.length >= 25) return;
    set("responses", [...draft.responses, ""]);
  };
  const removeResponse = (index: number) =>
    set("responses", draft.responses.length > 1 ? draft.responses.filter((_, i) => i !== index) : draft.responses);

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const result = await saveCommand(guildId, id, toInput(draft));
      if (result.ok) {
        setInitial(draft);
        toast.success("Saved", "Live within about 10 seconds.");
        if (!id && result.id) router.replace(`/dashboard/${guildId}/custom-commands/${result.id}`);
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
      router.push(`/dashboard/${guildId}/custom-commands`);
      return;
    }
    if (!confirm("Delete this command?")) return;
    const result = await deleteCommand(guildId, id);
    if (result.ok) {
      toast.success("Deleted");
      router.push(`/dashboard/${guildId}/custom-commands`);
    } else {
      toast.error("Couldn't delete that", result.error);
    }
  };

  const namePrefix = draft.matchType === "exact" ? "!" : "";

  return (
    <div className="space-y-6 pb-28 lg:pb-6">
      {/* Trigger */}
      <Card className="space-y-5 p-5 sm:p-6">
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="cc-name">Trigger word</Label>
            <div className="flex items-center gap-2">
              {namePrefix && <span className="text-fg-subtle">{namePrefix}</span>}
              <Input
                id="cc-name"
                value={draft.name}
                maxLength={32}
                onChange={(e) => set("name", e.target.value)}
                placeholder="apple"
              />
            </div>
            <FieldHint>Letters, numbers and underscores. Saved lowercased, without the !.</FieldHint>
          </div>
          <div className="space-y-2">
            <Label>How it triggers</Label>
            <EntityPicker
              items={MATCH_TYPES}
              value={draft.matchType}
              onChange={(next) => set("matchType", (next as MatchType) ?? "exact")}
              clearable={false}
              aria-label="Match type"
            />
            <FieldHint>
              {draft.matchType === "exact"
                ? "Fires when someone types !word."
                : draft.matchType === "startswith"
                  ? "Fires when a message begins with the word — no ! needed."
                  : "Fires when a message contains the word anywhere."}
            </FieldHint>
          </div>
        </div>

        {id && (
          <div className="space-y-2">
            <Label>Live</Label>
            <label className="flex h-9 items-center gap-2 text-sm text-fg-muted">
              <Switch
                checked={enabled}
                onCheckedChange={async (next) => {
                  setEnabled(next);
                  const r = await setCommandEnabled(guildId, id, next);
                  if (!r.ok) {
                    setEnabled(!next);
                    toast.error("Couldn't change that", r.error);
                  }
                }}
              />
              {enabled ? "Responding" : "Switched off"}
            </label>
          </div>
        )}
      </Card>

      {/* Responses */}
      <Card className="space-y-5 p-5 sm:p-6">
        <div className="flex items-center justify-between">
          <div>
            <Label className="text-base">Responses</Label>
            <FieldHint>One is picked at random each time. Placeholders and scripting are expanded.</FieldHint>
          </div>
          <Switch
            checked={!draft.plainText}
            onCheckedChange={(checked) => set("plainText", !checked)}
            aria-label="Send a rich embed"
          />
        </div>

        {draft.plainText ? (
          <div className="space-y-3">
            {draft.responses.map((response, index) => (
              <div key={index} className="flex items-start gap-2">
                <Textarea
                  value={response}
                  rows={2}
                  maxLength={2000}
                  onChange={(e) => setResponse(index, e.target.value)}
                  placeholder="What the bot says. e.g. {random:🍎|🍏} for {user}!"
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => removeResponse(index)}
                  disabled={draft.responses.length <= 1}
                  aria-label="Remove response"
                >
                  <Trash2 aria-hidden />
                </Button>
              </div>
            ))}
            <Button type="button" variant="ghost" size="sm" onClick={addResponse} disabled={draft.responses.length >= 25}>
              <Plus aria-hidden /> Add another response
            </Button>
          </div>
        ) : (
          <EmbedBuilder value={draft.embed} onChange={(embed) => set("embed", embed)} />
        )}

        <PlaceholderLegend />
      </Card>

      {/* Delivery */}
      <Card className="space-y-5 p-5 sm:p-6">
        <Label className="text-base">Delivery</Label>
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label>Where the reply lands</Label>
            <EntityPicker
              items={DELIVERIES}
              value={draft.delivery}
              onChange={(next) => set("delivery", (next as Delivery) ?? "channel")}
              clearable={false}
              aria-label="Delivery"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="cc-react">React to the trigger (optional)</Label>
            <Input
              id="cc-react"
              value={draft.reactEmoji}
              onChange={(e) => set("reactEmoji", e.target.value)}
              placeholder="🎉 or <:name:id>"
            />
          </div>
        </div>
        <label className="flex items-center gap-3 text-sm">
          <Switch checked={draft.deleteTrigger} onCheckedChange={(c) => set("deleteTrigger", c)} />
          Delete the message that triggered it
        </label>
      </Card>

      {/* Who & where */}
      <Card className="space-y-5 p-5 sm:p-6">
        <Label className="text-base">Who can use it, and where</Label>
        <FieldHint>Leave a list empty for no limit. Denied always wins over allowed.</FieldHint>
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label>Only these roles</Label>
            <EntityPicker
              multiple
              items={roleItems}
              value={draft.allowedRoleIds}
              onChange={(next) => set("allowedRoleIds", next)}
              placeholder="Anyone"
              aria-label="Allowed roles"
            />
          </div>
          <div className="space-y-2">
            <Label>Never these roles</Label>
            <EntityPicker
              multiple
              items={roleItems}
              value={draft.deniedRoleIds}
              onChange={(next) => set("deniedRoleIds", next)}
              placeholder="No exceptions"
              aria-label="Denied roles"
            />
          </div>
          <div className="space-y-2">
            <Label>Only in these channels</Label>
            <EntityPicker
              multiple
              items={channelItems}
              value={draft.allowedChannelIds}
              onChange={(next) => set("allowedChannelIds", next)}
              placeholder="Anywhere"
              aria-label="Allowed channels"
            />
          </div>
          <div className="space-y-2">
            <Label>Never in these channels</Label>
            <EntityPicker
              multiple
              items={channelItems}
              value={draft.deniedChannelIds}
              onChange={(next) => set("deniedChannelIds", next)}
              placeholder="No exceptions"
              aria-label="Denied channels"
            />
          </div>
        </div>
      </Card>

      {/* Cooldown */}
      <Card className="space-y-5 p-5 sm:p-6">
        <Label className="text-base">Cooldown</Label>
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="cc-cooldown">Seconds between uses</Label>
            <Input
              id="cc-cooldown"
              type="number"
              min={0}
              value={draft.cooldownSeconds}
              onChange={(e) => set("cooldownSeconds", Math.max(0, Number(e.target.value) || 0))}
            />
            <FieldHint>0 means no cooldown.</FieldHint>
          </div>
          <div className="space-y-2">
            <Label>Counted against</Label>
            <EntityPicker
              items={SCOPES}
              value={draft.cooldownScope}
              onChange={(next) => set("cooldownScope", (next as CooldownScope) ?? "user")}
              clearable={false}
              aria-label="Cooldown scope"
            />
          </div>
        </div>
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

function PlaceholderLegend() {
  const tokens: [string, string][] = [
    ["{user}", "mentions whoever ran it"],
    ["{username}", "their display name"],
    ["{server}", "the server name"],
    ["{channel}", "the current channel"],
    ["{membercount}", "how many members"],
    ["{count}", "this command's use number"],
    ["{random:a|b|c}", "picks one at random"],
    ["{math:2*21}", "does the sum"],
  ];
  return (
    <div className="rounded-lg bg-[var(--surface-hover)] p-3">
      <div className="mb-2 text-sm font-medium">Placeholders you can use</div>
      <div className="grid gap-x-4 gap-y-1 text-sm text-fg-subtle sm:grid-cols-2">
        {tokens.map(([token, what]) => (
          <div key={token} className="flex gap-2">
            <code className="text-fg-muted">{token}</code>
            <span className="truncate">{what}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
