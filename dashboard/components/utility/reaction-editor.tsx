"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EntityPicker } from "@/components/ui/entity-picker";
import { Input, Textarea, Label, FieldHint } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { useChannelItems, useRoleItems } from "@/components/providers/guild-provider";
import { ChannelType } from "@/lib/discord/types";
import { deleteReactionRule, saveReactionRule, setReactionEnabled } from "@/app/actions/utility";
import type { ReactionInput } from "@/lib/utility/store";
import type { ReactionRule } from "@/lib/utility/types";

const CHANNEL_TYPES = [
  ChannelType.GuildText,
  ChannelType.GuildAnnouncement,
  ChannelType.GuildVoice,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
  ChannelType.GuildForum,
];

const SCOPES = [
  { value: "all", label: "Everyone's reactions" },
  { value: "role_mention", label: "Only people with certain roles" },
  { value: "from_user", label: "Only certain people" },
];

const MODES = [
  { value: "remove", label: "Remove every reaction" },
  { value: "allowlist", label: "Allow only these emoji" },
  { value: "blocklist", label: "Block these emoji" },
];

interface Draft {
  channelId: string;
  scope: ReactionRule["scope"];
  mode: ReactionRule["mode"];
  roleIds: string[];
  userIds: string[];
  emoji: string[];
  bypassRoleIds: string[];
  removeAfterSeconds: number;
  maxReactions: number;
  includeThreads: boolean;
}

function draftFrom(rule: ReactionRule | null): Draft {
  return rule
    ? {
        channelId: rule.channelId,
        scope: rule.scope,
        mode: rule.mode,
        roleIds: rule.roleIds,
        userIds: rule.userIds,
        emoji: rule.emoji,
        bypassRoleIds: rule.bypassRoleIds,
        removeAfterSeconds: rule.removeAfterSeconds,
        maxReactions: rule.maxReactions,
        includeThreads: rule.includeThreads,
      }
    : {
        channelId: "",
        scope: "all",
        mode: "remove",
        roleIds: [],
        userIds: [],
        emoji: [],
        bypassRoleIds: [],
        removeAfterSeconds: 0,
        maxReactions: 0,
        includeThreads: true,
      };
}

const toList = (text: string): string[] =>
  text.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean);

export function ReactionEditor({ guildId, rule }: { guildId: string; rule: ReactionRule | null }) {
  const router = useRouter();
  const toast = useToast();
  const channelItems = useChannelItems(CHANNEL_TYPES);
  const roleItems = useRoleItems();

  const [initial, setInitial] = React.useState<Draft>(() => draftFrom(rule));
  const [draft, setDraft] = React.useState<Draft>(initial);
  const [enabled, setEnabled] = React.useState(rule?.enabled ?? true);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const id = rule?.id ?? null;

  const dirty = React.useMemo(() => JSON.stringify(draft) !== JSON.stringify(initial), [draft, initial]);
  const set = <K extends keyof Draft>(key: K, val: Draft[K]) => setDraft((d) => ({ ...d, [key]: val }));

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const result = await saveReactionRule(guildId, id, draft as ReactionInput);
      if (result.ok) {
        setInitial(draft);
        toast.success("Saved", "Live within about 10 seconds.");
        if (!id && result.id) router.replace(`/dashboard/${guildId}/utility/reactions/${result.id}`);
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
    if (!confirm("Delete this reaction rule?")) return;
    const result = await deleteReactionRule(guildId, id);
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
            <Label>Channel</Label>
            <EntityPicker
              items={channelItems}
              value={draft.channelId || null}
              onChange={(next) => set("channelId", next ?? "")}
              placeholder="Pick a channel"
              aria-label="Channel"
            />
          </div>
          {id && (
            <div className="space-y-2">
              <Label>Live</Label>
              <label className="flex h-9 items-center gap-2 text-sm text-fg-muted">
                <Switch
                  checked={enabled}
                  onCheckedChange={async (next) => {
                    setEnabled(next);
                    const r = await setReactionEnabled(guildId, id, next);
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

        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label>Applies to</Label>
            <EntityPicker
              items={SCOPES}
              value={draft.scope}
              onChange={(next) => set("scope", (next as Draft["scope"]) ?? "all")}
              clearable={false}
              aria-label="Scope"
            />
          </div>
          <div className="space-y-2">
            <Label>What to do</Label>
            <EntityPicker
              items={MODES}
              value={draft.mode}
              onChange={(next) => set("mode", (next as Draft["mode"]) ?? "remove")}
              clearable={false}
              aria-label="Mode"
            />
          </div>
        </div>

        {draft.scope === "role_mention" && (
          <div className="space-y-2">
            <Label>Roles this applies to</Label>
            <EntityPicker
              multiple
              items={roleItems}
              value={draft.roleIds}
              onChange={(next) => set("roleIds", next)}
              placeholder="Pick roles"
              aria-label="Roles"
            />
          </div>
        )}

        {draft.scope === "from_user" && (
          <div className="space-y-2">
            <Label htmlFor="react-users">User IDs</Label>
            <Textarea
              id="react-users"
              rows={2}
              value={draft.userIds.join(", ")}
              onChange={(e) => set("userIds", toList(e.target.value))}
              placeholder="Comma-separated user IDs"
            />
          </div>
        )}

        {draft.mode !== "remove" && (
          <div className="space-y-2">
            <Label htmlFor="react-emoji">Emoji</Label>
            <Textarea
              id="react-emoji"
              rows={2}
              value={draft.emoji.join(", ")}
              onChange={(e) => set("emoji", toList(e.target.value))}
              placeholder="🎉, 👍, <:custom:123…>"
            />
            <FieldHint>
              {draft.mode === "allowlist"
                ? "Only these emoji are allowed; anything else is removed."
                : "These emoji are removed; everything else stays."}
            </FieldHint>
          </div>
        )}
      </Card>

      <Card className="space-y-5 p-5 sm:p-6">
        <Label className="text-base">Fine tuning</Label>
        <div className="space-y-2">
          <Label>Roles that are never affected</Label>
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
            <Label htmlFor="react-after">Remove after (seconds)</Label>
            <Input
              id="react-after"
              type="number"
              min={0}
              value={draft.removeAfterSeconds}
              onChange={(e) => set("removeAfterSeconds", Math.max(0, Number(e.target.value) || 0))}
            />
            <FieldHint>0 removes immediately.</FieldHint>
          </div>
          <div className="space-y-2">
            <Label htmlFor="react-max">Max reactions per message</Label>
            <Input
              id="react-max"
              type="number"
              min={0}
              value={draft.maxReactions}
              onChange={(e) => set("maxReactions", Math.max(0, Number(e.target.value) || 0))}
            />
            <FieldHint>0 means no cap.</FieldHint>
          </div>
        </div>
        <label className="flex items-center gap-3 text-sm">
          <Switch checked={draft.includeThreads} onCheckedChange={(c) => set("includeThreads", c)} />
          Also apply inside this channel&apos;s threads
        </label>
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
