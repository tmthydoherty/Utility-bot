"use client";

import * as React from "react";
import { Plus, Trash2 } from "lucide-react";

import { ChannelType } from "@/lib/discord/types";
import type { CmGlobal } from "@/lib/custommatch/read";
import { setGlobal, addModRole, removeModRole, blacklistAdd, blacklistRemove } from "@/app/actions/custommatch";
import { useChannelItems, useGuild, useRoleItems } from "@/components/providers/guild-provider";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { EntityPicker, type PickerItem } from "@/components/ui/entity-picker";
import { useRun } from "./use-run";

export interface SetupMember {
  id: string;
  name: string;
  username: string;
}

const TEXT = [ChannelType.GuildText];
const TEXT_FORUM = [ChannelType.GuildText, ChannelType.GuildForum];
const CATEGORY = [ChannelType.GuildCategory];

/**
 * The global, cross-game setup: where logs and admin actions go, who counts as a
 * CM admin or mod, and the player blacklist. Every change applies immediately
 * through the bridge (there's no buffered save here — each control is one
 * setting), and the page re-reads once the bot has drained it.
 */
export function GlobalSetup({
  guildId,
  global,
  members,
  names,
}: {
  guildId: string;
  global: CmGlobal;
  members: SetupMember[];
  names: Record<string, string>;
}) {
  const { run, busy } = useRun();
  const roleItems = useRoleItems();
  const textChannels = useChannelItems(TEXT);
  const forumChannels = useChannelItems(TEXT_FORUM);
  const categories = useChannelItems(CATEGORY);

  const memberItems: PickerItem[] = React.useMemo(
    () => members.map((m) => ({ value: m.id, label: m.name, keywords: [m.username] })),
    [members],
  );

  const setKey = (key: string, value: string | null) =>
    run(() => setGlobal(guildId, key, value), { success: "Saved" });

  const takenMod = new Set(global.mod_role_ids);
  const availableModRoles = roleItems.filter((r) => !takenMod.has(r.value));
  const [modRole, setModRole] = React.useState<string | null>(null);

  const blacklisted = new Set(global.blacklist.map((b) => b.player_id));
  const availableMembers = memberItems.filter((m) => !blacklisted.has(m.value));
  const [banMember, setBanMember] = React.useState<string | null>(null);

  return (
    <div className="space-y-5">
      <Card className="space-y-4 p-5 sm:p-6">
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">Channels & roles</h2>
          <p className="text-sm text-fg-muted">Where the system posts, and who administers it.</p>
        </div>
        <div className="grid gap-5 sm:grid-cols-2">
          <PickerField label="Log channel" help="Every custom-match action is logged here." items={textChannels} value={global.log_channel_id} disabled={busy} onChange={(v) => setKey("log_channel_id", v)} />
          <PickerField label="Admin channel" help="Where admin prompts and controls are posted." items={textChannels} value={global.cm_admin_channel_id} disabled={busy} onChange={(v) => setKey("cm_admin_channel_id", v)} />
          <PickerField label="CM admin role" help="This role may run the match panels and settings." items={roleItems} value={global.cm_admin_role_id} disabled={busy} onChange={(v) => setKey("cm_admin_role_id", v)} />
          <PickerField label="Match category" help="Per-match lobby and voice channels are created here by default." items={categories} value={global.category_id} disabled={busy} onChange={(v) => setKey("category_id", v)} />
          <PickerField label="Discussion parent" help="Match discussion threads open under this channel." items={forumChannels} value={global.cm_discussion_parent_channel_id} disabled={busy} onChange={(v) => setKey("cm_discussion_parent_channel_id", v)} />
          <PickerField label="Rivals admin channel" help="Where Marvel Rivals scoreboard uploads are reviewed." items={textChannels} value={global.rivals_admin_channel_id} disabled={busy} onChange={(v) => setKey("rivals_admin_channel_id", v)} />
        </div>
      </Card>

      <Card className="space-y-4 p-5 sm:p-6">
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">Mod roles</h2>
          <p className="text-sm text-fg-muted">These roles get access to every match channel.</p>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div className="min-w-0 flex-1 space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">Add a role</span>
            <EntityPicker items={availableModRoles} value={modRole} onChange={setModRole} placeholder="Select a role…" disabled={busy} />
          </div>
          <Button
            disabled={busy || !modRole}
            onClick={async () => {
              if (!modRole) return;
              const ok = await run(() => addModRole(guildId, modRole), { success: "Mod role added" });
              if (ok) setModRole(null);
            }}
          >
            <Plus aria-hidden />
            Add
          </Button>
        </div>
        {global.mod_role_ids.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {global.mod_role_ids.map((roleId) => (
              <RoleChip key={roleId} roleId={roleId} disabled={busy} onRemove={() => run(() => removeModRole(guildId, roleId), { success: "Mod role removed" })} />
            ))}
          </div>
        )}
      </Card>

      <Card className="space-y-4 p-5 sm:p-6">
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">Blacklist</h2>
          <p className="text-sm text-fg-muted">Blacklisted members can&apos;t join any queue.</p>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div className="min-w-0 flex-1 space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">Blacklist a member</span>
            <EntityPicker items={availableMembers} value={banMember} onChange={setBanMember} placeholder="Search for a member…" searchPlaceholder="Search by name…" emptyMessage="No members match that." disabled={busy} />
          </div>
          <Button
            variant="danger"
            disabled={busy || !banMember}
            onClick={async () => {
              if (!banMember) return;
              const ok = await run(() => blacklistAdd(guildId, banMember), { success: "Member blacklisted" });
              if (ok) setBanMember(null);
            }}
          >
            <Plus aria-hidden />
            Blacklist
          </Button>
        </div>
        {global.blacklist.length === 0 ? (
          <EmptyState icon="Ban" title="Nobody's blacklisted" description="Members you blacklist can't join any queue until you remove them." />
        ) : (
          <Card className="divide-y divide-[var(--border)]">
            {global.blacklist.map((entry) => (
              <div key={entry.player_id} className="flex items-center gap-3 p-3">
                <span className="min-w-0 flex-1 truncate text-sm">
                  {names[entry.player_id] ?? `Member ${entry.player_id}`}
                </span>
                <Button variant="ghost" size="sm" disabled={busy} onClick={() => run(() => blacklistRemove(guildId, entry.player_id), { success: "Removed from blacklist" })}>
                  <Trash2 aria-hidden />
                  Remove
                </Button>
              </div>
            ))}
          </Card>
        )}
      </Card>
    </div>
  );
}

function PickerField({
  label,
  help,
  items,
  value,
  disabled,
  onChange,
}: {
  label: string;
  help?: string;
  items: PickerItem[];
  value: string | null;
  disabled: boolean;
  onChange: (value: string | null) => void;
}) {
  return (
    <div className="space-y-1.5">
      <span className="text-xs font-medium text-fg-muted">{label}</span>
      <EntityPicker items={items} value={value} onChange={onChange} clearable placeholder="None" disabled={disabled} />
      {help && <p className="text-xs text-fg-subtle">{help}</p>}
    </div>
  );
}

function RoleChip({ roleId, disabled, onRemove }: { roleId: string; disabled: boolean; onRemove: () => void }) {
  const { roles } = useGuild();
  const name = roles.find((r) => r.id === roleId)?.name ?? `Role ${roleId}`;
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-[var(--border)] py-1 pl-3 pr-1.5 text-sm">
      @{name}
      <button
        type="button"
        aria-label={`Remove ${name}`}
        disabled={disabled}
        onClick={onRemove}
        className="grid size-5 place-items-center rounded-full text-fg-subtle transition-colors hover:bg-[var(--surface-hover)] hover:text-fg disabled:opacity-50"
      >
        <Trash2 className="size-3.5" />
      </button>
    </span>
  );
}
