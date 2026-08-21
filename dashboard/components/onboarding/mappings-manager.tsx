"use client";

import * as React from "react";
import { Plus, Trash2 } from "lucide-react";

import { ChannelType } from "@/lib/discord/types";
import type { OnbMapping } from "@/lib/onboarding/store";
import { mappingAdd, mappingRemove } from "@/app/actions/onboarding";
import { useChannelItems, useGuild, useRoleItems } from "@/components/providers/guild-provider";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { EntityPicker } from "@/components/ui/entity-picker";
import { useRun } from "./use-run";

const LFG_CHANNEL_TYPES = [
  ChannelType.GuildText,
  ChannelType.GuildForum,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
];

export function MappingsManager({
  guildId,
  mappings,
  botOnline,
}: {
  guildId: string;
  mappings: OnbMapping[];
  botOnline: boolean;
}) {
  const { run, busy } = useRun();
  const roleItems = useRoleItems();
  const channelItems = useChannelItems(LFG_CHANNEL_TYPES);

  // Resolve each mapping's stored channel id to its name so the list reads
  // "→ #lfg-valorant" rather than a raw snowflake nobody can identify.
  const { channels } = useGuild();
  const channelName = React.useMemo(
    () => new Map(channels.map((c) => [c.id, c.name])),
    [channels],
  );

  const [roleId, setRoleId] = React.useState<string | null>(null);
  const [threadId, setThreadId] = React.useState<string | null>(null);

  const taken = new Set(mappings.map((m) => m.roleId));
  const availableRoles = roleItems.filter((r) => !taken.has(r.value));

  return (
    <div className="space-y-5">
      <Card className="p-5">
        <h3 className="text-sm font-semibold">Add a mapping</h3>
        <div className="mt-3 grid gap-3 sm:grid-cols-2">
          <div className="space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">Game role</span>
            <EntityPicker
              items={availableRoles}
              value={roleId}
              onChange={setRoleId}
              placeholder="Select a game role"
              searchPlaceholder="Search roles…"
              aria-label="Game role"
            />
          </div>
          <div className="space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">LFG thread / channel</span>
            <EntityPicker
              items={channelItems}
              value={threadId}
              onChange={setThreadId}
              placeholder="Select a thread or channel"
              searchPlaceholder="Search channels…"
              aria-label="LFG thread or channel"
            />
          </div>
        </div>
        <div className="mt-3 flex justify-end">
          <Button
            variant="primary"
            size="sm"
            loading={busy}
            disabled={!roleId || !threadId}
            onClick={async () => {
              if (!roleId || !threadId) return;
              const ok = await run(() => mappingAdd(guildId, roleId, threadId), {
                success: "Adding the mapping",
              });
              if (ok) {
                setRoleId(null);
                setThreadId(null);
              }
            }}
          >
            <Plus aria-hidden />
            Add mapping
          </Button>
        </div>
      </Card>

      <Card className="p-5">
        <h3 className="text-sm font-semibold">
          Mappings <span className="font-normal text-fg-muted">({mappings.length})</span>
        </h3>
        {mappings.length === 0 ? (
          <EmptyState
            icon="Users"
            title="No mappings yet"
            description="Pair a game role with its LFG thread above."
          />
        ) : (
          <ul className="mt-4 divide-y divide-[var(--border)] rounded-lg border border-[var(--border)]">
            {mappings.map((m) => (
              <li key={m.roleId} className="flex items-center gap-3 p-3 text-sm">
                <span className="min-w-0 flex-1 truncate">
                  <span className="font-medium">{m.roleName}</span>{" "}
                  <span className="text-fg-subtle">
                    →{" "}
                    {channelName.has(m.threadId)
                      ? `#${channelName.get(m.threadId)}`
                      : `unknown channel (${m.threadId})`}
                  </span>
                </span>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  aria-label={`Remove mapping for ${m.roleName}`}
                  disabled={busy}
                  onClick={() => {
                    if (confirm(`Remove the mapping for ${m.roleName}?`)) {
                      run(() => mappingRemove(guildId, m.roleId), { success: "Removing the mapping" });
                    }
                  }}
                >
                  <Trash2 aria-hidden />
                </Button>
              </li>
            ))}
          </ul>
        )}
      </Card>

      {!botOnline && <p className="text-xs text-fg-subtle">Changes are queued and apply when Vibey starts.</p>}
    </div>
  );
}
