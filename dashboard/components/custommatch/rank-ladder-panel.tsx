"use client";

import * as React from "react";
import { Plus, Trash2 } from "lucide-react";

import type { CmRank } from "@/lib/custommatch/read";
import { setRank, removeRank } from "@/app/actions/custommatch";
import { useGuild, useRoleItems } from "@/components/providers/guild-provider";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { EntityPicker } from "@/components/ui/entity-picker";
import { Input } from "@/components/ui/input";
import { useRun } from "./use-run";

/**
 * The MMR→role ladder for a game. Each rung is a Discord role granted when a
 * player's MMR reaches its threshold, and the labels are what the self-setup
 * flow offers a new player as their "peak rank". Rungs apply immediately (their
 * own bridge command), separate from the buffered settings save, so this panel
 * carries its own add/remove rather than riding the SaveBar.
 */
export function RankLadderPanel({
  guildId,
  gameId,
  ranks,
}: {
  guildId: string;
  gameId: number;
  ranks: CmRank[];
}) {
  const { run, busy } = useRun();
  const roleItems = useRoleItems();
  const { roles } = useGuild();
  const roleName = React.useMemo(() => new Map(roles.map((r) => [r.id, r.name])), [roles]);

  const [roleId, setRoleId] = React.useState<string | null>(null);
  const [mmr, setMmr] = React.useState("");
  const [label, setLabel] = React.useState("");

  const taken = new Set(ranks.map((r) => r.role_id));
  const availableRoles = roleItems.filter((r) => !taken.has(r.value));

  const canAdd = roleId != null && mmr.trim() !== "" && Number.isFinite(Number(mmr));

  const add = async () => {
    if (!canAdd || roleId == null) return;
    const ok = await run(
      () => setRank(guildId, gameId, roleId, Number(mmr), label.trim() || null),
      { success: "Rank added" },
    );
    if (ok) {
      setRoleId(null);
      setMmr("");
      setLabel("");
    }
  };

  return (
    <div className="space-y-5">
      <Card className="p-5">
        <h3 className="text-sm font-semibold">Add a rank</h3>
        <p className="mt-1 text-sm text-fg-muted">
          Pick the role, the MMR a player reaches it at, and the label shown at setup.
        </p>
        <div className="mt-3 grid gap-3 sm:grid-cols-[1fr_auto_1fr_auto] sm:items-end">
          <div className="space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">Role</span>
            <EntityPicker
              items={availableRoles}
              value={roleId}
              onChange={setRoleId}
              placeholder="Select a role…"
              disabled={busy}
            />
          </div>
          <div className="space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">MMR</span>
            <Input
              type="number"
              inputMode="numeric"
              value={mmr}
              onChange={(e) => setMmr(e.target.value)}
              placeholder="1000"
              className="sm:w-28"
              disabled={busy}
            />
          </div>
          <div className="space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">Label</span>
            <Input
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="e.g. Gold"
              maxLength={50}
              disabled={busy}
            />
          </div>
          <Button onClick={add} disabled={!canAdd || busy}>
            <Plus aria-hidden />
            Add
          </Button>
        </div>
      </Card>

      {ranks.length === 0 ? (
        <EmptyState
          icon="Trophy"
          title="No ranks yet"
          description="Without a ladder, a game can't require a rank on join and self-setup falls back to a verification ticket."
        />
      ) : (
        <Card className="divide-y divide-[var(--border)]">
          {ranks.map((rank) => (
            <div key={rank.role_id} className="flex items-center gap-3 p-3.5">
              <div className="min-w-0 flex-1">
                <p className="truncate text-sm font-medium">
                  {rank.label ?? roleName.get(rank.role_id) ?? `Role ${rank.role_id}`}
                </p>
                <p className="text-xs text-fg-subtle">
                  @{roleName.get(rank.role_id) ?? rank.role_id} · MMR {rank.mmr_value}
                </p>
              </div>
              <Button
                variant="ghost"
                size="icon"
                aria-label={`Remove ${rank.label ?? "rank"}`}
                disabled={busy}
                onClick={() => run(() => removeRank(guildId, gameId, rank.role_id), { success: "Rank removed" })}
              >
                <Trash2 aria-hidden />
              </Button>
            </div>
          ))}
        </Card>
      )}
    </div>
  );
}
