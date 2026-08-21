"use client";

import * as React from "react";

import { cn, formatRelative } from "@/lib/utils";
import type { CmPlayerProfile } from "@/lib/custommatch/read";
import {
  lookupPlayerProfile,
  setPlayerMmr,
  setPlayerOffset,
  setPlayerIgn,
  clearPenalty,
  removeSuspension,
  type ActionResult,
} from "@/app/actions/custommatch";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { EntityPicker, type PickerItem } from "@/components/ui/entity-picker";
import { Icon } from "@/components/ui/icon";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Sparkline } from "@/components/ui/sparkline";
import { useToast } from "@/components/ui/toast";

export interface InspectorGame {
  id: number;
  name: string;
  enabled: boolean;
}

export interface InspectorMember {
  id: string;
  name: string;
  username: string;
  avatar: string;
}

/** SQLite stores UTC without a zone marker; make it explicit before parsing. */
function parseTs(value: string | null): number | null {
  if (!value) return null;
  const ms = Date.parse(value.includes("T") ? value : `${value.replace(" ", "T")}Z`);
  return Number.isFinite(ms) ? ms : null;
}

export function PlayerInspector({
  guildId,
  games,
  members,
}: {
  guildId: string;
  games: InspectorGame[];
  members: InspectorMember[];
}) {
  const toast = useToast();
  const [gameId, setGameId] = React.useState<number | null>(games[0]?.id ?? null);
  const [playerId, setPlayerId] = React.useState<string | null>(null);
  const [data, setData] = React.useState<CmPlayerProfile | null>(null);
  const [names, setNames] = React.useState<Record<string, string>>({});
  const [error, setError] = React.useState<string | null>(null);
  const [pending, startTransition] = React.useTransition();
  const [busy, setBusy] = React.useState(false);

  const memberById = React.useMemo(() => new Map(members.map((m) => [m.id, m])), [members]);
  const items: PickerItem[] = React.useMemo(
    () => members.map((m) => ({ value: m.id, label: m.name, keywords: [m.username] })),
    [members],
  );

  const load = React.useCallback(
    (game: number, player: string) => {
      setError(null);
      startTransition(async () => {
        const result = await lookupPlayerProfile(guildId, game, player);
        if (result.ok && result.profile) {
          setData(result.profile);
          setNames(result.names ?? {});
        } else {
          setData(null);
          setError(result.error ?? "Couldn't load that player.");
        }
      });
    },
    [guildId],
  );

  // Re-load whenever the pair changes.
  React.useEffect(() => {
    if (gameId != null && playerId) load(gameId, playerId);
    else setData(null);
  }, [gameId, playerId, load]);

  const reconcile = () => {
    if (gameId != null && playerId) window.setTimeout(() => load(gameId, playerId), 11_000);
  };

  const apply = async (fn: () => Promise<ActionResult>, success: string, optimistic?: () => void) => {
    setBusy(true);
    try {
      const res = await fn();
      if (res.ok) {
        optimistic?.();
        toast.success(success, "Vibey applies this within about 10 seconds.");
        reconcile();
      } else {
        toast.error("Couldn't save that", res.error ?? "Try again.");
      }
    } catch {
      toast.error("Couldn't reach the server", "Nothing was changed.");
    } finally {
      setBusy(false);
    }
  };

  const member = playerId ? memberById.get(playerId) : undefined;

  return (
    <div className="space-y-6">
      <Card className="space-y-4 p-5">
        {games.length > 1 && (
          <div className="flex flex-wrap gap-2">
            {games.map((g) => (
              <Button
                key={g.id}
                size="sm"
                variant={g.id === gameId ? "primary" : "outline"}
                onClick={() => setGameId(g.id)}
              >
                {g.name}
                {!g.enabled && <span className="ml-1 text-xs opacity-70">(off)</span>}
              </Button>
            ))}
          </div>
        )}
        <div className="max-w-md">
          <EntityPicker
            items={items}
            value={playerId}
            onChange={setPlayerId}
            placeholder="Search for a member…"
            searchPlaceholder="Search by name…"
            emptyMessage="No members match that."
            aria-label="Choose a member"
          />
        </div>
      </Card>

      {pending && (
        <Card className="space-y-4 p-5">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-24 w-full" />
        </Card>
      )}

      {!pending && error && (
        <Card>
          <EmptyState icon="Users" title="Couldn't load that player" description={error} />
        </Card>
      )}

      {!pending && !error && playerId && data && (
        <Profile
          data={data}
          names={names}
          member={member}
          gameId={gameId!}
          guildId={guildId}
          busy={busy}
          apply={apply}
          setData={setData}
        />
      )}

      {!pending && !playerId && (
        <Card>
          <EmptyState
            icon="Users"
            title="Pick a member"
            description="Choose a game and search a member to see their MMR, streaks, rivalries and match history."
          />
        </Card>
      )}
    </div>
  );
}

function Profile({
  data,
  names,
  member,
  gameId,
  guildId,
  busy,
  apply,
  setData,
}: {
  data: CmPlayerProfile;
  names: Record<string, string>;
  member: InspectorMember | undefined;
  gameId: number;
  guildId: string;
  busy: boolean;
  apply: (fn: () => Promise<ActionResult>, success: string, optimistic?: () => void) => Promise<void>;
  setData: React.Dispatch<React.SetStateAction<CmPlayerProfile | null>>;
}) {
  const total = data.wins + data.losses;
  const winRate = total > 0 ? Math.round((data.wins / total) * 100) : 0;
  const streakLabel =
    data.streak === 0 ? "—" : `${data.streak > 0 ? "W" : "L"}${Math.abs(data.streak)}`;

  return (
    <div className="space-y-5">
      {!data.found && (
        <Card className="border-[var(--warning)]/30 bg-[var(--warning-soft)] p-4">
          <p className="text-sm text-fg-muted">
            This member has no stats for this game yet. Setting an MMR below registers them.
          </p>
        </Card>
      )}

      <Card className="p-5">
        <div className="flex flex-wrap items-center gap-4">
          <Avatar src={member?.avatar} name={member?.name ?? data.player_id} size="lg" />
          <div className="min-w-0 flex-1">
            <h2 className="flex items-center gap-2 text-lg font-semibold">
              {member?.name ?? `Member ${data.player_id}`}
              <Badge variant="neutral">{data.platform.toUpperCase()}</Badge>
              {data.ign && <span className="text-sm font-normal text-fg-subtle">IGN: {data.ign}</span>}
            </h2>
            <p className="text-sm text-fg-muted">
              {data.games_played} game{data.games_played === 1 ? "" : "s"} · Streak {streakLabel}
            </p>
          </div>
          <div className="text-right">
            <p className="text-3xl font-semibold tabular-nums">{data.effective}</p>
            <p className="text-xs text-fg-subtle">
              MMR{data.admin_offset ? ` (${data.mmr} ${data.admin_offset > 0 ? "+" : ""}${data.admin_offset})` : ""}
            </p>
          </div>
        </div>
        {data.mmrTrend.length >= 2 && (
          <div className="mt-4">
            <Sparkline data={data.mmrTrend} className="h-12 w-full" />
          </div>
        )}
      </Card>

      <div className="grid gap-4 sm:grid-cols-3">
        <StatBox label="Record" value={`${data.wins}–${data.losses}`} />
        <StatBox label="Win rate" value={total > 0 ? `${winRate}%` : "—"} />
        <StatBox label="Games" value={String(data.games_played)} />
      </div>

      <Card className="space-y-4 p-5">
        <h3 className="text-sm font-semibold">Adjust</h3>
        <div className="grid gap-4 sm:grid-cols-3">
          <NumberEdit
            label="Base MMR"
            initial={data.mmr}
            disabled={busy}
            onSave={(v) =>
              apply(() => setPlayerMmr(guildId, gameId, data.player_id, v), "MMR updated", () =>
                setData((d) => (d ? { ...d, mmr: v, effective: v + d.admin_offset, found: true } : d)),
              )
            }
          />
          <NumberEdit
            label="Admin offset"
            initial={data.admin_offset}
            disabled={busy}
            onSave={(v) =>
              apply(() => setPlayerOffset(guildId, gameId, data.player_id, v), "Offset updated", () =>
                setData((d) => (d ? { ...d, admin_offset: v, effective: d.mmr + v } : d)),
              )
            }
          />
          <TextEdit
            label="In-game name"
            initial={data.ign ?? ""}
            disabled={busy}
            onSave={(v) =>
              apply(() => setPlayerIgn(guildId, gameId, data.player_id, v), "IGN updated", () =>
                setData((d) => (d ? { ...d, ign: v } : d)),
              )
            }
          />
        </div>
      </Card>

      {data.roleStats.length > 0 && (
        <Card className="p-5">
          <h3 className="mb-3 text-sm font-semibold">Role MMR</h3>
          <div className="grid gap-3 sm:grid-cols-3">
            {data.roleStats.map((r) => (
              <div key={r.role} className="rounded-lg border border-[var(--border)] p-3">
                <p className="text-xs text-fg-subtle">{r.role}</p>
                <p className="text-lg font-semibold tabular-nums">{r.mmr}</p>
                <p className="text-xs text-fg-subtle">
                  {r.wins}–{r.losses} · {r.games_played}g
                </p>
              </div>
            ))}
          </div>
        </Card>
      )}

      <div className="grid gap-5 lg:grid-cols-2">
        <Card className="p-5">
          <h3 className="mb-3 text-sm font-semibold">Rivalries</h3>
          {data.rivals.length === 0 ? (
            <p className="text-sm text-fg-subtle">No head-to-heads yet.</p>
          ) : (
            <ul className="space-y-2">
              {data.rivals.map((r) => (
                <li key={r.opponent_id} className="flex items-center justify-between gap-3 text-sm">
                  <span className="truncate">{names[r.opponent_id] ?? `Member ${r.opponent_id}`}</span>
                  <span className="tabular-nums text-fg-muted">
                    <span className="text-[var(--success)]">{r.wins}</span>
                    {" – "}
                    <span className="text-[var(--danger)]">{r.losses}</span>
                  </span>
                </li>
              ))}
            </ul>
          )}
        </Card>

        <Card className="p-5">
          <h3 className="mb-3 text-sm font-semibold">Recent matches</h3>
          {data.recentMatches.length === 0 ? (
            <p className="text-sm text-fg-subtle">No matches recorded.</p>
          ) : (
            <ul className="space-y-2">
              {data.recentMatches.slice(0, 10).map((m) => {
                const ts = parseTs(m.created_at);
                return (
                  <li key={m.match_id} className="flex items-center gap-3 text-sm">
                    <span
                      className={cn(
                        "grid size-6 shrink-0 place-items-center rounded text-xs font-bold",
                        m.won === null
                          ? "bg-[var(--surface-hover)] text-fg-subtle"
                          : m.won
                            ? "bg-[var(--success-soft)] text-[var(--success)]"
                            : "bg-[var(--danger-soft)] text-[var(--danger)]",
                      )}
                    >
                      {m.won === null ? "•" : m.won ? "W" : "L"}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-fg-muted">
                      {m.map_name ?? `Match #${m.match_id}`}
                    </span>
                    {ts && <span className="shrink-0 text-xs text-fg-subtle">{formatRelative(ts)}</span>}
                  </li>
                );
              })}
            </ul>
          )}
        </Card>
      </div>

      {(data.penalties.length > 0 || data.suspensions.length > 0) && (
        <Card className="space-y-3 p-5">
          <h3 className="text-sm font-semibold">Penalties & suspensions</h3>
          {data.penalties.map((p) => (
            <div key={p.kind} className="flex items-center justify-between gap-3 text-sm">
              <span className="text-fg-muted">
                <Icon name="AlertTriangle" className="mr-1.5 inline size-3.5 text-[var(--warning)]" />
                {p.kind === "decline" ? "Declined ready-ups" : "Missed ready-ups"} · {p.offenses} offence
                {p.offenses === 1 ? "" : "s"}
              </span>
              <Button
                variant="ghost"
                size="sm"
                disabled={busy}
                onClick={() =>
                  apply(() => clearPenalty(guildId, data.player_id, p.kind), "Penalty cleared", () =>
                    setData((d) => (d ? { ...d, penalties: d.penalties.filter((x) => x.kind !== p.kind) } : d)),
                  )
                }
              >
                Clear
              </Button>
            </div>
          ))}
          {data.suspensions.map((s) => {
            const until = parseTs(s.until);
            return (
              <div key={s.suspension_id} className="flex items-center justify-between gap-3 text-sm">
                <span className="text-fg-muted">
                  <Icon name="Ban" className="mr-1.5 inline size-3.5 text-[var(--danger)]" />
                  Suspended{until ? ` until ${formatRelative(until)}` : ""}
                  {s.reason ? ` — ${s.reason}` : ""}
                </span>
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={busy}
                  onClick={() =>
                    apply(() => removeSuspension(guildId, s.suspension_id), "Suspension lifted", () =>
                      setData((d) =>
                        d ? { ...d, suspensions: d.suspensions.filter((x) => x.suspension_id !== s.suspension_id) } : d,
                      ),
                    )
                  }
                >
                  Lift
                </Button>
              </div>
            );
          })}
        </Card>
      )}
    </div>
  );
}

function StatBox({ label, value }: { label: string; value: string }) {
  return (
    <Card className="p-4">
      <p className="text-xs text-fg-subtle">{label}</p>
      <p className="mt-1 text-2xl font-semibold tabular-nums">{value}</p>
    </Card>
  );
}

function NumberEdit({
  label,
  initial,
  disabled,
  onSave,
}: {
  label: string;
  initial: number;
  disabled: boolean;
  onSave: (value: number) => void;
}) {
  const [value, setValue] = React.useState(String(initial));
  React.useEffect(() => setValue(String(initial)), [initial]);
  const changed = value.trim() !== String(initial) && value.trim() !== "" && Number.isFinite(Number(value));
  return (
    <div className="space-y-1.5">
      <span className="text-xs font-medium text-fg-muted">{label}</span>
      <div className="flex gap-2">
        <Input
          type="number"
          inputMode="numeric"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          disabled={disabled}
        />
        <Button size="sm" variant="secondary" disabled={disabled || !changed} onClick={() => onSave(Number(value))}>
          Save
        </Button>
      </div>
    </div>
  );
}

function TextEdit({
  label,
  initial,
  disabled,
  onSave,
}: {
  label: string;
  initial: string;
  disabled: boolean;
  onSave: (value: string) => void;
}) {
  const [value, setValue] = React.useState(initial);
  React.useEffect(() => setValue(initial), [initial]);
  const changed = value.trim() !== initial.trim() && value.trim() !== "";
  return (
    <div className="space-y-1.5">
      <span className="text-xs font-medium text-fg-muted">{label}</span>
      <div className="flex gap-2">
        <Input value={value} onChange={(e) => setValue(e.target.value)} disabled={disabled} maxLength={100} />
        <Button size="sm" variant="secondary" disabled={disabled || !changed} onClick={() => onSave(value.trim())}>
          Save
        </Button>
      </div>
    </div>
  );
}
