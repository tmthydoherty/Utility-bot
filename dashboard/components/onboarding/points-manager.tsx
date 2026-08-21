"use client";

import * as React from "react";
import { ChevronDown, Trash2 } from "lucide-react";

import { cn } from "@/lib/utils";
import type { OnbLists, OnbMember, OnbMeta, OnbUpgradeRow } from "@/lib/onboarding/store";
import { adjustPoints, wipeAllPoints, wipeMember } from "@/app/actions/onboarding";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useRun } from "./use-run";

const SNOWFLAKE = /^\d{17,20}$/;

export function PointsManager({
  guildId,
  members,
  lists,
  meta,
  botOnline,
}: {
  guildId: string;
  members: OnbMember[];
  lists: OnbLists;
  meta: OnbMeta;
  botOnline: boolean;
}) {
  const { run, busy } = useRun();
  const [query, setQuery] = React.useState("");

  const sorted = React.useMemo(
    () => [...members].sort((a, b) => b.effective - a.effective),
    [members],
  );
  const filtered = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return sorted;
    return sorted.filter((m) => m.name.toLowerCase().includes(needle) || m.userId.includes(needle));
  }, [sorted, query]);

  return (
    <div className="space-y-5">
      <AdjustByIdCard guildId={guildId} run={run} busy={busy} />

      <Card className="p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h3 className="text-sm font-semibold">
            Members with points <span className="font-normal text-fg-muted">({members.length})</span>
          </h3>
          <Button
            variant="ghost"
            size="sm"
            disabled={busy || members.length === 0}
            onClick={() => {
              if (confirm("Wipe ALL points, the decay ledger and thread logs for everyone? This cannot be undone.")) {
                run(() => wipeAllPoints(guildId), { success: "Wiping all points" });
              }
            }}
          >
            <Trash2 aria-hidden />
            Wipe everyone
          </Button>
        </div>

        <Input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search members…"
          aria-label="Search members"
          className="mt-3"
        />

        <div className="mt-4">
          {members.length === 0 ? (
            <EmptyState icon="Coins" title="No one has points yet" description="Points come from replying to newcomers and posting in intro threads." />
          ) : filtered.length === 0 ? (
            <p className="py-8 text-center text-sm text-fg-muted">Nothing matches that search.</p>
          ) : (
            <ul className="divide-y divide-[var(--border)] rounded-lg border border-[var(--border)]">
              {filtered.map((m) => (
                <MemberRow key={m.userId} guildId={guildId} member={m} run={run} busy={busy} />
              ))}
            </ul>
          )}
        </div>
      </Card>

      <ListsCard lists={lists} meta={meta} />

      {!botOnline && <p className="text-xs text-fg-subtle">Changes are queued and apply when Vibey starts.</p>}
    </div>
  );
}

type RunFn = ReturnType<typeof useRun>["run"];

function AdjustByIdCard({ guildId, run, busy }: { guildId: string; run: RunFn; busy: boolean }) {
  const [userId, setUserId] = React.useState("");
  const [amount, setAmount] = React.useState("");

  const valid = SNOWFLAKE.test(userId.trim()) && amount.trim() !== "" && Number(amount) >= 0;

  const act = (op: "add" | "remove" | "set") => {
    if (!valid) return;
    run(() => adjustPoints(guildId, userId.trim(), op, Number(amount)), {
      success: `${op === "add" ? "Adding" : op === "remove" ? "Removing" : "Setting"} points`,
    });
  };

  return (
    <Card className="p-5">
      <h3 className="text-sm font-semibold">Adjust a member by ID</h3>
      <p className="mt-1 text-sm text-fg-muted">
        Enter a member ID to add, remove or set points — useful for someone who has no points yet.
      </p>
      <div className="mt-3 flex flex-wrap gap-2">
        <Input
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          placeholder="Member ID"
          aria-label="Member ID"
          className="min-w-[12rem] flex-1"
        />
        <Input
          type="number"
          min={0}
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          placeholder="Amount"
          aria-label="Amount"
          className="w-28"
        />
        <Button variant="secondary" size="sm" loading={busy} disabled={!valid} onClick={() => act("add")}>
          Add
        </Button>
        <Button variant="secondary" size="sm" loading={busy} disabled={!valid} onClick={() => act("remove")}>
          Remove
        </Button>
        <Button variant="secondary" size="sm" loading={busy} disabled={!valid} onClick={() => act("set")}>
          Set
        </Button>
      </div>
    </Card>
  );
}

function MemberRow({
  guildId,
  member,
  run,
  busy,
}: {
  guildId: string;
  member: OnbMember;
  run: RunFn;
  busy: boolean;
}) {
  const [open, setOpen] = React.useState(false);
  const [amount, setAmount] = React.useState("");

  const valid = amount.trim() !== "" && Number(amount) >= 0;
  const act = (op: "add" | "remove" | "set") => {
    if (!valid) return;
    run(() => adjustPoints(guildId, member.userId, op, Number(amount)), {
      success: `${op === "add" ? "Adding" : op === "remove" ? "Removing" : "Setting"} points`,
    });
    setAmount("");
  };

  return (
    <li className="p-3">
      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          className="flex min-w-0 flex-1 items-center gap-2 text-left"
          aria-expanded={open}
        >
          <ChevronDown className={cn("size-4 shrink-0 text-fg-subtle transition-transform", open && "rotate-180")} aria-hidden />
          <span className="min-w-0 flex-1 truncate text-sm font-medium">{member.name}</span>
        </button>
        {/* Fixed-width columns so the Tier badge and the points read as two tidy
            columns down the list rather than jittering with each row's value. */}
        <span className="shrink-0">
          <Badge variant="neutral">Tier {member.tier}</Badge>
        </span>
        <span className="w-28 shrink-0 whitespace-nowrap text-right text-sm tabular-nums text-fg-muted">
          {member.effective}
          {member.vipBase > 0 && <span className="text-fg-subtle"> ({member.earned}+{member.vipBase})</span>}
        </span>
      </div>

      {open && (
        <div className="mt-3 space-y-3 rounded-md border border-[var(--border)] bg-[var(--surface)] p-3">
          <p className="text-xs text-fg-muted">
            Earned <span className="font-medium text-fg">{member.earned}</span>
            {member.vipBase > 0 && <> · VIP floor <span className="font-medium text-fg">+{member.vipBase}</span> · effective <span className="font-medium text-fg">{member.effective}</span></>}
          </p>

          {member.grants.length > 0 && (
            <div className="text-xs text-fg-muted">
              <p className="mb-1 font-medium text-fg">Active grants</p>
              <ul className="space-y-0.5">
                {member.grants.map((g, i) => (
                  <li key={i} className="tabular-nums">
                    {g.points} pts — expires {g.expiresAt}
                  </li>
                ))}
              </ul>
            </div>
          )}

          <div className="flex flex-wrap items-center gap-2">
            <Input
              type="number"
              min={0}
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
              placeholder="Amount"
              aria-label={`Amount for ${member.name}`}
              className="w-28"
            />
            <Button variant="secondary" size="sm" loading={busy} disabled={!valid} onClick={() => act("add")}>Add</Button>
            <Button variant="secondary" size="sm" loading={busy} disabled={!valid} onClick={() => act("remove")}>Remove</Button>
            <Button variant="secondary" size="sm" loading={busy} disabled={!valid} onClick={() => act("set")}>Set</Button>
            <Button
              variant="ghost"
              size="sm"
              className="ml-auto"
              disabled={busy}
              onClick={() => {
                if (confirm(`Wipe all points for ${member.name}?`)) {
                  run(() => wipeMember(guildId, member.userId), { success: "Wiping member points" });
                }
              }}
            >
              <Trash2 aria-hidden />
              Wipe
            </Button>
          </div>
        </div>
      )}
    </li>
  );
}

function ListsCard({ lists, meta }: { lists: OnbLists; meta: OnbMeta }) {
  return (
    <Card className="p-5">
      <h3 className="text-sm font-semibold">Point lists</h3>
      <Tabs defaultValue="30days" className="mt-3">
        <TabsList>
          <TabsTrigger value="30days">Past 30 days</TabsTrigger>
          <TabsTrigger value="60days">31–60 days</TabsTrigger>
          <TabsTrigger value="alltime">All-time</TabsTrigger>
          <TabsTrigger value="upgrade">Upgrade</TabsTrigger>
        </TabsList>
        <TabsContent value="30days">
          <RankedList rows={lists.days30} />
        </TabsContent>
        <TabsContent value="60days">
          <RankedList rows={lists.days60} />
        </TabsContent>
        <TabsContent value="alltime">
          <RankedList rows={lists.allTime} />
        </TabsContent>
        <TabsContent value="upgrade">
          <UpgradeList rows={lists.upgrade} tier2={meta.tier2Points} tier3={meta.tier3Points} />
        </TabsContent>
      </Tabs>
    </Card>
  );
}

function RankedList({ rows }: { rows: { userId: string; name: string; points: number }[] }) {
  if (rows.length === 0) return <p className="py-8 text-center text-sm text-fg-muted">No data for this window.</p>;
  return (
    <ol className="mt-3 divide-y divide-[var(--border)] rounded-lg border border-[var(--border)]">
      {rows.map((r, i) => (
        <li key={r.userId} className="flex items-center gap-3 p-2.5 text-sm">
          <span className="w-6 shrink-0 text-right tabular-nums text-fg-subtle">{i + 1}.</span>
          <span className="min-w-0 flex-1 truncate">{r.name}</span>
          <span className="tabular-nums text-fg-muted">{r.points} pts</span>
        </li>
      ))}
    </ol>
  );
}

function UpgradeList({
  rows,
  tier2,
  tier3,
}: {
  rows: OnbUpgradeRow[];
  tier2: number | null;
  tier3: number | null;
}) {
  if (rows.length === 0) return <p className="py-8 text-center text-sm text-fg-muted">No members with points yet.</p>;

  // A threshold line sits directly after the last member who clears it. Tier 3
  // first so it prints above Tier 2 when both land in the same spot.
  const markers = new Map<number, string[]>();
  for (const [tier, req] of [[3, tier3], [2, tier2]] as const) {
    if (req == null || req <= 0) continue;
    const idx = rows.filter((r) => r.effective >= req).length;
    const list = markers.get(idx) ?? [];
    list.push(`Tier ${tier} — ${req} pts`);
    markers.set(idx, list);
  }

  return (
    <ol className="mt-3 divide-y divide-[var(--border)] rounded-lg border border-[var(--border)]">
      {rows.map((r, i) => (
        <React.Fragment key={r.userId}>
          {markers.get(i)?.map((label) => (
            <li key={label} className="bg-[var(--surface)] px-2.5 py-1.5 text-center text-xs font-medium text-fg-subtle">
              {label}
            </li>
          ))}
          <li className="flex items-center gap-3 p-2.5 text-sm">
            <span className="w-6 shrink-0 text-right tabular-nums text-fg-subtle">{i + 1}.</span>
            <span className="min-w-0 flex-1 truncate">{r.name}</span>
            <span className="tabular-nums text-fg-muted">
              {r.earned}
              {r.vipBase > 0 && <span className="text-fg-subtle"> (+{r.vipBase})</span>} pts
            </span>
          </li>
        </React.Fragment>
      ))}
      {markers.get(rows.length)?.map((label) => (
        <li key={label} className="bg-[var(--surface)] px-2.5 py-1.5 text-center text-xs font-medium text-fg-subtle">
          {label}
        </li>
      ))}
    </ol>
  );
}
