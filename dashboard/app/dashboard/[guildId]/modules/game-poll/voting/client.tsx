"use client";

import * as React from "react";
import {
  Trophy,
  Crown,
  Users,
  Gamepad2,
  Vote,
  Sparkles,
  ChevronDown,
  CalendarDays,
  Coins,
} from "lucide-react";

import type {
  VotingHistory,
  VoteKind,
  PollWeek,
  GameRecord,
  VoterRecord,
  GameTally,
} from "@/lib/game-poll/store";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Avatar } from "@/components/ui/avatar";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { cn, formatNumber } from "@/lib/utils";

export interface ResolvedVoter {
  name: string;
  avatar: string | null;
  former: boolean;
}

// The four kinds of vote, ordered strongest → weakest. This order is the stack
// order in every meter and the reading order in every legend, so a boosted 1st
// always sits at the same, leftmost, brightest position.
const KINDS: { key: VoteKind; short: string; full: string; color: string }[] = [
  { key: "firstBoost", short: "1st+", full: "Boosted 1st", color: "var(--warning)" },
  { key: "first", short: "1st", full: "1st place", color: "var(--accent)" },
  { key: "second", short: "2nd", full: "2nd place", color: "var(--accent-2)" },
  { key: "third", short: "3rd", full: "3rd place", color: "var(--fg-subtle)" },
];
const KIND_META = new Map(KINDS.map((k) => [k.key, k] as const));

type KindCounts = Pick<GameTally, "third" | "second" | "first" | "firstBoost">;

function weekLabel(w: Pick<PollWeek, "endedAt" | "pollId">): string {
  if (!w.endedAt) return `Poll #${w.pollId}`;
  return new Date(w.endedAt * 1000).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function ordinal(n: number): string {
  const s = ["th", "st", "nd", "rd"];
  const v = n % 100;
  return `${n}${s[(v - 20) % 10] ?? s[v] ?? s[0]}`;
}

// ------------------------------------------------------------------ meter
function VoteMeter({ counts, className }: { counts: KindCounts; className?: string }) {
  const total = counts.third + counts.second + counts.first + counts.firstBoost;
  return (
    <div
      className={cn("flex h-1.5 w-full overflow-hidden rounded-full bg-surface-active", className)}
      role="img"
      aria-label={`${total} votes`}
    >
      {total > 0 &&
        KINDS.map((k) => {
          const n = counts[k.key];
          if (!n) return null;
          return (
            <div
              key={k.key}
              style={{ width: `${(n / total) * 100}%`, backgroundColor: k.color }}
              title={`${k.full}: ${n}`}
            />
          );
        })}
    </div>
  );
}

function KindChips({ counts, className }: { counts: KindCounts; className?: string }) {
  const any = counts.third + counts.second + counts.first + counts.firstBoost > 0;
  if (!any) return <span className="text-xs text-fg-subtle">No votes</span>;
  return (
    <div className={cn("flex flex-wrap items-center gap-1.5", className)}>
      {KINDS.map((k) => {
        const n = counts[k.key];
        if (!n) return null;
        return (
          <span
            key={k.key}
            className="inline-flex items-center gap-1.5 rounded-full bg-surface px-2 py-0.5 text-xs font-medium text-fg-muted"
            title={k.full}
          >
            <span className="size-2 rounded-full" style={{ backgroundColor: k.color }} />
            {k.short}
            <span className="tabular-nums text-fg">{n}</span>
          </span>
        );
      })}
    </div>
  );
}

function KindLegend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5">
      {KINDS.map((k) => (
        <span key={k.key} className="inline-flex items-center gap-1.5 text-xs text-fg-muted">
          <span className="size-2 rounded-full" style={{ backgroundColor: k.color }} />
          {k.full}
        </span>
      ))}
      <span className="text-xs text-fg-subtle">— 1st+ is a returning player&rsquo;s boosted (2&times;) top pick</span>
    </div>
  );
}

/** The little rank pill on a single ballot pick. */
function RankPill({ kind }: { kind: VoteKind }) {
  const meta = KIND_META.get(kind)!;
  return (
    <span
      className="inline-flex h-5 shrink-0 items-center rounded px-1.5 text-[11px] font-semibold leading-none text-fg"
      style={{ backgroundColor: `color-mix(in srgb, ${meta.color} 22%, transparent)` }}
      title={meta.full}
    >
      <span className="mr-1 size-1.5 rounded-full" style={{ backgroundColor: meta.color }} />
      {meta.short}
    </span>
  );
}

// ------------------------------------------------------------------ hero
function Stat({
  icon: Icon,
  label,
  value,
  hint,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string;
  hint: string;
}) {
  return (
    <Card className="p-5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 space-y-1">
          <p className="truncate text-sm text-fg-muted">{label}</p>
          <p className="text-3xl font-semibold leading-none tracking-tight">{value}</p>
          <p className="truncate text-xs text-fg-subtle">{hint}</p>
        </div>
        <div className="grid size-9 shrink-0 place-items-center rounded-md bg-accent-soft text-accent">
          <Icon className="size-[18px]" />
        </div>
      </div>
    </Card>
  );
}

// ------------------------------------------------------------------ avatar
function GameGlyph({ name, banner, size = 40 }: { name: string; banner: string | null; size?: number }) {
  if (banner) {
    return (
      // eslint-disable-next-line @next/next/no-img-element
      <img
        src={banner}
        alt=""
        style={{ width: size, height: size }}
        className="shrink-0 rounded-lg border border-border object-cover"
      />
    );
  }
  return (
    <div
      style={{ width: size, height: size }}
      className="grid shrink-0 place-items-center rounded-lg border border-border bg-surface-active text-sm font-semibold text-fg-muted"
    >
      {(name[0] ?? "?").toUpperCase()}
    </div>
  );
}

function VoterName({ voter }: { voter: ResolvedVoter | undefined }) {
  return (
    <span className="flex min-w-0 items-center gap-2">
      <Avatar src={voter?.avatar ?? null} name={voter?.name ?? "Member"} size="sm" />
      <span className={cn("truncate font-medium", voter?.former && "text-fg-muted")}>
        {voter?.name ?? "Member"}
      </span>
      {voter?.former && (
        <span className="shrink-0 text-[11px] text-fg-subtle" title="No longer in the server">
          left
        </span>
      )}
    </span>
  );
}

// ==================================================================== ROOT
export function VotingClient({
  history,
  voters,
}: {
  history: VotingHistory;
  voters: Record<string, ResolvedVoter>;
}) {
  const { weeks, games, voters: voterRecords, totals } = history;

  if (totals.votesCast === 0) {
    return (
      <div className="space-y-6">
        <Header />
        <Card className="p-10 text-center">
          <Vote className="mx-auto size-8 text-fg-subtle" />
          <p className="mt-3 font-medium">No votes recorded yet</p>
          <p className="mt-1 text-sm text-fg-muted">
            Once a poll closes, every ballot shows up here — the winning games, the points, and who
            they came from.
          </p>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Header />

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Stat icon={Coins} label="Points awarded" value={formatNumber(totals.pointsAwarded)} hint={`across ${totals.polls} poll${totals.polls === 1 ? "" : "s"}`} />
        <Stat icon={Vote} label="Votes cast" value={formatNumber(totals.votesCast)} hint="ranked picks, all time" />
        <Stat icon={Users} label="Voters" value={formatNumber(totals.voters)} hint="members who took part" />
        <Stat icon={Gamepad2} label="Games in rotation" value={formatNumber(games.length)} hint="have appeared on a ballot" />
      </div>

      <Tabs defaultValue="weeks">
        <TabsList>
          <TabsTrigger value="weeks">
            <CalendarDays /> Weeks
          </TabsTrigger>
          <TabsTrigger value="games">
            <Gamepad2 /> Games
          </TabsTrigger>
          <TabsTrigger value="members">
            <Users /> Members
          </TabsTrigger>
        </TabsList>

        <TabsContent value="weeks">
          <WeeksView weeks={weeks} voters={voters} />
        </TabsContent>
        <TabsContent value="games">
          <GamesView games={games} weeks={weeks} voters={voters} />
        </TabsContent>
        <TabsContent value="members">
          <MembersView voterRecords={voterRecords} weeks={weeks} voters={voters} />
        </TabsContent>
      </Tabs>

      <div className="rounded-lg border border-border bg-surface px-4 py-3">
        <KindLegend />
      </div>
    </div>
  );
}

function Header() {
  return (
    <div className="space-y-2">
      <h2 className="text-lg font-medium">Voting History</h2>
      <p className="text-sm text-fg-muted">
        Every closed poll&rsquo;s votes — which games won, the points they earned, and exactly who
        those points came from.
      </p>
    </div>
  );
}

// ==================================================================== WEEKS
function WeeksView({
  weeks,
  voters,
}: {
  weeks: PollWeek[];
  voters: Record<string, ResolvedVoter>;
}) {
  // Every week's ballots start collapsed; the winner and rankings are the
  // at-a-glance view, and a click opens the full ballot list on demand.
  const [open, setOpen] = React.useState<Set<number>>(() => new Set());
  const toggle = (id: number) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  return (
    <div className="space-y-4">
      {weeks.map((week, idx) => {
        const isOpen = open.has(week.pollId);
        const rest = week.games.slice(1);
        return (
          <Card key={week.pollId} className="overflow-hidden">
            {/* Week header */}
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border p-4 sm:px-5">
              <div className="flex items-center gap-2">
                <CalendarDays className="size-4 text-fg-subtle" />
                <span className="font-medium">{weekLabel(week)}</span>
                {idx === 0 && <Badge variant="accent">Latest</Badge>}
              </div>
              <div className="flex items-center gap-4 text-sm text-fg-muted">
                <span className="inline-flex items-center gap-1.5">
                  <Users className="size-3.5" /> {week.voters}
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <Coins className="size-3.5" /> {formatNumber(week.totalPoints)} pts
                </span>
              </div>
            </div>

            {/* Winner spotlight */}
            {week.winner && (
              <div className="flex items-center gap-4 bg-surface p-4 sm:px-5">
                <GameGlyph name={week.winner.name} banner={null} size={48} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <Crown className="size-4 text-[var(--warning)]" />
                    <span className="truncate text-base font-semibold">{week.winner.name}</span>
                  </div>
                  <div className="mt-1.5">
                    <KindChips counts={week.winner} />
                  </div>
                </div>
                <div className="shrink-0 text-right">
                  <div className="text-2xl font-semibold leading-none tabular-nums">
                    {formatNumber(week.winner.points)}
                  </div>
                  <div className="text-xs text-fg-subtle">points</div>
                </div>
              </div>
            )}

            {/* Runners-up */}
            {rest.length > 0 && (
              <div className="divide-y divide-border">
                {rest.map((g, i) => (
                  <div key={g.gameId} className="flex items-center gap-3 px-4 py-3 sm:px-5">
                    <span className="grid size-6 shrink-0 place-items-center rounded bg-surface-active text-xs font-medium text-fg-muted tabular-nums">
                      {i + 2}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center justify-between gap-3">
                        <span className="truncate font-medium">{g.name}</span>
                        <span className="shrink-0 text-sm tabular-nums text-fg-muted">
                          {formatNumber(g.points)} pts
                        </span>
                      </div>
                      <VoteMeter counts={g} className="mt-2" />
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* Ballots toggle + list */}
            <button
              type="button"
              onClick={() => toggle(week.pollId)}
              className="flex w-full items-center justify-center gap-1.5 border-t border-border py-2.5 text-sm font-medium text-fg-muted transition-colors hover:bg-surface-hover hover:text-fg"
            >
              {isOpen ? "Hide" : "Show"} the {week.voters} ballot{week.voters === 1 ? "" : "s"}
              <ChevronDown className={cn("size-4 transition-transform", isOpen && "rotate-180")} />
            </button>

            {isOpen && (
              <div className="divide-y divide-border border-t border-border">
                {week.ballots.map((ballot) => (
                  <div
                    key={ballot.userId}
                    className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:px-5"
                  >
                    <div className="min-w-0 sm:w-48">
                      <VoterName voter={voters[ballot.userId]} />
                    </div>
                    <div className="flex flex-wrap items-center gap-1.5">
                      {ballot.picks.map((p) => (
                        <span
                          key={`${p.gameId}-${p.rank}`}
                          className="inline-flex items-center gap-1.5 rounded-md bg-surface px-2 py-1 text-xs"
                        >
                          <RankPill kind={p.kind} />
                          <span className="max-w-[9rem] truncate font-medium text-fg">{p.name}</span>
                        </span>
                      ))}
                      <span className="ml-1 shrink-0 text-xs tabular-nums text-fg-subtle">
                        {ballot.points} pts
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        );
      })}
    </div>
  );
}

// ==================================================================== GAMES
function GamesView({
  games,
  weeks,
  voters,
}: {
  games: GameRecord[];
  weeks: PollWeek[];
  voters: Record<string, ResolvedVoter>;
}) {
  const [selected, setSelected] = React.useState<number | null>(null);
  const maxPoints = games[0]?.totalPoints || 1;

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2">
        {games.map((g) => (
          <button
            key={g.gameId}
            type="button"
            onClick={() => setSelected(selected === g.gameId ? null : g.gameId)}
            className={cn(
              "text-left",
              "rounded-lg border p-4 transition-all",
              selected === g.gameId
                ? "border-border-strong bg-surface-hover"
                : "border-border bg-surface hover:border-border-strong hover:bg-surface-hover",
            )}
          >
            <div className="flex items-center gap-3">
              <GameGlyph name={g.name} banner={g.banner} size={44} />
              <div className="min-w-0 flex-1">
                <div className="truncate font-semibold">{g.name}</div>
                <div className="mt-0.5 flex items-center gap-2 text-xs text-fg-muted">
                  <span>{formatNumber(g.totalVotes)} votes</span>
                  {g.wins > 0 && (
                    <span className="inline-flex items-center gap-1 text-[var(--warning)]">
                      <Trophy className="size-3" /> {g.wins} win{g.wins === 1 ? "" : "s"}
                    </span>
                  )}
                  <span className="text-fg-subtle">· {g.appearances} poll{g.appearances === 1 ? "" : "s"}</span>
                </div>
              </div>
              <div className="shrink-0 text-right">
                <div className="text-xl font-semibold leading-none tabular-nums">
                  {formatNumber(g.totalPoints)}
                </div>
                <div className="text-[11px] text-fg-subtle">points</div>
              </div>
            </div>
            <div
              className="mt-3 h-1.5 overflow-hidden rounded-full bg-surface-active"
              aria-hidden
            >
              <div
                className="h-full rounded-full bg-accent"
                style={{ width: `${(g.totalPoints / maxPoints) * 100}%` }}
              />
            </div>
            <div className="mt-2.5">
              <KindChips counts={g} />
            </div>
          </button>
        ))}
      </div>

      {selected != null && (
        <GameDetail
          game={games.find((g) => g.gameId === selected)!}
          weeks={weeks}
          voters={voters}
        />
      )}
    </div>
  );
}

function GameDetail({
  game,
  weeks,
  voters,
}: {
  game: GameRecord;
  weeks: PollWeek[];
  voters: Record<string, ResolvedVoter>;
}) {
  // Reconstruct this game's per-week points/placement and its all-time backers
  // from the ballots we already hold — no extra round trip.
  const { perWeek, backers } = React.useMemo(() => {
    const perWeek: { pollId: number; endedAt: number | null; tally: GameTally; placement: number }[] = [];
    const backerMap = new Map<
      string,
      { userId: string; points: number; votes: number; counts: KindCounts }
    >();
    for (const w of weeks) {
      const idx = w.games.findIndex((t) => t.gameId === game.gameId);
      if (idx === -1) continue;
      perWeek.push({ pollId: w.pollId, endedAt: w.endedAt, tally: w.games[idx]!, placement: idx + 1 });
      for (const b of w.ballots) {
        for (const p of b.picks) {
          if (p.gameId !== game.gameId) continue;
          let e = backerMap.get(b.userId);
          if (!e) {
            e = { userId: b.userId, points: 0, votes: 0, counts: { third: 0, second: 0, first: 0, firstBoost: 0 } };
            backerMap.set(b.userId, e);
          }
          e.points += p.points;
          e.votes += 1;
          e.counts[p.kind] += 1;
        }
      }
    }
    const backers = [...backerMap.values()].sort((a, b) => b.points - a.points);
    return { perWeek, backers };
  }, [game.gameId, weeks]);

  return (
    <Card className="overflow-hidden">
      <div className="flex items-center gap-3 border-b border-border p-4 sm:px-5">
        <GameGlyph name={game.name} banner={game.banner} size={40} />
        <div className="min-w-0">
          <div className="truncate font-semibold">{game.name}</div>
          <div className="text-xs text-fg-muted">
            {formatNumber(game.totalPoints)} pts · {formatNumber(game.totalVotes)} votes ·{" "}
            {game.wins} win{game.wins === 1 ? "" : "s"}
          </div>
        </div>
      </div>

      <div className="grid gap-0 sm:grid-cols-2 sm:divide-x sm:divide-border">
        {/* Per-week record */}
        <div className="p-4 sm:px-5">
          <p className="mb-3 text-xs font-medium uppercase tracking-wide text-fg-subtle">By week</p>
          <div className="space-y-3">
            {perWeek.map((w) => (
              <div key={w.pollId} className="flex items-center gap-3">
                <span
                  className={cn(
                    "grid size-6 shrink-0 place-items-center rounded text-xs font-semibold tabular-nums",
                    w.placement === 1
                      ? "bg-[color-mix(in_srgb,var(--warning)_22%,transparent)] text-fg"
                      : "bg-surface-active text-fg-muted",
                  )}
                  title={`${ordinal(w.placement)} place`}
                >
                  {w.placement}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate text-sm">{weekLabel(w)}</span>
                    <span className="shrink-0 text-sm tabular-nums text-fg-muted">{w.tally.points} pts</span>
                  </div>
                  <VoteMeter counts={w.tally} className="mt-1.5" />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Backers */}
        <div className="border-t border-border p-4 sm:border-t-0 sm:px-5">
          <p className="mb-3 text-xs font-medium uppercase tracking-wide text-fg-subtle">
            Where the points came from
          </p>
          <div className="space-y-2.5">
            {backers.map((b) => (
              <div key={b.userId} className="flex items-center justify-between gap-3">
                <div className="min-w-0 sm:w-40">
                  <VoterName voter={voters[b.userId]} />
                </div>
                <div className="flex items-center gap-2">
                  <KindChips counts={b.counts} />
                  <span className="w-12 shrink-0 text-right text-sm font-medium tabular-nums">
                    {b.points}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Card>
  );
}

// ================================================================== MEMBERS
function MembersView({
  voterRecords,
  weeks,
  voters,
}: {
  voterRecords: VoterRecord[];
  weeks: PollWeek[];
  voters: Record<string, ResolvedVoter>;
}) {
  const [selected, setSelected] = React.useState<string | null>(null);
  const maxPoints = voterRecords[0]?.totalPoints || 1;

  return (
    <div className="space-y-4">
      <Card className="divide-y divide-border overflow-hidden">
        {voterRecords.map((v, idx) => (
          <button
            key={v.userId}
            type="button"
            onClick={() => setSelected(selected === v.userId ? null : v.userId)}
            className={cn(
              "flex w-full items-center gap-3 px-4 py-3 text-left transition-colors sm:px-5",
              selected === v.userId ? "bg-surface-hover" : "hover:bg-surface-hover",
            )}
          >
            <span className="w-5 shrink-0 text-center text-xs font-medium text-fg-subtle tabular-nums">
              {idx + 1}
            </span>
            <Avatar src={voters[v.userId]?.avatar ?? null} name={voters[v.userId]?.name ?? "Member"} size="md" />
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className={cn("truncate font-medium", voters[v.userId]?.former && "text-fg-muted")}>
                  {voters[v.userId]?.name ?? "Member"}
                </span>
                {v.boosts > 0 && (
                  <span
                    className="inline-flex items-center gap-1 text-[11px] text-[var(--warning)]"
                    title={`Cast a boosted 1st ${v.boosts} time${v.boosts === 1 ? "" : "s"}`}
                  >
                    <Sparkles className="size-3" /> {v.boosts}
                  </span>
                )}
              </div>
              <div className="mt-1 h-1.5 w-full max-w-[14rem] overflow-hidden rounded-full bg-surface-active">
                <div className="h-full rounded-full bg-accent" style={{ width: `${(v.totalPoints / maxPoints) * 100}%` }} />
              </div>
            </div>
            <div className="shrink-0 text-right">
              <div className="text-base font-semibold leading-none tabular-nums">{formatNumber(v.totalPoints)}</div>
              <div className="text-[11px] text-fg-subtle">
                {v.totalVotes} votes · {v.pollsVotedIn} poll{v.pollsVotedIn === 1 ? "" : "s"}
              </div>
            </div>
          </button>
        ))}
      </Card>

      {selected != null && (
        <MemberDetail
          record={voterRecords.find((v) => v.userId === selected)!}
          voter={voters[selected]}
          weeks={weeks}
        />
      )}
    </div>
  );
}

function MemberDetail({
  record,
  voter,
  weeks,
}: {
  record: VoterRecord;
  voter: ResolvedVoter | undefined;
  weeks: PollWeek[];
}) {
  const perWeek = React.useMemo(() => {
    const out: { pollId: number; endedAt: number | null; picks: PollWeek["ballots"][number]["picks"]; points: number }[] = [];
    for (const w of weeks) {
      const ballot = w.ballots.find((b) => b.userId === record.userId);
      if (!ballot) continue;
      out.push({ pollId: w.pollId, endedAt: w.endedAt, picks: ballot.picks, points: ballot.points });
    }
    return out;
  }, [record.userId, weeks]);

  const maxFav = record.favourites[0]?.points || 1;

  return (
    <Card className="overflow-hidden">
      <div className="flex items-center gap-3 border-b border-border p-4 sm:px-5">
        <Avatar src={voter?.avatar ?? null} name={voter?.name ?? "Member"} size="lg" />
        <div className="min-w-0">
          <div className="truncate text-base font-semibold">{voter?.name ?? "Member"}</div>
          <div className="text-xs text-fg-muted">
            {formatNumber(record.totalPoints)} pts contributed · {record.totalVotes} votes ·{" "}
            {record.pollsVotedIn} poll{record.pollsVotedIn === 1 ? "" : "s"}
            {record.boosts > 0 && ` · ${record.boosts} boosted`}
          </div>
        </div>
      </div>

      <div className="grid gap-0 sm:grid-cols-2 sm:divide-x sm:divide-border">
        {/* Favourites */}
        <div className="p-4 sm:px-5">
          <p className="mb-3 text-xs font-medium uppercase tracking-wide text-fg-subtle">
            Games they back
          </p>
          <div className="space-y-2.5">
            {record.favourites.map((f) => (
              <div key={f.gameId} className="space-y-1">
                <div className="flex items-center justify-between gap-2">
                  <span className="truncate text-sm font-medium">{f.name}</span>
                  <span className="shrink-0 text-xs tabular-nums text-fg-muted">
                    {f.points} pts · {f.votes} vote{f.votes === 1 ? "" : "s"}
                  </span>
                </div>
                <div className="h-1.5 overflow-hidden rounded-full bg-surface-active">
                  <div className="h-full rounded-full bg-accent" style={{ width: `${(f.points / maxFav) * 100}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Per-week ballots */}
        <div className="border-t border-border p-4 sm:border-t-0 sm:px-5">
          <p className="mb-3 text-xs font-medium uppercase tracking-wide text-fg-subtle">Ballot history</p>
          <div className="space-y-3">
            {perWeek.map((w) => (
              <div key={w.pollId} className="space-y-1.5">
                <div className="flex items-center justify-between gap-2 text-xs text-fg-muted">
                  <span>{weekLabel(w)}</span>
                  <span className="tabular-nums text-fg-subtle">{w.points} pts</span>
                </div>
                <div className="flex flex-wrap items-center gap-1.5">
                  {w.picks.map((p) => (
                    <span
                      key={`${p.gameId}-${p.rank}`}
                      className="inline-flex items-center gap-1.5 rounded-md bg-surface px-2 py-1 text-xs"
                    >
                      <RankPill kind={p.kind} />
                      <span className="max-w-[9rem] truncate font-medium text-fg">{p.name}</span>
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Card>
  );
}
