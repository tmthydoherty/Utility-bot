import { formatCompact, formatNumber, formatVoice } from "@/lib/utils";
import { getEmojis, getMember } from "@/lib/discord/rest";
import { displayName, avatarUrl } from "@/lib/discord/types";
import { resolveChannels, resolveUsers, type ResolvedChannel } from "@/lib/bot/directory";
import {
  readChannelDetail,
  readChannelsOverview,
  readCompareChannel,
  readCompareUser,
  readEmojiBoard,
  readLeaderboards,
  readUserDetail,
  type WindowedCount,
} from "@/lib/bot/tracker-detail";
import { ActivityChart, type ActivityPoint, type ChartSeries } from "@/components/charts/activity-chart";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Icon } from "@/components/ui/icon";
import { LeaderList, type LeaderItem } from "./leader-list";
import { EmojiGlyph } from "./emoji-glyph";
import { emojiLabel } from "@/lib/bot/emoji-name";

const DISCORD_EPOCH = 1420070400000n;

/** A snowflake's creation time, in ms. */
function snowflakeMs(id: string): number {
  try {
    return Number((BigInt(id) >> 22n) + DISCORD_EPOCH);
  } catch {
    return 0;
  }
}

function fmtDate(ms: number): string {
  return new Date(ms).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

/** Turn a dense daily series into labelled points ending today. */
function trendPoints(trend: number[]): ActivityPoint[] {
  const now = new Date();
  return trend.map((value, index) => {
    const date = new Date(now);
    date.setDate(date.getDate() - (trend.length - 1 - index));
    return { label: date.toLocaleDateString("en-US", { month: "short", day: "numeric" }), value };
  });
}

const CHANNEL_ICON: Record<ResolvedChannel["kind"], string> = {
  text: "Hash",
  voice: "Volume2",
  stage: "Radio",
  announcement: "Megaphone",
  forum: "MessagesSquare",
  thread: "MessageSquare",
  channel: "Hash",
};

function unavailable(message: string) {
  return (
    <Card>
      <EmptyState icon="ChartLine" title="Nothing to show" description={message} />
    </Card>
  );
}

function StatCard({
  label,
  value,
  hint,
  icon,
}: {
  label: string;
  value: string;
  hint?: string;
  icon: string;
}) {
  return (
    <Card className="p-4">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="truncate text-sm text-fg-muted">{label}</p>
          <p className="mt-1.5 text-2xl font-semibold leading-none tracking-tight">{value}</p>
          {hint && <p className="mt-1.5 text-xs text-fg-subtle">{hint}</p>}
        </div>
        <div className="grid size-8 shrink-0 place-items-center rounded-md bg-[var(--accent-soft)] text-[var(--accent)]">
          <Icon name={icon} className="size-4" />
        </div>
      </div>
    </Card>
  );
}

function ChartCard({ title, subtitle, trend }: { title: string; subtitle: string; trend: number[] }) {
  return (
    <Card className="p-5 sm:p-6">
      <h2 className="font-semibold tracking-tight">{title}</h2>
      <p className="text-sm text-fg-muted">{subtitle}</p>
      <div className="mt-5">
        <ActivityChart data={trendPoints(trend)} />
      </div>
    </Card>
  );
}

// ==================================================================== USER

export async function UserView({
  guildId,
  userId,
  isViewer,
}: {
  guildId: string;
  userId: string;
  isViewer: boolean;
}) {
  const [detail, member, resolved] = await Promise.all([
    Promise.resolve(readUserDetail(guildId, userId)),
    getMember(guildId, userId).catch(() => null),
    resolveUsers(guildId, [userId]),
  ]);

  if (!detail) return unavailable("No activity has been logged for this member yet.");

  const name = member?.user ? displayName(member) : (resolved.get(userId)?.name ?? "Member");
  const avatar =
    member?.user?.avatar ? avatarUrl(member.user, 128) : resolved.get(userId)?.avatar ?? null;
  const username = member?.user?.username ? `@${member.user.username}` : null;

  const channels = await resolveChannels(
    guildId,
    detail.topChannels.map((c) => c.channelId),
  );
  const maxCh = detail.topChannels[0]?.messages ?? 1;
  const channelItems: LeaderItem[] = detail.topChannels.map((c) => {
    const ch = channels.get(c.channelId);
    return {
      key: c.channelId,
      visual: (
        <div className="grid size-7 place-items-center rounded-md bg-[var(--surface-hover)] text-fg-muted">
          <Icon name={CHANNEL_ICON[ch?.kind ?? "channel"]} className="size-4" />
        </div>
      ),
      name: ch?.name ?? "deleted channel",
      value: formatNumber(c.messages),
      subtitle: ch?.kind,
      barPct: (c.messages / maxCh) * 100,
    };
  });

  return (
    <div className="space-y-4">
      <Card className="flex flex-col gap-4 p-5 sm:flex-row sm:items-center sm:justify-between sm:p-6">
        <div className="flex items-center gap-4">
          <Avatar src={avatar} name={name} size="xl" />
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="truncate text-xl font-semibold tracking-tight">{name}</h2>
              {isViewer && <Badge variant="accent">You</Badge>}
            </div>
            {username && <p className="text-sm text-fg-muted">{username}</p>}
            <p className="mt-1 text-xs text-fg-subtle">
              Account created {fmtDate(snowflakeMs(userId))}
              {member?.joined_at && ` · Joined ${fmtDate(new Date(member.joined_at).getTime())}`}
            </p>
          </div>
        </div>
        <div className="flex gap-2">
          <Badge variant="neutral">
            Msg rank {detail.messageRank ? `#${detail.messageRank}` : "—"}
          </Badge>
          <Badge variant="neutral">
            Voice rank {detail.voiceRank ? `#${detail.voiceRank}` : "—"}
          </Badge>
        </div>
      </Card>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard label="Messages (30d)" value={formatNumber(detail.messages.d30)} hint={`${formatCompact(detail.messages.all)} all-time`} icon="MessagesSquare" />
        <StatCard label="Voice (30d)" value={formatVoice(detail.voiceSeconds.d30)} hint={`${formatVoice(detail.voiceSeconds.all)} all-time`} icon="Mic" />
        <StatCard label="Reactions received" value={formatNumber(detail.reactionsReceived30d)} hint="Last 30 days" icon="Smile" />
        <StatCard label="Messages today" value={formatNumber(detail.messages.d1)} hint={`${formatNumber(detail.messages.d7)} this week`} icon="Flame" />
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <div className="min-w-0">
          <ChartCard title="Message activity" subtitle="Last 14 days" trend={detail.trend14} />
        </div>
        <Card className="p-5 sm:p-6">
          <h2 className="mb-4 font-semibold tracking-tight">Top channels</h2>
          <LeaderList items={channelItems} emptyLabel="No channel activity in 30 days" />
        </Card>
      </div>

      <Card className="p-5 sm:p-6">
        <h2 className="mb-4 font-semibold tracking-tight">Across time</h2>
        <WindowedTable messages={detail.messages} voice={detail.voiceSeconds} />
      </Card>
    </div>
  );
}

function WindowedTable({ messages, voice }: { messages: WindowedCount; voice: WindowedCount }) {
  const rows: [string, keyof WindowedCount][] = [
    ["24 hours", "d1"],
    ["7 days", "d7"],
    ["14 days", "d14"],
    ["30 days", "d30"],
    ["All time", "all"],
  ];
  return (
    <div className="overflow-hidden rounded-lg border border-[var(--border)]">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[var(--border)] text-left text-xs uppercase tracking-wide text-fg-subtle">
            <th className="px-4 py-2.5 font-medium">Window</th>
            <th className="px-4 py-2.5 text-right font-medium">Messages</th>
            <th className="px-4 py-2.5 text-right font-medium">Voice</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([label, key]) => (
            <tr key={key} className="border-b border-[var(--border)] last:border-0">
              <td className="px-4 py-2.5 text-fg-muted">{label}</td>
              <td className="px-4 py-2.5 text-right font-medium tabular-nums">{formatNumber(messages[key])}</td>
              <td className="px-4 py-2.5 text-right font-medium tabular-nums">{formatVoice(voice[key])}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ================================================================= CHANNEL

export async function ChannelView({
  guildId,
  channelId,
  days,
  windowLabel,
}: {
  guildId: string;
  channelId: string;
  days: number;
  windowLabel: string;
}) {
  const detail = readChannelDetail(guildId, channelId, days);
  if (!detail) return unavailable("No messages have been logged in this channel for this window.");

  const [channels, users] = await Promise.all([
    resolveChannels(guildId, [channelId]),
    resolveUsers(guildId, detail.topContributors.map((c) => c.userId)),
  ]);
  const channel = channels.get(channelId);

  // Active members only — contributors who have since left are dropped.
  const activeContributors = detail.topContributors.filter((c) => {
    const u = users.get(c.userId);
    return u && !u.former;
  });
  const maxC = activeContributors[0]?.messages ?? 1;
  const contributorItems: LeaderItem[] = activeContributors.map((c) => {
    const u = users.get(c.userId)!;
    return {
      key: c.userId,
      visual: <Avatar src={u.avatar} name={u.name} size="sm" />,
      name: u.name,
      value: formatNumber(c.messages),
      subtitle: "msgs",
      barPct: (c.messages / maxC) * 100,
    };
  });

  return (
    <div className="space-y-4">
      <Card className="flex items-center gap-3 p-5 sm:p-6">
        <div className="grid size-11 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
          <Icon name={CHANNEL_ICON[channel?.kind ?? "channel"]} className="size-5" />
        </div>
        <div className="min-w-0">
          <h2 className="truncate text-xl font-semibold tracking-tight">{channel?.name ?? "deleted channel"}</h2>
          <p className="text-sm text-fg-muted capitalize">{channel?.kind ?? "channel"} · {windowLabel}</p>
        </div>
      </Card>

      <div className="grid gap-4 sm:grid-cols-3">
        <StatCard label="Messages" value={formatNumber(detail.messages)} hint={windowLabel} icon="MessagesSquare" />
        <StatCard label="Contributors" value={formatNumber(detail.contributors)} hint="Distinct members" icon="Users" />
        <StatCard label="Messages / day" value={formatNumber(detail.avgPerDay)} hint="Average over window" icon="Gauge" />
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <div className="min-w-0">
          <ChartCard title="Messages" subtitle={windowLabel} trend={detail.trend} />
        </div>
        <Card className="p-5 sm:p-6">
          <h2 className="mb-4 font-semibold tracking-tight">Top contributors</h2>
          <LeaderList items={contributorItems} emptyLabel="No contributors yet" />
        </Card>
      </div>
    </div>
  );
}

// ====================================================== CHANNELS OVERVIEW

/**
 * The server-wide channel view — what the Channel tab shows before a single
 * channel is picked. It leads with figures the per-channel detail can't give:
 * channels ranked against one another, and a voice board (voice never appears
 * on the text-only channel detail).
 */
export async function ChannelsOverviewView({
  guildId,
  days,
  windowLabel,
  windowShort,
}: {
  guildId: string;
  days: number;
  windowLabel: string;
  windowShort: string;
}) {
  const overview = readChannelsOverview(guildId, days);
  if (!overview) return unavailable("No channel activity has been logged for this window.");

  const channels = await resolveChannels(guildId, [
    ...overview.topByMessages.map((c) => c.channelId),
    ...overview.topByContributors.map((c) => c.channelId),
    ...overview.topVoice.map((c) => c.channelId),
  ]);

  const visualFor = (id: string) => {
    const ch = channels.get(id);
    return (
      <div className="grid size-7 place-items-center rounded-md bg-[var(--surface-hover)] text-fg-muted">
        <Icon name={CHANNEL_ICON[ch?.kind ?? "channel"]} className="size-4" />
      </div>
    );
  };
  const nameFor = (id: string) => channels.get(id)?.name ?? "deleted channel";
  const people = (n: number) => `${formatNumber(n)} ${n === 1 ? "member" : "members"}`;

  const maxMsg = overview.topByMessages[0]?.messages ?? 1;
  const msgItems: LeaderItem[] = overview.topByMessages.map((c) => ({
    key: c.channelId,
    visual: visualFor(c.channelId),
    name: nameFor(c.channelId),
    value: formatNumber(c.messages),
    subtitle: people(c.contributors),
    barPct: (c.messages / maxMsg) * 100,
  }));

  const maxVoice = overview.topVoice[0]?.seconds ?? 1;
  const voiceItems: LeaderItem[] = overview.topVoice.map((c) => ({
    key: c.channelId,
    visual: visualFor(c.channelId),
    name: nameFor(c.channelId),
    value: formatVoice(c.seconds),
    subtitle: people(c.members),
    barPct: (c.seconds / maxVoice) * 100,
  }));

  const maxContrib = overview.topByContributors[0]?.contributors ?? 1;
  const contribItems: LeaderItem[] = overview.topByContributors.map((c) => ({
    key: c.channelId,
    visual: visualFor(c.channelId),
    name: nameFor(c.channelId),
    value: formatNumber(c.contributors),
    subtitle: `${formatNumber(c.messages)} msgs`,
    barPct: (c.contributors / maxContrib) * 100,
  }));

  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-3">
        <StatCard label={`Active channels (${windowShort})`} value={formatNumber(overview.activeChannels)} icon="Hash" />
        <StatCard label={`Messages (${windowShort})`} value={formatNumber(overview.totalMessages)} icon="MessagesSquare" />
        <StatCard label={`Voice time (${windowShort})`} value={formatVoice(overview.voiceSeconds)} icon="Mic" />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="p-5 sm:p-6">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <h2 className="font-semibold tracking-tight">Busiest channels</h2>
              <p className="text-xs text-fg-subtle">By messages</p>
            </div>
            <Badge variant="neutral" className="shrink-0">{windowLabel}</Badge>
          </div>
          <LeaderList items={msgItems} emptyLabel="No channel activity this window" />
        </Card>
        <Card className="p-5 sm:p-6">
          <h2 className="font-semibold tracking-tight">Top voice channels</h2>
          <p className="mb-4 text-xs text-fg-subtle">By time spent in them</p>
          <LeaderList items={voiceItems} emptyLabel="No voice activity this window" />
        </Card>
      </div>

      <Card className="p-5 sm:p-6">
        <h2 className="font-semibold tracking-tight">Widest reach</h2>
        <p className="mb-4 text-xs text-fg-subtle">Channels with the most distinct people talking</p>
        <LeaderList items={contribItems} emptyLabel="No channel activity this window" />
      </Card>
    </div>
  );
}

// =================================================================== EMOJI

export async function EmojiView({
  guildId,
  days,
  scope,
  windowLabel,
}: {
  guildId: string;
  days: number;
  scope: "all" | "server";
  windowLabel: string;
}) {
  // Server scope lists every one of the guild's own custom emoji, including
  // ones nobody has used in the window — those come back with a zero count.
  let serverEmojis: { id: string; name: string }[] | null = null;
  if (scope === "server") {
    const emojis = await getEmojis(guildId).catch(() => []);
    serverEmojis = emojis.map((e) => ({ id: e.id, name: e.name }));
  }
  const board = readEmojiBoard(guildId, days, serverEmojis);
  if (!board) return unavailable("No emoji usage has been logged for this window.");

  const max = board.rows[0]?.uses ?? 1;
  const items: LeaderItem[] = board.rows.map((e) => ({
    key: e.emojiId ?? e.name,
    visual: <EmojiGlyph emojiId={e.emojiId} name={e.name} size={26} />,
    // Custom emoji use their typed shortcode; a unicode emoji is looked up to
    // Discord's name for it (😭 → :sob:) so the column reads rather than sits
    // blank beside the glyph.
    name: emojiLabel(e.emojiId, e.name),
    value: formatNumber(e.uses),
    subtitle: `${e.users} ${e.users === 1 ? "person" : "people"}`,
    barPct: (e.uses / max) * 100,
  }));

  // Split into two columns for a fuller board on wide screens.
  const half = Math.ceil(items.length / 2);
  const left = items.slice(0, half);
  const right = items.slice(half);

  return (
    <div className="space-y-4">
      {/* One summary strip rather than three loose cards: the two figures read
          as a pair, and the most-used emoji shows as its glyph, not its name. */}
      <Card className="flex flex-wrap items-center gap-x-10 gap-y-5 p-5 sm:p-6">
        <SummaryFigure
          icon="Smile"
          label="Total uses"
          value={formatNumber(board.totalUses)}
          hint={windowLabel}
        />
        <div className="hidden h-12 w-px bg-[var(--border)] sm:block" aria-hidden />
        <SummaryFigure
          icon="Sparkles"
          label="Distinct emoji"
          value={formatNumber(board.distinctCount)}
          hint={scope === "server" ? "Server emoji only" : "All emoji"}
        />
      </Card>

      <Card className="p-5 sm:p-6">
        <h2 className="mb-4 font-semibold tracking-tight">Emoji leaderboard</h2>
        {items.length === 0 ? (
          <p className="py-6 text-center text-sm text-fg-subtle">No emoji used in this window.</p>
        ) : (
          <div className="grid grid-cols-1 gap-x-8 gap-y-1 lg:grid-cols-2">
            {/* Each column wrapper is min-w-0: a grid item defaults to
                min-width:auto, which would let the rows (long shortcodes, the
                value and "N people") push past the card and clip off-screen. */}
            <div className="min-w-0">
              <LeaderList items={left} />
            </div>
            {/* Right column continues the numbering rather than restarting at 1. */}
            {right.length > 0 && (
              <div className="min-w-0">
                <LeaderList items={right} startRank={left.length} />
              </div>
            )}
          </div>
        )}
      </Card>
    </div>
  );
}

function SummaryFigure({
  icon,
  label,
  value,
  hint,
}: {
  icon: string;
  label: string;
  value: string;
  hint: string;
}) {
  return (
    <div className="flex items-center gap-3">
      <div className="grid size-10 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
        <Icon name={icon} className="size-5" />
      </div>
      <div>
        <p className="text-xs uppercase tracking-wide text-fg-subtle">{label}</p>
        <p className="text-2xl font-semibold leading-tight tabular-nums">{value}</p>
        <p className="text-xs text-fg-subtle">{hint}</p>
      </div>
    </div>
  );
}

// ============================================================= LEADERBOARD

export async function LeaderboardView({
  guildId,
  days,
  windowLabel,
  viewerId,
}: {
  guildId: string;
  days: number;
  windowLabel: string;
  viewerId?: string | null;
}) {
  const boards = readLeaderboards(guildId, days, viewerId);
  if (!boards) return unavailable("No activity has been logged for this window.");

  const users = await resolveUsers(guildId, [
    ...boards.messages.map((r) => r.userId),
    ...boards.voice.map((r) => r.userId),
    ...(viewerId ? [viewerId] : []),
  ]);

  // Active members only — anyone who has left the server is filtered out, so the
  // leaderboards never surface a "Former member" row.
  const isActive = (userId: string) => {
    const u = users.get(userId);
    return Boolean(u && !u.former);
  };

  /**
   * A board's rows: the top ten active members, then — if the viewer didn't
   * make that cut but has activity this window — the viewer pinned on below
   * with their real placement, so they always see where they stand.
   */
  const buildBoard = (
    rows: typeof boards.messages,
    viewerStat: { value: number; rank: number | null },
    format: (n: number) => string,
    subtitle?: string,
  ): LeaderItem[] => {
    const active = rows.filter((r) => isActive(r.userId)).slice(0, 10);
    const max = active[0]?.value ?? 1;
    const items: LeaderItem[] = active.map((r) => {
      const u = users.get(r.userId)!;
      return {
        key: r.userId,
        visual: <Avatar src={u.avatar} name={u.name} size="sm" />,
        name: u.name,
        value: format(r.value),
        subtitle,
        barPct: (r.value / max) * 100,
        highlight: r.userId === viewerId,
      };
    });

    if (
      viewerId &&
      viewerStat.rank !== null &&
      viewerStat.value > 0 &&
      !active.some((r) => r.userId === viewerId)
    ) {
      const u = users.get(viewerId);
      if (u) {
        items.push({
          key: `viewer-${viewerId}`,
          visual: <Avatar src={u.avatar} name={u.name} size="sm" />,
          name: u.name,
          value: format(viewerStat.value),
          subtitle,
          barPct: Math.min(100, (viewerStat.value / max) * 100),
          rankLabel: viewerStat.rank,
          highlight: true,
        });
      }
    }
    return items;
  };

  const msgItems = buildBoard(
    boards.messages,
    boards.viewer?.messages ?? { value: 0, rank: null },
    formatNumber,
    "msgs",
  );
  const voiceItems = buildBoard(
    boards.voice,
    boards.viewer?.voice ?? { value: 0, rank: null },
    formatVoice,
  );

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Card className="p-5 sm:p-6">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="font-semibold tracking-tight">Messages</h2>
          <Badge variant="neutral">{windowLabel}</Badge>
        </div>
        <LeaderList items={msgItems} emptyLabel="No messages this window" />
      </Card>
      <Card className="p-5 sm:p-6">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="font-semibold tracking-tight">Voice time</h2>
          <Badge variant="neutral">{windowLabel}</Badge>
        </div>
        <LeaderList items={voiceItems} emptyLabel="No voice activity this window" />
      </Card>
    </div>
  );
}

// ================================================================ COMPARE

interface CompareRow {
  label: string;
  a: string;
  b: string;
  aWins: boolean;
  bWins: boolean;
}

function CompareTable({ rows }: { rows: CompareRow[] }) {
  return (
    <div className="divide-y divide-[var(--border)]">
      {rows.map((row) => (
        <div key={row.label} className="grid grid-cols-[1fr_auto_1fr] items-center gap-3 py-3">
          <div className={row.aWins ? "text-left font-semibold" : "text-left text-fg-muted"}>{row.a}</div>
          <div className="px-2 text-center text-xs uppercase tracking-wide text-fg-subtle">{row.label}</div>
          <div className={row.bWins ? "text-right font-semibold" : "text-right text-fg-muted"}>{row.b}</div>
        </div>
      ))}
    </div>
  );
}

function ComparePrompt({ what }: { what: string }) {
  return (
    <Card>
      <EmptyState icon="GitBranch" title={`Pick two ${what} to compare`} description={`Choose ${what} above and their stats appear side by side.`} />
    </Card>
  );
}

export async function CompareUsersView({
  guildId,
  aId,
  bId,
  days,
  windowLabel,
}: {
  guildId: string;
  aId: string | null;
  bId: string | null;
  days: number;
  windowLabel: string;
}) {
  if (!aId || !bId) return <ComparePrompt what="members" />;
  if (aId === bId) return unavailable("Pick two different members to compare.");

  const [a, b, users] = await Promise.all([
    Promise.resolve(readCompareUser(guildId, aId, days)),
    Promise.resolve(readCompareUser(guildId, bId, days)),
    resolveUsers(guildId, [aId, bId]),
  ]);
  if (!a || !b) return unavailable("Couldn't read stats for one of those members.");

  const ua = users.get(aId);
  const ub = users.get(bId);
  const nameA = ua?.name ?? "Member A";
  const nameB = ub?.name ?? "Member B";
  const rows: CompareRow[] = [
    { label: "Messages", a: formatNumber(a.messages), b: formatNumber(b.messages), aWins: a.messages > b.messages, bWins: b.messages > a.messages },
    { label: "Voice", a: formatVoice(a.voiceSeconds), b: formatVoice(b.voiceSeconds), aWins: a.voiceSeconds > b.voiceSeconds, bWins: b.voiceSeconds > a.voiceSeconds },
    { label: "Msg rank", a: a.rank ? `#${a.rank}` : "—", b: b.rank ? `#${b.rank}` : "—", aWins: rankWins(a.rank, b.rank), bWins: rankWins(b.rank, a.rank) },
    { label: "Reactions", a: formatNumber(a.reactionsReceived), b: formatNumber(b.reactionsReceived), aWins: a.reactionsReceived > b.reactionsReceived, bWins: b.reactionsReceived > a.reactionsReceived },
  ];

  // Member A = --accent, Member B = --chart-2 — a CVD-validated categorical pair
  // (dataviz skill). The same two colours identify each member on both graphs.
  const seriesFor = (values: number[], other: number[]): [ChartSeries, ChartSeries] => [
    { label: nameA, color: "var(--accent)", values },
    { label: nameB, color: "var(--chart-2)", values: other },
  ];

  return (
    <div className="space-y-4">
      <Card className="p-5 sm:p-6">
        <div className="mb-4 flex items-center justify-between gap-3">
          <CompareHead avatar={ua?.avatar} name={ua?.name ?? "Member"} align="left" />
          <Badge variant="neutral" className="shrink-0">{windowLabel}</Badge>
          <CompareHead avatar={ub?.avatar} name={ub?.name ?? "Member"} align="right" />
        </div>
        <CompareTable rows={rows} />
      </Card>
      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="p-5 sm:p-6">
          <h2 className="font-semibold tracking-tight">Messages sent</h2>
          <p className="text-sm text-fg-muted">{windowLabel}</p>
          <div className="mt-5">
            <ActivityChart
              data={trendPoints(a.trend)}
              series={seriesFor(a.trend, b.trend)}
              valueLabel="messages"
            />
          </div>
        </Card>
        <Card className="p-5 sm:p-6">
          <h2 className="font-semibold tracking-tight">Time in voice</h2>
          <p className="text-sm text-fg-muted">{windowLabel}</p>
          <div className="mt-5">
            <ActivityChart
              data={trendPoints(a.voiceTrend)}
              series={seriesFor(a.voiceTrend, b.voiceTrend)}
              valueLabel="voice"
              format="voice"
            />
          </div>
        </Card>
      </div>
    </div>
  );
}

export async function CompareChannelsView({
  guildId,
  aId,
  bId,
  days,
  windowLabel,
}: {
  guildId: string;
  aId: string | null;
  bId: string | null;
  days: number;
  windowLabel: string;
}) {
  if (!aId || !bId) return <ComparePrompt what="channels" />;
  if (aId === bId) return unavailable("Pick two different channels to compare.");

  const [a, b, channels] = await Promise.all([
    Promise.resolve(readCompareChannel(guildId, aId, days)),
    Promise.resolve(readCompareChannel(guildId, bId, days)),
    resolveChannels(guildId, [aId, bId]),
  ]);
  if (!a || !b) return unavailable("Couldn't read stats for one of those channels.");

  const ca = channels.get(aId);
  const cb = channels.get(bId);
  const rows: CompareRow[] = [
    { label: "Messages", a: formatNumber(a.messages), b: formatNumber(b.messages), aWins: a.messages > b.messages, bWins: b.messages > a.messages },
    { label: "Contributors", a: formatNumber(a.contributors), b: formatNumber(b.contributors), aWins: a.contributors > b.contributors, bWins: b.contributors > a.contributors },
    { label: "Msgs / day", a: formatNumber(a.avgPerDay), b: formatNumber(b.avgPerDay), aWins: a.avgPerDay > b.avgPerDay, bWins: b.avgPerDay > a.avgPerDay },
  ];

  return (
    <div className="space-y-4">
      <Card className="p-5 sm:p-6">
        <div className="mb-4 flex items-center justify-between gap-3">
          <CompareHead icon={CHANNEL_ICON[ca?.kind ?? "channel"]} name={ca?.name ?? "deleted channel"} align="left" />
          <Badge variant="neutral" className="shrink-0">{windowLabel}</Badge>
          <CompareHead icon={CHANNEL_ICON[cb?.kind ?? "channel"]} name={cb?.name ?? "deleted channel"} align="right" />
        </div>
        <CompareTable rows={rows} />
      </Card>
      <div className="grid gap-4 lg:grid-cols-2">
        <ChartCard title={ca?.name ?? "Channel A"} subtitle="Messages over window" trend={a.trend} />
        <ChartCard title={cb?.name ?? "Channel B"} subtitle="Messages over window" trend={b.trend} />
      </div>
    </div>
  );
}

function rankWins(a: number | null, b: number | null): boolean {
  if (a === null) return false;
  if (b === null) return true;
  return a < b;
}

function CompareHead({
  avatar,
  icon,
  name,
  align,
}: {
  avatar?: string | null;
  icon?: string;
  name: string;
  align: "left" | "right";
}) {
  return (
    <div className={`flex min-w-0 items-center gap-2.5 ${align === "right" ? "flex-row-reverse text-right" : ""}`}>
      {icon ? (
        <div className="grid size-9 shrink-0 place-items-center rounded-lg bg-[var(--surface-hover)] text-fg-muted">
          <Icon name={icon} className="size-4" />
        </div>
      ) : (
        <Avatar src={avatar} name={name} size="md" />
      )}
      <span className="truncate font-medium">{name}</span>
    </div>
  );
}
