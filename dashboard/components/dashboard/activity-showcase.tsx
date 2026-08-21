import { formatCompact, formatNumber, formatVoice } from "@/lib/utils";
import type { ActivityView } from "@/lib/bot/tracker-view";
import type { ResolvedChannel } from "@/lib/bot/directory";
import { ActivityChart, type ActivityPoint } from "@/components/charts/activity-chart";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Icon } from "@/components/ui/icon";
import { Avatar } from "@/components/ui/avatar";
import { Reveal } from "@/components/ui/reveal";
import { Spotlight } from "./spotlight";
import { LeaderList, type LeaderItem } from "./leader-list";
import { EmojiGlyph } from "./emoji-glyph";
import { emojiLabel } from "@/lib/bot/emoji-name";

/**
 * The activity showcase — chart, quick figures, the three spotlights and four
 * ranked boards, all off one already-resolved ActivityView. Every figure but the
 * levels board (which is all-time XP) reflects the selected window; `windowLabel`
 * is that window in words, e.g. "Last 7 days".
 */
export function ActivityShowcase({
  view,
  windowLabel,
}: {
  view: ActivityView;
  windowLabel: string;
}) {
  const { stats } = view;

  // 14 labelled points ending today, built server-side so the x-axis doesn't
  // depend on the reader's clock.
  const now = new Date();
  const chart: ActivityPoint[] = stats.messagesTrend.map((value, index) => {
    const date = new Date(now);
    date.setDate(date.getDate() - (stats.messagesTrend.length - 1 - index));
    return {
      label: date.toLocaleDateString("en-US", { month: "short", day: "numeric" }),
      value,
    };
  });

  // Busiest day over the charted window — the most-recent day matching the peak,
  // so a tie resolves to the latest.
  const busiestValue = Math.max(...stats.messagesTrend, 0);
  const busiestIndex = stats.messagesTrend.lastIndexOf(busiestValue);
  const busiestDate = new Date(now);
  busiestDate.setDate(busiestDate.getDate() - (stats.messagesTrend.length - 1 - busiestIndex));

  const topUser = view.topUsers[0];
  const topChannel = view.topChannels[0];
  const topEmoji = view.topEmojis[0];

  const maxUserMsgs = view.topUsers[0]?.messages ?? 1;
  const maxLevelXp = view.levels[0]?.xp ?? 1;
  const maxChannelMsgs = view.topChannels[0]?.messages ?? 1;
  const maxEmojiUses = view.topEmojis[0]?.uses ?? 1;

  const activeItems: LeaderItem[] = view.topUsers.map((u) => ({
    key: u.userId,
    visual: <Avatar src={u.user.avatar} name={u.user.name} size="sm" />,
    name: u.user.name,
    value: formatNumber(u.messages),
    subtitle: "msgs",
    barPct: (u.messages / maxUserMsgs) * 100,
  }));

  const levelItems: LeaderItem[] = view.levels.map((l) => ({
    key: l.userId,
    visual: <Avatar src={l.user.avatar} name={l.user.name} size="sm" />,
    name: l.user.name,
    value: `Lv ${l.level}`,
    subtitle: `${formatCompact(l.xp)} XP`,
    barPct: (l.xp / maxLevelXp) * 100,
  }));

  const channelItems: LeaderItem[] = view.topChannels.map((c) => ({
    key: c.channelId,
    visual: <ChannelVisual kind={c.channel.kind} />,
    name: c.channel.name,
    value: formatNumber(c.messages),
    subtitle: c.channel.kind,
    barPct: (c.messages / maxChannelMsgs) * 100,
  }));

  const emojiItems: LeaderItem[] = view.topEmojis.map((e) => ({
    key: e.emojiId ?? e.name,
    visual: <EmojiGlyph emojiId={e.emojiId} name={e.name} size={26} />,
    // Custom emoji use their shortcode; unicode emoji are looked up to Discord's
    // name for the glyph (😭 → :sob:) rather than left blank.
    name: emojiLabel(e.emojiId, e.name),
    value: formatNumber(e.uses),
    subtitle: `${e.users} ${e.users === 1 ? "person" : "people"}`,
    barPct: (e.uses / maxEmojiUses) * 100,
  }));

  const avgPerDay = Math.round(stats.messages / stats.windowDays);

  return (
    <div className="space-y-4">
      {/* Chart + quick figures */}
      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <Reveal className="min-w-0">
          <Card className="h-full p-5 sm:p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="font-semibold tracking-tight">Message activity</h2>
                <p className="text-sm text-fg-muted">Last {stats.trendDays} days</p>
              </div>
              <Badge variant="accent">{formatNumber(stats.messages)} messages</Badge>
            </div>
            <div className="mt-5">
              <ActivityChart data={chart} />
            </div>
          </Card>
        </Reveal>

        <Reveal delay={1} className="min-w-0">
          <Card className="flex h-full flex-col p-5 sm:p-6">
            <h2 className="font-semibold tracking-tight">{windowLabel} in numbers</h2>
            <dl className="mt-4 flex-1 space-y-3.5">
              <QuickStat icon="Mic" label="Voice time" value={formatVoice(stats.voiceSeconds)} />
              <QuickStat icon="Smile" label="Reactions given" value={formatNumber(stats.reactions)} />
              <QuickStat icon="Gauge" label="Messages / day" value={formatNumber(avgPerDay)} />
              <QuickStat
                icon="Flame"
                label="Busiest day"
                value={busiestValue > 0 ? busiestDate.toLocaleDateString("en-US", { month: "short", day: "numeric" }) : "—"}
                hint={busiestValue > 0 ? `${formatNumber(busiestValue)} msgs` : undefined}
              />
            </dl>
          </Card>
        </Reveal>
      </div>

      {/* Weekly spotlights */}
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Reveal>
          <Spotlight
            eyebrow="Top member"
            icon="Crown"
            empty={!topUser}
            media={topUser && <Avatar src={topUser.user.avatar} name={topUser.user.name} size="lg" />}
            headline={topUser?.user.name ?? ""}
            value={topUser ? formatNumber(topUser.messages) : ""}
            sub="messages"
          />
        </Reveal>
        <Reveal delay={1}>
          <Spotlight
            eyebrow="Busiest channel"
            icon="Hash"
            empty={!topChannel}
            media={topChannel && <ChannelVisual kind={topChannel.channel.kind} size="lg" />}
            headline={topChannel?.channel.name ?? ""}
            value={topChannel ? formatNumber(topChannel.messages) : ""}
            sub="messages"
          />
        </Reveal>
        <Reveal delay={2}>
          <Spotlight
            eyebrow="Top emoji"
            icon="Sparkles"
            empty={!topEmoji}
            media={topEmoji && <div className="grid size-12 place-items-center rounded-lg bg-[var(--surface-hover)]"><EmojiGlyph emojiId={topEmoji.emojiId} name={topEmoji.name} size={30} /></div>}
            headline={topEmoji ? (emojiLabel(topEmoji.emojiId, topEmoji.name) || topEmoji.name) : ""}
            value={topEmoji ? formatNumber(topEmoji.uses) : ""}
            sub="uses"
          />
        </Reveal>
      </div>

      {/* Ranked boards */}
      <div className="grid gap-4 lg:grid-cols-2">
        <BoardCard title="Levels leaderboard" subtitle="Top XP holders" icon="TrendingUp">
          <LeaderList items={levelItems} emptyLabel="No XP earned yet" />
        </BoardCard>
        <BoardCard title="Most active" subtitle="By messages" icon="Users">
          <LeaderList items={activeItems} emptyLabel="No messages in this window" />
        </BoardCard>
        <BoardCard title="Busiest channels" subtitle="By messages" icon="Hash">
          <LeaderList items={channelItems} emptyLabel="No channel activity yet" />
        </BoardCard>
        <BoardCard title="Top emoji" subtitle="By uses" icon="Smile">
          <LeaderList items={emojiItems} emptyLabel="No emoji used yet" />
        </BoardCard>
      </div>
    </div>
  );
}

function BoardCard({
  title,
  subtitle,
  icon,
  children,
}: {
  title: string;
  subtitle: string;
  icon: string;
  children: React.ReactNode;
}) {
  return (
    <Reveal className="min-w-0">
      <Card className="h-full p-5 sm:p-6">
        <div className="mb-4 flex items-center gap-2.5">
          <div className="grid size-8 shrink-0 place-items-center rounded-md bg-[var(--accent-soft)] text-[var(--accent)]">
            <Icon name={icon} className="size-4" />
          </div>
          <div>
            <h2 className="font-semibold leading-tight tracking-tight">{title}</h2>
            <p className="text-xs text-fg-subtle">{subtitle}</p>
          </div>
        </div>
        {children}
      </Card>
    </Reveal>
  );
}

function QuickStat({
  icon,
  label,
  value,
  hint,
}: {
  icon: string;
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div className="flex items-center gap-3">
      <div className="grid size-8 shrink-0 place-items-center rounded-md bg-[var(--surface-hover)] text-fg-muted">
        <Icon name={icon} className="size-4" />
      </div>
      <dt className="min-w-0 flex-1 truncate text-sm text-fg-muted">{label}</dt>
      <dd className="shrink-0 text-right">
        <span className="text-sm font-semibold tabular-nums">{value}</span>
        {hint && <span className="ml-1.5 text-xs text-fg-subtle">{hint}</span>}
      </dd>
    </div>
  );
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

function ChannelVisual({
  kind,
  size = "sm",
}: {
  kind: ResolvedChannel["kind"];
  size?: "sm" | "lg";
}) {
  return (
    <div
      className={
        size === "lg"
          ? "grid size-12 place-items-center rounded-lg bg-[var(--surface-hover)] text-fg-muted"
          : "grid size-7 place-items-center rounded-md bg-[var(--surface-hover)] text-fg-muted"
      }
    >
      <Icon name={CHANNEL_ICON[kind]} className={size === "lg" ? "size-5" : "size-4"} />
    </div>
  );
}
