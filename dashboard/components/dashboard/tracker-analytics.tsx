import { Suspense } from "react";

import { getActiveSession } from "@/auth";
import { percentChange } from "@/lib/utils";
import { loadGuild } from "@/lib/guild";
import { loadActivityView } from "@/lib/bot/tracker-view";
import { loadMemberPool, resolveUsers } from "@/lib/bot/directory";
import { ActivityShowcase } from "@/components/dashboard/activity-showcase";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Reveal } from "@/components/ui/reveal";
import { Skeleton } from "@/components/ui/skeleton";
import { StatTile } from "@/components/ui/stat-tile";
import { SegmentedLinks, type SegmentOption } from "./segmented-links";
import { OverviewFilter } from "./overview-filter";
import { TimeframeMenu, type TimeframeOption } from "./timeframe-menu";
import { UserUrlPicker, ChannelUrlPicker, type PickerItem } from "./url-pickers";
import {
  ChannelsOverviewView,
  ChannelView,
  CompareChannelsView,
  CompareUsersView,
  EmojiView,
  LeaderboardView,
  UserView,
} from "./tracker-views";

/**
 * The Activity Tracker page — the web counterpart of `/tracker`'s panel.
 *
 * Its modes mirror the Discord dashboard's: look up a member (starting on
 * yourself), a channel, the emoji board, the leaderboard, and head-to-head
 * comparisons. All state lives in the URL, so the server renders each mode
 * directly and a selection is a navigation, not a round-trip through client
 * state. It's deliberately not the overview — that page is the server-wide
 * showcase; this one is for drilling into a specific member, channel or emoji.
 */

type SearchParams = Record<string, string | string[] | undefined>;

const TABS: { value: string; label: string; icon: string }[] = [
  { value: "server", label: "Server", icon: "LayoutDashboard" },
  { value: "user", label: "Member", icon: "User" },
  { value: "channel", label: "Channel", icon: "Hash" },
  { value: "emoji", label: "Emoji", icon: "Smile" },
  { value: "leaderboard", label: "Leaderboard", icon: "Trophy" },
  { value: "compare", label: "Compare", icon: "GitBranch" },
];

/** The date ranges every section of the tracker offers, shortest first. 7 days
 *  is the default across all sections. */
const RANGES: { value: string; label: string; days: number; short: string; long: string }[] = [
  { value: "1d", label: "24 hours", days: 1, short: "24h", long: "Last 24 hours" },
  { value: "7d", label: "7 days", days: 7, short: "7d", long: "Last 7 days" },
  { value: "14d", label: "14 days", days: 14, short: "14d", long: "Last 14 days" },
  { value: "30d", label: "30 days", days: 30, short: "30d", long: "Last 30 days" },
  { value: "90d", label: "90 days", days: 90, short: "90d", long: "Last 90 days" },
  { value: "all", label: "All time", days: 3650, short: "all time", long: "All time" },
];

const DEFAULT_RANGE = RANGES.find((r) => r.value === "7d")!;

function rangeFor(value: string | undefined) {
  return RANGES.find((r) => r.value === value) ?? DEFAULT_RANGE;
}

function pick(sp: SearchParams, key: string): string | undefined {
  const value = sp[key];
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value[0];
  return undefined;
}

/**
 * Synchronous shell.
 *
 * This renders from the URL alone — no awaits — so the mode tabs paint and stay
 * interactive the instant a tab is clicked. The heavy, per-mode reads (session,
 * member roster, database queries) live in {@link TrackerContent} behind a
 * Suspense boundary keyed on the params: switching modes shows the skeleton
 * immediately and streams the real content in, rather than freezing the whole
 * page until the server render finishes.
 */
export function TrackerAnalytics({
  guildId,
  searchParams,
  basePath,
}: {
  guildId: string;
  searchParams: SearchParams;
  /** Route the tabs live under. Defaults to the Overview, which hosts them. */
  basePath?: string;
}) {
  const base = basePath ?? `/dashboard/${guildId}`;
  const current = new URLSearchParams();
  for (const [key, value] of Object.entries(searchParams)) {
    const v = typeof value === "string" ? value : Array.isArray(value) ? value[0] : undefined;
    if (v) current.set(key, v);
  }
  const href = (changes: Record<string, string | null>) => {
    const p = new URLSearchParams(current);
    for (const [k, v] of Object.entries(changes)) {
      if (v === null) p.delete(k);
      else p.set(k, v);
    }
    const s = p.toString();
    return s ? `${base}?${s}` : base;
  };

  const tab = pick(searchParams, "tab") ?? "server";

  const tabOptions: SegmentOption[] = TABS.map((t) => ({
    label: t.label,
    icon: t.icon,
    href: href({ tab: t.value }),
    active: tab === t.value,
  }));

  // One date range drives every section that measures over time — the sole
  // exception is the Member view, which is its own multi-window breakdown. The
  // range shares a single `range` param across sections, so switching tabs keeps
  // the chosen timeframe.
  const activeRange = rangeFor(pick(searchParams, "range"));
  const rangeOptions: TimeframeOption[] = RANGES.map((r) => ({
    label: r.label,
    href: href({ range: r.value === "7d" ? null : r.value }),
    active: r.value === activeRange.value,
  }));

  return (
    <div className="space-y-5">
      {/* The six sections collapse into one filter control: only the current
          section shows, and the rest live in its menu. Keeps the top of the
          overview quiet instead of a two-row grid of tabs. The date range sits
          alongside it — an icon that reveals the timeframe only when opened. */}
      <div className="flex flex-wrap items-center gap-3">
        <OverviewFilter options={tabOptions} aria-label="Overview section" />
        {tab !== "user" && <TimeframeMenu options={rangeOptions} aria-label="Date range" />}
      </div>

      {/* Keyed on the params so every navigation re-suspends and shows the
          skeleton at once, instead of holding the last mode until the next
          render resolves. */}
      <Suspense key={current.toString()} fallback={<ContentSkeleton />}>
        <TrackerContent guildId={guildId} searchParams={searchParams} href={href} />
      </Suspense>
    </div>
  );
}

function ContentSkeleton() {
  return (
    <div className="space-y-5">
      <Skeleton className="h-11 w-full max-w-sm" />
      <div className="grid gap-4 sm:grid-cols-3">
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
      </div>
      <Skeleton className="h-64" />
    </div>
  );
}

async function TrackerContent({
  guildId,
  searchParams,
  href,
}: {
  guildId: string;
  searchParams: SearchParams;
  href: (changes: Record<string, string | null>) => string;
}) {
  const session = await getActiveSession();
  const viewerId = session?.user.id ?? null;

  const tab = pick(searchParams, "tab") ?? "server";
  if (tab === "server") {
    const range = rangeFor(pick(searchParams, "range"));
    return (
      <ServerView
        guildId={guildId}
        days={range.days}
        windowLabel={range.long}
        windowShort={range.short}
      />
    );
  }

  // Every non-Member section shares the header's date range.
  const range = rangeFor(pick(searchParams, "range"));
  const days = range.days;
  const windowLabel = range.long;

  // The user picker's roster, loaded only for the modes that need it.
  const needsUserPool = tab === "user" || (tab === "compare" && (pick(searchParams, "cmp") ?? "users") === "users");
  let userItems: PickerItem[] = [];
  if (needsUserPool) {
    userItems = await buildUserItems(guildId, [
      pick(searchParams, "user") ?? viewerId ?? undefined,
      pick(searchParams, "ua"),
      pick(searchParams, "ub"),
    ]);
  }

  return (
    <>
      {tab === "user" && (
        <UserMode
          guildId={guildId}
          userId={pick(searchParams, "user") ?? viewerId}
          viewerId={viewerId}
          items={userItems}
        />
      )}

      {tab === "channel" && (
        <div className="space-y-5">
          <div className="max-w-sm">
            <ChannelUrlPicker param="channel" value={pick(searchParams, "channel") ?? null} />
          </div>
          {pick(searchParams, "channel") ? (
            <ChannelView
              guildId={guildId}
              channelId={pick(searchParams, "channel")!}
              days={days}
              windowLabel={windowLabel}
            />
          ) : (
            <ChannelsOverviewView guildId={guildId} days={days} windowLabel={windowLabel} windowShort={range.short} />
          )}
        </div>
      )}

      {tab === "emoji" && (
        <div className="space-y-5">
          <SegmentedLinks
            size="sm"
            aria-label="Emoji scope"
            options={[
              { label: "All emoji", href: href({ scope: "all" }), active: (pick(searchParams, "scope") ?? "all") === "all" },
              { label: "Server only", href: href({ scope: "server" }), active: pick(searchParams, "scope") === "server" },
            ]}
          />
          <EmojiView
            guildId={guildId}
            days={days}
            scope={pick(searchParams, "scope") === "server" ? "server" : "all"}
            windowLabel={windowLabel}
          />
        </div>
      )}

      {tab === "leaderboard" && (
        <LeaderboardView guildId={guildId} days={days} windowLabel={windowLabel} viewerId={viewerId} />
      )}

      {tab === "compare" && (
        <CompareMode
          guildId={guildId}
          searchParams={searchParams}
          href={href}
          days={days}
          windowLabel={windowLabel}
          userItems={userItems}
        />
      )}
    </>
  );
}

/**
 * The server-wide overview — the dashboard's landing view, now the first tab.
 * Member count is always real; the activity tiles and showcase fill in once the
 * tracker's database is reachable.
 */
async function ServerView({
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
  const [guild, view] = await Promise.all([loadGuild(guildId), loadActivityView(guildId, days)]);
  const stats = view?.stats;
  const deltaPeriod = "vs previous period";

  // Member growth over the window: this window's joins as a share of the count
  // we started it with (current minus joins). Leaves aren't tracked anywhere in
  // the dashboard, so like every other figure here this is gross, not net.
  const memberDelta = stats
    ? (percentChange(guild.memberCount, guild.memberCount - stats.newMembers) ?? undefined)
    : undefined;

  return (
    <div className="space-y-6">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <Reveal>
          <StatTile
            label="Members"
            value={guild.memberCount}
            icon="Users"
            delta={memberDelta}
            deltaPeriod={deltaPeriod}
          />
        </Reveal>
        {stats && (
          <>
            <Reveal delay={1}>
              <StatTile
                label={`Messages (${windowShort})`}
                value={stats.messages}
                icon="MessagesSquare"
                delta={percentChange(stats.messages, stats.messagesPrev) ?? undefined}
                deltaPeriod={deltaPeriod}
                trend={stats.messagesTrend}
              />
            </Reveal>
            <Reveal delay={2}>
              <StatTile
                label={`New members (${windowShort})`}
                value={stats.newMembers}
                icon="Sparkles"
                delta={percentChange(stats.newMembers, stats.newMembersPrev) ?? undefined}
                deltaPeriod={deltaPeriod}
              />
            </Reveal>
          </>
        )}
      </div>

      {view ? (
        <ActivityShowcase view={view} windowLabel={windowLabel} />
      ) : (
        <Card>
          <EmptyState
            icon="ChartLine"
            title="Activity data isn't connected yet"
            description="The overview showcases live message, voice and emoji activity from Vibey's tracker. It fills in as soon as the bot is running and has logged some activity."
          />
        </Card>
      )}
    </div>
  );
}

function UserMode({
  guildId,
  userId,
  viewerId,
  items,
}: {
  guildId: string;
  userId: string | null;
  viewerId: string | null;
  items: PickerItem[];
}) {
  const effective = userId ?? viewerId;
  return (
    <div className="space-y-5">
      <div className="max-w-sm">
        <UserUrlPicker param="user" value={effective} items={items} />
      </div>
      {effective ? (
        <UserView guildId={guildId} userId={effective} isViewer={effective === viewerId} />
      ) : (
        <Card>
          <EmptyState icon="User" title="Look up a member" description="Search for a member above to see their activity." />
        </Card>
      )}
    </div>
  );
}

function CompareMode({
  guildId,
  searchParams,
  href,
  days,
  windowLabel,
  userItems,
}: {
  guildId: string;
  searchParams: SearchParams;
  href: (changes: Record<string, string | null>) => string;
  days: number;
  windowLabel: string;
  userItems: PickerItem[];
}) {
  const cmp = pick(searchParams, "cmp") ?? "users";
  return (
    <div className="space-y-5">
      <SegmentedLinks
        size="sm"
        aria-label="Compare kind"
        options={[
          { label: "Members", href: href({ cmp: "users" }), active: cmp === "users" },
          { label: "Channels", href: href({ cmp: "channels" }), active: cmp === "channels" },
        ]}
      />

      {cmp === "channels" ? (
        <>
          <div className="grid gap-3 sm:grid-cols-2">
            <ChannelUrlPicker param="ca" value={pick(searchParams, "ca") ?? null} placeholder="First channel…" />
            <ChannelUrlPicker param="cb" value={pick(searchParams, "cb") ?? null} placeholder="Second channel…" />
          </div>
          <CompareChannelsView
            guildId={guildId}
            aId={pick(searchParams, "ca") ?? null}
            bId={pick(searchParams, "cb") ?? null}
            days={days}
            windowLabel={windowLabel}
          />
        </>
      ) : (
        <>
          <div className="grid gap-3 sm:grid-cols-2">
            <UserUrlPicker param="ua" value={pick(searchParams, "ua") ?? null} items={userItems} placeholder="First member…" />
            <UserUrlPicker param="ub" value={pick(searchParams, "ub") ?? null} items={userItems} placeholder="Second member…" />
          </div>
          <CompareUsersView
            guildId={guildId}
            aId={pick(searchParams, "ua") ?? null}
            bId={pick(searchParams, "ub") ?? null}
            days={days}
            windowLabel={windowLabel}
          />
        </>
      )}
    </div>
  );
}

/**
 * User-picker items: the member roster, with any currently-selected member who
 * isn't in it resolved and folded in so the picker never shows a bare ID.
 */
async function buildUserItems(
  guildId: string,
  selected: (string | undefined)[],
): Promise<PickerItem[]> {
  const pool = await loadMemberPool(guildId);
  const byId = new Map<string, PickerItem>(
    pool.map((u) => [
      u.id,
      { value: u.id, label: u.name, keywords: u.username ? [u.username] : undefined },
    ]),
  );

  const missing = selected.filter((id): id is string => Boolean(id) && !byId.has(id!));
  if (missing.length) {
    const extra = await resolveUsers(guildId, missing);
    for (const id of missing) {
      const r = extra.get(id);
      if (r) byId.set(id, { value: id, label: r.name });
    }
  }

  return [...byId.values()].sort((a, b) => a.label.localeCompare(b.label));
}
