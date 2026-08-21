import "server-only";

import { readActivityStats, type ActivityStats, type RankedEmoji } from "@/lib/bot/tracker-stats";
import { resolveChannels, resolveUsers, type ResolvedChannel, type ResolvedUser } from "@/lib/bot/directory";

/**
 * The overview and the Activity Tracker page want the same thing: live stats
 * with the IDs turned into names. This assembles it once — read the numbers,
 * resolve exactly the users and channels those numbers reference, and hand back
 * arrays that are ready to render. Both pages stay declarative as a result.
 */

export interface RankedUserView {
  userId: string;
  messages: number;
  user: ResolvedUser;
}

export interface LevelView {
  userId: string;
  level: number;
  xp: number;
  messages: number;
  voiceSeconds: number;
  user: ResolvedUser;
}

export interface RankedChannelView {
  channelId: string;
  messages: number;
  channel: ResolvedChannel;
}

export interface ActivityView {
  stats: ActivityStats;
  topUsers: RankedUserView[];
  levels: LevelView[];
  topChannels: RankedChannelView[];
  topEmojis: RankedEmoji[];
}

const UNKNOWN_CHANNEL = (id: string): ResolvedChannel => ({
  id,
  name: "deleted channel",
  kind: "channel",
});

export async function loadActivityView(
  guildId: string,
  days = 7,
): Promise<ActivityView | null> {
  const stats = readActivityStats(guildId, days);
  if (!stats) return null;

  const userIds = [
    ...stats.topUsers.map((u) => u.userId),
    ...stats.levels.map((l) => l.userId),
  ];
  const channelIds = stats.topChannels.map((c) => c.channelId);

  const [users, channels] = await Promise.all([
    resolveUsers(guildId, userIds),
    resolveChannels(guildId, channelIds),
  ]);

  // Only current members appear in ranked lists. A user who has left resolves to
  // no member (or a `former` placeholder); their rows are dropped here so the
  // overview shows active people only, even though their activity is still
  // stored and still counts toward the server-wide totals above.
  const activeUser = (id: string): ResolvedUser | null => {
    const u = users.get(id);
    return u && !u.former ? u : null;
  };

  return {
    stats,
    topUsers: stats.topUsers.flatMap((u) => {
      const user = activeUser(u.userId);
      return user ? [{ ...u, user }] : [];
    }),
    levels: stats.levels.flatMap((l) => {
      const user = activeUser(l.userId);
      return user ? [{ ...l, user }] : [];
    }),
    topChannels: stats.topChannels.map((c) => ({
      ...c,
      channel: channels.get(c.channelId) ?? UNKNOWN_CHANNEL(c.channelId),
    })),
    topEmojis: stats.topEmojis,
  };
}
