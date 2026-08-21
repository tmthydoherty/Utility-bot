import "server-only";

import { getChannel, getChannels, getMember, listMembers } from "@/lib/discord/rest";
import { ChannelType, avatarUrl, displayName, type ChannelTypeValue } from "@/lib/discord/types";

/**
 * Turn the raw IDs the tracker stores into names and avatars for display.
 *
 * The stats layer stays pure — IDs and counts, no Discord calls — so the two
 * concerns can be reasoned about and cached separately. Resolution lives here,
 * batched: one member fetch per distinct ID, deduped, all in flight at once,
 * and every miss (a member who left, a deleted channel) degrades to a readable
 * placeholder rather than throwing.
 */

export interface ResolvedUser {
  id: string;
  name: string;
  avatar: string;
  /**
   * True when the ID no longer resolves to a current guild member — someone who
   * left. Their logged activity is kept, but the UI filters these out of every
   * ranked list so only active members are shown.
   */
  former?: boolean;
}

export interface ResolvedChannel {
  id: string;
  name: string;
  kind: "text" | "voice" | "thread" | "forum" | "announcement" | "stage" | "channel";
}

export async function resolveUsers(
  guildId: string,
  ids: string[],
): Promise<Map<string, ResolvedUser>> {
  const unique = [...new Set(ids)];
  if (unique.length === 0) return new Map();

  // Resolve from the guild roster — one call, cached, and shared with the member
  // picker — rather than a getMember round trip per id. That was the overview's
  // real cost: ten leaderboard names meant ten Discord calls every time the 60s
  // cache lapsed. Here it's a single fetch and a map lookup.
  const roster = await listMembers(guildId, 1000).catch(() => []);
  const byId = new Map(roster.filter((m) => m.user).map((m) => [m.user!.id, m]));
  // A short page means we have every current member, so a miss is definitely
  // someone who left. A full page might be truncated, so a miss there still
  // warrants a per-id check before we write them off as a former member.
  const rosterComplete = roster.length < 1000;

  const entries = await Promise.all(
    unique.map(async (id): Promise<[string, ResolvedUser]> => {
      let member = byId.get(id) ?? null;
      if (!member && !rosterComplete) {
        member = await getMember(guildId, id).catch(() => null);
      }
      if (!member?.user) {
        return [id, { id, name: "Former member", avatar: fallbackAvatar(id), former: true }];
      }
      return [
        id,
        {
          id,
          name: displayName(member),
          avatar: member.user.avatar
            ? avatarUrl(member.user, 64)
            : fallbackAvatar(id),
        },
      ];
    }),
  );
  return new Map(entries);
}

/**
 * The roster for the user picker — real members, non-bots, sorted by name.
 *
 * One member-list call rather than a fetch per candidate. Bots are dropped (the
 * tracker never logs them, so they'd be dead options), and anyone the pool
 * misses can still be reached by their stats appearing in leaderboards.
 */
export async function loadMemberPool(
  guildId: string,
): Promise<(ResolvedUser & { username: string })[]> {
  const members = await listMembers(guildId, 1000);
  return members
    .filter((m) => m.user && !("bot" in m.user && (m.user as { bot?: boolean }).bot))
    .map((m) => ({
      id: m.user!.id,
      name: displayName(m),
      // Kept alongside the display name so the picker can search either.
      username: m.user!.username,
      avatar: m.user!.avatar ? avatarUrl(m.user!, 64) : fallbackAvatar(m.user!.id),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

export async function resolveChannels(
  guildId: string,
  ids: string[],
): Promise<Map<string, ResolvedChannel>> {
  const unique = [...new Set(ids)];
  // The guild channel list covers everything but threads in one call; resolve
  // from it first, then fall back to a per-ID lookup for the thread leftovers.
  const listed = new Map(
    (await getChannels(guildId).catch(() => [])).map((c) => [c.id, c]),
  );

  const entries = await Promise.all(
    unique.map(async (id): Promise<[string, ResolvedChannel]> => {
      const known = listed.get(id) ?? (await getChannel(id).catch(() => null));
      if (!known) return [id, { id, name: "deleted channel", kind: "channel" }];
      return [id, { id, name: known.name, kind: channelKind(known.type) }];
    }),
  );
  return new Map(entries);
}

function channelKind(type: ChannelTypeValue): ResolvedChannel["kind"] {
  switch (type) {
    case ChannelType.GuildText:
      return "text";
    case ChannelType.GuildVoice:
      return "voice";
    case ChannelType.GuildStageVoice:
      return "stage";
    case ChannelType.GuildAnnouncement:
      return "announcement";
    case ChannelType.GuildForum:
    case ChannelType.GuildMedia:
      return "forum";
    case ChannelType.PublicThread:
    case ChannelType.PrivateThread:
    case ChannelType.AnnouncementThread:
      return "thread";
    default:
      return "channel";
  }
}

function fallbackAvatar(id: string): string {
  const index = Number((BigInt(id) >> 22n) % 6n);
  return `https://cdn.discordapp.com/embed/avatars/${index}.png`;
}
