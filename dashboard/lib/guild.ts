import "server-only";

import { env } from "@/lib/env";
import { getActiveThreads, getBotGuilds, getChannels, getEmojis, getGuild, getRoles } from "@/lib/discord/rest";
import { guildIconUrl, roleColorToCssServer } from "@/lib/discord/format";
import type { GuildContextValue } from "@/components/providers/guild-provider";

/**
 * The guild, shaped for the client.
 *
 * One function so every page loads the same thing and the 60s REST cache is
 * shared — a layout and three pages each fetching channels independently would
 * be four round trips for identical data.
 */
export async function loadGuild(guildId: string): Promise<GuildContextValue> {
  const [guild, channels, threads, roles, emojis] = await Promise.all([
    getGuild(guildId),
    getChannels(guildId),
    getActiveThreads(guildId),
    getRoles(guildId),
    // A missing Emojis intent or a transient failure must not blank the whole
    // page — the emoji picker just falls back to typing.
    getEmojis(guildId).catch(() => []),
  ]);

  // Threads (including forum posts) sit after the channels list. They carry no
  // position, so they're pushed below real channels and ordered by the picker.
  const allChannels = [...channels, ...threads];

  const usableEmojis = emojis
    .filter((emoji) => emoji.available)
    .map((emoji) => ({ id: emoji.id, name: emoji.name, animated: emoji.animated }));

  // The button emoji picker wants every server's custom emoji, but gathering
  // them is a fan-out of one REST call per server the bot is in — and this
  // function is awaited by the dashboard layout, so paying that cost here would
  // put it on the critical path of *every* page. It doesn't belong there: only
  // the automations builder's button picker ever reads the cross-server list.
  // So seed `allEmojis` with just this server's emoji (free — already fetched)
  // and let that one picker pull the full list lazily via loadCrossServerEmojis.
  const allEmojis = usableEmojis.map((emoji) => ({ ...emoji, guildName: guild.name }));

  return {
    id: guild.id,
    name: guild.name,
    iconUrl: guildIconUrl(guild, 128),
    memberCount: guild.approximate_member_count ?? 0,
    channels: allChannels.map((channel) => ({
      id: channel.id,
      name: channel.name,
      type: channel.type,
      parentId: channel.parent_id,
      position: channel.position ?? Number.MAX_SAFE_INTEGER,
    })),
    roles: roles.map((role) => ({
      id: role.id,
      name: role.name,
      color: roleColorToCssServer(role.color),
      position: role.position,
      managed: role.managed,
    })),
    // Only emoji the bot can actually react with: unavailable ones (lost to a
    // boost-tier drop, say) would show in the picker and then fail to apply.
    emojis: usableEmojis,
    allEmojis,
  };
}

/**
 * Custom emoji from every server the bot is in, tagged with their server name
 * and led by the administered server's own. This is the fan-out kept off the
 * layout's critical path — one REST call per bot guild — loaded on demand by
 * the automations button picker rather than eagerly for every page.
 */
export async function loadCrossServerEmojis(
  guildId: string,
): Promise<{ id: string; name: string; animated: boolean; guildName: string }[]> {
  const [guild, botGuilds] = await Promise.all([
    getGuild(guildId),
    getBotGuilds().catch(() => [] as { id: string; name: string }[]),
  ]);

  const perGuild = await Promise.all(
    botGuilds.map(async (g) => {
      const list = await getEmojis(g.id).catch(() => []);
      return list
        .filter((emoji) => emoji.available)
        .map((emoji) => ({
          id: emoji.id,
          name: emoji.name,
          animated: emoji.animated,
          guildName: g.name,
        }));
    }),
  );

  // The administered server's emoji lead; the rest follow in guild order.
  return perGuild.flat().sort((a, b) => {
    if (a.guildName === guild.name) return b.guildName === guild.name ? 0 : -1;
    return b.guildName === guild.name ? 1 : 0;
  });
}

/** The single guild this dashboard administers. */
export function primaryGuildId(): string {
  return env.VIBEY_GUILD_ID;
}
