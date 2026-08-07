import "server-only";

import { env } from "@/lib/env";
import { getChannels, getGuild, getRoles } from "@/lib/discord/rest";
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
  const [guild, channels, roles] = await Promise.all([
    getGuild(guildId),
    getChannels(guildId),
    getRoles(guildId),
  ]);

  return {
    id: guild.id,
    name: guild.name,
    iconUrl: guildIconUrl(guild, 128),
    memberCount: guild.approximate_member_count ?? 0,
    channels: channels.map((channel) => ({
      id: channel.id,
      name: channel.name,
      type: channel.type,
      parentId: channel.parent_id,
      position: channel.position,
    })),
    roles: roles.map((role) => ({
      id: role.id,
      name: role.name,
      color: roleColorToCssServer(role.color),
      position: role.position,
      managed: role.managed,
    })),
  };
}

/** The single guild this dashboard administers. */
export function primaryGuildId(): string {
  return env.VIBEY_GUILD_ID;
}
