/** The slices of Discord's API shapes this dashboard actually reads. */

export const ChannelType = {
  GuildText: 0,
  DM: 1,
  GuildVoice: 2,
  GroupDM: 3,
  GuildCategory: 4,
  GuildAnnouncement: 5,
  AnnouncementThread: 10,
  PublicThread: 11,
  PrivateThread: 12,
  GuildStageVoice: 13,
  GuildDirectory: 14,
  GuildForum: 15,
  GuildMedia: 16,
} as const;

export type ChannelTypeValue = (typeof ChannelType)[keyof typeof ChannelType];

/** Channels a setting can sensibly point at — no threads, no DMs. */
export const SELECTABLE_CHANNEL_TYPES: ChannelTypeValue[] = [
  ChannelType.GuildText,
  ChannelType.GuildVoice,
  ChannelType.GuildAnnouncement,
  ChannelType.GuildStageVoice,
  ChannelType.GuildForum,
  ChannelType.GuildMedia,
];

export interface DiscordChannel {
  id: string;
  name: string;
  type: ChannelTypeValue;
  position: number;
  parent_id: string | null;
  nsfw?: boolean;
}

export interface DiscordRole {
  id: string;
  name: string;
  color: number;
  position: number;
  permissions: string;
  managed: boolean;
  hoist: boolean;
  icon?: string | null;
  unicode_emoji?: string | null;
}

export interface DiscordEmoji {
  id: string;
  name: string;
  animated: boolean;
  available: boolean;
}

export interface DiscordUser {
  id: string;
  username: string;
  global_name: string | null;
  discriminator: string;
  avatar: string | null;
}

export interface DiscordMember {
  user?: DiscordUser;
  nick: string | null;
  avatar: string | null;
  roles: string[];
  joined_at: string;
}

export interface DiscordGuild {
  id: string;
  name: string;
  icon: string | null;
  banner: string | null;
  owner_id: string;
  approximate_member_count?: number;
  approximate_presence_count?: number;
  premium_subscription_count?: number;
  premium_tier?: number;
}

/** Discord permission bits the authorisation check cares about. */
export const Permissions = {
  ADMINISTRATOR: 1n << 3n,
  MANAGE_GUILD: 1n << 5n,
} as const;

export function avatarUrl(user: Pick<DiscordUser, "id" | "avatar">, size = 128): string {
  if (!user.avatar) {
    // Post-migration default avatars key off the ID, not the discriminator.
    const index = Number((BigInt(user.id) >> 22n) % 6n);
    return `https://cdn.discordapp.com/embed/avatars/${index}.png`;
  }
  const ext = user.avatar.startsWith("a_") ? "gif" : "png";
  return `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.${ext}?size=${size}`;
}

export function displayName(member: DiscordMember): string {
  return member.nick ?? member.user?.global_name ?? member.user?.username ?? "Unknown";
}
