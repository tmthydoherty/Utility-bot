"use client";

import { createContext, useContext, useMemo } from "react";

import { ChannelType } from "@/lib/discord/types";
import type { PickerItem } from "@/components/ui/entity-picker";

/**
 * The guild's structure, fetched once on the server and shared with every
 * picker on the page.
 *
 * Deliberately a plain serialisable shape rather than Discord's own objects:
 * this crosses the server/client boundary, and only what the UI actually
 * renders should make the trip.
 */

export interface GuildChannel {
  id: string;
  name: string;
  type: number;
  parentId: string | null;
  position: number;
}

export interface GuildRole {
  id: string;
  name: string;
  color: string | null;
  position: number;
  managed: boolean;
}

export interface GuildEmoji {
  id: string;
  name: string;
  animated: boolean;
  /** Which server this emoji is from — only set for cross-server emoji. */
  guildName?: string;
}

export interface GuildContextValue {
  id: string;
  name: string;
  iconUrl: string | null;
  memberCount: number;
  channels: GuildChannel[];
  roles: GuildRole[];
  /** This server's custom emoji — all a reaction here can use. */
  emojis: GuildEmoji[];
  /**
   * Custom emoji from every server the bot is in, tagged with their server
   * name for grouping. Used by fields that can carry any of them, like a
   * button's emoji.
   */
  allEmojis: GuildEmoji[];
}

const GuildContext = createContext<GuildContextValue | null>(null);

export function GuildProvider({
  value,
  children,
}: {
  value: GuildContextValue;
  children: React.ReactNode;
}) {
  return <GuildContext.Provider value={value}>{children}</GuildContext.Provider>;
}

export function useGuild(): GuildContextValue {
  const ctx = useContext(GuildContext);
  if (!ctx) throw new Error("useGuild must be used inside <GuildProvider>");
  return ctx;
}

const CHANNEL_GLYPHS: Record<number, string> = {
  [ChannelType.GuildText]: "#",
  [ChannelType.GuildAnnouncement]: "📣",
  [ChannelType.GuildVoice]: "🔊",
  [ChannelType.GuildStageVoice]: "🎙",
  [ChannelType.GuildForum]: "💬",
  [ChannelType.GuildMedia]: "🖼",
  [ChannelType.AnnouncementThread]: "🧵",
  [ChannelType.PublicThread]: "🧵",
  [ChannelType.PrivateThread]: "🧵",
};

const THREAD_TYPES = new Set<number>([
  ChannelType.AnnouncementThread,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
]);

/**
 * Channels as picker items, grouped under their category the way Discord shows
 * them — an alphabetical flat list of sixty channels is unnavigable when the
 * mental model is the sidebar. Threads and forum posts group under their parent
 * channel's name instead of a category, since that's where they live.
 */
export function useChannelItems(allowedTypes?: number[]): PickerItem[] {
  const { channels } = useGuild();

  return useMemo(() => {
    const categories = new Map(
      channels
        .filter((c) => c.type === ChannelType.GuildCategory)
        .map((c) => [c.id, c.name]),
    );
    // Parent channel names, so a thread can be filed under "in #general".
    const channelNames = new Map(channels.map((c) => [c.id, c.name]));

    const groupFor = (channel: GuildChannel): string => {
      if (THREAD_TYPES.has(channel.type)) {
        const parent = channel.parentId ? channelNames.get(channel.parentId) : undefined;
        return parent ? `Threads · ${parent}` : "Threads";
      }
      return channel.parentId
        ? (categories.get(channel.parentId) ?? "Uncategorised")
        : "Uncategorised";
    };

    return channels
      .filter((c) => allowedTypes ? allowedTypes.includes(c.type) : c.type !== ChannelType.GuildCategory)
      .sort((a, b) => a.position - b.position)
      .map((channel) => ({
        value: channel.id,
        label: channel.name,
        group: groupFor(channel),
        glyph: CHANNEL_GLYPHS[channel.type] ?? "#",
      }));
  }, [channels, allowedTypes]);
}

/** Roles as picker items, highest first, with their real colours. */
export function useRoleItems(): PickerItem[] {
  const { roles } = useGuild();

  return useMemo(
    () =>
      roles
        // @everyone is a role Discord will happily return and nobody ever means
        // to select; managed roles belong to integrations and can't be assigned.
        .filter((role) => role.name !== "@everyone")
        .sort((a, b) => b.position - a.position)
        .map((role) => ({
          value: role.id,
          label: role.name,
          color: role.color ?? undefined,
          description: role.managed ? "integration" : undefined,
          disabled: role.managed,
        })),
    [roles],
  );
}
