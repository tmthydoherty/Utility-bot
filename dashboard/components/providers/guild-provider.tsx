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

export interface GuildContextValue {
  id: string;
  name: string;
  iconUrl: string | null;
  memberCount: number;
  channels: GuildChannel[];
  roles: GuildRole[];
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
};

/**
 * Channels as picker items, grouped under their category the way Discord shows
 * them — an alphabetical flat list of sixty channels is unnavigable when the
 * mental model is the sidebar.
 */
export function useChannelItems(allowedTypes?: number[]): PickerItem[] {
  const { channels } = useGuild();

  return useMemo(() => {
    const categories = new Map(
      channels
        .filter((c) => c.type === ChannelType.GuildCategory)
        .map((c) => [c.id, c.name]),
    );

    return channels
      .filter((c) => c.type !== ChannelType.GuildCategory)
      .filter((c) => !allowedTypes || allowedTypes.includes(c.type))
      .sort((a, b) => a.position - b.position)
      .map((channel) => ({
        value: channel.id,
        label: channel.name,
        group: channel.parentId ? (categories.get(channel.parentId) ?? "Uncategorised") : "Uncategorised",
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
