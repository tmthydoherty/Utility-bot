"use client";

import * as React from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { ChannelType } from "@/lib/discord/types";
import { useChannelItems } from "@/components/providers/guild-provider";
import { EntityPicker, type PickerItem } from "@/components/ui/entity-picker";

/**
 * Pickers that drive the tracker page through the URL.
 *
 * The whole page is server-rendered off search params, so a selection isn't
 * component state — it's a navigation. Picking a user replaces `?user=…` and the
 * server re-renders that user's card; nothing here holds data. That keeps the
 * heavy reads on the server and the client bundle to just the popover.
 */

function useSetParam() {
  const router = useRouter();
  const pathname = usePathname();
  const search = useSearchParams();

  return React.useCallback(
    (param: string, value: string | null) => {
      const next = new URLSearchParams(search);
      if (value) next.set(param, value);
      else next.delete(param);
      // scroll:false — flipping a filter shouldn't yank the page to the top.
      router.replace(`${pathname}?${next.toString()}`, { scroll: false });
    },
    [router, pathname, search],
  );
}

export function UserUrlPicker({
  param,
  value,
  items,
  placeholder = "Look up a member…",
}: {
  param: string;
  value: string | null;
  items: PickerItem[];
  placeholder?: string;
}) {
  const setParam = useSetParam();
  return (
    <EntityPicker
      items={items}
      value={value}
      onChange={(v) => setParam(param, v)}
      placeholder={placeholder}
      searchPlaceholder="Search members…"
      emptyMessage="No members found."
      aria-label={placeholder}
      clearable={false}
    />
  );
}

const TRACKER_CHANNEL_TYPES = [
  ChannelType.GuildText,
  ChannelType.GuildAnnouncement,
  ChannelType.GuildVoice,
  ChannelType.GuildStageVoice,
  ChannelType.GuildForum,
  ChannelType.GuildMedia,
  // Threads and forum posts are logged per thread, so they're searchable too.
  ChannelType.AnnouncementThread,
  ChannelType.PublicThread,
  ChannelType.PrivateThread,
];

export function ChannelUrlPicker({
  param,
  value,
  placeholder = "Pick a channel…",
}: {
  param: string;
  value: string | null;
  placeholder?: string;
}) {
  const setParam = useSetParam();
  const items = useChannelItems(TRACKER_CHANNEL_TYPES);
  return (
    <EntityPicker
      items={items}
      value={value}
      onChange={(v) => setParam(param, v)}
      placeholder={placeholder}
      searchPlaceholder="Search channels…"
      emptyMessage="No channels found."
      aria-label={placeholder}
      clearable={false}
    />
  );
}

export type { PickerItem };
