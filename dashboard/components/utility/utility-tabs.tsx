"use client";

import * as React from "react";
import Link from "next/link";
import { Plus } from "lucide-react";

import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { EmptyState } from "@/components/ui/empty-state";
import { useGuild } from "@/components/providers/guild-provider";
import type { MediaChannel, ReactionRule, Reminder, Sticky } from "@/lib/utility/types";

const KIND_LABEL: Record<Reminder["kind"], string> = {
  scheduled: "On a schedule",
  interval: "Every so often",
  oneoff: "One-off",
};

export function UtilityTabs({
  guildId,
  reminders,
  stickies,
  reactions,
  media,
}: {
  guildId: string;
  reminders: Reminder[];
  stickies: Sticky[];
  reactions: ReactionRule[];
  media: MediaChannel[];
}) {
  const guild = useGuild();
  const channelName = React.useMemo(() => {
    const map = new Map(guild.channels.map((c) => [c.id, c.name]));
    return (id: string) => map.get(id) ?? `channel ${id}`;
  }, [guild.channels]);

  const base = `/dashboard/${guildId}/utility`;

  return (
    <Tabs defaultValue="reminders">
      <TabsList className="flex-wrap">
        <TabsTrigger value="reminders">Reminders &amp; messages</TabsTrigger>
        <TabsTrigger value="stickies">Sticky notes</TabsTrigger>
        <TabsTrigger value="reactions">Reaction rules</TabsTrigger>
        <TabsTrigger value="media">Media channels</TabsTrigger>
      </TabsList>

      <TabsContent value="reminders" className="space-y-3">
        <NewButton href={`${base}/reminders/new`} label="New reminder or message" />
        {reminders.length === 0 ? (
          <Empty title="No reminders yet" body="Build a scheduled post, a repeating message or a one-off — with a full embed, ping and buttons." />
        ) : (
          reminders.map((r) => (
            <RowCard
              key={r.id}
              href={`${base}/reminders/${r.id}`}
              title={r.name || "Untitled reminder"}
              subtitle={`${KIND_LABEL[r.kind]} · ${r.channelIds.map((c) => `#${channelName(c)}`).join(", ") || "no channel"}`}
              enabled={r.enabled}
            />
          ))
        )}
      </TabsContent>

      <TabsContent value="stickies" className="space-y-3">
        <NewButton href={`${base}/stickies/new`} label="New sticky" />
        {stickies.length === 0 ? (
          <Empty title="No sticky notes" body="Pin a message to the bottom of a channel so it's always the last thing people see." />
        ) : (
          stickies.map((s) => (
            <RowCard
              key={s.id}
              href={`${base}/stickies/${s.id}`}
              title={s.name || `Sticky in #${channelName(s.channelId)}`}
              subtitle={`#${channelName(s.channelId)}`}
              enabled={s.enabled}
            />
          ))
        )}
      </TabsContent>

      <TabsContent value="reactions" className="space-y-3">
        <NewButton href={`${base}/reactions/new`} label="New reaction rule" />
        {reactions.length === 0 ? (
          <Empty title="No reaction rules" body="Automatically remove or limit reactions in a channel." />
        ) : (
          reactions.map((r) => (
            <RowCard
              key={r.id}
              href={`${base}/reactions/${r.id}`}
              title={`#${channelName(r.channelId)}`}
              subtitle={r.mode === "remove" ? "Removes reactions" : r.mode === "allowlist" ? "Allowlist" : "Blocklist"}
              enabled={r.enabled}
            />
          ))
        )}
      </TabsContent>

      <TabsContent value="media" className="space-y-3">
        <NewButton href={`${base}/media/new`} label="New media channel" />
        {media.length === 0 ? (
          <Empty title="No media channels" body="Make a channel accept only images, videos or files — everything else is removed." />
        ) : (
          media.map((m) => (
            <RowCard
              key={m.id}
              href={`${base}/media/${m.id}`}
              title={`#${channelName(m.channelId)}`}
              subtitle={[
                m.allowAttachments && "attachments",
                m.allowLinks && "links",
                m.allowEmbeds && "embeds",
                m.allowStickers && "stickers",
              ]
                .filter(Boolean)
                .join(", ") || "nothing allowed"}
              enabled={m.enabled}
            />
          ))
        )}
      </TabsContent>
    </Tabs>
  );
}

function NewButton({ href, label }: { href: string; label: string }) {
  return (
    <Button asChild size="sm">
      <Link href={href}>
        <Plus aria-hidden /> {label}
      </Link>
    </Button>
  );
}

function RowCard({
  href,
  title,
  subtitle,
  enabled,
}: {
  href: string;
  title: string;
  subtitle: string;
  enabled: boolean;
}) {
  return (
    <Link href={href} className="block">
      <Card className="flex items-center gap-3 p-4 transition-colors hover:bg-[var(--surface-hover)]">
        <div className="min-w-0 flex-1">
          <div className="truncate font-medium">{title}</div>
          <div className="truncate text-sm text-fg-subtle">{subtitle}</div>
        </div>
        <Badge variant={enabled ? "success" : "neutral"}>{enabled ? "On" : "Off"}</Badge>
      </Card>
    </Link>
  );
}

function Empty({ title, body }: { title: string; body: string }) {
  return <EmptyState icon="Wand2" title={title} description={body} />;
}
