import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/utility/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { ReminderEditor } from "@/components/utility/reminder-editor";
import { StickyEditor } from "@/components/utility/sticky-editor";
import { ReactionEditor } from "@/components/utility/reaction-editor";
import { MediaEditor } from "@/components/utility/media-editor";

export const dynamic = "force-dynamic";

const KINDS = ["reminders", "stickies", "reactions", "media"] as const;
type Kind = (typeof KINDS)[number];

const TITLES: Record<Kind, { create: string; edit: string; description: string }> = {
  reminders: {
    create: "New reminder",
    edit: "Edit reminder",
    description: "A scheduled post, a repeating message or a one-off — with a full embed, ping and buttons.",
  },
  stickies: {
    create: "New sticky",
    edit: "Edit sticky",
    description: "A message that stays pinned to the bottom of a channel.",
  },
  reactions: {
    create: "New reaction rule",
    edit: "Edit reaction rule",
    description: "Remove or limit reactions in a channel.",
  },
  media: {
    create: "New media channel",
    edit: "Edit media channel",
    description: "Keep a channel to images, videos or files only.",
  },
};

export async function generateMetadata({
  params,
}: {
  params: Promise<{ kind: string }>;
}): Promise<Metadata> {
  const { kind } = await params;
  return { title: KINDS.includes(kind as Kind) ? TITLES[kind as Kind].create : "Utility" };
}

export default async function UtilityEditorPage({
  params,
}: {
  params: Promise<{ guildId: string; kind: string; id: string }>;
}) {
  const { guildId, kind, id } = await params;
  if (!KINDS.includes(kind as Kind)) notFound();
  if (!store.isAvailable()) notFound();

  const isNew = id === "new";
  const copy = TITLES[kind as Kind];

  const editor = await renderEditor(guildId, kind as Kind, isNew ? null : id);
  if (editor === null) notFound();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/utility`}>
          <ArrowLeft aria-hidden />
          Back to Utility
        </Link>
      </Button>
      <PageHeader title={isNew ? copy.create : copy.edit} description={copy.description} />
      {editor}
    </div>
  );
}

async function renderEditor(guildId: string, kind: Kind, id: string | null) {
  switch (kind) {
    case "reminders": {
      const item = id ? store.getReminder(guildId, id) : null;
      if (id && !item) return null;
      return <ReminderEditor guildId={guildId} reminder={item} />;
    }
    case "stickies": {
      const item = id ? store.getSticky(guildId, id) : null;
      if (id && !item) return null;
      return <StickyEditor guildId={guildId} sticky={item} />;
    }
    case "reactions": {
      const item = id ? store.getReactionRule(guildId, id) : null;
      if (id && !item) return null;
      return <ReactionEditor guildId={guildId} rule={item} />;
    }
    case "media": {
      const item = id ? store.getMediaChannel(guildId, id) : null;
      if (id && !item) return null;
      return <MediaEditor guildId={guildId} media={item} />;
    }
  }
}
