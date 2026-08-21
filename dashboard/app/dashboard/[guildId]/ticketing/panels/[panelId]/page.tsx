import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/ticketing/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { PanelBuilder } from "@/components/ticketing/panel-builder";

export const dynamic = "force-dynamic";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ panelId: string }>;
}): Promise<Metadata> {
  const { panelId } = await params;
  return { title: `${panelId} · Ticketing` };
}

export default async function PanelPage({
  params,
}: {
  params: Promise<{ guildId: string; panelId: string }>;
}) {
  const { guildId, panelId } = await params;

  if (!store.isAvailable()) notFound();
  const panel = store.getPanel(decodeURIComponent(panelId));
  if (!panel) notFound();
  const topics = store.listTopics();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/ticketing`}>
          <ArrowLeft aria-hidden />
          Ticketing
        </Link>
      </Button>
      <PageHeader
        title={panel.title || panel.name}
        description="The message members click to open a ticket. Add topics, arrange them, then publish."
      />
      <PanelBuilder guildId={guildId} panel={panel} allTopics={topics} />
    </div>
  );
}
