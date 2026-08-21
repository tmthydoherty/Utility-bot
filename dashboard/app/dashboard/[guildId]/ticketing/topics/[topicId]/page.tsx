import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/ticketing/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { TopicEditor } from "@/components/ticketing/topic-editor";

export const dynamic = "force-dynamic";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ topicId: string }>;
}): Promise<Metadata> {
  const { topicId } = await params;
  return { title: `${topicId} · Ticketing` };
}

export default async function TopicPage({
  params,
}: {
  params: Promise<{ guildId: string; topicId: string }>;
}) {
  const { guildId, topicId } = await params;

  if (!store.isAvailable()) notFound();
  const topic = store.getTopic(decodeURIComponent(topicId));
  if (!topic) notFound();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/ticketing`}>
          <ArrowLeft aria-hidden />
          Ticketing
        </Link>
      </Button>
      <PageHeader
        title={topic.label}
        description="How this topic behaves — where its tickets go, who staffs them, and what it asks."
      />
      <TopicEditor guildId={guildId} topic={topic} />
    </div>
  );
}
