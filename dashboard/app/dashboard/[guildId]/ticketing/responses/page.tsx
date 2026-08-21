import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/ticketing/store";
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { ResponsesView } from "@/components/ticketing/responses-view";

export const metadata: Metadata = { title: "Responses · Ticketing" };
export const dynamic = "force-dynamic";

export default async function ResponsesPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  if (!store.isAvailable()) {
    return (
      <div className="space-y-6">
        <PageHeader title="Responses" description="Application and survey answers." />
        <EmptyState
          icon="Inbox"
          title="Can't reach Vibey right now"
          description="Responses are stored by the bot itself, so this page needs it to be running."
        />
      </div>
    );
  }

  const topics = store.listTopics();
  const labelFor = new Map(topics.map((t) => [t.name, t.label]));
  const responses = store.listResponses();

  const groups = Object.entries(responses)
    .map(([name, list]) => ({ name, label: labelFor.get(name) ?? name, responses: list }))
    .filter((g) => g.responses.length > 0)
    .sort((a, b) => b.responses.length - a.responses.length);

  const askableTopics = topics
    .filter((t) => t.questions.length > 0)
    .map((t) => ({ name: t.name, label: t.label }));

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/ticketing`}>
          <ArrowLeft aria-hidden />
          Ticketing
        </Link>
      </Button>
      <PageHeader
        title="Responses"
        description="Answers people left on applications and surveys — and send a survey out."
      />
      <ResponsesView guildId={guildId} groups={groups} askableTopics={askableTopics} />
    </div>
  );
}
