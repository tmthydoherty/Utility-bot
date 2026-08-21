import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import * as store from "@/lib/custom-commands/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { CommandEditor } from "@/components/custom-commands/command-editor";

export const dynamic = "force-dynamic";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>;
}): Promise<Metadata> {
  const { id } = await params;
  return { title: id === "new" ? "New command" : "Edit command" };
}

export default async function CommandEditorPage({
  params,
}: {
  params: Promise<{ guildId: string; id: string }>;
}) {
  const { guildId, id } = await params;
  if (!store.isAvailable()) notFound();

  const isNew = id === "new";
  const command = isNew ? null : store.getCommand(guildId, id);
  if (!isNew && !command) notFound();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/custom-commands`}>
          <ArrowLeft aria-hidden />
          Back to Custom Commands
        </Link>
      </Button>
      <PageHeader
        title={isNew ? "New command" : "Edit command"}
        description="A trigger, one or more responses, and who may use it where."
      />
      <CommandEditor guildId={guildId} command={command} />
    </div>
  );
}
