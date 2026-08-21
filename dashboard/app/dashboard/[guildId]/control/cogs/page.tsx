import type { Metadata } from "next";
import Link from "next/link";
import { ChevronLeft } from "lucide-react";

import { controlConfigured, listCogs } from "@/lib/bot/control";
import { PageHeader } from "@/components/ui/page-header";
import { CogList } from "@/components/control/cog-list";

export const metadata: Metadata = { title: "Cogs" };

export default async function ControlCogsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const cogs = controlConfigured() ? await listCogs() : null;

  return (
    <div className="space-y-6">
      <Link
        href={`/dashboard/${guildId}/control`}
        className="inline-flex items-center gap-1 text-sm text-fg-muted transition-colors hover:text-fg"
      >
        <ChevronLeft className="size-4" aria-hidden />
        Bot control
      </Link>

      <PageHeader
        title="Cogs"
        description="Every one of Vibey's feature modules. Reload one to pick up a code change without restarting the whole bot; if a reload fails, its error shows here."
      />

      <CogList guildId={guildId} initialCogs={cogs} />
    </div>
  );
}
