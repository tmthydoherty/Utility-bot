import type { Metadata } from "next";
import Link from "next/link";
import { ChevronLeft } from "lucide-react";

import { controlConfigured, tailLogs } from "@/lib/bot/control";
import { PageHeader } from "@/components/ui/page-header";
import { LogView } from "@/components/control/log-view";

export const metadata: Metadata = { title: "Logs" };

// The tail is a live read; never serve a cached copy of it.
export const dynamic = "force-dynamic";

const LINES = 250;

export default async function ControlLogsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const tail = controlConfigured() ? await tailLogs(LINES) : null;

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
        title="Logs"
        description={`The last ${LINES} lines of Vibey's log. Read-only — the same stream that lands in the journal, without the SSH session.`}
      />

      <LogView lines={tail?.available ? tail.lines : null} />
    </div>
  );
}
