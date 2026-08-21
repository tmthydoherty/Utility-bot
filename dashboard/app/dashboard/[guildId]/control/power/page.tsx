import type { Metadata } from "next";
import Link from "next/link";
import { ChevronLeft } from "lucide-react";

import { controlConfigured, getHealth } from "@/lib/bot/control";
import { PageHeader } from "@/components/ui/page-header";
import { PowerControls } from "@/components/control/power-controls";

export const metadata: Metadata = { title: "Power" };

export const dynamic = "force-dynamic";

export default async function ControlPowerPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const reachable = controlConfigured() ? (await getHealth()) !== null : false;

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
        title="Power"
        description="Reload every feature at once, or restart Vibey entirely. Restarting drops it off Discord for a few seconds while it comes back."
      />

      <PowerControls guildId={guildId} reachable={reachable} />
    </div>
  );
}
