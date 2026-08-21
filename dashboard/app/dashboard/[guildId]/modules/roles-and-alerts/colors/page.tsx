import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { getGuildConfig } from "@/lib/roles-and-alerts/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { ColorsForm } from "./form";

export const metadata: Metadata = { title: "Name Colors" };

export default async function ColorsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const config = getGuildConfig(guildId);

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/roles-and-alerts`}>
          <ArrowLeft aria-hidden />
          Roles &amp; Alerts
        </Link>
      </Button>

      <PageHeader
        title="Name Colors"
        description="Configure the available name colors for each tier."
      />

      <ColorsForm guildId={guildId} initialConfig={config} />
    </div>
  );
}
