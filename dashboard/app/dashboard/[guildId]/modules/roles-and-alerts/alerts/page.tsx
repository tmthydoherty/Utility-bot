import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { getGuildConfig } from "@/lib/roles-and-alerts/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { AlertsForm } from "./form";

export const metadata: Metadata = { title: "Alert Roles" };

export default async function AlertsPage({
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
        title="Alert Roles"
        description="Configure the available ping roles that anyone can toggle."
      />

      <AlertsForm guildId={guildId} initialConfig={config} />
    </div>
  );
}
