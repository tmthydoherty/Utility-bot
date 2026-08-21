import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { getGuildConfig } from "@/lib/roles-and-alerts/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { RolesAndAlertsSettingsForm } from "./form";

export const metadata: Metadata = { title: "Roles & Alerts Settings" };

export default async function RolesAndAlertsSettingsPage({
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
        title="Settings"
        description="The channel and banner images for the alerts & colors."
      />

      <RolesAndAlertsSettingsForm guildId={guildId} initialConfig={config} />
    </div>
  );
}
