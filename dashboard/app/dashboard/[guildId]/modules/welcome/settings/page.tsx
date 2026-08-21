import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { formatRelative } from "@/lib/utils";
import { welcome } from "@/lib/schema/modules";
import {
  isLive,
  readModuleStates,
  readSettings,
  readSettingsMeta,
  storeReachable,
} from "@/lib/bot/adapter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { ModuleSettingsForm } from "@/components/settings/module-settings-form";
import { ModuleToggle } from "@/components/modules/module-toggle";

export const metadata: Metadata = { title: "Welcome settings" };

export default async function WelcomeSettingsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  const values = readSettings(guildId, welcome.id);
  const meta = readSettingsMeta(guildId, welcome.id);
  const live = isLive(welcome.id);
  const reachable = live ? storeReachable(welcome.id) : false;
  const enabled = readModuleStates(guildId).get(welcome.id)?.enabled !== false;

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/welcome`}>
          <ArrowLeft aria-hidden />
          Welcome &amp; Onboarding
        </Link>
      </Button>

      <PageHeader
        title="Settings"
        description="The greeting, introductions, roles, tiers and what replies are worth."
      />

      <ModuleToggle
        guildId={guildId}
        moduleId={welcome.id}
        moduleName={welcome.name}
        initialEnabled={enabled}
      />

      <div className="flex flex-wrap items-center gap-3">
        {live ? (
          reachable ? (
            <span className="text-xs text-fg-subtle">
              Changes apply to the bot within about 10 seconds.
            </span>
          ) : (
            <Badge variant="warning">Vibey isn&apos;t running — changes apply when it starts</Badge>
          )
        ) : (
          <Badge variant="warning">Saved here, not yet sent to the bot</Badge>
        )}
        {meta && (
          <span className="text-xs text-fg-subtle">
            Last changed {formatRelative(meta.updatedAt)}
          </span>
        )}
      </div>

      <ModuleSettingsForm module={welcome} guildId={guildId} initialValues={values} />
    </div>
  );
}
