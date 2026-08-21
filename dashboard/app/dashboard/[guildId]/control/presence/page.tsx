import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ChevronLeft } from "lucide-react";

import { formatRelative } from "@/lib/utils";
import { getModule } from "@/lib/schema/modules";
import { readSettings, readSettingsMeta, isLive, storeReachable } from "@/lib/bot/adapter";
import { Badge } from "@/components/ui/badge";
import { PageHeader } from "@/components/ui/page-header";
import { ModuleSettingsForm } from "@/components/settings/module-settings-form";

export const metadata: Metadata = { title: "Presence" };

export default async function ControlPresencePage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const moduleId = "bot-presence";
  const moduleSchema = getModule(moduleId);
  
  if (!moduleSchema) notFound();

  const values = readSettings(guildId, moduleId);
  const meta = readSettingsMeta(guildId, moduleId);
  const live = isLive(moduleId);
  const botReachable = live ? storeReachable(moduleId) : false;

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
        title={moduleSchema.name}
        description={moduleSchema.description}
      />

      <div className="flex flex-wrap items-center gap-3">
        {live ? (
          botReachable ? (
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

      <ModuleSettingsForm module={moduleSchema} guildId={guildId} initialValues={values} />
      
      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">
        {moduleSchema.cog}
      </p>
    </div>
  );
}
