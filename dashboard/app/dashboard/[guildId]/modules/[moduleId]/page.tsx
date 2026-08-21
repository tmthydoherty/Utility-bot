import type { Metadata } from "next";
import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { ArrowLeft, ExternalLink } from "lucide-react";

import { formatRelative } from "@/lib/utils";
import { getModule } from "@/lib/schema/modules";
import { readModuleStates, readSettings, readSettingsMeta, isLive, storeReachable } from "@/lib/bot/adapter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { ModuleSettingsForm } from "@/components/settings/module-settings-form";
import { ModuleToggle } from "@/components/modules/module-toggle";
import { QotdQuestionsPanel } from "@/components/qotd/questions-panel";
import { NhieQuestionsPanel } from "@/components/nhie/questions-panel";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ moduleId: string }>;
}): Promise<Metadata> {
  const { moduleId } = await params;
  return { title: getModule(moduleId)?.name ?? "Module" };
}

export default async function ModulePage({
  params,
}: {
  params: Promise<{ guildId: string; moduleId: string }>;
}) {
  const { guildId, moduleId } = await params;

  const moduleSchema = getModule(moduleId);
  if (!moduleSchema) notFound();

  // A relocated module (Economy) lives in its own section now; send any stale
  // link on to it rather than rendering a page that no longer belongs here.
  if (moduleSchema.relocated) {
    redirect(`/dashboard/${guildId}${moduleSchema.link ?? ""}`);
  }

  const values = readSettings(guildId, moduleId);
  const meta = readSettingsMeta(guildId, moduleId);
  const live = isLive(moduleId);
  const botReachable = live ? storeReachable(moduleId) : false;
  const enabled = readModuleStates(guildId).get(moduleId)?.enabled !== false;

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader
        title={moduleSchema.name}
        description={moduleSchema.description}
      />

      {/* The enable/disable switch lives here now, on the module's own page,
          rather than on the grid tile. An always-on module (Bot Presence) has
          no off state, so it shows no switch at all. */}
      {!moduleSchema.alwaysOn && (
        <ModuleToggle
          guildId={guildId}
          moduleId={moduleId}
          moduleName={moduleSchema.name}
          initialEnabled={enabled}
        />
      )}

      {moduleSchema.configurable ? (
        <>
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

          {/* QOTD's question pool isn't a flat setting, so it gets its own panel
              on the same page — the module is the settings and the pool together. */}
          {moduleId === "qotd" && (
            <div className="border-t border-[var(--border)] pt-6">
              <QotdQuestionsPanel guildId={guildId} />
            </div>
          )}

          {/* NHIE question pool */}
          {moduleId === "nhie" && (
            <div className="border-t border-[var(--border)] pt-6">
              <NhieQuestionsPanel guildId={guildId} />
            </div>
          )}
        </>
      ) : (
        <Card>
          <EmptyState
            icon={moduleSchema.icon}
            title="Not configurable from the dashboard yet"
            description={`${moduleSchema.name} still lives entirely in Discord. Its settings panel is in ${moduleSchema.cog}, and this page will fill in when that cog is wired up.`}
            action={
              <Button asChild variant="secondary" size="sm">
                <Link href={`/dashboard/${guildId}/modules`}>
                  Back to modules
                  <ExternalLink aria-hidden />
                </Link>
              </Button>
            }
          />
        </Card>
      )}

      {/* The source of truth in Discord, kept out of the way. It's provenance,
          not something to act on, so it sits at the very bottom and dimmed. */}
      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">
        {moduleSchema.cog}
      </p>
    </div>
  );
}
