import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft, ExternalLink } from "lucide-react";

import { formatRelative } from "@/lib/utils";
import { getModule } from "@/lib/schema/modules";
import { readSettings, readSettingsMeta } from "@/lib/bot/adapter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";
import { ModuleSettingsForm } from "@/components/settings/module-settings-form";

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

  const values = readSettings(guildId, moduleId);
  const meta = readSettingsMeta(guildId, moduleId);

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
        action={
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="font-mono">
              {moduleSchema.cog}
            </Badge>
          </div>
        }
      />

      {moduleSchema.configurable ? (
        <>
          <div className="flex flex-wrap items-center gap-3">
            <Badge variant="warning">Saved here, not yet sent to the bot</Badge>
            {meta && (
              <span className="text-xs text-fg-subtle">
                Last changed {formatRelative(meta.updatedAt)}
              </span>
            )}
          </div>

          <ModuleSettingsForm module={moduleSchema} guildId={guildId} initialValues={values} />
        </>
      ) : (
        <Card>
          <EmptyState
            icon={moduleSchema.icon}
            title="Not configurable from the web yet"
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

      <Card className="p-5">
        <div className="flex items-start gap-3">
          <Icon name="ShieldCheck" className="mt-0.5 size-5 shrink-0 text-fg-subtle" />
          <p className="text-sm leading-relaxed text-fg-muted">
            Changes made here are recorded in the{" "}
            <Link href={`/dashboard/${guildId}/audit`} className="text-accent hover:underline">
              audit log
            </Link>{" "}
            with who made them and what moved. Nothing on this page writes to Discord or to the
            bot&apos;s own configuration yet.
          </p>
        </div>
      </Card>
    </div>
  );
}
