import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { formatRelative } from "@/lib/utils";
import {
  readModuleStates,
  readSettings,
  readSettingsMeta,
  isLive,
  storeReachable,
} from "@/lib/bot/adapter";
import { economy } from "@/lib/schema/modules";
import { sectionsFor, type EconomyTab } from "@/lib/economy/tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { ModuleToggle } from "@/components/modules/module-toggle";
import { EconomySettingsPanel } from "@/components/economy/economy-settings-panel";

/**
 * The frame shared by the three Economy settings tabs (Points, Shop, Config).
 *
 * Each tab is the same economy module read once and shown a section at a time,
 * so the live/last-changed line and the back link live here rather than being
 * copied into three near-identical pages. Values are the full module set — the
 * panel needs them all to save without wiping the tabs it isn't showing.
 */
export function EconomyTabView({
  guildId,
  tab,
  showToggle = false,
}: {
  guildId: string;
  tab: EconomyTab;
  /** The Config tab carries the whole module's enable/disable switch. */
  showToggle?: boolean;
}) {
  const values = readSettings(guildId, economy.id);
  const meta = readSettingsMeta(guildId, economy.id);
  const live = isLive(economy.id);
  const reachable = live ? storeReachable(economy.id) : false;
  const sections = sectionsFor(tab);
  const enabled = showToggle
    ? readModuleStates(guildId).get(economy.id)?.enabled !== false
    : true;

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/economy`}>
          <ArrowLeft aria-hidden />
          Economy
        </Link>
      </Button>

      <PageHeader title={tab.title} description={tab.description} />

      {showToggle && (
        <ModuleToggle
          guildId={guildId}
          moduleId={economy.id}
          moduleName="Economy"
          initialEnabled={enabled}
        />
      )}

      <div className="flex flex-wrap items-center gap-3">
        {reachable ? (
          <span className="text-xs text-fg-subtle">
            Changes apply to the bot within about 10 seconds.
          </span>
        ) : (
          <Badge variant="warning">Vibey isn&apos;t running — changes apply when it starts</Badge>
        )}
        {meta && (
          <span className="text-xs text-fg-subtle">
            Last changed {formatRelative(meta.updatedAt)}
          </span>
        )}
      </div>

      <EconomySettingsPanel guildId={guildId} sections={sections} initialValues={values} />
    </div>
  );
}
