import type { Metadata } from "next";

import { MODULES } from "@/lib/schema/modules";
import { readModuleStates } from "@/lib/bot/adapter";
import * as automations from "@/lib/automations/store";
import { ModuleGrid } from "@/components/modules/module-grid";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: "Modules" };

export default async function ModulesPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const states = readModuleStates(guildId);

  // Economy has been lifted into its own top-level section, so it's dropped
  // from the grid here even though it stays in MODULES for config lookups.
  const listed = MODULES.filter((module) => !module.relocated);

  // Anything with no stored row is treated as on — that is what the bot does
  // today, and defaulting to off here would show a server full of working
  // features as entirely disabled.
  const initialStates = Object.fromEntries(
    MODULES.map((module) => [module.id, states.get(module.id)?.enabled !== false]),
  );

  // Automations track their on/off through the bot's own pause switch rather
  // than the generic module-state table, so the tile reflects that instead.
  if (automations.isAvailable()) {
    initialStates["automations"] = !automations.automationsPaused();
  }


  return (
    <div className="space-y-6">
      <PageHeader title="Modules" />
      <ModuleGrid modules={listed} guildId={guildId} initialStates={initialStates} />
    </div>
  );
}
