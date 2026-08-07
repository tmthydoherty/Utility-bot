import type { Metadata } from "next";

import { MODULES } from "@/lib/schema/modules";
import { readModuleStates } from "@/lib/bot/adapter";
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

  // Anything with no stored row is treated as on — that is what the bot does
  // today, and defaulting to off here would show a server full of working
  // features as entirely disabled.
  const initialStates = Object.fromEntries(
    MODULES.map((module) => [module.id, states.get(module.id)?.enabled !== false]),
  );

  const configurable = MODULES.filter((module) => module.configurable).length;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Modules"
        description={`Every cog Vibey loads. ${configurable} can be configured here so far — the rest are listed so nothing is hidden, and are still set up through Discord.`}
      />
      <ModuleGrid modules={MODULES} guildId={guildId} initialStates={initialStates} />
    </div>
  );
}
