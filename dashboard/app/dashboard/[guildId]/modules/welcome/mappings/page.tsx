import type { Metadata } from "next";

import { isAvailable, readMappings } from "@/lib/onboarding/store";
import { onboardingTile } from "@/lib/onboarding/tabs";
import { OnboardingSectionFrame } from "@/components/onboarding/section-frame";
import { MappingsManager } from "@/components/onboarding/mappings-manager";

const tile = onboardingTile("mappings");
export const metadata: Metadata = { title: tile.title };

export default async function WelcomeMappingsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const mappings = readMappings(guildId);
  const botOnline = isAvailable();

  return (
    <OnboardingSectionFrame
      guildId={guildId}
      title={tile.title}
      description="Pair a game role with the LFG thread the greeting points a new member to when they pick it up."
    >
      <MappingsManager guildId={guildId} mappings={mappings} botOnline={botOnline} />
    </OnboardingSectionFrame>
  );
}
