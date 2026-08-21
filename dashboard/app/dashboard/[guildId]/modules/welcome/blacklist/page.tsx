import type { Metadata } from "next";

import { isAvailable, readBlacklist } from "@/lib/onboarding/store";
import { onboardingTile } from "@/lib/onboarding/tabs";
import { OnboardingSectionFrame } from "@/components/onboarding/section-frame";
import { BlacklistManager } from "@/components/onboarding/blacklist-manager";

const tile = onboardingTile("blacklist");
export const metadata: Metadata = { title: tile.title };

export default async function WelcomeBlacklistPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const blacklist = readBlacklist();
  const botOnline = isAvailable();

  return (
    <OnboardingSectionFrame
      guildId={guildId}
      title={tile.title}
      description="Members blocked from using the Introduce Yourself button."
    >
      <BlacklistManager guildId={guildId} blacklist={blacklist} botOnline={botOnline} />
    </OnboardingSectionFrame>
  );
}
